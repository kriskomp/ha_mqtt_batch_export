"""MQTT Batch Export: one-way export of selected sensors to a remote MQTT broker."""
from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import timedelta
from pathlib import Path

import paho.mqtt.client as mqtt
import voluptuous as vol

from homeassistant.const import (
    EVENT_HOMEASSISTANT_STARTED,
    EVENT_HOMEASSISTANT_STOP,
    STATE_UNAVAILABLE,
    STATE_UNKNOWN,
)
from homeassistant.core import CoreState, Event, HomeAssistant, callback
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.event import (
    async_track_state_change_event,
    async_track_time_interval,
)
from homeassistant.helpers.typing import ConfigType
from homeassistant.util.yaml import Secrets, load_yaml_dict

_LOGGER = logging.getLogger(__name__)

DOMAIN = "mqtt_batch_export"
CONFIG_FILE = "mqtt_batch_export.yaml"
CONFIG_SCHEMA = cv.empty_config_schema(DOMAIN)

FILE_SCHEMA = vol.Schema(
    {
        vol.Required("alias"): cv.slug,
        vol.Required("broker"): vol.Schema(
            {
                vol.Required("host"): cv.string,
                vol.Optional("port", default=1883): cv.port,
                vol.Optional("username"): cv.string,
                vol.Optional("password"): cv.string,
                vol.Optional("tls", default=False): cv.boolean,
            }
        ),
        vol.Optional("discovery_prefix", default="homeassistant"): cv.string,
        vol.Required("groups"): {
            cv.string: vol.Schema(
                {
                    vol.Required("interval"): vol.All(vol.Coerce(int), vol.Range(min=1)),
                    vol.Required("entities"): vol.All(cv.ensure_list, [cv.entity_id]),
                }
            )
        },
    }
)


def _load(hass: HomeAssistant) -> dict:
    path = Path(hass.config.path(CONFIG_FILE))
    raw = load_yaml_dict(str(path), Secrets(Path(hass.config.config_dir)))
    return FILE_SCHEMA(raw)


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    try:
        cfg = await hass.async_add_executor_job(_load, hass)
    except Exception as err:  # noqa: BLE001
        _LOGGER.error("Cannot load %s: %s", CONFIG_FILE, err)
        return False

    exporter = Exporter(hass, cfg)
    if not exporter.interval_of:
        _LOGGER.warning("No sensor entities configured, nothing to export")
        return True

    async def _start(_: Event | None = None) -> None:
        await exporter.async_start()

    if hass.state is CoreState.running:
        await _start()
    else:
        hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STARTED, _start)
    hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, exporter.async_stop)
    return True


class Exporter:
    """Publishes state of listed sensors as <alias>/<entity_id> (retained)."""

    def __init__(self, hass: HomeAssistant, cfg: dict) -> None:
        self.hass = hass
        self.alias: str = cfg["alias"]
        self.prefix: str = cfg["discovery_prefix"]
        self.broker: dict = cfg["broker"]
        self.interval_of: dict[str, int] = {}
        self.by_interval: dict[int, list[str]] = defaultdict(list)
        for group in cfg["groups"].values():
            for ent in group["entities"]:
                if not ent.startswith("sensor."):
                    _LOGGER.warning("Skipping %s: only sensor.* is exported for now", ent)
                    continue
                if ent in self.interval_of:
                    continue
                self.interval_of[ent] = group["interval"]
                self.by_interval[group["interval"]].append(ent)
        self.pending: dict[str, object] = {}
        self.sent: dict[str, str] = {}
        self.disco: set[str] = set()
        self.connected = False
        self._unsubs: list = []
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"ha_{self.alias}_batch_export",
        )
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect

    # --- paho thread callbacks ---
    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code.is_failure:
            _LOGGER.error("MQTT connect refused: %s", reason_code)
            return
        self.hass.loop.call_soon_threadsafe(self._connected)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties=None):
        self.hass.loop.call_soon_threadsafe(self._disconnected)

    # --- event loop ---
    @callback
    def _connected(self) -> None:
        _LOGGER.info("Connected to %s:%s", self.broker["host"], self.broker["port"])
        self.connected = True
        self.sent.clear()
        self.disco.clear()
        for ent in self.interval_of:
            st = self.hass.states.get(ent)
            if self._valid(st):
                self.pending[ent] = st
        self._flush(list(self.interval_of))

    @callback
    def _disconnected(self) -> None:
        if self.connected:
            _LOGGER.warning("Disconnected from broker, will resend latest values on reconnect")
        self.connected = False

    @staticmethod
    def _valid(st) -> bool:
        return st is not None and st.state not in (STATE_UNKNOWN, STATE_UNAVAILABLE, "")

    async def async_start(self) -> None:
        await self.hass.async_add_executor_job(self._start_client)
        self._unsubs.append(
            async_track_state_change_event(
                self.hass, list(self.interval_of), self._state_changed
            )
        )
        for interval in self.by_interval:
            self._unsubs.append(
                async_track_time_interval(
                    self.hass, self._make_tick(interval), timedelta(seconds=interval)
                )
            )

    def _start_client(self) -> None:
        b = self.broker
        if b.get("username"):
            self.client.username_pw_set(b["username"], b.get("password"))
        if b["tls"]:
            self.client.tls_set()
        self.client.reconnect_delay_set(min_delay=1, max_delay=120)
        self.client.connect_async(b["host"], b["port"], keepalive=60)
        self.client.loop_start()

    def _make_tick(self, interval: int):
        @callback
        def _tick(_now) -> None:
            self._flush(self.by_interval[interval])

        return _tick

    @callback
    def _state_changed(self, event: Event) -> None:
        new = event.data.get("new_state")
        if self._valid(new):
            self.pending[new.entity_id] = new

    @callback
    def _flush(self, entities: list[str]) -> None:
        if not self.connected:
            return  # latest values stay pending, everything resent on reconnect
        for ent in entities:
            st = self.pending.pop(ent, None)
            if st is None or self.sent.get(ent) == st.state:
                continue
            if ent not in self.disco:
                self._publish_discovery(ent, st)
                self.disco.add(ent)
            self.client.publish(f"{self.alias}/{ent}", st.state, qos=1, retain=True)
            self.sent[ent] = st.state

    def _publish_discovery(self, ent: str, st) -> None:
        a = st.attributes
        obj = ent.split(".", 1)[1]
        cfg = {
            "name": a.get("friendly_name") or ent,
            "unique_id": f"{self.alias}_{ent}",
            "default_entity_id": f"sensor.{self.alias}_{obj}",
            "state_topic": f"{self.alias}/{ent}",
            "device": {
                "identifiers": [f"mqtt_batch_export_{self.alias}"],
                "name": self.alias,
            },
        }
        for key in ("unit_of_measurement", "device_class", "state_class"):
            if a.get(key):
                cfg[key] = a[key]
        if a.get("device_class") == "enum" and a.get("options"):
            cfg["options"] = list(a["options"])
        self.client.publish(
            f"{self.prefix}/sensor/{self.alias}_{obj}/config",
            json.dumps(cfg),
            qos=1,
            retain=True,
        )

    async def async_stop(self, _: Event | None = None) -> None:
        for unsub in self._unsubs:
            unsub()
        self._unsubs.clear()
        self.connected = False
        await self.hass.async_add_executor_job(self._stop_client)

    def _stop_client(self) -> None:
        self.client.disconnect()
        self.client.loop_stop()
