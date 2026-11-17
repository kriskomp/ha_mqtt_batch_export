# MQTT Batch Export

Home Assistant custom integration: one-way export of selected `sensor.*` entities to a remote MQTT broker (e.g. a second HA instance). State is sent 1:1 to `<alias>/<entity_id>` (retained), with MQTT Discovery so the entities appear on the receiving HA automatically. Entities are grouped; each group has its own interval and only changed values are sent. `unknown`/`unavailable` are not sent.

## Setup

`configuration.yaml`:
```yaml
mqtt_batch_export:
```

`/config/mqtt_batch_export.yaml`:
```yaml
alias: mha                       # source name, topic prefix and device name on the receiving HA
broker: {host: 100.x.x.x, port: 1883, username: mha, password: !secret mqtt_password, tls: false}
device:                          # optional, how this source appears as a device on the receiving HA
  model: HA Magnuszew            # description line of the device ("model"); default "HA <alias>"
  manufacturer: MQTT Batch Export  # default
  configuration_url: https://example.org  # optional link to the source HA
groups:
  pstryk: {interval: 60, entities: [sensor.pstryk_total_buy_cost]}
  inverter: {interval: 30, entities: [sensor.solax_...]}
```
`sw_version` of the device is the integration version. Restart HA after changing the file.

Not yet: other domains than `sensor`, attributes, availability/heartbeat, tests.
