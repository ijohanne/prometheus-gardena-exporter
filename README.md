# prometheus-gardena-exporter

Prometheus exporter for the [GARDENA smart system API](https://docs.developer.husqvarnagroup.cloud/gardena-smart-system-api/iapi-v2.yml), written in Rust.

## Table of Contents

- [Overview](#overview)
- [Status](#status)
- [Application Setup](#application-setup)
- [Running the Exporter](#running-the-exporter)
- [Finding Valve IDs](#finding-valve-ids)
- [Estimated Water Usage](#estimated-water-usage)
- [Metrics](#metrics)
- [Grafana Dashboard](#grafana-dashboard)
- [NixOS Module](#nixos-module)
- [Development](#development)
- [Notes and Limitations](#notes-and-limitations)
- [License](#license)

## Overview

The exporter:

- fetches OAuth access tokens automatically using the client credentials flow
- renews tokens without an interactive login
- discovers the selected Gardena location
- applies a full snapshot on startup and periodically for reconciliation
- keeps a live WebSocket connection for realtime updates
- exposes the latest device and service state as Prometheus metrics
- models estimated water usage from valve runtime with a default flow rate and optional per-valve overrides

The implemented data flow is:

1. Authenticate with the Husqvarna auth API using the application key and secret.
2. Call `GET /locations` to discover accessible locations.
3. Pick the configured location, or auto-select the only available one.
4. Call `GET /locations/{locationId}` to bootstrap the full device and service state.
5. Create a WebSocket subscription with `POST /websocket`.
6. Apply incoming JSON:API resource updates into an in-memory cache.
7. Periodically reconcile with another snapshot.
8. Expose that cache as Prometheus metrics.

## Status

The originally planned phases are implemented:

- OAuth token flow and application setup are documented
- location and device discovery are implemented
- snapshot and WebSocket sync are implemented
- Prometheus metrics, Grafana dashboard provisioning, reconnect handling, token renewal, and fixture-style tests are implemented

Live API behavior was verified against the Gardena API on April 6, 2026.

## Application Setup

Application creation is a manual developer-portal step rather than something this exporter automates.

1. Create or open an application in the Husqvarna developer portal:
   [developer.husqvarnagroup.cloud/apps](https://developer.husqvarnagroup.cloud/apps)
2. Add a redirect URL such as `http://localhost`.
3. Copy the application key and application secret.

One useful detail from the live API and portal UI: the application key is used both as:

- the `X-Api-Key` header
- the OAuth `client_id`

The application secret is used as the OAuth `client_secret`.

Helper command:

```bash
nix run . -- print-token-curl
```

That prints:

```bash
curl -fsSL -X POST -d "grant_type=client_credentials&client_id=$GARDENA_APPLICATION_KEY&client_secret=$GARDENA_APPLICATION_SECRET" \
  https://api.authentication.husqvarnagroup.dev/v1/oauth2/token
```

To fetch a token directly:

```bash
export GARDENA_APPLICATION_KEY="..."
export GARDENA_APPLICATION_SECRET="..."

nix run . -- fetch-token --raw
```

To discover available locations:

```bash
nix run . -- list-locations \
  --application-key "$GARDENA_APPLICATION_KEY" \
  --application-secret "$GARDENA_APPLICATION_SECRET"
```

If the application has access to only one location, the exporter selects it automatically. If multiple locations are available, pass `--location-id`.

## Running the Exporter

### Nix

```bash
nix develop
cargo build
```

### Non-Nix

```bash
cargo build
```

Run the exporter:

```bash
export GARDENA_APPLICATION_KEY="..."
export GARDENA_APPLICATION_SECRET="..."

cargo run -- serve --validate-auth-on-startup
```

If you have more than one location:

```bash
cargo run -- serve --location-id "<gardena-location-id>"
```

The exporter serves:

- `GET /` for a short status page
- `GET /healthz` for a health check
- `GET /metrics` for Prometheus metrics

## Finding Valve IDs

To map Gardena valve `service_id` values to the names you see in the app, use `list-valves`.

With Nix:

```bash
nix run . -- list-valves \
  --application-key "$GARDENA_APPLICATION_KEY" \
  --application-secret "$GARDENA_APPLICATION_SECRET"
```

Without Nix:

```bash
cargo run -- list-valves \
  --application-key "$GARDENA_APPLICATION_KEY" \
  --application-secret "$GARDENA_APPLICATION_SECRET"
```

If you have more than one Gardena location, pass `--location-id`.

The output is tab-separated:

```text
location_id	location	device_id	controller_name	service_id	valve_name
```

That makes it easy to copy the `service_id` values into `estimatedFlowLitersPerMinuteByValve` on NixOS or into repeated `--valve-estimated-flow-liters-per-minute SERVICE_ID=LPM` flags.

## Estimated Water Usage

The exporter includes a modeled water-usage estimate for watering zones.

- It is derived from valve open time, not from a physical flow meter.
- The built-in default is `3.5 L/min`.
- That default is based on an estimated `5 m3/month` total usage with three `15 minute` watering segments per day.
- The raw conversion is:

```text
5 m3/month = 5000 L/month
45 watering minutes/day x 30 days = 1350 watering minutes/month
5000 / 1350 = 3.7 L/min
```

- The exporter rounds that down slightly and uses `3.5 L/min` as a conservative default.
- This should be treated as a best-effort guess, not as a meter reading.

Override the default modeled flow rate:

```bash
cargo run -- serve --estimated-flow-liters-per-minute 2.8
```

Override individual valves by Gardena `service_id`:

```bash
cargo run -- serve \
  --valve-estimated-flow-liters-per-minute "5f7a3e6e-1111-2222-3333-444444444444=1.2" \
  --valve-estimated-flow-liters-per-minute "8c9d0a1b-5555-6666-7777-888888888888=6.0"
```

The `service_id` is the stable Gardena `VALVE` service identifier and is also exposed in the `service_id` label on `gardena_valve_info`.

This model is most meaningful when:

- only one valve is active at a time
- each zone has a roughly consistent aggregate emitter flow
- water pressure is fairly stable

## Metrics

Current metric families include:

- `gardena_exporter_connected`
- `gardena_exporter_last_event_timestamp_seconds`
- `gardena_exporter_last_snapshot_timestamp_seconds`
- `gardena_exporter_last_successful_sync_timestamp_seconds`
- `gardena_exporter_token_refreshes_total`
- `gardena_exporter_snapshot_refreshes_total`
- `gardena_exporter_websocket_reconnects_total`
- `gardena_exporter_info`
- `gardena_device_info`
- `gardena_sensor_info`
- `gardena_device_battery_level_percent`
- `gardena_device_rf_link_level_percent`
- `gardena_sensor_soil_humidity_percent`
- `gardena_sensor_soil_temperature_celsius`
- `gardena_sensor_ambient_temperature_celsius`
- `gardena_sensor_light_intensity_lux`
- `gardena_valve_info`
- `gardena_valve_open`
- `gardena_valve_duration_seconds`
- `gardena_valve_estimated_open_seconds_total`
- `gardena_valve_estimated_water_liters_total`
- `gardena_valve_estimated_flow_liters_per_minute`
- `gardena_valve_estimated_current_water_flow_liters_per_minute`
- `gardena_estimated_water_liters_total`
- `gardena_estimated_current_water_flow_liters_per_minute`
- `gardena_valve_set_info`

Timestamp companions are also exported for most readings and valve state transitions.

## Grafana Dashboard

The included dashboard focuses on the most useful watering views first:

- exporter connection and active valve count
- average soil humidity and lowest battery quick checks
- soil humidity by zone
- soil temperature by zone
- ambient temperature where available
- light intensity where available
- device battery levels
- RF link levels
- valve status table
- selected-range estimated water usage
- current modeled flow
- cumulative estimated water by zone

The dashboard is provisioned on NixOS only when `enableGrafanaDashboard = true`.

## NixOS Module

This repo includes a flake NixOS module.

Example:

```nix
{
  services.prometheus-gardena-exporter = {
    enable = true;
    enableLocalScraping = true;
    enableGrafanaDashboard = true;
    applicationKeyFile = /run/secrets/gardena-application-key;
    applicationSecretFile = /run/secrets/gardena-application-secret;
    locationId = "db789fe8-2af2-4eaf-a0ef-8ad795617971";
    estimatedFlowLitersPerMinute = 3.5;
    estimatedFlowLitersPerMinuteByValve = {
      "5f7a3e6e-1111-2222-3333-444444444444" = 1.2;
      "8c9d0a1b-5555-6666-7777-888888888888" = 6.0;
    };
    validateAuthOnStartup = true;
    restartSec = "30s";
  };
}
```

The module uses `LoadCredential` so the key and secret are not written into the Nix store.

If you want to derive a custom default from a monthly estimate instead:

```text
liters_per_minute =
  (monthly_cubic_meters * 1000)
  / (watering_minutes_per_day * days_per_month)
```

## Development

The development shell is flake-based and includes Rust plus pre-commit hooks for:

- `cargo fmt --check`
- `cargo clippy --all-targets --all-features -- -D warnings -D clippy::pedantic`

Tracked tests currently cover:

- snapshot parsing for common and sensor services
- modeled valve usage with per-valve flow overrides

Useful commands:

```bash
cargo fmt
cargo test
cargo clippy --all-targets --all-features -- -D warnings -D clippy::pedantic
nix build
```

## Notes and Limitations

- Snapshot endpoints are rate-limited, so the exporter serves cached data and does not poll on every scrape.
- `POST /websocket` currently returns a very short-lived URL, so the exporter connects immediately after requesting it.
- Some Gardena sensors report only `soilHumidity` and `soilTemperature`; `ambientTemperature` and `lightIntensity` are optional.
- Estimated water totals are kept in memory and reset when the exporter restarts.

## License

MIT. See [LICENSE](./LICENSE).
