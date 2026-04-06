use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use prometheus::{
    core::Collector, Counter, CounterVec, Encoder, Gauge, GaugeVec, IntCounter, IntGauge, Opts,
    Registry, TextEncoder,
};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::RwLock;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

pub const DEFAULT_AUTH_URL: &str = "https://api.authentication.husqvarnagroup.dev/v1";
pub const DEFAULT_API_URL: &str = "https://api.smart.gardena.dev/v2";
pub const DEFAULT_ESTIMATED_FLOW_LITERS_PER_MINUTE: f64 = 3.5;

#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub application_key: String,
    pub application_secret: String,
    pub auth_url: String,
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub auth: AuthConfig,
    pub api_url: String,
    pub location_id: Option<String>,
    pub snapshot_interval: Duration,
    pub reconnect_delay: Duration,
    pub max_reconnect_delay: Duration,
    pub estimated_flow_liters_per_minute: Option<f64>,
    pub valve_estimated_flow_liters_per_minute: BTreeMap<String, f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    #[serde(default)]
    pub token_type: Option<String>,
    #[serde(default)]
    pub expires_in: Option<u64>,
    #[serde(default)]
    pub scope: Option<String>,
}

#[derive(Debug, Clone)]
struct ActiveToken {
    access_token: String,
    expires_at: Instant,
}

impl ActiveToken {
    fn from_response(response: TokenResponse) -> Self {
        let expires_in = response.expires_in.unwrap_or(3600);
        let refresh_margin = expires_in.min(60);
        Self {
            access_token: response.access_token,
            expires_at: Instant::now()
                + Duration::from_secs(expires_in.saturating_sub(refresh_margin)),
        }
    }

    fn needs_refresh(&self) -> bool {
        Instant::now() >= self.expires_at
    }
}

#[derive(Debug, Clone, Deserialize)]
struct LocationListResponse {
    data: Vec<LocationResource>,
}

#[derive(Debug, Clone, Deserialize)]
struct LocationResource {
    id: String,
    #[serde(rename = "type")]
    kind: String,
    attributes: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
struct SnapshotResponse {
    data: JsonApiResource,
    #[serde(default)]
    included: Vec<JsonApiResource>,
}

#[derive(Debug, Clone, Deserialize)]
struct WebSocketCreatedResponse {
    data: JsonApiResource,
}

#[derive(Debug, Clone, Deserialize)]
pub struct JsonApiResource {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub attributes: Option<Value>,
    #[serde(default)]
    pub relationships: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct LocationSummary {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, Default)]
pub struct ExporterState {
    pub location_id: Option<String>,
    pub location_name: Option<String>,
    pub devices: BTreeMap<String, DeviceState>,
    pub connected: bool,
    pub last_event_timestamp: Option<f64>,
    pub last_snapshot_timestamp: Option<f64>,
    pub last_successful_sync_timestamp: Option<f64>,
    pub websocket_reconnects_total: u64,
    pub token_refreshes_total: u64,
    pub snapshot_refreshes_total: u64,
    pub last_error: Option<String>,
    pub valve_usage: BTreeMap<String, ValveUsageState>,
}

#[derive(Debug, Clone, Default)]
pub struct DeviceState {
    pub id: String,
    pub location_id: Option<String>,
    pub common: Option<CommonServiceState>,
    pub sensor: Option<SensorServiceState>,
    pub valves: BTreeMap<String, ValveServiceState>,
    pub valve_sets: BTreeMap<String, ValveSetServiceState>,
}

#[derive(Debug, Clone, Default)]
pub struct CommonServiceState {
    pub name: Option<String>,
    pub battery_level: Option<f64>,
    pub battery_level_timestamp: Option<f64>,
    pub battery_state: Option<String>,
    pub rf_link_level: Option<f64>,
    pub rf_link_level_timestamp: Option<f64>,
    pub serial: Option<String>,
    pub model_type: Option<String>,
    pub rf_link_state: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct SensorServiceState {
    pub soil_humidity: Option<f64>,
    pub soil_humidity_timestamp: Option<f64>,
    pub soil_temperature: Option<f64>,
    pub soil_temperature_timestamp: Option<f64>,
    pub ambient_temperature: Option<f64>,
    pub ambient_temperature_timestamp: Option<f64>,
    pub light_intensity: Option<f64>,
    pub light_intensity_timestamp: Option<f64>,
}

#[derive(Debug, Clone, Default)]
pub struct ValveServiceState {
    pub id: String,
    pub name: Option<String>,
    pub activity: Option<String>,
    pub activity_timestamp: Option<f64>,
    pub state: Option<String>,
    pub state_timestamp: Option<f64>,
    pub last_error: Option<String>,
    pub duration_seconds: Option<f64>,
}

#[derive(Debug, Clone, Default)]
pub struct ValveSetServiceState {
    pub id: String,
    pub state: Option<String>,
    pub state_timestamp: Option<f64>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ValveUsageState {
    pub total_open_seconds: f64,
    pub total_estimated_liters: f64,
    pub open_since_timestamp: Option<f64>,
    pub estimated_flow_liters_per_minute: Option<f64>,
    pub currently_open: bool,
}

#[derive(Debug, Clone, Copy, Default)]
struct ValveUsageTotals {
    open_seconds: f64,
    estimated_liters: f64,
    flow_liters_per_minute: f64,
    current_flow_liters_per_minute: f64,
}

pub type SharedState = Arc<RwLock<ExporterState>>;

pub async fn fetch_token(client: &Client, auth: &AuthConfig) -> Result<TokenResponse> {
    let token_url = format!("{}/oauth2/token", auth.auth_url.trim_end_matches('/'));
    let response = client
        .post(&token_url)
        .header("content-type", "application/x-www-form-urlencoded")
        .form(&[
            ("grant_type", "client_credentials"),
            ("client_id", auth.application_key.as_str()),
            ("client_secret", auth.application_secret.as_str()),
        ])
        .send()
        .await
        .with_context(|| format!("failed to call {token_url}"))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .context("failed to read token response")?;

    if !status.is_success() {
        bail!("token request failed with status {status}: {body}");
    }

    serde_json::from_str(&body).context("failed to decode token response JSON")
}

pub async fn list_locations(client: &Client, auth: &AuthConfig) -> Result<Vec<LocationSummary>> {
    let token = ActiveToken::from_response(fetch_token(client, auth).await?);
    let locations = list_locations_with_token(client, auth, &token)
        .await?
        .ok_or_else(|| anyhow!("listing locations unexpectedly returned unauthorized"))?;
    Ok(locations)
}

pub async fn validate_startup(
    client: &Client,
    config: &RuntimeConfig,
    shared_state: &SharedState,
) -> Result<()> {
    let mut token = refresh_active_token(client, &config.auth, shared_state).await?;
    let locations = list_locations_with_token(client, &config.auth, &token)
        .await?
        .ok_or_else(|| {
            anyhow!(
                "listing locations unexpectedly returned unauthorized during startup validation"
            )
        })?;
    let selected = select_location(&locations, config.location_id.as_deref())?;
    let snapshot = fetch_snapshot_with_token(client, config, &token, &selected.id)
        .await?
        .ok_or_else(|| {
            anyhow!("snapshot unexpectedly returned unauthorized during startup validation")
        })?;
    {
        let mut state = shared_state.write().await;
        state.apply_snapshot(
            &selected,
            &snapshot,
            config.estimated_flow_liters_per_minute,
            &config.valve_estimated_flow_liters_per_minute,
        );
        state.connected = false;
    }
    if token.needs_refresh() {
        token = refresh_active_token(client, &config.auth, shared_state).await?;
    }
    let websocket_url = create_websocket_url_with_token(client, config, &token, &selected.id)
        .await?
        .ok_or_else(|| {
            anyhow!(
                "websocket bootstrap unexpectedly returned unauthorized during startup validation"
            )
        })?;
    info!(
        location_id = %selected.id,
        location_name = %selected.name,
        websocket_url_present = !websocket_url.is_empty(),
        "validated Gardena startup configuration"
    );
    Ok(())
}

pub async fn run_sync_loop(shared_state: SharedState, client: Client, config: RuntimeConfig) {
    let mut backoff = config.reconnect_delay;
    loop {
        match sync_once(&shared_state, &client, &config).await {
            Ok(()) => {
                backoff = config.reconnect_delay;
            }
            Err(error) => {
                {
                    let mut state = shared_state.write().await;
                    state.connected = false;
                    state.last_error = Some(format!("{error:#}"));
                    state.websocket_reconnects_total =
                        state.websocket_reconnects_total.saturating_add(1);
                }
                warn!(error = ?error, delay_seconds = backoff.as_secs(), "Gardena sync loop failed; retrying");
                tokio::time::sleep(backoff).await;
                backoff = (backoff.saturating_mul(2)).min(config.max_reconnect_delay);
            }
        }
    }
}

async fn sync_once(
    shared_state: &SharedState,
    client: &Client,
    config: &RuntimeConfig,
) -> Result<()> {
    let mut token = refresh_active_token(client, &config.auth, shared_state).await?;
    let locations = ensure_locations(client, &config.auth, &mut token, shared_state).await?;
    let selected = select_location(&locations, config.location_id.as_deref())?;
    ensure_snapshot(client, config, &selected, &mut token, shared_state).await?;
    let websocket_url =
        ensure_websocket_url(client, config, &selected, &mut token, shared_state).await?;

    info!(
        location_id = %selected.id,
        location_name = %selected.name,
        "connecting to Gardena websocket"
    );

    let (mut stream, _) = connect_async(&websocket_url)
        .await
        .context("failed to connect to Gardena websocket")?;

    {
        let mut state = shared_state.write().await;
        state.location_id = Some(selected.id.clone());
        state.location_name = Some(selected.name.clone());
        state.connected = true;
        state.last_error = None;
    }

    let mut snapshot_interval = tokio::time::interval(config.snapshot_interval);
    snapshot_interval.tick().await;

    loop {
        tokio::select! {
            _ = snapshot_interval.tick() => {
                ensure_snapshot(client, config, &selected, &mut token, shared_state).await?;
            }
            message = stream.next() => {
                match message {
                    Some(Ok(Message::Text(text))) => {
                        let resource: JsonApiResource = serde_json::from_str(&text)
                            .context("failed to parse websocket JSON resource")?;
                        let mut state = shared_state.write().await;
                        state.apply_resource(
                            &resource,
                            config.estimated_flow_liters_per_minute,
                            &config.valve_estimated_flow_liters_per_minute,
                        );
                        state.connected = true;
                        state.last_event_timestamp = Some(now_timestamp());
                        state.last_successful_sync_timestamp = Some(now_timestamp());
                        state.last_error = None;
                    }
                    Some(Ok(Message::Close(frame))) => {
                        let detail = frame.map_or_else(
                            || "no close frame".to_string(),
                            |close_frame| format!("code={} reason={}", close_frame.code, close_frame.reason),
                        );
                        bail!("Gardena websocket closed: {detail}");
                    }
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => {
                        debug!("received Gardena websocket ping/pong");
                    }
                    Some(Ok(Message::Binary(_))) => {
                        debug!("received unexpected binary Gardena websocket frame");
                    }
                    Some(Ok(Message::Frame(_))) => {
                        debug!("received raw websocket frame");
                    }
                    Some(Err(error)) => {
                        bail!("Gardena websocket error: {error}");
                    }
                    None => {
                        bail!("Gardena websocket ended unexpectedly");
                    }
                }
            }
        }
    }
}

async fn ensure_locations(
    client: &Client,
    auth: &AuthConfig,
    token: &mut ActiveToken,
    shared_state: &SharedState,
) -> Result<Vec<LocationSummary>> {
    if token.needs_refresh() {
        *token = refresh_active_token(client, auth, shared_state).await?;
    }

    if let Some(locations) = list_locations_with_token(client, auth, token).await? {
        Ok(locations)
    } else {
        *token = refresh_active_token(client, auth, shared_state).await?;
        list_locations_with_token(client, auth, token)
            .await?
            .ok_or_else(|| {
                anyhow!("listing locations still returned unauthorized after refreshing token")
            })
    }
}

async fn ensure_snapshot(
    client: &Client,
    config: &RuntimeConfig,
    selected: &LocationSummary,
    token: &mut ActiveToken,
    shared_state: &SharedState,
) -> Result<()> {
    if token.needs_refresh() {
        *token = refresh_active_token(client, &config.auth, shared_state).await?;
    }

    let snapshot = if let Some(snapshot) =
        fetch_snapshot_with_token(client, config, token, &selected.id).await?
    {
        snapshot
    } else {
        *token = refresh_active_token(client, &config.auth, shared_state).await?;
        fetch_snapshot_with_token(client, config, token, &selected.id)
            .await?
            .ok_or_else(|| anyhow!("snapshot still returned unauthorized after refreshing token"))?
    };

    let mut state = shared_state.write().await;
    state.apply_snapshot(
        selected,
        &snapshot,
        config.estimated_flow_liters_per_minute,
        &config.valve_estimated_flow_liters_per_minute,
    );
    state.snapshot_refreshes_total = state.snapshot_refreshes_total.saturating_add(1);
    state.connected = true;
    state.last_error = None;
    Ok(())
}

async fn ensure_websocket_url(
    client: &Client,
    config: &RuntimeConfig,
    selected: &LocationSummary,
    token: &mut ActiveToken,
    shared_state: &SharedState,
) -> Result<String> {
    if token.needs_refresh() {
        *token = refresh_active_token(client, &config.auth, shared_state).await?;
    }

    if let Some(url) = create_websocket_url_with_token(client, config, token, &selected.id).await? {
        Ok(url)
    } else {
        *token = refresh_active_token(client, &config.auth, shared_state).await?;
        create_websocket_url_with_token(client, config, token, &selected.id)
            .await?
            .ok_or_else(|| {
                anyhow!("websocket bootstrap still returned unauthorized after refreshing token")
            })
    }
}

async fn refresh_active_token(
    client: &Client,
    auth: &AuthConfig,
    shared_state: &SharedState,
) -> Result<ActiveToken> {
    let response = fetch_token(client, auth).await?;
    let token = ActiveToken::from_response(response);
    let mut state = shared_state.write().await;
    state.token_refreshes_total = state.token_refreshes_total.saturating_add(1);
    state.last_error = None;
    Ok(token)
}

async fn list_locations_with_token(
    client: &Client,
    auth: &AuthConfig,
    token: &ActiveToken,
) -> Result<Option<Vec<LocationSummary>>> {
    let url = format!("{DEFAULT_API_URL}/locations");
    let response = client
        .get(&url)
        .header("X-Api-Key", &auth.application_key)
        .header("Authorization", format!("Bearer {}", token.access_token))
        .header("Accept", "application/vnd.api+json")
        .send()
        .await
        .with_context(|| format!("failed to call {url}"))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .context("failed to read location list response body")?;

    if status == StatusCode::UNAUTHORIZED {
        return Ok(None);
    }
    if !status.is_success() {
        bail!("listing locations failed with status {status}: {body}");
    }

    let parsed: LocationListResponse =
        serde_json::from_str(&body).context("failed to decode locations response")?;
    Ok(Some(
        parsed
            .data
            .into_iter()
            .filter(|resource| resource.kind == "LOCATION")
            .map(|resource| LocationSummary {
                name: string_attr(resource.attributes.as_ref(), "name")
                    .unwrap_or_else(|| resource.id.clone()),
                id: resource.id,
            })
            .collect(),
    ))
}

async fn fetch_snapshot_with_token(
    client: &Client,
    config: &RuntimeConfig,
    token: &ActiveToken,
    location_id: &str,
) -> Result<Option<SnapshotResponse>> {
    let url = format!(
        "{}/locations/{location_id}",
        config.api_url.trim_end_matches('/')
    );
    let response = client
        .get(&url)
        .header("X-Api-Key", &config.auth.application_key)
        .header("Authorization", format!("Bearer {}", token.access_token))
        .header("Accept", "application/vnd.api+json")
        .send()
        .await
        .with_context(|| format!("failed to call {url}"))?;

    decode_optional_json_api_response(response, "location snapshot").await
}

async fn create_websocket_url_with_token(
    client: &Client,
    config: &RuntimeConfig,
    token: &ActiveToken,
    location_id: &str,
) -> Result<Option<String>> {
    let url = format!("{}/websocket", config.api_url.trim_end_matches('/'));
    let response = client
        .post(&url)
        .header("X-Api-Key", &config.auth.application_key)
        .header("Authorization", format!("Bearer {}", token.access_token))
        .header("Accept", "application/vnd.api+json")
        .header("Content-Type", "application/vnd.api+json")
        .json(&serde_json::json!({
            "data": {
                "id": "prometheus-gardena-exporter",
                "type": "WEBSOCKET",
                "attributes": {
                    "locationId": location_id,
                }
            }
        }))
        .send()
        .await
        .with_context(|| format!("failed to call {url}"))?;

    let parsed: Option<WebSocketCreatedResponse> =
        decode_optional_json_api_response(response, "websocket bootstrap").await?;
    Ok(parsed.and_then(|payload| string_attr(payload.data.attributes.as_ref(), "url")))
}

async fn decode_optional_json_api_response<T>(
    response: reqwest::Response,
    context_name: &str,
) -> Result<Option<T>>
where
    T: for<'de> Deserialize<'de>,
{
    let status = response.status();
    let body = response
        .text()
        .await
        .with_context(|| format!("failed to read {context_name} body"))?;

    if status == StatusCode::UNAUTHORIZED {
        return Ok(None);
    }
    if !status.is_success() {
        bail!("{context_name} failed with status {status}: {body}");
    }

    let parsed = serde_json::from_str(&body)
        .with_context(|| format!("failed to decode {context_name} JSON"))?;
    Ok(Some(parsed))
}

fn select_location(
    locations: &[LocationSummary],
    configured_location_id: Option<&str>,
) -> Result<LocationSummary> {
    if let Some(location_id) = configured_location_id {
        return locations
            .iter()
            .find(|location| location.id == location_id)
            .cloned()
            .ok_or_else(|| anyhow!("configured location id {location_id} was not found"));
    }

    match locations {
        [] => bail!("no Gardena locations are available for this application"),
        [location] => Ok(location.clone()),
        _ => {
            let choices = locations
                .iter()
                .map(|location| format!("{} ({})", location.name, location.id))
                .collect::<Vec<_>>()
                .join(", ");
            bail!(
                "multiple Gardena locations are available; set --location-id to one of: {choices}"
            );
        }
    }
}

impl ExporterState {
    fn apply_snapshot(
        &mut self,
        selected: &LocationSummary,
        snapshot: &SnapshotResponse,
        default_estimated_flow_liters_per_minute: Option<f64>,
        valve_estimated_flow_liters_per_minute: &BTreeMap<String, f64>,
    ) {
        self.location_id = Some(selected.id.clone());
        self.location_name = Some(selected.name.clone());
        self.devices.clear();
        self.apply_resource(
            &snapshot.data,
            default_estimated_flow_liters_per_minute,
            valve_estimated_flow_liters_per_minute,
        );
        for resource in &snapshot.included {
            self.apply_resource(
                resource,
                default_estimated_flow_liters_per_minute,
                valve_estimated_flow_liters_per_minute,
            );
        }
        let timestamp = now_timestamp();
        self.last_snapshot_timestamp = Some(timestamp);
        self.last_successful_sync_timestamp = Some(timestamp);
        self.connected = true;
    }

    #[allow(clippy::too_many_lines)]
    fn apply_resource(
        &mut self,
        resource: &JsonApiResource,
        default_estimated_flow_liters_per_minute: Option<f64>,
        valve_estimated_flow_liters_per_minute: &BTreeMap<String, f64>,
    ) {
        match resource.kind.as_str() {
            "LOCATION" => {
                self.location_id = Some(resource.id.clone());
                self.location_name = string_attr(resource.attributes.as_ref(), "name");
            }
            "DEVICE" => {
                let device = self.device_mut(&resource.id);
                device.location_id =
                    relationship_one_id(resource.relationships.as_ref(), "location");
            }
            "COMMON" => {
                if let Some(device_id) =
                    relationship_one_id(resource.relationships.as_ref(), "device")
                {
                    let device = self.device_mut(&device_id);
                    device.common = Some(CommonServiceState {
                        name: string_nested_attr(resource.attributes.as_ref(), "name", "value"),
                        battery_level: number_nested_attr(
                            resource.attributes.as_ref(),
                            "batteryLevel",
                            "value",
                        ),
                        battery_level_timestamp: timestamp_nested_attr(
                            resource.attributes.as_ref(),
                            "batteryLevel",
                            "timestamp",
                        ),
                        battery_state: string_nested_attr(
                            resource.attributes.as_ref(),
                            "batteryState",
                            "value",
                        ),
                        rf_link_level: number_nested_attr(
                            resource.attributes.as_ref(),
                            "rfLinkLevel",
                            "value",
                        ),
                        rf_link_level_timestamp: timestamp_nested_attr(
                            resource.attributes.as_ref(),
                            "rfLinkLevel",
                            "timestamp",
                        ),
                        serial: string_nested_attr(resource.attributes.as_ref(), "serial", "value"),
                        model_type: string_nested_attr(
                            resource.attributes.as_ref(),
                            "modelType",
                            "value",
                        ),
                        rf_link_state: string_nested_attr(
                            resource.attributes.as_ref(),
                            "rfLinkState",
                            "value",
                        ),
                    });
                }
            }
            "SENSOR" => {
                if let Some(device_id) =
                    relationship_one_id(resource.relationships.as_ref(), "device")
                {
                    let device = self.device_mut(&device_id);
                    device.sensor = Some(SensorServiceState {
                        soil_humidity: number_nested_attr(
                            resource.attributes.as_ref(),
                            "soilHumidity",
                            "value",
                        ),
                        soil_humidity_timestamp: timestamp_nested_attr(
                            resource.attributes.as_ref(),
                            "soilHumidity",
                            "timestamp",
                        ),
                        soil_temperature: number_nested_attr(
                            resource.attributes.as_ref(),
                            "soilTemperature",
                            "value",
                        ),
                        soil_temperature_timestamp: timestamp_nested_attr(
                            resource.attributes.as_ref(),
                            "soilTemperature",
                            "timestamp",
                        ),
                        ambient_temperature: number_nested_attr(
                            resource.attributes.as_ref(),
                            "ambientTemperature",
                            "value",
                        ),
                        ambient_temperature_timestamp: timestamp_nested_attr(
                            resource.attributes.as_ref(),
                            "ambientTemperature",
                            "timestamp",
                        ),
                        light_intensity: number_nested_attr(
                            resource.attributes.as_ref(),
                            "lightIntensity",
                            "value",
                        ),
                        light_intensity_timestamp: timestamp_nested_attr(
                            resource.attributes.as_ref(),
                            "lightIntensity",
                            "timestamp",
                        ),
                    });
                }
            }
            "VALVE" => {
                if let Some(device_id) =
                    relationship_one_id(resource.relationships.as_ref(), "device")
                {
                    self.update_valve_usage(
                        resource,
                        resolve_valve_estimated_flow_liters_per_minute(
                            &resource.id,
                            default_estimated_flow_liters_per_minute,
                            valve_estimated_flow_liters_per_minute,
                        ),
                    );
                    let device = self.device_mut(&device_id);
                    device.valves.insert(
                        resource.id.clone(),
                        ValveServiceState {
                            id: resource.id.clone(),
                            name: string_nested_attr(resource.attributes.as_ref(), "name", "value"),
                            activity: string_nested_attr(
                                resource.attributes.as_ref(),
                                "activity",
                                "value",
                            ),
                            activity_timestamp: timestamp_nested_attr(
                                resource.attributes.as_ref(),
                                "activity",
                                "timestamp",
                            ),
                            state: string_nested_attr(
                                resource.attributes.as_ref(),
                                "state",
                                "value",
                            ),
                            state_timestamp: timestamp_nested_attr(
                                resource.attributes.as_ref(),
                                "state",
                                "timestamp",
                            ),
                            last_error: string_nested_attr(
                                resource.attributes.as_ref(),
                                "lastErrorCode",
                                "value",
                            ),
                            duration_seconds: number_nested_attr(
                                resource.attributes.as_ref(),
                                "duration",
                                "value",
                            ),
                        },
                    );
                }
            }
            "VALVE_SET" => {
                if let Some(device_id) =
                    relationship_one_id(resource.relationships.as_ref(), "device")
                {
                    let device = self.device_mut(&device_id);
                    device.valve_sets.insert(
                        resource.id.clone(),
                        ValveSetServiceState {
                            id: resource.id.clone(),
                            state: string_nested_attr(
                                resource.attributes.as_ref(),
                                "state",
                                "value",
                            ),
                            state_timestamp: timestamp_nested_attr(
                                resource.attributes.as_ref(),
                                "state",
                                "timestamp",
                            ),
                            last_error: string_nested_attr(
                                resource.attributes.as_ref(),
                                "lastErrorCode",
                                "value",
                            ),
                        },
                    );
                }
            }
            _ => {}
        }
    }

    fn device_mut(&mut self, device_id: &str) -> &mut DeviceState {
        self.devices
            .entry(device_id.to_string())
            .or_insert_with(|| DeviceState {
                id: device_id.to_string(),
                ..DeviceState::default()
            })
    }

    fn update_valve_usage(
        &mut self,
        resource: &JsonApiResource,
        estimated_flow_liters_per_minute: Option<f64>,
    ) {
        let is_open = string_nested_attr(resource.attributes.as_ref(), "activity", "value")
            .is_some_and(|activity| activity != "CLOSED");
        let transition_timestamp =
            timestamp_nested_attr(resource.attributes.as_ref(), "activity", "timestamp")
                .or_else(|| {
                    timestamp_nested_attr(resource.attributes.as_ref(), "state", "timestamp")
                })
                .unwrap_or_else(now_timestamp);

        let usage = self
            .valve_usage
            .entry(resource.id.clone())
            .or_insert_with(|| ValveUsageState {
                estimated_flow_liters_per_minute,
                ..ValveUsageState::default()
            });

        usage.estimated_flow_liters_per_minute = estimated_flow_liters_per_minute;

        match (usage.currently_open, is_open) {
            (false, true) => {
                usage.currently_open = true;
                usage.open_since_timestamp = Some(transition_timestamp);
            }
            (true, false) => {
                if let Some(open_since) = usage.open_since_timestamp {
                    let elapsed_seconds = (transition_timestamp - open_since).max(0.0);
                    usage.total_open_seconds += elapsed_seconds;
                    if let Some(flow_rate) = usage.estimated_flow_liters_per_minute {
                        usage.total_estimated_liters += elapsed_seconds / 60.0 * flow_rate;
                    }
                }
                usage.currently_open = false;
                usage.open_since_timestamp = None;
            }
            (true, true) | (false, false) => {}
        }
    }

    fn valve_usage_totals(&self, valve_id: &str) -> ValveUsageTotals {
        let Some(usage) = self.valve_usage.get(valve_id) else {
            return ValveUsageTotals::default();
        };

        let mut open_seconds = usage.total_open_seconds;
        let mut estimated_liters = usage.total_estimated_liters;
        let flow_liters_per_minute = usage.estimated_flow_liters_per_minute.unwrap_or_default();
        let current_flow_liters_per_minute = if usage.currently_open {
            flow_liters_per_minute
        } else {
            0.0
        };

        if usage.currently_open {
            if let Some(open_since_timestamp) = usage.open_since_timestamp {
                let elapsed_seconds = (now_timestamp() - open_since_timestamp).max(0.0);
                open_seconds += elapsed_seconds;
                estimated_liters += elapsed_seconds / 60.0 * flow_liters_per_minute;
            }
        }

        ValveUsageTotals {
            open_seconds,
            estimated_liters,
            flow_liters_per_minute,
            current_flow_liters_per_minute,
        }
    }
}

#[allow(clippy::too_many_lines)]
pub async fn render_metrics(shared_state: &SharedState) -> Result<String> {
    let state = shared_state.read().await.clone();
    let registry = Registry::new();

    let exporter_connected = IntGauge::new(
        "gardena_exporter_connected",
        "Whether the exporter currently has an active Gardena websocket connection",
    )?;
    let last_event_timestamp = Gauge::new(
        "gardena_exporter_last_event_timestamp_seconds",
        "Unix timestamp of the last Gardena websocket event processed by the exporter",
    )?;
    let last_snapshot_timestamp = Gauge::new(
        "gardena_exporter_last_snapshot_timestamp_seconds",
        "Unix timestamp of the last full Gardena snapshot applied by the exporter",
    )?;
    let last_successful_sync_timestamp = Gauge::new(
        "gardena_exporter_last_successful_sync_timestamp_seconds",
        "Unix timestamp of the last successful Gardena sync activity",
    )?;
    let websocket_reconnects_total = IntCounter::new(
        "gardena_exporter_websocket_reconnects_total",
        "Number of Gardena websocket reconnect attempts triggered by the exporter",
    )?;
    let token_refreshes_total = IntCounter::new(
        "gardena_exporter_token_refreshes_total",
        "Number of OAuth access tokens minted by the exporter",
    )?;
    let snapshot_refreshes_total = IntCounter::new(
        "gardena_exporter_snapshot_refreshes_total",
        "Number of full Gardena snapshot refreshes applied by the exporter",
    )?;
    let exporter_info = GaugeVec::new(
        Opts::new(
            "gardena_exporter_info",
            "Static information about the Gardena exporter",
        ),
        &["version", "location_id", "location"],
    )?;
    let device_info = GaugeVec::new(
        Opts::new(
            "gardena_device_info",
            "Static information about Gardena devices",
        ),
        &[
            "location",
            "device_id",
            "device_name",
            "model_type",
            "serial",
            "battery_state",
            "rf_link_state",
        ],
    )?;
    let sensor_info = GaugeVec::new(
        Opts::new(
            "gardena_sensor_info",
            "Static information about Gardena sensor devices",
        ),
        &[
            "location",
            "device_id",
            "device_name",
            "model_type",
            "serial",
            "battery_state",
            "rf_link_state",
        ],
    )?;
    let valve_info = GaugeVec::new(
        Opts::new(
            "gardena_valve_info",
            "Static information about Gardena valves",
        ),
        &[
            "location",
            "device_id",
            "controller_name",
            "service_id",
            "valve_name",
            "state",
            "activity",
            "last_error",
        ],
    )?;
    let valve_set_info = GaugeVec::new(
        Opts::new(
            "gardena_valve_set_info",
            "Static information about Gardena valve sets",
        ),
        &[
            "location",
            "device_id",
            "controller_name",
            "service_id",
            "state",
            "last_error",
        ],
    )?;
    let battery_level = GaugeVec::new(
        Opts::new(
            "gardena_device_battery_level_percent",
            "Device battery level percentage",
        ),
        &["location", "device_id", "device_name", "model_type"],
    )?;
    let battery_level_timestamp = GaugeVec::new(
        Opts::new(
            "gardena_device_battery_level_timestamp_seconds",
            "Unix timestamp of the latest device battery level reading",
        ),
        &["location", "device_id", "device_name", "model_type"],
    )?;
    let rf_link_level = GaugeVec::new(
        Opts::new(
            "gardena_device_rf_link_level_percent",
            "Device RF link level percentage",
        ),
        &["location", "device_id", "device_name", "model_type"],
    )?;
    let rf_link_level_timestamp = GaugeVec::new(
        Opts::new(
            "gardena_device_rf_link_level_timestamp_seconds",
            "Unix timestamp of the latest device RF link level reading",
        ),
        &["location", "device_id", "device_name", "model_type"],
    )?;
    let soil_humidity = GaugeVec::new(
        Opts::new(
            "gardena_sensor_soil_humidity_percent",
            "Soil humidity percentage",
        ),
        &["location", "device_id", "device_name", "model_type"],
    )?;
    let soil_humidity_timestamp = GaugeVec::new(
        Opts::new(
            "gardena_sensor_soil_humidity_timestamp_seconds",
            "Unix timestamp of the latest soil humidity reading",
        ),
        &["location", "device_id", "device_name", "model_type"],
    )?;
    let soil_temperature = GaugeVec::new(
        Opts::new(
            "gardena_sensor_soil_temperature_celsius",
            "Soil temperature in Celsius",
        ),
        &["location", "device_id", "device_name", "model_type"],
    )?;
    let soil_temperature_timestamp = GaugeVec::new(
        Opts::new(
            "gardena_sensor_soil_temperature_timestamp_seconds",
            "Unix timestamp of the latest soil temperature reading",
        ),
        &["location", "device_id", "device_name", "model_type"],
    )?;
    let ambient_temperature = GaugeVec::new(
        Opts::new(
            "gardena_sensor_ambient_temperature_celsius",
            "Ambient temperature in Celsius",
        ),
        &["location", "device_id", "device_name", "model_type"],
    )?;
    let ambient_temperature_timestamp = GaugeVec::new(
        Opts::new(
            "gardena_sensor_ambient_temperature_timestamp_seconds",
            "Unix timestamp of the latest ambient temperature reading",
        ),
        &["location", "device_id", "device_name", "model_type"],
    )?;
    let light_intensity = GaugeVec::new(
        Opts::new(
            "gardena_sensor_light_intensity_lux",
            "Light intensity in lux",
        ),
        &["location", "device_id", "device_name", "model_type"],
    )?;
    let light_intensity_timestamp = GaugeVec::new(
        Opts::new(
            "gardena_sensor_light_intensity_timestamp_seconds",
            "Unix timestamp of the latest light intensity reading",
        ),
        &["location", "device_id", "device_name", "model_type"],
    )?;
    let valve_open = GaugeVec::new(
        Opts::new(
            "gardena_valve_open",
            "Whether a Gardena valve is currently active and not closed",
        ),
        &[
            "location",
            "device_id",
            "controller_name",
            "service_id",
            "valve_name",
        ],
    )?;
    let valve_duration_seconds = GaugeVec::new(
        Opts::new(
            "gardena_valve_duration_seconds",
            "Configured or active valve duration in seconds",
        ),
        &[
            "location",
            "device_id",
            "controller_name",
            "service_id",
            "valve_name",
        ],
    )?;
    let valve_activity_timestamp = GaugeVec::new(
        Opts::new(
            "gardena_valve_activity_timestamp_seconds",
            "Unix timestamp of the latest valve activity update",
        ),
        &[
            "location",
            "device_id",
            "controller_name",
            "service_id",
            "valve_name",
        ],
    )?;
    let valve_state_timestamp = GaugeVec::new(
        Opts::new(
            "gardena_valve_state_timestamp_seconds",
            "Unix timestamp of the latest valve state update",
        ),
        &[
            "location",
            "device_id",
            "controller_name",
            "service_id",
            "valve_name",
        ],
    )?;
    let valve_set_state_timestamp = GaugeVec::new(
        Opts::new(
            "gardena_valve_set_state_timestamp_seconds",
            "Unix timestamp of the latest valve set state update",
        ),
        &["location", "device_id", "controller_name", "service_id"],
    )?;
    let valve_estimated_open_seconds_total = CounterVec::new(
        Opts::new(
            "gardena_valve_estimated_open_seconds_total",
            "Modeled valve open time in seconds accumulated since exporter start",
        ),
        &[
            "location",
            "device_id",
            "controller_name",
            "service_id",
            "valve_name",
        ],
    )?;
    let valve_estimated_water_liters_total = CounterVec::new(
        Opts::new(
            "gardena_valve_estimated_water_liters_total",
            "Modeled water volume in liters accumulated since exporter start",
        ),
        &[
            "location",
            "device_id",
            "controller_name",
            "service_id",
            "valve_name",
        ],
    )?;
    let valve_estimated_flow_liters_per_minute = GaugeVec::new(
        Opts::new(
            "gardena_valve_estimated_flow_liters_per_minute",
            "Configured modeled flow rate used for valve water estimation",
        ),
        &[
            "location",
            "device_id",
            "controller_name",
            "service_id",
            "valve_name",
        ],
    )?;
    let valve_estimated_current_water_flow_liters_per_minute = GaugeVec::new(
        Opts::new(
            "gardena_valve_estimated_current_water_flow_liters_per_minute",
            "Modeled current water flow rate in liters per minute for each valve",
        ),
        &[
            "location",
            "device_id",
            "controller_name",
            "service_id",
            "valve_name",
        ],
    )?;
    let estimated_water_liters_total = Counter::new(
        "gardena_estimated_water_liters_total",
        "Modeled total water volume in liters accumulated since exporter start",
    )?;
    let estimated_current_water_flow_liters_per_minute = Gauge::new(
        "gardena_estimated_current_water_flow_liters_per_minute",
        "Modeled current total water flow rate in liters per minute across all active valves",
    )?;

    register(&registry, &exporter_connected)?;
    register(&registry, &last_event_timestamp)?;
    register(&registry, &last_snapshot_timestamp)?;
    register(&registry, &last_successful_sync_timestamp)?;
    register(&registry, &websocket_reconnects_total)?;
    register(&registry, &token_refreshes_total)?;
    register(&registry, &snapshot_refreshes_total)?;
    register(&registry, &exporter_info)?;
    register(&registry, &device_info)?;
    register(&registry, &sensor_info)?;
    register(&registry, &valve_info)?;
    register(&registry, &valve_set_info)?;
    register(&registry, &battery_level)?;
    register(&registry, &battery_level_timestamp)?;
    register(&registry, &rf_link_level)?;
    register(&registry, &rf_link_level_timestamp)?;
    register(&registry, &soil_humidity)?;
    register(&registry, &soil_humidity_timestamp)?;
    register(&registry, &soil_temperature)?;
    register(&registry, &soil_temperature_timestamp)?;
    register(&registry, &ambient_temperature)?;
    register(&registry, &ambient_temperature_timestamp)?;
    register(&registry, &light_intensity)?;
    register(&registry, &light_intensity_timestamp)?;
    register(&registry, &valve_open)?;
    register(&registry, &valve_duration_seconds)?;
    register(&registry, &valve_activity_timestamp)?;
    register(&registry, &valve_state_timestamp)?;
    register(&registry, &valve_set_state_timestamp)?;
    register(&registry, &valve_estimated_open_seconds_total)?;
    register(&registry, &valve_estimated_water_liters_total)?;
    register(&registry, &valve_estimated_flow_liters_per_minute)?;
    register(
        &registry,
        &valve_estimated_current_water_flow_liters_per_minute,
    )?;
    register(&registry, &estimated_water_liters_total)?;
    register(&registry, &estimated_current_water_flow_liters_per_minute)?;

    exporter_connected.set(i64::from(state.connected));
    if let Some(timestamp) = state.last_event_timestamp {
        last_event_timestamp.set(timestamp);
    }
    if let Some(timestamp) = state.last_snapshot_timestamp {
        last_snapshot_timestamp.set(timestamp);
    }
    if let Some(timestamp) = state.last_successful_sync_timestamp {
        last_successful_sync_timestamp.set(timestamp);
    }
    websocket_reconnects_total.inc_by(state.websocket_reconnects_total);
    token_refreshes_total.inc_by(state.token_refreshes_total);
    snapshot_refreshes_total.inc_by(state.snapshot_refreshes_total);

    let location_id = state.location_id.as_deref().unwrap_or("");
    let location_name = state.location_name.as_deref().unwrap_or("");
    exporter_info
        .with_label_values(&[env!("CARGO_PKG_VERSION"), location_id, location_name])
        .set(1.0);

    let mut total_estimated_water_liters = 0.0;
    let mut total_estimated_current_flow_liters_per_minute = 0.0;

    for device in state.devices.values() {
        let location = location_name_or_id(&state, device);
        let common = device.common.as_ref();
        let device_name = common
            .and_then(|service| service.name.as_deref())
            .unwrap_or(&device.id);
        let model_type = common
            .and_then(|service| service.model_type.as_deref())
            .unwrap_or("");
        let serial = common
            .and_then(|service| service.serial.as_deref())
            .unwrap_or("");
        let battery_state_value = common
            .and_then(|service| service.battery_state.as_deref())
            .unwrap_or("");
        let rf_link_state_value = common
            .and_then(|service| service.rf_link_state.as_deref())
            .unwrap_or("");

        if common.is_some() {
            device_info
                .with_label_values(&[
                    &location,
                    &device.id,
                    device_name,
                    model_type,
                    serial,
                    battery_state_value,
                    rf_link_state_value,
                ])
                .set(1.0);
        }

        if let Some(common_service) = common {
            if device.sensor.is_some() {
                sensor_info
                    .with_label_values(&[
                        &location,
                        &device.id,
                        device_name,
                        model_type,
                        serial,
                        battery_state_value,
                        rf_link_state_value,
                    ])
                    .set(1.0);
            }

            if let Some(level) = common_service.battery_level {
                battery_level
                    .with_label_values(&[&location, &device.id, device_name, model_type])
                    .set(level);
            }
            if let Some(timestamp) = common_service.battery_level_timestamp {
                battery_level_timestamp
                    .with_label_values(&[&location, &device.id, device_name, model_type])
                    .set(timestamp);
            }
            if let Some(level) = common_service.rf_link_level {
                rf_link_level
                    .with_label_values(&[&location, &device.id, device_name, model_type])
                    .set(level);
            }
            if let Some(timestamp) = common_service.rf_link_level_timestamp {
                rf_link_level_timestamp
                    .with_label_values(&[&location, &device.id, device_name, model_type])
                    .set(timestamp);
            }
        }

        if let Some(sensor) = &device.sensor {
            if let Some(value) = sensor.soil_humidity {
                soil_humidity
                    .with_label_values(&[&location, &device.id, device_name, model_type])
                    .set(value);
            }
            if let Some(timestamp) = sensor.soil_humidity_timestamp {
                soil_humidity_timestamp
                    .with_label_values(&[&location, &device.id, device_name, model_type])
                    .set(timestamp);
            }
            if let Some(value) = sensor.soil_temperature {
                soil_temperature
                    .with_label_values(&[&location, &device.id, device_name, model_type])
                    .set(value);
            }
            if let Some(timestamp) = sensor.soil_temperature_timestamp {
                soil_temperature_timestamp
                    .with_label_values(&[&location, &device.id, device_name, model_type])
                    .set(timestamp);
            }
            if let Some(value) = sensor.ambient_temperature {
                ambient_temperature
                    .with_label_values(&[&location, &device.id, device_name, model_type])
                    .set(value);
            }
            if let Some(timestamp) = sensor.ambient_temperature_timestamp {
                ambient_temperature_timestamp
                    .with_label_values(&[&location, &device.id, device_name, model_type])
                    .set(timestamp);
            }
            if let Some(value) = sensor.light_intensity {
                light_intensity
                    .with_label_values(&[&location, &device.id, device_name, model_type])
                    .set(value);
            }
            if let Some(timestamp) = sensor.light_intensity_timestamp {
                light_intensity_timestamp
                    .with_label_values(&[&location, &device.id, device_name, model_type])
                    .set(timestamp);
            }
        }

        let controller_name = device_name;
        for valve in device.valves.values() {
            let valve_name = valve.name.as_deref().unwrap_or(&valve.id);
            let state_label = valve.state.as_deref().unwrap_or("");
            let activity_label = valve.activity.as_deref().unwrap_or("");
            let error_label = valve.last_error.as_deref().unwrap_or("");
            let usage_totals = state.valve_usage_totals(&valve.id);

            valve_info
                .with_label_values(&[
                    &location,
                    &device.id,
                    controller_name,
                    &valve.id,
                    valve_name,
                    state_label,
                    activity_label,
                    error_label,
                ])
                .set(1.0);

            valve_open
                .with_label_values(&[
                    &location,
                    &device.id,
                    controller_name,
                    &valve.id,
                    valve_name,
                ])
                .set(f64::from(activity_label != "CLOSED"));
            valve_estimated_open_seconds_total
                .with_label_values(&[
                    &location,
                    &device.id,
                    controller_name,
                    &valve.id,
                    valve_name,
                ])
                .inc_by(usage_totals.open_seconds);
            valve_estimated_water_liters_total
                .with_label_values(&[
                    &location,
                    &device.id,
                    controller_name,
                    &valve.id,
                    valve_name,
                ])
                .inc_by(usage_totals.estimated_liters);
            valve_estimated_flow_liters_per_minute
                .with_label_values(&[
                    &location,
                    &device.id,
                    controller_name,
                    &valve.id,
                    valve_name,
                ])
                .set(usage_totals.flow_liters_per_minute);
            valve_estimated_current_water_flow_liters_per_minute
                .with_label_values(&[
                    &location,
                    &device.id,
                    controller_name,
                    &valve.id,
                    valve_name,
                ])
                .set(usage_totals.current_flow_liters_per_minute);

            total_estimated_water_liters += usage_totals.estimated_liters;
            total_estimated_current_flow_liters_per_minute +=
                usage_totals.current_flow_liters_per_minute;

            if let Some(duration) = valve.duration_seconds {
                valve_duration_seconds
                    .with_label_values(&[
                        &location,
                        &device.id,
                        controller_name,
                        &valve.id,
                        valve_name,
                    ])
                    .set(duration);
            }
            if let Some(timestamp) = valve.activity_timestamp {
                valve_activity_timestamp
                    .with_label_values(&[
                        &location,
                        &device.id,
                        controller_name,
                        &valve.id,
                        valve_name,
                    ])
                    .set(timestamp);
            }
            if let Some(timestamp) = valve.state_timestamp {
                valve_state_timestamp
                    .with_label_values(&[
                        &location,
                        &device.id,
                        controller_name,
                        &valve.id,
                        valve_name,
                    ])
                    .set(timestamp);
            }
        }

        for valve_set in device.valve_sets.values() {
            valve_set_info
                .with_label_values(&[
                    &location,
                    &device.id,
                    controller_name,
                    &valve_set.id,
                    valve_set.state.as_deref().unwrap_or(""),
                    valve_set.last_error.as_deref().unwrap_or(""),
                ])
                .set(1.0);

            if let Some(timestamp) = valve_set.state_timestamp {
                valve_set_state_timestamp
                    .with_label_values(&[&location, &device.id, controller_name, &valve_set.id])
                    .set(timestamp);
            }
        }
    }

    estimated_water_liters_total.inc_by(total_estimated_water_liters);
    estimated_current_water_flow_liters_per_minute
        .set(total_estimated_current_flow_liters_per_minute);

    let encoder = TextEncoder::new();
    let families = registry.gather();
    let mut buffer = Vec::new();
    encoder.encode(&families, &mut buffer)?;
    String::from_utf8(buffer).context("metrics output was not valid UTF-8")
}

fn register<C>(registry: &Registry, collector: &C) -> Result<()>
where
    C: Collector + Clone + 'static,
{
    registry.register(Box::new(collector.clone()))?;
    Ok(())
}

fn location_name_or_id(state: &ExporterState, _device: &DeviceState) -> String {
    state
        .location_name
        .clone()
        .or_else(|| state.location_id.clone())
        .unwrap_or_else(|| "unknown".to_string())
}

fn string_attr(attributes: Option<&Value>, key: &str) -> Option<String> {
    attributes
        .and_then(|attrs| attrs.get(key))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn string_nested_attr(attributes: Option<&Value>, key: &str, nested_key: &str) -> Option<String> {
    attributes
        .and_then(|attrs| attrs.get(key))
        .and_then(|value| value.get(nested_key))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn number_nested_attr(attributes: Option<&Value>, key: &str, nested_key: &str) -> Option<f64> {
    attributes
        .and_then(|attrs| attrs.get(key))
        .and_then(|value| value.get(nested_key))
        .and_then(Value::as_f64)
}

fn timestamp_nested_attr(attributes: Option<&Value>, key: &str, nested_key: &str) -> Option<f64> {
    attributes
        .and_then(|attrs| attrs.get(key))
        .and_then(|value| value.get(nested_key))
        .and_then(Value::as_str)
        .and_then(parse_timestamp)
}

fn relationship_one_id(relationships: Option<&Value>, key: &str) -> Option<String> {
    relationships
        .and_then(|rels| rels.get(key))
        .and_then(|rel| rel.get("data"))
        .and_then(|data| data.get("id"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

#[allow(clippy::cast_precision_loss)]
fn parse_timestamp(value: &str) -> Option<f64> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|date_time| date_time.with_timezone(&Utc).timestamp_millis() as f64 / 1000.0)
}

#[allow(clippy::cast_precision_loss)]
fn now_timestamp() -> f64 {
    Utc::now().timestamp_millis() as f64 / 1000.0
}

fn resolve_valve_estimated_flow_liters_per_minute(
    valve_id: &str,
    default_estimated_flow_liters_per_minute: Option<f64>,
    valve_estimated_flow_liters_per_minute: &BTreeMap<String, f64>,
) -> Option<f64> {
    valve_estimated_flow_liters_per_minute
        .get(valve_id)
        .copied()
        .or(default_estimated_flow_liters_per_minute)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn applies_common_and_sensor_services_from_snapshot() {
        let selected = LocationSummary {
            id: "location-1".to_string(),
            name: "Terrace".to_string(),
        };
        let snapshot: SnapshotResponse = serde_json::from_value(json!({
            "data": {
                "id": "location-1",
                "type": "LOCATION",
                "attributes": { "name": "Terrace" }
            },
            "included": [
                {
                    "id": "device-1",
                    "type": "DEVICE",
                    "relationships": {
                        "location": { "data": { "id": "location-1", "type": "LOCATION" } }
                    }
                },
                {
                    "id": "common-1",
                    "type": "COMMON",
                    "attributes": {
                        "name": { "value": "Bed Sensor" },
                        "batteryLevel": { "value": 87.0, "timestamp": "2026-04-06T10:00:00Z" },
                        "modelType": { "value": "smart Sensor" },
                        "serial": { "value": "SER123" }
                    },
                    "relationships": {
                        "device": { "data": { "id": "device-1", "type": "DEVICE" } }
                    }
                },
                {
                    "id": "sensor-1",
                    "type": "SENSOR",
                    "attributes": {
                        "soilHumidity": { "value": 44.0, "timestamp": "2026-04-06T10:01:00Z" },
                        "soilTemperature": { "value": 17.5, "timestamp": "2026-04-06T10:01:00Z" }
                    },
                    "relationships": {
                        "device": { "data": { "id": "device-1", "type": "DEVICE" } }
                    }
                }
            ]
        }))
        .expect("snapshot JSON should deserialize");

        let mut state = ExporterState::default();
        state.apply_snapshot(&selected, &snapshot, Some(3.5), &BTreeMap::new());

        let device = state.devices.get("device-1").expect("device should exist");
        let common = device.common.as_ref().expect("common service should exist");
        let sensor = device.sensor.as_ref().expect("sensor service should exist");

        assert_eq!(state.location_name.as_deref(), Some("Terrace"));
        assert_eq!(common.name.as_deref(), Some("Bed Sensor"));
        assert_eq!(common.model_type.as_deref(), Some("smart Sensor"));
        assert_eq!(sensor.soil_humidity, Some(44.0));
        assert_eq!(sensor.soil_temperature, Some(17.5));
    }

    #[test]
    fn tracks_valve_usage_with_per_valve_flow_override() {
        let mut state = ExporterState::default();
        let mut overrides = BTreeMap::new();
        overrides.insert("valve-1".to_string(), 1.2);

        let open: JsonApiResource = serde_json::from_value(json!({
            "id": "valve-1",
            "type": "VALVE",
            "attributes": {
                "name": { "value": "Drip Zone" },
                "activity": { "value": "MANUAL_WATERING", "timestamp": "2026-04-06T10:00:00Z" },
                "state": { "value": "OK", "timestamp": "2026-04-06T10:00:00Z" }
            },
            "relationships": {
                "device": { "data": { "id": "device-1", "type": "DEVICE" } }
            }
        }))
        .expect("open valve JSON should deserialize");
        let close: JsonApiResource = serde_json::from_value(json!({
            "id": "valve-1",
            "type": "VALVE",
            "attributes": {
                "name": { "value": "Drip Zone" },
                "activity": { "value": "CLOSED", "timestamp": "2026-04-06T10:15:00Z" },
                "state": { "value": "OK", "timestamp": "2026-04-06T10:15:00Z" }
            },
            "relationships": {
                "device": { "data": { "id": "device-1", "type": "DEVICE" } }
            }
        }))
        .expect("closed valve JSON should deserialize");

        state.apply_resource(&open, Some(3.5), &overrides);
        state.apply_resource(&close, Some(3.5), &overrides);

        let usage = state
            .valve_usage
            .get("valve-1")
            .expect("valve usage should exist");

        assert!(!usage.currently_open);
        assert_eq!(usage.estimated_flow_liters_per_minute, Some(1.2));
        assert!((usage.total_open_seconds - 900.0).abs() < 0.001);
        assert!((usage.total_estimated_liters - 18.0).abs() < 0.001);
    }
}
