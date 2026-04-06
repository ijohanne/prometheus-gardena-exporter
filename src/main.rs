mod gardena;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use gardena::{
    AuthConfig, RuntimeConfig, SharedState, DEFAULT_API_URL, DEFAULT_AUTH_URL,
    DEFAULT_ESTIMATED_FLOW_LITERS_PER_MINUTE,
};
use reqwest::Client;
use std::{
    collections::BTreeMap, convert::Infallible, net::IpAddr, str::FromStr, sync::Arc,
    time::Duration,
};
use tokio::sync::RwLock;
use tracing::info;
use warp::{http::StatusCode, Filter, Reply};

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Serve(ServeArgs),
    FetchToken(FetchTokenArgs),
    PrintTokenCurl(PrintTokenCurlArgs),
    ListLocations(ListLocationsArgs),
    ListValves(ListValvesArgs),
}

#[derive(Debug, Clone, Args)]
struct AuthArgs {
    #[arg(long, env = "GARDENA_APPLICATION_KEY")]
    application_key: String,

    #[arg(long, env = "GARDENA_APPLICATION_SECRET")]
    application_secret: String,

    #[arg(long, env = "GARDENA_AUTH_URL", default_value = DEFAULT_AUTH_URL)]
    auth_url: String,
}

#[derive(Debug, Args)]
struct ServeArgs {
    #[arg(long, default_value = "127.0.0.1")]
    listen_address: IpAddr,

    #[arg(long, default_value_t = 9134)]
    listen_port: u16,

    #[arg(long, env = "GARDENA_LOCATION_ID")]
    location_id: Option<String>,

    #[arg(long, default_value = DEFAULT_API_URL)]
    api_url: String,

    #[arg(long, default_value_t = 1800)]
    snapshot_interval_seconds: u64,

    #[arg(long, default_value_t = 5)]
    reconnect_delay_seconds: u64,

    #[arg(long, default_value_t = 300)]
    max_reconnect_delay_seconds: u64,

    #[arg(long, default_value_t = DEFAULT_ESTIMATED_FLOW_LITERS_PER_MINUTE)]
    estimated_flow_liters_per_minute: f64,

    #[arg(
        long = "valve-estimated-flow-liters-per-minute",
        value_name = "SERVICE_ID=LPM"
    )]
    valve_estimated_flow_liters_per_minute: Vec<ValveFlowOverride>,

    #[arg(long)]
    validate_auth_on_startup: bool,

    #[command(flatten)]
    auth: AuthArgs,
}

#[derive(Debug, Args)]
struct FetchTokenArgs {
    #[arg(long)]
    raw: bool,

    #[command(flatten)]
    auth: AuthArgs,
}

#[derive(Debug, Args)]
struct PrintTokenCurlArgs {
    #[arg(long, default_value = DEFAULT_AUTH_URL)]
    auth_url: String,
}

#[derive(Debug, Args)]
struct ListLocationsArgs {
    #[command(flatten)]
    auth: AuthArgs,
}

#[derive(Debug, Args)]
struct ListValvesArgs {
    #[arg(long, env = "GARDENA_LOCATION_ID")]
    location_id: Option<String>,

    #[arg(long, default_value = DEFAULT_API_URL)]
    api_url: String,

    #[command(flatten)]
    auth: AuthArgs,
}

#[derive(Debug, Clone)]
struct ValveFlowOverride {
    service_id: String,
    liters_per_minute: f64,
}

impl FromStr for ValveFlowOverride {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        let Some((service_id, liters_per_minute)) = value.split_once('=') else {
            return Err("expected SERVICE_ID=LPM".to_string());
        };
        let service_id = service_id.trim();
        if service_id.is_empty() {
            return Err("service id must not be empty".to_string());
        }
        let liters_per_minute = liters_per_minute
            .trim()
            .parse::<f64>()
            .map_err(|_| "flow value must be a number".to_string())?;
        if liters_per_minute.is_sign_negative() {
            return Err("flow value must be zero or greater".to_string());
        }

        Ok(Self {
            service_id: service_id.to_string(),
            liters_per_minute,
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    let http_client = Client::builder()
        .user_agent(concat!(
            "prometheus-gardena-exporter/",
            env!("CARGO_PKG_VERSION")
        ))
        .build()
        .context("failed to build HTTP client")?;

    match cli.command {
        Command::Serve(args) => serve(args, http_client).await,
        Command::FetchToken(args) => fetch_token_command(args, http_client).await,
        Command::PrintTokenCurl(args) => {
            print_token_curl(&args.auth_url);
            Ok(())
        }
        Command::ListLocations(args) => list_locations_command(args, http_client).await,
        Command::ListValves(args) => list_valves_command(args, http_client).await,
    }
}

async fn serve(args: ServeArgs, http_client: Client) -> Result<()> {
    let valve_estimated_flow_liters_per_minute = args
        .valve_estimated_flow_liters_per_minute
        .into_iter()
        .fold(BTreeMap::new(), |mut overrides, entry| {
            overrides.insert(entry.service_id, entry.liters_per_minute);
            overrides
        });

    let runtime_config = RuntimeConfig {
        auth: AuthConfig {
            application_key: args.auth.application_key,
            application_secret: args.auth.application_secret,
            auth_url: args.auth.auth_url,
        },
        api_url: args.api_url,
        location_id: args.location_id,
        snapshot_interval: Duration::from_secs(args.snapshot_interval_seconds),
        reconnect_delay: Duration::from_secs(args.reconnect_delay_seconds),
        max_reconnect_delay: Duration::from_secs(args.max_reconnect_delay_seconds),
        estimated_flow_liters_per_minute: Some(args.estimated_flow_liters_per_minute),
        valve_estimated_flow_liters_per_minute,
    };

    let shared_state: SharedState = Arc::new(RwLock::new(gardena::ExporterState::default()));

    if args.validate_auth_on_startup {
        gardena::validate_startup(&http_client, &runtime_config, &shared_state).await?;
    }

    tokio::spawn(gardena::run_sync_loop(
        shared_state.clone(),
        http_client,
        runtime_config.clone(),
    ));

    let metrics_route = warp::path("metrics")
        .and(warp::get())
        .and(with_state(shared_state.clone()))
        .and_then(handle_metrics);

    let health_route = warp::path("healthz")
        .and(warp::get())
        .and(with_state(shared_state.clone()))
        .and_then(handle_health);

    let index_route = warp::path::end()
        .and(warp::get())
        .and(with_state(shared_state.clone()))
        .and_then(handle_index);

    let routes = metrics_route.or(health_route).or(index_route);

    info!(
        listen_address = %args.listen_address,
        listen_port = args.listen_port,
        "starting Gardena exporter HTTP server"
    );

    warp::serve(routes)
        .run((args.listen_address, args.listen_port))
        .await;

    Ok(())
}

async fn fetch_token_command(args: FetchTokenArgs, http_client: Client) -> Result<()> {
    let token = gardena::fetch_token(
        &http_client,
        &AuthConfig {
            application_key: args.auth.application_key,
            application_secret: args.auth.application_secret,
            auth_url: args.auth.auth_url,
        },
    )
    .await?;

    if args.raw {
        println!("{}", token.access_token);
    } else {
        println!(
            "token_type={}",
            token.token_type.as_deref().unwrap_or("unknown")
        );
        println!("expires_in={}", token.expires_in.unwrap_or_default());
        if let Some(scope) = token.scope {
            println!("scope={scope}");
        }
        println!("access_token={}", token.access_token);
    }

    Ok(())
}

async fn list_locations_command(args: ListLocationsArgs, http_client: Client) -> Result<()> {
    let locations = gardena::list_locations(
        &http_client,
        &AuthConfig {
            application_key: args.auth.application_key,
            application_secret: args.auth.application_secret,
            auth_url: args.auth.auth_url,
        },
    )
    .await?;

    for location in locations {
        println!("{}\t{}", location.id, location.name);
    }

    Ok(())
}

async fn list_valves_command(args: ListValvesArgs, http_client: Client) -> Result<()> {
    let valves = gardena::list_valves(
        &http_client,
        &AuthConfig {
            application_key: args.auth.application_key,
            application_secret: args.auth.application_secret,
            auth_url: args.auth.auth_url,
        },
        &args.api_url,
        args.location_id.as_deref(),
    )
    .await?;

    println!("location_id\tlocation\tdevice_id\tcontroller_name\tservice_id\tvalve_name");
    for valve in valves {
        println!(
            "{}\t{}\t{}\t{}\t{}\t{}",
            valve.location_id,
            valve.location_name,
            valve.device_id,
            valve.controller_name,
            valve.service_id,
            valve.valve_name,
        );
    }

    Ok(())
}

fn print_token_curl(auth_url: &str) {
    let token_url = format!("{}/oauth2/token", auth_url.trim_end_matches('/'));
    println!(
        "curl -fsSL -X POST -d \"grant_type=client_credentials&client_id=$GARDENA_APPLICATION_KEY&client_secret=$GARDENA_APPLICATION_SECRET\" {token_url}"
    );
}

fn with_state(
    state: SharedState,
) -> impl Filter<Extract = (SharedState,), Error = Infallible> + Clone {
    warp::any().map(move || state.clone())
}

async fn handle_metrics(state: SharedState) -> Result<impl Reply, Infallible> {
    let reply = match gardena::render_metrics(&state).await {
        Ok(body) => warp::reply::with_header(
            body,
            "content-type",
            "text/plain; version=0.0.4; charset=utf-8",
        )
        .into_response(),
        Err(error) => warp::reply::with_status(
            format!("failed to render metrics: {error:#}\n"),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .into_response(),
    };

    Ok(reply)
}

async fn handle_health(state: SharedState) -> Result<impl Reply, Infallible> {
    let state = state.read().await;
    let (status, body) = if state.last_successful_sync_timestamp.is_some() {
        (StatusCode::OK, "ok\n".to_string())
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "waiting for first successful Gardena sync{}\n",
                state
                    .last_error
                    .as_deref()
                    .map(|error| format!(": {error}"))
                    .unwrap_or_default()
            ),
        )
    };
    Ok(warp::reply::with_status(body, status))
}

async fn handle_index(state: SharedState) -> Result<impl Reply, Infallible> {
    let state = state.read().await;
    let location = state.location_name.as_deref().unwrap_or("not yet selected");
    let summary = format!(
        "prometheus-gardena-exporter\nlocation: {location}\nconnected: {}\nmetrics: /metrics\nhealth: /healthz\n",
        state.connected
    );
    Ok(warp::reply::with_status(summary, StatusCode::OK))
}
