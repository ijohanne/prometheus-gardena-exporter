self:
{ config, lib, pkgs, ... }:
with lib;
let
  cfg = config.services.prometheus-gardena-exporter;
  name = "gardena";
  package = self.packages.${pkgs.stdenv.hostPlatform.system}.default;
in
{
  options.services.prometheus-gardena-exporter = with types; mkOption {
    type = types.submodule {
      options = {
        enable = mkEnableOption "the prometheus ${name} exporter";
        enableLocalScraping = mkEnableOption "scraping by local prometheus";
        enableGrafanaDashboard = mkEnableOption "provisioning of the Grafana dashboard";
        port = mkOption {
          type = types.port;
          default = 9134;
          description = "Port to listen on.";
        };
        listenAddress = mkOption {
          type = types.str;
          default = "127.0.0.1";
          description = "Address to listen on.";
        };
        authUrl = mkOption {
          type = types.str;
          default = "https://api.authentication.husqvarnagroup.dev/v1";
          description = "Base URL of the Husqvarna authentication API.";
        };
        apiUrl = mkOption {
          type = types.str;
          default = "https://api.smart.gardena.dev/v2";
          description = "Base URL of the GARDENA smart system API.";
        };
        locationId = mkOption {
          type = types.nullOr types.str;
          default = null;
          description = ''
            Explicit GARDENA location ID to export. Leave null when the application has access to exactly one location.
          '';
        };
        snapshotIntervalSeconds = mkOption {
          type = types.int;
          default = 1800;
          description = ''
            Interval in seconds for periodic snapshot reconciliation alongside the live websocket stream.
          '';
        };
        reconnectDelaySeconds = mkOption {
          type = types.int;
          default = 5;
          description = ''
            Initial reconnect delay in seconds after websocket failures.
          '';
        };
        maxReconnectDelaySeconds = mkOption {
          type = types.int;
          default = 300;
          description = ''
            Maximum reconnect delay in seconds after websocket failures.
          '';
        };
        estimatedFlowLitersPerMinute = mkOption {
          type = types.float;
          default = 3.5;
          description = ''
            Modeled drip irrigation flow rate in liters per minute used to estimate water usage.
            The default is a conservative best-effort value derived from roughly 5 m3/month total usage
            over three 15 minute watering segments per day, which works out to about 3.7 L/min and is
            rounded down slightly here.
          '';
        };
        estimatedFlowLitersPerMinuteByValve = mkOption {
          type = types.attrsOf types.float;
          default = { };
          example = {
            "5f7a3e6e-1111-2222-3333-444444444444" = 1.2;
            "8c9d0a1b-5555-6666-7777-888888888888" = 6.0;
          };
          description = ''
            Per-valve modeled flow overrides in liters per minute, keyed by Gardena VALVE service ID.
            Use this when some zones are drip-based and others use sprinklers.
          '';
        };
        validateAuthOnStartup = mkOption {
          type = types.bool;
          default = true;
          description = ''
            Fetch an OAuth access token on service start and fail fast if that step does not work.
          '';
        };
        restartSec = mkOption {
          type = types.str;
          default = "30s";
          description = ''
            Delay before restarting the service after a failure.
          '';
        };
        applicationKeyFile = mkOption {
          type = types.path;
          description = ''
            File containing the GARDENA application key from the Husqvarna developer portal.
          '';
        };
        applicationSecretFile = mkOption {
          type = types.path;
          description = ''
            File containing the GARDENA application secret from the Husqvarna developer portal.
          '';
        };
        user = mkOption {
          type = types.str;
          default = "${name}-exporter";
          description = "User name under which the ${name} exporter shall be run.";
        };
        group = mkOption {
          type = types.str;
          default = "${name}-exporter";
          description = "Group under which the ${name} exporter shall be run.";
        };
      };
    };
    default = { };
  };

  config = mkIf cfg.enable {
    users.users."${cfg.user}" = {
      description = "Prometheus ${name} exporter service user";
      isSystemUser = true;
      group = cfg.group;
    };
    users.groups."${cfg.group}" = { };

    systemd.services."prometheus-${name}-exporter" =
      let
        valveFlowArguments = concatStringsSep " " (
          mapAttrsToList
            (
              serviceId: litersPerMinute:
              "--valve-estimated-flow-liters-per-minute "
              + escapeShellArg "${serviceId}=${toString litersPerMinute}"
            )
            cfg.estimatedFlowLitersPerMinuteByValve
        );
        wrapper = pkgs.writeShellScript "prometheus-${name}-exporter" ''
          export GARDENA_APPLICATION_KEY="$(tr -d '\n' < "$CREDENTIALS_DIRECTORY/application-key")"
          export GARDENA_APPLICATION_SECRET="$(tr -d '\n' < "$CREDENTIALS_DIRECTORY/application-secret")"

          exec ${getBin package}/bin/prometheus-gardena-exporter serve \
            --listen-address ${cfg.listenAddress} \
            --listen-port ${toString cfg.port} \
            --auth-url ${cfg.authUrl} \
            --api-url ${cfg.apiUrl} \
            ${optionalString (cfg.locationId != null) ''--location-id "${cfg.locationId}"''} \
            --snapshot-interval-seconds ${toString cfg.snapshotIntervalSeconds} \
            --reconnect-delay-seconds ${toString cfg.reconnectDelaySeconds} \
            --max-reconnect-delay-seconds ${toString cfg.maxReconnectDelaySeconds} \
            --estimated-flow-liters-per-minute ${toString cfg.estimatedFlowLitersPerMinute} \
            ${valveFlowArguments} \
            ${optionalString cfg.validateAuthOnStartup "--validate-auth-on-startup"}
        '';
      in
      {
        wantedBy = [ "multi-user.target" ];
        wants = [ "network-online.target" ];
        after = [ "network-online.target" ];
        serviceConfig = {
          Restart = "always";
          RestartSec = cfg.restartSec;
          PrivateTmp = true;
          WorkingDirectory = "/tmp";
          DynamicUser = false;
          User = cfg.user;
          Group = cfg.group;
          LoadCredential = [
            "application-key:${cfg.applicationKeyFile}"
            "application-secret:${cfg.applicationSecretFile}"
          ];
          ExecStart = toString wrapper;
        };
      };

    services.prometheus.scrapeConfigs = mkIf cfg.enableLocalScraping [
      {
        job_name = "${name}";
        honor_labels = true;
        static_configs = [
          {
            targets = [ "127.0.0.1:${toString cfg.port}" ];
          }
        ];
      }
    ];

    services.grafana.provision.dashboards.settings.providers = mkIf cfg.enableGrafanaDashboard (
      let
        dashboardDir = pkgs.runCommand "gardena-grafana-dashboard" { } ''
          mkdir -p $out
          cp ${self}/grafana/GardenaSmartSystem.json $out/GardenaSmartSystem.json
        '';
      in
      [
        {
          name = "${name}";
          options.path = dashboardDir;
          disableDeletion = true;
        }
      ]
    );
  };
}
