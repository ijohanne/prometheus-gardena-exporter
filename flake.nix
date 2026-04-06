{
  description = "Prometheus exporter for the GARDENA smart system API";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    git-hooks = {
      url = "github:cachix/git-hooks.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      rust-overlay,
      git-hooks,
      ...
    }:
    (flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ rust-overlay.overlays.default ];
        };

        rustToolchain = pkgs.rust-bin.stable.latest.default.override {
          extensions = [ "rust-src" ];
        };

        rustPlatform = pkgs.makeRustPlatform {
          cargo = rustToolchain;
          rustc = rustToolchain;
        };

        pre-commit-check = git-hooks.lib.${system}.run {
          src = ./.;
          hooks = {
            rustfmt = {
              enable = true;
              name = "rustfmt";
              entry = "cargo fmt --check";
              files = "^.*\\.rs$";
              pass_filenames = false;
              language = "system";
            };
            clippy = {
              enable = true;
              name = "clippy";
              entry = "cargo clippy --all-targets --all-features -- -D warnings -D clippy::pedantic";
              files = "^.*\\.rs$";
              pass_filenames = false;
              language = "system";
            };
          };
          tools = {
            cargo = rustToolchain;
            clippy = rustToolchain;
            rustfmt = rustToolchain;
          };
        };
      in
      {
        packages.default = rustPlatform.buildRustPackage {
          pname = "prometheus-gardena-exporter";
          version = "0.1.0";
          src = ./.;
          cargoLock.lockFile = ./Cargo.lock;
        };

        checks = {
          inherit pre-commit-check;
        };

        devShells.default = pkgs.mkShell {
          inherit (pre-commit-check) shellHook;
          buildInputs = with pkgs; [
            rustToolchain
          ];
        };
      }
    )) // {
      nixosModules.default = import ./module.nix self;
    };
}
