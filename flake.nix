{
  description = "wetink: a streaming blackboard for LLM agents";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        wetink = pkgs.buildGoModule {
          pname = "wetink";
          version = "0.1.0";
          src = ./.;
          vendorHash = "sha256-LGp0rDhcRrb80QeFjS4LNzDskcIUAvxRLiajZ0euxzo=";
          subPackages = [ "cmd/wetink" ];
          doCheck = false; # unit tests run in the dev shell; integration tests need services
        };
        # llama-server with /v1/systemone and LiquidAI d1 support, which landed after
        # nixpkgs' v0.6.0. Upstream's own flake no longer evaluates on current nixpkgs.
        llama-cpp = pkgs.llama-cpp.overrideAttrs (old: {
          version = "0.6.0-unstable-2026-10-09";
          src = pkgs.fetchFromGitHub {
            owner = "ggml-org";
            repo = "llama.cpp";
            rev = "b42b7e6d3059503dd4bed6ca5dcf342bb6379f67";
            hash = "sha256-zkHQPGopaW4JXEebCGZjwl3Jqx4yb24Msq+ZvJsyNP8=";
          };
          patches = [ ]; # nixpkgs' test-model patch does not apply to this revision
          # No web UI (it needs a large npm fetch); only the HTTP APIs are used.
          nativeBuildInputs = builtins.filter
            (p: p != pkgs.npmHooks.npmConfigHook && p != pkgs.nodejs_latest)
            old.nativeBuildInputs;
          npmDeps = null;
          preConfigure = "";
          cmakeFlags = old.cmakeFlags ++ [
            (pkgs.lib.cmakeBool "LLAMA_BUILD_UI" false)
            (pkgs.lib.cmakeBool "LLAMA_USE_PREBUILT_UI" false)
          ];
          doCheck = false;
        });
      in
      {
        packages.default = wetink;

        devShells.default = pkgs.mkShell {
          packages = [
            pkgs.go
            pkgs.gopls
            pkgs.golangci-lint
            llama-cpp
            pkgs.natscli
            pkgs.codex # coding agent for the experiments (wetink experiment)
            pkgs.postgresql
            pkgs.python3Packages.huggingface-hub # the hf CLI, for model downloads
          ];

          shellHook = ''
            export PATH="$PWD/bin:$PATH" # go build -o bin/wetink ./cmd/wetink
            export WETINK_TOKEN="''${WETINK_TOKEN:-dev-token}"
            export NATS_URL="''${NATS_URL:-nats://$WETINK_TOKEN@localhost:4222}"
            export DATABASE_URL="''${DATABASE_URL:-postgresql://wetink:wetink@localhost:5432/wetink}"
            export OPENAI_BASE_URL="''${OPENAI_BASE_URL:-http://localhost:8080/v1}"
            export OPENAI_API_KEY="''${OPENAI_API_KEY:-mock}"
            # Classifier: LiquidAI d1-3B decision model behind llama-server's /v1/systemone.
            export WETINK_CLASSIFIER_API="''${WETINK_CLASSIFIER_API:-systemone}"
            export WETINK_CLASSIFIER_BASE_URL="''${WETINK_CLASSIFIER_BASE_URL:-http://localhost:9931/v1}"
            export WETINK_CLASSIFIER_MODEL="''${WETINK_CLASSIFIER_MODEL:-default}"
            # Names new topics for the systemone classifier (decision models only pick options).
            export WETINK_TOPIC_NAMER_BASE_URL="''${WETINK_TOPIC_NAMER_BASE_URL:-http://localhost:1234/v1}"
            export WETINK_TOPIC_NAMER_MODEL="''${WETINK_TOPIC_NAMER_MODEL:-lfm2.5-1.2b-instruct-mlx}"
            export WETINK_TOPIC_NAMER_API_KEY="''${WETINK_TOPIC_NAMER_API_KEY:-lm-studio}"
          '';
        };
      });
}
