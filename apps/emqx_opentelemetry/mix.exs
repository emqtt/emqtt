defmodule EMQXOpentelemetry.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_opentelemetry,
      version: "0.1.0",
      build_path: "../../_build",
      erlc_options: UMP.erlc_options(),
      erlc_paths: UMP.erlc_paths(),
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [extra_applications: UMP.extra_applications(), mod: {:emqx_otel_app, []}]
  end

  def deps() do
    [
      {:emqx, in_umbrella: true},
      {:opentelemetry_api,
       github: "emqx/opentelemetry-erlang",
       tag: "v1.4.9-emqx",
       sparse: "apps/opentelemetry_api",
       override: true},
      {:opentelemetry,
       github: "emqx/opentelemetry-erlang",
       tag: "v1.4.9-emqx",
       sparse: "apps/opentelemetry",
       override: true},
      {:opentelemetry_experimental,
       github: "emqx/opentelemetry-erlang",
       tag: "v1.4.9-emqx",
       sparse: "apps/opentelemetry_experimental",
       override: true},
      {:opentelemetry_api_experimental,
       github: "emqx/opentelemetry-erlang",
       tag: "v1.4.9-emqx",
       sparse: "apps/opentelemetry_api_experimental",
       override: true},
      {:opentelemetry_exporter,
       github: "emqx/opentelemetry-erlang",
       tag: "v1.4.9-emqx",
       sparse: "apps/opentelemetry_exporter",
       override: true}
    ]
  end
end
