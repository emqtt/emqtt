defmodule EMQXUmbrella.MixProject do
  use Mix.Project

  @moduledoc """

  The purpose of this file is to configure the release of EMQX under
  Mix.  Since EMQX uses its own configuration conventions and startup
  procedures, one cannot simply use `iex -S mix`.  Instead, it's
  recommended to build and use the release.

  ## Profiles

  To control the profile and edition to build, we case split on the
  MIX_ENV value.

  The following profiles are valid:

    * `emqx`
    * `emqx-enterprise`
    * `emqx-pkg`
    * `emqx-enterprise-pkg`
    * `dev` -> same as `emqx`, for convenience

  ## Release Environment Variables

  The release build is controlled by a few environment variables.

    * `ELIXIR_MAKE_TAR` - If set to `yes`, will produce a `.tar.gz`
      tarball along with the release.
  """

  # TODO: remove once we switch to the new mix build
  def new_mix_build?() do
    System.get_env("NEW_MIX_BUILD") == "1"
  end

  def project() do
    profile_info = check_profile!()
    version = pkg_vsn()

    if new_mix_build?() do
      [
        apps_path: "apps",
        erlc_options: erlc_options(profile_info, version),
        version: version,
        deps: deps(profile_info, version),
        releases: releases(),
        aliases: aliases()
      ]
    else
      # TODO: this check and clause will be removed when we switch to using mix as the
      # manager for all umbrella apps.
      [
        app: :emqx_mix,
        erlc_options: erlc_options(profile_info, version),
        version: version,
        deps: deps(profile_info, version),
        releases: releases(),
        aliases: aliases()
      ]
    end
  end

  @doc """
  Please try to add dependencies that used by a single umbrella application in the
  application's own `mix.exs` file, if possible.  If it's shared by more than one
  application, or if the dependency requires an `override: true` option, add a new clause
  to `common_dep/1` so that we centralize versions in this root `mix.exs` file as much as
  possible.

  Here, transitive dependencies from our app dependencies should be placed when there's a
  need to override them.  For example, since `jsone` is a dependency to `rocketmq` and to
  `erlavro`, which are both dependencies and not umbrella apps, we need to add the
  override here.  Also, there are cases where adding `override: true` to the umbrella
  application dependency simply won't satisfy mix.  In such cases, it's fine to add it
  here.
  """
  def deps(profile_info, version) do
    # we need several overrides here because dependencies specify
    # other exact versions, and not ranges.

    if new_mix_build?() do
      new_deps()
    else
      old_deps(profile_info, version)
    end
  end

  def new_deps() do
    common_deps() ++
      quicer_dep() ++
      jq_dep() ++
      extra_release_apps() ++
      overridden_deps()
  end

  ## TODO: this should be removed once we migrate the release build to mix
  defp old_deps(profile_info, version) do
    rebar3_umbrella_apps = emqx_apps(profile_info, version) ++ enterprise_deps(profile_info)

    common_deps() ++
      extra_release_apps() ++
      overridden_deps() ++
      jq_dep() ++
      quicer_dep() ++ rebar3_umbrella_apps
  end

  def overridden_deps() do
    [
      common_dep(:lc),
      common_dep(:typerefl),
      common_dep(:ehttpc),
      common_dep(:gproc),
      common_dep(:jiffy),
      common_dep(:cowboy),
      common_dep(:esockd),
      common_dep(:rocksdb),
      common_dep(:ekka),
      common_dep(:gen_rpc),
      common_dep(:grpc),
      common_dep(:minirest),
      common_dep(:ecpool),
      common_dep(:replayq),
      # maybe forbid to fetch quicer
      common_dep(:emqtt),
      common_dep(:rulesql),
      common_dep(:telemetry),
      # in conflict by emqtt and hocon
      common_dep(:getopt),
      common_dep(:snabbkaffe),
      common_dep(:hocon),
      common_dep(:emqx_http_lib),
      common_dep(:jose),
      # in conflict by ehttpc and emqtt
      common_dep(:gun),
      # in conflict by emqx_connector and system_monitor
      common_dep(:epgsql),
      # in conflict by emqx and observer_cli
      {:recon, github: "ferd/recon", tag: "2.5.6", override: true},
      common_dep(:jsx),
      # in conflict by erlavro and rocketmq
      common_dep(:jsone),
      # dependencies of dependencies; we choose specific refs to match
      # what rebar3 chooses.
      # in conflict by gun and emqtt
      common_dep(:cowlib),
      # in conflict by cowboy_swagger and cowboy
      common_dep(:ranch),
      # in conflict by grpc and eetcd
      common_dep(:gpb),
      {:hackney, github: "emqx/hackney", tag: "1.18.1-1", override: true},
      # set by hackney (dependency)
      {:ssl_verify_fun, "1.1.7", override: true},
      common_dep(:bcrypt),
      common_dep(:uuid),
      {:quickrand, github: "okeuday/quickrand", tag: "v2.0.6", override: true},
      common_dep(:ra),
      {:mimerl, "1.2.0", override: true},
      common_dep(:sasl_auth),
      # avlizer currently uses older :erlavro version
      common_dep(:erlavro),
      # in conflict by erlavro
      common_dep(:snappyer),
      common_dep(:crc32cer)
    ]
  end

  def extra_release_apps() do
    [
      common_dep(:redbug),
      common_dep(:observer_cli),
      common_dep(:system_monitor)
    ]
  end

  def common_dep(dep_name, overrides) do
    case common_dep(dep_name) do
      {^dep_name, opts} ->
        {dep_name, Keyword.merge(opts, overrides)}

      {^dep_name, tag, opts} when is_binary(tag) ->
        {dep_name, tag, Keyword.merge(opts, overrides)}
    end
  end

  def common_dep(:ekka), do: {:ekka, github: "emqx/ekka", tag: "0.19.7", override: true}
  def common_dep(:esockd), do: {:esockd, github: "emqx/esockd", tag: "5.13.0", override: true}
  def common_dep(:gproc), do: {:gproc, github: "emqx/gproc", tag: "0.9.0.1", override: true}
  def common_dep(:hocon), do: {:hocon, github: "emqx/hocon", tag: "0.43.4", override: true}
  def common_dep(:lc), do: {:lc, github: "emqx/lc", tag: "0.3.3", override: true}
  # in conflict by ehttpc and emqtt
  def common_dep(:gun), do: {:gun, github: "emqx/gun", tag: "1.3.11", override: true}
  # in conflict by cowboy_swagger and cowboy
  def common_dep(:ranch), do: {:ranch, github: "emqx/ranch", tag: "1.8.1-emqx", override: true}

  def common_dep(:ehttpc),
    do: {:ehttpc, github: "emqx/ehttpc", tag: "0.6.0", override: true}

  def common_dep(:jiffy), do: {:jiffy, github: "emqx/jiffy", tag: "1.0.6", override: true}

  def common_dep(:grpc),
    do:
      {:grpc,
       github: "emqx/grpc-erl", tag: "0.6.12", override: true, system_env: emqx_app_system_env()}

  def common_dep(:cowboy), do: {:cowboy, github: "emqx/cowboy", tag: "2.9.2", override: true}
  def common_dep(:jsone), do: {:jsone, github: "emqx/jsone", tag: "1.7.1", override: true}
  def common_dep(:ecpool), do: {:ecpool, github: "emqx/ecpool", tag: "0.6.1", override: true}
  def common_dep(:replayq), do: {:replayq, github: "emqx/replayq", tag: "0.3.10", override: true}
  def common_dep(:jsx), do: {:jsx, github: "talentdeficit/jsx", tag: "v3.1.0", override: true}
  # in conflict by emqtt and hocon
  def common_dep(:getopt), do: {:getopt, "1.0.2", override: true}
  def common_dep(:telemetry), do: {:telemetry, "1.3.0", override: true}
  # in conflict by grpc and eetcd
  def common_dep(:gpb), do: {:gpb, "4.21.1", override: true, runtime: false}
  def common_dep(:ra), do: {:ra, "2.15.0", override: true}

  # in conflict by emqx_connector and system_monitor
  def common_dep(:epgsql), do: {:epgsql, github: "emqx/epgsql", tag: "4.7.1.3", override: true}
  def common_dep(:sasl_auth), do: {:sasl_auth, "2.3.3", override: true}
  def common_dep(:gen_rpc), do: {:gen_rpc, github: "emqx/gen_rpc", tag: "3.4.1", override: true}

  def common_dep(:system_monitor),
    do: {:system_monitor, github: "ieQu1/system_monitor", tag: "3.0.5"}

  def common_dep(:uuid), do: {:uuid, github: "okeuday/uuid", tag: "v2.0.6", override: true}
  def common_dep(:redbug), do: {:redbug, github: "emqx/redbug", tag: "2.0.10"}
  def common_dep(:observer_cli), do: {:observer_cli, "1.8.0"}

  def common_dep(:jose),
    do: {:jose, github: "potatosalad/erlang-jose", tag: "1.11.2", override: true}

  def common_dep(:rulesql), do: {:rulesql, github: "emqx/rulesql", tag: "0.2.1"}

  def common_dep(:bcrypt),
    do: {:bcrypt, github: "emqx/erlang-bcrypt", tag: "0.6.2", override: true}

  def common_dep(:minirest),
    do: {:minirest, github: "emqx/minirest", tag: "1.4.4", override: true}

  # maybe forbid to fetch quicer
  def common_dep(:emqtt),
    do:
      {:emqtt,
       github: "emqx/emqtt", tag: "1.13.5", override: true, system_env: maybe_no_quic_env()}

  def common_dep(:typerefl),
    do: {:typerefl, github: "ieQu1/typerefl", tag: "0.9.1", override: true}

  def common_dep(:rocksdb),
    do: {:rocksdb, github: "emqx/erlang-rocksdb", tag: "1.8.0-emqx-6", override: true}

  def common_dep(:emqx_http_lib),
    do: {:emqx_http_lib, github: "emqx/emqx_http_lib", tag: "0.5.3", override: true}

  def common_dep(:cowlib),
    do:
      {:cowlib,
       github: "ninenines/cowlib", ref: "c6553f8308a2ca5dcd69d845f0a7d098c40c3363", override: true}

  def common_dep(:snabbkaffe),
    do: {
      :snabbkaffe,
      ## without this, snabbkaffe is compiled with `-define(snk_kind, '$kind')`, which
      ## will basically make events in tests never match any predicates.
      github: "kafka4beam/snabbkaffe",
      tag: "1.0.10",
      override: true,
      system_env: emqx_app_system_env()
    }

  def common_dep(:influxdb),
    do: {:influxdb, github: "emqx/influxdb-client-erl", tag: "1.1.13", override: true}

  def common_dep(:wolff), do: {:wolff, "4.0.4"}
  def common_dep(:brod_gssapi), do: {:brod_gssapi, "0.1.3"}

  def common_dep(:kafka_protocol),
    do: {:kafka_protocol, "4.1.10", override: true}

  def common_dep(:brod), do: {:brod, "4.3.1"}
  ## TODO: remove `mix.exs` from `wolff` and remove this override
  ## TODO: remove `mix.exs` from `pulsar` and remove this override
  def common_dep(:snappyer), do: {:snappyer, "1.2.10", override: true}
  def common_dep(:crc32cer), do: {:crc32cer, "0.1.11", override: true}
  def common_dep(:jesse), do: {:jesse, github: "emqx/jesse", tag: "1.8.1.1"}

  def common_dep(:erlavro) do
    {:erlavro, github: "emqx/erlavro", tag: "2.10.2-emqx-1", override: true}
  end

  ###############################################################################################
  # BEGIN DEPRECATED FOR MIX BLOCK
  # These should be removed once we fully migrate to mix
  ###############################################################################################

  defp emqx_apps(profile_info, version) do
    apps = umbrella_apps(profile_info) ++ enterprise_apps(profile_info)
    set_emqx_app_system_env(apps, profile_info, version)
  end

  defp umbrella_apps(profile_info = %{release_type: release_type}) do
    enterprise_apps = enterprise_umbrella_apps(release_type)
    excluded_apps = excluded_apps(release_type)

    "apps/*"
    |> Path.wildcard()
    |> Enum.map(fn path ->
      app =
        path
        |> Path.basename()
        |> String.to_atom()

      {app, path: path, manager: :rebar3, override: true}
    end)
    |> Enum.reject(fn dep_spec ->
      dep_spec
      |> elem(0)
      |> then(&MapSet.member?(enterprise_apps, &1))
    end)
    |> Enum.reject(fn {app, _} ->
      case profile_info do
        %{edition_type: :enterprise} ->
          app == :emqx_telemetry

        _ ->
          false
      end
    end)
    |> Enum.reject(fn {app, _} -> app == :emqx_mix_utils end)
    |> Enum.reject(fn {app, _} -> app in excluded_apps end)
  end

  defp enterprise_apps(_profile_info = %{release_type: release_type, edition_type: :enterprise}) do
    Enum.map(enterprise_umbrella_apps(release_type), fn app_name ->
      path = "apps/#{app_name}"
      {app_name, path: path, manager: :rebar3, override: true}
    end)
  end

  defp enterprise_apps(_profile_info) do
    []
  end

  # need to remove those when listing `/apps/`...
  defp enterprise_umbrella_apps(:standard) do
    MapSet.new([
      :emqx_connector_aggregator,
      :emqx_bridge_kafka,
      :emqx_bridge_confluent,
      :emqx_bridge_gcp_pubsub,
      :emqx_bridge_cassandra,
      :emqx_bridge_opents,
      :emqx_bridge_dynamo,
      :emqx_bridge_es,
      :emqx_bridge_greptimedb,
      :emqx_bridge_hstreamdb,
      :emqx_bridge_influxdb,
      :emqx_bridge_iotdb,
      :emqx_bridge_matrix,
      :emqx_bridge_mongodb,
      :emqx_bridge_mysql,
      :emqx_bridge_pgsql,
      :emqx_bridge_redis,
      :emqx_bridge_rocketmq,
      :emqx_bridge_tdengine,
      :emqx_bridge_timescale,
      :emqx_bridge_sqlserver,
      :emqx_bridge_pulsar,
      :emqx_oracle,
      :emqx_bridge_oracle,
      :emqx_bridge_rabbitmq,
      :emqx_bridge_clickhouse,
      :emqx_ft,
      :emqx_license,
      :emqx_opentelemetry,
      :emqx_s3,
      :emqx_bridge_s3,
      :emqx_bridge_azure_blob_storage,
      :emqx_bridge_couchbase,
      :emqx_bridge_snowflake,
      :emqx_schema_registry,
      :emqx_schema_validation,
      :emqx_message_transformation,
      :emqx_enterprise,
      :emqx_bridge_kinesis,
      :emqx_bridge_azure_event_hub,
      :emqx_gcp_device,
      :emqx_dashboard_rbac,
      :emqx_dashboard_sso,
      :emqx_audit,
      :emqx_gateway_gbt32960,
      :emqx_gateway_ocpp,
      :emqx_gateway_jt808,
      :emqx_bridge_syskeeper,
      :emqx_ds_shared_sub,
      :emqx_auth_ext,
      :emqx_cluster_link,
      :emqx_ds_builtin_raft,
      :emqx_auth_kerberos,
      :emqx_bridge_datalayers,
      :emqx_auth_cinfo
    ])
  end

  defp enterprise_umbrella_apps(:platform) do
    MapSet.union(
      enterprise_umbrella_apps(:standard),
      MapSet.new([
        :emqx_fdb_ds,
        :emqx_fdb_cli,
        :emqx_fdb_management,
        :emqx_event_history,
        :emqx_ds_fdb_backend
      ])
    )
  end

  defp enterprise_deps(_profile_info = %{edition_type: :enterprise}) do
    [
      {:hstreamdb_erl,
       github: "hstreamdb/hstreamdb_erl", tag: "0.5.18+v0.18.1+ezstd-v1.0.5-emqx1"},
      common_dep(:influxdb),
      common_dep(:wolff),
      common_dep(:kafka_protocol),
      common_dep(:brod_gssapi),
      common_dep(:brod),
      common_dep(:snappyer),
      common_dep(:crc32cer),
      {:opentsdb, github: "emqx/opentsdb-client-erl", tag: "v0.5.1", override: true},
      {:greptimedb,
       github: "GreptimeTeam/greptimedb-ingester-erl", tag: "v0.1.8", override: true},
      # The following two are dependencies of rabbit_common. They are needed here to
      # make mix not complain about conflicting versions
      {:thoas, github: "emqx/thoas", tag: "v1.0.0", override: true},
      {:credentials_obfuscation,
       github: "emqx/credentials-obfuscation", tag: "v3.2.0", override: true},
      {:rabbit_common,
       github: "emqx/rabbitmq-server",
       tag: "v3.11.13.2",
       sparse: "deps/rabbit_common",
       override: true},
      {:amqp_client,
       github: "emqx/rabbitmq-server",
       tag: "v3.11.13.2",
       sparse: "deps/amqp_client",
       override: true}
    ]
  end

  defp enterprise_deps(_profile_info) do
    []
  end

  defp set_emqx_app_system_env(apps, profile_info, version) do
    system_env = emqx_app_system_env(profile_info, version) ++ maybe_no_quic_env()

    Enum.map(
      apps,
      fn {app, opts} ->
        {app,
         Keyword.update(
           opts,
           :system_env,
           system_env,
           &Keyword.merge(&1, system_env)
         )}
      end
    )
  end

  def emqx_app_system_env(profile_info, version) do
    erlc_options(profile_info, version)
    |> dump_as_erl()
    |> then(&[{"ERL_COMPILER_OPTIONS", &1}])
  end

  def emqx_app_system_env() do
    k = {__MODULE__, :emqx_app_system_env}

    get_memoized(k, fn ->
      emqx_app_system_env(profile_info(), pkg_vsn())
    end)
  end

  ###############################################################################################
  # END DEPRECATED FOR MIX BLOCK
  ###############################################################################################

  defp erlc_options(%{edition_type: edition_type}, version) do
    [
      :debug_info,
      {:compile_info, [{:emqx_vsn, String.to_charlist(version)}]},
      {:d, :EMQX_RELEASE_EDITION, erlang_edition(edition_type)},
      {:d, :EMQX_ELIXIR},
      {:d, :EMQX_FLAVOR, get_emqx_flavor()},
      {:d, :snk_kind, :msg}
    ] ++
      singleton(test_env?(), {:d, :TEST}) ++
      singleton(not enable_quicer?(), {:d, :BUILD_WITHOUT_QUIC}) ++
      singleton(store_state_in_ds?(), {:d, :STORE_STATE_IN_DS, true})
  end

  defp store_state_in_ds?() do
    "1" == System.get_env("STORE_STATE_IN_DS")
  end

  defp singleton(false, _value), do: []
  defp singleton(true, value), do: [value]

  def profile_info() do
    k = {__MODULE__, :profile_info}
    get_memoized(k, &check_profile!/0)
  end

  def pkg_vsn() do
    k = {__MODULE__, :pkg_vsn}
    get_memoized(k, &do_pkg_vsn/0)
  end

  def common_deps() do
    if test_env?() do
      [
        {:bbmustache, "1.10.0"},
        {:cth_readable, "1.5.1"},
        {:proper, "1.4.0"},
        {:meck, "0.9.2"}
      ]
    else
      []
    end
  end

  def extra_applications() do
    k = {__MODULE__, :extra_applications}

    get_memoized(k, fn ->
      if test_env?() do
        [:eunit, :common_test, :dialyzer, :mnesia]
      else
        []
      end
    end)
  end

  def erlc_paths() do
    k = {__MODULE__, :erlc_paths}

    get_memoized(k, fn ->
      if test_env?() do
        ["src", "test"]
      else
        ["src"]
      end
    end)
  end

  def erlc_options() do
    k = {__MODULE__, :erlc_options}

    get_memoized(k, fn ->
      profile_info = profile_info()
      version = pkg_vsn()
      erlc_options(profile_info, version)
    end)
  end

  def test_env?() do
    k = {__MODULE__, :test_env?}

    get_memoized(k, fn ->
      env = to_string(Mix.env())
      System.get_env("TEST") == "1" || env =~ ~r/-test$/
    end)
  end

  defp set_test_env!(test_env?) do
    k = {__MODULE__, :test_env?}
    :persistent_term.put(k, test_env?)
  end

  defp get_memoized(k, compute_fn) do
    case :persistent_term.get(k, :undefined) do
      :undefined ->
        res = compute_fn.()
        :persistent_term.put(k, res)
        res

      res ->
        res
    end
  end

  def maybe_no_quic_env() do
    if not enable_quicer?() do
      [{"BUILD_WITHOUT_QUIC", "true"}]
    else
      []
    end
  end

  defp releases() do
    [
      emqx: fn ->
        %{
          release_type: release_type,
          package_type: package_type,
          edition_type: edition_type
        } = check_profile!()

        base_steps = [
          &merge_config/1,
          &make_docs/1,
          :assemble,
          &create_RELEASES/1,
          &copy_files(&1, release_type, package_type, edition_type),
          &copy_escript(&1, "nodetool"),
          &copy_escript(&1, "install_upgrade.escript")
        ]

        steps =
          if System.get_env("ELIXIR_MAKE_TAR") == "yes" do
            base_steps ++ [&prepare_tar_overlays/1, :tar]
          else
            base_steps
          end

        [
          applications: applications(release_type, edition_type),
          skip_mode_validation_for: [
            :lc,
            :emqx_mix,
            :emqx_machine,
            :emqx_gateway,
            :emqx_gateway_stomp,
            :emqx_gateway_mqttsn,
            :emqx_gateway_coap,
            :emqx_gateway_lwm2m,
            :emqx_gateway_exproto,
            :emqx_dashboard,
            :emqx_dashboard_sso,
            :emqx_audit,
            :emqx_resource,
            :emqx_connector,
            :emqx_exhook,
            :emqx_bridge,
            :emqx_bridge_mqtt,
            :emqx_modules,
            :emqx_management,
            :emqx_retainer,
            :emqx_prometheus,
            :emqx_rule_engine,
            :emqx_auto_subscribe,
            :emqx_slow_subs,
            :emqx_plugins,
            :emqx_ft,
            :emqx_s3,
            :emqx_opentelemetry,
            :emqx_durable_storage,
            :emqx_ds_builtin_local,
            :emqx_ds_builtin_raft,
            :rabbit_common,
            :emqx_eviction_agent,
            :emqx_node_rebalance
          ],
          steps: steps,
          strip_beams: false
        ]
      end
    ]
  end

  def applications(release_type, edition_type) do
    {:ok,
     [
       %{
         db_apps: db_apps,
         system_apps: system_apps,
         common_business_apps: common_business_apps,
         ee_business_apps: ee_business_apps,
         ce_business_apps: ce_business_apps
       }
     ]} = :file.consult("apps/emqx_machine/priv/reboot_lists.eterm")

    edition_specific_apps =
      if edition_type == :enterprise do
        ee_business_apps
      else
        ce_business_apps
      end

    business_apps = common_business_apps ++ edition_specific_apps

    excluded_apps = excluded_apps(release_type)

    system_apps =
      Enum.map(system_apps, fn app ->
        if is_atom(app), do: {app, :permanent}, else: app
      end)

    db_apps = Enum.map(db_apps, &{&1, :load})
    business_apps = Enum.map(business_apps, &{&1, :load})

    [system_apps, db_apps, [emqx_ctl: :permanent, emqx_machine: :permanent], business_apps]
    |> List.flatten()
    |> Keyword.reject(fn {app, _type} ->
      app in excluded_apps ||
        (edition_type == :enterprise && app == :emqx_telemetry)
    end)
  end

  defp excluded_apps(:standard) do
    %{
      mnesia_rocksdb: enable_rocksdb?(),
      quicer: enable_quicer?(),
      jq: enable_jq?(),
      observer: is_app?(:observer),
      emqx_fdb_ds: false,
      emqx_fdb_cli: false,
      emqx_fdb_management: false,
      emqx_event_history: false,
      emqx_ds_fdb_backend: false
    }
    |> Enum.reject(&elem(&1, 1))
    |> Enum.map(&elem(&1, 0))
  end

  defp excluded_apps(:platform) do
    %{
      mnesia_rocksdb: enable_rocksdb?(),
      quicer: enable_quicer?(),
      jq: enable_jq?(),
      observer: is_app?(:observer)
    }
    |> Enum.reject(&elem(&1, 1))
    |> Enum.map(&elem(&1, 0))
  end

  defp is_app?(name) do
    case Application.load(name) do
      :ok ->
        true

      {:error, {:already_loaded, _}} ->
        true

      _ ->
        false
    end
  end

  def check_profile!() do
    valid_envs = [
      :emqx,
      :"emqx-test",
      :"emqx-pkg",
      :"emqx-enterprise",
      :"emqx-enterprise-test",
      :"emqx-enterprise-pkg"
    ]

    if Mix.env() == :dev do
      env_profile = System.get_env("PROFILE")

      if env_profile do
        # copy from PROFILE env var
        System.get_env("PROFILE")
        |> String.to_atom()
        |> Mix.env()
      else
        Mix.shell().info([
          :yellow,
          "Warning: env var PROFILE is unset; defaulting to emqx"
        ])

        Mix.env(:emqx)
      end
    end

    if Mix.env() not in valid_envs do
      formatted_envs =
        valid_envs
        |> Enum.map(&"  * #{&1}")
        |> Enum.join("\n")

      Mix.raise("""
      Invalid env #{Mix.env()}.  Valid options are:
      #{formatted_envs}
      """)
    end

    mix_env = Mix.env()

    {
      release_type,
      package_type,
      edition_type
    } =
      case mix_env do
        :dev ->
          {:standard, :bin, :community}

        :emqx ->
          {:standard, :bin, :community}

        :"emqx-test" ->
          {:standard, :bin, :community}

        :"emqx-enterprise" ->
          {:standard, :bin, :enterprise}

        :"emqx-enterprise-test" ->
          {:standard, :bin, :enterprise}

        :"emqx-pkg" ->
          {:standard, :pkg, :community}

        :"emqx-enterprise-pkg" ->
          {:standard, :pkg, :enterprise}
      end

    test? = to_string(mix_env) =~ ~r/-test$/ || test_env?()

    normalize_env!(test?)

    # Mix.debug(true)

    if Mix.debug?() do
      Mix.shell().info([
        :blue,
        "mix_env: #{Mix.env()}",
        "; release type: #{release_type}",
        "; package type: #{package_type}",
        "; edition type: #{edition_type}",
        "; test env?: #{test?}"
      ])
    end

    test? = to_string(mix_env) =~ ~r/-test$/ || test_env?()

    normalize_env!(test?)

    # Mix.debug(true)

    if Mix.debug?() do
      Mix.shell().info([
        :blue,
        "mix_env: #{Mix.env()}",
        "; release type: #{release_type}",
        "; package type: #{package_type}",
        "; edition type: #{edition_type}",
        "; test env?: #{test?}"
      ])
    end

    %{
      release_type: release_type,
      package_type: package_type,
      edition_type: edition_type,
      test?: test?
    }
  end

  #############################################################################
  #  Custom Steps
  #############################################################################

  # Gathers i18n files and merge them before producing docs and schemas.
  defp merge_config(release) do
    {_, 0} = System.cmd("bash", ["-c", "./scripts/merge-config.escript"])
    release
  end

  defp make_docs(release) do
    profile = System.get_env("MIX_ENV")
    os_cmd("build", [profile, "docs"])
    release
  end

  defp copy_files(release, release_type, package_type, edition_type) do
    overwrite? = Keyword.get(release.options, :overwrite, false)

    bin = Path.join(release.path, "bin")
    etc = Path.join(release.path, "etc")
    log = Path.join(release.path, "log")
    plugins = Path.join(release.path, "plugins")

    Mix.Generator.create_directory(bin)
    Mix.Generator.create_directory(etc)
    Mix.Generator.create_directory(log)
    Mix.Generator.create_directory(plugins)
    Mix.Generator.create_directory(Path.join(etc, "certs"))

    Enum.each(
      ["mnesia", "configs", "patches", "scripts"],
      fn dir ->
        path = Path.join([release.path, "data", dir])
        Mix.Generator.create_directory(path)
      end
    )

    Mix.Generator.copy_file(
      "apps/emqx_auth/etc/acl.conf",
      Path.join(etc, "acl.conf"),
      force: overwrite?
    )

    # required by emqx_auth
    File.cp_r!(
      "apps/emqx/etc/certs",
      Path.join(etc, "certs")
    )

    profile = System.get_env("MIX_ENV")

    File.cp_r!(
      "rel/config/examples",
      Path.join(etc, "examples"),
      force: overwrite?
    )

    # copy /rel/config/ee-examples if profile is enterprise
    case profile do
      "emqx-enterprise" ->
        File.cp_r!(
          "rel/config/ee-examples",
          Path.join(etc, "examples"),
          force: overwrite?
        )

      _ ->
        :ok
    end

    # this is required by the produced escript / nodetool
    Mix.Generator.copy_file(
      Path.join(release.version_path, "start_clean.boot"),
      Path.join(bin, "no_dot_erlang.boot"),
      force: overwrite?
    )

    assigns = template_vars(release, release_type, package_type, edition_type)

    # This is generated by `scripts/merge-config.escript` or `make merge-config`
    # So, this should be run before the release.
    # TODO: run as a "compiler" step???
    render_template(
      "apps/emqx_conf/etc/emqx.conf.all",
      assigns,
      Path.join(etc, "emqx.conf")
    )

    render_template(
      "apps/emqx_conf/etc/base.hocon",
      assigns,
      Path.join(etc, "base.hocon")
    )

    render_template(
      "rel/emqx_vars",
      assigns,
      Path.join([release.path, "releases", "emqx_vars"])
    )

    vm_args_template_path =
      case release_type do
        _ ->
          "apps/emqx/etc/vm.args.cloud"
      end

    render_template(
      vm_args_template_path,
      assigns,
      [
        Path.join(etc, "vm.args"),
        Path.join(release.version_path, "vm.args")
      ]
    )

    for name <- [
          "emqx",
          "emqx_ctl"
        ] do
      Mix.Generator.copy_file(
        "bin/#{name}",
        Path.join(bin, name),
        force: overwrite?
      )

      # Files with the version appended are expected by the release
      # upgrade script `install_upgrade.escript`
      Mix.Generator.copy_file(
        Path.join(bin, name),
        Path.join(bin, name <> "-#{release.version}"),
        force: overwrite?
      )
    end

    for base_name <- ["emqx", "emqx_ctl"],
        suffix <- ["", "-#{release.version}"] do
      name = base_name <> suffix
      File.chmod!(Path.join(bin, name), 0o755)
    end

    Mix.Generator.copy_file(
      "bin/node_dump",
      Path.join(bin, "node_dump"),
      force: overwrite?
    )

    File.chmod!(Path.join(bin, "node_dump"), 0o755)

    Mix.Generator.copy_file(
      "bin/emqx_cluster_rescue",
      Path.join(bin, "emqx_cluster_rescue"),
      force: overwrite?
    )

    File.chmod!(Path.join(bin, "emqx_cluster_rescue"), 0o755)

    render_template(
      "rel/BUILD_INFO",
      assigns,
      Path.join(release.version_path, "BUILD_INFO")
    )

    release
  end

  defp render_template(template, assigns, target) when is_binary(target) do
    render_template(template, assigns, [target])
  end

  defp render_template(template, assigns, tartgets) when is_list(tartgets) do
    rendered =
      File.read!(template)
      |> from_rebar_to_eex_template()
      |> EEx.eval_string(assigns)

    for target <- tartgets do
      File.write!(target, rendered)
    end
  end

  # needed by nodetool and by release_handler
  defp create_RELEASES(release) do
    apps =
      Enum.map(release.applications, fn {app_name, app_props} ->
        app_vsn = Keyword.fetch!(app_props, :vsn)

        app_path =
          "./lib"
          |> Path.join("#{app_name}-#{app_vsn}")
          |> to_charlist()

        {app_name, app_vsn, app_path}
      end)

    release_entry = [
      {
        :release,
        to_charlist(release.name),
        to_charlist(release.version),
        release.erts_version,
        apps,
        :permanent
      }
    ]

    release.path
    |> Path.join("releases")
    |> Path.join("RELEASES")
    |> File.open!([:write, :utf8], fn handle ->
      IO.puts(handle, "%% coding: utf-8")
      :io.format(handle, ~c"~tp.~n", [release_entry])
    end)

    release
  end

  defp copy_escript(release, escript_name) do
    [shebang, rest] =
      "bin/#{escript_name}"
      |> File.read!()
      |> String.split("\n", parts: 2)

    # the elixir version of escript + start.boot required the boot_var
    # RELEASE_LIB to be defined.
    # enable-feature is not required when 1.6.x
    boot_var = "%%!-boot_var RELEASE_LIB $RUNNER_ROOT_DIR/lib -enable-feature maybe_expr"

    # Files with the version appended are expected by the release
    # upgrade script `install_upgrade.escript`
    Enum.each(
      [escript_name, escript_name <> "-" <> release.version],
      fn name ->
        path = Path.join([release.path, "bin", name])
        File.write!(path, [shebang, "\n", boot_var, "\n", rest])
      end
    )

    release
  end

  # The `:tar` built-in step in Mix Release does not currently add the
  # `etc` directory into the resulting tarball.  The workaround is to
  # add those to the `:overlays` key before running `:tar`.
  # See: https://hexdocs.pm/mix/1.13.4/Mix.Release.html#__struct__/0
  defp prepare_tar_overlays(release) do
    Map.update!(
      release,
      :overlays,
      &[
        "etc",
        "data",
        "plugins",
        "bin/node_dump"
        | &1
      ]
    )
  end

  #############################################################################
  #  Helper functions
  #############################################################################

  defp template_vars(release, release_type, :bin = _package_type, edition_type) do
    [
      emqx_default_erlang_cookie: default_cookie(),
      emqx_configuration_doc: emqx_configuration_doc(edition_type, :root),
      emqx_configuration_doc_log: emqx_configuration_doc(edition_type, :log),
      platform_data_dir: "data",
      platform_etc_dir: "etc",
      platform_plugins_dir: "plugins",
      runner_bin_dir: "$RUNNER_ROOT_DIR/bin",
      emqx_etc_dir: "$RUNNER_ROOT_DIR/etc",
      runner_lib_dir: "$RUNNER_ROOT_DIR/lib",
      runner_log_dir: "$RUNNER_ROOT_DIR/log",
      runner_user: "",
      release_version: release.version,
      erts_vsn: release.erts_version,
      # FIXME: this is empty in `make emqx` ???
      erl_opts: "",
      emqx_description: emqx_description(release_type, edition_type),
      emqx_schema_mod: emqx_schema_mod(edition_type),
      is_elixir: "yes",
      is_enterprise: if(edition_type == :enterprise, do: "yes", else: "no")
    ] ++ build_info()
  end

  defp template_vars(release, release_type, :pkg = _package_type, edition_type) do
    [
      emqx_default_erlang_cookie: default_cookie(),
      emqx_configuration_doc: emqx_configuration_doc(edition_type, :root),
      emqx_configuration_doc_log: emqx_configuration_doc(edition_type, :log),
      platform_data_dir: "/var/lib/emqx",
      platform_etc_dir: "/etc/emqx",
      platform_plugins_dir: "/var/lib/emqx/plugins",
      runner_bin_dir: "/usr/bin",
      emqx_etc_dir: "/etc/emqx",
      runner_lib_dir: "$RUNNER_ROOT_DIR/lib",
      runner_log_dir: "/var/log/emqx",
      runner_user: "emqx",
      release_version: release.version,
      erts_vsn: release.erts_version,
      # FIXME: this is empty in `make emqx` ???
      erl_opts: "",
      emqx_description: emqx_description(release_type, edition_type),
      emqx_schema_mod: emqx_schema_mod(edition_type),
      is_elixir: "yes",
      is_enterprise: if(edition_type == :enterprise, do: "yes", else: "no")
    ] ++ build_info()
  end

  defp default_cookie() do
    "emqx50elixir"
  end

  defp emqx_description(release_type, edition_type) do
    case {release_type, edition_type} do
      {_, :enterprise} ->
        case get_emqx_flavor() do
          :official ->
            "EMQX Enterprise"

          flavor ->
            "EMQX Enterprise(#{flavor})"
        end

      {_, :community} ->
        "EMQX"
    end
  end

  defp emqx_configuration_doc(:enterprise, :root),
    do: "https://docs.emqx.com/en/enterprise/latest/configuration/configuration.html"

  defp emqx_configuration_doc(:enterprise, :log),
    do: "https://docs.emqx.com/en/enterprise/latest/configuration/logs.html"

  defp emqx_configuration_doc(:community, :root),
    do: "https://www.emqx.io/docs/en/latest/configuration/configuration.html"

  defp emqx_configuration_doc(:community, :log),
    do: "https://www.emqx.io/docs/en/latest/configuration/logs.html"

  defp emqx_schema_mod(:enterprise), do: :emqx_enterprise_schema
  defp emqx_schema_mod(:community), do: :emqx_conf_schema

  def jq_dep() do
    if enable_jq?(),
      do: [{:jq, github: "emqx/jq", tag: "v0.3.12", override: true}],
      else: []
  end

  def quicer_dep() do
    if enable_quicer?(),
      # in conflict with emqx and emqtt
      do: [
        {:quicer, github: "emqx/quic", tag: "0.2.1", override: true}
      ],
      else: []
  end

  defp enable_jq?() do
    not Enum.any?([
      build_without_jq?()
    ])
  end

  def enable_quicer?() do
    "1" == System.get_env("BUILD_WITH_QUIC") or
      not build_without_quic?()
  end

  def get_emqx_flavor() do
    case System.get_env("EMQX_FLAVOR") do
      nil -> :official
      "" -> :official
      flavor -> String.to_atom(flavor)
    end
  end

  defp enable_rocksdb?() do
    not Enum.any?([
      raspbian?(),
      build_without_rocksdb?()
    ])
  end

  defp do_pkg_vsn() do
    %{edition_type: edition_type} = check_profile!()
    basedir = Path.dirname(__ENV__.file)
    script = Path.join(basedir, "pkg-vsn.sh")
    os_cmd(script, [Atom.to_string(edition_type)])
  end

  defp os_cmd(script, args) do
    {str, 0} = System.cmd("bash", [script | args])
    String.trim(str)
  end

  defp raspbian?() do
    os_cmd("./scripts/get-distro.sh", []) =~ "raspbian"
  end

  defp build_without_jq?() do
    opt = System.get_env("BUILD_WITHOUT_JQ", "false")

    String.downcase(opt) != "false"
  end

  def build_without_quic?() do
    opt = System.get_env("BUILD_WITHOUT_QUIC", "false")

    String.downcase(opt) != "false"
  end

  defp build_without_rocksdb?() do
    opt = System.get_env("BUILD_WITHOUT_ROCKSDB", "false")

    String.downcase(opt) != "false"
  end

  defp from_rebar_to_eex_template(str) do
    # we must not consider surrounding space in the template var name
    # because some help strings contain informative variables that
    # should not be interpolated, and those have no spaces.
    Regex.replace(
      ~r/\{\{ ([a-zA-Z0-9_]+) \}\}/,
      str,
      "<%= \\g{1} %>"
    )
  end

  defp build_info() do
    [
      build_info_arch: to_string(:erlang.system_info(:system_architecture)),
      build_info_wordsize: wordsize(),
      build_info_os: os_cmd("./scripts/get-distro.sh", []),
      build_info_erlang: otp_release(),
      build_info_elixir: System.version(),
      build_info_relform: System.get_env("EMQX_REL_FORM", "tgz")
    ]
  end

  # https://github.com/erlang/rebar3/blob/e3108ac187b88fff01eca6001a856283a3e0ec87/src/rebar_utils.erl#L142
  defp wordsize() do
    size =
      try do
        :erlang.system_info({:wordsize, :external})
      rescue
        ErlangError ->
          :erlang.system_info(:wordsize)
      end

    to_string(8 * size)
  end

  defp normalize_env!(test_env?) do
    env =
      case Mix.env() do
        :dev ->
          :emqx

        env ->
          env
      end

    if test_env? do
      ensure_test_mix_env!()
    end

    Mix.env(env)
  end

  # As from Erlang/OTP 17, the OTP release number corresponds to the
  # major OTP version number. No erlang:system_info() argument gives
  # the exact OTP version.
  # https://www.erlang.org/doc/man/erlang.html#system_info_otp_release
  # https://github.com/erlang/rebar3/blob/e3108ac187b88fff01eca6001a856283a3e0ec87/src/rebar_utils.erl#L572-L577
  defp otp_release() do
    major_version = System.otp_release()
    root_dir = to_string(:code.root_dir())

    [root_dir, "releases", major_version, "OTP_VERSION"]
    |> Path.join()
    |> File.read()
    |> case do
      {:error, _} ->
        major_version

      {:ok, version} ->
        version
        |> String.trim()
        |> String.split("**")
        |> List.first()
    end
  end

  defp dump_as_erl(term) do
    term
    |> then(&:io_lib.format("~0p", [&1]))
    |> :erlang.iolist_to_binary()
  end

  defp erlang_edition(:community), do: :ce
  defp erlang_edition(:enterprise), do: :ee

  defp aliases() do
    [
      ct: &do_ct/1,
      eunit: &do_eunit/1,
      proper: &do_proper/1,
      dialyzer: &do_dialyzer/1
    ]
  end

  defp do_ct(args) do
    IO.inspect(args)
    Mix.shell().info("testing")

    ensure_test_mix_env!()
    set_test_env!(true)

    Mix.Task.run("emqx.ct", args)
  end

  defp do_eunit(args) do
    ensure_test_mix_env!()
    set_test_env!(true)
    Mix.Task.run("emqx.eunit", args)
  end

  defp do_proper(args) do
    ensure_test_mix_env!()
    set_test_env!(true)
    Mix.Task.run("emqx.proper", args)
  end

  defp do_dialyzer(args) do
    Mix.Task.run("emqx.dialyzer", args)
  end

  defp ensure_test_mix_env!() do
    Mix.env()
    |> to_string()
    |> then(fn env ->
      if String.ends_with?(env, "-test") do
        env
      else
        env <> "-test"
      end
    end)
    |> String.to_atom()
    |> Mix.env()
  end
end
