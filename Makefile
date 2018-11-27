.PHONY: plugins tests

PROJECT = emqx
PROJECT_DESCRIPTION = EMQ X Broker
PROJECT_VERSION = 3.0

DEPS = jsx gproc gen_rpc ekka esockd cowboy clique

dep_jsx     = hex-emqx 2.9.0
dep_gproc   = hex-emqx 0.8.0
dep_gen_rpc = git-emqx https://github.com/emqx/gen_rpc 2.3.0
dep_esockd  = git-emqx https://github.com/emqx/esockd v5.4.2
dep_ekka    = git-emqx https://github.com/emqx/ekka v0.5.1
dep_cowboy  = hex-emqx 2.4.0
dep_clique  = git-emqx https://github.com/emqx/clique develop

NO_AUTOPATCH = cuttlefish

ERLC_OPTS += +debug_info -DAPPLICATION=emqx

BUILD_DEPS = cuttlefish
dep_cuttlefish = git-emqx https://github.com/emqx/cuttlefish v2.1.1

#TEST_DEPS = emqx_ct_helplers
#dep_emqx_ct_helplers = git git@github.com:emqx/emqx-ct-helpers

TEST_ERLC_OPTS += +debug_info -DAPPLICATION=emqx

EUNIT_OPTS = verbose

# CT_SUITES = emqx_frame
## emqx_trie emqx_router emqx_frame emqx_mqtt_compat

CT_SUITES = emqx emqx_client emqx_zone emqx_banned emqx_session \
			emqx_access emqx_broker emqx_cm emqx_frame emqx_guid emqx_inflight emqx_json \
			emqx_keepalive emqx_lib emqx_metrics emqx_mod emqx_mod_sup emqx_mqtt_caps \
			emqx_mqtt_props emqx_mqueue emqx_net emqx_pqueue emqx_router emqx_sm \
			emqx_tables emqx_time emqx_topic emqx_trie emqx_vm emqx_mountpoint \
		 	emqx_listeners emqx_protocol emqx_pool emqx_shared_sub emqx_bridge emqx_hooks

CT_NODE_NAME = emqxct@127.0.0.1
CT_OPTS = -cover test/ct.cover.spec -erl_args -name $(CT_NODE_NAME)

COVER = true

PLT_APPS = sasl asn1 ssl syntax_tools runtime_tools crypto xmerl os_mon inets public_key ssl compiler mnesia
DIALYZER_DIRS := ebin/
DIALYZER_OPTS := --verbose --statistics -Werror_handling -Wrace_conditions #-Wunmatched_returns

define dep_fetch_git-emqx
	git clone -q --depth 1 -b $(call dep_commit,$(1)) -- $(call dep_repo,$(1)) $(DEPS_DIR)/$(call dep_name,$(1)) > /dev/null 2>&1; \
	cd $(DEPS_DIR)/$(call dep_name,$(1));
endef

core_http_get-emqx = curl -Lf$(if $(filter-out 0,$(V)),,s)o $(call core_native_path,$1) $2

define dep_fetch_hex-emqx
	mkdir -p $(ERLANG_MK_TMP)/hex $(DEPS_DIR)/$1; \
	$(call core_http_get-emqx,$(ERLANG_MK_TMP)/hex/$1.tar,\
		https://repo.hex.pm/tarballs/$1-$(strip $(word 2,$(dep_$1))).tar); \
	tar -xOf $(ERLANG_MK_TMP)/hex/$1.tar contents.tar.gz | tar -C $(DEPS_DIR)/$1 -xzf -;
endef

include erlang.mk

clean:: gen-clean

.PHONY: gen-clean
gen-clean:
	@rm -rf bbmustache
	@rm -f etc/gen.emqx.conf

bbmustache:
	$(verbose) git clone https://github.com/soranoba/bbmustache.git && cd bbmustache && ./rebar3 compile && cd ..

# This hack is to generate a conf file for testing
# relx overlay is used for release
etc/gen.emqx.conf: bbmustache etc/emqx.conf
	$(verbose) erl -noshell -pa bbmustache/_build/default/lib/bbmustache/ebin -eval \
		"{ok, Temp} = file:read_file('etc/emqx.conf'), \
		{ok, Vars0} = file:consult('vars'), \
		Vars = [{atom_to_list(N), list_to_binary(V)} || {N, V} <- Vars0], \
		Targ = bbmustache:render(Temp, Vars), \
		ok = file:write_file('etc/gen.emqx.conf', Targ), \
		halt(0)."

CUTTLEFISH_SCRIPT = _build/default/lib/cuttlefish/cuttlefish

app.config: $(CUTTLEFISH_SCRIPT) etc/gen.emqx.conf
	$(verbose) $(CUTTLEFISH_SCRIPT) -l info -e etc/ -c etc/gen.emqx.conf -i priv/emqx.schema -d data/

ct: app.config

rebar-cover:
	@rebar3 cover

coveralls:
	@rebar3 coveralls send


$(CUTTLEFISH_SCRIPT): rebar-deps
	@if [ ! -f cuttlefish ]; then make -C _build/default/lib/cuttlefish; fi

rebar-xref:
	@rebar3 xref

rebar-deps:
	@rebar3 get-deps

rebar-eunit: $(CUTTLEFISH_SCRIPT)
	@rebar3 eunit

rebar-compile:
	@rebar3 compile

rebar-ct: app.config
	@rebar3 as test compile
	@ln -s -f '../../../../etc' _build/test/lib/emqx/
	@ln -s -f '../../../../data' _build/test/lib/emqx/
	@rebar3 ct -v --readable=false --name $(CT_NODE_NAME) --suite=$(shell echo $(foreach var,$(CT_SUITES),test/$(var)_SUITE) | tr ' ' ',')

rebar-clean:
	@rebar3 clean

distclean::
	@rm -rf _build cover deps logs log data
	@rm -f rebar.lock compile_commands.json cuttlefish

## Below are for version consistency check during erlang.mk and rebar3 dual mode support
none=
space = $(none) $(none)
comma = ,
quote = \"
curly_l = "{"
curly_r = "}"
dep-versions = [$(foreach dep,$(DEPS) $(BUILD_DEPS),$(curly_l)$(dep),$(quote)$(word $(words $(dep_$(dep))),$(dep_$(dep)))$(quote)$(curly_r)$(comma))[]]

.PHONY: dep-vsn-check
dep-vsn-check:
	$(verbose) erl -noshell -eval \
		"MkVsns = lists:sort(lists:flatten($(dep-versions))), \
		{ok, Conf} = file:consult('rebar.config'), \
		{_, Deps1} = lists:keyfind(deps, 1, Conf), \
		{_, Deps2} = lists:keyfind(github_emqx_deps, 1, Conf), \
		F = fun({N, V}) when is_list(V) -> {N, V}; ({N, {git, _, {branch, V}}}) -> {N, V} end, \
		RebarVsns = lists:sort(lists:map(F, Deps1 ++ Deps2)), \
		case {RebarVsns -- MkVsns, MkVsns -- RebarVsns} of \
		  {[], []} -> halt(0); \
		  {Rebar, Mk} -> erlang:error({deps_version_discrepancy, [{rebar, Rebar}, {mk, Mk}]}) \
		end."
