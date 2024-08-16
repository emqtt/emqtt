%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(SHARED_SUBS_AGENT_HRL).
-define(SHARED_SUBS_AGENT_HRL, true).

-ifdef(EMQX_RELEASE_EDITION).

-if(?EMQX_RELEASE_EDITION == ee).

%% agent from BSL app

-define(shared_subs_agent, emqx_ds_shared_sub_agent).

%% clause of -if(?EMQX_RELEASE_EDITION == ee).
-else.

-define(shared_subs_agent, emqx_persistent_session_ds_shared_subs_null_agent).

%% end of -if(?EMQX_RELEASE_EDITION == ee).
-endif.

%% clause of -ifdef(EMQX_RELEASE_EDITION).
-else.

-define(shared_subs_agent, emqx_persistent_session_ds_shared_subs_null_agent).

%% end of -ifdef(EMQX_RELEASE_EDITION).
-endif.

-endif.
