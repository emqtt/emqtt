
Configuration
=============

TODO:...

Configuration files include:

+-------------------+-----------------------------------+
| File              | Description                       |
+-------------------+-----------------------------------+
| etc/vm.args       | Erlang VM Arguments               |
+-------------------+-----------------------------------+
| etc/app.config    | emqttd Broker Configuration       |
+-------------------+-----------------------------------+
| etc/acl.config    | ACL Rules Config                  |
+-------------------+-----------------------------------+
| etc/clients.config| Authentication with clientId      |
+-------------------+-----------------------------------+
| etc/ssl/*         | SSL certificate and key files     |
+-------------------+-----------------------------------+


etc/vm.args
-----------

Configure/Optimize the Erlang VM::

    ##-------------------------------------------------------------------------
    ## Name of the node
    ##-------------------------------------------------------------------------
    -name emqttd@127.0.0.1

    ## Cookie for distributed erlang
    -setcookie emqttdsecretcookie

    ##-------------------------------------------------------------------------
    ## Flags
    ##-------------------------------------------------------------------------

    ## Heartbeat management; auto-restarts VM if it dies or becomes unresponsive
    ## (Disabled by default..use with caution!)
    ##-heart
    -smp true

    ## Enable kernel poll and a few async threads
    +K true

    ## 12 threads/core.
    +A 48

    ## max process numbers
    +P 8192

    ## Sets the maximum number of simultaneously existing ports for this system
    +Q 8192

    ## max atom number
    ## +t

    ## Set the distribution buffer busy limit (dist_buf_busy_limit) in kilobytes.
    ## Valid range is 1-2097151. Default is 1024.
    ## +zdbbl 8192

    ## CPU Schedulers
    ## +sbt db

    ##-------------------------------------------------------------------------
    ## Env
    ##-------------------------------------------------------------------------

    ## Increase number of concurrent ports/sockets, deprecated in R17
    -env ERL_MAX_PORTS 8192

    -env ERTS_MAX_PORTS 8192

    -env ERL_MAX_ETS_TABLES 1024

    ## Tweak GC to run more often
    -env ERL_FULLSWEEP_AFTER 1000


.. NOTE:: +P Number > 2 * Max Connections


etc/app.config
--------------

TODO: The main configuration file for emqttd broker. Configure authentication, ACL, mqtt protocol parameters and listeners of the broker.

TODO: The file is erlang format.

Authentication and ACL::

    %% Authentication and Authorization
    {access, [
        %% Authetication. Anonymous Default
        {auth, [
            %% Authentication with username, password
            %{username, []},
            
            %% Authentication with clientid
            %{clientid, [{password, no}, {file, "etc/clients.config"}]},

            %% Authentication with LDAP
            % {ldap, [
            %    {servers, ["localhost"]},
            %    {port, 389},
            %    {timeout, 30},
            %    {user_dn, "uid=$u,ou=People,dc=example,dc=com"},
            %    {ssl, fasle},
            %    {sslopts, [
            %        {"certfile", "ssl.crt"},
            %        {"keyfile", "ssl.key"}]}
            % ]},

            %% Allow all
            {anonymous, []}
        ]},
        %% ACL config
        {acl, [
            %% Internal ACL module
            {internal,  [{file, "etc/acl.config"}, {nomatch, allow}]}
        ]}
    ]},


MQTT Packet, Client, Session, MQueue::

    {mqtt, [
        %% Packet
        {packet, [
            %% Max ClientId Length Allowed
            {max_clientid_len, 1024},
            %% Max Packet Size Allowed, 64K default
            {max_packet_size,  65536}
        ]},
        %% Client
        {client, [
            %% Socket is connected, but no 'CONNECT' packet received
            {idle_timeout, 20} %% seconds
            %TODO: Network ingoing limit
            %{ingoing_rate_limit, '64KB/s'}
            %TODO: Reconnet control
        ]},
        %% Session
        {session, [
            %% Max number of QoS 1 and 2 messages that can be “in flight” at one time.
            %% 0 means no limit
            {max_inflight, 100},

            %% Retry interval for redelivering QoS1/2 messages.
            {unack_retry_interval, 60},

            %% Awaiting PUBREL Timeout
            {await_rel_timeout, 20},

            %% Max Packets that Awaiting PUBREL, 0 means no limit
            {max_awaiting_rel, 0},

            %% Statistics Collection Interval(seconds)
            {collect_interval, 0},

            %% Expired after 2 days
            {expired_after, 48}

        ]},
        %% Session
        {queue, [
            %% Max queue length. enqueued messages when persistent client disconnected, 
            %% or inflight window is full.
            {max_length, 100},

            %% Low-water mark of queued messages
            {low_watermark, 0.2},

            %% High-water mark of queued messages
            {high_watermark, 0.6},

            %% Queue Qos0 messages?
            {queue_qos0, true}
        ]}
    ]},

Broker Options::

    {broker, [
        %% System interval of publishing broker $SYS messages
        {sys_interval, 60},

        %% Retained messages
        {retained, [
            %% Expired after seconds, never expired if 0
            {expired_after, 0},

            %% Max number of retained messages
            {max_message_num, 100000},

            %% Max Payload Size of retained message
            {max_playload_size, 65536}
        ]},

        %% PubSub and Router
        {pubsub, [
            %% Default should be scheduler numbers
            %% {pool_size, 8},
            
            %% Subscription: disc | ram | false
            {subscription, ram},

            %% Route shard
            {route_shard, false},

            %% Route delay, false | integer
            {route_delay, false},

            %% Route aging time(seconds)
            {route_aging, 5}
        ]},

        %% Bridge
        {bridge, [
            %%TODO: bridge queue size
            {max_queue_len, 10000},

            %% Ping Interval of bridge node
            {ping_down_interval, 1} %seconds
        ]}
    ]},

Extended Modules::

    {modules, [
        %% Client presence management module.
        %% Publish messages when client connected or disconnected
        {presence, [{qos, 0}]}

        %% Subscribe topics automatically when client connected
        %% {subscription, [
        %%    %% Subscription from stored table
        %%    stored,
        %%
        %%   %% $u will be replaced with username
        %%    {"$Q/username/$u", 1},
        %%
        %%   %% $c will be replaced with clientid
        %%    {"$Q/client/$c", 1}
        %% ]}

        %% Rewrite rules
        %% {rewrite, [{file, "etc/rewrite.config"}]}
    ]},

Listeners:: 

    {listeners, [
        {mqtt, 1883, [
            %% Size of acceptor pool
            {acceptors, 16},

            %% Maximum number of concurrent clients
            {max_clients, 8192},

            %% Socket Access Control
            {access, [{allow, all}]},

            %% Connection Options
            {connopts, [
                %% Rate Limit. Format is 'burst, rate', Unit is KB/Sec
                %% {rate_limit, "100,10"} %% 100K burst, 10K rate
            ]},

            %% Socket Options
            {sockopts, [
                %Set buffer if hight thoughtput
                %{recbuf, 4096},
                %{sndbuf, 4096},
                %{buffer, 4096},
                %{nodelay, true},
                {backlog, 1024}
            ]}
        ]},

        {mqtts, 8883, [
            %% Size of acceptor pool
            {acceptors, 4},

            %% Maximum number of concurrent clients
            {max_clients, 512},

            %% Socket Access Control
            {access, [{allow, all}]},

            %% SSL certificate and key files
            {ssl, [{certfile, "etc/ssl/ssl.crt"},
                   {keyfile,  "etc/ssl/ssl.key"}]},

            %% Socket Options
            {sockopts, [
                {backlog, 1024}
                %{buffer, 4096},
            ]}
        ]},
        %% WebSocket over HTTPS Listener
        %% {https, 8083, [
        %%  %% Size of acceptor pool
        %%  {acceptors, 4},
        %%  %% Maximum number of concurrent clients
        %%  {max_clients, 512},
        %%  %% Socket Access Control
        %%  {access, [{allow, all}]},
        %%  %% SSL certificate and key files
        %%  {ssl, [{certfile, "etc/ssl/ssl.crt"},
        %%         {keyfile,  "etc/ssl/ssl.key"}]},
        %%  %% Socket Options
        %%  {sockopts, [
        %%      %{buffer, 4096},
        %%      {backlog, 1024}
        %%  ]}
        %%]},

        %% HTTP and WebSocket Listener
        {http, 8083, [
            %% Size of acceptor pool
            {acceptors, 4},
            %% Maximum number of concurrent clients
            {max_clients, 64},
            %% Socket Access Control
            {access, [{allow, all}]},
            %% Socket Options
            {sockopts, [
                {backlog, 1024}
                %{buffer, 4096},
            ]}
        ]}
    ]},


etc/acl.config
--------------

Configuration file for ACL::

    %%%-----------------------------------------------------------------------------
    %%%
    %%% -type who() :: all | binary() |
    %%%                {ipaddr, esockd_access:cidr()} |
    %%%                {client, binary()} |
    %%%                {user, binary()}.
    %%%
    %%% -type access() :: subscribe | publish | pubsub.
    %%%
    %%% -type topic() :: binary().
    %%%
    %%% -type rule() :: {allow, all} |
    %%%                 {allow, who(), access(), list(topic())} |
    %%%                 {deny, all} |
    %%%                 {deny, who(), access(), list(topic())}.
    %%%
    %%%-----------------------------------------------------------------------------

    {allow, {user, "dashboard"}, subscribe, ["$SYS/#"]}.

    {allow, {ipaddr, "127.0.0.1"}, pubsub, ["$SYS/#", "#"]}.

    {deny, all, subscribe, ["$SYS/#", {eq, "#"}]}.

    {allow, all}.

.. NOTE:: Allow 'localhost' to pubsub '$SYS/#' and '#' by default.

etc/clients.config
------------------

TODO:

testclientid0
testclientid1 127.0.0.1
testclientid2 192.168.0.1/24



