-module(iroh_loopback_epmd).
-moduledoc """
Stateless EPMD replacement for running `inet_tcp` beside `iroh` on loopback, so
local tooling (`bin/<app> rpc`, remote shells) keeps working on a node whose
name is `name@<endpoint id>`. Every node is on 127.0.0.1 at the port in
`IROH_BEAM_LOOPBACK_PORT` (default 4370).

Node: `-proto_dist iroh inet_tcp -no_epmd -epmd_module iroh_loopback_epmd
-kernel inet_dist_use_interface {127,0,0,1}`.
Local client: `-proto_dist inet_tcp -epmd_module iroh_loopback_epmd
-start_epmd false -dist_listen false -hidden` (no `-no_epmd`, so OTP loads this
module through `start_link/0`).
""".

-export([start_link/0, register_node/2, register_node/3,
         listen_port_please/2, port_please/2, port_please/3,
         address_please/3, names/1]).

start_link() -> ignore.

register_node(Name, Port) -> register_node(Name, Port, inet).

%% A negative creation lets net_kernel pick one.
register_node(_Name, _Port, _Family) -> {ok, -1}.

listen_port_please(_Name, _Host) -> {ok, port()}.

port_please(Name, Ip) -> port_please(Name, Ip, infinity).

port_please(_Name, _Ip, _Timeout) -> {port, port(), 6}.

address_please(_Name, _Host, inet6) -> {ok, {0, 0, 0, 0, 0, 0, 0, 1}};
address_please(_Name, _Host, _Family) -> {ok, {127, 0, 0, 1}}.

names(_Host) -> {error, address}.

port() -> list_to_integer(os:getenv("IROH_BEAM_LOOPBACK_PORT", "4370")).
