-module(redis_interface).
-behaviour(gen_server).

%% API
-export([start_link/1, process_packet/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

start_link(_) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_Args) ->
    rets:start_link(),
    rets:init_db(db),
    udp_channel:start_link(10000, 1000, ?MODULE),
    {ok, #{}}.

handle_call(stop, _From, State) ->
   {stop, normal, stopped, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
   {noreply, State}.

handle_info({Sock, Addr, Port, Packet}, S) ->
    spawn(?MODULE, process_packet, [{Sock, Addr, Port, Packet}]),
    {noreply, S};

handle_info(_Info, State) ->
   {noreply, State}.

terminate(_Reason, _State) ->
   ok.

code_change(_OldVsn, State, _Extra) ->
   {ok, State}.

parse_net(Bin) when is_binary(Bin) ->
    parse_net(binary_to_list(Bin));
parse_net(L) when is_list(L) ->
    {ok, Tokens, _} = erl_scan:string(L),
    {ok, Value} = erl_parse:parse_term(Tokens),
    Value.

process_packet({Sock, Addr, Port, Packet}) ->
    V = parse_net(Packet),
    gen_udp:send(Sock, Addr, Port, to_bin(execute_cmd(V))).

execute_cmd(["SET", Key, Value]) ->
    rets:set(db, Key, Value);
execute_cmd(["GET", Key]) ->
    rets:get(db, Key);
execute_cmd(["HSET", Key, Field, Value]) ->
    rets:hset(db, Key, Field, Value);
execute_cmd(["HGET", Key, Field]) ->
    rets:hget(db, Key, Field);
execute_cmd(["HGETALL", Key]) ->
    rets:hgetall(db, Key);
execute_cmd(["DEL", Key]) ->
    rets:del(db, Key).    

to_bin(Num) when is_integer(Num) -> integer_to_binary(Num);
to_bin(Str) when is_list(Str) -> list_to_binary(Str);
to_bin(Bin) when is_binary(Bin) -> Bin;
to_bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8).

-ifdef(EUNIT).
-include_lib("eunit/include/eunit.hrl").

parse_net_test() ->
    V = parse_net(<<"[\"SET\", abc, {123,[],'122', #{p => q}}].">>),
    ?assert(V == ["SET", abc, {123,[],'122', #{p => q}}]),
    ok.

-endif.