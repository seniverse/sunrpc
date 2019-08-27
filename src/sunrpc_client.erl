%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%   http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.

-module(sunrpc_client).

-behaviour(gen_server).

-export([connect/3, close/1, call/5]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, format_status/2]).

connect(Address, Port, Options) ->
    gen_server:start_link(?MODULE, [Address, Port, Options], []).

close(Client) ->
    gen_server:call(Client, close).

call(Client, Prog, Vers, Proc, Args) ->
    case gen_server:call(
           Client,
           {call,
            Prog,
            Vers,
            Proc,
            #{flavor => 'AUTH_NONE',
              body => <<>>},
            #{flavor => 'AUTH_NONE',
              body => <<>>},
            Args})
    of
        {'MSG_ACCEPTED', #{reply_data := {'SUCCESS', Result}}} ->
            {ok, Result};
        {'MSG_ACCEPTED', #{reply_data := {'PROG_MISMATCH', Info}}} ->
            {error, {'PROG_MISMATCH', Info}};
        {'MSG_ACCEPTED', #{reply_data := {Stat, void}}} ->
            {error, Stat};
        {'MSG_DENIED', Error} ->
            {error, Error}
    end.

init([Address, Port, Options]) ->
    {ok, Socket} = gen_tcp:connect(Address, Port, [binary,{packet, sunrm}|Options]),
    {ok,
     #{socket => Socket,
       next_id => 1,
       calls => #{},
       acc => <<>>
      }
    }.

handle_call(
  {call, Prog, Vers, Proc, Cred, Verf, Args},
  From,
  State = #{socket := Socket, next_id := NextId, calls := Calls}) ->
    send(
      Socket,
      iolist_to_binary(
        [rpc_prot:encode_rpc_msg(
           #{xid => NextId,
             body =>
                 {'CALL',
                  #{rpcvers => 2,
                    prog => Prog,
                    vers => Vers,
                    proc => Proc,
                    cred => Cred,
                    verf => Verf
                   }
                 }
            }),
         Args])),

    {noreply,
     State#{next_id := NextId + 1,
            calls := Calls#{NextId => From}}};
handle_call(close, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.


handle_info({tcp, Socket, <<0:1, Len:31, Fragment:Len/binary>>}, State=#{socket := Socket, acc := Acc}) ->
    {noreply, State#{acc := <<Acc/binary, Fragment/binary>>}};
handle_info({tcp, Socket, <<1:1, Len:31, Fragment:Len/binary>>}, State=#{socket := Socket, calls := Calls, acc := Acc}) ->
    State1 = State#{acc := <<>>},
    {ok, #{xid := Xid, body := {'REPLY', Reply}}, Payload} =
        rpc_prot:decode_rpc_msg(<<Acc/binary, Fragment/binary>>),
    case maps:find(Xid, Calls) of
        error ->
            {noreply, State1};
        {ok, From} ->
            gen_server:reply(
              From,
              case Reply of
                  {'MSG_ACCEPTED', Data = #{reply_data := {'SUCCESS', <<>>}}} ->
                      {'MSG_ACCEPTED', Data#{reply_data := {'SUCCESS', Payload}}};
                  _ ->
                      Reply
              end),
            {noreply, State1#{calls := maps:remove(From, Calls)}}
    end;
handle_info({tcp_closed, Socket}, #{socket := Socket} = State) ->
    {stop, {shutdown, server}, maps:remove(socket, State)};
handle_info({tcp_error, Socket, Reason}, #{socket := Socket} = State) ->
    {stop, {error, {tcp_error, Reason}}, maps:remove(socket, State)};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, #{socket := Socket} = State) ->
    gen_tcp:close(Socket),
    terminate(Reason, maps:remove(socket, State));
terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, Status) ->
    Status.

fragment(<<Fragment:(16#7FFFFFFF)/binary, Rest/binary>>) when byte_size(Rest) > 0 ->
    [<<0:1, (16#7FFFFFFF):31>>, Fragment|fragment(Rest)];
fragment(Bin) ->
    [<<1:1, (byte_size(Bin)):31>>, Bin].

send(Socket, Bin)  ->
    gen_tcp:send(Socket, fragment(Bin)).
