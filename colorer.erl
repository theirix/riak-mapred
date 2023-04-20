-module(colorer).

-export([load/1]).
-export([colormap/3]).
-export([coloreduce/2]).
-export([get_color/1]).

%% Determine a cat color from its name
get_color(Name) ->
    NameTokens = string:tokens(Name, " "),
    case lists:member("White", NameTokens) of
        true ->
            "White";
        false ->
            case lists:member("Black", NameTokens) of
                true ->
                    "Black";
                false ->
                    "Mixed"
            end
    end.

%% map phase
colormap(Value, _Keydata, _Arg) ->
    ObjKey = riak_object:key(Value),
    KeyStr = binary_to_list(ObjKey),
    Color = get_color(KeyStr),
    error_logger:info_msg("colorer mapped key ~p to ~p", [KeyStr, Color]),
    [{Color, 1}].

%% map reduce
coloreduce(List, _Arg) ->
    error_logger:info_msg("colorer reduce list ~p", [List]),
    ListOfDicts = [dict:from_list([I]) || I <- List],
    Merged =
        lists:foldl(fun(Item, Acc) -> dict:merge(fun(_, X, Y) -> X + Y end, Item, Acc) end,
                    dict:new(),
                    ListOfDicts),
    % convert dict to list
    dict:to_list(Merged).

%% End of list
insert(_Client, []) ->
    ok;
%% Put a cat from head of list to the Riak
insert(Client, [Head | Tail]) ->
    % All cats are adorable, so it goes to the Riak value as a JSON
    Body = list_to_binary(mochijson2:encode(#{<<"adorable">> => true})),
    % Riak key is a cat name
    Key = Head,
    RawObj = riakc_obj:new(<<"cats">>, Key, Body),
    Obj = riakc_obj:update_content_type(RawObj, "application/json"),
    case riakc_pb_socket:put(Client, Obj) of
        ok ->
            io:format("Inserted key ~p~n", [Head]);
        {error, Error} ->
            io:format("Error inserting key ~p", [Error])
    end,

    insert(Client, Tail).

%% Load cats from text file
load(Filename) ->
    {ok, Client} = riakc_pb_socket:start_link("127.0.0.1", 10017),
    {ok, Data} = file:read_file(Filename),
    Lines = re:split(Data, "\r?\n", [{return, binary}, trim]),
    insert(Client, Lines).

%% vim: et tabstop=2 shiftwidth=2
