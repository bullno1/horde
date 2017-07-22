-module(horde_ecdsa).
-behaviour(horde_crypto).
-export([
	init/1,
	info/2,
	generate_keypair/1,
	address_of/2,
	sign/3,
	verify/4,
	serialize/3,
	deserialize/3
]).

init(#{hash_algo := _, curve := _, address_size := _} = Opts) -> Opts.

info(#{hash_algo := HashAlgo, address_size := AddrSize}, max_address) ->
	Hash = crypto:hash(HashAlgo, <<>>),
	MaxHash = << <<255>> || <<_>> <= Hash>>,
	<<MaxAddress:AddrSize, _/binary>> = MaxHash,
	MaxAddress.

generate_keypair(#{curve := Curve}) ->
	crypto:generate_key(ecdh, Curve).

address_of(#{hash_algo := HashAlgo, address_size := AddrSize}, PublicKey) ->
	<<Address:AddrSize, _/binary>> = crypto:hash(HashAlgo, PublicKey),
	Address.

sign(#{hash_algo := HashAlgo, curve := Curve}, Message, PrivKey) ->
	crypto:sign(ecdsa, HashAlgo, Message, [PrivKey, Curve]).

verify(#{hash_algo := HashAlgo, curve := Curve}, Message, Sig, PubKey) ->
	crypto:verify(ecdsa, HashAlgo, Message, Sig, [PubKey, Curve]).

serialize(_, _, Obj) -> Obj.

deserialize(_, _, Obj) -> Obj.
