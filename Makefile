.PHONY: all rel deps test apitest compile

all: deps compile

compile:
	./rebar compile

deps:
	./rebar get-deps

clean:
	./rebar clean

test:	mgrtest apitest unittest

unittest:
	./rebar skip_deps=true eunit

mgrtest:
	./run_ct_for_tc.sh stor_mgr

apitest:
	./run_ct_for_tc.sh binapi
