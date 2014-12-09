.PHONY: all rel deps test apitest compile

REBAR=rebar

all: deps compile

compile:
	$(REBAR) compile

deps:
	$(REBAR) get-deps

clean:
	$(REBAR) clean

test:	mgrtest apitest unittest

unittest:
	$(REBAR) skip_deps=true eunit

mgrtest:
	./run_ct_for_tc.sh stor_mgr

apitest:
	./run_ct_for_tc.sh binapi
