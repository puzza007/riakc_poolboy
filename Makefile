.PHONY: deps test doc

all: deps compile

compile:
	rebar compile

deps:
	rebar get-deps

clean:
	rebar clean

distclean: clean 
	rebar delete-deps

test: 
	rebar skip_deps=true ct

dialyzer: compile
	@dialyzer -Wno_undefined_callbacks \
        -r ebin \
        -r deps/folsom \
        -r deps/poolboy \
        -r deps/riakc
