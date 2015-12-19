.PHONY: deps test doc

all: compile

compile:
	./rebar3 compile

clean:
	./rebar3 clean

test: 
	./rebar3 ct
	./rebar3 dialyzer

dialyzer:
	./rebar3 dialyzer
