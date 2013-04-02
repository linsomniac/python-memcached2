TESTS = $(wildcard test_*.py)

all.PHONY: test test3

test:
	@- $(foreach TEST,$(TESTS), \
		echo === Running python2 test: $(TEST); \
		python $(TEST); \
		)
test3:
	@- $(foreach TEST,$(TESTS), \
		echo === Running python2 test: $(TEST); \
		python3 $(TEST); \
		)
