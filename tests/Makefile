# Copyright 2013 Sean Reifschneider, tummy.com, ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

TESTS = $(wildcard test_*.py)

all.PHONY: test test32 test33

test:
	@- $(foreach TEST,$(TESTS), \
		echo === Running python2 test: $(TEST); \
		python $(TEST); \
		)
test32:
	@- $(foreach TEST,$(TESTS), \
		echo === Running python3 test: $(TEST); \
		python3.2 $(TEST); \
		)
test33:
	@- $(foreach TEST,$(TESTS), \
		echo === Running python3 test: $(TEST); \
		python3.3 $(TEST); \
		)
