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

check:
	flake8 --show-source --max-complexity 12 --ignore=E126 tests/*.py memcached2.py
	pep8 --show-source tests/*.py memcached2.py
	bash -c 'cd tests && exec make'
	@make clean

commit: check
	git diff >/tmp/git-diff.out 2>&1
	git commit -a
	git push

clean:
	rm -rf __pycache__ tests/__pycache__ *.pyc tests/*.pyc docs/_build
	rm -rf .ropeproject
