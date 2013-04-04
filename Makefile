check: clean
	pep8 --ignore=W191 --show-source tests/*.py memcached2.py
	bash -c 'cd tests && exec make'

commit: check
	git diff >/tmp/git-diff.out 2>&1
	git commit -a
	git push

clean:
	rm -rf __pycache__ *.pyc
