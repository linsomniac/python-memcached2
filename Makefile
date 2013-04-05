check:
	pep8 --show-source tests/*.py memcached2.py
	bash -c 'cd tests && exec make'
	@make clean

commit: check
	git diff >/tmp/git-diff.out 2>&1
	git commit -a
	git push

clean:
	rm -rf __pycache__ tests/__pycache__ *.pyc tests/*.pyc
