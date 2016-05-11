.PHONY: dev-init
dev-init:
	echo "#!/bin/bash" > ./.git/hooks/pre-commit
	echo "make test" >> ./.git/hooks/pre-commit
	echo "exit $?" >> ./.git/hooks/pre-commit
	chmod +x ./.git/hooks/pre-commit

.PHONY: test
test:
	py.test pichu -v -x
