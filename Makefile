.PHONY: test demo generate
test:
	python -m unittest discover -s tests -v
demo:
	python -m clickstream.demo
generate:
	python -m clickstream.generator --count 10000
