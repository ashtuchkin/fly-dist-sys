
.PHONY: brew run-1

MAELSTROM=java -Djava.awt.headless=true -jar maelstrom/lib/maelstrom.jar

# Download maelstrom
maelstrom:
	curl https://github.com/jepsen-io/maelstrom/releases/download/v0.2.2/maelstrom.tar.bz2 | tar xj

# Install brew prerequisites
brew:
	brew install openjdk graphviz gnuplot

run-1: maelstrom
	$(MAELSTROM) test -w echo --bin ./01-echo.py --node-count 1 --time-limit 10
