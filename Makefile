
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

run-2: maelstrom
	$(MAELSTROM) test -w unique-ids --bin ./02-unique_id.py --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition

run-3a: maelstrom
	$(MAELSTROM) test -w broadcast --bin ./03-broadcast.py --node-count 1 --time-limit 20 --rate 10

run-3b: maelstrom
	$(MAELSTROM) test -w broadcast --bin ./03-broadcast.py --node-count 5 --time-limit 20 --rate 10

run-3c: maelstrom
	$(MAELSTROM) test -w broadcast --bin ./03-broadcast.py --node-count 5 --time-limit 20 --rate 10 --nemesis partition

run-3d: maelstrom
	$(MAELSTROM) test -w broadcast --bin ./03-broadcast.py --node-count 25 --time-limit 20 --rate 100 --latency 100

run-3d-partition: maelstrom
	$(MAELSTROM) test -w broadcast --bin ./03-broadcast.py --node-count 25 --time-limit 20 --rate 100 --latency 100 --nemesis partition

run-3e: run-3d
