
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

run-4: maelstrom
	$(MAELSTROM) test -w g-counter --bin ./04-grow-only-counter.py --node-count 3 --rate 100 --time-limit 20 --nemesis partition


run-5a: maelstrom
	$(MAELSTROM) test -w kafka --bin ./05-replicated-log.py --node-count 1 --concurrency 2n --time-limit 20 --rate 1000

run-5b: maelstrom
	$(MAELSTROM) test -w kafka --bin ./05-replicated-log.py --node-count 2 --concurrency 2n --time-limit 20 --rate 1000

run-5c: run-5b


run-6a: maelstrom
	$(MAELSTROM) test -w txn-rw-register --bin ./06-txns.py --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total

run-6b: maelstrom
	$(MAELSTROM) test -w txn-rw-register --bin ./06-txns.py --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted --availability total --nemesis partition

run-6c: maelstrom
	$(MAELSTROM) test -w txn-rw-register --bin ./06-txns.py --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-committed --availability total --nemesis partition
