# Makefile for the Ledge model-checked TLA+ specifications.
#
#   make check   - run TLC on all modules; fail on any invariant violation (default)
#   make sany    - syntax-check all modules
#   make neg     - run the Sharding negative control; PASS iff TLC reports a violation
#   make neg-txn - run the CrossShardTxn negative control; PASS iff TLC reports a violation
#   make neg-all - run both negative controls
#   make clean   - remove TLC scratch state and metadata directories
#
# Override the toolchain paths if needed:
#   make check JAVA=/path/to/java TLA=/path/to/tla2tools.jar

JAVA ?= /opt/homebrew/opt/openjdk/bin/java
TLA  ?= $(HOME)/.tla/tla2tools.jar

# Parallel GC keeps TLC throughput high and silences the startup warning.
TLC  = $(JAVA) -XX:+UseParallelGC -cp $(TLA) tlc2.TLC
SANY = $(JAVA) -cp $(TLA) tla2sany.SANY

MODULES = RefStore GcReachability Sharding CrossShardTxn

.PHONY: check sany neg neg-txn neg-all clean

check:
	@for m in $(MODULES); do \
		echo "=== TLC: $$m ==="; \
		$(TLC) -config $$m.cfg $$m.tla || exit $$?; \
	done
	@echo "=== All modules: zero invariant violations ==="

sany:
	@for m in $(MODULES); do \
		echo "=== SANY: $$m ==="; \
		$(SANY) $$m.tla || exit $$?; \
	done

# Negative control: the Sharding spec with BadRouting = TRUE MUST fail
# (RoutingTotality is violated by a ref double-assigned to two shards).
# This target PASSES iff TLC exits non-zero, proving the invariant has teeth.
neg:
	@echo "=== TLC negative control: Sharding (BadRouting = TRUE) ==="
	@if $(TLC) -config Sharding_neg.cfg Sharding.tla; then \
		echo "NEGATIVE CONTROL FAILED: TLC found no violation but one was expected"; \
		exit 1; \
	else \
		echo "=== Negative control fired as expected (RoutingTotality violated) ==="; \
	fi

# Negative control for the 2PC spec: BadCoord = TRUE enables BadCommit (a
# coordinator that rolls a ref forward with no durable Commit decision), making
# a split decision reachable. This MUST violate NoDirtyRead (and Atomicity).
# PASSES iff TLC exits non-zero.
neg-txn:
	@echo "=== TLC negative control: CrossShardTxn (BadCoord = TRUE) ==="
	@if $(TLC) -config CrossShardTxn_neg.cfg CrossShardTxn.tla; then \
		echo "NEGATIVE CONTROL FAILED: TLC found no violation but one was expected"; \
		exit 1; \
	else \
		echo "=== Negative control fired as expected (NoDirtyRead/Atomicity violated) ==="; \
	fi

# Run both negative controls; PASSES iff each fires its expected violation.
neg-all: neg neg-txn

clean:
	rm -rf states *_TTrace_*.tla *_TTrace_*.bin
	rm -f *.old
