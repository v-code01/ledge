# Makefile for the Ledge model-checked TLA+ specifications.
#
#   make check   - run TLC on both modules; fail on any invariant violation (default)
#   make sany    - syntax-check both modules
#   make clean   - remove TLC scratch state and metadata directories
#
# Override the toolchain paths if needed:
#   make check JAVA=/path/to/java TLA=/path/to/tla2tools.jar

JAVA ?= /opt/homebrew/opt/openjdk/bin/java
TLA  ?= $(HOME)/.tla/tla2tools.jar

# Parallel GC keeps TLC throughput high and silences the startup warning.
TLC  = $(JAVA) -XX:+UseParallelGC -cp $(TLA) tlc2.TLC
SANY = $(JAVA) -cp $(TLA) tla2sany.SANY

MODULES = RefStore GcReachability

.PHONY: check sany clean

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

clean:
	rm -rf states *_TTrace_*.tla *_TTrace_*.bin
	rm -f *.old
