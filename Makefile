# Top-level convenience Makefile.
.PHONY: all clean tracker client
all: tracker client
tracker:
	$(MAKE) -C tracker
client:
	$(MAKE) -C client
clean:
	$(MAKE) -C tracker clean
	$(MAKE) -C client clean
