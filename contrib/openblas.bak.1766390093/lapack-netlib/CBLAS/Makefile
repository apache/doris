TOPSRCDIR = ..
include $(TOPSRCDIR)/make.inc

.PHONY: all
all: cblas

.PHONY: cblas
cblas: include/cblas_mangling.h
	$(MAKE) -C src

include/cblas_mangling.h: include/cblas_mangling_with_flags.h.in
	cp include/cblas_mangling_with_flags.h.in $@

.PHONY: cblas_testing
cblas_testing: cblas
	$(MAKE) -C testing run

.PHONY: cblas_example
cblas_example: cblas
	$(MAKE) -C examples

.PHONY: clean cleanobj cleanlib cleanexe cleantest
clean:
	$(MAKE) -C src clean
	$(MAKE) -C testing clean
	$(MAKE) -C examples clean
cleanobj:
	$(MAKE) -C src cleanobj
	$(MAKE) -C testing cleanobj
	$(MAKE) -C examples cleanobj
cleanlib:
	$(MAKE) -C src cleanlib
cleanexe:
	$(MAKE) -C testing cleanexe
	$(MAKE) -C examples cleanexe
cleantest:
	$(MAKE) -C testing cleantest
