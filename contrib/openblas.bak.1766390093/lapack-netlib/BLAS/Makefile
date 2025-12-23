TOPSRCDIR = ..
include $(TOPSRCDIR)/make.inc

.PHONY: all
all: blas

.PHONY: blas
blas:
	$(MAKE) -C SRC

.PHONY: blas_testing
blas_testing: blas
	$(MAKE) -C TESTING run

.PHONY: clean cleanobj cleanlib cleanexe cleantest
clean:
	$(MAKE) -C SRC clean
	$(MAKE) -C TESTING clean
cleanobj:
	$(MAKE) -C SRC cleanobj
	$(MAKE) -C TESTING cleanobj
cleanlib:
	$(MAKE) -C SRC cleanlib
cleanexe:
	$(MAKE) -C TESTING cleanexe
cleantest:
	$(MAKE) -C TESTING cleantest
