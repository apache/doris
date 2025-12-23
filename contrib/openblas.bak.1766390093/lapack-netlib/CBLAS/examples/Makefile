TOPSRCDIR = ../..
include $(TOPSRCDIR)/make.inc

.SUFFIXES: .c .o
.c.o:
	$(CC) $(CFLAGS) -I../include -c -o $@ $<

.PHONY: all
all: cblas_ex1 cblas_ex2

cblas_ex1: cblas_example1.o $(CBLASLIB) $(BLASLIB)
	$(FC) $(FFLAGS) $(LDFLAGS) -o $@ $^
cblas_ex2: cblas_example2.o $(CBLASLIB) $(BLASLIB)
	$(FC) $(FFLAGS) $(LDFLAGS) -o $@ $^

.PHONY: clean cleanobj cleanexe
clean: cleanobj cleanexe
cleanobj:
	rm -f *.o
cleanexe:
	rm -f cblas_ex1 cblas_ex2
