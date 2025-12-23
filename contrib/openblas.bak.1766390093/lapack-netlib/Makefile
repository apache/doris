#
#  Top Level Makefile for LAPACK
#  Version 3.4.1
#  April 2012
#

TOPSRCDIR = .
include $(TOPSRCDIR)/make.inc

.PHONY: all
all: lapack_install lib blas_testing lapack_testing

.PHONY: lib
lib: lapacklib tmglib
#lib: blaslib variants lapacklib tmglib

.PHONY: blaslib
blaslib:
	$(MAKE) -C BLAS

.PHONY: cblaslib
cblaslib:
	$(MAKE) -C CBLAS

.PHONY: lapacklib
lapacklib:
	$(MAKE) -C SRC

.PHONY: lapackelib
lapackelib:
	$(MAKE) -C LAPACKE

.PHONY: blaspplib
blaspplib:
	@echo "Thank you for your interest in BLAS++, a newly developed C++ API for BLAS library"
	@echo "The objective of BLAS++ is to provide a convenient, performance oriented API for development in the C++ language, that, for the most part, preserves established conventions, while, at the same time, takes advantages of modern C++ features, such as: namespaces, templates, exceptions, etc."
	@echo "We are still working on integrating BLAS++ in our library. For the moment, you can download directly blas++ from https://bitbucket.org/icl/blaspp"
	@echo "For support BLAS++ related question, please email: slate-user@icl.utk.edu"

.PHONY: lapackpplib
lapackpplib:
	@echo "Thank you for your interest in LAPACK++, a newly developed C++ API for LAPACK library"
	@echo "The objective of LAPACK++ is to provide a convenient, performance oriented API for development in the C++ language, that, for the most part, preserves established conventions, while, at the same time, takes advantages of modern C++ features, such as: namespaces, templates, exceptions, etc."
	@echo "We are still working on integrating LAPACK++ in our library. For the moment, you can download directly lapack++ from https://bitbucket.org/icl/lapackpp"
	@echo "For support LAPACK++ related question, please email: slate-user@icl.utk.edu"

.PHONY: tmglib
tmglib:
	$(MAKE) -C TESTING/MATGEN

.PHONY: variants
variants:
	$(MAKE) -C SRC/VARIANTS

.PHONY: lapack_install
lapack_install:
	$(MAKE) -C INSTALL run

.PHONY: blas_testing
blas_testing: blaslib
	$(MAKE) -C BLAS blas_testing

.PHONY: cblas_testing
cblas_testing: cblaslib blaslib
	$(MAKE) -C CBLAS cblas_testing

.PHONY: lapack_testing
lapack_testing: tmglib lapacklib blaslib
	$(MAKE) -C TESTING/LIN cleanexe
	$(MAKE) -C TESTING
	./lapack_testing.py

.PHONY: variants_testing
variants_testing: tmglib variants lapacklib blaslib
	$(MAKE) -C TESTING/LIN cleanexe
	$(MAKE) -C TESTING/LIN VARLIB='../../SRC/VARIANTS/cholrl.a'
	$(MAKE) -C TESTING stest.out && mv TESTING/stest.out TESTING/stest_cholrl.out
	$(MAKE) -C TESTING dtest.out && mv TESTING/dtest.out TESTING/dtest_cholrl.out
	$(MAKE) -C TESTING ctest.out && mv TESTING/ctest.out TESTING/ctest_cholrl.out
	$(MAKE) -C TESTING ztest.out && mv TESTING/ztest.out TESTING/ztest_cholrl.out
	$(MAKE) -C TESTING/LIN cleanexe
	$(MAKE) -C TESTING/LIN VARLIB='../../SRC/VARIANTS/choltop.a'
	$(MAKE) -C TESTING stest.out && mv TESTING/stest.out TESTING/stest_choltop.out
	$(MAKE) -C TESTING dtest.out && mv TESTING/dtest.out TESTING/dtest_choltop.out
	$(MAKE) -C TESTING ctest.out && mv TESTING/ctest.out TESTING/ctest_choltop.out
	$(MAKE) -C TESTING ztest.out && mv TESTING/ztest.out TESTING/ztest_choltop.out
	$(MAKE) -C TESTING/LIN cleanexe
	$(MAKE) -C TESTING/LIN VARLIB='../../SRC/VARIANTS/lucr.a'
	$(MAKE) -C TESTING stest.out && mv TESTING/stest.out TESTING/stest_lucr.out
	$(MAKE) -C TESTING dtest.out && mv TESTING/dtest.out TESTING/dtest_lucr.out
	$(MAKE) -C TESTING ctest.out && mv TESTING/ctest.out TESTING/ctest_lucr.out
	$(MAKE) -C TESTING ztest.out && mv TESTING/ztest.out TESTING/ztest_lucr.out
	$(MAKE) -C TESTING/LIN cleanexe
	$(MAKE) -C TESTING/LIN VARLIB='../../SRC/VARIANTS/lull.a'
	$(MAKE) -C TESTING stest.out && mv TESTING/stest.out TESTING/stest_lull.out
	$(MAKE) -C TESTING dtest.out && mv TESTING/dtest.out TESTING/dtest_lull.out
	$(MAKE) -C TESTING ctest.out && mv TESTING/ctest.out TESTING/ctest_lull.out
	$(MAKE) -C TESTING ztest.out && mv TESTING/ztest.out TESTING/ztest_lull.out
	$(MAKE) -C TESTING/LIN cleanexe
	$(MAKE) -C TESTING/LIN VARLIB='../../SRC/VARIANTS/lurec.a'
	$(MAKE) -C TESTING stest.out && mv TESTING/stest.out TESTING/stest_lurec.out
	$(MAKE) -C TESTING dtest.out && mv TESTING/dtest.out TESTING/dtest_lurec.out
	$(MAKE) -C TESTING ctest.out && mv TESTING/ctest.out TESTING/ctest_lurec.out
	$(MAKE) -C TESTING ztest.out && mv TESTING/ztest.out TESTING/ztest_lurec.out
	$(MAKE) -C TESTING/LIN cleanexe
	$(MAKE) -C TESTING/LIN VARLIB='../../SRC/VARIANTS/qrll.a'
	$(MAKE) -C TESTING stest.out && mv TESTING/stest.out TESTING/stest_qrll.out
	$(MAKE) -C TESTING dtest.out && mv TESTING/dtest.out TESTING/dtest_qrll.out
	$(MAKE) -C TESTING ctest.out && mv TESTING/ctest.out TESTING/ctest_qrll.out
	$(MAKE) -C TESTING ztest.out && mv TESTING/ztest.out TESTING/ztest_qrll.out

.PHONY: cblas_example
cblas_example: cblaslib blaslib
	$(MAKE) -C CBLAS cblas_example

.PHONY: lapacke_example
lapacke_example: lapackelib lapacklib blaslib
	$(MAKE) -C LAPACKE lapacke_example

.PHONY: html
html:
	@echo "LAPACK HTML PAGES GENERATION with Doxygen"
	doxygen DOCS/Doxyfile
	@echo "=================="
	@echo "LAPACK HTML PAGES GENERATED in DOCS/explore-html"
	@echo "Usage: open DOCS/explore-html/index.html"
	@echo "Online version available at http://www.netlib.org/lapack/explore-html/"
	@echo "=================="

.PHONY: man
man:
	@echo "LAPACK MAN PAGES GENERATION with Doxygen"
	doxygen DOCS/Doxyfile_man
	@echo "=================="
	@echo "LAPACK MAN PAGES GENERATED in DOCS/MAN"
	@echo "Set your MANPATH env variable accordingly"
	@echo "Usage: man dgetrf.f"
	@echo "=================="

.PHONY: clean cleanobj cleanlib cleanexe cleantest
clean:
	$(MAKE) -C INSTALL clean
	$(MAKE) -C BLAS clean
	$(MAKE) -C CBLAS clean
	$(MAKE) -C SRC clean
	$(MAKE) -C SRC/VARIANTS clean
	$(MAKE) -C TESTING clean
	$(MAKE) -C TESTING/MATGEN clean
	$(MAKE) -C TESTING/LIN clean
	$(MAKE) -C TESTING/EIG clean
	$(MAKE) -C LAPACKE clean
	rm -f *.a
cleanobj:
	$(MAKE) -C INSTALL cleanobj
	$(MAKE) -C BLAS cleanobj
	$(MAKE) -C CBLAS cleanobj
	$(MAKE) -C SRC cleanobj
	$(MAKE) -C SRC/VARIANTS cleanobj
	$(MAKE) -C TESTING/MATGEN cleanobj
	$(MAKE) -C TESTING/LIN cleanobj
	$(MAKE) -C TESTING/EIG cleanobj
	$(MAKE) -C LAPACKE cleanobj
cleanlib:
	$(MAKE) -C BLAS cleanlib
	$(MAKE) -C CBLAS cleanlib
	$(MAKE) -C SRC cleanlib
	$(MAKE) -C SRC/VARIANTS cleanlib
	$(MAKE) -C TESTING/MATGEN cleanlib
	$(MAKE) -C LAPACKE cleanlib
	rm -f *.a
cleanexe:
	$(MAKE) -C INSTALL cleanexe
	$(MAKE) -C BLAS cleanexe
	$(MAKE) -C CBLAS cleanexe
	$(MAKE) -C TESTING/LIN cleanexe
	$(MAKE) -C TESTING/EIG cleanexe
	$(MAKE) -C LAPACKE cleanexe
cleantest:
	$(MAKE) -C INSTALL cleantest
	$(MAKE) -C BLAS cleantest
	$(MAKE) -C CBLAS cleantest
	$(MAKE) -C TESTING cleantest