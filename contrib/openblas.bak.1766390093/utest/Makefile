UTEST_CHECK = 1
TOPDIR	= ..
DIR_EXT=test_extensions

override TARGET_ARCH=
override TARGET_MACH=

UTESTBIN=openblas_utest
UTESTEXTBIN=openblas_utest_ext

.PHONY : all
.NOTPARALLEL : all run_test $(UTESTBIN) $(UTESTEXTBIN)

include $(TOPDIR)/Makefile.system

OBJS=utest_main.o test_min.o test_amax.o test_ismin.o test_rotmg.o test_axpy.o test_dotu.o test_dsdot.o test_swap.o test_rot.o test_dnrm2.o test_zscal.o \
     test_amin.o test_axpby.o test_gemv.o
#test_rot.o test_swap.o test_axpy.o test_dotu.o test_dsdot.o test_fork.o
OBJS_EXT=utest_main.o $(DIR_EXT)/xerbla.o $(DIR_EXT)/common.o 
OBJS_EXT+=$(DIR_EXT)/test_isamin.o $(DIR_EXT)/test_idamin.o $(DIR_EXT)/test_icamin.o $(DIR_EXT)/test_izamin.o 
OBJS_EXT+=$(DIR_EXT)/test_ssum.o $(DIR_EXT)/test_dsum.o $(DIR_EXT)/test_scsum.o $(DIR_EXT)/test_dzsum.o
OBJS_EXT+=$(DIR_EXT)/test_saxpby.o $(DIR_EXT)/test_daxpby.o $(DIR_EXT)/test_caxpby.o $(DIR_EXT)/test_zaxpby.o $(DIR_EXT)/test_zaxpyc.o $(DIR_EXT)/test_caxpyc.o
OBJS_EXT+=$(DIR_EXT)/test_samin.o $(DIR_EXT)/test_damin.o $(DIR_EXT)/test_scamin.o $(DIR_EXT)/test_dzamin.o $(DIR_EXT)/test_scamax.o $(DIR_EXT)/test_dzamax.o
OBJS_EXT+=$(DIR_EXT)/test_drotmg.o $(DIR_EXT)/test_srotmg.o $(DIR_EXT)/test_zrotg.o $(DIR_EXT)/test_crotg.o $(DIR_EXT)/test_crot.o $(DIR_EXT)/test_zrot.o
OBJS_EXT+=$(DIR_EXT)/test_zscal.o $(DIR_EXT)/test_cscal.o
OBJS_EXT+=$(DIR_EXT)/test_domatcopy.o $(DIR_EXT)/test_somatcopy.o $(DIR_EXT)/test_zomatcopy.o $(DIR_EXT)/test_comatcopy.o
OBJS_EXT+=$(DIR_EXT)/test_simatcopy.o $(DIR_EXT)/test_dimatcopy.o $(DIR_EXT)/test_cimatcopy.o $(DIR_EXT)/test_zimatcopy.o
OBJS_EXT+=$(DIR_EXT)/test_sgeadd.o $(DIR_EXT)/test_dgeadd.o $(DIR_EXT)/test_cgeadd.o $(DIR_EXT)/test_zgeadd.o
OBJS_EXT+=$(DIR_EXT)/test_cgemv_t.o $(DIR_EXT)/test_zgemv_t.o $(DIR_EXT)/test_cgemv_n.o $(DIR_EXT)/test_zgemv_n.o
OBJS_EXT+=$(DIR_EXT)/test_sgemmt.o $(DIR_EXT)/test_dgemmt.o $(DIR_EXT)/test_cgemmt.o $(DIR_EXT)/test_zgemmt.o
OBJS_EXT+=$(DIR_EXT)/test_ztrmv.o $(DIR_EXT)/test_ctrmv.o $(DIR_EXT)/test_ztrsv.o $(DIR_EXT)/test_ctrsv.o
OBJS_EXT+=$(DIR_EXT)/test_zgemm.o $(DIR_EXT)/test_cgemm.o $(DIR_EXT)/test_zgbmv.o $(DIR_EXT)/test_cgbmv.o

ifneq ($(NO_LAPACK), 1)
OBJS += test_potrs.o
OBJS_EXT += $(DIR_EXT)/test_zspmv.o $(DIR_EXT)/test_cspmv.o $(DIR_EXT)/test_zsbmv.o $(DIR_EXT)/test_csbmv.o
ifneq ($(NO_CBLAS), 1)
ifneq ($(NO_LAPACKE), 1)
OBJS += test_kernel_regress.o
endif
endif
endif

#this does not work with OpenMP nor with native Windows or Android threads
# FIXME TBD if this works on OSX, SunOS, POWER and zarch
ifeq ($(OSNAME), $(filter $(OSNAME),Linux CYGWIN_NT))
ifneq ($(USE_OPENMP), 1)
OBJS += test_fork.o
endif
OBJS += test_post_fork.o
endif

ifeq ($(C_COMPILER), PGI)
OBJS = utest_main2.o
endif
ifeq ($(C_COMPILER), SUN)
OBJS = utest_main2.o
endif
ifeq ($(OSNAME), AIX)
OBJS = utest_main2.o
OBJS_EXT = $(DIR_EXT)/utest_main2.o
endif

ifeq ($(NO_CBLAS), 1)
override CFLAGS += -DNO_CBLAS 
endif

all : run_test

ifeq ($(OSNAME), AIX)
$(UTESTBIN): $(OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ ../$(LIBNAME) $(EXTRALIB)

$(UTESTEXTBIN): $(OBJS_EXT)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ ../$(LIBNAME) $(EXTRALIB)
else
$(UTESTBIN): $(OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ ../$(LIBNAME) $(EXTRALIB) $(FEXTRALIB)

$(UTESTEXTBIN): $(OBJS_EXT)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ ../$(LIBNAME) $(EXTRALIB) $(FEXTRALIB)
endif

run_test: $(UTESTBIN) $(UTESTEXTBIN)
ifneq ($(CROSS), 1)
	./$(UTESTBIN)
	./$(UTESTEXTBIN)
endif

clean:
	-rm -f *.o $(UTESTBIN) $(UTESTEXTBIN)
	-rm -f $(DIR_EXT)/*.o

libs:
