TOPDIR	= .
include ./Makefile.system
LNCMD = ln -fs
ifeq ($(FIXED_LIBNAME), 1)
LNCMD = true
endif

BLASDIRS = interface driver/level2 driver/level3 driver/others

ifneq ($(DYNAMIC_ARCH), 1)
BLASDIRS += kernel
endif

ifdef SANITY_CHECK
BLASDIRS += reference
endif

SUBDIRS	= $(BLASDIRS)
ifneq ($(NO_LAPACK), 1)
SUBDIRS	+= lapack
endif

RELA =
ifeq ($(BUILD_RELAPACK), 1)
RELA = re_lapack
endif

ifeq ($(NO_FORTRAN), 1)
define NOFORTRAN
1
endef
ifneq ($(NO_LAPACK), 1)
define C_LAPACK
1
endef
endif
export NOFORTRAN
export NO_LAPACK
export C_LAPACK
endif

ifeq ($(F_COMPILER),CRAY)
LAPACK_NOOPT := $(filter-out -O0 -O1 -O2 -O3 -Ofast -Og -Os,$(LAPACK_FFLAGS))
else
LAPACK_NOOPT := $(filter-out -O0 -O1 -O2 -O3 -Ofast -O -Og -Os,$(LAPACK_FFLAGS))
endif

ifdef LAPACK_STRLEN
LAPACK_FFLAGS += -DLAPACK_STRLEN=$(LAPACK_STRLEN)
endif

SUBDIRS_ALL = $(SUBDIRS) test ctest utest exports benchmark ../laswp ../bench cpp_thread_test

.PHONY : all libs netlib $(RELA) test ctest shared install
.NOTPARALLEL : shared

all :: tests
	@echo
	@echo " OpenBLAS build complete. ($(LIB_COMPONENTS))"
	@echo
	@echo "  OS               ... $(OSNAME)             "
	@echo "  Architecture     ... $(ARCH)               "
ifndef BINARY64
	@echo "  BINARY           ... 32bit                 "
else
	@echo "  BINARY           ... 64bit                 "
endif

ifdef INTERFACE64
ifneq ($(INTERFACE64), 0)
	@echo "  Use 64 bits int    (equivalent to \"-i8\" in Fortran)      "
endif
endif
	@$(CC) --version > /dev/null 2>&1;\
	if [ $$? -eq 0 ]; then \
	   cverinfo=`$(CC) --version | sed -n '1p'`; \
	   if [ -z "$${cverinfo}" ]; then \
	   cverinfo=`$(CC) --version | sed -n '2p'`; \
	   fi; \
	   echo "  C compiler       ... $(C_COMPILER)  (cmd & version : $${cverinfo})";\
	else  \
	   echo "  C compiler       ... $(C_COMPILER)  (command line : $(CC))";\
	fi
ifeq ($(NOFORTRAN), $(filter 0,$(NOFORTRAN)))
	@$(FC) --version > /dev/null 2>&1;\
	if [ $$? -eq 0 ]; then \
	   fverinfo=`$(FC) --version | sed -n '1p'`; \
	   if [ -z "$${fverinfo}" ]; then \
	   fverinfo=`$(FC) --version | sed -n '2p'`; \
	   fi; \
	   echo "  Fortran compiler ... $(F_COMPILER)  (cmd & version : $${fverinfo})";\
	else \
	   echo "  Fortran compiler ... $(F_COMPILER)  (command line : $(FC))";\
	fi
endif

ifeq ($(OSNAME), WINNT)
	@-$(LNCMD) $(LIBNAME) $(LIBPREFIX).$(LIBSUFFIX)
endif

ifneq ($(OSNAME), AIX)
	@echo -n "  Library Name     ... $(LIBNAME)"
else
	@echo "  Library Name     ... $(LIBNAME)"
endif

ifndef SMP
	@echo " (Single-threading)  "
else
	@echo " (Multi-threading; Max num-threads is $(NUM_THREADS))"
endif

ifeq ($(DYNAMIC_ARCH), 1)
	@echo "  Supporting multiple $(ARCH) cpu models with minimum requirement for the common code being $(CORE)"
endif

ifeq ($(USE_OPENMP), 1)
	@echo
	@echo " Use OpenMP in the multithreading. Because of ignoring OPENBLAS_NUM_THREADS and GOTO_NUM_THREADS flags, "
	@echo " you should use OMP_NUM_THREADS environment variable to control the number of threads."
	@echo
endif

ifeq ($(OSNAME), Darwin)
	@echo "WARNING: If you plan to use the dynamic library $(LIBDYNNAME), you must run:"
	@echo
	@echo "\"make PREFIX=/your_installation_path/ install\"."
	@echo
	@echo "(or set PREFIX in Makefile.rule and run make install."
	@echo
	@echo "Note that any flags passed to make during build should also be passed to make install"
	@echo "to circumvent any install errors."
	@echo
	@echo "If you want to move the .dylib to a new location later, make sure you change"
	@echo "the internal name of the dylib with:"
	@echo
	@echo "install_name_tool -id /new/absolute/path/to/$(LIBDYNNAME) $(LIBDYNNAME)"
endif
	@echo
	@echo "To install the library, you can run \"make PREFIX=/path/to/your/installation install\"."
	@echo
	@echo "Note that any flags passed to make during build should also be passed to make install"
	@echo "to circumvent any install errors."
	@echo

shared : libs netlib $(RELA)
ifneq ($(NO_SHARED), 1)
ifeq ($(OSNAME), $(filter $(OSNAME),Linux SunOS Android Haiku FreeBSD DragonFly))
	@$(MAKE) -C exports so
	@$(LNCMD) $(LIBSONAME) $(LIBPREFIX).so
	@$(LNCMD) $(LIBSONAME) $(LIBPREFIX).so.$(MAJOR_VERSION)
endif
ifeq ($(OSNAME), $(filter $(OSNAME),OpenBSD NetBSD))
	@$(MAKE) -C exports so
	@$(LNCMD) $(LIBSONAME) $(LIBPREFIX).so
endif
ifeq ($(OSNAME), Darwin)
	@$(MAKE) -C exports dyn
	@$(LNCMD) $(LIBDYNNAME) $(LIBPREFIX).dylib
	@$(LNCMD) $(LIBDYNNAME) $(LIBPREFIX).$(MAJOR_VERSION).dylib
endif
ifeq ($(OSNAME), WINNT)
	@$(MAKE) -C exports dll
endif
ifeq ($(OSNAME), CYGWIN_NT)
	@$(MAKE) -C exports dll
endif
ifeq ($(OSNAME), AIX)
	@$(MAKE) -C exports so
endif
endif

tests : shared
ifeq ($(NOFORTRAN), $(filter 0,$(NOFORTRAN)))
	touch $(LIBNAME)
ifndef NO_FBLAS
	$(MAKE) -C test all
endif
endif
ifneq ($(ONLY_CBLAS), 1)
	$(MAKE) -C utest all
endif
ifneq ($(NO_CBLAS), 1)
ifneq ($(ONLY_CBLAS), 1)
	$(MAKE) -C ctest all
endif
ifeq ($(CPP_THREAD_SAFETY_TEST), 1)
	$(MAKE) -C cpp_thread_test all
endif
endif

libs :
ifeq ($(CORE), UNKNOWN)
	$(error OpenBLAS: Detecting CPU failed. Please set TARGET explicitly, e.g. make TARGET=your_cpu_target. Please read README for the detail.)
endif
ifeq ($(NOFORTRAN), 1)
	$(info OpenBLAS: Detecting fortran compiler failed. Can only compile BLAS and f2c-converted LAPACK.)
endif
ifeq ($(NO_STATIC), 1)
ifeq ($(NO_SHARED), 1)
	$(error OpenBLAS: neither static nor shared are enabled.)
endif
endif
	@for d in $(SUBDIRS) ; \
	do if test -d $$d; then \
	  $(MAKE) -C $$d $(@F) || exit 1 ; \
	fi; \
	done
#Save the config files for installation
	@cp Makefile.conf Makefile.conf_last
	@cp config.h config_last.h
ifdef QUAD_PRECISION
	@echo "#define QUAD_PRECISION">> config_last.h
endif
ifeq ($(EXPRECISION), 1)
	@echo "#define EXPRECISION">> config_last.h
endif
##
ifeq ($(DYNAMIC_ARCH), 1)
	@$(MAKE) -C kernel commonlibs || exit 1
	@for d in $(DYNAMIC_CORE) ; \
	do  $(MAKE) GOTOBLAS_MAKEFILE= -C kernel TARGET_CORE=$$d kernel || exit 1 ;\
	done
	@echo DYNAMIC_ARCH=1 >> Makefile.conf_last
ifeq ($(DYNAMIC_OLDER), 1)
	@echo DYNAMIC_OLDER=1 >> Makefile.conf_last
endif	
endif
	@echo TARGET=$(CORE) >> Makefile.conf_last
ifdef USE_THREAD
	@echo USE_THREAD=$(USE_THREAD) >>  Makefile.conf_last
endif
ifdef SMP
ifdef NUM_THREADS
	@echo NUM_THREADS=$(NUM_THREADS) >>  Makefile.conf_last
else
	@echo NUM_THREADS=$(NUM_CORES) >>  Makefile.conf_last
endif
endif
ifeq ($(USE_OPENMP),1)
	@echo USE_OPENMP=1 >>  Makefile.conf_last
endif
ifeq ($(INTERFACE64),1)
	@echo INTERFACE64=1 >>  Makefile.conf_last
endif
	@echo THELIBNAME=$(LIBNAME) >>  Makefile.conf_last
	@echo THELIBSONAME=$(LIBSONAME) >>  Makefile.conf_last
	@-$(LNCMD) $(LIBNAME) $(LIBPREFIX).$(LIBSUFFIX)
	@touch lib.grd

prof : prof_blas prof_lapack

prof_blas :
	$(LNCMD) $(LIBNAME_P) $(LIBPREFIX)_p.$(LIBSUFFIX)
	for d in $(SUBDIRS) ; \
	do if test -d $$d; then \
	  $(MAKE) -C $$d prof || exit 1 ; \
	fi; \
	done
ifeq ($(DYNAMIC_ARCH), 1)
	  $(MAKE) -C kernel commonprof || exit 1
endif

blas :
	$(LNCMD) $(LIBNAME) $(LIBPREFIX).$(LIBSUFFIX)
	for d in $(BLASDIRS) ; \
	do if test -d $$d; then \
	  $(MAKE) -C $$d libs || exit 1 ; \
	fi; \
	done

hpl :
	$(LNCMD) $(LIBNAME) $(LIBPREFIX).$(LIBSUFFIX)
	for d in $(BLASDIRS) ../laswp exports ; \
	do if test -d $$d; then \
	  $(MAKE) -C $$d $(@F) || exit 1 ; \
	fi; \
	done
ifeq ($(DYNAMIC_ARCH), 1)
	  $(MAKE) -C kernel commonlibs || exit 1
	for d in $(DYNAMIC_CORE) ; \
	do  $(MAKE) GOTOBLAS_MAKEFILE= -C kernel TARGET_CORE=$$d kernel || exit 1 ;\
	done
endif

hpl_p :
	$(LNCMD) $(LIBNAME_P) $(LIBPREFIX)_p.$(LIBSUFFIX)
	for d in $(SUBDIRS) ../laswp exports ; \
	do if test -d $$d; then \
	  $(MAKE) -C $$d $(@F) || exit 1 ; \
	fi; \
	done

netlib : lapack_prebuild
ifneq ($(NO_LAPACK), 1)
	@$(MAKE) -C $(NETLIB_LAPACK_DIR) lapacklib
	@$(MAKE) -C $(NETLIB_LAPACK_DIR) tmglib
endif
ifneq ($(NO_LAPACKE), 1)
	@$(MAKE) -C $(NETLIB_LAPACK_DIR) lapackelib
endif

ifeq ($(NO_LAPACK), 1)
re_lapack :

else
re_lapack :
	@$(MAKE) -C relapack
endif

prof_lapack : lapack_prebuild
	@$(MAKE) -C $(NETLIB_LAPACK_DIR) lapack_prof

lapack_prebuild :
ifeq ($(NO_LAPACK), $(filter 0,$(NO_LAPACK)))
	-@echo "FC          = $(FC)" > $(NETLIB_LAPACK_DIR)/make.inc
ifeq ($(F_COMPILER), GFORTRAN)
	-@echo "override FFLAGS      = $(LAPACK_FFLAGS) -fno-tree-vectorize" >> $(NETLIB_LAPACK_DIR)/make.inc
else
	-@echo "override FFLAGS      = $(LAPACK_FFLAGS)" >> $(NETLIB_LAPACK_DIR)/make.inc
endif
	-@echo "FFLAGS_DRV  = $(LAPACK_FFLAGS)" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "POPTS       = $(LAPACK_FPFLAGS)" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "FFLAGS_NOOPT       = -O0 $(LAPACK_NOOPT)" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "PNOOPT      = $(LAPACK_FPFLAGS) -O0" >> $(NETLIB_LAPACK_DIR)/make.inc
ifeq ($(C_COMPILER)$(F_COMPILER)$(USE_OPENMP), CLANGGFORTRAN1)
	-@echo "LDFLAGS     = $(FFLAGS) $(EXTRALIB) -lomp" >> $(NETLIB_LAPACK_DIR)/make.inc
else
ifeq ($(C_COMPILER)$(F_COMPILER)$(USE_OPENMP), CLANGIBM1)
	-@echo "LDFLAGS     = $(FFLAGS) $(EXTRALIB) -lomp" >> $(NETLIB_LAPACK_DIR)/make.inc
else
	-@echo "LDFLAGS     = $(FFLAGS) $(EXTRALIB)" >> $(NETLIB_LAPACK_DIR)/make.inc
endif
endif
	-@echo "CC          = $(CC)" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "override CFLAGS      = $(LAPACK_CFLAGS)" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "AR          = $(AR)" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "ARFLAGS     = $(ARFLAGS) -ru" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "RANLIB      = $(RANLIB)" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "LAPACKLIB   = ../../$(LIBNAME)" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "TMGLIB      = ../../../$(LIBNAME)" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "BLASLIB     = ../../../$(LIBNAME)" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "LAPACKELIB  = ../../../$(LIBNAME)" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "LAPACKLIB_P = ../$(LIBNAME_P)" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "SUFFIX      = $(SUFFIX)" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "PSUFFIX     = $(PSUFFIX)" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "CEXTRALIB   = $(EXTRALIB)" >> $(NETLIB_LAPACK_DIR)/make.inc
ifeq ($(F_COMPILER), GFORTRAN)
	-@echo "TIMER       = INT_ETIME" >> $(NETLIB_LAPACK_DIR)/make.inc
ifdef SMP
ifeq ($(OSNAME), WINNT)
	-@echo "LOADER      = $(FC)" >> $(NETLIB_LAPACK_DIR)/make.inc
else ifeq ($(OSNAME), Haiku)
	-@echo "LOADER      = $(FC)" >> $(NETLIB_LAPACK_DIR)/make.inc
else
	-@echo "LOADER      = $(FC) -pthread" >> $(NETLIB_LAPACK_DIR)/make.inc
endif
else
	-@echo "LOADER      = $(FC)" >> $(NETLIB_LAPACK_DIR)/make.inc
endif
else
	-@echo "TIMER       = NONE" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "LOADER      = $(FC)" >> $(NETLIB_LAPACK_DIR)/make.inc
endif
ifeq ($(BUILD_LAPACK_DEPRECATED), 1)
	-@echo "BUILD_DEPRECATED      = 1" >> $(NETLIB_LAPACK_DIR)/make.inc
endif
ifeq ($(BUILD_SINGLE), 1)
	-@echo "BUILD_SINGLE      = 1" >> $(NETLIB_LAPACK_DIR)/make.inc
endif
ifeq ($(BUILD_DOUBLE), 1)
	-@echo "BUILD_DOUBLE      = 1" >> $(NETLIB_LAPACK_DIR)/make.inc
endif
ifeq ($(BUILD_COMPLEX), 1)
	-@echo "BUILD_COMPLEX      = 1" >> $(NETLIB_LAPACK_DIR)/make.inc
endif
ifeq ($(BUILD_COMPLEX16), 1)
	-@echo "BUILD_COMPLEX16      = 1" >> $(NETLIB_LAPACK_DIR)/make.inc
endif
	-@echo "LAPACKE_WITH_TMG      = 1" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@cat  make.inc >> $(NETLIB_LAPACK_DIR)/make.inc
endif

large.tgz :
ifeq ($(NOFORTRAN), $(filter 0,$(NOFORTRAN)))
	if [ ! -a $< ]; then
	-wget http://www.netlib.org/lapack/timing/large.tgz;
	fi
endif

timing.tgz :
ifeq ($(NOFORTRAN), $(filter 0,$(NOFORTRAN)))
	if [ ! -a $< ]; then
	-wget http://www.netlib.org/lapack/timing/timing.tgz;
	fi
endif

lapack-timing : large.tgz timing.tgz
ifeq ($(NOFORTRAN), $(filter 0,$(NOFORTRAN)))
	(cd $(NETLIB_LAPACK_DIR); $(TAR) zxf ../timing.tgz TIMING)
	(cd $(NETLIB_LAPACK_DIR)/TIMING; $(TAR) zxf ../../large.tgz )
	$(MAKE) -C $(NETLIB_LAPACK_DIR)/TIMING
endif


lapack-test :
	(cd $(NETLIB_LAPACK_DIR)/TESTING && rm -f x* *.out)
	$(MAKE) -j 1 -C $(NETLIB_LAPACK_DIR)/TESTING/EIG xeigtstc  xeigtstd  xeigtsts  xeigtstz 
	$(MAKE) -j 1 -C $(NETLIB_LAPACK_DIR)/TESTING/LIN xlintstc  xlintstd  xlintstds  xlintstrfd  xlintstrfz  xlintsts  xlintstz  xlintstzc xlintstrfs xlintstrfc
ifneq ($(CROSS), 1)
	( cd $(NETLIB_LAPACK_DIR)/INSTALL; $(MAKE) all; ./testlsame; ./testslamch; ./testdlamch; \
        ./testsecond; ./testdsecnd; ./testieee; ./testversion )
	(cd $(NETLIB_LAPACK_DIR); ./lapack_testing.py -r -b TESTING)
endif

lapack-runtest: lapack-test
	( cd $(NETLIB_LAPACK_DIR)/INSTALL; ./testlsame; ./testslamch; ./testdlamch; \
        ./testsecond; ./testdsecnd; ./testieee; ./testversion )
	(cd $(NETLIB_LAPACK_DIR); ./lapack_testing.py -r -b TESTING )


blas-test:
	(cd $(NETLIB_LAPACK_DIR)/BLAS/TESTING && rm -f x* *.out)

	$(MAKE) -j 1 -C $(NETLIB_LAPACK_DIR) blas_testing
	(cd $(NETLIB_LAPACK_DIR)/BLAS/TESTING && cat *.out)


dummy :

install :
	$(MAKE) -f Makefile.install install

install_tests :
	$(MAKE) -f Makefile.install install_tests

clean ::
	@for d in $(SUBDIRS_ALL) ; \
	do if test -d $$d; then \
	  $(MAKE) -C $$d $(@F) || exit 1 ; \
	fi; \
	done
#ifdef DYNAMIC_ARCH
	@$(MAKE) -C kernel clean
#endif
	@$(MAKE) -C reference clean
	@rm -f *.$(LIBSUFFIX) *.so *~ *.exe getarch getarch_2nd *.dll *.lib *.$(SUFFIX) *.dwf $(LIBPREFIX).$(LIBSUFFIX) $(LIBPREFIX)_p.$(LIBSUFFIX) $(LIBPREFIX).so.$(MAJOR_VERSION) *.lnk myconfig.h *.so.renamed *.a.renamed *.so.0
ifeq ($(OSNAME), Darwin)
	@rm -rf getarch.dSYM getarch_2nd.dSYM
endif
	@rm -f Makefile.conf config.h Makefile_kernel.conf config_kernel.h st* *.dylib
	@rm -f cblas.tmp cblas.tmp2
	@touch $(NETLIB_LAPACK_DIR)/make.inc
	@$(MAKE) -C $(NETLIB_LAPACK_DIR) clean
	@rm -f $(NETLIB_LAPACK_DIR)/make.inc
	@$(MAKE) -C relapack clean
	@rm -f *.grd Makefile.conf_last config_last.h
	@(cd $(NETLIB_LAPACK_DIR)/TESTING && rm -f x* *.out testing_results.txt)
	@echo Done.
