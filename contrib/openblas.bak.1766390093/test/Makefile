TOPDIR	= ..
include ../Makefile.system
ifeq ($(F_COMPILER),GFORTRAN)
ifneq (, $(filter $(CORE),LOONGSON3R3 LOONGSON3R4))
	override FFLAGS := $(filter_out(-O2 -O3,$(FFLAGS))) -O0
endif
        override FFLAGS += -fno-tree-vectorize
endif

SUPPORT_GEMM3M = 0

ifeq ($(ARCH), x86)
SUPPORT_GEMM3M = 1
endif

ifeq ($(ARCH), x86_64)
SUPPORT_GEMM3M = 1
endif

ifeq ($(ARCH), ia64)
SUPPORT_GEMM3M = 1
endif

ifeq ($(ARCH), MIPS)
SUPPORT_GEMM3M = 1
endif

ifeq ($(NOFORTRAN),1)
all ::
else
all :: level1 level2 level3
endif

ifeq ($(BUILD_SINGLE),1)
S1=sblat1
endif
ifeq ($(BUILD_DOUBLE),1)
D1=dblat1
endif
ifeq ($(BUILD_COMPLEX),1)
C1=cblat1
endif
ifeq ($(BUILD_COMPLEX16),1)
Z1=zblat1
endif

level1: $(S1) $(D1) $(C1) $(Z1)

ifneq ($(CROSS), 1)
ifeq ($(BUILD_SINGLE),1)
	OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 ./sblat1
endif
ifeq ($(BUILD_DOUBLE),1)
	OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 ./dblat1
endif
ifeq ($(BUILD_COMPLEX),1)
	OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 ./cblat1
endif
ifeq ($(BUILD_COMPLEX16),1)
	OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 ./zblat1
endif
ifdef SMP
ifeq ($(USE_OPENMP), 1)
ifeq ($(BUILD_SINGLE),1)
	OMP_NUM_THREADS=2 ./sblat1
endif
ifeq ($(BUILD_DOUBLE),1)
	OMP_NUM_THREADS=2 ./dblat1
endif
ifeq ($(BUILD_COMPLEX),1)
	OMP_NUM_THREADS=2 ./cblat1
endif
ifeq ($(BUILD_COMPLEX16),1)
	OMP_NUM_THREADS=2 ./zblat1
endif
else
ifeq ($(BUILD_SINGLE),1)
	OPENBLAS_NUM_THREADS=2 ./sblat1
endif
ifeq ($(BUILD_DOUBLE),1)
	OPENBLAS_NUM_THREADS=2 ./dblat1
endif
ifeq ($(BUILD_COMPLEX),1)
	OPENBLAS_NUM_THREADS=2 ./cblat1
endif
ifeq ($(BUILD_COMPLEX16),1)
	OPENBLAS_NUM_THREADS=2 ./zblat1
endif
endif
endif
endif

ifeq ($(BUILD_SINGLE),1)
S2=sblat2
endif
ifeq ($(BUILD_DOUBLE),1)
D2=dblat2
endif
ifeq ($(BUILD_COMPLEX),1)
C2=cblat2
endif
ifeq ($(BUILD_COMPLEX16),1)
Z2=zblat2
endif

level2: $(S2) $(D2) $(C2) $(Z2)


ifneq ($(CROSS), 1)
	rm -f ?BLAT2.SUMM
ifeq ($(BUILD_SINGLE),1)
	OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 ./sblat2 < ./sblat2.dat
	@$(GREP) -q FATAL SBLAT2.SUMM && cat SBLAT2.SUMM || exit 0
endif
ifeq ($(BUILD_DOUBLE),1)
	OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 ./dblat2 < ./dblat2.dat
	@$(GREP) -q FATAL DBLAT2.SUMM && cat DBLAT2.SUMM || exit 0
endif
ifeq ($(BUILD_COMPLEX),1)
	OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 ./cblat2 < ./cblat2.dat
	@$(GREP) -q FATAL CBLAT2.SUMM && cat CBLAT2.SUMM || exit 0
endif
ifeq ($(BUILD_COMPLEX16),1)
	OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 ./zblat2 < ./zblat2.dat
	@$(GREP) -q FATAL ZBLAT2.SUMM && cat ZBLAT2.SUMM || exit 0
endif
ifdef SMP
	rm -f ?BLAT2.SUMM
ifeq ($(USE_OPENMP), 1)
ifeq ($(BUILD_SINGLE),1)
	OMP_NUM_THREADS=2 ./sblat2 < ./sblat2.dat
	@$(GREP) -q FATAL SBLAT2.SUMM && cat SBLAT2.SUMM || exit 0
endif
ifeq ($(BUILD_DOUBLE),1)
	OMP_NUM_THREADS=2 ./dblat2 < ./dblat2.dat
	@$(GREP) -q FATAL DBLAT2.SUMM && cat DBLAT2.SUMM || exit 0
endif
ifeq ($(BUILD_COMPLEX),1)
	OMP_NUM_THREADS=2 ./cblat2 < ./cblat2.dat
	@$(GREP) -q FATAL CBLAT2.SUMM && cat CBLAT2.SUMM || exit 0
endif
ifeq ($(BUILD_COMPLEX16),1)
	OMP_NUM_THREADS=2 ./zblat2 < ./zblat2.dat
	@$(GREP) -q FATAL ZBLAT2.SUMM && cat ZBLAT2.SUMM || exit 0
endif
else
ifeq ($(BUILD_SINGLE),1)
	OPENBLAS_NUM_THREADS=2 ./sblat2 < ./sblat2.dat
	@$(GREP) -q FATAL SBLAT2.SUMM && cat SBLAT2.SUMM || exit 0
endif
ifeq ($(BUILD_DOUBLE),1)
	OPENBLAS_NUM_THREADS=2 ./dblat2 < ./dblat2.dat
	@$(GREP) -q FATAL DBLAT2.SUMM && cat DBLAT2.SUMM || exit 0
endif
ifeq ($(BUILD_COMPLEX),1)
	OPENBLAS_NUM_THREADS=2 ./cblat2 < ./cblat2.dat
	@$(GREP) -q FATAL CBLAT2.SUMM && cat CBLAT2.SUMM || exit 0
endif
ifeq ($(BUILD_COMPLEX16),1)
	OPENBLAS_NUM_THREADS=2 ./zblat2 < ./zblat2.dat
	@$(GREP) -q FATAL ZBLAT2.SUMM && cat ZBLAT2.SUMM || exit 0
endif
endif
endif
endif

ifeq ($(BUILD_BFLOAT16),1)
B3= test_sbgemm
endif
ifeq ($(BUILD_SINGLE),1)
S3=sblat3
endif
ifeq ($(BUILD_DOUBLE),1)
D3=dblat3
endif
ifeq ($(BUILD_COMPLEX),1)
ifeq ($(SUPPORT_GEMM3M),1)
C3=cblat3 cblat3_3m
else
C3=cblat3
endif
endif
ifeq ($(BUILD_COMPLEX16),1)
ifeq ($(SUPPORT_GEMM3M),1)
Z3=zblat3 zblat3_3m
else
Z3=zblat3
endif
endif


ifeq ($(SUPPORT_GEMM3M),1)
level3: $(B3) $(S3) $(D3) $(C3) $(Z3) level3_3m
else
level3: $(B3) $(S3) $(D3) $(C3) $(Z3)
endif

ifneq ($(CROSS), 1)
	rm -f ?BLAT3.SUMM
ifeq ($(BUILD_BFLOAT16),1)
	OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 ./test_sbgemm > SBBLAT3.SUMM
	@$(GREP) -q FATAL SBBLAT3.SUMM && cat SBBLAT3.SUMM || exit 0
endif
ifeq ($(BUILD_SINGLE),1)
	OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 ./sblat3 < ./sblat3.dat
	@$(GREP) -q FATAL SBLAT3.SUMM && cat SBLAT3.SUMM || exit 0
endif
ifeq ($(BUILD_DOUBLE),1)
	OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 ./dblat3 < ./dblat3.dat
	@$(GREP) -q FATAL DBLAT3.SUMM && cat DBLAT3.SUMM || exit 0
endif
ifeq ($(BUILD_COMPLEX),1)
	OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 ./cblat3 < ./cblat3.dat
	@$(GREP) -q FATAL CBLAT3.SUMM && cat CBLAT3.SUMM || exit 0
endif
ifeq ($(BUILD_COMPLEX16),1)
	OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 ./zblat3 < ./zblat3.dat
	@$(GREP) -q FATAL ZBLAT3.SUMM && cat ZBLAT3.SUMM || exit 0
endif
ifdef SMP
	rm -f ?BLAT3.SUMM
ifeq ($(USE_OPENMP), 1)
ifeq ($(BUILD_BFLOAT16),1)
	OMP_NUM_THREADS=2 ./test_sbgemm > SBBLAT3.SUMM
	@$(GREP) -q FATAL SBBLAT3.SUMM && cat SBBLAT3.SUMM || exit 0
endif
ifeq ($(BUILD_SINGLE),1)
	OMP_NUM_THREADS=2 ./sblat3 < ./sblat3.dat
	@$(GREP) -q FATAL SBLAT3.SUMM && cat SBLAT3.SUMM || exit 0
endif
ifeq ($(BUILD_DOUBLE),1)
	OMP_NUM_THREADS=2 ./dblat3 < ./dblat3.dat
	@$(GREP) -q FATAL DBLAT3.SUMM && cat DBLAT3.SUMM || exit 0
endif
ifeq ($(BUILD_COMPLEX),1)
	OMP_NUM_THREADS=2 ./cblat3 < ./cblat3.dat
	@$(GREP) -q FATAL CBLAT3.SUMM && cat CBLAT3.SUMM || exit 0
endif
ifeq ($(BUILD_COMPLEX16),1)
	OMP_NUM_THREADS=2 ./zblat3 < ./zblat3.dat
	@$(GREP) -q FATAL ZBLAT3.SUMM && cat ZBLAT3.SUMM || exit 0
endif
else
ifeq ($(BUILD_BFLOAT16),1)
	OPENBLAS_NUM_THREADS=2 ./test_sbgemm > SBBLAT3.SUMM
	@$(GREP) -q FATAL SBBLAT3.SUMM && cat SBBLAT3.SUMM || exit 0
endif
ifeq ($(BUILD_SINGLE),1)
	OPENBLAS_NUM_THREADS=2 ./sblat3 < ./sblat3.dat
	@$(GREP) -q FATAL SBLAT3.SUMM && cat SBLAT3.SUMM || exit 0
endif
ifeq ($(BUILD_DOUBLE),1)
	OPENBLAS_NUM_THREADS=2 ./dblat3 < ./dblat3.dat
	@$(GREP) -q FATAL DBLAT3.SUMM && cat DBLAT3.SUMM || exit 0
endif
ifeq ($(BUILD_COMPLEX),1)
	OPENBLAS_NUM_THREADS=2 ./cblat3 < ./cblat3.dat
	@$(GREP) -q FATAL CBLAT3.SUMM && cat CBLAT3.SUMM || exit 0
endif
ifeq ($(BUILD_COMPLEX16),1)
	OPENBLAS_NUM_THREADS=2 ./zblat3 < ./zblat3.dat
	@$(GREP) -q FATAL ZBLAT3.SUMM && cat ZBLAT3.SUMM || exit 0
endif
endif
endif
endif


level3_3m: zblat3_3m cblat3_3m
ifneq ($(CROSS), 1)
	rm -f ?BLAT3_3M.SUMM
	OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 ./cblat3_3m < ./cblat3_3m.dat
	@$(GREP) -q FATAL CBLAT3_3M.SUMM && cat CBLAT3_3M.SUMM || exit 0
	OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 ./zblat3_3m < ./zblat3_3m.dat
	@$(GREP) -q FATAL ZBLAT3_3M.SUMM && cat ZBLAT3_3M.SUMM || exit 0
ifdef SMP
	rm -f ?BLAT3_3M.SUMM
ifeq ($(USE_OPENMP), 1)
	OMP_NUM_THREADS=2 ./cblat3_3m < ./cblat3_3m.dat
	@$(GREP) -q FATAL CBLAT3_3M.SUMM && cat CBLAT3_3M.SUMM || exit 0
	OMP_NUM_THREADS=2 ./zblat3_3m < ./zblat3_3m.dat
	@$(GREP) -q FATAL ZBLAT3_3M.SUMM && cat ZBLAT3_3M.SUMM || exit 0
else
	OPENBLAS_NUM_THREADS=2 ./cblat3_3m < ./cblat3_3m.dat
	@$(GREP) -q FATAL CBLAT3_3M.SUMM && cat CBLAT3_3M.SUMM || exit 0
	OPENBLAS_NUM_THREADS=2 ./zblat3_3m < ./zblat3_3m.dat
	@$(GREP) -q FATAL ZBLAT3_3M.SUMM && cat ZBLAT3_3M.SUMM || exit 0
endif
endif
endif




FLDFLAGS = $(FFLAGS:-fPIC=) $(LDFLAGS)
CLDFLAGS = $(CFLAGS) $(LDFLAGS)


ifeq ($(USE_OPENMP), 1)
ifeq ($(F_COMPILER), GFORTRAN)
ifeq ($(C_COMPILER), CLANG)
EXTRALIB += -lomp
endif
endif
ifeq ($(F_COMPILER), NAG)
EXTRALIB = -lgomp
endif
ifeq ($(F_COMPILER), IBM)
ifeq ($(C_COMPILER), GCC)
EXTRALIB += -lgomp
endif
ifeq ($(C_COMPILER), CLANG)
EXTRALIB += -lomp
endif
endif
endif

ifeq ($(BUILD_SINGLE),1)
sblat1 : sblat1.$(SUFFIX) ../$(LIBNAME)
	$(FC) $(FLDFLAGS) -o sblat1 sblat1.$(SUFFIX) ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)

sblat2 : sblat2.$(SUFFIX) ../$(LIBNAME)
	$(FC) $(FLDFLAGS) -o sblat2 sblat2.$(SUFFIX) ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)

sblat3 : sblat3.$(SUFFIX) ../$(LIBNAME)
	$(FC) $(FLDFLAGS) -o sblat3 sblat3.$(SUFFIX) ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)
endif

ifeq ($(BUILD_DOUBLE),1)
dblat1 : dblat1.$(SUFFIX) ../$(LIBNAME)
	$(FC) $(FLDFLAGS) -o dblat1 dblat1.$(SUFFIX) ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)

dblat2 : dblat2.$(SUFFIX) ../$(LIBNAME)
	$(FC) $(FLDFLAGS) -o dblat2 dblat2.$(SUFFIX) ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)

dblat3 : dblat3.$(SUFFIX) ../$(LIBNAME)
	$(FC) $(FLDFLAGS) -o dblat3 dblat3.$(SUFFIX) ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)
else
dblat2:
dblat3:
endif


qblat1 : qblat1.$(SUFFIX) ../$(LIBNAME)
	$(FC) $(FLDFLAGS) -o qblat1 qblat1.$(SUFFIX) ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)

ifeq ($(BUILD_COMPLEX),1)
cblat1 : cblat1.$(SUFFIX) ../$(LIBNAME)
	$(FC) $(FLDFLAGS) -o cblat1 cblat1.$(SUFFIX) ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)

cblat2 : cblat2.$(SUFFIX) ../$(LIBNAME)
	$(FC) $(FLDFLAGS) -o cblat2 cblat2.$(SUFFIX) ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)

cblat3 : cblat3.$(SUFFIX) ../$(LIBNAME)
	$(FC) $(FLDFLAGS) -o cblat3 cblat3.$(SUFFIX) ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)
endif

ifeq ($(BUILD_COMPLEX16),1)
zblat1 : zblat1.$(SUFFIX) ../$(LIBNAME)
	$(FC) $(FLDFLAGS) -o zblat1 zblat1.$(SUFFIX) ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)

zblat2 : zblat2.$(SUFFIX) ../$(LIBNAME)
	$(FC) $(FLDFLAGS) -o zblat2 zblat2.$(SUFFIX) ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)

zblat3 : zblat3.$(SUFFIX) ../$(LIBNAME)
	$(FC) $(FLDFLAGS) -o zblat3 zblat3.$(SUFFIX) ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)
endif

ifeq ($(BUILD_BFLOAT16),1)
test_sbgemm : compare_sgemm_sbgemm.c ../$(LIBNAME)
	$(CC) $(CLDFLAGS) -o test_sbgemm compare_sgemm_sbgemm.c ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)
endif

ifeq ($(BUILD_COMPLEX),1)
cblat3_3m : cblat3_3m.$(SUFFIX) ../$(LIBNAME)
	$(FC) $(FLDFLAGS) -o cblat3_3m cblat3_3m.$(SUFFIX) ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)
endif

ifeq ($(BUILD_COMPLEX16),1)
zblat3_3m : zblat3_3m.$(SUFFIX) ../$(LIBNAME)
	$(FC) $(FLDFLAGS) -o zblat3_3m zblat3_3m.$(SUFFIX) ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)
endif



clean:
	@rm -f *.$(SUFFIX) *.$(PSUFFIX) gmon.$(SUFFIX)ut *.SUMM *.cxml *.exe *.pdb *.dwf \
	sblat1 dblat1 cblat1 zblat1 \
	sblat2 dblat2 cblat2 zblat2 \
	test_sbgemm sblat3 dblat3 cblat3 zblat3 \
	sblat1p dblat1p cblat1p zblat1p \
	sblat2p dblat2p cblat2p zblat2p \
	sblat3p dblat3p cblat3p zblat3p \
	zblat3_3m zblat3_3mp \
	cblat3_3m cblat3_3mp \
	*.stackdump *.dll

libs:

prof:

quick :
	$(MAKE) -C $(TOPDIR) libs

# include ../Makefile.tail
