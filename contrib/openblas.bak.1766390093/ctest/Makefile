#
# The Makefile compiles c wrappers and testers for CBLAS.
#

TOPDIR = ..
include $(TOPDIR)/Makefile.system

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

override CFLAGS += -DADD$(BU) -DCBLAS
ifeq ($(F_COMPILER),GFORTRAN)
ifneq (, $(filter $(CORE),LOONGSON3R3 LOONGSON3R4))
	override FFLAGS := $(filter_out(-O2 -O3,$(FFLAGS))) -O0
endif
	override FFLAGS += -fno-tree-vectorize
endif
override TARGET_ARCH=
override TARGET_MACH=

LIB = $(TOPDIR)/$(LIBNAME)

stestl1o = c_sblas1.o

stestl2o = c_sblas2.o c_s2chke.o auxiliary.o c_xerbla.o constant.o

stestl3o = c_sblas3.o c_s3chke.o auxiliary.o c_xerbla.o constant.o

dtestl1o = c_dblas1.o

dtestl2o = c_dblas2.o c_d2chke.o auxiliary.o c_xerbla.o constant.o

dtestl3o = c_dblas3.o c_d3chke.o auxiliary.o c_xerbla.o constant.o

ctestl1o = c_cblas1.o

ctestl2o = c_cblas2.o c_c2chke.o auxiliary.o c_xerbla.o constant.o

ctestl3o = c_cblas3.o c_c3chke.o auxiliary.o c_xerbla.o constant.o

ctestl3o_3m = c_cblas3_3m.o c_c3chke_3m.o auxiliary.o c_xerbla.o constant.o

ztestl1o = c_zblas1.o

ztestl2o = c_zblas2.o c_z2chke.o auxiliary.o c_xerbla.o constant.o

ztestl3o = c_zblas3.o c_z3chke.o auxiliary.o c_xerbla.o constant.o

ztestl3o_3m = c_zblas3_3m.o c_z3chke_3m.o auxiliary.o c_xerbla.o constant.o


all :: all1 all2 all3

ifeq ($(BUILD_SINGLE),1)
all1targets += xscblat1
endif
ifeq ($(BUILD_DOUBLE),1)
all1targets += xdcblat1
endif
ifeq ($(BUILD_COMPLEX),1)
all1targets += xccblat1
endif
ifeq ($(BUILD_COMPLEX16),1)
all1targets += xzcblat1
endif

all1: $(all1targets)

ifneq ($(CROSS), 1)
ifeq ($(USE_OPENMP), 1)
ifeq ($(BUILD_SINGLE),1)
	OMP_NUM_THREADS=2 ./xscblat1
endif
ifeq ($(BUILD_DOUBLE),1)
	OMP_NUM_THREADS=2 ./xdcblat1
endif	
ifeq ($(BUILD_COMPLEX),1)
	OMP_NUM_THREADS=2 ./xccblat1
endif
ifeq ($(BUILD_COMPLEX16),1)
	OMP_NUM_THREADS=2 ./xzcblat1
endif
else
ifeq ($(BUILD_SINGLE),1)
	OPENBLAS_NUM_THREADS=2 ./xscblat1
endif
ifeq ($(BUILD_DOUBLE),1)
	OPENBLAS_NUM_THREADS=2 ./xdcblat1
endif
ifeq ($(BUILD_COMPLEX),1)
	OPENBLAS_NUM_THREADS=2 ./xccblat1
endif
ifeq ($(BUILD_COMPLEX16),1)
	OPENBLAS_NUM_THREADS=2 ./xzcblat1
endif
endif
endif

ifeq ($(BUILD_SINGLE),1)
all2targets += xscblat2
endif
ifeq ($(BUILD_DOUBLE),1)
all2targets += xdcblat2
endif
ifeq ($(BUILD_COMPLEX),1)
all2targets += xccblat2
endif
ifeq ($(BUILD_COMPLEX16),1)
all2targets += xzcblat2
endif

all2: $(all2targets)

ifneq ($(CROSS), 1)
ifeq ($(USE_OPENMP), 1)
ifeq ($(BUILD_SINGLE),1)
	OMP_NUM_THREADS=2 ./xscblat2 < sin2
endif
ifeq ($(BUILD_DOUBLE),1)
	OMP_NUM_THREADS=2 ./xdcblat2 < din2
endif
ifeq ($(BUILD_COMPLEX),1)
	OMP_NUM_THREADS=2 ./xccblat2 < cin2
endif
ifeq ($(BUILD_COMPLEX16),1)
	OMP_NUM_THREADS=2 ./xzcblat2 < zin2
endif
else
ifeq ($(BUILD_SINGLE),1)
	OPENBLAS_NUM_THREADS=2 ./xscblat2 < sin2
endif
ifeq ($(BUILD_DOUBLE),1)
	OPENBLAS_NUM_THREADS=2 ./xdcblat2 < din2
endif
ifeq ($(BUILD_COMPLEX),1)
	OPENBLAS_NUM_THREADS=2 ./xccblat2 < cin2
endif
ifeq ($(BUILD_COMPLEX16),1)
	OPENBLAS_NUM_THREADS=2 ./xzcblat2 < zin2
endif
endif
endif


ifeq ($(BUILD_SINGLE),1)
all3targets += xscblat3
endif
ifeq ($(BUILD_DOUBLE),1)
all3targets += xdcblat3
endif
ifeq ($(BUILD_COMPLEX),1)
all3targets += xccblat3
ifeq ($(SUPPORT_GEMM3M),1)
all3targets += xccblat3_3m
endif
endif
ifeq ($(BUILD_COMPLEX16),1)
all3targets += xzcblat3
ifeq ($(SUPPORT_GEMM3M),1)
all3targets += xzcblat3_3m
endif
endif

all3: $(all3targets)

ifneq ($(CROSS), 1)
ifeq ($(USE_OPENMP), 1)
ifeq ($(BUILD_SINGLE),1)
	OMP_NUM_THREADS=2 ./xscblat3 < sin3
endif
ifeq ($(BUILD_DOUBLE),1)
	OMP_NUM_THREADS=2 ./xdcblat3 < din3
endif
ifeq ($(BUILD_COMPLEX),1)
	OMP_NUM_THREADS=2 ./xccblat3 < cin3
endif
ifeq ($(BUILD_COMPLEX16),1)
	OMP_NUM_THREADS=2 ./xzcblat3 < zin3
endif
else
ifeq ($(BUILD_SINGLE),1)
	OPENBLAS_NUM_THREADS=2 ./xscblat3 < sin3
endif
ifeq ($(BUILD_DOUBLE),1)
	OPENBLAS_NUM_THREADS=2 ./xdcblat3 < din3
endif
ifeq ($(BUILD_COMPLEX),1)
	OPENBLAS_NUM_THREADS=2 ./xccblat3 < cin3
endif
ifeq ($(BUILD_COMPLEX16),1)
	OPENBLAS_NUM_THREADS=2 ./xzcblat3 < zin3
endif
endif

ifeq ($(SUPPORT_GEMM3M),1)
ifeq ($(USE_OPENMP), 1)
ifeq ($(BUILD_COMPLEX),1)
	OMP_NUM_THREADS=2 ./xccblat3_3m < cin3_3m
endif
ifeq ($(BUILD_COMPLEX16),1)
	OMP_NUM_THREADS=2 ./xzcblat3_3m < zin3_3m
endif
else
ifeq ($(BUILD_COMPLEX),1)
	OPENBLAS_NUM_THREADS=2 ./xccblat3_3m < cin3_3m
endif
ifeq ($(BUILD_COMPLEX16),1)
	OPENBLAS_NUM_THREADS=2 ./xzcblat3_3m < zin3_3m
endif
endif
endif
endif



clean ::
	rm -f x*

FLDFLAGS = $(FFLAGS:-fPIC=) $(LDFLAGS)
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
# Single real
ifeq ($(NOFORTRAN), $(filter 0 2,$(NOFORTRAN)))
xscblat1: $(stestl1o) c_sblat1.o $(TOPDIR)/$(LIBNAME)
	$(FC) $(FLDFLAGS) -o xscblat1 c_sblat1.o $(stestl1o) $(LIB) $(EXTRALIB) $(CEXTRALIB)
xscblat2: $(stestl2o) c_sblat2.o $(TOPDIR)/$(LIBNAME)
	$(FC) $(FLDFLAGS) -o xscblat2 c_sblat2.o $(stestl2o) $(LIB) $(EXTRALIB) $(CEXTRALIB)
xscblat3: $(stestl3o) c_sblat3.o $(TOPDIR)/$(LIBNAME)
	$(FC) $(FLDFLAGS) -o xscblat3 c_sblat3.o $(stestl3o) $(LIB) $(EXTRALIB) $(CEXTRALIB)
else
xscblat1: $(stestl1o) c_sblat1c.o $(TOPDIR)/$(LIBNAME)
	$(CC) $(CFLAGS) -o xscblat1 c_sblat1c.o $(stestl1o) $(LIB) $(CEXTRALIB) $(filter-out -lgfortran,$(EXTRALIB))
xscblat2: $(stestl2o) c_sblat2c.o $(TOPDIR)/$(LIBNAME)
	$(CC) $(CFLAGS) -o xscblat2 c_sblat2c.o $(stestl2o) $(LIB) $(CEXTRALIB) $(filter-out -lgfortran,$(EXTRALIB))
xscblat3: $(stestl3o) c_sblat3c.o $(TOPDIR)/$(LIBNAME)
	$(CC) $(CFLAGS) -o xscblat3 c_sblat3c.o $(stestl3o) $(LIB) $(CEXTRALIB) $(filter-out -lgfortran,$(EXTRALIB))
endif
endif

ifeq ($(BUILD_DOUBLE),1)
# Double real
ifeq ($(NOFORTRAN), $(filter 0 2,$(NOFORTRAN)))
xdcblat1: $(dtestl1o) c_dblat1.o $(TOPDIR)/$(LIBNAME)
	$(FC) $(FLDFLAGS) -o xdcblat1 c_dblat1.o $(dtestl1o) $(LIB) $(EXTRALIB) $(CEXTRALIB)
xdcblat2: $(dtestl2o) c_dblat2.o $(TOPDIR)/$(LIBNAME)
	$(FC) $(FLDFLAGS) -o xdcblat2 c_dblat2.o $(dtestl2o) $(LIB) $(EXTRALIB) $(CEXTRALIB)
xdcblat3: $(dtestl3o) c_dblat3.o $(TOPDIR)/$(LIBNAME)
	$(FC) $(FLDFLAGS) -o xdcblat3 c_dblat3.o $(dtestl3o) $(LIB) $(EXTRALIB) $(CEXTRALIB)
else
xdcblat1: $(dtestl1o) c_dblat1c.o $(TOPDIR)/$(LIBNAME)
	$(CC) $(CFLAGS) -o xdcblat1 c_dblat1c.o $(dtestl1o) $(LIB) $(CEXTRALIB) $(filter-out -lgfortran,$(EXTRALIB))
xdcblat2: $(dtestl2o) c_dblat2c.o $(TOPDIR)/$(LIBNAME)
	$(CC) $(CFLAGS) -o xdcblat2 c_dblat2c.o $(dtestl2o) $(LIB) $(CEXTRALIB) $(filter-out -lgfortran,$(EXTRALIB))
xdcblat3: $(dtestl3o) c_dblat3c.o $(TOPDIR)/$(LIBNAME)
	$(CC) $(CFLAGS) -o xdcblat3 c_dblat3c.o $(dtestl3o) $(LIB) $(CEXTRALIB) $(filter-out -lgfortran,$(EXTRALIB))
endif
endif

ifeq ($(BUILD_COMPLEX),1)
# Single complex
ifeq ($(NOFORTRAN), $(filter 0 2,$(NOFORTRAN)))
xccblat1: $(ctestl1o) c_cblat1.o $(TOPDIR)/$(LIBNAME)
	$(FC) $(FLDFLAGS) -o xccblat1 c_cblat1.o $(ctestl1o) $(LIB) $(EXTRALIB) $(CEXTRALIB)
xccblat2: $(ctestl2o) c_cblat2.o $(TOPDIR)/$(LIBNAME)
	$(FC) $(FLDFLAGS) -o xccblat2 c_cblat2.o $(ctestl2o) $(LIB) $(EXTRALIB) $(CEXTRALIB)
xccblat3: $(ctestl3o) c_cblat3.o $(TOPDIR)/$(LIBNAME)
	$(FC) $(FLDFLAGS) -o xccblat3 c_cblat3.o $(ctestl3o) $(LIB) $(EXTRALIB) $(CEXTRALIB)
ifeq ($(SUPPORT_GEMM3M),1)
xccblat3_3m: $(ctestl3o_3m) c_cblat3_3m.o $(TOPDIR)/$(LIBNAME)
	$(FC) $(FLDFLAGS) -o xccblat3_3m c_cblat3_3m.o $(ctestl3o_3m) $(LIB) $(EXTRALIB) $(CEXTRALIB)
endif
else
xccblat1: $(ctestl1o) c_cblat1c.o $(TOPDIR)/$(LIBNAME)
	$(CC) $(CFLAGS) -o xccblat1 c_cblat1c.o $(ctestl1o) $(LIB) $(CEXTRALIB) $(filter-out -lgfortran,$(EXTRALIB)) 
xccblat2: $(ctestl2o) c_cblat2c.o $(TOPDIR)/$(LIBNAME)
	$(CC) $(CFLAGS) -o xccblat2 c_cblat2c.o $(ctestl2o) $(LIB) $(CEXTRALIB) $(filter-out -lgfortran,$(EXTRALIB))
xccblat3: $(ctestl3o) c_cblat3c.o $(TOPDIR)/$(LIBNAME)
	$(CC) $(CFLAGS) -o xccblat3 c_cblat3c.o $(ctestl3o) $(LIB) $(CEXTRALIB) $(filter-out -lgfortran,$(EXTRALIB))
ifeq ($(SUPPORT_GEMM3M),1)
xccblat3_3m: $(ctestl3o_3m) c_cblat3c_3m.o $(TOPDIR)/$(LIBNAME)
	$(CC) $(CFLAGS) -o xccblat3_3m c_cblat3c_3m.o $(ctestl3o_3m) $(LIB) $(EXTRALIB) $(CEXTRALIB)
endif
endif
endif


ifeq ($(BUILD_COMPLEX16),1)
# Double complex
ifeq ($(NOFORTRAN), $(filter 0 2,$(NOFORTRAN)))
xzcblat1: $(ztestl1o) c_zblat1.o $(TOPDIR)/$(LIBNAME)
	$(FC) $(FLDFLAGS) -o xzcblat1 c_zblat1.o $(ztestl1o) $(LIB) $(EXTRALIB) $(CEXTRALIB)
xzcblat2: $(ztestl2o) c_zblat2.o $(TOPDIR)/$(LIBNAME)
	$(FC) $(FLDFLAGS) -o xzcblat2 c_zblat2.o $(ztestl2o) $(LIB) $(EXTRALIB) $(CEXTRALIB)
xzcblat3: $(ztestl3o) c_zblat3.o $(TOPDIR)/$(LIBNAME)
	$(FC) $(FLDFLAGS) -o xzcblat3 c_zblat3.o $(ztestl3o) $(LIB) $(EXTRALIB) $(CEXTRALIB)
ifeq ($(SUPPORT_GEMM3M),1)
xzcblat3_3m: $(ztestl3o_3m) c_zblat3_3m.o $(TOPDIR)/$(LIBNAME)
	$(FC) $(FLDFLAGS) -o xzcblat3_3m c_zblat3_3m.o $(ztestl3o_3m) $(LIB) $(EXTRALIB) $(CEXTRALIB)
endif
else
xzcblat1: $(ztestl1o) c_zblat1c.o $(TOPDIR)/$(LIBNAME)
	$(CC) $(CFLAGS) -o xzcblat1 c_zblat1c.o $(ztestl1o) $(LIB) $(CEXTRALIB) $(filter-out -lgfortran,$(EXTRALIB)) 
xzcblat2: $(ztestl2o) c_zblat2c.o $(TOPDIR)/$(LIBNAME)
	$(CC) $(CFLAGS) -o xzcblat2 c_zblat2c.o $(ztestl2o) $(LIB) $(CEXTRALIB) $(filter-out -lgfortran,$(EXTRALIB))
xzcblat3: $(ztestl3o) c_zblat3c.o $(TOPDIR)/$(LIBNAME)
	$(CC) $(CFLAGS) -o xzcblat3 c_zblat3c.o $(ztestl3o) $(LIB) $(CEXTRALIB) $(filter-out -lgfortran,$(EXTRALIB))
ifeq ($(SUPPORT_GEMM3M),1)
xzcblat3_3m: $(ztestl3o_3m) c_zblat3c_3m.o $(TOPDIR)/$(LIBNAME)
	$(CC) $(CFLAGS) -o xzcblat3_3m c_zblat3c_3m.o $(ztestl3o_3m) $(LIB) $(EXTRALIB) $(CEXTRALIB)
endif
endif
endif

include $(TOPDIR)/Makefile.tail
