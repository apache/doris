TOPDIR	= ../..
include ../../Makefile.system

SBLASOBJS = strtrs_UNU_single.$(SUFFIX) strtrs_UNN_single.$(SUFFIX) strtrs_UTU_single.$(SUFFIX) strtrs_UTN_single.$(SUFFIX) strtrs_LNU_single.$(SUFFIX) strtrs_LNN_single.$(SUFFIX) strtrs_LTU_single.$(SUFFIX) strtrs_LTN_single.$(SUFFIX)
DBLASOBJS = dtrtrs_UNU_single.$(SUFFIX) dtrtrs_UNN_single.$(SUFFIX) dtrtrs_UTU_single.$(SUFFIX) dtrtrs_UTN_single.$(SUFFIX) dtrtrs_LNU_single.$(SUFFIX) dtrtrs_LNN_single.$(SUFFIX) dtrtrs_LTU_single.$(SUFFIX) dtrtrs_LTN_single.$(SUFFIX)
QBLASOBJS = qtrtrs_UNU_single.$(SUFFIX) qtrtrs_UNN_single.$(SUFFIX) qtrtrs_UTU_single.$(SUFFIX) qtrtrs_UTN_single.$(SUFFIX) qtrtrs_LNU_single.$(SUFFIX) qtrtrs_LNN_single.$(SUFFIX) qtrtrs_LTU_single.$(SUFFIX) qtrtrs_LTN_single.$(SUFFIX)
CBLASOBJS = ctrtrs_UNU_single.$(SUFFIX) ctrtrs_UNN_single.$(SUFFIX) ctrtrs_UTU_single.$(SUFFIX) ctrtrs_UTN_single.$(SUFFIX) ctrtrs_URU_single.$(SUFFIX) ctrtrs_URN_single.$(SUFFIX) ctrtrs_UCU_single.$(SUFFIX) ctrtrs_UCN_single.$(SUFFIX) ctrtrs_LNU_single.$(SUFFIX) ctrtrs_LNN_single.$(SUFFIX) ctrtrs_LTU_single.$(SUFFIX) ctrtrs_LTN_single.$(SUFFIX) ctrtrs_LRU_single.$(SUFFIX) ctrtrs_LRN_single.$(SUFFIX) ctrtrs_LCU_single.$(SUFFIX) ctrtrs_LCN_single.$(SUFFIX)
ZBLASOBJS = ztrtrs_UNU_single.$(SUFFIX) ztrtrs_UNN_single.$(SUFFIX) ztrtrs_UTU_single.$(SUFFIX) ztrtrs_UTN_single.$(SUFFIX) ztrtrs_URU_single.$(SUFFIX) ztrtrs_URN_single.$(SUFFIX) ztrtrs_UCU_single.$(SUFFIX) ztrtrs_UCN_single.$(SUFFIX) ztrtrs_LNU_single.$(SUFFIX) ztrtrs_LNN_single.$(SUFFIX) ztrtrs_LTU_single.$(SUFFIX) ztrtrs_LTN_single.$(SUFFIX) ztrtrs_LRU_single.$(SUFFIX) ztrtrs_LRN_single.$(SUFFIX) ztrtrs_LCU_single.$(SUFFIX) ztrtrs_LCN_single.$(SUFFIX)
XBLASOBJS = xtrtrs_UNU_single.$(SUFFIX) xtrtrs_UNN_single.$(SUFFIX) xtrtrs_UTU_single.$(SUFFIX) xtrtrs_UTN_single.$(SUFFIX) xtrtrs_URU_single.$(SUFFIX) xtrtrs_URN_single.$(SUFFIX) xtrtrs_UCU_single.$(SUFFIX) xtrtrs_UCN_single.$(SUFFIX) xtrtrs_LNU_single.$(SUFFIX) xtrtrs_LNN_single.$(SUFFIX) xtrtrs_LTU_single.$(SUFFIX) xtrtrs_LTN_single.$(SUFFIX) xtrtrs_LRU_single.$(SUFFIX) xtrtrs_LRN_single.$(SUFFIX) xtrtrs_LCU_single.$(SUFFIX) xtrtrs_LCN_single.$(SUFFIX)

ifdef SMP
SBLASOBJS += strtrs_UNU_parallel.$(SUFFIX) strtrs_UNN_parallel.$(SUFFIX) strtrs_UTU_parallel.$(SUFFIX) strtrs_UTN_parallel.$(SUFFIX) strtrs_LNU_parallel.$(SUFFIX) strtrs_LNN_parallel.$(SUFFIX) strtrs_LTU_parallel.$(SUFFIX) strtrs_LTN_parallel.$(SUFFIX)
DBLASOBJS += dtrtrs_UNU_parallel.$(SUFFIX) dtrtrs_UNN_parallel.$(SUFFIX) dtrtrs_UTU_parallel.$(SUFFIX) dtrtrs_UTN_parallel.$(SUFFIX) dtrtrs_LNU_parallel.$(SUFFIX) dtrtrs_LNN_parallel.$(SUFFIX) dtrtrs_LTU_parallel.$(SUFFIX) dtrtrs_LTN_parallel.$(SUFFIX)
QBLASOBJS += qtrtrs_UNU_parallel.$(SUFFIX) qtrtrs_UNN_parallel.$(SUFFIX) qtrtrs_UTU_parallel.$(SUFFIX) qtrtrs_UTN_parallel.$(SUFFIX) qtrtrs_LNU_parallel.$(SUFFIX) qtrtrs_LNN_parallel.$(SUFFIX) qtrtrs_LTU_parallel.$(SUFFIX) qtrtrs_LTN_parallel.$(SUFFIX)
CBLASOBJS += ctrtrs_UNU_parallel.$(SUFFIX) ctrtrs_UNN_parallel.$(SUFFIX) ctrtrs_UTU_parallel.$(SUFFIX) ctrtrs_UTN_parallel.$(SUFFIX) ctrtrs_URU_parallel.$(SUFFIX) ctrtrs_URN_parallel.$(SUFFIX) ctrtrs_UCU_parallel.$(SUFFIX) ctrtrs_UCN_parallel.$(SUFFIX) ctrtrs_LNU_parallel.$(SUFFIX) ctrtrs_LNN_parallel.$(SUFFIX) ctrtrs_LTU_parallel.$(SUFFIX) ctrtrs_LTN_parallel.$(SUFFIX) ctrtrs_LRU_parallel.$(SUFFIX) ctrtrs_LRN_parallel.$(SUFFIX) ctrtrs_LCU_parallel.$(SUFFIX) ctrtrs_LCN_parallel.$(SUFFIX)
ZBLASOBJS += ztrtrs_UNU_parallel.$(SUFFIX) ztrtrs_UNN_parallel.$(SUFFIX) ztrtrs_UTU_parallel.$(SUFFIX) ztrtrs_UTN_parallel.$(SUFFIX) ztrtrs_URU_parallel.$(SUFFIX) ztrtrs_URN_parallel.$(SUFFIX) ztrtrs_UCU_parallel.$(SUFFIX) ztrtrs_UCN_parallel.$(SUFFIX) ztrtrs_LNU_parallel.$(SUFFIX) ztrtrs_LNN_parallel.$(SUFFIX) ztrtrs_LTU_parallel.$(SUFFIX) ztrtrs_LTN_parallel.$(SUFFIX) ztrtrs_LRU_parallel.$(SUFFIX) ztrtrs_LRN_parallel.$(SUFFIX) ztrtrs_LCU_parallel.$(SUFFIX) ztrtrs_LCN_parallel.$(SUFFIX)
XBLASOBJS += xtrtrs_UNU_parallel.$(SUFFIX) xtrtrs_UNN_parallel.$(SUFFIX) xtrtrs_UTU_parallel.$(SUFFIX) xtrtrs_UTN_parallel.$(SUFFIX) xtrtrs_URU_parallel.$(SUFFIX) xtrtrs_URN_parallel.$(SUFFIX) xtrtrs_UCU_parallel.$(SUFFIX) xtrtrs_UCN_parallel.$(SUFFIX) xtrtrs_LNU_parallel.$(SUFFIX) xtrtrs_LNN_parallel.$(SUFFIX) xtrtrs_LTU_parallel.$(SUFFIX) xtrtrs_LTN_parallel.$(SUFFIX) xtrtrs_LRU_parallel.$(SUFFIX) xtrtrs_LRN_parallel.$(SUFFIX) xtrtrs_LCU_parallel.$(SUFFIX) xtrtrs_LCN_parallel.$(SUFFIX)
endif

ifneq ($(BUILD_SINGLE),1)
SBLASOBJS=
endif
ifneq ($(BUILD_DOUBLE),1)
DBLASOBJS=
endif
ifneq ($(BUILD_COMPLEX),1)
CBLASOBJS=
endif
ifneq ($(BUILD_COMPLEX16),1)
ZBLASOBJS=
endif

strtrs_UNU_single.$(SUFFIX) : trtrs_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UUPLO -UTRANS -UDIAG $< -o $(@F)

strtrs_UNN_single.$(SUFFIX) : trtrs_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UUPLO -UTRANS -DDIAG $< -o $(@F)

strtrs_UTU_single.$(SUFFIX) : trtrs_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UUPLO -DTRANS -UDIAG $< -o $(@F)

strtrs_UTN_single.$(SUFFIX) : trtrs_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UUPLO -DTRANS -DDIAG $< -o $(@F)

strtrs_LNU_single.$(SUFFIX) : trtrs_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DUPLO -UTRANS -UDIAG $< -o $(@F)

strtrs_LNN_single.$(SUFFIX) : trtrs_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DUPLO -UTRANS -DDIAG $< -o $(@F)

strtrs_LTU_single.$(SUFFIX) : trtrs_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DUPLO -DTRANS -UDIAG $< -o $(@F)

strtrs_LTN_single.$(SUFFIX) : trtrs_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DUPLO -DTRANS -DDIAG $< -o $(@F)

strtrs_UNU_parallel.$(SUFFIX) : trtrs_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UUPLO -UTRANS -UDIAG $< -o $(@F)

strtrs_UNN_parallel.$(SUFFIX) : trtrs_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UUPLO -UTRANS -DDIAG $< -o $(@F)

strtrs_UTU_parallel.$(SUFFIX) : trtrs_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UUPLO -DTRANS -UDIAG $< -o $(@F)

strtrs_UTN_parallel.$(SUFFIX) : trtrs_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UUPLO -DTRANS -DDIAG $< -o $(@F)

strtrs_LNU_parallel.$(SUFFIX) : trtrs_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DUPLO -UTRANS -UDIAG $< -o $(@F)

strtrs_LNN_parallel.$(SUFFIX) : trtrs_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DUPLO -UTRANS -DDIAG $< -o $(@F)

strtrs_LTU_parallel.$(SUFFIX) : trtrs_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DUPLO -DTRANS -UDIAG $< -o $(@F)

strtrs_LTN_parallel.$(SUFFIX) : trtrs_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DUPLO -DTRANS -DDIAG $< -o $(@F)

dtrtrs_UNU_single.$(SUFFIX) : trtrs_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UUPLO -UTRANS -UDIAG $< -o $(@F)

dtrtrs_UNN_single.$(SUFFIX) : trtrs_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UUPLO -UTRANS -DDIAG $< -o $(@F)

dtrtrs_UTU_single.$(SUFFIX) : trtrs_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UUPLO -DTRANS -UDIAG $< -o $(@F)

dtrtrs_UTN_single.$(SUFFIX) : trtrs_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UUPLO -DTRANS -DDIAG $< -o $(@F)

dtrtrs_LNU_single.$(SUFFIX) : trtrs_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DUPLO -UTRANS -UDIAG $< -o $(@F)

dtrtrs_LNN_single.$(SUFFIX) : trtrs_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DUPLO -UTRANS -DDIAG $< -o $(@F)

dtrtrs_LTU_single.$(SUFFIX) : trtrs_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DUPLO -DTRANS -UDIAG $< -o $(@F)

dtrtrs_LTN_single.$(SUFFIX) : trtrs_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DUPLO -DTRANS -DDIAG $< -o $(@F)

dtrtrs_UNU_parallel.$(SUFFIX) : trtrs_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UUPLO -UTRANS -UDIAG $< -o $(@F)

dtrtrs_UNN_parallel.$(SUFFIX) : trtrs_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UUPLO -UTRANS -DDIAG $< -o $(@F)

dtrtrs_UTU_parallel.$(SUFFIX) : trtrs_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UUPLO -DTRANS -UDIAG $< -o $(@F)

dtrtrs_UTN_parallel.$(SUFFIX) : trtrs_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UUPLO -DTRANS -DDIAG $< -o $(@F)

dtrtrs_LNU_parallel.$(SUFFIX) : trtrs_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DUPLO -UTRANS -UDIAG $< -o $(@F)

dtrtrs_LNN_parallel.$(SUFFIX) : trtrs_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DUPLO -UTRANS -DDIAG $< -o $(@F)

dtrtrs_LTU_parallel.$(SUFFIX) : trtrs_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DUPLO -DTRANS -UDIAG $< -o $(@F)

dtrtrs_LTN_parallel.$(SUFFIX) : trtrs_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DUPLO -DTRANS -DDIAG $< -o $(@F)

qtrtrs_UNU_single.$(SUFFIX) : trtrs_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UUPLO -UTRANS -UDIAG $< -o $(@F)

qtrtrs_UNN_single.$(SUFFIX) : trtrs_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UUPLO -UTRANS -DDIAG $< -o $(@F)

qtrtrs_UTU_single.$(SUFFIX) : trtrs_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UUPLO -DTRANS -UDIAG $< -o $(@F)

qtrtrs_UTN_single.$(SUFFIX) : trtrs_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UUPLO -DTRANS -DDIAG $< -o $(@F)

qtrtrs_LNU_single.$(SUFFIX) : trtrs_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DUPLO -UTRANS -UDIAG $< -o $(@F)

qtrtrs_LNN_single.$(SUFFIX) : trtrs_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DUPLO -UTRANS -DDIAG $< -o $(@F)

qtrtrs_LTU_single.$(SUFFIX) : trtrs_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DUPLO -DTRANS -UDIAG $< -o $(@F)

qtrtrs_LTN_single.$(SUFFIX) : trtrs_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DUPLO -DTRANS -DDIAG $< -o $(@F)

qtrtrs_UNU_parallel.$(SUFFIX) : trtrs_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UUPLO -UTRANS -UDIAG $< -o $(@F)

qtrtrs_UNN_parallel.$(SUFFIX) : trtrs_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UUPLO -UTRANS -DDIAG $< -o $(@F)

qtrtrs_UTU_parallel.$(SUFFIX) : trtrs_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UUPLO -DTRANS -UDIAG $< -o $(@F)

qtrtrs_UTN_parallel.$(SUFFIX) : trtrs_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UUPLO -DTRANS -DDIAG $< -o $(@F)

qtrtrs_LNU_parallel.$(SUFFIX) : trtrs_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DUPLO -UTRANS -UDIAG $< -o $(@F)

qtrtrs_LNN_parallel.$(SUFFIX) : trtrs_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DUPLO -UTRANS -DDIAG $< -o $(@F)

qtrtrs_LTU_parallel.$(SUFFIX) : trtrs_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DUPLO -DTRANS -UDIAG $< -o $(@F)

qtrtrs_LTN_parallel.$(SUFFIX) : trtrs_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DUPLO -DTRANS -DDIAG $< -o $(@F)

ctrtrs_UNU_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UUPLO -DTRANS=1 -UDIAG $< -o $(@F)

ctrtrs_UNN_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UUPLO -DTRANS=1 -DDIAG $< -o $(@F)

ctrtrs_UTU_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UUPLO -DTRANS=2 -UDIAG $< -o $(@F)

ctrtrs_UTN_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UUPLO -DTRANS=2 -DDIAG $< -o $(@F)

ctrtrs_URU_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UUPLO -DTRANS=3 -UDIAG $< -o $(@F)

ctrtrs_URN_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UUPLO -DTRANS=3 -DDIAG $< -o $(@F)

ctrtrs_UCU_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UUPLO -DTRANS=4 -UDIAG $< -o $(@F)

ctrtrs_UCN_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UUPLO -DTRANS=4 -DDIAG $< -o $(@F)

ctrtrs_LNU_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DUPLO -DTRANS=1 -UDIAG $< -o $(@F)

ctrtrs_LNN_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DUPLO -DTRANS=1 -DDIAG $< -o $(@F)

ctrtrs_LTU_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DUPLO -DTRANS=2 -UDIAG $< -o $(@F)

ctrtrs_LTN_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DUPLO -DTRANS=2 -DDIAG $< -o $(@F)

ctrtrs_LRU_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DUPLO -DTRANS=3 -UDIAG $< -o $(@F)

ctrtrs_LRN_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DUPLO -DTRANS=3 -DDIAG $< -o $(@F)

ctrtrs_LCU_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DUPLO -DTRANS=4 -UDIAG $< -o $(@F)

ctrtrs_LCN_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DUPLO -DTRANS=4 -DDIAG $< -o $(@F)

ztrtrs_UNU_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UUPLO -DTRANS=1 -UDIAG $< -o $(@F)

ztrtrs_UNN_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UUPLO -DTRANS=1 -DDIAG $< -o $(@F)

ztrtrs_UTU_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UUPLO -DTRANS=2 -UDIAG $< -o $(@F)

ztrtrs_UTN_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UUPLO -DTRANS=2 -DDIAG $< -o $(@F)

ztrtrs_URU_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UUPLO -DTRANS=3 -UDIAG $< -o $(@F)

ztrtrs_URN_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UUPLO -DTRANS=3 -DDIAG $< -o $(@F)

ztrtrs_UCU_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UUPLO -DTRANS=4 -UDIAG $< -o $(@F)

ztrtrs_UCN_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UUPLO -DTRANS=4 -DDIAG $< -o $(@F)

ztrtrs_LNU_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DUPLO -DTRANS=1 -UDIAG $< -o $(@F)

ztrtrs_LNN_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DUPLO -DTRANS=1 -DDIAG $< -o $(@F)

ztrtrs_LTU_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DUPLO -DTRANS=2 -UDIAG $< -o $(@F)

ztrtrs_LTN_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DUPLO -DTRANS=2 -DDIAG $< -o $(@F)

ztrtrs_LRU_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DUPLO -DTRANS=3 -UDIAG $< -o $(@F)

ztrtrs_LRN_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DUPLO -DTRANS=3 -DDIAG $< -o $(@F)

ztrtrs_LCU_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DUPLO -DTRANS=4 -UDIAG $< -o $(@F)

ztrtrs_LCN_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DUPLO -DTRANS=4 -DDIAG $< -o $(@F)

xtrtrs_UNU_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UUPLO -DTRANS=1 -UDIAG $< -o $(@F)

xtrtrs_UNN_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UUPLO -DTRANS=1 -DDIAG $< -o $(@F)

xtrtrs_UTU_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UUPLO -DTRANS=2 -UDIAG $< -o $(@F)

xtrtrs_UTN_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UUPLO -DTRANS=2 -DDIAG $< -o $(@F)

xtrtrs_URU_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UUPLO -DTRANS=3 -UDIAG $< -o $(@F)

xtrtrs_URN_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UUPLO -DTRANS=3 -DDIAG $< -o $(@F)

xtrtrs_UCU_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UUPLO -DTRANS=4 -UDIAG $< -o $(@F)

xtrtrs_UCN_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UUPLO -DTRANS=4 -DDIAG $< -o $(@F)

xtrtrs_LNU_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DUPLO -DTRANS=1 -UDIAG $< -o $(@F)

xtrtrs_LNN_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DUPLO -DTRANS=1 -DDIAG $< -o $(@F)

xtrtrs_LTU_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DUPLO -DTRANS=2 -UDIAG $< -o $(@F)

xtrtrs_LTN_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DUPLO -DTRANS=2 -DDIAG $< -o $(@F)

xtrtrs_LRU_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DUPLO -DTRANS=3 -UDIAG $< -o $(@F)

xtrtrs_LRN_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DUPLO -DTRANS=3 -DDIAG $< -o $(@F)

xtrtrs_LCU_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DUPLO -DTRANS=4 -UDIAG $< -o $(@F)

xtrtrs_LCN_single.$(SUFFIX) : ztrtrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DUPLO -DTRANS=4 -DDIAG $< -o $(@F)

ctrtrs_UNU_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UUPLO -DTRANS=1 -UDIAG $< -o $(@F)

ctrtrs_UNN_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UUPLO -DTRANS=1 -DDIAG $< -o $(@F)

ctrtrs_UTU_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UUPLO -DTRANS=2 -UDIAG $< -o $(@F)

ctrtrs_UTN_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UUPLO -DTRANS=2 -DDIAG $< -o $(@F)

ctrtrs_URU_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UUPLO -DTRANS=3 -UDIAG $< -o $(@F)

ctrtrs_URN_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UUPLO -DTRANS=3 -DDIAG $< -o $(@F)

ctrtrs_UCU_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UUPLO -DTRANS=4 -UDIAG $< -o $(@F)

ctrtrs_UCN_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UUPLO -DTRANS=4 -DDIAG $< -o $(@F)

ctrtrs_LNU_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DUPLO -DTRANS=1 -UDIAG $< -o $(@F)

ctrtrs_LNN_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DUPLO -DTRANS=1 -DDIAG $< -o $(@F)

ctrtrs_LTU_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DUPLO -DTRANS=2 -UDIAG $< -o $(@F)

ctrtrs_LTN_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DUPLO -DTRANS=2 -DDIAG $< -o $(@F)

ctrtrs_LRU_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DUPLO -DTRANS=3 -UDIAG $< -o $(@F)

ctrtrs_LRN_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DUPLO -DTRANS=3 -DDIAG $< -o $(@F)

ctrtrs_LCU_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DUPLO -DTRANS=4 -UDIAG $< -o $(@F)

ctrtrs_LCN_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DUPLO -DTRANS=4 -DDIAG $< -o $(@F)

ztrtrs_UNU_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UUPLO -DTRANS=1 -UDIAG $< -o $(@F)

ztrtrs_UNN_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UUPLO -DTRANS=1 -DDIAG $< -o $(@F)

ztrtrs_UTU_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UUPLO -DTRANS=2 -UDIAG $< -o $(@F)

ztrtrs_UTN_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UUPLO -DTRANS=2 -DDIAG $< -o $(@F)

ztrtrs_URU_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UUPLO -DTRANS=3 -UDIAG $< -o $(@F)

ztrtrs_URN_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UUPLO -DTRANS=3 -DDIAG $< -o $(@F)

ztrtrs_UCU_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UUPLO -DTRANS=4 -UDIAG $< -o $(@F)

ztrtrs_UCN_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UUPLO -DTRANS=4 -DDIAG $< -o $(@F)

ztrtrs_LNU_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DUPLO -DTRANS=1 -UDIAG $< -o $(@F)

ztrtrs_LNN_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DUPLO -DTRANS=1 -DDIAG $< -o $(@F)

ztrtrs_LTU_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DUPLO -DTRANS=2 -UDIAG $< -o $(@F)

ztrtrs_LTN_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DUPLO -DTRANS=2 -DDIAG $< -o $(@F)

ztrtrs_LRU_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DUPLO -DTRANS=3 -UDIAG $< -o $(@F)

ztrtrs_LRN_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DUPLO -DTRANS=3 -DDIAG $< -o $(@F)

ztrtrs_LCU_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DUPLO -DTRANS=4 -UDIAG $< -o $(@F)

ztrtrs_LCN_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DUPLO -DTRANS=4 -DDIAG $< -o $(@F)

xtrtrs_UNU_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UUPLO -DTRANS=1 -UDIAG $< -o $(@F)

xtrtrs_UNN_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UUPLO -DTRANS=1 -DDIAG $< -o $(@F)

xtrtrs_UTU_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UUPLO -DTRANS=2 -UDIAG $< -o $(@F)

xtrtrs_UTN_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UUPLO -DTRANS=2 -DDIAG $< -o $(@F)

xtrtrs_URU_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UUPLO -DTRANS=3 -UDIAG $< -o $(@F)

xtrtrs_URN_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UUPLO -DTRANS=3 -DDIAG $< -o $(@F)

xtrtrs_UCU_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UUPLO -DTRANS=4 -UDIAG $< -o $(@F)

xtrtrs_UCN_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UUPLO -DTRANS=4 -DDIAG $< -o $(@F)

xtrtrs_LNU_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DUPLO -DTRANS=1 -UDIAG $< -o $(@F)

xtrtrs_LNN_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DUPLO -DTRANS=1 -DDIAG $< -o $(@F)

xtrtrs_LTU_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DUPLO -DTRANS=2 -UDIAG $< -o $(@F)

xtrtrs_LTN_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DUPLO -DTRANS=2 -DDIAG $< -o $(@F)

xtrtrs_LRU_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DUPLO -DTRANS=3 -UDIAG $< -o $(@F)

xtrtrs_LRN_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DUPLO -DTRANS=3 -DDIAG $< -o $(@F)

xtrtrs_LCU_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DUPLO -DTRANS=4 -UDIAG $< -o $(@F)

xtrtrs_LCN_parallel.$(SUFFIX) : ztrtrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DUPLO -DTRANS=4 -DDIAG $< -o $(@F)

include ../../Makefile.tail
