TOPDIR	= ../..
include ../../Makefile.system

ifneq "$(or $(BUILD_SINGLE),$(BUILD_DOUBLE))" ""
SBLASOBJS = sgetf2_k.$(SUFFIX)
endif
ifeq ($(BUILD_DOUBLE),1)
DBLASOBJS = dgetf2_k.$(SUFFIX)
endif
QBLASOBJS = qgetf2_k.$(SUFFIX)
ifneq "$(or $(BUILD_COMPLEX),$(BUILD_COMPLEX16))" ""
CBLASOBJS = cgetf2_k.$(SUFFIX)
endif
ifeq ($(BUILD_COMPLEX16),1)
ZBLASOBJS = zgetf2_k.$(SUFFIX)
endif
XBLASOBJS = xgetf2_k.$(SUFFIX)

sgetf2_k.$(SUFFIX) : getf2_k.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE $< -o $(@F)

dgetf2_k.$(SUFFIX) : getf2_k.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE $< -o $(@F)

qgetf2_k.$(SUFFIX) : getf2_k.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE $< -o $(@F)

cgetf2_k.$(SUFFIX) : zgetf2_k.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE $< -o $(@F)

zgetf2_k.$(SUFFIX) : zgetf2_k.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE $< -o $(@F)

xgetf2_k.$(SUFFIX) : zgetf2_k.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE $< -o $(@F)

sgetf2_k.$(PSUFFIX) : getf2_k.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE $< -o $(@F)

dgetf2_k.$(PSUFFIX) : getf2_k.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE $< -o $(@F)

qgetf2_k.$(PSUFFIX) : getf2_k.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE $< -o $(@F)

cgetf2_k.$(PSUFFIX) : zgetf2_k.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE $< -o $(@F)

zgetf2_k.$(PSUFFIX) : zgetf2_k.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE $< -o $(@F)

xgetf2_k.$(PSUFFIX) : zgetf2_k.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE $< -o $(@F)

include ../../Makefile.tail


