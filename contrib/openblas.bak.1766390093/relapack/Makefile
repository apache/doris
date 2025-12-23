TOPDIR  = ..
include $(TOPDIR)/Makefile.system

ifeq ($(RELAPACK_REPLACE),0)
RELAPREFIX=RELAPACK_
INCLALL=-DINCLUDE_ALL=0
else
INCLALL=-DINCLUDE_ALL=1
endif

SRC = $(wildcard src/*.c)

SRC1 = \
	slauum.c clauum.c dlauum.c zlauum.c \
	strtri.c dtrtri.c ctrtri.c ztrtri.c \
	spotrf.c dpotrf.c cpotrf.c zpotrf.c \
	sgetrf.c dgetrf.c cgetrf.c zgetrf.c

SRC2 = \
	cgbtrf.c cpbtrf.c dsytrf_rec2.c sgbtrf.c ssytrf_rook.c zhegst.c zsytrf_rec2.c \
	cgemmt.c dgbtrf.c dsytrf_rook.c sgemmt.c ssytrf_rook_rec2.c zhetrf.c zsytrf_rook.c \
	csytrf.c dgemmt.c dsytrf_rook_rec2.c stgsyl.c zhetrf_rec2.c zsytrf_rook_rec2.c \
	chegst.c csytrf_rec2.c dtgsyl.c strsyl.c zhetrf_rook.c ztgsyl.c \
	chetrf.c csytrf_rook.c dtrsyl.c spbtrf.c strsyl_rec2.c zhetrf_rook_rec2.c ztrsyl.c \
	chetrf_rec2.c csytrf_rook_rec2.c dpbtrf.c dtrsyl_rec2.c ztrsyl_rec2.c \
	chetrf_rook.c ctgsyl.c ssygst.c zgbtrf.c zpbtrf.c  \
	chetrf_rook_rec2.c ctrsyl.c dsygst.c f2c.c ssytrf.c zgemmt.c \
	ctrsyl_rec2.c dsytrf.c lapack_wrappers.c ssytrf_rec2.c zsytrf.c

SRCX = \
	cgbtrf.c cpbtrf.c ctrtri.c dsytrf_rec2.c sgbtrf.c ssytrf_rook.c zhegst.c zsytrf_rec2.c \
	cgemmt.c cpotrf.c dgbtrf.c dsytrf_rook.c sgemmt.c ssytrf_rook_rec2.c zhetrf.c zsytrf_rook.c \
	cgetrf.c csytrf.c dgemmt.c dsytrf_rook_rec2.c sgetrf.c stgsyl.c zhetrf_rec2.c zsytrf_rook_rec2.c \
	chegst.c csytrf_rec2.c dgetrf.c dtgsyl.c slauum.c strsyl.c zhetrf_rook.c ztgsyl.c \
	chetrf.c csytrf_rook.c dlauum.c dtrsyl.c spbtrf.c strsyl_rec2.c zhetrf_rook_rec2.c ztrsyl.c \
	chetrf_rec2.c csytrf_rook_rec2.c dpbtrf.c dtrsyl_rec2.c spotrf.c strtri.c zlauum.c ztrsyl_rec2.c \
	chetrf_rook.c ctgsyl.c dpotrf.c dtrtri.c ssygst.c zgbtrf.c zpbtrf.c ztrtri.c \
	chetrf_rook_rec2.c ctrsyl.c dsygst.c f2c.c ssytrf.c zgemmt.c zpotrf.c \
	clauum.c ctrsyl_rec2.c dsytrf.c lapack_wrappers.c ssytrf_rec2.c zgetrf.c zsytrf.c


OBJS1 = $(SRC1:%.c=src/$(RELAPREFIX)%.$(SUFFIX))
OBJS2 = $(SRC2:%.c=src/$(RELAPREFIX)%.o)
OBJS = $(OBJS1) $(OBJS2)

TEST_SUITS = \
	sgemmt dgemmt cgemmt zgemmt

	#	slauum dlauum clauum zlauum \
	#	spotrf dpotrf cpotrf zpotrf \
	#	spbtrf dpbtrf cpbtrf zpbtrf \
	#	ssygst dsygst chegst zhegst \
	#	ssytrf dsytrf csytrf chetrf zsytrf zhetrf \
	#	sgetrf dgetrf cgetrf zgetrf \
	#	sgbtrf dgbtrf cgbtrf zgbtrf \
	#	strsyl dtrsyl ctrsyl ztrsyl \
	#	stgsyl dtgsyl ctgsyl ztgsyl \

TESTS = $(TEST_SUITS:%=test/%.pass)  # dummies
TEST_EXES = $(TEST_SUITS:%=test/%.x)

LINK_TEST = -L$(TOPDIR) -lopenblas -lgfortran -lm

.SECONDARY: $(TEST_EXES)
.PHONY: test

# ReLAPACK compilation

libs:	$(OBJS)
	@echo "Building ReLAPACK library $(LIBNAME)"
	$(AR) -r  $(TOPDIR)/$(LIBNAME) $(OBJS)
	$(RANLIB) $(TOPDIR)/$(LIBNAME)

src/$(RELAPREFIX)%.$(SUFFIX): src/%.c relapack_config.h
	$(CC) -v $(CFLAGS) -I. $(INCLALL) -c $< -o $@

src/$(RELAPREFIX)%.o: src/%.c relapack_config.h
	$(CC) -v $(CFLAGS) -I. $(INCLALL) -c $< -o $@


# ReLAPACK testing

test: $(TEST_EXES) $(TESTS)
	@echo "passed all tests"

test/%.pass: test/%.x
	@echo -n $*:
	@./$< > /dev/null && echo " pass" || (echo " FAIL" && ./$<)

test/s%.x: test/x%.c test/util.$(SUFFIX) $(TOPDIR)/$(LIBNAME) test/config.h test/test.h
	$(CC) $(CFLAGS) -DDT_PREFIX=s $< test/util.$(SUFFIX) -o $@ $(LINK_TEST) $(TOPDIR)/$(LIBNAME) $(LINK_TEST)

test/d%.x: test/x%.c test/util.$(SUFFIX) $(TOPDIR)/$(LIBNAME) test/config.h test/test.h
	$(CC) $(CFLAGS) -DDT_PREFIX=d $< test/util.$(SUFFIX) -o $@ $(LINK_TEST) $(TOPDIR)/$(LIBNAME) $(LINK_TEST)

test/c%.x: test/x%.c test/util.$(SUFFIX) $(TOPDIR)/$(LIBNAME) test/config.h test/test.h
	$(CC) $(CFLAGS) -DDT_PREFIX=c $< test/util.$(SUFFIX) -o $@ $(LINK_TEST) $(TOPDIR)/$(LIBNAME) $(LINK_TEST)

test/z%.x: test/x%.c test/util.$(SUFFIX) $(TOPDIR)/$(LIBNAME) test/config.h test/test.h
	$(CC) $(CFLAGS) -DDT_PREFIX=z $< test/util.$(SUFFIX) -o $@ $(LINK_TEST) $(TOPDIR)/$(LIBNAME) $(LINK_TEST)


# cleaning up

clean:
	rm -f  $(OBJS) test/util.$(SUFFIX) test/*.x
