/*****************************************************************************
Copyright (c) 2011-2015, The OpenBLAS Project
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

   1. Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.

   2. Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in
      the documentation and/or other materials provided with the
      distribution.
   3. Neither the name of the OpenBLAS project nor the names of 
      its contributors may be used to endorse or promote products 
      derived from this software without specific prior written 
      permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
**********************************************************************************/

#ifndef COMMON_ARM64
#define COMMON_ARM64

#ifdef C_MSVC
#include <intrin.h>
#define MB __dmb(_ARM64_BARRIER_ISH)
#define WMB __dmb(_ARM64_BARRIER_ISHST)
#define RMB __dmb(_ARM64_BARRIER_ISHLD)
#else
#define MB   __asm__ __volatile__ ("dmb  ish" : : : "memory")
#define WMB  __asm__ __volatile__ ("dmb  ishst" : : : "memory")
#define RMB  __asm__ __volatile__ ("dmb  ishld" : : : "memory")
#endif

#if defined( F_INTERFACE_FLANG) || (defined(F_INTERFACE_PGI) && (defined(__NVCOMPILER) && (__NVCOMPILER_MAJOR__ < 23 || (__NVCOMPILER_MAJOR__ == 23 && __NVCOMPILER_MINOR__ < 9))))
#define RETURN_BY_STACK
#else
#define RETURN_BY_COMPLEX
#endif

#ifndef ASSEMBLER


#ifndef NO_AFFINITY
static __inline int WhereAmI(void){
  uint64_t ret;
  __asm__ volatile (
       "         mrs x0, mpidr_el1 \n"
       "         and x0, x0, 0xff  \n"
                 :"=r" (ret)
                 :: "memory"
               );
  ret +=1;
  if ((int)ret <0) ret = 0;
  return (int)ret;
}
#endif

static __inline void blas_lock(volatile BLASULONG *address){

  BLASULONG ret;

  do {
    #ifndef C_MSVC
    __asm__ __volatile__(
			 "mov	x4, #1							\n\t"
			 "sevl								\n\t"
                         "1:                                                            \n\t"
                         "wfe                                                           \n\t"
			 "2:								\n\t"
                         "ldaxr x2, [%1]                                                \n\t"
                         "cbnz  x2, 1b                                                  \n\t"
                         "stxr  w3, x4, [%1]                                            \n\t"
                         "cbnz  w3, 2b                                                  \n\t"
                         "mov   %0, #0                                                  \n\t"
                         : "=r"(ret), "=r"(address)
                         : "1"(address)
                         : "memory", "x2" , "x3", "x4"


    );
    #else
      while (*address) {YIELDING;}
      ret=InterlockedExchange64((volatile LONG64 *)(address), 1);
    #endif

  } while (ret);

}

#define BLAS_LOCK_DEFINED

#if !defined(OS_DARWIN) && !defined (OS_ANDROID)
static __inline BLASULONG rpcc(void){
  #ifdef C_MSVC
    const int64_t pmccntr_el0 = (((3 & 1) << 14) |  // op0
        ((3 & 7) << 11) |  // op1
        ((9 & 15) << 7) |  // crn
        ((13 & 15) << 3) | // crm
        ((0 & 7) << 0));   // op2
    return _ReadStatusReg(pmccntr_el0);
  #else
  BLASULONG ret = 0;
  blasint shift;
 
  __asm__ __volatile__ ("isb; mrs %0,cntvct_el0":"=r"(ret));
  __asm__ __volatile__ ("mrs %0,cntfrq_el0; clz %w0, %w0":"=&r"(shift));

  return ret << shift;
  #endif
}

#define RPCC_DEFINED
#define RPCC64BIT
#endif 

static inline int blas_quickdivide(blasint x, blasint y){
  return x / y;
}

#if defined(DOUBLE)
#define GET_IMAGE(res)  __asm__ __volatile__("str d1, %0" : "=m"(res) : : "memory")
#else
#define GET_IMAGE(res)  __asm__ __volatile__("str s1, %0" : "=m"(res) : : "memory")
#endif

#define GET_IMAGE_CANCEL

#endif


#ifndef F_INTERFACE
#define REALNAME ASMNAME
#else
#define REALNAME ASMFNAME
#endif

#if defined(ASSEMBLER) && !defined(NEEDPARAM)

.macro PROLOGUE 
	.text ;
	.p2align 2 ;
	.global	REALNAME ;
#if !defined(__APPLE__) && !defined(_WIN32)
	.type	REALNAME, %function ;
#endif
REALNAME:
.endm


#define EPILOGUE

#define PROFCODE

#endif


#define SEEK_ADDRESS

#ifndef PAGESIZE
#define PAGESIZE        ( 4 << 10)
#endif
#define HUGE_PAGESIZE   ( 4 << 20)

#ifndef BUFFERSIZE
#if defined(NEOVERSEN1) || defined(NEOVERSEN2) || defined(NEOVERSEV1) || defined(A64FX) || defined(ARMV8SVE) || defined(ARMV9SME)
#define BUFFER_SIZE     (32 << 22)
#else
#define BUFFER_SIZE     (32 << 20)
#endif
#else
#define BUFFER_SIZE	(32 << BUFFERSIZE)
#endif

#define BASE_ADDRESS (START_ADDRESS - BUFFER_SIZE * MAX_CPU_NUMBER)

#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif

#endif

