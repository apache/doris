/*********************************************************************/
/* Copyright 2009, 2010 The University of Texas at Austin.           */
/* All rights reserved.                                              */
/*                                                                   */
/* Redistribution and use in source and binary forms, with or        */
/* without modification, are permitted provided that the following   */
/* conditions are met:                                               */
/*                                                                   */
/*   1. Redistributions of source code must retain the above         */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer.                                                  */
/*                                                                   */
/*   2. Redistributions in binary form must reproduce the above      */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer in the documentation and/or other materials       */
/*      provided with the distribution.                              */
/*                                                                   */
/*    THIS  SOFTWARE IS PROVIDED  BY THE  UNIVERSITY OF  TEXAS AT    */
/*    AUSTIN  ``AS IS''  AND ANY  EXPRESS OR  IMPLIED WARRANTIES,    */
/*    INCLUDING, BUT  NOT LIMITED  TO, THE IMPLIED  WARRANTIES OF    */
/*    MERCHANTABILITY  AND FITNESS FOR  A PARTICULAR  PURPOSE ARE    */
/*    DISCLAIMED.  IN  NO EVENT SHALL THE UNIVERSITY  OF TEXAS AT    */
/*    AUSTIN OR CONTRIBUTORS BE  LIABLE FOR ANY DIRECT, INDIRECT,    */
/*    INCIDENTAL,  SPECIAL, EXEMPLARY,  OR  CONSEQUENTIAL DAMAGES    */
/*    (INCLUDING, BUT  NOT LIMITED TO,  PROCUREMENT OF SUBSTITUTE    */
/*    GOODS  OR  SERVICES; LOSS  OF  USE,  DATA,  OR PROFITS;  OR    */
/*    BUSINESS INTERRUPTION) HOWEVER CAUSED  AND ON ANY THEORY OF    */
/*    LIABILITY, WHETHER  IN CONTRACT, STRICT  LIABILITY, OR TORT    */
/*    (INCLUDING NEGLIGENCE OR OTHERWISE)  ARISING IN ANY WAY OUT    */
/*    OF  THE  USE OF  THIS  SOFTWARE,  EVEN  IF ADVISED  OF  THE    */
/*    POSSIBILITY OF SUCH DAMAGE.                                    */
/*                                                                   */
/* The views and conclusions contained in the software and           */
/* documentation are those of the authors and should not be          */
/* interpreted as representing official policies, either expressed   */
/* or implied, of The University of Texas at Austin.                 */
/*********************************************************************/

#include "common.h"

#ifdef _MSC_VER
#define strncasecmp _strnicmp
#define strcasecmp _stricmp
#endif

#ifdef ARCH_X86
#define EXTERN extern
#else
#define EXTERN
#endif

#ifdef DYNAMIC_LIST
extern gotoblas_t gotoblas_PRESCOTT;

#ifdef DYN_ATHLON
extern gotoblas_t gotoblas_ATHLON;
#else
#define gotoblas_ATHLON gotoblas_PRESCOTT
#endif
#ifdef DYN_KATMAI
extern gotoblas_t gotoblas_KATMAI;
#else
#define gotoblas_KATMAI gotoblas_PRESCOTT
#endif
#ifdef DYN_BANIAS
extern gotoblas_t gotoblas_BANIAS;
#else
#define gotoblas_BANIAS gotoblas_PRESCOTT
#endif
#ifdef DYN_COPPERMINE
extern gotoblas_t gotoblas_COPPERMINE;
#else
#define gotoblas_COPPERMINE gotoblas_PRESCOTT
#endif
#ifdef DYN_NORTHWOOD
extern gotoblas_t gotoblas_NORTHWOOD;
#else
#define gotoblas_NORTHWOOD gotoblas_PRESCOTT
#endif
#ifdef DYN_CORE2
extern gotoblas_t gotoblas_CORE2;
#else
#define gotoblas_CORE2 gotoblas_PRESCOTT
#endif
#ifdef DYN_NEHALEM
extern gotoblas_t gotoblas_NEHALEM;
#else
#define gotoblas_NEHALEM gotoblas_PRESCOTT
#endif
#ifdef DYN_BARCELONA
extern gotoblas_t gotoblas_BARCELONA;
#elif defined(DYN_NEHALEM)
#define gotoblas_BARCELONA gotoblas_NEHALEM
#else
#define gotoblas_BARCELONA gotoblas_PRESCOTT
#endif
#ifdef DYN_ATOM
extern gotoblas_t gotoblas_ATOM;
#elif defined(DYN_NEHALEM)
#define gotoblas_ATOM gotoblas_NEHALEM
#else
#define gotoblas_ATOM gotoblas_PRESCOTT
#endif
#ifdef DYN_NANO
extern gotoblas_t gotoblas_NANO;
#else
#define gotoblas_NANO gotoblas_PRESCOTT
#endif
#ifdef DYN_PENRYN
extern gotoblas_t gotoblas_PENRYN;
#else
#define gotoblas_PENRYN gotoblas_PRESCOTT
#endif
#ifdef DYN_DUNNINGTON
extern gotoblas_t gotoblas_DUNNINGTON;
#else
#define gotoblas_DUNNINGTON gotoblas_PRESCOTT
#endif
#ifdef DYN_OPTERON
extern gotoblas_t gotoblas_OPTERON;
#else
#define gotoblas_OPTERON gotoblas_PRESCOTT
#endif
#ifdef DYN_OPTERON_SSE3
extern gotoblas_t gotoblas_OPTERON_SSE3;
#else
#define gotoblas_OPTERON_SSE3 gotoblas_PRESCOTT
#endif
#ifdef DYN_BOBCAT
extern gotoblas_t gotoblas_BOBCAT;
#elif defined(DYN_NEHALEM)
#define gotoblas_BOBCAT gotoblas_NEHALEM
#else
#define gotoblas_BOBCAT gotoblas_PRESCOTT
#endif
#ifdef DYN_SANDYBRIDGE
extern gotoblas_t gotoblas_SANDYBRIDGE;
#elif defined(DYN_NEHALEM)
#define gotoblas_SANDYBRIDGE gotoblas_NEHALEM
#else
#define gotoblas_SANDYBRIDGE gotoblas_PRESCOTT
#endif
#ifdef DYN_BULLDOZER
extern gotoblas_t gotoblas_BULLDOZER;
#elif defined(DYN_SANDYBRIDGE)
#define gotoblas_BULLDOZER gotoblas_SANDYBRIDGE
#elif defined(DYN_NEHALEM)
#define gotoblas_BULLDOZER gotoblas_NEHALEM
#else
#define gotoblas_BULLDOZER gotoblas_PRESCOTT
#endif
#ifdef DYN_PILEDRIVER
extern gotoblas_t gotoblas_PILEDRIVER;
#elif defined(DYN_SANDYBRIDGE)
#define gotoblas_PILEDRIVER gotoblas_SANDYBRIDGE
#elif defined(DYN_NEHALEM)
#define gotoblas_PILEDRIVER gotoblas_NEHALEM
#else
#define gotoblas_PILEDRIVER gotoblas_PRESCOTT
#endif
#ifdef DYN_STEAMROLLER
extern gotoblas_t gotoblas_STEAMROLLER;
#elif defined(DYN_SANDYBRIDGE)
#define gotoblas_STEAMROLLER gotoblas_SANDYBRIDGE
#elif defined(DYN_NEHALEM)
#define gotoblas_STEAMROLLER gotoblas_NEHALEM
#else
#define gotoblas_STEAMROLLER gotoblas_PRESCOTT
#endif
#ifdef DYN_EXCAVATOR
extern gotoblas_t gotoblas_EXCAVATOR;
#elif defined(DYN_SANDYBRIDGE)
#define gotoblas_EXCAVATOR gotoblas_SANDYBRIDGE
#elif defined(DYN_NEHALEM)
#define gotoblas_EXCAVATOR gotoblas_NEHALEM
#else
#define gotoblas_EXCAVATOR gotoblas_PRESCOTT
#endif
#ifdef DYN_HASWELL
extern gotoblas_t gotoblas_HASWELL;
#elif defined(DYN_SANDYBRIDGE)
#define gotoblas_HASWELL gotoblas_SANDYBRIDGE
#elif defined(DYN_NEHALEM)
#define gotoblas_HASWELL gotoblas_NEHALEM
#else
#define gotoblas_HASWELL gotoblas_PRESCOTT
#endif
#ifdef DYN_ZEN
extern gotoblas_t gotoblas_ZEN;
#elif defined(DYN_HASWELL)
#define gotoblas_ZEN gotoblas_HASWELL
#elif defined(DYN_SANDYBRIDGE)
#define gotoblas_ZEN gotoblas_SANDYBRIDGE
#elif defined(DYN_NEHALEM)
#define gotoblas_ZEN gotoblas_NEHALEM
#else
#define gotoblas_ZEN gotoblas_PRESCOTT
#endif
#ifdef DYN_SKYLAKEX
extern gotoblas_t gotoblas_SKYLAKEX;
#elif defined(DYN_HASWELL)
#define gotoblas_SKYLAKEX gotoblas_HASWELL
#elif defined(DYN_SANDYBRIDGE)
#define gotoblas_SKYLAKEX gotoblas_SANDYBRIDGE
#elif defined(DYN_NEHALEM)
#define gotoblas_SKYLAKEX gotoblas_NEHALEM
#else
#define gotoblas_SKYLAKEX gotoblas_PRESCOTT
#endif
#ifdef DYN_COOPERLAKE
extern gotoblas_t gotoblas_COOPERLAKE;
#elif defined(DYN_SKYLAKEX)
#define gotoblas_COOPERLAKE gotoblas_SKYLAKEX
#elif defined(DYN_HASWELL)
#define gotoblas_COOPERLAKE gotoblas_HASWELL
#elif defined(DYN_SANDYBRIDGE)
#define gotoblas_COOPERLAKE gotoblas_SANDYBRIDGE
#elif defined(DYN_NEHALEM)
#define gotoblas_COOPERLAKE gotoblas_NEHALEM
#else
#define gotoblas_COOPERLAKE gotoblas_PRESCOTT
#endif
#ifdef DYN_SAPPHIRERAPIDS
extern gotoblas_t gotoblas_SAPPHIRERAPIDS;
#elif defined(DYN_SKYLAKEX)
#define gotoblas_SAPPHIRERAPIDS gotoblas_SKYLAKEX
#elif defined(DYN_HASWELL)
#define gotoblas_SAPPHIRERAPIDS gotoblas_HASWELL
#elif defined(DYN_SANDYBRIDGE)
#define gotoblas_SAPPHIRERAPIDS gotoblas_SANDYBRIDGE
#elif defined(DYN_NEHALEM)
#define gotoblas_SAPPHIRERAPIDS gotoblas_NEHALEM
#else
#define gotoblas_SAPPHIRERAPIDS gotoblas_PRESCOTT
#endif


#else // not DYNAMIC_LIST
EXTERN gotoblas_t  gotoblas_KATMAI;
EXTERN gotoblas_t  gotoblas_COPPERMINE;
EXTERN gotoblas_t  gotoblas_NORTHWOOD;
EXTERN gotoblas_t  gotoblas_BANIAS;
EXTERN gotoblas_t  gotoblas_ATHLON;

extern gotoblas_t  gotoblas_PRESCOTT;
extern gotoblas_t  gotoblas_CORE2;
extern gotoblas_t  gotoblas_NEHALEM;
extern gotoblas_t  gotoblas_BARCELONA;
#ifdef DYNAMIC_OLDER
extern gotoblas_t  gotoblas_ATOM;
extern gotoblas_t  gotoblas_NANO;
extern gotoblas_t  gotoblas_PENRYN;
extern gotoblas_t  gotoblas_DUNNINGTON;
extern gotoblas_t  gotoblas_OPTERON;
extern gotoblas_t  gotoblas_OPTERON_SSE3;
extern gotoblas_t  gotoblas_BOBCAT;
#else
#define gotoblas_ATOM gotoblas_NEHALEM
#define gotoblas_NANO gotoblas_NEHALEM
#define gotoblas_PENRYN gotoblas_CORE2
#define gotoblas_DUNNINGTON gotoblas_CORE2
#define gotoblas_OPTERON gotoblas_CORE2
#define gotoblas_OPTERON_SSE3 gotoblas_CORE2
#define gotoblas_BOBCAT gotoblas_CORE2
#endif

#ifndef NO_AVX
extern gotoblas_t  gotoblas_SANDYBRIDGE;
extern gotoblas_t  gotoblas_BULLDOZER;
extern gotoblas_t  gotoblas_PILEDRIVER;
extern gotoblas_t  gotoblas_STEAMROLLER;
extern gotoblas_t  gotoblas_EXCAVATOR;
#ifdef NO_AVX2
#define gotoblas_HASWELL gotoblas_SANDYBRIDGE
#define gotoblas_SKYLAKEX gotoblas_SANDYBRIDGE
#define gotoblas_COOPERLAKE gotoblas_SANDYBRIDGE
#define gotoblas_ZEN gotoblas_SANDYBRIDGE
#define gotoblas_SAPPHIRERAPIDS gotoblas_SANDYBRIDGE
#else
extern gotoblas_t  gotoblas_HASWELL;
extern gotoblas_t  gotoblas_ZEN;
#ifndef NO_AVX512
extern gotoblas_t  gotoblas_SKYLAKEX;
extern gotoblas_t  gotoblas_COOPERLAKE;
extern gotoblas_t  gotoblas_SAPPHIRERAPIDS;
#else
#define gotoblas_SKYLAKEX gotoblas_HASWELL
#define gotoblas_COOPERLAKE gotoblas_HASWELL
#define gotoblas_SAPPHIRERAPIDS gotoblas_HASWELL
#endif
#endif
#else
//Use NEHALEM kernels for sandy bridge
#define gotoblas_SANDYBRIDGE gotoblas_NEHALEM
#define gotoblas_HASWELL gotoblas_NEHALEM
#define gotoblas_SKYLAKEX gotoblas_NEHALEM
#define gotoblas_COOPERLAKE gotoblas_NEHALEM
#define gotoblas_SAPPHIRERAPIDS gotoblas_NEHALEM
#define gotoblas_BULLDOZER gotoblas_BARCELONA
#define gotoblas_PILEDRIVER gotoblas_BARCELONA
#define gotoblas_STEAMROLLER gotoblas_BARCELONA
#define gotoblas_EXCAVATOR gotoblas_BARCELONA
#define gotoblas_ZEN gotoblas_BARCELONA
#endif

#endif // DYNAMIC_LIST

#define VENDOR_INTEL      1
#define VENDOR_AMD        2
#define VENDOR_CENTAUR    3
#define VENDOR_HYGON	  4
#define VENDOR_ZHAOXIN    5
#define VENDOR_UNKNOWN   99

#define BITMASK(a, b, c) ((((a) >> (b)) & (c)))

#ifndef NO_AVX
static inline void xgetbv(int op, int * eax, int * edx){
  //Use binary code for xgetbv
  __asm__ __volatile__
    (".byte 0x0f, 0x01, 0xd0": "=a" (*eax), "=d" (*edx) : "c" (op) : "cc");
}
#endif

int support_avx(){
#ifndef NO_AVX
  int eax, ebx, ecx, edx;
  int ret=0;

  cpuid(1, &eax, &ebx, &ecx, &edx);
  if ((ecx & (1 << 28)) != 0 && (ecx & (1 << 27)) != 0 && (ecx & (1 << 26)) != 0){
    xgetbv(0, &eax, &edx);
    if((eax & 6) == 6){
      ret=1;  //OS support AVX
    }
  }
  return ret;
#else
  return 0;
#endif
}

int support_avx2(){
#ifndef NO_AVX2
  int eax, ebx, ecx=0, edx;
  int ret=0;

  if (!support_avx())
    return 0;
  cpuid(7, &eax, &ebx, &ecx, &edx);
  if((ebx & (1<<5)) != 0)
      ret=1;  //AVX2 flag is set
  return ret;
#else
  return 0;
#endif
}

int support_avx512(){
#if !defined(NO_AVX) && !defined(NO_AVX512)
  int eax, ebx, ecx, edx;
  int ret=0;

  if (!support_avx())
    return 0;
  cpuid(7, &eax, &ebx, &ecx, &edx);
  if((ebx & (1<<5)) == 0){
      ret=0;  //cpu does not have avx2 flag
  }
  if((ebx & (1<<31)) != 0){ //AVX512VL flag is set
    xgetbv(0, &eax, &edx);
    if((eax & 0xe0) == 0xe0)
      ret=1;  //OS supports saving zmm register
  }
  return ret;
#else
  return 0;
#endif
}

int support_avx512_bf16(){
#if !defined(NO_AVX) && !defined(NO_AVX512)
  int eax, ebx, ecx, edx;
  int ret=0;

  if (!support_avx512())
    return 0;
  cpuid_count(7, 1, &eax, &ebx, &ecx, &edx);
  if((eax & 32) == 32){
      ret=1;  // CPUID.7.1:EAX[bit 5] indicates whether avx512_bf16 supported or not
  }
  return ret;
#else
  return 0;
#endif
}

#define BIT_AMX_TILE	0x01000000
#define BIT_AMX_BF16	0x00400000
#define BIT_AMX_ENBD	0x00060000

int support_amx_bf16() {
#if !defined(NO_AVX) && !defined(NO_AVX512)
  int eax, ebx, ecx, edx;
  int ret=0;

  if (!support_avx512())
    return 0;
  // CPUID.7.0:EDX indicates AMX support
  cpuid_count(7, 0, &eax, &ebx, &ecx, &edx);
  if ((edx & BIT_AMX_TILE) && (edx & BIT_AMX_BF16)) {
    // CPUID.D.0:EAX[17:18] indicates AMX enabled
    cpuid_count(0xd, 0, &eax, &ebx, &ecx, &edx);
    if ((eax & BIT_AMX_ENBD) == BIT_AMX_ENBD)
      ret = 1;
  }
  return ret;
#else
  return 0;
#endif
}

extern void openblas_warning(int verbose, const char * msg);
#define FALLBACK_VERBOSE 1
#define NEHALEM_FALLBACK "OpenBLAS : Your OS does not support AVX instructions. OpenBLAS is using Nehalem kernels as a fallback, which may give poorer performance.\n"
#define SANDYBRIDGE_FALLBACK "OpenBLAS : Your OS does not support AVX2 instructions. OpenBLAS is using Sandybridge kernels as a fallback, which may give poorer performance.\n"
#define HASWELL_FALLBACK "OpenBLAS : Your OS does not support AVX512VL instructions. OpenBLAS is using Haswell kernels as a fallback, which may give poorer performance.\n"
#define BARCELONA_FALLBACK "OpenBLAS : Your OS does not support AVX instructions. OpenBLAS is using Barcelona kernels as a fallback, which may give poorer performance.\n"

static int get_vendor(void){
  int eax, ebx, ecx, edx;

  union
  {
        char vchar[16];
        int  vint[4];
  } vendor;

  cpuid(0, &eax, &ebx, &ecx, &edx);

  *(&vendor.vint[0]) = ebx;
  *(&vendor.vint[1]) = edx;
  *(&vendor.vint[2]) = ecx;

  vendor.vchar[12] = '\0';

  if (!strcmp(vendor.vchar, "GenuineIntel")) return VENDOR_INTEL;
  if (!strcmp(vendor.vchar, "AuthenticAMD")) return VENDOR_AMD;
  if (!strcmp(vendor.vchar, "CentaurHauls")) return VENDOR_CENTAUR;
  if (!strcmp(vendor.vchar, "  Shanghai  ")) return VENDOR_ZHAOXIN;
  if (!strcmp(vendor.vchar, "HygonGenuine")) return VENDOR_HYGON;

  if ((eax == 0) || ((eax & 0x500) != 0)) return VENDOR_INTEL;

  return VENDOR_UNKNOWN;
}

static gotoblas_t *get_coretype(void){

  int eax, ebx, ecx, edx;
  int family, exfamily, model, vendor, exmodel, stepping;

  cpuid(1, &eax, &ebx, &ecx, &edx);

  family   = BITMASK(eax,  8, 0x0f);
  exfamily = BITMASK(eax, 20, 0xff);
  model    = BITMASK(eax,  4, 0x0f);
  exmodel  = BITMASK(eax, 16, 0x0f);
  stepping = BITMASK(eax,  0, 0x0f);

  vendor = get_vendor();

  if (vendor == VENDOR_INTEL){
    switch (family) {
    case 0x6:
      switch (exmodel) {
      case 0:
	if (model <= 0x7) return &gotoblas_KATMAI;
	if ((model == 0x8) || (model == 0xa) || (model == 0xb)) return &gotoblas_COPPERMINE;
	if ((model == 0x9) || (model == 0xd)) return &gotoblas_BANIAS;
	if (model == 14) return &gotoblas_BANIAS;
	if (model == 15) return &gotoblas_CORE2;
	return NULL;

      case 1:
	if (model == 6) return &gotoblas_CORE2;
	if (model == 7) return &gotoblas_PENRYN;
	if (model == 13) return &gotoblas_DUNNINGTON;
	if ((model == 10) || (model == 11) || (model == 14) || (model == 15)) return &gotoblas_NEHALEM;
	if (model == 12) return &gotoblas_ATOM;
	return NULL;

      case 2:
	//Intel Core (Clarkdale) / Core (Arrandale)
	// Pentium (Clarkdale) / Pentium Mobile (Arrandale)
	// Xeon (Clarkdale), 32nm
	if (model ==  5) return &gotoblas_NEHALEM;

	//Intel Xeon Processor 5600 (Westmere-EP)
	//Xeon Processor E7 (Westmere-EX)
	//Xeon E7540
	if (model == 12 || model == 14 || model == 15) return &gotoblas_NEHALEM;

	//Intel Core i5-2000 /i7-2000 (Sandy Bridge)
	//Intel Core i7-3000 / Xeon E5
	if (model == 10 || model == 13) {
	  if(support_avx())
	    return &gotoblas_SANDYBRIDGE;
	  else{
	    openblas_warning(FALLBACK_VERBOSE, NEHALEM_FALLBACK);
	    return &gotoblas_NEHALEM; //OS doesn't support AVX. Use old kernels.
	  }
	}
	return NULL;
      case 3:
	//Intel Sandy Bridge 22nm (Ivy Bridge?)
	if (model == 10 || model == 14) {
	  if(support_avx())
	    return &gotoblas_SANDYBRIDGE;
	  else{
	    openblas_warning(FALLBACK_VERBOSE, NEHALEM_FALLBACK);
	    return &gotoblas_NEHALEM; //OS doesn't support AVX. Use old kernels.
	  }
	}
	//Intel Haswell
	if (model == 12 || model == 15) {
	  if(support_avx2())
	    return &gotoblas_HASWELL;
	  if(support_avx()) {
	    openblas_warning(FALLBACK_VERBOSE, SANDYBRIDGE_FALLBACK);
	    return &gotoblas_SANDYBRIDGE;
	  } else {
	    openblas_warning(FALLBACK_VERBOSE, NEHALEM_FALLBACK);
	    return &gotoblas_NEHALEM; //OS doesn't support AVX. Use old kernels.
	  }
	}
	//Intel Broadwell
	if (model == 13) {
	  if(support_avx2())
	    return &gotoblas_HASWELL;
	  if(support_avx()) {
	    openblas_warning(FALLBACK_VERBOSE, SANDYBRIDGE_FALLBACK);
	    return &gotoblas_SANDYBRIDGE;
	  } else {
	    openblas_warning(FALLBACK_VERBOSE, NEHALEM_FALLBACK);
	    return &gotoblas_NEHALEM; //OS doesn't support AVX. Use old kernels.
	  }
	}
	if (model == 7) return &gotoblas_ATOM; //Bay Trail	
	return NULL;
      case 4:
		//Intel Haswell
	if (model == 5 || model == 6) {
	  if(support_avx2())
	    return &gotoblas_HASWELL;
	  if(support_avx()) {
	    openblas_warning(FALLBACK_VERBOSE, SANDYBRIDGE_FALLBACK);
	    return &gotoblas_SANDYBRIDGE;
	  } else {
	    openblas_warning(FALLBACK_VERBOSE, NEHALEM_FALLBACK);
	    return &gotoblas_NEHALEM; //OS doesn't support AVX. Use old kernels.
	  }
	}
	//Intel Broadwell
	if (model == 7 || model == 15) {
	  if(support_avx2())
	    return &gotoblas_HASWELL;
	  if(support_avx()) {
	    openblas_warning(FALLBACK_VERBOSE, SANDYBRIDGE_FALLBACK);
	    return &gotoblas_SANDYBRIDGE;
	  } else {
	    openblas_warning(FALLBACK_VERBOSE, NEHALEM_FALLBACK);
	    return &gotoblas_NEHALEM; //OS doesn't support AVX. Use old kernels.
	  }
	}
	//Intel Skylake
	if (model == 14) {
	  if(support_avx2())
	    return &gotoblas_HASWELL;
	  if(support_avx()) {
	    openblas_warning(FALLBACK_VERBOSE, SANDYBRIDGE_FALLBACK);
	    return &gotoblas_SANDYBRIDGE;
	  } else {
	    openblas_warning(FALLBACK_VERBOSE, NEHALEM_FALLBACK);
	    return &gotoblas_NEHALEM; //OS doesn't support AVX. Use old kernels.
	  }
	}
	//Intel Braswell / Avoton
	if (model == 12 || model == 13) { 
	  return &gotoblas_NEHALEM;
	}	
	return NULL;
      case 5:
	//Intel Broadwell
	if (model == 6) {
	  if(support_avx2())
	    return &gotoblas_HASWELL;
	  if(support_avx()) {
	    openblas_warning(FALLBACK_VERBOSE, SANDYBRIDGE_FALLBACK);
	    return &gotoblas_SANDYBRIDGE;
	  } else {
	    openblas_warning(FALLBACK_VERBOSE, NEHALEM_FALLBACK);
	    return &gotoblas_NEHALEM; //OS doesn't support AVX. Use old kernels.
	  }
	}
	if (model == 5) {
	// Intel Cooperlake
          if(support_avx512_bf16())
             return &gotoblas_COOPERLAKE;
	// Intel Skylake X
          if (support_avx512()) 
	    return &gotoblas_SKYLAKEX;
	  if(support_avx2()){
	    openblas_warning(FALLBACK_VERBOSE, HASWELL_FALLBACK);
	    return &gotoblas_HASWELL;
          }
	  if(support_avx()) {
	    openblas_warning(FALLBACK_VERBOSE, SANDYBRIDGE_FALLBACK);
	    return &gotoblas_SANDYBRIDGE;
	  } else {
          openblas_warning(FALLBACK_VERBOSE, NEHALEM_FALLBACK);
          return &gotoblas_NEHALEM;
          }
	}
	//Intel Skylake
	if (model == 14) {
	  if(support_avx2())
	    return &gotoblas_HASWELL;
	  if(support_avx()) {
	    openblas_warning(FALLBACK_VERBOSE, SANDYBRIDGE_FALLBACK);
	    return &gotoblas_SANDYBRIDGE;
	  } else {
	    openblas_warning(FALLBACK_VERBOSE, NEHALEM_FALLBACK);
	    return &gotoblas_NEHALEM; //OS doesn't support AVX. Use old kernels.
	  }
	}
	//Intel Phi Knights Landing
	if (model == 7) {
	  if(support_avx2()){
	    openblas_warning(FALLBACK_VERBOSE, HASWELL_FALLBACK);
	    return &gotoblas_HASWELL;
	  }  
	  if(support_avx()) {
	    openblas_warning(FALLBACK_VERBOSE, SANDYBRIDGE_FALLBACK);
	    return &gotoblas_SANDYBRIDGE;
	  } else {
	    openblas_warning(FALLBACK_VERBOSE, NEHALEM_FALLBACK);
	    return &gotoblas_NEHALEM; //OS doesn't support AVX. Use old kernels.
	  }
	}
	//Apollo Lake or Denverton
	if (model == 12 || model == 15) { 
	  return &gotoblas_NEHALEM;
	}	
	return NULL;
      case 6:
        if (model == 6) {
          // Cannon Lake
	  if(support_avx2())
	    return &gotoblas_HASWELL;
	  if(support_avx()) {
	    openblas_warning(FALLBACK_VERBOSE, SANDYBRIDGE_FALLBACK);
	    return &gotoblas_SANDYBRIDGE;
	  } else {
	    openblas_warning(FALLBACK_VERBOSE, NEHALEM_FALLBACK);
	    return &gotoblas_NEHALEM;
	  }
        }
	if (model == 10 || model == 12){
          // Ice Lake SP
	   if(support_avx512_bf16())
             return &gotoblas_COOPERLAKE;
          if (support_avx512()) 
	    return &gotoblas_SKYLAKEX;
	  if(support_avx2())
	    return &gotoblas_HASWELL;
	  if(support_avx()) {
	    openblas_warning(FALLBACK_VERBOSE, SANDYBRIDGE_FALLBACK);
	    return &gotoblas_SANDYBRIDGE;
	  } else {
	    openblas_warning(FALLBACK_VERBOSE, NEHALEM_FALLBACK);
	    return &gotoblas_NEHALEM;
	  }
        }
        return NULL;  
      case 7:
	if (model == 10) // Goldmont Plus
	   return &gotoblas_NEHALEM;
        if (model == 13 || model == 14) {
	// Ice Lake
          if (support_avx512()) 
	    return &gotoblas_SKYLAKEX;
	  if(support_avx2()){
	    openblas_warning(FALLBACK_VERBOSE, HASWELL_FALLBACK);
	    return &gotoblas_HASWELL;
          }
	  if(support_avx()) {
	    openblas_warning(FALLBACK_VERBOSE, SANDYBRIDGE_FALLBACK);
	    return &gotoblas_SANDYBRIDGE;
	  } else {
          openblas_warning(FALLBACK_VERBOSE, NEHALEM_FALLBACK);
          return &gotoblas_NEHALEM;
          }
        }
        return NULL;  
      case 8:
        if (model == 12 || model == 13) { // Tiger Lake
          if (support_avx512()) 
            return &gotoblas_SKYLAKEX;
          if(support_avx2()){
            openblas_warning(FALLBACK_VERBOSE, HASWELL_FALLBACK);
            return &gotoblas_HASWELL;
          }
          if(support_avx()) {
            openblas_warning(FALLBACK_VERBOSE, SANDYBRIDGE_FALLBACK);
            return &gotoblas_SANDYBRIDGE;
          } else {
          openblas_warning(FALLBACK_VERBOSE, NEHALEM_FALLBACK);
          return &gotoblas_NEHALEM;
          }
        }
	if (model == 14 ) { // Kaby Lake, Coffee Lake
	  if(support_avx2())
	    return &gotoblas_HASWELL;
	  if(support_avx()) {
	    openblas_warning(FALLBACK_VERBOSE, SANDYBRIDGE_FALLBACK);
	    return &gotoblas_SANDYBRIDGE;
	  } else {
	    openblas_warning(FALLBACK_VERBOSE, NEHALEM_FALLBACK);
	    return &gotoblas_NEHALEM; //OS doesn't support AVX. Use old kernels.
	  }
	}
	if (model == 15){          // Sapphire Rapids
	   if(support_amx_bf16())
	     return &gotoblas_SAPPHIRERAPIDS;
	   if(support_avx512_bf16())
             return &gotoblas_COOPERLAKE;
          if (support_avx512()) 
	    return &gotoblas_SKYLAKEX;
	  if(support_avx2())
	    return &gotoblas_HASWELL;
	  if(support_avx()) {
	    openblas_warning(FALLBACK_VERBOSE, SANDYBRIDGE_FALLBACK);
	    return &gotoblas_SANDYBRIDGE;
	  } else {
	    openblas_warning(FALLBACK_VERBOSE, NEHALEM_FALLBACK);
	    return &gotoblas_NEHALEM;
	  }
        }
	return NULL;
	
	
      case 9:
        if (model == 7 || model == 10) { // Alder Lake
	   if(support_avx512_bf16())
             return &gotoblas_COOPERLAKE;
          if (support_avx512()) 
	    return &gotoblas_SKYLAKEX;
          if(support_avx2()){
            return &gotoblas_HASWELL;
          }
          if(support_avx()) {
            openblas_warning(FALLBACK_VERBOSE, SANDYBRIDGE_FALLBACK);
            return &gotoblas_SANDYBRIDGE;
          } else {
          openblas_warning(FALLBACK_VERBOSE, NEHALEM_FALLBACK);
          return &gotoblas_NEHALEM;
          }
        }
	if (model == 14 ) { // Kaby Lake, Coffee Lake
	  if(support_avx2())
	    return &gotoblas_HASWELL;
	  if(support_avx()) {
	    openblas_warning(FALLBACK_VERBOSE, SANDYBRIDGE_FALLBACK);
	    return &gotoblas_SANDYBRIDGE;
	  } else {
	    openblas_warning(FALLBACK_VERBOSE, NEHALEM_FALLBACK);
	    return &gotoblas_NEHALEM; //OS doesn't support AVX. Use old kernels.
	  }
	}
	return NULL;
      case 10:
        if (model == 5 || model == 6) {
	  if(support_avx2())
	    return &gotoblas_HASWELL;
	  if(support_avx()) {
	    openblas_warning(FALLBACK_VERBOSE, SANDYBRIDGE_FALLBACK);
	    return &gotoblas_SANDYBRIDGE;
	  } else {
	    openblas_warning(FALLBACK_VERBOSE, NEHALEM_FALLBACK);
	    return &gotoblas_NEHALEM; //OS doesn't support AVX. Use old kernels.
	  }
        }
        if (model == 7) {
	  if (support_avx512()) 
	    return &gotoblas_SKYLAKEX;
	  if(support_avx2())
	    return &gotoblas_HASWELL;
	  if(support_avx()) {
	    openblas_warning(FALLBACK_VERBOSE, SANDYBRIDGE_FALLBACK);
	    return &gotoblas_SANDYBRIDGE;
	  } else {
	    openblas_warning(FALLBACK_VERBOSE, NEHALEM_FALLBACK);
	    return &gotoblas_NEHALEM; //OS doesn't support AVX. Use old kernels.
	  }
        }      
	return NULL;
      }
      break;
    case 0xf:
      if (model <= 0x2) return &gotoblas_NORTHWOOD;
      return &gotoblas_PRESCOTT;
    }
  }

  if (vendor == VENDOR_AMD || vendor == VENDOR_HYGON){
    if (family <= 0xe) {
        // Verify that CPU has 3dnow and 3dnowext before claiming it is Athlon
        cpuid(0x80000000, &eax, &ebx, &ecx, &edx);
        if ( (eax & 0xffff)  >= 0x01) {
            cpuid(0x80000001, &eax, &ebx, &ecx, &edx);
            if ((edx & (1 << 30)) == 0 || (edx & (1u << 31)) == 0)
              return NULL;
          }
        else
          return NULL;

        return &gotoblas_ATHLON;
      }
    if (family == 0xf){
      if ((exfamily == 0) || (exfamily == 2)) {
	if (ecx & (1 <<  0)) return &gotoblas_OPTERON_SSE3;
	else return &gotoblas_OPTERON;
      }  else if (exfamily == 5 || exfamily == 7) {
	return &gotoblas_BOBCAT;
      } else if (exfamily == 6) {
	if(model == 1){
	  //AMD Bulldozer Opteron 6200 / Opteron 4200 / AMD FX-Series
	  if(support_avx())
	    return &gotoblas_BULLDOZER;
	  else{
	    openblas_warning(FALLBACK_VERBOSE, BARCELONA_FALLBACK);
	    return &gotoblas_BARCELONA; //OS doesn't support AVX. Use old kernels.
	  }
	}else if(model == 2 || model == 3){
	  //AMD Bulldozer Opteron 6300 / Opteron 4300 / Opteron 3300
	  if(support_avx())
	    return &gotoblas_PILEDRIVER;
	  else{
	    openblas_warning(FALLBACK_VERBOSE, BARCELONA_FALLBACK);
	    return &gotoblas_BARCELONA; //OS doesn't support AVX. Use old kernels.
	  }
	}else if(model == 5){
	  if(support_avx())
	    return &gotoblas_EXCAVATOR;
	  else{
	    openblas_warning(FALLBACK_VERBOSE, BARCELONA_FALLBACK);
	    return &gotoblas_BARCELONA; //OS doesn't support AVX. Use old kernels.
	  }
	}else if(model == 0 || model == 8){
	  if (exmodel == 1) {
	    //AMD Trinity
	    if(support_avx())
	      return &gotoblas_PILEDRIVER;
	    else{
	      openblas_warning(FALLBACK_VERBOSE, BARCELONA_FALLBACK);
	      return &gotoblas_BARCELONA; //OS doesn't support AVX. Use old kernels.
	    }
	   }else if (exmodel == 3) {
	    //AMD STEAMROLLER
	    if(support_avx())
	      return &gotoblas_STEAMROLLER;
	    else{
	      openblas_warning(FALLBACK_VERBOSE, BARCELONA_FALLBACK);
	      return &gotoblas_BARCELONA; //OS doesn't support AVX. Use old kernels.
	    }
	  }else if (exmodel == 6) {
	    if(support_avx())
	      return &gotoblas_EXCAVATOR;
	    else{
	      openblas_warning(FALLBACK_VERBOSE, BARCELONA_FALLBACK);
	      return &gotoblas_BARCELONA; //OS doesn't support AVX. Use old kernels.
	    }

	  }
	}
      } else if (exfamily == 8) {
	/* if (model == 1 || model == 8) */ {
	  if(support_avx())
	    return &gotoblas_ZEN;
	  else{
	    openblas_warning(FALLBACK_VERBOSE, BARCELONA_FALLBACK);
	    return &gotoblas_BARCELONA; //OS doesn't support AVX. Use old kernels.
	  }
	}
      } else if (exfamily == 9) {  
	  if(support_avx())
	    return &gotoblas_ZEN;
	  else{
	    openblas_warning(FALLBACK_VERBOSE, BARCELONA_FALLBACK);
	    return &gotoblas_BARCELONA; //OS doesn't support AVX. Use old kernels.
          }
      } else if (exfamily == 10) {
	  if(support_avx512_bf16())
	    return &gotoblas_COOPERLAKE;
	  if(support_avx512())
	    return &gotoblas_SKYLAKEX;
	  if(support_avx())
	    return &gotoblas_ZEN;
	  else{
	    openblas_warning(FALLBACK_VERBOSE, BARCELONA_FALLBACK);
	    return &gotoblas_BARCELONA; //OS doesn't support AVX. Use old kernels.
          }
      }else {
	return NULL;
      }
   
    }
  }

  if (vendor == VENDOR_CENTAUR) {
    switch (family) {
    case 0x6:
      if (model == 0xf && stepping < 0xe)
        return &gotoblas_NANO;
      return &gotoblas_NEHALEM;
	case 0x7:
      switch (exmodel) {
      case 5:
      case 6:
        if (support_avx2())
          return &gotoblas_ZEN;
        else
          return &gotoblas_DUNNINGTON;
      default:
        return &gotoblas_NEHALEM;
      }
    default:
      if (family >= 0x8)
        return &gotoblas_NEHALEM;
    }
  }

  if (vendor == VENDOR_ZHAOXIN) {
    switch (family) {
      case 0x7:
        switch (exmodel) {
        case 5:
          if (support_avx2())
            return &gotoblas_ZEN;
          else
            return &gotoblas_DUNNINGTON;
        default:
          return &gotoblas_NEHALEM;
        }
      default:
        return &gotoblas_NEHALEM;
    }
  }

  return NULL;
}

static char *corename[] = {
    "Unknown",
    "Katmai",
    "Coppermine",
    "Northwood",
    "Prescott",
    "Banias",
    "Atom",
    "Core2",
    "Penryn",
    "Dunnington",
    "Nehalem",
    "Athlon",
    "Opteron",
    "Opteron_SSE3",
    "Barcelona",
    "Nano",
    "Sandybridge",
    "Bobcat",
    "Bulldozer",
    "Piledriver",
    "Haswell",
    "Steamroller",
    "Excavator",
    "Zen",
    "SkylakeX",
    "Cooperlake",
    "SapphireRapids"
};

char *gotoblas_corename(void) {

  if (gotoblas == &gotoblas_KATMAI)       return corename[ 1];
  if (gotoblas == &gotoblas_COPPERMINE)   return corename[ 2];
  if (gotoblas == &gotoblas_NORTHWOOD)    return corename[ 3];
  if (gotoblas == &gotoblas_PRESCOTT)     return corename[ 4];
  if (gotoblas == &gotoblas_BANIAS)       return corename[ 5];
  if (gotoblas == &gotoblas_ATOM)
#ifdef DYNAMIC_OLDER
           return corename[ 6];
#else
           return corename[10];
#endif
  if (gotoblas == &gotoblas_CORE2)        return corename[ 7];
  if (gotoblas == &gotoblas_PENRYN)
#ifdef DYNAMIC_OLDER
           return corename[ 8];
#else
           return corename[7];
#endif
  if (gotoblas == &gotoblas_DUNNINGTON)
#ifdef DYNAMIC_OLDER
           return corename[ 9];
#else
           return corename[7];
#endif
  if (gotoblas == &gotoblas_NEHALEM)      return corename[10];
  if (gotoblas == &gotoblas_ATHLON)       return corename[11];
  if (gotoblas == &gotoblas_OPTERON_SSE3)
#ifdef DYNAMIC_OLDER
           return corename[12];
#else
           return corename[7];
#endif
  if (gotoblas == &gotoblas_OPTERON)
#ifdef DYNAMIC_OLDER
           return corename[13];
#else
           return corename[7];
#endif
  if (gotoblas == &gotoblas_BARCELONA)    return corename[14];
  if (gotoblas == &gotoblas_NANO)
#ifdef DYNAMIC_OLDER
           return corename[15];
#else
           return corename[10];
#endif
  if (gotoblas == &gotoblas_SANDYBRIDGE)  return corename[16];
  if (gotoblas == &gotoblas_BOBCAT)
#ifdef DYNAMIC_OLDER
           return corename[17];
#else
           return corename[7];
#endif
  if (gotoblas == &gotoblas_BULLDOZER)    return corename[18];
  if (gotoblas == &gotoblas_PILEDRIVER)   return corename[19];
  if (gotoblas == &gotoblas_HASWELL)      return corename[20];
  if (gotoblas == &gotoblas_STEAMROLLER)  return corename[21];
  if (gotoblas == &gotoblas_EXCAVATOR)    return corename[22];
  if (gotoblas == &gotoblas_ZEN)          return corename[23];
  if (gotoblas == &gotoblas_SKYLAKEX)     return corename[24];
  if (gotoblas == &gotoblas_COOPERLAKE)   return corename[25];
  if (gotoblas == &gotoblas_SAPPHIRERAPIDS) return corename[26];
  return corename[0];
}



static gotoblas_t *force_coretype(char *coretype){

	int i ;
	int found = -1;
	char message[128];
	//char mname[20];

	for ( i=1 ; i <= 25; i++)
	{
		if (!strncasecmp(coretype,corename[i],20))
		{
			found = i;
			break;
		}
	}
	if (found < 0)
	{
	        //strncpy(mname,coretype,20);
	        snprintf(message, 128, "Core not found: %s\n",coretype);
    		openblas_warning(1, message);
		return(NULL);
	}

	switch (found)
	{
		case 25: return (&gotoblas_COOPERLAKE);
		case 24: return (&gotoblas_SKYLAKEX);	
		case 23: return (&gotoblas_ZEN);
		case 22: return (&gotoblas_EXCAVATOR);
		case 21: return (&gotoblas_STEAMROLLER);
		case 20: return (&gotoblas_HASWELL);
		case 19: return (&gotoblas_PILEDRIVER);
		case 18: return (&gotoblas_BULLDOZER);
		case 17: return (&gotoblas_BOBCAT);
		case 16: return (&gotoblas_SANDYBRIDGE);
		case 15: return (&gotoblas_NANO);
		case 14: return (&gotoblas_BARCELONA);
		case 13: return (&gotoblas_OPTERON);
		case 12: return (&gotoblas_OPTERON_SSE3);
		case 11: return (&gotoblas_ATHLON);
		case 10: return (&gotoblas_NEHALEM);
		case  9: return (&gotoblas_DUNNINGTON);
		case  8: return (&gotoblas_PENRYN);
		case  7: return (&gotoblas_CORE2);
		case  6: return (&gotoblas_ATOM);
		case  5: return (&gotoblas_BANIAS);
		case  4: return (&gotoblas_PRESCOTT);
		case  3: return (&gotoblas_NORTHWOOD);
		case  2: return (&gotoblas_COPPERMINE);
		case  1: return (&gotoblas_KATMAI);
	}
	return(NULL);

}




void gotoblas_dynamic_init(void) {

  char coremsg[128];
  char coren[22];
  char *p;


  if (gotoblas) return;

  p = getenv("OPENBLAS_CORETYPE");
  if ( p )
  {
	gotoblas = force_coretype(p);
  }
  else
  {
  	gotoblas = get_coretype();
  }

#ifdef ARCH_X86
  if (gotoblas == NULL) gotoblas = &gotoblas_KATMAI;
#else
  if (gotoblas == NULL) {
   if (support_avx512_bf16()) gotoblas = &gotoblas_COOPERLAKE;
   else if (support_avx512()) gotoblas = &gotoblas_SKYLAKEX;
   else if   (support_avx2()) gotoblas = &gotoblas_HASWELL;
   else if    (support_avx()) gotoblas = &gotoblas_SANDYBRIDGE;
   else                       gotoblas = &gotoblas_PRESCOTT;
  }
  /* sanity check, if 64bit pointer we can't have a 32 bit cpu */
  if (sizeof(void*) == 8) {
      if (gotoblas == &gotoblas_KATMAI ||
          gotoblas == &gotoblas_COPPERMINE ||
          gotoblas == &gotoblas_NORTHWOOD ||
          gotoblas == &gotoblas_BANIAS ||
          gotoblas == &gotoblas_ATHLON)
          gotoblas = &gotoblas_PRESCOTT;
  }
#endif

  if (gotoblas && gotoblas -> init) {
    strncpy(coren,gotoblas_corename(),20);
    sprintf(coremsg, "Core: %s\n",coren);
    openblas_warning(2, coremsg);
    gotoblas -> init();
  } else {
    openblas_warning(0, "OpenBLAS : Architecture Initialization failed. No initialization function found.\n");
    exit(1);
  }

}

void gotoblas_dynamic_quit(void) {

  gotoblas = NULL;

}
