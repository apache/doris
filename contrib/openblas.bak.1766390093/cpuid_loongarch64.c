/*****************************************************************************
Copyright (c) 2011-2024, The OpenBLAS Project
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

#include <stdint.h>
#include <stdio.h>
#include <math.h>
#include <string.h>
#include <sys/auxv.h>

#define CPU_LA64_GENERIC     0
#define CPU_LA264            1
#define CPU_LA364            2
#define CPU_LA464            3
#define CPU_LA664            4

#define CORE_LA64_GENERIC    0
#define CORE_LA264           1
#define CORE_LA464           2

#define LA_HWCAP_LSX    (1U << 4)
#define LA_HWCAP_LASX   (1U << 5)

#define LOONGARCH_CFG0      0x00
#define LOONGARCH_CFG2      0x02
#define LOONGARCH_CFG10     0x10
#define LOONGARCH_CFG11     0x11
#define LOONGARCH_CFG12     0x12
#define LOONGARCH_CFG13     0x13
#define LOONGARCH_CFG14     0x14
#define LASX_MASK           1<<7
#define LSX_MASK            1<<6
#define PRID_SERIES_MASK    0xf000
#define PRID_SERIES_LA264   0xa000
#define PRID_SERIES_LA364   0xb000
#define PRID_SERIES_LA464   0xc000
#define PRID_SERIES_LA664   0xd000

#define CACHE_INFO_L1_IU    0
#define CACHE_INFO_L1_D     1
#define CACHE_INFO_L2_IU    2
#define CACHE_INFO_L2_D     3
#define CACHE_INFO_L3_IU    4
#define CACHE_INFO_L3_D     5
#define L1_IU_PRESENT_MASK  0x0001
#define L1_IU_UNITY_MASK    0x0002
#define L1_D_PRESENT_MASK   0x0004
#define L2_IU_PRESENT_MASK  0x0008
#define L2_IU_UNITY_MASK    0x0010
#define L2_D_PRESENT_MASK   0x0080
#define L3_IU_PRESENT_MASK  0x0400
#define L3_IU_UNITY_MASK    0x0800
#define L3_D_PRESENT_MASK   0x4000
#define CACHE_WAY_MINUS_1_MASK      0x0000ffff
#define CACHE_INDEX_LOG2_MASK       0x00ff0000
#define CACHE_LINESIZE_LOG2_MASK    0x7f000000

typedef struct {
  int size;
  int associative;
  int linesize;
  int unify;
  int present;
} cache_info_t;

/* Using microarchitecture representation */
static char *cpuname[] = {
  "LA64_GENERIC",
  "LA264", /* Loongson 64bit, 2-issue, Like 2K1000LA */
  "LA364", /* Loongson 64bit, 3-issue, Like 2K2000 */
  "LA464", /* Loongson 64bit, 4-issue, Like 3A5000, 3C5000L, 3C5000 and 3D5000 */
  "LA664"  /* Loongson 64bit, 6-issue, Like 3A6000, 3C6000 and 3D6000 */
};

static char *cpuname_lower[] = {
  "la64_generic",
  "la264",
  "la364",
  "la464",
  "la664"
};

static char *corename[] = {
  "LA64_GENERIC", /* Implies using scalar instructions for optimization */
  "LA264", /* Implies using LSX  instructions for optimization */
  "LA464", /* Implies using LASX instructions for optimization */
};

static char *corename_lower[] = {
  "la64_generic",
  "la264",
  "la464",
};

/*
 * Obtain cache and processor identification
 * through the cpucfg command.
 */
static void get_cacheinfo(int type, cache_info_t *cacheinfo) {
  cache_info_t cache_info;
  memset(&cache_info, 0, sizeof(cache_info));
  uint32_t reg_10 = 0;
  __asm__ volatile (
    "cpucfg %0, %1 \n\t"
    : "+&r"(reg_10)
    : "r"(LOONGARCH_CFG10)
  );

  switch (type) {
    case CACHE_INFO_L1_IU:
      if (reg_10 & L1_IU_PRESENT_MASK) {
        uint32_t reg_11 = 0;
        cache_info.present = reg_10 & L1_IU_PRESENT_MASK;
        cache_info.unify   = reg_10 & L1_IU_UNITY_MASK;
        __asm__ volatile (
          "cpucfg %0, %1 \n\t"
          : "+&r"(reg_11)
          : "r"(LOONGARCH_CFG11)
        );
        cache_info.associative  = (reg_11 & CACHE_WAY_MINUS_1_MASK) + 1;
        cache_info.linesize = 1 << ((reg_11 & CACHE_LINESIZE_LOG2_MASK) >> 24);
        cache_info.size = cache_info.associative * cache_info.linesize *
                          (1 << ((reg_11 & CACHE_INDEX_LOG2_MASK) >> 16));
      }
    break;

    case CACHE_INFO_L1_D:
      if (reg_10 & L1_D_PRESENT_MASK) {
        uint32_t reg_12 = 0;
        cache_info.present = reg_10 & L1_D_PRESENT_MASK;
        __asm__ volatile (
          "cpucfg %0, %1 \n\t"
          : "+&r"(reg_12)
          : "r"(LOONGARCH_CFG12)
        );
        cache_info.associative  = (reg_12 & CACHE_WAY_MINUS_1_MASK) + 1;
        cache_info.linesize = 1 << ((reg_12 & CACHE_LINESIZE_LOG2_MASK) >> 24);
        cache_info.size = cache_info.associative * cache_info.linesize *
                          (1 << ((reg_12 & CACHE_INDEX_LOG2_MASK) >> 16));
      }
    break;

    case CACHE_INFO_L2_IU:
      if (reg_10 & L2_IU_PRESENT_MASK) {
        uint32_t reg_13 = 0;
        cache_info.present = reg_10 & L2_IU_PRESENT_MASK;
        cache_info.unify   = reg_10 & L2_IU_UNITY_MASK;
        __asm__ volatile (
          "cpucfg %0, %1 \n\t"
          : "+&r"(reg_13)
          : "r"(LOONGARCH_CFG13)
        );
        cache_info.associative  = (reg_13 & CACHE_WAY_MINUS_1_MASK) + 1;
        cache_info.linesize = 1 << ((reg_13 & CACHE_LINESIZE_LOG2_MASK) >> 24);
        cache_info.size = cache_info.associative * cache_info.linesize *
                          (1 << ((reg_13 & CACHE_INDEX_LOG2_MASK) >> 16));
      }
    break;

    case CACHE_INFO_L2_D:
      if (reg_10 & L2_D_PRESENT_MASK) {
        cache_info.present = reg_10 & L2_D_PRESENT_MASK;
        // No date fetch
      }
    break;

    case CACHE_INFO_L3_IU:
      if (reg_10 & L3_IU_PRESENT_MASK) {
        uint32_t reg_14 = 0;
        cache_info.present = reg_10 & L3_IU_PRESENT_MASK;
        cache_info.unify   = reg_10 & L3_IU_UNITY_MASK;
        __asm__ volatile (
          "cpucfg %0, %1 \n\t"
          : "+&r"(reg_14)
          : "r"(LOONGARCH_CFG14)
        );
        cache_info.associative  = (reg_14 & CACHE_WAY_MINUS_1_MASK) + 1;
        cache_info.linesize = 1 << ((reg_14 & CACHE_LINESIZE_LOG2_MASK) >> 24);
        cache_info.size = cache_info.associative * cache_info.linesize *
                          (1 << ((reg_14 & CACHE_INDEX_LOG2_MASK) >> 16));
      }
    break;

    case CACHE_INFO_L3_D:
      if (reg_10 & L3_D_PRESENT_MASK) {
        cache_info.present = reg_10 & L3_D_PRESENT_MASK;
        // No data fetch
      }
    break;

    default:
    break;
  }
  *cacheinfo = cache_info;
}

static uint32_t get_prid() {
  uint32_t reg = 0;
  __asm__ volatile (
    "cpucfg %0, %1 \n\t"
    : "+&r"(reg)
    : "r"(LOONGARCH_CFG0)
  );
  return reg;
}

static void get_cpucount(uint32_t *count) {
  uint32_t num = 0;
  FILE *f = fopen("/proc/cpuinfo", "r");
  if (!f) return;
  char buf[200];
  while (fgets(buf, sizeof(buf), f))
  {
    if (!strncmp("processor", buf, 9))
      num ++;
  }
  fclose(f);
  *count = num;
}

/* Detect whether the OS supports the LASX instruction set */
static int os_support_lasx() {
  int hwcap  = (int)getauxval(AT_HWCAP);

  if (hwcap & LA_HWCAP_LASX)
    return 1;
  else
    return 0;
}

/* Detect whether the OS supports the LSX instruction set */
static int os_support_lsx() {
  int hwcap  = (int)getauxval(AT_HWCAP);

  if (hwcap & LA_HWCAP_LSX)
    return 1;
  else
    return 0;
}

int get_coretype(void) {
  uint32_t prid = get_prid();
  switch (prid & PRID_SERIES_MASK) {
    case (PRID_SERIES_LA464):
    case (PRID_SERIES_LA664):
      if (os_support_lasx())
        return CORE_LA464;
      else if (os_support_lsx())
        return CORE_LA264;
      else
        return CORE_LA64_GENERIC;
    break;

    case (PRID_SERIES_LA264):
    case (PRID_SERIES_LA364):
      if (os_support_lsx())
        return CORE_LA264;
      else
        return CORE_LA64_GENERIC;
    break;

    default:
      return CORE_LA64_GENERIC;
    break;
  }
}

int get_cputype(void) {
  uint32_t prid = get_prid();
  switch (prid & PRID_SERIES_MASK) {
    case (PRID_SERIES_LA264):
      return CPU_LA264;
    break;

    case (PRID_SERIES_LA364):
      return CPU_LA364;
    break;

    case (PRID_SERIES_LA464):
      return CPU_LA464;
    break;

    case (PRID_SERIES_LA664):
      return CPU_LA664;
    break;

    default:
      return CPU_LA64_GENERIC;
    break;
  }
}

char *get_corename(void) {
  return corename[get_coretype()];
}

void get_libname(void){
  printf("%s", corename_lower[get_coretype()]);
}

void get_architecture(void) {
  printf("LOONGARCH64");
}

void get_subarchitecture(void) {
  printf("%s", cpuname[get_cputype()]);
}

void get_subdirname(void) {
  printf("loongarch64");
}

void get_cpuconfig(void) {
  cache_info_t info;
  uint32_t num_cores = 0;

  printf("#define %s\n", corename[get_coretype()]); // Core name

  printf("#define CPU_NAME %s\n", cpuname[get_cputype()]); // Cpu microarchitecture name

  get_cacheinfo(CACHE_INFO_L1_IU, &info);
  if (info.present) {
    if (info.unify) { // Unified cache, without distinguishing between instructions and data
      printf("#define L1_SIZE %d\n", info.size);
      printf("#define L1_ASSOCIATIVE %d\n", info.associative);
      printf("#define L1_LINESIZE %d\n", info.linesize);
    } else {
      printf("#define L1_CODE_SIZE %d\n", info.size);
      printf("#define L1_CODE_ASSOCIATIVE %d\n", info.associative);
      printf("#define L1_CODE_LINESIZE %d\n", info.linesize);
    }
  }

  if (!info.unify) {
    get_cacheinfo(CACHE_INFO_L1_D, &info);
    if (info.present) {
      printf("#define L1_DATA_SIZE %d\n", info.size);
      printf("#define L1_DATA_ASSOCIATIVE %d\n", info.associative);
      printf("#define L1_DATA_LINESIZE %d\n", info.linesize);
    }
  }

  get_cacheinfo(CACHE_INFO_L2_IU, &info);
  if (info.present > 0) {
    if (info.unify) {
      printf("#define L2_SIZE %d\n", info.size);
      printf("#define L2_ASSOCIATIVE %d\n", info.associative);
      printf("#define L2_LINESIZE %d\n", info.linesize);
    } else {
      printf("#define L2_CODE_SIZE %d\n", info.size);
      printf("#define L2_CODE_ASSOCIATIVE %d\n", info.associative);
      printf("#define L2_CODE_LINESIZE %d\n", info.linesize);
    }
  }

  get_cacheinfo(CACHE_INFO_L3_IU, &info);
  if (info.present > 0) {
    if (info.unify) {
      printf("#define L3_SIZE %d\n", info.size);
      printf("#define L3_ASSOCIATIVE %d\n", info.associative);
      printf("#define L3_LINESIZE %d\n", info.linesize);
    } else {
      printf("#define L3_CODE_SIZE %d\n", info.size);
      printf("#define L3_CODE_ASSOCIATIVE %d\n", info.associative);
      printf("#define L3_CODE_LINESIZE %d\n", info.linesize);
    }
  }

  if(os_support_lsx)  printf("#define HAVE_LSX\n");
  if(os_support_lasx) printf("#define HAVE_LASX\n");

  get_cpucount(&num_cores);
  if (num_cores)
    printf("#define NUM_CORES %d\n", num_cores);

  //TODO: It’s unclear what this entry represents, but it is indeed necessary.
  //It has been set based on reference to other platforms.
  printf("#define DTB_DEFAULT_ENTRIES 64\n");
}
