#ifndef GEMM_COMMON_C
#define GEMM_COMMON_C
#include "common.h"

#include <altivec.h>
#include <inttypes.h>

#define NBMAX 4096

#define FORCEINLINE      inline __attribute__((always_inline))

#ifdef _ARCH_PWR10
#ifdef __has_builtin
#if !__has_builtin(__builtin_vsx_assemble_pair)
#define __builtin_vsx_assemble_pair __builtin_mma_assemble_pair
#endif
#if !__has_builtin(__builtin_vsx_disassemble_pair)
#define __builtin_vsx_disassemble_pair __builtin_mma_disassemble_pair
#endif
#endif

#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#define __builtin_vsx_assemble_pair2(vp0, v0, v1) __builtin_vsx_assemble_pair(vp0, v1, v0)
#else
#define __builtin_vsx_assemble_pair2(vp0, v0, v1) __builtin_vsx_assemble_pair(vp0, v0, v1)
#endif

#define USE_VECTOR_PAIRS
#endif

#ifdef _AIX
#include<stdbool.h>
typedef __vector unsigned short vec_bf16;
#else
typedef __vector IFLOAT        vec_bf16;
#endif
typedef __vector FLOAT         vec_f32;
typedef __vector unsigned char vec_uc8;

FORCEINLINE vec_uc8 vec_load_vec(void *src)
{
  return vec_xl(0, (unsigned char *)(src));
}

FORCEINLINE void vec_load_pair(vec_f32 *dst, vec_f32 *src)
{
#ifdef USE_VECTOR_PAIRS
  __vector_pair vy0p;
#ifdef __clang__
  vy0p = __builtin_vsx_lxvp(0L, (const __vector_pair *)(src));
#else
  vy0p = *(__vector_pair *)((void *)src);
#endif
  __builtin_vsx_disassemble_pair((void *)(dst), &vy0p);
#else
  dst[0] = src[0];
  dst[1] = src[1];
#endif
}

FORCEINLINE void vec_store_pair(vec_f32 *dst, vec_f32 *src)
{
#ifdef USE_VECTOR_PAIRS
  __vector_pair vy0p;
  __builtin_vsx_assemble_pair2(&vy0p, (vec_uc8)src[1], (vec_uc8)src[0]);
#ifdef __clang__
  __builtin_vsx_stxvp(vy0p, 0L, (__vector_pair *)(dst));
#else
  *(__vector_pair *)((void *)dst) = vy0p;
#endif
#else
  dst[0] = src[0];
  dst[1] = src[1];
#endif
}

FORCEINLINE vec_bf16 vec_loadN(void *src, BLASLONG n)
{
  IFLOAT *src2 = (IFLOAT *)(src);
#ifdef _ARCH_PWR9
  return vec_xl_len(src2, n * sizeof(IFLOAT));
#else
  __attribute__((aligned(16))) IFLOAT data[sizeof(vec_bf16) / sizeof(IFLOAT)];
  memset(data, 0, sizeof(vec_bf16));
  if (n & 4) {
    memcpy(data, src2, sizeof(uint64_t));
  }
  if (n & 2) {
    BLASLONG n4 = n & 4;
    memcpy(data + n4, src2 + n4, sizeof(uint32_t));
  }
  if (n & 1) {
    BLASLONG n6 = n & 6;
    data[n6] = src2[n6];
  }
  return (vec_bf16)vec_load_vec(data);
#endif
}

FORCEINLINE vec_f32 vec_loadN_f32(void *src, BLASLONG n)
{
#ifndef _ARCH_PWR9
  if (n & 4) {
    return (vec_f32)vec_load_vec(src);
  }
#endif
  return (vec_f32)vec_loadN(src, n * (sizeof(FLOAT) / sizeof(IFLOAT)));
}

FORCEINLINE void vec_loadN2_f32(vec_f32 *data, vec_f32 *src, BLASLONG n)
{
  data[0] = src[0];
  data[1] = vec_loadN_f32(&src[1], n);
}

FORCEINLINE void vec_storeN(vec_bf16 data, void *dst, BLASLONG n)
{
  IFLOAT *dst2 = (IFLOAT *)(dst);
#ifdef _ARCH_PWR9
  vec_xst_len(data, dst2, n * sizeof(IFLOAT));
#else
  if (n & 8) {
    vec_xst(data, 0, dst2);
    return;
  }
  __attribute__((aligned(16))) IFLOAT data2[sizeof(vec_f32) / sizeof(IFLOAT)];
  vec_xst(data, 0, data2);
  if (n & 4) {
    memcpy(dst2, data2, sizeof(uint64_t));
  }
  if (n & 2) {
    BLASLONG n4 = n & 4;
    memcpy(dst2 + n4, data2 + n4, sizeof(uint32_t));
  }
  if (n & 1) {
    BLASLONG n6 = n & 6;
    dst2[n6] = data2[n6];
  }
#endif
}

FORCEINLINE void vec_storeN_f32(vec_f32 data, void *dst, BLASLONG n)
{
#ifndef _ARCH_PWR9
  if (n & 4) {
    vec_xst(data, 0, (FLOAT *)dst);
    return;
  }
#endif
  return vec_storeN((vec_bf16)data, dst, n * (sizeof(FLOAT) / sizeof(IFLOAT)));
}

FORCEINLINE void vec_storeN2_f32(vec_f32 *data, vec_f32 *dst, BLASLONG n)
{
  dst[0] = data[0];
  vec_storeN_f32(data[1], &dst[1], n);
}
#endif
