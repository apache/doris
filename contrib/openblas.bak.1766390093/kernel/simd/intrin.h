#ifndef _INTRIN_H_
#define _INTRIN_H_

#if defined(_MSC_VER)
#define BLAS_INLINE __inline
#elif defined(__GNUC__)
#if defined(__STRICT_ANSI__)
#define BLAS_INLINE __inline__
#else
#define BLAS_INLINE inline
#endif
#else
#define BLAS_INLINE
#endif

#ifdef _MSC_VER
#define BLAS_FINLINE static __forceinline
#elif defined(__GNUC__)
#define BLAS_FINLINE static BLAS_INLINE __attribute__((always_inline))
#else
#define BLAS_FINLINE static
#endif

#ifdef __cplusplus
extern "C" {
#endif
// include head
/** SSE **/
#ifdef HAVE_SSE
#include <xmmintrin.h>
#endif
/** SSE2 **/
#ifdef HAVE_SSE2
#include <emmintrin.h>
#endif
/** SSE3 **/
#ifdef HAVE_SSE3
#include <pmmintrin.h>
#endif
/** SSSE3 **/
#ifdef HAVE_SSSE3
#include <tmmintrin.h>
#endif
/** SSE41 **/
#ifdef HAVE_SSE4_1
#include <smmintrin.h>
#endif

/** AVX **/
#if defined(HAVE_AVX) || defined(HAVE_FMA3)
#include <immintrin.h>
#endif

/** NEON **/
#ifdef HAVE_NEON
#include <arm_neon.h>
#endif

// distribute
#if defined(HAVE_AVX512VL) || defined(HAVE_AVX512BF16)
#include "intrin_avx512.h"
#elif defined(HAVE_AVX2)
#include "intrin_avx.h"
#elif defined(HAVE_SSE2)
#include "intrin_sse.h"
#endif

#ifdef HAVE_NEON
#include "intrin_neon.h"
#endif

#ifndef V_SIMD
    #define V_SIMD 0
    #define V_SIMD_F64 0
#endif

#ifdef __cplusplus
}
#endif
#endif // _INTRIN_H_
