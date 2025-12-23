/* need a new enough GCC for avx512 support */
#ifdef __NVCOMPILER
#define NVCOMPVERS ( __NVCOMPILER_MAJOR__ * 100 + __NVCOMPILER_MINOR__ )
#endif
#if (( defined(__GNUC__)  && __GNUC__   > 6 && defined(__AVX512CD__)) || (defined(__clang__) && __clang_major__ >= 9)) || ( defined(__NVCOMPILER) && NVCOMPVERS >= 2203 )

#define HAVE_DASUM_KERNEL 1

#include <immintrin.h>

#include <stdint.h>

#ifndef ABS_K
#define ABS_K(a) ((a) > 0 ? (a) : (-(a)))
#endif

static FLOAT dasum_kernel(BLASLONG n, FLOAT *x1)
{
    BLASLONG i = 0;
    FLOAT sumf = 0.0;

    if (n >= 256) {
        BLASLONG align_512 = ((64 - ((uintptr_t)x1 & (uintptr_t)0x3f)) >> 3) & 0x7;

        for (i = 0; i < align_512; i++) {
            sumf += ABS_K(x1[i]);
        }
        
        n -= align_512;
        x1 += align_512;
    }

    BLASLONG tail_index_SSE = n&(~7);
    BLASLONG tail_index_AVX512 = n&(~255);

    //
    if ( n >= 256 ) {

        __m512d accum_0, accum_1, accum_2, accum_3;
        accum_0 = _mm512_setzero_pd();
        accum_1 = _mm512_setzero_pd();
        accum_2 = _mm512_setzero_pd();
        accum_3 = _mm512_setzero_pd();
        for (i = 0; i < tail_index_AVX512; i += 32) {
            accum_0 += _mm512_abs_pd(_mm512_load_pd(&x1[i + 0]));
            accum_1 += _mm512_abs_pd(_mm512_load_pd(&x1[i + 8]));
            accum_2 += _mm512_abs_pd(_mm512_load_pd(&x1[i +16]));
            accum_3 += _mm512_abs_pd(_mm512_load_pd(&x1[i +24]));
        }

        accum_0 = accum_0 + accum_1 + accum_2 + accum_3;
        sumf += _mm512_reduce_add_pd(accum_0);
    }

    if (n >= 8) {
        __m128d accum_20, accum_21, accum_22, accum_23;
        accum_20 = _mm_setzero_pd();  
        accum_21 = _mm_setzero_pd(); 
        accum_22 = _mm_setzero_pd(); 
        accum_23 = _mm_setzero_pd(); 

        __m128i abs_mask2 = _mm_set1_epi64x(0x7fffffffffffffff);
        for (i = tail_index_AVX512; i < tail_index_SSE; i += 8) {
            accum_20 += (__m128d)_mm_and_si128(_mm_loadu_si128((__m128i*)&x1[i + 0]), abs_mask2);
            accum_21 += (__m128d)_mm_and_si128(_mm_loadu_si128((__m128i*)&x1[i + 2]), abs_mask2);
            accum_22 += (__m128d)_mm_and_si128(_mm_loadu_si128((__m128i*)&x1[i + 4]), abs_mask2);
            accum_23 += (__m128d)_mm_and_si128(_mm_loadu_si128((__m128i*)&x1[i + 6]), abs_mask2);
        }

        accum_20 = accum_20 + accum_21 + accum_22 + accum_23;
        __m128d half_accum20;
        half_accum20 = _mm_hadd_pd(accum_20, accum_20);

        sumf += half_accum20[0];
    }

    for (i = tail_index_SSE; i < n; ++i) {
        sumf += ABS_K(x1[i]);
    }

    return sumf;
}
#endif
