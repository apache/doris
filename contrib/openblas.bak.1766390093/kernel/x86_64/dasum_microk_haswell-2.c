#ifdef __NVCOMPILER
#define NVCOMPVERS ( __NVCOMPILER_MAJOR__ * 100 + __NVCOMPILER_MINOR__ )
#endif
#if (( defined(__GNUC__)  && __GNUC__   > 6) || (defined(__clang__) && __clang_major__ >= 6)) && defined(__AVX2__) || ( defined(__NVCOMPILER) && NVCOMPVERS >= 2203 )

#define HAVE_DASUM_KERNEL

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
        BLASLONG align_256 = ((32 - ((uintptr_t)x1 & (uintptr_t)0x1f)) >> 3) & 0x3;

        for (i = 0; i < align_256; i++) {
            sumf += ABS_K(x1[i]);
        }

        n -= align_256;
        x1 += align_256;
    }

    BLASLONG tail_index_SSE = n&(~7);
    BLASLONG tail_index_AVX2 = n&(~255);

    if (n >= 256) {
        __m256d accum_0, accum_1, accum_2, accum_3;

        accum_0 = _mm256_setzero_pd();
        accum_1 = _mm256_setzero_pd();
        accum_2 = _mm256_setzero_pd();
        accum_3 = _mm256_setzero_pd();

         __m256i abs_mask = _mm256_set1_epi64x(0x7fffffffffffffff);
        for (i = 0; i < tail_index_AVX2; i += 16) {
            accum_0 += (__m256d)_mm256_and_si256(_mm256_load_si256((__m256i*)&x1[i+ 0]), abs_mask);
            accum_1 += (__m256d)_mm256_and_si256(_mm256_load_si256((__m256i*)&x1[i+ 4]), abs_mask);
            accum_2 += (__m256d)_mm256_and_si256(_mm256_load_si256((__m256i*)&x1[i+ 8]), abs_mask);
            accum_3 += (__m256d)_mm256_and_si256(_mm256_load_si256((__m256i*)&x1[i+12]), abs_mask);
        }

        accum_0 = accum_0 + accum_1 + accum_2 + accum_3;

        __m128d half_accum0;
        half_accum0 = _mm_add_pd(_mm256_extractf128_pd(accum_0, 0), _mm256_extractf128_pd(accum_0, 1));

        half_accum0 = _mm_hadd_pd(half_accum0, half_accum0);

        sumf += half_accum0[0];
    }
    
    if (n >= 8) {
        __m128d accum_20, accum_21, accum_22, accum_23;
        accum_20 = _mm_setzero_pd();  
        accum_21 = _mm_setzero_pd(); 
        accum_22 = _mm_setzero_pd(); 
        accum_23 = _mm_setzero_pd(); 

        __m128i abs_mask2 = _mm_set1_epi64x(0x7fffffffffffffff);
        for (i = tail_index_AVX2; i < tail_index_SSE; i += 8) {
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
