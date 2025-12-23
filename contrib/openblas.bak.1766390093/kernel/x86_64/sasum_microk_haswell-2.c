#ifdef __NVCOMPILER
#define NVCOMPVERS ( __NVCOMPILER_MAJOR__ * 100 + __NVCOMPILER_MINOR__ )
#endif
#if (( defined(__GNUC__)  && __GNUC__   > 6 && defined(__AVX2__)) || (defined(__clang__) && __clang_major__ >= 6)) || (defined(__NVCOMPILER) && NVCOMPVERS >= 2203 )

#define HAVE_SASUM_KERNEL 1

#include <immintrin.h>
#include <stdint.h>

#ifndef ABS_K
#define ABS_K(a) ((a) > 0 ? (a) : (-(a)))
#endif

static FLOAT sasum_kernel(BLASLONG n, FLOAT *x1)
{
    BLASLONG i = 0;
    FLOAT sumf = 0.0;

    if (n >= 256) { 
        BLASLONG align_256 = ((32 - ((uintptr_t)x1 & (uintptr_t)0x1f)) >> 2) & 0x7;

        for (i = 0; i < align_256; i++) {
            sumf += ABS_K(x1[i]);
        }

        n -= align_256;
        x1 += align_256;
    }

    BLASLONG tail_index_SSE = n&(~7);
    BLASLONG tail_index_AVX2 = n&(~255);

    if (n >= 256) {
        __m256 accum_0, accum_1, accum_2, accum_3;
        
        accum_0 = _mm256_setzero_ps();
        accum_1 = _mm256_setzero_ps();
        accum_2 = _mm256_setzero_ps();
        accum_3 = _mm256_setzero_ps();

        __m256i abs_mask = _mm256_set1_epi32(0x7fffffff);
        for (i = 0; i < tail_index_AVX2; i += 32) {
            accum_0 += (__m256)_mm256_and_si256(_mm256_load_si256((__m256i*)&x1[i+ 0]), abs_mask);
            accum_1 += (__m256)_mm256_and_si256(_mm256_load_si256((__m256i*)&x1[i+ 8]), abs_mask);
            accum_2 += (__m256)_mm256_and_si256(_mm256_load_si256((__m256i*)&x1[i+16]), abs_mask);
            accum_3 += (__m256)_mm256_and_si256(_mm256_load_si256((__m256i*)&x1[i+24]), abs_mask);
        }

        accum_0 = accum_0 + accum_1 + accum_2 + accum_3;
        __m128 half_accum0;
        half_accum0 = _mm_add_ps(_mm256_extractf128_ps(accum_0, 0), _mm256_extractf128_ps(accum_0, 1));

        half_accum0 = _mm_hadd_ps(half_accum0, half_accum0);
        half_accum0 = _mm_hadd_ps(half_accum0, half_accum0);

        sumf += half_accum0[0];
        
    }
    
    if (n >= 8) {
        __m128 accum_20, accum_21;
        accum_20 = _mm_setzero_ps();
        accum_21 = _mm_setzero_ps();

        __m128i abs_mask2 = _mm_set1_epi32(0x7fffffff);
        for (i = tail_index_AVX2; i < tail_index_SSE; i += 8) {
            accum_20 += (__m128)_mm_and_si128(_mm_loadu_si128((__m128i*)&x1[i + 0]), abs_mask2);
            accum_21 += (__m128)_mm_and_si128(_mm_loadu_si128((__m128i*)&x1[i + 4]), abs_mask2);
        }
        
        accum_20 += accum_21;
        accum_20 = _mm_hadd_ps(accum_20, accum_20);
        accum_20 = _mm_hadd_ps(accum_20, accum_20);
        
        sumf += accum_20[0];
    }

    for (i = tail_index_SSE; i < n; ++i) {
        sumf += ABS_K(x1[i]);
    }

    return sumf;
}
#endif
