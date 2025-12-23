/* need a new enough GCC for avx512 support */
#ifdef __NVCOMPILER
#define NVCOMPVERS ( __NVCOMPILER_MAJOR__ * 100 + __NVCOMPILER_MINOR__ )
#endif
#if ((( defined(__GNUC__)  && __GNUC__   > 6 && defined(__AVX512CD__)) || (defined(__clang__) && __clang_major__ >= 9)) || (defined(__NVCOMPILER) && NVCOMPVERS >= 2203))

#if (!(defined(__NVCOMPILER) && NVCOMPVERS < 2203))

#define HAVE_ZSUM_KERNEL 1

#include <immintrin.h>

#include <stdint.h>

static FLOAT zsum_kernel(BLASLONG n, FLOAT *x)
{
    FLOAT *x1 = x;
    FLOAT sumf=0.0;
    BLASLONG n2 = n + n;
    

    if (n2 < 32) {
        __m128d accum_10, accum_11, accum_12, accum_13;

        accum_10 = _mm_setzero_pd();
        accum_11 = _mm_setzero_pd();
        accum_12 = _mm_setzero_pd();
        accum_13 = _mm_setzero_pd();
        
        _mm_prefetch(&x1[0], _MM_HINT_T0);
        if (n2 >= 16){
            __m128d x00 = _mm_loadu_pd(&x1[ 0]);
            __m128d x01 = _mm_loadu_pd(&x1[ 2]);
            __m128d x02 = _mm_loadu_pd(&x1[ 4]);
            __m128d x03 = _mm_loadu_pd(&x1[ 6]);
            
            _mm_prefetch(&x1[8], _MM_HINT_T0);
            __m128d x04 = _mm_loadu_pd(&x1[ 8]);
            __m128d x05 = _mm_loadu_pd(&x1[10]);
            __m128d x06 = _mm_loadu_pd(&x1[12]);
            __m128d x07 = _mm_loadu_pd(&x1[14]);

            accum_10 = _mm_add_pd(accum_10, x00);
            accum_11 = _mm_add_pd(accum_11, x01);
            accum_12 = _mm_add_pd(accum_12, x02);
            accum_13 = _mm_add_pd(accum_13, x03);

            accum_10 = _mm_add_pd(accum_10, x04);
            accum_11 = _mm_add_pd(accum_11, x05);
            accum_12 = _mm_add_pd(accum_12, x06);
            accum_13 = _mm_add_pd(accum_13, x07);

            x1 += 16;
            n2 -= 16;
        }

        if (n2 >= 8) {
            __m128d x00 = _mm_loadu_pd(&x1[ 0]);
            __m128d x01 = _mm_loadu_pd(&x1[ 2]);
            __m128d x02 = _mm_loadu_pd(&x1[ 4]);
            __m128d x03 = _mm_loadu_pd(&x1[ 6]);

            accum_10 = _mm_add_pd(accum_10, x00);
            accum_11 = _mm_add_pd(accum_11, x01);
            accum_12 = _mm_add_pd(accum_12, x02);
            accum_13 = _mm_add_pd(accum_13, x03);
            
            n2 -= 8;
            x1 += 8;
        }

        if (n2 >= 4) {
            __m128d x00 = _mm_loadu_pd(&x1[ 0]);
            __m128d x01 = _mm_loadu_pd(&x1[ 2]);
            accum_10 = _mm_add_pd(accum_10, x00);
            accum_11 = _mm_add_pd(accum_11, x01);

            n2 -= 4;
            x1 += 4;
        }
        
        if (n2) {
            __m128d x00 = _mm_loadu_pd(&x1[ 0]);
            accum_10 = _mm_add_pd(accum_10, x00);
        }

        accum_10 = _mm_add_pd(accum_10, accum_11);
        accum_12 = _mm_add_pd(accum_12, accum_13);
        accum_10 = _mm_add_pd(accum_10, accum_12);

        accum_10 = _mm_hadd_pd(accum_10, accum_10);

        sumf = accum_10[0];
    }
    else {
        __m512d accum_0, accum_1, accum_2, accum_3;
        __m512d x00, x01, x02, x03, x04, x05, x06, x07;
        __m512d abs_mask = (__m512d)_mm512_set1_epi64(0x7fffffffffffffff);
        
        accum_0 = _mm512_setzero_pd();
        accum_1 = _mm512_setzero_pd();
        accum_2 = _mm512_setzero_pd();
        accum_3 = _mm512_setzero_pd();

        // alignment has side-effect when the size of input array is not large enough
        if (n2 < 128) {
            if (n2 >= 64) {
                x00 = _mm512_loadu_pd(&x1[ 0]);
                x01 = _mm512_loadu_pd(&x1[ 8]);
                x02 = _mm512_loadu_pd(&x1[16]);
                x03 = _mm512_loadu_pd(&x1[24]);
                x04 = _mm512_loadu_pd(&x1[32]);
                x05 = _mm512_loadu_pd(&x1[40]);
                x06 = _mm512_loadu_pd(&x1[48]);
                x07 = _mm512_loadu_pd(&x1[56]);

                accum_0 = _mm512_add_pd(accum_0, x00);
                accum_1 = _mm512_add_pd(accum_1, x01);
                accum_2 = _mm512_add_pd(accum_2, x02);
                accum_3 = _mm512_add_pd(accum_3, x03);
                
                accum_0 = _mm512_add_pd(accum_0, x04);
                accum_1 = _mm512_add_pd(accum_1, x05);
                accum_2 = _mm512_add_pd(accum_2, x06);
                accum_3 = _mm512_add_pd(accum_3, x07);
                
                n2 -= 64;
                x1 += 64;
            }

            if (n2 >= 32) {
                x00 = _mm512_loadu_pd(&x1[ 0]);
                x01 = _mm512_loadu_pd(&x1[ 8]);
                x02 = _mm512_loadu_pd(&x1[16]);
                x03 = _mm512_loadu_pd(&x1[24]);
                accum_0 = _mm512_add_pd(accum_0, x00);
                accum_1 = _mm512_add_pd(accum_1, x01);
                accum_2 = _mm512_add_pd(accum_2, x02);
                accum_3 = _mm512_add_pd(accum_3, x03);

                n2 -= 32;
                x1 += 32;
            }

            if (n2 >= 16) {
                x00 = _mm512_loadu_pd(&x1[ 0]);
                x01 = _mm512_loadu_pd(&x1[ 8]);
                accum_0 = _mm512_add_pd(accum_0, x00);
                accum_1 = _mm512_add_pd(accum_1, x01);

                n2 -= 16;
                x1 += 16;
            }

            if (n2 >= 8) {
                x00 = _mm512_loadu_pd(&x1[ 0]);
                accum_0 = _mm512_add_pd(accum_0, x00);

                n2 -= 8;
                x1 += 8;
            }

            if (n2) {
                unsigned char tail_mask8 = (((unsigned char) 0xff) >> (8 - n2));
                x00 = _mm512_maskz_loadu_pd(*((__mmask8*) &tail_mask8), &x1[ 0]);
                accum_0 = _mm512_add_pd(accum_0, x00);
            }
            accum_0 = _mm512_add_pd(accum_0, accum_1);
            accum_2 = _mm512_add_pd(accum_2, accum_3);
            accum_0 = _mm512_add_pd(accum_0, accum_2);
            sumf =  _mm512_reduce_add_pd(accum_0);
        }
        // n2 >= 128, doing alignment
        else {

            int align_header = ((64 - ((uintptr_t)x1 & (uintptr_t)0x3f)) >> 3) & 0x7;

            if (0 != align_header) {
                unsigned char align_mask8 = (((unsigned char)0xff) >> (8 - align_header));
                x00 = _mm512_maskz_loadu_pd(*((__mmask8*) &align_mask8), &x1[0]);
                accum_0 = _mm512_add_pd(accum_0, x00);

                n2 -= align_header;
                x1 += align_header;
            }

            x00 = _mm512_load_pd(&x1[ 0]);
            x01 = _mm512_load_pd(&x1[ 8]);
            x02 = _mm512_load_pd(&x1[16]);
            x03 = _mm512_load_pd(&x1[24]);
            x04 = _mm512_load_pd(&x1[32]);
            x05 = _mm512_load_pd(&x1[40]);
            x06 = _mm512_load_pd(&x1[48]);
            x07 = _mm512_load_pd(&x1[56]);
            
            n2 -= 64;
            x1 += 64;

            while (n2 >= 64) {
                accum_0 = _mm512_add_pd(accum_0, x00);
                x00 = _mm512_load_pd(&x1[ 0]);
                accum_1 = _mm512_add_pd(accum_1, x01);
                x01 = _mm512_load_pd(&x1[ 8]);
                accum_2 = _mm512_add_pd(accum_2, x02);
                x02 = _mm512_load_pd(&x1[16]);
                accum_3 = _mm512_add_pd(accum_3, x03);
                x03 = _mm512_load_pd(&x1[24]);
                
                accum_0 = _mm512_add_pd(accum_0, x04);
                x04 = _mm512_load_pd(&x1[32]);
                accum_1 = _mm512_add_pd(accum_1, x05);
                x05 = _mm512_load_pd(&x1[40]);
                accum_2 = _mm512_add_pd(accum_2, x06);
                x06 = _mm512_load_pd(&x1[48]);
                accum_3 = _mm512_add_pd(accum_3, x07);
                x07 = _mm512_load_pd(&x1[56]);

                n2 -= 64;
                x1 += 64;
            }
            
            accum_0 = _mm512_add_pd(accum_0, x00);
            accum_1 = _mm512_add_pd(accum_1, x01);
            accum_2 = _mm512_add_pd(accum_2, x02);
            accum_3 = _mm512_add_pd(accum_3, x03);
            
            accum_0 = _mm512_add_pd(accum_0, x04);
            accum_1 = _mm512_add_pd(accum_1, x05);
            accum_2 = _mm512_add_pd(accum_2, x06);
            accum_3 = _mm512_add_pd(accum_3, x07);

            if (n2 >= 32) {
                x00 = _mm512_load_pd(&x1[ 0]);
                x01 = _mm512_load_pd(&x1[ 8]);
                x02 = _mm512_load_pd(&x1[16]);
                x03 = _mm512_load_pd(&x1[24]);
                accum_0 = _mm512_add_pd(accum_0, x00);
                accum_1 = _mm512_add_pd(accum_1, x01);
                accum_2 = _mm512_add_pd(accum_2, x02);
                accum_3 = _mm512_add_pd(accum_3, x03);

                n2 -= 32;
                x1 += 32;
            }

            if (n2 >= 16) {
                x00 = _mm512_load_pd(&x1[ 0]);
                x01 = _mm512_load_pd(&x1[ 8]);
                accum_0 = _mm512_add_pd(accum_0, x00);
                accum_1 = _mm512_add_pd(accum_1, x01);

                n2 -= 16;
                x1 += 16;
            }

            if (n2 >= 8) {
                x00 = _mm512_load_pd(&x1[ 0]);
                accum_0 = _mm512_add_pd(accum_0, x00);

                n2 -= 8;
                x1 += 8;
            }

            if (n2) {
                unsigned char tail_mask8 = (((unsigned char) 0xff) >> (8 - n2));
                x00 = _mm512_maskz_load_pd(*((__mmask8*) &tail_mask8), &x1[ 0]);
                accum_0 = _mm512_add_pd(accum_0, x00);
            }

            accum_0 = _mm512_add_pd(accum_0, accum_1);
            accum_2 = _mm512_add_pd(accum_2, accum_3);
            accum_0 = _mm512_add_pd(accum_0, accum_2);
            sumf = _mm512_reduce_add_pd(accum_0);
        }
    }

    return sumf;
}
#endif
#endif
