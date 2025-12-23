/* need a new enough GCC for avx512 support */
#ifdef __NVCOMPILER
#define NVCOMPVERS ( __NVCOMPILER_MAJOR__ * 100 + __NVCOMPILER_MINOR__ )
#endif
#if ((( defined(__GNUC__)  && __GNUC__   > 6 && defined(__AVX512CD__)) || (defined(__clang__) && __clang_major__ >= 9)) || (defined(__NVCOMPILER) && NVCOMPVERS >= 2203))

#if (!(defined(__NVCOMPILER) && NVCOMPVERS < 2203))

#define HAVE_CASUM_KERNEL 1

#include <immintrin.h>

#include <stdint.h>

static FLOAT casum_kernel(BLASLONG n, FLOAT *x)
{
    FLOAT *x1 = x;
    FLOAT sumf=0.0;
    BLASLONG n2 = n + n;
    
    if (n2 < 64) {
        __m128 accum_10, accum_11, accum_12, accum_13;
        __m128 abs_mask1;

        accum_10 = _mm_setzero_ps();
        accum_11 = _mm_setzero_ps();
        accum_12 = _mm_setzero_ps();
        accum_13 = _mm_setzero_ps();
        
        abs_mask1 = (__m128)_mm_set1_epi32(0x7fffffff);
                
        _mm_prefetch(&x1[0], _MM_HINT_T0);
        
        if (n2 >= 32){
            __m128 x00 = _mm_loadu_ps(&x1[ 0]);
            __m128 x01 = _mm_loadu_ps(&x1[ 4]);
            __m128 x02 = _mm_loadu_ps(&x1[ 8]);
            __m128 x03 = _mm_loadu_ps(&x1[12]);
            
            _mm_prefetch(&x1[16], _MM_HINT_T0);
            __m128 x04 = _mm_loadu_ps(&x1[16]);
            __m128 x05 = _mm_loadu_ps(&x1[20]);
            __m128 x06 = _mm_loadu_ps(&x1[24]);
            __m128 x07 = _mm_loadu_ps(&x1[28]);

            x00 = _mm_and_ps(x00, abs_mask1);
            x01 = _mm_and_ps(x01, abs_mask1);
            x02 = _mm_and_ps(x02, abs_mask1);
            x03 = _mm_and_ps(x03, abs_mask1);
            
            accum_10 = _mm_add_ps(accum_10, x00);
            accum_11 = _mm_add_ps(accum_11, x01);
            accum_12 = _mm_add_ps(accum_12, x02);
            accum_13 = _mm_add_ps(accum_13, x03);

            x04 = _mm_and_ps(x04, abs_mask1);
            x05 = _mm_and_ps(x05, abs_mask1);
            x06 = _mm_and_ps(x06, abs_mask1);
            x07 = _mm_and_ps(x07, abs_mask1);
            
            accum_10 = _mm_add_ps(accum_10, x04);
            accum_11 = _mm_add_ps(accum_11, x05);
            accum_12 = _mm_add_ps(accum_12, x06);
            accum_13 = _mm_add_ps(accum_13, x07);

            n2 -= 32;
            x1 += 32;
        }

        if (n2 >= 16) {
            __m128 x00 = _mm_loadu_ps(&x1[ 0]);
            __m128 x01 = _mm_loadu_ps(&x1[ 4]);
            __m128 x02 = _mm_loadu_ps(&x1[ 8]);
            __m128 x03 = _mm_loadu_ps(&x1[12]);

            x00 = _mm_and_ps(x00, abs_mask1);
            x01 = _mm_and_ps(x01, abs_mask1);
            x02 = _mm_and_ps(x02, abs_mask1);
            x03 = _mm_and_ps(x03, abs_mask1);
            accum_10 = _mm_add_ps(accum_10, x00);
            accum_11 = _mm_add_ps(accum_11, x01);
            accum_12 = _mm_add_ps(accum_12, x02);
            accum_13 = _mm_add_ps(accum_13, x03);
            
            n2 -= 16;
            x1 += 16;
        }

        if (n2 >= 8) {
            __m128 x00 = _mm_loadu_ps(&x1[ 0]);
            __m128 x01 = _mm_loadu_ps(&x1[ 4]);
            x00 = _mm_and_ps(x00, abs_mask1);
            x01 = _mm_and_ps(x01, abs_mask1);
            accum_10 = _mm_add_ps(accum_10, x00);
            accum_11 = _mm_add_ps(accum_11, x01);

            n2 -= 8;
            x1 += 8;
        }
        
        if (n2 >= 4) {
            __m128 x00 = _mm_loadu_ps(&x1[ 0]);
            x00 = _mm_and_ps(x00, abs_mask1);
            accum_10 = _mm_add_ps(accum_10, x00);

            n2 -= 4;
            x1 += 4;
        }

        if (n2) {
            sumf += (ABS_K(x1[0]) + ABS_K(x1[1]));
        }

        accum_10 = _mm_add_ps(accum_10, accum_11);
        accum_12 = _mm_add_ps(accum_12, accum_13);
        accum_10 = _mm_add_ps(accum_10, accum_12);

        accum_10 = _mm_hadd_ps(accum_10, accum_10);
        accum_10 = _mm_hadd_ps(accum_10, accum_10);

        sumf += accum_10[0];
    }
    else {
        __m512 accum_0, accum_1, accum_2, accum_3;
        __m512 x00, x01, x02, x03, x04, x05, x06, x07;
        __m512 abs_mask = (__m512)_mm512_set1_epi32(0x7fffffff);
        
        accum_0 = _mm512_setzero_ps();
        accum_1 = _mm512_setzero_ps();
        accum_2 = _mm512_setzero_ps();
        accum_3 = _mm512_setzero_ps();

        // alignment has side-effect when the size of input array is not large enough
        if (n2 < 256) {
            if (n2 >= 128) {
                x00 = _mm512_loadu_ps(&x1[  0]);
                x01 = _mm512_loadu_ps(&x1[ 16]);
                x02 = _mm512_loadu_ps(&x1[ 32]);
                x03 = _mm512_loadu_ps(&x1[ 48]);
                x04 = _mm512_loadu_ps(&x1[ 64]);
                x05 = _mm512_loadu_ps(&x1[ 80]);
                x06 = _mm512_loadu_ps(&x1[ 96]);
                x07 = _mm512_loadu_ps(&x1[112]);

                x00 = _mm512_and_ps(x00, abs_mask);
                x01 = _mm512_and_ps(x01, abs_mask);
                x02 = _mm512_and_ps(x02, abs_mask);
                x03 = _mm512_and_ps(x03, abs_mask);
                
                accum_0 = _mm512_add_ps(accum_0, x00);
                accum_1 = _mm512_add_ps(accum_1, x01);
                accum_2 = _mm512_add_ps(accum_2, x02);
                accum_3 = _mm512_add_ps(accum_3, x03);
                
                x04 = _mm512_and_ps(x04, abs_mask);
                x05 = _mm512_and_ps(x05, abs_mask);
                x06 = _mm512_and_ps(x06, abs_mask);
                x07 = _mm512_and_ps(x07, abs_mask);
                
                accum_0 = _mm512_add_ps(accum_0, x04);
                accum_1 = _mm512_add_ps(accum_1, x05);
                accum_2 = _mm512_add_ps(accum_2, x06);
                accum_3 = _mm512_add_ps(accum_3, x07);
                
                n2 -= 128;
                x1 += 128;
            }

            if (n2 >= 64) {
                x00 = _mm512_loadu_ps(&x1[ 0]);
                x01 = _mm512_loadu_ps(&x1[16]);
                x02 = _mm512_loadu_ps(&x1[32]);
                x03 = _mm512_loadu_ps(&x1[48]);
                x00 = _mm512_and_ps(x00, abs_mask);
                x01 = _mm512_and_ps(x01, abs_mask);
                x02 = _mm512_and_ps(x02, abs_mask);
                x03 = _mm512_and_ps(x03, abs_mask);
                accum_0 = _mm512_add_ps(accum_0, x00);
                accum_1 = _mm512_add_ps(accum_1, x01);
                accum_2 = _mm512_add_ps(accum_2, x02);
                accum_3 = _mm512_add_ps(accum_3, x03);

                n2 -= 64;
                x1 += 64;
            }

            if (n2 >= 32) {
                x00 = _mm512_loadu_ps(&x1[ 0]);
                x01 = _mm512_loadu_ps(&x1[16]);
                x00 = _mm512_and_ps(x00, abs_mask);
                x01 = _mm512_and_ps(x01, abs_mask);
                accum_0 = _mm512_add_ps(accum_0, x00);
                accum_1 = _mm512_add_ps(accum_1, x01);

                n2 -= 32;
                x1 += 32;
            }

            if (n2 >= 16) {
                x00 = _mm512_loadu_ps(&x1[ 0]);
                x00 = _mm512_and_ps(x00, abs_mask);
                accum_0 = _mm512_add_ps(accum_0, x00);

                n2 -= 16;
                x1 += 16;
            }

            if (n2) {
                uint16_t tail_mask16 = (((uint16_t) 0xffff) >> (16 - n2));
                x00 = _mm512_maskz_loadu_ps(*((__mmask16*) &tail_mask16), &x1[ 0]);
                x00 = _mm512_and_ps(x00, abs_mask);
                accum_0 = _mm512_add_ps(accum_0, x00);
            }
            accum_0 = _mm512_add_ps(accum_0, accum_1);
            accum_2 = _mm512_add_ps(accum_2, accum_3);
            accum_0 = _mm512_add_ps(accum_0, accum_2);

            sumf =  _mm512_reduce_add_ps(accum_0);
        }
        // n2 >= 256, doing alignment
        else {

            int align_header = ((64 - ((uintptr_t)x1 & (uintptr_t)0x3f)) >> 2) & 0xf;

            if (0 != align_header) {
                uint16_t align_mask16 = (((uint16_t)0xffff) >> (16 - align_header));
                x00 = _mm512_maskz_loadu_ps(*((__mmask16*) &align_mask16), &x1[0]);
                x00 = _mm512_and_ps(x00, abs_mask);
                accum_0 = _mm512_add_ps(accum_0, x00);

                n2 -= align_header;
                x1 += align_header;
            }

            x00 = _mm512_load_ps(&x1[  0]);
            x01 = _mm512_load_ps(&x1[ 16]);
            x02 = _mm512_load_ps(&x1[ 32]);
            x03 = _mm512_load_ps(&x1[ 48]);
            x04 = _mm512_load_ps(&x1[ 64]);
            x05 = _mm512_load_ps(&x1[ 80]);
            x06 = _mm512_load_ps(&x1[ 96]);
            x07 = _mm512_load_ps(&x1[112]);
            
            n2 -= 128;
            x1 += 128;

            while (n2 >= 128) {
                x00 = _mm512_and_ps(x00, abs_mask);
                x01 = _mm512_and_ps(x01, abs_mask);
                x02 = _mm512_and_ps(x02, abs_mask);
                x03 = _mm512_and_ps(x03, abs_mask);
                
                accum_0 = _mm512_add_ps(accum_0, x00);
                x00 = _mm512_load_ps(&x1[  0]);
                accum_1 = _mm512_add_ps(accum_1, x01);
                x01 = _mm512_load_ps(&x1[ 16]);
                accum_2 = _mm512_add_ps(accum_2, x02);
                x02 = _mm512_load_ps(&x1[ 32]);
                accum_3 = _mm512_add_ps(accum_3, x03);
                x03 = _mm512_load_ps(&x1[ 48]);
                
                x04 = _mm512_and_ps(x04, abs_mask);
                x05 = _mm512_and_ps(x05, abs_mask);
                x06 = _mm512_and_ps(x06, abs_mask);
                x07 = _mm512_and_ps(x07, abs_mask);
                accum_0 = _mm512_add_ps(accum_0, x04);
                x04 = _mm512_load_ps(&x1[ 64]);
                accum_1 = _mm512_add_ps(accum_1, x05);
                x05 = _mm512_load_ps(&x1[ 80]);
                accum_2 = _mm512_add_ps(accum_2, x06);
                x06 = _mm512_load_ps(&x1[ 96]);
                accum_3 = _mm512_add_ps(accum_3, x07);
                x07 = _mm512_load_ps(&x1[112]);

                n2 -= 128;
                x1 += 128;
            }
            x00 = _mm512_and_ps(x00, abs_mask);
            x01 = _mm512_and_ps(x01, abs_mask);
            x02 = _mm512_and_ps(x02, abs_mask);
            x03 = _mm512_and_ps(x03, abs_mask);
            
            accum_0 = _mm512_add_ps(accum_0, x00);
            accum_1 = _mm512_add_ps(accum_1, x01);
            accum_2 = _mm512_add_ps(accum_2, x02);
            accum_3 = _mm512_add_ps(accum_3, x03);
            
            x04 = _mm512_and_ps(x04, abs_mask);
            x05 = _mm512_and_ps(x05, abs_mask);
            x06 = _mm512_and_ps(x06, abs_mask);
            x07 = _mm512_and_ps(x07, abs_mask);
            
            accum_0 = _mm512_add_ps(accum_0, x04);
            accum_1 = _mm512_add_ps(accum_1, x05);
            accum_2 = _mm512_add_ps(accum_2, x06);
            accum_3 = _mm512_add_ps(accum_3, x07);

            if (n2 >= 64) {
                x00 = _mm512_load_ps(&x1[ 0]);
                x01 = _mm512_load_ps(&x1[16]);
                x02 = _mm512_load_ps(&x1[32]);
                x03 = _mm512_load_ps(&x1[48]);
                x00 = _mm512_and_ps(x00, abs_mask);
                x01 = _mm512_and_ps(x01, abs_mask);
                x02 = _mm512_and_ps(x02, abs_mask);
                x03 = _mm512_and_ps(x03, abs_mask);
                accum_0 = _mm512_add_ps(accum_0, x00);
                accum_1 = _mm512_add_ps(accum_1, x01);
                accum_2 = _mm512_add_ps(accum_2, x02);
                accum_3 = _mm512_add_ps(accum_3, x03);

                n2 -= 64;
                x1 += 64;
            }

            if (n2 >= 32) {
                x00 = _mm512_load_ps(&x1[ 0]);
                x01 = _mm512_load_ps(&x1[16]);
                x00 = _mm512_and_ps(x00, abs_mask);
                x01 = _mm512_and_ps(x01, abs_mask);
                accum_0 = _mm512_add_ps(accum_0, x00);
                accum_1 = _mm512_add_ps(accum_1, x01);

                n2 -= 32;
                x1 += 32;
            }

            if (n2 >= 16) {
                x00 = _mm512_load_ps(&x1[ 0]);
                x00 = _mm512_and_ps(x00, abs_mask);
                accum_0 = _mm512_add_ps(accum_0, x00);

                n2 -= 16;
                x1 += 16;
            }

            if (n2) {
                uint16_t tail_mask16 = (((uint16_t) 0xffff) >> (16 - n2));
                x00 = _mm512_maskz_load_ps(*((__mmask16*) &tail_mask16), &x1[ 0]);
                x00 = _mm512_and_ps(x00, abs_mask);
                accum_0 = _mm512_add_ps(accum_0, x00);
            }

            accum_0 = _mm512_add_ps(accum_0, accum_1);
            accum_2 = _mm512_add_ps(accum_2, accum_3);
            accum_0 = _mm512_add_ps(accum_0, accum_2);
            sumf = _mm512_reduce_add_ps(accum_0);
        }
    }

    return sumf;
}
#endif
#endif
