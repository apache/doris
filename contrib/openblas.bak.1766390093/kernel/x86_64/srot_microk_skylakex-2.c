/* need a new enough GCC for avx512 support */
#ifdef __NVCOMPILER
#define NVCOMPVERS ( __NVCOMPILER_MAJOR__ * 100 + __NVCOMPILER_MINOR__ )
#endif
#if (( defined(__GNUC__)  && __GNUC__   > 6 && defined(__AVX512CD__)) || (defined(__clang__) && __clang_major__ >= 9)) || (defined(__NVCOMPILER) && NVCOMPVERS >= 2203 )

#define HAVE_SROT_KERNEL 1

#include <immintrin.h>
#include <stdint.h>

static void srot_kernel(BLASLONG n, FLOAT *x, FLOAT *y, FLOAT c, FLOAT s)
{
    BLASLONG i = 0;
    __m512 c_512, s_512;
    c_512 = _mm512_set1_ps(c);
    s_512 = _mm512_set1_ps(s);

    BLASLONG tail_index_16 = n&(~15);
    BLASLONG tail_index_64 = n&(~63);


    __m512 x0, x1, x2, x3;
    __m512 y0, y1, y2, y3;
    __m512 t0, t1, t2, t3;

    for (i = 0; i < tail_index_64; i += 64) {
        x0 = _mm512_loadu_ps(&x[i + 0]);
        x1 = _mm512_loadu_ps(&x[i +16]);
        x2 = _mm512_loadu_ps(&x[i +32]);
        x3 = _mm512_loadu_ps(&x[i +48]);
        y0 = _mm512_loadu_ps(&y[i + 0]);
        y1 = _mm512_loadu_ps(&y[i +16]);
        y2 = _mm512_loadu_ps(&y[i +32]);
        y3 = _mm512_loadu_ps(&y[i +48]);

        t0 = _mm512_mul_ps(s_512, y0);
        t1 = _mm512_mul_ps(s_512, y1);
        t2 = _mm512_mul_ps(s_512, y2);
        t3 = _mm512_mul_ps(s_512, y3);

        t0 = _mm512_fmadd_ps(c_512, x0, t0);
        t1 = _mm512_fmadd_ps(c_512, x1, t1);
        t2 = _mm512_fmadd_ps(c_512, x2, t2);
        t3 = _mm512_fmadd_ps(c_512, x3, t3);

        _mm512_storeu_ps(&x[i + 0], t0);
        _mm512_storeu_ps(&x[i +16], t1);
        _mm512_storeu_ps(&x[i +32], t2);
        _mm512_storeu_ps(&x[i +48], t3);

        t0 = _mm512_mul_ps(s_512, x0);
        t1 = _mm512_mul_ps(s_512, x1);
        t2 = _mm512_mul_ps(s_512, x2);
        t3 = _mm512_mul_ps(s_512, x3);

        t0 = _mm512_fmsub_ps(c_512, y0, t0);
        t1 = _mm512_fmsub_ps(c_512, y1, t1);
        t2 = _mm512_fmsub_ps(c_512, y2, t2);
        t3 = _mm512_fmsub_ps(c_512, y3, t3);

        _mm512_storeu_ps(&y[i + 0], t0);
        _mm512_storeu_ps(&y[i +16], t1);
        _mm512_storeu_ps(&y[i +32], t2);
        _mm512_storeu_ps(&y[i +48], t3);
    }

    for (i = tail_index_64; i < tail_index_16; i += 16) {
        x0 = _mm512_loadu_ps(&x[i]);
        y0 = _mm512_loadu_ps(&y[i]);

        t0 = _mm512_mul_ps(s_512, y0);
        t0 = _mm512_fmadd_ps(c_512, x0, t0);
        _mm512_storeu_ps(&x[i], t0);

        t0 = _mm512_mul_ps(s_512, x0);
        t0 = _mm512_fmsub_ps(c_512, y0, t0);
        _mm512_storeu_ps(&y[i], t0);
    }


    if ((n & 15) > 0) {
        uint16_t tail_mask16 = (((uint16_t) 0xffff) >> (16-(n&15)));
        __m512 tail_x = _mm512_maskz_loadu_ps(*((__mmask16*)&tail_mask16), &x[tail_index_16]);
        __m512 tail_y = _mm512_maskz_loadu_ps(*((__mmask16*)&tail_mask16), &y[tail_index_16]);
	    __m512 temp = _mm512_mul_ps(s_512, tail_y);
	    temp = _mm512_fmadd_ps(c_512, tail_x, temp);
	    _mm512_mask_storeu_ps(&x[tail_index_16], *((__mmask16*)&tail_mask16), temp);
	    temp = _mm512_mul_ps(s_512, tail_x);
	    temp = _mm512_fmsub_ps(c_512, tail_y, temp);
	    _mm512_mask_storeu_ps(&y[tail_index_16], *((__mmask16*)&tail_mask16), temp);	
    }
}
#endif
