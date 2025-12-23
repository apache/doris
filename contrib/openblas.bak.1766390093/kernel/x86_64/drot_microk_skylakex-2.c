/* need a new enough GCC for avx512 support */
#ifdef __NVCOMPILER
#define NVCOMPVERS ( __NVCOMPILER_MAJOR__ * 100 + __NVCOMPILER_MINOR__ )
#endif
#if (( defined(__GNUC__)  && __GNUC__   > 6 && defined(__AVX512CD__)) || (defined(__clang__) && __clang_major__ >= 9)) || (defined(__NVCOMPILER) && NVCOMPVERS >= 2203 )

#define HAVE_DROT_KERNEL 1

#include <immintrin.h>
#include <stdint.h>

static void drot_kernel(BLASLONG n, FLOAT *x, FLOAT *y, FLOAT c, FLOAT s)
{
    BLASLONG i = 0;
    BLASLONG n1 = n;
    
    BLASLONG tail_index_8 = 0;
    BLASLONG tail_index_32 = 0;

    __m512d c_512 = _mm512_set1_pd(c);
    __m512d s_512 = _mm512_set1_pd(s);

    tail_index_8 = n1 & (~7);
    tail_index_32 = n1 & (~31);


    __m512d x0, x1, x2, x3;
    __m512d y0, y1, y2, y3;
    __m512d t0, t1, t2, t3;

    for (i = 0; i < tail_index_32; i += 32) {
        x0 = _mm512_loadu_pd(&x[i + 0]);
        x1 = _mm512_loadu_pd(&x[i + 8]);
        x2 = _mm512_loadu_pd(&x[i +16]);
        x3 = _mm512_loadu_pd(&x[i +24]);
        y0 = _mm512_loadu_pd(&y[i + 0]);
        y1 = _mm512_loadu_pd(&y[i + 8]);
        y2 = _mm512_loadu_pd(&y[i +16]);
        y3 = _mm512_loadu_pd(&y[i +24]);

        t0 = _mm512_mul_pd(s_512, y0);
        t1 = _mm512_mul_pd(s_512, y1);
        t2 = _mm512_mul_pd(s_512, y2);
        t3 = _mm512_mul_pd(s_512, y3);

        t0 = _mm512_fmadd_pd(c_512, x0, t0);
        t1 = _mm512_fmadd_pd(c_512, x1, t1);
        t2 = _mm512_fmadd_pd(c_512, x2, t2);
        t3 = _mm512_fmadd_pd(c_512, x3, t3);

        _mm512_storeu_pd(&x[i + 0], t0);
        _mm512_storeu_pd(&x[i + 8], t1);
        _mm512_storeu_pd(&x[i +16], t2);
        _mm512_storeu_pd(&x[i +24], t3);

        t0 = _mm512_mul_pd(s_512, x0);
        t1 = _mm512_mul_pd(s_512, x1);
        t2 = _mm512_mul_pd(s_512, x2);
        t3 = _mm512_mul_pd(s_512, x3);

        t0 = _mm512_fmsub_pd(c_512, y0, t0);
        t1 = _mm512_fmsub_pd(c_512, y1, t1);
        t2 = _mm512_fmsub_pd(c_512, y2, t2);
        t3 = _mm512_fmsub_pd(c_512, y3, t3);

        _mm512_storeu_pd(&y[i + 0], t0);
        _mm512_storeu_pd(&y[i + 8], t1);
        _mm512_storeu_pd(&y[i +16], t2);
        _mm512_storeu_pd(&y[i +24], t3);
    }

    for (i = tail_index_32; i < tail_index_8; i += 8) {
        x0 = _mm512_loadu_pd(&x[i]);
        y0 = _mm512_loadu_pd(&y[i]);

        t0 = _mm512_mul_pd(s_512, y0);
        t0 = _mm512_fmadd_pd(c_512, x0, t0);
        _mm512_storeu_pd(&x[i], t0);

        t0 = _mm512_mul_pd(s_512, x0);
        t0 = _mm512_fmsub_pd(c_512, y0, t0);
        _mm512_storeu_pd(&y[i], t0);
    }

    if ((n1&7) > 0) {
        unsigned char tail_mask8 = (((unsigned char) 0xff) >> (8 -(n1&7)));
	__m512d tail_x = _mm512_maskz_loadu_pd(*((__mmask8*) &tail_mask8), &x[tail_index_8]);
	__m512d tail_y = _mm512_maskz_loadu_pd(*((__mmask8*) &tail_mask8), &y[tail_index_8]);
	__m512d temp = _mm512_mul_pd(s_512, tail_y);
	temp = _mm512_fmadd_pd(c_512, tail_x, temp);
	_mm512_mask_storeu_pd(&x[tail_index_8],*((__mmask8*)&tail_mask8), temp);
        temp = _mm512_mul_pd(s_512, tail_x);
        temp = _mm512_fmsub_pd(c_512, tail_y, temp);
        _mm512_mask_storeu_pd(&y[tail_index_8], *((__mmask8*)&tail_mask8), temp);	
    }
}
#endif
