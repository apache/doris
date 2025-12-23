#if defined(HAVE_FMA3)  && defined(HAVE_AVX2)
#define HAVE_DROT_KERNEL 1

#include <immintrin.h>
#include <stdint.h>

static void drot_kernel(BLASLONG n, FLOAT *x, FLOAT *y, FLOAT c, FLOAT s)
{
    BLASLONG i = 0;

    BLASLONG tail_index_4 = n&(~3);
    BLASLONG tail_index_16 = n&(~15);

    __m256d c_256, s_256;
    if (n >= 4) {
        c_256 = _mm256_set1_pd(c);
        s_256 = _mm256_set1_pd(s);
    }

    __m256d x0, x1, x2, x3;
    __m256d y0, y1, y2, y3;
    __m256d t0, t1, t2, t3;

    for (i = 0; i < tail_index_16; i += 16) {
        x0 = _mm256_loadu_pd(&x[i + 0]);
        x1 = _mm256_loadu_pd(&x[i + 4]);
        x2 = _mm256_loadu_pd(&x[i + 8]);
        x3 = _mm256_loadu_pd(&x[i +12]);
        y0 = _mm256_loadu_pd(&y[i + 0]);
        y1 = _mm256_loadu_pd(&y[i + 4]);
        y2 = _mm256_loadu_pd(&y[i + 8]);
        y3 = _mm256_loadu_pd(&y[i +12]);

        t0 = _mm256_mul_pd(s_256, y0);
        t1 = _mm256_mul_pd(s_256, y1);
        t2 = _mm256_mul_pd(s_256, y2);
        t3 = _mm256_mul_pd(s_256, y3);

        t0 = _mm256_fmadd_pd(c_256, x0, t0);
        t1 = _mm256_fmadd_pd(c_256, x1, t1);
        t2 = _mm256_fmadd_pd(c_256, x2, t2);
        t3 = _mm256_fmadd_pd(c_256, x3, t3);

        _mm256_storeu_pd(&x[i + 0], t0);
        _mm256_storeu_pd(&x[i + 4], t1);
        _mm256_storeu_pd(&x[i + 8], t2);
        _mm256_storeu_pd(&x[i +12], t3);

        t0 = _mm256_mul_pd(s_256, x0);
        t1 = _mm256_mul_pd(s_256, x1);
        t2 = _mm256_mul_pd(s_256, x2);
        t3 = _mm256_mul_pd(s_256, x3);

        t0 = _mm256_fmsub_pd(c_256, y0, t0);
        t1 = _mm256_fmsub_pd(c_256, y1, t1);
        t2 = _mm256_fmsub_pd(c_256, y2, t2);
        t3 = _mm256_fmsub_pd(c_256, y3, t3);

        _mm256_storeu_pd(&y[i + 0], t0);
        _mm256_storeu_pd(&y[i + 4], t1);
        _mm256_storeu_pd(&y[i + 8], t2);
        _mm256_storeu_pd(&y[i +12], t3);

    }

    for (i = tail_index_16; i < tail_index_4; i += 4) {
        x0 = _mm256_loadu_pd(&x[i]);
        y0 = _mm256_loadu_pd(&y[i]);

        t0 = _mm256_mul_pd(s_256, y0);
        t0 = _mm256_fmadd_pd(c_256, x0, t0);
        _mm256_storeu_pd(&x[i], t0);
        
        t0 = _mm256_mul_pd(s_256, x0);
        t0 = _mm256_fmsub_pd(c_256, y0, t0);
        _mm256_storeu_pd(&y[i], t0);
    }

    for (i = tail_index_4; i < n; ++i) {
        FLOAT temp = c * x[i] + s * y[i];
        y[i] = c * y[i] - s * x[i];
        x[i] = temp;
    }
}
#endif
