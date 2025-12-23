#if defined(HAVE_FMA3)  && defined(HAVE_AVX2)

#define HAVE_SROT_KERNEL 1

#include <immintrin.h>
#include <stdint.h>

static void srot_kernel(BLASLONG n, FLOAT *x, FLOAT *y, FLOAT c, FLOAT s)
{
    BLASLONG i = 0;

    BLASLONG tail_index_8 = n&(~7);
    BLASLONG tail_index_32 = n&(~31);

    __m256 c_256, s_256;
    if (n >= 8) {
        c_256 = _mm256_set1_ps(c);
        s_256 = _mm256_set1_ps(s);
    }

    __m256 x0, x1, x2, x3;
    __m256 y0, y1, y2, y3;
    __m256 t0, t1, t2, t3;

    for (i = 0; i < tail_index_32; i += 32) {
        x0 = _mm256_loadu_ps(&x[i + 0]);
        x1 = _mm256_loadu_ps(&x[i + 8]);
        x2 = _mm256_loadu_ps(&x[i +16]);
        x3 = _mm256_loadu_ps(&x[i +24]);
        y0 = _mm256_loadu_ps(&y[i + 0]);
        y1 = _mm256_loadu_ps(&y[i + 8]);
        y2 = _mm256_loadu_ps(&y[i +16]);
        y3 = _mm256_loadu_ps(&y[i +24]);

        t0 = _mm256_mul_ps(s_256, y0);
        t1 = _mm256_mul_ps(s_256, y1);
        t2 = _mm256_mul_ps(s_256, y2);
        t3 = _mm256_mul_ps(s_256, y3);

        t0 = _mm256_fmadd_ps(c_256, x0, t0);
        t1 = _mm256_fmadd_ps(c_256, x1, t1);
        t2 = _mm256_fmadd_ps(c_256, x2, t2);
        t3 = _mm256_fmadd_ps(c_256, x3, t3);

        _mm256_storeu_ps(&x[i + 0], t0);
        _mm256_storeu_ps(&x[i + 8], t1);
        _mm256_storeu_ps(&x[i +16], t2);
        _mm256_storeu_ps(&x[i +24], t3);

        t0 = _mm256_mul_ps(s_256, x0);
        t1 = _mm256_mul_ps(s_256, x1);
        t2 = _mm256_mul_ps(s_256, x2);
        t3 = _mm256_mul_ps(s_256, x3);

        t0 = _mm256_fmsub_ps(c_256, y0, t0);
        t1 = _mm256_fmsub_ps(c_256, y1, t1);
        t2 = _mm256_fmsub_ps(c_256, y2, t2);
        t3 = _mm256_fmsub_ps(c_256, y3, t3);

        _mm256_storeu_ps(&y[i + 0], t0);
        _mm256_storeu_ps(&y[i + 8], t1);
        _mm256_storeu_ps(&y[i +16], t2);
        _mm256_storeu_ps(&y[i +24], t3);

    }

    for (i = tail_index_32; i < tail_index_8; i += 8) {
        x0 = _mm256_loadu_ps(&x[i]);
        y0 = _mm256_loadu_ps(&y[i]);

        t0 = _mm256_mul_ps(s_256, y0);
        t0 = _mm256_fmadd_ps(c_256, x0, t0);
        _mm256_storeu_ps(&x[i], t0);

        t0 = _mm256_mul_ps(s_256, x0);
        t0 = _mm256_fmsub_ps(c_256, y0, t0);
        _mm256_storeu_ps(&y[i], t0);
    }

    for (i = tail_index_8; i < n; ++i) {
        FLOAT temp = c * x[i] + s * y[i];
        y[i] = c * y[i] - s * x[i];
        x[i] = temp;
    }
}
#endif
