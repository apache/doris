#include "common.h"

#if defined(SKYLAKEX)
#include "drot_microk_skylakex-2.c"
#elif defined(HASWELL) || defined(ZEN)
#include "drot_microk_haswell-2.c"
#endif

#ifndef HAVE_DROT_KERNEL
#include "../simd/intrin.h"

static void drot_kernel(BLASLONG n, FLOAT *x, FLOAT *y, FLOAT c, FLOAT s)
{
    BLASLONG i = 0;
#if V_SIMD_F64 && V_SIMD > 256
    const int vstep = v_nlanes_f64;
    const int unrollx4 = n & (-vstep * 4);
    const int unrollx = n & -vstep;

    v_f64 __c = v_setall_f64(c);
    v_f64 __s = v_setall_f64(s);
    v_f64 vx0, vx1, vx2, vx3;
    v_f64 vy0, vy1, vy2, vy3;
    v_f64 vt0, vt1, vt2, vt3;

    for (; i < unrollx4; i += vstep * 4) {
        vx0 = v_loadu_f64(x + i);
        vx1 = v_loadu_f64(x + i + vstep);
        vx2 = v_loadu_f64(x + i + vstep * 2);
        vx3 = v_loadu_f64(x + i + vstep * 3);
        vy0 = v_loadu_f64(y + i);
        vy1 = v_loadu_f64(y + i + vstep);
        vy2 = v_loadu_f64(y + i + vstep * 2);
        vy3 = v_loadu_f64(y + i + vstep * 3);

        vt0 = v_mul_f64(__s, vy0);
        vt1 = v_mul_f64(__s, vy1);
        vt2 = v_mul_f64(__s, vy2);
        vt3 = v_mul_f64(__s, vy3);

        vt0 = v_muladd_f64(__c, vx0, vt0);
        vt1 = v_muladd_f64(__c, vx1, vt1);
        vt2 = v_muladd_f64(__c, vx2, vt2);
        vt3 = v_muladd_f64(__c, vx3, vt3);

        v_storeu_f64(x + i, vt0);
        v_storeu_f64(x + i + vstep, vt1);
        v_storeu_f64(x + i + vstep * 2, vt2);
        v_storeu_f64(x + i + vstep * 3, vt3);

        vt0 = v_mul_f64(__s, vx0);
        vt1 = v_mul_f64(__s, vx1);
        vt2 = v_mul_f64(__s, vx2);
        vt3 = v_mul_f64(__s, vx3);

        vt0 = v_mulsub_f64(__c, vy0, vt0);
        vt1 = v_mulsub_f64(__c, vy1, vt1);
        vt2 = v_mulsub_f64(__c, vy2, vt2);
        vt3 = v_mulsub_f64(__c, vy3, vt3);

        v_storeu_f64(y + i, vt0);
        v_storeu_f64(y + i + vstep, vt1);
        v_storeu_f64(y + i + vstep * 2, vt2);
        v_storeu_f64(y + i + vstep * 3, vt3);
    }

    for (; i < unrollx; i += vstep) {
        vx0 = v_loadu_f64(x + i);
        vy0 = v_loadu_f64(y + i);

        vt0 = v_mul_f64(__s, vy0);
        vt0 = v_muladd_f64(__c, vx0, vt0);
        v_storeu_f64(x + i, vt0);

        vt0 = v_mul_f64(__s, vx0);
        vt0 = v_mulsub_f64(__c, vy0, vt0);
        v_storeu_f64(y + i, vt0);
    }
#else
    FLOAT f0, f1, f2, f3;
    FLOAT x0, x1, x2, x3;
    FLOAT g0, g1, g2, g3;
    FLOAT y0, y1, y2, y3;

    FLOAT* xp = x;
    FLOAT* yp = y;

    BLASLONG n1 = n & (~7);

    while (i < n1) {
        x0 = xp[0];
        y0 = yp[0];
        x1 = xp[1];
        y1 = yp[1];
        x2 = xp[2];
        y2 = yp[2];
        x3 = xp[3];
        y3 = yp[3];

        f0 = c*x0 + s*y0;
        g0 = c*y0 - s*x0;
        f1 = c*x1 + s*y1;
        g1 = c*y1 - s*x1;
        f2 = c*x2 + s*y2;
        g2 = c*y2 - s*x2;
        f3 = c*x3 + s*y3;
        g3 = c*y3 - s*x3;

        xp[0] = f0;
        yp[0] = g0;
        xp[1] = f1;
        yp[1] = g1;
        xp[2] = f2;
        yp[2] = g2;
        xp[3] = f3;
        yp[3] = g3;

        xp += 4;
        yp += 4;
        i += 4;
    }
#endif
    while (i < n) {
        FLOAT temp = c*x[i] + s*y[i];
        y[i] = c*y[i] - s*x[i];
        x[i] = temp;

        i++;
    }
}

#endif
static void rot_compute(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT c, FLOAT s)
{
    BLASLONG i = 0;
    BLASLONG ix = 0, iy = 0;

    FLOAT temp;
    
    if (n <= 0)
        return;
    if ((inc_x == 1) && (inc_y == 1)) {
            drot_kernel(n, x, y, c, s);
    }
    else {
        while (i < n) {
            temp = c * x[ix] + s * y[iy];
            y[iy] = c * y[iy] - s * x[ix];
            x[ix] = temp;

            ix += inc_x;
            iy += inc_y;
            i++;
        }
    }
    return;
}


#if defined(SMP)
static int rot_thread_function(blas_arg_t *args)
{

    rot_compute(args->m, 
            args->a, args->lda, 
            args->b, args->ldb, 
            ((FLOAT *)args->alpha)[0], 
            ((FLOAT *)args->alpha)[1]);
    return 0;
}

extern int blas_level1_thread(int mode, BLASLONG m, BLASLONG n, BLASLONG k, void *alpha, void *a, BLASLONG lda, void *b, BLASLONG ldb, void *c, BLASLONG ldc, int (*function)(void), int nthreads);
#endif
int CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT c, FLOAT s)
{
#if defined(SMP)
    int nthreads;
    FLOAT alpha[2]={c, s};
    FLOAT dummy_c;
#endif

#if defined(SMP)
    if (inc_x == 0 || inc_y == 0 || n <= 100000) {
        nthreads = 1;
    }
    else {
        nthreads = num_cpu_avail(1);
    }

    if (nthreads == 1) {
        rot_compute(n, x, inc_x, y, inc_y, c, s);
    }
    else {
#if defined(DOUBLE)
	    int mode = BLAS_DOUBLE | BLAS_REAL | BLAS_PTHREAD;
#else
	    int mode = BLAS_SINGLE | BLAS_REAL | BLAS_PTHREAD;
#endif
	    blas_level1_thread(mode, n, 0, 0, alpha, x, inc_x, y, inc_y, &dummy_c, 0, (int (*)(void))rot_thread_function, nthreads);
    }
#else	
    rot_compute(n, x, inc_x, y, inc_y, c, s);
#endif
    return 0;
}
