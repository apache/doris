#include "common.h"

#ifndef ABS_K
#define ABS_K(a) ((a) > 0 ? (a) : (-(a)))
#endif

#if defined(SKYLAKEX) || defined(COOPERLAKE) || defined(SAPPHIRERAPIDS)
#include "zasum_microk_skylakex-2.c"
#endif

#ifndef HAVE_ZASUM_KERNEL
static FLOAT zasum_kernel(BLASLONG n, FLOAT *x)
{

    BLASLONG i=0;
    BLASLONG n_8 = n & -8;
    FLOAT *x1 = x;
    FLOAT temp0, temp1, temp2, temp3;
    FLOAT temp4, temp5, temp6, temp7;
    FLOAT sum0 = 0.0;
    FLOAT sum1 = 0.0;
    FLOAT sum2 = 0.0;
    FLOAT sum3 = 0.0;
    FLOAT sum4 = 0.0;
    
    while (i < n_8) {
        temp0 = ABS_K(x1[0]);
        temp1 = ABS_K(x1[1]);
        temp2 = ABS_K(x1[2]);
        temp3 = ABS_K(x1[3]);
        temp4 = ABS_K(x1[4]);
        temp5 = ABS_K(x1[5]);
        temp6 = ABS_K(x1[6]);
        temp7 = ABS_K(x1[7]);
        
        sum0 += temp0;
        sum1 += temp1;
        sum2 += temp2;
        sum3 += temp3;
        
        sum0 += temp4;
        sum1 += temp5;
        sum2 += temp6;
        sum3 += temp7;
        
        x1+=8;
        i+=4;
    }

     while (i < n) {
        sum4 += ABS_K(x1[0]) + ABS_K(x1[1]);
        x1 += 2;
        i++;
     }

    return sum0+sum1+sum2+sum3+sum4;
}

#endif

static FLOAT asum_compute(BLASLONG n, FLOAT *x, BLASLONG inc_x)
{
    BLASLONG i = 0;
    BLASLONG ip = 0;
    BLASLONG inc_x2;
    FLOAT sumf = 0.0;

    if (n <= 0 || inc_x <= 0) return(sumf);
    if (inc_x == 1) {
        sumf = zasum_kernel(n, x);
    }
    else {
        inc_x2 = 2 * inc_x;

        while (i < n) {
            sumf += ABS_K(x[ip]) + ABS_K(x[ip + 1]);
            ip += inc_x2;
            i++;
        }
    }

    return(sumf);
}

#if defined(SMP)
static int asum_thread_function(BLASLONG n, 
        BLASLONG dummy0, BLASLONG dummy1, FLOAT dummy2,
        FLOAT *x, BLASLONG inc_x,
        FLOAT * dummy3, BLASLONG dummy4,
        FLOAT * result, BLASLONG dummy5)
{
    *(FLOAT *) result = asum_compute(n, x, inc_x);
    return 0;
}

extern int blas_level1_thread_with_return_value(int mode, 
        BLASLONG m, BLASLONG n, BLASLONG k, void * alpha,
        void *a, BLASLONG lda, 
        void *b, BLASLONG ldb,
        void *c, BLASLONG ldc,
        int (*function)(),
        int nthread);
#endif

FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x)
{
#if defined(SMP)
    int nthreads;
    FLOAT dummy_alpha[2];
#endif
    FLOAT sumf = 0.0;

#if defined(SMP)
    int num_cpu = num_cpu_avail(1);
    if (n <= 10000 || inc_x <= 0)
        nthreads = 1;
    else
        nthreads = num_cpu < n/10000 ? num_cpu : n/10000;
    
    if (nthreads == 1) {
        sumf = asum_compute(n, x, inc_x);
    }
    else {
        int mode, i;
        char result[MAX_CPU_NUMBER * sizeof(double) *2];
        FLOAT *ptr;
#if !defined(DOUBLE)
        mode = BLAS_SINGLE | BLAS_COMPLEX;
#else
        mode = BLAS_DOUBLE | BLAS_COMPLEX;
#endif
        blas_level1_thread_with_return_value(mode, n, 0, 0, dummy_alpha, x, inc_x, 
                NULL, 0, result, 0, (int (*)(void))asum_thread_function, nthreads);
        ptr = (FLOAT *)result;
        for (i = 0; i < nthreads; i++) {
            sumf += (*ptr);
            ptr = (FLOAT *)(((char *)ptr) + sizeof(double) *2);
        }
    }
#else
    sumf = asum_compute(n, x, inc_x);
#endif
    return(sumf);
}
