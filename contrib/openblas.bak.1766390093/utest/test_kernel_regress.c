#include "openblas_utest.h"
#include <stdio.h>
#include <stdlib.h>
#include <cblas.h>

#define LAPACK_ROW_MAJOR               101
blasint LAPACKE_dgesvd( blasint matrix_layout, char jobu, char jobvt,
                           blasint m, blasint n, double* a,
                           blasint lda, double* s, double* u, blasint ldu,
                           double* vt, blasint ldvt, double* superb );
                                                                                 

#define DATASIZE 100

double s[DATASIZE];
double u[DATASIZE*DATASIZE];
double vt[DATASIZE*DATASIZE];
double X[DATASIZE*DATASIZE];
double superb[DATASIZE];
double tmp[DATASIZE*DATASIZE];
double m[DATASIZE*DATASIZE];

CTEST(kernel_regress,skx_avx)
{
#ifdef BUILD_DOUBLE
    double norm;
    int i, j, info;
    srand(0);
	for (i = 0; i < DATASIZE*DATASIZE; i++) {
        m[i] = (rand()+0.0)/RAND_MAX * 10;
        tmp[i] = m[i];
    }

    info = LAPACKE_dgesvd( LAPACK_ROW_MAJOR, 'A', 'A', DATASIZE, DATASIZE, m, DATASIZE,
                        s, u, DATASIZE, vt, DATASIZE, superb);

	for (i = 0; i < DATASIZE; i++) {
	    for (j = 0; j < DATASIZE; j++) {
            u[i*DATASIZE+j] = u[i*DATASIZE+j]*s[j];
        }
    }
    cblas_dgemm(CblasRowMajor, CblasNoTrans, CblasNoTrans, 
                DATASIZE, DATASIZE, DATASIZE, 1, u, DATASIZE, vt, DATASIZE, 0, X, DATASIZE);

	for (i = 0; i < DATASIZE*DATASIZE; i++) {
        X[i] = X[i] - tmp[i];
    }
    
    norm = cblas_dnrm2(DATASIZE*DATASIZE, X, 1);
    ASSERT_DBL_NEAR_TOL(0.0, norm, 1e-10);
#endif
}
