#include "lapacke_utils.h"

lapack_int LAPACKE_ztrsyl3( int matrix_layout, char trana, char tranb,
                            lapack_int isgn, lapack_int m, lapack_int n,
                            const lapack_complex_double* a, lapack_int lda,
                            const lapack_complex_double* b, lapack_int ldb,
                            lapack_complex_double* c, lapack_int ldc,
                            double* scale )
{
    lapack_int info = 0;
    double swork_query[2];
    double* swork = NULL;
    lapack_int ldswork = -1;
    lapack_int swork_size = -1;
    if( matrix_layout != LAPACK_COL_MAJOR && matrix_layout != LAPACK_ROW_MAJOR ) {
        LAPACKE_xerbla( "LAPACKE_ztrsyl3", -1 );
        return -1;
    }
#ifndef LAPACK_DISABLE_NAN_CHECK
    if( LAPACKE_get_nancheck() ) {
        /* Optionally check input matrices for NaNs */
        if( LAPACKE_zge_nancheck( matrix_layout, m, m, a, lda ) ) {
            return -7;
        }
        if( LAPACKE_zge_nancheck( matrix_layout, n, n, b, ldb ) ) {
            return -9;
        }
        if( LAPACKE_zge_nancheck( matrix_layout, m, n, c, ldc ) ) {
            return -11;
        }
    }
#endif
    /* Query optimal working array sizes */
    info = LAPACKE_ztrsyl3_work( matrix_layout, trana, tranb, isgn, m, n, a, lda,
                                 b, ldb, c, ldc, scale, swork_query, ldswork );
    if( info != 0 ) {
        goto exit_level_0;
    }
    ldswork = swork_query[0];
    swork_size = ldswork * swork_query[1];
    swork = (double*)LAPACKE_malloc( sizeof(double) * swork_size);
    if( swork == NULL ) {
        info = LAPACK_WORK_MEMORY_ERROR;
        goto exit_level_0;
    }
    /* Call middle-level interface */
    info = LAPACKE_ztrsyl3_work( matrix_layout, trana, tranb, isgn, m, n, a,
                                 lda, b, ldb, c, ldc, scale, swork, ldswork );
    /* Release memory and exit */
    LAPACKE_free( swork );
exit_level_0:
    if( info == LAPACK_WORK_MEMORY_ERROR ) {
        LAPACKE_xerbla( "LAPACKE_ztrsyl3", info );
    }
    return info;
}
