#include "lapacke_utils.h"

lapack_int LAPACKE_ctrsyl3( int matrix_layout, char trana, char tranb,
                            lapack_int isgn, lapack_int m, lapack_int n,
                            const lapack_complex_float* a, lapack_int lda,
                            const lapack_complex_float* b, lapack_int ldb,
                            lapack_complex_float* c, lapack_int ldc,
                            float* scale )
{
    lapack_int info = 0;
    float swork_query[2];
    float* swork = NULL;
    lapack_int ldswork = -1;
    lapack_int swork_size = -1;
    if( matrix_layout != LAPACK_COL_MAJOR && matrix_layout != LAPACK_ROW_MAJOR ) {
        LAPACKE_xerbla( "LAPACKE_ctrsyl3", -1 );
        return -1;
    }
#ifndef LAPACK_DISABLE_NAN_CHECK
    if( LAPACKE_get_nancheck() ) {
        /* Optionally check input matrices for NaNs */
        if( LAPACKE_cge_nancheck( matrix_layout, m, m, a, lda ) ) {
            return -7;
        }
        if( LAPACKE_cge_nancheck( matrix_layout, n, n, b, ldb ) ) {
            return -9;
        }
        if( LAPACKE_cge_nancheck( matrix_layout, m, n, c, ldc ) ) {
            return -11;
        }
    }
#endif
    /* Query optimal working array sizes */
    info = LAPACKE_ctrsyl3_work( matrix_layout, trana, tranb, isgn, m, n, a, lda,
                                 b, ldb, c, ldc, scale, swork_query, ldswork );
    if( info != 0 ) {
        goto exit_level_0;
    }
    ldswork = swork_query[0];
    swork_size = ldswork * swork_query[1];
    swork = (float*)LAPACKE_malloc( sizeof(float) * swork_size);
    if( swork == NULL ) {
        info = LAPACK_WORK_MEMORY_ERROR;
        goto exit_level_0;
    }
    /* Call middle-level interface */
    info = LAPACKE_ctrsyl3_work( matrix_layout, trana, tranb, isgn, m, n, a,
                                 lda, b, ldb, c, ldc, scale, swork, ldswork );
    /* Release memory and exit */
    LAPACKE_free( swork );
exit_level_0:
    if( info == LAPACK_WORK_MEMORY_ERROR ) {
        LAPACKE_xerbla( "LAPACKE_ctrsyl3", info );
    }
    return info;
}
