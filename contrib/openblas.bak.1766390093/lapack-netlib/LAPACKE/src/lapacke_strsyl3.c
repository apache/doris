#include "lapacke_utils.h"

lapack_int LAPACKE_strsyl3( int matrix_layout, char trana, char tranb,
                            lapack_int isgn, lapack_int m, lapack_int n,
                            const float* a, lapack_int lda, const float* b,
                            lapack_int ldb, float* c, lapack_int ldc,
                            float* scale )
{
    lapack_int info = 0;
    float swork_query[2];
    float* swork = NULL;
    lapack_int ldswork = -1;
    lapack_int swork_size = -1;
    lapack_int iwork_query;
    lapack_int* iwork = NULL;
    lapack_int liwork = -1;
    if( matrix_layout != LAPACK_COL_MAJOR && matrix_layout != LAPACK_ROW_MAJOR ) {
        LAPACKE_xerbla( "LAPACKE_strsyl3", -1 );
        return -1;
    }
#ifndef LAPACK_DISABLE_NAN_CHECK
    if( LAPACKE_get_nancheck() ) {
        /* Optionally check input matrices for NaNs */
        if( LAPACKE_sge_nancheck( matrix_layout, m, m, a, lda ) ) {
            return -7;
        }
        if( LAPACKE_sge_nancheck( matrix_layout, n, n, b, ldb ) ) {
            return -9;
        }
        if( LAPACKE_sge_nancheck( matrix_layout, m, n, c, ldc ) ) {
            return -11;
        }
    }
#endif
    /* Query optimal working array sizes */
    info = LAPACKE_strsyl3_work( matrix_layout, trana, tranb, isgn, m, n, a, lda,
                                 b, ldb, c, ldc, scale, &iwork_query, liwork,
                                 swork_query, ldswork );
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
    liwork = iwork_query;
    iwork = (lapack_int*)LAPACKE_malloc( sizeof(lapack_int) * liwork );
    if ( iwork == NULL ) {
        info = LAPACK_WORK_MEMORY_ERROR;
        goto exit_level_1;
    }
    /* Call middle-level interface */
    info = LAPACKE_strsyl3_work( matrix_layout, trana, tranb, isgn, m, n, a,
                                 lda, b, ldb, c, ldc, scale, iwork, liwork,
                                 swork, ldswork );
    /* Release memory and exit */
    LAPACKE_free( iwork );
exit_level_1:
    LAPACKE_free( swork );
exit_level_0:
    if( info == LAPACK_WORK_MEMORY_ERROR ) {
        LAPACKE_xerbla( "LAPACKE_strsyl3", info );
    }
    return info;
}
