/*****************************************************************************
  Copyright (c) 2014, Intel Corp.
  All rights reserved.

  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice,
      this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of Intel Corporation nor the names of its contributors
      may be used to endorse or promote products derived from this software
      without specific prior written permission.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
  THE POSSIBILITY OF SUCH DAMAGE.
*****************************************************************************
* Contents: Native middle-level C interface to LAPACK function zgedmd
* Author: Intel Corporation
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_zgedmd_work( int matrix_layout, char jobs, char jobz,
                                char jobr, char jobf, lapack_int whtsvd,
				lapack_int m, lapack_int n,
				lapack_complex_double* x,
                                lapack_int ldx, lapack_complex_double* y,
                                lapack_int ldy, lapack_int nrnk, double *tol, lapack_int k,
                                lapack_complex_double* eigs, lapack_complex_double* z,
                                lapack_int ldz, double* res,
                                lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* w, lapack_int ldw,
                                lapack_complex_double* s, lapack_int lds,
                                lapack_complex_double* zwork, lapack_int lzwork,
                                double* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork )
{
    lapack_int info = 0;
    if( matrix_layout == LAPACK_COL_MAJOR ) {
        /* Call LAPACK function and adjust info */
        LAPACK_zgedmd( &jobs, &jobz, &jobr, &jobf, &whtsvd, &m, &n, x, &ldx,
		       y, &ldy, &nrnk, tol, &k, eigs, z, &ldz, res, b, &ldb, w,
		       &ldw, s, &lds, zwork, &lzwork, work, &lwork, iwork, 
		       &liwork, &info );
        if( info < 0 ) {
            info = info - 1;
        }
    } else if( matrix_layout == LAPACK_ROW_MAJOR ) {
        lapack_int ldx_t = MAX(1,m);
        lapack_int ldy_t = MAX(1,m);
        lapack_int ldz_t = MAX(1,m);
        lapack_int ldb_t = MAX(1,m);
        lapack_int ldw_t = MAX(1,m);
        lapack_int lds_t = MAX(1,m);
        lapack_complex_double* x_t = NULL;
        lapack_complex_double* y_t = NULL;
        lapack_complex_double* z_t = NULL;
        lapack_complex_double* b_t = NULL;
        lapack_complex_double* w_t = NULL;
        lapack_complex_double* s_t = NULL;
        /* Check leading dimension(s) */
        if( ldx < n ) {
            info = -9;
            LAPACKE_xerbla( "LAPACKE_zgedmd_work", info );
            return info;
        }
        if( ldy < n ) {
            info = -11;
            LAPACKE_xerbla( "LAPACKE_zgedmd_work", info );
            return info;
        }
        if( ldz < n ) {
            info = -16;
            LAPACKE_xerbla( "LAPACKE_zgedmd_work", info );
            return info;
        }
        if( ldb < n ) {
            info = -19;
            LAPACKE_xerbla( "LAPACKE_zgedmd_work", info );
            return info;
        }
        if( ldw < n ) {
            info = -21;
            LAPACKE_xerbla( "LAPACKE_zgedmd_work", info );
            return info;
        }
        if( lds < n ) {
            info = -23;
            LAPACKE_xerbla( "LAPACKE_zgedmd_work", info );
            return info;
        }
        /* Query optimal working array(s) size if requested */
        if( lwork == -1 ) {
            LAPACK_zgedmd( &jobs, &jobz, &jobr, &jobf, &whtsvd, &m, &n, x,
			   &ldx, y, &ldy, &nrnk, tol, &k, eigs, z, &ldz, res, b,
			   &ldb, w, &ldw, s, &lds, zwork, &lzwork, work, 
			   &lwork, iwork, &liwork, &info );
            return (info < 0) ? (info - 1) : info;
        }
        /* Allocate memory for temporary array(s) */
        x_t = (lapack_complex_double*)LAPACKE_malloc( sizeof(lapack_complex_double) * ldx_t * MAX(1,n) );
        if( x_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_0;
        }
        y_t = (lapack_complex_double*)LAPACKE_malloc( sizeof(lapack_complex_double) * ldy_t * MAX(1,n) );
        if( y_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_1;
        }
        z_t = (lapack_complex_double*)LAPACKE_malloc( sizeof(lapack_complex_double) * ldz_t * MAX(1,n) );
        if( z_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_2;
        }
        b_t = (lapack_complex_double*)LAPACKE_malloc( sizeof(lapack_complex_double) * ldb_t * MAX(1,n) );
        if( b_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_3;
        }
        w_t = (lapack_complex_double*)LAPACKE_malloc( sizeof(lapack_complex_double) * ldw_t * MAX(1,n) );
        if( w_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_4;
        }
        s_t = (lapack_complex_double*)LAPACKE_malloc( sizeof(lapack_complex_double) * lds_t * MAX(1,n) );
        if( s_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_5;
        }
        /* Transpose input matrices */
        LAPACKE_zge_trans( matrix_layout, m, n, x, ldx, x_t, ldx_t );
        LAPACKE_zge_trans( matrix_layout, m, n, y, ldy, y_t, ldy_t );
        LAPACKE_zge_trans( matrix_layout, m, n, z, ldz, z_t, ldz_t );
        LAPACKE_zge_trans( matrix_layout, m, n, b, ldb, b_t, ldb_t );
        LAPACKE_zge_trans( matrix_layout, m, n, w, ldw, w_t, ldw_t );
        LAPACKE_zge_trans( matrix_layout, m, n, s, lds, s_t, lds_t );
        /* Call LAPACK function and adjust info */
        LAPACK_zgedmd( &jobs, &jobz, &jobr, &jobf, &whtsvd, &m, &n, x_t,
		       &ldx_t, y_t, &ldy_t, &nrnk, tol, &k, eigs, z_t, &ldz_t,
		       res, b_t, &ldb_t, w_t, &ldw_t, s_t, &lds_t, zwork,
		       &lzwork, work, &lwork, iwork, &liwork, &info );
        if( info < 0 ) {
            info = info - 1;
        }
        /* Transpose output matrices */
        LAPACKE_zge_trans( LAPACK_COL_MAJOR, m, n, x_t, ldx_t, x, ldx );
        LAPACKE_zge_trans( LAPACK_COL_MAJOR, m, n, y_t, ldy_t, y, ldy );
        LAPACKE_zge_trans( LAPACK_COL_MAJOR, m, n, z_t, ldz_t, z, ldz );
        LAPACKE_zge_trans( LAPACK_COL_MAJOR, m, n, b_t, ldb_t, b, ldb );
        LAPACKE_zge_trans( LAPACK_COL_MAJOR, m, n, w_t, ldw_t, w, ldw );
        LAPACKE_zge_trans( LAPACK_COL_MAJOR, m, n, s_t, lds_t, s, lds );
        /* Release memory and exit */
        LAPACKE_free( s_t );
exit_level_5:
        LAPACKE_free( w_t );
exit_level_4:
        LAPACKE_free( b_t );
exit_level_3:
        LAPACKE_free( z_t );
exit_level_2:
        LAPACKE_free( y_t );
exit_level_1:
        LAPACKE_free( x_t );
exit_level_0:
        if( info == LAPACK_TRANSPOSE_MEMORY_ERROR ) {
            LAPACKE_xerbla( "LAPACKE_zgedmd_work", info );
        }
    } else {
        info = -1;
        LAPACKE_xerbla( "LAPACKE_zgedmd_work", info );
    }
    return info;
}
