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
* Contents: Native high-level C interface to LAPACK function slarfb
* Author: Intel Corporation
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_slarfb( int matrix_layout, char side, char trans, char direct,
                           char storev, lapack_int m, lapack_int n,
                           lapack_int k, const float* v, lapack_int ldv,
                           const float* t, lapack_int ldt, float* c,
                           lapack_int ldc )
{
    lapack_int info = 0;
    lapack_int ldwork;
    float* work = NULL;
    lapack_int nrows_v, ncols_v;
    lapack_logical left, col, forward;
    char uplo;
    if( matrix_layout != LAPACK_COL_MAJOR && matrix_layout != LAPACK_ROW_MAJOR ) {
        LAPACKE_xerbla( "LAPACKE_slarfb", -1 );
        return -1;
    }
#ifndef LAPACK_DISABLE_NAN_CHECK
    if( LAPACKE_get_nancheck() ) {
        /* Optionally check input matrices for NaNs */
        left = LAPACKE_lsame( side, 'l' );
        col = LAPACKE_lsame( storev, 'c' );
        forward = LAPACKE_lsame( direct, 'f' );

        nrows_v = ( col && left ) ? m : ( ( col && !left ) ? n : ( !col ? k : 1) );
        ncols_v = ( !col && left ) ? m : ( ( !col && !left ) ? n : ( col ? k : 1 ) );
        uplo = ( ( forward && col ) || !( forward || col ) ) ? 'l' : 'u';

        if( ( col && k > nrows_v ) || ( !col && k > ncols_v ) ) {
            LAPACKE_xerbla( "LAPACKE_slarfb", -8 );
            return -8;
        }
        if( LAPACKE_stz_nancheck( matrix_layout, direct, uplo, 'u',
                                  nrows_v, ncols_v, v, ldv ) ) {
            return -9;
        }
        if( LAPACKE_sge_nancheck( matrix_layout, k, k, t, ldt ) ) {
            return -11;
        }
        if( LAPACKE_sge_nancheck( matrix_layout, m, n, c, ldc ) ) {
            return -13;
        }
    }
#endif
    if( LAPACKE_lsame( side, 'l' ) ) {
        ldwork = n;
    } else if( LAPACKE_lsame( side, 'r' ) ) {
        ldwork = m;
    } else {
        ldwork = 1;
    }
    /* Allocate memory for working array(s) */
    work = (float*)LAPACKE_malloc( sizeof(float) * ldwork * MAX(1,k) );
    if( work == NULL ) {
        info = LAPACK_WORK_MEMORY_ERROR;
        goto exit_level_0;
    }
    /* Call middle-level interface */
    info = LAPACKE_slarfb_work( matrix_layout, side, trans, direct, storev, m, n,
                                k, v, ldv, t, ldt, c, ldc, work, ldwork );
    /* Release memory and exit */
    LAPACKE_free( work );
exit_level_0:
    if( info == LAPACK_WORK_MEMORY_ERROR ) {
        LAPACKE_xerbla( "LAPACKE_slarfb", info );
    }
    return info;
}
