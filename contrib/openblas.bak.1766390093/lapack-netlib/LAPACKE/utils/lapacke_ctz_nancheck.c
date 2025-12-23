/*****************************************************************************
  Copyright (c) 2022, Intel Corp.
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
******************************************************************************
* Contents: Native C interface to LAPACK utility function
* Author: Simon Märtens
*****************************************************************************/

#include "lapacke_utils.h"

/*****************************************************************************
  Check a trapezoidal matrix for NaN entries. The shape of the trapezoidal
  matrix is determined by the arguments `direct` and `uplo`. `Direct` chooses
  the diagonal which shall be considered and `uplo` tells us whether we use the
  upper or lower part of the matrix with respect to the chosen diagonal.

      Diagonals 'F' (front / forward) and 'B' (back / backward):

        A = ( F       )           A = ( F     B       )
            (    F    )               (    F     B    )
            ( B     F )               (       F     B )
            (    B    )
            (       B )

      direct = 'F', uplo = 'L':

        A = ( *       )           A = ( *             )
            ( *  *    )               ( *  *          )
            ( *  *  * )               ( *  *  *       )
            ( *  *  * )
            ( *  *  * )

      direct = 'F', uplo = 'U':

        A = ( *  *  * )           A = ( *  *  *  *  * )
            (    *  * )               (    *  *  *  * )
            (       * )               (       *  *  * )
            (         )
            (         )

      direct = 'B', uplo = 'L':

        A = (         )           A = ( *  *  *       )
            (         )               ( *  *  *  *    )
            ( *       )               ( *  *  *  *  * )
            ( *  *    )
            ( *  *  * )

      direct = 'B', uplo = 'U':

        A = ( *  *  * )           A = (       *  *  * )
            ( *  *  * )               (          *  * )
            ( *  *  * )               (             * )
            (    *  * )
            (       * )

*****************************************************************************/

lapack_logical LAPACKE_ctz_nancheck( int matrix_layout, char direct, char uplo,
                                     char diag, lapack_int m, lapack_int n,
                                     const lapack_complex_float *a,
                                     lapack_int lda )
{
    lapack_logical colmaj, front, lower, unit;

    if( a == NULL ) return (lapack_logical) 0;

    colmaj = ( matrix_layout == LAPACK_COL_MAJOR );
    front  = LAPACKE_lsame( direct, 'f' );
    lower  = LAPACKE_lsame( uplo, 'l' );
    unit   = LAPACKE_lsame( diag, 'u' );

    if( ( !colmaj && ( matrix_layout != LAPACK_ROW_MAJOR ) ) ||
        ( !front  && !LAPACKE_lsame( direct, 'b' ) ) ||
        ( !lower  && !LAPACKE_lsame( uplo, 'u' ) ) ||
        ( !unit   && !LAPACKE_lsame( diag, 'n' ) ) ) {
        /* Just exit if any of input parameters are wrong */
        return (lapack_logical) 0;
    }

    /* Initial offsets and sizes of triangular and rectangular parts */
    lapack_int tri_offset = 0;
    lapack_int tri_n = MIN(m,n);
    lapack_int rect_offset = -1;
    lapack_int rect_m = ( m > n ) ? m - n : m;
    lapack_int rect_n = ( n > m ) ? n - m : n;

    /* Fix offsets depending on the shape of the matrix */
    if( front ) {
        if( lower && m > n ) {
            rect_offset = tri_n * ( !colmaj ? lda : 1 );
        } else if( !lower && n > m ) {
            rect_offset = tri_n * ( colmaj ? lda : 1 );
        }
    } else {
        if( m > n ) {
            tri_offset = rect_m * ( !colmaj ? lda : 1 );
            if( !lower ) {
                rect_offset = 0;
            }
        } else if( n > m ) {
            tri_offset = rect_n * ( colmaj ? lda : 1 );
            if( lower ) {
                rect_offset = 0;
            }
        }
    }

    /* Check rectangular part */
    if( rect_offset >= 0 ) {
        if( LAPACKE_cge_nancheck( matrix_layout, rect_m, rect_n,
                                  &a[rect_offset], lda) ) {
            return (lapack_logical) 1;
        }
    }

    /* Check triangular part */
    return LAPACKE_ctr_nancheck( matrix_layout, uplo, diag, tri_n,
                                 &a[tri_offset], lda );
}
