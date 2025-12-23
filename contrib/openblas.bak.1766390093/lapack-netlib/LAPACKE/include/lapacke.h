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
******************************************************************************
* Contents: Native C interface to LAPACK
* Author: Intel Corporation
*****************************************************************************/

#ifndef _LAPACKE_H_
#define _LAPACKE_H_

#include "lapack.h"

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

#ifndef LAPACKE_malloc
#define LAPACKE_malloc( size ) malloc( size )
#endif
#ifndef LAPACKE_free
#define LAPACKE_free( p )      free( p )
#endif

#define LAPACK_C2INT( x ) (lapack_int)(*((float*)&x ))
#define LAPACK_Z2INT( x ) (lapack_int)(*((double*)&x ))

#define LAPACK_ROW_MAJOR               101
#define LAPACK_COL_MAJOR               102

#define LAPACK_WORK_MEMORY_ERROR       -1010
#define LAPACK_TRANSPOSE_MEMORY_ERROR  -1011

lapack_complex_float lapack_make_complex_float( float re, float im );
lapack_complex_double lapack_make_complex_double( double re, double im );

/* C-LAPACK function prototypes */

lapack_int LAPACKE_sbdsdc( int matrix_layout, char uplo, char compq,
                           lapack_int n, float* d, float* e, float* u,
                           lapack_int ldu, float* vt, lapack_int ldvt, float* q,
                           lapack_int* iq );
lapack_int LAPACKE_dbdsdc( int matrix_layout, char uplo, char compq,
                           lapack_int n, double* d, double* e, double* u,
                           lapack_int ldu, double* vt, lapack_int ldvt,
                           double* q, lapack_int* iq );

lapack_int LAPACKE_sbdsqr( int matrix_layout, char uplo, lapack_int n,
                           lapack_int ncvt, lapack_int nru, lapack_int ncc,
                           float* d, float* e, float* vt, lapack_int ldvt,
                           float* u, lapack_int ldu, float* c, lapack_int ldc );
lapack_int LAPACKE_dbdsqr( int matrix_layout, char uplo, lapack_int n,
                           lapack_int ncvt, lapack_int nru, lapack_int ncc,
                           double* d, double* e, double* vt, lapack_int ldvt,
                           double* u, lapack_int ldu, double* c,
                           lapack_int ldc );
lapack_int LAPACKE_cbdsqr( int matrix_layout, char uplo, lapack_int n,
                           lapack_int ncvt, lapack_int nru, lapack_int ncc,
                           float* d, float* e, lapack_complex_float* vt,
                           lapack_int ldvt, lapack_complex_float* u,
                           lapack_int ldu, lapack_complex_float* c,
                           lapack_int ldc );
lapack_int LAPACKE_zbdsqr( int matrix_layout, char uplo, lapack_int n,
                           lapack_int ncvt, lapack_int nru, lapack_int ncc,
                           double* d, double* e, lapack_complex_double* vt,
                           lapack_int ldvt, lapack_complex_double* u,
                           lapack_int ldu, lapack_complex_double* c,
                           lapack_int ldc );
lapack_int LAPACKE_sbdsvdx( int matrix_layout, char uplo, char jobz, char range,
                           lapack_int n, float* d, float* e,
                           float vl, float vu,
                           lapack_int il, lapack_int iu, lapack_int* ns,
                           float* s, float* z, lapack_int ldz,
                           lapack_int* superb );
lapack_int LAPACKE_dbdsvdx( int matrix_layout, char uplo, char jobz, char range,
                           lapack_int n, double* d, double* e,
                           double vl, double vu,
                           lapack_int il, lapack_int iu, lapack_int* ns,
                           double* s, double* z, lapack_int ldz,
                           lapack_int* superb );
lapack_int LAPACKE_sdisna( char job, lapack_int m, lapack_int n, const float* d,
                           float* sep );
lapack_int LAPACKE_ddisna( char job, lapack_int m, lapack_int n,
                           const double* d, double* sep );

lapack_int LAPACKE_sgbbrd( int matrix_layout, char vect, lapack_int m,
                           lapack_int n, lapack_int ncc, lapack_int kl,
                           lapack_int ku, float* ab, lapack_int ldab, float* d,
                           float* e, float* q, lapack_int ldq, float* pt,
                           lapack_int ldpt, float* c, lapack_int ldc );
lapack_int LAPACKE_dgbbrd( int matrix_layout, char vect, lapack_int m,
                           lapack_int n, lapack_int ncc, lapack_int kl,
                           lapack_int ku, double* ab, lapack_int ldab,
                           double* d, double* e, double* q, lapack_int ldq,
                           double* pt, lapack_int ldpt, double* c,
                           lapack_int ldc );
lapack_int LAPACKE_cgbbrd( int matrix_layout, char vect, lapack_int m,
                           lapack_int n, lapack_int ncc, lapack_int kl,
                           lapack_int ku, lapack_complex_float* ab,
                           lapack_int ldab, float* d, float* e,
                           lapack_complex_float* q, lapack_int ldq,
                           lapack_complex_float* pt, lapack_int ldpt,
                           lapack_complex_float* c, lapack_int ldc );
lapack_int LAPACKE_zgbbrd( int matrix_layout, char vect, lapack_int m,
                           lapack_int n, lapack_int ncc, lapack_int kl,
                           lapack_int ku, lapack_complex_double* ab,
                           lapack_int ldab, double* d, double* e,
                           lapack_complex_double* q, lapack_int ldq,
                           lapack_complex_double* pt, lapack_int ldpt,
                           lapack_complex_double* c, lapack_int ldc );

lapack_int LAPACKE_sgbcon( int matrix_layout, char norm, lapack_int n,
                           lapack_int kl, lapack_int ku, const float* ab,
                           lapack_int ldab, const lapack_int* ipiv, float anorm,
                           float* rcond );
lapack_int LAPACKE_dgbcon( int matrix_layout, char norm, lapack_int n,
                           lapack_int kl, lapack_int ku, const double* ab,
                           lapack_int ldab, const lapack_int* ipiv,
                           double anorm, double* rcond );
lapack_int LAPACKE_cgbcon( int matrix_layout, char norm, lapack_int n,
                           lapack_int kl, lapack_int ku,
                           const lapack_complex_float* ab, lapack_int ldab,
                           const lapack_int* ipiv, float anorm, float* rcond );
lapack_int LAPACKE_zgbcon( int matrix_layout, char norm, lapack_int n,
                           lapack_int kl, lapack_int ku,
                           const lapack_complex_double* ab, lapack_int ldab,
                           const lapack_int* ipiv, double anorm,
                           double* rcond );

lapack_int LAPACKE_sgbequ( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int kl, lapack_int ku, const float* ab,
                           lapack_int ldab, float* r, float* c, float* rowcnd,
                           float* colcnd, float* amax );
lapack_int LAPACKE_dgbequ( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int kl, lapack_int ku, const double* ab,
                           lapack_int ldab, double* r, double* c,
                           double* rowcnd, double* colcnd, double* amax );
lapack_int LAPACKE_cgbequ( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int kl, lapack_int ku,
                           const lapack_complex_float* ab, lapack_int ldab,
                           float* r, float* c, float* rowcnd, float* colcnd,
                           float* amax );
lapack_int LAPACKE_zgbequ( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int kl, lapack_int ku,
                           const lapack_complex_double* ab, lapack_int ldab,
                           double* r, double* c, double* rowcnd, double* colcnd,
                           double* amax );

lapack_int LAPACKE_sgbequb( int matrix_layout, lapack_int m, lapack_int n,
                            lapack_int kl, lapack_int ku, const float* ab,
                            lapack_int ldab, float* r, float* c, float* rowcnd,
                            float* colcnd, float* amax );
lapack_int LAPACKE_dgbequb( int matrix_layout, lapack_int m, lapack_int n,
                            lapack_int kl, lapack_int ku, const double* ab,
                            lapack_int ldab, double* r, double* c,
                            double* rowcnd, double* colcnd, double* amax );
lapack_int LAPACKE_cgbequb( int matrix_layout, lapack_int m, lapack_int n,
                            lapack_int kl, lapack_int ku,
                            const lapack_complex_float* ab, lapack_int ldab,
                            float* r, float* c, float* rowcnd, float* colcnd,
                            float* amax );
lapack_int LAPACKE_zgbequb( int matrix_layout, lapack_int m, lapack_int n,
                            lapack_int kl, lapack_int ku,
                            const lapack_complex_double* ab, lapack_int ldab,
                            double* r, double* c, double* rowcnd,
                            double* colcnd, double* amax );

lapack_int LAPACKE_sgbrfs( int matrix_layout, char trans, lapack_int n,
                           lapack_int kl, lapack_int ku, lapack_int nrhs,
                           const float* ab, lapack_int ldab, const float* afb,
                           lapack_int ldafb, const lapack_int* ipiv,
                           const float* b, lapack_int ldb, float* x,
                           lapack_int ldx, float* ferr, float* berr );
lapack_int LAPACKE_dgbrfs( int matrix_layout, char trans, lapack_int n,
                           lapack_int kl, lapack_int ku, lapack_int nrhs,
                           const double* ab, lapack_int ldab, const double* afb,
                           lapack_int ldafb, const lapack_int* ipiv,
                           const double* b, lapack_int ldb, double* x,
                           lapack_int ldx, double* ferr, double* berr );
lapack_int LAPACKE_cgbrfs( int matrix_layout, char trans, lapack_int n,
                           lapack_int kl, lapack_int ku, lapack_int nrhs,
                           const lapack_complex_float* ab, lapack_int ldab,
                           const lapack_complex_float* afb, lapack_int ldafb,
                           const lapack_int* ipiv,
                           const lapack_complex_float* b, lapack_int ldb,
                           lapack_complex_float* x, lapack_int ldx, float* ferr,
                           float* berr );
lapack_int LAPACKE_zgbrfs( int matrix_layout, char trans, lapack_int n,
                           lapack_int kl, lapack_int ku, lapack_int nrhs,
                           const lapack_complex_double* ab, lapack_int ldab,
                           const lapack_complex_double* afb, lapack_int ldafb,
                           const lapack_int* ipiv,
                           const lapack_complex_double* b, lapack_int ldb,
                           lapack_complex_double* x, lapack_int ldx,
                           double* ferr, double* berr );

lapack_int LAPACKE_sgbrfsx( int matrix_layout, char trans, char equed,
                            lapack_int n, lapack_int kl, lapack_int ku,
                            lapack_int nrhs, const float* ab, lapack_int ldab,
                            const float* afb, lapack_int ldafb,
                            const lapack_int* ipiv, const float* r,
                            const float* c, const float* b, lapack_int ldb,
                            float* x, lapack_int ldx, float* rcond, float* berr,
                            lapack_int n_err_bnds, float* err_bnds_norm,
                            float* err_bnds_comp, lapack_int nparams,
                            float* params );
lapack_int LAPACKE_dgbrfsx( int matrix_layout, char trans, char equed,
                            lapack_int n, lapack_int kl, lapack_int ku,
                            lapack_int nrhs, const double* ab, lapack_int ldab,
                            const double* afb, lapack_int ldafb,
                            const lapack_int* ipiv, const double* r,
                            const double* c, const double* b, lapack_int ldb,
                            double* x, lapack_int ldx, double* rcond,
                            double* berr, lapack_int n_err_bnds,
                            double* err_bnds_norm, double* err_bnds_comp,
                            lapack_int nparams, double* params );
lapack_int LAPACKE_cgbrfsx( int matrix_layout, char trans, char equed,
                            lapack_int n, lapack_int kl, lapack_int ku,
                            lapack_int nrhs, const lapack_complex_float* ab,
                            lapack_int ldab, const lapack_complex_float* afb,
                            lapack_int ldafb, const lapack_int* ipiv,
                            const float* r, const float* c,
                            const lapack_complex_float* b, lapack_int ldb,
                            lapack_complex_float* x, lapack_int ldx,
                            float* rcond, float* berr, lapack_int n_err_bnds,
                            float* err_bnds_norm, float* err_bnds_comp,
                            lapack_int nparams, float* params );
lapack_int LAPACKE_zgbrfsx( int matrix_layout, char trans, char equed,
                            lapack_int n, lapack_int kl, lapack_int ku,
                            lapack_int nrhs, const lapack_complex_double* ab,
                            lapack_int ldab, const lapack_complex_double* afb,
                            lapack_int ldafb, const lapack_int* ipiv,
                            const double* r, const double* c,
                            const lapack_complex_double* b, lapack_int ldb,
                            lapack_complex_double* x, lapack_int ldx,
                            double* rcond, double* berr, lapack_int n_err_bnds,
                            double* err_bnds_norm, double* err_bnds_comp,
                            lapack_int nparams, double* params );

lapack_int LAPACKE_sgbsv( int matrix_layout, lapack_int n, lapack_int kl,
                          lapack_int ku, lapack_int nrhs, float* ab,
                          lapack_int ldab, lapack_int* ipiv, float* b,
                          lapack_int ldb );
lapack_int LAPACKE_dgbsv( int matrix_layout, lapack_int n, lapack_int kl,
                          lapack_int ku, lapack_int nrhs, double* ab,
                          lapack_int ldab, lapack_int* ipiv, double* b,
                          lapack_int ldb );
lapack_int LAPACKE_cgbsv( int matrix_layout, lapack_int n, lapack_int kl,
                          lapack_int ku, lapack_int nrhs,
                          lapack_complex_float* ab, lapack_int ldab,
                          lapack_int* ipiv, lapack_complex_float* b,
                          lapack_int ldb );
lapack_int LAPACKE_zgbsv( int matrix_layout, lapack_int n, lapack_int kl,
                          lapack_int ku, lapack_int nrhs,
                          lapack_complex_double* ab, lapack_int ldab,
                          lapack_int* ipiv, lapack_complex_double* b,
                          lapack_int ldb );

lapack_int LAPACKE_sgbsvx( int matrix_layout, char fact, char trans,
                           lapack_int n, lapack_int kl, lapack_int ku,
                           lapack_int nrhs, float* ab, lapack_int ldab,
                           float* afb, lapack_int ldafb, lapack_int* ipiv,
                           char* equed, float* r, float* c, float* b,
                           lapack_int ldb, float* x, lapack_int ldx,
                           float* rcond, float* ferr, float* berr,
                           float* rpivot );
lapack_int LAPACKE_dgbsvx( int matrix_layout, char fact, char trans,
                           lapack_int n, lapack_int kl, lapack_int ku,
                           lapack_int nrhs, double* ab, lapack_int ldab,
                           double* afb, lapack_int ldafb, lapack_int* ipiv,
                           char* equed, double* r, double* c, double* b,
                           lapack_int ldb, double* x, lapack_int ldx,
                           double* rcond, double* ferr, double* berr,
                           double* rpivot );
lapack_int LAPACKE_cgbsvx( int matrix_layout, char fact, char trans,
                           lapack_int n, lapack_int kl, lapack_int ku,
                           lapack_int nrhs, lapack_complex_float* ab,
                           lapack_int ldab, lapack_complex_float* afb,
                           lapack_int ldafb, lapack_int* ipiv, char* equed,
                           float* r, float* c, lapack_complex_float* b,
                           lapack_int ldb, lapack_complex_float* x,
                           lapack_int ldx, float* rcond, float* ferr,
                           float* berr, float* rpivot );
lapack_int LAPACKE_zgbsvx( int matrix_layout, char fact, char trans,
                           lapack_int n, lapack_int kl, lapack_int ku,
                           lapack_int nrhs, lapack_complex_double* ab,
                           lapack_int ldab, lapack_complex_double* afb,
                           lapack_int ldafb, lapack_int* ipiv, char* equed,
                           double* r, double* c, lapack_complex_double* b,
                           lapack_int ldb, lapack_complex_double* x,
                           lapack_int ldx, double* rcond, double* ferr,
                           double* berr, double* rpivot );

lapack_int LAPACKE_sgbsvxx( int matrix_layout, char fact, char trans,
                            lapack_int n, lapack_int kl, lapack_int ku,
                            lapack_int nrhs, float* ab, lapack_int ldab,
                            float* afb, lapack_int ldafb, lapack_int* ipiv,
                            char* equed, float* r, float* c, float* b,
                            lapack_int ldb, float* x, lapack_int ldx,
                            float* rcond, float* rpvgrw, float* berr,
                            lapack_int n_err_bnds, float* err_bnds_norm,
                            float* err_bnds_comp, lapack_int nparams,
                            float* params );
lapack_int LAPACKE_dgbsvxx( int matrix_layout, char fact, char trans,
                            lapack_int n, lapack_int kl, lapack_int ku,
                            lapack_int nrhs, double* ab, lapack_int ldab,
                            double* afb, lapack_int ldafb, lapack_int* ipiv,
                            char* equed, double* r, double* c, double* b,
                            lapack_int ldb, double* x, lapack_int ldx,
                            double* rcond, double* rpvgrw, double* berr,
                            lapack_int n_err_bnds, double* err_bnds_norm,
                            double* err_bnds_comp, lapack_int nparams,
                            double* params );
lapack_int LAPACKE_cgbsvxx( int matrix_layout, char fact, char trans,
                            lapack_int n, lapack_int kl, lapack_int ku,
                            lapack_int nrhs, lapack_complex_float* ab,
                            lapack_int ldab, lapack_complex_float* afb,
                            lapack_int ldafb, lapack_int* ipiv, char* equed,
                            float* r, float* c, lapack_complex_float* b,
                            lapack_int ldb, lapack_complex_float* x,
                            lapack_int ldx, float* rcond, float* rpvgrw,
                            float* berr, lapack_int n_err_bnds,
                            float* err_bnds_norm, float* err_bnds_comp,
                            lapack_int nparams, float* params );
lapack_int LAPACKE_zgbsvxx( int matrix_layout, char fact, char trans,
                            lapack_int n, lapack_int kl, lapack_int ku,
                            lapack_int nrhs, lapack_complex_double* ab,
                            lapack_int ldab, lapack_complex_double* afb,
                            lapack_int ldafb, lapack_int* ipiv, char* equed,
                            double* r, double* c, lapack_complex_double* b,
                            lapack_int ldb, lapack_complex_double* x,
                            lapack_int ldx, double* rcond, double* rpvgrw,
                            double* berr, lapack_int n_err_bnds,
                            double* err_bnds_norm, double* err_bnds_comp,
                            lapack_int nparams, double* params );

lapack_int LAPACKE_sgbtrf( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int kl, lapack_int ku, float* ab,
                           lapack_int ldab, lapack_int* ipiv );
lapack_int LAPACKE_dgbtrf( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int kl, lapack_int ku, double* ab,
                           lapack_int ldab, lapack_int* ipiv );
lapack_int LAPACKE_cgbtrf( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int kl, lapack_int ku,
                           lapack_complex_float* ab, lapack_int ldab,
                           lapack_int* ipiv );
lapack_int LAPACKE_zgbtrf( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int kl, lapack_int ku,
                           lapack_complex_double* ab, lapack_int ldab,
                           lapack_int* ipiv );

lapack_int LAPACKE_sgbtrs( int matrix_layout, char trans, lapack_int n,
                           lapack_int kl, lapack_int ku, lapack_int nrhs,
                           const float* ab, lapack_int ldab,
                           const lapack_int* ipiv, float* b, lapack_int ldb );
lapack_int LAPACKE_dgbtrs( int matrix_layout, char trans, lapack_int n,
                           lapack_int kl, lapack_int ku, lapack_int nrhs,
                           const double* ab, lapack_int ldab,
                           const lapack_int* ipiv, double* b, lapack_int ldb );
lapack_int LAPACKE_cgbtrs( int matrix_layout, char trans, lapack_int n,
                           lapack_int kl, lapack_int ku, lapack_int nrhs,
                           const lapack_complex_float* ab, lapack_int ldab,
                           const lapack_int* ipiv, lapack_complex_float* b,
                           lapack_int ldb );
lapack_int LAPACKE_zgbtrs( int matrix_layout, char trans, lapack_int n,
                           lapack_int kl, lapack_int ku, lapack_int nrhs,
                           const lapack_complex_double* ab, lapack_int ldab,
                           const lapack_int* ipiv, lapack_complex_double* b,
                           lapack_int ldb );

lapack_int LAPACKE_sgebak( int matrix_layout, char job, char side, lapack_int n,
                           lapack_int ilo, lapack_int ihi, const float* scale,
                           lapack_int m, float* v, lapack_int ldv );
lapack_int LAPACKE_dgebak( int matrix_layout, char job, char side, lapack_int n,
                           lapack_int ilo, lapack_int ihi, const double* scale,
                           lapack_int m, double* v, lapack_int ldv );
lapack_int LAPACKE_cgebak( int matrix_layout, char job, char side, lapack_int n,
                           lapack_int ilo, lapack_int ihi, const float* scale,
                           lapack_int m, lapack_complex_float* v,
                           lapack_int ldv );
lapack_int LAPACKE_zgebak( int matrix_layout, char job, char side, lapack_int n,
                           lapack_int ilo, lapack_int ihi, const double* scale,
                           lapack_int m, lapack_complex_double* v,
                           lapack_int ldv );

lapack_int LAPACKE_sgebal( int matrix_layout, char job, lapack_int n, float* a,
                           lapack_int lda, lapack_int* ilo, lapack_int* ihi,
                           float* scale );
lapack_int LAPACKE_dgebal( int matrix_layout, char job, lapack_int n, double* a,
                           lapack_int lda, lapack_int* ilo, lapack_int* ihi,
                           double* scale );
lapack_int LAPACKE_cgebal( int matrix_layout, char job, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_int* ilo, lapack_int* ihi, float* scale );
lapack_int LAPACKE_zgebal( int matrix_layout, char job, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_int* ilo, lapack_int* ihi, double* scale );

lapack_int LAPACKE_sgebrd( int matrix_layout, lapack_int m, lapack_int n,
                           float* a, lapack_int lda, float* d, float* e,
                           float* tauq, float* taup );
lapack_int LAPACKE_dgebrd( int matrix_layout, lapack_int m, lapack_int n,
                           double* a, lapack_int lda, double* d, double* e,
                           double* tauq, double* taup );
lapack_int LAPACKE_cgebrd( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_complex_float* a, lapack_int lda, float* d,
                           float* e, lapack_complex_float* tauq,
                           lapack_complex_float* taup );
lapack_int LAPACKE_zgebrd( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_complex_double* a, lapack_int lda, double* d,
                           double* e, lapack_complex_double* tauq,
                           lapack_complex_double* taup );

lapack_int LAPACKE_sgecon( int matrix_layout, char norm, lapack_int n,
                           const float* a, lapack_int lda, float anorm,
                           float* rcond );
lapack_int LAPACKE_dgecon( int matrix_layout, char norm, lapack_int n,
                           const double* a, lapack_int lda, double anorm,
                           double* rcond );
lapack_int LAPACKE_cgecon( int matrix_layout, char norm, lapack_int n,
                           const lapack_complex_float* a, lapack_int lda,
                           float anorm, float* rcond );
lapack_int LAPACKE_zgecon( int matrix_layout, char norm, lapack_int n,
                           const lapack_complex_double* a, lapack_int lda,
                           double anorm, double* rcond );

lapack_int LAPACKE_sgeequ( int matrix_layout, lapack_int m, lapack_int n,
                           const float* a, lapack_int lda, float* r, float* c,
                           float* rowcnd, float* colcnd, float* amax );
lapack_int LAPACKE_dgeequ( int matrix_layout, lapack_int m, lapack_int n,
                           const double* a, lapack_int lda, double* r,
                           double* c, double* rowcnd, double* colcnd,
                           double* amax );
lapack_int LAPACKE_cgeequ( int matrix_layout, lapack_int m, lapack_int n,
                           const lapack_complex_float* a, lapack_int lda,
                           float* r, float* c, float* rowcnd, float* colcnd,
                           float* amax );
lapack_int LAPACKE_zgeequ( int matrix_layout, lapack_int m, lapack_int n,
                           const lapack_complex_double* a, lapack_int lda,
                           double* r, double* c, double* rowcnd, double* colcnd,
                           double* amax );

lapack_int LAPACKE_sgeequb( int matrix_layout, lapack_int m, lapack_int n,
                            const float* a, lapack_int lda, float* r, float* c,
                            float* rowcnd, float* colcnd, float* amax );
lapack_int LAPACKE_dgeequb( int matrix_layout, lapack_int m, lapack_int n,
                            const double* a, lapack_int lda, double* r,
                            double* c, double* rowcnd, double* colcnd,
                            double* amax );
lapack_int LAPACKE_cgeequb( int matrix_layout, lapack_int m, lapack_int n,
                            const lapack_complex_float* a, lapack_int lda,
                            float* r, float* c, float* rowcnd, float* colcnd,
                            float* amax );
lapack_int LAPACKE_zgeequb( int matrix_layout, lapack_int m, lapack_int n,
                            const lapack_complex_double* a, lapack_int lda,
                            double* r, double* c, double* rowcnd,
                            double* colcnd, double* amax );

lapack_int LAPACKE_sgees( int matrix_layout, char jobvs, char sort,
                          LAPACK_S_SELECT2 select, lapack_int n, float* a,
                          lapack_int lda, lapack_int* sdim, float* wr,
                          float* wi, float* vs, lapack_int ldvs );
lapack_int LAPACKE_dgees( int matrix_layout, char jobvs, char sort,
                          LAPACK_D_SELECT2 select, lapack_int n, double* a,
                          lapack_int lda, lapack_int* sdim, double* wr,
                          double* wi, double* vs, lapack_int ldvs );
lapack_int LAPACKE_cgees( int matrix_layout, char jobvs, char sort,
                          LAPACK_C_SELECT1 select, lapack_int n,
                          lapack_complex_float* a, lapack_int lda,
                          lapack_int* sdim, lapack_complex_float* w,
                          lapack_complex_float* vs, lapack_int ldvs );
lapack_int LAPACKE_zgees( int matrix_layout, char jobvs, char sort,
                          LAPACK_Z_SELECT1 select, lapack_int n,
                          lapack_complex_double* a, lapack_int lda,
                          lapack_int* sdim, lapack_complex_double* w,
                          lapack_complex_double* vs, lapack_int ldvs );

lapack_int LAPACKE_sgeesx( int matrix_layout, char jobvs, char sort,
                           LAPACK_S_SELECT2 select, char sense, lapack_int n,
                           float* a, lapack_int lda, lapack_int* sdim,
                           float* wr, float* wi, float* vs, lapack_int ldvs,
                           float* rconde, float* rcondv );
lapack_int LAPACKE_dgeesx( int matrix_layout, char jobvs, char sort,
                           LAPACK_D_SELECT2 select, char sense, lapack_int n,
                           double* a, lapack_int lda, lapack_int* sdim,
                           double* wr, double* wi, double* vs, lapack_int ldvs,
                           double* rconde, double* rcondv );
lapack_int LAPACKE_cgeesx( int matrix_layout, char jobvs, char sort,
                           LAPACK_C_SELECT1 select, char sense, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_int* sdim, lapack_complex_float* w,
                           lapack_complex_float* vs, lapack_int ldvs,
                           float* rconde, float* rcondv );
lapack_int LAPACKE_zgeesx( int matrix_layout, char jobvs, char sort,
                           LAPACK_Z_SELECT1 select, char sense, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_int* sdim, lapack_complex_double* w,
                           lapack_complex_double* vs, lapack_int ldvs,
                           double* rconde, double* rcondv );

lapack_int LAPACKE_sgeev( int matrix_layout, char jobvl, char jobvr,
                          lapack_int n, float* a, lapack_int lda, float* wr,
                          float* wi, float* vl, lapack_int ldvl, float* vr,
                          lapack_int ldvr );
lapack_int LAPACKE_dgeev( int matrix_layout, char jobvl, char jobvr,
                          lapack_int n, double* a, lapack_int lda, double* wr,
                          double* wi, double* vl, lapack_int ldvl, double* vr,
                          lapack_int ldvr );
lapack_int LAPACKE_cgeev( int matrix_layout, char jobvl, char jobvr,
                          lapack_int n, lapack_complex_float* a, lapack_int lda,
                          lapack_complex_float* w, lapack_complex_float* vl,
                          lapack_int ldvl, lapack_complex_float* vr,
                          lapack_int ldvr );
lapack_int LAPACKE_zgeev( int matrix_layout, char jobvl, char jobvr,
                          lapack_int n, lapack_complex_double* a,
                          lapack_int lda, lapack_complex_double* w,
                          lapack_complex_double* vl, lapack_int ldvl,
                          lapack_complex_double* vr, lapack_int ldvr );

lapack_int LAPACKE_sgeevx( int matrix_layout, char balanc, char jobvl,
                           char jobvr, char sense, lapack_int n, float* a,
                           lapack_int lda, float* wr, float* wi, float* vl,
                           lapack_int ldvl, float* vr, lapack_int ldvr,
                           lapack_int* ilo, lapack_int* ihi, float* scale,
                           float* abnrm, float* rconde, float* rcondv );
lapack_int LAPACKE_dgeevx( int matrix_layout, char balanc, char jobvl,
                           char jobvr, char sense, lapack_int n, double* a,
                           lapack_int lda, double* wr, double* wi, double* vl,
                           lapack_int ldvl, double* vr, lapack_int ldvr,
                           lapack_int* ilo, lapack_int* ihi, double* scale,
                           double* abnrm, double* rconde, double* rcondv );
lapack_int LAPACKE_cgeevx( int matrix_layout, char balanc, char jobvl,
                           char jobvr, char sense, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_complex_float* w, lapack_complex_float* vl,
                           lapack_int ldvl, lapack_complex_float* vr,
                           lapack_int ldvr, lapack_int* ilo, lapack_int* ihi,
                           float* scale, float* abnrm, float* rconde,
                           float* rcondv );
lapack_int LAPACKE_zgeevx( int matrix_layout, char balanc, char jobvl,
                           char jobvr, char sense, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_complex_double* w, lapack_complex_double* vl,
                           lapack_int ldvl, lapack_complex_double* vr,
                           lapack_int ldvr, lapack_int* ilo, lapack_int* ihi,
                           double* scale, double* abnrm, double* rconde,
                           double* rcondv );

lapack_int LAPACKE_sgehrd( int matrix_layout, lapack_int n, lapack_int ilo,
                           lapack_int ihi, float* a, lapack_int lda,
                           float* tau );
lapack_int LAPACKE_dgehrd( int matrix_layout, lapack_int n, lapack_int ilo,
                           lapack_int ihi, double* a, lapack_int lda,
                           double* tau );
lapack_int LAPACKE_cgehrd( int matrix_layout, lapack_int n, lapack_int ilo,
                           lapack_int ihi, lapack_complex_float* a,
                           lapack_int lda, lapack_complex_float* tau );
lapack_int LAPACKE_zgehrd( int matrix_layout, lapack_int n, lapack_int ilo,
                           lapack_int ihi, lapack_complex_double* a,
                           lapack_int lda, lapack_complex_double* tau );

lapack_int LAPACKE_sgejsv( int matrix_layout, char joba, char jobu, char jobv,
                           char jobr, char jobt, char jobp, lapack_int m,
                           lapack_int n, float* a, lapack_int lda, float* sva,
                           float* u, lapack_int ldu, float* v, lapack_int ldv,
                           float* stat, lapack_int* istat );
lapack_int LAPACKE_dgejsv( int matrix_layout, char joba, char jobu, char jobv,
                           char jobr, char jobt, char jobp, lapack_int m,
                           lapack_int n, double* a, lapack_int lda, double* sva,
                           double* u, lapack_int ldu, double* v, lapack_int ldv,
                           double* stat, lapack_int* istat );
lapack_int LAPACKE_cgejsv( int matrix_layout, char joba, char jobu, char jobv,
                           char jobr, char jobt, char jobp, lapack_int m,
                           lapack_int n, lapack_complex_float* a, lapack_int lda, float* sva,
                           lapack_complex_float* u, lapack_int ldu, lapack_complex_float* v, lapack_int ldv,
                           float* stat, lapack_int* istat );
lapack_int LAPACKE_zgejsv( int matrix_layout, char joba, char jobu, char jobv,
                           char jobr, char jobt, char jobp, lapack_int m,
                           lapack_int n, lapack_complex_double* a, lapack_int lda, double* sva,
                           lapack_complex_double* u, lapack_int ldu, lapack_complex_double* v, lapack_int ldv,
                           double* stat, lapack_int* istat );

lapack_int LAPACKE_sgelq2( int matrix_layout, lapack_int m, lapack_int n,
                           float* a, lapack_int lda, float* tau );
lapack_int LAPACKE_dgelq2( int matrix_layout, lapack_int m, lapack_int n,
                           double* a, lapack_int lda, double* tau );
lapack_int LAPACKE_cgelq2( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_complex_float* tau );
lapack_int LAPACKE_zgelq2( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_complex_double* tau );

lapack_int LAPACKE_sgelqf( int matrix_layout, lapack_int m, lapack_int n,
                           float* a, lapack_int lda, float* tau );
lapack_int LAPACKE_dgelqf( int matrix_layout, lapack_int m, lapack_int n,
                           double* a, lapack_int lda, double* tau );
lapack_int LAPACKE_cgelqf( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_complex_float* tau );
lapack_int LAPACKE_zgelqf( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_complex_double* tau );

lapack_int LAPACKE_sgels( int matrix_layout, char trans, lapack_int m,
                          lapack_int n, lapack_int nrhs, float* a,
                          lapack_int lda, float* b, lapack_int ldb );
lapack_int LAPACKE_dgels( int matrix_layout, char trans, lapack_int m,
                          lapack_int n, lapack_int nrhs, double* a,
                          lapack_int lda, double* b, lapack_int ldb );
lapack_int LAPACKE_cgels( int matrix_layout, char trans, lapack_int m,
                          lapack_int n, lapack_int nrhs,
                          lapack_complex_float* a, lapack_int lda,
                          lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zgels( int matrix_layout, char trans, lapack_int m,
                          lapack_int n, lapack_int nrhs,
                          lapack_complex_double* a, lapack_int lda,
                          lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_sgelsd( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int nrhs, float* a, lapack_int lda, float* b,
                           lapack_int ldb, float* s, float rcond,
                           lapack_int* rank );
lapack_int LAPACKE_dgelsd( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int nrhs, double* a, lapack_int lda,
                           double* b, lapack_int ldb, double* s, double rcond,
                           lapack_int* rank );
lapack_int LAPACKE_cgelsd( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int nrhs, lapack_complex_float* a,
                           lapack_int lda, lapack_complex_float* b,
                           lapack_int ldb, float* s, float rcond,
                           lapack_int* rank );
lapack_int LAPACKE_zgelsd( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int nrhs, lapack_complex_double* a,
                           lapack_int lda, lapack_complex_double* b,
                           lapack_int ldb, double* s, double rcond,
                           lapack_int* rank );

lapack_int LAPACKE_sgelss( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int nrhs, float* a, lapack_int lda, float* b,
                           lapack_int ldb, float* s, float rcond,
                           lapack_int* rank );
lapack_int LAPACKE_dgelss( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int nrhs, double* a, lapack_int lda,
                           double* b, lapack_int ldb, double* s, double rcond,
                           lapack_int* rank );
lapack_int LAPACKE_cgelss( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int nrhs, lapack_complex_float* a,
                           lapack_int lda, lapack_complex_float* b,
                           lapack_int ldb, float* s, float rcond,
                           lapack_int* rank );
lapack_int LAPACKE_zgelss( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int nrhs, lapack_complex_double* a,
                           lapack_int lda, lapack_complex_double* b,
                           lapack_int ldb, double* s, double rcond,
                           lapack_int* rank );

lapack_int LAPACKE_sgelsy( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int nrhs, float* a, lapack_int lda, float* b,
                           lapack_int ldb, lapack_int* jpvt, float rcond,
                           lapack_int* rank );
lapack_int LAPACKE_dgelsy( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int nrhs, double* a, lapack_int lda,
                           double* b, lapack_int ldb, lapack_int* jpvt,
                           double rcond, lapack_int* rank );
lapack_int LAPACKE_cgelsy( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int nrhs, lapack_complex_float* a,
                           lapack_int lda, lapack_complex_float* b,
                           lapack_int ldb, lapack_int* jpvt, float rcond,
                           lapack_int* rank );
lapack_int LAPACKE_zgelsy( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int nrhs, lapack_complex_double* a,
                           lapack_int lda, lapack_complex_double* b,
                           lapack_int ldb, lapack_int* jpvt, double rcond,
                           lapack_int* rank );

lapack_int LAPACKE_sgeqlf( int matrix_layout, lapack_int m, lapack_int n,
                           float* a, lapack_int lda, float* tau );
lapack_int LAPACKE_dgeqlf( int matrix_layout, lapack_int m, lapack_int n,
                           double* a, lapack_int lda, double* tau );
lapack_int LAPACKE_cgeqlf( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_complex_float* tau );
lapack_int LAPACKE_zgeqlf( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_complex_double* tau );

lapack_int LAPACKE_sgeqp3( int matrix_layout, lapack_int m, lapack_int n,
                           float* a, lapack_int lda, lapack_int* jpvt,
                           float* tau );
lapack_int LAPACKE_dgeqp3( int matrix_layout, lapack_int m, lapack_int n,
                           double* a, lapack_int lda, lapack_int* jpvt,
                           double* tau );
lapack_int LAPACKE_cgeqp3( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_int* jpvt, lapack_complex_float* tau );
lapack_int LAPACKE_zgeqp3( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_int* jpvt, lapack_complex_double* tau );

lapack_int LAPACKE_sgeqpf( int matrix_layout, lapack_int m, lapack_int n,
                           float* a, lapack_int lda, lapack_int* jpvt,
                           float* tau );
lapack_int LAPACKE_dgeqpf( int matrix_layout, lapack_int m, lapack_int n,
                           double* a, lapack_int lda, lapack_int* jpvt,
                           double* tau );
lapack_int LAPACKE_cgeqpf( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_int* jpvt, lapack_complex_float* tau );
lapack_int LAPACKE_zgeqpf( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_int* jpvt, lapack_complex_double* tau );

lapack_int LAPACKE_sgeqr2( int matrix_layout, lapack_int m, lapack_int n,
                           float* a, lapack_int lda, float* tau );
lapack_int LAPACKE_dgeqr2( int matrix_layout, lapack_int m, lapack_int n,
                           double* a, lapack_int lda, double* tau );
lapack_int LAPACKE_cgeqr2( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_complex_float* tau );
lapack_int LAPACKE_zgeqr2( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_complex_double* tau );

lapack_int LAPACKE_sgeqrf( int matrix_layout, lapack_int m, lapack_int n,
                           float* a, lapack_int lda, float* tau );
lapack_int LAPACKE_dgeqrf( int matrix_layout, lapack_int m, lapack_int n,
                           double* a, lapack_int lda, double* tau );
lapack_int LAPACKE_cgeqrf( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_complex_float* tau );
lapack_int LAPACKE_zgeqrf( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_complex_double* tau );

lapack_int LAPACKE_sgeqrfp( int matrix_layout, lapack_int m, lapack_int n,
                            float* a, lapack_int lda, float* tau );
lapack_int LAPACKE_dgeqrfp( int matrix_layout, lapack_int m, lapack_int n,
                            double* a, lapack_int lda, double* tau );
lapack_int LAPACKE_cgeqrfp( int matrix_layout, lapack_int m, lapack_int n,
                            lapack_complex_float* a, lapack_int lda,
                            lapack_complex_float* tau );
lapack_int LAPACKE_zgeqrfp( int matrix_layout, lapack_int m, lapack_int n,
                            lapack_complex_double* a, lapack_int lda,
                            lapack_complex_double* tau );

lapack_int LAPACKE_sgerfs( int matrix_layout, char trans, lapack_int n,
                           lapack_int nrhs, const float* a, lapack_int lda,
                           const float* af, lapack_int ldaf,
                           const lapack_int* ipiv, const float* b,
                           lapack_int ldb, float* x, lapack_int ldx,
                           float* ferr, float* berr );
lapack_int LAPACKE_dgerfs( int matrix_layout, char trans, lapack_int n,
                           lapack_int nrhs, const double* a, lapack_int lda,
                           const double* af, lapack_int ldaf,
                           const lapack_int* ipiv, const double* b,
                           lapack_int ldb, double* x, lapack_int ldx,
                           double* ferr, double* berr );
lapack_int LAPACKE_cgerfs( int matrix_layout, char trans, lapack_int n,
                           lapack_int nrhs, const lapack_complex_float* a,
                           lapack_int lda, const lapack_complex_float* af,
                           lapack_int ldaf, const lapack_int* ipiv,
                           const lapack_complex_float* b, lapack_int ldb,
                           lapack_complex_float* x, lapack_int ldx, float* ferr,
                           float* berr );
lapack_int LAPACKE_zgerfs( int matrix_layout, char trans, lapack_int n,
                           lapack_int nrhs, const lapack_complex_double* a,
                           lapack_int lda, const lapack_complex_double* af,
                           lapack_int ldaf, const lapack_int* ipiv,
                           const lapack_complex_double* b, lapack_int ldb,
                           lapack_complex_double* x, lapack_int ldx,
                           double* ferr, double* berr );

lapack_int LAPACKE_sgerfsx( int matrix_layout, char trans, char equed,
                            lapack_int n, lapack_int nrhs, const float* a,
                            lapack_int lda, const float* af, lapack_int ldaf,
                            const lapack_int* ipiv, const float* r,
                            const float* c, const float* b, lapack_int ldb,
                            float* x, lapack_int ldx, float* rcond, float* berr,
                            lapack_int n_err_bnds, float* err_bnds_norm,
                            float* err_bnds_comp, lapack_int nparams,
                            float* params );
lapack_int LAPACKE_dgerfsx( int matrix_layout, char trans, char equed,
                            lapack_int n, lapack_int nrhs, const double* a,
                            lapack_int lda, const double* af, lapack_int ldaf,
                            const lapack_int* ipiv, const double* r,
                            const double* c, const double* b, lapack_int ldb,
                            double* x, lapack_int ldx, double* rcond,
                            double* berr, lapack_int n_err_bnds,
                            double* err_bnds_norm, double* err_bnds_comp,
                            lapack_int nparams, double* params );
lapack_int LAPACKE_cgerfsx( int matrix_layout, char trans, char equed,
                            lapack_int n, lapack_int nrhs,
                            const lapack_complex_float* a, lapack_int lda,
                            const lapack_complex_float* af, lapack_int ldaf,
                            const lapack_int* ipiv, const float* r,
                            const float* c, const lapack_complex_float* b,
                            lapack_int ldb, lapack_complex_float* x,
                            lapack_int ldx, float* rcond, float* berr,
                            lapack_int n_err_bnds, float* err_bnds_norm,
                            float* err_bnds_comp, lapack_int nparams,
                            float* params );
lapack_int LAPACKE_zgerfsx( int matrix_layout, char trans, char equed,
                            lapack_int n, lapack_int nrhs,
                            const lapack_complex_double* a, lapack_int lda,
                            const lapack_complex_double* af, lapack_int ldaf,
                            const lapack_int* ipiv, const double* r,
                            const double* c, const lapack_complex_double* b,
                            lapack_int ldb, lapack_complex_double* x,
                            lapack_int ldx, double* rcond, double* berr,
                            lapack_int n_err_bnds, double* err_bnds_norm,
                            double* err_bnds_comp, lapack_int nparams,
                            double* params );

lapack_int LAPACKE_sgerqf( int matrix_layout, lapack_int m, lapack_int n,
                           float* a, lapack_int lda, float* tau );
lapack_int LAPACKE_dgerqf( int matrix_layout, lapack_int m, lapack_int n,
                           double* a, lapack_int lda, double* tau );
lapack_int LAPACKE_cgerqf( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_complex_float* tau );
lapack_int LAPACKE_zgerqf( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_complex_double* tau );

lapack_int LAPACKE_sgesdd( int matrix_layout, char jobz, lapack_int m,
                           lapack_int n, float* a, lapack_int lda, float* s,
                           float* u, lapack_int ldu, float* vt,
                           lapack_int ldvt );
lapack_int LAPACKE_dgesdd( int matrix_layout, char jobz, lapack_int m,
                           lapack_int n, double* a, lapack_int lda, double* s,
                           double* u, lapack_int ldu, double* vt,
                           lapack_int ldvt );
lapack_int LAPACKE_cgesdd( int matrix_layout, char jobz, lapack_int m,
                           lapack_int n, lapack_complex_float* a,
                           lapack_int lda, float* s, lapack_complex_float* u,
                           lapack_int ldu, lapack_complex_float* vt,
                           lapack_int ldvt );
lapack_int LAPACKE_zgesdd( int matrix_layout, char jobz, lapack_int m,
                           lapack_int n, lapack_complex_double* a,
                           lapack_int lda, double* s, lapack_complex_double* u,
                           lapack_int ldu, lapack_complex_double* vt,
                           lapack_int ldvt );

lapack_int LAPACKE_sgesv( int matrix_layout, lapack_int n, lapack_int nrhs,
                          float* a, lapack_int lda, lapack_int* ipiv, float* b,
                          lapack_int ldb );
lapack_int LAPACKE_dgesv( int matrix_layout, lapack_int n, lapack_int nrhs,
                          double* a, lapack_int lda, lapack_int* ipiv,
                          double* b, lapack_int ldb );
lapack_int LAPACKE_cgesv( int matrix_layout, lapack_int n, lapack_int nrhs,
                          lapack_complex_float* a, lapack_int lda,
                          lapack_int* ipiv, lapack_complex_float* b,
                          lapack_int ldb );
lapack_int LAPACKE_zgesv( int matrix_layout, lapack_int n, lapack_int nrhs,
                          lapack_complex_double* a, lapack_int lda,
                          lapack_int* ipiv, lapack_complex_double* b,
                          lapack_int ldb );
lapack_int LAPACKE_dsgesv( int matrix_layout, lapack_int n, lapack_int nrhs,
                           double* a, lapack_int lda, lapack_int* ipiv,
                           double* b, lapack_int ldb, double* x, lapack_int ldx,
                           lapack_int* iter );
lapack_int LAPACKE_zcgesv( int matrix_layout, lapack_int n, lapack_int nrhs,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_int* ipiv, lapack_complex_double* b,
                           lapack_int ldb, lapack_complex_double* x,
                           lapack_int ldx, lapack_int* iter );

lapack_int LAPACKE_sgesvd( int matrix_layout, char jobu, char jobvt,
                           lapack_int m, lapack_int n, float* a, lapack_int lda,
                           float* s, float* u, lapack_int ldu, float* vt,
                           lapack_int ldvt, float* superb );
lapack_int LAPACKE_dgesvd( int matrix_layout, char jobu, char jobvt,
                           lapack_int m, lapack_int n, double* a,
                           lapack_int lda, double* s, double* u, lapack_int ldu,
                           double* vt, lapack_int ldvt, double* superb );
lapack_int LAPACKE_cgesvd( int matrix_layout, char jobu, char jobvt,
                           lapack_int m, lapack_int n, lapack_complex_float* a,
                           lapack_int lda, float* s, lapack_complex_float* u,
                           lapack_int ldu, lapack_complex_float* vt,
                           lapack_int ldvt, float* superb );
lapack_int LAPACKE_zgesvd( int matrix_layout, char jobu, char jobvt,
                           lapack_int m, lapack_int n, lapack_complex_double* a,
                           lapack_int lda, double* s, lapack_complex_double* u,
                           lapack_int ldu, lapack_complex_double* vt,
                           lapack_int ldvt, double* superb );

lapack_int LAPACKE_sgesvdx( int matrix_layout, char jobu, char jobvt, char range,
                           lapack_int m, lapack_int n, float* a,
                           lapack_int lda, float vl, float vu,
                           lapack_int il, lapack_int iu, lapack_int* ns,
                           float* s, float* u, lapack_int ldu,
                           float* vt, lapack_int ldvt,
                           lapack_int* superb );
lapack_int LAPACKE_dgesvdx( int matrix_layout, char jobu, char jobvt, char range,
                           lapack_int m, lapack_int n, double* a,
                           lapack_int lda, double vl, double vu,
                           lapack_int il, lapack_int iu, lapack_int* ns,
                           double* s, double* u, lapack_int ldu,
                           double* vt, lapack_int ldvt,
                           lapack_int* superb );
lapack_int LAPACKE_cgesvdx( int matrix_layout, char jobu, char jobvt, char range,
                           lapack_int m, lapack_int n, lapack_complex_float* a,
                           lapack_int lda, float vl, float vu,
                           lapack_int il, lapack_int iu, lapack_int* ns,
                           float* s, lapack_complex_float* u, lapack_int ldu,
                           lapack_complex_float* vt, lapack_int ldvt,
                           lapack_int* superb );
lapack_int LAPACKE_zgesvdx( int matrix_layout, char jobu, char jobvt, char range,
                           lapack_int m, lapack_int n, lapack_complex_double* a,
                           lapack_int lda, double vl, double vu,
                           lapack_int il, lapack_int iu, lapack_int* ns,
                           double* s, lapack_complex_double* u, lapack_int ldu,
                           lapack_complex_double* vt, lapack_int ldvt,
                           lapack_int* superb );

lapack_int LAPACKE_sgesvdq( int matrix_layout, char joba, char jobp, char jobr, char jobu, char jobv,
                           lapack_int m, lapack_int n, float* a, lapack_int lda,
                           float* s, float* u, lapack_int ldu, float* v,
                           lapack_int ldv, lapack_int* numrank );
lapack_int LAPACKE_dgesvdq( int matrix_layout, char joba, char jobp, char jobr, char jobu, char jobv,
                           lapack_int m, lapack_int n, double* a,
                           lapack_int lda, double* s, double* u, lapack_int ldu,
                           double* v, lapack_int ldv, lapack_int* numrank);
lapack_int LAPACKE_cgesvdq( int matrix_layout, char joba, char jobp, char jobr, char jobu, char jobv,
                           lapack_int m, lapack_int n, lapack_complex_float* a,
                           lapack_int lda, float* s, lapack_complex_float* u,
                           lapack_int ldu, lapack_complex_float* v,
                           lapack_int ldv, lapack_int* numrank );
lapack_int LAPACKE_zgesvdq( int matrix_layout, char joba, char jobp, char jobr, char jobu, char jobv,
                           lapack_int m, lapack_int n, lapack_complex_double* a,
                           lapack_int lda, double* s, lapack_complex_double* u,
                           lapack_int ldu, lapack_complex_double* v,
                           lapack_int ldv, lapack_int* numrank );

lapack_int LAPACKE_sgesvj( int matrix_layout, char joba, char jobu, char jobv,
                           lapack_int m, lapack_int n, float* a, lapack_int lda,
                           float* sva, lapack_int mv, float* v, lapack_int ldv,
                           float* stat );
lapack_int LAPACKE_dgesvj( int matrix_layout, char joba, char jobu, char jobv,
                           lapack_int m, lapack_int n, double* a,
                           lapack_int lda, double* sva, lapack_int mv,
                           double* v, lapack_int ldv, double* stat );
lapack_int LAPACKE_cgesvj( int matrix_layout, char joba, char jobu, char jobv,
                           lapack_int m, lapack_int n, lapack_complex_float* a,
                           lapack_int lda, float* sva, lapack_int mv,
                           lapack_complex_float* v, lapack_int ldv, float* stat );
lapack_int LAPACKE_zgesvj( int matrix_layout, char joba, char jobu, char jobv,
                           lapack_int m, lapack_int n, lapack_complex_double* a,
                           lapack_int lda, double* sva, lapack_int mv,
                           lapack_complex_double* v, lapack_int ldv, double* stat );

lapack_int LAPACKE_sgesvx( int matrix_layout, char fact, char trans,
                           lapack_int n, lapack_int nrhs, float* a,
                           lapack_int lda, float* af, lapack_int ldaf,
                           lapack_int* ipiv, char* equed, float* r, float* c,
                           float* b, lapack_int ldb, float* x, lapack_int ldx,
                           float* rcond, float* ferr, float* berr,
                           float* rpivot );
lapack_int LAPACKE_dgesvx( int matrix_layout, char fact, char trans,
                           lapack_int n, lapack_int nrhs, double* a,
                           lapack_int lda, double* af, lapack_int ldaf,
                           lapack_int* ipiv, char* equed, double* r, double* c,
                           double* b, lapack_int ldb, double* x, lapack_int ldx,
                           double* rcond, double* ferr, double* berr,
                           double* rpivot );
lapack_int LAPACKE_cgesvx( int matrix_layout, char fact, char trans,
                           lapack_int n, lapack_int nrhs,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_complex_float* af, lapack_int ldaf,
                           lapack_int* ipiv, char* equed, float* r, float* c,
                           lapack_complex_float* b, lapack_int ldb,
                           lapack_complex_float* x, lapack_int ldx,
                           float* rcond, float* ferr, float* berr,
                           float* rpivot );
lapack_int LAPACKE_zgesvx( int matrix_layout, char fact, char trans,
                           lapack_int n, lapack_int nrhs,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_complex_double* af, lapack_int ldaf,
                           lapack_int* ipiv, char* equed, double* r, double* c,
                           lapack_complex_double* b, lapack_int ldb,
                           lapack_complex_double* x, lapack_int ldx,
                           double* rcond, double* ferr, double* berr,
                           double* rpivot );

lapack_int LAPACKE_sgesvxx( int matrix_layout, char fact, char trans,
                            lapack_int n, lapack_int nrhs, float* a,
                            lapack_int lda, float* af, lapack_int ldaf,
                            lapack_int* ipiv, char* equed, float* r, float* c,
                            float* b, lapack_int ldb, float* x, lapack_int ldx,
                            float* rcond, float* rpvgrw, float* berr,
                            lapack_int n_err_bnds, float* err_bnds_norm,
                            float* err_bnds_comp, lapack_int nparams,
                            float* params );
lapack_int LAPACKE_dgesvxx( int matrix_layout, char fact, char trans,
                            lapack_int n, lapack_int nrhs, double* a,
                            lapack_int lda, double* af, lapack_int ldaf,
                            lapack_int* ipiv, char* equed, double* r, double* c,
                            double* b, lapack_int ldb, double* x,
                            lapack_int ldx, double* rcond, double* rpvgrw,
                            double* berr, lapack_int n_err_bnds,
                            double* err_bnds_norm, double* err_bnds_comp,
                            lapack_int nparams, double* params );
lapack_int LAPACKE_cgesvxx( int matrix_layout, char fact, char trans,
                            lapack_int n, lapack_int nrhs,
                            lapack_complex_float* a, lapack_int lda,
                            lapack_complex_float* af, lapack_int ldaf,
                            lapack_int* ipiv, char* equed, float* r, float* c,
                            lapack_complex_float* b, lapack_int ldb,
                            lapack_complex_float* x, lapack_int ldx,
                            float* rcond, float* rpvgrw, float* berr,
                            lapack_int n_err_bnds, float* err_bnds_norm,
                            float* err_bnds_comp, lapack_int nparams,
                            float* params );
lapack_int LAPACKE_zgesvxx( int matrix_layout, char fact, char trans,
                            lapack_int n, lapack_int nrhs,
                            lapack_complex_double* a, lapack_int lda,
                            lapack_complex_double* af, lapack_int ldaf,
                            lapack_int* ipiv, char* equed, double* r, double* c,
                            lapack_complex_double* b, lapack_int ldb,
                            lapack_complex_double* x, lapack_int ldx,
                            double* rcond, double* rpvgrw, double* berr,
                            lapack_int n_err_bnds, double* err_bnds_norm,
                            double* err_bnds_comp, lapack_int nparams,
                            double* params );

lapack_int LAPACKE_sgetf2( int matrix_layout, lapack_int m, lapack_int n,
                           float* a, lapack_int lda, lapack_int* ipiv );
lapack_int LAPACKE_dgetf2( int matrix_layout, lapack_int m, lapack_int n,
                           double* a, lapack_int lda, lapack_int* ipiv );
lapack_int LAPACKE_cgetf2( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_int* ipiv );
lapack_int LAPACKE_zgetf2( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_int* ipiv );

lapack_int LAPACKE_sgetrf( int matrix_layout, lapack_int m, lapack_int n,
                           float* a, lapack_int lda, lapack_int* ipiv );
lapack_int LAPACKE_dgetrf( int matrix_layout, lapack_int m, lapack_int n,
                           double* a, lapack_int lda, lapack_int* ipiv );
lapack_int LAPACKE_cgetrf( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_int* ipiv );
lapack_int LAPACKE_zgetrf( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_int* ipiv );

lapack_int LAPACKE_sgetrf2( int matrix_layout, lapack_int m, lapack_int n,
                           float* a, lapack_int lda, lapack_int* ipiv );
lapack_int LAPACKE_dgetrf2( int matrix_layout, lapack_int m, lapack_int n,
                           double* a, lapack_int lda, lapack_int* ipiv );
lapack_int LAPACKE_cgetrf2( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_int* ipiv );
lapack_int LAPACKE_zgetrf2( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_int* ipiv );

lapack_int LAPACKE_sgetri( int matrix_layout, lapack_int n, float* a,
                           lapack_int lda, const lapack_int* ipiv );
lapack_int LAPACKE_dgetri( int matrix_layout, lapack_int n, double* a,
                           lapack_int lda, const lapack_int* ipiv );
lapack_int LAPACKE_cgetri( int matrix_layout, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           const lapack_int* ipiv );
lapack_int LAPACKE_zgetri( int matrix_layout, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           const lapack_int* ipiv );

lapack_int LAPACKE_sgetrs( int matrix_layout, char trans, lapack_int n,
                           lapack_int nrhs, const float* a, lapack_int lda,
                           const lapack_int* ipiv, float* b, lapack_int ldb );
lapack_int LAPACKE_dgetrs( int matrix_layout, char trans, lapack_int n,
                           lapack_int nrhs, const double* a, lapack_int lda,
                           const lapack_int* ipiv, double* b, lapack_int ldb );
lapack_int LAPACKE_cgetrs( int matrix_layout, char trans, lapack_int n,
                           lapack_int nrhs, const lapack_complex_float* a,
                           lapack_int lda, const lapack_int* ipiv,
                           lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zgetrs( int matrix_layout, char trans, lapack_int n,
                           lapack_int nrhs, const lapack_complex_double* a,
                           lapack_int lda, const lapack_int* ipiv,
                           lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_sggbak( int matrix_layout, char job, char side, lapack_int n,
                           lapack_int ilo, lapack_int ihi, const float* lscale,
                           const float* rscale, lapack_int m, float* v,
                           lapack_int ldv );
lapack_int LAPACKE_dggbak( int matrix_layout, char job, char side, lapack_int n,
                           lapack_int ilo, lapack_int ihi, const double* lscale,
                           const double* rscale, lapack_int m, double* v,
                           lapack_int ldv );
lapack_int LAPACKE_cggbak( int matrix_layout, char job, char side, lapack_int n,
                           lapack_int ilo, lapack_int ihi, const float* lscale,
                           const float* rscale, lapack_int m,
                           lapack_complex_float* v, lapack_int ldv );
lapack_int LAPACKE_zggbak( int matrix_layout, char job, char side, lapack_int n,
                           lapack_int ilo, lapack_int ihi, const double* lscale,
                           const double* rscale, lapack_int m,
                           lapack_complex_double* v, lapack_int ldv );

lapack_int LAPACKE_sggbal( int matrix_layout, char job, lapack_int n, float* a,
                           lapack_int lda, float* b, lapack_int ldb,
                           lapack_int* ilo, lapack_int* ihi, float* lscale,
                           float* rscale );
lapack_int LAPACKE_dggbal( int matrix_layout, char job, lapack_int n, double* a,
                           lapack_int lda, double* b, lapack_int ldb,
                           lapack_int* ilo, lapack_int* ihi, double* lscale,
                           double* rscale );
lapack_int LAPACKE_cggbal( int matrix_layout, char job, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_complex_float* b, lapack_int ldb,
                           lapack_int* ilo, lapack_int* ihi, float* lscale,
                           float* rscale );
lapack_int LAPACKE_zggbal( int matrix_layout, char job, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_complex_double* b, lapack_int ldb,
                           lapack_int* ilo, lapack_int* ihi, double* lscale,
                           double* rscale );

lapack_int LAPACKE_sgges( int matrix_layout, char jobvsl, char jobvsr, char sort,
                          LAPACK_S_SELECT3 selctg, lapack_int n, float* a,
                          lapack_int lda, float* b, lapack_int ldb,
                          lapack_int* sdim, float* alphar, float* alphai,
                          float* beta, float* vsl, lapack_int ldvsl, float* vsr,
                          lapack_int ldvsr );
lapack_int LAPACKE_dgges( int matrix_layout, char jobvsl, char jobvsr, char sort,
                          LAPACK_D_SELECT3 selctg, lapack_int n, double* a,
                          lapack_int lda, double* b, lapack_int ldb,
                          lapack_int* sdim, double* alphar, double* alphai,
                          double* beta, double* vsl, lapack_int ldvsl,
                          double* vsr, lapack_int ldvsr );
lapack_int LAPACKE_cgges( int matrix_layout, char jobvsl, char jobvsr, char sort,
                          LAPACK_C_SELECT2 selctg, lapack_int n,
                          lapack_complex_float* a, lapack_int lda,
                          lapack_complex_float* b, lapack_int ldb,
                          lapack_int* sdim, lapack_complex_float* alpha,
                          lapack_complex_float* beta, lapack_complex_float* vsl,
                          lapack_int ldvsl, lapack_complex_float* vsr,
                          lapack_int ldvsr );
lapack_int LAPACKE_zgges( int matrix_layout, char jobvsl, char jobvsr, char sort,
                          LAPACK_Z_SELECT2 selctg, lapack_int n,
                          lapack_complex_double* a, lapack_int lda,
                          lapack_complex_double* b, lapack_int ldb,
                          lapack_int* sdim, lapack_complex_double* alpha,
                          lapack_complex_double* beta,
                          lapack_complex_double* vsl, lapack_int ldvsl,
                          lapack_complex_double* vsr, lapack_int ldvsr );

lapack_int LAPACKE_sgges3( int matrix_layout, char jobvsl, char jobvsr,
                           char sort, LAPACK_S_SELECT3 selctg, lapack_int n,
                           float* a, lapack_int lda, float* b, lapack_int ldb,
                           lapack_int* sdim, float* alphar, float* alphai,
                           float* beta, float* vsl, lapack_int ldvsl,
                           float* vsr, lapack_int ldvsr );
lapack_int LAPACKE_dgges3( int matrix_layout, char jobvsl, char jobvsr,
                           char sort, LAPACK_D_SELECT3 selctg, lapack_int n,
                           double* a, lapack_int lda, double* b, lapack_int ldb,
                           lapack_int* sdim, double* alphar, double* alphai,
                           double* beta, double* vsl, lapack_int ldvsl,
                           double* vsr, lapack_int ldvsr );
lapack_int LAPACKE_cgges3( int matrix_layout, char jobvsl, char jobvsr,
                           char sort, LAPACK_C_SELECT2 selctg, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_complex_float* b, lapack_int ldb,
                           lapack_int* sdim, lapack_complex_float* alpha,
                           lapack_complex_float* beta,
                           lapack_complex_float* vsl, lapack_int ldvsl,
                           lapack_complex_float* vsr, lapack_int ldvsr );
lapack_int LAPACKE_zgges3( int matrix_layout, char jobvsl, char jobvsr,
                           char sort, LAPACK_Z_SELECT2 selctg, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_complex_double* b, lapack_int ldb,
                           lapack_int* sdim, lapack_complex_double* alpha,
                           lapack_complex_double* beta,
                           lapack_complex_double* vsl, lapack_int ldvsl,
                           lapack_complex_double* vsr, lapack_int ldvsr );

lapack_int LAPACKE_sggesx( int matrix_layout, char jobvsl, char jobvsr,
                           char sort, LAPACK_S_SELECT3 selctg, char sense,
                           lapack_int n, float* a, lapack_int lda, float* b,
                           lapack_int ldb, lapack_int* sdim, float* alphar,
                           float* alphai, float* beta, float* vsl,
                           lapack_int ldvsl, float* vsr, lapack_int ldvsr,
                           float* rconde, float* rcondv );
lapack_int LAPACKE_dggesx( int matrix_layout, char jobvsl, char jobvsr,
                           char sort, LAPACK_D_SELECT3 selctg, char sense,
                           lapack_int n, double* a, lapack_int lda, double* b,
                           lapack_int ldb, lapack_int* sdim, double* alphar,
                           double* alphai, double* beta, double* vsl,
                           lapack_int ldvsl, double* vsr, lapack_int ldvsr,
                           double* rconde, double* rcondv );
lapack_int LAPACKE_cggesx( int matrix_layout, char jobvsl, char jobvsr,
                           char sort, LAPACK_C_SELECT2 selctg, char sense,
                           lapack_int n, lapack_complex_float* a,
                           lapack_int lda, lapack_complex_float* b,
                           lapack_int ldb, lapack_int* sdim,
                           lapack_complex_float* alpha,
                           lapack_complex_float* beta,
                           lapack_complex_float* vsl, lapack_int ldvsl,
                           lapack_complex_float* vsr, lapack_int ldvsr,
                           float* rconde, float* rcondv );
lapack_int LAPACKE_zggesx( int matrix_layout, char jobvsl, char jobvsr,
                           char sort, LAPACK_Z_SELECT2 selctg, char sense,
                           lapack_int n, lapack_complex_double* a,
                           lapack_int lda, lapack_complex_double* b,
                           lapack_int ldb, lapack_int* sdim,
                           lapack_complex_double* alpha,
                           lapack_complex_double* beta,
                           lapack_complex_double* vsl, lapack_int ldvsl,
                           lapack_complex_double* vsr, lapack_int ldvsr,
                           double* rconde, double* rcondv );

lapack_int LAPACKE_sggev( int matrix_layout, char jobvl, char jobvr,
                          lapack_int n, float* a, lapack_int lda, float* b,
                          lapack_int ldb, float* alphar, float* alphai,
                          float* beta, float* vl, lapack_int ldvl, float* vr,
                          lapack_int ldvr );
lapack_int LAPACKE_dggev( int matrix_layout, char jobvl, char jobvr,
                          lapack_int n, double* a, lapack_int lda, double* b,
                          lapack_int ldb, double* alphar, double* alphai,
                          double* beta, double* vl, lapack_int ldvl, double* vr,
                          lapack_int ldvr );
lapack_int LAPACKE_cggev( int matrix_layout, char jobvl, char jobvr,
                          lapack_int n, lapack_complex_float* a, lapack_int lda,
                          lapack_complex_float* b, lapack_int ldb,
                          lapack_complex_float* alpha,
                          lapack_complex_float* beta, lapack_complex_float* vl,
                          lapack_int ldvl, lapack_complex_float* vr,
                          lapack_int ldvr );
lapack_int LAPACKE_zggev( int matrix_layout, char jobvl, char jobvr,
                          lapack_int n, lapack_complex_double* a,
                          lapack_int lda, lapack_complex_double* b,
                          lapack_int ldb, lapack_complex_double* alpha,
                          lapack_complex_double* beta,
                          lapack_complex_double* vl, lapack_int ldvl,
                          lapack_complex_double* vr, lapack_int ldvr );

lapack_int LAPACKE_sggev3( int matrix_layout, char jobvl, char jobvr,
                           lapack_int n, float* a, lapack_int lda,
                           float* b, lapack_int ldb,
                           float* alphar, float* alphai, float* beta,
                           float* vl, lapack_int ldvl,
                           float* vr, lapack_int ldvr );
lapack_int LAPACKE_dggev3( int matrix_layout, char jobvl, char jobvr,
                           lapack_int n, double* a, lapack_int lda,
                           double* b, lapack_int ldb,
                           double* alphar, double* alphai, double* beta,
                           double* vl, lapack_int ldvl,
                           double* vr, lapack_int ldvr );
lapack_int LAPACKE_cggev3( int matrix_layout, char jobvl, char jobvr,
                           lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_complex_float* b, lapack_int ldb,
                           lapack_complex_float* alpha,
                           lapack_complex_float* beta,
                           lapack_complex_float* vl, lapack_int ldvl,
                           lapack_complex_float* vr, lapack_int ldvr );
lapack_int LAPACKE_zggev3( int matrix_layout, char jobvl, char jobvr,
                           lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_complex_double* b, lapack_int ldb,
                           lapack_complex_double* alpha,
                           lapack_complex_double* beta,
                           lapack_complex_double* vl, lapack_int ldvl,
                           lapack_complex_double* vr, lapack_int ldvr );

lapack_int LAPACKE_sggevx( int matrix_layout, char balanc, char jobvl,
                           char jobvr, char sense, lapack_int n, float* a,
                           lapack_int lda, float* b, lapack_int ldb,
                           float* alphar, float* alphai, float* beta, float* vl,
                           lapack_int ldvl, float* vr, lapack_int ldvr,
                           lapack_int* ilo, lapack_int* ihi, float* lscale,
                           float* rscale, float* abnrm, float* bbnrm,
                           float* rconde, float* rcondv );
lapack_int LAPACKE_dggevx( int matrix_layout, char balanc, char jobvl,
                           char jobvr, char sense, lapack_int n, double* a,
                           lapack_int lda, double* b, lapack_int ldb,
                           double* alphar, double* alphai, double* beta,
                           double* vl, lapack_int ldvl, double* vr,
                           lapack_int ldvr, lapack_int* ilo, lapack_int* ihi,
                           double* lscale, double* rscale, double* abnrm,
                           double* bbnrm, double* rconde, double* rcondv );
lapack_int LAPACKE_cggevx( int matrix_layout, char balanc, char jobvl,
                           char jobvr, char sense, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_complex_float* b, lapack_int ldb,
                           lapack_complex_float* alpha,
                           lapack_complex_float* beta, lapack_complex_float* vl,
                           lapack_int ldvl, lapack_complex_float* vr,
                           lapack_int ldvr, lapack_int* ilo, lapack_int* ihi,
                           float* lscale, float* rscale, float* abnrm,
                           float* bbnrm, float* rconde, float* rcondv );
lapack_int LAPACKE_zggevx( int matrix_layout, char balanc, char jobvl,
                           char jobvr, char sense, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_complex_double* b, lapack_int ldb,
                           lapack_complex_double* alpha,
                           lapack_complex_double* beta,
                           lapack_complex_double* vl, lapack_int ldvl,
                           lapack_complex_double* vr, lapack_int ldvr,
                           lapack_int* ilo, lapack_int* ihi, double* lscale,
                           double* rscale, double* abnrm, double* bbnrm,
                           double* rconde, double* rcondv );

lapack_int LAPACKE_sggglm( int matrix_layout, lapack_int n, lapack_int m,
                           lapack_int p, float* a, lapack_int lda, float* b,
                           lapack_int ldb, float* d, float* x, float* y );
lapack_int LAPACKE_dggglm( int matrix_layout, lapack_int n, lapack_int m,
                           lapack_int p, double* a, lapack_int lda, double* b,
                           lapack_int ldb, double* d, double* x, double* y );
lapack_int LAPACKE_cggglm( int matrix_layout, lapack_int n, lapack_int m,
                           lapack_int p, lapack_complex_float* a,
                           lapack_int lda, lapack_complex_float* b,
                           lapack_int ldb, lapack_complex_float* d,
                           lapack_complex_float* x, lapack_complex_float* y );
lapack_int LAPACKE_zggglm( int matrix_layout, lapack_int n, lapack_int m,
                           lapack_int p, lapack_complex_double* a,
                           lapack_int lda, lapack_complex_double* b,
                           lapack_int ldb, lapack_complex_double* d,
                           lapack_complex_double* x, lapack_complex_double* y );

lapack_int LAPACKE_sgghrd( int matrix_layout, char compq, char compz,
                           lapack_int n, lapack_int ilo, lapack_int ihi,
                           float* a, lapack_int lda, float* b, lapack_int ldb,
                           float* q, lapack_int ldq, float* z, lapack_int ldz );
lapack_int LAPACKE_dgghrd( int matrix_layout, char compq, char compz,
                           lapack_int n, lapack_int ilo, lapack_int ihi,
                           double* a, lapack_int lda, double* b, lapack_int ldb,
                           double* q, lapack_int ldq, double* z,
                           lapack_int ldz );
lapack_int LAPACKE_cgghrd( int matrix_layout, char compq, char compz,
                           lapack_int n, lapack_int ilo, lapack_int ihi,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_complex_float* b, lapack_int ldb,
                           lapack_complex_float* q, lapack_int ldq,
                           lapack_complex_float* z, lapack_int ldz );
lapack_int LAPACKE_zgghrd( int matrix_layout, char compq, char compz,
                           lapack_int n, lapack_int ilo, lapack_int ihi,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_complex_double* b, lapack_int ldb,
                           lapack_complex_double* q, lapack_int ldq,
                           lapack_complex_double* z, lapack_int ldz );

lapack_int LAPACKE_sgghd3( int matrix_layout, char compq, char compz,
                           lapack_int n, lapack_int ilo, lapack_int ihi,
                           float* a, lapack_int lda, float* b, lapack_int ldb,
                           float* q, lapack_int ldq, float* z, lapack_int ldz );
lapack_int LAPACKE_dgghd3( int matrix_layout, char compq, char compz,
                           lapack_int n, lapack_int ilo, lapack_int ihi,
                           double* a, lapack_int lda, double* b, lapack_int ldb,
                           double* q, lapack_int ldq, double* z,
                           lapack_int ldz );
lapack_int LAPACKE_cgghd3( int matrix_layout, char compq, char compz,
                           lapack_int n, lapack_int ilo, lapack_int ihi,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_complex_float* b, lapack_int ldb,
                           lapack_complex_float* q, lapack_int ldq,
                           lapack_complex_float* z, lapack_int ldz );
lapack_int LAPACKE_zgghd3( int matrix_layout, char compq, char compz,
                           lapack_int n, lapack_int ilo, lapack_int ihi,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_complex_double* b, lapack_int ldb,
                           lapack_complex_double* q, lapack_int ldq,
                           lapack_complex_double* z, lapack_int ldz );

lapack_int LAPACKE_sgglse( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int p, float* a, lapack_int lda, float* b,
                           lapack_int ldb, float* c, float* d, float* x );
lapack_int LAPACKE_dgglse( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int p, double* a, lapack_int lda, double* b,
                           lapack_int ldb, double* c, double* d, double* x );
lapack_int LAPACKE_cgglse( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int p, lapack_complex_float* a,
                           lapack_int lda, lapack_complex_float* b,
                           lapack_int ldb, lapack_complex_float* c,
                           lapack_complex_float* d, lapack_complex_float* x );
lapack_int LAPACKE_zgglse( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int p, lapack_complex_double* a,
                           lapack_int lda, lapack_complex_double* b,
                           lapack_int ldb, lapack_complex_double* c,
                           lapack_complex_double* d, lapack_complex_double* x );

lapack_int LAPACKE_sggqrf( int matrix_layout, lapack_int n, lapack_int m,
                           lapack_int p, float* a, lapack_int lda, float* taua,
                           float* b, lapack_int ldb, float* taub );
lapack_int LAPACKE_dggqrf( int matrix_layout, lapack_int n, lapack_int m,
                           lapack_int p, double* a, lapack_int lda,
                           double* taua, double* b, lapack_int ldb,
                           double* taub );
lapack_int LAPACKE_cggqrf( int matrix_layout, lapack_int n, lapack_int m,
                           lapack_int p, lapack_complex_float* a,
                           lapack_int lda, lapack_complex_float* taua,
                           lapack_complex_float* b, lapack_int ldb,
                           lapack_complex_float* taub );
lapack_int LAPACKE_zggqrf( int matrix_layout, lapack_int n, lapack_int m,
                           lapack_int p, lapack_complex_double* a,
                           lapack_int lda, lapack_complex_double* taua,
                           lapack_complex_double* b, lapack_int ldb,
                           lapack_complex_double* taub );

lapack_int LAPACKE_sggrqf( int matrix_layout, lapack_int m, lapack_int p,
                           lapack_int n, float* a, lapack_int lda, float* taua,
                           float* b, lapack_int ldb, float* taub );
lapack_int LAPACKE_dggrqf( int matrix_layout, lapack_int m, lapack_int p,
                           lapack_int n, double* a, lapack_int lda,
                           double* taua, double* b, lapack_int ldb,
                           double* taub );
lapack_int LAPACKE_cggrqf( int matrix_layout, lapack_int m, lapack_int p,
                           lapack_int n, lapack_complex_float* a,
                           lapack_int lda, lapack_complex_float* taua,
                           lapack_complex_float* b, lapack_int ldb,
                           lapack_complex_float* taub );
lapack_int LAPACKE_zggrqf( int matrix_layout, lapack_int m, lapack_int p,
                           lapack_int n, lapack_complex_double* a,
                           lapack_int lda, lapack_complex_double* taua,
                           lapack_complex_double* b, lapack_int ldb,
                           lapack_complex_double* taub );

lapack_int LAPACKE_sggsvd( int matrix_layout, char jobu, char jobv, char jobq,
                           lapack_int m, lapack_int n, lapack_int p,
                           lapack_int* k, lapack_int* l, float* a,
                           lapack_int lda, float* b, lapack_int ldb,
                           float* alpha, float* beta, float* u, lapack_int ldu,
                           float* v, lapack_int ldv, float* q, lapack_int ldq,
                           lapack_int* iwork );
lapack_int LAPACKE_dggsvd( int matrix_layout, char jobu, char jobv, char jobq,
                           lapack_int m, lapack_int n, lapack_int p,
                           lapack_int* k, lapack_int* l, double* a,
                           lapack_int lda, double* b, lapack_int ldb,
                           double* alpha, double* beta, double* u,
                           lapack_int ldu, double* v, lapack_int ldv, double* q,
                           lapack_int ldq, lapack_int* iwork );
lapack_int LAPACKE_cggsvd( int matrix_layout, char jobu, char jobv, char jobq,
                           lapack_int m, lapack_int n, lapack_int p,
                           lapack_int* k, lapack_int* l,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_complex_float* b, lapack_int ldb,
                           float* alpha, float* beta, lapack_complex_float* u,
                           lapack_int ldu, lapack_complex_float* v,
                           lapack_int ldv, lapack_complex_float* q,
                           lapack_int ldq, lapack_int* iwork );
lapack_int LAPACKE_zggsvd( int matrix_layout, char jobu, char jobv, char jobq,
                           lapack_int m, lapack_int n, lapack_int p,
                           lapack_int* k, lapack_int* l,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_complex_double* b, lapack_int ldb,
                           double* alpha, double* beta,
                           lapack_complex_double* u, lapack_int ldu,
                           lapack_complex_double* v, lapack_int ldv,
                           lapack_complex_double* q, lapack_int ldq,
                           lapack_int* iwork );

lapack_int LAPACKE_sggsvd3( int matrix_layout, char jobu, char jobv, char jobq,
                            lapack_int m, lapack_int n, lapack_int p,
                            lapack_int* k, lapack_int* l, float* a,
                            lapack_int lda, float* b, lapack_int ldb,
                            float* alpha, float* beta, float* u, lapack_int ldu,
                            float* v, lapack_int ldv, float* q, lapack_int ldq,
                            lapack_int* iwork );
lapack_int LAPACKE_dggsvd3( int matrix_layout, char jobu, char jobv, char jobq,
                            lapack_int m, lapack_int n, lapack_int p,
                            lapack_int* k, lapack_int* l, double* a,
                            lapack_int lda, double* b, lapack_int ldb,
                            double* alpha, double* beta, double* u,
                            lapack_int ldu, double* v, lapack_int ldv, double* q,
                            lapack_int ldq, lapack_int* iwork );
lapack_int LAPACKE_cggsvd3( int matrix_layout, char jobu, char jobv, char jobq,
                            lapack_int m, lapack_int n, lapack_int p,
                            lapack_int* k, lapack_int* l,
                            lapack_complex_float* a, lapack_int lda,
                            lapack_complex_float* b, lapack_int ldb,
                            float* alpha, float* beta, lapack_complex_float* u,
                            lapack_int ldu, lapack_complex_float* v,
                            lapack_int ldv, lapack_complex_float* q,
                            lapack_int ldq, lapack_int* iwork );
lapack_int LAPACKE_zggsvd3( int matrix_layout, char jobu, char jobv, char jobq,
                            lapack_int m, lapack_int n, lapack_int p,
                            lapack_int* k, lapack_int* l,
                            lapack_complex_double* a, lapack_int lda,
                            lapack_complex_double* b, lapack_int ldb,
                            double* alpha, double* beta,
                            lapack_complex_double* u, lapack_int ldu,
                            lapack_complex_double* v, lapack_int ldv,
                            lapack_complex_double* q, lapack_int ldq,
                            lapack_int* iwork );

lapack_int LAPACKE_sggsvp( int matrix_layout, char jobu, char jobv, char jobq,
                           lapack_int m, lapack_int p, lapack_int n, float* a,
                           lapack_int lda, float* b, lapack_int ldb, float tola,
                           float tolb, lapack_int* k, lapack_int* l, float* u,
                           lapack_int ldu, float* v, lapack_int ldv, float* q,
                           lapack_int ldq );
lapack_int LAPACKE_dggsvp( int matrix_layout, char jobu, char jobv, char jobq,
                           lapack_int m, lapack_int p, lapack_int n, double* a,
                           lapack_int lda, double* b, lapack_int ldb,
                           double tola, double tolb, lapack_int* k,
                           lapack_int* l, double* u, lapack_int ldu, double* v,
                           lapack_int ldv, double* q, lapack_int ldq );
lapack_int LAPACKE_cggsvp( int matrix_layout, char jobu, char jobv, char jobq,
                           lapack_int m, lapack_int p, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_complex_float* b, lapack_int ldb, float tola,
                           float tolb, lapack_int* k, lapack_int* l,
                           lapack_complex_float* u, lapack_int ldu,
                           lapack_complex_float* v, lapack_int ldv,
                           lapack_complex_float* q, lapack_int ldq );
lapack_int LAPACKE_zggsvp( int matrix_layout, char jobu, char jobv, char jobq,
                           lapack_int m, lapack_int p, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_complex_double* b, lapack_int ldb,
                           double tola, double tolb, lapack_int* k,
                           lapack_int* l, lapack_complex_double* u,
                           lapack_int ldu, lapack_complex_double* v,
                           lapack_int ldv, lapack_complex_double* q,
                           lapack_int ldq );

lapack_int LAPACKE_sggsvp3( int matrix_layout, char jobu, char jobv, char jobq,
                            lapack_int m, lapack_int p, lapack_int n, float* a,
                            lapack_int lda, float* b, lapack_int ldb, float tola,
                            float tolb, lapack_int* k, lapack_int* l, float* u,
                            lapack_int ldu, float* v, lapack_int ldv, float* q,
                            lapack_int ldq );
lapack_int LAPACKE_dggsvp3( int matrix_layout, char jobu, char jobv, char jobq,
                            lapack_int m, lapack_int p, lapack_int n, double* a,
                            lapack_int lda, double* b, lapack_int ldb,
                            double tola, double tolb, lapack_int* k,
                            lapack_int* l, double* u, lapack_int ldu, double* v,
                            lapack_int ldv, double* q, lapack_int ldq );
lapack_int LAPACKE_cggsvp3( int matrix_layout, char jobu, char jobv, char jobq,
                            lapack_int m, lapack_int p, lapack_int n,
                            lapack_complex_float* a, lapack_int lda,
                            lapack_complex_float* b, lapack_int ldb, float tola,
                            float tolb, lapack_int* k, lapack_int* l,
                            lapack_complex_float* u, lapack_int ldu,
                            lapack_complex_float* v, lapack_int ldv,
                            lapack_complex_float* q, lapack_int ldq );
lapack_int LAPACKE_zggsvp3( int matrix_layout, char jobu, char jobv, char jobq,
                            lapack_int m, lapack_int p, lapack_int n,
                            lapack_complex_double* a, lapack_int lda,
                            lapack_complex_double* b, lapack_int ldb,
                            double tola, double tolb, lapack_int* k,
                            lapack_int* l, lapack_complex_double* u,
                            lapack_int ldu, lapack_complex_double* v,
                            lapack_int ldv, lapack_complex_double* q,
                            lapack_int ldq );

lapack_int LAPACKE_sgtcon( char norm, lapack_int n, const float* dl,
                           const float* d, const float* du, const float* du2,
                           const lapack_int* ipiv, float anorm, float* rcond );
lapack_int LAPACKE_dgtcon( char norm, lapack_int n, const double* dl,
                           const double* d, const double* du, const double* du2,
                           const lapack_int* ipiv, double anorm,
                           double* rcond );
lapack_int LAPACKE_cgtcon( char norm, lapack_int n,
                           const lapack_complex_float* dl,
                           const lapack_complex_float* d,
                           const lapack_complex_float* du,
                           const lapack_complex_float* du2,
                           const lapack_int* ipiv, float anorm, float* rcond );
lapack_int LAPACKE_zgtcon( char norm, lapack_int n,
                           const lapack_complex_double* dl,
                           const lapack_complex_double* d,
                           const lapack_complex_double* du,
                           const lapack_complex_double* du2,
                           const lapack_int* ipiv, double anorm,
                           double* rcond );

lapack_int LAPACKE_sgtrfs( int matrix_layout, char trans, lapack_int n,
                           lapack_int nrhs, const float* dl, const float* d,
                           const float* du, const float* dlf, const float* df,
                           const float* duf, const float* du2,
                           const lapack_int* ipiv, const float* b,
                           lapack_int ldb, float* x, lapack_int ldx,
                           float* ferr, float* berr );
lapack_int LAPACKE_dgtrfs( int matrix_layout, char trans, lapack_int n,
                           lapack_int nrhs, const double* dl, const double* d,
                           const double* du, const double* dlf,
                           const double* df, const double* duf,
                           const double* du2, const lapack_int* ipiv,
                           const double* b, lapack_int ldb, double* x,
                           lapack_int ldx, double* ferr, double* berr );
lapack_int LAPACKE_cgtrfs( int matrix_layout, char trans, lapack_int n,
                           lapack_int nrhs, const lapack_complex_float* dl,
                           const lapack_complex_float* d,
                           const lapack_complex_float* du,
                           const lapack_complex_float* dlf,
                           const lapack_complex_float* df,
                           const lapack_complex_float* duf,
                           const lapack_complex_float* du2,
                           const lapack_int* ipiv,
                           const lapack_complex_float* b, lapack_int ldb,
                           lapack_complex_float* x, lapack_int ldx, float* ferr,
                           float* berr );
lapack_int LAPACKE_zgtrfs( int matrix_layout, char trans, lapack_int n,
                           lapack_int nrhs, const lapack_complex_double* dl,
                           const lapack_complex_double* d,
                           const lapack_complex_double* du,
                           const lapack_complex_double* dlf,
                           const lapack_complex_double* df,
                           const lapack_complex_double* duf,
                           const lapack_complex_double* du2,
                           const lapack_int* ipiv,
                           const lapack_complex_double* b, lapack_int ldb,
                           lapack_complex_double* x, lapack_int ldx,
                           double* ferr, double* berr );

lapack_int LAPACKE_sgtsv( int matrix_layout, lapack_int n, lapack_int nrhs,
                          float* dl, float* d, float* du, float* b,
                          lapack_int ldb );
lapack_int LAPACKE_dgtsv( int matrix_layout, lapack_int n, lapack_int nrhs,
                          double* dl, double* d, double* du, double* b,
                          lapack_int ldb );
lapack_int LAPACKE_cgtsv( int matrix_layout, lapack_int n, lapack_int nrhs,
                          lapack_complex_float* dl, lapack_complex_float* d,
                          lapack_complex_float* du, lapack_complex_float* b,
                          lapack_int ldb );
lapack_int LAPACKE_zgtsv( int matrix_layout, lapack_int n, lapack_int nrhs,
                          lapack_complex_double* dl, lapack_complex_double* d,
                          lapack_complex_double* du, lapack_complex_double* b,
                          lapack_int ldb );

lapack_int LAPACKE_sgtsvx( int matrix_layout, char fact, char trans,
                           lapack_int n, lapack_int nrhs, const float* dl,
                           const float* d, const float* du, float* dlf,
                           float* df, float* duf, float* du2, lapack_int* ipiv,
                           const float* b, lapack_int ldb, float* x,
                           lapack_int ldx, float* rcond, float* ferr,
                           float* berr );
lapack_int LAPACKE_dgtsvx( int matrix_layout, char fact, char trans,
                           lapack_int n, lapack_int nrhs, const double* dl,
                           const double* d, const double* du, double* dlf,
                           double* df, double* duf, double* du2,
                           lapack_int* ipiv, const double* b, lapack_int ldb,
                           double* x, lapack_int ldx, double* rcond,
                           double* ferr, double* berr );
lapack_int LAPACKE_cgtsvx( int matrix_layout, char fact, char trans,
                           lapack_int n, lapack_int nrhs,
                           const lapack_complex_float* dl,
                           const lapack_complex_float* d,
                           const lapack_complex_float* du,
                           lapack_complex_float* dlf, lapack_complex_float* df,
                           lapack_complex_float* duf, lapack_complex_float* du2,
                           lapack_int* ipiv, const lapack_complex_float* b,
                           lapack_int ldb, lapack_complex_float* x,
                           lapack_int ldx, float* rcond, float* ferr,
                           float* berr );
lapack_int LAPACKE_zgtsvx( int matrix_layout, char fact, char trans,
                           lapack_int n, lapack_int nrhs,
                           const lapack_complex_double* dl,
                           const lapack_complex_double* d,
                           const lapack_complex_double* du,
                           lapack_complex_double* dlf,
                           lapack_complex_double* df,
                           lapack_complex_double* duf,
                           lapack_complex_double* du2, lapack_int* ipiv,
                           const lapack_complex_double* b, lapack_int ldb,
                           lapack_complex_double* x, lapack_int ldx,
                           double* rcond, double* ferr, double* berr );

lapack_int LAPACKE_sgttrf( lapack_int n, float* dl, float* d, float* du,
                           float* du2, lapack_int* ipiv );
lapack_int LAPACKE_dgttrf( lapack_int n, double* dl, double* d, double* du,
                           double* du2, lapack_int* ipiv );
lapack_int LAPACKE_cgttrf( lapack_int n, lapack_complex_float* dl,
                           lapack_complex_float* d, lapack_complex_float* du,
                           lapack_complex_float* du2, lapack_int* ipiv );
lapack_int LAPACKE_zgttrf( lapack_int n, lapack_complex_double* dl,
                           lapack_complex_double* d, lapack_complex_double* du,
                           lapack_complex_double* du2, lapack_int* ipiv );

lapack_int LAPACKE_sgttrs( int matrix_layout, char trans, lapack_int n,
                           lapack_int nrhs, const float* dl, const float* d,
                           const float* du, const float* du2,
                           const lapack_int* ipiv, float* b, lapack_int ldb );
lapack_int LAPACKE_dgttrs( int matrix_layout, char trans, lapack_int n,
                           lapack_int nrhs, const double* dl, const double* d,
                           const double* du, const double* du2,
                           const lapack_int* ipiv, double* b, lapack_int ldb );
lapack_int LAPACKE_cgttrs( int matrix_layout, char trans, lapack_int n,
                           lapack_int nrhs, const lapack_complex_float* dl,
                           const lapack_complex_float* d,
                           const lapack_complex_float* du,
                           const lapack_complex_float* du2,
                           const lapack_int* ipiv, lapack_complex_float* b,
                           lapack_int ldb );
lapack_int LAPACKE_zgttrs( int matrix_layout, char trans, lapack_int n,
                           lapack_int nrhs, const lapack_complex_double* dl,
                           const lapack_complex_double* d,
                           const lapack_complex_double* du,
                           const lapack_complex_double* du2,
                           const lapack_int* ipiv, lapack_complex_double* b,
                           lapack_int ldb );

lapack_int LAPACKE_chbev( int matrix_layout, char jobz, char uplo, lapack_int n,
                          lapack_int kd, lapack_complex_float* ab,
                          lapack_int ldab, float* w, lapack_complex_float* z,
                          lapack_int ldz );
lapack_int LAPACKE_zhbev( int matrix_layout, char jobz, char uplo, lapack_int n,
                          lapack_int kd, lapack_complex_double* ab,
                          lapack_int ldab, double* w, lapack_complex_double* z,
                          lapack_int ldz );

lapack_int LAPACKE_chbevd( int matrix_layout, char jobz, char uplo, lapack_int n,
                           lapack_int kd, lapack_complex_float* ab,
                           lapack_int ldab, float* w, lapack_complex_float* z,
                           lapack_int ldz );
lapack_int LAPACKE_zhbevd( int matrix_layout, char jobz, char uplo, lapack_int n,
                           lapack_int kd, lapack_complex_double* ab,
                           lapack_int ldab, double* w, lapack_complex_double* z,
                           lapack_int ldz );

lapack_int LAPACKE_chbevx( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, lapack_int kd,
                           lapack_complex_float* ab, lapack_int ldab,
                           lapack_complex_float* q, lapack_int ldq, float vl,
                           float vu, lapack_int il, lapack_int iu, float abstol,
                           lapack_int* m, float* w, lapack_complex_float* z,
                           lapack_int ldz, lapack_int* ifail );
lapack_int LAPACKE_zhbevx( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, lapack_int kd,
                           lapack_complex_double* ab, lapack_int ldab,
                           lapack_complex_double* q, lapack_int ldq, double vl,
                           double vu, lapack_int il, lapack_int iu,
                           double abstol, lapack_int* m, double* w,
                           lapack_complex_double* z, lapack_int ldz,
                           lapack_int* ifail );

lapack_int LAPACKE_chbgst( int matrix_layout, char vect, char uplo, lapack_int n,
                           lapack_int ka, lapack_int kb,
                           lapack_complex_float* ab, lapack_int ldab,
                           const lapack_complex_float* bb, lapack_int ldbb,
                           lapack_complex_float* x, lapack_int ldx );
lapack_int LAPACKE_zhbgst( int matrix_layout, char vect, char uplo, lapack_int n,
                           lapack_int ka, lapack_int kb,
                           lapack_complex_double* ab, lapack_int ldab,
                           const lapack_complex_double* bb, lapack_int ldbb,
                           lapack_complex_double* x, lapack_int ldx );

lapack_int LAPACKE_chbgv( int matrix_layout, char jobz, char uplo, lapack_int n,
                          lapack_int ka, lapack_int kb,
                          lapack_complex_float* ab, lapack_int ldab,
                          lapack_complex_float* bb, lapack_int ldbb, float* w,
                          lapack_complex_float* z, lapack_int ldz );
lapack_int LAPACKE_zhbgv( int matrix_layout, char jobz, char uplo, lapack_int n,
                          lapack_int ka, lapack_int kb,
                          lapack_complex_double* ab, lapack_int ldab,
                          lapack_complex_double* bb, lapack_int ldbb, double* w,
                          lapack_complex_double* z, lapack_int ldz );

lapack_int LAPACKE_chbgvd( int matrix_layout, char jobz, char uplo, lapack_int n,
                           lapack_int ka, lapack_int kb,
                           lapack_complex_float* ab, lapack_int ldab,
                           lapack_complex_float* bb, lapack_int ldbb, float* w,
                           lapack_complex_float* z, lapack_int ldz );
lapack_int LAPACKE_zhbgvd( int matrix_layout, char jobz, char uplo, lapack_int n,
                           lapack_int ka, lapack_int kb,
                           lapack_complex_double* ab, lapack_int ldab,
                           lapack_complex_double* bb, lapack_int ldbb,
                           double* w, lapack_complex_double* z,
                           lapack_int ldz );

lapack_int LAPACKE_chbgvx( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, lapack_int ka, lapack_int kb,
                           lapack_complex_float* ab, lapack_int ldab,
                           lapack_complex_float* bb, lapack_int ldbb,
                           lapack_complex_float* q, lapack_int ldq, float vl,
                           float vu, lapack_int il, lapack_int iu, float abstol,
                           lapack_int* m, float* w, lapack_complex_float* z,
                           lapack_int ldz, lapack_int* ifail );
lapack_int LAPACKE_zhbgvx( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, lapack_int ka, lapack_int kb,
                           lapack_complex_double* ab, lapack_int ldab,
                           lapack_complex_double* bb, lapack_int ldbb,
                           lapack_complex_double* q, lapack_int ldq, double vl,
                           double vu, lapack_int il, lapack_int iu,
                           double abstol, lapack_int* m, double* w,
                           lapack_complex_double* z, lapack_int ldz,
                           lapack_int* ifail );

lapack_int LAPACKE_chbtrd( int matrix_layout, char vect, char uplo, lapack_int n,
                           lapack_int kd, lapack_complex_float* ab,
                           lapack_int ldab, float* d, float* e,
                           lapack_complex_float* q, lapack_int ldq );
lapack_int LAPACKE_zhbtrd( int matrix_layout, char vect, char uplo, lapack_int n,
                           lapack_int kd, lapack_complex_double* ab,
                           lapack_int ldab, double* d, double* e,
                           lapack_complex_double* q, lapack_int ldq );

lapack_int LAPACKE_checon( int matrix_layout, char uplo, lapack_int n,
                           const lapack_complex_float* a, lapack_int lda,
                           const lapack_int* ipiv, float anorm, float* rcond );
lapack_int LAPACKE_zhecon( int matrix_layout, char uplo, lapack_int n,
                           const lapack_complex_double* a, lapack_int lda,
                           const lapack_int* ipiv, double anorm,
                           double* rcond );

lapack_int LAPACKE_cheequb( int matrix_layout, char uplo, lapack_int n,
                            const lapack_complex_float* a, lapack_int lda,
                            float* s, float* scond, float* amax );
lapack_int LAPACKE_zheequb( int matrix_layout, char uplo, lapack_int n,
                            const lapack_complex_double* a, lapack_int lda,
                            double* s, double* scond, double* amax );

lapack_int LAPACKE_cheev( int matrix_layout, char jobz, char uplo, lapack_int n,
                          lapack_complex_float* a, lapack_int lda, float* w );
lapack_int LAPACKE_zheev( int matrix_layout, char jobz, char uplo, lapack_int n,
                          lapack_complex_double* a, lapack_int lda, double* w );

lapack_int LAPACKE_cheevd( int matrix_layout, char jobz, char uplo, lapack_int n,
                           lapack_complex_float* a, lapack_int lda, float* w );
lapack_int LAPACKE_zheevd( int matrix_layout, char jobz, char uplo, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           double* w );

lapack_int LAPACKE_cheevr( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, lapack_complex_float* a,
                           lapack_int lda, float vl, float vu, lapack_int il,
                           lapack_int iu, float abstol, lapack_int* m, float* w,
                           lapack_complex_float* z, lapack_int ldz,
                           lapack_int* isuppz );
lapack_int LAPACKE_zheevr( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, lapack_complex_double* a,
                           lapack_int lda, double vl, double vu, lapack_int il,
                           lapack_int iu, double abstol, lapack_int* m,
                           double* w, lapack_complex_double* z, lapack_int ldz,
                           lapack_int* isuppz );

lapack_int LAPACKE_cheevx( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, lapack_complex_float* a,
                           lapack_int lda, float vl, float vu, lapack_int il,
                           lapack_int iu, float abstol, lapack_int* m, float* w,
                           lapack_complex_float* z, lapack_int ldz,
                           lapack_int* ifail );
lapack_int LAPACKE_zheevx( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, lapack_complex_double* a,
                           lapack_int lda, double vl, double vu, lapack_int il,
                           lapack_int iu, double abstol, lapack_int* m,
                           double* w, lapack_complex_double* z, lapack_int ldz,
                           lapack_int* ifail );

lapack_int LAPACKE_chegst( int matrix_layout, lapack_int itype, char uplo,
                           lapack_int n, lapack_complex_float* a,
                           lapack_int lda, const lapack_complex_float* b,
                           lapack_int ldb );
lapack_int LAPACKE_zhegst( int matrix_layout, lapack_int itype, char uplo,
                           lapack_int n, lapack_complex_double* a,
                           lapack_int lda, const lapack_complex_double* b,
                           lapack_int ldb );

lapack_int LAPACKE_chegv( int matrix_layout, lapack_int itype, char jobz,
                          char uplo, lapack_int n, lapack_complex_float* a,
                          lapack_int lda, lapack_complex_float* b,
                          lapack_int ldb, float* w );
lapack_int LAPACKE_zhegv( int matrix_layout, lapack_int itype, char jobz,
                          char uplo, lapack_int n, lapack_complex_double* a,
                          lapack_int lda, lapack_complex_double* b,
                          lapack_int ldb, double* w );

lapack_int LAPACKE_chegvd( int matrix_layout, lapack_int itype, char jobz,
                           char uplo, lapack_int n, lapack_complex_float* a,
                           lapack_int lda, lapack_complex_float* b,
                           lapack_int ldb, float* w );
lapack_int LAPACKE_zhegvd( int matrix_layout, lapack_int itype, char jobz,
                           char uplo, lapack_int n, lapack_complex_double* a,
                           lapack_int lda, lapack_complex_double* b,
                           lapack_int ldb, double* w );

lapack_int LAPACKE_chegvx( int matrix_layout, lapack_int itype, char jobz,
                           char range, char uplo, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_complex_float* b, lapack_int ldb, float vl,
                           float vu, lapack_int il, lapack_int iu, float abstol,
                           lapack_int* m, float* w, lapack_complex_float* z,
                           lapack_int ldz, lapack_int* ifail );
lapack_int LAPACKE_zhegvx( int matrix_layout, lapack_int itype, char jobz,
                           char range, char uplo, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_complex_double* b, lapack_int ldb, double vl,
                           double vu, lapack_int il, lapack_int iu,
                           double abstol, lapack_int* m, double* w,
                           lapack_complex_double* z, lapack_int ldz,
                           lapack_int* ifail );

lapack_int LAPACKE_cherfs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_float* a,
                           lapack_int lda, const lapack_complex_float* af,
                           lapack_int ldaf, const lapack_int* ipiv,
                           const lapack_complex_float* b, lapack_int ldb,
                           lapack_complex_float* x, lapack_int ldx, float* ferr,
                           float* berr );
lapack_int LAPACKE_zherfs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_double* a,
                           lapack_int lda, const lapack_complex_double* af,
                           lapack_int ldaf, const lapack_int* ipiv,
                           const lapack_complex_double* b, lapack_int ldb,
                           lapack_complex_double* x, lapack_int ldx,
                           double* ferr, double* berr );

lapack_int LAPACKE_cherfsx( int matrix_layout, char uplo, char equed,
                            lapack_int n, lapack_int nrhs,
                            const lapack_complex_float* a, lapack_int lda,
                            const lapack_complex_float* af, lapack_int ldaf,
                            const lapack_int* ipiv, const float* s,
                            const lapack_complex_float* b, lapack_int ldb,
                            lapack_complex_float* x, lapack_int ldx,
                            float* rcond, float* berr, lapack_int n_err_bnds,
                            float* err_bnds_norm, float* err_bnds_comp,
                            lapack_int nparams, float* params );
lapack_int LAPACKE_zherfsx( int matrix_layout, char uplo, char equed,
                            lapack_int n, lapack_int nrhs,
                            const lapack_complex_double* a, lapack_int lda,
                            const lapack_complex_double* af, lapack_int ldaf,
                            const lapack_int* ipiv, const double* s,
                            const lapack_complex_double* b, lapack_int ldb,
                            lapack_complex_double* x, lapack_int ldx,
                            double* rcond, double* berr, lapack_int n_err_bnds,
                            double* err_bnds_norm, double* err_bnds_comp,
                            lapack_int nparams, double* params );

lapack_int LAPACKE_chesv( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, lapack_complex_float* a,
                          lapack_int lda, lapack_int* ipiv,
                          lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zhesv( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, lapack_complex_double* a,
                          lapack_int lda, lapack_int* ipiv,
                          lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_chesvx( int matrix_layout, char fact, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_float* a,
                           lapack_int lda, lapack_complex_float* af,
                           lapack_int ldaf, lapack_int* ipiv,
                           const lapack_complex_float* b, lapack_int ldb,
                           lapack_complex_float* x, lapack_int ldx,
                           float* rcond, float* ferr, float* berr );
lapack_int LAPACKE_zhesvx( int matrix_layout, char fact, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_double* a,
                           lapack_int lda, lapack_complex_double* af,
                           lapack_int ldaf, lapack_int* ipiv,
                           const lapack_complex_double* b, lapack_int ldb,
                           lapack_complex_double* x, lapack_int ldx,
                           double* rcond, double* ferr, double* berr );

lapack_int LAPACKE_chesvxx( int matrix_layout, char fact, char uplo,
                            lapack_int n, lapack_int nrhs,
                            lapack_complex_float* a, lapack_int lda,
                            lapack_complex_float* af, lapack_int ldaf,
                            lapack_int* ipiv, char* equed, float* s,
                            lapack_complex_float* b, lapack_int ldb,
                            lapack_complex_float* x, lapack_int ldx,
                            float* rcond, float* rpvgrw, float* berr,
                            lapack_int n_err_bnds, float* err_bnds_norm,
                            float* err_bnds_comp, lapack_int nparams,
                            float* params );
lapack_int LAPACKE_zhesvxx( int matrix_layout, char fact, char uplo,
                            lapack_int n, lapack_int nrhs,
                            lapack_complex_double* a, lapack_int lda,
                            lapack_complex_double* af, lapack_int ldaf,
                            lapack_int* ipiv, char* equed, double* s,
                            lapack_complex_double* b, lapack_int ldb,
                            lapack_complex_double* x, lapack_int ldx,
                            double* rcond, double* rpvgrw, double* berr,
                            lapack_int n_err_bnds, double* err_bnds_norm,
                            double* err_bnds_comp, lapack_int nparams,
                            double* params );

lapack_int LAPACKE_chetrd( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_float* a, lapack_int lda, float* d,
                           float* e, lapack_complex_float* tau );
lapack_int LAPACKE_zhetrd( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_double* a, lapack_int lda, double* d,
                           double* e, lapack_complex_double* tau );

lapack_int LAPACKE_chetrf( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_int* ipiv );
lapack_int LAPACKE_zhetrf( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_int* ipiv );

lapack_int LAPACKE_chetri( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           const lapack_int* ipiv );
lapack_int LAPACKE_zhetri( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           const lapack_int* ipiv );

lapack_int LAPACKE_chetrs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_float* a,
                           lapack_int lda, const lapack_int* ipiv,
                           lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zhetrs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_double* a,
                           lapack_int lda, const lapack_int* ipiv,
                           lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_chfrk( int matrix_layout, char transr, char uplo, char trans,
                          lapack_int n, lapack_int k, float alpha,
                          const lapack_complex_float* a, lapack_int lda,
                          float beta, lapack_complex_float* c );
lapack_int LAPACKE_zhfrk( int matrix_layout, char transr, char uplo, char trans,
                          lapack_int n, lapack_int k, double alpha,
                          const lapack_complex_double* a, lapack_int lda,
                          double beta, lapack_complex_double* c );

lapack_int LAPACKE_shgeqz( int matrix_layout, char job, char compq, char compz,
                           lapack_int n, lapack_int ilo, lapack_int ihi,
                           float* h, lapack_int ldh, float* t, lapack_int ldt,
                           float* alphar, float* alphai, float* beta, float* q,
                           lapack_int ldq, float* z, lapack_int ldz );
lapack_int LAPACKE_dhgeqz( int matrix_layout, char job, char compq, char compz,
                           lapack_int n, lapack_int ilo, lapack_int ihi,
                           double* h, lapack_int ldh, double* t, lapack_int ldt,
                           double* alphar, double* alphai, double* beta,
                           double* q, lapack_int ldq, double* z,
                           lapack_int ldz );
lapack_int LAPACKE_chgeqz( int matrix_layout, char job, char compq, char compz,
                           lapack_int n, lapack_int ilo, lapack_int ihi,
                           lapack_complex_float* h, lapack_int ldh,
                           lapack_complex_float* t, lapack_int ldt,
                           lapack_complex_float* alpha,
                           lapack_complex_float* beta, lapack_complex_float* q,
                           lapack_int ldq, lapack_complex_float* z,
                           lapack_int ldz );
lapack_int LAPACKE_zhgeqz( int matrix_layout, char job, char compq, char compz,
                           lapack_int n, lapack_int ilo, lapack_int ihi,
                           lapack_complex_double* h, lapack_int ldh,
                           lapack_complex_double* t, lapack_int ldt,
                           lapack_complex_double* alpha,
                           lapack_complex_double* beta,
                           lapack_complex_double* q, lapack_int ldq,
                           lapack_complex_double* z, lapack_int ldz );

lapack_int LAPACKE_chpcon( int matrix_layout, char uplo, lapack_int n,
                           const lapack_complex_float* ap,
                           const lapack_int* ipiv, float anorm, float* rcond );
lapack_int LAPACKE_zhpcon( int matrix_layout, char uplo, lapack_int n,
                           const lapack_complex_double* ap,
                           const lapack_int* ipiv, double anorm,
                           double* rcond );

lapack_int LAPACKE_chpev( int matrix_layout, char jobz, char uplo, lapack_int n,
                          lapack_complex_float* ap, float* w,
                          lapack_complex_float* z, lapack_int ldz );
lapack_int LAPACKE_zhpev( int matrix_layout, char jobz, char uplo, lapack_int n,
                          lapack_complex_double* ap, double* w,
                          lapack_complex_double* z, lapack_int ldz );

lapack_int LAPACKE_chpevd( int matrix_layout, char jobz, char uplo, lapack_int n,
                           lapack_complex_float* ap, float* w,
                           lapack_complex_float* z, lapack_int ldz );
lapack_int LAPACKE_zhpevd( int matrix_layout, char jobz, char uplo, lapack_int n,
                           lapack_complex_double* ap, double* w,
                           lapack_complex_double* z, lapack_int ldz );

lapack_int LAPACKE_chpevx( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, lapack_complex_float* ap, float vl,
                           float vu, lapack_int il, lapack_int iu, float abstol,
                           lapack_int* m, float* w, lapack_complex_float* z,
                           lapack_int ldz, lapack_int* ifail );
lapack_int LAPACKE_zhpevx( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, lapack_complex_double* ap, double vl,
                           double vu, lapack_int il, lapack_int iu,
                           double abstol, lapack_int* m, double* w,
                           lapack_complex_double* z, lapack_int ldz,
                           lapack_int* ifail );

lapack_int LAPACKE_chpgst( int matrix_layout, lapack_int itype, char uplo,
                           lapack_int n, lapack_complex_float* ap,
                           const lapack_complex_float* bp );
lapack_int LAPACKE_zhpgst( int matrix_layout, lapack_int itype, char uplo,
                           lapack_int n, lapack_complex_double* ap,
                           const lapack_complex_double* bp );

lapack_int LAPACKE_chpgv( int matrix_layout, lapack_int itype, char jobz,
                          char uplo, lapack_int n, lapack_complex_float* ap,
                          lapack_complex_float* bp, float* w,
                          lapack_complex_float* z, lapack_int ldz );
lapack_int LAPACKE_zhpgv( int matrix_layout, lapack_int itype, char jobz,
                          char uplo, lapack_int n, lapack_complex_double* ap,
                          lapack_complex_double* bp, double* w,
                          lapack_complex_double* z, lapack_int ldz );

lapack_int LAPACKE_chpgvd( int matrix_layout, lapack_int itype, char jobz,
                           char uplo, lapack_int n, lapack_complex_float* ap,
                           lapack_complex_float* bp, float* w,
                           lapack_complex_float* z, lapack_int ldz );
lapack_int LAPACKE_zhpgvd( int matrix_layout, lapack_int itype, char jobz,
                           char uplo, lapack_int n, lapack_complex_double* ap,
                           lapack_complex_double* bp, double* w,
                           lapack_complex_double* z, lapack_int ldz );

lapack_int LAPACKE_chpgvx( int matrix_layout, lapack_int itype, char jobz,
                           char range, char uplo, lapack_int n,
                           lapack_complex_float* ap, lapack_complex_float* bp,
                           float vl, float vu, lapack_int il, lapack_int iu,
                           float abstol, lapack_int* m, float* w,
                           lapack_complex_float* z, lapack_int ldz,
                           lapack_int* ifail );
lapack_int LAPACKE_zhpgvx( int matrix_layout, lapack_int itype, char jobz,
                           char range, char uplo, lapack_int n,
                           lapack_complex_double* ap, lapack_complex_double* bp,
                           double vl, double vu, lapack_int il, lapack_int iu,
                           double abstol, lapack_int* m, double* w,
                           lapack_complex_double* z, lapack_int ldz,
                           lapack_int* ifail );

lapack_int LAPACKE_chprfs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_float* ap,
                           const lapack_complex_float* afp,
                           const lapack_int* ipiv,
                           const lapack_complex_float* b, lapack_int ldb,
                           lapack_complex_float* x, lapack_int ldx, float* ferr,
                           float* berr );
lapack_int LAPACKE_zhprfs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_double* ap,
                           const lapack_complex_double* afp,
                           const lapack_int* ipiv,
                           const lapack_complex_double* b, lapack_int ldb,
                           lapack_complex_double* x, lapack_int ldx,
                           double* ferr, double* berr );

lapack_int LAPACKE_chpsv( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, lapack_complex_float* ap,
                          lapack_int* ipiv, lapack_complex_float* b,
                          lapack_int ldb );
lapack_int LAPACKE_zhpsv( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, lapack_complex_double* ap,
                          lapack_int* ipiv, lapack_complex_double* b,
                          lapack_int ldb );

lapack_int LAPACKE_chpsvx( int matrix_layout, char fact, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_float* ap,
                           lapack_complex_float* afp, lapack_int* ipiv,
                           const lapack_complex_float* b, lapack_int ldb,
                           lapack_complex_float* x, lapack_int ldx,
                           float* rcond, float* ferr, float* berr );
lapack_int LAPACKE_zhpsvx( int matrix_layout, char fact, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_double* ap,
                           lapack_complex_double* afp, lapack_int* ipiv,
                           const lapack_complex_double* b, lapack_int ldb,
                           lapack_complex_double* x, lapack_int ldx,
                           double* rcond, double* ferr, double* berr );

lapack_int LAPACKE_chptrd( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_float* ap, float* d, float* e,
                           lapack_complex_float* tau );
lapack_int LAPACKE_zhptrd( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_double* ap, double* d, double* e,
                           lapack_complex_double* tau );

lapack_int LAPACKE_chptrf( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_float* ap, lapack_int* ipiv );
lapack_int LAPACKE_zhptrf( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_double* ap, lapack_int* ipiv );

lapack_int LAPACKE_chptri( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_float* ap, const lapack_int* ipiv );
lapack_int LAPACKE_zhptri( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_double* ap, const lapack_int* ipiv );

lapack_int LAPACKE_chptrs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_float* ap,
                           const lapack_int* ipiv, lapack_complex_float* b,
                           lapack_int ldb );
lapack_int LAPACKE_zhptrs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_double* ap,
                           const lapack_int* ipiv, lapack_complex_double* b,
                           lapack_int ldb );

lapack_int LAPACKE_shsein( int matrix_layout, char job, char eigsrc, char initv,
                           lapack_logical* select, lapack_int n, const float* h,
                           lapack_int ldh, float* wr, const float* wi,
                           float* vl, lapack_int ldvl, float* vr,
                           lapack_int ldvr, lapack_int mm, lapack_int* m,
                           lapack_int* ifaill, lapack_int* ifailr );
lapack_int LAPACKE_dhsein( int matrix_layout, char job, char eigsrc, char initv,
                           lapack_logical* select, lapack_int n,
                           const double* h, lapack_int ldh, double* wr,
                           const double* wi, double* vl, lapack_int ldvl,
                           double* vr, lapack_int ldvr, lapack_int mm,
                           lapack_int* m, lapack_int* ifaill,
                           lapack_int* ifailr );
lapack_int LAPACKE_chsein( int matrix_layout, char job, char eigsrc, char initv,
                           const lapack_logical* select, lapack_int n,
                           const lapack_complex_float* h, lapack_int ldh,
                           lapack_complex_float* w, lapack_complex_float* vl,
                           lapack_int ldvl, lapack_complex_float* vr,
                           lapack_int ldvr, lapack_int mm, lapack_int* m,
                           lapack_int* ifaill, lapack_int* ifailr );
lapack_int LAPACKE_zhsein( int matrix_layout, char job, char eigsrc, char initv,
                           const lapack_logical* select, lapack_int n,
                           const lapack_complex_double* h, lapack_int ldh,
                           lapack_complex_double* w, lapack_complex_double* vl,
                           lapack_int ldvl, lapack_complex_double* vr,
                           lapack_int ldvr, lapack_int mm, lapack_int* m,
                           lapack_int* ifaill, lapack_int* ifailr );

lapack_int LAPACKE_shseqr( int matrix_layout, char job, char compz, lapack_int n,
                           lapack_int ilo, lapack_int ihi, float* h,
                           lapack_int ldh, float* wr, float* wi, float* z,
                           lapack_int ldz );
lapack_int LAPACKE_dhseqr( int matrix_layout, char job, char compz, lapack_int n,
                           lapack_int ilo, lapack_int ihi, double* h,
                           lapack_int ldh, double* wr, double* wi, double* z,
                           lapack_int ldz );
lapack_int LAPACKE_chseqr( int matrix_layout, char job, char compz, lapack_int n,
                           lapack_int ilo, lapack_int ihi,
                           lapack_complex_float* h, lapack_int ldh,
                           lapack_complex_float* w, lapack_complex_float* z,
                           lapack_int ldz );
lapack_int LAPACKE_zhseqr( int matrix_layout, char job, char compz, lapack_int n,
                           lapack_int ilo, lapack_int ihi,
                           lapack_complex_double* h, lapack_int ldh,
                           lapack_complex_double* w, lapack_complex_double* z,
                           lapack_int ldz );

lapack_int LAPACKE_clacgv( lapack_int n, lapack_complex_float* x,
                           lapack_int incx );
lapack_int LAPACKE_zlacgv( lapack_int n, lapack_complex_double* x,
                           lapack_int incx );

lapack_int LAPACKE_slacn2( lapack_int n, float* v, float* x, lapack_int* isgn,
                           float* est, lapack_int* kase, lapack_int* isave );
lapack_int LAPACKE_dlacn2( lapack_int n, double* v, double* x, lapack_int* isgn,
                           double* est, lapack_int* kase, lapack_int* isave );
lapack_int LAPACKE_clacn2( lapack_int n, lapack_complex_float* v,
                           lapack_complex_float* x,
                           float* est, lapack_int* kase, lapack_int* isave );
lapack_int LAPACKE_zlacn2( lapack_int n, lapack_complex_double* v,
                           lapack_complex_double* x,
                           double* est, lapack_int* kase, lapack_int* isave );

lapack_int LAPACKE_slacpy( int matrix_layout, char uplo, lapack_int m,
                           lapack_int n, const float* a, lapack_int lda, float* b,
                           lapack_int ldb );
lapack_int LAPACKE_dlacpy( int matrix_layout, char uplo, lapack_int m,
                           lapack_int n, const double* a, lapack_int lda, double* b,
                           lapack_int ldb );
lapack_int LAPACKE_clacpy( int matrix_layout, char uplo, lapack_int m,
                           lapack_int n, const lapack_complex_float* a,
                           lapack_int lda, lapack_complex_float* b,
                           lapack_int ldb );
lapack_int LAPACKE_zlacpy( int matrix_layout, char uplo, lapack_int m,
                           lapack_int n, const lapack_complex_double* a,
                           lapack_int lda, lapack_complex_double* b,
                           lapack_int ldb );

lapack_int LAPACKE_clacp2( int matrix_layout, char uplo, lapack_int m,
                           lapack_int n, const float* a, lapack_int lda,
                           lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zlacp2( int matrix_layout, char uplo, lapack_int m,
                           lapack_int n, const double* a, lapack_int lda,
                           lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_zlag2c( int matrix_layout, lapack_int m, lapack_int n,
                           const lapack_complex_double* a, lapack_int lda,
                           lapack_complex_float* sa, lapack_int ldsa );

lapack_int LAPACKE_slag2d( int matrix_layout, lapack_int m, lapack_int n,
                           const float* sa, lapack_int ldsa, double* a,
                           lapack_int lda );

lapack_int LAPACKE_dlag2s( int matrix_layout, lapack_int m, lapack_int n,
                           const double* a, lapack_int lda, float* sa,
                           lapack_int ldsa );

lapack_int LAPACKE_clag2z( int matrix_layout, lapack_int m, lapack_int n,
                           const lapack_complex_float* sa, lapack_int ldsa,
                           lapack_complex_double* a, lapack_int lda );

lapack_int LAPACKE_slagge( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int kl, lapack_int ku, const float* d,
                           float* a, lapack_int lda, lapack_int* iseed );
lapack_int LAPACKE_dlagge( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int kl, lapack_int ku, const double* d,
                           double* a, lapack_int lda, lapack_int* iseed );
lapack_int LAPACKE_clagge( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int kl, lapack_int ku, const float* d,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_int* iseed );
lapack_int LAPACKE_zlagge( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int kl, lapack_int ku, const double* d,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_int* iseed );

float LAPACKE_slamch( char cmach );
double LAPACKE_dlamch( char cmach );

float LAPACKE_slangb( int matrix_layout, char norm, lapack_int n,
                      lapack_int kl, lapack_int ku, const float* ab,
                      lapack_int ldab );
double LAPACKE_dlangb( int matrix_layout, char norm, lapack_int n,
                       lapack_int kl, lapack_int ku, const double* ab,
                       lapack_int ldab );
float LAPACKE_clangb( int matrix_layout, char norm, lapack_int n,
                      lapack_int kl, lapack_int ku,
                      const lapack_complex_float* ab, lapack_int ldab );
double LAPACKE_zlangb( int matrix_layout, char norm, lapack_int n,
                       lapack_int kl, lapack_int ku,
                       const lapack_complex_double* ab, lapack_int ldab );

float LAPACKE_slange( int matrix_layout, char norm, lapack_int m,
                           lapack_int n, const float* a, lapack_int lda );
double LAPACKE_dlange( int matrix_layout, char norm, lapack_int m,
                           lapack_int n, const double* a, lapack_int lda );
float LAPACKE_clange( int matrix_layout, char norm, lapack_int m,
                           lapack_int n, const lapack_complex_float* a,
                           lapack_int lda );
double LAPACKE_zlange( int matrix_layout, char norm, lapack_int m,
                           lapack_int n, const lapack_complex_double* a,
                           lapack_int lda );

float LAPACKE_clanhe( int matrix_layout, char norm, char uplo, lapack_int n,
                           const lapack_complex_float* a, lapack_int lda );
double LAPACKE_zlanhe( int matrix_layout, char norm, char uplo, lapack_int n,
                           const lapack_complex_double* a, lapack_int lda );

lapack_int LAPACKE_clacrm( int matrix_layout, lapack_int m, lapack_int n,
                          const lapack_complex_float* a,
                          lapack_int lda, const float* b,
                          lapack_int ldb, lapack_complex_float* c,
                          lapack_int ldc );
lapack_int LAPACKE_zlacrm( int matrix_layout, lapack_int m, lapack_int n,
                           const lapack_complex_double* a,
                           lapack_int lda, const double* b,
                           lapack_int ldb, lapack_complex_double* c,
                           lapack_int ldc );

lapack_int LAPACKE_clarcm( int matrix_layout, lapack_int m, lapack_int n,
                          const float* a, lapack_int lda,
                          const lapack_complex_float* b,
                          lapack_int ldb, lapack_complex_float* c,
                          lapack_int ldc );
lapack_int LAPACKE_zlarcm( int matrix_layout, lapack_int m, lapack_int n,
                           const double* a, lapack_int lda,
                           const lapack_complex_double* b,
                           lapack_int ldb, lapack_complex_double* c,
                           lapack_int ldc );

float LAPACKE_slansy( int matrix_layout, char norm, char uplo, lapack_int n,
                           const float* a, lapack_int lda );
double LAPACKE_dlansy( int matrix_layout, char norm, char uplo, lapack_int n,
                           const double* a, lapack_int lda );
float LAPACKE_clansy( int matrix_layout, char norm, char uplo, lapack_int n,
                           const lapack_complex_float* a, lapack_int lda );
double LAPACKE_zlansy( int matrix_layout, char norm, char uplo, lapack_int n,
                           const lapack_complex_double* a, lapack_int lda );

float LAPACKE_slantr( int matrix_layout, char norm, char uplo, char diag,
                           lapack_int m, lapack_int n, const float* a,
                           lapack_int lda );
double LAPACKE_dlantr( int matrix_layout, char norm, char uplo, char diag,
                           lapack_int m, lapack_int n, const double* a,
                           lapack_int lda );
float LAPACKE_clantr( int matrix_layout, char norm, char uplo, char diag,
                           lapack_int m, lapack_int n, const lapack_complex_float* a,
                           lapack_int lda );
double LAPACKE_zlantr( int matrix_layout, char norm, char uplo, char diag,
                           lapack_int m, lapack_int n, const lapack_complex_double* a,
                           lapack_int lda );


lapack_int LAPACKE_slarfb( int matrix_layout, char side, char trans, char direct,
                           char storev, lapack_int m, lapack_int n,
                           lapack_int k, const float* v, lapack_int ldv,
                           const float* t, lapack_int ldt, float* c,
                           lapack_int ldc );
lapack_int LAPACKE_dlarfb( int matrix_layout, char side, char trans, char direct,
                           char storev, lapack_int m, lapack_int n,
                           lapack_int k, const double* v, lapack_int ldv,
                           const double* t, lapack_int ldt, double* c,
                           lapack_int ldc );
lapack_int LAPACKE_clarfb( int matrix_layout, char side, char trans, char direct,
                           char storev, lapack_int m, lapack_int n,
                           lapack_int k, const lapack_complex_float* v,
                           lapack_int ldv, const lapack_complex_float* t,
                           lapack_int ldt, lapack_complex_float* c,
                           lapack_int ldc );
lapack_int LAPACKE_zlarfb( int matrix_layout, char side, char trans, char direct,
                           char storev, lapack_int m, lapack_int n,
                           lapack_int k, const lapack_complex_double* v,
                           lapack_int ldv, const lapack_complex_double* t,
                           lapack_int ldt, lapack_complex_double* c,
                           lapack_int ldc );

lapack_int LAPACKE_slarfg( lapack_int n, float* alpha, float* x,
                           lapack_int incx, float* tau );
lapack_int LAPACKE_dlarfg( lapack_int n, double* alpha, double* x,
                           lapack_int incx, double* tau );
lapack_int LAPACKE_clarfg( lapack_int n, lapack_complex_float* alpha,
                           lapack_complex_float* x, lapack_int incx,
                           lapack_complex_float* tau );
lapack_int LAPACKE_zlarfg( lapack_int n, lapack_complex_double* alpha,
                           lapack_complex_double* x, lapack_int incx,
                           lapack_complex_double* tau );

lapack_int LAPACKE_slarft( int matrix_layout, char direct, char storev,
                           lapack_int n, lapack_int k, const float* v,
                           lapack_int ldv, const float* tau, float* t,
                           lapack_int ldt );
lapack_int LAPACKE_dlarft( int matrix_layout, char direct, char storev,
                           lapack_int n, lapack_int k, const double* v,
                           lapack_int ldv, const double* tau, double* t,
                           lapack_int ldt );
lapack_int LAPACKE_clarft( int matrix_layout, char direct, char storev,
                           lapack_int n, lapack_int k,
                           const lapack_complex_float* v, lapack_int ldv,
                           const lapack_complex_float* tau,
                           lapack_complex_float* t, lapack_int ldt );
lapack_int LAPACKE_zlarft( int matrix_layout, char direct, char storev,
                           lapack_int n, lapack_int k,
                           const lapack_complex_double* v, lapack_int ldv,
                           const lapack_complex_double* tau,
                           lapack_complex_double* t, lapack_int ldt );

lapack_int LAPACKE_slarfx( int matrix_layout, char side, lapack_int m,
                           lapack_int n, const float* v, float tau, float* c,
                           lapack_int ldc, float* work );
lapack_int LAPACKE_dlarfx( int matrix_layout, char side, lapack_int m,
                           lapack_int n, const double* v, double tau, double* c,
                           lapack_int ldc, double* work );
lapack_int LAPACKE_clarfx( int matrix_layout, char side, lapack_int m,
                           lapack_int n, const lapack_complex_float* v,
                           lapack_complex_float tau, lapack_complex_float* c,
                           lapack_int ldc, lapack_complex_float* work );
lapack_int LAPACKE_zlarfx( int matrix_layout, char side, lapack_int m,
                           lapack_int n, const lapack_complex_double* v,
                           lapack_complex_double tau, lapack_complex_double* c,
                           lapack_int ldc, lapack_complex_double* work );

lapack_int LAPACKE_slarnv( lapack_int idist, lapack_int* iseed, lapack_int n,
                           float* x );
lapack_int LAPACKE_dlarnv( lapack_int idist, lapack_int* iseed, lapack_int n,
                           double* x );
lapack_int LAPACKE_clarnv( lapack_int idist, lapack_int* iseed, lapack_int n,
                           lapack_complex_float* x );
lapack_int LAPACKE_zlarnv( lapack_int idist, lapack_int* iseed, lapack_int n,
                           lapack_complex_double* x );

lapack_int LAPACKE_slascl( int matrix_layout, char type, lapack_int kl,
                           lapack_int ku, float cfrom, float cto,
                           lapack_int m, lapack_int n, float* a,
                           lapack_int lda );
lapack_int LAPACKE_dlascl( int matrix_layout, char type, lapack_int kl,
                           lapack_int ku, double cfrom, double cto,
                           lapack_int m, lapack_int n, double* a,
                           lapack_int lda );
lapack_int LAPACKE_clascl( int matrix_layout, char type, lapack_int kl,
                           lapack_int ku, float cfrom, float cto,
                           lapack_int m, lapack_int n, lapack_complex_float* a,
                           lapack_int lda );
lapack_int LAPACKE_zlascl( int matrix_layout, char type, lapack_int kl,
                           lapack_int ku, double cfrom, double cto,
                           lapack_int m, lapack_int n, lapack_complex_double* a,
                           lapack_int lda );

lapack_int LAPACKE_slaset( int matrix_layout, char uplo, lapack_int m,
                           lapack_int n, float alpha, float beta, float* a,
                           lapack_int lda );
lapack_int LAPACKE_dlaset( int matrix_layout, char uplo, lapack_int m,
                           lapack_int n, double alpha, double beta, double* a,
                           lapack_int lda );
lapack_int LAPACKE_claset( int matrix_layout, char uplo, lapack_int m,
                           lapack_int n, lapack_complex_float alpha,
                           lapack_complex_float beta, lapack_complex_float* a,
                           lapack_int lda );
lapack_int LAPACKE_zlaset( int matrix_layout, char uplo, lapack_int m,
                           lapack_int n, lapack_complex_double alpha,
                           lapack_complex_double beta, lapack_complex_double* a,
                           lapack_int lda );

lapack_int LAPACKE_slasrt( char id, lapack_int n, float* d );
lapack_int LAPACKE_dlasrt( char id, lapack_int n, double* d );

lapack_int LAPACKE_slassq( lapack_int n,                 float* x, lapack_int incx,  float* scale,  float* sumsq );
lapack_int LAPACKE_dlassq( lapack_int n,                double* x, lapack_int incx, double* scale, double* sumsq );
lapack_int LAPACKE_classq( lapack_int n,  lapack_complex_float* x, lapack_int incx,  float* scale,  float* sumsq );
lapack_int LAPACKE_zlassq( lapack_int n, lapack_complex_double* x, lapack_int incx, double* scale, double* sumsq );

lapack_int LAPACKE_slaswp( int matrix_layout, lapack_int n, float* a,
                           lapack_int lda, lapack_int k1, lapack_int k2,
                           const lapack_int* ipiv, lapack_int incx );
lapack_int LAPACKE_dlaswp( int matrix_layout, lapack_int n, double* a,
                           lapack_int lda, lapack_int k1, lapack_int k2,
                           const lapack_int* ipiv, lapack_int incx );
lapack_int LAPACKE_claswp( int matrix_layout, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_int k1, lapack_int k2, const lapack_int* ipiv,
                           lapack_int incx );
lapack_int LAPACKE_zlaswp( int matrix_layout, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_int k1, lapack_int k2, const lapack_int* ipiv,
                           lapack_int incx );

lapack_int LAPACKE_slatms( int matrix_layout, lapack_int m, lapack_int n,
                           char dist, lapack_int* iseed, char sym, float* d,
                           lapack_int mode, float cond, float dmax,
                           lapack_int kl, lapack_int ku, char pack, float* a,
                           lapack_int lda );
lapack_int LAPACKE_dlatms( int matrix_layout, lapack_int m, lapack_int n,
                           char dist, lapack_int* iseed, char sym, double* d,
                           lapack_int mode, double cond, double dmax,
                           lapack_int kl, lapack_int ku, char pack, double* a,
                           lapack_int lda );
lapack_int LAPACKE_clatms( int matrix_layout, lapack_int m, lapack_int n,
                           char dist, lapack_int* iseed, char sym, float* d,
                           lapack_int mode, float cond, float dmax,
                           lapack_int kl, lapack_int ku, char pack,
                           lapack_complex_float* a, lapack_int lda );
lapack_int LAPACKE_zlatms( int matrix_layout, lapack_int m, lapack_int n,
                           char dist, lapack_int* iseed, char sym, double* d,
                           lapack_int mode, double cond, double dmax,
                           lapack_int kl, lapack_int ku, char pack,
                           lapack_complex_double* a, lapack_int lda );

lapack_int LAPACKE_slauum( int matrix_layout, char uplo, lapack_int n, float* a,
                           lapack_int lda );
lapack_int LAPACKE_dlauum( int matrix_layout, char uplo, lapack_int n, double* a,
                           lapack_int lda );
lapack_int LAPACKE_clauum( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_float* a, lapack_int lda );
lapack_int LAPACKE_zlauum( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_double* a, lapack_int lda );

lapack_int LAPACKE_sopgtr( int matrix_layout, char uplo, lapack_int n,
                           const float* ap, const float* tau, float* q,
                           lapack_int ldq );
lapack_int LAPACKE_dopgtr( int matrix_layout, char uplo, lapack_int n,
                           const double* ap, const double* tau, double* q,
                           lapack_int ldq );

lapack_int LAPACKE_sopmtr( int matrix_layout, char side, char uplo, char trans,
                           lapack_int m, lapack_int n, const float* ap,
                           const float* tau, float* c, lapack_int ldc );
lapack_int LAPACKE_dopmtr( int matrix_layout, char side, char uplo, char trans,
                           lapack_int m, lapack_int n, const double* ap,
                           const double* tau, double* c, lapack_int ldc );

lapack_int LAPACKE_sorgbr( int matrix_layout, char vect, lapack_int m,
                           lapack_int n, lapack_int k, float* a, lapack_int lda,
                           const float* tau );
lapack_int LAPACKE_dorgbr( int matrix_layout, char vect, lapack_int m,
                           lapack_int n, lapack_int k, double* a,
                           lapack_int lda, const double* tau );

lapack_int LAPACKE_sorghr( int matrix_layout, lapack_int n, lapack_int ilo,
                           lapack_int ihi, float* a, lapack_int lda,
                           const float* tau );
lapack_int LAPACKE_dorghr( int matrix_layout, lapack_int n, lapack_int ilo,
                           lapack_int ihi, double* a, lapack_int lda,
                           const double* tau );

lapack_int LAPACKE_sorglq( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int k, float* a, lapack_int lda,
                           const float* tau );
lapack_int LAPACKE_dorglq( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int k, double* a, lapack_int lda,
                           const double* tau );

lapack_int LAPACKE_sorgql( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int k, float* a, lapack_int lda,
                           const float* tau );
lapack_int LAPACKE_dorgql( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int k, double* a, lapack_int lda,
                           const double* tau );

lapack_int LAPACKE_sorgqr( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int k, float* a, lapack_int lda,
                           const float* tau );
lapack_int LAPACKE_dorgqr( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int k, double* a, lapack_int lda,
                           const double* tau );

lapack_int LAPACKE_sorgrq( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int k, float* a, lapack_int lda,
                           const float* tau );
lapack_int LAPACKE_dorgrq( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int k, double* a, lapack_int lda,
                           const double* tau );

lapack_int LAPACKE_sorgtr( int matrix_layout, char uplo, lapack_int n, float* a,
                           lapack_int lda, const float* tau );
lapack_int LAPACKE_dorgtr( int matrix_layout, char uplo, lapack_int n, double* a,
                           lapack_int lda, const double* tau );

lapack_int LAPACKE_sorgtsqr_row( int matrix_layout, lapack_int m, lapack_int n,
                                 lapack_int mb, lapack_int nb,
                                 float* a, lapack_int lda,
                                 const float* t, lapack_int ldt );
lapack_int LAPACKE_dorgtsqr_row( int matrix_layout, lapack_int m, lapack_int n,
                                 lapack_int mb, lapack_int nb,
                                 double* a, lapack_int lda,
                                 const double* t, lapack_int ldt );

lapack_int LAPACKE_sormbr( int matrix_layout, char vect, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           const float* a, lapack_int lda, const float* tau,
                           float* c, lapack_int ldc );
lapack_int LAPACKE_dormbr( int matrix_layout, char vect, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           const double* a, lapack_int lda, const double* tau,
                           double* c, lapack_int ldc );

lapack_int LAPACKE_sormhr( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int ilo,
                           lapack_int ihi, const float* a, lapack_int lda,
                           const float* tau, float* c, lapack_int ldc );
lapack_int LAPACKE_dormhr( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int ilo,
                           lapack_int ihi, const double* a, lapack_int lda,
                           const double* tau, double* c, lapack_int ldc );

lapack_int LAPACKE_sormlq( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           const float* a, lapack_int lda, const float* tau,
                           float* c, lapack_int ldc );
lapack_int LAPACKE_dormlq( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           const double* a, lapack_int lda, const double* tau,
                           double* c, lapack_int ldc );

lapack_int LAPACKE_sormql( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           const float* a, lapack_int lda, const float* tau,
                           float* c, lapack_int ldc );
lapack_int LAPACKE_dormql( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           const double* a, lapack_int lda, const double* tau,
                           double* c, lapack_int ldc );

lapack_int LAPACKE_sormqr( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           const float* a, lapack_int lda, const float* tau,
                           float* c, lapack_int ldc );
lapack_int LAPACKE_dormqr( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           const double* a, lapack_int lda, const double* tau,
                           double* c, lapack_int ldc );

lapack_int LAPACKE_sormrq( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           const float* a, lapack_int lda, const float* tau,
                           float* c, lapack_int ldc );
lapack_int LAPACKE_dormrq( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           const double* a, lapack_int lda, const double* tau,
                           double* c, lapack_int ldc );

lapack_int LAPACKE_sormrz( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           lapack_int l, const float* a, lapack_int lda,
                           const float* tau, float* c, lapack_int ldc );
lapack_int LAPACKE_dormrz( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           lapack_int l, const double* a, lapack_int lda,
                           const double* tau, double* c, lapack_int ldc );

lapack_int LAPACKE_sormtr( int matrix_layout, char side, char uplo, char trans,
                           lapack_int m, lapack_int n, const float* a,
                           lapack_int lda, const float* tau, float* c,
                           lapack_int ldc );
lapack_int LAPACKE_dormtr( int matrix_layout, char side, char uplo, char trans,
                           lapack_int m, lapack_int n, const double* a,
                           lapack_int lda, const double* tau, double* c,
                           lapack_int ldc );

lapack_int LAPACKE_spbcon( int matrix_layout, char uplo, lapack_int n,
                           lapack_int kd, const float* ab, lapack_int ldab,
                           float anorm, float* rcond );
lapack_int LAPACKE_dpbcon( int matrix_layout, char uplo, lapack_int n,
                           lapack_int kd, const double* ab, lapack_int ldab,
                           double anorm, double* rcond );
lapack_int LAPACKE_cpbcon( int matrix_layout, char uplo, lapack_int n,
                           lapack_int kd, const lapack_complex_float* ab,
                           lapack_int ldab, float anorm, float* rcond );
lapack_int LAPACKE_zpbcon( int matrix_layout, char uplo, lapack_int n,
                           lapack_int kd, const lapack_complex_double* ab,
                           lapack_int ldab, double anorm, double* rcond );

lapack_int LAPACKE_spbequ( int matrix_layout, char uplo, lapack_int n,
                           lapack_int kd, const float* ab, lapack_int ldab,
                           float* s, float* scond, float* amax );
lapack_int LAPACKE_dpbequ( int matrix_layout, char uplo, lapack_int n,
                           lapack_int kd, const double* ab, lapack_int ldab,
                           double* s, double* scond, double* amax );
lapack_int LAPACKE_cpbequ( int matrix_layout, char uplo, lapack_int n,
                           lapack_int kd, const lapack_complex_float* ab,
                           lapack_int ldab, float* s, float* scond,
                           float* amax );
lapack_int LAPACKE_zpbequ( int matrix_layout, char uplo, lapack_int n,
                           lapack_int kd, const lapack_complex_double* ab,
                           lapack_int ldab, double* s, double* scond,
                           double* amax );

lapack_int LAPACKE_spbrfs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int kd, lapack_int nrhs, const float* ab,
                           lapack_int ldab, const float* afb, lapack_int ldafb,
                           const float* b, lapack_int ldb, float* x,
                           lapack_int ldx, float* ferr, float* berr );
lapack_int LAPACKE_dpbrfs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int kd, lapack_int nrhs, const double* ab,
                           lapack_int ldab, const double* afb, lapack_int ldafb,
                           const double* b, lapack_int ldb, double* x,
                           lapack_int ldx, double* ferr, double* berr );
lapack_int LAPACKE_cpbrfs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int kd, lapack_int nrhs,
                           const lapack_complex_float* ab, lapack_int ldab,
                           const lapack_complex_float* afb, lapack_int ldafb,
                           const lapack_complex_float* b, lapack_int ldb,
                           lapack_complex_float* x, lapack_int ldx, float* ferr,
                           float* berr );
lapack_int LAPACKE_zpbrfs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int kd, lapack_int nrhs,
                           const lapack_complex_double* ab, lapack_int ldab,
                           const lapack_complex_double* afb, lapack_int ldafb,
                           const lapack_complex_double* b, lapack_int ldb,
                           lapack_complex_double* x, lapack_int ldx,
                           double* ferr, double* berr );

lapack_int LAPACKE_spbstf( int matrix_layout, char uplo, lapack_int n,
                           lapack_int kb, float* bb, lapack_int ldbb );
lapack_int LAPACKE_dpbstf( int matrix_layout, char uplo, lapack_int n,
                           lapack_int kb, double* bb, lapack_int ldbb );
lapack_int LAPACKE_cpbstf( int matrix_layout, char uplo, lapack_int n,
                           lapack_int kb, lapack_complex_float* bb,
                           lapack_int ldbb );
lapack_int LAPACKE_zpbstf( int matrix_layout, char uplo, lapack_int n,
                           lapack_int kb, lapack_complex_double* bb,
                           lapack_int ldbb );

lapack_int LAPACKE_spbsv( int matrix_layout, char uplo, lapack_int n,
                          lapack_int kd, lapack_int nrhs, float* ab,
                          lapack_int ldab, float* b, lapack_int ldb );
lapack_int LAPACKE_dpbsv( int matrix_layout, char uplo, lapack_int n,
                          lapack_int kd, lapack_int nrhs, double* ab,
                          lapack_int ldab, double* b, lapack_int ldb );
lapack_int LAPACKE_cpbsv( int matrix_layout, char uplo, lapack_int n,
                          lapack_int kd, lapack_int nrhs,
                          lapack_complex_float* ab, lapack_int ldab,
                          lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zpbsv( int matrix_layout, char uplo, lapack_int n,
                          lapack_int kd, lapack_int nrhs,
                          lapack_complex_double* ab, lapack_int ldab,
                          lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_spbsvx( int matrix_layout, char fact, char uplo, lapack_int n,
                           lapack_int kd, lapack_int nrhs, float* ab,
                           lapack_int ldab, float* afb, lapack_int ldafb,
                           char* equed, float* s, float* b, lapack_int ldb,
                           float* x, lapack_int ldx, float* rcond, float* ferr,
                           float* berr );
lapack_int LAPACKE_dpbsvx( int matrix_layout, char fact, char uplo, lapack_int n,
                           lapack_int kd, lapack_int nrhs, double* ab,
                           lapack_int ldab, double* afb, lapack_int ldafb,
                           char* equed, double* s, double* b, lapack_int ldb,
                           double* x, lapack_int ldx, double* rcond,
                           double* ferr, double* berr );
lapack_int LAPACKE_cpbsvx( int matrix_layout, char fact, char uplo, lapack_int n,
                           lapack_int kd, lapack_int nrhs,
                           lapack_complex_float* ab, lapack_int ldab,
                           lapack_complex_float* afb, lapack_int ldafb,
                           char* equed, float* s, lapack_complex_float* b,
                           lapack_int ldb, lapack_complex_float* x,
                           lapack_int ldx, float* rcond, float* ferr,
                           float* berr );
lapack_int LAPACKE_zpbsvx( int matrix_layout, char fact, char uplo, lapack_int n,
                           lapack_int kd, lapack_int nrhs,
                           lapack_complex_double* ab, lapack_int ldab,
                           lapack_complex_double* afb, lapack_int ldafb,
                           char* equed, double* s, lapack_complex_double* b,
                           lapack_int ldb, lapack_complex_double* x,
                           lapack_int ldx, double* rcond, double* ferr,
                           double* berr );

lapack_int LAPACKE_spbtrf( int matrix_layout, char uplo, lapack_int n,
                           lapack_int kd, float* ab, lapack_int ldab );
lapack_int LAPACKE_dpbtrf( int matrix_layout, char uplo, lapack_int n,
                           lapack_int kd, double* ab, lapack_int ldab );
lapack_int LAPACKE_cpbtrf( int matrix_layout, char uplo, lapack_int n,
                           lapack_int kd, lapack_complex_float* ab,
                           lapack_int ldab );
lapack_int LAPACKE_zpbtrf( int matrix_layout, char uplo, lapack_int n,
                           lapack_int kd, lapack_complex_double* ab,
                           lapack_int ldab );

lapack_int LAPACKE_spbtrs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int kd, lapack_int nrhs, const float* ab,
                           lapack_int ldab, float* b, lapack_int ldb );
lapack_int LAPACKE_dpbtrs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int kd, lapack_int nrhs, const double* ab,
                           lapack_int ldab, double* b, lapack_int ldb );
lapack_int LAPACKE_cpbtrs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int kd, lapack_int nrhs,
                           const lapack_complex_float* ab, lapack_int ldab,
                           lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zpbtrs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int kd, lapack_int nrhs,
                           const lapack_complex_double* ab, lapack_int ldab,
                           lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_spftrf( int matrix_layout, char transr, char uplo,
                           lapack_int n, float* a );
lapack_int LAPACKE_dpftrf( int matrix_layout, char transr, char uplo,
                           lapack_int n, double* a );
lapack_int LAPACKE_cpftrf( int matrix_layout, char transr, char uplo,
                           lapack_int n, lapack_complex_float* a );
lapack_int LAPACKE_zpftrf( int matrix_layout, char transr, char uplo,
                           lapack_int n, lapack_complex_double* a );

lapack_int LAPACKE_spftri( int matrix_layout, char transr, char uplo,
                           lapack_int n, float* a );
lapack_int LAPACKE_dpftri( int matrix_layout, char transr, char uplo,
                           lapack_int n, double* a );
lapack_int LAPACKE_cpftri( int matrix_layout, char transr, char uplo,
                           lapack_int n, lapack_complex_float* a );
lapack_int LAPACKE_zpftri( int matrix_layout, char transr, char uplo,
                           lapack_int n, lapack_complex_double* a );

lapack_int LAPACKE_spftrs( int matrix_layout, char transr, char uplo,
                           lapack_int n, lapack_int nrhs, const float* a,
                           float* b, lapack_int ldb );
lapack_int LAPACKE_dpftrs( int matrix_layout, char transr, char uplo,
                           lapack_int n, lapack_int nrhs, const double* a,
                           double* b, lapack_int ldb );
lapack_int LAPACKE_cpftrs( int matrix_layout, char transr, char uplo,
                           lapack_int n, lapack_int nrhs,
                           const lapack_complex_float* a,
                           lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zpftrs( int matrix_layout, char transr, char uplo,
                           lapack_int n, lapack_int nrhs,
                           const lapack_complex_double* a,
                           lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_spocon( int matrix_layout, char uplo, lapack_int n,
                           const float* a, lapack_int lda, float anorm,
                           float* rcond );
lapack_int LAPACKE_dpocon( int matrix_layout, char uplo, lapack_int n,
                           const double* a, lapack_int lda, double anorm,
                           double* rcond );
lapack_int LAPACKE_cpocon( int matrix_layout, char uplo, lapack_int n,
                           const lapack_complex_float* a, lapack_int lda,
                           float anorm, float* rcond );
lapack_int LAPACKE_zpocon( int matrix_layout, char uplo, lapack_int n,
                           const lapack_complex_double* a, lapack_int lda,
                           double anorm, double* rcond );

lapack_int LAPACKE_spoequ( int matrix_layout, lapack_int n, const float* a,
                           lapack_int lda, float* s, float* scond,
                           float* amax );
lapack_int LAPACKE_dpoequ( int matrix_layout, lapack_int n, const double* a,
                           lapack_int lda, double* s, double* scond,
                           double* amax );
lapack_int LAPACKE_cpoequ( int matrix_layout, lapack_int n,
                           const lapack_complex_float* a, lapack_int lda,
                           float* s, float* scond, float* amax );
lapack_int LAPACKE_zpoequ( int matrix_layout, lapack_int n,
                           const lapack_complex_double* a, lapack_int lda,
                           double* s, double* scond, double* amax );

lapack_int LAPACKE_spoequb( int matrix_layout, lapack_int n, const float* a,
                            lapack_int lda, float* s, float* scond,
                            float* amax );
lapack_int LAPACKE_dpoequb( int matrix_layout, lapack_int n, const double* a,
                            lapack_int lda, double* s, double* scond,
                            double* amax );
lapack_int LAPACKE_cpoequb( int matrix_layout, lapack_int n,
                            const lapack_complex_float* a, lapack_int lda,
                            float* s, float* scond, float* amax );
lapack_int LAPACKE_zpoequb( int matrix_layout, lapack_int n,
                            const lapack_complex_double* a, lapack_int lda,
                            double* s, double* scond, double* amax );

lapack_int LAPACKE_sporfs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const float* a, lapack_int lda,
                           const float* af, lapack_int ldaf, const float* b,
                           lapack_int ldb, float* x, lapack_int ldx,
                           float* ferr, float* berr );
lapack_int LAPACKE_dporfs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const double* a, lapack_int lda,
                           const double* af, lapack_int ldaf, const double* b,
                           lapack_int ldb, double* x, lapack_int ldx,
                           double* ferr, double* berr );
lapack_int LAPACKE_cporfs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_float* a,
                           lapack_int lda, const lapack_complex_float* af,
                           lapack_int ldaf, const lapack_complex_float* b,
                           lapack_int ldb, lapack_complex_float* x,
                           lapack_int ldx, float* ferr, float* berr );
lapack_int LAPACKE_zporfs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_double* a,
                           lapack_int lda, const lapack_complex_double* af,
                           lapack_int ldaf, const lapack_complex_double* b,
                           lapack_int ldb, lapack_complex_double* x,
                           lapack_int ldx, double* ferr, double* berr );

lapack_int LAPACKE_sporfsx( int matrix_layout, char uplo, char equed,
                            lapack_int n, lapack_int nrhs, const float* a,
                            lapack_int lda, const float* af, lapack_int ldaf,
                            const float* s, const float* b, lapack_int ldb,
                            float* x, lapack_int ldx, float* rcond, float* berr,
                            lapack_int n_err_bnds, float* err_bnds_norm,
                            float* err_bnds_comp, lapack_int nparams,
                            float* params );
lapack_int LAPACKE_dporfsx( int matrix_layout, char uplo, char equed,
                            lapack_int n, lapack_int nrhs, const double* a,
                            lapack_int lda, const double* af, lapack_int ldaf,
                            const double* s, const double* b, lapack_int ldb,
                            double* x, lapack_int ldx, double* rcond,
                            double* berr, lapack_int n_err_bnds,
                            double* err_bnds_norm, double* err_bnds_comp,
                            lapack_int nparams, double* params );
lapack_int LAPACKE_cporfsx( int matrix_layout, char uplo, char equed,
                            lapack_int n, lapack_int nrhs,
                            const lapack_complex_float* a, lapack_int lda,
                            const lapack_complex_float* af, lapack_int ldaf,
                            const float* s, const lapack_complex_float* b,
                            lapack_int ldb, lapack_complex_float* x,
                            lapack_int ldx, float* rcond, float* berr,
                            lapack_int n_err_bnds, float* err_bnds_norm,
                            float* err_bnds_comp, lapack_int nparams,
                            float* params );
lapack_int LAPACKE_zporfsx( int matrix_layout, char uplo, char equed,
                            lapack_int n, lapack_int nrhs,
                            const lapack_complex_double* a, lapack_int lda,
                            const lapack_complex_double* af, lapack_int ldaf,
                            const double* s, const lapack_complex_double* b,
                            lapack_int ldb, lapack_complex_double* x,
                            lapack_int ldx, double* rcond, double* berr,
                            lapack_int n_err_bnds, double* err_bnds_norm,
                            double* err_bnds_comp, lapack_int nparams,
                            double* params );

lapack_int LAPACKE_sposv( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, float* a, lapack_int lda, float* b,
                          lapack_int ldb );
lapack_int LAPACKE_dposv( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, double* a, lapack_int lda, double* b,
                          lapack_int ldb );
lapack_int LAPACKE_cposv( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, lapack_complex_float* a,
                          lapack_int lda, lapack_complex_float* b,
                          lapack_int ldb );
lapack_int LAPACKE_zposv( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, lapack_complex_double* a,
                          lapack_int lda, lapack_complex_double* b,
                          lapack_int ldb );
lapack_int LAPACKE_dsposv( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, double* a, lapack_int lda,
                           double* b, lapack_int ldb, double* x, lapack_int ldx,
                           lapack_int* iter );
lapack_int LAPACKE_zcposv( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, lapack_complex_double* a,
                           lapack_int lda, lapack_complex_double* b,
                           lapack_int ldb, lapack_complex_double* x,
                           lapack_int ldx, lapack_int* iter );

lapack_int LAPACKE_sposvx( int matrix_layout, char fact, char uplo, lapack_int n,
                           lapack_int nrhs, float* a, lapack_int lda, float* af,
                           lapack_int ldaf, char* equed, float* s, float* b,
                           lapack_int ldb, float* x, lapack_int ldx,
                           float* rcond, float* ferr, float* berr );
lapack_int LAPACKE_dposvx( int matrix_layout, char fact, char uplo, lapack_int n,
                           lapack_int nrhs, double* a, lapack_int lda,
                           double* af, lapack_int ldaf, char* equed, double* s,
                           double* b, lapack_int ldb, double* x, lapack_int ldx,
                           double* rcond, double* ferr, double* berr );
lapack_int LAPACKE_cposvx( int matrix_layout, char fact, char uplo, lapack_int n,
                           lapack_int nrhs, lapack_complex_float* a,
                           lapack_int lda, lapack_complex_float* af,
                           lapack_int ldaf, char* equed, float* s,
                           lapack_complex_float* b, lapack_int ldb,
                           lapack_complex_float* x, lapack_int ldx,
                           float* rcond, float* ferr, float* berr );
lapack_int LAPACKE_zposvx( int matrix_layout, char fact, char uplo, lapack_int n,
                           lapack_int nrhs, lapack_complex_double* a,
                           lapack_int lda, lapack_complex_double* af,
                           lapack_int ldaf, char* equed, double* s,
                           lapack_complex_double* b, lapack_int ldb,
                           lapack_complex_double* x, lapack_int ldx,
                           double* rcond, double* ferr, double* berr );

lapack_int LAPACKE_sposvxx( int matrix_layout, char fact, char uplo,
                            lapack_int n, lapack_int nrhs, float* a,
                            lapack_int lda, float* af, lapack_int ldaf,
                            char* equed, float* s, float* b, lapack_int ldb,
                            float* x, lapack_int ldx, float* rcond,
                            float* rpvgrw, float* berr, lapack_int n_err_bnds,
                            float* err_bnds_norm, float* err_bnds_comp,
                            lapack_int nparams, float* params );
lapack_int LAPACKE_dposvxx( int matrix_layout, char fact, char uplo,
                            lapack_int n, lapack_int nrhs, double* a,
                            lapack_int lda, double* af, lapack_int ldaf,
                            char* equed, double* s, double* b, lapack_int ldb,
                            double* x, lapack_int ldx, double* rcond,
                            double* rpvgrw, double* berr, lapack_int n_err_bnds,
                            double* err_bnds_norm, double* err_bnds_comp,
                            lapack_int nparams, double* params );
lapack_int LAPACKE_cposvxx( int matrix_layout, char fact, char uplo,
                            lapack_int n, lapack_int nrhs,
                            lapack_complex_float* a, lapack_int lda,
                            lapack_complex_float* af, lapack_int ldaf,
                            char* equed, float* s, lapack_complex_float* b,
                            lapack_int ldb, lapack_complex_float* x,
                            lapack_int ldx, float* rcond, float* rpvgrw,
                            float* berr, lapack_int n_err_bnds,
                            float* err_bnds_norm, float* err_bnds_comp,
                            lapack_int nparams, float* params );
lapack_int LAPACKE_zposvxx( int matrix_layout, char fact, char uplo,
                            lapack_int n, lapack_int nrhs,
                            lapack_complex_double* a, lapack_int lda,
                            lapack_complex_double* af, lapack_int ldaf,
                            char* equed, double* s, lapack_complex_double* b,
                            lapack_int ldb, lapack_complex_double* x,
                            lapack_int ldx, double* rcond, double* rpvgrw,
                            double* berr, lapack_int n_err_bnds,
                            double* err_bnds_norm, double* err_bnds_comp,
                            lapack_int nparams, double* params );

lapack_int LAPACKE_spotrf2( int matrix_layout, char uplo, lapack_int n, float* a,
                           lapack_int lda );
lapack_int LAPACKE_dpotrf2( int matrix_layout, char uplo, lapack_int n, double* a,
                           lapack_int lda );
lapack_int LAPACKE_cpotrf2( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_float* a, lapack_int lda );
lapack_int LAPACKE_zpotrf2( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_double* a, lapack_int lda );

lapack_int LAPACKE_spotrf( int matrix_layout, char uplo, lapack_int n, float* a,
                           lapack_int lda );
lapack_int LAPACKE_dpotrf( int matrix_layout, char uplo, lapack_int n, double* a,
                           lapack_int lda );
lapack_int LAPACKE_cpotrf( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_float* a, lapack_int lda );
lapack_int LAPACKE_zpotrf( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_double* a, lapack_int lda );

lapack_int LAPACKE_spotri( int matrix_layout, char uplo, lapack_int n, float* a,
                           lapack_int lda );
lapack_int LAPACKE_dpotri( int matrix_layout, char uplo, lapack_int n, double* a,
                           lapack_int lda );
lapack_int LAPACKE_cpotri( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_float* a, lapack_int lda );
lapack_int LAPACKE_zpotri( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_double* a, lapack_int lda );

lapack_int LAPACKE_spotrs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const float* a, lapack_int lda,
                           float* b, lapack_int ldb );
lapack_int LAPACKE_dpotrs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const double* a, lapack_int lda,
                           double* b, lapack_int ldb );
lapack_int LAPACKE_cpotrs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_float* a,
                           lapack_int lda, lapack_complex_float* b,
                           lapack_int ldb );
lapack_int LAPACKE_zpotrs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_double* a,
                           lapack_int lda, lapack_complex_double* b,
                           lapack_int ldb );

lapack_int LAPACKE_sppcon( int matrix_layout, char uplo, lapack_int n,
                           const float* ap, float anorm, float* rcond );
lapack_int LAPACKE_dppcon( int matrix_layout, char uplo, lapack_int n,
                           const double* ap, double anorm, double* rcond );
lapack_int LAPACKE_cppcon( int matrix_layout, char uplo, lapack_int n,
                           const lapack_complex_float* ap, float anorm,
                           float* rcond );
lapack_int LAPACKE_zppcon( int matrix_layout, char uplo, lapack_int n,
                           const lapack_complex_double* ap, double anorm,
                           double* rcond );

lapack_int LAPACKE_sppequ( int matrix_layout, char uplo, lapack_int n,
                           const float* ap, float* s, float* scond,
                           float* amax );
lapack_int LAPACKE_dppequ( int matrix_layout, char uplo, lapack_int n,
                           const double* ap, double* s, double* scond,
                           double* amax );
lapack_int LAPACKE_cppequ( int matrix_layout, char uplo, lapack_int n,
                           const lapack_complex_float* ap, float* s,
                           float* scond, float* amax );
lapack_int LAPACKE_zppequ( int matrix_layout, char uplo, lapack_int n,
                           const lapack_complex_double* ap, double* s,
                           double* scond, double* amax );

lapack_int LAPACKE_spprfs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const float* ap, const float* afp,
                           const float* b, lapack_int ldb, float* x,
                           lapack_int ldx, float* ferr, float* berr );
lapack_int LAPACKE_dpprfs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const double* ap, const double* afp,
                           const double* b, lapack_int ldb, double* x,
                           lapack_int ldx, double* ferr, double* berr );
lapack_int LAPACKE_cpprfs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_float* ap,
                           const lapack_complex_float* afp,
                           const lapack_complex_float* b, lapack_int ldb,
                           lapack_complex_float* x, lapack_int ldx, float* ferr,
                           float* berr );
lapack_int LAPACKE_zpprfs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_double* ap,
                           const lapack_complex_double* afp,
                           const lapack_complex_double* b, lapack_int ldb,
                           lapack_complex_double* x, lapack_int ldx,
                           double* ferr, double* berr );

lapack_int LAPACKE_sppsv( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, float* ap, float* b,
                          lapack_int ldb );
lapack_int LAPACKE_dppsv( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, double* ap, double* b,
                          lapack_int ldb );
lapack_int LAPACKE_cppsv( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, lapack_complex_float* ap,
                          lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zppsv( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, lapack_complex_double* ap,
                          lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_sppsvx( int matrix_layout, char fact, char uplo, lapack_int n,
                           lapack_int nrhs, float* ap, float* afp, char* equed,
                           float* s, float* b, lapack_int ldb, float* x,
                           lapack_int ldx, float* rcond, float* ferr,
                           float* berr );
lapack_int LAPACKE_dppsvx( int matrix_layout, char fact, char uplo, lapack_int n,
                           lapack_int nrhs, double* ap, double* afp,
                           char* equed, double* s, double* b, lapack_int ldb,
                           double* x, lapack_int ldx, double* rcond,
                           double* ferr, double* berr );
lapack_int LAPACKE_cppsvx( int matrix_layout, char fact, char uplo, lapack_int n,
                           lapack_int nrhs, lapack_complex_float* ap,
                           lapack_complex_float* afp, char* equed, float* s,
                           lapack_complex_float* b, lapack_int ldb,
                           lapack_complex_float* x, lapack_int ldx,
                           float* rcond, float* ferr, float* berr );
lapack_int LAPACKE_zppsvx( int matrix_layout, char fact, char uplo, lapack_int n,
                           lapack_int nrhs, lapack_complex_double* ap,
                           lapack_complex_double* afp, char* equed, double* s,
                           lapack_complex_double* b, lapack_int ldb,
                           lapack_complex_double* x, lapack_int ldx,
                           double* rcond, double* ferr, double* berr );

lapack_int LAPACKE_spptrf( int matrix_layout, char uplo, lapack_int n,
                           float* ap );
lapack_int LAPACKE_dpptrf( int matrix_layout, char uplo, lapack_int n,
                           double* ap );
lapack_int LAPACKE_cpptrf( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_float* ap );
lapack_int LAPACKE_zpptrf( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_double* ap );

lapack_int LAPACKE_spptri( int matrix_layout, char uplo, lapack_int n,
                           float* ap );
lapack_int LAPACKE_dpptri( int matrix_layout, char uplo, lapack_int n,
                           double* ap );
lapack_int LAPACKE_cpptri( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_float* ap );
lapack_int LAPACKE_zpptri( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_double* ap );

lapack_int LAPACKE_spptrs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const float* ap, float* b,
                           lapack_int ldb );
lapack_int LAPACKE_dpptrs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const double* ap, double* b,
                           lapack_int ldb );
lapack_int LAPACKE_cpptrs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_float* ap,
                           lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zpptrs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_double* ap,
                           lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_spstrf( int matrix_layout, char uplo, lapack_int n, float* a,
                           lapack_int lda, lapack_int* piv, lapack_int* rank,
                           float tol );
lapack_int LAPACKE_dpstrf( int matrix_layout, char uplo, lapack_int n, double* a,
                           lapack_int lda, lapack_int* piv, lapack_int* rank,
                           double tol );
lapack_int LAPACKE_cpstrf( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_int* piv, lapack_int* rank, float tol );
lapack_int LAPACKE_zpstrf( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_int* piv, lapack_int* rank, double tol );

lapack_int LAPACKE_sptcon( lapack_int n, const float* d, const float* e,
                           float anorm, float* rcond );
lapack_int LAPACKE_dptcon( lapack_int n, const double* d, const double* e,
                           double anorm, double* rcond );
lapack_int LAPACKE_cptcon( lapack_int n, const float* d,
                           const lapack_complex_float* e, float anorm,
                           float* rcond );
lapack_int LAPACKE_zptcon( lapack_int n, const double* d,
                           const lapack_complex_double* e, double anorm,
                           double* rcond );

lapack_int LAPACKE_spteqr( int matrix_layout, char compz, lapack_int n, float* d,
                           float* e, float* z, lapack_int ldz );
lapack_int LAPACKE_dpteqr( int matrix_layout, char compz, lapack_int n,
                           double* d, double* e, double* z, lapack_int ldz );
lapack_int LAPACKE_cpteqr( int matrix_layout, char compz, lapack_int n, float* d,
                           float* e, lapack_complex_float* z, lapack_int ldz );
lapack_int LAPACKE_zpteqr( int matrix_layout, char compz, lapack_int n,
                           double* d, double* e, lapack_complex_double* z,
                           lapack_int ldz );

lapack_int LAPACKE_sptrfs( int matrix_layout, lapack_int n, lapack_int nrhs,
                           const float* d, const float* e, const float* df,
                           const float* ef, const float* b, lapack_int ldb,
                           float* x, lapack_int ldx, float* ferr, float* berr );
lapack_int LAPACKE_dptrfs( int matrix_layout, lapack_int n, lapack_int nrhs,
                           const double* d, const double* e, const double* df,
                           const double* ef, const double* b, lapack_int ldb,
                           double* x, lapack_int ldx, double* ferr,
                           double* berr );
lapack_int LAPACKE_cptrfs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const float* d,
                           const lapack_complex_float* e, const float* df,
                           const lapack_complex_float* ef,
                           const lapack_complex_float* b, lapack_int ldb,
                           lapack_complex_float* x, lapack_int ldx, float* ferr,
                           float* berr );
lapack_int LAPACKE_zptrfs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const double* d,
                           const lapack_complex_double* e, const double* df,
                           const lapack_complex_double* ef,
                           const lapack_complex_double* b, lapack_int ldb,
                           lapack_complex_double* x, lapack_int ldx,
                           double* ferr, double* berr );

lapack_int LAPACKE_sptsv( int matrix_layout, lapack_int n, lapack_int nrhs,
                          float* d, float* e, float* b, lapack_int ldb );
lapack_int LAPACKE_dptsv( int matrix_layout, lapack_int n, lapack_int nrhs,
                          double* d, double* e, double* b, lapack_int ldb );
lapack_int LAPACKE_cptsv( int matrix_layout, lapack_int n, lapack_int nrhs,
                          float* d, lapack_complex_float* e,
                          lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zptsv( int matrix_layout, lapack_int n, lapack_int nrhs,
                          double* d, lapack_complex_double* e,
                          lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_sptsvx( int matrix_layout, char fact, lapack_int n,
                           lapack_int nrhs, const float* d, const float* e,
                           float* df, float* ef, const float* b, lapack_int ldb,
                           float* x, lapack_int ldx, float* rcond, float* ferr,
                           float* berr );
lapack_int LAPACKE_dptsvx( int matrix_layout, char fact, lapack_int n,
                           lapack_int nrhs, const double* d, const double* e,
                           double* df, double* ef, const double* b,
                           lapack_int ldb, double* x, lapack_int ldx,
                           double* rcond, double* ferr, double* berr );
lapack_int LAPACKE_cptsvx( int matrix_layout, char fact, lapack_int n,
                           lapack_int nrhs, const float* d,
                           const lapack_complex_float* e, float* df,
                           lapack_complex_float* ef,
                           const lapack_complex_float* b, lapack_int ldb,
                           lapack_complex_float* x, lapack_int ldx,
                           float* rcond, float* ferr, float* berr );
lapack_int LAPACKE_zptsvx( int matrix_layout, char fact, lapack_int n,
                           lapack_int nrhs, const double* d,
                           const lapack_complex_double* e, double* df,
                           lapack_complex_double* ef,
                           const lapack_complex_double* b, lapack_int ldb,
                           lapack_complex_double* x, lapack_int ldx,
                           double* rcond, double* ferr, double* berr );

lapack_int LAPACKE_spttrf( lapack_int n, float* d, float* e );
lapack_int LAPACKE_dpttrf( lapack_int n, double* d, double* e );
lapack_int LAPACKE_cpttrf( lapack_int n, float* d, lapack_complex_float* e );
lapack_int LAPACKE_zpttrf( lapack_int n, double* d, lapack_complex_double* e );

lapack_int LAPACKE_spttrs( int matrix_layout, lapack_int n, lapack_int nrhs,
                           const float* d, const float* e, float* b,
                           lapack_int ldb );
lapack_int LAPACKE_dpttrs( int matrix_layout, lapack_int n, lapack_int nrhs,
                           const double* d, const double* e, double* b,
                           lapack_int ldb );
lapack_int LAPACKE_cpttrs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const float* d,
                           const lapack_complex_float* e,
                           lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zpttrs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const double* d,
                           const lapack_complex_double* e,
                           lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_ssbev( int matrix_layout, char jobz, char uplo, lapack_int n,
                          lapack_int kd, float* ab, lapack_int ldab, float* w,
                          float* z, lapack_int ldz );
lapack_int LAPACKE_dsbev( int matrix_layout, char jobz, char uplo, lapack_int n,
                          lapack_int kd, double* ab, lapack_int ldab, double* w,
                          double* z, lapack_int ldz );

lapack_int LAPACKE_ssbevd( int matrix_layout, char jobz, char uplo, lapack_int n,
                           lapack_int kd, float* ab, lapack_int ldab, float* w,
                           float* z, lapack_int ldz );
lapack_int LAPACKE_dsbevd( int matrix_layout, char jobz, char uplo, lapack_int n,
                           lapack_int kd, double* ab, lapack_int ldab,
                           double* w, double* z, lapack_int ldz );

lapack_int LAPACKE_ssbevx( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, lapack_int kd, float* ab,
                           lapack_int ldab, float* q, lapack_int ldq, float vl,
                           float vu, lapack_int il, lapack_int iu, float abstol,
                           lapack_int* m, float* w, float* z, lapack_int ldz,
                           lapack_int* ifail );
lapack_int LAPACKE_dsbevx( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, lapack_int kd, double* ab,
                           lapack_int ldab, double* q, lapack_int ldq,
                           double vl, double vu, lapack_int il, lapack_int iu,
                           double abstol, lapack_int* m, double* w, double* z,
                           lapack_int ldz, lapack_int* ifail );

lapack_int LAPACKE_ssbgst( int matrix_layout, char vect, char uplo, lapack_int n,
                           lapack_int ka, lapack_int kb, float* ab,
                           lapack_int ldab, const float* bb, lapack_int ldbb,
                           float* x, lapack_int ldx );
lapack_int LAPACKE_dsbgst( int matrix_layout, char vect, char uplo, lapack_int n,
                           lapack_int ka, lapack_int kb, double* ab,
                           lapack_int ldab, const double* bb, lapack_int ldbb,
                           double* x, lapack_int ldx );

lapack_int LAPACKE_ssbgv( int matrix_layout, char jobz, char uplo, lapack_int n,
                          lapack_int ka, lapack_int kb, float* ab,
                          lapack_int ldab, float* bb, lapack_int ldbb, float* w,
                          float* z, lapack_int ldz );
lapack_int LAPACKE_dsbgv( int matrix_layout, char jobz, char uplo, lapack_int n,
                          lapack_int ka, lapack_int kb, double* ab,
                          lapack_int ldab, double* bb, lapack_int ldbb,
                          double* w, double* z, lapack_int ldz );

lapack_int LAPACKE_ssbgvd( int matrix_layout, char jobz, char uplo, lapack_int n,
                           lapack_int ka, lapack_int kb, float* ab,
                           lapack_int ldab, float* bb, lapack_int ldbb,
                           float* w, float* z, lapack_int ldz );
lapack_int LAPACKE_dsbgvd( int matrix_layout, char jobz, char uplo, lapack_int n,
                           lapack_int ka, lapack_int kb, double* ab,
                           lapack_int ldab, double* bb, lapack_int ldbb,
                           double* w, double* z, lapack_int ldz );

lapack_int LAPACKE_ssbgvx( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, lapack_int ka, lapack_int kb,
                           float* ab, lapack_int ldab, float* bb,
                           lapack_int ldbb, float* q, lapack_int ldq, float vl,
                           float vu, lapack_int il, lapack_int iu, float abstol,
                           lapack_int* m, float* w, float* z, lapack_int ldz,
                           lapack_int* ifail );
lapack_int LAPACKE_dsbgvx( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, lapack_int ka, lapack_int kb,
                           double* ab, lapack_int ldab, double* bb,
                           lapack_int ldbb, double* q, lapack_int ldq,
                           double vl, double vu, lapack_int il, lapack_int iu,
                           double abstol, lapack_int* m, double* w, double* z,
                           lapack_int ldz, lapack_int* ifail );

lapack_int LAPACKE_ssbtrd( int matrix_layout, char vect, char uplo, lapack_int n,
                           lapack_int kd, float* ab, lapack_int ldab, float* d,
                           float* e, float* q, lapack_int ldq );
lapack_int LAPACKE_dsbtrd( int matrix_layout, char vect, char uplo, lapack_int n,
                           lapack_int kd, double* ab, lapack_int ldab,
                           double* d, double* e, double* q, lapack_int ldq );

lapack_int LAPACKE_ssfrk( int matrix_layout, char transr, char uplo, char trans,
                          lapack_int n, lapack_int k, float alpha,
                          const float* a, lapack_int lda, float beta,
                          float* c );
lapack_int LAPACKE_dsfrk( int matrix_layout, char transr, char uplo, char trans,
                          lapack_int n, lapack_int k, double alpha,
                          const double* a, lapack_int lda, double beta,
                          double* c );

lapack_int LAPACKE_sspcon( int matrix_layout, char uplo, lapack_int n,
                           const float* ap, const lapack_int* ipiv, float anorm,
                           float* rcond );
lapack_int LAPACKE_dspcon( int matrix_layout, char uplo, lapack_int n,
                           const double* ap, const lapack_int* ipiv,
                           double anorm, double* rcond );
lapack_int LAPACKE_cspcon( int matrix_layout, char uplo, lapack_int n,
                           const lapack_complex_float* ap,
                           const lapack_int* ipiv, float anorm, float* rcond );
lapack_int LAPACKE_zspcon( int matrix_layout, char uplo, lapack_int n,
                           const lapack_complex_double* ap,
                           const lapack_int* ipiv, double anorm,
                           double* rcond );

lapack_int LAPACKE_sspev( int matrix_layout, char jobz, char uplo, lapack_int n,
                          float* ap, float* w, float* z, lapack_int ldz );
lapack_int LAPACKE_dspev( int matrix_layout, char jobz, char uplo, lapack_int n,
                          double* ap, double* w, double* z, lapack_int ldz );

lapack_int LAPACKE_sspevd( int matrix_layout, char jobz, char uplo, lapack_int n,
                           float* ap, float* w, float* z, lapack_int ldz );
lapack_int LAPACKE_dspevd( int matrix_layout, char jobz, char uplo, lapack_int n,
                           double* ap, double* w, double* z, lapack_int ldz );

lapack_int LAPACKE_sspevx( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, float* ap, float vl, float vu,
                           lapack_int il, lapack_int iu, float abstol,
                           lapack_int* m, float* w, float* z, lapack_int ldz,
                           lapack_int* ifail );
lapack_int LAPACKE_dspevx( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, double* ap, double vl, double vu,
                           lapack_int il, lapack_int iu, double abstol,
                           lapack_int* m, double* w, double* z, lapack_int ldz,
                           lapack_int* ifail );

lapack_int LAPACKE_sspgst( int matrix_layout, lapack_int itype, char uplo,
                           lapack_int n, float* ap, const float* bp );
lapack_int LAPACKE_dspgst( int matrix_layout, lapack_int itype, char uplo,
                           lapack_int n, double* ap, const double* bp );

lapack_int LAPACKE_sspgv( int matrix_layout, lapack_int itype, char jobz,
                          char uplo, lapack_int n, float* ap, float* bp,
                          float* w, float* z, lapack_int ldz );
lapack_int LAPACKE_dspgv( int matrix_layout, lapack_int itype, char jobz,
                          char uplo, lapack_int n, double* ap, double* bp,
                          double* w, double* z, lapack_int ldz );

lapack_int LAPACKE_sspgvd( int matrix_layout, lapack_int itype, char jobz,
                           char uplo, lapack_int n, float* ap, float* bp,
                           float* w, float* z, lapack_int ldz );
lapack_int LAPACKE_dspgvd( int matrix_layout, lapack_int itype, char jobz,
                           char uplo, lapack_int n, double* ap, double* bp,
                           double* w, double* z, lapack_int ldz );

lapack_int LAPACKE_sspgvx( int matrix_layout, lapack_int itype, char jobz,
                           char range, char uplo, lapack_int n, float* ap,
                           float* bp, float vl, float vu, lapack_int il,
                           lapack_int iu, float abstol, lapack_int* m, float* w,
                           float* z, lapack_int ldz, lapack_int* ifail );
lapack_int LAPACKE_dspgvx( int matrix_layout, lapack_int itype, char jobz,
                           char range, char uplo, lapack_int n, double* ap,
                           double* bp, double vl, double vu, lapack_int il,
                           lapack_int iu, double abstol, lapack_int* m,
                           double* w, double* z, lapack_int ldz,
                           lapack_int* ifail );

lapack_int LAPACKE_ssprfs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const float* ap, const float* afp,
                           const lapack_int* ipiv, const float* b,
                           lapack_int ldb, float* x, lapack_int ldx,
                           float* ferr, float* berr );
lapack_int LAPACKE_dsprfs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const double* ap, const double* afp,
                           const lapack_int* ipiv, const double* b,
                           lapack_int ldb, double* x, lapack_int ldx,
                           double* ferr, double* berr );
lapack_int LAPACKE_csprfs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_float* ap,
                           const lapack_complex_float* afp,
                           const lapack_int* ipiv,
                           const lapack_complex_float* b, lapack_int ldb,
                           lapack_complex_float* x, lapack_int ldx, float* ferr,
                           float* berr );
lapack_int LAPACKE_zsprfs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_double* ap,
                           const lapack_complex_double* afp,
                           const lapack_int* ipiv,
                           const lapack_complex_double* b, lapack_int ldb,
                           lapack_complex_double* x, lapack_int ldx,
                           double* ferr, double* berr );

lapack_int LAPACKE_sspsv( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, float* ap, lapack_int* ipiv,
                          float* b, lapack_int ldb );
lapack_int LAPACKE_dspsv( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, double* ap, lapack_int* ipiv,
                          double* b, lapack_int ldb );
lapack_int LAPACKE_cspsv( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, lapack_complex_float* ap,
                          lapack_int* ipiv, lapack_complex_float* b,
                          lapack_int ldb );
lapack_int LAPACKE_zspsv( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, lapack_complex_double* ap,
                          lapack_int* ipiv, lapack_complex_double* b,
                          lapack_int ldb );

lapack_int LAPACKE_sspsvx( int matrix_layout, char fact, char uplo, lapack_int n,
                           lapack_int nrhs, const float* ap, float* afp,
                           lapack_int* ipiv, const float* b, lapack_int ldb,
                           float* x, lapack_int ldx, float* rcond, float* ferr,
                           float* berr );
lapack_int LAPACKE_dspsvx( int matrix_layout, char fact, char uplo, lapack_int n,
                           lapack_int nrhs, const double* ap, double* afp,
                           lapack_int* ipiv, const double* b, lapack_int ldb,
                           double* x, lapack_int ldx, double* rcond,
                           double* ferr, double* berr );
lapack_int LAPACKE_cspsvx( int matrix_layout, char fact, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_float* ap,
                           lapack_complex_float* afp, lapack_int* ipiv,
                           const lapack_complex_float* b, lapack_int ldb,
                           lapack_complex_float* x, lapack_int ldx,
                           float* rcond, float* ferr, float* berr );
lapack_int LAPACKE_zspsvx( int matrix_layout, char fact, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_double* ap,
                           lapack_complex_double* afp, lapack_int* ipiv,
                           const lapack_complex_double* b, lapack_int ldb,
                           lapack_complex_double* x, lapack_int ldx,
                           double* rcond, double* ferr, double* berr );

lapack_int LAPACKE_ssptrd( int matrix_layout, char uplo, lapack_int n, float* ap,
                           float* d, float* e, float* tau );
lapack_int LAPACKE_dsptrd( int matrix_layout, char uplo, lapack_int n,
                           double* ap, double* d, double* e, double* tau );

lapack_int LAPACKE_ssptrf( int matrix_layout, char uplo, lapack_int n, float* ap,
                           lapack_int* ipiv );
lapack_int LAPACKE_dsptrf( int matrix_layout, char uplo, lapack_int n,
                           double* ap, lapack_int* ipiv );
lapack_int LAPACKE_csptrf( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_float* ap, lapack_int* ipiv );
lapack_int LAPACKE_zsptrf( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_double* ap, lapack_int* ipiv );

lapack_int LAPACKE_ssptri( int matrix_layout, char uplo, lapack_int n, float* ap,
                           const lapack_int* ipiv );
lapack_int LAPACKE_dsptri( int matrix_layout, char uplo, lapack_int n,
                           double* ap, const lapack_int* ipiv );
lapack_int LAPACKE_csptri( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_float* ap, const lapack_int* ipiv );
lapack_int LAPACKE_zsptri( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_double* ap, const lapack_int* ipiv );

lapack_int LAPACKE_ssptrs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const float* ap,
                           const lapack_int* ipiv, float* b, lapack_int ldb );
lapack_int LAPACKE_dsptrs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const double* ap,
                           const lapack_int* ipiv, double* b, lapack_int ldb );
lapack_int LAPACKE_csptrs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_float* ap,
                           const lapack_int* ipiv, lapack_complex_float* b,
                           lapack_int ldb );
lapack_int LAPACKE_zsptrs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_double* ap,
                           const lapack_int* ipiv, lapack_complex_double* b,
                           lapack_int ldb );

lapack_int LAPACKE_sstebz( char range, char order, lapack_int n, float vl,
                           float vu, lapack_int il, lapack_int iu, float abstol,
                           const float* d, const float* e, lapack_int* m,
                           lapack_int* nsplit, float* w, lapack_int* iblock,
                           lapack_int* isplit );
lapack_int LAPACKE_dstebz( char range, char order, lapack_int n, double vl,
                           double vu, lapack_int il, lapack_int iu,
                           double abstol, const double* d, const double* e,
                           lapack_int* m, lapack_int* nsplit, double* w,
                           lapack_int* iblock, lapack_int* isplit );

lapack_int LAPACKE_sstedc( int matrix_layout, char compz, lapack_int n, float* d,
                           float* e, float* z, lapack_int ldz );
lapack_int LAPACKE_dstedc( int matrix_layout, char compz, lapack_int n,
                           double* d, double* e, double* z, lapack_int ldz );
lapack_int LAPACKE_cstedc( int matrix_layout, char compz, lapack_int n, float* d,
                           float* e, lapack_complex_float* z, lapack_int ldz );
lapack_int LAPACKE_zstedc( int matrix_layout, char compz, lapack_int n,
                           double* d, double* e, lapack_complex_double* z,
                           lapack_int ldz );

lapack_int LAPACKE_sstegr( int matrix_layout, char jobz, char range,
                           lapack_int n, float* d, float* e, float vl, float vu,
                           lapack_int il, lapack_int iu, float abstol,
                           lapack_int* m, float* w, float* z, lapack_int ldz,
                           lapack_int* isuppz );
lapack_int LAPACKE_dstegr( int matrix_layout, char jobz, char range,
                           lapack_int n, double* d, double* e, double vl,
                           double vu, lapack_int il, lapack_int iu,
                           double abstol, lapack_int* m, double* w, double* z,
                           lapack_int ldz, lapack_int* isuppz );
lapack_int LAPACKE_cstegr( int matrix_layout, char jobz, char range,
                           lapack_int n, float* d, float* e, float vl, float vu,
                           lapack_int il, lapack_int iu, float abstol,
                           lapack_int* m, float* w, lapack_complex_float* z,
                           lapack_int ldz, lapack_int* isuppz );
lapack_int LAPACKE_zstegr( int matrix_layout, char jobz, char range,
                           lapack_int n, double* d, double* e, double vl,
                           double vu, lapack_int il, lapack_int iu,
                           double abstol, lapack_int* m, double* w,
                           lapack_complex_double* z, lapack_int ldz,
                           lapack_int* isuppz );

lapack_int LAPACKE_sstein( int matrix_layout, lapack_int n, const float* d,
                           const float* e, lapack_int m, const float* w,
                           const lapack_int* iblock, const lapack_int* isplit,
                           float* z, lapack_int ldz, lapack_int* ifailv );
lapack_int LAPACKE_dstein( int matrix_layout, lapack_int n, const double* d,
                           const double* e, lapack_int m, const double* w,
                           const lapack_int* iblock, const lapack_int* isplit,
                           double* z, lapack_int ldz, lapack_int* ifailv );
lapack_int LAPACKE_cstein( int matrix_layout, lapack_int n, const float* d,
                           const float* e, lapack_int m, const float* w,
                           const lapack_int* iblock, const lapack_int* isplit,
                           lapack_complex_float* z, lapack_int ldz,
                           lapack_int* ifailv );
lapack_int LAPACKE_zstein( int matrix_layout, lapack_int n, const double* d,
                           const double* e, lapack_int m, const double* w,
                           const lapack_int* iblock, const lapack_int* isplit,
                           lapack_complex_double* z, lapack_int ldz,
                           lapack_int* ifailv );

lapack_int LAPACKE_sstemr( int matrix_layout, char jobz, char range,
                           lapack_int n, float* d, float* e, float vl, float vu,
                           lapack_int il, lapack_int iu, lapack_int* m,
                           float* w, float* z, lapack_int ldz, lapack_int nzc,
                           lapack_int* isuppz, lapack_logical* tryrac );
lapack_int LAPACKE_dstemr( int matrix_layout, char jobz, char range,
                           lapack_int n, double* d, double* e, double vl,
                           double vu, lapack_int il, lapack_int iu,
                           lapack_int* m, double* w, double* z, lapack_int ldz,
                           lapack_int nzc, lapack_int* isuppz,
                           lapack_logical* tryrac );
lapack_int LAPACKE_cstemr( int matrix_layout, char jobz, char range,
                           lapack_int n, float* d, float* e, float vl, float vu,
                           lapack_int il, lapack_int iu, lapack_int* m,
                           float* w, lapack_complex_float* z, lapack_int ldz,
                           lapack_int nzc, lapack_int* isuppz,
                           lapack_logical* tryrac );
lapack_int LAPACKE_zstemr( int matrix_layout, char jobz, char range,
                           lapack_int n, double* d, double* e, double vl,
                           double vu, lapack_int il, lapack_int iu,
                           lapack_int* m, double* w, lapack_complex_double* z,
                           lapack_int ldz, lapack_int nzc, lapack_int* isuppz,
                           lapack_logical* tryrac );

lapack_int LAPACKE_ssteqr( int matrix_layout, char compz, lapack_int n, float* d,
                           float* e, float* z, lapack_int ldz );
lapack_int LAPACKE_dsteqr( int matrix_layout, char compz, lapack_int n,
                           double* d, double* e, double* z, lapack_int ldz );
lapack_int LAPACKE_csteqr( int matrix_layout, char compz, lapack_int n, float* d,
                           float* e, lapack_complex_float* z, lapack_int ldz );
lapack_int LAPACKE_zsteqr( int matrix_layout, char compz, lapack_int n,
                           double* d, double* e, lapack_complex_double* z,
                           lapack_int ldz );

lapack_int LAPACKE_ssterf( lapack_int n, float* d, float* e );
lapack_int LAPACKE_dsterf( lapack_int n, double* d, double* e );

lapack_int LAPACKE_sstev( int matrix_layout, char jobz, lapack_int n, float* d,
                          float* e, float* z, lapack_int ldz );
lapack_int LAPACKE_dstev( int matrix_layout, char jobz, lapack_int n, double* d,
                          double* e, double* z, lapack_int ldz );

lapack_int LAPACKE_sstevd( int matrix_layout, char jobz, lapack_int n, float* d,
                           float* e, float* z, lapack_int ldz );
lapack_int LAPACKE_dstevd( int matrix_layout, char jobz, lapack_int n, double* d,
                           double* e, double* z, lapack_int ldz );

lapack_int LAPACKE_sstevr( int matrix_layout, char jobz, char range,
                           lapack_int n, float* d, float* e, float vl, float vu,
                           lapack_int il, lapack_int iu, float abstol,
                           lapack_int* m, float* w, float* z, lapack_int ldz,
                           lapack_int* isuppz );
lapack_int LAPACKE_dstevr( int matrix_layout, char jobz, char range,
                           lapack_int n, double* d, double* e, double vl,
                           double vu, lapack_int il, lapack_int iu,
                           double abstol, lapack_int* m, double* w, double* z,
                           lapack_int ldz, lapack_int* isuppz );

lapack_int LAPACKE_sstevx( int matrix_layout, char jobz, char range,
                           lapack_int n, float* d, float* e, float vl, float vu,
                           lapack_int il, lapack_int iu, float abstol,
                           lapack_int* m, float* w, float* z, lapack_int ldz,
                           lapack_int* ifail );
lapack_int LAPACKE_dstevx( int matrix_layout, char jobz, char range,
                           lapack_int n, double* d, double* e, double vl,
                           double vu, lapack_int il, lapack_int iu,
                           double abstol, lapack_int* m, double* w, double* z,
                           lapack_int ldz, lapack_int* ifail );

lapack_int LAPACKE_ssycon( int matrix_layout, char uplo, lapack_int n,
                           const float* a, lapack_int lda,
                           const lapack_int* ipiv, float anorm, float* rcond );
lapack_int LAPACKE_dsycon( int matrix_layout, char uplo, lapack_int n,
                           const double* a, lapack_int lda,
                           const lapack_int* ipiv, double anorm,
                           double* rcond );
lapack_int LAPACKE_csycon( int matrix_layout, char uplo, lapack_int n,
                           const lapack_complex_float* a, lapack_int lda,
                           const lapack_int* ipiv, float anorm, float* rcond );
lapack_int LAPACKE_zsycon( int matrix_layout, char uplo, lapack_int n,
                           const lapack_complex_double* a, lapack_int lda,
                           const lapack_int* ipiv, double anorm,
                           double* rcond );

lapack_int LAPACKE_ssyequb( int matrix_layout, char uplo, lapack_int n,
                            const float* a, lapack_int lda, float* s,
                            float* scond, float* amax );
lapack_int LAPACKE_dsyequb( int matrix_layout, char uplo, lapack_int n,
                            const double* a, lapack_int lda, double* s,
                            double* scond, double* amax );
lapack_int LAPACKE_csyequb( int matrix_layout, char uplo, lapack_int n,
                            const lapack_complex_float* a, lapack_int lda,
                            float* s, float* scond, float* amax );
lapack_int LAPACKE_zsyequb( int matrix_layout, char uplo, lapack_int n,
                            const lapack_complex_double* a, lapack_int lda,
                            double* s, double* scond, double* amax );

lapack_int LAPACKE_ssyev( int matrix_layout, char jobz, char uplo, lapack_int n,
                          float* a, lapack_int lda, float* w );
lapack_int LAPACKE_dsyev( int matrix_layout, char jobz, char uplo, lapack_int n,
                          double* a, lapack_int lda, double* w );

lapack_int LAPACKE_ssyevd( int matrix_layout, char jobz, char uplo, lapack_int n,
                           float* a, lapack_int lda, float* w );
lapack_int LAPACKE_dsyevd( int matrix_layout, char jobz, char uplo, lapack_int n,
                           double* a, lapack_int lda, double* w );

lapack_int LAPACKE_ssyevr( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, float* a, lapack_int lda, float vl,
                           float vu, lapack_int il, lapack_int iu, float abstol,
                           lapack_int* m, float* w, float* z, lapack_int ldz,
                           lapack_int* isuppz );
lapack_int LAPACKE_dsyevr( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, double* a, lapack_int lda, double vl,
                           double vu, lapack_int il, lapack_int iu,
                           double abstol, lapack_int* m, double* w, double* z,
                           lapack_int ldz, lapack_int* isuppz );

lapack_int LAPACKE_ssyevx( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, float* a, lapack_int lda, float vl,
                           float vu, lapack_int il, lapack_int iu, float abstol,
                           lapack_int* m, float* w, float* z, lapack_int ldz,
                           lapack_int* ifail );
lapack_int LAPACKE_dsyevx( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, double* a, lapack_int lda, double vl,
                           double vu, lapack_int il, lapack_int iu,
                           double abstol, lapack_int* m, double* w, double* z,
                           lapack_int ldz, lapack_int* ifail );

lapack_int LAPACKE_ssygst( int matrix_layout, lapack_int itype, char uplo,
                           lapack_int n, float* a, lapack_int lda,
                           const float* b, lapack_int ldb );
lapack_int LAPACKE_dsygst( int matrix_layout, lapack_int itype, char uplo,
                           lapack_int n, double* a, lapack_int lda,
                           const double* b, lapack_int ldb );

lapack_int LAPACKE_ssygv( int matrix_layout, lapack_int itype, char jobz,
                          char uplo, lapack_int n, float* a, lapack_int lda,
                          float* b, lapack_int ldb, float* w );
lapack_int LAPACKE_dsygv( int matrix_layout, lapack_int itype, char jobz,
                          char uplo, lapack_int n, double* a, lapack_int lda,
                          double* b, lapack_int ldb, double* w );

lapack_int LAPACKE_ssygvd( int matrix_layout, lapack_int itype, char jobz,
                           char uplo, lapack_int n, float* a, lapack_int lda,
                           float* b, lapack_int ldb, float* w );
lapack_int LAPACKE_dsygvd( int matrix_layout, lapack_int itype, char jobz,
                           char uplo, lapack_int n, double* a, lapack_int lda,
                           double* b, lapack_int ldb, double* w );

lapack_int LAPACKE_ssygvx( int matrix_layout, lapack_int itype, char jobz,
                           char range, char uplo, lapack_int n, float* a,
                           lapack_int lda, float* b, lapack_int ldb, float vl,
                           float vu, lapack_int il, lapack_int iu, float abstol,
                           lapack_int* m, float* w, float* z, lapack_int ldz,
                           lapack_int* ifail );
lapack_int LAPACKE_dsygvx( int matrix_layout, lapack_int itype, char jobz,
                           char range, char uplo, lapack_int n, double* a,
                           lapack_int lda, double* b, lapack_int ldb, double vl,
                           double vu, lapack_int il, lapack_int iu,
                           double abstol, lapack_int* m, double* w, double* z,
                           lapack_int ldz, lapack_int* ifail );

lapack_int LAPACKE_ssyrfs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const float* a, lapack_int lda,
                           const float* af, lapack_int ldaf,
                           const lapack_int* ipiv, const float* b,
                           lapack_int ldb, float* x, lapack_int ldx,
                           float* ferr, float* berr );
lapack_int LAPACKE_dsyrfs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const double* a, lapack_int lda,
                           const double* af, lapack_int ldaf,
                           const lapack_int* ipiv, const double* b,
                           lapack_int ldb, double* x, lapack_int ldx,
                           double* ferr, double* berr );
lapack_int LAPACKE_csyrfs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_float* a,
                           lapack_int lda, const lapack_complex_float* af,
                           lapack_int ldaf, const lapack_int* ipiv,
                           const lapack_complex_float* b, lapack_int ldb,
                           lapack_complex_float* x, lapack_int ldx, float* ferr,
                           float* berr );
lapack_int LAPACKE_zsyrfs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_double* a,
                           lapack_int lda, const lapack_complex_double* af,
                           lapack_int ldaf, const lapack_int* ipiv,
                           const lapack_complex_double* b, lapack_int ldb,
                           lapack_complex_double* x, lapack_int ldx,
                           double* ferr, double* berr );

lapack_int LAPACKE_ssyrfsx( int matrix_layout, char uplo, char equed,
                            lapack_int n, lapack_int nrhs, const float* a,
                            lapack_int lda, const float* af, lapack_int ldaf,
                            const lapack_int* ipiv, const float* s,
                            const float* b, lapack_int ldb, float* x,
                            lapack_int ldx, float* rcond, float* berr,
                            lapack_int n_err_bnds, float* err_bnds_norm,
                            float* err_bnds_comp, lapack_int nparams,
                            float* params );
lapack_int LAPACKE_dsyrfsx( int matrix_layout, char uplo, char equed,
                            lapack_int n, lapack_int nrhs, const double* a,
                            lapack_int lda, const double* af, lapack_int ldaf,
                            const lapack_int* ipiv, const double* s,
                            const double* b, lapack_int ldb, double* x,
                            lapack_int ldx, double* rcond, double* berr,
                            lapack_int n_err_bnds, double* err_bnds_norm,
                            double* err_bnds_comp, lapack_int nparams,
                            double* params );
lapack_int LAPACKE_csyrfsx( int matrix_layout, char uplo, char equed,
                            lapack_int n, lapack_int nrhs,
                            const lapack_complex_float* a, lapack_int lda,
                            const lapack_complex_float* af, lapack_int ldaf,
                            const lapack_int* ipiv, const float* s,
                            const lapack_complex_float* b, lapack_int ldb,
                            lapack_complex_float* x, lapack_int ldx,
                            float* rcond, float* berr, lapack_int n_err_bnds,
                            float* err_bnds_norm, float* err_bnds_comp,
                            lapack_int nparams, float* params );
lapack_int LAPACKE_zsyrfsx( int matrix_layout, char uplo, char equed,
                            lapack_int n, lapack_int nrhs,
                            const lapack_complex_double* a, lapack_int lda,
                            const lapack_complex_double* af, lapack_int ldaf,
                            const lapack_int* ipiv, const double* s,
                            const lapack_complex_double* b, lapack_int ldb,
                            lapack_complex_double* x, lapack_int ldx,
                            double* rcond, double* berr, lapack_int n_err_bnds,
                            double* err_bnds_norm, double* err_bnds_comp,
                            lapack_int nparams, double* params );

lapack_int LAPACKE_ssysv( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, float* a, lapack_int lda,
                          lapack_int* ipiv, float* b, lapack_int ldb );
lapack_int LAPACKE_dsysv( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, double* a, lapack_int lda,
                          lapack_int* ipiv, double* b, lapack_int ldb );
lapack_int LAPACKE_csysv( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, lapack_complex_float* a,
                          lapack_int lda, lapack_int* ipiv,
                          lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zsysv( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, lapack_complex_double* a,
                          lapack_int lda, lapack_int* ipiv,
                          lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_ssysvx( int matrix_layout, char fact, char uplo, lapack_int n,
                           lapack_int nrhs, const float* a, lapack_int lda,
                           float* af, lapack_int ldaf, lapack_int* ipiv,
                           const float* b, lapack_int ldb, float* x,
                           lapack_int ldx, float* rcond, float* ferr,
                           float* berr );
lapack_int LAPACKE_dsysvx( int matrix_layout, char fact, char uplo, lapack_int n,
                           lapack_int nrhs, const double* a, lapack_int lda,
                           double* af, lapack_int ldaf, lapack_int* ipiv,
                           const double* b, lapack_int ldb, double* x,
                           lapack_int ldx, double* rcond, double* ferr,
                           double* berr );
lapack_int LAPACKE_csysvx( int matrix_layout, char fact, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_float* a,
                           lapack_int lda, lapack_complex_float* af,
                           lapack_int ldaf, lapack_int* ipiv,
                           const lapack_complex_float* b, lapack_int ldb,
                           lapack_complex_float* x, lapack_int ldx,
                           float* rcond, float* ferr, float* berr );
lapack_int LAPACKE_zsysvx( int matrix_layout, char fact, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_double* a,
                           lapack_int lda, lapack_complex_double* af,
                           lapack_int ldaf, lapack_int* ipiv,
                           const lapack_complex_double* b, lapack_int ldb,
                           lapack_complex_double* x, lapack_int ldx,
                           double* rcond, double* ferr, double* berr );

lapack_int LAPACKE_ssysvxx( int matrix_layout, char fact, char uplo,
                            lapack_int n, lapack_int nrhs, float* a,
                            lapack_int lda, float* af, lapack_int ldaf,
                            lapack_int* ipiv, char* equed, float* s, float* b,
                            lapack_int ldb, float* x, lapack_int ldx,
                            float* rcond, float* rpvgrw, float* berr,
                            lapack_int n_err_bnds, float* err_bnds_norm,
                            float* err_bnds_comp, lapack_int nparams,
                            float* params );
lapack_int LAPACKE_dsysvxx( int matrix_layout, char fact, char uplo,
                            lapack_int n, lapack_int nrhs, double* a,
                            lapack_int lda, double* af, lapack_int ldaf,
                            lapack_int* ipiv, char* equed, double* s, double* b,
                            lapack_int ldb, double* x, lapack_int ldx,
                            double* rcond, double* rpvgrw, double* berr,
                            lapack_int n_err_bnds, double* err_bnds_norm,
                            double* err_bnds_comp, lapack_int nparams,
                            double* params );
lapack_int LAPACKE_csysvxx( int matrix_layout, char fact, char uplo,
                            lapack_int n, lapack_int nrhs,
                            lapack_complex_float* a, lapack_int lda,
                            lapack_complex_float* af, lapack_int ldaf,
                            lapack_int* ipiv, char* equed, float* s,
                            lapack_complex_float* b, lapack_int ldb,
                            lapack_complex_float* x, lapack_int ldx,
                            float* rcond, float* rpvgrw, float* berr,
                            lapack_int n_err_bnds, float* err_bnds_norm,
                            float* err_bnds_comp, lapack_int nparams,
                            float* params );
lapack_int LAPACKE_zsysvxx( int matrix_layout, char fact, char uplo,
                            lapack_int n, lapack_int nrhs,
                            lapack_complex_double* a, lapack_int lda,
                            lapack_complex_double* af, lapack_int ldaf,
                            lapack_int* ipiv, char* equed, double* s,
                            lapack_complex_double* b, lapack_int ldb,
                            lapack_complex_double* x, lapack_int ldx,
                            double* rcond, double* rpvgrw, double* berr,
                            lapack_int n_err_bnds, double* err_bnds_norm,
                            double* err_bnds_comp, lapack_int nparams,
                            double* params );

lapack_int LAPACKE_ssytrd( int matrix_layout, char uplo, lapack_int n, float* a,
                           lapack_int lda, float* d, float* e, float* tau );
lapack_int LAPACKE_dsytrd( int matrix_layout, char uplo, lapack_int n, double* a,
                           lapack_int lda, double* d, double* e, double* tau );

lapack_int LAPACKE_ssytrf( int matrix_layout, char uplo, lapack_int n, float* a,
                           lapack_int lda, lapack_int* ipiv );
lapack_int LAPACKE_dsytrf( int matrix_layout, char uplo, lapack_int n, double* a,
                           lapack_int lda, lapack_int* ipiv );
lapack_int LAPACKE_csytrf( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_int* ipiv );
lapack_int LAPACKE_zsytrf( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_int* ipiv );

lapack_int LAPACKE_ssytri( int matrix_layout, char uplo, lapack_int n, float* a,
                           lapack_int lda, const lapack_int* ipiv );
lapack_int LAPACKE_dsytri( int matrix_layout, char uplo, lapack_int n, double* a,
                           lapack_int lda, const lapack_int* ipiv );
lapack_int LAPACKE_csytri( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           const lapack_int* ipiv );
lapack_int LAPACKE_zsytri( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           const lapack_int* ipiv );

lapack_int LAPACKE_ssytrs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const float* a, lapack_int lda,
                           const lapack_int* ipiv, float* b, lapack_int ldb );
lapack_int LAPACKE_dsytrs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const double* a, lapack_int lda,
                           const lapack_int* ipiv, double* b, lapack_int ldb );
lapack_int LAPACKE_csytrs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_float* a,
                           lapack_int lda, const lapack_int* ipiv,
                           lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zsytrs( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_double* a,
                           lapack_int lda, const lapack_int* ipiv,
                           lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_stbcon( int matrix_layout, char norm, char uplo, char diag,
                           lapack_int n, lapack_int kd, const float* ab,
                           lapack_int ldab, float* rcond );
lapack_int LAPACKE_dtbcon( int matrix_layout, char norm, char uplo, char diag,
                           lapack_int n, lapack_int kd, const double* ab,
                           lapack_int ldab, double* rcond );
lapack_int LAPACKE_ctbcon( int matrix_layout, char norm, char uplo, char diag,
                           lapack_int n, lapack_int kd,
                           const lapack_complex_float* ab, lapack_int ldab,
                           float* rcond );
lapack_int LAPACKE_ztbcon( int matrix_layout, char norm, char uplo, char diag,
                           lapack_int n, lapack_int kd,
                           const lapack_complex_double* ab, lapack_int ldab,
                           double* rcond );

lapack_int LAPACKE_stbrfs( int matrix_layout, char uplo, char trans, char diag,
                           lapack_int n, lapack_int kd, lapack_int nrhs,
                           const float* ab, lapack_int ldab, const float* b,
                           lapack_int ldb, const float* x, lapack_int ldx,
                           float* ferr, float* berr );
lapack_int LAPACKE_dtbrfs( int matrix_layout, char uplo, char trans, char diag,
                           lapack_int n, lapack_int kd, lapack_int nrhs,
                           const double* ab, lapack_int ldab, const double* b,
                           lapack_int ldb, const double* x, lapack_int ldx,
                           double* ferr, double* berr );
lapack_int LAPACKE_ctbrfs( int matrix_layout, char uplo, char trans, char diag,
                           lapack_int n, lapack_int kd, lapack_int nrhs,
                           const lapack_complex_float* ab, lapack_int ldab,
                           const lapack_complex_float* b, lapack_int ldb,
                           const lapack_complex_float* x, lapack_int ldx,
                           float* ferr, float* berr );
lapack_int LAPACKE_ztbrfs( int matrix_layout, char uplo, char trans, char diag,
                           lapack_int n, lapack_int kd, lapack_int nrhs,
                           const lapack_complex_double* ab, lapack_int ldab,
                           const lapack_complex_double* b, lapack_int ldb,
                           const lapack_complex_double* x, lapack_int ldx,
                           double* ferr, double* berr );

lapack_int LAPACKE_stbtrs( int matrix_layout, char uplo, char trans, char diag,
                           lapack_int n, lapack_int kd, lapack_int nrhs,
                           const float* ab, lapack_int ldab, float* b,
                           lapack_int ldb );
lapack_int LAPACKE_dtbtrs( int matrix_layout, char uplo, char trans, char diag,
                           lapack_int n, lapack_int kd, lapack_int nrhs,
                           const double* ab, lapack_int ldab, double* b,
                           lapack_int ldb );
lapack_int LAPACKE_ctbtrs( int matrix_layout, char uplo, char trans, char diag,
                           lapack_int n, lapack_int kd, lapack_int nrhs,
                           const lapack_complex_float* ab, lapack_int ldab,
                           lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_ztbtrs( int matrix_layout, char uplo, char trans, char diag,
                           lapack_int n, lapack_int kd, lapack_int nrhs,
                           const lapack_complex_double* ab, lapack_int ldab,
                           lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_stfsm( int matrix_layout, char transr, char side, char uplo,
                          char trans, char diag, lapack_int m, lapack_int n,
                          float alpha, const float* a, float* b,
                          lapack_int ldb );
lapack_int LAPACKE_dtfsm( int matrix_layout, char transr, char side, char uplo,
                          char trans, char diag, lapack_int m, lapack_int n,
                          double alpha, const double* a, double* b,
                          lapack_int ldb );
lapack_int LAPACKE_ctfsm( int matrix_layout, char transr, char side, char uplo,
                          char trans, char diag, lapack_int m, lapack_int n,
                          lapack_complex_float alpha,
                          const lapack_complex_float* a,
                          lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_ztfsm( int matrix_layout, char transr, char side, char uplo,
                          char trans, char diag, lapack_int m, lapack_int n,
                          lapack_complex_double alpha,
                          const lapack_complex_double* a,
                          lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_stftri( int matrix_layout, char transr, char uplo, char diag,
                           lapack_int n, float* a );
lapack_int LAPACKE_dtftri( int matrix_layout, char transr, char uplo, char diag,
                           lapack_int n, double* a );
lapack_int LAPACKE_ctftri( int matrix_layout, char transr, char uplo, char diag,
                           lapack_int n, lapack_complex_float* a );
lapack_int LAPACKE_ztftri( int matrix_layout, char transr, char uplo, char diag,
                           lapack_int n, lapack_complex_double* a );

lapack_int LAPACKE_stfttp( int matrix_layout, char transr, char uplo,
                           lapack_int n, const float* arf, float* ap );
lapack_int LAPACKE_dtfttp( int matrix_layout, char transr, char uplo,
                           lapack_int n, const double* arf, double* ap );
lapack_int LAPACKE_ctfttp( int matrix_layout, char transr, char uplo,
                           lapack_int n, const lapack_complex_float* arf,
                           lapack_complex_float* ap );
lapack_int LAPACKE_ztfttp( int matrix_layout, char transr, char uplo,
                           lapack_int n, const lapack_complex_double* arf,
                           lapack_complex_double* ap );

lapack_int LAPACKE_stfttr( int matrix_layout, char transr, char uplo,
                           lapack_int n, const float* arf, float* a,
                           lapack_int lda );
lapack_int LAPACKE_dtfttr( int matrix_layout, char transr, char uplo,
                           lapack_int n, const double* arf, double* a,
                           lapack_int lda );
lapack_int LAPACKE_ctfttr( int matrix_layout, char transr, char uplo,
                           lapack_int n, const lapack_complex_float* arf,
                           lapack_complex_float* a, lapack_int lda );
lapack_int LAPACKE_ztfttr( int matrix_layout, char transr, char uplo,
                           lapack_int n, const lapack_complex_double* arf,
                           lapack_complex_double* a, lapack_int lda );

lapack_int LAPACKE_stgevc( int matrix_layout, char side, char howmny,
                           const lapack_logical* select, lapack_int n,
                           const float* s, lapack_int lds, const float* p,
                           lapack_int ldp, float* vl, lapack_int ldvl,
                           float* vr, lapack_int ldvr, lapack_int mm,
                           lapack_int* m );
lapack_int LAPACKE_dtgevc( int matrix_layout, char side, char howmny,
                           const lapack_logical* select, lapack_int n,
                           const double* s, lapack_int lds, const double* p,
                           lapack_int ldp, double* vl, lapack_int ldvl,
                           double* vr, lapack_int ldvr, lapack_int mm,
                           lapack_int* m );
lapack_int LAPACKE_ctgevc( int matrix_layout, char side, char howmny,
                           const lapack_logical* select, lapack_int n,
                           const lapack_complex_float* s, lapack_int lds,
                           const lapack_complex_float* p, lapack_int ldp,
                           lapack_complex_float* vl, lapack_int ldvl,
                           lapack_complex_float* vr, lapack_int ldvr,
                           lapack_int mm, lapack_int* m );
lapack_int LAPACKE_ztgevc( int matrix_layout, char side, char howmny,
                           const lapack_logical* select, lapack_int n,
                           const lapack_complex_double* s, lapack_int lds,
                           const lapack_complex_double* p, lapack_int ldp,
                           lapack_complex_double* vl, lapack_int ldvl,
                           lapack_complex_double* vr, lapack_int ldvr,
                           lapack_int mm, lapack_int* m );

lapack_int LAPACKE_stgexc( int matrix_layout, lapack_logical wantq,
                           lapack_logical wantz, lapack_int n, float* a,
                           lapack_int lda, float* b, lapack_int ldb, float* q,
                           lapack_int ldq, float* z, lapack_int ldz,
                           lapack_int* ifst, lapack_int* ilst );
lapack_int LAPACKE_dtgexc( int matrix_layout, lapack_logical wantq,
                           lapack_logical wantz, lapack_int n, double* a,
                           lapack_int lda, double* b, lapack_int ldb, double* q,
                           lapack_int ldq, double* z, lapack_int ldz,
                           lapack_int* ifst, lapack_int* ilst );
lapack_int LAPACKE_ctgexc( int matrix_layout, lapack_logical wantq,
                           lapack_logical wantz, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_complex_float* b, lapack_int ldb,
                           lapack_complex_float* q, lapack_int ldq,
                           lapack_complex_float* z, lapack_int ldz,
                           lapack_int ifst, lapack_int ilst );
lapack_int LAPACKE_ztgexc( int matrix_layout, lapack_logical wantq,
                           lapack_logical wantz, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_complex_double* b, lapack_int ldb,
                           lapack_complex_double* q, lapack_int ldq,
                           lapack_complex_double* z, lapack_int ldz,
                           lapack_int ifst, lapack_int ilst );

lapack_int LAPACKE_stgsen( int matrix_layout, lapack_int ijob,
                           lapack_logical wantq, lapack_logical wantz,
                           const lapack_logical* select, lapack_int n, float* a,
                           lapack_int lda, float* b, lapack_int ldb,
                           float* alphar, float* alphai, float* beta, float* q,
                           lapack_int ldq, float* z, lapack_int ldz,
                           lapack_int* m, float* pl, float* pr, float* dif );
lapack_int LAPACKE_dtgsen( int matrix_layout, lapack_int ijob,
                           lapack_logical wantq, lapack_logical wantz,
                           const lapack_logical* select, lapack_int n,
                           double* a, lapack_int lda, double* b, lapack_int ldb,
                           double* alphar, double* alphai, double* beta,
                           double* q, lapack_int ldq, double* z, lapack_int ldz,
                           lapack_int* m, double* pl, double* pr, double* dif );
lapack_int LAPACKE_ctgsen( int matrix_layout, lapack_int ijob,
                           lapack_logical wantq, lapack_logical wantz,
                           const lapack_logical* select, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_complex_float* b, lapack_int ldb,
                           lapack_complex_float* alpha,
                           lapack_complex_float* beta, lapack_complex_float* q,
                           lapack_int ldq, lapack_complex_float* z,
                           lapack_int ldz, lapack_int* m, float* pl, float* pr,
                           float* dif );
lapack_int LAPACKE_ztgsen( int matrix_layout, lapack_int ijob,
                           lapack_logical wantq, lapack_logical wantz,
                           const lapack_logical* select, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_complex_double* b, lapack_int ldb,
                           lapack_complex_double* alpha,
                           lapack_complex_double* beta,
                           lapack_complex_double* q, lapack_int ldq,
                           lapack_complex_double* z, lapack_int ldz,
                           lapack_int* m, double* pl, double* pr, double* dif );

lapack_int LAPACKE_stgsja( int matrix_layout, char jobu, char jobv, char jobq,
                           lapack_int m, lapack_int p, lapack_int n,
                           lapack_int k, lapack_int l, float* a, lapack_int lda,
                           float* b, lapack_int ldb, float tola, float tolb,
                           float* alpha, float* beta, float* u, lapack_int ldu,
                           float* v, lapack_int ldv, float* q, lapack_int ldq,
                           lapack_int* ncycle );
lapack_int LAPACKE_dtgsja( int matrix_layout, char jobu, char jobv, char jobq,
                           lapack_int m, lapack_int p, lapack_int n,
                           lapack_int k, lapack_int l, double* a,
                           lapack_int lda, double* b, lapack_int ldb,
                           double tola, double tolb, double* alpha,
                           double* beta, double* u, lapack_int ldu, double* v,
                           lapack_int ldv, double* q, lapack_int ldq,
                           lapack_int* ncycle );
lapack_int LAPACKE_ctgsja( int matrix_layout, char jobu, char jobv, char jobq,
                           lapack_int m, lapack_int p, lapack_int n,
                           lapack_int k, lapack_int l, lapack_complex_float* a,
                           lapack_int lda, lapack_complex_float* b,
                           lapack_int ldb, float tola, float tolb, float* alpha,
                           float* beta, lapack_complex_float* u, lapack_int ldu,
                           lapack_complex_float* v, lapack_int ldv,
                           lapack_complex_float* q, lapack_int ldq,
                           lapack_int* ncycle );
lapack_int LAPACKE_ztgsja( int matrix_layout, char jobu, char jobv, char jobq,
                           lapack_int m, lapack_int p, lapack_int n,
                           lapack_int k, lapack_int l, lapack_complex_double* a,
                           lapack_int lda, lapack_complex_double* b,
                           lapack_int ldb, double tola, double tolb,
                           double* alpha, double* beta,
                           lapack_complex_double* u, lapack_int ldu,
                           lapack_complex_double* v, lapack_int ldv,
                           lapack_complex_double* q, lapack_int ldq,
                           lapack_int* ncycle );

lapack_int LAPACKE_stgsna( int matrix_layout, char job, char howmny,
                           const lapack_logical* select, lapack_int n,
                           const float* a, lapack_int lda, const float* b,
                           lapack_int ldb, const float* vl, lapack_int ldvl,
                           const float* vr, lapack_int ldvr, float* s,
                           float* dif, lapack_int mm, lapack_int* m );
lapack_int LAPACKE_dtgsna( int matrix_layout, char job, char howmny,
                           const lapack_logical* select, lapack_int n,
                           const double* a, lapack_int lda, const double* b,
                           lapack_int ldb, const double* vl, lapack_int ldvl,
                           const double* vr, lapack_int ldvr, double* s,
                           double* dif, lapack_int mm, lapack_int* m );
lapack_int LAPACKE_ctgsna( int matrix_layout, char job, char howmny,
                           const lapack_logical* select, lapack_int n,
                           const lapack_complex_float* a, lapack_int lda,
                           const lapack_complex_float* b, lapack_int ldb,
                           const lapack_complex_float* vl, lapack_int ldvl,
                           const lapack_complex_float* vr, lapack_int ldvr,
                           float* s, float* dif, lapack_int mm, lapack_int* m );
lapack_int LAPACKE_ztgsna( int matrix_layout, char job, char howmny,
                           const lapack_logical* select, lapack_int n,
                           const lapack_complex_double* a, lapack_int lda,
                           const lapack_complex_double* b, lapack_int ldb,
                           const lapack_complex_double* vl, lapack_int ldvl,
                           const lapack_complex_double* vr, lapack_int ldvr,
                           double* s, double* dif, lapack_int mm,
                           lapack_int* m );

lapack_int LAPACKE_stgsyl( int matrix_layout, char trans, lapack_int ijob,
                           lapack_int m, lapack_int n, const float* a,
                           lapack_int lda, const float* b, lapack_int ldb,
                           float* c, lapack_int ldc, const float* d,
                           lapack_int ldd, const float* e, lapack_int lde,
                           float* f, lapack_int ldf, float* scale, float* dif );
lapack_int LAPACKE_dtgsyl( int matrix_layout, char trans, lapack_int ijob,
                           lapack_int m, lapack_int n, const double* a,
                           lapack_int lda, const double* b, lapack_int ldb,
                           double* c, lapack_int ldc, const double* d,
                           lapack_int ldd, const double* e, lapack_int lde,
                           double* f, lapack_int ldf, double* scale,
                           double* dif );
lapack_int LAPACKE_ctgsyl( int matrix_layout, char trans, lapack_int ijob,
                           lapack_int m, lapack_int n,
                           const lapack_complex_float* a, lapack_int lda,
                           const lapack_complex_float* b, lapack_int ldb,
                           lapack_complex_float* c, lapack_int ldc,
                           const lapack_complex_float* d, lapack_int ldd,
                           const lapack_complex_float* e, lapack_int lde,
                           lapack_complex_float* f, lapack_int ldf,
                           float* scale, float* dif );
lapack_int LAPACKE_ztgsyl( int matrix_layout, char trans, lapack_int ijob,
                           lapack_int m, lapack_int n,
                           const lapack_complex_double* a, lapack_int lda,
                           const lapack_complex_double* b, lapack_int ldb,
                           lapack_complex_double* c, lapack_int ldc,
                           const lapack_complex_double* d, lapack_int ldd,
                           const lapack_complex_double* e, lapack_int lde,
                           lapack_complex_double* f, lapack_int ldf,
                           double* scale, double* dif );

lapack_int LAPACKE_stpcon( int matrix_layout, char norm, char uplo, char diag,
                           lapack_int n, const float* ap, float* rcond );
lapack_int LAPACKE_dtpcon( int matrix_layout, char norm, char uplo, char diag,
                           lapack_int n, const double* ap, double* rcond );
lapack_int LAPACKE_ctpcon( int matrix_layout, char norm, char uplo, char diag,
                           lapack_int n, const lapack_complex_float* ap,
                           float* rcond );
lapack_int LAPACKE_ztpcon( int matrix_layout, char norm, char uplo, char diag,
                           lapack_int n, const lapack_complex_double* ap,
                           double* rcond );

lapack_int LAPACKE_stprfs( int matrix_layout, char uplo, char trans, char diag,
                           lapack_int n, lapack_int nrhs, const float* ap,
                           const float* b, lapack_int ldb, const float* x,
                           lapack_int ldx, float* ferr, float* berr );
lapack_int LAPACKE_dtprfs( int matrix_layout, char uplo, char trans, char diag,
                           lapack_int n, lapack_int nrhs, const double* ap,
                           const double* b, lapack_int ldb, const double* x,
                           lapack_int ldx, double* ferr, double* berr );
lapack_int LAPACKE_ctprfs( int matrix_layout, char uplo, char trans, char diag,
                           lapack_int n, lapack_int nrhs,
                           const lapack_complex_float* ap,
                           const lapack_complex_float* b, lapack_int ldb,
                           const lapack_complex_float* x, lapack_int ldx,
                           float* ferr, float* berr );
lapack_int LAPACKE_ztprfs( int matrix_layout, char uplo, char trans, char diag,
                           lapack_int n, lapack_int nrhs,
                           const lapack_complex_double* ap,
                           const lapack_complex_double* b, lapack_int ldb,
                           const lapack_complex_double* x, lapack_int ldx,
                           double* ferr, double* berr );

lapack_int LAPACKE_stptri( int matrix_layout, char uplo, char diag, lapack_int n,
                           float* ap );
lapack_int LAPACKE_dtptri( int matrix_layout, char uplo, char diag, lapack_int n,
                           double* ap );
lapack_int LAPACKE_ctptri( int matrix_layout, char uplo, char diag, lapack_int n,
                           lapack_complex_float* ap );
lapack_int LAPACKE_ztptri( int matrix_layout, char uplo, char diag, lapack_int n,
                           lapack_complex_double* ap );

lapack_int LAPACKE_stptrs( int matrix_layout, char uplo, char trans, char diag,
                           lapack_int n, lapack_int nrhs, const float* ap,
                           float* b, lapack_int ldb );
lapack_int LAPACKE_dtptrs( int matrix_layout, char uplo, char trans, char diag,
                           lapack_int n, lapack_int nrhs, const double* ap,
                           double* b, lapack_int ldb );
lapack_int LAPACKE_ctptrs( int matrix_layout, char uplo, char trans, char diag,
                           lapack_int n, lapack_int nrhs,
                           const lapack_complex_float* ap,
                           lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_ztptrs( int matrix_layout, char uplo, char trans, char diag,
                           lapack_int n, lapack_int nrhs,
                           const lapack_complex_double* ap,
                           lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_stpttf( int matrix_layout, char transr, char uplo,
                           lapack_int n, const float* ap, float* arf );
lapack_int LAPACKE_dtpttf( int matrix_layout, char transr, char uplo,
                           lapack_int n, const double* ap, double* arf );
lapack_int LAPACKE_ctpttf( int matrix_layout, char transr, char uplo,
                           lapack_int n, const lapack_complex_float* ap,
                           lapack_complex_float* arf );
lapack_int LAPACKE_ztpttf( int matrix_layout, char transr, char uplo,
                           lapack_int n, const lapack_complex_double* ap,
                           lapack_complex_double* arf );

lapack_int LAPACKE_stpttr( int matrix_layout, char uplo, lapack_int n,
                           const float* ap, float* a, lapack_int lda );
lapack_int LAPACKE_dtpttr( int matrix_layout, char uplo, lapack_int n,
                           const double* ap, double* a, lapack_int lda );
lapack_int LAPACKE_ctpttr( int matrix_layout, char uplo, lapack_int n,
                           const lapack_complex_float* ap,
                           lapack_complex_float* a, lapack_int lda );
lapack_int LAPACKE_ztpttr( int matrix_layout, char uplo, lapack_int n,
                           const lapack_complex_double* ap,
                           lapack_complex_double* a, lapack_int lda );

lapack_int LAPACKE_strcon( int matrix_layout, char norm, char uplo, char diag,
                           lapack_int n, const float* a, lapack_int lda,
                           float* rcond );
lapack_int LAPACKE_dtrcon( int matrix_layout, char norm, char uplo, char diag,
                           lapack_int n, const double* a, lapack_int lda,
                           double* rcond );
lapack_int LAPACKE_ctrcon( int matrix_layout, char norm, char uplo, char diag,
                           lapack_int n, const lapack_complex_float* a,
                           lapack_int lda, float* rcond );
lapack_int LAPACKE_ztrcon( int matrix_layout, char norm, char uplo, char diag,
                           lapack_int n, const lapack_complex_double* a,
                           lapack_int lda, double* rcond );

lapack_int LAPACKE_strevc( int matrix_layout, char side, char howmny,
                           lapack_logical* select, lapack_int n, const float* t,
                           lapack_int ldt, float* vl, lapack_int ldvl,
                           float* vr, lapack_int ldvr, lapack_int mm,
                           lapack_int* m );
lapack_int LAPACKE_dtrevc( int matrix_layout, char side, char howmny,
                           lapack_logical* select, lapack_int n,
                           const double* t, lapack_int ldt, double* vl,
                           lapack_int ldvl, double* vr, lapack_int ldvr,
                           lapack_int mm, lapack_int* m );
lapack_int LAPACKE_ctrevc( int matrix_layout, char side, char howmny,
                           const lapack_logical* select, lapack_int n,
                           lapack_complex_float* t, lapack_int ldt,
                           lapack_complex_float* vl, lapack_int ldvl,
                           lapack_complex_float* vr, lapack_int ldvr,
                           lapack_int mm, lapack_int* m );
lapack_int LAPACKE_ztrevc( int matrix_layout, char side, char howmny,
                           const lapack_logical* select, lapack_int n,
                           lapack_complex_double* t, lapack_int ldt,
                           lapack_complex_double* vl, lapack_int ldvl,
                           lapack_complex_double* vr, lapack_int ldvr,
                           lapack_int mm, lapack_int* m );

lapack_int LAPACKE_strexc( int matrix_layout, char compq, lapack_int n, float* t,
                           lapack_int ldt, float* q, lapack_int ldq,
                           lapack_int* ifst, lapack_int* ilst );
lapack_int LAPACKE_dtrexc( int matrix_layout, char compq, lapack_int n,
                           double* t, lapack_int ldt, double* q, lapack_int ldq,
                           lapack_int* ifst, lapack_int* ilst );
lapack_int LAPACKE_ctrexc( int matrix_layout, char compq, lapack_int n,
                           lapack_complex_float* t, lapack_int ldt,
                           lapack_complex_float* q, lapack_int ldq,
                           lapack_int ifst, lapack_int ilst );
lapack_int LAPACKE_ztrexc( int matrix_layout, char compq, lapack_int n,
                           lapack_complex_double* t, lapack_int ldt,
                           lapack_complex_double* q, lapack_int ldq,
                           lapack_int ifst, lapack_int ilst );

lapack_int LAPACKE_strrfs( int matrix_layout, char uplo, char trans, char diag,
                           lapack_int n, lapack_int nrhs, const float* a,
                           lapack_int lda, const float* b, lapack_int ldb,
                           const float* x, lapack_int ldx, float* ferr,
                           float* berr );
lapack_int LAPACKE_dtrrfs( int matrix_layout, char uplo, char trans, char diag,
                           lapack_int n, lapack_int nrhs, const double* a,
                           lapack_int lda, const double* b, lapack_int ldb,
                           const double* x, lapack_int ldx, double* ferr,
                           double* berr );
lapack_int LAPACKE_ctrrfs( int matrix_layout, char uplo, char trans, char diag,
                           lapack_int n, lapack_int nrhs,
                           const lapack_complex_float* a, lapack_int lda,
                           const lapack_complex_float* b, lapack_int ldb,
                           const lapack_complex_float* x, lapack_int ldx,
                           float* ferr, float* berr );
lapack_int LAPACKE_ztrrfs( int matrix_layout, char uplo, char trans, char diag,
                           lapack_int n, lapack_int nrhs,
                           const lapack_complex_double* a, lapack_int lda,
                           const lapack_complex_double* b, lapack_int ldb,
                           const lapack_complex_double* x, lapack_int ldx,
                           double* ferr, double* berr );

lapack_int LAPACKE_strsen( int matrix_layout, char job, char compq,
                           const lapack_logical* select, lapack_int n, float* t,
                           lapack_int ldt, float* q, lapack_int ldq, float* wr,
                           float* wi, lapack_int* m, float* s, float* sep );
lapack_int LAPACKE_dtrsen( int matrix_layout, char job, char compq,
                           const lapack_logical* select, lapack_int n,
                           double* t, lapack_int ldt, double* q, lapack_int ldq,
                           double* wr, double* wi, lapack_int* m, double* s,
                           double* sep );
lapack_int LAPACKE_ctrsen( int matrix_layout, char job, char compq,
                           const lapack_logical* select, lapack_int n,
                           lapack_complex_float* t, lapack_int ldt,
                           lapack_complex_float* q, lapack_int ldq,
                           lapack_complex_float* w, lapack_int* m, float* s,
                           float* sep );
lapack_int LAPACKE_ztrsen( int matrix_layout, char job, char compq,
                           const lapack_logical* select, lapack_int n,
                           lapack_complex_double* t, lapack_int ldt,
                           lapack_complex_double* q, lapack_int ldq,
                           lapack_complex_double* w, lapack_int* m, double* s,
                           double* sep );

lapack_int LAPACKE_strsna( int matrix_layout, char job, char howmny,
                           const lapack_logical* select, lapack_int n,
                           const float* t, lapack_int ldt, const float* vl,
                           lapack_int ldvl, const float* vr, lapack_int ldvr,
                           float* s, float* sep, lapack_int mm, lapack_int* m );
lapack_int LAPACKE_dtrsna( int matrix_layout, char job, char howmny,
                           const lapack_logical* select, lapack_int n,
                           const double* t, lapack_int ldt, const double* vl,
                           lapack_int ldvl, const double* vr, lapack_int ldvr,
                           double* s, double* sep, lapack_int mm,
                           lapack_int* m );
lapack_int LAPACKE_ctrsna( int matrix_layout, char job, char howmny,
                           const lapack_logical* select, lapack_int n,
                           const lapack_complex_float* t, lapack_int ldt,
                           const lapack_complex_float* vl, lapack_int ldvl,
                           const lapack_complex_float* vr, lapack_int ldvr,
                           float* s, float* sep, lapack_int mm, lapack_int* m );
lapack_int LAPACKE_ztrsna( int matrix_layout, char job, char howmny,
                           const lapack_logical* select, lapack_int n,
                           const lapack_complex_double* t, lapack_int ldt,
                           const lapack_complex_double* vl, lapack_int ldvl,
                           const lapack_complex_double* vr, lapack_int ldvr,
                           double* s, double* sep, lapack_int mm,
                           lapack_int* m );

lapack_int LAPACKE_strsyl( int matrix_layout, char trana, char tranb,
                           lapack_int isgn, lapack_int m, lapack_int n,
                           const float* a, lapack_int lda, const float* b,
                           lapack_int ldb, float* c, lapack_int ldc,
                           float* scale );
lapack_int LAPACKE_dtrsyl( int matrix_layout, char trana, char tranb,
                           lapack_int isgn, lapack_int m, lapack_int n,
                           const double* a, lapack_int lda, const double* b,
                           lapack_int ldb, double* c, lapack_int ldc,
                           double* scale );
lapack_int LAPACKE_ctrsyl( int matrix_layout, char trana, char tranb,
                           lapack_int isgn, lapack_int m, lapack_int n,
                           const lapack_complex_float* a, lapack_int lda,
                           const lapack_complex_float* b, lapack_int ldb,
                           lapack_complex_float* c, lapack_int ldc,
                           float* scale );
lapack_int LAPACKE_ztrsyl( int matrix_layout, char trana, char tranb,
                           lapack_int isgn, lapack_int m, lapack_int n,
                           const lapack_complex_double* a, lapack_int lda,
                           const lapack_complex_double* b, lapack_int ldb,
                           lapack_complex_double* c, lapack_int ldc,
                           double* scale );

lapack_int LAPACKE_strsyl3( int matrix_layout, char trana, char tranb,
                            lapack_int isgn, lapack_int m, lapack_int n,
                            const float* a, lapack_int lda, const float* b,
                            lapack_int ldb, float* c, lapack_int ldc,
                            float* scale );
lapack_int LAPACKE_dtrsyl3( int matrix_layout, char trana, char tranb,
                            lapack_int isgn, lapack_int m, lapack_int n,
                            const double* a, lapack_int lda, const double* b,
                            lapack_int ldb, double* c, lapack_int ldc,
                            double* scale );
lapack_int LAPACKE_ztrsyl3( int matrix_layout, char trana, char tranb,
                            lapack_int isgn, lapack_int m, lapack_int n,
                            const lapack_complex_double* a, lapack_int lda,
                            const lapack_complex_double* b, lapack_int ldb,
                            lapack_complex_double* c, lapack_int ldc,
                            double* scale );

lapack_int LAPACKE_strtri( int matrix_layout, char uplo, char diag, lapack_int n,
                           float* a, lapack_int lda );
lapack_int LAPACKE_dtrtri( int matrix_layout, char uplo, char diag, lapack_int n,
                           double* a, lapack_int lda );
lapack_int LAPACKE_ctrtri( int matrix_layout, char uplo, char diag, lapack_int n,
                           lapack_complex_float* a, lapack_int lda );
lapack_int LAPACKE_ztrtri( int matrix_layout, char uplo, char diag, lapack_int n,
                           lapack_complex_double* a, lapack_int lda );

lapack_int LAPACKE_strtrs( int matrix_layout, char uplo, char trans, char diag,
                           lapack_int n, lapack_int nrhs, const float* a,
                           lapack_int lda, float* b, lapack_int ldb );
lapack_int LAPACKE_dtrtrs( int matrix_layout, char uplo, char trans, char diag,
                           lapack_int n, lapack_int nrhs, const double* a,
                           lapack_int lda, double* b, lapack_int ldb );
lapack_int LAPACKE_ctrtrs( int matrix_layout, char uplo, char trans, char diag,
                           lapack_int n, lapack_int nrhs,
                           const lapack_complex_float* a, lapack_int lda,
                           lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_ztrtrs( int matrix_layout, char uplo, char trans, char diag,
                           lapack_int n, lapack_int nrhs,
                           const lapack_complex_double* a, lapack_int lda,
                           lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_strttf( int matrix_layout, char transr, char uplo,
                           lapack_int n, const float* a, lapack_int lda,
                           float* arf );
lapack_int LAPACKE_dtrttf( int matrix_layout, char transr, char uplo,
                           lapack_int n, const double* a, lapack_int lda,
                           double* arf );
lapack_int LAPACKE_ctrttf( int matrix_layout, char transr, char uplo,
                           lapack_int n, const lapack_complex_float* a,
                           lapack_int lda, lapack_complex_float* arf );
lapack_int LAPACKE_ztrttf( int matrix_layout, char transr, char uplo,
                           lapack_int n, const lapack_complex_double* a,
                           lapack_int lda, lapack_complex_double* arf );

lapack_int LAPACKE_strttp( int matrix_layout, char uplo, lapack_int n,
                           const float* a, lapack_int lda, float* ap );
lapack_int LAPACKE_dtrttp( int matrix_layout, char uplo, lapack_int n,
                           const double* a, lapack_int lda, double* ap );
lapack_int LAPACKE_ctrttp( int matrix_layout, char uplo, lapack_int n,
                           const lapack_complex_float* a, lapack_int lda,
                           lapack_complex_float* ap );
lapack_int LAPACKE_ztrttp( int matrix_layout, char uplo, lapack_int n,
                           const lapack_complex_double* a, lapack_int lda,
                           lapack_complex_double* ap );

lapack_int LAPACKE_stzrzf( int matrix_layout, lapack_int m, lapack_int n,
                           float* a, lapack_int lda, float* tau );
lapack_int LAPACKE_dtzrzf( int matrix_layout, lapack_int m, lapack_int n,
                           double* a, lapack_int lda, double* tau );
lapack_int LAPACKE_ctzrzf( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_complex_float* tau );
lapack_int LAPACKE_ztzrzf( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_complex_double* tau );

lapack_int LAPACKE_cungbr( int matrix_layout, char vect, lapack_int m,
                           lapack_int n, lapack_int k, lapack_complex_float* a,
                           lapack_int lda, const lapack_complex_float* tau );
lapack_int LAPACKE_zungbr( int matrix_layout, char vect, lapack_int m,
                           lapack_int n, lapack_int k, lapack_complex_double* a,
                           lapack_int lda, const lapack_complex_double* tau );

lapack_int LAPACKE_cunghr( int matrix_layout, lapack_int n, lapack_int ilo,
                           lapack_int ihi, lapack_complex_float* a,
                           lapack_int lda, const lapack_complex_float* tau );
lapack_int LAPACKE_zunghr( int matrix_layout, lapack_int n, lapack_int ilo,
                           lapack_int ihi, lapack_complex_double* a,
                           lapack_int lda, const lapack_complex_double* tau );

lapack_int LAPACKE_cunglq( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int k, lapack_complex_float* a,
                           lapack_int lda, const lapack_complex_float* tau );
lapack_int LAPACKE_zunglq( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int k, lapack_complex_double* a,
                           lapack_int lda, const lapack_complex_double* tau );

lapack_int LAPACKE_cungql( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int k, lapack_complex_float* a,
                           lapack_int lda, const lapack_complex_float* tau );
lapack_int LAPACKE_zungql( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int k, lapack_complex_double* a,
                           lapack_int lda, const lapack_complex_double* tau );

lapack_int LAPACKE_cungqr( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int k, lapack_complex_float* a,
                           lapack_int lda, const lapack_complex_float* tau );
lapack_int LAPACKE_zungqr( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int k, lapack_complex_double* a,
                           lapack_int lda, const lapack_complex_double* tau );

lapack_int LAPACKE_cungrq( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int k, lapack_complex_float* a,
                           lapack_int lda, const lapack_complex_float* tau );
lapack_int LAPACKE_zungrq( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int k, lapack_complex_double* a,
                           lapack_int lda, const lapack_complex_double* tau );

lapack_int LAPACKE_cungtr( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           const lapack_complex_float* tau );
lapack_int LAPACKE_zungtr( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           const lapack_complex_double* tau );

lapack_int LAPACKE_cungtsqr_row( int matrix_layout, lapack_int m, lapack_int n,
                                 lapack_int mb, lapack_int nb,
                                 lapack_complex_float* a, lapack_int lda,
                                 const lapack_complex_float* t, lapack_int ldt );
lapack_int LAPACKE_zungtsqr_row( int matrix_layout, lapack_int m, lapack_int n,
                                 lapack_int mb, lapack_int nb,
                                 lapack_complex_double* a, lapack_int lda,
                                 const lapack_complex_double* t, lapack_int ldt );

lapack_int LAPACKE_cunmbr( int matrix_layout, char vect, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           const lapack_complex_float* a, lapack_int lda,
                           const lapack_complex_float* tau,
                           lapack_complex_float* c, lapack_int ldc );
lapack_int LAPACKE_zunmbr( int matrix_layout, char vect, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           const lapack_complex_double* a, lapack_int lda,
                           const lapack_complex_double* tau,
                           lapack_complex_double* c, lapack_int ldc );

lapack_int LAPACKE_cunmhr( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int ilo,
                           lapack_int ihi, const lapack_complex_float* a,
                           lapack_int lda, const lapack_complex_float* tau,
                           lapack_complex_float* c, lapack_int ldc );
lapack_int LAPACKE_zunmhr( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int ilo,
                           lapack_int ihi, const lapack_complex_double* a,
                           lapack_int lda, const lapack_complex_double* tau,
                           lapack_complex_double* c, lapack_int ldc );

lapack_int LAPACKE_cunmlq( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           const lapack_complex_float* a, lapack_int lda,
                           const lapack_complex_float* tau,
                           lapack_complex_float* c, lapack_int ldc );
lapack_int LAPACKE_zunmlq( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           const lapack_complex_double* a, lapack_int lda,
                           const lapack_complex_double* tau,
                           lapack_complex_double* c, lapack_int ldc );

lapack_int LAPACKE_cunmql( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           const lapack_complex_float* a, lapack_int lda,
                           const lapack_complex_float* tau,
                           lapack_complex_float* c, lapack_int ldc );
lapack_int LAPACKE_zunmql( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           const lapack_complex_double* a, lapack_int lda,
                           const lapack_complex_double* tau,
                           lapack_complex_double* c, lapack_int ldc );

lapack_int LAPACKE_cunmqr( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           const lapack_complex_float* a, lapack_int lda,
                           const lapack_complex_float* tau,
                           lapack_complex_float* c, lapack_int ldc );
lapack_int LAPACKE_zunmqr( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           const lapack_complex_double* a, lapack_int lda,
                           const lapack_complex_double* tau,
                           lapack_complex_double* c, lapack_int ldc );

lapack_int LAPACKE_cunmrq( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           const lapack_complex_float* a, lapack_int lda,
                           const lapack_complex_float* tau,
                           lapack_complex_float* c, lapack_int ldc );
lapack_int LAPACKE_zunmrq( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           const lapack_complex_double* a, lapack_int lda,
                           const lapack_complex_double* tau,
                           lapack_complex_double* c, lapack_int ldc );

lapack_int LAPACKE_cunmrz( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           lapack_int l, const lapack_complex_float* a,
                           lapack_int lda, const lapack_complex_float* tau,
                           lapack_complex_float* c, lapack_int ldc );
lapack_int LAPACKE_zunmrz( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           lapack_int l, const lapack_complex_double* a,
                           lapack_int lda, const lapack_complex_double* tau,
                           lapack_complex_double* c, lapack_int ldc );

lapack_int LAPACKE_cunmtr( int matrix_layout, char side, char uplo, char trans,
                           lapack_int m, lapack_int n,
                           const lapack_complex_float* a, lapack_int lda,
                           const lapack_complex_float* tau,
                           lapack_complex_float* c, lapack_int ldc );
lapack_int LAPACKE_zunmtr( int matrix_layout, char side, char uplo, char trans,
                           lapack_int m, lapack_int n,
                           const lapack_complex_double* a, lapack_int lda,
                           const lapack_complex_double* tau,
                           lapack_complex_double* c, lapack_int ldc );

lapack_int LAPACKE_cupgtr( int matrix_layout, char uplo, lapack_int n,
                           const lapack_complex_float* ap,
                           const lapack_complex_float* tau,
                           lapack_complex_float* q, lapack_int ldq );
lapack_int LAPACKE_zupgtr( int matrix_layout, char uplo, lapack_int n,
                           const lapack_complex_double* ap,
                           const lapack_complex_double* tau,
                           lapack_complex_double* q, lapack_int ldq );

lapack_int LAPACKE_cupmtr( int matrix_layout, char side, char uplo, char trans,
                           lapack_int m, lapack_int n,
                           const lapack_complex_float* ap,
                           const lapack_complex_float* tau,
                           lapack_complex_float* c, lapack_int ldc );
lapack_int LAPACKE_zupmtr( int matrix_layout, char side, char uplo, char trans,
                           lapack_int m, lapack_int n,
                           const lapack_complex_double* ap,
                           const lapack_complex_double* tau,
                           lapack_complex_double* c, lapack_int ldc );

lapack_int LAPACKE_sbdsdc_work( int matrix_layout, char uplo, char compq,
                                lapack_int n, float* d, float* e, float* u,
                                lapack_int ldu, float* vt, lapack_int ldvt,
                                float* q, lapack_int* iq, float* work,
                                lapack_int* iwork );
lapack_int LAPACKE_dbdsdc_work( int matrix_layout, char uplo, char compq,
                                lapack_int n, double* d, double* e, double* u,
                                lapack_int ldu, double* vt, lapack_int ldvt,
                                double* q, lapack_int* iq, double* work,
                                lapack_int* iwork );

lapack_int LAPACKE_sbdsvdx_work( int matrix_layout, char uplo, char jobz, char range,
                                 lapack_int n, float* d, float* e,
                                 float vl, float vu,
                                 lapack_int il, lapack_int iu, lapack_int* ns,
                                 float* s, float* z, lapack_int ldz,
                                 float* work, lapack_int* iwork );
lapack_int LAPACKE_dbdsvdx_work( int matrix_layout, char uplo, char jobz, char range,
                                 lapack_int n, double* d, double* e,
                                 double vl, double vu,
                                 lapack_int il, lapack_int iu, lapack_int* ns,
                                 double* s, double* z, lapack_int ldz,
                                 double* work, lapack_int* iwork );

lapack_int LAPACKE_sbdsqr_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int ncvt, lapack_int nru, lapack_int ncc,
                                float* d, float* e, float* vt, lapack_int ldvt,
                                float* u, lapack_int ldu, float* c,
                                lapack_int ldc, float* work );
lapack_int LAPACKE_dbdsqr_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int ncvt, lapack_int nru, lapack_int ncc,
                                double* d, double* e, double* vt,
                                lapack_int ldvt, double* u, lapack_int ldu,
                                double* c, lapack_int ldc, double* work );
lapack_int LAPACKE_cbdsqr_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int ncvt, lapack_int nru, lapack_int ncc,
                                float* d, float* e, lapack_complex_float* vt,
                                lapack_int ldvt, lapack_complex_float* u,
                                lapack_int ldu, lapack_complex_float* c,
                                lapack_int ldc, float* work );
lapack_int LAPACKE_zbdsqr_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int ncvt, lapack_int nru, lapack_int ncc,
                                double* d, double* e, lapack_complex_double* vt,
                                lapack_int ldvt, lapack_complex_double* u,
                                lapack_int ldu, lapack_complex_double* c,
                                lapack_int ldc, double* work );

lapack_int LAPACKE_sdisna_work( char job, lapack_int m, lapack_int n,
                                const float* d, float* sep );
lapack_int LAPACKE_ddisna_work( char job, lapack_int m, lapack_int n,
                                const double* d, double* sep );

lapack_int LAPACKE_sgbbrd_work( int matrix_layout, char vect, lapack_int m,
                                lapack_int n, lapack_int ncc, lapack_int kl,
                                lapack_int ku, float* ab, lapack_int ldab,
                                float* d, float* e, float* q, lapack_int ldq,
                                float* pt, lapack_int ldpt, float* c,
                                lapack_int ldc, float* work );
lapack_int LAPACKE_dgbbrd_work( int matrix_layout, char vect, lapack_int m,
                                lapack_int n, lapack_int ncc, lapack_int kl,
                                lapack_int ku, double* ab, lapack_int ldab,
                                double* d, double* e, double* q, lapack_int ldq,
                                double* pt, lapack_int ldpt, double* c,
                                lapack_int ldc, double* work );
lapack_int LAPACKE_cgbbrd_work( int matrix_layout, char vect, lapack_int m,
                                lapack_int n, lapack_int ncc, lapack_int kl,
                                lapack_int ku, lapack_complex_float* ab,
                                lapack_int ldab, float* d, float* e,
                                lapack_complex_float* q, lapack_int ldq,
                                lapack_complex_float* pt, lapack_int ldpt,
                                lapack_complex_float* c, lapack_int ldc,
                                lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_zgbbrd_work( int matrix_layout, char vect, lapack_int m,
                                lapack_int n, lapack_int ncc, lapack_int kl,
                                lapack_int ku, lapack_complex_double* ab,
                                lapack_int ldab, double* d, double* e,
                                lapack_complex_double* q, lapack_int ldq,
                                lapack_complex_double* pt, lapack_int ldpt,
                                lapack_complex_double* c, lapack_int ldc,
                                lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_sgbcon_work( int matrix_layout, char norm, lapack_int n,
                                lapack_int kl, lapack_int ku, const float* ab,
                                lapack_int ldab, const lapack_int* ipiv,
                                float anorm, float* rcond, float* work,
                                lapack_int* iwork );
lapack_int LAPACKE_dgbcon_work( int matrix_layout, char norm, lapack_int n,
                                lapack_int kl, lapack_int ku, const double* ab,
                                lapack_int ldab, const lapack_int* ipiv,
                                double anorm, double* rcond, double* work,
                                lapack_int* iwork );
lapack_int LAPACKE_cgbcon_work( int matrix_layout, char norm, lapack_int n,
                                lapack_int kl, lapack_int ku,
                                const lapack_complex_float* ab, lapack_int ldab,
                                const lapack_int* ipiv, float anorm,
                                float* rcond, lapack_complex_float* work,
                                float* rwork );
lapack_int LAPACKE_zgbcon_work( int matrix_layout, char norm, lapack_int n,
                                lapack_int kl, lapack_int ku,
                                const lapack_complex_double* ab,
                                lapack_int ldab, const lapack_int* ipiv,
                                double anorm, double* rcond,
                                lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_sgbequ_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int kl, lapack_int ku, const float* ab,
                                lapack_int ldab, float* r, float* c,
                                float* rowcnd, float* colcnd, float* amax );
lapack_int LAPACKE_dgbequ_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int kl, lapack_int ku, const double* ab,
                                lapack_int ldab, double* r, double* c,
                                double* rowcnd, double* colcnd, double* amax );
lapack_int LAPACKE_cgbequ_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int kl, lapack_int ku,
                                const lapack_complex_float* ab, lapack_int ldab,
                                float* r, float* c, float* rowcnd,
                                float* colcnd, float* amax );
lapack_int LAPACKE_zgbequ_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int kl, lapack_int ku,
                                const lapack_complex_double* ab,
                                lapack_int ldab, double* r, double* c,
                                double* rowcnd, double* colcnd, double* amax );

lapack_int LAPACKE_sgbequb_work( int matrix_layout, lapack_int m, lapack_int n,
                                 lapack_int kl, lapack_int ku, const float* ab,
                                 lapack_int ldab, float* r, float* c,
                                 float* rowcnd, float* colcnd, float* amax );
lapack_int LAPACKE_dgbequb_work( int matrix_layout, lapack_int m, lapack_int n,
                                 lapack_int kl, lapack_int ku, const double* ab,
                                 lapack_int ldab, double* r, double* c,
                                 double* rowcnd, double* colcnd, double* amax );
lapack_int LAPACKE_cgbequb_work( int matrix_layout, lapack_int m, lapack_int n,
                                 lapack_int kl, lapack_int ku,
                                 const lapack_complex_float* ab,
                                 lapack_int ldab, float* r, float* c,
                                 float* rowcnd, float* colcnd, float* amax );
lapack_int LAPACKE_zgbequb_work( int matrix_layout, lapack_int m, lapack_int n,
                                 lapack_int kl, lapack_int ku,
                                 const lapack_complex_double* ab,
                                 lapack_int ldab, double* r, double* c,
                                 double* rowcnd, double* colcnd, double* amax );

lapack_int LAPACKE_sgbrfs_work( int matrix_layout, char trans, lapack_int n,
                                lapack_int kl, lapack_int ku, lapack_int nrhs,
                                const float* ab, lapack_int ldab,
                                const float* afb, lapack_int ldafb,
                                const lapack_int* ipiv, const float* b,
                                lapack_int ldb, float* x, lapack_int ldx,
                                float* ferr, float* berr, float* work,
                                lapack_int* iwork );
lapack_int LAPACKE_dgbrfs_work( int matrix_layout, char trans, lapack_int n,
                                lapack_int kl, lapack_int ku, lapack_int nrhs,
                                const double* ab, lapack_int ldab,
                                const double* afb, lapack_int ldafb,
                                const lapack_int* ipiv, const double* b,
                                lapack_int ldb, double* x, lapack_int ldx,
                                double* ferr, double* berr, double* work,
                                lapack_int* iwork );
lapack_int LAPACKE_cgbrfs_work( int matrix_layout, char trans, lapack_int n,
                                lapack_int kl, lapack_int ku, lapack_int nrhs,
                                const lapack_complex_float* ab, lapack_int ldab,
                                const lapack_complex_float* afb,
                                lapack_int ldafb, const lapack_int* ipiv,
                                const lapack_complex_float* b, lapack_int ldb,
                                lapack_complex_float* x, lapack_int ldx,
                                float* ferr, float* berr,
                                lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_zgbrfs_work( int matrix_layout, char trans, lapack_int n,
                                lapack_int kl, lapack_int ku, lapack_int nrhs,
                                const lapack_complex_double* ab,
                                lapack_int ldab,
                                const lapack_complex_double* afb,
                                lapack_int ldafb, const lapack_int* ipiv,
                                const lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* x, lapack_int ldx,
                                double* ferr, double* berr,
                                lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_sgbrfsx_work( int matrix_layout, char trans, char equed,
                                 lapack_int n, lapack_int kl, lapack_int ku,
                                 lapack_int nrhs, const float* ab,
                                 lapack_int ldab, const float* afb,
                                 lapack_int ldafb, const lapack_int* ipiv,
                                 const float* r, const float* c, const float* b,
                                 lapack_int ldb, float* x, lapack_int ldx,
                                 float* rcond, float* berr,
                                 lapack_int n_err_bnds, float* err_bnds_norm,
                                 float* err_bnds_comp, lapack_int nparams,
                                 float* params, float* work,
                                 lapack_int* iwork );
lapack_int LAPACKE_dgbrfsx_work( int matrix_layout, char trans, char equed,
                                 lapack_int n, lapack_int kl, lapack_int ku,
                                 lapack_int nrhs, const double* ab,
                                 lapack_int ldab, const double* afb,
                                 lapack_int ldafb, const lapack_int* ipiv,
                                 const double* r, const double* c,
                                 const double* b, lapack_int ldb, double* x,
                                 lapack_int ldx, double* rcond, double* berr,
                                 lapack_int n_err_bnds, double* err_bnds_norm,
                                 double* err_bnds_comp, lapack_int nparams,
                                 double* params, double* work,
                                 lapack_int* iwork );
lapack_int LAPACKE_cgbrfsx_work( int matrix_layout, char trans, char equed,
                                 lapack_int n, lapack_int kl, lapack_int ku,
                                 lapack_int nrhs,
                                 const lapack_complex_float* ab,
                                 lapack_int ldab,
                                 const lapack_complex_float* afb,
                                 lapack_int ldafb, const lapack_int* ipiv,
                                 const float* r, const float* c,
                                 const lapack_complex_float* b, lapack_int ldb,
                                 lapack_complex_float* x, lapack_int ldx,
                                 float* rcond, float* berr,
                                 lapack_int n_err_bnds, float* err_bnds_norm,
                                 float* err_bnds_comp, lapack_int nparams,
                                 float* params, lapack_complex_float* work,
                                 float* rwork );
lapack_int LAPACKE_zgbrfsx_work( int matrix_layout, char trans, char equed,
                                 lapack_int n, lapack_int kl, lapack_int ku,
                                 lapack_int nrhs,
                                 const lapack_complex_double* ab,
                                 lapack_int ldab,
                                 const lapack_complex_double* afb,
                                 lapack_int ldafb, const lapack_int* ipiv,
                                 const double* r, const double* c,
                                 const lapack_complex_double* b, lapack_int ldb,
                                 lapack_complex_double* x, lapack_int ldx,
                                 double* rcond, double* berr,
                                 lapack_int n_err_bnds, double* err_bnds_norm,
                                 double* err_bnds_comp, lapack_int nparams,
                                 double* params, lapack_complex_double* work,
                                 double* rwork );

lapack_int LAPACKE_sgbsv_work( int matrix_layout, lapack_int n, lapack_int kl,
                               lapack_int ku, lapack_int nrhs, float* ab,
                               lapack_int ldab, lapack_int* ipiv, float* b,
                               lapack_int ldb );
lapack_int LAPACKE_dgbsv_work( int matrix_layout, lapack_int n, lapack_int kl,
                               lapack_int ku, lapack_int nrhs, double* ab,
                               lapack_int ldab, lapack_int* ipiv, double* b,
                               lapack_int ldb );
lapack_int LAPACKE_cgbsv_work( int matrix_layout, lapack_int n, lapack_int kl,
                               lapack_int ku, lapack_int nrhs,
                               lapack_complex_float* ab, lapack_int ldab,
                               lapack_int* ipiv, lapack_complex_float* b,
                               lapack_int ldb );
lapack_int LAPACKE_zgbsv_work( int matrix_layout, lapack_int n, lapack_int kl,
                               lapack_int ku, lapack_int nrhs,
                               lapack_complex_double* ab, lapack_int ldab,
                               lapack_int* ipiv, lapack_complex_double* b,
                               lapack_int ldb );

lapack_int LAPACKE_sgbsvx_work( int matrix_layout, char fact, char trans,
                                lapack_int n, lapack_int kl, lapack_int ku,
                                lapack_int nrhs, float* ab, lapack_int ldab,
                                float* afb, lapack_int ldafb, lapack_int* ipiv,
                                char* equed, float* r, float* c, float* b,
                                lapack_int ldb, float* x, lapack_int ldx,
                                float* rcond, float* ferr, float* berr,
                                float* work, lapack_int* iwork );
lapack_int LAPACKE_dgbsvx_work( int matrix_layout, char fact, char trans,
                                lapack_int n, lapack_int kl, lapack_int ku,
                                lapack_int nrhs, double* ab, lapack_int ldab,
                                double* afb, lapack_int ldafb, lapack_int* ipiv,
                                char* equed, double* r, double* c, double* b,
                                lapack_int ldb, double* x, lapack_int ldx,
                                double* rcond, double* ferr, double* berr,
                                double* work, lapack_int* iwork );
lapack_int LAPACKE_cgbsvx_work( int matrix_layout, char fact, char trans,
                                lapack_int n, lapack_int kl, lapack_int ku,
                                lapack_int nrhs, lapack_complex_float* ab,
                                lapack_int ldab, lapack_complex_float* afb,
                                lapack_int ldafb, lapack_int* ipiv, char* equed,
                                float* r, float* c, lapack_complex_float* b,
                                lapack_int ldb, lapack_complex_float* x,
                                lapack_int ldx, float* rcond, float* ferr,
                                float* berr, lapack_complex_float* work,
                                float* rwork );
lapack_int LAPACKE_zgbsvx_work( int matrix_layout, char fact, char trans,
                                lapack_int n, lapack_int kl, lapack_int ku,
                                lapack_int nrhs, lapack_complex_double* ab,
                                lapack_int ldab, lapack_complex_double* afb,
                                lapack_int ldafb, lapack_int* ipiv, char* equed,
                                double* r, double* c, lapack_complex_double* b,
                                lapack_int ldb, lapack_complex_double* x,
                                lapack_int ldx, double* rcond, double* ferr,
                                double* berr, lapack_complex_double* work,
                                double* rwork );

lapack_int LAPACKE_sgbsvxx_work( int matrix_layout, char fact, char trans,
                                 lapack_int n, lapack_int kl, lapack_int ku,
                                 lapack_int nrhs, float* ab, lapack_int ldab,
                                 float* afb, lapack_int ldafb, lapack_int* ipiv,
                                 char* equed, float* r, float* c, float* b,
                                 lapack_int ldb, float* x, lapack_int ldx,
                                 float* rcond, float* rpvgrw, float* berr,
                                 lapack_int n_err_bnds, float* err_bnds_norm,
                                 float* err_bnds_comp, lapack_int nparams,
                                 float* params, float* work,
                                 lapack_int* iwork );
lapack_int LAPACKE_dgbsvxx_work( int matrix_layout, char fact, char trans,
                                 lapack_int n, lapack_int kl, lapack_int ku,
                                 lapack_int nrhs, double* ab, lapack_int ldab,
                                 double* afb, lapack_int ldafb,
                                 lapack_int* ipiv, char* equed, double* r,
                                 double* c, double* b, lapack_int ldb,
                                 double* x, lapack_int ldx, double* rcond,
                                 double* rpvgrw, double* berr,
                                 lapack_int n_err_bnds, double* err_bnds_norm,
                                 double* err_bnds_comp, lapack_int nparams,
                                 double* params, double* work,
                                 lapack_int* iwork );
lapack_int LAPACKE_cgbsvxx_work( int matrix_layout, char fact, char trans,
                                 lapack_int n, lapack_int kl, lapack_int ku,
                                 lapack_int nrhs, lapack_complex_float* ab,
                                 lapack_int ldab, lapack_complex_float* afb,
                                 lapack_int ldafb, lapack_int* ipiv,
                                 char* equed, float* r, float* c,
                                 lapack_complex_float* b, lapack_int ldb,
                                 lapack_complex_float* x, lapack_int ldx,
                                 float* rcond, float* rpvgrw, float* berr,
                                 lapack_int n_err_bnds, float* err_bnds_norm,
                                 float* err_bnds_comp, lapack_int nparams,
                                 float* params, lapack_complex_float* work,
                                 float* rwork );
lapack_int LAPACKE_zgbsvxx_work( int matrix_layout, char fact, char trans,
                                 lapack_int n, lapack_int kl, lapack_int ku,
                                 lapack_int nrhs, lapack_complex_double* ab,
                                 lapack_int ldab, lapack_complex_double* afb,
                                 lapack_int ldafb, lapack_int* ipiv,
                                 char* equed, double* r, double* c,
                                 lapack_complex_double* b, lapack_int ldb,
                                 lapack_complex_double* x, lapack_int ldx,
                                 double* rcond, double* rpvgrw, double* berr,
                                 lapack_int n_err_bnds, double* err_bnds_norm,
                                 double* err_bnds_comp, lapack_int nparams,
                                 double* params, lapack_complex_double* work,
                                 double* rwork );

lapack_int LAPACKE_sgbtrf_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int kl, lapack_int ku, float* ab,
                                lapack_int ldab, lapack_int* ipiv );
lapack_int LAPACKE_dgbtrf_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int kl, lapack_int ku, double* ab,
                                lapack_int ldab, lapack_int* ipiv );
lapack_int LAPACKE_cgbtrf_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int kl, lapack_int ku,
                                lapack_complex_float* ab, lapack_int ldab,
                                lapack_int* ipiv );
lapack_int LAPACKE_zgbtrf_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int kl, lapack_int ku,
                                lapack_complex_double* ab, lapack_int ldab,
                                lapack_int* ipiv );

lapack_int LAPACKE_sgbtrs_work( int matrix_layout, char trans, lapack_int n,
                                lapack_int kl, lapack_int ku, lapack_int nrhs,
                                const float* ab, lapack_int ldab,
                                const lapack_int* ipiv, float* b,
                                lapack_int ldb );
lapack_int LAPACKE_dgbtrs_work( int matrix_layout, char trans, lapack_int n,
                                lapack_int kl, lapack_int ku, lapack_int nrhs,
                                const double* ab, lapack_int ldab,
                                const lapack_int* ipiv, double* b,
                                lapack_int ldb );
lapack_int LAPACKE_cgbtrs_work( int matrix_layout, char trans, lapack_int n,
                                lapack_int kl, lapack_int ku, lapack_int nrhs,
                                const lapack_complex_float* ab, lapack_int ldab,
                                const lapack_int* ipiv, lapack_complex_float* b,
                                lapack_int ldb );
lapack_int LAPACKE_zgbtrs_work( int matrix_layout, char trans, lapack_int n,
                                lapack_int kl, lapack_int ku, lapack_int nrhs,
                                const lapack_complex_double* ab,
                                lapack_int ldab, const lapack_int* ipiv,
                                lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_sgebak_work( int matrix_layout, char job, char side,
                                lapack_int n, lapack_int ilo, lapack_int ihi,
                                const float* scale, lapack_int m, float* v,
                                lapack_int ldv );
lapack_int LAPACKE_dgebak_work( int matrix_layout, char job, char side,
                                lapack_int n, lapack_int ilo, lapack_int ihi,
                                const double* scale, lapack_int m, double* v,
                                lapack_int ldv );
lapack_int LAPACKE_cgebak_work( int matrix_layout, char job, char side,
                                lapack_int n, lapack_int ilo, lapack_int ihi,
                                const float* scale, lapack_int m,
                                lapack_complex_float* v, lapack_int ldv );
lapack_int LAPACKE_zgebak_work( int matrix_layout, char job, char side,
                                lapack_int n, lapack_int ilo, lapack_int ihi,
                                const double* scale, lapack_int m,
                                lapack_complex_double* v, lapack_int ldv );

lapack_int LAPACKE_sgebal_work( int matrix_layout, char job, lapack_int n,
                                float* a, lapack_int lda, lapack_int* ilo,
                                lapack_int* ihi, float* scale );
lapack_int LAPACKE_dgebal_work( int matrix_layout, char job, lapack_int n,
                                double* a, lapack_int lda, lapack_int* ilo,
                                lapack_int* ihi, double* scale );
lapack_int LAPACKE_cgebal_work( int matrix_layout, char job, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_int* ilo, lapack_int* ihi,
                                float* scale );
lapack_int LAPACKE_zgebal_work( int matrix_layout, char job, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_int* ilo, lapack_int* ihi,
                                double* scale );

lapack_int LAPACKE_sgebrd_work( int matrix_layout, lapack_int m, lapack_int n,
                                float* a, lapack_int lda, float* d, float* e,
                                float* tauq, float* taup, float* work,
                                lapack_int lwork );
lapack_int LAPACKE_dgebrd_work( int matrix_layout, lapack_int m, lapack_int n,
                                double* a, lapack_int lda, double* d, double* e,
                                double* tauq, double* taup, double* work,
                                lapack_int lwork );
lapack_int LAPACKE_cgebrd_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                float* d, float* e, lapack_complex_float* tauq,
                                lapack_complex_float* taup,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zgebrd_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                double* d, double* e,
                                lapack_complex_double* tauq,
                                lapack_complex_double* taup,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_sgecon_work( int matrix_layout, char norm, lapack_int n,
                                const float* a, lapack_int lda, float anorm,
                                float* rcond, float* work, lapack_int* iwork );
lapack_int LAPACKE_dgecon_work( int matrix_layout, char norm, lapack_int n,
                                const double* a, lapack_int lda, double anorm,
                                double* rcond, double* work,
                                lapack_int* iwork );
lapack_int LAPACKE_cgecon_work( int matrix_layout, char norm, lapack_int n,
                                const lapack_complex_float* a, lapack_int lda,
                                float anorm, float* rcond,
                                lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_zgecon_work( int matrix_layout, char norm, lapack_int n,
                                const lapack_complex_double* a, lapack_int lda,
                                double anorm, double* rcond,
                                lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_sgeequ_work( int matrix_layout, lapack_int m, lapack_int n,
                                const float* a, lapack_int lda, float* r,
                                float* c, float* rowcnd, float* colcnd,
                                float* amax );
lapack_int LAPACKE_dgeequ_work( int matrix_layout, lapack_int m, lapack_int n,
                                const double* a, lapack_int lda, double* r,
                                double* c, double* rowcnd, double* colcnd,
                                double* amax );
lapack_int LAPACKE_cgeequ_work( int matrix_layout, lapack_int m, lapack_int n,
                                const lapack_complex_float* a, lapack_int lda,
                                float* r, float* c, float* rowcnd,
                                float* colcnd, float* amax );
lapack_int LAPACKE_zgeequ_work( int matrix_layout, lapack_int m, lapack_int n,
                                const lapack_complex_double* a, lapack_int lda,
                                double* r, double* c, double* rowcnd,
                                double* colcnd, double* amax );

lapack_int LAPACKE_sgeequb_work( int matrix_layout, lapack_int m, lapack_int n,
                                 const float* a, lapack_int lda, float* r,
                                 float* c, float* rowcnd, float* colcnd,
                                 float* amax );
lapack_int LAPACKE_dgeequb_work( int matrix_layout, lapack_int m, lapack_int n,
                                 const double* a, lapack_int lda, double* r,
                                 double* c, double* rowcnd, double* colcnd,
                                 double* amax );
lapack_int LAPACKE_cgeequb_work( int matrix_layout, lapack_int m, lapack_int n,
                                 const lapack_complex_float* a, lapack_int lda,
                                 float* r, float* c, float* rowcnd,
                                 float* colcnd, float* amax );
lapack_int LAPACKE_zgeequb_work( int matrix_layout, lapack_int m, lapack_int n,
                                 const lapack_complex_double* a, lapack_int lda,
                                 double* r, double* c, double* rowcnd,
                                 double* colcnd, double* amax );

lapack_int LAPACKE_sgees_work( int matrix_layout, char jobvs, char sort,
                               LAPACK_S_SELECT2 select, lapack_int n, float* a,
                               lapack_int lda, lapack_int* sdim, float* wr,
                               float* wi, float* vs, lapack_int ldvs,
                               float* work, lapack_int lwork,
                               lapack_logical* bwork );
lapack_int LAPACKE_dgees_work( int matrix_layout, char jobvs, char sort,
                               LAPACK_D_SELECT2 select, lapack_int n, double* a,
                               lapack_int lda, lapack_int* sdim, double* wr,
                               double* wi, double* vs, lapack_int ldvs,
                               double* work, lapack_int lwork,
                               lapack_logical* bwork );
lapack_int LAPACKE_cgees_work( int matrix_layout, char jobvs, char sort,
                               LAPACK_C_SELECT1 select, lapack_int n,
                               lapack_complex_float* a, lapack_int lda,
                               lapack_int* sdim, lapack_complex_float* w,
                               lapack_complex_float* vs, lapack_int ldvs,
                               lapack_complex_float* work, lapack_int lwork,
                               float* rwork, lapack_logical* bwork );
lapack_int LAPACKE_zgees_work( int matrix_layout, char jobvs, char sort,
                               LAPACK_Z_SELECT1 select, lapack_int n,
                               lapack_complex_double* a, lapack_int lda,
                               lapack_int* sdim, lapack_complex_double* w,
                               lapack_complex_double* vs, lapack_int ldvs,
                               lapack_complex_double* work, lapack_int lwork,
                               double* rwork, lapack_logical* bwork );

lapack_int LAPACKE_sgeesx_work( int matrix_layout, char jobvs, char sort,
                                LAPACK_S_SELECT2 select, char sense,
                                lapack_int n, float* a, lapack_int lda,
                                lapack_int* sdim, float* wr, float* wi,
                                float* vs, lapack_int ldvs, float* rconde,
                                float* rcondv, float* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork,
                                lapack_logical* bwork );
lapack_int LAPACKE_dgeesx_work( int matrix_layout, char jobvs, char sort,
                                LAPACK_D_SELECT2 select, char sense,
                                lapack_int n, double* a, lapack_int lda,
                                lapack_int* sdim, double* wr, double* wi,
                                double* vs, lapack_int ldvs, double* rconde,
                                double* rcondv, double* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork,
                                lapack_logical* bwork );
lapack_int LAPACKE_cgeesx_work( int matrix_layout, char jobvs, char sort,
                                LAPACK_C_SELECT1 select, char sense,
                                lapack_int n, lapack_complex_float* a,
                                lapack_int lda, lapack_int* sdim,
                                lapack_complex_float* w,
                                lapack_complex_float* vs, lapack_int ldvs,
                                float* rconde, float* rcondv,
                                lapack_complex_float* work, lapack_int lwork,
                                float* rwork, lapack_logical* bwork );
lapack_int LAPACKE_zgeesx_work( int matrix_layout, char jobvs, char sort,
                                LAPACK_Z_SELECT1 select, char sense,
                                lapack_int n, lapack_complex_double* a,
                                lapack_int lda, lapack_int* sdim,
                                lapack_complex_double* w,
                                lapack_complex_double* vs, lapack_int ldvs,
                                double* rconde, double* rcondv,
                                lapack_complex_double* work, lapack_int lwork,
                                double* rwork, lapack_logical* bwork );

lapack_int LAPACKE_sgeev_work( int matrix_layout, char jobvl, char jobvr,
                               lapack_int n, float* a, lapack_int lda,
                               float* wr, float* wi, float* vl, lapack_int ldvl,
                               float* vr, lapack_int ldvr, float* work,
                               lapack_int lwork );
lapack_int LAPACKE_dgeev_work( int matrix_layout, char jobvl, char jobvr,
                               lapack_int n, double* a, lapack_int lda,
                               double* wr, double* wi, double* vl,
                               lapack_int ldvl, double* vr, lapack_int ldvr,
                               double* work, lapack_int lwork );
lapack_int LAPACKE_cgeev_work( int matrix_layout, char jobvl, char jobvr,
                               lapack_int n, lapack_complex_float* a,
                               lapack_int lda, lapack_complex_float* w,
                               lapack_complex_float* vl, lapack_int ldvl,
                               lapack_complex_float* vr, lapack_int ldvr,
                               lapack_complex_float* work, lapack_int lwork,
                               float* rwork );
lapack_int LAPACKE_zgeev_work( int matrix_layout, char jobvl, char jobvr,
                               lapack_int n, lapack_complex_double* a,
                               lapack_int lda, lapack_complex_double* w,
                               lapack_complex_double* vl, lapack_int ldvl,
                               lapack_complex_double* vr, lapack_int ldvr,
                               lapack_complex_double* work, lapack_int lwork,
                               double* rwork );

lapack_int LAPACKE_sgeevx_work( int matrix_layout, char balanc, char jobvl,
                                char jobvr, char sense, lapack_int n, float* a,
                                lapack_int lda, float* wr, float* wi, float* vl,
                                lapack_int ldvl, float* vr, lapack_int ldvr,
                                lapack_int* ilo, lapack_int* ihi, float* scale,
                                float* abnrm, float* rconde, float* rcondv,
                                float* work, lapack_int lwork,
                                lapack_int* iwork );
lapack_int LAPACKE_dgeevx_work( int matrix_layout, char balanc, char jobvl,
                                char jobvr, char sense, lapack_int n, double* a,
                                lapack_int lda, double* wr, double* wi,
                                double* vl, lapack_int ldvl, double* vr,
                                lapack_int ldvr, lapack_int* ilo,
                                lapack_int* ihi, double* scale, double* abnrm,
                                double* rconde, double* rcondv, double* work,
                                lapack_int lwork, lapack_int* iwork );
lapack_int LAPACKE_cgeevx_work( int matrix_layout, char balanc, char jobvl,
                                char jobvr, char sense, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_complex_float* w,
                                lapack_complex_float* vl, lapack_int ldvl,
                                lapack_complex_float* vr, lapack_int ldvr,
                                lapack_int* ilo, lapack_int* ihi, float* scale,
                                float* abnrm, float* rconde, float* rcondv,
                                lapack_complex_float* work, lapack_int lwork,
                                float* rwork );
lapack_int LAPACKE_zgeevx_work( int matrix_layout, char balanc, char jobvl,
                                char jobvr, char sense, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* w,
                                lapack_complex_double* vl, lapack_int ldvl,
                                lapack_complex_double* vr, lapack_int ldvr,
                                lapack_int* ilo, lapack_int* ihi, double* scale,
                                double* abnrm, double* rconde, double* rcondv,
                                lapack_complex_double* work, lapack_int lwork,
                                double* rwork );

lapack_int LAPACKE_sgehrd_work( int matrix_layout, lapack_int n, lapack_int ilo,
                                lapack_int ihi, float* a, lapack_int lda,
                                float* tau, float* work, lapack_int lwork );
lapack_int LAPACKE_dgehrd_work( int matrix_layout, lapack_int n, lapack_int ilo,
                                lapack_int ihi, double* a, lapack_int lda,
                                double* tau, double* work, lapack_int lwork );
lapack_int LAPACKE_cgehrd_work( int matrix_layout, lapack_int n, lapack_int ilo,
                                lapack_int ihi, lapack_complex_float* a,
                                lapack_int lda, lapack_complex_float* tau,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zgehrd_work( int matrix_layout, lapack_int n, lapack_int ilo,
                                lapack_int ihi, lapack_complex_double* a,
                                lapack_int lda, lapack_complex_double* tau,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_sgejsv_work( int matrix_layout, char joba, char jobu,
                                char jobv, char jobr, char jobt, char jobp,
                                lapack_int m, lapack_int n, float* a,
                                lapack_int lda, float* sva, float* u,
                                lapack_int ldu, float* v, lapack_int ldv,
                                float* work, lapack_int lwork,
                                lapack_int* iwork );
lapack_int LAPACKE_dgejsv_work( int matrix_layout, char joba, char jobu,
                                char jobv, char jobr, char jobt, char jobp,
                                lapack_int m, lapack_int n, double* a,
                                lapack_int lda, double* sva, double* u,
                                lapack_int ldu, double* v, lapack_int ldv,
                                double* work, lapack_int lwork,
                                lapack_int* iwork );
lapack_int LAPACKE_cgejsv_work( int matrix_layout, char joba, char jobu,
                                char jobv, char jobr, char jobt, char jobp,
                                lapack_int m, lapack_int n, lapack_complex_float* a,
                                lapack_int lda, float* sva, lapack_complex_float* u,
                                lapack_int ldu, lapack_complex_float* v, lapack_int ldv,
                                lapack_complex_float* cwork, lapack_int lwork,
                                float* work, lapack_int lrwork,
                                lapack_int* iwork );
lapack_int LAPACKE_zgejsv_work( int matrix_layout, char joba, char jobu,
                                char jobv, char jobr, char jobt, char jobp,
                                lapack_int m, lapack_int n, lapack_complex_double* a,
                                lapack_int lda, double* sva, lapack_complex_double* u,
                                lapack_int ldu, lapack_complex_double* v, lapack_int ldv,
                                lapack_complex_double* cwork, lapack_int lwork,
                                double* work, lapack_int lrwork,
                                lapack_int* iwork );

lapack_int LAPACKE_sgelq2_work( int matrix_layout, lapack_int m, lapack_int n,
                                float* a, lapack_int lda, float* tau,
                                float* work );
lapack_int LAPACKE_dgelq2_work( int matrix_layout, lapack_int m, lapack_int n,
                                double* a, lapack_int lda, double* tau,
                                double* work );
lapack_int LAPACKE_cgelq2_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_complex_float* tau,
                                lapack_complex_float* work );
lapack_int LAPACKE_zgelq2_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* tau,
                                lapack_complex_double* work );

lapack_int LAPACKE_sgelqf_work( int matrix_layout, lapack_int m, lapack_int n,
                                float* a, lapack_int lda, float* tau,
                                float* work, lapack_int lwork );
lapack_int LAPACKE_dgelqf_work( int matrix_layout, lapack_int m, lapack_int n,
                                double* a, lapack_int lda, double* tau,
                                double* work, lapack_int lwork );
lapack_int LAPACKE_cgelqf_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_complex_float* tau,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zgelqf_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* tau,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_sgels_work( int matrix_layout, char trans, lapack_int m,
                               lapack_int n, lapack_int nrhs, float* a,
                               lapack_int lda, float* b, lapack_int ldb,
                               float* work, lapack_int lwork );
lapack_int LAPACKE_dgels_work( int matrix_layout, char trans, lapack_int m,
                               lapack_int n, lapack_int nrhs, double* a,
                               lapack_int lda, double* b, lapack_int ldb,
                               double* work, lapack_int lwork );
lapack_int LAPACKE_cgels_work( int matrix_layout, char trans, lapack_int m,
                               lapack_int n, lapack_int nrhs,
                               lapack_complex_float* a, lapack_int lda,
                               lapack_complex_float* b, lapack_int ldb,
                               lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zgels_work( int matrix_layout, char trans, lapack_int m,
                               lapack_int n, lapack_int nrhs,
                               lapack_complex_double* a, lapack_int lda,
                               lapack_complex_double* b, lapack_int ldb,
                               lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_sgelsd_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int nrhs, float* a, lapack_int lda,
                                float* b, lapack_int ldb, float* s, float rcond,
                                lapack_int* rank, float* work, lapack_int lwork,
                                lapack_int* iwork );
lapack_int LAPACKE_dgelsd_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int nrhs, double* a, lapack_int lda,
                                double* b, lapack_int ldb, double* s,
                                double rcond, lapack_int* rank, double* work,
                                lapack_int lwork, lapack_int* iwork );
lapack_int LAPACKE_cgelsd_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int nrhs, lapack_complex_float* a,
                                lapack_int lda, lapack_complex_float* b,
                                lapack_int ldb, float* s, float rcond,
                                lapack_int* rank, lapack_complex_float* work,
                                lapack_int lwork, float* rwork,
                                lapack_int* iwork );
lapack_int LAPACKE_zgelsd_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int nrhs, lapack_complex_double* a,
                                lapack_int lda, lapack_complex_double* b,
                                lapack_int ldb, double* s, double rcond,
                                lapack_int* rank, lapack_complex_double* work,
                                lapack_int lwork, double* rwork,
                                lapack_int* iwork );

lapack_int LAPACKE_sgelss_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int nrhs, float* a, lapack_int lda,
                                float* b, lapack_int ldb, float* s, float rcond,
                                lapack_int* rank, float* work,
                                lapack_int lwork );
lapack_int LAPACKE_dgelss_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int nrhs, double* a, lapack_int lda,
                                double* b, lapack_int ldb, double* s,
                                double rcond, lapack_int* rank, double* work,
                                lapack_int lwork );
lapack_int LAPACKE_cgelss_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int nrhs, lapack_complex_float* a,
                                lapack_int lda, lapack_complex_float* b,
                                lapack_int ldb, float* s, float rcond,
                                lapack_int* rank, lapack_complex_float* work,
                                lapack_int lwork, float* rwork );
lapack_int LAPACKE_zgelss_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int nrhs, lapack_complex_double* a,
                                lapack_int lda, lapack_complex_double* b,
                                lapack_int ldb, double* s, double rcond,
                                lapack_int* rank, lapack_complex_double* work,
                                lapack_int lwork, double* rwork );

lapack_int LAPACKE_sgelsy_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int nrhs, float* a, lapack_int lda,
                                float* b, lapack_int ldb, lapack_int* jpvt,
                                float rcond, lapack_int* rank, float* work,
                                lapack_int lwork );
lapack_int LAPACKE_dgelsy_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int nrhs, double* a, lapack_int lda,
                                double* b, lapack_int ldb, lapack_int* jpvt,
                                double rcond, lapack_int* rank, double* work,
                                lapack_int lwork );
lapack_int LAPACKE_cgelsy_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int nrhs, lapack_complex_float* a,
                                lapack_int lda, lapack_complex_float* b,
                                lapack_int ldb, lapack_int* jpvt, float rcond,
                                lapack_int* rank, lapack_complex_float* work,
                                lapack_int lwork, float* rwork );
lapack_int LAPACKE_zgelsy_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int nrhs, lapack_complex_double* a,
                                lapack_int lda, lapack_complex_double* b,
                                lapack_int ldb, lapack_int* jpvt, double rcond,
                                lapack_int* rank, lapack_complex_double* work,
                                lapack_int lwork, double* rwork );

lapack_int LAPACKE_sgeqlf_work( int matrix_layout, lapack_int m, lapack_int n,
                                float* a, lapack_int lda, float* tau,
                                float* work, lapack_int lwork );
lapack_int LAPACKE_dgeqlf_work( int matrix_layout, lapack_int m, lapack_int n,
                                double* a, lapack_int lda, double* tau,
                                double* work, lapack_int lwork );
lapack_int LAPACKE_cgeqlf_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_complex_float* tau,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zgeqlf_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* tau,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_sgeqp3_work( int matrix_layout, lapack_int m, lapack_int n,
                                float* a, lapack_int lda, lapack_int* jpvt,
                                float* tau, float* work, lapack_int lwork );
lapack_int LAPACKE_dgeqp3_work( int matrix_layout, lapack_int m, lapack_int n,
                                double* a, lapack_int lda, lapack_int* jpvt,
                                double* tau, double* work, lapack_int lwork );
lapack_int LAPACKE_cgeqp3_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_int* jpvt, lapack_complex_float* tau,
                                lapack_complex_float* work, lapack_int lwork,
                                float* rwork );
lapack_int LAPACKE_zgeqp3_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_int* jpvt, lapack_complex_double* tau,
                                lapack_complex_double* work, lapack_int lwork,
                                double* rwork );

lapack_int LAPACKE_sgeqpf_work( int matrix_layout, lapack_int m, lapack_int n,
                                float* a, lapack_int lda, lapack_int* jpvt,
                                float* tau, float* work );
lapack_int LAPACKE_dgeqpf_work( int matrix_layout, lapack_int m, lapack_int n,
                                double* a, lapack_int lda, lapack_int* jpvt,
                                double* tau, double* work );
lapack_int LAPACKE_cgeqpf_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_int* jpvt, lapack_complex_float* tau,
                                lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_zgeqpf_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_int* jpvt, lapack_complex_double* tau,
                                lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_sgeqr2_work( int matrix_layout, lapack_int m, lapack_int n,
                                float* a, lapack_int lda, float* tau,
                                float* work );
lapack_int LAPACKE_dgeqr2_work( int matrix_layout, lapack_int m, lapack_int n,
                                double* a, lapack_int lda, double* tau,
                                double* work );
lapack_int LAPACKE_cgeqr2_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_complex_float* tau,
                                lapack_complex_float* work );
lapack_int LAPACKE_zgeqr2_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* tau,
                                lapack_complex_double* work );

lapack_int LAPACKE_sgeqrf_work( int matrix_layout, lapack_int m, lapack_int n,
                                float* a, lapack_int lda, float* tau,
                                float* work, lapack_int lwork );
lapack_int LAPACKE_dgeqrf_work( int matrix_layout, lapack_int m, lapack_int n,
                                double* a, lapack_int lda, double* tau,
                                double* work, lapack_int lwork );
lapack_int LAPACKE_cgeqrf_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_complex_float* tau,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zgeqrf_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* tau,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_sgeqrfp_work( int matrix_layout, lapack_int m, lapack_int n,
                                 float* a, lapack_int lda, float* tau,
                                 float* work, lapack_int lwork );
lapack_int LAPACKE_dgeqrfp_work( int matrix_layout, lapack_int m, lapack_int n,
                                 double* a, lapack_int lda, double* tau,
                                 double* work, lapack_int lwork );
lapack_int LAPACKE_cgeqrfp_work( int matrix_layout, lapack_int m, lapack_int n,
                                 lapack_complex_float* a, lapack_int lda,
                                 lapack_complex_float* tau,
                                 lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zgeqrfp_work( int matrix_layout, lapack_int m, lapack_int n,
                                 lapack_complex_double* a, lapack_int lda,
                                 lapack_complex_double* tau,
                                 lapack_complex_double* work,
                                 lapack_int lwork );

lapack_int LAPACKE_sgerfs_work( int matrix_layout, char trans, lapack_int n,
                                lapack_int nrhs, const float* a, lapack_int lda,
                                const float* af, lapack_int ldaf,
                                const lapack_int* ipiv, const float* b,
                                lapack_int ldb, float* x, lapack_int ldx,
                                float* ferr, float* berr, float* work,
                                lapack_int* iwork );
lapack_int LAPACKE_dgerfs_work( int matrix_layout, char trans, lapack_int n,
                                lapack_int nrhs, const double* a,
                                lapack_int lda, const double* af,
                                lapack_int ldaf, const lapack_int* ipiv,
                                const double* b, lapack_int ldb, double* x,
                                lapack_int ldx, double* ferr, double* berr,
                                double* work, lapack_int* iwork );
lapack_int LAPACKE_cgerfs_work( int matrix_layout, char trans, lapack_int n,
                                lapack_int nrhs, const lapack_complex_float* a,
                                lapack_int lda, const lapack_complex_float* af,
                                lapack_int ldaf, const lapack_int* ipiv,
                                const lapack_complex_float* b, lapack_int ldb,
                                lapack_complex_float* x, lapack_int ldx,
                                float* ferr, float* berr,
                                lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_zgerfs_work( int matrix_layout, char trans, lapack_int n,
                                lapack_int nrhs, const lapack_complex_double* a,
                                lapack_int lda, const lapack_complex_double* af,
                                lapack_int ldaf, const lapack_int* ipiv,
                                const lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* x, lapack_int ldx,
                                double* ferr, double* berr,
                                lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_sgerfsx_work( int matrix_layout, char trans, char equed,
                                 lapack_int n, lapack_int nrhs, const float* a,
                                 lapack_int lda, const float* af,
                                 lapack_int ldaf, const lapack_int* ipiv,
                                 const float* r, const float* c, const float* b,
                                 lapack_int ldb, float* x, lapack_int ldx,
                                 float* rcond, float* berr,
                                 lapack_int n_err_bnds, float* err_bnds_norm,
                                 float* err_bnds_comp, lapack_int nparams,
                                 float* params, float* work,
                                 lapack_int* iwork );
lapack_int LAPACKE_dgerfsx_work( int matrix_layout, char trans, char equed,
                                 lapack_int n, lapack_int nrhs, const double* a,
                                 lapack_int lda, const double* af,
                                 lapack_int ldaf, const lapack_int* ipiv,
                                 const double* r, const double* c,
                                 const double* b, lapack_int ldb, double* x,
                                 lapack_int ldx, double* rcond, double* berr,
                                 lapack_int n_err_bnds, double* err_bnds_norm,
                                 double* err_bnds_comp, lapack_int nparams,
                                 double* params, double* work,
                                 lapack_int* iwork );
lapack_int LAPACKE_cgerfsx_work( int matrix_layout, char trans, char equed,
                                 lapack_int n, lapack_int nrhs,
                                 const lapack_complex_float* a, lapack_int lda,
                                 const lapack_complex_float* af,
                                 lapack_int ldaf, const lapack_int* ipiv,
                                 const float* r, const float* c,
                                 const lapack_complex_float* b, lapack_int ldb,
                                 lapack_complex_float* x, lapack_int ldx,
                                 float* rcond, float* berr,
                                 lapack_int n_err_bnds, float* err_bnds_norm,
                                 float* err_bnds_comp, lapack_int nparams,
                                 float* params, lapack_complex_float* work,
                                 float* rwork );
lapack_int LAPACKE_zgerfsx_work( int matrix_layout, char trans, char equed,
                                 lapack_int n, lapack_int nrhs,
                                 const lapack_complex_double* a, lapack_int lda,
                                 const lapack_complex_double* af,
                                 lapack_int ldaf, const lapack_int* ipiv,
                                 const double* r, const double* c,
                                 const lapack_complex_double* b, lapack_int ldb,
                                 lapack_complex_double* x, lapack_int ldx,
                                 double* rcond, double* berr,
                                 lapack_int n_err_bnds, double* err_bnds_norm,
                                 double* err_bnds_comp, lapack_int nparams,
                                 double* params, lapack_complex_double* work,
                                 double* rwork );

lapack_int LAPACKE_sgerqf_work( int matrix_layout, lapack_int m, lapack_int n,
                                float* a, lapack_int lda, float* tau,
                                float* work, lapack_int lwork );
lapack_int LAPACKE_dgerqf_work( int matrix_layout, lapack_int m, lapack_int n,
                                double* a, lapack_int lda, double* tau,
                                double* work, lapack_int lwork );
lapack_int LAPACKE_cgerqf_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_complex_float* tau,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zgerqf_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* tau,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_sgesdd_work( int matrix_layout, char jobz, lapack_int m,
                                lapack_int n, float* a, lapack_int lda,
                                float* s, float* u, lapack_int ldu, float* vt,
                                lapack_int ldvt, float* work, lapack_int lwork,
                                lapack_int* iwork );
lapack_int LAPACKE_dgesdd_work( int matrix_layout, char jobz, lapack_int m,
                                lapack_int n, double* a, lapack_int lda,
                                double* s, double* u, lapack_int ldu,
                                double* vt, lapack_int ldvt, double* work,
                                lapack_int lwork, lapack_int* iwork );
lapack_int LAPACKE_cgesdd_work( int matrix_layout, char jobz, lapack_int m,
                                lapack_int n, lapack_complex_float* a,
                                lapack_int lda, float* s,
                                lapack_complex_float* u, lapack_int ldu,
                                lapack_complex_float* vt, lapack_int ldvt,
                                lapack_complex_float* work, lapack_int lwork,
                                float* rwork, lapack_int* iwork );
lapack_int LAPACKE_zgesdd_work( int matrix_layout, char jobz, lapack_int m,
                                lapack_int n, lapack_complex_double* a,
                                lapack_int lda, double* s,
                                lapack_complex_double* u, lapack_int ldu,
                                lapack_complex_double* vt, lapack_int ldvt,
                                lapack_complex_double* work, lapack_int lwork,
                                double* rwork, lapack_int* iwork );

lapack_int LAPACKE_sgedmd_work( int matrix_layout, char jobs, char jobz,
                                char jobr, char jobf, lapack_int whtsvd,
				lapack_int m, lapack_int n, float* x,
				lapack_int ldx, float* y, lapack_int ldy,
				lapack_int nrnk, float* tol, lapack_int k,
			       	float* reig, float* imeig,
				float* z, lapack_int ldz, float* res,
				float* b, lapack_int ldb, float* w,
				lapack_int ldw, float* s, lapack_int lds,
				float* work, lapack_int lwork,
				lapack_int* iwork, lapack_int liwork );

lapack_int LAPACKE_dgedmd_work( int matrix_layout, char jobs, char jobz,
                                char jobr, char jobf, lapack_int whtsvd,
				lapack_int m, lapack_int n, double* x,
				lapack_int ldx, double* y, lapack_int ldy,
				lapack_int nrnk, double* tol, lapack_int k,
			       	double* reig, double *imeig,
				double* z, lapack_int ldz, double* res,
				double* b, lapack_int ldb, double* w,
				lapack_int ldw, double* s, lapack_int lds,
				double* work, lapack_int lwork,
				lapack_int* iwork, lapack_int liwork );

lapack_int LAPACKE_cgedmd_work( int matrix_layout, char jobs, char jobz,
                                char jobr, char jobf, lapack_int whtsvd,
				lapack_int m, lapack_int n,
				lapack_complex_float* x, lapack_int ldx,
				lapack_complex_float* y, lapack_int ldy,
				lapack_int nrnk, float* tol, lapack_int k,
			       	lapack_complex_float* eigs,
                                lapack_complex_float* z, lapack_int ldz,
                                float* res,
                                lapack_complex_float* b, lapack_int ldb,
                                lapack_complex_float* w, lapack_int ldw,
                                lapack_complex_float* s, lapack_int lds,
                                lapack_complex_float* zwork, lapack_int lzwork,
                                float* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );

lapack_int LAPACKE_zgedmd_work( int matrix_layout, char jobs, char jobz,
                                char jobr, char jobf, lapack_int whtsvd,
				lapack_int m, lapack_int n,
				lapack_complex_double* x, lapack_int ldx,
				lapack_complex_double* y, lapack_int ldy,
				lapack_int nrnk, double* tol, lapack_int k, 
				lapack_complex_double* eigs,
                                lapack_complex_double* z, lapack_int ldz,
                                double* res,
                                lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* w, lapack_int ldw,
                                lapack_complex_double* s, lapack_int lds,
                                lapack_complex_double* zwork, lapack_int lzwork,
                                double* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );

lapack_int LAPACKE_sgedmdq_work( int matrix_layout, char jobs, char jobz,
                                 char jobr, char jobq, char jobt, char jobf,
                                 lapack_int whtsvd, lapack_int m, lapack_int n,
                                 float* f, lapack_int ldf, float* x,
                                 lapack_int ldx, float* y, lapack_int ldy,
                                 lapack_int nrnk, float* tol, lapack_int k,
				 float* reig, float *imeig, float* z,
                                 lapack_int ldz, float* res, float* b,
                                 lapack_int ldb, float* v, lapack_int ldv,
                                 float* s, lapack_int lds, float* work,
                                 lapack_int lwork, lapack_int* iwork,
                                 lapack_int liwork );

lapack_int LAPACKE_dgedmdq_work( int matrix_layout, char jobs, char jobz,
                                 char jobr, char jobq, char jobt, char jobf,
                                 lapack_int whtsvd, lapack_int m, lapack_int n,
                                 double* f, lapack_int ldf, double* x,
                                 lapack_int ldx, double* y, lapack_int ldy,
                                 lapack_int nrnk, double* tol, lapack_int k,
				 double* reig, double* imeig, double* z,
                                 lapack_int ldz, double* res, double* b,
                                 lapack_int ldb, double* v, lapack_int ldv,
                                 double* s, lapack_int lds, double* work,
                                 lapack_int lwork, lapack_int* iwork,
                                 lapack_int liwork );

lapack_int LAPACKE_cgedmdq_work( int matrix_layout, char jobs, char jobz,
                                 char jobr, char jobq, char jobt, char jobf,
                                 lapack_int whtsvd, lapack_int m, lapack_int n,
                                 lapack_complex_float* f, lapack_int ldf,
                                 lapack_complex_float* x, lapack_int ldx,
                                 lapack_complex_float* y, lapack_int ldy,
                                 lapack_int nrnk, float* tol, lapack_int k,
                                 lapack_complex_float* eigs,
                                 lapack_complex_float* z, lapack_int ldz,
                                 float* res,
                                 lapack_complex_float* b, lapack_int ldb,
                                 lapack_complex_float* v, lapack_int ldv,
                                 lapack_complex_float* s, lapack_int lds,
                                 lapack_complex_float* zwork, lapack_int lzwork,
                                 float* work, lapack_int lwork,
                                 lapack_int* iwork, lapack_int liwork);

lapack_int LAPACKE_zgedmdq_work( int matrix_layout, char jobs, char jobz,
                                 char jobr, char jobq, char jobt, char jobf,
                                 lapack_int whtsvd, lapack_int m, lapack_int n,
                                 lapack_complex_double* f, lapack_int ldf,
                                 lapack_complex_double* x, lapack_int ldx,
                                 lapack_complex_double* y, lapack_int ldy,
                                 lapack_int nrnk, double* tol, lapack_int k,
                                 lapack_complex_double* eigs,
                                 lapack_complex_double* z, lapack_int ldz,
                                 double* res,
                                 lapack_complex_double* b, lapack_int ldb,
                                 lapack_complex_double* v, lapack_int ldv,
                                 lapack_complex_double* s, lapack_int lds,
                                 lapack_complex_double* zwork, lapack_int lzwork,
                                 double* work, lapack_int lwork,
                                 lapack_int* iwork, lapack_int liwork);

lapack_int LAPACKE_sgesv_work( int matrix_layout, lapack_int n, lapack_int nrhs,
                               float* a, lapack_int lda, lapack_int* ipiv,
                               float* b, lapack_int ldb );
lapack_int LAPACKE_dgesv_work( int matrix_layout, lapack_int n, lapack_int nrhs,
                               double* a, lapack_int lda, lapack_int* ipiv,
                               double* b, lapack_int ldb );
lapack_int LAPACKE_cgesv_work( int matrix_layout, lapack_int n, lapack_int nrhs,
                               lapack_complex_float* a, lapack_int lda,
                               lapack_int* ipiv, lapack_complex_float* b,
                               lapack_int ldb );
lapack_int LAPACKE_zgesv_work( int matrix_layout, lapack_int n, lapack_int nrhs,
                               lapack_complex_double* a, lapack_int lda,
                               lapack_int* ipiv, lapack_complex_double* b,
                               lapack_int ldb );
lapack_int LAPACKE_dsgesv_work( int matrix_layout, lapack_int n, lapack_int nrhs,
                                double* a, lapack_int lda, lapack_int* ipiv,
                                double* b, lapack_int ldb, double* x,
                                lapack_int ldx, double* work, float* swork,
                                lapack_int* iter );
lapack_int LAPACKE_zcgesv_work( int matrix_layout, lapack_int n, lapack_int nrhs,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_int* ipiv, lapack_complex_double* b,
                                lapack_int ldb, lapack_complex_double* x,
                                lapack_int ldx, lapack_complex_double* work,
                                lapack_complex_float* swork, double* rwork,
                                lapack_int* iter );

lapack_int LAPACKE_sgesvd_work( int matrix_layout, char jobu, char jobvt,
                                lapack_int m, lapack_int n, float* a,
                                lapack_int lda, float* s, float* u,
                                lapack_int ldu, float* vt, lapack_int ldvt,
                                float* work, lapack_int lwork );
lapack_int LAPACKE_dgesvd_work( int matrix_layout, char jobu, char jobvt,
                                lapack_int m, lapack_int n, double* a,
                                lapack_int lda, double* s, double* u,
                                lapack_int ldu, double* vt, lapack_int ldvt,
                                double* work, lapack_int lwork );
lapack_int LAPACKE_cgesvd_work( int matrix_layout, char jobu, char jobvt,
                                lapack_int m, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                float* s, lapack_complex_float* u,
                                lapack_int ldu, lapack_complex_float* vt,
                                lapack_int ldvt, lapack_complex_float* work,
                                lapack_int lwork, float* rwork );
lapack_int LAPACKE_zgesvd_work( int matrix_layout, char jobu, char jobvt,
                                lapack_int m, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                double* s, lapack_complex_double* u,
                                lapack_int ldu, lapack_complex_double* vt,
                                lapack_int ldvt, lapack_complex_double* work,
                                lapack_int lwork, double* rwork );

lapack_int LAPACKE_sgesvdx_work( int matrix_layout, char jobu, char jobvt, char range,
                                 lapack_int m, lapack_int n, float* a,
                                 lapack_int lda, float vl, float vu,
                                 lapack_int il, lapack_int iu, lapack_int* ns,
                                 float* s, float* u, lapack_int ldu,
                                 float* vt, lapack_int ldvt,
                                 float* work, lapack_int lwork, lapack_int* iwork );
lapack_int LAPACKE_dgesvdx_work( int matrix_layout, char jobu, char jobvt, char range,
                                 lapack_int m, lapack_int n, double* a,
                                 lapack_int lda, double vl, double vu,
                                 lapack_int il, lapack_int iu, lapack_int* ns,
                                 double* s, double* u, lapack_int ldu,
                                 double* vt, lapack_int ldvt,
                                 double* work, lapack_int lwork, lapack_int* iwork );
lapack_int LAPACKE_cgesvdx_work( int matrix_layout, char jobu, char jobvt, char range,
                                 lapack_int m, lapack_int n, lapack_complex_float* a,
                                 lapack_int lda, float vl, float vu,
                                 lapack_int il, lapack_int iu, lapack_int* ns,
                                 float* s, lapack_complex_float* u, lapack_int ldu,
                                 lapack_complex_float* vt, lapack_int ldvt,
                                 lapack_complex_float* work, lapack_int lwork,
                                 float* rwork, lapack_int* iwork );
lapack_int LAPACKE_zgesvdx_work( int matrix_layout, char jobu, char jobvt, char range,
                                 lapack_int m, lapack_int n, lapack_complex_double* a,
                                 lapack_int lda, double vl, double vu,
                                 lapack_int il, lapack_int iu, lapack_int* ns,
                                 double* s, lapack_complex_double* u, lapack_int ldu,
                                 lapack_complex_double* vt, lapack_int ldvt,
                                 lapack_complex_double* work, lapack_int lwork,
                                 double* rwork, lapack_int* iwork );

lapack_int LAPACKE_sgesvdq_work( int matrix_layout, char joba, char jobp,
                                char jobr, char jobu, char jobv,
                                lapack_int m, lapack_int n, float* a,
                                lapack_int lda, float* s, float* u,
                                lapack_int ldu, float* v, lapack_int ldv,
                                lapack_int* numrank,
                                lapack_int* iwork, lapack_int liwork,
                                float* work, lapack_int lwork,
                                float* rwork, lapack_int lrwork);
lapack_int LAPACKE_dgesvdq_work( int matrix_layout, char joba, char jobp,
                                char jobr, char jobu, char jobv,
                                lapack_int m, lapack_int n, double* a,
                                lapack_int lda, double* s, double* u,
                                lapack_int ldu, double* v, lapack_int ldv,
                                lapack_int* numrank,
                                lapack_int* iwork, lapack_int liwork,
                                double* work, lapack_int lwork,
                                double* rwork, lapack_int lrwork);
lapack_int LAPACKE_cgesvdq_work( int matrix_layout, char joba, char jobp,
                                char jobr, char jobu, char jobv,
                                lapack_int m, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                float* s, lapack_complex_float* u,
                                lapack_int ldu, lapack_complex_float* v,
                                lapack_int ldv, lapack_int* numrank,
                                lapack_int* iwork, lapack_int liwork,
                                lapack_complex_float* cwork, lapack_int lcwork,
                                float* rwork, lapack_int lrwork);
lapack_int LAPACKE_zgesvdq_work( int matrix_layout, char joba, char jobp,
                                char jobr, char jobu, char jobv,
                                lapack_int m, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                double* s, lapack_complex_double* u,
                                lapack_int ldu, lapack_complex_double* v,
                                lapack_int ldv, lapack_int* numrank,
                                lapack_int* iwork, lapack_int liwork,
                                lapack_complex_double* cwork, lapack_int lcwork,
                                double* rwork, lapack_int lrwork);

lapack_int LAPACKE_sgesvj_work( int matrix_layout, char joba, char jobu,
                                char jobv, lapack_int m, lapack_int n, float* a,
                                lapack_int lda, float* sva, lapack_int mv,
                                float* v, lapack_int ldv, float* work,
                                lapack_int lwork );
lapack_int LAPACKE_dgesvj_work( int matrix_layout, char joba, char jobu,
                                char jobv, lapack_int m, lapack_int n,
                                double* a, lapack_int lda, double* sva,
                                lapack_int mv, double* v, lapack_int ldv,
                                double* work, lapack_int lwork );
lapack_int LAPACKE_cgesvj_work( int matrix_layout, char joba, char jobu,
                                char jobv, lapack_int m, lapack_int n, lapack_complex_float* a,
                                lapack_int lda, float* sva, lapack_int mv,
                                lapack_complex_float* v, lapack_int ldv,
                                lapack_complex_float* cwork, lapack_int lwork,
                                float* rwork,lapack_int lrwork );
lapack_int LAPACKE_zgesvj_work( int matrix_layout, char joba, char jobu,
                                char jobv, lapack_int m, lapack_int n,
                                lapack_complex_double* a, lapack_int lda, double* sva,
                                lapack_int mv, lapack_complex_double* v, lapack_int ldv,
                                lapack_complex_double* cwork, lapack_int lwork,
                                double* rwork, lapack_int lrwork );

lapack_int LAPACKE_sgesvx_work( int matrix_layout, char fact, char trans,
                                lapack_int n, lapack_int nrhs, float* a,
                                lapack_int lda, float* af, lapack_int ldaf,
                                lapack_int* ipiv, char* equed, float* r,
                                float* c, float* b, lapack_int ldb, float* x,
                                lapack_int ldx, float* rcond, float* ferr,
                                float* berr, float* work, lapack_int* iwork );
lapack_int LAPACKE_dgesvx_work( int matrix_layout, char fact, char trans,
                                lapack_int n, lapack_int nrhs, double* a,
                                lapack_int lda, double* af, lapack_int ldaf,
                                lapack_int* ipiv, char* equed, double* r,
                                double* c, double* b, lapack_int ldb, double* x,
                                lapack_int ldx, double* rcond, double* ferr,
                                double* berr, double* work, lapack_int* iwork );
lapack_int LAPACKE_cgesvx_work( int matrix_layout, char fact, char trans,
                                lapack_int n, lapack_int nrhs,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_complex_float* af, lapack_int ldaf,
                                lapack_int* ipiv, char* equed, float* r,
                                float* c, lapack_complex_float* b,
                                lapack_int ldb, lapack_complex_float* x,
                                lapack_int ldx, float* rcond, float* ferr,
                                float* berr, lapack_complex_float* work,
                                float* rwork );
lapack_int LAPACKE_zgesvx_work( int matrix_layout, char fact, char trans,
                                lapack_int n, lapack_int nrhs,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* af, lapack_int ldaf,
                                lapack_int* ipiv, char* equed, double* r,
                                double* c, lapack_complex_double* b,
                                lapack_int ldb, lapack_complex_double* x,
                                lapack_int ldx, double* rcond, double* ferr,
                                double* berr, lapack_complex_double* work,
                                double* rwork );

lapack_int LAPACKE_sgesvxx_work( int matrix_layout, char fact, char trans,
                                 lapack_int n, lapack_int nrhs, float* a,
                                 lapack_int lda, float* af, lapack_int ldaf,
                                 lapack_int* ipiv, char* equed, float* r,
                                 float* c, float* b, lapack_int ldb, float* x,
                                 lapack_int ldx, float* rcond, float* rpvgrw,
                                 float* berr, lapack_int n_err_bnds,
                                 float* err_bnds_norm, float* err_bnds_comp,
                                 lapack_int nparams, float* params, float* work,
                                 lapack_int* iwork );
lapack_int LAPACKE_dgesvxx_work( int matrix_layout, char fact, char trans,
                                 lapack_int n, lapack_int nrhs, double* a,
                                 lapack_int lda, double* af, lapack_int ldaf,
                                 lapack_int* ipiv, char* equed, double* r,
                                 double* c, double* b, lapack_int ldb,
                                 double* x, lapack_int ldx, double* rcond,
                                 double* rpvgrw, double* berr,
                                 lapack_int n_err_bnds, double* err_bnds_norm,
                                 double* err_bnds_comp, lapack_int nparams,
                                 double* params, double* work,
                                 lapack_int* iwork );
lapack_int LAPACKE_cgesvxx_work( int matrix_layout, char fact, char trans,
                                 lapack_int n, lapack_int nrhs,
                                 lapack_complex_float* a, lapack_int lda,
                                 lapack_complex_float* af, lapack_int ldaf,
                                 lapack_int* ipiv, char* equed, float* r,
                                 float* c, lapack_complex_float* b,
                                 lapack_int ldb, lapack_complex_float* x,
                                 lapack_int ldx, float* rcond, float* rpvgrw,
                                 float* berr, lapack_int n_err_bnds,
                                 float* err_bnds_norm, float* err_bnds_comp,
                                 lapack_int nparams, float* params,
                                 lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_zgesvxx_work( int matrix_layout, char fact, char trans,
                                 lapack_int n, lapack_int nrhs,
                                 lapack_complex_double* a, lapack_int lda,
                                 lapack_complex_double* af, lapack_int ldaf,
                                 lapack_int* ipiv, char* equed, double* r,
                                 double* c, lapack_complex_double* b,
                                 lapack_int ldb, lapack_complex_double* x,
                                 lapack_int ldx, double* rcond, double* rpvgrw,
                                 double* berr, lapack_int n_err_bnds,
                                 double* err_bnds_norm, double* err_bnds_comp,
                                 lapack_int nparams, double* params,
                                 lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_sgetf2_work( int matrix_layout, lapack_int m, lapack_int n,
                                float* a, lapack_int lda, lapack_int* ipiv );
lapack_int LAPACKE_dgetf2_work( int matrix_layout, lapack_int m, lapack_int n,
                                double* a, lapack_int lda, lapack_int* ipiv );
lapack_int LAPACKE_cgetf2_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_int* ipiv );
lapack_int LAPACKE_zgetf2_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_int* ipiv );

lapack_int LAPACKE_sgetrf_work( int matrix_layout, lapack_int m, lapack_int n,
                                float* a, lapack_int lda, lapack_int* ipiv );
lapack_int LAPACKE_dgetrf_work( int matrix_layout, lapack_int m, lapack_int n,
                                double* a, lapack_int lda, lapack_int* ipiv );
lapack_int LAPACKE_cgetrf_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_int* ipiv );
lapack_int LAPACKE_zgetrf_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_int* ipiv );

lapack_int LAPACKE_sgetrf2_work( int matrix_layout, lapack_int m, lapack_int n,
                                float* a, lapack_int lda, lapack_int* ipiv );
lapack_int LAPACKE_dgetrf2_work( int matrix_layout, lapack_int m, lapack_int n,
                                double* a, lapack_int lda, lapack_int* ipiv );
lapack_int LAPACKE_cgetrf2_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_int* ipiv );
lapack_int LAPACKE_zgetrf2_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_int* ipiv );

lapack_int LAPACKE_sgetri_work( int matrix_layout, lapack_int n, float* a,
                                lapack_int lda, const lapack_int* ipiv,
                                float* work, lapack_int lwork );
lapack_int LAPACKE_dgetri_work( int matrix_layout, lapack_int n, double* a,
                                lapack_int lda, const lapack_int* ipiv,
                                double* work, lapack_int lwork );
lapack_int LAPACKE_cgetri_work( int matrix_layout, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                const lapack_int* ipiv,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zgetri_work( int matrix_layout, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                const lapack_int* ipiv,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_sgetrs_work( int matrix_layout, char trans, lapack_int n,
                                lapack_int nrhs, const float* a, lapack_int lda,
                                const lapack_int* ipiv, float* b,
                                lapack_int ldb );
lapack_int LAPACKE_dgetrs_work( int matrix_layout, char trans, lapack_int n,
                                lapack_int nrhs, const double* a,
                                lapack_int lda, const lapack_int* ipiv,
                                double* b, lapack_int ldb );
lapack_int LAPACKE_cgetrs_work( int matrix_layout, char trans, lapack_int n,
                                lapack_int nrhs, const lapack_complex_float* a,
                                lapack_int lda, const lapack_int* ipiv,
                                lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zgetrs_work( int matrix_layout, char trans, lapack_int n,
                                lapack_int nrhs, const lapack_complex_double* a,
                                lapack_int lda, const lapack_int* ipiv,
                                lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_sggbak_work( int matrix_layout, char job, char side,
                                lapack_int n, lapack_int ilo, lapack_int ihi,
                                const float* lscale, const float* rscale,
                                lapack_int m, float* v, lapack_int ldv );
lapack_int LAPACKE_dggbak_work( int matrix_layout, char job, char side,
                                lapack_int n, lapack_int ilo, lapack_int ihi,
                                const double* lscale, const double* rscale,
                                lapack_int m, double* v, lapack_int ldv );
lapack_int LAPACKE_cggbak_work( int matrix_layout, char job, char side,
                                lapack_int n, lapack_int ilo, lapack_int ihi,
                                const float* lscale, const float* rscale,
                                lapack_int m, lapack_complex_float* v,
                                lapack_int ldv );
lapack_int LAPACKE_zggbak_work( int matrix_layout, char job, char side,
                                lapack_int n, lapack_int ilo, lapack_int ihi,
                                const double* lscale, const double* rscale,
                                lapack_int m, lapack_complex_double* v,
                                lapack_int ldv );

lapack_int LAPACKE_sggbal_work( int matrix_layout, char job, lapack_int n,
                                float* a, lapack_int lda, float* b,
                                lapack_int ldb, lapack_int* ilo,
                                lapack_int* ihi, float* lscale, float* rscale,
                                float* work );
lapack_int LAPACKE_dggbal_work( int matrix_layout, char job, lapack_int n,
                                double* a, lapack_int lda, double* b,
                                lapack_int ldb, lapack_int* ilo,
                                lapack_int* ihi, double* lscale, double* rscale,
                                double* work );
lapack_int LAPACKE_cggbal_work( int matrix_layout, char job, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_complex_float* b, lapack_int ldb,
                                lapack_int* ilo, lapack_int* ihi, float* lscale,
                                float* rscale, float* work );
lapack_int LAPACKE_zggbal_work( int matrix_layout, char job, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* b, lapack_int ldb,
                                lapack_int* ilo, lapack_int* ihi,
                                double* lscale, double* rscale, double* work );

lapack_int LAPACKE_sgges_work( int matrix_layout, char jobvsl, char jobvsr,
                               char sort, LAPACK_S_SELECT3 selctg, lapack_int n,
                               float* a, lapack_int lda, float* b,
                               lapack_int ldb, lapack_int* sdim, float* alphar,
                               float* alphai, float* beta, float* vsl,
                               lapack_int ldvsl, float* vsr, lapack_int ldvsr,
                               float* work, lapack_int lwork,
                               lapack_logical* bwork );
lapack_int LAPACKE_dgges_work( int matrix_layout, char jobvsl, char jobvsr,
                               char sort, LAPACK_D_SELECT3 selctg, lapack_int n,
                               double* a, lapack_int lda, double* b,
                               lapack_int ldb, lapack_int* sdim, double* alphar,
                               double* alphai, double* beta, double* vsl,
                               lapack_int ldvsl, double* vsr, lapack_int ldvsr,
                               double* work, lapack_int lwork,
                               lapack_logical* bwork );
lapack_int LAPACKE_cgges_work( int matrix_layout, char jobvsl, char jobvsr,
                               char sort, LAPACK_C_SELECT2 selctg, lapack_int n,
                               lapack_complex_float* a, lapack_int lda,
                               lapack_complex_float* b, lapack_int ldb,
                               lapack_int* sdim, lapack_complex_float* alpha,
                               lapack_complex_float* beta,
                               lapack_complex_float* vsl, lapack_int ldvsl,
                               lapack_complex_float* vsr, lapack_int ldvsr,
                               lapack_complex_float* work, lapack_int lwork,
                               float* rwork, lapack_logical* bwork );
lapack_int LAPACKE_zgges_work( int matrix_layout, char jobvsl, char jobvsr,
                               char sort, LAPACK_Z_SELECT2 selctg, lapack_int n,
                               lapack_complex_double* a, lapack_int lda,
                               lapack_complex_double* b, lapack_int ldb,
                               lapack_int* sdim, lapack_complex_double* alpha,
                               lapack_complex_double* beta,
                               lapack_complex_double* vsl, lapack_int ldvsl,
                               lapack_complex_double* vsr, lapack_int ldvsr,
                               lapack_complex_double* work, lapack_int lwork,
                               double* rwork, lapack_logical* bwork );

lapack_int LAPACKE_sgges3_work( int matrix_layout, char jobvsl, char jobvsr,
                                char sort, LAPACK_S_SELECT3 selctg,
                                lapack_int n,
                                float* a, lapack_int lda,
                                float* b, lapack_int ldb, lapack_int* sdim,
                                float* alphar, float* alphai, float* beta,
                                float* vsl, lapack_int ldvsl,
                                float* vsr, lapack_int ldvsr,
                                float* work, lapack_int lwork,
                                lapack_logical* bwork );
lapack_int LAPACKE_dgges3_work( int matrix_layout, char jobvsl, char jobvsr,
                                char sort, LAPACK_D_SELECT3 selctg,
                                lapack_int n,
                                double* a, lapack_int lda,
                                double* b, lapack_int ldb, lapack_int* sdim,
                                double* alphar, double* alphai, double* beta,
                                double* vsl, lapack_int ldvsl,
                                double* vsr, lapack_int ldvsr,
                                double* work, lapack_int lwork,
                                lapack_logical* bwork );
lapack_int LAPACKE_cgges3_work( int matrix_layout, char jobvsl, char jobvsr,
                                char sort, LAPACK_C_SELECT2 selctg,
                                lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_complex_float* b, lapack_int ldb,
                                lapack_int* sdim, lapack_complex_float* alpha,
                                lapack_complex_float* beta,
                                lapack_complex_float* vsl, lapack_int ldvsl,
                                lapack_complex_float* vsr, lapack_int ldvsr,
                                lapack_complex_float* work, lapack_int lwork,
                                float* rwork, lapack_logical* bwork );
lapack_int LAPACKE_zgges3_work( int matrix_layout, char jobvsl, char jobvsr,
                                char sort, LAPACK_Z_SELECT2 selctg,
                                lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* b, lapack_int ldb,
                                lapack_int* sdim, lapack_complex_double* alpha,
                                lapack_complex_double* beta,
                                lapack_complex_double* vsl, lapack_int ldvsl,
                                lapack_complex_double* vsr, lapack_int ldvsr,
                                lapack_complex_double* work, lapack_int lwork,
                                double* rwork, lapack_logical* bwork );

lapack_int LAPACKE_sggesx_work( int matrix_layout, char jobvsl, char jobvsr,
                                char sort, LAPACK_S_SELECT3 selctg, char sense,
                                lapack_int n, float* a, lapack_int lda,
                                float* b, lapack_int ldb, lapack_int* sdim,
                                float* alphar, float* alphai, float* beta,
                                float* vsl, lapack_int ldvsl, float* vsr,
                                lapack_int ldvsr, float* rconde, float* rcondv,
                                float* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork,
                                lapack_logical* bwork );
lapack_int LAPACKE_dggesx_work( int matrix_layout, char jobvsl, char jobvsr,
                                char sort, LAPACK_D_SELECT3 selctg, char sense,
                                lapack_int n, double* a, lapack_int lda,
                                double* b, lapack_int ldb, lapack_int* sdim,
                                double* alphar, double* alphai, double* beta,
                                double* vsl, lapack_int ldvsl, double* vsr,
                                lapack_int ldvsr, double* rconde,
                                double* rcondv, double* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork,
                                lapack_logical* bwork );
lapack_int LAPACKE_cggesx_work( int matrix_layout, char jobvsl, char jobvsr,
                                char sort, LAPACK_C_SELECT2 selctg, char sense,
                                lapack_int n, lapack_complex_float* a,
                                lapack_int lda, lapack_complex_float* b,
                                lapack_int ldb, lapack_int* sdim,
                                lapack_complex_float* alpha,
                                lapack_complex_float* beta,
                                lapack_complex_float* vsl, lapack_int ldvsl,
                                lapack_complex_float* vsr, lapack_int ldvsr,
                                float* rconde, float* rcondv,
                                lapack_complex_float* work, lapack_int lwork,
                                float* rwork, lapack_int* iwork,
                                lapack_int liwork, lapack_logical* bwork );
lapack_int LAPACKE_zggesx_work( int matrix_layout, char jobvsl, char jobvsr,
                                char sort, LAPACK_Z_SELECT2 selctg, char sense,
                                lapack_int n, lapack_complex_double* a,
                                lapack_int lda, lapack_complex_double* b,
                                lapack_int ldb, lapack_int* sdim,
                                lapack_complex_double* alpha,
                                lapack_complex_double* beta,
                                lapack_complex_double* vsl, lapack_int ldvsl,
                                lapack_complex_double* vsr, lapack_int ldvsr,
                                double* rconde, double* rcondv,
                                lapack_complex_double* work, lapack_int lwork,
                                double* rwork, lapack_int* iwork,
                                lapack_int liwork, lapack_logical* bwork );

lapack_int LAPACKE_sggev_work( int matrix_layout, char jobvl, char jobvr,
                               lapack_int n, float* a, lapack_int lda, float* b,
                               lapack_int ldb, float* alphar, float* alphai,
                               float* beta, float* vl, lapack_int ldvl,
                               float* vr, lapack_int ldvr, float* work,
                               lapack_int lwork );
lapack_int LAPACKE_dggev_work( int matrix_layout, char jobvl, char jobvr,
                               lapack_int n, double* a, lapack_int lda,
                               double* b, lapack_int ldb, double* alphar,
                               double* alphai, double* beta, double* vl,
                               lapack_int ldvl, double* vr, lapack_int ldvr,
                               double* work, lapack_int lwork );
lapack_int LAPACKE_cggev_work( int matrix_layout, char jobvl, char jobvr,
                               lapack_int n, lapack_complex_float* a,
                               lapack_int lda, lapack_complex_float* b,
                               lapack_int ldb, lapack_complex_float* alpha,
                               lapack_complex_float* beta,
                               lapack_complex_float* vl, lapack_int ldvl,
                               lapack_complex_float* vr, lapack_int ldvr,
                               lapack_complex_float* work, lapack_int lwork,
                               float* rwork );
lapack_int LAPACKE_zggev_work( int matrix_layout, char jobvl, char jobvr,
                               lapack_int n, lapack_complex_double* a,
                               lapack_int lda, lapack_complex_double* b,
                               lapack_int ldb, lapack_complex_double* alpha,
                               lapack_complex_double* beta,
                               lapack_complex_double* vl, lapack_int ldvl,
                               lapack_complex_double* vr, lapack_int ldvr,
                               lapack_complex_double* work, lapack_int lwork,
                               double* rwork );

lapack_int LAPACKE_sggev3_work( int matrix_layout, char jobvl, char jobvr,
                                lapack_int n,
                                float* a, lapack_int lda,
                                float* b, lapack_int ldb,
                                float* alphar, float* alphai, float* beta,
                                float* vl, lapack_int ldvl,
                                float* vr, lapack_int ldvr,
                                float* work, lapack_int lwork );
lapack_int LAPACKE_dggev3_work( int matrix_layout, char jobvl, char jobvr,
                                lapack_int n,
                                double* a, lapack_int lda,
                                double* b, lapack_int ldb,
                                double* alphar, double* alphai, double* beta,
                                double* vl, lapack_int ldvl,
                                double* vr, lapack_int ldvr,
                                double* work, lapack_int lwork );
lapack_int LAPACKE_cggev3_work( int matrix_layout, char jobvl, char jobvr,
                                lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_complex_float* b, lapack_int ldb,
                                lapack_complex_float* alpha,
                                lapack_complex_float* beta,
                                lapack_complex_float* vl, lapack_int ldvl,
                                lapack_complex_float* vr, lapack_int ldvr,
                                lapack_complex_float* work, lapack_int lwork,
                                float* rwork );
lapack_int LAPACKE_zggev3_work( int matrix_layout, char jobvl, char jobvr,
                                lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* alpha,
                                lapack_complex_double* beta,
                                lapack_complex_double* vl, lapack_int ldvl,
                                lapack_complex_double* vr, lapack_int ldvr,
                                lapack_complex_double* work, lapack_int lwork,
                                double* rwork );

lapack_int LAPACKE_sggevx_work( int matrix_layout, char balanc, char jobvl,
                                char jobvr, char sense, lapack_int n, float* a,
                                lapack_int lda, float* b, lapack_int ldb,
                                float* alphar, float* alphai, float* beta,
                                float* vl, lapack_int ldvl, float* vr,
                                lapack_int ldvr, lapack_int* ilo,
                                lapack_int* ihi, float* lscale, float* rscale,
                                float* abnrm, float* bbnrm, float* rconde,
                                float* rcondv, float* work, lapack_int lwork,
                                lapack_int* iwork, lapack_logical* bwork );
lapack_int LAPACKE_dggevx_work( int matrix_layout, char balanc, char jobvl,
                                char jobvr, char sense, lapack_int n, double* a,
                                lapack_int lda, double* b, lapack_int ldb,
                                double* alphar, double* alphai, double* beta,
                                double* vl, lapack_int ldvl, double* vr,
                                lapack_int ldvr, lapack_int* ilo,
                                lapack_int* ihi, double* lscale, double* rscale,
                                double* abnrm, double* bbnrm, double* rconde,
                                double* rcondv, double* work, lapack_int lwork,
                                lapack_int* iwork, lapack_logical* bwork );
lapack_int LAPACKE_cggevx_work( int matrix_layout, char balanc, char jobvl,
                                char jobvr, char sense, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_complex_float* b, lapack_int ldb,
                                lapack_complex_float* alpha,
                                lapack_complex_float* beta,
                                lapack_complex_float* vl, lapack_int ldvl,
                                lapack_complex_float* vr, lapack_int ldvr,
                                lapack_int* ilo, lapack_int* ihi, float* lscale,
                                float* rscale, float* abnrm, float* bbnrm,
                                float* rconde, float* rcondv,
                                lapack_complex_float* work, lapack_int lwork,
                                float* rwork, lapack_int* iwork,
                                lapack_logical* bwork );
lapack_int LAPACKE_zggevx_work( int matrix_layout, char balanc, char jobvl,
                                char jobvr, char sense, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* alpha,
                                lapack_complex_double* beta,
                                lapack_complex_double* vl, lapack_int ldvl,
                                lapack_complex_double* vr, lapack_int ldvr,
                                lapack_int* ilo, lapack_int* ihi,
                                double* lscale, double* rscale, double* abnrm,
                                double* bbnrm, double* rconde, double* rcondv,
                                lapack_complex_double* work, lapack_int lwork,
                                double* rwork, lapack_int* iwork,
                                lapack_logical* bwork );

lapack_int LAPACKE_sggglm_work( int matrix_layout, lapack_int n, lapack_int m,
                                lapack_int p, float* a, lapack_int lda,
                                float* b, lapack_int ldb, float* d, float* x,
                                float* y, float* work, lapack_int lwork );
lapack_int LAPACKE_dggglm_work( int matrix_layout, lapack_int n, lapack_int m,
                                lapack_int p, double* a, lapack_int lda,
                                double* b, lapack_int ldb, double* d, double* x,
                                double* y, double* work, lapack_int lwork );
lapack_int LAPACKE_cggglm_work( int matrix_layout, lapack_int n, lapack_int m,
                                lapack_int p, lapack_complex_float* a,
                                lapack_int lda, lapack_complex_float* b,
                                lapack_int ldb, lapack_complex_float* d,
                                lapack_complex_float* x,
                                lapack_complex_float* y,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zggglm_work( int matrix_layout, lapack_int n, lapack_int m,
                                lapack_int p, lapack_complex_double* a,
                                lapack_int lda, lapack_complex_double* b,
                                lapack_int ldb, lapack_complex_double* d,
                                lapack_complex_double* x,
                                lapack_complex_double* y,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_sgghrd_work( int matrix_layout, char compq, char compz,
                                lapack_int n, lapack_int ilo, lapack_int ihi,
                                float* a, lapack_int lda, float* b,
                                lapack_int ldb, float* q, lapack_int ldq,
                                float* z, lapack_int ldz );
lapack_int LAPACKE_dgghrd_work( int matrix_layout, char compq, char compz,
                                lapack_int n, lapack_int ilo, lapack_int ihi,
                                double* a, lapack_int lda, double* b,
                                lapack_int ldb, double* q, lapack_int ldq,
                                double* z, lapack_int ldz );
lapack_int LAPACKE_cgghrd_work( int matrix_layout, char compq, char compz,
                                lapack_int n, lapack_int ilo, lapack_int ihi,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_complex_float* b, lapack_int ldb,
                                lapack_complex_float* q, lapack_int ldq,
                                lapack_complex_float* z, lapack_int ldz );
lapack_int LAPACKE_zgghrd_work( int matrix_layout, char compq, char compz,
                                lapack_int n, lapack_int ilo, lapack_int ihi,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* q, lapack_int ldq,
                                lapack_complex_double* z, lapack_int ldz );

lapack_int LAPACKE_sgghd3_work( int matrix_layout, char compq, char compz,
                                lapack_int n, lapack_int ilo, lapack_int ihi,
                                float* a, lapack_int lda,
                                float* b, lapack_int ldb,
                                float* q, lapack_int ldq,
                                float* z, lapack_int ldz,
                                float* work, lapack_int lwork );
lapack_int LAPACKE_dgghd3_work( int matrix_layout, char compq, char compz,
                                lapack_int n, lapack_int ilo, lapack_int ihi,
                                double* a, lapack_int lda,
                                double* b, lapack_int ldb,
                                double* q, lapack_int ldq,
                                double* z, lapack_int ldz,
                                double* work, lapack_int lwork );
lapack_int LAPACKE_cgghd3_work( int matrix_layout, char compq, char compz,
                                lapack_int n, lapack_int ilo, lapack_int ihi,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_complex_float* b, lapack_int ldb,
                                lapack_complex_float* q, lapack_int ldq,
                                lapack_complex_float* z, lapack_int ldz,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zgghd3_work( int matrix_layout, char compq, char compz,
                                lapack_int n, lapack_int ilo, lapack_int ihi,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* q, lapack_int ldq,
                                lapack_complex_double* z, lapack_int ldz,
                                lapack_complex_double* work,
                                lapack_int lwork );

lapack_int LAPACKE_sgglse_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int p, float* a, lapack_int lda,
                                float* b, lapack_int ldb, float* c, float* d,
                                float* x, float* work, lapack_int lwork );
lapack_int LAPACKE_dgglse_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int p, double* a, lapack_int lda,
                                double* b, lapack_int ldb, double* c, double* d,
                                double* x, double* work, lapack_int lwork );
lapack_int LAPACKE_cgglse_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int p, lapack_complex_float* a,
                                lapack_int lda, lapack_complex_float* b,
                                lapack_int ldb, lapack_complex_float* c,
                                lapack_complex_float* d,
                                lapack_complex_float* x,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zgglse_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int p, lapack_complex_double* a,
                                lapack_int lda, lapack_complex_double* b,
                                lapack_int ldb, lapack_complex_double* c,
                                lapack_complex_double* d,
                                lapack_complex_double* x,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_sggqrf_work( int matrix_layout, lapack_int n, lapack_int m,
                                lapack_int p, float* a, lapack_int lda,
                                float* taua, float* b, lapack_int ldb,
                                float* taub, float* work, lapack_int lwork );
lapack_int LAPACKE_dggqrf_work( int matrix_layout, lapack_int n, lapack_int m,
                                lapack_int p, double* a, lapack_int lda,
                                double* taua, double* b, lapack_int ldb,
                                double* taub, double* work, lapack_int lwork );
lapack_int LAPACKE_cggqrf_work( int matrix_layout, lapack_int n, lapack_int m,
                                lapack_int p, lapack_complex_float* a,
                                lapack_int lda, lapack_complex_float* taua,
                                lapack_complex_float* b, lapack_int ldb,
                                lapack_complex_float* taub,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zggqrf_work( int matrix_layout, lapack_int n, lapack_int m,
                                lapack_int p, lapack_complex_double* a,
                                lapack_int lda, lapack_complex_double* taua,
                                lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* taub,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_sggrqf_work( int matrix_layout, lapack_int m, lapack_int p,
                                lapack_int n, float* a, lapack_int lda,
                                float* taua, float* b, lapack_int ldb,
                                float* taub, float* work, lapack_int lwork );
lapack_int LAPACKE_dggrqf_work( int matrix_layout, lapack_int m, lapack_int p,
                                lapack_int n, double* a, lapack_int lda,
                                double* taua, double* b, lapack_int ldb,
                                double* taub, double* work, lapack_int lwork );
lapack_int LAPACKE_cggrqf_work( int matrix_layout, lapack_int m, lapack_int p,
                                lapack_int n, lapack_complex_float* a,
                                lapack_int lda, lapack_complex_float* taua,
                                lapack_complex_float* b, lapack_int ldb,
                                lapack_complex_float* taub,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zggrqf_work( int matrix_layout, lapack_int m, lapack_int p,
                                lapack_int n, lapack_complex_double* a,
                                lapack_int lda, lapack_complex_double* taua,
                                lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* taub,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_sggsvd_work( int matrix_layout, char jobu, char jobv,
                                char jobq, lapack_int m, lapack_int n,
                                lapack_int p, lapack_int* k, lapack_int* l,
                                float* a, lapack_int lda, float* b,
                                lapack_int ldb, float* alpha, float* beta,
                                float* u, lapack_int ldu, float* v,
                                lapack_int ldv, float* q, lapack_int ldq,
                                float* work, lapack_int* iwork );
lapack_int LAPACKE_dggsvd_work( int matrix_layout, char jobu, char jobv,
                                char jobq, lapack_int m, lapack_int n,
                                lapack_int p, lapack_int* k, lapack_int* l,
                                double* a, lapack_int lda, double* b,
                                lapack_int ldb, double* alpha, double* beta,
                                double* u, lapack_int ldu, double* v,
                                lapack_int ldv, double* q, lapack_int ldq,
                                double* work, lapack_int* iwork );
lapack_int LAPACKE_cggsvd_work( int matrix_layout, char jobu, char jobv,
                                char jobq, lapack_int m, lapack_int n,
                                lapack_int p, lapack_int* k, lapack_int* l,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_complex_float* b, lapack_int ldb,
                                float* alpha, float* beta,
                                lapack_complex_float* u, lapack_int ldu,
                                lapack_complex_float* v, lapack_int ldv,
                                lapack_complex_float* q, lapack_int ldq,
                                lapack_complex_float* work, float* rwork,
                                lapack_int* iwork );
lapack_int LAPACKE_zggsvd_work( int matrix_layout, char jobu, char jobv,
                                char jobq, lapack_int m, lapack_int n,
                                lapack_int p, lapack_int* k, lapack_int* l,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* b, lapack_int ldb,
                                double* alpha, double* beta,
                                lapack_complex_double* u, lapack_int ldu,
                                lapack_complex_double* v, lapack_int ldv,
                                lapack_complex_double* q, lapack_int ldq,
                                lapack_complex_double* work, double* rwork,
                                lapack_int* iwork );

lapack_int LAPACKE_sggsvd3_work( int matrix_layout, char jobu, char jobv,
                                 char jobq, lapack_int m, lapack_int n,
                                 lapack_int p, lapack_int* k, lapack_int* l,
                                 float* a, lapack_int lda, float* b,
                                 lapack_int ldb, float* alpha, float* beta,
                                 float* u, lapack_int ldu, float* v,
                                 lapack_int ldv, float* q, lapack_int ldq,
                                 float* work, lapack_int lwork,
                                 lapack_int* iwork );
lapack_int LAPACKE_dggsvd3_work( int matrix_layout, char jobu, char jobv,
                                 char jobq, lapack_int m, lapack_int n,
                                 lapack_int p, lapack_int* k, lapack_int* l,
                                 double* a, lapack_int lda, double* b,
                                 lapack_int ldb, double* alpha, double* beta,
                                 double* u, lapack_int ldu, double* v,
                                 lapack_int ldv, double* q, lapack_int ldq,
                                 double* work, lapack_int lwork,
                                 lapack_int* iwork );
lapack_int LAPACKE_cggsvd3_work( int matrix_layout, char jobu, char jobv,
                                 char jobq, lapack_int m, lapack_int n,
                                 lapack_int p, lapack_int* k, lapack_int* l,
                                 lapack_complex_float* a, lapack_int lda,
                                 lapack_complex_float* b, lapack_int ldb,
                                 float* alpha, float* beta,
                                 lapack_complex_float* u, lapack_int ldu,
                                 lapack_complex_float* v, lapack_int ldv,
                                 lapack_complex_float* q, lapack_int ldq,
                                 lapack_complex_float* work, lapack_int lwork,
                                 float* rwork, lapack_int* iwork );
lapack_int LAPACKE_zggsvd3_work( int matrix_layout, char jobu, char jobv,
                                 char jobq, lapack_int m, lapack_int n,
                                 lapack_int p, lapack_int* k, lapack_int* l,
                                 lapack_complex_double* a, lapack_int lda,
                                 lapack_complex_double* b, lapack_int ldb,
                                 double* alpha, double* beta,
                                 lapack_complex_double* u, lapack_int ldu,
                                 lapack_complex_double* v, lapack_int ldv,
                                 lapack_complex_double* q, lapack_int ldq,
                                 lapack_complex_double* work, lapack_int lwork,
                                 double* rwork, lapack_int* iwork );

lapack_int LAPACKE_sggsvp_work( int matrix_layout, char jobu, char jobv,
                                char jobq, lapack_int m, lapack_int p,
                                lapack_int n, float* a, lapack_int lda,
                                float* b, lapack_int ldb, float tola,
                                float tolb, lapack_int* k, lapack_int* l,
                                float* u, lapack_int ldu, float* v,
                                lapack_int ldv, float* q, lapack_int ldq,
                                lapack_int* iwork, float* tau, float* work );
lapack_int LAPACKE_dggsvp_work( int matrix_layout, char jobu, char jobv,
                                char jobq, lapack_int m, lapack_int p,
                                lapack_int n, double* a, lapack_int lda,
                                double* b, lapack_int ldb, double tola,
                                double tolb, lapack_int* k, lapack_int* l,
                                double* u, lapack_int ldu, double* v,
                                lapack_int ldv, double* q, lapack_int ldq,
                                lapack_int* iwork, double* tau, double* work );
lapack_int LAPACKE_cggsvp_work( int matrix_layout, char jobu, char jobv,
                                char jobq, lapack_int m, lapack_int p,
                                lapack_int n, lapack_complex_float* a,
                                lapack_int lda, lapack_complex_float* b,
                                lapack_int ldb, float tola, float tolb,
                                lapack_int* k, lapack_int* l,
                                lapack_complex_float* u, lapack_int ldu,
                                lapack_complex_float* v, lapack_int ldv,
                                lapack_complex_float* q, lapack_int ldq,
                                lapack_int* iwork, float* rwork,
                                lapack_complex_float* tau,
                                lapack_complex_float* work );
lapack_int LAPACKE_zggsvp_work( int matrix_layout, char jobu, char jobv,
                                char jobq, lapack_int m, lapack_int p,
                                lapack_int n, lapack_complex_double* a,
                                lapack_int lda, lapack_complex_double* b,
                                lapack_int ldb, double tola, double tolb,
                                lapack_int* k, lapack_int* l,
                                lapack_complex_double* u, lapack_int ldu,
                                lapack_complex_double* v, lapack_int ldv,
                                lapack_complex_double* q, lapack_int ldq,
                                lapack_int* iwork, double* rwork,
                                lapack_complex_double* tau,
                                lapack_complex_double* work );

lapack_int LAPACKE_sggsvp3_work( int matrix_layout, char jobu, char jobv,
                                 char jobq, lapack_int m, lapack_int p,
                                 lapack_int n, float* a, lapack_int lda,
                                 float* b, lapack_int ldb, float tola,
                                 float tolb, lapack_int* k, lapack_int* l,
                                 float* u, lapack_int ldu, float* v,
                                 lapack_int ldv, float* q, lapack_int ldq,
                                 lapack_int* iwork, float* tau,
                                 float* work, lapack_int lwork );
lapack_int LAPACKE_dggsvp3_work( int matrix_layout, char jobu, char jobv,
                                 char jobq, lapack_int m, lapack_int p,
                                 lapack_int n, double* a, lapack_int lda,
                                 double* b, lapack_int ldb, double tola,
                                 double tolb, lapack_int* k, lapack_int* l,
                                 double* u, lapack_int ldu, double* v,
                                 lapack_int ldv, double* q, lapack_int ldq,
                                 lapack_int* iwork, double* tau, double* work,
                                 lapack_int lwork );
lapack_int LAPACKE_cggsvp3_work( int matrix_layout, char jobu, char jobv,
                                 char jobq, lapack_int m, lapack_int p,
                                 lapack_int n, lapack_complex_float* a,
                                 lapack_int lda, lapack_complex_float* b,
                                 lapack_int ldb, float tola, float tolb,
                                 lapack_int* k, lapack_int* l,
                                 lapack_complex_float* u, lapack_int ldu,
                                 lapack_complex_float* v, lapack_int ldv,
                                 lapack_complex_float* q, lapack_int ldq,
                                 lapack_int* iwork, float* rwork,
                                 lapack_complex_float* tau,
                                 lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zggsvp3_work( int matrix_layout, char jobu, char jobv,
                                 char jobq, lapack_int m, lapack_int p,
                                 lapack_int n, lapack_complex_double* a,
                                 lapack_int lda, lapack_complex_double* b,
                                 lapack_int ldb, double tola, double tolb,
                                 lapack_int* k, lapack_int* l,
                                 lapack_complex_double* u, lapack_int ldu,
                                 lapack_complex_double* v, lapack_int ldv,
                                 lapack_complex_double* q, lapack_int ldq,
                                 lapack_int* iwork, double* rwork,
                                 lapack_complex_double* tau,
                                 lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_sgtcon_work( char norm, lapack_int n, const float* dl,
                                const float* d, const float* du,
                                const float* du2, const lapack_int* ipiv,
                                float anorm, float* rcond, float* work,
                                lapack_int* iwork );
lapack_int LAPACKE_dgtcon_work( char norm, lapack_int n, const double* dl,
                                const double* d, const double* du,
                                const double* du2, const lapack_int* ipiv,
                                double anorm, double* rcond, double* work,
                                lapack_int* iwork );
lapack_int LAPACKE_cgtcon_work( char norm, lapack_int n,
                                const lapack_complex_float* dl,
                                const lapack_complex_float* d,
                                const lapack_complex_float* du,
                                const lapack_complex_float* du2,
                                const lapack_int* ipiv, float anorm,
                                float* rcond, lapack_complex_float* work );
lapack_int LAPACKE_zgtcon_work( char norm, lapack_int n,
                                const lapack_complex_double* dl,
                                const lapack_complex_double* d,
                                const lapack_complex_double* du,
                                const lapack_complex_double* du2,
                                const lapack_int* ipiv, double anorm,
                                double* rcond, lapack_complex_double* work );

lapack_int LAPACKE_sgtrfs_work( int matrix_layout, char trans, lapack_int n,
                                lapack_int nrhs, const float* dl,
                                const float* d, const float* du,
                                const float* dlf, const float* df,
                                const float* duf, const float* du2,
                                const lapack_int* ipiv, const float* b,
                                lapack_int ldb, float* x, lapack_int ldx,
                                float* ferr, float* berr, float* work,
                                lapack_int* iwork );
lapack_int LAPACKE_dgtrfs_work( int matrix_layout, char trans, lapack_int n,
                                lapack_int nrhs, const double* dl,
                                const double* d, const double* du,
                                const double* dlf, const double* df,
                                const double* duf, const double* du2,
                                const lapack_int* ipiv, const double* b,
                                lapack_int ldb, double* x, lapack_int ldx,
                                double* ferr, double* berr, double* work,
                                lapack_int* iwork );
lapack_int LAPACKE_cgtrfs_work( int matrix_layout, char trans, lapack_int n,
                                lapack_int nrhs, const lapack_complex_float* dl,
                                const lapack_complex_float* d,
                                const lapack_complex_float* du,
                                const lapack_complex_float* dlf,
                                const lapack_complex_float* df,
                                const lapack_complex_float* duf,
                                const lapack_complex_float* du2,
                                const lapack_int* ipiv,
                                const lapack_complex_float* b, lapack_int ldb,
                                lapack_complex_float* x, lapack_int ldx,
                                float* ferr, float* berr,
                                lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_zgtrfs_work( int matrix_layout, char trans, lapack_int n,
                                lapack_int nrhs,
                                const lapack_complex_double* dl,
                                const lapack_complex_double* d,
                                const lapack_complex_double* du,
                                const lapack_complex_double* dlf,
                                const lapack_complex_double* df,
                                const lapack_complex_double* duf,
                                const lapack_complex_double* du2,
                                const lapack_int* ipiv,
                                const lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* x, lapack_int ldx,
                                double* ferr, double* berr,
                                lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_sgtsv_work( int matrix_layout, lapack_int n, lapack_int nrhs,
                               float* dl, float* d, float* du, float* b,
                               lapack_int ldb );
lapack_int LAPACKE_dgtsv_work( int matrix_layout, lapack_int n, lapack_int nrhs,
                               double* dl, double* d, double* du, double* b,
                               lapack_int ldb );
lapack_int LAPACKE_cgtsv_work( int matrix_layout, lapack_int n, lapack_int nrhs,
                               lapack_complex_float* dl,
                               lapack_complex_float* d,
                               lapack_complex_float* du,
                               lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zgtsv_work( int matrix_layout, lapack_int n, lapack_int nrhs,
                               lapack_complex_double* dl,
                               lapack_complex_double* d,
                               lapack_complex_double* du,
                               lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_sgtsvx_work( int matrix_layout, char fact, char trans,
                                lapack_int n, lapack_int nrhs, const float* dl,
                                const float* d, const float* du, float* dlf,
                                float* df, float* duf, float* du2,
                                lapack_int* ipiv, const float* b,
                                lapack_int ldb, float* x, lapack_int ldx,
                                float* rcond, float* ferr, float* berr,
                                float* work, lapack_int* iwork );
lapack_int LAPACKE_dgtsvx_work( int matrix_layout, char fact, char trans,
                                lapack_int n, lapack_int nrhs, const double* dl,
                                const double* d, const double* du, double* dlf,
                                double* df, double* duf, double* du2,
                                lapack_int* ipiv, const double* b,
                                lapack_int ldb, double* x, lapack_int ldx,
                                double* rcond, double* ferr, double* berr,
                                double* work, lapack_int* iwork );
lapack_int LAPACKE_cgtsvx_work( int matrix_layout, char fact, char trans,
                                lapack_int n, lapack_int nrhs,
                                const lapack_complex_float* dl,
                                const lapack_complex_float* d,
                                const lapack_complex_float* du,
                                lapack_complex_float* dlf,
                                lapack_complex_float* df,
                                lapack_complex_float* duf,
                                lapack_complex_float* du2, lapack_int* ipiv,
                                const lapack_complex_float* b, lapack_int ldb,
                                lapack_complex_float* x, lapack_int ldx,
                                float* rcond, float* ferr, float* berr,
                                lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_zgtsvx_work( int matrix_layout, char fact, char trans,
                                lapack_int n, lapack_int nrhs,
                                const lapack_complex_double* dl,
                                const lapack_complex_double* d,
                                const lapack_complex_double* du,
                                lapack_complex_double* dlf,
                                lapack_complex_double* df,
                                lapack_complex_double* duf,
                                lapack_complex_double* du2, lapack_int* ipiv,
                                const lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* x, lapack_int ldx,
                                double* rcond, double* ferr, double* berr,
                                lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_sgttrf_work( lapack_int n, float* dl, float* d, float* du,
                                float* du2, lapack_int* ipiv );
lapack_int LAPACKE_dgttrf_work( lapack_int n, double* dl, double* d, double* du,
                                double* du2, lapack_int* ipiv );
lapack_int LAPACKE_cgttrf_work( lapack_int n, lapack_complex_float* dl,
                                lapack_complex_float* d,
                                lapack_complex_float* du,
                                lapack_complex_float* du2, lapack_int* ipiv );
lapack_int LAPACKE_zgttrf_work( lapack_int n, lapack_complex_double* dl,
                                lapack_complex_double* d,
                                lapack_complex_double* du,
                                lapack_complex_double* du2, lapack_int* ipiv );

lapack_int LAPACKE_sgttrs_work( int matrix_layout, char trans, lapack_int n,
                                lapack_int nrhs, const float* dl,
                                const float* d, const float* du,
                                const float* du2, const lapack_int* ipiv,
                                float* b, lapack_int ldb );
lapack_int LAPACKE_dgttrs_work( int matrix_layout, char trans, lapack_int n,
                                lapack_int nrhs, const double* dl,
                                const double* d, const double* du,
                                const double* du2, const lapack_int* ipiv,
                                double* b, lapack_int ldb );
lapack_int LAPACKE_cgttrs_work( int matrix_layout, char trans, lapack_int n,
                                lapack_int nrhs, const lapack_complex_float* dl,
                                const lapack_complex_float* d,
                                const lapack_complex_float* du,
                                const lapack_complex_float* du2,
                                const lapack_int* ipiv, lapack_complex_float* b,
                                lapack_int ldb );
lapack_int LAPACKE_zgttrs_work( int matrix_layout, char trans, lapack_int n,
                                lapack_int nrhs,
                                const lapack_complex_double* dl,
                                const lapack_complex_double* d,
                                const lapack_complex_double* du,
                                const lapack_complex_double* du2,
                                const lapack_int* ipiv,
                                lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_chbev_work( int matrix_layout, char jobz, char uplo,
                               lapack_int n, lapack_int kd,
                               lapack_complex_float* ab, lapack_int ldab,
                               float* w, lapack_complex_float* z,
                               lapack_int ldz, lapack_complex_float* work,
                               float* rwork );
lapack_int LAPACKE_zhbev_work( int matrix_layout, char jobz, char uplo,
                               lapack_int n, lapack_int kd,
                               lapack_complex_double* ab, lapack_int ldab,
                               double* w, lapack_complex_double* z,
                               lapack_int ldz, lapack_complex_double* work,
                               double* rwork );

lapack_int LAPACKE_chbevd_work( int matrix_layout, char jobz, char uplo,
                                lapack_int n, lapack_int kd,
                                lapack_complex_float* ab, lapack_int ldab,
                                float* w, lapack_complex_float* z,
                                lapack_int ldz, lapack_complex_float* work,
                                lapack_int lwork, float* rwork,
                                lapack_int lrwork, lapack_int* iwork,
                                lapack_int liwork );
lapack_int LAPACKE_zhbevd_work( int matrix_layout, char jobz, char uplo,
                                lapack_int n, lapack_int kd,
                                lapack_complex_double* ab, lapack_int ldab,
                                double* w, lapack_complex_double* z,
                                lapack_int ldz, lapack_complex_double* work,
                                lapack_int lwork, double* rwork,
                                lapack_int lrwork, lapack_int* iwork,
                                lapack_int liwork );

lapack_int LAPACKE_chbevx_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n, lapack_int kd,
                                lapack_complex_float* ab, lapack_int ldab,
                                lapack_complex_float* q, lapack_int ldq,
                                float vl, float vu, lapack_int il,
                                lapack_int iu, float abstol, lapack_int* m,
                                float* w, lapack_complex_float* z,
                                lapack_int ldz, lapack_complex_float* work,
                                float* rwork, lapack_int* iwork,
                                lapack_int* ifail );
lapack_int LAPACKE_zhbevx_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n, lapack_int kd,
                                lapack_complex_double* ab, lapack_int ldab,
                                lapack_complex_double* q, lapack_int ldq,
                                double vl, double vu, lapack_int il,
                                lapack_int iu, double abstol, lapack_int* m,
                                double* w, lapack_complex_double* z,
                                lapack_int ldz, lapack_complex_double* work,
                                double* rwork, lapack_int* iwork,
                                lapack_int* ifail );

lapack_int LAPACKE_chbgst_work( int matrix_layout, char vect, char uplo,
                                lapack_int n, lapack_int ka, lapack_int kb,
                                lapack_complex_float* ab, lapack_int ldab,
                                const lapack_complex_float* bb, lapack_int ldbb,
                                lapack_complex_float* x, lapack_int ldx,
                                lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_zhbgst_work( int matrix_layout, char vect, char uplo,
                                lapack_int n, lapack_int ka, lapack_int kb,
                                lapack_complex_double* ab, lapack_int ldab,
                                const lapack_complex_double* bb,
                                lapack_int ldbb, lapack_complex_double* x,
                                lapack_int ldx, lapack_complex_double* work,
                                double* rwork );

lapack_int LAPACKE_chbgv_work( int matrix_layout, char jobz, char uplo,
                               lapack_int n, lapack_int ka, lapack_int kb,
                               lapack_complex_float* ab, lapack_int ldab,
                               lapack_complex_float* bb, lapack_int ldbb,
                               float* w, lapack_complex_float* z,
                               lapack_int ldz, lapack_complex_float* work,
                               float* rwork );
lapack_int LAPACKE_zhbgv_work( int matrix_layout, char jobz, char uplo,
                               lapack_int n, lapack_int ka, lapack_int kb,
                               lapack_complex_double* ab, lapack_int ldab,
                               lapack_complex_double* bb, lapack_int ldbb,
                               double* w, lapack_complex_double* z,
                               lapack_int ldz, lapack_complex_double* work,
                               double* rwork );

lapack_int LAPACKE_chbgvd_work( int matrix_layout, char jobz, char uplo,
                                lapack_int n, lapack_int ka, lapack_int kb,
                                lapack_complex_float* ab, lapack_int ldab,
                                lapack_complex_float* bb, lapack_int ldbb,
                                float* w, lapack_complex_float* z,
                                lapack_int ldz, lapack_complex_float* work,
                                lapack_int lwork, float* rwork,
                                lapack_int lrwork, lapack_int* iwork,
                                lapack_int liwork );
lapack_int LAPACKE_zhbgvd_work( int matrix_layout, char jobz, char uplo,
                                lapack_int n, lapack_int ka, lapack_int kb,
                                lapack_complex_double* ab, lapack_int ldab,
                                lapack_complex_double* bb, lapack_int ldbb,
                                double* w, lapack_complex_double* z,
                                lapack_int ldz, lapack_complex_double* work,
                                lapack_int lwork, double* rwork,
                                lapack_int lrwork, lapack_int* iwork,
                                lapack_int liwork );

lapack_int LAPACKE_chbgvx_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n, lapack_int ka,
                                lapack_int kb, lapack_complex_float* ab,
                                lapack_int ldab, lapack_complex_float* bb,
                                lapack_int ldbb, lapack_complex_float* q,
                                lapack_int ldq, float vl, float vu,
                                lapack_int il, lapack_int iu, float abstol,
                                lapack_int* m, float* w,
                                lapack_complex_float* z, lapack_int ldz,
                                lapack_complex_float* work, float* rwork,
                                lapack_int* iwork, lapack_int* ifail );
lapack_int LAPACKE_zhbgvx_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n, lapack_int ka,
                                lapack_int kb, lapack_complex_double* ab,
                                lapack_int ldab, lapack_complex_double* bb,
                                lapack_int ldbb, lapack_complex_double* q,
                                lapack_int ldq, double vl, double vu,
                                lapack_int il, lapack_int iu, double abstol,
                                lapack_int* m, double* w,
                                lapack_complex_double* z, lapack_int ldz,
                                lapack_complex_double* work, double* rwork,
                                lapack_int* iwork, lapack_int* ifail );

lapack_int LAPACKE_chbtrd_work( int matrix_layout, char vect, char uplo,
                                lapack_int n, lapack_int kd,
                                lapack_complex_float* ab, lapack_int ldab,
                                float* d, float* e, lapack_complex_float* q,
                                lapack_int ldq, lapack_complex_float* work );
lapack_int LAPACKE_zhbtrd_work( int matrix_layout, char vect, char uplo,
                                lapack_int n, lapack_int kd,
                                lapack_complex_double* ab, lapack_int ldab,
                                double* d, double* e, lapack_complex_double* q,
                                lapack_int ldq, lapack_complex_double* work );

lapack_int LAPACKE_checon_work( int matrix_layout, char uplo, lapack_int n,
                                const lapack_complex_float* a, lapack_int lda,
                                const lapack_int* ipiv, float anorm,
                                float* rcond, lapack_complex_float* work );
lapack_int LAPACKE_zhecon_work( int matrix_layout, char uplo, lapack_int n,
                                const lapack_complex_double* a, lapack_int lda,
                                const lapack_int* ipiv, double anorm,
                                double* rcond, lapack_complex_double* work );

lapack_int LAPACKE_cheequb_work( int matrix_layout, char uplo, lapack_int n,
                                 const lapack_complex_float* a, lapack_int lda,
                                 float* s, float* scond, float* amax,
                                 lapack_complex_float* work );
lapack_int LAPACKE_zheequb_work( int matrix_layout, char uplo, lapack_int n,
                                 const lapack_complex_double* a, lapack_int lda,
                                 double* s, double* scond, double* amax,
                                 lapack_complex_double* work );

lapack_int LAPACKE_cheev_work( int matrix_layout, char jobz, char uplo,
                               lapack_int n, lapack_complex_float* a,
                               lapack_int lda, float* w,
                               lapack_complex_float* work, lapack_int lwork,
                               float* rwork );
lapack_int LAPACKE_zheev_work( int matrix_layout, char jobz, char uplo,
                               lapack_int n, lapack_complex_double* a,
                               lapack_int lda, double* w,
                               lapack_complex_double* work, lapack_int lwork,
                               double* rwork );

lapack_int LAPACKE_cheevd_work( int matrix_layout, char jobz, char uplo,
                                lapack_int n, lapack_complex_float* a,
                                lapack_int lda, float* w,
                                lapack_complex_float* work, lapack_int lwork,
                                float* rwork, lapack_int lrwork,
                                lapack_int* iwork, lapack_int liwork );
lapack_int LAPACKE_zheevd_work( int matrix_layout, char jobz, char uplo,
                                lapack_int n, lapack_complex_double* a,
                                lapack_int lda, double* w,
                                lapack_complex_double* work, lapack_int lwork,
                                double* rwork, lapack_int lrwork,
                                lapack_int* iwork, lapack_int liwork );

lapack_int LAPACKE_cheevr_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                float vl, float vu, lapack_int il,
                                lapack_int iu, float abstol, lapack_int* m,
                                float* w, lapack_complex_float* z,
                                lapack_int ldz, lapack_int* isuppz,
                                lapack_complex_float* work, lapack_int lwork,
                                float* rwork, lapack_int lrwork,
                                lapack_int* iwork, lapack_int liwork );
lapack_int LAPACKE_zheevr_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                double vl, double vu, lapack_int il,
                                lapack_int iu, double abstol, lapack_int* m,
                                double* w, lapack_complex_double* z,
                                lapack_int ldz, lapack_int* isuppz,
                                lapack_complex_double* work, lapack_int lwork,
                                double* rwork, lapack_int lrwork,
                                lapack_int* iwork, lapack_int liwork );

lapack_int LAPACKE_cheevx_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                float vl, float vu, lapack_int il,
                                lapack_int iu, float abstol, lapack_int* m,
                                float* w, lapack_complex_float* z,
                                lapack_int ldz, lapack_complex_float* work,
                                lapack_int lwork, float* rwork,
                                lapack_int* iwork, lapack_int* ifail );
lapack_int LAPACKE_zheevx_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                double vl, double vu, lapack_int il,
                                lapack_int iu, double abstol, lapack_int* m,
                                double* w, lapack_complex_double* z,
                                lapack_int ldz, lapack_complex_double* work,
                                lapack_int lwork, double* rwork,
                                lapack_int* iwork, lapack_int* ifail );

lapack_int LAPACKE_chegst_work( int matrix_layout, lapack_int itype, char uplo,
                                lapack_int n, lapack_complex_float* a,
                                lapack_int lda, const lapack_complex_float* b,
                                lapack_int ldb );
lapack_int LAPACKE_zhegst_work( int matrix_layout, lapack_int itype, char uplo,
                                lapack_int n, lapack_complex_double* a,
                                lapack_int lda, const lapack_complex_double* b,
                                lapack_int ldb );

lapack_int LAPACKE_chegv_work( int matrix_layout, lapack_int itype, char jobz,
                               char uplo, lapack_int n, lapack_complex_float* a,
                               lapack_int lda, lapack_complex_float* b,
                               lapack_int ldb, float* w,
                               lapack_complex_float* work, lapack_int lwork,
                               float* rwork );
lapack_int LAPACKE_zhegv_work( int matrix_layout, lapack_int itype, char jobz,
                               char uplo, lapack_int n,
                               lapack_complex_double* a, lapack_int lda,
                               lapack_complex_double* b, lapack_int ldb,
                               double* w, lapack_complex_double* work,
                               lapack_int lwork, double* rwork );

lapack_int LAPACKE_chegvd_work( int matrix_layout, lapack_int itype, char jobz,
                                char uplo, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_complex_float* b, lapack_int ldb,
                                float* w, lapack_complex_float* work,
                                lapack_int lwork, float* rwork,
                                lapack_int lrwork, lapack_int* iwork,
                                lapack_int liwork );
lapack_int LAPACKE_zhegvd_work( int matrix_layout, lapack_int itype, char jobz,
                                char uplo, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* b, lapack_int ldb,
                                double* w, lapack_complex_double* work,
                                lapack_int lwork, double* rwork,
                                lapack_int lrwork, lapack_int* iwork,
                                lapack_int liwork );

lapack_int LAPACKE_chegvx_work( int matrix_layout, lapack_int itype, char jobz,
                                char range, char uplo, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_complex_float* b, lapack_int ldb,
                                float vl, float vu, lapack_int il,
                                lapack_int iu, float abstol, lapack_int* m,
                                float* w, lapack_complex_float* z,
                                lapack_int ldz, lapack_complex_float* work,
                                lapack_int lwork, float* rwork,
                                lapack_int* iwork, lapack_int* ifail );
lapack_int LAPACKE_zhegvx_work( int matrix_layout, lapack_int itype, char jobz,
                                char range, char uplo, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* b, lapack_int ldb,
                                double vl, double vu, lapack_int il,
                                lapack_int iu, double abstol, lapack_int* m,
                                double* w, lapack_complex_double* z,
                                lapack_int ldz, lapack_complex_double* work,
                                lapack_int lwork, double* rwork,
                                lapack_int* iwork, lapack_int* ifail );

lapack_int LAPACKE_cherfs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const lapack_complex_float* a,
                                lapack_int lda, const lapack_complex_float* af,
                                lapack_int ldaf, const lapack_int* ipiv,
                                const lapack_complex_float* b, lapack_int ldb,
                                lapack_complex_float* x, lapack_int ldx,
                                float* ferr, float* berr,
                                lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_zherfs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const lapack_complex_double* a,
                                lapack_int lda, const lapack_complex_double* af,
                                lapack_int ldaf, const lapack_int* ipiv,
                                const lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* x, lapack_int ldx,
                                double* ferr, double* berr,
                                lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_cherfsx_work( int matrix_layout, char uplo, char equed,
                                 lapack_int n, lapack_int nrhs,
                                 const lapack_complex_float* a, lapack_int lda,
                                 const lapack_complex_float* af,
                                 lapack_int ldaf, const lapack_int* ipiv,
                                 const float* s, const lapack_complex_float* b,
                                 lapack_int ldb, lapack_complex_float* x,
                                 lapack_int ldx, float* rcond, float* berr,
                                 lapack_int n_err_bnds, float* err_bnds_norm,
                                 float* err_bnds_comp, lapack_int nparams,
                                 float* params, lapack_complex_float* work,
                                 float* rwork );
lapack_int LAPACKE_zherfsx_work( int matrix_layout, char uplo, char equed,
                                 lapack_int n, lapack_int nrhs,
                                 const lapack_complex_double* a, lapack_int lda,
                                 const lapack_complex_double* af,
                                 lapack_int ldaf, const lapack_int* ipiv,
                                 const double* s,
                                 const lapack_complex_double* b, lapack_int ldb,
                                 lapack_complex_double* x, lapack_int ldx,
                                 double* rcond, double* berr,
                                 lapack_int n_err_bnds, double* err_bnds_norm,
                                 double* err_bnds_comp, lapack_int nparams,
                                 double* params, lapack_complex_double* work,
                                 double* rwork );

lapack_int LAPACKE_chesv_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, lapack_complex_float* a,
                               lapack_int lda, lapack_int* ipiv,
                               lapack_complex_float* b, lapack_int ldb,
                               lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zhesv_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, lapack_complex_double* a,
                               lapack_int lda, lapack_int* ipiv,
                               lapack_complex_double* b, lapack_int ldb,
                               lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_chesvx_work( int matrix_layout, char fact, char uplo,
                                lapack_int n, lapack_int nrhs,
                                const lapack_complex_float* a, lapack_int lda,
                                lapack_complex_float* af, lapack_int ldaf,
                                lapack_int* ipiv, const lapack_complex_float* b,
                                lapack_int ldb, lapack_complex_float* x,
                                lapack_int ldx, float* rcond, float* ferr,
                                float* berr, lapack_complex_float* work,
                                lapack_int lwork, float* rwork );
lapack_int LAPACKE_zhesvx_work( int matrix_layout, char fact, char uplo,
                                lapack_int n, lapack_int nrhs,
                                const lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* af, lapack_int ldaf,
                                lapack_int* ipiv,
                                const lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* x, lapack_int ldx,
                                double* rcond, double* ferr, double* berr,
                                lapack_complex_double* work, lapack_int lwork,
                                double* rwork );

lapack_int LAPACKE_chesvxx_work( int matrix_layout, char fact, char uplo,
                                 lapack_int n, lapack_int nrhs,
                                 lapack_complex_float* a, lapack_int lda,
                                 lapack_complex_float* af, lapack_int ldaf,
                                 lapack_int* ipiv, char* equed, float* s,
                                 lapack_complex_float* b, lapack_int ldb,
                                 lapack_complex_float* x, lapack_int ldx,
                                 float* rcond, float* rpvgrw, float* berr,
                                 lapack_int n_err_bnds, float* err_bnds_norm,
                                 float* err_bnds_comp, lapack_int nparams,
                                 float* params, lapack_complex_float* work,
                                 float* rwork );
lapack_int LAPACKE_zhesvxx_work( int matrix_layout, char fact, char uplo,
                                 lapack_int n, lapack_int nrhs,
                                 lapack_complex_double* a, lapack_int lda,
                                 lapack_complex_double* af, lapack_int ldaf,
                                 lapack_int* ipiv, char* equed, double* s,
                                 lapack_complex_double* b, lapack_int ldb,
                                 lapack_complex_double* x, lapack_int ldx,
                                 double* rcond, double* rpvgrw, double* berr,
                                 lapack_int n_err_bnds, double* err_bnds_norm,
                                 double* err_bnds_comp, lapack_int nparams,
                                 double* params, lapack_complex_double* work,
                                 double* rwork );

lapack_int LAPACKE_chetrd_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                float* d, float* e, lapack_complex_float* tau,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zhetrd_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                double* d, double* e,
                                lapack_complex_double* tau,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_chetrf_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_int* ipiv, lapack_complex_float* work,
                                lapack_int lwork );
lapack_int LAPACKE_zhetrf_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_int* ipiv, lapack_complex_double* work,
                                lapack_int lwork );

lapack_int LAPACKE_chetri_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                const lapack_int* ipiv,
                                lapack_complex_float* work );
lapack_int LAPACKE_zhetri_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                const lapack_int* ipiv,
                                lapack_complex_double* work );

lapack_int LAPACKE_chetrs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const lapack_complex_float* a,
                                lapack_int lda, const lapack_int* ipiv,
                                lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zhetrs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const lapack_complex_double* a,
                                lapack_int lda, const lapack_int* ipiv,
                                lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_chfrk_work( int matrix_layout, char transr, char uplo,
                               char trans, lapack_int n, lapack_int k,
                               float alpha, const lapack_complex_float* a,
                               lapack_int lda, float beta,
                               lapack_complex_float* c );
lapack_int LAPACKE_zhfrk_work( int matrix_layout, char transr, char uplo,
                               char trans, lapack_int n, lapack_int k,
                               double alpha, const lapack_complex_double* a,
                               lapack_int lda, double beta,
                               lapack_complex_double* c );

lapack_int LAPACKE_shgeqz_work( int matrix_layout, char job, char compq,
                                char compz, lapack_int n, lapack_int ilo,
                                lapack_int ihi, float* h, lapack_int ldh,
                                float* t, lapack_int ldt, float* alphar,
                                float* alphai, float* beta, float* q,
                                lapack_int ldq, float* z, lapack_int ldz,
                                float* work, lapack_int lwork );
lapack_int LAPACKE_dhgeqz_work( int matrix_layout, char job, char compq,
                                char compz, lapack_int n, lapack_int ilo,
                                lapack_int ihi, double* h, lapack_int ldh,
                                double* t, lapack_int ldt, double* alphar,
                                double* alphai, double* beta, double* q,
                                lapack_int ldq, double* z, lapack_int ldz,
                                double* work, lapack_int lwork );
lapack_int LAPACKE_chgeqz_work( int matrix_layout, char job, char compq,
                                char compz, lapack_int n, lapack_int ilo,
                                lapack_int ihi, lapack_complex_float* h,
                                lapack_int ldh, lapack_complex_float* t,
                                lapack_int ldt, lapack_complex_float* alpha,
                                lapack_complex_float* beta,
                                lapack_complex_float* q, lapack_int ldq,
                                lapack_complex_float* z, lapack_int ldz,
                                lapack_complex_float* work, lapack_int lwork,
                                float* rwork );
lapack_int LAPACKE_zhgeqz_work( int matrix_layout, char job, char compq,
                                char compz, lapack_int n, lapack_int ilo,
                                lapack_int ihi, lapack_complex_double* h,
                                lapack_int ldh, lapack_complex_double* t,
                                lapack_int ldt, lapack_complex_double* alpha,
                                lapack_complex_double* beta,
                                lapack_complex_double* q, lapack_int ldq,
                                lapack_complex_double* z, lapack_int ldz,
                                lapack_complex_double* work, lapack_int lwork,
                                double* rwork );

lapack_int LAPACKE_chpcon_work( int matrix_layout, char uplo, lapack_int n,
                                const lapack_complex_float* ap,
                                const lapack_int* ipiv, float anorm,
                                float* rcond, lapack_complex_float* work );
lapack_int LAPACKE_zhpcon_work( int matrix_layout, char uplo, lapack_int n,
                                const lapack_complex_double* ap,
                                const lapack_int* ipiv, double anorm,
                                double* rcond, lapack_complex_double* work );

lapack_int LAPACKE_chpev_work( int matrix_layout, char jobz, char uplo,
                               lapack_int n, lapack_complex_float* ap, float* w,
                               lapack_complex_float* z, lapack_int ldz,
                               lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_zhpev_work( int matrix_layout, char jobz, char uplo,
                               lapack_int n, lapack_complex_double* ap,
                               double* w, lapack_complex_double* z,
                               lapack_int ldz, lapack_complex_double* work,
                               double* rwork );

lapack_int LAPACKE_chpevd_work( int matrix_layout, char jobz, char uplo,
                                lapack_int n, lapack_complex_float* ap,
                                float* w, lapack_complex_float* z,
                                lapack_int ldz, lapack_complex_float* work,
                                lapack_int lwork, float* rwork,
                                lapack_int lrwork, lapack_int* iwork,
                                lapack_int liwork );
lapack_int LAPACKE_zhpevd_work( int matrix_layout, char jobz, char uplo,
                                lapack_int n, lapack_complex_double* ap,
                                double* w, lapack_complex_double* z,
                                lapack_int ldz, lapack_complex_double* work,
                                lapack_int lwork, double* rwork,
                                lapack_int lrwork, lapack_int* iwork,
                                lapack_int liwork );

lapack_int LAPACKE_chpevx_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n,
                                lapack_complex_float* ap, float vl, float vu,
                                lapack_int il, lapack_int iu, float abstol,
                                lapack_int* m, float* w,
                                lapack_complex_float* z, lapack_int ldz,
                                lapack_complex_float* work, float* rwork,
                                lapack_int* iwork, lapack_int* ifail );
lapack_int LAPACKE_zhpevx_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n,
                                lapack_complex_double* ap, double vl, double vu,
                                lapack_int il, lapack_int iu, double abstol,
                                lapack_int* m, double* w,
                                lapack_complex_double* z, lapack_int ldz,
                                lapack_complex_double* work, double* rwork,
                                lapack_int* iwork, lapack_int* ifail );

lapack_int LAPACKE_chpgst_work( int matrix_layout, lapack_int itype, char uplo,
                                lapack_int n, lapack_complex_float* ap,
                                const lapack_complex_float* bp );
lapack_int LAPACKE_zhpgst_work( int matrix_layout, lapack_int itype, char uplo,
                                lapack_int n, lapack_complex_double* ap,
                                const lapack_complex_double* bp );

lapack_int LAPACKE_chpgv_work( int matrix_layout, lapack_int itype, char jobz,
                               char uplo, lapack_int n,
                               lapack_complex_float* ap,
                               lapack_complex_float* bp, float* w,
                               lapack_complex_float* z, lapack_int ldz,
                               lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_zhpgv_work( int matrix_layout, lapack_int itype, char jobz,
                               char uplo, lapack_int n,
                               lapack_complex_double* ap,
                               lapack_complex_double* bp, double* w,
                               lapack_complex_double* z, lapack_int ldz,
                               lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_chpgvd_work( int matrix_layout, lapack_int itype, char jobz,
                                char uplo, lapack_int n,
                                lapack_complex_float* ap,
                                lapack_complex_float* bp, float* w,
                                lapack_complex_float* z, lapack_int ldz,
                                lapack_complex_float* work, lapack_int lwork,
                                float* rwork, lapack_int lrwork,
                                lapack_int* iwork, lapack_int liwork );
lapack_int LAPACKE_zhpgvd_work( int matrix_layout, lapack_int itype, char jobz,
                                char uplo, lapack_int n,
                                lapack_complex_double* ap,
                                lapack_complex_double* bp, double* w,
                                lapack_complex_double* z, lapack_int ldz,
                                lapack_complex_double* work, lapack_int lwork,
                                double* rwork, lapack_int lrwork,
                                lapack_int* iwork, lapack_int liwork );

lapack_int LAPACKE_chpgvx_work( int matrix_layout, lapack_int itype, char jobz,
                                char range, char uplo, lapack_int n,
                                lapack_complex_float* ap,
                                lapack_complex_float* bp, float vl, float vu,
                                lapack_int il, lapack_int iu, float abstol,
                                lapack_int* m, float* w,
                                lapack_complex_float* z, lapack_int ldz,
                                lapack_complex_float* work, float* rwork,
                                lapack_int* iwork, lapack_int* ifail );
lapack_int LAPACKE_zhpgvx_work( int matrix_layout, lapack_int itype, char jobz,
                                char range, char uplo, lapack_int n,
                                lapack_complex_double* ap,
                                lapack_complex_double* bp, double vl, double vu,
                                lapack_int il, lapack_int iu, double abstol,
                                lapack_int* m, double* w,
                                lapack_complex_double* z, lapack_int ldz,
                                lapack_complex_double* work, double* rwork,
                                lapack_int* iwork, lapack_int* ifail );

lapack_int LAPACKE_chprfs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const lapack_complex_float* ap,
                                const lapack_complex_float* afp,
                                const lapack_int* ipiv,
                                const lapack_complex_float* b, lapack_int ldb,
                                lapack_complex_float* x, lapack_int ldx,
                                float* ferr, float* berr,
                                lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_zhprfs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs,
                                const lapack_complex_double* ap,
                                const lapack_complex_double* afp,
                                const lapack_int* ipiv,
                                const lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* x, lapack_int ldx,
                                double* ferr, double* berr,
                                lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_chpsv_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, lapack_complex_float* ap,
                               lapack_int* ipiv, lapack_complex_float* b,
                               lapack_int ldb );
lapack_int LAPACKE_zhpsv_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, lapack_complex_double* ap,
                               lapack_int* ipiv, lapack_complex_double* b,
                               lapack_int ldb );

lapack_int LAPACKE_chpsvx_work( int matrix_layout, char fact, char uplo,
                                lapack_int n, lapack_int nrhs,
                                const lapack_complex_float* ap,
                                lapack_complex_float* afp, lapack_int* ipiv,
                                const lapack_complex_float* b, lapack_int ldb,
                                lapack_complex_float* x, lapack_int ldx,
                                float* rcond, float* ferr, float* berr,
                                lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_zhpsvx_work( int matrix_layout, char fact, char uplo,
                                lapack_int n, lapack_int nrhs,
                                const lapack_complex_double* ap,
                                lapack_complex_double* afp, lapack_int* ipiv,
                                const lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* x, lapack_int ldx,
                                double* rcond, double* ferr, double* berr,
                                lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_chptrd_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_float* ap, float* d, float* e,
                                lapack_complex_float* tau );
lapack_int LAPACKE_zhptrd_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_double* ap, double* d, double* e,
                                lapack_complex_double* tau );

lapack_int LAPACKE_chptrf_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_float* ap, lapack_int* ipiv );
lapack_int LAPACKE_zhptrf_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_double* ap, lapack_int* ipiv );

lapack_int LAPACKE_chptri_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_float* ap,
                                const lapack_int* ipiv,
                                lapack_complex_float* work );
lapack_int LAPACKE_zhptri_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_double* ap,
                                const lapack_int* ipiv,
                                lapack_complex_double* work );

lapack_int LAPACKE_chptrs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const lapack_complex_float* ap,
                                const lapack_int* ipiv, lapack_complex_float* b,
                                lapack_int ldb );
lapack_int LAPACKE_zhptrs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs,
                                const lapack_complex_double* ap,
                                const lapack_int* ipiv,
                                lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_shsein_work( int matrix_layout, char job, char eigsrc,
                                char initv, lapack_logical* select,
                                lapack_int n, const float* h, lapack_int ldh,
                                float* wr, const float* wi, float* vl,
                                lapack_int ldvl, float* vr, lapack_int ldvr,
                                lapack_int mm, lapack_int* m, float* work,
                                lapack_int* ifaill, lapack_int* ifailr );
lapack_int LAPACKE_dhsein_work( int matrix_layout, char job, char eigsrc,
                                char initv, lapack_logical* select,
                                lapack_int n, const double* h, lapack_int ldh,
                                double* wr, const double* wi, double* vl,
                                lapack_int ldvl, double* vr, lapack_int ldvr,
                                lapack_int mm, lapack_int* m, double* work,
                                lapack_int* ifaill, lapack_int* ifailr );
lapack_int LAPACKE_chsein_work( int matrix_layout, char job, char eigsrc,
                                char initv, const lapack_logical* select,
                                lapack_int n, const lapack_complex_float* h,
                                lapack_int ldh, lapack_complex_float* w,
                                lapack_complex_float* vl, lapack_int ldvl,
                                lapack_complex_float* vr, lapack_int ldvr,
                                lapack_int mm, lapack_int* m,
                                lapack_complex_float* work, float* rwork,
                                lapack_int* ifaill, lapack_int* ifailr );
lapack_int LAPACKE_zhsein_work( int matrix_layout, char job, char eigsrc,
                                char initv, const lapack_logical* select,
                                lapack_int n, const lapack_complex_double* h,
                                lapack_int ldh, lapack_complex_double* w,
                                lapack_complex_double* vl, lapack_int ldvl,
                                lapack_complex_double* vr, lapack_int ldvr,
                                lapack_int mm, lapack_int* m,
                                lapack_complex_double* work, double* rwork,
                                lapack_int* ifaill, lapack_int* ifailr );

lapack_int LAPACKE_shseqr_work( int matrix_layout, char job, char compz,
                                lapack_int n, lapack_int ilo, lapack_int ihi,
                                float* h, lapack_int ldh, float* wr, float* wi,
                                float* z, lapack_int ldz, float* work,
                                lapack_int lwork );
lapack_int LAPACKE_dhseqr_work( int matrix_layout, char job, char compz,
                                lapack_int n, lapack_int ilo, lapack_int ihi,
                                double* h, lapack_int ldh, double* wr,
                                double* wi, double* z, lapack_int ldz,
                                double* work, lapack_int lwork );
lapack_int LAPACKE_chseqr_work( int matrix_layout, char job, char compz,
                                lapack_int n, lapack_int ilo, lapack_int ihi,
                                lapack_complex_float* h, lapack_int ldh,
                                lapack_complex_float* w,
                                lapack_complex_float* z, lapack_int ldz,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zhseqr_work( int matrix_layout, char job, char compz,
                                lapack_int n, lapack_int ilo, lapack_int ihi,
                                lapack_complex_double* h, lapack_int ldh,
                                lapack_complex_double* w,
                                lapack_complex_double* z, lapack_int ldz,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_clacgv_work( lapack_int n, lapack_complex_float* x,
                                lapack_int incx );
lapack_int LAPACKE_zlacgv_work( lapack_int n, lapack_complex_double* x,
                                lapack_int incx );

lapack_int LAPACKE_slacn2_work( lapack_int n, float* v, float* x,
                                lapack_int* isgn, float* est, lapack_int* kase,
                                lapack_int* isave );
lapack_int LAPACKE_dlacn2_work( lapack_int n, double* v, double* x,
                                lapack_int* isgn, double* est, lapack_int* kase,
                                lapack_int* isave );
lapack_int LAPACKE_clacn2_work( lapack_int n, lapack_complex_float* v,
                                lapack_complex_float* x,
                                float* est, lapack_int* kase,
                                lapack_int* isave );
lapack_int LAPACKE_zlacn2_work( lapack_int n, lapack_complex_double* v,
                                lapack_complex_double* x,
                                double* est, lapack_int* kase,
                                lapack_int* isave );

lapack_int LAPACKE_slacpy_work( int matrix_layout, char uplo, lapack_int m,
                                lapack_int n, const float* a, lapack_int lda,
                                float* b, lapack_int ldb );
lapack_int LAPACKE_dlacpy_work( int matrix_layout, char uplo, lapack_int m,
                                lapack_int n, const double* a, lapack_int lda,
                                double* b, lapack_int ldb );
lapack_int LAPACKE_clacpy_work( int matrix_layout, char uplo, lapack_int m,
                                lapack_int n, const lapack_complex_float* a,
                                lapack_int lda, lapack_complex_float* b,
                                lapack_int ldb );
lapack_int LAPACKE_zlacpy_work( int matrix_layout, char uplo, lapack_int m,
                                lapack_int n, const lapack_complex_double* a,
                                lapack_int lda, lapack_complex_double* b,
                                lapack_int ldb );

lapack_int LAPACKE_clacp2_work( int matrix_layout, char uplo, lapack_int m,
                                lapack_int n, const float* a, lapack_int lda,
                                lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zlacp2_work( int matrix_layout, char uplo, lapack_int m,
                                lapack_int n, const double* a, lapack_int lda,
                                lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_zlag2c_work( int matrix_layout, lapack_int m, lapack_int n,
                                const lapack_complex_double* a, lapack_int lda,
                                lapack_complex_float* sa, lapack_int ldsa );

lapack_int LAPACKE_slag2d_work( int matrix_layout, lapack_int m, lapack_int n,
                                const float* sa, lapack_int ldsa, double* a,
                                lapack_int lda );

lapack_int LAPACKE_dlag2s_work( int matrix_layout, lapack_int m, lapack_int n,
                                const double* a, lapack_int lda, float* sa,
                                lapack_int ldsa );

lapack_int LAPACKE_clag2z_work( int matrix_layout, lapack_int m, lapack_int n,
                                const lapack_complex_float* sa, lapack_int ldsa,
                                lapack_complex_double* a, lapack_int lda );

lapack_int LAPACKE_slagge_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int kl, lapack_int ku, const float* d,
                                float* a, lapack_int lda, lapack_int* iseed,
                                float* work );
lapack_int LAPACKE_dlagge_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int kl, lapack_int ku, const double* d,
                                double* a, lapack_int lda, lapack_int* iseed,
                                double* work );
lapack_int LAPACKE_clagge_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int kl, lapack_int ku, const float* d,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_int* iseed, lapack_complex_float* work );
lapack_int LAPACKE_zlagge_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int kl, lapack_int ku, const double* d,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_int* iseed,
                                lapack_complex_double* work );

lapack_int LAPACKE_claghe_work( int matrix_layout, lapack_int n, lapack_int k,
                                const float* d, lapack_complex_float* a,
                                lapack_int lda, lapack_int* iseed,
                                lapack_complex_float* work );
lapack_int LAPACKE_zlaghe_work( int matrix_layout, lapack_int n, lapack_int k,
                                const double* d, lapack_complex_double* a,
                                lapack_int lda, lapack_int* iseed,
                                lapack_complex_double* work );

lapack_int LAPACKE_slagsy_work( int matrix_layout, lapack_int n, lapack_int k,
                                const float* d, float* a, lapack_int lda,
                                lapack_int* iseed, float* work );
lapack_int LAPACKE_dlagsy_work( int matrix_layout, lapack_int n, lapack_int k,
                                const double* d, double* a, lapack_int lda,
                                lapack_int* iseed, double* work );
lapack_int LAPACKE_clagsy_work( int matrix_layout, lapack_int n, lapack_int k,
                                const float* d, lapack_complex_float* a,
                                lapack_int lda, lapack_int* iseed,
                                lapack_complex_float* work );
lapack_int LAPACKE_zlagsy_work( int matrix_layout, lapack_int n, lapack_int k,
                                const double* d, lapack_complex_double* a,
                                lapack_int lda, lapack_int* iseed,
                                lapack_complex_double* work );

lapack_int LAPACKE_slapmr_work( int matrix_layout, lapack_logical forwrd,
                                lapack_int m, lapack_int n, float* x,
                                lapack_int ldx, lapack_int* k );
lapack_int LAPACKE_dlapmr_work( int matrix_layout, lapack_logical forwrd,
                                lapack_int m, lapack_int n, double* x,
                                lapack_int ldx, lapack_int* k );
lapack_int LAPACKE_clapmr_work( int matrix_layout, lapack_logical forwrd,
                                lapack_int m, lapack_int n,
                                lapack_complex_float* x, lapack_int ldx,
                                lapack_int* k );
lapack_int LAPACKE_zlapmr_work( int matrix_layout, lapack_logical forwrd,
                                lapack_int m, lapack_int n,
                                lapack_complex_double* x, lapack_int ldx,
                                lapack_int* k );

lapack_int LAPACKE_slapmt_work( int matrix_layout, lapack_logical forwrd,
                                lapack_int m, lapack_int n, float* x,
                                lapack_int ldx, lapack_int* k );
lapack_int LAPACKE_dlapmt_work( int matrix_layout, lapack_logical forwrd,
                                lapack_int m, lapack_int n, double* x,
                                lapack_int ldx, lapack_int* k );
lapack_int LAPACKE_clapmt_work( int matrix_layout, lapack_logical forwrd,
                                lapack_int m, lapack_int n,
                                lapack_complex_float* x, lapack_int ldx,
                                lapack_int* k );
lapack_int LAPACKE_zlapmt_work( int matrix_layout, lapack_logical forwrd,
                                lapack_int m, lapack_int n,
                                lapack_complex_double* x, lapack_int ldx,
                                lapack_int* k );

lapack_int LAPACKE_slartgp_work( float f, float g, float* cs, float* sn,
                                 float* r );
lapack_int LAPACKE_dlartgp_work( double f, double g, double* cs, double* sn,
                                 double* r );

lapack_int LAPACKE_slartgs_work( float x, float y, float sigma, float* cs,
                                 float* sn );
lapack_int LAPACKE_dlartgs_work( double x, double y, double sigma, double* cs,
                                 double* sn );

float LAPACKE_slapy2_work( float x, float y );
double LAPACKE_dlapy2_work( double x, double y );

float LAPACKE_slapy3_work( float x, float y, float z );
double LAPACKE_dlapy3_work( double x, double y, double z );

float LAPACKE_slamch_work( char cmach );
double LAPACKE_dlamch_work( char cmach );

float LAPACKE_slangb_work( int matrix_layout, char norm, lapack_int n,
                           lapack_int kl, lapack_int ku, const float* ab,
                           lapack_int ldab, float* work );
double LAPACKE_dlangb_work( int matrix_layout, char norm, lapack_int n,
                            lapack_int kl, lapack_int ku, const double* ab,
                            lapack_int ldab, double* work );
float LAPACKE_clangb_work( int matrix_layout, char norm, lapack_int n,
                           lapack_int kl, lapack_int ku,
                           const lapack_complex_float* ab, lapack_int ldab,
                           float* work );
double LAPACKE_zlangb_work( int matrix_layout, char norm, lapack_int n,
                            lapack_int kl, lapack_int ku,
                            const lapack_complex_double* ab, lapack_int ldab,
                            double* work );

float LAPACKE_slange_work( int matrix_layout, char norm, lapack_int m,
                                lapack_int n, const float* a, lapack_int lda,
                                float* work );
double LAPACKE_dlange_work( int matrix_layout, char norm, lapack_int m,
                                lapack_int n, const double* a, lapack_int lda,
                                double* work );
float LAPACKE_clange_work( int matrix_layout, char norm, lapack_int m,
                                lapack_int n, const lapack_complex_float* a,
                                lapack_int lda, float* work );
double LAPACKE_zlange_work( int matrix_layout, char norm, lapack_int m,
                                lapack_int n, const lapack_complex_double* a,
                                lapack_int lda, double* work );

float LAPACKE_clanhe_work( int matrix_layout, char norm, char uplo,
                                lapack_int n, const lapack_complex_float* a,
                                lapack_int lda, float* work );
double LAPACKE_zlanhe_work( int matrix_layout, char norm, char uplo,
                                lapack_int n, const lapack_complex_double* a,
                                lapack_int lda, double* work );

lapack_int LAPACKE_clacrm_work( int matrix_layout, lapack_int m, lapack_int n,
                                const lapack_complex_float* a,
                                lapack_int lda, const float* b,
                                lapack_int ldb, lapack_complex_float* c,
                                lapack_int ldc, float* work );
lapack_int LAPACKE_zlacrm_work( int matrix_layout, lapack_int m, lapack_int n,
                                const lapack_complex_double* a,
                                lapack_int lda, const double* b,
                                lapack_int ldb, lapack_complex_double* c,
                                lapack_int ldc, double* work );

lapack_int LAPACKE_clarcm_work( int matrix_layout, lapack_int m, lapack_int n,
                                const float* a, lapack_int lda,
                                const lapack_complex_float* b,
                                lapack_int ldb, lapack_complex_float* c,
                                lapack_int ldc, float* work );
lapack_int LAPACKE_zlarcm_work( int matrix_layout, lapack_int m, lapack_int n,
                                const double* a, lapack_int lda,
                                const lapack_complex_double* b,
                                lapack_int ldb, lapack_complex_double* c,
                                lapack_int ldc, double* work );

float LAPACKE_slansy_work( int matrix_layout, char norm, char uplo,
                                lapack_int n, const float* a, lapack_int lda,
                                float* work );
double LAPACKE_dlansy_work( int matrix_layout, char norm, char uplo,
                                lapack_int n, const double* a, lapack_int lda,
                                double* work );
float LAPACKE_clansy_work( int matrix_layout, char norm, char uplo,
                                lapack_int n, const lapack_complex_float* a,
                                lapack_int lda, float* work );
double LAPACKE_zlansy_work( int matrix_layout, char norm, char uplo,
                                lapack_int n, const lapack_complex_double* a,
                                lapack_int lda, double* work );

float LAPACKE_slantr_work( int matrix_layout, char norm, char uplo,
                                char diag, lapack_int m, lapack_int n, const float* a,
                                lapack_int lda, float* work );
double LAPACKE_dlantr_work( int matrix_layout, char norm, char uplo,
                                char diag, lapack_int m, lapack_int n,
                                const double* a, lapack_int lda, double* work );
float LAPACKE_clantr_work( int matrix_layout, char norm, char uplo,
                                char diag, lapack_int m, lapack_int n,
                                const lapack_complex_float* a, lapack_int lda,
                                float* work );
double LAPACKE_zlantr_work( int matrix_layout, char norm, char uplo,
                                char diag, lapack_int m, lapack_int n,
                                const lapack_complex_double* a, lapack_int lda,
                                double* work );

lapack_int LAPACKE_slarfb_work( int matrix_layout, char side, char trans,
                                char direct, char storev, lapack_int m,
                                lapack_int n, lapack_int k, const float* v,
                                lapack_int ldv, const float* t, lapack_int ldt,
                                float* c, lapack_int ldc, float* work,
                                lapack_int ldwork );
lapack_int LAPACKE_dlarfb_work( int matrix_layout, char side, char trans,
                                char direct, char storev, lapack_int m,
                                lapack_int n, lapack_int k, const double* v,
                                lapack_int ldv, const double* t, lapack_int ldt,
                                double* c, lapack_int ldc, double* work,
                                lapack_int ldwork );
lapack_int LAPACKE_clarfb_work( int matrix_layout, char side, char trans,
                                char direct, char storev, lapack_int m,
                                lapack_int n, lapack_int k,
                                const lapack_complex_float* v, lapack_int ldv,
                                const lapack_complex_float* t, lapack_int ldt,
                                lapack_complex_float* c, lapack_int ldc,
                                lapack_complex_float* work, lapack_int ldwork );
lapack_int LAPACKE_zlarfb_work( int matrix_layout, char side, char trans,
                                char direct, char storev, lapack_int m,
                                lapack_int n, lapack_int k,
                                const lapack_complex_double* v, lapack_int ldv,
                                const lapack_complex_double* t, lapack_int ldt,
                                lapack_complex_double* c, lapack_int ldc,
                                lapack_complex_double* work,
                                lapack_int ldwork );

lapack_int LAPACKE_slarfg_work( lapack_int n, float* alpha, float* x,
                                lapack_int incx, float* tau );
lapack_int LAPACKE_dlarfg_work( lapack_int n, double* alpha, double* x,
                                lapack_int incx, double* tau );
lapack_int LAPACKE_clarfg_work( lapack_int n, lapack_complex_float* alpha,
                                lapack_complex_float* x, lapack_int incx,
                                lapack_complex_float* tau );
lapack_int LAPACKE_zlarfg_work( lapack_int n, lapack_complex_double* alpha,
                                lapack_complex_double* x, lapack_int incx,
                                lapack_complex_double* tau );

lapack_int LAPACKE_slarft_work( int matrix_layout, char direct, char storev,
                                lapack_int n, lapack_int k, const float* v,
                                lapack_int ldv, const float* tau, float* t,
                                lapack_int ldt );
lapack_int LAPACKE_dlarft_work( int matrix_layout, char direct, char storev,
                                lapack_int n, lapack_int k, const double* v,
                                lapack_int ldv, const double* tau, double* t,
                                lapack_int ldt );
lapack_int LAPACKE_clarft_work( int matrix_layout, char direct, char storev,
                                lapack_int n, lapack_int k,
                                const lapack_complex_float* v, lapack_int ldv,
                                const lapack_complex_float* tau,
                                lapack_complex_float* t, lapack_int ldt );
lapack_int LAPACKE_zlarft_work( int matrix_layout, char direct, char storev,
                                lapack_int n, lapack_int k,
                                const lapack_complex_double* v, lapack_int ldv,
                                const lapack_complex_double* tau,
                                lapack_complex_double* t, lapack_int ldt );

lapack_int LAPACKE_slarfx_work( int matrix_layout, char side, lapack_int m,
                                lapack_int n, const float* v, float tau,
                                float* c, lapack_int ldc, float* work );
lapack_int LAPACKE_dlarfx_work( int matrix_layout, char side, lapack_int m,
                                lapack_int n, const double* v, double tau,
                                double* c, lapack_int ldc, double* work );
lapack_int LAPACKE_clarfx_work( int matrix_layout, char side, lapack_int m,
                                lapack_int n, const lapack_complex_float* v,
                                lapack_complex_float tau,
                                lapack_complex_float* c, lapack_int ldc,
                                lapack_complex_float* work );
lapack_int LAPACKE_zlarfx_work( int matrix_layout, char side, lapack_int m,
                                lapack_int n, const lapack_complex_double* v,
                                lapack_complex_double tau,
                                lapack_complex_double* c, lapack_int ldc,
                                lapack_complex_double* work );

lapack_int LAPACKE_slarnv_work( lapack_int idist, lapack_int* iseed,
                                lapack_int n, float* x );
lapack_int LAPACKE_dlarnv_work( lapack_int idist, lapack_int* iseed,
                                lapack_int n, double* x );
lapack_int LAPACKE_clarnv_work( lapack_int idist, lapack_int* iseed,
                                lapack_int n, lapack_complex_float* x );
lapack_int LAPACKE_zlarnv_work( lapack_int idist, lapack_int* iseed,
                                lapack_int n, lapack_complex_double* x );


lapack_int LAPACKE_slascl_work( int matrix_layout, char type, lapack_int kl,
                           lapack_int ku, float cfrom, float cto,
                           lapack_int m, lapack_int n, float* a,
                           lapack_int lda );
lapack_int LAPACKE_dlascl_work( int matrix_layout, char type, lapack_int kl,
                           lapack_int ku, double cfrom, double cto,
                           lapack_int m, lapack_int n, double* a,
                           lapack_int lda );
lapack_int LAPACKE_clascl_work( int matrix_layout, char type, lapack_int kl,
                           lapack_int ku, float cfrom, float cto,
                           lapack_int m, lapack_int n, lapack_complex_float* a,
                           lapack_int lda );
lapack_int LAPACKE_zlascl_work( int matrix_layout, char type, lapack_int kl,
                           lapack_int ku, double cfrom, double cto,
                           lapack_int m, lapack_int n, lapack_complex_double* a,
                           lapack_int lda );

lapack_int LAPACKE_slaset_work( int matrix_layout, char uplo, lapack_int m,
                                lapack_int n, float alpha, float beta, float* a,
                                lapack_int lda );
lapack_int LAPACKE_dlaset_work( int matrix_layout, char uplo, lapack_int m,
                                lapack_int n, double alpha, double beta,
                                double* a, lapack_int lda );
lapack_int LAPACKE_claset_work( int matrix_layout, char uplo, lapack_int m,
                                lapack_int n, lapack_complex_float alpha,
                                lapack_complex_float beta,
                                lapack_complex_float* a, lapack_int lda );
lapack_int LAPACKE_zlaset_work( int matrix_layout, char uplo, lapack_int m,
                                lapack_int n, lapack_complex_double alpha,
                                lapack_complex_double beta,
                                lapack_complex_double* a, lapack_int lda );

lapack_int LAPACKE_slasrt_work( char id, lapack_int n, float* d );
lapack_int LAPACKE_dlasrt_work( char id, lapack_int n, double* d );

lapack_int LAPACKE_slassq_work( lapack_int n,                 float* x, lapack_int incx,  float* scale,  float* sumsq );
lapack_int LAPACKE_dlassq_work( lapack_int n,                double* x, lapack_int incx, double* scale, double* sumsq );
lapack_int LAPACKE_classq_work( lapack_int n,  lapack_complex_float* x, lapack_int incx,  float* scale,  float* sumsq );
lapack_int LAPACKE_zlassq_work( lapack_int n, lapack_complex_double* x, lapack_int incx, double* scale, double* sumsq );

lapack_int LAPACKE_slaswp_work( int matrix_layout, lapack_int n, float* a,
                                lapack_int lda, lapack_int k1, lapack_int k2,
                                const lapack_int* ipiv, lapack_int incx );
lapack_int LAPACKE_dlaswp_work( int matrix_layout, lapack_int n, double* a,
                                lapack_int lda, lapack_int k1, lapack_int k2,
                                const lapack_int* ipiv, lapack_int incx );
lapack_int LAPACKE_claswp_work( int matrix_layout, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_int k1, lapack_int k2,
                                const lapack_int* ipiv, lapack_int incx );
lapack_int LAPACKE_zlaswp_work( int matrix_layout, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_int k1, lapack_int k2,
                                const lapack_int* ipiv, lapack_int incx );

lapack_int LAPACKE_slatms_work( int matrix_layout, lapack_int m, lapack_int n,
                                char dist, lapack_int* iseed, char sym,
                                float* d, lapack_int mode, float cond,
                                float dmax, lapack_int kl, lapack_int ku,
                                char pack, float* a, lapack_int lda,
                                float* work );
lapack_int LAPACKE_dlatms_work( int matrix_layout, lapack_int m, lapack_int n,
                                char dist, lapack_int* iseed, char sym,
                                double* d, lapack_int mode, double cond,
                                double dmax, lapack_int kl, lapack_int ku,
                                char pack, double* a, lapack_int lda,
                                double* work );
lapack_int LAPACKE_clatms_work( int matrix_layout, lapack_int m, lapack_int n,
                                char dist, lapack_int* iseed, char sym,
                                float* d, lapack_int mode, float cond,
                                float dmax, lapack_int kl, lapack_int ku,
                                char pack, lapack_complex_float* a,
                                lapack_int lda, lapack_complex_float* work );
lapack_int LAPACKE_zlatms_work( int matrix_layout, lapack_int m, lapack_int n,
                                char dist, lapack_int* iseed, char sym,
                                double* d, lapack_int mode, double cond,
                                double dmax, lapack_int kl, lapack_int ku,
                                char pack, lapack_complex_double* a,
                                lapack_int lda, lapack_complex_double* work );

lapack_int LAPACKE_slauum_work( int matrix_layout, char uplo, lapack_int n,
                                float* a, lapack_int lda );
lapack_int LAPACKE_dlauum_work( int matrix_layout, char uplo, lapack_int n,
                                double* a, lapack_int lda );
lapack_int LAPACKE_clauum_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_float* a, lapack_int lda );
lapack_int LAPACKE_zlauum_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_double* a, lapack_int lda );

lapack_int LAPACKE_sopgtr_work( int matrix_layout, char uplo, lapack_int n,
                                const float* ap, const float* tau, float* q,
                                lapack_int ldq, float* work );
lapack_int LAPACKE_dopgtr_work( int matrix_layout, char uplo, lapack_int n,
                                const double* ap, const double* tau, double* q,
                                lapack_int ldq, double* work );

lapack_int LAPACKE_sopmtr_work( int matrix_layout, char side, char uplo,
                                char trans, lapack_int m, lapack_int n,
                                const float* ap, const float* tau, float* c,
                                lapack_int ldc, float* work );
lapack_int LAPACKE_dopmtr_work( int matrix_layout, char side, char uplo,
                                char trans, lapack_int m, lapack_int n,
                                const double* ap, const double* tau, double* c,
                                lapack_int ldc, double* work );

lapack_int LAPACKE_sorgbr_work( int matrix_layout, char vect, lapack_int m,
                                lapack_int n, lapack_int k, float* a,
                                lapack_int lda, const float* tau, float* work,
                                lapack_int lwork );
lapack_int LAPACKE_dorgbr_work( int matrix_layout, char vect, lapack_int m,
                                lapack_int n, lapack_int k, double* a,
                                lapack_int lda, const double* tau, double* work,
                                lapack_int lwork );

lapack_int LAPACKE_sorghr_work( int matrix_layout, lapack_int n, lapack_int ilo,
                                lapack_int ihi, float* a, lapack_int lda,
                                const float* tau, float* work,
                                lapack_int lwork );
lapack_int LAPACKE_dorghr_work( int matrix_layout, lapack_int n, lapack_int ilo,
                                lapack_int ihi, double* a, lapack_int lda,
                                const double* tau, double* work,
                                lapack_int lwork );

lapack_int LAPACKE_sorglq_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int k, float* a, lapack_int lda,
                                const float* tau, float* work,
                                lapack_int lwork );
lapack_int LAPACKE_dorglq_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int k, double* a, lapack_int lda,
                                const double* tau, double* work,
                                lapack_int lwork );

lapack_int LAPACKE_sorgql_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int k, float* a, lapack_int lda,
                                const float* tau, float* work,
                                lapack_int lwork );
lapack_int LAPACKE_dorgql_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int k, double* a, lapack_int lda,
                                const double* tau, double* work,
                                lapack_int lwork );

lapack_int LAPACKE_sorgqr_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int k, float* a, lapack_int lda,
                                const float* tau, float* work,
                                lapack_int lwork );
lapack_int LAPACKE_dorgqr_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int k, double* a, lapack_int lda,
                                const double* tau, double* work,
                                lapack_int lwork );

lapack_int LAPACKE_sorgrq_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int k, float* a, lapack_int lda,
                                const float* tau, float* work,
                                lapack_int lwork );
lapack_int LAPACKE_dorgrq_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int k, double* a, lapack_int lda,
                                const double* tau, double* work,
                                lapack_int lwork );

lapack_int LAPACKE_sorgtr_work( int matrix_layout, char uplo, lapack_int n,
                                float* a, lapack_int lda, const float* tau,
                                float* work, lapack_int lwork );
lapack_int LAPACKE_dorgtr_work( int matrix_layout, char uplo, lapack_int n,
                                double* a, lapack_int lda, const double* tau,
                                double* work, lapack_int lwork );

lapack_int LAPACKE_sorgtsqr_row_work( int matrix_layout,
                                      lapack_int m, lapack_int n,
                                      lapack_int mb, lapack_int nb,
                                      float* a, lapack_int lda,
                                      const float* t, lapack_int ldt,
                                      float* work, lapack_int lwork );
lapack_int LAPACKE_dorgtsqr_row_work( int matrix_layout,
                                      lapack_int m, lapack_int n,
                                      lapack_int mb, lapack_int nb,
                                      double* a, lapack_int lda,
                                      const double* t, lapack_int ldt,
                                      double* work, lapack_int lwork );

lapack_int LAPACKE_sormbr_work( int matrix_layout, char vect, char side,
                                char trans, lapack_int m, lapack_int n,
                                lapack_int k, const float* a, lapack_int lda,
                                const float* tau, float* c, lapack_int ldc,
                                float* work, lapack_int lwork );
lapack_int LAPACKE_dormbr_work( int matrix_layout, char vect, char side,
                                char trans, lapack_int m, lapack_int n,
                                lapack_int k, const double* a, lapack_int lda,
                                const double* tau, double* c, lapack_int ldc,
                                double* work, lapack_int lwork );

lapack_int LAPACKE_sormhr_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int ilo,
                                lapack_int ihi, const float* a, lapack_int lda,
                                const float* tau, float* c, lapack_int ldc,
                                float* work, lapack_int lwork );
lapack_int LAPACKE_dormhr_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int ilo,
                                lapack_int ihi, const double* a, lapack_int lda,
                                const double* tau, double* c, lapack_int ldc,
                                double* work, lapack_int lwork );

lapack_int LAPACKE_sormlq_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int k,
                                const float* a, lapack_int lda,
                                const float* tau, float* c, lapack_int ldc,
                                float* work, lapack_int lwork );
lapack_int LAPACKE_dormlq_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int k,
                                const double* a, lapack_int lda,
                                const double* tau, double* c, lapack_int ldc,
                                double* work, lapack_int lwork );

lapack_int LAPACKE_sormql_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int k,
                                const float* a, lapack_int lda,
                                const float* tau, float* c, lapack_int ldc,
                                float* work, lapack_int lwork );
lapack_int LAPACKE_dormql_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int k,
                                const double* a, lapack_int lda,
                                const double* tau, double* c, lapack_int ldc,
                                double* work, lapack_int lwork );

lapack_int LAPACKE_sormqr_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int k,
                                const float* a, lapack_int lda,
                                const float* tau, float* c, lapack_int ldc,
                                float* work, lapack_int lwork );
lapack_int LAPACKE_dormqr_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int k,
                                const double* a, lapack_int lda,
                                const double* tau, double* c, lapack_int ldc,
                                double* work, lapack_int lwork );

lapack_int LAPACKE_sormrq_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int k,
                                const float* a, lapack_int lda,
                                const float* tau, float* c, lapack_int ldc,
                                float* work, lapack_int lwork );
lapack_int LAPACKE_dormrq_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int k,
                                const double* a, lapack_int lda,
                                const double* tau, double* c, lapack_int ldc,
                                double* work, lapack_int lwork );

lapack_int LAPACKE_sormrz_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int k,
                                lapack_int l, const float* a, lapack_int lda,
                                const float* tau, float* c, lapack_int ldc,
                                float* work, lapack_int lwork );
lapack_int LAPACKE_dormrz_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int k,
                                lapack_int l, const double* a, lapack_int lda,
                                const double* tau, double* c, lapack_int ldc,
                                double* work, lapack_int lwork );

lapack_int LAPACKE_sormtr_work( int matrix_layout, char side, char uplo,
                                char trans, lapack_int m, lapack_int n,
                                const float* a, lapack_int lda,
                                const float* tau, float* c, lapack_int ldc,
                                float* work, lapack_int lwork );
lapack_int LAPACKE_dormtr_work( int matrix_layout, char side, char uplo,
                                char trans, lapack_int m, lapack_int n,
                                const double* a, lapack_int lda,
                                const double* tau, double* c, lapack_int ldc,
                                double* work, lapack_int lwork );

lapack_int LAPACKE_spbcon_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int kd, const float* ab, lapack_int ldab,
                                float anorm, float* rcond, float* work,
                                lapack_int* iwork );
lapack_int LAPACKE_dpbcon_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int kd, const double* ab,
                                lapack_int ldab, double anorm, double* rcond,
                                double* work, lapack_int* iwork );
lapack_int LAPACKE_cpbcon_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int kd, const lapack_complex_float* ab,
                                lapack_int ldab, float anorm, float* rcond,
                                lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_zpbcon_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int kd, const lapack_complex_double* ab,
                                lapack_int ldab, double anorm, double* rcond,
                                lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_spbequ_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int kd, const float* ab, lapack_int ldab,
                                float* s, float* scond, float* amax );
lapack_int LAPACKE_dpbequ_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int kd, const double* ab,
                                lapack_int ldab, double* s, double* scond,
                                double* amax );
lapack_int LAPACKE_cpbequ_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int kd, const lapack_complex_float* ab,
                                lapack_int ldab, float* s, float* scond,
                                float* amax );
lapack_int LAPACKE_zpbequ_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int kd, const lapack_complex_double* ab,
                                lapack_int ldab, double* s, double* scond,
                                double* amax );

lapack_int LAPACKE_spbrfs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int kd, lapack_int nrhs, const float* ab,
                                lapack_int ldab, const float* afb,
                                lapack_int ldafb, const float* b,
                                lapack_int ldb, float* x, lapack_int ldx,
                                float* ferr, float* berr, float* work,
                                lapack_int* iwork );
lapack_int LAPACKE_dpbrfs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int kd, lapack_int nrhs,
                                const double* ab, lapack_int ldab,
                                const double* afb, lapack_int ldafb,
                                const double* b, lapack_int ldb, double* x,
                                lapack_int ldx, double* ferr, double* berr,
                                double* work, lapack_int* iwork );
lapack_int LAPACKE_cpbrfs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int kd, lapack_int nrhs,
                                const lapack_complex_float* ab, lapack_int ldab,
                                const lapack_complex_float* afb,
                                lapack_int ldafb, const lapack_complex_float* b,
                                lapack_int ldb, lapack_complex_float* x,
                                lapack_int ldx, float* ferr, float* berr,
                                lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_zpbrfs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int kd, lapack_int nrhs,
                                const lapack_complex_double* ab,
                                lapack_int ldab,
                                const lapack_complex_double* afb,
                                lapack_int ldafb,
                                const lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* x, lapack_int ldx,
                                double* ferr, double* berr,
                                lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_spbstf_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int kb, float* bb, lapack_int ldbb );
lapack_int LAPACKE_dpbstf_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int kb, double* bb, lapack_int ldbb );
lapack_int LAPACKE_cpbstf_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int kb, lapack_complex_float* bb,
                                lapack_int ldbb );
lapack_int LAPACKE_zpbstf_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int kb, lapack_complex_double* bb,
                                lapack_int ldbb );

lapack_int LAPACKE_spbsv_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int kd, lapack_int nrhs, float* ab,
                               lapack_int ldab, float* b, lapack_int ldb );
lapack_int LAPACKE_dpbsv_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int kd, lapack_int nrhs, double* ab,
                               lapack_int ldab, double* b, lapack_int ldb );
lapack_int LAPACKE_cpbsv_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int kd, lapack_int nrhs,
                               lapack_complex_float* ab, lapack_int ldab,
                               lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zpbsv_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int kd, lapack_int nrhs,
                               lapack_complex_double* ab, lapack_int ldab,
                               lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_spbsvx_work( int matrix_layout, char fact, char uplo,
                                lapack_int n, lapack_int kd, lapack_int nrhs,
                                float* ab, lapack_int ldab, float* afb,
                                lapack_int ldafb, char* equed, float* s,
                                float* b, lapack_int ldb, float* x,
                                lapack_int ldx, float* rcond, float* ferr,
                                float* berr, float* work, lapack_int* iwork );
lapack_int LAPACKE_dpbsvx_work( int matrix_layout, char fact, char uplo,
                                lapack_int n, lapack_int kd, lapack_int nrhs,
                                double* ab, lapack_int ldab, double* afb,
                                lapack_int ldafb, char* equed, double* s,
                                double* b, lapack_int ldb, double* x,
                                lapack_int ldx, double* rcond, double* ferr,
                                double* berr, double* work, lapack_int* iwork );
lapack_int LAPACKE_cpbsvx_work( int matrix_layout, char fact, char uplo,
                                lapack_int n, lapack_int kd, lapack_int nrhs,
                                lapack_complex_float* ab, lapack_int ldab,
                                lapack_complex_float* afb, lapack_int ldafb,
                                char* equed, float* s, lapack_complex_float* b,
                                lapack_int ldb, lapack_complex_float* x,
                                lapack_int ldx, float* rcond, float* ferr,
                                float* berr, lapack_complex_float* work,
                                float* rwork );
lapack_int LAPACKE_zpbsvx_work( int matrix_layout, char fact, char uplo,
                                lapack_int n, lapack_int kd, lapack_int nrhs,
                                lapack_complex_double* ab, lapack_int ldab,
                                lapack_complex_double* afb, lapack_int ldafb,
                                char* equed, double* s,
                                lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* x, lapack_int ldx,
                                double* rcond, double* ferr, double* berr,
                                lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_spbtrf_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int kd, float* ab, lapack_int ldab );
lapack_int LAPACKE_dpbtrf_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int kd, double* ab, lapack_int ldab );
lapack_int LAPACKE_cpbtrf_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int kd, lapack_complex_float* ab,
                                lapack_int ldab );
lapack_int LAPACKE_zpbtrf_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int kd, lapack_complex_double* ab,
                                lapack_int ldab );

lapack_int LAPACKE_spbtrs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int kd, lapack_int nrhs, const float* ab,
                                lapack_int ldab, float* b, lapack_int ldb );
lapack_int LAPACKE_dpbtrs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int kd, lapack_int nrhs,
                                const double* ab, lapack_int ldab, double* b,
                                lapack_int ldb );
lapack_int LAPACKE_cpbtrs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int kd, lapack_int nrhs,
                                const lapack_complex_float* ab, lapack_int ldab,
                                lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zpbtrs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int kd, lapack_int nrhs,
                                const lapack_complex_double* ab,
                                lapack_int ldab, lapack_complex_double* b,
                                lapack_int ldb );

lapack_int LAPACKE_spftrf_work( int matrix_layout, char transr, char uplo,
                                lapack_int n, float* a );
lapack_int LAPACKE_dpftrf_work( int matrix_layout, char transr, char uplo,
                                lapack_int n, double* a );
lapack_int LAPACKE_cpftrf_work( int matrix_layout, char transr, char uplo,
                                lapack_int n, lapack_complex_float* a );
lapack_int LAPACKE_zpftrf_work( int matrix_layout, char transr, char uplo,
                                lapack_int n, lapack_complex_double* a );

lapack_int LAPACKE_spftri_work( int matrix_layout, char transr, char uplo,
                                lapack_int n, float* a );
lapack_int LAPACKE_dpftri_work( int matrix_layout, char transr, char uplo,
                                lapack_int n, double* a );
lapack_int LAPACKE_cpftri_work( int matrix_layout, char transr, char uplo,
                                lapack_int n, lapack_complex_float* a );
lapack_int LAPACKE_zpftri_work( int matrix_layout, char transr, char uplo,
                                lapack_int n, lapack_complex_double* a );

lapack_int LAPACKE_spftrs_work( int matrix_layout, char transr, char uplo,
                                lapack_int n, lapack_int nrhs, const float* a,
                                float* b, lapack_int ldb );
lapack_int LAPACKE_dpftrs_work( int matrix_layout, char transr, char uplo,
                                lapack_int n, lapack_int nrhs, const double* a,
                                double* b, lapack_int ldb );
lapack_int LAPACKE_cpftrs_work( int matrix_layout, char transr, char uplo,
                                lapack_int n, lapack_int nrhs,
                                const lapack_complex_float* a,
                                lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zpftrs_work( int matrix_layout, char transr, char uplo,
                                lapack_int n, lapack_int nrhs,
                                const lapack_complex_double* a,
                                lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_spocon_work( int matrix_layout, char uplo, lapack_int n,
                                const float* a, lapack_int lda, float anorm,
                                float* rcond, float* work, lapack_int* iwork );
lapack_int LAPACKE_dpocon_work( int matrix_layout, char uplo, lapack_int n,
                                const double* a, lapack_int lda, double anorm,
                                double* rcond, double* work,
                                lapack_int* iwork );
lapack_int LAPACKE_cpocon_work( int matrix_layout, char uplo, lapack_int n,
                                const lapack_complex_float* a, lapack_int lda,
                                float anorm, float* rcond,
                                lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_zpocon_work( int matrix_layout, char uplo, lapack_int n,
                                const lapack_complex_double* a, lapack_int lda,
                                double anorm, double* rcond,
                                lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_spoequ_work( int matrix_layout, lapack_int n, const float* a,
                                lapack_int lda, float* s, float* scond,
                                float* amax );
lapack_int LAPACKE_dpoequ_work( int matrix_layout, lapack_int n, const double* a,
                                lapack_int lda, double* s, double* scond,
                                double* amax );
lapack_int LAPACKE_cpoequ_work( int matrix_layout, lapack_int n,
                                const lapack_complex_float* a, lapack_int lda,
                                float* s, float* scond, float* amax );
lapack_int LAPACKE_zpoequ_work( int matrix_layout, lapack_int n,
                                const lapack_complex_double* a, lapack_int lda,
                                double* s, double* scond, double* amax );

lapack_int LAPACKE_spoequb_work( int matrix_layout, lapack_int n, const float* a,
                                 lapack_int lda, float* s, float* scond,
                                 float* amax );
lapack_int LAPACKE_dpoequb_work( int matrix_layout, lapack_int n,
                                 const double* a, lapack_int lda, double* s,
                                 double* scond, double* amax );
lapack_int LAPACKE_cpoequb_work( int matrix_layout, lapack_int n,
                                 const lapack_complex_float* a, lapack_int lda,
                                 float* s, float* scond, float* amax );
lapack_int LAPACKE_zpoequb_work( int matrix_layout, lapack_int n,
                                 const lapack_complex_double* a, lapack_int lda,
                                 double* s, double* scond, double* amax );

lapack_int LAPACKE_sporfs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const float* a, lapack_int lda,
                                const float* af, lapack_int ldaf,
                                const float* b, lapack_int ldb, float* x,
                                lapack_int ldx, float* ferr, float* berr,
                                float* work, lapack_int* iwork );
lapack_int LAPACKE_dporfs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const double* a,
                                lapack_int lda, const double* af,
                                lapack_int ldaf, const double* b,
                                lapack_int ldb, double* x, lapack_int ldx,
                                double* ferr, double* berr, double* work,
                                lapack_int* iwork );
lapack_int LAPACKE_cporfs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const lapack_complex_float* a,
                                lapack_int lda, const lapack_complex_float* af,
                                lapack_int ldaf, const lapack_complex_float* b,
                                lapack_int ldb, lapack_complex_float* x,
                                lapack_int ldx, float* ferr, float* berr,
                                lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_zporfs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const lapack_complex_double* a,
                                lapack_int lda, const lapack_complex_double* af,
                                lapack_int ldaf, const lapack_complex_double* b,
                                lapack_int ldb, lapack_complex_double* x,
                                lapack_int ldx, double* ferr, double* berr,
                                lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_sporfsx_work( int matrix_layout, char uplo, char equed,
                                 lapack_int n, lapack_int nrhs, const float* a,
                                 lapack_int lda, const float* af,
                                 lapack_int ldaf, const float* s,
                                 const float* b, lapack_int ldb, float* x,
                                 lapack_int ldx, float* rcond, float* berr,
                                 lapack_int n_err_bnds, float* err_bnds_norm,
                                 float* err_bnds_comp, lapack_int nparams,
                                 float* params, float* work,
                                 lapack_int* iwork );
lapack_int LAPACKE_dporfsx_work( int matrix_layout, char uplo, char equed,
                                 lapack_int n, lapack_int nrhs, const double* a,
                                 lapack_int lda, const double* af,
                                 lapack_int ldaf, const double* s,
                                 const double* b, lapack_int ldb, double* x,
                                 lapack_int ldx, double* rcond, double* berr,
                                 lapack_int n_err_bnds, double* err_bnds_norm,
                                 double* err_bnds_comp, lapack_int nparams,
                                 double* params, double* work,
                                 lapack_int* iwork );
lapack_int LAPACKE_cporfsx_work( int matrix_layout, char uplo, char equed,
                                 lapack_int n, lapack_int nrhs,
                                 const lapack_complex_float* a, lapack_int lda,
                                 const lapack_complex_float* af,
                                 lapack_int ldaf, const float* s,
                                 const lapack_complex_float* b, lapack_int ldb,
                                 lapack_complex_float* x, lapack_int ldx,
                                 float* rcond, float* berr,
                                 lapack_int n_err_bnds, float* err_bnds_norm,
                                 float* err_bnds_comp, lapack_int nparams,
                                 float* params, lapack_complex_float* work,
                                 float* rwork );
lapack_int LAPACKE_zporfsx_work( int matrix_layout, char uplo, char equed,
                                 lapack_int n, lapack_int nrhs,
                                 const lapack_complex_double* a, lapack_int lda,
                                 const lapack_complex_double* af,
                                 lapack_int ldaf, const double* s,
                                 const lapack_complex_double* b, lapack_int ldb,
                                 lapack_complex_double* x, lapack_int ldx,
                                 double* rcond, double* berr,
                                 lapack_int n_err_bnds, double* err_bnds_norm,
                                 double* err_bnds_comp, lapack_int nparams,
                                 double* params, lapack_complex_double* work,
                                 double* rwork );

lapack_int LAPACKE_sposv_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, float* a, lapack_int lda,
                               float* b, lapack_int ldb );
lapack_int LAPACKE_dposv_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, double* a, lapack_int lda,
                               double* b, lapack_int ldb );
lapack_int LAPACKE_cposv_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, lapack_complex_float* a,
                               lapack_int lda, lapack_complex_float* b,
                               lapack_int ldb );
lapack_int LAPACKE_zposv_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, lapack_complex_double* a,
                               lapack_int lda, lapack_complex_double* b,
                               lapack_int ldb );
lapack_int LAPACKE_dsposv_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, double* a, lapack_int lda,
                                double* b, lapack_int ldb, double* x,
                                lapack_int ldx, double* work, float* swork,
                                lapack_int* iter );
lapack_int LAPACKE_zcposv_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, lapack_complex_double* a,
                                lapack_int lda, lapack_complex_double* b,
                                lapack_int ldb, lapack_complex_double* x,
                                lapack_int ldx, lapack_complex_double* work,
                                lapack_complex_float* swork, double* rwork,
                                lapack_int* iter );

lapack_int LAPACKE_sposvx_work( int matrix_layout, char fact, char uplo,
                                lapack_int n, lapack_int nrhs, float* a,
                                lapack_int lda, float* af, lapack_int ldaf,
                                char* equed, float* s, float* b, lapack_int ldb,
                                float* x, lapack_int ldx, float* rcond,
                                float* ferr, float* berr, float* work,
                                lapack_int* iwork );
lapack_int LAPACKE_dposvx_work( int matrix_layout, char fact, char uplo,
                                lapack_int n, lapack_int nrhs, double* a,
                                lapack_int lda, double* af, lapack_int ldaf,
                                char* equed, double* s, double* b,
                                lapack_int ldb, double* x, lapack_int ldx,
                                double* rcond, double* ferr, double* berr,
                                double* work, lapack_int* iwork );
lapack_int LAPACKE_cposvx_work( int matrix_layout, char fact, char uplo,
                                lapack_int n, lapack_int nrhs,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_complex_float* af, lapack_int ldaf,
                                char* equed, float* s, lapack_complex_float* b,
                                lapack_int ldb, lapack_complex_float* x,
                                lapack_int ldx, float* rcond, float* ferr,
                                float* berr, lapack_complex_float* work,
                                float* rwork );
lapack_int LAPACKE_zposvx_work( int matrix_layout, char fact, char uplo,
                                lapack_int n, lapack_int nrhs,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* af, lapack_int ldaf,
                                char* equed, double* s,
                                lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* x, lapack_int ldx,
                                double* rcond, double* ferr, double* berr,
                                lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_sposvxx_work( int matrix_layout, char fact, char uplo,
                                 lapack_int n, lapack_int nrhs, float* a,
                                 lapack_int lda, float* af, lapack_int ldaf,
                                 char* equed, float* s, float* b,
                                 lapack_int ldb, float* x, lapack_int ldx,
                                 float* rcond, float* rpvgrw, float* berr,
                                 lapack_int n_err_bnds, float* err_bnds_norm,
                                 float* err_bnds_comp, lapack_int nparams,
                                 float* params, float* work,
                                 lapack_int* iwork );
lapack_int LAPACKE_dposvxx_work( int matrix_layout, char fact, char uplo,
                                 lapack_int n, lapack_int nrhs, double* a,
                                 lapack_int lda, double* af, lapack_int ldaf,
                                 char* equed, double* s, double* b,
                                 lapack_int ldb, double* x, lapack_int ldx,
                                 double* rcond, double* rpvgrw, double* berr,
                                 lapack_int n_err_bnds, double* err_bnds_norm,
                                 double* err_bnds_comp, lapack_int nparams,
                                 double* params, double* work,
                                 lapack_int* iwork );
lapack_int LAPACKE_cposvxx_work( int matrix_layout, char fact, char uplo,
                                 lapack_int n, lapack_int nrhs,
                                 lapack_complex_float* a, lapack_int lda,
                                 lapack_complex_float* af, lapack_int ldaf,
                                 char* equed, float* s, lapack_complex_float* b,
                                 lapack_int ldb, lapack_complex_float* x,
                                 lapack_int ldx, float* rcond, float* rpvgrw,
                                 float* berr, lapack_int n_err_bnds,
                                 float* err_bnds_norm, float* err_bnds_comp,
                                 lapack_int nparams, float* params,
                                 lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_zposvxx_work( int matrix_layout, char fact, char uplo,
                                 lapack_int n, lapack_int nrhs,
                                 lapack_complex_double* a, lapack_int lda,
                                 lapack_complex_double* af, lapack_int ldaf,
                                 char* equed, double* s,
                                 lapack_complex_double* b, lapack_int ldb,
                                 lapack_complex_double* x, lapack_int ldx,
                                 double* rcond, double* rpvgrw, double* berr,
                                 lapack_int n_err_bnds, double* err_bnds_norm,
                                 double* err_bnds_comp, lapack_int nparams,
                                 double* params, lapack_complex_double* work,
                                 double* rwork );

lapack_int LAPACKE_spotrf2_work( int matrix_layout, char uplo, lapack_int n,
                                float* a, lapack_int lda );
lapack_int LAPACKE_dpotrf2_work( int matrix_layout, char uplo, lapack_int n,
                                double* a, lapack_int lda );
lapack_int LAPACKE_cpotrf2_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_float* a, lapack_int lda );
lapack_int LAPACKE_zpotrf2_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_double* a, lapack_int lda );

lapack_int LAPACKE_spotrf_work( int matrix_layout, char uplo, lapack_int n,
                                float* a, lapack_int lda );
lapack_int LAPACKE_dpotrf_work( int matrix_layout, char uplo, lapack_int n,
                                double* a, lapack_int lda );
lapack_int LAPACKE_cpotrf_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_float* a, lapack_int lda );
lapack_int LAPACKE_zpotrf_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_double* a, lapack_int lda );

lapack_int LAPACKE_spotri_work( int matrix_layout, char uplo, lapack_int n,
                                float* a, lapack_int lda );
lapack_int LAPACKE_dpotri_work( int matrix_layout, char uplo, lapack_int n,
                                double* a, lapack_int lda );
lapack_int LAPACKE_cpotri_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_float* a, lapack_int lda );
lapack_int LAPACKE_zpotri_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_double* a, lapack_int lda );

lapack_int LAPACKE_spotrs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const float* a, lapack_int lda,
                                float* b, lapack_int ldb );
lapack_int LAPACKE_dpotrs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const double* a,
                                lapack_int lda, double* b, lapack_int ldb );
lapack_int LAPACKE_cpotrs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const lapack_complex_float* a,
                                lapack_int lda, lapack_complex_float* b,
                                lapack_int ldb );
lapack_int LAPACKE_zpotrs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const lapack_complex_double* a,
                                lapack_int lda, lapack_complex_double* b,
                                lapack_int ldb );

lapack_int LAPACKE_sppcon_work( int matrix_layout, char uplo, lapack_int n,
                                const float* ap, float anorm, float* rcond,
                                float* work, lapack_int* iwork );
lapack_int LAPACKE_dppcon_work( int matrix_layout, char uplo, lapack_int n,
                                const double* ap, double anorm, double* rcond,
                                double* work, lapack_int* iwork );
lapack_int LAPACKE_cppcon_work( int matrix_layout, char uplo, lapack_int n,
                                const lapack_complex_float* ap, float anorm,
                                float* rcond, lapack_complex_float* work,
                                float* rwork );
lapack_int LAPACKE_zppcon_work( int matrix_layout, char uplo, lapack_int n,
                                const lapack_complex_double* ap, double anorm,
                                double* rcond, lapack_complex_double* work,
                                double* rwork );

lapack_int LAPACKE_sppequ_work( int matrix_layout, char uplo, lapack_int n,
                                const float* ap, float* s, float* scond,
                                float* amax );
lapack_int LAPACKE_dppequ_work( int matrix_layout, char uplo, lapack_int n,
                                const double* ap, double* s, double* scond,
                                double* amax );
lapack_int LAPACKE_cppequ_work( int matrix_layout, char uplo, lapack_int n,
                                const lapack_complex_float* ap, float* s,
                                float* scond, float* amax );
lapack_int LAPACKE_zppequ_work( int matrix_layout, char uplo, lapack_int n,
                                const lapack_complex_double* ap, double* s,
                                double* scond, double* amax );

lapack_int LAPACKE_spprfs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const float* ap,
                                const float* afp, const float* b,
                                lapack_int ldb, float* x, lapack_int ldx,
                                float* ferr, float* berr, float* work,
                                lapack_int* iwork );
lapack_int LAPACKE_dpprfs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const double* ap,
                                const double* afp, const double* b,
                                lapack_int ldb, double* x, lapack_int ldx,
                                double* ferr, double* berr, double* work,
                                lapack_int* iwork );
lapack_int LAPACKE_cpprfs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const lapack_complex_float* ap,
                                const lapack_complex_float* afp,
                                const lapack_complex_float* b, lapack_int ldb,
                                lapack_complex_float* x, lapack_int ldx,
                                float* ferr, float* berr,
                                lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_zpprfs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs,
                                const lapack_complex_double* ap,
                                const lapack_complex_double* afp,
                                const lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* x, lapack_int ldx,
                                double* ferr, double* berr,
                                lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_sppsv_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, float* ap, float* b,
                               lapack_int ldb );
lapack_int LAPACKE_dppsv_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, double* ap, double* b,
                               lapack_int ldb );
lapack_int LAPACKE_cppsv_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, lapack_complex_float* ap,
                               lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zppsv_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, lapack_complex_double* ap,
                               lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_sppsvx_work( int matrix_layout, char fact, char uplo,
                                lapack_int n, lapack_int nrhs, float* ap,
                                float* afp, char* equed, float* s, float* b,
                                lapack_int ldb, float* x, lapack_int ldx,
                                float* rcond, float* ferr, float* berr,
                                float* work, lapack_int* iwork );
lapack_int LAPACKE_dppsvx_work( int matrix_layout, char fact, char uplo,
                                lapack_int n, lapack_int nrhs, double* ap,
                                double* afp, char* equed, double* s, double* b,
                                lapack_int ldb, double* x, lapack_int ldx,
                                double* rcond, double* ferr, double* berr,
                                double* work, lapack_int* iwork );
lapack_int LAPACKE_cppsvx_work( int matrix_layout, char fact, char uplo,
                                lapack_int n, lapack_int nrhs,
                                lapack_complex_float* ap,
                                lapack_complex_float* afp, char* equed,
                                float* s, lapack_complex_float* b,
                                lapack_int ldb, lapack_complex_float* x,
                                lapack_int ldx, float* rcond, float* ferr,
                                float* berr, lapack_complex_float* work,
                                float* rwork );
lapack_int LAPACKE_zppsvx_work( int matrix_layout, char fact, char uplo,
                                lapack_int n, lapack_int nrhs,
                                lapack_complex_double* ap,
                                lapack_complex_double* afp, char* equed,
                                double* s, lapack_complex_double* b,
                                lapack_int ldb, lapack_complex_double* x,
                                lapack_int ldx, double* rcond, double* ferr,
                                double* berr, lapack_complex_double* work,
                                double* rwork );

lapack_int LAPACKE_spptrf_work( int matrix_layout, char uplo, lapack_int n,
                                float* ap );
lapack_int LAPACKE_dpptrf_work( int matrix_layout, char uplo, lapack_int n,
                                double* ap );
lapack_int LAPACKE_cpptrf_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_float* ap );
lapack_int LAPACKE_zpptrf_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_double* ap );

lapack_int LAPACKE_spptri_work( int matrix_layout, char uplo, lapack_int n,
                                float* ap );
lapack_int LAPACKE_dpptri_work( int matrix_layout, char uplo, lapack_int n,
                                double* ap );
lapack_int LAPACKE_cpptri_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_float* ap );
lapack_int LAPACKE_zpptri_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_double* ap );

lapack_int LAPACKE_spptrs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const float* ap, float* b,
                                lapack_int ldb );
lapack_int LAPACKE_dpptrs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const double* ap, double* b,
                                lapack_int ldb );
lapack_int LAPACKE_cpptrs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const lapack_complex_float* ap,
                                lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zpptrs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs,
                                const lapack_complex_double* ap,
                                lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_spstrf_work( int matrix_layout, char uplo, lapack_int n,
                                float* a, lapack_int lda, lapack_int* piv,
                                lapack_int* rank, float tol, float* work );
lapack_int LAPACKE_dpstrf_work( int matrix_layout, char uplo, lapack_int n,
                                double* a, lapack_int lda, lapack_int* piv,
                                lapack_int* rank, double tol, double* work );
lapack_int LAPACKE_cpstrf_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_int* piv, lapack_int* rank, float tol,
                                float* work );
lapack_int LAPACKE_zpstrf_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_int* piv, lapack_int* rank, double tol,
                                double* work );

lapack_int LAPACKE_sptcon_work( lapack_int n, const float* d, const float* e,
                                float anorm, float* rcond, float* work );
lapack_int LAPACKE_dptcon_work( lapack_int n, const double* d, const double* e,
                                double anorm, double* rcond, double* work );
lapack_int LAPACKE_cptcon_work( lapack_int n, const float* d,
                                const lapack_complex_float* e, float anorm,
                                float* rcond, float* work );
lapack_int LAPACKE_zptcon_work( lapack_int n, const double* d,
                                const lapack_complex_double* e, double anorm,
                                double* rcond, double* work );

lapack_int LAPACKE_spteqr_work( int matrix_layout, char compz, lapack_int n,
                                float* d, float* e, float* z, lapack_int ldz,
                                float* work );
lapack_int LAPACKE_dpteqr_work( int matrix_layout, char compz, lapack_int n,
                                double* d, double* e, double* z, lapack_int ldz,
                                double* work );
lapack_int LAPACKE_cpteqr_work( int matrix_layout, char compz, lapack_int n,
                                float* d, float* e, lapack_complex_float* z,
                                lapack_int ldz, float* work );
lapack_int LAPACKE_zpteqr_work( int matrix_layout, char compz, lapack_int n,
                                double* d, double* e, lapack_complex_double* z,
                                lapack_int ldz, double* work );

lapack_int LAPACKE_sptrfs_work( int matrix_layout, lapack_int n, lapack_int nrhs,
                                const float* d, const float* e, const float* df,
                                const float* ef, const float* b, lapack_int ldb,
                                float* x, lapack_int ldx, float* ferr,
                                float* berr, float* work );
lapack_int LAPACKE_dptrfs_work( int matrix_layout, lapack_int n, lapack_int nrhs,
                                const double* d, const double* e,
                                const double* df, const double* ef,
                                const double* b, lapack_int ldb, double* x,
                                lapack_int ldx, double* ferr, double* berr,
                                double* work );
lapack_int LAPACKE_cptrfs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const float* d,
                                const lapack_complex_float* e, const float* df,
                                const lapack_complex_float* ef,
                                const lapack_complex_float* b, lapack_int ldb,
                                lapack_complex_float* x, lapack_int ldx,
                                float* ferr, float* berr,
                                lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_zptrfs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const double* d,
                                const lapack_complex_double* e,
                                const double* df,
                                const lapack_complex_double* ef,
                                const lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* x, lapack_int ldx,
                                double* ferr, double* berr,
                                lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_sptsv_work( int matrix_layout, lapack_int n, lapack_int nrhs,
                               float* d, float* e, float* b, lapack_int ldb );
lapack_int LAPACKE_dptsv_work( int matrix_layout, lapack_int n, lapack_int nrhs,
                               double* d, double* e, double* b,
                               lapack_int ldb );
lapack_int LAPACKE_cptsv_work( int matrix_layout, lapack_int n, lapack_int nrhs,
                               float* d, lapack_complex_float* e,
                               lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zptsv_work( int matrix_layout, lapack_int n, lapack_int nrhs,
                               double* d, lapack_complex_double* e,
                               lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_sptsvx_work( int matrix_layout, char fact, lapack_int n,
                                lapack_int nrhs, const float* d, const float* e,
                                float* df, float* ef, const float* b,
                                lapack_int ldb, float* x, lapack_int ldx,
                                float* rcond, float* ferr, float* berr,
                                float* work );
lapack_int LAPACKE_dptsvx_work( int matrix_layout, char fact, lapack_int n,
                                lapack_int nrhs, const double* d,
                                const double* e, double* df, double* ef,
                                const double* b, lapack_int ldb, double* x,
                                lapack_int ldx, double* rcond, double* ferr,
                                double* berr, double* work );
lapack_int LAPACKE_cptsvx_work( int matrix_layout, char fact, lapack_int n,
                                lapack_int nrhs, const float* d,
                                const lapack_complex_float* e, float* df,
                                lapack_complex_float* ef,
                                const lapack_complex_float* b, lapack_int ldb,
                                lapack_complex_float* x, lapack_int ldx,
                                float* rcond, float* ferr, float* berr,
                                lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_zptsvx_work( int matrix_layout, char fact, lapack_int n,
                                lapack_int nrhs, const double* d,
                                const lapack_complex_double* e, double* df,
                                lapack_complex_double* ef,
                                const lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* x, lapack_int ldx,
                                double* rcond, double* ferr, double* berr,
                                lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_spttrf_work( lapack_int n, float* d, float* e );
lapack_int LAPACKE_dpttrf_work( lapack_int n, double* d, double* e );
lapack_int LAPACKE_cpttrf_work( lapack_int n, float* d,
                                lapack_complex_float* e );
lapack_int LAPACKE_zpttrf_work( lapack_int n, double* d,
                                lapack_complex_double* e );

lapack_int LAPACKE_spttrs_work( int matrix_layout, lapack_int n, lapack_int nrhs,
                                const float* d, const float* e, float* b,
                                lapack_int ldb );
lapack_int LAPACKE_dpttrs_work( int matrix_layout, lapack_int n, lapack_int nrhs,
                                const double* d, const double* e, double* b,
                                lapack_int ldb );
lapack_int LAPACKE_cpttrs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const float* d,
                                const lapack_complex_float* e,
                                lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zpttrs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const double* d,
                                const lapack_complex_double* e,
                                lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_ssbev_work( int matrix_layout, char jobz, char uplo,
                               lapack_int n, lapack_int kd, float* ab,
                               lapack_int ldab, float* w, float* z,
                               lapack_int ldz, float* work );
lapack_int LAPACKE_dsbev_work( int matrix_layout, char jobz, char uplo,
                               lapack_int n, lapack_int kd, double* ab,
                               lapack_int ldab, double* w, double* z,
                               lapack_int ldz, double* work );

lapack_int LAPACKE_ssbevd_work( int matrix_layout, char jobz, char uplo,
                                lapack_int n, lapack_int kd, float* ab,
                                lapack_int ldab, float* w, float* z,
                                lapack_int ldz, float* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );
lapack_int LAPACKE_dsbevd_work( int matrix_layout, char jobz, char uplo,
                                lapack_int n, lapack_int kd, double* ab,
                                lapack_int ldab, double* w, double* z,
                                lapack_int ldz, double* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );

lapack_int LAPACKE_ssbevx_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n, lapack_int kd,
                                float* ab, lapack_int ldab, float* q,
                                lapack_int ldq, float vl, float vu,
                                lapack_int il, lapack_int iu, float abstol,
                                lapack_int* m, float* w, float* z,
                                lapack_int ldz, float* work,
                                lapack_int* iwork, lapack_int* ifail );
lapack_int LAPACKE_dsbevx_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n, lapack_int kd,
                                double* ab, lapack_int ldab, double* q,
                                lapack_int ldq, double vl, double vu,
                                lapack_int il, lapack_int iu, double abstol,
                                lapack_int* m, double* w, double* z,
                                lapack_int ldz, double* work,
                                lapack_int* iwork, lapack_int* ifail );

lapack_int LAPACKE_ssbgst_work( int matrix_layout, char vect, char uplo,
                                lapack_int n, lapack_int ka, lapack_int kb,
                                float* ab, lapack_int ldab, const float* bb,
                                lapack_int ldbb, float* x, lapack_int ldx,
                                float* work );
lapack_int LAPACKE_dsbgst_work( int matrix_layout, char vect, char uplo,
                                lapack_int n, lapack_int ka, lapack_int kb,
                                double* ab, lapack_int ldab, const double* bb,
                                lapack_int ldbb, double* x, lapack_int ldx,
                                double* work );

lapack_int LAPACKE_ssbgv_work( int matrix_layout, char jobz, char uplo,
                               lapack_int n, lapack_int ka, lapack_int kb,
                               float* ab, lapack_int ldab, float* bb,
                               lapack_int ldbb, float* w, float* z,
                               lapack_int ldz, float* work );
lapack_int LAPACKE_dsbgv_work( int matrix_layout, char jobz, char uplo,
                               lapack_int n, lapack_int ka, lapack_int kb,
                               double* ab, lapack_int ldab, double* bb,
                               lapack_int ldbb, double* w, double* z,
                               lapack_int ldz, double* work );

lapack_int LAPACKE_ssbgvd_work( int matrix_layout, char jobz, char uplo,
                                lapack_int n, lapack_int ka, lapack_int kb,
                                float* ab, lapack_int ldab, float* bb,
                                lapack_int ldbb, float* w, float* z,
                                lapack_int ldz, float* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );
lapack_int LAPACKE_dsbgvd_work( int matrix_layout, char jobz, char uplo,
                                lapack_int n, lapack_int ka, lapack_int kb,
                                double* ab, lapack_int ldab, double* bb,
                                lapack_int ldbb, double* w, double* z,
                                lapack_int ldz, double* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );

lapack_int LAPACKE_ssbgvx_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n, lapack_int ka,
                                lapack_int kb, float* ab, lapack_int ldab,
                                float* bb, lapack_int ldbb, float* q,
                                lapack_int ldq, float vl, float vu,
                                lapack_int il, lapack_int iu, float abstol,
                                lapack_int* m, float* w, float* z,
                                lapack_int ldz, float* work, lapack_int* iwork,
                                lapack_int* ifail );
lapack_int LAPACKE_dsbgvx_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n, lapack_int ka,
                                lapack_int kb, double* ab, lapack_int ldab,
                                double* bb, lapack_int ldbb, double* q,
                                lapack_int ldq, double vl, double vu,
                                lapack_int il, lapack_int iu, double abstol,
                                lapack_int* m, double* w, double* z,
                                lapack_int ldz, double* work, lapack_int* iwork,
                                lapack_int* ifail );

lapack_int LAPACKE_ssbtrd_work( int matrix_layout, char vect, char uplo,
                                lapack_int n, lapack_int kd, float* ab,
                                lapack_int ldab, float* d, float* e, float* q,
                                lapack_int ldq, float* work );
lapack_int LAPACKE_dsbtrd_work( int matrix_layout, char vect, char uplo,
                                lapack_int n, lapack_int kd, double* ab,
                                lapack_int ldab, double* d, double* e,
                                double* q, lapack_int ldq, double* work );

lapack_int LAPACKE_ssfrk_work( int matrix_layout, char transr, char uplo,
                               char trans, lapack_int n, lapack_int k,
                               float alpha, const float* a, lapack_int lda,
                               float beta, float* c );
lapack_int LAPACKE_dsfrk_work( int matrix_layout, char transr, char uplo,
                               char trans, lapack_int n, lapack_int k,
                               double alpha, const double* a, lapack_int lda,
                               double beta, double* c );

lapack_int LAPACKE_sspcon_work( int matrix_layout, char uplo, lapack_int n,
                                const float* ap, const lapack_int* ipiv,
                                float anorm, float* rcond, float* work,
                                lapack_int* iwork );
lapack_int LAPACKE_dspcon_work( int matrix_layout, char uplo, lapack_int n,
                                const double* ap, const lapack_int* ipiv,
                                double anorm, double* rcond, double* work,
                                lapack_int* iwork );
lapack_int LAPACKE_cspcon_work( int matrix_layout, char uplo, lapack_int n,
                                const lapack_complex_float* ap,
                                const lapack_int* ipiv, float anorm,
                                float* rcond, lapack_complex_float* work );
lapack_int LAPACKE_zspcon_work( int matrix_layout, char uplo, lapack_int n,
                                const lapack_complex_double* ap,
                                const lapack_int* ipiv, double anorm,
                                double* rcond, lapack_complex_double* work );

lapack_int LAPACKE_sspev_work( int matrix_layout, char jobz, char uplo,
                               lapack_int n, float* ap, float* w, float* z,
                               lapack_int ldz, float* work );
lapack_int LAPACKE_dspev_work( int matrix_layout, char jobz, char uplo,
                               lapack_int n, double* ap, double* w, double* z,
                               lapack_int ldz, double* work );

lapack_int LAPACKE_sspevd_work( int matrix_layout, char jobz, char uplo,
                                lapack_int n, float* ap, float* w, float* z,
                                lapack_int ldz, float* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );
lapack_int LAPACKE_dspevd_work( int matrix_layout, char jobz, char uplo,
                                lapack_int n, double* ap, double* w, double* z,
                                lapack_int ldz, double* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );

lapack_int LAPACKE_sspevx_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n, float* ap, float vl,
                                float vu, lapack_int il, lapack_int iu,
                                float abstol, lapack_int* m, float* w, float* z,
                                lapack_int ldz, float* work, lapack_int* iwork,
                                lapack_int* ifail );
lapack_int LAPACKE_dspevx_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n, double* ap, double vl,
                                double vu, lapack_int il, lapack_int iu,
                                double abstol, lapack_int* m, double* w,
                                double* z, lapack_int ldz, double* work,
                                lapack_int* iwork, lapack_int* ifail );

lapack_int LAPACKE_sspgst_work( int matrix_layout, lapack_int itype, char uplo,
                                lapack_int n, float* ap, const float* bp );
lapack_int LAPACKE_dspgst_work( int matrix_layout, lapack_int itype, char uplo,
                                lapack_int n, double* ap, const double* bp );

lapack_int LAPACKE_sspgv_work( int matrix_layout, lapack_int itype, char jobz,
                               char uplo, lapack_int n, float* ap, float* bp,
                               float* w, float* z, lapack_int ldz,
                               float* work );
lapack_int LAPACKE_dspgv_work( int matrix_layout, lapack_int itype, char jobz,
                               char uplo, lapack_int n, double* ap, double* bp,
                               double* w, double* z, lapack_int ldz,
                               double* work );

lapack_int LAPACKE_sspgvd_work( int matrix_layout, lapack_int itype, char jobz,
                                char uplo, lapack_int n, float* ap, float* bp,
                                float* w, float* z, lapack_int ldz, float* work,
                                lapack_int lwork, lapack_int* iwork,
                                lapack_int liwork );
lapack_int LAPACKE_dspgvd_work( int matrix_layout, lapack_int itype, char jobz,
                                char uplo, lapack_int n, double* ap, double* bp,
                                double* w, double* z, lapack_int ldz,
                                double* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );

lapack_int LAPACKE_sspgvx_work( int matrix_layout, lapack_int itype, char jobz,
                                char range, char uplo, lapack_int n, float* ap,
                                float* bp, float vl, float vu, lapack_int il,
                                lapack_int iu, float abstol, lapack_int* m,
                                float* w, float* z, lapack_int ldz, float* work,
                                lapack_int* iwork, lapack_int* ifail );
lapack_int LAPACKE_dspgvx_work( int matrix_layout, lapack_int itype, char jobz,
                                char range, char uplo, lapack_int n, double* ap,
                                double* bp, double vl, double vu, lapack_int il,
                                lapack_int iu, double abstol, lapack_int* m,
                                double* w, double* z, lapack_int ldz,
                                double* work, lapack_int* iwork,
                                lapack_int* ifail );

lapack_int LAPACKE_ssprfs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const float* ap,
                                const float* afp, const lapack_int* ipiv,
                                const float* b, lapack_int ldb, float* x,
                                lapack_int ldx, float* ferr, float* berr,
                                float* work, lapack_int* iwork );
lapack_int LAPACKE_dsprfs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const double* ap,
                                const double* afp, const lapack_int* ipiv,
                                const double* b, lapack_int ldb, double* x,
                                lapack_int ldx, double* ferr, double* berr,
                                double* work, lapack_int* iwork );
lapack_int LAPACKE_csprfs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const lapack_complex_float* ap,
                                const lapack_complex_float* afp,
                                const lapack_int* ipiv,
                                const lapack_complex_float* b, lapack_int ldb,
                                lapack_complex_float* x, lapack_int ldx,
                                float* ferr, float* berr,
                                lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_zsprfs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs,
                                const lapack_complex_double* ap,
                                const lapack_complex_double* afp,
                                const lapack_int* ipiv,
                                const lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* x, lapack_int ldx,
                                double* ferr, double* berr,
                                lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_sspsv_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, float* ap, lapack_int* ipiv,
                               float* b, lapack_int ldb );
lapack_int LAPACKE_dspsv_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, double* ap, lapack_int* ipiv,
                               double* b, lapack_int ldb );
lapack_int LAPACKE_cspsv_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, lapack_complex_float* ap,
                               lapack_int* ipiv, lapack_complex_float* b,
                               lapack_int ldb );
lapack_int LAPACKE_zspsv_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, lapack_complex_double* ap,
                               lapack_int* ipiv, lapack_complex_double* b,
                               lapack_int ldb );

lapack_int LAPACKE_sspsvx_work( int matrix_layout, char fact, char uplo,
                                lapack_int n, lapack_int nrhs, const float* ap,
                                float* afp, lapack_int* ipiv, const float* b,
                                lapack_int ldb, float* x, lapack_int ldx,
                                float* rcond, float* ferr, float* berr,
                                float* work, lapack_int* iwork );
lapack_int LAPACKE_dspsvx_work( int matrix_layout, char fact, char uplo,
                                lapack_int n, lapack_int nrhs, const double* ap,
                                double* afp, lapack_int* ipiv, const double* b,
                                lapack_int ldb, double* x, lapack_int ldx,
                                double* rcond, double* ferr, double* berr,
                                double* work, lapack_int* iwork );
lapack_int LAPACKE_cspsvx_work( int matrix_layout, char fact, char uplo,
                                lapack_int n, lapack_int nrhs,
                                const lapack_complex_float* ap,
                                lapack_complex_float* afp, lapack_int* ipiv,
                                const lapack_complex_float* b, lapack_int ldb,
                                lapack_complex_float* x, lapack_int ldx,
                                float* rcond, float* ferr, float* berr,
                                lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_zspsvx_work( int matrix_layout, char fact, char uplo,
                                lapack_int n, lapack_int nrhs,
                                const lapack_complex_double* ap,
                                lapack_complex_double* afp, lapack_int* ipiv,
                                const lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* x, lapack_int ldx,
                                double* rcond, double* ferr, double* berr,
                                lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_ssptrd_work( int matrix_layout, char uplo, lapack_int n,
                                float* ap, float* d, float* e, float* tau );
lapack_int LAPACKE_dsptrd_work( int matrix_layout, char uplo, lapack_int n,
                                double* ap, double* d, double* e, double* tau );

lapack_int LAPACKE_ssptrf_work( int matrix_layout, char uplo, lapack_int n,
                                float* ap, lapack_int* ipiv );
lapack_int LAPACKE_dsptrf_work( int matrix_layout, char uplo, lapack_int n,
                                double* ap, lapack_int* ipiv );
lapack_int LAPACKE_csptrf_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_float* ap, lapack_int* ipiv );
lapack_int LAPACKE_zsptrf_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_double* ap, lapack_int* ipiv );

lapack_int LAPACKE_ssptri_work( int matrix_layout, char uplo, lapack_int n,
                                float* ap, const lapack_int* ipiv,
                                float* work );
lapack_int LAPACKE_dsptri_work( int matrix_layout, char uplo, lapack_int n,
                                double* ap, const lapack_int* ipiv,
                                double* work );
lapack_int LAPACKE_csptri_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_float* ap,
                                const lapack_int* ipiv,
                                lapack_complex_float* work );
lapack_int LAPACKE_zsptri_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_double* ap,
                                const lapack_int* ipiv,
                                lapack_complex_double* work );

lapack_int LAPACKE_ssptrs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const float* ap,
                                const lapack_int* ipiv, float* b,
                                lapack_int ldb );
lapack_int LAPACKE_dsptrs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const double* ap,
                                const lapack_int* ipiv, double* b,
                                lapack_int ldb );
lapack_int LAPACKE_csptrs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const lapack_complex_float* ap,
                                const lapack_int* ipiv, lapack_complex_float* b,
                                lapack_int ldb );
lapack_int LAPACKE_zsptrs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs,
                                const lapack_complex_double* ap,
                                const lapack_int* ipiv,
                                lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_sstebz_work( char range, char order, lapack_int n, float vl,
                                float vu, lapack_int il, lapack_int iu,
                                float abstol, const float* d, const float* e,
                                lapack_int* m, lapack_int* nsplit, float* w,
                                lapack_int* iblock, lapack_int* isplit,
                                float* work, lapack_int* iwork );
lapack_int LAPACKE_dstebz_work( char range, char order, lapack_int n, double vl,
                                double vu, lapack_int il, lapack_int iu,
                                double abstol, const double* d, const double* e,
                                lapack_int* m, lapack_int* nsplit, double* w,
                                lapack_int* iblock, lapack_int* isplit,
                                double* work, lapack_int* iwork );

lapack_int LAPACKE_sstedc_work( int matrix_layout, char compz, lapack_int n,
                                float* d, float* e, float* z, lapack_int ldz,
                                float* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );
lapack_int LAPACKE_dstedc_work( int matrix_layout, char compz, lapack_int n,
                                double* d, double* e, double* z, lapack_int ldz,
                                double* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );
lapack_int LAPACKE_cstedc_work( int matrix_layout, char compz, lapack_int n,
                                float* d, float* e, lapack_complex_float* z,
                                lapack_int ldz, lapack_complex_float* work,
                                lapack_int lwork, float* rwork,
                                lapack_int lrwork, lapack_int* iwork,
                                lapack_int liwork );
lapack_int LAPACKE_zstedc_work( int matrix_layout, char compz, lapack_int n,
                                double* d, double* e, lapack_complex_double* z,
                                lapack_int ldz, lapack_complex_double* work,
                                lapack_int lwork, double* rwork,
                                lapack_int lrwork, lapack_int* iwork,
                                lapack_int liwork );

lapack_int LAPACKE_sstegr_work( int matrix_layout, char jobz, char range,
                                lapack_int n, float* d, float* e, float vl,
                                float vu, lapack_int il, lapack_int iu,
                                float abstol, lapack_int* m, float* w, float* z,
                                lapack_int ldz, lapack_int* isuppz, float* work,
                                lapack_int lwork, lapack_int* iwork,
                                lapack_int liwork );
lapack_int LAPACKE_dstegr_work( int matrix_layout, char jobz, char range,
                                lapack_int n, double* d, double* e, double vl,
                                double vu, lapack_int il, lapack_int iu,
                                double abstol, lapack_int* m, double* w,
                                double* z, lapack_int ldz, lapack_int* isuppz,
                                double* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );
lapack_int LAPACKE_cstegr_work( int matrix_layout, char jobz, char range,
                                lapack_int n, float* d, float* e, float vl,
                                float vu, lapack_int il, lapack_int iu,
                                float abstol, lapack_int* m, float* w,
                                lapack_complex_float* z, lapack_int ldz,
                                lapack_int* isuppz, float* work,
                                lapack_int lwork, lapack_int* iwork,
                                lapack_int liwork );
lapack_int LAPACKE_zstegr_work( int matrix_layout, char jobz, char range,
                                lapack_int n, double* d, double* e, double vl,
                                double vu, lapack_int il, lapack_int iu,
                                double abstol, lapack_int* m, double* w,
                                lapack_complex_double* z, lapack_int ldz,
                                lapack_int* isuppz, double* work,
                                lapack_int lwork, lapack_int* iwork,
                                lapack_int liwork );

lapack_int LAPACKE_sstein_work( int matrix_layout, lapack_int n, const float* d,
                                const float* e, lapack_int m, const float* w,
                                const lapack_int* iblock,
                                const lapack_int* isplit, float* z,
                                lapack_int ldz, float* work, lapack_int* iwork,
                                lapack_int* ifailv );
lapack_int LAPACKE_dstein_work( int matrix_layout, lapack_int n, const double* d,
                                const double* e, lapack_int m, const double* w,
                                const lapack_int* iblock,
                                const lapack_int* isplit, double* z,
                                lapack_int ldz, double* work, lapack_int* iwork,
                                lapack_int* ifailv );
lapack_int LAPACKE_cstein_work( int matrix_layout, lapack_int n, const float* d,
                                const float* e, lapack_int m, const float* w,
                                const lapack_int* iblock,
                                const lapack_int* isplit,
                                lapack_complex_float* z, lapack_int ldz,
                                float* work, lapack_int* iwork,
                                lapack_int* ifailv );
lapack_int LAPACKE_zstein_work( int matrix_layout, lapack_int n, const double* d,
                                const double* e, lapack_int m, const double* w,
                                const lapack_int* iblock,
                                const lapack_int* isplit,
                                lapack_complex_double* z, lapack_int ldz,
                                double* work, lapack_int* iwork,
                                lapack_int* ifailv );

lapack_int LAPACKE_sstemr_work( int matrix_layout, char jobz, char range,
                                lapack_int n, float* d, float* e, float vl,
                                float vu, lapack_int il, lapack_int iu,
                                lapack_int* m, float* w, float* z,
                                lapack_int ldz, lapack_int nzc,
                                lapack_int* isuppz, lapack_logical* tryrac,
                                float* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );
lapack_int LAPACKE_dstemr_work( int matrix_layout, char jobz, char range,
                                lapack_int n, double* d, double* e, double vl,
                                double vu, lapack_int il, lapack_int iu,
                                lapack_int* m, double* w, double* z,
                                lapack_int ldz, lapack_int nzc,
                                lapack_int* isuppz, lapack_logical* tryrac,
                                double* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );
lapack_int LAPACKE_cstemr_work( int matrix_layout, char jobz, char range,
                                lapack_int n, float* d, float* e, float vl,
                                float vu, lapack_int il, lapack_int iu,
                                lapack_int* m, float* w,
                                lapack_complex_float* z, lapack_int ldz,
                                lapack_int nzc, lapack_int* isuppz,
                                lapack_logical* tryrac, float* work,
                                lapack_int lwork, lapack_int* iwork,
                                lapack_int liwork );
lapack_int LAPACKE_zstemr_work( int matrix_layout, char jobz, char range,
                                lapack_int n, double* d, double* e, double vl,
                                double vu, lapack_int il, lapack_int iu,
                                lapack_int* m, double* w,
                                lapack_complex_double* z, lapack_int ldz,
                                lapack_int nzc, lapack_int* isuppz,
                                lapack_logical* tryrac, double* work,
                                lapack_int lwork, lapack_int* iwork,
                                lapack_int liwork );

lapack_int LAPACKE_ssteqr_work( int matrix_layout, char compz, lapack_int n,
                                float* d, float* e, float* z, lapack_int ldz,
                                float* work );
lapack_int LAPACKE_dsteqr_work( int matrix_layout, char compz, lapack_int n,
                                double* d, double* e, double* z, lapack_int ldz,
                                double* work );
lapack_int LAPACKE_csteqr_work( int matrix_layout, char compz, lapack_int n,
                                float* d, float* e, lapack_complex_float* z,
                                lapack_int ldz, float* work );
lapack_int LAPACKE_zsteqr_work( int matrix_layout, char compz, lapack_int n,
                                double* d, double* e, lapack_complex_double* z,
                                lapack_int ldz, double* work );

lapack_int LAPACKE_ssterf_work( lapack_int n, float* d, float* e );
lapack_int LAPACKE_dsterf_work( lapack_int n, double* d, double* e );

lapack_int LAPACKE_sstev_work( int matrix_layout, char jobz, lapack_int n,
                               float* d, float* e, float* z, lapack_int ldz,
                               float* work );
lapack_int LAPACKE_dstev_work( int matrix_layout, char jobz, lapack_int n,
                               double* d, double* e, double* z, lapack_int ldz,
                               double* work );

lapack_int LAPACKE_sstevd_work( int matrix_layout, char jobz, lapack_int n,
                                float* d, float* e, float* z, lapack_int ldz,
                                float* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );
lapack_int LAPACKE_dstevd_work( int matrix_layout, char jobz, lapack_int n,
                                double* d, double* e, double* z, lapack_int ldz,
                                double* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );

lapack_int LAPACKE_sstevr_work( int matrix_layout, char jobz, char range,
                                lapack_int n, float* d, float* e, float vl,
                                float vu, lapack_int il, lapack_int iu,
                                float abstol, lapack_int* m, float* w, float* z,
                                lapack_int ldz, lapack_int* isuppz, float* work,
                                lapack_int lwork, lapack_int* iwork,
                                lapack_int liwork );
lapack_int LAPACKE_dstevr_work( int matrix_layout, char jobz, char range,
                                lapack_int n, double* d, double* e, double vl,
                                double vu, lapack_int il, lapack_int iu,
                                double abstol, lapack_int* m, double* w,
                                double* z, lapack_int ldz, lapack_int* isuppz,
                                double* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );

lapack_int LAPACKE_sstevx_work( int matrix_layout, char jobz, char range,
                                lapack_int n, float* d, float* e, float vl,
                                float vu, lapack_int il, lapack_int iu,
                                float abstol, lapack_int* m, float* w, float* z,
                                lapack_int ldz, float* work, lapack_int* iwork,
                                lapack_int* ifail );
lapack_int LAPACKE_dstevx_work( int matrix_layout, char jobz, char range,
                                lapack_int n, double* d, double* e, double vl,
                                double vu, lapack_int il, lapack_int iu,
                                double abstol, lapack_int* m, double* w,
                                double* z, lapack_int ldz, double* work,
                                lapack_int* iwork, lapack_int* ifail );

lapack_int LAPACKE_ssycon_work( int matrix_layout, char uplo, lapack_int n,
                                const float* a, lapack_int lda,
                                const lapack_int* ipiv, float anorm,
                                float* rcond, float* work, lapack_int* iwork );
lapack_int LAPACKE_dsycon_work( int matrix_layout, char uplo, lapack_int n,
                                const double* a, lapack_int lda,
                                const lapack_int* ipiv, double anorm,
                                double* rcond, double* work,
                                lapack_int* iwork );
lapack_int LAPACKE_csycon_work( int matrix_layout, char uplo, lapack_int n,
                                const lapack_complex_float* a, lapack_int lda,
                                const lapack_int* ipiv, float anorm,
                                float* rcond, lapack_complex_float* work );
lapack_int LAPACKE_zsycon_work( int matrix_layout, char uplo, lapack_int n,
                                const lapack_complex_double* a, lapack_int lda,
                                const lapack_int* ipiv, double anorm,
                                double* rcond, lapack_complex_double* work );

lapack_int LAPACKE_ssyequb_work( int matrix_layout, char uplo, lapack_int n,
                                 const float* a, lapack_int lda, float* s,
                                 float* scond, float* amax, float* work );
lapack_int LAPACKE_dsyequb_work( int matrix_layout, char uplo, lapack_int n,
                                 const double* a, lapack_int lda, double* s,
                                 double* scond, double* amax, double* work );
lapack_int LAPACKE_csyequb_work( int matrix_layout, char uplo, lapack_int n,
                                 const lapack_complex_float* a, lapack_int lda,
                                 float* s, float* scond, float* amax,
                                 lapack_complex_float* work );
lapack_int LAPACKE_zsyequb_work( int matrix_layout, char uplo, lapack_int n,
                                 const lapack_complex_double* a, lapack_int lda,
                                 double* s, double* scond, double* amax,
                                 lapack_complex_double* work );

lapack_int LAPACKE_ssyev_work( int matrix_layout, char jobz, char uplo,
                               lapack_int n, float* a, lapack_int lda, float* w,
                               float* work, lapack_int lwork );
lapack_int LAPACKE_dsyev_work( int matrix_layout, char jobz, char uplo,
                               lapack_int n, double* a, lapack_int lda,
                               double* w, double* work, lapack_int lwork );

lapack_int LAPACKE_ssyevd_work( int matrix_layout, char jobz, char uplo,
                                lapack_int n, float* a, lapack_int lda,
                                float* w, float* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );
lapack_int LAPACKE_dsyevd_work( int matrix_layout, char jobz, char uplo,
                                lapack_int n, double* a, lapack_int lda,
                                double* w, double* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );

lapack_int LAPACKE_ssyevr_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n, float* a,
                                lapack_int lda, float vl, float vu,
                                lapack_int il, lapack_int iu, float abstol,
                                lapack_int* m, float* w, float* z,
                                lapack_int ldz, lapack_int* isuppz, float* work,
                                lapack_int lwork, lapack_int* iwork,
                                lapack_int liwork );
lapack_int LAPACKE_dsyevr_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n, double* a,
                                lapack_int lda, double vl, double vu,
                                lapack_int il, lapack_int iu, double abstol,
                                lapack_int* m, double* w, double* z,
                                lapack_int ldz, lapack_int* isuppz,
                                double* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );

lapack_int LAPACKE_ssyevx_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n, float* a,
                                lapack_int lda, float vl, float vu,
                                lapack_int il, lapack_int iu, float abstol,
                                lapack_int* m, float* w, float* z,
                                lapack_int ldz, float* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int* ifail );
lapack_int LAPACKE_dsyevx_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n, double* a,
                                lapack_int lda, double vl, double vu,
                                lapack_int il, lapack_int iu, double abstol,
                                lapack_int* m, double* w, double* z,
                                lapack_int ldz, double* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int* ifail );

lapack_int LAPACKE_ssygst_work( int matrix_layout, lapack_int itype, char uplo,
                                lapack_int n, float* a, lapack_int lda,
                                const float* b, lapack_int ldb );
lapack_int LAPACKE_dsygst_work( int matrix_layout, lapack_int itype, char uplo,
                                lapack_int n, double* a, lapack_int lda,
                                const double* b, lapack_int ldb );

lapack_int LAPACKE_ssygv_work( int matrix_layout, lapack_int itype, char jobz,
                               char uplo, lapack_int n, float* a,
                               lapack_int lda, float* b, lapack_int ldb,
                               float* w, float* work, lapack_int lwork );
lapack_int LAPACKE_dsygv_work( int matrix_layout, lapack_int itype, char jobz,
                               char uplo, lapack_int n, double* a,
                               lapack_int lda, double* b, lapack_int ldb,
                               double* w, double* work, lapack_int lwork );

lapack_int LAPACKE_ssygvd_work( int matrix_layout, lapack_int itype, char jobz,
                                char uplo, lapack_int n, float* a,
                                lapack_int lda, float* b, lapack_int ldb,
                                float* w, float* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );
lapack_int LAPACKE_dsygvd_work( int matrix_layout, lapack_int itype, char jobz,
                                char uplo, lapack_int n, double* a,
                                lapack_int lda, double* b, lapack_int ldb,
                                double* w, double* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );

lapack_int LAPACKE_ssygvx_work( int matrix_layout, lapack_int itype, char jobz,
                                char range, char uplo, lapack_int n, float* a,
                                lapack_int lda, float* b, lapack_int ldb,
                                float vl, float vu, lapack_int il,
                                lapack_int iu, float abstol, lapack_int* m,
                                float* w, float* z, lapack_int ldz, float* work,
                                lapack_int lwork, lapack_int* iwork,
                                lapack_int* ifail );
lapack_int LAPACKE_dsygvx_work( int matrix_layout, lapack_int itype, char jobz,
                                char range, char uplo, lapack_int n, double* a,
                                lapack_int lda, double* b, lapack_int ldb,
                                double vl, double vu, lapack_int il,
                                lapack_int iu, double abstol, lapack_int* m,
                                double* w, double* z, lapack_int ldz,
                                double* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int* ifail );

lapack_int LAPACKE_ssyrfs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const float* a, lapack_int lda,
                                const float* af, lapack_int ldaf,
                                const lapack_int* ipiv, const float* b,
                                lapack_int ldb, float* x, lapack_int ldx,
                                float* ferr, float* berr, float* work,
                                lapack_int* iwork );
lapack_int LAPACKE_dsyrfs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const double* a,
                                lapack_int lda, const double* af,
                                lapack_int ldaf, const lapack_int* ipiv,
                                const double* b, lapack_int ldb, double* x,
                                lapack_int ldx, double* ferr, double* berr,
                                double* work, lapack_int* iwork );
lapack_int LAPACKE_csyrfs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const lapack_complex_float* a,
                                lapack_int lda, const lapack_complex_float* af,
                                lapack_int ldaf, const lapack_int* ipiv,
                                const lapack_complex_float* b, lapack_int ldb,
                                lapack_complex_float* x, lapack_int ldx,
                                float* ferr, float* berr,
                                lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_zsyrfs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const lapack_complex_double* a,
                                lapack_int lda, const lapack_complex_double* af,
                                lapack_int ldaf, const lapack_int* ipiv,
                                const lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* x, lapack_int ldx,
                                double* ferr, double* berr,
                                lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_ssyrfsx_work( int matrix_layout, char uplo, char equed,
                                 lapack_int n, lapack_int nrhs, const float* a,
                                 lapack_int lda, const float* af,
                                 lapack_int ldaf, const lapack_int* ipiv,
                                 const float* s, const float* b, lapack_int ldb,
                                 float* x, lapack_int ldx, float* rcond,
                                 float* berr, lapack_int n_err_bnds,
                                 float* err_bnds_norm, float* err_bnds_comp,
                                 lapack_int nparams, float* params, float* work,
                                 lapack_int* iwork );
lapack_int LAPACKE_dsyrfsx_work( int matrix_layout, char uplo, char equed,
                                 lapack_int n, lapack_int nrhs, const double* a,
                                 lapack_int lda, const double* af,
                                 lapack_int ldaf, const lapack_int* ipiv,
                                 const double* s, const double* b,
                                 lapack_int ldb, double* x, lapack_int ldx,
                                 double* rcond, double* berr,
                                 lapack_int n_err_bnds, double* err_bnds_norm,
                                 double* err_bnds_comp, lapack_int nparams,
                                 double* params, double* work,
                                 lapack_int* iwork );
lapack_int LAPACKE_csyrfsx_work( int matrix_layout, char uplo, char equed,
                                 lapack_int n, lapack_int nrhs,
                                 const lapack_complex_float* a, lapack_int lda,
                                 const lapack_complex_float* af,
                                 lapack_int ldaf, const lapack_int* ipiv,
                                 const float* s, const lapack_complex_float* b,
                                 lapack_int ldb, lapack_complex_float* x,
                                 lapack_int ldx, float* rcond, float* berr,
                                 lapack_int n_err_bnds, float* err_bnds_norm,
                                 float* err_bnds_comp, lapack_int nparams,
                                 float* params, lapack_complex_float* work,
                                 float* rwork );
lapack_int LAPACKE_zsyrfsx_work( int matrix_layout, char uplo, char equed,
                                 lapack_int n, lapack_int nrhs,
                                 const lapack_complex_double* a, lapack_int lda,
                                 const lapack_complex_double* af,
                                 lapack_int ldaf, const lapack_int* ipiv,
                                 const double* s,
                                 const lapack_complex_double* b, lapack_int ldb,
                                 lapack_complex_double* x, lapack_int ldx,
                                 double* rcond, double* berr,
                                 lapack_int n_err_bnds, double* err_bnds_norm,
                                 double* err_bnds_comp, lapack_int nparams,
                                 double* params, lapack_complex_double* work,
                                 double* rwork );

lapack_int LAPACKE_ssysv_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, float* a, lapack_int lda,
                               lapack_int* ipiv, float* b, lapack_int ldb,
                               float* work, lapack_int lwork );
lapack_int LAPACKE_dsysv_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, double* a, lapack_int lda,
                               lapack_int* ipiv, double* b, lapack_int ldb,
                               double* work, lapack_int lwork );
lapack_int LAPACKE_csysv_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, lapack_complex_float* a,
                               lapack_int lda, lapack_int* ipiv,
                               lapack_complex_float* b, lapack_int ldb,
                               lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zsysv_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, lapack_complex_double* a,
                               lapack_int lda, lapack_int* ipiv,
                               lapack_complex_double* b, lapack_int ldb,
                               lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_ssysvx_work( int matrix_layout, char fact, char uplo,
                                lapack_int n, lapack_int nrhs, const float* a,
                                lapack_int lda, float* af, lapack_int ldaf,
                                lapack_int* ipiv, const float* b,
                                lapack_int ldb, float* x, lapack_int ldx,
                                float* rcond, float* ferr, float* berr,
                                float* work, lapack_int lwork,
                                lapack_int* iwork );
lapack_int LAPACKE_dsysvx_work( int matrix_layout, char fact, char uplo,
                                lapack_int n, lapack_int nrhs, const double* a,
                                lapack_int lda, double* af, lapack_int ldaf,
                                lapack_int* ipiv, const double* b,
                                lapack_int ldb, double* x, lapack_int ldx,
                                double* rcond, double* ferr, double* berr,
                                double* work, lapack_int lwork,
                                lapack_int* iwork );
lapack_int LAPACKE_csysvx_work( int matrix_layout, char fact, char uplo,
                                lapack_int n, lapack_int nrhs,
                                const lapack_complex_float* a, lapack_int lda,
                                lapack_complex_float* af, lapack_int ldaf,
                                lapack_int* ipiv, const lapack_complex_float* b,
                                lapack_int ldb, lapack_complex_float* x,
                                lapack_int ldx, float* rcond, float* ferr,
                                float* berr, lapack_complex_float* work,
                                lapack_int lwork, float* rwork );
lapack_int LAPACKE_zsysvx_work( int matrix_layout, char fact, char uplo,
                                lapack_int n, lapack_int nrhs,
                                const lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* af, lapack_int ldaf,
                                lapack_int* ipiv,
                                const lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* x, lapack_int ldx,
                                double* rcond, double* ferr, double* berr,
                                lapack_complex_double* work, lapack_int lwork,
                                double* rwork );

lapack_int LAPACKE_ssysvxx_work( int matrix_layout, char fact, char uplo,
                                 lapack_int n, lapack_int nrhs, float* a,
                                 lapack_int lda, float* af, lapack_int ldaf,
                                 lapack_int* ipiv, char* equed, float* s,
                                 float* b, lapack_int ldb, float* x,
                                 lapack_int ldx, float* rcond, float* rpvgrw,
                                 float* berr, lapack_int n_err_bnds,
                                 float* err_bnds_norm, float* err_bnds_comp,
                                 lapack_int nparams, float* params, float* work,
                                 lapack_int* iwork );
lapack_int LAPACKE_dsysvxx_work( int matrix_layout, char fact, char uplo,
                                 lapack_int n, lapack_int nrhs, double* a,
                                 lapack_int lda, double* af, lapack_int ldaf,
                                 lapack_int* ipiv, char* equed, double* s,
                                 double* b, lapack_int ldb, double* x,
                                 lapack_int ldx, double* rcond, double* rpvgrw,
                                 double* berr, lapack_int n_err_bnds,
                                 double* err_bnds_norm, double* err_bnds_comp,
                                 lapack_int nparams, double* params,
                                 double* work, lapack_int* iwork );
lapack_int LAPACKE_csysvxx_work( int matrix_layout, char fact, char uplo,
                                 lapack_int n, lapack_int nrhs,
                                 lapack_complex_float* a, lapack_int lda,
                                 lapack_complex_float* af, lapack_int ldaf,
                                 lapack_int* ipiv, char* equed, float* s,
                                 lapack_complex_float* b, lapack_int ldb,
                                 lapack_complex_float* x, lapack_int ldx,
                                 float* rcond, float* rpvgrw, float* berr,
                                 lapack_int n_err_bnds, float* err_bnds_norm,
                                 float* err_bnds_comp, lapack_int nparams,
                                 float* params, lapack_complex_float* work,
                                 float* rwork );
lapack_int LAPACKE_zsysvxx_work( int matrix_layout, char fact, char uplo,
                                 lapack_int n, lapack_int nrhs,
                                 lapack_complex_double* a, lapack_int lda,
                                 lapack_complex_double* af, lapack_int ldaf,
                                 lapack_int* ipiv, char* equed, double* s,
                                 lapack_complex_double* b, lapack_int ldb,
                                 lapack_complex_double* x, lapack_int ldx,
                                 double* rcond, double* rpvgrw, double* berr,
                                 lapack_int n_err_bnds, double* err_bnds_norm,
                                 double* err_bnds_comp, lapack_int nparams,
                                 double* params, lapack_complex_double* work,
                                 double* rwork );

lapack_int LAPACKE_ssytrd_work( int matrix_layout, char uplo, lapack_int n,
                                float* a, lapack_int lda, float* d, float* e,
                                float* tau, float* work, lapack_int lwork );
lapack_int LAPACKE_dsytrd_work( int matrix_layout, char uplo, lapack_int n,
                                double* a, lapack_int lda, double* d, double* e,
                                double* tau, double* work, lapack_int lwork );

lapack_int LAPACKE_ssytrf_work( int matrix_layout, char uplo, lapack_int n,
                                float* a, lapack_int lda, lapack_int* ipiv,
                                float* work, lapack_int lwork );
lapack_int LAPACKE_dsytrf_work( int matrix_layout, char uplo, lapack_int n,
                                double* a, lapack_int lda, lapack_int* ipiv,
                                double* work, lapack_int lwork );
lapack_int LAPACKE_csytrf_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_int* ipiv, lapack_complex_float* work,
                                lapack_int lwork );
lapack_int LAPACKE_zsytrf_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_int* ipiv, lapack_complex_double* work,
                                lapack_int lwork );

lapack_int LAPACKE_ssytri_work( int matrix_layout, char uplo, lapack_int n,
                                float* a, lapack_int lda,
                                const lapack_int* ipiv, float* work );
lapack_int LAPACKE_dsytri_work( int matrix_layout, char uplo, lapack_int n,
                                double* a, lapack_int lda,
                                const lapack_int* ipiv, double* work );
lapack_int LAPACKE_csytri_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                const lapack_int* ipiv,
                                lapack_complex_float* work );
lapack_int LAPACKE_zsytri_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                const lapack_int* ipiv,
                                lapack_complex_double* work );

lapack_int LAPACKE_ssytrs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const float* a, lapack_int lda,
                                const lapack_int* ipiv, float* b,
                                lapack_int ldb );
lapack_int LAPACKE_dsytrs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const double* a,
                                lapack_int lda, const lapack_int* ipiv,
                                double* b, lapack_int ldb );
lapack_int LAPACKE_csytrs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const lapack_complex_float* a,
                                lapack_int lda, const lapack_int* ipiv,
                                lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zsytrs_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const lapack_complex_double* a,
                                lapack_int lda, const lapack_int* ipiv,
                                lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_stbcon_work( int matrix_layout, char norm, char uplo,
                                char diag, lapack_int n, lapack_int kd,
                                const float* ab, lapack_int ldab, float* rcond,
                                float* work, lapack_int* iwork );
lapack_int LAPACKE_dtbcon_work( int matrix_layout, char norm, char uplo,
                                char diag, lapack_int n, lapack_int kd,
                                const double* ab, lapack_int ldab,
                                double* rcond, double* work,
                                lapack_int* iwork );
lapack_int LAPACKE_ctbcon_work( int matrix_layout, char norm, char uplo,
                                char diag, lapack_int n, lapack_int kd,
                                const lapack_complex_float* ab, lapack_int ldab,
                                float* rcond, lapack_complex_float* work,
                                float* rwork );
lapack_int LAPACKE_ztbcon_work( int matrix_layout, char norm, char uplo,
                                char diag, lapack_int n, lapack_int kd,
                                const lapack_complex_double* ab,
                                lapack_int ldab, double* rcond,
                                lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_stbrfs_work( int matrix_layout, char uplo, char trans,
                                char diag, lapack_int n, lapack_int kd,
                                lapack_int nrhs, const float* ab,
                                lapack_int ldab, const float* b, lapack_int ldb,
                                const float* x, lapack_int ldx, float* ferr,
                                float* berr, float* work, lapack_int* iwork );
lapack_int LAPACKE_dtbrfs_work( int matrix_layout, char uplo, char trans,
                                char diag, lapack_int n, lapack_int kd,
                                lapack_int nrhs, const double* ab,
                                lapack_int ldab, const double* b,
                                lapack_int ldb, const double* x, lapack_int ldx,
                                double* ferr, double* berr, double* work,
                                lapack_int* iwork );
lapack_int LAPACKE_ctbrfs_work( int matrix_layout, char uplo, char trans,
                                char diag, lapack_int n, lapack_int kd,
                                lapack_int nrhs, const lapack_complex_float* ab,
                                lapack_int ldab, const lapack_complex_float* b,
                                lapack_int ldb, const lapack_complex_float* x,
                                lapack_int ldx, float* ferr, float* berr,
                                lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_ztbrfs_work( int matrix_layout, char uplo, char trans,
                                char diag, lapack_int n, lapack_int kd,
                                lapack_int nrhs,
                                const lapack_complex_double* ab,
                                lapack_int ldab, const lapack_complex_double* b,
                                lapack_int ldb, const lapack_complex_double* x,
                                lapack_int ldx, double* ferr, double* berr,
                                lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_stbtrs_work( int matrix_layout, char uplo, char trans,
                                char diag, lapack_int n, lapack_int kd,
                                lapack_int nrhs, const float* ab,
                                lapack_int ldab, float* b, lapack_int ldb );
lapack_int LAPACKE_dtbtrs_work( int matrix_layout, char uplo, char trans,
                                char diag, lapack_int n, lapack_int kd,
                                lapack_int nrhs, const double* ab,
                                lapack_int ldab, double* b, lapack_int ldb );
lapack_int LAPACKE_ctbtrs_work( int matrix_layout, char uplo, char trans,
                                char diag, lapack_int n, lapack_int kd,
                                lapack_int nrhs, const lapack_complex_float* ab,
                                lapack_int ldab, lapack_complex_float* b,
                                lapack_int ldb );
lapack_int LAPACKE_ztbtrs_work( int matrix_layout, char uplo, char trans,
                                char diag, lapack_int n, lapack_int kd,
                                lapack_int nrhs,
                                const lapack_complex_double* ab,
                                lapack_int ldab, lapack_complex_double* b,
                                lapack_int ldb );

lapack_int LAPACKE_stfsm_work( int matrix_layout, char transr, char side,
                               char uplo, char trans, char diag, lapack_int m,
                               lapack_int n, float alpha, const float* a,
                               float* b, lapack_int ldb );
lapack_int LAPACKE_dtfsm_work( int matrix_layout, char transr, char side,
                               char uplo, char trans, char diag, lapack_int m,
                               lapack_int n, double alpha, const double* a,
                               double* b, lapack_int ldb );
lapack_int LAPACKE_ctfsm_work( int matrix_layout, char transr, char side,
                               char uplo, char trans, char diag, lapack_int m,
                               lapack_int n, lapack_complex_float alpha,
                               const lapack_complex_float* a,
                               lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_ztfsm_work( int matrix_layout, char transr, char side,
                               char uplo, char trans, char diag, lapack_int m,
                               lapack_int n, lapack_complex_double alpha,
                               const lapack_complex_double* a,
                               lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_stftri_work( int matrix_layout, char transr, char uplo,
                                char diag, lapack_int n, float* a );
lapack_int LAPACKE_dtftri_work( int matrix_layout, char transr, char uplo,
                                char diag, lapack_int n, double* a );
lapack_int LAPACKE_ctftri_work( int matrix_layout, char transr, char uplo,
                                char diag, lapack_int n,
                                lapack_complex_float* a );
lapack_int LAPACKE_ztftri_work( int matrix_layout, char transr, char uplo,
                                char diag, lapack_int n,
                                lapack_complex_double* a );

lapack_int LAPACKE_stfttp_work( int matrix_layout, char transr, char uplo,
                                lapack_int n, const float* arf, float* ap );
lapack_int LAPACKE_dtfttp_work( int matrix_layout, char transr, char uplo,
                                lapack_int n, const double* arf, double* ap );
lapack_int LAPACKE_ctfttp_work( int matrix_layout, char transr, char uplo,
                                lapack_int n, const lapack_complex_float* arf,
                                lapack_complex_float* ap );
lapack_int LAPACKE_ztfttp_work( int matrix_layout, char transr, char uplo,
                                lapack_int n, const lapack_complex_double* arf,
                                lapack_complex_double* ap );

lapack_int LAPACKE_stfttr_work( int matrix_layout, char transr, char uplo,
                                lapack_int n, const float* arf, float* a,
                                lapack_int lda );
lapack_int LAPACKE_dtfttr_work( int matrix_layout, char transr, char uplo,
                                lapack_int n, const double* arf, double* a,
                                lapack_int lda );
lapack_int LAPACKE_ctfttr_work( int matrix_layout, char transr, char uplo,
                                lapack_int n, const lapack_complex_float* arf,
                                lapack_complex_float* a, lapack_int lda );
lapack_int LAPACKE_ztfttr_work( int matrix_layout, char transr, char uplo,
                                lapack_int n, const lapack_complex_double* arf,
                                lapack_complex_double* a, lapack_int lda );

lapack_int LAPACKE_stgevc_work( int matrix_layout, char side, char howmny,
                                const lapack_logical* select, lapack_int n,
                                const float* s, lapack_int lds, const float* p,
                                lapack_int ldp, float* vl, lapack_int ldvl,
                                float* vr, lapack_int ldvr, lapack_int mm,
                                lapack_int* m, float* work );
lapack_int LAPACKE_dtgevc_work( int matrix_layout, char side, char howmny,
                                const lapack_logical* select, lapack_int n,
                                const double* s, lapack_int lds,
                                const double* p, lapack_int ldp, double* vl,
                                lapack_int ldvl, double* vr, lapack_int ldvr,
                                lapack_int mm, lapack_int* m, double* work );
lapack_int LAPACKE_ctgevc_work( int matrix_layout, char side, char howmny,
                                const lapack_logical* select, lapack_int n,
                                const lapack_complex_float* s, lapack_int lds,
                                const lapack_complex_float* p, lapack_int ldp,
                                lapack_complex_float* vl, lapack_int ldvl,
                                lapack_complex_float* vr, lapack_int ldvr,
                                lapack_int mm, lapack_int* m,
                                lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_ztgevc_work( int matrix_layout, char side, char howmny,
                                const lapack_logical* select, lapack_int n,
                                const lapack_complex_double* s, lapack_int lds,
                                const lapack_complex_double* p, lapack_int ldp,
                                lapack_complex_double* vl, lapack_int ldvl,
                                lapack_complex_double* vr, lapack_int ldvr,
                                lapack_int mm, lapack_int* m,
                                lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_stgexc_work( int matrix_layout, lapack_logical wantq,
                                lapack_logical wantz, lapack_int n, float* a,
                                lapack_int lda, float* b, lapack_int ldb,
                                float* q, lapack_int ldq, float* z,
                                lapack_int ldz, lapack_int* ifst,
                                lapack_int* ilst, float* work,
                                lapack_int lwork );
lapack_int LAPACKE_dtgexc_work( int matrix_layout, lapack_logical wantq,
                                lapack_logical wantz, lapack_int n, double* a,
                                lapack_int lda, double* b, lapack_int ldb,
                                double* q, lapack_int ldq, double* z,
                                lapack_int ldz, lapack_int* ifst,
                                lapack_int* ilst, double* work,
                                lapack_int lwork );
lapack_int LAPACKE_ctgexc_work( int matrix_layout, lapack_logical wantq,
                                lapack_logical wantz, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_complex_float* b, lapack_int ldb,
                                lapack_complex_float* q, lapack_int ldq,
                                lapack_complex_float* z, lapack_int ldz,
                                lapack_int ifst, lapack_int ilst );
lapack_int LAPACKE_ztgexc_work( int matrix_layout, lapack_logical wantq,
                                lapack_logical wantz, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* q, lapack_int ldq,
                                lapack_complex_double* z, lapack_int ldz,
                                lapack_int ifst, lapack_int ilst );

lapack_int LAPACKE_stgsen_work( int matrix_layout, lapack_int ijob,
                                lapack_logical wantq, lapack_logical wantz,
                                const lapack_logical* select, lapack_int n,
                                float* a, lapack_int lda, float* b,
                                lapack_int ldb, float* alphar, float* alphai,
                                float* beta, float* q, lapack_int ldq, float* z,
                                lapack_int ldz, lapack_int* m, float* pl,
                                float* pr, float* dif, float* work,
                                lapack_int lwork, lapack_int* iwork,
                                lapack_int liwork );
lapack_int LAPACKE_dtgsen_work( int matrix_layout, lapack_int ijob,
                                lapack_logical wantq, lapack_logical wantz,
                                const lapack_logical* select, lapack_int n,
                                double* a, lapack_int lda, double* b,
                                lapack_int ldb, double* alphar, double* alphai,
                                double* beta, double* q, lapack_int ldq,
                                double* z, lapack_int ldz, lapack_int* m,
                                double* pl, double* pr, double* dif,
                                double* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );
lapack_int LAPACKE_ctgsen_work( int matrix_layout, lapack_int ijob,
                                lapack_logical wantq, lapack_logical wantz,
                                const lapack_logical* select, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_complex_float* b, lapack_int ldb,
                                lapack_complex_float* alpha,
                                lapack_complex_float* beta,
                                lapack_complex_float* q, lapack_int ldq,
                                lapack_complex_float* z, lapack_int ldz,
                                lapack_int* m, float* pl, float* pr, float* dif,
                                lapack_complex_float* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );
lapack_int LAPACKE_ztgsen_work( int matrix_layout, lapack_int ijob,
                                lapack_logical wantq, lapack_logical wantz,
                                const lapack_logical* select, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* alpha,
                                lapack_complex_double* beta,
                                lapack_complex_double* q, lapack_int ldq,
                                lapack_complex_double* z, lapack_int ldz,
                                lapack_int* m, double* pl, double* pr,
                                double* dif, lapack_complex_double* work,
                                lapack_int lwork, lapack_int* iwork,
                                lapack_int liwork );

lapack_int LAPACKE_stgsja_work( int matrix_layout, char jobu, char jobv,
                                char jobq, lapack_int m, lapack_int p,
                                lapack_int n, lapack_int k, lapack_int l,
                                float* a, lapack_int lda, float* b,
                                lapack_int ldb, float tola, float tolb,
                                float* alpha, float* beta, float* u,
                                lapack_int ldu, float* v, lapack_int ldv,
                                float* q, lapack_int ldq, float* work,
                                lapack_int* ncycle );
lapack_int LAPACKE_dtgsja_work( int matrix_layout, char jobu, char jobv,
                                char jobq, lapack_int m, lapack_int p,
                                lapack_int n, lapack_int k, lapack_int l,
                                double* a, lapack_int lda, double* b,
                                lapack_int ldb, double tola, double tolb,
                                double* alpha, double* beta, double* u,
                                lapack_int ldu, double* v, lapack_int ldv,
                                double* q, lapack_int ldq, double* work,
                                lapack_int* ncycle );
lapack_int LAPACKE_ctgsja_work( int matrix_layout, char jobu, char jobv,
                                char jobq, lapack_int m, lapack_int p,
                                lapack_int n, lapack_int k, lapack_int l,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_complex_float* b, lapack_int ldb,
                                float tola, float tolb, float* alpha,
                                float* beta, lapack_complex_float* u,
                                lapack_int ldu, lapack_complex_float* v,
                                lapack_int ldv, lapack_complex_float* q,
                                lapack_int ldq, lapack_complex_float* work,
                                lapack_int* ncycle );
lapack_int LAPACKE_ztgsja_work( int matrix_layout, char jobu, char jobv,
                                char jobq, lapack_int m, lapack_int p,
                                lapack_int n, lapack_int k, lapack_int l,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* b, lapack_int ldb,
                                double tola, double tolb, double* alpha,
                                double* beta, lapack_complex_double* u,
                                lapack_int ldu, lapack_complex_double* v,
                                lapack_int ldv, lapack_complex_double* q,
                                lapack_int ldq, lapack_complex_double* work,
                                lapack_int* ncycle );

lapack_int LAPACKE_stgsna_work( int matrix_layout, char job, char howmny,
                                const lapack_logical* select, lapack_int n,
                                const float* a, lapack_int lda, const float* b,
                                lapack_int ldb, const float* vl,
                                lapack_int ldvl, const float* vr,
                                lapack_int ldvr, float* s, float* dif,
                                lapack_int mm, lapack_int* m, float* work,
                                lapack_int lwork, lapack_int* iwork );
lapack_int LAPACKE_dtgsna_work( int matrix_layout, char job, char howmny,
                                const lapack_logical* select, lapack_int n,
                                const double* a, lapack_int lda,
                                const double* b, lapack_int ldb,
                                const double* vl, lapack_int ldvl,
                                const double* vr, lapack_int ldvr, double* s,
                                double* dif, lapack_int mm, lapack_int* m,
                                double* work, lapack_int lwork,
                                lapack_int* iwork );
lapack_int LAPACKE_ctgsna_work( int matrix_layout, char job, char howmny,
                                const lapack_logical* select, lapack_int n,
                                const lapack_complex_float* a, lapack_int lda,
                                const lapack_complex_float* b, lapack_int ldb,
                                const lapack_complex_float* vl, lapack_int ldvl,
                                const lapack_complex_float* vr, lapack_int ldvr,
                                float* s, float* dif, lapack_int mm,
                                lapack_int* m, lapack_complex_float* work,
                                lapack_int lwork, lapack_int* iwork );
lapack_int LAPACKE_ztgsna_work( int matrix_layout, char job, char howmny,
                                const lapack_logical* select, lapack_int n,
                                const lapack_complex_double* a, lapack_int lda,
                                const lapack_complex_double* b, lapack_int ldb,
                                const lapack_complex_double* vl,
                                lapack_int ldvl,
                                const lapack_complex_double* vr,
                                lapack_int ldvr, double* s, double* dif,
                                lapack_int mm, lapack_int* m,
                                lapack_complex_double* work, lapack_int lwork,
                                lapack_int* iwork );

lapack_int LAPACKE_stgsyl_work( int matrix_layout, char trans, lapack_int ijob,
                                lapack_int m, lapack_int n, const float* a,
                                lapack_int lda, const float* b, lapack_int ldb,
                                float* c, lapack_int ldc, const float* d,
                                lapack_int ldd, const float* e, lapack_int lde,
                                float* f, lapack_int ldf, float* scale,
                                float* dif, float* work, lapack_int lwork,
                                lapack_int* iwork );
lapack_int LAPACKE_dtgsyl_work( int matrix_layout, char trans, lapack_int ijob,
                                lapack_int m, lapack_int n, const double* a,
                                lapack_int lda, const double* b, lapack_int ldb,
                                double* c, lapack_int ldc, const double* d,
                                lapack_int ldd, const double* e, lapack_int lde,
                                double* f, lapack_int ldf, double* scale,
                                double* dif, double* work, lapack_int lwork,
                                lapack_int* iwork );
lapack_int LAPACKE_ctgsyl_work( int matrix_layout, char trans, lapack_int ijob,
                                lapack_int m, lapack_int n,
                                const lapack_complex_float* a, lapack_int lda,
                                const lapack_complex_float* b, lapack_int ldb,
                                lapack_complex_float* c, lapack_int ldc,
                                const lapack_complex_float* d, lapack_int ldd,
                                const lapack_complex_float* e, lapack_int lde,
                                lapack_complex_float* f, lapack_int ldf,
                                float* scale, float* dif,
                                lapack_complex_float* work, lapack_int lwork,
                                lapack_int* iwork );
lapack_int LAPACKE_ztgsyl_work( int matrix_layout, char trans, lapack_int ijob,
                                lapack_int m, lapack_int n,
                                const lapack_complex_double* a, lapack_int lda,
                                const lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* c, lapack_int ldc,
                                const lapack_complex_double* d, lapack_int ldd,
                                const lapack_complex_double* e, lapack_int lde,
                                lapack_complex_double* f, lapack_int ldf,
                                double* scale, double* dif,
                                lapack_complex_double* work, lapack_int lwork,
                                lapack_int* iwork );

lapack_int LAPACKE_stpcon_work( int matrix_layout, char norm, char uplo,
                                char diag, lapack_int n, const float* ap,
                                float* rcond, float* work, lapack_int* iwork );
lapack_int LAPACKE_dtpcon_work( int matrix_layout, char norm, char uplo,
                                char diag, lapack_int n, const double* ap,
                                double* rcond, double* work,
                                lapack_int* iwork );
lapack_int LAPACKE_ctpcon_work( int matrix_layout, char norm, char uplo,
                                char diag, lapack_int n,
                                const lapack_complex_float* ap, float* rcond,
                                lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_ztpcon_work( int matrix_layout, char norm, char uplo,
                                char diag, lapack_int n,
                                const lapack_complex_double* ap, double* rcond,
                                lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_stprfs_work( int matrix_layout, char uplo, char trans,
                                char diag, lapack_int n, lapack_int nrhs,
                                const float* ap, const float* b, lapack_int ldb,
                                const float* x, lapack_int ldx, float* ferr,
                                float* berr, float* work, lapack_int* iwork );
lapack_int LAPACKE_dtprfs_work( int matrix_layout, char uplo, char trans,
                                char diag, lapack_int n, lapack_int nrhs,
                                const double* ap, const double* b,
                                lapack_int ldb, const double* x, lapack_int ldx,
                                double* ferr, double* berr, double* work,
                                lapack_int* iwork );
lapack_int LAPACKE_ctprfs_work( int matrix_layout, char uplo, char trans,
                                char diag, lapack_int n, lapack_int nrhs,
                                const lapack_complex_float* ap,
                                const lapack_complex_float* b, lapack_int ldb,
                                const lapack_complex_float* x, lapack_int ldx,
                                float* ferr, float* berr,
                                lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_ztprfs_work( int matrix_layout, char uplo, char trans,
                                char diag, lapack_int n, lapack_int nrhs,
                                const lapack_complex_double* ap,
                                const lapack_complex_double* b, lapack_int ldb,
                                const lapack_complex_double* x, lapack_int ldx,
                                double* ferr, double* berr,
                                lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_stptri_work( int matrix_layout, char uplo, char diag,
                                lapack_int n, float* ap );
lapack_int LAPACKE_dtptri_work( int matrix_layout, char uplo, char diag,
                                lapack_int n, double* ap );
lapack_int LAPACKE_ctptri_work( int matrix_layout, char uplo, char diag,
                                lapack_int n, lapack_complex_float* ap );
lapack_int LAPACKE_ztptri_work( int matrix_layout, char uplo, char diag,
                                lapack_int n, lapack_complex_double* ap );

lapack_int LAPACKE_stptrs_work( int matrix_layout, char uplo, char trans,
                                char diag, lapack_int n, lapack_int nrhs,
                                const float* ap, float* b, lapack_int ldb );
lapack_int LAPACKE_dtptrs_work( int matrix_layout, char uplo, char trans,
                                char diag, lapack_int n, lapack_int nrhs,
                                const double* ap, double* b, lapack_int ldb );
lapack_int LAPACKE_ctptrs_work( int matrix_layout, char uplo, char trans,
                                char diag, lapack_int n, lapack_int nrhs,
                                const lapack_complex_float* ap,
                                lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_ztptrs_work( int matrix_layout, char uplo, char trans,
                                char diag, lapack_int n, lapack_int nrhs,
                                const lapack_complex_double* ap,
                                lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_stpttf_work( int matrix_layout, char transr, char uplo,
                                lapack_int n, const float* ap, float* arf );
lapack_int LAPACKE_dtpttf_work( int matrix_layout, char transr, char uplo,
                                lapack_int n, const double* ap, double* arf );
lapack_int LAPACKE_ctpttf_work( int matrix_layout, char transr, char uplo,
                                lapack_int n, const lapack_complex_float* ap,
                                lapack_complex_float* arf );
lapack_int LAPACKE_ztpttf_work( int matrix_layout, char transr, char uplo,
                                lapack_int n, const lapack_complex_double* ap,
                                lapack_complex_double* arf );

lapack_int LAPACKE_stpttr_work( int matrix_layout, char uplo, lapack_int n,
                                const float* ap, float* a, lapack_int lda );
lapack_int LAPACKE_dtpttr_work( int matrix_layout, char uplo, lapack_int n,
                                const double* ap, double* a, lapack_int lda );
lapack_int LAPACKE_ctpttr_work( int matrix_layout, char uplo, lapack_int n,
                                const lapack_complex_float* ap,
                                lapack_complex_float* a, lapack_int lda );
lapack_int LAPACKE_ztpttr_work( int matrix_layout, char uplo, lapack_int n,
                                const lapack_complex_double* ap,
                                lapack_complex_double* a, lapack_int lda );

lapack_int LAPACKE_strcon_work( int matrix_layout, char norm, char uplo,
                                char diag, lapack_int n, const float* a,
                                lapack_int lda, float* rcond, float* work,
                                lapack_int* iwork );
lapack_int LAPACKE_dtrcon_work( int matrix_layout, char norm, char uplo,
                                char diag, lapack_int n, const double* a,
                                lapack_int lda, double* rcond, double* work,
                                lapack_int* iwork );
lapack_int LAPACKE_ctrcon_work( int matrix_layout, char norm, char uplo,
                                char diag, lapack_int n,
                                const lapack_complex_float* a, lapack_int lda,
                                float* rcond, lapack_complex_float* work,
                                float* rwork );
lapack_int LAPACKE_ztrcon_work( int matrix_layout, char norm, char uplo,
                                char diag, lapack_int n,
                                const lapack_complex_double* a, lapack_int lda,
                                double* rcond, lapack_complex_double* work,
                                double* rwork );

lapack_int LAPACKE_strevc_work( int matrix_layout, char side, char howmny,
                                lapack_logical* select, lapack_int n,
                                const float* t, lapack_int ldt, float* vl,
                                lapack_int ldvl, float* vr, lapack_int ldvr,
                                lapack_int mm, lapack_int* m, float* work );
lapack_int LAPACKE_dtrevc_work( int matrix_layout, char side, char howmny,
                                lapack_logical* select, lapack_int n,
                                const double* t, lapack_int ldt, double* vl,
                                lapack_int ldvl, double* vr, lapack_int ldvr,
                                lapack_int mm, lapack_int* m, double* work );
lapack_int LAPACKE_ctrevc_work( int matrix_layout, char side, char howmny,
                                const lapack_logical* select, lapack_int n,
                                lapack_complex_float* t, lapack_int ldt,
                                lapack_complex_float* vl, lapack_int ldvl,
                                lapack_complex_float* vr, lapack_int ldvr,
                                lapack_int mm, lapack_int* m,
                                lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_ztrevc_work( int matrix_layout, char side, char howmny,
                                const lapack_logical* select, lapack_int n,
                                lapack_complex_double* t, lapack_int ldt,
                                lapack_complex_double* vl, lapack_int ldvl,
                                lapack_complex_double* vr, lapack_int ldvr,
                                lapack_int mm, lapack_int* m,
                                lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_strexc_work( int matrix_layout, char compq, lapack_int n,
                                float* t, lapack_int ldt, float* q,
                                lapack_int ldq, lapack_int* ifst,
                                lapack_int* ilst, float* work );
lapack_int LAPACKE_dtrexc_work( int matrix_layout, char compq, lapack_int n,
                                double* t, lapack_int ldt, double* q,
                                lapack_int ldq, lapack_int* ifst,
                                lapack_int* ilst, double* work );
lapack_int LAPACKE_ctrexc_work( int matrix_layout, char compq, lapack_int n,
                                lapack_complex_float* t, lapack_int ldt,
                                lapack_complex_float* q, lapack_int ldq,
                                lapack_int ifst, lapack_int ilst );
lapack_int LAPACKE_ztrexc_work( int matrix_layout, char compq, lapack_int n,
                                lapack_complex_double* t, lapack_int ldt,
                                lapack_complex_double* q, lapack_int ldq,
                                lapack_int ifst, lapack_int ilst );

lapack_int LAPACKE_strrfs_work( int matrix_layout, char uplo, char trans,
                                char diag, lapack_int n, lapack_int nrhs,
                                const float* a, lapack_int lda, const float* b,
                                lapack_int ldb, const float* x, lapack_int ldx,
                                float* ferr, float* berr, float* work,
                                lapack_int* iwork );
lapack_int LAPACKE_dtrrfs_work( int matrix_layout, char uplo, char trans,
                                char diag, lapack_int n, lapack_int nrhs,
                                const double* a, lapack_int lda,
                                const double* b, lapack_int ldb,
                                const double* x, lapack_int ldx, double* ferr,
                                double* berr, double* work, lapack_int* iwork );
lapack_int LAPACKE_ctrrfs_work( int matrix_layout, char uplo, char trans,
                                char diag, lapack_int n, lapack_int nrhs,
                                const lapack_complex_float* a, lapack_int lda,
                                const lapack_complex_float* b, lapack_int ldb,
                                const lapack_complex_float* x, lapack_int ldx,
                                float* ferr, float* berr,
                                lapack_complex_float* work, float* rwork );
lapack_int LAPACKE_ztrrfs_work( int matrix_layout, char uplo, char trans,
                                char diag, lapack_int n, lapack_int nrhs,
                                const lapack_complex_double* a, lapack_int lda,
                                const lapack_complex_double* b, lapack_int ldb,
                                const lapack_complex_double* x, lapack_int ldx,
                                double* ferr, double* berr,
                                lapack_complex_double* work, double* rwork );

lapack_int LAPACKE_strsen_work( int matrix_layout, char job, char compq,
                                const lapack_logical* select, lapack_int n,
                                float* t, lapack_int ldt, float* q,
                                lapack_int ldq, float* wr, float* wi,
                                lapack_int* m, float* s, float* sep,
                                float* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );
lapack_int LAPACKE_dtrsen_work( int matrix_layout, char job, char compq,
                                const lapack_logical* select, lapack_int n,
                                double* t, lapack_int ldt, double* q,
                                lapack_int ldq, double* wr, double* wi,
                                lapack_int* m, double* s, double* sep,
                                double* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );
lapack_int LAPACKE_ctrsen_work( int matrix_layout, char job, char compq,
                                const lapack_logical* select, lapack_int n,
                                lapack_complex_float* t, lapack_int ldt,
                                lapack_complex_float* q, lapack_int ldq,
                                lapack_complex_float* w, lapack_int* m,
                                float* s, float* sep,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_ztrsen_work( int matrix_layout, char job, char compq,
                                const lapack_logical* select, lapack_int n,
                                lapack_complex_double* t, lapack_int ldt,
                                lapack_complex_double* q, lapack_int ldq,
                                lapack_complex_double* w, lapack_int* m,
                                double* s, double* sep,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_strsna_work( int matrix_layout, char job, char howmny,
                                const lapack_logical* select, lapack_int n,
                                const float* t, lapack_int ldt, const float* vl,
                                lapack_int ldvl, const float* vr,
                                lapack_int ldvr, float* s, float* sep,
                                lapack_int mm, lapack_int* m, float* work,
                                lapack_int ldwork, lapack_int* iwork );
lapack_int LAPACKE_dtrsna_work( int matrix_layout, char job, char howmny,
                                const lapack_logical* select, lapack_int n,
                                const double* t, lapack_int ldt,
                                const double* vl, lapack_int ldvl,
                                const double* vr, lapack_int ldvr, double* s,
                                double* sep, lapack_int mm, lapack_int* m,
                                double* work, lapack_int ldwork,
                                lapack_int* iwork );
lapack_int LAPACKE_ctrsna_work( int matrix_layout, char job, char howmny,
                                const lapack_logical* select, lapack_int n,
                                const lapack_complex_float* t, lapack_int ldt,
                                const lapack_complex_float* vl, lapack_int ldvl,
                                const lapack_complex_float* vr, lapack_int ldvr,
                                float* s, float* sep, lapack_int mm,
                                lapack_int* m, lapack_complex_float* work,
                                lapack_int ldwork, float* rwork );
lapack_int LAPACKE_ztrsna_work( int matrix_layout, char job, char howmny,
                                const lapack_logical* select, lapack_int n,
                                const lapack_complex_double* t, lapack_int ldt,
                                const lapack_complex_double* vl,
                                lapack_int ldvl,
                                const lapack_complex_double* vr,
                                lapack_int ldvr, double* s, double* sep,
                                lapack_int mm, lapack_int* m,
                                lapack_complex_double* work, lapack_int ldwork,
                                double* rwork );

lapack_int LAPACKE_strsyl_work( int matrix_layout, char trana, char tranb,
                                lapack_int isgn, lapack_int m, lapack_int n,
                                const float* a, lapack_int lda, const float* b,
                                lapack_int ldb, float* c, lapack_int ldc,
                                float* scale );
lapack_int LAPACKE_dtrsyl_work( int matrix_layout, char trana, char tranb,
                                lapack_int isgn, lapack_int m, lapack_int n,
                                const double* a, lapack_int lda,
                                const double* b, lapack_int ldb, double* c,
                                lapack_int ldc, double* scale );
lapack_int LAPACKE_ctrsyl_work( int matrix_layout, char trana, char tranb,
                                lapack_int isgn, lapack_int m, lapack_int n,
                                const lapack_complex_float* a, lapack_int lda,
                                const lapack_complex_float* b, lapack_int ldb,
                                lapack_complex_float* c, lapack_int ldc,
                                float* scale );
lapack_int LAPACKE_ztrsyl_work( int matrix_layout, char trana, char tranb,
                                lapack_int isgn, lapack_int m, lapack_int n,
                                const lapack_complex_double* a, lapack_int lda,
                                const lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* c, lapack_int ldc,
                                double* scale );

lapack_int LAPACKE_strsyl3_work( int matrix_layout, char trana, char tranb,
                                 lapack_int isgn, lapack_int m, lapack_int n,
                                 const float* a, lapack_int lda,
                                 const float* b, lapack_int ldb,
                                 float* c, lapack_int ldc, float* scale,
                                 lapack_int* iwork, lapack_int liwork,
                                 float* swork, lapack_int ldswork );
lapack_int LAPACKE_dtrsyl3_work( int matrix_layout, char trana, char tranb,
                                 lapack_int isgn, lapack_int m, lapack_int n,
                                 const double* a, lapack_int lda,
                                 const double* b, lapack_int ldb,
                                 double* c, lapack_int ldc, double* scale,
                                 lapack_int* iwork, lapack_int liwork,
                                 double* swork, lapack_int ldswork );
lapack_int LAPACKE_ctrsyl3_work( int matrix_layout, char trana, char tranb,
                                 lapack_int isgn, lapack_int m, lapack_int n,
                                 const lapack_complex_float* a, lapack_int lda,
                                 const lapack_complex_float* b, lapack_int ldb,
                                 lapack_complex_float* c, lapack_int ldc,
                                 float* scale, float* swork,
                                 lapack_int ldswork );
lapack_int LAPACKE_ztrsyl3_work( int matrix_layout, char trana, char tranb,
                                 lapack_int isgn, lapack_int m, lapack_int n,
                                 const lapack_complex_double* a, lapack_int lda,
                                 const lapack_complex_double* b, lapack_int ldb,
                                 lapack_complex_double* c, lapack_int ldc,
                                 double* scale, double* swork,
                                 lapack_int ldswork );

lapack_int LAPACKE_strtri_work( int matrix_layout, char uplo, char diag,
                                lapack_int n, float* a, lapack_int lda );
lapack_int LAPACKE_dtrtri_work( int matrix_layout, char uplo, char diag,
                                lapack_int n, double* a, lapack_int lda );
lapack_int LAPACKE_ctrtri_work( int matrix_layout, char uplo, char diag,
                                lapack_int n, lapack_complex_float* a,
                                lapack_int lda );
lapack_int LAPACKE_ztrtri_work( int matrix_layout, char uplo, char diag,
                                lapack_int n, lapack_complex_double* a,
                                lapack_int lda );

lapack_int LAPACKE_strtrs_work( int matrix_layout, char uplo, char trans,
                                char diag, lapack_int n, lapack_int nrhs,
                                const float* a, lapack_int lda, float* b,
                                lapack_int ldb );
lapack_int LAPACKE_dtrtrs_work( int matrix_layout, char uplo, char trans,
                                char diag, lapack_int n, lapack_int nrhs,
                                const double* a, lapack_int lda, double* b,
                                lapack_int ldb );
lapack_int LAPACKE_ctrtrs_work( int matrix_layout, char uplo, char trans,
                                char diag, lapack_int n, lapack_int nrhs,
                                const lapack_complex_float* a, lapack_int lda,
                                lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_ztrtrs_work( int matrix_layout, char uplo, char trans,
                                char diag, lapack_int n, lapack_int nrhs,
                                const lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_strttf_work( int matrix_layout, char transr, char uplo,
                                lapack_int n, const float* a, lapack_int lda,
                                float* arf );
lapack_int LAPACKE_dtrttf_work( int matrix_layout, char transr, char uplo,
                                lapack_int n, const double* a, lapack_int lda,
                                double* arf );
lapack_int LAPACKE_ctrttf_work( int matrix_layout, char transr, char uplo,
                                lapack_int n, const lapack_complex_float* a,
                                lapack_int lda, lapack_complex_float* arf );
lapack_int LAPACKE_ztrttf_work( int matrix_layout, char transr, char uplo,
                                lapack_int n, const lapack_complex_double* a,
                                lapack_int lda, lapack_complex_double* arf );

lapack_int LAPACKE_strttp_work( int matrix_layout, char uplo, lapack_int n,
                                const float* a, lapack_int lda, float* ap );
lapack_int LAPACKE_dtrttp_work( int matrix_layout, char uplo, lapack_int n,
                                const double* a, lapack_int lda, double* ap );
lapack_int LAPACKE_ctrttp_work( int matrix_layout, char uplo, lapack_int n,
                                const lapack_complex_float* a, lapack_int lda,
                                lapack_complex_float* ap );
lapack_int LAPACKE_ztrttp_work( int matrix_layout, char uplo, lapack_int n,
                                const lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* ap );

lapack_int LAPACKE_stzrzf_work( int matrix_layout, lapack_int m, lapack_int n,
                                float* a, lapack_int lda, float* tau,
                                float* work, lapack_int lwork );
lapack_int LAPACKE_dtzrzf_work( int matrix_layout, lapack_int m, lapack_int n,
                                double* a, lapack_int lda, double* tau,
                                double* work, lapack_int lwork );
lapack_int LAPACKE_ctzrzf_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_complex_float* tau,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_ztzrzf_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* tau,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_cungbr_work( int matrix_layout, char vect, lapack_int m,
                                lapack_int n, lapack_int k,
                                lapack_complex_float* a, lapack_int lda,
                                const lapack_complex_float* tau,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zungbr_work( int matrix_layout, char vect, lapack_int m,
                                lapack_int n, lapack_int k,
                                lapack_complex_double* a, lapack_int lda,
                                const lapack_complex_double* tau,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_cunghr_work( int matrix_layout, lapack_int n, lapack_int ilo,
                                lapack_int ihi, lapack_complex_float* a,
                                lapack_int lda, const lapack_complex_float* tau,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zunghr_work( int matrix_layout, lapack_int n, lapack_int ilo,
                                lapack_int ihi, lapack_complex_double* a,
                                lapack_int lda,
                                const lapack_complex_double* tau,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_cunglq_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int k, lapack_complex_float* a,
                                lapack_int lda, const lapack_complex_float* tau,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zunglq_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int k, lapack_complex_double* a,
                                lapack_int lda,
                                const lapack_complex_double* tau,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_cungql_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int k, lapack_complex_float* a,
                                lapack_int lda, const lapack_complex_float* tau,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zungql_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int k, lapack_complex_double* a,
                                lapack_int lda,
                                const lapack_complex_double* tau,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_cungqr_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int k, lapack_complex_float* a,
                                lapack_int lda, const lapack_complex_float* tau,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zungqr_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int k, lapack_complex_double* a,
                                lapack_int lda,
                                const lapack_complex_double* tau,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_cungrq_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int k, lapack_complex_float* a,
                                lapack_int lda, const lapack_complex_float* tau,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zungrq_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int k, lapack_complex_double* a,
                                lapack_int lda,
                                const lapack_complex_double* tau,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_cungtr_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                const lapack_complex_float* tau,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zungtr_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                const lapack_complex_double* tau,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_cungtsqr_row_work( int matrix_layout,
                                      lapack_int m, lapack_int n,
                                      lapack_int mb, lapack_int nb,
                                      lapack_complex_float* a, lapack_int lda,
                                      const lapack_complex_float* t, lapack_int ldt,
                                      lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zungtsqr_row_work( int matrix_layout,
                                      lapack_int m, lapack_int n,
                                      lapack_int mb, lapack_int nb,
                                      lapack_complex_double* a, lapack_int lda,
                                      const lapack_complex_double* t, lapack_int ldt,
                                      lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_cunmbr_work( int matrix_layout, char vect, char side,
                                char trans, lapack_int m, lapack_int n,
                                lapack_int k, const lapack_complex_float* a,
                                lapack_int lda, const lapack_complex_float* tau,
                                lapack_complex_float* c, lapack_int ldc,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zunmbr_work( int matrix_layout, char vect, char side,
                                char trans, lapack_int m, lapack_int n,
                                lapack_int k, const lapack_complex_double* a,
                                lapack_int lda,
                                const lapack_complex_double* tau,
                                lapack_complex_double* c, lapack_int ldc,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_cunmhr_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int ilo,
                                lapack_int ihi, const lapack_complex_float* a,
                                lapack_int lda, const lapack_complex_float* tau,
                                lapack_complex_float* c, lapack_int ldc,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zunmhr_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int ilo,
                                lapack_int ihi, const lapack_complex_double* a,
                                lapack_int lda,
                                const lapack_complex_double* tau,
                                lapack_complex_double* c, lapack_int ldc,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_cunmlq_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int k,
                                const lapack_complex_float* a, lapack_int lda,
                                const lapack_complex_float* tau,
                                lapack_complex_float* c, lapack_int ldc,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zunmlq_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int k,
                                const lapack_complex_double* a, lapack_int lda,
                                const lapack_complex_double* tau,
                                lapack_complex_double* c, lapack_int ldc,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_cunmql_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int k,
                                const lapack_complex_float* a, lapack_int lda,
                                const lapack_complex_float* tau,
                                lapack_complex_float* c, lapack_int ldc,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zunmql_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int k,
                                const lapack_complex_double* a, lapack_int lda,
                                const lapack_complex_double* tau,
                                lapack_complex_double* c, lapack_int ldc,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_cunmqr_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int k,
                                const lapack_complex_float* a, lapack_int lda,
                                const lapack_complex_float* tau,
                                lapack_complex_float* c, lapack_int ldc,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zunmqr_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int k,
                                const lapack_complex_double* a, lapack_int lda,
                                const lapack_complex_double* tau,
                                lapack_complex_double* c, lapack_int ldc,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_cunmrq_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int k,
                                const lapack_complex_float* a, lapack_int lda,
                                const lapack_complex_float* tau,
                                lapack_complex_float* c, lapack_int ldc,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zunmrq_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int k,
                                const lapack_complex_double* a, lapack_int lda,
                                const lapack_complex_double* tau,
                                lapack_complex_double* c, lapack_int ldc,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_cunmrz_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int k,
                                lapack_int l, const lapack_complex_float* a,
                                lapack_int lda, const lapack_complex_float* tau,
                                lapack_complex_float* c, lapack_int ldc,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zunmrz_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int k,
                                lapack_int l, const lapack_complex_double* a,
                                lapack_int lda,
                                const lapack_complex_double* tau,
                                lapack_complex_double* c, lapack_int ldc,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_cunmtr_work( int matrix_layout, char side, char uplo,
                                char trans, lapack_int m, lapack_int n,
                                const lapack_complex_float* a, lapack_int lda,
                                const lapack_complex_float* tau,
                                lapack_complex_float* c, lapack_int ldc,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zunmtr_work( int matrix_layout, char side, char uplo,
                                char trans, lapack_int m, lapack_int n,
                                const lapack_complex_double* a, lapack_int lda,
                                const lapack_complex_double* tau,
                                lapack_complex_double* c, lapack_int ldc,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_cupgtr_work( int matrix_layout, char uplo, lapack_int n,
                                const lapack_complex_float* ap,
                                const lapack_complex_float* tau,
                                lapack_complex_float* q, lapack_int ldq,
                                lapack_complex_float* work );
lapack_int LAPACKE_zupgtr_work( int matrix_layout, char uplo, lapack_int n,
                                const lapack_complex_double* ap,
                                const lapack_complex_double* tau,
                                lapack_complex_double* q, lapack_int ldq,
                                lapack_complex_double* work );

lapack_int LAPACKE_cupmtr_work( int matrix_layout, char side, char uplo,
                                char trans, lapack_int m, lapack_int n,
                                const lapack_complex_float* ap,
                                const lapack_complex_float* tau,
                                lapack_complex_float* c, lapack_int ldc,
                                lapack_complex_float* work );
lapack_int LAPACKE_zupmtr_work( int matrix_layout, char side, char uplo,
                                char trans, lapack_int m, lapack_int n,
                                const lapack_complex_double* ap,
                                const lapack_complex_double* tau,
                                lapack_complex_double* c, lapack_int ldc,
                                lapack_complex_double* work );

lapack_int LAPACKE_claghe( int matrix_layout, lapack_int n, lapack_int k,
                           const float* d, lapack_complex_float* a,
                           lapack_int lda, lapack_int* iseed );
lapack_int LAPACKE_zlaghe( int matrix_layout, lapack_int n, lapack_int k,
                           const double* d, lapack_complex_double* a,
                           lapack_int lda, lapack_int* iseed );

lapack_int LAPACKE_slagsy( int matrix_layout, lapack_int n, lapack_int k,
                           const float* d, float* a, lapack_int lda,
                           lapack_int* iseed );
lapack_int LAPACKE_dlagsy( int matrix_layout, lapack_int n, lapack_int k,
                           const double* d, double* a, lapack_int lda,
                           lapack_int* iseed );
lapack_int LAPACKE_clagsy( int matrix_layout, lapack_int n, lapack_int k,
                           const float* d, lapack_complex_float* a,
                           lapack_int lda, lapack_int* iseed );
lapack_int LAPACKE_zlagsy( int matrix_layout, lapack_int n, lapack_int k,
                           const double* d, lapack_complex_double* a,
                           lapack_int lda, lapack_int* iseed );

lapack_int LAPACKE_slapmr( int matrix_layout, lapack_logical forwrd,
                           lapack_int m, lapack_int n, float* x, lapack_int ldx,
                           lapack_int* k );
lapack_int LAPACKE_dlapmr( int matrix_layout, lapack_logical forwrd,
                           lapack_int m, lapack_int n, double* x,
                           lapack_int ldx, lapack_int* k );
lapack_int LAPACKE_clapmr( int matrix_layout, lapack_logical forwrd,
                           lapack_int m, lapack_int n, lapack_complex_float* x,
                           lapack_int ldx, lapack_int* k );
lapack_int LAPACKE_zlapmr( int matrix_layout, lapack_logical forwrd,
                           lapack_int m, lapack_int n, lapack_complex_double* x,
                           lapack_int ldx, lapack_int* k );

lapack_int LAPACKE_slapmt( int matrix_layout, lapack_logical forwrd,
                           lapack_int m, lapack_int n, float* x, lapack_int ldx,
                           lapack_int* k );
lapack_int LAPACKE_dlapmt( int matrix_layout, lapack_logical forwrd,
                           lapack_int m, lapack_int n, double* x,
                           lapack_int ldx, lapack_int* k );
lapack_int LAPACKE_clapmt( int matrix_layout, lapack_logical forwrd,
                           lapack_int m, lapack_int n, lapack_complex_float* x,
                           lapack_int ldx, lapack_int* k );
lapack_int LAPACKE_zlapmt( int matrix_layout, lapack_logical forwrd,
                           lapack_int m, lapack_int n, lapack_complex_double* x,
                           lapack_int ldx, lapack_int* k );

float LAPACKE_slapy2( float x, float y );
double LAPACKE_dlapy2( double x, double y );

float LAPACKE_slapy3( float x, float y, float z );
double LAPACKE_dlapy3( double x, double y, double z );

lapack_int LAPACKE_slartgp( float f, float g, float* cs, float* sn, float* r );
lapack_int LAPACKE_dlartgp( double f, double g, double* cs, double* sn,
                            double* r );

lapack_int LAPACKE_slartgs( float x, float y, float sigma, float* cs,
                            float* sn );
lapack_int LAPACKE_dlartgs( double x, double y, double sigma, double* cs,
                            double* sn );


//LAPACK 3.3.0
lapack_int LAPACKE_cbbcsd( int matrix_layout, char jobu1, char jobu2,
                           char jobv1t, char jobv2t, char trans, lapack_int m,
                           lapack_int p, lapack_int q, float* theta, float* phi,
                           lapack_complex_float* u1, lapack_int ldu1,
                           lapack_complex_float* u2, lapack_int ldu2,
                           lapack_complex_float* v1t, lapack_int ldv1t,
                           lapack_complex_float* v2t, lapack_int ldv2t,
                           float* b11d, float* b11e, float* b12d, float* b12e,
                           float* b21d, float* b21e, float* b22d, float* b22e );
lapack_int LAPACKE_cbbcsd_work( int matrix_layout, char jobu1, char jobu2,
                                char jobv1t, char jobv2t, char trans,
                                lapack_int m, lapack_int p, lapack_int q,
                                float* theta, float* phi,
                                lapack_complex_float* u1, lapack_int ldu1,
                                lapack_complex_float* u2, lapack_int ldu2,
                                lapack_complex_float* v1t, lapack_int ldv1t,
                                lapack_complex_float* v2t, lapack_int ldv2t,
                                float* b11d, float* b11e, float* b12d,
                                float* b12e, float* b21d, float* b21e,
                                float* b22d, float* b22e, float* rwork,
                                lapack_int lrwork );
lapack_int LAPACKE_cheswapr( int matrix_layout, char uplo, lapack_int n,
                             lapack_complex_float* a, lapack_int lda,
                             lapack_int i1, lapack_int i2 );
lapack_int LAPACKE_cheswapr_work( int matrix_layout, char uplo, lapack_int n,
                                  lapack_complex_float* a, lapack_int lda,
                                  lapack_int i1, lapack_int i2 );
lapack_int LAPACKE_chetri2( int matrix_layout, char uplo, lapack_int n,
                            lapack_complex_float* a, lapack_int lda,
                            const lapack_int* ipiv );
lapack_int LAPACKE_chetri2_work( int matrix_layout, char uplo, lapack_int n,
                                 lapack_complex_float* a, lapack_int lda,
                                 const lapack_int* ipiv,
                                 lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_chetri2x( int matrix_layout, char uplo, lapack_int n,
                             lapack_complex_float* a, lapack_int lda,
                             const lapack_int* ipiv, lapack_int nb );
lapack_int LAPACKE_chetri2x_work( int matrix_layout, char uplo, lapack_int n,
                                  lapack_complex_float* a, lapack_int lda,
                                  const lapack_int* ipiv,
                                  lapack_complex_float* work, lapack_int nb );
lapack_int LAPACKE_chetrs2( int matrix_layout, char uplo, lapack_int n,
                            lapack_int nrhs, const lapack_complex_float* a,
                            lapack_int lda, const lapack_int* ipiv,
                            lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_chetrs2_work( int matrix_layout, char uplo, lapack_int n,
                                 lapack_int nrhs, const lapack_complex_float* a,
                                 lapack_int lda, const lapack_int* ipiv,
                                 lapack_complex_float* b, lapack_int ldb,
                                 lapack_complex_float* work );
lapack_int LAPACKE_csyconv( int matrix_layout, char uplo, char way, lapack_int n,
                            lapack_complex_float* a, lapack_int lda,
                            const lapack_int* ipiv, lapack_complex_float* e  );
lapack_int LAPACKE_csyconv_work( int matrix_layout, char uplo, char way,
                                 lapack_int n, lapack_complex_float* a,
                                 lapack_int lda, const lapack_int* ipiv,
                                 lapack_complex_float* e );
lapack_int LAPACKE_csyswapr( int matrix_layout, char uplo, lapack_int n,
                             lapack_complex_float* a, lapack_int lda,
                             lapack_int i1, lapack_int i2 );
lapack_int LAPACKE_csyswapr_work( int matrix_layout, char uplo, lapack_int n,
                                  lapack_complex_float* a, lapack_int lda,
                                  lapack_int i1, lapack_int i2 );
lapack_int LAPACKE_csytri2( int matrix_layout, char uplo, lapack_int n,
                            lapack_complex_float* a, lapack_int lda,
                            const lapack_int* ipiv );
lapack_int LAPACKE_csytri2_work( int matrix_layout, char uplo, lapack_int n,
                                 lapack_complex_float* a, lapack_int lda,
                                 const lapack_int* ipiv,
                                 lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_csytri2x( int matrix_layout, char uplo, lapack_int n,
                             lapack_complex_float* a, lapack_int lda,
                             const lapack_int* ipiv, lapack_int nb );
lapack_int LAPACKE_csytri2x_work( int matrix_layout, char uplo, lapack_int n,
                                  lapack_complex_float* a, lapack_int lda,
                                  const lapack_int* ipiv,
                                  lapack_complex_float* work, lapack_int nb );
lapack_int LAPACKE_csytrs2( int matrix_layout, char uplo, lapack_int n,
                            lapack_int nrhs, const lapack_complex_float* a,
                            lapack_int lda, const lapack_int* ipiv,
                            lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_csytrs2_work( int matrix_layout, char uplo, lapack_int n,
                                 lapack_int nrhs, const lapack_complex_float* a,
                                 lapack_int lda, const lapack_int* ipiv,
                                 lapack_complex_float* b, lapack_int ldb,
                                 lapack_complex_float* work );
lapack_int LAPACKE_cunbdb( int matrix_layout, char trans, char signs,
                           lapack_int m, lapack_int p, lapack_int q,
                           lapack_complex_float* x11, lapack_int ldx11,
                           lapack_complex_float* x12, lapack_int ldx12,
                           lapack_complex_float* x21, lapack_int ldx21,
                           lapack_complex_float* x22, lapack_int ldx22,
                           float* theta, float* phi,
                           lapack_complex_float* taup1,
                           lapack_complex_float* taup2,
                           lapack_complex_float* tauq1,
                           lapack_complex_float* tauq2 );
lapack_int LAPACKE_cunbdb_work( int matrix_layout, char trans, char signs,
                                lapack_int m, lapack_int p, lapack_int q,
                                lapack_complex_float* x11, lapack_int ldx11,
                                lapack_complex_float* x12, lapack_int ldx12,
                                lapack_complex_float* x21, lapack_int ldx21,
                                lapack_complex_float* x22, lapack_int ldx22,
                                float* theta, float* phi,
                                lapack_complex_float* taup1,
                                lapack_complex_float* taup2,
                                lapack_complex_float* tauq1,
                                lapack_complex_float* tauq2,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_cuncsd( int matrix_layout, char jobu1, char jobu2,
                           char jobv1t, char jobv2t, char trans, char signs,
                           lapack_int m, lapack_int p, lapack_int q,
                           lapack_complex_float* x11, lapack_int ldx11,
                           lapack_complex_float* x12, lapack_int ldx12,
                           lapack_complex_float* x21, lapack_int ldx21,
                           lapack_complex_float* x22, lapack_int ldx22,
                           float* theta, lapack_complex_float* u1,
                           lapack_int ldu1, lapack_complex_float* u2,
                           lapack_int ldu2, lapack_complex_float* v1t,
                           lapack_int ldv1t, lapack_complex_float* v2t,
                           lapack_int ldv2t );
lapack_int LAPACKE_cuncsd_work( int matrix_layout, char jobu1, char jobu2,
                                char jobv1t, char jobv2t, char trans,
                                char signs, lapack_int m, lapack_int p,
                                lapack_int q, lapack_complex_float* x11,
                                lapack_int ldx11, lapack_complex_float* x12,
                                lapack_int ldx12, lapack_complex_float* x21,
                                lapack_int ldx21, lapack_complex_float* x22,
                                lapack_int ldx22, float* theta,
                                lapack_complex_float* u1, lapack_int ldu1,
                                lapack_complex_float* u2, lapack_int ldu2,
                                lapack_complex_float* v1t, lapack_int ldv1t,
                                lapack_complex_float* v2t, lapack_int ldv2t,
                                lapack_complex_float* work, lapack_int lwork,
                                float* rwork, lapack_int lrwork,
                                lapack_int* iwork );
lapack_int LAPACKE_cuncsd2by1( int matrix_layout, char jobu1, char jobu2,
                           char jobv1t, lapack_int m, lapack_int p, lapack_int q,
                           lapack_complex_float* x11, lapack_int ldx11,
                           lapack_complex_float* x21, lapack_int ldx21,
                           float* theta, lapack_complex_float* u1,
                           lapack_int ldu1, lapack_complex_float* u2,
                           lapack_int ldu2, lapack_complex_float* v1t, lapack_int ldv1t );
lapack_int LAPACKE_cuncsd2by1_work( int matrix_layout, char jobu1, char jobu2,
                                char jobv1t, lapack_int m, lapack_int p,
                                lapack_int q, lapack_complex_float* x11, lapack_int ldx11,
                                lapack_complex_float* x21, lapack_int ldx21,
                                float* theta, lapack_complex_float* u1,
                                lapack_int ldu1, lapack_complex_float* u2,
                                lapack_int ldu2, lapack_complex_float* v1t,
                                lapack_int ldv1t, lapack_complex_float* work,
                                lapack_int lwork, float* rwork, lapack_int lrwork,
                                lapack_int* iwork );
lapack_int LAPACKE_dbbcsd( int matrix_layout, char jobu1, char jobu2,
                           char jobv1t, char jobv2t, char trans, lapack_int m,
                           lapack_int p, lapack_int q, double* theta,
                           double* phi, double* u1, lapack_int ldu1, double* u2,
                           lapack_int ldu2, double* v1t, lapack_int ldv1t,
                           double* v2t, lapack_int ldv2t, double* b11d,
                           double* b11e, double* b12d, double* b12e,
                           double* b21d, double* b21e, double* b22d,
                           double* b22e );
lapack_int LAPACKE_dbbcsd_work( int matrix_layout, char jobu1, char jobu2,
                                char jobv1t, char jobv2t, char trans,
                                lapack_int m, lapack_int p, lapack_int q,
                                double* theta, double* phi, double* u1,
                                lapack_int ldu1, double* u2, lapack_int ldu2,
                                double* v1t, lapack_int ldv1t, double* v2t,
                                lapack_int ldv2t, double* b11d, double* b11e,
                                double* b12d, double* b12e, double* b21d,
                                double* b21e, double* b22d, double* b22e,
                                double* work, lapack_int lwork );
lapack_int LAPACKE_dorbdb( int matrix_layout, char trans, char signs,
                           lapack_int m, lapack_int p, lapack_int q,
                           double* x11, lapack_int ldx11, double* x12,
                           lapack_int ldx12, double* x21, lapack_int ldx21,
                           double* x22, lapack_int ldx22, double* theta,
                           double* phi, double* taup1, double* taup2,
                           double* tauq1, double* tauq2 );
lapack_int LAPACKE_dorbdb_work( int matrix_layout, char trans, char signs,
                                lapack_int m, lapack_int p, lapack_int q,
                                double* x11, lapack_int ldx11, double* x12,
                                lapack_int ldx12, double* x21, lapack_int ldx21,
                                double* x22, lapack_int ldx22, double* theta,
                                double* phi, double* taup1, double* taup2,
                                double* tauq1, double* tauq2, double* work,
                                lapack_int lwork );
lapack_int LAPACKE_dorcsd( int matrix_layout, char jobu1, char jobu2,
                           char jobv1t, char jobv2t, char trans, char signs,
                           lapack_int m, lapack_int p, lapack_int q,
                           double* x11, lapack_int ldx11, double* x12,
                           lapack_int ldx12, double* x21, lapack_int ldx21,
                           double* x22, lapack_int ldx22, double* theta,
                           double* u1, lapack_int ldu1, double* u2,
                           lapack_int ldu2, double* v1t, lapack_int ldv1t,
                           double* v2t, lapack_int ldv2t );
lapack_int LAPACKE_dorcsd_work( int matrix_layout, char jobu1, char jobu2,
                                char jobv1t, char jobv2t, char trans,
                                char signs, lapack_int m, lapack_int p,
                                lapack_int q, double* x11, lapack_int ldx11,
                                double* x12, lapack_int ldx12, double* x21,
                                lapack_int ldx21, double* x22, lapack_int ldx22,
                                double* theta, double* u1, lapack_int ldu1,
                                double* u2, lapack_int ldu2, double* v1t,
                                lapack_int ldv1t, double* v2t, lapack_int ldv2t,
                                double* work, lapack_int lwork,
                                lapack_int* iwork );
lapack_int LAPACKE_dorcsd2by1( int matrix_layout, char jobu1, char jobu2,
                           char jobv1t, lapack_int m, lapack_int p, lapack_int q,
                           double* x11, lapack_int ldx11, double* x21, lapack_int ldx21,
                           double* theta, double* u1, lapack_int ldu1, double* u2,
                           lapack_int ldu2, double* v1t, lapack_int ldv1t);
lapack_int LAPACKE_dorcsd2by1_work( int matrix_layout, char jobu1, char jobu2,
                           char jobv1t, lapack_int m, lapack_int p, lapack_int q,
                           double* x11, lapack_int ldx11, double* x21, lapack_int ldx21,
                           double* theta, double* u1, lapack_int ldu1, double* u2,
                           lapack_int ldu2, double* v1t, lapack_int ldv1t,
                           double* work, lapack_int lwork, lapack_int* iwork );
lapack_int LAPACKE_dsyconv( int matrix_layout, char uplo, char way, lapack_int n,
                            double* a, lapack_int lda, const lapack_int* ipiv, double* e);
lapack_int LAPACKE_dsyconv_work( int matrix_layout, char uplo, char way,
                                 lapack_int n, double* a, lapack_int lda,
                                 const lapack_int* ipiv, double* e );
lapack_int LAPACKE_dsyswapr( int matrix_layout, char uplo, lapack_int n,
                             double* a, lapack_int lda, lapack_int i1,
                             lapack_int i2 );
lapack_int LAPACKE_dsyswapr_work( int matrix_layout, char uplo, lapack_int n,
                                  double* a, lapack_int lda, lapack_int i1,
                                  lapack_int i2 );
lapack_int LAPACKE_dsytri2( int matrix_layout, char uplo, lapack_int n,
                            double* a, lapack_int lda, const lapack_int* ipiv );
lapack_int LAPACKE_dsytri2_work( int matrix_layout, char uplo, lapack_int n,
                                 double* a, lapack_int lda,
                                 const lapack_int* ipiv,
                                 double* work, lapack_int lwork );
lapack_int LAPACKE_dsytri2x( int matrix_layout, char uplo, lapack_int n,
                             double* a, lapack_int lda, const lapack_int* ipiv,
                             lapack_int nb );
lapack_int LAPACKE_dsytri2x_work( int matrix_layout, char uplo, lapack_int n,
                                  double* a, lapack_int lda,
                                  const lapack_int* ipiv, double* work,
                                  lapack_int nb );
lapack_int LAPACKE_dsytrs2( int matrix_layout, char uplo, lapack_int n,
                            lapack_int nrhs, const double* a, lapack_int lda,
                            const lapack_int* ipiv, double* b, lapack_int ldb );
lapack_int LAPACKE_dsytrs2_work( int matrix_layout, char uplo, lapack_int n,
                                 lapack_int nrhs, const double* a,
                                 lapack_int lda, const lapack_int* ipiv,
                                 double* b, lapack_int ldb, double* work );
lapack_int LAPACKE_sbbcsd( int matrix_layout, char jobu1, char jobu2,
                           char jobv1t, char jobv2t, char trans, lapack_int m,
                           lapack_int p, lapack_int q, float* theta, float* phi,
                           float* u1, lapack_int ldu1, float* u2,
                           lapack_int ldu2, float* v1t, lapack_int ldv1t,
                           float* v2t, lapack_int ldv2t, float* b11d,
                           float* b11e, float* b12d, float* b12e, float* b21d,
                           float* b21e, float* b22d, float* b22e );
lapack_int LAPACKE_sbbcsd_work( int matrix_layout, char jobu1, char jobu2,
                                char jobv1t, char jobv2t, char trans,
                                lapack_int m, lapack_int p, lapack_int q,
                                float* theta, float* phi, float* u1,
                                lapack_int ldu1, float* u2, lapack_int ldu2,
                                float* v1t, lapack_int ldv1t, float* v2t,
                                lapack_int ldv2t, float* b11d, float* b11e,
                                float* b12d, float* b12e, float* b21d,
                                float* b21e, float* b22d, float* b22e,
                                float* work, lapack_int lwork );
lapack_int LAPACKE_sorbdb( int matrix_layout, char trans, char signs,
                           lapack_int m, lapack_int p, lapack_int q, float* x11,
                           lapack_int ldx11, float* x12, lapack_int ldx12,
                           float* x21, lapack_int ldx21, float* x22,
                           lapack_int ldx22, float* theta, float* phi,
                           float* taup1, float* taup2, float* tauq1,
                           float* tauq2 );
lapack_int LAPACKE_sorbdb_work( int matrix_layout, char trans, char signs,
                                lapack_int m, lapack_int p, lapack_int q,
                                float* x11, lapack_int ldx11, float* x12,
                                lapack_int ldx12, float* x21, lapack_int ldx21,
                                float* x22, lapack_int ldx22, float* theta,
                                float* phi, float* taup1, float* taup2,
                                float* tauq1, float* tauq2, float* work,
                                lapack_int lwork );
lapack_int LAPACKE_sorcsd( int matrix_layout, char jobu1, char jobu2,
                           char jobv1t, char jobv2t, char trans, char signs,
                           lapack_int m, lapack_int p, lapack_int q, float* x11,
                           lapack_int ldx11, float* x12, lapack_int ldx12,
                           float* x21, lapack_int ldx21, float* x22,
                           lapack_int ldx22, float* theta, float* u1,
                           lapack_int ldu1, float* u2, lapack_int ldu2,
                           float* v1t, lapack_int ldv1t, float* v2t,
                           lapack_int ldv2t );
lapack_int LAPACKE_sorcsd_work( int matrix_layout, char jobu1, char jobu2,
                                char jobv1t, char jobv2t, char trans,
                                char signs, lapack_int m, lapack_int p,
                                lapack_int q, float* x11, lapack_int ldx11,
                                float* x12, lapack_int ldx12, float* x21,
                                lapack_int ldx21, float* x22, lapack_int ldx22,
                                float* theta, float* u1, lapack_int ldu1,
                                float* u2, lapack_int ldu2, float* v1t,
                                lapack_int ldv1t, float* v2t, lapack_int ldv2t,
                                float* work, lapack_int lwork,
                                lapack_int* iwork );
lapack_int LAPACKE_sorcsd2by1( int matrix_layout, char jobu1, char jobu2,
                           char jobv1t, lapack_int m, lapack_int p, lapack_int q,
                           float* x11, lapack_int ldx11, float* x21, lapack_int ldx21,
                           float* theta, float* u1, lapack_int ldu1, float* u2,
                           lapack_int ldu2, float* v1t, lapack_int ldv1t);
lapack_int LAPACKE_sorcsd2by1_work( int matrix_layout, char jobu1, char jobu2,
                           char jobv1t, lapack_int m, lapack_int p, lapack_int q,
                           float* x11, lapack_int ldx11, float* x21, lapack_int ldx21,
                           float* theta, float* u1, lapack_int ldu1, float* u2,
                           lapack_int ldu2, float* v1t, lapack_int ldv1t,
                           float* work, lapack_int lwork, lapack_int* iwork );
lapack_int LAPACKE_ssyconv( int matrix_layout, char uplo, char way, lapack_int n,
                            float* a, lapack_int lda, const lapack_int* ipiv, float* e );
lapack_int LAPACKE_ssyconv_work( int matrix_layout, char uplo, char way,
                                 lapack_int n, float* a, lapack_int lda,
                                 const lapack_int* ipiv, float* e );
lapack_int LAPACKE_ssyswapr( int matrix_layout, char uplo, lapack_int n,
                             float* a, lapack_int lda, lapack_int i1,
                             lapack_int i2 );
lapack_int LAPACKE_ssyswapr_work( int matrix_layout, char uplo, lapack_int n,
                                  float* a, lapack_int lda, lapack_int i1,
                                  lapack_int i2 );
lapack_int LAPACKE_ssytri2( int matrix_layout, char uplo, lapack_int n, float* a,
                            lapack_int lda, const lapack_int* ipiv );
lapack_int LAPACKE_ssytri2_work( int matrix_layout, char uplo, lapack_int n,
                                 float* a, lapack_int lda,
                                 const lapack_int* ipiv,
                                 float* work, lapack_int lwork );
lapack_int LAPACKE_ssytri2x( int matrix_layout, char uplo, lapack_int n,
                             float* a, lapack_int lda, const lapack_int* ipiv,
                             lapack_int nb );
lapack_int LAPACKE_ssytri2x_work( int matrix_layout, char uplo, lapack_int n,
                                  float* a, lapack_int lda,
                                  const lapack_int* ipiv, float* work,
                                  lapack_int nb );
lapack_int LAPACKE_ssytrs2( int matrix_layout, char uplo, lapack_int n,
                            lapack_int nrhs, const float* a, lapack_int lda,
                            const lapack_int* ipiv, float* b, lapack_int ldb );
lapack_int LAPACKE_ssytrs2_work( int matrix_layout, char uplo, lapack_int n,
                                 lapack_int nrhs, const float* a,
                                 lapack_int lda, const lapack_int* ipiv,
                                 float* b, lapack_int ldb, float* work );
lapack_int LAPACKE_zbbcsd( int matrix_layout, char jobu1, char jobu2,
                           char jobv1t, char jobv2t, char trans, lapack_int m,
                           lapack_int p, lapack_int q, double* theta,
                           double* phi, lapack_complex_double* u1,
                           lapack_int ldu1, lapack_complex_double* u2,
                           lapack_int ldu2, lapack_complex_double* v1t,
                           lapack_int ldv1t, lapack_complex_double* v2t,
                           lapack_int ldv2t, double* b11d, double* b11e,
                           double* b12d, double* b12e, double* b21d,
                           double* b21e, double* b22d, double* b22e );
lapack_int LAPACKE_zbbcsd_work( int matrix_layout, char jobu1, char jobu2,
                                char jobv1t, char jobv2t, char trans,
                                lapack_int m, lapack_int p, lapack_int q,
                                double* theta, double* phi,
                                lapack_complex_double* u1, lapack_int ldu1,
                                lapack_complex_double* u2, lapack_int ldu2,
                                lapack_complex_double* v1t, lapack_int ldv1t,
                                lapack_complex_double* v2t, lapack_int ldv2t,
                                double* b11d, double* b11e, double* b12d,
                                double* b12e, double* b21d, double* b21e,
                                double* b22d, double* b22e, double* rwork,
                                lapack_int lrwork );
lapack_int LAPACKE_zheswapr( int matrix_layout, char uplo, lapack_int n,
                             lapack_complex_double* a, lapack_int lda,
                             lapack_int i1, lapack_int i2 );
lapack_int LAPACKE_zheswapr_work( int matrix_layout, char uplo, lapack_int n,
                                  lapack_complex_double* a, lapack_int lda,
                                  lapack_int i1, lapack_int i2 );
lapack_int LAPACKE_zhetri2( int matrix_layout, char uplo, lapack_int n,
                            lapack_complex_double* a, lapack_int lda,
                            const lapack_int* ipiv );
lapack_int LAPACKE_zhetri2_work( int matrix_layout, char uplo, lapack_int n,
                                 lapack_complex_double* a, lapack_int lda,
                                 const lapack_int* ipiv,
                                 lapack_complex_double* work, lapack_int lwork );
lapack_int LAPACKE_zhetri2x( int matrix_layout, char uplo, lapack_int n,
                             lapack_complex_double* a, lapack_int lda,
                             const lapack_int* ipiv, lapack_int nb );
lapack_int LAPACKE_zhetri2x_work( int matrix_layout, char uplo, lapack_int n,
                                  lapack_complex_double* a, lapack_int lda,
                                  const lapack_int* ipiv,
                                  lapack_complex_double* work, lapack_int nb );
lapack_int LAPACKE_zhetrs2( int matrix_layout, char uplo, lapack_int n,
                            lapack_int nrhs, const lapack_complex_double* a,
                            lapack_int lda, const lapack_int* ipiv,
                            lapack_complex_double* b, lapack_int ldb );
lapack_int LAPACKE_zhetrs2_work( int matrix_layout, char uplo, lapack_int n,
                                 lapack_int nrhs, const lapack_complex_double* a,
                                 lapack_int lda, const lapack_int* ipiv,
                                 lapack_complex_double* b, lapack_int ldb,
                                 lapack_complex_double* work );
lapack_int LAPACKE_zsyconv( int matrix_layout, char uplo, char way, lapack_int n,
                            lapack_complex_double* a, lapack_int lda,
                            const lapack_int* ipiv, lapack_complex_double* e );
lapack_int LAPACKE_zsyconv_work( int matrix_layout, char uplo, char way,
                                 lapack_int n, lapack_complex_double* a,
                                 lapack_int lda, const lapack_int* ipiv,
                                 lapack_complex_double* e );
lapack_int LAPACKE_zsyswapr( int matrix_layout, char uplo, lapack_int n,
                             lapack_complex_double* a, lapack_int lda,
                             lapack_int i1, lapack_int i2 );
lapack_int LAPACKE_zsyswapr_work( int matrix_layout, char uplo, lapack_int n,
                                  lapack_complex_double* a, lapack_int lda,
                                  lapack_int i1, lapack_int i2 );
lapack_int LAPACKE_zsytri2( int matrix_layout, char uplo, lapack_int n,
                            lapack_complex_double* a, lapack_int lda,
                            const lapack_int* ipiv );
lapack_int LAPACKE_zsytri2_work( int matrix_layout, char uplo, lapack_int n,
                                 lapack_complex_double* a, lapack_int lda,
                                 const lapack_int* ipiv,
                                 lapack_complex_double* work, lapack_int lwork );
lapack_int LAPACKE_zsytri2x( int matrix_layout, char uplo, lapack_int n,
                             lapack_complex_double* a, lapack_int lda,
                             const lapack_int* ipiv, lapack_int nb );
lapack_int LAPACKE_zsytri2x_work( int matrix_layout, char uplo, lapack_int n,
                                  lapack_complex_double* a, lapack_int lda,
                                  const lapack_int* ipiv,
                                  lapack_complex_double* work, lapack_int nb );
lapack_int LAPACKE_zsytrs2( int matrix_layout, char uplo, lapack_int n,
                            lapack_int nrhs, const lapack_complex_double* a,
                            lapack_int lda, const lapack_int* ipiv,
                            lapack_complex_double* b, lapack_int ldb );
lapack_int LAPACKE_zsytrs2_work( int matrix_layout, char uplo, lapack_int n,
                                 lapack_int nrhs, const lapack_complex_double* a,
                                 lapack_int lda, const lapack_int* ipiv,
                                 lapack_complex_double* b, lapack_int ldb,
                                 lapack_complex_double* work );
lapack_int LAPACKE_zunbdb( int matrix_layout, char trans, char signs,
                           lapack_int m, lapack_int p, lapack_int q,
                           lapack_complex_double* x11, lapack_int ldx11,
                           lapack_complex_double* x12, lapack_int ldx12,
                           lapack_complex_double* x21, lapack_int ldx21,
                           lapack_complex_double* x22, lapack_int ldx22,
                           double* theta, double* phi,
                           lapack_complex_double* taup1,
                           lapack_complex_double* taup2,
                           lapack_complex_double* tauq1,
                           lapack_complex_double* tauq2 );
lapack_int LAPACKE_zunbdb_work( int matrix_layout, char trans, char signs,
                                lapack_int m, lapack_int p, lapack_int q,
                                lapack_complex_double* x11, lapack_int ldx11,
                                lapack_complex_double* x12, lapack_int ldx12,
                                lapack_complex_double* x21, lapack_int ldx21,
                                lapack_complex_double* x22, lapack_int ldx22,
                                double* theta, double* phi,
                                lapack_complex_double* taup1,
                                lapack_complex_double* taup2,
                                lapack_complex_double* tauq1,
                                lapack_complex_double* tauq2,
                                lapack_complex_double* work, lapack_int lwork );
lapack_int LAPACKE_zuncsd( int matrix_layout, char jobu1, char jobu2,
                           char jobv1t, char jobv2t, char trans, char signs,
                           lapack_int m, lapack_int p, lapack_int q,
                           lapack_complex_double* x11, lapack_int ldx11,
                           lapack_complex_double* x12, lapack_int ldx12,
                           lapack_complex_double* x21, lapack_int ldx21,
                           lapack_complex_double* x22, lapack_int ldx22,
                           double* theta, lapack_complex_double* u1,
                           lapack_int ldu1, lapack_complex_double* u2,
                           lapack_int ldu2, lapack_complex_double* v1t,
                           lapack_int ldv1t, lapack_complex_double* v2t,
                           lapack_int ldv2t );
lapack_int LAPACKE_zuncsd_work( int matrix_layout, char jobu1, char jobu2,
                                char jobv1t, char jobv2t, char trans,
                                char signs, lapack_int m, lapack_int p,
                                lapack_int q, lapack_complex_double* x11,
                                lapack_int ldx11, lapack_complex_double* x12,
                                lapack_int ldx12, lapack_complex_double* x21,
                                lapack_int ldx21, lapack_complex_double* x22,
                                lapack_int ldx22, double* theta,
                                lapack_complex_double* u1, lapack_int ldu1,
                                lapack_complex_double* u2, lapack_int ldu2,
                                lapack_complex_double* v1t, lapack_int ldv1t,
                                lapack_complex_double* v2t, lapack_int ldv2t,
                                lapack_complex_double* work, lapack_int lwork,
                                double* rwork, lapack_int lrwork,
                                lapack_int* iwork );
lapack_int LAPACKE_zuncsd2by1( int matrix_layout, char jobu1, char jobu2,
                           char jobv1t, lapack_int m, lapack_int p, lapack_int q,
                           lapack_complex_double* x11, lapack_int ldx11,
                           lapack_complex_double* x21, lapack_int ldx21,
                           double* theta, lapack_complex_double* u1,
                           lapack_int ldu1, lapack_complex_double* u2,
                           lapack_int ldu2, lapack_complex_double* v1t, lapack_int ldv1t );
lapack_int LAPACKE_zuncsd2by1_work( int matrix_layout, char jobu1, char jobu2,
                                char jobv1t, lapack_int m, lapack_int p,
                                lapack_int q, lapack_complex_double* x11, lapack_int ldx11,
                                lapack_complex_double* x21, lapack_int ldx21,
                                double* theta, lapack_complex_double* u1,
                                lapack_int ldu1, lapack_complex_double* u2,
                                lapack_int ldu2, lapack_complex_double* v1t,
                                lapack_int ldv1t, lapack_complex_double* work,
                                lapack_int lwork, double* rwork, lapack_int lrwork,
                                lapack_int* iwork );

//LAPACK 3.4.0
lapack_int LAPACKE_sgemqrt( int matrix_layout, char side, char trans,
                            lapack_int m, lapack_int n, lapack_int k,
                            lapack_int nb, const float* v, lapack_int ldv,
                            const float* t, lapack_int ldt, float* c,
                            lapack_int ldc );
lapack_int LAPACKE_dgemqrt( int matrix_layout, char side, char trans,
                            lapack_int m, lapack_int n, lapack_int k,
                            lapack_int nb, const double* v, lapack_int ldv,
                            const double* t, lapack_int ldt, double* c,
                            lapack_int ldc );
lapack_int LAPACKE_cgemqrt( int matrix_layout, char side, char trans,
                            lapack_int m, lapack_int n, lapack_int k,
                            lapack_int nb, const lapack_complex_float* v,
                            lapack_int ldv, const lapack_complex_float* t,
                            lapack_int ldt, lapack_complex_float* c,
                            lapack_int ldc );
lapack_int LAPACKE_zgemqrt( int matrix_layout, char side, char trans,
                            lapack_int m, lapack_int n, lapack_int k,
                            lapack_int nb, const lapack_complex_double* v,
                            lapack_int ldv, const lapack_complex_double* t,
                            lapack_int ldt, lapack_complex_double* c,
                            lapack_int ldc );

lapack_int LAPACKE_sgeqrt( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int nb, float* a, lapack_int lda, float* t,
                           lapack_int ldt );
lapack_int LAPACKE_dgeqrt( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int nb, double* a, lapack_int lda, double* t,
                           lapack_int ldt );
lapack_int LAPACKE_cgeqrt( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int nb, lapack_complex_float* a,
                           lapack_int lda, lapack_complex_float* t,
                           lapack_int ldt );
lapack_int LAPACKE_zgeqrt( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int nb, lapack_complex_double* a,
                           lapack_int lda, lapack_complex_double* t,
                           lapack_int ldt );

lapack_int LAPACKE_sgeqrt2( int matrix_layout, lapack_int m, lapack_int n,
                            float* a, lapack_int lda, float* t,
                            lapack_int ldt );
lapack_int LAPACKE_dgeqrt2( int matrix_layout, lapack_int m, lapack_int n,
                            double* a, lapack_int lda, double* t,
                            lapack_int ldt );
lapack_int LAPACKE_cgeqrt2( int matrix_layout, lapack_int m, lapack_int n,
                            lapack_complex_float* a, lapack_int lda,
                            lapack_complex_float* t, lapack_int ldt );
lapack_int LAPACKE_zgeqrt2( int matrix_layout, lapack_int m, lapack_int n,
                            lapack_complex_double* a, lapack_int lda,
                            lapack_complex_double* t, lapack_int ldt );

lapack_int LAPACKE_sgeqrt3( int matrix_layout, lapack_int m, lapack_int n,
                            float* a, lapack_int lda, float* t,
                            lapack_int ldt );
lapack_int LAPACKE_dgeqrt3( int matrix_layout, lapack_int m, lapack_int n,
                            double* a, lapack_int lda, double* t,
                            lapack_int ldt );
lapack_int LAPACKE_cgeqrt3( int matrix_layout, lapack_int m, lapack_int n,
                            lapack_complex_float* a, lapack_int lda,
                            lapack_complex_float* t, lapack_int ldt );
lapack_int LAPACKE_zgeqrt3( int matrix_layout, lapack_int m, lapack_int n,
                            lapack_complex_double* a, lapack_int lda,
                            lapack_complex_double* t, lapack_int ldt );

lapack_int LAPACKE_stpmqrt( int matrix_layout, char side, char trans,
                            lapack_int m, lapack_int n, lapack_int k,
                            lapack_int l, lapack_int nb, const float* v,
                            lapack_int ldv, const float* t, lapack_int ldt,
                            float* a, lapack_int lda, float* b,
                            lapack_int ldb );
lapack_int LAPACKE_dtpmqrt( int matrix_layout, char side, char trans,
                            lapack_int m, lapack_int n, lapack_int k,
                            lapack_int l, lapack_int nb, const double* v,
                            lapack_int ldv, const double* t, lapack_int ldt,
                            double* a, lapack_int lda, double* b,
                            lapack_int ldb );
lapack_int LAPACKE_ctpmqrt( int matrix_layout, char side, char trans,
                            lapack_int m, lapack_int n, lapack_int k,
                            lapack_int l, lapack_int nb,
                            const lapack_complex_float* v, lapack_int ldv,
                            const lapack_complex_float* t, lapack_int ldt,
                            lapack_complex_float* a, lapack_int lda,
                            lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_ztpmqrt( int matrix_layout, char side, char trans,
                            lapack_int m, lapack_int n, lapack_int k,
                            lapack_int l, lapack_int nb,
                            const lapack_complex_double* v, lapack_int ldv,
                            const lapack_complex_double* t, lapack_int ldt,
                            lapack_complex_double* a, lapack_int lda,
                            lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_stpqrt( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int l, lapack_int nb, float* a,
                           lapack_int lda, float* b, lapack_int ldb, float* t,
                           lapack_int ldt );

lapack_int LAPACKE_dtpqrt( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int l, lapack_int nb, double* a,
                           lapack_int lda, double* b, lapack_int ldb, double* t,
                           lapack_int ldt );
lapack_int LAPACKE_ctpqrt( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int l, lapack_int nb,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_complex_float* b, lapack_int ldb,
                           lapack_complex_float* t, lapack_int ldt );
lapack_int LAPACKE_ztpqrt( int matrix_layout, lapack_int m, lapack_int n,
                           lapack_int l, lapack_int nb,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_complex_double* b, lapack_int ldb,
                           lapack_complex_double* t, lapack_int ldt );

lapack_int LAPACKE_stpqrt2( int matrix_layout,
                            lapack_int m, lapack_int n, lapack_int l,
                            float* a, lapack_int lda,
                            float* b, lapack_int ldb,
                            float* t, lapack_int ldt );
lapack_int LAPACKE_dtpqrt2( int matrix_layout,
                            lapack_int m, lapack_int n, lapack_int l,
                            double* a, lapack_int lda,
                            double* b, lapack_int ldb,
                            double* t, lapack_int ldt );
lapack_int LAPACKE_ctpqrt2( int matrix_layout,
                            lapack_int m, lapack_int n, lapack_int l,
                            lapack_complex_float* a, lapack_int lda,
                            lapack_complex_float* b, lapack_int ldb,
                            lapack_complex_float* t, lapack_int ldt );
lapack_int LAPACKE_ztpqrt2( int matrix_layout,
                            lapack_int m, lapack_int n, lapack_int l,
                            lapack_complex_double* a, lapack_int lda,
                            lapack_complex_double* b, lapack_int ldb,
                            lapack_complex_double* t, lapack_int ldt );

lapack_int LAPACKE_stprfb( int matrix_layout, char side, char trans, char direct,
                           char storev, lapack_int m, lapack_int n,
                           lapack_int k, lapack_int l, const float* v,
                           lapack_int ldv, const float* t, lapack_int ldt,
                           float* a, lapack_int lda, float* b, lapack_int ldb );
lapack_int LAPACKE_dtprfb( int matrix_layout, char side, char trans, char direct,
                           char storev, lapack_int m, lapack_int n,
                           lapack_int k, lapack_int l, const double* v,
                           lapack_int ldv, const double* t, lapack_int ldt,
                           double* a, lapack_int lda, double* b, lapack_int ldb );
lapack_int LAPACKE_ctprfb( int matrix_layout, char side, char trans, char direct,
                           char storev, lapack_int m, lapack_int n,
                           lapack_int k, lapack_int l,
                           const lapack_complex_float* v, lapack_int ldv,
                           const lapack_complex_float* t, lapack_int ldt,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_ztprfb( int matrix_layout, char side, char trans, char direct,
                           char storev, lapack_int m, lapack_int n,
                           lapack_int k, lapack_int l,
                           const lapack_complex_double* v, lapack_int ldv,
                           const lapack_complex_double* t, lapack_int ldt,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_sgemqrt_work( int matrix_layout, char side, char trans,
                                 lapack_int m, lapack_int n, lapack_int k,
                                 lapack_int nb, const float* v, lapack_int ldv,
                                 const float* t, lapack_int ldt, float* c,
                                 lapack_int ldc, float* work );
lapack_int LAPACKE_dgemqrt_work( int matrix_layout, char side, char trans,
                                 lapack_int m, lapack_int n, lapack_int k,
                                 lapack_int nb, const double* v, lapack_int ldv,
                                 const double* t, lapack_int ldt, double* c,
                                 lapack_int ldc, double* work );
lapack_int LAPACKE_cgemqrt_work( int matrix_layout, char side, char trans,
                                 lapack_int m, lapack_int n, lapack_int k,
                                 lapack_int nb, const lapack_complex_float* v,
                                 lapack_int ldv, const lapack_complex_float* t,
                                 lapack_int ldt, lapack_complex_float* c,
                                 lapack_int ldc, lapack_complex_float* work );
lapack_int LAPACKE_zgemqrt_work( int matrix_layout, char side, char trans,
                                 lapack_int m, lapack_int n, lapack_int k,
                                 lapack_int nb, const lapack_complex_double* v,
                                 lapack_int ldv, const lapack_complex_double* t,
                                 lapack_int ldt, lapack_complex_double* c,
                                 lapack_int ldc, lapack_complex_double* work );

lapack_int LAPACKE_sgeqrt_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int nb, float* a, lapack_int lda,
                                float* t, lapack_int ldt, float* work );
lapack_int LAPACKE_dgeqrt_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int nb, double* a, lapack_int lda,
                                double* t, lapack_int ldt, double* work );
lapack_int LAPACKE_cgeqrt_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int nb, lapack_complex_float* a,
                                lapack_int lda, lapack_complex_float* t,
                                lapack_int ldt, lapack_complex_float* work );
lapack_int LAPACKE_zgeqrt_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int nb, lapack_complex_double* a,
                                lapack_int lda, lapack_complex_double* t,
                                lapack_int ldt, lapack_complex_double* work );

lapack_int LAPACKE_sgeqrt2_work( int matrix_layout, lapack_int m, lapack_int n,
                                 float* a, lapack_int lda, float* t,
                                 lapack_int ldt );
lapack_int LAPACKE_dgeqrt2_work( int matrix_layout, lapack_int m, lapack_int n,
                                 double* a, lapack_int lda, double* t,
                                 lapack_int ldt );
lapack_int LAPACKE_cgeqrt2_work( int matrix_layout, lapack_int m, lapack_int n,
                                 lapack_complex_float* a, lapack_int lda,
                                 lapack_complex_float* t, lapack_int ldt );
lapack_int LAPACKE_zgeqrt2_work( int matrix_layout, lapack_int m, lapack_int n,
                                 lapack_complex_double* a, lapack_int lda,
                                 lapack_complex_double* t, lapack_int ldt );

lapack_int LAPACKE_sgeqrt3_work( int matrix_layout, lapack_int m, lapack_int n,
                                 float* a, lapack_int lda, float* t,
                                 lapack_int ldt );
lapack_int LAPACKE_dgeqrt3_work( int matrix_layout, lapack_int m, lapack_int n,
                                 double* a, lapack_int lda, double* t,
                                 lapack_int ldt );
lapack_int LAPACKE_cgeqrt3_work( int matrix_layout, lapack_int m, lapack_int n,
                                 lapack_complex_float* a, lapack_int lda,
                                 lapack_complex_float* t, lapack_int ldt );
lapack_int LAPACKE_zgeqrt3_work( int matrix_layout, lapack_int m, lapack_int n,
                                 lapack_complex_double* a, lapack_int lda,
                                 lapack_complex_double* t, lapack_int ldt );

lapack_int LAPACKE_stpmqrt_work( int matrix_layout, char side, char trans,
                                 lapack_int m, lapack_int n, lapack_int k,
                                 lapack_int l, lapack_int nb, const float* v,
                                 lapack_int ldv, const float* t, lapack_int ldt,
                                 float* a, lapack_int lda, float* b,
                                 lapack_int ldb, float* work );
lapack_int LAPACKE_dtpmqrt_work( int matrix_layout, char side, char trans,
                                 lapack_int m, lapack_int n, lapack_int k,
                                 lapack_int l, lapack_int nb, const double* v,
                                 lapack_int ldv, const double* t,
                                 lapack_int ldt, double* a, lapack_int lda,
                                 double* b, lapack_int ldb, double* work );
lapack_int LAPACKE_ctpmqrt_work( int matrix_layout, char side, char trans,
                                 lapack_int m, lapack_int n, lapack_int k,
                                 lapack_int l, lapack_int nb,
                                 const lapack_complex_float* v, lapack_int ldv,
                                 const lapack_complex_float* t, lapack_int ldt,
                                 lapack_complex_float* a, lapack_int lda,
                                 lapack_complex_float* b, lapack_int ldb,
                                 lapack_complex_float* work );
lapack_int LAPACKE_ztpmqrt_work( int matrix_layout, char side, char trans,
                                 lapack_int m, lapack_int n, lapack_int k,
                                 lapack_int l, lapack_int nb,
                                 const lapack_complex_double* v, lapack_int ldv,
                                 const lapack_complex_double* t, lapack_int ldt,
                                 lapack_complex_double* a, lapack_int lda,
                                 lapack_complex_double* b, lapack_int ldb,
                                 lapack_complex_double* work );

lapack_int LAPACKE_stpqrt_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int l, lapack_int nb, float* a,
                                lapack_int lda, float* b, lapack_int ldb,
                                float* t, lapack_int ldt, float* work );
lapack_int LAPACKE_dtpqrt_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int l, lapack_int nb, double* a,
                                lapack_int lda, double* b, lapack_int ldb,
                                double* t, lapack_int ldt, double* work );
lapack_int LAPACKE_ctpqrt_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int l, lapack_int nb,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_complex_float* b, lapack_int ldb,
                                lapack_complex_float* t, lapack_int ldt,
                                lapack_complex_float* work );
lapack_int LAPACKE_ztpqrt_work( int matrix_layout, lapack_int m, lapack_int n,
                                lapack_int l, lapack_int nb,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* t, lapack_int ldt,
                                lapack_complex_double* work );

lapack_int LAPACKE_stpqrt2_work( int matrix_layout,
                                 lapack_int m, lapack_int n, lapack_int l,
                                 float* a, lapack_int lda,
                                 float* b, lapack_int ldb,
                                 float* t, lapack_int ldt );
lapack_int LAPACKE_dtpqrt2_work( int matrix_layout,
                                 lapack_int m, lapack_int n, lapack_int l,
                                 double* a, lapack_int lda,
                                 double* b, lapack_int ldb,
                                 double* t, lapack_int ldt );
lapack_int LAPACKE_ctpqrt2_work( int matrix_layout,
                                 lapack_int m, lapack_int n, lapack_int l,
                                 lapack_complex_float* a, lapack_int lda,
                                 lapack_complex_float* b, lapack_int ldb,
                                 lapack_complex_float* t, lapack_int ldt );
lapack_int LAPACKE_ztpqrt2_work( int matrix_layout,
                                 lapack_int m, lapack_int n, lapack_int l,
                                 lapack_complex_double* a, lapack_int lda,
                                 lapack_complex_double* b, lapack_int ldb,
                                 lapack_complex_double* t, lapack_int ldt );

lapack_int LAPACKE_stprfb_work( int matrix_layout, char side, char trans,
                                char direct, char storev, lapack_int m,
                                lapack_int n, lapack_int k, lapack_int l,
                                const float* v, lapack_int ldv, const float* t,
                                lapack_int ldt, float* a, lapack_int lda,
                                float* b, lapack_int ldb, float* work,
                                lapack_int ldwork );
lapack_int LAPACKE_dtprfb_work( int matrix_layout, char side, char trans,
                                char direct, char storev, lapack_int m,
                                lapack_int n, lapack_int k, lapack_int l,
                                const double* v, lapack_int ldv,
                                const double* t, lapack_int ldt, double* a,
                                lapack_int lda, double* b, lapack_int ldb,
                                double* work, lapack_int ldwork );
lapack_int LAPACKE_ctprfb_work( int matrix_layout, char side, char trans,
                                char direct, char storev, lapack_int m,
                                lapack_int n, lapack_int k, lapack_int l,
                                const lapack_complex_float* v, lapack_int ldv,
                                const lapack_complex_float* t, lapack_int ldt,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_complex_float* b, lapack_int ldb,
                                lapack_complex_float* work, lapack_int ldwork );
lapack_int LAPACKE_ztprfb_work( int matrix_layout, char side, char trans,
                                char direct, char storev, lapack_int m,
                                lapack_int n, lapack_int k, lapack_int l,
                                const lapack_complex_double* v, lapack_int ldv,
                                const lapack_complex_double* t, lapack_int ldt,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* work, lapack_int ldwork );
//LAPACK 3.X.X
lapack_int LAPACKE_ssysv_rook( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, float* a, lapack_int lda,
                               lapack_int* ipiv, float* b, lapack_int ldb );
lapack_int LAPACKE_dsysv_rook( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, double* a, lapack_int lda,
                               lapack_int* ipiv, double* b, lapack_int ldb );
lapack_int LAPACKE_csysv_rook( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, lapack_complex_float* a,
                               lapack_int lda, lapack_int* ipiv,
                               lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zsysv_rook( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, lapack_complex_double* a,
                               lapack_int lda, lapack_int* ipiv,
                               lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_ssytrf_rook( int matrix_layout, char uplo, lapack_int n, float* a,
                           lapack_int lda, lapack_int* ipiv );
lapack_int LAPACKE_dsytrf_rook( int matrix_layout, char uplo, lapack_int n, double* a,
                           lapack_int lda, lapack_int* ipiv );
lapack_int LAPACKE_csytrf_rook( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_int* ipiv );
lapack_int LAPACKE_zsytrf_rook( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_int* ipiv );

lapack_int LAPACKE_ssytrs_rook( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const float* a, lapack_int lda,
                           const lapack_int* ipiv, float* b, lapack_int ldb );
lapack_int LAPACKE_dsytrs_rook( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const double* a, lapack_int lda,
                           const lapack_int* ipiv, double* b, lapack_int ldb );
lapack_int LAPACKE_csytrs_rook( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_float* a,
                           lapack_int lda, const lapack_int* ipiv,
                           lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zsytrs_rook( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_double* a,
                           lapack_int lda, const lapack_int* ipiv,
                           lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_chetrf_rook( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_int* ipiv );
lapack_int LAPACKE_zhetrf_rook( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_int* ipiv );

lapack_int LAPACKE_chetrs_rook( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_float* a,
                           lapack_int lda, const lapack_int* ipiv,
                           lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zhetrs_rook( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const lapack_complex_double* a,
                           lapack_int lda, const lapack_int* ipiv,
                           lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_csyr( int matrix_layout, char uplo, lapack_int n,
                             lapack_complex_float alpha,
                             const lapack_complex_float* x, lapack_int incx,
                             lapack_complex_float* a, lapack_int lda );
lapack_int LAPACKE_zsyr( int matrix_layout, char uplo, lapack_int n,
                             lapack_complex_double alpha,
                             const lapack_complex_double* x, lapack_int incx,
                             lapack_complex_double* a, lapack_int lda );

lapack_int LAPACKE_ssysv_rook_work( int matrix_layout, char uplo, lapack_int n,
                                    lapack_int nrhs, float* a, lapack_int lda,
                                    lapack_int* ipiv, float* b, lapack_int ldb,
                                    float* work, lapack_int lwork );
lapack_int LAPACKE_dsysv_rook_work( int matrix_layout, char uplo, lapack_int n,
                                    lapack_int nrhs, double* a, lapack_int lda,
                                    lapack_int* ipiv, double* b, lapack_int ldb,
                                    double* work, lapack_int lwork );
lapack_int LAPACKE_csysv_rook_work( int matrix_layout, char uplo, lapack_int n,
                                    lapack_int nrhs, lapack_complex_float* a,
                                    lapack_int lda, lapack_int* ipiv,
                                    lapack_complex_float* b, lapack_int ldb,
                                    lapack_complex_float* work,
                                    lapack_int lwork );
lapack_int LAPACKE_zsysv_rook_work( int matrix_layout, char uplo, lapack_int n,
                                    lapack_int nrhs, lapack_complex_double* a,
                                    lapack_int lda, lapack_int* ipiv,
                                    lapack_complex_double* b, lapack_int ldb,
                                    lapack_complex_double* work,
                                    lapack_int lwork );

lapack_int LAPACKE_ssytrf_rook_work( int matrix_layout, char uplo, lapack_int n,
                                float* a, lapack_int lda, lapack_int* ipiv,
                                float* work, lapack_int lwork );
lapack_int LAPACKE_dsytrf_rook_work( int matrix_layout, char uplo, lapack_int n,
                                double* a, lapack_int lda, lapack_int* ipiv,
                                double* work, lapack_int lwork );
lapack_int LAPACKE_csytrf_rook_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_int* ipiv, lapack_complex_float* work,
                                lapack_int lwork );
lapack_int LAPACKE_zsytrf_rook_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_int* ipiv, lapack_complex_double* work,
                                lapack_int lwork );

lapack_int LAPACKE_ssytrs_rook_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const float* a, lapack_int lda,
                                const lapack_int* ipiv, float* b,
                                lapack_int ldb );
lapack_int LAPACKE_dsytrs_rook_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const double* a,
                                lapack_int lda, const lapack_int* ipiv,
                                double* b, lapack_int ldb );
lapack_int LAPACKE_csytrs_rook_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const lapack_complex_float* a,
                                lapack_int lda, const lapack_int* ipiv,
                                lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zsytrs_rook_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const lapack_complex_double* a,
                                lapack_int lda, const lapack_int* ipiv,
                                lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_chetrf_rook_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_int* ipiv, lapack_complex_float* work,
                                lapack_int lwork );
lapack_int LAPACKE_zhetrf_rook_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_int* ipiv, lapack_complex_double* work,
                                lapack_int lwork );

lapack_int LAPACKE_chetrs_rook_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const lapack_complex_float* a,
                                lapack_int lda, const lapack_int* ipiv,
                                lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zhetrs_rook_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const lapack_complex_double* a,
                                lapack_int lda, const lapack_int* ipiv,
                                lapack_complex_double* b, lapack_int ldb );


lapack_int LAPACKE_csyr_work( int matrix_layout, char uplo, lapack_int n,
                                  lapack_complex_float alpha,
                                  const lapack_complex_float* x,
                                  lapack_int incx, lapack_complex_float* a,
                                  lapack_int lda );
lapack_int LAPACKE_zsyr_work( int matrix_layout, char uplo, lapack_int n,
                                  lapack_complex_double alpha,
                                  const lapack_complex_double* x,
                                  lapack_int incx, lapack_complex_double* a,
                                  lapack_int lda );
void LAPACKE_ilaver( lapack_int* vers_major,
                     lapack_int* vers_minor,
                     lapack_int* vers_patch );
// LAPACK 3.7.0
lapack_int LAPACKE_ssysv_aa( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, float* a, lapack_int lda,
                          lapack_int* ipiv, float* b, lapack_int ldb );
lapack_int LAPACKE_ssysv_aa_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, float* a, lapack_int lda,
                               lapack_int* ipiv, float* b, lapack_int ldb,
                               float* work, lapack_int lwork );
lapack_int LAPACKE_dsysv_aa( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, double* a, lapack_int lda,
                          lapack_int* ipiv, double* b, lapack_int ldb );
lapack_int LAPACKE_dsysv_aa_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, double* a, lapack_int lda,
                               lapack_int* ipiv, double* b, lapack_int ldb,
                               double* work, lapack_int lwork );
lapack_int LAPACKE_csysv_aa( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, lapack_complex_float* a,
                          lapack_int lda, lapack_int* ipiv,
                          lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_csysv_aa_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, lapack_complex_float* a,
                               lapack_int lda, lapack_int* ipiv,
                               lapack_complex_float* b, lapack_int ldb,
                               lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zsysv_aa( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, lapack_complex_double* a,
                          lapack_int lda, lapack_int* ipiv,
                          lapack_complex_double* b, lapack_int ldb );
lapack_int LAPACKE_zsysv_aa_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, lapack_complex_double* a,
                               lapack_int lda, lapack_int* ipiv,
                               lapack_complex_double* b, lapack_int ldb,
                               lapack_complex_double* work, lapack_int lwork );
lapack_int LAPACKE_chesv_aa( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, lapack_complex_float* a,
                          lapack_int lda, lapack_int* ipiv,
                          lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_chesv_aa_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, lapack_complex_float* a,
                               lapack_int lda, lapack_int* ipiv,
                               lapack_complex_float* b, lapack_int ldb,
                               lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zhesv_aa( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, lapack_complex_double* a,
                          lapack_int lda, lapack_int* ipiv,
                          lapack_complex_double* b, lapack_int ldb );
lapack_int LAPACKE_zhesv_aa_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, lapack_complex_double* a,
                               lapack_int lda, lapack_int* ipiv,
                               lapack_complex_double* b, lapack_int ldb,
                               lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_ssytrf_aa( int matrix_layout, char uplo, lapack_int n, float* a,
                           lapack_int lda, lapack_int* ipiv );
lapack_int LAPACKE_dsytrf_aa( int matrix_layout, char uplo, lapack_int n, double* a,
                           lapack_int lda, lapack_int* ipiv );
lapack_int LAPACKE_csytrf_aa( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_int* ipiv );
lapack_int LAPACKE_zsytrf_aa( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_int* ipiv );
lapack_int LAPACKE_chetrf_aa( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_int* ipiv );
lapack_int LAPACKE_zhetrf_aa( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_int* ipiv );

lapack_int LAPACKE_ssytrf_aa_work( int matrix_layout, char uplo, lapack_int n,
                                float* a, lapack_int lda, lapack_int* ipiv,
                                float* work, lapack_int lwork );
lapack_int LAPACKE_dsytrf_aa_work( int matrix_layout, char uplo, lapack_int n,
                                double* a, lapack_int lda, lapack_int* ipiv,
                                double* work, lapack_int lwork );
lapack_int LAPACKE_csytrf_aa_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_int* ipiv, lapack_complex_float* work,
                                lapack_int lwork );
lapack_int LAPACKE_zsytrf_aa_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_int* ipiv, lapack_complex_double* work,
                                lapack_int lwork );
lapack_int LAPACKE_chetrf_aa_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_int* ipiv, lapack_complex_float* work,
                                lapack_int lwork );
lapack_int LAPACKE_zhetrf_aa_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_int* ipiv, lapack_complex_double* work,
                                lapack_int lwork );


lapack_int LAPACKE_csytrs_aa( int matrix_layout, char uplo, lapack_int n,
                            lapack_int nrhs, const lapack_complex_float* a,
                            lapack_int lda, const lapack_int* ipiv,
                            lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_csytrs_aa_work( int matrix_layout, char uplo, lapack_int n,
                                 lapack_int nrhs, const lapack_complex_float* a,
                                 lapack_int lda, const lapack_int* ipiv,
                                 lapack_complex_float* b, lapack_int ldb,
                                 lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_chetrs_aa( int matrix_layout, char uplo, lapack_int n,
                            lapack_int nrhs, const lapack_complex_float* a,
                            lapack_int lda, const lapack_int* ipiv,
                            lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_chetrs_aa_work( int matrix_layout, char uplo, lapack_int n,
                                 lapack_int nrhs, const lapack_complex_float* a,
                                 lapack_int lda, const lapack_int* ipiv,
                                 lapack_complex_float* b, lapack_int ldb,
                                 lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_dsytrs_aa( int matrix_layout, char uplo, lapack_int n,
                            lapack_int nrhs, const double* a, lapack_int lda,
                            const lapack_int* ipiv, double* b, lapack_int ldb );
lapack_int LAPACKE_dsytrs_aa_work( int matrix_layout, char uplo, lapack_int n,
                                 lapack_int nrhs, const double* a,
                                 lapack_int lda, const lapack_int* ipiv,
                                 double* b, lapack_int ldb, double* work, lapack_int lwork );
lapack_int LAPACKE_ssytrs_aa( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const float* a, lapack_int lda,
                           const lapack_int* ipiv, float* b, lapack_int ldb );
lapack_int LAPACKE_ssytrs_aa_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const float* a, lapack_int lda,
                                const lapack_int* ipiv, float* b,
                                lapack_int ldb, float* work, lapack_int lwork );
lapack_int LAPACKE_zsytrs_aa( int matrix_layout, char uplo, lapack_int n,
                            lapack_int nrhs, const lapack_complex_double* a,
                            lapack_int lda, const lapack_int* ipiv,
                            lapack_complex_double* b, lapack_int ldb );
lapack_int LAPACKE_zsytrs_aa_work( int matrix_layout, char uplo, lapack_int n,
                                 lapack_int nrhs, const lapack_complex_double* a,
                                 lapack_int lda, const lapack_int* ipiv,
                                 lapack_complex_double* b, lapack_int ldb,
                                 lapack_complex_double* work,  lapack_int lwork);
lapack_int LAPACKE_zhetrs_aa( int matrix_layout, char uplo, lapack_int n,
                            lapack_int nrhs, const lapack_complex_double* a,
                            lapack_int lda, const lapack_int* ipiv,
                            lapack_complex_double* b, lapack_int ldb );
lapack_int LAPACKE_zhetrs_aa_work( int matrix_layout, char uplo, lapack_int n,
                                 lapack_int nrhs, const lapack_complex_double* a,
                                 lapack_int lda, const lapack_int* ipiv,
                                 lapack_complex_double* b, lapack_int ldb,
                                 lapack_complex_double* work,  lapack_int lwork);


lapack_int LAPACKE_ssysv_rk( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, float* a, lapack_int lda,
                          float* e, lapack_int* ipiv, float* b, lapack_int ldb );
lapack_int LAPACKE_ssysv_rk_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, float* a, lapack_int lda,
                               float* e, lapack_int* ipiv, float* b, lapack_int ldb,
                               float* work, lapack_int lwork );
lapack_int LAPACKE_dsysv_rk( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, double* a, lapack_int lda,
                          double* e, lapack_int* ipiv, double* b, lapack_int ldb );
lapack_int LAPACKE_dsysv_rk_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, double* a, lapack_int lda,
                               double* e, lapack_int* ipiv, double* b, lapack_int ldb,
                               double* work, lapack_int lwork );
lapack_int LAPACKE_csysv_rk( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, lapack_complex_float* a,
                          lapack_int lda, lapack_complex_float* e, lapack_int* ipiv,
                          lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_csysv_rk_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, lapack_complex_float* a,
                               lapack_int lda, lapack_complex_float* e, lapack_int* ipiv,
                               lapack_complex_float* b, lapack_int ldb,
                               lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zsysv_rk( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, lapack_complex_double* a,
                          lapack_int lda, lapack_complex_double* e, lapack_int* ipiv,
                          lapack_complex_double* b, lapack_int ldb );
lapack_int LAPACKE_zsysv_rk_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, lapack_complex_double* a,
                               lapack_int lda, lapack_complex_double* e, lapack_int* ipiv,
                               lapack_complex_double* b, lapack_int ldb,
                               lapack_complex_double* work, lapack_int lwork );
lapack_int LAPACKE_chesv_rk( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, lapack_complex_float* a,
                          lapack_int lda, lapack_complex_float* e, lapack_int* ipiv,
                          lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_chesv_rk_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, lapack_complex_float* a,
                               lapack_int lda, lapack_complex_float* e, lapack_int* ipiv,
                               lapack_complex_float* b, lapack_int ldb,
                               lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zhesv_rk( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, lapack_complex_double* a,
                          lapack_int lda, lapack_complex_double* e, lapack_int* ipiv,
                          lapack_complex_double* b, lapack_int ldb );
lapack_int LAPACKE_zhesv_rk_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, lapack_complex_double* a,
                               lapack_int lda, lapack_complex_double* e, lapack_int* ipiv,
                               lapack_complex_double* b, lapack_int ldb,
                               lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_ssytrf_rk( int matrix_layout, char uplo, lapack_int n, float* a,
                           lapack_int lda, float* e, lapack_int* ipiv );
lapack_int LAPACKE_dsytrf_rk( int matrix_layout, char uplo, lapack_int n, double* a,
                           lapack_int lda, double* e, lapack_int* ipiv );
lapack_int LAPACKE_csytrf_rk( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_complex_float* e, lapack_int* ipiv );
lapack_int LAPACKE_zsytrf_rk( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_complex_double* e, lapack_int* ipiv );
lapack_int LAPACKE_chetrf_rk( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           lapack_complex_float* e, lapack_int* ipiv );
lapack_int LAPACKE_zhetrf_rk( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           lapack_complex_double* e, lapack_int* ipiv );
lapack_int LAPACKE_ssytrf_rk_work( int matrix_layout, char uplo, lapack_int n,
                                float* a, lapack_int lda, float* e, lapack_int* ipiv,
                                float* work, lapack_int lwork );
lapack_int LAPACKE_dsytrf_rk_work( int matrix_layout, char uplo, lapack_int n,
                                double* a, lapack_int lda, double* e, lapack_int* ipiv,
                                double* work, lapack_int lwork );
lapack_int LAPACKE_csytrf_rk_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_complex_float* e,
                                lapack_int* ipiv, lapack_complex_float* work,
                                lapack_int lwork );
lapack_int LAPACKE_zsytrf_rk_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* e,
                                lapack_int* ipiv, lapack_complex_double* work,
                                lapack_int lwork );
lapack_int LAPACKE_chetrf_rk_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                lapack_complex_float* e,
                                lapack_int* ipiv, lapack_complex_float* work,
                                lapack_int lwork );
lapack_int LAPACKE_zhetrf_rk_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* e,
                                lapack_int* ipiv, lapack_complex_double* work,
                                lapack_int lwork );

lapack_int LAPACKE_csytrs_3( int matrix_layout, char uplo, lapack_int n,
                            lapack_int nrhs, const lapack_complex_float* a,
                            lapack_int lda, const lapack_complex_float* e,
                            const lapack_int* ipiv,
                            lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_csytrs_3_work( int matrix_layout, char uplo, lapack_int n,
                                 lapack_int nrhs, const lapack_complex_float* a,
                                 lapack_int lda, const lapack_complex_float* e,
                                 const lapack_int* ipiv,
                                 lapack_complex_float* b, lapack_int ldb);
lapack_int LAPACKE_chetrs_3( int matrix_layout, char uplo, lapack_int n,
                            lapack_int nrhs, const lapack_complex_float* a,
                            lapack_int lda, const lapack_complex_float* e,
                            const lapack_int* ipiv,
                            lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_chetrs_3_work( int matrix_layout, char uplo, lapack_int n,
                                 lapack_int nrhs, const lapack_complex_float* a,
                                 lapack_int lda, const lapack_complex_float* e,
                                 const lapack_int* ipiv,
                                 lapack_complex_float* b, lapack_int ldb);
lapack_int LAPACKE_dsytrs_3( int matrix_layout, char uplo, lapack_int n,
                            lapack_int nrhs, const double* a, lapack_int lda,
                            const double* e,
                            const lapack_int* ipiv, double* b, lapack_int ldb );
lapack_int LAPACKE_dsytrs_3_work( int matrix_layout, char uplo, lapack_int n,
                                 lapack_int nrhs, const double* a,
                                 lapack_int lda, const double* e,
                                 const lapack_int* ipiv,
                                 double* b, lapack_int ldb);
lapack_int LAPACKE_ssytrs_3( int matrix_layout, char uplo, lapack_int n,
                           lapack_int nrhs, const float* a, lapack_int lda,
                           const float* e,
                           const lapack_int* ipiv, float* b, lapack_int ldb );
lapack_int LAPACKE_ssytrs_3_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int nrhs, const float* a, lapack_int lda,
                                const float* e, const lapack_int* ipiv, float* b,
                                lapack_int ldb);
lapack_int LAPACKE_zsytrs_3( int matrix_layout, char uplo, lapack_int n,
                            lapack_int nrhs, const lapack_complex_double* a,
                            lapack_int lda, const lapack_complex_double* e,
                            const lapack_int* ipiv,
                            lapack_complex_double* b, lapack_int ldb );
lapack_int LAPACKE_zsytrs_3_work( int matrix_layout, char uplo, lapack_int n,
                                 lapack_int nrhs, const lapack_complex_double* a,
                                 lapack_int lda, const lapack_complex_double* e,
                                 const lapack_int* ipiv,
                                 lapack_complex_double* b, lapack_int ldb);
lapack_int LAPACKE_zhetrs_3( int matrix_layout, char uplo, lapack_int n,
                            lapack_int nrhs, const lapack_complex_double* a,
                            lapack_int lda, const lapack_complex_double* e,
                            const lapack_int* ipiv,
                            lapack_complex_double* b, lapack_int ldb );
lapack_int LAPACKE_zhetrs_3_work( int matrix_layout, char uplo, lapack_int n,
                                 lapack_int nrhs, const lapack_complex_double* a,
                                 lapack_int lda, const lapack_complex_double* e,
                                 const lapack_int* ipiv,
                                 lapack_complex_double* b, lapack_int ldb);

lapack_int LAPACKE_ssytri_3( int matrix_layout, char uplo, lapack_int n, float* a,
                           lapack_int lda, const float* e, const lapack_int* ipiv );
lapack_int LAPACKE_dsytri_3( int matrix_layout, char uplo, lapack_int n, double* a,
                           lapack_int lda, const double* e, const lapack_int* ipiv );
lapack_int LAPACKE_csytri_3( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           const lapack_complex_float* e, const lapack_int* ipiv );
lapack_int LAPACKE_zsytri_3( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           const lapack_complex_double* e, const lapack_int* ipiv );
lapack_int LAPACKE_chetri_3( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           const lapack_complex_float* e, const lapack_int* ipiv );
lapack_int LAPACKE_zhetri_3( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           const lapack_complex_double* e, const lapack_int* ipiv );
lapack_int LAPACKE_ssytri_3_work( int matrix_layout, char uplo, lapack_int n, float* a,
                           lapack_int lda, const float* e, const lapack_int* ipiv,
                           float* work, lapack_int lwork  );
lapack_int LAPACKE_dsytri_3_work( int matrix_layout, char uplo, lapack_int n, double* a,
                           lapack_int lda, const double* e, const lapack_int* ipiv,
                           double* work, lapack_int lwork  );
lapack_int LAPACKE_csytri_3_work( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           const lapack_complex_float* e, const lapack_int* ipiv,
                           lapack_complex_float* work, lapack_int lwork  );
lapack_int LAPACKE_zsytri_3_work( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           const lapack_complex_double* e, const lapack_int* ipiv,
                           lapack_complex_double* work, lapack_int lwork  );
lapack_int LAPACKE_chetri_3_work( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_float* a, lapack_int lda,
                           const lapack_complex_float* e, const lapack_int* ipiv,
                           lapack_complex_float* work, lapack_int lwork  );
lapack_int LAPACKE_zhetri_3_work( int matrix_layout, char uplo, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           const lapack_complex_double* e, const lapack_int* ipiv,
                           lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_ssycon_3( int matrix_layout, char uplo, lapack_int n,
                           const float* a, lapack_int lda, const float* e,
                           const lapack_int* ipiv, float anorm, float* rcond );
lapack_int LAPACKE_dsycon_3( int matrix_layout, char uplo, lapack_int n,
                           const double* a, lapack_int lda, const double* e,
                           const lapack_int* ipiv, double anorm,
                           double* rcond );
lapack_int LAPACKE_csycon_3( int matrix_layout, char uplo, lapack_int n,
                           const lapack_complex_float* a, lapack_int lda,
                           const lapack_complex_float* e,
                           const lapack_int* ipiv, float anorm, float* rcond );
lapack_int LAPACKE_zsycon_3( int matrix_layout, char uplo, lapack_int n,
                           const lapack_complex_double* a, lapack_int lda,
                           const lapack_complex_double* e,
                           const lapack_int* ipiv, double anorm,
                           double* rcond );
lapack_int LAPACKE_checon_3( int matrix_layout, char uplo, lapack_int n,
                           const lapack_complex_float* a, lapack_int lda,
                           const lapack_complex_float* e,
                           const lapack_int* ipiv, float anorm, float* rcond );
lapack_int LAPACKE_zhecon_3( int matrix_layout, char uplo, lapack_int n,
                           const lapack_complex_double* a, lapack_int lda,
                           const lapack_complex_double* e,
                           const lapack_int* ipiv, double anorm,
                           double* rcond );
lapack_int LAPACKE_ssycon_3_work( int matrix_layout, char uplo, lapack_int n,
                                const float* a, lapack_int lda, const float* e,
                                const lapack_int* ipiv, float anorm,
                                float* rcond, float* work, lapack_int* iwork );
lapack_int LAPACKE_dsycon_3_work( int matrix_layout, char uplo, lapack_int n,
                                const double* a, lapack_int lda, const double* e,
                                const lapack_int* ipiv, double anorm,
                                double* rcond, double* work,
                                lapack_int* iwork );
lapack_int LAPACKE_csycon_3_work( int matrix_layout, char uplo, lapack_int n,
                                const lapack_complex_float* a, lapack_int lda,
                                const lapack_complex_float* e,
                                const lapack_int* ipiv, float anorm,
                                float* rcond, lapack_complex_float* work );
lapack_int LAPACKE_zsycon_3_work( int matrix_layout, char uplo, lapack_int n,
                                const lapack_complex_double* a, lapack_int lda,
                                const lapack_complex_double* e,
                                const lapack_int* ipiv, double anorm,
                                double* rcond, lapack_complex_double* work );
lapack_int LAPACKE_checon_3_work( int matrix_layout, char uplo, lapack_int n,
                                const lapack_complex_float* a, lapack_int lda,
                                const lapack_complex_float* e,
                                const lapack_int* ipiv, float anorm,
                                float* rcond, lapack_complex_float* work );
lapack_int LAPACKE_zhecon_3_work( int matrix_layout, char uplo, lapack_int n,
                                const lapack_complex_double* a, lapack_int lda,
                                const lapack_complex_double* e,
                                const lapack_int* ipiv, double anorm,
                                double* rcond, lapack_complex_double* work );

lapack_int LAPACKE_sgelq( int matrix_layout, lapack_int m, lapack_int n,
                          float* a, lapack_int lda,
                          float* t, lapack_int tsize );
lapack_int LAPACKE_dgelq( int matrix_layout, lapack_int m, lapack_int n,
                          double* a, lapack_int lda,
                          double* t, lapack_int tsize );
lapack_int LAPACKE_cgelq( int matrix_layout, lapack_int m, lapack_int n,
                          lapack_complex_float* a, lapack_int lda,
                          lapack_complex_float* t, lapack_int tsize );
lapack_int LAPACKE_zgelq( int matrix_layout, lapack_int m, lapack_int n,
                          lapack_complex_double* a, lapack_int lda,
                          lapack_complex_double* t, lapack_int tsize );

lapack_int LAPACKE_sgelq_work( int matrix_layout, lapack_int m, lapack_int n,
                               float* a, lapack_int lda,
                               float* t, lapack_int tsize,
                               float* work, lapack_int lwork );
lapack_int LAPACKE_dgelq_work( int matrix_layout, lapack_int m, lapack_int n,
                               double* a, lapack_int lda,
                               double* t, lapack_int tsize,
                               double* work, lapack_int lwork );
lapack_int LAPACKE_cgelq_work( int matrix_layout, lapack_int m, lapack_int n,
                               lapack_complex_float* a, lapack_int lda,
                               lapack_complex_float* t, lapack_int tsize,
                               lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zgelq_work( int matrix_layout, lapack_int m, lapack_int n,
                               lapack_complex_double* a, lapack_int lda,
                               lapack_complex_double* t, lapack_int tsize,
                               lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_sgemlq( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           const float* a, lapack_int lda,
                           const float* t, lapack_int tsize,
                           float* c, lapack_int ldc );
lapack_int LAPACKE_dgemlq( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           const double* a, lapack_int lda,
                           const double* t, lapack_int tsize,
                           double* c, lapack_int ldc );
lapack_int LAPACKE_cgemlq( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           const lapack_complex_float* a, lapack_int lda,
                           const lapack_complex_float* t, lapack_int tsize,
                           lapack_complex_float* c, lapack_int ldc );
lapack_int LAPACKE_zgemlq( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           const lapack_complex_double* a, lapack_int lda,
                           const lapack_complex_double* t, lapack_int tsize,
                           lapack_complex_double* c, lapack_int ldc );

lapack_int LAPACKE_sgemlq_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int k,
                                const float* a, lapack_int lda,
                                const float* t, lapack_int tsize,
                                float* c, lapack_int ldc,
                                float* work, lapack_int lwork );
lapack_int LAPACKE_dgemlq_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int k,
                                const double* a, lapack_int lda,
                                const double* t, lapack_int tsize,
                                double* c, lapack_int ldc,
                                double* work, lapack_int lwork );
lapack_int LAPACKE_cgemlq_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int k,
                                const lapack_complex_float* a, lapack_int lda,
                                const lapack_complex_float* t, lapack_int tsize,
                                lapack_complex_float* c, lapack_int ldc,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zgemlq_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int k,
                                const lapack_complex_double* a, lapack_int lda,
                                const lapack_complex_double* t, lapack_int tsize,
                                lapack_complex_double* c, lapack_int ldc,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_sgeqr( int matrix_layout, lapack_int m, lapack_int n,
                          float* a, lapack_int lda,
                          float* t, lapack_int tsize );
lapack_int LAPACKE_dgeqr( int matrix_layout, lapack_int m, lapack_int n,
                          double* a, lapack_int lda,
                          double* t, lapack_int tsize );
lapack_int LAPACKE_cgeqr( int matrix_layout, lapack_int m, lapack_int n,
                          lapack_complex_float* a, lapack_int lda,
                          lapack_complex_float* t, lapack_int tsize );
lapack_int LAPACKE_zgeqr( int matrix_layout, lapack_int m, lapack_int n,
                          lapack_complex_double* a, lapack_int lda,
                          lapack_complex_double* t, lapack_int tsize );

lapack_int LAPACKE_sgeqr_work( int matrix_layout, lapack_int m, lapack_int n,
                               float* a, lapack_int lda,
                               float* t, lapack_int tsize,
                               float* work, lapack_int lwork );
lapack_int LAPACKE_dgeqr_work( int matrix_layout, lapack_int m, lapack_int n,
                               double* a, lapack_int lda,
                               double* t, lapack_int tsize,
                               double* work, lapack_int lwork );
lapack_int LAPACKE_cgeqr_work( int matrix_layout, lapack_int m, lapack_int n,
                               lapack_complex_float* a, lapack_int lda,
                               lapack_complex_float* t, lapack_int tsize,
                               lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zgeqr_work( int matrix_layout, lapack_int m, lapack_int n,
                               lapack_complex_double* a, lapack_int lda,
                               lapack_complex_double* t, lapack_int tsize,
                               lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_sgemqr( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           const float* a, lapack_int lda,
                           const float* t, lapack_int tsize,
                           float* c, lapack_int ldc );
lapack_int LAPACKE_dgemqr( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           const double* a, lapack_int lda,
                           const double* t, lapack_int tsize,
                           double* c, lapack_int ldc );
lapack_int LAPACKE_cgemqr( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           const lapack_complex_float* a, lapack_int lda,
                           const lapack_complex_float* t, lapack_int tsize,
                           lapack_complex_float* c, lapack_int ldc );
lapack_int LAPACKE_zgemqr( int matrix_layout, char side, char trans,
                           lapack_int m, lapack_int n, lapack_int k,
                           const lapack_complex_double* a, lapack_int lda,
                           const lapack_complex_double* t, lapack_int tsize,
                           lapack_complex_double* c, lapack_int ldc );

lapack_int LAPACKE_sgemqr_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int k,
                                const float* a, lapack_int lda,
                                const float* t, lapack_int tsize,
                                float* c, lapack_int ldc,
                                float* work, lapack_int lwork );
lapack_int LAPACKE_dgemqr_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int k,
                                const double* a, lapack_int lda,
                                const double* t, lapack_int tsize,
                                double* c, lapack_int ldc,
                                double* work, lapack_int lwork );
lapack_int LAPACKE_cgemqr_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int k,
                                const lapack_complex_float* a, lapack_int lda,
                                const lapack_complex_float* t, lapack_int tsize,
                                lapack_complex_float* c, lapack_int ldc,
                                lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zgemqr_work( int matrix_layout, char side, char trans,
                                lapack_int m, lapack_int n, lapack_int k,
                                const lapack_complex_double* a, lapack_int lda,
                                const lapack_complex_double* t, lapack_int tsize,
                                lapack_complex_double* c, lapack_int ldc,
                                lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_sgetsls( int matrix_layout, char trans, lapack_int m,
                            lapack_int n, lapack_int nrhs, float* a,
                            lapack_int lda, float* b, lapack_int ldb );
lapack_int LAPACKE_dgetsls( int matrix_layout, char trans, lapack_int m,
                            lapack_int n, lapack_int nrhs, double* a,
                            lapack_int lda, double* b, lapack_int ldb );
lapack_int LAPACKE_cgetsls( int matrix_layout, char trans, lapack_int m,
                            lapack_int n, lapack_int nrhs,
                            lapack_complex_float* a, lapack_int lda,
                            lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zgetsls( int matrix_layout, char trans, lapack_int m,
                            lapack_int n, lapack_int nrhs,
                            lapack_complex_double* a, lapack_int lda,
                            lapack_complex_double* b, lapack_int ldb );

lapack_int LAPACKE_sgetsls_work( int matrix_layout, char trans, lapack_int m,
                                 lapack_int n, lapack_int nrhs, float* a,
                                 lapack_int lda, float* b, lapack_int ldb,
                                 float* work, lapack_int lwork );
lapack_int LAPACKE_dgetsls_work( int matrix_layout, char trans, lapack_int m,
                                 lapack_int n, lapack_int nrhs, double* a,
                                 lapack_int lda, double* b, lapack_int ldb,
                                 double* work, lapack_int lwork );
lapack_int LAPACKE_cgetsls_work( int matrix_layout, char trans, lapack_int m,
                                 lapack_int n, lapack_int nrhs,
                                 lapack_complex_float* a, lapack_int lda,
                                 lapack_complex_float* b, lapack_int ldb,
                                 lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zgetsls_work( int matrix_layout, char trans, lapack_int m,
                                 lapack_int n, lapack_int nrhs,
                                 lapack_complex_double* a, lapack_int lda,
                                 lapack_complex_double* b, lapack_int ldb,
                                 lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_sgetsqrhrt( int matrix_layout, lapack_int m, lapack_int n,
                               lapack_int mb1, lapack_int nb1, lapack_int nb2,
                               float* a, lapack_int lda,
                               float* t, lapack_int ldt );
lapack_int LAPACKE_dgetsqrhrt( int matrix_layout, lapack_int m, lapack_int n,
                               lapack_int mb1, lapack_int nb1, lapack_int nb2,
                               double* a, lapack_int lda,
                               double* t, lapack_int ldt );
lapack_int LAPACKE_cgetsqrhrt( int matrix_layout, lapack_int m, lapack_int n,
                               lapack_int mb1, lapack_int nb1, lapack_int nb2,
                               lapack_complex_float* a, lapack_int lda,
                               lapack_complex_float* t, lapack_int ldt );
lapack_int LAPACKE_zgetsqrhrt( int matrix_layout, lapack_int m, lapack_int n,
                               lapack_int mb1, lapack_int nb1, lapack_int nb2,
                               lapack_complex_double* a, lapack_int lda,
                               lapack_complex_double* t, lapack_int ldt );

lapack_int LAPACKE_sgetsqrhrt_work( int matrix_layout, lapack_int m, lapack_int n,
                                    lapack_int mb1, lapack_int nb1, lapack_int nb2,
                                    float* a, lapack_int lda,
                                    float* t, lapack_int ldt,
                                    float* work, lapack_int lwork );
lapack_int LAPACKE_dgetsqrhrt_work( int matrix_layout, lapack_int m, lapack_int n,
                                    lapack_int mb1, lapack_int nb1, lapack_int nb2,
                                    double* a, lapack_int lda,
                                    double* t, lapack_int ldt,
                                    double* work, lapack_int lwork );
lapack_int LAPACKE_cgetsqrhrt_work( int matrix_layout, lapack_int m, lapack_int n,
                                    lapack_int mb1, lapack_int nb1, lapack_int nb2,
                                    lapack_complex_float* a, lapack_int lda,
                                    lapack_complex_float* t, lapack_int ldt,
                                    lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zgetsqrhrt_work( int matrix_layout, lapack_int m, lapack_int n,
                                    lapack_int mb1, lapack_int nb1, lapack_int nb2,
                                    lapack_complex_double* a, lapack_int lda,
                                    lapack_complex_double* t, lapack_int ldt,
                                    lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_ssyev_2stage( int matrix_layout, char jobz, char uplo, lapack_int n,
                          float* a, lapack_int lda, float* w );
lapack_int LAPACKE_dsyev_2stage( int matrix_layout, char jobz, char uplo, lapack_int n,
                          double* a, lapack_int lda, double* w );

lapack_int LAPACKE_ssyevd_2stage( int matrix_layout, char jobz, char uplo, lapack_int n,
                           float* a, lapack_int lda, float* w );
lapack_int LAPACKE_dsyevd_2stage( int matrix_layout, char jobz, char uplo, lapack_int n,
                           double* a, lapack_int lda, double* w );

lapack_int LAPACKE_ssyevr_2stage( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, float* a, lapack_int lda, float vl,
                           float vu, lapack_int il, lapack_int iu, float abstol,
                           lapack_int* m, float* w, float* z, lapack_int ldz,
                           lapack_int* isuppz );
lapack_int LAPACKE_dsyevr_2stage( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, double* a, lapack_int lda, double vl,
                           double vu, lapack_int il, lapack_int iu,
                           double abstol, lapack_int* m, double* w, double* z,
                           lapack_int ldz, lapack_int* isuppz );

lapack_int LAPACKE_ssyevx_2stage( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, float* a, lapack_int lda, float vl,
                           float vu, lapack_int il, lapack_int iu, float abstol,
                           lapack_int* m, float* w, float* z, lapack_int ldz,
                           lapack_int* ifail );
lapack_int LAPACKE_dsyevx_2stage( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, double* a, lapack_int lda, double vl,
                           double vu, lapack_int il, lapack_int iu,
                           double abstol, lapack_int* m, double* w, double* z,
                           lapack_int ldz, lapack_int* ifail );

lapack_int LAPACKE_ssyev_2stage_work( int matrix_layout, char jobz, char uplo,
                               lapack_int n, float* a, lapack_int lda, float* w,
                               float* work, lapack_int lwork );
lapack_int LAPACKE_dsyev_2stage_work( int matrix_layout, char jobz, char uplo,
                               lapack_int n, double* a, lapack_int lda,
                               double* w, double* work, lapack_int lwork );

lapack_int LAPACKE_ssyevd_2stage_work( int matrix_layout, char jobz, char uplo,
                                lapack_int n, float* a, lapack_int lda,
                                float* w, float* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );
lapack_int LAPACKE_dsyevd_2stage_work( int matrix_layout, char jobz, char uplo,
                                lapack_int n, double* a, lapack_int lda,
                                double* w, double* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );

lapack_int LAPACKE_ssyevr_2stage_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n, float* a,
                                lapack_int lda, float vl, float vu,
                                lapack_int il, lapack_int iu, float abstol,
                                lapack_int* m, float* w, float* z,
                                lapack_int ldz, lapack_int* isuppz, float* work,
                                lapack_int lwork, lapack_int* iwork,
                                lapack_int liwork );
lapack_int LAPACKE_dsyevr_2stage_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n, double* a,
                                lapack_int lda, double vl, double vu,
                                lapack_int il, lapack_int iu, double abstol,
                                lapack_int* m, double* w, double* z,
                                lapack_int ldz, lapack_int* isuppz,
                                double* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );

lapack_int LAPACKE_ssyevx_2stage_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n, float* a,
                                lapack_int lda, float vl, float vu,
                                lapack_int il, lapack_int iu, float abstol,
                                lapack_int* m, float* w, float* z,
                                lapack_int ldz, float* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int* ifail );
lapack_int LAPACKE_dsyevx_2stage_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n, double* a,
                                lapack_int lda, double vl, double vu,
                                lapack_int il, lapack_int iu, double abstol,
                                lapack_int* m, double* w, double* z,
                                lapack_int ldz, double* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int* ifail );

lapack_int LAPACKE_cheev_2stage( int matrix_layout, char jobz, char uplo, lapack_int n,
                          lapack_complex_float* a, lapack_int lda, float* w );
lapack_int LAPACKE_zheev_2stage( int matrix_layout, char jobz, char uplo, lapack_int n,
                          lapack_complex_double* a, lapack_int lda, double* w );

lapack_int LAPACKE_cheevd_2stage( int matrix_layout, char jobz, char uplo, lapack_int n,
                           lapack_complex_float* a, lapack_int lda, float* w );
lapack_int LAPACKE_zheevd_2stage( int matrix_layout, char jobz, char uplo, lapack_int n,
                           lapack_complex_double* a, lapack_int lda,
                           double* w );

lapack_int LAPACKE_cheevr_2stage( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, lapack_complex_float* a,
                           lapack_int lda, float vl, float vu, lapack_int il,
                           lapack_int iu, float abstol, lapack_int* m, float* w,
                           lapack_complex_float* z, lapack_int ldz,
                           lapack_int* isuppz );
lapack_int LAPACKE_zheevr_2stage( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, lapack_complex_double* a,
                           lapack_int lda, double vl, double vu, lapack_int il,
                           lapack_int iu, double abstol, lapack_int* m,
                           double* w, lapack_complex_double* z, lapack_int ldz,
                           lapack_int* isuppz );

lapack_int LAPACKE_cheevx_2stage( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, lapack_complex_float* a,
                           lapack_int lda, float vl, float vu, lapack_int il,
                           lapack_int iu, float abstol, lapack_int* m, float* w,
                           lapack_complex_float* z, lapack_int ldz,
                           lapack_int* ifail );
lapack_int LAPACKE_zheevx_2stage( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, lapack_complex_double* a,
                           lapack_int lda, double vl, double vu, lapack_int il,
                           lapack_int iu, double abstol, lapack_int* m,
                           double* w, lapack_complex_double* z, lapack_int ldz,
                           lapack_int* ifail );

lapack_int LAPACKE_cheev_2stage_work( int matrix_layout, char jobz, char uplo,
                               lapack_int n, lapack_complex_float* a,
                               lapack_int lda, float* w,
                               lapack_complex_float* work, lapack_int lwork,
                               float* rwork );
lapack_int LAPACKE_zheev_2stage_work( int matrix_layout, char jobz, char uplo,
                               lapack_int n, lapack_complex_double* a,
                               lapack_int lda, double* w,
                               lapack_complex_double* work, lapack_int lwork,
                               double* rwork );

lapack_int LAPACKE_cheevd_2stage_work( int matrix_layout, char jobz, char uplo,
                                lapack_int n, lapack_complex_float* a,
                                lapack_int lda, float* w,
                                lapack_complex_float* work, lapack_int lwork,
                                float* rwork, lapack_int lrwork,
                                lapack_int* iwork, lapack_int liwork );
lapack_int LAPACKE_zheevd_2stage_work( int matrix_layout, char jobz, char uplo,
                                lapack_int n, lapack_complex_double* a,
                                lapack_int lda, double* w,
                                lapack_complex_double* work, lapack_int lwork,
                                double* rwork, lapack_int lrwork,
                                lapack_int* iwork, lapack_int liwork );

lapack_int LAPACKE_cheevr_2stage_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                float vl, float vu, lapack_int il,
                                lapack_int iu, float abstol, lapack_int* m,
                                float* w, lapack_complex_float* z,
                                lapack_int ldz, lapack_int* isuppz,
                                lapack_complex_float* work, lapack_int lwork,
                                float* rwork, lapack_int lrwork,
                                lapack_int* iwork, lapack_int liwork );
lapack_int LAPACKE_zheevr_2stage_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                double vl, double vu, lapack_int il,
                                lapack_int iu, double abstol, lapack_int* m,
                                double* w, lapack_complex_double* z,
                                lapack_int ldz, lapack_int* isuppz,
                                lapack_complex_double* work, lapack_int lwork,
                                double* rwork, lapack_int lrwork,
                                lapack_int* iwork, lapack_int liwork );

lapack_int LAPACKE_cheevx_2stage_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n,
                                lapack_complex_float* a, lapack_int lda,
                                float vl, float vu, lapack_int il,
                                lapack_int iu, float abstol, lapack_int* m,
                                float* w, lapack_complex_float* z,
                                lapack_int ldz, lapack_complex_float* work,
                                lapack_int lwork, float* rwork,
                                lapack_int* iwork, lapack_int* ifail );
lapack_int LAPACKE_zheevx_2stage_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                double vl, double vu, lapack_int il,
                                lapack_int iu, double abstol, lapack_int* m,
                                double* w, lapack_complex_double* z,
                                lapack_int ldz, lapack_complex_double* work,
                                lapack_int lwork, double* rwork,
                                lapack_int* iwork, lapack_int* ifail );

lapack_int LAPACKE_ssbev_2stage( int matrix_layout, char jobz, char uplo, lapack_int n,
                          lapack_int kd, float* ab, lapack_int ldab, float* w,
                          float* z, lapack_int ldz );
lapack_int LAPACKE_dsbev_2stage( int matrix_layout, char jobz, char uplo, lapack_int n,
                          lapack_int kd, double* ab, lapack_int ldab, double* w,
                          double* z, lapack_int ldz );

lapack_int LAPACKE_ssbevd_2stage( int matrix_layout, char jobz, char uplo, lapack_int n,
                           lapack_int kd, float* ab, lapack_int ldab, float* w,
                           float* z, lapack_int ldz );
lapack_int LAPACKE_dsbevd_2stage( int matrix_layout, char jobz, char uplo, lapack_int n,
                           lapack_int kd, double* ab, lapack_int ldab,
                           double* w, double* z, lapack_int ldz );

lapack_int LAPACKE_ssbevx_2stage( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, lapack_int kd, float* ab,
                           lapack_int ldab, float* q, lapack_int ldq, float vl,
                           float vu, lapack_int il, lapack_int iu, float abstol,
                           lapack_int* m, float* w, float* z, lapack_int ldz,
                           lapack_int* ifail );
lapack_int LAPACKE_dsbevx_2stage( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, lapack_int kd, double* ab,
                           lapack_int ldab, double* q, lapack_int ldq,
                           double vl, double vu, lapack_int il, lapack_int iu,
                           double abstol, lapack_int* m, double* w, double* z,
                           lapack_int ldz, lapack_int* ifail );

lapack_int LAPACKE_ssbev_2stage_work( int matrix_layout, char jobz, char uplo,
                               lapack_int n, lapack_int kd, float* ab,
                               lapack_int ldab, float* w, float* z,
                               lapack_int ldz, float* work, lapack_int lwork );
lapack_int LAPACKE_dsbev_2stage_work( int matrix_layout, char jobz, char uplo,
                               lapack_int n, lapack_int kd, double* ab,
                               lapack_int ldab, double* w, double* z,
                               lapack_int ldz, double* work, lapack_int lwork );

lapack_int LAPACKE_ssbevd_2stage_work( int matrix_layout, char jobz, char uplo,
                                lapack_int n, lapack_int kd, float* ab,
                                lapack_int ldab, float* w, float* z,
                                lapack_int ldz, float* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );
lapack_int LAPACKE_dsbevd_2stage_work( int matrix_layout, char jobz, char uplo,
                                lapack_int n, lapack_int kd, double* ab,
                                lapack_int ldab, double* w, double* z,
                                lapack_int ldz, double* work, lapack_int lwork,
                                lapack_int* iwork, lapack_int liwork );

lapack_int LAPACKE_ssbevx_2stage_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n, lapack_int kd,
                                float* ab, lapack_int ldab, float* q,
                                lapack_int ldq, float vl, float vu,
                                lapack_int il, lapack_int iu, float abstol,
                                lapack_int* m, float* w, float* z,
                                lapack_int ldz, float* work, lapack_int lwork, lapack_int* iwork,
                                lapack_int* ifail );
lapack_int LAPACKE_dsbevx_2stage_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n, lapack_int kd,
                                double* ab, lapack_int ldab, double* q,
                                lapack_int ldq, double vl, double vu,
                                lapack_int il, lapack_int iu, double abstol,
                                lapack_int* m, double* w, double* z,
                                lapack_int ldz, double* work, lapack_int lwork, lapack_int* iwork,
                                lapack_int* ifail );

lapack_int LAPACKE_chbev_2stage( int matrix_layout, char jobz, char uplo, lapack_int n,
                          lapack_int kd, lapack_complex_float* ab,
                          lapack_int ldab, float* w, lapack_complex_float* z,
                          lapack_int ldz );
lapack_int LAPACKE_zhbev_2stage( int matrix_layout, char jobz, char uplo, lapack_int n,
                          lapack_int kd, lapack_complex_double* ab,
                          lapack_int ldab, double* w, lapack_complex_double* z,
                          lapack_int ldz );

lapack_int LAPACKE_chbevd_2stage( int matrix_layout, char jobz, char uplo, lapack_int n,
                           lapack_int kd, lapack_complex_float* ab,
                           lapack_int ldab, float* w, lapack_complex_float* z,
                           lapack_int ldz );
lapack_int LAPACKE_zhbevd_2stage( int matrix_layout, char jobz, char uplo, lapack_int n,
                           lapack_int kd, lapack_complex_double* ab,
                           lapack_int ldab, double* w, lapack_complex_double* z,
                           lapack_int ldz );

lapack_int LAPACKE_chbevx_2stage( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, lapack_int kd,
                           lapack_complex_float* ab, lapack_int ldab,
                           lapack_complex_float* q, lapack_int ldq, float vl,
                           float vu, lapack_int il, lapack_int iu, float abstol,
                           lapack_int* m, float* w, lapack_complex_float* z,
                           lapack_int ldz, lapack_int* ifail );
lapack_int LAPACKE_zhbevx_2stage( int matrix_layout, char jobz, char range, char uplo,
                           lapack_int n, lapack_int kd,
                           lapack_complex_double* ab, lapack_int ldab,
                           lapack_complex_double* q, lapack_int ldq, double vl,
                           double vu, lapack_int il, lapack_int iu,
                           double abstol, lapack_int* m, double* w,
                           lapack_complex_double* z, lapack_int ldz,
                           lapack_int* ifail );

lapack_int LAPACKE_chbev_2stage_work( int matrix_layout, char jobz, char uplo,
                               lapack_int n, lapack_int kd,
                               lapack_complex_float* ab, lapack_int ldab,
                               float* w, lapack_complex_float* z,
                               lapack_int ldz, lapack_complex_float* work,
                               lapack_int lwork, float* rwork );
lapack_int LAPACKE_zhbev_2stage_work( int matrix_layout, char jobz, char uplo,
                               lapack_int n, lapack_int kd,
                               lapack_complex_double* ab, lapack_int ldab,
                               double* w, lapack_complex_double* z,
                               lapack_int ldz, lapack_complex_double* work,
                               lapack_int lwork, double* rwork );

lapack_int LAPACKE_chbevd_2stage_work( int matrix_layout, char jobz, char uplo,
                                lapack_int n, lapack_int kd,
                                lapack_complex_float* ab, lapack_int ldab,
                                float* w, lapack_complex_float* z,
                                lapack_int ldz, lapack_complex_float* work,
                                lapack_int lwork, float* rwork,
                                lapack_int lrwork, lapack_int* iwork,
                                lapack_int liwork );
lapack_int LAPACKE_zhbevd_2stage_work( int matrix_layout, char jobz, char uplo,
                                lapack_int n, lapack_int kd,
                                lapack_complex_double* ab, lapack_int ldab,
                                double* w, lapack_complex_double* z,
                                lapack_int ldz, lapack_complex_double* work,
                                lapack_int lwork, double* rwork,
                                lapack_int lrwork, lapack_int* iwork,
                                lapack_int liwork );

lapack_int LAPACKE_chbevx_2stage_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n, lapack_int kd,
                                lapack_complex_float* ab, lapack_int ldab,
                                lapack_complex_float* q, lapack_int ldq,
                                float vl, float vu, lapack_int il,
                                lapack_int iu, float abstol, lapack_int* m,
                                float* w, lapack_complex_float* z,
                                lapack_int ldz, lapack_complex_float* work,
                                lapack_int lwork, float* rwork, lapack_int* iwork,
                                lapack_int* ifail );
lapack_int LAPACKE_zhbevx_2stage_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n, lapack_int kd,
                                lapack_complex_double* ab, lapack_int ldab,
                                lapack_complex_double* q, lapack_int ldq,
                                double vl, double vu, lapack_int il,
                                lapack_int iu, double abstol, lapack_int* m,
                                double* w, lapack_complex_double* z,
                                lapack_int ldz, lapack_complex_double* work,
                                lapack_int lwork, double* rwork, lapack_int* iwork,
                                lapack_int* ifail );

lapack_int LAPACKE_ssygv_2stage( int matrix_layout, lapack_int itype, char jobz,
                          char uplo, lapack_int n, float* a, lapack_int lda,
                          float* b, lapack_int ldb, float* w );
lapack_int LAPACKE_dsygv_2stage( int matrix_layout, lapack_int itype, char jobz,
                          char uplo, lapack_int n, double* a, lapack_int lda,
                          double* b, lapack_int ldb, double* w );
lapack_int LAPACKE_ssygv_2stage_work( int matrix_layout, lapack_int itype, char jobz,
                               char uplo, lapack_int n, float* a,
                               lapack_int lda, float* b, lapack_int ldb,
                               float* w, float* work, lapack_int lwork );
lapack_int LAPACKE_dsygv_2stage_work( int matrix_layout, lapack_int itype, char jobz,
                               char uplo, lapack_int n, double* a,
                               lapack_int lda, double* b, lapack_int ldb,
                               double* w, double* work, lapack_int lwork );

lapack_int LAPACKE_chegv_2stage( int matrix_layout, lapack_int itype, char jobz,
                          char uplo, lapack_int n, lapack_complex_float* a,
                          lapack_int lda, lapack_complex_float* b,
                          lapack_int ldb, float* w );
lapack_int LAPACKE_zhegv_2stage( int matrix_layout, lapack_int itype, char jobz,
                          char uplo, lapack_int n, lapack_complex_double* a,
                          lapack_int lda, lapack_complex_double* b,
                          lapack_int ldb, double* w );
lapack_int LAPACKE_chegv_2stage_work( int matrix_layout, lapack_int itype, char jobz,
                               char uplo, lapack_int n, lapack_complex_float* a,
                               lapack_int lda, lapack_complex_float* b,
                               lapack_int ldb, float* w,
                               lapack_complex_float* work, lapack_int lwork,
                               float* rwork );
lapack_int LAPACKE_zhegv_2stage_work( int matrix_layout, lapack_int itype, char jobz,
                               char uplo, lapack_int n,
                               lapack_complex_double* a, lapack_int lda,
                               lapack_complex_double* b, lapack_int ldb,
                               double* w, lapack_complex_double* work,
                               lapack_int lwork, double* rwork );

//LAPACK 3.8.0
lapack_int LAPACKE_ssysv_aa_2stage( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, float* a, lapack_int lda,
                          float* tb, lapack_int ltb, lapack_int* ipiv,
                          lapack_int* ipiv2, float* b, lapack_int ldb );
lapack_int LAPACKE_ssysv_aa_2stage_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, float* a, lapack_int lda,
                               float* tb, lapack_int ltb, lapack_int* ipiv,
                               lapack_int* ipiv2, float* b, lapack_int ldb,
                               float* work, lapack_int lwork );
lapack_int LAPACKE_dsysv_aa_2stage( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, double* a, lapack_int lda,
                          double* tb, lapack_int ltb,
                          lapack_int* ipiv, lapack_int* ipiv2,
                          double* b, lapack_int ldb );
lapack_int LAPACKE_dsysv_aa_2stage_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, double* a, lapack_int lda,
                               double* tb, lapack_int ltb,
                               lapack_int* ipiv, lapack_int* ipiv2,
                               double* b, lapack_int ldb,
                               double* work, lapack_int lwork );
lapack_int LAPACKE_csysv_aa_2stage( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, lapack_complex_float* a,
                          lapack_int lda, lapack_complex_float* tb,
                          lapack_int ltb, lapack_int* ipiv, lapack_int* ipiv2,
                          lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_csysv_aa_2stage_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, lapack_complex_float* a,
                               lapack_int lda, lapack_complex_float* tb,
                               lapack_int ltb, lapack_int* ipiv, lapack_int* ipiv2,
                               lapack_complex_float* b, lapack_int ldb,
                               lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zsysv_aa_2stage( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, lapack_complex_double* a,
                          lapack_int lda, lapack_complex_double* tb,
                          lapack_int ltb, lapack_int* ipiv, lapack_int* ipiv2,
                          lapack_complex_double* b, lapack_int ldb );
lapack_int LAPACKE_zsysv_aa_2stage_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, lapack_complex_double* a,
                               lapack_int lda, lapack_complex_double* tb,
                               lapack_int ltb, lapack_int* ipiv, lapack_int* ipiv2,
                               lapack_complex_double* b, lapack_int ldb,
                               lapack_complex_double* work, lapack_int lwork );
lapack_int LAPACKE_chesv_aa_2stage( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, lapack_complex_float* a,
                          lapack_int lda, lapack_complex_float* tb,
                          lapack_int ltb, lapack_int* ipiv, lapack_int* ipiv2,
                          lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_chesv_aa_2stage_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, lapack_complex_float* a,
                               lapack_int lda, lapack_complex_float* tb,
                               lapack_int ltb, lapack_int* ipiv, lapack_int* ipiv2,
                               lapack_complex_float* b, lapack_int ldb,
                               lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zhesv_aa_2stage( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, lapack_complex_double* a,
                          lapack_int lda, lapack_complex_double* tb,
                          lapack_int ltb, lapack_int* ipiv, lapack_int* ipiv2,
                          lapack_complex_double* b, lapack_int ldb );
lapack_int LAPACKE_zhesv_aa_2stage_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, lapack_complex_double* a,
                               lapack_int lda, lapack_complex_double* tb,
                               lapack_int ltb, lapack_int* ipiv, lapack_int* ipiv2,
                               lapack_complex_double* b, lapack_int ldb,
                               lapack_complex_double* work, lapack_int lwork );

lapack_int LAPACKE_ssytrf_aa_2stage( int matrix_layout, char uplo, lapack_int n,
                          float* a, lapack_int lda,
                          float* tb, lapack_int ltb, lapack_int* ipiv,
                          lapack_int* ipiv2 );
lapack_int LAPACKE_ssytrf_aa_2stage_work( int matrix_layout, char uplo, lapack_int n,
                               float* a, lapack_int lda,
                               float* tb, lapack_int ltb, lapack_int* ipiv,
                               lapack_int* ipiv2,
                               float* work, lapack_int lwork );
lapack_int LAPACKE_dsytrf_aa_2stage( int matrix_layout, char uplo, lapack_int n,
                          double* a, lapack_int lda,
                          double* tb, lapack_int ltb,
                          lapack_int* ipiv, lapack_int* ipiv2 );
lapack_int LAPACKE_dsytrf_aa_2stage_work( int matrix_layout, char uplo, lapack_int n,
                               double* a, lapack_int lda,
                               double* tb, lapack_int ltb,
                               lapack_int* ipiv, lapack_int* ipiv2,
                               double* work, lapack_int lwork );
lapack_int LAPACKE_csytrf_aa_2stage( int matrix_layout, char uplo, lapack_int n,
                          lapack_complex_float* a,
                          lapack_int lda, lapack_complex_float* tb,
                          lapack_int ltb, lapack_int* ipiv, lapack_int* ipiv2 );
lapack_int LAPACKE_csytrf_aa_2stage_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_complex_float* a,
                               lapack_int lda, lapack_complex_float* tb,
                               lapack_int ltb, lapack_int* ipiv, lapack_int* ipiv2,
                               lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zsytrf_aa_2stage( int matrix_layout, char uplo, lapack_int n,
                          lapack_complex_double* a,
                          lapack_int lda, lapack_complex_double* tb,
                          lapack_int ltb, lapack_int* ipiv, lapack_int* ipiv2 );
lapack_int LAPACKE_zsytrf_aa_2stage_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_complex_double* a,
                               lapack_int lda, lapack_complex_double* tb,
                               lapack_int ltb, lapack_int* ipiv, lapack_int* ipiv2,
                               lapack_complex_double* work, lapack_int lwork );
lapack_int LAPACKE_chetrf_aa_2stage( int matrix_layout, char uplo, lapack_int n,
                          lapack_complex_float* a,
                          lapack_int lda, lapack_complex_float* tb,
                          lapack_int ltb, lapack_int* ipiv, lapack_int* ipiv2 );
lapack_int LAPACKE_chetrf_aa_2stage_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_complex_float* a,
                               lapack_int lda, lapack_complex_float* tb,
                               lapack_int ltb, lapack_int* ipiv, lapack_int* ipiv2,
                               lapack_complex_float* work, lapack_int lwork );
lapack_int LAPACKE_zhetrf_aa_2stage( int matrix_layout, char uplo, lapack_int n,
                          lapack_complex_double* a,
                          lapack_int lda, lapack_complex_double* tb,
                          lapack_int ltb, lapack_int* ipiv, lapack_int* ipiv2 );
lapack_int LAPACKE_zhetrf_aa_2stage_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_complex_double* a,
                               lapack_int lda, lapack_complex_double* tb,
                               lapack_int ltb, lapack_int* ipiv, lapack_int* ipiv2,
                               lapack_complex_double* work, lapack_int lwork );


lapack_int LAPACKE_ssytrs_aa_2stage( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, float* a, lapack_int lda,
                          float* tb, lapack_int ltb, lapack_int* ipiv,
                          lapack_int* ipiv2, float* b, lapack_int ldb );
lapack_int LAPACKE_ssytrs_aa_2stage_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, float* a, lapack_int lda,
                               float* tb, lapack_int ltb, lapack_int* ipiv,
                               lapack_int* ipiv2, float* b, lapack_int ldb );
lapack_int LAPACKE_dsytrs_aa_2stage( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, double* a, lapack_int lda,
                          double* tb, lapack_int ltb,
                          lapack_int* ipiv, lapack_int* ipiv2,
                          double* b, lapack_int ldb );
lapack_int LAPACKE_dsytrs_aa_2stage_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, double* a, lapack_int lda,
                               double* tb, lapack_int ltb,
                               lapack_int* ipiv, lapack_int* ipiv2,
                               double* b, lapack_int ldb );
lapack_int LAPACKE_csytrs_aa_2stage( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, lapack_complex_float* a,
                          lapack_int lda, lapack_complex_float* tb,
                          lapack_int ltb, lapack_int* ipiv, lapack_int* ipiv2,
                          lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_csytrs_aa_2stage_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, lapack_complex_float* a,
                               lapack_int lda, lapack_complex_float* tb,
                               lapack_int ltb, lapack_int* ipiv, lapack_int* ipiv2,
                               lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zsytrs_aa_2stage( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, lapack_complex_double* a,
                          lapack_int lda, lapack_complex_double* tb,
                          lapack_int ltb, lapack_int* ipiv, lapack_int* ipiv2,
                          lapack_complex_double* b, lapack_int ldb );
lapack_int LAPACKE_zsytrs_aa_2stage_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, lapack_complex_double* a,
                               lapack_int lda, lapack_complex_double* tb,
                               lapack_int ltb, lapack_int* ipiv, lapack_int* ipiv2,
                               lapack_complex_double* b, lapack_int ldb );
lapack_int LAPACKE_chetrs_aa_2stage( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, lapack_complex_float* a,
                          lapack_int lda, lapack_complex_float* tb,
                          lapack_int ltb, lapack_int* ipiv, lapack_int* ipiv2,
                          lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_chetrs_aa_2stage_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, lapack_complex_float* a,
                               lapack_int lda, lapack_complex_float* tb,
                               lapack_int ltb, lapack_int* ipiv, lapack_int* ipiv2,
                               lapack_complex_float* b, lapack_int ldb );
lapack_int LAPACKE_zhetrs_aa_2stage( int matrix_layout, char uplo, lapack_int n,
                          lapack_int nrhs, lapack_complex_double* a,
                          lapack_int lda, lapack_complex_double* tb,
                          lapack_int ltb, lapack_int* ipiv, lapack_int* ipiv2,
                          lapack_complex_double* b, lapack_int ldb );
lapack_int LAPACKE_zhetrs_aa_2stage_work( int matrix_layout, char uplo, lapack_int n,
                               lapack_int nrhs, lapack_complex_double* a,
                               lapack_int lda, lapack_complex_double* tb,
                               lapack_int ltb, lapack_int* ipiv, lapack_int* ipiv2,
                               lapack_complex_double* b, lapack_int ldb );
//LAPACK 3.10.0
lapack_int LAPACKE_sorhr_col( int matrix_layout, lapack_int m, lapack_int n,
                              lapack_int nb, float* a,
                              lapack_int lda, float* t,
                              lapack_int ldt, float* d );
lapack_int LAPACKE_sorhr_col_work( int matrix_layout, lapack_int m, lapack_int n,
                                   lapack_int nb, float* a,
                                   lapack_int lda, float* t,
                                   lapack_int ldt, float* d );
lapack_int LAPACKE_dorhr_col( int matrix_layout, lapack_int m, lapack_int n,
                              lapack_int nb, double* a,
                              lapack_int lda, double* t,
                              lapack_int ldt, double* d );
lapack_int LAPACKE_dorhr_col_work( int matrix_layout, lapack_int m, lapack_int n,
                                   lapack_int nb, double* a,
                                   lapack_int lda, double* t,
                                   lapack_int ldt, double* d );
lapack_int LAPACKE_cunhr_col( int matrix_layout, lapack_int m, lapack_int n,
                              lapack_int nb, lapack_complex_float* a,
                              lapack_int lda, lapack_complex_float* t,
                              lapack_int ldt, lapack_complex_float* d );
lapack_int LAPACKE_cunhr_col_work( int matrix_layout, lapack_int m, lapack_int n,
                                   lapack_int nb, lapack_complex_float* a,
                                   lapack_int lda, lapack_complex_float* t,
                                   lapack_int ldt, lapack_complex_float* d );
lapack_int LAPACKE_zunhr_col( int matrix_layout, lapack_int m, lapack_int n,
                              lapack_int nb, lapack_complex_double* a,
                              lapack_int lda, lapack_complex_double* t,
                              lapack_int ldt, lapack_complex_double* d );
lapack_int LAPACKE_zunhr_col_work( int matrix_layout, lapack_int m, lapack_int n,
                                   lapack_int nb, lapack_complex_double* a,
                                   lapack_int lda, lapack_complex_double* t,
                                   lapack_int ldt, lapack_complex_double* d );

/* APIs for set/get nancheck flags */
void LAPACKE_set_nancheck( int flag );
int LAPACKE_get_nancheck( void );

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* _LAPACKE_H_ */
