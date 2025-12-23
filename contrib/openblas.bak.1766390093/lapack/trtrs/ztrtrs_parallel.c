/*********************************************************************/
/* Copyright 2009, 2010 The University of Texas at Austin.           */
/* All rights reserved.                                              */
/*                                                                   */
/* Redistribution and use in source and binary forms, with or        */
/* without modification, are permitted provided that the following   */
/* conditions are met:                                               */
/*                                                                   */
/*   1. Redistributions of source code must retain the above         */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer.                                                  */
/*                                                                   */
/*   2. Redistributions in binary form must reproduce the above      */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer in the documentation and/or other materials       */
/*      provided with the distribution.                              */
/*                                                                   */
/*    THIS  SOFTWARE IS PROVIDED  BY THE  UNIVERSITY OF  TEXAS AT    */
/*    AUSTIN  ``AS IS''  AND ANY  EXPRESS OR  IMPLIED WARRANTIES,    */
/*    INCLUDING, BUT  NOT LIMITED  TO, THE IMPLIED  WARRANTIES OF    */
/*    MERCHANTABILITY  AND FITNESS FOR  A PARTICULAR  PURPOSE ARE    */
/*    DISCLAIMED.  IN  NO EVENT SHALL THE UNIVERSITY  OF TEXAS AT    */
/*    AUSTIN OR CONTRIBUTORS BE  LIABLE FOR ANY DIRECT, INDIRECT,    */
/*    INCIDENTAL,  SPECIAL, EXEMPLARY,  OR  CONSEQUENTIAL DAMAGES    */
/*    (INCLUDING, BUT  NOT LIMITED TO,  PROCUREMENT OF SUBSTITUTE    */
/*    GOODS  OR  SERVICES; LOSS  OF  USE,  DATA,  OR PROFITS;  OR    */
/*    BUSINESS INTERRUPTION) HOWEVER CAUSED  AND ON ANY THEORY OF    */
/*    LIABILITY, WHETHER  IN CONTRACT, STRICT  LIABILITY, OR TORT    */
/*    (INCLUDING NEGLIGENCE OR OTHERWISE)  ARISING IN ANY WAY OUT    */
/*    OF  THE  USE OF  THIS  SOFTWARE,  EVEN  IF ADVISED  OF  THE    */
/*    POSSIBILITY OF SUCH DAMAGE.                                    */
/*                                                                   */
/* The views and conclusions contained in the software and           */
/* documentation are those of the authors and should not be          */
/* interpreted as representing official policies, either expressed   */
/* or implied, of The University of Texas at Austin.                 */
/*********************************************************************/

#include <stdio.h>
#include "common.h"

#if   TRANS == 1 && !defined(UPLO) && !defined(DIAG)
#define TRSM TRSM_LNUU
#define ZTRSV ZTRSV_NUU
#elif TRANS == 1 && !defined(UPLO) && defined(DIAG)
#define TRSM TRSM_LNUN
#define ZTRSV ZTRSV_NUN
#elif TRANS == 1 && defined(UPLO) && !defined(DIAG)
#define TRSM TRSM_LNLU
#define ZTRSV ZTRSV_NLU
#elif TRANS == 1 && defined(UPLO) && defined(DIAG)
#define TRSM TRSM_LNLN
#define ZTRSV ZTRSV_NLN
#elif TRANS == 2 && !defined(UPLO) && !defined(DIAG)
#define TRSM TRSM_LTUU
#define ZTRSV ZTRSV_TUU
#elif TRANS == 2 && !defined(UPLO) && defined(DIAG)
#define TRSM TRSM_LTUN
#define ZTRSV ZTRSV_TUN
#elif TRANS == 2 && defined(UPLO) && !defined(DIAG)
#define TRSM TRSM_LTLU
#define ZTRSV ZTRSV_TLU
#elif TRANS == 2 && defined(UPLO) && defined(DIAG)
#define TRSM TRSM_LTLN
#define ZTRSV ZTRSV_TLN
#elif TRANS == 3 && !defined(UPLO) && !defined(DIAG)
#define TRSM TRSM_LRUU
#define ZTRSV ZTRSV_RUU
#elif TRANS == 3 && !defined(UPLO) && defined(DIAG)
#define TRSM TRSM_LRUN
#define ZTRSV ZTRSV_RUN
#elif TRANS == 3 && defined(UPLO) && !defined(DIAG)
#define TRSM TRSM_LRLU
#define ZTRSV ZTRSV_RLU
#elif TRANS == 3 && defined(UPLO) && defined(DIAG)
#define TRSM TRSM_LRLN
#define ZTRSV ZTRSV_RLN
#elif TRANS == 4 && !defined(UPLO) && !defined(DIAG)
#define TRSM TRSM_LCUU
#define ZTRSV ZTRSV_CUU
#elif TRANS == 4 && !defined(UPLO) && defined(DIAG)
#define TRSM TRSM_LCUN
#define ZTRSV ZTRSV_CUN
#elif TRANS == 4 && defined(UPLO) && !defined(DIAG)
#define TRSM TRSM_LCLU
#define ZTRSV ZTRSV_CLU
#elif TRANS == 4 && defined(UPLO) && defined(DIAG)
#define TRSM TRSM_LCLN
#define ZTRSV ZTRSV_CLN
#endif

static int inner_thread(blas_arg_t *args, BLASLONG *range_m, BLASLONG *range_n,
			 FLOAT *sa, FLOAT *sb, BLASLONG mypos) {

  TRSM (args, range_m, range_n, sa, sb, 0);
  return 0;
}

blasint CNAME(blas_arg_t *args, BLASLONG *range_m, BLASLONG *range_n, FLOAT *sa, FLOAT *sb, BLASLONG mypos) {

  int mode;

    if (args -> n == 1){
      ZTRSV (args -> m, args -> a, args -> lda, args -> b, 1, sb);
    } else {
#ifdef XDOUBLE
      mode  =  BLAS_XDOUBLE | BLAS_COMPLEX;
#elif defined(DOUBLE)
      mode  =  BLAS_DOUBLE  | BLAS_COMPLEX;
#else
      mode  =  BLAS_SINGLE  | BLAS_COMPLEX;
#endif

      gemm_thread_n(mode, args, NULL, NULL, inner_thread, sa, sb, args -> nthreads);
    }

   return 0;
  }
