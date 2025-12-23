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

#if   !defined(TRANS) && !defined(UPLO) && !defined(DIAG)
#define TRSM TRSM_LNUU
#define TRSV TRSV_NUU
#elif !defined(TRANS) && !defined(UPLO) && defined(DIAG)
#define TRSM TRSM_LNUN
#define TRSV TRSV_NUN
#elif !defined(TRANS) && defined(UPLO) && !defined(DIAG)
#define TRSM TRSM_LNLU
#define TRSV TRSV_NLU
#elif !defined(TRANS) && defined(UPLO) && defined(DIAG)
#define TRSM TRSM_LNLN
#define TRSV TRSV_NLN
#elif defined(TRANS) && !defined(UPLO) && !defined(DIAG)
#define TRSM TRSM_LTUU
#define TRSV TRSV_TUU
#elif defined(TRANS) && !defined(UPLO) && defined(DIAG)
#define TRSM TRSM_LTUN
#define TRSV TRSV_TUN
#elif defined(TRANS) && defined(UPLO) && !defined(DIAG)
#define TRSM TRSM_LTLU
#define TRSV TRSV_TLU
#elif defined(TRANS) && defined(UPLO) && defined(DIAG)
#define TRSM TRSM_LTLN
#define TRSV TRSV_TLN
#endif

blasint CNAME(blas_arg_t *args, BLASLONG *range_m, BLASLONG *range_n, FLOAT *sa, FLOAT *sb, BLASLONG mypos) {

    if (args -> n == 1){
        TRSV (args -> m, args -> a, args -> lda, args -> b, 1, sb);
    } else {
        TRSM (args, range_m, range_n, sa, sb, 0);
    }
  return 0;  }
