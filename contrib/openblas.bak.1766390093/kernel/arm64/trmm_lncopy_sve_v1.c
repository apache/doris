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

#ifdef __ARM_FEATURE_SVE
#include <arm_sve.h>
#endif

int CNAME(BLASLONG m, BLASLONG n, FLOAT *a, BLASLONG lda, BLASLONG posX, BLASLONG posY, FLOAT *b){

    BLASLONG i, js;
    BLASLONG X;

    js = 0;
    FLOAT *ao;
#ifdef DOUBLE
    svint64_t index = svindex_s64(0LL, lda);
    svbool_t pn = svwhilelt_b64((uint64_t)js, (uint64_t)n);
    int n_active = svcntp_b64(svptrue_b64(), pn);
#else
    svint32_t index = svindex_s32(0, lda);
    svbool_t pn = svwhilelt_b32((uint64_t)js, (uint64_t)n);
    int n_active = svcntp_b32(svptrue_b32(), pn);
#endif
    do
    {
        X = posX;

        if (posX <= posY) {
            ao = a + posY + posX * lda;
        } else {
            ao = a + posX + posY * lda;
        }

        i = 0;
        do 
        {
            if (X > posY) {
#ifdef DOUBLE
                svfloat64_t aj_vec = svld1_gather_index(pn, ao, index);
#else
                svfloat32_t aj_vec = svld1_gather_index(pn, ao, index);
#endif
                svst1(pn, b, aj_vec);
                ao ++;
                b += n_active;
                X ++;
                i ++;
            } else 
                if (X < posY) {
                    ao += lda;
                    b += n_active;
                    X ++;
                    i ++;
                } else {
                    /* I did not find a way to unroll this while preserving vector-length-agnostic code. */
#ifdef UNIT
                    int temp = 0;
                    for (int j = 0; j < n_active; j++) {
                        for (int k = 0 ; k < j; k++) {
                            b[temp++] = *(ao+k*lda+j);
                        }
                        b[temp++] = ONE;
                        for (int k = j+1; k < n_active; k++) {
                            b[temp++] = ZERO;
                        }
                    }
#else 
                    int temp = 0;
                    for (int j = 0; j < n_active; j++) {
                        for (int k = 0 ; k <= j; k++) {
                            b[temp++] = *(ao+k*lda+j);
                        }
                        for (int k = j+1; k < n_active; k++) {
                            b[temp++] = ZERO;
                        }
                    }
#endif
                    ao += n_active;
                    b += n_active*n_active;
                    X += n_active;
                    i += n_active;
                }
        } while (i < m);

        posY += n_active;
        js += n_active;
#ifdef DOUBLE
        pn = svwhilelt_b64((uint64_t)js, (uint64_t)n);
        n_active = svcntp_b64(svptrue_b64(), pn);
    } while (svptest_any(svptrue_b64(), pn));
#else
        pn = svwhilelt_b32((uint64_t)js, (uint64_t)n);
        n_active = svcntp_b32(svptrue_b32(), pn);
    } while (svptest_any(svptrue_b32(), pn));
#endif

    return 0;
}
