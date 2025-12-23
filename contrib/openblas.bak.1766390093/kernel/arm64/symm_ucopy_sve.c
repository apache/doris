/*********************************************************************/
/* Copyright 2009, 2010 The University of Texas at Austin.           */
/* Copyright 2023 The OpenBLAS Project                               */
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
#include <arm_sve.h>

int CNAME(BLASLONG m, BLASLONG n, FLOAT *a, BLASLONG lda, BLASLONG posX, BLASLONG posY, FLOAT *b){

  BLASLONG i, offset;

#if defined(DOUBLE)
  uint64_t sve_size = svcntd();
  svint64_t posY_vec = svdup_s64(posY);
  svint64_t posX_vec = svdup_s64(posX);
  svint64_t lda_vec = svdup_s64(lda);
  svint64_t one_vec = svdup_s64(1LL);

  int64_t j = 0;
  svbool_t pg = svwhilelt_b64((uint64_t)j, (uint64_t)n);
  int64_t active = svcntp_b64(svptrue_b64(), pg);
  svint64_t index_neg = svindex_s64(0LL, -1LL);
  svint64_t index = svindex_s64(0LL, 1LL);
  do {
    offset = posX - posY;
    svint64_t vec_off = svdup_s64(offset);
    svbool_t cmp = svcmpgt(pg, vec_off, index_neg);

    svint64_t temp = svadd_z(pg, posX_vec, index);
    svint64_t temp1 = svmla_z(pg, temp, posY_vec, lda_vec);
    svint64_t temp2 = svmla_z(pg, posY_vec, temp, lda);
    svint64_t gat_ind = svsel(cmp, temp2, temp1);

    i = m;
    while (i>0) {
        svfloat64_t data_vec = svld1_gather_index(pg, a, gat_ind);

        gat_ind = svadd_m(cmp, gat_ind, one_vec);
        gat_ind = svadd_m(svnot_z(pg, cmp) , gat_ind, lda_vec);

        svst1(pg, b, data_vec);

        b += active;
        offset --;
        vec_off = svsub_z(pg, vec_off, one_vec);
        cmp = svcmpgt(pg, vec_off, index_neg);
        
        i--;
    }

    posX += sve_size;
    posX_vec = svdup_s64(posX);
    j += sve_size;
    pg = svwhilelt_b64((uint64_t)j, (uint64_t)n);
    active = svcntp_b64(svptrue_b64(), pg);
  } while (svptest_any(svptrue_b64(), pg));

#else
  uint32_t sve_size = svcntw();
  svint32_t posY_vec = svdup_s32(posY);
  svint32_t posX_vec = svdup_s32(posX);
  svint32_t lda_vec = svdup_s32(lda);
  svint32_t one_vec = svdup_s32(1);

  int32_t N = n;
  int32_t j = 0;
  svbool_t pg = svwhilelt_b32((uint32_t)j, (uint32_t)N);
  int32_t active = svcntp_b32(svptrue_b32(), pg);
  svint32_t index_neg = svindex_s32(0, -1);
  svint32_t index = svindex_s32(0, 1);
  do {
    offset = posX - posY;
    svint32_t vec_off = svdup_s32(offset);
    svbool_t cmp = svcmpgt(pg, vec_off, index_neg);

    svint32_t temp = svadd_z(pg, posX_vec, index);
    svint32_t temp1 = svmla_z(pg, temp, posY_vec, lda_vec);
    svint32_t temp2 = svmla_z(pg, posY_vec, temp, lda);
    svint32_t gat_ind = svsel(cmp, temp2, temp1);

    i = m;
    while (i>0) {
        svfloat32_t data_vec = svld1_gather_index(pg, a, gat_ind);

        gat_ind = svadd_m(cmp, gat_ind, one_vec);
        gat_ind = svadd_m(svnot_z(pg, cmp) , gat_ind, lda_vec);

        svst1(pg, b, data_vec);

        b += active;
        offset --;
        vec_off = svsub_z(pg, vec_off, one_vec);
        cmp = svcmpgt(pg, vec_off, index_neg);
        
        i--;
    }

    posX += sve_size;
    posX_vec = svdup_s32(posX);
    j += sve_size;
    pg = svwhilelt_b32((uint32_t)j, (uint32_t)N);
    active = svcntp_b32(svptrue_b32(), pg);
  } while (svptest_any(svptrue_b32(), pg));

#endif

  return 0;
}
