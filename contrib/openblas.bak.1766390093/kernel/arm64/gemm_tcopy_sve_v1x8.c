/***************************************************************************
Copyright (c) 2023, The OpenBLAS Project
All rights reserved.
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
1. Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in
the documentation and/or other materials provided with the
distribution.
3. Neither the name of the OpenBLAS project nor the names of
its contributors may be used to endorse or promote products
derived from this software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A00 PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE OPENBLAS PROJECT OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*****************************************************************************/

#include <stdint.h>
#include <stdio.h>
#include <arm_sve.h>

#include "common.h"

#ifdef DOUBLE
#define COUNT "cntd"
#define SV_TYPE svfloat64_t
#define SV_TRUE svptrue_b64
#define SV_WHILE svwhilelt_b64
#else
#define COUNT "cntw"
#define SV_TYPE svfloat32_t
#define SV_TRUE svptrue_b32
#define SV_WHILE svwhilelt_b32
#endif

#define INNER_COPY(pg, a_offset_inner, b_offset, lda, active)   \
    a_vec = svld1(pg, a_offset_inner);                          \
    svst1(pg, b_offset, a_vec);                                 \
    a_offset_inner += lda;                                      \
    b_offset += active;

int CNAME(BLASLONG m, BLASLONG n, IFLOAT *a, BLASLONG lda, IFLOAT *b){
    uint64_t sve_size = svcntw();
    asm(COUNT" %[SIZE_]" : [SIZE_]  "=r" (sve_size) : : );

    IFLOAT *a_offset, *a_offset_inner, *b_offset;
    a_offset = a;
    b_offset = b;

    SV_TYPE a_vec;
    svbool_t pg_true = SV_TRUE();

    BLASLONG single_vectors_n = n & -sve_size;
    for (BLASLONG j = 0; j < single_vectors_n; j += sve_size) {
        a_offset_inner = a_offset;

        svbool_t pg = pg_true;
        uint64_t active = sve_size;
        uint64_t i_cnt = m >> 3;
        while (i_cnt--) {
            INNER_COPY(pg, a_offset_inner, b_offset, lda, active);
            INNER_COPY(pg, a_offset_inner, b_offset, lda, active);
            INNER_COPY(pg, a_offset_inner, b_offset, lda, active);
            INNER_COPY(pg, a_offset_inner, b_offset, lda, active);
            INNER_COPY(pg, a_offset_inner, b_offset, lda, active);
            INNER_COPY(pg, a_offset_inner, b_offset, lda, active);
            INNER_COPY(pg, a_offset_inner, b_offset, lda, active);
            INNER_COPY(pg, a_offset_inner, b_offset, lda, active);
        }

        if (m & 4) {
            INNER_COPY(pg, a_offset_inner, b_offset, lda, active);
            INNER_COPY(pg, a_offset_inner, b_offset, lda, active);
            INNER_COPY(pg, a_offset_inner, b_offset, lda, active);
            INNER_COPY(pg, a_offset_inner, b_offset, lda, active);
        }

        if (m & 2) {
            INNER_COPY(pg, a_offset_inner, b_offset, lda, active);
            INNER_COPY(pg, a_offset_inner, b_offset, lda, active);
        }

        if (m & 1) {
            INNER_COPY(pg, a_offset_inner, b_offset, lda, active);
        }

        a_offset += sve_size;
    }

    BLASLONG remaining_n = n - single_vectors_n;
    if (remaining_n) {
        a_offset_inner = a_offset;
        svbool_t pg = SV_WHILE((uint64_t)0L, (uint64_t)remaining_n);
        uint64_t active = remaining_n;
        uint64_t i_cnt = m >> 2;
        while (i_cnt--) {
            INNER_COPY(pg, a_offset_inner, b_offset, lda, active);
            INNER_COPY(pg, a_offset_inner, b_offset, lda, active);
            INNER_COPY(pg, a_offset_inner, b_offset, lda, active);
            INNER_COPY(pg, a_offset_inner, b_offset, lda, active);
        }

        if (m & 2) {
            INNER_COPY(pg, a_offset_inner, b_offset, lda, active);
            INNER_COPY(pg, a_offset_inner, b_offset, lda, active);
        }

        if (m & 1) {
            INNER_COPY(pg, a_offset_inner, b_offset, lda, active);
        }
    }

    return 0;
}

