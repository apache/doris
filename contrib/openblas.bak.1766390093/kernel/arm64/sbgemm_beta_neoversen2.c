/***************************************************************************
 * Copyright (c) 2022, The OpenBLAS Project
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 * 1. Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in
 * the documentation and/or other materials provided with the
 * distribution.
 * 3. Neither the name of the OpenBLAS project nor the names of
 * its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE OPENBLAS PROJECT OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * *****************************************************************************/

#include "common.h"

int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT beta, IFLOAT *dummy2,
          BLASLONG dummy3, IFLOAT *dummy4, BLASLONG dummy5, FLOAT *c,
          BLASLONG ldc) {

    BLASLONG i, j;
    BLASLONG chunk, remain;
    FLOAT *c_offset1, *c_offset;
    c_offset = c;
    chunk = m >> 3;
    remain = m & 7;
    if (beta == ZERO) {
        for (j = n; j > 0; j--) {
            c_offset1 = c_offset;
            c_offset += ldc;
            for (i = chunk; i > 0; i--) {
                *(c_offset1 + 0) = ZERO;
                *(c_offset1 + 1) = ZERO;
                *(c_offset1 + 2) = ZERO;
                *(c_offset1 + 3) = ZERO;
                *(c_offset1 + 4) = ZERO;
                *(c_offset1 + 5) = ZERO;
                *(c_offset1 + 6) = ZERO;
                *(c_offset1 + 7) = ZERO;
                c_offset1 += 8;
            }
            for (i = remain; i > 0; i--) {
                *c_offset1 = ZERO;
                c_offset1++;
            }
        }
    } else {
        for (j = n; j > 0; j--) {
            c_offset1 = c_offset;
            c_offset += ldc;
            for (i = chunk; i > 0; i--) {
                *(c_offset1 + 0) *= beta;
                *(c_offset1 + 1) *= beta;
                *(c_offset1 + 2) *= beta;
                *(c_offset1 + 3) *= beta;
                *(c_offset1 + 4) *= beta;
                *(c_offset1 + 5) *= beta;
                *(c_offset1 + 6) *= beta;
                *(c_offset1 + 7) *= beta;
                c_offset1 += 8;
            }
            for (i = remain; i > 0; i--) {
                *c_offset1 *= beta;
                c_offset1++;
            }
        }
    }
    return 0;
};