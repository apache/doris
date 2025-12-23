/***************************************************************************
Copyright (c) 2022, The OpenBLAS Project
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
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE OPENBLAS PROJECT OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*****************************************************************************/

#include <stdio.h>
#include "common.h"

#if !defined(DOUBLE)
#define VSETVL(n)               __riscv_vsetvl_e32m2(n)
#define FLOAT_VX2_T             vfloat32m2x2_t
#define VLSSEG2_FLOAT           __riscv_vlsseg2e32_v_f32m2x2
#define VSSEG2_FLOAT            __riscv_vsseg2e32_v_f32m2x2
#else
#define VSETVL(n)               __riscv_vsetvl_e64m2(n)
#define FLOAT_VX2_T             vfloat64m2x2_t
#define VLSSEG2_FLOAT           __riscv_vlsseg2e64_v_f64m2x2
#define VSSEG2_FLOAT            __riscv_vsseg2e64_v_f64m2x2
#endif

int CNAME(BLASLONG m, BLASLONG n, IFLOAT *a, BLASLONG lda, IFLOAT *b){

    BLASLONG i, j;

    FLOAT *a_offset;
    FLOAT *a_offset1;
    FLOAT *b_offset;

    FLOAT_VX2_T vx2;
    size_t vl;

    //fprintf(stderr, "%s, m=%ld n=%ld lda=%ld\n", __FUNCTION__, m, n, lda);
    a_offset = a;
    b_offset = b;

    for(j = n; j > 0; j -= vl) {
        vl = VSETVL(j);

        a_offset1 = a_offset;
        a_offset += vl * lda * 2;

        for(i = m; i > 0; i--) {
            vx2 = VLSSEG2_FLOAT(a_offset1, lda * sizeof(FLOAT) * 2, vl);
            VSSEG2_FLOAT(b_offset, vx2, vl);

            a_offset1 += 2;
            b_offset += vl * 2;
        }
    }
    return 0;
}

