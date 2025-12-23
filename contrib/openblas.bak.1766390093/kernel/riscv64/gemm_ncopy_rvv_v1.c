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

#include "common.h"

#if !defined(DOUBLE)
#define VSETVL(n)               __riscv_vsetvl_e32m2(n)
#define FLOAT_V_T               vfloat32m2_t
#define VLEV_FLOAT              __riscv_vle32_v_f32m2
#define VLSEV_FLOAT             __riscv_vlse32_v_f32m2
#define VSEV_FLOAT              __riscv_vse32_v_f32m2
#else
#define VSETVL(n)               __riscv_vsetvl_e64m2(n)
#define FLOAT_V_T               vfloat64m2_t
#define VLEV_FLOAT              __riscv_vle64_v_f64m2
#define VLSEV_FLOAT             __riscv_vlse64_v_f64m2
#define VSEV_FLOAT              __riscv_vse64_v_f64m2
#endif

int CNAME(BLASLONG m, BLASLONG n, FLOAT *a, BLASLONG lda, FLOAT *b)
{
    BLASLONG i, j;

    FLOAT *a_offset;
    FLOAT *a_offset1;
    FLOAT *b_offset;

    FLOAT_V_T v0;
    size_t vl;

    //fprintf(stderr, "%s, m=%ld n=%ld lda=%ld\n", __FUNCTION__, m, n, lda);

    a_offset = a;
    b_offset = b;

    for(j = n; j > 0; j -= vl) {
        vl = VSETVL(j);

        a_offset1 = a_offset;
        a_offset += vl * lda;

        for(i = m; i > 0; i--) {
            v0 = VLSEV_FLOAT(a_offset1, lda * sizeof(FLOAT), vl);
            VSEV_FLOAT(b_offset, v0, vl);

            a_offset1++;
            b_offset += vl;
        }
    }

    return 0;
}
