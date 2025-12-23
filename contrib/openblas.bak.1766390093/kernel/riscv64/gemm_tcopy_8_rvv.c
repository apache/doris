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
#define FLOAT_V_T               vfloat32m2_t
#define FLOAT_V_T_HALF          vfloat32m1_t
#define VLEV_FLOAT              __riscv_vle32_v_f32m2
#define VLEV_FLOAT_HALF         __riscv_vle32_v_f32m1
#define VSEV_FLOAT              __riscv_vse32_v_f32m2
#define VSEV_FLOAT_HALF         __riscv_vse32_v_f32m1
#else
#define FLOAT_V_T               vfloat64m4_t
#define FLOAT_V_T_HALF          vfloat64m2_t
#define VLEV_FLOAT              __riscv_vle64_v_f64m4
#define VLEV_FLOAT_HALF         __riscv_vle64_v_f64m2
#define VSEV_FLOAT              __riscv_vse64_v_f64m4
#define VSEV_FLOAT_HALF         __riscv_vse64_v_f64m2
#endif

int CNAME(BLASLONG m, BLASLONG n, IFLOAT *a, BLASLONG lda, IFLOAT *b)
{
    BLASLONG i, j;

    IFLOAT *aoffset;
    IFLOAT *aoffset1;

    IFLOAT *boffset, *boffset1, *boffset2, *boffset3, *boffset4;

    FLOAT_V_T v0;
    FLOAT_V_T_HALF v1;

    // fprintf(stderr, "gemm_tcopy_8 m=%ld n=%ld lda=%ld\n", m, n, lda);

    aoffset   = a;
    boffset   = b;
    boffset2  = b + m  * (n & ~7);
    boffset3  = b + m  * (n & ~3);
    boffset4  = b + m  * (n & ~1);

    for(j = m; j > 0; j--) {
        aoffset1  = aoffset;
        boffset1  = boffset;

        aoffset += lda;
        boffset  += 8;

        for(i = (n >> 3); i > 0; i--) {
            size_t vl = 8;

            v0 = VLEV_FLOAT(aoffset1, vl);
            VSEV_FLOAT(boffset1, v0, vl);

            aoffset1 += 8;
            boffset1 += 8 * m;
        }

        if (n & 4) {
            size_t vl = 4;

            v1 = VLEV_FLOAT_HALF(aoffset1, vl);
            VSEV_FLOAT_HALF(boffset2, v1, vl);

            aoffset1 += 4;
            boffset2 += 4;
        }

        if (n & 2) {
            *(boffset3) = *(aoffset1);
            *(boffset3 + 1) = *(aoffset1 + 1);

            aoffset1 += 2;
            boffset3 += 2;
        }

        if (n & 1) {
            *(boffset4) = *(aoffset1);
            aoffset1 ++;
            boffset4 ++;
        }
    }

    return 0;
}
