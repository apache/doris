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
#define VSETVL(n)               __riscv_vsetvl_e32m1(n)
#define FLOAT_VX2_T             vfloat32m1x2_t
#define FLOAT_VX4_T             vfloat32m1x4_t
#define FLOAT_VX8_T             vfloat32m1x8_t
#define VGET_VX2                __riscv_vget_v_f32m1x2_f32m1
#define VSET_VX2                __riscv_vset_v_f32m1_f32m1x2
#define VSET_VX4                __riscv_vset_v_f32m1_f32m1x4
#define VSET_VX8                __riscv_vset_v_f32m1_f32m1x8
#define VLSEG2_FLOAT            __riscv_vlseg2e32_v_f32m1x2
#define VSSEG2_FLOAT            __riscv_vsseg2e32_v_f32m1x2
#define VSSEG4_FLOAT            __riscv_vsseg4e32_v_f32m1x4
#define VSSEG8_FLOAT            __riscv_vsseg8e32_v_f32m1x8
#else
#define VSETVL(n)               __riscv_vsetvl_e64m1(n)
#define FLOAT_VX2_T             vfloat64m1x2_t
#define FLOAT_VX4_T             vfloat64m1x4_t
#define FLOAT_VX8_T             vfloat64m1x8_t
#define VGET_VX2                __riscv_vget_v_f64m1x2_f64m1
#define VSET_VX2                __riscv_vset_v_f64m1_f64m1x2
#define VSET_VX4                __riscv_vset_v_f64m1_f64m1x4
#define VSET_VX8                __riscv_vset_v_f64m1_f64m1x8
#define VLSEG2_FLOAT            __riscv_vlseg2e64_v_f64m1x2
#define VSSEG2_FLOAT            __riscv_vsseg2e64_v_f64m1x2
#define VSSEG4_FLOAT            __riscv_vsseg4e64_v_f64m1x4
#define VSSEG8_FLOAT            __riscv_vsseg8e64_v_f64m1x8
#endif

// Optimizes the implementation in ../generic/zgemm_ncopy_4.c

int CNAME(BLASLONG m, BLASLONG n, FLOAT *a, BLASLONG lda, FLOAT *b){
    BLASLONG i, j;

    FLOAT *aoffset;
    FLOAT *aoffset1, *aoffset2, *aoffset3, *aoffset4;

    FLOAT *boffset;

    FLOAT_VX2_T v1x2, v2x2, v3x2, v4x2;
    FLOAT_VX4_T vxx4;
    FLOAT_VX8_T vxx8;
    size_t vl;

    aoffset = a;
    boffset = b;
    lda *= 2;

    for (j = (n >> 2); j > 0; j--) {
        aoffset1  = aoffset;
        aoffset2  = aoffset1 + lda;
        aoffset3  = aoffset2 + lda;
        aoffset4  = aoffset3 + lda;
        aoffset  += 4 * lda;

        for (i = m; i > 0; i -= vl) {
            vl = VSETVL(i);
            v1x2 = VLSEG2_FLOAT(aoffset1, vl);
            v2x2 = VLSEG2_FLOAT(aoffset2, vl);
            v3x2 = VLSEG2_FLOAT(aoffset3, vl);
            v4x2 = VLSEG2_FLOAT(aoffset4, vl);
            
            vxx8 = VSET_VX8(vxx8, 0, VGET_VX2(v1x2, 0));
            vxx8 = VSET_VX8(vxx8, 1, VGET_VX2(v1x2, 1));
            vxx8 = VSET_VX8(vxx8, 2, VGET_VX2(v2x2, 0));
            vxx8 = VSET_VX8(vxx8, 3, VGET_VX2(v2x2, 1));
            vxx8 = VSET_VX8(vxx8, 4, VGET_VX2(v3x2, 0));
            vxx8 = VSET_VX8(vxx8, 5, VGET_VX2(v3x2, 1));
            vxx8 = VSET_VX8(vxx8, 6, VGET_VX2(v4x2, 0));
            vxx8 = VSET_VX8(vxx8, 7, VGET_VX2(v4x2, 1));

            VSSEG8_FLOAT(boffset, vxx8, vl);

            aoffset1 += vl * 2;
            aoffset2 += vl * 2;
            aoffset3 += vl * 2;
            aoffset4 += vl * 2;
            boffset  += vl * 8;
        }
    }

    if (n & 2) {
        aoffset1  = aoffset;
        aoffset2  = aoffset1 + lda;
        aoffset  += 2 * lda;
        
        for (i = m; i > 0; i -= vl) {
            vl = VSETVL(i);
            v1x2 = VLSEG2_FLOAT(aoffset1, vl);
            v2x2 = VLSEG2_FLOAT(aoffset2, vl);

            vxx4 = VSET_VX4(vxx4, 0, VGET_VX2(v1x2, 0));
            vxx4 = VSET_VX4(vxx4, 1, VGET_VX2(v1x2, 1));
            vxx4 = VSET_VX4(vxx4, 2, VGET_VX2(v2x2, 0));
            vxx4 = VSET_VX4(vxx4, 3, VGET_VX2(v2x2, 1));
        
            VSSEG4_FLOAT(boffset, vxx4, vl);
        
            aoffset1 += vl * 2;
            aoffset2 += vl * 2;
            boffset  += vl * 4;
        }
    }

    if (n & 1) {
        aoffset1  = aoffset;
        aoffset  += lda;

        for (i = m; i > 0; i -= vl) {
            vl = VSETVL(i);
            v1x2 = VLSEG2_FLOAT(aoffset1, vl);

            VSSEG2_FLOAT(boffset, v1x2, vl);

            aoffset1 += vl * 2;
            boffset  += vl * 2;
        }
    }

     return 0;
}
