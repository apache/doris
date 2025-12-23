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
#define FLOAT_V_T               vfloat32m2_t
#define VLEV_FLOAT              __riscv_vle32_v_f32m2
#define VSEV_FLOAT              __riscv_vse32_v_f32m2
#define VSEV_FLOAT_M            __riscv_vse32_v_f32m2_m
#define VLSEV_FLOAT             __riscv_vlse32_v_f32m2
#define VBOOL_T                 vbool16_t
#define UINT_V_T                vuint32m2_t
#define VID_V_UINT              __riscv_vid_v_u32m2
#define VMSLTU_VX_UINT          __riscv_vmsltu_vx_u32m2_b16
#else
#define VSETVL(n)               __riscv_vsetvl_e64m2(n)
#define FLOAT_V_T               vfloat64m2_t
#define VLEV_FLOAT              __riscv_vle64_v_f64m2
#define VSEV_FLOAT              __riscv_vse64_v_f64m2
#define VSEV_FLOAT_M            __riscv_vse64_v_f64m2_m
#define VLSEV_FLOAT             __riscv_vlse64_v_f64m2
#define VBOOL_T                 vbool32_t
#define UINT_V_T                vuint64m2_t
#define VID_V_UINT              __riscv_vid_v_u64m2
#define VMSLTU_VX_UINT          __riscv_vmsltu_vx_u64m2_b32

#endif

#ifndef UNIT
#define INV(a) (ONE / (a))
#else
#define INV(a) (ONE)
#endif

// Optimizes the implementation in ../arm64/trsm_lncopy_sve.c

int CNAME(BLASLONG m, BLASLONG n, FLOAT *a, BLASLONG lda, BLASLONG offset, FLOAT *b){

    BLASLONG i, ii, jj, js;

    FLOAT *ao;

    jj = offset;

    BLASLONG stride_lda = sizeof(FLOAT)*lda;

    FLOAT_V_T va1;
    VBOOL_T vbool_cmp;
    UINT_V_T vindex;
    size_t vl;

    for (js = n; js > 0; js -= vl)
    {
        vl = VSETVL(js);
        ao = a;

        ii = 0;
        for (i = 0; i < m;)
        {
            if (ii == jj) 
            {
                vindex  = VID_V_UINT(vl);
                for (unsigned int j = 0; j < vl; j++) 
                {
                    va1 = VLSEV_FLOAT(ao, stride_lda, vl);
                    vbool_cmp = VMSLTU_VX_UINT(vindex, j, vl);
                    VSEV_FLOAT_M(vbool_cmp, b, va1, vl);

                    *(b + j) = INV(*(ao + j * lda));
                    ao++;
                    b += vl;
                }
                i += vl;
                ii += vl;
            }
            else
            {
                if (ii > jj)
                {
                    va1 = VLSEV_FLOAT(ao, stride_lda, vl);
                    VSEV_FLOAT(b, va1, vl);
                }
                ao++;
                b += vl;
                i++;
                ii++;
            }
        }

        a += vl * lda;
        jj += vl;
    }

    return 0;
}
