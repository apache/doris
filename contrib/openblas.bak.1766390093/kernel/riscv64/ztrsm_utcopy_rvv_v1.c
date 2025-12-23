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
#define VLSEG2_FLOAT            __riscv_vlseg2e32_v_f32m2x2
#define VSSEG2_FLOAT            __riscv_vsseg2e32_v_f32m2x2
#define VSSEG2_FLOAT_M          __riscv_vsseg2e32_v_f32m2x2_m
#define VBOOL_T                 vbool16_t
#define UINT_V_T                vuint32m2_t
#define VID_V_UINT              __riscv_vid_v_u32m2
#define VMSLTU_VX_UINT          __riscv_vmsltu_vx_u32m2_b16
#else
#define VSETVL(n)               __riscv_vsetvl_e64m2(n)
#define FLOAT_VX2_T             vfloat64m2x2_t
#define VLSEG2_FLOAT            __riscv_vlseg2e64_v_f64m2x2
#define VSSEG2_FLOAT            __riscv_vsseg2e64_v_f64m2x2
#define VSSEG2_FLOAT_M          __riscv_vsseg2e64_v_f64m2x2_m
#define VBOOL_T                 vbool32_t
#define UINT_V_T                vuint64m2_t
#define VID_V_UINT              __riscv_vid_v_u64m2
#define VMSLTU_VX_UINT          __riscv_vmsltu_vx_u64m2_b32
#endif


int CNAME(BLASLONG m, BLASLONG n, FLOAT *a, BLASLONG lda, BLASLONG offset, FLOAT *b){

    //fprintf(stderr, "%s , %s, m = %4ld  n = %4ld  lda = %4ld offset = %4ld\n", __FILE__, __FUNCTION__, m, n, lda, offset); // Debug

    BLASLONG i, ii, jj, js;

    FLOAT *ao;

    jj = offset;
    FLOAT_VX2_T vax2;

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
                    vax2 = VLSEG2_FLOAT(ao, vl);
                    vbool_cmp = VMSLTU_VX_UINT(vindex, j, vl);
                    VSSEG2_FLOAT_M(vbool_cmp, b, vax2, vl);

                    compinv((b + j * 2), *(ao + j * 2), *(ao + j * 2 + 1));

                    ao  += lda * 2;
                    b   += vl * 2;
                }
                i += vl;
                ii += vl;
            } 
            else 
            {
                if (ii > jj) 
                {
                    vax2 = VLSEG2_FLOAT(ao, vl);
                    VSSEG2_FLOAT(b, vax2, vl);
                }
                ao  += lda * 2;
                b   += vl * 2;
                i ++;
                ii ++;
            }
        }

        a += vl * 2;
        jj += vl;
    }

    return 0;
}
