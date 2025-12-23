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
#define FLOAT_VX2_T             vfloat32m2x2_t
#define VGET_VX2                __riscv_vget_v_f32m2x2_f32m2
#define VSET_VX2                __riscv_vset_v_f32m2_f32m2x2
#define VLEV_FLOAT              __riscv_vle32_v_f32m2
#define VSEV_FLOAT              __riscv_vse32_v_f32m2
#define VLSEV_FLOAT             __riscv_vlse32_v_f32m2
#define VLSSEG2_FLOAT           __riscv_vlsseg2e32_v_f32m2x2
#define VSSEG2_FLOAT            __riscv_vsseg2e32_v_f32m2x2
#define VBOOL_T                 vbool16_t
#define UINT_V_T                vint32m2_t
#define VID_V_UINT              __riscv_vid_v_u32m2
#define VMSGTU_VX_UINT          __riscv_vmsgt_vx_i32m2_b16
#define VMSEQ_VX_UINT           __riscv_vmseq_vx_i32m2_b16
#define VFMERGE_VFM_FLOAT       __riscv_vfmerge_vfm_f32m2
#define V_UM2_TO_IM2            __riscv_vreinterpret_v_u32m2_i32m2
#else
#define VSETVL(n)               __riscv_vsetvl_e64m2(n)
#define FLOAT_V_T               vfloat64m2_t
#define FLOAT_VX2_T             vfloat64m2x2_t
#define VGET_VX2                __riscv_vget_v_f64m2x2_f64m2
#define VSET_VX2                __riscv_vset_v_f64m2_f64m2x2
#define VLEV_FLOAT              __riscv_vle64_v_f64m2
#define VSEV_FLOAT              __riscv_vse64_v_f64m2
#define VLSEV_FLOAT             __riscv_vlse64_v_f64m2
#define VLSSEG2_FLOAT           __riscv_vlsseg2e64_v_f64m2x2
#define VSSEG2_FLOAT            __riscv_vsseg2e64_v_f64m2x2
#define VBOOL_T                 vbool32_t
#define UINT_V_T                vuint64m2_t
#define VID_V_UINT              __riscv_vid_v_u64m2
#define VMSGTU_VX_UINT          __riscv_vmsgtu_vx_u64m2_b32
#define VMSEQ_VX_UINT           __riscv_vmseq_vx_u64m2_b32
#define VFMERGE_VFM_FLOAT       __riscv_vfmerge_vfm_f64m2
#define V_UM2_TO_IM2(values)    values
#endif

int CNAME(BLASLONG m, BLASLONG n, FLOAT *a, BLASLONG lda, BLASLONG posX, BLASLONG posY, FLOAT *b){

    BLASLONG i, js, X;

    FLOAT *ao;

    BLASLONG stride_lda = sizeof(FLOAT)*lda*2;
    
    FLOAT_VX2_T vax2;
    FLOAT_V_T va0, va1;

    size_t vl;
#ifdef UNIT
    VBOOL_T vbool_eq;
#endif

    VBOOL_T vbool_cmp;
    UINT_V_T vindex;

    for (js = n; js > 0; js -= vl)
    {
        vl = VSETVL(js);
        X = posX;

        if (posX <= posY) 
        {
            ao = a + posY * 2 + posX * lda * 2;
        } 
        else 
        {
            ao = a + posX * 2 + posY * lda * 2;
        }

        i = 0;
        do
        {
            if (X > posY) 
            {
                vax2 = VLSSEG2_FLOAT(ao, stride_lda, vl);
                VSSEG2_FLOAT(b, vax2, vl);

                ao  += 2;
                b   += vl * 2;

                X ++;
                i ++;
            } 
            else if (X < posY) 
            {
                ao  += lda * 2;
                b   += vl * 2;
                X ++;
                i ++;
            } 
            else
            {
                vindex  = V_UM2_TO_IM2(VID_V_UINT(vl));
                for (unsigned int j = 0; j < vl; j++) 
                {
                    vax2 = VLSSEG2_FLOAT(ao, stride_lda, vl);
                    va0 = VGET_VX2(vax2, 0);
                    va1 = VGET_VX2(vax2, 1);

                    vbool_cmp = VMSGTU_VX_UINT(vindex, j, vl);
                    va0 = VFMERGE_VFM_FLOAT(va0, ZERO, vbool_cmp, vl);
                    va1 = VFMERGE_VFM_FLOAT(va1, ZERO, vbool_cmp, vl);
#ifdef UNIT
                    vbool_eq = VMSEQ_VX_UINT(vindex, j, vl);
                    va0 =  VFMERGE_VFM_FLOAT(va0, ONE, vbool_eq, vl);
                    va1 =  VFMERGE_VFM_FLOAT(va1, ZERO, vbool_eq, vl);
#endif
                    vax2 = VSET_VX2(vax2, 0, va0);
                    vax2 = VSET_VX2(vax2, 1, va1);
                    VSSEG2_FLOAT(b, vax2, vl);
                    ao  += 2;
                    b   += vl * 2;
                }

                X += vl;
                i += vl;
            }
        } while (i < m);

        posY += vl;
    }

    return 0;
}