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
#define VSETVL_MAX              __riscv_vsetvlmax_e32m2()
#define FLOAT_V_T               vfloat32m2_t
#define FLOAT_VX2_T             vfloat32m2x2_t
#define VGET_VX2                __riscv_vget_v_f32m2x2_f32m2
#define VSET_VX2                __riscv_vset_v_f32m2_f32m2x2
#define VLEV_FLOAT              __riscv_vle32_v_f32m2
#define VSEV_FLOAT              __riscv_vse32_v_f32m2
#define VLSEV_FLOAT             __riscv_vlse32_v_f32m2
#define VLSEG2_FLOAT            __riscv_vlseg2e32_v_f32m2x2
#define VLSSEG2_FLOAT           __riscv_vlsseg2e32_v_f32m2x2
#define VSSEG2_FLOAT            __riscv_vsseg2e32_v_f32m2x2
#define INT_V_T                 vint32m2_t
#define VID_V_INT               __riscv_vid_v_u32m2
#define VADD_VX_INT             __riscv_vadd_vx_i32m2
#define VFRSUB_VF_FLOAT         __riscv_vfrsub_vf_f32m2
#define VMSGT_VX_INT            __riscv_vmsgt_vx_i32m2_b16
#define VMSLT_VX_INT            __riscv_vmslt_vx_i32m2_b16
#define VMSEQ_VX_INT            __riscv_vmseq_vx_i32m2_b16
#define VBOOL_T                 vbool16_t
#define VMERGE_VVM_FLOAT        __riscv_vmerge_vvm_f32m2
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f32m2
#define V_UM2_TO_IM2            __riscv_vreinterpret_v_u32m2_i32m2
#else
#define VSETVL(n)               __riscv_vsetvl_e64m2(n)
#define VSETVL_MAX              __riscv_vsetvlmax_e64m2()
#define FLOAT_V_T               vfloat64m2_t
#define FLOAT_VX2_T             vfloat64m2x2_t
#define VGET_VX2                __riscv_vget_v_f64m2x2_f64m2
#define VSET_VX2                __riscv_vset_v_f64m2_f64m2x2
#define VLEV_FLOAT              __riscv_vle64_v_f64m2
#define VSEV_FLOAT              __riscv_vse64_v_f64m2
#define VLSEV_FLOAT             __riscv_vlse64_v_f64m2
#define VLSEG2_FLOAT            __riscv_vlseg2e64_v_f64m2x2
#define VLSSEG2_FLOAT           __riscv_vlsseg2e64_v_f64m2x2
#define VSSEG2_FLOAT            __riscv_vsseg2e64_v_f64m2x2
#define INT_V_T                 vint64m2_t
#define VID_V_INT               __riscv_vid_v_u64m2
#define VADD_VX_INT             __riscv_vadd_vx_i64m2
#define VFRSUB_VF_FLOAT         __riscv_vfrsub_vf_f64m2
#define VMSGT_VX_INT            __riscv_vmsgt_vx_i64m2_b32
#define VMSLT_VX_INT            __riscv_vmslt_vx_i64m2_b32
#define VMSEQ_VX_INT            __riscv_vmseq_vx_i64m2_b32
#define VBOOL_T                 vbool32_t
#define VMERGE_VVM_FLOAT        __riscv_vmerge_vvm_f64m2
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f64m2
#define V_UM2_TO_IM2            __riscv_vreinterpret_v_u64m2_i64m2
#endif


int CNAME(BLASLONG m, BLASLONG n, FLOAT *a, BLASLONG lda, BLASLONG posX, BLASLONG posY, FLOAT *b)
{
    BLASLONG i, js, offset;

    FLOAT *ao1, *ao2;
    //fprintf(stderr, "%s, %s, m=%ld n=%ld lda=%ld posX=%ld posY=%ld\n", __FUNCTION__, __FILE__, m, n, lda, posX, posY);
    BLASLONG stride_lda = sizeof(FLOAT) * lda * 2;
    
    FLOAT_V_T vb0, vb1, vb2, va10, va11, va20, va21, vzero;
    FLOAT_VX2_T va1x2, va2x2, vbx2;
    VBOOL_T vbool_gt0, vbool_eq0;
    INT_V_T vindex_max, vindex;

    size_t vl = VSETVL_MAX;
    vindex_max   = V_UM2_TO_IM2(VID_V_INT(vl));
    vzero = VFMVVF_FLOAT(ZERO, vl);

    for (js = n; js > 0; js -= vl, posX += vl) {
        vl = VSETVL(js);
        offset = posX - posY;

        ao1 = a + posY * 2 + posX * lda * 2;
        ao2 = a + posX * 2 + posY * lda * 2;

        for (i = m; i > 0; i--, offset--) {
            va1x2 = VLSSEG2_FLOAT(ao1, stride_lda, vl);
            va2x2 = VLSEG2_FLOAT(ao2, vl);

            va20 = VGET_VX2(va2x2, 0);
            va21 = VGET_VX2(va2x2, 1);
            va10 = VGET_VX2(va1x2, 0);
            va11 = VGET_VX2(va1x2, 1);

            vindex = VADD_VX_INT(vindex_max, offset, vl);
            vbool_gt0  = VMSGT_VX_INT(vindex, 0, vl);
            vbool_eq0  = VMSEQ_VX_INT(vindex, 0, vl);

            vb0 =  VMERGE_VVM_FLOAT(va20, va10, vbool_gt0, vl);
            vb1 =  VMERGE_VVM_FLOAT(va21, va11, vbool_gt0, vl);

            vb2 =  VFRSUB_VF_FLOAT(vb1, ZERO, vl);

            vb1 =  VMERGE_VVM_FLOAT(vb1, vb2, vbool_gt0, vl);
            vb1 =  VMERGE_VVM_FLOAT(vb1, vzero, vbool_eq0, vl);

            vbx2 = VSET_VX2(vbx2, 0, vb0);
            vbx2 = VSET_VX2(vbx2, 1, vb1);
            VSSEG2_FLOAT(b, vbx2, vl);

            b   += vl * 2;
            ao1 += 2;
            ao2 += lda * 2;
        }
    }

    return 0;
}