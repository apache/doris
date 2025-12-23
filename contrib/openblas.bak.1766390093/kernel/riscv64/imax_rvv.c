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
#include <float.h>

#if defined(DOUBLE)
#define VSETVL(n)               __riscv_vsetvl_e64m8(n)
#define VSETVL_MAX              __riscv_vsetvlmax_e64m8()
#define FLOAT_V_T               vfloat64m8_t
#define FLOAT_V_T_M1            vfloat64m1_t
#define VLEV_FLOAT              __riscv_vle64_v_f64m8
#define VLSEV_FLOAT             __riscv_vlse64_v_f64m8
#define VFREDMAXVS_FLOAT        __riscv_vfredmax_vs_f64m8_f64m1
#define MASK_T                  vbool8_t
#define VMFLTVF_FLOAT           __riscv_vmflt_vf_f64m8_b8
#define VMFLTVV_FLOAT           __riscv_vmflt_vv_f64m8_b8
#define VMFGEVF_FLOAT           __riscv_vmfge_vf_f64m8_b8
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f64m8
#define VFMVVF_FLOAT_M1         __riscv_vfmv_v_f_f64m1
#define VFMAXVV_FLOAT_TU        __riscv_vfmax_vv_f64m8_tu
#define VFIRSTM                 __riscv_vfirst_m_b8
#define UINT_V_T                vuint64m8_t
#define VIDV_MASK_UINT_TU       __riscv_vid_v_u64m8_tumu
#define VIDV_UINT               __riscv_vid_v_u64m8
#define VADDVX_MASK_UINT_TU     __riscv_vadd_vx_u64m8_tumu
#define VADDVX_UINT             __riscv_vadd_vx_u64m8
#define VMVVX_UINT              __riscv_vmv_v_x_u64m8
#define VFMVFS_FLOAT_M1         __riscv_vfmv_f_s_f64m1_f64
#define VSLIDEDOWN_UINT         __riscv_vslidedown_vx_u64m8
#define VMVVXS_UINT             __riscv_vmv_x_s_u64m8_u64
#else
#define VSETVL(n)               __riscv_vsetvl_e32m8(n)
#define VSETVL_MAX              __riscv_vsetvlmax_e32m8()
#define FLOAT_V_T               vfloat32m8_t
#define FLOAT_V_T_M1            vfloat32m1_t
#define VLEV_FLOAT              __riscv_vle32_v_f32m8
#define VLSEV_FLOAT             __riscv_vlse32_v_f32m8
#define VFREDMAXVS_FLOAT        __riscv_vfredmax_vs_f32m8_f32m1
#define MASK_T                  vbool4_t
#define VMFLTVF_FLOAT           __riscv_vmflt_vf_f32m8_b4
#define VMFLTVV_FLOAT           __riscv_vmflt_vv_f32m8_b4
#define VMFGEVF_FLOAT           __riscv_vmfge_vf_f32m8_b4
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f32m8
#define VFMVVF_FLOAT_M1         __riscv_vfmv_v_f_f32m1
#define VFMAXVV_FLOAT_TU        __riscv_vfmax_vv_f32m8_tu
#define VFIRSTM                 __riscv_vfirst_m_b4
#define UINT_V_T                vuint32m8_t
#define VIDV_MASK_UINT_TU       __riscv_vid_v_u32m8_tumu
#define VIDV_UINT               __riscv_vid_v_u32m8
#define VADDVX_MASK_UINT_TU     __riscv_vadd_vx_u32m8_tumu
#define VADDVX_UINT             __riscv_vadd_vx_u32m8
#define VMVVX_UINT              __riscv_vmv_v_x_u32m8
#define VFMVFS_FLOAT_M1         __riscv_vfmv_f_s_f32m1_f32
#define VSLIDEDOWN_UINT         __riscv_vslidedown_vx_u32m8
#define VMVVXS_UINT             __riscv_vmv_x_s_u32m8_u32
#endif

BLASLONG CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x)
{
    unsigned int max_index = 0;
    if (n <= 0 || inc_x <= 0) return(max_index);

    FLOAT_V_T vx, v_max;
    UINT_V_T v_max_index;
    MASK_T mask;
  
    size_t vlmax = VSETVL_MAX;
    v_max_index = VMVVX_UINT(0, vlmax);
    v_max = VFMVVF_FLOAT(-FLT_MAX, vlmax);
    BLASLONG j=0;
    FLOAT maxf=0.0;
    
    if(inc_x == 1) {

        for (size_t vl; n > 0; n -= vl, x += vl, j += vl) {
            vl = VSETVL(n);

            vx = VLEV_FLOAT(x, vl);

            //index where element greater than v_max
            mask = VMFLTVV_FLOAT(v_max, vx, vl);
            v_max_index = VIDV_MASK_UINT_TU(mask, v_max_index, vl);
            v_max_index = VADDVX_MASK_UINT_TU(mask, v_max_index, v_max_index, j, vl);

            //update v_max and start_index j
            v_max = VFMAXVV_FLOAT_TU(v_max, v_max, vx, vl);
        }

    } else {
  
        BLASLONG stride_x = inc_x * sizeof(FLOAT);

        for (size_t vl; n > 0; n -= vl, x += vl*inc_x, j += vl) {
            vl = VSETVL(n);

            vx = VLSEV_FLOAT(x, stride_x, vl);

            //index where element greater than v_max
            mask = VMFLTVV_FLOAT(v_max, vx, vl);
            v_max_index = VIDV_MASK_UINT_TU(mask, v_max_index, vl);
            v_max_index = VADDVX_MASK_UINT_TU(mask, v_max_index, v_max_index, j, vl);

            //update v_max and start_index j
            v_max = VFMAXVV_FLOAT_TU(v_max, v_max, vx, vl);
        }
  
    }

    FLOAT_V_T_M1 v_res;
    v_res = VFMVVF_FLOAT_M1(-FLT_MAX, vlmax);

    v_res = VFREDMAXVS_FLOAT(v_max, v_res, vlmax);
    maxf = VFMVFS_FLOAT_M1(v_res);
    mask = VMFGEVF_FLOAT(v_max, maxf, vlmax);
    max_index = VFIRSTM(mask, vlmax);
    
    v_max_index = VSLIDEDOWN_UINT(v_max_index, max_index, vlmax);
    max_index = VMVVXS_UINT(v_max_index);

    return(max_index+1);
}
