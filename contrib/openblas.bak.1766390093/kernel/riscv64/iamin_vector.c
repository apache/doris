/***************************************************************************
Copyright (c) 2020, The OpenBLAS Project
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
#include <math.h>
#include <float.h>

#if defined(DOUBLE)

#define VSETVL(n) RISCV_RVV(vsetvl_e64m8)(n)
#define FLOAT_V_T vfloat64m8_t
#define FLOAT_V_T_M1 vfloat64m1_t
#define VLEV_FLOAT RISCV_RVV(vle64_v_f64m8)
#define VLSEV_FLOAT RISCV_RVV(vlse64_v_f64m8)
#ifdef RISCV_0p10_INTRINSICS
#define VFREDMINVS_FLOAT(va, vb, gvl) vfredmin_vs_f64m8_f64m1(v_res, va, vb, gvl)
#define VIDV_MASK_UINT vid_v_u64m8_m
#define VADDVX_MASK_UINT vadd_vx_u64m8_m
#define VCOMPRESS(va, vm, gvl) RISCV_RVV(vcompress_vm_u64m8)(vm, compressed, va, gvl)
#else
#define VFREDMINVS_FLOAT __riscv_vfredmin_vs_f64m8_f64m1
#define VIDV_MASK_UINT __riscv_vid_v_u64m8_mu
#define VADDVX_MASK_UINT __riscv_vadd_vx_u64m8_mu
#define VCOMPRESS RISCV_RVV(vcompress_vm_u64m8)
#endif
#define MASK_T vbool8_t
#define VMFGTVV_FLOAT RISCV_RVV(vmfgt_vv_f64m8_b8)
#define VFMVVF_FLOAT RISCV_RVV(vfmv_v_f_f64m8)
#define VFMVVF_FLOAT_M1 RISCV_RVV(vfmv_v_f_f64m1)
#define VFMINVV_FLOAT RISCV_RVV(vfmin_vv_f64m8)
#define VMFLEVF_FLOAT RISCV_RVV(vmfle_vf_f64m8_b8)
#define VMFIRSTM RISCV_RVV(vfirst_m_b8)
#define UINT_V_T vuint64m8_t
#define VIDV_UINT RISCV_RVV(vid_v_u64m8)
#define VADDVX_UINT RISCV_RVV(vadd_vx_u64m8)
#define VMVVX_UINT RISCV_RVV(vmv_v_x_u64m8)
#define VFABS_FLOAT RISCV_RVV(vfabs_v_f64m8)
#define VMV_X RISCV_RVV(vmv_x_s_u64m8_u64)
#else

#define VSETVL(n) RISCV_RVV(vsetvl_e32m8)(n)
#define FLOAT_V_T vfloat32m8_t
#define FLOAT_V_T_M1 vfloat32m1_t
#define VLEV_FLOAT RISCV_RVV(vle32_v_f32m8)
#define VLSEV_FLOAT RISCV_RVV(vlse32_v_f32m8)
#ifdef RISCV_0p10_INTRINSICS
#define VFREDMINVS_FLOAT(va, vb, gvl) vfredmin_vs_f32m8_f32m1(v_res, va, vb, gvl)
#define VIDV_MASK_UINT vid_v_u32m8_m
#define VADDVX_MASK_UINT vadd_vx_u32m8_m
#define VCOMPRESS(va, vm, gvl) RISCV_RVV(vcompress_vm_u32m8)(vm, compressed, va, gvl)
#else
#define VFREDMINVS_FLOAT __riscv_vfredmin_vs_f32m8_f32m1
#define VIDV_MASK_UINT __riscv_vid_v_u32m8_mu
#define VADDVX_MASK_UINT __riscv_vadd_vx_u32m8_mu
#define VCOMPRESS RISCV_RVV(vcompress_vm_u32m8)
#endif
#define MASK_T vbool4_t
#define VMFGTVV_FLOAT RISCV_RVV(vmfgt_vv_f32m8_b4)
#define VFMVVF_FLOAT RISCV_RVV(vfmv_v_f_f32m8)
#define VFMVVF_FLOAT_M1 RISCV_RVV(vfmv_v_f_f32m1)
#define VFMINVV_FLOAT RISCV_RVV(vfmin_vv_f32m8)
#define VMFLEVF_FLOAT RISCV_RVV(vmfle_vf_f32m8_b4)
#define VMFIRSTM RISCV_RVV(vfirst_m_b4)
#define UINT_V_T vuint32m8_t
#define VIDV_UINT RISCV_RVV(vid_v_u32m8)
#define VADDVX_UINT RISCV_RVV(vadd_vx_u32m8)
#define VMVVX_UINT RISCV_RVV(vmv_v_x_u32m8)
#define VFABS_FLOAT RISCV_RVV(vfabs_v_f32m8)
#define VMV_X RISCV_RVV(vmv_x_s_u32m8_u32)
#endif


BLASLONG CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x)
{
        BLASLONG i=0, j=0;
        unsigned int min_index = 0;
        if (n <= 0 || inc_x <= 0) return(min_index);
        FLOAT minf=FLT_MAX;

        FLOAT_V_T vx, v_min;
        UINT_V_T v_min_index;
        MASK_T mask;
        unsigned int gvl = 0;
        FLOAT_V_T_M1 v_res;
        v_res = VFMVVF_FLOAT_M1(FLT_MAX, 1);

        if(inc_x == 1){
                gvl = VSETVL(n);
                v_min_index = VMVVX_UINT(0, gvl);
                v_min = VFMVVF_FLOAT(FLT_MAX, gvl);
                for(i=0,j=0; i < n/gvl; i++){
                        vx = VLEV_FLOAT(&x[j], gvl);
                        vx = VFABS_FLOAT(vx, gvl);

                        //index where element greater than v_min
                        mask = VMFGTVV_FLOAT(v_min, vx, gvl);
                        v_min_index = VIDV_MASK_UINT(mask, v_min_index, gvl);
                        v_min_index = VADDVX_MASK_UINT(mask, v_min_index, v_min_index, j, gvl);

                        //update v_min and start_index j
                        v_min = VFMINVV_FLOAT(v_min, vx, gvl);
                        j += gvl;
                }
                v_res = VFREDMINVS_FLOAT(v_min, v_res, gvl);
                minf = EXTRACT_FLOAT(v_res);
                mask = VMFLEVF_FLOAT(v_min, minf, gvl);
                UINT_V_T compressed;
                compressed = VCOMPRESS(v_min_index, mask, gvl);
                min_index = VMV_X(compressed);

                if(j < n){
                        gvl = VSETVL(n-j);
                        v_min = VLEV_FLOAT(&x[j], gvl);
                        v_min = VFABS_FLOAT(v_min, gvl);

                        v_res = VFREDMINVS_FLOAT(v_min, v_res, gvl);
                        FLOAT cur_minf = EXTRACT_FLOAT(v_res);
                        if(cur_minf < minf){
                                //tail index
                                v_min_index = VIDV_UINT(gvl);
                                v_min_index = VADDVX_UINT(v_min_index, j, gvl);

                                mask = VMFLEVF_FLOAT(v_min, cur_minf, gvl);
                                UINT_V_T compressed;
                                compressed = VCOMPRESS(v_min_index, mask, gvl);
                                min_index = VMV_X(compressed);
                        }
                }
        }else{
                gvl = VSETVL(n);
                unsigned int stride_x = inc_x * sizeof(FLOAT);
                unsigned int idx = 0, inc_v = gvl * inc_x;

                v_min = VFMVVF_FLOAT(FLT_MAX, gvl);
                v_min_index = VMVVX_UINT(0, gvl);
                for(i=0,j=0; i < n/gvl; i++){
                        vx = VLSEV_FLOAT(&x[idx], stride_x, gvl);
                        vx = VFABS_FLOAT(vx, gvl);

                        //index where element greater than v_min
                        mask = VMFGTVV_FLOAT(v_min, vx, gvl);
                        v_min_index = VIDV_MASK_UINT(mask, v_min_index, gvl);
                        v_min_index = VADDVX_MASK_UINT(mask, v_min_index, v_min_index, j, gvl);

                        //update v_min and start_index j
                        v_min = VFMINVV_FLOAT(v_min, vx, gvl);
                        j += gvl;
                        idx += inc_v;
                }
                v_res = VFREDMINVS_FLOAT(v_min, v_res, gvl);
                minf = EXTRACT_FLOAT(v_res);
                mask = VMFLEVF_FLOAT(v_min, minf, gvl);
                UINT_V_T compressed;
                compressed = VCOMPRESS(v_min_index, mask, gvl);
                min_index = VMV_X(compressed);

                if(j < n){
                        gvl = VSETVL(n-j);
                        v_min = VLSEV_FLOAT(&x[idx], stride_x, gvl);
                        v_min = VFABS_FLOAT(v_min, gvl);

                        v_res = VFREDMINVS_FLOAT(v_min, v_res, gvl);
                        FLOAT cur_minf = EXTRACT_FLOAT(v_res);
                        if(cur_minf < minf){
                                //tail index
                                v_min_index = VIDV_UINT(gvl);
                                v_min_index = VADDVX_UINT(v_min_index, j, gvl);

                                mask = VMFLEVF_FLOAT(v_min, cur_minf, gvl);
                                UINT_V_T compressed;
                                compressed = VCOMPRESS(v_min_index, mask, gvl);
                                min_index = VMV_X(compressed);
                        }
                }
        }
        return(min_index+1);
}
