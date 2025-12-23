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
#if !defined(DOUBLE)
#define VSETVL(n) __riscv_vsetvl_e32m4(n)
#define VSETVL_MAX __riscv_vsetvlmax_e32m1()
#define FLOAT_V_T vfloat32m4_t
#define FLOAT_V_T_M1 vfloat32m1_t
#define VLEV_FLOAT __riscv_vle32_v_f32m4
#define VLSEV_FLOAT __riscv_vlse32_v_f32m4
#define VSEV_FLOAT __riscv_vse32_v_f32m4
#define VSSEV_FLOAT __riscv_vsse32_v_f32m4
#define VFREDSUM_FLOAT __riscv_vfredusum_vs_f32m4_f32m1
#define VFMACCVV_FLOAT __riscv_vfmacc_vv_f32m4
#define VFNMSACVV_FLOAT __riscv_vfnmsac_vv_f32m4
#define VFMACCVV_FLOAT_TU __riscv_vfmacc_vv_f32m4_tu
#define VFNMSACVV_FLOAT_TU __riscv_vfnmsac_vv_f32m4_tu
#define VFMACCVF_FLOAT __riscv_vfmacc_vf_f32m4
#define VFNMSACVF_FLOAT __riscv_vfnmsac_vf_f32m4
#define VFMVVF_FLOAT __riscv_vfmv_v_f_f32m4
#define VFMVVF_FLOAT_M1 __riscv_vfmv_v_f_f32m1
#define VFMULVV_FLOAT __riscv_vfmul_vv_f32m4
#define VFMVFS_FLOAT_M1 __riscv_vfmv_f_s_f32m1_f32
#define VFNEGV_FLOAT __riscv_vfneg_v_f32mf4
#else
#define VSETVL(n) __riscv_vsetvl_e64m4(n)
#define VSETVL_MAX __riscv_vsetvlmax_e64m1()
#define FLOAT_V_T vfloat64m4_t
#define FLOAT_V_T_M1 vfloat64m1_t
#define VLEV_FLOAT __riscv_vle64_v_f64m4
#define VLSEV_FLOAT __riscv_vlse64_v_f64m4
#define VSEV_FLOAT __riscv_vse64_v_f64m4
#define VSSEV_FLOAT __riscv_vsse64_v_f64m4
#define VFREDSUM_FLOAT __riscv_vfredusum_vs_f64m4_f64m1
#define VFMACCVV_FLOAT __riscv_vfmacc_vv_f64m4
#define VFNMSACVV_FLOAT __riscv_vfnmsac_vv_f64m4
#define VFMACCVV_FLOAT_TU __riscv_vfmacc_vv_f64m4_tu
#define VFNMSACVV_FLOAT_TU __riscv_vfnmsac_vv_f64m4_tu
#define VFMACCVF_FLOAT __riscv_vfmacc_vf_f64m4
#define VFNMSACVF_FLOAT __riscv_vfnmsac_vf_f64m4
#define VFMVVF_FLOAT __riscv_vfmv_v_f_f64m4
#define VFMVVF_FLOAT_M1 __riscv_vfmv_v_f_f64m1
#define VFMULVV_FLOAT __riscv_vfmul_vv_f64m4
#define VFMVFS_FLOAT_M1 __riscv_vfmv_f_s_f64m1_f64
#define VFNEGV_FLOAT __riscv_vfneg_v_f64mf4
#endif

int CNAME(BLASLONG m, BLASLONG offset, FLOAT alpha_r, FLOAT alpha_i,
	  FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *buffer)
{
        BLASLONG i, j, k;
        BLASLONG ix,iy;
        BLASLONG jx,jy;
        FLOAT temp1[2];
        FLOAT temp2[2];
        FLOAT *a_ptr = a;
        BLASLONG gvl = VSETVL_MAX;
        FLOAT_V_T_M1 v_res, v_z0;
        v_res = VFMVVF_FLOAT_M1(0, gvl);
        v_z0 = VFMVVF_FLOAT_M1(0, gvl);

        FLOAT_V_T va_r, va_i, vx_r, vx_i, vy_r, vy_i, vr_r, vr_i;
        BLASLONG stride_x, stride_y, inc_xv, inc_yv, len;

        stride_x = 2 * inc_x * sizeof(FLOAT);
        stride_y = 2 * inc_y * sizeof(FLOAT);
        jx = 0;
        jy = 0;
        for (j=0; j<offset; j++)
        {
                temp1[0] = alpha_r * x[2 * jx] - alpha_i * x[2 * jx + 1];
                temp1[1] = alpha_r * x[2 * jx + 1] + alpha_i * x[2 * jx];
                temp2[0] = 0;
                temp2[1] = 0;

		y[2 * jy] += temp1[0] * a_ptr[j * 2] - temp1[1] * a_ptr[j * 2 + 1];
		y[2 * jy + 1] += temp1[1] * a_ptr[j * 2] + temp1[0] * a_ptr[j * 2 + 1];

                ix = jx + inc_x;
                iy = jy + inc_y;
                i = j + 1;
                len = m - i;
                if(len > 0){
                        gvl = VSETVL(len);
                        inc_xv = inc_x * gvl;
                        inc_yv = inc_y * gvl;
                        vr_r = VFMVVF_FLOAT(0, gvl);
                        vr_i = VFMVVF_FLOAT(0, gvl);
                        for(k = 0; k < len / gvl; k++){
                                va_r = VLSEV_FLOAT(&a_ptr[2 * i], 2 * sizeof(FLOAT), gvl);
                                va_i = VLSEV_FLOAT(&a_ptr[2 * i + 1], 2 * sizeof(FLOAT), gvl);

                                vy_r = VLSEV_FLOAT(&y[2 * iy], stride_y, gvl);
                                vy_i = VLSEV_FLOAT(&y[2 * iy + 1], stride_y, gvl);
                                
                                vy_r = VFMACCVF_FLOAT(vy_r, temp1[0], va_r, gvl);
                                vy_r = VFNMSACVF_FLOAT(vy_r, temp1[1], va_i, gvl);
                                vy_i = VFMACCVF_FLOAT(vy_i, temp1[0], va_i, gvl);
                                vy_i = VFMACCVF_FLOAT(vy_i, temp1[1], va_r, gvl);
                                
                                VSSEV_FLOAT(&y[2 * iy], stride_y, vy_r, gvl);
                                VSSEV_FLOAT(&y[2 * iy + 1], stride_y, vy_i, gvl);

                                vx_r = VLSEV_FLOAT(&x[2 * ix], stride_x, gvl);
                                vx_i = VLSEV_FLOAT(&x[2 * ix + 1], stride_x, gvl);
                                vr_r = VFMACCVV_FLOAT(vr_r, vx_r, va_r, gvl);
                                vr_r = VFNMSACVV_FLOAT(vr_r, vx_i, va_i, gvl);
                                vr_i = VFMACCVV_FLOAT(vr_i, vx_r, va_i, gvl);
                                vr_i = VFMACCVV_FLOAT(vr_i, vx_i, va_r, gvl);

                                i += gvl;
                                ix += inc_xv;
                                iy += inc_yv;
                        }

                        if(i < m){
                                unsigned int gvl_rem = VSETVL(m-i);
                                vy_r = VLSEV_FLOAT(&y[2 * iy], stride_y, gvl_rem);
                                vy_i = VLSEV_FLOAT(&y[2 * iy + 1], stride_y, gvl_rem);
                                va_r = VLSEV_FLOAT(&a_ptr[2 * i], 2 * sizeof(FLOAT), gvl_rem);
                                va_i = VLSEV_FLOAT(&a_ptr[2 * i + 1], 2 * sizeof(FLOAT), gvl_rem);

                                vy_r = VFMACCVF_FLOAT(vy_r, temp1[0], va_r, gvl_rem);
                                vy_r = VFNMSACVF_FLOAT(vy_r, temp1[1], va_i, gvl_rem);
                                vy_i = VFMACCVF_FLOAT(vy_i, temp1[0], va_i, gvl_rem);
                                vy_i = VFMACCVF_FLOAT(vy_i, temp1[1], va_r, gvl_rem);
                                
                                VSSEV_FLOAT(&y[2 * iy], stride_y, vy_r, gvl_rem);
                                VSSEV_FLOAT(&y[2 * iy + 1], stride_y, vy_i, gvl_rem);

                                vx_r = VLSEV_FLOAT(&x[2 * ix], stride_x, gvl_rem);
                                vx_i = VLSEV_FLOAT(&x[2 * ix + 1], stride_x, gvl_rem);
                                vr_r = VFMACCVV_FLOAT_TU(vr_r, vx_r, va_r, gvl_rem);
                                vr_r = VFNMSACVV_FLOAT_TU(vr_r, vx_i, va_i, gvl_rem);
                                vr_i = VFMACCVV_FLOAT_TU(vr_i, vx_r, va_i, gvl_rem);
                                vr_i = VFMACCVV_FLOAT_TU(vr_i, vx_i, va_r, gvl_rem);
                                
                        }
                        v_res = VFREDSUM_FLOAT(vr_r, v_z0, gvl);
                        temp2[0] = VFMVFS_FLOAT_M1(v_res);
                        v_res = VFREDSUM_FLOAT(vr_i, v_z0, gvl);
                        temp2[1] = VFMVFS_FLOAT_M1(v_res);
                }
                y[2 * jy] += alpha_r * temp2[0] - alpha_i * temp2[1];
                y[2 * jy + 1] += alpha_r * temp2[1] + alpha_i * temp2[0];

                jx    += inc_x;
                jy    += inc_y;
                a_ptr += 2 * lda;
        }
                
        return(0);
}

