/***************************************************************************
Copyright (c) 2013, The OpenBLAS Project
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
#define VFMVFS_FLOAT __riscv_vfmv_f_s_f32m1_f32
#define VLSEV_FLOAT __riscv_vlse32_v_f32m4
#define VSSEV_FLOAT __riscv_vsse32_v_f32m4
#define VFREDSUM_FLOAT __riscv_vfredusum_vs_f32m4_f32m1
#define VFMACCVV_FLOAT __riscv_vfmacc_vv_f32m4
#define VFMACCVV_FLOAT_TU __riscv_vfmacc_vv_f32m4_tu
#define VFMACCVF_FLOAT __riscv_vfmacc_vf_f32m4
#define VFMVVF_FLOAT __riscv_vfmv_v_f_f32m4
#define VFMVVF_FLOAT_M1 __riscv_vfmv_v_f_f32m1
#define VFMULVV_FLOAT __riscv_vfmul_vv_f32m4
#define VFNMSACVF_FLOAT __riscv_vfnmsac_vf_f32m4
#define VFNMSACVV_FLOAT __riscv_vfnmsac_vv_f32m4
#define VFNMSACVV_FLOAT_TU __riscv_vfnmsac_vv_f32m4_tu
#else
#define VSETVL(n) __riscv_vsetvl_e64m4(n)
#define VSETVL_MAX __riscv_vsetvlmax_e64m1()
#define FLOAT_V_T vfloat64m4_t
#define FLOAT_V_T_M1 vfloat64m1_t
#define VFMVFS_FLOAT __riscv_vfmv_f_s_f64m1_f64
#define VLSEV_FLOAT __riscv_vlse64_v_f64m4
#define VSSEV_FLOAT __riscv_vsse64_v_f64m4
#define VFREDSUM_FLOAT __riscv_vfredusum_vs_f64m4_f64m1
#define VFMACCVV_FLOAT __riscv_vfmacc_vv_f64m4
#define VFMACCVV_FLOAT_TU __riscv_vfmacc_vv_f64m4_tu
#define VFMACCVF_FLOAT __riscv_vfmacc_vf_f64m4
#define VFMVVF_FLOAT __riscv_vfmv_v_f_f64m4
#define VFMVVF_FLOAT_M1 __riscv_vfmv_v_f_f64m1
#define VFMULVV_FLOAT __riscv_vfmul_vv_f64m4
#define VFNMSACVF_FLOAT __riscv_vfnmsac_vf_f64m4
#define VFNMSACVV_FLOAT __riscv_vfnmsac_vv_f64m4
#define VFNMSACVV_FLOAT_TU __riscv_vfnmsac_vv_f64m4_tu
#endif

int CNAME(BLASLONG m, BLASLONG offset, FLOAT alpha_r, FLOAT alpha_i, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG incx, FLOAT *y, BLASLONG incy, FLOAT *buffer){
        BLASLONG i, j, k;
        BLASLONG ix, iy, ia;
        BLASLONG jx, jy, ja;
        FLOAT temp_r1, temp_i1;
        FLOAT temp_r2, temp_i2;
        FLOAT *a_ptr = a;
        unsigned int gvl = 0;
        FLOAT_V_T_M1 v_res, v_z0;
        gvl = VSETVL_MAX;
        v_res = VFMVVF_FLOAT_M1(0, gvl);
        v_z0 = VFMVVF_FLOAT_M1(0, gvl);

        FLOAT_V_T va0, va1, vx0, vx1, vy0, vy1, vr0, vr1;
        BLASLONG stride_x, stride_y, stride_a, inc_xv, inc_yv, inc_av, lda2;

        BLASLONG inc_x2 = incx * 2;
        BLASLONG inc_y2 = incy * 2;
        stride_x = inc_x2 * sizeof(FLOAT);
        stride_y = inc_y2 * sizeof(FLOAT);
        stride_a = 2 * sizeof(FLOAT);
        lda2 = lda * 2;

        BLASLONG m1 = m - offset;
        a_ptr = a + m1 * lda2;
        jx = m1 * inc_x2;
        jy = m1 * inc_y2;
        ja = m1 * 2;
        for(j = m1; j < m; j++){
                temp_r1 = alpha_r * x[jx]   - alpha_i * x[jx+1];;
                temp_i1 = alpha_r * x[jx+1] + alpha_i * x[jx];
                temp_r2 = 0;
                temp_i2 = 0;
                ix = 0;
                iy = 0;
                ia = 0;
                i = 0;
                if(j > 0){
                        gvl = VSETVL(j);
                        inc_xv = incx * gvl * 2;
                        inc_yv = incy * gvl * 2;
                        inc_av = gvl * 2;
                        vr0 = VFMVVF_FLOAT(0, gvl);
                        vr1 = VFMVVF_FLOAT(0, gvl);
                        for(k = 0; k < j / gvl; k++){
                                va0 = VLSEV_FLOAT(&a_ptr[ia], stride_a, gvl);
                                va1 = VLSEV_FLOAT(&a_ptr[ia+1], stride_a, gvl);
                                vy0 = VLSEV_FLOAT(&y[iy], stride_y, gvl);
                                vy1 = VLSEV_FLOAT(&y[iy+1], stride_y, gvl);
#ifndef HEMVREV
                                vy0 = VFMACCVF_FLOAT(vy0, temp_r1, va0, gvl);
                                vy0 = VFNMSACVF_FLOAT(vy0, temp_i1, va1, gvl);
                                vy1 = VFMACCVF_FLOAT(vy1, temp_r1, va1, gvl);
                                vy1 = VFMACCVF_FLOAT(vy1, temp_i1, va0, gvl);
#else
                                vy0 = VFMACCVF_FLOAT(vy0, temp_r1, va0, gvl);
                                vy0 = VFMACCVF_FLOAT(vy0, temp_i1, va1, gvl);
                                vy1 = VFNMSACVF_FLOAT(vy1, temp_r1, va1, gvl);
                                vy1 = VFMACCVF_FLOAT(vy1, temp_i1, va0, gvl);
#endif
                                VSSEV_FLOAT(&y[iy], stride_y, vy0, gvl);
                                VSSEV_FLOAT(&y[iy+1], stride_y, vy1, gvl);

                                vx0 = VLSEV_FLOAT(&x[ix], stride_x, gvl);
                                vx1 = VLSEV_FLOAT(&x[ix+1], stride_x, gvl);
#ifndef HEMVREV
                                vr0 = VFMACCVV_FLOAT(vr0, vx0, va0, gvl);
                                vr0 = VFMACCVV_FLOAT(vr0, vx1, va1, gvl);
                                vr1 = VFMACCVV_FLOAT(vr1, vx1, va0, gvl);
                                vr1 = VFNMSACVV_FLOAT(vr1, vx0, va1, gvl);
#else
                                vr0 = VFMACCVV_FLOAT(vr0, vx0, va0, gvl);
                                vr0 = VFNMSACVV_FLOAT(vr0, vx1, va1, gvl);
                                vr1 = VFMACCVV_FLOAT(vr1, vx1, va0, gvl);
                                vr1 = VFMACCVV_FLOAT(vr1, vx0, va1, gvl);

#endif
                                i += gvl;
                                ix += inc_xv;
                                iy += inc_yv;
                                ia += inc_av;
                        }

                        if(i < j){
				unsigned int gvl_rem = VSETVL(j-i);
                                va0 = VLSEV_FLOAT(&a_ptr[ia], stride_a, gvl_rem);
                                va1 = VLSEV_FLOAT(&a_ptr[ia+1], stride_a, gvl_rem);
                                vy0 = VLSEV_FLOAT(&y[iy], stride_y, gvl_rem);
                                vy1 = VLSEV_FLOAT(&y[iy+1], stride_y, gvl_rem);
#ifndef HEMVREV
                                vy0 = VFMACCVF_FLOAT(vy0, temp_r1, va0, gvl_rem);
                                vy0 = VFNMSACVF_FLOAT(vy0, temp_i1, va1, gvl_rem);
                                vy1 = VFMACCVF_FLOAT(vy1, temp_r1, va1, gvl_rem);
                                vy1 = VFMACCVF_FLOAT(vy1, temp_i1, va0, gvl_rem);
#else
                                vy0 = VFMACCVF_FLOAT(vy0, temp_r1, va0, gvl_rem);
                                vy0 = VFMACCVF_FLOAT(vy0, temp_i1, va1, gvl_rem);
                                vy1 = VFNMSACVF_FLOAT(vy1, temp_r1, va1, gvl_rem);
                                vy1 = VFMACCVF_FLOAT(vy1, temp_i1, va0, gvl_rem);
#endif
                                VSSEV_FLOAT(&y[iy], stride_y, vy0, gvl_rem);
                                VSSEV_FLOAT(&y[iy+1], stride_y, vy1, gvl_rem);

                                vx0 = VLSEV_FLOAT(&x[ix], stride_x, gvl_rem);
                                vx1 = VLSEV_FLOAT(&x[ix+1], stride_x, gvl_rem);
#ifndef HEMVREV
                                vr0 = VFMACCVV_FLOAT_TU(vr0, vx0, va0, gvl_rem);
                                vr0 = VFMACCVV_FLOAT_TU(vr0, vx1, va1, gvl_rem);
                                vr1 = VFMACCVV_FLOAT_TU(vr1, vx1, va0, gvl_rem);
                                vr1 = VFNMSACVV_FLOAT_TU(vr1, vx0, va1, gvl_rem);
#else
                                vr0 = VFMACCVV_FLOAT_TU(vr0, vx0, va0, gvl_rem);
                                vr0 = VFNMSACVV_FLOAT_TU(vr0, vx1, va1, gvl_rem);
                                vr1 = VFMACCVV_FLOAT_TU(vr1, vx1, va0, gvl_rem);
                                vr1 = VFMACCVV_FLOAT_TU(vr1, vx0, va1, gvl_rem);
#endif
                        }
                        v_res = VFREDSUM_FLOAT(vr0, v_z0, gvl);
                        temp_r2 = VFMVFS_FLOAT(v_res);
                        v_res = VFREDSUM_FLOAT(vr1, v_z0, gvl);
                        temp_i2 = VFMVFS_FLOAT(v_res);
                }
                y[jy] += temp_r1 * a_ptr[ja];
                y[jy+1] += temp_i1 * a_ptr[ja];
		y[jy] += alpha_r * temp_r2 - alpha_i * temp_i2;
		y[jy+1] += alpha_r * temp_i2 + alpha_i * temp_r2;
		jx    += inc_x2;
		jy    += inc_y2;
		ja    += 2;
		a_ptr += lda2;
        }
	return(0);
}
