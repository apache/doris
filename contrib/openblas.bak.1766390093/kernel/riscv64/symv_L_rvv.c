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
#define VSETVL_MAX_M1           __riscv_vsetvlmax_e32m1()
#define VSETVL(n)               __riscv_vsetvl_e32m8(n)
#define VSETVL_MAX              __riscv_vsetvlmax_e32m8()
#define FLOAT_V_T_M1            vfloat32m1_t
#define FLOAT_V_T               vfloat32m8_t
#define VLEV_FLOAT              __riscv_vle32_v_f32m8
#define VSEV_FLOAT              __riscv_vse32_v_f32m8
#define VLSEV_FLOAT             __riscv_vlse32_v_f32m8
#define VSSEV_FLOAT             __riscv_vsse32_v_f32m8
#define VFMACCVV_FLOAT_TU       __riscv_vfmacc_vv_f32m8_tu
#define VFMACCVF_FLOAT          __riscv_vfmacc_vf_f32m8
#define VFNMSACVF_FLOAT         __riscv_vfnmsac_vf_f32m8
#define VFMULVF_FLOAT           __riscv_vfmul_vf_f32m8
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f32m8
#define VFMSACVF_FLOAT          __riscv_vfmsac_vf_f32m8
#define VFMVVF_FLOAT_M1         __riscv_vfmv_v_f_f32m1
#define VFREDSUM_FLOAT          __riscv_vfredusum_vs_f32m8_f32m1
#define VFMVFS_FLOAT_M1         __riscv_vfmv_f_s_f32m1_f32
#else
#define VSETVL_MAX_M1           __riscv_vsetvlmax_e64m1()
#define VSETVL(n)               __riscv_vsetvl_e64m8(n)
#define VSETVL_MAX              __riscv_vsetvlmax_e64m8()
#define FLOAT_V_T_M1            vfloat64m1_t
#define FLOAT_V_T               vfloat64m8_t
#define VLEV_FLOAT              __riscv_vle64_v_f64m8
#define VSEV_FLOAT              __riscv_vse64_v_f64m8
#define VLSEV_FLOAT             __riscv_vlse64_v_f64m8
#define VSSEV_FLOAT             __riscv_vsse64_v_f64m8
#define VFMACCVV_FLOAT_TU       __riscv_vfmacc_vv_f64m8_tu
#define VFMACCVF_FLOAT          __riscv_vfmacc_vf_f64m8
#define VFNMSACVF_FLOAT         __riscv_vfnmsac_vf_f64m8
#define VFMULVF_FLOAT           __riscv_vfmul_vf_f64m8
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f64m8
#define VFMSACVF_FLOAT          __riscv_vfmsac_vf_f64m8
#define VFMVVF_FLOAT_M1         __riscv_vfmv_v_f_f64m1
#define VFREDSUM_FLOAT          __riscv_vfredusum_vs_f64m8_f64m1
#define VFMVFS_FLOAT_M1         __riscv_vfmv_f_s_f64m1_f64
#endif

int CNAME(BLASLONG m, BLASLONG offset, FLOAT alpha, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *buffer)
{
        BLASLONG i, j, k;
        BLASLONG ix,iy;
        BLASLONG jx,jy;
        FLOAT temp1;
        FLOAT *a_ptr = a;

        FLOAT_V_T_M1 v_res, v_z0;
        size_t vlmax = VSETVL_MAX_M1, vl;
        v_z0 = VFMVVF_FLOAT_M1(0, vlmax);
        vlmax = VSETVL_MAX;

        FLOAT_V_T va, vx, vy, vr;
        BLASLONG stride_x, stride_y, inc_xv, inc_yv;

        if(inc_x == 1 && inc_y == 1)
        {
                for (j=0; j<offset; j++)
                {
                        temp1 = alpha * x[j];
                        y[j] += temp1 * a_ptr[j];
                        i = j + 1;
                        vr = VFMVVF_FLOAT(0, vlmax);
                        for (k = (m-i); k > 0; k -= vl, i += vl)
                        {
                                vl = VSETVL(k);
                                va = VLEV_FLOAT(&a_ptr[i], vl);
                                vy = VLEV_FLOAT(&y[i], vl);
                                vy = VFMACCVF_FLOAT(vy, temp1, va, vl);
                                VSEV_FLOAT(&y[i], vy, vl);

                                vx = VLEV_FLOAT(&x[i], vl);
                                vr = VFMACCVV_FLOAT_TU(vr, vx, va, vl);

                        }
                        v_res = VFREDSUM_FLOAT(vr, v_z0, vlmax);

                        y[j] += alpha * VFMVFS_FLOAT_M1(v_res);
                        a_ptr += lda;
                }
        }
        else if(inc_x == 1)
        {
                jy = 0;
                stride_y = inc_y * sizeof(FLOAT);
                for (j=0; j<offset; j++)
                {
                        temp1 = alpha * x[j];
                        y[jy] += temp1 * a_ptr[j];
                        iy = jy + inc_y;
                        i = j + 1;
                        vr = VFMVVF_FLOAT(0, vlmax);
                        for (k = (m-i); k > 0; k -= vl, i += vl)
                        {
                                vl = VSETVL(k);
                                inc_yv = inc_y * vl;
                                va = VLEV_FLOAT(&a_ptr[i], vl);
                                vy = VLSEV_FLOAT(&y[iy], stride_y, vl);
                                vy = VFMACCVF_FLOAT(vy, temp1, va, vl);
                                VSSEV_FLOAT(&y[iy], stride_y, vy, vl);

                                vx = VLEV_FLOAT(&x[i], vl);
                                vr = VFMACCVV_FLOAT_TU(vr, vx, va, vl);

                                iy += inc_yv;
                        }
                        v_res = VFREDSUM_FLOAT(vr, v_z0, vlmax);

                        y[jy] += alpha * VFMVFS_FLOAT_M1(v_res);
                        jy    += inc_y;
                        a_ptr += lda;
                }
        }
        else if(inc_y == 1)
        {
                jx = 0;
                stride_x = inc_x * sizeof(FLOAT);
                for (j=0; j<offset; j++)
                {
                        temp1 = alpha * x[jx];
                        y[j] += temp1 * a_ptr[j];
                        ix = jx + inc_x;
                        i = j + 1;
                        vr = VFMVVF_FLOAT(0, vlmax);
                        for (k = (m-i); k > 0; k -= vl, i += vl)
                        {
                                vl = VSETVL(k);
                                inc_xv = inc_x * vl;

                                va = VLEV_FLOAT(&a_ptr[i], vl);
                                vy = VLEV_FLOAT(&y[i], vl);
                                vy = VFMACCVF_FLOAT(vy, temp1, va, vl);
                                VSEV_FLOAT(&y[i], vy, vl);

                                vx = VLSEV_FLOAT(&x[ix], stride_x, vl);
                                vr = VFMACCVV_FLOAT_TU(vr, vx, va, vl);

                                ix += inc_xv;
                        }

                        v_res = VFREDSUM_FLOAT(vr, v_z0, vlmax);

                        y[j] += alpha * VFMVFS_FLOAT_M1(v_res);
                        jx    += inc_x;
                        a_ptr += lda;
                }
        }
        else
        {
                stride_x = inc_x * sizeof(FLOAT);
                stride_y = inc_y * sizeof(FLOAT);
                jx = 0;
                jy = 0;
                for (j=0; j<offset; j++)
                {
                        temp1 = alpha * x[jx];
                        y[jy] += temp1 * a_ptr[j];
                        ix = jx + inc_x;
                        iy = jy + inc_y;
                        i = j + 1;
                        vr = VFMVVF_FLOAT(0, vlmax);
                        for (k = (m-i); k > 0; k -= vl, i += vl)
                        {
                                vl = VSETVL(k);
                                inc_xv = inc_x * vl;
                                inc_yv = inc_y * vl;
                                
                                va = VLEV_FLOAT(&a_ptr[i], vl);
                                vy = VLSEV_FLOAT(&y[iy], stride_y, vl);
                                vy = VFMACCVF_FLOAT(vy, temp1, va, vl);
                                VSSEV_FLOAT(&y[iy], stride_y, vy, vl);

                                vx = VLSEV_FLOAT(&x[ix], stride_x, vl);
                                vr = VFMACCVV_FLOAT_TU(vr, vx, va, vl);

                                ix += inc_xv;
                                iy += inc_yv;
                        }
                        v_res = VFREDSUM_FLOAT(vr, v_z0, vlmax);

                        y[jy] += alpha * VFMVFS_FLOAT_M1(v_res);
                        jx    += inc_x;
                        jy    += inc_y;
                        a_ptr += lda;
                }
        }
        return(0);
}

