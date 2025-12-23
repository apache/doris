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
#define VSETVL(n) RISCV_RVV(vsetvl_e32m2)(n)
#define FLOAT_V_T vfloat32m2_t
#define VLEV_FLOAT RISCV_RVV(vle32_v_f32m2)
#define VLSEV_FLOAT RISCV_RVV(vlse32_v_f32m2)
#define VSEV_FLOAT RISCV_RVV(vse32_v_f32m2)
#define VSSEV_FLOAT RISCV_RVV(vsse32_v_f32m2)
#define VFMACCVF_FLOAT RISCV_RVV(vfmacc_vf_f32m2)
#define VFNMSACVF_FLOAT RISCV_RVV(vfnmsac_vf_f32m2)
#define VFMUL_VF_FLOAT RISCV_RVV(vfmul_vf_f32m2)
#define VSEV_FLOAT RISCV_RVV(vse32_v_f32m2)
#else
#define VSETVL(n) RISCV_RVV(vsetvl_e64m2)(n)
#define FLOAT_V_T vfloat64m2_t
#define VLEV_FLOAT RISCV_RVV(vle64_v_f64m2)
#define VLSEV_FLOAT RISCV_RVV(vlse64_v_f64m2)
#define VSEV_FLOAT RISCV_RVV(vse64_v_f64m2)
#define VSSEV_FLOAT RISCV_RVV(vsse64_v_f64m2)
#define VFMACCVF_FLOAT RISCV_RVV(vfmacc_vf_f64m2)
#define VFNMSACVF_FLOAT RISCV_RVV(vfnmsac_vf_f64m2)
#define VFMUL_VF_FLOAT RISCV_RVV(vfmul_vf_f64m2)
#define VSEV_FLOAT RISCV_RVV(vse64_v_f64m2)
#endif

int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alpha_r, FLOAT alpha_i, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *buffer)
{
        BLASLONG i = 0, j = 0, k = 0;
        BLASLONG ix = 0, iy = 0;
        FLOAT *a_ptr = a;
        FLOAT temp_r = 0.0, temp_i = 0.0, temp_r1, temp_i1, temp_r2, temp_i2, temp_r3, temp_i3, temp_rr[4], temp_ii[4];
        FLOAT_V_T va0, va1, vy0, vy1, vy0_new, vy1_new, va2, va3, va4, va5, va6, va7, temp_iv, temp_rv, x_v0, x_v1, temp_v1, temp_v2, temp_v3, temp_v4;
        unsigned int gvl = 0;
        BLASLONG stride_a = sizeof(FLOAT) * 2;
        BLASLONG stride_y = inc_y * sizeof(FLOAT) * 2;
        gvl = VSETVL(m);
        BLASLONG inc_yv = inc_y * gvl * 2;
        BLASLONG inc_x2 = inc_x * 2;
        BLASLONG lda2 = lda * 2;
        vy0_new = VLSEV_FLOAT(&y[iy], stride_y, gvl);
        vy1_new = VLSEV_FLOAT(&y[iy + 1], stride_y, gvl);
        for (k = 0, j = 0; k < m / gvl; k ++)
        {
                a_ptr = a;
                ix = 0;
                vy0 = vy0_new;
                vy1 = vy1_new;

                if (k < m / gvl - 1)
                {
                        vy0_new = VLSEV_FLOAT(&y[iy + inc_yv], stride_y, gvl);
                        vy1_new = VLSEV_FLOAT(&y[iy + inc_yv + 1], stride_y, gvl);
                }
                for (i = 0; i < n % 4; i++)
                {
#if !defined(XCONJ)
                        temp_r = alpha_r * x[ix] - alpha_i * x[ix + 1];
                        temp_i = alpha_r * x[ix + 1] + alpha_i * x[ix];
#else
                        temp_r = alpha_r * x[ix] + alpha_i * x[ix + 1];
                        temp_i = alpha_r * x[ix + 1] - alpha_i * x[ix];
#endif

                        va0 = VLSEV_FLOAT(&a_ptr[j], stride_a, gvl);
                        va1 = VLSEV_FLOAT(&a_ptr[j + 1], stride_a, gvl);
#if !defined(CONJ)
#if !defined(XCONJ)
                        vy0 = VFMACCVF_FLOAT(vy0, temp_r, va0, gvl);
                        vy0 = VFNMSACVF_FLOAT(vy0, temp_i, va1, gvl);
                        vy1 = VFMACCVF_FLOAT(vy1, temp_r, va1, gvl);
                        vy1 = VFMACCVF_FLOAT(vy1, temp_i, va0, gvl);
#else

                        vy0 = VFMACCVF_FLOAT(vy0, temp_r, va0, gvl);
                        vy0 = VFMACCVF_FLOAT(vy0, temp_i, va1, gvl);
                        vy1 = VFMACCVF_FLOAT(vy1, temp_r, va1, gvl);
                        vy1 = VFNMSACVF_FLOAT(vy1, temp_i, va0, gvl);
#endif

#else

#if !defined(XCONJ)
                        vy0 = VFMACCVF_FLOAT(vy0, temp_r, va0, gvl);
                        vy0 = VFMACCVF_FLOAT(vy0, temp_i, va1, gvl);
                        vy1 = VFNMSACVF_FLOAT(vy1, temp_r, va1, gvl);
                        vy1 = VFMACCVF_FLOAT(vy1, temp_i, va0, gvl);
#else
                        vy0 = VFMACCVF_FLOAT(vy0, temp_r, va0, gvl);
                        vy0 = VFNMSACVF_FLOAT(vy0, temp_i, va1, gvl);
                        vy1 = VFNMSACVF_FLOAT(vy1, temp_r, va1, gvl);
                        vy1 = VFNMSACVF_FLOAT(vy1, temp_i, va0, gvl);
#endif

#endif
                        a_ptr += lda2;
                        ix += inc_x2;
                        
                }

                for (i = n % 4 ; i < n; i += 4)
                {
#if !defined(XCONJ)
                        // temp_rr[0] = alpha_r * x[ix] - alpha_i * x[ix + 1];
                        // temp_rr[1] = alpha_r * x[ix + inc_x2] - alpha_i * x[ix + inc_x2 + 1];
                        x_v0 = VLSEV_FLOAT(&x[ix], inc_x2 * sizeof(FLOAT), 2);
                        x_v1 = VLSEV_FLOAT(&x[ix + 1], inc_x2 * sizeof(FLOAT), 2);
                        temp_rv = VFMUL_VF_FLOAT(x_v0, alpha_r, 2);
                        temp_rv = VFNMSACVF_FLOAT(temp_rv, alpha_i, x_v1, 2);         

                        // temp_ii[0] = alpha_r * x[ix + 1] + alpha_i * x[ix]; 
                        // temp_ii[1] = alpha_r * x[ix + inc_x2 + 1] + alpha_i * x[ix + inc_x2];
                        temp_iv = VFMUL_VF_FLOAT(x_v0, alpha_i, 2);
                        temp_iv = VFMACCVF_FLOAT(temp_iv, alpha_r, x_v1, 2);
                        VSEV_FLOAT(&temp_rr[0], temp_rv, 2);
                        VSEV_FLOAT(&temp_ii[0], temp_iv, 2);
                                  
                        // temp_rr[2] = alpha_r * x[ix + inc_x2 * 2] - alpha_i * x[ix + inc_x2 * 2 + 1];
                        // temp_rr[3] = alpha_r * x[ix + inc_x2 * 3] - alpha_i * x[ix + inc_x2 * 3 + 1];
                        x_v0 = VLSEV_FLOAT(&x[ix + inc_x2 * 2], inc_x2 * sizeof(FLOAT), 2);
                        x_v1 = VLSEV_FLOAT(&x[ix + inc_x2 * 2 + 1], inc_x2 * sizeof(FLOAT), 2);
                        temp_rv = VFMUL_VF_FLOAT(x_v0, alpha_r, 2);
                        temp_rv = VFNMSACVF_FLOAT(temp_rv, alpha_i, x_v1, 2);

                        // temp_ii[2] = alpha_r * x[ix + inc_x2 * 2 + 1] + alpha_i * x[ix + inc_x2 * 2];
                        // temp_ii[3] = alpha_r * x[ix + inc_x2 * 3 + 1] + alpha_i * x[ix + inc_x2 * 3];
                        temp_iv = VFMUL_VF_FLOAT(x_v0, alpha_i, 2);
                        temp_iv = VFMACCVF_FLOAT(temp_iv, alpha_r, x_v1, 2);
                        VSEV_FLOAT(&temp_rr[2], temp_rv, 2);
                        VSEV_FLOAT(&temp_ii[2], temp_iv, 2);

#else
                        //  temp_rr[0] = alpha_r * x[ix] + alpha_i * x[ix + 1];
                        //  temp_rr[1] = alpha_r * x[ix + inc_x2] + alpha_i * x[ix + inc_x2 + 1];
                        x_v0 = VLSEV_FLOAT(&x[ix], inc_x2 * sizeof(FLOAT), 2);
                        x_v1 = VLSEV_FLOAT(&x[ix + 1], inc_x2 * sizeof(FLOAT), 2);
                        temp_rv = VFMUL_VF_FLOAT(x_v0, alpha_r, 2);
                        temp_rv = VFMACCVF_FLOAT(temp_rv, alpha_i, x_v1, 2);


                        // temp_ii[0] = alpha_r * x[ix + 1] - alpha_i * x[ix]; 
                        // temp_ii[1] = alpha_r * x[ix + inc_x2 + 1] - alpha_i * x[ix + inc_x2];
                        temp_iv = VFMUL_VF_FLOAT(x_v1, alpha_r, 2);
                        temp_iv = VFNMSACVF_FLOAT(temp_iv, alpha_i, x_v0, 2);
                        VSEV_FLOAT(&temp_rr[0], temp_rv, 2);
                        VSEV_FLOAT(&temp_ii[0], temp_iv, 2);

                        
                        // temp_rr[2] = alpha_r * x[ix + inc_x2 * 2] + alpha_i * x[ix + inc_x2 * 2 + 1];
                        // temp_rr[3] = alpha_r * x[ix + inc_x2 * 3] + alpha_i * x[ix + inc_x2 * 3 + 1];
                        x_v0 = VLSEV_FLOAT(&x[ix + inc_x2 * 2], inc_x2 * sizeof(FLOAT), 2);
                        x_v1 = VLSEV_FLOAT(&x[ix + inc_x2 * 2 + 1], inc_x2 * sizeof(FLOAT), 2);
                        temp_rv = VFMUL_VF_FLOAT(x_v0, alpha_r, 2);
                        temp_rv = VFMACCVF_FLOAT(temp_rv, alpha_i, x_v1, 2);


                        temp_ii[2] = alpha_r * x[ix + inc_x2 * 2 + 1] - alpha_i * x[ix + inc_x2 * 2];
                        temp_ii[3] = alpha_r * x[ix + inc_x2 * 3 + 1] - alpha_i * x[ix + inc_x2 * 3];
                        temp_iv = VFMUL_VF_FLOAT(x_v1, alpha_r, 2);
                        temp_iv = VFNMSACVF_FLOAT(temp_iv, alpha_i, x_v0, 2);
                        VSEV_FLOAT(&temp_rr[2], temp_rv, 2);
                        VSEV_FLOAT(&temp_ii[2], temp_iv, 2);



#endif

                        va0 = VLSEV_FLOAT(&a_ptr[j], stride_a, gvl);
                        va1 = VLSEV_FLOAT(&a_ptr[j + 1], stride_a, gvl);
                        va2 = VLSEV_FLOAT(&a_ptr[j + lda2], stride_a, gvl);
                        va3 = VLSEV_FLOAT(&a_ptr[j + lda2 + 1], stride_a, gvl);
                        va4 = VLSEV_FLOAT(&a_ptr[j + lda2 * 2], stride_a, gvl);
                        va5 = VLSEV_FLOAT(&a_ptr[j + lda2 * 2 + 1], stride_a, gvl);
                        va6 = VLSEV_FLOAT(&a_ptr[j + lda2 * 3], stride_a, gvl);
                        va7 = VLSEV_FLOAT(&a_ptr[j + lda2 * 3 + 1], stride_a, gvl);

#if !defined(CONJ)
#if !defined(XCONJ)
                        vy0 = VFMACCVF_FLOAT(vy0, temp_rr[0], va0, gvl);
                        vy0 = VFNMSACVF_FLOAT(vy0, temp_ii[0], va1, gvl);
                        vy1 = VFMACCVF_FLOAT(vy1, temp_rr[0], va1, gvl);
                        vy1 = VFMACCVF_FLOAT(vy1, temp_ii[0], va0, gvl);

                        vy0 = VFMACCVF_FLOAT(vy0, temp_rr[1], va2, gvl);
                        vy0 = VFNMSACVF_FLOAT(vy0, temp_ii[1], va3, gvl);
                        vy1 = VFMACCVF_FLOAT(vy1, temp_rr[1], va3, gvl);
                        vy1 = VFMACCVF_FLOAT(vy1, temp_ii[1], va2, gvl);

                        vy0 = VFMACCVF_FLOAT(vy0, temp_rr[2], va4, gvl);
                        vy0 = VFNMSACVF_FLOAT(vy0, temp_ii[2], va5, gvl);
                        vy1 = VFMACCVF_FLOAT(vy1, temp_rr[2], va5, gvl);
                        vy1 = VFMACCVF_FLOAT(vy1, temp_ii[2], va4, gvl);

                        vy0 = VFMACCVF_FLOAT(vy0, temp_rr[3], va6, gvl);
                        vy0 = VFNMSACVF_FLOAT(vy0, temp_ii[3], va7, gvl);
                        vy1 = VFMACCVF_FLOAT(vy1, temp_rr[3], va7, gvl);
                        vy1 = VFMACCVF_FLOAT(vy1, temp_ii[3], va6, gvl);

#else

                        vy0 = VFMACCVF_FLOAT(vy0, temp_rr[0], va0, gvl);
                        vy0 = VFMACCVF_FLOAT(vy0, temp_ii[0], va1, gvl);
                        vy1 = VFMACCVF_FLOAT(vy1, temp_rr[0], va1, gvl);
                        vy1 = VFNMSACVF_FLOAT(vy1, temp_ii[0], va0, gvl);

                        vy0 = VFMACCVF_FLOAT(vy0, temp_rr[1], va2, gvl);
                        vy0 = VFMACCVF_FLOAT(vy0, temp_ii[1], va3, gvl);
                        vy1 = VFMACCVF_FLOAT(vy1, temp_rr[1], va3, gvl);
                        vy1 = VFNMSACVF_FLOAT(vy1, temp_ii[1], va2, gvl);

                        vy0 = VFMACCVF_FLOAT(vy0, temp_rr[2], va4, gvl);
                        vy0 = VFMACCVF_FLOAT(vy0, temp_ii[2], va5, gvl);
                        vy1 = VFMACCVF_FLOAT(vy1, temp_rr[2], va5, gvl);
                        vy1 = VFNMSACVF_FLOAT(vy1, temp_ii[2], va4, gvl);

                        vy0 = VFMACCVF_FLOAT(vy0, temp_rr[3], va6, gvl);
                        vy0 = VFMACCVF_FLOAT(vy0, temp_ii[3], va7, gvl);
                        vy1 = VFMACCVF_FLOAT(vy1, temp_rr[3], va7, gvl);
                        vy1 = VFNMSACVF_FLOAT(vy1, temp_ii[3], va6, gvl);

#endif

#else

#if !defined(XCONJ)
                        vy0 = VFMACCVF_FLOAT(vy0, temp_rr[0], va0, gvl);
                        vy0 = VFMACCVF_FLOAT(vy0, temp_ii[0], va1, gvl);
                        vy1 = VFNMSACVF_FLOAT(vy1, temp_rr[0], va1, gvl);
                        vy1 = VFMACCVF_FLOAT(vy1, temp_ii[0], va0, gvl);

                        vy0 = VFMACCVF_FLOAT(vy0, temp_rr[1], va2, gvl);
                        vy0 = VFMACCVF_FLOAT(vy0, temp_ii[1], va3, gvl);
                        vy1 = VFNMSACVF_FLOAT(vy1, temp_rr[1], va3, gvl);
                        vy1 = VFMACCVF_FLOAT(vy1, temp_ii[1], va2, gvl);

                        vy0 = VFMACCVF_FLOAT(vy0, temp_rr[2], va4, gvl);
                        vy0 = VFMACCVF_FLOAT(vy0, temp_ii[2], va5, gvl);
                        vy1 = VFNMSACVF_FLOAT(vy1, temp_rr[2], va5, gvl);
                        vy1 = VFMACCVF_FLOAT(vy1, temp_ii[2], va4, gvl);

                        vy0 = VFMACCVF_FLOAT(vy0, temp_rr[3], va6, gvl);
                        vy0 = VFMACCVF_FLOAT(vy0, temp_ii[3], va7, gvl);
                        vy1 = VFNMSACVF_FLOAT(vy1, temp_rr[3], va7, gvl);
                        vy1 = VFMACCVF_FLOAT(vy1, temp_ii[3], va6, gvl);

#else
                        vy0 = VFMACCVF_FLOAT(vy0, temp_rr[0], va0, gvl);
                        vy0 = VFNMSACVF_FLOAT(vy0, temp_ii[0], va1, gvl);
                        vy1 = VFNMSACVF_FLOAT(vy1, temp_rr[0], va1, gvl);
                        vy1 = VFNMSACVF_FLOAT(vy1, temp_ii[0], va0, gvl);

                        vy0 = VFMACCVF_FLOAT(vy0, temp_rr[1], va2, gvl);
                        vy0 = VFNMSACVF_FLOAT(vy0, temp_ii[1], va3, gvl);
                        vy1 = VFNMSACVF_FLOAT(vy1, temp_rr[1], va3, gvl);
                        vy1 = VFNMSACVF_FLOAT(vy1, temp_ii[1], va2, gvl);

                        vy0 = VFMACCVF_FLOAT(vy0, temp_rr[2], va4, gvl);
                        vy0 = VFNMSACVF_FLOAT(vy0, temp_ii[2], va5, gvl);
                        vy1 = VFNMSACVF_FLOAT(vy1, temp_rr[2], va5, gvl);
                        vy1 = VFNMSACVF_FLOAT(vy1, temp_ii[2], va4, gvl);

                        vy0 = VFMACCVF_FLOAT(vy0, temp_rr[3], va6, gvl);
                        vy0 = VFNMSACVF_FLOAT(vy0, temp_ii[3], va7, gvl);
                        vy1 = VFNMSACVF_FLOAT(vy1, temp_rr[3], va7, gvl);
                        vy1 = VFNMSACVF_FLOAT(vy1, temp_ii[3], va6, gvl);

#endif

#endif
                        a_ptr += lda2 * 4;
                        ix += inc_x2 * 4;
                }

                VSSEV_FLOAT(&y[iy], stride_y, vy0, gvl);
                VSSEV_FLOAT(&y[iy + 1], stride_y, vy1, gvl);
                j += gvl * 2;
                iy += inc_yv  ;
        }
        // tail
        if (j / 2 < m)
        {
                gvl = VSETVL(m - j / 2);
                a_ptr = a;
                ix = 0;
                vy0 = VLSEV_FLOAT(&y[iy], stride_y, gvl);
                vy1 = VLSEV_FLOAT(&y[iy + 1], stride_y, gvl);
                for (i = 0; i < n; i++)
                {
#if !defined(XCONJ)
                        temp_r = alpha_r * x[ix] - alpha_i * x[ix + 1];
                        temp_i = alpha_r * x[ix + 1] + alpha_i * x[ix];
#else
                        temp_r = alpha_r * x[ix] + alpha_i * x[ix + 1];
                        temp_i = alpha_r * x[ix + 1] - alpha_i * x[ix];
#endif

                        va0 = VLSEV_FLOAT(&a_ptr[j], stride_a, gvl);
                        va1 = VLSEV_FLOAT(&a_ptr[j + 1], stride_a, gvl);
#if !defined(CONJ)

#if !defined(XCONJ)
                        vy0 = VFMACCVF_FLOAT(vy0, temp_r, va0, gvl);
                        vy0 = VFNMSACVF_FLOAT(vy0, temp_i, va1, gvl);
                        vy1 = VFMACCVF_FLOAT(vy1, temp_r, va1, gvl);
                        vy1 = VFMACCVF_FLOAT(vy1, temp_i, va0, gvl);
#else

                        vy0 = VFMACCVF_FLOAT(vy0, temp_r, va0, gvl);
                        vy0 = VFMACCVF_FLOAT(vy0, temp_i, va1, gvl);
                        vy1 = VFMACCVF_FLOAT(vy1, temp_r, va1, gvl);
                        vy1 = VFNMSACVF_FLOAT(vy1, temp_i, va0, gvl);
#endif

#else

#if !defined(XCONJ)
                        vy0 = VFMACCVF_FLOAT(vy0, temp_r, va0, gvl);
                        vy0 = VFMACCVF_FLOAT(vy0, temp_i, va1, gvl);
                        vy1 = VFNMSACVF_FLOAT(vy1, temp_r, va1, gvl);
                        vy1 = VFMACCVF_FLOAT(vy1, temp_i, va0, gvl);
#else
                        vy0 = VFMACCVF_FLOAT(vy0, temp_r, va0, gvl);
                        vy0 = VFNMSACVF_FLOAT(vy0, temp_i, va1, gvl);
                        vy1 = VFNMSACVF_FLOAT(vy1, temp_r, va1, gvl);
                        vy1 = VFNMSACVF_FLOAT(vy1, temp_i, va0, gvl);
#endif

#endif
                        a_ptr += lda2;
                        ix += inc_x2;
                }
                VSSEV_FLOAT(&y[iy], stride_y, vy0, gvl);
                VSSEV_FLOAT(&y[iy + 1], stride_y, vy1, gvl);
        }
        return (0);
}

