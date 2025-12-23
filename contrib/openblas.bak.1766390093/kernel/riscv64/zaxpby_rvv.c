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

/***************************************************************************
* 2014/06/07 Saar
*
***************************************************************************/

#include "common.h"

#if !defined(DOUBLE)
#define VSETVL(n)               __riscv_vsetvl_e32m4(n)
#define FLOAT_V_T               vfloat32m4_t
#define FLOAT_VX2_T             vfloat32m4x2_t
#define VGET_VX2                __riscv_vget_v_f32m4x2_f32m4
#define VSET_VX2                __riscv_vset_v_f32m4_f32m4x2
#define VLSEV_FLOAT             __riscv_vlse32_v_f32m4
#define VSSEV_FLOAT             __riscv_vsse32_v_f32m4
#define VFMACCVF_FLOAT          __riscv_vfmacc_vf_f32m4
#define VFNMSACVF_FLOAT         __riscv_vfnmsac_vf_f32m4
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f32m4
#define VFMULVF_FLOAT           __riscv_vfmul_vf_f32m4
#define VFMSACVF_FLOAT          __riscv_vfmsac_vf_f32m4
#define VLSEG_FLOAT             __riscv_vlseg2e32_v_f32m4x2
#define VSSEG_FLOAT             __riscv_vsseg2e32_v_f32m4x2
#define VLSSEG_FLOAT            __riscv_vlsseg2e32_v_f32m4x2
#define VSSSEG_FLOAT            __riscv_vssseg2e32_v_f32m4x2
#else
#define VSETVL(n)               __riscv_vsetvl_e64m4(n)
#define FLOAT_V_T               vfloat64m4_t
#define FLOAT_VX2_T             vfloat64m4x2_t
#define VGET_VX2                __riscv_vget_v_f64m4x2_f64m4
#define VSET_VX2                __riscv_vset_v_f64m4_f64m4x2
#define VLSEV_FLOAT             __riscv_vlse64_v_f64m4
#define VSSEV_FLOAT             __riscv_vsse64_v_f64m4
#define VFMACCVF_FLOAT          __riscv_vfmacc_vf_f64m4
#define VFNMSACVF_FLOAT         __riscv_vfnmsac_vf_f64m4
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f64m4
#define VFMULVF_FLOAT           __riscv_vfmul_vf_f64m4
#define VFMSACVF_FLOAT          __riscv_vfmsac_vf_f64m4
#define VLSEG_FLOAT             __riscv_vlseg2e64_v_f64m4x2
#define VSSEG_FLOAT             __riscv_vsseg2e64_v_f64m4x2
#define VLSSEG_FLOAT            __riscv_vlsseg2e64_v_f64m4x2
#define VSSSEG_FLOAT            __riscv_vssseg2e64_v_f64m4x2
#endif

int CNAME(BLASLONG n, FLOAT alpha_r, FLOAT alpha_i, FLOAT *x, BLASLONG inc_x, FLOAT beta_r, FLOAT beta_i,FLOAT *y, BLASLONG inc_y)
{
    BLASLONG inc_x2, inc_y2;

    if ( n <= 0     )  return(0);

    inc_x2 = 2 * inc_x;
    inc_y2 = 2 * inc_y;
    
    BLASLONG stride_x = inc_x2 * sizeof(FLOAT);
    BLASLONG stride_y = inc_y2 * sizeof(FLOAT);
    BLASLONG ix;
    FLOAT_V_T vx0, vx1, vy0, vy1;
    FLOAT_VX2_T vxx2, vyx2;
    FLOAT temp;

    if ( beta_r == 0.0 && beta_i == 0.0)
    {
        if ( alpha_r == 0.0 && alpha_i == 0.0 )
        {
            size_t vl = VSETVL(n);
            FLOAT_V_T temp = VFMVVF_FLOAT(0.0, vl);
            vxx2 = VSET_VX2(vxx2, 0, temp);
            vxx2 = VSET_VX2(vxx2, 1, temp);
            for ( ; n > 0; n -= vl, y += vl*inc_y2)
            {
                vl = VSETVL(n);
                VSSSEG_FLOAT(y, stride_y, vxx2, vl);
            }
        }
        else
        {
            for (size_t vl; n > 0; n -= vl, x += vl*inc_x2, y += vl*inc_y2) 
            {
                vl = VSETVL(n);

                vxx2 = VLSSEG_FLOAT(x, stride_x, vl);
                vx0 = VGET_VX2(vxx2, 0);
                vx1 = VGET_VX2(vxx2, 1);
                
                vy0 = VFMULVF_FLOAT(vx1, alpha_i, vl);
                vy0 = VFMSACVF_FLOAT(vy0, alpha_r, vx0, vl);

                vy1 = VFMULVF_FLOAT(vx1, alpha_r, vl);
                vy1 = VFMACCVF_FLOAT(vy1, alpha_i, vx0, vl);

                vyx2 = VSET_VX2(vyx2, 0, vy0);
                vyx2 = VSET_VX2(vyx2, 1, vy1);
                VSSSEG_FLOAT(y, stride_y, vyx2, vl);
            }
        }
    }
    else
    {
        FLOAT_V_T v0, v1;
        FLOAT_VX2_T v_x2;

        if ( alpha_r == 0.0 && alpha_i == 0.0 )
        {
            if ( inc_y == 0 ) {
                for (; n > 0; n--)
                {
                    temp = (beta_r * y[0] - beta_i * y[1]);
                    y[1] = (beta_r * y[1] + beta_i * y[0]);
                    y[0] = temp;
                }
            } else {
                for (size_t vl; n > 0; n -= vl, y += vl*inc_y2)
                {
                    vl = VSETVL(n);

                    vyx2 = VLSSEG_FLOAT(y, stride_y, vl);
                    vy0 = VGET_VX2(vyx2, 0);
                    vy1 = VGET_VX2(vyx2, 1);

                    v0 = VFMULVF_FLOAT(vy1, beta_i, vl);
                    v0 = VFMSACVF_FLOAT(v0, beta_r, vy0, vl);

                    v1 = VFMULVF_FLOAT(vy1, beta_r, vl);
                    v1 = VFMACCVF_FLOAT(v1, beta_i, vy0, vl);

                    v_x2 = VSET_VX2(v_x2, 0, v0);
                    v_x2 = VSET_VX2(v_x2, 1, v1);
                    VSSSEG_FLOAT(y, stride_y, v_x2, vl);
                }
            }
        }
        else
        {
            if ( inc_y == 0 ) {
                ix = 0;
                for (; n > 0; n--) {
                    temp = (alpha_r * x[ix] - alpha_i * x[ix+1] ) +
                        (beta_r * y[0] - beta_i * y[1]);
                    y[1] = (alpha_r * x[ix+1] + alpha_i * x[ix]) +
                        (beta_r * y[1] + beta_i * y[0]);
                    y[0] = temp;
                    ix += inc_x2;
                }
            } else {
                for (size_t vl; n > 0; n -= vl, x += vl*inc_x2, y += vl*inc_y2)
                {
                    vl = VSETVL(n);

                    vxx2 = VLSSEG_FLOAT(x, stride_x, vl);
                    vyx2 = VLSSEG_FLOAT(y, stride_y, vl);

                    vx0 = VGET_VX2(vxx2, 0);
                    vx1 = VGET_VX2(vxx2, 1);
                    vy0 = VGET_VX2(vyx2, 0);
                    vy1 = VGET_VX2(vyx2, 1);

                    v0 = VFMULVF_FLOAT(vx0, alpha_r, vl);
                    v0 = VFNMSACVF_FLOAT(v0, alpha_i, vx1, vl);
                    v0 = VFMACCVF_FLOAT(v0, beta_r, vy0, vl);
                    v0 = VFNMSACVF_FLOAT(v0, beta_i, vy1, vl);

                    v1 = VFMULVF_FLOAT(vx1, alpha_r, vl);
                    v1 = VFMACCVF_FLOAT(v1, alpha_i, vx0, vl);
                    v1 = VFMACCVF_FLOAT(v1, beta_r, vy1, vl);
                    v1 = VFMACCVF_FLOAT(v1, beta_i, vy0, vl);

                    v_x2 = VSET_VX2(v_x2, 0, v0);
                    v_x2 = VSET_VX2(v_x2, 1, v1);

                    VSSSEG_FLOAT(y, stride_y, v_x2, vl);
                }
            }
        }
    }
    return(0);

}
