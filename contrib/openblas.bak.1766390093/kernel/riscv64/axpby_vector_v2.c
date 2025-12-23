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
#define VSETVL(n) RISCV_RVV(vsetvl_e32m8)(n)
#define FLOAT_V_T vfloat32m8_t
#define VLEV_FLOAT RISCV_RVV(vle32_v_f32m8)
#define VLSEV_FLOAT RISCV_RVV(vlse32_v_f32m8)
#define VSEV_FLOAT RISCV_RVV(vse32_v_f32m8)
#define VSSEV_FLOAT RISCV_RVV(vsse32_v_f32m8)
#define VFMACCVF_FLOAT RISCV_RVV(vfmacc_vf_f32m8)
#define VFMULVF_FLOAT RISCV_RVV(vfmul_vf_f32m8)
#define VFMVVF_FLOAT RISCV_RVV(vfmv_v_f_f32m8)
#else
#define VSETVL(n) RISCV_RVV(vsetvl_e64m4)(n)
#define FLOAT_V_T vfloat64m4_t
#define VLEV_FLOAT RISCV_RVV(vle64_v_f64m4)
#define VLSEV_FLOAT RISCV_RVV(vlse64_v_f64m4)
#define VSEV_FLOAT RISCV_RVV(vse64_v_f64m4)
#define VSSEV_FLOAT RISCV_RVV(vsse64_v_f64m4)
#define VFMACCVF_FLOAT RISCV_RVV(vfmacc_vf_f64m4)
#define VFMULVF_FLOAT RISCV_RVV(vfmul_vf_f64m4)
#define VFMVVF_FLOAT RISCV_RVV(vfmv_v_f_f64m4)
#endif

int CNAME(BLASLONG n, FLOAT alpha, FLOAT *x, BLASLONG inc_x, FLOAT beta, FLOAT *y, BLASLONG inc_y)
{
    FLOAT_V_T vx, vy;
    unsigned int gvl;
    if (n <= 0)
        return (0);
    if (inc_x == 1 && inc_y == 1)
    {
        while (n > 0)
        {
            gvl = VSETVL(n);

            vx = VLEV_FLOAT(x, gvl);
            vy = VLEV_FLOAT(y, gvl);

            vy = VFMULVF_FLOAT(vy, beta, gvl);
            vy = VFMACCVF_FLOAT(vy, alpha, vx, gvl);

            VSEV_FLOAT(y, vy, gvl);

            x += gvl;
            y += gvl;
            n -= gvl;
        }
    }
    else if (1 == inc_x)
    {
        BLASLONG stride_y = inc_y * sizeof(FLOAT);
        while (n > 0)
        {
            gvl = VSETVL(n);
            vy = VLSEV_FLOAT(y, stride_y, gvl);
            vx = VLEV_FLOAT(x, gvl);

            vy = VFMULVF_FLOAT(vy, beta, gvl);
            vy = VFMACCVF_FLOAT(vy, alpha, vx, gvl);

            VSSEV_FLOAT(y, stride_y, vy, gvl);

            x += gvl;
            y += gvl * inc_y;
            n -= gvl;
        }
    }
    else if (1 == inc_y)
    {
        BLASLONG stride_x = inc_x * sizeof(FLOAT);

        while (n > 0)
        {
            gvl = VSETVL(n);

            vx = VLSEV_FLOAT(x, stride_x, gvl);
            vy = VLEV_FLOAT(y, gvl);

            vy = VFMULVF_FLOAT(vy, beta, gvl);
            vy = VFMACCVF_FLOAT(vy, alpha, vx, gvl);

            VSEV_FLOAT(y, vy, gvl);

            x += gvl * inc_x;
            y += gvl;
            n -= gvl;
        }
    }
    else if (inc_y == 0)
    {
        FLOAT vf = y[0];
        for (; n > 0; n--)
        {
            vf = (vf * beta) + (x[0] * alpha);
            x += inc_x;
        }
        y[0] = vf;
    }
    else
    {
        BLASLONG stride_x = inc_x * sizeof(FLOAT);
        BLASLONG stride_y = inc_y * sizeof(FLOAT);
        while (n > 0)
        {
            gvl = VSETVL(n);
            vy = VLSEV_FLOAT(y, stride_y, gvl);
            vx = VLSEV_FLOAT(x, stride_x, gvl);

            vy = VFMULVF_FLOAT(vy, beta, gvl);
            vy = VFMACCVF_FLOAT(vy, alpha, vx, gvl);

            VSSEV_FLOAT(y, stride_y, vy, gvl);

            x += gvl * inc_x;
            y += gvl * inc_y;
            n -= gvl;
        }
    }

    return (0);
}
