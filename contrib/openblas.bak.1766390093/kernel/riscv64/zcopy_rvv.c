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
#define VSETVL_M8(n)            __riscv_vsetvl_e32m8(n)
#define FLOAT_V_T_M8            vfloat32m8_t
#define VLEV_FLOAT_M8           __riscv_vle32_v_f32m8
#define VSEV_FLOAT_M8           __riscv_vse32_v_f32m8

#define VSETVL_M4(n)            __riscv_vsetvl_e32m4(n)
#define FLOAT_VX2_T_M4          vfloat32m4x2_t
#define VLSEG_FLOAT_M4          __riscv_vlseg2e32_v_f32m4x2
#define VSSEG_FLOAT_M4          __riscv_vsseg2e32_v_f32m4x2
#define VLSSEG_FLOAT_M4         __riscv_vlsseg2e32_v_f32m4x2
#define VSSSEG_FLOAT_M4         __riscv_vssseg2e32_v_f32m4x2
#else
#define VSETVL_M8(n)            __riscv_vsetvl_e64m8(n)
#define FLOAT_V_T_M8            vfloat64m8_t
#define VLEV_FLOAT_M8           __riscv_vle64_v_f64m8
#define VSEV_FLOAT_M8           __riscv_vse64_v_f64m8

#define VSETVL_M4(n)            __riscv_vsetvl_e64m4(n)
#define FLOAT_VX2_T_M4          vfloat64m4x2_t
#define VLSEG_FLOAT_M4          __riscv_vlseg2e64_v_f64m4x2
#define VSSEG_FLOAT_M4          __riscv_vsseg2e64_v_f64m4x2
#define VLSSEG_FLOAT_M4         __riscv_vlsseg2e64_v_f64m4x2
#define VSSSEG_FLOAT_M4         __riscv_vssseg2e64_v_f64m4x2
#endif

int CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
{
    if(n <= 0) return(0);

    if(inc_x == 1 && inc_y == 1) {

        FLOAT_V_T_M8 vx;
        n *= 2; // convert to words

        for(size_t vl; n > 0; n -= vl, x += vl, y += vl) {
            vl = VSETVL_M8(n);
            vx = VLEV_FLOAT_M8(x, vl);
            VSEV_FLOAT_M8(y, vx, vl);
        }

    }else if (1 == inc_x) {

        FLOAT_VX2_T_M4 vx2;
        BLASLONG stride_y = inc_y * 2 * sizeof(FLOAT);

        for(size_t vl; n > 0; n -= vl, x += vl*2, y += vl*inc_y*2) {
            vl = VSETVL_M4(n);
            vx2 = VLSEG_FLOAT_M4(x, vl);
            VSSSEG_FLOAT_M4(y, stride_y, vx2, vl);
        }
    } else if (1 == inc_y) {

        FLOAT_VX2_T_M4 vx2;
        BLASLONG stride_x = inc_x * 2 * sizeof(FLOAT);

        for(size_t vl; n > 0; n -= vl, x += vl*inc_x*2, y += vl*2) {
            vl = VSETVL_M4(n);
            vx2 = VLSSEG_FLOAT_M4(x, stride_x, vl);
            VSSEG_FLOAT_M4(y, vx2, vl);
        }
    } else {

        FLOAT_VX2_T_M4 vx2;
        BLASLONG stride_x = inc_x * 2 * sizeof(FLOAT);
        BLASLONG stride_y = inc_y * 2 * sizeof(FLOAT);

        for(size_t vl; n > 0; n -= vl, x += vl*inc_x*2, y += vl*inc_y*2) {
            vl = VSETVL_M4(n);
            vx2 = VLSSEG_FLOAT_M4(x, stride_x, vl);
            VSSSEG_FLOAT_M4(y, stride_y, vx2, vl);
        }
    }

    return(0);
}
