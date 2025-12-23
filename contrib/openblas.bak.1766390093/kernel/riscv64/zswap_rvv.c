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
#define VSETVL(n)               __riscv_vsetvl_e32m4(n)
#define FLOAT_VX2_T             vfloat32m4x2_t
#define VLSEG_FLOAT             __riscv_vlseg2e32_v_f32m4x2
#define VLSSEG_FLOAT            __riscv_vlsseg2e32_v_f32m4x2
#define VSSEG_FLOAT             __riscv_vsseg2e32_v_f32m4x2
#define VSSSEG_FLOAT            __riscv_vssseg2e32_v_f32m4x2
#else
#define VSETVL(n)               __riscv_vsetvl_e64m4(n)
#define FLOAT_VX2_T             vfloat64m4x2_t
#define VLSEG_FLOAT             __riscv_vlseg2e64_v_f64m4x2
#define VLSSEG_FLOAT            __riscv_vlsseg2e64_v_f64m4x2
#define VSSEG_FLOAT             __riscv_vsseg2e64_v_f64m4x2
#define VSSSEG_FLOAT            __riscv_vssseg2e64_v_f64m4x2
#endif

int CNAME(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT dummy3, FLOAT dummy4, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *dummy, BLASLONG dummy2)
{

    if (n <= 0) return(0);

    FLOAT_VX2_T vxx2, vyx2;

    if (inc_x == 0 && inc_y == 0) {
        if (n & 1) {
            FLOAT temp[2];
            temp[0] = x[0];
            temp[1] = x[1];
            x[0] = y[0];
            x[1] = y[1];
            y[0] = temp[0];
            y[1] = temp[1];
        }
        else {
            return 0;
        }
    }
    else if(inc_x == 0) {
        FLOAT temp[2];
        temp[0] = x[0];
        temp[1] = x[1];
        x[0] = y[(n - 1) * inc_y * 2];
        x[0] = y[(n - 1) * inc_y * 2 + 1];
        FLOAT* ptr = y + (n - 1) * inc_y * 2;   // start from the last one
        BLASLONG stride_y = (0 - inc_y) * sizeof(FLOAT) * 2; // reverse
        BLASLONG m = n - 1;
        for (size_t vl; m > 0; m -= vl * 2, ptr -= vl*inc_y * 2) {
            vl = VSETVL(m);
            vyx2 = VLSSEG_FLOAT(ptr - 2, stride_y, vl);
            VSSSEG_FLOAT(ptr, stride_y, vyx2, vl);
        }
        y[0] = temp[0];
        y[1] = temp[1];
    }
    else if(inc_y == 0) {
        FLOAT temp[2];
        temp[0] = y[0];
        temp[1] = y[1];
        y[0] = x[(n - 1) * inc_x * 2];
        y[0] = x[(n - 1) * inc_x * 2 + 1];
        FLOAT* ptr = x + (n - 1) * inc_x * 2;   // start from the last one
        BLASLONG stride_x = (0 - inc_x) * sizeof(FLOAT) * 2; // reverse
        BLASLONG m = n - 1;
        for (size_t vl; m > 0; m -= vl * 2, ptr -= vl*inc_x * 2) {
            vl = VSETVL(m);
            vxx2 = VLSSEG_FLOAT(ptr - 2, stride_x, vl);
            VSSSEG_FLOAT(ptr, stride_x, vxx2, vl);
        }
        x[0] = temp[0];
        x[1] = temp[1];
    }
    else if(inc_x == 1 && inc_y == 1) {

        for (size_t vl; n > 0; n -= vl, x += vl*2, y += vl*2) {
            vl = VSETVL(n);

            vxx2 = VLSEG_FLOAT(x, vl);
            vyx2 = VLSEG_FLOAT(y, vl);

            VSSEG_FLOAT(y, vxx2, vl);
            VSSEG_FLOAT(x, vyx2, vl);
        }

    } else if (inc_x == 1){
        BLASLONG stride_y = inc_y * 2 * sizeof(FLOAT);

        for (size_t vl; n > 0; n -= vl, x += vl*2, y += vl*inc_y*2) {
            vl = VSETVL(n);

            vxx2 = VLSEG_FLOAT(x, vl);
            vyx2 = VLSSEG_FLOAT(y, stride_y, vl);

            VSSSEG_FLOAT(y, stride_y, vxx2, vl);
            VSSEG_FLOAT(x, vyx2, vl);
        }

    } else if (inc_y == 1){
        BLASLONG stride_x = inc_x * 2 * sizeof(FLOAT);

        for (size_t vl; n > 0; n -= vl, x += vl*inc_x*2, y += vl*2) {
            vl = VSETVL(n);

            vxx2 = VLSSEG_FLOAT(x, stride_x, vl);
            vyx2 = VLSEG_FLOAT(y, vl);

            VSSEG_FLOAT(y, vxx2, vl);
            VSSSEG_FLOAT(x, stride_x, vyx2, vl);
        }

    } else {
        BLASLONG stride_x = inc_x * 2 * sizeof(FLOAT);
        BLASLONG stride_y = inc_y * 2 * sizeof(FLOAT);

        for (size_t vl; n > 0; n -= vl, x += vl*inc_x*2, y += vl*inc_y*2) {
            vl = VSETVL(n);

            vxx2 = VLSSEG_FLOAT(x, stride_x, vl);
            vyx2 = VLSSEG_FLOAT(y, stride_y, vl);

            VSSSEG_FLOAT(y, stride_y, vxx2, vl);
            VSSSEG_FLOAT(x, stride_x, vyx2, vl);
        }

    }

    return(0);
}
