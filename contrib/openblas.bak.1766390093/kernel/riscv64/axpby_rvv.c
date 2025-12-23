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
#define VSETVL(n)               __riscv_vsetvl_e32m8(n)
#define FLOAT_V_T               vfloat32m8_t
#define VLEV_FLOAT              __riscv_vle32_v_f32m8
#define VLSEV_FLOAT             __riscv_vlse32_v_f32m8
#define VSEV_FLOAT              __riscv_vse32_v_f32m8
#define VSSEV_FLOAT             __riscv_vsse32_v_f32m8
#define VFMACCVF_FLOAT          __riscv_vfmacc_vf_f32m8
#define VFMULVF_FLOAT           __riscv_vfmul_vf_f32m8
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f32m8
#else
#define VSETVL(n)               __riscv_vsetvl_e64m8(n)
#define FLOAT_V_T               vfloat64m8_t
#define VLEV_FLOAT              __riscv_vle64_v_f64m8
#define VLSEV_FLOAT             __riscv_vlse64_v_f64m8
#define VSEV_FLOAT              __riscv_vse64_v_f64m8
#define VSSEV_FLOAT             __riscv_vsse64_v_f64m8
#define VFMACCVF_FLOAT          __riscv_vfmacc_vf_f64m8
#define VFMULVF_FLOAT           __riscv_vfmul_vf_f64m8
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f64m8
#endif

int CNAME(BLASLONG n, FLOAT alpha, FLOAT *x, BLASLONG inc_x, FLOAT beta, FLOAT *y, BLASLONG inc_y)
{
    FLOAT_V_T vx, vy;

    if ( n <= 0     )  return(0);

    if ( beta == 0.0 ) {
        if ( alpha == 0.0 ) {
            if (1 == inc_y) {
                memset(&y[0], 0, n * sizeof(FLOAT));
            } else {
                BLASLONG stride_y = inc_y * sizeof(FLOAT);
                size_t vl = VSETVL(n);
                vy = VFMVVF_FLOAT(0.0, vl);
                for ( ; n > 0; n -= vl, y += vl*inc_y) {
                    vl = VSETVL(n);
                    VSSEV_FLOAT(y, stride_y, vy, vl);
                }
            }

        } else {
            if ((1 == inc_x) && (1 == inc_y)) {
                for (size_t vl; n > 0; n -= vl, x += vl, y += vl) {
                    vl = VSETVL(n);
                    vx = VLEV_FLOAT(x, vl);
                    vy = VFMULVF_FLOAT(vx, alpha, vl);
                    VSEV_FLOAT (y, vy, vl);
                }
            } else if (1 == inc_x) {
                BLASLONG stride_y = inc_y * sizeof(FLOAT);
                for (size_t vl; n > 0; n -= vl, x += vl, y += vl*inc_y) {
                    vl = VSETVL(n);
                    vx = VLEV_FLOAT(x, vl);
                    vy = VFMULVF_FLOAT(vx, alpha, vl);
                    VSSEV_FLOAT (y, stride_y, vy, vl);
                }
            } else if (1 == inc_y) {
                BLASLONG stride_x = inc_x * sizeof(FLOAT);
                for (size_t vl; n > 0; n -= vl, x += vl*inc_x, y += vl) {
                    vl = VSETVL(n);
                    vx = VLSEV_FLOAT(x, stride_x, vl);
                    vy = VFMULVF_FLOAT(vx, alpha, vl);
                    VSEV_FLOAT (y, vy, vl);
                }
            } else {
                BLASLONG stride_x = inc_x * sizeof(FLOAT);
                BLASLONG stride_y = inc_y * sizeof(FLOAT);
                for (size_t vl; n > 0; n -= vl, x += vl*inc_x, y += vl*inc_y) {
                    vl = VSETVL(n);
                    vx = VLSEV_FLOAT(x, stride_x, vl);
                    vy = VFMULVF_FLOAT(vx, alpha, vl);
                    VSSEV_FLOAT (y, stride_y, vy, vl);
                }
            }
        }

    } else {
        if ( alpha == 0.0 ) {
            if (1 == inc_y) {
                for (size_t vl; n > 0; n -= vl, y += vl) {
                    vl = VSETVL(n);
                    vy = VLEV_FLOAT(y, vl);
                    vy = VFMULVF_FLOAT(vy, beta, vl);
                    VSEV_FLOAT (y, vy, vl);
                }
            } else if (inc_y == 0) {
                    FLOAT vf = y[0];
                    for (; n > 0; n--)
                            vf *= beta;
                    y[0] = vf;
            } else {
                BLASLONG stride_y = inc_y * sizeof(FLOAT);
                for (size_t vl; n > 0; n -= vl, y += vl*inc_y) {
                    vl = VSETVL(n);
                    vy = VLSEV_FLOAT(y, stride_y, vl);
                    vy = VFMULVF_FLOAT(vy, beta, vl);
                    VSSEV_FLOAT (y, stride_y, vy, vl);
                }
            }

        } else {
            if ((1 == inc_x) && (1 == inc_y)) {
                for (size_t vl; n > 0; n -= vl, x += vl, y += vl) {
                    vl = VSETVL(n);
                    vx = VLEV_FLOAT(x, vl);
                    vy = VLEV_FLOAT(y, vl);
                    vy = VFMULVF_FLOAT(vy, beta, vl);
                    vy = VFMACCVF_FLOAT(vy, alpha, vx, vl);
                    VSEV_FLOAT (y, vy, vl);
                }
            } else if (inc_y == 0) {
                    FLOAT vf = y[0];
                    for (; n > 0; n--) {
                            vf = (vf * beta) + (x[0] * alpha);
                            x += inc_x;
                    }
                    y[0] = vf;
            } else if (1 == inc_x) {
                BLASLONG stride_y = inc_y * sizeof(FLOAT);
                for (size_t vl; n > 0; n -= vl, x += vl, y += vl*inc_y) {
                    vl = VSETVL(n);
                    vx = VLEV_FLOAT(x, vl);
                    vy = VLSEV_FLOAT(y, stride_y, vl);
                    vy = VFMULVF_FLOAT(vy, beta, vl);
                    vy = VFMACCVF_FLOAT(vy, alpha, vx, vl);
                    VSSEV_FLOAT (y, stride_y, vy, vl);
                }
            } else if (1 == inc_y) {
                BLASLONG stride_x = inc_x * sizeof(FLOAT);
                for (size_t vl; n > 0; n -= vl, x += vl*inc_x, y += vl) {
                    vl = VSETVL(n);
                    vx = VLSEV_FLOAT(x, stride_x, vl);
                    vy = VLEV_FLOAT(y, vl);
                    vy = VFMULVF_FLOAT(vy, beta, vl);
                    vy = VFMACCVF_FLOAT(vy, alpha, vx, vl);
                    VSEV_FLOAT (y, vy, vl);
                }
            } else {
                BLASLONG stride_x = inc_x * sizeof(FLOAT);
                BLASLONG stride_y = inc_y * sizeof(FLOAT);
                for (size_t vl; n > 0; n -= vl, x += vl*inc_x, y += vl*inc_y) {
                    vl = VSETVL(n);
                    vx = VLSEV_FLOAT(x, stride_x, vl);
                    vy = VLSEV_FLOAT(y, stride_y, vl);
                    vy = VFMULVF_FLOAT(vy, beta, vl);
                    vy = VFMACCVF_FLOAT(vy, alpha, vx, vl);
                    VSSEV_FLOAT (y, stride_y, vy, vl);
                }
            }
        }
    }

    return(0);
}
