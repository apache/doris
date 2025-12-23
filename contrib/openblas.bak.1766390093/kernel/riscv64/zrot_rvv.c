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
#define FLOAT_V_T               vfloat32m4_t
#define FLOAT_VX2_T             vfloat32m4x2_t
#define VGET_VX2                __riscv_vget_v_f32m4x2_f32m4
#define VSET_VX2                __riscv_vset_v_f32m4_f32m4x2
#define VLEV_FLOAT              __riscv_vle32_v_f32m4
#define VLSEV_FLOAT             __riscv_vlse32_v_f32m4
#define VSEV_FLOAT              __riscv_vse32_v_f32m4
#define VSSEV_FLOAT             __riscv_vsse32_v_f32m4
#define VLSEG_FLOAT             __riscv_vlseg2e32_v_f32m4x2
#define VSSEG_FLOAT             __riscv_vsseg2e32_v_f32m4x2
#define VLSSEG_FLOAT            __riscv_vlsseg2e32_v_f32m4x2
#define VSSSEG_FLOAT            __riscv_vssseg2e32_v_f32m4x2
#define VFMACCVF_FLOAT          __riscv_vfmacc_vf_f32m4
#define VFMULVF_FLOAT           __riscv_vfmul_vf_f32m4
#define VFNMSACVF_FLOAT         __riscv_vfnmsac_vf_f32m4
#else
#define VSETVL(n)               __riscv_vsetvl_e64m4(n)
#define FLOAT_V_T               vfloat64m4_t
#define FLOAT_VX2_T             vfloat64m4x2_t
#define VGET_VX2                __riscv_vget_v_f64m4x2_f64m4
#define VSET_VX2                __riscv_vset_v_f64m4_f64m4x2
#define VLEV_FLOAT              __riscv_vle64_v_f64m4
#define VLSEV_FLOAT             __riscv_vlse64_v_f64m4
#define VSEV_FLOAT              __riscv_vse64_v_f64m4
#define VSSEV_FLOAT             __riscv_vsse64_v_f64m4
#define VLSEG_FLOAT             __riscv_vlseg2e64_v_f64m4x2
#define VSSEG_FLOAT             __riscv_vsseg2e64_v_f64m4x2
#define VLSSEG_FLOAT            __riscv_vlsseg2e64_v_f64m4x2
#define VSSSEG_FLOAT            __riscv_vssseg2e64_v_f64m4x2
#define VFMACCVF_FLOAT          __riscv_vfmacc_vf_f64m4
#define VFMULVF_FLOAT           __riscv_vfmul_vf_f64m4
#define VFNMSACVF_FLOAT         __riscv_vfnmsac_vf_f64m4
#endif

int CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT c, FLOAT s)
{

    if (n <= 0) return(0);

    FLOAT_V_T vt0, vt1, vx0, vx1, vy0, vy1;
    FLOAT_VX2_T vxx2, vyx2, vtx2;

    if (inc_x == 0 && inc_y == 0) {
        BLASLONG i=0;
        BLASLONG ix=0,iy=0;
        FLOAT temp[2];
        BLASLONG inc_x2;
        BLASLONG inc_y2;

        inc_x2 = 2 * inc_x ;
        inc_y2 = 2 * inc_y ;

        while(i < n)
        {
            temp[0]   = c*x[ix]   + s*y[iy] ;
            temp[1]   = c*x[ix+1] + s*y[iy+1] ;
            y[iy]     = c*y[iy]   - s*x[ix] ;
            y[iy+1]   = c*y[iy+1] - s*x[ix+1] ;
            x[ix]     = temp[0] ;
            x[ix+1]   = temp[1] ;

            ix += inc_x2 ;
            iy += inc_y2 ;
            i++ ;
        }
    }
    else if(inc_x == 1 && inc_y == 1) {

        for (size_t vl; n > 0; n -= vl, x += vl*2, y += vl*2) {
            vl = VSETVL(n);

            vxx2 = VLSEG_FLOAT(x, vl);
            vyx2 = VLSEG_FLOAT(y, vl);

            vx0 = VGET_VX2(vxx2, 0);
            vx1 = VGET_VX2(vxx2, 1);
            vy0 = VGET_VX2(vyx2, 0);
            vy1 = VGET_VX2(vyx2, 1);

            vt0 = VFMULVF_FLOAT(vx0, c, vl);
            vt0 = VFMACCVF_FLOAT(vt0, s, vy0, vl);
            vt1 = VFMULVF_FLOAT(vx1, c, vl);
            vt1 = VFMACCVF_FLOAT(vt1, s, vy1, vl);
            vy0 = VFMULVF_FLOAT(vy0, c, vl);
            vy0 = VFNMSACVF_FLOAT(vy0, s, vx0, vl);
            vy1 = VFMULVF_FLOAT(vy1, c, vl);
            vy1 = VFNMSACVF_FLOAT(vy1, s, vx1, vl);

            vtx2 = VSET_VX2(vtx2, 0, vt0);
            vtx2 = VSET_VX2(vtx2, 1, vt1);
            vyx2 = VSET_VX2(vyx2, 0, vy0);
            vyx2 = VSET_VX2(vyx2, 1, vy1);

            VSSEG_FLOAT(x, vtx2, vl);
            VSSEG_FLOAT(y, vyx2, vl);
        }

    } else if (inc_x == 1){
        BLASLONG stride_y = inc_y * 2 * sizeof(FLOAT);

        for (size_t vl; n > 0; n -= vl, x += vl*2, y += vl*inc_y*2) {
            vl = VSETVL(n);

            vxx2 = VLSEG_FLOAT(x, vl);
            vyx2 = VLSSEG_FLOAT(y, stride_y, vl);

            vx0 = VGET_VX2(vxx2, 0);
            vx1 = VGET_VX2(vxx2, 1);
            vy0 = VGET_VX2(vyx2, 0);
            vy1 = VGET_VX2(vyx2, 1);

            vt0 = VFMULVF_FLOAT(vx0, c, vl);
            vt0 = VFMACCVF_FLOAT(vt0, s, vy0, vl);
            vt1 = VFMULVF_FLOAT(vx1, c, vl);
            vt1 = VFMACCVF_FLOAT(vt1, s, vy1, vl);
            vy0 = VFMULVF_FLOAT(vy0, c, vl);
            vy0 = VFNMSACVF_FLOAT(vy0, s, vx0, vl);
            vy1 = VFMULVF_FLOAT(vy1, c, vl);
            vy1 = VFNMSACVF_FLOAT(vy1, s, vx1, vl);

            vtx2 = VSET_VX2(vtx2, 0, vt0);
            vtx2 = VSET_VX2(vtx2, 1, vt1);
            vyx2 = VSET_VX2(vyx2, 0, vy0);
            vyx2 = VSET_VX2(vyx2, 1, vy1);

            VSSEG_FLOAT(x, vtx2, vl);
            VSSSEG_FLOAT(y, stride_y, vyx2, vl);
        }

    } else if (inc_y == 1){
        BLASLONG stride_x = inc_x * 2 * sizeof(FLOAT);

        for (size_t vl; n > 0; n -= vl, x += vl*inc_x*2, y += vl*2) {
            vl = VSETVL(n);

            vxx2 = VLSSEG_FLOAT(x, stride_x, vl);
            vyx2 = VLSEG_FLOAT(y, vl);

            vx0 = VGET_VX2(vxx2, 0);
            vx1 = VGET_VX2(vxx2, 1);
            vy0 = VGET_VX2(vyx2, 0);
            vy1 = VGET_VX2(vyx2, 1);

            vt0 = VFMULVF_FLOAT(vx0, c, vl);
            vt0 = VFMACCVF_FLOAT(vt0, s, vy0, vl);
            vt1 = VFMULVF_FLOAT(vx1, c, vl);
            vt1 = VFMACCVF_FLOAT(vt1, s, vy1, vl);
            vy0 = VFMULVF_FLOAT(vy0, c, vl);
            vy0 = VFNMSACVF_FLOAT(vy0, s, vx0, vl);
            vy1 = VFMULVF_FLOAT(vy1, c, vl);
            vy1 = VFNMSACVF_FLOAT(vy1, s, vx1, vl);

            vtx2 = VSET_VX2(vtx2, 0, vt0);
            vtx2 = VSET_VX2(vtx2, 1, vt1);
            vyx2 = VSET_VX2(vyx2, 0, vy0);
            vyx2 = VSET_VX2(vyx2, 1, vy1);

            VSSSEG_FLOAT(x, stride_x, vtx2, vl);
            VSSEG_FLOAT(y, vyx2, vl);
        }

    } else {
        BLASLONG stride_x = inc_x * 2 * sizeof(FLOAT);
        BLASLONG stride_y = inc_y * 2 * sizeof(FLOAT);

        for (size_t vl; n > 0; n -= vl, x += vl*inc_x*2, y += vl*inc_y*2) {
            vl = VSETVL(n);

            vxx2 = VLSSEG_FLOAT(x, stride_x, vl);
            vyx2 = VLSSEG_FLOAT(y, stride_y, vl);

            vx0 = VGET_VX2(vxx2, 0);
            vx1 = VGET_VX2(vxx2, 1);
            vy0 = VGET_VX2(vyx2, 0);
            vy1 = VGET_VX2(vyx2, 1);

            vt0 = VFMULVF_FLOAT(vx0, c, vl);
            vt0 = VFMACCVF_FLOAT(vt0, s, vy0, vl);
            vt1 = VFMULVF_FLOAT(vx1, c, vl);
            vt1 = VFMACCVF_FLOAT(vt1, s, vy1, vl);
            vy0 = VFMULVF_FLOAT(vy0, c, vl);
            vy0 = VFNMSACVF_FLOAT(vy0, s, vx0, vl);
            vy1 = VFMULVF_FLOAT(vy1, c, vl);
            vy1 = VFNMSACVF_FLOAT(vy1, s, vx1, vl);

            vtx2 = VSET_VX2(vtx2, 0, vt0);
            vtx2 = VSET_VX2(vtx2, 1, vt1);
            vyx2 = VSET_VX2(vyx2, 0, vy0);
            vyx2 = VSET_VX2(vyx2, 1, vy1);

            VSSSEG_FLOAT(x, stride_x, vtx2, vl);
            VSSSEG_FLOAT(y, stride_y, vyx2, vl);
        }
    }

    return 0;
}
