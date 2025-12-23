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
#define VSETVL_MAX              __riscv_vsetvlmax_e32m4()
#define FLOAT_V_T               vfloat32m4_t
#define FLOAT_VX2_T             vfloat32m4x2_t
#define VGET_VX2                __riscv_vget_v_f32m4x2_f32m4
#define VSET_VX2                __riscv_vset_v_f32m4_f32m4x2
#define VLSEG_FLOAT             __riscv_vlseg2e32_v_f32m4x2
#define VLSSEG_FLOAT            __riscv_vlsseg2e32_v_f32m4x2
#define VSSEG_FLOAT             __riscv_vsseg2e32_v_f32m4x2
#define VSSSEG_FLOAT            __riscv_vssseg2e32_v_f32m4x2
#define VFMACCVF_FLOAT          __riscv_vfmacc_vf_f32m4
#define VFMULVF_FLOAT           __riscv_vfmul_vf_f32m4
#define VFNMSACVF_FLOAT         __riscv_vfnmsac_vf_f32m4
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f32m4
#else
#define VSETVL(n)               __riscv_vsetvl_e64m4(n)
#define VSETVL_MAX              __riscv_vsetvlmax_e64m4()
#define FLOAT_V_T               vfloat64m4_t
#define FLOAT_VX2_T             vfloat64m4x2_t
#define VGET_VX2                __riscv_vget_v_f64m4x2_f64m4
#define VSET_VX2                __riscv_vset_v_f64m4_f64m4x2
#define VLSEG_FLOAT             __riscv_vlseg2e64_v_f64m4x2
#define VLSSEG_FLOAT            __riscv_vlsseg2e64_v_f64m4x2
#define VSSEG_FLOAT             __riscv_vsseg2e64_v_f64m4x2
#define VSSSEG_FLOAT            __riscv_vssseg2e64_v_f64m4x2
#define VFMACCVF_FLOAT          __riscv_vfmacc_vf_f64m4
#define VFMULVF_FLOAT           __riscv_vfmul_vf_f64m4
#define VFNMSACVF_FLOAT         __riscv_vfnmsac_vf_f64m4
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f64m4
#endif

int CNAME(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT da_r,FLOAT da_i, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *dummy, BLASLONG dummy2)
{

    if((n <= 0) || (inc_x <= 0)) return(0);

    FLOAT_V_T vt, vr, vi;
    BLASLONG stride_x = inc_x * 2 * sizeof(FLOAT);
    size_t vlmax = VSETVL_MAX;
    FLOAT_VX2_T vx2;

    if(inc_x == 1) {
	if (dummy2 == 0 && da_r==0. && da_i == 0.) {
		BLASLONG i;
		for (i=0; i < n*2; i++) x[i]=0.;
		return(0);
	} else {

        for (size_t vl; n > 0; n -= vl, x += vl*2) {
            vl = VSETVL(n);

            vx2 = VLSEG_FLOAT(x, vl);
            vr = VGET_VX2(vx2, 0);
            vi = VGET_VX2(vx2, 1);

            vt = VFMULVF_FLOAT(vr, da_r, vl);
            vt = VFNMSACVF_FLOAT(vt, da_i, vi, vl);

            vi = VFMULVF_FLOAT(vi, da_r, vl);
            vi = VFMACCVF_FLOAT(vi, da_i, vr, vl);

            vx2 = VSET_VX2(vx2, 0, vt);
            vx2 = VSET_VX2(vx2, 1, vi);
            VSSEG_FLOAT(x, vx2, vl);
        }
        }

    } else {
	if (dummy2 == 0 && da_r==0. && da_i == 0.) {
		BLASLONG i,ix=0,inc_x2=2*inc_x;
		for (i=0; i < n; i++) {x[ix]=0.;x[ix+1]=0.;ix+=inc_x2;};
		return(0);
	} else {
        for (size_t vl; n > 0; n -= vl, x += vl*inc_x*2) {
            vl = VSETVL(n);

            vx2 = VLSSEG_FLOAT(x, stride_x, vl);
            vr = VGET_VX2(vx2, 0);
            vi = VGET_VX2(vx2, 1);

            vt = VFMULVF_FLOAT(vr, da_r, vl);
            vt = VFNMSACVF_FLOAT(vt, da_i, vi, vl);
            vi = VFMULVF_FLOAT(vi, da_r, vl);
            vi = VFMACCVF_FLOAT(vi, da_i, vr, vl);

            vx2 = VSET_VX2(vx2, 0, vt);
            vx2 = VSET_VX2(vx2, 1, vi);
            VSSSEG_FLOAT(x, stride_x, vx2, vl);
	    }
        }
    }

    return(0);
}
