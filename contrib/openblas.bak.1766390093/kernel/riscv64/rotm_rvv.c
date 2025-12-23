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
#define VSETVL(n)               __riscv_vsetvl_e32m8(n)
#define FLOAT_V_T               vfloat32m8_t
#define VLSEV_FLOAT             __riscv_vlse32_v_f32m8
#define VSSEV_FLOAT             __riscv_vsse32_v_f32m8
#define VFMACCVF_FLOAT          __riscv_vfmacc_vf_f32m8
#define VFMULVF_FLOAT           __riscv_vfmul_vf_f32m8
#define VFMSACVF_FLOAT          __riscv_vfmsac_vf_f32m8
#else
#define VSETVL(n)               __riscv_vsetvl_e64m8(n)
#define FLOAT_V_T               vfloat64m8_t
#define VLSEV_FLOAT             __riscv_vlse64_v_f64m8
#define VSSEV_FLOAT             __riscv_vsse64_v_f64m8
#define VFMACCVF_FLOAT          __riscv_vfmacc_vf_f64m8
#define VFMULVF_FLOAT           __riscv_vfmul_vf_f64m8
#define VFMSACVF_FLOAT          __riscv_vfmsac_vf_f64m8
#endif

int CNAME(BLASLONG n, FLOAT *dx, BLASLONG incx, FLOAT *dy, BLASLONG incy, FLOAT *dparam)
{
	BLASLONG i__1, i__2;
	BLASLONG kx, ky;
	FLOAT dh11, dh12, dh22, dh21, dflag;
	BLASLONG nsteps;

	--dparam;
	--dy;
	--dx;

  	FLOAT_V_T v_w, v_z__, v_dx, v_dy;
  	BLASLONG stride, stride_x, stride_y, offset;

  	dflag = dparam[1];
    if (n <= 0 || dflag == - 2.0) goto L140;

    if (!(incx == incy && incx > 0)) goto L70;

    nsteps = n * incx;
    if (dflag < 0.) {
	goto L50;
    } else if (dflag == 0) {
	goto L10;
    } else {
	goto L30;
    }
L10:
    dh12 = dparam[4];
    dh21 = dparam[3];
    i__1 = nsteps;
    i__2 = incx;
    if(i__2 < 0){
        offset = i__1 - 2;
        dx += offset;
        dy += offset;
        i__1 = -i__1;
        i__2 = -i__2;
    }
    stride = i__2 * sizeof(FLOAT);
    n = i__1 / i__2;
    for (size_t vl; n > 0; n -= vl, dx += vl*i__2, dy += vl*i__2) {
        vl = VSETVL(n);

        v_w = VLSEV_FLOAT(&dx[1], stride, vl);
        v_z__ = VLSEV_FLOAT(&dy[1], stride, vl);

        v_dx = VFMACCVF_FLOAT(v_w, dh12, v_z__, vl);
        v_dy = VFMACCVF_FLOAT(v_z__, dh21, v_w, vl);

        VSSEV_FLOAT(&dx[1], stride, v_dx, vl);
        VSSEV_FLOAT(&dy[1], stride, v_dy, vl);
    }
    goto L140;
L30:
    dh11 = dparam[2];
    dh22 = dparam[5];
    i__2 = nsteps;
    i__1 = incx;
    if(i__1 < 0){
        offset = i__2 - 2;
        dx += offset;
        dy += offset;
        i__1 = -i__1;
        i__2 = -i__2;
    }
    stride = i__1 * sizeof(FLOAT);
    n = i__2  / i__1;
    for (size_t vl; n > 0; n -= vl, dx += vl*i__1, dy += vl*i__1) {
        vl = VSETVL(n);

        v_w = VLSEV_FLOAT(&dx[1], stride, vl);
        v_z__ = VLSEV_FLOAT(&dy[1], stride, vl);

        v_dx = VFMACCVF_FLOAT(v_z__, dh11, v_w, vl);
        v_dy = VFMSACVF_FLOAT(v_w, dh22, v_z__, vl);

        VSSEV_FLOAT(&dx[1], stride, v_dx, vl);
        VSSEV_FLOAT(&dy[1], stride, v_dy, vl);
    }
    goto L140;
L50:
    dh11 = dparam[2];
    dh12 = dparam[4];
    dh21 = dparam[3];
    dh22 = dparam[5];
    i__1 = nsteps;
    i__2 = incx;
    if(i__2 < 0){
        offset = i__1 - 2;
        dx += offset;
        dy += offset;
        i__1 = -i__1;
        i__2 = -i__2;
    }
    stride = i__2 * sizeof(FLOAT);
    n = i__1 / i__2;
    for (size_t vl; n > 0; n -= vl, dx += vl*i__2, dy += vl*i__2) {
        vl = VSETVL(n);

        v_w = VLSEV_FLOAT(&dx[1], stride, vl);
        v_z__ = VLSEV_FLOAT(&dy[1], stride, vl);

        v_dx = VFMULVF_FLOAT(v_w, dh11, vl);
        v_dx = VFMACCVF_FLOAT(v_dx, dh12, v_z__, vl);
        VSSEV_FLOAT(&dx[1], stride, v_dx, vl);

        v_dy = VFMULVF_FLOAT(v_w, dh21, vl);
        v_dy = VFMACCVF_FLOAT(v_dy, dh22, v_z__, vl);
        VSSEV_FLOAT(&dy[1], stride, v_dy, vl);
    }
    goto L140;
L70:
    kx = 1;
    ky = 1;
    if (incx < 0) {
	kx = (1 - n) * incx + 1;
    }
    if (incy < 0) {
	ky = (1 - n) * incy + 1;
    }

    if (dflag < 0.) {
	goto L120;
    } else if (dflag == 0) {
	goto L80;
    } else {
	goto L100;
    }
L80:
    dh12 = dparam[4];
    dh21 = dparam[3];
    if(incx < 0){
        incx = -incx;
        dx -= n*incx;
    }
    if(incy < 0){
        incy = -incy;
        dy -= n*incy;
    }
    stride_x = incx * sizeof(FLOAT);
    stride_y = incy * sizeof(FLOAT);
    for (size_t vl; n > 0; n -= vl, dx += vl*incx, dy += vl*incy) {
        vl = VSETVL(n);

        v_w = VLSEV_FLOAT(&dx[kx], stride_x, vl);
        v_z__ = VLSEV_FLOAT(&dy[ky], stride_y, vl);

        v_dx = VFMACCVF_FLOAT(v_w, dh12, v_z__, vl);
        v_dy = VFMACCVF_FLOAT(v_z__, dh21, v_w, vl);

        VSSEV_FLOAT(&dx[kx], stride_x, v_dx, vl);
        VSSEV_FLOAT(&dy[ky], stride_y, v_dy, vl);
    }
    goto L140;
L100:
    dh11 = dparam[2];
    dh22 = dparam[5];
    if(incx < 0){
        incx = -incx;
        dx -= n*incx;
    }
    if(incy < 0){
        incy = -incy;
        dy -= n*incy;
    }
    stride_x = incx * sizeof(FLOAT);
    stride_y = incy * sizeof(FLOAT);
    for (size_t vl; n > 0; n -= vl, dx += vl*incx, dy += vl*incy) {
        vl = VSETVL(n);

        v_w = VLSEV_FLOAT(&dx[kx], stride_x, vl);
        v_z__ = VLSEV_FLOAT(&dy[ky], stride_y, vl);

        v_dx = VFMACCVF_FLOAT(v_z__, dh11, v_w, vl);
        v_dy = VFMSACVF_FLOAT(v_w, dh22, v_z__, vl);

        VSSEV_FLOAT(&dx[kx], stride_x, v_dx, vl);
        VSSEV_FLOAT(&dy[ky], stride_y, v_dy, vl);
    }
    goto L140;
L120:
    dh11 = dparam[2];
    dh12 = dparam[4];
    dh21 = dparam[3];
    dh22 = dparam[5];
    if(incx < 0){
        incx = -incx;
        dx -= n*incx;
    }
    if(incy < 0){
        incy = -incy;
        dy -= n*incy;
    }
    stride_x = incx * sizeof(FLOAT);
    stride_y = incy * sizeof(FLOAT);
    for (size_t vl; n > 0; n -= vl, dx += vl*incx, dy += vl*incy) {
        vl = VSETVL(n);

        v_w = VLSEV_FLOAT(&dx[kx], stride_x, vl);
        v_z__ = VLSEV_FLOAT(&dy[ky], stride_y, vl);

        v_dx = VFMULVF_FLOAT(v_w, dh11, vl);
        v_dx = VFMACCVF_FLOAT(v_dx, dh12, v_z__, vl);
        VSSEV_FLOAT(&dx[kx], stride_x, v_dx, vl);

        v_dy = VFMULVF_FLOAT(v_w, dh21, vl);
        v_dy = VFMACCVF_FLOAT(v_dy, dh22, v_z__, vl);
        VSSEV_FLOAT(&dy[ky], stride_y, v_dy, vl);
    }
L140:
	return(0);
}
