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

int CNAME(BLASLONG n, FLOAT *dx, BLASLONG incx, FLOAT *dy, BLASLONG incy, FLOAT *dparam)
{
  	BLASLONG i__1, i__2;
  	BLASLONG i__;
  	FLOAT w, z__;
  	BLASLONG kx, ky;
  	FLOAT dh11, dh12, dh22, dh21, dflag;
  	BLASLONG nsteps;

  	--dparam;
  	--dy;
  	--dx;

  	dflag = dparam[1];
    if (n <= 0 || dflag == - 2.0) goto L140;

    if (! (incx == incy && incx > 0)) goto L70;

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
    for (i__ = 1; i__2 < 0 ? i__ >= i__1 : i__ <= i__1; i__ += i__2) {
	w = dx[i__];
	z__ = dy[i__];
	dx[i__] = w + z__ * dh12;
	dy[i__] = w * dh21 + z__;
/* L20: */
    }
    goto L140;
L30:
    dh11 = dparam[2];
    dh22 = dparam[5];
    i__2 = nsteps;
    i__1 = incx;
    for (i__ = 1; i__1 < 0 ? i__ >= i__2 : i__ <= i__2; i__ += i__1) {
	w = dx[i__];
	z__ = dy[i__];
	dx[i__] = w * dh11 + z__;
	dy[i__] = -w + dh22 * z__;
/* L40: */
    }
    goto L140;
L50:
    dh11 = dparam[2];
    dh12 = dparam[4];
    dh21 = dparam[3];
    dh22 = dparam[5];
    i__1 = nsteps;
    i__2 = incx;
    for (i__ = 1; i__2 < 0 ? i__ >= i__1 : i__ <= i__1; i__ += i__2) {
	w = dx[i__];
	z__ = dy[i__];
	dx[i__] = w * dh11 + z__ * dh12;
	dy[i__] = w * dh21 + z__ * dh22;
/* L60: */
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
    i__2 = n;
    for (i__ = 1; i__ <= i__2; ++i__) {
	w = dx[kx];
	z__ = dy[ky];
	dx[kx] = w + z__ * dh12;
	dy[ky] = w * dh21 + z__;
	kx += incx;
	ky += incy;
/* L90: */
    }
    goto L140;
L100:
    dh11 = dparam[2];
    dh22 = dparam[5];
    i__2 = n;
    for (i__ = 1; i__ <= i__2; ++i__) {
	w = dx[kx];
	z__ = dy[ky];
	dx[kx] = w * dh11 + z__;
	dy[ky] = -w + dh22 * z__;
	kx += incx;
	ky += incy;
/* L110: */
    }
    goto L140;
L120:
    dh11 = dparam[2];
    dh12 = dparam[4];
    dh21 = dparam[3];
    dh22 = dparam[5];
    i__2 = n;
    for (i__ = 1; i__ <= i__2; ++i__) {
	w = dx[kx];
	z__ = dy[ky];
	dx[kx] = w * dh11 + z__ * dh12;
	dy[ky] = w * dh21 + z__ * dh22;
	kx += incx;
	ky += incy;
/* L130: */
    }
L140:
	return(0);
}
