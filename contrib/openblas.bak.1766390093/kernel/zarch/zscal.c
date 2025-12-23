/***************************************************************************
Copyright (c) 2013-2019, The OpenBLAS Project
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

/*
 * Avoid contraction of floating point operations, specifically fused
 * multiply-add, because they can cause unexpected results in complex
 * multiplication.
 */
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC optimize ("fp-contract=off")
#endif

#if defined(__clang__)
#pragma clang fp contract(off)
#endif

#include "common.h"
#include "vector-common.h"

static void zscal_kernel_8(BLASLONG n, FLOAT da_r, FLOAT da_i, FLOAT *x) {
    vector_float da_r_vec = vec_splats(da_r);
    vector_float da_i_vec = { -da_i, da_i };

    vector_float * x_vec_ptr = (vector_float *)x;

#pragma GCC unroll 16
    for (size_t i = 0; i < n; i++) {
	vector_float x_vec = vec_load_hinted(x + i * VLEN_FLOATS);
	vector_float x_swapped = {x_vec[1], x_vec[0]};

	x_vec_ptr[i] = x_vec * da_r_vec + x_swapped * da_i_vec;
    }
}

static void zscal_kernel_8_zero_r(BLASLONG n, FLOAT *alpha, FLOAT *x) {
  __asm__("vleg   %%v0,8(%[alpha]),0\n\t"
    "wflcdb %%v0,%%v0\n\t"
    "vleg   %%v0,8(%[alpha]),1\n\t"
    "srlg %[n],%[n],3\n\t"
    "xgr   %%r1,%%r1\n\t"
    "0:\n\t"
    "pfd 2, 1024(%%r1,%[x])\n\t"
    "vl   %%v16,0(%%r1,%[x])\n\t"
    "vl   %%v17,16(%%r1,%[x])\n\t"
    "vl   %%v18,32(%%r1,%[x])\n\t"
    "vl   %%v19,48(%%r1,%[x])\n\t"
    "vl   %%v20,64(%%r1,%[x])\n\t"
    "vl   %%v21,80(%%r1,%[x])\n\t"
    "vl   %%v22,96(%%r1,%[x])\n\t"
    "vl   %%v23,112(%%r1,%[x])\n\t"
    "vpdi %%v16,%%v16,%%v16,4\n\t"
    "vpdi %%v17,%%v17,%%v17,4\n\t"
    "vpdi %%v18,%%v18,%%v18,4\n\t"
    "vpdi %%v19,%%v19,%%v19,4\n\t"
    "vpdi %%v20,%%v20,%%v20,4\n\t"
    "vpdi %%v21,%%v21,%%v21,4\n\t"
    "vpdi %%v22,%%v22,%%v22,4\n\t"
    "vpdi %%v23,%%v23,%%v23,4\n\t"
    "vfmdb %%v16,%%v16,%%v0\n\t"
    "vfmdb %%v17,%%v17,%%v0\n\t"
    "vfmdb %%v18,%%v18,%%v0\n\t"
    "vfmdb %%v19,%%v19,%%v0\n\t"
    "vfmdb %%v20,%%v20,%%v0\n\t"
    "vfmdb %%v21,%%v21,%%v0\n\t"
    "vfmdb %%v22,%%v22,%%v0\n\t"
    "vfmdb %%v23,%%v23,%%v0\n\t"
    "vst %%v16,0(%%r1,%[x])\n\t"
    "vst %%v17,16(%%r1,%[x])\n\t"
    "vst %%v18,32(%%r1,%[x])\n\t"
    "vst %%v19,48(%%r1,%[x])\n\t"
    "vst %%v20,64(%%r1,%[x])\n\t"
    "vst %%v21,80(%%r1,%[x])\n\t"
    "vst %%v22,96(%%r1,%[x])\n\t"
    "vst %%v23,112(%%r1,%[x])\n\t"
    "agfi  %%r1,128\n\t"
    "brctg %[n],0b"
    : "+m"(*(FLOAT (*)[n * 2]) x),[n] "+&r"(n)
    : [x] "a"(x), "m"(*(const FLOAT (*)[2]) alpha),
       [alpha] "a"(alpha)
    : "cc", "r1", "v0", "v16", "v17", "v18", "v19", "v20", "v21", "v22",
       "v23");
}

static void zscal_kernel_8_zero_i(BLASLONG n, FLOAT *alpha, FLOAT *x) {
  __asm__("vlrepg %%v0,0(%[alpha])\n\t"
    "srlg %[n],%[n],3\n\t"
    "xgr   %%r1,%%r1\n\t"
    "0:\n\t"
    "pfd 2, 1024(%%r1,%[x])\n\t"
    "vl   %%v16,0(%%r1,%[x])\n\t"
    "vl   %%v17,16(%%r1,%[x])\n\t"
    "vl   %%v18,32(%%r1,%[x])\n\t"
    "vl   %%v19,48(%%r1,%[x])\n\t"
    "vl   %%v20,64(%%r1,%[x])\n\t"
    "vl   %%v21,80(%%r1,%[x])\n\t"
    "vl   %%v22,96(%%r1,%[x])\n\t"
    "vl   %%v23,112(%%r1,%[x])\n\t"
    "vfmdb %%v16,%%v16,%%v0\n\t"
    "vfmdb %%v17,%%v17,%%v0\n\t"
    "vfmdb %%v18,%%v18,%%v0\n\t"
    "vfmdb %%v19,%%v19,%%v0\n\t"
    "vfmdb %%v20,%%v20,%%v0\n\t"
    "vfmdb %%v21,%%v21,%%v0\n\t"
    "vfmdb %%v22,%%v22,%%v0\n\t"
    "vfmdb %%v23,%%v23,%%v0\n\t"
    "vst %%v16,0(%%r1,%[x])\n\t"
    "vst %%v17,16(%%r1,%[x])\n\t"
    "vst %%v18,32(%%r1,%[x])\n\t"
    "vst %%v19,48(%%r1,%[x])\n\t"
    "vst %%v20,64(%%r1,%[x])\n\t"
    "vst %%v21,80(%%r1,%[x])\n\t"
    "vst %%v22,96(%%r1,%[x])\n\t"
    "vst %%v23,112(%%r1,%[x])\n\t"
    "agfi  %%r1,128\n\t"
    "brctg %[n],0b"
    : "+m"(*(FLOAT (*)[n * 2]) x),[n] "+&r"(n)
    : [x] "a"(x), "m"(*(const FLOAT (*)[2]) alpha),
       [alpha] "a"(alpha)
    : "cc", "r1", "v0", "v16", "v17", "v18", "v19", "v20", "v21", "v22",
       "v23");
}

static void zscal_kernel_8_zero(BLASLONG n, FLOAT *x) {
  __asm__("vzero %%v0\n\t"
    "srlg %[n],%[n],3\n\t"
    "xgr   %%r1,%%r1\n\t"
    "0:\n\t"
    "pfd 2, 1024(%%r1,%[x])\n\t"
    "vst  %%v0,0(%%r1,%[x])\n\t"
    "vst  %%v0,16(%%r1,%[x])\n\t"
    "vst  %%v0,32(%%r1,%[x])\n\t"
    "vst  %%v0,48(%%r1,%[x])\n\t"
    "vst  %%v0,64(%%r1,%[x])\n\t"
    "vst  %%v0,80(%%r1,%[x])\n\t"
    "vst  %%v0,96(%%r1,%[x])\n\t"
    "vst  %%v0,112(%%r1,%[x])\n\t"
    "agfi  %%r1,128\n\t"
    "brctg %[n],0b"
    : "=m"(*(FLOAT (*)[n * 2]) x),[n] "+&r"(n)
    : [x] "a"(x)
    : "cc", "r1", "v0");
}

static void zscal_kernel_inc_8(BLASLONG n, FLOAT da_r, FLOAT da_i, FLOAT *x,
                               BLASLONG inc_x) {
  BLASLONG i;
  BLASLONG inc_x2 = 2 * inc_x;
  BLASLONG inc_x3 = inc_x2 + inc_x;
  FLOAT t0, t1, t2, t3;

  for (i = 0; i < n; i += 4) {
    t0 = da_r * x[0] - da_i * x[1];
    t1 = da_r * x[inc_x] - da_i * x[inc_x + 1];
    t2 = da_r * x[inc_x2] - da_i * x[inc_x2 + 1];
    t3 = da_r * x[inc_x3] - da_i * x[inc_x3 + 1];

    x[1] = da_i * x[0] + da_r * x[1];
    x[inc_x + 1] = da_i * x[inc_x] + da_r * x[inc_x + 1];
    x[inc_x2 + 1] = da_i * x[inc_x2] + da_r * x[inc_x2 + 1];
    x[inc_x3 + 1] = da_i * x[inc_x3] + da_r * x[inc_x3 + 1];

    x[0] = t0;
    x[inc_x] = t1;
    x[inc_x2] = t2;
    x[inc_x3] = t3;

    x += 4 * inc_x;
  }
}

int CNAME(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT da_r, FLOAT da_i,
          FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *dummy,
          BLASLONG dummy2) {
  BLASLONG i = 0, j = 0;
  FLOAT temp0;
  FLOAT temp1;
  FLOAT alpha[2] __attribute__ ((aligned(16)));

  if (inc_x != 1) {
    inc_x <<= 1;

    if (da_r == 0.0) {

      BLASLONG n1 = n & -2;

      if (da_i == 0.0) {
       if (dummy2 == 0) {
        while (j < n1) {

          x[i] = 0.0;
          x[i + 1] = 0.0;
          x[i + inc_x] = 0.0;
          x[i + 1 + inc_x] = 0.0;
          i += 2 * inc_x;
          j += 2;

        }

        while (j < n) {

          x[i] = 0.0;
          x[i + 1] = 0.0;
          i += inc_x;
          j++;

        }
       } else {
        while (j < n1) {
	if (isnan(x[i]) || isinf(x[i]) || isnan(x[i+1])) {
		x[i] = NAN;
		x[i+1] = NAN;
	} else {
		x[i] = 0.0;
		x[i+1] = 0.0;
	}
	if (isnan(x[i+inc_x]) || isinf(x[i+inc_x]) || isnan(x[i+inc_x+1])) {
		x[i + inc_x] = NAN;
		x[i + inc_x + 1] = NAN;
	} else {
		x[i + inc_x] = 0.;
		x[i + inc_x + 1] = 0.;
	}
	i += 2 * inc_x;
	j += 2;
	}
	       
	while (j < n) {
	if (isnan(x[i]) || isinf(x[i]) || isnan(x[i+1])) {
		x[i] = NAN;
		x[i+1] = NAN;
	} else {
		x[i] = 0.;
		x[i+1] = 0.;
	}
		i += inc_x;
		j++;
        }
       }
      } else {

        while (j < n1) {

	  if (isnan(x[i]) || isinf(x[i]))
		temp0 = NAN;
	  else
		temp0 = -da_i * x[i + 1];
	  if (!isinf(x[i + 1]))
          	x[i + 1] = da_i * x[i];
	  else
		x[i + 1] = NAN;
          x[i] = temp0;
	  if (isnan(x[i + inc_x]) || isinf(x[i + inc_x]))
		temp1 = NAN;
	  else
          temp1 = -da_i * x[i + 1 + inc_x];
	  if (!isinf(x[i + 1 + inc_x]))
            x[i + 1 + inc_x] = da_i * x[i + inc_x];
	  else
	    x[i + 1 + inc_x] = NAN;
          x[i + inc_x] = temp1;
          i += 2 * inc_x;
          j += 2;

        }

        while (j < n) {

	  if (isnan(x[i]) || isinf(x[i]))
		temp0 = NAN;
	  else
          	temp0 = -da_i * x[i + 1];
	  if (!isinf(x[i +1]))
          	x[i + 1] = da_i * x[i];
	  else
		x[i + 1] = NAN;
          x[i] = temp0;
          i += inc_x;
          j++;

        }

      }

    } else {

      if (da_i == 0.0 && dummy2) {
        BLASLONG n1 = n & -2;

        while (j < n1) {

          temp0 = da_r * x[i];
          x[i + 1] = da_r * x[i + 1];
          x[i] = temp0;
          temp1 = da_r * x[i + inc_x];
          x[i + 1 + inc_x] = da_r * x[i + 1 + inc_x];
          x[i + inc_x] = temp1;
          i += 2 * inc_x;
          j += 2;

        }

        while (j < n) {

          temp0 = da_r * x[i];
          x[i + 1] = da_r * x[i + 1];
          x[i] = temp0;
          i += inc_x;
          j++;

        }

      } else {

        BLASLONG n1 = n & -8;
        if (n1 > 0) {
          zscal_kernel_inc_8(n1, da_r, da_i, x, inc_x);
          j = n1;
          i = n1 * inc_x;
        }

        while (j < n) {

          temp0 = da_r * x[i] - da_i * x[i + 1];
          x[i + 1] = da_r * x[i + 1] + da_i * x[i];
          x[i] = temp0;
          i += inc_x;
          j++;

        }

      }

    }

    return (0);
  }

  BLASLONG n1 = n & -8;
  if (n1 > 0) {

    alpha[0] = da_r;
    alpha[1] = da_i;

    if (da_r == 0.0)
      if (da_i == 0 && dummy2 == 0)
        zscal_kernel_8_zero(n1, x);
      else
        zscal_kernel_8(n1, da_r, da_i, x);
    else
      zscal_kernel_8(n1, da_r, da_i, x);

    i = n1 << 1;
    j = n1;
  }

  if (da_r == 0.0 || isnan(da_r)) {

    if (da_i == 0.0) {
      double res= 0.0;
      if (isnan(da_r)) res = da_r;
      while (j < n) {
	if (dummy2)
		if (isnan(x[i]) || isnan(x[i+1])) res = NAN;
        x[i] = res;
        x[i + 1] = res;
        i += 2;
        j++;

      }

    } else if (isinf(da_r)) {
      while (j < n) {
	x[i] = NAN;
	x[i + 1] = da_r;
	i += 2;
	j++;
      }
    } else {

      while (j < n) {

	if (isinf(x[i]))
		temp0 = NAN;
	  else
        	temp0 = -da_i * x[i + 1];
	if (!isinf(x[i + 1]))
        	x[i + 1] = da_i * x[i];
	else
		x[i + 1] = NAN;
	if (!isnan(x[i]))
          x[i] = temp0;
        i += 2;
        j++;

      }

    }

  } else {

    if (da_i == 0.0) {

      while (j < n) {

        temp0 = da_r * x[i];
	if (dummy2) {
		if (isnan(x[i]) || isinf(x[i])) temp0 = NAN;
		if (isnan(x[i + 1]) || isinf(x[i + 1]))
			x[i + 1] = NAN;
		else
			x[i + 1] = da_r * x[i + 1];
	} else {
		if (isnan(x[i]))
			x[i + 1] = NAN;
		else
	        x[i + 1] = da_r * x[i + 1];
	}
	x[i] = temp0;
        i += 2;
        j++;

      }

    } else {

      while (j < n) {

        temp0 = da_r * x[i] - da_i * x[i + 1];
        x[i + 1] = da_r * x[i + 1] + da_i * x[i];
        if (!isnan(x[i])) x[i] = temp0;
        i += 2;
        j++;

      }

    }

  }

  return (0);
}
