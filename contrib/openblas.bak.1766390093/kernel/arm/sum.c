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

/**************************************************************************************
* trivial copy of asum.c with the ABS() removed                                       *
**************************************************************************************/

#include "common.h"
#include "../simd/intrin.h"
#include <math.h>

FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x)
{
	BLASLONG i = 0;
	FLOAT sumf = 0.0;
	if (n <= 0 || inc_x <= 0)
		return (sumf);
	n *= inc_x;
	if (inc_x == 1)
	{
#if V_SIMD && (!defined(DOUBLE) || (defined(DOUBLE) && V_SIMD_F64 && V_SIMD > 128))
#ifdef DOUBLE
		const int vstep = v_nlanes_f64;
		const int unrollx4 = n & (-vstep * 4);
		const int unrollx = n & -vstep;
		v_f64 vsum0 = v_zero_f64();
		v_f64 vsum1 = v_zero_f64();
		v_f64 vsum2 = v_zero_f64();
		v_f64 vsum3 = v_zero_f64();
		for (; i < unrollx4; i += vstep * 4)
		{
			vsum0 = v_add_f64(vsum0, v_loadu_f64(x + i));
			vsum1 = v_add_f64(vsum1, v_loadu_f64(x + i + vstep));
			vsum2 = v_add_f64(vsum2, v_loadu_f64(x + i + vstep * 2));
			vsum3 = v_add_f64(vsum3, v_loadu_f64(x + i + vstep * 3));
		}
		vsum0 = v_add_f64(
			v_add_f64(vsum0, vsum1), v_add_f64(vsum2, vsum3));
		for (; i < unrollx; i += vstep)
		{
			vsum0 = v_add_f64(vsum0, v_loadu_f64(x + i));
		}
		sumf = v_sum_f64(vsum0);
#else
		const int vstep = v_nlanes_f32;
		const int unrollx4 = n & (-vstep * 4);
		const int unrollx = n & -vstep;
		v_f32 vsum0 = v_zero_f32();
		v_f32 vsum1 = v_zero_f32();
		v_f32 vsum2 = v_zero_f32();
		v_f32 vsum3 = v_zero_f32();
		for (; i < unrollx4; i += vstep * 4)
		{
			vsum0 = v_add_f32(vsum0, v_loadu_f32(x + i));
			vsum1 = v_add_f32(vsum1, v_loadu_f32(x + i + vstep));
			vsum2 = v_add_f32(vsum2, v_loadu_f32(x + i + vstep * 2));
			vsum3 = v_add_f32(vsum3, v_loadu_f32(x + i + vstep * 3));
		}
		vsum0 = v_add_f32(
			v_add_f32(vsum0, vsum1), v_add_f32(vsum2, vsum3));
		for (; i < unrollx; i += vstep)
		{
			vsum0 = v_add_f32(vsum0, v_loadu_f32(x + i));
		}
		sumf = v_sum_f32(vsum0);
#endif
#else
		int n1 = n & -4;
		for (; i < n1; i += 4)
		{
			sumf += x[i] + x[i + 1] + x[i + 2] + x[i + 3];
		}
#endif
	}
	while (i < n)
	{
		sumf += x[i];
		i += inc_x;
	}
	return (sumf);
}
