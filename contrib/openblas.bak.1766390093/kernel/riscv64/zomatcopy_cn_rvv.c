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
#include <stdio.h>

#if defined(DOUBLE)
#define VLSEG2_FLOAT		__riscv_vlseg2e64_v_f64m4x2
#define VSSEG2_FLOAT		__riscv_vsseg2e64_v_f64m4x2
#define VSETVL              __riscv_vsetvl_e64m4
#define FLOAT_VX2_T         vfloat64m4x2_t
#define VGET_VX2			__riscv_vget_v_f64m4x2_f64m4
#define VSET_VX2			__riscv_vset_v_f64m4_f64m4x2
#define FLOAT_V             vfloat64m4_t
#define VFMULVF_FLOAT       __riscv_vfmul_vf_f64m4
#define VFMACCVF_FLOAT      __riscv_vfmacc_vf_f64m4
#else
#define VLSEG2_FLOAT		__riscv_vlseg2e32_v_f32m4x2
#define VSSEG2_FLOAT		__riscv_vsseg2e32_v_f32m4x2
#define VSETVL              __riscv_vsetvl_e32m4
#define FLOAT_VX2_T         vfloat32m4x2_t
#define VGET_VX2			__riscv_vget_v_f32m4x2_f32m4
#define VSET_VX2			__riscv_vset_v_f32m4_f32m4x2
#define FLOAT_V             vfloat32m4_t
#define VFMULVF_FLOAT       __riscv_vfmul_vf_f32m4
#define VFMACCVF_FLOAT      __riscv_vfmacc_vf_f32m4
#endif

int CNAME(BLASLONG rows, BLASLONG cols, FLOAT alpha_r, FLOAT alpha_i, FLOAT *a, BLASLONG lda, FLOAT *b, BLASLONG ldb)
{
	BLASLONG i,j,ia;
	FLOAT *aptr,*bptr;
	size_t vl;
	FLOAT_VX2_T va, vb;
	FLOAT_V va0, va1, vb0, vb1, vtemp;

	if ( rows <= 0 )  return(0);
	if ( cols <= 0 )  return(0);

	aptr = a;
	bptr = b;

	lda *= 2;
	ldb *= 2;

	for ( i=0; i<cols ; i++ )
	{
		ia = 0;

		for(j=0; j<rows; j+=vl)
		{
			vl = VSETVL(rows - j);
			va = VLSEG2_FLOAT(aptr + ia, vl);

			va0 = VGET_VX2(va, 0);
			va1 = VGET_VX2(va, 1);

			vb0 = VFMULVF_FLOAT(va0, alpha_r, vl);
			vb0 = VFMACCVF_FLOAT(vb0, -alpha_i, va1, vl);

			vb1 = VFMULVF_FLOAT(va0, alpha_i, vl);
			vb1 = VFMACCVF_FLOAT(vb1, alpha_r, va1, vl);

			vb = VSET_VX2(vb, 0, vb0);
			vb = VSET_VX2(vb, 1, vb1);

			VSSEG2_FLOAT(bptr + ia, vb, vl);

			ia += vl * 2;
		}
		aptr += lda;
		bptr += ldb;
	}

	return(0);
}
