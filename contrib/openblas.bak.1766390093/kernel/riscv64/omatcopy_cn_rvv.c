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

#if !defined(DOUBLE)
#define VSETVL_MAX				__riscv_vsetvlmax_e32m8()
#define VSETVL(n)               __riscv_vsetvl_e32m8(n)
#define FLOAT_V_T               vfloat32m8_t
#define VLEV_FLOAT              __riscv_vle32_v_f32m8
#define VSEV_FLOAT              __riscv_vse32_v_f32m8
#define VFMULVF_FLOAT           __riscv_vfmul_vf_f32m8
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f32m8
#else
#define VSETVL_MAX				__riscv_vsetvlmax_e64m8()
#define VSETVL(n)               __riscv_vsetvl_e64m8(n)
#define FLOAT_V_T               vfloat64m8_t
#define VLEV_FLOAT              __riscv_vle64_v_f64m8
#define VSEV_FLOAT              __riscv_vse64_v_f64m8
#define VFMULVF_FLOAT           __riscv_vfmul_vf_f64m8
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f64m8
#endif


int CNAME(BLASLONG rows, BLASLONG cols, FLOAT alpha, FLOAT *a, BLASLONG lda, FLOAT *b, BLASLONG ldb)
{
	BLASLONG i,j;
	FLOAT *aptr,*bptr;
	size_t vl;

	FLOAT_V_T va, vb;
	if ( rows <= 0 )  return(0);
	if ( cols <= 0 )  return(0);

	aptr = a;
	bptr = b;

	if ( alpha == 0.0 )
	{
		vl = VSETVL_MAX;
		va = VFMVVF_FLOAT(0, vl);
		for ( i=0; i<cols ; i++ )
		{
			for(j=0; j<rows; j+=vl)
			{
				vl = VSETVL(rows - j);
				VSEV_FLOAT(bptr + j, va, vl);
			}
			bptr += ldb;
		}
		return(0);
	}

	if ( alpha == 1.0 )
	{
		for ( i=0; i<cols ; i++ )
		{
			for(j=0; j<rows; j+=vl)
			{
				vl = VSETVL(rows - j);
				va = VLEV_FLOAT(aptr + j, vl);
				VSEV_FLOAT(bptr + j, va, vl);
			}
			aptr += lda;
			bptr += ldb;
		}
		return(0);
	}

	for ( i=0; i<cols ; i++ )
	{
		for(j=0; j<rows; j+=vl)
		{
			vl = VSETVL(rows - j);
			va = VLEV_FLOAT(aptr + j, vl);
			va = VFMULVF_FLOAT(va, alpha, vl);
			VSEV_FLOAT(bptr + j, va, vl);
		}
		aptr += lda;
		bptr += ldb;
	}

	return(0);
}
