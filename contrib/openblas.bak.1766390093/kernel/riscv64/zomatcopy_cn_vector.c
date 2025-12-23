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
#define VSETVL(n) RISCV_RVV(vsetvl_e32m4)(n)
#define FLOAT_V_T vfloat32m4_t
#define VLEV_FLOAT RISCV_RVV(vle32_v_f32m4)
#define VLSEV_FLOAT RISCV_RVV(vlse32_v_f32m4)
#define VSEV_FLOAT RISCV_RVV(vse32_v_f32m4)
#define VSSEV_FLOAT RISCV_RVV(vsse32_v_f32m4)
#define VFMACCVF_FLOAT RISCV_RVV(vfmacc_vf_f32m4)
#define VFNMSACVF_FLOAT RISCV_RVV(vfnmsac_vf_f32m4)
#define VFMUL_VF_FLOAT RISCV_RVV(vfmul_vf_f32m4)
#define VSEV_FLOAT RISCV_RVV(vse32_v_f32m4)
#define VLSEG2_FLOAT RISCV_RVV(vlseg2e32_v_f32m4x2)
#define VSSEG2_FLOAT RISCV_RVV(vsseg2e32_v_f32m4x2)
#define FLOAT_VX2_T vfloat32m4x2_t
#define VGET_VX2 RISCV_RVV(vget_v_f32m4x2_f32m4)
#define VSET_VX2 RISCV_RVV(vset_v_f32m4_f32m4x2)
#else
#define VSETVL(n) RISCV_RVV(vsetvl_e64m4)(n)
#define FLOAT_V_T vfloat64m4_t
#define VLEV_FLOAT RISCV_RVV(vle64_v_f64m4)
#define VLSEV_FLOAT RISCV_RVV(vlse64_v_f64m4)
#define VSEV_FLOAT RISCV_RVV(vse64_v_f64m4)
#define VSSEV_FLOAT RISCV_RVV(vsse64_v_f64m4)
#define VFMACCVF_FLOAT RISCV_RVV(vfmacc_vf_f64m4)
#define VFNMSACVF_FLOAT RISCV_RVV(vfnmsac_vf_f64m4)
#define VFMUL_VF_FLOAT RISCV_RVV(vfmul_vf_f64m4)
#define VSEV_FLOAT RISCV_RVV(vse64_v_f64m4)
#define VLSEG2_FLOAT RISCV_RVV(vlseg2e64_v_f64m4x2)
#define VSSEG2_FLOAT RISCV_RVV(vsseg2e64_v_f64m4x2)
#define FLOAT_VX2_T vfloat64m4x2_t
#define VGET_VX2 RISCV_RVV(vget_v_f64m4x2_f64m4)
#define VSET_VX2 RISCV_RVV(vset_v_f64m4_f64m4x2)
#endif

int CNAME(BLASLONG rows, BLASLONG cols, FLOAT alpha_r, FLOAT alpha_i, FLOAT *a, BLASLONG lda, FLOAT *b, BLASLONG ldb)
{
	BLASLONG i,j,ia;
	FLOAT *aptr,*bptr;
	FLOAT_V_T bptr_v0 , bptr_v1 , aptr_v0 ,aptr_v1;
	FLOAT_VX2_T va, vb;
	unsigned int gvl = 0;

	if ( rows <= 0     )  return(0);
	if ( cols <= 0     )  return(0);

	aptr = a;
	bptr = b;

	lda *= 2;
	ldb *= 2;
	for ( i=0; i<cols ; i++ )
	{
		ia = 0;
		for(j=0; j<rows ; j+=gvl)
		{
			gvl = VSETVL(rows - j);
			va = VLSEG2_FLOAT(aptr + ia, gvl);
			aptr_v0 = VGET_VX2(va, 0);
			aptr_v1 = VGET_VX2(va, 1);
			bptr_v1 = VFMUL_VF_FLOAT( aptr_v1, alpha_r,gvl);
			bptr_v1 = VFMACCVF_FLOAT(bptr_v1, alpha_i, aptr_v0, gvl);
			bptr_v0 = VFMUL_VF_FLOAT(  aptr_v0,alpha_r, gvl);
			bptr_v0 = VFNMSACVF_FLOAT(bptr_v0, alpha_i, aptr_v1, gvl);
			vb = VSET_VX2(vb, 0, bptr_v0);
			vb = VSET_VX2(vb, 1, bptr_v1);
			VSSEG2_FLOAT(&bptr[ia], vb, gvl);
			ia += gvl * 2 ;

		}
		aptr += lda;
		bptr += ldb;
	}

	return(0);

}
