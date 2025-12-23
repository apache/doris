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
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f32m8
#define VFMULVF_FLOAT           __riscv_vfmul_vf_f32m8
#define VSEV_FLOAT              __riscv_vse32_v_f32m8
#else
#define VSETVL(n)               __riscv_vsetvl_e64m8(n)
#define FLOAT_V_T               vfloat64m8_t
#define VLEV_FLOAT              __riscv_vle64_v_f64m8
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f64m8
#define VFMULVF_FLOAT           __riscv_vfmul_vf_f64m8
#define VSEV_FLOAT              __riscv_vse64_v_f64m8
#endif

// Optimizes the implementation in ../generic/gemm_beta.c

int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT beta,
          IFLOAT *dummy2, BLASLONG dummy3, IFLOAT *dummy4, BLASLONG dummy5,
          FLOAT *c, BLASLONG ldc)
{
    BLASLONG chunk;
    FLOAT *c_offset;
	size_t vl;
    FLOAT_V_T vx;

    if (beta == ZERO) {

        vl = VSETVL(m);
        vx = VFMVVF_FLOAT(0.0, vl);

        for( ; n > 0; n--, c += ldc) {
            c_offset = c;

            for(chunk=m; chunk > 0; chunk -= vl, c_offset += vl) {
                vl = VSETVL(chunk);

                VSEV_FLOAT(c_offset, vx, vl);
			}
		}

	} else {

        for( ; n > 0; n--, c += ldc) {
            c_offset = c;

            for(chunk=m; chunk > 0; chunk -= vl, c_offset += vl) {
                vl = VSETVL(chunk);

                vx = VLEV_FLOAT(c_offset, vl);
                vx = VFMULVF_FLOAT(vx, beta, vl);
                VSEV_FLOAT(c_offset, vx, vl);
			}
		}

	}

    return 0;
}
