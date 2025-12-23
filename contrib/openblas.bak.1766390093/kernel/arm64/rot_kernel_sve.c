/*******************************************************************************
Copyright (c) 2015, The OpenBLAS Project
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
*******************************************************************************/
#include "common.h"
#include <arm_sve.h>

#ifdef DOUBLE
#define SVE_TYPE svfloat64_t
#define SVE_ZERO svdup_f64(0.0)
#define SVE_WHILELT svwhilelt_b64
#define SVE_ALL svptrue_b64()
#define SVE_WIDTH svcntd()
#else
#define SVE_TYPE svfloat32_t
#define SVE_ZERO svdup_f32(0.0)
#define SVE_WHILELT svwhilelt_b32
#define SVE_ALL svptrue_b32()
#define SVE_WIDTH svcntw()
#endif

static int rot_kernel_sve(BLASLONG n, FLOAT *x, FLOAT *y, FLOAT c, FLOAT s)
{
       for (BLASLONG i = 0; i < n; i += SVE_WIDTH)
              {
              svbool_t pg = SVE_WHILELT((uint64_t)i, (uint64_t)n);
              SVE_TYPE x_vec = svld1(pg, &x[i]);
              SVE_TYPE y_vec = svld1(pg, &y[i]);
              SVE_TYPE cx_vec = svmul_z(pg, x_vec, c);
              SVE_TYPE sy_vec = svmul_z(pg, y_vec, s);
              SVE_TYPE sx_vec = svmul_z(pg, x_vec, s);
              SVE_TYPE cy_vec = svmul_z(pg, y_vec, c);
              svst1(pg, &x[i], svadd_z(pg, cx_vec, sy_vec));
              svst1(pg, &y[i], svsub_z(pg, cy_vec, sx_vec));
       }
       return (0);
}