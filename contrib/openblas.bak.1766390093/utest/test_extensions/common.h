/*****************************************************************************
Copyright (c) 2023, The OpenBLAS Project
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
      derived from this software without specific prior written
      permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

**********************************************************************************/

#ifndef _TEST_EXTENSION_COMMON_H_
#define _TEST_EXTENSION_COMMON_H_

#include <cblas.h>
#include <ctype.h>

#define TRUE 1
#define FALSE 0
#define INVALID -1
#define SINGLE_TOL 1e-02f
#define DOUBLE_TOL 1e-10

extern int check_error(void);
extern void set_xerbla(char* current_rout, int expected_info);
extern int BLASFUNC(xerbla)(char *name, blasint *info, blasint length);

extern void srand_generate(float *alpha, blasint n);
extern void drand_generate(double *alpha, blasint n);

extern float smatrix_difference(float *a, float *b, blasint cols, blasint rows, blasint ld);
extern double dmatrix_difference(double *a, double *b, blasint cols, blasint rows, blasint ld);

extern void cconjugate_vector(blasint n, blasint inc_x, float *x_ptr);
extern void zconjugate_vector(blasint n, blasint inc_x, double *x_ptr);

extern void stranspose(blasint rows, blasint cols, float alpha, float *a_src, int lda_src, 
                       float *a_dst, blasint lda_dst);
extern void dtranspose(blasint rows, blasint cols, double alpha, double *a_src, int lda_src, 
                double *a_dst, blasint lda_dst);
extern void ctranspose(blasint rows, blasint cols, float *alpha, float *a_src, int lda_src, 
                      float *a_dst, blasint lda_dst, int conj);
extern void ztranspose(blasint rows, blasint cols, double *alpha, double *a_src, int lda_src, 
                double *a_dst, blasint lda_dst, int conj);

extern void my_scopy(blasint rows, blasint cols, float alpha, float *a_src, int lda_src, 
           float *a_dst, blasint lda_dst);
extern void my_dcopy(blasint rows, blasint cols, double alpha, double *a_src, int lda_src, 
           double *a_dst, blasint lda_dst);
extern void my_ccopy(blasint rows, blasint cols, float *alpha, float *a_src, int lda_src, 
           float *a_dst, blasint lda_dst, int conj);
extern void my_zcopy(blasint rows, blasint cols, double *alpha, double *a_src, int lda_src, 
           double *a_dst, blasint lda_dst, int conj);                
#endif
