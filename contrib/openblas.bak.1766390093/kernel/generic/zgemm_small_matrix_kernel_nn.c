/***************************************************************************
Copyright (c) 2020, The OpenBLAS Project
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

#ifndef B0
int CNAME(BLASLONG M, BLASLONG N, BLASLONG K, FLOAT * A, BLASLONG lda, FLOAT alpha0, FLOAT alpha1, FLOAT * B, BLASLONG ldb, FLOAT beta0, FLOAT beta1, FLOAT * C, BLASLONG ldc)
#else
int CNAME(BLASLONG M, BLASLONG N, BLASLONG K, FLOAT * A, BLASLONG lda, FLOAT alpha0, FLOAT alpha1, FLOAT * B, BLASLONG ldb, FLOAT * C, BLASLONG ldc)
#endif
{
	FLOAT real, imag;
#ifndef B0
	FLOAT tmp0, tmp1;
#endif
	int i, j, l;
	for(i = 0; i < M; i++){
		for(j = 0; j < N; j++){
			real=0;
			imag=0;

			for(l = 0; l < K; l++){
#if defined(NN)
				real += (A[l*2*lda + 2*i]*B[j*2*ldb + 2*l]      
					 -A[l*2*lda + 2*i + 1] * B[j*2*ldb + 2*l + 1]);

				imag+=(A[l*2*lda + 2*i] * B[j*2*ldb + 2*l + 1]        
				       + A[l*2*lda + 2*i + 1] * B[j*2*ldb + 2*l]);
#elif defined(NR)
				real += (A[l*2*lda + 2*i]*B[j*2*ldb + 2*l]      
					 +A[l*2*lda + 2*i + 1] * B[j*2*ldb + 2*l + 1]);

				imag+=(-A[l*2*lda + 2*i] * B[j*2*ldb + 2*l + 1]      
				       + A[l*2*lda + 2*i + 1] * B[j*2*ldb + 2*l]);
#elif defined(RN)
				real += (A[l*2*lda + 2*i]*B[j*2*ldb + 2*l]
					 +A[l*2*lda + 2*i + 1] * B[j*2*ldb + 2*l + 1]);

				imag+=(A[l*2*lda + 2*i] * B[j*2*ldb + 2*l + 1]
				       - A[l*2*lda + 2*i + 1] * B[j*2*ldb + 2*l]);
#elif defined(RR)
				real += (A[l*2*lda + 2*i]*B[j*2*ldb + 2*l]
					 -A[l*2*lda + 2*i + 1] * B[j*2*ldb + 2*l + 1]);

				imag+=(-A[l*2*lda + 2*i] * B[j*2*ldb + 2*l + 1]
				       - A[l*2*lda + 2*i + 1] * B[j*2*ldb + 2*l]);
#endif
			}

#ifndef B0
			tmp0 = beta0*C[j*2*ldc + 2*i] - beta1*C[j*2*ldc+ 2*i + 1];
			tmp1 = beta0*C[j*2*ldc+ 2*i + 1] + beta1*C[j*2*ldc + 2*i];


			C[j*2*ldc + 2*i] =tmp0+ alpha0*real - alpha1*imag;
			C[j*2*ldc+ 2*i + 1] = tmp1+ alpha0*imag + real*alpha1;
#else
			C[j*2*ldc + 2*i] = alpha0*real - alpha1*imag;
			C[j*2*ldc+ 2*i + 1] = alpha0*imag + real*alpha1;
#endif
		}
	}
	
	return 0;
}
