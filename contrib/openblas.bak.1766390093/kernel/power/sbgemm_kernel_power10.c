/*********************************************************************************
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
**********************************************************************************/
#include "common.h"
#include <altivec.h>
#if defined(BFLOAT16) && defined(BFLOAT16CONVERSION)
static float
bfloat16tof32 (bfloat16 f16)
{
  float result = 0;
  unsigned short *q = (unsigned short *) (&result);
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
  q[0] = f16;
#else
  q[1] = f16;
#endif
  return result;
}

#define BF16TOF32(x) (bfloat16tof32(x))
#else
#define BF16TOF32(x) x
#endif

typedef __vector unsigned char  vec_t;
typedef FLOAT v4sf_t __attribute__ ((vector_size (16)));
typedef FLOAT v2sf_t __attribute__ ((vector_size (8)));

/* 
 * BFLOAT16 xvbf16ger2pp instruction needs 4×2 matrix of
 * bfloat16 floating-point values as input. Hence this
 * merging is needed on A and B matrices. 
 */
#define MERGE_HIGH(x, y) (vec_t) vec_mergeh ((vector short)x, (vector short)y)
#define MERGE_LOW(x, y) (vec_t) vec_mergel ((vector short)x, (vector short)y)

#define SAVE_ACC(ACC, J)  \
	  __builtin_mma_disassemble_acc ((void *)result, ACC); \
	  rowC = (v4sf_t *) &CO[0* ldc+J]; \
          rowC[0] += result[0] * alpha; \
          rowC = (v4sf_t *) &CO[1*ldc+J]; \
          rowC[0] += result[1] * alpha; \
          rowC = (v4sf_t *) &CO[2*ldc+J]; \
          rowC[0] += result[2] * alpha; \
          rowC = (v4sf_t *) &CO[3*ldc+J]; \
          rowC[0] += result[3] * alpha;
#define SAVE_ACC1(ACC, J)  \
	  __builtin_mma_disassemble_acc ((void *)result, ACC); \
	  rowC = (v4sf_t *) &CO[4* ldc+J]; \
          rowC[0] += result[0] * alpha; \
          rowC = (v4sf_t *) &CO[5*ldc+J]; \
          rowC[0] += result[1] * alpha; \
          rowC = (v4sf_t *) &CO[6*ldc+J]; \
          rowC[0] += result[2] * alpha; \
          rowC = (v4sf_t *) &CO[7*ldc+J]; \
          rowC[0] += result[3] * alpha;
#define  SAVE4x2_ACC(ACC, J)  \
	  __builtin_mma_disassemble_acc ((void *)result, ACC); \
	  rowC = (v2sf_t *) &CO[0* ldc+J]; \
          rowC[0] += result[0] * alpha; \
	  rowC = (v2sf_t *) &CO[1* ldc+J]; \
          rowC[0] += result[2] * alpha; \
	  rowC = (v2sf_t *) &CO[2* ldc+J]; \
          rowC[0] += result[4] * alpha; \
	  rowC = (v2sf_t *) &CO[3* ldc+J]; \
          rowC[0] += result[6] * alpha;
#define  SAVE4x2_ACC1(ACC, J)  \
	  __builtin_mma_disassemble_acc ((void *)result, ACC); \
	  rowC = (v2sf_t *) &CO[4* ldc+J]; \
          rowC[0] += result[0] * alpha; \
	  rowC = (v2sf_t *) &CO[5* ldc+J]; \
          rowC[0] += result[2] * alpha; \
	  rowC = (v2sf_t *) &CO[6* ldc+J]; \
          rowC[0] += result[4] * alpha; \
	  rowC = (v2sf_t *) &CO[7* ldc+J]; \
          rowC[0] += result[6] * alpha;

 #define  SAVE4x2_ACC_SCALAR(ACC) {                             \
           __builtin_mma_disassemble_acc ((void *)result, ACC); \
           res[0] = result[0] * alpha;                          \
           res[1] = result[1] * alpha;                          \
           res[2] = result[2] * alpha;                          \
           res[3] = result[3] * alpha;                          \
           CO[0 * ldc] += res[0][0];                            \
           CO[1 * ldc] += res[1][0];                            \
           CO[2 * ldc] += res[2][0];                            \
           CO[3 * ldc] += res[3][0];                            \
 }

 #define  SAVE4x2_ACC1_SCALAR(ACC) {                            \
           __builtin_mma_disassemble_acc ((void *)result, ACC); \
           res[0] = result[0] * alpha;                          \
           res[1] = result[1] * alpha;                          \
           res[2] = result[2] * alpha;                          \
           res[3] = result[3] * alpha;                          \
           CO[4 * ldc] += res[0][0];                            \
           CO[5 * ldc] += res[1][0];                            \
           CO[6 * ldc] += res[2][0];                            \
           CO[7 * ldc] += res[3][0];                            \
}

#define MMA __builtin_mma_xvbf16ger2pp

#define  SAVE2x4_ACC(ACC, J)  \
	  __builtin_mma_disassemble_acc ((void *)result, ACC); \
	  rowC = (v4sf_t *) &CO[0* ldc+J]; \
          rowC[0] += result[0] * alpha; \
	  rowC = (v4sf_t *) &CO[1* ldc+J]; \
          rowC[0] += result[1] * alpha;

#define SET_ACC_ZERO4() \
	  __builtin_mma_xxsetaccz (&acc0); \
	  __builtin_mma_xxsetaccz (&acc1); \
	  __builtin_mma_xxsetaccz (&acc2); \
	  __builtin_mma_xxsetaccz (&acc3);

#define SET_ACC_ZERO8() \
	  __builtin_mma_xxsetaccz (&acc0); \
	  __builtin_mma_xxsetaccz (&acc1); \
	  __builtin_mma_xxsetaccz (&acc2); \
	  __builtin_mma_xxsetaccz (&acc3); \
	  __builtin_mma_xxsetaccz (&acc4); \
	  __builtin_mma_xxsetaccz (&acc5); \
	  __builtin_mma_xxsetaccz (&acc6); \
	  __builtin_mma_xxsetaccz (&acc7);

#define PREFETCH1(x, y) asm volatile ("dcbt %0, %1" : : "r" (x), "b" (y) : "memory");
/*************************************************************************************
* SBGEMM Kernel
*************************************************************************************/
int
CNAME (BLASLONG m, BLASLONG n, BLASLONG k, FLOAT alpha, IFLOAT * A,
       IFLOAT * B, FLOAT * C, BLASLONG ldc)
{
  BLASLONG i1;
  v4sf_t valpha = { alpha, alpha, alpha, alpha };
  vector short vzero = { 0, 0, 0, 0, 0, 0, 0, 0 };
  /* Loop for n >= 8. */
  for (i1 = 0; i1 < (n >> 3); i1++)
    {
      BLASLONG j;
      FLOAT *CO;
      IFLOAT *AO;
      CO = C;
      C += ldc << 3;
      AO = A;
      PREFETCH1 (A, 128);
      PREFETCH1 (A, 256);
      /* Loop for m >= 16. */
      for (j = 0; j < (m >> 4); j++)
	{
	  IFLOAT *BO = B;
	  v4sf_t *rowC;
	  v4sf_t result[4];
	  __vector_quad acc0, acc1, acc2, acc3, acc4, acc5, acc6, acc7;
	  SET_ACC_ZERO8 ();
	  BLASLONG l = 0;
	  for (l = 0; l < k / 2; l++)
	    {
	      vec_t *rowA = (vec_t *) & (AO[l << 5]);
	      vec_t *rowB = (vec_t *) & (BO[l << 4]);
	      MMA (&acc0, rowB[0], rowA[0]);
	      MMA (&acc1, rowB[1], rowA[0]);
	      MMA (&acc2, rowB[0], rowA[1]);
	      MMA (&acc3, rowB[1], rowA[1]);
	      MMA (&acc4, rowB[0], rowA[2]);
	      MMA (&acc5, rowB[1], rowA[2]);
	      MMA (&acc6, rowB[0], rowA[3]);
	      MMA (&acc7, rowB[1], rowA[3]);
	    }
	  if (k % 2 == 1)
	    {
	      if (k > 1)
		l = (k / 2) << 4;
	      vec_t *rowA = (vec_t *) & (AO[l << 1]);
	      vec_t *rowB = (vec_t *) & (BO[l]);
	      vec_t rowB_h = MERGE_HIGH (rowB[0], vzero);
	      vec_t rowB_l = MERGE_LOW (rowB[0], vzero);
	      vec_t rowA_h = MERGE_HIGH (rowA[0], vzero);
	      vec_t rowA_l = MERGE_LOW (rowA[0], vzero);
	      vec_t rowA2_h = MERGE_HIGH (rowA[1], vzero);
	      vec_t rowA2_l = MERGE_LOW (rowA[1], vzero);
	      MMA (&acc0, rowB_h, rowA_h);
	      MMA (&acc1, rowB_l, rowA_h);
	      MMA (&acc2, rowB_h, rowA_l);
	      MMA (&acc3, rowB_l, rowA_l);
	      MMA (&acc4, rowB_h, rowA2_h);
	      MMA (&acc5, rowB_l, rowA2_h);
	      MMA (&acc6, rowB_h, rowA2_l);
	      MMA (&acc7, rowB_l, rowA2_l);
	    }
	  SAVE_ACC (&acc0, 0);
	  SAVE_ACC (&acc2, 4);
	  SAVE_ACC1 (&acc1, 0);
	  SAVE_ACC1 (&acc3, 4);
	  SAVE_ACC (&acc4, 8);
	  SAVE_ACC (&acc6, 12);
	  SAVE_ACC1 (&acc5, 8);
	  SAVE_ACC1 (&acc7, 12);
	  CO += 16;

	  AO += (k << 4);
	  BO += (k << 3);
	}
      if (m & 8)
	{
	  IFLOAT *BO = B;
	  v4sf_t *rowC;
	  v4sf_t result[4];
	  __vector_quad acc0, acc1, acc2, acc3;
	  SET_ACC_ZERO4 ();
	  BLASLONG l = 0;
	  for (l = 0; l < k / 2; l++)
	    {
	      vec_t *rowA = (vec_t *) & (AO[l << 4]);
	      vec_t *rowB = (vec_t *) & (BO[l << 4]);

	      MMA (&acc0, rowB[0], rowA[0]);
	      MMA (&acc1, rowB[1], rowA[0]);
	      MMA (&acc2, rowB[0], rowA[1]);
	      MMA (&acc3, rowB[1], rowA[1]);
	    }
	  if (k % 2 == 1)
	    {
	      if (k > 1)
		l = (k / 2) << 4;
	      vec_t *rowA = (vec_t *) & (AO[l]);
	      vec_t *rowB = (vec_t *) & (BO[l]);
	      vec_t rowB_h = MERGE_HIGH (rowB[0], vzero);
	      vec_t rowB_l = MERGE_LOW (rowB[0], vzero);
	      vec_t rowA_h = MERGE_HIGH (rowA[0], vzero);
	      vec_t rowA_l = MERGE_LOW (rowA[0], vzero);
	      MMA (&acc0, rowB_h, rowA_h);
	      MMA (&acc1, rowB_l, rowA_h);
	      MMA (&acc2, rowB_h, rowA_l);
	      MMA (&acc3, rowB_l, rowA_l);
	    }
	  SAVE_ACC (&acc0, 0);
	  SAVE_ACC (&acc2, 4);
	  SAVE_ACC1 (&acc1, 0);
	  SAVE_ACC1 (&acc3, 4);
	  CO += 8;
	  AO += (k << 3);
	  BO += (k << 3);
	}
      if (m & 4)
	{
	  IFLOAT *BO = B;
	  v4sf_t *rowC;
	  v4sf_t result[4];
	  __vector_quad acc0, acc1;
	  __builtin_mma_xxsetaccz (&acc0);
	  __builtin_mma_xxsetaccz (&acc1);
	  BLASLONG l = 0;
	  for (l = 0; l < k / 2; l++)
	    {
	      vec_t *rowA = (vec_t *) & (AO[l << 3]);
	      vec_t *rowB = (vec_t *) & (BO[l << 4]);
	      MMA (&acc0, rowB[0], rowA[0]);
	      MMA (&acc1, rowB[1], rowA[0]);
	    }
	  if (k % 2 == 1)
	    {
	      if (k > 1)
		l = (k / 2) << 3;
	      vector short rowA =
		{ AO[l + 0], 0, AO[l + 1], 0, AO[l + 2], 0, AO[l + 3], 0 };
	      vec_t *rowB = (vec_t *) & (BO[l << 1]);
	      MMA (&acc0, MERGE_HIGH (rowB[0], vzero), (vec_t) rowA);
	      MMA (&acc1, MERGE_LOW (rowB[0], vzero), (vec_t) rowA);
	    }
	  SAVE_ACC (&acc0, 0);
	  SAVE_ACC1 (&acc1, 0);
	  CO += 4;
	  AO += (k << 2);
	  BO += (k << 3);
	}
      if (m & 2)
	{
	  IFLOAT *BO = B;
	  v2sf_t *rowC;
	  v2sf_t result[8];
	  __vector_quad acc0, acc1;
	  __builtin_mma_xxsetaccz (&acc0);
	  __builtin_mma_xxsetaccz (&acc1);
	  BLASLONG l = 0;
	  for (l = 0; l < k / 2; l++)
	    {
	      vector short rowA =
		{ AO[(l << 2) + 0], AO[(l << 2) + 2], AO[(l << 2) + 1],
		AO[(l << 2) + 3],
		0, 0, 0, 0
	      };
	      vec_t *rowB = (vec_t *) & (BO[l << 4]);
	      MMA (&acc0, rowB[0], (vec_t) rowA);
	      MMA (&acc1, rowB[1], (vec_t) rowA);
	    }
	  if (k % 2 == 1)
	    {
	      if (k > 1)
		l = (k / 2) << 2;
	      vector short rowA = { AO[l + 0], 0, AO[l + 1], 0, 0, 0, 0, 0 };
	      vec_t *rowB = (vec_t *) & (BO[(l << 2)]);
	      MMA (&acc0, MERGE_HIGH (rowB[0], vzero), (vec_t) rowA);
	      MMA (&acc1, MERGE_LOW (rowB[0], vzero), (vec_t) rowA);
	    }
	  SAVE4x2_ACC (&acc0, 0);
	  SAVE4x2_ACC1 (&acc1, 0);
	  CO += 2;
	  AO += (k << 1);
	  BO += (k << 3);
	}
      if (m & 1)
	{
	  IFLOAT *BO = B;
	  v4sf_t result[4], res[4];
	  __vector_quad acc0, acc1;
	  __builtin_mma_xxsetaccz (&acc0);
	  __builtin_mma_xxsetaccz (&acc1);
	  BLASLONG l = 0;
	  for (l = 0; l < k / 2; l++)
	    {
	      vector short rowA =
		{ AO[(l << 1) + 0], AO[(l << 1) + 1], 0, 0, 0, 0, 0, 0};
	      vec_t *rowB = (vec_t *) & (BO[l << 4]);
	      MMA (&acc0, rowB[0], (vec_t) rowA);
	      MMA (&acc1, rowB[1], (vec_t) rowA);
	    }
	  if (k % 2 == 1)
	    {
	      if (k > 1)
		l = (k / 2) << 1;
	      vector short rowA = { AO[l], 0, 0, 0, 0, 0, 0, 0 };
	      vec_t *rowB = (vec_t *) & (BO[(l << 3)]);
	      MMA (&acc0, MERGE_HIGH (rowB[0], vzero), (vec_t) rowA);
	      MMA (&acc1, MERGE_LOW (rowB[0], vzero), (vec_t) rowA);
	    }
	  SAVE4x2_ACC_SCALAR (&acc0);
	  SAVE4x2_ACC1_SCALAR (&acc1);
	  CO += 1;
	  AO += k;
	  BO += (k << 3);
	}
      B += k << 3;
    }
  if (n & 4)
    {
      BLASLONG j;
      FLOAT *CO;
      IFLOAT *AO;
      CO = C;
      C += ldc << 2;
      AO = A;
      /* Loop for m >= 32. */
      for (j = 0; j < (m >> 5); j++)
	{
	  IFLOAT *BO = B;
	  IFLOAT *A1 = AO + (16 * k);
	  v4sf_t *rowC;
	  v4sf_t result[4];
	  __vector_quad acc0, acc1, acc2, acc3, acc4, acc5, acc6, acc7;
	  SET_ACC_ZERO8 ();
	  BLASLONG l = 0;
	  for (l = 0; l < k / 2; l++)
	    {
	      vec_t *rowA = (vec_t *) & (AO[l << 5]);
	      vec_t *rowA1 = (vec_t *) & (A1[l << 5]);
	      vec_t *rowB = (vec_t *) & (BO[l << 3]);
	      MMA (&acc0, rowB[0], rowA[0]);
	      MMA (&acc1, rowB[0], rowA[1]);
	      MMA (&acc2, rowB[0], rowA[2]);
	      MMA (&acc3, rowB[0], rowA[3]);
	      MMA (&acc4, rowB[0], rowA1[0]);
	      MMA (&acc5, rowB[0], rowA1[1]);
	      MMA (&acc6, rowB[0], rowA1[2]);
	      MMA (&acc7, rowB[0], rowA1[3]);
	    }
	  if (k % 2 == 1)
	    {
	      if (k > 1)
		l = (k / 2) << 3;
	      vec_t *rowA = (vec_t *) & (AO[(l << 2)]);
	      vec_t *rowA1 = (vec_t *) & (A1[(l << 2)]);
	      vector short rowB_mrg =
		{ BO[l], 0, BO[l + 1], 0, BO[l + 2], 0, BO[l + 3], 0 };
	      MMA (&acc0, (vec_t)rowB_mrg, MERGE_HIGH (rowA[0], vzero));
	      MMA (&acc1, (vec_t)rowB_mrg, MERGE_LOW (rowA[0], vzero));
	      MMA (&acc2, (vec_t)rowB_mrg, MERGE_HIGH (rowA[1], vzero));
	      MMA (&acc3, (vec_t)rowB_mrg, MERGE_LOW (rowA[1], vzero));
	      MMA (&acc4, (vec_t)rowB_mrg, MERGE_HIGH (rowA1[0], vzero));
	      MMA (&acc5, (vec_t)rowB_mrg, MERGE_LOW (rowA1[0], vzero));
	      MMA (&acc6, (vec_t)rowB_mrg, MERGE_HIGH (rowA1[1], vzero));
	      MMA (&acc7, (vec_t)rowB_mrg, MERGE_LOW (rowA1[1], vzero));
	    }

	  SAVE_ACC (&acc0, 0);
	  SAVE_ACC (&acc1, 4);
	  CO += 8;
	  SAVE_ACC (&acc2, 0);
	  SAVE_ACC (&acc3, 4);
	  CO += 8;
	  SAVE_ACC (&acc4, 0);
	  SAVE_ACC (&acc5, 4);
	  CO += 8;
	  SAVE_ACC (&acc6, 0);
	  SAVE_ACC (&acc7, 4);
	  CO += 8;
	  AO += k << 5;
	  BO += k << 2;
	}
      if (m & 16)
	{
	  IFLOAT *BO = B;
	  v4sf_t *rowC;
	  v4sf_t result[4];
	  __vector_quad acc0, acc1, acc2, acc3;
	  SET_ACC_ZERO4 ();
	  BLASLONG l = 0;
	  for (l = 0; l < k / 2; l++)
	    {
	      vec_t *rowA = (vec_t *) & (AO[l << 5]);
	      vec_t *rowB = (vec_t *) & (BO[l << 3]);
	      MMA (&acc0, rowB[0], rowA[0]);
	      MMA (&acc1, rowB[0], rowA[1]);
	      MMA (&acc2, rowB[0], rowA[2]);
	      MMA (&acc3, rowB[0], rowA[3]);
	    }
	  if (k % 2 == 1)
	    {
	      if (k > 1)
		l = (k / 2) << 3;
	      vec_t *rowA = (vec_t *) & (AO[(l << 2)]);
	      vector short rowB_mrg =
		{ BO[l], 0, BO[l + 1], 0, BO[l + 2], 0, BO[l + 3], 0 };
	      MMA (&acc0, (vec_t)rowB_mrg, MERGE_HIGH (rowA[0], vzero));
	      MMA (&acc1, (vec_t)rowB_mrg, MERGE_LOW (rowA[0], vzero));
	      MMA (&acc2, (vec_t)rowB_mrg, MERGE_HIGH (rowA[1], vzero));
	      MMA (&acc3, (vec_t)rowB_mrg, MERGE_LOW (rowA[1], vzero));
	    }

	  SAVE_ACC (&acc0, 0);
	  SAVE_ACC (&acc1, 4);
	  CO += 8;
	  SAVE_ACC (&acc2, 0);
	  SAVE_ACC (&acc3, 4);
	  CO += 8;
	  AO += k << 4;
	  BO += k << 2;
	}
      if (m & 8)
	{
	  IFLOAT *BO = B;
	  v4sf_t *rowC;
	  v4sf_t result[4];
	  __vector_quad acc0, acc1;
	  __builtin_mma_xxsetaccz (&acc0);
	  __builtin_mma_xxsetaccz (&acc1);
	  BLASLONG l = 0;
	  for (l = 0; l < k / 2; l++)
	    {
	      vec_t *rowA = (vec_t *) & (AO[l << 4]);
	      vec_t *rowB = (vec_t *) & (BO[l << 3]);
	      MMA (&acc0, rowB[0], rowA[0]);
	      MMA (&acc1, rowB[0], rowA[1]);
	    }
	  if (k % 2 == 1)
	    {
	      if (k > 1)
		l = (k / 2) << 3;
	      vec_t *rowA = (vec_t *) & (AO[l << 1]);
	      vector short rowB_mrg =
		{ BO[l], 0, BO[l + 1], 0, BO[l + 2], 0, BO[l + 3], 0 };
	      MMA (&acc0, (vec_t)rowB_mrg, MERGE_HIGH (rowA[0], vzero));
	      MMA (&acc1, (vec_t)rowB_mrg, MERGE_LOW (rowA[0], vzero));
	    }
	  SAVE_ACC (&acc0, 0);
	  SAVE_ACC (&acc1, 4);
	  CO += 8;
	  AO += k << 3;
	  BO += k << 2;
	}
      if (m & 4)
	{
	  IFLOAT *BO = B;
	  v4sf_t *rowC;
	  __vector_quad acc0;
	  v4sf_t result[4];
	  BLASLONG l = 0;
	  __builtin_mma_xxsetaccz (&acc0);
	  for (l = 0; l < k / 2; l++)
	    {
	      vec_t *rowA = (vec_t *) & (AO[l << 3]);
	      vec_t *rowB = (vec_t *) & (BO[l << 3]);
	      MMA (&acc0, rowB[0], rowA[0]);
	    }
	  if (k % 2 == 1)
	    {
	      if (k > 1)
		l = (k / 2) << 3;
	      vector short rowA =
		{ AO[l], 0, AO[l + 1], 0, AO[l + 2], 0, AO[l + 3], 0 };
	      vector short rowB_mrg =
		{ BO[l], 0, BO[l + 1], 0, BO[l + 2], 0, BO[l + 3], 0 };
	      MMA (&acc0, (vec_t)(rowB_mrg), (vec_t) rowA);
	    }
	  SAVE_ACC (&acc0, 0);
	  CO += 4;
	  AO += k << 2;
	  BO += k << 2;
	}
      if (m & 2)
	{
	  IFLOAT *BO = B;
	  v2sf_t *rowC;
	  v2sf_t result[8];
	  __vector_quad acc0;
	  BLASLONG l = 0;
	  __builtin_mma_xxsetaccz (&acc0);
	  for (l = 0; l < k / 2; l++)
	    {
	      vector short rowA =
		{ AO[(l << 2) + 0], AO[(l << 2) + 2], AO[(l << 2) + 1],
		AO[(l << 2) + 3],
		0, 0, 0, 0
	      };
	      vec_t *rowB = (vec_t *) & (BO[l << 3]);
	      MMA (&acc0, rowB[0], (vec_t) rowA);
	    }
	  if (k % 2 == 1)
	    {
	      if (k > 1)
		l = (k / 2) << 2;
	      vector short rowA = { AO[l], 0, AO[l + 1], 0, 0, 0, 0, 0 };
	      vector short rowB_mrg =
		{ BO[(l<<1)], 0, BO[(l<<1) + 1], 0, BO[(l<<1) + 2], 0,
		BO[(l<<1) + 3], 0
	      };
	      MMA (&acc0, (vec_t)(rowB_mrg), (vec_t) rowA);
	    }
	  SAVE4x2_ACC (&acc0, 0);
	  CO += 2;
	  AO += k << 1;
	  BO += k << 2;
	}
      if (m & 1)
	{
	  IFLOAT *BO = B;
	  v4sf_t result[4], res[4];
	  __vector_quad acc0;
	  BLASLONG l = 0;
	  __builtin_mma_xxsetaccz (&acc0);
	  for (l = 0; l < k / 2; l++)
	    {
	      vector short rowA =
		{ AO[(l << 1) + 0], AO[(l << 1) + 1], 0,
		0, 0, 0, 0
	      };
	      vec_t *rowB = (vec_t *) & (BO[l << 3]);
	      MMA (&acc0, rowB[0], (vec_t) rowA);
	    }
	  if (k % 2 == 1)
	    {
	      if (k > 1)
		l = (k / 2) << 1;
	      vector short rowA = { AO[l], 0, 0, 0, 0, 0, 0, 0 };
	      vector short rowB_mrg =
		{ BO[(l<<2) + 0], 0, BO[(l<<2) + 1], 0, BO[(l <<2) + 2], 0,
		BO[(l<<2) + 3], 0
	      };
	      MMA (&acc0, (vec_t)(rowB_mrg), (vec_t) rowA);
	    }
	  SAVE4x2_ACC_SCALAR (&acc0);
	  AO += k;
	  BO += (k << 2);
	  CO += 1;
	}

      B += k << 2;
    }
  if (n & 2)
    {
      BLASLONG j;
      FLOAT *CO;
      IFLOAT *AO;
      CO = C;
      C += ldc << 1;
      AO = A;
      /* Loop for m >= 32. */
      for (j = 0; j < (m >> 5); j++)
	{
	  IFLOAT *BO = B;
	  v4sf_t *rowC;
	  v4sf_t result[4];
	  IFLOAT *A1 = AO + (16 * k);
	  __vector_quad acc0, acc1, acc2, acc3, acc4, acc5, acc6, acc7;
	  SET_ACC_ZERO8 ();
	  BLASLONG l = 0;
	  for (l = 0; l < k / 2; l++)
	    {
	      vector short rowB =
		{ BO[(l << 2) + 0], BO[(l << 2) + 2], BO[(l << 2) + 1],
		BO[(l << 2) + 3],
		0, 0, 0, 0
	      };
	      vec_t *rowA = (vec_t *) & (AO[l << 5]);
	      vec_t *rowA1 = (vec_t *) & (A1[l << 5]);
	      MMA (&acc0, (vec_t) rowB, rowA[0]);
	      MMA (&acc1, (vec_t) rowB, rowA[1]);
	      MMA (&acc2, (vec_t) rowB, rowA[2]);
	      MMA (&acc3, (vec_t) rowB, rowA[3]);
	      MMA (&acc4, (vec_t) rowB, rowA1[0]);
	      MMA (&acc5, (vec_t) rowB, rowA1[1]);
	      MMA (&acc6, (vec_t) rowB, rowA1[2]);
	      MMA (&acc7, (vec_t) rowB, rowA1[3]);
	    }
	  if (k % 2 == 1)
	    {
	      if (k > 1)
		l = (k / 2) << 2;
	      vector short rowB = { BO[l + 0], 0, BO[l + 1], 0, 0, 0, 0, 0 };
	      vec_t *rowA = (vec_t *) & (AO[l << 3]);
	      vec_t *rowA1 = (vec_t *) & (A1[l << 3]);
	      MMA (&acc0, (vec_t) rowB, MERGE_HIGH (rowA[0], vzero));
	      MMA (&acc1, (vec_t) rowB, MERGE_LOW (rowA[0], vzero));
	      MMA (&acc2, (vec_t) rowB, MERGE_HIGH (rowA[1], vzero));
	      MMA (&acc3, (vec_t) rowB, MERGE_LOW (rowA[1], vzero));
	      MMA (&acc4, (vec_t) rowB, MERGE_HIGH (rowA1[0], vzero));
	      MMA (&acc5, (vec_t) rowB, MERGE_LOW (rowA1[0], vzero));
	      MMA (&acc6, (vec_t) rowB, MERGE_HIGH (rowA1[1], vzero));
	      MMA (&acc7, (vec_t) rowB, MERGE_LOW (rowA1[1], vzero));
	    }
	  SAVE2x4_ACC (&acc0, 0);
	  SAVE2x4_ACC (&acc1, 4);
	  SAVE2x4_ACC (&acc2, 8);
	  SAVE2x4_ACC (&acc3, 12);
	  CO += 16;
	  SAVE2x4_ACC (&acc4, 0);
	  SAVE2x4_ACC (&acc5, 4);
	  SAVE2x4_ACC (&acc6, 8);
	  SAVE2x4_ACC (&acc7, 12);
	  CO += 16;
	  AO += k << 5;
	  BO += k << 1;
	}
      if (m & 16)
	{
	  IFLOAT *BO = B;
	  v4sf_t *rowC;
	  v4sf_t result[4];
	  __vector_quad acc0, acc1, acc2, acc3;
	  SET_ACC_ZERO4 ();
	  BLASLONG l = 0;
	  for (l = 0; l < k / 2; l++)
	    {
	      vector short rowB =
		{ BO[(l << 2) + 0], BO[(l << 2) + 2], BO[(l << 2) + 1],
		BO[(l << 2) + 3],
		0, 0, 0, 0
	      };
	      vec_t *rowA = (vec_t *) & (AO[l << 5]);
	      MMA (&acc0, (vec_t) rowB, rowA[0]);
	      MMA (&acc1, (vec_t) rowB, rowA[1]);
	      MMA (&acc2, (vec_t) rowB, rowA[2]);
	      MMA (&acc3, (vec_t) rowB, rowA[3]);
	    }
	  if (k % 2 == 1)
	    {
	      if (k > 1)
		l = (k / 2) << 2;
	      vector short rowB = { BO[l + 0], 0, BO[l + 1], 0, 0, 0, 0, 0 };
	      vec_t *rowA = (vec_t *) & (AO[l << 3]);
	      MMA (&acc0, (vec_t) rowB, MERGE_HIGH (rowA[0], vzero ));
	      MMA (&acc1, (vec_t) rowB, MERGE_LOW (rowA[0], vzero));
	      MMA (&acc2, (vec_t) rowB, MERGE_HIGH (rowA[1], vzero));
	      MMA (&acc3, (vec_t) rowB, MERGE_LOW (rowA[1], vzero));
	    }
	  SAVE2x4_ACC (&acc0, 0);
	  SAVE2x4_ACC (&acc1, 4);
	  SAVE2x4_ACC (&acc2, 8);
	  SAVE2x4_ACC (&acc3, 12);
	  CO += 16;
	  AO += k << 4;
	  BO += k << 1;
	}
      if (m & 8)
	{
	  IFLOAT *BO = B;
	  v4sf_t *rowC;
	  v4sf_t result[4];
	  __vector_quad acc0, acc1;
	  __builtin_mma_xxsetaccz (&acc0);
	  __builtin_mma_xxsetaccz (&acc1);
	  BLASLONG l = 0;
	  for (l = 0; l < k / 2; l++)
	    {
	      vector short rowB =
		{ BO[(l << 2) + 0], BO[(l << 2) + 2], BO[(l << 2) + 1],
		BO[(l << 2) + 3],
		0, 0, 0, 0
	      };
	      vec_t *rowA = (vec_t *) & (AO[l << 4]);
	      MMA (&acc0, (vec_t) rowB, rowA[0]);
	      MMA (&acc1, (vec_t) rowB, rowA[1]);
	    }
	  if (k % 2 == 1)
	    {
	      if (k > 1)
		l = (k / 2) << 2;
	      vector short rowB = { BO[l + 0], 0, BO[l + 1], 0, 0, 0, 0, 0 };
	      vec_t *rowA = (vec_t *) & (AO[(l << 2)]);
	      MMA (&acc0, (vec_t) rowB, MERGE_HIGH (rowA[0], vzero));
	      MMA (&acc1, (vec_t) rowB, MERGE_LOW (rowA[0], vzero));
	    }
	  SAVE2x4_ACC (&acc0, 0);
	  SAVE2x4_ACC (&acc1, 4);
	  CO += 8;
	  AO += k << 3;
	  BO += k << 1;
	}
      if (m & 4)
	{
	  IFLOAT *BO = B;
	  v4sf_t *rowC;
	  v4sf_t result[4];
	  __vector_quad acc0;
	  __builtin_mma_xxsetaccz (&acc0);
	  BLASLONG l = 0;
	  for (l = 0; l < k / 2; l++)
	    {
	      vector short rowB =
		{ BO[(l << 2) + 0], BO[(l << 2) + 2], BO[(l << 2) + 1],
		BO[(l << 2) + 3],
		0, 0, 0, 0
	      };
	      vec_t *rowA = (vec_t *) & (AO[l << 3]);
	      MMA (&acc0, (vec_t) rowB, rowA[0]);
	    }
	  if (k % 2 == 1)
	    {
	      if (k > 1)
		l = (k / 2) << 2;
	      vector short rowB = { BO[l + 0], 0, BO[l + 1], 0, 0, 0, 0, 0 };
	      vector short rowA =
	        { AO[(l << 1)], 0, AO[(l << 1) + 1] , 0 , AO[(l<<1) + 2],
	        0, AO[(l << 1) + 3], 0 };
	      MMA (&acc0, (vec_t) rowB, (vec_t)(rowA));
	    }
	  SAVE2x4_ACC (&acc0, 0);
	  CO += 4;
	  AO += k << 2;
	  BO += k << 1;
	}
      if (m & 2)
	{
	  IFLOAT *BO = B;
	  BLASLONG l = 0;
	  v4sf_t t = { 0, 0, 0, 0 };
	  for (l = 0; l < (k << 1); l += 2)
	    {
	      v4sf_t rowA =
		{ BF16TOF32 (AO[l]), BF16TOF32 (AO[l]), BF16TOF32 (AO[l + 1]),
		BF16TOF32 (AO[l + 1])
	      };
	      v4sf_t rowB =
		{ BF16TOF32 (BO[l]), BF16TOF32 (BO[l + 1]), BF16TOF32 (BO[l]),
		BF16TOF32 (BO[l + 1])
	      };
	      t += rowA * rowB;
	    }
	  t = t * valpha;
	  CO[0 * ldc] += t[0];
	  CO[1 * ldc] += t[1];
	  CO[0 * ldc + 1] += t[2];
	  CO[1 * ldc + 1] += t[3];
	  CO += 2;
	  AO += k << 1;
	  BO += k << 1;
	}
      if (m & 1)
	{
	  IFLOAT *BO = B;
	  BLASLONG l = 0;
	  v4sf_t t = { 0, 0, 0, 0 };
	  for (l = 0; l < k; l++)
	    {
	      v4sf_t rowA = { BF16TOF32 (AO[l]), BF16TOF32 (AO[l]), 0, 0 };
	      v4sf_t rowB =
		{ BF16TOF32 (BO[l << 1]), BF16TOF32 (BO[(l << 1) + 1]), 0,
		0
	      };
	      t += rowA * rowB;
	    }
	  CO[0 * ldc] += t[0] * alpha;
	  CO[1 * ldc] += t[1] * alpha;
	  CO += 1;
	  AO += k;
	  BO += k << 1;
	}
      B += k << 1;
    }
  if (n & 1)
    {
      BLASLONG j;
      FLOAT *CO;
      IFLOAT *AO;
      CO = C;
      C += ldc;
      AO = A;
      /* Loop for m >= 16. */
      for (j = 0; j < (m >> 4); j++)
	{
	  IFLOAT *BO = B;
	  v4sf_t *rowC;
	  v4sf_t result[4];
	  __vector_quad acc0, acc1, acc2, acc3;
	  SET_ACC_ZERO4 ();
	  BLASLONG l = 0;
	  for (l = 0; l < k / 2; l++)
	    {
	      vector short rowB =
		{ BO[l << 1], BO[(l << 1) + 1], 0, 0, 0, 0, 0, 0};
	      vec_t *rowA = (vec_t *) & (AO[l << 5]);
	      MMA (&acc0, (vec_t) rowB, rowA[0]);
	      MMA (&acc1, (vec_t) rowB, rowA[1]);
	      MMA (&acc2, (vec_t) rowB, rowA[2]);
	      MMA (&acc3, (vec_t) rowB, rowA[3]);
	    }
	  if (k % 2 == 1)
	    {
	      if (k > 1)
		l = (k / 2) << 1;
	      vector short rowB = { BO[l], 0, 0, 0, 0, 0, 0, 0 };
	      vec_t *rowA = (vec_t *) & (AO[(l << 4)]);
	      MMA (&acc0, (vec_t) rowB, MERGE_HIGH (rowA[0], vzero));
	      MMA (&acc1, (vec_t) rowB, MERGE_LOW (rowA[0], vzero));
	      MMA (&acc2, (vec_t) rowB, MERGE_HIGH (rowA[1], vzero));
	      MMA (&acc3, (vec_t) rowB, MERGE_LOW (rowA[1], vzero));
	    }
	  rowC = (v4sf_t *) &CO[0];
	  __builtin_mma_disassemble_acc ((void *)result, &acc0);
          rowC[0] += result[0] * alpha;
	  __builtin_mma_disassemble_acc ((void *)result, &acc1);
          rowC[1] += result[0] * alpha;
	  __builtin_mma_disassemble_acc ((void *)result, &acc2);
          rowC[2] += result[0] * alpha;
	  __builtin_mma_disassemble_acc ((void *)result, &acc3);
          rowC[3] += result[0] * alpha;
	  AO += k << 4;
	  BO += k;
	  CO += 16;
	}
      /* Loop for m >= 8. */
      if (m & 8)
	{
	  IFLOAT *BO = B;
	  v4sf_t *rowC;
	  v4sf_t result[4];
	  __vector_quad acc0, acc1;
	  __builtin_mma_xxsetaccz (&acc0);
	  __builtin_mma_xxsetaccz (&acc1);
	  BLASLONG l = 0;
	  for (l = 0; l < k / 2; l++)
	    {
	      vector short rowB =
		{ BO[l << 1], BO[(l << 1) + 1], 0, 0, 0, 0, 0, 0};
	      vec_t *rowA = (vec_t *) & (AO[l << 4]);
	      MMA (&acc0, (vec_t) rowB, rowA[0]);
	      MMA (&acc1, (vec_t) rowB, rowA[1]);
	    }
	  if (k % 2 == 1)
	    {
	      if (k > 1)
		l = (k / 2) << 1;
	      vector short rowB = { BO[l], 0, 0, 0, 0, 0, 0, 0 };
	      vec_t *rowA = (vec_t *) & (AO[(l << 3)]);
	      MMA (&acc0, (vec_t) rowB, MERGE_HIGH (rowA[0], vzero));
	      MMA (&acc1, (vec_t) rowB, MERGE_LOW (rowA[0], vzero));
	    }
	  rowC = (v4sf_t *) &CO[0];
	  __builtin_mma_disassemble_acc ((void *)result, &acc0);
          rowC[0] += result[0] * alpha;
	  __builtin_mma_disassemble_acc ((void *)result, &acc1);
          rowC[1] += result[0] * alpha;
	  AO += k << 3;
	  BO += k;
	  CO += 8;
	}
      /* Loop for m >= 4. */
      if (m & 4)
	{
	  IFLOAT *BO = B;
	  v4sf_t *rowC;
	  v4sf_t result[4];
	  __vector_quad acc0;
	  __builtin_mma_xxsetaccz (&acc0);
	  BLASLONG l = 0;
	  for (l = 0; l < k / 2; l++)
	    {
	      vector short rowB =
		{ BO[l << 1], BO[(l << 1) + 1], 0, 0, 0, 0, 0, 0};
	      vec_t *rowA = (vec_t *) & (AO[l << 3]);
	      MMA (&acc0, (vec_t) rowB, rowA[0]);
	    }
	  if (k % 2 == 1)
	    {
	      if (k > 1)
		l = (k / 2) << 1;
	      vector short rowB = { BO[l], 0, 0, 0, 0, 0, 0, 0 };
	      vector short rowA =
	        { AO[(l << 2)], 0, AO[(l << 2) + 1] , 0 ,
		AO[(l << 2) + 2], 0, AO[(l << 2) + 3], 0 };
	      MMA (&acc0, (vec_t) rowB, (vec_t)(rowA));
	    }
	  rowC = (v4sf_t *) &CO[0];
	  __builtin_mma_disassemble_acc ((void *)result, &acc0);
          rowC[0] += result[0] * alpha;
	  AO += k << 2;
	  BO += k;
	  CO += 4;
	}
      /* Loop for m >= 2. */
      if (m & 2)
	{
	  IFLOAT *BO = B;
	  BLASLONG l = 0;
	  v4sf_t t = { 0, 0, 0, 0 };
	  for (l = 0; l < k; l++)
	    {
	      v4sf_t rowB = { BF16TOF32 (BO[l]), BF16TOF32 (BO[l]), 0, 0 };
	      v4sf_t rowA =
		{ BF16TOF32 (AO[l << 1]), BF16TOF32 (AO[(l << 1) + 1]), 0,
		0
	      };
	      t += rowA * rowB;
	    }
	  t = t * valpha;
	  CO[0] += t[0];
	  CO[1] += t[1];
	  AO += k << 1;
	  BO += k;
	  CO += 2;
	}
      /* Loop for m = 1. */
      if (m & 1)
	{
	  IFLOAT *BO = B;
	  BLASLONG l = 0;
	  FLOAT t = 0;
	  for (l = 0; l < k; l++)
	    {
	      t += BF16TOF32 (AO[l]) * BF16TOF32 (BO[l]);
	    }
	  AO += k;
	  BO += k;
	  CO[0] += t * alpha;
	  CO += 1;
	}

      B += k;
    }

  return 0;
}
