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

typedef __vector unsigned char  vec_t;
typedef FLOAT v4sf_t __attribute__ ((vector_size (16)));
typedef FLOAT v2sf_t __attribute__ ((vector_size (8)));
#if defined(TRMMKERNEL)
#define SAVE_ACC(ACC, J)  \
	  __builtin_mma_disassemble_acc ((void *)result, ACC); \
	  rowC = (v4sf_t *) &CO[0* ldc+J]; \
          rowC[0] = result[0] * alpha; \
          rowC = (v4sf_t *) &CO[1*ldc+J]; \
          rowC[0] = result[1] * alpha; \
          rowC = (v4sf_t *) &CO[2*ldc+J]; \
          rowC[0] = result[2] * alpha; \
          rowC = (v4sf_t *) &CO[3*ldc+J]; \
          rowC[0] = result[3] * alpha;
#define SAVE_ACC1(ACC, J)  \
	  __builtin_mma_disassemble_acc ((void *)result, ACC); \
	  rowC = (v4sf_t *) &CO[4* ldc+J]; \
          rowC[0] = result[0] * alpha; \
          rowC = (v4sf_t *) &CO[5*ldc+J]; \
          rowC[0] = result[1] * alpha; \
          rowC = (v4sf_t *) &CO[6*ldc+J]; \
          rowC[0] = result[2] * alpha; \
          rowC = (v4sf_t *) &CO[7*ldc+J]; \
          rowC[0] = result[3] * alpha;
#define  SAVE4x2_ACC(ACC, J)  \
	  __builtin_mma_disassemble_acc ((void *)result, ACC); \
	  rowC = (v2sf_t *) &CO[0* ldc+J]; \
          rowC[0] = result[0] * alpha; \
	  rowC = (v2sf_t *) &CO[1* ldc+J]; \
          rowC[0] = result[2] * alpha; \
	  rowC = (v2sf_t *) &CO[2* ldc+J]; \
          rowC[0] = result[4] * alpha; \
	  rowC = (v2sf_t *) &CO[3* ldc+J]; \
          rowC[0] = result[6] * alpha;
#define  SAVE4x2_ACC1(ACC, J)  \
	  __builtin_mma_disassemble_acc ((void *)result, ACC); \
	  rowC = (v2sf_t *) &CO[4* ldc+J]; \
          rowC[0] = result[0] * alpha; \
	  rowC = (v2sf_t *) &CO[5* ldc+J]; \
          rowC[0] = result[2] * alpha; \
	  rowC = (v2sf_t *) &CO[6* ldc+J]; \
          rowC[0] = result[4] * alpha; \
	  rowC = (v2sf_t *) &CO[7* ldc+J]; \
          rowC[0] = result[6] * alpha;
#define  SAVE2x4_ACC(ACC, J)  \
	  __builtin_mma_disassemble_acc ((void *)result, ACC); \
	  rowC = (v4sf_t *) &CO[0* ldc+J]; \
          rowC[0] = result[0] * alpha; \
	  rowC = (v4sf_t *) &CO[1* ldc+J]; \
          rowC[0] = result[1] * alpha;
#else
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
#define  SAVE2x4_ACC(ACC, J)  \
	  __builtin_mma_disassemble_acc ((void *)result, ACC); \
	  rowC = (v4sf_t *) &CO[0* ldc+J]; \
          rowC[0] += result[0] * alpha; \
	  rowC = (v4sf_t *) &CO[1* ldc+J]; \
          rowC[0] += result[1] * alpha;
#endif
#define KERNEL(i, j) \
          __builtin_mma_xvf32gerpp (&acc0, rowB[i], rowA[j]); \
          __builtin_mma_xvf32gerpp (&acc1, rowB[i+1], rowA[j]); \
          __builtin_mma_xvf32gerpp (&acc2, rowB[i], rowA[j+1]); \
          __builtin_mma_xvf32gerpp (&acc3, rowB[i+1], rowA[j+1]); \
          __builtin_mma_xvf32gerpp (&acc4, rowB[i], rowA[j+2]); \
          __builtin_mma_xvf32gerpp (&acc5, rowB[i+1], rowA[j+2]); \
          __builtin_mma_xvf32gerpp (&acc6, rowB[i], rowA[j+3]); \
          __builtin_mma_xvf32gerpp (&acc7, rowB[i+1], rowA[j+3]);

#define PREFETCH1(x, y) asm volatile ("dcbt %0, %1" : : "r" (x), "b" (y) : "memory");

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
#define REFRESH_TEMP_BK(x, y) \
            temp = k - off;
#elif defined(LEFT)
#define REFRESH_TEMP_BK(x, y) \
            temp = off + x;
#else
#define REFRESH_TEMP_BK(x, y) \
            temp = off + y;
#endif
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
#define REFRESH_POINTERS(x, y) \
	  BO = B; \
          REFRESH_TEMP_BK(x, y)
#else
#define REFRESH_POINTERS(x, y) \
          AO += off * x; \
          BO = B + off * y; \
          REFRESH_TEMP_BK(x, y)
#endif

#ifdef LEFT
#define REFRESH_OFF(x) \
            off += x;
#else
#define REFRESH_OFF(x)
#endif

#ifdef LEFT
#define UPDATE_TEMP(x, y) \
            temp -= x;
#else
#define UPDATE_TEMP(x, y) \
            temp -= y;
#endif

#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
#define REFRESH_TMP_AFTER_SAVE(x, y) \
            temp = k - off; \
            UPDATE_TEMP(x, y) \
            AO += temp * x; \
            BO += temp * y;
#else
#define REFRESH_TMP_AFTER_SAVE(x, y)
#endif

#define REFRESH_AFTER_SAVE(x,y) \
        REFRESH_TMP_AFTER_SAVE(x, y) \
	REFRESH_OFF(x)
/*************************************************************************************
* GEMM Kernel
*************************************************************************************/
int
CNAME (BLASLONG m, BLASLONG n, BLASLONG k, FLOAT alpha, FLOAT * A, FLOAT * B,
       FLOAT * C, BLASLONG ldc
#ifdef TRMMKERNEL
       , BLASLONG offset
#endif
  )
{
  BLASLONG i1;
#if defined(TRMMKERNEL)
  BLASLONG off;
#endif
#if defined(TRMMKERNEL) && !defined(LEFT)
  off = -offset;
#endif

  v4sf_t valpha = { alpha, alpha, alpha, alpha };
  for (i1 = 0; i1 < (n >> 3); i1++)
    {
      BLASLONG j, temp;
      FLOAT *CO;
      FLOAT *AO;
#if defined(TRMMKERNEL) && defined(LEFT)
      off = offset;
#endif
      CO = C;
      C += ldc << 3;
      AO = A;
      PREFETCH1 (A, 128);
      PREFETCH1 (A, 256);
      for (j = 0; j < (m >> 4); j++)
	{
	  FLOAT *BO;
#if defined(TRMMKERNEL)
	  REFRESH_POINTERS (16, 8);
#else
	  BO = B;
	  temp = k;
#endif
	  v4sf_t *rowC;
	  v4sf_t result[4];
	  __vector_quad acc0, acc1, acc2, acc3, acc4, acc5, acc6, acc7;
	  BLASLONG l = 0;
	  vec_t *rowA1 = (vec_t *) & AO[0];
	  vec_t *rowB1 = (vec_t *) & BO[0];
          __builtin_mma_xvf32ger (&acc0, rowB1[0], rowA1[0]);
          __builtin_mma_xvf32ger (&acc1, rowB1[1], rowA1[0]);
          __builtin_mma_xvf32ger (&acc2, rowB1[0], rowA1[1]);
          __builtin_mma_xvf32ger (&acc3, rowB1[1], rowA1[1]);
          __builtin_mma_xvf32ger (&acc4, rowB1[0], rowA1[2]);
          __builtin_mma_xvf32ger (&acc5, rowB1[1], rowA1[2]);
          __builtin_mma_xvf32ger (&acc6, rowB1[0], rowA1[3]);
          __builtin_mma_xvf32ger (&acc7, rowB1[1], rowA1[3]);
	  AO += 16;
	  BO += 8;
	  temp--;
	  BLASLONG K = temp / 64;
	  for (l = 0; l < K; l++)
	    {
	      vec_t *rowA = (vec_t *) & AO[0];
	      vec_t *rowB = (vec_t *) & BO[0];
	      KERNEL (0, 0);
	      KERNEL (2, 4);
	      KERNEL (4, 8);
	      KERNEL (6, 12);
	      KERNEL (8, 16);
	      KERNEL (10, 20);
	      KERNEL (12, 24);
	      KERNEL (14, 28);
	      KERNEL (16, 32);
	      KERNEL (18, 36);
	      KERNEL (20, 40);
	      KERNEL (22, 44);
	      KERNEL (24, 48);
	      KERNEL (26, 52);
	      KERNEL (28, 56);
	      KERNEL (30, 60);
	      KERNEL (32, 64);
	      KERNEL (34, 68);
	      KERNEL (36, 72);
	      KERNEL (38, 76);
	      KERNEL (40, 80);
	      KERNEL (42, 84);
	      KERNEL (44, 88);
	      KERNEL (46, 92);
	      KERNEL (48, 96);
	      KERNEL (50, 100);
	      KERNEL (52, 104);
	      KERNEL (54, 108);
	      KERNEL (56, 112);
	      KERNEL (58, 116);
	      KERNEL (60, 120);
	      KERNEL (62, 124);
	      KERNEL (64, 128);
	      KERNEL (66, 132);
	      KERNEL (68, 136);
	      KERNEL (70, 140);
	      KERNEL (72, 144);
	      KERNEL (74, 148);
	      KERNEL (76, 152);
	      KERNEL (78, 156);
	      KERNEL (80, 160);
	      KERNEL (82, 164);
	      KERNEL (84, 168);
	      KERNEL (86, 172);
	      KERNEL (88, 176);
	      KERNEL (90, 180);
	      KERNEL (92, 184);
	      KERNEL (94, 188);
	      KERNEL (96, 192);
	      KERNEL (98, 196);
	      KERNEL (100, 200);
	      KERNEL (102, 204);
	      KERNEL (104, 208);
	      KERNEL (106, 212);
	      KERNEL (108, 216);
	      KERNEL (110, 220);
	      KERNEL (112, 224);
	      KERNEL (114, 228);
	      KERNEL (116, 232);
	      KERNEL (118, 236);
	      KERNEL (120, 240);
	      KERNEL (122, 244);
	      KERNEL (124, 248);
	      KERNEL (126, 252);
	      AO += 1024;
	      BO += 512;
	    }
	  if ((temp & 63) >> 5)
	    {
	      vec_t *rowA = (vec_t *) & AO[0];
	      vec_t *rowB = (vec_t *) & BO[0];
	      KERNEL (0, 0);
	      KERNEL (2, 4);
	      KERNEL (4, 8);
	      KERNEL (6, 12);
	      KERNEL (8, 16);
	      KERNEL (10, 20);
	      KERNEL (12, 24);
	      KERNEL (14, 28);
	      KERNEL (16, 32);
	      KERNEL (18, 36);
	      KERNEL (20, 40);
	      KERNEL (22, 44);
	      KERNEL (24, 48);
	      KERNEL (26, 52);
	      KERNEL (28, 56);
	      KERNEL (30, 60);
	      KERNEL (32, 64);
	      KERNEL (34, 68);
	      KERNEL (36, 72);
	      KERNEL (38, 76);
	      KERNEL (40, 80);
	      KERNEL (42, 84);
	      KERNEL (44, 88);
	      KERNEL (46, 92);
	      KERNEL (48, 96);
	      KERNEL (50, 100);
	      KERNEL (52, 104);
	      KERNEL (54, 108);
	      KERNEL (56, 112);
	      KERNEL (58, 116);
	      KERNEL (60, 120);
	      KERNEL (62, 124);
	      AO += 512;
	      BO += 256;
	    }
	  if ((temp & 31) >> 4)
	    {
	      vec_t *rowA = (vec_t *) & AO[0];
	      vec_t *rowB = (vec_t *) & BO[0];
	      KERNEL (0, 0);
	      KERNEL (2, 4);
	      KERNEL (4, 8);
	      KERNEL (6, 12);
	      KERNEL (8, 16);
	      KERNEL (10, 20);
	      KERNEL (12, 24);
	      KERNEL (14, 28);
	      KERNEL (16, 32);
	      KERNEL (18, 36);
	      KERNEL (20, 40);
	      KERNEL (22, 44);
	      KERNEL (24, 48);
	      KERNEL (26, 52);
	      KERNEL (28, 56);
	      KERNEL (30, 60);
	      AO += 256;
	      BO += 128;
	    }
	  if ((temp & 15) >> 3)
	    {
	      vec_t *rowA = (vec_t *) & AO[0];
	      vec_t *rowB = (vec_t *) & BO[0];
	      KERNEL (0, 0);
	      KERNEL (2, 4);
	      KERNEL (4, 8);
	      KERNEL (6, 12);
	      KERNEL (8, 16);
	      KERNEL (10, 20);
	      KERNEL (12, 24);
	      KERNEL (14, 28);
	      AO += 128;
	      BO += 64;
	    }
	  if ((temp & 7) >> 2)
	    {
	      vec_t *rowA = (vec_t *) & AO[0];
	      vec_t *rowB = (vec_t *) & BO[0];
	      KERNEL (0, 0);
	      KERNEL (2, 4);
	      KERNEL (4, 8);
	      KERNEL (6, 12);
	      AO += 64;
	      BO += 32;
	    }
	  if ((temp & 3) >> 1)
	    {
	      vec_t *rowA = (vec_t *) & AO[0];
	      vec_t *rowB = (vec_t *) & BO[0];
	      KERNEL (0, 0);
	      KERNEL (2, 4);
	      AO += 32;
	      BO += 16;
	    }
	  if ((temp & 1) >> 0)
	    {
	      vec_t *rowA = (vec_t *) & AO[0];
	      vec_t *rowB = (vec_t *) & BO[0];
	      KERNEL (0, 0);
	      AO += 16;
	      BO += 8;
	    }
	  SAVE_ACC (&acc0, 0);
	  SAVE_ACC (&acc2, 4);
	  SAVE_ACC1 (&acc1, 0);
	  SAVE_ACC1 (&acc3, 4);
	  SAVE_ACC (&acc4, 8);
	  SAVE_ACC (&acc6, 12);
	  SAVE_ACC1 (&acc5, 8);
	  SAVE_ACC1 (&acc7, 12);
#if defined(TRMMKERNEL)
	  REFRESH_AFTER_SAVE (16, 8)
#endif
	    CO += 16;
	}
      if (m & 8)
	{
	  FLOAT *BO;
#if defined(TRMMKERNEL)
	  REFRESH_POINTERS (8, 8);
#else
	  BO = B;
	  temp = k;
#endif
	  v4sf_t *rowC;
	  v4sf_t result[4];
	  __vector_quad acc0, acc1, acc2, acc3;
	  BLASLONG l = 0;
	  vec_t *rowA = (vec_t *) & AO[0];
	  vec_t *rowB = (vec_t *) & BO[0];
	  __builtin_mma_xvf32ger (&acc0, rowB[0], rowA[0]);
	  __builtin_mma_xvf32ger (&acc1, rowB[1], rowA[0]);
	  __builtin_mma_xvf32ger (&acc2, rowB[0], rowA[1]);
	  __builtin_mma_xvf32ger (&acc3, rowB[1], rowA[1]);
	  for (l = 1; l < temp; l++)
	    {
	      rowA = (vec_t *) & AO[l << 3];
	      rowB = (vec_t *) & BO[l << 3];
	      __builtin_mma_xvf32gerpp (&acc0, rowB[0], rowA[0]);
	      __builtin_mma_xvf32gerpp (&acc1, rowB[1], rowA[0]);
	      __builtin_mma_xvf32gerpp (&acc2, rowB[0], rowA[1]);
	      __builtin_mma_xvf32gerpp (&acc3, rowB[1], rowA[1]);
	    }
	  SAVE_ACC (&acc0, 0);
	  SAVE_ACC (&acc2, 4);
	  SAVE_ACC1 (&acc1, 0);
	  SAVE_ACC1 (&acc3, 4);
	  AO += (temp << 3);
	  BO += (temp << 3);
	  CO += 8;
#if defined(TRMMKERNEL)
	  REFRESH_AFTER_SAVE (8, 8)
#endif
	}
      if (m & 4)
	{
	  FLOAT *BO;
#if defined(TRMMKERNEL)
	  REFRESH_POINTERS (4, 8);
#else
	  BO = B;
	  temp = k;
#endif
	  v4sf_t *rowC;
	  v4sf_t result[4];
	  __vector_quad acc0, acc1;
	  BLASLONG l = 0;
	  vec_t *rowA = (vec_t *) & AO[0];
	  vec_t *rowB = (vec_t *) & BO[0];
	  __builtin_mma_xvf32ger (&acc0, rowB[0], rowA[0]);
	  __builtin_mma_xvf32ger (&acc1, rowB[1], rowA[0]);
	  for (l = 1; l < temp; l++)
	    {
	      rowA = (vec_t *) & AO[l << 2];
	      rowB = (vec_t *) & BO[l << 3];
	      __builtin_mma_xvf32gerpp (&acc0, rowB[0], rowA[0]);
	      __builtin_mma_xvf32gerpp (&acc1, rowB[1], rowA[0]);
	    }
	  SAVE_ACC (&acc0, 0);
	  SAVE_ACC1 (&acc1, 0);
	  CO += 4;
	  AO += (temp << 2);
	  BO += (temp << 3);
#if defined(TRMMKERNEL)
	  REFRESH_AFTER_SAVE (4, 8)
#endif
	}
      if (m & 2)
	{
	  FLOAT *BO;
#if defined(TRMMKERNEL)
	  REFRESH_POINTERS (2, 8);
#else
	  BO = B;
	  temp = k;
#endif

	  v2sf_t *rowC;
	  v2sf_t result[8];
	  __vector_quad acc0, acc1;
	  BLASLONG l = 0;
	  FLOAT t[4] = { 0 };
	  t[0] = AO[0], t[1] = AO[1];
	  vec_t *rowA = (vec_t *) & t[0];
	  vec_t *rowB = (vec_t *) & BO[0];
	  __builtin_mma_xvf32ger (&acc0, rowB[0], rowA[0]);
	  __builtin_mma_xvf32ger (&acc1, rowB[1], rowA[0]);
	  for (l = 1; l < temp; l++)
	    {
	      t[0] = AO[l << 1], t[1] = AO[(l << 1) + 1];
	      rowA = (vec_t *) & t[0];
	      rowB = (vec_t *) & BO[l << 3];
	      __builtin_mma_xvf32gerpp (&acc0, rowB[0], rowA[0]);
	      __builtin_mma_xvf32gerpp (&acc1, rowB[1], rowA[0]);
	    }
	  SAVE4x2_ACC (&acc0, 0);
	  SAVE4x2_ACC1 (&acc1, 0);
	  CO += 2;
	  AO += (temp << 1);
	  BO += (temp << 3);
#if defined(TRMMKERNEL)
	  REFRESH_AFTER_SAVE (2, 8)
#endif
	}
      if (m & 1)
	{
	  FLOAT *BO;
#if defined(TRMMKERNEL)
	  REFRESH_POINTERS (1, 8);
#else
	  BO = B;
	  temp = k;
#endif
	  BLASLONG l = 0;
	  v4sf_t t = { 0, 0, 0, 0 };
	  v4sf_t t1 = { 0, 0, 0, 0 };
	  for (l = 0; l < temp; l++)
	    {
	      v4sf_t rowA = { AO[l], AO[l], AO[l], AO[l] };
	      v4sf_t rowB = { BO[l << 3], BO[(l << 3) + 1], BO[(l << 3) + 2],
		BO[(l << 3) + 3]
	      };
	      v4sf_t rowB1 =
		{ BO[(l << 3) + 4], BO[(l << 3) + 5], BO[(l << 3) + 6],
		BO[(l << 3) + 7]
	      };
	      t += rowA * rowB;
	      t1 += rowA * rowB1;
	    }
	  t = t * valpha;
	  t1 = t1 * valpha;
#if defined(TRMMKERNEL)
	  CO[0 * ldc] = t[0];
	  CO[1 * ldc] = t[1];
	  CO[2 * ldc] = t[2];
	  CO[3 * ldc] = t[3];
	  CO[4 * ldc] = t1[0];
	  CO[5 * ldc] = t1[1];
	  CO[6 * ldc] = t1[2];
	  CO[7 * ldc] = t1[3];
#else
	  CO[0 * ldc] += t[0];
	  CO[1 * ldc] += t[1];
	  CO[2 * ldc] += t[2];
	  CO[3 * ldc] += t[3];
	  CO[4 * ldc] += t1[0];
	  CO[5 * ldc] += t1[1];
	  CO[6 * ldc] += t1[2];
	  CO[7 * ldc] += t1[3];
#endif
	  CO += 1;
	  AO += temp;
	  BO += (temp << 3);
#if defined(TRMMKERNEL)
	  REFRESH_AFTER_SAVE (1, 8)
#endif
	}
#if defined(TRMMKERNEL) && !defined(LEFT)
      off += 8;			// number of values in A
#endif

      B += k << 3;
    }
  if (n & 4)
    {
      BLASLONG i, j, temp;
#if defined(TRMMKERNEL) && defined(LEFT)
      off = offset;
#endif
      FLOAT *CO;
      FLOAT *AO;
      CO = C;
      C += ldc << 2;
      AO = A;
#if !defined(TRMMKERNEL)
      i = m >> 5;
      for (j = 0; j < i; j++)
	{
	  FLOAT *BO = B;
	  v4sf_t *rowC;
	  v4sf_t result[4];
	  FLOAT *A1;
	  A1 = AO + (16 * k);
	  __vector_quad acc0, acc1, acc2, acc3, acc4, acc5, acc6, acc7;
	  BLASLONG l = 0;
	  vec_t *rowA = (vec_t *) & AO[0];
	  vec_t *rowA1 = (vec_t *) & A1[0];
	  vec_t *rowB = (vec_t *) & BO[0];
	  __builtin_mma_xvf32ger (&acc0, rowB[0], rowA[0]);
	  __builtin_mma_xvf32ger (&acc1, rowB[0], rowA[1]);
	  __builtin_mma_xvf32ger (&acc2, rowB[0], rowA[2]);
	  __builtin_mma_xvf32ger (&acc3, rowB[0], rowA[3]);
	  __builtin_mma_xvf32ger (&acc4, rowB[0], rowA1[0]);
	  __builtin_mma_xvf32ger (&acc5, rowB[0], rowA1[1]);
	  __builtin_mma_xvf32ger (&acc6, rowB[0], rowA1[2]);
	  __builtin_mma_xvf32ger (&acc7, rowB[0], rowA1[3]);
	  for (l = 1; l < k; l++)
	    {
	      rowA = (vec_t *) & AO[l << 4];
	      rowA1 = (vec_t *) & A1[l << 4];
	      rowB = (vec_t *) & BO[l << 2];
	      __builtin_mma_xvf32gerpp (&acc0, rowB[0], rowA[0]);
	      __builtin_mma_xvf32gerpp (&acc1, rowB[0], rowA[1]);
	      __builtin_mma_xvf32gerpp (&acc2, rowB[0], rowA[2]);
	      __builtin_mma_xvf32gerpp (&acc3, rowB[0], rowA[3]);
	      __builtin_mma_xvf32gerpp (&acc4, rowB[0], rowA1[0]);
	      __builtin_mma_xvf32gerpp (&acc5, rowB[0], rowA1[1]);
	      __builtin_mma_xvf32gerpp (&acc6, rowB[0], rowA1[2]);
	      __builtin_mma_xvf32gerpp (&acc7, rowB[0], rowA1[3]);
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
      i = (m & 31) >> 4;
#else
      i = m >> 4;
#endif
      for (j = 0; j < i; j++)
	{
	  FLOAT *BO;
#if defined(TRMMKERNEL)
	  REFRESH_POINTERS (16, 4);
#else
	  BO = B;
	  temp = k;
#endif
	  v4sf_t *rowC;
	  v4sf_t result[4];
	  __vector_quad acc0, acc1, acc2, acc3;
	  BLASLONG l = 0;
	  vec_t *rowA = (vec_t *) & AO[0];
	  vec_t *rowB = (vec_t *) & BO[0];
	  __builtin_mma_xvf32ger (&acc0, rowB[0], rowA[0]);
	  __builtin_mma_xvf32ger (&acc1, rowB[0], rowA[1]);
	  __builtin_mma_xvf32ger (&acc2, rowB[0], rowA[2]);
	  __builtin_mma_xvf32ger (&acc3, rowB[0], rowA[3]);
	  for (l = 1; l < temp; l++)
	    {
	      rowA = (vec_t *) & AO[l << 4];
	      rowB = (vec_t *) & BO[l << 2];
	      __builtin_mma_xvf32gerpp (&acc0, rowB[0], rowA[0]);
	      __builtin_mma_xvf32gerpp (&acc1, rowB[0], rowA[1]);
	      __builtin_mma_xvf32gerpp (&acc2, rowB[0], rowA[2]);
	      __builtin_mma_xvf32gerpp (&acc3, rowB[0], rowA[3]);
	    }

	  SAVE_ACC (&acc0, 0);
	  SAVE_ACC (&acc1, 4);
	  CO += 8;
	  SAVE_ACC (&acc2, 0);
	  SAVE_ACC (&acc3, 4);
	  CO += 8;
	  AO += temp << 4;
	  BO += temp << 2;
#if defined(TRMMKERNEL)
	  REFRESH_AFTER_SAVE (16, 4)
#endif
	}
      if (m & 8)
	{
	  FLOAT *BO;
#if defined(TRMMKERNEL)
	  REFRESH_POINTERS (8, 4);
#else
	  BO = B;
	  temp = k;
#endif
	  v4sf_t *rowC;
	  v4sf_t result[4];
	  __vector_quad acc0, acc1;
	  BLASLONG l = 0;
	  vec_t *rowA = (vec_t *) & AO[0];
	  vec_t *rowB = (vec_t *) & BO[0];
	  __builtin_mma_xvf32ger (&acc0, rowB[0], rowA[0]);
	  __builtin_mma_xvf32ger (&acc1, rowB[0], rowA[1]);
	  for (l = 1; l < temp; l++)
	    {
	      rowA = (vec_t *) & AO[l << 3];
	      rowB = (vec_t *) & BO[l << 2];
	      __builtin_mma_xvf32gerpp (&acc0, rowB[0], rowA[0]);
	      __builtin_mma_xvf32gerpp (&acc1, rowB[0], rowA[1]);
	    }
	  SAVE_ACC (&acc0, 0);
	  SAVE_ACC (&acc1, 4);
	  CO += 8;
	  AO += temp << 3;
	  BO += temp << 2;
#if defined(TRMMKERNEL)
	  REFRESH_AFTER_SAVE (8, 4)
#endif
	}
      if (m & 4)
	{
	  FLOAT *BO;
#if defined(TRMMKERNEL)
	  REFRESH_POINTERS (4, 4);
#else
	  BO = B;
	  temp = k;
#endif
	  v4sf_t *rowC;
	  __vector_quad acc0;
	  v4sf_t result[4];
	  BLASLONG l = 0;
	  vec_t *rowA = (vec_t *) & AO[0];
	  vec_t *rowB = (vec_t *) & BO[0];
	  __builtin_mma_xvf32ger (&acc0, rowB[0], rowA[0]);
	  for (l = 1; l < temp; l++)
	    {
	      rowA = (vec_t *) & AO[l << 2];
	      rowB = (vec_t *) & BO[l << 2];
	      __builtin_mma_xvf32gerpp (&acc0, rowB[0], rowA[0]);
	    }
	  SAVE_ACC (&acc0, 0);
	  CO += 4;
	  AO += temp << 2;
	  BO += temp << 2;
#if defined(TRMMKERNEL)
	  REFRESH_AFTER_SAVE (4, 4)
#endif
	}
      if (m & 2)
	{
	  FLOAT *BO;
#if defined(TRMMKERNEL)
	  REFRESH_POINTERS (2, 4);
#else
	  BO = B;
	  temp = k;
#endif
	  v2sf_t *rowC;
	  v2sf_t result[8];
	  __vector_quad acc0;
	  BLASLONG l = 0;
	  FLOAT t[4] = { 0 };
	  t[0] = AO[0], t[1] = AO[1];
	  vec_t *rowA = (vec_t *) & t[0];
	  vec_t *rowB = (vec_t *) & BO[0];
	  __builtin_mma_xvf32ger (&acc0, rowB[0], rowA[0]);
	  for (l = 1; l < temp; l++)
	    {
	      t[0] = AO[l << 1], t[1] = AO[(l << 1) + 1];
	      rowA = (vec_t *) & t[0];
	      rowB = (vec_t *) & BO[l << 2];
	      __builtin_mma_xvf32gerpp (&acc0, rowB[0], rowA[0]);
	    }
	  SAVE4x2_ACC (&acc0, 0);
	  CO += 2;
	  AO += temp << 1;
	  BO += temp << 2;
#if defined(TRMMKERNEL)
	  REFRESH_AFTER_SAVE (2, 4)
#endif
	}
      if (m & 1)
	{
	  FLOAT *BO;
#if defined(TRMMKERNEL)
	  REFRESH_POINTERS (1, 4)
#else
	  BO = B;
	  temp = k;
#endif
	  BLASLONG l = 0;
	  v4sf_t t = { 0, 0, 0, 0 };
	  for (l = 0; l < temp; l++)
	    {
	      v4sf_t rowA = { AO[l], AO[l], AO[l], AO[l] };
	      v4sf_t rowB = { BO[l << 2], BO[(l << 2) + 1], BO[(l << 2) + 2],
		BO[(l << 2) + 3]
	      };
	      t += rowA * rowB;
	    }
	  t = t * valpha;
#if defined(TRMMKERNEL)
	  CO[0 * ldc] = t[0];
	  CO[1 * ldc] = t[1];
	  CO[2 * ldc] = t[2];
	  CO[3 * ldc] = t[3];
#else
	  CO[0 * ldc] += t[0];
	  CO[1 * ldc] += t[1];
	  CO[2 * ldc] += t[2];
	  CO[3 * ldc] += t[3];
#endif
	  CO += 1;
	  AO += temp;
	  BO += temp << 2;
#if defined(TRMMKERNEL)
	  REFRESH_AFTER_SAVE (1, 4)
#endif
	}
#if defined(TRMMKERNEL) && !defined(LEFT)
      off += 4;			// number of values in A
#endif

      B += k << 2;
    }
  if (n & 2)
    {
      BLASLONG i, j, temp;
#if defined(TRMMKERNEL) && defined(LEFT)
      off = offset;
#endif
      FLOAT *CO;
      FLOAT *AO;
      CO = C;
      C += ldc << 1;
      AO = A;
#if !defined(TRMMKERNEL)
      i = m >> 5;
      for (j = 0; j < i; j++)
	{
	  FLOAT *BO = B;
	  v4sf_t *rowC;
	  v4sf_t result[4];
	  FLOAT *A1;
	  A1 = AO + (16 * k);
	  __vector_quad acc0, acc1, acc2, acc3, acc4, acc5, acc6, acc7;
	  BLASLONG l = 0;
	  FLOAT t[4] = { 0 };
	  t[0] = BO[0], t[1] = BO[1];
	  vec_t *rowB = (vec_t *) & t[0];
	  vec_t *rowA = (vec_t *) & AO[0];
	  vec_t *rowA1 = (vec_t *) & A1[0];
	  __builtin_mma_xvf32ger (&acc0, rowB[0], rowA[0]);
	  __builtin_mma_xvf32ger (&acc1, rowB[0], rowA[1]);
	  __builtin_mma_xvf32ger (&acc2, rowB[0], rowA[2]);
	  __builtin_mma_xvf32ger (&acc3, rowB[0], rowA[3]);
	  __builtin_mma_xvf32ger (&acc4, rowB[0], rowA1[0]);
	  __builtin_mma_xvf32ger (&acc5, rowB[0], rowA1[1]);
	  __builtin_mma_xvf32ger (&acc6, rowB[0], rowA1[2]);
	  __builtin_mma_xvf32ger (&acc7, rowB[0], rowA1[3]);
	  for (l = 1; l < k; l++)
	    {
	      t[0] = BO[l << 1], t[1] = BO[(l << 1) + 1];
	      rowB = (vec_t *) & t[0];
	      rowA = (vec_t *) & AO[l << 4];
	      rowA1 = (vec_t *) & A1[l << 4];
	      __builtin_mma_xvf32gerpp (&acc0, rowB[0], rowA[0]);
	      __builtin_mma_xvf32gerpp (&acc1, rowB[0], rowA[1]);
	      __builtin_mma_xvf32gerpp (&acc2, rowB[0], rowA[2]);
	      __builtin_mma_xvf32gerpp (&acc3, rowB[0], rowA[3]);
	      __builtin_mma_xvf32gerpp (&acc4, rowB[0], rowA1[0]);
	      __builtin_mma_xvf32gerpp (&acc5, rowB[0], rowA1[1]);
	      __builtin_mma_xvf32gerpp (&acc6, rowB[0], rowA1[2]);
	      __builtin_mma_xvf32gerpp (&acc7, rowB[0], rowA1[3]);
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
      i = (m & 31) >> 4;
#else
      i = m >> 4;
#endif
      for (j = 0; j < i; j++)
	{
	  FLOAT *BO;
	  v4sf_t *rowC;
	  v4sf_t result[4];
	  __vector_quad acc0, acc1, acc2, acc3;
	  BLASLONG l = 0;
#if defined(TRMMKERNEL)
	  REFRESH_POINTERS (16, 2)
#else
	  BO = B;
	  temp = k;
#endif
	 FLOAT t[4] = { 0 };
	 t[0] = BO[0], t[1] = BO[1];
	 vec_t *rowB = (vec_t *) & t[0];
	 vec_t *rowA = (vec_t *) & AO[0];
	 __builtin_mma_xvf32ger (&acc0, rowB[0], rowA[0]);
	 __builtin_mma_xvf32ger (&acc1, rowB[0], rowA[1]);
	 __builtin_mma_xvf32ger (&acc2, rowB[0], rowA[2]);
	 __builtin_mma_xvf32ger (&acc3, rowB[0], rowA[3]);
	  for (l = 1; l < temp; l++)
	    {
	      t[0] = BO[l << 1], t[1] = BO[(l << 1) + 1];
	      rowB = (vec_t *) & t[0];
	      rowA = (vec_t *) & AO[l << 4];
	      __builtin_mma_xvf32gerpp (&acc0, rowB[0], rowA[0]);
	      __builtin_mma_xvf32gerpp (&acc1, rowB[0], rowA[1]);
	      __builtin_mma_xvf32gerpp (&acc2, rowB[0], rowA[2]);
	      __builtin_mma_xvf32gerpp (&acc3, rowB[0], rowA[3]);
	    }
	  SAVE2x4_ACC (&acc0, 0);
	  SAVE2x4_ACC (&acc1, 4);
	  SAVE2x4_ACC (&acc2, 8);
	  SAVE2x4_ACC (&acc3, 12);
	  CO += 16;
	  AO += temp << 4;
	  BO += temp << 1;
#if defined(TRMMKERNEL)
	  REFRESH_AFTER_SAVE (16, 2)
#endif
	}
      if (m & 8)
	{
	  FLOAT *BO;
	  v4sf_t *rowC;
	  v4sf_t result[4];
	  __vector_quad acc0, acc1;
#if defined(TRMMKERNEL)
	  REFRESH_POINTERS (8, 2)
#else
	  BO = B;
	  temp = k;
#endif
	  BLASLONG l = 0;
	  FLOAT t[4] = { 0 };
	  t[0] = BO[0], t[1] = BO[1];
	  vec_t *rowB = (vec_t *) & t[0];
	  vec_t *rowA = (vec_t *) & AO[0];
	  __builtin_mma_xvf32ger (&acc0, rowB[0], rowA[0]);
	  __builtin_mma_xvf32ger (&acc1, rowB[0], rowA[1]);
	  for (l = 1; l < temp; l++)
	    {
	      t[0] = BO[l << 1], t[1] = BO[(l << 1) + 1];
	      rowB = (vec_t *) & t[0];
	      rowA = (vec_t *) & AO[l << 3];
	      __builtin_mma_xvf32gerpp (&acc0, rowB[0], rowA[0]);
	      __builtin_mma_xvf32gerpp (&acc1, rowB[0], rowA[1]);
	    }
	  SAVE2x4_ACC (&acc0, 0);
	  SAVE2x4_ACC (&acc1, 4);
	  CO += 8;
	  AO += temp << 3;
	  BO += temp << 1;
#if defined(TRMMKERNEL)
	  REFRESH_AFTER_SAVE (8, 2)
#endif
	}
      if (m & 4)
	{
	  FLOAT *BO;
	  v4sf_t *rowC;
	  v4sf_t result[4];
	  __vector_quad acc0;
#if defined(TRMMKERNEL)
	  REFRESH_POINTERS (4, 2)
#else
	  BO = B;
	  temp = k;
#endif
	  BLASLONG l = 0;
	  FLOAT t[4] = { 0 };
	  t[0] = BO[0], t[1] = BO[1];
	  vec_t *rowB = (vec_t *) & t[0];
	  vec_t *rowA = (vec_t *) & AO[0];
	  __builtin_mma_xvf32ger (&acc0, rowB[0], rowA[0]);
	  for (l = 1; l < temp; l++)
	    {
	      t[0] = BO[l << 1], t[1] = BO[(l << 1) + 1];
	      rowB = (vec_t *) & t[0];
	      rowA = (vec_t *) & AO[l << 2];
	      __builtin_mma_xvf32gerpp (&acc0, rowB[0], rowA[0]);
	    }
	  SAVE2x4_ACC (&acc0, 0);
	  CO += 4;
	  AO += temp << 2;
	  BO += temp << 1;
#if defined(TRMMKERNEL)
	  REFRESH_AFTER_SAVE (4, 2)
#endif
	}
      if (m & 2)
	{
	  FLOAT *BO;
	  BLASLONG l = 0;
#if defined(TRMMKERNEL)
	  REFRESH_POINTERS (2, 2)
#else
	  BO = B;
	  temp = k;
#endif
	  v4sf_t t = { 0, 0, 0, 0 };
	  for (l = 0; l < (temp << 1); l += 2)
	    {
	      v4sf_t rowA = { AO[l], AO[l], AO[l + 1], AO[l + 1] };
	      v4sf_t rowB = { BO[l], BO[l + 1], BO[l], BO[l + 1] };
	      t += rowA * rowB;
	    }
	  t = t * valpha;
#if defined(TRMMKERNEL)
	  CO[0 * ldc] = t[0];
	  CO[1 * ldc] = t[1];
	  CO[0 * ldc + 1] = t[2];
	  CO[1 * ldc + 1] = t[3];
#else
	  CO[0 * ldc] += t[0];
	  CO[1 * ldc] += t[1];
	  CO[0 * ldc + 1] += t[2];
	  CO[1 * ldc + 1] += t[3];
#endif
	  CO += 2;
	  AO += temp << 1;
	  BO += temp << 1;
#if defined(TRMMKERNEL)
	  REFRESH_AFTER_SAVE (2, 2)
#endif
	}
      if (m & 1)
	{
	  FLOAT *BO;
	  BLASLONG l = 0;
#if defined(TRMMKERNEL)
	  REFRESH_POINTERS (1, 2)
#else
	  BO = B;
	  temp = k;
#endif
	  v4sf_t t = { 0, 0, 0, 0 };
	  for (l = 0; l < temp; l++)
	    {
	      v4sf_t rowA = { AO[l], AO[l], 0, 0 };
	      v4sf_t rowB = { BO[l << 1], BO[(l << 1) + 1], 0, 0 };
	      t += rowA * rowB;
	    }
	  t = t * valpha;
#if defined(TRMMKERNEL)
	  CO[0 * ldc] = t[0];
	  CO[1 * ldc] = t[1];
#else
	  CO[0 * ldc] += t[0];
	  CO[1 * ldc] += t[1];
#endif
	  CO += 1;
	  AO += temp;
	  BO += temp << 1;
#if defined(TRMMKERNEL)
	  REFRESH_AFTER_SAVE (1, 2)
#endif
	}
#if defined(TRMMKERNEL) && !defined(LEFT)
      off += 2;			// number of values in A
#endif

      B += k << 1;
    }
  if (n & 1)
    {
      BLASLONG i, temp;
#if defined(TRMMKERNEL) && defined(LEFT)
      off = offset;
#endif
      FLOAT *CO;
      FLOAT *AO;
      CO = C;
      C += ldc;
      AO = A;
      for (i = 0; i < (m >> 4); i++)
	{
	  FLOAT *BO;
	  BLASLONG l = 0;
#if defined(TRMMKERNEL)
	  REFRESH_POINTERS (16, 1)
#else
	  BO = B;
	  temp = k;
#endif

	  v4sf_t t = { 0, 0, 0, 0 };
	  v4sf_t t1 = { 0, 0, 0, 0 };
	  v4sf_t t2 = { 0, 0, 0, 0 };
	  v4sf_t t3 = { 0, 0, 0, 0 };
	  for (l = 0; l < temp; l++)
	    {
	      v4sf_t rowB = { BO[l], BO[l], BO[l], BO[l] };
	      v4sf_t rowA = { AO[l << 4], AO[(l << 4) + 1], AO[(l << 4) + 2],
		AO[(l << 4) + 3]
	      };
	      v4sf_t rowA1 =
		{ AO[(l << 4) + 4], AO[(l << 4) + 5], AO[(l << 4) + 6],
		AO[(l << 4) + 7]
	      };
	      v4sf_t rowA2 =
		{ AO[(l << 4) + 8], AO[(l << 4) + 9], AO[(l << 4) + 10],
		AO[(l << 4) + 11]
	      };
	      v4sf_t rowA3 =
		{ AO[(l << 4) + 12], AO[(l << 4) + 13], AO[(l << 4) + 14],
		AO[(l << 4) + 15]
	      };
	      t += rowA * rowB;
	      t1 += rowA1 * rowB;
	      t2 += rowA2 * rowB;
	      t3 += rowA3 * rowB;
	    }
	  t = t * valpha;
	  t1 = t1 * valpha;
	  t2 = t2 * valpha;
	  t3 = t3 * valpha;
#if defined(TRMMKERNEL)
	  CO[0] = t[0];
	  CO[1] = t[1];
	  CO[2] = t[2];
	  CO[3] = t[3];
	  CO[4] = t1[0];
	  CO[5] = t1[1];
	  CO[6] = t1[2];
	  CO[7] = t1[3];
	  CO[8] = t2[0];
	  CO[9] = t2[1];
	  CO[10] = t2[2];
	  CO[11] = t2[3];
	  CO[12] = t3[0];
	  CO[13] = t3[1];
	  CO[14] = t3[2];
	  CO[15] = t3[3];
#else
	  CO[0] += t[0];
	  CO[1] += t[1];
	  CO[2] += t[2];
	  CO[3] += t[3];
	  CO[4] += t1[0];
	  CO[5] += t1[1];
	  CO[6] += t1[2];
	  CO[7] += t1[3];
	  CO[8] += t2[0];
	  CO[9] += t2[1];
	  CO[10] += t2[2];
	  CO[11] += t2[3];
	  CO[12] += t3[0];
	  CO[13] += t3[1];
	  CO[14] += t3[2];
	  CO[15] += t3[3];
#endif
	  AO += temp << 4;
	  BO += temp;
	  CO += 16;
#if defined(TRMMKERNEL)
	  REFRESH_AFTER_SAVE (16, 1)
#endif
	}
      if (m & 8)
	{
	  FLOAT *BO;
	  BLASLONG l = 0;
	  v4sf_t t = { 0, 0, 0, 0 };
	  v4sf_t t1 = { 0, 0, 0, 0 };
#if defined(TRMMKERNEL)
	  REFRESH_POINTERS (8, 1)
#else
	  BO = B;
	  temp = k;
#endif

	  for (l = 0; l < temp; l++)
	    {
	      v4sf_t rowB = { BO[l], BO[l], BO[l], BO[l] };
	      v4sf_t rowA = { AO[l << 3], AO[(l << 3) + 1], AO[(l << 3) + 2],
		AO[(l << 3) + 3]
	      };
	      v4sf_t rowA1 =
		{ AO[(l << 3) + 4], AO[(l << 3) + 5], AO[(l << 3) + 6],
		AO[(l << 3) + 7]
	      };
	      t += rowA * rowB;
	      t1 += rowA1 * rowB;
	    }
	  t = t * valpha;
	  t1 = t1 * valpha;
#if defined(TRMMKERNEL)
	  CO[0] = t[0];
	  CO[1] = t[1];
	  CO[2] = t[2];
	  CO[3] = t[3];
	  CO[4] = t1[0];
	  CO[5] = t1[1];
	  CO[6] = t1[2];
	  CO[7] = t1[3];
#else
	  CO[0] += t[0];
	  CO[1] += t[1];
	  CO[2] += t[2];
	  CO[3] += t[3];
	  CO[4] += t1[0];
	  CO[5] += t1[1];
	  CO[6] += t1[2];
	  CO[7] += t1[3];
#endif
	  AO += temp << 3;
	  BO += temp;
	  CO += 8;
#if defined(TRMMKERNEL)
	  REFRESH_AFTER_SAVE (8, 1)
#endif
	}
      if (m & 4)
	{
	  FLOAT *BO;
	  BLASLONG l = 0;
	  v4sf_t t = { 0, 0, 0, 0 };
#if defined(TRMMKERNEL)
	  REFRESH_POINTERS (4, 1)
#else
	  BO = B;
	  temp = k;
#endif

	  for (l = 0; l < temp; l++)
	    {
	      v4sf_t rowB = { BO[l], BO[l], BO[l], BO[l] };
	      v4sf_t rowA = { AO[l << 2], AO[(l << 2) + 1], AO[(l << 2) + 2],
		AO[(l << 2) + 3]
	      };
	      t += rowA * rowB;
	    }
	  t = t * valpha;
#if defined(TRMMKERNEL)
	  CO[0] = t[0];
	  CO[1] = t[1];
	  CO[2] = t[2];
	  CO[3] = t[3];
#else
	  CO[0] += t[0];
	  CO[1] += t[1];
	  CO[2] += t[2];
	  CO[3] += t[3];
#endif
	  AO += temp << 2;
	  BO += temp;
	  CO += 4;
#if defined(TRMMKERNEL)
	  REFRESH_AFTER_SAVE (4, 1)
#endif
	}
      if (m & 2)
	{
	  FLOAT *BO;
	  BLASLONG l = 0;
#if defined(TRMMKERNEL)
	  REFRESH_POINTERS (2, 1)
#else
	  BO = B;
	  temp = k;
#endif

	  v4sf_t t = { 0, 0, 0, 0 };
	  for (l = 0; l < temp; l++)
	    {
	      v4sf_t rowB = { BO[l], BO[l], 0, 0 };
	      v4sf_t rowA = { AO[l << 1], AO[(l << 1) + 1], 0, 0 };
	      t += rowA * rowB;
	    }
	  t = t * valpha;
#if defined(TRMMKERNEL)
	  CO[0] = t[0];
	  CO[1] = t[1];
#else
	  CO[0] += t[0];
	  CO[1] += t[1];
#endif
	  AO += temp << 1;
	  BO += temp;
	  CO += 2;
#if defined(TRMMKERNEL)
	  REFRESH_AFTER_SAVE (2, 1)
#endif
	}
      if (m & 1)
	{
	  FLOAT *BO;
#if defined(TRMMKERNEL)
	  REFRESH_POINTERS (1, 1)
#else
	  BO = B;
	  temp = k;
#endif

	  BLASLONG l = 0;
	  FLOAT t = 0;
	  for (l = 0; l < temp; l++)
	    {
	      t += AO[l] * BO[l];
	    }
	  AO += temp;
	  BO += temp;
#if defined(TRMMKERNEL)
	  CO[0] = t * alpha;
#else
	  CO[0] += t * alpha;
#endif
	  CO += 1;
#if defined(TRMMKERNEL)
	  REFRESH_AFTER_SAVE (1, 1)
#endif
	}

#if defined(TRMMKERNEL) && !defined(LEFT)
      off += 1;			// number of values in A
#endif
      B += k;
    }
  return 0;
}
