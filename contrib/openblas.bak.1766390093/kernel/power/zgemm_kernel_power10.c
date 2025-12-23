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

#define SET_ACC_ZERO() \
          __builtin_mma_xxsetaccz (&acc0); \
          __builtin_mma_xxsetaccz (&acc1); \
          __builtin_mma_xxsetaccz (&acc2); \
          __builtin_mma_xxsetaccz (&acc3); \
          __builtin_mma_xxsetaccz (&acc4); \
          __builtin_mma_xxsetaccz (&acc5); \
          __builtin_mma_xxsetaccz (&acc6); \
          __builtin_mma_xxsetaccz (&acc7);

#if (defined(NN) || defined(NT) || defined(TN) || defined(TT))
#define COMP_MUL(_real, _arbr, _aibi, _imag, _arbi, _aibr) { _real  = _arbr - _aibi; _imag  =  _arbi + _aibr; }
#define COMP_MAC(_real, _arbr, _aibi, _imag, _arbi, _aibr) { _real += _arbr - _aibi; _imag +=  _arbi + _aibr; }
#endif

#if (defined(NR) || defined(NC) || defined(TR) || defined(TC))
#define COMP_MUL(_real, _arbr, _aibi, _imag, _arbi, _aibr) { _real  = _arbr + _aibi; _imag  = -_arbi + _aibr; }
#define COMP_MAC(_real, _arbr, _aibi, _imag, _arbi, _aibr) { _real += _arbr + _aibi; _imag += -_arbi + _aibr; }
#endif

#if (defined(RN) || defined(RT) || defined(CN) || defined(CT))
#define COMP_MUL(_real, _arbr, _aibi, _imag, _arbi, _aibr) { _real  = _arbr + _aibi; _imag  =  _arbi - _aibr; }
#define COMP_MAC(_real, _arbr, _aibi, _imag, _arbi, _aibr) { _real += _arbr + _aibi; _imag +=  _arbi - _aibr; }
#endif

#if (defined(RR) || defined(RC) || defined(CR) || defined(CC)) 
#define COMP_MUL(_real, _arbr, _aibi, _imag, _arbi, _aibr) { _real  = _arbr - _aibi; _imag  = -_arbi - _aibr; }
#define COMP_MAC(_real, _arbr, _aibi, _imag, _arbi, _aibr) { _real += _arbr - _aibi; _imag += -_arbi - _aibr; }
#endif

#if defined(TRMMKERNEL) 
#define A_OP =
#else
#define A_OP +=
#endif

#define BUILTIN_MMA_DISASSEMBLE_ACC_8                                 \
          __builtin_mma_disassemble_acc ((void *)result, &acc0);      \
          __builtin_mma_disassemble_acc ((void *)&result[4], &acc1);  \
          __builtin_mma_disassemble_acc ((void *)&result[8], &acc2);  \
          __builtin_mma_disassemble_acc ((void *)&result[12], &acc3); \
          __builtin_mma_disassemble_acc ((void *)&result[16], &acc4); \
          __builtin_mma_disassemble_acc ((void *)&result[20], &acc5); \
          __builtin_mma_disassemble_acc ((void *)&result[24], &acc6); \
          __builtin_mma_disassemble_acc ((void *)&result[28], &acc7); 

#define SAVE_ACC_COMPLEX_11                                           \
          BUILTIN_MMA_DISASSEMBLE_ACC_8                               \
          COMP_MUL(tr[0], res[ 0], res[ 3], ti[0], res[ 1], res[ 2])  \
          COMP_MAC(tr[0], res[ 8], res[11], ti[0], res[ 9], res[10])  \
          COMP_MAC(tr[0], res[16], res[19], ti[0], res[17], res[18])  \
          COMP_MAC(tr[0], res[24], res[27], ti[0], res[25], res[26])  \
          COMP_MAC(tr[0], res[32], res[35], ti[0], res[33], res[34])  \
          COMP_MAC(tr[0], res[40], res[43], ti[0], res[41], res[42])  \
          COMP_MAC(tr[0], res[48], res[51], ti[0], res[49], res[50])  \
          COMP_MAC(tr[0], res[56], res[59], ti[0], res[57], res[58])  \
	  CO[0] A_OP tr[0] * alpha_r - ti[0] * alpha_i;               \
	  CO[1] A_OP ti[0] * alpha_r + tr[0] * alpha_i;

#define SAVE_ACC_COMPLEX_12                                           \
          BUILTIN_MMA_DISASSEMBLE_ACC_8                               \
          COMP_MUL(tr[0], res[ 0], res[ 3], ti[0], res[ 1], res[ 2])  \
          COMP_MUL(tr[1], res[ 8], res[11], ti[1], res[ 9], res[10])  \
          COMP_MAC(tr[0], res[16], res[19], ti[0], res[17], res[18])  \
          COMP_MAC(tr[1], res[24], res[27], ti[1], res[25], res[26])  \
          COMP_MAC(tr[0], res[32], res[35], ti[0], res[33], res[34])  \
          COMP_MAC(tr[1], res[40], res[43], ti[1], res[41], res[42])  \
          COMP_MAC(tr[0], res[48], res[51], ti[0], res[49], res[50])  \
          COMP_MAC(tr[1], res[56], res[59], ti[1], res[57], res[58])  \
	  CO[0] A_OP tr[0] * alpha_r - ti[0] * alpha_i;               \
	  CO[1] A_OP ti[0] * alpha_r + tr[0] * alpha_i;               \
	  CO[2*ldc+0] A_OP tr[1] * alpha_r - ti[1] * alpha_i;         \
	  CO[2*ldc+1] A_OP ti[1] * alpha_r + tr[1] * alpha_i;

#define SAVE_ACC_COMPLEX_21_1                                         \
          BUILTIN_MMA_DISASSEMBLE_ACC_8                               \
          COMP_MUL(tr[0], res[ 0], res[ 3], ti[0], res[ 1], res[ 2])  \
          COMP_MUL(tr[1], res[ 4], res[ 7], ti[1], res[ 5], res[ 6])  \
          COMP_MAC(tr[0], res[ 8], res[11], ti[0], res[ 9], res[10])  \
          COMP_MAC(tr[1], res[12], res[15], ti[1], res[13], res[14])  \
          COMP_MAC(tr[0], res[16], res[19], ti[0], res[17], res[18])  \
          COMP_MAC(tr[1], res[20], res[23], ti[1], res[21], res[22])  \
          COMP_MAC(tr[0], res[24], res[27], ti[0], res[25], res[26])  \
          COMP_MAC(tr[1], res[28], res[31], ti[1], res[29], res[30])  \
          COMP_MAC(tr[0], res[32], res[35], ti[0], res[33], res[34])  \
          COMP_MAC(tr[1], res[36], res[39], ti[1], res[37], res[38])  \
          COMP_MAC(tr[0], res[40], res[43], ti[0], res[41], res[42])  \
          COMP_MAC(tr[1], res[44], res[47], ti[1], res[45], res[46])  \
          COMP_MAC(tr[0], res[48], res[51], ti[0], res[49], res[50])  \
          COMP_MAC(tr[1], res[52], res[55], ti[1], res[53], res[54])  \
          COMP_MAC(tr[0], res[56], res[59], ti[0], res[57], res[58])  \
          COMP_MAC(tr[1], res[60], res[63], ti[1], res[61], res[62])  \
	  CO[0] A_OP tr[0] * alpha_r - ti[0] * alpha_i;               \
	  CO[1] A_OP ti[0] * alpha_r + tr[0] * alpha_i;               \
	  CO[2] A_OP tr[1] * alpha_r - ti[1] * alpha_i;               \
	  CO[3] A_OP ti[1] * alpha_r + tr[1] * alpha_i;

#define SAVE_ACC_COMPLEX_21_2                                         \
          BUILTIN_MMA_DISASSEMBLE_ACC_8                               \
          COMP_MUL(tr[0], res[ 0], res[ 3], ti[0], res[ 1], res[ 2])  \
          COMP_MUL(tr[1], res[ 4], res[ 7], ti[1], res[ 5], res[ 6])  \
          COMP_MUL(tr[2], res[ 8], res[11], ti[2], res[ 9], res[10])  \
          COMP_MUL(tr[3], res[12], res[15], ti[3], res[13], res[14])  \
          COMP_MAC(tr[0], res[16], res[19], ti[0], res[17], res[18])  \
          COMP_MAC(tr[1], res[20], res[23], ti[1], res[21], res[22])  \
          COMP_MAC(tr[2], res[24], res[27], ti[2], res[25], res[26])  \
          COMP_MAC(tr[3], res[28], res[31], ti[3], res[29], res[30])  \
          COMP_MAC(tr[0], res[32], res[35], ti[0], res[33], res[34])  \
          COMP_MAC(tr[1], res[36], res[39], ti[1], res[37], res[38])  \
          COMP_MAC(tr[2], res[40], res[43], ti[2], res[41], res[42])  \
          COMP_MAC(tr[3], res[44], res[47], ti[3], res[45], res[46])  \
          COMP_MAC(tr[0], res[48], res[51], ti[0], res[49], res[50])  \
          COMP_MAC(tr[1], res[52], res[55], ti[1], res[53], res[54])  \
          COMP_MAC(tr[2], res[56], res[59], ti[2], res[57], res[58])  \
          COMP_MAC(tr[3], res[60], res[63], ti[3], res[61], res[62])  \
	  CO[0] A_OP tr[0] * alpha_r - ti[0] * alpha_i;               \
	  CO[1] A_OP ti[0] * alpha_r + tr[0] * alpha_i;               \
	  CO[2] A_OP tr[1] * alpha_r - ti[1] * alpha_i;               \
	  CO[3] A_OP ti[1] * alpha_r + tr[1] * alpha_i;               \
	  CO[4] A_OP tr[2] * alpha_r - ti[2] * alpha_i;               \
	  CO[5] A_OP ti[2] * alpha_r + tr[2] * alpha_i;               \
	  CO[6] A_OP tr[3] * alpha_r - ti[3] * alpha_i;               \
	  CO[7] A_OP ti[3] * alpha_r + tr[3] * alpha_i;

#define SAVE_ACC_COMPLEX_21_4                                         \
          BUILTIN_MMA_DISASSEMBLE_ACC_8                               \
          COMP_MUL(tr[0], res[ 0], res[ 3], ti[0], res[ 1], res[ 2])  \
          COMP_MUL(tr[1], res[ 4], res[ 7], ti[1], res[ 5], res[ 6])  \
          COMP_MUL(tr[2], res[ 8], res[11], ti[2], res[ 9], res[10])  \
          COMP_MUL(tr[3], res[12], res[15], ti[3], res[13], res[14])  \
          COMP_MUL(tr[4], res[16], res[19], ti[4], res[17], res[18])  \
          COMP_MUL(tr[5], res[20], res[23], ti[5], res[21], res[22])  \
          COMP_MUL(tr[6], res[24], res[27], ti[6], res[25], res[26])  \
          COMP_MUL(tr[7], res[28], res[31], ti[7], res[29], res[30])  \
          COMP_MAC(tr[0], res[32], res[35], ti[0], res[33], res[34])  \
          COMP_MAC(tr[1], res[36], res[39], ti[1], res[37], res[38])  \
          COMP_MAC(tr[2], res[40], res[43], ti[2], res[41], res[42])  \
          COMP_MAC(tr[3], res[44], res[47], ti[3], res[45], res[46])  \
          COMP_MAC(tr[4], res[48], res[51], ti[4], res[49], res[50])  \
          COMP_MAC(tr[5], res[52], res[55], ti[5], res[53], res[54])  \
          COMP_MAC(tr[6], res[56], res[59], ti[6], res[57], res[58])  \
          COMP_MAC(tr[7], res[60], res[63], ti[7], res[61], res[62])  \
	  CO[ 0] A_OP tr[0] * alpha_r - ti[0] * alpha_i;              \
	  CO[ 1] A_OP ti[0] * alpha_r + tr[0] * alpha_i;              \
	  CO[ 2] A_OP tr[1] * alpha_r - ti[1] * alpha_i;              \
	  CO[ 3] A_OP ti[1] * alpha_r + tr[1] * alpha_i;              \
	  CO[ 4] A_OP tr[2] * alpha_r - ti[2] * alpha_i;              \
	  CO[ 5] A_OP ti[2] * alpha_r + tr[2] * alpha_i;              \
	  CO[ 6] A_OP tr[3] * alpha_r - ti[3] * alpha_i;              \
	  CO[ 7] A_OP ti[3] * alpha_r + tr[3] * alpha_i;              \
	  CO[ 8] A_OP tr[4] * alpha_r - ti[4] * alpha_i;              \
	  CO[ 9] A_OP ti[4] * alpha_r + tr[4] * alpha_i;              \
	  CO[10] A_OP tr[5] * alpha_r - ti[5] * alpha_i;              \
	  CO[11] A_OP ti[5] * alpha_r + tr[5] * alpha_i;              \
	  CO[12] A_OP tr[6] * alpha_r - ti[6] * alpha_i;              \
	  CO[13] A_OP ti[6] * alpha_r + tr[6] * alpha_i;              \
	  CO[14] A_OP tr[7] * alpha_r - ti[7] * alpha_i;              \
	  CO[15] A_OP ti[7] * alpha_r + tr[7] * alpha_i;

#define SAVE_ACC_COMPLEX_22_1                                          \
          __builtin_mma_disassemble_acc ((void *)result, &acc0);       \
          __builtin_mma_disassemble_acc ((void *)(&result[4]), &acc1); \
          COMP_MUL(tr[0], res[0], res[3], ti[0], res[1], res[2])       \
          COMP_MUL(tr[1], res[4], res[7], ti[1], res[5], res[6])       \
          COMP_MUL(tr[2], res[8], res[11], ti[2], res[9], res[10])     \
          COMP_MUL(tr[3], res[12], res[15], ti[3], res[13], res[14] )  \
	  CO[0] A_OP tr[0] * alpha_r - ti[0] * alpha_i;            \
	  CO[1] A_OP ti[0] * alpha_r + tr[0] * alpha_i;            \
	  CO[2] A_OP tr[1] * alpha_r - ti[1] * alpha_i;            \
	  CO[3] A_OP ti[1] * alpha_r + tr[1] * alpha_i;            \
	  CO[2*ldc+0] A_OP tr[2] * alpha_r - ti[2] * alpha_i;      \
	  CO[2*ldc+1] A_OP ti[2] * alpha_r + tr[2] * alpha_i;      \
	  CO[2*ldc+2] A_OP tr[3] * alpha_r - ti[3] * alpha_i;      \
	  CO[2*ldc+3] A_OP ti[3] * alpha_r + tr[3] * alpha_i;

#define SAVE_ACC_COMPLEX_22_2(ACC1, ACC2, CI)                         \
          __builtin_mma_disassemble_acc ((void *)result, ACC1);       \
          __builtin_mma_disassemble_acc ((void *)(&result[4]), ACC2); \
          COMP_MUL(tr[0], res[0], res[3], ti[0], res[1], res[2])      \
          COMP_MUL(tr[1], res[4], res[7], ti[1], res[5], res[6])      \
          COMP_MUL(tr[2], res[8], res[11], ti[2], res[9], res[10])    \
          COMP_MUL(tr[3], res[12], res[15], ti[3], res[13], res[14])  \
	  CO[CI+0] A_OP tr[0] * alpha_r - ti[0] * alpha_i;            \
	  CO[CI+1] A_OP ti[0] * alpha_r + tr[0] * alpha_i;            \
	  CO[CI+2] A_OP tr[1] * alpha_r - ti[1] * alpha_i;            \
	  CO[CI+3] A_OP ti[1] * alpha_r + tr[1] * alpha_i;            \
	  CO[2*ldc+CI+0] A_OP tr[2] * alpha_r - ti[2] * alpha_i;      \
	  CO[2*ldc+CI+1] A_OP ti[2] * alpha_r + tr[2] * alpha_i;      \
	  CO[2*ldc+CI+2] A_OP tr[3] * alpha_r - ti[3] * alpha_i;      \
	  CO[2*ldc+CI+3] A_OP ti[3] * alpha_r + tr[3] * alpha_i;

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
#define REFRESH_POINTERS(x, y)  \
          BO = B;               \
          REFRESH_TEMP_BK(x, y)
#else
#define REFRESH_POINTERS(x, y)  \
          AO += off * (2*x);    \
          BO = B + off * (2*y); \
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
            AO += temp * (2*x); \
            BO += temp * (2*y);
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
CNAME (BLASLONG m, BLASLONG n, BLASLONG k, FLOAT alpha_r, FLOAT alpha_i, FLOAT * A, FLOAT * B,
       FLOAT * C, BLASLONG ldc
#ifdef TRMMKERNEL
       , BLASLONG offset
#endif
  )
{
  BLASLONG i1, i, l, temp;
  FLOAT *AO, *BO, *CO;
#if defined(TRMMKERNEL)
  BLASLONG off;
#endif
#if defined(TRMMKERNEL) && !defined(LEFT)
  off = -offset;
#endif
  __vector_quad acc0, acc1, acc2, acc3, acc4, acc5, acc6, acc7;

  v4sf_t result[32];
  FLOAT *res, tr[16], ti[16];
  res = (FLOAT *) result;

  for (i1 = 0; i1 < (n >> 1); i1++)
    {
#if defined(TRMMKERNEL) && defined(LEFT)
      off = offset;
#endif
      AO = A;
      CO = C;
      C += ldc<<2;
      for (i = 0; i < (m >> 3); i++)
	{
#if defined(TRMMKERNEL)
          REFRESH_POINTERS (8, 2)
#else
          BO = B;
          temp = k;
#endif
          SET_ACC_ZERO()
	  for (l = 0; l < temp; ++l)
	    {
              __vector_pair rowA1 = *((__vector_pair *)((void *)&AO[l<<4]));
              __vector_pair rowA2 = *((__vector_pair *)((void *)&AO[(l<<4)+4]));
              __vector_pair rowA3 = *((__vector_pair *)((void *)&AO[(l<<4)+8]));
              __vector_pair rowA4 = *((__vector_pair *)((void *)&AO[(l<<4)+12]));
              vec_t rowB1 = *(vec_t *) & BO[l<<2];
              vec_t rowB2 = *(vec_t *) & BO[(l<<2)+2];
              __builtin_mma_xvf64gerpp(&acc0, rowA1, rowB1);
              __builtin_mma_xvf64gerpp(&acc1, rowA2, rowB1);
              __builtin_mma_xvf64gerpp(&acc2, rowA3, rowB1);
              __builtin_mma_xvf64gerpp(&acc3, rowA4, rowB1);
              __builtin_mma_xvf64gerpp(&acc4, rowA1, rowB2);
              __builtin_mma_xvf64gerpp(&acc5, rowA2, rowB2);
              __builtin_mma_xvf64gerpp(&acc6, rowA3, rowB2);
              __builtin_mma_xvf64gerpp(&acc7, rowA4, rowB2);
	    }
        __builtin_mma_disassemble_acc ((void *)result,        &acc0);
        __builtin_mma_disassemble_acc ((void *)(&result[ 4]), &acc1);
        __builtin_mma_disassemble_acc ((void *)(&result[ 8]), &acc2);
        __builtin_mma_disassemble_acc ((void *)(&result[12]), &acc3);
        __builtin_mma_disassemble_acc ((void *)(&result[16]), &acc4);
        __builtin_mma_disassemble_acc ((void *)(&result[20]), &acc5);
        __builtin_mma_disassemble_acc ((void *)(&result[24]), &acc6);
        __builtin_mma_disassemble_acc ((void *)(&result[28]), &acc7);
        COMP_MUL(tr[ 0], res[ 0], res[ 3], ti[ 0], res[ 1], res[ 2])
        COMP_MUL(tr[ 1], res[ 4], res[ 7], ti[ 1], res[ 5], res[ 6])
        COMP_MUL(tr[ 2], res[ 8], res[11], ti[ 2], res[ 9], res[10])
        COMP_MUL(tr[ 3], res[12], res[15], ti[ 3], res[13], res[14])
        COMP_MUL(tr[ 4], res[16], res[19], ti[ 4], res[17], res[18])
        COMP_MUL(tr[ 5], res[20], res[23], ti[ 5], res[21], res[22])
        COMP_MUL(tr[ 6], res[24], res[27], ti[ 6], res[25], res[26])
        COMP_MUL(tr[ 7], res[28], res[31], ti[ 7], res[29], res[30])
        COMP_MUL(tr[ 8], res[32], res[35], ti[ 8], res[33], res[34])
        COMP_MUL(tr[ 9], res[36], res[39], ti[ 9], res[37], res[38])
        COMP_MUL(tr[10], res[40], res[43], ti[10], res[41], res[42])
        COMP_MUL(tr[11], res[44], res[47], ti[11], res[45], res[46])
        COMP_MUL(tr[12], res[48], res[51], ti[12], res[49], res[50])
        COMP_MUL(tr[13], res[52], res[55], ti[13], res[53], res[54])
        COMP_MUL(tr[14], res[56], res[59], ti[14], res[57], res[58])
        COMP_MUL(tr[15], res[60], res[63], ti[15], res[61], res[62])
        CO[ 0] A_OP tr[0] * alpha_r - ti[0] * alpha_i;
        CO[ 1] A_OP ti[0] * alpha_r + tr[0] * alpha_i;
        CO[ 2] A_OP tr[1] * alpha_r - ti[1] * alpha_i;
        CO[ 3] A_OP ti[1] * alpha_r + tr[1] * alpha_i;
        CO[ 4] A_OP tr[2] * alpha_r - ti[2] * alpha_i;
        CO[ 5] A_OP ti[2] * alpha_r + tr[2] * alpha_i;
        CO[ 6] A_OP tr[3] * alpha_r - ti[3] * alpha_i;
        CO[ 7] A_OP ti[3] * alpha_r + tr[3] * alpha_i;
        CO[ 8] A_OP tr[4] * alpha_r - ti[4] * alpha_i;
        CO[ 9] A_OP ti[4] * alpha_r + tr[4] * alpha_i;
        CO[10] A_OP tr[5] * alpha_r - ti[5] * alpha_i;
        CO[11] A_OP ti[5] * alpha_r + tr[5] * alpha_i;
        CO[12] A_OP tr[6] * alpha_r - ti[6] * alpha_i;
        CO[13] A_OP ti[6] * alpha_r + tr[6] * alpha_i;
        CO[14] A_OP tr[7] * alpha_r - ti[7] * alpha_i;
        CO[15] A_OP ti[7] * alpha_r + tr[7] * alpha_i;
        CO[2*ldc+ 0] A_OP tr[ 8] * alpha_r - ti[ 8] * alpha_i;
        CO[2*ldc+ 1] A_OP ti[ 8] * alpha_r + tr[ 8] * alpha_i;
        CO[2*ldc+ 2] A_OP tr[ 9] * alpha_r - ti[ 9] * alpha_i;
        CO[2*ldc+ 3] A_OP ti[ 9] * alpha_r + tr[ 9] * alpha_i;
        CO[2*ldc+ 4] A_OP tr[10] * alpha_r - ti[10] * alpha_i;
        CO[2*ldc+ 5] A_OP ti[10] * alpha_r + tr[10] * alpha_i;
        CO[2*ldc+ 6] A_OP tr[11] * alpha_r - ti[11] * alpha_i;
        CO[2*ldc+ 7] A_OP ti[11] * alpha_r + tr[11] * alpha_i;
        CO[2*ldc+ 8] A_OP tr[12] * alpha_r - ti[12] * alpha_i;
        CO[2*ldc+ 9] A_OP ti[12] * alpha_r + tr[12] * alpha_i;
        CO[2*ldc+10] A_OP tr[13] * alpha_r - ti[13] * alpha_i;
        CO[2*ldc+11] A_OP ti[13] * alpha_r + tr[13] * alpha_i;
        CO[2*ldc+12] A_OP tr[14] * alpha_r - ti[14] * alpha_i;
        CO[2*ldc+13] A_OP ti[14] * alpha_r + tr[14] * alpha_i;
        CO[2*ldc+14] A_OP tr[15] * alpha_r - ti[15] * alpha_i;
        CO[2*ldc+15] A_OP ti[15] * alpha_r + tr[15] * alpha_i;

	  AO += temp << 4;
	  BO += temp << 2;
	  CO += 16;
#if defined(TRMMKERNEL)
          REFRESH_AFTER_SAVE (8, 2)
#endif
	}
      if (m & 4)
	{
#if defined(TRMMKERNEL)
          REFRESH_POINTERS (4, 2)
#else
          BO = B;
          temp = k;
#endif
          SET_ACC_ZERO()
	  for (l = 0; l < (temp & (~1)); l+=2)
	    {
              __vector_pair rowA1 = *((__vector_pair *)((void *)&AO[l<<3]));
              __vector_pair rowA2 = *((__vector_pair *)((void *)&AO[(l<<3)+4]));
              __vector_pair rowA3 = *((__vector_pair *)((void *)&AO[(l<<3)+8]));
              __vector_pair rowA4 = *((__vector_pair *)((void *)&AO[(l<<3)+12]));
              vec_t rowB1 = *(vec_t *) & BO[l<<2];
              vec_t rowB2 = *(vec_t *) & BO[(l<<2)+2];
              vec_t rowB3 = *(vec_t *) & BO[(l<<2)+4];
              vec_t rowB4 = *(vec_t *) & BO[(l<<2)+6];
              __builtin_mma_xvf64gerpp(&acc0, rowA1, rowB1);
              __builtin_mma_xvf64gerpp(&acc1, rowA2, rowB1);
              __builtin_mma_xvf64gerpp(&acc2, rowA1, rowB2);
              __builtin_mma_xvf64gerpp(&acc3, rowA2, rowB2);
              __builtin_mma_xvf64gerpp(&acc0, rowA3, rowB3);
              __builtin_mma_xvf64gerpp(&acc1, rowA4, rowB3);
              __builtin_mma_xvf64gerpp(&acc2, rowA3, rowB4);
              __builtin_mma_xvf64gerpp(&acc3, rowA4, rowB4);
	    }
	  for (l = (temp & (~1)); l < temp; ++l)
	    {
              __vector_pair rowA1 = *((__vector_pair *)((void *)&AO[l<<3]));
              __vector_pair rowA2 = *((__vector_pair *)((void *)&AO[(l<<3)+4]));
              vec_t rowB1 = *(vec_t *) & BO[l<<2];
              vec_t rowB2 = *(vec_t *) & BO[(l<<2)+2];
              __builtin_mma_xvf64gerpp(&acc0, rowA1, rowB1);
              __builtin_mma_xvf64gerpp(&acc1, rowA2, rowB1);
              __builtin_mma_xvf64gerpp(&acc2, rowA1, rowB2);
              __builtin_mma_xvf64gerpp(&acc3, rowA2, rowB2);
	    }
          SAVE_ACC_COMPLEX_22_2(&acc0, &acc2, 0)
          SAVE_ACC_COMPLEX_22_2(&acc1, &acc3, 4)
	  AO += temp << 3;
	  BO += temp << 2;
	  CO += 8;
#if defined(TRMMKERNEL)
          REFRESH_AFTER_SAVE (4, 2)
#endif
	}
      if (m & 2)
	{
#if defined(TRMMKERNEL)
          REFRESH_POINTERS (2, 2)
#else
          BO = B;
          temp = k;
#endif
          SET_ACC_ZERO()
	  for (l = 0; l < (temp & (~3)); l+=4)
	    {
              __vector_pair rowA1 = *((__vector_pair *)((void *)&AO[l<<2]));
              __vector_pair rowA2 = *((__vector_pair *)((void *)&AO[(l<<2)+4]));
              __vector_pair rowA3 = *((__vector_pair *)((void *)&AO[(l<<2)+8]));
              __vector_pair rowA4 = *((__vector_pair *)((void *)&AO[(l<<2)+12]));
              vec_t rowB1 = *(vec_t *) & BO[l<<2];
              vec_t rowB2 = *(vec_t *) & BO[(l<<2)+2];
              vec_t rowB3 = *(vec_t *) & BO[(l<<2)+4];
              vec_t rowB4 = *(vec_t *) & BO[(l<<2)+6];
              vec_t rowB5 = *(vec_t *) & BO[(l<<2)+8];
              vec_t rowB6 = *(vec_t *) & BO[(l<<2)+10];
              vec_t rowB7 = *(vec_t *) & BO[(l<<2)+12];
              vec_t rowB8 = *(vec_t *) & BO[(l<<2)+14];
              __builtin_mma_xvf64gerpp(&acc0, rowA1, rowB1);
              __builtin_mma_xvf64gerpp(&acc1, rowA1, rowB2);
              __builtin_mma_xvf64gerpp(&acc0, rowA2, rowB3);
              __builtin_mma_xvf64gerpp(&acc1, rowA2, rowB4);
              __builtin_mma_xvf64gerpp(&acc0, rowA3, rowB5);
              __builtin_mma_xvf64gerpp(&acc1, rowA3, rowB6);
              __builtin_mma_xvf64gerpp(&acc0, rowA4, rowB7);
              __builtin_mma_xvf64gerpp(&acc1, rowA4, rowB8);
	    }
	  for (l = (temp & (~3)); l < temp; ++l)
	    {
              __vector_pair rowA1 = *((__vector_pair *)((void *)&AO[l<<2]));
              vec_t rowB1 = *(vec_t *) & BO[l<<2];
              vec_t rowB2 = *(vec_t *) & BO[(l<<2)+2];
              __builtin_mma_xvf64gerpp(&acc0, rowA1, rowB1);
              __builtin_mma_xvf64gerpp(&acc1, rowA1, rowB2);
	    }
          SAVE_ACC_COMPLEX_22_1
	  AO += temp << 2;
	  BO += temp << 2;
	  CO += 4;
#if defined(TRMMKERNEL)
          REFRESH_AFTER_SAVE (2, 2)
#endif
	}
      if (m & 1)
	{
#if defined(TRMMKERNEL)
          REFRESH_POINTERS (1, 2)
#else
          BO = B;
          temp = k;
#endif
	  // RIP OUT MMA STUFF!
          SET_ACC_ZERO()
	  for (l = 0; l < (temp & (~3)); l+=4)
	    {
              __vector_pair rowA1 = *((__vector_pair *)((void *)&AO[l<<1]));
              __vector_pair rowA2 = *((__vector_pair *)((void *)&AO[(l<<1)+2]));
              __vector_pair rowA3 = *((__vector_pair *)((void *)&AO[(l<<1)+4]));
              __vector_pair rowA4 = *((__vector_pair *)((void *)&AO[(l<<1)+6]));
              vec_t rowB1 = *(vec_t *) & BO[l<<2];
              vec_t rowB2 = *(vec_t *) & BO[(l<<2)+2];
              vec_t rowB3 = *(vec_t *) & BO[(l<<2)+4];
              vec_t rowB4 = *(vec_t *) & BO[(l<<2)+6];
              vec_t rowB5 = *(vec_t *) & BO[(l<<2)+8];
              vec_t rowB6 = *(vec_t *) & BO[(l<<2)+10];
              vec_t rowB7 = *(vec_t *) & BO[(l<<2)+12];
              vec_t rowB8 = *(vec_t *) & BO[(l<<2)+14];
              __builtin_mma_xvf64gerpp(&acc0, rowA1, rowB1);
              __builtin_mma_xvf64gerpp(&acc1, rowA1, rowB2);
              __builtin_mma_xvf64gerpp(&acc0, rowA2, rowB3);
              __builtin_mma_xvf64gerpp(&acc1, rowA2, rowB4);
              __builtin_mma_xvf64gerpp(&acc0, rowA3, rowB5);
              __builtin_mma_xvf64gerpp(&acc1, rowA3, rowB6);
              __builtin_mma_xvf64gerpp(&acc0, rowA4, rowB7);
              __builtin_mma_xvf64gerpp(&acc1, rowA4, rowB8);
	    }
	  for (l = (temp & (~3)); l < temp; ++l)
	    {
              __vector_pair rowA1 = *((__vector_pair *)((void *)&AO[l<<1]));
              vec_t rowB1 = *(vec_t *) & BO[l<<2];
              vec_t rowB2 = *(vec_t *) & BO[(l<<2)+2];
              __builtin_mma_xvf64gerpp(&acc0, rowA1, rowB1);
              __builtin_mma_xvf64gerpp(&acc1, rowA1, rowB2);
	    }
          SAVE_ACC_COMPLEX_12                
	  AO += temp << 1;
	  BO += temp << 2;
	  CO += 2;
#if defined(TRMMKERNEL)
          REFRESH_AFTER_SAVE (1, 2)
#endif
	}
#if defined(TRMMKERNEL) && !defined(LEFT)
      off += 2;                 // number of values in A
#endif
      B += k << 2;
    }
  if (n & 1)
    {
#if defined(TRMMKERNEL) && defined(LEFT)
      off = offset;
#endif
      AO = A;
      CO = C;
      C += ldc<<1;
      for (i = 0; i < (m >> 3); i++)
	{
#if defined(TRMMKERNEL)
          REFRESH_POINTERS (8, 1)
#else
          BO = B;
          temp = k;
#endif
          SET_ACC_ZERO()
	  for (l = 0; l < (temp & (~1)); l+=2)
	    {
              __vector_pair rowA1 = *((__vector_pair *)((void *)&AO[l<<4]));
              __vector_pair rowA2 = *((__vector_pair *)((void *)&AO[(l<<4)+4]));
              __vector_pair rowA3 = *((__vector_pair *)((void *)&AO[(l<<4)+8]));
              __vector_pair rowA4 = *((__vector_pair *)((void *)&AO[(l<<4)+12]));
              __vector_pair rowA5 = *((__vector_pair *)((void *)&AO[(l<<4)+16]));
              __vector_pair rowA6 = *((__vector_pair *)((void *)&AO[(l<<4)+20]));
              __vector_pair rowA7 = *((__vector_pair *)((void *)&AO[(l<<4)+24]));
              __vector_pair rowA8 = *((__vector_pair *)((void *)&AO[(l<<4)+28]));
              vec_t rowB1 = *(vec_t *) & BO[l<<1];
              vec_t rowB2 = *(vec_t *) & BO[(l<<1)+2];
              __builtin_mma_xvf64gerpp(&acc0, rowA1, rowB1);
              __builtin_mma_xvf64gerpp(&acc1, rowA2, rowB1);
              __builtin_mma_xvf64gerpp(&acc2, rowA3, rowB1);
              __builtin_mma_xvf64gerpp(&acc3, rowA4, rowB1);
              __builtin_mma_xvf64gerpp(&acc0, rowA5, rowB2);
              __builtin_mma_xvf64gerpp(&acc1, rowA6, rowB2);
              __builtin_mma_xvf64gerpp(&acc2, rowA7, rowB2);
              __builtin_mma_xvf64gerpp(&acc3, rowA8, rowB2);
	    }
	  for (l = (temp & (~1)); l < temp; ++l)
	    {
              __vector_pair rowA1 = *((__vector_pair *)((void *)&AO[l<<4]));
              __vector_pair rowA2 = *((__vector_pair *)((void *)&AO[(l<<4)+4]));
              __vector_pair rowA3 = *((__vector_pair *)((void *)&AO[(l<<4)+8]));
              __vector_pair rowA4 = *((__vector_pair *)((void *)&AO[(l<<4)+12]));
              vec_t rowB1 = *(vec_t *) & BO[l<<1];
              __builtin_mma_xvf64gerpp(&acc0, rowA1, rowB1);
              __builtin_mma_xvf64gerpp(&acc1, rowA2, rowB1);
              __builtin_mma_xvf64gerpp(&acc2, rowA3, rowB1);
              __builtin_mma_xvf64gerpp(&acc3, rowA4, rowB1);
	    }
          SAVE_ACC_COMPLEX_21_4

	  AO += temp << 4;
	  BO += temp << 1;
	  CO += 16;
#if defined(TRMMKERNEL)
          REFRESH_AFTER_SAVE (8, 1)
#endif
	}
      if (m & 4)
	{
#if defined(TRMMKERNEL)
          REFRESH_POINTERS (4, 1)
#else
          BO = B;
          temp = k;
#endif
          SET_ACC_ZERO()
	  for (l = 0; l < (temp & (~3)); l+=4)
	    {
              __vector_pair rowA1 = *((__vector_pair *)((void *)&AO[l<<3]));
              __vector_pair rowA2 = *((__vector_pair *)((void *)&AO[(l<<3)+4]));
              __vector_pair rowA3 = *((__vector_pair *)((void *)&AO[(l<<3)+8]));
              __vector_pair rowA4 = *((__vector_pair *)((void *)&AO[(l<<3)+12]));
              __vector_pair rowA5 = *((__vector_pair *)((void *)&AO[(l<<3)+16]));
              __vector_pair rowA6 = *((__vector_pair *)((void *)&AO[(l<<3)+20]));
              __vector_pair rowA7 = *((__vector_pair *)((void *)&AO[(l<<3)+24]));
              __vector_pair rowA8 = *((__vector_pair *)((void *)&AO[(l<<3)+28]));
              vec_t rowB1 = *(vec_t *) & BO[l<<1];
              vec_t rowB2 = *(vec_t *) & BO[(l<<1)+2];
              vec_t rowB3 = *(vec_t *) & BO[(l<<1)+4];
              vec_t rowB4 = *(vec_t *) & BO[(l<<1)+6];
              __builtin_mma_xvf64gerpp(&acc0, rowA1, rowB1);
              __builtin_mma_xvf64gerpp(&acc1, rowA2, rowB1);
              __builtin_mma_xvf64gerpp(&acc2, rowA3, rowB2);
              __builtin_mma_xvf64gerpp(&acc3, rowA4, rowB2);
              __builtin_mma_xvf64gerpp(&acc4, rowA5, rowB3);
              __builtin_mma_xvf64gerpp(&acc5, rowA6, rowB3);
              __builtin_mma_xvf64gerpp(&acc6, rowA7, rowB4);
              __builtin_mma_xvf64gerpp(&acc7, rowA8, rowB4);
	    }
	  for (l = (temp & (~3)); l < temp; ++l)
	    {
              __vector_pair rowA1 = *((__vector_pair *)((void *)&AO[l<<3]));
              __vector_pair rowA2 = *((__vector_pair *)((void *)&AO[(l<<3)+4]));
              vec_t rowB1 = *(vec_t *) & BO[l<<1];
              __builtin_mma_xvf64gerpp(&acc0, rowA1, rowB1);
              __builtin_mma_xvf64gerpp(&acc1, rowA2, rowB1);
	    }
          SAVE_ACC_COMPLEX_21_2
	  AO += temp << 3;
	  BO += temp << 1;
	  CO += 8;
#if defined(TRMMKERNEL)
          REFRESH_AFTER_SAVE (4, 1)
#endif
	} if (m & 2)
	{
#if defined(TRMMKERNEL)
          REFRESH_POINTERS (2, 1)
#else
          BO = B;
          temp = k;
#endif
          SET_ACC_ZERO()
	  for (l = 0; l < (temp & (~7)); l+=8)
	    {
              __vector_pair rowA1 = *((__vector_pair *)((void *)&AO[l<<2]));
              __vector_pair rowA2 = *((__vector_pair *)((void *)&AO[(l<<2)+4]));
              __vector_pair rowA3 = *((__vector_pair *)((void *)&AO[(l<<2)+8]));
              __vector_pair rowA4 = *((__vector_pair *)((void *)&AO[(l<<2)+12]));
              __vector_pair rowA5 = *((__vector_pair *)((void *)&AO[(l<<2)+16]));
              __vector_pair rowA6 = *((__vector_pair *)((void *)&AO[(l<<2)+20]));
              __vector_pair rowA7 = *((__vector_pair *)((void *)&AO[(l<<2)+24]));
              __vector_pair rowA8 = *((__vector_pair *)((void *)&AO[(l<<2)+28]));
              vec_t rowB1 = *(vec_t *) & BO[l<<1];
              vec_t rowB2 = *(vec_t *) & BO[(l<<1)+2];
              vec_t rowB3 = *(vec_t *) & BO[(l<<1)+4];
              vec_t rowB4 = *(vec_t *) & BO[(l<<1)+6];
              vec_t rowB5 = *(vec_t *) & BO[(l<<1)+8];
              vec_t rowB6 = *(vec_t *) & BO[(l<<1)+10];
              vec_t rowB7 = *(vec_t *) & BO[(l<<1)+12];
              vec_t rowB8 = *(vec_t *) & BO[(l<<1)+14];
              __builtin_mma_xvf64gerpp(&acc0, rowA1, rowB1);
              __builtin_mma_xvf64gerpp(&acc1, rowA2, rowB2);
              __builtin_mma_xvf64gerpp(&acc2, rowA3, rowB3);
              __builtin_mma_xvf64gerpp(&acc3, rowA4, rowB4);
              __builtin_mma_xvf64gerpp(&acc4, rowA5, rowB5);
              __builtin_mma_xvf64gerpp(&acc5, rowA6, rowB6);
              __builtin_mma_xvf64gerpp(&acc6, rowA7, rowB7);
              __builtin_mma_xvf64gerpp(&acc7, rowA8, rowB8);
	    }
	  for (l = (temp & (~7)); l < temp; ++l)
	    {
              __vector_pair rowA1 = *((__vector_pair *)((void *)&AO[l<<2]));
              vec_t rowB1 = *(vec_t *) & BO[l<<1];
              __builtin_mma_xvf64gerpp(&acc0, rowA1, rowB1);
	    }
          SAVE_ACC_COMPLEX_21_1
	  AO += temp << 2;
	  BO += temp << 1;
	  CO += 4;
#if defined(TRMMKERNEL)
          REFRESH_AFTER_SAVE (2, 1)
#endif
	}
      if (m & 1)
	{
#if defined(TRMMKERNEL)
          REFRESH_POINTERS (1, 1)
#else
          BO = B;
          temp = k;
#endif
	  // RIP OUT MMA STUFF!
          SET_ACC_ZERO()
	  for (l = 0; l < (temp & (~7)); l+=8)
	    {
              __vector_pair rowA1 = *((__vector_pair *)((void *)&AO[l<<1]));
              __vector_pair rowA2 = *((__vector_pair *)((void *)&AO[(l<<1)+2]));
              __vector_pair rowA3 = *((__vector_pair *)((void *)&AO[(l<<1)+4]));
              __vector_pair rowA4 = *((__vector_pair *)((void *)&AO[(l<<1)+6]));
              __vector_pair rowA5 = *((__vector_pair *)((void *)&AO[(l<<1)+8]));
              __vector_pair rowA6 = *((__vector_pair *)((void *)&AO[(l<<1)+10]));
              __vector_pair rowA7 = *((__vector_pair *)((void *)&AO[(l<<1)+12]));
              __vector_pair rowA8 = *((__vector_pair *)((void *)&AO[(l<<1)+14]));
              vec_t rowB1 = *(vec_t *) & BO[l<<1];
              vec_t rowB2 = *(vec_t *) & BO[(l<<1)+2];
              vec_t rowB3 = *(vec_t *) & BO[(l<<1)+4];
              vec_t rowB4 = *(vec_t *) & BO[(l<<1)+6];
              vec_t rowB5 = *(vec_t *) & BO[(l<<1)+8];
              vec_t rowB6 = *(vec_t *) & BO[(l<<1)+10];
              vec_t rowB7 = *(vec_t *) & BO[(l<<1)+12];
              vec_t rowB8 = *(vec_t *) & BO[(l<<1)+14];
              __builtin_mma_xvf64gerpp(&acc0, rowA1, rowB1);
              __builtin_mma_xvf64gerpp(&acc1, rowA2, rowB2);
              __builtin_mma_xvf64gerpp(&acc2, rowA3, rowB3);
              __builtin_mma_xvf64gerpp(&acc3, rowA4, rowB4);
              __builtin_mma_xvf64gerpp(&acc4, rowA5, rowB5);
              __builtin_mma_xvf64gerpp(&acc5, rowA6, rowB6);
              __builtin_mma_xvf64gerpp(&acc6, rowA7, rowB7);
              __builtin_mma_xvf64gerpp(&acc7, rowA8, rowB8);
	    }
	  for (l = (temp & (~7)); l < temp; ++l)
	    {
              __vector_pair rowA1 = *((__vector_pair *)((void *)&AO[l<<1]));
              vec_t rowB1 = *(vec_t *) & BO[l<<1];
              __builtin_mma_xvf64gerpp(&acc0, rowA1, rowB1);
	    }
          SAVE_ACC_COMPLEX_11       
	  AO += temp << 1;
	  BO += temp << 1;
	  CO += 2;
#if defined(TRMMKERNEL)
          REFRESH_AFTER_SAVE (1, 1)
#endif
	}
#if defined(TRMMKERNEL) && !defined(LEFT)
      off += 1;                 // number of values in A
#endif
      B += k << 1;
    }
  return 0;
}
