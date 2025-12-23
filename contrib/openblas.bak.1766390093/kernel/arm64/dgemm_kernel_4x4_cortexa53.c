/***************************************************************************
Copyright (c) 2021, The OpenBLAS Project
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
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A00 PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE OPENBLAS PROJECT OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*****************************************************************************/

#include "common.h"
#include <arm_neon.h>

/**********************************************************
 * Function: dgemm_kernel_arm_cortex_a53_4x4_m4n12
 * Operation: C[4][12] += alpha * sa[4][K] * sb[K][12]
 * Matrix orders:
 *    sa: column-major (leading dimension == 4)
 *    sb: 3 concatenated row-major 4-column submatrices
 *    C: column-major (leading dimension == LDC)
 *********************************************************/
static inline void dgemm_kernel_arm_cortex_a53_4x4_m4n12(
  const FLOAT *sa, const FLOAT *sb, FLOAT *C,
  BLASLONG K, BLASLONG LDC, FLOAT alpha) {

  /** prefetch 4x12 elements from matrix C for RW purpose */
  __asm__ __volatile__(
    "mov x0,%[C]\n\t"
    "prfm pstl1keep,[x0]; prfm pstl1keep,[x0,#24]; add x0,x0,%[LDC],LSL #3\n\t"
    "prfm pstl1keep,[x0]; prfm pstl1keep,[x0,#24]; add x0,x0,%[LDC],LSL #3\n\t"
    "prfm pstl1keep,[x0]; prfm pstl1keep,[x0,#24]; add x0,x0,%[LDC],LSL #3\n\t"
    "prfm pstl1keep,[x0]; prfm pstl1keep,[x0,#24]; add x0,x0,%[LDC],LSL #3\n\t"
    "prfm pstl1keep,[x0]; prfm pstl1keep,[x0,#24]; add x0,x0,%[LDC],LSL #3\n\t"
    "prfm pstl1keep,[x0]; prfm pstl1keep,[x0,#24]; add x0,x0,%[LDC],LSL #3\n\t"
    "prfm pstl1keep,[x0]; prfm pstl1keep,[x0,#24]; add x0,x0,%[LDC],LSL #3\n\t"
    "prfm pstl1keep,[x0]; prfm pstl1keep,[x0,#24]; add x0,x0,%[LDC],LSL #3\n\t"
    "prfm pstl1keep,[x0]; prfm pstl1keep,[x0,#24]; add x0,x0,%[LDC],LSL #3\n\t"
    "prfm pstl1keep,[x0]; prfm pstl1keep,[x0,#24]; add x0,x0,%[LDC],LSL #3\n\t"
    "prfm pstl1keep,[x0]; prfm pstl1keep,[x0,#24]; add x0,x0,%[LDC],LSL #3\n\t"
    "prfm pstl1keep,[x0]; prfm pstl1keep,[x0,#24]\n\t"
   ::[C]"r"(C), [LDC]"r"(LDC):"x0");

  /** 3 pointers to 3 submatrices of sb respectively */
  const FLOAT *b1_ = sb;
  const FLOAT *b2_ = sb + K * 4;
  const FLOAT *b3_ = sb + K * 8;

  /** register mapping of 4x12 elements of C, row-id ==> coordinate-M, column-id ==> coordinate-N */
  /** v8.d[0] v10.d[0] v12.d[0] v14.d[0] v16.d[0] v18.d[0] v20.d[0] v22.d[0] v24.d[0] v26.d[0] v28.d[0] v30.d[0] */
  /** v8.d[1] v10.d[1] v12.d[1] v14.d[1] v16.d[1] v18.d[1] v20.d[1] v22.d[1] v24.d[1] v26.d[1] v28.d[1] v30.d[1] */
  /** v9.d[0] v11.d[0] v13.d[0] v15.d[0] v17.d[0] v19.d[0] v21.d[0] v23.d[0] v25.d[0] v27.d[0] v29.d[0] v31.d[0] */
  /** v9.d[1] v11.d[1] v13.d[1] v15.d[1] v17.d[1] v19.d[1] v21.d[1] v23.d[1] v25.d[1] v27.d[1] v29.d[1] v31.d[1] */

  __asm__ __volatile__(
    "cmp %[K],#0\n\t"
    /** fill registers holding elements of C with 0.0 */
    "movi v8.16b,#0; movi v9.16b,#0; movi v10.16b,#0; movi v11.16b,#0\n\t"
    "movi v12.16b,#0; movi v13.16b,#0; movi v14.16b,#0; movi v15.16b,#0\n\t"
    "movi v16.16b,#0; movi v17.16b,#0; movi v18.16b,#0; movi v19.16b,#0\n\t"
    "movi v20.16b,#0; movi v21.16b,#0; movi v22.16b,#0; movi v23.16b,#0\n\t"
    "movi v24.16b,#0; movi v25.16b,#0; movi v26.16b,#0; movi v27.16b,#0\n\t"
    "movi v28.16b,#0; movi v29.16b,#0; movi v30.16b,#0; movi v31.16b,#0\n\t"
    "beq 4f; cmp %[K],#2\n\t"
    /** register v0-v3 for loading A, v4-v7 for loading B, x0 for transporting data */
    "ldp q0,q1,[%[sa]]; ldp q4,q5,[%[b1_]]\n\t"
    "ldr d6,[%[b2_]]; ldr x0,[%[b2_],#8]\n\t"
    "blt 3f; beq 2f\n\t"
    "1:\n\t"
    /** main loop with unroll_k = 2, specially designed for cortex-A53 NEON pipeline */
    "ldr d7,[%[b2_],#16]; fmov v6.d[1],x0\n\t"
    "fmla v8.2d,v0.2d,v4.d[0]; ldr x0,[%[b2_],#24]\n\t"
    "fmla v9.2d,v1.2d,v4.d[0]; prfm pldl1keep,[%[sa],#128]\n\t"
    "fmla v10.2d,v0.2d,v4.d[1]\n\t"
    "ldr d2,[%[sa],#32]; fmov v7.d[1],x0\n\t"
    "fmla v11.2d,v1.2d,v4.d[1]; ldr x0,[%[sa],#40]\n\t"
    "fmla v12.2d,v0.2d,v5.d[0]\n\t"
    "fmla v13.2d,v1.2d,v5.d[0]\n\t"
    "ldr d4,[%[b3_]]; fmov v2.d[1],x0\n\t"
    "fmla v14.2d,v0.2d,v5.d[1]; ldr x0,[%[b3_],#8]\n\t"
    "fmla v15.2d,v1.2d,v5.d[1]\n\t"
    "fmla v16.2d,v0.2d,v6.d[0]\n\t"
    "ldr d5,[%[b3_],#16]; fmov v4.d[1],x0\n\t"
    "fmla v17.2d,v1.2d,v6.d[0]; ldr x0,[%[b3_],#24]\n\t"
    "fmla v18.2d,v0.2d,v6.d[1]\n\t"
    "fmla v19.2d,v1.2d,v6.d[1]\n\t"
    "ldr d3,[%[sa],#48]; fmov v5.d[1],x0\n\t"
    "fmla v20.2d,v0.2d,v7.d[0]; ldr x0,[%[sa],#56]\n\t"
    "fmla v21.2d,v1.2d,v7.d[0]; add %[sa],%[sa],#64\n\t"
    "fmla v22.2d,v0.2d,v7.d[1]\n\t"
    "ldr d6,[%[b1_],#32]; fmov v3.d[1],x0\n\t"
    "fmla v23.2d,v1.2d,v7.d[1]; ldr x0,[%[b1_],#40]\n\t"
    "fmla v24.2d,v0.2d,v4.d[0]; prfm pldl1keep,[%[b1_],#128]\n\t"
    "fmla v25.2d,v1.2d,v4.d[0]\n\t"
    "ldr d7,[%[b1_],#48]; fmov v6.d[1],x0\n\t"
    "fmla v26.2d,v0.2d,v4.d[1]; ldr x0,[%[b1_],#56]\n\t"
    "fmla v27.2d,v1.2d,v4.d[1]; add %[b1_],%[b1_],#64\n\t"
    "fmla v28.2d,v0.2d,v5.d[0]\n\t"
    "ldr d4,[%[b2_],#32]; fmov v7.d[1],x0\n\t"
    "fmla v29.2d,v1.2d,v5.d[0]; ldr x0,[%[b2_],#40]\n\t"
    "fmla v30.2d,v0.2d,v5.d[1]; prfm pldl1keep,[%[b2_],#128]\n\t"
    "fmla v31.2d,v1.2d,v5.d[1]\n\t"
    "ldr d0,[%[sa]]; fmov v4.d[1],x0\n\t"
    "fmla v8.2d,v2.2d,v6.d[0]; ldr x0,[%[sa],#8]\n\t"
    "fmla v9.2d,v3.2d,v6.d[0]\n\t"
    "fmla v10.2d,v2.2d,v6.d[1]\n\t"
    "ldr d5,[%[b2_],#48]; fmov v0.d[1],x0\n\t"
    "fmla v11.2d,v3.2d,v6.d[1]; ldr x0,[%[b2_],#56]\n\t"
    "fmla v12.2d,v2.2d,v7.d[0]; add %[b2_],%[b2_],#64\n\t"
    "fmla v13.2d,v3.2d,v7.d[0]\n\t"
    "ldr d6,[%[b3_],#32]; fmov v5.d[1],x0\n\t"
    "fmla v14.2d,v2.2d,v7.d[1]; ldr x0,[%[b3_],#40]\n\t"
    "fmla v15.2d,v3.2d,v7.d[1]; prfm pldl1keep,[%[b3_],#128]\n\t"
    "fmla v16.2d,v2.2d,v4.d[0]\n\t"
    "ldr d7,[%[b3_],#48]; fmov v6.d[1],x0\n\t"
    "fmla v17.2d,v3.2d,v4.d[0]; ldr x0,[%[b3_],#56]\n\t"
    "fmla v18.2d,v2.2d,v4.d[1]; add %[b3_],%[b3_],#64\n\t"
    "fmla v19.2d,v3.2d,v4.d[1]\n\t"
    "ldr d1,[%[sa],#16]; fmov v7.d[1],x0\n\t"
    "fmla v20.2d,v2.2d,v5.d[0]; ldr x0,[%[sa],#24]\n\t"
    "fmla v21.2d,v3.2d,v5.d[0]\n\t"
    "fmla v22.2d,v2.2d,v5.d[1]\n\t"
    "ldr d4,[%[b1_]]; fmov v1.d[1],x0\n\t"
    "fmla v23.2d,v3.2d,v5.d[1]; ldr x0,[%[b1_],#8]\n\t"
    "fmla v24.2d,v2.2d,v6.d[0]\n\t"
    "fmla v25.2d,v3.2d,v6.d[0]\n\t"
    "ldr d5,[%[b1_],#16]; fmov v4.d[1],x0\n\t"
    "fmla v26.2d,v2.2d,v6.d[1]; ldr x0,[%[b1_],#24]\n\t"
    "fmla v27.2d,v3.2d,v6.d[1]; sub %[K],%[K],#2\n\t"
    "fmla v28.2d,v2.2d,v7.d[0]\n\t"
    "ldr d6,[%[b2_]]; fmov v5.d[1],x0\n\t"
    "fmla v29.2d,v3.2d,v7.d[0]; ldr x0,[%[b2_],#8]\n\t"
    "fmla v30.2d,v2.2d,v7.d[1]; cmp %[K],#2\n\t"
    "fmla v31.2d,v3.2d,v7.d[1]\n\t"
    "bgt 1b; blt 3f\n\t"
    "2:\n\t"
    /** tail part with k = 2 */
    "ldr d7,[%[b2_],#16]; fmov v6.d[1],x0\n\t"
    "fmla v8.2d,v0.2d,v4.d[0]; ldr x0,[%[b2_],#24]\n\t"
    "fmla v9.2d,v1.2d,v4.d[0]; prfm pldl1keep,[%[sa],#128]\n\t"
    "fmla v10.2d,v0.2d,v4.d[1]\n\t"
    "ldr d2,[%[sa],#32]; fmov v7.d[1],x0\n\t"
    "fmla v11.2d,v1.2d,v4.d[1]; ldr x0,[%[sa],#40]\n\t"
    "fmla v12.2d,v0.2d,v5.d[0]\n\t"
    "fmla v13.2d,v1.2d,v5.d[0]\n\t"
    "ldr d4,[%[b3_]]; fmov v2.d[1],x0\n\t"
    "fmla v14.2d,v0.2d,v5.d[1]; ldr x0,[%[b3_],#8]\n\t"
    "fmla v15.2d,v1.2d,v5.d[1]\n\t"
    "fmla v16.2d,v0.2d,v6.d[0]\n\t"
    "ldr d5,[%[b3_],#16]; fmov v4.d[1],x0\n\t"
    "fmla v17.2d,v1.2d,v6.d[0]; ldr x0,[%[b3_],#24]\n\t"
    "fmla v18.2d,v0.2d,v6.d[1]\n\t"
    "fmla v19.2d,v1.2d,v6.d[1]\n\t"
    "ldr d3,[%[sa],#48]; fmov v5.d[1],x0\n\t"
    "fmla v20.2d,v0.2d,v7.d[0]; ldr x0,[%[sa],#56]\n\t"
    "fmla v21.2d,v1.2d,v7.d[0]; add %[sa],%[sa],#64\n\t"
    "fmla v22.2d,v0.2d,v7.d[1]\n\t"
    "ldr d6,[%[b1_],#32]; fmov v3.d[1],x0\n\t"
    "fmla v23.2d,v1.2d,v7.d[1]; ldr x0,[%[b1_],#40]\n\t"
    "fmla v24.2d,v0.2d,v4.d[0]\n\t"
    "fmla v25.2d,v1.2d,v4.d[0]\n\t"
    "ldr d7,[%[b1_],#48]; fmov v6.d[1],x0\n\t"
    "fmla v26.2d,v0.2d,v4.d[1]; ldr x0,[%[b1_],#56]\n\t"
    "fmla v27.2d,v1.2d,v4.d[1]; add %[b1_],%[b1_],#64\n\t"
    "fmla v28.2d,v0.2d,v5.d[0]\n\t"
    "ldr d4,[%[b2_],#32]; fmov v7.d[1],x0\n\t"
    "fmla v29.2d,v1.2d,v5.d[0]; ldr x0,[%[b2_],#40]\n\t"
    "fmla v30.2d,v0.2d,v5.d[1]\n\t"
    "fmla v31.2d,v1.2d,v5.d[1]\n\t"
    "fmov v4.d[1],x0\n\t"
    "fmla v8.2d,v2.2d,v6.d[0]\n\t"
    "fmla v9.2d,v3.2d,v6.d[0]\n\t"
    "fmla v10.2d,v2.2d,v6.d[1]\n\t"
    "ldr d5,[%[b2_],#48]\n\t"
    "fmla v11.2d,v3.2d,v6.d[1]; ldr x0,[%[b2_],#56]\n\t"
    "fmla v12.2d,v2.2d,v7.d[0]; add %[b2_],%[b2_],#64\n\t"
    "fmla v13.2d,v3.2d,v7.d[0]\n\t"
    "ldr d6,[%[b3_],#32]; fmov v5.d[1],x0\n\t"
    "fmla v14.2d,v2.2d,v7.d[1]; ldr x0,[%[b3_],#40]\n\t"
    "fmla v15.2d,v3.2d,v7.d[1]\n\t"
    "fmla v16.2d,v2.2d,v4.d[0]\n\t"
    "ldr d7,[%[b3_],#48]; fmov v6.d[1],x0\n\t"
    "fmla v17.2d,v3.2d,v4.d[0]; ldr x0,[%[b3_],#56]\n\t"
    "fmla v18.2d,v2.2d,v4.d[1]; add %[b3_],%[b3_],#64\n\t"
    "fmla v19.2d,v3.2d,v4.d[1]\n\t"
    "fmov v7.d[1],x0\n\t"
    "fmla v20.2d,v2.2d,v5.d[0]\n\t"
    "fmla v21.2d,v3.2d,v5.d[0]\n\t"
    "fmla v22.2d,v2.2d,v5.d[1]\n\t"
    "fmla v23.2d,v3.2d,v5.d[1]\n\t"
    "fmla v24.2d,v2.2d,v6.d[0]\n\t"
    "fmla v25.2d,v3.2d,v6.d[0]\n\t"
    "fmla v26.2d,v2.2d,v6.d[1]\n\t"
    "fmla v27.2d,v3.2d,v6.d[1]; sub %[K],%[K],#2\n\t"
    "fmla v28.2d,v2.2d,v7.d[0]\n\t"
    "fmla v29.2d,v3.2d,v7.d[0]\n\t"
    "fmla v30.2d,v2.2d,v7.d[1]\n\t"
    "fmla v31.2d,v3.2d,v7.d[1]\n\t"
    "b 4f\n\t"
    "3:\n\t"
    /** tail part with k = 1 */
    "ldr d7,[%[b2_],#16]; fmov v6.d[1],x0\n\t"
    "fmla v8.2d,v0.2d,v4.d[0]; ldr x0,[%[b2_],#24]\n\t"
    "fmla v9.2d,v1.2d,v4.d[0]; add %[b2_],%[b2_],#32\n\t"
    "fmla v10.2d,v0.2d,v4.d[1]\n\t"
    "fmov v7.d[1],x0\n\t"
    "fmla v11.2d,v1.2d,v4.d[1]; add %[sa],%[sa],#32\n\t"
    "fmla v12.2d,v0.2d,v5.d[0]; add %[b1_],%[b1_],#32\n\t"
    "fmla v13.2d,v1.2d,v5.d[0]; sub %[K],%[K],#1\n\t"
    "ldr d4,[%[b3_]]\n\t"
    "fmla v14.2d,v0.2d,v5.d[1]; ldr x0,[%[b3_],#8]\n\t"
    "fmla v15.2d,v1.2d,v5.d[1]\n\t"
    "fmla v16.2d,v0.2d,v6.d[0]\n\t"
    "ldr d5,[%[b3_],#16]; fmov v4.d[1],x0\n\t"
    "fmla v17.2d,v1.2d,v6.d[0]; ldr x0,[%[b3_],#24]\n\t"
    "fmla v18.2d,v0.2d,v6.d[1]; add %[b3_],%[b3_],#32\n\t"
    "fmla v19.2d,v1.2d,v6.d[1]\n\t"
    "fmov v5.d[1],x0\n\t"
    "fmla v20.2d,v0.2d,v7.d[0]\n\t"
    "fmla v21.2d,v1.2d,v7.d[0]\n\t"
    "fmla v22.2d,v0.2d,v7.d[1]\n\t"
    "fmla v23.2d,v1.2d,v7.d[1]\n\t"
    "fmla v24.2d,v0.2d,v4.d[0]\n\t"
    "fmla v25.2d,v1.2d,v4.d[0]\n\t"
    "fmla v26.2d,v0.2d,v4.d[1]\n\t"
    "fmla v27.2d,v1.2d,v4.d[1]\n\t"
    "fmla v28.2d,v0.2d,v5.d[0]\n\t"
    "fmla v29.2d,v1.2d,v5.d[0]\n\t"
    "fmla v30.2d,v0.2d,v5.d[1]\n\t"
    "fmla v31.2d,v1.2d,v5.d[1]\n\t"
    /** store 4x12 elements to C */
    "4:\n\t"
    "ldr d0,%[alpha]; add x0,%[C],%[LDC],LSL #3\n\t"
    "ldp q1,q2,[%[C]]; ldp q3,q4,[x0]\n\t"
    "fmla v1.2d,v8.2d,v0.d[0]; fmla v2.2d,v9.2d,v0.d[0]\n\t"
    "fmla v3.2d,v10.2d,v0.d[0]; fmla v4.2d,v11.2d,v0.d[0]\n\t"
    "stp q1,q2,[%[C]]; add %[C],%[C],%[LDC],LSL #4\n\t"
    "stp q3,q4,[x0]; add x0,x0,%[LDC],LSL #4\n\t"
    "ldp q1,q2,[%[C]]; ldp q3,q4,[x0]\n\t"
    "fmla v1.2d,v12.2d,v0.d[0]; fmla v2.2d,v13.2d,v0.d[0]\n\t"
    "fmla v3.2d,v14.2d,v0.d[0]; fmla v4.2d,v15.2d,v0.d[0]\n\t"
    "stp q1,q2,[%[C]]; add %[C],%[C],%[LDC],LSL #4\n\t"
    "stp q3,q4,[x0]; add x0,x0,%[LDC],LSL #4\n\t"
    "ldp q1,q2,[%[C]]; ldp q3,q4,[x0]\n\t"
    "fmla v1.2d,v16.2d,v0.d[0]; fmla v2.2d,v17.2d,v0.d[0]\n\t"
    "fmla v3.2d,v18.2d,v0.d[0]; fmla v4.2d,v19.2d,v0.d[0]\n\t"
    "stp q1,q2,[%[C]]; add %[C],%[C],%[LDC],LSL #4\n\t"
    "stp q3,q4,[x0]; add x0,x0,%[LDC],LSL #4\n\t"
    "ldp q1,q2,[%[C]]; ldp q3,q4,[x0]\n\t"
    "fmla v1.2d,v20.2d,v0.d[0]; fmla v2.2d,v21.2d,v0.d[0]\n\t"
    "fmla v3.2d,v22.2d,v0.d[0]; fmla v4.2d,v23.2d,v0.d[0]\n\t"
    "stp q1,q2,[%[C]]; add %[C],%[C],%[LDC],LSL #4\n\t"
    "stp q3,q4,[x0]; add x0,x0,%[LDC],LSL #4\n\t"
    "ldp q1,q2,[%[C]]; ldp q3,q4,[x0]\n\t"
    "fmla v1.2d,v24.2d,v0.d[0]; fmla v2.2d,v25.2d,v0.d[0]\n\t"
    "fmla v3.2d,v26.2d,v0.d[0]; fmla v4.2d,v27.2d,v0.d[0]\n\t"
    "stp q1,q2,[%[C]]; add %[C],%[C],%[LDC],LSL #4\n\t"
    "stp q3,q4,[x0]; add x0,x0,%[LDC],LSL #4\n\t"
    "ldp q1,q2,[%[C]]; ldp q3,q4,[x0]\n\t"
    "fmla v1.2d,v28.2d,v0.d[0]; fmla v2.2d,v29.2d,v0.d[0]\n\t"
    "fmla v3.2d,v30.2d,v0.d[0]; fmla v4.2d,v31.2d,v0.d[0]\n\t"
    "stp q1,q2,[%[C]]; stp q3,q4,[x0]\n\t"
   :[sa]"+r"(sa), [b1_]"+r"(b1_), [b2_]"+r"(b2_), [b3_]"+r"(b3_), [C]"+r"(C), [K]"+r"(K)
   :[LDC]"r"(LDC), [alpha]"m"(alpha)
   :"cc", "memory", "x0", "v0", "v1", "v2", "v3", "v4", "v5", "v6", "v7",
    "v8", "v9", "v10", "v11", "v12", "v13", "v14", "v15", "v16", "v17", "v18", "v19",
    "v20", "v21", "v22", "v23", "v24", "v25", "v26", "v27", "v28", "v29", "v30", "v31");
}

/**********************************************************
 * Operation:
  C[0] += alpha * up[0]; C[1] += alpha * up[1];
  C[2] += alpha * down[0]; C[3] += alpha * down[1];
 *********************************************************/
static inline void dgemm_store_m4n1(FLOAT *C, float64x2_t up, float64x2_t down, FLOAT alpha) {
  float64x2_t t1 = vld1q_f64(C), t2 = vld1q_f64(C + 2);
  t1 = vfmaq_n_f64(t1, up, alpha);
  t2 = vfmaq_n_f64(t2, down, alpha);
  vst1q_f64(C, t1);
  vst1q_f64(C + 2, t2);
}

/**********************************************************
 * Function: dgemm_kernel_arm64_4x4_m4n8
 * Operation: C[4][8] += alpha * sa[4][K] * sb[K][8]
 * Matrix orders:
 *    sa: column-major (leading dimension == 4)
 *    sb: 2 concatenated row-major 4-column submatrices
 *    C: column-major (leading dimension == LDC)
 *********************************************************/
static inline void dgemm_kernel_arm64_4x4_m4n8(
  const FLOAT *sa, const FLOAT *sb, FLOAT *C,
  BLASLONG K, BLASLONG LDC, FLOAT alpha) {

  const FLOAT *b1_ = sb;
  const FLOAT *b2_ = sb + K * 4;

  /** register naming: c + m_id + n_id, m_id=1~2, n_id=1~8 */
  float64x2_t c11, c12, c13, c14, c15, c16, c17, c18;
  float64x2_t c21, c22, c23, c24, c25, c26, c27, c28;
  c11 = c12 = c13 = c14 = c15 = c16 = c17 = c18 = vdupq_n_f64(0);
  c21 = c22 = c23 = c24 = c25 = c26 = c27 = c28 = vdupq_n_f64(0);

  for (; K; K--) {
    float64x2_t a1 = vld1q_f64(sa);
    float64x2_t a2 = vld1q_f64(sa + 2); sa += 4;

    float64x2_t b1 = vld1q_f64(b1_);
    c11 = vfmaq_laneq_f64(c11, a1, b1, 0);
    c21 = vfmaq_laneq_f64(c21, a2, b1, 0);
    c12 = vfmaq_laneq_f64(c12, a1, b1, 1);
    c22 = vfmaq_laneq_f64(c22, a2, b1, 1);

    float64x2_t b2 = vld1q_f64(b1_ + 2); b1_ += 4;
    c13 = vfmaq_laneq_f64(c13, a1, b2, 0);
    c23 = vfmaq_laneq_f64(c23, a2, b2, 0);
    c14 = vfmaq_laneq_f64(c14, a1, b2, 1);
    c24 = vfmaq_laneq_f64(c24, a2, b2, 1);

    float64x2_t b3 = vld1q_f64(b2_);
    c15 = vfmaq_laneq_f64(c15, a1, b3, 0);
    c25 = vfmaq_laneq_f64(c25, a2, b3, 0);
    c16 = vfmaq_laneq_f64(c16, a1, b3, 1);
    c26 = vfmaq_laneq_f64(c26, a2, b3, 1);

    float64x2_t b4 = vld1q_f64(b2_ + 2); b2_ += 4;
    c17 = vfmaq_laneq_f64(c17, a1, b4, 0);
    c27 = vfmaq_laneq_f64(c27, a2, b4, 0);
    c18 = vfmaq_laneq_f64(c18, a1, b4, 1);
    c28 = vfmaq_laneq_f64(c28, a2, b4, 1);
  }

  dgemm_store_m4n1(C, c11, c21, alpha); C += LDC;
  dgemm_store_m4n1(C, c12, c22, alpha); C += LDC;
  dgemm_store_m4n1(C, c13, c23, alpha); C += LDC;
  dgemm_store_m4n1(C, c14, c24, alpha); C += LDC;
  dgemm_store_m4n1(C, c15, c25, alpha); C += LDC;
  dgemm_store_m4n1(C, c16, c26, alpha); C += LDC;
  dgemm_store_m4n1(C, c17, c27, alpha); C += LDC;
  dgemm_store_m4n1(C, c18, c28, alpha);
}

/**********************************************************
 * Function: dgemm_kernel_arm64_4x4_m4n4
 * Operation: C[4][4] += alpha * sa[4][K] * sb[K][4]
 * Matrix orders:
 *    sa: column-major (leading dimension == 4)
 *    sb: row-major (leading dimension == 4)
 *    C: column-major (leading dimension == LDC)
 *********************************************************/
static inline void dgemm_kernel_arm64_4x4_m4n4(
  const FLOAT *sa, const FLOAT *sb, FLOAT *C,
  BLASLONG K, BLASLONG LDC, FLOAT alpha) {

  float64x2_t c11, c21, c12, c22, c13, c23, c14, c24;
  c11 = c21 = c12 = c22 = c13 = c23 = c14 = c24 = vdupq_n_f64(0);

  for (; K; K--) {
    float64x2_t a1 = vld1q_f64(sa);
    float64x2_t a2 = vld1q_f64(sa + 2); sa += 4;
    float64x2_t b1 = vld1q_f64(sb);
    float64x2_t b2 = vld1q_f64(sb + 2); sb += 4;
    c11 = vfmaq_laneq_f64(c11, a1, b1, 0);
    c21 = vfmaq_laneq_f64(c21, a2, b1, 0);
    c12 = vfmaq_laneq_f64(c12, a1, b1, 1);
    c22 = vfmaq_laneq_f64(c22, a2, b1, 1);
    c13 = vfmaq_laneq_f64(c13, a1, b2, 0);
    c23 = vfmaq_laneq_f64(c23, a2, b2, 0);
    c14 = vfmaq_laneq_f64(c14, a1, b2, 1);
    c24 = vfmaq_laneq_f64(c24, a2, b2, 1);
  }

  dgemm_store_m4n1(C, c11, c21, alpha); C += LDC;
  dgemm_store_m4n1(C, c12, c22, alpha); C += LDC;
  dgemm_store_m4n1(C, c13, c23, alpha); C += LDC;
  dgemm_store_m4n1(C, c14, c24, alpha);
}

static inline void dgemm_kernel_arm64_4x4_m4n2(
  const FLOAT *sa, const FLOAT *sb, FLOAT *C,
  BLASLONG K, BLASLONG LDC, FLOAT alpha) {

  float64x2_t c11_1, c11_2, c21_1, c21_2, c12_1, c12_2, c22_1, c22_2;
  c11_1 = c11_2 = c21_1 = c21_2 = c12_1 = c12_2 = c22_1 = c22_2 = vdupq_n_f64(0);

  for (; K > 1; K -= 2) {
    float64x2_t b1 = vld1q_f64(sb), b2 = vld1q_f64(sb + 2); sb += 4;
    float64x2_t a1_1 = vld1q_f64(sa), a2_1 = vld1q_f64(sa + 2),
      a1_2 = vld1q_f64(sa + 4), a2_2 = vld1q_f64(sa + 6); sa += 8;
    c11_1 = vfmaq_laneq_f64(c11_1, a1_1, b1, 0);
    c21_1 = vfmaq_laneq_f64(c21_1, a2_1, b1, 0);
    c12_1 = vfmaq_laneq_f64(c12_1, a1_1, b1, 1);
    c22_1 = vfmaq_laneq_f64(c22_1, a2_1, b1, 1);
    c11_2 = vfmaq_laneq_f64(c11_2, a1_2, b2, 0);
    c21_2 = vfmaq_laneq_f64(c21_2, a2_2, b2, 0);
    c12_2 = vfmaq_laneq_f64(c12_2, a1_2, b2, 1);
    c22_2 = vfmaq_laneq_f64(c22_2, a2_2, b2, 1);
  }
  c11_1 = vaddq_f64(c11_1, c11_2);
  c21_1 = vaddq_f64(c21_1, c21_2);
  c12_1 = vaddq_f64(c12_1, c12_2);
  c22_1 = vaddq_f64(c22_1, c22_2);
  if (K) {
    float64x2_t b1 = vld1q_f64(sb); sb += 2;
    float64x2_t a1 = vld1q_f64(sa), a2 = vld1q_f64(sa + 2); sa += 4;
    c11_1 = vfmaq_laneq_f64(c11_1, a1, b1, 0);
    c21_1 = vfmaq_laneq_f64(c21_1, a2, b1, 0);
    c12_1 = vfmaq_laneq_f64(c12_1, a1, b1, 1);
    c22_1 = vfmaq_laneq_f64(c22_1, a2, b1, 1);
  }

  dgemm_store_m4n1(C, c11_1, c21_1, alpha); C += LDC;
  dgemm_store_m4n1(C, c12_1, c22_1, alpha);
}

static inline void dgemm_kernel_arm64_4x4_m4n1(
  const FLOAT *sa, const FLOAT *sb, FLOAT *C,
  BLASLONG K, BLASLONG LDC, FLOAT alpha) {

  float64x2_t c11_1, c11_2, c21_1, c21_2;
  c11_1 = c11_2 = c21_1 = c21_2 = vdupq_n_f64(0);

  for (; K > 1; K -= 2) {
    float64x2_t b1 = vld1q_f64(sb); sb += 2;
    c11_1 = vfmaq_laneq_f64(c11_1, vld1q_f64(sa), b1, 0);
    c21_1 = vfmaq_laneq_f64(c21_1, vld1q_f64(sa + 2), b1, 0);
    c11_2 = vfmaq_laneq_f64(c11_2, vld1q_f64(sa + 4), b1, 1);
    c21_2 = vfmaq_laneq_f64(c21_2, vld1q_f64(sa + 6), b1, 1);
    sa += 8;
  }
  c11_1 = vaddq_f64(c11_1, c11_2);
  c21_1 = vaddq_f64(c21_1, c21_2);
  if (K) {
    double b1 = *sb++;
    c11_1 = vfmaq_n_f64(c11_1, vld1q_f64(sa), b1);
    c21_1 = vfmaq_n_f64(c21_1, vld1q_f64(sa + 2), b1);
    sa += 4;
  }

  dgemm_store_m4n1(C, c11_1, c21_1, alpha);
}

static inline void dgemm_kernel_arm64_4x4_m2n12(
  const FLOAT *sa, const FLOAT *sb, FLOAT *c,
  BLASLONG K, BLASLONG LDC, FLOAT alpha) {

  float64x2_t c01, c02, c03, c04, c11, c12, c13, c14, c21, c22, c23, c24;
  c01 = c02 = c03 = c04 = c11 = c12 = c13 = c14 =
    c21 = c22 = c23 = c24 = vdupq_n_f64(0);

  const FLOAT *b1_ = sb;
  const FLOAT *b2_ = sb + 4 * K;
  const FLOAT *b3_ = b2_ + 4 * K;

  for (; K; K--) {
    const float64x2_t a1 = vld1q_f64(sa); sa += 2;

    float64x2_t b1 = vld1q_f64(b1_), b2 = vld1q_f64(b1_ + 2); b1_ += 4;
    c01 = vfmaq_laneq_f64(c01, a1, b1, 0);
    c02 = vfmaq_laneq_f64(c02, a1, b1, 1);
    c03 = vfmaq_laneq_f64(c03, a1, b2, 0);
    c04 = vfmaq_laneq_f64(c04, a1, b2, 1);

    b1 = vld1q_f64(b2_); b2 = vld1q_f64(b2_ + 2); b2_ += 4;
    c11 = vfmaq_laneq_f64(c11, a1, b1, 0);
    c12 = vfmaq_laneq_f64(c12, a1, b1, 1);
    c13 = vfmaq_laneq_f64(c13, a1, b2, 0);
    c14 = vfmaq_laneq_f64(c14, a1, b2, 1);

    b1 = vld1q_f64(b3_); b2 = vld1q_f64(b3_ + 2); b3_ += 4;
    c21 = vfmaq_laneq_f64(c21, a1, b1, 0);
    c22 = vfmaq_laneq_f64(c22, a1, b1, 1);
    c23 = vfmaq_laneq_f64(c23, a1, b2, 0);
    c24 = vfmaq_laneq_f64(c24, a1, b2, 1);
  }

  vst1q_f64(c, vfmaq_n_f64(vld1q_f64(c), c01, alpha)); c += LDC;
  vst1q_f64(c, vfmaq_n_f64(vld1q_f64(c), c02, alpha)); c += LDC;
  vst1q_f64(c, vfmaq_n_f64(vld1q_f64(c), c03, alpha)); c += LDC;
  vst1q_f64(c, vfmaq_n_f64(vld1q_f64(c), c04, alpha)); c += LDC;
  vst1q_f64(c, vfmaq_n_f64(vld1q_f64(c), c11, alpha)); c += LDC;
  vst1q_f64(c, vfmaq_n_f64(vld1q_f64(c), c12, alpha)); c += LDC;
  vst1q_f64(c, vfmaq_n_f64(vld1q_f64(c), c13, alpha)); c += LDC;
  vst1q_f64(c, vfmaq_n_f64(vld1q_f64(c), c14, alpha)); c += LDC;
  vst1q_f64(c, vfmaq_n_f64(vld1q_f64(c), c21, alpha)); c += LDC;
  vst1q_f64(c, vfmaq_n_f64(vld1q_f64(c), c22, alpha)); c += LDC;
  vst1q_f64(c, vfmaq_n_f64(vld1q_f64(c), c23, alpha)); c += LDC;
  vst1q_f64(c, vfmaq_n_f64(vld1q_f64(c), c24, alpha));
}

static inline void dgemm_kernel_arm64_4x4_m2n8(
  const FLOAT *sa, const FLOAT *sb, FLOAT *c,
  BLASLONG K, BLASLONG LDC, FLOAT alpha) {

  float64x2_t c01, c02, c03, c04, c11, c12, c13, c14;
  c01 = c02 = c03 = c04 = c11 = c12 = c13 = c14 = vdupq_n_f64(0);

  const FLOAT *b1_ = sb;
  const FLOAT *b2_ = sb + 4 * K;

  for (; K; K--) {
    const float64x2_t a1 = vld1q_f64(sa); sa += 2;

    float64x2_t b1 = vld1q_f64(b1_), b2 = vld1q_f64(b1_ + 2); b1_ += 4;
    c01 = vfmaq_laneq_f64(c01, a1, b1, 0);
    c02 = vfmaq_laneq_f64(c02, a1, b1, 1);
    c03 = vfmaq_laneq_f64(c03, a1, b2, 0);
    c04 = vfmaq_laneq_f64(c04, a1, b2, 1);

    b1 = vld1q_f64(b2_); b2 = vld1q_f64(b2_ + 2); b2_ += 4;
    c11 = vfmaq_laneq_f64(c11, a1, b1, 0);
    c12 = vfmaq_laneq_f64(c12, a1, b1, 1);
    c13 = vfmaq_laneq_f64(c13, a1, b2, 0);
    c14 = vfmaq_laneq_f64(c14, a1, b2, 1);
  }

  vst1q_f64(c, vfmaq_n_f64(vld1q_f64(c), c01, alpha)); c += LDC;
  vst1q_f64(c, vfmaq_n_f64(vld1q_f64(c), c02, alpha)); c += LDC;
  vst1q_f64(c, vfmaq_n_f64(vld1q_f64(c), c03, alpha)); c += LDC;
  vst1q_f64(c, vfmaq_n_f64(vld1q_f64(c), c04, alpha)); c += LDC;
  vst1q_f64(c, vfmaq_n_f64(vld1q_f64(c), c11, alpha)); c += LDC;
  vst1q_f64(c, vfmaq_n_f64(vld1q_f64(c), c12, alpha)); c += LDC;
  vst1q_f64(c, vfmaq_n_f64(vld1q_f64(c), c13, alpha)); c += LDC;
  vst1q_f64(c, vfmaq_n_f64(vld1q_f64(c), c14, alpha));
}

static inline void dgemm_kernel_arm64_4x4_m2n4(
  const FLOAT *sa, const FLOAT *sb, FLOAT *c,
  BLASLONG K, BLASLONG LDC, FLOAT alpha) {

  float64x2_t c1_1, c1_2, c2_1, c2_2, c3_1, c3_2, c4_1, c4_2;
  c1_1 = c1_2 = c2_1 = c2_2 = c3_1 = c3_2 = c4_1 = c4_2 = vdupq_n_f64(0);

  for (; K > 1; K -= 2) {
    float64x2_t a1 = vld1q_f64(sa), a2 = vld1q_f64(sa + 2); sa += 4;
    float64x2_t b1_1 = vld1q_f64(sb), b2_1 = vld1q_f64(sb + 2);
    float64x2_t b1_2 = vld1q_f64(sb + 4), b2_2 = vld1q_f64(sb + 6); sb += 8;

    c1_1 = vfmaq_laneq_f64(c1_1, a1, b1_1, 0);
    c2_1 = vfmaq_laneq_f64(c2_1, a1, b1_1, 1);
    c3_1 = vfmaq_laneq_f64(c3_1, a1, b2_1, 0);
    c4_1 = vfmaq_laneq_f64(c4_1, a1, b2_1, 1);

    c1_2 = vfmaq_laneq_f64(c1_2, a2, b1_2, 0);
    c2_2 = vfmaq_laneq_f64(c2_2, a2, b1_2, 1);
    c3_2 = vfmaq_laneq_f64(c3_2, a2, b2_2, 0);
    c4_2 = vfmaq_laneq_f64(c4_2, a2, b2_2, 1);
  }
  c1_1 = vaddq_f64(c1_1, c1_2);
  c2_1 = vaddq_f64(c2_1, c2_2);
  c3_1 = vaddq_f64(c3_1, c3_2);
  c4_1 = vaddq_f64(c4_1, c4_2);
  if (K) {
    float64x2_t a1 = vld1q_f64(sa); sa += 2;
    float64x2_t b1 = vld1q_f64(sb), b2 = vld1q_f64(sb + 2); sb += 4;
    c1_1 = vfmaq_laneq_f64(c1_1, a1, b1, 0);
    c2_1 = vfmaq_laneq_f64(c2_1, a1, b1, 1);
    c3_1 = vfmaq_laneq_f64(c3_1, a1, b2, 0);
    c4_1 = vfmaq_laneq_f64(c4_1, a1, b2, 1);
  }

  vst1q_f64(c, vfmaq_n_f64(vld1q_f64(c), c1_1, alpha)); c += LDC;
  vst1q_f64(c, vfmaq_n_f64(vld1q_f64(c), c2_1, alpha)); c += LDC;
  vst1q_f64(c, vfmaq_n_f64(vld1q_f64(c), c3_1, alpha)); c += LDC;
  vst1q_f64(c, vfmaq_n_f64(vld1q_f64(c), c4_1, alpha));
}

static inline void dgemm_kernel_arm64_4x4_m2n2(
  const FLOAT *sa, const FLOAT *sb, FLOAT *c,
  BLASLONG K, BLASLONG LDC, FLOAT alpha) {

  float64x2_t c1_1, c1_2, c2_1, c2_2;
  c1_1 = c1_2 = c2_1 = c2_2 = vdupq_n_f64(0);

  for (; K > 1; K -= 2) {
    float64x2_t a1 = vld1q_f64(sa), a2 = vld1q_f64(sa + 2); sa += 4;
    float64x2_t b1 = vld1q_f64(sb), b2 = vld1q_f64(sb + 2); sb += 4;

    c1_1 = vfmaq_laneq_f64(c1_1, a1, b1, 0);
    c2_1 = vfmaq_laneq_f64(c2_1, a1, b1, 1);
    c1_2 = vfmaq_laneq_f64(c1_2, a2, b2, 0);
    c2_2 = vfmaq_laneq_f64(c2_2, a2, b2, 1);
  }
  c1_1 = vaddq_f64(c1_1, c1_2);
  c2_1 = vaddq_f64(c2_1, c2_2);
  if (K) {
    float64x2_t a1 = vld1q_f64(sa); sa += 2;
    float64x2_t b1 = vld1q_f64(sb); sb += 2;
    c1_1 = vfmaq_laneq_f64(c1_1, a1, b1, 0);
    c2_1 = vfmaq_laneq_f64(c2_1, a1, b1, 1);
  }

  vst1q_f64(c, vfmaq_n_f64(vld1q_f64(c), c1_1, alpha)); c += LDC;
  vst1q_f64(c, vfmaq_n_f64(vld1q_f64(c), c2_1, alpha));
}

static inline void dgemm_kernel_arm64_4x4_m2n1(
  const FLOAT *sa, const FLOAT *sb, FLOAT *c,
  BLASLONG K, BLASLONG LDC, FLOAT alpha) {

  float64x2_t c1, c2, c3, c4;
  c1 = c2 = c3 = c4 = vdupq_n_f64(0);

  for (; K > 3; K -= 4) {
    float64x2_t b12 = vld1q_f64(sb), b34 = vld1q_f64(sb + 2); sb += 4;
    c1 = vfmaq_laneq_f64(c1, vld1q_f64(sa), b12, 0);
    c2 = vfmaq_laneq_f64(c2, vld1q_f64(sa + 2), b12, 1);
    c3 = vfmaq_laneq_f64(c3, vld1q_f64(sa + 4), b34, 0);
    c4 = vfmaq_laneq_f64(c4, vld1q_f64(sa + 6), b34, 1);
    sa += 8;
  }
  c1 = vaddq_f64(c1, c2);
  c3 = vaddq_f64(c3, c4);
  c1 = vaddq_f64(c1, c3);
  for (; K; K--) {
    c1 = vfmaq_n_f64(c1, vld1q_f64(sa), *sb++);
    sa += 2;
  }

  vst1q_f64(c, vfmaq_n_f64(vld1q_f64(c), c1, alpha));
}

static inline void dgemm_store_m1n2(double *C, float64x2_t vc,
  double alpha, BLASLONG LDC) {
  double c0 = vgetq_lane_f64(vc, 0);
  double c1 = vgetq_lane_f64(vc, 1);
  C[0] += c0 * alpha;
  C[LDC] += c1 * alpha;
}

static inline void dgemm_kernel_arm64_4x4_m1n12(
  const FLOAT *sa, const FLOAT *sb, FLOAT *C,
  BLASLONG K, BLASLONG LDC, FLOAT alpha) {

  float64x2_t c1, c2, c3, c4, c5, c6;
  c1 = c2 = c3 = c4 = c5 = c6 = vdupq_n_f64(0);

  const double *b1_ = sb;
  const double *b2_ = sb + 4 * K;
  const double *b3_ = b2_ + 4 * K;

  for (; K; K--) {
    const double a1 = *sa++;
    c1 = vfmaq_n_f64(c1, vld1q_f64(b1_), a1);
    c2 = vfmaq_n_f64(c2, vld1q_f64(b1_ + 2), a1); b1_ += 4;
    c3 = vfmaq_n_f64(c3, vld1q_f64(b2_), a1);
    c4 = vfmaq_n_f64(c4, vld1q_f64(b2_ + 2), a1); b2_ += 4;
    c5 = vfmaq_n_f64(c5, vld1q_f64(b3_), a1);
    c6 = vfmaq_n_f64(c6, vld1q_f64(b3_ + 2), a1); b3_ += 4;
  }

  dgemm_store_m1n2(C, c1, alpha, LDC); C += LDC * 2;
  dgemm_store_m1n2(C, c2, alpha, LDC); C += LDC * 2;
  dgemm_store_m1n2(C, c3, alpha, LDC); C += LDC * 2;
  dgemm_store_m1n2(C, c4, alpha, LDC); C += LDC * 2;
  dgemm_store_m1n2(C, c5, alpha, LDC); C += LDC * 2;
  dgemm_store_m1n2(C, c6, alpha, LDC);
}

static inline void dgemm_kernel_arm64_4x4_m1n8(
  const FLOAT *sa, const FLOAT *sb, FLOAT *C,
  BLASLONG K, BLASLONG LDC, FLOAT alpha) {

  float64x2_t c1, c2, c3, c4;
  c1 = c2 = c3 = c4 = vdupq_n_f64(0);

  const double *b1_ = sb;
  const double *b2_ = sb + 4 * K;

  for (; K; K--) {
    const double a1 = *sa++;
    c1 = vfmaq_n_f64(c1, vld1q_f64(b1_), a1);
    c2 = vfmaq_n_f64(c2, vld1q_f64(b1_ + 2), a1); b1_ += 4;
    c3 = vfmaq_n_f64(c3, vld1q_f64(b2_), a1);
    c4 = vfmaq_n_f64(c4, vld1q_f64(b2_ + 2), a1); b2_ += 4;
  }

  dgemm_store_m1n2(C, c1, alpha, LDC); C += LDC * 2;
  dgemm_store_m1n2(C, c2, alpha, LDC); C += LDC * 2;
  dgemm_store_m1n2(C, c3, alpha, LDC); C += LDC * 2;
  dgemm_store_m1n2(C, c4, alpha, LDC);
}

static inline void dgemm_kernel_arm64_4x4_m1n4(
  const FLOAT *sa, const FLOAT *sb, FLOAT *C,
  BLASLONG K, BLASLONG LDC, FLOAT alpha) {

  float64x2_t c1_1, c1_2, c2_1, c2_2;
  c1_1 = c1_2 = c2_1 = c2_2 = vdupq_n_f64(0);

  for (; K > 1; K -= 2) {
    float64x2_t a1 = vld1q_f64(sa); sa += 2;
    c1_1 = vfmaq_laneq_f64(c1_1, vld1q_f64(sb), a1, 0);
    c2_1 = vfmaq_laneq_f64(c2_1, vld1q_f64(sb + 2), a1, 0);
    c1_2 = vfmaq_laneq_f64(c1_2, vld1q_f64(sb + 4), a1, 1);
    c2_2 = vfmaq_laneq_f64(c2_2, vld1q_f64(sb + 6), a1, 1); sb += 8;
  }
  c1_1 = vaddq_f64(c1_1, c1_2);
  c2_1 = vaddq_f64(c2_1, c2_2);
  if (K) {
    double a1 = *sa++;
    c1_1 = vfmaq_n_f64(c1_1, vld1q_f64(sb), a1);
    c2_1 = vfmaq_n_f64(c2_1, vld1q_f64(sb + 2), a1);
    sb += 4;
  }

  dgemm_store_m1n2(C, c1_1, alpha, LDC); C += LDC * 2;
  dgemm_store_m1n2(C, c2_1, alpha, LDC);
}

static inline void dgemm_kernel_arm64_4x4_m1n2(
  const FLOAT *sa, const FLOAT *sb, FLOAT *C,
  BLASLONG K, BLASLONG LDC, FLOAT alpha) {

  float64x2_t c1, c2, c3, c4;
  c1 = c2 = c3 = c4 = vdupq_n_f64(0);

  for (; K > 3; K -= 4) {
    float64x2_t a12 = vld1q_f64(sa), a34 = vld1q_f64(sa + 2); sa += 4;
    c1 = vfmaq_laneq_f64(c1, vld1q_f64(sb), a12, 0);
    c2 = vfmaq_laneq_f64(c2, vld1q_f64(sb + 2), a12, 1);
    c3 = vfmaq_laneq_f64(c3, vld1q_f64(sb + 4), a34, 0);
    c4 = vfmaq_laneq_f64(c4, vld1q_f64(sb + 6), a34, 1); sb += 8;
  }
  c1 = vaddq_f64(c1, c2);
  c3 = vaddq_f64(c3, c4);
  c1 = vaddq_f64(c1, c3);
  for (; K; K--) {
    c1 = vfmaq_n_f64(c1, vld1q_f64(sb), *sa++);
    sb += 2;
  }

  dgemm_store_m1n2(C, c1, alpha, LDC);
}

static inline void dgemm_kernel_arm64_4x4_m1n1(
  const FLOAT *sa, const FLOAT *sb, FLOAT *C,
  BLASLONG K, BLASLONG LDC, FLOAT alpha) {

  float64x2_t c1, c2, c3, c4;
  c1 = c2 = c3 = c4 = vdupq_n_f64(0);

  for (; K > 7; K -= 8) {
    c1 = vfmaq_f64(c1, vld1q_f64(sb), vld1q_f64(sa));
    c2 = vfmaq_f64(c2, vld1q_f64(sb + 2), vld1q_f64(sa + 2));
    c3 = vfmaq_f64(c3, vld1q_f64(sb + 4), vld1q_f64(sa + 4));
    c4 = vfmaq_f64(c4, vld1q_f64(sb + 6), vld1q_f64(sa + 6));
    sa += 8; sb += 8;
  }
  c1 = vaddq_f64(c1, c2);
  c3 = vaddq_f64(c3, c4);
  c1 = vaddq_f64(c1, c3);
  double cs1 = vpaddd_f64(c1);
  for (; K; K--) {
    cs1 += (*sa++) * (*sb++);
  }

  C[0] += cs1 * alpha;
}

int CNAME(BLASLONG M, BLASLONG N, BLASLONG K, FLOAT alpha,
  FLOAT *sa, FLOAT *sb, FLOAT *C, BLASLONG LDC) {

  for (; N >= 12; N -= 12) {
    BLASLONG m_left = M;
    const FLOAT *a_ = sa;
    FLOAT *c_ = C;
    for (; m_left >= 4; m_left -= 4) {
      dgemm_kernel_arm_cortex_a53_4x4_m4n12(a_, sb, c_, K, LDC, alpha);
      c_ += 4;
      a_ += 4 * K;
    }
    if (m_left >= 2) {
      m_left -= 2;
      dgemm_kernel_arm64_4x4_m2n12(a_, sb, c_, K, LDC, alpha);
      c_ += 2;
      a_ += 2 * K;
    }
    if (m_left) {
      dgemm_kernel_arm64_4x4_m1n12(a_, sb, c_, K, LDC, alpha);
    }
    sb += 12 * K;
    C += 12 * LDC;
  }

  if (N >= 8) {
    N -= 8;
    BLASLONG m_left = M;
    const FLOAT *a_ = sa;
    FLOAT *c_ = C;
    for (; m_left >= 4; m_left -= 4) {
      dgemm_kernel_arm64_4x4_m4n8(a_, sb, c_, K, LDC, alpha);
      c_ += 4;
      a_ += 4 * K;
    }
    if (m_left >= 2) {
      m_left -= 2;
      dgemm_kernel_arm64_4x4_m2n8(a_, sb, c_, K, LDC, alpha);
      c_ += 2;
      a_ += 2 * K;
    }
    if (m_left) {
      dgemm_kernel_arm64_4x4_m1n8(a_, sb, c_, K, LDC, alpha);
    }
    sb += 8 * K;
    C += 8 * LDC;
  } else if (N >= 4) {
    N -= 4;
    BLASLONG m_left = M;
    const FLOAT *a_ = sa;
    FLOAT *c_ = C;
    for (; m_left >= 4; m_left -= 4) {
      dgemm_kernel_arm64_4x4_m4n4(a_, sb, c_, K, LDC, alpha);
      c_ += 4;
      a_ += 4 * K;
    }
    if (m_left >= 2) {
      m_left -= 2;
      dgemm_kernel_arm64_4x4_m2n4(a_, sb, c_, K, LDC, alpha);
      c_ += 2;
      a_ += 2 * K;
    }
    if (m_left) {
      dgemm_kernel_arm64_4x4_m1n4(a_, sb, c_, K, LDC, alpha);
    }
    sb += 4 * K;
    C += 4 * LDC;
  }

  if (N >= 2) {
    N -= 2;
    BLASLONG m_left = M;
    const FLOAT *a_ = sa;
    FLOAT *c_ = C;
    for (; m_left >= 4; m_left -= 4) {
      dgemm_kernel_arm64_4x4_m4n2(a_, sb, c_, K, LDC, alpha);
      c_ += 4;
      a_ += 4 * K;
    }
    if (m_left >= 2) {
      m_left -= 2;
      dgemm_kernel_arm64_4x4_m2n2(a_, sb, c_, K, LDC, alpha);
      c_ += 2;
      a_ += 2 * K;
    }
    if (m_left) {
      dgemm_kernel_arm64_4x4_m1n2(a_, sb, c_, K, LDC, alpha);
    }
    sb += 2 * K;
    C += 2 * LDC;
  }

  if (N) {
    BLASLONG m_left = M;
    const FLOAT *a_ = sa;
    FLOAT *c_ = C;
    for (; m_left >= 4; m_left -= 4) {
      dgemm_kernel_arm64_4x4_m4n1(a_, sb, c_, K, LDC, alpha);
      c_ += 4;
      a_ += 4 * K;
    }
    if (m_left >= 2) {
      m_left -= 2;
      dgemm_kernel_arm64_4x4_m2n1(a_, sb, c_, K, LDC, alpha);
      c_ += 2;
      a_ += 2 * K;
    }
    if (m_left) {
      dgemm_kernel_arm64_4x4_m1n1(a_, sb, c_, K, LDC, alpha);
    }
  }
  return 0;
}

