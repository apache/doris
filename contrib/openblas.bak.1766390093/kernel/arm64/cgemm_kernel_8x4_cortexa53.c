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
#include <arm_neon.h>

#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define FMLA_RI "fmla "
#define FMLA_IR "fmla "
#define FMLA_II "fmls "
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define FMLA_RI "fmls "
#define FMLA_IR "fmla "
#define FMLA_II "fmla "
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define FMLA_RI "fmla "
#define FMLA_IR "fmls "
#define FMLA_II "fmla "
#else
#define FMLA_RI "fmls "
#define FMLA_IR "fmls "
#define FMLA_II "fmls "
#endif
#define FMLA_RR "fmla "

static inline void store_m8n1_contracted(float *C,
  float32x4_t c1r, float32x4_t c1i, float32x4_t c2r, float32x4_t c2i,
  float alphar, float alphai) {

  float32x4x2_t ld1 = vld2q_f32(C), ld2 = vld2q_f32(C + 8);
  ld1.val[0] = vfmaq_n_f32(ld1.val[0], c1r, alphar);
  ld2.val[0] = vfmaq_n_f32(ld2.val[0], c2r, alphar);
  ld1.val[1] = vfmaq_n_f32(ld1.val[1], c1r, alphai);
  ld2.val[1] = vfmaq_n_f32(ld2.val[1], c2r, alphai);
  ld1.val[0] = vfmsq_n_f32(ld1.val[0], c1i, alphai);
  ld2.val[0] = vfmsq_n_f32(ld2.val[0], c2i, alphai);
  ld1.val[1] = vfmaq_n_f32(ld1.val[1], c1i, alphar);
  ld2.val[1] = vfmaq_n_f32(ld2.val[1], c2i, alphar);
  vst2q_f32(C, ld1);
  vst2q_f32(C + 8, ld2);
}

static inline void kernel_8x4(const float *sa, const float *sb, float *C,
  float alphar, float alphai, BLASLONG K, BLASLONG LDC) {

  const float *c_pref = C;
  float32x4_t c1r, c1i, c2r, c2i, c3r, c3i, c4r, c4i;
  float32x4_t c5r, c5i, c6r, c6i, c7r, c7i, c8r, c8i;

  /** x0 for filling A, x1-x6 for filling B (x5 and x6 for real, x2 and x4 for imag) */
  /** v0-v1 and v10-v11 for B, v2-v9 for A */
  __asm__ __volatile__(
    "cmp %[K],#0; mov %[c_pref],%[C]\n\t"
    "movi %[c1r].16b,#0; prfm pstl1keep,[%[c_pref]]\n\t"
    "movi %[c1i].16b,#0; prfm pstl1keep,[%[c_pref],#64]\n\t"
    "movi %[c2r].16b,#0; add %[c_pref],%[c_pref],%[LDC],LSL#3\n\t"
    "movi %[c2i].16b,#0; prfm pstl1keep,[%[c_pref]]\n\t"
    "movi %[c3r].16b,#0; prfm pstl1keep,[%[c_pref],#64]\n\t"
    "movi %[c3i].16b,#0; add %[c_pref],%[c_pref],%[LDC],LSL#3\n\t"
    "movi %[c4r].16b,#0; prfm pstl1keep,[%[c_pref]]\n\t"
    "movi %[c4i].16b,#0; prfm pstl1keep,[%[c_pref],#64]\n\t"
    "movi %[c5r].16b,#0; add %[c_pref],%[c_pref],%[LDC],LSL#3\n\t"
    "movi %[c5i].16b,#0; prfm pstl1keep,[%[c_pref]]\n\t"
    "movi %[c6r].16b,#0; prfm pstl1keep,[%[c_pref],#64]\n\t"
    "movi %[c6i].16b,#0\n\t"
    "movi %[c7r].16b,#0; movi %[c7i].16b,#0\n\t"
    "movi %[c8r].16b,#0; movi %[c8i].16b,#0\n\t"
    "beq 4f\n\t"
    "cmp %[K],#2\n\t"
    "ldp x1,x2,[%[sb]],#16; ldr q2,[%[sa]],#64\n\t"
    "ldp x3,x4,[%[sb]],#16; ldr d3,[%[sa],#-48]\n\t"
    "mov w5,w1; mov w6,w3; ldr x0,[%[sa],#-40]\n\t"
    "bfi x5,x2,#32,#32; bfi x6,x4,#32,#32; fmov d0,x5\n\t"
    "bfxil x2,x1,#32,#32; bfxil x4,x3,#32,#32; fmov v0.d[1],x6\n\t"
    
    "blt 3f; beq 2f\n\t"
    "1:\n\t"
    "fmov v3.d[1],x0; ldr d4,[%[sa],#-32]\n\t"
    FMLA_RR "%[c1r].4s,v0.4s,v2.s[0]; ldr x0,[%[sa],#-24]\n\t"
    FMLA_IR "%[c1i].4s,v0.4s,v2.s[1]; ldr x1,[%[sb]],#64\n\t"
    FMLA_RR "%[c2r].4s,v0.4s,v2.s[2]\n\t"
    "fmov v4.d[1],x0; ldr d5,[%[sa],#-16]\n\t"
    FMLA_IR "%[c2i].4s,v0.4s,v2.s[3]; ldr x0,[%[sa],#-8]\n\t"
    FMLA_RR "%[c3r].4s,v0.4s,v3.s[0]; mov w5,w1\n\t"
    FMLA_IR "%[c3i].4s,v0.4s,v3.s[1]\n\t"
    "fmov v5.d[1],x0; fmov d1,x2\n\t"
    FMLA_RR "%[c4r].4s,v0.4s,v3.s[2]; ldr x2,[%[sb],#-56]\n\t"
    FMLA_IR "%[c4i].4s,v0.4s,v3.s[3]; ldr x3,[%[sb],#-48]\n\t"
    FMLA_RR "%[c5r].4s,v0.4s,v4.s[0]\n\t"
    "fmov v1.d[1],x4; ldr d6,[%[sa]]\n\t"
    FMLA_IR "%[c5i].4s,v0.4s,v4.s[1]; ldr x0,[%[sa],#8]\n\t"
    FMLA_RR "%[c6r].4s,v0.4s,v4.s[2]; ldr x4,[%[sb],#-40]\n\t"
    FMLA_IR "%[c6i].4s,v0.4s,v4.s[3]; bfi x5,x2,#32,#32\n\t" 
    "fmov v6.d[1],x0; ldr d7,[%[sa],#16]\n\t"
    FMLA_RR "%[c7r].4s,v0.4s,v5.s[0]; ldr x0,[%[sa],#24]\n\t"
    FMLA_IR "%[c7i].4s,v0.4s,v5.s[1]; mov w6,w3\n\t"
    FMLA_RR "%[c8r].4s,v0.4s,v5.s[2]; bfxil x2,x1,#32,#32\n\t"
    "fmov v7.d[1],x0; fmov d10,x5\n\t"
    FMLA_IR "%[c8i].4s,v0.4s,v5.s[3]; bfi x6,x4,#32,#32\n\t"
    FMLA_II "%[c1r].4s,v1.4s,v2.s[1]; ldr x1,[%[sb],#-32]\n\t"
    FMLA_RI "%[c1i].4s,v1.4s,v2.s[0]; bfxil x4,x3,#32,#32\n\t" 
    "fmov v10.d[1],x6; fmov d11,x2\n\t"
    FMLA_II "%[c2r].4s,v1.4s,v2.s[3]; ldr x2,[%[sb],#-24]\n\t"
    FMLA_RI "%[c2i].4s,v1.4s,v2.s[2]; ldr x3,[%[sb],#-16]\n\t"
    FMLA_II "%[c3r].4s,v1.4s,v3.s[1]; mov w5,w1\n\t"
    "fmov v11.d[1],x4; ldr d8,[%[sa],#32]\n\t"
    FMLA_RI "%[c3i].4s,v1.4s,v3.s[0]; ldr x0,[%[sa],#40]\n\t"
    FMLA_II "%[c4r].4s,v1.4s,v3.s[3]; ldr x4,[%[sb],#-8]\n\t"
    FMLA_RI "%[c4i].4s,v1.4s,v3.s[2]; bfi x5,x2,#32,#32\n\t" 
    "fmov v8.d[1],x0; ldr d9,[%[sa],#48]\n\t"
    FMLA_II "%[c5r].4s,v1.4s,v4.s[1]; ldr x0,[%[sa],#56]\n\t"
    FMLA_RI "%[c5i].4s,v1.4s,v4.s[0]; mov w6,w3\n\t"
    FMLA_II "%[c6r].4s,v1.4s,v4.s[3]\n\t"
    "fmov v9.d[1],x0; fmov d0,x5\n\t"
    FMLA_RI "%[c6i].4s,v1.4s,v4.s[2]; bfi x6,x4,#32,#32\n\t" 
    FMLA_II "%[c7r].4s,v1.4s,v5.s[1]\n\t"
    FMLA_RI "%[c7i].4s,v1.4s,v5.s[0]\n\t"
    "fmov v0.d[1],x6; ldr d2,[%[sa],#64]\n\t"
    FMLA_II "%[c8r].4s,v1.4s,v5.s[3]; ldr x0,[%[sa],#72]\n\t"
    FMLA_RI "%[c8i].4s,v1.4s,v5.s[2]\n\t"
    FMLA_RR "%[c1r].4s,v10.4s,v6.s[0]\n\t"
    "fmov v2.d[1],x0; ldr d3,[%[sa],#80]\n\t" 
    FMLA_IR "%[c1i].4s,v10.4s,v6.s[1]\n\t"
    FMLA_RR "%[c2r].4s,v10.4s,v6.s[2]; ldr x0,[%[sa],#88]\n\t"
    FMLA_IR "%[c2i].4s,v10.4s,v6.s[3]; bfxil x2,x1,#32,#32\n\t"
    FMLA_RR "%[c3r].4s,v10.4s,v7.s[0]; bfxil x4,x3,#32,#32\n\t"
    FMLA_IR "%[c3i].4s,v10.4s,v7.s[1]; add %[sa],%[sa],#128\n\t"
    FMLA_RR "%[c4r].4s,v10.4s,v7.s[2]; prfm pldl1keep,[%[sb],#128]\n\t"
    FMLA_IR "%[c4i].4s,v10.4s,v7.s[3]; sub %[K],%[K],#2\n\t"
    FMLA_RR "%[c5r].4s,v10.4s,v8.s[0]; prfm pldl1keep,[%[sa],#128]\n\t"
    FMLA_IR "%[c5i].4s,v10.4s,v8.s[1]; prfm pldl1keep,[%[sa],#192]\n\t"
    FMLA_RR "%[c6r].4s,v10.4s,v8.s[2]; cmp %[K],#2\n\t"
    FMLA_IR "%[c6i].4s,v10.4s,v8.s[3]\n\t"
    FMLA_RR "%[c7r].4s,v10.4s,v9.s[0]\n\t" FMLA_IR "%[c7i].4s,v10.4s,v9.s[1]\n\t"
    FMLA_RR "%[c8r].4s,v10.4s,v9.s[2]\n\t" FMLA_IR "%[c8i].4s,v10.4s,v9.s[3]\n\t"
    FMLA_II "%[c1r].4s,v11.4s,v6.s[1]\n\t" FMLA_RI "%[c1i].4s,v11.4s,v6.s[0]\n\t"
    FMLA_II "%[c2r].4s,v11.4s,v6.s[3]\n\t" FMLA_RI "%[c2i].4s,v11.4s,v6.s[2]\n\t"
    FMLA_II "%[c3r].4s,v11.4s,v7.s[1]\n\t" FMLA_RI "%[c3i].4s,v11.4s,v7.s[0]\n\t"
    FMLA_II "%[c4r].4s,v11.4s,v7.s[3]\n\t" FMLA_RI "%[c4i].4s,v11.4s,v7.s[2]\n\t"
    FMLA_II "%[c5r].4s,v11.4s,v8.s[1]\n\t" FMLA_RI "%[c5i].4s,v11.4s,v8.s[0]\n\t"
    FMLA_II "%[c6r].4s,v11.4s,v8.s[3]\n\t" FMLA_RI "%[c6i].4s,v11.4s,v8.s[2]\n\t"
    FMLA_II "%[c7r].4s,v11.4s,v9.s[1]\n\t" FMLA_RI "%[c7i].4s,v11.4s,v9.s[0]\n\t"
    FMLA_II "%[c8r].4s,v11.4s,v9.s[3]\n\t" FMLA_RI "%[c8i].4s,v11.4s,v9.s[2]\n\t"
    "bgt 1b; blt 3f\n\t"
    "2:\n\t"
    "fmov v3.d[1],x0; ldr d4,[%[sa],#-32]\n\t"
    FMLA_RR "%[c1r].4s,v0.4s,v2.s[0]; ldr x0,[%[sa],#-24]\n\t"
    FMLA_IR "%[c1i].4s,v0.4s,v2.s[1]; ldr x1,[%[sb]],#32\n\t"
    FMLA_RR "%[c2r].4s,v0.4s,v2.s[2]\n\t"
    "fmov v4.d[1],x0; ldr d5,[%[sa],#-16]\n\t"
    FMLA_IR "%[c2i].4s,v0.4s,v2.s[3]; ldr x0,[%[sa],#-8]\n\t"
    FMLA_RR "%[c3r].4s,v0.4s,v3.s[0]; mov w5,w1\n\t"
    FMLA_IR "%[c3i].4s,v0.4s,v3.s[1]\n\t"
    "fmov v5.d[1],x0; fmov d1,x2\n\t"
    FMLA_RR "%[c4r].4s,v0.4s,v3.s[2]; ldr x2,[%[sb],#-24]\n\t"
    FMLA_IR "%[c4i].4s,v0.4s,v3.s[3]; ldr x3,[%[sb],#-16]\n\t"
    FMLA_RR "%[c5r].4s,v0.4s,v4.s[0]\n\t"
    "fmov v1.d[1],x4; ldr d6,[%[sa]]\n\t"
    FMLA_IR "%[c5i].4s,v0.4s,v4.s[1]; ldr x0,[%[sa],#8]\n\t"
    FMLA_RR "%[c6r].4s,v0.4s,v4.s[2]; ldr x4,[%[sb],#-8]\n\t"
    FMLA_IR "%[c6i].4s,v0.4s,v4.s[3]; bfi x5,x2,#32,#32\n\t" 
    "fmov v6.d[1],x0; ldr d7,[%[sa],#16]\n\t"
    FMLA_RR "%[c7r].4s,v0.4s,v5.s[0]; ldr x0,[%[sa],#24]\n\t"
    FMLA_IR "%[c7i].4s,v0.4s,v5.s[1]; mov w6,w3\n\t"
    FMLA_RR "%[c8r].4s,v0.4s,v5.s[2]; bfxil x2,x1,#32,#32\n\t"
    "fmov v7.d[1],x0; fmov d10,x5\n\t"
    FMLA_IR "%[c8i].4s,v0.4s,v5.s[3]; bfi x6,x4,#32,#32\n\t"
    FMLA_II "%[c1r].4s,v1.4s,v2.s[1]\n\t"
    FMLA_RI "%[c1i].4s,v1.4s,v2.s[0]; bfxil x4,x3,#32,#32\n\t" 
    "fmov v10.d[1],x6; fmov d11,x2\n\t"
    FMLA_II "%[c2r].4s,v1.4s,v2.s[3]\n\t"
    FMLA_RI "%[c2i].4s,v1.4s,v2.s[2]\n\t"
    FMLA_II "%[c3r].4s,v1.4s,v3.s[1]\n\t"
    "fmov v11.d[1],x4; ldr d8,[%[sa],#32]\n\t"
    FMLA_RI "%[c3i].4s,v1.4s,v3.s[0]; ldr x0,[%[sa],#40]\n\t"
    FMLA_II "%[c4r].4s,v1.4s,v3.s[3]; sub %[K],%[K],#2\n\t"
    FMLA_RI "%[c4i].4s,v1.4s,v3.s[2]\n\t"
    "fmov v8.d[1],x0; ldr d9,[%[sa],#48]\n\t"
    FMLA_II "%[c5r].4s,v1.4s,v4.s[1]; ldr x0,[%[sa],#56]\n\t"
    FMLA_RI "%[c5i].4s,v1.4s,v4.s[0]; add %[sa],%[sa],#64\n\t"
    FMLA_II "%[c6r].4s,v1.4s,v4.s[3]\n\t"
    "fmov v9.d[1],x0\n\t"
    FMLA_RI "%[c6i].4s,v1.4s,v4.s[2]\n\t"
    FMLA_II "%[c7r].4s,v1.4s,v5.s[1]\n\t" FMLA_RI "%[c7i].4s,v1.4s,v5.s[0]\n\t"
    FMLA_II "%[c8r].4s,v1.4s,v5.s[3]\n\t" FMLA_RI "%[c8i].4s,v1.4s,v5.s[2]\n\t"
    FMLA_RR "%[c1r].4s,v10.4s,v6.s[0]\n\t" FMLA_IR "%[c1i].4s,v10.4s,v6.s[1]\n\t"
    FMLA_RR "%[c2r].4s,v10.4s,v6.s[2]\n\t" FMLA_IR "%[c2i].4s,v10.4s,v6.s[3]\n\t"
    FMLA_RR "%[c3r].4s,v10.4s,v7.s[0]\n\t" FMLA_IR "%[c3i].4s,v10.4s,v7.s[1]\n\t"
    FMLA_RR "%[c4r].4s,v10.4s,v7.s[2]\n\t" FMLA_IR "%[c4i].4s,v10.4s,v7.s[3]\n\t"
    FMLA_RR "%[c5r].4s,v10.4s,v8.s[0]\n\t" FMLA_IR "%[c5i].4s,v10.4s,v8.s[1]\n\t"
    FMLA_RR "%[c6r].4s,v10.4s,v8.s[2]\n\t" FMLA_IR "%[c6i].4s,v10.4s,v8.s[3]\n\t"
    FMLA_RR "%[c7r].4s,v10.4s,v9.s[0]\n\t" FMLA_IR "%[c7i].4s,v10.4s,v9.s[1]\n\t"
    FMLA_RR "%[c8r].4s,v10.4s,v9.s[2]\n\t" FMLA_IR "%[c8i].4s,v10.4s,v9.s[3]\n\t"
    FMLA_II "%[c1r].4s,v11.4s,v6.s[1]\n\t" FMLA_RI "%[c1i].4s,v11.4s,v6.s[0]\n\t"
    FMLA_II "%[c2r].4s,v11.4s,v6.s[3]\n\t" FMLA_RI "%[c2i].4s,v11.4s,v6.s[2]\n\t"
    FMLA_II "%[c3r].4s,v11.4s,v7.s[1]\n\t" FMLA_RI "%[c3i].4s,v11.4s,v7.s[0]\n\t"
    FMLA_II "%[c4r].4s,v11.4s,v7.s[3]\n\t" FMLA_RI "%[c4i].4s,v11.4s,v7.s[2]\n\t"
    FMLA_II "%[c5r].4s,v11.4s,v8.s[1]\n\t" FMLA_RI "%[c5i].4s,v11.4s,v8.s[0]\n\t"
    FMLA_II "%[c6r].4s,v11.4s,v8.s[3]\n\t" FMLA_RI "%[c6i].4s,v11.4s,v8.s[2]\n\t"
    FMLA_II "%[c7r].4s,v11.4s,v9.s[1]\n\t" FMLA_RI "%[c7i].4s,v11.4s,v9.s[0]\n\t"
    FMLA_II "%[c8r].4s,v11.4s,v9.s[3]\n\t" FMLA_RI "%[c8i].4s,v11.4s,v9.s[2]\n\t"
    "b 4f\n\t"
    "3:\n\t"
    "fmov v3.d[1],x0; ldr d4,[%[sa],#-32]\n\t"
    FMLA_RR "%[c1r].4s,v0.4s,v2.s[0]; ldr x0,[%[sa],#-24]\n\t"
    FMLA_IR "%[c1i].4s,v0.4s,v2.s[1]\n\t"
    FMLA_RR "%[c2r].4s,v0.4s,v2.s[2]\n\t"
    "fmov v4.d[1],x0; ldr d5,[%[sa],#-16]\n\t"
    FMLA_IR "%[c2i].4s,v0.4s,v2.s[3]; ldr x0,[%[sa],#-8]\n\t"
    FMLA_RR "%[c3r].4s,v0.4s,v3.s[0]\n\t"
    FMLA_IR "%[c3i].4s,v0.4s,v3.s[1]\n\t"
    "fmov v5.d[1],x0; fmov d1,x2\n\t"
    FMLA_RR "%[c4r].4s,v0.4s,v3.s[2]\n\t"
    FMLA_IR "%[c4i].4s,v0.4s,v3.s[3]\n\t"
    FMLA_RR "%[c5r].4s,v0.4s,v4.s[0]\n\t"
    "fmov v1.d[1],x4\n\t"
    FMLA_IR "%[c5i].4s,v0.4s,v4.s[1]; sub %[K],%[K],#1\n\t"
    FMLA_RR "%[c6r].4s,v0.4s,v4.s[2]\n\t" FMLA_IR "%[c6i].4s,v0.4s,v4.s[3]\n\t"
    FMLA_RR "%[c7r].4s,v0.4s,v5.s[0]\n\t" FMLA_IR "%[c7i].4s,v0.4s,v5.s[1]\n\t"
    FMLA_RR "%[c8r].4s,v0.4s,v5.s[2]\n\t" FMLA_IR "%[c8i].4s,v0.4s,v5.s[3]\n\t"
    FMLA_II "%[c1r].4s,v1.4s,v2.s[1]\n\t" FMLA_RI "%[c1i].4s,v1.4s,v2.s[0]\n\t"
    FMLA_II "%[c2r].4s,v1.4s,v2.s[3]\n\t" FMLA_RI "%[c2i].4s,v1.4s,v2.s[2]\n\t"
    FMLA_II "%[c3r].4s,v1.4s,v3.s[1]\n\t" FMLA_RI "%[c3i].4s,v1.4s,v3.s[0]\n\t"
    FMLA_II "%[c4r].4s,v1.4s,v3.s[3]\n\t" FMLA_RI "%[c4i].4s,v1.4s,v3.s[2]\n\t"
    FMLA_II "%[c5r].4s,v1.4s,v4.s[1]\n\t" FMLA_RI "%[c5i].4s,v1.4s,v4.s[0]\n\t"
    FMLA_II "%[c6r].4s,v1.4s,v4.s[3]\n\t" FMLA_RI "%[c6i].4s,v1.4s,v4.s[2]\n\t"
    FMLA_II "%[c7r].4s,v1.4s,v5.s[1]\n\t" FMLA_RI "%[c7i].4s,v1.4s,v5.s[0]\n\t"
    FMLA_II "%[c8r].4s,v1.4s,v5.s[3]\n\t" FMLA_RI "%[c8i].4s,v1.4s,v5.s[2]\n\t"
    "4:\n\t"
    "mov %[c_pref],%[C]\n\t"
    "zip1 v0.4s,%[c1r].4s,%[c2r].4s; prfm pstl1keep,[%[c_pref]]\n\t"
    "zip1 v4.4s,%[c1i].4s,%[c2i].4s; prfm pstl1keep,[%[c_pref],#64]\n\t"
    "zip1 v1.4s,%[c3r].4s,%[c4r].4s; add %[c_pref],%[c_pref],%[LDC],LSL#3\n\t"
    "zip1 v5.4s,%[c3i].4s,%[c4i].4s; prfm pstl1keep,[%[c_pref]]\n\t"
    "zip2 v2.4s,%[c1r].4s,%[c2r].4s; prfm pstl1keep,[%[c_pref],#64]\n\t"
    "zip2 v6.4s,%[c1i].4s,%[c2i].4s; add %[c_pref],%[c_pref],%[LDC],LSL#3\n\t"
    "zip2 v3.4s,%[c3r].4s,%[c4r].4s; prfm pstl1keep,[%[c_pref]]\n\t"
    "zip2 v7.4s,%[c3i].4s,%[c4i].4s; prfm pstl1keep,[%[c_pref],#64]\n\t"
    "zip1 %[c1r].2d,v0.2d,v1.2d; add %[c_pref],%[c_pref],%[LDC],LSL#3\n\t"
    "zip1 %[c1i].2d,v4.2d,v5.2d; prfm pstl1keep,[%[c_pref]]\n\t"
    "zip2 %[c2r].2d,v0.2d,v1.2d; prfm pstl1keep,[%[c_pref],#64]\n\t"
    "zip2 %[c2i].2d,v4.2d,v5.2d\n\t"
    "zip1 %[c3r].2d,v2.2d,v3.2d; zip1 %[c3i].2d,v6.2d,v7.2d\n\t"
    "zip2 %[c4r].2d,v2.2d,v3.2d; zip2 %[c4i].2d,v6.2d,v7.2d\n\t"
    "zip1 v0.4s,%[c5r].4s,%[c6r].4s; zip1 v4.4s,%[c5i].4s,%[c6i].4s\n\t"
    "zip1 v1.4s,%[c7r].4s,%[c8r].4s; zip1 v5.4s,%[c7i].4s,%[c8i].4s\n\t"
    "zip2 v2.4s,%[c5r].4s,%[c6r].4s; zip2 v6.4s,%[c5i].4s,%[c6i].4s\n\t"
    "zip2 v3.4s,%[c7r].4s,%[c8r].4s; zip2 v7.4s,%[c7i].4s,%[c8i].4s\n\t"
    "zip1 %[c5r].2d,v0.2d,v1.2d; zip1 %[c5i].2d,v4.2d,v5.2d\n\t"
    "zip2 %[c6r].2d,v0.2d,v1.2d; zip2 %[c6i].2d,v4.2d,v5.2d\n\t"
    "zip1 %[c7r].2d,v2.2d,v3.2d; zip1 %[c7i].2d,v6.2d,v7.2d\n\t"
    "zip2 %[c8r].2d,v2.2d,v3.2d; zip2 %[c8i].2d,v6.2d,v7.2d\n\t"
   :[c1r]"=w"(c1r), [c1i]"=w"(c1i), [c2r]"=w"(c2r), [c2i]"=w"(c2i),
    [c3r]"=w"(c3r), [c3i]"=w"(c3i), [c4r]"=w"(c4r), [c4i]"=w"(c4i),
    [c5r]"=w"(c5r), [c5i]"=w"(c5i), [c6r]"=w"(c6r), [c6i]"=w"(c6i),
    [c7r]"=w"(c7r), [c7i]"=w"(c7i), [c8r]"=w"(c8r), [c8i]"=w"(c8i),
    [K]"+r"(K), [sa]"+r"(sa), [sb]"+r"(sb), [c_pref]"+r"(c_pref)
   :[C]"r"(C), [LDC]"r"(LDC)
   :"cc","memory","x0","x1","x2","x3","x4","x5","x6",
    "v0","v1","v2","v3","v4","v5","v6","v7","v8","v9","v10","v11");

  store_m8n1_contracted(C, c1r, c1i, c5r, c5i, alphar, alphai); C += LDC * 2;
  store_m8n1_contracted(C, c2r, c2i, c6r, c6i, alphar, alphai); C += LDC * 2;
  store_m8n1_contracted(C, c3r, c3i, c7r, c7i, alphar, alphai); C += LDC * 2;
  store_m8n1_contracted(C, c4r, c4i, c8r, c8i, alphar, alphai);
}

static inline float32x4x4_t acc_expanded_m2n2(float32x4x4_t acc,
  float32x4_t a, float32x4_t b) {

  acc.val[0] = vfmaq_laneq_f32(acc.val[0], a, b, 0);
  acc.val[1] = vfmaq_laneq_f32(acc.val[1], a, b, 1);
  acc.val[2] = vfmaq_laneq_f32(acc.val[2], a, b, 2);
  acc.val[3] = vfmaq_laneq_f32(acc.val[3], a, b, 3);
  return acc;
}

static inline float32x4x4_t expand_alpha(float alphar, float alphai) {
  float32x4x4_t ret;
  const float maskp[] = { -1, 1, -1, 1 };
  const float maskn[] = { 1, -1, 1, -1 };
  const float32x4_t vrevp = vld1q_f32(maskp);
  const float32x4_t vrevn = vld1q_f32(maskn);
#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
  ret.val[0] = vdupq_n_f32(alphar);
  ret.val[1] = vdupq_n_f32(-alphai);
  ret.val[2] = vmulq_f32(ret.val[1], vrevn);
  ret.val[3] = vmulq_f32(ret.val[0], vrevp);
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
  ret.val[0] = vdupq_n_f32(alphar);
  ret.val[1] = vdupq_n_f32(alphai);
  ret.val[2] = vmulq_f32(ret.val[1], vrevp);
  ret.val[3] = vmulq_f32(ret.val[0], vrevn);
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
  ret.val[2] = vdupq_n_f32(alphai);
  ret.val[3] = vdupq_n_f32(alphar);
  ret.val[0] = vmulq_f32(ret.val[3], vrevn);
  ret.val[1] = vmulq_f32(ret.val[2], vrevp);
#else
  ret.val[2] = vdupq_n_f32(alphai);
  ret.val[3] = vdupq_n_f32(-alphar);
  ret.val[0] = vmulq_f32(ret.val[3], vrevp);
  ret.val[1] = vmulq_f32(ret.val[2], vrevn);
#endif
  return ret;
}

static inline void store_expanded_m2n2(float *C, BLASLONG LDC,
  float32x4x4_t acc, float32x4x4_t expanded_alpha) {

  float32x4_t ld1 = vld1q_f32(C), ld2 = vld1q_f32(C + LDC * 2);
  ld1 = vfmaq_f32(ld1, acc.val[0], expanded_alpha.val[0]);
  ld2 = vfmaq_f32(ld2, acc.val[2], expanded_alpha.val[0]);
  acc.val[0] = vrev64q_f32(acc.val[0]);
  acc.val[2] = vrev64q_f32(acc.val[2]);
  ld1 = vfmaq_f32(ld1, acc.val[1], expanded_alpha.val[1]);
  ld2 = vfmaq_f32(ld2, acc.val[3], expanded_alpha.val[1]);
  acc.val[1] = vrev64q_f32(acc.val[1]);
  acc.val[3] = vrev64q_f32(acc.val[3]);
  ld1 = vfmaq_f32(ld1, acc.val[0], expanded_alpha.val[2]);
  ld2 = vfmaq_f32(ld2, acc.val[2], expanded_alpha.val[2]);
  ld1 = vfmaq_f32(ld1, acc.val[1], expanded_alpha.val[3]);
  ld2 = vfmaq_f32(ld2, acc.val[3], expanded_alpha.val[3]);
  vst1q_f32(C, ld1);
  vst1q_f32(C + LDC * 2, ld2);
}

static inline float32x4x4_t init_expanded_m2n2() {
  float32x4x4_t ret = {{ vdupq_n_f32(0), vdupq_n_f32(0),
    vdupq_n_f32(0), vdupq_n_f32(0) }};
  return ret;
}

static inline void kernel_4x4(const float *sa, const float *sb, float *C,
  float alphar, float alphai, BLASLONG K, BLASLONG LDC) {

  float32x4x4_t c1, c2, c3, c4;
  c1 = c2 = c3 = c4 = init_expanded_m2n2();

  for (; K > 1; K -= 2) {
    float32x4_t a1 = vld1q_f32(sa), a2 = vld1q_f32(sa + 4),
      a3 = vld1q_f32(sa + 8), a4 = vld1q_f32(sa + 12); sa += 16;
    float32x4_t b1 = vld1q_f32(sb), b2 = vld1q_f32(sb + 4),
      b3 = vld1q_f32(sb + 8), b4 = vld1q_f32(sb + 12); sb += 16;
    c1 = acc_expanded_m2n2(c1, a1, b1);
    c2 = acc_expanded_m2n2(c2, a2, b1);
    c3 = acc_expanded_m2n2(c3, a1, b2);
    c4 = acc_expanded_m2n2(c4, a2, b2);
    c1 = acc_expanded_m2n2(c1, a3, b3);
    c2 = acc_expanded_m2n2(c2, a4, b3);
    c3 = acc_expanded_m2n2(c3, a3, b4);
    c4 = acc_expanded_m2n2(c4, a4, b4);
  }
  if (K) {
    float32x4_t a1 = vld1q_f32(sa), a2 = vld1q_f32(sa + 4);
    float32x4_t b1 = vld1q_f32(sb), b2 = vld1q_f32(sb + 4);
    c1 = acc_expanded_m2n2(c1, a1, b1);
    c2 = acc_expanded_m2n2(c2, a2, b1);
    c3 = acc_expanded_m2n2(c3, a1, b2);
    c4 = acc_expanded_m2n2(c4, a2, b2);
  }

  float32x4x4_t e_alpha = expand_alpha(alphar, alphai);
  store_expanded_m2n2(C, LDC, c1, e_alpha);
  store_expanded_m2n2(C + 4, LDC, c2, e_alpha);
  C += LDC * 4;
  store_expanded_m2n2(C, LDC, c3, e_alpha);
  store_expanded_m2n2(C + 4, LDC, c4, e_alpha);
}

static inline void kernel_8x2(const float *sa, const float *sb, float *C,
  float alphar, float alphai, BLASLONG K, BLASLONG LDC) {

  float32x4x4_t c1, c2, c3, c4;
  c1 = c2 = c3 = c4 = init_expanded_m2n2();

  for (; K > 1; K -= 2) {
    float32x4_t a1 = vld1q_f32(sa), a2 = vld1q_f32(sa + 4);
    float32x4_t a3 = vld1q_f32(sa + 8), a4 = vld1q_f32(sa + 12);
    float32x4_t a5 = vld1q_f32(sa + 16), a6 = vld1q_f32(sa + 20);
    float32x4_t a7 = vld1q_f32(sa + 24), a8 = vld1q_f32(sa + 28); sa += 32;
    float32x4_t b1 = vld1q_f32(sb), b2 = vld1q_f32(sb + 4); sb += 8;
    c1 = acc_expanded_m2n2(c1, a1, b1);
    c2 = acc_expanded_m2n2(c2, a2, b1);
    c3 = acc_expanded_m2n2(c3, a3, b1);
    c4 = acc_expanded_m2n2(c4, a4, b1);
    c1 = acc_expanded_m2n2(c1, a5, b2);
    c2 = acc_expanded_m2n2(c2, a6, b2);
    c3 = acc_expanded_m2n2(c3, a7, b2);
    c4 = acc_expanded_m2n2(c4, a8, b2);
  }
  if (K) {
    float32x4_t a1 = vld1q_f32(sa), a2 = vld1q_f32(sa + 4);
    float32x4_t a3 = vld1q_f32(sa + 8), a4 = vld1q_f32(sa + 12);
    float32x4_t b1 = vld1q_f32(sb);
    c1 = acc_expanded_m2n2(c1, a1, b1);
    c2 = acc_expanded_m2n2(c2, a2, b1);
    c3 = acc_expanded_m2n2(c3, a3, b1);
    c4 = acc_expanded_m2n2(c4, a4, b1);
  }

  float32x4x4_t e_alpha = expand_alpha(alphar, alphai);
  store_expanded_m2n2(C, LDC, c1, e_alpha);
  store_expanded_m2n2(C + 4, LDC, c2, e_alpha);
  store_expanded_m2n2(C + 8, LDC, c3, e_alpha);
  store_expanded_m2n2(C + 12, LDC, c4, e_alpha);
}

static inline void kernel_4x2(const float *sa, const float *sb, float *C,
  float alphar, float alphai, BLASLONG K, BLASLONG LDC) {

  float32x4x4_t c1, c2;
  c1 = c2 = init_expanded_m2n2();

  for (; K > 1; K -= 2) {
    float32x4_t a1 = vld1q_f32(sa), a2 = vld1q_f32(sa + 4);
    float32x4_t a3 = vld1q_f32(sa + 8), a4 = vld1q_f32(sa + 12); sa += 16;
    float32x4_t b1 = vld1q_f32(sb), b2 = vld1q_f32(sb + 4); sb += 8;
    c1 = acc_expanded_m2n2(c1, a1, b1);
    c2 = acc_expanded_m2n2(c2, a2, b1);
    c1 = acc_expanded_m2n2(c1, a3, b2);
    c2 = acc_expanded_m2n2(c2, a4, b2);
  }
  if (K) {
    float32x4_t a1 = vld1q_f32(sa), a2 = vld1q_f32(sa + 4);
    float32x4_t b1 = vld1q_f32(sb);
    c1 = acc_expanded_m2n2(c1, a1, b1);
    c2 = acc_expanded_m2n2(c2, a2, b1);
  }

  float32x4x4_t e_alpha = expand_alpha(alphar, alphai);
  store_expanded_m2n2(C, LDC, c1, e_alpha);
  store_expanded_m2n2(C + 4, LDC, c2, e_alpha);
}

static inline void kernel_2x4(const float *sa, const float *sb, float *C,
  float alphar, float alphai, BLASLONG K, BLASLONG LDC) {

  float32x4x4_t c1, c2;
  c1 = c2 = init_expanded_m2n2();

  for (; K > 1; K -= 2) {
    float32x4_t a1 = vld1q_f32(sa), a2 = vld1q_f32(sa + 4); sa += 8;
    float32x4_t b1 = vld1q_f32(sb), b2 = vld1q_f32(sb + 4);
    float32x4_t b3 = vld1q_f32(sb + 8), b4 = vld1q_f32(sb + 12); sb += 16;
    c1 = acc_expanded_m2n2(c1, a1, b1);
    c2 = acc_expanded_m2n2(c2, a1, b2);
    c1 = acc_expanded_m2n2(c1, a2, b3);
    c2 = acc_expanded_m2n2(c2, a2, b4);
  }
  if (K) {
    float32x4_t a1 = vld1q_f32(sa);
    float32x4_t b1 = vld1q_f32(sb), b2 = vld1q_f32(sb + 4);
    c1 = acc_expanded_m2n2(c1, a1, b1);
    c2 = acc_expanded_m2n2(c2, a1, b2);
  }

  float32x4x4_t e_alpha = expand_alpha(alphar, alphai);
  store_expanded_m2n2(C, LDC, c1, e_alpha);
  store_expanded_m2n2(C + LDC * 4, LDC, c2, e_alpha);
}

static inline void kernel_2x2(const float *sa, const float *sb, float *C,
  float alphar, float alphai, BLASLONG K, BLASLONG LDC) {

  float32x4x4_t c1, c2;
  c1 = c2 = init_expanded_m2n2();

  for (; K > 1; K -= 2) {
    float32x4_t a1 = vld1q_f32(sa), a2 = vld1q_f32(sa + 4); sa += 8;
    float32x4_t b1 = vld1q_f32(sb), b2 = vld1q_f32(sb + 4); sb += 8;
    c1 = acc_expanded_m2n2(c1, a1, b1);
    c2 = acc_expanded_m2n2(c2, a2, b2);
  }
  c1.val[0] = vaddq_f32(c1.val[0], c2.val[0]);
  c1.val[1] = vaddq_f32(c1.val[1], c2.val[1]);
  c1.val[2] = vaddq_f32(c1.val[2], c2.val[2]);
  c1.val[3] = vaddq_f32(c1.val[3], c2.val[3]);
  if (K) {
    float32x4_t a1 = vld1q_f32(sa);
    float32x4_t b1 = vld1q_f32(sb);
    c1 = acc_expanded_m2n2(c1, a1, b1);
  }

  store_expanded_m2n2(C, LDC, c1, expand_alpha(alphar, alphai));
}

static inline float32x4x2_t acc_expanded_m2n1(float32x4x2_t acc,
  float32x4_t a, float32x2_t b) {

  acc.val[0] = vfmaq_lane_f32(acc.val[0], a, b, 0);
  acc.val[1] = vfmaq_lane_f32(acc.val[1], a, b, 1);
  return acc;
}

static inline void store_expanded_m2n1(float *C,
  float32x4x2_t acc, float32x4x4_t expanded_alpha) {

  float32x4_t ld1 = vld1q_f32(C);
  ld1 = vfmaq_f32(ld1, acc.val[0], expanded_alpha.val[0]);
  acc.val[0] = vrev64q_f32(acc.val[0]);
  ld1 = vfmaq_f32(ld1, acc.val[1], expanded_alpha.val[1]);
  acc.val[1] = vrev64q_f32(acc.val[1]);
  ld1 = vfmaq_f32(ld1, acc.val[0], expanded_alpha.val[2]);
  ld1 = vfmaq_f32(ld1, acc.val[1], expanded_alpha.val[3]);
  vst1q_f32(C, ld1);
}

static inline float32x4x2_t init_expanded_m2n1() {
  float32x4x2_t ret = {{ vdupq_n_f32(0), vdupq_n_f32(0) }};
  return ret;
}

static inline void kernel_8x1(const float *sa, const float *sb, float *C,
  float alphar, float alphai, BLASLONG K) {

  float32x4x2_t c1, c2, c3, c4;
  c1 = c2 = c3 = c4 = init_expanded_m2n1();

  for (; K > 1; K -= 2) {
    float32x4_t a1 = vld1q_f32(sa), a2 = vld1q_f32(sa + 4),
      a3 = vld1q_f32(sa + 8), a4 = vld1q_f32(sa + 12),
      a5 = vld1q_f32(sa + 16), a6 = vld1q_f32(sa + 20),
      a7 = vld1q_f32(sa + 24), a8 = vld1q_f32(sa + 28); sa += 32;
    float32x2_t b1 = vld1_f32(sb), b2 = vld1_f32(sb + 2); sb += 4;
    c1 = acc_expanded_m2n1(c1, a1, b1);
    c2 = acc_expanded_m2n1(c2, a2, b1);
    c3 = acc_expanded_m2n1(c3, a3, b1);
    c4 = acc_expanded_m2n1(c4, a4, b1);
    c1 = acc_expanded_m2n1(c1, a5, b2);
    c2 = acc_expanded_m2n1(c2, a6, b2);
    c3 = acc_expanded_m2n1(c3, a7, b2);
    c4 = acc_expanded_m2n1(c4, a8, b2);
  }
  if (K) {
    float32x4_t a1 = vld1q_f32(sa), a2 = vld1q_f32(sa + 4),
      a3 = vld1q_f32(sa + 8), a4 = vld1q_f32(sa + 12);
    float32x2_t b1 = vld1_f32(sb);
    c1 = acc_expanded_m2n1(c1, a1, b1);
    c2 = acc_expanded_m2n1(c2, a2, b1);
    c3 = acc_expanded_m2n1(c3, a3, b1);
    c4 = acc_expanded_m2n1(c4, a4, b1);
  }

  float32x4x4_t expanded_alpha = expand_alpha(alphar, alphai);
  store_expanded_m2n1(C, c1, expanded_alpha);
  store_expanded_m2n1(C + 4, c2, expanded_alpha);
  store_expanded_m2n1(C + 8, c3, expanded_alpha);
  store_expanded_m2n1(C + 12, c4, expanded_alpha);
}

static inline void kernel_4x1(const float *sa, const float *sb, float *C,
  float alphar, float alphai, BLASLONG K) {

  float32x4x2_t c1, c2, c3, c4;
  c1 = c2 = c3 = c4 = init_expanded_m2n1();

  for (; K > 1; K -= 2) {
    float32x4_t a1 = vld1q_f32(sa), a2 = vld1q_f32(sa + 4),
      a3 = vld1q_f32(sa + 8), a4 = vld1q_f32(sa + 12); sa += 16;
    float32x2_t b1 = vld1_f32(sb), b2 = vld1_f32(sb + 2); sb += 4;
    c1 = acc_expanded_m2n1(c1, a1, b1);
    c2 = acc_expanded_m2n1(c2, a2, b1);
    c3 = acc_expanded_m2n1(c3, a3, b2);
    c4 = acc_expanded_m2n1(c4, a4, b2);
  }
  c1.val[0] = vaddq_f32(c1.val[0], c3.val[0]);
  c1.val[1] = vaddq_f32(c1.val[1], c3.val[1]);
  c2.val[0] = vaddq_f32(c2.val[0], c4.val[0]);
  c2.val[1] = vaddq_f32(c2.val[1], c4.val[1]);
  if (K) {
    float32x4_t a1 = vld1q_f32(sa), a2 = vld1q_f32(sa + 4);
    float32x2_t b1 = vld1_f32(sb);
    c1 = acc_expanded_m2n1(c1, a1, b1);
    c2 = acc_expanded_m2n1(c2, a2, b1);
  }

  float32x4x4_t expanded_alpha = expand_alpha(alphar, alphai);
  store_expanded_m2n1(C, c1, expanded_alpha);
  store_expanded_m2n1(C + 4, c2, expanded_alpha);
}

static inline void kernel_2x1(const float *sa, const float *sb, float *C,
  float alphar, float alphai, BLASLONG K) {

  float32x4x2_t c1, c2, c3, c4;
  c1 = c2 = c3 = c4 = init_expanded_m2n1();

  for (; K > 3; K -= 4) {
    float32x4_t a1 = vld1q_f32(sa), a2 = vld1q_f32(sa + 4),
      a3 = vld1q_f32(sa + 8), a4 = vld1q_f32(sa + 12); sa += 16;
    float32x2_t b1 = vld1_f32(sb), b2 = vld1_f32(sb + 2),
      b3 = vld1_f32(sb + 4), b4 = vld1_f32(sb + 6); sb += 8;
    c1 = acc_expanded_m2n1(c1, a1, b1);
    c2 = acc_expanded_m2n1(c2, a2, b2);
    c3 = acc_expanded_m2n1(c3, a3, b3);
    c4 = acc_expanded_m2n1(c4, a4, b4);
  }
  c1.val[0] = vaddq_f32(c1.val[0], c3.val[0]);
  c1.val[1] = vaddq_f32(c1.val[1], c3.val[1]);
  c2.val[0] = vaddq_f32(c2.val[0], c4.val[0]);
  c2.val[1] = vaddq_f32(c2.val[1], c4.val[1]);
  c1.val[0] = vaddq_f32(c1.val[0], c2.val[0]);
  c1.val[1] = vaddq_f32(c1.val[1], c2.val[1]);
  for (; K; K--) {
    float32x4_t a1 = vld1q_f32(sa); sa += 4;
    float32x2_t b1 = vld1_f32(sb); sb += 2;
    c1 = acc_expanded_m2n1(c1, a1, b1);
  }

  float32x4x4_t expanded_alpha = expand_alpha(alphar, alphai);
  store_expanded_m2n1(C, c1, expanded_alpha);
}

static inline float32x2x4_t expand_alpha_d(float alphar, float alphai) {
  float32x2x4_t ret;
  const float maskp[] = { -1, 1 };
  const float maskn[] = { 1, -1 };
  const float32x2_t vrevp = vld1_f32(maskp);
  const float32x2_t vrevn = vld1_f32(maskn);
#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
  ret.val[0] = vdup_n_f32(alphar);
  ret.val[1] = vdup_n_f32(-alphai);
  ret.val[2] = vmul_f32(ret.val[1], vrevn);
  ret.val[3] = vmul_f32(ret.val[0], vrevp);
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
  ret.val[0] = vdup_n_f32(alphar);
  ret.val[1] = vdup_n_f32(alphai);
  ret.val[2] = vmul_f32(ret.val[1], vrevp);
  ret.val[3] = vmul_f32(ret.val[0], vrevn);
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
  ret.val[2] = vdup_n_f32(alphai);
  ret.val[3] = vdup_n_f32(alphar);
  ret.val[0] = vmul_f32(ret.val[3], vrevn);
  ret.val[1] = vmul_f32(ret.val[2], vrevp);
#else
  ret.val[2] = vdup_n_f32(alphai);
  ret.val[3] = vdup_n_f32(-alphar);
  ret.val[0] = vmul_f32(ret.val[3], vrevp);
  ret.val[1] = vmul_f32(ret.val[2], vrevn);
#endif
  return ret;
}

static inline float32x2x2_t acc_expanded_m1n1(float32x2x2_t acc,
  float32x2_t a, float32x2_t b) {

  acc.val[0] = vfma_lane_f32(acc.val[0], a, b, 0);
  acc.val[1] = vfma_lane_f32(acc.val[1], a, b, 1);
  return acc;
}

static inline void store_expanded_m1n1(float *C,
  float32x2x2_t acc, float32x2x4_t expanded_alpha) {

  float32x2_t ld1 = vld1_f32(C);
  ld1 = vfma_f32(ld1, acc.val[0], expanded_alpha.val[0]);
  acc.val[0] = vrev64_f32(acc.val[0]);
  ld1 = vfma_f32(ld1, acc.val[1], expanded_alpha.val[1]);
  acc.val[1] = vrev64_f32(acc.val[1]);
  ld1 = vfma_f32(ld1, acc.val[0], expanded_alpha.val[2]);
  ld1 = vfma_f32(ld1, acc.val[1], expanded_alpha.val[3]);
  vst1_f32(C, ld1);
}

static inline float32x2x2_t init_expanded_m1n1() {
  float32x2x2_t ret = {{ vdup_n_f32(0), vdup_n_f32(0) }};
  return ret;
}

static inline void kernel_1x4(const float *sa, const float *sb, float *C,
  float alphar, float alphai, BLASLONG K, BLASLONG LDC) {

  float32x2x2_t c1, c2, c3, c4;
  c1 = c2 = c3 = c4 = init_expanded_m1n1();

  for (; K; K--) {
    float32x2_t a1 = vld1_f32(sa); sa += 2;
    c1 = acc_expanded_m1n1(c1, a1, vld1_f32(sb));
    c2 = acc_expanded_m1n1(c2, a1, vld1_f32(sb + 2));
    c3 = acc_expanded_m1n1(c3, a1, vld1_f32(sb + 4));
    c4 = acc_expanded_m1n1(c4, a1, vld1_f32(sb + 6));
    sb += 8;
  }

  float32x2x4_t expanded_alpha = expand_alpha_d(alphar, alphai);
  store_expanded_m1n1(C, c1, expanded_alpha); C += LDC * 2;
  store_expanded_m1n1(C, c2, expanded_alpha); C += LDC * 2;
  store_expanded_m1n1(C, c3, expanded_alpha); C += LDC * 2;
  store_expanded_m1n1(C, c4, expanded_alpha);
}

static inline void kernel_1x2(const float *sa, const float *sb, float *C,
  float alphar, float alphai, BLASLONG K, BLASLONG LDC) {

  float32x2x2_t c1, c2, c3, c4;
  c1 = c2 = c3 = c4 = init_expanded_m1n1();

  for (; K > 1; K -= 2) {
    float32x2_t a1 = vld1_f32(sa), a2 = vld1_f32(sa + 2); sa += 4;
    c1 = acc_expanded_m1n1(c1, a1, vld1_f32(sb));
    c2 = acc_expanded_m1n1(c2, a1, vld1_f32(sb + 2));
    c3 = acc_expanded_m1n1(c3, a2, vld1_f32(sb + 4));
    c4 = acc_expanded_m1n1(c4, a2, vld1_f32(sb + 6));
    sb += 8;
  }
  c1.val[0] = vadd_f32(c1.val[0], c3.val[0]);
  c1.val[1] = vadd_f32(c1.val[1], c3.val[1]);
  c2.val[0] = vadd_f32(c2.val[0], c4.val[0]);
  c2.val[1] = vadd_f32(c2.val[1], c4.val[1]);
  if (K) {
    float32x2_t a1 = vld1_f32(sa);
    c1 = acc_expanded_m1n1(c1, a1, vld1_f32(sb));
    c2 = acc_expanded_m1n1(c2, a1, vld1_f32(sb + 2));
  }

  float32x2x4_t expanded_alpha = expand_alpha_d(alphar, alphai);
  store_expanded_m1n1(C, c1, expanded_alpha); C += LDC * 2;
  store_expanded_m1n1(C, c2, expanded_alpha);
}

static inline void kernel_1x1(const float *sa, const float *sb, float *C,
  float alphar, float alphai, BLASLONG K) {

  float32x2x2_t c1, c2, c3, c4;
  c1 = c2 = c3 = c4 = init_expanded_m1n1();

  for (; K > 3; K -= 4) {
    c1 = acc_expanded_m1n1(c1, vld1_f32(sa), vld1_f32(sb));
    c2 = acc_expanded_m1n1(c2, vld1_f32(sa + 2), vld1_f32(sb + 2));
    c3 = acc_expanded_m1n1(c3, vld1_f32(sa + 4), vld1_f32(sb + 4));
    c4 = acc_expanded_m1n1(c4, vld1_f32(sa + 6), vld1_f32(sb + 6));
    sa += 8; sb += 8;
  }
  c1.val[0] = vadd_f32(c1.val[0], c3.val[0]);
  c1.val[1] = vadd_f32(c1.val[1], c3.val[1]);
  c2.val[0] = vadd_f32(c2.val[0], c4.val[0]);
  c2.val[1] = vadd_f32(c2.val[1], c4.val[1]);
  c1.val[0] = vadd_f32(c1.val[0], c2.val[0]);
  c1.val[1] = vadd_f32(c1.val[1], c2.val[1]);
  for (; K; K--) {
    c1 = acc_expanded_m1n1(c1, vld1_f32(sa), vld1_f32(sb));
    sa += 2; sb += 2;
  }

  store_expanded_m1n1(C, c1, expand_alpha_d(alphar, alphai));
}

int CNAME(BLASLONG M, BLASLONG N, BLASLONG K, FLOAT alphar, FLOAT alphai,
  FLOAT *sa, FLOAT *sb, FLOAT *C, BLASLONG LDC) {

  BLASLONG n_left = N;
  for (; n_left >= 8; n_left -= 8) {
    const FLOAT *a_ = sa;
    FLOAT *c1_ = C;
    FLOAT *c2_ = C + LDC * 8;
    const FLOAT *b1_ = sb;
    const FLOAT *b2_ = sb + K * 8;
    BLASLONG m_left = M;
    for (; m_left >= 8; m_left -= 8) {
      kernel_8x4(a_, b1_, c1_, alphar, alphai, K, LDC);
      kernel_8x4(a_, b2_, c2_, alphar, alphai, K, LDC);
      a_ += 16 * K;
      c1_ += 16;
      c2_ += 16;
    }
    if (m_left >= 4) {
      m_left -= 4;
      kernel_4x4(a_, b1_, c1_, alphar, alphai, K, LDC);
      kernel_4x4(a_, b2_, c2_, alphar, alphai, K, LDC);
      a_ += 8 * K;
      c1_ += 8;
      c2_ += 8;
    }
    if (m_left >= 2) {
      m_left -= 2;
      kernel_2x4(a_, b1_, c1_, alphar, alphai, K, LDC);
      kernel_2x4(a_, b2_, c2_, alphar, alphai, K, LDC);
      a_ += 4 * K;
      c1_ += 4;
      c2_ += 4;
    }
    if (m_left) {
      kernel_1x4(a_, b1_, c1_, alphar, alphai, K, LDC);
      kernel_1x4(a_, b2_, c2_, alphar, alphai, K, LDC);
    }
    C += 16 * LDC;
    sb += 16 * K;
  }

  if (n_left >= 4) {
    n_left -= 4;
    const FLOAT *a_ = sa;
    FLOAT *c_ = C;
    BLASLONG m_left = M;
    for (; m_left >= 8; m_left -= 8) {
      kernel_8x4(a_, sb, c_, alphar, alphai, K, LDC);
      a_ += 16 * K;
      c_ += 16;
    }
    if (m_left >= 4) {
      m_left -= 4;
      kernel_4x4(a_, sb, c_, alphar, alphai, K, LDC);
      a_ += 8 * K;
      c_ += 8;
    }
    if (m_left >= 2) {
      m_left -= 2;
      kernel_2x4(a_, sb, c_, alphar, alphai, K, LDC);
      a_ += 4 * K;
      c_ += 4;
    }
    if (m_left) {
      kernel_1x4(a_, sb, c_, alphar, alphai, K, LDC);
    }
    C += 8 * LDC;
    sb += 8 * K;
  }

  if (n_left >= 2) {
    n_left -= 2;
    const FLOAT *a_ = sa;
    FLOAT *c_ = C;
    BLASLONG m_left = M;
    for (; m_left >= 8; m_left -= 8) {
      kernel_8x2(a_, sb, c_, alphar, alphai, K, LDC);
      a_ += 16 * K;
      c_ += 16;
    }
    if (m_left >= 4) {
      m_left -= 4;
      kernel_4x2(a_, sb, c_, alphar, alphai, K, LDC);
      a_ += 8 * K;
      c_ += 8;
    }
    if (m_left >= 2) {
      m_left -= 2;
      kernel_2x2(a_, sb, c_, alphar, alphai, K, LDC);
      a_ += 4 * K;
      c_ += 4;
    }
    if (m_left) {
      kernel_1x2(a_, sb, c_, alphar, alphai, K, LDC);
    }
    C += 4 * LDC;
    sb += 4 * K;
  }

  if (n_left) {
    BLASLONG m_left = M;
    for (; m_left >= 8; m_left -= 8) {
      kernel_8x1(sa, sb, C, alphar, alphai, K);
      sa += 16 * K;
      C += 16;
    }
    if (m_left >= 4) {
      m_left -= 4;
      kernel_4x1(sa, sb, C, alphar, alphai, K);
      sa += 8 * K;
      C += 8;
    }
    if (m_left >= 2) {
      m_left -= 2;
      kernel_2x1(sa, sb, C, alphar, alphai, K);
      sa += 4 * K;
      C += 4;
    }
    if (m_left) {
      kernel_1x1(sa, sb, C, alphar, alphai, K);
    }
  }
  return 0;
}

