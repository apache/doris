/***************************************************************************
Copyright (c) 2024, The OpenBLAS Project
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
#define SAVE_4x1(b1, b2, b3, b4) \
    "vstelm.w $vr"#b1",%1,0,0;add.d %1,%1,%3;vstelm.w $vr"#b2",%1,0,0;add.d %1,%1,%3;\n\t" \
    "vstelm.w $vr"#b3",%1,0,0;add.d %1,%1,%3;vstelm.w $vr"#b4",%1,0,0;add.d %1,%1,%3;\n\t"
#define SAVE_4x2(b1, b2, b3, b4) \
    "vstelm.d $vr"#b1",%1,0,0;add.d %1,%1,%3;vstelm.d $vr"#b2",%1,0,0;add.d %1,%1,%3;\n\t" \
    "vstelm.d $vr"#b3",%1,0,0;add.d %1,%1,%3;vstelm.d $vr"#b4",%1,0,0;add.d %1,%1,%3;\n\t"
#define SAVE_4x4(b1, b2, b3, b4) \
    "vst $vr"#b1",%1,0;add.d %1,%1,%3;vst $vr"#b2",%1,0;add.d %1,%1,%3;\n\t" \
    "vst $vr"#b3",%1,0;add.d %1,%1,%3;vst $vr"#b4",%1,0;add.d %1,%1,%3;\n\t"
#define SAVE_4x8(b1, b2, b3, b4) \
    "xvst $xr"#b1",%1,0;add.d %1,%1,%3;xvst $xr"#b2",%1,0;add.d %1,%1,%3;\n\t" \
    "xvst $xr"#b3",%1,0;add.d %1,%1,%3;xvst $xr"#b4",%1,0;add.d %1,%1,%3;\n\t"
#define SAVE_4x16(b1, b2, b3, b4, b5, b6, b7, b8) \
    "xvst $xr"#b1",%1,0;xvst $xr"#b2",%1,32;add.d %1,%1,%3;\n\t" \
    "xvst $xr"#b3",%1,0;xvst $xr"#b4",%1,32;add.d %1,%1,%3;\n\t" \
    "xvst $xr"#b5",%1,0;xvst $xr"#b6",%1,32;add.d %1,%1,%3;\n\t" \
    "xvst $xr"#b7",%1,0;xvst $xr"#b8",%1,32;add.d %1,%1,%3;\n\t"
#define SAVE_2x16(b1, b2, b3, b4) \
    "xvst $xr"#b1",%1,0;xvst $xr"#b2",%1,32;add.d %1,%1,%3;\n\t" \
    "xvst $xr"#b3",%1,0;xvst $xr"#b4",%1,32;add.d %1,%1,%3;\n\t"
#define SAVE_2x8(b1, b2) \
    "xvst $xr"#b1",%1,0;add.d %1,%1,%3;xvst $xr"#b2",%1,0;add.d %1,%1,%3;\n\t"
#define SAVE_2x4(b1, b2) \
    "vst $vr"#b1",%1,0;add.d %1,%1,%3;vst $vr"#b2",%1,0;add.d %1,%1,%3;\n\t"
#define SAVE_2x2(b1, b2) \
    "vstelm.d $vr"#b1",%1,0,0;add.d %1,%1,%3;vstelm.d $vr"#b2",%1,0,0;add.d %1,%1,%3;\n\t"
#define SAVE_2x1(b1, b2) \
    "vstelm.w $vr"#b1",%1,0,0;add.d %1,%1,%3;vstelm.w $vr"#b2",%1,0,0;add.d %1,%1,%3;\n\t"
#define SAVE_1x16(b1, b2) \
    "xvst $xr"#b1",%1,0;xvst $xr"#b2",%1,32;add.d %1,%1,%3;\n\t"
#define SAVE_1x8(b1) \
    "xvst $xr"#b1",%1,0;add.d %1,%1,%3;\n\t"
#define SAVE_1x4(b1) \
    "vst $vr"#b1",%1,0;add.d %1,%1,%3;\n\t"
#define SAVE_1x2(b1) \
    "vstelm.d $vr"#b1",%1,0,0;add.d %1,%1,%3;\n\t"
#define SAVE_1x1(b1) \
    "vstelm.w $vr"#b1",%1,0,0;add.d %1,%1,%3;\n\t"
#define COPY_4x16 \
    "xvld $xr0,%0,0; xvld $xr4,%0,32; add.d %0,%0,%2; \n\t" \
    "xvld $xr1,%0,0; xvld $xr5,%0,32; add.d %0,%0,%2; \n\t" \
    "xvld $xr2,%0,0; xvld $xr6,%0,32; add.d %0,%0,%2; \n\t" \
    "xvld $xr3,%0,0; xvld $xr7,%0,32; add.d %0,%0,%2; \n\t" \
    "xvfmul.s $xr0,$xr0,$xr15;xvfmul.s $xr1,$xr1,$xr15;xvfmul.s $xr2,$xr2,$xr15;xvfmul.s $xr3,$xr3,$xr15 \n\t" \
    "xvfmul.s $xr4,$xr4,$xr15;xvfmul.s $xr5,$xr5,$xr15;xvfmul.s $xr6,$xr6,$xr15;xvfmul.s $xr7,$xr7,$xr15 \n\t" \
     SAVE_4x16(0,4,1,5,2,6,3,7)
#define COPY_4x8 \
    "xvld $xr0,%0,0; add.d %0,%0,%2; \n\t" \
    "xvld $xr1,%0,0; add.d %0,%0,%2; \n\t" \
    "xvld $xr2,%0,0; add.d %0,%0,%2; \n\t" \
    "xvld $xr3,%0,0; add.d %0,%0,%2; \n\t" \
    "xvfmul.s $xr0,$xr0,$xr15;xvfmul.s $xr1,$xr1,$xr15;xvfmul.s $xr2,$xr2,$xr15;xvfmul.s $xr3,$xr3,$xr15 \n\t" \
    SAVE_4x8(0,1,2,3)
#define COPY_4x4 \
    "vld $vr0,%0,0; add.d %0,%0,%2; \n\t" \
    "vld $vr1,%0,0; add.d %0,%0,%2; \n\t" \
    "vld $vr2,%0,0; add.d %0,%0,%2; \n\t" \
    "vld $vr3,%0,0; add.d %0,%0,%2; \n\t" \
    "vfmul.s $vr0,$vr0,$vr15;vfmul.s $vr1,$vr1,$vr15;vfmul.s $vr2,$vr2,$vr15;vfmul.s $vr3,$vr3,$vr15 \n\t" \
    SAVE_4x4(0,1,2,3)
#define COPY_4x2 \
    "vld $vr0,%0,0; add.d %0,%0,%2; \n\t" \
    "vld $vr1,%0,0; add.d %0,%0,%2; \n\t" \
    "vld $vr2,%0,0; add.d %0,%0,%2; \n\t" \
    "vld $vr3,%0,0; add.d %0,%0,%2; \n\t" \
    "vfmul.s $vr0,$vr0,$vr15;vfmul.s $vr1,$vr1,$vr15;vfmul.s $vr2,$vr2,$vr15;vfmul.s $vr3,$vr3,$vr15 \n\t" \
    SAVE_4x2(0,1,2,3)
#define COPY_4x1 \
    "fld.s $f0,%0,0; add.d %0,%0,%2; \n\t" \
    "fld.s $f1,%0,0; add.d %0,%0,%2; \n\t" \
    "fld.s $f2,%0,0; add.d %0,%0,%2; \n\t" \
    "fld.s $f3,%0,0; add.d %0,%0,%2; \n\t" \
    "fmul.s $f0,$f0,$f15;fmul.s $f1,$f1,$f15;fmul.s $f2,$f2,$f15;fmul.s $f3,$f3,$f15 \n\t" \
    SAVE_4x1(0,1,2,3)
#define COPY_2x16 \
    "xvld $xr0,%0,0; xvld $xr2,%0,32; add.d %0,%0,%2; \n\t" \
    "xvld $xr1,%0,0; xvld $xr3,%0,32; add.d %0,%0,%2; \n\t" \
    "xvfmul.s $xr0,$xr0,$xr15;xvfmul.s $xr1,$xr1,$xr15;xvfmul.s $xr2,$xr2,$xr15;xvfmul.s $xr3,$xr3,$xr15 \n\t" \
    SAVE_2x16(0,2,1,3)
#define COPY_2x8 \
    "xvld $xr0,%0,0; add.d %0,%0,%2; \n\t" \
    "xvld $xr1,%0,0; add.d %0,%0,%2; \n\t" \
    "xvfmul.s $xr0,$xr0,$xr15;xvfmul.s $xr1,$xr1,$xr15; \n\t" \
    SAVE_2x8(0,1)
#define COPY_2x4 \
    "vld $vr0,%0,0; add.d %0,%0,%2; \n\t" \
    "vld $vr1,%0,0; add.d %0,%0,%2; \n\t" \
    "vfmul.s $vr0,$vr0,$vr15;vfmul.s $vr1,$vr1,$vr15; \n\t" \
    SAVE_2x4(0,1)
#define COPY_2x2 \
    "fld.d $f0,%0,0;add.d %0,%0,%2; \n\t" \
    "fld.d $f1,%0,0;add.d %0,%0,%2; \n\t" \
    "vfmul.s $vr0,$vr0,$vr15;vfmul.s $vr1,$vr1,$vr15; \n\t" \
     SAVE_2x2(0,1)
#define COPY_2x1 \
    "fld.s $f0,%0,0;add.d %0,%0,%2; \n\t" \
    "fld.s $f1,%0,0;add.d %0,%0,%2; \n\t" \
    "fmul.s $f0,$f0,$f15;fmul.s $f1,$f1,$f15; \n\t" \
     SAVE_2x1(0,1)
#define COPY_1x16 \
    "xvld $xr0,%0,0; xvld $xr1,%0,32; add.d %0,%0,%2; \n\t" \
    "xvfmul.s $xr0,$xr0,$xr15;xvfmul.s $xr1,$xr1,$xr15; \n\t" \
    SAVE_1x16(0,1)
#define COPY_1x8 \
    "xvld $xr0,%0,0; add.d %0,%0,%2; \n\t" \
    "xvfmul.s $xr0,$xr0,$xr15; \n\t" \
    SAVE_1x8(0)
#define COPY_1x4 \
    "vld $vr0,%0,0; add.d %0,%0,%2; \n\t" \
    "vfmul.s $vr0,$vr0,$vr15; \n\t" \
    SAVE_1x4(0)
#define COPY_1x2 \
    "fld.d $f0,%0,0;add.d %0,%0,%2; \n\t" \
    "vfmul.s $vr0,$vr0,$vr15; \n\t" \
     SAVE_1x2(0)
#define COPY_1x1 \
    "fld.s $f0,%0,0;add.d %0,%0,%2; \n\t" \
    "fmul.s $f0,$f0,$f15; \n\t" \
     SAVE_1x1(0)
#define ROWS_OF_BLOCK 128
#define COMPUTE(ndim) \
    src = src_base; dst = dst_base; \
    __asm__ __volatile__( \
    "xvldrepl.w $xr15,   %6,    0    \n\t" \
    "srli.d     $r6,     %5,    2    \n\t" \
    "beqz       $r6,     "#ndim"3f   \n\t" \
    #ndim"4: \n\t" \
    COPY_4x##ndim  \
    "addi.d     $r6,     $r6,   -1   \n\t" \
    "bnez       $r6,     "#ndim"4b   \n\t" \
    #ndim"3: \n\t" \
    "andi       $r6,     %5,    2    \n\t" \
    "beqz       $r6,     "#ndim"1f   \n\t" \
    #ndim"2: \n\t" \
    COPY_2x##ndim  \
    #ndim"1: \n\t" \
    "andi       $r6,     %5,    1    \n\t" \
    "beqz       $r6,     "#ndim"0f   \n\t" \
    COPY_1x##ndim  \
    #ndim"0: \n\t" \
    :"+r"(src),"+r"(dst),"+r"(src_ld_bytes),"+r"(dst_ld_bytes),"+r"(dst_tmp) \
    :"r"(num_cols),"r"(&ALPHA) \
    :"memory", "$r6", "$f0", "$f1", "$f2", "$f3", "$f4", "$f5", "$f6", "$f7", "$f8", "$f9", "$f10", "$f11", "$f15" \
    );

int CNAME(BLASLONG rows, BLASLONG cols, FLOAT alpha, FLOAT *a, BLASLONG lda, FLOAT *b, BLASLONG ldb){
  float *src, *dst, *dst_tmp=0, *src_base, *dst_base;
  uint64_t src_ld_bytes = (uint64_t)lda * sizeof(float), dst_ld_bytes = (uint64_t)ldb * sizeof(float), num_cols = 0;
  BLASLONG rows_left, cols_done; float ALPHA = alpha;
  if (ALPHA == 0.0) {
    dst_base = b;
    for (rows_left = rows; rows_left > 0; rows_left--) {memset(dst_base, 0, cols * sizeof(float)); dst_base += ldb;}
    return 0;
  }
  for (cols_done = 0; cols_done < cols; cols_done += num_cols) {
    num_cols = cols - cols_done;
    if (num_cols > ROWS_OF_BLOCK) num_cols = ROWS_OF_BLOCK;
    rows_left = rows; src_base = a + (int64_t)lda * (int64_t)cols_done; dst_base = b + cols_done;
    for (;rows_left > 15; rows_left -= 16) {COMPUTE(16) src_base += 16; dst_base += 16;}
    for (;rows_left > 7;  rows_left -= 8)  {COMPUTE(8) src_base += 8; dst_base += 8;}
    for (;rows_left > 3;  rows_left -= 4)  {COMPUTE(4) src_base += 4; dst_base += 4;}
    for (;rows_left > 1;  rows_left -= 2)  {COMPUTE(2) src_base += 2; dst_base += 2;}
    if (rows_left > 0) {COMPUTE(1) src_base ++; dst_base ++;}
  }
}
