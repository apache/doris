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

#define SAVE1x4(c1) \
    "vfmul.s $vr"#c1",$vr"#c1",$vr15;vstelm.w $vr"#c1",%4,0,0;add.d %4,%4,%3;\n\t" \
    "vstelm.w $vr"#c1",%4,0,1;add.d %4,%4,%3;vstelm.w $vr"#c1",%4,0,2;add.d %4,%4,%3; \n\t" \
    "vstelm.w $vr"#c1",%4,0,3;add.d %4,%4,%3;\n\t"
#define SAVE_2x4(c1, c2, t1, t2) \
    "vilvl.w $vr"#t1",$vr"#c2",$vr"#c1";vfmul.s $vr"#t1",$vr"#t1",$vr15; \n\t" \
    "vstelm.d $vr"#t1",%4,0,0;add.d %4,%4,%3;vstelm.d $vr"#t1",%4,0,1;add.d %4,%4,%3 \n\t" \
    "vilvh.w $vr"#t2",$vr"#c2",$vr"#c1";vfmul.s $vr"#t2",$vr"#t2",$vr15; \n\t" \
    "vstelm.d $vr"#t2",%4,0,0;add.d %4,%4,%3;vstelm.d $vr"#t2",%4,0,1;add.d %4,%4,%3 \n\t"
#define SAVE_4x1(b1) \
    "vst $vr"#b1",%4,0; add.d %4,%4,%3 \n\t"
#define SAVE_4x2(b1, b2) \
    "vst $vr"#b1",%4,0; add.d %4,%4,%3; vst $vr"#b2",%4,0; add.d %4,%4,%3 \n\t"
#define SAVE_4x4(b1, b2, b3, b4) \
    "vst $vr"#b1",%4,0; add.d %4,%4,%3; vst $vr"#b2",%4,0; add.d %4,%4,%3 \n\t" \
    "vst $vr"#b3",%4,0; add.d %4,%4,%3; vst $vr"#b4",%4,0; add.d %4,%4,%3 \n\t"
#define SAVE_4x8(b1, b2, b3, b4) \
    SAVE_4x4(b1, b2, b3, b4) \
    "xvpermi.q $xr"#b1",$xr"#b1",1; xvpermi.q $xr"#b2",$xr"#b2",1; xvpermi.q $xr"#b3",$xr"#b3",1; xvpermi.q $xr"#b4",$xr"#b4",1; \n\t" \
    SAVE_4x4(b1, b2, b3, b4)

#define TRANS_4x4(a1, a2, a3, a4, t1, t2, t3, t4) \
    "vilvl.w $vr"#t1",$vr"#a2",$vr"#a1";vilvh.w $vr"#t2",$vr"#a2",$vr"#a1"; \n\t" \
    "vilvl.w $vr"#t3",$vr"#a4",$vr"#a3";vilvh.w $vr"#t4",$vr"#a4",$vr"#a3"; \n\t" \
    "vilvl.d $vr"#a1",$vr"#t3",$vr"#t1";vilvh.d $vr"#a2",$vr"#t3",$vr"#t1"; \n\t" \
    "vilvl.d $vr"#a3",$vr"#t4",$vr"#t2";vilvh.d $vr"#a4",$vr"#t4",$vr"#t2"; \n\t"
#define TRANS_4x8(a1, a2, a3, a4, t1, t2, t3, t4) \
    "xvilvl.w $xr"#t1",$xr"#a2",$xr"#a1"; xvilvh.w $xr"#t2",$xr"#a2",$xr"#a1"; \n\t" \
    "xvilvl.w $xr"#t3",$xr"#a4",$xr"#a3"; xvilvh.w $xr"#t4",$xr"#a4",$xr"#a3"; \n\t" \
    "xvilvl.d $xr"#a1",$xr"#t3",$xr"#t1"; xvilvh.d $xr"#a2",$xr"#t3",$xr"#t1"; \n\t" \
    "xvilvl.d $xr"#a3",$xr"#t4",$xr"#t2"; xvilvh.d $xr"#a4",$xr"#t4",$xr"#t2"; \n\t"
#define COPY_4x16 \
    "move %4,%1; addi.d %1,%1,16                      \n\t" \
    "xvld $xr0,%0,0; xvld $xr4,%0,32; add.d %0,%0,%2; \n\t" \
    "xvld $xr1,%0,0; xvld $xr5,%0,32; add.d %0,%0,%2; \n\t" \
    "xvld $xr2,%0,0; xvld $xr6,%0,32; add.d %0,%0,%2; \n\t" \
    "xvld $xr3,%0,0; xvld $xr7,%0,32; add.d %0,%0,%2; \n\t" \
    "xvfmul.s $xr0,$xr0,$xr15;xvfmul.s $xr1,$xr1,$xr15;xvfmul.s $xr2,$xr2,$xr15;xvfmul.s $xr3,$xr3,$xr15 \n\t" \
    "xvfmul.s $xr4,$xr4,$xr15;xvfmul.s $xr5,$xr5,$xr15;xvfmul.s $xr6,$xr6,$xr15;xvfmul.s $xr7,$xr7,$xr15 \n\t" \
    TRANS_4x8(0,1,2,3,8,9,10,11) SAVE_4x8(0,1,2,3)          \
    TRANS_4x8(4,5,6,7,8,9,10,11) SAVE_4x8(4,5,6,7)
#define COPY_4x8 \
    "move %4,%1; addi.d %1,%1,16     \n\t" \
    "xvld $xr0,%0,0; add.d %0,%0,%2; \n\t" \
    "xvld $xr1,%0,0; add.d %0,%0,%2; \n\t" \
    "xvld $xr2,%0,0; add.d %0,%0,%2; \n\t" \
    "xvld $xr3,%0,0; add.d %0,%0,%2; \n\t" \
    "xvfmul.s $xr0,$xr0,$xr15;xvfmul.s $xr1,$xr1,$xr15;xvfmul.s $xr2,$xr2,$xr15;xvfmul.s $xr3,$xr3,$xr15 \n\t" \
    TRANS_4x8(0,1,2,3,8,9,10,11) SAVE_4x8(0,1,2,3)
#define COPY_4x4 \
    "move %4,%1; addi.d %1,%1,16    \n\t" \
    "vld $vr0,%0,0; add.d %0,%0,%2; \n\t" \
    "vld $vr1,%0,0; add.d %0,%0,%2; \n\t" \
    "vld $vr2,%0,0; add.d %0,%0,%2; \n\t" \
    "vld $vr3,%0,0; add.d %0,%0,%2; \n\t" \
    "vfmul.s $vr0,$vr0,$vr15;vfmul.s $vr1,$vr1,$vr15;vfmul.s $vr2,$vr2,$vr15;vfmul.s $vr3,$vr3,$vr15 \n\t" \
    TRANS_4x4(0,1,2,3,8,9,10,11) SAVE_4x4(0,1,2,3)
#define COPY_4x2 \
    "move %4,%1; addi.d %1,%1,16    \n\t" \
    "vld $vr0,%0,0; add.d %0,%0,%2; \n\t" \
    "vld $vr1,%0,0; add.d %0,%0,%2; \n\t" \
    "vld $vr2,%0,0; add.d %0,%0,%2; \n\t" \
    "vld $vr3,%0,0; add.d %0,%0,%2; \n\t" \
    "vfmul.s $vr0,$vr0,$vr15;vfmul.s $vr1,$vr1,$vr15;vfmul.s $vr2,$vr2,$vr15;vfmul.s $vr3,$vr3,$vr15 \n\t" \
    TRANS_4x4(0,1,2,3,8,9,10,11) SAVE_4x2(0,1)
#define COPY_4x1 \
    "move %4,%1; addi.d %1,%1,16     \n\t" \
    "fld.s $f0,%0,0; add.d %0,%0,%2; \n\t" \
    "fld.s $f1,%0,0; add.d %0,%0,%2; \n\t" \
    "fld.s $f2,%0,0; add.d %0,%0,%2; \n\t" \
    "fld.s $f3,%0,0; add.d %0,%0,%2; \n\t" \
    "xvinsve0.w $xr0,$xr1,1;xvinsve0.w $xr0,$xr2,2;xvinsve0.w $xr0,$xr3,3; \n\t" \
    "vfmul.s $vr0,$vr0,$vr15; \n\t" \
    SAVE_4x1(0)

#define COPY_2x16 \
    "move %4,%1; addi.d %1,%1,8                       \n\t" \
    "xvld $xr0,%0,0; xvld $xr2,%0,32; add.d %0,%0,%2; \n\t" \
    "xvld $xr1,%0,0; xvld $xr3,%0,32; add.d %0,%0,%2; \n\t" \
    "xvpermi.q $xr4,$xr0,1;xvpermi.q $xr6,$xr2,1;xvpermi.q $xr5,$xr1,1;xvpermi.q $xr7,$xr3,1; \n\t" \
    SAVE_2x4(0,1,8,9) SAVE_2x4(4,5,8,9) SAVE_2x4(2,3,8,9) SAVE_2x4(6,7,8,9)
#define COPY_2x8 \
    "move %4,%1; addi.d %1,%1,8      \n\t" \
    "xvld $xr0,%0,0; add.d %0,%0,%2; \n\t" \
    "xvld $xr1,%0,0; add.d %0,%0,%2; \n\t" \
    "xvpermi.q $xr2,$xr0,1;xvpermi.q $xr3,$xr1,1; \n\t" \
    SAVE_2x4(0,1,4,5) SAVE_2x4(2,3,4,5)
#define COPY_2x4 \
    "move %4,%1; addi.d %1,%1,8     \n\t" \
    "vld $vr0,%0,0; add.d %0,%0,%2; \n\t" \
    "vld $vr1,%0,0; add.d %0,%0,%2; \n\t" \
    SAVE_2x4(0,1,4,5)
#define COPY_2x2 \
    "move %4,%1; addi.d %1,%1,8     \n\t" \
    "fld.d $f0,%0,0;add.d %0,%0,%2; \n\t" \
    "fld.d $f1,%0,0;add.d %0,%0,%2; \n\t" \
    "xvinsve0.d $xr0,$xr1,1;vfmul.s $vr0,$vr0,$vr15;vshuf4i.w $vr0,$vr0,0xd8 \n\t" \
    "vstelm.d $vr0,%4,0,0;add.d %4,%4,%3;vstelm.d $vr0,%4,0,1 \n\t"
#define COPY_2x1 \
    "move %4,%1; addi.d %1,%1,8     \n\t" \
    "fld.s $f0,%0,0;add.d %0,%0,%2; \n\t" \
    "fld.s $f1,%0,0;add.d %0,%0,%2; \n\t" \
    "xvinsve0.w $xr0,$xr1,1;vfmul.s $vr0,$vr0,$vr15; \n\t" \
    "vstelm.d $vr0,%4,0,0; \n\t"

#define COPY_1x16 \
    "move %4,%1; addi.d %1,%1,4 \n\t" \
    "vld $vr1,%0,0;"  SAVE1x4(1) "vld $vr2,%0,16;" SAVE1x4(2) \
    "vld $vr1,%0,32;" SAVE1x4(1) "vld $vr2,%0,48;" SAVE1x4(2) \
    "add.d %0,%0,%2 \n\t"
#define COPY_1x8 \
    "move %4,%1; addi.d %1,%1,4 \n\t" \
    "vld $vr1,%0,0;" SAVE1x4(1) "vld $vr2,%0,16;" SAVE1x4(2) \
    "add.d %0,%0,%2 \n\t"
#define COPY_1x4 \
    "move %4,%1; addi.d %1,%1,4 \n\t" \
    "vld $vr1,%0,0;" SAVE1x4(1) \
    "add.d %0,%0,%2 \n\t"
#define COPY_1x2 \
    "move %4,%1;fld.d $f1,%0,0;add.d %0,%0,%2;vfmul.s $vr1,$vr1,$vr15;vstelm.w $vr1,%4,0,0;add.d %4,%4,%3;vstelm.w $vr1,%4,0,1;\n\t" \
    "addi.d %1,%1,4;\n\t"
#define COPY_1x1 \
    "fld.s $f1,%0,0;fmul.s $f1,$f1,$f15;fst.s $f1,%1,0;add.d %0,%0,%2;addi.d %1,%1,4;\n\t"

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
    for (;rows_left > 15; rows_left -= 16) {COMPUTE(16) src_base += 16; dst_base += 16 * ldb;}
    for (;rows_left > 7; rows_left -= 8) {COMPUTE(8) src_base += 8; dst_base += 8 * ldb;}
    for (;rows_left > 3; rows_left -= 4) {COMPUTE(4) src_base += 4; dst_base += 4 * ldb;}
    for (;rows_left > 1; rows_left -= 2) {COMPUTE(2) src_base += 2; dst_base += 2 * ldb;}
    if (rows_left > 0) {COMPUTE(1) src_base ++; dst_base += ldb;}
  }
}
