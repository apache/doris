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

#ifdef HAVE_AVX

#define ROWS_OF_BLOCK 384

  /* +r: %0 = src, %1 = dst, %2 = src_ld, %3 = dst_ld, %4 = dst_tmp */
/* m: %5 = num_rows, %6 = alpha */
/* xmm15 = alpha */
#define TRANS_4x4(a1_no,a2_no,a3_no,a4_no,t1_no,t2_no,t3_no,t4_no)\
  "vunpcklps %%xmm"#a2_no",%%xmm"#a1_no",%%xmm"#t1_no"; vunpckhps %%xmm"#a2_no",%%xmm"#a1_no",%%xmm"#t2_no";"\
  "vunpcklps %%xmm"#a4_no",%%xmm"#a3_no",%%xmm"#t3_no"; vunpckhps %%xmm"#a4_no",%%xmm"#a3_no",%%xmm"#t4_no";"\
  "vunpcklpd %%xmm"#t3_no",%%xmm"#t1_no",%%xmm"#a1_no"; vunpckhpd %%xmm"#t3_no",%%xmm"#t1_no",%%xmm"#a2_no";"\
  "vunpcklpd %%xmm"#t4_no",%%xmm"#t2_no",%%xmm"#a3_no"; vunpckhpd %%xmm"#t4_no",%%xmm"#t2_no",%%xmm"#a4_no";"

#define TRANS_4x8(a1_no,a2_no,a3_no,a4_no,t1_no,t2_no,t3_no,t4_no)\
  "vunpcklps %%ymm"#a2_no",%%ymm"#a1_no",%%ymm"#t1_no"; vunpckhps %%ymm"#a2_no",%%ymm"#a1_no",%%ymm"#t2_no";"\
  "vunpcklps %%ymm"#a4_no",%%ymm"#a3_no",%%ymm"#t3_no"; vunpckhps %%ymm"#a4_no",%%ymm"#a3_no",%%ymm"#t4_no";"\
  "vunpcklpd %%ymm"#t3_no",%%ymm"#t1_no",%%ymm"#a1_no"; vunpckhpd %%ymm"#t3_no",%%ymm"#t1_no",%%ymm"#a2_no";"\
  "vunpcklpd %%ymm"#t4_no",%%ymm"#t2_no",%%ymm"#a3_no"; vunpckhpd %%ymm"#t4_no",%%ymm"#t2_no",%%ymm"#a4_no";"

#define SAVE_4x4(b1_no,b2_no,b3_no,b4_no)\
  "vmovups %%xmm"#b1_no",(%4); vmovups %%xmm"#b2_no",(%4,%3,1); leaq (%4,%3,2),%4;"\
  "vmovups %%xmm"#b3_no",(%4); vmovups %%xmm"#b4_no",(%4,%3,1); leaq (%4,%3,2),%4;"

#define SAVE_4x8(b1_no,b2_no,b3_no,b4_no) SAVE_4x4(b1_no,b2_no,b3_no,b4_no)\
  "vextractf128 $1,%%ymm"#b1_no",(%4); vextractf128 $1,%%ymm"#b2_no",(%4,%3,1); leaq (%4,%3,2),%4;"\
  "vextractf128 $1,%%ymm"#b3_no",(%4); vextractf128 $1,%%ymm"#b4_no",(%4,%3,1); leaq (%4,%3,2),%4;"

#define COPY_4x16 "movq %1,%4; addq $16,%1;"\
  "vmulps (%0),%%ymm15,%%ymm0; vmulps 32(%0),%%ymm15,%%ymm4; vmulps (%0,%2,1),%%ymm15,%%ymm1; vmulps 32(%0,%2,1),%%ymm15,%%ymm5; leaq (%0,%2,2),%0;"\
  "vmulps (%0),%%ymm15,%%ymm2; vmulps 32(%0),%%ymm15,%%ymm6; vmulps (%0,%2,1),%%ymm15,%%ymm3; vmulps 32(%0,%2,1),%%ymm15,%%ymm7; leaq (%0,%2,2),%0;"\
  TRANS_4x8(0,1,2,3,8,9,10,11) SAVE_4x8(0,1,2,3)\
  TRANS_4x8(4,5,6,7,8,9,10,11) SAVE_4x8(4,5,6,7)

#define COPY_4x8 "movq %1,%4; addq $16,%1;"\
  "vmulps (%0),%%ymm15,%%ymm0; vmulps (%0,%2,1),%%ymm15,%%ymm1; leaq (%0,%2,2),%0;"\
  "vmulps (%0),%%ymm15,%%ymm2; vmulps (%0,%2,1),%%ymm15,%%ymm3; leaq (%0,%2,2),%0;"\
  TRANS_4x8(0,1,2,3,8,9,10,11) SAVE_4x8(0,1,2,3)

#define COPY_4x4 "movq %1,%4; addq $16,%1;"\
  "vmulps (%0),%%xmm15,%%xmm0; vmulps (%0,%2,1),%%xmm15,%%xmm1; leaq (%0,%2,2),%0;"\
  "vmulps (%0),%%xmm15,%%xmm2; vmulps (%0,%2,1),%%xmm15,%%xmm3; leaq (%0,%2,2),%0;"\
  TRANS_4x4(0,1,2,3,8,9,10,11) SAVE_4x4(0,1,2,3)

#define COPY_4x2 \
  "vmovsd (%0),%%xmm0; vmovhpd (%0,%2,1),%%xmm0,%%xmm0; vmulps %%xmm15,%%xmm0,%%xmm0; leaq (%0,%2,2),%0;"\
  "vmovsd (%0),%%xmm1; vmovhpd (%0,%2,1),%%xmm1,%%xmm1; vmulps %%xmm15,%%xmm1,%%xmm1; leaq (%0,%2,2),%0;"\
  "vpermilps $216,%%xmm0,%%xmm0; vpermilps $216,%%xmm1,%%xmm1; vunpcklpd %%xmm1,%%xmm0,%%xmm2; vunpckhpd %%xmm1,%%xmm0,%%xmm3;"\
  "vmovups %%xmm2,(%1); vmovups %%xmm3,(%1,%3,1); addq $16,%1;"

#define COPY_4x1 \
  "vmovss (%0),%%xmm0; vinsertps $16,(%0,%2,1),%%xmm0,%%xmm0; leaq (%0,%2,2),%0;"\
  "vinsertps $32,(%0),%%xmm0,%%xmm0; vinsertps $48,(%0,%2,1),%%xmm0,%%xmm0; leaq (%0,%2,2),%0;"\
  "vmulps %%xmm15,%%xmm0,%%xmm0; vmovups %%xmm0,(%1); addq $16,%1;"

#define SAVE_2x4(c1_no,c2_no,t1_no,t2_no) \
  "vunpcklps %%xmm"#c2_no",%%xmm"#c1_no",%%xmm"#t1_no"; vmulps %%xmm15,%%xmm"#t1_no",%%xmm"#t1_no";"\
  "vmovsd %%xmm"#t1_no",(%4); vmovhpd %%xmm"#t1_no",(%4,%3,1); leaq (%4,%3,2),%4;"\
  "vunpckhps %%xmm"#c2_no",%%xmm"#c1_no",%%xmm"#t2_no"; vmulps %%xmm15,%%xmm"#t2_no",%%xmm"#t2_no";"\
  "vmovsd %%xmm"#t2_no",(%4); vmovhpd %%xmm"#t2_no",(%4,%3,1); leaq (%4,%3,2),%4;"

#define COPY_2x16 "movq %1,%4; addq $8,%1;"\
  "vmovups (%0),%%ymm0; vmovups 32(%0),%%ymm2; vmovups (%0,%2,1),%%ymm1; vmovups 32(%0,%2,1),%%ymm3; leaq (%0,%2,2),%0;"\
  "vextractf128 $1,%%ymm0,%%xmm4; vextractf128 $1,%%ymm2,%%xmm6; vextractf128 $1,%%ymm1,%%xmm5; vextractf128 $1,%%ymm3,%%xmm7;"\
  SAVE_2x4(0,1,8,9) SAVE_2x4(4,5,8,9) SAVE_2x4(2,3,8,9) SAVE_2x4(6,7,8,9)

#define COPY_2x8 "movq %1,%4; addq $8,%1;"\
  "vmovups (%0),%%ymm0; vmovups (%0,%2,1),%%ymm1; leaq (%0,%2,2),%0;"\
  "vextractf128 $1,%%ymm0,%%xmm2; vextractf128 $1,%%ymm1,%%xmm3;"\
  SAVE_2x4(0,1,4,5) SAVE_2x4(2,3,4,5)

#define COPY_2x4 "movq %1,%4; addq $8,%1;"\
  "vmovups (%0),%%xmm0; vmovups (%0,%2,1),%%xmm1; leaq (%0,%2,2),%0;"\
  SAVE_2x4(0,1,4,5)

#define COPY_2x2 \
  "vmovsd (%0),%%xmm0; vmovhpd (%0,%2,1),%%xmm0,%%xmm0; vmulps %%xmm15,%%xmm0,%%xmm0; leaq (%0,%2,2),%0; vpermilps $216,%%xmm0,%%xmm0;"\
  "vmovsd %%xmm0,(%1); vmovhpd %%xmm0,(%1,%3,1); addq $8,%1;"

#define COPY_2x1 \
  "vmovss (%0),%%xmm0; vinsertps $16,(%0,%2,1),%%xmm0,%%xmm0; vmulps %%xmm15,%%xmm0,%%xmm0; leaq (%0,%2,2),%0; vmovsd %%xmm0,(%1); addq $8,%1;"

#define SAVE_1x4(c1_no)\
  "vmulps %%xmm15,%%xmm"#c1_no",%%xmm"#c1_no"; vmovss %%xmm"#c1_no",(%4); vextractps $1,%%xmm"#c1_no",(%4,%3,1); leaq (%4,%3,2),%4;"\
  "vextractps $2,%%xmm"#c1_no",(%4); vextractps $3,%%xmm"#c1_no",(%4,%3,1); leaq (%4,%3,2),%4;"

#define COPY_1x16 "movq %1,%4; addq $4,%1;"\
  "vmovups (%0),%%xmm1;" SAVE_1x4(1) "vmovups 16(%0),%%xmm2;" SAVE_1x4(2)\
  "vmovups 32(%0),%%xmm1;" SAVE_1x4(1) "vmovups 48(%0),%%xmm2;" SAVE_1x4(2) "addq %2,%0;"

#define COPY_1x8 "movq %1,%4; addq $4,%1;"\
  "vmovups (%0),%%xmm1;" SAVE_1x4(1) "vmovups 16(%0),%%xmm2;" SAVE_1x4(2) "addq %2,%0;"

#define COPY_1x4 "movq %1,%4; addq $4,%1; vmovups (%0),%%xmm1;" SAVE_1x4(1) "addq %2,%0;"

#define COPY_1x2 "vmovsd (%0),%%xmm1; addq %2,%0; vmulps %%xmm15,%%xmm1,%%xmm1; vmovss %%xmm1,(%1); vextractps $1,%%xmm1,(%1,%3,1); addq $4,%1;"

#define COPY_1x1 "vmulss (%0),%%xmm15,%%xmm1; vmovss %%xmm1,(%1); addq %2,%0; addq $4,%1;"

#define COMPUTE(ndim){\
  src = src_base; dst = dst_base;\
  __asm__ __volatile__(\
    "vbroadcastss %6,%%ymm15; movq %5,%%r11; cmpq $4,%%r11; jb "#ndim"32f;"\
    #ndim"31:\n\t"\
    COPY_4x##ndim "subq $4,%%r11; cmpq $4,%%r11; jnb "#ndim"31b;"\
    #ndim"32:\n\t"\
    "cmpq $2,%%r11; jb "#ndim"33f;"\
    COPY_2x##ndim "subq $2,%%r11;"\
    #ndim"33:\n\t"\
    "testq %%r11,%%r11; jz "#ndim"34f;"\
    COPY_1x##ndim "subq $1,%%r11;"\
    #ndim"34:\n\t"\
    :"+r"(src),"+r"(dst),"+r"(src_ld_bytes),"+r"(dst_ld_bytes),"+r"(dst_tmp):"m"(num_rows),"m"(ALPHA):"r11","cc","memory"\
    ,"xmm0","xmm1","xmm2","xmm3","xmm4","xmm5","xmm6","xmm7","xmm8","xmm9","xmm10","xmm11","xmm12","xmm13","xmm14","xmm15");\
}
int CNAME(BLASLONG rows, BLASLONG cols, FLOAT alpha, FLOAT *a, BLASLONG lda, FLOAT *b, BLASLONG ldb){
  float *src, *dst, *dst_tmp=0, *src_base, *dst_base;
  uint64_t src_ld_bytes = (uint64_t)lda * sizeof(float), dst_ld_bytes = (uint64_t)ldb * sizeof(float), num_rows = 0;
  BLASLONG cols_left, rows_done; float ALPHA = alpha;
  if(ALPHA==0.0){
    dst_base = b;
    for(cols_left=cols;cols_left>0;cols_left--) {memset(dst_base,0,rows*sizeof(float)); dst_base += ldb;}
    return 0;
  }
  for(rows_done=0;rows_done<rows;rows_done+=num_rows){
    num_rows = rows-rows_done;
    if(num_rows > ROWS_OF_BLOCK) num_rows = ROWS_OF_BLOCK;
    cols_left = cols; src_base = a + (int64_t)lda * (int64_t)rows_done; dst_base = b + rows_done;
    if(ldb%1024>3 && ldb%1024<1021) for(;cols_left>15;cols_left-=16){COMPUTE(16) src_base += 16; dst_base += 16 * ldb;}
    for(;cols_left>7;cols_left-=8){COMPUTE(8) src_base += 8; dst_base += 8 * ldb;}
    for(;cols_left>3;cols_left-=4){COMPUTE(4) src_base += 4; dst_base += 4 * ldb;}
    for(;cols_left>1;cols_left-=2){COMPUTE(2) src_base += 2; dst_base += 2 * ldb;}
    if(cols_left>0){COMPUTE(1) src_base ++; dst_base += ldb;}
  }
  return 0;
}

#else

int CNAME(BLASLONG rows, BLASLONG cols, FLOAT alpha, FLOAT *a, BLASLONG lda, FLOAT *b, BLASLONG ldb)
{
    BLASLONG i, j;
    FLOAT *a_offset, *a_offset1, *a_offset2, *a_offset3, *a_offset4;
    FLOAT *b_offset, *b_offset1, *b_offset2, *b_offset3, *b_offset4;

    if (rows <= 0)  return 0;
    if (cols <= 0)  return 0;

    a_offset = a;
    b_offset = b;

    i = (rows >> 2);
    if (i > 0) {
        do {
            a_offset1 = a_offset;
            a_offset2 = a_offset1 + lda;
            a_offset3 = a_offset2 + lda;
            a_offset4 = a_offset3 + lda;
            a_offset += 4 * lda;

            b_offset1 = b_offset;
            b_offset2 = b_offset1 + ldb;
            b_offset3 = b_offset2 + ldb;
            b_offset4 = b_offset3 + ldb;
            b_offset += 4;

            j = (cols >> 2);
            if (j > 0) {
                do {
                /* Column 1 of MAT_B */
                *(b_offset1 + 0) = *(a_offset1 + 0)*alpha; // Row 1 of MAT_A
                *(b_offset2 + 0) = *(a_offset1 + 1)*alpha;
                *(b_offset3 + 0) = *(a_offset1 + 2)*alpha;
                *(b_offset4 + 0) = *(a_offset1 + 3)*alpha;

                /* Column 2 of MAT_B */
                *(b_offset1 + 1) = *(a_offset2 + 0)*alpha; // Row 2 of MAT_A
                *(b_offset2 + 1) = *(a_offset2 + 1)*alpha;
                *(b_offset3 + 1) = *(a_offset2 + 2)*alpha;
                *(b_offset4 + 1) = *(a_offset2 + 3)*alpha;

                /* Column 3 of MAT_B */
                *(b_offset1 + 2) = *(a_offset3 + 0)*alpha; // Row 3 of MAT_A
                *(b_offset2 + 2) = *(a_offset3 + 1)*alpha;
                *(b_offset3 + 2) = *(a_offset3 + 2)*alpha;
                *(b_offset4 + 2) = *(a_offset3 + 3)*alpha;

                /* Column 4 of MAT_B */
                *(b_offset1 + 3) = *(a_offset4 + 0)*alpha; // Row 4 of MAT_A
                *(b_offset2 + 3) = *(a_offset4 + 1)*alpha;
                *(b_offset3 + 3) = *(a_offset4 + 2)*alpha;
                *(b_offset4 + 3) = *(a_offset4 + 3)*alpha;

                a_offset1 += 4;
                a_offset2 += 4;
                a_offset3 += 4;
                a_offset4 += 4;
                b_offset1 += ldb * 4;
                b_offset2 += ldb * 4;
                b_offset3 += ldb * 4;
                b_offset4 += ldb * 4;
                
                j--;
                } while (j > 0);
            } // if(j > 0)


            if (cols & 2) {
                *(b_offset1 + 0) = *(a_offset1 + 0)*alpha;
                *(b_offset2 + 0) = *(a_offset1 + 1)*alpha;

                *(b_offset1 + 1) = *(a_offset2 + 0)*alpha;
                *(b_offset2 + 1) = *(a_offset2 + 1)*alpha;

                *(b_offset1 + 2) = *(a_offset3 + 0)*alpha;
                *(b_offset2 + 2) = *(a_offset3 + 1)*alpha;

                *(b_offset1 + 3) = *(a_offset4 + 0)*alpha;
                *(b_offset2 + 3) = *(a_offset4 + 1)*alpha;

                a_offset1 += 2;
                a_offset2 += 2;
                a_offset3 += 2;
                a_offset4 += 2;

                b_offset1 += ldb*2;

            }

            if (cols & 1) {
                *(b_offset1 + 0) = *(a_offset1 + 0)*alpha;

                *(b_offset1 + 1) = *(a_offset2 + 0)*alpha;

                *(b_offset1 + 2) = *(a_offset3 + 0)*alpha;

                *(b_offset1 + 3) = *(a_offset4 + 0)*alpha;
            }

            i--;
        } while (i > 0);
    }


    if (rows & 2) {
        a_offset1 = a_offset;
        a_offset2 = a_offset1 + lda;
        a_offset += 2 * lda;

        b_offset1 = b_offset;
        b_offset2 = b_offset1 + ldb;
        b_offset3 = b_offset2 + ldb;
        b_offset4 = b_offset3 + ldb;
        b_offset += 2;

        j = (cols >> 2);
        if (j > 0){
            do {
                *(b_offset1 + 0) = *(a_offset1 + 0)*alpha;
                *(b_offset2 + 0) = *(a_offset1 + 1)*alpha;
                *(b_offset3 + 0) = *(a_offset1 + 2)*alpha;
                *(b_offset4 + 0) = *(a_offset1 + 3)*alpha;

                *(b_offset1 + 1) = *(a_offset2 + 0)*alpha;
                *(b_offset2 + 1) = *(a_offset2 + 1)*alpha;
                *(b_offset3 + 1) = *(a_offset2 + 2)*alpha;
                *(b_offset4 + 1) = *(a_offset2 + 3)*alpha;
                
                a_offset1 += 4;
                a_offset2 += 4;
                b_offset1 += ldb * 4;
                b_offset2 += ldb * 4;
                b_offset3 += ldb * 4;
                b_offset4 += ldb * 4;

                j--;
            } while (j > 0);
        }


        if (cols & 2){
            *(b_offset1 + 0) = *(a_offset1 + 0)*alpha;
            *(b_offset2 + 0) = *(a_offset1 + 1)*alpha;

            *(b_offset1 + 1) = *(a_offset2 + 0)*alpha;
            *(b_offset2 + 1) = *(a_offset2 + 1)*alpha;

            a_offset1 += 2;
            a_offset2 += 2;
            b_offset1 += ldb*2;

        }


        if (cols & 1){
            *(b_offset1 + 0) = *(a_offset1 + 0)*alpha;
            *(b_offset1 + 1) = *(a_offset2 + 0)*alpha;
        }
    } // if (rows & 2)


    if (rows & 1) {
        a_offset1 = a_offset;
        a_offset += lda;

        b_offset1 = b_offset;
        b_offset2 = b_offset1 + ldb;
        b_offset3 = b_offset2 + ldb;
        b_offset4 = b_offset3 + ldb;

        j = (cols >> 2);
        if (j > 0){
            do {
                *(b_offset1 + 0) = *(a_offset1 + 0)*alpha;
                *(b_offset2 + 0) = *(a_offset1 + 1)*alpha;
                *(b_offset3 + 0) = *(a_offset1 + 2)*alpha;
                *(b_offset4 + 0) = *(a_offset1 + 3)*alpha;
                
                a_offset1 += 4;
                b_offset1 += ldb * 4;
                b_offset2 += ldb * 4;
                b_offset3 += ldb * 4;
                b_offset4 += ldb * 4;
                
                j--;
            } while (j > 0);
        }

        if (cols & 2){
            *(b_offset1 + 0) = *(a_offset1 + 0)*alpha;
            *(b_offset2 + 0) = *(a_offset1 + 1)*alpha;

            a_offset1 += 2;
            b_offset1 += ldb * 2;
        }

        if (cols & 1){
            *(b_offset1 + 0) = *(a_offset1 + 0)*alpha;
        }
    }

    return 0;
}

#endif
