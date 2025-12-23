#include <stdio.h>
#include "common.h"
#include <immintrin.h>

int CNAME(BLASLONG dim_second, BLASLONG dim_first, double *src, BLASLONG lead_dim, double *dst){
  double *src1, *src2, *src3, *src4, *dst1;
  __m512d z1,z2,z3,z4,z5,z6,z7,z8; __m256d y1,y2,y3,y4; __m128d x1,x2,x3,x4; double s1,s2,s3,s4;
  BLASLONG dim1_count, dim2_count, src_inc;
  src_inc = 4 * lead_dim - dim_first;
  src1 = src; src2 = src + lead_dim; src3 = src2 + lead_dim; src4 = src3 + lead_dim;
  for(dim2_count=dim_second; dim2_count>3; dim2_count-=4){
    dst1 = dst + 16 * (dim_second - dim2_count);
    for(dim1_count=dim_first; dim1_count>15; dim1_count-=16){
      z1 = _mm512_loadu_pd(src1); z2 = _mm512_loadu_pd(src1+8); src1 += 16;
      z3 = _mm512_loadu_pd(src2); z4 = _mm512_loadu_pd(src2+8); src2 += 16;
      z5 = _mm512_loadu_pd(src3); z6 = _mm512_loadu_pd(src3+8); src3 += 16;
      z7 = _mm512_loadu_pd(src4); z8 = _mm512_loadu_pd(src4+8); src4 += 16;
      _mm512_storeu_pd(dst1+ 0,z1); _mm512_storeu_pd(dst1+ 8,z2);
      _mm512_storeu_pd(dst1+16,z3); _mm512_storeu_pd(dst1+24,z4);
      _mm512_storeu_pd(dst1+32,z5); _mm512_storeu_pd(dst1+40,z6);
      _mm512_storeu_pd(dst1+48,z7); _mm512_storeu_pd(dst1+56,z8); dst1 += 16 * dim_second;
    }
    dst1 -= 8 * (dim_second - dim2_count);
    if(dim1_count>7){
      z1 = _mm512_loadu_pd(src1); src1 += 8;
      z2 = _mm512_loadu_pd(src2); src2 += 8;
      z3 = _mm512_loadu_pd(src3); src3 += 8;
      z4 = _mm512_loadu_pd(src4); src4 += 8;
      _mm512_storeu_pd(dst1+ 0,z1); _mm512_storeu_pd(dst1+ 8,z2);
      _mm512_storeu_pd(dst1+16,z3); _mm512_storeu_pd(dst1+24,z4); dst1 += 8 * dim_second;
      dim1_count -= 8;
    }
    dst1 -= 4 * (dim_second - dim2_count);
    if(dim1_count>3){
      y1 = _mm256_loadu_pd(src1); src1 += 4;
      y2 = _mm256_loadu_pd(src2); src2 += 4;
      y3 = _mm256_loadu_pd(src3); src3 += 4;
      y4 = _mm256_loadu_pd(src4); src4 += 4;
      _mm256_storeu_pd(dst1+ 0,y1); _mm256_storeu_pd(dst1+ 4,y2);
      _mm256_storeu_pd(dst1+ 8,y3); _mm256_storeu_pd(dst1+12,y4); dst1 += 4 * dim_second;
      dim1_count -= 4;
    }
    dst1 -= 2 * (dim_second - dim2_count);
    if(dim1_count>1){
      x1 = _mm_loadu_pd(src1); src1 += 2;
      x2 = _mm_loadu_pd(src2); src2 += 2;
      x3 = _mm_loadu_pd(src3); src3 += 2;
      x4 = _mm_loadu_pd(src4); src4 += 2;
      _mm_storeu_pd(dst1+0,x1); _mm_storeu_pd(dst1+2,x2);
      _mm_storeu_pd(dst1+4,x3); _mm_storeu_pd(dst1+6,x4); dst1 += 2 * dim_second;
      dim1_count -= 2;
    }
    dst1 -= dim_second - dim2_count;
    if(dim1_count>0){
      s1 = *src1; src1++; s2 = *src2; src2++; s3 = *src3; src3++; s4 = *src4; src4++;
      dst1[0] = s1; dst1[1] = s2; dst1[2] = s3; dst1[3] = s4;
    }
    src1 += src_inc; src2 += src_inc; src3 += src_inc; src4 += src_inc;
  }
  src_inc -= 2 * lead_dim;
  for(; dim2_count>1; dim2_count-=2){
    dst1 = dst + 16 * (dim_second - dim2_count);
    for(dim1_count=dim_first; dim1_count>15; dim1_count-=16){
      z1 = _mm512_loadu_pd(src1); z2 = _mm512_loadu_pd(src1+8); src1 += 16;
      z3 = _mm512_loadu_pd(src2); z4 = _mm512_loadu_pd(src2+8); src2 += 16;
      _mm512_storeu_pd(dst1+ 0,z1); _mm512_storeu_pd(dst1+ 8,z2);
      _mm512_storeu_pd(dst1+16,z3); _mm512_storeu_pd(dst1+24,z4); dst1 += 16 * dim_second;
    }
    dst1 -= 8 * (dim_second - dim2_count);
    if(dim1_count>7){
      z1 = _mm512_loadu_pd(src1); src1 += 8;
      z2 = _mm512_loadu_pd(src2); src2 += 8;
      _mm512_storeu_pd(dst1+ 0,z1); _mm512_storeu_pd(dst1+ 8,z2); dst1 += 8 * dim_second;
      dim1_count -= 8;
    }
    dst1 -= 4 * (dim_second - dim2_count);
    if(dim1_count>3){
      y1 = _mm256_loadu_pd(src1); src1 += 4;
      y2 = _mm256_loadu_pd(src2); src2 += 4;
      _mm256_storeu_pd(dst1+ 0,y1); _mm256_storeu_pd(dst1+ 4,y2); dst1 += 4 * dim_second;
      dim1_count -= 4;
    }
    dst1 -= 2 * (dim_second - dim2_count);
    if(dim1_count>1){
      x1 = _mm_loadu_pd(src1); src1 += 2;
      x2 = _mm_loadu_pd(src2); src2 += 2;
      _mm_storeu_pd(dst1+0,x1); _mm_storeu_pd(dst1+2,x2); dst1 += 2 * dim_second;
      dim1_count -= 2;
    }
    dst1 -= dim_second - dim2_count;
    if(dim1_count>0){
      s1 = *src1; src1++; s2 = *src2; src2++;
      dst1[0] = s1; dst1[1] = s2;
    }
    src1 += src_inc; src2 += src_inc;
  }
  src_inc -= lead_dim;
  for(; dim2_count>0; dim2_count--){
    dst1 = dst + 16 * (dim_second - dim2_count);
    for(dim1_count=dim_first; dim1_count>15; dim1_count-=16){
      z1 = _mm512_loadu_pd(src1); z2 = _mm512_loadu_pd(src1+8); src1 += 16;
      _mm512_storeu_pd(dst1+ 0,z1); _mm512_storeu_pd(dst1+ 8,z2); dst1 += 16 * dim_second;
    }
    dst1 -= 8 * (dim_second - dim2_count);
    if(dim1_count>7){
      z1 = _mm512_loadu_pd(src1); src1 += 8;
      _mm512_storeu_pd(dst1+ 0,z1); dst1 += 8 * dim_second;
      dim1_count -= 8;
    }
    dst1 -= 4 * (dim_second - dim2_count);
    if(dim1_count>3){
      y1 = _mm256_loadu_pd(src1); src1 += 4;
      _mm256_storeu_pd(dst1+ 0,y1); dst1 += 4 * dim_second;
      dim1_count -= 4;
    }
    dst1 -= 2 * (dim_second - dim2_count);
    if(dim1_count>1){
      x1 = _mm_loadu_pd(src1); src1 += 2;
      _mm_storeu_pd(dst1+0,x1); dst1 += 2 * dim_second;
      dim1_count -= 2;
    }
    dst1 -= dim_second - dim2_count;
    if(dim1_count>0){
      s1 = *src1; src1++;
      dst1[0] = s1;
    }
    src1 += src_inc;
  }
  return 0;
}
