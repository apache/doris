#include "common.h"
#include <stdint.h>
#include "strsm_kernel_8x4_haswell_L_common.h"

#define SOLVE_LT_m1n4 \
  "movq %2,%3;" GEMM_SUM_REORDER_1x4(4)\
  SOLVE_m1n4(0,4) SAVE_b_m1n4(0,4)\
  "movq %2,%3; addq $4,%2;" save_c_m1n4(4)

#define SOLVE_LT_m1n8 \
  "movq %2,%3;" GEMM_SUM_REORDER_1x4(4) GEMM_SUM_REORDER_1x4(5)\
  SOLVE_m1n8(0,4,5) SAVE_b_m1n8(0,4,5)\
  "movq %2,%3; addq $4,%2;" save_c_m1n4(4) save_c_m1n4(5)

#define SOLVE_LT_m1n12 \
  "movq %2,%3;" GEMM_SUM_REORDER_1x4(4) GEMM_SUM_REORDER_1x4(5) GEMM_SUM_REORDER_1x4(6)\
  SOLVE_m1n12(0,4,5,6) SAVE_b_m1n12(0,4,5,6)\
  "movq %2,%3; addq $4,%2;" save_c_m1n4(4) save_c_m1n4(5) save_c_m1n4(6)

#define SOLVE_LT_m2n4 \
  "movq %2,%3;" GEMM_SUM_REORDER_2x4(4,5,4)\
  SOLVE_uplo_m2n4(0,4)\
  SOLVE_lo_m2n4(8,4) SAVE_b_m2n4(0,4)\
  "movq %2,%3; addq $8,%2;" save_c_m2n4(4)

#define SOLVE_LT_m2n8 \
  "movq %2,%3;" GEMM_SUM_REORDER_2x4(4,5,4) GEMM_SUM_REORDER_2x4(6,7,5)\
  SOLVE_uplo_m2n8(0,4,5)\
  SOLVE_lo_m2n8(8,4,5) SAVE_b_m2n8(0,4,5)\
  "movq %2,%3; addq $8,%2;" save_c_m2n4(4) save_c_m2n4(5)

#define SOLVE_LT_m2n12 \
  "movq %2,%3;" GEMM_SUM_REORDER_2x4(4,5,4) GEMM_SUM_REORDER_2x4(6,7,5) GEMM_SUM_REORDER_2x4(8,9,6)\
  SOLVE_uplo_m2n12(0,4,5,6)\
  SOLVE_lo_m2n12(8,4,5,6) SAVE_b_m2n12(0,4,5,6)\
  "movq %2,%3; addq $8,%2;" save_c_m2n4(4) save_c_m2n4(5) save_c_m2n4(6)

#define SOLVE_LT_m4n4 \
  "movq %2,%3;" GEMM_SUM_REORDER_4x4(4,5,6,7,4,5)\
\
  SOLVE_uplo_m2n4(0,4) SUBTRACT_m2n4(8,5)\
  SOLVE_lo_m2n4(16,4) SUBTRACT_m2n4(24,5) SAVE_b_m2n4(0,4)\
\
  SOLVE_uplo_m2n4(40,5)\
  SOLVE_lo_m2n4(56,5) SAVE_b_m2n4(32,5)\
\
  "movq %2,%3; addq $16,%2;" save_c_m4n4(4,5)

#define SOLVE_LT_m4n8 \
  "movq %2,%3;" GEMM_SUM_REORDER_4x4(4,5,6,7,4,5) GEMM_SUM_REORDER_4x4(8,9,10,11,6,7)\
\
  SOLVE_uplo_m2n8(0,4,6) SUBTRACT_m2n8(8,5,7)\
  SOLVE_lo_m2n8(16,4,6) SUBTRACT_m2n8(24,5,7) SAVE_b_m2n8(0,4,6)\
\
  SOLVE_uplo_m2n8(40,5,7)\
  SOLVE_lo_m2n8(56,5,7) SAVE_b_m2n8(32,5,7)\
\
  "movq %2,%3; addq $16,%2;" save_c_m4n4(4,5) save_c_m4n4(6,7)

#define SOLVE_LT_m4n12 \
  "movq %2,%3;" GEMM_SUM_REORDER_4x4(4,5,6,7,4,5) GEMM_SUM_REORDER_4x4(8,9,10,11,6,7) GEMM_SUM_REORDER_4x4(12,13,14,15,8,9)\
\
  SOLVE_uplo_m2n12(0,4,6,8) SUBTRACT_m2n12(8,5,7,9)\
  SOLVE_lo_m2n12(16,4,6,8) SUBTRACT_m2n12(24,5,7,9) SAVE_b_m2n12(0,4,6,8)\
\
  SOLVE_uplo_m2n12(40,5,7,9)\
  SOLVE_lo_m2n12(56,5,7,9) SAVE_b_m2n12(32,5,7,9)\
\
  "movq %2,%3; addq $16,%2;" save_c_m4n4(4,5) save_c_m4n4(6,7) save_c_m4n4(8,9)

#define SOLVE_LT_m8n4 \
  "movq %2,%3;" GEMM_SUM_REORDER_8x4(4,5,6,7,63)\
\
  SOLVE_uplo_m2n4(0,4) SUBTRACT_m2n4(8,5) SUBTRACT_m2n4(16,6) SUBTRACT_m2n4(24,7)\
  SOLVE_lo_m2n4(32,4) SUBTRACT_m2n4(40,5) SUBTRACT_m2n4(48,6) SUBTRACT_m2n4(56,7) SAVE_b_m2n4(0,4)\
\
  SOLVE_uplo_m2n4(72,5) SUBTRACT_m2n4(80,6) SUBTRACT_m2n4(88,7)\
  SOLVE_lo_m2n4(104,5) SUBTRACT_m2n4(112,6) SUBTRACT_m2n4(120,7) SAVE_b_m2n4(32,5)\
\
  SOLVE_uplo_m2n4(144,6) SUBTRACT_m2n4(152,7)\
  SOLVE_lo_m2n4(176,6) SUBTRACT_m2n4(184,7) SAVE_b_m2n4(64,6)\
\
  SOLVE_uplo_m2n4(216,7)\
  SOLVE_lo_m2n4(248,7) SAVE_b_m2n4(96,7)\
\
  "movq %2,%3; addq $32,%2;" save_c_m8n4(4,5,6,7)

#define SOLVE_LT_m8n8 \
  "movq %2,%3;" GEMM_SUM_REORDER_8x4(4,5,6,7,63) GEMM_SUM_REORDER_8x4(8,9,10,11,63)\
\
  SOLVE_uplo_m2n8(0,4,8) SUBTRACT_m2n8(8,5,9) SUBTRACT_m2n8(16,6,10) SUBTRACT_m2n8(24,7,11)\
  SOLVE_lo_m2n8(32,4,8) SUBTRACT_m2n8(40,5,9) SUBTRACT_m2n8(48,6,10) SUBTRACT_m2n8(56,7,11) SAVE_b_m2n8(0,4,8)\
\
  SOLVE_uplo_m2n8(72,5,9) SUBTRACT_m2n8(80,6,10) SUBTRACT_m2n8(88,7,11)\
  SOLVE_lo_m2n8(104,5,9) SUBTRACT_m2n8(112,6,10) SUBTRACT_m2n8(120,7,11) SAVE_b_m2n8(32,5,9)\
\
  SOLVE_uplo_m2n8(144,6,10) SUBTRACT_m2n8(152,7,11)\
  SOLVE_lo_m2n8(176,6,10) SUBTRACT_m2n8(184,7,11) SAVE_b_m2n8(64,6,10)\
\
  SOLVE_uplo_m2n8(216,7,11)\
  SOLVE_lo_m2n8(248,7,11) SAVE_b_m2n8(96,7,11)\
\
  "movq %2,%3; addq $32,%2;" save_c_m8n4(4,5,6,7) save_c_m8n4(8,9,10,11)

#define SOLVE_LT_m8n12 \
  "movq %2,%3;" GEMM_SUM_REORDER_8x4(4,5,6,7,63) GEMM_SUM_REORDER_8x4(8,9,10,11,63) GEMM_SUM_REORDER_8x4(12,13,14,15,63)\
\
  SOLVE_uplo_m2n12(0,4,8,12) SUBTRACT_m2n12(8,5,9,13) SUBTRACT_m2n12(16,6,10,14) SUBTRACT_m2n12(24,7,11,15)\
  SOLVE_lo_m2n12(32,4,8,12) SUBTRACT_m2n12(40,5,9,13) SUBTRACT_m2n12(48,6,10,14) SUBTRACT_m2n12(56,7,11,15) SAVE_b_m2n12(0,4,8,12)\
\
  SOLVE_uplo_m2n12(72,5,9,13) SUBTRACT_m2n12(80,6,10,14) SUBTRACT_m2n12(88,7,11,15)\
  SOLVE_lo_m2n12(104,5,9,13) SUBTRACT_m2n12(112,6,10,14) SUBTRACT_m2n12(120,7,11,15) SAVE_b_m2n12(32,5,9,13)\
\
  SOLVE_uplo_m2n12(144,6,10,14) SUBTRACT_m2n12(152,7,11,15)\
  SOLVE_lo_m2n12(176,6,10,14) SUBTRACT_m2n12(184,7,11,15) SAVE_b_m2n12(64,6,10,14)\
\
  SOLVE_uplo_m2n12(216,7,11,15)\
  SOLVE_lo_m2n12(248,7,11,15) SAVE_b_m2n12(96,7,11,15)\
\
  "movq %2,%3; addq $32,%2;" save_c_m8n4(4,5,6,7) save_c_m8n4(8,9,10,11) save_c_m8n4(12,13,14,15)

#define GEMM_LT_SIMPLE(mdim,ndim) \
  "movq %%r15,%0; leaq (%%r15,%%r12,"#mdim"),%%r15; movq %%r13,%5; addq $"#mdim",%%r13; movq %%r14,%1;" INIT_m##mdim##n##ndim\
  "testq %5,%5; jz 1"#mdim""#ndim"2f;"\
  "1"#mdim""#ndim"1:\n\t"\
  GEMM_KERNEL_k1m##mdim##n##ndim "addq $16,%1; addq $"#mdim"*4,%0; decq %5; jnz 1"#mdim""#ndim"1b;"\
  "1"#mdim""#ndim"2:\n\t"
#define GEMM_LT_m8n4 GEMM_LT_SIMPLE(8,4)
#define GEMM_LT_m8n8 GEMM_LT_SIMPLE(8,8)
#define GEMM_LT_m8n12 \
  "movq %%r15,%0; leaq (%%r15,%%r12,8),%%r15; movq %%r13,%5; addq $8,%%r13; movq %%r14,%1;" INIT_m8n12\
  "cmpq $8,%5; jb 18122f;"\
  "18121:\n\t"\
  GEMM_KERNEL_k1m8n12 "prefetcht0 384(%0); addq $32,%0; addq $16,%1;"\
  GEMM_KERNEL_k1m8n12 "addq $32,%0; addq $16,%1;"\
  GEMM_KERNEL_k1m8n12 "prefetcht0 384(%0); addq $32,%0; addq $16,%1;"\
  GEMM_KERNEL_k1m8n12 "addq $32,%0; addq $16,%1;"\
  GEMM_KERNEL_k1m8n12 "prefetcht0 384(%0); addq $32,%0; addq $16,%1;"\
  GEMM_KERNEL_k1m8n12 "addq $32,%0; addq $16,%1;"\
  GEMM_KERNEL_k1m8n12 "prefetcht0 384(%0); addq $32,%0; addq $16,%1;"\
  GEMM_KERNEL_k1m8n12 "addq $32,%0; addq $16,%1;"\
  "subq $8,%5; cmpq $8,%5; jnb 18121b;"\
  "18122:\n\t"\
  "testq %5,%5; jz 18124f;"\
  "18123:\n\t"\
  GEMM_KERNEL_k1m8n12 "addq $32,%0; addq $16,%1; decq %5; jnz 18123b;"\
  "18124:\n\t"
#define GEMM_LT_m4n4 GEMM_LT_SIMPLE(4,4)
#define GEMM_LT_m4n8 GEMM_LT_SIMPLE(4,8)
#define GEMM_LT_m4n12 GEMM_LT_SIMPLE(4,12)
#define GEMM_LT_m2n4 GEMM_LT_SIMPLE(2,4)
#define GEMM_LT_m2n8 GEMM_LT_SIMPLE(2,8)
#define GEMM_LT_m2n12 GEMM_LT_SIMPLE(2,12)
#define GEMM_LT_m1n4 GEMM_LT_SIMPLE(1,4)
#define GEMM_LT_m1n8 GEMM_LT_SIMPLE(1,8)
#define GEMM_LT_m1n12 GEMM_LT_SIMPLE(1,12)

#define COMPUTE(ndim) {\
  __asm__ __volatile__(\
    "movq %0,%%r15; movq %1,%%r14; movq %7,%%r13; movq %6,%%r12; salq $2,%%r12; movq %10,%%r11;"\
    "cmpq $8,%%r11; jb "#ndim"772f;"\
    #ndim"771:\n\t"\
    GEMM_LT_m8n##ndim SOLVE_LT_m8n##ndim "subq $8,%%r11; cmpq $8,%%r11; jnb "#ndim"771b;"\
    #ndim"772:\n\t"\
    "testq $4,%%r11; jz "#ndim"773f;"\
    GEMM_LT_m4n##ndim SOLVE_LT_m4n##ndim "subq $4,%%r11;"\
    #ndim"773:\n\t"\
    "testq $2,%%r11; jz "#ndim"774f;"\
    GEMM_LT_m2n##ndim SOLVE_LT_m2n##ndim "subq $2,%%r11;"\
    #ndim"774:\n\t"\
    "testq $1,%%r11; jz "#ndim"775f;"\
    GEMM_LT_m1n##ndim SOLVE_LT_m1n##ndim "subq $1,%%r11;"\
    #ndim"775:\n\t"\
    "movq %%r15,%0; movq %%r14,%1; vzeroupper;"\
  :"+r"(a_ptr),"+r"(b_ptr),"+r"(c_ptr),"+r"(c_tmp),"+r"(ldc_bytes),"+r"(k_cnt):"m"(K),"m"(OFF),"m"(one[0]),"m"(zero[0]),"m"(M)\
  :"r11","r12","r13","r14","r15","cc","memory",\
  "xmm0","xmm1","xmm2","xmm3","xmm4","xmm5","xmm6","xmm7","xmm8","xmm9","xmm10","xmm11","xmm12","xmm13","xmm14","xmm15");\
  a_ptr -= M * K; b_ptr += ndim * K; c_ptr += ldc * ndim - M;\
}
static void solve_LT(BLASLONG m, BLASLONG n, FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc) {
  FLOAT a0, b0;
  int i, j, k;
  for (i=0;i<m;i++) {
    a0 = a[i*m+i];
    for (j=0;j<n;j++) {
      b0 = c[j*ldc+i] * a0;
      b[i*n+j] = c[j*ldc+i] = b0;
      for (k=i+1;k<m;k++) c[j*ldc+k] -= b0 * a[i*m+k];
    }
  }
}
static void COMPUTE_EDGE_1_nchunk(BLASLONG m, BLASLONG n, FLOAT *sa, FLOAT *sb, FLOAT *C, BLASLONG ldc, BLASLONG k, BLASLONG offset) {
  BLASLONG m_count = m, kk = offset; FLOAT *a_ptr = sa, *c_ptr = C;
  for(;m_count>7;m_count-=8){
    if(kk>0) GEMM_KERNEL_N(8,n,kk,-1.0,a_ptr,sb,c_ptr,ldc);
    solve_LT(8,n,a_ptr+kk*8,sb+kk*n,c_ptr,ldc);
    kk += 8; a_ptr += k * 8; c_ptr += 8;
  }
  for(;m_count>3;m_count-=4){
    if(kk>0) GEMM_KERNEL_N(4,n,kk,-1.0,a_ptr,sb,c_ptr,ldc);
    solve_LT(4,n,a_ptr+kk*4,sb+kk*n,c_ptr,ldc);
    kk += 4; a_ptr += k * 4; c_ptr += 4;
  }
  for(;m_count>1;m_count-=2){
    if(kk>0) GEMM_KERNEL_N(2,n,kk,-1.0,a_ptr,sb,c_ptr,ldc);
    solve_LT(2,n,a_ptr+kk*2,sb+kk*n,c_ptr,ldc);
    kk += 2; a_ptr += k * 2; c_ptr += 2;
  }
  if(m_count>0){
    if(kk>0) GEMM_KERNEL_N(1,n,kk,-1.0,a_ptr,sb,c_ptr,ldc);
    solve_LT(1,n,a_ptr+kk*1,sb+kk*n,c_ptr,ldc);
    kk += 1; a_ptr += k * 1; c_ptr += 1;
  }
}
int CNAME(BLASLONG m, BLASLONG n, BLASLONG k, FLOAT dummy1, FLOAT *sa, FLOAT *sb, FLOAT *C, BLASLONG ldc, BLASLONG offset){
  float *a_ptr = sa, *b_ptr = sb, *c_ptr = C, *c_tmp = C;
  float one[8] = {1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0};
  float zero[8] = {0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0};
  uint64_t ldc_bytes = (uint64_t)ldc * sizeof(float), K = (uint64_t)k, M = (uint64_t)m, OFF = (uint64_t)offset, k_cnt = 0;
  BLASLONG n_count = n;
  for(;n_count>11;n_count-=12) COMPUTE(12)
  for(;n_count>7;n_count-=8) COMPUTE(8)
  for(;n_count>3;n_count-=4) COMPUTE(4)
  for(;n_count>1;n_count-=2) { COMPUTE_EDGE_1_nchunk(m,2,a_ptr,b_ptr,c_ptr,ldc,k,offset); b_ptr += 2*k; c_ptr += ldc*2;}
  if(n_count>0) COMPUTE_EDGE_1_nchunk(m,1,a_ptr,b_ptr,c_ptr,ldc,k,offset);
  return 0;
}

