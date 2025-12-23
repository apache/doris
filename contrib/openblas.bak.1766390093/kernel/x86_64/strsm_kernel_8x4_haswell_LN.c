#include "common.h"
#include <stdint.h>
#include "strsm_kernel_8x4_haswell_L_common.h"

#define SOLVE_LN_m1n4 \
  "subq $4,%2; movq %2,%3;" GEMM_SUM_REORDER_1x4(4)\
  SOLVE_m1n4(-4,4) SAVE_b_m1n4(-16,4)\
  "movq %2,%3;" save_c_m1n4(4)

#define SOLVE_LN_m1n8 \
  "subq $4,%2; movq %2,%3;" GEMM_SUM_REORDER_1x4(4) GEMM_SUM_REORDER_1x4(5)\
  SOLVE_m1n8(-4,4,5) SAVE_b_m1n8(-16,4,5)\
  "movq %2,%3;" save_c_m1n4(4) save_c_m1n4(5)

#define SOLVE_LN_m1n12 \
  "subq $4,%2; movq %2,%3;" GEMM_SUM_REORDER_1x4(4) GEMM_SUM_REORDER_1x4(5) GEMM_SUM_REORDER_1x4(6)\
  SOLVE_m1n12(-4,4,5,6) SAVE_b_m1n12(-16,4,5,6)\
  "movq %2,%3;" save_c_m1n4(4) save_c_m1n4(5) save_c_m1n4(6)

#define SOLVE_LN_m2n4 \
  "subq $8,%2; movq %2,%3;" GEMM_SUM_REORDER_2x4(4,5,4)\
  SOLVE_loup_m2n4(-8,4)\
  SOLVE_up_m2n4(-16,4) SAVE_b_m2n4(-32,4)\
  "movq %2,%3;" save_c_m2n4(4)

#define SOLVE_LN_m2n8 \
  "subq $8,%2; movq %2,%3;" GEMM_SUM_REORDER_2x4(4,5,4) GEMM_SUM_REORDER_2x4(6,7,5)\
  SOLVE_loup_m2n8(-8,4,5)\
  SOLVE_up_m2n8(-16,4,5) SAVE_b_m2n8(-32,4,5)\
  "movq %2,%3;" save_c_m2n4(4) save_c_m2n4(5)

#define SOLVE_LN_m2n12 \
  "subq $8,%2; movq %2,%3;" GEMM_SUM_REORDER_2x4(4,5,4) GEMM_SUM_REORDER_2x4(6,7,5) GEMM_SUM_REORDER_2x4(8,9,6)\
  SOLVE_loup_m2n12(-8,4,5,6)\
  SOLVE_up_m2n12(-16,4,5,6) SAVE_b_m2n12(-32,4,5,6)\
  "movq %2,%3;" save_c_m2n4(4) save_c_m2n4(5) save_c_m2n4(6)

#define SOLVE_LN_m4n4 \
  "subq $16,%2; movq %2,%3;" GEMM_SUM_REORDER_4x4(4,5,6,7,4,5)\
\
  SOLVE_loup_m2n4(-8,5) SUBTRACT_m2n4(-16,4)\
  SOLVE_up_m2n4(-24,5) SUBTRACT_m2n4(-32,4) SAVE_b_m2n4(-32,5)\
\
  SOLVE_loup_m2n4(-48,4)\
  SOLVE_up_m2n4(-64,4) SAVE_b_m2n4(-64,4)\
\
  "movq %2,%3;" save_c_m4n4(4,5)

#define SOLVE_LN_m4n8 \
  "subq $16,%2; movq %2,%3;" GEMM_SUM_REORDER_4x4(4,5,6,7,4,5) GEMM_SUM_REORDER_4x4(8,9,10,11,6,7)\
\
  SOLVE_loup_m2n8(-8,5,7) SUBTRACT_m2n8(-16,4,6)\
  SOLVE_up_m2n8(-24,5,7) SUBTRACT_m2n8(-32,4,6) SAVE_b_m2n8(-32,5,7)\
\
  SOLVE_loup_m2n8(-48,4,6)\
  SOLVE_up_m2n8(-64,4,6) SAVE_b_m2n8(-64,4,6)\
\
  "movq %2,%3;" save_c_m4n4(4,5) save_c_m4n4(6,7)

#define SOLVE_LN_m4n12 \
  "subq $16,%2; movq %2,%3;" GEMM_SUM_REORDER_4x4(4,5,6,7,4,5) GEMM_SUM_REORDER_4x4(8,9,10,11,6,7) GEMM_SUM_REORDER_4x4(12,13,14,15,8,9)\
\
  SOLVE_loup_m2n12(-8,5,7,9) SUBTRACT_m2n12(-16,4,6,8)\
  SOLVE_up_m2n12(-24,5,7,9) SUBTRACT_m2n12(-32,4,6,8) SAVE_b_m2n12(-32,5,7,9)\
\
  SOLVE_loup_m2n12(-48,4,6,8)\
  SOLVE_up_m2n12(-64,4,6,8) SAVE_b_m2n12(-64,4,6,8)\
\
  "movq %2,%3;" save_c_m4n4(4,5) save_c_m4n4(6,7) save_c_m4n4(8,9)

#define SOLVE_LN_m8n4 \
  "subq $32,%2; movq %2,%3;" GEMM_SUM_REORDER_8x4(4,5,6,7,-32)\
\
  SOLVE_loup_m2n4(-8,7) SUBTRACT_m2n4(-16,6) SUBTRACT_m2n4(-24,5) SUBTRACT_m2n4(-32,4)\
  SOLVE_up_m2n4(-40,7) SUBTRACT_m2n4(-48,6) SUBTRACT_m2n4(-56,5) SUBTRACT_m2n4(-64,4) SAVE_b_m2n4(-32,7)\
\
  SOLVE_loup_m2n4(-80,6) SUBTRACT_m2n4(-88,5) SUBTRACT_m2n4(-96,4)\
  SOLVE_up_m2n4(-112,6) SUBTRACT_m2n4(-120,5) SUBTRACT_m2n4(-128,4) SAVE_b_m2n4(-64,6)\
\
  SOLVE_loup_m2n4(-152,5) SUBTRACT_m2n4(-160,4)\
  SOLVE_up_m2n4(-184,5) SUBTRACT_m2n4(-192,4) SAVE_b_m2n4(-96,5)\
\
  SOLVE_loup_m2n4(-224,4)\
  SOLVE_up_m2n4(-256,4) SAVE_b_m2n4(-128,4)\
\
  "movq %2,%3;" save_c_m8n4(4,5,6,7)

#define SOLVE_LN_m8n8 \
  "subq $32,%2; movq %2,%3;" GEMM_SUM_REORDER_8x4(4,5,6,7,-32) GEMM_SUM_REORDER_8x4(8,9,10,11,-32)\
\
  SOLVE_loup_m2n8(-8,7,11) SUBTRACT_m2n8(-16,6,10) SUBTRACT_m2n8(-24,5,9) SUBTRACT_m2n8(-32,4,8)\
  SOLVE_up_m2n8(-40,7,11) SUBTRACT_m2n8(-48,6,10) SUBTRACT_m2n8(-56,5,9) SUBTRACT_m2n8(-64,4,8) SAVE_b_m2n8(-32,7,11)\
\
  SOLVE_loup_m2n8(-80,6,10) SUBTRACT_m2n8(-88,5,9) SUBTRACT_m2n8(-96,4,8)\
  SOLVE_up_m2n8(-112,6,10) SUBTRACT_m2n8(-120,5,9) SUBTRACT_m2n8(-128,4,8) SAVE_b_m2n8(-64,6,10)\
\
  SOLVE_loup_m2n8(-152,5,9) SUBTRACT_m2n8(-160,4,8)\
  SOLVE_up_m2n8(-184,5,9) SUBTRACT_m2n8(-192,4,8) SAVE_b_m2n8(-96,5,9)\
\
  SOLVE_loup_m2n8(-224,4,8)\
  SOLVE_up_m2n8(-256,4,8) SAVE_b_m2n8(-128,4,8)\
\
  "movq %2,%3;" save_c_m8n4(4,5,6,7) save_c_m8n4(8,9,10,11)

#define SOLVE_LN_m8n12 \
  "subq $32,%2; movq %2,%3;" GEMM_SUM_REORDER_8x4(4,5,6,7,-32) GEMM_SUM_REORDER_8x4(8,9,10,11,-32) GEMM_SUM_REORDER_8x4(12,13,14,15,-32)\
\
  SOLVE_loup_m2n12(-8,7,11,15) SUBTRACT_m2n12(-16,6,10,14) SUBTRACT_m2n12(-24,5,9,13) SUBTRACT_m2n12(-32,4,8,12)\
  SOLVE_up_m2n12(-40,7,11,15) SUBTRACT_m2n12(-48,6,10,14) SUBTRACT_m2n12(-56,5,9,13) SUBTRACT_m2n12(-64,4,8,12) SAVE_b_m2n12(-32,7,11,15)\
\
  SOLVE_loup_m2n12(-80,6,10,14) SUBTRACT_m2n12(-88,5,9,13) SUBTRACT_m2n12(-96,4,8,12)\
  SOLVE_up_m2n12(-112,6,10,14) SUBTRACT_m2n12(-120,5,9,13) SUBTRACT_m2n12(-128,4,8,12) SAVE_b_m2n12(-64,6,10,14)\
\
  SOLVE_loup_m2n12(-152,5,9,13) SUBTRACT_m2n12(-160,4,8,12)\
  SOLVE_up_m2n12(-184,5,9,13) SUBTRACT_m2n12(-192,4,8,12) SAVE_b_m2n12(-96,5,9,13)\
\
  SOLVE_loup_m2n12(-224,4,8,12)\
  SOLVE_up_m2n12(-256,4,8,12) SAVE_b_m2n12(-128,4,8,12)\
\
  "movq %2,%3;" save_c_m8n4(4,5,6,7) save_c_m8n4(8,9,10,11) save_c_m8n4(12,13,14,15)

/* r13 = k-kk, r14 = b_tail, r15 = a_tail */

#define GEMM_LN_SIMPLE(mdim,ndim) \
  "movq %%r15,%0; negq %%r12; leaq (%%r15,%%r12,"#mdim"),%%r15; negq %%r12;"\
  "movq %%r13,%5; addq $"#mdim",%%r13; movq %%r14,%1;" INIT_m##mdim##n##ndim\
  "testq %5,%5; jz 2"#mdim""#ndim"2f;"\
  "2"#mdim""#ndim"1:\n\t"\
  "subq $16,%1; subq $"#mdim"*4,%0;" GEMM_KERNEL_k1m##mdim##n##ndim "decq %5; jnz 2"#mdim""#ndim"1b;"\
  "2"#mdim""#ndim"2:\n\t"
#define GEMM_LN_m8n4 GEMM_LN_SIMPLE(8,4)
#define GEMM_LN_m8n8 GEMM_LN_SIMPLE(8,8)
#define GEMM_LN_m8n12 \
  "movq %%r15,%0; negq %%r12; leaq (%%r15,%%r12,8),%%r15; negq %%r12; movq %%r13,%5; addq $8,%%r13; movq %%r14,%1;" INIT_m8n12\
  "cmpq $8,%5; jb 28122f;"\
  "28121:\n\t"\
  "prefetcht0 -384(%0); subq $32,%0; subq $16,%1;" GEMM_KERNEL_k1m8n12\
                       "subq $32,%0; subq $16,%1;" GEMM_KERNEL_k1m8n12\
  "prefetcht0 -384(%0); subq $32,%0; subq $16,%1;" GEMM_KERNEL_k1m8n12\
                       "subq $32,%0; subq $16,%1;" GEMM_KERNEL_k1m8n12\
  "prefetcht0 -384(%0); subq $32,%0; subq $16,%1;" GEMM_KERNEL_k1m8n12\
                       "subq $32,%0; subq $16,%1;" GEMM_KERNEL_k1m8n12\
  "prefetcht0 -384(%0); subq $32,%0; subq $16,%1;" GEMM_KERNEL_k1m8n12\
                       "subq $32,%0; subq $16,%1;" GEMM_KERNEL_k1m8n12\
  "subq $8,%5; cmpq $8,%5; jnb 28121b;"\
  "28122:\n\t"\
  "testq %5,%5; jz 28124f;"\
  "28123:\n\t"\
  "subq $32,%0; subq $16,%1;" GEMM_KERNEL_k1m8n12 "decq %5; jnz 28123b;"\
  "28124:\n\t"
#define GEMM_LN_m4n4 GEMM_LN_SIMPLE(4,4)
#define GEMM_LN_m4n8 GEMM_LN_SIMPLE(4,8)
#define GEMM_LN_m4n12 GEMM_LN_SIMPLE(4,12)
#define GEMM_LN_m2n4 GEMM_LN_SIMPLE(2,4)
#define GEMM_LN_m2n8 GEMM_LN_SIMPLE(2,8)
#define GEMM_LN_m2n12 GEMM_LN_SIMPLE(2,12)
#define GEMM_LN_m1n4 GEMM_LN_SIMPLE(1,4)
#define GEMM_LN_m1n8 GEMM_LN_SIMPLE(1,8)
#define GEMM_LN_m1n12 GEMM_LN_SIMPLE(1,12)

#define COMPUTE(ndim) {\
  c_ptr += M;\
  __asm__ __volatile__(\
    "movq %0,%%r15; movq %7,%%r13; movq %6,%%r12; salq $2,%%r12; leaq (%1,%%r12,4),%%r14; movq %10,%%r11;"\
    "testq $1,%%r11; jz "#ndim"772f;"\
    #ndim"771:\n\t"\
    GEMM_LN_m1n##ndim SOLVE_LN_m1n##ndim "subq $1,%%r11;"\
    #ndim"772:\n\t"\
    "testq $2,%%r11; jz "#ndim"773f;"\
    GEMM_LN_m2n##ndim SOLVE_LN_m2n##ndim "subq $2,%%r11;"\
    #ndim"773:\n\t"\
    "testq $4,%%r11; jz "#ndim"774f;"\
    GEMM_LN_m4n##ndim SOLVE_LN_m4n##ndim "subq $4,%%r11;"\
    #ndim"774:\n\t"\
    "testq %%r11,%%r11; jz "#ndim"776f;"\
    #ndim"775:\n\t"\
    GEMM_LN_m8n##ndim SOLVE_LN_m8n##ndim "subq $8,%%r11; jnz "#ndim"775b;"\
    #ndim"776:\n\t"\
    "movq %%r15,%0; movq %%r14,%1; vzeroupper;"\
  :"+r"(a_ptr),"+r"(b_ptr),"+r"(c_ptr),"+r"(c_tmp),"+r"(ldc_bytes),"+r"(k_cnt):"m"(K),"m"(kmkkinp),"m"(one[0]),"m"(zero[0]),"m"(M)\
  :"r11","r12","r13","r14","r15","cc","memory",\
  "xmm0","xmm1","xmm2","xmm3","xmm4","xmm5","xmm6","xmm7","xmm8","xmm9","xmm10","xmm11","xmm12","xmm13","xmm14","xmm15");\
  a_ptr += M * K; b_ptr += (ndim-4) * K; c_ptr += ldc * ndim;\
}
static void solve_LN(BLASLONG m, BLASLONG n, FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc) {
  FLOAT a0, b0;
  int i, j, k;
  for (i=m-1;i>=0;i--) {
    a0 = a[i*m+i]; //reciprocal of the original value
    for (j=0;j<n;j++) {
      b0 = c[j*ldc+i]*a0;
      c[j*ldc+i] = b[i*n+j] = b0;
      for (k=0;k<i;k++) c[j*ldc+k] -= b0*a[i*m+k];
    }
  }
}
static void COMPUTE_EDGE_1_nchunk(BLASLONG m, BLASLONG n, FLOAT *sa, FLOAT *sb, FLOAT *C, BLASLONG ldc, BLASLONG k, BLASLONG offset) {
  BLASLONG m_count = m, kk = m+offset; FLOAT *a_ptr = sa+m*k, *c_ptr = C+m;
  if(m_count&1){
    a_ptr-=k; c_ptr--;
    if(k-kk>0) GEMM_KERNEL_N(1,n,k-kk,-1.0,a_ptr+kk*1,sb+kk*n,c_ptr,ldc);
    solve_LN(1,n,a_ptr+(kk-1)*1,sb+(kk-1)*n,c_ptr,ldc);
    kk -= 1;
    m_count--;
  }
  if(m_count&2){
    a_ptr-=k*2; c_ptr-=2;
    if(k-kk>0) GEMM_KERNEL_N(2,n,k-kk,-1.0,a_ptr+kk*2,sb+kk*n,c_ptr,ldc);
    solve_LN(2,n,a_ptr+(kk-2)*2,sb+(kk-2)*n,c_ptr,ldc);
    kk -= 2;
    m_count-=2;
  }
  if(m_count&4){
    a_ptr-=k*4; c_ptr-=4;
    if(k-kk>0) GEMM_KERNEL_N(4,n,k-kk,-1.0,a_ptr+kk*4,sb+kk*n,c_ptr,ldc);
    solve_LN(4,n,a_ptr+(kk-4)*4,sb+(kk-4)*n,c_ptr,ldc);
    kk -= 4;
    m_count-=4;
  }
  for(;m_count>7;m_count-=8){
    a_ptr-=k*8; c_ptr-=8;
    if(k-kk>0) GEMM_KERNEL_N(8,n,k-kk,-1.0,a_ptr+kk*8,sb+kk*n,c_ptr,ldc);
    solve_LN(8,n,a_ptr+(kk-8)*8,sb+(kk-8)*n,c_ptr,ldc);
    kk -= 8;
  }
}
int CNAME(BLASLONG m, BLASLONG n, BLASLONG k, FLOAT dummy1, FLOAT *sa, FLOAT *sb, FLOAT *C, BLASLONG ldc, BLASLONG offset){
  float *a_ptr = sa+m*k, *b_ptr = sb, *c_ptr = C, *c_tmp = C;
  float one[8] = {1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0};
  float zero[8] = {0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0};
  uint64_t ldc_bytes = (uint64_t)ldc * sizeof(float), K = (uint64_t)k, M = (uint64_t)m, kmkkinp = (uint64_t)(k-m-offset), k_cnt = 0;
  BLASLONG n_count = n;
  for(;n_count>11;n_count-=12) COMPUTE(12)
  for(;n_count>7;n_count-=8) COMPUTE(8)
  for(;n_count>3;n_count-=4) COMPUTE(4)
  for(;n_count>1;n_count-=2) { COMPUTE_EDGE_1_nchunk(m,2,sa,b_ptr,c_ptr,ldc,k,offset); b_ptr += 2*k; c_ptr += ldc*2;}
  if(n_count>0) COMPUTE_EDGE_1_nchunk(m,1,sa,b_ptr,c_ptr,ldc,k,offset);
  return 0;
}

