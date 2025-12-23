#include "common.h"
#include <stdint.h>
#include "strsm_kernel_8x4_haswell_R_common.h"

#define SOLVE_RN_m8n4 \
  "movq %2,%3;" GEMM_SUM_REORDER_8x4(4,5,6,7,63) "movq %2,%3; addq $32,%2;"\
  SOLVE_leri_m8n2(0,4,5,%1) SUBTRACT_m8n2(8,6,7,%1)\
  SOLVE_ri_m8n2(16,4,5,%1) SUBTRACT_m8n2(24,6,7,%1)\
  SAVE_SOLUTION_m8n2(4,5,0)\
  SOLVE_leri_m8n2(40,6,7,%1)\
  SOLVE_ri_m8n2(56,6,7,%1)\
  SAVE_SOLUTION_m8n2(6,7,64)

#define SOLVE_RN_m8n8 \
  "movq %2,%3;" GEMM_SUM_REORDER_8x4(4,5,6,7,63) GEMM_SUM_REORDER_8x4(8,9,10,11,63) "movq %2,%3; addq $32,%2;"\
  SOLVE_leri_m8n2(0,4,5,%1) SUBTRACT_m8n2(8,6,7,%1) SUBTRACT_m8n2(0,8,9,%1,%%r12,4) SUBTRACT_m8n2(8,10,11,%1,%%r12,4)\
  SOLVE_ri_m8n2(16,4,5,%1) SUBTRACT_m8n2(24,6,7,%1) SUBTRACT_m8n2(16,8,9,%1,%%r12,4) SUBTRACT_m8n2(24,10,11,%1,%%r12,4)\
  SAVE_SOLUTION_m8n2(4,5,0)\
  SOLVE_leri_m8n2(40,6,7,%1) SUBTRACT_m8n2(32,8,9,%1,%%r12,4) SUBTRACT_m8n2(40,10,11,%1,%%r12,4)\
  SOLVE_ri_m8n2(56,6,7,%1) SUBTRACT_m8n2(48,8,9,%1,%%r12,4) SUBTRACT_m8n2(56,10,11,%1,%%r12,4)\
  SAVE_SOLUTION_m8n2(6,7,64)\
  SOLVE_leri_m8n2(64,8,9,%1,%%r12,4) SUBTRACT_m8n2(72,10,11,%1,%%r12,4)\
  SOLVE_ri_m8n2(80,8,9,%1,%%r12,4) SUBTRACT_m8n2(88,10,11,%1,%%r12,4)\
  SAVE_SOLUTION_m8n2(8,9,128)\
  SOLVE_leri_m8n2(104,10,11,%1,%%r12,4)\
  SOLVE_ri_m8n2(120,10,11,%1,%%r12,4)\
  SAVE_SOLUTION_m8n2(10,11,192)

#define SOLVE_RN_m8n12 \
  "movq %2,%3;" GEMM_SUM_REORDER_8x4(4,5,6,7,63) GEMM_SUM_REORDER_8x4(8,9,10,11,63) GEMM_SUM_REORDER_8x4(12,13,14,15,63) "movq %2,%3; addq $32,%2;"\
  SOLVE_leri_m8n2(0,4,5,%1) SUBTRACT_m8n2(8,6,7,%1) SUBTRACT_m8n2(0,8,9,%1,%%r12,4) SUBTRACT_m8n2(8,10,11,%1,%%r12,4) SUBTRACT_m8n2(0,12,13,%1,%%r12,8) SUBTRACT_m8n2(8,14,15,%1,%%r12,8)\
  SOLVE_ri_m8n2(16,4,5,%1) SUBTRACT_m8n2(24,6,7,%1) SUBTRACT_m8n2(16,8,9,%1,%%r12,4) SUBTRACT_m8n2(24,10,11,%1,%%r12,4) SUBTRACT_m8n2(16,12,13,%1,%%r12,8) SUBTRACT_m8n2(24,14,15,%1,%%r12,8)\
  SAVE_SOLUTION_m8n2(4,5,0)\
  SOLVE_leri_m8n2(40,6,7,%1) SUBTRACT_m8n2(32,8,9,%1,%%r12,4) SUBTRACT_m8n2(40,10,11,%1,%%r12,4) SUBTRACT_m8n2(32,12,13,%1,%%r12,8) SUBTRACT_m8n2(40,14,15,%1,%%r12,8)\
  SOLVE_ri_m8n2(56,6,7,%1) SUBTRACT_m8n2(48,8,9,%1,%%r12,4) SUBTRACT_m8n2(56,10,11,%1,%%r12,4) SUBTRACT_m8n2(48,12,13,%1,%%r12,8) SUBTRACT_m8n2(56,14,15,%1,%%r12,8)\
  SAVE_SOLUTION_m8n2(6,7,64)\
  SOLVE_leri_m8n2(64,8,9,%1,%%r12,4) SUBTRACT_m8n2(72,10,11,%1,%%r12,4) SUBTRACT_m8n2(64,12,13,%1,%%r12,8) SUBTRACT_m8n2(72,14,15,%1,%%r12,8)\
  SOLVE_ri_m8n2(80,8,9,%1,%%r12,4) SUBTRACT_m8n2(88,10,11,%1,%%r12,4) SUBTRACT_m8n2(80,12,13,%1,%%r12,8) SUBTRACT_m8n2(88,14,15,%1,%%r12,8)\
  SAVE_SOLUTION_m8n2(8,9,128)\
  SOLVE_leri_m8n2(104,10,11,%1,%%r12,4) SUBTRACT_m8n2(96,12,13,%1,%%r12,8) SUBTRACT_m8n2(104,14,15,%1,%%r12,8)\
  SOLVE_ri_m8n2(120,10,11,%1,%%r12,4) SUBTRACT_m8n2(112,12,13,%1,%%r12,8) SUBTRACT_m8n2(120,14,15,%1,%%r12,8)\
  SAVE_SOLUTION_m8n2(10,11,192)\
  SOLVE_leri_m8n2(128,12,13,%1,%%r12,8) SUBTRACT_m8n2(136,14,15,%1,%%r12,8)\
  SOLVE_ri_m8n2(144,12,13,%1,%%r12,8) SUBTRACT_m8n2(152,14,15,%1,%%r12,8)\
  SAVE_SOLUTION_m8n2(12,13,256)\
  SOLVE_leri_m8n2(168,14,15,%1,%%r12,8)\
  SOLVE_ri_m8n2(184,14,15,%1,%%r12,8)\
  SAVE_SOLUTION_m8n2(14,15,320)

#define SOLVE_RN_m4n4 \
  "movq %2,%3;" GEMM_SUM_REORDER_4x4(4,5,6,7,4,5) "movq %2,%3; addq $16,%2;"\
  SOLVE_leri_m4n2(0,4,%1) SUBTRACT_m4n2(8,5,%1)\
  SOLVE_ri_m4n2(16,4,%1) SUBTRACT_m4n2(24,5,%1)\
  SAVE_SOLUTION_m4n2(4,0)\
  SOLVE_leri_m4n2(40,5,%1)\
  SOLVE_ri_m4n2(56,5,%1)\
  SAVE_SOLUTION_m4n2(5,32)

#define SOLVE_RN_m4n8 \
  "movq %2,%3;" GEMM_SUM_REORDER_4x4(4,5,6,7,4,5) GEMM_SUM_REORDER_4x4(8,9,10,11,6,7) "movq %2,%3; addq $16,%2;"\
  SOLVE_leri_m4n2(0,4,%1) SUBTRACT_m4n2(8,5,%1) SUBTRACT_m4n2(0,6,%1,%%r12,4) SUBTRACT_m4n2(8,7,%1,%%r12,4)\
  SOLVE_ri_m4n2(16,4,%1) SUBTRACT_m4n2(24,5,%1) SUBTRACT_m4n2(16,6,%1,%%r12,4) SUBTRACT_m4n2(24,7,%1,%%r12,4)\
  SAVE_SOLUTION_m4n2(4,0)\
  SOLVE_leri_m4n2(40,5,%1) SUBTRACT_m4n2(32,6,%1,%%r12,4) SUBTRACT_m4n2(40,7,%1,%%r12,4)\
  SOLVE_ri_m4n2(56,5,%1) SUBTRACT_m4n2(48,6,%1,%%r12,4) SUBTRACT_m4n2(56,7,%1,%%r12,4)\
  SAVE_SOLUTION_m4n2(5,32)\
  SOLVE_leri_m4n2(64,6,%1,%%r12,4) SUBTRACT_m4n2(72,7,%1,%%r12,4)\
  SOLVE_ri_m4n2(80,6,%1,%%r12,4) SUBTRACT_m4n2(88,7,%1,%%r12,4)\
  SAVE_SOLUTION_m4n2(6,64)\
  SOLVE_leri_m4n2(104,7,%1,%%r12,4)\
  SOLVE_ri_m4n2(120,7,%1,%%r12,4)\
  SAVE_SOLUTION_m4n2(7,96)

#define SOLVE_RN_m4n12 \
  "movq %2,%3;" GEMM_SUM_REORDER_4x4(4,5,6,7,4,5) GEMM_SUM_REORDER_4x4(8,9,10,11,6,7) GEMM_SUM_REORDER_4x4(12,13,14,15,8,9) "movq %2,%3; addq $16,%2;"\
  SOLVE_leri_m4n2(0,4,%1) SUBTRACT_m4n2(8,5,%1) SUBTRACT_m4n2(0,6,%1,%%r12,4) SUBTRACT_m4n2(8,7,%1,%%r12,4) SUBTRACT_m4n2(0,8,%1,%%r12,8) SUBTRACT_m4n2(8,9,%1,%%r12,8)\
  SOLVE_ri_m4n2(16,4,%1) SUBTRACT_m4n2(24,5,%1) SUBTRACT_m4n2(16,6,%1,%%r12,4) SUBTRACT_m4n2(24,7,%1,%%r12,4) SUBTRACT_m4n2(16,8,%1,%%r12,8) SUBTRACT_m4n2(24,9,%1,%%r12,8)\
  SAVE_SOLUTION_m4n2(4,0)\
  SOLVE_leri_m4n2(40,5,%1) SUBTRACT_m4n2(32,6,%1,%%r12,4) SUBTRACT_m4n2(40,7,%1,%%r12,4) SUBTRACT_m4n2(32,8,%1,%%r12,8) SUBTRACT_m4n2(40,9,%1,%%r12,8)\
  SOLVE_ri_m4n2(56,5,%1) SUBTRACT_m4n2(48,6,%1,%%r12,4) SUBTRACT_m4n2(56,7,%1,%%r12,4) SUBTRACT_m4n2(48,8,%1,%%r12,8) SUBTRACT_m4n2(56,9,%1,%%r12,8)\
  SAVE_SOLUTION_m4n2(5,32)\
  SOLVE_leri_m4n2(64,6,%1,%%r12,4) SUBTRACT_m4n2(72,7,%1,%%r12,4) SUBTRACT_m4n2(64,8,%1,%%r12,8) SUBTRACT_m4n2(72,9,%1,%%r12,8)\
  SOLVE_ri_m4n2(80,6,%1,%%r12,4) SUBTRACT_m4n2(88,7,%1,%%r12,4) SUBTRACT_m4n2(80,8,%1,%%r12,8) SUBTRACT_m4n2(88,9,%1,%%r12,8)\
  SAVE_SOLUTION_m4n2(6,64)\
  SOLVE_leri_m4n2(104,7,%1,%%r12,4) SUBTRACT_m4n2(96,8,%1,%%r12,8) SUBTRACT_m4n2(104,9,%1,%%r12,8)\
  SOLVE_ri_m4n2(120,7,%1,%%r12,4) SUBTRACT_m4n2(112,8,%1,%%r12,8) SUBTRACT_m4n2(120,9,%1,%%r12,8)\
  SAVE_SOLUTION_m4n2(7,96)\
  SOLVE_leri_m4n2(128,8,%1,%%r12,8) SUBTRACT_m4n2(136,9,%1,%%r12,8)\
  SOLVE_ri_m4n2(144,8,%1,%%r12,8) SUBTRACT_m4n2(152,9,%1,%%r12,8)\
  SAVE_SOLUTION_m4n2(8,128)\
  SOLVE_leri_m4n2(168,9,%1,%%r12,8)\
  SOLVE_ri_m4n2(184,9,%1,%%r12,8)\
  SAVE_SOLUTION_m4n2(9,160)

#define SOLVE_RN_m2n4 \
  "movq %2,%3;" GEMM_SUM_REORDER_2x4(4,5) "movq %2,%3; addq $8,%2;"\
  SOLVE_col1_ltor_m2n4(0,4,5,%1)\
  SOLVE_col2_ltor_m2n4(16,4,5,%1)\
  SOLVE_col3_ltor_m2n4(32,4,5,%1)\
  SOLVE_col4_ltor_m2n4(48,4,5,%1)\
  SAVE_SOLUTION_m2n4(4,5,0)

#define SOLVE_RN_m2n8 \
  "movq %2,%3;" GEMM_SUM_REORDER_2x4(4,5) GEMM_SUM_REORDER_2x4(6,7) "movq %2,%3; addq $8,%2;"\
  SOLVE_col1_ltor_m2n4(0,4,5,%1) SUBTRACT_m2n4(0,6,7,%1,%%r12,4)\
  SOLVE_col2_ltor_m2n4(16,4,5,%1) SUBTRACT_m2n4(16,6,7,%1,%%r12,4)\
  SOLVE_col3_ltor_m2n4(32,4,5,%1) SUBTRACT_m2n4(32,6,7,%1,%%r12,4)\
  SOLVE_col4_ltor_m2n4(48,4,5,%1) SUBTRACT_m2n4(48,6,7,%1,%%r12,4)\
  SAVE_SOLUTION_m2n4(4,5,0)\
  SOLVE_col1_ltor_m2n4(64,6,7,%1,%%r12,4)\
  SOLVE_col2_ltor_m2n4(80,6,7,%1,%%r12,4)\
  SOLVE_col3_ltor_m2n4(96,6,7,%1,%%r12,4)\
  SOLVE_col4_ltor_m2n4(112,6,7,%1,%%r12,4)\
  SAVE_SOLUTION_m2n4(6,7,32)

#define SOLVE_RN_m2n12 \
  "movq %2,%3;" GEMM_SUM_REORDER_2x4(4,5) GEMM_SUM_REORDER_2x4(6,7) GEMM_SUM_REORDER_2x4(8,9) "movq %2,%3; addq $8,%2;"\
  SOLVE_col1_ltor_m2n4(0,4,5,%1) SUBTRACT_m2n4(0,6,7,%1,%%r12,4) SUBTRACT_m2n4(0,8,9,%1,%%r12,8)\
  SOLVE_col2_ltor_m2n4(16,4,5,%1) SUBTRACT_m2n4(16,6,7,%1,%%r12,4) SUBTRACT_m2n4(16,8,9,%1,%%r12,8)\
  SOLVE_col3_ltor_m2n4(32,4,5,%1) SUBTRACT_m2n4(32,6,7,%1,%%r12,4) SUBTRACT_m2n4(32,8,9,%1,%%r12,8)\
  SOLVE_col4_ltor_m2n4(48,4,5,%1) SUBTRACT_m2n4(48,6,7,%1,%%r12,4) SUBTRACT_m2n4(48,8,9,%1,%%r12,8)\
  SAVE_SOLUTION_m2n4(4,5,0)\
  SOLVE_col1_ltor_m2n4(64,6,7,%1,%%r12,4) SUBTRACT_m2n4(64,8,9,%1,%%r12,8)\
  SOLVE_col2_ltor_m2n4(80,6,7,%1,%%r12,4) SUBTRACT_m2n4(80,8,9,%1,%%r12,8)\
  SOLVE_col3_ltor_m2n4(96,6,7,%1,%%r12,4) SUBTRACT_m2n4(96,8,9,%1,%%r12,8)\
  SOLVE_col4_ltor_m2n4(112,6,7,%1,%%r12,4) SUBTRACT_m2n4(112,8,9,%1,%%r12,8)\
  SAVE_SOLUTION_m2n4(6,7,32)\
  SOLVE_col1_ltor_m2n4(128,8,9,%1,%%r12,8)\
  SOLVE_col2_ltor_m2n4(144,8,9,%1,%%r12,8)\
  SOLVE_col3_ltor_m2n4(160,8,9,%1,%%r12,8)\
  SOLVE_col4_ltor_m2n4(176,8,9,%1,%%r12,8)\
  SAVE_SOLUTION_m2n4(8,9,64)

#define SOLVE_RN_m1n4 \
  "movq %2,%3;" GEMM_SUM_REORDER_1x4(4) "movq %2,%3; addq $4,%2;"\
  SOLVE_col1_ltor_m1n4(0,4,%1)\
  SOLVE_col2_ltor_m1n4(16,4,%1)\
  SOLVE_col3_ltor_m1n4(32,4,%1)\
  SOLVE_col4_ltor_m1n4(48,4,%1)\
  SAVE_SOLUTION_m1n4(4,0)

#define SOLVE_RN_m1n8 \
  "movq %2,%3;" GEMM_SUM_REORDER_1x4(4) GEMM_SUM_REORDER_1x4(5) "movq %2,%3; addq $4,%2;"\
  SOLVE_col1_ltor_m1n4(0,4,%1) SUBTRACT_m1n4(0,5,%1,%%r12,4)\
  SOLVE_col2_ltor_m1n4(16,4,%1) SUBTRACT_m1n4(16,5,%1,%%r12,4)\
  SOLVE_col3_ltor_m1n4(32,4,%1) SUBTRACT_m1n4(32,5,%1,%%r12,4)\
  SOLVE_col4_ltor_m1n4(48,4,%1) SUBTRACT_m1n4(48,5,%1,%%r12,4)\
  SAVE_SOLUTION_m1n4(4,0)\
  SOLVE_col1_ltor_m1n4(64,5,%1,%%r12,4)\
  SOLVE_col2_ltor_m1n4(80,5,%1,%%r12,4)\
  SOLVE_col3_ltor_m1n4(96,5,%1,%%r12,4)\
  SOLVE_col4_ltor_m1n4(112,5,%1,%%r12,4)\
  SAVE_SOLUTION_m1n4(5,16)

#define SOLVE_RN_m1n12 \
  "movq %2,%3;" GEMM_SUM_REORDER_1x4(4) GEMM_SUM_REORDER_1x4(5) GEMM_SUM_REORDER_1x4(6) "movq %2,%3; addq $4,%2;"\
  SOLVE_col1_ltor_m1n4(0,4,%1) SUBTRACT_m1n4(0,5,%1,%%r12,4) SUBTRACT_m1n4(0,6,%1,%%r12,8)\
  SOLVE_col2_ltor_m1n4(16,4,%1) SUBTRACT_m1n4(16,5,%1,%%r12,4) SUBTRACT_m1n4(16,6,%1,%%r12,8)\
  SOLVE_col3_ltor_m1n4(32,4,%1) SUBTRACT_m1n4(32,5,%1,%%r12,4) SUBTRACT_m1n4(32,6,%1,%%r12,8)\
  SOLVE_col4_ltor_m1n4(48,4,%1) SUBTRACT_m1n4(48,5,%1,%%r12,4) SUBTRACT_m1n4(48,6,%1,%%r12,8)\
  SAVE_SOLUTION_m1n4(4,0)\
  SOLVE_col1_ltor_m1n4(64,5,%1,%%r12,4) SUBTRACT_m1n4(64,6,%1,%%r12,8)\
  SOLVE_col2_ltor_m1n4(80,5,%1,%%r12,4) SUBTRACT_m1n4(80,6,%1,%%r12,8)\
  SOLVE_col3_ltor_m1n4(96,5,%1,%%r12,4) SUBTRACT_m1n4(96,6,%1,%%r12,8)\
  SOLVE_col4_ltor_m1n4(112,5,%1,%%r12,4) SUBTRACT_m1n4(112,6,%1,%%r12,8)\
  SAVE_SOLUTION_m1n4(5,16)\
  SOLVE_col1_ltor_m1n4(128,6,%1,%%r12,8)\
  SOLVE_col2_ltor_m1n4(144,6,%1,%%r12,8)\
  SOLVE_col3_ltor_m1n4(160,6,%1,%%r12,8)\
  SOLVE_col4_ltor_m1n4(176,6,%1,%%r12,8)\
  SAVE_SOLUTION_m1n4(6,32)

#define GEMM_RN_SIMPLE(mdim,ndim) \
  "movq %%r15,%0; leaq (%%r15,%%r12,"#mdim"),%%r15; movq %%r13,%5; movq %%r14,%1;" INIT_m##mdim##n##ndim\
  "testq %5,%5; jz 1"#mdim""#ndim"2f;"\
  "1"#mdim""#ndim"1:\n\t"\
  GEMM_KERNEL_k1m##mdim##n##ndim "addq $16,%1; addq $"#mdim"*4,%0; decq %5; jnz 1"#mdim""#ndim"1b;"\
  "1"#mdim""#ndim"2:\n\t"
#define GEMM_RN_m8n4 GEMM_RN_SIMPLE(8,4)
#define GEMM_RN_m8n8 GEMM_RN_SIMPLE(8,8)
#define GEMM_RN_m8n12 \
  "movq %%r15,%0; leaq (%%r15,%%r12,8),%%r15; movq %%r13,%5; movq %%r14,%1;" INIT_m8n12\
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
#define GEMM_RN_m4n4 GEMM_RN_SIMPLE(4,4)
#define GEMM_RN_m4n8 GEMM_RN_SIMPLE(4,8)
#define GEMM_RN_m4n12 GEMM_RN_SIMPLE(4,12)
#define GEMM_RN_m2n4 GEMM_RN_SIMPLE(2,4)
#define GEMM_RN_m2n8 GEMM_RN_SIMPLE(2,8)
#define GEMM_RN_m2n12 GEMM_RN_SIMPLE(2,12)
#define GEMM_RN_m1n4 GEMM_RN_SIMPLE(1,4)
#define GEMM_RN_m1n8 GEMM_RN_SIMPLE(1,8)
#define GEMM_RN_m1n12 GEMM_RN_SIMPLE(1,12)

#define COMPUTE(ndim) {\
  __asm__ __volatile__(\
    "movq %0,%%r15; movq %1,%%r14; movq %7,%%r13; movq %6,%%r12; salq $2,%%r12; movq %10,%%r11;"\
    "cmpq $8,%%r11; jb "#ndim"772f;"\
    #ndim"771:\n\t"\
    GEMM_RN_m8n##ndim SOLVE_RN_m8n##ndim "subq $8,%%r11; cmpq $8,%%r11; jnb "#ndim"771b;"\
    #ndim"772:\n\t"\
    "testq $4,%%r11; jz "#ndim"773f;"\
    GEMM_RN_m4n##ndim SOLVE_RN_m4n##ndim "subq $4,%%r11;"\
    #ndim"773:\n\t"\
    "testq $2,%%r11; jz "#ndim"774f;"\
    GEMM_RN_m2n##ndim SOLVE_RN_m2n##ndim "subq $2,%%r11;"\
    #ndim"774:\n\t"\
    "testq $1,%%r11; jz "#ndim"775f;"\
    GEMM_RN_m1n##ndim SOLVE_RN_m1n##ndim "subq $1,%%r11;"\
    #ndim"775:\n\t"\
    "movq %%r15,%0; movq %%r14,%1; vzeroupper;"\
  :"+r"(a_ptr),"+r"(b_ptr),"+r"(c_ptr),"+r"(c_tmp),"+r"(ldc_bytes),"+r"(k_cnt):"m"(K),"m"(OFF),"m"(one[0]),"m"(zero[0]),"m"(M)\
  :"r11","r12","r13","r14","r15","cc","memory",\
  "xmm0","xmm1","xmm2","xmm3","xmm4","xmm5","xmm6","xmm7","xmm8","xmm9","xmm10","xmm11","xmm12","xmm13","xmm14","xmm15");\
  a_ptr -= M * K; b_ptr += ndim * K; c_ptr += ldc * ndim - M; OFF += ndim;\
}

static void solve_RN(BLASLONG m, BLASLONG n, FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc) {
  FLOAT a0, b0;
  int i, j, k;
  for (i=0; i<n; i++) {
    b0 = b[i*n+i];
    for (j=0; j<m; j++) {
      a0 = c[i*ldc+j] * b0;
      a[i*m+j] = c[i*ldc+j] = a0;
      for (k=i+1; k<n; k++) c[k*ldc+j] -= a0 * b[i*n+k];
    }
  }
}
static void COMPUTE_EDGE_1_nchunk(BLASLONG m, BLASLONG n, FLOAT *sa, FLOAT *sb, FLOAT *C, BLASLONG ldc, BLASLONG k, BLASLONG offset) {
  BLASLONG m_count = m, kk = offset; FLOAT *a_ptr = sa, *c_ptr = C;
  for(;m_count>7;m_count-=8){
    if(kk>0) GEMM_KERNEL_N(8,n,kk,-1.0,a_ptr,sb,c_ptr,ldc);
    solve_RN(8,n,a_ptr+kk*8,sb+kk*n,c_ptr,ldc);
    a_ptr += k * 8; c_ptr += 8;
  }
  for(;m_count>3;m_count-=4){
    if(kk>0) GEMM_KERNEL_N(4,n,kk,-1.0,a_ptr,sb,c_ptr,ldc);
    solve_RN(4,n,a_ptr+kk*4,sb+kk*n,c_ptr,ldc);
    a_ptr += k * 4; c_ptr += 4;
  }
  for(;m_count>1;m_count-=2){
    if(kk>0) GEMM_KERNEL_N(2,n,kk,-1.0,a_ptr,sb,c_ptr,ldc);
    solve_RN(2,n,a_ptr+kk*2,sb+kk*n,c_ptr,ldc);
    a_ptr += k * 2; c_ptr += 2;
  }
  if(m_count>0){
    if(kk>0) GEMM_KERNEL_N(1,n,kk,-1.0,a_ptr,sb,c_ptr,ldc);
    solve_RN(1,n,a_ptr+kk*1,sb+kk*n,c_ptr,ldc);
    a_ptr += k * 1; c_ptr += 1;
  }
}
int CNAME(BLASLONG m, BLASLONG n, BLASLONG k, FLOAT dummy1, FLOAT *sa, FLOAT *sb, FLOAT *C, BLASLONG ldc, BLASLONG offset){
  float *a_ptr = sa, *b_ptr = sb, *c_ptr = C, *c_tmp = C;
  float one[8] = {1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0};
  float zero[8] = {0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0};
  uint64_t ldc_bytes = (uint64_t)ldc * sizeof(float), K = (uint64_t)k, M = (uint64_t)m, OFF = (uint64_t)-offset, k_cnt = 0;
  BLASLONG n_count = n;
  for(;n_count>11;n_count-=12) COMPUTE(12)
  for(;n_count>7;n_count-=8) COMPUTE(8)
  for(;n_count>3;n_count-=4) COMPUTE(4)
  for(;n_count>1;n_count-=2) { COMPUTE_EDGE_1_nchunk(m,2,a_ptr,b_ptr,c_ptr,ldc,k,OFF); b_ptr += 2*k; c_ptr += ldc*2; OFF+=2;}
  if(n_count>0) COMPUTE_EDGE_1_nchunk(m,1,a_ptr,b_ptr,c_ptr,ldc,k,OFF);
  return 0;
}
