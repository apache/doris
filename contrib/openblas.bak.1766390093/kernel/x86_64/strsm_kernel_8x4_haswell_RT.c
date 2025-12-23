#include "common.h"
#include <stdint.h>
#include "strsm_kernel_8x4_haswell_R_common.h"

#define SOLVE_RT_m8n4 \
  "movq %2,%3;" GEMM_SUM_REORDER_8x4(4,5,6,7,63) "negq %4; leaq (%3,%4,2),%3; negq %4; addq $32,%2;"\
  SOLVE_rile_m8n2(-8,6,7,%1) SUBTRACT_m8n2(-16,4,5,%1)\
  SOLVE_le_m8n2(-24,6,7,%1) SUBTRACT_m8n2(-32,4,5,%1)\
  SAVE_SOLUTION_m8n2(6,7,-64) "negq %4; leaq (%3,%4,4),%3; negq %4;"\
  SOLVE_rile_m8n2(-48,4,5,%1)\
  SOLVE_le_m8n2(-64,4,5,%1)\
  SAVE_SOLUTION_m8n2(4,5,-128)

#define SOLVE_RT_m8n8 \
  "movq %2,%3;" GEMM_SUM_REORDER_8x4(4,5,6,7,63) GEMM_SUM_REORDER_8x4(8,9,10,11,63) "negq %4; leaq (%3,%4,2),%3; negq %4; addq $32,%2;"\
  SOLVE_rile_m8n2(-8,10,11,%1,%%r12,4) SUBTRACT_m8n2(-16,8,9,%1,%%r12,4) SUBTRACT_m8n2(-8,6,7,%1) SUBTRACT_m8n2(-16,4,5,%1)\
  SOLVE_le_m8n2(-24,10,11,%1,%%r12,4) SUBTRACT_m8n2(-32,8,9,%1,%%r12,4) SUBTRACT_m8n2(-24,6,7,%1) SUBTRACT_m8n2(-32,4,5,%1)\
  SAVE_SOLUTION_m8n2(10,11,-64) "negq %4; leaq (%3,%4,4),%3; negq %4;"\
  SOLVE_rile_m8n2(-48,8,9,%1,%%r12,4) SUBTRACT_m8n2(-40,6,7,%1) SUBTRACT_m8n2(-48,4,5,%1)\
  SOLVE_le_m8n2(-64,8,9,%1,%%r12,4) SUBTRACT_m8n2(-56,6,7,%1) SUBTRACT_m8n2(-64,4,5,%1)\
  SAVE_SOLUTION_m8n2(8,9,-128) "negq %4; leaq (%3,%4,4),%3; negq %4;"\
  SOLVE_rile_m8n2(-72,6,7,%1) SUBTRACT_m8n2(-80,4,5,%1)\
  SOLVE_le_m8n2(-88,6,7,%1) SUBTRACT_m8n2(-96,4,5,%1)\
  SAVE_SOLUTION_m8n2(6,7,-192) "negq %4; leaq (%3,%4,4),%3; negq %4;"\
  SOLVE_rile_m8n2(-112,4,5,%1)\
  SOLVE_le_m8n2(-128,4,5,%1)\
  SAVE_SOLUTION_m8n2(4,5,-256)

#define SOLVE_RT_m8n12 \
  "movq %2,%3;" GEMM_SUM_REORDER_8x4(4,5,6,7,63) GEMM_SUM_REORDER_8x4(8,9,10,11,63) GEMM_SUM_REORDER_8x4(12,13,14,15,63) "negq %4; leaq (%3,%4,2),%3; negq %4; addq $32,%2;"\
  SOLVE_rile_m8n2(-8,14,15,%1,%%r12,8) SUBTRACT_m8n2(-16,12,13,%1,%%r12,8) SUBTRACT_m8n2(-8,10,11,%1,%%r12,4) SUBTRACT_m8n2(-16,8,9,%1,%%r12,4) SUBTRACT_m8n2(-8,6,7,%1) SUBTRACT_m8n2(-16,4,5,%1)\
  SOLVE_le_m8n2(-24,14,15,%1,%%r12,8) SUBTRACT_m8n2(-32,12,13,%1,%%r12,8) SUBTRACT_m8n2(-24,10,11,%1,%%r12,4) SUBTRACT_m8n2(-32,8,9,%1,%%r12,4) SUBTRACT_m8n2(-24,6,7,%1) SUBTRACT_m8n2(-32,4,5,%1)\
  SAVE_SOLUTION_m8n2(14,15,-64) "negq %4; leaq (%3,%4,4),%3; negq %4;"\
  SOLVE_rile_m8n2(-48,12,13,%1,%%r12,8) SUBTRACT_m8n2(-40,10,11,%1,%%r12,4) SUBTRACT_m8n2(-48,8,9,%1,%%r12,4) SUBTRACT_m8n2(-40,6,7,%1) SUBTRACT_m8n2(-48,4,5,%1)\
  SOLVE_le_m8n2(-64,12,13,%1,%%r12,8) SUBTRACT_m8n2(-56,10,11,%1,%%r12,4) SUBTRACT_m8n2(-64,8,9,%1,%%r12,4) SUBTRACT_m8n2(-56,6,7,%1) SUBTRACT_m8n2(-64,4,5,%1)\
  SAVE_SOLUTION_m8n2(12,13,-128) "negq %4; leaq (%3,%4,4),%3; negq %4;"\
  SOLVE_rile_m8n2(-72,10,11,%1,%%r12,4) SUBTRACT_m8n2(-80,8,9,%1,%%r12,4) SUBTRACT_m8n2(-72,6,7,%1) SUBTRACT_m8n2(-80,4,5,%1)\
  SOLVE_le_m8n2(-88,10,11,%1,%%r12,4) SUBTRACT_m8n2(-96,8,9,%1,%%r12,4) SUBTRACT_m8n2(-88,6,7,%1) SUBTRACT_m8n2(-96,4,5,%1)\
  SAVE_SOLUTION_m8n2(10,11,-192) "negq %4; leaq (%3,%4,4),%3; negq %4;"\
  SOLVE_rile_m8n2(-112,8,9,%1,%%r12,4) SUBTRACT_m8n2(-104,6,7,%1) SUBTRACT_m8n2(-112,4,5,%1)\
  SOLVE_le_m8n2(-128,8,9,%1,%%r12,4) SUBTRACT_m8n2(-120,6,7,%1) SUBTRACT_m8n2(-128,4,5,%1)\
  SAVE_SOLUTION_m8n2(8,9,-256) "negq %4; leaq (%3,%4,4),%3; negq %4;"\
  SOLVE_rile_m8n2(-136,6,7,%1) SUBTRACT_m8n2(-144,4,5,%1)\
  SOLVE_le_m8n2(-152,6,7,%1) SUBTRACT_m8n2(-160,4,5,%1)\
  SAVE_SOLUTION_m8n2(6,7,-320) "negq %4; leaq (%3,%4,4),%3; negq %4;"\
  SOLVE_rile_m8n2(-176,4,5,%1)\
  SOLVE_le_m8n2(-192,4,5,%1)\
  SAVE_SOLUTION_m8n2(4,5,-384)

#define SOLVE_RT_m4n4 \
  "movq %2,%3;" GEMM_SUM_REORDER_4x4(4,5,6,7,4,5) "negq %4; leaq (%3,%4,2),%3; negq %4; addq $16,%2;"\
  SOLVE_rile_m4n2(-8,5,%1) SUBTRACT_m4n2(-16,4,%1)\
  SOLVE_le_m4n2(-24,5,%1) SUBTRACT_m4n2(-32,4,%1)\
  SAVE_SOLUTION_m4n2(5,-32) "negq %4; leaq (%3,%4,4),%3; negq %4;"\
  SOLVE_rile_m4n2(-48,4,%1)\
  SOLVE_le_m4n2(-64,4,%1)\
  SAVE_SOLUTION_m4n2(4,-64)

#define SOLVE_RT_m4n8 \
  "movq %2,%3;" GEMM_SUM_REORDER_4x4(4,5,6,7,4,5) GEMM_SUM_REORDER_4x4(8,9,10,11,6,7) "negq %4; leaq (%3,%4,2),%3; negq %4; addq $16,%2;"\
  SOLVE_rile_m4n2(-8,7,%1,%%r12,4) SUBTRACT_m4n2(-16,6,%1,%%r12,4) SUBTRACT_m4n2(-8,5,%1) SUBTRACT_m4n2(-16,4,%1)\
  SOLVE_le_m4n2(-24,7,%1,%%r12,4) SUBTRACT_m4n2(-32,6,%1,%%r12,4) SUBTRACT_m4n2(-24,5,%1) SUBTRACT_m4n2(-32,4,%1)\
  SAVE_SOLUTION_m4n2(7,-32) "negq %4; leaq (%3,%4,4),%3; negq %4;"\
  SOLVE_rile_m4n2(-48,6,%1,%%r12,4) SUBTRACT_m4n2(-40,5,%1) SUBTRACT_m4n2(-48,4,%1)\
  SOLVE_le_m4n2(-64,6,%1,%%r12,4) SUBTRACT_m4n2(-56,5,%1) SUBTRACT_m4n2(-64,4,%1)\
  SAVE_SOLUTION_m4n2(6,-64) "negq %4; leaq (%3,%4,4),%3; negq %4;"\
  SOLVE_rile_m4n2(-72,5,%1) SUBTRACT_m4n2(-80,4,%1)\
  SOLVE_le_m4n2(-88,5,%1) SUBTRACT_m4n2(-96,4,%1)\
  SAVE_SOLUTION_m4n2(5,-96) "negq %4; leaq (%3,%4,4),%3; negq %4;"\
  SOLVE_rile_m4n2(-112,4,%1)\
  SOLVE_le_m4n2(-128,4,%1)\
  SAVE_SOLUTION_m4n2(4,-128)

#define SOLVE_RT_m4n12 \
  "movq %2,%3;" GEMM_SUM_REORDER_4x4(4,5,6,7,4,5) GEMM_SUM_REORDER_4x4(8,9,10,11,6,7) GEMM_SUM_REORDER_4x4(12,13,14,15,8,9) "negq %4; leaq (%3,%4,2),%3; negq %4; addq $16,%2;"\
  SOLVE_rile_m4n2(-8,9,%1,%%r12,8) SUBTRACT_m4n2(-16,8,%1,%%r12,8) SUBTRACT_m4n2(-8,7,%1,%%r12,4) SUBTRACT_m4n2(-16,6,%1,%%r12,4) SUBTRACT_m4n2(-8,5,%1) SUBTRACT_m4n2(-16,4,%1)\
  SOLVE_le_m4n2(-24,9,%1,%%r12,8) SUBTRACT_m4n2(-32,8,%1,%%r12,8) SUBTRACT_m4n2(-24,7,%1,%%r12,4) SUBTRACT_m4n2(-32,6,%1,%%r12,4) SUBTRACT_m4n2(-24,5,%1) SUBTRACT_m4n2(-32,4,%1)\
  SAVE_SOLUTION_m4n2(9,-32) "negq %4; leaq (%3,%4,4),%3; negq %4;"\
  SOLVE_rile_m4n2(-48,8,%1,%%r12,8) SUBTRACT_m4n2(-40,7,%1,%%r12,4) SUBTRACT_m4n2(-48,6,%1,%%r12,4) SUBTRACT_m4n2(-40,5,%1) SUBTRACT_m4n2(-48,4,%1)\
  SOLVE_le_m4n2(-64,8,%1,%%r12,8) SUBTRACT_m4n2(-56,7,%1,%%r12,4) SUBTRACT_m4n2(-64,6,%1,%%r12,4) SUBTRACT_m4n2(-56,5,%1) SUBTRACT_m4n2(-64,4,%1)\
  SAVE_SOLUTION_m4n2(8,-64) "negq %4; leaq (%3,%4,4),%3; negq %4;"\
  SOLVE_rile_m4n2(-72,7,%1,%%r12,4) SUBTRACT_m4n2(-80,6,%1,%%r12,4) SUBTRACT_m4n2(-72,5,%1) SUBTRACT_m4n2(-80,4,%1)\
  SOLVE_le_m4n2(-88,7,%1,%%r12,4) SUBTRACT_m4n2(-96,6,%1,%%r12,4) SUBTRACT_m4n2(-88,5,%1) SUBTRACT_m4n2(-96,4,%1)\
  SAVE_SOLUTION_m4n2(7,-96) "negq %4; leaq (%3,%4,4),%3; negq %4;"\
  SOLVE_rile_m4n2(-112,6,%1,%%r12,4) SUBTRACT_m4n2(-104,5,%1) SUBTRACT_m4n2(-112,4,%1)\
  SOLVE_le_m4n2(-128,6,%1,%%r12,4) SUBTRACT_m4n2(-120,5,%1) SUBTRACT_m4n2(-128,4,%1)\
  SAVE_SOLUTION_m4n2(6,-128) "negq %4; leaq (%3,%4,4),%3; negq %4;"\
  SOLVE_rile_m4n2(-136,5,%1) SUBTRACT_m4n2(-144,4,%1)\
  SOLVE_le_m4n2(-152,5,%1) SUBTRACT_m4n2(-160,4,%1)\
  SAVE_SOLUTION_m4n2(5,-160) "negq %4; leaq (%3,%4,4),%3; negq %4;"\
  SOLVE_rile_m4n2(-176,4,%1)\
  SOLVE_le_m4n2(-192,4,%1)\
  SAVE_SOLUTION_m4n2(4,-192)

#define SOLVE_RT_m2n4 \
  "movq %2,%3;" GEMM_SUM_REORDER_2x4(4,5) "negq %4; leaq (%3,%4,4),%3; negq %4; addq $8,%2;"\
  SOLVE_col4_rtol_m2n4(-16,4,5,%1)\
  SOLVE_col3_rtol_m2n4(-32,4,5,%1)\
  SOLVE_col2_rtol_m2n4(-48,4,5,%1)\
  SOLVE_col1_rtol_m2n4(-64,4,5,%1)\
  SAVE_SOLUTION_m2n4(4,5,-32)

#define SOLVE_RT_m2n8 \
  "movq %2,%3;" GEMM_SUM_REORDER_2x4(4,5) GEMM_SUM_REORDER_2x4(6,7) "negq %4; leaq (%3,%4,4),%3; negq %4; addq $8,%2;"\
  SOLVE_col4_rtol_m2n4(-16,6,7,%1,%%r12,4) SUBTRACT_m2n4(-16,4,5,%1)\
  SOLVE_col3_rtol_m2n4(-32,6,7,%1,%%r12,4) SUBTRACT_m2n4(-32,4,5,%1)\
  SOLVE_col2_rtol_m2n4(-48,6,7,%1,%%r12,4) SUBTRACT_m2n4(-48,4,5,%1)\
  SOLVE_col1_rtol_m2n4(-64,6,7,%1,%%r12,4) SUBTRACT_m2n4(-64,4,5,%1)\
  SAVE_SOLUTION_m2n4(6,7,-32) "negq %4; leaq (%3,%4,8),%3; negq %4;"\
  SOLVE_col4_rtol_m2n4(-80,4,5,%1)\
  SOLVE_col3_rtol_m2n4(-96,4,5,%1)\
  SOLVE_col2_rtol_m2n4(-112,4,5,%1)\
  SOLVE_col1_rtol_m2n4(-128,4,5,%1)\
  SAVE_SOLUTION_m2n4(4,5,-64)

#define SOLVE_RT_m2n12 \
  "movq %2,%3;" GEMM_SUM_REORDER_2x4(4,5) GEMM_SUM_REORDER_2x4(6,7) GEMM_SUM_REORDER_2x4(8,9) "negq %4; leaq (%3,%4,4),%3; negq %4; addq $8,%2;"\
  SOLVE_col4_rtol_m2n4(-16,8,9,%1,%%r12,8) SUBTRACT_m2n4(-16,6,7,%1,%%r12,4) SUBTRACT_m2n4(-16,4,5,%1)\
  SOLVE_col3_rtol_m2n4(-32,8,9,%1,%%r12,8) SUBTRACT_m2n4(-32,6,7,%1,%%r12,4) SUBTRACT_m2n4(-32,4,5,%1)\
  SOLVE_col2_rtol_m2n4(-48,8,9,%1,%%r12,8) SUBTRACT_m2n4(-48,6,7,%1,%%r12,4) SUBTRACT_m2n4(-48,4,5,%1)\
  SOLVE_col1_rtol_m2n4(-64,8,9,%1,%%r12,8) SUBTRACT_m2n4(-64,6,7,%1,%%r12,4) SUBTRACT_m2n4(-64,4,5,%1)\
  SAVE_SOLUTION_m2n4(8,9,-32) "negq %4; leaq (%3,%4,8),%3; negq %4;"\
  SOLVE_col4_rtol_m2n4(-80,6,7,%1,%%r12,4) SUBTRACT_m2n4(-80,4,5,%1)\
  SOLVE_col3_rtol_m2n4(-96,6,7,%1,%%r12,4) SUBTRACT_m2n4(-96,4,5,%1)\
  SOLVE_col2_rtol_m2n4(-112,6,7,%1,%%r12,4) SUBTRACT_m2n4(-112,4,5,%1)\
  SOLVE_col1_rtol_m2n4(-128,6,7,%1,%%r12,4) SUBTRACT_m2n4(-128,4,5,%1)\
  SAVE_SOLUTION_m2n4(6,7,-64) "negq %4; leaq (%3,%4,8),%3; negq %4;"\
  SOLVE_col4_rtol_m2n4(-144,4,5,%1)\
  SOLVE_col3_rtol_m2n4(-160,4,5,%1)\
  SOLVE_col2_rtol_m2n4(-176,4,5,%1)\
  SOLVE_col1_rtol_m2n4(-192,4,5,%1)\
  SAVE_SOLUTION_m2n4(4,5,-96)

#define SOLVE_RT_m1n4 \
  "movq %2,%3;" GEMM_SUM_REORDER_1x4(4) "negq %4; leaq (%3,%4,4),%3; negq %4; addq $4,%2;"\
  SOLVE_col4_rtol_m1n4(-16,4,%1)\
  SOLVE_col3_rtol_m1n4(-32,4,%1)\
  SOLVE_col2_rtol_m1n4(-48,4,%1)\
  SOLVE_col1_rtol_m1n4(-64,4,%1)\
  SAVE_SOLUTION_m1n4(4,-16)

#define SOLVE_RT_m1n8 \
  "movq %2,%3;" GEMM_SUM_REORDER_1x4(4) GEMM_SUM_REORDER_1x4(5) "negq %4; leaq (%3,%4,4),%3; negq %4; addq $4,%2;"\
  SOLVE_col4_rtol_m1n4(-16,5,%1,%%r12,4) SUBTRACT_m1n4(-16,4,%1)\
  SOLVE_col3_rtol_m1n4(-32,5,%1,%%r12,4) SUBTRACT_m1n4(-32,4,%1)\
  SOLVE_col2_rtol_m1n4(-48,5,%1,%%r12,4) SUBTRACT_m1n4(-48,4,%1)\
  SOLVE_col1_rtol_m1n4(-64,5,%1,%%r12,4) SUBTRACT_m1n4(-64,4,%1)\
  SAVE_SOLUTION_m1n4(5,-16) "negq %4; leaq (%3,%4,8),%3; negq %4;"\
  SOLVE_col4_rtol_m1n4(-80,4,%1)\
  SOLVE_col3_rtol_m1n4(-96,4,%1)\
  SOLVE_col2_rtol_m1n4(-112,4,%1)\
  SOLVE_col1_rtol_m1n4(-128,4,%1)\
  SAVE_SOLUTION_m1n4(4,-32)

#define SOLVE_RT_m1n12 \
  "movq %2,%3;" GEMM_SUM_REORDER_1x4(4) GEMM_SUM_REORDER_1x4(5) GEMM_SUM_REORDER_1x4(6) "negq %4; leaq (%3,%4,4),%3; negq %4; addq $4,%2;"\
  SOLVE_col4_rtol_m1n4(-16,6,%1,%%r12,8) SUBTRACT_m1n4(-16,5,%1,%%r12,4) SUBTRACT_m1n4(-16,4,%1)\
  SOLVE_col3_rtol_m1n4(-32,6,%1,%%r12,8) SUBTRACT_m1n4(-32,5,%1,%%r12,4) SUBTRACT_m1n4(-32,4,%1)\
  SOLVE_col2_rtol_m1n4(-48,6,%1,%%r12,8) SUBTRACT_m1n4(-48,5,%1,%%r12,4) SUBTRACT_m1n4(-48,4,%1)\
  SOLVE_col1_rtol_m1n4(-64,6,%1,%%r12,8) SUBTRACT_m1n4(-64,5,%1,%%r12,4) SUBTRACT_m1n4(-64,4,%1)\
  SAVE_SOLUTION_m1n4(6,-16) "negq %4; leaq (%3,%4,8),%3; negq %4;"\
  SOLVE_col4_rtol_m1n4(-80,5,%1,%%r12,4) SUBTRACT_m1n4(-80,4,%1)\
  SOLVE_col3_rtol_m1n4(-96,5,%1,%%r12,4) SUBTRACT_m1n4(-96,4,%1)\
  SOLVE_col2_rtol_m1n4(-112,5,%1,%%r12,4) SUBTRACT_m1n4(-112,4,%1)\
  SOLVE_col1_rtol_m1n4(-128,5,%1,%%r12,4) SUBTRACT_m1n4(-128,4,%1)\
  SAVE_SOLUTION_m1n4(5,-32) "negq %4; leaq (%3,%4,8),%3; negq %4;"\
  SOLVE_col4_rtol_m1n4(-144,4,%1)\
  SOLVE_col3_rtol_m1n4(-160,4,%1)\
  SOLVE_col2_rtol_m1n4(-176,4,%1)\
  SOLVE_col1_rtol_m1n4(-192,4,%1)\
  SAVE_SOLUTION_m1n4(4,-48)

/* r14 = b_tail, r15 = a_tail, r13 = k-kk */
#define GEMM_RT_SIMPLE(mdim,ndim) \
  "leaq (%%r15,%%r12,"#mdim"),%%r15; movq %%r15,%0; movq %%r13,%5; movq %%r14,%1;" INIT_m##mdim##n##ndim\
  "testq %5,%5; jz 1"#mdim""#ndim"2f;"\
  "1"#mdim""#ndim"1:\n\t"\
  "subq $16,%1; subq $"#mdim"*4,%0;" GEMM_KERNEL_k1m##mdim##n##ndim "decq %5; jnz 1"#mdim""#ndim"1b;"\
  "1"#mdim""#ndim"2:\n\t"
#define GEMM_RT_m8n4 GEMM_RT_SIMPLE(8,4)
#define GEMM_RT_m8n8 GEMM_RT_SIMPLE(8,8)
#define GEMM_RT_m8n12 \
  "leaq (%%r15,%%r12,8),%%r15; movq %%r15,%0; movq %%r13,%5; movq %%r14,%1;" INIT_m8n12\
  "cmpq $8,%5; jb 18122f;"\
  "18121:\n\t"\
  "prefetcht0 -384(%0); subq $32,%0; subq $16,%1;" GEMM_KERNEL_k1m8n12\
                       "subq $32,%0; subq $16,%1;" GEMM_KERNEL_k1m8n12\
  "prefetcht0 -384(%0); subq $32,%0; subq $16,%1;" GEMM_KERNEL_k1m8n12\
                       "subq $32,%0; subq $16,%1;" GEMM_KERNEL_k1m8n12\
  "prefetcht0 -384(%0); subq $32,%0; subq $16,%1;" GEMM_KERNEL_k1m8n12\
                       "subq $32,%0; subq $16,%1;" GEMM_KERNEL_k1m8n12\
  "prefetcht0 -384(%0); subq $32,%0; subq $16,%1;" GEMM_KERNEL_k1m8n12\
                       "subq $32,%0; subq $16,%1;" GEMM_KERNEL_k1m8n12\
  "subq $8,%5; cmpq $8,%5; jnb 18121b;"\
  "18122:\n\t"\
  "testq %5,%5; jz 18124f;"\
  "18123:\n\t"\
  "subq $32,%0; subq $16,%1;" GEMM_KERNEL_k1m8n12 "decq %5; jnz 18123b;"\
  "18124:\n\t"
#define GEMM_RT_m4n4 GEMM_RT_SIMPLE(4,4)
#define GEMM_RT_m4n8 GEMM_RT_SIMPLE(4,8)
#define GEMM_RT_m4n12 GEMM_RT_SIMPLE(4,12)
#define GEMM_RT_m2n4 GEMM_RT_SIMPLE(2,4)
#define GEMM_RT_m2n8 GEMM_RT_SIMPLE(2,8)
#define GEMM_RT_m2n12 GEMM_RT_SIMPLE(2,12)
#define GEMM_RT_m1n4 GEMM_RT_SIMPLE(1,4)
#define GEMM_RT_m1n8 GEMM_RT_SIMPLE(1,8)
#define GEMM_RT_m1n12 GEMM_RT_SIMPLE(1,12)

#define COMPUTE(ndim) {\
  b_ptr -= (ndim-4)*K; c_ptr -= ndim * ldc;\
  __asm__ __volatile__(\
    "movq %0,%%r15; movq %6,%%r13; subq %7,%%r13; movq %6,%%r12; salq $2,%%r12; movq %1,%%r14; movq %10,%%r11;"\
    "cmpq $8,%%r11; jb "#ndim"772f;"\
    #ndim"771:\n\t"\
    GEMM_RT_m8n##ndim SOLVE_RT_m8n##ndim "subq $8,%%r11; cmpq $8,%%r11; jnb "#ndim"771b;"\
    #ndim"772:\n\t"\
    "testq $4,%%r11; jz "#ndim"773f;"\
    GEMM_RT_m4n##ndim SOLVE_RT_m4n##ndim "subq $4,%%r11;"\
    #ndim"773:\n\t"\
    "testq $2,%%r11; jz "#ndim"774f;"\
    GEMM_RT_m2n##ndim SOLVE_RT_m2n##ndim "subq $2,%%r11;"\
    #ndim"774:\n\t"\
    "testq $1,%%r11; jz "#ndim"775f;"\
    GEMM_RT_m1n##ndim SOLVE_RT_m1n##ndim "subq $1,%%r11;"\
    #ndim"775:\n\t"\
    "movq %%r15,%0; movq %%r14,%1; vzeroupper;"\
  :"+r"(a_ptr),"+r"(b_ptr),"+r"(c_ptr),"+r"(c_tmp),"+r"(ldc_bytes),"+r"(k_cnt):"m"(K),"m"(OFF),"m"(one[0]),"m"(zero[0]),"m"(M)\
  :"r11","r12","r13","r14","r15","cc","memory",\
  "xmm0","xmm1","xmm2","xmm3","xmm4","xmm5","xmm6","xmm7","xmm8","xmm9","xmm10","xmm11","xmm12","xmm13","xmm14","xmm15");\
  a_ptr -= M * K; b_ptr -= 4 * K; c_ptr -= M; OFF -= ndim;\
}

static void solve_RT(BLASLONG m, BLASLONG n, FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc){
  FLOAT a0, b0;
  int i, j, k;
  for (i=n-1;i>=0;i--) {
    b0 = b[i*n+i];
    for (j=0;j<m;j++) {
      a0 = c[i*ldc+j] * b0;
      a[i*m+j] = c[i*ldc+j] = a0;
      for (k=0;k<i;k++) c[k*ldc+j] -= a0 * b[i*n+k];
    }
  }
}
static void COMPUTE_EDGE_1_nchunk(BLASLONG m, BLASLONG n, FLOAT *sa, FLOAT *sb, FLOAT *C, BLASLONG ldc, BLASLONG k, BLASLONG offset) {
  BLASLONG m_count = m, kk = offset; FLOAT *a_ptr = sa, *c_ptr = C;
  for(;m_count>7;m_count-=8){
    if(k-kk>0) GEMM_KERNEL_N(8,n,k-kk,-1.0,a_ptr+kk*8,sb+kk*n,c_ptr,ldc);
    solve_RT(8,n,a_ptr+(kk-n)*8,sb+(kk-n)*n,c_ptr,ldc);
    a_ptr += k * 8; c_ptr += 8;
  }
  for(;m_count>3;m_count-=4){
    if(k-kk>0) GEMM_KERNEL_N(4,n,k-kk,-1.0,a_ptr+kk*4,sb+kk*n,c_ptr,ldc);
    solve_RT(4,n,a_ptr+(kk-n)*4,sb+(kk-n)*n,c_ptr,ldc);
    a_ptr += k * 4; c_ptr += 4;
  }
  for(;m_count>1;m_count-=2){
    if(k-kk>0) GEMM_KERNEL_N(2,n,k-kk,-1.0,a_ptr+kk*2,sb+kk*n,c_ptr,ldc);
    solve_RT(2,n,a_ptr+(kk-n)*2,sb+(kk-n)*n,c_ptr,ldc);
    a_ptr += k * 2; c_ptr += 2;
  }
  if(m_count>0){
    if(k-kk>0) GEMM_KERNEL_N(1,n,k-kk,-1.0,a_ptr+kk*1,sb+kk*n,c_ptr,ldc);
    solve_RT(1,n,a_ptr+(kk-n)*1,sb+(kk-n)*n,c_ptr,ldc);
    a_ptr += k * 1; c_ptr += 1;
  }
}
int CNAME(BLASLONG m, BLASLONG n, BLASLONG k, FLOAT dummy1, FLOAT *sa, FLOAT *sb, FLOAT *C, BLASLONG ldc, BLASLONG offset){
  float *a_ptr = sa, *b_ptr = sb+n*k, *c_ptr = C+n*ldc, *c_tmp = C;
  float one[8] = {1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0};
  float zero[8] = {0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0};
  uint64_t ldc_bytes = (uint64_t)ldc * sizeof(float), K = (uint64_t)k, M = (uint64_t)m, OFF = (uint64_t)(n-offset), k_cnt = 0;
  BLASLONG n_count = n;
  if(n&1){b_ptr-=k; c_ptr-=ldc; COMPUTE_EDGE_1_nchunk(m,1,a_ptr,b_ptr,c_ptr,ldc,k,OFF); OFF--; n_count--;}
  if(n&2){b_ptr-=k*2; c_ptr-=ldc*2; COMPUTE_EDGE_1_nchunk(m,2,a_ptr,b_ptr,c_ptr,ldc,k,OFF); OFF-=2; n_count-=2;}
  for(;n_count>11;n_count-=12) COMPUTE(12)
  for(;n_count>7;n_count-=8) COMPUTE(8)
  for(;n_count>3;n_count-=4) COMPUTE(4)
  return 0;
}
