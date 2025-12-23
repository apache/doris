/* %0 = "+r"(a_pointer), %1 = "+r"(b_pointer), %2 = "+r"(c_pointer), %3 = "+r"(ldc_in_bytes), %4 for k_count, %5 for c_store */
/* r10 to assist prefetch, r12 = k << 4(const), r13 = k(const), r14 = b_head_pos(const), r15 = %1 + 3r12 */

#include "common.h"
#include <stdint.h>

/* m = 16 */ /* zmm8-zmm31 for accumulators, zmm4-zmm7 for temporary use, zmm0 for alpha */
#define KERNEL_k1m16n1 \
    "vmovups (%0),%%zmm4; addq $64,%0;"\
    "vbroadcastss (%1),%%zmm6; vfmadd231ps %%zmm4,%%zmm6,%%zmm8;"\
    "addq $4,%1;"
#define KERNEL_h_k1m16n2 \
    "vmovsldup (%0),%%zmm4; vmovshdup (%0),%%zmm5; prefetcht0 512(%0); addq $64,%0;"\
    "vbroadcastsd (%1),%%zmm6; vfmadd231ps %%zmm4,%%zmm6,%%zmm8; vfmadd231ps %%zmm5,%%zmm6,%%zmm9;"
#define KERNEL_k1m16n2 KERNEL_h_k1m16n2 "addq $8,%1;"
#define KERNEL_h_k1m16n4 KERNEL_h_k1m16n2 "vbroadcastsd 8(%1),%%zmm7; vfmadd231ps %%zmm4,%%zmm7,%%zmm10; vfmadd231ps %%zmm5,%%zmm7,%%zmm11;"
#define KERNEL_k1m16n4 KERNEL_h_k1m16n4 "addq $16,%1;"
#define unit_kernel_k1m16n4(c1,c2,c3,c4, ...) \
    "vbroadcastsd  ("#__VA_ARGS__"),%%zmm6; vfmadd231ps %%zmm4,%%zmm6,"#c1"; vfmadd231ps %%zmm5,%%zmm6,"#c2";"\
    "vbroadcastsd 8("#__VA_ARGS__"),%%zmm7; vfmadd231ps %%zmm4,%%zmm7,"#c3"; vfmadd231ps %%zmm5,%%zmm7,"#c4";"
#define KERNEL_h_k1m16n8 KERNEL_h_k1m16n4 unit_kernel_k1m16n4(%%zmm12,%%zmm13,%%zmm14,%%zmm15,%1,%%r12,1)
#define KERNEL_k1m16n8 KERNEL_h_k1m16n8 "addq $16,%1;"
#define KERNEL_h_k1m16n12 KERNEL_h_k1m16n8 unit_kernel_k1m16n4(%%zmm16,%%zmm17,%%zmm18,%%zmm19,%1,%%r12,2)
#define KERNEL_k1m16n12 KERNEL_h_k1m16n12 "addq $16,%1;"
#define KERNEL_h_k1m16n16 KERNEL_k1m16n12 unit_kernel_k1m16n4(%%zmm20,%%zmm21,%%zmm22,%%zmm23,%%r15)
#define KERNEL_k1m16n16 KERNEL_h_k1m16n16 "addq $16,%%r15;"
#define KERNEL_h_k1m16n20 KERNEL_h_k1m16n16 unit_kernel_k1m16n4(%%zmm24,%%zmm25,%%zmm26,%%zmm27,%%r15,%%r12,1)
#define KERNEL_k1m16n20 KERNEL_h_k1m16n20 "addq $16,%%r15;"
#define KERNEL_h_k1m16n24 KERNEL_h_k1m16n20 unit_kernel_k1m16n4(%%zmm28,%%zmm29,%%zmm30,%%zmm31,%%r15,%%r12,2)
#define KERNEL_k1m16n24 KERNEL_h_k1m16n24 "addq $16,%%r15;"
#define INIT_m16n1 "vpxorq %%zmm8,%%zmm8,%%zmm8;"
#define INIT_m16n2 INIT_m16n1 "vpxorq %%zmm9,%%zmm9,%%zmm9;"
#define INIT_m16n4 INIT_m16n2 "vpxorq %%zmm10,%%zmm10,%%zmm10;vpxorq %%zmm11,%%zmm11,%%zmm11;"
#define unit_init_m16n4(c1,c2,c3,c4) \
    "vpxorq "#c1","#c1","#c1";vpxorq "#c2","#c2","#c2";vpxorq "#c3","#c3","#c3";vpxorq "#c4","#c4","#c4";"
#define INIT_m16n8 INIT_m16n4 unit_init_m16n4(%%zmm12,%%zmm13,%%zmm14,%%zmm15)
#define INIT_m16n12 INIT_m16n8 unit_init_m16n4(%%zmm16,%%zmm17,%%zmm18,%%zmm19)
#define INIT_m16n16 INIT_m16n12 unit_init_m16n4(%%zmm20,%%zmm21,%%zmm22,%%zmm23)
#define INIT_m16n20 INIT_m16n16 unit_init_m16n4(%%zmm24,%%zmm25,%%zmm26,%%zmm27)
#define INIT_m16n24 INIT_m16n20 unit_init_m16n4(%%zmm28,%%zmm29,%%zmm30,%%zmm31)
#define SAVE_h_m16n1 "vfmadd213ps (%2),%%zmm0,%%zmm8; vmovups %%zmm8,(%2);"
#define unit_save_m16n2(c1,c2) \
    "vunpcklps "#c2","#c1",%%zmm6; vunpckhps "#c2","#c1",%%zmm7; vunpcklpd %%zmm7,%%zmm6,%%zmm4; vunpckhpd %%zmm7,%%zmm6,%%zmm5;"\
    "vfmadd213ps (%5),%%zmm0,%%zmm4; vfmadd213ps (%5,%3,1),%%zmm0,%%zmm5;"\
    "vmovups %%zmm4,(%5); vmovups %%zmm5,(%5,%3,1); leaq (%5,%3,2),%5;"
#define SAVE_h_m16n2 "movq %2,%5;" unit_save_m16n2(%%zmm8,%%zmm9)
#define SAVE_h_m16n4  SAVE_h_m16n2  unit_save_m16n2(%%zmm10,%%zmm11)
#define SAVE_h_m16n8  SAVE_h_m16n4  unit_save_m16n2(%%zmm12,%%zmm13) unit_save_m16n2(%%zmm14,%%zmm15)
#define SAVE_h_m16n12 SAVE_h_m16n8  unit_save_m16n2(%%zmm16,%%zmm17) unit_save_m16n2(%%zmm18,%%zmm19)
#define SAVE_h_m16n16 SAVE_h_m16n12 unit_save_m16n2(%%zmm20,%%zmm21) unit_save_m16n2(%%zmm22,%%zmm23)
#define SAVE_h_m16n20 SAVE_h_m16n16 unit_save_m16n2(%%zmm24,%%zmm25) unit_save_m16n2(%%zmm26,%%zmm27)
#define SAVE_h_m16n24 SAVE_h_m16n20 unit_save_m16n2(%%zmm28,%%zmm29) unit_save_m16n2(%%zmm30,%%zmm31)
#define SAVE_m16(ndim) SAVE_h_m16n##ndim "addq $64,%2;"
#define COMPUTE_m16(ndim) \
    INIT_m16n##ndim\
    "movq %%r13,%4; movq %%r14,%1; leaq (%1,%%r12,2),%%r15; addq %%r12,%%r15; movq %2,%5; xorq %%r10,%%r10;"\
    "cmpq $16,%4; jb "#ndim"016162f;"\
    #ndim"016161:\n\t"\
    "cmpq $126,%%r10; movq $126,%%r10; cmoveq %3,%%r10;"\
    KERNEL_k1m16n##ndim\
    KERNEL_k1m16n##ndim\
    "prefetcht1 (%5); subq $63,%5; addq %%r10,%5;"\
    KERNEL_k1m16n##ndim\
    KERNEL_k1m16n##ndim\
    "prefetcht1 (%6); addq $32,%6;"\
    "subq $4,%4; cmpq $16,%4; jnb "#ndim"016161b;"\
    "movq %2,%5;"\
    #ndim"016162:\n\t"\
    "testq %4,%4; jz "#ndim"016164f;"\
    #ndim"016163:\n\t"\
    "prefetcht0 (%5); prefetcht0 63(%5); prefetcht0 (%5,%3,1); prefetcht0 63(%5,%3,1);"\
    KERNEL_k1m16n##ndim\
    "leaq (%5,%3,2),%5; decq %4; jnz "#ndim"016163b;"\
    #ndim"016164:\n\t"\
    "prefetcht0 (%%r14); prefetcht0 64(%%r14);"\
    SAVE_m16(ndim)
#define unit_save_m16n2_rscr(c1,c2,scr_off) \
    "vunpcklps "#c2","#c1",%%zmm6; vunpckhps "#c2","#c1",%%zmm7; vunpcklpd %%zmm7,%%zmm6,%%zmm4; vunpckhpd %%zmm7,%%zmm6,%%zmm5;"\
    "vmovups "#scr_off"(%7),%%zmm6; vfmadd213ps -64(%5),%%zmm0,%%zmm6; vfmadd213ps (%5),%%zmm0,%%zmm4;"\
    "vmovups %%zmm6,-64(%5); vmovups %%zmm4,(%5);"\
    "vmovups "#scr_off"+64(%7),%%zmm6; vfmadd213ps -64(%5,%3,1),%%zmm0,%%zmm6; vfmadd213ps (%5,%3,1),%%zmm0,%%zmm5;"\
    "vmovups %%zmm6,-64(%5,%3,1); vmovups %%zmm5,(%5,%3,1); leaq (%5,%3,2),%5;"
#define unit_save_m16n2_wscr(c1,c2,scr_off) \
    "vunpcklps "#c2","#c1",%%zmm6; vunpckhps "#c2","#c1",%%zmm7; vunpcklpd %%zmm7,%%zmm6,%%zmm4; vunpckhpd %%zmm7,%%zmm6,%%zmm5;"\
    "vmovups %%zmm4,"#scr_off"(%7); vmovups %%zmm5,"#scr_off"+64(%7);"
#define COMPUTE_m16n24_LSAVE \
    INIT_m16n24\
    "movq %%r13,%4; movq %%r14,%1; leaq (%1,%%r12,2),%%r15; addq %%r12,%%r15; movq %2,%5;"\
    "cmpq $16,%4; jb 24716162f; movq $16,%4;"\
    "24716161:\n\t"\
    KERNEL_k1m16n24 "addq $4,%4; testq $12,%4; movq $172,%%r10; cmovz %3,%%r10;"\
    KERNEL_k1m16n24 "prefetcht1 -64(%5); leaq -129(%5,%%r10,1),%5;"\
    KERNEL_k1m16n24 "prefetcht1 (%6); addq $32,%6; cmpq $208,%4; cmoveq %2,%5;"\
    KERNEL_k1m16n24 "cmpq %4,%%r13; jnb 24716161b;"\
    "movq %2,%5; negq %4; leaq 16(%%r13,%4,1),%4;"\
    "24716162:\n\t"\
    "testq %4,%4; jz 24716164f; movq %7,%%r10;"\
    "24716163:\n\t"\
    "prefetcht0 -64(%5); prefetcht0 (%5); prefetcht0 63(%5); addq %3,%5;"\
    KERNEL_k1m16n24 "prefetcht0 (%%r10); addq $64,%%r10; decq %4; jnz 24716163b;"\
    "24716164:\n\t"\
    "prefetcht0 (%%r14); prefetcht0 64(%%r14); movq %2,%5; addq $64,%2;"\
    unit_save_m16n2_rscr(%%zmm8,%%zmm9,0) unit_save_m16n2_rscr(%%zmm10,%%zmm11,128) unit_save_m16n2_rscr(%%zmm12,%%zmm13,256)\
    unit_save_m16n2_rscr(%%zmm14,%%zmm15,384) unit_save_m16n2_rscr(%%zmm16,%%zmm17,512) unit_save_m16n2_rscr(%%zmm18,%%zmm19,640)\
    unit_save_m16n2_wscr(%%zmm20,%%zmm21,0) unit_save_m16n2_wscr(%%zmm22,%%zmm23,128) unit_save_m16n2_wscr(%%zmm24,%%zmm25,256)\
    unit_save_m16n2_wscr(%%zmm26,%%zmm27,384) unit_save_m16n2_wscr(%%zmm28,%%zmm29,512) unit_save_m16n2_wscr(%%zmm30,%%zmm31,640)
#define COMPUTE_m16n24_RSAVE \
    INIT_m16n24 "leaq (%2,%3,8),%2; leaq (%2,%3,4),%2;"\
    "movq %%r13,%4; movq %%r14,%1; leaq (%1,%%r12,2),%%r15; addq %%r12,%%r15; movq %2,%5;"\
    "cmpq $16,%4; jb 24616162f; movq $16,%4;"\
    "24616161:\n\t"\
    KERNEL_k1m16n24 "addq $4,%4; testq $12,%4; movq $172,%%r10; cmovz %3,%%r10;"\
    KERNEL_k1m16n24 "prefetcht1 -64(%5); leaq -129(%5,%%r10,1),%5;"\
    KERNEL_k1m16n24 "prefetcht1 (%6); addq $32,%6; cmpq $208,%4; cmoveq %2,%5;"\
    KERNEL_k1m16n24 "cmpq %4,%%r13; jnb 24616161b;"\
    "movq %2,%5; negq %4; leaq 16(%%r13,%4,1),%4;"\
    "24616162:\n\t"\
    "testq %4,%4; jz 24616164f; movq %7,%%r10;"\
    "24616163:\n\t"\
    "prefetcht0 -64(%5); prefetcht0 (%5); prefetcht0 63(%5); addq %3,%5;"\
    KERNEL_k1m16n24 "prefetcht0 (%%r10); addq $64,%%r10; decq %4; jnz 24616163b;"\
    "24616164:\n\t"\
    "prefetcht0 (%%r14); prefetcht0 64(%%r14); movq %2,%5; addq $64,%2;"\
    unit_save_m16n2_rscr(%%zmm20,%%zmm21,0) unit_save_m16n2_rscr(%%zmm22,%%zmm23,128) unit_save_m16n2_rscr(%%zmm24,%%zmm25,256)\
    unit_save_m16n2_rscr(%%zmm26,%%zmm27,384) unit_save_m16n2_rscr(%%zmm28,%%zmm29,512) unit_save_m16n2_rscr(%%zmm30,%%zmm31,640)\
    unit_save_m16n2_wscr(%%zmm8,%%zmm9,0) unit_save_m16n2_wscr(%%zmm10,%%zmm11,128) unit_save_m16n2_wscr(%%zmm12,%%zmm13,256)\
    unit_save_m16n2_wscr(%%zmm14,%%zmm15,384) unit_save_m16n2_wscr(%%zmm16,%%zmm17,512) unit_save_m16n2_wscr(%%zmm18,%%zmm19,640)\
    "negq %3; leaq (%2,%3,8),%2; leaq (%2,%3,4),%2; negq %3;"
#define COMPUTE_m16n24_LINIT \
    INIT_m16n24\
    "movq %%r13,%4; movq %%r14,%1; leaq (%1,%%r12,2),%%r15; addq %%r12,%%r15; movq %2,%5;"\
    "cmpq $16,%4; jb 24516162f; movq $16,%4;"\
    "24516161:\n\t"\
    KERNEL_k1m16n24 "addq $4,%4; testq $12,%4; movq $84,%%r10; cmovz %3,%%r10;"\
    KERNEL_k1m16n24 "prefetcht1 (%5); leaq -63(%5,%%r10,1),%5;"\
    KERNEL_k1m16n24 "prefetcht1 (%6); addq $32,%6; cmpq $208,%4; cmoveq %2,%5;"\
    KERNEL_k1m16n24 "cmpq %4,%%r13; jnb 24516161b;"\
    "movq %2,%5; negq %4; leaq 16(%%r13,%4,1),%4;"\
    "24516162:\n\t"\
    "testq %4,%4; jz 24516164f; movq %7,%%r10;"\
    "24516163:\n\t"\
    "prefetcht0 (%5); prefetcht0 63(%5); addq %3,%5;"\
    KERNEL_k1m16n24 "prefetcht0 (%%r10); addq $64,%%r10; decq %4; jnz 24516163b;"\
    "24516164:\n\t"\
    "prefetcht0 (%%r14); prefetcht0 64(%%r14); movq %2,%5; addq $64,%2;"\
    unit_save_m16n2(%%zmm8,%%zmm9) unit_save_m16n2(%%zmm10,%%zmm11) unit_save_m16n2(%%zmm12,%%zmm13)\
    unit_save_m16n2(%%zmm14,%%zmm15) unit_save_m16n2(%%zmm16,%%zmm17) unit_save_m16n2(%%zmm18,%%zmm19)\
    unit_save_m16n2_wscr(%%zmm20,%%zmm21,0) unit_save_m16n2_wscr(%%zmm22,%%zmm23,128) unit_save_m16n2_wscr(%%zmm24,%%zmm25,256)\
    unit_save_m16n2_wscr(%%zmm26,%%zmm27,384) unit_save_m16n2_wscr(%%zmm28,%%zmm29,512) unit_save_m16n2_wscr(%%zmm30,%%zmm31,640)
#define COMPUTE_m16n24_LTAIL \
    INIT_m16n24\
    "movq %%r13,%4; movq %%r14,%1; leaq (%1,%%r12,2),%%r15; addq %%r12,%%r15; movq %2,%5;"\
    "cmpq $16,%4; jb 24416162f; movq $16,%4;"\
    "24416161:\n\t"\
    KERNEL_k1m16n24 "addq $4,%4; testq $4,%4; movq $126,%%r10; cmovz %3,%%r10;"\
    KERNEL_k1m16n24 "prefetcht1 -64(%5); prefetcht1 (%5); leaq -63(%5,%%r10,1),%5;"\
    KERNEL_k1m16n24 "prefetcht1 (%6); addq $32,%6; cmpq $208,%4; cmoveq %2,%5;"\
    KERNEL_k1m16n24 "cmpq %4,%%r13; jnb 24416161b;"\
    "movq %2,%5; negq %4; leaq 16(%%r13,%4,1),%4;"\
    "24416162:\n\t"\
    "testq %4,%4; jz 24416164f; movq %7,%%r10;"\
    "24416163:\n\t"\
    "prefetcht0 -64(%5); prefetcht0 (%5); prefetcht0 63(%5); prefetcht0 -64(%5,%3,1); prefetcht0 (%5,%3,1); prefetcht0 63(%5,%3,1); leaq (%5,%3,2),%5;"\
    KERNEL_k1m16n24 "prefetcht0 (%%r10); addq $64,%%r10; decq %4; jnz 24416163b;"\
    "24416164:\n\t"\
    "prefetcht0 (%%r14); prefetcht0 64(%%r14); movq %2,%5; addq $64,%2;"\
    unit_save_m16n2_rscr(%%zmm8,%%zmm9,0) unit_save_m16n2_rscr(%%zmm10,%%zmm11,128) unit_save_m16n2_rscr(%%zmm12,%%zmm13,256)\
    unit_save_m16n2_rscr(%%zmm14,%%zmm15,384) unit_save_m16n2_rscr(%%zmm16,%%zmm17,512) unit_save_m16n2_rscr(%%zmm18,%%zmm19,640)\
    unit_save_m16n2(%%zmm20,%%zmm21) unit_save_m16n2(%%zmm22,%%zmm23) unit_save_m16n2(%%zmm24,%%zmm25)\
    unit_save_m16n2(%%zmm26,%%zmm27) unit_save_m16n2(%%zmm28,%%zmm29) unit_save_m16n2(%%zmm30,%%zmm31)
#define COMPUTE_m16n24_RTAIL \
    INIT_m16n24\
    "movq %%r13,%4; movq %%r14,%1; leaq (%1,%%r12,2),%%r15; addq %%r12,%%r15; movq %2,%5;"\
    "cmpq $16,%4; jb 24416162f; movq $16,%4;"\
    "24416161:\n\t"\
    KERNEL_k1m16n24 "addq $4,%4; testq $4,%4; movq $126,%%r10; cmovz %3,%%r10;"\
    KERNEL_k1m16n24 "prefetcht1 -64(%5); prefetcht1 (%5); leaq -63(%5,%%r10,1),%5;"\
    KERNEL_k1m16n24 "prefetcht1 (%6); addq $32,%6; cmpq $208,%4; cmoveq %2,%5;"\
    KERNEL_k1m16n24 "cmpq %4,%%r13; jnb 24416161b;"\
    "movq %2,%5; negq %4; leaq 16(%%r13,%4,1),%4;"\
    "24416162:\n\t"\
    "testq %4,%4; jz 24416164f; movq %7,%%r10;"\
    "24416163:\n\t"\
    "prefetcht0 -64(%5); prefetcht0 (%5); prefetcht0 63(%5); prefetcht0 -64(%5,%3,1); prefetcht0 (%5,%3,1); prefetcht0 63(%5,%3,1); leaq (%5,%3,2),%5;"\
    KERNEL_k1m16n24 "prefetcht0 (%%r10); addq $64,%%r10; decq %4; jnz 24416163b;"\
    "24416164:\n\t"\
    "prefetcht0 (%%r14); prefetcht0 64(%%r14); movq %2,%5; addq $64,%2;"\
    unit_save_m16n2(%%zmm8,%%zmm9) unit_save_m16n2(%%zmm10,%%zmm11) unit_save_m16n2(%%zmm12,%%zmm13)\
    unit_save_m16n2(%%zmm14,%%zmm15) unit_save_m16n2(%%zmm16,%%zmm17) unit_save_m16n2(%%zmm18,%%zmm19)\
    unit_save_m16n2_rscr(%%zmm20,%%zmm21,0) unit_save_m16n2_rscr(%%zmm22,%%zmm23,128) unit_save_m16n2_rscr(%%zmm24,%%zmm25,256)\
    unit_save_m16n2_rscr(%%zmm26,%%zmm27,384) unit_save_m16n2_rscr(%%zmm28,%%zmm29,512) unit_save_m16n2_rscr(%%zmm30,%%zmm31,640)

/* m = 8 *//* zmm0 for alpha, zmm1-2 for perm words, zmm4-7 for temporary use, zmm8-19 for accumulators */
#define KERNEL_k1m8n1 \
    "vbroadcastss (%1),%%ymm4; addq $4,%1; vfmadd231ps (%0),%%ymm4,%%ymm8; addq $32,%0;"
#define KERNEL_k1m8n2 \
    "vmovups (%0),%%ymm4; addq $32,%0;"\
    "vbroadcastss (%1),%%ymm5; vfmadd231ps %%ymm5,%%ymm4,%%ymm8;"\
    "vbroadcastss 4(%1),%%ymm6; vfmadd231ps %%ymm6,%%ymm4,%%ymm9; addq $8,%1;"
#define unit_kernel_k1m8n4(c1,c2,...)\
    "vbroadcastf32x4 ("#__VA_ARGS__"),%%zmm7; vfmadd231ps %%zmm7,%%zmm4,"#c1"; vfmadd231ps %%zmm7,%%zmm5,"#c2";"
#define KERNEL_h_k1m8n4 \
    "vbroadcastf32x4 (%0),%%zmm4; vpermilps %%zmm2,%%zmm4,%%zmm4; vbroadcastf32x4 16(%0),%%zmm5; vpermilps %%zmm2,%%zmm5,%%zmm5; addq $32,%0;"\
    unit_kernel_k1m8n4(%%zmm8,%%zmm9,%1)
#define KERNEL_k1m8n4 KERNEL_h_k1m8n4 "addq $16,%1;"
#define KERNEL_h_k1m8n8 KERNEL_h_k1m8n4 unit_kernel_k1m8n4(%%zmm10,%%zmm11,%1,%%r12,1)
#define KERNEL_k1m8n8 KERNEL_h_k1m8n8 "addq $16,%1;"
#define KERNEL_k1m8n12 KERNEL_h_k1m8n8 unit_kernel_k1m8n4(%%zmm12,%%zmm13,%1,%%r12,2) "addq $16,%1;"
#define KERNEL_h_k1m8n16 KERNEL_k1m8n12 unit_kernel_k1m8n4(%%zmm14,%%zmm15,%%r15)
#define KERNEL_k1m8n16 KERNEL_h_k1m8n16 "addq $16,%%r15;"
#define KERNEL_h_k1m8n20 KERNEL_h_k1m8n16 unit_kernel_k1m8n4(%%zmm16,%%zmm17,%%r15,%%r12,1)
#define KERNEL_k1m8n20 KERNEL_h_k1m8n20 "addq $16,%%r15;"
#define KERNEL_k1m8n24 KERNEL_h_k1m8n20 unit_kernel_k1m8n4(%%zmm18,%%zmm19,%%r15,%%r12,2) "addq $16,%%r15;"
#define INIT_m8n1 "vpxor %%ymm8,%%ymm8,%%ymm8;"
#define INIT_m8n2 "vpxor %%ymm8,%%ymm8,%%ymm8; vpxor %%ymm9,%%ymm9,%%ymm9;"
#define unit_init_m8n4(c1,c2) "vpxorq "#c1","#c1","#c1";vpxorq "#c2","#c2","#c2";"
#define INIT_m8n4 unit_init_m8n4(%%zmm8,%%zmm9)
#define INIT_m8n8 INIT_m8n4 unit_init_m8n4(%%zmm10,%%zmm11)
#define INIT_m8n12 INIT_m8n8 unit_init_m8n4(%%zmm12,%%zmm13)
#define INIT_m8n16 INIT_m8n12 unit_init_m8n4(%%zmm14,%%zmm15)
#define INIT_m8n20 INIT_m8n16 unit_init_m8n4(%%zmm16,%%zmm17)
#define INIT_m8n24 INIT_m8n20 unit_init_m8n4(%%zmm18,%%zmm19)
#define SAVE_h_m8n1 "vfmadd213ps (%2),%%ymm0,%%ymm8; vmovups %%ymm8,(%2);"
#define SAVE_h_m8n2 \
    "vfmadd213ps (%2),%%ymm0,%%ymm8; vmovups %%ymm8,(%2);"\
    "vfmadd213ps (%2,%3,1),%%ymm0,%%ymm9; vmovups %%ymm9,(%2,%3,1);"
#define unit_save_m8n4(c1_no,c2_no)\
    "vpermps %%zmm"#c1_no",%%zmm1,%%zmm"#c1_no"; vpermps %%zmm"#c2_no",%%zmm1,%%zmm"#c2_no";"\
    "vextractf64x4 $1,%%zmm"#c1_no",%%ymm5; vextractf64x4 $1,%%zmm"#c2_no",%%ymm6;"\
    "vmovups (%5),%%xmm4; vinsertf128 $1,(%5,%3,1),%%ymm4,%%ymm4; vfmadd231ps %%ymm"#c1_no",%%ymm0,%%ymm4;"\
    "vmovups %%xmm4,(%5); vextractf128 $1,%%ymm4,(%5,%3,1);"\
    "vmovups 16(%5),%%xmm4; vinsertf128 $1,16(%5,%3,1),%%ymm4,%%ymm4; vfmadd231ps %%ymm"#c2_no",%%ymm0,%%ymm4;"\
    "vmovups %%xmm4,16(%5); vextractf128 $1,%%ymm4,16(%5,%3,1); leaq (%5,%3,2),%5;"\
    "vmovups (%5),%%xmm4; vinsertf128 $1,(%5,%3,1),%%ymm4,%%ymm4; vfmadd231ps %%ymm5,%%ymm0,%%ymm4;"\
    "vmovups %%xmm4,(%5); vextractf128 $1,%%ymm4,(%5,%3,1);"\
    "vmovups 16(%5),%%xmm4; vinsertf128 $1,16(%5,%3,1),%%ymm4,%%ymm4; vfmadd231ps %%ymm6,%%ymm0,%%ymm4;"\
    "vmovups %%xmm4,16(%5); vextractf128 $1,%%ymm4,16(%5,%3,1); leaq (%5,%3,2),%5;"
#define SAVE_h_m8n4 "movq %2,%5;" unit_save_m8n4(8,9)
#define SAVE_h_m8n8 SAVE_h_m8n4 unit_save_m8n4(10,11)
#define SAVE_h_m8n12 SAVE_h_m8n8 unit_save_m8n4(12,13)
#define SAVE_h_m8n16 SAVE_h_m8n12 unit_save_m8n4(14,15)
#define SAVE_h_m8n20 SAVE_h_m8n16 unit_save_m8n4(16,17)
#define SAVE_h_m8n24 SAVE_h_m8n20 unit_save_m8n4(18,19)
#define SAVE_m8(ndim) SAVE_h_m8n##ndim "addq $32,%2;"
#define COMPUTE_m8(ndim) \
    INIT_m8n##ndim\
    "movq %%r13,%4; movq %%r14,%1; leaq (%1,%%r12,2),%%r15; addq %%r12,%%r15;"\
    "testq %4,%4; jz "#ndim"008082f;"\
    #ndim"008081:\n\t"\
    KERNEL_k1m8n##ndim "decq %4; jnz "#ndim"008081b;"\
    #ndim"008082:\n\t"\
    SAVE_m8(ndim)

/* m = 4 *//* zmm0 for alpha, zmm1-2 for perm words, zmm4-7 for temporary use, zmm8-15 for accumulators */
#define KERNEL_k1m4n1 "vbroadcastss (%1),%%xmm4; addq $4,%1; vfmadd231ps (%0),%%xmm4,%%xmm8; addq $16,%0;"
#define KERNEL_k1m4n2 "vmovups (%0),%%xmm4; addq $16,%0;"\
    "vbroadcastss (%1),%%xmm5; vfmadd231ps %%xmm5,%%xmm4,%%xmm8;"\
    "vbroadcastss 4(%1),%%xmm5; vfmadd231ps %%xmm5,%%xmm4,%%xmm9; addq $8,%1;"
#define unit_kernel_k1m4n4(c1,...) "vbroadcastf32x4 ("#__VA_ARGS__"),%%zmm7; vfmadd231ps %%zmm7,%%zmm4,"#c1";"
#define KERNEL_h_k1m4n4 "vbroadcastf32x4 (%0),%%zmm4; vpermilps %%zmm2,%%zmm4,%%zmm4; addq $16,%0;" unit_kernel_k1m4n4(%%zmm8,%1)
#define KERNEL_k1m4n4 KERNEL_h_k1m4n4 "addq $16,%1;"
#define KERNEL_h_k1m4n8 KERNEL_h_k1m4n4 unit_kernel_k1m4n4(%%zmm9,%1,%%r12,1)
#define KERNEL_k1m4n8 KERNEL_h_k1m4n8 "addq $16,%1;"
#define KERNEL_k1m4n12 KERNEL_h_k1m4n8 unit_kernel_k1m4n4(%%zmm10,%1,%%r12,2) "addq $16,%1;"
#define KERNEL_h_k1m4n16 KERNEL_k1m4n12 unit_kernel_k1m4n4(%%zmm11,%%r15)
#define KERNEL_k1m4n16 KERNEL_h_k1m4n16 "addq $16,%%r15;"
#define KERNEL_h_k1m4n20 KERNEL_h_k1m4n16 unit_kernel_k1m4n4(%%zmm12,%%r15,%%r12,1)
#define KERNEL_k1m4n20 KERNEL_h_k1m4n20 "addq $16,%%r15;"
#define KERNEL_h_k1m4n24 KERNEL_h_k1m4n20 unit_kernel_k1m4n4(%%zmm13,%%r15,%%r12,2)
#define KERNEL_k1m4n24 KERNEL_h_k1m4n24 "addq $16,%%r15;"
#define INIT_m4n1 "vpxor %%xmm8,%%xmm8,%%xmm8;"
#define INIT_m4n2 "vpxor %%xmm8,%%xmm8,%%xmm8; vpxor %%xmm9,%%xmm9,%%xmm9;"
#define INIT_m4n4 "vpxorq %%zmm8,%%zmm8,%%zmm8;"
#define INIT_m4n8 INIT_m4n4 "vpxorq %%zmm9,%%zmm9,%%zmm9;"
#define INIT_m4n12 INIT_m4n8 "vpxorq %%zmm10,%%zmm10,%%zmm10;"
#define INIT_m4n16 INIT_m4n12 "vpxorq %%zmm11,%%zmm11,%%zmm11;"
#define INIT_m4n20 INIT_m4n16 "vpxorq %%zmm12,%%zmm12,%%zmm12;"
#define INIT_m4n24 INIT_m4n20 "vpxorq %%zmm13,%%zmm13,%%zmm13;"
#define SAVE_h_m4n1 "vfmadd213ps (%2),%%xmm0,%%xmm8; vmovups %%xmm8,(%2);"
#define SAVE_h_m4n2 "vfmadd213ps (%2),%%xmm0,%%xmm8; vmovups %%xmm8,(%2); vfmadd213ps (%2,%3,1),%%xmm0,%%xmm9; vmovups %%xmm9,(%2,%3,1);"
#define unit_save_m4n4(c1_no)\
    "vpermps %%zmm"#c1_no",%%zmm1,%%zmm"#c1_no"; vextractf64x4 $1,%%zmm"#c1_no",%%ymm5;"\
    "vmovups (%5),%%xmm4; vinsertf128 $1,(%5,%3,1),%%ymm4,%%ymm4; vfmadd231ps %%ymm0,%%ymm"#c1_no",%%ymm4;"\
    "vmovups %%xmm4,(%5); vextractf128 $1,%%ymm4,(%5,%3,1); leaq (%5,%3,2),%5;"\
    "vmovups (%5),%%xmm4; vinsertf128 $1,(%5,%3,1),%%ymm4,%%ymm4; vfmadd231ps %%ymm0,%%ymm5,%%ymm4;"\
    "vmovups %%xmm4,(%5); vextractf128 $1,%%ymm4,(%5,%3,1); leaq (%5,%3,2),%5;"
#define SAVE_h_m4n4 "movq %2,%5;" unit_save_m4n4(8)
#define SAVE_h_m4n8 SAVE_h_m4n4 unit_save_m4n4(9)
#define SAVE_h_m4n12 SAVE_h_m4n8 unit_save_m4n4(10)
#define SAVE_h_m4n16 SAVE_h_m4n12 unit_save_m4n4(11)
#define SAVE_h_m4n20 SAVE_h_m4n16 unit_save_m4n4(12)
#define SAVE_h_m4n24 SAVE_h_m4n20 unit_save_m4n4(13)
#define SAVE_m4(ndim) SAVE_h_m4n##ndim "addq $16,%2;"
#define COMPUTE_m4(ndim) \
    INIT_m4n##ndim\
    "movq %%r13,%4; movq %%r14,%1; leaq (%1,%%r12,2),%%r15; addq %%r12,%%r15;"\
    "testq %4,%4; jz "#ndim"004042f;"\
    #ndim"004041:\n\t"\
    KERNEL_k1m4n##ndim "decq %4; jnz "#ndim"004041b;"\
    #ndim"004042:\n\t"\
    SAVE_m4(ndim)

/* m = 2 *//* xmm0 for alpha, xmm1-xmm3 for temporary use, xmm4-xmm15 for accumulators */
#define INIT_m2n1 "vpxor %%xmm4,%%xmm4,%%xmm4;"
#define KERNEL_k1m2n1 \
    "vmovsd (%0),%%xmm1; addq $8,%0;"\
    "vbroadcastss (%1),%%xmm2; vfmadd231ps %%xmm1,%%xmm2,%%xmm4;"\
    "addq $4,%1;"
#define SAVE_h_m2n1 "vmovsd (%2),%%xmm1; vfmadd213ps %%xmm1,%%xmm0,%%xmm4; vmovsd %%xmm4,(%2);"
#define INIT_m2n2 INIT_m2n1 "vpxor %%xmm5,%%xmm5,%%xmm5;"
#define KERNEL_k1m2n2 \
    "vmovsd (%0),%%xmm1; addq $8,%0;"\
    "vbroadcastss  (%1),%%xmm2; vfmadd231ps %%xmm1,%%xmm2,%%xmm4;"\
    "vbroadcastss 4(%1),%%xmm3; vfmadd231ps %%xmm1,%%xmm3,%%xmm5;"\
    "addq $8,%1;"
#define SAVE_h_m2n2 SAVE_h_m2n1 "vmovsd (%2,%3,1),%%xmm1; vfmadd213ps %%xmm1,%%xmm0,%%xmm5; vmovsd %%xmm5,(%2,%3,1);"
#define INIT_m2n4  INIT_m2n2
#define INIT_m2n8  INIT_m2n4 "vpxor %%xmm6,%%xmm6,%%xmm6; vpxor %%xmm7,%%xmm7,%%xmm7;"
#define INIT_m2n12 INIT_m2n8 "vpxor %%xmm8,%%xmm8,%%xmm8; vpxor %%xmm9,%%xmm9,%%xmm9;"
#define INIT_m2n16 INIT_m2n12 "vpxor %%xmm10,%%xmm10,%%xmm10; vpxor %%xmm11,%%xmm11,%%xmm11;"
#define INIT_m2n20 INIT_m2n16 "vpxor %%xmm12,%%xmm12,%%xmm12; vpxor %%xmm13,%%xmm13,%%xmm13;"
#define INIT_m2n24 INIT_m2n20 "vpxor %%xmm14,%%xmm14,%%xmm14; vpxor %%xmm15,%%xmm15,%%xmm15;"
#define KERNEL_h_k1m2n4 \
    "vbroadcastss (%0),%%xmm1; vbroadcastss 4(%0),%%xmm2; addq $8,%0;"\
    "vmovups (%1),%%xmm3; vfmadd231ps %%xmm1,%%xmm3,%%xmm4; vfmadd231ps %%xmm2,%%xmm3,%%xmm5;"
#define KERNEL_k1m2n4 KERNEL_h_k1m2n4 "addq $16,%1;"
#define KERNEL_h_k1m2n8 KERNEL_h_k1m2n4 "vmovups (%1,%%r12,1),%%xmm3; vfmadd231ps %%xmm1,%%xmm3,%%xmm6; vfmadd231ps %%xmm2,%%xmm3,%%xmm7;"
#define KERNEL_k1m2n8 KERNEL_h_k1m2n8 "addq $16,%1;"
#define KERNEL_k1m2n12 KERNEL_h_k1m2n8 \
    "vmovups (%1,%%r12,2),%%xmm3; vfmadd231ps %%xmm1,%%xmm3,%%xmm8; vfmadd231ps %%xmm2,%%xmm3,%%xmm9; addq $16,%1;"
#define KERNEL_h_k1m2n16 KERNEL_k1m2n12 "vmovups (%%r15),%%xmm3; vfmadd231ps %%xmm1,%%xmm3,%%xmm10; vfmadd231ps %%xmm2,%%xmm3,%%xmm11;"
#define KERNEL_k1m2n16 KERNEL_h_k1m2n16 "addq $16,%%r15;"
#define KERNEL_h_k1m2n20 KERNEL_h_k1m2n16 "vmovups (%%r15,%%r12,1),%%xmm3; vfmadd231ps %%xmm1,%%xmm3,%%xmm12; vfmadd231ps %%xmm2,%%xmm3,%%xmm13;"
#define KERNEL_k1m2n20 KERNEL_h_k1m2n20 "addq $16,%%r15;"
#define KERNEL_h_k1m2n24 KERNEL_h_k1m2n20 "vmovups (%%r15,%%r12,2),%%xmm3; vfmadd231ps %%xmm1,%%xmm3,%%xmm14; vfmadd231ps %%xmm2,%%xmm3,%%xmm15;"
#define KERNEL_k1m2n24 KERNEL_h_k1m2n24 "addq $16,%%r15;"
#define unit_save_m2n4(c1,c2) \
    "vunpcklps "#c2","#c1",%%xmm1; vunpckhps "#c2","#c1",%%xmm2;"\
    "vmovsd (%5),%%xmm3; vmovhpd (%5,%3,1),%%xmm3,%%xmm3; vfmadd213ps %%xmm3,%%xmm0,%%xmm1; vmovsd %%xmm1,(%5); vmovhpd %%xmm1,(%5,%3,1);"\
    "leaq (%5,%3,2),%5;"\
    "vmovsd (%5),%%xmm3; vmovhpd (%5,%3,1),%%xmm3,%%xmm3; vfmadd213ps %%xmm3,%%xmm0,%%xmm2; vmovsd %%xmm2,(%5); vmovhpd %%xmm2,(%5,%3,1);"\
    "leaq (%5,%3,2),%5;"
#define SAVE_h_m2n4  "movq %2,%5;" unit_save_m2n4(%%xmm4,%%xmm5)
#define SAVE_h_m2n8  SAVE_h_m2n4   unit_save_m2n4(%%xmm6,%%xmm7)
#define SAVE_h_m2n12 SAVE_h_m2n8   unit_save_m2n4(%%xmm8,%%xmm9)
#define SAVE_h_m2n16 SAVE_h_m2n12  unit_save_m2n4(%%xmm10,%%xmm11)
#define SAVE_h_m2n20 SAVE_h_m2n16  unit_save_m2n4(%%xmm12,%%xmm13)
#define SAVE_h_m2n24 SAVE_h_m2n20  unit_save_m2n4(%%xmm14,%%xmm15)
#define SAVE_m2(ndim) SAVE_h_m2n##ndim "addq $8,%2;"
#define COMPUTE_m2(ndim) \
    INIT_m2n##ndim\
    "movq %%r13,%4; movq %%r14,%1; leaq (%1,%%r12,2),%%r15; addq %%r12,%%r15;"\
    "testq %4,%4; jz "#ndim"002022f;"\
    #ndim"002021:\n\t"\
    KERNEL_k1m2n##ndim "decq %4; jnz "#ndim"002021b;"\
    #ndim"002022:\n\t"\
    SAVE_m2(ndim)

/* m = 1 *//* xmm0 for alpha, xmm1-xmm3 and xmm10 for temporary use, xmm4-xmm9 for accumulators */
#define INIT_m1n1 "vpxor %%xmm4,%%xmm4,%%xmm4;"
#define KERNEL_k1m1n1 \
    "vmovss (%1),%%xmm3; addq $4,%1;"\
    "vmovss (%0),%%xmm1; vfmadd231ss %%xmm3,%%xmm1,%%xmm4;"\
    "addq $4,%0;"
#define SAVE_h_m1n1 "vfmadd213ss (%2),%%xmm0,%%xmm4; vmovss %%xmm4,(%2);"
#define INIT_m1n2 INIT_m1n1
#define KERNEL_k1m1n2 \
    "vmovsd (%1),%%xmm3; addq $8,%1;"\
    "vbroadcastss  (%0),%%xmm1; vfmadd231ps %%xmm3,%%xmm1,%%xmm4;"\
    "addq $4,%0;"
#define SAVE_h_m1n2 \
    "vmovss (%2),%%xmm3; vinsertps $16,(%2,%3,1),%%xmm3,%%xmm3; vfmadd213ps %%xmm3,%%xmm0,%%xmm4;"\
    "vmovss %%xmm4,(%2); vextractps $1,%%xmm4,(%2,%3,1);"
#define INIT_m1n4  INIT_m1n2
#define INIT_m1n8  INIT_m1n4 "vpxor %%xmm5,%%xmm5,%%xmm5;"
#define INIT_m1n12 INIT_m1n8 "vpxor %%xmm6,%%xmm6,%%xmm6;"
#define INIT_m1n16 INIT_m1n12 "vpxor %%xmm7,%%xmm7,%%xmm7;"
#define INIT_m1n20 INIT_m1n16 "vpxor %%xmm8,%%xmm8,%%xmm8;"
#define INIT_m1n24 INIT_m1n20 "vpxor %%xmm9,%%xmm9,%%xmm9;"
#define KERNEL_h_k1m1n4 \
    "vbroadcastss (%0),%%xmm1; addq $4,%0; vfmadd231ps (%1),%%xmm1,%%xmm4;"
#define KERNEL_k1m1n4 KERNEL_h_k1m1n4 "addq $16,%1;"
#define KERNEL_h_k1m1n8 KERNEL_h_k1m1n4 "vfmadd231ps (%1,%%r12,1),%%xmm1,%%xmm5;"
#define KERNEL_k1m1n8 KERNEL_h_k1m1n8 "addq $16,%1;"
#define KERNEL_k1m1n12 KERNEL_h_k1m1n8 "vfmadd231ps (%1,%%r12,2),%%xmm1,%%xmm6; addq $16,%1;"
#define KERNEL_h_k1m1n16 KERNEL_k1m1n12 "vfmadd231ps (%%r15),%%xmm1,%%xmm7;"
#define KERNEL_k1m1n16 KERNEL_h_k1m1n16 "addq $16,%%r15;"
#define KERNEL_h_k1m1n20 KERNEL_h_k1m1n16 "vfmadd231ps (%%r15,%%r12,1),%%xmm1,%%xmm8;"
#define KERNEL_k1m1n20 KERNEL_h_k1m1n20 "addq $16,%%r15;"
#define KERNEL_h_k1m1n24 KERNEL_h_k1m1n20 "vfmadd231ps (%%r15,%%r12,2),%%xmm1,%%xmm9;"
#define KERNEL_k1m1n24 KERNEL_h_k1m1n24 "addq $16,%%r15;"
#define unit_save_m1n4(c1) \
    "vpxor %%xmm10,%%xmm10,%%xmm10; vmovsd "#c1",%%xmm10,%%xmm2; vmovhlps "#c1",%%xmm10,%%xmm1;"\
    "vmovss (%5),%%xmm3; vinsertps $16,(%5,%3,1),%%xmm3,%%xmm3; vfmadd213ps %%xmm3,%%xmm0,%%xmm2;"\
    "vmovss %%xmm2,(%5); vextractps $1,%%xmm2,(%5,%3,1); leaq (%5,%3,2),%5;"\
    "vmovss (%5),%%xmm3; vinsertps $16,(%5,%3,1),%%xmm3,%%xmm3; vfmadd213ps %%xmm3,%%xmm0,%%xmm1;"\
    "vmovss %%xmm1,(%5); vextractps $1,%%xmm1,(%5,%3,1); leaq (%5,%3,2),%5;"
#define SAVE_h_m1n4 "movq %2,%5;" unit_save_m1n4(%%xmm4)
#define SAVE_h_m1n8  SAVE_h_m1n4  unit_save_m1n4(%%xmm5)
#define SAVE_h_m1n12 SAVE_h_m1n8  unit_save_m1n4(%%xmm6)
#define SAVE_h_m1n16 SAVE_h_m1n12 unit_save_m1n4(%%xmm7)
#define SAVE_h_m1n20 SAVE_h_m1n16 unit_save_m1n4(%%xmm8)
#define SAVE_h_m1n24 SAVE_h_m1n20 unit_save_m1n4(%%xmm9)
#define SAVE_m1(ndim) SAVE_h_m1n##ndim "addq $4,%2;"
#define COMPUTE_m1(ndim) \
    INIT_m1n##ndim\
    "movq %%r13,%4; movq %%r14,%1; leaq (%1,%%r12,2),%%r15; addq %%r12,%%r15;"\
    "testq %4,%4; jz "#ndim"001012f;"\
    #ndim"001011:\n\t"\
    KERNEL_k1m1n##ndim "decq %4; jnz "#ndim"001011b;"\
    #ndim"001012:\n\t"\
    SAVE_m1(ndim)

/* %0 = "+r"(a_pointer), %1 = "+r"(b_pointer), %2 = "+r"(c_pointer), %3 = "+r"(ldc_in_bytes), %4 = "+r"(K), %5 = "+r"(ctemp) */
/* %6 = "+r"(next_b), %7 = "m"(ALPHA), %8 = "m"(M) */
/* r11 = m_counter, r12 = k << 4(const), r13 = k(const), r14 = b_head_pos(const), r15 = %1 + 3r12 */

#define COMPUTE(ndim) {\
    next_b = b_pointer + ndim * K;\
    __asm__ __volatile__(\
    "vbroadcastss %7,%%zmm0; vmovups %9,%%zmm1; vmovups %10,%%zmm2;"\
    "movq %4,%%r13; movq %4,%%r12; salq $4,%%r12; movq %1,%%r14; movq %8,%%r11;"\
    "cmpq $16,%%r11;jb 33101"#ndim"f;"\
    "33109"#ndim":\n\t"\
    COMPUTE_m16(ndim)\
    "subq $16,%%r11;cmpq $16,%%r11;jnb 33109"#ndim"b;"\
    "33101"#ndim":\n\t"\
    "cmpq $8,%%r11;jb 33102"#ndim"f;"\
    COMPUTE_m8(ndim)\
    "subq $8,%%r11;"\
    "33102"#ndim":\n\t"\
    "cmpq $4,%%r11;jb 33103"#ndim"f;"\
    COMPUTE_m4(ndim)\
    "subq $4,%%r11;"\
    "33103"#ndim":\n\t"\
    "cmpq $2,%%r11;jb 33104"#ndim"f;"\
    COMPUTE_m2(ndim)\
    "subq $2,%%r11;"\
    "33104"#ndim":\n\t"\
    "testq %%r11,%%r11;jz 33105"#ndim"f;"\
    COMPUTE_m1(ndim)\
    "33105"#ndim":\n\t"\
    "movq %%r13,%4; movq %%r14,%1; vzeroupper;"\
    :"+r"(a_pointer),"+r"(b_pointer),"+r"(c_pointer),"+r"(ldc_in_bytes),"+r"(K),"+r"(ctemp),"+r"(next_b):"m"(ALPHA),"m"(M),"m"(perm[0]),"m"(permil[0])\
    :"r10","r11","r12","r13","r14","r15","zmm0","zmm1","zmm2","zmm3","zmm4","zmm5","zmm6","zmm7","zmm8","zmm9","zmm10","zmm11","zmm12","zmm13","zmm14",\
    "zmm15","zmm16","zmm17","zmm18","zmm19","zmm20","zmm21","zmm22","zmm23","zmm24","zmm25","zmm26","zmm27","zmm28","zmm29","zmm30","zmm31",\
    "cc","memory");\
    a_pointer -= M * K; b_pointer += ndim * K; c_pointer += LDC * ndim - M;\
}

#define COMPUTE_n24 {\
    next_b = b_pointer + 24 * K;\
    __asm__ __volatile__(\
    "vbroadcastss %8,%%zmm0; vmovups %10,%%zmm1; vmovups %11,%%zmm2;"\
    "movq %4,%%r13; movq %4,%%r12; salq $4,%%r12; movq %1,%%r14; movq %9,%%r11;"\
    "cmpq $32,%%r11;jb 3310024f;"\
    COMPUTE_m16n24_LINIT "subq $16,%%r11; cmpq $32,%%r11;jb 3310724f;"\
    "3310924:\n\t"\
    COMPUTE_m16n24_RSAVE "subq $16,%%r11; cmpq $32,%%r11;jb 3310824f;"\
    COMPUTE_m16n24_LSAVE "subq $16,%%r11; cmpq $32,%%r11;jnb 3310924b;"\
    "3310724:\n\t"\
    COMPUTE_m16n24_RTAIL "subq $16,%%r11; jmp 3310124f;"\
    "3310824:\n\t"\
    COMPUTE_m16n24_LTAIL "subq $16,%%r11; jmp 3310124f;"\
    "3310024:\n\t"\
    "cmpq $16,%%r11;jb 3310124f;"\
    COMPUTE_m16(24)\
    "subq $16,%%r11;"\
    "3310124:\n\t"\
    "cmpq $8,%%r11;jb 3310224f;"\
    COMPUTE_m8(24)\
    "subq $8,%%r11;"\
    "3310224:\n\t"\
    "cmpq $4,%%r11;jb 3310324f;"\
    COMPUTE_m4(24)\
    "subq $4,%%r11;"\
    "3310324:\n\t"\
    "cmpq $2,%%r11;jb 3310424f;"\
    COMPUTE_m2(24)\
    "subq $2,%%r11;"\
    "3310424:\n\t"\
    "testq %%r11,%%r11;jz 3310524f;"\
    COMPUTE_m1(24)\
    "3310524:\n\t"\
    "movq %%r13,%4; movq %%r14,%1; vzeroupper;"\
    :"+r"(a_pointer),"+r"(b_pointer),"+r"(c_pointer),"+r"(ldc_in_bytes),"+r"(K),"+r"(ctemp),"+r"(next_b),"+r"(wscr):"m"(ALPHA),"m"(M),"m"(perm[0]),"m"(permil[0])\
    :"r10","r11","r12","r13","r14","r15","zmm0","zmm1","zmm2","zmm3","zmm4","zmm5","zmm6","zmm7","zmm8","zmm9","zmm10","zmm11","zmm12","zmm13","zmm14",\
    "zmm15","zmm16","zmm17","zmm18","zmm19","zmm20","zmm21","zmm22","zmm23","zmm24","zmm25","zmm26","zmm27","zmm28","zmm29","zmm30","zmm31",\
    "cc","memory");\
    a_pointer -= M * K; b_pointer += 24 * K; c_pointer += LDC * 24 - M;\
}

int __attribute__ ((noinline))
CNAME(BLASLONG m, BLASLONG n, BLASLONG k, float alpha, float * __restrict__ A, float * __restrict__ B, float * __restrict__ C, BLASLONG LDC)
{
    if(m==0||n==0||k==0||alpha==(float)0.0) return 0;
    float scr[192]; float *wscr = scr;
    int64_t ldc_in_bytes = (int64_t)LDC * sizeof(float);float ALPHA = alpha;
    int64_t M = (int64_t)m, K = (int64_t)k;
    int32_t perm[16] = {0,4,8,12,1,5,9,13,2,6,10,14,3,7,11,15};
    int32_t permil[16] = {0,0,0,0,1,1,1,1,2,2,2,2,3,3,3,3};
    BLASLONG n_count = n;
    float *a_pointer = A,*b_pointer = B,*c_pointer = C,*ctemp = C,*next_b = B;
#if defined(__clang__)
    for(;n_count>23;n_count-=24) COMPUTE(24)
#else
    for(;n_count>23;n_count-=24) COMPUTE_n24
#endif    
    for(;n_count>19;n_count-=20) COMPUTE(20)
    for(;n_count>15;n_count-=16) COMPUTE(16)
    for(;n_count>11;n_count-=12) COMPUTE(12)
    for(;n_count>7;n_count-=8) COMPUTE(8)
    for(;n_count>3;n_count-=4) COMPUTE(4)
    for(;n_count>1;n_count-=2) COMPUTE(2)
    if(n_count>0) COMPUTE(1)
    return 0;
}
#include <immintrin.h>
//#include "sgemm_direct_skylakex.c"
