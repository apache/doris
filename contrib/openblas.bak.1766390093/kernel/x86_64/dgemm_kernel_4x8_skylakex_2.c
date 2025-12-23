#include "common.h"
#include <stdint.h>
#include <immintrin.h>

//register usage: zmm3 for alpha, zmm0-zmm2 and zmm4-zmm7 for temporary use, zmm8-zmm31 for accumulators.

/* row-major c_block */
#define INNER_KERNEL_k1m1n8 \
    "prefetcht0 384(%1);"\
    "vmovupd (%1),%%zmm5; addq $64,%1;"\
    "vbroadcastsd   (%0),%%zmm4;vfmadd231pd %%zmm5,%%zmm4,%%zmm8;"

#define INNER_KERNEL_k1m2n8 \
    INNER_KERNEL_k1m1n8\
    "vbroadcastsd  8(%0),%%zmm4;vfmadd231pd %%zmm5,%%zmm4,%%zmm9;"

#define INNER_KERNEL_k1m1n16 \
    "prefetcht0 128(%1); prefetcht0 128(%1,%%r12,2);"\
    "vmovupd (%1),%%zmm5; vmovupd (%1,%%r12,2),%%zmm6; addq $64,%1;"\
    "vbroadcastsd   (%0),%%zmm4;vfmadd231pd %%zmm5,%%zmm4,%%zmm8; vfmadd231pd %%zmm6,%%zmm4,%%zmm9;"

#define INNER_KERNEL_k1m2n16 \
    INNER_KERNEL_k1m1n16\
    "vbroadcastsd  8(%0),%%zmm4;vfmadd231pd %%zmm5,%%zmm4,%%zmm10;vfmadd231pd %%zmm6,%%zmm4,%%zmm11;"

#define INNER_KERNEL_k1m1n24 \
    "prefetcht0 128(%1); prefetcht0 128(%1,%%r12,2); prefetcht0 128(%1,%%r12,4);"\
    "vmovupd (%1),%%zmm5; vmovupd (%1,%%r12,2),%%zmm6; vmovupd (%1,%%r12,4),%%zmm7; addq $64,%1;"\
    "vbroadcastsd   (%0),%%zmm4;vfmadd231pd %%zmm5,%%zmm4,%%zmm8; vfmadd231pd %%zmm6,%%zmm4,%%zmm9; vfmadd231pd %%zmm7,%%zmm4,%%zmm10;"

#define INNER_KERNEL_k1m2n24 \
    INNER_KERNEL_k1m1n24\
    "vbroadcastsd  8(%0),%%zmm4;vfmadd231pd %%zmm5,%%zmm4,%%zmm11;vfmadd231pd %%zmm6,%%zmm4,%%zmm12;vfmadd231pd %%zmm7,%%zmm4,%%zmm13;"

/* row-major z-partition c_block */
#define INNER_KERNEL_k1m4n8 \
    "vbroadcastf32x4 (%0),%%zmm4; vbroadcastf32x4 16(%0),%%zmm5; addq $32,%0;"\
    "vmovddup (%1),%%zmm6; vfmadd231pd %%zmm4,%%zmm6,%%zmm8; vfmadd231pd %%zmm5,%%zmm6,%%zmm10;"\
    "vmovddup 8(%1),%%zmm7; vfmadd231pd %%zmm4,%%zmm7,%%zmm9; vfmadd231pd %%zmm5,%%zmm7,%%zmm11;"

#define INNER_KERNEL_k1m4n16 \
    INNER_KERNEL_k1m4n8\
    "vmovddup (%1,%%r12,2),%%zmm6; vfmadd231pd %%zmm4,%%zmm6,%%zmm12; vfmadd231pd %%zmm5,%%zmm6,%%zmm14;"\
    "vmovddup 8(%1,%%r12,2),%%zmm7; vfmadd231pd %%zmm4,%%zmm7,%%zmm13; vfmadd231pd %%zmm5,%%zmm7,%%zmm15;"

#define INNER_KERNEL_k1m4n24 \
    INNER_KERNEL_k1m4n16\
    "vmovddup (%1,%%r12,4),%%zmm6; vfmadd231pd %%zmm4,%%zmm6,%%zmm16; vfmadd231pd %%zmm5,%%zmm6,%%zmm18;"\
    "vmovddup 8(%1,%%r12,4),%%zmm7; vfmadd231pd %%zmm4,%%zmm7,%%zmm17; vfmadd231pd %%zmm5,%%zmm7,%%zmm19;"

#define INNER_KERNEL_k1m8n8 \
    "vbroadcastf32x4 (%0),%%zmm4; vbroadcastf32x4 16(%0),%%zmm5;"\
    "vbroadcastf32x4 (%0,%%r12,1),%%zmm6; vbroadcastf32x4 16(%0,%%r12,1),%%zmm7; addq $32,%0;"\
    "prefetcht0 128(%1);"\
    "vmovddup (%1),%%zmm2; vfmadd231pd %%zmm4,%%zmm2,%%zmm8; vfmadd231pd %%zmm5,%%zmm2,%%zmm10;"\
    "vfmadd231pd %%zmm6,%%zmm2,%%zmm12; vfmadd231pd %%zmm7,%%zmm2,%%zmm14;"\
    "vmovddup 8(%1),%%zmm1; vfmadd231pd %%zmm4,%%zmm1,%%zmm9; vfmadd231pd %%zmm5,%%zmm1,%%zmm11;"\
    "vfmadd231pd %%zmm6,%%zmm1,%%zmm13; vfmadd231pd %%zmm7,%%zmm1,%%zmm15;"

#define INNER_KERNEL_k1m8n16 \
    INNER_KERNEL_k1m8n8\
    "prefetcht0 128(%1,%%r12,2);"\
    "vmovddup (%1,%%r12,2),%%zmm2; vfmadd231pd %%zmm4,%%zmm2,%%zmm16; vfmadd231pd %%zmm5,%%zmm2,%%zmm18;"\
    "vfmadd231pd %%zmm6,%%zmm2,%%zmm20; vfmadd231pd %%zmm7,%%zmm2,%%zmm22;"\
    "vmovddup 8(%1,%%r12,2),%%zmm1; vfmadd231pd %%zmm4,%%zmm1,%%zmm17; vfmadd231pd %%zmm5,%%zmm1,%%zmm19;"\
    "vfmadd231pd %%zmm6,%%zmm1,%%zmm21; vfmadd231pd %%zmm7,%%zmm1,%%zmm23;"

#define INNER_KERNEL_k1m8n24 \
    INNER_KERNEL_k1m8n16\
    "prefetcht0 128(%1,%%r12,4);"\
    "vmovddup (%1,%%r12,4),%%zmm2; vfmadd231pd %%zmm4,%%zmm2,%%zmm24; vfmadd231pd %%zmm5,%%zmm2,%%zmm26;"\
    "vfmadd231pd %%zmm6,%%zmm2,%%zmm28; vfmadd231pd %%zmm7,%%zmm2,%%zmm30;"\
    "vmovddup 8(%1,%%r12,4),%%zmm1; vfmadd231pd %%zmm4,%%zmm1,%%zmm25; vfmadd231pd %%zmm5,%%zmm1,%%zmm27;"\
    "vfmadd231pd %%zmm6,%%zmm1,%%zmm29; vfmadd231pd %%zmm7,%%zmm1,%%zmm31;"

/* micro kernels */
#define INNER_KERNELm1(nn) \
    "cmpq $1,%2;jb "#nn"3f;"\
    #nn"4:\n\t"\
    INNER_KERNEL_k1m1n##nn "addq $8,%0;"\
    "decq %2;cmpq $1,%2;jnb "#nn"4b;"\
    #nn"3:\n\t"

#define INNER_KERNELm2(nn) \
    "cmpq $1,%2;jb "#nn"0f;"\
    #nn"1:\n\t"\
    INNER_KERNEL_k1m2n##nn "addq $16,%0;"\
    "decq %2;cmpq $1,%2;jnb "#nn"1b;"\
    #nn"0:\n\t"

#define INNER_KERNELm4(nn) \
    "cmpq $1,%2;jb "#nn"00f;"\
    #nn"01:\n\t"\
    INNER_KERNEL_k1m4n##nn "addq $64,%1;"\
    "decq %2;cmpq $1,%2;jnb "#nn"01b;"\
    #nn"00:\n\t"

/* %10 for prefetch of C elements before storage; %4 = ldc(in bytes),%11 for prefetch of next B block */
#define INNER_KERNELm8(nn) \
    "movq %3,%10;cmpq $18,%2;jb "#nn"001f;"\
    #nn"008:\n\t"\
    INNER_KERNEL_k1m8n##nn "addq $64,%1;"\
    INNER_KERNEL_k1m8n##nn "addq $64,%1;"\
    INNER_KERNEL_k1m8n##nn "addq $64,%1;"\
    "prefetcht1 (%10); prefetcht1 63(%10); addq %4,%10;"\
    INNER_KERNEL_k1m8n##nn "addq $64,%1;"\
    INNER_KERNEL_k1m8n##nn "addq $64,%1;"\
    INNER_KERNEL_k1m8n##nn "addq $64,%1;"\
    "prefetcht1 (%11); addq $32,%11;"\
    "subq $6,%2;cmpq $18,%2;jnb "#nn"008b;"\
    "movq %3,%10;"\
    #nn"001:\n\t"\
    "cmpq $1,%2;jb "#nn"000f;"\
    "prefetcht0 (%10); prefetcht0 63(%10); prefetcht0 (%10,%4,1); prefetcht0 63(%10,%4,1); leaq (%10,%4,2),%10;"\
    INNER_KERNEL_k1m8n##nn "addq $64,%1;"\
    "decq %2;jmp "#nn"001b;"\
    ""#nn"000:\n\t"

#define INNER_INIT_m1n8 \
    "vpxorq %%zmm8, %%zmm8, %%zmm8;"

#define INNER_INIT_m2n8 \
    "vpxorq %%zmm8, %%zmm8, %%zmm8; vpxorq %%zmm9, %%zmm9, %%zmm9;"

#define INNER_INIT_m4n8 \
    "vpxorq %%zmm8, %%zmm8, %%zmm8; vpxorq %%zmm9, %%zmm9, %%zmm9; vpxorq %%zmm10,%%zmm10,%%zmm10;vpxorq %%zmm11,%%zmm11,%%zmm11;"

#define INNER_INIT_m8n8 \
    INNER_INIT_m4n8\
    "vpxorq %%zmm12,%%zmm12,%%zmm12;vpxorq %%zmm13,%%zmm13,%%zmm13;vpxorq %%zmm14,%%zmm14,%%zmm14;vpxorq %%zmm15,%%zmm15,%%zmm15;"

#define INNER_INIT_m1n16 INNER_INIT_m2n8

#define INNER_INIT_m2n16 INNER_INIT_m4n8

#define INNER_INIT_m4n16 INNER_INIT_m8n8

#define INNER_INIT_m8n16 \
    INNER_INIT_m8n8\
    "vpxorq %%zmm16,%%zmm16,%%zmm16;vpxorq %%zmm17,%%zmm17,%%zmm17;vpxorq %%zmm18,%%zmm18,%%zmm18;vpxorq %%zmm19,%%zmm19,%%zmm19;"\
    "vpxorq %%zmm20,%%zmm20,%%zmm20;vpxorq %%zmm21,%%zmm21,%%zmm21;vpxorq %%zmm22,%%zmm22,%%zmm22;vpxorq %%zmm23,%%zmm23,%%zmm23;"

#define INNER_INIT_m1n24 \
    "vpxorq %%zmm8, %%zmm8, %%zmm8; vpxorq %%zmm9, %%zmm9, %%zmm9; vpxorq %%zmm10,%%zmm10,%%zmm10;"

#define INNER_INIT_m2n24 \
    INNER_INIT_m1n24\
    "vpxorq %%zmm11,%%zmm11,%%zmm11; vpxorq %%zmm12,%%zmm12,%%zmm12; vpxorq %%zmm13,%%zmm13,%%zmm13;"

#define INNER_INIT_m4n24 \
    INNER_INIT_m4n16\
    "vpxorq %%zmm16,%%zmm16,%%zmm16;vpxorq %%zmm17,%%zmm17,%%zmm17;vpxorq %%zmm18,%%zmm18,%%zmm18;vpxorq %%zmm19,%%zmm19,%%zmm19;"

#define INNER_INIT_m8n24 \
    INNER_INIT_m8n16\
    "vpxorq %%zmm24,%%zmm24,%%zmm24;vpxorq %%zmm25,%%zmm25,%%zmm25;vpxorq %%zmm26,%%zmm26,%%zmm26;vpxorq %%zmm27,%%zmm27,%%zmm27;"\
    "vpxorq %%zmm28,%%zmm28,%%zmm28;vpxorq %%zmm29,%%zmm29,%%zmm29;vpxorq %%zmm30,%%zmm30,%%zmm30;vpxorq %%zmm31,%%zmm31,%%zmm31;"

#define INNER_SETINDEX \
    "vpinsrq $0,%4,%%xmm4,%%xmm4; vbroadcastsd %%xmm4,%%zmm4;"\
    "kxnorw %%k1,%%k1,%%k1; kshiftlw $1,%%k1,%%k1; vpxorq %%zmm6,%%zmm6,%%zmm6; vmovapd %%zmm4,%%zmm6%{%%k1%};"\
    "kshiftlw $1,%%k1,%%k1; vpaddq %%zmm4,%%zmm6,%%zmm6%{%%k1%};"\
    "kshiftlw $1,%%k1,%%k1; vpaddq %%zmm4,%%zmm6,%%zmm6%{%%k1%};"\
    "kshiftlw $1,%%k1,%%k1; vpaddq %%zmm4,%%zmm6,%%zmm6%{%%k1%};"\
    "kshiftlw $1,%%k1,%%k1; vpaddq %%zmm4,%%zmm6,%%zmm6%{%%k1%};"\
    "kshiftlw $1,%%k1,%%k1; vpaddq %%zmm4,%%zmm6,%%zmm6%{%%k1%};"\
    "kshiftlw $1,%%k1,%%k1; vpaddq %%zmm4,%%zmm6,%%zmm6%{%%k1%};"

#define INNER_STORE_m1n8(c1,disp) \
    "kxnorw %%k1,%%k1,%%k1;"\
    "vgatherqpd "#disp"(%10,%%zmm6,1), %%zmm7 %{%%k1%};"\
    "vfmadd132pd %%zmm3,%%zmm7,"#c1";"\
    "kxnorw %%k1,%%k1,%%k1;"\
    "vscatterqpd "#c1", "#disp"(%10,%%zmm6,1) %{%%k1%};"

#define INNER_SAVE_m1n8 \
    "movq %3,%10;"\
    INNER_SETINDEX\
    INNER_STORE_m1n8(%%zmm8,0)

#define INNER_SAVE_m1n16 \
    INNER_SAVE_m1n8\
    "leaq (%10,%4,8),%10;"\
    INNER_STORE_m1n8(%%zmm9,0)

#define INNER_SAVE_m1n24 \
    INNER_SAVE_m1n16\
    "leaq (%10,%4,8),%10;"\
    INNER_STORE_m1n8(%%zmm10,0)

#define INNER_SAVE_m2n8 \
    "movq %3,%10;"\
    INNER_SETINDEX\
    INNER_STORE_m1n8(%%zmm8,0)\
    INNER_STORE_m1n8(%%zmm9,8)

#define INNER_SAVE_m2n16 \
    "movq %3,%10;"\
    INNER_SETINDEX\
    INNER_STORE_m1n8(%%zmm8,0)\
    INNER_STORE_m1n8(%%zmm10,8)\
    "leaq (%10,%4,8),%10;"\
    INNER_STORE_m1n8(%%zmm9,0)\
    INNER_STORE_m1n8(%%zmm11,8)

#define INNER_SAVE_m2n24 \
    "movq %3,%10;"\
    INNER_SETINDEX\
    INNER_STORE_m1n8(%%zmm8,0)\
    INNER_STORE_m1n8(%%zmm11,8)\
    "leaq (%10,%4,8),%10;"\
    INNER_STORE_m1n8(%%zmm9,0)\
    INNER_STORE_m1n8(%%zmm12,8)\
    "leaq (%10,%4,8),%10;"\
    INNER_STORE_m1n8(%%zmm10,0)\
    INNER_STORE_m1n8(%%zmm13,8)

#define INNER_TRANS_4x8(c1,c2,c3,c4) \
    "vblendmpd "#c3","#c1",%%zmm4%{%6%}; vblendmpd "#c4","#c2",%%zmm6%{%6%};"\
    "vshuff64x2 $177,%%zmm4,%%zmm4,%%zmm4; vshuff64x2 $177,%%zmm6,%%zmm6,%%zmm6;"\
    "vblendmpd "#c1",%%zmm4,"#c1"%{%6%}; vblendmpd "#c2",%%zmm6,"#c2"%{%6%};"\
    "vblendmpd %%zmm4,"#c3","#c3"%{%6%}; vblendmpd %%zmm6,"#c4","#c4"%{%6%};"\

#define INNER_TRANS_f128_4x4(c1,c2,c3,c4) \
    "vshuff64x2 $68,"#c3","#c1",%%zmm4; vshuff64x2 $17,"#c4","#c2",%%zmm5;"\
    "vshuff64x2 $238,"#c3","#c1",%%zmm6; vshuff64x2 $187,"#c4","#c2",%%zmm7;"\
    "vblendmpd %%zmm5,%%zmm4,"#c2"%{%6%}; vshuff64x2 $177,"#c2","#c2","#c2"; vblendmpd %%zmm4,%%zmm5,"#c1"%{%6%};"\
    "vblendmpd %%zmm7,%%zmm6,"#c4"%{%6%}; vshuff64x2 $177,"#c4","#c4","#c4"; vblendmpd %%zmm6,%%zmm7,"#c3"%{%6%};"

#define INNER_TRANS_8x8(c1,c2,c3,c4,c5,c6,c7,c8) \
    INNER_TRANS_f128_4x4(c1,c3,c5,c7) INNER_TRANS_f128_4x4(c2,c4,c6,c8)

//%7 for k01(input) only when m=4
#define INNER_STORE_4x8(c1,c2,c3,c4) \
    "vmovupd (%10),%%zmm4%{%5%};vmovupd -32(%10,%4,4),%%zmm4%{%7%};vfmadd132pd %%zmm3,%%zmm4,"#c1";"\
    "vmovupd "#c1",(%10)%{%5%}; vmovupd "#c1",-32(%10,%4,4)%{%7%}; leaq (%10,%4,1),%10;"\
    "vmovupd (%10),%%zmm5%{%5%};vmovupd -32(%10,%4,4),%%zmm5%{%7%};vfmadd132pd %%zmm3,%%zmm5,"#c2";"\
    "vmovupd "#c2",(%10)%{%5%}; vmovupd "#c2",-32(%10,%4,4)%{%7%}; leaq (%10,%4,1),%10;"\
    "vmovupd (%10),%%zmm6%{%5%};vmovupd -32(%10,%4,4),%%zmm6%{%7%};vfmadd132pd %%zmm3,%%zmm6,"#c3";"\
    "vmovupd "#c3",(%10)%{%5%}; vmovupd "#c3",-32(%10,%4,4)%{%7%}; leaq (%10,%4,1),%10;"\
    "vmovupd (%10),%%zmm7%{%5%};vmovupd -32(%10,%4,4),%%zmm7%{%7%};vfmadd132pd %%zmm3,%%zmm7,"#c4";"\
    "vmovupd "#c4",(%10)%{%5%}; vmovupd "#c4",-32(%10,%4,4)%{%7%}; leaq (%10,%4,1),%10;"\
    "leaq (%10,%4,4),%10;"

#define INNER_STORE_8x8(c1,c2,c3,c4,c5,c6,c7,c8) \
    "vfmadd213pd (%10),%%zmm3,"#c1"; vmovupd "#c1",(%10); vfmadd213pd (%10,%4,1),%%zmm3,"#c2"; vmovupd "#c2",(%10,%4,1); leaq (%10,%4,2),%10;"\
    "vfmadd213pd (%10),%%zmm3,"#c3"; vmovupd "#c3",(%10); vfmadd213pd (%10,%4,1),%%zmm3,"#c4"; vmovupd "#c4",(%10,%4,1); leaq (%10,%4,2),%10;"\
    "vfmadd213pd (%10),%%zmm3,"#c5"; vmovupd "#c5",(%10); vfmadd213pd (%10,%4,1),%%zmm3,"#c6"; vmovupd "#c6",(%10,%4,1); leaq (%10,%4,2),%10;"\
    "vfmadd213pd (%10),%%zmm3,"#c7"; vmovupd "#c7",(%10); vfmadd213pd (%10,%4,1),%%zmm3,"#c8"; vmovupd "#c8",(%10,%4,1); leaq (%10,%4,2),%10;"

#define INNER_SAVE_m4n8 \
    "movq %3,%10;"\
    INNER_TRANS_4x8(%%zmm8,%%zmm9,%%zmm10,%%zmm11)\
    INNER_STORE_4x8(%%zmm8,%%zmm9,%%zmm10,%%zmm11)

#define INNER_SAVE_m4n16 \
    INNER_SAVE_m4n8\
    INNER_TRANS_4x8(%%zmm12,%%zmm13,%%zmm14,%%zmm15)\
    INNER_STORE_4x8(%%zmm12,%%zmm13,%%zmm14,%%zmm15)

#define INNER_SAVE_m4n24 \
    INNER_SAVE_m4n16\
    INNER_TRANS_4x8(%%zmm16,%%zmm17,%%zmm18,%%zmm19)\
    INNER_STORE_4x8(%%zmm16,%%zmm17,%%zmm18,%%zmm19)

#define INNER_SAVE_m8n8 \
    "movq %3,%10;"\
    INNER_TRANS_8x8(%%zmm8,%%zmm9,%%zmm10,%%zmm11,%%zmm12,%%zmm13,%%zmm14,%%zmm15)\
    INNER_STORE_8x8(%%zmm8,%%zmm9,%%zmm10,%%zmm11,%%zmm12,%%zmm13,%%zmm14,%%zmm15)

#define INNER_SAVE_m8n16 \
    INNER_SAVE_m8n8\
    INNER_TRANS_8x8(%%zmm16,%%zmm17,%%zmm18,%%zmm19,%%zmm20,%%zmm21,%%zmm22,%%zmm23)\
    INNER_STORE_8x8(%%zmm16,%%zmm17,%%zmm18,%%zmm19,%%zmm20,%%zmm21,%%zmm22,%%zmm23)

#define INNER_SAVE_m8n24 \
    INNER_SAVE_m8n16\
    INNER_TRANS_8x8(%%zmm24,%%zmm25,%%zmm26,%%zmm27,%%zmm28,%%zmm29,%%zmm30,%%zmm31)\
    INNER_STORE_8x8(%%zmm24,%%zmm25,%%zmm26,%%zmm27,%%zmm28,%%zmm29,%%zmm30,%%zmm31)

#define COMPUTE_n8 {\
    b_pref = packed_b_pointer + 8 * K;\
    __asm__ __volatile__(\
    "vbroadcastsd (%9),%%zmm3;"\
    "movq %8,%%r14;movq %2,%%r13;movq %2,%%r12;shlq $5,%%r12;"\
    "cmpq $8,%8; jb 42222f;"\
    "42221:\n\t"\
    INNER_INIT_m8n8\
    INNER_KERNELm8(8)\
    INNER_SAVE_m8n8\
    "movq %%r13,%2; subq %%r12,%1; subq %%r12,%1; addq %%r12,%0;"\
    "addq $64,%3;"\
    "subq $8,%8; cmpq $8,%8; jnb 42221b;"\
    "42222:\n\t"\
    "cmpq $4,%8; jb 42223f;"\
    INNER_INIT_m4n8\
    INNER_KERNELm4(8)\
    INNER_SAVE_m4n8\
    "movq %%r13,%2; subq %%r12,%1; subq %%r12,%1;"\
    "addq $32,%3;"\
    "subq $4,%8;"\
    "42223:\n\t"\
    "cmpq $2,%8; jb 42224f;"\
    INNER_INIT_m2n8\
    INNER_KERNELm2(8)\
    INNER_SAVE_m2n8\
    "movq %%r13,%2; subq %%r12,%1; subq %%r12,%1;"\
    "addq $16,%3;"\
    "subq $2,%8;"\
    "42224:\n\t"\
    "cmpq $1,%8; jb 42225f;"\
    INNER_INIT_m1n8\
    INNER_KERNELm1(8)\
    INNER_SAVE_m1n8\
    "movq %%r13,%2; subq %%r12,%1; subq %%r12,%1;"\
    "addq $8,%3;"\
    "42225:\n\t"\
    "movq %%r14,%8;shlq $3,%8;subq %8,%3;shrq $3,%8;"\
    "shlq $3,%4;addq %4,%3;shrq $3,%4;"\
    :"+r"(a_block_pointer),"+r"(packed_b_pointer),"+r"(K),"+r"(c_pointer),"+r"(ldc_in_bytes),"+Yk"(k02),"+Yk"(k03),"+Yk"(k01),\
    "+r"(M),"+r"(alpha),"+r"(c_store),"+r"(b_pref)\
    ::"zmm0","zmm1","zmm2","zmm3","zmm4","zmm5","zmm6","zmm7","zmm8","zmm9","zmm10","zmm11","zmm12","zmm13","zmm14","zmm15","cc","memory","k1","r12","r13","r14");\
    a_block_pointer -= M * K;\
}
#define COMPUTE_n16 {\
    b_pref = packed_b_pointer + 16 * K;\
    __asm__ __volatile__(\
    "vbroadcastsd (%9),%%zmm3;"\
    "movq %8,%%r14;movq %2,%%r13;movq %2,%%r12;shlq $5,%%r12;"\
    "cmpq $8,%8; jb 32222f;"\
    "32221:\n\t"\
    INNER_INIT_m8n16\
    INNER_KERNELm8(16)\
    INNER_SAVE_m8n16\
    "movq %%r13,%2; subq %%r12,%1; subq %%r12,%1; addq %%r12,%0;"\
    "addq $64,%3;"\
    "subq $8,%8; cmpq $8,%8; jnb 32221b;"\
    "32222:\n\t"\
    "cmpq $4,%8; jb 32223f;"\
    INNER_INIT_m4n16\
    INNER_KERNELm4(16)\
    INNER_SAVE_m4n16\
    "movq %%r13,%2; subq %%r12,%1; subq %%r12,%1;"\
    "addq $32,%3;"\
    "subq $4,%8;"\
    "32223:\n\t"\
    "cmpq $2,%8; jb 32224f;"\
    INNER_INIT_m2n16\
    INNER_KERNELm2(16)\
    INNER_SAVE_m2n16\
    "movq %%r13,%2; subq %%r12,%1; subq %%r12,%1;"\
    "addq $16,%3;"\
    "subq $2,%8;"\
    "32224:\n\t"\
    "cmpq $1,%8; jb 32225f;"\
    INNER_INIT_m1n16\
    INNER_KERNELm1(16)\
    INNER_SAVE_m1n16\
    "movq %%r13,%2; subq %%r12,%1; subq %%r12,%1;"\
    "addq $8,%3;"\
    "32225:\n\t"\
    "movq %%r14,%8;shlq $3,%8;subq %8,%3;shrq $3,%8;"\
    "shlq $4,%4;addq %4,%3;shrq $4,%4;"\
    "leaq (%1,%%r12,4),%1;"\
    :"+r"(a_block_pointer),"+r"(packed_b_pointer),"+r"(K),"+r"(c_pointer),"+r"(ldc_in_bytes),"+Yk"(k02),"+Yk"(k03),"+Yk"(k01),\
    "+r"(M),"+r"(alpha),"+r"(c_store),"+r"(b_pref)\
    ::"zmm0","zmm1","zmm2","zmm3","zmm4","zmm5","zmm6","zmm7","zmm8","zmm9","zmm10","zmm11","zmm12","zmm13","zmm14","zmm15","zmm16","zmm17",\
    "zmm18","zmm19","zmm20","zmm21","zmm22","zmm23","cc","memory","k1","r12","r13","r14");\
    a_block_pointer -= M * K;\
}
#define COMPUTE_n24 {\
    b_pref = packed_b_pointer + 24 * K;\
    __asm__ __volatile__(\
    "vbroadcastsd (%9),%%zmm3;"\
    "movq %8,%%r14;movq %2,%%r13;movq %2,%%r12;shlq $5,%%r12;"\
    "cmpq $8,%8; jb 22222f;"\
    "22221:\n\t"\
    INNER_INIT_m8n24\
    INNER_KERNELm8(24)\
    INNER_SAVE_m8n24\
    "movq %%r13,%2; subq %%r12,%1; subq %%r12,%1; addq %%r12,%0;"\
    "addq $64,%3;"\
    "subq $8,%8; cmpq $8,%8; jnb 22221b;"\
    "22222:\n\t"\
    "cmpq $4,%8; jb 22223f;"\
    INNER_INIT_m4n24\
    INNER_KERNELm4(24)\
    INNER_SAVE_m4n24\
    "movq %%r13,%2; subq %%r12,%1; subq %%r12,%1;"\
    "addq $32,%3;"\
    "subq $4,%8;"\
    "22223:\n\t"\
    "cmpq $2,%8; jb 22224f;"\
    INNER_INIT_m2n24\
    INNER_KERNELm2(24)\
    INNER_SAVE_m2n24\
    "movq %%r13,%2; subq %%r12,%1; subq %%r12,%1;"\
    "addq $16,%3;"\
    "subq $2,%8;"\
    "22224:\n\t"\
    "cmpq $1,%8; jb 22225f;"\
    INNER_INIT_m1n24\
    INNER_KERNELm1(24)\
    INNER_SAVE_m1n24\
    "movq %%r13,%2; subq %%r12,%1; subq %%r12,%1;"\
    "addq $8,%3;"\
    "22225:\n\t"\
    "movq %%r14,%8;shlq $3,%8;subq %8,%3;shrq $3,%8;"\
    "shlq $3,%4;addq %4,%3;shlq $1,%4;addq %4,%3;shrq $4,%4;"\
    "leaq (%1,%%r12,4),%1; leaq (%1,%%r12,2),%1;"\
    :"+r"(a_block_pointer),"+r"(packed_b_pointer),"+r"(K),"+r"(c_pointer),"+r"(ldc_in_bytes),"+Yk"(k02),"+Yk"(k03),"+Yk"(k01),\
    "+r"(M),"+r"(alpha),"+r"(c_store),"+r"(b_pref)::\
    "zmm0","zmm1","zmm2","zmm3","zmm4","zmm5","zmm6","zmm7","zmm8","zmm9","zmm10","zmm11","zmm12","zmm13","zmm14","zmm15","zmm16","zmm17","zmm18",\
    "zmm19","zmm20","zmm21","zmm22","zmm23","zmm24","zmm25","zmm26","zmm27","zmm28","zmm29","zmm30","zmm31","cc","memory","k1","r12","r13","r14");\
    a_block_pointer -= M * K;\
}
static void KERNEL_MAIN(double *packed_a, double *packed_b, BLASLONG m, BLASLONG ndiv8, BLASLONG k, BLASLONG LDC, double *c,double *alpha){//icopy=4,ocopy=8
//perform C += A<pack> B<pack>
    if(k==0 || m==0 || ndiv8==0) return;
    int64_t ldc_in_bytes = (int64_t)LDC * sizeof(double);
    int64_t K = (int64_t)k; int64_t M = (int64_t)m;
    double *a_block_pointer,*b_pref;
    double *c_pointer = c,*c_store = c;
    __mmask16 k01 = 0x00f0,k02 = 0x000f,k03 = 0x0033;
    BLASLONG ndiv8_count;
    double *packed_b_pointer = packed_b;
    a_block_pointer = packed_a;
    for(ndiv8_count=ndiv8;ndiv8_count>2;ndiv8_count-=3){
      COMPUTE_n24
    }
    for(;ndiv8_count>1;ndiv8_count-=2){
      COMPUTE_n16
    }
    if(ndiv8_count>0){
      COMPUTE_n8
    }
}

/* __m256d accumulators: yc1-yc4; temporary variables: ya1,yb1-yb2 */
/* __m128d accumulators: xc1-xc2; temporary variables: xa1,xb1-xb2 */
/*  double accumulator:  sc1;     temporary variables: sa1,sb1 */
/* column-major c_block */
#define KERNEL_m4n4k1 {\
    ya1 = _mm256_loadu_pd(a_block_pointer);a_block_pointer+=4;\
    yb1 = _mm256_broadcast_sd(b_block_pointer);   yc1 = _mm256_fmadd_pd(ya1,yb1,yc1);\
    yb2 = _mm256_broadcast_sd(b_block_pointer+1); yc2 = _mm256_fmadd_pd(ya1,yb2,yc2);\
    yb1 = _mm256_broadcast_sd(b_block_pointer+2); yc3 = _mm256_fmadd_pd(ya1,yb1,yc3);\
    yb2 = _mm256_broadcast_sd(b_block_pointer+3); yc4 = _mm256_fmadd_pd(ya1,yb2,yc4);\
    b_block_pointer+=4;\
}
#define KERNEL_m4n2k1 {\
    ya1 = _mm256_loadu_pd(a_block_pointer);a_block_pointer+=4;\
    yb1 = _mm256_broadcast_sd(b_block_pointer);   yc1 = _mm256_fmadd_pd(ya1,yb1,yc1);\
    yb2 = _mm256_broadcast_sd(b_block_pointer+1); yc2 = _mm256_fmadd_pd(ya1,yb2,yc2);\
    b_block_pointer+=2;\
}
#define KERNEL_m4n1k1 {\
    ya1 = _mm256_loadu_pd(a_block_pointer);a_block_pointer+=4;\
    yb1 = _mm256_broadcast_sd(b_block_pointer);   yc1 = _mm256_fmadd_pd(ya1,yb1,yc1);\
    b_block_pointer++;\
}
#define INIT_m4n1 yc1=_mm256_setzero_pd();
#define INIT_m4n2 yc2=INIT_m4n1
#define INIT_m4n4 yc4=yc3=INIT_m4n2
#define SAVE_m4n1 {\
    yb1 = _mm256_broadcast_sd(alpha);\
    ya1 = _mm256_loadu_pd(c_pointer);\
    yc1 = _mm256_fmadd_pd(yc1,yb1,ya1);\
    _mm256_storeu_pd(c_pointer,yc1);\
    c_pointer += 4;\
}
#define SAVE_m4n2 {\
    ya1 = _mm256_broadcast_sd(alpha);\
    yb1 = _mm256_loadu_pd(c_pointer); yb2 = _mm256_loadu_pd(c_pointer+LDC);\
    yc1 = _mm256_fmadd_pd(yc1,ya1,yb1); yc2 = _mm256_fmadd_pd(yc2,ya1,yb2);\
    _mm256_storeu_pd(c_pointer,yc1); _mm256_storeu_pd(c_pointer+LDC,yc2);\
    c_pointer += 4;\
}
#define SAVE_m4n4 {\
    ya1 = _mm256_broadcast_sd(alpha);\
    yb1 = _mm256_loadu_pd(c_pointer); yb2 = _mm256_loadu_pd(c_pointer+LDC);\
    yc1 = _mm256_fmadd_pd(yc1,ya1,yb1); yc2 = _mm256_fmadd_pd(yc2,ya1,yb2);\
    _mm256_storeu_pd(c_pointer,yc1); _mm256_storeu_pd(c_pointer+LDC,yc2);\
    c_pointer += LDC*2;\
    yb1 = _mm256_loadu_pd(c_pointer); yb2 = _mm256_loadu_pd(c_pointer+LDC);\
    yc3 = _mm256_fmadd_pd(yc3,ya1,yb1); yc4 = _mm256_fmadd_pd(yc4,ya1,yb2);\
    _mm256_storeu_pd(c_pointer,yc3); _mm256_storeu_pd(c_pointer+LDC,yc4);\
    c_pointer += 4-LDC*2;\
}
#define KERNEL_m2n2k1 {\
    xa1 = _mm_loadu_pd(a_block_pointer); a_block_pointer+=2;\
    xb1 = _mm_loaddup_pd(b_block_pointer);   xc1 = _mm_fmadd_pd(xa1,xb1,xc1);\
    xb2 = _mm_loaddup_pd(b_block_pointer+1); xc2 = _mm_fmadd_pd(xa1,xb2,xc2);\
    b_block_pointer += 2;\
}
#define KERNEL_m2n1k1 {\
    xa1 = _mm_loadu_pd(a_block_pointer); a_block_pointer+=2;\
    xb1 = _mm_loaddup_pd(b_block_pointer);   xc1 = _mm_fmadd_pd(xa1,xb1,xc1);\
    b_block_pointer ++;\
}
#define INIT_m2n1 xc1=_mm_setzero_pd();
#define INIT_m2n2 xc2=INIT_m2n1
#define SAVE_m2n1 {\
    xb1 = _mm_loaddup_pd(alpha);\
    xa1 = _mm_loadu_pd(c_pointer);\
    xc1 = _mm_fmadd_pd(xc1,xb1,xa1);\
    _mm_storeu_pd(c_pointer,xc1);\
    c_pointer += 2;\
}
#define SAVE_m2n2 {\
    xa1 = _mm_loaddup_pd(alpha);\
    xb1 = _mm_loadu_pd(c_pointer); xb2 = _mm_loadu_pd(c_pointer+LDC);\
    xc1 = _mm_fmadd_pd(xc1,xa1,xb1); xc2 = _mm_fmadd_pd(xc2,xa1,xb2);\
    _mm_storeu_pd(c_pointer,xc1); _mm_storeu_pd(c_pointer+LDC,xc2);\
    c_pointer += 2;\
}
#define KERNEL_m1n1k1 {\
    sa1 = *a_block_pointer; a_block_pointer++;\
    sb1 = *b_block_pointer; sc1 += sa1 * sb1;\
    b_block_pointer ++;\
}
#define INIT_m1n1 sc1=0.0;
#define SAVE_m1n1 {\
    *c_pointer += sc1 * (*alpha);\
    c_pointer++;\
}
/* row-major c_block */
#define KERNEL_m2n4k1 {\
    yb1 = _mm256_loadu_pd(b_block_pointer);b_block_pointer+=4;\
    ya1 = _mm256_broadcast_sd(a_block_pointer);  yc1 = _mm256_fmadd_pd(ya1,yb1,yc1);\
    ya1 = _mm256_broadcast_sd(a_block_pointer+1);yc2 = _mm256_fmadd_pd(ya1,yb1,yc2);\
    a_block_pointer += 2;\
}
#define KERNEL_m1n4k1 {\
    yb1 = _mm256_loadu_pd(b_block_pointer);b_block_pointer+=4;\
    ya1 = _mm256_broadcast_sd(a_block_pointer);  yc1 = _mm256_fmadd_pd(ya1,yb1,yc1);\
    a_block_pointer ++;\
}
#define KERNEL_m1n2k1 {\
    xb1 = _mm_loadu_pd(b_block_pointer);b_block_pointer+=2;\
    xa1 = _mm_loaddup_pd(a_block_pointer); xc1 = _mm_fmadd_pd(xa1,xb1,xc1);\
    a_block_pointer ++;\
}
#define INIT_m1n2 INIT_m2n1
#define INIT_m1n4 INIT_m4n1
#define INIT_m2n4 INIT_m4n2
#define SAVE_m2n4 {\
    ya1 = _mm256_broadcast_sd(alpha);\
    yc1 = _mm256_mul_pd(yc1,ya1);\
    yc2 = _mm256_mul_pd(yc2,ya1);\
    yb1 = _mm256_unpacklo_pd(yc1,yc2);\
    yb2 = _mm256_unpackhi_pd(yc1,yc2);\
    xb1 = _mm_add_pd(_mm_loadu_pd(c_pointer),_mm256_extractf128_pd(yb1,0));\
    xb2 = _mm_add_pd(_mm_loadu_pd(c_pointer+LDC),_mm256_extractf128_pd(yb2,0));\
    _mm_storeu_pd(c_pointer,xb1);\
    _mm_storeu_pd(c_pointer+LDC,xb2);\
    xb1 = _mm_add_pd(_mm_loadu_pd(c_pointer+2*LDC),_mm256_extractf128_pd(yb1,1));\
    xb2 = _mm_add_pd(_mm_loadu_pd(c_pointer+3*LDC),_mm256_extractf128_pd(yb2,1));\
    _mm_storeu_pd(c_pointer+2*LDC,xb1);\
    _mm_storeu_pd(c_pointer+3*LDC,xb2);\
    c_pointer += 2;\
}
#define SAVE_m1n2 {\
    xb1 = _mm_loaddup_pd(alpha);\
    xc1 = _mm_mul_pd(xc1,xb1);\
    *c_pointer += _mm_cvtsd_f64(xc1);\
    xa1 = _mm_unpackhi_pd(xc1,xc1);\
    c_pointer[LDC]+= _mm_cvtsd_f64(xa1);\
    c_pointer ++;\
}
#define SAVE_m1n4 {\
    ya1 = _mm256_broadcast_sd(alpha);\
    yc1 = _mm256_mul_pd(yc1,ya1);\
    xb1 = _mm256_extractf128_pd(yc1,0);\
    *c_pointer += _mm_cvtsd_f64(xb1);\
    xb2 = _mm_unpackhi_pd(xb1,xb1);\
    c_pointer[LDC] += _mm_cvtsd_f64(xb2);\
    xb1 = _mm256_extractf128_pd(yc1,1);\
    c_pointer[LDC*2] += _mm_cvtsd_f64(xb1);\
    xb2 = _mm_unpackhi_pd(xb1,xb1);\
    c_pointer[LDC*3] += _mm_cvtsd_f64(xb2);\
    c_pointer ++;\
}
static void KERNEL_EDGE(double *packed_a, double *packed_b, BLASLONG m, BLASLONG edge_n, BLASLONG k, BLASLONG LDC, double *c,double *alpha){//icopy=8,ocopy=8
//perform C += A<pack> B<pack> , edge_n<8 must be satisfied.
    if(k==0 || m==0 || edge_n==0 || (*alpha)==0.0) return;
    double *a_block_pointer,*b_block_pointer,*b_base_pointer;
    double *c_pointer = c;
    __m256d yc1,yc2,yc3,yc4,ya1,yb1,yb2;
    __m128d xc1,xc2,xa1,xb1,xb2;
    double sc1,sa1,sb1;
    BLASLONG m_count,n_count,k_count;
    b_base_pointer = packed_b;
//now start calculation of the edge part
    for(n_count=edge_n;n_count>3;n_count-=4){
      a_block_pointer = packed_a;
      for(m_count=m;m_count>3;m_count-=4){
        b_block_pointer = b_base_pointer;
        INIT_m4n4
        for(k_count=0;k_count<k;k_count++) KERNEL_m4n4k1
        SAVE_m4n4
      }
      for(;m_count>1;m_count-=2){
        b_block_pointer = b_base_pointer;
        INIT_m2n4
        for(k_count=0;k_count<k;k_count++) KERNEL_m2n4k1
        SAVE_m2n4
      }
      if(m_count>0){
        b_block_pointer = b_base_pointer;
        INIT_m1n4
        for(k_count=0;k_count<k;k_count++) KERNEL_m1n4k1
        SAVE_m1n4
      }
      b_base_pointer += 4*k;
      c_pointer += 4 * LDC - m;
    }
    for(;n_count>1;n_count-=2){
      a_block_pointer = packed_a;
      for(m_count=m;m_count>3;m_count-=4){
        b_block_pointer = b_base_pointer;
        INIT_m4n2
        for(k_count=0;k_count<k;k_count++) KERNEL_m4n2k1
        SAVE_m4n2
      }
      for(;m_count>1;m_count-=2){
        b_block_pointer = b_base_pointer;
        INIT_m2n2
        for(k_count=0;k_count<k;k_count++) KERNEL_m2n2k1
        SAVE_m2n2
      }
      if(m_count>0){
        b_block_pointer = b_base_pointer;
        INIT_m1n2
        for(k_count=0;k_count<k;k_count++) KERNEL_m1n2k1
        SAVE_m1n2
      }
      b_base_pointer += 2*k;
      c_pointer += 2 * LDC - m;
    }
    if(n_count>0){
      a_block_pointer = packed_a;
      for(m_count=m;m_count>3;m_count-=4){
        b_block_pointer = b_base_pointer;
        INIT_m4n1
        for(k_count=0;k_count<k;k_count++) KERNEL_m4n1k1
        SAVE_m4n1
      }
      for(;m_count>1;m_count-=2){
        b_block_pointer = b_base_pointer;
        INIT_m2n1
        for(k_count=0;k_count<k;k_count++) KERNEL_m2n1k1
        SAVE_m2n1
      }
      if(m_count>0){
        b_block_pointer = b_base_pointer;
        INIT_m1n1
        for(k_count=0;k_count<k;k_count++) KERNEL_m1n1k1
        SAVE_m1n1
      }
    }
}
int __attribute__ ((noinline)) CNAME(BLASLONG m, BLASLONG n, BLASLONG k, double alpha, double * __restrict__ A, double * __restrict__ B, double * __restrict__ C, BLASLONG ldc){
    if(m==0 || n==0 || k==0 || alpha == 0.0) return 0;
    BLASLONG ndiv8 = n/8;double ALPHA = alpha;
    double *packed_a = A;
    if(ndiv8>0) KERNEL_MAIN(packed_a,B,m,ndiv8,k,ldc,C,&ALPHA);
    if(n>ndiv8*8) KERNEL_EDGE(packed_a,B+(int64_t)k*(int64_t)ndiv8*8,m,n-ndiv8*8,k,ldc,C+(int64_t)ldc*(int64_t)ndiv8*8,&ALPHA);
    return 0;
}
