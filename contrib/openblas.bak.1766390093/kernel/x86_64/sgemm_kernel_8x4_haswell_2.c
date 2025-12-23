/* %0 = "+r"(a_pointer), %1 = "+r"(b_pointer), %2 = "+r"(c_pointer), %3 = "+r"(ldc_in_bytes), %4 for k_count, %5 for c_store, %6 = b_pref */
/* r10 = tmp, r11 = m_counter, r12 = k << 2(const), r13 = tmp, r14 = b_head_pos(const), r15 = tmp */

/* m = 8 *//* ymm0 for alpha, ymm1-ymm3 for temporary use, ymm4-ymm15 for accumulators */
#define KERNEL_k1m8n1 \
    "vmovups (%0),%%ymm1; addq $32,%0;"\
    "vbroadcastss (%1),%%ymm2; vfmadd231ps %%ymm1,%%ymm2,%%ymm4;"\
    "addq $4,%1;"
#define KERNEL_h_k1m8n2 \
    "vmovsldup (%0),%%ymm1; vmovshdup (%0),%%ymm2; addq $32,%0;"\
    "vbroadcastsd (%1),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm4; vfmadd231ps %%ymm2,%%ymm3,%%ymm5;"
#define KERNEL_k1m8n2 KERNEL_h_k1m8n2 "addq $8,%1;"
#define KERNEL_h_k1m8n4 \
    KERNEL_h_k1m8n2 "vbroadcastsd 8(%1),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm6; vfmadd231ps %%ymm2,%%ymm3,%%ymm7;"
#define KERNEL_k1m8n4 KERNEL_h_k1m8n4 "addq $16,%1;"
#define unit_kernel_k1m8n4(c1,c2,c3,c4,boff,...) \
    "vbroadcastsd "#boff"("#__VA_ARGS__"),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,"#c1"; vfmadd231ps %%ymm2,%%ymm3,"#c2";"\
    "vbroadcastsd "#boff"+8("#__VA_ARGS__"),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,"#c3"; vfmadd231ps %%ymm2,%%ymm3,"#c4";"
#define KERNEL_h_k1m8n8 KERNEL_h_k1m8n4 unit_kernel_k1m8n4(%%ymm8,%%ymm9,%%ymm10,%%ymm11,0,%1,%%r12,4)
#define KERNEL_k1m8n8 KERNEL_h_k1m8n8 "addq $16,%1;"
#define KERNEL_h_k1m8n12 KERNEL_h_k1m8n8 unit_kernel_k1m8n4(%%ymm12,%%ymm13,%%ymm14,%%ymm15,0,%1,%%r12,8)
#define KERNEL_k1m8n12 KERNEL_h_k1m8n12 "addq $16,%1;"
#define KERNEL_k2m8n4 \
    "vmovsldup (%0),%%ymm1; vmovshdup (%0),%%ymm2; prefetcht0 512(%0);"\
    unit_kernel_k1m8n4(%%ymm4,%%ymm5,%%ymm6,%%ymm7,0,%1)\
    "vmovsldup 32(%0),%%ymm1; vmovshdup 32(%0),%%ymm2; addq $64,%0;"\
    unit_kernel_k1m8n4(%%ymm8,%%ymm9,%%ymm10,%%ymm11,16,%1)\
    "addq $32,%1;"
#define KERNEL_L_k1m8n6 \
    "vmovsldup (%0),%%ymm1; vmovshdup (%0),%%ymm2; prefetcht0 512(%0); addq $32,%0;"\
    "vbroadcastsd    (%1)        ,%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm4;  vfmadd231ps %%ymm2,%%ymm3,%%ymm5;"\
    "vbroadcastsd   8(%1)        ,%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm6;  vfmadd231ps %%ymm2,%%ymm3,%%ymm7;"\
    "vbroadcastsd    (%1,%%r12,4),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm8;  vfmadd231ps %%ymm2,%%ymm3,%%ymm9;"\
    "addq $16,%1;"
#define KERNEL_L_k2m8n6 \
    "vmovsldup (%0),%%ymm1; vmovshdup (%0),%%ymm2; prefetcht0 512(%0);"\
    "vbroadcastsd    (%1)        ,%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm4;  vfmadd231ps %%ymm2,%%ymm3,%%ymm5;"\
    "vbroadcastsd   8(%1)        ,%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm6;  vfmadd231ps %%ymm2,%%ymm3,%%ymm7;"\
    "vbroadcastsd    (%1,%%r12,4),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm8;  vfmadd231ps %%ymm2,%%ymm3,%%ymm9;"\
    "vmovsldup 32(%0),%%ymm1; vmovshdup 32(%0),%%ymm2; addq $64,%0;"\
    "vbroadcastsd  16(%1)        ,%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm10; vfmadd231ps %%ymm2,%%ymm3,%%ymm11;"\
    "vbroadcastsd  24(%1)        ,%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm12; vfmadd231ps %%ymm2,%%ymm3,%%ymm13;"\
    "vbroadcastsd  16(%1,%%r12,4),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm14; vfmadd231ps %%ymm2,%%ymm3,%%ymm15;"\
    "addq $32,%1;"
#define KERNEL_L_k1m16n6 \
    "vmovups (%0),%%ymm1; vmovups (%0,%%r12,8),%%ymm2; prefetcht0 512(%0,%%r12,8); addq $32,%0;"\
    "vbroadcastss    (%1)        ,%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm4;  vfmadd231ps %%ymm2,%%ymm3,%%ymm5;"\
    "vbroadcastss   4(%1)        ,%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm6;  vfmadd231ps %%ymm2,%%ymm3,%%ymm7;"\
    "vbroadcastss   8(%1)        ,%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm8;  vfmadd231ps %%ymm2,%%ymm3,%%ymm9;"\
    "vbroadcastss  12(%1)        ,%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm10; vfmadd231ps %%ymm2,%%ymm3,%%ymm11;"\
    "vbroadcastss    (%1,%%r12,4),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm12; vfmadd231ps %%ymm2,%%ymm3,%%ymm13;"\
    "vbroadcastss   4(%1,%%r12,4),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm14; vfmadd231ps %%ymm2,%%ymm3,%%ymm15;"\
    "addq $16,%1;"
#define KERNEL_L_k2m16n6 \
    "vmovups (%0),%%ymm1; vmovups (%0,%%r12,8),%%ymm2; prefetcht0 512(%0,%%r12,8);"\
    "vbroadcastss    (%1)        ,%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm4;  vfmadd231ps %%ymm2,%%ymm3,%%ymm5;"\
    "vbroadcastss   4(%1)        ,%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm6;  vfmadd231ps %%ymm2,%%ymm3,%%ymm7;"\
    "vbroadcastss   8(%1)        ,%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm8;  vfmadd231ps %%ymm2,%%ymm3,%%ymm9;"\
    "vbroadcastss  12(%1)        ,%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm10; vfmadd231ps %%ymm2,%%ymm3,%%ymm11;"\
    "vbroadcastss    (%1,%%r12,4),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm12; vfmadd231ps %%ymm2,%%ymm3,%%ymm13;"\
    "vbroadcastss   4(%1,%%r12,4),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm14; vfmadd231ps %%ymm2,%%ymm3,%%ymm15;"\
    "vmovups 32(%0),%%ymm1; vmovups 32(%0,%%r12,8),%%ymm2; addq $64,%0;"\
    "vbroadcastss  16(%1)        ,%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm4;  vfmadd231ps %%ymm2,%%ymm3,%%ymm5;"\
    "vbroadcastss  20(%1)        ,%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm6;  vfmadd231ps %%ymm2,%%ymm3,%%ymm7;"\
    "vbroadcastss  24(%1)        ,%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm8;  vfmadd231ps %%ymm2,%%ymm3,%%ymm9;"\
    "vbroadcastss  28(%1)        ,%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm10; vfmadd231ps %%ymm2,%%ymm3,%%ymm11;"\
    "vbroadcastss  16(%1,%%r12,4),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm12; vfmadd231ps %%ymm2,%%ymm3,%%ymm13;"\
    "vbroadcastss  20(%1,%%r12,4),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm14; vfmadd231ps %%ymm2,%%ymm3,%%ymm15;"\
    "addq $32,%1;"
#define KERNEL_R_k1m16n6 \
    "vmovups (%0),%%ymm1; vmovups (%0,%%r12,8),%%ymm2; prefetcht0 512(%0,%%r12,8); addq $32,%0;"\
    "vbroadcastss   8(%1,%%r12,4),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm4;  vfmadd231ps %%ymm2,%%ymm3,%%ymm5;"\
    "vbroadcastss  12(%1,%%r12,4),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm6;  vfmadd231ps %%ymm2,%%ymm3,%%ymm7;"\
    "vbroadcastss    (%1,%%r12,8),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm8;  vfmadd231ps %%ymm2,%%ymm3,%%ymm9;"\
    "vbroadcastss   4(%1,%%r12,8),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm10; vfmadd231ps %%ymm2,%%ymm3,%%ymm11;"\
    "vbroadcastss   8(%1,%%r12,8),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm12; vfmadd231ps %%ymm2,%%ymm3,%%ymm13;"\
    "vbroadcastss  12(%1,%%r12,8),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm14; vfmadd231ps %%ymm2,%%ymm3,%%ymm15;"\
    "addq $16,%1;"
#define KERNEL_R_k2m16n6 \
    "vmovups (%0),%%ymm1; vmovups (%0,%%r12,8),%%ymm2; prefetcht0 512(%0,%%r12,8);"\
    "vbroadcastss   8(%1,%%r12,4),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm4;  vfmadd231ps %%ymm2,%%ymm3,%%ymm5;"\
    "vbroadcastss  12(%1,%%r12,4),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm6;  vfmadd231ps %%ymm2,%%ymm3,%%ymm7;"\
    "vbroadcastss    (%1,%%r12,8),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm8;  vfmadd231ps %%ymm2,%%ymm3,%%ymm9;"\
    "vbroadcastss   4(%1,%%r12,8),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm10; vfmadd231ps %%ymm2,%%ymm3,%%ymm11;"\
    "vbroadcastss   8(%1,%%r12,8),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm12; vfmadd231ps %%ymm2,%%ymm3,%%ymm13;"\
    "vbroadcastss  12(%1,%%r12,8),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm14; vfmadd231ps %%ymm2,%%ymm3,%%ymm15;"\
    "vmovups 32(%0),%%ymm1; vmovups 32(%0,%%r12,8),%%ymm2; addq $64,%0;"\
    "vbroadcastss  24(%1,%%r12,4),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm4;  vfmadd231ps %%ymm2,%%ymm3,%%ymm5;"\
    "vbroadcastss  28(%1,%%r12,4),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm6;  vfmadd231ps %%ymm2,%%ymm3,%%ymm7;"\
    "vbroadcastss  16(%1,%%r12,8),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm8;  vfmadd231ps %%ymm2,%%ymm3,%%ymm9;"\
    "vbroadcastss  20(%1,%%r12,8),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm10; vfmadd231ps %%ymm2,%%ymm3,%%ymm11;"\
    "vbroadcastss  24(%1,%%r12,8),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm12; vfmadd231ps %%ymm2,%%ymm3,%%ymm13;"\
    "vbroadcastss  28(%1,%%r12,8),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm14; vfmadd231ps %%ymm2,%%ymm3,%%ymm15;"\
    "addq $32,%1;"
#define KERNEL_R_k1m8n6 \
    "vmovsldup (%0),%%ymm1; vmovshdup (%0),%%ymm2; prefetcht0 512(%0); addq $32,%0;"\
    "vbroadcastsd   8(%1,%%r12,4),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm4;  vfmadd231ps %%ymm2,%%ymm3,%%ymm5;"\
    "vbroadcastsd    (%1,%%r12,8),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm6;  vfmadd231ps %%ymm2,%%ymm3,%%ymm7;"\
    "vbroadcastsd   8(%1,%%r12,8),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm8;  vfmadd231ps %%ymm2,%%ymm3,%%ymm9;"\
    "addq $16,%1;"
#define KERNEL_R_k2m8n6 \
    "vmovsldup (%0),%%ymm1; vmovshdup (%0),%%ymm2; prefetcht0 512(%0);"\
    "vbroadcastsd   8(%1,%%r12,4),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm4;  vfmadd231ps %%ymm2,%%ymm3,%%ymm5;"\
    "vbroadcastsd    (%1,%%r12,8),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm6;  vfmadd231ps %%ymm2,%%ymm3,%%ymm7;"\
    "vbroadcastsd   8(%1,%%r12,8),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm8;  vfmadd231ps %%ymm2,%%ymm3,%%ymm9;"\
    "vmovsldup 32(%0),%%ymm1; vmovshdup 32(%0),%%ymm2; addq $64,%0;"\
    "vbroadcastsd  24(%1,%%r12,4),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm10; vfmadd231ps %%ymm2,%%ymm3,%%ymm11;"\
    "vbroadcastsd  16(%1,%%r12,8),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm12; vfmadd231ps %%ymm2,%%ymm3,%%ymm13;"\
    "vbroadcastsd  24(%1,%%r12,8),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm14; vfmadd231ps %%ymm2,%%ymm3,%%ymm15;"\
    "addq $32,%1;"
#define INIT_m8n1 "vpxor %%ymm4,%%ymm4,%%ymm4;"
#define INIT_m8n2 INIT_m8n1 "vpxor %%ymm5,%%ymm5,%%ymm5;"
#define unit_init_m8n4(c1,c2,c3,c4) \
    "vpxor "#c1","#c1","#c1";vpxor "#c2","#c2","#c2";vpxor "#c3","#c3","#c3";vpxor "#c4","#c4","#c4";"
#define INIT_m8n8  unit_init_m8n4(%%ymm4,%%ymm5,%%ymm6,%%ymm7) unit_init_m8n4(%%ymm8,%%ymm9,%%ymm10,%%ymm11)
#define INIT_m8n4  INIT_m8n8
#define INIT_m8n12 INIT_m8n8 unit_init_m8n4(%%ymm12,%%ymm13,%%ymm14,%%ymm15)
#define INIT_m8n6  INIT_m8n12
#define INIT_m16n6 INIT_m8n12
#define SAVE_m8n1 "vfmadd213ps (%2),%%ymm0,%%ymm4; vmovups %%ymm4,(%2);"
#define unit_save_m8n2(c1,c2) \
    "vunpcklps "#c2","#c1",%%ymm2; vunpckhps "#c2","#c1",%%ymm3; vunpcklpd %%ymm3,%%ymm2,"#c1"; vunpckhpd %%ymm3,%%ymm2,"#c2";"\
    "vfmadd213ps (%5),%%ymm0,"#c1"; vfmadd213ps (%5,%3,1),%%ymm0,"#c2"; vmovups "#c1",(%5); vmovups "#c2",(%5,%3,1); leaq (%5,%3,2),%5;"
#define SAVE_m8n2 "movq %2,%5;" unit_save_m8n2(%%ymm4,%%ymm5)
#define SAVE_m8n4 "movq %2,%5;"\
    "vaddps %%ymm4,%%ymm8,%%ymm4; vaddps %%ymm5,%%ymm9,%%ymm5; vaddps %%ymm6,%%ymm10,%%ymm6; vaddps %%ymm7,%%ymm11,%%ymm7;"\
    unit_save_m8n2(%%ymm4,%%ymm5)  unit_save_m8n2(%%ymm6,%%ymm7)
#define SAVE_m8n8 "movq %2,%5;"\
    unit_save_m8n2(%%ymm4,%%ymm5)  unit_save_m8n2(%%ymm6,%%ymm7)  unit_save_m8n2(%%ymm8,%%ymm9)   unit_save_m8n2(%%ymm10,%%ymm11)
#define SAVE_m8n12 SAVE_m8n8  unit_save_m8n2(%%ymm12,%%ymm13) unit_save_m8n2(%%ymm14,%%ymm15)
#define unit_save_m16n2(c1,c2,c3,c4) \
    "vfmadd213ps (%5),%%ymm0,"#c1"; vfmadd213ps 32(%5),%%ymm0,"#c2"; vmovups "#c1",(%5); vmovups "#c2",32(%5);"\
    "vfmadd213ps (%5,%3,1),%%ymm0,"#c3"; vfmadd213ps 32(%5,%3,1),%%ymm0,"#c4"; vmovups "#c3",(%5,%3,1); vmovups "#c4",32(%5,%3,1); leaq (%5,%3,2),%5;"
#define SAVE_L_m16n6 "movq %2,%5;"\
    unit_save_m16n2(%%ymm4,%%ymm5,%%ymm6,%%ymm7) unit_save_m16n2(%%ymm8,%%ymm9,%%ymm10,%%ymm11) unit_save_m16n2(%%ymm12,%%ymm13,%%ymm14,%%ymm15)
#define SAVE_R_m16n6 "leaq (%2,%3,4),%5; leaq (%5,%3,2),%5;"\
    unit_save_m16n2(%%ymm4,%%ymm5,%%ymm6,%%ymm7) unit_save_m16n2(%%ymm8,%%ymm9,%%ymm10,%%ymm11) unit_save_m16n2(%%ymm12,%%ymm13,%%ymm14,%%ymm15)
#define SAVE_L_m8n6 "movq %2,%5;"\
    "vaddps %%ymm4,%%ymm10,%%ymm4; vaddps %%ymm5,%%ymm11,%%ymm5; vaddps %%ymm6,%%ymm12,%%ymm6;"\
    "vaddps %%ymm7,%%ymm13,%%ymm7; vaddps %%ymm8,%%ymm14,%%ymm8; vaddps %%ymm9,%%ymm15,%%ymm9;"\
    unit_save_m8n2(%%ymm4,%%ymm5) unit_save_m8n2(%%ymm6,%%ymm7) unit_save_m8n2(%%ymm8,%%ymm9)
#define SAVE_R_m8n6 "leaq (%2,%3,4),%5; leaq (%5,%3,2),%5;"\
    "vaddps %%ymm4,%%ymm10,%%ymm4; vaddps %%ymm5,%%ymm11,%%ymm5; vaddps %%ymm6,%%ymm12,%%ymm6;"\
    "vaddps %%ymm7,%%ymm13,%%ymm7; vaddps %%ymm8,%%ymm14,%%ymm8; vaddps %%ymm9,%%ymm15,%%ymm9;"\
    unit_save_m8n2(%%ymm4,%%ymm5) unit_save_m8n2(%%ymm6,%%ymm7) unit_save_m8n2(%%ymm8,%%ymm9)

/* m = 4 *//* xmm0 for alpha, xmm1-xmm3 for temporary use, xmm4-xmm15 for accumulators */
#define KERNEL_k1m4n1 \
    "vmovups (%0),%%xmm1; addq $16,%0;"\
    "vbroadcastss (%1),%%xmm2; vfmadd231ps %%xmm1,%%xmm2,%%xmm4;"\
    "addq $4,%1;"
#define KERNEL_h_k1m4n2 \
    "vmovsldup (%0),%%xmm1; vmovshdup (%0),%%xmm2; addq $16,%0;"\
    "vmovddup (%1),%%xmm3; vfmadd231ps %%xmm1,%%xmm3,%%xmm4; vfmadd231ps %%xmm2,%%xmm3,%%xmm5;"
#define KERNEL_k1m4n2 KERNEL_h_k1m4n2 "addq $8,%1;"
#define KERNEL_h_k1m4n4 \
    KERNEL_h_k1m4n2 "vmovddup 8(%1),%%xmm3; vfmadd231ps %%xmm1,%%xmm3,%%xmm6; vfmadd231ps %%xmm2,%%xmm3,%%xmm7;"
#define KERNEL_k1m4n4 KERNEL_h_k1m4n4 "addq $16,%1;"
#define unit_kernel_k1m4n4(c1,c2,c3,c4,...) \
    "vmovddup ("#__VA_ARGS__"),%%xmm3; vfmadd231ps %%xmm1,%%xmm3,"#c1"; vfmadd231ps %%xmm2,%%xmm3,"#c2";"\
    "vmovddup 8("#__VA_ARGS__"),%%xmm3; vfmadd231ps %%xmm1,%%xmm3,"#c3"; vfmadd231ps %%xmm2,%%xmm3,"#c4";"
#define KERNEL_h_k1m4n8 KERNEL_h_k1m4n4 unit_kernel_k1m4n4(%%xmm8,%%xmm9,%%xmm10,%%xmm11,%1,%%r12,4)
#define KERNEL_k1m4n8 KERNEL_h_k1m4n8 "addq $16,%1;"
#define KERNEL_h_k1m4n12 KERNEL_h_k1m4n8 unit_kernel_k1m4n4(%%xmm12,%%xmm13,%%xmm14,%%xmm15,%1,%%r12,8)
#define KERNEL_k1m4n12 KERNEL_h_k1m4n12 "addq $16,%1;"
#define INIT_m4n1 "vpxor %%xmm4,%%xmm4,%%xmm4;"
#define INIT_m4n2 INIT_m4n1 "vpxor %%xmm5,%%xmm5,%%xmm5;"
#define INIT_m4n4 INIT_m4n2 "vpxor %%xmm6,%%xmm6,%%xmm6;vpxor %%xmm7,%%xmm7,%%xmm7;"
#define unit_init_m4n4(c1,c2,c3,c4) \
    "vpxor "#c1","#c1","#c1";vpxor "#c2","#c2","#c2";vpxor "#c3","#c3","#c3";vpxor "#c4","#c4","#c4";"
#define INIT_m4n8  INIT_m4n4 unit_init_m4n4(%%xmm8,%%xmm9,%%xmm10,%%xmm11)
#define INIT_m4n12 INIT_m4n8 unit_init_m4n4(%%xmm12,%%xmm13,%%xmm14,%%xmm15)
#define SAVE_m4n1 "vfmadd213ps (%2),%%xmm0,%%xmm4; vmovups %%xmm4,(%2);"
#define unit_save_m4n2(c1,c2) \
    "vunpcklps "#c2","#c1",%%xmm2; vunpckhps "#c2","#c1",%%xmm3; vunpcklpd %%xmm3,%%xmm2,"#c1"; vunpckhpd %%xmm3,%%xmm2,"#c2";"\
    "vfmadd213ps (%5),%%xmm0,"#c1"; vmovups "#c1",(%5);"\
    "vfmadd213ps (%5,%3,1),%%xmm0,"#c2"; vmovups "#c2",(%5,%3,1);"\
    "leaq (%5,%3,2),%5;"
#define SAVE_m4n2 "movq %2,%5;" unit_save_m4n2(%%xmm4,%%xmm5)
#define SAVE_m4n4  SAVE_m4n2  unit_save_m4n2(%%xmm6,%%xmm7)
#define SAVE_m4n8  SAVE_m4n4  unit_save_m4n2(%%xmm8,%%xmm9)   unit_save_m4n2(%%xmm10,%%xmm11)
#define SAVE_m4n12 SAVE_m4n8  unit_save_m4n2(%%xmm12,%%xmm13) unit_save_m4n2(%%xmm14,%%xmm15)

/* m = 2 *//* xmm0 for alpha, xmm1-xmm3 and xmm10 for temporary use, xmm4-xmm9 for accumulators */
#define INIT_m2n1 "vpxor %%xmm4,%%xmm4,%%xmm4;"
#define KERNEL_k1m2n1 \
    "vmovsd (%0),%%xmm1; addq $8,%0;"\
    "vbroadcastss (%1),%%xmm2; vfmadd231ps %%xmm1,%%xmm2,%%xmm4;"\
    "addq $4,%1;"
#define SAVE_m2n1 "vmovsd (%2),%%xmm1; vfmadd213ps %%xmm1,%%xmm0,%%xmm4; vmovsd %%xmm4,(%2);"
#define INIT_m2n2 INIT_m2n1 "vpxor %%xmm5,%%xmm5,%%xmm5;"
#define KERNEL_k1m2n2 \
    "vmovsd (%0),%%xmm1; addq $8,%0;"\
    "vbroadcastss  (%1),%%xmm2; vfmadd231ps %%xmm1,%%xmm2,%%xmm4;"\
    "vbroadcastss 4(%1),%%xmm3; vfmadd231ps %%xmm1,%%xmm3,%%xmm5;"\
    "addq $8,%1;"
#define SAVE_m2n2 SAVE_m2n1 "vmovsd (%2,%3,1),%%xmm1; vfmadd213ps %%xmm1,%%xmm0,%%xmm5; vmovsd %%xmm5,(%2,%3,1);"
#define INIT_m2n4  INIT_m2n2
#define INIT_m2n8  INIT_m2n4 "vpxor %%xmm6,%%xmm6,%%xmm6; vpxor %%xmm7,%%xmm7,%%xmm7;"
#define INIT_m2n12 INIT_m2n8 "vpxor %%xmm8,%%xmm8,%%xmm8; vpxor %%xmm9,%%xmm9,%%xmm9;"
#define KERNEL_k1m2n4 \
    "vmovups (%1),%%xmm3; addq $16,%1;"\
    "vbroadcastss  (%0),%%xmm1; vfmadd231ps %%xmm3,%%xmm1,%%xmm4;"\
    "vbroadcastss 4(%0),%%xmm2; vfmadd231ps %%xmm3,%%xmm2,%%xmm5;"\
    "addq $8,%0;"
#define KERNEL_k1m2n8 \
    "vmovups (%1),%%xmm3; vmovups (%1,%%r12,4),%%xmm2; addq $16,%1;"\
    "vbroadcastss  (%0),%%xmm1; vfmadd231ps %%xmm3,%%xmm1,%%xmm4; vfmadd231ps %%xmm2,%%xmm1,%%xmm6;"\
    "vbroadcastss 4(%0),%%xmm1; vfmadd231ps %%xmm3,%%xmm1,%%xmm5; vfmadd231ps %%xmm2,%%xmm1,%%xmm7;"\
    "addq $8,%0;"
#define KERNEL_k1m2n12 \
    "vmovups (%1),%%xmm3; vmovups (%1,%%r12,4),%%xmm2; vmovups (%1,%%r12,8),%%xmm1; addq $16,%1;"\
    "vbroadcastss  (%0),%%xmm10; vfmadd231ps %%xmm3,%%xmm10,%%xmm4; vfmadd231ps %%xmm2,%%xmm10,%%xmm6; vfmadd231ps %%xmm1,%%xmm10,%%xmm8;"\
    "vbroadcastss 4(%0),%%xmm10; vfmadd231ps %%xmm3,%%xmm10,%%xmm5; vfmadd231ps %%xmm2,%%xmm10,%%xmm7; vfmadd231ps %%xmm1,%%xmm10,%%xmm9;"\
    "addq $8,%0;"
#define unit_save_m2n4(c1,c2) \
    "vunpcklps "#c2","#c1",%%xmm1; vunpckhps "#c2","#c1",%%xmm2;"\
    "vmovsd (%5),%%xmm3; vmovhpd (%5,%3,1),%%xmm3,%%xmm3; vfmadd213ps %%xmm3,%%xmm0,%%xmm1;"\
    "vmovsd %%xmm1,(%5); vmovhpd %%xmm1,(%5,%3,1); leaq (%5,%3,2),%5;"\
    "vmovsd (%5),%%xmm3; vmovhpd (%5,%3,1),%%xmm3,%%xmm3; vfmadd213ps %%xmm3,%%xmm0,%%xmm2;"\
    "vmovsd %%xmm2,(%5); vmovhpd %%xmm2,(%5,%3,1); leaq (%5,%3,2),%5;"
#define SAVE_m2n4 "movq %2,%5;" unit_save_m2n4(%%xmm4,%%xmm5)
#define SAVE_m2n8   SAVE_m2n4   unit_save_m2n4(%%xmm6,%%xmm7)
#define SAVE_m2n12  SAVE_m2n8   unit_save_m2n4(%%xmm8,%%xmm9)

/* m = 1 *//* xmm0 for alpha, xmm1-xmm3 and xmm10 for temporary use, xmm4-xmm6 for accumulators */
#define INIT_m1n1 "vpxor %%xmm4,%%xmm4,%%xmm4;"
#define KERNEL_k1m1n1 \
    "vmovss (%1),%%xmm3; addq $4,%1;"\
    "vmovss (%0),%%xmm1; vfmadd231ss %%xmm3,%%xmm1,%%xmm4;"\
    "addq $4,%0;"
#define SAVE_m1n1 "vfmadd213ss (%2),%%xmm0,%%xmm4; vmovss %%xmm4,(%2);"
#define INIT_m1n2 INIT_m1n1
#define KERNEL_k1m1n2 \
    "vmovsd (%1),%%xmm3; addq $8,%1;"\
    "vbroadcastss  (%0),%%xmm1; vfmadd231ps %%xmm3,%%xmm1,%%xmm4;"\
    "addq $4,%0;"
#define SAVE_m1n2 \
    "vmovss (%2),%%xmm3; vinsertps $16,(%2,%3,1),%%xmm3,%%xmm3; vfmadd213ps %%xmm3,%%xmm0,%%xmm4;"\
    "vmovss %%xmm4,(%2); vextractps $1,%%xmm4,(%2,%3,1);"
#define INIT_m1n4  INIT_m1n2
#define INIT_m1n8  INIT_m1n4 "vpxor %%xmm5,%%xmm5,%%xmm5;"
#define INIT_m1n12 INIT_m1n8 "vpxor %%xmm6,%%xmm6,%%xmm6;"
#define KERNEL_k1m1n4 \
    "vmovups (%1),%%xmm3; addq $16,%1;"\
    "vbroadcastss  (%0),%%xmm1; vfmadd231ps %%xmm3,%%xmm1,%%xmm4;"\
    "addq $4,%0;"
#define KERNEL_k1m1n8 \
    "vmovups (%1),%%xmm3; vmovups (%1,%%r12,4),%%xmm2; addq $16,%1;"\
    "vbroadcastss  (%0),%%xmm1; vfmadd231ps %%xmm3,%%xmm1,%%xmm4; vfmadd231ps %%xmm2,%%xmm1,%%xmm5;"\
    "addq $4,%0;"
#define KERNEL_k1m1n12 \
    "vmovups (%1),%%xmm3; vmovups (%1,%%r12,4),%%xmm2; vmovups (%1,%%r12,8),%%xmm1; addq $16,%1;"\
    "vbroadcastss  (%0),%%xmm10; vfmadd231ps %%xmm3,%%xmm10,%%xmm4; vfmadd231ps %%xmm2,%%xmm10,%%xmm5; vfmadd231ps %%xmm1,%%xmm10,%%xmm6;"\
    "addq $4,%0;"
#define unit_save_m1n4(c1) \
    "vpxor %%xmm10,%%xmm10,%%xmm10; vmovsd "#c1",%%xmm10,%%xmm2; vmovhlps "#c1",%%xmm10,%%xmm1;"\
    "vmovss (%5),%%xmm3; vinsertps $16,(%5,%3,1),%%xmm3,%%xmm3; vfmadd213ps %%xmm3,%%xmm0,%%xmm2;"\
    "vmovss %%xmm2,(%5); vextractps $1,%%xmm2,(%5,%3,1); leaq (%5,%3,2),%5;"\
    "vmovss (%5),%%xmm3; vinsertps $16,(%5,%3,1),%%xmm3,%%xmm3; vfmadd213ps %%xmm3,%%xmm0,%%xmm1;"\
    "vmovss %%xmm1,(%5); vextractps $1,%%xmm1,(%5,%3,1); leaq (%5,%3,2),%5;"
#define SAVE_m1n4 "movq %2,%5;" unit_save_m1n4(%%xmm4)
#define SAVE_m1n8  SAVE_m1n4    unit_save_m1n4(%%xmm5)
#define SAVE_m1n12 SAVE_m1n8    unit_save_m1n4(%%xmm6)

/* %0 = "+r"(a_pointer), %1 = "+r"(b_pointer), %2 = "+r"(c_pointer), %3 = "+r"(ldc_in_bytes), %4 for k_count, %5 for c_store, %6 = b_pref */
/* r10 = tmp, r11 = m_counter, r12 = k << 2(const), r13 = tmp, r14 = b_head_pos(const), r15 = tmp */

#define COMPUTE_SIMPLE(mdim,ndim) \
    "movq %%r12,%4; sarq $2,%4; movq %%r14,%1;" INIT_m##mdim##n##ndim\
    "testq %4,%4; jz 7"#mdim"7"#ndim"2f;"\
    "7"#mdim"7"#ndim"1:\n\t"\
    KERNEL_k1m##mdim##n##ndim "decq %4; jnz 7"#mdim"7"#ndim"1b;"\
    "7"#mdim"7"#ndim"2:\n\t"\
    SAVE_m##mdim##n##ndim "addq $"#mdim"*4,%2;"
#define COMPUTE_m8n1 COMPUTE_SIMPLE(8,1)
#define COMPUTE_m8n2 COMPUTE_SIMPLE(8,2)
#define COMPUTE_m8n8 COMPUTE_SIMPLE(8,8)
#define COMPUTE_m8n12 COMPUTE_SIMPLE(8,12)
#define COMPUTE_m8n4 \
    "movq %%r12,%4; sarq $2,%4; movq %%r14,%1;" INIT_m8n4\
    "cmpq $8,%4; jb 78740f;"\
    "78749:\n\t"\
    KERNEL_k2m8n4 KERNEL_k2m8n4 KERNEL_k2m8n4 KERNEL_k2m8n4\
    "subq $8,%4; cmpq $8,%4; jnb 78749b;"\
    "78740:\n\t"\
    "testq %4,%4; jz 78742f;"\
    "78741:\n\t"\
    KERNEL_k1m8n4 "decq %4; jnz 78741b;"\
    "78742:\n\t"\
    SAVE_m8n4 "addq $32,%2;"
#define COMPUTE_L_m16n6 \
    "movq %%r12,%%r13; sarq $2,%%r13; movq %%r14,%1;" INIT_m16n6\
    "movq %%r13,%4; movq %2,%5; cmpq $16,%%r13; jb 7116762f; movq $14,%4;"\
    "7116761:\n\t"\
    KERNEL_L_k2m16n6 "prefetcht0 128(%1); testq $24,%4; movq $84,%%r15; cmovz %3,%%r15;"\
    KERNEL_L_k2m16n6 "prefetcht1 (%5); subq $63,%5; addq %%r15,%5;"\
    KERNEL_L_k2m16n6 "prefetcht0 128(%1); prefetcht1 (%6); cmpq $198,%4; cmoveq %2,%5;"\
    KERNEL_L_k2m16n6 "addq $16,%6; addq $8,%4; cmpq %4,%%r13; jnb 7116761b;"\
    "movq %2,%5; negq %4; leaq 14(%%r13,%4,1),%4;"\
    "7116762:\n\t"\
    "xorq %%r15,%%r15; testq %4,%4; jz 7116764f;"\
    "7116763:\n\t"\
    "prefetcht0 (%5); prefetcht0 63(%5); addq %3,%5; incq %%r15;"\
    KERNEL_L_k1m16n6 "cmpq $6,%%r15; cmoveq %2,%5; decq %4; jnz 7116763b;"\
    "7116764:\n\t"\
    SAVE_L_m16n6 "addq $32,%2;"
#define COMPUTE_R_m16n6 \
    "movq %%r12,%%r13; sarq $2,%%r13; movq %%r14,%1;" INIT_m16n6\
    "movq %%r13,%4; leaq (%2,%3,4),%5; leaq (%5,%3,2),%5; movq %5,%%r10; cmpq $16,%%r13; jb 7216762f; movq $14,%4;"\
    "7216761:\n\t"\
    KERNEL_R_k2m16n6 "prefetcht0 128(%1,%%r12,8); testq $24,%4; movq $84,%%r15; cmovz %3,%%r15;"\
    KERNEL_R_k2m16n6 "prefetcht1 (%5); subq $63,%5; addq %%r15,%5;"\
    KERNEL_R_k2m16n6 "prefetcht0 128(%1,%%r12,8); prefetcht1 (%6); cmpq $198,%4; cmoveq %%r10,%5;"\
    KERNEL_R_k2m16n6 "addq $16,%6; addq $8,%4; cmpq %4,%%r13; jnb 7216761b;"\
    "movq %%r10,%5; negq %4; leaq 14(%%r13,%4,1),%4;"\
    "7216762:\n\t"\
    "xorq %%r15,%%r15; testq %4,%4; jz 7216764f;"\
    "7216763:\n\t"\
    "prefetcht0 (%5); prefetcht0 63(%5); addq %3,%5; incq %%r15;"\
    KERNEL_R_k1m16n6 "cmpq $6,%%r15; cmoveq %%r10,%5; decq %4; jnz 7216763b;"\
    "7216764:\n\t"\
    "prefetcht0 (%%r14); prefetcht0 64(%%r14);" SAVE_R_m16n6 "addq $32,%2;"
#define COMPUTE_H_m8n6 \
    "movq %%r12,%4; sarq $2,%4; movq %%r14,%1;" INIT_m8n6\
    "cmpq $8,%4; jb 718760f; movq %2,%5; xorq %%r15,%%r15;"\
    "718769:\n\t"\
    KERNEL_L_k2m8n6 KERNEL_L_k2m8n6 "cmpq $62,%%r15; movq $62,%%r15; cmoveq %3,%%r15;"\
    KERNEL_L_k2m8n6 KERNEL_L_k2m8n6 "prefetcht2 (%5); leaq -31(%5,%%r15,1),%5;"\
    "subq $8,%4; cmpq $8,%4; jnb 718769b;"\
    "718760:\n\t"\
    "testq %4,%4; jz 718762f;"\
    "718761:\n\t"\
    KERNEL_L_k1m8n6 "decq %4; jnz 718761b;"\
    "718762:\n\t"\
    SAVE_L_m8n6 "negq %%r12; leaq (%0,%%r12,8),%0; negq %%r12;"
#define COMPUTE_T_m8n6(side,sim) \
    "movq %%r12,%4; sarq $2,%4; movq %%r14,%1;" INIT_m8n6\
    "cmpq $8,%4; jb 72"#sim"8760f;"\
    "72"#sim"8769:\n\t"\
    KERNEL_##side##_k2m8n6 KERNEL_##side##_k2m8n6 KERNEL_##side##_k2m8n6 KERNEL_##side##_k2m8n6\
    "subq $8,%4; cmpq $8,%4; jnb 72"#sim"8769b;"\
    "72"#sim"8760:\n\t"\
    "testq %4,%4; jz 72"#sim"8762f;"\
    "72"#sim"8761:\n\t"\
    KERNEL_##side##_k1m8n6 "decq %4; jnz 72"#sim"8761b;"\
    "72"#sim"8762:\n\t"\
    SAVE_##side##_m8n6 "addq $32,%2;"
#define COMPUTE_NORMAL(ndim) {\
    next_b = b_pointer + ndim * K;\
    __asm__ __volatile__(\
    "vbroadcastss %9,%%ymm0;"\
    "movq %8,%%r12; salq $2,%%r12; movq %1,%%r14; movq %7,%%r11;"\
    "cmpq $8,%%r11;jb 33101"#ndim"f;"\
    "33109"#ndim":\n\t"\
    COMPUTE_m8n##ndim\
    "subq $8,%%r11;cmpq $8,%%r11;jnb 33109"#ndim"b;"\
    "33101"#ndim":\n\t"\
    "cmpq $4,%%r11;jb 33103"#ndim"f;"\
    COMPUTE_SIMPLE(4,ndim) "subq $4,%%r11;"\
    "33103"#ndim":\n\t"\
    "cmpq $2,%%r11;jb 33104"#ndim"f;"\
    COMPUTE_SIMPLE(2,ndim) "subq $2,%%r11;"\
    "33104"#ndim":\n\t"\
    "testq %%r11,%%r11;jz 33105"#ndim"f;"\
    COMPUTE_SIMPLE(1,ndim)\
    "33105"#ndim":\n\t"\
    "movq %%r14,%1; vzeroupper;"\
    :"+r"(a_pointer),"+r"(b_pointer),"+r"(c_pointer),"+r"(ldc_in_bytes),"+r"(k_count),"+r"(ctemp),"+r"(next_b)\
    :"m"(M),"m"(K),"m"(ALPHA):"r10","r11","r12","r13","r14","r15",\
    "xmm0","xmm1","xmm2","xmm3","xmm4","xmm5","xmm6","xmm7","xmm8","xmm9","xmm10","xmm11","xmm12","xmm13","xmm14","xmm15","cc","memory");\
    a_pointer -= M * K; b_pointer += ndim * K; c_pointer += (LDC * ndim - M);\
}
#define COMPUTE_n12 {\
    next_b = b_pointer + 12 * K;\
    __asm__ __volatile__(\
    "vbroadcastss %9,%%ymm0;"\
    "movq %8,%%r12; salq $2,%%r12; movq %1,%%r14; movq %7,%%r11;"\
    "cmpq $16,%%r11;jb 3310112f;"\
    COMPUTE_H_m8n6\
    "3310612:\n\t"\
    COMPUTE_R_m16n6 "subq $8,%%r11; cmpq $16,%%r11;jb 3310712f;"\
    COMPUTE_L_m16n6 "subq $8,%%r11; cmpq $16,%%r11;jnb 3310612b;"\
    COMPUTE_T_m8n6(R,5) "subq $8,%%r11; jmp 3310212f;"\
    "3310712:\n\t"\
    COMPUTE_T_m8n6(L,7) "subq $8,%%r11; jmp 3310212f;"\
    "3310112:\n\t"\
    "cmpq $8,%%r11;jb 3310212f;"\
    COMPUTE_SIMPLE(8,12) "subq $8,%%r11;"\
    "3310212:\n\t"\
    "cmpq $4,%%r11;jb 3310312f;"\
    COMPUTE_SIMPLE(4,12) "subq $4,%%r11;"\
    "3310312:\n\t"\
    "cmpq $2,%%r11;jb 3310412f;"\
    COMPUTE_SIMPLE(2,12) "subq $2,%%r11;"\
    "3310412:\n\t"\
    "testq %%r11,%%r11;jz 3310512f;"\
    COMPUTE_SIMPLE(1,12)\
    "3310512:\n\t"\
    "movq %%r14,%1; vzeroupper;"\
    :"+r"(a_pointer),"+r"(b_pointer),"+r"(c_pointer),"+r"(ldc_in_bytes),"+r"(k_count),"+r"(ctemp),"+r"(next_b)\
    :"m"(M),"m"(K),"m"(ALPHA):"r10","r11","r12","r13","r14","r15",\
    "xmm0","xmm1","xmm2","xmm3","xmm4","xmm5","xmm6","xmm7","xmm8","xmm9","xmm10","xmm11","xmm12","xmm13","xmm14","xmm15","cc","memory");\
    a_pointer -= M * K; b_pointer += 12 * K; c_pointer += (LDC * 12 - M);\
}

#include "common.h"
#include <stdint.h>
int __attribute__ ((noinline))
CNAME(BLASLONG m, BLASLONG n, BLASLONG k, float alpha, float * __restrict__ A, float * __restrict__ B, float * __restrict__ C, BLASLONG LDC){
    if(m==0||n==0||k==0||alpha==(float)0.0) return 0;
    int64_t ldc_in_bytes = (int64_t)LDC * sizeof(float);
    float ALPHA = alpha;
    int64_t M = (int64_t)m, K = (int64_t)k, k_count = 0;
    BLASLONG n_count = n;
    float *a_pointer = A,*b_pointer = B,*c_pointer = C,*ctemp = C,*next_b = B;
    for(;n_count>11;n_count-=12) COMPUTE_n12
    for(;n_count>7;n_count-=8) COMPUTE_NORMAL(8)
    for(;n_count>3;n_count-=4) COMPUTE_NORMAL(4)
    for(;n_count>1;n_count-=2) COMPUTE_NORMAL(2)
    if(n_count>0) COMPUTE_NORMAL(1)
    return 0;
}

