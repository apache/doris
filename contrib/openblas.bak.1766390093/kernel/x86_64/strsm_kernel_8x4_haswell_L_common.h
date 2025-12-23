/* r11 = m_counter, r12 = size_of_k_elements, r13 = kk, r14 = b_head, r15 = a_head */
/* register i/o: %0 = a_ptr, %1 = b_ptr, %2 = c_ptr, %3 = c_tmp, %4 = ldc, %5 = k_counter */
/* memory input: %6 = K, %7 = offset, %8 = {1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0}, %9 = {0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0}, %10 = M */

#define init_m8n4(c1,c2,c3,c4)\
  "vpxor %%ymm"#c1",%%ymm"#c1",%%ymm"#c1"; vpxor %%ymm"#c2",%%ymm"#c2",%%ymm"#c2"; vpxor %%ymm"#c3",%%ymm"#c3",%%ymm"#c3"; vpxor %%ymm"#c4",%%ymm"#c4",%%ymm"#c4";"
#define INIT_m8n4 init_m8n4(4,5,6,7)
#define INIT_m8n8 INIT_m8n4 init_m8n4(8,9,10,11)
#define INIT_m8n12 INIT_m8n8 init_m8n4(12,13,14,15)

#define init_m4n4(c1,c2,c3,c4)\
  "vpxor %%xmm"#c1",%%xmm"#c1",%%xmm"#c1"; vpxor %%xmm"#c2",%%xmm"#c2",%%xmm"#c2"; vpxor %%xmm"#c3",%%xmm"#c3",%%xmm"#c3"; vpxor %%xmm"#c4",%%xmm"#c4",%%xmm"#c4";"
#define INIT_m4n4 init_m4n4(4,5,6,7)
#define INIT_m4n8 INIT_m4n4 init_m4n4(8,9,10,11)
#define INIT_m4n12 INIT_m4n8 init_m4n4(12,13,14,15)

#define init_m2n4(c1,c2)\
  "vpxor %%xmm"#c1",%%xmm"#c1",%%xmm"#c1"; vpxor %%xmm"#c2",%%xmm"#c2",%%xmm"#c2";"
#define INIT_m2n4 init_m2n4(4,5)
#define INIT_m2n8 INIT_m2n4 init_m2n4(6,7)
#define INIT_m2n12 INIT_m2n8 init_m2n4(8,9)

#define init_m1n4(c1) "vpxor %%xmm"#c1",%%xmm"#c1",%%xmm"#c1";"
#define INIT_m1n4 init_m1n4(4)
#define INIT_m1n8 INIT_m1n4 init_m1n4(5)
#define INIT_m1n12 INIT_m1n8 init_m1n4(6)

#define GEMM_KERNEL_k1m8n4 \
  "vmovsldup (%0),%%ymm1; vmovshdup (%0),%%ymm2;"\
  "vbroadcastsd (%1),%%ymm3; vfnmadd231ps %%ymm3,%%ymm1,%%ymm4; vfnmadd231ps %%ymm3,%%ymm2,%%ymm5;"\
  "vbroadcastsd 8(%1),%%ymm3; vfnmadd231ps %%ymm3,%%ymm1,%%ymm6; vfnmadd231ps %%ymm3,%%ymm2,%%ymm7;"
#define GEMM_KERNEL_k1m8n8 GEMM_KERNEL_k1m8n4\
  "vbroadcastsd (%1,%%r12,4),%%ymm3; vfnmadd231ps %%ymm3,%%ymm1,%%ymm8; vfnmadd231ps %%ymm3,%%ymm2,%%ymm9;"\
  "vbroadcastsd 8(%1,%%r12,4),%%ymm3; vfnmadd231ps %%ymm3,%%ymm1,%%ymm10; vfnmadd231ps %%ymm3,%%ymm2,%%ymm11;"
#define GEMM_KERNEL_k1m8n12 GEMM_KERNEL_k1m8n8\
  "vbroadcastsd (%1,%%r12,8),%%ymm3; vfnmadd231ps %%ymm3,%%ymm1,%%ymm12; vfnmadd231ps %%ymm3,%%ymm2,%%ymm13;"\
  "vbroadcastsd 8(%1,%%r12,8),%%ymm3; vfnmadd231ps %%ymm3,%%ymm1,%%ymm14; vfnmadd231ps %%ymm3,%%ymm2,%%ymm15;"

#define GEMM_KERNEL_k1m4n4 \
  "vmovsldup (%0),%%xmm1; vmovshdup (%0),%%xmm2;"\
  "vmovddup (%1),%%xmm3; vfnmadd231ps %%xmm3,%%xmm1,%%xmm4; vfnmadd231ps %%xmm3,%%xmm2,%%xmm5;"\
  "vmovddup 8(%1),%%xmm3; vfnmadd231ps %%xmm3,%%xmm1,%%xmm6; vfnmadd231ps %%xmm3,%%xmm2,%%xmm7;"
#define GEMM_KERNEL_k1m4n8 GEMM_KERNEL_k1m4n4\
  "vmovddup (%1,%%r12,4),%%xmm3; vfnmadd231ps %%xmm3,%%xmm1,%%xmm8; vfnmadd231ps %%xmm3,%%xmm2,%%xmm9;"\
  "vmovddup 8(%1,%%r12,4),%%xmm3; vfnmadd231ps %%xmm3,%%xmm1,%%xmm10; vfnmadd231ps %%xmm3,%%xmm2,%%xmm11;"
#define GEMM_KERNEL_k1m4n12 GEMM_KERNEL_k1m4n8\
  "vmovddup (%1,%%r12,8),%%xmm3; vfnmadd231ps %%xmm3,%%xmm1,%%xmm12; vfnmadd231ps %%xmm3,%%xmm2,%%xmm13;"\
  "vmovddup 8(%1,%%r12,8),%%xmm3; vfnmadd231ps %%xmm3,%%xmm1,%%xmm14; vfnmadd231ps %%xmm3,%%xmm2,%%xmm15;"

#define GEMM_KERNEL_k1m2n4 \
  "vbroadcastss (%0),%%xmm1; vbroadcastss 4(%0),%%xmm2;"\
  "vmovups (%1),%%xmm3; vfnmadd231ps %%xmm3,%%xmm1,%%xmm4; vfnmadd231ps %%xmm3,%%xmm2,%%xmm5;"
#define GEMM_KERNEL_k1m2n8 GEMM_KERNEL_k1m2n4\
  "vmovups (%1,%%r12,4),%%xmm3; vfnmadd231ps %%xmm3,%%xmm1,%%xmm6; vfnmadd231ps %%xmm3,%%xmm2,%%xmm7;"
#define GEMM_KERNEL_k1m2n12 GEMM_KERNEL_k1m2n8\
  "vmovups (%1,%%r12,8),%%xmm3; vfnmadd231ps %%xmm3,%%xmm1,%%xmm8; vfnmadd231ps %%xmm3,%%xmm2,%%xmm9;"

#define GEMM_KERNEL_k1m1n4 "vbroadcastss (%0),%%xmm1; vfnmadd231ps (%1),%%xmm1,%%xmm4;"
#define GEMM_KERNEL_k1m1n8 GEMM_KERNEL_k1m1n4 "vfnmadd231ps (%1,%%r12,4),%%xmm1,%%xmm5;"
#define GEMM_KERNEL_k1m1n12 GEMM_KERNEL_k1m1n8 "vfnmadd231ps (%1,%%r12,8),%%xmm1,%%xmm6;"

#define GEMM_SUM_REORDER_8x4(c1,c2,c3,c4,prefpos)\
  "vunpcklps %%ymm"#c2",%%ymm"#c1",%%ymm0; vunpckhps %%ymm"#c2",%%ymm"#c1",%%ymm1;"\
  "vunpcklps %%ymm"#c4",%%ymm"#c3",%%ymm2; vunpckhps %%ymm"#c4",%%ymm"#c3",%%ymm3;"\
  "vmovups (%3),%%ymm"#c1"; vmovups (%3,%4,1),%%ymm"#c2"; prefetcht1 "#prefpos"(%3); prefetcht1 "#prefpos"(%3,%4,1);"\
  "vunpcklpd %%ymm"#c2",%%ymm"#c1",%%ymm"#c3"; vunpckhpd %%ymm"#c2",%%ymm"#c1",%%ymm"#c4";"\
  "vaddps %%ymm0,%%ymm"#c3",%%ymm0; vaddps %%ymm1,%%ymm"#c4",%%ymm1;"\
  "leaq (%3,%4,2),%3;"\
  "vmovups (%3),%%ymm"#c1"; vmovups (%3,%4,1),%%ymm"#c2"; prefetcht1 "#prefpos"(%3); prefetcht1 "#prefpos"(%3,%4,1);"\
  "vunpcklpd %%ymm"#c2",%%ymm"#c1",%%ymm"#c3"; vunpckhpd %%ymm"#c2",%%ymm"#c1",%%ymm"#c4";"\
  "vaddps %%ymm2,%%ymm"#c3",%%ymm2; vaddps %%ymm3,%%ymm"#c4",%%ymm3;"\
  "leaq (%3,%4,2),%3;"\
  "vperm2f128 $2,%%ymm0,%%ymm2,%%ymm"#c1"; vperm2f128 $2,%%ymm1,%%ymm3,%%ymm"#c2";"\
  "vperm2f128 $19,%%ymm0,%%ymm2,%%ymm"#c3"; vperm2f128 $19,%%ymm1,%%ymm3,%%ymm"#c4";"

#define GEMM_SUM_REORDER_4x4(c1,c2,c3,c4,co1,co2)\
  "vunpcklps %%xmm"#c2",%%xmm"#c1",%%xmm0; vunpckhps %%xmm"#c2",%%xmm"#c1",%%xmm1;"\
  "vunpcklps %%xmm"#c4",%%xmm"#c3",%%xmm2; vunpckhps %%xmm"#c4",%%xmm"#c3",%%xmm3;"\
  "vmovups (%3),%%xmm"#c1"; vmovups (%3,%4,1),%%xmm"#c2";"\
  "vunpcklpd %%xmm"#c2",%%xmm"#c1",%%xmm"#c3"; vunpckhpd %%xmm"#c2",%%xmm"#c1",%%xmm"#c4";"\
  "vaddps %%xmm0,%%xmm"#c3",%%xmm0; vaddps %%xmm1,%%xmm"#c4",%%xmm1;"\
  "leaq (%3,%4,2),%3;"\
  "vmovups (%3),%%xmm"#c1"; vmovups (%3,%4,1),%%xmm"#c2";"\
  "vunpcklpd %%xmm"#c2",%%xmm"#c1",%%xmm"#c3"; vunpckhpd %%xmm"#c2",%%xmm"#c1",%%xmm"#c4";"\
  "vaddps %%xmm2,%%xmm"#c3",%%xmm2; vaddps %%xmm3,%%xmm"#c4",%%xmm3;"\
  "leaq (%3,%4,2),%3;"\
  "vperm2f128 $2,%%ymm0,%%ymm2,%%ymm"#co1"; vperm2f128 $2,%%ymm1,%%ymm3,%%ymm"#co2";"

#define GEMM_SUM_REORDER_2x4(c1,c2,co1)\
  "vunpcklps %%xmm"#c2",%%xmm"#c1",%%xmm0; vunpckhps %%xmm"#c2",%%xmm"#c1",%%xmm1;"\
  "vmovsd (%3),%%xmm2; vmovhpd (%3,%4,1),%%xmm2,%%xmm2; vaddps %%xmm0,%%xmm2,%%xmm0; leaq (%3,%4,2),%3;"\
  "vmovsd (%3),%%xmm2; vmovhpd (%3,%4,1),%%xmm2,%%xmm2; vaddps %%xmm1,%%xmm2,%%xmm1; leaq (%3,%4,2),%3;"\
  "vperm2f128 $2,%%ymm0,%%ymm1,%%ymm"#co1";"

#define GEMM_SUM_REORDER_1x4(c1)\
  "vmovss (%3),%%xmm1; vinsertps $16,(%3,%4,1),%%xmm1,%%xmm1; leaq (%3,%4,2),%3;"\
  "vinsertps $32,(%3),%%xmm1,%%xmm1; vinsertps $48,(%3,%4,1),%%xmm1,%%xmm1; leaq (%3,%4,2),%3;"\
  "vaddps %%xmm"#c1",%%xmm1,%%xmm"#c1";"

#define save_c_m8n4(c1,c2,c3,c4)\
  "vunpcklpd %%ymm"#c2",%%ymm"#c1",%%ymm0; vunpckhpd %%ymm"#c2",%%ymm"#c1",%%ymm1;"\
  "vunpcklpd %%ymm"#c4",%%ymm"#c3",%%ymm2; vunpckhpd %%ymm"#c4",%%ymm"#c3",%%ymm3;"\
  "vperm2f128 $2,%%ymm0,%%ymm2,%%ymm"#c1"; vperm2f128 $2,%%ymm1,%%ymm3,%%ymm"#c2";"\
  "vmovups %%ymm"#c1",(%3); vmovups %%ymm"#c2",(%3,%4,1); leaq (%3,%4,2),%3;"\
  "vperm2f128 $19,%%ymm0,%%ymm2,%%ymm"#c3"; vperm2f128 $19,%%ymm1,%%ymm3,%%ymm"#c4";"\
  "vmovups %%ymm"#c3",(%3); vmovups %%ymm"#c4",(%3,%4,1); leaq (%3,%4,2),%3;"

#define save_c_m4n4(c1,c2)\
  "vunpcklpd %%ymm"#c2",%%ymm"#c1",%%ymm0; vunpckhpd %%ymm"#c2",%%ymm"#c1",%%ymm1;"\
  "vmovups %%xmm0,(%3); vmovups %%xmm1,(%3,%4,1); leaq (%3,%4,2),%3;"\
  "vextractf128 $1,%%ymm0,(%3); vextractf128 $1,%%ymm1,(%3,%4,1); leaq (%3,%4,2),%3;"

#define save_c_m2n4(c1)\
  "vextractf128 $1,%%ymm"#c1",%%xmm1; vmovsd %%xmm"#c1",(%3); vmovhpd %%xmm"#c1",(%3,%4,1); leaq (%3,%4,2),%3;"\
  "vmovsd %%xmm1,(%3); vmovhpd %%xmm1,(%3,%4,1); leaq (%3,%4,2),%3;"

#define save_c_m1n4(c1)\
  "vmovss %%xmm"#c1",(%3); vextractps $1,%%xmm"#c1",(%3,%4,1); leaq (%3,%4,2),%3;"\
  "vextractps $2,%%xmm"#c1",(%3); vextractps $3,%%xmm"#c1",(%3,%4,1); leaq (%3,%4,2),%3;"

#define SOLVE_up_m2n4(a_off,c1)\
  "vbroadcastsd "#a_off"(%0),%%ymm0; vblendps $170,%8,%%ymm0,%%ymm2;"\
  "vmulps %%ymm2,%%ymm"#c1",%%ymm"#c1";"\
  "vmovsldup %%ymm"#c1",%%ymm1;"

#define SOLVE_up_m2n8(a_off,c1,c2)\
  "vbroadcastsd "#a_off"(%0),%%ymm0; vblendps $170,%8,%%ymm0,%%ymm2;"\
  "vmulps %%ymm2,%%ymm"#c1",%%ymm"#c1"; vmulps %%ymm2,%%ymm"#c2",%%ymm"#c2";"\
  "vmovsldup %%ymm"#c1",%%ymm1; vmovsldup %%ymm"#c2",%%ymm2;"

#define SOLVE_up_m2n12(a_off,c1,c2,c3)\
  "vbroadcastsd "#a_off"(%0),%%ymm0; vblendps $170,%8,%%ymm0,%%ymm2;"\
  "vmulps %%ymm2,%%ymm"#c1",%%ymm"#c1"; vmulps %%ymm2,%%ymm"#c2",%%ymm"#c2"; vmulps %%ymm2,%%ymm"#c3",%%ymm"#c3";"\
  "vmovsldup %%ymm"#c1",%%ymm1; vmovsldup %%ymm"#c2",%%ymm2; vmovsldup %%ymm"#c3",%%ymm3;"

#define SOLVE_uplo_m2n4(a_off,c1) SOLVE_up_m2n4(a_off,c1)\
  "vblendps $85,%9,%%ymm0,%%ymm0; vfnmadd231ps %%ymm0,%%ymm1,%%ymm"#c1";"

#define SOLVE_uplo_m2n8(a_off,c1,c2) SOLVE_up_m2n8(a_off,c1,c2)\
  "vblendps $85,%9,%%ymm0,%%ymm0; vfnmadd231ps %%ymm0,%%ymm1,%%ymm"#c1"; vfnmadd231ps %%ymm0,%%ymm2,%%ymm"#c2";"

#define SOLVE_uplo_m2n12(a_off,c1,c2,c3) SOLVE_up_m2n12(a_off,c1,c2,c3)\
  "vblendps $85,%9,%%ymm0,%%ymm0; vfnmadd231ps %%ymm0,%%ymm1,%%ymm"#c1"; vfnmadd231ps %%ymm0,%%ymm2,%%ymm"#c2"; vfnmadd231ps %%ymm0,%%ymm3,%%ymm"#c3";"

#define SOLVE_lo_m2n4(a_off,c1)\
  "vbroadcastsd "#a_off"(%0),%%ymm0; vblendps $85,%8,%%ymm0,%%ymm2;"\
  "vmulps %%ymm2,%%ymm"#c1",%%ymm"#c1";"\
  "vmovshdup %%ymm"#c1",%%ymm1;"

#define SOLVE_lo_m2n8(a_off,c1,c2)\
  "vbroadcastsd "#a_off"(%0),%%ymm0; vblendps $85,%8,%%ymm0,%%ymm2;"\
  "vmulps %%ymm2,%%ymm"#c1",%%ymm"#c1"; vmulps %%ymm2,%%ymm"#c2",%%ymm"#c2";"\
  "vmovshdup %%ymm"#c1",%%ymm1; vmovshdup %%ymm"#c2",%%ymm2;"

#define SOLVE_lo_m2n12(a_off,c1,c2,c3)\
  "vbroadcastsd "#a_off"(%0),%%ymm0; vblendps $85,%8,%%ymm0,%%ymm2;"\
  "vmulps %%ymm2,%%ymm"#c1",%%ymm"#c1"; vmulps %%ymm2,%%ymm"#c2",%%ymm"#c2"; vmulps %%ymm2,%%ymm"#c3",%%ymm"#c3";"\
  "vmovshdup %%ymm"#c1",%%ymm1; vmovshdup %%ymm"#c2",%%ymm2; vmovshdup %%ymm"#c3",%%ymm3;"

#define SOLVE_loup_m2n4(a_off,c1) SOLVE_lo_m2n4(a_off,c1)\
  "vblendps $170,%9,%%ymm0,%%ymm0; vfnmadd231ps %%ymm0,%%ymm1,%%ymm"#c1";"

#define SOLVE_loup_m2n8(a_off,c1,c2) SOLVE_lo_m2n8(a_off,c1,c2)\
  "vblendps $170,%9,%%ymm0,%%ymm0; vfnmadd231ps %%ymm0,%%ymm1,%%ymm"#c1"; vfnmadd231ps %%ymm0,%%ymm2,%%ymm"#c2";"

#define SOLVE_loup_m2n12(a_off,c1,c2,c3) SOLVE_lo_m2n12(a_off,c1,c2,c3)\
  "vblendps $170,%9,%%ymm0,%%ymm0; vfnmadd231ps %%ymm0,%%ymm1,%%ymm"#c1"; vfnmadd231ps %%ymm0,%%ymm2,%%ymm"#c2"; vfnmadd231ps %%ymm0,%%ymm3,%%ymm"#c3";"

#define SOLVE_m1n4(a_off,c1) "vbroadcastss "#a_off"(%0),%%xmm0; vmulps %%xmm0,%%xmm"#c1",%%xmm"#c1";"
#define SOLVE_m1n8(a_off,c1,c2) SOLVE_m1n4(a_off,c1) "vmulps %%xmm0,%%xmm"#c2",%%xmm"#c2";"
#define SOLVE_m1n12(a_off,c1,c2,c3) SOLVE_m1n8(a_off,c1,c2) "vmulps %%xmm0,%%xmm"#c3",%%xmm"#c3";"

#define SUBTRACT_m2n4(a_off,c1) "vbroadcastsd "#a_off"(%0),%%ymm0; vfnmadd231ps %%ymm0,%%ymm1,%%ymm"#c1";"
#define SUBTRACT_m2n8(a_off,c1,c2) SUBTRACT_m2n4(a_off,c1) "vfnmadd231ps %%ymm0,%%ymm2,%%ymm"#c2";"
#define SUBTRACT_m2n12(a_off,c1,c2,c3) SUBTRACT_m2n8(a_off,c1,c2) "vfnmadd231ps %%ymm0,%%ymm3,%%ymm"#c3";"

#define save_b_m2n4(c1,tmp,b_off,...)\
  "vpermilps $216,%%ymm"#c1",%%ymm"#tmp"; vpermpd $216,%%ymm"#tmp",%%ymm"#tmp"; vmovups %%ymm"#tmp","#b_off"("#__VA_ARGS__");"

#define SAVE_b_m2n4(b_off,c1) save_b_m2n4(c1,1,b_off,%1)
#define SAVE_b_m2n8(b_off,c1,c2) SAVE_b_m2n4(b_off,c1) save_b_m2n4(c2,2,b_off,%1,%%r12,4)
#define SAVE_b_m2n12(b_off,c1,c2,c3) SAVE_b_m2n8(b_off,c1,c2) save_b_m2n4(c3,3,b_off,%1,%%r12,8)

#define SAVE_b_m1n4(b_off,c1) "vmovups %%xmm"#c1","#b_off"(%1);"
#define SAVE_b_m1n8(b_off,c1,c2) SAVE_b_m1n4(b_off,c1) "vmovups %%xmm"#c2","#b_off"(%1,%%r12,4);"
#define SAVE_b_m1n12(b_off,c1,c2,c3) SAVE_b_m1n8(b_off,c1,c2) "vmovups %%xmm"#c3","#b_off"(%1,%%r12,8);"

