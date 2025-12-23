/* r11 = m_counter, r12 = size_of_k_elements, r13 = kk, r14 = b_head, r15 = a_head */
/* register i/o: %0 = a_ptr, %1 = b_ptr, %2 = c_ptr, %3 = c_tmp, %4 = ldc, %5 = k_counter */
/* memory input: %6 = K, %7 = offset, %8 = {1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0}, %9 = {0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0}, %10 = M */

#define init_m8n4(c1,c2,c3,c4)\
  "vpxor %%ymm"#c1",%%ymm"#c1",%%ymm"#c1"; vpxor %%ymm"#c2",%%ymm"#c2",%%ymm"#c2";"\
  "vpxor %%ymm"#c3",%%ymm"#c3",%%ymm"#c3"; vpxor %%ymm"#c4",%%ymm"#c4",%%ymm"#c4";"
#define INIT_m8n4 init_m8n4(4,5,6,7)
#define INIT_m8n8 INIT_m8n4 init_m8n4(8,9,10,11)
#define INIT_m8n12 INIT_m8n8 init_m8n4(12,13,14,15)

#define init_m4n4(c1,c2,c3,c4)\
  "vpxor %%xmm"#c1",%%xmm"#c1",%%xmm"#c1"; vpxor %%xmm"#c2",%%xmm"#c2",%%xmm"#c2";"\
  "vpxor %%xmm"#c3",%%xmm"#c3",%%xmm"#c3"; vpxor %%xmm"#c4",%%xmm"#c4",%%xmm"#c4";"
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
  "vmovups (%3),%%ymm0; vmovups (%3,%4,1),%%ymm1; prefetcht1 "#prefpos"(%3); prefetcht1 "#prefpos"(%3,%4,1); leaq (%3,%4,2),%3;"\
  "vunpcklps %%ymm1,%%ymm0,%%ymm2; vunpckhps %%ymm1,%%ymm0,%%ymm3; vunpcklpd %%ymm3,%%ymm2,%%ymm0; vunpckhpd %%ymm3,%%ymm2,%%ymm1;"\
  "vaddps %%ymm0,%%ymm"#c1",%%ymm"#c1"; vaddps %%ymm1,%%ymm"#c2",%%ymm"#c2";"\
  "vmovups (%3),%%ymm0; vmovups (%3,%4,1),%%ymm1; prefetcht1 "#prefpos"(%3); prefetcht1 "#prefpos"(%3,%4,1); leaq (%3,%4,2),%3;"\
  "vunpcklps %%ymm1,%%ymm0,%%ymm2; vunpckhps %%ymm1,%%ymm0,%%ymm3; vunpcklpd %%ymm3,%%ymm2,%%ymm0; vunpckhpd %%ymm3,%%ymm2,%%ymm1;"\
  "vaddps %%ymm0,%%ymm"#c3",%%ymm"#c3"; vaddps %%ymm1,%%ymm"#c4",%%ymm"#c4";"

#define GEMM_SUM_REORDER_4x4(c1,c2,c3,c4,co1,co2)\
  "vmovups (%3),%%xmm0; vmovups (%3,%4,1),%%xmm1; leaq (%3,%4,2),%3;"\
  "vunpcklps %%xmm1,%%xmm0,%%xmm2; vunpckhps %%xmm1,%%xmm0,%%xmm3;"\
  "vunpcklpd %%xmm"#c2",%%xmm"#c1",%%xmm0; vunpckhpd %%xmm"#c2",%%xmm"#c1",%%xmm1;"\
  "vaddps %%xmm0,%%xmm2,%%xmm"#c1"; vaddps %%xmm1,%%xmm3,%%xmm"#c2";"\
  "vmovups (%3),%%xmm0; vmovups (%3,%4,1),%%xmm1; leaq (%3,%4,2),%3;"\
  "vunpcklps %%xmm1,%%xmm0,%%xmm2; vunpckhps %%xmm1,%%xmm0,%%xmm3;"\
  "vunpcklpd %%xmm"#c4",%%xmm"#c3",%%xmm0; vunpckhpd %%xmm"#c4",%%xmm"#c3",%%xmm1;"\
  "vaddps %%xmm0,%%xmm2,%%xmm"#c3"; vaddps %%xmm1,%%xmm3,%%xmm"#c4";"\
  "vperm2f128 $2,%%ymm"#c1",%%ymm"#c2",%%ymm"#co1"; vperm2f128 $2,%%ymm"#c3",%%ymm"#c4",%%ymm"#co2";"

#define GEMM_SUM_REORDER_2x4(c1,c2)\
  "vmovsd (%3),%%xmm0; vmovhpd (%3,%4,1),%%xmm0,%%xmm0; leaq (%3,%4,2),%3; vpermilps $216,%%xmm0,%%xmm0;"\
  "vmovsd (%3),%%xmm1; vmovhpd (%3,%4,1),%%xmm1,%%xmm1; leaq (%3,%4,2),%3; vpermilps $216,%%xmm1,%%xmm1;"\
  "vunpcklpd %%xmm1,%%xmm0,%%xmm2; vaddps %%xmm2,%%xmm"#c1",%%xmm"#c1";"\
  "vunpckhpd %%xmm1,%%xmm0,%%xmm3; vaddps %%xmm3,%%xmm"#c2",%%xmm"#c2";"\

#define GEMM_SUM_REORDER_1x4(c1)\
  "vmovss (%3),%%xmm1; vinsertps $16,(%3,%4,1),%%xmm1,%%xmm1; leaq (%3,%4,2),%3;"\
  "vinsertps $32,(%3),%%xmm1,%%xmm1; vinsertps $48,(%3,%4,1),%%xmm1,%%xmm1; leaq (%3,%4,2),%3;"\
  "vaddps %%xmm"#c1",%%xmm1,%%xmm"#c1";"

#define SOLVE_le_m4n2(b_off,c1,...)\
  "vbroadcastsd "#b_off"("#__VA_ARGS__"),%%ymm0; vblendps $170,%8,%%ymm0,%%ymm2;"\
  "vmulps %%ymm2,%%ymm"#c1",%%ymm"#c1";"\
  "vmovsldup %%ymm"#c1",%%ymm1;"

#define SOLVE_le_m8n2(b_off,c1,c2,...)\
  "vbroadcastsd "#b_off"("#__VA_ARGS__"),%%ymm0; vblendps $170,%8,%%ymm0,%%ymm2;"\
  "vmulps %%ymm2,%%ymm"#c1",%%ymm"#c1"; vmulps %%ymm2,%%ymm"#c2",%%ymm"#c2";"\
  "vmovsldup %%ymm"#c1",%%ymm1; vmovsldup %%ymm"#c2",%%ymm2;"

#define SOLVE_leri_m4n2(b_off,c1,...) SOLVE_le_m4n2(b_off,c1,__VA_ARGS__)\
  "vblendps $85,%9,%%ymm0,%%ymm0; vfnmadd231ps %%ymm0,%%ymm1,%%ymm"#c1";"

#define SOLVE_leri_m8n2(b_off,c1,c2,...) SOLVE_le_m8n2(b_off,c1,c2,__VA_ARGS__)\
  "vblendps $85,%9,%%ymm0,%%ymm0; vfnmadd231ps %%ymm0,%%ymm1,%%ymm"#c1"; vfnmadd231ps %%ymm0,%%ymm2,%%ymm"#c2";"

#define SOLVE_ri_m4n2(b_off,c1,...)\
  "vbroadcastsd "#b_off"("#__VA_ARGS__"),%%ymm0; vblendps $85,%8,%%ymm0,%%ymm2;"\
  "vmulps %%ymm2,%%ymm"#c1",%%ymm"#c1";"\
  "vmovshdup %%ymm"#c1",%%ymm1;"

#define SOLVE_ri_m8n2(b_off,c1,c2,...)\
  "vbroadcastsd "#b_off"("#__VA_ARGS__"),%%ymm0; vblendps $85,%8,%%ymm0,%%ymm2;"\
  "vmulps %%ymm2,%%ymm"#c1",%%ymm"#c1"; vmulps %%ymm2,%%ymm"#c2",%%ymm"#c2";"\
  "vmovshdup %%ymm"#c1",%%ymm1; vmovshdup %%ymm"#c2",%%ymm2;"

#define SOLVE_rile_m4n2(b_off,c1,...) SOLVE_ri_m4n2(b_off,c1,__VA_ARGS__)\
  "vblendps $170,%9,%%ymm0,%%ymm0; vfnmadd231ps %%ymm0,%%ymm1,%%ymm"#c1";"

#define SOLVE_rile_m8n2(b_off,c1,c2,...) SOLVE_ri_m8n2(b_off,c1,c2,__VA_ARGS__)\
  "vblendps $170,%9,%%ymm0,%%ymm0; vfnmadd231ps %%ymm0,%%ymm1,%%ymm"#c1"; vfnmadd231ps %%ymm0,%%ymm2,%%ymm"#c2";"

#define SOLVE_col1_rtol_m1n4(b_off,c1,...)\
  "vmovups "#b_off"("#__VA_ARGS__"),%%xmm0; vblendps $14,%8,%%xmm0,%%xmm2;"\
  "vmulps %%xmm2,%%xmm"#c1",%%xmm"#c1";"\
  "vpermilps $0,%%xmm"#c1",%%xmm1;"

#define SOLVE_col1_rtol_m2n4(b_off,c1,c2,...)\
  "vmovups "#b_off"("#__VA_ARGS__"),%%xmm0; vblendps $14,%8,%%xmm0,%%xmm2;"\
  "vmulps %%xmm2,%%xmm"#c1",%%xmm"#c1"; vmulps %%xmm2,%%xmm"#c2",%%xmm"#c2";"\
  "vpermilps $0,%%xmm"#c1",%%xmm1; vpermilps $0,%%xmm"#c2",%%xmm2;"

#define SOLVE_col1_ltor_m1n4(b_off,c1,...) SOLVE_col1_rtol_m1n4(b_off,c1,__VA_ARGS__)\
  "vblendps $1,%9,%%xmm0,%%xmm0; vfnmadd231ps %%xmm0,%%xmm1,%%xmm"#c1";"

#define SOLVE_col1_ltor_m2n4(b_off,c1,c2,...) SOLVE_col1_rtol_m2n4(b_off,c1,c2,__VA_ARGS__)\
  "vblendps $1,%9,%%xmm0,%%xmm0; vfnmadd231ps %%xmm0,%%xmm1,%%xmm"#c1"; vfnmadd231ps %%xmm0,%%xmm2,%%xmm"#c2";"

#define SOLVE_col2_mul_m1n4(b_off,c1,...)\
  "vmovups "#b_off"("#__VA_ARGS__"),%%xmm0; vblendps $13,%8,%%xmm0,%%xmm2;"\
  "vmulps %%xmm2,%%xmm"#c1",%%xmm"#c1";"\
  "vpermilps $85,%%xmm"#c1",%%xmm1;"

#define SOLVE_col2_mul_m2n4(b_off,c1,c2,...)\
  "vmovups "#b_off"("#__VA_ARGS__"),%%xmm0; vblendps $13,%8,%%xmm0,%%xmm2;"\
  "vmulps %%xmm2,%%xmm"#c1",%%xmm"#c1"; vmulps %%xmm2,%%xmm"#c2",%%xmm"#c2";"\
  "vpermilps $85,%%xmm"#c1",%%xmm1; vpermilps $85,%%xmm"#c2",%%xmm2;"

#define SOLVE_col2_rtol_m1n4(b_off,c1,...) SOLVE_col2_mul_m1n4(b_off,c1,__VA_ARGS__)\
  "vblendps $14,%9,%%xmm0,%%xmm0; vfnmadd231ps %%xmm0,%%xmm1,%%xmm"#c1";"

#define SOLVE_col2_rtol_m2n4(b_off,c1,c2,...) SOLVE_col2_mul_m2n4(b_off,c1,c2,__VA_ARGS__)\
  "vblendps $14,%9,%%xmm0,%%xmm0; vfnmadd231ps %%xmm0,%%xmm1,%%xmm"#c1"; vfnmadd231ps %%xmm0,%%xmm2,%%xmm"#c2";"

#define SOLVE_col2_ltor_m1n4(b_off,c1,...) SOLVE_col2_mul_m1n4(b_off,c1,__VA_ARGS__)\
  "vblendps $3,%9,%%xmm0,%%xmm0; vfnmadd231ps %%xmm0,%%xmm1,%%xmm"#c1";"

#define SOLVE_col2_ltor_m2n4(b_off,c1,c2,...) SOLVE_col2_mul_m2n4(b_off,c1,c2,__VA_ARGS__)\
  "vblendps $3,%9,%%xmm0,%%xmm0; vfnmadd231ps %%xmm0,%%xmm1,%%xmm"#c1"; vfnmadd231ps %%xmm0,%%xmm2,%%xmm"#c2";"

#define SOLVE_col3_mul_m1n4(b_off,c1,...)\
  "vmovups "#b_off"("#__VA_ARGS__"),%%xmm0; vblendps $11,%8,%%xmm0,%%xmm2;"\
  "vmulps %%xmm2,%%xmm"#c1",%%xmm"#c1";"\
  "vpermilps $170,%%xmm"#c1",%%xmm1;"

#define SOLVE_col3_mul_m2n4(b_off,c1,c2,...)\
  "vmovups "#b_off"("#__VA_ARGS__"),%%xmm0; vblendps $11,%8,%%xmm0,%%xmm2;"\
  "vmulps %%xmm2,%%xmm"#c1",%%xmm"#c1"; vmulps %%xmm2,%%xmm"#c2",%%xmm"#c2";"\
  "vpermilps $170,%%xmm"#c1",%%xmm1; vpermilps $170,%%xmm"#c2",%%xmm2;"

#define SOLVE_col3_rtol_m1n4(b_off,c1,...) SOLVE_col3_mul_m1n4(b_off,c1,__VA_ARGS__)\
  "vblendps $12,%9,%%xmm0,%%xmm0; vfnmadd231ps %%xmm0,%%xmm1,%%xmm"#c1";"

#define SOLVE_col3_rtol_m2n4(b_off,c1,c2,...) SOLVE_col3_mul_m2n4(b_off,c1,c2,__VA_ARGS__)\
  "vblendps $12,%9,%%xmm0,%%xmm0; vfnmadd231ps %%xmm0,%%xmm1,%%xmm"#c1"; vfnmadd231ps %%xmm0,%%xmm2,%%xmm"#c2";"

#define SOLVE_col3_ltor_m1n4(b_off,c1,...) SOLVE_col3_mul_m1n4(b_off,c1,__VA_ARGS__)\
  "vblendps $7,%9,%%xmm0,%%xmm0; vfnmadd231ps %%xmm0,%%xmm1,%%xmm"#c1";"

#define SOLVE_col3_ltor_m2n4(b_off,c1,c2,...) SOLVE_col3_mul_m2n4(b_off,c1,c2,__VA_ARGS__)\
  "vblendps $7,%9,%%xmm0,%%xmm0; vfnmadd231ps %%xmm0,%%xmm1,%%xmm"#c1"; vfnmadd231ps %%xmm0,%%xmm2,%%xmm"#c2";"

#define SOLVE_col4_ltor_m1n4(b_off,c1,...)\
  "vmovups "#b_off"("#__VA_ARGS__"),%%xmm0; vblendps $7,%8,%%xmm0,%%xmm2;"\
  "vmulps %%xmm2,%%xmm"#c1",%%xmm"#c1";"\
  "vpermilps $255,%%xmm"#c1",%%xmm1;"

#define SOLVE_col4_ltor_m2n4(b_off,c1,c2,...)\
  "vmovups "#b_off"("#__VA_ARGS__"),%%xmm0; vblendps $7,%8,%%xmm0,%%xmm2;"\
  "vmulps %%xmm2,%%xmm"#c1",%%xmm"#c1"; vmulps %%xmm2,%%xmm"#c2",%%xmm"#c2";"\
  "vpermilps $255,%%xmm"#c1",%%xmm1; vpermilps $255,%%xmm"#c2",%%xmm2;"

#define SOLVE_col4_rtol_m1n4(b_off,c1,...) SOLVE_col4_ltor_m1n4(b_off,c1,__VA_ARGS__)\
  "vblendps $8,%9,%%xmm0,%%xmm0; vfnmadd231ps %%xmm0,%%xmm1,%%xmm"#c1";"

#define SOLVE_col4_rtol_m2n4(b_off,c1,c2,...) SOLVE_col4_ltor_m2n4(b_off,c1,c2,__VA_ARGS__)\
  "vblendps $8,%9,%%xmm0,%%xmm0; vfnmadd231ps %%xmm0,%%xmm1,%%xmm"#c1"; vfnmadd231ps %%xmm0,%%xmm2,%%xmm"#c2";"

#define SUBTRACT_m4n2(b_off,c1,...) "vbroadcastsd "#b_off"("#__VA_ARGS__"),%%ymm0; vfnmadd231ps %%ymm0,%%ymm1,%%ymm"#c1";"

#define SUBTRACT_m8n2(b_off,c1,c2,...) SUBTRACT_m4n2(b_off,c1,__VA_ARGS__) "vfnmadd231ps %%ymm0,%%ymm2,%%ymm"#c2";"

#define SUBTRACT_m1n4(b_off,c1,...) "vmovups "#b_off"("#__VA_ARGS__"),%%xmm0; vfnmadd231ps %%xmm0,%%xmm1,%%xmm"#c1";"

#define SUBTRACT_m2n4(b_off,c1,c2,...) SUBTRACT_m1n4(b_off,c1,__VA_ARGS__) "vfnmadd231ps %%xmm0,%%xmm2,%%xmm"#c2";"

#define SAVE_SOLUTION_m8n2(c1,c2,a_off)\
  "vunpcklps %%ymm"#c2",%%ymm"#c1",%%ymm0; vunpckhps %%ymm"#c2",%%ymm"#c1",%%ymm1;"\
  "vunpcklpd %%ymm1,%%ymm0,%%ymm"#c1"; vunpckhpd %%ymm1,%%ymm0,%%ymm"#c2";"\
  "vmovups %%ymm"#c1","#a_off"(%0); vmovups %%ymm"#c2","#a_off"+32(%0);"\
  "vmovups %%ymm"#c1",(%3); vmovups %%ymm"#c2",(%3,%4,1); leaq (%3,%4,2),%3;"

#define SAVE_SOLUTION_m4n2(c1,a_off)\
  "vpermilps $216,%%ymm"#c1",%%ymm"#c1"; vpermpd $216,%%ymm"#c1",%%ymm"#c1";"\
  "vmovups %%ymm"#c1","#a_off"(%0); vmovups %%xmm"#c1",(%3); vextractf128 $1,%%ymm"#c1",(%3,%4,1); leaq (%3,%4,2),%3;"

#define SAVE_SOLUTION_m2n4(c1,c2,a_off)\
  "vunpcklps %%xmm"#c2",%%xmm"#c1",%%xmm0; vmovups %%xmm0,"#a_off"(%0); vmovsd %%xmm0,(%3); vmovhpd %%xmm0,(%3,%4,1); leaq (%3,%4,2),%3;"\
  "vunpckhps %%xmm"#c2",%%xmm"#c1",%%xmm0; vmovups %%xmm0,"#a_off"+16(%0); vmovsd %%xmm0,(%3); vmovhpd %%xmm0,(%3,%4,1); leaq (%3,%4,2),%3;"

#define SAVE_SOLUTION_m1n4(c1,a_off)\
  "vmovups %%xmm"#c1","#a_off"(%0); vmovss %%xmm"#c1",(%3); vextractps $1,%%xmm"#c1",(%3,%4,1); leaq (%3,%4,2),%3;"\
  "vextractps $2,%%xmm"#c1",(%3); vextractps $3,%%xmm"#c1",(%3,%4,1); leaq (%3,%4,2),%3;"
