/* %0 = "+r"(a_pointer), %1 = "+r"(b_pointer), %2 = "+r"(c_pointer), %3 = "+r"(ldc_in_bytes), %4 for k_count, %5 for c_store, %6 = b_pref */
/* r10 to assist prefetch, r11 = m_counter, r12 = k << 4(const), r13 = k_todo, r14 = b_head_pos(const), r15 = %1 + 3r12 */

#include "common.h"
#include <stdint.h>
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
  #define BACKWARDS 1
#else
  #define BACKWARDS 0
#endif
#define REC_POINTER_1(ptr) "salq $2,%%r13; subq %%r13,"#ptr"; sarq $2,%%r13;"
#define REC_POINTER_2(ptr) "salq $3,%%r13; subq %%r13,"#ptr"; sarq $3,%%r13;"
#define REC_POINTER_4(ptr) "salq $4,%%r13; subq %%r13,"#ptr"; sarq $4,%%r13;"
#define REC_POINTER_8(ptr) "salq $5,%%r13; subq %%r13,"#ptr"; sarq $5,%%r13;"
#define REC_POINTER_16(ptr) "salq $6,%%r13; subq %%r13,"#ptr"; sarq $6,%%r13;"
#define INC_POINTER_1(ptr) "sarq $2,%%r12; addq %%r12,"#ptr"; salq $2,%%r12;"
#define INC_POINTER_2(ptr) "sarq $1,%%r12; addq %%r12,"#ptr"; salq $1,%%r12;"
#define INC_POINTER_4(ptr) "addq %%r12,"#ptr";"
#define INC_POINTER_8(ptr) "leaq ("#ptr",%%r12,2),"#ptr";"
#define INC_POINTER_16(ptr) "leaq ("#ptr",%%r12,4),"#ptr";"
#define SET_POINTER(ptr,dim) REC_POINTER_##dim(ptr) INC_POINTER_##dim(ptr)
#define SET_PB_1 SET_POINTER(%1,1)
#define SET_PB_2 SET_POINTER(%1,2)
#define SET_PB_4 SET_POINTER(%1,4)
#define SET_PB_8 SET_POINTER(%1,4)
#define SET_PB_12 SET_POINTER(%1,4)
#define SET_PB_16 SET_POINTER(%1,4)
#define SET_PB_20 SET_POINTER(%1,4)
#define SET_PB_24 SET_POINTER(%1,4)
#ifdef TRMMKERNEL
  #if BACKWARDS == 1
    #define START_SET_PAPB(mdim,ndim) SET_POINTER(%0,mdim) "movq %%r14,%1;" SET_PB_##ndim "leaq (%1,%%r12,2),%%r15; addq %%r12,%%r15;"
    #define END_SET_PA(mdim) ""
  #else
    #define START_SET_PAPB(mdim,ndim) "movq %%r14,%1; leaq (%1,%%r12,2),%%r15; addq %%r12,%%r15;"
    #define END_SET_PA(mdim) SET_POINTER(%0,mdim)
  #endif
#else
  #define START_SET_PAPB(mdim,ndim) "movq %%r14,%1; leaq (%1,%%r12,2),%%r15; addq %%r12,%%r15;"
  #define END_SET_PA(mdim) ""
#endif
#define RECOVER_PA(mdim) REC_POINTER_##mdim(%0)

#if defined(TRMMKERNEL) && !defined(LEFT)
  #if BACKWARDS == 1
    #define KERNEL_HEAD_C_n8(mdim) \
      KERNEL_k1m##mdim##n4 KERNEL_k1m##mdim##n4 KERNEL_k1m##mdim##n4 KERNEL_k1m##mdim##n4 "subq $4,%4; addq $64,%%r15;"
    #define KERNEL_HEAD_C_n12(mdim) KERNEL_HEAD_C_n8(mdim)\
      KERNEL_k1m##mdim##n8 KERNEL_k1m##mdim##n8 KERNEL_k1m##mdim##n8 KERNEL_k1m##mdim##n8 "subq $4,%4; addq $64,%%r15;"
    #define KERNEL_HEAD_C_n16(mdim) KERNEL_HEAD_C_n12(mdim)\
      KERNEL_k1m##mdim##n12 KERNEL_k1m##mdim##n12 KERNEL_k1m##mdim##n12 KERNEL_k1m##mdim##n12 "subq $4,%4; addq $64,%%r15;"
    #define KERNEL_HEAD_C_n20(mdim) KERNEL_HEAD_C_n16(mdim)\
      KERNEL_k1m##mdim##n16 KERNEL_k1m##mdim##n16 KERNEL_k1m##mdim##n16 KERNEL_k1m##mdim##n16 "subq $4,%4;"
    #define KERNEL_HEAD_C_n24(mdim) KERNEL_HEAD_C_n20(mdim)\
      KERNEL_k1m##mdim##n20 KERNEL_k1m##mdim##n20 KERNEL_k1m##mdim##n20 KERNEL_k1m##mdim##n20 "subq $4,%4;"
    #define KERNEL_HEAD_R_n4(mdim) "subq $12,%4; addq $64,%%r15; addq $"#mdim"*48,%0;"
    #define KERNEL_HEAD_R_n8(mdim) KERNEL_HEAD_R_n4(mdim)\
      kernel_k1m##mdim##n4(%%r15) kernel_k1m##mdim##n4(%%r15) kernel_k1m##mdim##n4(%%r15) kernel_k1m##mdim##n4(%%r15) "subq $4,%4;"
    #define KERNEL_HEAD_R_n12(mdim) KERNEL_HEAD_R_n8(mdim)\
      kernel_k1m##mdim##n8(%%r15) kernel_k1m##mdim##n8(%%r15) kernel_k1m##mdim##n8(%%r15) kernel_k1m##mdim##n8(%%r15) "subq $4,%4;"
    #define KERNEL_TAIL_C_n8(mdim) ""
    #define KERNEL_TAIL_C_n12(mdim) ""
    #define KERNEL_TAIL_C_n16(mdim) ""
    #define KERNEL_TAIL_C_n20(mdim) ""
    #define KERNEL_TAIL_C_n24(mdim) ""
    #define KERNEL_TAIL_R_n4(mdim) ""
    #define KERNEL_TAIL_R_n8(mdim) ""
    #define KERNEL_TAIL_R_n12(mdim) ""
  #else
    #define KERNEL_HEAD_C_n8(mdim) ""
    #define KERNEL_HEAD_C_n12(mdim) ""
    #define KERNEL_HEAD_C_n16(mdim) ""
    #define KERNEL_HEAD_C_n20(mdim) ""
    #define KERNEL_HEAD_C_n24(mdim) ""
    #define KERNEL_HEAD_R_n4(mdim) ""
    #define KERNEL_HEAD_R_n8(mdim) ""
    #define KERNEL_HEAD_R_n12(mdim) ""
    #define end_kernel_k4_ncx1(k_0,k_1,k_2,k_3,n1,mdim) \
      end_load_a_k1m##mdim(k_0) end_acc_nc##n1##_k1m##mdim(k_0)\
      end_load_a_k1m##mdim(k_1) end_acc_nc##n1##_k1m##mdim(k_1)\
      end_load_a_k1m##mdim(k_2) end_acc_nc##n1##_k1m##mdim(k_2)\
      end_load_a_k1m##mdim(k_3) end_acc_nc##n1##_k1m##mdim(k_3)
    #define end_kernel_k4_ncx2(k_0,k_1,k_2,k_3,n1,n2,mdim) \
      end_load_a_k1m##mdim(k_0) end_acc_nc##n1##_k1m##mdim(k_0) end_acc_nc##n2##_k1m##mdim(k_0)\
      end_load_a_k1m##mdim(k_1) end_acc_nc##n1##_k1m##mdim(k_1) end_acc_nc##n2##_k1m##mdim(k_1)\
      end_load_a_k1m##mdim(k_2) end_acc_nc##n1##_k1m##mdim(k_2) end_acc_nc##n2##_k1m##mdim(k_2)\
      end_load_a_k1m##mdim(k_3) end_acc_nc##n1##_k1m##mdim(k_3) end_acc_nc##n2##_k1m##mdim(k_3)
    #define end_kernel_k4_ncx3(k_0,k_1,k_2,k_3,n1,n2,n3,mdim) \
      end_load_a_k1m##mdim(k_0) end_acc_nc##n1##_k1m##mdim(k_0) end_acc_nc##n2##_k1m##mdim(k_0) end_acc_nc##n3##_k1m##mdim(k_0)\
      end_load_a_k1m##mdim(k_1) end_acc_nc##n1##_k1m##mdim(k_1) end_acc_nc##n2##_k1m##mdim(k_1) end_acc_nc##n3##_k1m##mdim(k_1)\
      end_load_a_k1m##mdim(k_2) end_acc_nc##n1##_k1m##mdim(k_2) end_acc_nc##n2##_k1m##mdim(k_2) end_acc_nc##n3##_k1m##mdim(k_2)\
      end_load_a_k1m##mdim(k_3) end_acc_nc##n1##_k1m##mdim(k_3) end_acc_nc##n2##_k1m##mdim(k_3) end_acc_nc##n3##_k1m##mdim(k_3)
    #define end_kernel_k4_ncx4(k_0,k_1,k_2,k_3,n1,n2,n3,n4,mdim) \
      end_load_a_k1m##mdim(k_0) end_acc_nc##n1##_k1m##mdim(k_0) end_acc_nc##n2##_k1m##mdim(k_0) end_acc_nc##n3##_k1m##mdim(k_0) end_acc_nc##n4##_k1m##mdim(k_0)\
      end_load_a_k1m##mdim(k_1) end_acc_nc##n1##_k1m##mdim(k_1) end_acc_nc##n2##_k1m##mdim(k_1) end_acc_nc##n3##_k1m##mdim(k_1) end_acc_nc##n4##_k1m##mdim(k_1)\
      end_load_a_k1m##mdim(k_2) end_acc_nc##n1##_k1m##mdim(k_2) end_acc_nc##n2##_k1m##mdim(k_2) end_acc_nc##n3##_k1m##mdim(k_2) end_acc_nc##n4##_k1m##mdim(k_2)\
      end_load_a_k1m##mdim(k_3) end_acc_nc##n1##_k1m##mdim(k_3) end_acc_nc##n2##_k1m##mdim(k_3) end_acc_nc##n3##_k1m##mdim(k_3) end_acc_nc##n4##_k1m##mdim(k_3)
    #define end_kernel_k4_ncx5(k_0,k_1,k_2,k_3,n1,n2,n3,n4,n5,mdim) \
      end_load_a_k1m##mdim(k_0)     end_acc_nc##n1##_k1m##mdim(k_0) end_acc_nc##n2##_k1m##mdim(k_0)\
      end_acc_nc##n3##_k1m##mdim(k_0) end_acc_nc##n4##_k1m##mdim(k_0) end_acc_nc##n5##_k1m##mdim(k_0)\
      end_load_a_k1m##mdim(k_1)     end_acc_nc##n1##_k1m##mdim(k_1) end_acc_nc##n2##_k1m##mdim(k_1)\
      end_acc_nc##n3##_k1m##mdim(k_1) end_acc_nc##n4##_k1m##mdim(k_1) end_acc_nc##n5##_k1m##mdim(k_1)\
      end_load_a_k1m##mdim(k_2)     end_acc_nc##n1##_k1m##mdim(k_2) end_acc_nc##n2##_k1m##mdim(k_2)\
      end_acc_nc##n3##_k1m##mdim(k_2) end_acc_nc##n4##_k1m##mdim(k_2) end_acc_nc##n5##_k1m##mdim(k_2)\
      end_load_a_k1m##mdim(k_3)     end_acc_nc##n1##_k1m##mdim(k_3) end_acc_nc##n2##_k1m##mdim(k_3)\
      end_acc_nc##n3##_k1m##mdim(k_3) end_acc_nc##n4##_k1m##mdim(k_3) end_acc_nc##n5##_k1m##mdim(k_3)
    #define KERNEL_TAIL_C_n8(mdim) end_kernel_k4_ncx1(0,1,2,3,2,mdim)
    #define KERNEL_TAIL_C_n12(mdim) \
      end_kernel_k4_ncx2(0,1,2,3,2,3,mdim) end_kernel_k4_ncx1(4,5,6,7,3,mdim)
    #define KERNEL_TAIL_C_n16(mdim) \
      end_kernel_k4_ncx3(0,1,2,3,2,3,4,mdim) end_kernel_k4_ncx2(4,5,6,7,3,4,mdim) end_kernel_k4_ncx1(8,9,10,11,4,mdim)
    #define KERNEL_TAIL_C_n20(mdim) \
      end_kernel_k4_ncx4(0,1,2,3,2,3,4,5,mdim) end_kernel_k4_ncx3(4,5,6,7,3,4,5,mdim)\
      end_kernel_k4_ncx2(8,9,10,11,4,5,mdim) end_kernel_k4_ncx1(12,13,14,15,5,mdim)
    #define KERNEL_TAIL_C_n24(mdim) \
      end_kernel_k4_ncx5(0,1,2,3,2,3,4,5,6,mdim) end_kernel_k4_ncx4(4,5,6,7,3,4,5,6,mdim) end_kernel_k4_ncx3(8,9,10,11,4,5,6,mdim)\
      end_kernel_k4_ncx2(12,13,14,15,5,6,mdim) end_kernel_k4_ncx1(16,17,18,19,6,mdim)
    #define KERNEL_TAIL_R_n4(mdim) \
      end_kernel_k4_ncx1(0,1,2,3,4,mdim) end_kernel_k4_ncx1(4,5,6,7,4,mdim) end_kernel_k4_ncx1(8,9,10,11,4,mdim)
    #define KERNEL_TAIL_R_n8(mdim) \
      end_kernel_k4_ncx2(0,1,2,3,4,5,mdim) end_kernel_k4_ncx2(4,5,6,7,4,5,mdim) end_kernel_k4_ncx2(8,9,10,11,4,5,mdim) end_kernel_k4_ncx1(12,13,14,15,5,mdim)
    #define KERNEL_TAIL_R_n12(mdim) \
      end_kernel_k4_ncx3(0,1,2,3,4,5,6,mdim) end_kernel_k4_ncx3(4,5,6,7,4,5,6,mdim) end_kernel_k4_ncx3(8,9,10,11,4,5,6,mdim)\
      end_kernel_k4_ncx2(12,13,14,15,5,6,mdim) end_kernel_k4_ncx1(16,17,18,19,6,mdim)
  #endif
#else
  #define KERNEL_HEAD_C_n8(mdim) ""
  #define KERNEL_HEAD_C_n12(mdim) ""
  #define KERNEL_HEAD_C_n16(mdim) ""
  #define KERNEL_HEAD_C_n20(mdim) ""
  #define KERNEL_HEAD_C_n24(mdim) ""
  #define KERNEL_HEAD_R_n4(mdim) ""
  #define KERNEL_HEAD_R_n8(mdim) ""
  #define KERNEL_HEAD_R_n12(mdim) ""
  #define KERNEL_TAIL_C_n8(mdim) ""
  #define KERNEL_TAIL_C_n12(mdim) ""
  #define KERNEL_TAIL_C_n16(mdim) ""
  #define KERNEL_TAIL_C_n20(mdim) ""
  #define KERNEL_TAIL_C_n24(mdim) ""
  #define KERNEL_TAIL_R_n4(mdim) ""
  #define KERNEL_TAIL_R_n8(mdim) ""
  #define KERNEL_TAIL_R_n12(mdim) ""
#endif
#define KERNEL_HEAD_C_n1(mdim) ""
#define KERNEL_HEAD_C_n2(mdim) ""
#define KERNEL_HEAD_C_n4(mdim) ""
#define KERNEL_TAIL_C_n1(mdim) ""
#define KERNEL_TAIL_C_n2(mdim) ""
#define KERNEL_TAIL_C_n4(mdim) ""

/* m = 16 */ /* zmm8-zmm31 for accumulators, zmm1-zmm7 for temporary use, zmm0 for alpha */
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
#define unit_gen_kernel_k1m16n4(c1,c2,c3,c4,k_no,...) \
    "vbroadcastsd "#k_no"*16  ("#__VA_ARGS__"),%%zmm6; vfmadd231ps %%zmm4,%%zmm6,"#c1"; vfmadd231ps %%zmm5,%%zmm6,"#c2";"\
    "vbroadcastsd "#k_no"*16+8("#__VA_ARGS__"),%%zmm7; vfmadd231ps %%zmm4,%%zmm7,"#c3"; vfmadd231ps %%zmm5,%%zmm7,"#c4";"
#define unit_kernel_k1m16n4(c1,c2,c3,c4, ...) unit_gen_kernel_k1m16n4(c1,c2,c3,c4,0,__VA_ARGS__)
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
#define end_load_a_k1m16(k_no) "vmovsldup "#k_no"*64(%0),%%zmm4; vmovshdup "#k_no"*64(%0),%%zmm5;"
#define end_acc_nc2_k1m16(k_no) unit_gen_kernel_k1m16n4(%%zmm12,%%zmm13,%%zmm14,%%zmm15,k_no,%1,%%r12,1)
#define end_acc_nc3_k1m16(k_no) unit_gen_kernel_k1m16n4(%%zmm16,%%zmm17,%%zmm18,%%zmm19,k_no,%1,%%r12,2)
#define end_acc_nc4_k1m16(k_no) unit_gen_kernel_k1m16n4(%%zmm20,%%zmm21,%%zmm22,%%zmm23,k_no,%%r15)
#define end_acc_nc5_k1m16(k_no) unit_gen_kernel_k1m16n4(%%zmm24,%%zmm25,%%zmm26,%%zmm27,k_no,%%r15,%%r12,1)
#define end_acc_nc6_k1m16(k_no) unit_gen_kernel_k1m16n4(%%zmm28,%%zmm29,%%zmm30,%%zmm31,k_no,%%r15,%%r12,2)
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
#ifdef TRMMKERNEL
  #define SAVE_h_m16n1 "vmulps %%zmm8,%%zmm0,%%zmm8; vmovups %%zmm8,(%2);"
  #define unit_save_m16n2(c1,c2) \
    "vunpcklps "#c2","#c1",%%zmm6; vunpckhps "#c2","#c1",%%zmm7; vunpcklpd %%zmm7,%%zmm6,%%zmm4; vunpckhpd %%zmm7,%%zmm6,%%zmm5;"\
    "vmulps %%zmm4,%%zmm0,%%zmm4; vmulps %%zmm5,%%zmm0,%%zmm5;"\
    "vmovups %%zmm4,(%5); vmovups %%zmm5,(%5,%3,1); leaq (%5,%3,2),%5;"
#else
  #define SAVE_h_m16n1 "vfmadd213ps (%2),%%zmm0,%%zmm8; vmovups %%zmm8,(%2);"
  #define unit_save_m16n2(c1,c2) \
    "vunpcklps "#c2","#c1",%%zmm6; vunpckhps "#c2","#c1",%%zmm7; vunpcklpd %%zmm7,%%zmm6,%%zmm4; vunpckhpd %%zmm7,%%zmm6,%%zmm5;"\
    "vfmadd213ps (%5),%%zmm0,%%zmm4; vfmadd213ps (%5,%3,1),%%zmm0,%%zmm5;"\
    "vmovups %%zmm4,(%5); vmovups %%zmm5,(%5,%3,1); leaq (%5,%3,2),%5;"
#endif
#define SAVE_h_m16n2 "movq %2,%5;" unit_save_m16n2(%%zmm8,%%zmm9)
#define SAVE_h_m16n4  SAVE_h_m16n2  unit_save_m16n2(%%zmm10,%%zmm11)
#define SAVE_h_m16n8  SAVE_h_m16n4  unit_save_m16n2(%%zmm12,%%zmm13) unit_save_m16n2(%%zmm14,%%zmm15)
#define SAVE_h_m16n12 SAVE_h_m16n8  unit_save_m16n2(%%zmm16,%%zmm17) unit_save_m16n2(%%zmm18,%%zmm19)
#define SAVE_h_m16n16 SAVE_h_m16n12 unit_save_m16n2(%%zmm20,%%zmm21) unit_save_m16n2(%%zmm22,%%zmm23)
#define SAVE_h_m16n20 SAVE_h_m16n16 unit_save_m16n2(%%zmm24,%%zmm25) unit_save_m16n2(%%zmm26,%%zmm27)
#define SAVE_h_m16n24 SAVE_h_m16n20 unit_save_m16n2(%%zmm28,%%zmm29) unit_save_m16n2(%%zmm30,%%zmm31)
#define SAVE_m16(ndim) SAVE_h_m16n##ndim "addq $64,%2;"
#define COMPUTE_m16(ndim) \
    INIT_m16n##ndim START_SET_PAPB(16,ndim)\
    "movq %%r13,%4; movq %2,%5; xorq %%r10,%%r10;"\
    KERNEL_HEAD_C_n##ndim(16)\
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
    KERNEL_TAIL_C_n##ndim(16)\
    "prefetcht0 (%%r14); prefetcht0 64(%%r14);"\
    SAVE_m16(ndim) END_SET_PA(16)

/* m = 8 *//* ymm0 for alpha, ymm1-ymm3 for temporary use, ymm4-ymm15 for accumulators */
#define kernel_k1m8n1(b_addr) \
    "vmovups (%0),%%ymm1; addq $32,%0;"\
    "vbroadcastss ("#b_addr"),%%ymm2; vfmadd231ps %%ymm1,%%ymm2,%%ymm4;"\
    "addq $4,"#b_addr";"
#define kernel_h_k1m8n2(b_addr) \
    "vmovsldup (%0),%%ymm1; vmovshdup (%0),%%ymm2; addq $32,%0;"\
    "vbroadcastsd ("#b_addr"),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm4; vfmadd231ps %%ymm2,%%ymm3,%%ymm5;"
#define kernel_k1m8n2(b_addr) kernel_h_k1m8n2(b_addr) "addq $8,"#b_addr";"
#define kernel_h_k1m8n4(b_addr) \
    kernel_h_k1m8n2(b_addr) "vbroadcastsd 8("#b_addr"),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,%%ymm6; vfmadd231ps %%ymm2,%%ymm3,%%ymm7;"
#define kernel_k1m8n4(b_addr) kernel_h_k1m8n4(b_addr) "addq $16,"#b_addr";"
#define unit_gen_kernel_k1m8n4(c1,c2,c3,c4,k_no,...) \
    "vbroadcastsd "#k_no"*16  ("#__VA_ARGS__"),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,"#c1"; vfmadd231ps %%ymm2,%%ymm3,"#c2";"\
    "vbroadcastsd "#k_no"*16+8("#__VA_ARGS__"),%%ymm3; vfmadd231ps %%ymm1,%%ymm3,"#c3"; vfmadd231ps %%ymm2,%%ymm3,"#c4";"
#define unit_kernel_k1m8n4(c1,c2,c3,c4,...) unit_gen_kernel_k1m8n4(c1,c2,c3,c4,0,__VA_ARGS__)
#define kernel_h_k1m8n8(b_addr) kernel_h_k1m8n4(b_addr) unit_kernel_k1m8n4(%%ymm8,%%ymm9,%%ymm10,%%ymm11,b_addr,%%r12,1)
#define kernel_k1m8n8(b_addr) kernel_h_k1m8n8(b_addr) "addq $16,"#b_addr";"
#define kernel_h_k1m8n12(b_addr) kernel_h_k1m8n8(b_addr) unit_kernel_k1m8n4(%%ymm12,%%ymm13,%%ymm14,%%ymm15,b_addr,%%r12,2)
#define kernel_k1m8n12(b_addr) kernel_h_k1m8n12(b_addr) "addq $16,"#b_addr";"
#define KERNEL_k1m8n1 kernel_k1m8n1(%1)
#define KERNEL_k1m8n2 kernel_k1m8n2(%1)
#define KERNEL_k1m8n4 kernel_k1m8n4(%1)
#define KERNEL_k1m8n8 kernel_k1m8n8(%1)
#define KERNEL_k1m8n12 kernel_k1m8n12(%1)
#define end_load_a_k1m8(k_no) "vmovsldup "#k_no"*32(%0),%%ymm1; vmovshdup "#k_no"*32(%0),%%ymm2;"
#define end_acc_nc2_k1m8(k_no) unit_gen_kernel_k1m8n4(%%ymm8,%%ymm9,%%ymm10,%%ymm11,k_no,%1,%%r12,1)
#define end_acc_nc3_k1m8(k_no) unit_gen_kernel_k1m8n4(%%ymm12,%%ymm13,%%ymm14,%%ymm15,k_no,%1,%%r12,2)
#define end_acc_nc4_k1m8(k_no) unit_gen_kernel_k1m8n4(%%ymm4,%%ymm5,%%ymm6,%%ymm7,k_no,%%r15)
#define end_acc_nc5_k1m8(k_no) unit_gen_kernel_k1m8n4(%%ymm8,%%ymm9,%%ymm10,%%ymm11,k_no,%%r15,%%r12,1)
#define end_acc_nc6_k1m8(k_no) unit_gen_kernel_k1m8n4(%%ymm12,%%ymm13,%%ymm14,%%ymm15,k_no,%%r15,%%r12,2)
#define INIT_m8n1 "vpxor %%ymm4,%%ymm4,%%ymm4;"
#define INIT_m8n2 INIT_m8n1 "vpxor %%ymm5,%%ymm5,%%ymm5;"
#define INIT_m8n4 INIT_m8n2 "vpxor %%ymm6,%%ymm6,%%ymm6;vpxor %%ymm7,%%ymm7,%%ymm7;"
#define unit_init_m8n4(c1,c2,c3,c4) \
    "vpxor "#c1","#c1","#c1";vpxor "#c2","#c2","#c2";vpxor "#c3","#c3","#c3";vpxor "#c4","#c4","#c4";"
#define INIT_m8n8  INIT_m8n4 unit_init_m8n4(%%ymm8,%%ymm9,%%ymm10,%%ymm11)
#define INIT_m8n12 INIT_m8n8 unit_init_m8n4(%%ymm12,%%ymm13,%%ymm14,%%ymm15)
#ifdef TRMMKERNEL
  #define SAVE_L_m8n1 "vmulps %%ymm4,%%ymm0,%%ymm4; vmovups %%ymm4,(%2);"
  #define unit_save_m8n2(c1,c2) \
    "vunpcklps "#c2","#c1",%%ymm2; vunpckhps "#c2","#c1",%%ymm3;"\
    "vunpcklpd %%ymm3,%%ymm2,%%ymm1; vmulps %%ymm1,%%ymm0,%%ymm1; vmovups %%ymm1,(%5);"\
    "vunpckhpd %%ymm3,%%ymm2,%%ymm1; vmulps %%ymm1,%%ymm0,%%ymm1; vmovups %%ymm1,(%5,%3,1);"\
    "leaq (%5,%3,2),%5;"
#else
  #define SAVE_L_m8n1 "vfmadd213ps (%2),%%ymm0,%%ymm4; vmovups %%ymm4,(%2);"
  #define unit_save_m8n2(c1,c2) \
    "vunpcklps "#c2","#c1",%%ymm2; vunpckhps "#c2","#c1",%%ymm3;"\
    "vunpcklpd %%ymm3,%%ymm2,%%ymm1; vfmadd213ps (%5),     %%ymm0,%%ymm1; vmovups %%ymm1,(%5);"\
    "vunpckhpd %%ymm3,%%ymm2,%%ymm1; vfmadd213ps (%5,%3,1),%%ymm0,%%ymm1; vmovups %%ymm1,(%5,%3,1);"\
    "leaq (%5,%3,2),%5;"
#endif
#define SAVE_L_m8n2 "movq %2,%5;" unit_save_m8n2(%%ymm4,%%ymm5)
#define SAVE_L_m8n4  SAVE_L_m8n2  unit_save_m8n2(%%ymm6,%%ymm7)
#define SAVE_L_m8n8  SAVE_L_m8n4  unit_save_m8n2(%%ymm8,%%ymm9)   unit_save_m8n2(%%ymm10,%%ymm11)
#define SAVE_L_m8n12 SAVE_L_m8n8  unit_save_m8n2(%%ymm12,%%ymm13) unit_save_m8n2(%%ymm14,%%ymm15)
#define SAVE_R_m8n4               unit_save_m8n2(%%ymm4,%%ymm5)   unit_save_m8n2(%%ymm6,%%ymm7)
#define SAVE_R_m8n8  SAVE_R_m8n4  unit_save_m8n2(%%ymm8,%%ymm9)   unit_save_m8n2(%%ymm10,%%ymm11)
#define SAVE_R_m8n12 SAVE_R_m8n8  unit_save_m8n2(%%ymm12,%%ymm13) unit_save_m8n2(%%ymm14,%%ymm15)
#define COMPUTE_L_m8(ndim,sim) \
    INIT_m8n##ndim START_SET_PAPB(8,ndim)\
    "movq %%r13,%4;"\
    KERNEL_HEAD_C_n##ndim(8)\
    "testq %4,%4; jz "#ndim""#sim"883f;"\
    #ndim""#sim"882:\n\t"\
    kernel_k1m8n##ndim(%1)\
    "decq %4; jnz "#ndim""#sim"882b;"\
    #ndim""#sim"883:\n\t"\
    KERNEL_TAIL_C_n##ndim(8)\
    SAVE_L_m8n##ndim "addq $32,%2;"
#define COMPUTE_R_m8(ndim,sim) \
    INIT_m8n##ndim RECOVER_PA(8)\
    "movq %%r13,%4;"\
    KERNEL_HEAD_R_n##ndim(8)\
    "testq %4,%4; jz "#ndim""#sim"883f;"\
    #ndim""#sim"882:\n\t"\
    kernel_k1m8n##ndim(%%r15)\
    "decq %4; jnz "#ndim""#sim"882b;"\
    #ndim""#sim"883:\n\t"\
    KERNEL_TAIL_R_n##ndim(8)\
    SAVE_R_m8n##ndim END_SET_PA(8)
#define COMPUTE_m8_n1  COMPUTE_L_m8(1,33833) END_SET_PA(8)
#define COMPUTE_m8_n2  COMPUTE_L_m8(2,33833) END_SET_PA(8)
#define COMPUTE_m8_n4  COMPUTE_L_m8(4,33833) END_SET_PA(8)
#define COMPUTE_m8_n8  COMPUTE_L_m8(8,33833) END_SET_PA(8)
#define COMPUTE_m8_n12 COMPUTE_L_m8(12,33833) END_SET_PA(8)
#define COMPUTE_m8_n16 COMPUTE_L_m8(12,33733) COMPUTE_R_m8(4,33933)
#define COMPUTE_m8_n20 COMPUTE_L_m8(12,33633) COMPUTE_R_m8(8,33933)
#define COMPUTE_m8_n24 COMPUTE_L_m8(12,33533) COMPUTE_R_m8(12,33933)
#define COMPUTE_m8(ndim) COMPUTE_m8_n##ndim

/* m = 4 *//* xmm0 for alpha, xmm1-xmm3 for temporary use, xmm4-xmm15 for accumulators */
#define kernel_k1m4n1(b_addr) \
    "vmovups (%0),%%xmm1; addq $16,%0;"\
    "vbroadcastss ("#b_addr"),%%xmm2; vfmadd231ps %%xmm1,%%xmm2,%%xmm4;"\
    "addq $4,"#b_addr";"
#define kernel_h_k1m4n2(b_addr) \
    "vmovsldup (%0),%%xmm1; vmovshdup (%0),%%xmm2; addq $16,%0;"\
    "vmovddup ("#b_addr"),%%xmm3; vfmadd231ps %%xmm1,%%xmm3,%%xmm4; vfmadd231ps %%xmm2,%%xmm3,%%xmm5;"
#define kernel_k1m4n2(b_addr) kernel_h_k1m4n2(b_addr) "addq $8,"#b_addr";"
#define kernel_h_k1m4n4(b_addr) \
    kernel_h_k1m4n2(b_addr) "vmovddup 8("#b_addr"),%%xmm3; vfmadd231ps %%xmm1,%%xmm3,%%xmm6; vfmadd231ps %%xmm2,%%xmm3,%%xmm7;"
#define kernel_k1m4n4(b_addr) kernel_h_k1m4n4(b_addr) "addq $16,"#b_addr";"
#define unit_gen_kernel_k1m4n4(c1,c2,c3,c4,k_no,...) \
    "vmovddup "#k_no"*16  ("#__VA_ARGS__"),%%xmm3; vfmadd231ps %%xmm1,%%xmm3,"#c1"; vfmadd231ps %%xmm2,%%xmm3,"#c2";"\
    "vmovddup "#k_no"*16+8("#__VA_ARGS__"),%%xmm3; vfmadd231ps %%xmm1,%%xmm3,"#c3"; vfmadd231ps %%xmm2,%%xmm3,"#c4";"
#define unit_kernel_k1m4n4(c1,c2,c3,c4,...) unit_gen_kernel_k1m4n4(c1,c2,c3,c4,0,__VA_ARGS__)
#define kernel_h_k1m4n8(b_addr) kernel_h_k1m4n4(b_addr) unit_kernel_k1m4n4(%%xmm8,%%xmm9,%%xmm10,%%xmm11,b_addr,%%r12,1)
#define kernel_k1m4n8(b_addr) kernel_h_k1m4n8(b_addr) "addq $16,"#b_addr";"
#define kernel_h_k1m4n12(b_addr) kernel_h_k1m4n8(b_addr) unit_kernel_k1m4n4(%%xmm12,%%xmm13,%%xmm14,%%xmm15,b_addr,%%r12,2)
#define kernel_k1m4n12(b_addr) kernel_h_k1m4n12(b_addr) "addq $16,"#b_addr";"
#define KERNEL_k1m4n1 kernel_k1m4n1(%1)
#define KERNEL_k1m4n2 kernel_k1m4n2(%1)
#define KERNEL_k1m4n4 kernel_k1m4n4(%1)
#define KERNEL_k1m4n8 kernel_k1m4n8(%1)
#define KERNEL_k1m4n12 kernel_k1m4n12(%1)
#define end_load_a_k1m4(k_no) "vmovsldup "#k_no"*16(%0),%%xmm1; vmovshdup "#k_no"*16(%0),%%xmm2;"
#define end_acc_nc2_k1m4(k_no) unit_gen_kernel_k1m4n4(%%xmm8,%%xmm9,%%xmm10,%%xmm11,k_no,%1,%%r12,1)
#define end_acc_nc3_k1m4(k_no) unit_gen_kernel_k1m4n4(%%xmm12,%%xmm13,%%xmm14,%%xmm15,k_no,%1,%%r12,2)
#define end_acc_nc4_k1m4(k_no) unit_gen_kernel_k1m4n4(%%xmm4,%%xmm5,%%xmm6,%%xmm7,k_no,%%r15)
#define end_acc_nc5_k1m4(k_no) unit_gen_kernel_k1m4n4(%%xmm8,%%xmm9,%%xmm10,%%xmm11,k_no,%%r15,%%r12,1)
#define end_acc_nc6_k1m4(k_no) unit_gen_kernel_k1m4n4(%%xmm12,%%xmm13,%%xmm14,%%xmm15,k_no,%%r15,%%r12,2)
#define INIT_m4n1 "vpxor %%xmm4,%%xmm4,%%xmm4;"
#define INIT_m4n2 INIT_m4n1 "vpxor %%xmm5,%%xmm5,%%xmm5;"
#define INIT_m4n4 INIT_m4n2 "vpxor %%xmm6,%%xmm6,%%xmm6;vpxor %%xmm7,%%xmm7,%%xmm7;"
#define unit_init_m4n4(c1,c2,c3,c4) \
    "vpxor "#c1","#c1","#c1";vpxor "#c2","#c2","#c2";vpxor "#c3","#c3","#c3";vpxor "#c4","#c4","#c4";"
#define INIT_m4n8  INIT_m4n4 unit_init_m4n4(%%xmm8,%%xmm9,%%xmm10,%%xmm11)
#define INIT_m4n12 INIT_m4n8 unit_init_m4n4(%%xmm12,%%xmm13,%%xmm14,%%xmm15)
#ifdef TRMMKERNEL
  #define SAVE_L_m4n1 "vmulps %%xmm4,%%xmm0,%%xmm4; vmovups %%xmm4,(%2);"
  #define unit_save_m4n2(c1,c2) \
    "vunpcklps "#c2","#c1",%%xmm2; vunpckhps "#c2","#c1",%%xmm3;"\
    "vunpcklpd %%xmm3,%%xmm2,%%xmm1;vmulps %%xmm1,%%xmm0,%%xmm1;vmovups %%xmm1,(%5);"\
    "vunpckhpd %%xmm3,%%xmm2,%%xmm1;vmulps %%xmm1,%%xmm0,%%xmm1;vmovups %%xmm1,(%5,%3,1);"\
    "leaq (%5,%3,2),%5;"
#else
  #define SAVE_L_m4n1 "vfmadd213ps (%2),%%xmm0,%%xmm4; vmovups %%xmm4,(%2);"
  #define unit_save_m4n2(c1,c2) \
    "vunpcklps "#c2","#c1",%%xmm2; vunpckhps "#c2","#c1",%%xmm3;"\
    "vunpcklpd %%xmm3,%%xmm2,%%xmm1;vfmadd213ps (%5),     %%xmm0,%%xmm1;vmovups %%xmm1,(%5);"\
    "vunpckhpd %%xmm3,%%xmm2,%%xmm1;vfmadd213ps (%5,%3,1),%%xmm0,%%xmm1;vmovups %%xmm1,(%5,%3,1);"\
    "leaq (%5,%3,2),%5;"
#endif
#define SAVE_L_m4n2 "movq %2,%5;" unit_save_m4n2(%%xmm4,%%xmm5)
#define SAVE_L_m4n4  SAVE_L_m4n2  unit_save_m4n2(%%xmm6,%%xmm7)
#define SAVE_L_m4n8  SAVE_L_m4n4  unit_save_m4n2(%%xmm8,%%xmm9)   unit_save_m4n2(%%xmm10,%%xmm11)
#define SAVE_L_m4n12 SAVE_L_m4n8  unit_save_m4n2(%%xmm12,%%xmm13) unit_save_m4n2(%%xmm14,%%xmm15)
#define SAVE_R_m4n4               unit_save_m4n2(%%xmm4,%%xmm5)   unit_save_m4n2(%%xmm6,%%xmm7)
#define SAVE_R_m4n8  SAVE_R_m4n4  unit_save_m4n2(%%xmm8,%%xmm9)   unit_save_m4n2(%%xmm10,%%xmm11)
#define SAVE_R_m4n12 SAVE_R_m4n8  unit_save_m4n2(%%xmm12,%%xmm13) unit_save_m4n2(%%xmm14,%%xmm15)
#define COMPUTE_L_m4(ndim,sim) \
    INIT_m4n##ndim START_SET_PAPB(4,ndim)\
    "movq %%r13,%4;"\
    KERNEL_HEAD_C_n##ndim(4)\
    "testq %4,%4; jz "#ndim""#sim"443f;"\
    #ndim""#sim"442:\n\t"\
    kernel_k1m4n##ndim(%1)\
    "decq %4; jnz "#ndim""#sim"442b;"\
    #ndim""#sim"443:\n\t"\
    KERNEL_TAIL_C_n##ndim(4)\
    SAVE_L_m4n##ndim "addq $16,%2;"
#define COMPUTE_R_m4(ndim,sim) \
    INIT_m4n##ndim RECOVER_PA(4)\
    "movq %%r13,%4;"\
    KERNEL_HEAD_R_n##ndim(4)\
    "testq %4,%4; jz "#ndim""#sim"443f;"\
    #ndim""#sim"442:\n\t"\
    kernel_k1m4n##ndim(%%r15)\
    "decq %4; jnz "#ndim""#sim"442b;"\
    #ndim""#sim"443:\n\t"\
    KERNEL_TAIL_R_n##ndim(4)\
    SAVE_R_m4n##ndim END_SET_PA(4)
#define COMPUTE_m4_n1  COMPUTE_L_m4(1,55855) END_SET_PA(4)
#define COMPUTE_m4_n2  COMPUTE_L_m4(2,55855) END_SET_PA(4)
#define COMPUTE_m4_n4  COMPUTE_L_m4(4,55855) END_SET_PA(4)
#define COMPUTE_m4_n8  COMPUTE_L_m4(8,55855) END_SET_PA(4)
#define COMPUTE_m4_n12 COMPUTE_L_m4(12,55855) END_SET_PA(4)
#define COMPUTE_m4_n16 COMPUTE_L_m4(12,55755) COMPUTE_R_m4(4,55955)
#define COMPUTE_m4_n20 COMPUTE_L_m4(12,55655) COMPUTE_R_m4(8,55955)
#define COMPUTE_m4_n24 COMPUTE_L_m4(12,55555) COMPUTE_R_m4(12,55955)
#define COMPUTE_m4(ndim) COMPUTE_m4_n##ndim

/* m = 2 *//* xmm0 for alpha, xmm1-xmm3 for temporary use, xmm4-xmm15 for accumulators */
#define INIT_m2n1 "vpxor %%xmm4,%%xmm4,%%xmm4;"
#define KERNEL_k1m2n1 \
    "vmovsd (%0),%%xmm1; addq $8,%0;"\
    "vbroadcastss (%1),%%xmm2; vfmadd231ps %%xmm1,%%xmm2,%%xmm4;"\
    "addq $4,%1;"
#define INIT_m2n2 INIT_m2n1 "vpxor %%xmm5,%%xmm5,%%xmm5;"
#define KERNEL_k1m2n2 \
    "vmovsd (%0),%%xmm1; addq $8,%0;"\
    "vbroadcastss  (%1),%%xmm2; vfmadd231ps %%xmm1,%%xmm2,%%xmm4;"\
    "vbroadcastss 4(%1),%%xmm3; vfmadd231ps %%xmm1,%%xmm3,%%xmm5;"\
    "addq $8,%1;"
#ifdef TRMMKERNEL
  #define SAVE_h_m2n1 "vmulps %%xmm4,%%xmm0,%%xmm4; vmovsd %%xmm4,(%2);"
  #define SAVE_h_m2n2 SAVE_h_m2n1 "vmulps %%xmm5,%%xmm0,%%xmm5; vmovsd %%xmm5,(%2,%3,1);"
#else
  #define SAVE_h_m2n1 "vmovsd (%2),%%xmm1; vfmadd213ps %%xmm1,%%xmm0,%%xmm4; vmovsd %%xmm4,(%2);"
  #define SAVE_h_m2n2 SAVE_h_m2n1 "vmovsd (%2,%3,1),%%xmm1; vfmadd213ps %%xmm1,%%xmm0,%%xmm5; vmovsd %%xmm5,(%2,%3,1);"
#endif
#define INIT_m2n4  INIT_m2n2
#define INIT_m2n8  INIT_m2n4 "vpxor %%xmm6,%%xmm6,%%xmm6; vpxor %%xmm7,%%xmm7,%%xmm7;"
#define INIT_m2n12 INIT_m2n8 "vpxor %%xmm8,%%xmm8,%%xmm8; vpxor %%xmm9,%%xmm9,%%xmm9;"
#define INIT_m2n16 INIT_m2n12 "vpxor %%xmm10,%%xmm10,%%xmm10; vpxor %%xmm11,%%xmm11,%%xmm11;"
#define INIT_m2n20 INIT_m2n16 "vpxor %%xmm12,%%xmm12,%%xmm12; vpxor %%xmm13,%%xmm13,%%xmm13;"
#define INIT_m2n24 INIT_m2n20 "vpxor %%xmm14,%%xmm14,%%xmm14; vpxor %%xmm15,%%xmm15,%%xmm15;"
#define unit_gen_kernel_k1m2n4(c1,c2,k_no,...) \
    "vmovups "#k_no"*16("#__VA_ARGS__"),%%xmm3; vfmadd231ps %%xmm1,%%xmm3,"#c1"; vfmadd231ps %%xmm2,%%xmm3,"#c2";"
#define KERNEL_h_k1m2n4 \
    "vbroadcastss (%0),%%xmm1; vbroadcastss 4(%0),%%xmm2; addq $8,%0;" unit_gen_kernel_k1m2n4(%%xmm4,%%xmm5,0,%1)
#define KERNEL_k1m2n4 KERNEL_h_k1m2n4 "addq $16,%1;"
#define KERNEL_h_k1m2n8 KERNEL_h_k1m2n4 unit_gen_kernel_k1m2n4(%%xmm6,%%xmm7,0,%1,%%r12,1)
#define KERNEL_k1m2n8 KERNEL_h_k1m2n8 "addq $16,%1;"
#define KERNEL_k1m2n12 KERNEL_h_k1m2n8 unit_gen_kernel_k1m2n4(%%xmm8,%%xmm9,0,%1,%%r12,2) "addq $16,%1;"
#define KERNEL_h_k1m2n16 KERNEL_k1m2n12 unit_gen_kernel_k1m2n4(%%xmm10,%%xmm11,0,%%r15)
#define KERNEL_k1m2n16 KERNEL_h_k1m2n16 "addq $16,%%r15;"
#define KERNEL_h_k1m2n20 KERNEL_h_k1m2n16 unit_gen_kernel_k1m2n4(%%xmm12,%%xmm13,0,%%r15,%%r12,1)
#define KERNEL_k1m2n20 KERNEL_h_k1m2n20 "addq $16,%%r15;"
#define KERNEL_h_k1m2n24 KERNEL_h_k1m2n20 unit_gen_kernel_k1m2n4(%%xmm14,%%xmm15,0,%%r15,%%r12,2)
#define KERNEL_k1m2n24 KERNEL_h_k1m2n24 "addq $16,%%r15;"
#define end_load_a_k1m2(k_no) "vbroadcastss "#k_no"*8(%0),%%xmm1; vbroadcastss "#k_no"*8+4(%0),%%xmm2;"
#define end_acc_nc2_k1m2(k_no) unit_gen_kernel_k1m2n4(%%xmm6,%%xmm7,k_no,%1,%%r12,1)
#define end_acc_nc3_k1m2(k_no) unit_gen_kernel_k1m2n4(%%xmm8,%%xmm9,k_no,%1,%%r12,2)
#define end_acc_nc4_k1m2(k_no) unit_gen_kernel_k1m2n4(%%xmm10,%%xmm11,k_no,%%r15)
#define end_acc_nc5_k1m2(k_no) unit_gen_kernel_k1m2n4(%%xmm12,%%xmm13,k_no,%%r15,%%r12,1)
#define end_acc_nc6_k1m2(k_no) unit_gen_kernel_k1m2n4(%%xmm14,%%xmm15,k_no,%%r15,%%r12,2)
#ifdef TRMMKERNEL
  #define unit_save_m2n4(c1,c2) \
    "vunpcklps "#c2","#c1",%%xmm1; vunpckhps "#c2","#c1",%%xmm2;"\
    "vmulps %%xmm1,%%xmm0,%%xmm1; vmovsd %%xmm1,(%5); vmovhpd %%xmm1,(%5,%3,1);"\
    "leaq (%5,%3,2),%5;"\
    "vmulps %%xmm2,%%xmm0,%%xmm2; vmovsd %%xmm2,(%5); vmovhpd %%xmm2,(%5,%3,1);"\
    "leaq (%5,%3,2),%5;"
#else
  #define unit_save_m2n4(c1,c2) \
    "vunpcklps "#c2","#c1",%%xmm1; vunpckhps "#c2","#c1",%%xmm2;"\
    "vmovsd (%5),%%xmm3; vmovhpd (%5,%3,1),%%xmm3,%%xmm3; vfmadd213ps %%xmm3,%%xmm0,%%xmm1; vmovsd %%xmm1,(%5); vmovhpd %%xmm1,(%5,%3,1);"\
    "leaq (%5,%3,2),%5;"\
    "vmovsd (%5),%%xmm3; vmovhpd (%5,%3,1),%%xmm3,%%xmm3; vfmadd213ps %%xmm3,%%xmm0,%%xmm2; vmovsd %%xmm2,(%5); vmovhpd %%xmm2,(%5,%3,1);"\
    "leaq (%5,%3,2),%5;"
#endif
#define SAVE_h_m2n4  "movq %2,%5;" unit_save_m2n4(%%xmm4,%%xmm5)
#define SAVE_h_m2n8  SAVE_h_m2n4   unit_save_m2n4(%%xmm6,%%xmm7)
#define SAVE_h_m2n12 SAVE_h_m2n8   unit_save_m2n4(%%xmm8,%%xmm9)
#define SAVE_h_m2n16 SAVE_h_m2n12  unit_save_m2n4(%%xmm10,%%xmm11)
#define SAVE_h_m2n20 SAVE_h_m2n16  unit_save_m2n4(%%xmm12,%%xmm13)
#define SAVE_h_m2n24 SAVE_h_m2n20  unit_save_m2n4(%%xmm14,%%xmm15)
#define SAVE_m2(ndim) SAVE_h_m2n##ndim "addq $8,%2;"
#define COMPUTE_m2(ndim) \
    INIT_m2n##ndim START_SET_PAPB(2,ndim)\
    "movq %%r13,%4;"\
    KERNEL_HEAD_C_n##ndim(2)\
    "testq %4,%4; jz "#ndim"002022f;"\
    #ndim"002021:\n\t"\
    KERNEL_k1m2n##ndim "decq %4; jnz "#ndim"002021b;"\
    #ndim"002022:\n\t"\
    KERNEL_TAIL_C_n##ndim(2)\
    SAVE_m2(ndim) END_SET_PA(2)

/* m = 1 *//* xmm0 for alpha, xmm1-xmm3 and xmm10 for temporary use, xmm4-xmm9 for accumulators */
#define INIT_m1n1 "vpxor %%xmm4,%%xmm4,%%xmm4;"
#define KERNEL_k1m1n1 \
    "vmovss (%1),%%xmm3; addq $4,%1;"\
    "vmovss (%0),%%xmm1; vfmadd231ss %%xmm3,%%xmm1,%%xmm4;"\
    "addq $4,%0;"
#ifdef TRMMKERNEL
  #define SAVE_h_m1n1 "vmulss %%xmm4,%%xmm0,%%xmm4; vmovss %%xmm4,(%2);"
#else
  #define SAVE_h_m1n1 "vfmadd213ss (%2),%%xmm0,%%xmm4; vmovss %%xmm4,(%2);"
#endif
#define INIT_m1n2 INIT_m1n1
#define KERNEL_k1m1n2 \
    "vmovsd (%1),%%xmm3; addq $8,%1;"\
    "vbroadcastss  (%0),%%xmm1; vfmadd231ps %%xmm3,%%xmm1,%%xmm4;"\
    "addq $4,%0;"
#ifdef TRMMKERNEL
  #define SAVE_h_m1n2 \
    "vmulps %%xmm4,%%xmm0,%%xmm4;"\
    "vmovss %%xmm4,(%2); vextractps $1,%%xmm4,(%2,%3,1);"
#else
  #define SAVE_h_m1n2 \
    "vmovss (%2),%%xmm3; vinsertps $16,(%2,%3,1),%%xmm3,%%xmm3; vfmadd213ps %%xmm3,%%xmm0,%%xmm4;"\
    "vmovss %%xmm4,(%2); vextractps $1,%%xmm4,(%2,%3,1);"
#endif
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
#define end_load_a_k1m1(k_no) "vbroadcastss "#k_no"*4(%0),%%xmm1;"
#define end_acc_nc2_k1m1(k_no) "vfmadd231ps "#k_no"*16(%1,%%r12,1),%%xmm1,%%xmm5;"
#define end_acc_nc3_k1m1(k_no) "vfmadd231ps "#k_no"*16(%1,%%r12,2),%%xmm1,%%xmm6;"
#define end_acc_nc4_k1m1(k_no) "vfmadd231ps "#k_no"*16(%%r15),%%xmm1,%%xmm7;"
#define end_acc_nc5_k1m1(k_no) "vfmadd231ps "#k_no"*16(%%r15,%%r12,1),%%xmm1,%%xmm8;"
#define end_acc_nc6_k1m1(k_no) "vfmadd231ps "#k_no"*16(%%r15,%%r12,2),%%xmm1,%%xmm9;"
#ifdef TRMMKERNEL
  #define unit_save_m1n4(c1) \
    "vmulps "#c1",%%xmm0,"#c1"; vpxor %%xmm10,%%xmm10,%%xmm10; vmovsd "#c1",%%xmm10,%%xmm2; vmovhlps "#c1",%%xmm10,%%xmm1;"\
    "vmovss %%xmm2,(%5); vextractps $1,%%xmm2,(%5,%3,1); leaq (%5,%3,2),%5;"\
    "vmovss %%xmm1,(%5); vextractps $1,%%xmm1,(%5,%3,1); leaq (%5,%3,2),%5;"
#else
  #define unit_save_m1n4(c1) \
    "vpxor %%xmm10,%%xmm10,%%xmm10; vmovsd "#c1",%%xmm10,%%xmm2; vmovhlps "#c1",%%xmm10,%%xmm1;"\
    "vmovss (%5),%%xmm3; vinsertps $16,(%5,%3,1),%%xmm3,%%xmm3; vfmadd213ps %%xmm3,%%xmm0,%%xmm2;"\
    "vmovss %%xmm2,(%5); vextractps $1,%%xmm2,(%5,%3,1); leaq (%5,%3,2),%5;"\
    "vmovss (%5),%%xmm3; vinsertps $16,(%5,%3,1),%%xmm3,%%xmm3; vfmadd213ps %%xmm3,%%xmm0,%%xmm1;"\
    "vmovss %%xmm1,(%5); vextractps $1,%%xmm1,(%5,%3,1); leaq (%5,%3,2),%5;"
#endif
#define SAVE_h_m1n4 "movq %2,%5;" unit_save_m1n4(%%xmm4)
#define SAVE_h_m1n8  SAVE_h_m1n4  unit_save_m1n4(%%xmm5)
#define SAVE_h_m1n12 SAVE_h_m1n8  unit_save_m1n4(%%xmm6)
#define SAVE_h_m1n16 SAVE_h_m1n12 unit_save_m1n4(%%xmm7)
#define SAVE_h_m1n20 SAVE_h_m1n16 unit_save_m1n4(%%xmm8)
#define SAVE_h_m1n24 SAVE_h_m1n20 unit_save_m1n4(%%xmm9)
#define SAVE_m1(ndim) SAVE_h_m1n##ndim "addq $4,%2;"
#define COMPUTE_m1(ndim) \
    INIT_m1n##ndim START_SET_PAPB(1,ndim)\
    "movq %%r13,%4;"\
    KERNEL_HEAD_C_n##ndim(1)\
    "testq %4,%4; jz "#ndim"001012f;"\
    #ndim"001011:\n\t"\
    KERNEL_k1m1n##ndim "decq %4; jnz "#ndim"001011b;"\
    #ndim"001012:\n\t"\
    KERNEL_TAIL_C_n##ndim(1)\
    SAVE_m1(ndim) END_SET_PA(1)

/* %7 = "m"(ALPHA), %8 = "m"(M), %9 = "m"(K), %10 = "m"(off) */

#ifdef TRMMKERNEL
  #if BACKWARDS == 1
    #define OFFSET_TO_K "movq %9,%%r13; subq %10,%%r13;"
  #else
    #define OFFSET_TO_K "movq %10,%%r13;"
  #endif
#else
  #define OFFSET_TO_K "movq %9,%%r13;"
#endif
#if defined(TRMMKERNEL) && !defined(LEFT)
  #if BACKWARDS == 1
    #define START_UPDATE_OFFSET(ndim) {}
    #define END_UPDATE_OFFSET(ndim) {off += (ndim);}
  #else
    #define START_UPDATE_OFFSET(ndim) {off += (ndim)>4 ? 4:(ndim);}
    #define END_UPDATE_OFFSET(ndim) {off += (ndim)>4 ? ((ndim)-4):0;}
  #endif
#else
  #define START_UPDATE_OFFSET(ndim) {}
  #define END_UPDATE_OFFSET(ndim) {}
#endif
#if defined(TRMMKERNEL) && defined(LEFT)
  #if BACKWARDS == 1
    #define START_UPDATE_K(mdim) ""
    #define END_UPDATE_K(mdim) "subq $"#mdim",%%r13;"
  #else
    #define START_UPDATE_K(mdim) "addq $"#mdim",%%r13;"
    #define END_UPDATE_K(mdim) ""
  #endif
#else
  #define START_UPDATE_K(mdim) ""
  #define END_UPDATE_K(mdim) ""
#endif
#define COMPUTE(ndim) {\
  next_b = b_pointer + ndim * K; START_UPDATE_OFFSET(ndim)\
  __asm__ __volatile__(\
    "vbroadcastss %7,%%zmm0;"\
    OFFSET_TO_K "movq %9,%%r12; salq $4,%%r12; movq %1,%%r14; movq %8,%%r11;"\
    "cmpq $16,%%r11;jb 33101"#ndim"f;"\
    "33109"#ndim":\n\t"\
    START_UPDATE_K(16) COMPUTE_m16(ndim) END_UPDATE_K(16)\
    "subq $16,%%r11;cmpq $16,%%r11;jnb 33109"#ndim"b;"\
    "33101"#ndim":\n\t"\
    "cmpq $8,%%r11;jb 33102"#ndim"f;"\
    START_UPDATE_K(8) COMPUTE_m8(ndim) END_UPDATE_K(8)\
    "subq $8,%%r11;"\
    "33102"#ndim":\n\t"\
    "cmpq $4,%%r11;jb 33103"#ndim"f;"\
    START_UPDATE_K(4) COMPUTE_m4(ndim) END_UPDATE_K(4)\
    "subq $4,%%r11;"\
    "33103"#ndim":\n\t"\
    "cmpq $2,%%r11;jb 33104"#ndim"f;"\
    START_UPDATE_K(2) COMPUTE_m2(ndim) END_UPDATE_K(2)\
    "subq $2,%%r11;"\
    "33104"#ndim":\n\t"\
    "testq %%r11,%%r11;jz 33105"#ndim"f;"\
    START_UPDATE_K(1) COMPUTE_m1(ndim) END_UPDATE_K(1)\
    "33105"#ndim":\n\t"\
    "movq %%r14,%1; vzeroupper;"\
    :"+r"(a_pointer),"+r"(b_pointer),"+r"(c_pointer),"+r"(ldc_in_bytes),"+r"(k_counter),"+r"(ctemp),"+r"(next_b)\
    :"m"(ALPHA),"m"(M),"m"(K),"m"(off):"r10","r11","r12","r13","r14","r15","cc","memory",\
    "zmm0","zmm1","zmm2","zmm3","zmm4","zmm5","zmm6","zmm7","zmm8","zmm9","zmm10","zmm11","zmm12","zmm13","zmm14","zmm15",\
    "zmm16","zmm17","zmm18","zmm19","zmm20","zmm21","zmm22","zmm23","zmm24","zmm25","zmm26","zmm27","zmm28","zmm29","zmm30","zmm31");\
  a_pointer -= M * K; b_pointer += ndim * K; c_pointer += LDC * ndim - M; END_UPDATE_OFFSET(ndim)\
}
int __attribute__ ((noinline))
CNAME(BLASLONG m, BLASLONG n, BLASLONG k, float alpha, float * __restrict__ A, float * __restrict__ B, float * __restrict__ C, BLASLONG LDC
#ifdef TRMMKERNEL
,BLASLONG offset
#endif
)
{
    if(m==0||n==0) return 0;
    int64_t ldc_in_bytes = (int64_t)LDC * sizeof(float);float ALPHA = alpha;
    int64_t M = (int64_t)m, K = (int64_t)k, k_counter = K, off = 0;
    BLASLONG n_count = n;
    float *a_pointer = A,*b_pointer = B,*c_pointer = C,*ctemp = C,*next_b = B;
#ifdef TRMMKERNEL
  #ifdef LEFT
    off = offset;
  #else
    off = -offset;
  #endif
#endif
    for(;n_count>23;n_count-=24) COMPUTE(24)
    for(;n_count>19;n_count-=20) COMPUTE(20)
    for(;n_count>15;n_count-=16) COMPUTE(16)
    for(;n_count>11;n_count-=12) COMPUTE(12)
    for(;n_count>7;n_count-=8) COMPUTE(8)
    for(;n_count>3;n_count-=4) COMPUTE(4)
    for(;n_count>1;n_count-=2) COMPUTE(2)
    if(n_count>0) COMPUTE(1)
    return 0;
}
#ifndef TRMMKERNEL
  #include <immintrin.h>
  #include "sgemm_direct_skylakex.c"
#endif
