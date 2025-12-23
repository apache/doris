#include <stdint.h>
#include "common.h"

#if  defined(NN) || defined(NT) || defined(TN) || defined(TT)
  #define CGEMM_SKX_MODE 0 //not to do conjugation on a_block and b_block
#endif
#if  defined(RN) || defined(RT) || defined(CN) || defined(CT)
  #define CGEMM_SKX_MODE 1 //do conjugation on a_block, not b_block
#endif
#if  defined(NR) || defined(NC) || defined(TR) || defined(TC)
  #define CGEMM_SKX_MODE 2 //do conjugation on b_block, not a_block
#endif
#if  defined(RR) || defined(RC) || defined(CR) || defined(CC)
  #define CGEMM_SKX_MODE 3 //do conjugation on a_block and b_block
#endif

// recommended settings: GEMM_DEFAULT_Q = 192, GEMM_DEFAULT_P = 384
/* %0=a_pointer, %1=b_pointer, %2=c_pointer, %3=c_store, %4=ldc(bytes), %5=&constval, %6 = k_counter, %7 = m_counter, %8 = b_pref */
// const float constval[4] = {alpha_r, alpha_i, -1, 1};
/* r11 = m; r12 = k * 16; r13 = k; r14 = b_head; r15 = %1 + r12 * 3; */
#define GENERAL_INIT "movq %7,%%r11; movq %1,%%r14; movq %6,%%r13; movq %6,%%r12; salq $4,%%r12;"
#define GENERAL_RECOVER "movq %%r11,%7; movq %%r13,%6; movq %%r14,%1;"
#define CONSTZMM_INIT "vbroadcastss (%5),%%zmm0; vbroadcastss 4(%5),%%zmm1; vbroadcastsd 8(%5),%%zmm2;"
#define COMPUTE_INIT "movq %%r13,%6; movq %%r14,%1; leaq (%%r14,%%r12,2),%%r15; addq %%r12,%%r15;"

/* m=8, zmm0=alpha_r, zmm1=alpha_i, zmm2={-1,1,...,-1,1}, zmm3-zmm7 for temporary use, zmm8-zmm31 for accumulators */
#if CGEMM_SKX_MODE == 0 || CGEMM_SKX_MODE == 2 //not to do conjugation on a_block
  #define unit_kernel_k1m8n1(a_r,a_i,b_off,c_le,c_ri,...) \
    "vbroadcastsd "#b_off"("#__VA_ARGS__"),%%zmm3; vfmadd231ps "#a_r",%%zmm3,"#c_le"; vfmadd231ps "#a_i",%%zmm3,"#c_ri";"
#else //do conjugation on a_block
  #define unit_kernel_k1m8n1(a_r,a_i,b_off,c_le,c_ri,...) \
    "vbroadcastsd "#b_off"("#__VA_ARGS__"),%%zmm3; vfmadd231ps "#a_r",%%zmm3,"#c_le"; vfnmadd231ps "#a_i",%%zmm3,"#c_ri";"
#endif
#define KERNEL_h_k1m8n1 \
    "vmovsldup (%0),%%zmm4; vmovshdup (%0),%%zmm5; prefetcht0 512(%0); addq $64,%0;"\
    unit_kernel_k1m8n1(%%zmm4,%%zmm5,0,%%zmm8,%%zmm9,%1)
#define KERNEL_t_k1m8n1 KERNEL_h_k1m8n1 "addq $8,%1;"
#define KERNEL_h_k1m8n2 KERNEL_h_k1m8n1 unit_kernel_k1m8n1(%%zmm4,%%zmm5,8,%%zmm10,%%zmm11,%1)
#define KERNEL_t_k1m8n2 KERNEL_h_k1m8n2 "addq $16,%1;"
#define unit_kernel_k1m8n2(c1le,c1ri,c2le,c2ri,...) \
    unit_kernel_k1m8n1(%%zmm4,%%zmm5,0,c1le,c1ri,__VA_ARGS__)\
    unit_kernel_k1m8n1(%%zmm4,%%zmm5,8,c2le,c2ri,__VA_ARGS__)
#define KERNEL_h_k1m8n4 KERNEL_h_k1m8n2 unit_kernel_k1m8n2(%%zmm12,%%zmm13,%%zmm14,%%zmm15,%1,%%r12,1)
#define KERNEL_t_k1m8n4 KERNEL_h_k1m8n4 "addq $16,%1;"
#define KERNEL_t_k1m8n6 KERNEL_h_k1m8n4 unit_kernel_k1m8n2(%%zmm16,%%zmm17,%%zmm18,%%zmm19,%1,%%r12,2) "addq $16,%1;"
#define KERNEL_h_k1m8n8 KERNEL_t_k1m8n6 unit_kernel_k1m8n2(%%zmm20,%%zmm21,%%zmm22,%%zmm23,%%r15)
#define KERNEL_t_k1m8n8 KERNEL_h_k1m8n8 "addq $16,%%r15;"
#define KERNEL_h_k1m8n10 KERNEL_h_k1m8n8 unit_kernel_k1m8n2(%%zmm24,%%zmm25,%%zmm26,%%zmm27,%%r15,%%r12,1)
#define KERNEL_t_k1m8n10 KERNEL_h_k1m8n10 "addq $16,%%r15;"
#define KERNEL_h_k1m8n12 KERNEL_h_k1m8n10 unit_kernel_k1m8n2(%%zmm28,%%zmm29,%%zmm30,%%zmm31,%%r15,%%r12,2)
#define KERNEL_t_k1m8n12 KERNEL_h_k1m8n12 "addq $16,%%r15;"
#if CGEMM_SKX_MODE == 0 || CGEMM_SKX_MODE == 1 //not to do conjugation on b_block
  #define unit_save_m8n1(c_le,c_ri,...) \
    "vpermilps $177,"#c_ri","#c_ri"; vfmadd231ps "#c_ri",%%zmm2,"#c_le"; vpermilps $177,"#c_le",%%zmm4;"\
    "vfmaddsub213ps ("#__VA_ARGS__"),%%zmm1,%%zmm4; vfmaddsub213ps %%zmm4,%%zmm0,"#c_le"; vmovups "#c_le",("#__VA_ARGS__");"
#else //do conjugation on b_block
  #define unit_save_m8n1(c_le,c_ri,...) \
    "vpermilps $177,"#c_ri","#c_ri"; vfnmadd231ps "#c_ri",%%zmm2,"#c_le"; vpermilps $177,"#c_le",%%zmm4;"\
    "vfmsubadd213ps ("#__VA_ARGS__"),%%zmm0,"#c_le"; vfmsubadd231ps %%zmm4,%%zmm1,"#c_le"; vmovups "#c_le",("#__VA_ARGS__");"
#endif
#define SAVE_SETUP_m8 "movq %2,%3; addq $64,%2;"
#define SAVE_m8n1 SAVE_SETUP_m8 unit_save_m8n1(%%zmm8,%%zmm9,%3)
#define SAVE_m8n2 SAVE_m8n1 unit_save_m8n1(%%zmm10,%%zmm11,%3,%4,1)
#define unit_save_m8n2(c1le,c1ri,c2le,c2ri) \
    "leaq (%3,%4,2),%3;" unit_save_m8n1(c1le,c1ri,%3) unit_save_m8n1(c2le,c2ri,%3,%4,1)
#define SAVE_m8n4 SAVE_m8n2 unit_save_m8n2(%%zmm12,%%zmm13,%%zmm14,%%zmm15)
#define SAVE_m8n6 SAVE_m8n4 unit_save_m8n2(%%zmm16,%%zmm17,%%zmm18,%%zmm19)
#define SAVE_m8n8 SAVE_m8n6 unit_save_m8n2(%%zmm20,%%zmm21,%%zmm22,%%zmm23)
#define SAVE_m8n10 SAVE_m8n8 unit_save_m8n2(%%zmm24,%%zmm25,%%zmm26,%%zmm27)
#define SAVE_m8n12 SAVE_m8n10 unit_save_m8n2(%%zmm28,%%zmm29,%%zmm30,%%zmm31)
#define unit_init_m8n1(c_le,c_ri) "vpxorq "#c_le","#c_le","#c_le"; vpxorq "#c_ri","#c_ri","#c_ri";"
#define INIT_m8n1 unit_init_m8n1(%%zmm8,%%zmm9)
#define INIT_m8n2 INIT_m8n1 unit_init_m8n1(%%zmm10,%%zmm11)
#define INIT_m8n4 INIT_m8n2 unit_init_m8n1(%%zmm12,%%zmm13) unit_init_m8n1(%%zmm14,%%zmm15)
#define INIT_m8n6 INIT_m8n4 unit_init_m8n1(%%zmm16,%%zmm17) unit_init_m8n1(%%zmm18,%%zmm19)
#define INIT_m8n8 INIT_m8n6 unit_init_m8n1(%%zmm20,%%zmm21) unit_init_m8n1(%%zmm22,%%zmm23)
#define INIT_m8n10 INIT_m8n8 unit_init_m8n1(%%zmm24,%%zmm25) unit_init_m8n1(%%zmm26,%%zmm27)
#define INIT_m8n12 INIT_m8n10 unit_init_m8n1(%%zmm28,%%zmm29) unit_init_m8n1(%%zmm30,%%zmm31)
#define COMPUTE_m8(ndim) \
    INIT_m8n##ndim\
    COMPUTE_INIT "movq %2,%3;"\
    "cmpq $18,%6; jb "#ndim"88880f;"\
    #ndim"88889:\n\t"\
    KERNEL_t_k1m8n##ndim\
    KERNEL_t_k1m8n##ndim\
    KERNEL_t_k1m8n##ndim\
    "prefetcht1 (%3); prefetcht1 63(%3); addq %4,%3;"\
    KERNEL_t_k1m8n##ndim\
    KERNEL_t_k1m8n##ndim\
    KERNEL_t_k1m8n##ndim\
    "prefetcht1 (%8); addq $40,%8;"\
    "subq $6,%6; cmpq $18,%6; jnb "#ndim"88889b;"\
    "movq %2,%3;"\
    #ndim"88880:\n\t"\
    "testq %6,%6; jz "#ndim"88881f;"\
    "prefetcht0 (%3); prefetcht0 63(%3); addq %4,%3;"\
    KERNEL_t_k1m8n##ndim\
    "decq %6; jmp "#ndim"88880b;"\
    #ndim"88881:\n\t"\
    SAVE_m8n##ndim

/* m=4, ymm0-ymm3 for temporary use, ymm4-ymm15 for accumulators */
#if CGEMM_SKX_MODE == 0 || CGEMM_SKX_MODE == 3 //conjg_a == conjg_b; ap = permilps($177,a0)
  #define unit_kernel_k1m4n1(a0,ap,b_off_r,b_off_i,c1,...) \
    "vbroadcastss "#b_off_i"("#__VA_ARGS__"),%%ymm2; vfmaddsub231ps "#ap",%%ymm2,"#c1";"\
    "vbroadcastss "#b_off_r"("#__VA_ARGS__"),%%ymm2; vfmaddsub231ps "#a0",%%ymm2,"#c1";"
#else //conjg_a != conjg_b
  #define unit_kernel_k1m4n1(a0,ap,b_off_r,b_off_i,c1,...) \
    "vbroadcastss "#b_off_i"("#__VA_ARGS__"),%%ymm2; vfmsubadd231ps "#ap",%%ymm2,"#c1";"\
    "vbroadcastss "#b_off_r"("#__VA_ARGS__"),%%ymm2; vfmsubadd231ps "#a0",%%ymm2,"#c1";"
#endif
#define KERNEL_h_k1m4n1 \
    "vmovups (%0),%%ymm0; vpermilps $177,%%ymm0,%%ymm1; addq $32,%0;"\
    unit_kernel_k1m4n1(%%ymm0,%%ymm1,0,4,%%ymm4,%1)
#define KERNEL_t_k1m4n1 KERNEL_h_k1m4n1 "addq $8,%1;"
#define KERNEL_h_k1m4n2 KERNEL_h_k1m4n1 unit_kernel_k1m4n1(%%ymm0,%%ymm1,8,12,%%ymm5,%1)
#define KERNEL_t_k1m4n2 KERNEL_h_k1m4n2 "addq $16,%1;"
#define unit_kernel_k1m4n2(c1,c2,...) \
    unit_kernel_k1m4n1(%%ymm0,%%ymm1,0,4,c1,__VA_ARGS__)\
    unit_kernel_k1m4n1(%%ymm0,%%ymm1,8,12,c2,__VA_ARGS__)
#define KERNEL_h_k1m4n4 KERNEL_h_k1m4n2 unit_kernel_k1m4n2(%%ymm6,%%ymm7,%1,%%r12,1)
#define KERNEL_t_k1m4n4 KERNEL_h_k1m4n4 "addq $16,%1;"
#define KERNEL_t_k1m4n6 KERNEL_h_k1m4n4 unit_kernel_k1m4n2(%%ymm8,%%ymm9,%1,%%r12,2) "addq $16,%1;"
#define KERNEL_h_k1m4n8 KERNEL_t_k1m4n6 unit_kernel_k1m4n2(%%ymm10,%%ymm11,%%r15)
#define KERNEL_t_k1m4n8 KERNEL_h_k1m4n8 "addq $16,%%r15;"
#define KERNEL_h_k1m4n10 KERNEL_h_k1m4n8 unit_kernel_k1m4n2(%%ymm12,%%ymm13,%%r15,%%r12,1)
#define KERNEL_t_k1m4n10 KERNEL_h_k1m4n10 "addq $16,%%r15;"
#define KERNEL_h_k1m4n12 KERNEL_h_k1m4n10 unit_kernel_k1m4n2(%%ymm14,%%ymm15,%%r15,%%r12,2)
#define KERNEL_t_k1m4n12 KERNEL_h_k1m4n12 "addq $16,%%r15;"
#if CGEMM_SKX_MODE == 0 || CGEMM_SKX_MODE == 2 //not to do conjugation on a_block
  #define unit_save_m4n1(alp_r,alp_i,c1,...) \
    "vpermilps $177,"#c1",%%ymm3; vfmaddsub213ps ("#__VA_ARGS__"),"#alp_i",%%ymm3;"\
    "vfmaddsub213ps %%ymm3,"#alp_r","#c1";vmovups "#c1",("#__VA_ARGS__");"
#else //do conjugation on a_block
  #define unit_save_m4n1(alp_r,alp_i,c1,...) \
    "vpermilps $177,"#c1",%%ymm3; vfmsubadd213ps ("#__VA_ARGS__"),"#alp_r","#c1";"\
    "vfmsubadd231ps %%ymm3,"#alp_i","#c1";vmovups "#c1",("#__VA_ARGS__");"
#endif
#define SAVE_SETUP_m4 "movq %2,%3; addq $32,%2; vbroadcastss (%5),%%ymm0; vbroadcastss 4(%5),%%ymm1;"
#define SAVE_m4n1 SAVE_SETUP_m4 unit_save_m4n1(%%ymm0,%%ymm1,%%ymm4,%3)
#define SAVE_m4n2 SAVE_m4n1 unit_save_m4n1(%%ymm0,%%ymm1,%%ymm5,%3,%4,1)
#define unit_save_m4n2(c1,c2) \
    "leaq (%3,%4,2),%3;" unit_save_m4n1(%%ymm0,%%ymm1,c1,%3) unit_save_m4n1(%%ymm0,%%ymm1,c2,%3,%4,1)
#define SAVE_m4n4 SAVE_m4n2 unit_save_m4n2(%%ymm6,%%ymm7)
#define SAVE_m4n6 SAVE_m4n4 unit_save_m4n2(%%ymm8,%%ymm9)
#define SAVE_m4n8 SAVE_m4n6 unit_save_m4n2(%%ymm10,%%ymm11)
#define SAVE_m4n10 SAVE_m4n8 unit_save_m4n2(%%ymm12,%%ymm13)
#define SAVE_m4n12 SAVE_m4n10 unit_save_m4n2(%%ymm14,%%ymm15)
#define INIT_m4n1 "vpxor %%ymm4,%%ymm4,%%ymm4;"
#define unit_init_m4n2(c1,c2) "vpxor "#c1","#c1","#c1"; vpxor "#c2","#c2","#c2";"
#define INIT_m4n2 unit_init_m4n2(%%ymm4,%%ymm5)
#define INIT_m4n4 INIT_m4n2 unit_init_m4n2(%%ymm6,%%ymm7)
#define INIT_m4n6 INIT_m4n4 unit_init_m4n2(%%ymm8,%%ymm9)
#define INIT_m4n8 INIT_m4n6 unit_init_m4n2(%%ymm10,%%ymm11)
#define INIT_m4n10 INIT_m4n8 unit_init_m4n2(%%ymm12,%%ymm13)
#define INIT_m4n12 INIT_m4n10 unit_init_m4n2(%%ymm14,%%ymm15)
#define COMPUTE_m4(ndim) \
    INIT_m4n##ndim\
    COMPUTE_INIT\
    #ndim"88440:\n\t"\
    "testq %6,%6; jz "#ndim"88441f;"\
    KERNEL_t_k1m4n##ndim\
    "decq %6; jmp "#ndim"88440b;"\
    #ndim"88441:\n\t"\
    SAVE_m4n##ndim

/* m=2, xmm0-xmm3 for temporary use, xmm4-xmm15 for accumulators */
#if CGEMM_SKX_MODE == 0 || CGEMM_SKX_MODE == 3 //conjg_a == conjg_b;
  #define unit_kernel_k1m2n1(a0,ap,b_off_r,b_off_i,c1,...) \
    "vbroadcastss "#b_off_i"("#__VA_ARGS__"),%%xmm2; vfmaddsub231ps "#ap",%%xmm2,"#c1";"\
    "vbroadcastss "#b_off_r"("#__VA_ARGS__"),%%xmm2; vfmaddsub231ps "#a0",%%xmm2,"#c1";"
#else //conjg_a != conjg_b
  #define unit_kernel_k1m2n1(a0,ap,b_off_r,b_off_i,c1,...) \
    "vbroadcastss "#b_off_i"("#__VA_ARGS__"),%%xmm2; vfmsubadd231ps "#ap",%%xmm2,"#c1";"\
    "vbroadcastss "#b_off_r"("#__VA_ARGS__"),%%xmm2; vfmsubadd231ps "#a0",%%xmm2,"#c1";"
#endif
#define KERNEL_h_k1m2n1 \
    "vmovups (%0),%%xmm0; vpermilps $177,%%xmm0,%%xmm1; addq $16,%0;"\
    unit_kernel_k1m2n1(%%xmm0,%%xmm1,0,4,%%xmm4,%1)
#define KERNEL_t_k1m2n1 KERNEL_h_k1m2n1 "addq $8,%1;"
#define KERNEL_h_k1m2n2 KERNEL_h_k1m2n1 unit_kernel_k1m2n1(%%xmm0,%%xmm1,8,12,%%xmm5,%1)
#define KERNEL_t_k1m2n2 KERNEL_h_k1m2n2 "addq $16,%1;"
#define unit_kernel_k1m2n2(c1,c2,...) \
    unit_kernel_k1m2n1(%%xmm0,%%xmm1,0,4,c1,__VA_ARGS__)\
    unit_kernel_k1m2n1(%%xmm0,%%xmm1,8,12,c2,__VA_ARGS__)
#define KERNEL_h_k1m2n4 KERNEL_h_k1m2n2 unit_kernel_k1m2n2(%%xmm6,%%xmm7,%1,%%r12,1)
#define KERNEL_t_k1m2n4 KERNEL_h_k1m2n4 "addq $16,%1;"
#define KERNEL_t_k1m2n6 KERNEL_h_k1m2n4 unit_kernel_k1m2n2(%%xmm8,%%xmm9,%1,%%r12,2) "addq $16,%1;"
#define KERNEL_h_k1m2n8 KERNEL_t_k1m2n6 unit_kernel_k1m2n2(%%xmm10,%%xmm11,%%r15)
#define KERNEL_t_k1m2n8 KERNEL_h_k1m2n8 "addq $16,%%r15;"
#define KERNEL_h_k1m2n10 KERNEL_h_k1m2n8 unit_kernel_k1m2n2(%%xmm12,%%xmm13,%%r15,%%r12,1)
#define KERNEL_t_k1m2n10 KERNEL_h_k1m2n10 "addq $16,%%r15;"
#define KERNEL_h_k1m2n12 KERNEL_h_k1m2n10 unit_kernel_k1m2n2(%%xmm14,%%xmm15,%%r15,%%r12,2)
#define KERNEL_t_k1m2n12 KERNEL_h_k1m2n12 "addq $16,%%r15;"
#if CGEMM_SKX_MODE == 0 || CGEMM_SKX_MODE == 2 //not to do conjugation on a_block
  #define unit_save_m2n1(alp_r,alp_i,c1,...) \
    "vpermilps $177,"#c1",%%xmm3; vfmaddsub213ps ("#__VA_ARGS__"),"#alp_i",%%xmm3;"\
    "vfmaddsub213ps %%xmm3,"#alp_r","#c1";vmovups "#c1",("#__VA_ARGS__");"
#else //do conjugation on a_block
  #define unit_save_m2n1(alp_r,alp_i,c1,...) \
    "vpermilps $177,"#c1",%%xmm3; vfmsubadd213ps ("#__VA_ARGS__"),"#alp_r","#c1";"\
    "vfmsubadd231ps %%xmm3,"#alp_i","#c1";vmovups "#c1",("#__VA_ARGS__");"
#endif
#define SAVE_SETUP_m2 "movq %2,%3; addq $16,%2; vbroadcastss (%5),%%xmm0; vbroadcastss 4(%5),%%xmm1;"
#define SAVE_m2n1 SAVE_SETUP_m2 unit_save_m2n1(%%xmm0,%%xmm1,%%xmm4,%3)
#define SAVE_m2n2 SAVE_m2n1 unit_save_m2n1(%%xmm0,%%xmm1,%%xmm5,%3,%4,1)
#define unit_save_m2n2(c1,c2) \
    "leaq (%3,%4,2),%3;" unit_save_m2n1(%%xmm0,%%xmm1,c1,%3) unit_save_m2n1(%%xmm0,%%xmm1,c2,%3,%4,1)
#define SAVE_m2n4 SAVE_m2n2 unit_save_m2n2(%%xmm6,%%xmm7)
#define SAVE_m2n6 SAVE_m2n4 unit_save_m2n2(%%xmm8,%%xmm9)
#define SAVE_m2n8 SAVE_m2n6 unit_save_m2n2(%%xmm10,%%xmm11)
#define SAVE_m2n10 SAVE_m2n8 unit_save_m2n2(%%xmm12,%%xmm13)
#define SAVE_m2n12 SAVE_m2n10 unit_save_m2n2(%%xmm14,%%xmm15)
#define INIT_m2n1 "vpxor %%xmm4,%%xmm4,%%xmm4;"
#define unit_init_m2n2(c1,c2) "vpxor "#c1","#c1","#c1"; vpxor "#c2","#c2","#c2";"
#define INIT_m2n2 unit_init_m2n2(%%xmm4,%%xmm5)
#define INIT_m2n4 INIT_m2n2 unit_init_m2n2(%%xmm6,%%xmm7)
#define INIT_m2n6 INIT_m2n4 unit_init_m2n2(%%xmm8,%%xmm9)
#define INIT_m2n8 INIT_m2n6 unit_init_m2n2(%%xmm10,%%xmm11)
#define INIT_m2n10 INIT_m2n8 unit_init_m2n2(%%xmm12,%%xmm13)
#define INIT_m2n12 INIT_m2n10 unit_init_m2n2(%%xmm14,%%xmm15)
#define COMPUTE_m2(ndim) \
    INIT_m2n##ndim\
    COMPUTE_INIT\
    #ndim"88220:\n\t"\
    "testq %6,%6; jz "#ndim"88221f;"\
    KERNEL_t_k1m2n##ndim\
    "decq %6; jmp "#ndim"88220b;"\
    #ndim"88221:\n\t"\
    SAVE_m2n##ndim

/* m=1, xmm0-xmm3 and xmm10-xmm15 for temporary use, xmm4-xmm9 for accumulators */
#if CGEMM_SKX_MODE == 0 || CGEMM_SKX_MODE == 3 //conjg_a == conjg_b; ap = permilps($177,a0)
  #define unit_kernel_k1m1n1(a0,ap,b_off_r,b_off_i,c1,...) \
    "vbroadcastss "#b_off_i"("#__VA_ARGS__"),%%xmm2; vfmaddsub231ps "#ap",%%xmm2,"#c1";"\
    "vbroadcastss "#b_off_r"("#__VA_ARGS__"),%%xmm2; vfmaddsub231ps "#a0",%%xmm2,"#c1";"
  #define unit_kernel_k1m1n2(a0,ap,c1,...) \
    "vmovshdup ("#__VA_ARGS__"),%%xmm2; vfmaddsub231ps "#ap",%%xmm2,"#c1";"\
    "vmovsldup ("#__VA_ARGS__"),%%xmm2; vfmaddsub231ps "#a0",%%xmm2,"#c1";"
#else //conjg_a != conjg_b
  #define unit_kernel_k1m1n1(a0,ap,b_off_r,b_off_i,c1,...) \
    "vbroadcastss "#b_off_i"("#__VA_ARGS__"),%%xmm2; vfmsubadd231ps "#ap",%%xmm2,"#c1";"\
    "vbroadcastss "#b_off_r"("#__VA_ARGS__"),%%xmm2; vfmsubadd231ps "#a0",%%xmm2,"#c1";"
  #define unit_kernel_k1m1n2(a0,ap,c1,...) \
    "vmovshdup ("#__VA_ARGS__"),%%xmm2; vfmsubadd231ps "#ap",%%xmm2,"#c1";"\
    "vmovsldup ("#__VA_ARGS__"),%%xmm2; vfmsubadd231ps "#a0",%%xmm2,"#c1";"
#endif
#define KERNEL_h_k1m1n1 \
    "vmovsd (%0),%%xmm0; vpermilps $177,%%xmm0,%%xmm1; addq $8,%0;"\
    unit_kernel_k1m1n1(%%xmm0,%%xmm1,0,4,%%xmm4,%1)
#define KERNEL_t_k1m1n1 KERNEL_h_k1m1n1 "addq $8,%1;"
#define KERNEL_h_k1m1n2 \
    "vmovddup (%0),%%xmm0; vpermilps $177,%%xmm0,%%xmm1; addq $8,%0;"\
    unit_kernel_k1m1n2(%%xmm0,%%xmm1,%%xmm4,%1)
#define KERNEL_t_k1m1n2 KERNEL_h_k1m1n2 "addq $16,%1;"
#define KERNEL_h_k1m1n4 KERNEL_h_k1m1n2 unit_kernel_k1m1n2(%%xmm0,%%xmm1,%%xmm5,%1,%%r12,1)
#define KERNEL_t_k1m1n4 KERNEL_h_k1m1n4 "addq $16,%1;"
#define KERNEL_t_k1m1n6 KERNEL_h_k1m1n4 unit_kernel_k1m1n2(%%xmm0,%%xmm1,%%xmm6,%1,%%r12,2) "addq $16,%1;"
#define KERNEL_h_k1m1n8 KERNEL_t_k1m1n6 unit_kernel_k1m1n2(%%xmm0,%%xmm1,%%xmm7,%%r15)
#define KERNEL_t_k1m1n8 KERNEL_h_k1m1n8 "addq $16,%%r15;"
#define KERNEL_h_k1m1n10 KERNEL_h_k1m1n8 unit_kernel_k1m1n2(%%xmm0,%%xmm1,%%xmm8,%%r15,%%r12,1)
#define KERNEL_t_k1m1n10 KERNEL_h_k1m1n10 "addq $16,%%r15;"
#define KERNEL_h_k1m1n12 KERNEL_h_k1m1n10 unit_kernel_k1m1n2(%%xmm0,%%xmm1,%%xmm9,%%r15,%%r12,2)
#define KERNEL_t_k1m1n12 KERNEL_h_k1m1n12 "addq $16,%%r15;"
#if CGEMM_SKX_MODE == 0 || CGEMM_SKX_MODE == 2 //not to do conjugation on a_block
  #define unit_save_m1n1(alp_r,alp_i,c1,...) \
    "vpermilps $177,"#c1",%%xmm3; vmovsd ("#__VA_ARGS__"),%%xmm2; vfmaddsub213ps %%xmm2,"#alp_i",%%xmm3;"\
    "vfmaddsub213ps %%xmm3,"#alp_r","#c1";vmovsd "#c1",("#__VA_ARGS__");"
  #define unit_save_m1n2(alp_r,alp_i,c1) \
    "vpermilps $177,"#c1",%%xmm3; vmovsd (%3),%%xmm2; vmovhpd (%3,%4,1),%%xmm2,%%xmm2;"\
    "vfmaddsub213ps %%xmm2,"#alp_i",%%xmm3; vfmaddsub231ps "#c1","#alp_r",%%xmm3;"\
    "vmovsd %%xmm3,(%3); vmovhpd %%xmm3,(%3,%4,1); leaq (%3,%4,2),%3;"
#else //do conjugation on a_block
  #define unit_save_m1n1(alp_r,alp_i,c1,...) \
    "vpermilps $177,"#c1",%%xmm3; vmovsd ("#__VA_ARGS__"),%%xmm2; vfmsubadd213ps %%xmm2,"#alp_r","#c1";"\
    "vfmsubadd231ps %%xmm3,"#alp_i","#c1";vmovsd "#c1",("#__VA_ARGS__");"
  #define unit_save_m1n2(alp_r,alp_i,c1) \
    "vpermilps $177,"#c1",%%xmm3; vmovsd (%3),%%xmm2; vmovhpd (%3,%4,1),%%xmm2,%%xmm2;"\
    "vfmsubadd213ps %%xmm2,"#alp_r","#c1"; vfmsubadd213ps "#c1","#alp_i",%%xmm3;"\
    "vmovsd %%xmm3,(%3); vmovhpd %%xmm3,(%3,%4,1); leaq (%3,%4,2),%3;"
#endif
#define SAVE_SETUP_m1 "movq %2,%3; addq $8,%2; vbroadcastss (%5),%%xmm0; vbroadcastss 4(%5),%%xmm1;"
#define SAVE_m1n1 SAVE_SETUP_m1 unit_save_m1n1(%%xmm0,%%xmm1,%%xmm4,%3)
#define SAVE_m1n2 SAVE_SETUP_m1 unit_save_m1n2(%%xmm0,%%xmm1,%%xmm4)
#define SAVE_m1n4 SAVE_m1n2 unit_save_m1n2(%%xmm0,%%xmm1,%%xmm5)
#define SAVE_m1n6 SAVE_m1n4 unit_save_m1n2(%%xmm0,%%xmm1,%%xmm6)
#define SAVE_m1n8 SAVE_m1n6 unit_save_m1n2(%%xmm0,%%xmm1,%%xmm7)
#define SAVE_m1n10 SAVE_m1n8 unit_save_m1n2(%%xmm0,%%xmm1,%%xmm8)
#define SAVE_m1n12 SAVE_m1n10 unit_save_m1n2(%%xmm0,%%xmm1,%%xmm9)
#define INIT_m1n1 "vpxor %%xmm4,%%xmm4,%%xmm4;"
#define INIT_m1n2 INIT_m2n1
#define INIT_m1n4 INIT_m1n2 "vpxor %%xmm5,%%xmm5,%%xmm5;"
#define INIT_m1n6 INIT_m1n4 "vpxor %%xmm6,%%xmm6,%%xmm6;"
#define INIT_m1n8 INIT_m1n6 "vpxor %%xmm7,%%xmm7,%%xmm7;"
#define INIT_m1n10 INIT_m1n8 "vpxor %%xmm8,%%xmm8,%%xmm8;"
#define INIT_m1n12 INIT_m1n10 "vpxor %%xmm9,%%xmm9,%%xmm9;"
#define COMPUTE_m1(ndim) \
    INIT_m1n##ndim\
    COMPUTE_INIT\
    #ndim"88110:\n\t"\
    "testq %6,%6; jz "#ndim"88111f;"\
    KERNEL_t_k1m1n##ndim\
    "decq %6; jmp "#ndim"88110b;"\
    #ndim"88111:\n\t"\
    SAVE_m1n##ndim

#define COMPUTE(ndim) {\
    b_pref = b_pointer + ndim * K * 2;\
    __asm__ __volatile__(\
    GENERAL_INIT\
    CONSTZMM_INIT\
    "cmpq $8,%7;jb 33101"#ndim"f;"\
    "33109"#ndim":\n\t"\
    COMPUTE_m8(ndim)\
    "subq $8,%7;cmpq $8,%7;jnb 33109"#ndim"b;"\
    "33101"#ndim":\n\t"\
    "cmpq $4,%7;jb 33102"#ndim"f;"\
    COMPUTE_m4(ndim)\
    "subq $4,%7;"\
    "33102"#ndim":\n\t"\
    "cmpq $2,%7;jb 33103"#ndim"f;"\
    COMPUTE_m2(ndim)\
    "subq $2,%7;"\
    "33103"#ndim":\n\t"\
    "testq %7,%7;jz 33104"#ndim"f;"\
    COMPUTE_m1(ndim)\
    "33104"#ndim":\n\t"\
    GENERAL_RECOVER\
    :"+r"(a_pointer),"+r"(b_pointer),"+r"(c_pointer),"+r"(c_store),"+r"(ldc_in_bytes),"+r"(constval),"+r"(K),"+r"(M),"+r"(b_pref)\
    ::"r11","r12","r13","r14","r15","zmm0","zmm1","zmm2","zmm3","zmm4","zmm5","zmm6","zmm7","zmm8","zmm9","zmm10","zmm11","zmm12","zmm13","zmm14",\
    "zmm15","zmm16","zmm17","zmm18","zmm19","zmm20","zmm21","zmm22","zmm23","zmm24","zmm25","zmm26","zmm27","zmm28","zmm29","zmm30","zmm31",\
    "cc","memory");\
    a_pointer -= M * K * 2; b_pointer += ndim * K * 2; c_pointer += (LDC * ndim - M) * 2;\
}

int __attribute__ ((noinline))
CNAME(BLASLONG m, BLASLONG n, BLASLONG k, float alphar, float alphai, float * __restrict__ A, float * __restrict__ B, float * __restrict__ C, BLASLONG LDC)
{
    if(m==0||n==0||k==0) return 0;
    int64_t ldc_in_bytes = (int64_t)LDC * sizeof(float) * 2; float const_val[4] = {alphar, alphai, -1, 1};
    int64_t M = (int64_t)m, K = (int64_t)k;
    BLASLONG n_count = n;
    float *a_pointer = A,*b_pointer = B,*c_pointer = C,*c_store = C,*constval = const_val,*b_pref = B;
    for(;n_count>11;n_count-=12) COMPUTE(12)
    for(;n_count>9;n_count-=10) COMPUTE(10)
    for(;n_count>7;n_count-=8) COMPUTE(8)
    for(;n_count>5;n_count-=6) COMPUTE(6)
    for(;n_count>3;n_count-=4) COMPUTE(4)
    for(;n_count>1;n_count-=2) COMPUTE(2)
    if(n_count>0) COMPUTE(1)
    return 0;
}
