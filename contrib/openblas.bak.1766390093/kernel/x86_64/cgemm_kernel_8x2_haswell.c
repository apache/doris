#include "common.h"
#include <stdint.h>

/* recommended settings: GEMM_P = 256, GEMM_Q = 256 */

#if  defined(NN) || defined(NT) || defined(TN) || defined(TT)
  #define A_CONJ 0
  #define B_CONJ 0
#endif
#if  defined(RN) || defined(RT) || defined(CN) || defined(CT)
  #define A_CONJ 1
  #define B_CONJ 0
#endif
#if  defined(NR) || defined(NC) || defined(TR) || defined(TC)
  #define A_CONJ 0
  #define B_CONJ 1
#endif
#if  defined(RR) || defined(RC) || defined(CR) || defined(CC)
  #define A_CONJ 1
  #define B_CONJ 1
#endif

/* %0 = a_ptr, %1 = b_ptr, %2 = c_ptr, %3 = c_tmp, %4 = ldc(bytes), %5 = k_counter, %6 = &alpha, %7 = m_counter, %8 = b_pref */
/* r11 = m, r12 = k << 4, r13 = k, r14 = b_head, r15 = temp */

/* m=8, ymm 0-3 temp, ymm 4-15 acc */
#if A_CONJ == B_CONJ
  #define acc_m4n1_exp(ar,ai,b2,cl,cr) "vfmadd231ps %%ymm"#ar",%%ymm"#b2",%%ymm"#cl"; vfmadd231ps %%ymm"#ai",%%ymm"#b2",%%ymm"#cr";"
  #define acc_m8n1_con(ua,la,b1,uc,lc) "vfmaddsub231ps %%ymm"#ua",%%ymm"#b1",%%ymm"#uc"; vfmaddsub231ps %%ymm"#la",%%ymm"#b1",%%ymm"#lc";"
#else
  #define acc_m4n1_exp(ar,ai,b2,cl,cr) "vfmadd231ps %%ymm"#ar",%%ymm"#b2",%%ymm"#cl"; vfnmadd231ps %%ymm"#ai",%%ymm"#b2",%%ymm"#cr";"
  #define acc_m8n1_con(ua,la,b1,uc,lc) "vfmsubadd231ps %%ymm"#ua",%%ymm"#b1",%%ymm"#uc"; vfmsubadd231ps %%ymm"#la",%%ymm"#b1",%%ymm"#lc";"
#endif
/* expanded accumulators for m8n1 and m8n2 */
#define KERNEL_k1m8n1 \
  "vbroadcastsd (%1),%%ymm0; addq $8,%1;"\
  "vmovsldup (%0),%%ymm1; vmovshdup (%0),%%ymm2;" acc_m4n1_exp(1,2,0,4,5)\
  "vmovsldup 32(%0),%%ymm1; vmovshdup 32(%0),%%ymm2;" acc_m4n1_exp(1,2,0,6,7)\
  "addq $64,%0;"
#define KERNEL_k1m8n2 \
  "vbroadcastsd (%1),%%ymm0; vbroadcastsd 8(%1),%%ymm1; addq $16,%1;"\
  "vmovsldup (%0),%%ymm2; vmovshdup (%0),%%ymm3;" acc_m4n1_exp(2,3,0,4,5) acc_m4n1_exp(2,3,1,8,9)\
  "vmovsldup 32(%0),%%ymm2; vmovshdup 32(%0),%%ymm3;" acc_m4n1_exp(2,3,0,6,7) acc_m4n1_exp(2,3,1,10,11)\
  "addq $64,%0;"
/* contracted accumulators for m8n4 and m8n6 */
#define acc_m8n2_con(ua,la,luc,llc,ruc,rlc,lboff,rboff,...) \
  "vbroadcastss "#lboff"("#__VA_ARGS__"),%%ymm2;" acc_m8n1_con(ua,la,2,luc,llc)\
  "vbroadcastss "#rboff"("#__VA_ARGS__"),%%ymm3;" acc_m8n1_con(ua,la,3,ruc,rlc)
#define KERNEL_1_k1m8n4 \
  "vmovups (%0),%%ymm0; vmovups 32(%0),%%ymm1; prefetcht0 512(%0); addq $64,%0;"\
  acc_m8n2_con(0,1,4,5,6,7,0,8,%1) acc_m8n2_con(0,1,8,9,10,11,0,8,%1,%%r12,1)
#define KERNEL_2_k1m8n4 \
  "vpermilps $177,-64(%0),%%ymm0; vpermilps $177,-32(%0),%%ymm1;"\
  acc_m8n2_con(0,1,4,5,6,7,4,12,%1) acc_m8n2_con(0,1,8,9,10,11,4,12,%1,%%r12,1)
#define KERNEL_1_k1m8n6 KERNEL_1_k1m8n4 acc_m8n2_con(0,1,12,13,14,15,0,8,%1,%%r12,2)
#define KERNEL_2_k1m8n6 KERNEL_2_k1m8n4 acc_m8n2_con(0,1,12,13,14,15,4,12,%1,%%r12,2)
#define KERNEL_k1m8n4 KERNEL_1_k1m8n4 KERNEL_2_k1m8n4 "addq $16,%1;"
#define KERNEL_k1m8n6 KERNEL_1_k1m8n6 KERNEL_2_k1m8n6 "addq $16,%1;"
#define zero_4ymm(no1,no2,no3,no4) \
  "vpxor %%ymm"#no1",%%ymm"#no1",%%ymm"#no1"; vpxor %%ymm"#no2",%%ymm"#no2",%%ymm"#no2";"\
  "vpxor %%ymm"#no3",%%ymm"#no3",%%ymm"#no3"; vpxor %%ymm"#no4",%%ymm"#no4",%%ymm"#no4";"
/* initialization and storage macros */
#define INIT_m8n1 zero_4ymm(4,5,6,7)
#define INIT_m8n2 zero_4ymm(4,5,6,7) zero_4ymm(8,9,10,11)
#define INIT_m8n4 zero_4ymm(4,5,6,7) zero_4ymm(8,9,10,11)
#define INIT_m8n6 INIT_m8n4 zero_4ymm(12,13,14,15)
#if A_CONJ == B_CONJ
  #define cont_expacc(cl,cr,dst) "vpermilps $177,%%ymm"#cr",%%ymm"#cr"; vaddsubps %%ymm"#cl",%%ymm"#cr",%%ymm"#dst";"
#else
  #define cont_expacc(cl,cr,dst) "vpermilps $177,%%ymm"#cr",%%ymm"#cr"; vaddsubps %%ymm"#cr",%%ymm"#cl",%%ymm"#dst";"
#endif
#if A_CONJ == 0
  #define save_1ymm(c,tmp,off,alpr,alpi,...) \
    "vpermilps $177,%%ymm"#c",%%ymm"#tmp"; vfmsubadd213ps "#off"("#__VA_ARGS__"),%%ymm"#alpr",%%ymm"#c";"\
    "vfmsubadd231ps %%ymm"#tmp",%%ymm"#alpi",%%ymm"#c"; vmovups %%ymm"#c","#off"("#__VA_ARGS__");"
#else
  #define save_1ymm(c,tmp,off,alpr,alpi,...) \
    "vpermilps $177,%%ymm"#c",%%ymm"#tmp"; vfmaddsub213ps "#off"("#__VA_ARGS__"),%%ymm"#alpi",%%ymm"#tmp";"\
    "vfmaddsub231ps %%ymm"#c",%%ymm"#alpr",%%ymm"#tmp"; vmovups %%ymm"#tmp","#off"("#__VA_ARGS__");"
#endif
#define save_init_m8 "movq %2,%3; addq $64,%2; vbroadcastss (%6),%%ymm0; vbroadcastss 4(%6),%%ymm1;"
#define SAVE_m8n1 save_init_m8 cont_expacc(4,5,4) cont_expacc(6,7,6) save_1ymm(4,2,0,0,1,%3) save_1ymm(6,3,32,0,1,%3)
#define SAVE_m8n2 SAVE_m8n1\
  cont_expacc(8,9,8) cont_expacc(10,11,10) save_1ymm(8,2,0,0,1,%3,%4,1) save_1ymm(10,3,32,0,1,%3,%4,1)
#define SAVE_m8n4 save_init_m8\
  save_1ymm(4,2,0,0,1,%3) save_1ymm(5,3,32,0,1,%3) save_1ymm(6,2,0,0,1,%3,%4,1) save_1ymm(7,3,32,0,1,%3,%4,1) "leaq (%3,%4,2),%3;"\
  save_1ymm(8,2,0,0,1,%3) save_1ymm(9,3,32,0,1,%3) save_1ymm(10,2,0,0,1,%3,%4,1) save_1ymm(11,3,32,0,1,%3,%4,1)
#define SAVE_m8n6 SAVE_m8n4 "leaq (%3,%4,2),%3;"\
  save_1ymm(12,2,0,0,1,%3) save_1ymm(13,3,32,0,1,%3) save_1ymm(14,2,0,0,1,%3,%4,1) save_1ymm(15,3,32,0,1,%3,%4,1)
#define COMPUTE_m8(ndim) \
  "movq %%r14,%1;" INIT_m8n##ndim "movq %2,%3; movq %%r13,%5;"\
  "testq %5,%5; jz "#ndim"8883f; cmpq $10,%5; jb "#ndim"8882f;"\
  "movq $10,%5; movq $84,%%r15;"\
  #ndim"8881:\n\t"\
  "prefetcht1 (%3); subq $63,%3; addq %%r15,%3;"\
  KERNEL_k1m8n##ndim KERNEL_k1m8n##ndim\
  "testq $12,%5; movq $84,%%r15; cmovz %4,%%r15; prefetcht1 (%8); addq $16,%8;"\
  KERNEL_k1m8n##ndim KERNEL_k1m8n##ndim\
  "addq $4,%5; cmpq %5,%%r13; jnb "#ndim"8881b;"\
  "movq %2,%3; negq %5; leaq 10(%%r13,%5,1),%5; prefetcht0 (%6); prefetcht0 7(%6);"\
  #ndim"8882:\n\t"\
  "prefetcht0 (%3); prefetcht0 63(%3); addq %4,%3;"\
  KERNEL_k1m8n##ndim "decq %5; jnz "#ndim"8882b;"\
  #ndim"8883:\n\t"\
  "prefetcht0 (%%r14); prefetcht0 64(%%r14);" SAVE_m8n##ndim

/* m=4, ymm 0-3 temp, ymm 4-15 acc, expanded accumulators */
#define KERNEL_k1m4n1 \
  "vmovsldup (%0),%%ymm1; vmovshdup (%0),%%ymm2; addq $32,%0;"\
  "vbroadcastsd (%1),%%ymm0;" acc_m4n1_exp(1,2,0,4,5) "addq $8,%1;"
#define acc_m4n2_exp(c1l,c1r,c2l,c2r,...) \
  "vbroadcastsd ("#__VA_ARGS__"),%%ymm2;" acc_m4n1_exp(0,1,2,c1l,c1r)\
  "vbroadcastsd 8("#__VA_ARGS__"),%%ymm3;" acc_m4n1_exp(0,1,3,c2l,c2r)
#define KERNEL_h_k1m4n2 \
  "vmovsldup (%0),%%ymm0; vmovshdup (%0),%%ymm1; addq $32,%0;" acc_m4n2_exp(4,5,6,7,%1)
#define KERNEL_h_k1m4n4 KERNEL_h_k1m4n2 acc_m4n2_exp(8,9,10,11,%1,%%r12,1)
#define KERNEL_h_k1m4n6 KERNEL_h_k1m4n4 acc_m4n2_exp(12,13,14,15,%1,%%r12,2)
#define KERNEL_k1m4n2 KERNEL_h_k1m4n2 "addq $16,%1;"
#define KERNEL_k1m4n4 KERNEL_h_k1m4n4 "addq $16,%1;"
#define KERNEL_k1m4n6 KERNEL_h_k1m4n6 "addq $16,%1;"
#define INIT_m4n1 "vpxor %%ymm4,%%ymm4,%%ymm4; vpxor %%ymm5,%%ymm5,%%ymm5;"
#define INIT_m4n2 zero_4ymm(4,5,6,7)
#define INIT_m4n4 INIT_m4n2 zero_4ymm(8,9,10,11)
#define INIT_m4n6 INIT_m4n4 zero_4ymm(12,13,14,15)
#define save_init_m4 "movq %2,%3; addq $32,%2; vbroadcastss (%6),%%ymm0; vbroadcastss 4(%6),%%ymm1;"
#define SAVE_m4n1 save_init_m4 cont_expacc(4,5,4) save_1ymm(4,2,0,0,1,%3)
#define SAVE_m4n2 SAVE_m4n1 cont_expacc(6,7,6) save_1ymm(6,3,0,0,1,%3,%4,1)
#define SAVE_m4n4 SAVE_m4n2 "leaq (%3,%4,2),%3;"\
  cont_expacc(8,9,8) cont_expacc(10,11,10) save_1ymm(8,2,0,0,1,%3) save_1ymm(10,3,0,0,1,%3,%4,1)
#define SAVE_m4n6 SAVE_m4n4 "leaq (%3,%4,2),%3;"\
  cont_expacc(12,13,12) cont_expacc(14,15,14) save_1ymm(12,2,0,0,1,%3) save_1ymm(14,3,0,0,1,%3,%4,1)
#define COMPUTE_m4(ndim) \
  "movq %%r14,%1;" INIT_m4n##ndim "movq %%r13,%5;"\
  "testq %5,%5; jz "#ndim"4442f;"\
  #ndim"4441:\n\t"\
  KERNEL_k1m4n##ndim\
  "decq %5; jnz "#ndim"4441b;"\
  #ndim"4442:\n\t"\
  SAVE_m4n##ndim

/* m=2, xmm 0-3 temp, xmm 4-15 acc, expanded accumulators */
#if A_CONJ == B_CONJ
  #define acc_m2n1_exp(ar,ai,b2,cl,cr) "vfmadd231ps %%xmm"#ar",%%xmm"#b2",%%xmm"#cl"; vfmadd231ps %%xmm"#ai",%%xmm"#b2",%%xmm"#cr";"
#else
  #define acc_m2n1_exp(ar,ai,b2,cl,cr) "vfmadd231ps %%xmm"#ar",%%xmm"#b2",%%xmm"#cl"; vfnmadd231ps %%xmm"#ai",%%xmm"#b2",%%xmm"#cr";"
#endif
#define KERNEL_h_k1m2n1 \
  "vmovsldup (%0),%%xmm0; vmovshdup (%0),%%xmm1; addq $16,%0;"\
  "vmovddup (%1),%%xmm2;" acc_m2n1_exp(0,1,2,4,5)
#define KERNEL_h_k1m2n2 KERNEL_h_k1m2n1\
  "vmovddup 8(%1),%%xmm3;" acc_m2n1_exp(0,1,3,6,7)
#define acc_m2n2_exp(c1,c2,c3,c4,...)\
  "vmovddup ("#__VA_ARGS__"),%%xmm2;" acc_m2n1_exp(0,1,2,c1,c2)\
  "vmovddup 8("#__VA_ARGS__"),%%xmm3;" acc_m2n1_exp(0,1,3,c3,c4)
#define KERNEL_h_k1m2n4 KERNEL_h_k1m2n2 acc_m2n2_exp(8,9,10,11,%1,%%r12,1)
#define KERNEL_h_k1m2n6 KERNEL_h_k1m2n4 acc_m2n2_exp(12,13,14,15,%1,%%r12,2)
#define KERNEL_k1m2n1 KERNEL_h_k1m2n1 "addq $8,%1;"
#define KERNEL_k1m2n2 KERNEL_h_k1m2n2 "addq $16,%1;"
#define KERNEL_k1m2n4 KERNEL_h_k1m2n4 "addq $16,%1;"
#define KERNEL_k1m2n6 KERNEL_h_k1m2n6 "addq $16,%1;"
#define zero_2xmm(no1,no2) "vpxor %%xmm"#no1",%%xmm"#no1",%%xmm"#no1"; vpxor %%xmm"#no2",%%xmm"#no2",%%xmm"#no2";"
#define INIT_m2n1 zero_2xmm(4,5)
#define INIT_m2n2 INIT_m2n1 zero_2xmm(6,7)
#define INIT_m2n4 INIT_m2n2 zero_2xmm(8,9) zero_2xmm(10,11)
#define INIT_m2n6 INIT_m2n4 zero_2xmm(12,13) zero_2xmm(14,15)
#if A_CONJ == B_CONJ
  #define cont_expxmmacc(cl,cr,dst) "vpermilps $177,%%xmm"#cr",%%xmm"#cr"; vaddsubps %%xmm"#cl",%%xmm"#cr",%%xmm"#dst";"
#else
  #define cont_expxmmacc(cl,cr,dst) "vpermilps $177,%%xmm"#cr",%%xmm"#cr"; vaddsubps %%xmm"#cr",%%xmm"#cl",%%xmm"#dst";"
#endif
#if A_CONJ == 0
  #define save_1xmm(c,tmp,alpr,alpi) \
    "vpermilps $177,%%xmm"#c",%%xmm"#tmp"; vfmsubadd213ps (%3),%%xmm"#alpr",%%xmm"#c";"\
    "vfmsubadd231ps %%xmm"#tmp",%%xmm"#alpi",%%xmm"#c"; vmovups %%xmm"#c",(%3); addq %4,%3;"
#else
  #define save_1xmm(c,tmp,alpr,alpi) \
    "vpermilps $177,%%xmm"#c",%%xmm"#tmp"; vfmaddsub213ps (%3),%%xmm"#alpi",%%xmm"#tmp";"\
    "vfmaddsub231ps %%xmm"#c",%%xmm"#alpr",%%xmm"#tmp"; vmovups %%xmm"#tmp",(%3); addq %4,%3;"
#endif
#define save_init_m2 "movq %2,%3; addq $16,%2; vbroadcastss (%6),%%xmm0; vbroadcastss 4(%6),%%xmm1;"
#define SAVE_m2n1 save_init_m2 cont_expxmmacc(4,5,4) save_1xmm(4,2,0,1)
#define SAVE_m2n2 SAVE_m2n1 cont_expacc(6,7,6) save_1xmm(6,3,0,1)
#define SAVE_m2n4 SAVE_m2n2 cont_expacc(8,9,8) save_1xmm(8,2,0,1) cont_expacc(10,11,10) save_1xmm(10,3,0,1)
#define SAVE_m2n6 SAVE_m2n4 cont_expacc(12,13,12) save_1xmm(12,2,0,1) cont_expacc(14,15,14) save_1xmm(14,3,0,1)
#define COMPUTE_m2(ndim) \
  "movq %%r14,%1;" INIT_m2n##ndim "movq %%r13,%5;"\
  "testq %5,%5; jz "#ndim"2222f;"\
  #ndim"2221:\n\t"\
  KERNEL_k1m2n##ndim\
  "decq %5; jnz "#ndim"2221b;"\
  #ndim"2222:\n\t"\
  SAVE_m2n##ndim

/* m=1, xmm 0-3 temp, xmm 4-9 acc, expanded accumulators */
#if A_CONJ == B_CONJ
  #define acc_m1n1_exp(ar,ai,b2,cl,cr) "vfmadd231ps %%xmm"#ar",%%xmm"#b2",%%xmm"#cl"; vfmadd231ps %%xmm"#ai",%%xmm"#b2",%%xmm"#cr";"
  #define acc_m1n2_exp(arb,aib,b4,cl,cr) "vfmadd231ps %%xmm"#arb",%%xmm"#b4",%%xmm"#cl"; vfmadd231ps %%xmm"#aib",%%xmm"#b4",%%xmm"#cr";"
#else
  #define acc_m1n1_exp(ar,ai,b2,cl,cr) "vfmadd231ps %%xmm"#ar",%%xmm"#b2",%%xmm"#cl"; vfnmadd231ps %%xmm"#ai",%%xmm"#b2",%%xmm"#cr";"
  #define acc_m1n2_exp(arb,aib,b4,cl,cr) "vfmadd231ps %%xmm"#arb",%%xmm"#b4",%%xmm"#cl"; vfnmadd231ps %%xmm"#aib",%%xmm"#b4",%%xmm"#cr";"
#endif
#define KERNEL_k1m1n1 \
  "vbroadcastss (%0),%%xmm0; vbroadcastss 4(%0),%%xmm1; addq $8,%0;"\
  "vmovsd (%1),%%xmm2; addq $8,%1;" acc_m1n1_exp(0,1,2,4,5)
#define KERNEL_h_k1m1n2 \
  "vbroadcastss (%0),%%xmm0; vbroadcastss 4(%0),%%xmm1; addq $8,%0;"\
  "vmovups (%1),%%xmm2;" acc_m1n2_exp(0,1,2,4,5)
#define KERNEL_h_k1m1n4 KERNEL_h_k1m1n2 "vmovups (%1,%%r12,1),%%xmm2;" acc_m1n2_exp(0,1,2,6,7)
#define KERNEL_h_k1m1n6 KERNEL_h_k1m1n4 "vmovups (%1,%%r12,2),%%xmm2;" acc_m1n2_exp(0,1,2,8,9)
#define KERNEL_k1m1n2 KERNEL_h_k1m1n2 "addq $16,%1;"
#define KERNEL_k1m1n4 KERNEL_h_k1m1n4 "addq $16,%1;"
#define KERNEL_k1m1n6 KERNEL_h_k1m1n6 "addq $16,%1;"
#define INIT_m1n1 zero_2xmm(4,5)
#define INIT_m1n2 zero_2xmm(4,5)
#define INIT_m1n4 INIT_m1n2 zero_2xmm(6,7)
#define INIT_m1n6 INIT_m1n4 zero_2xmm(8,9)
#if A_CONJ == 0
  #define save_m1n1(c,tmp1,tmp2,alpr,alpi) \
    "vpermilps $177,%%xmm"#c",%%xmm"#tmp1"; vmovsd (%3),%%xmm"#tmp2"; vfmsubadd213ps %%xmm"#tmp2",%%xmm"#alpr",%%xmm"#c";"\
    "vfmsubadd231ps %%xmm"#tmp1",%%xmm"#alpi",%%xmm"#c"; vmovsd %%xmm"#c",(%3);"
  #define save_m1n2(c,tmp1,tmp2,alpr,alpi) \
    "vpermilps $177,%%xmm"#c",%%xmm"#tmp1"; vmovsd (%3),%%xmm"#tmp2"; vmovhpd (%3,%4,1),%%xmm"#tmp2",%%xmm"#tmp2";"\
    "vfmsubadd213ps %%xmm"#tmp2",%%xmm"#alpr",%%xmm"#c"; vfmsubadd231ps %%xmm"#tmp1",%%xmm"#alpi",%%xmm"#c";"\
    "vmovsd %%xmm"#c",(%3); vmovhpd %%xmm"#c",(%3,%4,1); leaq (%3,%4,2),%3;"
#else
  #define save_m1n1(c,tmp1,tmp2,alpr,alpi) \
    "vpermilps $177,%%xmm"#c",%%xmm"#tmp1"; vmovsd (%3),%%xmm"#tmp2"; vfmaddsub213ps %%xmm"#tmp2",%%xmm"#alpi",%%xmm"#tmp1";"\
    "vfmaddsub231ps %%xmm"#c",%%xmm"#alpr",%%xmm"#tmp1"; vmovsd %%xmm"#tmp1",(%3);"
  #define save_m1n2(c,tmp1,tmp2,alpr,alpi) \
    "vpermilps $177,%%xmm"#c",%%xmm"#tmp1"; vmovsd (%3),%%xmm"#tmp2"; vmovhpd (%3,%4,1),%%xmm"#tmp2",%%xmm"#tmp2";"\
    "vfmaddsub213ps %%xmm"#tmp2",%%xmm"#alpi",%%xmm"#tmp1"; vfmaddsub231ps %%xmm"#c",%%xmm"#alpr",%%xmm"#tmp1";"\
    "vmovsd %%xmm"#tmp1",(%3); vmovhpd %%xmm"#tmp1",(%3,%4,1); leaq (%3,%4,2),%3;"
#endif
#define save_init_m1 "movq %2,%3; addq $8,%2; vbroadcastss (%6),%%xmm0; vbroadcastss 4(%6),%%xmm1;"
#define SAVE_m1n1 save_init_m1 cont_expxmmacc(4,5,4) save_m1n1(4,2,3,0,1)
#define SAVE_m1n2 save_init_m1 cont_expxmmacc(4,5,4) save_m1n2(4,2,3,0,1)
#define SAVE_m1n4 SAVE_m1n2 cont_expxmmacc(6,7,6) save_m1n2(6,2,3,0,1)
#define SAVE_m1n6 SAVE_m1n4 cont_expxmmacc(8,9,8) save_m1n2(8,2,3,0,1)
#define COMPUTE_m1(ndim) \
  "movq %%r14,%1;" INIT_m1n##ndim "movq %%r13,%5;"\
  "testq %5,%5; jz "#ndim"1112f;"\
  #ndim"1111:\n\t"\
  KERNEL_k1m1n##ndim\
  "decq %5; jnz "#ndim"1111b;"\
  #ndim"1112:\n\t"\
  SAVE_m1n##ndim

#define COMPUTE(ndim) {\
  b_pref = b_ptr + ndim * K *2;\
  __asm__ __volatile__ (\
    "movq %1,%%r14; movq %5,%%r13; movq %5,%%r12; salq $4,%%r12; movq %7,%%r11;"\
    "cmpq $8,%7; jb "#ndim"9992f;"\
    #ndim"9991:\n\t"\
    COMPUTE_m8(ndim)\
    "subq $8,%7; cmpq $8,%7; jnb "#ndim"9991b;"\
    #ndim"9992:\n\t"\
    "cmpq $4,%7; jb "#ndim"9993f;"\
    COMPUTE_m4(ndim) "subq $4,%7;"\
    #ndim"9993:\n\t"\
    "cmpq $2,%7; jb "#ndim"9994f;"\
    COMPUTE_m2(ndim) "subq $2,%7;"\
    #ndim"9994:\n\t"\
    "testq %7,%7; jz "#ndim"9995f;"\
    COMPUTE_m1(ndim)\
    #ndim"9995:\n\t"\
    "movq %%r14,%1; movq %%r13,%5; movq %%r11,%7; vzeroupper;"\
    :"+r"(a_ptr),"+r"(b_ptr),"+r"(c_ptr),"+r"(c_tmp),"+r"(ldc_in_bytes),"+r"(K),"+r"(alp),"+r"(M),"+r"(b_pref)\
    ::"cc","memory","r11","r12","r13","r14","r15","xmm0","xmm1","xmm2","xmm3","xmm4","xmm5",\
    "xmm6","xmm7","xmm8","xmm9","xmm10","xmm11","xmm12","xmm13","xmm14","xmm15");\
  a_ptr -= M * K *2; b_ptr += ndim * K *2; c_ptr += (ndim * LDC - M) * 2;\
}

int __attribute__ ((noinline))
CNAME(BLASLONG m, BLASLONG n, BLASLONG k, float alphar, float alphai, float * __restrict__ A, float * __restrict__ B, float * __restrict__ C, BLASLONG LDC)
{
    if(m==0||n==0||k==0||(alphar==0.0 && alphai==0.0)) return 0;
    int64_t ldc_in_bytes = (int64_t)LDC * sizeof(float) * 2;
#if A_CONJ == B_CONJ
    float const_val[2] = {-alphar, -alphai};
#else
    float const_val[2] = {alphar, alphai};
#endif
    int64_t M = (int64_t)m, K = (int64_t)k;
    BLASLONG n_count = n;
    float *a_ptr = A,*b_ptr = B,*c_ptr = C,*c_tmp = C,*alp = const_val,*b_pref = B;
    for(;n_count>5;n_count-=6) COMPUTE(6)
    for(;n_count>3;n_count-=4) COMPUTE(4)
    for(;n_count>1;n_count-=2) COMPUTE(2)
    if(n_count>0) COMPUTE(1)
    return 0;
}
