/* %0 = "+r"(a_pointer), %1 = "+r"(b_pointer), %2 = "+r"(c_pointer), %3 = "+r"(ldc_in_bytes), %4 for k_count, %5 for c_store */
/* r12 = k << 5(const), r13 = k(const), r14 = b_head_pos(const), r15 = tmp */

#include "common.h"
#include <stdint.h>

//recommended settings: GEMM_Q=256, GEMM_P=256

/* m = 4 *//* ymm0 for alpha, ymm1-ymm3 for temporary use, ymm4-ymm15 for accumulators */
#define KERNEL_k1m4n1 \
    "vmovupd (%0),%%ymm1; addq $32,%0;"\
    "vbroadcastsd (%1),%%ymm2; vfmadd231pd %%ymm1,%%ymm2,%%ymm4;"\
    "addq $8,%1;"
#define KERNEL_h_k1m4n2 \
    "vmovddup (%0),%%ymm1; vmovddup 8(%0),%%ymm2; addq $32,%0;"\
    "vbroadcastf128 (%1),%%ymm3; vfmadd231pd %%ymm1,%%ymm3,%%ymm4; vfmadd231pd %%ymm2,%%ymm3,%%ymm5;"
#define KERNEL_k1m4n2 KERNEL_h_k1m4n2 "addq $16,%1;"
#define KERNEL_h_k1m4n4 \
    KERNEL_h_k1m4n2 "vbroadcastf128 16(%1),%%ymm3; vfmadd231pd %%ymm1,%%ymm3,%%ymm6; vfmadd231pd %%ymm2,%%ymm3,%%ymm7;"
#define KERNEL_k1m4n4 KERNEL_h_k1m4n4 "addq $32,%1;"
#define unit_kernel_k1m4n4(c1,c2,c3,c4,off1,off2,...) \
    "vbroadcastf128 "#off1"("#__VA_ARGS__"),%%ymm3; vfmadd231pd %%ymm1,%%ymm3,"#c1"; vfmadd231pd %%ymm2,%%ymm3,"#c2";"\
    "vbroadcastf128 "#off2"("#__VA_ARGS__"),%%ymm3; vfmadd231pd %%ymm1,%%ymm3,"#c3"; vfmadd231pd %%ymm2,%%ymm3,"#c4";"
#define KERNEL_h_k1m4n8 KERNEL_h_k1m4n4 unit_kernel_k1m4n4(%%ymm8,%%ymm9,%%ymm10,%%ymm11,0,16,%1,%%r12,1)
#define KERNEL_k1m4n8 KERNEL_h_k1m4n8 "addq $32,%1;"
#define KERNEL_h_k1m4n12 KERNEL_h_k1m4n8 unit_kernel_k1m4n4(%%ymm12,%%ymm13,%%ymm14,%%ymm15,0,16,%1,%%r12,2)
#define KERNEL_k1m4n12 KERNEL_h_k1m4n12 "addq $32,%1;"
#define KERNEL_k2m4n1 KERNEL_k1m4n1 KERNEL_k1m4n1
#define KERNEL_k2m4n2 KERNEL_k1m4n2 KERNEL_k1m4n2
#define KERNEL_k2m4n4 KERNEL_k1m4n4 KERNEL_k1m4n4
#define KERNEL_k2m4n8 KERNEL_k1m4n8 KERNEL_k1m4n8
#define KERNEL_k2m4n12 \
    "vmovddup (%0),%%ymm1; vmovddup 8(%0),%%ymm2;"\
    unit_kernel_k1m4n4(%%ymm4,%%ymm5,%%ymm6,%%ymm7,0,16,%1)\
    unit_kernel_k1m4n4(%%ymm8,%%ymm9,%%ymm10,%%ymm11,0,16,%1,%%r12,1)\
    unit_kernel_k1m4n4(%%ymm12,%%ymm13,%%ymm14,%%ymm15,0,16,%1,%%r12,2)\
    "vmovddup 32(%0),%%ymm1; vmovddup 40(%0),%%ymm2; prefetcht0 512(%0); addq $64,%0;"\
    unit_kernel_k1m4n4(%%ymm4,%%ymm5,%%ymm6,%%ymm7,32,48,%1)\
    unit_kernel_k1m4n4(%%ymm8,%%ymm9,%%ymm10,%%ymm11,32,48,%1,%%r12,1)\
    unit_kernel_k1m4n4(%%ymm12,%%ymm13,%%ymm14,%%ymm15,32,48,%1,%%r12,2) "addq $64,%1;"
#define INIT_m4n1 "vpxor %%ymm4,%%ymm4,%%ymm4;"
#define INIT_m4n2 INIT_m4n1 "vpxor %%ymm5,%%ymm5,%%ymm5;"
#define INIT_m4n4 INIT_m4n2 "vpxor %%ymm6,%%ymm6,%%ymm6;vpxor %%ymm7,%%ymm7,%%ymm7;"
#define unit_init_m4n4(c1,c2,c3,c4) \
    "vpxor "#c1","#c1","#c1";vpxor "#c2","#c2","#c2";vpxor "#c3","#c3","#c3";vpxor "#c4","#c4","#c4";"
#define INIT_m4n8  INIT_m4n4 unit_init_m4n4(%%ymm8,%%ymm9,%%ymm10,%%ymm11)
#define INIT_m4n12 INIT_m4n8 unit_init_m4n4(%%ymm12,%%ymm13,%%ymm14,%%ymm15)
#define SAVE_h_m4n1 \
    "vpermpd $216,%%ymm4,%%ymm3; vunpcklpd %%ymm3,%%ymm3,%%ymm1; vunpckhpd %%ymm3,%%ymm3,%%ymm2;"\
    "vfmadd213pd (%2),%%ymm0,%%ymm1; vfmadd213pd 32(%2),%%ymm0,%%ymm2; vmovupd %%ymm1,(%2); vmovupd %%ymm2,32(%2);"
#define unit_save_m4n2(c1,c2) \
    "vperm2f128 $2,"#c1","#c2",%%ymm2; vperm2f128 $19,"#c1","#c2","#c2"; vmovapd %%ymm2,"#c1";"\
    "vunpcklpd "#c1","#c1",%%ymm2; vunpcklpd "#c2","#c2",%%ymm3;"\
    "vfmadd213pd (%5),%%ymm0,%%ymm2; vfmadd213pd 32(%5),%%ymm0,%%ymm3; vmovupd %%ymm2,(%5); vmovupd %%ymm3,32(%5);"\
    "vunpckhpd "#c1","#c1",%%ymm2; vunpckhpd "#c2","#c2",%%ymm3;"\
    "vfmadd213pd (%5,%3,1),%%ymm0,%%ymm2; vfmadd213pd 32(%5,%3,1),%%ymm0,%%ymm3; vmovupd %%ymm2,(%5,%3,1); vmovupd %%ymm3,32(%5,%3,1);"\
    "leaq (%5,%3,2),%5;"
#define SAVE_h_m4n2 "movq %2,%5;" unit_save_m4n2(%%ymm4,%%ymm5)
#define SAVE_h_m4n4  SAVE_h_m4n2  unit_save_m4n2(%%ymm6,%%ymm7)
#define SAVE_h_m4n8  SAVE_h_m4n4  unit_save_m4n2(%%ymm8,%%ymm9)   unit_save_m4n2(%%ymm10,%%ymm11)
#define SAVE_h_m4n12 SAVE_h_m4n8  unit_save_m4n2(%%ymm12,%%ymm13) unit_save_m4n2(%%ymm14,%%ymm15)
#define SAVE_m4(ndim) SAVE_h_m4n##ndim "addq $64,%2;"
#define COMPUTE_m4(ndim) \
    INIT_m4n##ndim\
    "movq %%r13,%4; movq %%r14,%1; movq %2,%5; xorq %%r15,%%r15;"\
    "cmpq $24,%4; jb "#ndim"004042f;"\
    #ndim"004041:\n\t"\
    "cmpq $126,%%r15; movq $126,%%r15; cmoveq %3,%%r15;"\
    KERNEL_k2m4n##ndim KERNEL_k2m4n##ndim\
    "prefetcht1 (%5); subq $63,%5;"\
    KERNEL_k2m4n##ndim KERNEL_k2m4n##ndim\
    "addq %%r15,%5; prefetcht1 (%8); addq $32,%8;"\
    "subq $8,%4; cmpq $16,%4; jnb "#ndim"004041b;"\
    "movq %2,%5;"\
    #ndim"004042:\n\t"\
    "testq %4,%4; jz "#ndim"004043f;"\
    "prefetcht0 (%5); prefetcht0 63(%5);"\
    KERNEL_k1m4n##ndim\
    "prefetcht0 (%5,%3,4); prefetcht0 63(%5,%3,4); addq %3,%5;"\
    "decq %4; jmp "#ndim"004042b;"\
    #ndim"004043:\n\t"\
    "prefetcht0 (%%r14); prefetcht0 64(%%r14);"\
    SAVE_m4(ndim)

/* m = 2 *//* vmm0 for alpha, vmm1-vmm3 for temporary use, vmm4-vmm9 for accumulators */
#define KERNEL_k1m2n1 \
    "vmovupd (%0),%%xmm1; addq $16,%0;"\
    "vmovddup (%1),%%xmm2; vfmadd231pd %%xmm1,%%xmm2,%%xmm4;"\
    "addq $8,%1;"
#define KERNEL_h_k1m2n2 \
    "vmovddup (%0),%%xmm1; vmovddup 8(%0),%%xmm2; addq $16,%0;"\
    "vmovupd (%1),%%xmm3; vfmadd231pd %%xmm1,%%xmm3,%%xmm4; vfmadd231pd %%xmm2,%%xmm3,%%xmm5;"
#define KERNEL_k1m2n2 KERNEL_h_k1m2n2 "addq $16,%1;"
#define unit_kernel_k1m2n4(c1,c2,...) \
    "vmovupd ("#__VA_ARGS__"),%%ymm3; vfmadd231pd %%ymm1,%%ymm3,"#c1"; vfmadd231pd %%ymm2,%%ymm3,"#c2";"
#define KERNEL_h_k1m2n4 \
    "vbroadcastsd (%0),%%ymm1; vbroadcastsd 8(%0),%%ymm2; addq $16,%0;"\
    unit_kernel_k1m2n4(%%ymm4,%%ymm5,%1)
#define KERNEL_k1m2n4 KERNEL_h_k1m2n4 "addq $32,%1;"
#define KERNEL_h_k1m2n8 KERNEL_h_k1m2n4 \
    unit_kernel_k1m2n4(%%ymm6,%%ymm7,%1,%%r12,1)
#define KERNEL_k1m2n8 KERNEL_h_k1m2n8 "addq $32,%1;"
#define KERNEL_h_k1m2n12 KERNEL_h_k1m2n8 \
    unit_kernel_k1m2n4(%%ymm8,%%ymm9,%1,%%r12,2)
#define KERNEL_k1m2n12 KERNEL_h_k1m2n12 "addq $32,%1;"
#define INIT_m2n1 "vpxor %%xmm4,%%xmm4,%%xmm4;"
#define INIT_m2n2 INIT_m2n1 "vpxor %%xmm5,%%xmm5,%%xmm5;"
#define unit_init_m2n4(c1,c2) "vpxor "#c1","#c1","#c1";vpxor "#c2","#c2","#c2";"
#define INIT_m2n4 unit_init_m2n4(%%ymm4,%%ymm5)
#define INIT_m2n8 INIT_m2n4 unit_init_m2n4(%%ymm6,%%ymm7)
#define INIT_m2n12 INIT_m2n8 unit_init_m2n4(%%ymm8,%%ymm9)
#define SAVE_h_m2n1 \
    "vinsertf128 $1,%%xmm4,%%ymm4,%%ymm4; vpermilpd $12,%%ymm4,%%ymm4; vfmadd213pd (%2),%%ymm0,%%ymm4; vmovupd %%ymm4,(%2);"
#define SAVE_h_m2n2 \
    "vinsertf128 $1,%%xmm5,%%ymm4,%%ymm4; vunpcklpd %%ymm4,%%ymm4,%%ymm1; vunpckhpd %%ymm4,%%ymm4,%%ymm2;"\
    "vfmadd213pd (%2),%%ymm0,%%ymm1; vmovupd %%ymm1,(%2);"\
    "vfmadd213pd (%2,%3,1),%%ymm0,%%ymm2; vmovupd %%ymm2,(%2,%3,1);"
#define unit_save_m2n4(c1,c2) \
    "vperm2f128 $2,"#c1","#c2",%%ymm1; vunpcklpd %%ymm1,%%ymm1,%%ymm2; vunpckhpd %%ymm1,%%ymm1,%%ymm3;"\
    "vfmadd213pd (%5),%%ymm0,%%ymm2; vfmadd213pd (%5,%3,1),%%ymm0,%%ymm3; vmovupd %%ymm2,(%5); vmovupd %%ymm3,(%5,%3,1); leaq (%5,%3,2),%5;"\
    "vperm2f128 $19,"#c1","#c2",%%ymm1; vunpcklpd %%ymm1,%%ymm1,%%ymm2; vunpckhpd %%ymm1,%%ymm1,%%ymm3;"\
    "vfmadd213pd (%5),%%ymm0,%%ymm2; vfmadd213pd (%5,%3,1),%%ymm0,%%ymm3; vmovupd %%ymm2,(%5); vmovupd %%ymm3,(%5,%3,1); leaq (%5,%3,2),%5;"
#define SAVE_h_m2n4 "movq %2,%5;" unit_save_m2n4(%%ymm4,%%ymm5)
#define SAVE_h_m2n8 SAVE_h_m2n4 unit_save_m2n4(%%ymm6,%%ymm7)
#define SAVE_h_m2n12 SAVE_h_m2n8 unit_save_m2n4(%%ymm8,%%ymm9)
#define SAVE_m2(ndim) SAVE_h_m2n##ndim "addq $32,%2;"
#define COMPUTE_m2(ndim) \
    INIT_m2n##ndim\
    "movq %%r13,%4; movq %%r14,%1;"\
    #ndim"002022:\n\t"\
    "testq %4,%4; jz "#ndim"002023f;"\
    KERNEL_k1m2n##ndim\
    "decq %4; jmp "#ndim"002022b;"\
    #ndim"002023:\n\t"\
    SAVE_m2(ndim)

/* m = 1 *//* vmm0 for alpha, vmm1-vmm3 and vmm10-vmm15 for temporary use, vmm4-vmm6 for accumulators */
#define KERNEL_k1m1n1 \
    "vmovsd (%0),%%xmm1; addq $8,%0;"\
    "vfmadd231sd (%1),%%xmm1,%%xmm4; addq $8,%1;"
#define KERNEL_k1m1n2 \
    "vmovddup (%0),%%xmm1; addq $8,%0;"\
    "vfmadd231pd (%1),%%xmm1,%%xmm4; addq $16,%1;"
#define unit_kernel_k1m1n4(c1,...) \
    "vmovupd ("#__VA_ARGS__"),%%ymm2; vfmadd231pd %%ymm1,%%ymm2,"#c1";"
#define KERNEL_h_k1m1n4 \
    "vbroadcastsd (%0),%%ymm1; addq $8,%0;"\
    unit_kernel_k1m1n4(%%ymm4,%1)
#define KERNEL_k1m1n4 KERNEL_h_k1m1n4 "addq $32,%1;"
#define KERNEL_h_k1m1n8 KERNEL_h_k1m1n4 unit_kernel_k1m1n4(%%ymm5,%1,%%r12,1)
#define KERNEL_k1m1n8 KERNEL_h_k1m1n8 "addq $32,%1;"
#define KERNEL_h_k1m1n12 KERNEL_h_k1m1n8 unit_kernel_k1m1n4(%%ymm6,%1,%%r12,2)
#define KERNEL_k1m1n12 KERNEL_h_k1m1n12 "addq $32,%1;"
#define INIT_m1n1 INIT_m2n1
#define INIT_m1n2 INIT_m2n1
#define INIT_m1n4 "vpxor %%ymm4,%%ymm4,%%ymm4;"
#define INIT_m1n8 INIT_m1n4 "vpxor %%ymm5,%%ymm5,%%ymm5;"
#define INIT_m1n12 INIT_m1n8 "vpxor %%ymm6,%%ymm6,%%ymm6;"
#define SAVE_h_m1n1 \
    "vmovddup %%xmm4,%%xmm4; vfmadd213pd (%2),%%xmm0,%%xmm4; vmovupd %%xmm4,(%2);"
#define SAVE_h_m1n2 \
    "vunpcklpd %%xmm4,%%xmm4,%%xmm1; vunpckhpd %%xmm4,%%xmm4,%%xmm2;"\
    "vfmadd213pd (%2),%%xmm0,%%xmm1; vmovupd %%xmm1,(%2);"\
    "vfmadd213pd (%2,%3,1),%%xmm0,%%xmm2; vmovupd %%xmm2,(%2,%3,1);"
#define unit_save_m1n4(c1) \
    "vunpcklpd "#c1","#c1",%%ymm1; vunpckhpd "#c1","#c1",%%ymm2;"\
    "vmovupd (%5),%%xmm3; vinsertf128 $1,(%5,%3,2),%%ymm3,%%ymm3;"\
    "vfmadd213pd %%ymm3,%%ymm0,%%ymm1; vmovupd %%xmm1,(%5); vextractf128 $1,%%ymm1,(%5,%3,2); addq %3,%5;"\
    "vmovupd (%5),%%xmm3; vinsertf128 $1,(%5,%3,2),%%ymm3,%%ymm3;"\
    "vfmadd213pd %%ymm3,%%ymm0,%%ymm2; vmovupd %%xmm2,(%5); vextractf128 $1,%%ymm2,(%5,%3,2); addq %3,%5; leaq (%5,%3,2),%5;"
#define SAVE_h_m1n4 "movq %2,%5;" unit_save_m1n4(%%ymm4)
#define SAVE_h_m1n8 SAVE_h_m1n4 unit_save_m1n4(%%ymm5)
#define SAVE_h_m1n12 SAVE_h_m1n8 unit_save_m1n4(%%ymm6)
#define SAVE_m1(ndim) SAVE_h_m1n##ndim "addq $16,%2;"
#define COMPUTE_m1(ndim) \
    INIT_m1n##ndim\
    "movq %%r13,%4; movq %%r14,%1;"\
    #ndim"001011:\n\t"\
    "testq %4,%4; jz "#ndim"001012f;"\
    KERNEL_k1m1n##ndim\
    "decq %4; jmp "#ndim"001011b;"\
    #ndim"001012:\n\t"\
    SAVE_m1(ndim)

#define COMPUTE(ndim) {\
    next_b = b_pointer + ndim * K;\
    __asm__ __volatile__(\
    "vbroadcastf128 (%6),%%ymm0;"\
    "movq %4,%%r13; movq %4,%%r12; salq $5,%%r12; movq %1,%%r14; movq %7,%%r11;"\
    "cmpq $4,%7;jb 33101"#ndim"f;"\
    "33109"#ndim":\n\t"\
    COMPUTE_m4(ndim)\
    "subq $4,%7;cmpq $4,%7;jnb 33109"#ndim"b;"\
    "33101"#ndim":\n\t"\
    "cmpq $2,%7;jb 33104"#ndim"f;"\
    COMPUTE_m2(ndim)\
    "subq $2,%7;"\
    "33104"#ndim":\n\t"\
    "testq %7,%7;jz 33105"#ndim"f;"\
    COMPUTE_m1(ndim)\
    "33105"#ndim":\n\t"\
    "movq %%r13,%4; movq %%r14,%1; movq %%r11,%7;"\
    :"+r"(a_pointer),"+r"(b_pointer),"+r"(c_pointer),"+r"(ldc_in_bytes),"+r"(K),"+r"(ctemp),"+r"(const_val),"+r"(M),"+r"(next_b)\
    ::"r11","r12","r13","r14","r15","xmm0","xmm1","xmm2","xmm3","xmm4","xmm5","xmm6","xmm7","xmm8","xmm9","xmm10","xmm11","xmm12","xmm13","xmm14",\
    "xmm15","cc","memory");\
    a_pointer -= M * K; b_pointer += ndim * K; c_pointer += 2*(LDC * ndim - M);\
}
int __attribute__ ((noinline))
CNAME(BLASLONG m, BLASLONG n, BLASLONG k, double alphar, double alphai, double * __restrict__ A, double * __restrict__ B, double * __restrict__ C, BLASLONG LDC)
{
    if(m==0||n==0||k==0) return 0;
    int64_t ldc_in_bytes = (int64_t)LDC * sizeof(double) * 2;
    double constval[2]; constval[0] = alphar; constval[1] = alphai;
    double *const_val=constval;
    int64_t M = (int64_t)m, K = (int64_t)k;
    BLASLONG n_count = n;
    double *a_pointer = A,*b_pointer = B,*c_pointer = C,*ctemp = C,*next_b = B;
    for(;n_count>11;n_count-=12) COMPUTE(12)
    for(;n_count>7;n_count-=8) COMPUTE(8)
    for(;n_count>3;n_count-=4) COMPUTE(4)
    for(;n_count>1;n_count-=2) COMPUTE(2)
    if(n_count>0) COMPUTE(1)
    return 0;
}
