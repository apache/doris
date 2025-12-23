/* %0 = a_ptr, %1 = b_ptr, %2 = c_ptr, %3 = c_tmp, %4 = ldc(bytes), %5 = k_counter, %6 = b_pref */
/* r10 = tmp, r11 = m_counter, r12 = size_of_1_tile_in_b, r13 = k, r14 = b_head, r15 = %1+3*r12 */

#if (defined (LEFT) && !defined(TRANSA)) || (!defined (LEFT) && defined(TRANSA))
  #define BACKWARDS 1
#else
  #define BACKWARDS 0
#endif
#define GEMM_SET_PB "movq %%r14,%1; leaq (%%r14,%%r12,2),%%r15; addq %%r12,%%r15;"
#define set_p_copy1(ptr) "sarq $1,%%r12; addq %%r12,"#ptr"; salq $1,%%r12; salq $3,%%r13; subq %%r13,"#ptr"; sarq $3,%%r13;"
#define set_p_copy2(ptr) "addq %%r12,"#ptr"; salq $4,%%r13; subq %%r13,"#ptr"; sarq $4,%%r13;"
#define set_p_copy4(ptr) "leaq ("#ptr",%%r12,2),"#ptr"; salq $5,%%r13; subq %%r13,"#ptr"; sarq $5,%%r13;"
#define set_p_copy8(ptr) "leaq ("#ptr",%%r12,4),"#ptr"; salq $6,%%r13; subq %%r13,"#ptr"; sarq $6,%%r13;"
#define set_p_copy16(ptr) "leaq ("#ptr",%%r12,8),"#ptr"; salq $7,%%r13; subq %%r13,"#ptr"; sarq $7,%%r13;"
#define set_p_b_dim1(ptr) set_p_copy1(ptr)
#define set_p_b_dim2(ptr) set_p_copy2(ptr)
#define set_p_b_dim4(ptr) set_p_copy2(ptr)
#define set_p_b_dim6(ptr) set_p_copy2(ptr)
#define set_p_b_dim8(ptr) set_p_copy2(ptr)
#define set_p_b_dim10(ptr) set_p_copy2(ptr)
#define set_p_b_dim12(ptr) set_p_copy2(ptr)
#ifdef TRMMKERNEL
  #if BACKWARDS == 1
    #define INIT_set_papb(mdim,ndim) GEMM_SET_PB set_p_copy##mdim(%0) set_p_b_dim##ndim(%1) set_p_b_dim##ndim(%%r15)
    #define SAVE_set_pa(mdim) ""
  #else
    #define INIT_set_papb(mdim,ndim) GEMM_SET_PB
    #define SAVE_set_pa(mdim) set_p_copy##mdim(%0)
  #endif
#else
  #define INIT_set_papb(mdim,ndim) GEMM_SET_PB
  #define SAVE_set_pa(mdim) ""
#endif
#if defined(TRMMKERNEL) && !defined(LEFT)
  #if BACKWARDS == 1
    #define HEAD_SET_OFF(ndim) {}
    #define TAIL_SET_OFF(ndim) {off += ndim;}
    #define kernel_kstart_n4(mdim,updk) KERNEL_k1m##mdim##n2 KERNEL_k1m##mdim##n2 "addq $32,%%r15; "#updk" $2,%5;"
    #define kernel_kstart_n6(mdim,updk) kernel_kstart_n4(mdim,updk) KERNEL_k1m##mdim##n4 KERNEL_k1m##mdim##n4 "addq $32,%%r15; "#updk" $2,%5;"
    #define kernel_kstart_n8(mdim,updk) kernel_kstart_n6(mdim,updk) KERNEL_k1m##mdim##n6 KERNEL_k1m##mdim##n6 "addq $32,%%r15; "#updk" $2,%5;"
    #define kernel_kstart_n10(mdim,updk) kernel_kstart_n8(mdim,updk) KERNEL_k1m##mdim##n8 KERNEL_k1m##mdim##n8 #updk" $2,%5;"
    #define kernel_kstart_n12(mdim,updk) kernel_kstart_n10(mdim,updk) KERNEL_k1m##mdim##n10 KERNEL_k1m##mdim##n10 #updk" $2,%5;"
    #define kernel_kend_n4(mdim) ""
    #define kernel_kend_n6(mdim) ""
    #define kernel_kend_n8(mdim) ""
    #define kernel_kend_n10(mdim) ""
    #define kernel_kend_n12(mdim) ""
  #else
    #define HEAD_SET_OFF(ndim) {off += (ndim > 2 ? 2 : ndim);}
    #define TAIL_SET_OFF(ndim) {off += (ndim > 2 ? (ndim-2) : 0);}
    #define kernel_kstart_n4(mdim,updk) ""
    #define kernel_kstart_n6(mdim,updk) ""
    #define kernel_kstart_n8(mdim,updk) ""
    #define kernel_kstart_n10(mdim,updk) ""
    #define kernel_kstart_n12(mdim,updk) ""
    #define kernel_kend_n4(mdim) "xorq %3,%3;"\
      loada_kend_k1m##mdim acc_kend_nc2_k1m##mdim(0)\
      loada_kend_k1m##mdim acc_kend_nc2_k1m##mdim(16)
    #define kernel_kend_n6(mdim) "xorq %3,%3;"\
      loada_kend_k1m##mdim acc_kend_nc2_k1m##mdim(0) acc_kend_nc3_k1m##mdim(0)\
      loada_kend_k1m##mdim acc_kend_nc2_k1m##mdim(16) acc_kend_nc3_k1m##mdim(16)\
      loada_kend_k1m##mdim acc_kend_nc3_k1m##mdim(32)\
      loada_kend_k1m##mdim acc_kend_nc3_k1m##mdim(48)
    #define kernel_kend_n8(mdim) "xorq %3,%3;"\
      loada_kend_k1m##mdim acc_kend_nc2_k1m##mdim(0) acc_kend_nc3_k1m##mdim(0) acc_kend_nc4_k1m##mdim(0)\
      loada_kend_k1m##mdim acc_kend_nc2_k1m##mdim(16) acc_kend_nc3_k1m##mdim(16) acc_kend_nc4_k1m##mdim(16)\
      loada_kend_k1m##mdim acc_kend_nc3_k1m##mdim(32) acc_kend_nc4_k1m##mdim(32)\
      loada_kend_k1m##mdim acc_kend_nc3_k1m##mdim(48) acc_kend_nc4_k1m##mdim(48)\
      loada_kend_k1m##mdim acc_kend_nc4_k1m##mdim(64)\
      loada_kend_k1m##mdim acc_kend_nc4_k1m##mdim(80)
    #define kernel_kend_n10(mdim) "xorq %3,%3;"\
      loada_kend_k1m##mdim acc_kend_nc2_k1m##mdim(0) acc_kend_nc3_k1m##mdim(0) acc_kend_nc4_k1m##mdim(0) acc_kend_nc5_k1m##mdim(0)\
      loada_kend_k1m##mdim acc_kend_nc2_k1m##mdim(16) acc_kend_nc3_k1m##mdim(16) acc_kend_nc4_k1m##mdim(16) acc_kend_nc5_k1m##mdim(16)\
      loada_kend_k1m##mdim acc_kend_nc3_k1m##mdim(32) acc_kend_nc4_k1m##mdim(32) acc_kend_nc5_k1m##mdim(32)\
      loada_kend_k1m##mdim acc_kend_nc3_k1m##mdim(48) acc_kend_nc4_k1m##mdim(48) acc_kend_nc5_k1m##mdim(48)\
      loada_kend_k1m##mdim acc_kend_nc4_k1m##mdim(64) acc_kend_nc5_k1m##mdim(64)\
      loada_kend_k1m##mdim acc_kend_nc4_k1m##mdim(80) acc_kend_nc5_k1m##mdim(80)\
      loada_kend_k1m##mdim acc_kend_nc5_k1m##mdim(96)\
      loada_kend_k1m##mdim acc_kend_nc5_k1m##mdim(112)
    #define kernel_kend_n12(mdim) "xorq %3,%3;"\
      loada_kend_k1m##mdim acc_kend_nc2_k1m##mdim(0) acc_kend_nc3_k1m##mdim(0) acc_kend_nc4_k1m##mdim(0) acc_kend_nc5_k1m##mdim(0) acc_kend_nc6_k1m##mdim(0)\
      loada_kend_k1m##mdim acc_kend_nc2_k1m##mdim(16) acc_kend_nc3_k1m##mdim(16) acc_kend_nc4_k1m##mdim(16) acc_kend_nc5_k1m##mdim(16) acc_kend_nc6_k1m##mdim(16)\
      loada_kend_k1m##mdim acc_kend_nc3_k1m##mdim(32) acc_kend_nc4_k1m##mdim(32) acc_kend_nc5_k1m##mdim(32) acc_kend_nc6_k1m##mdim(32)\
      loada_kend_k1m##mdim acc_kend_nc3_k1m##mdim(48) acc_kend_nc4_k1m##mdim(48) acc_kend_nc5_k1m##mdim(48) acc_kend_nc6_k1m##mdim(48)\
      loada_kend_k1m##mdim acc_kend_nc4_k1m##mdim(64) acc_kend_nc5_k1m##mdim(64) acc_kend_nc6_k1m##mdim(64)\
      loada_kend_k1m##mdim acc_kend_nc4_k1m##mdim(80) acc_kend_nc5_k1m##mdim(80) acc_kend_nc6_k1m##mdim(80)\
      loada_kend_k1m##mdim acc_kend_nc5_k1m##mdim(96) acc_kend_nc6_k1m##mdim(96)\
      loada_kend_k1m##mdim acc_kend_nc5_k1m##mdim(112) acc_kend_nc6_k1m##mdim(112)\
      loada_kend_k1m##mdim acc_kend_nc6_k1m##mdim(128)\
      loada_kend_k1m##mdim acc_kend_nc6_k1m##mdim(144)
  #endif
#else
  #define HEAD_SET_OFF(ndim) {}
  #define TAIL_SET_OFF(ndim) {}
  #define kernel_kstart_n4(mdim,updk) ""
  #define kernel_kstart_n6(mdim,updk) ""
  #define kernel_kstart_n8(mdim,updk) ""
  #define kernel_kstart_n10(mdim,updk) ""
  #define kernel_kstart_n12(mdim,updk) ""
  #define kernel_kend_n4(mdim) ""
  #define kernel_kend_n6(mdim) ""
  #define kernel_kend_n8(mdim) ""
  #define kernel_kend_n10(mdim) ""
  #define kernel_kend_n12(mdim) ""
#endif
#define kernel_kstart_n1(mdim,updk) ""
#define kernel_kstart_n2(mdim,updk) ""
#define kernel_kend_n1(mdim) ""
#define kernel_kend_n2(mdim) ""

#ifdef TRMMKERNEL
  #if BACKWARDS == 1
    #define INITASM_SET_K "movq %10,%%r13; subq %9,%%r13;"
  #else
    #define INITASM_SET_K "movq %9,%%r13;"
  #endif
#else
  #define INITASM_SET_K "movq %10,%%r13;"
#endif
#if defined(TRMMKERNEL) && defined(LEFT)
  #if BACKWARDS==1
    #define init_update_k(mdim) ""
    #define save_update_k(mdim) "subq $"#mdim",%%r13;"
  #else
    #define init_update_k(mdim) "addq $"#mdim",%%r13;"
    #define save_update_k(mdim) ""
  #endif
#else
  #define init_update_k(mdim) ""
  #define save_update_k(mdim) ""
#endif

#define KERNEL_h_k1m16n1 \
  "vmovupd (%0),%%zmm1; vmovupd 64(%0),%%zmm2; addq $128,%0;"\
  "vbroadcastsd (%1),%%zmm3; vfmadd231pd %%zmm1,%%zmm3,%%zmm8; vfmadd231pd %%zmm2,%%zmm3,%%zmm9;"
#define KERNEL_k1m16n1 KERNEL_h_k1m16n1 "addq $8,%1;"
#ifdef BROADCAST_KERNEL
 #define KERNEL_h_k1m16n2 KERNEL_h_k1m16n1\
  "vbroadcastsd 8(%1),%%zmm4; vfmadd231pd %%zmm1,%%zmm4,%%zmm10; vfmadd231pd %%zmm2,%%zmm4,%%zmm11;"
 #define unit_acc_gen_m16n2(c1_no,c2_no,c3_no,c4_no,boff1,...)\
  "vbroadcastsd "#boff1"("#__VA_ARGS__"),%%zmm3; vfmadd231pd %%zmm1,%%zmm3,%%zmm"#c1_no"; vfmadd231pd %%zmm2,%%zmm3,%%zmm"#c2_no";"\
  "vbroadcastsd "#boff1"+8("#__VA_ARGS__"),%%zmm4; vfmadd231pd %%zmm1,%%zmm4,%%zmm"#c3_no"; vfmadd231pd %%zmm2,%%zmm4,%%zmm"#c4_no";"
 #define unit_acc_m16n2(c1_no,c2_no,c3_no,c4_no,...) unit_acc_gen_m16n2(c1_no,c2_no,c3_no,c4_no,0,__VA_ARGS__)
#else
 #define unit_acc_gen_m16n2(c1_no,c2_no,c3_no,c4_no,boff1,...)\
  "vbroadcastf32x4 "#boff1"("#__VA_ARGS__"),%%zmm5; vfmadd231pd %%zmm1,%%zmm5,%%zmm"#c1_no"; vfmadd231pd %%zmm2,%%zmm5,%%zmm"#c2_no";"\
  "vfmadd231pd %%zmm3,%%zmm5,%%zmm"#c3_no"; vfmadd231pd %%zmm4,%%zmm5,%%zmm"#c4_no";"
 #define unit_acc_m16n2(c1_no,c2_no,c3_no,c4_no,...) unit_acc_gen_m16n2(c1_no,c2_no,c3_no,c4_no,0,__VA_ARGS__)
 #define KERNEL_h_k1m16n2 \
  "vmovddup (%0),%%zmm1; vmovddup 8(%0),%%zmm2; vmovddup 64(%0),%%zmm3; vmovddup 72(%0),%%zmm4; addq $128,%0;"\
  unit_acc_m16n2(8,9,10,11,%1)

#endif
#define KERNEL_k1m16n2 KERNEL_h_k1m16n2 "addq $16,%1;"
#define KERNEL_h_k1m16n4 KERNEL_h_k1m16n2 "prefetcht0 384(%0);" unit_acc_m16n2(12,13,14,15,%1,%%r12,1)
#define KERNEL_k1m16n4 KERNEL_h_k1m16n4 "addq $16,%1;"
#define KERNEL_k1m16n6 KERNEL_h_k1m16n4 unit_acc_m16n2(16,17,18,19,%1,%%r12,2) "addq $16,%1;"
#define KERNEL_h_k1m16n8 KERNEL_k1m16n6 "prefetcht0 448(%0);" unit_acc_m16n2(20,21,22,23,%%r15)
#define KERNEL_k1m16n8 KERNEL_h_k1m16n8 "addq $16,%%r15;"
#define KERNEL_h_k1m16n10 KERNEL_h_k1m16n8 unit_acc_m16n2(24,25,26,27,%%r15,%%r12,1)
#define KERNEL_k1m16n10 KERNEL_h_k1m16n10 "addq $16,%%r15;"
#define KERNEL_h_k1m16n12 KERNEL_h_k1m16n10 unit_acc_m16n2(28,29,30,31,%%r15,%%r12,2)
#define KERNEL_k1m16n12 KERNEL_h_k1m16n12 "addq $16,%%r15;"
#if defined(TRMMKERNEL) && !defined(LEFT) && (BACKWARDS == 0)
 #ifdef BROADCAST_KERNEL
  #define loada_kend_k1m16 "vmovupd (%0,%3,1),%%zmm1; vmovupd 64(%0,%3,1),%%zmm2; addq $128,%3;"
 #else
  #define loada_kend_k1m16 "vmovddup (%0,%3,1),%%zmm1; vmovddup 8(%0,%3,1),%%zmm2; vmovddup 64(%0,%3,1),%%zmm3; vmovddup 72(%0,%3,1),%%zmm4; addq $128,%3;"
 #endif
 #define acc_kend_nc2_k1m16(boff1) unit_acc_gen_m16n2(12,13,14,15,boff1,%1,%%r12,1)
 #define acc_kend_nc3_k1m16(boff1) unit_acc_gen_m16n2(16,17,18,19,boff1,%1,%%r12,2)
 #define acc_kend_nc4_k1m16(boff1) unit_acc_gen_m16n2(20,21,22,23,boff1,%%r15)
 #define acc_kend_nc5_k1m16(boff1) unit_acc_gen_m16n2(24,25,26,27,boff1,%%r15,%%r12,1)
 #define acc_kend_nc6_k1m16(boff1) unit_acc_gen_m16n2(28,29,30,31,boff1,%%r15,%%r12,2)
#endif
#define save_init_m16 "movq %2,%3; addq $128,%2;"
#ifdef TRMMKERNEL
  #define SAVE_m16n1 "vmulpd %%zmm8,%%zmm0,%%zmm8; vmovupd %%zmm8,(%2); vmulpd %%zmm9,%%zmm0,%%zmm9; vmovupd %%zmm9,64(%2); addq $128,%2;"
 #ifdef BROADCAST_KERNEL
  #define unit_save_m16n2(c1_no,c2_no,c3_no,c4_no)\
    "vmulpd %%zmm"#c1_no",%%zmm0,%%zmm"#c1_no"; vmovupd %%zmm"#c1_no",(%3); vmulpd %%zmm"#c2_no",%%zmm0,%%zmm"#c2_no"; vmovupd %%zmm"#c2_no",64(%3);"\
    "vmulpd %%zmm"#c3_no",%%zmm0,%%zmm"#c3_no"; vmovupd %%zmm"#c3_no",(%3,%4,1); vmulpd %%zmm"#c4_no",%%zmm0,%%zmm"#c4_no"; vmovupd %%zmm"#c4_no",64(%3,%4,1); leaq (%3,%4,2),%3;"
 #else
  #define unit_save_m16n2(c1_no,c2_no,c3_no,c4_no)\
    "vunpcklpd %%zmm"#c2_no",%%zmm"#c1_no",%%zmm1; vunpcklpd %%zmm"#c4_no",%%zmm"#c3_no",%%zmm2; vunpckhpd %%zmm"#c2_no",%%zmm"#c1_no",%%zmm3; vunpckhpd %%zmm"#c4_no",%%zmm"#c3_no",%%zmm4;"\
    "vmulpd %%zmm1,%%zmm0,%%zmm1; vmovupd %%zmm1,(%3); vmulpd %%zmm2,%%zmm0,%%zmm2; vmovupd %%zmm2,64(%3);"\
    "vmulpd %%zmm3,%%zmm0,%%zmm3; vmovupd %%zmm3,(%3,%4,1); vmulpd %%zmm4,%%zmm0,%%zmm4; vmovupd %%zmm4,64(%3,%4,1); leaq (%3,%4,2),%3;"
 #endif
#else
  #define SAVE_m16n1 "vfmadd213pd (%2),%%zmm0,%%zmm8; vmovupd %%zmm8,(%2); vfmadd213pd 64(%2),%%zmm0,%%zmm9; vmovupd %%zmm9,64(%2); addq $128,%2;"
 #ifdef BROADCAST_KERNEL
  #define unit_save_m16n2(c1_no,c2_no,c3_no,c4_no)\
    "vfmadd213pd (%3),%%zmm0,%%zmm"#c1_no"; vmovupd %%zmm"#c1_no",(%3); vfmadd213pd 64(%3),%%zmm0,%%zmm"#c2_no"; vmovupd %%zmm"#c2_no",64(%3);"\
    "vfmadd213pd (%3,%4,1),%%zmm0,%%zmm"#c3_no"; vmovupd %%zmm"#c3_no",(%3,%4,1); vfmadd213pd 64(%3,%4,1),%%zmm0,%%zmm"#c4_no"; vmovupd %%zmm"#c4_no",64(%3,%4,1); leaq (%3,%4,2),%3;"
 #else
  #define unit_save_m16n2(c1_no,c2_no,c3_no,c4_no)\
    "vunpcklpd %%zmm"#c2_no",%%zmm"#c1_no",%%zmm1; vunpcklpd %%zmm"#c4_no",%%zmm"#c3_no",%%zmm2; vunpckhpd %%zmm"#c2_no",%%zmm"#c1_no",%%zmm3; vunpckhpd %%zmm"#c4_no",%%zmm"#c3_no",%%zmm4;"\
    "vfmadd213pd (%3),%%zmm0,%%zmm1; vmovupd %%zmm1,(%3); vfmadd213pd 64(%3),%%zmm0,%%zmm2; vmovupd %%zmm2,64(%3);"\
    "vfmadd213pd (%3,%4,1),%%zmm0,%%zmm3; vmovupd %%zmm3,(%3,%4,1); vfmadd213pd 64(%3,%4,1),%%zmm0,%%zmm4; vmovupd %%zmm4,64(%3,%4,1); leaq (%3,%4,2),%3;"
 #endif
#endif
#define SAVE_m16n2 save_init_m16 unit_save_m16n2(8,9,10,11)
#define SAVE_m16n4 SAVE_m16n2 unit_save_m16n2(12,13,14,15)
#define SAVE_m16n6 SAVE_m16n4 unit_save_m16n2(16,17,18,19)
#define SAVE_m16n8 SAVE_m16n6 unit_save_m16n2(20,21,22,23)
#define SAVE_m16n10 SAVE_m16n8 unit_save_m16n2(24,25,26,27)
#define SAVE_m16n12 SAVE_m16n10 unit_save_m16n2(28,29,30,31)
#define unit_init_2zmm(c1_no,c2_no) "vpxorq %%zmm"#c1_no",%%zmm"#c1_no",%%zmm"#c1_no"; vpxorq %%zmm"#c2_no",%%zmm"#c2_no",%%zmm"#c2_no";"
#define unit_init_4zmm(c1_no,c2_no,c3_no,c4_no) unit_init_2zmm(c1_no,c2_no) unit_init_2zmm(c3_no,c4_no)
#define INIT_m16n1 unit_init_2zmm(8,9)
#define INIT_m16n2 unit_init_4zmm(8,9,10,11)
#define INIT_m16n4 INIT_m16n2 unit_init_4zmm(12,13,14,15)
#define INIT_m16n6 INIT_m16n4 unit_init_4zmm(16,17,18,19)
#define INIT_m16n8 INIT_m16n6 unit_init_4zmm(20,21,22,23)
#define INIT_m16n10 INIT_m16n8 unit_init_4zmm(24,25,26,27)
#define INIT_m16n12 INIT_m16n10 unit_init_4zmm(28,29,30,31)

#define KERNEL_k1m8n1 \
  "vbroadcastsd (%1),%%zmm1; addq $8,%1;"\
  "vfmadd231pd (%0),%%zmm1,%%zmm8; addq $64,%0;"
#define unit_acc_gen_m8n2(c1_no,c2_no,boff,...)\
  "vbroadcastf32x4 "#boff"("#__VA_ARGS__"),%%zmm3; vfmadd231pd %%zmm1,%%zmm3,%%zmm"#c1_no"; vfmadd231pd %%zmm2,%%zmm3,%%zmm"#c2_no";"
#define unit_acc_m8n2(c1_no,c2_no,...) unit_acc_gen_m8n2(c1_no,c2_no,0,__VA_ARGS__)
#define KERNEL_h_k1m8n2 \
  "vmovddup (%0),%%zmm1; vmovddup 8(%0),%%zmm2; addq $64,%0;" unit_acc_m8n2(8,9,%1)
#define KERNEL_k1m8n2 KERNEL_h_k1m8n2 "addq $16,%1;"
#define KERNEL_h_k1m8n4 KERNEL_h_k1m8n2 unit_acc_m8n2(10,11,%1,%%r12,1)
#define KERNEL_k1m8n4 KERNEL_h_k1m8n4 "addq $16,%1;"
#define KERNEL_k1m8n6 KERNEL_h_k1m8n4 unit_acc_m8n2(12,13,%1,%%r12,2) "addq $16,%1;"
#define KERNEL_h_k1m8n8 KERNEL_k1m8n6 unit_acc_m8n2(14,15,%%r15)
#define KERNEL_k1m8n8 KERNEL_h_k1m8n8 "addq $16,%%r15;"
#define KERNEL_h_k1m8n10 KERNEL_h_k1m8n8 unit_acc_m8n2(16,17,%%r15,%%r12,1)
#define KERNEL_k1m8n10 KERNEL_h_k1m8n10 "addq $16,%%r15;"
#define KERNEL_h_k1m8n12 KERNEL_h_k1m8n10 unit_acc_m8n2(18,19,%%r15,%%r12,2)
#define KERNEL_k1m8n12 KERNEL_h_k1m8n12 "addq $16,%%r15;"
#if defined(TRMMKERNEL) && !defined(LEFT) && (BACKWARDS == 0)
  #define loada_kend_k1m8 "vmovddup (%0,%3,1),%%zmm1; vmovddup 8(%0,%3,1),%%zmm2; addq $64,%3;"
  #define acc_kend_nc2_k1m8(boff1) unit_acc_gen_m8n2(10,11,boff1,%1,%%r12,1)
  #define acc_kend_nc3_k1m8(boff1) unit_acc_gen_m8n2(12,13,boff1,%1,%%r12,2)
  #define acc_kend_nc4_k1m8(boff1) unit_acc_gen_m8n2(14,15,boff1,%%r15)
  #define acc_kend_nc5_k1m8(boff1) unit_acc_gen_m8n2(16,17,boff1,%%r15,%%r12,1)
  #define acc_kend_nc6_k1m8(boff1) unit_acc_gen_m8n2(18,19,boff1,%%r15,%%r12,2)
#endif
#define save_init_m8 "movq %2,%3; addq $64,%2;"
#ifdef TRMMKERNEL
  #define SAVE_m8n1 "vmulpd %%zmm8,%%zmm0,%%zmm8; vmovupd %%zmm8,(%2); addq $64,%2;"
  #define unit_save_m8n2(c1_no,c2_no)\
    "vunpcklpd %%zmm"#c2_no",%%zmm"#c1_no",%%zmm1; vmulpd %%zmm1,%%zmm0,%%zmm1; vmovupd %%zmm1,(%3);"\
    "vunpckhpd %%zmm"#c2_no",%%zmm"#c1_no",%%zmm2; vmulpd %%zmm2,%%zmm0,%%zmm2; vmovupd %%zmm2,(%3,%4,1); leaq (%3,%4,2),%3;"
#else
  #define SAVE_m8n1 "vfmadd213pd (%2),%%zmm0,%%zmm8; vmovupd %%zmm8,(%2); addq $64,%2;"
  #define unit_save_m8n2(c1_no,c2_no)\
    "vunpcklpd %%zmm"#c2_no",%%zmm"#c1_no",%%zmm1; vfmadd213pd (%3),%%zmm0,%%zmm1; vmovupd %%zmm1,(%3);"\
    "vunpckhpd %%zmm"#c2_no",%%zmm"#c1_no",%%zmm2; vfmadd213pd (%3,%4,1),%%zmm0,%%zmm2; vmovupd %%zmm2,(%3,%4,1); leaq (%3,%4,2),%3;"
#endif
#define SAVE_m8n2 save_init_m8 unit_save_m8n2(8,9)
#define SAVE_m8n4 SAVE_m8n2 unit_save_m8n2(10,11)
#define SAVE_m8n6 SAVE_m8n4 unit_save_m8n2(12,13)
#define SAVE_m8n8 SAVE_m8n6 unit_save_m8n2(14,15)
#define SAVE_m8n10 SAVE_m8n8 unit_save_m8n2(16,17)
#define SAVE_m8n12 SAVE_m8n10 unit_save_m8n2(18,19)
#define INIT_m8n1 "vpxorq %%zmm8,%%zmm8,%%zmm8;"
#define INIT_m8n2 unit_init_2zmm(8,9)
#define INIT_m8n4 INIT_m8n2 unit_init_2zmm(10,11)
#define INIT_m8n6 INIT_m8n4 unit_init_2zmm(12,13)
#define INIT_m8n8 INIT_m8n6 unit_init_2zmm(14,15)
#define INIT_m8n10 INIT_m8n8 unit_init_2zmm(16,17)
#define INIT_m8n12 INIT_m8n10 unit_init_2zmm(18,19)

#define KERNEL_k1m4n1 \
  "vbroadcastsd (%1),%%ymm1; addq $8,%1;"\
  "vfmadd231pd (%0),%%ymm1,%%ymm4; addq $32,%0;"
#define unit_acc_gen_m4n2(c1_no,c2_no,boff,...)\
  "vbroadcastf128 "#boff"("#__VA_ARGS__"),%%ymm3; vfmadd231pd %%ymm1,%%ymm3,%%ymm"#c1_no"; vfmadd231pd %%ymm2,%%ymm3,%%ymm"#c2_no";"
#define unit_acc_m4n2(c1_no,c2_no,...) unit_acc_gen_m4n2(c1_no,c2_no,0,__VA_ARGS__)
#define KERNEL_h_k1m4n2 \
  "vmovddup (%0),%%ymm1; vmovddup 8(%0),%%ymm2; addq $32,%0;" unit_acc_m4n2(4,5,%1)
#define KERNEL_k1m4n2 KERNEL_h_k1m4n2 "addq $16,%1;"
#define KERNEL_h_k1m4n4 KERNEL_h_k1m4n2 unit_acc_m4n2(6,7,%1,%%r12,1)
#define KERNEL_k1m4n4 KERNEL_h_k1m4n4 "addq $16,%1;"
#define KERNEL_k1m4n6 KERNEL_h_k1m4n4 unit_acc_m4n2(8,9,%1,%%r12,2) "addq $16,%1;"
#define KERNEL_h_k1m4n8 KERNEL_k1m4n6 unit_acc_m4n2(10,11,%%r15)
#define KERNEL_k1m4n8 KERNEL_h_k1m4n8 "addq $16,%%r15;"
#define KERNEL_h_k1m4n10 KERNEL_h_k1m4n8 unit_acc_m4n2(12,13,%%r15,%%r12,1)
#define KERNEL_k1m4n10 KERNEL_h_k1m4n10 "addq $16,%%r15;"
#define KERNEL_h_k1m4n12 KERNEL_h_k1m4n10 unit_acc_m4n2(14,15,%%r15,%%r12,2)
//#define KERNEL_k1m4n12 KERNEL_h_k1m4n12 "addq $16,%%r15;"
#define unit_acc_k2m4n2(c1_no,c2_no,...)\
  "vbroadcastf64x4 ("#__VA_ARGS__"),%%zmm3; vpermpd %%zmm3,%%zmm30,%%zmm3;"\
  "vfmadd231pd %%zmm1,%%zmm3,%%zmm"#c1_no"; vfmadd231pd %%zmm2,%%zmm3,%%zmm"#c2_no";"

#define unit_merge_to_ymm(c1_no) \
  "vextractf64x4 $1,%%zmm"#c1_no",%%ymm30; vaddpd %%ymm"#c1_no",%%ymm30,%%ymm"#c1_no";"

#define KERNEL_k1m4n12 \
  "cmpq $2, %5; jb 104912f;"\
  "vmovupd 64+%11,%%zmm30;"\
  "\n204912:"\
  "vmovddup (%0),%%zmm1; vmovddup 8(%0),%%zmm2; addq $64,%0;" \
  unit_acc_k2m4n2(4,5,%1) unit_acc_k2m4n2(6,7,%1,%%r12,1) unit_acc_k2m4n2(8, 9, %1, %%r12, 2) "addq $32,%1;" \
  unit_acc_k2m4n2(10,11,%%r15) unit_acc_k2m4n2(12,13,%%r15,%%r12,1) unit_acc_k2m4n2(14,15,%%r15,%%r12,2) "addq $32,%%r15;" \
  "subq $2, %5; cmpq $2, %5; jnb 204912b;"\
  unit_merge_to_ymm(4) unit_merge_to_ymm(5) unit_merge_to_ymm(6) unit_merge_to_ymm(7) \
  unit_merge_to_ymm(8) unit_merge_to_ymm(9) unit_merge_to_ymm(10) unit_merge_to_ymm(11) \
  unit_merge_to_ymm(12) unit_merge_to_ymm(13) unit_merge_to_ymm(14) unit_merge_to_ymm(15) \
  "testq %5, %5; jz 1004912f;"\
  "\n104912:"\
  KERNEL_h_k1m4n12 "addq $16,%%r15;"\
  "decq %5; jnz 104912b;"\
  "\n1004912:"\
  "incq %5;"

#if defined(TRMMKERNEL) && !defined(LEFT) && (BACKWARDS == 0)
  #define loada_kend_k1m4 "vmovddup (%0,%3,1),%%ymm1; vmovddup 8(%0,%3,1),%%ymm2; addq $32,%3;"
  #define acc_kend_nc2_k1m4(boff1) unit_acc_gen_m4n2(6,7,boff1,%1,%%r12,1)
  #define acc_kend_nc3_k1m4(boff1) unit_acc_gen_m4n2(8,9,boff1,%1,%%r12,2)
  #define acc_kend_nc4_k1m4(boff1) unit_acc_gen_m4n2(10,11,boff1,%%r15)
  #define acc_kend_nc5_k1m4(boff1) unit_acc_gen_m4n2(12,13,boff1,%%r15,%%r12,1)
  #define acc_kend_nc6_k1m4(boff1) unit_acc_gen_m4n2(14,15,boff1,%%r15,%%r12,2)
#endif
#define save_init_m4 "movq %2,%3; addq $32,%2;"
#ifdef TRMMKERNEL
  #define SAVE_m4n1 "vmulpd %%ymm4,%%ymm0,%%ymm4; vmovupd %%ymm4,(%2); addq $32,%2;"
  #define unit_save_m4n2(c1_no,c2_no)\
    "vunpcklpd %%ymm"#c2_no",%%ymm"#c1_no",%%ymm1; vmulpd %%ymm1,%%ymm0,%%ymm1; vmovupd %%ymm1,(%3);"\
    "vunpckhpd %%ymm"#c2_no",%%ymm"#c1_no",%%ymm2; vmulpd %%ymm2,%%ymm0,%%ymm2; vmovupd %%ymm2,(%3,%4,1); leaq (%3,%4,2),%3;"
#else
  #define SAVE_m4n1 "vfmadd213pd (%2),%%ymm0,%%ymm4; vmovupd %%ymm4,(%2); addq $32,%2;"
  #define unit_save_m4n2(c1_no,c2_no)\
    "vunpcklpd %%ymm"#c2_no",%%ymm"#c1_no",%%ymm1; vfmadd213pd (%3),%%ymm0,%%ymm1; vmovupd %%ymm1,(%3);"\
    "vunpckhpd %%ymm"#c2_no",%%ymm"#c1_no",%%ymm2; vfmadd213pd (%3,%4,1),%%ymm0,%%ymm2; vmovupd %%ymm2,(%3,%4,1); leaq (%3,%4,2),%3;"
#endif
#define SAVE_m4n2 save_init_m4 unit_save_m4n2(4,5)
#define SAVE_m4n4 SAVE_m4n2 unit_save_m4n2(6,7)
#define SAVE_m4n6 SAVE_m4n4 unit_save_m4n2(8,9)
#define SAVE_m4n8 SAVE_m4n6 unit_save_m4n2(10,11)
#define SAVE_m4n10 SAVE_m4n8 unit_save_m4n2(12,13)
#define SAVE_m4n12 SAVE_m4n10 unit_save_m4n2(14,15)
#define INIT_m4n1 "vpxor %%ymm4,%%ymm4,%%ymm4;"
#define unit_init_2ymm(c1_no,c2_no) "vpxor %%ymm"#c1_no",%%ymm"#c1_no",%%ymm"#c1_no"; vpxor %%ymm"#c2_no",%%ymm"#c2_no",%%ymm"#c2_no";"
#define INIT_m4n2 unit_init_2ymm(4,5)
#define INIT_m4n4 INIT_m4n2 unit_init_2ymm(6,7)
#define INIT_m4n6 INIT_m4n4 unit_init_2ymm(8,9)
#define INIT_m4n8 INIT_m4n6 unit_init_2ymm(10,11)
#define INIT_m4n10 INIT_m4n8 unit_init_2ymm(12,13)
#define INIT_m4n12 INIT_m4n10 unit_init_2ymm(14,15)

#define KERNEL_k1m2n1 \
  "vmovddup (%1),%%xmm1; addq $8,%1;"\
  "vfmadd231pd (%0),%%xmm1,%%xmm4; addq $16,%0;"
#define unit_acc_gen_m2n2(c1_no,c2_no,boff,...)\
  "vmovupd "#boff"("#__VA_ARGS__"),%%xmm3; vfmadd231pd %%xmm1,%%xmm3,%%xmm"#c1_no"; vfmadd231pd %%xmm2,%%xmm3,%%xmm"#c2_no";"
#define unit_acc_m2n2(c1_no,c2_no,...) unit_acc_gen_m2n2(c1_no,c2_no,0,__VA_ARGS__)
#define KERNEL_h_k1m2n2 \
  "vmovddup (%0),%%xmm1; vmovddup 8(%0),%%xmm2; addq $16,%0;" unit_acc_m2n2(4,5,%1)
#define KERNEL_k1m2n2 KERNEL_h_k1m2n2 "addq $16,%1;"
#define KERNEL_h_k1m2n4 KERNEL_h_k1m2n2 unit_acc_m2n2(6,7,%1,%%r12,1)
#define KERNEL_k1m2n4 KERNEL_h_k1m2n4 "addq $16,%1;"
#define KERNEL_k1m2n6 KERNEL_h_k1m2n4 unit_acc_m2n2(8,9,%1,%%r12,2) "addq $16,%1;"
#define KERNEL_h_k1m2n8 KERNEL_k1m2n6 unit_acc_m2n2(10,11,%%r15)
#define KERNEL_k1m2n8 KERNEL_h_k1m2n8 "addq $16,%%r15;"
#define KERNEL_h_k1m2n10 KERNEL_h_k1m2n8 unit_acc_m2n2(12,13,%%r15,%%r12,1)
#define KERNEL_k1m2n10 KERNEL_h_k1m2n10 "addq $16,%%r15;"
#define KERNEL_h_k1m2n12 KERNEL_h_k1m2n10 unit_acc_m2n2(14,15,%%r15,%%r12,2)
//#define KERNEL_k1m2n12 KERNEL_h_k1m2n12 "addq $16,%%r15;"

#define unit_acc_k4m2n2(c1_no,c2_no,...) \
  "vmovupd ("#__VA_ARGS__"),%%zmm3; vfmadd231pd %%zmm1,%%zmm3,%%zmm"#c1_no"; vfmadd231pd %%zmm2,%%zmm3,%%zmm"#c2_no";"

#define unit_merge_to_xmm(c1_no) \
  "vextractf64x2 $0,%%zmm"#c1_no",%%xmm20; vextractf64x2 $1,%%zmm"#c1_no",%%xmm21; vextractf64x2 $2,%%zmm"#c1_no",%%xmm22; vextractf64x2 $3,%%zmm"#c1_no",%%xmm23;"\
  "vaddpd %%xmm20,%%xmm21,%%xmm20; vaddpd %%xmm22,%%xmm23,%%xmm22; vaddpd %%xmm20,%%xmm22,%%xmm"#c1_no";"

#define KERNEL_k1m2n12 \
  "cmpq $4,%5; jb 102912f;"\
  "\n402912:"\
  "vmovddup (%0),%%zmm1; vmovddup 8(%0),%%zmm2; addq $64,%0;" \
  unit_acc_k4m2n2(4,5,%1) unit_acc_k4m2n2(6,7,%1,%%r12,1) unit_acc_k4m2n2(8,9,%1,%%r12,2) "addq $64,%1;" \
  unit_acc_k4m2n2(10,11,%%r15) unit_acc_k4m2n2(12,13,%%r15,%%r12,1) unit_acc_k4m2n2(14,15,%%r15,%%r12,2) "addq $64,%%r15;" \
  "subq $4,%5; cmpq $4,%5; jnb 402912b;"\
  unit_merge_to_xmm(4) unit_merge_to_xmm(5) unit_merge_to_xmm(6) unit_merge_to_xmm(7) unit_merge_to_xmm(8) unit_merge_to_xmm(9) \
  unit_merge_to_xmm(10) unit_merge_to_xmm(11) unit_merge_to_xmm(12) unit_merge_to_xmm(13) unit_merge_to_xmm(14) unit_merge_to_xmm(15) \
  "testq %5,%5; jz 1002912f;"\
  "\n102912:"\
  KERNEL_h_k1m2n12 "addq $16,%%r15;" \
  "decq %5; jnz 102912b;" \
  "\n1002912:"\
  "incq %5;"

#if defined(TRMMKERNEL) && !defined(LEFT) && (BACKWARDS == 0)
  #define loada_kend_k1m2 "vmovddup (%0,%3,1),%%xmm1; vmovddup 8(%0,%3,1),%%xmm2; addq $16,%3;"
  #define acc_kend_nc2_k1m2(boff1) unit_acc_gen_m2n2(6,7,boff1,%1,%%r12,1)
  #define acc_kend_nc3_k1m2(boff1) unit_acc_gen_m2n2(8,9,boff1,%1,%%r12,2)
  #define acc_kend_nc4_k1m2(boff1) unit_acc_gen_m2n2(10,11,boff1,%%r15)
  #define acc_kend_nc5_k1m2(boff1) unit_acc_gen_m2n2(12,13,boff1,%%r15,%%r12,1)
  #define acc_kend_nc6_k1m2(boff1) unit_acc_gen_m2n2(14,15,boff1,%%r15,%%r12,2)
#endif
#define save_init_m2 "movq %2,%3; addq $16,%2;"
#ifdef TRMMKERNEL
  #define SAVE_m2n1 "vmulpd %%xmm4,%%xmm0,%%xmm4; vmovupd %%xmm4,(%2); addq $16,%2;"
  #define unit_save_m2n2(c1_no,c2_no)\
    "vunpcklpd %%xmm"#c2_no",%%xmm"#c1_no",%%xmm1; vmulpd %%xmm1,%%xmm0,%%xmm1; vmovupd %%xmm1,(%3);"\
    "vunpckhpd %%xmm"#c2_no",%%xmm"#c1_no",%%xmm2; vmulpd %%xmm2,%%xmm0,%%xmm2; vmovupd %%xmm2,(%3,%4,1); leaq (%3,%4,2),%3;"
#else
  #define SAVE_m2n1 "vfmadd213pd (%2),%%xmm0,%%xmm4; vmovupd %%xmm4,(%2); addq $16,%2;"
  #define unit_save_m2n2(c1_no,c2_no)\
    "vunpcklpd %%xmm"#c2_no",%%xmm"#c1_no",%%xmm1; vfmadd213pd (%3),%%xmm0,%%xmm1; vmovupd %%xmm1,(%3);"\
    "vunpckhpd %%xmm"#c2_no",%%xmm"#c1_no",%%xmm2; vfmadd213pd (%3,%4,1),%%xmm0,%%xmm2; vmovupd %%xmm2,(%3,%4,1); leaq (%3,%4,2),%3;"
#endif
#define SAVE_m2n2 save_init_m2 unit_save_m2n2(4,5)
#define SAVE_m2n4 SAVE_m2n2 unit_save_m2n2(6,7)
#define SAVE_m2n6 SAVE_m2n4 unit_save_m2n2(8,9)
#define SAVE_m2n8 SAVE_m2n6 unit_save_m2n2(10,11)
#define SAVE_m2n10 SAVE_m2n8 unit_save_m2n2(12,13)
#define SAVE_m2n12 SAVE_m2n10 unit_save_m2n2(14,15)
#define INIT_m2n1 "vpxor %%xmm4,%%xmm4,%%xmm4;"
#define unit_init_2xmm(c1_no,c2_no) "vpxor %%xmm"#c1_no",%%xmm"#c1_no",%%xmm"#c1_no"; vpxor %%xmm"#c2_no",%%xmm"#c2_no",%%xmm"#c2_no";"
#define INIT_m2n2 unit_init_2xmm(4,5)
#define INIT_m2n4 INIT_m2n2 unit_init_2xmm(6,7)
#define INIT_m2n6 INIT_m2n4 unit_init_2xmm(8,9)
#define INIT_m2n8 INIT_m2n6 unit_init_2xmm(10,11)
#define INIT_m2n10 INIT_m2n8 unit_init_2xmm(12,13)
#define INIT_m2n12 INIT_m2n10 unit_init_2xmm(14,15)

#define KERNEL_k1m1n1 \
  "vmovsd (%1),%%xmm1; addq $8,%1;"\
  "vfmadd231sd (%0),%%xmm1,%%xmm4; addq $8,%0;"
#define KERNEL_h_k1m1n2 \
  "vmovddup (%0),%%xmm1; addq $8,%0;"\
  "vfmadd231pd (%1),%%xmm1,%%xmm4;"
#define KERNEL_k1m1n2 KERNEL_h_k1m1n2 "addq $16,%1;"
#define KERNEL_h_k1m1n4 KERNEL_h_k1m1n2 "vfmadd231pd (%1,%%r12,1),%%xmm1,%%xmm5;"
#define KERNEL_k1m1n4 KERNEL_h_k1m1n4 "addq $16,%1;"
#define KERNEL_k1m1n6 KERNEL_h_k1m1n4 "vfmadd231pd (%1,%%r12,2),%%xmm1,%%xmm6; addq $16,%1;"
#define KERNEL_h_k1m1n8 KERNEL_k1m1n6 "vfmadd231pd (%%r15),%%xmm1,%%xmm7;"
#define KERNEL_k1m1n8 KERNEL_h_k1m1n8 "addq $16,%%r15;"
#define KERNEL_h_k1m1n10 KERNEL_h_k1m1n8 "vfmadd231pd (%%r15,%%r12,1),%%xmm1,%%xmm8;"
#define KERNEL_k1m1n10 KERNEL_h_k1m1n10 "addq $16,%%r15;"
#define KERNEL_h_k1m1n12 KERNEL_h_k1m1n10 "vfmadd231pd (%%r15,%%r12,2),%%xmm1,%%xmm9;"
//#define KERNEL_k1m1n12 KERNEL_h_k1m1n12 "addq $16,%%r15;"
#define KERNEL_k1m1n12 \
  "cmpq $4,%5; jb 101912f;" \
  "vmovupd %11,%%zmm2;"\
  "\n401912:"\
  "vmovupd (%0),%%ymm1; vpermpd %%zmm1,%%zmm2,%%zmm1; addq $32,%0;" \
  "vfmadd231pd (%1),%%zmm1,%%zmm4; vfmadd231pd (%1,%%r12,1),%%zmm1,%%zmm5; vfmadd231pd (%1,%%r12,2),%%zmm1,%%zmm6; addq $64,%1;"\
  "vfmadd231pd (%%r15),%%zmm1,%%zmm7; vfmadd231pd (%%r15,%%r12,1),%%zmm1,%%zmm8; vfmadd231pd (%%r15,%%r12,2),%%zmm1,%%zmm9; addq $64,%%r15;"\
  "subq $4,%5; cmpq $4,%5; jnb 401912b;"\
  unit_merge_to_xmm(4) unit_merge_to_xmm(5) unit_merge_to_xmm(6) \
  unit_merge_to_xmm(7) unit_merge_to_xmm(8) unit_merge_to_xmm(9) \
  "testq %5,%5; jz 1001912f;"\
  "\n101912:"\
  KERNEL_h_k1m1n12 "addq $16,%%r15;" \
  "decq %5; jnz 101912b;" \
  "\n1001912:"\
  "incq %5;"

#if defined(TRMMKERNEL) && !defined(LEFT) && (BACKWARDS == 0)
  #define loada_kend_k1m1 "vmovddup (%0,%3,1),%%xmm1; addq $8,%3;"
  #define acc_kend_nc2_k1m1(boff1) "vfmadd231pd "#boff1"(%1,%%r12,1),%%xmm1,%%xmm5;"
  #define acc_kend_nc3_k1m1(boff1) "vfmadd231pd "#boff1"(%1,%%r12,2),%%xmm1,%%xmm6;"
  #define acc_kend_nc4_k1m1(boff1) "vfmadd231pd "#boff1"(%%r15),%%xmm1,%%xmm7;"
  #define acc_kend_nc5_k1m1(boff1) "vfmadd231pd "#boff1"(%%r15,%%r12,1),%%xmm1,%%xmm8;"
  #define acc_kend_nc6_k1m1(boff1) "vfmadd231pd "#boff1"(%%r15,%%r12,2),%%xmm1,%%xmm9;"
#endif
#define save_init_m1 "movq %2,%3; addq $8,%2;"
#ifdef TRMMKERNEL
  #define SAVE_m1n1 "vmulsd %%xmm4,%%xmm0,%%xmm4; vmovsd %%xmm4,(%2); addq $8,%2;"
  #define unit_save_m1n2(c1_no)\
    "vmulpd %%xmm"#c1_no",%%xmm0,%%xmm2; vmovsd %%xmm2,(%3); vmovhpd %%xmm2,(%3,%4,1); leaq (%3,%4,2),%3;"
#else
  #define SAVE_m1n1 "vfmadd213sd (%2),%%xmm0,%%xmm4; vmovsd %%xmm4,(%2); addq $8,%2;"
  #define unit_save_m1n2(c1_no)\
    "vmovsd (%3),%%xmm2; vmovhpd (%3,%4,1),%%xmm2,%%xmm2; vfmadd231pd %%xmm"#c1_no",%%xmm0,%%xmm2; vmovsd %%xmm2,(%3); vmovhpd %%xmm2,(%3,%4,1); leaq (%3,%4,2),%3;"
#endif
#define SAVE_m1n2 save_init_m1 unit_save_m1n2(4)
#define SAVE_m1n4 SAVE_m1n2 unit_save_m1n2(5)
#define SAVE_m1n6 SAVE_m1n4 unit_save_m1n2(6)
#define SAVE_m1n8 SAVE_m1n6 unit_save_m1n2(7)
#define SAVE_m1n10 SAVE_m1n8 unit_save_m1n2(8)
#define SAVE_m1n12 SAVE_m1n10 unit_save_m1n2(9)
#define INIT_m1n1 "vpxor %%xmm4,%%xmm4,%%xmm4;"
#define INIT_m1n2 INIT_m1n1
#define INIT_m1n4 INIT_m1n2 "vpxor %%xmm5,%%xmm5,%%xmm5;"
#define INIT_m1n6 INIT_m1n4 "vpxor %%xmm6,%%xmm6,%%xmm6;"
#define INIT_m1n8 INIT_m1n6 "vpxor %%xmm7,%%xmm7,%%xmm7;"
#define INIT_m1n10 INIT_m1n8 "vpxor %%xmm8,%%xmm8,%%xmm8;"
#define INIT_m1n12 INIT_m1n10 "vpxor %%xmm9,%%xmm9,%%xmm9;"

#define COMPUTE_SIMPLE(mdim,ndim)\
  init_update_k(mdim) INIT_m##mdim##n##ndim\
  "movq %%r13,%5;" INIT_set_papb(mdim,ndim)\
  kernel_kstart_n##ndim(mdim,subq)\
  "testq %5,%5; jz 7"#mdim"7"#ndim"9f;"\
  "7"#mdim"7"#ndim"1:\n\t"\
  KERNEL_k1m##mdim##n##ndim "decq %5; jnz 7"#mdim"7"#ndim"1b;"\
  "7"#mdim"7"#ndim"9:\n\t"\
  kernel_kend_n##ndim(mdim)\
  SAVE_set_pa(mdim) SAVE_m##mdim##n##ndim save_update_k(mdim)
#define COMPUTE_m16n1 COMPUTE_SIMPLE(16,1)
#define COMPUTE_m16n2 COMPUTE_SIMPLE(16,2)
#define COMPUTE_m16n4 COMPUTE_SIMPLE(16,4)
#define COMPUTE_m16n6 COMPUTE_SIMPLE(16,6)
#define COMPUTE_m16n8 COMPUTE_SIMPLE(16,8)
#define COMPUTE_m16n10 COMPUTE_SIMPLE(16,10)
#if defined(TRMMKERNEL) && !defined(LEFT) && defined(TRANSA)
  #define INVERSE_K_MID "negq %5; leaq 6(%%r13,%5,1),%5;"
#else
  #define INVERSE_K_MID "negq %5; leaq 16(%%r13,%5,1),%5;"
#endif
#define COMPUTE_m16n12 \
  init_update_k(16) INIT_m16n12 "movq %%r13,%5;" INIT_set_papb(16,12) "movq %2,%3;"\
  kernel_kstart_n12(16,subq)\
  "cmpq $16,%5; jb 7167123f; movq $16,%5;"\
  "7167121:\n\t"\
  KERNEL_k1m16n12 "addq $4,%5; testq $12,%5; movq $172,%%r10; cmovz %4,%%r10;"\
  KERNEL_k1m16n12 "prefetcht1 (%3); subq $129,%3; addq %%r10,%3;"\
  KERNEL_k1m16n12 "prefetcht1 (%6); addq $32,%6; cmpq $208,%5; cmoveq %2,%3;"\
  KERNEL_k1m16n12 "cmpq %5,%%r13; jnb 7167121b;"\
  "movq %2,%3;" INVERSE_K_MID\
  "7167123:\n\t"\
  "testq %5,%5; jz 7167129f;"\
  "7167125:\n\t"\
  "prefetcht0 (%3); prefetcht0 64(%3); prefetcht0 127(%3);"\
  KERNEL_k1m16n12 "addq %4,%3; decq %5;jnz 7167125b;"\
  "7167129:\n\t"\
  kernel_kend_n12(16)\
  "prefetcht0 (%%r14);" SAVE_set_pa(16) SAVE_m16n12 save_update_k(16)
#define COMPUTE(ndim) {\
  b_pref = b_ptr + ndim * K; HEAD_SET_OFF(ndim)\
  __asm__ __volatile__(\
    "vbroadcastsd %8,%%zmm0; movq %7,%%r11; movq %1,%%r14; movq %10,%%r12; salq $4,%%r12;" INITASM_SET_K\
    "cmpq $16,%%r11; jb "#ndim"33102f;"\
    #ndim"33101:\n\t"\
    COMPUTE_m16n##ndim "subq $16,%%r11; cmpq $16,%%r11; jnb "#ndim"33101b;"\
    #ndim"33102:\n\t"\
    "cmpq $8,%%r11; jb "#ndim"33103f;"\
    COMPUTE_SIMPLE(8,ndim) "subq $8,%%r11;"\
    #ndim"33103:\n\t"\
    "cmpq $4,%%r11; jb "#ndim"33104f;"\
    COMPUTE_SIMPLE(4,ndim) "subq $4,%%r11;"\
    #ndim"33104:\n\t"\
    "cmpq $2,%%r11; jb "#ndim"33105f;"\
    COMPUTE_SIMPLE(2,ndim) "subq $2,%%r11;"\
    #ndim"33105:\n\t"\
    "testq %%r11,%%r11; jz "#ndim"33106f;"\
    COMPUTE_SIMPLE(1,ndim) "subq $1,%%r11;"\
    #ndim"33106:\n\t"\
    "movq %%r14,%1;"\
  :"+r"(a_ptr),"+r"(b_ptr),"+r"(c_ptr),"+r"(c_tmp),"+r"(ldc_in_bytes),"+r"(k_count),"+r"(b_pref):"m"(M),"m"(ALPHA),"m"(off),"m"(K), "o"(permute_table):"r10","r11","r12","r13","r14","r15","cc","memory",\
    "zmm0","zmm1","zmm2","zmm3","zmm4","zmm5","zmm6","zmm7","zmm8","zmm9","zmm10","zmm11","zmm12","zmm13","zmm14","zmm15",\
    "zmm16","zmm17","zmm18","zmm19","zmm20","zmm21","zmm22","zmm23","zmm24","zmm25","zmm26","zmm27","zmm28","zmm29","zmm30","zmm31");\
  a_ptr -= M * K; b_ptr += ndim * K; c_ptr += ndim * ldc - M; TAIL_SET_OFF(ndim)\
}

#include "common.h"
#include <stdint.h>

int __attribute__ ((noinline))
CNAME(BLASLONG m, BLASLONG n, BLASLONG k, double alpha, double * __restrict__ A, double * __restrict__ B, double * __restrict__ C, BLASLONG ldc
#ifdef TRMMKERNEL
  , BLASLONG offset
#endif
)
{
    if(m==0||n==0) return 0;
    int64_t ldc_in_bytes = (int64_t)ldc * sizeof(double); double ALPHA = alpha;
    int64_t M = (int64_t)m, K = (int64_t)k, k_count = 0;
    BLASLONG n_count = n, off = 0;
    double *a_ptr = A,*b_ptr = B,*c_ptr = C,*c_tmp = C,*b_pref = B;
    int64_t permute_table[] = {
	    0, 0, 1, 1, 2, 2, 3, 3,  // abcdxxxx -> aabbccdd
	    0, 1, 0, 1, 2, 3, 2, 3,  // abcdxxxx -> ababcdcd
    };
#ifdef TRMMKERNEL
  #ifdef LEFT
    off = offset;
  #else
    off = -offset;
  #endif
#endif
    for(;n_count>11;n_count-=12) COMPUTE(12)
    for(;n_count>9;n_count-=10) COMPUTE(10)
    for(;n_count>7;n_count-=8) COMPUTE(8)
    for(;n_count>5;n_count-=6) COMPUTE(6)
    for(;n_count>3;n_count-=4) COMPUTE(4)
    for(;n_count>1;n_count-=2) COMPUTE(2)
    if(n_count>0) COMPUTE(1)
    return 0;
}

