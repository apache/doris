#define ASSEMBLER

#include "common.h"
#define N      $r4
#define ALPHAR $f0
#define ALPHAI $f1
#define X      $r5
#define INCX   $r6
#define BETAR  $f2
#define BETAI  $f3
#define Y      $r7
#define INCY   $r8

#define I      $r12
#define TEMP   $r13
#define t1     $r14
#define t2     $r16
#define t3     $r15
#define t4     $r17
#define XX     $r18
#define YY     $r19
#define a1     $f12
#define a2     $f13
#define a3     $f14
#define a4     $f15
#define s1     $f16
#define s2     $f17
#define s3     $f18
#define s4     $f19
#define VX0    $vr8
#define VX1    $vr20
#define VX2    $vr21
#define VX3    $vr22
#define VXAR   $vr23
#define VXAI   $vr19
#define VXBR   $vr14
#define VXBI   $vr13
#define VXZ    $vr12
#define x1     $vr18
#define x2     $vr17
#define x3     $vr16
#define x4     $vr15

    PROLOGUE

    bge $r0, N, .L999
    movgr2fr.d a1, $r0
#ifdef DOUBLE
    ffint.d.l a1, a1
#else
    ffint.s.l a1, a1
#endif
    slli.d  INCX, INCX, ZBASE_SHIFT
    slli.d  INCY, INCY, ZBASE_SHIFT
#ifdef DOUBLE
    movfr2gr.d t1, ALPHAR
    vreplgr2vr.d VXAR, t1
    movfr2gr.d t2, ALPHAI
    vreplgr2vr.d VXAI, t2
    movfr2gr.d t3, BETAR
    vreplgr2vr.d VXBR, t3
    movfr2gr.d t4, BETAI
    vreplgr2vr.d VXBI, t4
#else
    movfr2gr.s t1, ALPHAR
    vreplgr2vr.w VXAR, t1
    movfr2gr.s t2, ALPHAI
    vreplgr2vr.w VXAI, t2
    movfr2gr.s t3, BETAR
    vreplgr2vr.w VXBR, t3
    movfr2gr.s t4, BETAI
    vreplgr2vr.w VXBI, t4
#endif
    vxor.v VXZ, VXZ, VXZ
    // If incx == 0 || incy == 0, do one by one
    and TEMP, INCX, INCY
    or  I,    N,    N
    beqz TEMP, .L998

    li.d TEMP, 1
    slli.d  TEMP, TEMP, ZBASE_SHIFT
    srai.d I, N, 2
    bne INCX, TEMP, .L20
    bne INCY, TEMP, .L12 // INCX==1 and INCY!=1
    b .L11  // INCX==1 and INCY==1
.L20:
    bne INCY, TEMP, .L22 // INCX!=1 and INCY!=1
    b .L21 // INCX!=1 and INCY==1

.L11:
    bge $r0, I, .L997
#ifdef DOUBLE
    fcmp.ceq.d $fcc0, BETAR, a1
    fcmp.ceq.d $fcc1, BETAI, a1
    fcmp.ceq.d $fcc2, ALPHAR, a1
    fcmp.ceq.d $fcc3, ALPHAI, a1
#else
    fcmp.ceq.s $fcc0, BETAR, a1
    fcmp.ceq.s $fcc1, BETAI, a1
    fcmp.ceq.s $fcc2, ALPHAR, a1
    fcmp.ceq.s $fcc3, ALPHAI, a1
#endif
    bceqz $fcc0, .L13
    bceqz $fcc1, .L13
    b .L14
    .align 3

.L13:
    bceqz $fcc2, .L114
    bceqz $fcc3, .L114 //!(beta_r == 0.0 && beta_i == 0.0) and !(alpha_r == 0.0 && alpha_i == 0.0)
    b .L113 //!(beta_r == 0.0 && beta_i == 0.0) and (alpha_r == 0.0 && alpha_i == 0.0)

.L14:
    bceqz $fcc2, .L112
    bceqz $fcc3, .L112 //(beta_r == 0.0 && beta_i == 0.0) and !(alpha_r == 0.0 && alpha_i == 0.0)
    b .L111 //(beta_r == 0.0 && beta_i == 0.0) and (alpha_r == 0.0 && alpha_i == 0.0)
    .align 3

.L111:  //(beta_r == 0.0 && beta_i == 0.0) and (alpha_r == 0.0 && alpha_i == 0.0)
#ifdef DOUBLE
    vst VXZ, Y, 0 * SIZE
    vst VXZ, Y, 2 * SIZE
    vst VXZ, Y, 4 * SIZE
    vst VXZ, Y, 6 * SIZE
    addi.d Y, Y, 8 * SIZE
    addi.d  I, I, -1
    blt $r0, I, .L111
    b .L997
    .align 3
#else
    vst VXZ, Y, 0 * SIZE
    vst VXZ, Y, 4 * SIZE
    addi.d Y, Y, 8 * SIZE
    addi.d  I, I, -1
    blt $r0, I, .L111
    b .L997
    .align 3
#endif

.L112:  //(beta_r == 0.0 && beta_i == 0.0) and !(alpha_r == 0.0 && alpha_i == 0.0)
#ifdef DOUBLE
    vld VX0, X, 0 * SIZE
    vld VX1, X, 2 * SIZE
    vpickev.d x1, VX1, VX0
    vpickod.d x2, VX1, VX0
    vfmul.d x3, VXAI, x2
    vfmul.d x4, VXAI, x1
    vfmsub.d x3, VXAR, x1, x3
    vfmadd.d x4, VXAR, x2, x4
    vilvl.d VX2, x4 ,x3
    vilvh.d VX3, x4, x3
    vst VX2, Y, 0 * SIZE
    vst VX3, Y, 2 * SIZE

    vld VX0, X, 4 * SIZE
    vld VX1, X, 6 * SIZE
    vpickev.d x1, VX1, VX0
    vpickod.d x2, VX1, VX0
    vfmul.d x3, VXAI, x2
    vfmul.d x4, VXAI, x1
    vfmsub.d x3, VXAR, x1, x3
    vfmadd.d x4, VXAR, x2, x4
    vilvl.d VX2, x4 ,x3
    vilvh.d VX3, x4, x3
    vst VX2, Y, 4 * SIZE
    vst VX3, Y, 6 * SIZE
    addi.d X, X, 8 * SIZE
    addi.d Y, Y, 8 * SIZE
    addi.d  I, I, -1
    blt $r0, I, .L112
    b .L997
    .align 3
#else
    vld VX0, X, 0 * SIZE
    vld VX1, X, 4 * SIZE
    vpickev.w x1, VX1, VX0
    vpickod.w x2, VX1, VX0
    vfmul.s x3, VXAI, x2
    vfmul.s x4, VXAI, x1
    vfmsub.s x3, VXAR, x1, x3
    vfmadd.s x4, VXAR, x2, x4
    vilvl.w VX2, x4 ,x3
    vilvh.w VX3, x4, x3
    vst VX2, Y, 0 * SIZE
    vst VX3, Y, 4 * SIZE
    addi.d X, X, 8 * SIZE
    addi.d Y, Y, 8 * SIZE
    addi.d  I, I, -1
    blt $r0, I, .L112
    b .L997
    .align 3
#endif

.L113: //!(beta_r == 0.0 && beta_i == 0.0) and (alpha_r == 0.0 && alpha_i == 0.0)
#ifdef DOUBLE
    vld VX0, Y, 0 * SIZE
    vld VX1, Y, 2 * SIZE
    vpickev.d x1, VX1, VX0
    vpickod.d x2, VX1, VX0
    vfmul.d x3, VXBI, x2
    vfmul.d x4, VXBI, x1
    vfmsub.d x3, VXBR, x1, x3
    vfmadd.d x4, VXBR, x2, x4
    vilvl.d VX2, x4 ,x3
    vilvh.d VX3, x4, x3
    vst VX2, Y, 0 * SIZE
    vst VX3, Y, 2 * SIZE
    vld VX0, Y, 4 * SIZE
    vld VX1, Y, 6 * SIZE
    vpickev.d x1, VX1, VX0
    vpickod.d x2, VX1, VX0
    vfmul.d x3, VXBI, x2
    vfmul.d x4, VXBI, x1
    vfmsub.d x3, VXBR, x1, x3
    vfmadd.d x4, VXBR, x2, x4
    vilvl.d VX2, x4 ,x3
    vilvh.d VX3, x4, x3
    vst VX2, Y, 4 * SIZE
    vst VX3, Y, 6 * SIZE
    addi.d Y, Y, 8 * SIZE
    addi.d  I, I, -1
    blt $r0, I, .L113
    b .L997
    .align 3
#else
    vld VX0, Y, 0 * SIZE
    vld VX1, Y, 4 * SIZE
    vpickev.w x1, VX1, VX0
    vpickod.w x2, VX1, VX0
    vfmul.s x3, VXBI, x2
    vfmul.s x4, VXBI, x1
    vfmsub.s x3, VXBR, x1, x3
    vfmadd.s x4, VXBR, x2, x4
    vilvl.w VX2, x4 ,x3
    vilvh.w VX3, x4, x3
    vst VX2, Y, 0 * SIZE
    vst VX3, Y, 4 * SIZE
    addi.d Y, Y, 8 * SIZE
    addi.d  I, I, -1
    blt $r0, I, .L113
    b .L997
    .align 3
#endif

.L114:
#ifdef DOUBLE
    vld VX0, X, 0 * SIZE
    vld VX1, X, 2 * SIZE
    vld VX2, Y, 0 * SIZE
    vld VX3, Y, 2 * SIZE
    vpickev.d x1, VX1, VX0
    vpickod.d x2, VX1, VX0
    vpickev.d x3, VX3, VX2
    vpickod.d x4, VX3, VX2
    vfmul.d VX0, VXAI, x2
    vfmul.d VX1, VXAI, x1
    vfmul.d VX2, VXBI, x4
    vfmul.d VX3, VXBI, x3
    vfmsub.d VX0, VXAR, x1, VX0
    vfmadd.d VX1, VXAR, x2, VX1
    vfmsub.d VX2, VXBR, x3, VX2
    vfmadd.d VX3, VXBR, x4, VX3
    vfadd.d x3, VX0, VX2
    vfadd.d x4, VX1, VX3
    vilvl.d VX2, x4 ,x3
    vilvh.d VX3, x4, x3
    vst VX2, Y, 0 * SIZE
    vst VX3, Y, 2 * SIZE

    vld VX0, X, 4 * SIZE
    vld VX1, X, 6 * SIZE
    vld VX2, Y, 4 * SIZE
    vld VX3, Y, 6 * SIZE
    vpickev.d x1, VX1, VX0
    vpickod.d x2, VX1, VX0
    vpickev.d x3, VX3, VX2
    vpickod.d x4, VX3, VX2
    vfmul.d VX0, VXAI, x2
    vfmul.d VX1, VXAI, x1
    vfmul.d VX2, VXBI, x4
    vfmul.d VX3, VXBI, x3
    vfmsub.d VX0, VXAR, x1, VX0
    vfmadd.d VX1, VXAR, x2, VX1
    vfmsub.d VX2, VXBR, x3, VX2
    vfmadd.d VX3, VXBR, x4, VX3
    vfadd.d x3, VX0, VX2
    vfadd.d x4, VX1, VX3
    vilvl.d VX2, x4 ,x3
    vilvh.d VX3, x4, x3
    vst VX2, Y, 4 * SIZE
    vst VX3, Y, 6 * SIZE
    addi.d X, X, 8 * SIZE
    addi.d Y, Y, 8 * SIZE
    addi.d  I, I, -1
    blt $r0, I, .L114
    b .L997
    .align 3
#else
    vld VX0, X, 0 * SIZE
    vld VX1, X, 4 * SIZE
    vld VX2, Y, 0 * SIZE
    vld VX3, Y, 4 * SIZE
    vpickev.w x1, VX1, VX0
    vpickod.w x2, VX1, VX0
    vpickev.w x3, VX3, VX2
    vpickod.w x4, VX3, VX2
    vfmul.s VX0, VXAI, x2
    vfmul.s VX1, VXAI, x1
    vfmul.s VX2, VXBI, x4
    vfmul.s VX3, VXBI, x3
    vfmsub.s VX0, VXAR, x1, VX0
    vfmadd.s VX1, VXAR, x2, VX1
    vfmsub.s VX2, VXBR, x3, VX2
    vfmadd.s VX3, VXBR, x4, VX3
    vfadd.s x3, VX0, VX2
    vfadd.s x4, VX1, VX3
    vilvl.w VX2, x4 ,x3
    vilvh.w VX3, x4, x3
    vst VX2, Y, 0 * SIZE
    vst VX3, Y, 4 * SIZE
    addi.d X, X, 8 * SIZE
    addi.d Y, Y, 8 * SIZE
    addi.d  I, I, -1
    blt $r0, I, .L114
    b .L997
    .align 3
#endif

.L12: // INCX==1 and INCY!=1
    bge $r0, I, .L997
    move YY, Y
    .align 3

.L121:
#ifdef DOUBLE
    vld VX0, X, 0 * SIZE
    vld VX1, X, 2 * SIZE
    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    vinsgr2vr.d x3, t1, 0
    vinsgr2vr.d x4, t2, 0
    vinsgr2vr.d x3, t3, 1
    vinsgr2vr.d x4, t4, 1
    add.d Y, Y, INCY
    vpickev.d x1, VX1, VX0
    vpickod.d x2, VX1, VX0
    vfmul.d VX0, VXAI, x2
    vfmul.d VX1, VXAI, x1
    vfmul.d VX2, VXBI, x4
    vfmul.d VX3, VXBI, x3
    vfmsub.d VX0, VXAR, x1, VX0
    vfmadd.d VX1, VXAR, x2, VX1
    vfmsub.d VX2, VXBR, x3, VX2
    vfmadd.d VX3, VXBR, x4, VX3
    vfadd.d x3, VX0, VX2
    vfadd.d x4, VX1, VX3
    vstelm.d x3, YY, 0 * SIZE, 0
    vstelm.d x4, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    vstelm.d x3, YY, 0 * SIZE, 1
    vstelm.d x4, YY, 1 * SIZE, 1
    add.d YY, YY, INCY

    vld VX0, X, 4 * SIZE
    vld VX1, X, 6 * SIZE
    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    vinsgr2vr.d x3, t1, 0
    vinsgr2vr.d x4, t2, 0
    vinsgr2vr.d x3, t3, 1
    vinsgr2vr.d x4, t4, 1
    add.d Y, Y, INCY
    vpickev.d x1, VX1, VX0
    vpickod.d x2, VX1, VX0
    vfmul.d VX0, VXAI, x2
    vfmul.d VX1, VXAI, x1
    vfmul.d VX2, VXBI, x4
    vfmul.d VX3, VXBI, x3
    vfmsub.d VX0, VXAR, x1, VX0
    vfmadd.d VX1, VXAR, x2, VX1
    vfmsub.d VX2, VXBR, x3, VX2
    vfmadd.d VX3, VXBR, x4, VX3
    vfadd.d x3, VX0, VX2
    vfadd.d x4, VX1, VX3
    addi.d  I, I, -1
    vstelm.d x3, YY, 0 * SIZE, 0
    vstelm.d x4, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    vstelm.d x3, YY, 0 * SIZE, 1
    vstelm.d x4, YY, 1 * SIZE, 1
    add.d YY, YY, INCY
    addi.d X, X, 8 * SIZE
    blt $r0, I, .L121
    b .L997
    .align 3
#else
    vld VX0, X, 0 * SIZE
    ld.w t1, Y, 0 * SIZE
    ld.w t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.w t3, Y, 0 * SIZE
    ld.w t4, Y, 1 * SIZE
    add.d Y, Y, INCY
    vinsgr2vr.w x3, t1, 0
    vinsgr2vr.w x4, t2, 0
    vinsgr2vr.w x3, t3, 1
    vinsgr2vr.w x4, t4, 1

    vld VX1, X, 4 * SIZE
    ld.w t1, Y, 0 * SIZE
    ld.w t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.w t3, Y, 0 * SIZE
    ld.w t4, Y, 1 * SIZE
    vinsgr2vr.w x3, t1, 2
    vinsgr2vr.w x4, t2, 2
    vinsgr2vr.w x3, t3, 3
    vinsgr2vr.w x4, t4, 3
    add.d Y, Y, INCY

    vpickev.w x1, VX1, VX0
    vpickod.w x2, VX1, VX0
    vfmul.s VX0, VXAI, x2
    vfmul.s VX1, VXAI, x1
    vfmul.s VX2, VXBI, x4
    vfmul.s VX3, VXBI, x3
    vfmsub.s VX0, VXAR, x1, VX0
    vfmadd.s VX1, VXAR, x2, VX1
    vfmsub.s VX2, VXBR, x3, VX2
    vfmadd.s VX3, VXBR, x4, VX3
    vfadd.s x3, VX0, VX2
    vfadd.s x4, VX1, VX3
    addi.d  I, I, -1
    vstelm.w x3, YY, 0 * SIZE, 0
    vstelm.w x4, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    vstelm.w x3, YY, 0 * SIZE, 1
    vstelm.w x4, YY, 1 * SIZE, 1
    add.d YY, YY, INCY
    vstelm.w x3, YY, 0 * SIZE, 2
    vstelm.w x4, YY, 1 * SIZE, 2
    add.d YY, YY, INCY
    vstelm.w x3, YY, 0 * SIZE, 3
    vstelm.w x4, YY, 1 * SIZE, 3
    add.d YY, YY, INCY
    addi.d X, X, 8 * SIZE
    blt $r0, I, .L121
    b .L997
    .align 3
#endif

.L21:// INCX!=1 and INCY==1
    bge $r0, I, .L997
    .align 3

.L211:
#ifdef DOUBLE
    vld VX2, Y, 0 * SIZE
    vld VX3, Y, 2 * SIZE
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    vinsgr2vr.d x1, t1, 0
    vinsgr2vr.d x2, t2, 0
    vinsgr2vr.d x1, t3, 1
    vinsgr2vr.d x2, t4, 1
    add.d X, X, INCX
    vpickev.d x3, VX3, VX2
    vpickod.d x4, VX3, VX2
    vfmul.d VX0, VXAI, x2
    vfmul.d VX1, VXAI, x1
    vfmul.d VX2, VXBI, x4
    vfmul.d VX3, VXBI, x3
    vfmsub.d VX0, VXAR, x1, VX0
    vfmadd.d VX1, VXAR, x2, VX1
    vfmsub.d VX2, VXBR, x3, VX2
    vfmadd.d VX3, VXBR, x4, VX3
    vfadd.d x3, VX0, VX2
    vfadd.d x4, VX1, VX3
    vilvl.d VX2, x4 ,x3
    vilvh.d VX3, x4, x3
    vst VX2, Y, 0 * SIZE
    vst VX3, Y, 2 * SIZE

    vld VX2, Y, 4 * SIZE
    vld VX3, Y, 6 * SIZE
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    vinsgr2vr.d x1, t1, 0
    vinsgr2vr.d x2, t2, 0
    vinsgr2vr.d x1, t3, 1
    vinsgr2vr.d x2, t4, 1
    add.d X, X, INCX
    vpickev.d x3, VX3, VX2
    vpickod.d x4, VX3, VX2
    vfmul.d VX0, VXAI, x2
    vfmul.d VX1, VXAI, x1
    vfmul.d VX2, VXBI, x4
    vfmul.d VX3, VXBI, x3
    vfmsub.d VX0, VXAR, x1, VX0
    vfmadd.d VX1, VXAR, x2, VX1
    vfmsub.d VX2, VXBR, x3, VX2
    vfmadd.d VX3, VXBR, x4, VX3
    vfadd.d x3, VX0, VX2
    vfadd.d x4, VX1, VX3
    vilvl.d VX2, x4 ,x3
    vilvh.d VX3, x4, x3
    addi.d  I, I, -1
    vst VX3, Y, 4 * SIZE
    vst VX3, Y, 6 * SIZE
    addi.d Y, Y, 8 * SIZE
    blt $r0, I, .L211
    b .L997
    .align 3
#else
    vld VX2, Y, 0 * SIZE
    ld.w t1, X, 0 * SIZE
    ld.w t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    ld.w t4, X, 1 * SIZE
    add.d X, X, INCX
    vinsgr2vr.w x1, t1, 0
    vinsgr2vr.w x2, t2, 0
    vinsgr2vr.w x1, t3, 1
    vinsgr2vr.w x2, t4, 1
    vld VX3, Y, 4 * SIZE
    ld.w t1, X, 0 * SIZE
    ld.w t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    ld.w t4, X, 1 * SIZE
    vinsgr2vr.w x1, t1, 2
    vinsgr2vr.w x2, t2, 2
    vinsgr2vr.w x1, t3, 3
    vinsgr2vr.w x2, t4, 3
    add.d X, X, INCX

    vpickev.w x3, VX3, VX2
    vpickod.w x4, VX3, VX2
    vfmul.s VX0, VXAI, x2
    vfmul.s VX1, VXAI, x1
    vfmul.s VX2, VXBI, x4
    vfmul.s VX3, VXBI, x3
    vfmsub.s VX0, VXAR, x1, VX0
    vfmadd.s VX1, VXAR, x2, VX1
    vfmsub.s VX2, VXBR, x3, VX2
    vfmadd.s VX3, VXBR, x4, VX3
    vfadd.s x3, VX0, VX2
    vfadd.s x4, VX1, VX3
    vilvl.w VX2, x4 ,x3
    vilvh.w VX3, x4, x3
    addi.d  I, I, -1
    vst VX2, Y, 0 * SIZE
    vst VX3, Y, 4 * SIZE
    addi.d Y, Y, 8 * SIZE
    blt $r0, I, .L211
    b .L997
    .align 3
#endif

.L22:
    bge $r0, I, .L997
    move YY, Y
#ifdef DOUBLE
    fcmp.ceq.d $fcc0, BETAR, a1
    fcmp.ceq.d $fcc1, BETAI, a1
    fcmp.ceq.d $fcc2, ALPHAR, a1
    fcmp.ceq.d $fcc3, ALPHAI, a1
#else
    fcmp.ceq.s $fcc0, BETAR, a1
    fcmp.ceq.s $fcc1, BETAI, a1
    fcmp.ceq.s $fcc2, ALPHAR, a1
    fcmp.ceq.s $fcc3, ALPHAI, a1
#endif
    bceqz $fcc0, .L23
    bceqz $fcc1, .L23
    b .L24
    .align 3

.L23:
    bceqz $fcc2, .L224
    bceqz $fcc3, .L224 //!(beta_r == 0.0 && beta_i == 0.0) and !(alpha_r == 0.0 && alpha_i == 0.0)
    b .L223 //!(beta_r == 0.0 && beta_i == 0.0) and (alpha_r == 0.0 && alpha_i == 0.0)
    .align 3

.L24:
    bceqz $fcc2, .L222
    bceqz $fcc3, .L222 //(beta_r == 0.0 && beta_i == 0.0) and !(alpha_r == 0.0 && alpha_i == 0.0)
    b .L221 //(beta_r == 0.0 && beta_i == 0.0) and (alpha_r == 0.0 && alpha_i == 0.0)
    .align 3

.L221:  //(beta_r == 0.0 && beta_i == 0.0) and (alpha_r == 0.0 && alpha_i == 0.0)
#ifdef DOUBLE
    vstelm.d VXZ, Y, 0, 0
    vstelm.d VXZ, Y, 0, 0
    add.d Y, Y, INCY
    vstelm.d VXZ, Y, 0, 0
    vstelm.d VXZ, Y, 0, 0
    add.d Y, Y, INCY
    vstelm.d VXZ, Y, 0, 0
    vstelm.d VXZ, Y, 0, 0
    add.d Y, Y, INCY
    vstelm.d VXZ, Y, 0, 0
    vstelm.d VXZ, Y, 0, 0
    add.d Y, Y, INCY
    addi.d I, I, -1
    blt $r0, I, .L221
    b .L997
    .align 3
#else
    vstelm.w VXZ, Y, 0, 0
    vstelm.w VXZ, Y, 0, 0
    add.d Y, Y, INCY
    vstelm.w VXZ, Y, 0, 0
    vstelm.w VXZ, Y, 0, 0
    add.d Y, Y, INCY
    vstelm.w VXZ, Y, 0, 0
    vstelm.w VXZ, Y, 0, 0
    add.d Y, Y, INCY
    vstelm.w VXZ, Y, 0, 0
    vstelm.w VXZ, Y, 0, 0
    add.d Y, Y, INCY
    addi.d I, I, -1
    blt $r0, I, .L221
    b .L997
    .align 3
#endif

.L222:  //(beta_r == 0.0 && beta_i == 0.0) and !(alpha_r == 0.0 && alpha_i == 0.0)
#ifdef DOUBLE
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d x1, t1, 0
    vinsgr2vr.d x2, t2, 0
    vinsgr2vr.d x1, t3, 1
    vinsgr2vr.d x2, t4, 1
    vfmul.d x3, VXAI, x2
    vfmul.d x4, VXAI, x1
    vfmsub.d x3, VXAR, x1, x3
    vfmadd.d x4, VXAR, x2, x4
    vstelm.d x3, YY, 0 * SIZE, 0
    vstelm.d x4, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    vstelm.d x3, YY, 0 * SIZE, 1
    vstelm.d x4, YY, 1 * SIZE, 1
    add.d YY, YY, INCY

    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    vinsgr2vr.d x1, t1, 0
    vinsgr2vr.d x2, t2, 0
    vinsgr2vr.d x1, t3, 1
    vinsgr2vr.d x2, t4, 1
    add.d X, X, INCX
    vfmul.d x3, VXAI, x2
    vfmul.d x4, VXAI, x1
    vfmsub.d x3, VXAR, x1, x3
    vfmadd.d x4, VXAR, x2, x4
    addi.d  I, I, -1
    vstelm.d x3, YY, 0 * SIZE, 0
    vstelm.d x4, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    vstelm.d x3, YY, 0 * SIZE, 1
    vstelm.d x4, YY, 1 * SIZE, 1
    add.d YY, YY, INCY
    blt $r0, I, .L222
    b .L997
    .align 3
#else
    ld.w t1, X, 0 * SIZE
    ld.w t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    ld.w t4, X, 1 * SIZE
    add.d X, X, INCX
    vinsgr2vr.w x1, t1, 0
    vinsgr2vr.w x2, t2, 0
    vinsgr2vr.w x1, t3, 1
    vinsgr2vr.w x2, t4, 1

    ld.w t1, X, 0 * SIZE
    ld.w t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    ld.w t4, X, 1 * SIZE
    vinsgr2vr.w x1, t1, 2
    vinsgr2vr.w x2, t2, 2
    vinsgr2vr.w x1, t3, 3
    vinsgr2vr.w x2, t4, 3
    add.d X, X, INCX
    vfmul.s x3, VXAI, x2
    vfmul.s x4, VXAI, x1
    vfmsub.s x3, VXAR, x1, x3
    vfmadd.s x4, VXAR, x2, x4
    addi.d  I, I, -1
    vstelm.w x3, YY, 0 * SIZE, 0
    vstelm.w x4, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    vstelm.w x3, YY, 0 * SIZE, 1
    vstelm.w x4, YY, 1 * SIZE, 1
    add.d YY, YY, INCY
    vstelm.w x3, YY, 0 * SIZE, 2
    vstelm.w x4, YY, 1 * SIZE, 2
    add.d YY, YY, INCY
    vstelm.w x3, YY, 0 * SIZE, 3
    vstelm.w x4, YY, 1 * SIZE, 3
    add.d YY, YY, INCY
    blt $r0, I, .L222
    b .L997
    .align 3
#endif

.L223:
#ifdef DOUBLE
    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    vinsgr2vr.d x1, t1, 0
    vinsgr2vr.d x2, t2, 0
    vinsgr2vr.d x1, t3, 1
    vinsgr2vr.d x2, t4, 1
    add.d Y, Y, INCY
    vfmul.d x3, VXBI, x2
    vfmul.d x4, VXBI, x1
    vfmsub.d x3, VXBR, x1, x3
    vfmadd.d x4, VXBR, x2, x4
    vstelm.d x3, YY, 0 * SIZE, 0
    vstelm.d x4, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    vstelm.d x3, YY, 0 * SIZE, 1
    vstelm.d x4, YY, 1 * SIZE, 1
    add.d YY, YY, INCY
    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    vinsgr2vr.d x1, t1, 0
    vinsgr2vr.d x2, t2, 0
    vinsgr2vr.d x1, t3, 1
    vinsgr2vr.d x2, t4, 1
    add.d Y, Y, INCY
    vfmul.d x3, VXBI, x2
    vfmul.d x4, VXBI, x1
    vfmsub.d x3, VXBR, x1, x3
    vfmadd.d x4, VXBR, x2, x4
    addi.d  I, I, -1
    vstelm.d x3, YY, 0 * SIZE, 0
    vstelm.d x4, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    vstelm.d x3, YY, 0 * SIZE, 1
    vstelm.d x4, YY, 1 * SIZE, 1
    add.d YY, YY, INCY
    blt $r0, I, .L223
    b .L997
    .align 3
#else
    ld.w t1, Y, 0 * SIZE
    ld.w t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.w t3, Y, 0 * SIZE
    ld.w t4, Y, 1 * SIZE
    add.d Y, Y, INCY
    vinsgr2vr.w x1, t1, 0
    vinsgr2vr.w x2, t2, 0
    vinsgr2vr.w x1, t3, 1
    vinsgr2vr.w x2, t4, 1

    ld.w t1, Y, 0 * SIZE
    ld.w t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.w t3, Y, 0 * SIZE
    ld.w t4, Y, 1 * SIZE
    vinsgr2vr.w x1, t1, 2
    vinsgr2vr.w x2, t2, 2
    vinsgr2vr.w x1, t3, 3
    vinsgr2vr.w x2, t4, 3
    add.d Y, Y, INCY
    vfmul.s x3, VXBI, x2
    vfmul.s x4, VXBI, x1
    vfmsub.s x3, VXBR, x1, x3
    vfmadd.s x4, VXBR, x2, x4

    addi.d  I, I, -1
    vstelm.w x3, YY, 0 * SIZE, 0
    vstelm.w x4, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    vstelm.w x3, YY, 0 * SIZE, 1
    vstelm.w x4, YY, 1 * SIZE, 1
    add.d YY, YY, INCY
    vstelm.w x3, YY, 0 * SIZE, 2
    vstelm.w x4, YY, 1 * SIZE, 2
    add.d YY, YY, INCY
    vstelm.w x3, YY, 0 * SIZE, 3
    vstelm.w x4, YY, 1 * SIZE, 3
    add.d YY, YY, INCY
    blt $r0, I, .L223
    b .L997
    .align 3
#endif

.L224:
#ifdef DOUBLE
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d x1, t1, 0
    vinsgr2vr.d x2, t2, 0
    vinsgr2vr.d x1, t3, 1
    vinsgr2vr.d x2, t4, 1
    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    vinsgr2vr.d x3, t1, 0
    vinsgr2vr.d x4, t2, 0
    vinsgr2vr.d x3, t3, 1
    vinsgr2vr.d x4, t4, 1
    add.d Y, Y, INCY
    vfmul.d VX0, VXAI, x2
    vfmul.d VX1, VXAI, x1
    vfmul.d VX2, VXBI, x4
    vfmul.d VX3, VXBI, x3
    vfmsub.d VX0, VXAR, x1, VX0
    vfmadd.d VX1, VXAR, x2, VX1
    vfmsub.d VX2, VXBR, x3, VX2
    vfmadd.d VX3, VXBR, x4, VX3
    vfadd.d x3, VX0, VX2
    vfadd.d x4, VX1, VX3
    vstelm.d x3, YY, 0 * SIZE, 0
    vstelm.d x4, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    vstelm.d x3, YY, 0 * SIZE, 1
    vstelm.d x4, YY, 1 * SIZE, 1
    add.d YY, YY, INCY

    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d x1, t1, 0
    vinsgr2vr.d x2, t2, 0
    vinsgr2vr.d x1, t3, 1
    vinsgr2vr.d x2, t4, 1
    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    vinsgr2vr.d x3, t1, 0
    vinsgr2vr.d x4, t2, 0
    vinsgr2vr.d x3, t3, 1
    vinsgr2vr.d x4, t4, 1
    add.d Y, Y, INCY
    vfmul.d VX0, VXAI, x2
    vfmul.d VX1, VXAI, x1
    vfmul.d VX2, VXBI, x4
    vfmul.d VX3, VXBI, x3
    vfmsub.d VX0, VXAR, x1, VX0
    vfmadd.d VX1, VXAR, x2, VX1
    vfmsub.d VX2, VXBR, x3, VX2
    vfmadd.d VX3, VXBR, x4, VX3
    vfadd.d x3, VX0, VX2
    vfadd.d x4, VX1, VX3
    vstelm.d x3, YY, 0 * SIZE, 0
    vstelm.d x4, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    vstelm.d x3, YY, 0 * SIZE, 1
    vstelm.d x4, YY, 1 * SIZE, 1
    add.d YY, YY, INCY
    addi.d  I, I, -1
    blt $r0, I, .L224
    b .L997
    .align 3
#else
    ld.w t1, X, 0 * SIZE
    ld.w t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    ld.w t4, X, 1 * SIZE
    add.d X, X, INCX
    vinsgr2vr.w x1, t1, 0
    vinsgr2vr.w x2, t2, 0
    vinsgr2vr.w x1, t3, 1
    vinsgr2vr.w x2, t4, 1
    ld.w t1, X, 0 * SIZE
    ld.w t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    ld.w t4, X, 1 * SIZE
    add.d X, X, INCX
    vinsgr2vr.w x1, t1, 2
    vinsgr2vr.w x2, t2, 2
    vinsgr2vr.w x1, t3, 3
    vinsgr2vr.w x2, t4, 3

    ld.w t1, Y, 0 * SIZE
    ld.w t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.w t3, Y, 0 * SIZE
    ld.w t4, Y, 1 * SIZE
    add.d Y, Y, INCY
    vinsgr2vr.w x3, t1, 0
    vinsgr2vr.w x4, t2, 0
    vinsgr2vr.w x3, t3, 1
    vinsgr2vr.w x4, t4, 1
    ld.w t1, Y, 0 * SIZE
    ld.w t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.w t3, Y, 0 * SIZE
    ld.w t4, Y, 1 * SIZE
    vinsgr2vr.w x3, t1, 2
    vinsgr2vr.w x4, t2, 2
    vinsgr2vr.w x3, t3, 3
    vinsgr2vr.w x4, t4, 3
    add.d Y, Y, INCY
    vfmul.s VX0, VXAI, x2
    vfmul.s VX1, VXAI, x1
    vfmul.s VX2, VXBI, x4
    vfmul.s VX3, VXBI, x3
    vfmsub.s VX0, VXAR, x1, VX0
    vfmadd.s VX1, VXAR, x2, VX1
    vfmsub.s VX2, VXBR, x3, VX2
    vfmadd.s VX3, VXBR, x4, VX3
    vfadd.s x3, VX0, VX2
    vfadd.s x4, VX1, VX3
    addi.d  I, I, -1

    vstelm.w x3, YY, 0 * SIZE, 0
    vstelm.w x4, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    vstelm.w x3, YY, 0 * SIZE, 1
    vstelm.w x4, YY, 1 * SIZE, 1
    add.d YY, YY, INCY
    vstelm.w x3, YY, 0 * SIZE, 2
    vstelm.w x4, YY, 1 * SIZE, 2
    add.d YY, YY, INCY
    vstelm.w x3, YY, 0 * SIZE, 3
    vstelm.w x4, YY, 1 * SIZE, 3
    add.d YY, YY, INCY
    blt $r0, I, .L224
    b .L997
    .align 3
#endif

.L997:
    andi I, N, 3
    bge $r0, I, .L999
    .align 3

.L998:
#ifdef DOUBLE
    fld.d a1, X, 0 * SIZE
    fld.d a2, X, 1 * SIZE
    fld.d a3, Y, 0 * SIZE
    fld.d a4, Y, 1 * SIZE
    addi.d I, I, -1
    fmul.d s1, ALPHAI, a2
    fmul.d s2, ALPHAI, a1
    fmul.d s3, BETAI, a4
    fmul.d s4, BETAI, a3
    fmsub.d s1, ALPHAR, a1, s1
    fmadd.d s2, a2, ALPHAR, s2
    fmsub.d s3, BETAR, a3, s3
    fmadd.d s4, a4, BETAR, s4
    fadd.d s3, s3, s1
    fadd.d s4, s4, s2
    fst.d s3, Y, 0 * SIZE
    fst.d s4, Y, 1 * SIZE
    add.d X, X, INCX
    add.d Y, Y, INCY
    blt $r0, I, .L998
    .align 3
#else
    fld.s a1, X, 0 * SIZE
    fld.s a2, X, 1 * SIZE
    fld.s a3, Y, 0 * SIZE
    fld.s a4, Y, 1 * SIZE
    addi.d I, I, -1
    fmul.s s1, ALPHAI, a2
    fmul.s s2, ALPHAI, a1
    fmul.s s3, BETAI, a4
    fmul.s s4, BETAI, a3
    fmsub.s s1, ALPHAR, a1, s1
    fmadd.s s2, a2, ALPHAR, s2
    fmsub.s s3, BETAR, a3, s3
    fmadd.s s4, a4, BETAR, s4
    fadd.s s3, s3, s1
    fadd.s s4, s4, s2
    fst.s s3, Y, 0 * SIZE
    fst.s s4, Y, 1 * SIZE
    add.d X, X, INCX
    add.d Y, Y, INCY
    blt $r0, I, .L998
    .align 3
#endif
.L999:
    move $r4, $r12
    jirl $r0, $r1, 0x0
    .align 3

    EPILOGUE
