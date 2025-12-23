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
#define VX0    $xr8
#define VX1    $xr20
#define VX2    $xr21
#define VX3    $xr22
#define VXAR   $xr23
#define VXAI   $xr19
#define VXBR   $xr14
#define VXBI   $xr13
#define VXZ    $xr12
#define x1     $xr18
#define x2     $xr17
#define x3     $xr16
#define x4     $xr15

    PROLOGUE

    bge $r0, N, .L999
    movgr2fr.d a1, $r0
    FFINT a1, a1
    slli.d  INCX, INCX, ZBASE_SHIFT
    slli.d  INCY, INCY, ZBASE_SHIFT
    MTG t1, ALPHAR
    MTG t2, ALPHAI
    MTG t3, BETAR
    MTG t4, BETAI
#ifdef DOUBLE
    xvreplgr2vr.d VXAR, t1
    xvreplgr2vr.d VXAI, t2
    xvreplgr2vr.d VXBR, t3
    xvreplgr2vr.d VXBI, t4
#else
    xvreplgr2vr.w VXAR, t1
    xvreplgr2vr.w VXAI, t2
    xvreplgr2vr.w VXBR, t3
    xvreplgr2vr.w VXBI, t4
#endif
    xvxor.v VXZ, VXZ, VXZ
    // If incx == 0 || incy == 0, do one by one
    and TEMP, INCX, INCY
    or  I,    N,    N
    beqz TEMP, .L998

    li.d TEMP, 1
    slli.d  TEMP, TEMP, ZBASE_SHIFT
#ifdef DOUBLE
    srai.d I, N, 2
#else
    srai.d I, N, 3
#endif
    bne INCX, TEMP, .L20
    bne INCY, TEMP, .L12 // INCX==1 and INCY!=1
    b .L11  // INCX==1 and INCY==1
.L20:
    bne INCY, TEMP, .L22 // INCX!=1 and INCY!=1
    b .L21 // INCX!=1 and INCY==1

.L11:
    bge $r0, I, .L997
    CMPEQ $fcc0, BETAR, a1
    CMPEQ $fcc1, BETAI, a1
    CMPEQ $fcc2, ALPHAR, a1
    CMPEQ $fcc3, ALPHAI, a1
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
    xvst VXZ, Y, 0 * SIZE
#ifdef DOUBLE
    xvst VXZ, Y, 4 * SIZE
    addi.d Y, Y, 8 * SIZE
#else
    xvst VXZ, Y, 8 * SIZE
    addi.d Y, Y, 16 * SIZE
#endif
    addi.d  I, I, -1
    blt $r0, I, .L111
    b .L997
    .align 3

.L112:  //(beta_r == 0.0 && beta_i == 0.0) and !(alpha_r == 0.0 && alpha_i == 0.0)
#ifdef DOUBLE
    xvld VX0, X, 0 * SIZE
    xvld VX1, X, 4 * SIZE
    xvpickev.d x1, VX1, VX0
    xvpickod.d x2, VX1, VX0
#else
    xvld VX0, X, 0 * SIZE
    xvld VX1, X, 8 * SIZE
    xvpickev.w x1, VX1, VX0
    xvpickod.w x2, VX1, VX0
#endif
    XVFMUL x3, VXAI, x2
    XVFMUL x4, VXAI, x1
    XVMSUB x3, VXAR, x1, x3
    XVFMADD x4, VXAR, x2, x4
#ifdef DOUBLE
    xvilvl.d VX2, x4 ,x3
    xvilvh.d VX3, x4, x3
    xvst VX2, Y, 0 * SIZE
    xvst VX3, Y, 4 * SIZE
    addi.d X, X, 8 * SIZE
    addi.d Y, Y, 8 * SIZE
#else
    xvilvl.w VX2, x4 ,x3
    xvilvh.w VX3, x4, x3
    xvst VX2, Y, 0 * SIZE
    xvst VX3, Y, 8 * SIZE
    addi.d X, X, 16 * SIZE
    addi.d Y, Y, 16 * SIZE
#endif
    addi.d  I, I, -1
    blt $r0, I, .L112
    b .L997
    .align 3

.L113: //!(beta_r == 0.0 && beta_i == 0.0) and (alpha_r == 0.0 && alpha_i == 0.0)
#ifdef DOUBLE
    xvld VX0, Y, 0 * SIZE
    xvld VX1, Y, 4 * SIZE
    xvpickev.d x1, VX1, VX0
    xvpickod.d x2, VX1, VX0
#else
    xvld VX0, Y, 0 * SIZE
    xvld VX1, Y, 8 * SIZE
    xvpickev.w x1, VX1, VX0
    xvpickod.w x2, VX1, VX0
#endif
    XVFMUL x3, VXBI, x2
    XVFMUL x4, VXBI, x1
    XVMSUB x3, VXBR, x1, x3
    XVFMADD x4, VXBR, x2, x4
#ifdef DOUBLE
    xvilvl.d VX2, x4 ,x3
    xvilvh.d VX3, x4, x3
    xvst VX2, Y, 0 * SIZE
    xvst VX3, Y, 4 * SIZE
    addi.d Y, Y, 8 * SIZE
#else
    xvilvl.w VX2, x4 ,x3
    xvilvh.w VX3, x4, x3
    xvst VX2, Y, 0 * SIZE
    xvst VX3, Y, 8 * SIZE
    addi.d Y, Y, 16 * SIZE
#endif
    addi.d  I, I, -1
    blt $r0, I, .L113
    b .L997
    .align 3

.L114:
#ifdef DOUBLE
    xvld VX0, X, 0 * SIZE
    xvld VX1, X, 4 * SIZE
    xvld VX2, Y, 0 * SIZE
    xvld VX3, Y, 4 * SIZE
    xvpickev.d x1, VX1, VX0
    xvpickod.d x2, VX1, VX0
    xvpickev.d x3, VX3, VX2
    xvpickod.d x4, VX3, VX2
#else
    xvld VX0, X, 0 * SIZE
    xvld VX1, X, 8 * SIZE
    xvld VX2, Y, 0 * SIZE
    xvld VX3, Y, 8 * SIZE
    xvpickev.w x1, VX1, VX0
    xvpickod.w x2, VX1, VX0
    xvpickev.w x3, VX3, VX2
    xvpickod.w x4, VX3, VX2
#endif
    XVFMUL VX0, VXAI, x2
    XVFMUL VX1, VXAI, x1
    XVFMUL VX2, VXBI, x4
    XVFMUL VX3, VXBI, x3
    XVMSUB VX0, VXAR, x1, VX0
    XVFMADD VX1, VXAR, x2, VX1
    XVMSUB VX2, VXBR, x3, VX2
    XVFMADD VX3, VXBR, x4, VX3
    XVFADD x3, VX0, VX2
    XVFADD x4, VX1, VX3
#ifdef DOUBLE
    xvilvl.d VX2, x4 ,x3
    xvilvh.d VX3, x4, x3
    xvst VX2, Y, 0 * SIZE
    xvst VX3, Y, 4 * SIZE
    addi.d X, X, 8 * SIZE
    addi.d Y, Y, 8 * SIZE
#else
    xvilvl.w VX2, x4 ,x3
    xvilvh.w VX3, x4, x3
    xvst VX2, Y, 0 * SIZE
    xvst VX3, Y, 8 * SIZE
    addi.d X, X, 16 * SIZE
    addi.d Y, Y, 16 * SIZE
#endif
    addi.d  I, I, -1
    blt $r0, I, .L114
    b .L997
    .align 3

.L12: // INCX==1 and INCY!=1
    bge $r0, I, .L997
    move YY, Y
    .align 3

.L121:
#ifdef DOUBLE
    xvld VX0, X, 0 * SIZE
    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    add.d Y, Y, INCY
    xvinsgr2vr.d x3, t1, 0
    xvinsgr2vr.d x4, t2, 0
    xvinsgr2vr.d x3, t3, 2
    xvinsgr2vr.d x4, t4, 2

    xvld VX1, X, 4 * SIZE
    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    xvinsgr2vr.d x3, t1, 1
    xvinsgr2vr.d x4, t2, 1
    xvinsgr2vr.d x3, t3, 3
    xvinsgr2vr.d x4, t4, 3
    add.d Y, Y, INCY

    xvpickev.d x1, VX1, VX0
    xvpickod.d x2, VX1, VX0
    xvfmul.d VX0, VXAI, x2
    xvfmul.d VX1, VXAI, x1
    xvfmul.d VX2, VXBI, x4
    xvfmul.d VX3, VXBI, x3
    xvfmsub.d VX0, VXAR, x1, VX0
    xvfmadd.d VX1, VXAR, x2, VX1
    xvfmsub.d VX2, VXBR, x3, VX2
    xvfmadd.d VX3, VXBR, x4, VX3
    xvfadd.d x3, VX0, VX2
    xvfadd.d x4, VX1, VX3
    addi.d  I, I, -1
    xvstelm.d x3, YY, 0 * SIZE, 0
    xvstelm.d x4, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    xvstelm.d x3, YY, 0 * SIZE, 2
    xvstelm.d x4, YY, 1 * SIZE, 2
    add.d YY, YY, INCY
    xvstelm.d x3, YY, 0 * SIZE, 1
    xvstelm.d x4, YY, 1 * SIZE, 1
    add.d YY, YY, INCY
    xvstelm.d x3, YY, 0 * SIZE, 3
    xvstelm.d x4, YY, 1 * SIZE, 3
    add.d YY, YY, INCY
    addi.d X, X, 8 * SIZE
    blt $r0, I, .L121
    b .L997
    .align 3
#else
    xvld VX0, X, 0 * SIZE
    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    add.d Y, Y, INCY
    xvinsgr2vr.w x3, t1, 0
    xvinsgr2vr.w x4, t2, 0
    xvinsgr2vr.w x3, t3, 1
    xvinsgr2vr.w x4, t4, 1
    xvld VX1, X, 8 * SIZE
    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    xvinsgr2vr.w x3, t1, 4
    xvinsgr2vr.w x4, t2, 4
    xvinsgr2vr.w x3, t3, 5
    xvinsgr2vr.w x4, t4, 5
    add.d Y, Y, INCY
    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    add.d Y, Y, INCY
    xvinsgr2vr.w x3, t1, 2
    xvinsgr2vr.w x4, t2, 2
    xvinsgr2vr.w x3, t3, 3
    xvinsgr2vr.w x4, t4, 3
    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    xvinsgr2vr.w x3, t1, 6
    xvinsgr2vr.w x4, t2, 6
    xvinsgr2vr.w x3, t3, 7
    xvinsgr2vr.w x4, t4, 7
    add.d Y, Y, INCY

    xvpickev.w x1, VX1, VX0
    xvpickod.w x2, VX1, VX0
    XVFMUL VX0, VXAI, x2
    XVFMUL VX1, VXAI, x1
    XVFMUL VX2, VXBI, x4
    XVFMUL VX3, VXBI, x3
    XVMSUB VX0, VXAR, x1, VX0
    XVFMADD VX1, VXAR, x2, VX1
    XVMSUB VX2, VXBR, x3, VX2
    XVFMADD VX3, VXBR, x4, VX3
    XVFADD x3, VX0, VX2
    XVFADD x4, VX1, VX3
    addi.d  I, I, -1
    xvstelm.w x3, YY, 0 * SIZE, 0
    xvstelm.w x4, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 1
    xvstelm.w x4, YY, 1 * SIZE, 1
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 4
    xvstelm.w x4, YY, 1 * SIZE, 4
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 5
    xvstelm.w x4, YY, 1 * SIZE, 5
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 2
    xvstelm.w x4, YY, 1 * SIZE, 2
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 3
    xvstelm.w x4, YY, 1 * SIZE, 3
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 6
    xvstelm.w x4, YY, 1 * SIZE, 6
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 7
    xvstelm.w x4, YY, 1 * SIZE, 7
    add.d YY, YY, INCY
    addi.d X, X, 16 * SIZE
    blt $r0, I, .L121
    b .L997
    .align 3
#endif

.L21:// INCX!=1 and INCY==1
    bge $r0, I, .L997
    .align 3

.L211:
#ifdef DOUBLE
    xvld VX2, Y, 0 * SIZE
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.d x1, t1, 0
    xvinsgr2vr.d x2, t2, 0
    xvinsgr2vr.d x1, t3, 2
    xvinsgr2vr.d x2, t4, 2
    xvld VX3, Y, 4 * SIZE
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    xvinsgr2vr.d x1, t1, 1
    xvinsgr2vr.d x2, t2, 1
    xvinsgr2vr.d x1, t3, 3
    xvinsgr2vr.d x2, t4, 3
    add.d X, X, INCX

    xvpickev.d x3, VX3, VX2
    xvpickod.d x4, VX3, VX2
    xvfmul.d VX0, VXAI, x2
    xvfmul.d VX1, VXAI, x1
    xvfmul.d VX2, VXBI, x4
    xvfmul.d VX3, VXBI, x3
    xvfmsub.d VX0, VXAR, x1, VX0
    xvfmadd.d VX1, VXAR, x2, VX1
    xvfmsub.d VX2, VXBR, x3, VX2
    xvfmadd.d VX3, VXBR, x4, VX3
    xvfadd.d x3, VX0, VX2
    xvfadd.d x4, VX1, VX3
    xvilvl.d VX2, x4 ,x3
    xvilvh.d VX3, x4, x3
    addi.d  I, I, -1
    xvst VX2, Y, 0 * SIZE
    xvst VX3, Y, 4 * SIZE
    addi.d Y, Y, 8 * SIZE
    blt $r0, I, .L211
    b .L997
    .align 3
#else
    xvld VX2, Y, 0 * SIZE
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w x1, t1, 0
    xvinsgr2vr.w x2, t2, 0
    xvinsgr2vr.w x1, t3, 1
    xvinsgr2vr.w x2, t4, 1
    xvld VX3, Y, 8 * SIZE
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w x1, t1, 4
    xvinsgr2vr.w x2, t2, 4
    xvinsgr2vr.w x1, t3, 5
    xvinsgr2vr.w x2, t4, 5
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w x1, t1, 2
    xvinsgr2vr.w x2, t2, 2
    xvinsgr2vr.w x1, t3, 3
    xvinsgr2vr.w x2, t4, 3
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    xvinsgr2vr.w x1, t1, 6
    xvinsgr2vr.w x2, t2, 6
    xvinsgr2vr.w x1, t3, 7
    xvinsgr2vr.w x2, t4, 7
    add.d X, X, INCX

    xvpickev.w x3, VX3, VX2
    xvpickod.w x4, VX3, VX2
    XVFMUL VX0, VXAI, x2
    XVFMUL VX1, VXAI, x1
    XVFMUL VX2, VXBI, x4
    XVFMUL VX3, VXBI, x3
    XVMSUB VX0, VXAR, x1, VX0
    XVFMADD VX1, VXAR, x2, VX1
    XVMSUB VX2, VXBR, x3, VX2
    XVFMADD VX3, VXBR, x4, VX3
    XVFADD x3, VX0, VX2
    XVFADD x4, VX1, VX3
    xvilvl.w VX2, x4 ,x3
    xvilvh.w VX3, x4, x3
    addi.d  I, I, -1
    xvst VX2, Y, 0 * SIZE
    xvst VX3, Y, 8 * SIZE
    addi.d Y, Y, 16 * SIZE
    blt $r0, I, .L211
    b .L997
    .align 3
#endif

.L22:
    bge $r0, I, .L997
    move YY, Y
    CMPEQ $fcc0, BETAR, a1
    CMPEQ $fcc1, BETAI, a1
    CMPEQ $fcc2, ALPHAR, a1
    CMPEQ $fcc3, ALPHAI, a1
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
    xvstelm.d VXZ, Y, 0, 0
    xvstelm.d VXZ, Y, 0, 0
    add.d Y, Y, INCY
    xvstelm.d VXZ, Y, 0, 0
    xvstelm.d VXZ, Y, 0, 0
    add.d Y, Y, INCY
    xvstelm.d VXZ, Y, 0, 0
    xvstelm.d VXZ, Y, 0, 0
    add.d Y, Y, INCY
    xvstelm.d VXZ, Y, 0, 0
    xvstelm.d VXZ, Y, 0, 0
    add.d Y, Y, INCY
    addi.d I, I, -1
    blt $r0, I, .L221
    b .L997
    .align 3
#else
    xvstelm.w VXZ, Y, 0, 0
    xvstelm.w VXZ, Y, 0, 0
    add.d Y, Y, INCY
    xvstelm.w VXZ, Y, 0, 0
    xvstelm.w VXZ, Y, 0, 0
    add.d Y, Y, INCY
    xvstelm.w VXZ, Y, 0, 0
    xvstelm.w VXZ, Y, 0, 0
    add.d Y, Y, INCY
    xvstelm.w VXZ, YY, 0, 0
    xvstelm.w VXZ, YY, 0, 0
    add.d Y, Y, INCY
    xvstelm.w VXZ, Y, 0, 0
    xvstelm.w VXZ, Y, 0, 0
    add.d Y, Y, INCY
    xvstelm.w VXZ, Y, 0, 0
    xvstelm.w VXZ, Y, 0, 0
    add.d Y, Y, INCY
    xvstelm.w VXZ, Y, 0, 0
    xvstelm.w VXZ, Y, 0, 0
    add.d Y, Y, INCY
    xvstelm.w VXZ, Y, 0, 0
    xvstelm.w VXZ, Y, 0, 0
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
    xvinsgr2vr.d x1, t1, 0
    xvinsgr2vr.d x2, t2, 0
    xvinsgr2vr.d x1, t3, 1
    xvinsgr2vr.d x2, t4, 1

    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    xvinsgr2vr.d x1, t1, 2
    xvinsgr2vr.d x2, t2, 2
    xvinsgr2vr.d x1, t3, 3
    xvinsgr2vr.d x2, t4, 3
    add.d X, X, INCX
    xvfmul.d x3, VXAI, x2
    xvfmul.d x4, VXAI, x1
    xvfmsub.d x3, VXAR, x1, x3
    xvfmadd.d x4, VXAR, x2, x4
    addi.d  I, I, -1
    xvstelm.d x3, YY, 0 * SIZE, 0
    xvstelm.d x4, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    xvstelm.d x3, YY, 0 * SIZE, 1
    xvstelm.d x4, YY, 1 * SIZE, 1
    add.d YY, YY, INCY
    xvstelm.d x3, YY, 0 * SIZE, 2
    xvstelm.d x4, YY, 1 * SIZE, 2
    add.d YY, YY, INCY
    xvstelm.d x3, YY, 0 * SIZE, 3
    xvstelm.d x4, YY, 1 * SIZE, 3
    add.d YY, YY, INCY
    blt $r0, I, .L222
    move  Y, YY
    b .L997
    .align 3
#else
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w x1, t1, 0
    xvinsgr2vr.w x2, t2, 0
    xvinsgr2vr.w x1, t3, 1
    xvinsgr2vr.w x2, t4, 1
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w x1, t1, 2
    xvinsgr2vr.w x2, t2, 2
    xvinsgr2vr.w x1, t3, 3
    xvinsgr2vr.w x2, t4, 3

    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w x1, t1, 4
    xvinsgr2vr.w x2, t2, 4
    xvinsgr2vr.w x1, t3, 5
    xvinsgr2vr.w x2, t4, 5
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    xvinsgr2vr.w x1, t1, 6
    xvinsgr2vr.w x2, t2, 6
    xvinsgr2vr.w x1, t3, 7
    xvinsgr2vr.w x2, t4, 7
    add.d X, X, INCX
    XVFMUL x3, VXAI, x2
    XVFMUL x4, VXAI, x1
    XVMSUB x3, VXAR, x1, x3
    XVFMADD x4, VXAR, x2, x4
    addi.d  I, I, -1
    xvstelm.w x3, YY, 0 * SIZE, 0
    xvstelm.w x4, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 1
    xvstelm.w x4, YY, 1 * SIZE, 1
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 2
    xvstelm.w x4, YY, 1 * SIZE, 2
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 3
    xvstelm.w x4, YY, 1 * SIZE, 3
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 4
    xvstelm.w x4, YY, 1 * SIZE, 4
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 5
    xvstelm.w x4, YY, 1 * SIZE, 5
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 6
    xvstelm.w x4, YY, 1 * SIZE, 6
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 7
    xvstelm.w x4, YY, 1 * SIZE, 7
    add.d YY, YY, INCY
    blt $r0, I, .L222
    move  Y, YY
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
    add.d Y, Y, INCY
    xvinsgr2vr.d x1, t1, 0
    xvinsgr2vr.d x2, t2, 0
    xvinsgr2vr.d x1, t3, 1
    xvinsgr2vr.d x2, t4, 1

    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    xvinsgr2vr.d x1, t1, 2
    xvinsgr2vr.d x2, t2, 2
    xvinsgr2vr.d x1, t3, 3
    xvinsgr2vr.d x2, t4, 3
    add.d Y, Y, INCY
    xvfmul.d x3, VXBI, x2
    xvfmul.d x4, VXBI, x1
    xvfmsub.d x3, VXBR, x1, x3
    xvfmadd.d x4, VXBR, x2, x4

    addi.d  I, I, -1
    xvstelm.d x3, YY, 0 * SIZE, 0
    xvstelm.d x4, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    xvstelm.d x3, YY, 0 * SIZE, 1
    xvstelm.d x4, YY, 1 * SIZE, 1
    add.d YY, YY, INCY
    xvstelm.d x3, YY, 0 * SIZE, 2
    xvstelm.d x4, YY, 1 * SIZE, 2
    add.d YY, YY, INCY
    xvstelm.d x3, YY, 0 * SIZE, 3
    xvstelm.d x4, YY, 1 * SIZE, 3
    add.d YY, YY, INCY
    blt $r0, I, .L223
    b .L997
    .align 3
#else
    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    add.d Y, Y, INCY
    xvinsgr2vr.w x1, t1, 0
    xvinsgr2vr.w x2, t2, 0
    xvinsgr2vr.w x1, t3, 1
    xvinsgr2vr.w x2, t4, 1
    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    add.d Y, Y, INCY
    xvinsgr2vr.w x1, t1, 2
    xvinsgr2vr.w x2, t2, 2
    xvinsgr2vr.w x1, t3, 3
    xvinsgr2vr.w x2, t4, 3

    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    add.d Y, Y, INCY
    xvinsgr2vr.w x1, t1, 4
    xvinsgr2vr.w x2, t2, 4
    xvinsgr2vr.w x1, t3, 5
    xvinsgr2vr.w x2, t4, 5
    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    xvinsgr2vr.w x1, t1, 6
    xvinsgr2vr.w x2, t2, 6
    xvinsgr2vr.w x1, t3, 7
    xvinsgr2vr.w x2, t4, 7
    add.d Y, Y, INCY

    XVFMUL x3, VXBI, x2
    XVFMUL x4, VXBI, x1
    XVMSUB x3, VXBR, x1, x3
    XVFMADD x4, VXBR, x2, x4
    addi.d  I, I, -1
    xvstelm.w x3, YY, 0 * SIZE, 0
    xvstelm.w x4, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 1
    xvstelm.w x4, YY, 1 * SIZE, 1
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 2
    xvstelm.w x4, YY, 1 * SIZE, 2
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 3
    xvstelm.w x4, YY, 1 * SIZE, 3
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 4
    xvstelm.w x4, YY, 1 * SIZE, 4
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 5
    xvstelm.w x4, YY, 1 * SIZE, 5
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 6
    xvstelm.w x4, YY, 1 * SIZE, 6
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 7
    xvstelm.w x4, YY, 1 * SIZE, 7
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
    xvinsgr2vr.d x1, t1, 0
    xvinsgr2vr.d x2, t2, 0
    xvinsgr2vr.d x1, t3, 1
    xvinsgr2vr.d x2, t4, 1
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.d x1, t1, 2
    xvinsgr2vr.d x2, t2, 2
    xvinsgr2vr.d x1, t3, 3
    xvinsgr2vr.d x2, t4, 3

    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    add.d Y, Y, INCY
    xvinsgr2vr.d x3, t1, 0
    xvinsgr2vr.d x4, t2, 0
    xvinsgr2vr.d x3, t3, 1
    xvinsgr2vr.d x4, t4, 1
    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    xvinsgr2vr.d x3, t1, 2
    xvinsgr2vr.d x4, t2, 2
    xvinsgr2vr.d x3, t3, 3
    xvinsgr2vr.d x4, t4, 3
    add.d Y, Y, INCY
    xvfmul.d VX0, VXAI, x2
    xvfmul.d VX1, VXAI, x1
    xvfmul.d VX2, VXBI, x4
    xvfmul.d VX3, VXBI, x3
    xvfmsub.d VX0, VXAR, x1, VX0
    xvfmadd.d VX1, VXAR, x2, VX1
    xvfmsub.d VX2, VXBR, x3, VX2
    xvfmadd.d VX3, VXBR, x4, VX3
    xvfadd.d x3, VX0, VX2
    xvfadd.d x4, VX1, VX3
    addi.d  I, I, -1

    xvstelm.d x3, YY, 0 * SIZE, 0
    xvstelm.d x4, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    xvstelm.d x3, YY, 0 * SIZE, 1
    xvstelm.d x4, YY, 1 * SIZE, 1
    add.d YY, YY, INCY
    xvstelm.d x3, YY, 0 * SIZE, 2
    xvstelm.d x4, YY, 1 * SIZE, 2
    add.d YY, YY, INCY
    xvstelm.d x3, YY, 0 * SIZE, 3
    xvstelm.d x4, YY, 1 * SIZE, 3
    add.d YY, YY, INCY
    blt $r0, I, .L224
    b .L997
    .align 3
#else
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w x1, t1, 0
    xvinsgr2vr.w x2, t2, 0
    xvinsgr2vr.w x1, t3, 1
    xvinsgr2vr.w x2, t4, 1
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w x1, t1, 2
    xvinsgr2vr.w x2, t2, 2
    xvinsgr2vr.w x1, t3, 3
    xvinsgr2vr.w x2, t4, 3
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w x1, t1, 4
    xvinsgr2vr.w x2, t2, 4
    xvinsgr2vr.w x1, t3, 5
    xvinsgr2vr.w x2, t4, 5
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w x1, t1, 6
    xvinsgr2vr.w x2, t2, 6
    xvinsgr2vr.w x1, t3, 7
    xvinsgr2vr.w x2, t4, 7

    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    add.d Y, Y, INCY
    xvinsgr2vr.w x3, t1, 0
    xvinsgr2vr.w x4, t2, 0
    xvinsgr2vr.w x3, t3, 1
    xvinsgr2vr.w x4, t4, 1
    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    add.d Y, Y, INCY
    xvinsgr2vr.w x3, t1, 2
    xvinsgr2vr.w x4, t2, 2
    xvinsgr2vr.w x3, t3, 3
    xvinsgr2vr.w x4, t4, 3
    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    add.d Y, Y, INCY
    xvinsgr2vr.w x3, t1, 4
    xvinsgr2vr.w x4, t2, 4
    xvinsgr2vr.w x3, t3, 5
    xvinsgr2vr.w x4, t4, 5
    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    xvinsgr2vr.w x3, t1, 6
    xvinsgr2vr.w x4, t2, 6
    xvinsgr2vr.w x3, t3, 7
    xvinsgr2vr.w x4, t4, 7
    add.d Y, Y, INCY

    XVFMUL VX0, VXAI, x2
    XVFMUL VX1, VXAI, x1
    XVFMUL VX2, VXBI, x4
    XVFMUL VX3, VXBI, x3
    XVMSUB VX0, VXAR, x1, VX0
    XVFMADD VX1, VXAR, x2, VX1
    XVMSUB VX2, VXBR, x3, VX2
    XVFMADD VX3, VXBR, x4, VX3
    XVFADD x3, VX0, VX2
    XVFADD x4, VX1, VX3
    addi.d  I, I, -1

    xvstelm.w x3, YY, 0 * SIZE, 0
    xvstelm.w x4, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 1
    xvstelm.w x4, YY, 1 * SIZE, 1
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 2
    xvstelm.w x4, YY, 1 * SIZE, 2
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 3
    xvstelm.w x4, YY, 1 * SIZE, 3
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 4
    xvstelm.w x4, YY, 1 * SIZE, 4
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 5
    xvstelm.w x4, YY, 1 * SIZE, 5
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 6
    xvstelm.w x4, YY, 1 * SIZE, 6
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 7
    xvstelm.w x4, YY, 1 * SIZE, 7
    add.d YY, YY, INCY
    blt $r0, I, .L224
    b .L997
    .align 3
#endif

.L997:
#ifdef DOUBLE
    andi I, N, 3
#else
    andi I, N, 7
#endif
    bge $r0, I, .L999
    .align 3

.L998:
    LD a1, X, 0 * SIZE
    LD a2, X, 1 * SIZE
    LD a3, Y, 0 * SIZE
    LD a4, Y, 1 * SIZE
    addi.d I, I, -1
    MUL s1, ALPHAI, a2
    MUL s2, ALPHAI, a1
    MUL s3, BETAI, a4
    MUL s4, BETAI, a3
    MSUB s1, ALPHAR, a1, s1
    MADD s2, a2, ALPHAR, s2
    MSUB s3, BETAR, a3, s3
    MADD s4, a4, BETAR, s4
    ADD s3, s3, s1
    ADD s4, s4, s2
    ST s3, Y, 0 * SIZE
    ST s4, Y, 1 * SIZE
    add.d X, X, INCX
    add.d Y, Y, INCY
    blt $r0, I, .L998
    .align 3

.L999:
    move $r4, $r12
    jirl $r0, $r1, 0x0
    .align 3

    EPILOGUE
