#define ASSEMBLER
#include "common.h"

#define N $r4
#define ALPHA $f0
#define X  $r7
#define INCX $r8
#define I $r12
#define TEMP $r13
#define t1 $r14
#define t2 $r18
#define t3 $r15
#define t4 $r17
#define XX $r16
#define VX0 $vr12
#define VX1 $vr13
#define VT0 $vr14
#define VT1 $vr15
#define VALPHA $vr19
#define a1 $f8
#define a2 $f23

    PROLOGUE

    bge $r0, N, .L999
    bge $r0, INCX, .L999
    li.d TEMP, 1
    movgr2fr.d a1, $r0
    ffint.d.l a1, a1
    movgr2fr.d a2, TEMP
    ffint.d.l a2, a2
    slli.d TEMP, TEMP, BASE_SHIFT
    slli.d INCX, INCX, BASE_SHIFT
    fcmp.ceq.d $fcc0, ALPHA, a1
    bcnez $fcc0, .L20   //ALPHA==0
    fcmp.ceq.d $fcc0, ALPHA, a2
    bcnez $fcc0, .L999  //ALPHA==1 return
    srai.d I, N, 3
    beq INCX, TEMP, .L30    //ALPHA！=0|1 and INCX==1
    movfr2gr.d TEMP, ALPHA
    vreplgr2vr.d VALPHA, TEMP
    move XX, X
    .align 3

.L10: //ALPHA！=0|1 and INCX!=1
    bge $r0, I, .L32
    .align 3

.L11:
    ld.d t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t2, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX0, t1, 0
    vinsgr2vr.d VX0, t2, 1
    vfmul.d VT0, VX0, VALPHA
    ld.d t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t4, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX1, t3, 0
    vinsgr2vr.d VX1, t4, 1
    vstelm.d VT0, XX, 0, 0
    add.d XX, XX, INCX
    vstelm.d VT0, XX, 0, 1
    add.d XX, XX, INCX
    vfmul.d VT1, VX1, VALPHA
    ld.d t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t2, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX0, t1, 0
    vinsgr2vr.d VX0, t2, 1
    vstelm.d VT1, XX, 0, 0
    add.d XX, XX, INCX
    vstelm.d VT1, XX, 0, 1
    add.d XX, XX, INCX
    vfmul.d VT0, VX0, VALPHA
    ld.d t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t4, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX1, t3, 0
    vinsgr2vr.d VX1, t4, 1
    vstelm.d VT0, XX, 0, 0
    add.d XX, XX, INCX
    vstelm.d VT0, XX, 0, 1
    add.d XX, XX, INCX
    vfmul.d VT1, VX1, VALPHA
    vstelm.d VT1, XX, 0, 0
    add.d XX, XX, INCX
    vstelm.d VT1, XX, 0, 1
    add.d XX, XX, INCX
    addi.d  I, I, -1
    blt $r0, I, .L11
    b .L32
    .align 3

.L20:
    srai.d I, N, 3
    beq INCX, TEMP, .L24
    bge $r0, I, .L22
    .align 3

.L21:
    fst.d a1, X, 0
    add.d X, X, INCX
    fst.d a1, X, 0
    add.d X, X, INCX
    fst.d a1, X, 0
    add.d X, X, INCX
    fst.d a1, X, 0
    add.d X, X, INCX
    fst.d a1, X, 0
    add.d X, X, INCX
    fst.d a1, X, 0
    add.d X, X, INCX
    fst.d a1, X, 0
    add.d X, X, INCX
    fst.d a1, X, 0
    add.d X, X, INCX
    addi.d  I, I, -1
    blt $r0, I, .L21
    .align 3

.L22:
    andi I, N, 7
    bge $r0, I, .L999
    .align 3
.L23:
    fst.d a1, X, 0 * SIZE
    addi.d I, I, -1
    add.d X, X, INCX
    blt $r0, I, .L23
    jirl $r0, $r1, 0
    .align 3

.L24:
    bge $r0, I, .L26 /*N<8 INCX==1*/
    .align 3
.L25:
    vxor.v VX0, VX0, VX0
    vst VX0, X, 0 * SIZE
    vst VX0, X, 2 * SIZE
    vst VX0, X, 4 * SIZE
    vst VX0, X, 6 * SIZE
    addi.d I, I, -1
    addi.d X, X, 8 * SIZE
    blt $r0, I, .L25
    .align 3

.L26:
    andi I, N, 7
    bge $r0, I, .L999
    .align 3
.L27:
    fst.d a1, X, 0 * SIZE
    addi.d I, I, -1
    addi.d X, X, SIZE
    blt $r0, I, .L27
    jirl $r0, $r1, 0
    .align 3

.L30:
    bge $r0, I, .L32/*N<8 INCX==1*/
    movfr2gr.d TEMP, ALPHA
    vreplgr2vr.d VALPHA , TEMP
    .align 3

.L31:
    vld VX0, X, 0 * SIZE
    vld VX1, X, 2 * SIZE
    vfmul.d VT0, VX0, VALPHA
    vfmul.d VT1, VX1, VALPHA
    vld VX0, X, 4 * SIZE
    vst VT0, X, 0 * SIZE
    vst VT1, X, 2 * SIZE
    vfmul.d VT0, VX0, VALPHA
    vld VX1, X, 6 * SIZE
    vst VT0, X, 4 * SIZE
    vfmul.d VT1, VX1, VALPHA
    vst VT1, X, 6 * SIZE
    addi.d I, I, -1
    addi.d X, X, 8 * SIZE
    blt $r0, I, .L31
    .align 3

.L32:
    andi I, N, 7
    bge $r0, I, .L999
    .align 3
.L33:
    fld.d a1, X, 0 * SIZE
    addi.d I, I, -1
    fmul.d a1, ALPHA, a1
    fst.d a1, X, 0 * SIZE
    add.d X, X, INCX
    blt $r0, I, .L33
    jirl $r0, $r1, 0
    .align 3

.L999:
    jirl    $r0, $r1, 0x0

    EPILOGUE
