/*******************************************************************************
Copyright (c) 2015, The OpenBLAS Project
All rights reserved.
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
1. Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in
the documentation and/or other materials provided with the
distribution.
3. Neither the name of the OpenBLAS project nor the names of
its contributors may be used to endorse or promote products
derived from this software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE OPENBLAS PROJECT OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*******************************************************************************/

#define ASSEMBLER
#include "common.h"

/*                   X0          X1          X2          s0         X3        x4       x5           x6 */
/*int CNAME(BLASLONG bm,BLASLONG bn,BLASLONG bk,FLOAT alpha0,FLOAT* ba,FLOAT* bb,FLOAT* C,BLASLONG ldc )*/

#define origM		x0
#define origN		x1
#define origK		x2
#define origPA		x3
#define origPB		x4
#define pC		x5
#define LDC		x6
#define temp		x7
#define counterL	x8
#define counterI	x9
#define counterJ	x10
#define pB		x11
#define pCRow0		x12
#define pCRow1		x13
#define pCRow2		x14

#define lanes		x15
#define pA		x16
#define alpha		w17

#define alpha0		s10
#define alphaZ		z2.s

#define A_PRE_SIZE	1536
#define B_PRE_SIZE	512
#define C_PRE_SIZE	128

// 00 origM
// 01 origN
// 02 origK
// 03 origPA
// 04 origPB
// 05 pC
// 06 origLDC -> LDC
// 07 temp
// 08 counterL
// 09 counterI
// 10 counterJ
// 11 pB
// 12 pCRow0
// 13 pCRow1
// 14 pCRow2
// 15 lanes
// 16 pA
// 17 
// 18 must save
// 19 must save
// 20 must save
// 21 must save
// 22 must save
// 23 must save
// 24 must save
// 25 must save
// 26 must save
// 27 must save
// 28 must save
// 29 frame
// 30 link
// 31 sp

//v00 ALPHA -> pA0_0
//v01 pA0_1
//v02 ALPHA0
//v03 
//v04 
//v05 
//v06 
//v07 
//v08 must save pB0_0
//v09 must save pB0_1
//v10 must save pB0_2 
//v11 must save pB0_3
//v12 must save pB0_4
//v13 must save pB0_5
//v14 must save pB0_6
//v15 must save pB0_7
//v16 must save C0
//v17 must save C1
//v18 must save C2
//v19 must save C3
//v20 must save C4
//v21 must save C5
//v22 must save C6
//v23 must save C7

/*******************************************************************************
* Macro definitions
*******************************************************************************/

.macro INITv1x8
    dup         z16.s, #0
    dup         z17.s, #0
    dup         z18.s, #0
    dup         z19.s, #0
    dup         z20.s, #0
    dup         z21.s, #0
    dup         z22.s, #0
    dup         z23.s, #0
.endm

.macro KERNELv1x8_I
    ld1w  z0.s, p1/z, [pA] 
    ld1w  z1.s, p1/z, [pA, lanes, lsl #2]   // next one
	add	pA, pA, lanes, lsl #3	// pA = pA + lanes * 2 * 4

    ld1rw  z8.s, p0/z,  [pB]
    ld1rw  z9.s, p0/z,  [pB, 4]
    ld1rw  z10.s, p0/z, [pB, 8]
    ld1rw  z11.s, p0/z, [pB, 12]
    ld1rw  z12.s, p0/z, [pB, 16]
    ld1rw  z13.s, p0/z, [pB, 20]
    ld1rw  z14.s, p0/z, [pB, 24]
    ld1rw  z15.s, p0/z, [pB, 28]

    add pB, pB, 32

    fmla z16.s, p1/m, z0.s, z8.s
    ld1rw  z8.s, p0/z,  [pB]
    fmla z17.s, p1/m, z0.s, z9.s
    ld1rw  z9.s, p0/z,  [pB, 4]
    fmla z18.s, p1/m, z0.s, z10.s
    ld1rw  z10.s, p0/z, [pB, 8]
    fmla z19.s, p1/m, z0.s, z11.s
    ld1rw  z11.s, p0/z, [pB, 12]
    fmla z20.s, p1/m, z0.s, z12.s
	prfm	PLDL1KEEP, [pA, #A_PRE_SIZE]
    ld1rw  z12.s, p0/z, [pB, 16]
    fmla z21.s, p1/m, z0.s, z13.s
    ld1rw  z13.s, p0/z, [pB, 20]
    fmla z22.s, p1/m, z0.s, z14.s
    ld1rw  z14.s, p0/z, [pB, 24]
    fmla z23.s, p1/m, z0.s, z15.s
	prfm	PLDL1KEEP, [pA, #A_PRE_SIZE+64]
    ld1rw  z15.s, p0/z, [pB, 28]

    add pB, pB, 32
.endm

.macro KERNELv1x8_M1
    ld1w  z1.s, p1/z, [pA] 
	add	pA, pA, lanes, lsl #2	// pA = pA + lanes  * 4

    fmla z16.s, p1/m, z0.s, z8.s
    ld1rw  z8.s, p0/z,  [pB]
    fmla z17.s, p1/m, z0.s, z9.s
    ld1rw  z9.s, p0/z,  [pB, 4]
    fmla z18.s, p1/m, z0.s, z10.s
    ld1rw  z10.s, p0/z, [pB, 8]
    fmla z19.s, p1/m, z0.s, z11.s
    ld1rw  z11.s, p0/z, [pB, 12]
    fmla z20.s, p1/m, z0.s, z12.s
	prfm	PLDL1KEEP, [pA, #A_PRE_SIZE]
    ld1rw  z12.s, p0/z, [pB, 16]
    fmla z21.s, p1/m, z0.s, z13.s
    ld1rw  z13.s, p0/z, [pB, 20]
    fmla z22.s, p1/m, z0.s, z14.s
    ld1rw  z14.s, p0/z, [pB, 24]
    fmla z23.s, p1/m, z0.s, z15.s
	prfm	PLDL1KEEP, [pA, #A_PRE_SIZE+64]
    ld1rw  z15.s, p0/z, [pB, 28]

    add pB, pB, 32
.endm

.macro KERNELv1x8_M2
    ld1w  z0.s, p1/z, [pA] 
	add	pA, pA, lanes, lsl #2	// pA = pA + lanes  * 4

    fmla z16.s, p1/m, z1.s, z8.s
    ld1rw  z8.s, p0/z,  [pB]
    fmla z17.s, p1/m, z1.s, z9.s
    ld1rw  z9.s, p0/z,  [pB, 4]
    fmla z18.s, p1/m, z1.s, z10.s
    ld1rw  z10.s, p0/z, [pB, 8]
    fmla z19.s, p1/m, z1.s, z11.s
    ld1rw  z11.s, p0/z, [pB, 12]
    fmla z20.s, p1/m, z1.s, z12.s
    ld1rw  z12.s, p0/z, [pB, 16]
	prfm	PLDL1KEEP, [pB, #B_PRE_SIZE]
    fmla z21.s, p1/m, z1.s, z13.s
    ld1rw  z13.s, p0/z, [pB, 20]
    fmla z22.s, p1/m, z1.s, z14.s
    ld1rw  z14.s, p0/z, [pB, 24]
    fmla z23.s, p1/m, z1.s, z15.s
    ld1rw  z15.s, p0/z, [pB, 28]

    add pB, pB, 32
.endm

.macro KERNELv1x8_E
    fmla z16.s, p1/m, z1.s, z8.s
    fmla z17.s, p1/m, z1.s, z9.s
    fmla z18.s, p1/m, z1.s, z10.s
    fmla z19.s, p1/m, z1.s, z11.s
    fmla z20.s, p1/m, z1.s, z12.s
	prfm	PLDL1KEEP, [pB, #B_PRE_SIZE]
    fmla z21.s, p1/m, z1.s, z13.s
    fmla z22.s, p1/m, z1.s, z14.s
    fmla z23.s, p1/m, z1.s, z15.s
.endm

.macro KERNELv1x8_SUB
    ld1w  z0.s, p1/z, [pA] 
	add	pA, pA, lanes, lsl #2	// pA = pA + lanes  * 4

    ld1rw  z8.s, p0/z,  [pB]
    ld1rw  z9.s, p0/z,  [pB, 4]
    ld1rw  z10.s, p0/z, [pB, 8]
    ld1rw  z11.s, p0/z, [pB, 12]
    ld1rw  z12.s, p0/z, [pB, 16]
    ld1rw  z13.s, p0/z, [pB, 20]
    ld1rw  z14.s, p0/z, [pB, 24]
    ld1rw  z15.s, p0/z, [pB, 28]

    add pB, pB, 32

    fmla z16.s, p1/m, z0.s, z8.s
    fmla z17.s, p1/m, z0.s, z9.s
    fmla z18.s, p1/m, z0.s, z10.s
	prfm	PLDL1KEEP, [pA, #A_PRE_SIZE]
    fmla z19.s, p1/m, z0.s, z11.s
    fmla z20.s, p1/m, z0.s, z12.s
    fmla z21.s, p1/m, z0.s, z13.s
	prfm	PLDL1KEEP, [pB, #B_PRE_SIZE]
    fmla z22.s, p1/m, z0.s, z14.s
    fmla z23.s, p1/m, z0.s, z15.s

.endm

.macro SAVEv1x8

	prfm	PLDL2KEEP, [pCRow0, #C_PRE_SIZE]

	add	pCRow1, pCRow0, LDC
    ld1w  z24.s, p1/z, [pCRow0] 
    fmla z24.s, p1/m, z16.s, alphaZ
    st1w  z24.s, p1, [pCRow0]
	prfm	PLDL2KEEP, [pCRow1, #C_PRE_SIZE]

	add	pCRow2, pCRow1, LDC
    ld1w  z25.s, p1/z, [pCRow1] 
    fmla z25.s, p1/m, z17.s, alphaZ
    st1w  z25.s, p1, [pCRow1]
	prfm	PLDL2KEEP, [pCRow2, #C_PRE_SIZE]

	add	pCRow1, pCRow2, LDC
    ld1w  z26.s, p1/z, [pCRow2] 
    fmla z26.s, p1/m, z18.s, alphaZ
    st1w z26.s, p1, [pCRow2]
	prfm	PLDL2KEEP, [pCRow1, #C_PRE_SIZE]

	add	pCRow2, pCRow1, LDC
    ld1w  z27.s, p1/z, [pCRow1] 
    fmla z27.s, p1/m, z19.s, alphaZ
    st1w  z27.s, p1, [pCRow1]
	prfm	PLDL2KEEP, [pCRow2, #C_PRE_SIZE]

	add	pCRow1, pCRow2, LDC
    ld1w  z28.s, p1/z, [pCRow2] 
    fmla z28.s, p1/m, z20.s, alphaZ
    st1w  z28.s, p1, [pCRow2]
	prfm	PLDL2KEEP, [pCRow1, #C_PRE_SIZE]

	add	pCRow2, pCRow1, LDC
    ld1w  z29.s, p1/z, [pCRow1] 
    fmla z29.s, p1/m, z21.s, alphaZ
    st1w  z29.s, p1, [pCRow1]
	prfm	PLDL2KEEP, [pCRow2, #C_PRE_SIZE]

	add	pCRow1, pCRow2, LDC
    ld1w  z30.s, p1/z, [pCRow2] 
    fmla z30.s, p1/m, z22.s, alphaZ
    st1w  z30.s, p1, [pCRow2]
	prfm	PLDL2KEEP, [pCRow1, #C_PRE_SIZE]

    ld1w  z31.s, p1/z, [pCRow1] 
    fmla z31.s, p1/m, z23.s, alphaZ
    st1w  z31.s, p1, [pCRow1]

	add	pCRow0, pCRow0, lanes, lsl #2	// pC = pC + lanes  * 4

.endm

/******************************************************************************/

.macro INITv1x4
    dup         z16.s, #0
    dup         z17.s, #0
    dup         z18.s, #0
    dup         z19.s, #0
.endm

.macro KERNELv1x4_SUB
    ld1w  z0.s, p1/z, [pA] 
	add	pA, pA, lanes, lsl #2	// pA = pA + lanes * 4

    ld1rw  z8.s, p0/z,  [pB]
    ld1rw  z9.s, p0/z,  [pB, 4]
    ld1rw  z10.s, p0/z, [pB, 8]
    ld1rw  z11.s, p0/z, [pB, 12]

    add pB, pB, 16

    fmla z16.s, p1/m, z0.s, z8.s
    fmla z17.s, p1/m, z0.s, z9.s
	prfm	PLDL1KEEP, [pA, #A_PRE_SIZE]
    fmla z18.s, p1/m, z0.s, z10.s
    fmla z19.s, p1/m, z0.s, z11.s

.endm

.macro SAVEv1x4

	prfm	PLDL2KEEP, [pCRow0, #C_PRE_SIZE]

	add	pCRow1, pCRow0, LDC
    ld1w  z24.s, p1/z, [pCRow0] 
    fmla z24.s, p1/m, z16.s, alphaZ
    st1w  z24.s, p1, [pCRow0]
	prfm	PLDL2KEEP, [pCRow1, #C_PRE_SIZE]

	add	pCRow2, pCRow1, LDC
    ld1w  z25.s, p1/z, [pCRow1] 
    fmla z25.s, p1/m, z17.s, alphaZ
    st1w  z25.s, p1, [pCRow1]
	prfm	PLDL2KEEP, [pCRow2, #C_PRE_SIZE]

	add	pCRow1, pCRow2, LDC
    ld1w  z26.s, p1/z, [pCRow2] 
    fmla z26.s, p1/m, z18.s, alphaZ
    st1w z26.s, p1, [pCRow2]
	prfm	PLDL2KEEP, [pCRow1, #C_PRE_SIZE]

    ld1w  z27.s, p1/z, [pCRow1] 
    fmla z27.s, p1/m, z19.s, alphaZ
    st1w  z27.s, p1, [pCRow1]

	add	pCRow0, pCRow0, lanes, lsl #2	// pC = pC + lanes  * 4

.endm

/******************************************************************************/

.macro INITv1x2
    dup         z16.s, #0
    dup         z17.s, #0
.endm

.macro KERNELv1x2_SUB
    ld1w  z0.s, p1/z, [pA] 
	add	pA, pA, lanes, lsl #2	// pA = pA + lanes * 4

    ld1rw  z8.s, p0/z,  [pB]
    ld1rw  z9.s, p0/z,  [pB, 4]

    add pB, pB, 8

    fmla z16.s, p1/m, z0.s, z8.s
	prfm	PLDL1KEEP, [pA, #A_PRE_SIZE]
    fmla z17.s, p1/m, z0.s, z9.s

.endm

.macro SAVEv1x2

	prfm	PLDL2KEEP, [pCRow0, #C_PRE_SIZE]

	add	pCRow1, pCRow0, LDC
    ld1w  z24.s, p1/z, [pCRow0] 
    fmla z24.s, p1/m, z16.s, alphaZ
    st1w  z24.s, p1, [pCRow0]
	prfm	PLDL2KEEP, [pCRow1, #C_PRE_SIZE]

    ld1w  z25.s, p1/z, [pCRow1] 
    fmla z25.s, p1/m, z17.s, alphaZ
    st1w  z25.s, p1, [pCRow1]

	add	pCRow0, pCRow0, lanes, lsl #2	// pC = pC + lanes  * 4

.endm

/******************************************************************************/

.macro INITv1x1
    dup         z16.s, #0
.endm

.macro KERNELv1x1_SUB
    ld1w  z0.s, p1/z, [pA] 
	add	pA, pA, lanes, lsl #2	// pA = pA + lanes  * 8

    ld1rw  z8.s, p0/z,  [pB]

    add pB, pB, 4

    fmla z16.s, p1/m, z0.s, z8.s
	prfm	PLDL1KEEP, [pA, #A_PRE_SIZE]

.endm

.macro SAVEv1x1

	prfm	PLDL2KEEP, [pCRow0, #C_PRE_SIZE]

    ld1w  z24.s, p1/z, [pCRow0] 
    fmla z24.s, p1/m, z16.s, alphaZ
    st1w  z24.s, p1, [pCRow0]


	add	pCRow0, pCRow0, lanes, lsl #2	// pC = pC + lanes  * 4

.endm


/*******************************************************************************
* End of macro definitions
*******************************************************************************/

	PROLOGUE

	.align 5
	add	sp, sp, #-(11 * 16)
	stp	d8, d9, [sp, #(0 * 16)]
	stp	d10, d11, [sp, #(1 * 16)]
	stp	d12, d13, [sp, #(2 * 16)]
	stp	d14, d15, [sp, #(3 * 16)]
	stp	d16, d17, [sp, #(4 * 16)]
	stp	x18, x19, [sp, #(5 * 16)]
	stp	x20, x21, [sp, #(6 * 16)]
	stp	x22, x23, [sp, #(7 * 16)]
	stp	x24, x25, [sp, #(8 * 16)]
	stp	x26, x27, [sp, #(9 * 16)]
	str	x28, [sp, #(10 * 16)]

	prfm	PLDL1KEEP, [origPB]
	prfm	PLDL1KEEP, [origPA]

	fmov	alpha, s0
	dup	alphaZ, alpha

	lsl	LDC, LDC, #2			// ldc = ldc * 4
    ptrue p0.s                  // create true predicate 

	mov	pB, origPB
// Loop over N
	mov	counterJ, origN
	asr 	counterJ, counterJ, #3		// J = J / 8
	cmp 	counterJ, #0
	ble	.Ldgemm_kernel_L4_BEGIN

/******************************************************************************/
/* Repeat this as long as there are 8 left in N */

	.align 5
.Ldgemm_kernel_L8_BEGIN:
	mov	pCRow0, pC

    add pC, pC, LDC, lsl #3 // add 8 x LDC

	mov	pA, origPA			// pA = start of A array

.Ldgemm_kernel_L8_Mv1_BEGIN:

/* Loop over M is done in an SVE fashion. This has the benefit of the last M%SVE_LEN iterations being done in a single sweep */
    mov counterI, #0
    whilelt p1.s, counterI, origM   
    cntp lanes, p0, p1.s                        // lanes contain number of active SVE lanes in M dimension

	.align 5
.Ldgemm_kernel_L8_Mv1_20:

	mov	pB, origPB
    INITv1x8                     // fill with zeros

	asr 	counterL , origK, #3		// L = K / 8
	cmp	counterL , #2			// is there at least 4 to do?
	blt	.Ldgemm_kernel_L8_Mv1_32

	KERNELv1x8_I
	KERNELv1x8_M2
	KERNELv1x8_M1
	KERNELv1x8_M2
	KERNELv1x8_M1
	KERNELv1x8_M2
	KERNELv1x8_M1
	KERNELv1x8_M2

	subs	counterL, counterL, #2		// subtract 2
	ble	.Ldgemm_kernel_L8_Mv1_22a

	.align 5
.Ldgemm_kernel_L8_Mv1_22:

	KERNELv1x8_M1
	KERNELv1x8_M2
	KERNELv1x8_M1
	KERNELv1x8_M2
	KERNELv1x8_M1
	KERNELv1x8_M2
	KERNELv1x8_M1
	KERNELv1x8_M2

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L8_Mv1_22

	.align 5
.Ldgemm_kernel_L8_Mv1_22a:

	KERNELv1x8_M1
	KERNELv1x8_M2
	KERNELv1x8_M1
	KERNELv1x8_M2
	KERNELv1x8_M1
	KERNELv1x8_M2
	KERNELv1x8_M1
	KERNELv1x8_E

	b	 .Ldgemm_kernel_L8_Mv1_44

	.align 5
.Ldgemm_kernel_L8_Mv1_32:

	tst	counterL, #1
	ble	.Ldgemm_kernel_L8_Mv1_40

	KERNELv1x8_I
	KERNELv1x8_M2
	KERNELv1x8_M1
	KERNELv1x8_M2
	KERNELv1x8_M1
	KERNELv1x8_M2
	KERNELv1x8_M1
	KERNELv1x8_E


	b	.Ldgemm_kernel_L8_Mv1_44

.Ldgemm_kernel_L8_Mv1_40:

	INITv1x8

.Ldgemm_kernel_L8_Mv1_44:

	ands	counterL , origK, #7
	ble	.Ldgemm_kernel_L8_Mv1_100

	.align 5
.Ldgemm_kernel_L8_Mv1_46:

	KERNELv1x8_SUB

	subs	counterL, counterL, #1
	bne	.Ldgemm_kernel_L8_Mv1_46

.Ldgemm_kernel_L8_Mv1_100:
	prfm	PLDL1KEEP, [pA]
	prfm	PLDL1KEEP, [pA, #64]
	prfm	PLDL1KEEP, [origPB]

	SAVEv1x8

.Ldgemm_kernel_L8_Mv1_END:

    incw    counterI
    whilelt p1.s, counterI, origM             //SVE instruction
    cntp lanes, p0, p1.s                        // lanes contain number of active SVE lanes in M dimension
    b.any   .Ldgemm_kernel_L8_Mv1_20   

.Ldgemm_kernel_L8_END:

	lsl	temp, origK, #5 
	add	origPB, origPB, temp		// B = B + K * 8 * 4

	subs	counterJ, counterJ , #1		// j--
	bgt	.Ldgemm_kernel_L8_BEGIN

/******************************************************************************/
/* Repeat the same thing if 4 left in N */

	.align 5
.Ldgemm_kernel_L4_BEGIN:

	mov	counterJ , origN
	tst	counterJ , #4
	ble	.Ldgemm_kernel_L2_BEGIN


	mov	pCRow0, pC

    add pC, pC, LDC, lsl #2 // add 4 x LDC

	mov	pA, origPA			// pA = start of A array

.Ldgemm_kernel_L4_Mv1_BEGIN:

    mov counterI, #0
    whilelt p1.s, counterI, origM               //SVE instruction
    cntp lanes, p0, p1.s

	.align 5
.Ldgemm_kernel_L4_Mv1_20:

	mov	pB, origPB
    INITv1x4                     // fill with zeros

	asr 	counterL , origK, #3		// L = K / 8
	cmp	counterL , #0			// is there at least 4 to do?
	ble	.Ldgemm_kernel_L4_Mv1_44

	.align 5
.Ldgemm_kernel_L4_Mv1_22:

	prfm	PLDL1KEEP, [pB, #B_PRE_SIZE]
	KERNELv1x4_SUB
	KERNELv1x4_SUB
	prfm	PLDL1KEEP, [pB, #B_PRE_SIZE]
	KERNELv1x4_SUB
	KERNELv1x4_SUB
	prfm	PLDL1KEEP, [pB, #B_PRE_SIZE]
	KERNELv1x4_SUB
	KERNELv1x4_SUB
	prfm	PLDL1KEEP, [pB, #B_PRE_SIZE]
	KERNELv1x4_SUB
	KERNELv1x4_SUB

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L4_Mv1_22

.Ldgemm_kernel_L4_Mv1_44:

	ands	counterL , origK, #7
	ble	.Ldgemm_kernel_L4_Mv1_100

	.align 5
.Ldgemm_kernel_L4_Mv1_46:

	prfm	PLDL1KEEP, [pB, #B_PRE_SIZE]
	KERNELv1x4_SUB

	subs	counterL, counterL, #1
	bne	.Ldgemm_kernel_L4_Mv1_46

.Ldgemm_kernel_L4_Mv1_100:
	prfm	PLDL1KEEP, [pA]
	prfm	PLDL1KEEP, [pA, #64]
	prfm	PLDL1KEEP, [origPB]

	SAVEv1x4

.Ldgemm_kernel_L4_Mv1_END:

    incw    counterI
    whilelt p1.s, counterI, origM             //SVE instruction
    cntp lanes, p0, p1.s
    b.any   .Ldgemm_kernel_L4_Mv1_20   


.Ldgemm_kernel_L4_END:
	lsl	temp, origK, #4 
	add	origPB, origPB, temp	// B = B + K * 4 * 4

/******************************************************************************/
/* Repeat the same thing if 2 left in N */

	.align 5
.Ldgemm_kernel_L2_BEGIN:

	mov	counterJ , origN
	tst	counterJ , #2
	ble	.Ldgemm_kernel_L1_BEGIN

	mov	pCRow0, pC

    add pC, pC, LDC, lsl #1 // add 2 x LDC

	mov	pA, origPA			// pA = start of A array

.Ldgemm_kernel_L2_Mv1_BEGIN:

    mov counterI, #0
    whilelt p1.s, counterI, origM               //SVE instruction
    cntp lanes, p0, p1.s

	.align 5
.Ldgemm_kernel_L2_Mv1_20:

	mov	pB, origPB
    INITv1x2                     // fill with zeros

	asr 	counterL , origK, #3		// L = K / 8
	cmp	counterL , #0			// is there at least 4 to do?
	ble	.Ldgemm_kernel_L2_Mv1_44

	.align 5
.Ldgemm_kernel_L2_Mv1_22:

	prfm	PLDL1KEEP, [pB, #B_PRE_SIZE]
	KERNELv1x2_SUB
	KERNELv1x2_SUB
	KERNELv1x2_SUB
	KERNELv1x2_SUB
	prfm	PLDL1KEEP, [pB, #B_PRE_SIZE]
	KERNELv1x2_SUB
	KERNELv1x2_SUB
	KERNELv1x2_SUB
	KERNELv1x2_SUB

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L2_Mv1_22

.Ldgemm_kernel_L2_Mv1_44:

	ands	counterL , origK, #7
	ble	.Ldgemm_kernel_L2_Mv1_100

	.align 5
.Ldgemm_kernel_L2_Mv1_46:

	prfm	PLDL1KEEP, [pB, #B_PRE_SIZE]
	KERNELv1x2_SUB

	subs	counterL, counterL, #1
	bne	.Ldgemm_kernel_L2_Mv1_46

.Ldgemm_kernel_L2_Mv1_100:
	prfm	PLDL1KEEP, [pA]
	prfm	PLDL1KEEP, [pA, #64]
	prfm	PLDL1KEEP, [origPB]

	SAVEv1x2

.Ldgemm_kernel_L2_Mv1_END:

    incw    counterI
    whilelt p1.s, counterI, origM             //SVE instruction
    cntp lanes, p0, p1.s
    b.any   .Ldgemm_kernel_L2_Mv1_20   


.Ldgemm_kernel_L2_END:
	add	origPB, origPB, origK, lsl #3	// B = B + K * 2 * 4

/******************************************************************************/
/* Repeat the same thing if 1 left in N */

	.align 5
.Ldgemm_kernel_L1_BEGIN:

	mov	counterJ , origN
	tst	counterJ , #1
	ble	.Ldgemm_kernel_L999 // done

	mov	pCRow0, pC

    add pC, pC, LDC // add 1 x LDC

	mov	pA, origPA			// pA = start of A array

.Ldgemm_kernel_L1_Mv1_BEGIN:

    mov counterI, #0
    whilelt p1.s, counterI, origM               //SVE instruction
    cntp lanes, p0, p1.s

	.align 5
.Ldgemm_kernel_L1_Mv1_20:

	mov	pB, origPB
    INITv1x1                     // fill with zeros

	asr 	counterL , origK, #3		// L = K / 8
	cmp	counterL , #0			// is there at least 8 to do?
	ble	.Ldgemm_kernel_L1_Mv1_44

	.align 5
.Ldgemm_kernel_L1_Mv1_22:

	prfm	PLDL1KEEP, [pB, #B_PRE_SIZE]
	KERNELv1x1_SUB
	KERNELv1x1_SUB
	KERNELv1x1_SUB
	KERNELv1x1_SUB
	KERNELv1x1_SUB
	KERNELv1x1_SUB
	KERNELv1x1_SUB
	KERNELv1x1_SUB

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L1_Mv1_22

.Ldgemm_kernel_L1_Mv1_44:

	ands	counterL , origK, #7
	ble	.Ldgemm_kernel_L1_Mv1_100

	.align 5
.Ldgemm_kernel_L1_Mv1_46:

	prfm	PLDL1KEEP, [pB, #B_PRE_SIZE]
	KERNELv1x1_SUB

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L1_Mv1_46

.Ldgemm_kernel_L1_Mv1_100:
	prfm	PLDL1KEEP, [pA]
	prfm	PLDL1KEEP, [pA, #64]
	prfm	PLDL1KEEP, [origPB]

	SAVEv1x1

.Ldgemm_kernel_L1_Mv1_END:

    incw    counterI
    whilelt p1.s, counterI, origM             //SVE instruction
    cntp lanes, p0, p1.s
    b.any   .Ldgemm_kernel_L1_Mv1_20   


.Ldgemm_kernel_L1_END:

/******************************************************************************/

.Ldgemm_kernel_L999:
	mov	x0, #0				// set return value
	ldp	d8, d9, [sp, #(0 * 16)]
	ldp	d10, d11, [sp, #(1 * 16)]
	ldp	d12, d13, [sp, #(2 * 16)]
	ldp	d14, d15, [sp, #(3 * 16)]
	ldp	d16, d17, [sp, #(4 * 16)]
	ldp	x18, x19, [sp, #(5 * 16)]
	ldp	x20, x21, [sp, #(6 * 16)]
	ldp	x22, x23, [sp, #(7 * 16)]
	ldp	x24, x25, [sp, #(8 * 16)]
	ldp	x26, x27, [sp, #(9 * 16)]
	ldr	x28, [sp, #(10 * 16)]
	add	sp, sp, #(11*16)
	ret

	EPILOGUE

