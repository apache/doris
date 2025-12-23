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

/*                   X0          X1          X2          s0        X3        x4       x5           x6 */
/*int CNAME(BLASLONG bm,BLASLONG bn,BLASLONG bk,FLOAT alpha0,FLOAT* ba,FLOAT* bb,FLOAT* C,BLASLONG ldc */

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
#define pCRow3		x15
#define pA		x16
#define lanes		x17

#define alphaR		w19
#define alphaI		w20

#define alphaz_R	z6.s
#define alphaz_I	z7.s
#define alpha0_R	s4
#define alpha0_I	s5


#define A_PRE_SIZE	2560
#define B_PRE_SIZE	448
#define C_PRE_SIZE	128

#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define OP_rr		fmla
#define OP_ii		fmls
#define OP_ri		fmla
#define OP_ir		fmla
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define OP_rr		fmla
#define OP_ii		fmla
#define OP_ri		fmls
#define OP_ir		fmla
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define OP_rr		fmla
#define OP_ii		fmla
#define OP_ri		fmla
#define OP_ir		fmls
#elif defined(RR) || defined(RC) || defined(CR) || defined(CC)
#define OP_rr		fmla
#define OP_ii		fmls
#define OP_ri		fmls
#define OP_ir		fmls
#endif

// 00 origM
// 01 origN
// 02 origK
// 03 origPA
// 04 origPB
// 05 pC
// 06 origLDC -> LDC
// 07 offset -> temp
// 08 counterL
// 09 counterI
// 10 counterJ
// 11 pB
// 12 pCRow0
// 13 pCRow1
// 14 pCRow2
// 15 pCRow3
// 16 pA
// 17 alpha_save_R
// 18 must save alpha_save_I
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

//v00 ALPHA_R -> pA00_R, pA01_R
//v01 ALPHA_I -> pA00_I, pA01_I
//v02 pA02_R, pA03_R
//v03 pA02_I, pA03_I
//v04 pA10_R, pA11_R
//v05 pA10_I, pA11_I
//v06 pA12_R, pA13_R
//v07 pA12_I, pA13_I
//v08 must save pB00_R, pB01_R
//v09 must save pB00_I, pB01_I
//v10 must save pB02_R, pB03_R OR ALPHA0_R
//v11 must save pB02_I, pB03_I OR ALPHA0_I
//v12 must save pB10_R, pB11_R
//v13 must save pB10_I, pB11_I
//v14 must save pB12_R, pB13_R OR ALPHA1_R
//v15 must save pB12_I, pB13_I OR ALPHA1_R
//v16 pC0R
//v17 pC0I
//v18 pC1R
//v19 pC1I
//v20 pC2R
//v21 pC2I
//v22 pC3R
//v23 pC3I
//v24 pC3R
//v25 pC3I
//v26 pC22_R, pC23_R
//v27 pC22_I, pC23_I
//v28 pC30_R, pC31_R
//v29 pC30_I, pC31_I
//v30 pC32_R, pC33_R
//v31 pC32_I, pC33_I

/*******************************************************************************
* Macro definitions
*******************************************************************************/

.macro INITv1x4
	dup		z16.s, #0
	dup		z17.s, #0
	dup		z18.s, #0
	dup		z19.s, #0
	dup		z20.s, #0
	dup		z21.s, #0
	dup		z22.s, #0
	dup		z23.s, #0
.endm

.macro KERNELv1x4_I
	ld2w	{z0.s, z1.s}, p1/z, [pA]
	add	pA, pA, lanes, lsl #3    // pA += lanes*2*4
	ld2w	{z2.s, z3.s}, p1/z, [pA] // next one
	add	pA, pA, lanes, lsl #3    // pA += lanes*2*4

    ld1rw  z8.s, p0/z,  [pB]
    ld1rw  z9.s, p0/z,  [pB, 4]
    ld1rw  z10.s, p0/z, [pB, 8]
    ld1rw  z11.s, p0/z, [pB, 12]
    ld1rw  z12.s, p0/z, [pB, 16]
    ld1rw  z13.s, p0/z, [pB, 20]
    ld1rw  z14.s, p0/z, [pB, 24]
    ld1rw  z15.s, p0/z, [pB, 28]

    add pB, pB, 32

	fmla	z16.s, p1/m, z0.s, z8.s
	OP_ir	z17.s, p1/m, z1.s, z8.s
    ld1rw  z8.s, p0/z,  [pB]
#if defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	#eor	z17.16b, z17.16b, z17.16b
	fmls	z17.s, p1/m, z0.s, z9.s
#else
	fmla	z17.s, p1/m, z0.s, z9.s
#endif
	OP_ii	z16.s, p1/m, z1.s, z9.s
    ld1rw  z9.s, p0/z,  [pB, 4]


	fmla	z18.s, p1/m, z0.s, z10.s
	OP_ir	z19.s, p1/m, z1.s, z10.s
    ld1rw  z10.s, p0/z,  [pB, 8]
	OP_ii	z18.s, p1/m, z1.s, z11.s
#if defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	#eor	z19.16b, z21.16b, z21.16b
	fmls	z19.s, p1/m, z0.s, z11.s
#else
	fmla	z19.s, p1/m, z0.s, z11.s
#endif
    ld1rw  z11.s, p0/z,  [pB, 12]


	fmla	z20.s, p1/m, z0.s, z12.s
	OP_ir	z21.s, p1/m, z1.s, z12.s
    ld1rw  z12.s, p0/z,  [pB, 16]
#if defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	#eor	z21.16b, z23.16b, z23.16b
	fmls	z21.s, p1/m, z0.s, z13.s
#else
	fmla	z21.s, p1/m, z0.s, z13.s
#endif
	OP_ii	z20.s, p1/m, z1.s, z13.s
    ld1rw  z13.s, p0/z,  [pB, 20]


	fmla	z22.s, p1/m, z0.s, z14.s
	OP_ir	z23.s, p1/m, z1.s, z14.s
    ld1rw  z14.s, p0/z,  [pB, 24]
#if defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	#eor	z23.16b, z19.16b, z19.16b
	fmls	z23.s, p1/m, z0.s, z15.s
#else
	fmla	z23.s, p1/m, z0.s, z15.s
#endif
	OP_ii	z22.s, p1/m, z1.s, z15.s
    ld1rw  z15.s, p0/z,  [pB, 28]

    add pB, pB, 32

.endm

.macro KERNELv1x4_M1
	ld2w	{z2.s, z3.s}, p1/z, [pA]
	add	pA, pA, lanes, lsl #3	// pA = pA + lanes * 2 * 4

	OP_rr	z16.s, p1/m, z0.s, z8.s
	OP_ir	z17.s, p1/m, z1.s, z8.s
    ld1rw  z8.s, p0/z,  [pB]
	OP_ii	z16.s, p1/m, z1.s, z9.s
	OP_ri	z17.s, p1/m, z0.s, z9.s
    ld1rw  z9.s, p0/z,  [pB, 4]

	OP_rr	z18.s, p1/m, z0.s, z10.s
	OP_ir	z19.s, p1/m, z1.s, z10.s
    ld1rw  z10.s, p0/z,  [pB, 8]
	OP_ii	z18.s, p1/m, z1.s, z11.s
	OP_ri	z19.s, p1/m, z0.s, z11.s
    ld1rw  z11.s, p0/z,  [pB, 12]

	OP_rr	z20.s, p1/m, z0.s, z12.s
	OP_ir	z21.s, p1/m, z1.s, z12.s
    ld1rw  z12.s, p0/z,  [pB, 16]
	OP_ii	z20.s, p1/m, z1.s, z13.s
	OP_ri	z21.s, p1/m, z0.s, z13.s
    ld1rw  z13.s, p0/z,  [pB, 20]

	OP_rr	z22.s, p1/m, z0.s, z14.s
	OP_ir	z23.s, p1/m, z1.s, z14.s
    ld1rw  z14.s, p0/z,  [pB, 24]
	OP_ii	z22.s, p1/m, z1.s, z15.s
	OP_ri	z23.s, p1/m, z0.s, z15.s
    ld1rw  z15.s, p0/z,  [pB, 28]

    add pB, pB, 32
.endm

.macro KERNELv1x4_M2
	ld2w	{z0.s, z1.s}, p1/z, [pA]
	add	pA, pA, lanes, lsl #3	// pA = pA + lanes *2 * 4

	OP_rr	z16.s, p1/m, z2.s, z8.s
	OP_ir	z17.s, p1/m, z3.s, z8.s
    ld1rw  z8.s, p0/z,  [pB]
	OP_ii	z16.s, p1/m, z3.s, z9.s
	OP_ri	z17.s, p1/m, z2.s, z9.s
    ld1rw  z9.s, p0/z,  [pB, 4]

	OP_rr	z18.s, p1/m, z2.s, z10.s
	OP_ir	z19.s, p1/m, z3.s, z10.s
    ld1rw  z10.s, p0/z,  [pB, 8]
	OP_ii	z18.s, p1/m, z3.s, z11.s
	OP_ri	z19.s, p1/m, z2.s, z11.s
    ld1rw  z11.s, p0/z,  [pB, 12]

	OP_rr	z20.s, p1/m, z2.s, z12.s
	OP_ir	z21.s, p1/m, z3.s, z12.s
    ld1rw  z12.s, p0/z,  [pB, 16]
	OP_ii	z20.s, p1/m, z3.s, z13.s
	OP_ri	z21.s, p1/m, z2.s, z13.s
    ld1rw  z13.s, p0/z,  [pB, 20]

	OP_rr	z22.s, p1/m, z2.s, z14.s
	OP_ir	z23.s, p1/m, z3.s, z14.s
    ld1rw  z14.s, p0/z,  [pB, 24]
	OP_ii	z22.s, p1/m, z3.s, z15.s
	OP_ri	z23.s, p1/m, z2.s, z15.s
    ld1rw  z15.s, p0/z,  [pB, 28]

    add pB, pB, 32
.endm

.macro KERNELv1x4_E
	OP_rr	z16.s, p1/m, z2.s, z8.s
	OP_ir	z17.s, p1/m, z3.s, z8.s
	OP_ii	z16.s, p1/m, z3.s, z9.s
	OP_ri	z17.s, p1/m, z2.s, z9.s

	OP_rr	z18.s, p1/m, z2.s, z10.s
	OP_ir	z19.s, p1/m, z3.s, z10.s
	OP_ii	z18.s, p1/m, z3.s, z11.s
	OP_ri	z19.s, p1/m, z2.s, z11.s

	OP_rr	z20.s, p1/m, z2.s, z12.s
	OP_ir	z21.s, p1/m, z3.s, z12.s
	OP_ii	z20.s, p1/m, z3.s, z13.s
	OP_ri	z21.s, p1/m, z2.s, z13.s

	OP_rr	z22.s, p1/m, z2.s, z14.s
	OP_ir	z23.s, p1/m, z3.s, z14.s
	OP_ii	z22.s, p1/m, z3.s, z15.s
	OP_ri	z23.s, p1/m, z2.s, z15.s

.endm

.macro KERNELv1x4_SUB
	ld2w	{z0.s, z1.s}, p1/z, [pA]
	add	pA, pA, lanes, lsl #3	// pA = pA + lanes* 2 * 4

    ld1rw  z8.s, p0/z,  [pB]
    ld1rw  z9.s, p0/z,  [pB, 4]
    ld1rw  z10.s, p0/z,  [pB, 8]
    ld1rw  z11.s, p0/z,  [pB, 12]

	OP_rr	z16.s, p1/m, z0.s, z8.s
	OP_ir	z17.s, p1/m, z1.s, z8.s
	OP_ii	z16.s, p1/m, z1.s, z9.s
	OP_ri	z17.s, p1/m, z0.s, z9.s

    ld1rw  z12.s, p0/z,  [pB, 16]
    ld1rw  z13.s, p0/z,  [pB, 20]
    ld1rw  z14.s, p0/z,  [pB, 24]
    ld1rw  z15.s, p0/z,  [pB, 28]

	OP_rr	z18.s, p1/m, z0.s, z10.s
	OP_ir	z19.s, p1/m, z1.s, z10.s
	OP_ii	z18.s, p1/m, z1.s, z11.s
	OP_ri	z19.s, p1/m, z0.s, z11.s

    add pB, pB, 32

	OP_rr	z20.s, p1/m, z0.s, z12.s
	OP_ir	z21.s, p1/m, z1.s, z12.s
	OP_ii	z20.s, p1/m, z1.s, z13.s
	OP_ri	z21.s, p1/m, z0.s, z13.s

	OP_rr	z22.s, p1/m, z0.s, z14.s
	OP_ir	z23.s, p1/m, z1.s, z14.s
	OP_ii	z22.s, p1/m, z1.s, z15.s
	OP_ri	z23.s, p1/m, z0.s, z15.s

.endm

.macro SAVEv1x4
	ld2w	{z24.s, z25.s}, p1/z, [pCRow0]
	fmla	z24.s, p1/m, z16.s, alphaz_R
	fmls	z24.s, p1/m, z17.s, alphaz_I
	fmla	z25.s, p1/m, z16.s, alphaz_I
	fmla	z25.s, p1/m, z17.s, alphaz_R
	st2w 	{z24.s, z25.s}, p1, [pCRow0]

	add	pCRow0, pCRow0, lanes, lsl #3

	ld2w	{z26.s, z27.s}, p1/z, [pCRow1]
	fmla	z26.s, p1/m, z18.s, alphaz_R
	fmls	z26.s, p1/m, z19.s, alphaz_I
	fmla	z27.s, p1/m, z18.s, alphaz_I
	fmla	z27.s, p1/m, z19.s, alphaz_R
	st2w 	{z26.s, z27.s}, p1, [pCRow1]

	add	pCRow1, pCRow1, lanes, lsl #3
	ld2w	{z28.s, z29.s}, p1/z, [pCRow2]
	fmla	z28.s, p1/m, z20.s, alphaz_R
	fmls	z28.s, p1/m, z21.s, alphaz_I
	fmla	z29.s, p1/m, z20.s, alphaz_I
	fmla	z29.s, p1/m, z21.s, alphaz_R
	st2w 	{z28.s, z29.s}, p1, [pCRow2]

	add	pCRow2, pCRow2, lanes, lsl #3

	ld2w	{z30.s, z31.s}, p1/z, [pCRow3]
	fmla	z30.s, p1/m, z22.s, alphaz_R
	fmls	z30.s, p1/m, z23.s, alphaz_I
	fmla	z31.s, p1/m, z22.s, alphaz_I
	fmla	z31.s, p1/m, z23.s, alphaz_R
	st2w 	{z30.s, z31.s}, p1, [pCRow3]

	add	pCRow3, pCRow3, lanes, lsl #3	// pC = pC + lanes  * 2 *4

.endm

/******************************************************************************/


.macro INITv1x2
	dup		z16.s, #0
	dup		z17.s, #0
	dup		z18.s, #0
	dup		z19.s, #0
.endm

.macro KERNELv1x2_SUB
	ld2w	{z0.s, z1.s}, p1/z, [pA]
	add	pA, pA, lanes, lsl #3	// pA = pA + lanes* 2 * 4

    ld1rw  z8.s, p0/z,  [pB]
    ld1rw  z9.s, p0/z,  [pB, 4]
    ld1rw  z10.s, p0/z,  [pB, 8]
    ld1rw  z11.s, p0/z,  [pB, 12]

	OP_rr	z16.s, p1/m, z0.s, z8.s
	OP_ir	z17.s, p1/m, z1.s, z8.s
	OP_ii	z16.s, p1/m, z1.s, z9.s
	OP_ri	z17.s, p1/m, z0.s, z9.s

	OP_rr	z18.s, p1/m, z0.s, z10.s
	OP_ir	z19.s, p1/m, z1.s, z10.s
	OP_ii	z18.s, p1/m, z1.s, z11.s
	OP_ri	z19.s, p1/m, z0.s, z11.s

    add pB, pB, 16
.endm

.macro SAVEv1x2
	ld2w	{z24.s, z25.s}, p1/z, [pCRow0]
	fmla	z24.s, p1/m, z16.s, alphaz_R
	fmls	z24.s, p1/m, z17.s, alphaz_I
	fmla	z25.s, p1/m, z16.s, alphaz_I
	fmla	z25.s, p1/m, z17.s, alphaz_R
	st2w 	{z24.s, z25.s}, p1, [pCRow0]

	add	pCRow0, pCRow0, lanes, lsl #3

	ld2w	{z26.s, z27.s}, p1/z, [pCRow1]
	fmla	z26.s, p1/m, z18.s, alphaz_R
	fmls	z26.s, p1/m, z19.s, alphaz_I
	fmla	z27.s, p1/m, z18.s, alphaz_I
	fmla	z27.s, p1/m, z19.s, alphaz_R
	st2w 	{z26.s, z27.s}, p1, [pCRow1]

	add	pCRow1, pCRow1, lanes, lsl #3
.endm

/******************************************************************************/


.macro INITv1x1
	dup		z16.s, #0
	dup		z17.s, #0
.endm


.macro KERNELv1x1_SUB
	ld2w	{z0.s, z1.s}, p1/z, [pA]
	add	pA, pA, lanes, lsl #3	// pA = pA + lanes* 2 * 4

    ld1rw  z8.s, p0/z,  [pB]
    ld1rw  z9.s, p0/z,  [pB, 4]

    add pB, pB, 8

	OP_rr	z16.s, p1/m, z0.s, z8.s
	OP_ir	z17.s, p1/m, z1.s, z8.s
	OP_ii	z16.s, p1/m, z1.s, z9.s
	OP_ri	z17.s, p1/m, z0.s, z9.s
.endm

.macro SAVEv1x1
	ld2w	{z24.s, z25.s}, p1/z, [pCRow0]
	fmla	z24.s, p1/m, z16.s, alphaz_R
	fmls	z24.s, p1/m, z17.s, alphaz_I
	fmla	z25.s, p1/m, z16.s, alphaz_I
	fmla	z25.s, p1/m, z17.s, alphaz_R
	st2w 	{z24.s, z25.s}, p1, [pCRow0]

	add	pCRow0, pCRow0, lanes, lsl #3	// pC = pC + lanes  * 2 *4

.endm

/******************************************************************************/

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

	fmov	alphaR, s0
	dup	    alphaz_R, alphaR
	fmov	alphaI, s1
	dup	    alphaz_I, alphaI

	lsl	LDC, LDC, #3			// ldc = ldc * 2 * 4
    ptrue p0.s                  // create true predicate 

	mov	pB, origPB

// Loop over N
	mov	counterJ, origN
	asr 	counterJ, counterJ, #2		// J = J / 4
	cmp 	counterJ, #0
	ble	.Lcgemm_kernel_L2_BEGIN

/******************************************************************************/
.Lcgemm_kernel_L4_BEGIN:
	mov	pCRow0, pC
	add	pCRow1, pCRow0, LDC
	add	pCRow2, pCRow1, LDC
	add	pCRow3, pCRow2, LDC

	add	pC, pCRow3, LDC

	mov	pA, origPA			// pA = start of A array

.Lcgemm_kernel_L4_Mv1_BEGIN:

/* Loop over M is done in an SVE fashion. This has the benefit of the last M%SVE_LEN iterations being done in a single sweep */
    mov counterI, #0
    whilelt p1.s, counterI, origM   
    cntp lanes, p0, p1.s                        // lanes contain number of active SVE lanes in M dimension

	.align 5
.Lcgemm_kernel_L4_Mv1_20:

	mov	pB, origPB
    INITv1x4                     // fill with zeros

	asr 	counterL , origK, #3
	cmp	counterL , #2
	blt	.Lcgemm_kernel_L4_Mv1_32

	KERNELv1x4_I
	KERNELv1x4_M2
	KERNELv1x4_M1
	KERNELv1x4_M2
	KERNELv1x4_M1
	KERNELv1x4_M2
	KERNELv1x4_M1
	KERNELv1x4_M2

	subs	counterL, counterL, #2		// subtract 2
	ble	.Lcgemm_kernel_L4_Mv1_22a

	.align 5
.Lcgemm_kernel_L4_Mv1_22:

	KERNELv1x4_M1
	KERNELv1x4_M2
	KERNELv1x4_M1
	KERNELv1x4_M2
	KERNELv1x4_M1
	KERNELv1x4_M2
	KERNELv1x4_M1
	KERNELv1x4_M2

	subs	counterL, counterL, #1
	bgt	.Lcgemm_kernel_L4_Mv1_22

	.align 5
.Lcgemm_kernel_L4_Mv1_22a:

	KERNELv1x4_M1
	KERNELv1x4_M2
	KERNELv1x4_M1
	KERNELv1x4_M2
	KERNELv1x4_M1
	KERNELv1x4_M2
	KERNELv1x4_M1
	KERNELv1x4_E

	b	 .Lcgemm_kernel_L4_Mv1_44

	.align 5
.Lcgemm_kernel_L4_Mv1_32:

	tst	counterL, #1
	ble	.Lcgemm_kernel_L4_Mv1_40

	KERNELv1x4_I
	KERNELv1x4_M2
	KERNELv1x4_M1
	KERNELv1x4_M2
	KERNELv1x4_M1
	KERNELv1x4_M2
	KERNELv1x4_M1
	KERNELv1x4_E

	b	.Lcgemm_kernel_L4_Mv1_44


.Lcgemm_kernel_L4_Mv1_40:

	INITv1x4

.Lcgemm_kernel_L4_Mv1_44:

	ands	counterL , origK, #7
	ble	.Lcgemm_kernel_L4_Mv1_100

	.align 5
.Lcgemm_kernel_L4_Mv1_46:
	KERNELv1x4_SUB

	subs	counterL, counterL, #1
	bne	.Lcgemm_kernel_L4_Mv1_46

.Lcgemm_kernel_L4_Mv1_100:
	SAVEv1x4

.Lcgemm_kernel_L4_Mv1_END:

    incw    counterI
    whilelt p1.s, counterI, origM             //SVE instruction
    cntp lanes, p0, p1.s                        // lanes contain number of active SVE lanes in M dimension
    b.any   .Lcgemm_kernel_L4_Mv1_20   



.Lcgemm_kernel_L4_END:

	lsl	temp, origK, #5
	add	origPB, origPB, temp		// B = B + K * 4 * 4 * 2

	subs	counterJ, counterJ , #1		// j--
	bgt	.Lcgemm_kernel_L4_BEGIN


/******************************************************************************/

.Lcgemm_kernel_L2_BEGIN:   // less than 2 left in N direction

	mov	counterJ , origN
	tst	counterJ , #3
	ble	.Lcgemm_kernel_L999

	tst	counterJ , #2
	ble	.Lcgemm_kernel_L1_BEGIN

	mov	pCRow0, pC			// pCRow0 = pC
	add	pCRow1, pCRow0, LDC

	add	pC,pC,LDC, lsl #1

	mov	pA, origPA			// pA = A



.Lcgemm_kernel_L2_Mv1_BEGIN:

    mov counterI, #0
    whilelt p1.s, counterI, origM               //SVE instruction
    cntp lanes, p0, p1.s


.Lcgemm_kernel_L2_Mv1_20:

	INITv1x2

	mov	pB, origPB
	asr	counterL , origK, #3		// counterL = counterL / 8
	cmp	counterL,#0
	ble	.Lcgemm_kernel_L2_Mv1_40
	.align 5

.Lcgemm_kernel_L2_Mv1_22:
	KERNELv1x2_SUB
	KERNELv1x2_SUB
	KERNELv1x2_SUB
	KERNELv1x2_SUB

	KERNELv1x2_SUB
	KERNELv1x2_SUB
	KERNELv1x2_SUB
	KERNELv1x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lcgemm_kernel_L2_Mv1_22


.Lcgemm_kernel_L2_Mv1_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Lcgemm_kernel_L2_Mv1_100

.Lcgemm_kernel_L2_Mv1_42:

	KERNELv1x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lcgemm_kernel_L2_Mv1_42

.Lcgemm_kernel_L2_Mv1_100:

	SAVEv1x2

.Lcgemm_kernel_L2_Mv1_END:


    incw    counterI
    whilelt p1.s, counterI, origM             //SVE instruction
    cntp lanes, p0, p1.s
    b.any   .Lcgemm_kernel_L2_Mv1_20   


.Lcgemm_kernel_L2_END:
	lsl	temp, origK, #4
	add	origPB, origPB, temp // B = B + K * 2 * 4 * 2

/******************************************************************************/

.Lcgemm_kernel_L1_BEGIN:

	mov	counterJ , origN
	tst	counterJ , #1
	ble	.Lcgemm_kernel_L999 // done


	mov	pCRow0, pC			// pCRow0 = C
	add	pC , pC , LDC			// Update pC to point to next

	mov	pA, origPA			// pA = A

.Lcgemm_kernel_L1_Mv1_BEGIN:

    mov counterI, #0
    whilelt p1.s, counterI, origM               //SVE instruction
    cntp lanes, p0, p1.s


.Lcgemm_kernel_L1_Mv1_20:

	INITv1x1

	mov	pB, origPB
	asr	counterL , origK, #3		// counterL = counterL / 8
	cmp	counterL , #0
	ble	.Lcgemm_kernel_L1_Mv1_40
	.align 5

.Lcgemm_kernel_L1_Mv1_22:
	KERNELv1x1_SUB
	KERNELv1x1_SUB
	KERNELv1x1_SUB
	KERNELv1x1_SUB

	KERNELv1x1_SUB
	KERNELv1x1_SUB
	KERNELv1x1_SUB
	KERNELv1x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lcgemm_kernel_L1_Mv1_22


.Lcgemm_kernel_L1_Mv1_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Lcgemm_kernel_L1_Mv1_100

.Lcgemm_kernel_L1_Mv1_42:

	KERNELv1x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lcgemm_kernel_L1_Mv1_42

.Lcgemm_kernel_L1_Mv1_100:

	SAVEv1x1

.Lcgemm_kernel_L1_Mv1_END:

    incw    counterI
    whilelt p1.s, counterI, origM             //SVE instruction
    cntp lanes, p0, p1.s
    b.any   .Lcgemm_kernel_L1_Mv1_20   

.Lcgemm_kernel_L1_END:

/******************************************************************************/

.Lcgemm_kernel_L999:
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

