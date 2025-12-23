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

/* This is an SVE dgemm kernel with size 2*SVE_LEN x 8.
However, the data layout is the same as for the kernel 1*SVE_LEN x 8.
This means that we sweep two panels of packed A when iterating in a loop over K.
With this approach, we can reuse dgemm_n|tcopy_sve_v1.c packing functions. */

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
#define pA1	    	x16
#define pA2	    	x17
#define alpha		x18
#define vec_len		x19
#define vec_lenx2   x20

#define alpha0		d10
#define alphaZ		z7.d

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
// 16 pA1
// 17 pA1
// 18 must save alpha
// 19 must save vec_len
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

//v00 ALPHA -> pA10_0
//v01 pA10_1
//v02 pA20_0
//v03 pA20_1
//v04 
//v05 
//v06 
//v07 ALPHA0
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
//v24 must save C8
//v25 must save C9
//v26 must save C10
//v27 must save C11
//v28 must save C12
//v29 must save C13
//v30 must save C14
//v31 must save C15

/*******************************************************************************
* Macro definitions
*******************************************************************************/

.macro INITv2x8
    dup         z16.d, #0
    dup         z17.d, #0
    dup         z18.d, #0
    dup         z19.d, #0
    dup         z20.d, #0
    dup         z21.d, #0
    dup         z22.d, #0
    dup         z23.d, #0
    dup         z24.d, #0
    dup         z25.d, #0
    dup         z26.d, #0
    dup         z27.d, #0
    dup         z28.d, #0
    dup         z29.d, #0
    dup         z30.d, #0
    dup         z31.d, #0
.endm

.macro KERNELv2x8_I
    ld1d  z0.d, p0/z, [pA1] 
    ld1d  z1.d, p0/z, [pA2]   
    ld1d  z2.d, p0/z, [pA1, vec_len, lsl #3] 
    ld1d  z3.d, p0/z, [pA2, vec_len, lsl #3]   
	add	pA1, pA1, vec_len, lsl #4	// pA1 = pA1 + vec_len * 8 *2
	add	pA2, pA2, vec_len, lsl #4	// pA1 = pA1 + vec_len * 8 *2


    ld1rd  z8.d, p0/z,  [pB]
    ld1rd  z9.d, p0/z,  [pB, 8]
    ld1rd  z10.d, p0/z, [pB, 16]
    ld1rd  z11.d, p0/z, [pB, 24]
    ld1rd  z12.d, p0/z, [pB, 32]
    ld1rd  z13.d, p0/z, [pB, 40]
    ld1rd  z14.d, p0/z, [pB, 48]
    ld1rd  z15.d, p0/z, [pB, 56]

    add pB, pB, 64

    fmla z16.d, p0/m, z0.d, z8.d
    fmla z17.d, p0/m, z1.d, z8.d
    ld1rd  z8.d, p0/z,  [pB]
    fmla z18.d, p0/m, z0.d, z9.d
    fmla z19.d, p0/m, z1.d, z9.d
    ld1rd  z9.d, p0/z,  [pB, 8]
    fmla z20.d, p0/m, z0.d, z10.d
    fmla z21.d, p0/m, z1.d, z10.d
    ld1rd  z10.d, p0/z, [pB, 16]
    fmla z22.d, p0/m, z0.d, z11.d
    fmla z23.d, p0/m, z1.d, z11.d
    ld1rd  z11.d, p0/z, [pB, 24]
    fmla z24.d, p0/m, z0.d, z12.d
    fmla z25.d, p0/m, z1.d, z12.d
    ld1rd  z12.d, p0/z, [pB, 32]
    fmla z26.d, p0/m, z0.d, z13.d
    fmla z27.d, p0/m, z1.d, z13.d
    ld1rd  z13.d, p0/z, [pB, 40]
    fmla z28.d, p0/m, z0.d, z14.d
    fmla z29.d, p0/m, z1.d, z14.d
    ld1rd  z14.d, p0/z, [pB, 48]
    fmla z30.d, p0/m, z0.d, z15.d
    fmla z31.d, p0/m, z1.d, z15.d
    ld1rd  z15.d, p0/z, [pB, 56]

    add pB, pB, 64
.endm

.macro KERNELv2x8_M1
    ld1d  z2.d, p0/z, [pA1] 
    ld1d  z3.d, p0/z, [pA2]   
	add	pA1, pA1, vec_len, lsl #3	// pA1 = pA1 + vec_len * 8
	add	pA2, pA2, vec_len, lsl #3	// pA1 = pA1 + vec_len * 8

    fmla z16.d, p0/m, z0.d, z8.d
    fmla z17.d, p0/m, z1.d, z8.d
    ld1rd  z8.d, p0/z,  [pB]
    fmla z18.d, p0/m, z0.d, z9.d
    fmla z19.d, p0/m, z1.d, z9.d
    ld1rd  z9.d, p0/z,  [pB, 8]
    fmla z20.d, p0/m, z0.d, z10.d
    fmla z21.d, p0/m, z1.d, z10.d
    ld1rd  z10.d, p0/z, [pB, 16]
    fmla z22.d, p0/m, z0.d, z11.d
    fmla z23.d, p0/m, z1.d, z11.d
    ld1rd  z11.d, p0/z, [pB, 24]
    fmla z24.d, p0/m, z0.d, z12.d
    fmla z25.d, p0/m, z1.d, z12.d
    ld1rd  z12.d, p0/z, [pB, 32]
    fmla z26.d, p0/m, z0.d, z13.d
    fmla z27.d, p0/m, z1.d, z13.d
    ld1rd  z13.d, p0/z, [pB, 40]
    fmla z28.d, p0/m, z0.d, z14.d
    fmla z29.d, p0/m, z1.d, z14.d
    ld1rd  z14.d, p0/z, [pB, 48]
    fmla z30.d, p0/m, z0.d, z15.d
    fmla z31.d, p0/m, z1.d, z15.d
    ld1rd  z15.d, p0/z, [pB, 56]

    add pB, pB, 64
.endm

.macro KERNELv2x8_M2
    ld1d  z0.d, p0/z, [pA1] 
    ld1d  z1.d, p0/z, [pA2]   
	add	pA1, pA1, vec_len, lsl #3	// pA1 = pA1 + vec_len * 2 * 8
	add	pA2, pA2, vec_len, lsl #3	// pA1 = pA1 + vec_len * 2 * 8

    fmla z16.d, p0/m, z2.d, z8.d
    fmla z17.d, p0/m, z3.d, z8.d 
    ld1rd  z8.d, p0/z,  [pB]
    fmla z18.d, p0/m, z2.d, z9.d
    fmla z19.d, p0/m, z3.d, z9.d
    ld1rd  z9.d, p0/z,  [pB, 8]
    fmla z20.d, p0/m, z2.d, z10.d
    fmla z21.d, p0/m, z3.d, z10.d
    ld1rd  z10.d, p0/z, [pB, 16]
    fmla z22.d, p0/m, z2.d, z11.d
    fmla z23.d, p0/m, z3.d, z11.d
    ld1rd  z11.d, p0/z, [pB, 24]
    fmla z24.d, p0/m, z2.d, z12.d
    fmla z25.d, p0/m, z3.d, z12.d
    ld1rd  z12.d, p0/z, [pB, 32]
    fmla z26.d, p0/m, z2.d, z13.d
    fmla z27.d, p0/m, z3.d, z13.d
    ld1rd  z13.d, p0/z, [pB, 40]
    fmla z28.d, p0/m, z2.d, z14.d
    fmla z29.d, p0/m, z3.d, z14.d
    ld1rd  z14.d, p0/z, [pB, 48]
    fmla z30.d, p0/m, z2.d, z15.d
    fmla z31.d, p0/m, z3.d, z15.d
    ld1rd  z15.d, p0/z, [pB, 56]

    add pB, pB, 64
.endm

.macro KERNELv2x8_E
    fmla z16.d, p0/m, z2.d, z8.d
    fmla z17.d, p0/m, z3.d, z8.d
    fmla z18.d, p0/m, z2.d, z9.d
    fmla z19.d, p0/m, z3.d, z9.d
    fmla z20.d, p0/m, z2.d, z10.d
    fmla z21.d, p0/m, z3.d, z10.d
    fmla z22.d, p0/m, z2.d, z11.d
    fmla z23.d, p0/m, z3.d, z11.d
    fmla z24.d, p0/m, z2.d, z12.d
    fmla z25.d, p0/m, z3.d, z12.d
    fmla z26.d, p0/m, z2.d, z13.d
    fmla z27.d, p0/m, z3.d, z13.d
    fmla z28.d, p0/m, z2.d, z14.d
    fmla z29.d, p0/m, z3.d, z14.d
    fmla z30.d, p0/m, z2.d, z15.d
    fmla z31.d, p0/m, z3.d, z15.d
.endm

.macro KERNELv2x8_SUB
    ld1d  z0.d, p0/z, [pA1] 
    ld1d  z1.d, p0/z, [pA2]   
	add	pA1, pA1, vec_len, lsl #3	// pA1 = pA1 + vec_len * 8
	add	pA2, pA2, vec_len, lsl #3	// pA1 = pA1 + vec_len * 8

    ld1rd  z8.d, p0/z,  [pB]
    ld1rd  z9.d, p0/z,  [pB, 8]
    ld1rd  z10.d, p0/z, [pB, 16]
    ld1rd  z11.d, p0/z, [pB, 24]
    ld1rd  z12.d, p0/z, [pB, 32]
    ld1rd  z13.d, p0/z, [pB, 40]
    ld1rd  z14.d, p0/z, [pB, 48]
    ld1rd  z15.d, p0/z, [pB, 56]

    add pB, pB, 64

    fmla z16.d, p0/m, z0.d, z8.d
    fmla z17.d, p0/m, z1.d, z8.d
    fmla z18.d, p0/m, z0.d, z9.d
    fmla z19.d, p0/m, z1.d, z9.d
    fmla z20.d, p0/m, z0.d, z10.d
    fmla z21.d, p0/m, z1.d, z10.d
    fmla z22.d, p0/m, z0.d, z11.d
    fmla z23.d, p0/m, z1.d, z11.d
    fmla z24.d, p0/m, z0.d, z12.d
    fmla z25.d, p0/m, z1.d, z12.d
    fmla z26.d, p0/m, z0.d, z13.d
    fmla z27.d, p0/m, z1.d, z13.d
    fmla z28.d, p0/m, z0.d, z14.d
    fmla z29.d, p0/m, z1.d, z14.d
    fmla z30.d, p0/m, z0.d, z15.d
    fmla z31.d, p0/m, z1.d, z15.d
.endm

.macro SAVEv2x8


	add	pCRow1, pCRow0, LDC
    ld1d  z8.d, p0/z, [pCRow0] 
    ld1d  z9.d, p0/z, [pCRow0, #1, mul vl] 
    fmla z8.d, p0/m, z16.d, alphaZ
    fmla z9.d, p0/m, z17.d, alphaZ
    st1d  z8.d, p0, [pCRow0]
    st1d  z9.d, p0, [pCRow0, #1, mul vl]

	add	pCRow2, pCRow1, LDC
    ld1d  z10.d, p0/z, [pCRow1] 
    ld1d  z11.d, p0/z, [pCRow1, #1, mul vl] 
    fmla z10.d, p0/m, z18.d, alphaZ
    fmla z11.d, p0/m, z19.d, alphaZ
    st1d  z10.d, p0, [pCRow1]
    st1d  z11.d, p0, [pCRow1, #1, mul vl]

	add	pCRow1, pCRow2, LDC
    ld1d  z12.d, p0/z, [pCRow2] 
    ld1d  z13.d, p0/z, [pCRow2, #1, mul vl] 
    fmla z12.d, p0/m, z20.d, alphaZ
    fmla z13.d, p0/m, z21.d, alphaZ
    st1d  z12.d, p0, [pCRow2]
    st1d  z13.d, p0, [pCRow2, #1, mul vl]

	add	pCRow2, pCRow1, LDC
    ld1d  z14.d, p0/z, [pCRow1] 
    ld1d  z15.d, p0/z, [pCRow1, #1, mul vl] 
    fmla z14.d, p0/m, z22.d, alphaZ
    fmla z15.d, p0/m, z23.d, alphaZ
    st1d  z14.d, p0, [pCRow1]
    st1d  z15.d, p0, [pCRow1, #1, mul vl]

	add	pCRow1, pCRow2, LDC
    ld1d  z8.d, p0/z, [pCRow2] 
    ld1d  z9.d, p0/z, [pCRow2, #1, mul vl] 
    fmla z8.d, p0/m, z24.d, alphaZ
    fmla z9.d, p0/m, z25.d, alphaZ
    st1d  z8.d, p0, [pCRow2]
    st1d  z9.d, p0, [pCRow2, #1, mul vl]

	add	pCRow2, pCRow1, LDC
    ld1d  z10.d, p0/z, [pCRow1] 
    ld1d  z11.d, p0/z, [pCRow1, #1, mul vl] 
    fmla z10.d, p0/m, z26.d, alphaZ
    fmla z11.d, p0/m, z27.d, alphaZ
    st1d  z10.d, p0, [pCRow1]
    st1d  z11.d, p0, [pCRow1, #1, mul vl]

	add	pCRow1, pCRow2, LDC
    ld1d  z12.d, p0/z, [pCRow2] 
    ld1d  z13.d, p0/z, [pCRow2, #1, mul vl] 
    fmla z12.d, p0/m, z28.d, alphaZ
    fmla z13.d, p0/m, z29.d, alphaZ
    st1d  z12.d, p0, [pCRow2]
    st1d  z13.d, p0, [pCRow2, #1, mul vl]

    ld1d  z14.d, p0/z, [pCRow1] 
    ld1d  z15.d, p0/z, [pCRow1, #1, mul vl] 
    fmla z14.d, p0/m, z30.d, alphaZ
    fmla z15.d, p0/m, z31.d, alphaZ
    st1d  z14.d, p0, [pCRow1]
    st1d  z15.d, p0, [pCRow1, #1, mul vl]

	add	pCRow0, pCRow0, vec_len, lsl #4	// pC = pC + vec_len  * 8 * 2

.endm

.macro INITv2x4
    dup         z16.d, #0
    dup         z17.d, #0
    dup         z18.d, #0
    dup         z19.d, #0
    dup         z20.d, #0
    dup         z21.d, #0
    dup         z22.d, #0
    dup         z23.d, #0
.endm

.macro KERNELv2x4_SUB
    ld1d  z0.d, p0/z, [pA1] 
    ld1d  z1.d, p0/z, [pA2]   
	add	pA1, pA1, vec_len, lsl #3	// pA1 = pA1 + vec_len * 8
	add	pA2, pA2, vec_len, lsl #3	// pA1 = pA1 + vec_len * 8

    ld1rd  z8.d, p0/z,  [pB]
    ld1rd  z9.d, p0/z,  [pB, 8]
    ld1rd  z10.d, p0/z, [pB, 16]
    ld1rd  z11.d, p0/z, [pB, 24]

    add pB, pB, 32

    fmla z16.d, p0/m, z0.d, z8.d
    fmla z17.d, p0/m, z1.d, z8.d
    fmla z18.d, p0/m, z0.d, z9.d
    fmla z19.d, p0/m, z1.d, z9.d
    fmla z20.d, p0/m, z0.d, z10.d
    fmla z21.d, p0/m, z1.d, z10.d
    fmla z22.d, p0/m, z0.d, z11.d
    fmla z23.d, p0/m, z1.d, z11.d
.endm

.macro SAVEv2x4


	add	pCRow1, pCRow0, LDC
    ld1d  z8.d, p0/z, [pCRow0] 
    ld1d  z9.d, p0/z, [pCRow0, #1, mul vl] 
    fmla z8.d, p0/m, z16.d, alphaZ
    fmla z9.d, p0/m, z17.d, alphaZ
    st1d  z8.d, p0, [pCRow0]
    st1d  z9.d, p0, [pCRow0, #1, mul vl]

	add	pCRow2, pCRow1, LDC
    ld1d  z10.d, p0/z, [pCRow1] 
    ld1d  z11.d, p0/z, [pCRow1, #1, mul vl] 
    fmla z10.d, p0/m, z18.d, alphaZ
    fmla z11.d, p0/m, z19.d, alphaZ
    st1d  z10.d, p0, [pCRow1]
    st1d  z11.d, p0, [pCRow1, #1, mul vl]

	add	pCRow1, pCRow2, LDC
    ld1d  z12.d, p0/z, [pCRow2] 
    ld1d  z13.d, p0/z, [pCRow2, #1, mul vl] 
    fmla z12.d, p0/m, z20.d, alphaZ
    fmla z13.d, p0/m, z21.d, alphaZ
    st1d  z12.d, p0, [pCRow2]
    st1d  z13.d, p0, [pCRow2, #1, mul vl]

    ld1d  z14.d, p0/z, [pCRow1] 
    ld1d  z15.d, p0/z, [pCRow1, #1, mul vl] 
    fmla z14.d, p0/m, z22.d, alphaZ
    fmla z15.d, p0/m, z23.d, alphaZ
    st1d  z14.d, p0, [pCRow1]
    st1d  z15.d, p0, [pCRow1, #1, mul vl]

	add	pCRow0, pCRow0, vec_len, lsl #4	// pC = pC + vec_len  * 8 * 2

.endm

.macro INITv2x2
    dup         z16.d, #0
    dup         z17.d, #0
    dup         z18.d, #0
    dup         z19.d, #0
.endm

.macro KERNELv2x2_SUB
    ld1d  z0.d, p0/z, [pA1] 
    ld1d  z1.d, p0/z, [pA2]   
	add	pA1, pA1, vec_len, lsl #3	// pA1 = pA1 + vec_len * 8
	add	pA2, pA2, vec_len, lsl #3	// pA1 = pA1 + vec_len * 8

    ld1rd  z8.d, p0/z,  [pB]
    ld1rd  z9.d, p0/z,  [pB, 8]

    add pB, pB, 16

    fmla z16.d, p0/m, z0.d, z8.d
    fmla z17.d, p0/m, z1.d, z8.d
    fmla z18.d, p0/m, z0.d, z9.d
    fmla z19.d, p0/m, z1.d, z9.d
.endm

.macro SAVEv2x2


	add	pCRow1, pCRow0, LDC
    ld1d  z8.d, p0/z, [pCRow0] 
    ld1d  z9.d, p0/z, [pCRow0, #1, mul vl] 
    fmla z8.d, p0/m, z16.d, alphaZ
    fmla z9.d, p0/m, z17.d, alphaZ
    st1d  z8.d, p0, [pCRow0]
    st1d  z9.d, p0, [pCRow0, #1, mul vl]

    ld1d  z10.d, p0/z, [pCRow1] 
    ld1d  z11.d, p0/z, [pCRow1, #1, mul vl] 
    fmla z10.d, p0/m, z18.d, alphaZ
    fmla z11.d, p0/m, z19.d, alphaZ
    st1d  z10.d, p0, [pCRow1]
    st1d  z11.d, p0, [pCRow1, #1, mul vl]



	add	pCRow0, pCRow0, vec_len, lsl #4	// pC = pC + vec_len  * 8 * 2
.endm

.macro INITv2x1
    dup         z16.d, #0
    dup         z17.d, #0
.endm

.macro KERNELv2x1_SUB
    ld1d  z0.d, p0/z, [pA1] 
    ld1d  z1.d, p0/z, [pA2]   
	add	pA1, pA1, vec_len, lsl #3	// pA1 = pA1 + vec_len * 8
	add	pA2, pA2, vec_len, lsl #3	// pA1 = pA1 + vec_len * 8

    ld1rd  z8.d, p0/z,  [pB]

    add pB, pB, 8

    fmla z16.d, p0/m, z0.d, z8.d
    fmla z17.d, p0/m, z1.d, z8.d
.endm

.macro SAVEv2x1


	add	pCRow1, pCRow0, LDC
    ld1d  z8.d, p0/z, [pCRow0] 
    ld1d  z9.d, p0/z, [pCRow0, #1, mul vl] 
    fmla z8.d, p0/m, z16.d, alphaZ
    fmla z9.d, p0/m, z17.d, alphaZ
    st1d  z8.d, p0, [pCRow0]
    st1d  z9.d, p0, [pCRow0, #1, mul vl]

	add	pCRow0, pCRow0, vec_len, lsl #4	// pC = pC + vec_len  * 8 * 2

.endm

.macro INITv1x8
    dup         z16.d, #0
    dup         z17.d, #0
    dup         z18.d, #0
    dup         z19.d, #0
    dup         z20.d, #0
    dup         z21.d, #0
    dup         z22.d, #0
    dup         z23.d, #0
.endm

.macro KERNELv1x8_I
    ld1d  z0.d, p1/z, [pA1] 
    ld1d  z1.d, p1/z, [pA1, lanes, lsl #3]   // next one
	add	pA1, pA1, lanes, lsl #4	// pA1 = pA1 + lanes * 2 * 8

    ld1rd  z8.d, p0/z,  [pB]
    ld1rd  z9.d, p0/z,  [pB, 8]
    ld1rd  z10.d, p0/z, [pB, 16]
    ld1rd  z11.d, p0/z, [pB, 24]
    ld1rd  z12.d, p0/z, [pB, 32]
    ld1rd  z13.d, p0/z, [pB, 40]
    ld1rd  z14.d, p0/z, [pB, 48]
    ld1rd  z15.d, p0/z, [pB, 56]

    add pB, pB, 64

    fmla z16.d, p1/m, z0.d, z8.d
    ld1rd  z8.d, p0/z,  [pB]
    fmla z17.d, p1/m, z0.d, z9.d
    ld1rd  z9.d, p0/z,  [pB, 8]
    fmla z18.d, p1/m, z0.d, z10.d
    ld1rd  z10.d, p0/z, [pB, 16]
    fmla z19.d, p1/m, z0.d, z11.d
    ld1rd  z11.d, p0/z, [pB, 24]
    fmla z20.d, p1/m, z0.d, z12.d
    ld1rd  z12.d, p0/z, [pB, 32]
    fmla z21.d, p1/m, z0.d, z13.d
    ld1rd  z13.d, p0/z, [pB, 40]
    fmla z22.d, p1/m, z0.d, z14.d
    ld1rd  z14.d, p0/z, [pB, 48]
    fmla z23.d, p1/m, z0.d, z15.d
    ld1rd  z15.d, p0/z, [pB, 56]

    add pB, pB, 64
.endm

.macro KERNELv1x8_M1
    ld1d  z1.d, p1/z, [pA1] 
	add	pA1, pA1, lanes, lsl #3	// pA1 = pA1 + lanes  * 8

    fmla z16.d, p1/m, z0.d, z8.d
    ld1rd  z8.d, p0/z,  [pB]
    fmla z17.d, p1/m, z0.d, z9.d
    ld1rd  z9.d, p0/z,  [pB, 8]
    fmla z18.d, p1/m, z0.d, z10.d
    ld1rd  z10.d, p0/z, [pB, 16]
    fmla z19.d, p1/m, z0.d, z11.d
    ld1rd  z11.d, p0/z, [pB, 24]
    fmla z20.d, p1/m, z0.d, z12.d
    ld1rd  z12.d, p0/z, [pB, 32]
    fmla z21.d, p1/m, z0.d, z13.d
    ld1rd  z13.d, p0/z, [pB, 40]
    fmla z22.d, p1/m, z0.d, z14.d
    ld1rd  z14.d, p0/z, [pB, 48]
    fmla z23.d, p1/m, z0.d, z15.d
    ld1rd  z15.d, p0/z, [pB, 56]

    add pB, pB, 64
.endm

.macro KERNELv1x8_M2
    ld1d  z0.d, p1/z, [pA1] 
	add	pA1, pA1, lanes, lsl #3	// pA1 = pA1 + lanes  * 8

    fmla z16.d, p1/m, z1.d, z8.d
    ld1rd  z8.d, p0/z,  [pB]
    fmla z17.d, p1/m, z1.d, z9.d
    ld1rd  z9.d, p0/z,  [pB, 8]
    fmla z18.d, p1/m, z1.d, z10.d
    ld1rd  z10.d, p0/z, [pB, 16]
    fmla z19.d, p1/m, z1.d, z11.d
    ld1rd  z11.d, p0/z, [pB, 24]
    fmla z20.d, p1/m, z1.d, z12.d
    ld1rd  z12.d, p0/z, [pB, 32]
    fmla z21.d, p1/m, z1.d, z13.d
    ld1rd  z13.d, p0/z, [pB, 40]
    fmla z22.d, p1/m, z1.d, z14.d
    ld1rd  z14.d, p0/z, [pB, 48]
    fmla z23.d, p1/m, z1.d, z15.d
    ld1rd  z15.d, p0/z, [pB, 56]

    add pB, pB, 64
.endm

.macro KERNELv1x8_E
    fmla z16.d, p1/m, z1.d, z8.d
    fmla z17.d, p1/m, z1.d, z9.d
    fmla z18.d, p1/m, z1.d, z10.d
    fmla z19.d, p1/m, z1.d, z11.d
    fmla z20.d, p1/m, z1.d, z12.d
    fmla z21.d, p1/m, z1.d, z13.d
    fmla z22.d, p1/m, z1.d, z14.d
    fmla z23.d, p1/m, z1.d, z15.d
.endm

.macro KERNELv1x8_SUB
    ld1d  z0.d, p1/z, [pA1] 
	add	pA1, pA1, lanes, lsl #3	// pA1 = pA1 + lanes  * 8

    ld1rd  z8.d, p0/z,  [pB]
    ld1rd  z9.d, p0/z,  [pB, 8]
    ld1rd  z10.d, p0/z, [pB, 16]
    ld1rd  z11.d, p0/z, [pB, 24]
    ld1rd  z12.d, p0/z, [pB, 32]
    ld1rd  z13.d, p0/z, [pB, 40]
    ld1rd  z14.d, p0/z, [pB, 48]
    ld1rd  z15.d, p0/z, [pB, 56]

    add pB, pB, 64

    fmla z16.d, p1/m, z0.d, z8.d
    fmla z17.d, p1/m, z0.d, z9.d
    fmla z18.d, p1/m, z0.d, z10.d
    fmla z19.d, p1/m, z0.d, z11.d
    fmla z20.d, p1/m, z0.d, z12.d
    fmla z21.d, p1/m, z0.d, z13.d
    fmla z22.d, p1/m, z0.d, z14.d
    fmla z23.d, p1/m, z0.d, z15.d


.endm

.macro SAVEv1x8


	add	pCRow1, pCRow0, LDC
    ld1d  z24.d, p1/z, [pCRow0] 
    fmla z24.d, p1/m, z16.d, alphaZ
    st1d  z24.d, p1, [pCRow0]

	add	pCRow2, pCRow1, LDC
    ld1d  z25.d, p1/z, [pCRow1] 
    fmla z25.d, p1/m, z17.d, alphaZ
    st1d  z25.d, p1, [pCRow1]

	add	pCRow1, pCRow2, LDC
    ld1d  z26.d, p1/z, [pCRow2] 
    fmla z26.d, p1/m, z18.d, alphaZ
    st1d z26.d, p1, [pCRow2]

	add	pCRow2, pCRow1, LDC
    ld1d  z27.d, p1/z, [pCRow1] 
    fmla z27.d, p1/m, z19.d, alphaZ
    st1d  z27.d, p1, [pCRow1]

	add	pCRow1, pCRow2, LDC
    ld1d  z28.d, p1/z, [pCRow2] 
    fmla z28.d, p1/m, z20.d, alphaZ
    st1d  z28.d, p1, [pCRow2]

	add	pCRow2, pCRow1, LDC
    ld1d  z29.d, p1/z, [pCRow1] 
    fmla z29.d, p1/m, z21.d, alphaZ
    st1d  z29.d, p1, [pCRow1]

	add	pCRow1, pCRow2, LDC
    ld1d  z30.d, p1/z, [pCRow2] 
    fmla z30.d, p1/m, z22.d, alphaZ
    st1d  z30.d, p1, [pCRow2]

    ld1d  z31.d, p1/z, [pCRow1] 
    fmla z31.d, p1/m, z23.d, alphaZ
    st1d  z31.d, p1, [pCRow1]

	add	pCRow0, pCRow0, lanes, lsl #3	// pC = pC + lanes  * 8

.endm

/******************************************************************************/

.macro INITv1x4
    dup         z16.d, #0
    dup         z17.d, #0
    dup         z18.d, #0
    dup         z19.d, #0
.endm

.macro KERNELv1x4_SUB
    ld1d  z0.d, p1/z, [pA1] 
	add	pA1, pA1, lanes, lsl #3	// pA1 = pA1 + lanes  * 8

    ld1rd  z8.d, p0/z,  [pB]
    ld1rd  z9.d, p0/z,  [pB, 8]
    ld1rd  z10.d, p0/z, [pB, 16]
    ld1rd  z11.d, p0/z, [pB, 24]

    add pB, pB, 32

    fmla z16.d, p1/m, z0.d, z8.d
    fmla z17.d, p1/m, z0.d, z9.d
    fmla z18.d, p1/m, z0.d, z10.d
    fmla z19.d, p1/m, z0.d, z11.d

.endm

.macro SAVEv1x4


	add	pCRow1, pCRow0, LDC
    ld1d  z24.d, p1/z, [pCRow0] 
    fmla z24.d, p1/m, z16.d, alphaZ
    st1d  z24.d, p1, [pCRow0]

	add	pCRow2, pCRow1, LDC
    ld1d  z25.d, p1/z, [pCRow1] 
    fmla z25.d, p1/m, z17.d, alphaZ
    st1d  z25.d, p1, [pCRow1]

	add	pCRow1, pCRow2, LDC
    ld1d  z26.d, p1/z, [pCRow2] 
    fmla z26.d, p1/m, z18.d, alphaZ
    st1d z26.d, p1, [pCRow2]

    ld1d  z27.d, p1/z, [pCRow1] 
    fmla z27.d, p1/m, z19.d, alphaZ
    st1d  z27.d, p1, [pCRow1]

	add	pCRow0, pCRow0, lanes, lsl #3	// pC = pC + lanes  * 8

.endm

/******************************************************************************/

.macro INITv1x2
    dup         z16.d, #0
    dup         z17.d, #0
.endm

.macro KERNELv1x2_SUB
    ld1d  z0.d, p1/z, [pA1] 
	add	pA1, pA1, lanes, lsl #3	// pA1 = pA1 + lanes  * 8

    ld1rd  z8.d, p0/z,  [pB]
    ld1rd  z9.d, p0/z,  [pB, 8]

    add pB, pB, 16

    fmla z16.d, p1/m, z0.d, z8.d
    fmla z17.d, p1/m, z0.d, z9.d

.endm

.macro SAVEv1x2


	add	pCRow1, pCRow0, LDC
    ld1d  z24.d, p1/z, [pCRow0] 
    fmla z24.d, p1/m, z16.d, alphaZ
    st1d  z24.d, p1, [pCRow0]

    ld1d  z25.d, p1/z, [pCRow1] 
    fmla z25.d, p1/m, z17.d, alphaZ
    st1d  z25.d, p1, [pCRow1]

	add	pCRow0, pCRow0, lanes, lsl #3	// pC = pC + lanes  * 8

.endm

/******************************************************************************/

.macro INITv1x1
    dup         z16.d, #0
.endm

.macro KERNELv1x1_SUB
    ld1d  z0.d, p1/z, [pA1] 
	add	pA1, pA1, lanes, lsl #3	// pA1 = pA1 + lanes  * 8

    ld1rd  z8.d, p0/z,  [pB]

    add pB, pB, 8

    fmla z16.d, p1/m, z0.d, z8.d

.endm

.macro SAVEv1x1


    ld1d  z24.d, p1/z, [pCRow0] 
    fmla z24.d, p1/m, z16.d, alphaZ
    st1d  z24.d, p1, [pCRow0]


	add	pCRow0, pCRow0, lanes, lsl #3	// pC = pC + lanes  * 8

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


	fmov	alpha, d0
	dup	alphaZ, alpha
    cntd vec_len
    lsl vec_lenx2, vec_len, #1

	lsl	LDC, LDC, #3			// ldc = ldc * 8
    ptrue p0.d                  // create true predicate 

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

	mov	pA1, origPA			// pA1 = start of A array

.Ldgemm_kernel_L8_Mv2_BEGIN:

    mov counterI, #0
    cmp origM, vec_lenx2        // Check if M < 2*SVE_LEN
    blt .Ldgemm_kernel_L8_Mv1_BEGIN

    mov counterI, origM

/* Until we have at least 2*SVE_LEN iters left in M, we do them with V2*8 kernel */
    mul temp, vec_len, origK                // generate address of pA2
	add	pA2, pA1, temp, lsl #3			// pA1 = start of A array

	.align 5
.Ldgemm_kernel_L8_Mv2_20:

	mov	pB, origPB
    INITv2x8                     // fill with zeros

	asr 	counterL , origK, #3		// L = K / 8
	cmp	counterL , #2			// is there at least 4 to do?
	blt	.Ldgemm_kernel_L8_Mv2_32

	KERNELv2x8_I
	KERNELv2x8_M2
	KERNELv2x8_M1
	KERNELv2x8_M2
	KERNELv2x8_M1
	KERNELv2x8_M2
	KERNELv2x8_M1
	KERNELv2x8_M2

	subs	counterL, counterL, #2		// subtract 2
	ble	.Ldgemm_kernel_L8_Mv2_22a

	.align 5
.Ldgemm_kernel_L8_Mv2_22:

	KERNELv2x8_M1
	KERNELv2x8_M2
	KERNELv2x8_M1
	KERNELv2x8_M2
	KERNELv2x8_M1
	KERNELv2x8_M2
	KERNELv2x8_M1
	KERNELv2x8_M2

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L8_Mv2_22

	.align 5
.Ldgemm_kernel_L8_Mv2_22a:

	KERNELv2x8_M1
	KERNELv2x8_M2
	KERNELv2x8_M1
	KERNELv2x8_M2
	KERNELv2x8_M1
	KERNELv2x8_M2
	KERNELv2x8_M1
	KERNELv2x8_E

	b	 .Ldgemm_kernel_L8_Mv2_44

	.align 5
.Ldgemm_kernel_L8_Mv2_32:

	tst	counterL, #1
	ble	.Ldgemm_kernel_L8_Mv2_40

	KERNELv2x8_I
	KERNELv2x8_M2
	KERNELv2x8_M1
	KERNELv2x8_M2
	KERNELv2x8_M1
	KERNELv2x8_M2
	KERNELv2x8_M1
	KERNELv2x8_E


	b	.Ldgemm_kernel_L8_Mv2_44

.Ldgemm_kernel_L8_Mv2_40:

	INITv2x8

.Ldgemm_kernel_L8_Mv2_44:

	ands	counterL , origK, #7
	ble	.Ldgemm_kernel_L8_Mv2_100

	.align 5
.Ldgemm_kernel_L8_Mv2_46:

	KERNELv2x8_SUB

	subs	counterL, counterL, #1
	bne	.Ldgemm_kernel_L8_Mv2_46

.Ldgemm_kernel_L8_Mv2_100:

	SAVEv2x8
    mov pA1, pA2                            // pA1 = pA2
    mul temp, vec_len, origK                // generate address of pA2
	add	pA2, pA1, temp, lsl #3			    // 

.Ldgemm_kernel_L8_Mv2_END:
    sub counterI, counterI, vec_lenx2
    cmp counterI, vec_lenx2
    bge .Ldgemm_kernel_L8_Mv2_20
    sub counterI, origM, counterI

    cmp counterI, origM
    beq .Ldgemm_kernel_L8_END

//////////////////////////////////////////
// We have less than 2*SVE_LEN left. We do this with V1x8 kernel.
.Ldgemm_kernel_L8_Mv1_BEGIN:

    whilelt p1.d, counterI, origM               //SVE instruction
    cntp lanes, p0, p1.d                        // lanes contain number of active SVE lanes in M dimension

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

	SAVEv1x8

.Ldgemm_kernel_L8_Mv1_END:

    incd    counterI
    whilelt p1.d, counterI, origM             //SVE instruction
    cntp lanes, p0, p1.d                        // lanes contain number of active SVE lanes in M dimension
    b.any   .Ldgemm_kernel_L8_Mv1_20   

.Ldgemm_kernel_L8_END:

	lsl	temp, origK, #6 
	add	origPB, origPB, temp		// B = B + K * 8 * 8

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

	mov	pA1, origPA			// pA1 = start of A array

.Ldgemm_kernel_L4_Mv2_BEGIN:

    mov counterI, #0
    cmp origM, vec_lenx2
    blt .Ldgemm_kernel_L4_Mv1_BEGIN

    mov counterI, origM

    mul temp, vec_len, origK                // generate address of pA2
	add	pA2, pA1, temp, lsl #3			// pA1 = start of A array

	.align 5
.Ldgemm_kernel_L4_Mv2_20:

	mov	pB, origPB
    INITv2x4                     // fill with zeros

	asr 	counterL , origK, #3		// L = K / 8
	cmp	counterL , #0			// is there at least 4 to do?
	ble	.Ldgemm_kernel_L4_Mv2_44

	.align 5
.Ldgemm_kernel_L4_Mv2_22:

	KERNELv2x4_SUB
	KERNELv2x4_SUB
	KERNELv2x4_SUB
	KERNELv2x4_SUB
	KERNELv2x4_SUB
	KERNELv2x4_SUB
	KERNELv2x4_SUB
	KERNELv2x4_SUB

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L4_Mv2_22

.Ldgemm_kernel_L4_Mv2_44:

	ands	counterL , origK, #7
	ble	.Ldgemm_kernel_L4_Mv2_100

	.align 5
.Ldgemm_kernel_L4_Mv2_46:

	KERNELv2x4_SUB

	subs	counterL, counterL, #1
	bne	.Ldgemm_kernel_L4_Mv2_46

.Ldgemm_kernel_L4_Mv2_100:

	SAVEv2x4
    mov pA1, pA2                            // pA1 = pA2
    mul temp, vec_len, origK                // generate address of pA2
	add	pA2, pA1, temp, lsl #3			    // 

.Ldgemm_kernel_L4_Mv2_END:
    sub counterI, counterI, vec_lenx2
    cmp counterI, vec_lenx2
    bge .Ldgemm_kernel_L4_Mv2_20
    sub counterI, origM, counterI

    cmp counterI, origM
    beq .Ldgemm_kernel_L4_END

//////////////////////////////////
// We have less than 2*SVE_LEN left. We do this with V1x4 kernel.
.Ldgemm_kernel_L4_Mv1_BEGIN:

    whilelt p1.d, counterI, origM               //SVE instruction
    cntp lanes, p0, p1.d                        // lanes contain number of active SVE lanes in M dimension

	.align 5
.Ldgemm_kernel_L4_Mv1_20:

	mov	pB, origPB
    INITv1x4                     // fill with zeros

	asr 	counterL , origK, #3		// L = K / 8
	cmp	counterL , #0			// is there at least 4 to do?
	ble	.Ldgemm_kernel_L4_Mv1_44

	.align 5
.Ldgemm_kernel_L4_Mv1_22:

	KERNELv1x4_SUB
	KERNELv1x4_SUB
	KERNELv1x4_SUB
	KERNELv1x4_SUB
	KERNELv1x4_SUB
	KERNELv1x4_SUB
	KERNELv1x4_SUB
	KERNELv1x4_SUB

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L4_Mv1_22

.Ldgemm_kernel_L4_Mv1_44:

	ands	counterL , origK, #7
	ble	.Ldgemm_kernel_L4_Mv1_100

	.align 5
.Ldgemm_kernel_L4_Mv1_46:

	KERNELv1x4_SUB

	subs	counterL, counterL, #1
	bne	.Ldgemm_kernel_L4_Mv1_46

.Ldgemm_kernel_L4_Mv1_100:

	SAVEv1x4

.Ldgemm_kernel_L4_Mv1_END:

    incd    counterI
    whilelt p1.d, counterI, origM             //SVE instruction
    cntp lanes, p0, p1.d
    b.any   .Ldgemm_kernel_L4_Mv1_20   


.Ldgemm_kernel_L4_END:
	lsl	temp, origK, #5 
	add	origPB, origPB, temp	// B = B + K * 4 * 8

/******************************************************************************/
/* Repeat the same thing if 2 left in N */

	.align 5
.Ldgemm_kernel_L2_BEGIN:

	mov	counterJ , origN
	tst	counterJ , #2
	ble	.Ldgemm_kernel_L1_BEGIN

	mov	pCRow0, pC

    add pC, pC, LDC, lsl #1 // add 2 x LDC

	mov	pA1, origPA			// pA1 = start of A array

.Ldgemm_kernel_L2_Mv2_BEGIN:

    mov counterI, #0
    cmp origM, vec_lenx2
    blt .Ldgemm_kernel_L2_Mv1_BEGIN

    mov counterI, origM

    mul temp, vec_len, origK                // generate address of pA2
	add	pA2, pA1, temp, lsl #3			// pA1 = start of A array

	.align 5
.Ldgemm_kernel_L2_Mv2_20:

	mov	pB, origPB
    INITv2x2                     // fill with zeros

	asr 	counterL , origK, #3		// L = K / 8
	cmp	counterL , #0			// is there at least 4 to do?
	ble	.Ldgemm_kernel_L2_Mv2_44

	.align 5
.Ldgemm_kernel_L2_Mv2_22:

	KERNELv2x2_SUB
	KERNELv2x2_SUB
	KERNELv2x2_SUB
	KERNELv2x2_SUB
	KERNELv2x2_SUB
	KERNELv2x2_SUB
	KERNELv2x2_SUB
	KERNELv2x2_SUB

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L2_Mv2_22

.Ldgemm_kernel_L2_Mv2_44:

	ands	counterL , origK, #7
	ble	.Ldgemm_kernel_L2_Mv2_100

	.align 5
.Ldgemm_kernel_L2_Mv2_46:

	KERNELv2x2_SUB

	subs	counterL, counterL, #1
	bne	.Ldgemm_kernel_L2_Mv2_46

.Ldgemm_kernel_L2_Mv2_100:

	SAVEv2x2
    mov pA1, pA2                            // pA1 = pA2
    mul temp, vec_len, origK                // generate address of pA2
	add	pA2, pA1, temp, lsl #3			    // 

.Ldgemm_kernel_L2_Mv2_END:
    sub counterI, counterI, vec_lenx2
    cmp counterI, vec_lenx2
    bge .Ldgemm_kernel_L2_Mv2_20
    sub counterI, origM, counterI

    cmp counterI, origM
    beq .Ldgemm_kernel_L2_END


//////////////////////////////////
// We have less than 2*SVE_LEN left. We do this with V1x2 kernel.
.Ldgemm_kernel_L2_Mv1_BEGIN:

    whilelt p1.d, counterI, origM               //SVE instruction
    cntp lanes, p0, p1.d

	.align 5
.Ldgemm_kernel_L2_Mv1_20:

	mov	pB, origPB
    INITv1x2                     // fill with zeros

	asr 	counterL , origK, #3		// L = K / 8
	cmp	counterL , #0			// is there at least 4 to do?
	ble	.Ldgemm_kernel_L2_Mv1_44

	.align 5
.Ldgemm_kernel_L2_Mv1_22:

	KERNELv1x2_SUB
	KERNELv1x2_SUB
	KERNELv1x2_SUB
	KERNELv1x2_SUB
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

	KERNELv1x2_SUB

	subs	counterL, counterL, #1
	bne	.Ldgemm_kernel_L2_Mv1_46

.Ldgemm_kernel_L2_Mv1_100:

	SAVEv1x2

.Ldgemm_kernel_L2_Mv1_END:

    incd    counterI
    whilelt p1.d, counterI, origM             //SVE instruction
    cntp lanes, p0, p1.d
    b.any   .Ldgemm_kernel_L2_Mv1_20   


.Ldgemm_kernel_L2_END:
	add	origPB, origPB, origK, lsl #4	// B = B + K * 2 * 8

/******************************************************************************/
/* Repeat the same thing if 1 left in N */

	.align 5
.Ldgemm_kernel_L1_BEGIN:

	mov	counterJ , origN
	tst	counterJ , #1
	ble	.Ldgemm_kernel_L999 // done

	mov	pCRow0, pC

    add pC, pC, LDC // add 1 x LDC

	mov	pA1, origPA			// pA1 = start of A array

.Ldgemm_kernel_L1_Mv2_BEGIN:

    mov counterI, #0
    cmp origM, vec_lenx2
    blt .Ldgemm_kernel_L1_Mv1_BEGIN

    mov counterI, origM

    mul temp, vec_len, origK                // generate address of pA2
	add	pA2, pA1, temp, lsl #3			// pA1 = start of A array


	.align 5
.Ldgemm_kernel_L1_Mv2_20:

	mov	pB, origPB
    INITv2x1                     // fill with zeros

	asr 	counterL , origK, #3		// L = K / 8
	cmp	counterL , #0			// is there at least 8 to do?
	ble	.Ldgemm_kernel_L1_Mv2_44

	.align 5
.Ldgemm_kernel_L1_Mv2_22:

	KERNELv2x1_SUB
	KERNELv2x1_SUB
	KERNELv2x1_SUB
	KERNELv2x1_SUB
	KERNELv2x1_SUB
	KERNELv2x1_SUB
	KERNELv2x1_SUB
	KERNELv2x1_SUB

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L1_Mv2_22

.Ldgemm_kernel_L1_Mv2_44:

	ands	counterL , origK, #7
	ble	.Ldgemm_kernel_L1_Mv2_100

	.align 5
.Ldgemm_kernel_L1_Mv2_46:

	KERNELv2x1_SUB

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L1_Mv2_46

.Ldgemm_kernel_L1_Mv2_100:

	SAVEv2x1
    mov pA1, pA2                            // pA1 = pA2
    mul temp, vec_len, origK                // generate address of pA2
	add	pA2, pA1, temp, lsl #3			    // 

.Ldgemm_kernel_L1_Mv2_END:
    sub counterI, counterI, vec_lenx2
    cmp counterI, vec_lenx2
    bge .Ldgemm_kernel_L1_Mv2_20
    sub counterI, origM, counterI

    cmp counterI, origM
    beq .Ldgemm_kernel_L1_END


//////////////////////////////////
// We have less than 2*SVE_LEN left. We do this with V1x1 kernel.
.Ldgemm_kernel_L1_Mv1_BEGIN:

    whilelt p1.d, counterI, origM               //SVE instruction
    cntp lanes, p0, p1.d

	.align 5
.Ldgemm_kernel_L1_Mv1_20:

	mov	pB, origPB
    INITv1x1                     // fill with zeros

	asr 	counterL , origK, #3		// L = K / 8
	cmp	counterL , #0			// is there at least 8 to do?
	ble	.Ldgemm_kernel_L1_Mv1_44

	.align 5
.Ldgemm_kernel_L1_Mv1_22:

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

	KERNELv1x1_SUB

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L1_Mv1_46

.Ldgemm_kernel_L1_Mv1_100:

	SAVEv1x1

.Ldgemm_kernel_L1_Mv1_END:

    incd    counterI
    whilelt p1.d, counterI, origM             //SVE instruction
    cntp lanes, p0, p1.d
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

