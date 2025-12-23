/***************************************************************************
Copyright (c) 2016, The OpenBLAS Project
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
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A00 PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE OPENBLAS PROJECT OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*****************************************************************************/
#define ASSEMBLER
#include "common.h"

#define	M		x0
#define	N		x1
#define	A		x2
#define	LDA		x3
#define	B		x4

#define M8		x5

#define	A01		x6
#define	A02		x7
#define	A03		x8
#define	A04		x9
#define	A05		x10
#define	A06		x11
#define	A07		x12
#define	A08		x13

#define	B01		x14
#define	B02		x15
#define	B03		x16
#define	B04		x17
#define	B00		x22


#define I		x18
#define	J		x19

#define TEMP1		x20

#define A_PREFETCH	256

/**************************************************************************************
* Macro definitions
**************************************************************************************/
.macro SAVE_REGS
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
.endm

.macro RESTORE_REGS
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
.endm

/*************************************************************************************************************************/

.macro COPY8x8
	prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	prfm	PLDL1KEEP, [A02, #A_PREFETCH]
	prfm	PLDL1KEEP, [A03, #A_PREFETCH]
	prfm	PLDL1KEEP, [A04, #A_PREFETCH]
	prfm	PLDL1KEEP, [A05, #A_PREFETCH]
	prfm	PLDL1KEEP, [A06, #A_PREFETCH]
	prfm	PLDL1KEEP, [A07, #A_PREFETCH]
	prfm	PLDL1KEEP, [A08, #A_PREFETCH]

	ldp	q0, q1, [A01]
	ldp	q2, q3, [A02]
	add	A01, A01, #32
	add	A02, A02, #32

	st1	{v0.4s, v1.4s, v2.4s, v3.4s}, [B00]
	add	TEMP1, B00, #64

	ldp	q4, q5, [A03]
	ldp	q6, q7, [A04]
	add	A03, A03, #32
	add	A04, A04, #32

	st1	{v4.4s, v5.4s, v6.4s, v7.4s}, [TEMP1]
	add	TEMP1, TEMP1, #64

	ldp	q8, q9, [A05]
	ldp	q10, q11, [A06]
	add	A05, A05, #32
	add	A06, A06, #32

	st1	{v8.4s, v9.4s, v10.4s, v11.4s}, [TEMP1]
	add	TEMP1, TEMP1, #64

	ldp	q12, q13, [A07]
	ldp	q14, q15, [A08]
	add	A07, A07, #32
	add	A08, A08, #32

	st1	{v12.4s, v13.4s, v14.4s, v15.4s}, [TEMP1]
	add	TEMP1, TEMP1, #64

	add	B00, B00, M8
.endm

.macro COPY4x8
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A03, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A04, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A05, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A06, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A07, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A08, #A_PREFETCH]

	ldr	q0, [A01]
	ldr	q1, [A02]
	ldr	q2, [A03]
	ldr	q3, [A04]
	add	A01, A01, #16
	add	A02, A02, #16
	add	A03, A03, #16
	add	A04, A04, #16

	st1	{v0.4s, v1.4s, v2.4s, v3.4s}, [B01]
	add	B01, B01, #64

	ldr	q4, [A05]
	ldr	q5, [A06]
	ldr	q6, [A07]
	ldr	q7, [A08]

	add	A05, A05, #16
	add	A06, A06, #16
	add	A07, A07, #16
	add	A08, A08, #16

	st1	{v4.4s, v5.4s, v6.4s, v7.4s}, [B01]
	add	B01, B01, #64
.endm

.macro COPY2x8
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A03, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A04, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A05, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A06, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A07, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A08, #A_PREFETCH]

	ldr	d0, [A01]
	ldr	d1, [A02]
	ldr	d2, [A03]
	ldr	d3, [A04]

	add	A01, A01, #8
	add	A02, A02, #8
	add	A03, A03, #8
	add	A04, A04, #8

	stp	d0, d1, [B02]
	add	B02, B02, #16
	stp	d2, d3, [B02]
	add	B02, B02, #16

	ldr	d4, [A05]
	ldr	d5, [A06]
	ldr	d6, [A07]
	ldr	d7, [A08]

	add	A05, A05, #8
	add	A06, A06, #8
	add	A07, A07, #8
	add	A08, A08, #8

	stp	d4, d5, [B02]
	add	B02, B02, #16
	stp	d6, d7, [B02]
	add	B02, B02, #16

.endm

.macro COPY1x8
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A03, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A04, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A05, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A06, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A07, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A08, #A_PREFETCH]

	ldr	s0, [A01]
	ldr	s1, [A02]
	ldr	s2, [A03]
	ldr	s3, [A04]

	add	A01, A01, #4
	add	A02, A02, #4
	add	A03, A03, #4
	add	A04, A04, #4

	stp	s0, s1, [B03]
	add	B03, B03, #8
	stp	s2, s3, [B03]
	add	B03, B03, #8

	ldr	s4, [A05]
	ldr	s5, [A06]
	ldr	s6, [A07]
	ldr	s7, [A08]

	ldr	d4, [A05], #8
	ldr	d5, [A06], #8
	ldr	d6, [A07], #8
	ldr	d7, [A08], #8

	stp	s4, s5, [B03]
	add	B03, B03, #8
	stp	s6, s7, [B03]
	add	B03, B03, #8

.endm

/*************************************************************************************************************************/

.macro COPY8x4
	prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	prfm	PLDL1KEEP, [A02, #A_PREFETCH]
	prfm	PLDL1KEEP, [A03, #A_PREFETCH]
	prfm	PLDL1KEEP, [A04, #A_PREFETCH]

	ldp	q0, q1, [A01]
	ldp	q2, q3, [A02]
	add	A01, A01, #32
	add	A02, A02, #32

	st1	{v0.4s, v1.4s, v2.4s, v3.4s}, [B00]
	add	TEMP1, B00, #64

	ldp	q4, q5, [A03]
	ldp	q6, q7, [A04]
	add	A03, A03, #32
	add	A04, A04, #32

	st1	{v4.4s, v5.4s, v6.4s, v7.4s}, [TEMP1]
	add	TEMP1, TEMP1, #64

	add	B00, B00, M8
.endm

.macro COPY4x4
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A03, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A04, #A_PREFETCH]

	ldr	q0, [A01]
	ldr	q1, [A02]
	ldr	q2, [A03]
	ldr	q3, [A04]
	add	A01, A01, #16
	add	A02, A02, #16
	add	A03, A03, #16
	add	A04, A04, #16

	st1	{v0.4s, v1.4s, v2.4s, v3.4s}, [B01]

	add	B01, B01, #64
.endm

.macro COPY2x4
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A03, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A04, #A_PREFETCH]

	ldr	d0, [A01]
	ldr	d1, [A02]
	ldr	d2, [A03]
	ldr	d3, [A04]

	add	A01, A01, #8
	add	A02, A02, #8
	add	A03, A03, #8
	add	A04, A04, #8

	stp	d0, d1, [B02]
	add	B02, B02, #16
	stp	d2, d3, [B02]

	add	B02, B02, #16
.endm

.macro COPY1x4
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A03, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A04, #A_PREFETCH]

	ldr	s0, [A01]
	ldr	s1, [A02]
	ldr	s2, [A03]
	ldr	s3, [A04]

	add	A01, A01, #4
	add	A02, A02, #4
	add	A03, A03, #4
	add	A04, A04, #4

	stp	s0, s1, [B03]
	add	B03, B03, #8
	stp	s2, s3, [B03]
	add	B03, B03, #8

.endm

/*************************************************************************************************************************/

.macro COPY8x2
	prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	prfm	PLDL1KEEP, [A02, #A_PREFETCH]

	ld1	{v0.4s, v1.4s}, [A01]
	ld1	{v2.4s, v3.4s}, [A02]
	add	A01, A01, #32
	add	A02, A02, #32

	st1	{v0.4s, v1.4s, v2.4s, v3.4s}, [B00]
	add	B00, B00, M8
.endm

.macro COPY4x2
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]

	ldr	q0, [A01]
	ldr	q1, [A02]
	add	A01, A01, #16
	add	A02, A02, #16

	stp	q0, q1, [B01]
	add	B01, B01, #32
.endm

.macro COPY2x2
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]

	ldr	d0, [A01]
	ldr	d1, [A02]

	add	A01, A01, #8
	add	A02, A02, #8

	stp	d0, d1, [B02]
	add	B02, B02, #16
.endm

.macro COPY1x2
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]

	ldr	s0, [A01]
	ldr	s1, [A02]

	add	A01, A01, #4
	add	A02, A02, #4

	stp	s0, s1, [B03]

	add	B03, B03, #8
.endm

/*************************************************************************************************************************/

.macro COPY8x1
	prfm	PLDL1KEEP, [A01, #A_PREFETCH]

	ldp	q0, q1, [A01]
	add	A01, A01, #32
	stp	q0, q1, [B00]

	add	B00, B00, M8
.endm

.macro COPY4x1
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]

	ldr q0, [A01]
	add	A01, A01, #16
	str q0, [B01]

	add	B01, B01, #16
.endm

.macro COPY2x1
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]

	ldr	d0, [A01]
	add	A01, A01, #8
	str d0, [B02]

	add	B02, B02, #8
.endm

.macro COPY1x1
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]

	ldr	s0, [A01]
	add	A01, A01, #4
	str	s0, [B03]

	add	B03, B03, #4
.endm

/**************************************************************************************
* End of macro definitions
**************************************************************************************/

	PROLOGUE

	.align 5

	SAVE_REGS

	lsl	LDA, LDA, #2					// LDA = LDA * SIZE

	lsl	TEMP1, M, #2					// TEMP1 = M * SIZE

	and	B01 , N , #-8
	and	B02 , N , #-4
	and	B03 , N , #-2

	mul	B01, B01, TEMP1
	mul	B02, B02, TEMP1
	mul	B03, B03, TEMP1

	add	B01 , B01, B
	add	B02 , B02, B
	add	B03 , B03, B

	lsl	M8, M, #5					// M8 = M * 8 * SIZE

.Lsgemm_tcopy_L8_BEGIN:

	asr 	J, M, #3					// J = M / 8
	cmp 	J, #0
	ble	.Lsgemm_tcopy_L4_BEGIN

	.align	5
.Lsgemm_tcopy_L8_M8_BEGIN:

	mov	A01, A
	add	A02, A01, LDA
	add	A03, A02, LDA
	add	A04, A03, LDA
	add	A05, A04, LDA
	add	A06, A05, LDA
	add	A07, A06, LDA
	add	A08, A07, LDA
	add	A, A08, LDA

	mov	B00, B
	add	B, B00, #256					// B = B + 8 * 8 * SIZE

	asr	I, N, #3					// I = N / 8
	cmp 	I, #0
	ble	.Lsgemm_tcopy_L8_M8_40

	.align	5
.Lsgemm_tcopy_L8_M8_20:

	COPY8x8

	subs	I , I , #1
	bne	.Lsgemm_tcopy_L8_M8_20

.Lsgemm_tcopy_L8_M8_40:

	tst	N , #4
	ble	.Lsgemm_tcopy_L8_M8_60

	COPY4x8

.Lsgemm_tcopy_L8_M8_60:

	tst	N , #2
	ble	.Lsgemm_tcopy_L8_M8_80

	COPY2x8

.Lsgemm_tcopy_L8_M8_80:

	tst	N, #1
	ble	.Lsgemm_tcopy_L8_M8_END

	COPY1x8

.Lsgemm_tcopy_L8_M8_END:

	subs	J, J, #1						// j--
	bne	.Lsgemm_tcopy_L8_M8_BEGIN

/*********************************************************************************************/

.Lsgemm_tcopy_L4_BEGIN:

	tst	M, #7
	ble	.Lsgemm_tcopy_L999

	tst	M, #4
	ble	.Lsgemm_tcopy_L2_BEGIN

.Lsgemm_tcopy_L4_M8_BEGIN:

	mov	A01, A
	add	A02, A01, LDA
	add	A03, A02, LDA
	add	A04, A03, LDA
	add	A, A04, LDA

	mov	B00, B
	add	B, B00, #128					// B = B + 4 * 8 * SIZE

	asr	I, N, #3					// I = N / 8
	cmp 	I, #0
	ble	.Lsgemm_tcopy_L4_M8_40

	.align	5
.Lsgemm_tcopy_L4_M8_20:

	COPY8x4

	subs	I , I , #1
	bne	.Lsgemm_tcopy_L4_M8_20

.Lsgemm_tcopy_L4_M8_40:

	tst	N , #4
	ble	.Lsgemm_tcopy_L4_M8_60

	COPY4x4

.Lsgemm_tcopy_L4_M8_60:

	tst	N , #2
	ble	.Lsgemm_tcopy_L4_M8_80

	COPY2x4

.Lsgemm_tcopy_L4_M8_80:

	tst	N , #1
	ble	.Lsgemm_tcopy_L4_M8_END

	COPY1x4


.Lsgemm_tcopy_L4_M8_END:

/*********************************************************************************************/

.Lsgemm_tcopy_L2_BEGIN:

	tst	M, #3
	ble	.Lsgemm_tcopy_L999

	tst	M, #2
	ble	.Lsgemm_tcopy_L1_BEGIN

.Lsgemm_tcopy_L2_M16_BEGIN:

	mov	A01, A
	add	A02, A01, LDA
	add	A, A02, LDA

	mov	B00, B
	add	B, B00, #64					// B = B + 2 * 8 * SIZE

	asr	I, N, #3					// I = N / 8
	cmp 	I, #0
	ble	.Lsgemm_tcopy_L2_M8_40

	.align	5
.Lsgemm_tcopy_L2_M8_20:

	COPY8x2

	subs	I , I , #1
	bne	.Lsgemm_tcopy_L2_M8_20

.Lsgemm_tcopy_L2_M8_40:

	tst	N , #4
	ble	.Lsgemm_tcopy_L2_M8_60

	COPY4x2

.Lsgemm_tcopy_L2_M8_60:

	tst	N , #2
	ble	.Lsgemm_tcopy_L2_M8_80

	COPY2x2

.Lsgemm_tcopy_L2_M8_80:

	tst	N , #1
	ble	.Lsgemm_tcopy_L2_M8_END

	COPY1x2

.Lsgemm_tcopy_L2_M8_END:

/*********************************************************************************************/

.Lsgemm_tcopy_L1_BEGIN:

	tst	M, #1
	ble	.Lsgemm_tcopy_L999


.Lsgemm_tcopy_L1_M16_BEGIN:

	mov	A01, A						// A01 = A
	mov	B00, B

	asr	I, N, #3					// I = M / 8
	cmp 	I, #0
	ble	.Lsgemm_tcopy_L1_M8_40

	.align	5
.Lsgemm_tcopy_L1_M8_20:

	COPY8x1

	subs	I , I , #1
	bne	.Lsgemm_tcopy_L1_M8_20

.Lsgemm_tcopy_L1_M8_40:

	tst	N , #4
	ble	.Lsgemm_tcopy_L1_M8_60

	COPY4x1

.Lsgemm_tcopy_L1_M8_60:

	tst	N , #2
	ble	.Lsgemm_tcopy_L1_M8_80

	COPY2x1

.Lsgemm_tcopy_L1_M8_80:

	tst	N , #1
	ble	.Lsgemm_tcopy_L1_M8_END

	COPY1x1


.Lsgemm_tcopy_L1_M8_END:

.Lsgemm_tcopy_L999:

	mov	x0, #0						// set return value
	RESTORE_REGS
	ret

	EPILOGUE
