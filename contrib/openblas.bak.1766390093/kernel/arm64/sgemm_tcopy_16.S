/***************************************************************************
Copyright (c) 2019, The OpenBLAS Project
All rights reserved.
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


#define I		x21
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

.macro COPY16x8
	prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	prfm	PLDL1KEEP, [A02, #A_PREFETCH]
	prfm	PLDL1KEEP, [A03, #A_PREFETCH]
	prfm	PLDL1KEEP, [A04, #A_PREFETCH]
	prfm	PLDL1KEEP, [A05, #A_PREFETCH]
	prfm	PLDL1KEEP, [A06, #A_PREFETCH]
	prfm	PLDL1KEEP, [A07, #A_PREFETCH]
	prfm	PLDL1KEEP, [A08, #A_PREFETCH]
	//prfm	PSTL1KEEP, [B00, M8]
	
	ld1 {v0.4s, v1.4s, v2.4s, v3.4s}, [A01]
	add  A01, A01, #64
	
	st1 {v0.4s, v1.4s, v2.4s, v3.4s}, [B00]
	add TEMP1, B00, #64

	ld1 {v4.4s, v5.4s, v6.4s, v7.4s}, [A02]
	add  A02, A02, #64
	
	st1 {v4.4s, v5.4s, v6.4s, v7.4s}, [TEMP1]
	add TEMP1, TEMP1, #64

	ld1 {v8.4s, v9.4s, v10.4s, v11.4s}, [A03]
	add  A03, A03, #64
	
	st1 {v8.4s, v9.4s, v10.4s, v11.4s}, [TEMP1]
	add TEMP1, TEMP1, #64

	ld1 {v12.4s, v13.4s, v14.4s, v15.4s}, [A04]
	add  A04, A04, #64
	
	st1 {v12.4s, v13.4s, v14.4s, v15.4s}, [TEMP1]
	add TEMP1, TEMP1, #64

	ld1 {v16.4s, v17.4s, v18.4s, v19.4s}, [A05]
	add  A05, A05, #64
	
	st1 {v16.4s, v17.4s, v18.4s, v19.4s}, [TEMP1]
	add TEMP1, TEMP1, #64

	ld1 {v20.4s, v21.4s, v22.4s, v23.4s}, [A06]
	add  A06, A06, #64
	
	st1 {v20.4s, v21.4s, v22.4s, v23.4s}, [TEMP1]
	add TEMP1, TEMP1, #64

	ld1 {v24.4s, v25.4s, v26.4s, v27.4s}, [A07]
	add  A07, A07, #64
	
	st1 {v24.4s, v25.4s, v26.4s, v27.4s}, [TEMP1]
	add TEMP1, TEMP1, #64

	ld1 {v28.4s, v29.4s, v30.4s, v31.4s}, [A08]
	add  A08, A08, #64
	
	st1 {v28.4s, v29.4s, v30.4s, v31.4s}, [TEMP1]
	add TEMP1, TEMP1, #64
	
	add	B00, B00, M8

.endm

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
	
	st1	{v0.4s, v1.4s, v2.4s, v3.4s}, [B01]
	add	B01, B01, #64
	
	ldp	q4, q5, [A03]
	ldp	q6, q7, [A04]
	add	A03, A03, #32
	add	A04, A04, #32

	st1	{v4.4s, v5.4s, v6.4s, v7.4s}, [B01]
	add	B01, B01, #64

	ldp	q8, q9, [A05]
	ldp	q10, q11, [A06]
	add	A05, A05, #32
	add	A06, A06, #32

	st1	{v8.4s, v9.4s, v10.4s, v11.4s}, [B01]
	add	B01, B01, #64

	ldp	q12, q13, [A07]
	ldp	q14, q15, [A08]
	add	A07, A07, #32
	add	A08, A08, #32

	st1	{v12.4s, v13.4s, v14.4s, v15.4s}, [B01]
	add	B01, B01, #64
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

	st1	{v0.4s, v1.4s, v2.4s, v3.4s}, [B02]
	add	B02, B02, #64

	ldr	q4, [A05]
	ldr	q5, [A06]
	ldr	q6, [A07]
	ldr	q7, [A08]

	add	A05, A05, #16
	add	A06, A06, #16
	add	A07, A07, #16
	add	A08, A08, #16

	st1	{v4.4s, v5.4s, v6.4s, v7.4s}, [B02]
	add	B02, B02, #64
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

	stp	d0, d1, [B03]
	add	B03, B03, #16
	stp	d2, d3, [B03]
	add	B03, B03, #16

	ldr	d4, [A05]
	ldr	d5, [A06]
	ldr	d6, [A07]
	ldr	d7, [A08]
	
	add	A05, A05, #8
	add	A06, A06, #8
	add	A07, A07, #8
	add	A08, A08, #8

	stp	d4, d5, [B03]
	add	B03, B03, #16
	stp	d6, d7, [B03]
	add	B03, B03, #16

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

	stp	s0, s1, [B04]
	add	B04, B04, #8
	stp	s2, s3, [B04]
	add	B04, B04, #8

	ldr	s4, [A05]
	ldr	s5, [A06]
	ldr	s6, [A07]
	ldr	s7, [A08]

	stp	s4, s5, [B04]
	add	B04, B04, #8
	stp	s6, s7, [B04]
	add	B04, B04, #8

.endm

/*************************************************************************************************************************/
.macro COPY16x4
	prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	prfm	PLDL1KEEP, [A02, #A_PREFETCH]
	prfm	PLDL1KEEP, [A03, #A_PREFETCH]
	prfm	PLDL1KEEP, [A04, #A_PREFETCH]

	ld1	{v0.4s, v1.4s, v2.4s, v3.4s}, [A01]
	add	A01, A01, #64

	st1	{v0.4s, v1.4s, v2.4s, v3.4s}, [B00]
	add	TEMP1, B00, #64

	ld1	{v4.4s, v5.4s, v6.4s, v7.4s}, [A02]
	add	A02, A02, #64

	st1	{v4.4s, v5.4s, v6.4s, v7.4s}, [TEMP1]
	add	TEMP1, TEMP1, #64

	ld1	{v8.4s, v9.4s, v10.4s, v11.4s}, [A03]
	add	A03, A03, #64

	st1	{v8.4s, v9.4s, v10.4s, v11.4s}, [TEMP1]
	add	TEMP1, TEMP1, #64

	ld1	{v12.4s, v13.4s, v14.4s, v15.4s}, [A04]
	add	A04, A04, #64

	st1	{v12.4s, v13.4s, v14.4s, v15.4s}, [TEMP1]

	add	B00, B00, M8
.endm

.macro COPY8x4
	prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	prfm	PLDL1KEEP, [A02, #A_PREFETCH]
	prfm	PLDL1KEEP, [A03, #A_PREFETCH]
	prfm	PLDL1KEEP, [A04, #A_PREFETCH]

	ldp	q0, q1, [A01]
	ldp	q2, q3, [A02]
	add	A01, A01, #32
	add	A02, A02, #32

	st1	{v0.4s, v1.4s, v2.4s, v3.4s}, [B01]
	add	B01, B01, #64

	ldp	q4, q5, [A03]
	ldp	q6, q7, [A04]
	add	A03, A03, #32
	add	A04, A04, #32

	st1	{v4.4s, v5.4s, v6.4s, v7.4s}, [B01]
	add	B01, B01, #64
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

	st1	{v0.4s, v1.4s, v2.4s, v3.4s}, [B02]

	add	B02, B02, #64
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
	
	stp	d0, d1, [B03]
	add	B03, B03, #16
	stp	d2, d3, [B03]

	add	B03, B03, #16
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

	stp	s0, s1, [B04]
	add	B04, B04, #8
	stp	s2, s3, [B04]
	add	B04, B04, #8

.endm

/*************************************************************************************************************************/

.macro COPY16x2
	prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	prfm	PLDL1KEEP, [A02, #A_PREFETCH]

	ld1	{v0.4s, v1.4s, v2.4s, v3.4s}, [A01]
	add	A01, A01, #64
	
	ld1	{v4.4s, v5.4s, v6.4s, v7.4s}, [A02]
	add	A02, A02, #64

	st1	{v0.4s, v1.4s, v2.4s, v3.4s}, [B00]
	add	TEMP1, B00, #64
	st1	{v4.4s, v5.4s, v6.4s, v7.4s}, [TEMP1]
	add	B00, B00, M8
.endm

.macro COPY8x2
	prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	prfm	PLDL1KEEP, [A02, #A_PREFETCH]

	ld1	{v0.4s, v1.4s}, [A01]
	ld1	{v2.4s, v3.4s}, [A02]
	add	A01, A01, #32
	add	A02, A02, #32

	st1	{v0.4s, v1.4s, v2.4s, v3.4s}, [B01]
	add	B01, B01, #64
.endm

.macro COPY4x2
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]

	ldr	q0, [A01]
	ldr	q1, [A02]
	add	A01, A01, #16
	add	A02, A02, #16

	stp	q0, q1, [B02]
	add	B02, B02, #32
.endm

.macro COPY2x2
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]

	ldr	d0, [A01]
	ldr	d1, [A02]
	
	add	A01, A01, #8
	add	A02, A02, #8
	
	stp	d0, d1, [B03]
	add	B03, B03, #16
.endm

.macro COPY1x2
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]

	ldr	s0, [A01]
	ldr	s1, [A02]
	
	add	A01, A01, #4
	add	A02, A02, #4

	stp	s0, s1, [B04]

	add	B04, B04, #8
.endm

/*************************************************************************************************************************/

.macro COPY16x1
	prfm	PLDL1KEEP, [A01, #A_PREFETCH]

	ld1	{v0.4s, v1.4s, v2.4s, v3.4s}, [A01]
	add	A01, A01, #64

	st1	{v0.4s, v1.4s, v2.4s, v3.4s}, [B00]
	add	B00, B00, M8
.endm

.macro COPY8x1
	prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	
	ldp	q0, q1, [A01]
	add	A01, A01, #32
	stp	q0, q1, [B01]

	add	B01, B01, #32
.endm

.macro COPY4x1
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]

	ldr q0, [A01]
	add	A01, A01, #16
	str q0, [B02]

	add	B02, B02, #16
.endm

.macro COPY2x1
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]

	ldr	d0, [A01]
	add	A01, A01, #8
	str d0, [B03]

	add	B03, B03, #8
.endm

.macro COPY1x1
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]

	ldr	s0, [A01]
	add	A01, A01, #4
	str	s0, [B04]

	add	B04, B04, #4
.endm

/**************************************************************************************
* End of macro definitions
**************************************************************************************/

	PROLOGUE

	.align 5

	SAVE_REGS

	lsl	LDA, LDA, #2					// LDA = LDA * SIZE

	lsl	TEMP1, M, #2					// TEMP1 = M * SIZE

	and	B01 , N , #-16
	and	B02 , N , #-8
	and	B03 , N , #-4
	and	B04 , N , #-2

	mul	B01, B01, TEMP1
	mul	B02, B02, TEMP1
	mul	B03, B03, TEMP1
	mul	B04, B04, TEMP1

	add	B01 , B01, B
	add	B02 , B02, B
	add	B03 , B03, B
	add	B04 , B04, B

	lsl	M8, M, #6					// M8 = M * 16 * SIZE

.Lsgemm_tcopy_L8_BEGIN:
	asr 	J, M, #3					// J = M / 8
	cmp 	J, #0
	ble	.Lsgemm_tcopy_L4_BEGIN

	.align	5
.Lsgemm_tcopy_L8_M16_BEGIN:

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
	add	B, B00, #512					// B = B + 8 * 16 * SIZE

	asr	I, N, #4					// I = N / 16
	cmp 	I, #0
	ble	.Lsgemm_tcopy_L8_M16_40

	.align	5
.Lsgemm_tcopy_L8_M16_20:

	COPY16x8

	subs	I , I , #1
	bne	.Lsgemm_tcopy_L8_M16_20

.Lsgemm_tcopy_L8_M16_40:
	tst	N , #8
	ble	.Lsgemm_tcopy_L8_M16_60

	COPY8x8
	
.Lsgemm_tcopy_L8_M16_60:
	tst	N , #4
	ble	.Lsgemm_tcopy_L8_M16_80

	COPY4x8

.Lsgemm_tcopy_L8_M16_80:

	tst	N , #2
	ble	.Lsgemm_tcopy_L8_M16_100

	COPY2x8

.Lsgemm_tcopy_L8_M16_100:

	tst	N, #1
	ble	.Lsgemm_tcopy_L8_M16_END

	COPY1x8

.Lsgemm_tcopy_L8_M16_END:

	subs	J , J, #1						// j--
	bne	.Lsgemm_tcopy_L8_M16_BEGIN

/*********************************************************************************************/

.Lsgemm_tcopy_L4_BEGIN:
	tst	M, #7
	ble	.Lsgemm_tcopy_L999

	tst	M, #4
	ble	.Lsgemm_tcopy_L2_BEGIN
	
.Lsgemm_tcopy_L4_M16_BEGIN:

	mov	A01, A
	add	A02, A01, LDA
	add	A03, A02, LDA
	add	A04, A03, LDA
	add	A, A04, LDA

	mov	B00, B
	add	B, B00, #256					// B = B + 4 * 16 * SIZE

	asr	I, N, #4					// I = N / 16
	cmp 	I, #0
	ble	.Lsgemm_tcopy_L4_M16_40

	.align	5
.Lsgemm_tcopy_L4_M16_20:

	COPY16x4

	subs	I , I , #1
	bne	.Lsgemm_tcopy_L4_M16_20

.Lsgemm_tcopy_L4_M16_40:
	tst	N , #8
	ble	.Lsgemm_tcopy_L4_M16_60

	COPY8x4
	
.Lsgemm_tcopy_L4_M16_60:
	tst	N , #4
	ble	.Lsgemm_tcopy_L4_M16_80

	COPY4x4

.Lsgemm_tcopy_L4_M16_80:

	tst	N , #2
	ble	.Lsgemm_tcopy_L4_M16_100

	COPY2x4


.Lsgemm_tcopy_L4_M16_100:

	tst	N, #1
	ble	.Lsgemm_tcopy_L4_M16_END

	COPY1x4


.Lsgemm_tcopy_L4_M16_END:

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
	add	B, B00, #128					// B = B + 2 * 16 * SIZE

	asr	I, N, #4					// I = N / 16
	cmp 	I, #0
	ble	.Lsgemm_tcopy_L2_M16_40

	.align	5
.Lsgemm_tcopy_L2_M16_20:

	COPY16x2

	subs	I , I , #1
	bne	.Lsgemm_tcopy_L2_M16_20

.Lsgemm_tcopy_L2_M16_40:
	tst	N , #8
	ble	.Lsgemm_tcopy_L2_M16_60

	COPY8x2

.Lsgemm_tcopy_L2_M16_60:
	tst	N , #4
	ble	.Lsgemm_tcopy_L2_M16_80

	COPY4x2

.Lsgemm_tcopy_L2_M16_80:

	tst	N , #2
	ble	.Lsgemm_tcopy_L2_M16_100

	COPY2x2

.Lsgemm_tcopy_L2_M16_100:

	tst	N , #1
	ble	.Lsgemm_tcopy_L2_M16_END

	COPY1x2

.Lsgemm_tcopy_L2_M16_END:

/*********************************************************************************************/

.Lsgemm_tcopy_L1_BEGIN:

	tst	M, #1
	ble	.Lsgemm_tcopy_L999


.Lsgemm_tcopy_L1_M16_BEGIN:

	mov	A01, A						// A01 = A
	mov	B00, B

	asr	I, N, #4					// I = M / 16
	cmp 	I, #0
	ble	.Lsgemm_tcopy_L1_M16_40

	.align	5
.Lsgemm_tcopy_L1_M16_20:

	COPY16x1

	subs	I , I , #1
	bne	.Lsgemm_tcopy_L1_M16_20
	
.Lsgemm_tcopy_L1_M16_40:
	tst	N , #8
	ble	.Lsgemm_tcopy_L1_M16_60

	COPY8x1

.Lsgemm_tcopy_L1_M16_60:
	tst	N , #4
	ble	.Lsgemm_tcopy_L1_M16_80

	COPY4x1

.Lsgemm_tcopy_L1_M16_80:

	tst	N , #2
	ble	.Lsgemm_tcopy_L1_M16_100

	COPY2x1

.Lsgemm_tcopy_L1_M16_100:

	tst	N , #1
	ble	.Lsgemm_tcopy_L1_M16_END

	COPY1x1


.Lsgemm_tcopy_L1_M16_END:

.Lsgemm_tcopy_L999:
	mov	x0, #0						// set return value
	RESTORE_REGS
	ret

	EPILOGUE


