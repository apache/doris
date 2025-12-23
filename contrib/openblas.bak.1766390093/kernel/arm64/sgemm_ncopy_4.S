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

#define	M	x0
#define	N	x1
#define	A00	x2
#define	LDA	x3
#define	B00	x4

#define	A01	x5
#define	A02	x6
#define	A03	x7
#define	A04	x8

#define I	x9
#define	J	x10

#define	TEMP1	x11
#define	TEMP2	x12

#define A_PREFETCH	2560

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

.macro COPY4x4
	prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	prfm	PLDL1KEEP, [A02, #A_PREFETCH]
	prfm	PLDL1KEEP, [A03, #A_PREFETCH]
	prfm	PLDL1KEEP, [A04, #A_PREFETCH]

	ldr	q0, [A01], #16
	ldr	q1, [A02], #16
	ldr	q2, [A03], #16
	ldr	q3, [A04], #16

	zip1    v12.4s, v0.4s, v1.4s
	zip1    v13.4s, v2.4s, v3.4s
	zip2    v14.4s, v0.4s, v1.4s
	zip2    v15.4s, v2.4s, v3.4s

	zip1    v8.2d, v12.2d, v13.2d
	zip2    v9.2d, v12.2d, v13.2d
	zip1    v10.2d, v14.2d, v15.2d
	zip2    v11.2d, v14.2d, v15.2d

	st1	{v8.4s, v9.4s, v10.4s, v11.4s}, [B00]
	add	B00, B00, #64

.endm

.macro COPY1x4
	prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	prfm	PLDL1KEEP, [A02, #A_PREFETCH]
	prfm	PLDL1KEEP, [A03, #A_PREFETCH]
	prfm	PLDL1KEEP, [A04, #A_PREFETCH]

	ldr	s0, [A01], #4
	ldr	s1, [A02], #4
	ldr	s2, [A03], #4
	ldr	s3, [A04], #4

	stp	s0, s1, [B00]
	add	B00, B00, #8
   	stp	s2, s3, [B00]
	add	B00, B00, #8
.endm

.macro COPY4x2
	prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	prfm	PLDL1KEEP, [A02, #A_PREFETCH]

	ldr	q0, [A01], #16
	ldr	q1, [A02], #16

	zip1    v12.4s, v0.4s, v1.4s
	zip2    v13.4s, v0.4s, v1.4s

	dup     v8.2d, v12.d[0]
	dup     v9.2d, v12.d[1]
	dup     v10.2d, v13.d[0]
	dup     v11.2d , v13.d[1]

	st1	{v8.2s, v9.2s, v10.2s, v11.2s}, [B00]
	add	B00, B00, #32
.endm


.macro COPY1x2
	prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	prfm	PLDL1KEEP, [A02, #A_PREFETCH]

	ldr	s0, [A01], #4
	ldr	s1, [A02], #4

	stp	s0, s1, [B00]
	add	B00, B00, #8
.endm

.macro COPY4x1
	prfm	PLDL1KEEP, [A01, #A_PREFETCH]

	ldr	q0, [A01], #16
	str	q0, [B00], #16
.endm


.macro COPY1x1
	prfm	PLDL1KEEP, [A01, #A_PREFETCH]

	ldr	s0, [A01], #4
	str	s0, [B00], #4
.endm

/**************************************************************************************
* End of macro definitions
**************************************************************************************/

	PROLOGUE

	.align 5

	SAVE_REGS

	lsl	LDA, LDA, #2					// LDA = LDA * SIZE

.Ldgemm_ncopy_L4_BEGIN:

	asr	J, N, #2					// J = N / 4
	cmp 	J, #0
	ble	.Ldgemm_ncopy_L2_BEGIN

	.align	5
.Ldgemm_ncopy_L4_M4_BEGIN:

	mov	A01, A00
	add	A02, A01, LDA
	add	A03, A02, LDA
	add	A04, A03, LDA
	add	A00, A04, LDA

	asr	I, M, #2					// I = M / 4
	cmp	I, #0
	ble	.Ldgemm_ncopy_L4_M4_40

	.align	5
.Ldgemm_ncopy_L4_M4_20:

	COPY4x4

	subs	I , I , #1
	bne	.Ldgemm_ncopy_L4_M4_20

.Ldgemm_ncopy_L4_M4_40:

	and	I, M , #3
	cmp	I, #0
	ble	.Ldgemm_ncopy_L4_M4_END

	.align	5
.Ldgemm_ncopy_L4_M4_60:

	COPY1x4

	subs	I , I , #1
	bne	.Ldgemm_ncopy_L4_M4_60

.Ldgemm_ncopy_L4_M4_END:

	subs	J , J, #1						// j--
	bne	.Ldgemm_ncopy_L4_M4_BEGIN

/*********************************************************************************************/

.Ldgemm_ncopy_L2_BEGIN:

	tst	N, #3
	ble	.Ldgemm_ncopy_L999

	tst	N, #2
	ble	.Ldgemm_ncopy_L1_BEGIN

.Ldgemm_ncopy_L2_M4_BEGIN:
	mov	A01, A00
	add	A02, A01, LDA
	add	A00, A02, LDA

	asr	I, M, #2					// I = M / 4
	cmp 	I, #0
	ble	.Ldgemm_ncopy_L2_M4_40

	.align	5
.Ldgemm_ncopy_L2_M4_20:

	COPY4x2

	subs	I , I , #1
	bne	.Ldgemm_ncopy_L2_M4_20

.Ldgemm_ncopy_L2_M4_40:

	and	I, M , #3
	cmp	I, #0
	ble	.Ldgemm_ncopy_L2_M4_END

	.align	5
.Ldgemm_ncopy_L2_M4_60:

	COPY1x2

	subs	I , I , #1
	bne	.Ldgemm_ncopy_L2_M4_60

.Ldgemm_ncopy_L2_M4_END:


/*********************************************************************************************/

.Ldgemm_ncopy_L1_BEGIN:

	tst	N, #1
	ble	.Ldgemm_ncopy_L999

.Ldgemm_ncopy_L1_M4_BEGIN:

	mov	A01, A00

	asr	I, M, #2					// I = M / 4
	cmp	I, #0
	ble	.Ldgemm_ncopy_L1_M4_40

	.align	5
.Ldgemm_ncopy_L1_M4_20:

	COPY4x1

	subs	I , I , #1
	bne	.Ldgemm_ncopy_L1_M4_20


.Ldgemm_ncopy_L1_M4_40:

	and	I, M , #3
	cmp	I, #0
	ble	.Ldgemm_ncopy_L1_M4_END

	.align	5
.Ldgemm_ncopy_L1_M4_60:

	COPY1x1

	subs	I , I , #1
	bne	.Ldgemm_ncopy_L1_M4_60


.Ldgemm_ncopy_L1_M4_END:

.Ldgemm_ncopy_L999:

	mov	x0, #0
	RESTORE_REGS
	ret

	EPILOGUE
