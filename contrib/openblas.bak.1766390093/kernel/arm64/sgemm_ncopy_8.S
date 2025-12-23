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
#define	A05	x9
#define	A06	x10
#define	A07	x11
#define	A08	x12

#define I	x13
#define	J	x14
#define	K	x15

#define	TEMP1	x16
#define	TEMP2	x17

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

.macro COPY4x8
	ldr	q0, [A01], #16
	ldr	q1, [A02], #16
	ldr	q2, [A03], #16
	ldr	q3, [A04], #16

	zip1    v16.4s, v0.4s, v1.4s
	zip1    v17.4s, v2.4s, v3.4s
	zip2    v18.4s, v0.4s, v1.4s
	zip2    v19.4s, v2.4s, v3.4s

	zip1    v8.2d, v16.2d, v17.2d
	zip2    v10.2d, v16.2d, v17.2d
	zip1    v12.2d, v18.2d, v19.2d
	zip2    v14.2d, v18.2d, v19.2d

	ldr	q4, [A05], #16
	ldr	q5, [A06], #16
	ldr	q6, [A07], #16
	ldr	q7, [A08], #16

	zip1    v16.4s, v4.4s, v5.4s
	zip1    v17.4s, v6.4s, v7.4s
	zip2    v18.4s, v4.4s, v5.4s
	zip2    v19.4s, v6.4s, v7.4s

	zip1    v9.2d, v16.2d, v17.2d
	zip2    v11.2d, v16.2d, v17.2d
	zip1    v13.2d, v18.2d, v19.2d
	zip2    v15.2d, v18.2d, v19.2d

	st1	{v8.4s, v9.4s, v10.4s, v11.4s}, [B00], #64
	st1	{v12.4s, v13.4s, v14.4s, v15.4s}, [B00], #64
.endm

.macro COPY2x8
	ldr	d0, [A01], #8
	ldr	d1, [A02], #8
	ldr	d2, [A03], #8
	ldr	d3, [A04], #8

	zip1    v12.4s, v0.4s, v1.4s
	zip1    v13.4s, v2.4s, v3.4s

	zip1    v8.2d, v12.2d, v13.2d
	zip2    v10.2d, v12.2d, v13.2d

	ldr	d4, [A05], #8
	ldr	d5, [A06], #8
	ldr	d6, [A07], #8
	ldr	d7, [A08], #8

	zip1    v12.4s, v4.4s, v5.4s
	zip1    v13.4s, v6.4s, v7.4s

	zip1    v9.2d, v12.2d, v13.2d
	zip2    v11.2d, v12.2d, v13.2d

	st1	{v8.4s, v9.4s, v10.4s, v11.4s}, [B00], #64
.endm

.macro COPY1x8
	ldr	s0, [A01], #4
	ldr	s1, [A02], #4
	ins	v8.s[0], v0.s[0]
	ins	v8.s[1], v1.s[0]

	ldr	s2, [A03], #4
	ldr	s3, [A04], #4
	ins	v8.s[2], v2.s[0]
	ins	v8.s[3], v3.s[0]

	ldr	s4, [A05], #4
	ldr	s5, [A06], #4
	ins	v9.s[0], v4.s[0]
	ins	v9.s[1], v5.s[0]

	ldr	s6, [A07], #4
	ldr	s7, [A08], #4
	ins	v9.s[2], v6.s[0]
	ins	v9.s[3], v7.s[0]

	st1	{v8.4s, v9.4s}, [B00], #32
.endm

.macro COPY4x4
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

	st1	{v8.4s, v9.4s, v10.4s, v11.4s}, [B00], #64
.endm

.macro COPY2x4
	ldr	d0, [A01], #8
	ldr	d1, [A02], #8
	ldr	d2, [A03], #8
	ldr	d3, [A04], #8

	zip1    v10.4s, v0.4s, v1.4s
	zip1    v11.4s, v2.4s, v3.4s

	zip1    v8.2d, v10.2d, v11.2d
	zip2    v9.2d, v10.2d, v11.2d

	st1	{v8.4s, v9.4s}, [B00], #32
.endm

.macro COPY1x4
	ldr	s0, [A01], #4
	ldr	s1, [A02], #4
	ins	v8.s[0], v0.s[0]
	ins	v8.s[1], v1.s[0]

	ldr	s2, [A03], #4
	ldr	s3, [A04], #4
	ins	v8.s[2], v2.s[0]
	ins	v8.s[3], v3.s[0]

	st1	{v8.4s}, [B00], #16
.endm

.macro COPY4x2
	ldr	q0, [A01], #16
	ldr	q1, [A02], #16

	zip1    v12.4s, v0.4s, v1.4s
	zip2    v13.4s, v0.4s, v1.4s

	dup     v8.2d, v12.d[0]
	dup     v9.2d, v12.d[1]
	dup     v10.2d, v13.d[0]
	dup     v11.2d , v13.d[1]

	st1	{v8.2s, v9.2s, v10.2s, v11.2s}, [B00], #32
.endm

.macro COPY2x2
	ldr	d0, [A01], #8
	ldr	d1, [A02], #8

	zip1    v8.2s, v0.2s, v1.2s
	zip2    v9.2s, v0.2s, v1.2s

	st1	{v8.2s, v9.2s}, [B00], #16
.endm

.macro COPY1x2
	ldr	s0, [A01], #4
	ldr	s1, [A02], #4
	ins	v8.s[0], v0.s[0]
	ins	v8.s[1], v1.s[0]

	st1	{v8.2s}, [B00], #8
.endm

.macro COPY1x1
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

.Lsgemm_ncopy_L8_BEGIN:

	asr	J, N, #3					// J = N / 8
	cmp 	J, #0
	ble	.Lsgemm_ncopy_L4_BEGIN

	.align	5
.Lsgemm_ncopy_L8_M4_BEGIN:

	mov	A01, A00
	add	A02, A01, LDA
	add	A03, A02, LDA
	add	A04, A03, LDA
	add	A05, A04, LDA
	add	A06, A05, LDA
	add	A07, A06, LDA
	add	A08, A07, LDA
	add	A00, A08, LDA

	asr	I, M, #2					// I = M / 4
	cmp	I, #0
	ble	.Lsgemm_ncopy_L8_M4_40

	asr	K, M, #4					// K = M / 16(cacheline)
	mov	TEMP1, A01

	.align	5
.Lsgemm_tcopy_L8_warnup_1:

	ldr	s0, [TEMP1], #64

	subs	K, K, #1
	bgt	.Lsgemm_tcopy_L8_warnup_1

	asr	K, M, #4					// K = M / 16(cacheline)
	mov	TEMP1, A02

	.align	5
.Lsgemm_tcopy_L8_warnup_2:

	ldr	s0, [TEMP1], #64

	subs	K, K, #1
	bgt	.Lsgemm_tcopy_L8_warnup_2

	asr	K, M, #4					// K = M / 16(cacheline)
	mov	TEMP1, A03

	.align	5
.Lsgemm_tcopy_L8_warnup_3:

	ldr	s0, [TEMP1], #64

	subs	K, K, #1
	bgt	.Lsgemm_tcopy_L8_warnup_3

	asr	K, M, #4					// K = M / 16(cacheline)
	mov	TEMP1, A04

	.align	5
.Lsgemm_tcopy_L8_warnup_4:

	ldr	s0, [TEMP1], #64

	subs	K, K, #1
	bgt	.Lsgemm_tcopy_L8_warnup_4

	asr	K, M, #4					// K = M / 16(cacheline)
	mov	TEMP1, A05

	.align	5
.Lsgemm_tcopy_L8_warnup_5:

	ldr	s0, [TEMP1], #64

	subs	K, K, #1
	bgt	.Lsgemm_tcopy_L8_warnup_5

	asr	K, M, #4					// K = M / 16(cacheline)
	mov	TEMP1, A06

	.align	5
.Lsgemm_tcopy_L8_warnup_6:

	ldr	s0, [TEMP1], #64

	subs	K, K, #1
	bgt	.Lsgemm_tcopy_L8_warnup_6

	asr	K, M, #4					// K = M / 16(cacheline)
	mov	TEMP1, A07

	.align	5
.Lsgemm_tcopy_L8_warnup_7:

	ldr	s0, [TEMP1], #64

	subs	K, K, #1
	bgt	.Lsgemm_tcopy_L8_warnup_7

	asr	K, M, #4					// K = M / 16(cacheline)
	mov	TEMP1, A08

	.align	5
.Lsgemm_tcopy_L8_warnup_8:

	ldr	s0, [TEMP1], #64

	subs	K, K, #1
	bgt	.Lsgemm_tcopy_L8_warnup_8

	.align	5
.Lsgemm_ncopy_L8_M4_20:

	COPY4x8

	subs	I, I, #1
	bne	.Lsgemm_ncopy_L8_M4_20

.Lsgemm_ncopy_L8_M4_40:

	and	I, M, #2
	cmp	I, #0
	ble	.Lsgemm_ncopy_L8_M4_60

	COPY2x8

.Lsgemm_ncopy_L8_M4_60:

	and	I, M, #1
	cmp	I, #0
	ble	.Lsgemm_ncopy_L8_M4_END

	COPY1x8

.Lsgemm_ncopy_L8_M4_END:

	subs	J , J, #1						// j--
	bne	.Lsgemm_ncopy_L8_M4_BEGIN

/*********************************************************************************************/

.Lsgemm_ncopy_L4_BEGIN:

	tst	N, #7
	ble	.Lsgemm_ncopy_L999

	tst	N, #4
	ble	.Lsgemm_ncopy_L2_BEGIN

.Lsgemm_ncopy_L4_M4_BEGIN:
	mov	A01, A00
	add	A02, A01, LDA
	add	A03, A02, LDA
	add	A04, A03, LDA
	add	A00, A04, LDA

	asr	I, M, #2					// I = M / 4
	cmp 	I, #0
	ble	.Lsgemm_ncopy_L4_M4_40

	.align	5
.Lsgemm_ncopy_L4_M4_20:

	COPY4x4

	subs	I, I, #1
	bne	.Lsgemm_ncopy_L4_M4_20

.Lsgemm_ncopy_L4_M4_40:

	and	I, M, #2
	cmp	I, #0
	ble	.Lsgemm_ncopy_L4_M4_60

	COPY2x4

.Lsgemm_ncopy_L4_M4_60:

	and	I, M, #1
	cmp	I, #0
	ble	.Lsgemm_ncopy_L4_M4_END

	COPY1x4

.Lsgemm_ncopy_L4_M4_END:


/*********************************************************************************************/

.Lsgemm_ncopy_L2_BEGIN:

	tst	N, #2
	ble	.Lsgemm_ncopy_L1_BEGIN

.Lsgemm_ncopy_L2_M4_BEGIN:

	mov	A01, A00
	add	A02, A01, LDA
	add	A00, A02, LDA

	asr	I, M, #2					// I = M / 4
	cmp	I, #0
	ble	.Lsgemm_ncopy_L2_M4_40

	.align	5
.Lsgemm_ncopy_L2_M4_20:

	COPY4x2

	subs	I , I , #1
	bne	.Lsgemm_ncopy_L2_M4_20


.Lsgemm_ncopy_L2_M4_40:

	and	I, M, #2
	cmp	I, #0
	ble	.Lsgemm_ncopy_L2_M4_60

	COPY2x2

.Lsgemm_ncopy_L2_M4_60:

	and	I, M, #1
	cmp	I, #0
	ble	.Lsgemm_ncopy_L2_M4_END

	COPY1x2

.Lsgemm_ncopy_L2_M4_END:

.Lsgemm_ncopy_L1_BEGIN:

	tst	N, #1
	ble	.Lsgemm_ncopy_L999

.Lsgemm_ncopy_L1_M1_BEGIN:

	mov	A01, A00

	mov	I, M
	cmp	I, #0
	ble	.Lsgemm_ncopy_L1_M1_END

	.align	5
.Lsgemm_ncopy_L1_M1_20:

	COPY1x1

	subs	I, I, #1
	bne	.Lsgemm_ncopy_L1_M1_20

.Lsgemm_ncopy_L1_M1_END:

.Lsgemm_ncopy_L999:

	mov	x0, #0
	RESTORE_REGS
	ret

	EPILOGUE
