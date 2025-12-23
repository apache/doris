/***************************************************************************
Copyright (c) 2013-2019, The OpenBLAS Project
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
*****************************************************************************/
#define ASSEMBLER
#include "common.h"
#include "def_vsx.h"

#define LOAD	ld
 
#define STACKSIZE 512

#define FZERO	312+192(SP)

#define FLINK_SAVE (STACKSIZE+16) /* 16($r12) */

#define	M	r3
#define	N	r4
#define	K	r5

 
#define A	r8
#define	B	r9
#define	C	r10
#define	LDC	r6
#define OFFSET	r7
 
 

#define o0	0
#define alpha_r vs30
#define alpha_i vs31

#define VECSAVE r11

#define FRAMEPOINTER r12

#define T10 r14

#define L	r15
#define T8	r16
#define T5	r17
#define T2	r19
#define TEMP_REG	r20
#define	T6	r21
#define	I	r22
#define J	r23
#define AO	r24
#define	BO	r25
#define	CO	r26
#define T7	r27
#define	T3	r28
#define T4	r29

#define PRE	r30
#define T1  	r31

#ifndef NEEDPARAM

	PROLOGUE
	PROFCODE

	mr      FRAMEPOINTER, SP
    addi    SP, SP, -STACKSIZE 
    mflr    r0
	stfd	f14,    0(SP)
	stfd	f15,    8(SP)
	stfd	f16,   16(SP)
	stfd	f17,   24(SP)

	stfd	f18,   32(SP)
	stfd	f19,   40(SP)
	stfd	f20,   48(SP)
	stfd	f21,   56(SP)

	stfd	f22,   64(SP)
	stfd	f23,   72(SP)
	stfd	f24,   80(SP)
	stfd	f25,   88(SP)

	stfd	f26,   96(SP)
	stfd	f27,  104(SP)
	stfd	f28,  112(SP)
	stfd	f29,  120(SP)

	stfd	f30,  128(SP)
	stfd	f31,  136(SP)

    xxspltd  alpha_r,vs1,0  /*copy from register f1 */
    xxspltd  alpha_i,vs2,0  /*copy from register f2 */
 
	std	r31,  144(SP)
	std	r30,  152(SP)
	std	r29,  160(SP)
	std	r28,  168(SP)
	std	r27,  176(SP)
	std	r26,  184(SP)
	std	r25,  192(SP)
	std	r24,  200(SP)
	std	r23,  208(SP)
	std	r22,  216(SP)
	std	r21,  224(SP)
	std	r20,  232(SP)
	std	r19,  240(SP)
	std	r18,  248(SP)
	std	r17,  256(SP)
	std	r16,  264(SP)
	std	r15,  272(SP)
	std	r14,  280(SP)
 
 
    stxv    vs52,  288(SP)
    stxv    vs53,  304(SP)
    stxv    vs54,  320(SP)
    stxv    vs55,  336(SP)
    stxv    vs56,  352(SP)
    stxv    vs57,  368(SP)
    stxv    vs58,  384(SP)
    stxv    vs59,  400(SP)
    stxv    vs60,  416(SP)
    stxv    vs61,  432(SP)
    stxv    vs62,  448(SP)
    stxv    vs63,  464(SP)

    std    r0, FLINK_SAVE(SP)
 

#if defined(linux) || defined(__FreeBSD__)
	ld	LDC, FRAMESLOT(0) + 0(FRAMEPOINTER)
#endif


#ifdef TRMMKERNEL
#if (defined(linux) || defined(__FreeBSD__)) && defined(__64BIT__)
	ld	OFFSET,  FRAMESLOT(1) + 0(FRAMEPOINTER)
#endif 
#endif


#include "zgemm_macros_power9.S"

 

	slwi	LDC, LDC, ZBASE_SHIFT
	li	PRE,  512 
    li  r0,   0
 

#if defined(CC) || defined(CR) || defined(RC) || defined(RR) 
/*negate for this case as we will use addition -1*(a+b) */
  xvnegdp alpha_r,alpha_r
  xvnegdp alpha_i,alpha_i
#endif
	.align 4

#include "zgemm_logic_power9.S"

L999:
 
	lfd	f14,    0(SP)
	lfd	f15,    8(SP)
	lfd	f16,   16(SP)
	lfd	f17,   24(SP)

	lfd	f18,   32(SP)
	lfd	f19,   40(SP)
	lfd	f20,   48(SP)
	lfd	f21,   56(SP)

	lfd	f22,   64(SP)
	lfd	f23,   72(SP)
	lfd	f24,   80(SP)
	lfd	f25,   88(SP)

	lfd	f26,   96(SP)
	lfd	f27,  104(SP)
	lfd	f28,  112(SP)
	lfd	f29,  120(SP)

	lfd	f30,  128(SP)
	lfd	f31,  136(SP)

 
	ld	r31,  144(SP)
	ld	r30,  152(SP)
	ld	r29,  160(SP)
	ld	r28,  168(SP)
	ld	r27,  176(SP)
	ld	r26,  184(SP)
	ld	r25,  192(SP)
	ld	r24,  200(SP)
	ld	r23,  208(SP)
	ld	r22,  216(SP)
	ld	r21,  224(SP)
	ld	r20,  232(SP)
	ld	r19,  240(SP)
	ld	r18,  248(SP)
	ld	r17,  256(SP)
	ld	r16,  264(SP)
	ld	r15,  272(SP)
	ld	r14,  280(SP)

	ld    r0, 	 FLINK_SAVE(SP)	
 
    lxv    vs52,  288(SP)
    lxv    vs53,  304(SP)
    lxv    vs54,  320(SP)
    lxv    vs55,  336(SP)
    lxv    vs56,  352(SP)
    lxv    vs57,  368(SP)
    lxv    vs58,  384(SP) 
    lxv    vs59,  400(SP)
	mtlr r0
    lxv    vs60,  416(SP)
    lxv    vs61,  432(SP) 
    lxv    vs62,  448(SP)
    lxv    vs63,  464(SP)

	addi	SP, SP, STACKSIZE 
	blr

	EPILOGUE
#endif