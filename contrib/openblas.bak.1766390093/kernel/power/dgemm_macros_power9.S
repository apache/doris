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

/**************************************************************************************
* Abdelrauf(quickwritereader@googlemail.com)
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
*	 LAPACK-TEST		: OK
**************************************************************************************/

/*********************************************************************
* Macros for N=4, M=16                                               *
*********************************************************************/
.macro LOAD4x16_1
   LOAD4x16 1
.endm

.macro LOAD4x16_0
   LOAD4x16 0
.endm
.macro LOAD4x16  Zero

	lxv	vs24,	0(BO)
	lxv	vs26,	16(BO)
	xxpermdi	vs25,	vs24,	vs24,2	
	xxpermdi	vs27,	vs26,	vs26,2

	lxv	vs0,	 0(AO)
	lxv	vs1,	16(AO)
	lxv	vs2,	32(AO)
	lxv	vs3,	48(AO)
 

	lxv	vs4,	64(AO)
	lxv	vs5,	80(AO)
	lxv	vs6,	96(AO)
	lxv	vs7,	112(AO)
.if \Zero==1 
    xxlxor		vs32,vs32,vs32
    xxlxor		vs33,vs33,vs33
	xxlxor		vs34,vs34,vs34
	xxlxor		vs35,vs35,vs35
	xxlxor		vs36,vs36,vs36
	xxlxor		vs37,vs37,vs37
	xxlxor		vs38,vs38,vs38
	xxlxor		vs39,vs39,vs39
	xxlxor		vs40,	vs40,	vs40
	xxlxor		vs41,	vs41,	vs41
	xxlxor		vs42,	vs42,	vs42
	xxlxor		vs43,	vs43,	vs43
	xxlxor		vs44,	vs44,	vs44
	xxlxor		vs45,	vs45,	vs45
	xxlxor		vs46,	vs46,	vs46
	xxlxor		vs47,	vs47,	vs47
	xxlxor		vs48,	vs48,	vs48
	xxlxor		vs49,	vs49,	vs49
	xxlxor		vs50,	vs50,	vs50
	xxlxor		vs51,	vs51,	vs51 
	xxlxor		vs52,	vs52,	vs52
	xxlxor		vs53,	vs53,	vs53
	xxlxor		vs54,	vs54,	vs54
	xxlxor		vs55,	vs55,	vs55 
	xxlxor		vs56,	vs56,	vs56
	xxlxor		vs57,	vs57,	vs57
	xxlxor		vs58,	vs58,	vs58
	xxlxor		vs59,	vs59,	vs59 
	xxlxor		vs60,	vs60,	vs60
	xxlxor		vs61,	vs61,	vs61
	xxlxor		vs62,	vs62,	vs62
	xxlxor		vs63,	vs63,	vs63	
.endif
.endm

  
#define unit_size 8
#define DISP32(ind,disp) (ind*unit_size*32+disp)
#define DISP16(ind,disp) (ind*unit_size*16+disp)
#define DISP8(ind,disp) (ind*unit_size*8+disp)
#define DISP4(ind,disp) (ind*unit_size*4+disp)
#define DISP2(ind,disp) (ind*unit_size*2+disp)
#define DISP1(ind,disp) (ind*unit_size+disp)

.macro KERNEL4x16_L1_L2  Index,IsLast
  KERNEL4x16_L1_L2_I AO,BO, 0,0,0, \Index,\IsLast,0
.endm



.macro KERNEL4x16_I1_L2  OffsetA,OffsetB, Index,IsLast
  KERNEL4x16_L1_L2_I  AO,BO,1,\OffsetA,\OffsetB,\Index,\IsLast,0
.endm

.macro KERNEL4x16_I1_L2_2  OffsetA,OffsetB, Index,IsLast
  KERNEL4x16_L1_L2_I  AO,BO, 0,\OffsetA,\OffsetB,\Index,\IsLast,0
.endm

.macro KERNEL4x16_I1_L2_3  OffsetA,OffsetB, Index,IsLast
  KERNEL4x16_L1_L2_I  AO,BO, 0,\OffsetA,\OffsetB,\Index,\IsLast,1
.endm

.macro KERNEL4x16_I2_L2  AREG,BREG,OffsetA,OffsetB, Index,IsLast
  KERNEL4x16_L1_L2_I  \AREG,\BREG,1,\OffsetA,\OffsetB,\Index,\IsLast,0
.endm

.macro KERNEL4x16_I2_L2_2  AREG,BREG,OffsetA,OffsetB, Index,IsLast
  KERNEL4x16_L1_L2_I  \AREG,\BREG, 0,\OffsetA,\OffsetB,\Index,\IsLast,0
.endm

.macro KERNEL4x16_I2_L2_3  AREG,BREG,OffsetA,OffsetB, Index,IsLast
  KERNEL4x16_L1_L2_I \AREG,\BREG, 0,\OffsetA,\OffsetB,\Index,\IsLast,1
.endm

.macro KERNEL4x16_L1_L2_I  AREG,BREG, First, OffsetA,OffsetB, Index,IsLast ,Complete

.if \First ==1
	xvmuldp		vs32,	vs0,	vs24
	xvmuldp		vs33,	vs1,	vs24
	xvmuldp		vs34,	vs2,	vs24
	xvmuldp		vs35,	vs3,	vs24
.else
	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24
	xvmaddadp		vs34,	vs2,	vs24
	xvmaddadp		vs35,	vs3,	vs24
.endif
	lxv	vs8,	DISP32(\Index,0+\OffsetA)(\AREG)
	lxv	vs9,	DISP32(\Index,16+\OffsetA)(\AREG)
	lxv	vs10,	DISP32(\Index,32+\OffsetA)(\AREG)
	lxv	vs11,	DISP32(\Index,48+\OffsetA)(\AREG)
.if \First ==1
	xvmuldp		vs36,	vs4,	vs24
	xvmuldp		vs37,	vs5,	vs24
	xvmuldp		vs38,	vs6,	vs24
	xvmuldp		vs39,	vs7,	vs24
.else
	xvmaddadp		vs36,	vs4,	vs24
	xvmaddadp		vs37,	vs5,	vs24
	xvmaddadp		vs38,	vs6,	vs24
	xvmaddadp		vs39,	vs7,	vs24
.endif
	lxv	vs28,	DISP8(\Index,0  +\OffsetB)(\BREG)
	lxv	vs30,	DISP8(\Index,16  +\OffsetB)(\BREG)
	xxpermdi	vs29,	vs28,	vs28,2	
	xxpermdi	vs31,	vs30,	vs30,2
.if \First ==1
	xvmuldp		vs40,	vs0,	vs25
	xvmuldp		vs41,	vs1,	vs25
	xvmuldp		vs42,	vs2,	vs25
	xvmuldp		vs43,	vs3,	vs25


	xvmuldp		vs44,	vs4,	vs25
	xvmuldp		vs45,	vs5,	vs25
	xvmuldp		vs46,	vs6,	vs25
	xvmuldp		vs47,	vs7,	vs25


	xvmuldp		vs48,	vs0,	vs26
	xvmuldp		vs49,	vs1,	vs26
	xvmuldp		vs50,	vs2,	vs26
	xvmuldp		vs51,	vs3,	vs26


.else
	xvmaddadp		vs40,	vs0,	vs25
	xvmaddadp		vs41,	vs1,	vs25
	xvmaddadp		vs42,	vs2,	vs25
	xvmaddadp		vs43,	vs3,	vs25


	xvmaddadp		vs44,	vs4,	vs25
	xvmaddadp		vs45,	vs5,	vs25
	xvmaddadp		vs46,	vs6,	vs25
	xvmaddadp		vs47,	vs7,	vs25


	xvmaddadp		vs48,	vs0,	vs26
	xvmaddadp		vs49,	vs1,	vs26
	xvmaddadp		vs50,	vs2,	vs26
	xvmaddadp		vs51,	vs3,	vs26

.endif
	lxv	vs12,  DISP32(\Index,64+\OffsetA)(\AREG)
	lxv	vs13,  DISP32(\Index,80+\OffsetA)(\AREG)
.if \First ==1
	xvmuldp		vs52,	vs4,	vs26
	xvmuldp		vs53,	vs5,	vs26
	xvmuldp		vs54,	vs6,	vs26
	xvmuldp		vs55,	vs7,	vs26

.else
	xvmaddadp		vs52,	vs4,	vs26
	xvmaddadp		vs53,	vs5,	vs26
	xvmaddadp		vs54,	vs6,	vs26
	xvmaddadp		vs55,	vs7,	vs26
.endif
	lxv	vs14,  DISP32(\Index,96+\OffsetA)(\AREG)
	lxv	vs15,  DISP32(\Index,112+\OffsetA)(\AREG)
.if \First ==1
	xvmuldp		vs56,	vs0,	vs27
	xvmuldp		vs57,	vs1,	vs27
	xvmuldp		vs58,	vs2,	vs27
	xvmuldp		vs59,	vs3,	vs27

 

	xvmuldp		vs60,	vs4,	vs27
	xvmuldp		vs61,	vs5,	vs27
	xvmuldp		vs62,	vs6,	vs27
	xvmuldp		vs63,	vs7,	vs27

.else
	xvmaddadp		vs56,	vs0,	vs27
	xvmaddadp		vs57,	vs1,	vs27
	xvmaddadp		vs58,	vs2,	vs27
	xvmaddadp		vs59,	vs3,	vs27

 

	xvmaddadp		vs60,	vs4,	vs27
	xvmaddadp		vs61,	vs5,	vs27
	xvmaddadp		vs62,	vs6,	vs27
	xvmaddadp		vs63,	vs7,	vs27
.endif

	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28
	xvmaddadp		vs34,	vs10,	vs28
	xvmaddadp		vs35,	vs11,	vs28
.if \Complete==0
	lxv	vs0,	DISP32(\Index,128+\OffsetA)(\AREG)
	lxv	vs1,	DISP32(\Index,144+\OffsetA)(\AREG)
.endif
	xvmaddadp		vs36,	vs12,	vs28
	xvmaddadp		vs37,	vs13,	vs28
	xvmaddadp		vs38,	vs14,	vs28
	xvmaddadp		vs39,	vs15,	vs28
.if \Complete==0
	lxv	vs24,	DISP8(\Index,32  +\OffsetB)(\BREG)
	lxv	vs26,	DISP8(\Index,48  +\OffsetB)(\BREG)
	xxpermdi	vs25,	vs24,	vs24,2	
	xxpermdi	vs27,	vs26,	vs26,2
.endif
	xvmaddadp		vs40,	vs8,	vs29
	xvmaddadp		vs41,	vs9,	vs29
	xvmaddadp		vs42,	vs10,	vs29
	xvmaddadp		vs43,	vs11,	vs29
.if \Complete==0
	lxv	vs2,	DISP32(\Index,160+\OffsetA)(\AREG)
	lxv	vs3,	DISP32(\Index,176+\OffsetA)(\AREG)
.endif
	xvmaddadp		vs44,	vs12,	vs29
	xvmaddadp		vs45,	vs13,	vs29
	xvmaddadp		vs46,	vs14,	vs29
	xvmaddadp		vs47,	vs15,	vs29


	xvmaddadp		vs48,	vs8,	vs30
	xvmaddadp		vs49,	vs9,	vs30
	xvmaddadp		vs50,	vs10,	vs30
	xvmaddadp		vs51,	vs11,	vs30
.if \Complete==0
	lxv	vs4,	DISP32(\Index,192+\OffsetA)(\AREG)
	lxv	vs5,	DISP32(\Index,208+\OffsetA)(\AREG)
.endif
	xvmaddadp		vs52,	vs12,	vs30
	xvmaddadp		vs53,	vs13,	vs30
	xvmaddadp		vs54,	vs14,	vs30
	xvmaddadp		vs55,	vs15,	vs30
.if \Complete==0
	lxv	vs6,	DISP32(\Index,224+\OffsetA)(\AREG)
	lxv	vs7,	DISP32(\Index,240+\OffsetA)(\AREG)
.endif
	xvmaddadp		vs56,	vs8,	vs31
	xvmaddadp		vs57,	vs9,	vs31
	xvmaddadp		vs58,	vs10,	vs31
	xvmaddadp		vs59,	vs11,	vs31
 

	xvmaddadp		vs60,	vs12,	vs31
	
	xvmaddadp		vs61,	vs13,	vs31
	xvmaddadp		vs62,	vs14,	vs31
	
	xvmaddadp		vs63,	vs15,	vs31
  .if \IsLast==1	
  .if \Complete==1
	addi		\AREG, \AREG, DISP32(\Index,128+\OffsetA)
	addi		\BREG, \BREG,  DISP8(\Index,32+\OffsetB)
  .else
	addi		\AREG, \AREG, DISP32(\Index,256)
	addi		\BREG, \BREG,  DISP8(\Index,64)
  .endif
  .endif
  

.endm

 

.macro KERNEL4x16 First

	lxv	vs24,	0(BO)
	lxv	vs26,	16(BO)
	xxpermdi	vs25,	vs24,	vs24,2	
	xxpermdi	vs27,	vs26,	vs26,2

	lxv	vs0,	0(AO)
	lxv	vs1,	16(AO)
	lxv	vs2,	32(AO)
	lxv	vs3,	48(AO) 

	lxv	vs4,	64(AO)
	lxv	vs5,	80(AO)
	lxv	vs6,	96(AO)
	lxv	vs7,	112(AO)


 
	addi		BO, BO, 32
  addi		AO, AO, 128

.if \First==1
	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24
	xvmuldp			vs34,	vs2,	vs24
	xvmuldp			vs35,	vs3,	vs24
	xvmuldp			vs36,	vs4,	vs24
	xvmuldp			vs37,	vs5,	vs24
	xvmuldp			vs38,	vs6,	vs24
	xvmuldp			vs39,	vs7,	vs24

	xvmuldp			vs40,	vs0,	vs25
	xvmuldp			vs41,	vs1,	vs25
	xvmuldp			vs42,	vs2,	vs25
	xvmuldp			vs43,	vs3,	vs25
	xvmuldp			vs44,	vs4,	vs25
	xvmuldp			vs45,	vs5,	vs25
	xvmuldp			vs46,	vs6,	vs25
	xvmuldp			vs47,	vs7,	vs25

	xvmuldp			vs48,	vs0,	vs26
	xvmuldp			vs49,	vs1,	vs26
	xvmuldp			vs50,	vs2,	vs26
	xvmuldp			vs51,	vs3,	vs26
	xvmuldp			vs52,	vs4,	vs26
	xvmuldp			vs53,	vs5,	vs26
	xvmuldp			vs54,	vs6,	vs26
	xvmuldp			vs55,	vs7,	vs26

	xvmuldp			vs56,	vs0,	vs27
	xvmuldp			vs57,	vs1,	vs27
	xvmuldp			vs58,	vs2,	vs27
	xvmuldp			vs59,	vs3,	vs27
	xvmuldp			vs60,	vs4,	vs27
	xvmuldp			vs61,	vs5,	vs27
	xvmuldp			vs62,	vs6,	vs27
	xvmuldp			vs63,	vs7,	vs27
.else
	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24
	xvmaddadp		vs34,	vs2,	vs24
	xvmaddadp		vs35,	vs3,	vs24
	xvmaddadp		vs36,	vs4,	vs24
	xvmaddadp		vs37,	vs5,	vs24
	xvmaddadp		vs38,	vs6,	vs24
	xvmaddadp		vs39,	vs7,	vs24

	xvmaddadp		vs40,	vs0,	vs25
	xvmaddadp		vs41,	vs1,	vs25
	xvmaddadp		vs42,	vs2,	vs25
	xvmaddadp		vs43,	vs3,	vs25
 
	xvmaddadp		vs44,	vs4,	vs25
	xvmaddadp		vs45,	vs5,	vs25
	xvmaddadp		vs46,	vs6,	vs25
	xvmaddadp		vs47,	vs7,	vs25

	xvmaddadp		vs48,	vs0,	vs26
	xvmaddadp		vs49,	vs1,	vs26
	xvmaddadp		vs50,	vs2,	vs26
	xvmaddadp		vs51,	vs3,	vs26
 
	xvmaddadp		vs52,	vs4,	vs26
	xvmaddadp		vs53,	vs5,	vs26
	xvmaddadp		vs54,	vs6,	vs26
	xvmaddadp		vs55,	vs7,	vs26

	xvmaddadp		vs56,	vs0,	vs27
	xvmaddadp		vs57,	vs1,	vs27
	xvmaddadp		vs58,	vs2,	vs27
	xvmaddadp		vs59,	vs3,	vs27
	xvmaddadp		vs60,	vs4,	vs27
	xvmaddadp		vs61,	vs5,	vs27
	xvmaddadp		vs62,	vs6,	vs27
	xvmaddadp		vs63,	vs7,	vs27

.endif
.endm

.macro SAVE4x16_REGS
	add		C2,	CO,	LDC
	add		C3,	C2,	LDC
	add		C4,	C3,	LDC
.endm

.macro SAVE4x16
#ifndef TRMMKERNEL
	lxv		vs0,	0(CO)
	lxv		vs2,	16(CO)
	lxv		vs4,	32(CO)
	lxv		vs6,	48(CO)
#endif	
	xxpermdi  vs8, vs40,vs32,1
 	xxpermdi  vs9 ,vs32,vs40,1
#ifndef TRMMKERNEL
	lxv		vs24,	64(CO)
	lxv		vs26,	80(CO)
	lxv		vs28,	96(CO)
	lxv		vs30,	112(CO)
#endif	
	xxpermdi  vs10, vs41,vs33,1		 
 	xxpermdi  vs11 ,vs33,vs41,1
#ifndef TRMMKERNEL	 
	lxv		vs1,	0(C2)
	lxv		vs3,	16(C2)
	lxv		vs5,	32(C2)
	lxv		vs7,	48(C2)
#endif	
	xxpermdi  vs12, vs42,vs34,1
 	xxpermdi  vs13 ,vs34,vs42,1
#ifndef TRMMKERNEL
	lxv		vs25,	64(C2)
	lxv		vs27,	80(C2)
#endif	
	xxpermdi  vs14, vs43,vs35,1		 
 	xxpermdi  vs15 ,vs35,vs43,1	
#ifndef TRMMKERNEL	 
	lxv		vs29,	96(C2)
	lxv		vs31,	112(C2)	
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs0,	vs8,	alpha_r 
	xvmaddadp	vs1,	vs9,	alpha_r 
	xvmaddadp	vs2,	vs10,	alpha_r 
	xvmaddadp	vs3,	vs11,	alpha_r 
#else
	xvmuldp	vs0,	vs8,	alpha_r 
	xvmuldp	vs1,	vs9,	alpha_r 
	xvmuldp	vs2,	vs10,	alpha_r 
	xvmuldp	vs3,	vs11,	alpha_r 

#endif
	xxpermdi  vs8, vs44,vs36,1
 	xxpermdi  vs9 ,vs36,vs44,1
	xxpermdi  vs10, vs45,vs37,1		 
 	xxpermdi  vs11 ,vs37,vs45,1
#ifndef TRMMKERNEL
	xvmaddadp	vs4,	vs12,	alpha_r 
	xvmaddadp	vs5,	vs13,	alpha_r 
	xvmaddadp	vs6,	vs14,	alpha_r 
	xvmaddadp	vs7,	vs15,	alpha_r 
#else
	xvmuldp	vs4,	vs12,	alpha_r 
	xvmuldp	vs5,	vs13,	alpha_r 
	xvmuldp	vs6,	vs14,	alpha_r 
	xvmuldp	vs7,	vs15,	alpha_r 
#endif
	xxpermdi  vs12, vs46,vs38,1
 	xxpermdi  vs13 ,vs38,vs46,1
	xxpermdi  vs14, vs47,vs39,1		 
 	xxpermdi  vs15 ,vs39,vs47,1

#ifndef TRMMKERNEL 
	xvmaddadp	vs24,	vs8,	alpha_r 
	xvmaddadp	vs25,	vs9,	alpha_r 
	xvmaddadp	vs26,	vs10,	alpha_r 
	xvmaddadp	vs27,	vs11,	alpha_r 

	xvmaddadp	vs28,	vs12,	alpha_r 
	xvmaddadp	vs29,	vs13,	alpha_r 
	xvmaddadp	vs30,	vs14,	alpha_r 
	xvmaddadp	vs31,	vs15,	alpha_r 
#else
	xvmuldp	vs24,	vs8,	alpha_r 
	xvmuldp	vs25,	vs9,	alpha_r 
	xvmuldp	vs26,	vs10,	alpha_r 
	xvmuldp	vs27,	vs11,	alpha_r 

	xvmuldp	vs28,	vs12,	alpha_r 
	xvmuldp	vs29,	vs13,	alpha_r 
	xvmuldp	vs30,	vs14,	alpha_r 
	xvmuldp	vs31,	vs15,	alpha_r 

#endif
	stxv		vs0,	0(CO)
	stxv		vs2,	16(CO)
	stxv		vs4,	32(CO)
	stxv		vs6,	48(CO)

	stxv		vs24,	64(CO)
	stxv		vs26,	80(CO)
	stxv		vs28,	96(CO)
	stxv		vs30,	112(CO)

	stxv		vs1,	0(C2)
	stxv		vs3,	16(C2)
	stxv		vs5,	32(C2)
	stxv		vs7,	48(C2)
	
	stxv		vs25,	64(C2)
	stxv		vs27,	80(C2)
	stxv		vs29,	96(C2)
	stxv		vs31,	112(C2)	
#ifndef TRMMKERNEL
 	lxv		vs0,	0(C3)
	lxv		vs2,	16(C3)
	lxv		vs4,	32(C3)
	lxv		vs6,	48(C3)
#endif	
	xxpermdi  vs8, vs56,vs48,1
 	xxpermdi  vs9 ,vs48,vs56,1
#ifndef TRMMKERNEL	 
	lxv		vs24,	64(C3)
	lxv		vs26,	80(C3)
#endif	
	xxpermdi  vs10, vs57,vs49,1		 
 	xxpermdi  vs11 ,vs49,vs57,1	
#ifndef TRMMKERNEL	 
	lxv		vs28,	96(C3)
	lxv		vs30,	112(C3)
#endif	
	xxpermdi  vs12, vs58,vs50,1
 	xxpermdi  vs13 ,vs50,vs58,1
#ifndef TRMMKERNEL	 
	lxv		vs1,	0(C4)
	lxv		vs3,	16(C4)
#endif	
	xxpermdi  vs14, vs59,vs51,1		 
 	xxpermdi  vs15 ,vs51,vs59,1	
#ifndef TRMMKERNEL	 
	lxv		vs5,	32(C4)
	lxv		vs7,	48(C4)

	lxv		vs25,	64(C4)
	lxv		vs27,	80(C4)
	lxv		vs29,	96(C4)
	lxv		vs31,	112(C4)	
#endif
 
#ifndef TRMMKERNEL 
	xvmaddadp	vs0,	vs8,	alpha_r 
	xvmaddadp	vs1,	vs9,	alpha_r 
	xvmaddadp	vs2,	vs10,	alpha_r 
	xvmaddadp	vs3,	vs11,	alpha_r 
#else
	xvmuldp	vs0,	vs8,	alpha_r 
	xvmuldp	vs1,	vs9,	alpha_r 
	xvmuldp	vs2,	vs10,	alpha_r 
	xvmuldp	vs3,	vs11,	alpha_r 

#endif

	xxpermdi  vs8, vs60,vs52,1
 	xxpermdi  vs9 ,vs52,vs60,1
	xxpermdi  vs10, vs61,vs53,1		 
 	xxpermdi  vs11 ,vs53,vs61,1
#ifndef TRMMKERNEL
	xvmaddadp	vs4,	vs12,	alpha_r 
	xvmaddadp	vs5,	vs13,	alpha_r 
	xvmaddadp	vs6,	vs14,	alpha_r 
	xvmaddadp	vs7,	vs15,	alpha_r 
#else
	xvmuldp	vs4,	vs12,	alpha_r 
	xvmuldp	vs5,	vs13,	alpha_r 
	xvmuldp	vs6,	vs14,	alpha_r 
	xvmuldp	vs7,	vs15,	alpha_r 
#endif


	xxpermdi  vs12, vs62,vs54,1
 	xxpermdi  vs13 ,vs54,vs62,1
	xxpermdi  vs14, vs63,vs55,1		 
 	xxpermdi  vs15 ,vs55,vs63,1
#ifndef TRMMKERNEL 
	xvmaddadp	vs24,	vs8,	alpha_r 
	xvmaddadp	vs25,	vs9,	alpha_r 
	xvmaddadp	vs26,	vs10,	alpha_r 
	xvmaddadp	vs27,	vs11,	alpha_r 

	xvmaddadp	vs28,	vs12,	alpha_r 
	xvmaddadp	vs29,	vs13,	alpha_r 
	xvmaddadp	vs30,	vs14,	alpha_r 
	xvmaddadp	vs31,	vs15,	alpha_r 
#else
	xvmuldp	vs24,	vs8,	alpha_r 
	xvmuldp	vs25,	vs9,	alpha_r 
	xvmuldp	vs26,	vs10,	alpha_r 
	xvmuldp	vs27,	vs11,	alpha_r 

	xvmuldp	vs28,	vs12,	alpha_r 
	xvmuldp	vs29,	vs13,	alpha_r 
	xvmuldp	vs30,	vs14,	alpha_r 
	xvmuldp	vs31,	vs15,	alpha_r 
#endif
 	stxv		vs0,	0(C3)
	stxv		vs2,	16(C3)
	stxv		vs4,	32(C3)
	stxv		vs6,	48(C3)

	stxv		vs24,	64(C3)
	stxv		vs26,	80(C3)
	stxv		vs28,	96(C3)
	stxv		vs30,	112(C3)

	stxv		vs1,	0(C4)
	stxv		vs3,	16(C4)
	stxv		vs5,	32(C4)
	stxv		vs7,	48(C4)
	
	stxv		vs25,	64(C4)
	stxv		vs27,	80(C4)
	stxv		vs29,	96(C4)
	stxv		vs31,	112(C4)	

	addi		CO,	CO,	128
.endm

/*********************************************************************
* Macros for N=4, M=8                                                *
*********************************************************************/

.macro LOAD4x8_1
   LOAD4x8 1
.endm

.macro LOAD4x8_0
   LOAD4x8 0
.endm
.macro LOAD4x8  Zero

	lxv	vs24,	0(BO)
	lxv	vs26,	16(BO)
	xxpermdi	vs25,	vs24,	vs24,2	
	xxpermdi	vs27,	vs26,	vs26,2

	lxv	vs0,	 0(AO)
	lxv	vs1,	16(AO)
	lxv	vs2,	32(AO)
	lxv	vs3,	48(AO)
 


.if \Zero==1 
    xxlxor		vs32,vs32,vs32
    xxlxor		vs33,vs33,vs33
	xxlxor		vs34,vs34,vs34
	xxlxor		vs35,vs35,vs35

	xxlxor		vs40,	vs40,	vs40
	xxlxor		vs41,	vs41,	vs41
	xxlxor		vs42,	vs42,	vs42
	xxlxor		vs43,	vs43,	vs43

	xxlxor		vs48,	vs48,	vs48
	xxlxor		vs49,	vs49,	vs49
	xxlxor		vs50,	vs50,	vs50
	xxlxor		vs51,	vs51,	vs51 

	xxlxor		vs56,	vs56,	vs56
	xxlxor		vs57,	vs57,	vs57
	xxlxor		vs58,	vs58,	vs58
	xxlxor		vs59,	vs59,	vs59 

.endif
.endm

  
 
.macro KERNEL4x8_L1_L2  Index,IsLast
  KERNEL4x8_L1_L2_I  0,0,0, \Index,\IsLast,0
.endm



.macro KERNEL4x8_I1_L2  OffsetA,OffsetB, Index,IsLast
  KERNEL4x8_L1_L2_I  1,\OffsetA,\OffsetB,\Index,\IsLast,0
.endm

.macro KERNEL4x8_I1_L2_2  OffsetA,OffsetB, Index,IsLast
  KERNEL4x8_L1_L2_I  0,\OffsetA,\OffsetB,\Index,\IsLast,0
.endm

.macro KERNEL4x8_I1_L2_3  OffsetA,OffsetB, Index,IsLast
  KERNEL4x8_L1_L2_I  0,\OffsetA,\OffsetB,\Index,\IsLast,1
.endm

.macro KERNEL4x8_L1_L2_I  First, OffsetA,OffsetB, Index,IsLast ,Complete

	lxv	vs8,	DISP16(\Index,0+\OffsetA)(AO)
	lxv	vs9,	DISP16(\Index,16+\OffsetA)(AO)
.if \First ==1
	xvmuldp		vs32,	vs0,	vs24
	xvmuldp		vs33,	vs1,	vs24
	xvmuldp		vs34,	vs2,	vs24
	xvmuldp		vs35,	vs3,	vs24
.else
	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24
	xvmaddadp		vs34,	vs2,	vs24
	xvmaddadp		vs35,	vs3,	vs24
.endif

	lxv	vs10,	DISP16(\Index,32+\OffsetA)(AO)
	lxv	vs11,	DISP16(\Index,48+\OffsetA)(AO)



.if \First ==1
	xvmuldp		vs40,	vs0,	vs25
	xvmuldp		vs41,	vs1,	vs25
	xvmuldp		vs42,	vs2,	vs25
	xvmuldp		vs43,	vs3,	vs25


	xvmuldp		vs48,	vs0,	vs26
	xvmuldp		vs49,	vs1,	vs26
	xvmuldp		vs50,	vs2,	vs26
	xvmuldp		vs51,	vs3,	vs26


.else

	lxv	vs28,	DISP8(\Index,0  +\OffsetB)(BO)
	lxv	vs30,	DISP8(\Index,16  +\OffsetB)(BO)

	xvmaddadp		vs40,	vs0,	vs25
	xvmaddadp		vs41,	vs1,	vs25
	xvmaddadp		vs42,	vs2,	vs25
	xvmaddadp		vs43,	vs3,	vs25


	xvmaddadp		vs48,	vs0,	vs26
	xvmaddadp		vs49,	vs1,	vs26
	xvmaddadp		vs50,	vs2,	vs26
	xvmaddadp		vs51,	vs3,	vs26

.endif
	xxpermdi	vs29,	vs28,	vs28,2	
	xxpermdi	vs31,	vs30,	vs30,2
.if \First ==1
	xvmuldp		vs56,	vs0,	vs27
	xvmuldp		vs57,	vs1,	vs27
	xvmuldp		vs58,	vs2,	vs27
	xvmuldp		vs59,	vs3,	vs27

.else
	xvmaddadp		vs56,	vs0,	vs27
	xvmaddadp		vs57,	vs1,	vs27
	xvmaddadp		vs58,	vs2,	vs27
	xvmaddadp		vs59,	vs3,	vs27

.endif

	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28
	xvmaddadp		vs34,	vs10,	vs28
	xvmaddadp		vs35,	vs11,	vs28
.if \Complete==0
	lxv	vs0,	DISP16(\Index,64+\OffsetA)(AO)
	lxv	vs1,	DISP16(\Index,80+\OffsetA)(AO) 
.endif


	xvmaddadp		vs40,	vs8,	vs29
	xvmaddadp		vs41,	vs9,	vs29
	xvmaddadp		vs42,	vs10,	vs29
	xvmaddadp		vs43,	vs11,	vs29

.if \Complete==0 
	lxv	vs2,	DISP16(\Index,96+\OffsetA)(AO)
	lxv	vs3,	DISP16(\Index,112+\OffsetA)(AO)
.endif	


	xvmaddadp		vs48,	vs8,	vs30
	xvmaddadp		vs49,	vs9,	vs30
	xvmaddadp		vs50,	vs10,	vs30
	xvmaddadp		vs51,	vs11,	vs30
.if \Complete==0
	lxv	vs24,	DISP8(\Index,32  +\OffsetB)(BO)
	lxv	vs26,	DISP8(\Index,48  +\OffsetB)(BO) 
.endif
 
	xvmaddadp		vs56,	vs8,	vs31
	xvmaddadp		vs57,	vs9,	vs31
	xvmaddadp		vs58,	vs10,	vs31
	xvmaddadp		vs59,	vs11,	vs31
.if \Complete==0 
	xxpermdi	vs25,	vs24,	vs24,2	
	xxpermdi	vs27,	vs26,	vs26,2
.endif

  .if \IsLast==1	
  .if \Complete==1
	addi		AO, AO, DISP16(\Index,64+\OffsetA)
	addi		BO, BO,  DISP8(\Index,32+\OffsetB)
  .else
	addi		AO, AO, DISP16(\Index,128)
	addi		BO, BO,  DISP8(\Index,64)
  .endif
  .endif
  

.endm

 

.macro KERNEL4x8 First

	lxv	vs24,	0(BO)
	lxv	vs26,	16(BO)
	xxpermdi	vs25,	vs24,	vs24,2	
	xxpermdi	vs27,	vs26,	vs26,2

	lxv	vs0,	0(AO)
	lxv	vs1,	16(AO)
	lxv	vs2,	32(AO)
	lxv	vs3,	48(AO) 



 
	addi		BO, BO, 32
    addi		AO, AO, 64

.if \First==1
	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24
	xvmuldp			vs34,	vs2,	vs24
	xvmuldp			vs35,	vs3,	vs24
 

	xvmuldp			vs40,	vs0,	vs25
	xvmuldp			vs41,	vs1,	vs25
	xvmuldp			vs42,	vs2,	vs25
	xvmuldp			vs43,	vs3,	vs25
 

	xvmuldp			vs48,	vs0,	vs26
	xvmuldp			vs49,	vs1,	vs26
	xvmuldp			vs50,	vs2,	vs26
	xvmuldp			vs51,	vs3,	vs26
 

	xvmuldp			vs56,	vs0,	vs27
	xvmuldp			vs57,	vs1,	vs27
	xvmuldp			vs58,	vs2,	vs27
	xvmuldp			vs59,	vs3,	vs27
 
.else
	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24
	xvmaddadp		vs34,	vs2,	vs24
	xvmaddadp		vs35,	vs3,	vs24


	xvmaddadp		vs40,	vs0,	vs25
	xvmaddadp		vs41,	vs1,	vs25
	xvmaddadp		vs42,	vs2,	vs25
	xvmaddadp		vs43,	vs3,	vs25
 


	xvmaddadp		vs48,	vs0,	vs26
	xvmaddadp		vs49,	vs1,	vs26
	xvmaddadp		vs50,	vs2,	vs26
	xvmaddadp		vs51,	vs3,	vs26
 


	xvmaddadp		vs56,	vs0,	vs27
	xvmaddadp		vs57,	vs1,	vs27
	xvmaddadp		vs58,	vs2,	vs27
	xvmaddadp		vs59,	vs3,	vs27


.endif
.endm

 

.macro SAVE4x8
	add		T2,	CO,	LDC
	add		T3,	T2,	LDC
	add		T4,	T3,	LDC
#ifndef TRMMKERNEL
	lxv		vs0,	0(CO)
	lxv		vs2,	16(CO)
#endif	
	xxpermdi  vs8, vs40,vs32,1
 	xxpermdi  vs9 ,vs32,vs40,1
#ifndef TRMMKERNEL	 
	lxv		vs4,	32(CO)
	lxv		vs6,	48(CO)
#endif	
	xxpermdi  vs10, vs41,vs33,1		 
 	xxpermdi  vs11 ,vs33,vs41,1
#ifndef TRMMKERNEL	 
	lxv		vs1,	0(T2)
	lxv		vs3,	16(T2)
#endif	
	xxpermdi  vs12, vs42,vs34,1
 	xxpermdi  vs13 ,vs34,vs42,1
#ifndef TRMMKERNEL	 
	lxv		vs5,	32(T2)
	lxv		vs7,	48(T2)
#endif	
	xxpermdi  vs14, vs43,vs35,1		 
 	xxpermdi  vs15 ,vs35,vs43,1	
 


#ifndef TRMMKERNEL 
	xvmaddadp	vs0,	vs8,	alpha_r 
	xvmaddadp	vs1,	vs9,	alpha_r 
	xvmaddadp	vs2,	vs10,	alpha_r 
	xvmaddadp	vs3,	vs11,	alpha_r 

	xvmaddadp	vs4,	vs12,	alpha_r 
	xvmaddadp	vs5,	vs13,	alpha_r 
	xvmaddadp	vs6,	vs14,	alpha_r 
	xvmaddadp	vs7,	vs15,	alpha_r 
#else
	xvmuldp	vs0,	vs8,	alpha_r 
	xvmuldp	vs1,	vs9,	alpha_r 
	xvmuldp	vs2,	vs10,	alpha_r 
	xvmuldp	vs3,	vs11,	alpha_r 

	xvmuldp	vs4,	vs12,	alpha_r 
	xvmuldp	vs5,	vs13,	alpha_r 
	xvmuldp	vs6,	vs14,	alpha_r 
	xvmuldp	vs7,	vs15,	alpha_r 

#endif
 

	stxv		vs0,	0(CO)
	stxv		vs2,	16(CO)
	stxv		vs4,	32(CO)
	stxv		vs6,	48(CO)

 
	stxv		vs1,	0(T2)
	stxv		vs3,	16(T2)
	stxv		vs5,	32(T2)
	stxv		vs7,	48(T2)
	
 
	xxpermdi  vs8, vs56,vs48,1
 	xxpermdi  vs9 ,vs48,vs56,1
#ifndef TRMMKERNEL 
 	lxv		vs0,	0(T3)
	lxv		vs2,	16(T3)
#endif	
	xxpermdi  vs10, vs57,vs49,1		 
 	xxpermdi  vs11 ,vs49,vs57,1	
#ifndef TRMMKERNEL 	 
	lxv		vs4,	32(T3)
	lxv		vs6,	48(T3)
#endif 
	xxpermdi  vs12, vs58,vs50,1
 	xxpermdi  vs13 ,vs50,vs58,1
#ifndef TRMMKERNEL 	 
	lxv		vs1,	0(T4)
	lxv		vs3,	16(T4)
#endif	
	xxpermdi  vs14, vs59,vs51,1		 
 	xxpermdi  vs15 ,vs51,vs59,1	
#ifndef TRMMKERNEL 	 
	lxv		vs5,	32(T4)
	lxv		vs7,	48(T4)
 
 
	xvmaddadp	vs0,	vs8,	alpha_r 
	xvmaddadp	vs1,	vs9,	alpha_r 
	xvmaddadp	vs2,	vs10,	alpha_r 
	xvmaddadp	vs3,	vs11,	alpha_r 
	


	xvmaddadp	vs4,	vs12,	alpha_r 
	xvmaddadp	vs5,	vs13,	alpha_r 
	xvmaddadp	vs6,	vs14,	alpha_r 
	xvmaddadp	vs7,	vs15,	alpha_r 
#else
	xvmuldp	vs0,	vs8,	alpha_r 
	xvmuldp	vs1,	vs9,	alpha_r 
	xvmuldp	vs2,	vs10,	alpha_r 
	xvmuldp	vs3,	vs11,	alpha_r 
	


	xvmuldp	vs4,	vs12,	alpha_r 
	xvmuldp	vs5,	vs13,	alpha_r 
	xvmuldp	vs6,	vs14,	alpha_r 
	xvmuldp	vs7,	vs15,	alpha_r 

#endif


 	stxv		vs0,	0(T3)
	stxv		vs2,	16(T3)
	stxv		vs4,	32(T3)
	stxv		vs6,	48(T3)

 
	stxv		vs1,	0(T4)
	stxv		vs3,	16(T4)
	stxv		vs5,	32(T4)
	stxv		vs7,	48(T4)
	
 

	addi		CO,	CO,	64
.endm


/*********************************************************************
* Macros for N=4, M=4                                                *
*********************************************************************/

.macro LOAD4x4_1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO
	lxvdsx	vs26,	o16,	BO
	lxvdsx	vs27,	o24,	BO

	addi		AO, AO, 32
	addi		BO, BO, 32

.endm

.macro KERNEL4x4_I1

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO

	lxvdsx	vs28,	0,	BO
	lxvdsx	vs29,	o8,	BO
	lxvdsx	vs30,	o16,	BO
	lxvdsx	vs31,	o24,	BO

	addi		AO, AO, 32
	addi		BO, BO, 32


	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24

	xvmuldp			vs40,	vs0,	vs25
	xvmuldp			vs41,	vs1,	vs25

	xvmuldp			vs48,	vs0,	vs26
	xvmuldp			vs49,	vs1,	vs26

	xvmuldp			vs56,	vs0,	vs27
	xvmuldp			vs57,	vs1,	vs27

.endm

.macro KERNEL4x4_1

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO

	lxvdsx	vs28,	0,	BO
	lxvdsx	vs29,	o8,	BO
	lxvdsx	vs30,	o16,	BO
	lxvdsx	vs31,	o24,	BO

	addi		AO, AO, 32
	addi		BO, BO, 32


	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24

	xvmaddadp		vs40,	vs0,	vs25
	xvmaddadp		vs41,	vs1,	vs25

	xvmaddadp		vs48,	vs0,	vs26
	xvmaddadp		vs49,	vs1,	vs26

	xvmaddadp		vs56,	vs0,	vs27
	xvmaddadp		vs57,	vs1,	vs27

.endm

.macro KERNEL4x4_2

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO
	lxvdsx	vs26,	o16,	BO
	lxvdsx	vs27,	o24,	BO

	addi		AO, AO, 32
	addi		BO, BO, 32


	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28

	xvmaddadp		vs40,	vs8,	vs29
	xvmaddadp		vs41,	vs9,	vs29

	xvmaddadp		vs48,	vs8,	vs30
	xvmaddadp		vs49,	vs9,	vs30

	xvmaddadp		vs56,	vs8,	vs31
	xvmaddadp		vs57,	vs9,	vs31

.endm

.macro KERNEL4x4_E2


	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28

	xvmaddadp		vs40,	vs8,	vs29
	xvmaddadp		vs41,	vs9,	vs29

	xvmaddadp		vs48,	vs8,	vs30
	xvmaddadp		vs49,	vs9,	vs30

	xvmaddadp		vs56,	vs8,	vs31
	xvmaddadp		vs57,	vs9,	vs31

.endm

.macro KERNEL4x4_SUBI1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO
	lxvdsx	vs26,	o16,	BO
	lxvdsx	vs27,	o24,	BO

	addi		AO, AO, 32
	addi		BO, BO, 32


	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24

	xvmuldp			vs40,	vs0,	vs25
	xvmuldp			vs41,	vs1,	vs25

	xvmuldp			vs48,	vs0,	vs26
	xvmuldp			vs49,	vs1,	vs26

	xvmuldp			vs56,	vs0,	vs27
	xvmuldp			vs57,	vs1,	vs27

.endm

.macro KERNEL4x4_SUB1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO
	lxvdsx	vs26,	o16,	BO
	lxvdsx	vs27,	o24,	BO

	addi		AO, AO, 32
	addi		BO, BO, 32


	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24

	xvmaddadp		vs40,	vs0,	vs25
	xvmaddadp		vs41,	vs1,	vs25

	xvmaddadp		vs48,	vs0,	vs26
	xvmaddadp		vs49,	vs1,	vs26

	xvmaddadp		vs56,	vs0,	vs27
	xvmaddadp		vs57,	vs1,	vs27

.endm

.macro SAVE4x4

	mr		T1,	CO

#ifndef TRMMKERNEL
	lxvd2x		vs0,	0,	T1
	lxvd2x		vs1,	o16,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs0,	vs32,	alpha_r
	xvmaddadp	vs1,	vs33,	alpha_r
#else
	xvmuldp		vs0,	vs32,	alpha_r
	xvmuldp		vs1,	vs33,	alpha_r
#endif

	stxvd2x		vs0,	0,	T1
	stxvd2x		vs1,	o16,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL
	lxvd2x		vs8,	0,	T1
	lxvd2x		vs9,	o16,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs8,	vs40,	alpha_r
	xvmaddadp	vs9,	vs41,	alpha_r
#else
	xvmuldp		vs8,	vs40,	alpha_r
	xvmuldp		vs9,	vs41,	alpha_r
#endif

	stxvd2x		vs8,	0,	T1
	stxvd2x		vs9,	o16,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL
	lxvd2x		vs0,	0,	T1
	lxvd2x		vs1,	o16,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs0,	vs48,	alpha_r
	xvmaddadp	vs1,	vs49,	alpha_r
#else
	xvmuldp		vs0,	vs48,	alpha_r
	xvmuldp		vs1,	vs49,	alpha_r
#endif

	stxvd2x		vs0,	0,	T1
	stxvd2x		vs1,	o16,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL
	lxvd2x		vs8,	0,	T1
	lxvd2x		vs9,	o16,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs8,	vs56,	alpha_r
	xvmaddadp	vs9,	vs57,	alpha_r
#else
	xvmuldp		vs8,	vs56,	alpha_r
	xvmuldp		vs9,	vs57,	alpha_r
#endif

	stxvd2x		vs8,	0,	T1
	stxvd2x		vs9,	o16,	T1

	addi		CO,	CO,	32

.endm

/*********************************************************************
* Macros for N=4, M=2                                                *
*********************************************************************/

.macro LOAD4x2_1

	lxvd2x	vs0,	0,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO
	lxvdsx	vs26,	o16,	BO
	lxvdsx	vs27,	o24,	BO

	addi		AO, AO, 16
	addi		BO, BO, 32

.endm

.macro KERNEL4x2_I1

	lxvd2x	vs8,	0,	AO

	lxvdsx	vs28,	0,	BO
	lxvdsx	vs29,	o8,	BO
	lxvdsx	vs30,	o16,	BO
	lxvdsx	vs31,	o24,	BO

	addi		AO, AO, 16
	addi		BO, BO, 32


	xvmuldp			vs32,	vs0,	vs24

	xvmuldp			vs40,	vs0,	vs25

	xvmuldp			vs48,	vs0,	vs26

	xvmuldp			vs56,	vs0,	vs27

.endm

.macro KERNEL4x2_1

	lxvd2x	vs8,	0,	AO

	lxvdsx	vs28,	0,	BO
	lxvdsx	vs29,	o8,	BO
	lxvdsx	vs30,	o16,	BO
	lxvdsx	vs31,	o24,	BO

	addi		AO, AO, 16
	addi		BO, BO, 32


	xvmaddadp		vs32,	vs0,	vs24

	xvmaddadp		vs40,	vs0,	vs25

	xvmaddadp		vs48,	vs0,	vs26

	xvmaddadp		vs56,	vs0,	vs27

.endm

.macro KERNEL4x2_2

	lxvd2x	vs0,	0,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO
	lxvdsx	vs26,	o16,	BO
	lxvdsx	vs27,	o24,	BO

	addi		AO, AO, 16
	addi		BO, BO, 32


	xvmaddadp		vs32,	vs8,	vs28

	xvmaddadp		vs40,	vs8,	vs29

	xvmaddadp		vs48,	vs8,	vs30

	xvmaddadp		vs56,	vs8,	vs31

.endm

.macro KERNEL4x2_E2


	xvmaddadp		vs32,	vs8,	vs28

	xvmaddadp		vs40,	vs8,	vs29

	xvmaddadp		vs48,	vs8,	vs30

	xvmaddadp		vs56,	vs8,	vs31

.endm

.macro KERNEL4x2_SUBI1

	lxvd2x	vs0,	0,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO
	lxvdsx	vs26,	o16,	BO
	lxvdsx	vs27,	o24,	BO

	addi		AO, AO, 16
	addi		BO, BO, 32


	xvmuldp			vs32,	vs0,	vs24

	xvmuldp			vs40,	vs0,	vs25

	xvmuldp			vs48,	vs0,	vs26

	xvmuldp			vs56,	vs0,	vs27

.endm

.macro KERNEL4x2_SUB1

	lxvd2x	vs0,	0,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO
	lxvdsx	vs26,	o16,	BO
	lxvdsx	vs27,	o24,	BO

	addi		AO, AO, 16
	addi		BO, BO, 32


	xvmaddadp		vs32,	vs0,	vs24

	xvmaddadp		vs40,	vs0,	vs25

	xvmaddadp		vs48,	vs0,	vs26

	xvmaddadp		vs56,	vs0,	vs27

.endm

.macro SAVE4x2

	mr		T1,	CO

#ifndef TRMMKERNEL
	lxvd2x		vs0,	0,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs0,	vs32,	alpha_r
#else
	xvmuldp		vs0,	vs32,	alpha_r
#endif

	stxvd2x		vs0,	0,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL
	lxvd2x		vs8,	0,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs8,	vs40,	alpha_r
#else
	xvmuldp		vs8,	vs40,	alpha_r
#endif

	stxvd2x		vs8,	0,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL
	lxvd2x		vs0,	0,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs0,	vs48,	alpha_r
#else
	xvmuldp		vs0,	vs48,	alpha_r
#endif

	stxvd2x		vs0,	0,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL
	lxvd2x		vs8,	0,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs8,	vs56,	alpha_r
#else
	xvmuldp		vs8,	vs56,	alpha_r
#endif

	stxvd2x		vs8,	0,	T1

	addi		CO,	CO,	16

.endm

/*********************************************************************
* Macros for N=4, M=1                                                *
*********************************************************************/

.macro LOAD4x1_1

	lxsdx	vs0,	0,	AO

	lxsdx	vs24,	0,	BO
	lxsdx	vs25,	o8,	BO
	lxsdx	vs26,	o16,	BO
	lxsdx	vs27,	o24,	BO

	addi		AO, AO, 8
	addi		BO, BO, 32

.endm

.macro KERNEL4x1_I1

	lxsdx	vs8,	0,	AO

	lxsdx	vs28,	0,	BO
	lxsdx	vs29,	o8,	BO
	lxsdx	vs30,	o16,	BO
	lxsdx	vs31,	o24,	BO

	addi		AO, AO, 8
	addi		BO, BO, 32


	xsmuldp			vs32,	vs0,	vs24

	xsmuldp			vs40,	vs0,	vs25

	xsmuldp			vs48,	vs0,	vs26

	xsmuldp			vs56,	vs0,	vs27

.endm

.macro KERNEL4x1_1

	lxsdx	vs8,	0,	AO

	lxsdx	vs28,	0,	BO
	lxsdx	vs29,	o8,	BO
	lxsdx	vs30,	o16,	BO
	lxsdx	vs31,	o24,	BO

	addi		AO, AO, 8
	addi		BO, BO, 32


	xsmaddadp		vs32,	vs0,	vs24

	xsmaddadp		vs40,	vs0,	vs25

	xsmaddadp		vs48,	vs0,	vs26

	xsmaddadp		vs56,	vs0,	vs27

.endm

.macro KERNEL4x1_2

	lxsdx	vs0,	0,	AO

	lxsdx	vs24,	0,	BO
	lxsdx	vs25,	o8,	BO
	lxsdx	vs26,	o16,	BO
	lxsdx	vs27,	o24,	BO

	addi		AO, AO, 8
	addi		BO, BO, 32


	xsmaddadp		vs32,	vs8,	vs28

	xsmaddadp		vs40,	vs8,	vs29

	xsmaddadp		vs48,	vs8,	vs30

	xsmaddadp		vs56,	vs8,	vs31

.endm

.macro KERNEL4x1_E2


	xsmaddadp		vs32,	vs8,	vs28

	xsmaddadp		vs40,	vs8,	vs29

	xsmaddadp		vs48,	vs8,	vs30

	xsmaddadp		vs56,	vs8,	vs31

.endm

.macro KERNEL4x1_SUBI1

	lxsdx	vs0,	0,	AO

	lxsdx	vs24,	0,	BO
	lxsdx	vs25,	o8,	BO
	lxsdx	vs26,	o16,	BO
	lxsdx	vs27,	o24,	BO

	addi		AO, AO, 8
	addi		BO, BO, 32


	xsmuldp			vs32,	vs0,	vs24

	xsmuldp			vs40,	vs0,	vs25

	xsmuldp			vs48,	vs0,	vs26

	xsmuldp			vs56,	vs0,	vs27

.endm

.macro KERNEL4x1_SUB1

	lxsdx	vs0,	0,	AO

	lxsdx	vs24,	0,	BO
	lxsdx	vs25,	o8,	BO
	lxsdx	vs26,	o16,	BO
	lxsdx	vs27,	o24,	BO

	addi		AO, AO, 8
	addi		BO, BO, 32


	xsmaddadp		vs32,	vs0,	vs24

	xsmaddadp		vs40,	vs0,	vs25

	xsmaddadp		vs48,	vs0,	vs26

	xsmaddadp		vs56,	vs0,	vs27

.endm

.macro SAVE4x1

	mr		T1,	CO

#ifndef TRMMKERNEL
	lxsdx		vs0,	0,	T1
#endif

#ifndef TRMMKERNEL
	xsmaddadp	vs0,	vs32,	alpha_r
#else
	xsmuldp		vs0,	vs32,	alpha_r
#endif

	stxsdx		vs0,	0,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL
	lxsdx		vs8,	0,	T1
#endif

#ifndef TRMMKERNEL
	xsmaddadp	vs8,	vs40,	alpha_r
#else
	xsmuldp		vs8,	vs40,	alpha_r
#endif

	stxsdx		vs8,	0,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL
	lxsdx		vs0,	0,	T1
#endif

#ifndef TRMMKERNEL
	xsmaddadp	vs0,	vs48,	alpha_r
#else
	xsmuldp		vs0,	vs48,	alpha_r
#endif

	stxsdx		vs0,	0,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL
	lxsdx		vs8,	0,	T1
#endif

#ifndef TRMMKERNEL
	xsmaddadp	vs8,	vs56,	alpha_r
#else
	xsmuldp		vs8,	vs56,	alpha_r
#endif

	stxsdx		vs8,	0,	T1

	addi		CO,	CO,	8

.endm

/*********************************************************************
* Macros for N=2, M=16                                               *
*********************************************************************/

.macro LOAD2x16_1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 64
	addi		BO, BO, 16

	lxvd2x	vs4,	0,	AO
	lxvd2x	vs5,	o16,	AO
	lxvd2x	vs6,	o32,	AO
	lxvd2x	vs7,	o48,	AO

	addi		AO, AO, 64

.endm

.macro KERNEL2x16_I1

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO
	lxvd2x	vs10,	o32,	AO
	lxvd2x	vs11,	o48,	AO

	lxvdsx	vs28,	0,	BO
	lxvdsx	vs29,	o8,	BO

	addi		AO, AO, 64
	addi		BO, BO, 16

	lxvd2x	vs12,	0,	AO
	lxvd2x	vs13,	o16,	AO
	lxvd2x	vs14,	o32,	AO
	lxvd2x	vs15,	o48,	AO

	addi		AO, AO, 64


	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24
	xvmuldp			vs34,	vs2,	vs24
	xvmuldp			vs35,	vs3,	vs24
	xvmuldp			vs36,	vs4,	vs24
	xvmuldp			vs37,	vs5,	vs24
	xvmuldp			vs38,	vs6,	vs24
	xvmuldp			vs39,	vs7,	vs24

	xvmuldp			vs40,	vs0,	vs25
	xvmuldp			vs41,	vs1,	vs25
	xvmuldp			vs42,	vs2,	vs25
	xvmuldp			vs43,	vs3,	vs25
	xvmuldp			vs44,	vs4,	vs25
	xvmuldp			vs45,	vs5,	vs25
	xvmuldp			vs46,	vs6,	vs25
	xvmuldp			vs47,	vs7,	vs25

.endm

.macro KERNEL2x16_1

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO
	lxvd2x	vs10,	o32,	AO
	lxvd2x	vs11,	o48,	AO

	lxvdsx	vs28,	0,	BO
	lxvdsx	vs29,	o8,	BO

	addi		AO, AO, 64
	addi		BO, BO, 16

	lxvd2x	vs12,	0,	AO
	lxvd2x	vs13,	o16,	AO
	lxvd2x	vs14,	o32,	AO
	lxvd2x	vs15,	o48,	AO

	addi		AO, AO, 64


	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24
	xvmaddadp		vs34,	vs2,	vs24
	xvmaddadp		vs35,	vs3,	vs24
	xvmaddadp		vs36,	vs4,	vs24
	xvmaddadp		vs37,	vs5,	vs24
	xvmaddadp		vs38,	vs6,	vs24
	xvmaddadp		vs39,	vs7,	vs24

	xvmaddadp		vs40,	vs0,	vs25
	xvmaddadp		vs41,	vs1,	vs25
	xvmaddadp		vs42,	vs2,	vs25
	xvmaddadp		vs43,	vs3,	vs25
	xvmaddadp		vs44,	vs4,	vs25
	xvmaddadp		vs45,	vs5,	vs25
	xvmaddadp		vs46,	vs6,	vs25
	xvmaddadp		vs47,	vs7,	vs25

.endm

.macro KERNEL2x16_2

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 64
	addi		BO, BO, 16

	lxvd2x	vs4,	0,	AO
	lxvd2x	vs5,	o16,	AO
	lxvd2x	vs6,	o32,	AO
	lxvd2x	vs7,	o48,	AO

	addi		AO, AO, 64


	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28
	xvmaddadp		vs34,	vs10,	vs28
	xvmaddadp		vs35,	vs11,	vs28
	xvmaddadp		vs36,	vs12,	vs28
	xvmaddadp		vs37,	vs13,	vs28
	xvmaddadp		vs38,	vs14,	vs28
	xvmaddadp		vs39,	vs15,	vs28

	xvmaddadp		vs40,	vs8,	vs29
	xvmaddadp		vs41,	vs9,	vs29
	xvmaddadp		vs42,	vs10,	vs29
	xvmaddadp		vs43,	vs11,	vs29
	xvmaddadp		vs44,	vs12,	vs29
	xvmaddadp		vs45,	vs13,	vs29
	xvmaddadp		vs46,	vs14,	vs29
	xvmaddadp		vs47,	vs15,	vs29

.endm

.macro KERNEL2x16_E2


	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28
	xvmaddadp		vs34,	vs10,	vs28
	xvmaddadp		vs35,	vs11,	vs28
	xvmaddadp		vs36,	vs12,	vs28
	xvmaddadp		vs37,	vs13,	vs28
	xvmaddadp		vs38,	vs14,	vs28
	xvmaddadp		vs39,	vs15,	vs28

	xvmaddadp		vs40,	vs8,	vs29
	xvmaddadp		vs41,	vs9,	vs29
	xvmaddadp		vs42,	vs10,	vs29
	xvmaddadp		vs43,	vs11,	vs29
	xvmaddadp		vs44,	vs12,	vs29
	xvmaddadp		vs45,	vs13,	vs29
	xvmaddadp		vs46,	vs14,	vs29
	xvmaddadp		vs47,	vs15,	vs29

.endm

.macro KERNEL2x16_SUBI1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 64
	addi		BO, BO, 16

	lxvd2x	vs4,	0,	AO
	lxvd2x	vs5,	o16,	AO
	lxvd2x	vs6,	o32,	AO
	lxvd2x	vs7,	o48,	AO

	addi		AO, AO, 64


	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24
	xvmuldp			vs34,	vs2,	vs24
	xvmuldp			vs35,	vs3,	vs24
	xvmuldp			vs36,	vs4,	vs24
	xvmuldp			vs37,	vs5,	vs24
	xvmuldp			vs38,	vs6,	vs24
	xvmuldp			vs39,	vs7,	vs24

	xvmuldp			vs40,	vs0,	vs25
	xvmuldp			vs41,	vs1,	vs25
	xvmuldp			vs42,	vs2,	vs25
	xvmuldp			vs43,	vs3,	vs25
	xvmuldp			vs44,	vs4,	vs25
	xvmuldp			vs45,	vs5,	vs25
	xvmuldp			vs46,	vs6,	vs25
	xvmuldp			vs47,	vs7,	vs25

.endm

.macro KERNEL2x16_SUB1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 64
	addi		BO, BO, 16

	lxvd2x	vs4,	0,	AO
	lxvd2x	vs5,	o16,	AO
	lxvd2x	vs6,	o32,	AO
	lxvd2x	vs7,	o48,	AO

	addi		AO, AO, 64


	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24
	xvmaddadp		vs34,	vs2,	vs24
	xvmaddadp		vs35,	vs3,	vs24
	xvmaddadp		vs36,	vs4,	vs24
	xvmaddadp		vs37,	vs5,	vs24
	xvmaddadp		vs38,	vs6,	vs24
	xvmaddadp		vs39,	vs7,	vs24

	xvmaddadp		vs40,	vs0,	vs25
	xvmaddadp		vs41,	vs1,	vs25
	xvmaddadp		vs42,	vs2,	vs25
	xvmaddadp		vs43,	vs3,	vs25
	xvmaddadp		vs44,	vs4,	vs25
	xvmaddadp		vs45,	vs5,	vs25
	xvmaddadp		vs46,	vs6,	vs25
	xvmaddadp		vs47,	vs7,	vs25

.endm

.macro SAVE2x16

	mr		T1,	CO
	addi		T2,	T1,	64

#ifndef TRMMKERNEL
	lxvd2x		vs0,	0,	T1
	lxvd2x		vs1,	o16,	T1
	lxvd2x		vs2,	o32,	T1
	lxvd2x		vs3,	o48,	T1

	lxvd2x		vs4,	0,	T2
	lxvd2x		vs5,	o16,	T2
	lxvd2x		vs6,	o32,	T2
	lxvd2x		vs7,	o48,	T2
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs0,	vs32,	alpha_r
	xvmaddadp	vs1,	vs33,	alpha_r
	xvmaddadp	vs2,	vs34,	alpha_r
	xvmaddadp	vs3,	vs35,	alpha_r
	xvmaddadp	vs4,	vs36,	alpha_r
	xvmaddadp	vs5,	vs37,	alpha_r
	xvmaddadp	vs6,	vs38,	alpha_r
	xvmaddadp	vs7,	vs39,	alpha_r
#else
	xvmuldp		vs0,	vs32,	alpha_r
	xvmuldp		vs1,	vs33,	alpha_r
	xvmuldp		vs2,	vs34,	alpha_r
	xvmuldp		vs3,	vs35,	alpha_r
	xvmuldp		vs4,	vs36,	alpha_r
	xvmuldp		vs5,	vs37,	alpha_r
	xvmuldp		vs6,	vs38,	alpha_r
	xvmuldp		vs7,	vs39,	alpha_r
#endif

	stxvd2x		vs0,	0,	T1
	stxvd2x		vs1,	o16,	T1
	stxvd2x		vs2,	o32,	T1
	stxvd2x		vs3,	o48,	T1

	stxvd2x		vs4,	0,	T2
	stxvd2x		vs5,	o16,	T2
	stxvd2x		vs6,	o32,	T2
	stxvd2x		vs7,	o48,	T2

	add		T1,	T1,	LDC
	add		T2,	T2,	LDC

#ifndef TRMMKERNEL
	lxvd2x		vs8,	0,	T1
	lxvd2x		vs9,	o16,	T1
	lxvd2x		vs10,	o32,	T1
	lxvd2x		vs11,	o48,	T1

	lxvd2x		vs12,	0,	T2
	lxvd2x		vs13,	o16,	T2
	lxvd2x		vs14,	o32,	T2
	lxvd2x		vs15,	o48,	T2
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs8,	vs40,	alpha_r
	xvmaddadp	vs9,	vs41,	alpha_r
	xvmaddadp	vs10,	vs42,	alpha_r
	xvmaddadp	vs11,	vs43,	alpha_r
	xvmaddadp	vs12,	vs44,	alpha_r
	xvmaddadp	vs13,	vs45,	alpha_r
	xvmaddadp	vs14,	vs46,	alpha_r
	xvmaddadp	vs15,	vs47,	alpha_r
#else
	xvmuldp		vs8,	vs40,	alpha_r
	xvmuldp		vs9,	vs41,	alpha_r
	xvmuldp		vs10,	vs42,	alpha_r
	xvmuldp		vs11,	vs43,	alpha_r
	xvmuldp		vs12,	vs44,	alpha_r
	xvmuldp		vs13,	vs45,	alpha_r
	xvmuldp		vs14,	vs46,	alpha_r
	xvmuldp		vs15,	vs47,	alpha_r
#endif

	stxvd2x		vs8,	0,	T1
	stxvd2x		vs9,	o16,	T1
	stxvd2x		vs10,	o32,	T1
	stxvd2x		vs11,	o48,	T1

	stxvd2x		vs12,	0,	T2
	stxvd2x		vs13,	o16,	T2
	stxvd2x		vs14,	o32,	T2
	stxvd2x		vs15,	o48,	T2

	addi		CO,	CO,	128

.endm

/*********************************************************************
* Macros for N=4, M=8                                                *
*********************************************************************/

.macro LOAD2x8_1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 64
	addi		BO, BO, 16

.endm

.macro KERNEL2x8_I1

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO
	lxvd2x	vs10,	o32,	AO
	lxvd2x	vs11,	o48,	AO

	lxvdsx	vs28,	0,	BO
	lxvdsx	vs29,	o8,	BO

	addi		AO, AO, 64
	addi		BO, BO, 16


	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24
	xvmuldp			vs34,	vs2,	vs24
	xvmuldp			vs35,	vs3,	vs24

	xvmuldp			vs40,	vs0,	vs25
	xvmuldp			vs41,	vs1,	vs25
	xvmuldp			vs42,	vs2,	vs25
	xvmuldp			vs43,	vs3,	vs25

.endm

.macro KERNEL2x8_1

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO
	lxvd2x	vs10,	o32,	AO
	lxvd2x	vs11,	o48,	AO

	lxvdsx	vs28,	0,	BO
	lxvdsx	vs29,	o8,	BO

	addi		AO, AO, 64
	addi		BO, BO, 16


	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24
	xvmaddadp		vs34,	vs2,	vs24
	xvmaddadp		vs35,	vs3,	vs24

	xvmaddadp		vs40,	vs0,	vs25
	xvmaddadp		vs41,	vs1,	vs25
	xvmaddadp		vs42,	vs2,	vs25
	xvmaddadp		vs43,	vs3,	vs25

.endm

.macro KERNEL2x8_2

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 64
	addi		BO, BO, 16


	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28
	xvmaddadp		vs34,	vs10,	vs28
	xvmaddadp		vs35,	vs11,	vs28

	xvmaddadp		vs40,	vs8,	vs29
	xvmaddadp		vs41,	vs9,	vs29
	xvmaddadp		vs42,	vs10,	vs29
	xvmaddadp		vs43,	vs11,	vs29

.endm

.macro KERNEL2x8_E2


	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28
	xvmaddadp		vs34,	vs10,	vs28
	xvmaddadp		vs35,	vs11,	vs28

	xvmaddadp		vs40,	vs8,	vs29
	xvmaddadp		vs41,	vs9,	vs29
	xvmaddadp		vs42,	vs10,	vs29
	xvmaddadp		vs43,	vs11,	vs29

.endm

.macro KERNEL2x8_SUBI1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 64
	addi		BO, BO, 16


	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24
	xvmuldp			vs34,	vs2,	vs24
	xvmuldp			vs35,	vs3,	vs24

	xvmuldp			vs40,	vs0,	vs25
	xvmuldp			vs41,	vs1,	vs25
	xvmuldp			vs42,	vs2,	vs25
	xvmuldp			vs43,	vs3,	vs25

.endm

.macro KERNEL2x8_SUB1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 64
	addi		BO, BO, 16


	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24
	xvmaddadp		vs34,	vs2,	vs24
	xvmaddadp		vs35,	vs3,	vs24

	xvmaddadp		vs40,	vs0,	vs25
	xvmaddadp		vs41,	vs1,	vs25
	xvmaddadp		vs42,	vs2,	vs25
	xvmaddadp		vs43,	vs3,	vs25

.endm

.macro SAVE2x8

	mr		T1,	CO

#ifndef TRMMKERNEL
	lxvd2x		vs0,	0,	T1
	lxvd2x		vs1,	o16,	T1
	lxvd2x		vs2,	o32,	T1
	lxvd2x		vs3,	o48,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs0,	vs32,	alpha_r
	xvmaddadp	vs1,	vs33,	alpha_r
	xvmaddadp	vs2,	vs34,	alpha_r
	xvmaddadp	vs3,	vs35,	alpha_r
#else
	xvmuldp		vs0,	vs32,	alpha_r
	xvmuldp		vs1,	vs33,	alpha_r
	xvmuldp		vs2,	vs34,	alpha_r
	xvmuldp		vs3,	vs35,	alpha_r
#endif

	stxvd2x		vs0,	0,	T1
	stxvd2x		vs1,	o16,	T1
	stxvd2x		vs2,	o32,	T1
	stxvd2x		vs3,	o48,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL
	lxvd2x		vs8,	0,	T1
	lxvd2x		vs9,	o16,	T1
	lxvd2x		vs10,	o32,	T1
	lxvd2x		vs11,	o48,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs8,	vs40,	alpha_r
	xvmaddadp	vs9,	vs41,	alpha_r
	xvmaddadp	vs10,	vs42,	alpha_r
	xvmaddadp	vs11,	vs43,	alpha_r
#else
	xvmuldp		vs8,	vs40,	alpha_r
	xvmuldp		vs9,	vs41,	alpha_r
	xvmuldp		vs10,	vs42,	alpha_r
	xvmuldp		vs11,	vs43,	alpha_r
#endif

	stxvd2x		vs8,	0,	T1
	stxvd2x		vs9,	o16,	T1
	stxvd2x		vs10,	o32,	T1
	stxvd2x		vs11,	o48,	T1

	addi		CO,	CO,	64

.endm

/*********************************************************************
* Macros for N=2, M=4                                                *
*********************************************************************/

.macro LOAD2x4_1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 32
	addi		BO, BO, 16

.endm

.macro KERNEL2x4_I1

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO

	lxvdsx	vs28,	0,	BO
	lxvdsx	vs29,	o8,	BO

	addi		AO, AO, 32
	addi		BO, BO, 16


	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24

	xvmuldp			vs40,	vs0,	vs25
	xvmuldp			vs41,	vs1,	vs25

.endm

.macro KERNEL2x4_1

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO

	lxvdsx	vs28,	0,	BO
	lxvdsx	vs29,	o8,	BO

	addi		AO, AO, 32
	addi		BO, BO, 16


	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24

	xvmaddadp		vs40,	vs0,	vs25
	xvmaddadp		vs41,	vs1,	vs25

.endm

.macro KERNEL2x4_2

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 32
	addi		BO, BO, 16


	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28

	xvmaddadp		vs40,	vs8,	vs29
	xvmaddadp		vs41,	vs9,	vs29

.endm

.macro KERNEL2x4_E2


	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28

	xvmaddadp		vs40,	vs8,	vs29
	xvmaddadp		vs41,	vs9,	vs29

.endm

.macro KERNEL2x4_SUBI1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 32
	addi		BO, BO, 16


	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24

	xvmuldp			vs40,	vs0,	vs25
	xvmuldp			vs41,	vs1,	vs25

.endm

.macro KERNEL2x4_SUB1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 32
	addi		BO, BO, 16


	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24

	xvmaddadp		vs40,	vs0,	vs25
	xvmaddadp		vs41,	vs1,	vs25

.endm

.macro SAVE2x4

	mr		T1,	CO

#ifndef TRMMKERNEL
	lxvd2x		vs0,	0,	T1
	lxvd2x		vs1,	o16,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs0,	vs32,	alpha_r
	xvmaddadp	vs1,	vs33,	alpha_r
#else
	xvmuldp		vs0,	vs32,	alpha_r
	xvmuldp		vs1,	vs33,	alpha_r
#endif

	stxvd2x		vs0,	0,	T1
	stxvd2x		vs1,	o16,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL
	lxvd2x		vs8,	0,	T1
	lxvd2x		vs9,	o16,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs8,	vs40,	alpha_r
	xvmaddadp	vs9,	vs41,	alpha_r
#else
	xvmuldp		vs8,	vs40,	alpha_r
	xvmuldp		vs9,	vs41,	alpha_r
#endif

	stxvd2x		vs8,	0,	T1
	stxvd2x		vs9,	o16,	T1

	addi		CO,	CO,	32

.endm

/*********************************************************************
* Macros for N=2, M=2                                                *
*********************************************************************/

.macro LOAD2x2_1

	lxvd2x	vs0,	0,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 16
	addi		BO, BO, 16

.endm

.macro KERNEL2x2_I1

	lxvd2x	vs8,	0,	AO

	lxvdsx	vs28,	0,	BO
	lxvdsx	vs29,	o8,	BO

	addi		AO, AO, 16
	addi		BO, BO, 16


	xvmuldp			vs32,	vs0,	vs24

	xvmuldp			vs40,	vs0,	vs25

.endm

.macro KERNEL2x2_1

	lxvd2x	vs8,	0,	AO

	lxvdsx	vs28,	0,	BO
	lxvdsx	vs29,	o8,	BO

	addi		AO, AO, 16
	addi		BO, BO, 16


	xvmaddadp		vs32,	vs0,	vs24

	xvmaddadp		vs40,	vs0,	vs25

.endm

.macro KERNEL2x2_2

	lxvd2x	vs0,	0,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 16
	addi		BO, BO, 16


	xvmaddadp		vs32,	vs8,	vs28

	xvmaddadp		vs40,	vs8,	vs29

.endm

.macro KERNEL2x2_E2


	xvmaddadp		vs32,	vs8,	vs28

	xvmaddadp		vs40,	vs8,	vs29

.endm

.macro KERNEL2x2_SUBI1

	lxvd2x	vs0,	0,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 16
	addi		BO, BO, 16


	xvmuldp			vs32,	vs0,	vs24

	xvmuldp			vs40,	vs0,	vs25

.endm

.macro KERNEL2x2_SUB1

	lxvd2x	vs0,	0,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 16
	addi		BO, BO, 16


	xvmaddadp		vs32,	vs0,	vs24

	xvmaddadp		vs40,	vs0,	vs25

.endm

.macro SAVE2x2

	mr		T1,	CO

#ifndef TRMMKERNEL
	lxvd2x		vs0,	0,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs0,	vs32,	alpha_r
#else
	xvmuldp		vs0,	vs32,	alpha_r
#endif

	stxvd2x		vs0,	0,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL
	lxvd2x		vs8,	0,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs8,	vs40,	alpha_r
#else
	xvmuldp		vs8,	vs40,	alpha_r
#endif

	stxvd2x		vs8,	0,	T1

	addi		CO,	CO,	16

.endm

/*********************************************************************
* Macros for N=2, M=1                                                *
*********************************************************************/

.macro LOAD2x1_1

	lxsdx	vs0,	0,	AO

	lxsdx	vs24,	0,	BO
	lxsdx	vs25,	o8,	BO

	addi		AO, AO, 8
	addi		BO, BO, 16

.endm

.macro KERNEL2x1_I1

	lxsdx	vs8,	0,	AO

	lxsdx	vs28,	0,	BO
	lxsdx	vs29,	o8,	BO

	addi		AO, AO, 8
	addi		BO, BO, 16


	xsmuldp			vs32,	vs0,	vs24

	xsmuldp			vs40,	vs0,	vs25

.endm

.macro KERNEL2x1_1

	lxsdx	vs8,	0,	AO

	lxsdx	vs28,	0,	BO
	lxsdx	vs29,	o8,	BO

	addi		AO, AO, 8
	addi		BO, BO, 16


	xsmaddadp		vs32,	vs0,	vs24

	xsmaddadp		vs40,	vs0,	vs25

.endm

.macro KERNEL2x1_2

	lxsdx	vs0,	0,	AO

	lxsdx	vs24,	0,	BO
	lxsdx	vs25,	o8,	BO

	addi		AO, AO, 8
	addi		BO, BO, 16


	xsmaddadp		vs32,	vs8,	vs28

	xsmaddadp		vs40,	vs8,	vs29

.endm

.macro KERNEL2x1_E2


	xsmaddadp		vs32,	vs8,	vs28

	xsmaddadp		vs40,	vs8,	vs29

.endm

.macro KERNEL2x1_SUBI1

	lxsdx	vs0,	0,	AO

	lxsdx	vs24,	0,	BO
	lxsdx	vs25,	o8,	BO

	addi		AO, AO, 8
	addi		BO, BO, 16


	xsmuldp			vs32,	vs0,	vs24

	xsmuldp			vs40,	vs0,	vs25

.endm

.macro KERNEL2x1_SUB1

	lxsdx	vs0,	0,	AO

	lxsdx	vs24,	0,	BO
	lxsdx	vs25,	o8,	BO

	addi		AO, AO, 8
	addi		BO, BO, 16


	xsmaddadp		vs32,	vs0,	vs24

	xsmaddadp		vs40,	vs0,	vs25

.endm

.macro SAVE2x1

	mr		T1,	CO

#ifndef TRMMKERNEL
	lxsdx		vs0,	0,	T1
#endif

#ifndef TRMMKERNEL
	xsmaddadp	vs0,	vs32,	alpha_r
#else
	xsmuldp		vs0,	vs32,	alpha_r
#endif

	stxsdx		vs0,	0,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL
	lxsdx		vs8,	0,	T1
#endif

#ifndef TRMMKERNEL
	xsmaddadp	vs8,	vs40,	alpha_r
#else
	xsmuldp		vs8,	vs40,	alpha_r
#endif

	stxsdx		vs8,	0,	T1

	addi		CO,	CO,	8

.endm

/*********************************************************************
* Macros for N=1, M=16                                               *
*********************************************************************/

.macro LOAD1x16_1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 64
	addi		BO, BO, 8

	lxvd2x	vs4,	0,	AO
	lxvd2x	vs5,	o16,	AO
	lxvd2x	vs6,	o32,	AO
	lxvd2x	vs7,	o48,	AO

	addi		AO, AO, 64

.endm

.macro KERNEL1x16_I1

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO
	lxvd2x	vs10,	o32,	AO
	lxvd2x	vs11,	o48,	AO

	lxvdsx	vs28,	0,	BO

	addi		AO, AO, 64
	addi		BO, BO, 8

	lxvd2x	vs12,	0,	AO
	lxvd2x	vs13,	o16,	AO
	lxvd2x	vs14,	o32,	AO
	lxvd2x	vs15,	o48,	AO

	addi		AO, AO, 64


	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24
	xvmuldp			vs34,	vs2,	vs24
	xvmuldp			vs35,	vs3,	vs24
	xvmuldp			vs36,	vs4,	vs24
	xvmuldp			vs37,	vs5,	vs24
	xvmuldp			vs38,	vs6,	vs24
	xvmuldp			vs39,	vs7,	vs24

.endm

.macro KERNEL1x16_1

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO
	lxvd2x	vs10,	o32,	AO
	lxvd2x	vs11,	o48,	AO

	lxvdsx	vs28,	0,	BO

	addi		AO, AO, 64
	addi		BO, BO, 8

	lxvd2x	vs12,	0,	AO
	lxvd2x	vs13,	o16,	AO
	lxvd2x	vs14,	o32,	AO
	lxvd2x	vs15,	o48,	AO

	addi		AO, AO, 64


	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24
	xvmaddadp		vs34,	vs2,	vs24
	xvmaddadp		vs35,	vs3,	vs24
	xvmaddadp		vs36,	vs4,	vs24
	xvmaddadp		vs37,	vs5,	vs24
	xvmaddadp		vs38,	vs6,	vs24
	xvmaddadp		vs39,	vs7,	vs24

.endm

.macro KERNEL1x16_2

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 64
	addi		BO, BO, 8

	lxvd2x	vs4,	0,	AO
	lxvd2x	vs5,	o16,	AO
	lxvd2x	vs6,	o32,	AO
	lxvd2x	vs7,	o48,	AO

	addi		AO, AO, 64


	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28
	xvmaddadp		vs34,	vs10,	vs28
	xvmaddadp		vs35,	vs11,	vs28
	xvmaddadp		vs36,	vs12,	vs28
	xvmaddadp		vs37,	vs13,	vs28
	xvmaddadp		vs38,	vs14,	vs28
	xvmaddadp		vs39,	vs15,	vs28

.endm

.macro KERNEL1x16_E2


	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28
	xvmaddadp		vs34,	vs10,	vs28
	xvmaddadp		vs35,	vs11,	vs28
	xvmaddadp		vs36,	vs12,	vs28
	xvmaddadp		vs37,	vs13,	vs28
	xvmaddadp		vs38,	vs14,	vs28
	xvmaddadp		vs39,	vs15,	vs28

.endm

.macro KERNEL1x16_SUBI1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 64
	addi		BO, BO, 8

	lxvd2x	vs4,	0,	AO
	lxvd2x	vs5,	o16,	AO
	lxvd2x	vs6,	o32,	AO
	lxvd2x	vs7,	o48,	AO

	addi		AO, AO, 64


	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24
	xvmuldp			vs34,	vs2,	vs24
	xvmuldp			vs35,	vs3,	vs24
	xvmuldp			vs36,	vs4,	vs24
	xvmuldp			vs37,	vs5,	vs24
	xvmuldp			vs38,	vs6,	vs24
	xvmuldp			vs39,	vs7,	vs24

.endm

.macro KERNEL1x16_SUB1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 64
	addi		BO, BO, 8

	lxvd2x	vs4,	0,	AO
	lxvd2x	vs5,	o16,	AO
	lxvd2x	vs6,	o32,	AO
	lxvd2x	vs7,	o48,	AO

	addi		AO, AO, 64


	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24
	xvmaddadp		vs34,	vs2,	vs24
	xvmaddadp		vs35,	vs3,	vs24
	xvmaddadp		vs36,	vs4,	vs24
	xvmaddadp		vs37,	vs5,	vs24
	xvmaddadp		vs38,	vs6,	vs24
	xvmaddadp		vs39,	vs7,	vs24

.endm

.macro SAVE1x16

	mr		T1,	CO
	addi		T2,	T1,	64

#ifndef TRMMKERNEL
	lxvd2x		vs0,	0,	T1
	lxvd2x		vs1,	o16,	T1
	lxvd2x		vs2,	o32,	T1
	lxvd2x		vs3,	o48,	T1

	lxvd2x		vs4,	0,	T2
	lxvd2x		vs5,	o16,	T2
	lxvd2x		vs6,	o32,	T2
	lxvd2x		vs7,	o48,	T2
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs0,	vs32,	alpha_r
	xvmaddadp	vs1,	vs33,	alpha_r
	xvmaddadp	vs2,	vs34,	alpha_r
	xvmaddadp	vs3,	vs35,	alpha_r
	xvmaddadp	vs4,	vs36,	alpha_r
	xvmaddadp	vs5,	vs37,	alpha_r
	xvmaddadp	vs6,	vs38,	alpha_r
	xvmaddadp	vs7,	vs39,	alpha_r
#else
	xvmuldp		vs0,	vs32,	alpha_r
	xvmuldp		vs1,	vs33,	alpha_r
	xvmuldp		vs2,	vs34,	alpha_r
	xvmuldp		vs3,	vs35,	alpha_r
	xvmuldp		vs4,	vs36,	alpha_r
	xvmuldp		vs5,	vs37,	alpha_r
	xvmuldp		vs6,	vs38,	alpha_r
	xvmuldp		vs7,	vs39,	alpha_r
#endif

	stxvd2x		vs0,	0,	T1
	stxvd2x		vs1,	o16,	T1
	stxvd2x		vs2,	o32,	T1
	stxvd2x		vs3,	o48,	T1

	stxvd2x		vs4,	0,	T2
	stxvd2x		vs5,	o16,	T2
	stxvd2x		vs6,	o32,	T2
	stxvd2x		vs7,	o48,	T2

	addi		CO,	CO,	128

.endm

/*********************************************************************
* Macros for N=4, M=8                                                *
*********************************************************************/

.macro LOAD1x8_1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 64
	addi		BO, BO, 8

.endm

.macro KERNEL1x8_I1

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO
	lxvd2x	vs10,	o32,	AO
	lxvd2x	vs11,	o48,	AO

	lxvdsx	vs28,	0,	BO

	addi		AO, AO, 64
	addi		BO, BO, 8


	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24
	xvmuldp			vs34,	vs2,	vs24
	xvmuldp			vs35,	vs3,	vs24

.endm

.macro KERNEL1x8_1

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO
	lxvd2x	vs10,	o32,	AO
	lxvd2x	vs11,	o48,	AO

	lxvdsx	vs28,	0,	BO

	addi		AO, AO, 64
	addi		BO, BO, 8


	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24
	xvmaddadp		vs34,	vs2,	vs24
	xvmaddadp		vs35,	vs3,	vs24

.endm

.macro KERNEL1x8_2

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 64
	addi		BO, BO, 8


	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28
	xvmaddadp		vs34,	vs10,	vs28
	xvmaddadp		vs35,	vs11,	vs28

.endm

.macro KERNEL1x8_E2


	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28
	xvmaddadp		vs34,	vs10,	vs28
	xvmaddadp		vs35,	vs11,	vs28

.endm

.macro KERNEL1x8_SUBI1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 64
	addi		BO, BO, 8


	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24
	xvmuldp			vs34,	vs2,	vs24
	xvmuldp			vs35,	vs3,	vs24

.endm

.macro KERNEL1x8_SUB1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 64
	addi		BO, BO, 8


	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24
	xvmaddadp		vs34,	vs2,	vs24
	xvmaddadp		vs35,	vs3,	vs24

.endm

.macro SAVE1x8

	mr		T1,	CO

#ifndef TRMMKERNEL
	lxvd2x		vs0,	0,	T1
	lxvd2x		vs1,	o16,	T1
	lxvd2x		vs2,	o32,	T1
	lxvd2x		vs3,	o48,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs0,	vs32,	alpha_r
	xvmaddadp	vs1,	vs33,	alpha_r
	xvmaddadp	vs2,	vs34,	alpha_r
	xvmaddadp	vs3,	vs35,	alpha_r
#else
	xvmuldp		vs0,	vs32,	alpha_r
	xvmuldp		vs1,	vs33,	alpha_r
	xvmuldp		vs2,	vs34,	alpha_r
	xvmuldp		vs3,	vs35,	alpha_r
#endif

	stxvd2x		vs0,	0,	T1
	stxvd2x		vs1,	o16,	T1
	stxvd2x		vs2,	o32,	T1
	stxvd2x		vs3,	o48,	T1

	addi		CO,	CO,	64

.endm

/*********************************************************************
* Macros for N=1, M=4                                                *
*********************************************************************/

.macro LOAD1x4_1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 32
	addi		BO, BO, 8

.endm

.macro KERNEL1x4_I1

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO

	lxvdsx	vs28,	0,	BO

	addi		AO, AO, 32
	addi		BO, BO, 8


	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24

.endm

.macro KERNEL1x4_1

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO

	lxvdsx	vs28,	0,	BO

	addi		AO, AO, 32
	addi		BO, BO, 8


	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24

.endm

.macro KERNEL1x4_2

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 32
	addi		BO, BO, 8


	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28

.endm

.macro KERNEL1x4_E2


	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28

.endm

.macro KERNEL1x4_SUBI1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 32
	addi		BO, BO, 8


	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24

.endm

.macro KERNEL1x4_SUB1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 32
	addi		BO, BO, 8


	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24

.endm

.macro SAVE1x4

	mr		T1,	CO

#ifndef TRMMKERNEL
	lxvd2x		vs0,	0,	T1
	lxvd2x		vs1,	o16,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs0,	vs32,	alpha_r
	xvmaddadp	vs1,	vs33,	alpha_r
#else
	xvmuldp		vs0,	vs32,	alpha_r
	xvmuldp		vs1,	vs33,	alpha_r
#endif

	stxvd2x		vs0,	0,	T1
	stxvd2x		vs1,	o16,	T1

	addi		CO,	CO,	32

.endm

/*********************************************************************
* Macros for N=1, M=2                                                *
*********************************************************************/

.macro LOAD1x2_1

	lxvd2x	vs0,	0,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 16
	addi		BO, BO, 8

.endm

.macro KERNEL1x2_I1

	lxvd2x	vs8,	0,	AO

	lxvdsx	vs28,	0,	BO

	addi		AO, AO, 16
	addi		BO, BO, 8


	xvmuldp			vs32,	vs0,	vs24

.endm

.macro KERNEL1x2_1

	lxvd2x	vs8,	0,	AO

	lxvdsx	vs28,	0,	BO

	addi		AO, AO, 16
	addi		BO, BO, 8


	xvmaddadp		vs32,	vs0,	vs24

.endm

.macro KERNEL1x2_2

	lxvd2x	vs0,	0,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 16
	addi		BO, BO, 8


	xvmaddadp		vs32,	vs8,	vs28

.endm

.macro KERNEL1x2_E2


	xvmaddadp		vs32,	vs8,	vs28

.endm

.macro KERNEL1x2_SUBI1

	lxvd2x	vs0,	0,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 16
	addi		BO, BO, 8


	xvmuldp			vs32,	vs0,	vs24

.endm

.macro KERNEL1x2_SUB1

	lxvd2x	vs0,	0,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 16
	addi		BO, BO, 8


	xvmaddadp		vs32,	vs0,	vs24

.endm

.macro SAVE1x2

	mr		T1,	CO

#ifndef TRMMKERNEL
	lxvd2x		vs0,	0,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs0,	vs32,	alpha_r
#else
	xvmuldp		vs0,	vs32,	alpha_r
#endif

	stxvd2x		vs0,	0,	T1

	addi		CO,	CO,	16

.endm

/*********************************************************************
* Macros for N=1, M=1                                                *
*********************************************************************/

.macro LOAD1x1_1

	lxsdx	vs0,	0,	AO

	lxsdx	vs24,	0,	BO

	addi		AO, AO, 8
	addi		BO, BO, 8

.endm

.macro KERNEL1x1_I1

	lxsdx	vs8,	0,	AO

	lxsdx	vs28,	0,	BO

	addi		AO, AO, 8
	addi		BO, BO, 8


	xsmuldp			vs32,	vs0,	vs24

.endm

.macro KERNEL1x1_1

	lxsdx	vs8,	0,	AO

	lxsdx	vs28,	0,	BO

	addi		AO, AO, 8
	addi		BO, BO, 8


	xsmaddadp		vs32,	vs0,	vs24

.endm

.macro KERNEL1x1_2

	lxsdx	vs0,	0,	AO

	lxsdx	vs24,	0,	BO

	addi		AO, AO, 8
	addi		BO, BO, 8


	xsmaddadp		vs32,	vs8,	vs28

.endm

.macro KERNEL1x1_E2


	xsmaddadp		vs32,	vs8,	vs28

.endm

.macro KERNEL1x1_SUBI1

	lxsdx	vs0,	0,	AO

	lxsdx	vs24,	0,	BO

	addi		AO, AO, 8
	addi		BO, BO, 8


	xsmuldp			vs32,	vs0,	vs24

.endm

.macro KERNEL1x1_SUB1

	lxsdx	vs0,	0,	AO

	lxsdx	vs24,	0,	BO

	addi		AO, AO, 8
	addi		BO, BO, 8


	xsmaddadp		vs32,	vs0,	vs24

.endm

.macro SAVE1x1

	mr		T1,	CO

#ifndef TRMMKERNEL
	lxsdx		vs0,	0,	T1
#endif

#ifndef TRMMKERNEL
	xsmaddadp	vs0,	vs32,	alpha_r
#else
	xsmuldp		vs0,	vs32,	alpha_r
#endif

	stxsdx		vs0,	0,	T1

	addi		CO,	CO,	8

.endm




/****************************TRMM POINTER REFRESH MACROSES*************************/

.macro SHIFT_REG  REG1,REG2,SHIFT_VAL
		.if \SHIFT_VAL==16 
			slwi		\REG1,	\REG2,	7			
		.elseif \SHIFT_VAL==8  
			slwi		\REG1,	\REG2,	6			 
		.elseif \SHIFT_VAL==4
			slwi		\REG1,	\REG2,	5			  
		.elseif \SHIFT_VAL==2
			slwi		\REG1,	\REG2,	4			 
		.elseif \SHIFT_VAL==1
			slwi		\REG1,	\REG2,	3			 
		.endif
.endm

/*
//#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
// 		ptrbb = bb;
// #else
// 		ptrba += off*16;
// 		ptrbb = bb + off*2;
// #endif
*/
.macro REFRESH_POINTERS  PTR_A,PTR_B,OFF_VAL,B_VAL,C_A,C_B
    #if (defined(LEFT) &&  defined(TRANSA)) ||  (!defined(LEFT) && !defined(TRANSA))
        /* ptrbb = bb;*/
        mr \PTR_B,\B_VAL     /* refresh BPOINT */

    #else
		    /*
        // ptrba  =ptrba+ off*C_A;
        // ptrbb = bb + off*C_B; 
				*/
		SHIFT_REG T4,\OFF_VAL,\C_B		/* Number of values in B shifted  */
		SHIFT_REG T2,\OFF_VAL,\C_A		/* Number of values in A shifted  */
		add		\PTR_B,	\B_VAL ,	T4				/* Add values to BO */
		add		\PTR_A,	\PTR_A,	T2				/* Add values to AO  */
    #endif 
.endm


/*
// #if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
// 		temp = bk-off;
// #elif defined(LEFT)
// 		temp = off+16;	// number of values in A
// #else
// 		temp = off+2;	// number of values in B
// #endif
*/
.macro REFRESH_TEMP_BK TEMP_BK,BK_VAL,OFF_VAL,INCR_A,INCR_B
    #if (defined(LEFT) && !defined(TRANSA)) ||  (!defined(LEFT) && defined(TRANSA))
                            /* temp = bk-off;*/
           sub \TEMP_BK,\BK_VAL,\OFF_VAL

    #elif defined(LEFT)
                            /* temp = off+INCR_A;	// number of values in A */
           addi \TEMP_BK, \OFF_VAL, \INCR_A
    #else
                            /* temp = off+INCR_B	// number of values in B*/
           addi \TEMP_BK,\OFF_VAL, \INCR_B
    #endif

.endm
/*
// #if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
// 		temp = bk - off;
// #ifdef LEFT
// 		temp -= 16; // number of values in A
// #else
// 		temp -= 2; // number of values in B
// #endif
// 		ptrba += temp*16;
// 		ptrbb += temp*2;
// #endif

// #ifdef LEFT
// 		off += 16; // number of values in A
// #endif
*/
 

.macro REFRESH_AFTER_SAVE TEMP_BK,BK_VAL,OFF_VAL,PTR_B,PTR_A,C_A,C_B

    #if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
                    /*temp = bk - off;*/
                sub \TEMP_BK,\BK_VAL,\OFF_VAL
    #ifdef LEFT
                    /*temp -= 8; // number of values in A*/
                addi \TEMP_BK,\TEMP_BK,-\C_A
    #else
                    /*temp -= 4; // number of values in B*/
                addi \TEMP_BK,\TEMP_BK,-\C_B 
    #endif
                    /*ptrba += temp*C_A;
                    ptrbb += temp*C_B;*/ 
                SHIFT_REG T4,\TEMP_BK,\C_A
								SHIFT_REG T2,\TEMP_BK,\C_B
                add \PTR_A, \PTR_A,T4/*ptrba+temp*C_A*/ 
								add \PTR_B, \PTR_B,T2 

    #endif

    #ifdef LEFT
                    /*off += 8; // number of values in A*/
                 addi \OFF_VAL,\OFF_VAL,\C_A
    #endif
.endm