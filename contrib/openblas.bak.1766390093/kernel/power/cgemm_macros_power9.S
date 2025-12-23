
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
* Abdelrauf(quickwritereader@gmail.com)
* BLASTEST 	     	: OK
*  CTEST		    	: OK
*  TEST			      : OK
*	 LAPACK-TEST		: OK
**************************************************************************************/
#define unit_size 8
#define DISP32(ind,disp) (ind*unit_size*32+disp)
#define DISP16(ind,disp) (ind*unit_size*16+disp)
#define DISP8(ind,disp) (ind*unit_size*8+disp)
#define DISP4(ind,disp) (ind*unit_size*4+disp)
#define DISP2(ind,disp) (ind*unit_size*2+disp)
#define DISP1(ind,disp) (ind*unit_size+disp)
#define DISPX(disp)  (disp)

.macro  AGGREGATE_REALS_IMAGES  VSINR_OUT1,VSINR,VSINI_OUT2,VSINI
#if	defined(NN) || defined(NT) || defined(TN) || defined(TT) 
	xvsubsp  \VSINR_OUT1,\VSINR_OUT1,\VSINR
	xvaddsp  \VSINI_OUT2,\VSINI_OUT2,\VSINI  
#elif  defined(CN) || defined(CT) || defined(RN) || defined(RT) 
	xvaddsp  \VSINR_OUT1,\VSINR_OUT1,\VSINR
	xvsubsp  \VSINI_OUT2,\VSINI_OUT2,\VSINI 
#elif  defined(NC) || defined(TC) || defined(NR) || defined(TR) 
	xvaddsp  \VSINR_OUT1,\VSINR_OUT1,\VSINR
	xvsubsp  \VSINI_OUT2,\VSINI,\VSINI_OUT2  
#else	// CC || CR || RC || RR 
    /*we will assume {-alpha_r,-alpha_i} for this case */
    /*i1i2-r1r2 so we will negate alpha real instead to fix sign*/
	xvsubsp  \VSINR_OUT1,\VSINR,\VSINR_OUT1
    /*we will negate alpha image   instead to fix sign*/
	xvaddsp  \VSINI_OUT2,\VSINI_OUT2,\VSINI 
#endif
.endm


.macro  AGGREGATE_REALS_IMAGES_A_PERMUTE  VSINR_OUT1,VSINR,VSINI_OUT2,VSINI
#if	defined(NN) || defined(NT) || defined(TN) || defined(TT) 
	xvsubsp  \VSINR_OUT1,\VSINR_OUT1,\VSINR
	xvaddsp  \VSINI_OUT2,\VSINI_OUT2,\VSINI  
#elif  defined(CN) || defined(CT) || defined(RN) || defined(RT) 
	xvaddsp  \VSINR_OUT1,\VSINR_OUT1,\VSINR
	xvsubsp  \VSINI_OUT2,\VSINI,\VSINI_OUT2  
#elif  defined(NC) || defined(TC) || defined(NR) || defined(TR) 
	xvaddsp  \VSINR_OUT1,\VSINR_OUT1,\VSINR
	xvsubsp  \VSINI_OUT2,\VSINI_OUT2,\VSINI 
#else	// CC || CR || RC || RR 
    /*we will assume {-alpha_r,-alpha_i} for this case */
    /*i1i2-r1r2 so we will negate alpha real instead to fix sign*/
	xvsubsp  \VSINR_OUT1,\VSINR,\VSINR_OUT1
    /*we will negate alpha image   instead to fix sign*/
	xvaddsp  \VSINI_OUT2,\VSINI_OUT2,\VSINI 
#endif
.endm
 
/* {i0,i1} * {alpha_i,alpha_i} [- VSOUT1] ;[VSOUT2 +] {r0,r1}*{alpha_i,alpha_i} */

.macro MULT_APLHA_PART1  VSINRR,VSINII,VSOUT1,VSOUT2
	xvmulsp \VSOUT1,\VSINII, alpha_i 
	xvmulsp  \VSOUT2,\VSINRR, alpha_i
.endm

/*   {r0,r1} * {alpha_r,alpha_r} -  VSOUT1 ;VSOUT2 + {i0,i1} * {alpha_r,alpha_r} */

.macro MULT_APLHA_PART2  VSINRR,VSINII,VSOUT1,VSOUT2 
	xvmsubasp  \VSOUT1,\VSINRR, alpha_r
	xvmaddasp \VSOUT2,\VSINII, alpha_r
.endm

/*                                             macros for N=4 and M=8
**********************************************************************************************/

.macro Zero4x8
	xxlxor	vs32,	vs32,	vs32
	xxlxor	vs33,	vs33,	vs33
	xxlxor	vs34,	vs34,	vs34
	xxlxor	vs35,	vs35,	vs35
	xxlxor	vs36,	vs36,	vs36
	xxlxor	vs37,	vs37,	vs37
	xxlxor	vs38,	vs38,	vs38
	xxlxor	vs39,	vs39,	vs39
	xxlxor	vs40,	vs40,	vs40
	xxlxor	vs41,	vs41,	vs41
	xxlxor	vs42,	vs42,	vs42
	xxlxor	vs43,	vs43,	vs43
	xxlxor	vs44,	vs44,	vs44
	xxlxor	vs45,	vs45,	vs45
	xxlxor	vs46,	vs46,	vs46
	xxlxor	vs47,	vs47,	vs47
	xxlxor	vs48,	vs48,	vs48
	xxlxor	vs49,	vs49,	vs49
	xxlxor	vs50,	vs50,	vs50
	xxlxor	vs51,	vs51,	vs51
	xxlxor	vs52,	vs52,	vs52
	xxlxor	vs53,	vs53,	vs53
	xxlxor	vs54,	vs54,	vs54
	xxlxor	vs55,	vs55,	vs55
	xxlxor	vs56,	vs56,	vs56
	xxlxor	vs57,	vs57,	vs57
	xxlxor	vs58,	vs58,	vs58
	xxlxor	vs59,	vs59,	vs59
	xxlxor	vs60,	vs60,	vs60
	xxlxor	vs61,	vs61,	vs61
	xxlxor	vs62,	vs62,	vs62
	xxlxor	vs63,	vs63,	vs63
.endm


.macro LOAD4x8   
	LOAD4x8O 0,0 
.endm


.macro LOAD4x8O  OffsetA,OffsetB
	lxv	vs24,	(\OffsetB+0)(BO)
	lxv	vs28,	(\OffsetB+16)(BO)
	xxperm  	vs26,	vs24,		permute_mask
	xxperm  	vs30,	vs28,		permute_mask	  
	lxv	vs0,	(\OffsetA+0)(AO)
	lxv	vs1,	(\OffsetA+16)(AO)
	xxpermdi	vs25,	vs24,	vs24,2	   
	xxpermdi	vs29,	vs28,	vs28,2	  
	lxv	vs2,	(\OffsetA+32)(AO)
	lxv	vs3,	(\OffsetA+48)(AO) 
	xxpermdi	vs27,	vs26,	vs26,2	
	xxpermdi	vs31,	vs30,	vs30,2	 	
.endm


.macro END4x8_NORMAL
	END4x8 AO,BO,64,32
.endm


.macro END4x8_WITHOUT_ADD
	END4x8 AO,BO,0,0
.endm


.macro END4x8	AREG, BREG, OffsetA, OffsetB
.if \OffsetB != 0
	addi	\BREG, \BREG, \OffsetB
.endif

.if \OffsetA != 0
	addi	\AREG, \AREG, \OffsetA
.endif

    xvmaddasp       vs32, vs0,vs24
    xvmaddasp       vs33, vs1,vs24
    xvmaddasp       vs34, vs2,vs24  
    xvmaddasp       vs35, vs3,vs24  
    xvmaddasp       vs36, vs0,vs25
    xvmaddasp       vs37, vs1,vs25
    xvmaddasp       vs38, vs2,vs25  
    xvmaddasp       vs39, vs3,vs25 
    xvmaddasp       vs40, vs0,vs26
    xvmaddasp       vs41, vs1,vs26
    xvmaddasp       vs42, vs2,vs26  
    xvmaddasp       vs43, vs3,vs26
    xvmaddasp       vs44, vs0,vs27
    xvmaddasp       vs45, vs1,vs27
    xvmaddasp       vs46, vs2,vs27  
    xvmaddasp       vs47, vs3,vs27
    xvmaddasp       vs48, vs0,vs28
    xvmaddasp       vs49, vs1,vs28
    xvmaddasp       vs50, vs2,vs28  
    xvmaddasp       vs51, vs3,vs28  
    xvmaddasp       vs52, vs0,vs29
    xvmaddasp       vs53, vs1,vs29
    xvmaddasp       vs54, vs2,vs29  
    xvmaddasp       vs55, vs3,vs29
    xvmaddasp       vs56, vs0,vs30
    xvmaddasp       vs57, vs1,vs30
    xvmaddasp       vs58, vs2,vs30  
    xvmaddasp       vs59, vs3,vs30
    xvmaddasp       vs60, vs0,vs31
    xvmaddasp       vs61, vs1,vs31
    xvmaddasp       vs62, vs2,vs31  
    xvmaddasp       vs63, vs3,vs31 
.endm


.macro LOAD4x8_2
    LOAD4x8_2O 0,0
.endm
	

.macro LOAD4x8_2O  OffsetA,OffsetB
  lxv	vs8,	(\OffsetB)(BO)
  lxv	vs12,	(16+\OffsetB)(BO)
  lxv	vs24,	(32+\OffsetB)(BO)
  lxv	vs28,	(32+16+\OffsetB)(BO)
  lxv	vs4,	(0+\OffsetA)(AO)
  lxv	vs5,	(16+\OffsetA)(AO)
  xxperm  	vs10,	vs8,		permute_mask
  xxperm  	vs14,	vs12,		permute_mask	
  lxv	vs6,	(32+\OffsetA)(AO)
  lxv	vs7,	(48+\OffsetA)(AO) 
  xxpermdi	vs9,	vs8,	 vs8,2	 
  xxpermdi	vs13,	vs12,	vs12,2	 
  lxv	vs0,	(64+\OffsetA)(AO)
  lxv	vs1,	(64+16+\OffsetA)(AO) 
  xxpermdi	vs11,	vs10,	vs10,2	
  xxpermdi	vs15,	vs14,	vs14,2	
  lxv	vs2,	(64+32+\OffsetA)(AO)
  lxv	vs3,	(64+48+\OffsetA)(AO)
  xxperm  	vs26,	vs24,	permute_mask
  xxperm  	vs30,	vs28,	permute_mask	
  xxpermdi	vs25,	vs24,	vs24,2 
  xxpermdi	vs29,	vs28,	vs28,2	      
  xxpermdi	vs27,	vs26,	vs26,2	
  xxpermdi	vs31,	vs30,	vs30,2	 
.endm
	

.macro END4x8_2	  
  /*for load2 offset will be 128 and 64*/
   KERNEL4x8_2	AO,BO,	128,64,0 ,1,1 
.endm


.macro KERNEL4x8_E2	OffsetA,OffsetB, Index,IsLast 
  KERNEL4x8_2	AO,BO,	\OffsetA,\OffsetB, \Index,\IsLast ,1 
.endm


.macro KERNEL4x8_L2	OffsetA,OffsetB, Index,IsLast
  KERNEL4x8_2	AO,BO,	\OffsetA,\OffsetB, \Index,\IsLast ,0 
.endm


.macro KERNEL4x8_2	AREG,BREG,	OffsetA,OffsetB, Index,IsLast ,Complete
  xvmaddasp		vs32, vs4,vs8
  xvmaddasp		vs33, vs5,vs8
  xvmaddasp		vs48, vs4,vs12
  xvmaddasp		vs49, vs5,vs12
  xvmaddasp		vs40, vs4,vs10
  xvmaddasp		vs41, vs5,vs10
  xvmaddasp		vs56, vs4,vs14
  xvmaddasp		vs57, vs5,vs14
  xvmaddasp		vs36, vs4,vs9
  xvmaddasp		vs37, vs5,vs9
  xvmaddasp		vs52, vs4,vs13
  xvmaddasp		vs53, vs5,vs13
  xvmaddasp		vs44, vs4,vs11
  xvmaddasp		vs45, vs5,vs11
  xvmaddasp		vs60, vs4,vs15
  xvmaddasp		vs61, vs5,vs15
.if \Complete==0	
   lxv	vs4,	DISP16(\Index,0+\OffsetA)(\AREG)
   lxv	vs5,	DISP16(\Index,16+\OffsetA)(\AREG)
.endif

  xvmaddasp		vs34, vs6,vs8	
  xvmaddasp		vs35, vs7,vs8	
  xvmaddasp		vs50, vs6,vs12
  xvmaddasp		vs51, vs7,vs12
.if \Complete==0  
  lxv vs8,  DISP8(\Index,\OffsetB)(\BREG)
  lxv vs12, DISP8(\Index,16+\OffsetB)(\BREG)
.endif    
  xvmaddasp		vs42, vs6,vs10
  xvmaddasp		vs43, vs7,vs10
  xvmaddasp		vs58, vs6,vs14
  xvmaddasp		vs59, vs7,vs14
.if \Complete==0  
  xxperm    vs10, vs8,    permute_mask
  xxperm    vs14, vs12,   permute_mask    
.endif    
  xvmaddasp		vs38, vs6,vs9	
  xvmaddasp		vs39, vs7,vs9	
  xvmaddasp   vs54, vs6,vs13
  xvmaddasp   vs55, vs7,vs13
.if \Complete==0
  xxpermdi  vs9,  vs8,   vs8,2   
  xxpermdi  vs13, vs12, vs12,2   
.endif    
  xvmaddasp		vs46, vs6,vs11
  xvmaddasp		vs47, vs7,vs11
  xvmaddasp		vs62, vs6,vs15
  xvmaddasp		vs63, vs7,vs15
.if \Complete==0
  xxpermdi  vs11, vs10, vs10,2  
  xxpermdi  vs15, vs14, vs14,2  
.endif  
.if \Complete==0
   lxv	vs6,	DISP16(\Index,32+\OffsetA)(\AREG)
   lxv	vs7,	DISP16(\Index,48+\OffsetA)(\AREG) 
.endif 
  xvmaddasp		vs32, vs0,vs24
  xvmaddasp		vs33, vs1,vs24
  xvmaddasp		vs48, vs0,vs28
  xvmaddasp		vs49, vs1,vs28
  xvmaddasp		vs40, vs0,vs26
  xvmaddasp		vs41, vs1,vs26
  xvmaddasp		vs56, vs0,vs30
  xvmaddasp		vs57, vs1,vs30
  xvmaddasp		vs36, vs0,vs25
  xvmaddasp		vs37, vs1,vs25
  xvmaddasp		vs52, vs0,vs29
  xvmaddasp		vs53, vs1,vs29
  xvmaddasp		vs44, vs0,vs27
  xvmaddasp		vs45, vs1,vs27
  xvmaddasp		vs60, vs0,vs31
  xvmaddasp		vs61, vs1,vs31 
.if \Complete==0
  lxv	vs0,	DISP16(\Index,64+\OffsetA)(\AREG)
  lxv	vs1,	DISP16(\Index,64+16+\OffsetA)(\AREG) 
.endif

  xvmaddasp		vs34, vs2,vs24
  xvmaddasp		vs35, vs3,vs24	  
  xvmaddasp		vs50, vs2,vs28
  xvmaddasp		vs51, vs3,vs28
.if \Complete==0
  lxv vs24, DISP8(\Index,32+\OffsetB)(\BREG)
  lxv vs28, DISP8(\Index,32+16+\OffsetB)(\BREG)
.endif  
  xvmaddasp		vs42, vs2,vs26
  xvmaddasp		vs43, vs3,vs26
  xvmaddasp		vs58, vs2,vs30
  xvmaddasp		vs59, vs3,vs30
.if \Complete==0
  xxperm    vs26, vs24, permute_mask
  xxperm    vs30, vs28, permute_mask  
.endif  
  xvmaddasp		vs38, vs2,vs25
  xvmaddasp		vs39, vs3,vs25
  xvmaddasp		vs54, vs2,vs29
  xvmaddasp		vs55, vs3,vs29
.if \Complete==0
  xxpermdi  vs25, vs24, vs24,2 
  xxpermdi  vs29, vs28, vs28,2    
.endif  
  xvmaddasp		vs46, vs2,vs27
  xvmaddasp		vs47, vs3,vs27
  xvmaddasp		vs62, vs2,vs31	
  xvmaddasp		vs63, vs3,vs31
.if \Complete==0
  xxpermdi  vs27, vs26, vs26,2  
  xxpermdi  vs31, vs30, vs30,2   
.endif

.if \Complete==0
  lxv	vs2,	DISP16(\Index,64+32+\OffsetA)(\AREG)
  lxv	vs3,	DISP16(\Index,64+48+\OffsetA)(\AREG)
.endif

.if \IsLast==1	
.if \Complete==1
	addi		\BREG, \BREG,  DISP8(\Index,\OffsetB)
  addi    \AREG, \AREG, DISP16(\Index,\OffsetA)  
.else
	addi		\BREG, \BREG,  DISP8(\Index,64)
  addi    \AREG, \AREG, DISP16(\Index,128)  
.endif

.endif   
.endm


.macro KERNEL4x8
  LOAD4x8
  END4x8  AO, BO, 64,32
.endm


.macro SAVE4x8
  add T4, LDC,LDC
	add	T1, CO ,LDC  
#ifndef TRMMKERNEL  
  lxv vs24 , 0(CO)
  lxv vs25 , 16(CO)
#endif
  xxperm  vs0,vs32,permute_mask
  xxperm  vs4,vs40,permute_mask
#ifndef TRMMKERNEL  
  lxv vs26 , 32(CO)
  lxv vs27 , 48(CO)
#endif  
  xxperm  vs1,vs33,permute_mask
  xxperm  vs5,vs41,permute_mask
#ifndef TRMMKERNEL  
  lxv vs28 , 0(T1)
  lxv vs29 , 16(T1)
#endif  
  xxperm  vs2,vs34,permute_mask
  xxperm  vs6,vs42,permute_mask
#ifndef TRMMKERNEL  
  lxv vs30 , 32(T1)
  lxv vs31 , 48(T1)
#endif 
  xxperm  vs3,vs35,permute_mask
  xxperm  vs7,vs43,permute_mask 
  add T2,CO,T4
  add T3,T1,T4  
  AGGREGATE_REALS_IMAGES vs32,vs0,vs40,vs4
  xxperm  vs8,vs36,permute_mask
  xxperm  vs12,vs44,permute_mask
  AGGREGATE_REALS_IMAGES vs33,vs1,vs41,vs5
  xxperm  vs9,vs37,permute_mask
  xxperm  vs13,vs45,permute_mask
  AGGREGATE_REALS_IMAGES vs34,vs2,vs42,vs6
  xxperm  vs10,vs38,permute_mask
  xxperm  vs14,vs46,permute_mask
  AGGREGATE_REALS_IMAGES vs35,vs3,vs43,vs7 
  xxperm  vs11,vs39,permute_mask
  xxperm  vs15,vs47,permute_mask 
  AGGREGATE_REALS_IMAGES vs36,vs8,vs44,vs12
  xxperm  vs0,vs48,permute_mask
  xxperm  vs4,vs56,permute_mask
  AGGREGATE_REALS_IMAGES vs37,vs9,vs45,vs13
  xxperm  vs1,vs49,permute_mask
  xxperm  vs5,vs57,permute_mask
  AGGREGATE_REALS_IMAGES vs38,vs10,vs46,vs14
  xxperm  vs2,vs50,permute_mask
  xxperm  vs6,vs58,permute_mask
  AGGREGATE_REALS_IMAGES vs39,vs11,vs47,vs15 
  xxperm  vs3,vs51,permute_mask
  xxperm  vs7,vs59,permute_mask 
  AGGREGATE_REALS_IMAGES vs48,vs0,vs56,vs4
  xxperm  vs8,vs52,permute_mask
  xxperm  vs12,vs60,permute_mask
  AGGREGATE_REALS_IMAGES vs49,vs1,vs57,vs5
  xxperm  vs9,vs53,permute_mask
  xxperm  vs13,vs61,permute_mask
  AGGREGATE_REALS_IMAGES vs50,vs2,vs58,vs6
  xxperm  vs10,vs54,permute_mask
  xxperm  vs14,vs62,permute_mask
  AGGREGATE_REALS_IMAGES vs51,vs3,vs59,vs7 
  xxperm  vs11,vs55,permute_mask
  xxperm  vs15,vs63,permute_mask 
  AGGREGATE_REALS_IMAGES vs52,vs8,vs60,vs12
  AGGREGATE_REALS_IMAGES vs53,vs9,vs61,vs13
  /*VSINRR,VSINII,VSOUT1,VSOUT2*/
  MULT_APLHA_PART1    vs32,vs40,vs0,vs1
  AGGREGATE_REALS_IMAGES vs54,vs10,vs62,vs14
  MULT_APLHA_PART1    vs33,vs41,vs2,vs3  
  AGGREGATE_REALS_IMAGES vs55,vs11,vs63,vs15  
  MULT_APLHA_PART1    vs34,vs42,vs4,vs5
  MULT_APLHA_PART1    vs35,vs43,vs6,vs7  
  MULT_APLHA_PART2    vs32,vs40,vs0,vs1    
  MULT_APLHA_PART2    vs33,vs41,vs2,vs3   
  MULT_APLHA_PART2    vs34,vs42,vs4,vs5
  MULT_APLHA_PART2    vs35,vs43,vs6,vs7  
 #ifndef TRMMKERNEL  
  lxv vs32 , 0(T2)
  lxv vs40 , 16(T2)
#endif 
  MULT_APLHA_PART1    vs36,vs44,vs8,vs9
  MULT_APLHA_PART1    vs37,vs45,vs10,vs11
#ifndef TRMMKERNEL  
  lxv vs33 , 32(T2)
  lxv vs41 , 48(T2)
#endif  
  MULT_APLHA_PART1    vs38,vs46,vs12,vs13
  MULT_APLHA_PART1    vs39,vs47,vs14,vs15
#ifndef TRMMKERNEL  
  lxv vs34 , 0(T3)
  lxv vs42 , 16(T3)
#endif  
  MULT_APLHA_PART2    vs36,vs44,vs8,vs9
  MULT_APLHA_PART2    vs37,vs45,vs10,vs11
#ifndef TRMMKERNEL  
  lxv vs35 , 32(T3)
  lxv vs43 , 48(T3)
#endif    
  MULT_APLHA_PART2    vs38,vs46,vs12,vs13
  MULT_APLHA_PART2    vs39,vs47,vs14,vs15
/* reconstruct r,i pairs*/
  xxperm  vs0,vs1, save_permute_1
  xxperm  vs2,vs3, save_permute_1
  xxperm  vs4,vs5, save_permute_1
  xxperm  vs6,vs7, save_permute_1
  xxperm  vs8,vs9, save_permute_1
  xxperm  vs10,vs11, save_permute_1
  xxperm  vs12,vs13, save_permute_1
  xxperm  vs14,vs15, save_permute_1
#ifndef TRMMKERNEL
  /* add */
  xxpermdi vs1,vs8,vs0,2
  xxpermdi vs3,vs10,vs2,2
  xxpermdi vs5,vs12,vs4,2
  xxpermdi vs7,vs14,vs6,2
  xxpermdi vs9,vs0,vs8,2
  xxpermdi vs11,vs2,vs10,2  
  xvaddsp vs24,vs24,vs1
  xvaddsp vs25,vs25,vs3
  xxpermdi vs13,vs4,vs12,2  
  xxpermdi vs15,vs6,vs14,2
  xvaddsp vs26,vs26,vs5
  xvaddsp  vs27,vs27,vs7
  xvaddsp vs28,vs28,vs9
  xvaddsp vs29,vs29,vs11 
  xvaddsp vs30,vs30,vs13
  xvaddsp vs31,vs31,vs15  
#else
  xxpermdi vs24,vs8,vs0,2
  xxpermdi vs25,vs10,vs2,2
  xxpermdi vs26,vs12,vs4,2
  xxpermdi vs27,vs14,vs6,2 
  xxpermdi vs28,vs0,vs8,2
  xxpermdi vs29,vs2,vs10,2  
  xxpermdi vs30,vs4,vs12,2  
  xxpermdi vs31,vs6,vs14,2
#endif
  stxv vs24 , 0(CO)
  stxv vs25 , 16(CO)
  MULT_APLHA_PART1    vs48,vs56,vs0,vs1
  MULT_APLHA_PART1    vs49,vs57,vs2,vs3
  stxv vs26 , 32(CO)
  stxv vs27 , 48(CO)
  MULT_APLHA_PART1    vs50,vs58,vs4,vs5
  MULT_APLHA_PART1    vs51,vs59,vs6,vs7
  stxv vs28 , 0(T1)
  stxv vs29 , 16(T1)
  MULT_APLHA_PART2    vs48,vs56,vs0,vs1
  MULT_APLHA_PART2    vs49,vs57,vs2,vs3
  stxv vs30 , 32(T1)
  stxv vs31 , 48(T1)  
  MULT_APLHA_PART2    vs50,vs58,vs4,vs5
  MULT_APLHA_PART2    vs51,vs59,vs6,vs7
  MULT_APLHA_PART1    vs52,vs60,vs8,vs9
  MULT_APLHA_PART1    vs53,vs61,vs10,vs11
  xxperm  vs0,vs1, save_permute_1
  xxperm  vs2,vs3, save_permute_1
  MULT_APLHA_PART1    vs54,vs62,vs12,vs13
  MULT_APLHA_PART1    vs55,vs63,vs14,vs15
  xxperm  vs4,vs5, save_permute_1
  xxperm  vs6,vs7, save_permute_1
  MULT_APLHA_PART2    vs52,vs60,vs8,vs9
  MULT_APLHA_PART2    vs53,vs61,vs10,vs11
  xxperm  vs8,vs9, save_permute_1
  xxperm  vs10,vs11, save_permute_1
  MULT_APLHA_PART2    vs54,vs62,vs12,vs13
  MULT_APLHA_PART2    vs55,vs63,vs14,vs15
  xxperm  vs12,vs13, save_permute_1
  xxperm  vs14,vs15, save_permute_1
#ifndef TRMMKERNEL
  /* add */
  xxpermdi vs1,vs8,vs0,2
  xxpermdi vs3,vs10,vs2,2
  xxpermdi vs5,vs12,vs4,2
  xxpermdi vs7,vs14,vs6,2
  xxpermdi vs9,vs0,vs8,2
  xxpermdi vs11,vs2,vs10,2  
  xvaddsp vs32,vs32,vs1
  xvaddsp vs40,vs40,vs3
  xxpermdi vs13,vs4,vs12,2  
  xxpermdi vs15,vs6,vs14,2
  xvaddsp vs33,vs33,vs5
  xvaddsp  vs41,vs41,vs7
  xvaddsp vs34,vs34,vs9
  xvaddsp vs42,vs42,vs11 
  xvaddsp vs35,vs35,vs13
  xvaddsp vs43,vs43,vs15  
#else
  xxpermdi vs32,vs8,vs0,2
  xxpermdi vs40,vs10,vs2,2
  xxpermdi vs33,vs12,vs4,2
  xxpermdi vs41,vs14,vs6,2 
  xxpermdi vs34,vs0,vs8,2
  xxpermdi vs42,vs2,vs10,2  
  xxpermdi vs35,vs4,vs12,2  
  xxpermdi vs43,vs6,vs14,2
#endif
  stxv vs32 , 0(T2)
  stxv vs40 , 16(T2)
  stxv vs33 , 32(T2)
  stxv vs41 , 48(T2)
  stxv vs34 , 0(T3)
  stxv vs42 , 16(T3)
  stxv vs35 , 32(T3)
  stxv vs43 , 48(T3)  
	addi	CO, CO, 64
.endm

/*                                             macros for N=4 and M=4
**********************************************************************************************/

.macro Zero4x4
	xxlxor	vs32,	vs32,	vs32
	xxlxor	vs33,	vs33,	vs33
	xxlxor	vs36,	vs36,	vs36
	xxlxor	vs37,	vs37,	vs37
	xxlxor	vs40,	vs40,	vs40
	xxlxor	vs41,	vs41,	vs41
	xxlxor	vs44,	vs44,	vs44
	xxlxor	vs45,	vs45,	vs45
	xxlxor	vs48,	vs48,	vs48
	xxlxor	vs49,	vs49,	vs49
	xxlxor	vs52,	vs52,	vs52
	xxlxor	vs53,	vs53,	vs53
	xxlxor	vs56,	vs56,	vs56
	xxlxor	vs57,	vs57,	vs57
	xxlxor	vs60,	vs60,	vs60
	xxlxor	vs61,	vs61,	vs61
.endm


.macro LOAD4x4   
	LOAD4x4O 0,0 
.endm


.macro LOAD4x4O  OffsetA,OffsetB
	lxv	vs24,	(\OffsetB+0)(BO)
	lxv	vs28,	(\OffsetB+16)(BO)
	xxperm  	vs26,	vs24,		permute_mask
	xxperm  	vs30,	vs28,		permute_mask	  
	lxv	vs0,	(\OffsetA+0)(AO)
	lxv	vs1,	(\OffsetA+16)(AO)
	xxpermdi	vs25,	vs24,	vs24,2	   
	xxpermdi	vs29,	vs28,	vs28,2	  
	xxpermdi	vs27,	vs26,	vs26,2	
	xxpermdi	vs31,	vs30,	vs30,2	 	
.endm


.macro END4x4_NORMAL
	END4x4 AO,BO,32,32
.endm


.macro END4x4_WITHOUT_ADD
	END4x4 AO,BO,0,0
.endm


.macro END4x4	AREG, BREG, OffsetA, OffsetB
.if \OffsetB != 0
	addi	\BREG, \BREG, \OffsetB
.endif

.if \OffsetA != 0
	addi	\AREG, \AREG, \OffsetA
.endif

    xvmaddasp       vs32, vs0,vs24
    xvmaddasp       vs33, vs1,vs24
    xvmaddasp       vs36, vs0,vs25
    xvmaddasp       vs37, vs1,vs25
    xvmaddasp       vs40, vs0,vs26
    xvmaddasp       vs41, vs1,vs26
    xvmaddasp       vs44, vs0,vs27
    xvmaddasp       vs45, vs1,vs27
    xvmaddasp       vs48, vs0,vs28
    xvmaddasp       vs49, vs1,vs28
    xvmaddasp       vs52, vs0,vs29
    xvmaddasp       vs53, vs1,vs29
    xvmaddasp       vs56, vs0,vs30
    xvmaddasp       vs57, vs1,vs30
    xvmaddasp       vs60, vs0,vs31
    xvmaddasp       vs61, vs1,vs31
.endm


.macro LOAD4x4_2
    LOAD4x4_2O 0,0
.endm
	

.macro LOAD4x4_2O  OffsetA,OffsetB
  lxv	vs8,	(\OffsetB)(BO)
  lxv	vs12,	(16+\OffsetB)(BO)
  lxv	vs24,	(32+\OffsetB)(BO)
  lxv	vs28,	(32+16+\OffsetB)(BO)
  lxv	vs4,	(0+\OffsetA)(AO)
  lxv	vs5,	(16+\OffsetA)(AO)
  xxperm  	vs10,	vs8,		permute_mask
  xxperm  	vs14,	vs12,		permute_mask	
  xxpermdi	vs9,	vs8,	 vs8,2	 
  xxpermdi	vs13,	vs12,	vs12,2	 
  lxv	vs0,	(32+\OffsetA)(AO)
  lxv	vs1,	(32+16+\OffsetA)(AO) 
  xxpermdi	vs11,	vs10,	vs10,2	
  xxpermdi	vs15,	vs14,	vs14,2	
  xxperm  	vs26,	vs24,	permute_mask
  xxperm  	vs30,	vs28,	permute_mask	
  xxpermdi	vs25,	vs24,	vs24,2 
  xxpermdi	vs29,	vs28,	vs28,2	      
  xxpermdi	vs27,	vs26,	vs26,2	
  xxpermdi	vs31,	vs30,	vs30,2	 
.endm


.macro END4x4_2	  
  /*for load2 offset will be 64 and 64*/
   KERNEL4x4_2	AO,BO,	64,64,0 ,1,1 
.endm


.macro KERNEL4x4_E2	OffsetA,OffsetB, Index,IsLast 
  KERNEL4x4_2	AO,BO,	\OffsetA,\OffsetB, \Index,\IsLast ,1 
.endm


.macro KERNEL4x4_L2	OffsetA,OffsetB, Index,IsLast
  KERNEL4x4_2	AO,BO,	\OffsetA,\OffsetB, \Index,\IsLast ,0 
.endm


.macro KERNEL4x4_2	AREG,BREG,	OffsetA,OffsetB, Index,IsLast ,Complete
  xvmaddasp		vs32, vs4,vs8
  xvmaddasp		vs33, vs5,vs8
  xvmaddasp		vs48, vs4,vs12
  xvmaddasp		vs49, vs5,vs12
  xvmaddasp		vs40, vs4,vs10
  xvmaddasp		vs41, vs5,vs10
  xvmaddasp		vs56, vs4,vs14
  xvmaddasp		vs57, vs5,vs14
.if \Complete==0  
  lxv vs8,  DISP8(\Index,\OffsetB)(\BREG)
  lxv vs12, DISP8(\Index,16+\OffsetB)(\BREG)
.endif  
  xvmaddasp		vs36, vs4,vs9
  xvmaddasp		vs37, vs5,vs9
  xvmaddasp		vs52, vs4,vs13
  xvmaddasp		vs53, vs5,vs13
.if \Complete==0  
  xxperm    vs10, vs8,    permute_mask
  xxperm    vs14, vs12,   permute_mask    
.endif    
  xvmaddasp		vs44, vs4,vs11
  xvmaddasp		vs45, vs5,vs11
  xvmaddasp		vs60, vs4,vs15
  xvmaddasp		vs61, vs5,vs15
.if \Complete==0
  xxpermdi  vs9,  vs8,   vs8,2   
  xxpermdi  vs13, vs12, vs12,2   
.endif    
.if \Complete==0	
   lxv	vs4,	DISP8(\Index,0+\OffsetA)(\AREG)
   lxv	vs5,	DISP8(\Index,16+\OffsetA)(\AREG)
.endif

.if \Complete==0
  xxpermdi  vs11, vs10, vs10,2  
  xxpermdi  vs15, vs14, vs14,2  
.endif  
  xvmaddasp		vs32, vs0,vs24
  xvmaddasp		vs33, vs1,vs24
  xvmaddasp		vs48, vs0,vs28
  xvmaddasp		vs49, vs1,vs28
  xvmaddasp		vs40, vs0,vs26
  xvmaddasp		vs41, vs1,vs26
  xvmaddasp		vs56, vs0,vs30
  xvmaddasp		vs57, vs1,vs30
.if \Complete==0
  lxv vs24, DISP8(\Index,32+\OffsetB)(\BREG)
  lxv vs28, DISP8(\Index,32+16+\OffsetB)(\BREG)
.endif   
  xvmaddasp		vs36, vs0,vs25
  xvmaddasp		vs37, vs1,vs25
  xvmaddasp		vs52, vs0,vs29
  xvmaddasp		vs53, vs1,vs29
.if \Complete==0
  xxperm    vs26, vs24, permute_mask
  xxperm    vs30, vs28, permute_mask  
.endif    
  xvmaddasp		vs44, vs0,vs27
  xvmaddasp		vs45, vs1,vs27
  xvmaddasp		vs60, vs0,vs31
  xvmaddasp		vs61, vs1,vs31 
.if \Complete==0
  xxpermdi  vs25, vs24, vs24,2 
  xxpermdi  vs29, vs28, vs28,2    
.endif  
.if \Complete==0
  lxv	vs0,	DISP8(\Index,32+\OffsetA)(\AREG)
  lxv	vs1,	DISP8(\Index,32+16+\OffsetA)(\AREG) 
.endif

.if \Complete==0
  xxpermdi  vs27, vs26, vs26,2  
  xxpermdi  vs31, vs30, vs30,2   
.endif

.if \IsLast==1	
.if \Complete==1
	addi		\BREG, \BREG,  DISP8(\Index,\OffsetB)
  addi    \AREG, \AREG, DISP8(\Index,\OffsetA)  
.else
	addi		\BREG, \BREG,  DISP8(\Index,64)
  addi    \AREG, \AREG, DISP8(\Index,64)  
.endif

.endif   
.endm


.macro KERNEL4x4
  LOAD4x4
  END4x4  AO, BO, 32,32
.endm


.macro SAVE4x4
  add T4, LDC,LDC
  add T1, CO ,LDC  
#ifndef TRMMKERNEL  
  lxv vs24 , 0(CO)
  lxv vs25 , 16(CO)
#endif
  add T2,CO,T4
  add T3,T1,T4  
#ifndef TRMMKERNEL  
  lxv vs26 , 0(T1)
  lxv vs27 , 16(T1)
#endif  
 #ifndef TRMMKERNEL  
  lxv vs28 , 0(T2)
  lxv vs29 , 16(T2)
#endif
#ifndef TRMMKERNEL  
  lxv vs30 , 0(T3)
  lxv vs31 , 16(T3)
#endif   
  xxperm  vs0,vs32,permute_mask
  xxperm  vs4,vs40,permute_mask
  xxperm  vs1,vs33,permute_mask
  xxperm  vs5,vs41,permute_mask
  xxperm  vs8,vs36,permute_mask
  xxperm  vs12,vs44,permute_mask
  xxperm  vs9,vs37,permute_mask
  xxperm  vs13,vs45,permute_mask
  AGGREGATE_REALS_IMAGES vs32,vs0,vs40,vs4
  AGGREGATE_REALS_IMAGES vs33,vs1,vs41,vs5
  AGGREGATE_REALS_IMAGES vs36,vs8,vs44,vs12
  AGGREGATE_REALS_IMAGES vs37,vs9,vs45,vs13
  xxperm  vs0,vs48,permute_mask
  xxperm  vs4,vs56,permute_mask
  xxperm  vs1,vs49,permute_mask
  xxperm  vs5,vs57,permute_mask 
  xxperm  vs8,vs52,permute_mask
  xxperm  vs12,vs60,permute_mask
  xxperm  vs9,vs53,permute_mask
  xxperm  vs13,vs61,permute_mask
  AGGREGATE_REALS_IMAGES vs48,vs0,vs56,vs4
  AGGREGATE_REALS_IMAGES vs49,vs1,vs57,vs5
  AGGREGATE_REALS_IMAGES vs52,vs8,vs60,vs12
  AGGREGATE_REALS_IMAGES vs53,vs9,vs61,vs13
  /*VSINRR,VSINII,VSOUT1,VSOUT2*/
  MULT_APLHA_PART1    vs32,vs40,vs0,vs1
  MULT_APLHA_PART1    vs33,vs41,vs2,vs3    
  MULT_APLHA_PART1    vs36,vs44,vs8,vs9
  MULT_APLHA_PART1    vs37,vs45,vs10,vs11
  MULT_APLHA_PART1    vs48,vs56,vs4,vs5
  MULT_APLHA_PART1    vs49,vs57,vs6,vs7    
  MULT_APLHA_PART1    vs52,vs60,vs12,vs13
  MULT_APLHA_PART1    vs53,vs61,vs14,vs15
  MULT_APLHA_PART2    vs32,vs40,vs0,vs1    
  MULT_APLHA_PART2    vs33,vs41,vs2,vs3   
  MULT_APLHA_PART2    vs36,vs44,vs8,vs9
  MULT_APLHA_PART2    vs37,vs45,vs10,vs11
  MULT_APLHA_PART2    vs48,vs56,vs4,vs5
  MULT_APLHA_PART2    vs49,vs57,vs6,vs7    
  MULT_APLHA_PART2    vs52,vs60,vs12,vs13
  MULT_APLHA_PART2    vs53,vs61,vs14,vs15
/* reconstruct r,i pairs*/
  xxperm  vs0,vs1, save_permute_1
  xxperm  vs2,vs3, save_permute_1
  xxperm  vs8,vs9, save_permute_1
  xxperm  vs10,vs11, save_permute_1
  xxperm  vs4,vs5, save_permute_1
  xxperm  vs6,vs7, save_permute_1
  xxperm  vs12,vs13, save_permute_1
  xxperm  vs14,vs15, save_permute_1
#ifndef TRMMKERNEL
  /* add */
  xxpermdi vs1,vs8,vs0,2
  xxpermdi vs3,vs10,vs2,2 
  xxpermdi vs9,vs0,vs8,2
  xxpermdi vs11,vs2,vs10,2  
  xxpermdi vs5,vs12,vs4,2
  xxpermdi vs7,vs14,vs6,2 
  xxpermdi vs13,vs4,vs12,2
  xxpermdi vs15,vs6,vs14,2   
  xvaddsp vs24,vs24,vs1
  xvaddsp vs25,vs25,vs3 
  xvaddsp vs26,vs26,vs9
  xvaddsp vs27,vs27,vs11 
  xvaddsp vs28,vs28,vs5
  xvaddsp vs29,vs29,vs7 
  xvaddsp vs30,vs30,vs13
  xvaddsp vs31,vs31,vs15 
#else
  xxpermdi vs24,vs8,vs0,2
  xxpermdi vs25,vs10,vs2,2
  xxpermdi vs26,vs0,vs8,2
  xxpermdi vs27,vs2,vs10,2  
  xxpermdi vs28,vs12,vs4,2
  xxpermdi vs29,vs14,vs6,2 
  xxpermdi vs30,vs4,vs12,2
  xxpermdi vs31,vs6,vs14,2   
#endif
  stxv vs24 , 0(CO)
  stxv vs25 , 16(CO)
  stxv vs26 , 0(T1)
  stxv vs27 , 16(T1)
  stxv vs28 , 0(T2)
  stxv vs29 , 16(T2)
  stxv vs30 , 0(T3)
  stxv vs31 , 16(T3)  
  addi  CO, CO, 32
.endm

/*                                             macros for N=4 and M=2
**********************************************************************************************/

.macro Zero4x2
	xxlxor	vs32,	vs32,	vs32
	xxlxor	vs33,	vs33,	vs33
	xxlxor	vs36,	vs36,	vs36
	xxlxor	vs37,	vs37,	vs37
	xxlxor	vs40,	vs40,	vs40
	xxlxor	vs41,	vs41,	vs41
	xxlxor	vs44,	vs44,	vs44
	xxlxor	vs45,	vs45,	vs45
.endm


.macro LOAD4x2   
	LOAD4x2O 0,0 
.endm


.macro LOAD4x2O  OffsetA,OffsetB
	lxv	vs24,	(\OffsetA+0)(AO)
  lxv vs0,  (\OffsetB+0)(BO)
  lxv vs1,  (\OffsetB+16)(BO)
	xxperm  	vs26,	vs24,		permute_mask  
	xxpermdi	vs25,	vs24,	vs24,2	    
	xxpermdi	vs27,	vs26,	vs26,2	
.endm


.macro END4x2_NORMAL
	END4x2 AO,BO,16,32
.endm


.macro END4x2_WITHOUT_ADD
	END4x2 AO,BO,0,0
.endm


.macro END4x2	AREG, BREG, OffsetA, OffsetB
.if \OffsetB != 0
	addi	\BREG, \BREG, \OffsetB
.endif

.if \OffsetA != 0
	addi	\AREG, \AREG, \OffsetA
.endif

    xvmaddasp       vs32, vs0,vs24
    xvmaddasp       vs33, vs1,vs24
    xvmaddasp       vs36, vs0,vs25
    xvmaddasp       vs37, vs1,vs25
    xvmaddasp       vs40, vs0,vs26
    xvmaddasp       vs41, vs1,vs26
    xvmaddasp       vs44, vs0,vs27
    xvmaddasp       vs45, vs1,vs27
.endm


.macro LOAD4x2_2
    LOAD4x2_2O 0,0
.endm
	

.macro LOAD4x2_2O  OffsetA,OffsetB
  lxv	vs8,	(\OffsetA)(AO) 
  lxv	vs24,	(16+\OffsetA)(AO) 
  lxv	vs4,	(0+\OffsetB)(BO)
  lxv	vs5,	(16+\OffsetB)(BO)
  xxperm  	vs10,	vs8,		permute_mask
  xxpermdi	vs9,	vs8,	 vs8,2	 
  xxperm  	vs26,	vs24,	permute_mask
  xxpermdi	vs25,	vs24,	vs24,2    
  lxv vs0,  (32+\OffsetB)(BO)
  lxv vs1,  (32+16+\OffsetB)(BO) 
  xxpermdi  vs11, vs10, vs10,2  
  xxpermdi	vs27,	vs26,	vs26,2	
.endm


.macro END4x2_2	  
  /*for load2 offset will be 32 and 64*/
   KERNEL4x2_2	AO,BO,	32,64,0 ,1,1 
.endm


.macro KERNEL4x2_E2	OffsetA,OffsetB, Index,IsLast 
  KERNEL4x2_2	AO,BO,	\OffsetA,\OffsetB, \Index,\IsLast ,1 
.endm


.macro KERNEL4x2_L2	OffsetA,OffsetB, Index,IsLast
  KERNEL4x2_2	AO,BO,	\OffsetA,\OffsetB, \Index,\IsLast ,0 
.endm


.macro KERNEL4x2_2	AREG,BREG,	OffsetA,OffsetB, Index,IsLast ,Complete
  xvmaddasp		vs32, vs4,vs8
  xvmaddasp		vs33, vs5,vs8
  xvmaddasp		vs40, vs4,vs10
  xvmaddasp		vs41, vs5,vs10
.if \Complete==0  
  lxv vs8,  DISP4(\Index,\OffsetA)(\AREG) 
.endif  
  xvmaddasp		vs36, vs4,vs9
  xvmaddasp		vs37, vs5,vs9
  xvmaddasp   vs44, vs4,vs11
  xvmaddasp   vs45, vs5,vs11
.if \Complete==0  
  xxperm    vs10, vs8,    permute_mask 
  xxpermdi  vs9,  vs8,   vs8,2  
.endif    
.if \Complete==0	
   lxv	vs4,	DISP8(\Index,0+\OffsetB)(\BREG)
   lxv	vs5,	DISP8(\Index,16+\OffsetB)(\BREG)
.endif

.if \Complete==0
  xxpermdi  vs11, vs10, vs10,2   
.endif  
  xvmaddasp		vs32, vs0,vs24
  xvmaddasp		vs33, vs1,vs24
  xvmaddasp		vs40, vs0,vs26
  xvmaddasp		vs41, vs1,vs26
.if \Complete==0
  lxv vs24, DISP4(\Index,16+\OffsetA)(\AREG) 
.endif   
  xvmaddasp		vs36, vs0,vs25
  xvmaddasp		vs37, vs1,vs25
  xvmaddasp		vs44, vs0,vs27
  xvmaddasp		vs45, vs1,vs27
.if \Complete==0
  xxperm    vs26, vs24, permute_mask 
  xxpermdi  vs25, vs24, vs24,2    
.endif  
.if \Complete==0
  lxv	vs0,	DISP8(\Index,32+\OffsetB)(\BREG)
  lxv	vs1,	DISP8(\Index,32+16+\OffsetB)(\BREG) 
.endif

.if \Complete==0
  xxpermdi  vs27, vs26, vs26,2    
.endif

.if \IsLast==1	
.if \Complete==1
  addi    \AREG, \AREG, DISP4(\Index,\OffsetA) 
	addi		\BREG, \BREG,  DISP8(\Index,\OffsetB)
.else
  addi    \AREG, \AREG, DISP4(\Index,32)  
	addi		\BREG, \BREG,  DISP8(\Index,64)
.endif

.endif   
.endm


.macro KERNEL4x2
  LOAD4x2
  END4x2  AO, BO, 16,32
.endm


.macro SAVE4x2
  add T4, LDC,LDC
  add T1, CO ,LDC  
  add T2,CO,T4
  add T3,T1,T4  
#ifndef TRMMKERNEL  
  lxv vs24 , 0(CO) 
#endif
#ifndef TRMMKERNEL  
  lxv vs25 , 0(T1) 
#endif  
#ifndef TRMMKERNEL  
  lxv vs26 , 0(T2) 
#endif
#ifndef TRMMKERNEL  
  lxv vs27 , 0(T3) 
#endif   
  xxperm  vs0,vs32,permute_mask
  xxperm  vs4,vs40,permute_mask
  xxperm  vs1,vs33,permute_mask
  xxperm  vs5,vs41,permute_mask 
  xxperm  vs8,vs36,permute_mask
  xxperm  vs12,vs44,permute_mask
  xxperm  vs9,vs37,permute_mask
  xxperm  vs13,vs45,permute_mask
  AGGREGATE_REALS_IMAGES_A_PERMUTE vs32,vs0,vs40,vs4
  AGGREGATE_REALS_IMAGES_A_PERMUTE vs33,vs1,vs41,vs5
  AGGREGATE_REALS_IMAGES_A_PERMUTE vs36,vs8,vs44,vs12
  AGGREGATE_REALS_IMAGES_A_PERMUTE vs37,vs9,vs45,vs13
  /*VSINRR,VSINII,VSOUT1,VSOUT2*/
  MULT_APLHA_PART1    vs32,vs40,vs0,vs1
  MULT_APLHA_PART1    vs33,vs41,vs2,vs3    
  MULT_APLHA_PART1    vs36,vs44,vs8,vs9
  MULT_APLHA_PART1    vs37,vs45,vs10,vs11
  MULT_APLHA_PART2    vs32,vs40,vs0,vs1    
  MULT_APLHA_PART2    vs33,vs41,vs2,vs3   
  MULT_APLHA_PART2    vs36,vs44,vs8,vs9
  MULT_APLHA_PART2    vs37,vs45,vs10,vs11
/* reconstruct r,i pairs*/
  xxperm  vs0,vs1, save_permute_1
  xxperm  vs2,vs3, save_permute_1
  xxperm  vs8,vs9, save_permute_1
  xxperm  vs10,vs11, save_permute_1
#ifndef TRMMKERNEL
  /* add */
  xxpermdi vs1,vs8,vs0,0
  xxpermdi vs9,vs10,vs2,0 
  xxpermdi vs3,vs0,vs8,3
  xxpermdi vs11,vs2,vs10,3 
  xvaddsp vs24,vs24,vs1
  xvaddsp vs26,vs26,vs9 
  xvaddsp vs25,vs25,vs3 
  xvaddsp vs27,vs27,vs11 
#else
  xxpermdi vs24,vs8,vs0,0
  xxpermdi vs26,vs10,vs2,0 
  xxpermdi vs25,vs0,vs8,3
  xxpermdi vs27,vs2,vs10,3 
#endif
  stxv vs24 , 0(CO) 
  stxv vs25 , 0(T1) 
  stxv vs26 , 0(T2) 
  stxv vs27 , 0(T3)  
  addi  CO, CO, 16
.endm

/*                                             macros for N=4 and M=2
**********************************************************************************************/

.macro Zero4x1
  xxlxor  vs32, vs32, vs32
  xxlxor  vs33, vs33, vs33 
  xxlxor  vs40, vs40, vs40
  xxlxor  vs41, vs41, vs41 
.endm


.macro LOAD4x1   
  LOAD4x1O 0,0 
.endm


.macro LOAD4x1O  OffsetA,OffsetB
  lxsd v4, (\OffsetA+0)(AO) 
  lxv vs0,  (\OffsetB+0)(BO)
  lxv vs1,  (\OffsetB+16)(BO)
  xxspltd  vs24,vs36,0
  xxperm    vs26, vs24,   permute_mask   
.endm


.macro END4x1_NORMAL
  END4x1 AO,BO,8,32
.endm


.macro END4x1_WITHOUT_ADD
  END4x1 AO,BO,0,0
.endm


.macro END4x1 AREG, BREG, OffsetA, OffsetB
.if \OffsetB != 0
  addi  \BREG, \BREG, \OffsetB
.endif

.if \OffsetA != 0
  addi  \AREG, \AREG, \OffsetA
.endif

    xvmaddasp       vs32, vs0,vs24
    xvmaddasp       vs33, vs1,vs24
    xvmaddasp       vs40, vs0,vs26
    xvmaddasp       vs41, vs1,vs26
.endm


.macro LOAD4x1_2
    LOAD4x1_2O 0,0
.endm
 

.macro LOAD4x1_2O  OffsetA,OffsetB
  lxv vs27,  (\OffsetA)(AO) 
  xxspltd  vs8,vs27,1
  xxspltd  vs24,vs27,0  
  lxv vs4,  (0+\OffsetB)(BO)
  lxv vs5,  (16+\OffsetB)(BO) 
  xxperm    vs10, vs8,    permute_mask 
  xxperm    vs26, vs24, permute_mask      
  lxv vs0,  (32+\OffsetB)(BO)
  lxv vs1,  (32+16+\OffsetB)(BO)
.endm


.macro END4x1_2   
  /*for load2 offset will be 16 and 64*/
   KERNEL4x1_2  AO,BO,  16,64,0 ,1,1 
.endm


.macro KERNEL4x1_E2 OffsetA,OffsetB, Index,IsLast 
  KERNEL4x1_2 AO,BO,  \OffsetA,\OffsetB, \Index,\IsLast ,1 
.endm


.macro KERNEL4x1_L2 OffsetA,OffsetB, Index,IsLast
  KERNEL4x1_2 AO,BO,  \OffsetA,\OffsetB, \Index,\IsLast ,0 
.endm


.macro KERNEL4x1_2  AREG,BREG,  OffsetA,OffsetB, Index,IsLast ,Complete
  xvmaddasp   vs32, vs4,vs8
  xvmaddasp   vs33, vs5,vs8
  xvmaddasp   vs40, vs4,vs10
  xvmaddasp   vs41, vs5,vs10
.if \Complete==0  
  lxv vs27,  DISP2(\Index,\OffsetA)(\AREG) 
  xxspltd  vs8,vs27,1 
.endif  
.if \Complete==0  
   lxv  vs4,  DISP8(\Index,0+\OffsetB)(\BREG)
   lxv  vs5,  DISP8(\Index,16+\OffsetB)(\BREG)
.endif

.if \Complete==0  
  xxperm    vs10, vs8,    permute_mask  
.endif    
  xvmaddasp   vs32, vs0,vs24
  xvmaddasp   vs33, vs1,vs24
  xvmaddasp   vs40, vs0,vs26
  xvmaddasp   vs41, vs1,vs26
.if \Complete==0 
  xxspltd  vs24,vs27,0  
  xxperm   vs26, vs24, permute_mask   
.endif  
.if \Complete==0
  lxv vs0,  DISP8(\Index,32+\OffsetB)(\BREG)
  lxv vs1,  DISP8(\Index,32+16+\OffsetB)(\BREG) 
.endif

.if \IsLast==1  
.if \Complete==1
  addi    \AREG, \AREG, DISP2(\Index,\OffsetA) 
  addi    \BREG, \BREG,  DISP8(\Index,\OffsetB)
.else
  addi    \AREG, \AREG, DISP2(\Index,16)  
  addi    \BREG, \BREG,  DISP8(\Index,64)
.endif

.endif   
.endm


.macro KERNEL4x1
  LOAD4x1
  END4x1  AO, BO, 8,32
.endm


.macro SAVE4x1
  add T4, LDC,LDC
  add T1, CO ,LDC  
  add T2,CO,T4
  add T3,T1,T4  
#ifndef TRMMKERNEL  
  lxsd v4 , 0(CO) 
#endif
#ifndef TRMMKERNEL  
  lxsd v5 , 0(T1) 
#endif  
#ifndef TRMMKERNEL  
  lxsd v6 , 0(T2) 
#endif
#ifndef TRMMKERNEL  
  lxsd v7 , 0(T3) 
#endif   
  xxperm  vs0,vs32,permute_mask
  xxperm  vs4,vs40,permute_mask
  xxperm  vs1,vs33,permute_mask
  xxperm  vs5,vs41,permute_mask 
  AGGREGATE_REALS_IMAGES_A_PERMUTE vs32,vs0,vs40,vs4
  AGGREGATE_REALS_IMAGES_A_PERMUTE vs33,vs1,vs41,vs5
  /*VSINRR,VSINII,VSOUT1,VSOUT2*/
  MULT_APLHA_PART1    vs32,vs40,vs0,vs1
  MULT_APLHA_PART1    vs33,vs41,vs2,vs3     
  MULT_APLHA_PART2    vs32,vs40,vs0,vs1    
  MULT_APLHA_PART2    vs33,vs41,vs2,vs3    
/* reconstruct r,i pairs*/
  xxperm  vs0,vs1, save_permute_1
  xxperm  vs2,vs3, save_permute_1
#ifndef TRMMKERNEL
  /* add */
  xxspltd vs1,vs0,0
  xxspltd vs3,vs0,1
  xxspltd vs9,vs2,0
  xxspltd vs11,vs2,1
 /*--v4==vs36 v5==vs37 v6==vs38 v7==vs39---*/
  xvaddsp vs36,vs36,vs1
  xvaddsp vs37,vs37,vs3   
  xvaddsp vs38,vs38,vs9  
  xvaddsp vs39,vs39,vs11 
#else 
 /*--v4==vs36 v5==vs37 v6==vs38 v7==vs39---*/
  xxspltd vs36,vs0,0
  xxspltd vs37,vs0,1
  xxspltd vs38,vs2,0
  xxspltd vs39,vs2,1
#endif
  stxsd v4 , 0(CO) 
  stxsd v5 , 0(T1) 
  stxsd v6 , 0(T2) 
  stxsd v7 , 0(T3)  
  addi  CO, CO, 8
.endm

/*                                             macros for N=2 and M=8
**********************************************************************************************/

.macro Zero2x8
  xxlxor  vs32, vs32, vs32
  xxlxor  vs33, vs33, vs33
  xxlxor  vs34, vs34, vs34
  xxlxor  vs35, vs35, vs35
  xxlxor  vs36, vs36, vs36
  xxlxor  vs37, vs37, vs37
  xxlxor  vs38, vs38, vs38
  xxlxor  vs39, vs39, vs39
  xxlxor  vs40, vs40, vs40
  xxlxor  vs41, vs41, vs41
  xxlxor  vs42, vs42, vs42
  xxlxor  vs43, vs43, vs43
  xxlxor  vs44, vs44, vs44
  xxlxor  vs45, vs45, vs45
  xxlxor  vs46, vs46, vs46
  xxlxor  vs47, vs47, vs47
.endm


.macro LOAD2x8   
  LOAD2x8O 0,0 
.endm


.macro LOAD2x8O  OffsetA,OffsetB
  lxv vs24, (\OffsetB+0)(BO) 
  xxperm    vs26, vs24,   permute_mask    
  lxv vs0,  (\OffsetA+0)(AO)
  lxv vs1,  (\OffsetA+16)(AO)
  lxv vs2,  (\OffsetA+32)(AO)
  lxv vs3,  (\OffsetA+48)(AO) 
  xxpermdi  vs25, vs24, vs24,2  
  xxpermdi  vs27, vs26, vs26,2
.endm


.macro END2x8_NORMAL
  END2x8 AO,BO,64,16
.endm


.macro END2x8_WITHOUT_ADD
  END2x8 AO,BO,0,0
.endm


.macro END2x8 AREG, BREG, OffsetA, OffsetB
.if \OffsetB != 0
  addi  \BREG, \BREG, \OffsetB
.endif

.if \OffsetA != 0
  addi  \AREG, \AREG, \OffsetA
.endif

    xvmaddasp       vs32, vs0,vs24
    xvmaddasp       vs33, vs1,vs24
    xvmaddasp       vs34, vs2,vs24  
    xvmaddasp       vs35, vs3,vs24  
    xvmaddasp       vs36, vs0,vs25
    xvmaddasp       vs37, vs1,vs25
    xvmaddasp       vs38, vs2,vs25  
    xvmaddasp       vs39, vs3,vs25 
    xvmaddasp       vs40, vs0,vs26
    xvmaddasp       vs41, vs1,vs26
    xvmaddasp       vs42, vs2,vs26  
    xvmaddasp       vs43, vs3,vs26
    xvmaddasp       vs44, vs0,vs27
    xvmaddasp       vs45, vs1,vs27
    xvmaddasp       vs46, vs2,vs27  
    xvmaddasp       vs47, vs3,vs27
.endm


.macro LOAD2x8_2
    LOAD2x8_2O 0,0
.endm
 

.macro LOAD2x8_2O  OffsetA,OffsetB
  lxv vs8,  (\OffsetB)(BO)
  lxv vs24, (16+\OffsetB)(BO)
  lxv vs4,  (0+\OffsetA)(AO)
  lxv vs5,  (16+\OffsetA)(AO)
  xxperm    vs10, vs8,    permute_mask 
  xxperm    vs26, vs24, permute_mask  
  lxv vs6,  (32+\OffsetA)(AO)
  lxv vs7,  (48+\OffsetA)(AO) 
  lxv vs0,  (64+\OffsetA)(AO)
  lxv vs1,  (64+16+\OffsetA)(AO) 
  xxpermdi  vs9,  vs8,   vs8,2    
  xxpermdi  vs25, vs24, vs24,2     
  lxv vs2,  (64+32+\OffsetA)(AO)
  lxv vs3,  (64+48+\OffsetA)(AO)
  xxpermdi  vs11, vs10, vs10,2
  xxpermdi  vs27, vs26, vs26,2 
.endm
 

.macro END2x8_2   
  /*for load2 offset will be 128 and 32*/
   KERNEL2x8_2  AO,BO,  128,32,0 ,1,1 
.endm


.macro KERNEL2x8_E2 OffsetA,OffsetB, Index,IsLast 
  KERNEL2x8_2 AO,BO,  \OffsetA,\OffsetB, \Index,\IsLast ,1 
.endm


.macro KERNEL2x8_L2 OffsetA,OffsetB, Index,IsLast
  KERNEL2x8_2 AO,BO,  \OffsetA,\OffsetB, \Index,\IsLast ,0 
.endm


.macro KERNEL2x8_2  AREG,BREG,  OffsetA,OffsetB, Index,IsLast ,Complete
  xvmaddasp   vs32, vs4,vs8
  xvmaddasp   vs33, vs5,vs8
  xvmaddasp   vs40, vs4,vs10
  xvmaddasp   vs41, vs5,vs10
  xvmaddasp   vs36, vs4,vs9
  xvmaddasp   vs37, vs5,vs9
  xvmaddasp   vs44, vs4,vs11
  xvmaddasp   vs45, vs5,vs11
.if \Complete==0  
   lxv  vs4,  DISP16(\Index,0+\OffsetA)(\AREG)
   lxv  vs5,  DISP16(\Index,16+\OffsetA)(\AREG)
.endif

  xvmaddasp   vs34, vs6,vs8 
  xvmaddasp   vs35, vs7,vs8
.if \Complete==0  
  lxv vs8,  DISP4(\Index,\OffsetB)(\BREG)
.endif    
  xvmaddasp   vs42, vs6,vs10
  xvmaddasp   vs43, vs7,vs10
  xvmaddasp   vs38, vs6,vs9 
  xvmaddasp   vs39, vs7,vs9
.if \Complete==0
  xxperm    vs10, vs8,    permute_mask  
  xxpermdi  vs9,  vs8,   vs8,2   
.endif    
  xvmaddasp   vs46, vs6,vs11
  xvmaddasp   vs47, vs7,vs11
.if \Complete==0
  xxpermdi  vs11, vs10, vs10,2   
.endif  
.if \Complete==0
   lxv  vs6,  DISP16(\Index,32+\OffsetA)(\AREG)
   lxv  vs7,  DISP16(\Index,48+\OffsetA)(\AREG) 
.endif 
  xvmaddasp   vs32, vs0,vs24
  xvmaddasp   vs33, vs1,vs24
  xvmaddasp   vs40, vs0,vs26
  xvmaddasp   vs41, vs1,vs26
  xvmaddasp   vs36, vs0,vs25
  xvmaddasp   vs37, vs1,vs25
  xvmaddasp   vs44, vs0,vs27
  xvmaddasp   vs45, vs1,vs27
.if \Complete==0
  lxv vs0,  DISP16(\Index,64+\OffsetA)(\AREG)
  lxv vs1,  DISP16(\Index,64+16+\OffsetA)(\AREG) 
.endif

  xvmaddasp   vs34, vs2,vs24
  xvmaddasp   vs35, vs3,vs24    
.if \Complete==0
  lxv vs24, DISP4(\Index,16+\OffsetB)(\BREG)
.endif  
  xvmaddasp   vs42, vs2,vs26
  xvmaddasp   vs43, vs3,vs26
  xvmaddasp   vs38, vs2,vs25
  xvmaddasp   vs39, vs3,vs25
.if \Complete==0
  xxperm    vs26, vs24, permute_mask 
  xxpermdi  vs25, vs24, vs24,2   
.endif  
  xvmaddasp   vs46, vs2,vs27
  xvmaddasp   vs47, vs3,vs27
.if \Complete==0
  xxpermdi  vs27, vs26, vs26,2   
.endif

.if \Complete==0
  lxv vs2,  DISP16(\Index,64+32+\OffsetA)(\AREG)
  lxv vs3,  DISP16(\Index,64+48+\OffsetA)(\AREG)
.endif

.if \IsLast==1  
.if \Complete==1
  addi    \BREG, \BREG,  DISP4(\Index,\OffsetB)
  addi    \AREG, \AREG, DISP16(\Index,\OffsetA)  
.else
  addi    \BREG, \BREG,  DISP4(\Index,32)
  addi    \AREG, \AREG, DISP16(\Index,128)  
.endif

.endif   
.endm


.macro KERNEL2x8
  LOAD2x8
  END2x8  AO, BO, 64,16
.endm


.macro SAVE2x8
  add T1, CO ,LDC  
#ifndef TRMMKERNEL  
  lxv vs24 , 0(CO)
  lxv vs25 , 16(CO)
#endif
  xxperm  vs0,vs32,permute_mask
  xxperm  vs4,vs40,permute_mask
#ifndef TRMMKERNEL  
  lxv vs26 , 32(CO)
  lxv vs27 , 48(CO)
#endif  
  xxperm  vs1,vs33,permute_mask
  xxperm  vs5,vs41,permute_mask
#ifndef TRMMKERNEL  
  lxv vs28 , 0(T1)
  lxv vs29 , 16(T1)
#endif  
  xxperm  vs2,vs34,permute_mask
  xxperm  vs6,vs42,permute_mask
#ifndef TRMMKERNEL  
  lxv vs30 , 32(T1)
  lxv vs31 , 48(T1)
#endif 
  xxperm  vs3,vs35,permute_mask
  xxperm  vs7,vs43,permute_mask 
  add T2,CO,T4
  add T3,T1,T4  
  AGGREGATE_REALS_IMAGES vs32,vs0,vs40,vs4
  xxperm  vs8,vs36,permute_mask
  xxperm  vs12,vs44,permute_mask
  AGGREGATE_REALS_IMAGES vs33,vs1,vs41,vs5
  xxperm  vs9,vs37,permute_mask
  xxperm  vs13,vs45,permute_mask
  AGGREGATE_REALS_IMAGES vs34,vs2,vs42,vs6
  xxperm  vs10,vs38,permute_mask
  xxperm  vs14,vs46,permute_mask
  AGGREGATE_REALS_IMAGES vs35,vs3,vs43,vs7 
  xxperm  vs11,vs39,permute_mask
  xxperm  vs15,vs47,permute_mask 
  AGGREGATE_REALS_IMAGES vs36,vs8,vs44,vs12
  AGGREGATE_REALS_IMAGES vs37,vs9,vs45,vs13
  AGGREGATE_REALS_IMAGES vs38,vs10,vs46,vs14
  AGGREGATE_REALS_IMAGES vs39,vs11,vs47,vs15 
  /*VSINRR,VSINII,VSOUT1,VSOUT2*/
  MULT_APLHA_PART1    vs32,vs40,vs0,vs1 
  MULT_APLHA_PART1    vs33,vs41,vs2,vs3    
  MULT_APLHA_PART1    vs34,vs42,vs4,vs5
  MULT_APLHA_PART1    vs35,vs43,vs6,vs7  
  MULT_APLHA_PART2    vs32,vs40,vs0,vs1    
  MULT_APLHA_PART2    vs33,vs41,vs2,vs3   
  MULT_APLHA_PART2    vs34,vs42,vs4,vs5
  MULT_APLHA_PART2    vs35,vs43,vs6,vs7  
  MULT_APLHA_PART1    vs36,vs44,vs8,vs9
  MULT_APLHA_PART1    vs37,vs45,vs10,vs11
  MULT_APLHA_PART1    vs38,vs46,vs12,vs13
  MULT_APLHA_PART1    vs39,vs47,vs14,vs15
  MULT_APLHA_PART2    vs36,vs44,vs8,vs9
  MULT_APLHA_PART2    vs37,vs45,vs10,vs11
  MULT_APLHA_PART2    vs38,vs46,vs12,vs13
  MULT_APLHA_PART2    vs39,vs47,vs14,vs15
/* reconstruct r,i pairs*/
  xxperm  vs0,vs1, save_permute_1
  xxperm  vs2,vs3, save_permute_1
  xxperm  vs4,vs5, save_permute_1
  xxperm  vs6,vs7, save_permute_1
  xxperm  vs8,vs9, save_permute_1
  xxperm  vs10,vs11, save_permute_1
  xxperm  vs12,vs13, save_permute_1
  xxperm  vs14,vs15, save_permute_1
#ifndef TRMMKERNEL
  /* add */
  xxpermdi vs1,vs8,vs0,2
  xxpermdi vs3,vs10,vs2,2
  xxpermdi vs5,vs12,vs4,2
  xxpermdi vs7,vs14,vs6,2
  xxpermdi vs9,vs0,vs8,2
  xxpermdi vs11,vs2,vs10,2  
  xvaddsp vs24,vs24,vs1
  xvaddsp vs25,vs25,vs3
  xxpermdi vs13,vs4,vs12,2  
  xxpermdi vs15,vs6,vs14,2
  xvaddsp vs26,vs26,vs5
  xvaddsp  vs27,vs27,vs7
  xvaddsp vs28,vs28,vs9
  xvaddsp vs29,vs29,vs11 
  xvaddsp vs30,vs30,vs13
  xvaddsp vs31,vs31,vs15  
#else
  xxpermdi vs24,vs8,vs0,2
  xxpermdi vs25,vs10,vs2,2
  xxpermdi vs26,vs12,vs4,2
  xxpermdi vs27,vs14,vs6,2 
  xxpermdi vs28,vs0,vs8,2
  xxpermdi vs29,vs2,vs10,2  
  xxpermdi vs30,vs4,vs12,2  
  xxpermdi vs31,vs6,vs14,2
#endif
  stxv vs24 , 0(CO)
  stxv vs25 , 16(CO) 
  stxv vs26 , 32(CO)
  stxv vs27 , 48(CO) 
  stxv vs28 , 0(T1)
  stxv vs29 , 16(T1) 
  stxv vs30 , 32(T1)
  stxv vs31 , 48(T1)  
  addi  CO, CO, 64
.endm

/*                                             macros for N=2 and M=4
**********************************************************************************************/

.macro Zero2x4
  xxlxor  vs32, vs32, vs32
  xxlxor  vs33, vs33, vs33
  xxlxor  vs36, vs36, vs36
  xxlxor  vs37, vs37, vs37
  xxlxor  vs40, vs40, vs40
  xxlxor  vs41, vs41, vs41
  xxlxor  vs44, vs44, vs44
  xxlxor  vs45, vs45, vs45
.endm


.macro LOAD2x4   
  LOAD2x4O 0,0 
.endm


.macro LOAD2x4O  OffsetA,OffsetB
  lxv vs24, (\OffsetB+0)(BO)
  lxv vs0,  (\OffsetA+0)(AO)
  lxv vs1,  (\OffsetA+16)(AO)
  xxperm    vs26, vs24,   permute_mask  
  xxpermdi  vs25, vs24, vs24,2     
  xxpermdi  vs27, vs26, vs26,2  
.endm


.macro END2x4_NORMAL
  END2x4 AO,BO,32,16
.endm


.macro END2x4_WITHOUT_ADD
  END2x4 AO,BO,0,0
.endm


.macro END2x4 AREG, BREG, OffsetA, OffsetB
.if \OffsetB != 0
  addi  \BREG, \BREG, \OffsetB
.endif

.if \OffsetA != 0
  addi  \AREG, \AREG, \OffsetA
.endif

    xvmaddasp       vs32, vs0,vs24
    xvmaddasp       vs33, vs1,vs24
    xvmaddasp       vs36, vs0,vs25
    xvmaddasp       vs37, vs1,vs25
    xvmaddasp       vs40, vs0,vs26
    xvmaddasp       vs41, vs1,vs26
    xvmaddasp       vs44, vs0,vs27
    xvmaddasp       vs45, vs1,vs27
.endm


.macro LOAD2x4_2
    LOAD2x4_2O 0,0
.endm
 

.macro LOAD2x4_2O  OffsetA,OffsetB
  lxv vs8,  (\OffsetB)(BO)
  lxv vs24, (16+\OffsetB)(BO)
  lxv vs4,  (0+\OffsetA)(AO)
  lxv vs5,  (16+\OffsetA)(AO)
  xxperm    vs10, vs8,    permute_mask
  xxperm    vs26, vs24, permute_mask
  xxpermdi  vs9,  vs8,   vs8,2   
  xxpermdi  vs25, vs24, vs24,2     
  lxv vs0,  (32+\OffsetA)(AO)
  lxv vs1,  (32+16+\OffsetA)(AO) 
  xxpermdi  vs11, vs10, vs10,2  
  xxpermdi  vs27, vs26, vs26,2  
.endm


.macro END2x4_2   
  /*for load2 offset will be 64 and 32*/
   KERNEL2x4_2  AO,BO,  64,32,0 ,1,1 
.endm


.macro KERNEL2x4_E2 OffsetA,OffsetB, Index,IsLast 
  KERNEL2x4_2 AO,BO,  \OffsetA,\OffsetB, \Index,\IsLast ,1 
.endm


.macro KERNEL2x4_L2 OffsetA,OffsetB, Index,IsLast
  KERNEL2x4_2 AO,BO,  \OffsetA,\OffsetB, \Index,\IsLast ,0 
.endm


.macro KERNEL2x4_2  AREG,BREG,  OffsetA,OffsetB, Index,IsLast ,Complete
  xvmaddasp   vs32, vs4,vs8
  xvmaddasp   vs33, vs5,vs8
  xvmaddasp   vs40, vs4,vs10
  xvmaddasp   vs41, vs5,vs10
.if \Complete==0  
  lxv vs8,  DISP4(\Index,\OffsetB)(\BREG)
.endif  
  xvmaddasp   vs36, vs4,vs9
  xvmaddasp   vs37, vs5,vs9
  xvmaddasp   vs44, vs4,vs11
  xvmaddasp   vs45, vs5,vs11
.if \Complete==0
  xxperm    vs10, vs8,    permute_mask 
  xxpermdi  vs9,  vs8,   vs8,2   
.endif    
.if \Complete==0  
   lxv  vs4,  DISP8(\Index,0+\OffsetA)(\AREG)
   lxv  vs5,  DISP8(\Index,16+\OffsetA)(\AREG)
.endif

.if \Complete==0
  xxpermdi  vs11, vs10, vs10,2 
.endif  
  xvmaddasp   vs32, vs0,vs24
  xvmaddasp   vs33, vs1,vs24
  xvmaddasp   vs40, vs0,vs26
  xvmaddasp   vs41, vs1,vs26
.if \Complete==0
  lxv vs24, DISP4(\Index,16+\OffsetB)(\BREG)
.endif   
  xvmaddasp   vs36, vs0,vs25
  xvmaddasp   vs37, vs1,vs25
  xvmaddasp   vs44, vs0,vs27
  xvmaddasp   vs45, vs1,vs27
.if \Complete==0
  xxperm    vs26, vs24, permute_mask
  xxpermdi  vs25, vs24, vs24,2 
.endif  
.if \Complete==0
  lxv vs0,  DISP8(\Index,32+\OffsetA)(\AREG)
  lxv vs1,  DISP8(\Index,32+16+\OffsetA)(\AREG) 
.endif

.if \Complete==0
  xxpermdi  vs27, vs26, vs26,2  
.endif

.if \IsLast==1  
.if \Complete==1
  addi    \BREG, \BREG,  DISP4(\Index,\OffsetB)
  addi    \AREG, \AREG, DISP8(\Index,\OffsetA)  
.else
  addi    \BREG, \BREG,  DISP4(\Index,32)
  addi    \AREG, \AREG, DISP8(\Index,64)  
.endif

.endif   
.endm


.macro KERNEL2x4
  LOAD2x4
  END2x4  AO, BO, 32,16
.endm


.macro SAVE2x4
  add T1, CO ,LDC  
#ifndef TRMMKERNEL  
  lxv vs24 , 0(CO)
  lxv vs25 , 16(CO)
#endif
#ifndef TRMMKERNEL  
  lxv vs26 , 0(T1)
  lxv vs27 , 16(T1)
#endif  
  xxperm  vs0,vs32,permute_mask
  xxperm  vs4,vs40,permute_mask
  xxperm  vs1,vs33,permute_mask
  xxperm  vs5,vs41,permute_mask
  xxperm  vs8,vs36,permute_mask
  xxperm  vs12,vs44,permute_mask
  xxperm  vs9,vs37,permute_mask
  xxperm  vs13,vs45,permute_mask
  AGGREGATE_REALS_IMAGES vs32,vs0,vs40,vs4
  AGGREGATE_REALS_IMAGES vs33,vs1,vs41,vs5
  AGGREGATE_REALS_IMAGES vs36,vs8,vs44,vs12
  AGGREGATE_REALS_IMAGES vs37,vs9,vs45,vs13
  /*VSINRR,VSINII,VSOUT1,VSOUT2*/
  MULT_APLHA_PART1    vs32,vs40,vs0,vs1
  MULT_APLHA_PART1    vs33,vs41,vs2,vs3    
  MULT_APLHA_PART1    vs36,vs44,vs8,vs9
  MULT_APLHA_PART1    vs37,vs45,vs10,vs11
  MULT_APLHA_PART2    vs32,vs40,vs0,vs1    
  MULT_APLHA_PART2    vs33,vs41,vs2,vs3   
  MULT_APLHA_PART2    vs36,vs44,vs8,vs9
  MULT_APLHA_PART2    vs37,vs45,vs10,vs11
/* reconstruct r,i pairs*/
  xxperm  vs0,vs1, save_permute_1
  xxperm  vs2,vs3, save_permute_1
  xxperm  vs8,vs9, save_permute_1
  xxperm  vs10,vs11, save_permute_1
#ifndef TRMMKERNEL
  /* add */
  xxpermdi vs1,vs8,vs0,2
  xxpermdi vs3,vs10,vs2,2 
  xxpermdi vs9,vs0,vs8,2
  xxpermdi vs11,vs2,vs10,2  
  xvaddsp vs24,vs24,vs1
  xvaddsp vs25,vs25,vs3 
  xvaddsp vs26,vs26,vs9
  xvaddsp vs27,vs27,vs11 
#else
  xxpermdi vs24,vs8,vs0,2
  xxpermdi vs25,vs10,vs2,2
  xxpermdi vs26,vs0,vs8,2
  xxpermdi vs27,vs2,vs10,2  
#endif
  stxv vs24 , 0(CO)
  stxv vs25 , 16(CO)
  stxv vs26 , 0(T1)
  stxv vs27 , 16(T1)
  addi  CO, CO, 32
.endm

/*                                             macros for N=2 and M=2
**********************************************************************************************/

.macro Zero2x2
  xxlxor  vs32, vs32, vs32
  xxlxor  vs36, vs36, vs36
  xxlxor  vs40, vs40, vs40
  xxlxor  vs44, vs44, vs44
.endm


.macro LOAD2x2   
  LOAD2x2O 0,0 
.endm


.macro LOAD2x2O  OffsetA,OffsetB
  lxv vs24, (\OffsetA+0)(AO)
  lxv vs0,  (\OffsetB+0)(BO)
  xxperm    vs26, vs24,   permute_mask  
  xxpermdi  vs25, vs24, vs24,2      
  xxpermdi  vs27, vs26, vs26,2  
.endm


.macro END2x2_NORMAL
  END2x2 AO,BO,16,16
.endm


.macro END2x2_WITHOUT_ADD
  END2x2 AO,BO,0,0
.endm


.macro END2x2 AREG, BREG, OffsetA, OffsetB
.if \OffsetB != 0
  addi  \BREG, \BREG, \OffsetB
.endif

.if \OffsetA != 0
  addi  \AREG, \AREG, \OffsetA
.endif

    xvmaddasp       vs32, vs0,vs24
    xvmaddasp       vs36, vs0,vs25
    xvmaddasp       vs40, vs0,vs26
    xvmaddasp       vs44, vs0,vs27
.endm


.macro LOAD2x2_2
    LOAD2x2_2O 0,0
.endm
 

.macro LOAD2x2_2O  OffsetA,OffsetB
  lxv vs8,  (\OffsetA)(AO) 
  lxv vs24, (16+\OffsetA)(AO) 
  lxv vs4,  (0+\OffsetB)(BO)
  lxv vs0,  (16+\OffsetB)(BO)
  xxperm    vs10, vs8,    permute_mask
  xxpermdi  vs9,  vs8,   vs8,2   
  xxperm    vs26, vs24, permute_mask
  xxpermdi  vs25, vs24, vs24,2    
  xxpermdi  vs11, vs10, vs10,2  
  xxpermdi  vs27, vs26, vs26,2  
.endm


.macro END2x2_2   
  /*for load2 offset will be 32 and 32*/
   KERNEL2x2_2  AO,BO,  32,32,0 ,1,1 
.endm


.macro KERNEL2x2_E2 OffsetA,OffsetB, Index,IsLast 
  KERNEL2x2_2 AO,BO,  \OffsetA,\OffsetB, \Index,\IsLast ,1 
.endm


.macro KERNEL2x2_L2 OffsetA,OffsetB, Index,IsLast
  KERNEL2x2_2 AO,BO,  \OffsetA,\OffsetB, \Index,\IsLast ,0 
.endm


.macro KERNEL2x2_2  AREG,BREG,  OffsetA,OffsetB, Index,IsLast ,Complete
  xvmaddasp   vs32, vs4,vs8
  xvmaddasp   vs40, vs4,vs10
.if \Complete==0  
  lxv vs8,  DISP4(\Index,\OffsetA)(\AREG) 
.endif  
  xvmaddasp   vs36, vs4,vs9
  xvmaddasp   vs44, vs4,vs11
.if \Complete==0  
  xxperm    vs10, vs8,    permute_mask 
  xxpermdi  vs9,  vs8,   vs8,2  
.endif    
.if \Complete==0  
   lxv  vs4,  DISP4(\Index,0+\OffsetB)(\BREG)
.endif

.if \Complete==0
  xxpermdi  vs11, vs10, vs10,2   
.endif  
  xvmaddasp   vs32, vs0,vs24
  xvmaddasp   vs40, vs0,vs26
.if \Complete==0
  lxv vs24, DISP4(\Index,16+\OffsetA)(\AREG) 
.endif   
  xvmaddasp   vs36, vs0,vs25
  xvmaddasp   vs44, vs0,vs27
.if \Complete==0
  xxperm    vs26, vs24, permute_mask 
  xxpermdi  vs25, vs24, vs24,2    
.endif  
.if \Complete==0
  lxv vs0,  DISP4(\Index,16+\OffsetB)(\BREG)
.endif

.if \Complete==0
  xxpermdi  vs27, vs26, vs26,2    
.endif

.if \IsLast==1  
.if \Complete==1
  addi    \AREG, \AREG, DISP4(\Index,\OffsetA) 
  addi    \BREG, \BREG,  DISP4(\Index,\OffsetB)
.else
  addi    \AREG, \AREG, DISP4(\Index,32)  
  addi    \BREG, \BREG,  DISP4(\Index,32)
.endif

.endif   
.endm


.macro KERNEL2x2
  LOAD2x2
  END2x2  AO, BO, 16,16
.endm


.macro SAVE2x2
  add T1, CO ,LDC  
#ifndef TRMMKERNEL  
  lxv vs24 , 0(CO) 
#endif
#ifndef TRMMKERNEL  
  lxv vs26 , 0(T1) 
#endif  
  xxperm  vs0,vs32,permute_mask
  xxperm  vs4,vs40,permute_mask
  xxperm  vs8,vs36,permute_mask
  xxperm  vs12,vs44,permute_mask
  AGGREGATE_REALS_IMAGES_A_PERMUTE vs32,vs0,vs40,vs4
  AGGREGATE_REALS_IMAGES_A_PERMUTE vs36,vs8,vs44,vs12
  /*VSINRR,VSINII,VSOUT1,VSOUT2*/
  MULT_APLHA_PART1    vs32,vs40,vs0,vs1
  MULT_APLHA_PART1    vs36,vs44,vs8,vs9
  MULT_APLHA_PART2    vs32,vs40,vs0,vs1   
  MULT_APLHA_PART2    vs36,vs44,vs8,vs9
/* reconstruct r,i pairs*/
  xxperm  vs0,vs1, save_permute_1
  xxperm  vs8,vs9, save_permute_1
#ifndef TRMMKERNEL
  /* add */
  xxpermdi vs1,vs8,vs0,0
  xxpermdi vs9,vs0,vs8,3 
  xvaddsp vs24,vs24,vs1
  xvaddsp vs26,vs26,vs9 
#else
  xxpermdi vs24,vs8,vs0,0
  xxpermdi vs26,vs0,vs8,3 
#endif
  stxv vs24 , 0(CO) 
  stxv vs26 , 0(T1)
  addi  CO, CO, 16
.endm

/*                                             macros for N=2 and M=1
**********************************************************************************************/

.macro Zero2x1
  xxlxor  vs32, vs32, vs32
  xxlxor  vs40, vs40, vs40
.endm


.macro LOAD2x1   
  LOAD2x1O 0,0 
.endm


.macro LOAD2x1O  OffsetA,OffsetB
  lxsd v4, (\OffsetA+0)(AO) 
  lxv vs0,  (\OffsetB+0)(BO)
  xxspltd  vs24,vs36,0
  xxperm    vs26, vs24,   permute_mask   
.endm


.macro END2x1_NORMAL
  END2x1 AO,BO,8,16
.endm


.macro END2x1_WITHOUT_ADD
  END2x1 AO,BO,0,0
.endm


.macro END2x1 AREG, BREG, OffsetA, OffsetB
.if \OffsetB != 0
  addi  \BREG, \BREG, \OffsetB
.endif

.if \OffsetA != 0
  addi  \AREG, \AREG, \OffsetA
.endif

    xvmaddasp       vs32, vs0,vs24
    xvmaddasp       vs40, vs0,vs26
.endm


.macro LOAD2x1_2
    LOAD2x1_2O 0,0
.endm
 

.macro LOAD2x1_2O  OffsetA,OffsetB
  lxv vs27,  (\OffsetA)(AO) 
  lxv vs4,  (0+\OffsetB)(BO)
  lxv vs0,  (16+\OffsetB)(BO)
  xxspltd  vs8,vs27,1
  xxspltd  vs24,vs27,0  
  xxperm    vs10, vs8,    permute_mask 
  xxperm    vs26, vs24, permute_mask      
.endm


.macro END2x1_2   
  /*for load2 offset will be 16 and 32*/
   KERNEL2x1_2  AO,BO,  16,32,0 ,1,1 
.endm


.macro KERNEL2x1_E2 OffsetA,OffsetB, Index,IsLast 
  KERNEL2x1_2 AO,BO,  \OffsetA,\OffsetB, \Index,\IsLast ,1 
.endm


.macro KERNEL2x1_L2 OffsetA,OffsetB, Index,IsLast
  KERNEL2x1_2 AO,BO,  \OffsetA,\OffsetB, \Index,\IsLast ,0 
.endm


.macro KERNEL2x1_2  AREG,BREG,  OffsetA,OffsetB, Index,IsLast ,Complete
  xvmaddasp   vs32, vs4,vs8
  xvmaddasp   vs40, vs4,vs10
.if \Complete==0  
  lxv vs27,  DISP2(\Index,\OffsetA)(\AREG) 
  xxspltd  vs8,vs27,1 
.endif  
.if \Complete==0  
   lxv  vs4,  DISP4(\Index,0+\OffsetB)(\BREG)
.endif

.if \Complete==0  
  xxperm    vs10, vs8,    permute_mask  
.endif    
  xvmaddasp   vs32, vs0,vs24
  xvmaddasp   vs40, vs0,vs26
.if \Complete==0 
  xxspltd  vs24,vs27,0  
  xxperm   vs26, vs24, permute_mask   
.endif  
.if \Complete==0
  lxv vs0,  DISP4(\Index,16+\OffsetB)(\BREG)
.endif

.if \IsLast==1  
.if \Complete==1
  addi    \AREG, \AREG, DISP2(\Index,\OffsetA) 
  addi    \BREG, \BREG,  DISP4(\Index,\OffsetB)
.else
  addi    \AREG, \AREG, DISP2(\Index,16)  
  addi    \BREG, \BREG,  DISP4(\Index,32)
.endif

.endif   
.endm


.macro KERNEL2x1
  LOAD2x1
  END2x1  AO, BO, 8,16
.endm


.macro SAVE2x1
  add T1, CO ,LDC  
#ifndef TRMMKERNEL  
  lxsd v4 , 0(CO) 
#endif
#ifndef TRMMKERNEL  
  lxsd v5 , 0(T1) 
#endif  
  xxperm  vs0,vs32,permute_mask
  xxperm  vs4,vs40,permute_mask
  AGGREGATE_REALS_IMAGES_A_PERMUTE vs32,vs0,vs40,vs4
  AGGREGATE_REALS_IMAGES_A_PERMUTE vs33,vs1,vs41,vs5
  /*VSINRR,VSINII,VSOUT1,VSOUT2*/
  MULT_APLHA_PART1    vs32,vs40,vs0,vs1 
  MULT_APLHA_PART2    vs32,vs40,vs0,vs1      
/* reconstruct r,i pairs*/
  xxperm  vs0,vs1, save_permute_1 
#ifndef TRMMKERNEL
  /* add */
  xxspltd vs1,vs0,0
  xxspltd vs3,vs0,1
 /*--v4==vs36 v5==vs37---*/
  xvaddsp vs36,vs36,vs1
  xvaddsp vs37,vs37,vs3  
#else 
 /*--v4==vs36 v5==vs37---*/
  xxspltd vs36,vs0,0
  xxspltd vs37,vs0,1
#endif
  stxsd v4 , 0(CO) 
  stxsd v5 , 0(T1) 
  addi  CO, CO, 8
.endm

/*                                             macros for N=1 and M=8
**********************************************************************************************/

.macro Zero1x8
  xxlxor  vs32, vs32, vs32
  xxlxor  vs33, vs33, vs33
  xxlxor  vs34, vs34, vs34
  xxlxor  vs35, vs35, vs35
  xxlxor  vs40, vs40, vs40
  xxlxor  vs41, vs41, vs41
  xxlxor  vs42, vs42, vs42
  xxlxor  vs43, vs43, vs43
.endm


.macro LOAD1x8   
  LOAD1x8O 0,0 
.endm


.macro LOAD1x8O  OffsetA,OffsetB
  lxsd vs4, (\OffsetB+0)(BO) 
  lxv vs0,  (\OffsetA+0)(AO)
  lxv vs1,  (\OffsetA+16)(AO)
  lxv vs2,  (\OffsetA+32)(AO)
  lxv vs3,  (\OffsetA+48)(AO) 
  xxspltd   vs24,vs36,0
  xxperm    vs26, vs24,   permute_mask    
.endm


.macro END1x8_NORMAL
  END1x8 AO,BO,64,8
.endm


.macro END1x8_WITHOUT_ADD
  END1x8 AO,BO,0,0
.endm


.macro END1x8 AREG, BREG, OffsetA, OffsetB
.if \OffsetB != 0
  addi  \BREG, \BREG, \OffsetB
.endif

.if \OffsetA != 0
  addi  \AREG, \AREG, \OffsetA
.endif

    xvmaddasp       vs32, vs0,vs24
    xvmaddasp       vs33, vs1,vs24
    xvmaddasp       vs34, vs2,vs24  
    xvmaddasp       vs35, vs3,vs24  
    xvmaddasp       vs40, vs0,vs26
    xvmaddasp       vs41, vs1,vs26
    xvmaddasp       vs42, vs2,vs26  
    xvmaddasp       vs43, vs3,vs26
.endm


.macro LOAD1x8_2
    LOAD1x8_2O 0,0
.endm
 

.macro LOAD1x8_2O  OffsetA,OffsetB
  lxv vs27,  (\OffsetB)(BO)
  lxv vs4,  (0+\OffsetA)(AO)
  lxv vs5,  (16+\OffsetA)(AO)
  xxspltd  vs8,vs27,1
  xxspltd  vs24,vs27,0    
  lxv vs6,  (32+\OffsetA)(AO)
  lxv vs7,  (48+\OffsetA)(AO) 
  lxv vs0,  (64+\OffsetA)(AO)
  lxv vs1,  (64+16+\OffsetA)(AO)     
  lxv vs2,  (64+32+\OffsetA)(AO)
  lxv vs3,  (64+48+\OffsetA)(AO)
  xxperm    vs10, vs8,    permute_mask 
  xxperm    vs26, vs24, permute_mask   
.endm
 

.macro END1x8_2   
  /*for load2 offset will be 128 and 16*/
   KERNEL1x8_2  AO,BO,  128,16,0 ,1,1 
.endm


.macro KERNEL1x8_E2 OffsetA,OffsetB, Index,IsLast 
  KERNEL1x8_2 AO,BO,  \OffsetA,\OffsetB, \Index,\IsLast ,1 
.endm


.macro KERNEL1x8_L2 OffsetA,OffsetB, Index,IsLast
  KERNEL1x8_2 AO,BO,  \OffsetA,\OffsetB, \Index,\IsLast ,0 
.endm


.macro KERNEL1x8_2  AREG,BREG,  OffsetA,OffsetB, Index,IsLast ,Complete
.if \Complete==0  
  lxv vs27,  DISP2(\Index,\OffsetB)(\BREG)
.endif    
  xvmaddasp   vs32, vs4,vs8
  xvmaddasp   vs33, vs5,vs8
  xvmaddasp   vs40, vs4,vs10
  xvmaddasp   vs41, vs5,vs10
.if \Complete==0  
   lxv  vs4,  DISP16(\Index,0+\OffsetA)(\AREG)
   lxv  vs5,  DISP16(\Index,16+\OffsetA)(\AREG)
.endif

  xvmaddasp   vs34, vs6,vs8 
  xvmaddasp   vs35, vs7,vs8
  xvmaddasp   vs42, vs6,vs10
  xvmaddasp   vs43, vs7,vs10
.if \Complete==0
   lxv  vs6,  DISP16(\Index,32+\OffsetA)(\AREG)
   lxv  vs7,  DISP16(\Index,48+\OffsetA)(\AREG) 
.endif 
.if \Complete==0 
  xxspltd  vs8,vs27,1    
  xxperm    vs10, vs8,    permute_mask   
.endif    
  xvmaddasp   vs32, vs0,vs24
  xvmaddasp   vs33, vs1,vs24
  xvmaddasp   vs40, vs0,vs26
  xvmaddasp   vs41, vs1,vs26
.if \Complete==0
  lxv vs0,  DISP16(\Index,64+\OffsetA)(\AREG)
  lxv vs1,  DISP16(\Index,64+16+\OffsetA)(\AREG) 
.endif

  xvmaddasp   vs34, vs2,vs24
  xvmaddasp   vs35, vs3,vs24    
  xvmaddasp   vs42, vs2,vs26
  xvmaddasp   vs43, vs3,vs26
.if \Complete==0
  xxspltd  vs24,vs27,0   
  xxperm    vs26, vs24, permute_mask  
.endif  
.if \Complete==0
  lxv vs2,  DISP16(\Index,64+32+\OffsetA)(\AREG)
  lxv vs3,  DISP16(\Index,64+48+\OffsetA)(\AREG)
.endif

.if \IsLast==1  
.if \Complete==1
  addi    \BREG, \BREG,  DISP2(\Index,\OffsetB)
  addi    \AREG, \AREG, DISP16(\Index,\OffsetA)  
.else
  addi    \BREG, \BREG,  DISP2(\Index,16)
  addi    \AREG, \AREG, DISP16(\Index,128)  
.endif

.endif   
.endm


.macro KERNEL1x8
  LOAD1x8
  END1x8  AO, BO, 64,8
.endm


.macro SAVE1x8
#ifndef TRMMKERNEL  
  lxv vs24 , 0(CO)
  lxv vs25 , 16(CO)
#endif
  xxperm  vs0,vs32,permute_mask
  xxperm  vs4,vs40,permute_mask
#ifndef TRMMKERNEL  
  lxv vs26 , 32(CO)
  lxv vs27 , 48(CO)
#endif  
  xxperm  vs1,vs33,permute_mask
  xxperm  vs5,vs41,permute_mask
  xxperm  vs2,vs34,permute_mask
  xxperm  vs6,vs42,permute_mask
  xxperm  vs3,vs35,permute_mask
  xxperm  vs7,vs43,permute_mask 
  AGGREGATE_REALS_IMAGES vs32,vs0,vs40,vs4
  AGGREGATE_REALS_IMAGES vs33,vs1,vs41,vs5
  AGGREGATE_REALS_IMAGES vs34,vs2,vs42,vs6
  AGGREGATE_REALS_IMAGES vs35,vs3,vs43,vs7 
  /*inner reverse save_permute and store vs28 */
  xxpermdi vs28,save_permute_1,save_permute_1,2
  /*VSINRR,VSINII,VSOUT1,VSOUT2*/
  MULT_APLHA_PART1    vs32,vs40,vs0,vs1 
  MULT_APLHA_PART1    vs33,vs41,vs2,vs3    
  MULT_APLHA_PART1    vs34,vs42,vs4,vs5
  MULT_APLHA_PART1    vs35,vs43,vs6,vs7  
  MULT_APLHA_PART2    vs32,vs40,vs0,vs1    
  MULT_APLHA_PART2    vs33,vs41,vs2,vs3   
  MULT_APLHA_PART2    vs34,vs42,vs4,vs5
  MULT_APLHA_PART2    vs35,vs43,vs6,vs7  
/* reconstruct r,i pairs*/
  xxperm  vs0,vs1, vs28
  xxperm  vs2,vs3, vs28
  xxperm  vs4,vs5, vs28
  xxperm  vs6,vs7, vs28  
#ifndef TRMMKERNEL
  /* add */
  xvaddsp vs24,vs24,vs0
  xvaddsp vs25,vs25,vs2
  xvaddsp vs26,vs26,vs4
  xvaddsp  vs27,vs27,vs6
  stxv vs24 , 0(CO)
  stxv vs25 , 16(CO) 
  stxv vs26 , 32(CO)
  stxv vs27 , 48(CO)    
#else
/* reconstruct r,i pairs*/
  stxv vs0 , 0(CO)
  stxv vs2 , 16(CO) 
  stxv vs4 , 32(CO)
  stxv vs6 , 48(CO)  
#endif
  addi  CO, CO, 64
.endm

/*                                             macros for N=1 and M=4
**********************************************************************************************/

.macro Zero1x4
  xxlxor  vs32, vs32, vs32
  xxlxor  vs33, vs33, vs33
  xxlxor  vs40, vs40, vs40
  xxlxor  vs41, vs41, vs41
.endm


.macro LOAD1x4   
  LOAD1x4O 0,0 
.endm


.macro LOAD1x4O  OffsetA,OffsetB
  lxsd vs4, (\OffsetB+0)(BO) 
  lxv vs0,  (\OffsetA+0)(AO)
  lxv vs1,  (\OffsetA+16)(AO)
  xxspltd   vs24,vs36,0
  xxperm    vs26, vs24,   permute_mask    
.endm


.macro END1x4_NORMAL
  END1x4 AO,BO,32,8
.endm


.macro END1x4_WITHOUT_ADD
  END1x4 AO,BO,0,0
.endm


.macro END1x4 AREG, BREG, OffsetA, OffsetB
.if \OffsetB != 0
  addi  \BREG, \BREG, \OffsetB
.endif

.if \OffsetA != 0
  addi  \AREG, \AREG, \OffsetA
.endif

    xvmaddasp       vs32, vs0,vs24
    xvmaddasp       vs33, vs1,vs24
    xvmaddasp       vs40, vs0,vs26
    xvmaddasp       vs41, vs1,vs26
.endm


.macro LOAD1x4_2
    LOAD1x4_2O 0,0
.endm
 

.macro LOAD1x4_2O  OffsetA,OffsetB
  lxv vs27,  (\OffsetB)(BO)
  lxv vs4,  (0+\OffsetA)(AO)
  lxv vs5,  (16+\OffsetA)(AO)
  xxspltd  vs8,vs27,1
  xxspltd  vs24,vs27,0    
  lxv vs0,  (32+\OffsetA)(AO)
  lxv vs1,  (32+16+\OffsetA)(AO)     
  xxperm    vs10, vs8,    permute_mask 
  xxperm    vs26, vs24, permute_mask   
.endm
 

.macro END1x4_2   
  /*for load2 offset will be 64 and 16*/
   KERNEL1x4_2  AO,BO,  64,16,0 ,1,1 
.endm


.macro KERNEL1x4_E2 OffsetA,OffsetB, Index,IsLast 
  KERNEL1x4_2 AO,BO,  \OffsetA,\OffsetB, \Index,\IsLast ,1 
.endm


.macro KERNEL1x4_L2 OffsetA,OffsetB, Index,IsLast
  KERNEL1x4_2 AO,BO,  \OffsetA,\OffsetB, \Index,\IsLast ,0 
.endm


.macro KERNEL1x4_2  AREG,BREG,  OffsetA,OffsetB, Index,IsLast ,Complete
.if \Complete==0  
  lxv vs27,  DISP2(\Index,\OffsetB)(\BREG)
.endif    
  xvmaddasp   vs32, vs4,vs8
  xvmaddasp   vs33, vs5,vs8
  xvmaddasp   vs40, vs4,vs10
  xvmaddasp   vs41, vs5,vs10
.if \Complete==0  
   lxv  vs4,  DISP8(\Index,0+\OffsetA)(\AREG)
   lxv  vs5,  DISP8(\Index,16+\OffsetA)(\AREG)
.endif

.if \Complete==0 
  xxspltd  vs8,vs27,1    
  xxperm    vs10, vs8,    permute_mask   
.endif    
  xvmaddasp   vs32, vs0,vs24
  xvmaddasp   vs33, vs1,vs24
  xvmaddasp   vs40, vs0,vs26
  xvmaddasp   vs41, vs1,vs26
.if \Complete==0
  lxv vs0,  DISP8(\Index,32+\OffsetA)(\AREG)
  lxv vs1,  DISP8(\Index,32+16+\OffsetA)(\AREG) 
.endif

.if \Complete==0
  xxspltd  vs24,vs27,0   
  xxperm    vs26, vs24, permute_mask  
.endif  
.if \IsLast==1  
.if \Complete==1
  addi    \BREG, \BREG,  DISP2(\Index,\OffsetB)
  addi    \AREG, \AREG, DISP8(\Index,\OffsetA)  
.else
  addi    \BREG, \BREG,  DISP2(\Index,16)
  addi    \AREG, \AREG, DISP8(\Index,64)  
.endif

.endif   
.endm


.macro KERNEL1x4
  LOAD1x4
  END1x4  AO, BO, 32,8
.endm


.macro SAVE1x4
#ifndef TRMMKERNEL  
  lxv vs24 , 0(CO)
  lxv vs25 , 16(CO)
#endif
  xxperm  vs0,vs32,permute_mask
  xxperm  vs4,vs40,permute_mask
  xxperm  vs1,vs33,permute_mask
  xxperm  vs5,vs41,permute_mask
  AGGREGATE_REALS_IMAGES vs32,vs0,vs40,vs4
  AGGREGATE_REALS_IMAGES vs33,vs1,vs41,vs5
  /*inner reverse save_permute and store vs28 */
  xxpermdi vs28,save_permute_1,save_permute_1,2
  /*VSINRR,VSINII,VSOUT1,VSOUT2*/
  MULT_APLHA_PART1    vs32,vs40,vs0,vs1 
  MULT_APLHA_PART1    vs33,vs41,vs2,vs3    
  MULT_APLHA_PART2    vs32,vs40,vs0,vs1    
  MULT_APLHA_PART2    vs33,vs41,vs2,vs3   
/* reconstruct r,i pairs*/
  xxperm  vs0,vs1, vs28
  xxperm  vs2,vs3, vs28
#ifndef TRMMKERNEL
  /* add */
  xvaddsp vs24,vs24,vs0
  xvaddsp vs25,vs25,vs2
  stxv vs24 , 0(CO)
  stxv vs25 , 16(CO) 
#else
/* reconstruct r,i pairs*/
  stxv vs0 , 0(CO)
  stxv vs2 , 16(CO) 
#endif
  addi  CO, CO, 32
.endm

/*                                             macros for N=1 and M=2
**********************************************************************************************/

.macro Zero1x2
  xxlxor  vs32, vs32, vs32
  xxlxor  vs40, vs40, vs40
.endm


.macro LOAD1x2   
  LOAD1x2O 0,0 
.endm


.macro LOAD1x2O  OffsetA,OffsetB
  lxsd vs4, (\OffsetB+0)(BO) 
  lxv vs0,  (\OffsetA+0)(AO)
  xxspltd   vs24,vs36,0
  xxperm    vs26, vs24,   permute_mask    
.endm


.macro END1x2_NORMAL
  END1x2 AO,BO,16,8
.endm


.macro END1x2_WITHOUT_ADD
  END1x2 AO,BO,0,0
.endm


.macro END1x2 AREG, BREG, OffsetA, OffsetB
.if \OffsetB != 0
  addi  \BREG, \BREG, \OffsetB
.endif

.if \OffsetA != 0
  addi  \AREG, \AREG, \OffsetA
.endif

    xvmaddasp       vs32, vs0,vs24
    xvmaddasp       vs40, vs0,vs26
.endm


.macro LOAD1x2_2
    LOAD1x2_2O 0,0
.endm
 

.macro LOAD1x2_2O  OffsetA,OffsetB
  lxv vs27,  (\OffsetB)(BO)
  lxv vs4,  (0+\OffsetA)(AO)
  lxv vs0,  (16+\OffsetA)(AO)
  xxspltd  vs8,vs27,1
  xxspltd  vs24,vs27,0    
  xxperm    vs10, vs8,    permute_mask 
  xxperm    vs26, vs24, permute_mask   
.endm
 

.macro END1x2_2   
  /*for load2 offset will be 32 and 16*/
   KERNEL1x2_2  AO,BO,  32,16,0 ,1,1 
.endm


.macro KERNEL1x2_E2 OffsetA,OffsetB, Index,IsLast 
  KERNEL1x2_2 AO,BO,  \OffsetA,\OffsetB, \Index,\IsLast ,1 
.endm


.macro KERNEL1x2_L2 OffsetA,OffsetB, Index,IsLast
  KERNEL1x2_2 AO,BO,  \OffsetA,\OffsetB, \Index,\IsLast ,0 
.endm


.macro KERNEL1x2_2  AREG,BREG,  OffsetA,OffsetB, Index,IsLast ,Complete
.if \Complete==0  
  lxv vs27,  DISP2(\Index,\OffsetB)(\BREG)
.endif    
  xvmaddasp   vs32, vs4,vs8
  xvmaddasp   vs40, vs4,vs10
.if \Complete==0  
   lxv  vs4,  DISP4(\Index,0+\OffsetA)(\AREG)
.endif

.if \Complete==0 
  xxspltd  vs8,vs27,1    
  xxperm    vs10, vs8,    permute_mask   
.endif    
  xvmaddasp   vs32, vs0,vs24
  xvmaddasp   vs40, vs0,vs26
.if \Complete==0
  lxv vs0,  DISP4(\Index,16+\OffsetA)(\AREG)
.endif

.if \Complete==0
  xxspltd  vs24,vs27,0   
  xxperm    vs26, vs24, permute_mask  
.endif  
.if \IsLast==1  
.if \Complete==1
  addi    \BREG, \BREG,  DISP2(\Index,\OffsetB)
  addi    \AREG, \AREG, DISP4(\Index,\OffsetA)  
.else
  addi    \BREG, \BREG,  DISP2(\Index,16)
  addi    \AREG, \AREG, DISP4(\Index,32)  
.endif

.endif   
.endm


.macro KERNEL1x2
  LOAD1x2
  END1x2  AO, BO, 16,8
.endm


.macro SAVE1x2
#ifndef TRMMKERNEL  
  lxv vs24 , 0(CO)
#endif
  xxperm  vs0,vs32,permute_mask
  xxperm  vs4,vs40,permute_mask
  AGGREGATE_REALS_IMAGES vs32,vs0,vs40,vs4
  /*inner reverse save_permute and store vs28 */
  xxpermdi vs28,save_permute_1,save_permute_1,2
  /*VSINRR,VSINII,VSOUT1,VSOUT2*/
  MULT_APLHA_PART1    vs32,vs40,vs0,vs1 
  MULT_APLHA_PART2    vs32,vs40,vs0,vs1    
/* reconstruct r,i pairs*/
  xxperm  vs0,vs1, vs28
#ifndef TRMMKERNEL
  /* add */
  xvaddsp vs24,vs24,vs0
  stxv vs24 , 0(CO)
#else
/* reconstruct r,i pairs*/
  stxv vs0 , 0(CO)
#endif
  addi  CO, CO, 16
.endm

/*                                             macros for N=1 and M=1
**********************************************************************************************/
.macro Zero1x1
  xxlxor  vs32, vs32, vs32
  xxlxor  vs40, vs40, vs40
.endm


.macro LOAD1x1   
  LOAD1x1O 0,0 
.endm


.macro LOAD1x1O  OffsetA,OffsetB
  lxsd v4, (\OffsetB+0)(BO) 
  lxsd v5,  (\OffsetA+0)(AO)
  xxperm    vs38, vs36,   permute_mask    
.endm


.macro END1x1_NORMAL
  END1x1 AO,BO,8,8
.endm


.macro END1x1_WITHOUT_ADD
  END1x1 AO,BO,0,0
.endm


.macro END1x1 AREG, BREG, OffsetA, OffsetB
.if \OffsetB != 0
  addi  \BREG, \BREG, \OffsetB
.endif

.if \OffsetA != 0
  addi  \AREG, \AREG, \OffsetA
.endif

    xvmaddasp       vs32, vs37,vs36
    xvmaddasp       vs40, vs37,vs38
.endm


.macro LOAD1x1_2
    LOAD1x1_2O 0,0
.endm
 

.macro LOAD1x1_2O  OffsetA,OffsetB
  lxv vs8,  (\OffsetB)(BO)
  lxv vs4,  (0+\OffsetA)(AO) 
  xxperm    vs10, vs8,    permute_mask  
.endm
 

.macro END1x1_2   
  /*for load2 offset will be 16 and 16*/
   KERNEL1x1_2  AO,BO,  16,16,0 ,1,1 
.endm


.macro KERNEL1x1_E2 OffsetA,OffsetB, Index,IsLast 
  KERNEL1x1_2 AO,BO,  \OffsetA,\OffsetB, \Index,\IsLast ,1 
.endm


.macro KERNEL1x1_L2 OffsetA,OffsetB, Index,IsLast
  KERNEL1x1_2 AO,BO,  \OffsetA,\OffsetB, \Index,\IsLast ,0 
.endm


.macro KERNEL1x1_2  AREG,BREG,  OffsetA,OffsetB, Index,IsLast ,Complete
 
  xvmaddasp   vs32, vs4,vs8
  xvmaddasp   vs40, vs4,vs10
.if \Complete==0  
  lxv vs8,  DISP2(\Index,\OffsetB)(\BREG)
  lxv vs4,  DISP2(\Index,\OffsetB)(\AREG)
  xxperm    vs10, vs8,    permute_mask  
.endif

.if \IsLast==1  
.if \Complete==1
  addi    \BREG, \BREG,  DISP2(\Index,\OffsetB)
  addi    \AREG, \AREG, DISP2(\Index,\OffsetA)  
.else
  addi    \BREG, \BREG,  DISP2(\Index,16)
  addi    \AREG, \AREG, DISP2(\Index,16)  
.endif

.endif   
.endm


.macro KERNEL1x1
  LOAD1x1
  END1x1  AO, BO, 8,8
.endm


.macro SAVE1x1
#ifndef TRMMKERNEL  
  lxsd v4 , 0(CO)
#endif
  /*aggregate x2*/
  xxpermdi vs33,vs32,vs32,2
  xxpermdi vs41,vs40,vs40,2 
  xvaddsp vs32,vs32,vs33
  xvaddsp vs40,vs40,vs41

  xxperm  vs0,vs32,permute_mask
  xxperm  vs4,vs40,permute_mask
  AGGREGATE_REALS_IMAGES vs32,vs0,vs40,vs4
  /*inner reverse save_permute and store vs28 */
  xxpermdi vs28,save_permute_1,save_permute_1,2
  /*VSINRR,VSINII,VSOUT1,VSOUT2*/
  MULT_APLHA_PART1    vs32,vs40,vs37,vs1 
  MULT_APLHA_PART2    vs32,vs40,vs37,vs1    

/* reconstruct r,i pairs*/
  xxperm  vs37,vs1, vs28  

#ifndef TRMMKERNEL
  /* add */
  xvaddsp vs36,vs36,vs37
  stxsd v4 , 0(CO)
#else

/* vs37 is v5 */
  stxsd v5 , 0(CO)
#endif
  addi  CO, CO, 8
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
// 		ptrba += off*8;
// 		ptrbb = bb + off*4;
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
// 		temp = off+8;	// number of values in A
// #else
// 		temp = off+4;	// number of values in B
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
// 		temp -= 8; // number of values in A
// #else
// 		temp -= 4; // number of values in B
// #endif
// 		ptrba += temp*8;
// 		ptrbb += temp*4;
// #endif

// #ifdef LEFT
// 		off += 8; // number of values in A
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