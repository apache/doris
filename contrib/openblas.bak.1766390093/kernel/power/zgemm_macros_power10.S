/***************************************************************************
Copyright (c) 2013-2020, The OpenBLAS Project
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

#define unit_size 16
#define DISP32(ind,disp) (ind*unit_size*32+disp)
#define DISP16(ind,disp) (ind*unit_size*16+disp)
#define DISP8(ind,disp) (ind*unit_size*8+disp)
#define DISP4(ind,disp) (ind*unit_size*4+disp)
#define DISP2(ind,disp) (ind*unit_size*2+disp)
#define DISP1(ind,disp) (ind*unit_size+disp)
#define DISPX(disp)  (disp)
/*	HELPERS FOR SAVE	*/
/* {r0,i0} and {r1,i1} into  {r0,r1} {i0,i1} */


.macro LOAD_COUPLE_AS_RR_II  VS_OUT1,VS_OUT2,VS_TEMP1,VS_TEMP2,REG,LOFFSET 
#ifndef TRMMKERNEL 
  lxv	\VS_TEMP1,	DISPX(\LOFFSET)(\REG)
  lxv	\VS_TEMP2,	DISPX(\LOFFSET+16)(\REG)
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__) 
  xxmrghd  \VS_OUT1,\VS_TEMP1,\VS_TEMP2
  xxmrgld  \VS_OUT2,\VS_TEMP1,\VS_TEMP2
#else
  xxmrgld  \VS_OUT1,\VS_TEMP1,\VS_TEMP2
  xxmrghd  \VS_OUT2,\VS_TEMP1,\VS_TEMP2	
#endif
#endif	
.endm
/*from 2 result {a0r*br,a0i*bi} and {a1r*br,a1i*bi} pack into {a0r*br,a1r*br} and {a0i*bi,a1i*bi}*/


.macro RESULT_INTO_REALREAL_IMAGEIMAGE VSIN1,VSIN2,VSOUT1,VSOUT2
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
    xxmrghd \VSOUT1, \VSIN1,\VSIN2 /*  real*real from 2 results*/
    xxmrgld \VSOUT2, \VSIN1,\VSIN2 /*  imag*imag from 2 results*/
#else
	xxmrgld	\VSOUT1, \VSIN1,\VSIN2 /*  real*real from 2 results*/
	xxmrghd	\VSOUT2, \VSIN1,\VSIN2 /*  imag*imag from 2 results*/
#endif
.endm 
/*from 2 result {a0r*bi,a0i*br} and {a1r*bi,a1i*br} pack into {a0r*bi,a1r*bi} and {a0i*br,a1i*br}*/


.macro RESULT_INTO_REALIMAG_IMAGREAL VSIN1,VSIN2,VSOUT1,VSOUT2 
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
    xxmrghd \VSOUT1, \VSIN1,\VSIN2 /*  real*imag */
    xxmrgld \VSOUT2, \VSIN1,\VSIN2 /*  imag*real*/
#else
	xxmrgld	\VSOUT1, \VSIN1,\VSIN2 /*  real*imag */
	xxmrghd	\VSOUT2, \VSIN1,\VSIN2 /*  imag*real*/
#endif
.endm
/* {a0r*br op a0i*bi ,a1r*br op a1i*bi} ~ {r0,r1}; {a0r*bi op a0i*br ,a1r*bi op a1i*br} ~ {i0,i1}*/


.macro  AGGREGATE_REALS_IMAGES  VSINR_OUT1,VSINR,VSINI_OUT2,VSINI
#if	defined(NN) || defined(NT) || defined(TN) || defined(TT) 
	xvsubdp  \VSINR_OUT1,\VSINR_OUT1,\VSINR
	xvadddp  \VSINI_OUT2,\VSINI_OUT2,\VSINI  
#elif  defined(CN) || defined(CT) || defined(RN) || defined(RT) 
	xvadddp  \VSINR_OUT1,\VSINR_OUT1,\VSINR
	xvsubdp  \VSINI_OUT2,\VSINI_OUT2,\VSINI 
#elif  defined(NC) || defined(TC) || defined(NR) || defined(TR) 
	xvadddp  \VSINR_OUT1,\VSINR_OUT1,\VSINR
	xvsubdp  \VSINI_OUT2,\VSINI,\VSINI_OUT2  
#else	// CC || CR || RC || RR 
    /*we will assume {-alpha_r,-alpha_i} for this case */
    /*i1i2-r1r2 so we will negate alpha real instead to fix sign*/
	xvsubdp  \VSINR_OUT1,\VSINR,\VSINR_OUT1
    /*we will negate alpha image instead  instead to fix sign*/
	xvadddp  \VSINI_OUT2,\VSINI_OUT2,\VSINI 
#endif
.endm 
/* {i0,i1} * {alpha_i,alpha_i} - VSOUT1 ;VSOUT2 + {r0,r1}*{alpha_i,alpha_i} */


.macro MULT_APLHA_PART1  VSINRR,VSINII,VSOUT1,VSOUT2
#ifndef TRMMKERNEL  
	xvmsubadp \VSOUT1,\VSINII, alpha_i
	xvmaddadp  \VSOUT2,\VSINRR, alpha_i
#else 
	xvmuldp \VSOUT1,\VSINII, alpha_i 
	xvmuldp  \VSOUT2,\VSINRR, alpha_i
#endif 
.endm
/*   {r0,r1} * {alpha_r,alpha_r} -  VSOUT1 ;VSOUT2 + {i0,i1} * {alpha_r,alpha_r} */


.macro MULT_APLHA_PART2  VSINRR,VSINII,VSOUT1,VSOUT2 
	xvmsubadp  \VSOUT1,\VSINRR, alpha_r
	xvmaddadp \VSOUT2,\VSINII, alpha_r
.endm
/* unpack to store 2{r,r} {i,i} into  {r,i} {r,i} (big endian because of stxv) */


.macro UNPACK_FOR_STORE VSIN1,VSIN2,VSOUT1,VSOUT2 
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
    xxmrghd  \VSOUT1,\VSIN1,\VSIN2
    xxmrgld  \VSOUT2,\VSIN1,\VSIN2
#else
	xxmrghd  \VSOUT1,\VSIN2,\VSIN1
	xxmrgld  \VSOUT2,\VSIN2,\VSIN1
#endif
.endm


.macro STORE_COUPLE REG,LOFFSET,VSIN1,VSIN2
	stxv	\VSIN1,	DISPX(\LOFFSET)(\REG)
	stxv	\VSIN2,	DISPX(\LOFFSET+16)(\REG)
.endm


.macro SAVE8 VSRes1,VSRes2,VSRes3,VSRes4,VSRes5,VSRes6,VSRes7,VSRes8,VSRes9,VSRes10,VSRes11,VSRes12,VSRes13,VSRes14,VSRes15,VSRes16,BASE_REG,LOFFSET
  RESULT_INTO_REALREAL_IMAGEIMAGE \VSRes1,\VSRes3,vs34,vs35
  LOAD_COUPLE_AS_RR_II	vs46,vs47,vs50,vs51,\BASE_REG,\LOFFSET
  RESULT_INTO_REALIMAG_IMAGREAL	\VSRes2,\VSRes4,vs36,vs37
  LOAD_COUPLE_AS_RR_II	vs48,vs49,vs52,vs53,\BASE_REG,(\LOFFSET+32)
  RESULT_INTO_REALREAL_IMAGEIMAGE \VSRes5,\VSRes7,vs38,vs39
  LOAD_COUPLE_AS_RR_II	vs56,vs57,vs50,vs51,\BASE_REG,(\LOFFSET +64)
  RESULT_INTO_REALIMAG_IMAGREAL	\VSRes6,\VSRes8,vs40,vs41 
  LOAD_COUPLE_AS_RR_II	vs58,vs59,vs52,vs53,\BASE_REG,(\LOFFSET+96)
  RESULT_INTO_REALREAL_IMAGEIMAGE \VSRes9,\VSRes11,vs42,vs43
  AGGREGATE_REALS_IMAGES	vs34,vs35,vs36,vs37
  RESULT_INTO_REALIMAG_IMAGREAL	\VSRes10,\VSRes12,vs44,vs45 
  AGGREGATE_REALS_IMAGES	vs38,vs39,vs40,vs41  
  RESULT_INTO_REALREAL_IMAGEIMAGE \VSRes13,\VSRes15,\VSRes1,\VSRes2
  MULT_APLHA_PART1	vs34,vs36, vs46,vs47
  RESULT_INTO_REALIMAG_IMAGREAL	\VSRes14,\VSRes16,\VSRes3,\VSRes4
  MULT_APLHA_PART1	vs38,vs40,vs48,vs49
  MULT_APLHA_PART2  vs34,vs36,vs46,vs47 
  AGGREGATE_REALS_IMAGES	vs42,vs43,vs44,vs45
  MULT_APLHA_PART2	vs38,vs40,vs48,vs49
  AGGREGATE_REALS_IMAGES	\VSRes1,\VSRes2,\VSRes3,\VSRes4	
  UNPACK_FOR_STORE	vs46,vs47,vs39,vs41
  MULT_APLHA_PART1	vs42,vs44, vs56,vs57
  UNPACK_FOR_STORE	vs48,vs49,vs35,vs37 
  MULT_APLHA_PART1	\VSRes1,\VSRes3, vs58,vs59
  STORE_COUPLE	\BASE_REG,\LOFFSET,vs39,vs41
  MULT_APLHA_PART2	vs42,vs44,vs56,vs57
  STORE_COUPLE	\BASE_REG,(\LOFFSET+32),vs35,vs37 
  MULT_APLHA_PART2	\VSRes1,\VSRes3, vs58,vs59
  UNPACK_FOR_STORE	vs56,vs57,vs42,vs44
  UNPACK_FOR_STORE	vs58,vs59,\VSRes1,\VSRes3
  STORE_COUPLE	\BASE_REG,(\LOFFSET +64),vs42,vs44
  STORE_COUPLE	\BASE_REG,(\LOFFSET+96),\VSRes1,\VSRes3
.endm


.macro SAVE4  VSRes1,VSRes2,VSRes3,VSRes4,VSRes5,VSRes6,VSRes7,VSRes8,BASE_REG,LOFFSET
  RESULT_INTO_REALREAL_IMAGEIMAGE \VSRes1,\VSRes3,vs34,vs35
  LOAD_COUPLE_AS_RR_II	vs46,vs47,vs50,vs51,\BASE_REG,\LOFFSET
  RESULT_INTO_REALIMAG_IMAGREAL	\VSRes2,\VSRes4,vs36,vs37
  LOAD_COUPLE_AS_RR_II	vs48,vs49,vs52,vs53,\BASE_REG,(\LOFFSET+32)
  RESULT_INTO_REALREAL_IMAGEIMAGE \VSRes5,\VSRes7,vs38,vs39
  RESULT_INTO_REALIMAG_IMAGREAL	\VSRes6,\VSRes8,vs40,vs41 
  AGGREGATE_REALS_IMAGES	vs34,vs35,vs36,vs37	
  AGGREGATE_REALS_IMAGES	vs38,vs39,vs40,vs41  
  MULT_APLHA_PART1	vs34,vs36, vs46,vs47
  MULT_APLHA_PART1	vs38,vs40, vs48,vs49
  MULT_APLHA_PART2	vs34,vs36, vs46,vs47 
  MULT_APLHA_PART2	vs38,vs40,vs48,vs49
  UNPACK_FOR_STORE	vs46,vs47,vs39,vs41
  UNPACK_FOR_STORE	vs48,vs49,vs35,vs37
  STORE_COUPLE	\BASE_REG,\LOFFSET,vs39,vs41
  STORE_COUPLE	\BASE_REG,(\LOFFSET+32),vs35,vs37
.endm


.macro SAVE2  VSRes1,VSRes2,VSRes3,VSRes4,BASE_REG,LOFFSET
  RESULT_INTO_REALREAL_IMAGEIMAGE \VSRes1,\VSRes3,vs34,vs35
  LOAD_COUPLE_AS_RR_II	vs46,vs47,vs50,vs51,\BASE_REG,\LOFFSET
  RESULT_INTO_REALIMAG_IMAGREAL	\VSRes2,\VSRes4,vs36,vs37	
  AGGREGATE_REALS_IMAGES	vs34,vs35,vs36,vs37	
  MULT_APLHA_PART1	vs34,vs36, vs46,vs47	
  MULT_APLHA_PART2	vs34,vs36, vs46,vs47  
  UNPACK_FOR_STORE	vs46,vs47,vs39,vs41	
  STORE_COUPLE	\BASE_REG,\LOFFSET,vs39,vs41  
.endm


.macro SAVE1  VSRes1,VSRes2,BASE_REG,LOFFSET
  RESULT_INTO_REALREAL_IMAGEIMAGE \VSRes1,\VSRes1,vs34,vs35
#ifndef TRMMKERNEL 
  lxv	vs50,	(\LOFFSET)(\BASE_REG) 
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
  xxmrghd  vs46,vs50,vs50
  xxmrgld  vs47,vs50,vs50
#else
  xxmrgld  vs46,vs50,vs50
  xxmrghd  vs47,vs50,vs50	
#endif
#endif	
  RESULT_INTO_REALIMAG_IMAGREAL	\VSRes2,\VSRes2,vs36,vs37	
  AGGREGATE_REALS_IMAGES	vs34,vs35,vs36,vs37	
  MULT_APLHA_PART1	vs34,vs36, vs46,vs47	
  MULT_APLHA_PART2	vs34,vs36, vs46,vs47  
  UNPACK_FOR_STORE	vs46,vs47,vs39,vs41 
#if (__BYTE_ORDER__ != __ORDER_BIG_ENDIAN__)
  xxmrghd  vs39,vs47,vs46	
#endif
  stxv	vs39,	(\LOFFSET)(\BASE_REG) 
.endm

/**********************************************************************************************
*

.macros for N=2 and M=8
**********************************************************************************************/

.macro  KERNEL2x8_ZERO_AND_PRIME_MMA
        /* zero out and prime the MMA accumulators */
        xxsetaccz 0
        xxsetaccz 1
        xxsetaccz 2
        xxsetaccz 3
        xxsetaccz 4
        xxsetaccz 5
        xxsetaccz 6
        xxsetaccz 7
.endm


.macro KERNEL2x8_PRELOAD
	lxvp	vs32,	 0(AO)	// load real,imag from A
	lxvp	vs34,	32(AO)	// load real,imag from A
	lxvp	vs36,	64(AO)	// load real,imag from A
	lxvp	vs38,	96(AO)	// load real,imag from A
	lxvp	vs48,	 0(BO)	// load real imag from B
.endm


.macro KERNEL2x8_2	Index, IsLast
	lxvp	vs40,	DISP16(\Index,128)(AO)	// load real,imag from A
	lxvp	vs42,	DISP16(\Index,160)(AO)	// load real,imag from A
	lxvp	vs44,	DISP16(\Index,192)(AO)	// load real,imag from A
	lxvp	vs46,	DISP16(\Index,224)(AO)	// load real,imag from A
 	lxvp	vs50,	DISP4(\Index,  32)(BO)	// load real,imag from B
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
	xvf64gerpp  0,	vs32,	vs48
	xvf64gerpp  1,  vs34,   vs48
	xvf64gerpp  2,  vs36,   vs48
	xvf64gerpp  3,  vs38,   vs48
	xvf64gerpp  4,  vs32,   vs49
	xvf64gerpp  5,  vs34,   vs49
	xvf64gerpp  6,  vs36,   vs49
	xvf64gerpp  7,  vs38,   vs49
#else
	xvf64gerpp	0,	vs32,	vs49
	xvf64gerpp	1,	vs34,	vs49
	xvf64gerpp	2,	vs36,	vs49
	xvf64gerpp	3,	vs38,	vs49
	xvf64gerpp	4,	vs32,	vs48
	xvf64gerpp	5,	vs34,	vs48
	xvf64gerpp	6,	vs36,	vs48
	xvf64gerpp	7,	vs38,	vs48
#endif
	lxvp	vs32,	DISP16(\Index, 256)(AO)	// load real,imag from A
	lxvp	vs34,	DISP16(\Index, 288)(AO)	// load real,imag from A
	lxvp	vs36,	DISP16(\Index, 320)(AO)	// load real,imag from A
	lxvp	vs38,	DISP16(\Index, 352)(AO)	// load real,imag from A
	lxvp	vs48,	DISP4(\Index,  64)(BO)	// load real imag from B
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
	xvf64gerpp  0,  vs40,   vs50
	xvf64gerpp  1,  vs42,   vs50
	xvf64gerpp  2,  vs44,   vs50
	xvf64gerpp  3,  vs46,   vs50
	xvf64gerpp  4,  vs40,   vs51
	xvf64gerpp  5,  vs42,   vs51
	xvf64gerpp  6,  vs44,   vs51
	xvf64gerpp  7,  vs46,   vs51
#else
	xvf64gerpp	0,	vs40,	vs51
	xvf64gerpp	1,	vs42,	vs51
	xvf64gerpp	2,	vs44,	vs51
	xvf64gerpp	3,	vs46,	vs51
	xvf64gerpp	4,	vs40,	vs50
	xvf64gerpp	5,	vs42,	vs50
	xvf64gerpp	6,	vs44,	vs50
	xvf64gerpp	7,	vs46,	vs50
#endif
.if \IsLast==1
	addi	AO, AO,  DISP16(\Index,256)
	addi	BO, BO,  DISP4(\Index,64)
.endif 
.endm


.macro LOAD_END_2x8  OffsetA,OffsetB
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
	xvf64gerpp  0,  vs32,   vs48
	xvf64gerpp  1,  vs34,   vs48
	xvf64gerpp  2,  vs36,   vs48
	xvf64gerpp  3,  vs38,   vs48
	xvf64gerpp  4,  vs32,   vs49
	xvf64gerpp  5,  vs34,   vs49
	xvf64gerpp  6,  vs36,   vs49
	xvf64gerpp  7,  vs38,   vs49	
#else
	xvf64gerpp	0,	vs32,	vs49
	xvf64gerpp	1,	vs34,	vs49
	xvf64gerpp	2,	vs36,	vs49
	xvf64gerpp	3,	vs38,	vs49
	xvf64gerpp	4,	vs32,	vs48
	xvf64gerpp	5,	vs34,	vs48
	xvf64gerpp	6,	vs36,	vs48
	xvf64gerpp	7,	vs38,	vs48
#endif
	addi	BO, BO, \OffsetB
	addi	AO, AO, \OffsetA
.endm


.macro  KERNEL2x8_UNPRIME_MMA
        /* "unprime" MMA accumulators */
        xxmfacc 0
        xxmfacc 1
        xxmfacc 2
        xxmfacc 3
        xxmfacc 4
        xxmfacc 5
        xxmfacc 6
        xxmfacc 7
.endm


.macro SAVE2x8
	add	T1, CO ,LDC 
        xxpermdi vs32, vs0, vs1, 0b01
        xxpermdi vs33, vs0, vs1, 0b10
        xxpermdi vs34, vs2, vs3, 0b01
        xxpermdi vs35, vs2, vs3, 0b10
        xxpermdi vs36, vs4, vs5, 0b01
        xxpermdi vs37, vs4, vs5, 0b10
        xxpermdi vs38, vs6, vs7, 0b01
        xxpermdi vs39, vs6, vs7, 0b10
        xxpermdi vs40, vs8, vs9, 0b01
        xxpermdi vs41, vs8, vs9, 0b10
        xxpermdi vs42, vs10, vs11, 0b01
        xxpermdi vs43, vs10, vs11, 0b10
        xxpermdi vs44, vs12, vs13, 0b01
        xxpermdi vs45, vs12, vs13, 0b10
        xxpermdi vs46, vs14, vs15, 0b01
        xxpermdi vs47, vs14, vs15, 0b10
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
	xxlor vs0, vs32, vs32
	xxlor vs1, vs33, vs33
	xxlor vs2, vs34, vs34
	xxlor vs3, vs35, vs35
	xxlor vs4, vs36, vs36
	xxlor vs5, vs37, vs37
	xxlor vs6, vs38, vs38
	xxlor vs7, vs39, vs39
	xxlor vs8, vs40, vs40
	xxlor vs9, vs41, vs41
	xxlor vs10, vs42, vs42
	xxlor vs11, vs43, vs43
	xxlor vs12, vs44, vs44
	xxlor vs13, vs45, vs45
	xxlor vs14, vs46, vs46
	xxlor vs15, vs47, vs47
#else
	xxlor vs2, vs32, vs32
	xxlor vs3, vs33, vs33
	xxlor vs0, vs34, vs34
	xxlor vs1, vs35, vs35
	xxlor vs6, vs36, vs36
	xxlor vs7, vs37, vs37
	xxlor vs4, vs38, vs38
	xxlor vs5, vs39, vs39
	xxlor vs10, vs40, vs40
	xxlor vs11, vs41, vs41
	xxlor vs8, vs42, vs42
	xxlor vs9, vs43, vs43
	xxlor vs14, vs44, vs44
	xxlor vs15, vs45, vs45
	xxlor vs12, vs46, vs46
	xxlor vs13, vs47, vs47
#endif
        xxpermdi vs32, vs16, vs17, 0b01
        xxpermdi vs33, vs16, vs17, 0b10
        xxpermdi vs34, vs18, vs19, 0b01
        xxpermdi vs35, vs18, vs19, 0b10
        xxpermdi vs36, vs20, vs21, 0b01
        xxpermdi vs37, vs20, vs21, 0b10
        xxpermdi vs38, vs22, vs23, 0b01
        xxpermdi vs39, vs22, vs23, 0b10
        xxpermdi vs40, vs24, vs25, 0b01
        xxpermdi vs41, vs24, vs25, 0b10
        xxpermdi vs42, vs26, vs27, 0b01
        xxpermdi vs43, vs26, vs27, 0b10
        xxpermdi vs44, vs28, vs29, 0b01
        xxpermdi vs45, vs28, vs29, 0b10
        xxpermdi vs46, vs30, vs31, 0b01
        xxpermdi vs47, vs30, vs31, 0b10
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
	xxlor vs16, vs32, vs32
	xxlor vs17, vs33, vs33
	xxlor vs18, vs34, vs34
	xxlor vs19, vs35, vs35
	xxlor vs20, vs36, vs36
	xxlor vs21, vs37, vs37
	xxlor vs22, vs38, vs38
	xxlor vs23, vs39, vs39
	xxlor vs24, vs40, vs40
	xxlor vs25, vs41, vs41
	xxlor vs26, vs42, vs42
	xxlor vs27, vs43, vs43
	xxlor vs28, vs44, vs44
	xxlor vs29, vs45, vs45
	xxlor vs30, vs46, vs46
	xxlor vs31, vs47, vs47
#else
	xxlor vs18, vs32, vs32
	xxlor vs19, vs33, vs33
	xxlor vs16, vs34, vs34
	xxlor vs17, vs35, vs35
	xxlor vs22, vs36, vs36
	xxlor vs23, vs37, vs37
	xxlor vs20, vs38, vs38
	xxlor vs21, vs39, vs39
	xxlor vs26, vs40, vs40
	xxlor vs27, vs41, vs41
	xxlor vs24, vs42, vs42
	xxlor vs25, vs43, vs43
	xxlor vs30, vs44, vs44
	xxlor vs31, vs45, vs45
	xxlor vs28, vs46, vs46
	xxlor vs29, vs47, vs47
#endif
	SAVE8  vs0,vs1,vs2,vs3,vs4,vs5,vs6,vs7,vs8,vs9,vs10,vs11,vs12,vs13,vs14,vs15,CO,0
	SAVE8  vs16,vs17,vs18,vs19,vs20,vs21,vs22,vs23,vs24,vs25,vs26,vs27,vs28,vs29,vs30,vs31,T1,0  
	addi	CO, CO, 128
.endm

/**********************************************************************************************
*

.macros for N=2 and M=4
**********************************************************************************************/

.macro  KERNEL2x4_ZERO_AND_PRIME_MMA
        /* zero out and prime the MMA accumulators */
        xxsetaccz 0
        xxsetaccz 1
        xxsetaccz 2
        xxsetaccz 3
.endm


.macro KERNEL2x4_PRELOAD
	lxvp	vs32,	 0(AO)	// load real,imag from A
	lxvp	vs34,	32(AO)	// load real,imag from A
	lxvp	vs48,	 0(BO)	// load real imag from B
.endm


.macro KERNEL2x4_2 Index, IsLast
	lxvp	vs40,	DISP8(\Index,  64)(AO)	// load real,imag from A
	lxvp	vs42,	DISP8(\Index,  96)(AO)	// load real,imag from A
 	lxvp	vs50,	DISP4(\Index,  32)(BO)  // load real,imag from B
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
    xvf64gerpp      0,      vs32,   vs48
    xvf64gerpp      1,      vs34,   vs48
    xvf64gerpp      2,      vs32,   vs49
    xvf64gerpp      3,      vs34,   vs49
#else
    xvf64gerpp      0,      vs32,   vs49
    xvf64gerpp      1,      vs34,   vs49
    xvf64gerpp      2,      vs32,   vs48
    xvf64gerpp      3,      vs34,   vs48
#endif
	lxvp	vs32,	DISP8(\Index, 128)(AO)	// load real,imag from A
	lxvp	vs34,	DISP8(\Index, 160)(AO)	// load real,imag from A
 	lxvp	vs48,	DISP4(\Index,  64)(BO)  // load real,imag from B
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
    xvf64gerpp      0,      vs40,   vs50
    xvf64gerpp      1,      vs42,   vs50
    xvf64gerpp      2,      vs40,   vs51
    xvf64gerpp      3,      vs42,   vs51
#else
    xvf64gerpp      0,      vs40,   vs51 
    xvf64gerpp      1,      vs42,   vs51
    xvf64gerpp      2,      vs40,   vs50
    xvf64gerpp      3,      vs42,   vs50
#endif
.if \IsLast==1
	addi	AO, AO, DISP8(\Index,128)
	addi	BO, BO, DISP4(\Index,64)
.endif 
.endm
 

.macro LOAD_END_2x4	OffsetA, OffsetB
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
	xvf64gerpp      0,      vs32,   vs48
	xvf64gerpp      1,      vs34,   vs48
	xvf64gerpp      2,      vs32,   vs49
	xvf64gerpp      3,      vs34,   vs49
#else
	xvf64gerpp      0,      vs32,   vs49
	xvf64gerpp      1,      vs34,   vs49
	xvf64gerpp      2,      vs32,   vs48
	xvf64gerpp      3,      vs34,   vs48
#endif
	addi	BO, BO, \OffsetB
	addi	AO, AO, \OffsetA
.endm


.macro  KERNEL2x4_UNPRIME_MMA
        /* "unprime" MMA accumulators */
        xxmfacc 0
        xxmfacc 1
        xxmfacc 2
        xxmfacc 3
.endm


.macro SAVE2x4 
	add	T1, CO ,LDC 
        xxpermdi vs32, vs0, vs1, 0b01
        xxpermdi vs33, vs0, vs1, 0b10
        xxpermdi vs34, vs2, vs3, 0b01
        xxpermdi vs35, vs2, vs3, 0b10
        xxpermdi vs36, vs4, vs5, 0b01
        xxpermdi vs37, vs4, vs5, 0b10
        xxpermdi vs38, vs6, vs7, 0b01
        xxpermdi vs39, vs6, vs7, 0b10
        xxpermdi vs40, vs8, vs9, 0b01
        xxpermdi vs41, vs8, vs9, 0b10
        xxpermdi vs42, vs10, vs11, 0b01
        xxpermdi vs43, vs10, vs11, 0b10
        xxpermdi vs44, vs12, vs13, 0b01
        xxpermdi vs45, vs12, vs13, 0b10
        xxpermdi vs46, vs14, vs15, 0b01
        xxpermdi vs47, vs14, vs15, 0b10
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
	xxlor vs0, vs32, vs32
	xxlor vs1, vs33, vs33
	xxlor vs2, vs34, vs34
	xxlor vs3, vs35, vs35
	xxlor vs4, vs36, vs36 
	xxlor vs5, vs37, vs37
	xxlor vs6, vs38, vs38
	xxlor vs7, vs39, vs39
	xxlor vs8, vs40, vs40
	xxlor vs9, vs41, vs41
	xxlor vs10, vs42, vs42
	xxlor vs11, vs43, vs43
	xxlor vs12, vs44, vs44
	xxlor vs13, vs45, vs45
	xxlor vs14, vs46, vs46
	xxlor vs15, vs47, vs47
#else
	xxlor vs2, vs32, vs32
	xxlor vs3, vs33, vs33
	xxlor vs0, vs34, vs34
	xxlor vs1, vs35, vs35
	xxlor vs6, vs36, vs36
	xxlor vs7, vs37, vs37
	xxlor vs4, vs38, vs38
	xxlor vs5, vs39, vs39
	xxlor vs10, vs40, vs40
	xxlor vs11, vs41, vs41
	xxlor vs8, vs42, vs42
	xxlor vs9, vs43, vs43
	xxlor vs14, vs44, vs44
	xxlor vs15, vs45, vs45
	xxlor vs12, vs46, vs46
	xxlor vs13, vs47, vs47
#endif
	SAVE4  vs0,vs1,vs2,vs3,vs4,vs5,vs6,vs7,CO,0
	SAVE4  vs8,vs9,vs10,vs11,vs12,vs13,vs14,vs15,T1,0  
	addi	CO, CO, 64
.endm

/**********************************************************************************************
*

.macros for N=2 and M=2
**********************************************************************************************/

.macro  KERNEL2x2_ZERO_AND_PRIME_MMA
        /* zero out and prime the MMA accumulators */
        xxsetaccz 0
        xxsetaccz 1
.endm


.macro KERNEL2x2_PRELOAD
	lxvp	vs32,	 0(AO)	// load real,imag from A
	lxvp	vs48,	 0(BO)	// load real imag from B
.endm


.macro KERNEL2x2_2 Index, IsLast
	lxvp	vs40,	DISP4(\Index, 32)(AO)	// load real,imag from A
 	lxvp	vs50,	DISP4(\Index, 32)(BO)	// load real,imag from B
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
	xvf64gerpp	0,	vs32,	vs48
	xvf64gerpp	1,	vs32,	vs49
#else
	xvf64gerpp      0,      vs32,   vs49
	xvf64gerpp      1,      vs32,   vs48
#endif
	lxvp	vs32,	DISP4(\Index, 64)(AO)	// load real,imag from A
	lxvp	vs48,	DISP4(\Index, 64)(BO)	// load real imag from B
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
	xvf64gerpp	0,	vs40,	vs50
	xvf64gerpp	1,	vs40,	vs51
#else
	xvf64gerpp      0,      vs40,   vs51
	xvf64gerpp      1,      vs40,   vs50
#endif
.if \IsLast==1
	addi	AO, AO, DISP4(\Index,64)
	addi	BO, BO, DISP4(\Index,64)
.endif 
.endm

 
.macro LOAD_END_2x2  OffsetA,OffsetB
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
	xvf64gerpp	0,	vs32,	vs48
	xvf64gerpp	1,	vs32,	vs49
#else
	xvf64gerpp      0,      vs32,   vs49
	xvf64gerpp      1,      vs32,   vs48
#endif
	addi	BO, BO, \OffsetB
	addi	AO, AO, \OffsetA
.endm


.macro  KERNEL2x2_UNPRIME_MMA
        /* "unprime" MMA accumulators */
        xxmfacc 0
        xxmfacc 1
.endm


.macro SAVE2x2 
	add	T1, CO ,LDC 
        xxpermdi vs32, vs0, vs1, 0b01
        xxpermdi vs33, vs0, vs1, 0b10
        xxpermdi vs34, vs2, vs3, 0b01
        xxpermdi vs35, vs2, vs3, 0b10
        xxpermdi vs36, vs4, vs5, 0b01
        xxpermdi vs37, vs4, vs5, 0b10
        xxpermdi vs38, vs6, vs7, 0b01
        xxpermdi vs39, vs6, vs7, 0b10
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
	xxlor vs0, vs32, vs32
	xxlor vs1, vs33, vs33
	xxlor vs2, vs34, vs34
	xxlor vs3, vs35, vs35
	xxlor vs4, vs36, vs36
	xxlor vs5, vs37, vs37
	xxlor vs6, vs38, vs38
	xxlor vs7, vs39, vs39
#else
	xxlor vs2, vs32, vs32
	xxlor vs3, vs33, vs33
	xxlor vs0, vs34, vs34
	xxlor vs1, vs35, vs35
	xxlor vs6, vs36, vs36
	xxlor vs7, vs37, vs37
	xxlor vs4, vs38, vs38
	xxlor vs5, vs39, vs39
#endif
	SAVE2  vs0,vs1,vs2,vs3,CO,0
	SAVE2  vs4,vs5,vs6,vs7,T1,0 
	addi	CO, CO, 32 
.endm

/**********************************************************************************************
*

.macros for N=2 and M=1
**********************************************************************************************/

.macro ZERO2x1
	xxlxor	vs0,	vs0,	vs0
	xxlxor	vs1,	vs1,	vs1
	xxlxor	vs2,	vs2,	vs2
	xxlxor	vs3,	vs3,	vs3
 
.endm


.macro LOAD2x1   
	LOAD2x1O 0,0 
.endm


.macro LOAD2x1O  OffsetA,OffsetB
	lxv	vs48,(\OffsetB+	0)(BO)	// load real imag from B
	lxv	vs50,	(\OffsetB+16)(BO)	// load real,imag from B 
	xxswapd	vs49, vs48
	xxswapd	vs51, vs50
	lxv	vs32,	(0+\OffsetA)(AO)	// load real,imag from A 
.endm


.macro END2x1_WITHOUT_ADD
	END2x1 AO,BO,0,0
.endm


.macro END2x1	AREG, BREG, OffsetA, OffsetB
.if \OffsetB != 0
	addi	\BREG, \BREG, \OffsetB
.endif
.if \OffsetA != 0
	addi	\AREG, \AREG, \OffsetA
.endif
	xvmaddadp	vs0,	vs32,	vs48
	xvmaddadp	vs2,	vs32,	vs50
	xvmaddadp	vs1,	vs32,	vs49
	xvmaddadp	vs3,	vs32,	vs51 
.endm


.macro LOAD2x1_2
    LOAD2x1_2O 0,0
.endm	


.macro LOAD2x1_2O  OffsetA,OffsetB
	lxv	vs48,(\OffsetB+	0)(BO)	// load real imag from B
	lxv	vs50,	(\OffsetB+16)(BO)	// load real,imag from B
	lxv	vs52,	(\OffsetB+32)(BO)	// load real,imag	from B
	lxv	vs54,	(\OffsetB+48)(BO)	// load real,imag  from B	
	xxswapd	vs49, vs48
	xxswapd	vs51, vs50
	lxv	vs32,	(0+\OffsetA)(AO)	// load real,imag from A
	lxv	vs40,	(16+\OffsetA)(AO)	// load real,imag from A 
.endm	


.macro END2x1_2	  
  /*for load2 offset will be 32 and 64*/
   KERNEL2x1_2	AO,BO,	32,64,0 ,1,1 
.endm


.macro KERNEL2x1_E2	OffsetA,OffsetB, Index,IsLast 
  KERNEL2x1_2	AO,BO,	\OffsetA,\OffsetB, \Index,\IsLast ,1 
.endm


.macro KERNEL2x1_L2	OffsetA,OffsetB, Index,IsLast
  KERNEL2x1_2	AO,BO,	\OffsetA,\OffsetB, \Index,\IsLast ,0 
.endm


.macro KERNEL2x1_2	AREG,BREG,	OffsetA,OffsetB, Index,IsLast ,Complete
  xxswapd	vs53, vs52
  xxswapd	vs55, vs54 
	xvmaddadp	vs0,	vs32,	vs48
	xvmaddadp	vs2,	vs32,	vs50
	xvmaddadp	vs1,	vs32,	vs49
	xvmaddadp	vs3,	vs32,	vs51
.if \Complete==0	
	lxv	vs32,	DISP2(\Index, 0 + \OffsetA)(\AREG)	// load real,imag from A 
.endif	 
.if \Complete==0		
	lxv	vs48,	DISP4(\Index, 0+\OffsetB)(\BREG)	// load real imag from B
	lxv	vs50,	DISP4(\Index, 16+\OffsetB)(\BREG)	// load real,imag from B
.endif
.if \Complete==0		
  xxswapd	vs49, vs48
  xxswapd	vs51, vs50
.endif 
	xvmaddadp	vs0,	vs40,	vs52
	xvmaddadp	vs2,	vs40,	vs54 
	xvmaddadp	vs1,	vs40,	vs53
	xvmaddadp	vs3,	vs40,	vs55
.if \Complete==0		
	lxv	vs40,	DISP2(\Index,16+0+ \OffsetA)(\AREG)	// load real,imag from A 
.endif
 
.if \Complete==0	 
 	lxv	vs52,	DISP4(\Index, 32+\OffsetB)(\BREG)	// load real,imag	from B
	lxv	vs54,	DISP4(\Index, 48+\OffsetB)(\BREG)	// load real,imag  from B
.endif
.if \IsLast==1
.if \Complete==1
	addi	\AREG, \AREG,  DISP2(\Index,\OffsetA)
	addi	\BREG, \BREG,  DISP4(\Index,\OffsetB)
.else
	addi	\AREG, \AREG, DISP2(\Index,32)
	addi	\BREG, \BREG,  DISP4(\Index,64)
.endif
.endif 
.endm
 

.macro KERNEL2x1
  LOAD2x1
  END2x1  AO, BO, 16,32
.endm


.macro SAVE2x1
	add	T1, CO ,LDC 
	SAVE1  vs0,vs1,CO,0
	SAVE1  vs2,vs3,T1,0  
	addi	CO, CO, 16 
.endm

/**********************************************************************************************
*

.macros for N=1 and M=8
**********************************************************************************************/

.macro  KERNEL1x8_ZERO_AND_PRIME_MMA
        /* zero out and prime the MMA accumulators */
        xxsetaccz 0
        xxsetaccz 1
        xxsetaccz 2
        xxsetaccz 3
.endm


.macro KERNEL1x8_2	Index,IsLast
	lxvp	vs32,	DISP16(\Index,   0)(AO)	// load real,imag from A
	lxvp	vs34,	DISP16(\Index,  32)(AO)	// load real,imag from A
	lxvp	vs36,	DISP16(\Index,  64)(AO)	// load real,imag from A
	lxvp	vs38,	DISP16(\Index,  96)(AO)	// load real,imag from A
	lxvp	vs40,	DISP16(\Index, 128)(AO)	// load real,imag from A
	lxvp	vs42,	DISP16(\Index, 160)(AO)	// load real,imag from A
	lxvp	vs44,	DISP16(\Index, 192)(AO)	// load real,imag from A
	lxvp	vs46,	DISP16(\Index, 224)(AO)	// load real,imag from A
	lxvp	vs48,	DISP2(\Index,    0)(BO)	// load real imag from B
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
	xvf64gerpp	0,	vs32,	vs48
	xvf64gerpp	1,	vs34,	vs48
	xvf64gerpp	2,	vs36,	vs48
	xvf64gerpp	3,	vs38,	vs48
	xvf64gerpp	0,	vs40,	vs49
	xvf64gerpp	1,	vs42,	vs49
	xvf64gerpp	2,	vs44,	vs49
	xvf64gerpp	3,	vs46,	vs49
#else
	xvf64gerpp      0,      vs32,   vs49
	xvf64gerpp      1,      vs34,   vs49
	xvf64gerpp      2,      vs36,   vs49
	xvf64gerpp      3,      vs38,   vs49
	xvf64gerpp      0,      vs40,   vs48
	xvf64gerpp      1,      vs42,   vs48
	xvf64gerpp      2,      vs44,   vs48
	xvf64gerpp      3,      vs46,   vs48
#endif
.if \IsLast==1
	addi	AO, AO, DISP16(\Index,256)
	addi	BO, BO,  DISP2(\Index,32)
.endif 
.endm


.macro LOAD_END_1x8  OffsetA,OffsetB
	lxvp	vs32,	0(AO)	// load real,imag from A
	lxvp	vs34,	32(AO)	// load real,imag from A
	lxvp	vs36,	64(AO)	// load real,imag from A
	lxvp	vs38,	96(AO)	// load real,imag from A
	lxv	vs48,	0(BO)	// load real imag from B 
        xvf64gerpp      0,      vs32,   vs48
        xvf64gerpp      1,      vs34,   vs48
        xvf64gerpp      2,      vs36,   vs48
        xvf64gerpp      3,      vs38,   vs48
	addi	BO, BO, \OffsetB
	addi	AO, AO, \OffsetA
.endm


.macro  KERNEL1x8_UNPRIME_MMA
        /* "unprime" MMA accumulators */
        xxmfacc 0
        xxmfacc 1
        xxmfacc 2
        xxmfacc 3
.endm


.macro SAVE1x8
        xxpermdi vs32, vs0, vs1, 0b01
        xxpermdi vs33, vs0, vs1, 0b10
        xxpermdi vs34, vs2, vs3, 0b01
        xxpermdi vs35, vs2, vs3, 0b10
        xxpermdi vs36, vs4, vs5, 0b01
        xxpermdi vs37, vs4, vs5, 0b10
        xxpermdi vs38, vs6, vs7, 0b01
        xxpermdi vs39, vs6, vs7, 0b10
        xxpermdi vs40, vs8, vs9, 0b01
        xxpermdi vs41, vs8, vs9, 0b10
        xxpermdi vs42, vs10, vs11, 0b01
        xxpermdi vs43, vs10, vs11, 0b10
        xxpermdi vs44, vs12, vs13, 0b01
        xxpermdi vs45, vs12, vs13, 0b10
        xxpermdi vs46, vs14, vs15, 0b01
        xxpermdi vs47, vs14, vs15, 0b10
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
	xxlor vs0, vs32, vs32
	xxlor vs1, vs33, vs33
	xxlor vs2, vs34, vs34
	xxlor vs3, vs35, vs35
	xxlor vs4, vs36, vs36
	xxlor vs5, vs37, vs37
	xxlor vs6, vs38, vs38
	xxlor vs7, vs39, vs39
	xxlor vs8, vs40, vs40
	xxlor vs9, vs41, vs41
	xxlor vs10, vs42, vs42
	xxlor vs11, vs43, vs43
	xxlor vs12, vs44, vs44
	xxlor vs13, vs45, vs45
	xxlor vs14, vs46, vs46
	xxlor vs15, vs47, vs47
#else
	xxlor vs2, vs32, vs32
	xxlor vs3, vs33, vs33
	xxlor vs0, vs34, vs34
	xxlor vs1, vs35, vs35
	xxlor vs6, vs36, vs36
	xxlor vs7, vs37, vs37
	xxlor vs4, vs38, vs38
	xxlor vs5, vs39, vs39
	xxlor vs10, vs40, vs40
	xxlor vs11, vs41, vs41
	xxlor vs8, vs42, vs42
	xxlor vs9, vs43, vs43
	xxlor vs14, vs44, vs44
	xxlor vs15, vs45, vs45
	xxlor vs12, vs46, vs46
	xxlor vs13, vs47, vs47
#endif
	SAVE8  vs0,vs1,vs2,vs3,vs4,vs5,vs6,vs7,vs8,vs9,vs10,vs11,vs12,vs13,vs14,vs15,CO,0
	addi	CO, CO, 128
.endm

/**********************************************************************************************
*

.macros for N=1 and M=4
**********************************************************************************************/

.macro  KERNEL1x4_ZERO_AND_PRIME_MMA
        /* zero out and prime the MMA accumulators */
        xxsetaccz 0
        xxsetaccz 1
.endm


.macro KERNEL1x4_2	Index,IsLast
	lxvp	vs32,	DISP8(\Index,  0)(AO)	// load real,imag from A
	lxvp	vs34,	DISP8(\Index, 32)(AO)	// load real,imag from A
	lxvp	vs40,	DISP8(\Index, 64)(AO)	// load real,imag from A
	lxvp	vs42,	DISP8(\Index, 96)(AO)	// load real,imag from A
	lxvp	vs48,	DISP2(\Index,  0)(BO)	// load real imag from B
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
	xvf64gerpp	0,	vs32,	vs48
	xvf64gerpp	1,	vs34,	vs48
	xvf64gerpp	0,	vs40,	vs49
	xvf64gerpp	1,	vs42,	vs49
#else
	xvf64gerpp      0,      vs32,   vs49
	xvf64gerpp      1,      vs34,   vs49
	xvf64gerpp      0,      vs40,   vs48
	xvf64gerpp      1,      vs42,   vs48
#endif
.if \IsLast==1
	addi	AO, AO, DISP8(\Index,128)
	addi	BO, BO,  DISP2(\Index,32)
.endif 
.endm
 

.macro LOAD_END_1x4  OffsetA,OffsetB
	lxvp	vs32,	0(AO)	// load real,imag from A
	lxvp	vs34,	32(AO)	// load real,imag from A
	lxv	vs48,	0(BO)	// load real imag from B
        xvf64gerpp      0,      vs32,   vs48
        xvf64gerpp      1,      vs34,   vs48
	addi	BO, BO, \OffsetB
	addi	AO, AO, \OffsetA
.endm


.macro  KERNEL1x4_UNPRIME_MMA
        /* "unprime" MMA accumulators */
        xxmfacc 0
        xxmfacc 1
.endm


.macro SAVE1x4 
        xxpermdi vs32, vs0, vs1, 0b01
        xxpermdi vs33, vs0, vs1, 0b10
        xxpermdi vs34, vs2, vs3, 0b01
        xxpermdi vs35, vs2, vs3, 0b10
        xxpermdi vs36, vs4, vs5, 0b01
        xxpermdi vs37, vs4, vs5, 0b10
        xxpermdi vs38, vs6, vs7, 0b01
        xxpermdi vs39, vs6, vs7, 0b10
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
	xxlor vs0, vs32, vs32
	xxlor vs1, vs33, vs33
	xxlor vs2, vs34, vs34
	xxlor vs3, vs35, vs35
	xxlor vs4, vs36, vs36
	xxlor vs5, vs37, vs37
	xxlor vs6, vs38, vs38
	xxlor vs7, vs39, vs39
#else
	xxlor vs2, vs32, vs32
	xxlor vs3, vs33, vs33
	xxlor vs0, vs34, vs34
	xxlor vs1, vs35, vs35
	xxlor vs6, vs36, vs36
	xxlor vs7, vs37, vs37
	xxlor vs4, vs38, vs38
	xxlor vs5, vs39, vs39
#endif
	SAVE4  vs0,vs1,vs2,vs3,vs4,vs5,vs6,vs7,CO,0
	addi	CO, CO, 64
.endm

/**********************************************************************************************
*

.macros for N=1 and M=2
**********************************************************************************************/

.macro  KERNEL1x2_ZERO_AND_PRIME_MMA
        /* zero out and prime the MMA accumulators */
        xxsetaccz 0
.endm


.macro KERNEL1x2_2	Index,IsLast
	lxvp	vs32,	DISP4(\Index,  0)(AO)	// load real,imag from A
	lxvp	vs40,	DISP4(\Index, 32)(AO)	// load real,imag from A
	lxvp	vs48,	DISP2(\Index,  0)(BO)	// load real imag from B
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
	xvf64gerpp	0,	vs32,	vs48
	xvf64gerpp	0,	vs40,	vs49
#else
	xvf64gerpp      0,      vs32,   vs49
	xvf64gerpp      0,      vs40,   vs48
#endif
.if \IsLast==1
	addi	AO, AO, DISP4(\Index,64)
	addi	BO, BO, DISP2(\Index,32)
.endif 
.endm
 

.macro LOAD_END_1x2  OffsetA,OffsetB
	lxvp	vs32,	0(AO)	// load real,imag from A
	lxv	vs48,	0(BO)	// load real imag from B
        xvf64gerpp      0,      vs32,   vs48
	addi	BO, BO, \OffsetB
	addi	AO, AO, \OffsetA
.endm


.macro  KERNEL1x2_UNPRIME_MMA
        /* "unprime" MMA accumulators */
        xxmfacc 0
.endm


.macro SAVE1x2 
        xxpermdi vs32, vs0, vs1, 0b01
        xxpermdi vs33, vs0, vs1, 0b10
        xxpermdi vs34, vs2, vs3, 0b01
        xxpermdi vs35, vs2, vs3, 0b10
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
	xxlor vs0, vs32, vs32
	xxlor vs1, vs33, vs33
	xxlor vs2, vs34, vs34
	xxlor vs3, vs35, vs35
#else
	xxlor vs2, vs32, vs32
	xxlor vs3, vs33, vs33
	xxlor vs0, vs34, vs34
	xxlor vs1, vs35, vs35
#endif

	SAVE2  vs0,vs1,vs2,vs3,CO,0
	addi	CO, CO, 32 
.endm

/**********************************************************************************************
*

.macros for N=1 and M=1
**********************************************************************************************/

.macro ZERO1x1
	xxlxor	vs0,	vs0,	vs0
	xxlxor	vs1,	vs1,	vs1 
.endm


.macro LOAD1x1   
	LOAD1x1O 0,0 
.endm


.macro LOAD1x1O  OffsetA,OffsetB
	lxv	vs48,(\OffsetB+	0)(BO)	// load real imag from B
	lxv	vs32,	(0+\OffsetA)(AO)	// load real,imag from A 
	xxswapd	vs49, vs48

.endm


.macro END1x1_WITHOUT_ADD
	END1x1 AO,BO,0,0
.endm


.macro END1x1	AREG, BREG, OffsetA, OffsetB
.if \OffsetB != 0
	addi	\BREG, \BREG, \OffsetB
.endif
.if \OffsetA != 0
	addi	\AREG, \AREG, \OffsetA
.endif
	xvmaddadp	vs0,	vs32,	vs48 
	xvmaddadp	vs1,	vs32,	vs49 
.endm


.macro LOAD1x1_2
    LOAD1x1_2O 0,0
.endm	


.macro LOAD1x1_2O  OffsetA,OffsetB
	lxv	vs48,(\OffsetB+	0)(BO)	// load real imag from B
	lxv	vs52,	(\OffsetB+16)(BO)	// load real,imag	from B
	xxswapd	vs49, vs48

	lxv	vs32,	(0+\OffsetA)(AO)	// load real,imag from A
	lxv	vs40,	(16+\OffsetA)(AO)	// load real,imag from A 
.endm	


.macro END1x1_2	  
  /*for load2 offset will be 32 and 32*/
   KERNEL1x1_2	AO,BO,	32,32,0 ,1,1 
.endm
 


.macro KERNEL1x1_E2	OffsetA,OffsetB, Index,IsLast 
  KERNEL1x1_2	AO,BO,	\OffsetA,\OffsetB, \Index,\IsLast ,1 
.endm


.macro KERNEL1x1_L2	OffsetA,OffsetB, Index,IsLast
  KERNEL1x1_2	AO,BO,	\OffsetA,\OffsetB, \Index,\IsLast ,0 
.endm


.macro KERNEL1x1_2	AREG,BREG,	OffsetA,OffsetB, Index,IsLast ,Complete
  xxswapd	vs53, vs52
	xvmaddadp	vs0,	vs32,	vs48 
	xvmaddadp	vs1,	vs32,	vs49 
.if \Complete==0	
	lxv	vs32,	DISP2(\Index, 0 + \OffsetA)(\AREG)	// load real,imag from A 
.endif	 
.if \Complete==0		
	lxv	vs48,	DISP2(\Index, 0+\OffsetB)(\BREG)	// load real imag from B
.endif
.if \Complete==0		
  xxswapd	vs49, vs48
.endif 
	xvmaddadp	vs0,	vs40,	vs52
	xvmaddadp	vs1,	vs40,	vs53 
.if \Complete==0		
	lxv	vs40,	DISP2(\Index,16+0+ \OffsetA)(\AREG)	// load real,imag from A 
.endif
 
.if \Complete==0	 
 	lxv	vs52,	DISP2(\Index, 16+\OffsetB)(\BREG)	// load real,imag	from B
.endif
.if \IsLast==1
.if \Complete==1
	addi	\AREG, \AREG,  DISP2(\Index,\OffsetA)
	addi	\BREG, \BREG,  DISP2(\Index,\OffsetB)
.else
	addi	\AREG, \AREG, DISP2(\Index,32)
	addi	\BREG, \BREG,  DISP2(\Index,32)
.endif
.endif 
.endm
 


.macro KERNEL1x1
  LOAD1x1
  END1x1  AO, BO, 16,16
.endm



.macro SAVE1x1
	SAVE1  vs0,vs1,CO,0
	addi	CO, CO, 16 
.endm

/****************************TRMM POINTER REFRESH

.macroSES*************************/


.macro SHIFT_REG  REG1,REG2,SHIFT_VAL
		.if \SHIFT_VAL==16 
			slwi		\REG1,	\REG2,	8			
		.elseif \SHIFT_VAL==8  
			slwi		\REG1,	\REG2,	7			 
		.elseif \SHIFT_VAL==4
			slwi		\REG1,	\REG2,	6			  
		.elseif \SHIFT_VAL==2
			slwi		\REG1,	\REG2,	5			 
		.elseif \SHIFT_VAL==1
			slwi		\REG1,	\REG2,	4			 
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

