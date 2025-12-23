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
 
#define unit_size 4
#define DISP64(ind,disp) (ind*unit_size*64+disp)
#define DISP32(ind,disp) (ind*unit_size*32+disp)
#define DISP16(ind,disp) (ind*unit_size*16+disp)
#define DISP8(ind,disp) (ind*unit_size*8+disp)
#define DISP4(ind,disp) (ind*unit_size*4+disp)
#define DISP2(ind,disp) (ind*unit_size*2+disp)
#define DISP1(ind,disp) (ind*unit_size+disp)

/**********************************************************************************************
* Macros for N=8 and M=16
**********************************************************************************************/

 

.macro KERNEL8x16_L1_L4  Index,IsLast
  KERNEL8x16_L1_L4_I AO,BO, 0,0, \Index,\IsLast,0
.endm

.macro KERNEL8x16_I1_L4  OffsetA,OffsetB, Index,IsLast
  KERNEL8x16_L1_L4_I  AO,BO, \OffsetA,\OffsetB,\Index,\IsLast,0
.endm

.macro KERNEL8x16_I1_L4_2  OffsetA,OffsetB, Index,IsLast
  KERNEL8x16_L1_L4_I  AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast,0
.endm

.macro KERNEL8x16_I1_L4_3  OffsetA,OffsetB, Index,IsLast
  KERNEL8x16_L1_L4_I  AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast,1
.endm
 
.macro KERNEL8x16_I2_L4_2  AREG,BREG,OffsetA,OffsetB, Index,IsLast
  KERNEL8x16_L1_L4_I  \AREG,\BREG,  \OffsetA,\OffsetB,\Index,\IsLast,0
.endm

.macro KERNEL8x16_I2_L4_3  AREG,BREG,OffsetA,OffsetB, Index,IsLast
  KERNEL8x16_L1_L4_I \AREG,\BREG,  \OffsetA,\OffsetB,\Index,\IsLast,1
.endm

.macro Zero8X16
    xxlxor		vs32,	vs32,	vs32
    xxlxor		vs33,	vs33,	vs33
	xxlxor		vs34,	vs34,	vs34
	xxlxor		vs35,	vs35,	vs35
	xxlxor		vs36,	vs36,	vs36
	xxlxor		vs37,	vs37,	vs37
	xxlxor		vs38,	vs38,	vs38
	xxlxor		vs39,	vs39,	vs39
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
.endm

.macro LOAD8x16  OffsetA,OffsetB

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

.macro END8x16_NORMAL
  END8x16 0, AO, BO, 64,32 
.endm

.macro END8x16_WITHOUT_ADD
	END8x16 0, AO,BO,0,0
.endm

.macro END8x16 First, AREG, BREG, OffsetA, OffsetB

.if \OffsetB != 0 
    addi        \BREG, \BREG, \OffsetB 
.endif
.if \OffsetA != 0 
    addi        \AREG, \AREG, \OffsetA 
.endif  

.if \First==1
    xvmulsp     vs32, vs0,vs24
    xvmulsp     vs33, vs1,vs24
    xvmulsp     vs34, vs2,vs24  
    xvmulsp     vs35, vs3,vs24  

    xvmulsp     vs36, vs0,vs25
    xvmulsp     vs37, vs1,vs25
    xvmulsp     vs38, vs2,vs25  
    xvmulsp     vs39, vs3,vs25

    xvmulsp     vs40, vs0,vs26
    xvmulsp     vs41, vs1,vs26
    xvmulsp     vs42, vs2,vs26  
    xvmulsp     vs43, vs3,vs26

    xvmulsp     vs44, vs0,vs27
    xvmulsp     vs45, vs1,vs27
    xvmulsp     vs46, vs2,vs27  
    xvmulsp     vs47, vs3,vs27

    xvmulsp     vs48, vs0,vs28
    xvmulsp     vs49, vs1,vs28
    xvmulsp     vs50, vs2,vs28  
    xvmulsp     vs51, vs3,vs28  

    xvmulsp     vs52, vs0,vs29
    xvmulsp     vs53, vs1,vs29
    xvmulsp     vs54, vs2,vs29  
    xvmulsp     vs55, vs3,vs29

    xvmulsp     vs56, vs0,vs30
    xvmulsp     vs57, vs1,vs30
    xvmulsp     vs58, vs2,vs30  
    xvmulsp     vs59, vs3,vs30

    xvmulsp     vs60, vs0,vs31
    xvmulsp     vs61, vs1,vs31
    xvmulsp     vs62, vs2,vs31  
    xvmulsp     vs63, vs3,vs31

.else
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

.endif
.endm  

.macro KERNEL8x16_L1_L4_I  AREG,BREG,   OffsetA,OffsetB, Index,IsLast ,Complete

KERNEL8x16_2  \AREG,\BREG, \OffsetA,\OffsetB, (\Index*2),0 ,0
KERNEL8x16_2  \AREG,\BREG,\OffsetA,\OffsetB, (\Index*2+1),\IsLast ,\Complete

.endm

.macro KERNEL8x16 First

  LOAD8x16 0,0
  END8x16 \First, AO, BO, 64,32 
.endm

.macro LOAD8x16_2
    LOAD8x16_2O AO,BO, 0,0
.endm	

.macro LOAD8x16_2O  AREG,BREG, OffsetA,OffsetB
  lxv	vs8,	(\OffsetB)(\BREG)
  lxv	vs12,	(16+\OffsetB)(\BREG)
  lxv	vs24,	(32+\OffsetB)(\BREG)
  lxv	vs28,	(32+16+\OffsetB)(\BREG)
  lxv	vs4,	(0+\OffsetA)(\AREG)
  lxv	vs5,	(16+\OffsetA)(\AREG)
  xxperm  	vs10,	vs8,		permute_mask
  xxperm  	vs14,	vs12,		permute_mask	
  lxv	vs6,	(32+\OffsetA)(\AREG)
  lxv	vs7,	(48+\OffsetA)(\AREG) 
  xxpermdi	vs9,	vs8,	 vs8,2	 
  xxpermdi	vs13,	vs12,	vs12,2	 
  lxv	vs0,	(64+\OffsetA)(\AREG)
  lxv	vs1,	(64+16+\OffsetA)(\AREG) 
  xxpermdi	vs11,	vs10,	vs10,2	
  xxpermdi	vs15,	vs14,	vs14,2	
  lxv	vs2,	(64+32+\OffsetA)(\AREG)
  lxv	vs3,	(64+48+\OffsetA)(\AREG)

  xxperm  	vs26,	vs24,	permute_mask
  xxperm  	vs30,	vs28,	permute_mask	
  xxpermdi	vs25,	vs24,	vs24,2 
  xxpermdi	vs29,	vs28,	vs28,2	      
  xxpermdi	vs27,	vs26,	vs26,2	
  xxpermdi	vs31,	vs30,	vs30,2	 
.endm

.macro END8x16_2	  
  /*for load2 offset will be 128 and 64*/
   KERNEL8x16_2	AO,BO,	128,64,0 ,1,1 
.endm
 


.macro KERNEL8x16_E2	OffsetA,OffsetB, Index,IsLast 
  KERNEL8x16_2	AO,BO,	\OffsetA,\OffsetB, \Index,\IsLast ,1 
.endm


.macro KERNEL8x16_L2	OffsetA,OffsetB, Index,IsLast
  KERNEL8x16_2	AO,BO,	\OffsetA,\OffsetB, \Index,\IsLast ,0 
.endm


.macro KERNEL8x16_2  AREG,BREG,  OffsetA,OffsetB, Index,IsLast ,Complete
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
   lxv	vs4,	DISP32(\Index,0+\OffsetA)(\AREG)
   lxv	vs5,	DISP32(\Index,16+\OffsetA)(\AREG)
.endif

  xvmaddasp		vs34, vs6,vs8	
  xvmaddasp		vs35, vs7,vs8	
  xvmaddasp		vs50, vs6,vs12
  xvmaddasp		vs51, vs7,vs12
.if \Complete==0  
  lxv vs8,  DISP16(\Index,\OffsetB)(\BREG)
  lxv vs12, DISP16(\Index,16+\OffsetB)(\BREG)
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
   lxv	vs6,	DISP32(\Index,32+\OffsetA)(\AREG)
   lxv	vs7,	DISP32(\Index,48+\OffsetA)(\AREG) 
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
  lxv	vs0,	DISP32(\Index,64+\OffsetA)(\AREG)
  lxv	vs1,	DISP32(\Index,64+16+\OffsetA)(\AREG) 
.endif

  xvmaddasp		vs34, vs2,vs24
  xvmaddasp		vs35, vs3,vs24	  
  xvmaddasp		vs50, vs2,vs28
  xvmaddasp		vs51, vs3,vs28
.if \Complete==0
  lxv vs24, DISP16(\Index,32+\OffsetB)(\BREG)
  lxv vs28, DISP16(\Index,32+16+\OffsetB)(\BREG)
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
  lxv	vs2,	DISP32(\Index,64+32+\OffsetA)(\AREG)
  lxv	vs3,	DISP32(\Index,64+48+\OffsetA)(\AREG)
.endif


.if \IsLast==1	
.if \Complete==1
	addi		\BREG, \BREG,  DISP16(\Index,\OffsetB)
  addi    \AREG, \AREG, DISP32(\Index,\OffsetA)  

.else
	addi		\BREG, \BREG,  DISP16(\Index,64)
  addi    \AREG, \AREG, DISP32(\Index,128)  

.endif
.endif   


.endm

 
.macro SAVE8x16

  slwi    T10, LDC ,   1 
  add     T1, CO, LDC 

  add     T2, CO, T10  
  add     T3, T1, T10  

  add     T4, T2, T10  
  add     T5, T3, T10 

  add     T6, T4, T10 
  add     T7, T5, T10 



   /* permute to restore butterfly rank 1 updateto normal promoted one */  
    /* permute 16 vs8 MEM(CO) vs9 MEM(CO+LDC) vs10 MEM(CO+2*LDC)  vs11 MEM(CO+3*LDC) */
    /* permute 16 vs12 MEM(16+CO) vs13 MEM(16+CO+LDC) vs14 MEM(16+CO+2*LDC)  vs15 MEM(16+CO+3*LDC) */
    /* permute 16 vs16 MEM(32+CO) vs17 MEM(32+CO+LDC) vs18 MEM(32+CO+2*LDC)  vs19 MEM(32+CO+3*LDC) */
    /* permute 16 vs24 MEM(32+CO) vs25 MEM(32+CO+LDC) vs26 MEM(32+CO+2*LDC)  vs27 MEM(32+CO+3*LDC) */

    xxmrglw     vs8,    vs32,   vs44
    xxmrglw     vs10,   vs36,   vs40  

    xxmrghw     vs1,    vs32,   vs44
    xxmrghw     vs0,    vs36,   vs40

    xxmrglw     vs12,   vs33,   vs45
    xxmrglw     vs14,   vs37,   vs41  

    xxmrghw     vs2,    vs37,   vs41
    xxmrghw     vs3,    vs33,   vs45
#ifndef TRMMKERNEL    
    lxv        vs32, 0(CO)
    lxv        vs33, 16(CO) 
#endif 
    xxmrglw     vs16,   vs34,   vs46
    xxmrglw     vs18,   vs38,   vs42   

    xxlor      vs9, vs8,    vs8
    xxlor      vs11,    vs10,   vs10 

    xxmrghw     vs4,    vs38,   vs42
    xxmrghw     vs5,    vs34,   vs46

    xxlor      vs13,    vs12,   vs12
    xxlor      vs15,    vs14,   vs14

    xxmrglw     vs24,   vs35,   vs47
    xxmrglw     vs26,   vs39,   vs43  

    xxlor      vs17,    vs16,   vs16
    xxlor      vs19,    vs18,   vs18

    xxmrghw     vs30,   vs39,   vs43 
    xxmrghw     vs31,   vs35,   vs47
#ifndef TRMMKERNEL       
    lxv        vs34, 32(CO)  
    lxv        vs35, 48(CO)      
#endif
    xxperm      vs8,    vs0,    save_permute_1
    xxperm      vs10,   vs1,    save_permute_1
#ifndef TRMMKERNEL    
    lxv        vs36, 0(T1)
    lxv        vs37, 16(T1) 
#endif
    xxperm      vs9,    vs0,    save_permute_2  
    xxperm      vs11,   vs1,    save_permute_2      

#ifndef TRMMKERNEL      
    lxv        vs38, 32(T1)  
    lxv        vs39, 48(T1)     
#endif

    xxlor      vs25,    vs24,   vs24
    xxlor      vs27,    vs26,   vs26 



#ifndef TRMMKERNEL       
    lxv        vs40, 0(T2)
    lxv        vs41, 16(T2) 
#endif

    xxperm     vs12,    vs2,    save_permute_1
    xxperm     vs14,    vs3,    save_permute_1
#ifndef TRMMKERNEL     
    lxv        vs42, 32(T2)  
    lxv        vs43, 48(T2)     
#endif  
       
    xxperm     vs13,    vs2,    save_permute_2   
    xxperm     vs15,    vs3,    save_permute_2  
#ifndef TRMMKERNEL    
    lxv        vs44, 0(T3)
    lxv        vs45, 16(T3)
#endif
    xxperm     vs16,    vs4,    save_permute_1
    xxperm     vs18,    vs5,    save_permute_1
#ifndef TRMMKERNEL      
    lxv        vs46, 32(T3)  
    lxv        vs47, 48(T3)                 
#endif  

    


      
    xxperm     vs17,    vs4,    save_permute_2   
    xxperm     vs19,    vs5,    save_permute_2      
#ifdef TRMMKERNEL
    xvmulsp     vs32,   vs8,    alpha_r 
    xvmulsp     vs33,   vs12,   alpha_r                 
#else 
    xvmaddasp   vs32,   vs8,    alpha_r 
    xvmaddasp   vs33,   vs12,   alpha_r            
#endif 
    xxperm     vs24,    vs30,   save_permute_1
    xxperm     vs26,    vs31,   save_permute_1 

 
    stxv        vs32, 0(CO)
    stxv        vs33, 16(CO)     
#ifdef TRMMKERNEL   
    xvmulsp     vs34,   vs16,   alpha_r 
    xvmulsp     vs35,   vs24,   alpha_r                 
#else    
    xvmaddasp   vs34,   vs16,   alpha_r 
    xvmaddasp   vs35,   vs24,   alpha_r           
#endif 
         
    xxperm     vs25,    vs30,   save_permute_2   
    xxperm     vs27,    vs31,   save_permute_2  


    stxv        vs34, 32(CO)  
    stxv        vs35, 48(CO)  
#ifdef TRMMKERNEL  
    xvmulsp     vs36,   vs9,    alpha_r 
    xvmulsp     vs37,   vs13,   alpha_r                
#else   
    xvmaddasp   vs36,   vs9,    alpha_r 
    xvmaddasp   vs37,   vs13,   alpha_r           
#endif 
    stxv        vs36, 0(T1)
    stxv        vs37, 16(T1)
#ifdef TRMMKERNEL  
    xvmulsp     vs38,   vs17,   alpha_r 
    xvmulsp     vs39,   vs25,   alpha_r               
#else   
    xvmaddasp   vs38,   vs17,   alpha_r 
    xvmaddasp   vs39,   vs25,   alpha_r         
#endif 
    stxv        vs38, 32(T1)  
    stxv        vs39, 48(T1)

#ifdef TRMMKERNEL
    xvmulsp     vs40,   vs10,   alpha_r 
    xvmulsp     vs41,   vs14,   alpha_r                    
#else 
    xvmaddasp   vs40,   vs10,   alpha_r 
    xvmaddasp   vs41,   vs14,   alpha_r   
#endif   

    stxv        vs40, 0(T2)
    stxv        vs41, 16(T2)  
#ifdef TRMMKERNEL 
    xvmulsp     vs42,   vs18,   alpha_r 
    xvmulsp     vs43,   vs26,   alpha_r                     
#else   
    xvmaddasp   vs42,   vs18,   alpha_r 
    xvmaddasp   vs43,   vs26,   alpha_r
#endif      
    stxv        vs42, 32(T2)  
    stxv        vs43, 48(T2)  
#ifdef TRMMKERNEL  
    xvmulsp     vs44,   vs11,   alpha_r 
    xvmulsp     vs45,   vs15,   alpha_r                    
#else
    xvmaddasp   vs44,   vs11,   alpha_r 
    xvmaddasp   vs45,   vs15,   alpha_r    
#endif      
    stxv        vs44, 0(T3)
    stxv        vs45, 16(T3) 
#ifdef TRMMKERNEL 
    xvmulsp     vs46,   vs19,   alpha_r 
    xvmulsp     vs47,   vs27,   alpha_r                   
#else 
    xvmaddasp   vs46,   vs19,   alpha_r 
    xvmaddasp   vs47,   vs27,   alpha_r 
#endif      
    stxv        vs46, 32(T3)  
    stxv        vs47, 48(T3)
  
 /*****the same with the second 8X8 ****/
 #ifndef TRMMKERNEL 
    lxv        vs32, 0(T4)
    lxv        vs33, 16(T4) 
#endif  
    xxmrglw     vs8,    vs48,   vs60
    xxmrglw     vs10,   vs52,   vs56  
#ifndef TRMMKERNEL    
    lxv        vs34, 32(T4)  
    lxv        vs35, 48(T4)  
#endif  
    xxmrghw     vs1,    vs48,   vs60
    xxmrghw     vs0,    vs52,   vs56
#ifndef TRMMKERNEL        
    lxv        vs36, 0(T5)
    lxv        vs37, 16(T5) 
#endif  
    xxmrglw     vs12,   vs49,   vs61
    xxmrglw     vs14,   vs53,   vs57  
#ifndef TRMMKERNEL    
    lxv        vs38,32(T5)  
    lxv        vs39, 48(T5)     
#endif   
 
    xxmrghw     vs2,    vs53,   vs57
    xxmrghw     vs3,    vs49,   vs61
#ifndef TRMMKERNEL   
    lxv        vs40, 0(T6)
    lxv        vs41, 16(T6)
#endif  
    xxmrglw     vs16,   vs50,   vs62
    xxmrglw     vs18,   vs54,   vs58   
#ifndef TRMMKERNEL      
    lxv        vs42, 32(T6)  
    lxv        vs43, 48(T6) 
#endif  
    xxlor      vs9, vs8,    vs8
    xxlor      vs11,    vs10,   vs10 
    xxmrghw     vs4,    vs54,   vs58
    xxmrghw     vs5,    vs50,   vs62
#ifndef TRMMKERNEL              
    lxv        vs44, 0(T7)
    lxv        vs45, 16(T7) 
#endif  
    xxlor      vs13,    vs12,   vs12
    xxlor      vs15,    vs14,   vs14
 
    xxmrglw     vs24,   vs51,   vs63
    xxmrglw     vs26,   vs55,   vs59 
#ifndef TRMMKERNEL    
    lxv        vs46, 32(T7)  
    lxv        vs47, 48(T7)     
#endif  
    xxlor      vs17,    vs16,   vs16
    xxlor      vs19,    vs18,   vs18
    xxmrghw     vs30,   vs55,   vs59 
    xxmrghw     vs31,   vs51,   vs63 

 

    xxperm      vs8,    vs0,    save_permute_1
    xxperm      vs10,   vs1,    save_permute_1
     
    xxperm      vs9,    vs0,    save_permute_2  
    xxperm      vs11,   vs1,    save_permute_2      

    xxlor      vs25,    vs24,   vs24
    xxlor      vs27,    vs26,   vs26 
    xxperm     vs12,    vs2,    save_permute_1
    xxperm     vs14,    vs3,    save_permute_1

    xxperm     vs13,    vs2,    save_permute_2   
    xxperm     vs15,    vs3,    save_permute_2      
 #ifdef TRMMKERNEL
    xvmulsp     vs32,   vs8,    alpha_r 
    xvmulsp     vs33,   vs12,   alpha_r                 
#else 
    xvmaddasp   vs32,   vs8,    alpha_r 
    xvmaddasp   vs33,   vs12,   alpha_r            
#endif  
    xxperm     vs16,    vs4,    save_permute_1
    xxperm     vs18,    vs5,    save_permute_1
    stxv        vs32, 0(T4)
    stxv        vs33, 16(T4) 
    xxperm     vs17,    vs4,    save_permute_2   
    xxperm     vs19,    vs5,    save_permute_2      
    xxperm     vs24,    vs30,   save_permute_1
    xxperm     vs26,    vs31,   save_permute_1 
    xxperm     vs25,    vs30,   save_permute_2   
    xxperm     vs27,    vs31,   save_permute_2      

#ifdef TRMMKERNEL   
    xvmulsp     vs34,   vs16,   alpha_r 
    xvmulsp     vs35,   vs24,   alpha_r                 
#else    
    xvmaddasp   vs34,   vs16,   alpha_r 
    xvmaddasp   vs35,   vs24,   alpha_r           
#endif 
    stxv        vs34, 32(T4)  
    stxv        vs35, 48(T4)  

#ifdef TRMMKERNEL  
    xvmulsp     vs36,   vs9,    alpha_r 
    xvmulsp     vs37,   vs13,   alpha_r                
#else   
    xvmaddasp   vs36,   vs9,    alpha_r 
    xvmaddasp   vs37,   vs13,   alpha_r           
#endif 
    stxv        vs36, 0(T5)
    stxv        vs37, 16(T5) 

#ifdef TRMMKERNEL  
    xvmulsp     vs38,   vs17,   alpha_r 
    xvmulsp     vs39,   vs25,   alpha_r               
#else  
    xvmaddasp   vs38,   vs17,   alpha_r 
    xvmaddasp   vs39,   vs25,   alpha_r         
#endif 



 
    stxv        vs38, 32(T5)  
    stxv        vs39, 48(T5)


#ifdef TRMMKERNEL
    xvmulsp     vs40,   vs10,   alpha_r 
    xvmulsp     vs41,   vs14,   alpha_r                    
#else 
    xvmaddasp   vs40,   vs10,   alpha_r 
    xvmaddasp   vs41,   vs14,   alpha_r   
#endif  
    stxv        vs40, 0(T6)
    stxv        vs41, 16(T6) 
#ifdef TRMMKERNEL 
    xvmulsp     vs42,   vs18,   alpha_r 
    xvmulsp     vs43,   vs26,   alpha_r                     
#else   
    xvmaddasp   vs42,   vs18,   alpha_r 
    xvmaddasp   vs43,   vs26,   alpha_r
#endif  
    stxv        vs42, 32(T6)  
    stxv        vs43, 48(T6)  
#ifdef TRMMKERNEL  
    xvmulsp     vs44,   vs11,   alpha_r 
    xvmulsp     vs45,   vs15,   alpha_r                    
#else
    xvmaddasp   vs44,   vs11,   alpha_r 
    xvmaddasp   vs45,   vs15,   alpha_r    
#endif  

    stxv        vs44, 0(T7)
    stxv        vs45, 16(T7) 
#ifdef TRMMKERNEL 
    xvmulsp     vs46,   vs19,   alpha_r 
    xvmulsp     vs47,   vs27,   alpha_r                   
#else 
    xvmaddasp   vs46,   vs19,   alpha_r 
    xvmaddasp   vs47,   vs27,   alpha_r 
#endif  
 
    stxv        vs46, 32(T7)  
    stxv        vs47, 48(T7)
  

    addi CO,CO,64


.endm



/**********************************************************************************************
* Macros for N=8 and M=8
**********************************************************************************************/

.macro LOAD8x8_1
   LOAD8x8 1
.endm

.macro LOAD8x8_0
   LOAD8x8 0
.endm

.macro KERNEL8x8_L1_L4  Index,IsLast
  KERNEL8x8_L1_L4_I AO,BO, 0,0, \Index,\IsLast,0
.endm

.macro KERNEL8x8_I1_L4  OffsetA,OffsetB, Index,IsLast
  KERNEL8x8_L1_L4_I  AO,BO, \OffsetA,\OffsetB,\Index,\IsLast,0
.endm

.macro KERNEL8x8_I1_L4_2  OffsetA,OffsetB, Index,IsLast
  KERNEL8x8_L1_L4_I  AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast,0
.endm

.macro KERNEL8x8_I1_L4_3  OffsetA,OffsetB, Index,IsLast
  KERNEL8x8_L1_L4_I  AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast,1
.endm
.macro KERNEL8x8_I1_L2_3  OffsetA,OffsetB, Index,IsLast
   KERNEL8x8_L1_L2_I AO,BO,0,  \OffsetA,\OffsetB,\Index,\IsLast,1
.endm

.macro KERNEL8x8_I2_L4_2  AREG,BREG,OffsetA,OffsetB, Index,IsLast
  KERNEL8x8_L1_L4_I  \AREG,\BREG,  \OffsetA,\OffsetB,\Index,\IsLast,0
.endm

.macro KERNEL8x8_I2_L4_3  AREG,BREG,OffsetA,OffsetB, Index,IsLast
  KERNEL8x8_L1_L4_I \AREG,\BREG,  \OffsetA,\OffsetB,\Index,\IsLast,1
.endm

.macro END8x8_NORMAL
  END8x8 0, AO, BO, 32,32 
.endm

.macro Zero8X8
    xxlxor      vs32,   vs32,   vs32
    xxlxor      vs33,   vs33,   vs33
 
    xxlxor      vs36,   vs36,   vs36
    xxlxor      vs37,   vs37,   vs37
 
    xxlxor      vs40,   vs40,   vs40
    xxlxor      vs41,   vs41,   vs41
 
    xxlxor      vs44,   vs44,   vs44
    xxlxor      vs45,   vs45,   vs45
 
    xxlxor      vs48,   vs48,   vs48
    xxlxor      vs49,   vs49,   vs49
 
    xxlxor      vs52,   vs52,   vs52
    xxlxor      vs53,   vs53,   vs53
 
    xxlxor      vs56,   vs56,   vs56
    xxlxor      vs57,   vs57,   vs57
  
    xxlxor      vs60,   vs60,   vs60
    xxlxor      vs61,   vs61,   vs61
    
.endm

.macro LOAD8x8  Zero

    lxv vs24,   0(BO)
    lxv vs28,   16(BO)
    lxv vs0,     0(AO)
    lxv vs1,    16(AO)

    xxperm      vs26,   vs24,       permute_mask
    xxperm      vs30,   vs28,       permute_mask    
    xxpermdi    vs25,   vs24,   vs24,2     
    xxpermdi    vs29,   vs28,   vs28,2    

    xxpermdi    vs27,   vs26,   vs26,2  
    xxpermdi    vs31,   vs30,   vs30,2      

.if \Zero==1 
    xxlxor      vs32,   vs32,   vs32
    xxlxor      vs33,   vs33,   vs33 
    xxlxor      vs36,   vs36,   vs36
    xxlxor      vs37,   vs37,   vs37
    xxlxor      vs40,   vs40,   vs40
    xxlxor      vs41,   vs41,   vs41 
    xxlxor      vs44,   vs44,   vs44
    xxlxor      vs45,   vs45,   vs45 
    xxlxor      vs48,   vs48,   vs48
    xxlxor      vs49,   vs49,   vs49 
    xxlxor      vs52,   vs52,   vs52
    xxlxor      vs53,   vs53,   vs53 
    xxlxor      vs56,   vs56,   vs56
    xxlxor      vs57,   vs57,   vs57  
    xxlxor      vs60,   vs60,   vs60
    xxlxor      vs61,   vs61,   vs61  
.endif
.endm


.macro END8x8 First, AREG, BREG, OffsetA, OffsetB

.if \OffsetB != 0 
    addi        \BREG, \BREG, \OffsetB 
.endif
.if \OffsetA != 0 
    addi        \AREG, \AREG, \OffsetA 
.endif  

.if \First==1
    xvmulsp     vs32, vs0,vs24
    xvmulsp     vs33, vs1,vs24

    xvmulsp     vs36, vs0,vs25
    xvmulsp     vs37, vs1,vs25

    xvmulsp     vs40, vs0,vs26
    xvmulsp     vs41, vs1,vs26

    xvmulsp     vs44, vs0,vs27
    xvmulsp     vs45, vs1,vs27

    xvmulsp     vs48, vs0,vs28
    xvmulsp     vs49, vs1,vs28

    xvmulsp     vs52, vs0,vs29
    xvmulsp     vs53, vs1,vs29

    xvmulsp     vs56, vs0,vs30
    xvmulsp     vs57, vs1,vs30

    xvmulsp     vs60, vs0,vs31
    xvmulsp     vs61, vs1,vs31

.else
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

.endif
.endm  

.macro KERNEL8x8_L1_L4_I  AREG,BREG,   OffsetA,OffsetB, Index,IsLast ,Complete

    lxv vs8,    DISP32(\Index, 0+\OffsetB)(\BREG)
    lxv vs12,   DISP32(\Index,16+\OffsetB)(\BREG)

    lxv vs4,    DISP32(\Index, 0+\OffsetA)(\AREG)
    lxv vs5,    DISP32(\Index,16+\OffsetA)(\AREG)

    xxperm      vs10,   vs8,        permute_mask
    xxperm      vs14,   vs12,       permute_mask    

    xvmaddasp       vs32, vs0,vs24
    xvmaddasp       vs33, vs1,vs24

    xxpermdi    vs9,    vs8,    vs8,2    
    xxpermdi    vs13,   vs12,   vs12,2   


    xvmaddasp       vs36, vs0,vs25
    xvmaddasp       vs37, vs1,vs25

    xxpermdi    vs11,   vs10,   vs10,2  
    xxpermdi    vs15,   vs14,   vs14,2  

    xvmaddasp       vs40, vs0,vs26
    xvmaddasp       vs41, vs1,vs26

    xvmaddasp       vs44, vs0,vs27
    xvmaddasp       vs45, vs1,vs27

    xvmaddasp       vs48, vs0,vs28
    xvmaddasp       vs49, vs1,vs28

    xvmaddasp       vs52, vs0,vs29
    xvmaddasp       vs53, vs1,vs29
    lxv vs24,   DISP32(\Index,32+\OffsetB)(\BREG)
    lxv vs28,   DISP32(\Index,32+16+\OffsetB)(\BREG)
    xvmaddasp       vs56, vs0,vs30
    xvmaddasp       vs57, vs1,vs30

    xvmaddasp       vs60, vs0,vs31
    xvmaddasp       vs61, vs1,vs31

    xxperm      vs26,   vs24,       permute_mask
    xxperm      vs30,   vs28,       permute_mask    

    lxv vs0,    DISP32(\Index,32+\OffsetA)(\AREG)
    lxv vs1,    DISP32(\Index,32+16+\OffsetA)(\AREG)


    xxpermdi    vs25,   vs24,   vs24,2     
    xxpermdi    vs29,   vs28,   vs28,2    

    xvmaddasp       vs32, vs4,vs8
    xvmaddasp       vs33, vs5,vs8

    xvmaddasp       vs36, vs4,vs9
    xvmaddasp       vs37, vs5,vs9

    xxpermdi    vs27,   vs26,   vs26,2  
    xxpermdi    vs31,   vs30,   vs30,2      

    xvmaddasp       vs40, vs4,vs10
    xvmaddasp       vs41, vs5,vs10

    xvmaddasp       vs44, vs4,vs11
    xvmaddasp       vs45, vs5,vs11

    xvmaddasp       vs48, vs4,vs12
    xvmaddasp       vs49, vs5,vs12

    xvmaddasp       vs52, vs4,vs13
    xvmaddasp       vs53, vs5,vs13
    lxv vs8,    DISP32(\Index,64+\OffsetB)(\BREG)
    lxv vs12,   DISP32(\Index,64+16+\OffsetB)(\BREG)
    xvmaddasp       vs56, vs4,vs14
    xvmaddasp       vs57, vs5,vs14

    xvmaddasp       vs60, vs4,vs15
    xvmaddasp       vs61, vs5,vs15

    xxperm      vs10,   vs8,        permute_mask
    xxperm      vs14,   vs12,       permute_mask   
 

    lxv vs4,    DISP32(\Index,64+0+\OffsetA)(\AREG)
    lxv vs5,    DISP32(\Index,64+16+\OffsetA)(\AREG)

 
    xxpermdi    vs9,    vs8,    vs8,2    
    xxpermdi    vs13,   vs12,   vs12,2  

    xvmaddasp       vs32, vs0,vs24
    xvmaddasp       vs33, vs1,vs24

    xvmaddasp       vs36, vs0,vs25
    xvmaddasp       vs37, vs1,vs25

    xxpermdi    vs11,   vs10,   vs10,2  
    xxpermdi    vs15,   vs14,   vs14,2  

    xvmaddasp       vs40, vs0,vs26
    xvmaddasp       vs41, vs1,vs26

    xvmaddasp       vs44, vs0,vs27
    xvmaddasp       vs45, vs1,vs27

    xvmaddasp       vs48, vs0,vs28
    xvmaddasp       vs49, vs1,vs28

    xvmaddasp       vs52, vs0,vs29
    xvmaddasp       vs53, vs1,vs29
.if \Complete==0
    lxv vs24,   DISP32(\Index,96+\OffsetB)(\BREG)
    lxv vs28,   DISP32(\Index,96+16+\OffsetB)(\BREG)
.endif 
    xvmaddasp       vs56, vs0,vs30
    xvmaddasp       vs57, vs1,vs30
.if \Complete==0
    xxperm      vs26,   vs24,   permute_mask
    xxperm      vs30,   vs28,   permute_mask   
.endif 
    xvmaddasp       vs60, vs0,vs31
    xvmaddasp       vs61, vs1,vs31


.if \Complete==0
    lxv vs0,    DISP32(\Index,96+\OffsetA)(\AREG)
    lxv vs1,    DISP32(\Index,96+16+\OffsetA)(\AREG) 
.endif 

.if \Complete==0     
    xxpermdi    vs25,   vs24,   vs24,2 
    xxpermdi    vs29,   vs28,   vs28,2      

.endif 
.if \IsLast==1  
.if \Complete==1
  
    addi        \BREG, \BREG,  DISP32(\Index,32*3+\OffsetB)
    addi        \AREG, \AREG, DISP32(\Index,32*3+\OffsetA)
.else
  
    addi        \BREG, \BREG,  DISP32(\Index,128)
    addi        \AREG, \AREG, DISP32(\Index,128)
.endif
.endif   
 
    xvmaddasp       vs32, vs4,vs8
    xvmaddasp       vs33, vs5,vs8

    xvmaddasp       vs36, vs4,vs9
    xvmaddasp       vs37, vs5,vs9

.if \Complete==0        
    xxpermdi    vs27,   vs26,   vs26,2  
    xxpermdi    vs31,   vs30,   vs30,2  
    
.endif
 
    xvmaddasp       vs40, vs4,vs10
    xvmaddasp       vs41, vs5,vs10

    xvmaddasp       vs44, vs4,vs11
    xvmaddasp       vs45, vs5,vs11

    xvmaddasp       vs48, vs4,vs12
    xvmaddasp       vs49, vs5,vs12

    xvmaddasp       vs52, vs4,vs13
    xvmaddasp       vs53, vs5,vs13

    xvmaddasp       vs56, vs4,vs14
    xvmaddasp       vs57, vs5,vs14

    xvmaddasp       vs60, vs4,vs15
    xvmaddasp       vs61, vs5,vs15

.endm

.macro KERNEL8x8 First

  LOAD8x8 0
  END8x8 \First, AO, BO, 32,32  
.endm

.macro KERNEL8x8_L1_L2_I  AREG,BREG,First,OffsetA,OffsetB, Index,IsLast ,Complete
    
    lxv vs8,    DISP16(\Index, 0+\OffsetB)(\BREG)
    lxv vs12,   DISP16(\Index,16+\OffsetB)(\BREG)

    lxv vs4,    DISP16(\Index, 0+\OffsetA)(\AREG)
    lxv vs5,    DISP16(\Index,16+\OffsetA)(\AREG)

    xxperm      vs10,   vs8,        permute_mask
    xxperm      vs14,   vs12,       permute_mask    
    xxpermdi    vs9,    vs8,    vs8,2    
    xxpermdi    vs13,   vs12,   vs12,2   
.if \First==1
    xvmulsp     vs32, vs0,vs24
    xvmulsp     vs33, vs1,vs24

    xvmulsp     vs36, vs0,vs25
    xvmulsp     vs37, vs1,vs25

.else
    xvmaddasp       vs32, vs0,vs24
    xvmaddasp       vs33, vs1,vs24

    xvmaddasp       vs36, vs0,vs25
    xvmaddasp       vs37, vs1,vs25

.endif

    xxpermdi    vs11,   vs10,   vs10,2  
    xxpermdi    vs15,   vs14,   vs14,2  
 
.if \First==1  
    xvmulsp     vs40, vs0,vs26
    xvmulsp     vs41, vs1,vs26

    xvmulsp     vs44, vs0,vs27
    xvmulsp     vs45, vs1,vs27

    xvmulsp     vs48, vs0,vs28
    xvmulsp     vs49, vs1,vs28

    xvmulsp     vs52, vs0,vs29
    xvmulsp     vs53, vs1,vs29

    xvmulsp     vs56, vs0,vs30
    xvmulsp     vs57, vs1,vs30

    xvmulsp     vs60, vs0,vs31
    xvmulsp     vs61, vs1,vs31

.else 
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

.endif
.if \Complete==0
    lxv vs24,   DISP16(\Index,32+\OffsetB)(\BREG)
    lxv vs28,   DISP16(\Index,32+16+\OffsetB)(\BREG)

    lxv vs0,    DISP16(\Index,32+\OffsetA)(\AREG)
    lxv vs1,    DISP16(\Index,32+16+\OffsetA)(\AREG)

    xxperm      vs26,   vs24,   permute_mask
    xxperm      vs30,   vs28,   permute_mask    
    xxpermdi    vs25,   vs24,   vs24,2   
    xxpermdi    vs29,   vs28,   vs28,2  
.endif    
.if \IsLast==1  
.if \Complete==1
    addi        \BREG, \BREG,  DISP16(\Index,32+\OffsetB) 
    addi        \AREG, \AREG,  DISP16(\Index,32+\OffsetA)

.else
    addi        \BREG, \BREG,  DISP16(\Index,64)
    addi        \AREG, \AREG,  DISP16(\Index,64) 
.endif
.endif

.if \First==1
    xvmulsp     vs32, vs4,vs8
    xvmulsp     vs33, vs5,vs8

    xvmulsp     vs36, vs4,vs9
    xvmulsp     vs37, vs5,vs9

.else
    xvmaddasp       vs32, vs4,vs8
    xvmaddasp       vs33, vs5,vs8

    xvmaddasp       vs36, vs4,vs9
    xvmaddasp       vs37, vs5,vs9

.endif 
 
.if \Complete==0        
    xxpermdi    vs27,   vs26,   vs26,2  
    xxpermdi    vs31,   vs30,   vs30,2  
 
.endif
.if \First==1  
    xvmulsp     vs40, vs4,vs10
    xvmulsp     vs41, vs5,vs10

    xvmulsp     vs44, vs4,vs11
    xvmulsp     vs45, vs5,vs11

    xvmulsp     vs48, vs4,vs12
    xvmulsp     vs49, vs5,vs12

    xvmulsp     vs52, vs4,vs13
    xvmulsp     vs53, vs5,vs13

    xvmulsp     vs56, vs4,vs14
    xvmulsp     vs57, vs5,vs14

    xvmulsp     vs60, vs4,vs15
    xvmulsp     vs61, vs5,vs15

.else 
    xvmaddasp       vs40, vs4,vs10
    xvmaddasp       vs41, vs5,vs10

    xvmaddasp       vs44, vs4,vs11
    xvmaddasp       vs45, vs5,vs11

    xvmaddasp       vs48, vs4,vs12
    xvmaddasp       vs49, vs5,vs12

    xvmaddasp       vs52, vs4,vs13
    xvmaddasp       vs53, vs5,vs13

    xvmaddasp       vs56, vs4,vs14
    xvmaddasp       vs57, vs5,vs14

    xvmaddasp       vs60, vs4,vs15
    xvmaddasp       vs61, vs5,vs15

.endif

.endm


.macro SAVE8x8 
 
  slwi    T10, LDC ,   1 
  add     T1, CO, LDC 

  add     T2, CO, T10  
  add     T3, T1, T10  

  add     T4, T2, T10  
  add     T5, T3, T10 

  add     T6, T4, T10 
  add     T7, T5, T10 

#ifndef TRMMKERNEL    
    lxv        vs34, 0(CO)
    lxv        vs35, 16(CO)      
    lxv        vs38, 0(T1)
    lxv        vs39, 16(T1)  
    lxv        vs42, 0(T2)
    lxv        vs43, 16(T2)     
    lxv        vs46, 0(T3)
    lxv        vs47, 16(T3)  

    lxv        vs50, 0(T4)
    lxv        vs51, 16(T4)      
    lxv        vs54, 0(T5)
    lxv        vs55, 16(T5)  
    lxv        vs58, 0(T6)
    lxv        vs59, 16(T6)     
    lxv        vs62, 0(T7)
    lxv        vs63, 16(T7) 
#endif  

    xxmrglw     vs8,    vs32,   vs44
    xxmrglw     vs10,   vs36,   vs40  

    xxmrghw     vs1,    vs32,   vs44
    xxmrghw     vs0,    vs36,   vs40

    xxmrglw     vs12,   vs33,   vs45
    xxmrglw     vs14,   vs37,   vs41  

    xxmrghw     vs2,    vs37,   vs41
    xxmrghw     vs3,    vs33,   vs45

    xxlor      vs9, vs8,    vs8
    xxlor      vs11,    vs10,   vs10 
 
    xxlor      vs13,    vs12,   vs12
    xxlor      vs15,    vs14,   vs14

    xxperm      vs8,    vs0,    save_permute_1
    xxperm      vs10,   vs1,    save_permute_1
    xxperm      vs9,    vs0,    save_permute_2  
    xxperm      vs11,   vs1,    save_permute_2      

    xxperm     vs12,    vs2,    save_permute_1
    xxperm     vs14,    vs3,    save_permute_1
      
    xxperm     vs13,    vs2,    save_permute_2   
    xxperm     vs15,    vs3,    save_permute_2      


    /* multiply add normal way */
 
#ifdef TRMMKERNEL
    xvmulsp     vs34,   vs8,    alpha_r 
    xvmulsp     vs35,   vs12,   alpha_r 
    xvmulsp     vs38,   vs9,    alpha_r 
    xvmulsp     vs39,   vs13,   alpha_r 
    xvmulsp     vs42,   vs10,   alpha_r 
    xvmulsp     vs43,   vs14,   alpha_r 
    xvmulsp     vs46,   vs11,   alpha_r 
    xvmulsp     vs47,   vs15,   alpha_r                    
#else 
    xvmaddasp   vs34,   vs8,    alpha_r 
    xvmaddasp   vs35,   vs12,   alpha_r 
    xvmaddasp   vs38,   vs9,    alpha_r 
    xvmaddasp   vs39,   vs13,   alpha_r  
    xvmaddasp   vs42,   vs10,   alpha_r 
    xvmaddasp   vs43,   vs14,   alpha_r   
    xvmaddasp   vs46,   vs11,   alpha_r 
    xvmaddasp   vs47,   vs15,   alpha_r                     
#endif     
 
   
    xxmrglw     vs8,    vs48,   vs60
    xxmrglw     vs10,   vs52,   vs56  

    xxmrghw     vs1,    vs48,   vs60
    xxmrghw     vs0,    vs52,   vs56
    stxv        vs34, 0(CO)
    stxv        vs35, 16(CO) 
    xxmrglw     vs12,   vs49,   vs61
    xxmrglw     vs14,   vs53,   vs57  
    stxv        vs38, 0(T1)
    stxv        vs39, 16(T1) 
    xxmrghw     vs2,    vs53,   vs57
    xxmrghw     vs3,    vs49,   vs61
    stxv        vs42, 0(T2)
    stxv        vs43, 16(T2)   
    xxlor      vs9, vs8,    vs8
    xxlor      vs11,    vs10,   vs10  
    stxv        vs46, 0(T3)
    stxv        vs47, 16(T3)  
    xxlor      vs13,    vs12,   vs12
    xxlor      vs15,    vs14,   vs14
   
    xxperm      vs8,    vs0,    save_permute_1
    xxperm      vs10,   vs1,    save_permute_1
    
 
    xxperm      vs9,    vs0,    save_permute_2  
    xxperm      vs11,   vs1,    save_permute_2      
 
    xxperm     vs12,    vs2,    save_permute_1
    xxperm     vs14,    vs3,    save_permute_1
    xxperm     vs13,    vs2,    save_permute_2   
    xxperm     vs15,    vs3,    save_permute_2      
    
 #ifdef TRMMKERNEL
    xvmulsp     vs50,   vs8,    alpha_r 
    xvmulsp     vs51,   vs12,   alpha_r 
    xvmulsp     vs54,   vs9,    alpha_r 
    xvmulsp     vs55,   vs13,   alpha_r 
    xvmulsp     vs58,   vs10,   alpha_r 
    xvmulsp     vs59,   vs14,   alpha_r 
    xvmulsp     vs62,   vs11,   alpha_r 
    xvmulsp     vs63,   vs15,   alpha_r                    
#else 
    xvmaddasp     vs50,   vs8,    alpha_r 
    xvmaddasp     vs51,   vs12,   alpha_r 
    xvmaddasp     vs54,   vs9,    alpha_r 
    xvmaddasp     vs55,   vs13,   alpha_r 
    xvmaddasp     vs58,   vs10,   alpha_r 
    xvmaddasp     vs59,   vs14,   alpha_r 
    xvmaddasp     vs62,   vs11,   alpha_r 
    xvmaddasp     vs63,   vs15,   alpha_r                     
#endif  

    stxv        vs50, 0(T4)
    stxv        vs51, 16(T4)      
    stxv        vs54, 0(T5)
    stxv        vs55, 16(T5)  
    stxv        vs58, 0(T6)
    stxv        vs59, 16(T6)     
    stxv        vs62, 0(T7)
    stxv        vs63, 16(T7)   

    addi CO,CO,32

.endm


/**********************************************************************************************
* Macros for N=8 and M=4
**********************************************************************************************/

.macro LOAD8x4_1
   LOAD8x4 1
.endm

.macro LOAD8x4_0
   LOAD8x4 0
.endm

.macro KERNEL8x4_L1_L4  Index,IsLast
  KERNEL8x4_L1_L4_I AO,BO, 0,0, \Index,\IsLast,0
.endm

.macro KERNEL8x4_I1_L4  OffsetA,OffsetB, Index,IsLast
  KERNEL8x4_L1_L4_I  AO,BO, \OffsetA,\OffsetB,\Index,\IsLast,0
.endm

.macro KERNEL8x4_I1_L4_2  OffsetA,OffsetB, Index,IsLast
  KERNEL8x4_L1_L4_I  AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast,0
.endm

.macro KERNEL8x4_I1_L4_3  OffsetA,OffsetB, Index,IsLast
  KERNEL8x4_L1_L4_I  AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast,1
.endm
.macro KERNEL8x4_I1_L2_3  OffsetA,OffsetB, Index,IsLast
   KERNEL8x4_L1_L2_I AO,BO,0,  \OffsetA,\OffsetB,\Index,\IsLast,1
.endm

.macro KERNEL8x4_I2_L4_2  AREG,BREG,OffsetA,OffsetB, Index,IsLast
  KERNEL8x4_L1_L4_I  \AREG,\BREG,  \OffsetA,\OffsetB,\Index,\IsLast,0
.endm

.macro KERNEL8x4_I2_L4_3  AREG,BREG,OffsetA,OffsetB, Index,IsLast
  KERNEL8x4_L1_L4_I \AREG,\BREG,  \OffsetA,\OffsetB,\Index,\IsLast,1
.endm

.macro Zero8X4
    xxlxor      vs32,   vs32,   vs32
    xxlxor      vs33,   vs33,   vs33 
    xxlxor      vs34,   vs34,   vs34
    xxlxor      vs35,   vs35,   vs35
    
    xxlxor      vs48,   vs48,   vs48
    xxlxor      vs49,   vs49,   vs49
    xxlxor      vs50,   vs50,   vs50
    xxlxor      vs51,   vs51,   vs51  
    
.endm

.macro LOAD8x4  Zero

    lxv vs0,     0(AO)
    lxv vs24,   0(BO)
    lxv vs25,   16(BO)



    xxperm      vs2,   vs0,       permute_mask  
    xxpermdi    vs1,   vs0,   vs0,2      
    xxpermdi    vs3,   vs2,   vs2,2        

.if \Zero==1 
    xxlxor      vs32,   vs32,   vs32
    xxlxor      vs33,   vs33,   vs33 
    xxlxor      vs34,   vs34,   vs34
    xxlxor      vs35,   vs35,   vs35

    xxlxor      vs48,   vs48,   vs48
    xxlxor      vs49,   vs49,   vs49
    xxlxor      vs50,   vs50,   vs50
    xxlxor      vs51,   vs51,   vs51  
.endif
.endm

.macro END8x4_NORMAL
  END8x4 0, AO, BO, 16,32 
.endm

.macro END8x4 First, AREG, BREG, OffsetA, OffsetB

.if \OffsetB != 0 
    addi        \BREG, \BREG, \OffsetB 
.endif
.if \OffsetA != 0 
    addi        \AREG, \AREG, \OffsetA 
.endif  

.if \First==1
    xvmulsp      vs32,   vs24,   vs0
    xvmulsp      vs33,   vs24,   vs1 
    xvmulsp      vs34,   vs24,   vs2
    xvmulsp      vs35,   vs24,   vs3

    xvmulsp      vs48,   vs25,   vs0
    xvmulsp      vs49,   vs25,   vs1
    xvmulsp      vs50,   vs25,   vs2
    xvmulsp      vs51,   vs25,   vs3  
.else
    xvmaddasp      vs32,   vs24,   vs0
    xvmaddasp      vs33,   vs24,   vs1 
    xvmaddasp      vs34,   vs24,   vs2
    xvmaddasp      vs35,   vs24,   vs3

    xvmaddasp      vs48,   vs25,   vs0
    xvmaddasp      vs49,   vs25,   vs1
    xvmaddasp      vs50,   vs25,   vs2
    xvmaddasp      vs51,   vs25,   vs3 

.endif
.endm  

.macro KERNEL8x4_L1_L4_I  AREG,BREG,   OffsetA,OffsetB, Index,IsLast ,Complete

    lxv vs4,    DISP16(\Index, 0+\OffsetA)(\AREG)
    lxv vs26,   DISP32(\Index, 0+\OffsetB)(\BREG)
    lxv vs27,   DISP32(\Index,16+\OffsetB)(\BREG)

    xxperm      vs6,   vs4,       permute_mask  
    xxpermdi    vs5,   vs4,   vs4,2      
    xxpermdi    vs7,   vs6,   vs6,2 
 
    xvmaddasp      vs32,   vs24,   vs0
    xvmaddasp      vs33,   vs24,   vs1 
    xvmaddasp      vs34,   vs24,   vs2
    xvmaddasp      vs35,   vs24,   vs3

    xvmaddasp      vs48,   vs25,   vs0
    xvmaddasp      vs49,   vs25,   vs1
    xvmaddasp      vs50,   vs25,   vs2
    xvmaddasp      vs51,   vs25,   vs3 

    lxv vs0,    DISP16(\Index, 16+\OffsetA)(\AREG)
    lxv vs24,   DISP32(\Index, 32+\OffsetB)(\BREG)
    lxv vs25,   DISP32(\Index, 48+\OffsetB)(\BREG) 

    xxperm      vs2,   vs0,       permute_mask  
    xxpermdi    vs1,   vs0,   vs0,2      
    xxpermdi    vs3,   vs2,   vs2,2   

    xvmaddasp      vs32,   vs26,   vs4
    xvmaddasp      vs33,   vs26,   vs5 
    xvmaddasp      vs34,   vs26,   vs6
    xvmaddasp      vs35,   vs26,   vs7

    xvmaddasp      vs48,   vs27,   vs4
    xvmaddasp      vs49,   vs27,   vs5
    xvmaddasp      vs50,   vs27,   vs6
    xvmaddasp      vs51,   vs27,   vs7
 

    lxv vs4,    DISP16(\Index, 32+\OffsetA)(\AREG)
    lxv vs26,   DISP32(\Index, 64+\OffsetB)(\BREG)
    lxv vs27,   DISP32(\Index, 80+\OffsetB)(\BREG)

    xxperm      vs6,   vs4,       permute_mask  
    xxpermdi    vs5,   vs4,   vs4,2      
    xxpermdi    vs7,   vs6,   vs6,2 
 
    xvmaddasp      vs32,   vs24,   vs0
    xvmaddasp      vs33,   vs24,   vs1 
    xvmaddasp      vs34,   vs24,   vs2
    xvmaddasp      vs35,   vs24,   vs3

    xvmaddasp      vs48,   vs25,   vs0
    xvmaddasp      vs49,   vs25,   vs1
    xvmaddasp      vs50,   vs25,   vs2
    xvmaddasp      vs51,   vs25,   vs3 

.if \Complete==0 

    lxv vs0,    DISP16(\Index, 48+\OffsetA)(\AREG)
    lxv vs24,   DISP32(\Index, 96+\OffsetB)(\BREG)
    lxv vs25,   DISP32(\Index, 96+16+\OffsetB)(\BREG) 

    xxperm      vs2,   vs0,       permute_mask  
    xxpermdi    vs1,   vs0,   vs0,2      
    xxpermdi    vs3,   vs2,   vs2,2   
.endif
    xvmaddasp      vs32,   vs26,   vs4
    xvmaddasp      vs33,   vs26,   vs5 
    xvmaddasp      vs34,   vs26,   vs6
    xvmaddasp      vs35,   vs26,   vs7

    xvmaddasp      vs48,   vs27,   vs4
    xvmaddasp      vs49,   vs27,   vs5
    xvmaddasp      vs50,   vs27,   vs6
    xvmaddasp      vs51,   vs27,   vs7

 
 
.if \IsLast==1  
.if \Complete==1
    addi        \AREG, \AREG, DISP16(\Index,16*3+\OffsetA)  
    addi        \BREG, \BREG,  DISP32(\Index,32*3+\OffsetB)

.else
    addi        \AREG, \AREG, DISP16(\Index,64)  
    addi        \BREG, \BREG,  DISP32(\Index,128)

.endif
.endif   
 
 
.endm

.macro KERNEL8x4 First
    LOAD8x4 0
    END8x4 \First, AO, BO, 16,32  
.endm

.macro KERNEL8x4_L1_L2_I  AREG,BREG,First,OffsetA,OffsetB, Index,IsLast ,Complete

    lxv vs4,    DISP8(\Index, 0+\OffsetA)(\AREG)
    lxv vs26,   DISP16(\Index, 0+\OffsetB)(\BREG)
    lxv vs27,   DISP16(\Index,16+\OffsetB)(\BREG)

    xxperm      vs6,   vs4,       permute_mask  
    xxpermdi    vs5,   vs4,   vs4,2      
    xxpermdi    vs7,   vs6,   vs6,2 
.if \First==1
    xvmulsp      vs32,   vs24,   vs0
    xvmulsp      vs33,   vs24,   vs1 
    xvmulsp      vs34,   vs24,   vs2
    xvmulsp      vs35,   vs24,   vs3

    xvmulsp      vs48,   vs25,   vs0
    xvmulsp      vs49,   vs25,   vs1
    xvmulsp      vs50,   vs25,   vs2
    xvmulsp      vs51,   vs25,   vs3  
.else 
    xvmaddasp      vs32,   vs24,   vs0
    xvmaddasp      vs33,   vs24,   vs1 
    xvmaddasp      vs34,   vs24,   vs2
    xvmaddasp      vs35,   vs24,   vs3

    xvmaddasp      vs48,   vs25,   vs0
    xvmaddasp      vs49,   vs25,   vs1
    xvmaddasp      vs50,   vs25,   vs2
    xvmaddasp      vs51,   vs25,   vs3 
.endif

.if \Complete==0 

    lxv vs0,    DISP8(\Index, 16+\OffsetA)(\AREG)
    lxv vs24,   DISP16(\Index, 32+\OffsetB)(\BREG)
    lxv vs25,   DISP16(\Index, 48+\OffsetB)(\BREG) 

    xxperm      vs2,   vs0,       permute_mask  
    xxpermdi    vs1,   vs0,   vs0,2      
    xxpermdi    vs3,   vs2,   vs2,2   
.endif

.if \First==1
    xvmulsp      vs32,   vs26,   vs4
    xvmulsp      vs33,   vs26,   vs5 
    xvmulsp      vs34,   vs26,   vs6
    xvmulsp      vs35,   vs26,   vs7

    xvmulsp      vs48,   vs27,   vs4
    xvmulsp      vs49,   vs27,   vs5
    xvmulsp      vs50,   vs27,   vs6
    xvmulsp      vs51,   vs27,   vs7


.else
    xvmaddasp      vs32,   vs26,   vs4
    xvmaddasp      vs33,   vs26,   vs5 
    xvmaddasp      vs34,   vs26,   vs6
    xvmaddasp      vs35,   vs26,   vs7

    xvmaddasp      vs48,   vs27,   vs4
    xvmaddasp      vs49,   vs27,   vs5
    xvmaddasp      vs50,   vs27,   vs6
    xvmaddasp      vs51,   vs27,   vs7
.endif
 
 
.if \IsLast==1  
.if \Complete==1
    addi        \AREG, \AREG, DISP8(\Index,16+\OffsetA)  
    addi        \BREG, \BREG,  DISP16(\Index,32+\OffsetB)

.else
    addi        \AREG, \AREG, DISP8(\Index,32)  
    addi        \BREG, \BREG,  DISP16(\Index,64)

.endif
.endif   
     
  
.endm


.macro SAVE8x4
  slwi    T10, LDC ,   1 
  add     T1, CO, LDC 
#if !defined(TRMMKERNEL)  
  lxv        vs36, 0(CO)
  lxv        vs37, 0(T1)
#endif  
  add     T2, CO, T10  
  add     T3, T1, T10 
#if !defined(TRMMKERNEL)    
  lxv        vs38, 0(T2)
  lxv        vs39, 0(T3)   
#endif   
  add     T4, T2, T10 
  add     T5, T3, T10
#if !defined(TRMMKERNEL)    
  lxv        vs40, 0(T4)
  lxv        vs41, 0(T5)
#endif  
  add     T6, T4, T10 
  add     T7, T5, T10
#if !defined(TRMMKERNEL)    
  lxv        vs42, 0(T6)
  lxv        vs43, 0(T7)
#endif
  xxmrglw  vs0, vs35,vs32
  xxmrglw  vs1, vs34,vs33 
  xxmrglw  vs4, vs32,vs35
  xxmrglw  vs5, vs33,vs34 


  xxmrghw  vs2, vs35,vs32
  xxmrghw  vs3, vs34,vs33 
  xxmrghw  vs6, vs32,vs35
  xxmrghw  vs7, vs33,vs34  

  xxmrgld  vs24, vs1, vs0  
  xxmrghd  vs25,vs5,vs4 

  xxmrgld  vs26, vs2, vs3  
  xxmrghd  vs27,vs6,vs7


  xxmrglw  vs0, vs51,vs48
  xxmrglw  vs1, vs50,vs49  
  xxmrglw  vs4, vs48,vs51
  xxmrglw  vs5, vs49,vs50 

  xxmrghw  vs2, vs51,vs48
  xxmrghw  vs3, vs50,vs49  
  xxmrghw  vs6, vs48,vs51
  xxmrghw  vs7, vs49,vs50   

  xxmrgld  vs28, vs1, vs0  
  xxmrghd  vs29,vs5,vs4

  xxmrgld  vs30, vs2, vs3   
  xxmrghd  vs31,vs6,vs7
#if defined(TRMMKERNEL)

  xvmulsp        vs36, vs24, alpha_r
  xvmulsp        vs37, vs25, alpha_r 
  xvmulsp        vs38, vs26, alpha_r
  xvmulsp        vs39, vs27, alpha_r   
  xvmulsp        vs40, vs28, alpha_r
  xvmulsp        vs41, vs29, alpha_r 
  xvmulsp        vs42, vs30, alpha_r
  xvmulsp        vs43, vs31, alpha_r
#else
  xvmaddasp        vs36, vs24, alpha_r
  xvmaddasp        vs37, vs25, alpha_r 
  xvmaddasp        vs38, vs26, alpha_r
  xvmaddasp        vs39, vs27, alpha_r   
  xvmaddasp        vs40, vs28, alpha_r
  xvmaddasp        vs41, vs29, alpha_r 
  xvmaddasp        vs42, vs30, alpha_r
  xvmaddasp        vs43, vs31, alpha_r
#endif

  stxv        vs36, 0(CO)
  stxv        vs37, 0(T1) 
  stxv        vs38, 0(T2)
  stxv        vs39, 0(T3)   
  stxv        vs40, 0(T4)
  stxv        vs41, 0(T5) 
  stxv        vs42, 0(T6)
  stxv        vs43, 0(T7)


  addi CO,CO,16
.endm


/**********************************************************************************************
* Macros for N=8 and M=2
**********************************************************************************************/

 
.macro KERNEL8x2_2   OffsetA,OffsetB, Index,IsLast
  KERNEL8x2_I_2 AO,BO, 0, \OffsetA,\OffsetB,\Index,\IsLast
.endm

 

.macro Zero8x2
    xxlxor      vs0,   vs0,   vs0
    xxlxor      vs1,   vs1,   vs1 
    xxlxor      vs2,   vs2,   vs2
    xxlxor      vs3,   vs3,   vs3
       
.endm
 
.macro KERNEL8x2
  KERNEL8x2_1 AO,BO, 0, 0,0,0
.endm
.macro KERNEL8x2_1 AREG,BREG,First,OffsetA,OffsetB,Index


    lxsd v4,   DISP2(\Index, 0+\OffsetA)(\AREG)
    lxv vs26,   DISP8(\Index, 0+\OffsetB)(\BREG)
    lxv vs27,   DISP8(\Index,16+\OffsetB)(\BREG)      
    xxspltw   vs8,  vs36, 0 
    xxspltw   vs9,  vs36, 1  
 
.if \First==1
    xvmulsp      vs0,   vs26,   vs8
    xvmulsp      vs1,   vs27,   vs8 
    xvmulsp      vs2,   vs26,   vs9
    xvmulsp      vs3,   vs27,   vs9 
     
.else 
    xvmaddasp      vs0,   vs26,   vs8
    xvmaddasp      vs1,   vs27,   vs8 
    xvmaddasp      vs2,   vs26,   vs9
    xvmaddasp      vs3,   vs27,   vs9
 
 .endif
   
    addi        \AREG, \AREG, DISP2(\Index,8)  
    addi        \BREG, \BREG, DISP8(\Index,32)
 
.endm

.macro KERNEL8x2_I_2  AREG,BREG,First,OffsetA,OffsetB, Index,IsLast  

    lxv vs4,    DISP4(\Index, 0+\OffsetA)(\AREG)
    lxv vs26,   DISP16(\Index, 0+\OffsetB)(\BREG)
    lxv vs27,   DISP16(\Index,16+\OffsetB)(\BREG)
    lxv vs28,   DISP16(\Index,32+\OffsetB)(\BREG)
    lxv vs29,   DISP16(\Index,48+\OffsetB)(\BREG)      
    xxspltw   vs8,  vs4, 2  
    xxspltw   vs9,  vs4, 3 
    xxspltw   vs10, vs4, 0 
    xxspltw   vs11, vs4, 1
 
.if \First==1
    xvmulsp      vs0,   vs26,   vs8
    xvmulsp      vs1,   vs27,   vs8 
    xvmulsp      vs2,   vs26,   vs9
    xvmulsp      vs3,   vs27,   vs9 

    xvmulsp      vs0,   vs28,   vs10
    xvmulsp      vs1,   vs29,   vs10 
    xvmulsp      vs2,   vs28,   vs11
    xvmulsp      vs3,   vs29,   vs11     
.else 
    xvmaddasp      vs0,   vs26,   vs8
    xvmaddasp      vs1,   vs27,   vs8 
    xvmaddasp      vs2,   vs26,   vs9
    xvmaddasp      vs3,   vs27,   vs9

    xvmaddasp      vs0,   vs28,   vs10
    xvmaddasp      vs1,   vs29,   vs10 
    xvmaddasp      vs2,   vs28,   vs11
    xvmaddasp      vs3,   vs29,   vs11  
 .endif

 
.if \IsLast==1   
    addi        \AREG, \AREG, DISP4(\Index,16)  
    addi        \BREG, \BREG, DISP16(\Index,64)
.endif 
  
.endm


.macro SAVE8x2
  slwi    T10, LDC ,   1 
  add     T1, CO, LDC  
  add     T2, CO, T10  
  add     T3, T1, T10     
  add     T4, T2, T10 
  add     T5, T3, T10 
  add     T6, T4, T10 
  add     T7, T5, T10 
  /*convert alpha_r for multiply*/
  xscvspdp  vs4,alpha_r
/* v0 corresponds to vs32, do not forget*/
#if !defined(TRMMKERNEL)
  lxssp  v0,0(CO) 
  lxssp  v1,4(CO) 

  lxssp  v2,0(T1)
  lxssp  v3,4(T1)

  lxssp  v4,0(T2)
  lxssp  v5,4(T2)

  lxssp  v6,0(T3)
  lxssp  v7,4(T3)

  lxssp  v8,0(T4)
  lxssp  v9,4(T4)

  lxssp  v10,0(T5)
  lxssp  v11,4(T5)

  lxssp  v12,0(T6)
  lxssp  v13,4(T6)

  lxssp  v14,0(T7)
  lxssp  v15,4(T7)
#endif
  xscvspdp  vs5, vs2
  xxspltw   vs6, vs2, 1 
  xxspltw   vs7, vs2, 2 
  xxspltw   vs8, vs2, 3  
  xscvspdp  vs6,vs6
  xscvspdp  vs7,vs7
  xscvspdp  vs8,vs8

  xscvspdp  vs24, vs0
  xxspltw   vs25, vs0, 1 
  xxspltw   vs26, vs0, 2 
  xxspltw   vs27, vs0, 3  
  xscvspdp  vs25,vs25
  xscvspdp  vs26,vs26
  xscvspdp  vs27,vs27

  xscvspdp  vs9, vs3
  xxspltw   vs10, vs3, 1 
  xxspltw   vs11, vs3, 2 
  xxspltw   vs12, vs3, 3  
  xscvspdp  vs10,vs10
  xscvspdp  vs11,vs11
  xscvspdp  vs12,vs12

  xscvspdp  vs28, vs1
  xxspltw   vs29, vs1, 1 
  xxspltw   vs30, vs1, 2 
  xxspltw   vs31, vs1, 3  
  xscvspdp  vs29,vs29
  xscvspdp  vs30,vs30
  xscvspdp  vs31,vs31




#if defined(TRMMKERNEL)
  xsmuldp  vs32,vs8, vs4 
  xsmuldp  vs33,vs27, vs4 

  xsmuldp  vs34,vs7, vs4 
  xsmuldp  vs35,vs26, vs4 

  xsmuldp  vs36,vs6, vs4 
  xsmuldp  vs37,vs25, vs4  

  xsmuldp  vs38,vs5, vs4 
  xsmuldp  vs39,vs24, vs4  

  xsmuldp  vs40,vs12, vs4 
  xsmuldp  vs41,vs31, vs4

  xsmuldp  vs42,vs11, vs4 
  xsmuldp  vs43,vs30, vs4  

  xsmuldp  vs44,vs10, vs4 
  xsmuldp  vs45,vs29, vs4 

  xsmuldp  vs46,vs9, vs4 
  xsmuldp  vs47,vs28, vs4      
#else
  xsmaddadp  vs32,vs8, vs4 
  xsmaddadp  vs33,vs27, vs4 

  xsmaddadp  vs34,vs7, vs4 
  xsmaddadp  vs35,vs26, vs4 

  xsmaddadp  vs36,vs6, vs4 
  xsmaddadp  vs37,vs25, vs4  

  xsmaddadp  vs38,vs5, vs4 
  xsmaddadp  vs39,vs24, vs4  

  xsmaddadp  vs40,vs12, vs4 
  xsmaddadp  vs41,vs31, vs4

  xsmaddadp  vs42,vs11, vs4 
  xsmaddadp  vs43,vs30, vs4  

  xsmaddadp  vs44,vs10, vs4 
  xsmaddadp  vs45,vs29, vs4 

  xsmaddadp  vs46,vs9, vs4 
  xsmaddadp  vs47,vs28, vs4     
#endif  

  stxssp  v0,0(CO) 
  stxssp  v1,4(CO) 

  stxssp  v2,0(T1)
  stxssp  v3,4(T1)

  stxssp  v4,0(T2)
  stxssp  v5,4(T2)

  stxssp  v6,0(T3)
  stxssp  v7,4(T3)

  stxssp  v8,0(T4)
  stxssp  v9,4(T4)

  stxssp  v10,0(T5)
  stxssp  v11,4(T5)

  stxssp  v12,0(T6)
  stxssp  v13,4(T6)

  stxssp  v14,0(T7)
  stxssp  v15,4(T7)
 

  addi CO,CO,8
.endm


/**********************************************************************************************
* Macros for N=8 and M=1
**********************************************************************************************/
.macro KERNEL8x1_4   OffsetA,OffsetB, Index,IsLast
  KERNEL8x1_I_4 AO,BO, 0, \OffsetA,\OffsetB,\Index,\IsLast
.endm

.macro Zero8x1
    xxlxor      vs0,   vs0,   vs0
    xxlxor      vs1,   vs1,   vs1  
.endm

.macro KERNEL8x1
  KERNEL8x1_1 AO,BO, 0 
.endm

.macro KERNEL8x1_2
  KERNEL8x1_2_1 AO,BO, 0 
.endm

.macro KERNEL8x1_1 AREG,BREG,First 
    lxvwsx vs8,  0, \AREG
    lxv vs26,   0(\BREG)
    lxv vs27,   16(\BREG)      
.if \First==1
    xvmulsp      vs0,   vs26,   vs8
    xvmulsp      vs1,   vs27,   vs8  
.else 
    xvmaddasp      vs0,   vs26,   vs8
    xvmaddasp      vs1,   vs27,   vs8  
 .endif
    addi        \AREG, \AREG,  4  
    addi        \BREG, \BREG,  32
.endm

.macro KERNEL8x1_2_1 AREG,BREG,First 
    lxsd v4,    0(\AREG)
    lxv vs26,   0(\BREG)
    lxv vs27,  16(\BREG)      
    lxv vs28,  32(\BREG)
    lxv vs29,  48(\BREG) 
    xxspltw   vs8,  vs36, 1 
    xxspltw   vs9,  vs36, 0  
.if \First==1
    xvmulsp      vs0,   vs26,   vs8
    xvmulsp      vs1,   vs27,   vs8  
    xvmulsp      vs0,   vs28,   vs9
    xvmulsp      vs1,   vs29,   vs9     
.else 
    xvmaddasp      vs0,   vs26,   vs8
    xvmaddasp      vs1,   vs27,   vs8  
    xvmaddasp      vs0,   vs28,   vs9
    xvmaddasp      vs1,   vs29,   vs9 
 .endif
    addi        \AREG, \AREG,  8 
    addi        \BREG, \BREG,  64
.endm

.macro KERNEL8x1_I_4  AREG,BREG,First,OffsetA,OffsetB, Index,IsLast  
    lxv vs4,    DISP4(\Index, 0+\OffsetA)(\AREG)
    xxspltw   vs8,  vs4, 3 
    xxspltw   vs9,  vs4, 2 
    xxspltw   vs10, vs4, 1 
    xxspltw   vs11, vs4, 0
    lxv vs26,   DISP32(\Index, 0+\OffsetB)(\BREG)
    lxv vs27,   DISP32(\Index,16+\OffsetB)(\BREG)
    lxv vs28,   DISP32(\Index,32+\OffsetB)(\BREG)
    lxv vs29,   DISP32(\Index,48+\OffsetB)(\BREG) 
    lxv vs30,   DISP32(\Index,64+ 0+\OffsetB)(\BREG)
    lxv vs31,   DISP32(\Index,64+16+\OffsetB)(\BREG)
    lxv vs32,   DISP32(\Index,64+32+\OffsetB)(\BREG)
    lxv vs33,   DISP32(\Index,64+48+\OffsetB)(\BREG)         
.if \First==1
    xvmulsp      vs0,   vs26,   vs8
    xvmulsp      vs1,   vs27,   vs8  
    xvmulsp      vs0,   vs28,   vs9
    xvmulsp      vs1,   vs29,   vs9     
    xvmulsp      vs0,   vs30,   vs10
    xvmulsp      vs1,   vs31,   vs10  
    xvmulsp      vs0,   vs32,   vs11
    xvmulsp      vs1,   vs33,   vs11     
.else 
    xvmaddasp      vs0,   vs26,   vs8
    xvmaddasp      vs1,   vs27,   vs8  
    xvmaddasp      vs0,   vs28,   vs9
    xvmaddasp      vs1,   vs29,   vs9     
    xvmaddasp      vs0,   vs30,   vs10
    xvmaddasp      vs1,   vs31,   vs10  
    xvmaddasp      vs0,   vs32,   vs11
    xvmaddasp      vs1,   vs33,   vs11  
 .endif
.if \IsLast==1   
    addi        \AREG, \AREG, DISP4(\Index,16)  
    addi        \BREG, \BREG, DISP32(\Index,128)
.endif 
.endm

.macro SAVE8x1
  slwi    T10, LDC ,   1 
  add     T1, CO, LDC  
  add     T2, CO, T10  
  add     T3, T1, T10     
  add     T4, T2, T10 
  add     T5, T3, T10 
  add     T6, T4, T10 
  add     T7, T5, T10 
  /*convert alpha_r for multiply*/
  xscvspdp  vs4,alpha_r
/* v0 corresponds to vs32, do not forget*/
#if !defined(TRMMKERNEL)
  lxssp  v0,0(CO)  
  lxssp  v2,0(T1) 
  lxssp  v4,0(T2) 
  lxssp  v6,0(T3) 
  lxssp  v8,0(T4) 
  lxssp  v10,0(T5) 
  lxssp  v12,0(T6) 
  lxssp  v14,0(T7)
#endif
  xscvspdp  vs24, vs0
  xxspltw   vs25, vs0, 1 
  xxspltw   vs26, vs0, 2 
  xxspltw   vs27, vs0, 3  
  xscvspdp  vs25,vs25
  xscvspdp  vs26,vs26
  xscvspdp  vs27,vs27
  xscvspdp  vs28, vs1
  xxspltw   vs29, vs1, 1 
  xxspltw   vs30, vs1, 2 
  xxspltw   vs31, vs1, 3  
  xscvspdp  vs29,vs29
  xscvspdp  vs30,vs30
  xscvspdp  vs31,vs31
#if defined(TRMMKERNEL)
  xsmuldp  vs32,vs27, vs4 
  xsmuldp  vs34,vs26, vs4 
  xsmuldp  vs36,vs25, vs4 
  xsmuldp  vs38,vs24, vs4 
  xsmuldp  vs40,vs31, vs4 
  xsmuldp  vs42,vs30, vs4 
  xsmuldp  vs44,vs29, vs4 
  xsmuldp  vs46,vs28, vs4 
#else
  xsmaddadp  vs32,vs27, vs4 
  xsmaddadp  vs34,vs26, vs4 
  xsmaddadp  vs36,vs25, vs4 
  xsmaddadp  vs38,vs24, vs4 
  xsmaddadp  vs40,vs31, vs4 
  xsmaddadp  vs42,vs30, vs4 
  xsmaddadp  vs44,vs29, vs4 
  xsmaddadp  vs46,vs28, vs4  
#endif  
  stxssp  v0,0(CO)  
  stxssp  v2,0(T1) 
  stxssp  v4,0(T2) 
  stxssp  v6,0(T3) 
  stxssp  v8,0(T4) 
  stxssp  v10,0(T5) 
  stxssp  v12,0(T6) 
  stxssp  v14,0(T7) 
  addi CO,CO,4
.endm



/**********************************************************************************************
* Macros for N=4 and M=16
**********************************************************************************************/

.macro LOAD4x16_1
   LOAD4x16 1
.endm

.macro LOAD4x16_0
   LOAD4x16 0
.endm

.macro KERNEL4x16_L1_L4  Index,IsLast
  KERNEL4x16_L1_L4_I AO,BO, 0,0, \Index,\IsLast,0
.endm

.macro KERNEL4x16_I1_L4  OffsetA,OffsetB, Index,IsLast
  KERNEL4x16_L1_L4_I  AO,BO, \OffsetA,\OffsetB,\Index,\IsLast,0
.endm

.macro KERNEL4x16_I1_L4_2  OffsetA,OffsetB, Index,IsLast
  KERNEL4x16_L1_L4_I  AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast,0
.endm

.macro KERNEL4x16_I1_L4_3  OffsetA,OffsetB, Index,IsLast
  KERNEL4x16_L1_L4_I  AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast,1
.endm
.macro KERNEL4x16_I1_L2_3  OffsetA,OffsetB, Index,IsLast
   KERNEL4x16_L1_L2_I AO,BO,0,  \OffsetA,\OffsetB,\Index,\IsLast,1
.endm

.macro KERNEL4x16_I2_L4_2  AREG,BREG,OffsetA,OffsetB, Index,IsLast
  KERNEL4x16_L1_L4_I  \AREG,\BREG,  \OffsetA,\OffsetB,\Index,\IsLast,0
.endm

.macro KERNEL4x16_I2_L4_3  AREG,BREG,OffsetA,OffsetB, Index,IsLast
  KERNEL4x16_L1_L4_I \AREG,\BREG,  \OffsetA,\OffsetB,\Index,\IsLast,1
.endm

.macro Zero4X16
    xxlxor		vs32,	vs32,	vs32
    xxlxor		vs33,	vs33,	vs33
	xxlxor		vs34,	vs34,	vs34
	xxlxor		vs35,	vs35,	vs35
	xxlxor		vs36,	vs36,	vs36
	xxlxor		vs37,	vs37,	vs37
	xxlxor		vs38,	vs38,	vs38
	xxlxor		vs39,	vs39,	vs39
	xxlxor		vs40,	vs40,	vs40
	xxlxor		vs41,	vs41,	vs41
	xxlxor		vs42,	vs42,	vs42
	xxlxor		vs43,	vs43,	vs43
	xxlxor		vs44,	vs44,	vs44
	xxlxor		vs45,	vs45,	vs45
	xxlxor		vs46,	vs46,	vs46
	xxlxor		vs47,	vs47,	vs47	
.endm

.macro LOAD4x16  Zero

	lxv	vs24,	0(BO) 
	lxv	vs0,	 0(AO)
	lxv	vs1,	16(AO)
	lxv	vs2,	32(AO)
	lxv	vs3,	48(AO)
	xxperm  	vs26,	vs24,		permute_mask 	
	xxpermdi	vs25,	vs24,	vs24,2 
	xxpermdi	vs27,	vs26,	vs26,2	 	

.if \Zero==1 
    xxlxor		vs32,	vs32,	vs32
    xxlxor		vs33,	vs33,	vs33
	xxlxor		vs34,	vs34,	vs34
	xxlxor		vs35,	vs35,	vs35
	xxlxor		vs36,	vs36,	vs36
	xxlxor		vs37,	vs37,	vs37
	xxlxor		vs38,	vs38,	vs38
	xxlxor		vs39,	vs39,	vs39
	xxlxor		vs40,	vs40,	vs40
	xxlxor		vs41,	vs41,	vs41
	xxlxor		vs42,	vs42,	vs42
	xxlxor		vs43,	vs43,	vs43
	xxlxor		vs44,	vs44,	vs44
	xxlxor		vs45,	vs45,	vs45
	xxlxor		vs46,	vs46,	vs46
	xxlxor		vs47,	vs47,	vs47
 
.endif
.endm

.macro END4x16_NORMAL
  END4x16 0, AO, BO, 64,16 
.endm

.macro END4x16 First, AREG, BREG, OffsetA, OffsetB

.if \OffsetB != 0 
    addi        \BREG, \BREG, \OffsetB 
.endif
.if \OffsetA != 0 
    addi        \AREG, \AREG, \OffsetA 
.endif  

.if \First==1
    xvmulsp     vs32, vs0,vs24
    xvmulsp     vs33, vs1,vs24
    xvmulsp     vs34, vs2,vs24  
    xvmulsp     vs35, vs3,vs24  

    xvmulsp     vs36, vs0,vs25
    xvmulsp     vs37, vs1,vs25
    xvmulsp     vs38, vs2,vs25  
    xvmulsp     vs39, vs3,vs25

    xvmulsp     vs40, vs0,vs26
    xvmulsp     vs41, vs1,vs26
    xvmulsp     vs42, vs2,vs26  
    xvmulsp     vs43, vs3,vs26

    xvmulsp     vs44, vs0,vs27
    xvmulsp     vs45, vs1,vs27
    xvmulsp     vs46, vs2,vs27  
    xvmulsp     vs47, vs3,vs27

.else
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

.endif
.endm  

.macro KERNEL4x16_L1_L4_I  AREG,BREG,   OffsetA,OffsetB, Index,IsLast ,Complete

	lxv	vs8,	DISP16(\Index, 0+\OffsetB)(\BREG) 

 	lxv	vs4,	DISP64(\Index, 0+\OffsetA)(\AREG)
	lxv	vs5,	DISP64(\Index,16+\OffsetA)(\AREG)
	lxv	vs6,	DISP64(\Index,32+\OffsetA)(\AREG)
	lxv	vs7,	DISP64(\Index,48+\OffsetA)(\AREG) 

	xxperm  	vs10,	vs8,		permute_mask 
	xxpermdi	vs9,	vs8,	vs8,2	  

    xvmaddasp		vs32, vs0,vs24
	xvmaddasp		vs33, vs1,vs24
	xvmaddasp		vs34, vs2,vs24	
	xvmaddasp		vs35, vs3,vs24	 

    xvmaddasp		vs36, vs0,vs25
	xvmaddasp		vs37, vs1,vs25
	xvmaddasp		vs38, vs2,vs25	
	xvmaddasp		vs39, vs3,vs25 

 	xxpermdi	vs11,	vs10,	vs10,2	 

    xvmaddasp		vs40, vs0,vs26
	xvmaddasp		vs41, vs1,vs26
	xvmaddasp		vs42, vs2,vs26	
	xvmaddasp		vs43, vs3,vs26

    xvmaddasp		vs44, vs0,vs27
	xvmaddasp		vs45, vs1,vs27
	xvmaddasp		vs46, vs2,vs27	
	xvmaddasp		vs47, vs3,vs27



	lxv	vs24,	DISP16(\Index,16+\OffsetB)(\BREG) 

	lxv	vs0,	DISP64(\Index,64+\OffsetA)(\AREG)
	lxv	vs1,	DISP64(\Index,64+16+\OffsetA)(\AREG)
	lxv	vs2,	DISP64(\Index,64+32+\OffsetA)(\AREG)
	lxv	vs3,	DISP64(\Index,64+48+\OffsetA)(\AREG)

	xxperm  	vs26,	vs24,		permute_mask 
	xxpermdi	vs25,	vs24,	vs24,2	    
 

    xvmaddasp		vs32, vs4,vs8
	xvmaddasp		vs33, vs5,vs8
	xvmaddasp		vs34, vs6,vs8	
	xvmaddasp		vs35, vs7,vs8	
 
    xvmaddasp		vs36, vs4,vs9
	xvmaddasp		vs37, vs5,vs9
	xvmaddasp		vs38, vs6,vs9	
	xvmaddasp		vs39, vs7,vs9
         
	xxpermdi	vs27,	vs26,	vs26,2	 	

    xvmaddasp		vs40, vs4,vs10
	xvmaddasp		vs41, vs5,vs10
	xvmaddasp		vs42, vs6,vs10	
	xvmaddasp		vs43, vs7,vs10

    xvmaddasp		vs44, vs4,vs11
	xvmaddasp		vs45, vs5,vs11
	xvmaddasp		vs46, vs6,vs11	
	xvmaddasp		vs47, vs7,vs11
 

	lxv	vs8,	DISP16(\Index,32+\OffsetB)(\BREG) 

 	lxv	vs4,	DISP64(\Index,128+0+\OffsetA)(\AREG)
	lxv	vs5,	DISP64(\Index,128+16+\OffsetA)(\AREG)
	lxv	vs6,	DISP64(\Index,128+32+\OffsetA)(\AREG)
	lxv	vs7,	DISP64(\Index,128+48+\OffsetA)(\AREG) 

	xxperm  	vs10,	vs8,		permute_mask 
	xxpermdi	vs9,	vs8,	vs8,2	  

    xvmaddasp		vs32, vs0,vs24
	xvmaddasp		vs33, vs1,vs24
	xvmaddasp		vs34, vs2,vs24	
	xvmaddasp		vs35, vs3,vs24	 

    xvmaddasp		vs36, vs0,vs25
	xvmaddasp		vs37, vs1,vs25
	xvmaddasp		vs38, vs2,vs25	
	xvmaddasp		vs39, vs3,vs25

 	xxpermdi	vs11,	vs10,	vs10,2	 

    xvmaddasp		vs40, vs0,vs26
	xvmaddasp		vs41, vs1,vs26
	xvmaddasp		vs42, vs2,vs26	
	xvmaddasp		vs43, vs3,vs26

    xvmaddasp		vs44, vs0,vs27
	xvmaddasp		vs45, vs1,vs27
	xvmaddasp		vs46, vs2,vs27	
	xvmaddasp		vs47, vs3,vs27

 
 
.if \Complete==0
	lxv	vs24,	DISP16(\Index,48+\OffsetB)(\BREG) 

	lxv	vs0,	DISP64(\Index,192+\OffsetA)(\AREG)
	lxv	vs1,	DISP64(\Index,192+16+\OffsetA)(\AREG) 
	lxv	vs2,	DISP64(\Index,192+32+\OffsetA)(\AREG)
	lxv	vs3,	DISP64(\Index,192+48+\OffsetA)(\AREG)

	xxperm  	vs26,	vs24,	permute_mask 	
	xxpermdi	vs25,	vs24,	vs24,2  	

.endif 
.if \IsLast==1	
.if \Complete==1
  
	addi		\BREG, \BREG,  DISP16(\Index,16*3+\OffsetB)
	addi		\AREG, \AREG, DISP64(\Index,64*3+\OffsetA)
.else
  
	addi		\BREG, \BREG,  DISP16(\Index,64)
	addi		\AREG, \AREG, DISP64(\Index,256)
.endif
.endif   
 
    xvmaddasp		vs32, vs4,vs8
	xvmaddasp		vs33, vs5,vs8
	xvmaddasp		vs34, vs6,vs8	
	xvmaddasp		vs35, vs7,vs8	 
 
    xvmaddasp		vs36, vs4,vs9
	xvmaddasp		vs37, vs5,vs9
	xvmaddasp		vs38, vs6,vs9	
	xvmaddasp		vs39, vs7,vs9
  
.if \Complete==0        
	xxpermdi	vs27,	vs26,	vs26,2	 
 	
.endif
 
    xvmaddasp		vs40, vs4,vs10
	xvmaddasp		vs41, vs5,vs10
	xvmaddasp		vs42, vs6,vs10	
	xvmaddasp		vs43, vs7,vs10

    xvmaddasp		vs44, vs4,vs11
	xvmaddasp		vs45, vs5,vs11
	xvmaddasp		vs46, vs6,vs11	
	xvmaddasp		vs47, vs7,vs11

 

.endm

.macro KERNEL4x16 First

  LOAD4x16 0
  END4x16 \First, AO, BO, 64,16 
.endm

.macro KERNEL4x16_L1_L2_I  AREG,BREG,First,OffsetA,OffsetB, Index,IsLast ,Complete
	
	lxv	vs8,	DISP8(\Index, 0+\OffsetB)(\BREG) 
 	lxv	vs4,	DISP32(\Index, 0+\OffsetA)(\AREG)
	lxv	vs5,	DISP32(\Index,16+\OffsetA)(\AREG)
	lxv	vs6,	DISP32(\Index,32+\OffsetA)(\AREG)
	lxv	vs7,	DISP32(\Index,48+\OffsetA)(\AREG) 

	xxperm  	vs10,	vs8,		permute_mask 
	xxpermdi	vs9,	vs8,	vs8,2	  
.if \First==1
    xvmulsp		vs32, vs0,vs24
	xvmulsp		vs33, vs1,vs24
	xvmulsp		vs34, vs2,vs24	
	xvmulsp		vs35, vs3,vs24	

    xvmulsp		vs36, vs0,vs25
	xvmulsp		vs37, vs1,vs25
	xvmulsp		vs38, vs2,vs25	
	xvmulsp		vs39, vs3,vs25	
.else
    xvmaddasp		vs32, vs0,vs24
	xvmaddasp		vs33, vs1,vs24
	xvmaddasp		vs34, vs2,vs24	
	xvmaddasp		vs35, vs3,vs24

    xvmaddasp		vs36, vs0,vs25
	xvmaddasp		vs37, vs1,vs25
	xvmaddasp		vs38, vs2,vs25	
	xvmaddasp		vs39, vs3,vs25		
.endif

 	xxpermdi	vs11,	vs10,	vs10,2	 	
 
.if \First==1  
    xvmulsp		vs40, vs0,vs26
	xvmulsp		vs41, vs1,vs26
	xvmulsp		vs42, vs2,vs26	
	xvmulsp		vs43, vs3,vs26

    xvmulsp		vs44, vs0,vs27
	xvmulsp		vs45, vs1,vs27
	xvmulsp		vs46, vs2,vs27	
	xvmulsp		vs47, vs3,vs27

  
.else 
    xvmaddasp		vs40, vs0,vs26
	xvmaddasp		vs41, vs1,vs26
	xvmaddasp		vs42, vs2,vs26	
	xvmaddasp		vs43, vs3,vs26

    xvmaddasp		vs44, vs0,vs27
	xvmaddasp		vs45, vs1,vs27
	xvmaddasp		vs46, vs2,vs27	
	xvmaddasp		vs47, vs3,vs27
 

.endif
.if \Complete==0
	lxv	vs24,	DISP8(\Index,16+\OffsetB)(\BREG) 
	lxv	vs0,	DISP32(\Index,64+\OffsetA)(\AREG)
	lxv	vs1,	DISP32(\Index,64+16+\OffsetA)(\AREG)
	lxv	vs2,	DISP32(\Index,64+32+\OffsetA)(\AREG)
	lxv	vs3,	DISP32(\Index,64+48+\OffsetA)(\AREG)

	xxperm  	vs26,	vs24,	permute_mask 
	xxpermdi	vs25,	vs24,	vs24,2	  
.endif    
.if \IsLast==1	
.if \Complete==1
 	addi		\BREG, \BREG,  DISP8(\Index,16+\OffsetB) 
	addi		\AREG, \AREG, DISP32(\Index,64+\OffsetA)

.else
  	addi		\BREG, \BREG,  DISP8(\Index,32)
	addi		\AREG, \AREG, DISP32(\Index,128) 
.endif
.endif

.if \First==1
    xvmulsp		vs32, vs4,vs8
	xvmulsp		vs33, vs5,vs8
	xvmulsp		vs34, vs6,vs8	
	xvmulsp		vs35, vs7,vs8

    xvmulsp		vs36, vs4,vs9
	xvmulsp		vs37, vs5,vs9
	xvmulsp		vs38, vs6,vs9	
	xvmulsp		vs39, vs7,vs9
.else
    xvmaddasp		vs32, vs4,vs8
	xvmaddasp		vs33, vs5,vs8
	xvmaddasp		vs34, vs6,vs8	
	xvmaddasp		vs35, vs7,vs8	

    xvmaddasp		vs36, vs4,vs9
	xvmaddasp		vs37, vs5,vs9
	xvmaddasp		vs38, vs6,vs9	
	xvmaddasp		vs39, vs7,vs9
.endif 
 
.if \Complete==0        
	xxpermdi	vs27,	vs26,	vs26,2	 
 
.endif
.if \First==1  
    xvmulsp		vs40, vs4,vs10
	xvmulsp		vs41, vs5,vs10
	xvmulsp		vs42, vs6,vs10	
	xvmulsp		vs43, vs7,vs10

    xvmulsp		vs44, vs4,vs11
	xvmulsp		vs45, vs5,vs11
	xvmulsp		vs46, vs6,vs11	
	xvmulsp		vs47, vs7,vs11

 

.else 
    xvmaddasp		vs40, vs4,vs10
	xvmaddasp		vs41, vs5,vs10
	xvmaddasp		vs42, vs6,vs10	
	xvmaddasp		vs43, vs7,vs10

    xvmaddasp		vs44, vs4,vs11
	xvmaddasp		vs45, vs5,vs11
	xvmaddasp		vs46, vs6,vs11	
	xvmaddasp		vs47, vs7,vs11

 

.endif

.endm

 
.macro SAVE4x16

  slwi    T10, LDC ,   1 
  add     T1, CO, LDC 

  add     T2, CO, T10  
  add     T3, T1, T10  

  
 
    xxmrglw     vs8,    vs32,   vs44
    xxmrglw     vs10,   vs36,   vs40  

    xxmrghw     vs1,    vs32,   vs44
    xxmrghw     vs0,    vs36,   vs40

    xxmrglw     vs12,   vs33,   vs45
    xxmrglw     vs14,   vs37,   vs41  

    xxmrghw     vs2,    vs37,   vs41
    xxmrghw     vs3,    vs33,   vs45

    xxmrglw     vs16,   vs34,   vs46
    xxmrglw     vs18,   vs38,   vs42   

    xxlor      vs9, vs8,    vs8
    xxlor      vs11,    vs10,   vs10 

    xxmrghw     vs4,    vs38,   vs42
    xxmrghw     vs5,    vs34,   vs46

    xxlor      vs13,    vs12,   vs12
    xxlor      vs15,    vs14,   vs14

    xxmrglw     vs24,   vs35,   vs47
    xxmrglw     vs26,   vs39,   vs43  

    xxlor      vs17,    vs16,   vs16
    xxlor      vs19,    vs18,   vs18

    xxmrghw     vs30,   vs39,   vs43 
    xxmrghw     vs31,   vs35,   vs47

    xxperm      vs8,    vs0,    save_permute_1
    xxperm      vs10,   vs1,    save_permute_1
    xxperm      vs9,    vs0,    save_permute_2  
    xxperm      vs11,   vs1,    save_permute_2      

#ifndef TRMMKERNEL    
    lxv        vs32, 0(CO)
    lxv        vs33, 16(CO) 
    lxv        vs34, 32(CO)  
    lxv        vs35, 48(CO)      
#endif
    xxlor      vs25,    vs24,   vs24
    xxlor      vs27,    vs26,   vs26 

#ifndef TRMMKERNEL    
    lxv        vs36, 0(T1)
    lxv        vs37, 16(T1) 
    lxv        vs38, 32(T1)  
    lxv        vs39, 48(T1)     
#endif
#ifndef TRMMKERNEL       
    lxv        vs40, 0(T2)
    lxv        vs41, 16(T2) 
    lxv        vs42, 32(T2)  
    lxv        vs43, 48(T2)     
#endif  
#ifndef TRMMKERNEL    
    lxv        vs44, 0(T3)
    lxv        vs45, 16(T3) 
    lxv        vs46, 32(T3)  
    lxv        vs47, 48(T3)                 
#endif  

    xxperm     vs12,    vs2,    save_permute_1
    xxperm     vs14,    vs3,    save_permute_1
       
    xxperm     vs13,    vs2,    save_permute_2   
    xxperm     vs15,    vs3,    save_permute_2      

    xxperm     vs16,    vs4,    save_permute_1
    xxperm     vs18,    vs5,    save_permute_1
      
    xxperm     vs17,    vs4,    save_permute_2   
    xxperm     vs19,    vs5,    save_permute_2      

    xxperm     vs24,    vs30,   save_permute_1
    xxperm     vs26,    vs31,   save_permute_1 
         
    xxperm     vs25,    vs30,   save_permute_2   
    xxperm     vs27,    vs31,   save_permute_2  


    /* multiply add normal way */
 
#ifdef TRMMKERNEL
    xvmulsp     vs32,   vs8,    alpha_r 
    xvmulsp     vs33,   vs12,   alpha_r   
    xvmulsp     vs34,   vs16,   alpha_r 
    xvmulsp     vs35,   vs24,   alpha_r  
    xvmulsp     vs36,   vs9,    alpha_r 
    xvmulsp     vs37,   vs13,   alpha_r  
    xvmulsp     vs38,   vs17,   alpha_r 
    xvmulsp     vs39,   vs25,   alpha_r               
#else 
    xvmaddasp   vs32,   vs8,    alpha_r 
    xvmaddasp   vs33,   vs12,   alpha_r   
    xvmaddasp   vs34,   vs16,   alpha_r 
    xvmaddasp   vs35,   vs24,   alpha_r  
    xvmaddasp   vs36,   vs9,    alpha_r 
    xvmaddasp   vs37,   vs13,   alpha_r   
    xvmaddasp   vs38,   vs17,   alpha_r 
    xvmaddasp   vs39,   vs25,   alpha_r         
#endif 



#ifdef TRMMKERNEL
    xvmulsp     vs40,   vs10,   alpha_r 
    xvmulsp     vs41,   vs14,   alpha_r 
    xvmulsp     vs42,   vs18,   alpha_r 
    xvmulsp     vs43,   vs26,   alpha_r  
    xvmulsp     vs44,   vs11,   alpha_r 
    xvmulsp     vs45,   vs15,   alpha_r  
    xvmulsp     vs46,   vs19,   alpha_r 
    xvmulsp     vs47,   vs27,   alpha_r                   
#else

    xvmaddasp   vs40,   vs10,   alpha_r 
    xvmaddasp   vs41,   vs14,   alpha_r   
    xvmaddasp   vs42,   vs18,   alpha_r 
    xvmaddasp   vs43,   vs26,   alpha_r  
    xvmaddasp   vs44,   vs11,   alpha_r 
    xvmaddasp   vs45,   vs15,   alpha_r 
    xvmaddasp   vs46,   vs19,   alpha_r 
    xvmaddasp   vs47,   vs27,   alpha_r  
        
#endif  

    stxv        vs32, 0(CO)
    stxv        vs33, 16(CO) 
    stxv        vs34, 32(CO)  
    stxv        vs35, 48(CO)  

    stxv        vs36, 0(T1)
    stxv        vs37, 16(T1)  
    stxv        vs38, 32(T1)  
    stxv        vs39, 48(T1)

    stxv        vs40, 0(T2)
    stxv        vs41, 16(T2)  
    stxv        vs42, 32(T2)  
    stxv        vs43, 48(T2)  
    stxv        vs44, 0(T3)
    stxv        vs45, 16(T3) 
    stxv        vs46, 32(T3)  
    stxv        vs47, 48(T3)
   
    addi CO,CO,64


.endm



/**********************************************************************************************
* Macros for N=4 and M=8
**********************************************************************************************/

.macro LOAD4x8_1
   LOAD4x8 1
.endm

.macro LOAD4x8_0
   LOAD4x8 0
.endm

.macro KERNEL4x8_L1_L4  Index,IsLast
  KERNEL4x8_L1_L4_I AO,BO, 0,0, \Index,\IsLast,0
.endm

.macro KERNEL4x8_I1_L4  OffsetA,OffsetB, Index,IsLast
  KERNEL4x8_L1_L4_I  AO,BO, \OffsetA,\OffsetB,\Index,\IsLast,0
.endm

.macro KERNEL4x8_I1_L4_2  OffsetA,OffsetB, Index,IsLast
  KERNEL4x8_L1_L4_I  AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast,0
.endm

.macro KERNEL4x8_I1_L4_3  OffsetA,OffsetB, Index,IsLast
  KERNEL4x8_L1_L4_I  AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast,1
.endm
.macro KERNEL4x8_I1_L2_3  OffsetA,OffsetB, Index,IsLast
   KERNEL4x8_L1_L2_I AO,BO,0,  \OffsetA,\OffsetB,\Index,\IsLast,1
.endm

.macro KERNEL4x8_I2_L4_2  AREG,BREG,OffsetA,OffsetB, Index,IsLast
  KERNEL4x8_L1_L4_I  \AREG,\BREG,  \OffsetA,\OffsetB,\Index,\IsLast,0
.endm

.macro KERNEL4x8_I2_L4_3  AREG,BREG,OffsetA,OffsetB, Index,IsLast
  KERNEL4x8_L1_L4_I \AREG,\BREG,  \OffsetA,\OffsetB,\Index,\IsLast,1
.endm

.macro END4x8_NORMAL
  END4x8 0, AO, BO, 32,16 
.endm

.macro Zero4X8
    xxlxor      vs32,   vs32,   vs32
    xxlxor      vs33,   vs33,   vs33
 
    xxlxor      vs36,   vs36,   vs36
    xxlxor      vs37,   vs37,   vs37
 
    xxlxor      vs40,   vs40,   vs40
    xxlxor      vs41,   vs41,   vs41
 
    xxlxor      vs44,   vs44,   vs44
    xxlxor      vs45,   vs45,   vs45
    
.endm

.macro LOAD4x8  Zero

    lxv vs24,   0(BO) 
    lxv vs0,     0(AO)
    lxv vs1,    16(AO)

    xxperm      vs26,   vs24,       permute_mask    
    xxpermdi    vs25,   vs24,   vs24,2      

    xxpermdi    vs27,   vs26,   vs26,2      

.if \Zero==1 
    xxlxor      vs32,   vs32,   vs32
    xxlxor      vs33,   vs33,   vs33 
    xxlxor      vs36,   vs36,   vs36
    xxlxor      vs37,   vs37,   vs37
    xxlxor      vs40,   vs40,   vs40
    xxlxor      vs41,   vs41,   vs41 
    xxlxor      vs44,   vs44,   vs44
    xxlxor      vs45,   vs45,   vs45 
 
.endif
.endm


.macro END4x8 First, AREG, BREG, OffsetA, OffsetB

.if \OffsetB != 0 
    addi        \BREG, \BREG, \OffsetB 
.endif
.if \OffsetA != 0 
    addi        \AREG, \AREG, \OffsetA 
.endif  

.if \First==1
    xvmulsp     vs32, vs0,vs24
    xvmulsp     vs33, vs1,vs24

    xvmulsp     vs36, vs0,vs25
    xvmulsp     vs37, vs1,vs25

    xvmulsp     vs40, vs0,vs26
    xvmulsp     vs41, vs1,vs26

    xvmulsp     vs44, vs0,vs27
    xvmulsp     vs45, vs1,vs27
 

.else
    xvmaddasp       vs32, vs0,vs24
    xvmaddasp       vs33, vs1,vs24

    xvmaddasp       vs36, vs0,vs25
    xvmaddasp       vs37, vs1,vs25

    xvmaddasp       vs40, vs0,vs26
    xvmaddasp       vs41, vs1,vs26

    xvmaddasp       vs44, vs0,vs27
    xvmaddasp       vs45, vs1,vs27
 

.endif
.endm  

.macro KERNEL4x8_L1_L4_I  AREG,BREG,   OffsetA,OffsetB, Index,IsLast ,Complete

    lxv vs8,    DISP16(\Index, 0+\OffsetB)(\BREG) 

    lxv vs4,    DISP32(\Index, 0+\OffsetA)(\AREG)
    lxv vs5,    DISP32(\Index,16+\OffsetA)(\AREG)

    xxperm      vs10,   vs8,        permute_mask    
    xxpermdi    vs9,    vs8,    vs8,2     

    xvmaddasp       vs32, vs0,vs24
    xvmaddasp       vs33, vs1,vs24

    xvmaddasp       vs36, vs0,vs25
    xvmaddasp       vs37, vs1,vs25

    xxpermdi    vs11,   vs10,   vs10,2   

    xvmaddasp       vs40, vs0,vs26
    xvmaddasp       vs41, vs1,vs26

    xvmaddasp       vs44, vs0,vs27
    xvmaddasp       vs45, vs1,vs27

 

    lxv vs24,   DISP16(\Index,16+\OffsetB)(\BREG) 

    lxv vs0,    DISP32(\Index,32+\OffsetA)(\AREG)
    lxv vs1,    DISP32(\Index,32+16+\OffsetA)(\AREG)

    xxperm      vs26,   vs24,       permute_mask   
    xxpermdi    vs25,   vs24,   vs24,2      

    xvmaddasp       vs32, vs4,vs8
    xvmaddasp       vs33, vs5,vs8

    xvmaddasp       vs36, vs4,vs9
    xvmaddasp       vs37, vs5,vs9

    xxpermdi    vs27,   vs26,   vs26,2       

    xvmaddasp       vs40, vs4,vs10
    xvmaddasp       vs41, vs5,vs10

    xvmaddasp       vs44, vs4,vs11
    xvmaddasp       vs45, vs5,vs11

 

    lxv vs8,    DISP16(\Index,32+\OffsetB)(\BREG) 

    lxv vs4,    DISP32(\Index,64+0+\OffsetA)(\AREG)
    lxv vs5,    DISP32(\Index,64+16+\OffsetA)(\AREG)

    xxperm      vs10,   vs8,        permute_mask     
    xxpermdi    vs9,    vs8,    vs8,2     

    xvmaddasp       vs32, vs0,vs24
    xvmaddasp       vs33, vs1,vs24

    xvmaddasp       vs36, vs0,vs25
    xvmaddasp       vs37, vs1,vs25

    xxpermdi    vs11,   vs10,   vs10,2   

    xvmaddasp       vs40, vs0,vs26
    xvmaddasp       vs41, vs1,vs26

    xvmaddasp       vs44, vs0,vs27
    xvmaddasp       vs45, vs1,vs27

 

.if \Complete==0
    lxv vs24,   DISP16(\Index,48+\OffsetB)(\BREG) 

    lxv vs0,    DISP32(\Index,96+\OffsetA)(\AREG)
    lxv vs1,    DISP32(\Index,96+16+\OffsetA)(\AREG) 

    xxperm      vs26,   vs24,   permute_mask     
    xxpermdi    vs25,   vs24,   vs24,2      

.endif 
.if \IsLast==1  
.if \Complete==1
  
    addi        \BREG, \BREG,  DISP16(\Index,16*3+\OffsetB)
    addi        \AREG, \AREG, DISP32(\Index,32*3+\OffsetA)
.else
  
    addi        \BREG, \BREG,  DISP16(\Index,64)
    addi        \AREG, \AREG, DISP32(\Index,128)
.endif
.endif   
 
    xvmaddasp       vs32, vs4,vs8
    xvmaddasp       vs33, vs5,vs8

    xvmaddasp       vs36, vs4,vs9
    xvmaddasp       vs37, vs5,vs9

.if \Complete==0        
    xxpermdi    vs27,   vs26,   vs26,2    
    
.endif
 
    xvmaddasp       vs40, vs4,vs10
    xvmaddasp       vs41, vs5,vs10

    xvmaddasp       vs44, vs4,vs11
    xvmaddasp       vs45, vs5,vs11

 

.endm

.macro KERNEL4x8 First

  LOAD4x8 0
  END4x8 \First, AO, BO, 32,16  
.endm

.macro KERNEL4x8_L1_L2_I  AREG,BREG,First,OffsetA,OffsetB, Index,IsLast ,Complete
    
    lxv vs8,     DISP8(\Index, 0+\OffsetB)(\BREG) 
    lxv vs4,    DISP16(\Index, 0+\OffsetA)(\AREG)
    lxv vs5,    DISP16(\Index,16+\OffsetA)(\AREG)

    xxperm      vs10,   vs8,        permute_mask  
    xxpermdi    vs9,    vs8,    vs8,2     
.if \First==1
    xvmulsp     vs32, vs0,vs24
    xvmulsp     vs33, vs1,vs24

    xvmulsp     vs36, vs0,vs25
    xvmulsp     vs37, vs1,vs25

.else
    xvmaddasp       vs32, vs0,vs24
    xvmaddasp       vs33, vs1,vs24

    xvmaddasp       vs36, vs0,vs25
    xvmaddasp       vs37, vs1,vs25

.endif

    xxpermdi    vs11,   vs10,   vs10,2    
 
.if \First==1  
    xvmulsp     vs40, vs0,vs26
    xvmulsp     vs41, vs1,vs26

    xvmulsp     vs44, vs0,vs27
    xvmulsp     vs45, vs1,vs27
 

.else 
    xvmaddasp       vs40, vs0,vs26
    xvmaddasp       vs41, vs1,vs26

    xvmaddasp       vs44, vs0,vs27
    xvmaddasp       vs45, vs1,vs27
 

.endif
.if \Complete==0
    lxv vs24,    DISP8(\Index,16+\OffsetB)(\BREG) 

    lxv vs0,    DISP16(\Index,32+\OffsetA)(\AREG)
    lxv vs1,    DISP16(\Index,32+16+\OffsetA)(\AREG)

    xxperm      vs26,   vs24,   permute_mask   
    xxpermdi    vs25,   vs24,   vs24,2    
.endif    
.if \IsLast==1  
.if \Complete==1
    addi        \BREG, \BREG,   DISP8(\Index,16+\OffsetB) 
    addi        \AREG, \AREG,  DISP16(\Index,32+\OffsetA)

.else
    addi        \BREG, \BREG,   DISP8(\Index,32)
    addi        \AREG, \AREG,  DISP16(\Index,64) 
.endif
.endif

.if \First==1
    xvmulsp     vs32, vs4,vs8
    xvmulsp     vs33, vs5,vs8

    xvmulsp     vs36, vs4,vs9
    xvmulsp     vs37, vs5,vs9

.else
    xvmaddasp       vs32, vs4,vs8
    xvmaddasp       vs33, vs5,vs8

    xvmaddasp       vs36, vs4,vs9
    xvmaddasp       vs37, vs5,vs9

.endif 
 
.if \Complete==0        
    xxpermdi    vs27,   vs26,   vs26,2   
 
.endif
.if \First==1  
    xvmulsp     vs40, vs4,vs10
    xvmulsp     vs41, vs5,vs10

    xvmulsp     vs44, vs4,vs11
    xvmulsp     vs45, vs5,vs11
 
.else 
    xvmaddasp       vs40, vs4,vs10
    xvmaddasp       vs41, vs5,vs10

    xvmaddasp       vs44, vs4,vs11
    xvmaddasp       vs45, vs5,vs11 

.endif

.endm


.macro SAVE4x8 
 
  slwi    T10, LDC ,   1 
  add     T1, CO, LDC 

  add     T2, CO, T10  
  add     T3, T1, T10  

 

#ifndef TRMMKERNEL    
    lxv        vs34, 0(CO)
    lxv        vs35, 16(CO)      
    lxv        vs38, 0(T1)
    lxv        vs39, 16(T1)  
    lxv        vs42, 0(T2)
    lxv        vs43, 16(T2)     
    lxv        vs46, 0(T3)
    lxv        vs47, 16(T3)  

 
#endif  

    xxmrglw     vs8,    vs32,   vs44
    xxmrglw     vs10,   vs36,   vs40  

    xxmrghw     vs1,    vs32,   vs44
    xxmrghw     vs0,    vs36,   vs40

    xxmrglw     vs12,   vs33,   vs45
    xxmrglw     vs14,   vs37,   vs41  

    xxmrghw     vs2,    vs37,   vs41
    xxmrghw     vs3,    vs33,   vs45

    xxlor      vs9, vs8,    vs8
    xxlor      vs11,    vs10,   vs10 
 
    xxlor      vs13,    vs12,   vs12
    xxlor      vs15,    vs14,   vs14

    xxperm      vs8,    vs0,    save_permute_1
    xxperm      vs10,   vs1,    save_permute_1
    xxperm      vs9,    vs0,    save_permute_2  
    xxperm      vs11,   vs1,    save_permute_2      

    xxperm     vs12,    vs2,    save_permute_1
    xxperm     vs14,    vs3,    save_permute_1
      
    xxperm     vs13,    vs2,    save_permute_2   
    xxperm     vs15,    vs3,    save_permute_2      


    /* multiply add normal way */
 
#ifdef TRMMKERNEL
    xvmulsp     vs34,   vs8,    alpha_r 
    xvmulsp     vs35,   vs12,   alpha_r 
    xvmulsp     vs38,   vs9,    alpha_r 
    xvmulsp     vs39,   vs13,   alpha_r 
    xvmulsp     vs42,   vs10,   alpha_r 
    xvmulsp     vs43,   vs14,   alpha_r 
    xvmulsp     vs46,   vs11,   alpha_r 
    xvmulsp     vs47,   vs15,   alpha_r                    
#else 
    xvmaddasp   vs34,   vs8,    alpha_r 
    xvmaddasp   vs35,   vs12,   alpha_r 
    xvmaddasp   vs38,   vs9,    alpha_r 
    xvmaddasp   vs39,   vs13,   alpha_r  
    xvmaddasp   vs42,   vs10,   alpha_r 
    xvmaddasp   vs43,   vs14,   alpha_r   
    xvmaddasp   vs46,   vs11,   alpha_r 
    xvmaddasp   vs47,   vs15,   alpha_r                     
#endif     
 
    
    stxv        vs34, 0(CO)
    stxv        vs35, 16(CO)  
    stxv        vs38, 0(T1)
    stxv        vs39, 16(T1)  
    stxv        vs42, 0(T2)
    stxv        vs43, 16(T2)     
    stxv        vs46, 0(T3)
    stxv        vs47, 16(T3)  
  

    addi CO,CO,32

.endm


/**********************************************************************************************
* Macros for N=4 and M=4
**********************************************************************************************/

.macro LOAD4x4_1
   LOAD4x4 1
.endm

.macro LOAD4x4_0
   LOAD4x4 0
.endm

.macro KERNEL4x4_L1_L4  Index,IsLast
  KERNEL4x4_L1_L4_I AO,BO, 0,0, \Index,\IsLast,0
.endm

.macro KERNEL4x4_I1_L4  OffsetA,OffsetB, Index,IsLast
  KERNEL4x4_L1_L4_I  AO,BO, \OffsetA,\OffsetB,\Index,\IsLast,0
.endm

.macro KERNEL4x4_I1_L4_2  OffsetA,OffsetB, Index,IsLast
  KERNEL4x4_L1_L4_I  AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast,0
.endm

.macro KERNEL4x4_I1_L4_3  OffsetA,OffsetB, Index,IsLast
  KERNEL4x4_L1_L4_I  AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast,1
.endm
.macro KERNEL4x4_I1_L2_3  OffsetA,OffsetB, Index,IsLast
   KERNEL4x4_L1_L2_I AO,BO,0,  \OffsetA,\OffsetB,\Index,\IsLast,1
.endm

.macro KERNEL4x4_I2_L4_2  AREG,BREG,OffsetA,OffsetB, Index,IsLast
  KERNEL4x4_L1_L4_I  \AREG,\BREG,  \OffsetA,\OffsetB,\Index,\IsLast,0
.endm

.macro KERNEL4x4_I2_L4_3  AREG,BREG,OffsetA,OffsetB, Index,IsLast
  KERNEL4x4_L1_L4_I \AREG,\BREG,  \OffsetA,\OffsetB,\Index,\IsLast,1
.endm

.macro Zero4X4
    xxlxor      vs32,   vs32,   vs32
    xxlxor      vs33,   vs33,   vs33 
    xxlxor      vs34,   vs34,   vs34
    xxlxor      vs35,   vs35,   vs35
 
.endm

.macro LOAD4x4  Zero

    lxv vs0,     0(AO)
    lxv vs24,   0(BO) 



    xxperm      vs2,   vs0,       permute_mask  
    xxpermdi    vs1,   vs0,   vs0,2      
    xxpermdi    vs3,   vs2,   vs2,2        

.if \Zero==1 
    xxlxor      vs32,   vs32,   vs32
    xxlxor      vs33,   vs33,   vs33 
    xxlxor      vs34,   vs34,   vs34
    xxlxor      vs35,   vs35,   vs35
 
.endif
.endm

.macro END4x4_NORMAL
  END4x4 0, AO, BO, 16,16 
.endm

.macro END4x4 First, AREG, BREG, OffsetA, OffsetB

.if \OffsetB != 0 
    addi        \BREG, \BREG, \OffsetB 
.endif
.if \OffsetA != 0 
    addi        \AREG, \AREG, \OffsetA 
.endif  

.if \First==1
    xvmulsp      vs32,   vs24,   vs0
    xvmulsp      vs33,   vs24,   vs1 
    xvmulsp      vs34,   vs24,   vs2
    xvmulsp      vs35,   vs24,   vs3  
.else
    xvmaddasp      vs32,   vs24,   vs0
    xvmaddasp      vs33,   vs24,   vs1 
    xvmaddasp      vs34,   vs24,   vs2
    xvmaddasp      vs35,   vs24,   vs3
 

.endif
.endm  

.macro KERNEL4x4_L1_L4_I  AREG,BREG,   OffsetA,OffsetB, Index,IsLast ,Complete

    lxv vs4,    DISP16(\Index, 0+\OffsetA)(\AREG)
    lxv vs26,   DISP16(\Index, 0+\OffsetB)(\BREG) 

    xxperm      vs6,   vs4,       permute_mask  
    xxpermdi    vs5,   vs4,   vs4,2      
    xxpermdi    vs7,   vs6,   vs6,2 
 
    xvmaddasp      vs32,   vs24,   vs0
    xvmaddasp      vs33,   vs24,   vs1 
    xvmaddasp      vs34,   vs24,   vs2
    xvmaddasp      vs35,   vs24,   vs3
 

    lxv vs0,    DISP16(\Index, 16+\OffsetA)(\AREG)
    lxv vs24,   DISP16(\Index, 16+\OffsetB)(\BREG)  

    xxperm      vs2,   vs0,       permute_mask  
    xxpermdi    vs1,   vs0,   vs0,2      
    xxpermdi    vs3,   vs2,   vs2,2   

    xvmaddasp      vs32,   vs26,   vs4
    xvmaddasp      vs33,   vs26,   vs5 
    xvmaddasp      vs34,   vs26,   vs6
    xvmaddasp      vs35,   vs26,   vs7
 
 

    lxv vs4,    DISP16(\Index, 32+\OffsetA)(\AREG)
    lxv vs26,   DISP16(\Index, 32+\OffsetB)(\BREG) 

    xxperm      vs6,   vs4,       permute_mask  
    xxpermdi    vs5,   vs4,   vs4,2      
    xxpermdi    vs7,   vs6,   vs6,2 
 
    xvmaddasp      vs32,   vs24,   vs0
    xvmaddasp      vs33,   vs24,   vs1 
    xvmaddasp      vs34,   vs24,   vs2
    xvmaddasp      vs35,   vs24,   vs3
 

.if \Complete==0 

    lxv vs0,    DISP16(\Index, 48+\OffsetA)(\AREG)
    lxv vs24,   DISP16(\Index, 48+\OffsetB)(\BREG) 

    xxperm      vs2,   vs0,       permute_mask  
    xxpermdi    vs1,   vs0,   vs0,2      
    xxpermdi    vs3,   vs2,   vs2,2   
.endif
    xvmaddasp      vs32,   vs26,   vs4
    xvmaddasp      vs33,   vs26,   vs5 
    xvmaddasp      vs34,   vs26,   vs6
    xvmaddasp      vs35,   vs26,   vs7
 

 
 
.if \IsLast==1  
.if \Complete==1
    addi        \AREG, \AREG, DISP16(\Index,16*3+\OffsetA)  
    addi        \BREG, \BREG,  DISP16(\Index,16*3+\OffsetB)

.else
    addi        \AREG, \AREG, DISP16(\Index,64)  
    addi        \BREG, \BREG,  DISP16(\Index,64)

.endif
.endif   
 
 
.endm

.macro KERNEL4x4 First
    LOAD4x4 0
    END4x4 \First, AO, BO, 16,16  
.endm

.macro KERNEL4x4_L1_L2_I  AREG,BREG,First,OffsetA,OffsetB, Index,IsLast ,Complete

    lxv vs4,    DISP8(\Index, 0+\OffsetA)(\AREG)
    lxv vs26,   DISP8(\Index, 0+\OffsetB)(\BREG) 

    xxperm      vs6,   vs4,       permute_mask  
    xxpermdi    vs5,   vs4,   vs4,2      
    xxpermdi    vs7,   vs6,   vs6,2 
.if \First==1
    xvmulsp      vs32,   vs24,   vs0
    xvmulsp      vs33,   vs24,   vs1 
    xvmulsp      vs34,   vs24,   vs2
    xvmulsp      vs35,   vs24,   vs3
 
.else 
    xvmaddasp      vs32,   vs24,   vs0
    xvmaddasp      vs33,   vs24,   vs1 
    xvmaddasp      vs34,   vs24,   vs2
    xvmaddasp      vs35,   vs24,   vs3
 
.endif

.if \Complete==0 

    lxv vs0,    DISP8(\Index, 16+\OffsetA)(\AREG)
    lxv vs24,   DISP8(\Index, 16+\OffsetB)(\BREG) 

    xxperm      vs2,   vs0,       permute_mask  
    xxpermdi    vs1,   vs0,   vs0,2      
    xxpermdi    vs3,   vs2,   vs2,2   
.endif

.if \First==1
    xvmulsp      vs32,   vs26,   vs4
    xvmulsp      vs33,   vs26,   vs5 
    xvmulsp      vs34,   vs26,   vs6
    xvmulsp      vs35,   vs26,   vs7 


.else
    xvmaddasp      vs32,   vs26,   vs4
    xvmaddasp      vs33,   vs26,   vs5 
    xvmaddasp      vs34,   vs26,   vs6
    xvmaddasp      vs35,   vs26,   vs7
 
.endif
 
 
.if \IsLast==1  
.if \Complete==1
    addi        \AREG, \AREG, DISP8(\Index,16+\OffsetA)  
    addi        \BREG, \BREG,  DISP8(\Index,16+\OffsetB)

.else
    addi        \AREG, \AREG, DISP8(\Index,32)  
    addi        \BREG, \BREG,  DISP8(\Index,32)

.endif
.endif   
     
  
.endm


.macro SAVE4x4
  slwi    T10, LDC ,   1 
  add     T1, CO, LDC 
#if !defined(TRMMKERNEL)  
  lxv        vs36, 0(CO)
  lxv        vs37, 0(T1)
#endif
  add     T2, CO, T10  
  add     T3, T1, T10 
#if !defined(TRMMKERNEL)   
  lxv        vs38, 0(T2)
  lxv        vs39, 0(T3)    
#endif   

  xxmrglw  vs0, vs35,vs32
  xxmrglw  vs1, vs34,vs33 
  xxmrglw  vs4, vs32,vs35
  xxmrglw  vs5, vs33,vs34 


  xxmrghw  vs2, vs35,vs32
  xxmrghw  vs3, vs34,vs33 
  xxmrghw  vs6, vs32,vs35
  xxmrghw  vs7, vs33,vs34  

  xxmrgld  vs24, vs1, vs0  
  xxmrghd  vs25,vs5,vs4 

  xxmrgld  vs26, vs2, vs3  
  xxmrghd  vs27,vs6,vs7

 #if defined(TRMMKERNEL)
  xvmulsp        vs36, vs24, alpha_r
  xvmulsp        vs37, vs25, alpha_r 
  xvmulsp        vs38, vs26, alpha_r
  xvmulsp        vs39, vs27, alpha_r 
#else
  xvmaddasp        vs36, vs24, alpha_r
  xvmaddasp        vs37, vs25, alpha_r 
  xvmaddasp        vs38, vs26, alpha_r
  xvmaddasp        vs39, vs27, alpha_r   
 #endif
  stxv        vs36, 0(CO)
  stxv        vs37, 0(T1) 
  stxv        vs38, 0(T2)
  stxv        vs39, 0(T3)   
 


  addi CO,CO,16
.endm


/**********************************************************************************************
* Macros for N=4 and M=2
**********************************************************************************************/

 
.macro KERNEL4x2_2   OffsetA,OffsetB, Index,IsLast
  KERNEL4x2_I_2 AO,BO, 0, \OffsetA,\OffsetB,\Index,\IsLast
.endm

 

.macro Zero4x2
    xxlxor      vs0,   vs0,   vs0 
    xxlxor      vs2,   vs2,   vs2 
       
.endm
 
.macro KERNEL4x2
  KERNEL4x2_1 AO,BO, 0, 0,0,0
.endm
.macro KERNEL4x2_1 AREG,BREG,First,OffsetA,OffsetB,Index


    lxsd v4,   DISP2(\Index, 0+\OffsetA)(\AREG)
    lxv vs26,   DISP8(\Index, 0+\OffsetB)(\BREG)      
    xxspltw   vs8,  vs36, 0 
    xxspltw   vs9,  vs36, 1  
 
.if \First==1
    xvmulsp      vs0,   vs26,   vs8 
    xvmulsp      vs2,   vs26,   vs9 
     
.else 
    xvmaddasp      vs0,   vs26,   vs8  
    xvmaddasp      vs2,   vs26,   vs9 
 
 .endif
   
    addi        \AREG, \AREG, DISP2(\Index,8)  
    addi        \BREG, \BREG, DISP4(\Index,16)
 
.endm

.macro KERNEL4x2_I_2  AREG,BREG,First,OffsetA,OffsetB, Index,IsLast  

    lxv vs4,    DISP4(\Index, 0+\OffsetA)(\AREG)
    lxv vs26,   DISP8(\Index, 0+\OffsetB)(\BREG) 
    lxv vs28,   DISP8(\Index,16+\OffsetB)(\BREG)       
    xxspltw   vs8,  vs4, 2  
    xxspltw   vs9,  vs4, 3 
    xxspltw   vs10, vs4, 0 
    xxspltw   vs11, vs4, 1
 
.if \First==1
    xvmulsp      vs0,   vs26,   vs8 
    xvmulsp      vs2,   vs26,   vs9  

    xvmulsp      vs0,   vs28,   vs10 
    xvmulsp      vs2,   vs28,   vs11     
.else 
    xvmaddasp      vs0,   vs26,   vs8 
    xvmaddasp      vs2,   vs26,   vs9 

    xvmaddasp      vs0,   vs28,   vs10 
    xvmaddasp      vs2,   vs28,   vs11   
 .endif

 
.if \IsLast==1   
    addi        \AREG, \AREG, DISP4(\Index,16)  
    addi        \BREG, \BREG, DISP8(\Index,32)
.endif 
  
.endm


.macro SAVE4x2
  slwi    T10, LDC ,   1 
  add     T1, CO, LDC  
  add     T2, CO, T10  
  add     T3, T1, T10     
  /*convert alpha_r for multiply*/
  xscvspdp  vs4,alpha_r
/* v0 corresponds to vs32, do not forget*/
#if !defined(TRMMKERNEL)
  lxssp  v0,0(CO) 
  lxssp  v1,4(CO) 

  lxssp  v2,0(T1)
  lxssp  v3,4(T1)

  lxssp  v4,0(T2)
  lxssp  v5,4(T2)

  lxssp  v6,0(T3)
  lxssp  v7,4(T3)

   
#endif
  xscvspdp  vs5, vs2
  xxspltw   vs6, vs2, 1 
  xxspltw   vs7, vs2, 2 
  xxspltw   vs8, vs2, 3  
  xscvspdp  vs6,vs6
  xscvspdp  vs7,vs7
  xscvspdp  vs8,vs8

  xscvspdp  vs24, vs0
  xxspltw   vs25, vs0, 1 
  xxspltw   vs26, vs0, 2 
  xxspltw   vs27, vs0, 3  
  xscvspdp  vs25,vs25
  xscvspdp  vs26,vs26
  xscvspdp  vs27,vs27
 

#if defined(TRMMKERNEL)
  xsmuldp  vs32,vs8, vs4 
  xsmuldp  vs33,vs27, vs4 

  xsmuldp  vs34,vs7, vs4 
  xsmuldp  vs35,vs26, vs4 

  xsmuldp  vs36,vs6, vs4 
  xsmuldp  vs37,vs25, vs4  

  xsmuldp  vs38,vs5, vs4 
  xsmuldp  vs39,vs24, vs4  

      
#else
  xsmaddadp  vs32,vs8, vs4 
  xsmaddadp  vs33,vs27, vs4 

  xsmaddadp  vs34,vs7, vs4 
  xsmaddadp  vs35,vs26, vs4 

  xsmaddadp  vs36,vs6, vs4 
  xsmaddadp  vs37,vs25, vs4  

  xsmaddadp  vs38,vs5, vs4 
  xsmaddadp  vs39,vs24, vs4  

    
#endif  

  stxssp  v0,0(CO) 
  stxssp  v1,4(CO) 

  stxssp  v2,0(T1)
  stxssp  v3,4(T1)

  stxssp  v4,0(T2)
  stxssp  v5,4(T2)

  stxssp  v6,0(T3)
  stxssp  v7,4(T3)

 
 

  addi CO,CO,8
.endm


/**********************************************************************************************
* Macros for N=4 and M=1
**********************************************************************************************/
.macro KERNEL4x1_4   OffsetA,OffsetB, Index,IsLast
  KERNEL4x1_I_4 AO,BO, 0, \OffsetA,\OffsetB,\Index,\IsLast
.endm

.macro Zero4x1
    xxlxor      vs0,   vs0,   vs0 
.endm

.macro KERNEL4x1
  KERNEL4x1_1 AO,BO, 0 
.endm

.macro KERNEL4x1_2
  KERNEL4x1_2_1 AO,BO, 0 
.endm

.macro KERNEL4x1_1 AREG,BREG,First 
    lxvwsx vs8,  0, \AREG
    lxv vs26,   0(\BREG)       
.if \First==1
    xvmulsp      vs0,   vs26,   vs8  
.else 
    xvmaddasp      vs0,   vs26,   vs8  
 .endif
    addi        \AREG, \AREG,  4  
    addi        \BREG, \BREG,  16
.endm

.macro KERNEL4x1_2_1 AREG,BREG,First 
    lxsd v4,    0(\AREG)
    lxv vs26,   0(\BREG)      
    lxv vs28,  16(\BREG) 
    xxspltw   vs8,  vs36, 1 
    xxspltw   vs9,  vs36, 0  
.if \First==1
    xvmulsp      vs0,   vs26,   vs8 
    xvmulsp      vs0,   vs28,   vs9     
.else 
    xvmaddasp      vs0,   vs26,   vs8  
    xvmaddasp      vs0,   vs28,   vs9  
 .endif
    addi        \AREG, \AREG,  8 
    addi        \BREG, \BREG,  32
.endm

.macro KERNEL4x1_I_4  AREG,BREG,First,OffsetA,OffsetB, Index,IsLast  
    lxv vs4,    DISP4(\Index, 0+\OffsetA)(\AREG)
    xxspltw   vs8,  vs4, 3 
    xxspltw   vs9,  vs4, 2 
    xxspltw   vs10, vs4, 1 
    xxspltw   vs11, vs4, 0
    lxv vs26,   DISP16(\Index, 0+\OffsetB)(\BREG) 
    lxv vs28,   DISP16(\Index,16+\OffsetB)(\BREG)  
    lxv vs30,   DISP16(\Index,32+\OffsetB)(\BREG) 
    lxv vs32,   DISP16(\Index,48+\OffsetB)(\BREG)          
.if \First==1
    xvmulsp      vs0,   vs26,   vs8  
    xvmulsp      vs0,   vs28,   vs9      
    xvmulsp      vs0,   vs30,   vs10  
    xvmulsp      vs0,   vs32,   vs11     
.else 
    xvmaddasp      vs0,   vs26,   vs8  
    xvmaddasp      vs0,   vs28,   vs9     
    xvmaddasp      vs0,   vs30,   vs10  
    xvmaddasp      vs0,   vs32,   vs11  
 .endif
.if \IsLast==1   
    addi        \AREG, \AREG, DISP4(\Index,16)  
    addi        \BREG, \BREG, DISP16(\Index,64)
.endif 
.endm

.macro SAVE4x1
  slwi    T10, LDC ,   1 
  add     T1, CO, LDC  
  add     T2, CO, T10  
  add     T3, T1, T10     
  /*convert alpha_r for multiply*/
  xscvspdp  vs4,alpha_r
/* v0 corresponds to vs32, do not forget*/
#if !defined(TRMMKERNEL)
  lxssp  v0,0(CO)  
  lxssp  v2,0(T1) 
  lxssp  v4,0(T2) 
  lxssp  v6,0(T3)  
#endif
  xscvspdp  vs24, vs0
  xxspltw   vs25, vs0, 1 
  xxspltw   vs26, vs0, 2 
  xxspltw   vs27, vs0, 3  
  xscvspdp  vs25,vs25
  xscvspdp  vs26,vs26
  xscvspdp  vs27,vs27

#if defined(TRMMKERNEL)
  xsmuldp  vs32,vs27, vs4 
  xsmuldp  vs34,vs26, vs4 
  xsmuldp  vs36,vs25, vs4 
  xsmuldp  vs38,vs24, vs4  
#else
  xsmaddadp  vs32,vs27, vs4 
  xsmaddadp  vs34,vs26, vs4 
  xsmaddadp  vs36,vs25, vs4 
  xsmaddadp  vs38,vs24, vs4   
#endif  
  stxssp  v0,0(CO)  
  stxssp  v2,0(T1) 
  stxssp  v4,0(T2) 
  stxssp  v6,0(T3)  
  addi CO,CO,4
.endm

/****************************N=2 section*****************/

.macro KERNEL2x16_2   OffsetA,OffsetB, Index,IsLast
  KERNEL2x16_I_2 AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast
.endm

 
.macro Zero2x16
    xxlxor      vs0,   vs0,   vs0
    xxlxor      vs1,   vs1,   vs1 
    xxlxor      vs2,   vs2,   vs2
    xxlxor      vs3,   vs3,   vs3
    xxlxor      vs4,   vs4,   vs4
    xxlxor      vs5,   vs5,   vs5 
    xxlxor      vs6,   vs6,   vs6
    xxlxor      vs7,   vs7,   vs7      
.endm
 
.macro KERNEL2x16
  KERNEL2x16_1 AO,BO, 0, 0,0,0
.endm
.macro KERNEL2x16_4 OffsetA,OffsetB, Index,IsLast  
  KERNEL2x16_I_4 AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast
.endm
 
.macro KERNEL2x16_1 AREG,BREG,First,OffsetA,OffsetB,Index


    lxsd v4,   DISP2(\Index, 0+\OffsetB)(\BREG)
    xxspltw   vs8,  vs36, 1 
    xxspltw   vs9,  vs36, 0 
    lxv vs26,   DISP16(\Index, 0+\OffsetA)(\AREG)
    lxv vs27,   DISP16(\Index,16+\OffsetA)(\AREG)   
    lxv vs28,   DISP16(\Index, 32+\OffsetA)(\AREG)
    lxv vs29,   DISP16(\Index,48+\OffsetA)(\AREG)        
 
 
.if \First==1
    xvmulsp      vs0,   vs26,   vs8
    xvmulsp      vs1,   vs27,   vs8 
    xvmulsp      vs2,   vs28,   vs8
    xvmulsp      vs3,   vs29,   vs8 

    xvmulsp      vs4,   vs26,   vs9
    xvmulsp      vs5,   vs27,   vs9 
    xvmulsp      vs6,   vs28,   vs9
    xvmulsp      vs7,   vs29,   vs9     
     
.else 
    xvmaddasp      vs0,   vs26,   vs8
    xvmaddasp      vs1,   vs27,   vs8 
    xvmaddasp      vs2,   vs28,   vs8
    xvmaddasp      vs3,   vs29,   vs8 

    xvmaddasp      vs4,   vs26,   vs9
    xvmaddasp      vs5,   vs27,   vs9 
    xvmaddasp      vs6,   vs28,   vs9
    xvmaddasp      vs7,   vs29,   vs9
 
 .endif
   
    addi        \BREG, \BREG, DISP2(\Index,8)
    addi        \AREG, \AREG, DISP16(\Index,64)  
 
.endm




.macro KERNEL2x16_I_4  AREG,BREG, OffsetA,OffsetB, Index,IsLast  

    lxv vs38,    DISP8(\Index, 0+\OffsetB)(\BREG)
    lxv vs39,    DISP8(\Index, 16+\OffsetB)(\BREG) 

    lxv vs26,   DISP64(\Index, 0+\OffsetA)(\AREG)
    lxv vs27,   DISP64(\Index,16+\OffsetA)(\AREG)
    lxv vs28,   DISP64(\Index,32+\OffsetA)(\AREG)
    lxv vs29,   DISP64(\Index,48+\OffsetA)(\AREG)  

    lxv vs16,   DISP64(\Index,64+ 0+\OffsetA)(\AREG)
    lxv vs17,   DISP64(\Index,64+ 16+\OffsetA)(\AREG)
    lxv vs18,   DISP64(\Index,64+ 32+\OffsetA)(\AREG)
    lxv vs19,   DISP64(\Index,64+ 48+\OffsetA)(\AREG) 

    lxv vs30,   DISP64(\Index,128+ 0+\OffsetA)(\AREG)
    lxv vs31,   DISP64(\Index,128+ 16+\OffsetA)(\AREG)
    lxv vs32,   DISP64(\Index,128+ 32+\OffsetA)(\AREG)
    lxv vs33,   DISP64(\Index,128+ 48+\OffsetA)(\AREG)  
        
    lxv vs34,   DISP64(\Index,128+ 64+ 0+\OffsetA)(\AREG)
    lxv vs35,   DISP64(\Index,128+ 64+ 16+\OffsetA)(\AREG)
    lxv vs36,   DISP64(\Index,128+ 64+ 32+\OffsetA)(\AREG)
    lxv vs37,   DISP64(\Index,128+ 64+ 48+\OffsetA)(\AREG) 

    xxspltw   vs8,  vs38, 3  
    xxspltw   vs9,  vs38, 2 
    xxspltw   vs10, vs38, 1 
    xxspltw   vs11, vs38, 0  

    xxspltw   vs12,  vs39, 3  
    xxspltw   vs13,  vs39, 2 
    xxspltw   vs14, vs39, 1 
    xxspltw   vs15, vs39, 0  

 
    xvmaddasp      vs0,   vs26,   vs8
    xvmaddasp      vs1,   vs27,   vs8 
    xvmaddasp      vs2,   vs28,   vs8
    xvmaddasp      vs3,   vs29,   vs8 

    xvmaddasp      vs4,   vs26,   vs9
    xvmaddasp      vs5,   vs27,   vs9 
    xvmaddasp      vs6,   vs28,   vs9
    xvmaddasp      vs7,   vs29,   vs9 

    xvmaddasp      vs0,   vs16,   vs10
    xvmaddasp      vs1,   vs17,   vs10 
    xvmaddasp      vs2,   vs18,   vs10
    xvmaddasp      vs3,   vs19,   vs10 

    xvmaddasp      vs4,   vs16,   vs11
    xvmaddasp      vs5,   vs17,   vs11 
    xvmaddasp      vs6,   vs18,   vs11
    xvmaddasp      vs7,   vs19,   vs11  

    xvmaddasp      vs0,   vs30,   vs12
    xvmaddasp      vs1,   vs31,   vs12 
    xvmaddasp      vs2,   vs32,   vs12
    xvmaddasp      vs3,   vs33,   vs12 

    xvmaddasp      vs4,   vs30,   vs13
    xvmaddasp      vs5,   vs31,   vs13 
    xvmaddasp      vs6,   vs32,   vs13
    xvmaddasp      vs7,   vs33,   vs13 

    xvmaddasp      vs0,   vs34,   vs14
    xvmaddasp      vs1,   vs35,   vs14 
    xvmaddasp      vs2,   vs36,   vs14
    xvmaddasp      vs3,   vs37,   vs14 

    xvmaddasp      vs4,   vs34,   vs15
    xvmaddasp      vs5,   vs35,   vs15 
    xvmaddasp      vs6,   vs36,   vs15
    xvmaddasp      vs7,   vs37,   vs15    
 
 
.if \IsLast==1   
    addi        \BREG, \BREG, DISP8(\Index,32)  
    addi        \AREG, \AREG, DISP64(\Index,256)
.endif 
  
.endm

.macro KERNEL2x16_I_2  AREG,BREG, OffsetA,OffsetB, Index,IsLast  

    lxv vs36,    DISP4(\Index, 0+\OffsetB)(\BREG)
    xxspltw   vs8,  vs36, 3  
    xxspltw   vs9,  vs36, 2 
    xxspltw   vs10, vs36, 1 
    xxspltw   vs11, vs36, 0    
    lxv vs26,   DISP32(\Index, 0+\OffsetA)(\AREG)
    lxv vs27,   DISP32(\Index,16+\OffsetA)(\AREG)
    lxv vs28,   DISP32(\Index,32+\OffsetA)(\AREG)
    lxv vs29,   DISP32(\Index,48+\OffsetA)(\AREG)      
    lxv vs16,   DISP32(\Index,64+ 0+\OffsetA)(\AREG)
    lxv vs17,   DISP32(\Index,64+ 16+\OffsetA)(\AREG)
    lxv vs18,   DISP32(\Index,64+ 32+\OffsetA)(\AREG)
    lxv vs19,   DISP32(\Index,64+ 48+\OffsetA)(\AREG) 
 
 
    xvmaddasp      vs0,   vs26,   vs8
    xvmaddasp      vs1,   vs27,   vs8 
    xvmaddasp      vs2,   vs28,   vs8
    xvmaddasp      vs3,   vs29,   vs8 

    xvmaddasp      vs4,   vs26,   vs9
    xvmaddasp      vs5,   vs27,   vs9 
    xvmaddasp      vs6,   vs28,   vs9
    xvmaddasp      vs7,   vs29,   vs9 

    xvmaddasp      vs0,   vs16,   vs10
    xvmaddasp      vs1,   vs17,   vs10 
    xvmaddasp      vs2,   vs18,   vs10
    xvmaddasp      vs3,   vs19,   vs10 

    xvmaddasp      vs4,   vs16,   vs11
    xvmaddasp      vs5,   vs17,   vs11 
    xvmaddasp      vs6,   vs18,   vs11
    xvmaddasp      vs7,   vs19,   vs11   
 
.if \IsLast==1   
    addi        \BREG, \BREG, DISP4(\Index,16)  
    addi        \AREG, \AREG, DISP32(\Index,128)
.endif 
  
.endm


.macro SAVE2x16

#ifndef TRMMKERNEL    
    lxv        vs16, 0(CO)
    lxv        vs17, 16(CO) 
    lxv        vs18, 32(CO)  
    lxv        vs19, 48(CO)      
#endif
  add     T1, CO, LDC 
#ifndef TRMMKERNEL    
    lxv        vs26, 0(T1)
    lxv        vs27, 16(T1) 
    lxv        vs28, 32(T1)  
    lxv        vs29, 48(T1)      
#endif

#if defined(TRMMKERNEL)
  xvmulsp        vs16, vs0, alpha_r
  xvmulsp        vs17, vs1, alpha_r 
  xvmulsp        vs18, vs2, alpha_r
  xvmulsp        vs19, vs3, alpha_r   
  xvmulsp        vs26, vs4, alpha_r
  xvmulsp        vs27, vs5, alpha_r 
  xvmulsp        vs28, vs6, alpha_r
  xvmulsp        vs29, vs7, alpha_r
#else
  xvmaddasp        vs16, vs0, alpha_r
  xvmaddasp        vs17, vs1, alpha_r 
  xvmaddasp        vs18, vs2, alpha_r
  xvmaddasp        vs19, vs3, alpha_r   
  xvmaddasp        vs26, vs4, alpha_r
  xvmaddasp        vs27, vs5, alpha_r 
  xvmaddasp        vs28, vs6, alpha_r
  xvmaddasp        vs29, vs7, alpha_r
#endif
    stxv        vs16, 0(CO)
    stxv        vs17, 16(CO) 
    stxv        vs18, 32(CO)  
    stxv        vs19, 48(CO)      
    
    stxv        vs26, 0(T1)
    stxv        vs27, 16(T1) 
    stxv        vs28, 32(T1)  
    stxv        vs29, 48(T1) 
 
  addi CO,CO,64

.endm

/*       M=8 N=2 */

.macro KERNEL2x8_2   OffsetA,OffsetB, Index,IsLast
  KERNEL2x8_I_2 AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast
.endm

 
.macro Zero2x8
    xxlxor      vs0,   vs0,   vs0
    xxlxor      vs1,   vs1,   vs1 
 
    xxlxor      vs4,   vs4,   vs4
    xxlxor      vs5,   vs5,   vs5 
     
.endm
 
.macro KERNEL2x8
  KERNEL2x8_1 AO,BO, 0, 0,0,0
.endm
.macro KERNEL2x8_4 OffsetA,OffsetB, Index,IsLast  
  KERNEL2x8_I_4 AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast
.endm
 
.macro KERNEL2x8_1 AREG,BREG,First,OffsetA,OffsetB,Index


    lxsd v4,   DISP2(\Index, 0+\OffsetB)(\BREG)
    xxspltw   vs8,  vs36, 1 
    xxspltw   vs9,  vs36, 0 
    lxv vs26,   DISP8(\Index, 0+\OffsetA)(\AREG)
    lxv vs27,   DISP8(\Index,16+\OffsetA)(\AREG)          
 
 
.if \First==1
    xvmulsp      vs0,   vs26,   vs8
    xvmulsp      vs1,   vs27,   vs8  

    xvmulsp      vs4,   vs26,   vs9
    xvmulsp      vs5,   vs27,   vs9      
     
.else 
    xvmaddasp      vs0,   vs26,   vs8
    xvmaddasp      vs1,   vs27,   vs8   

    xvmaddasp      vs4,   vs26,   vs9
    xvmaddasp      vs5,   vs27,   vs9  
 
 .endif
   
    addi        \BREG, \BREG, DISP2(\Index,8)
    addi        \AREG, \AREG, DISP8(\Index,32)  
 
.endm




.macro KERNEL2x8_I_4  AREG,BREG, OffsetA,OffsetB, Index,IsLast  

    lxv vs38,    DISP8(\Index, 0+\OffsetB)(\BREG)
    lxv vs39,    DISP8(\Index, 16+\OffsetB)(\BREG) 

    lxv vs26,   DISP32(\Index, 0+\OffsetA)(\AREG)
    lxv vs27,   DISP32(\Index,16+\OffsetA)(\AREG) 

    lxv vs16,   DISP32(\Index,32+ 0+\OffsetA)(\AREG)
    lxv vs17,   DISP32(\Index,32+ 16+\OffsetA)(\AREG) 

    lxv vs30,   DISP32(\Index,64+ 0+\OffsetA)(\AREG)
    lxv vs31,   DISP32(\Index,64+ 16+\OffsetA)(\AREG)  
        
    lxv vs34,   DISP32(\Index, 96+ 0+\OffsetA)(\AREG)
    lxv vs35,   DISP32(\Index, 96+ 16+\OffsetA)(\AREG) 

    xxspltw   vs8,  vs38, 3  
    xxspltw   vs9,  vs38, 2 
    xxspltw   vs10, vs38, 1 
    xxspltw   vs11, vs38, 0  

    xxspltw   vs12,  vs39, 3  
    xxspltw   vs13,  vs39, 2 
    xxspltw   vs14, vs39, 1 
    xxspltw   vs15, vs39, 0  

 
 
    xvmaddasp      vs0,   vs26,   vs8
    xvmaddasp      vs1,   vs27,   vs8 
    xvmaddasp      vs4,   vs26,   vs9
    xvmaddasp      vs5,   vs27,   vs9 
 

    xvmaddasp      vs0,   vs16,   vs10
    xvmaddasp      vs1,   vs17,   vs10 
    xvmaddasp      vs4,   vs16,   vs11
    xvmaddasp      vs5,   vs17,   vs11 
 

    xvmaddasp      vs0,   vs30,   vs12
    xvmaddasp      vs1,   vs31,   vs12 
    xvmaddasp      vs4,   vs30,   vs13
    xvmaddasp      vs5,   vs31,   vs13 

    xvmaddasp      vs0,   vs34,   vs14
    xvmaddasp      vs1,   vs35,   vs14 
    xvmaddasp      vs4,   vs34,   vs15
    xvmaddasp      vs5,   vs35,   vs15 
   
 
 
.if \IsLast==1   
    addi        \BREG, \BREG, DISP8(\Index,32)  
    addi        \AREG, \AREG, DISP32(\Index,128)
.endif 
  
.endm

.macro KERNEL2x8_I_2  AREG,BREG, OffsetA,OffsetB, Index,IsLast  

    lxv vs36,    DISP4(\Index, 0+\OffsetB)(\BREG)
    xxspltw   vs8,  vs36, 3  
    xxspltw   vs9,  vs36, 2 
    xxspltw   vs10, vs36, 1 
    xxspltw   vs11, vs36, 0    
    lxv vs26,   DISP16(\Index, 0+\OffsetA)(\AREG)
    lxv vs27,   DISP16(\Index,16+\OffsetA)(\AREG)
    lxv vs16,   DISP16(\Index,32+\OffsetA)(\AREG)
    lxv vs17,   DISP16(\Index,48+\OffsetA)(\AREG)      
 
    xvmaddasp      vs0,   vs26,   vs8
    xvmaddasp      vs1,   vs27,   vs8  

    xvmaddasp      vs4,   vs26,   vs9
    xvmaddasp      vs5,   vs27,   vs9  

    xvmaddasp      vs0,   vs16,   vs10
    xvmaddasp      vs1,   vs17,   vs10   

    xvmaddasp      vs4,   vs16,   vs11
    xvmaddasp      vs5,   vs17,   vs11     
 
.if \IsLast==1   
    addi        \BREG, \BREG, DISP4(\Index,16)  
    addi        \AREG, \AREG, DISP16(\Index,64)
.endif 
  
.endm


.macro SAVE2x8

#ifndef TRMMKERNEL    
    lxv        vs16, 0(CO)
    lxv        vs17, 16(CO)     
#endif
  add     T1, CO, LDC 
#ifndef TRMMKERNEL    
    lxv        vs26, 0(T1)
    lxv        vs27, 16(T1) 
    
#endif

#if defined(TRMMKERNEL)
  xvmulsp        vs16, vs0, alpha_r
  xvmulsp        vs17, vs1, alpha_r  
  xvmulsp        vs26, vs4, alpha_r
  xvmulsp        vs27, vs5, alpha_r 
#else
  xvmaddasp        vs16, vs0, alpha_r
  xvmaddasp        vs17, vs1, alpha_r  
  xvmaddasp        vs26, vs4, alpha_r
  xvmaddasp        vs27, vs5, alpha_r 
#endif

    stxv        vs16, 0(CO)
    stxv        vs17, 16(CO) 
     
    
    stxv        vs26, 0(T1)
    stxv        vs27, 16(T1) 

  addi CO,CO,32

.endm


/*M=4*/


.macro KERNEL2x4_2   OffsetA,OffsetB, Index,IsLast
  KERNEL2x4_I_2 AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast
.endm

 /* we will aggregate on save vs0 +vs4 vs11+vs5 */
.macro Zero2x4
    xxlxor      vs0,   vs0,   vs0
    xxlxor      vs1,   vs1,   vs1 
 
    xxlxor      vs4,   vs4,   vs4
    xxlxor      vs5,   vs5,   vs5 
    
.endm
 
.macro KERNEL2x4
  KERNEL2x4_1 AO,BO, 0, 0,0,0
.endm
.macro KERNEL2x4_4 OffsetA,OffsetB, Index,IsLast  
  KERNEL2x4_I_4 AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast
.endm
 
.macro KERNEL2x4_1 AREG,BREG,First,OffsetA,OffsetB,Index


    lxsd v4,   DISP2(\Index, 0+\OffsetB)(\BREG)
    xxspltw   vs8,  vs36, 1 
    xxspltw   vs9,  vs36, 0 
    lxv vs26,   DISP4(\Index, 0+\OffsetA)(\AREG)        
 
 
.if \First==1
    xvmulsp      vs0,   vs26,   vs8 
    xvmulsp      vs1,   vs26,   vs9     
     
.else 
    xvmaddasp      vs0,   vs26,   vs8 
    xvmaddasp      vs1,   vs26,   vs9 
 .endif
   
    addi        \BREG, \BREG, DISP2(\Index,8)
    addi        \AREG, \AREG, DISP4(\Index,16)  
 
.endm




.macro KERNEL2x4_I_4  AREG,BREG, OffsetA,OffsetB, Index,IsLast  

    lxv vs38,    DISP8(\Index, 0+\OffsetB)(\BREG)
    lxv vs39,    DISP8(\Index, 16+\OffsetB)(\BREG) 

    lxv vs26,   DISP16(\Index, 0+\OffsetA)(\AREG)
    lxv vs16,   DISP16(\Index,16+\OffsetA)(\AREG) 

    lxv vs30,   DISP16(\Index,32+ 0+\OffsetA)(\AREG)
    lxv vs34,   DISP16(\Index,32+ 16+\OffsetA)(\AREG) 
 

    xxspltw   vs8,  vs38, 3  
    xxspltw   vs9,  vs38, 2 
    xxspltw   vs10, vs38, 1 
    xxspltw   vs11, vs38, 0  

    xxspltw   vs12,  vs39, 3  
    xxspltw   vs13,  vs39, 2 
    xxspltw   vs14, vs39, 1 
    xxspltw   vs15, vs39, 0  

 
    xvmaddasp      vs0,   vs26,   vs8
    xvmaddasp      vs1,   vs26,   vs9 
    xvmaddasp      vs4,   vs16,   vs10
    xvmaddasp      vs5,   vs16,   vs11 
 

    xvmaddasp      vs0,   vs30,   vs12
    xvmaddasp      vs1,   vs30,   vs13 
    xvmaddasp      vs4,   vs34,   vs14
    xvmaddasp      vs5,   vs34,   vs15 
 
   
 
 
.if \IsLast==1   
    addi        \BREG, \BREG, DISP8(\Index,32)  
    addi        \AREG, \AREG, DISP16(\Index,64)
.endif 
  
.endm

.macro KERNEL2x4_I_2  AREG,BREG, OffsetA,OffsetB, Index,IsLast  

    lxv vs36,    DISP4(\Index, 0+\OffsetB)(\BREG)
    xxspltw   vs8,  vs36, 3  
    xxspltw   vs9,  vs36, 2 
    xxspltw   vs10, vs36, 1 
    xxspltw   vs11, vs36, 0    
    lxv vs26,   DISP8(\Index, 0+\OffsetA)(\AREG)
    lxv vs16,   DISP8(\Index, 16+\OffsetA)(\AREG)      
 
    xvmaddasp      vs0,   vs26,   vs8
    xvmaddasp      vs1,   vs26,   vs9 
    xvmaddasp      vs4,   vs16,   vs10
    xvmaddasp      vs5,   vs16,   vs11     
 
.if \IsLast==1   
    addi        \BREG, \BREG, DISP4(\Index,16)  
    addi        \AREG, \AREG, DISP8(\Index,32)
.endif 
  
.endm


.macro SAVE2x4

#ifndef TRMMKERNEL    
    lxv        vs16, 0(CO)     
#endif
  add     T1, CO, LDC 
#ifndef TRMMKERNEL    
    lxv        vs26, 0(T1) 
    
#endif
    /*aggregate vectors*/
  xvaddsp         vs0,vs0,vs4
  xvaddsp         vs1,vs1,vs5 
#if defined(TRMMKERNEL)
  xvmulsp        vs16, vs0, alpha_r 
  xvmulsp        vs26, vs1, alpha_r 
#else
  xvmaddasp        vs16, vs0, alpha_r 
  xvmaddasp        vs26, vs1, alpha_r 
#endif

  stxv        vs16, 0(CO) 
  stxv        vs26, 0(T1)  

  addi CO,CO,16

.endm


/* M=2 N=2 we will have inner pemrute action before permute was revrsing 3,2,1,0 not iw 2ill inner reverse 1,0,3,2  */
.macro SWITCH_PERMUTE_INNER
    xxpermdi	permute_mask,	permute_mask,	permute_mask,2
.endm

.macro Zero2x2
    xxlxor      vs0,   vs0,   vs0
    xxlxor      vs1,   vs1,   vs1 
    SWITCH_PERMUTE_INNER
.endm
 
.macro KERNEL2x2
  KERNEL2x2_1 AO,BO, 0, 0,0,0
.endm
.macro KERNEL2x2_4 OffsetA,OffsetB, Index,IsLast  
  KERNEL2x2_I_4 AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast
.endm

.macro KERNEL2x2_2 OffsetA,OffsetB, Index,IsLast  
  KERNEL2x2_I_2 AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast
.endm
 
.macro KERNEL2x2_1 AREG,BREG,First,OffsetA,OffsetB,Index


    lxsd v4,   DISP2(\Index, 0+\OffsetB)(\BREG)
    xxperm   vs9,  vs36, permute_mask 
    lxsd v5,   DISP2(\Index, 0+\OffsetA)(\AREG)        
 
 
.if \First==1
    xvmulsp      vs0,   vs37,   vs36 
    xvmulsp      vs1,   vs37,   vs9     
     
.else 
    xvmaddasp      vs0,   vs37,   vs36 
    xvmaddasp      vs1,   vs37,   vs9 
 .endif
   
    addi        \BREG, \BREG, DISP2(\Index,8)
    addi        \AREG, \AREG, DISP2(\Index,8)  
 
.endm




.macro KERNEL2x2_I_4  AREG,BREG, OffsetA,OffsetB, Index,IsLast  

    lxv vs8,    DISP8(\Index, 0+\OffsetB)(\BREG)
    lxv vs10,    DISP8(\Index, 16+\OffsetB)(\BREG) 

    lxv vs26,   DISP8(\Index, 0+\OffsetA)(\AREG)
    lxv vs16,   DISP8(\Index,16+\OffsetA)(\AREG) 

 
    xxperm   vs9,  vs8, permute_mask   
    xxperm   vs11, vs10, permute_mask  


 
    xvmaddasp      vs0,   vs26,   vs8
    xvmaddasp      vs1,   vs26,   vs9 
    xvmaddasp      vs0,   vs16,   vs10
    xvmaddasp      vs1,   vs16,   vs11 
 
 
 
.if \IsLast==1   
    addi        \BREG, \BREG, DISP8(\Index,32)  
    addi        \AREG, \AREG, DISP8(\Index,32)
.endif 
  
.endm

.macro KERNEL2x2_I_2  AREG,BREG, OffsetA,OffsetB, Index,IsLast  

    lxv vs8,    DISP4(\Index, 0+\OffsetB)(\BREG)  
    lxv vs26,   DISP4(\Index, 0+\OffsetA)(\AREG) 

 
    xxperm   vs9,  vs8, permute_mask    

 
    xvmaddasp      vs0,   vs26,   vs8
    xvmaddasp      vs1,   vs26,   vs9  
 
.if \IsLast==1   
    addi        \BREG, \BREG, DISP4(\Index,16)  
    addi        \AREG, \AREG, DISP4(\Index,16)
.endif 
.endm


.macro SAVE2x2

#ifndef TRMMKERNEL    
    lxsd v4   , 0(CO)     
#endif
  add     T1, CO, LDC 
#ifndef TRMMKERNEL    
    lxsd v5   , 0(T1) 
    
#endif
    /*aggregate vectors*/
  xxpermdi         vs4,vs0,vs0,2
  xxpermdi         vs5,vs1,vs1,2  
  xvaddsp          vs0,vs0,vs4
  xvaddsp         vs1,vs1,vs5 
  /*   */
  /* lets correct the order to 00 10 and 10 ,11 from {00,11} {01,10}  */
  xxperm    vs1,vs1, permute_mask


  xxmrghw   vs2 ,vs1,vs0
  xxpermdi         vs2,vs2,vs2,2  
  xxmrghw   vs3 ,vs0,vs1  
#if defined(TRMMKERNEL)
  xvmulsp        vs36, vs2, alpha_r 
  xvmulsp        vs37, vs3, alpha_r 
#else
  xvmaddasp        vs36, vs2, alpha_r 
  xvmaddasp        vs37, vs3, alpha_r 
#endif
  /**** store last two words*/


  stxsd       v4, 0(CO) 
  stxsd        v5, 0(T1)  

  addi CO,CO,8

.endm

/*--------------------------- M=1 N=2 */
.macro Zero2x1
    xxlxor      vs0,   vs0,   vs0
    xxlxor      vs1,   vs1,   vs1 
    xxlxor    vs2,vs2,vs2 
    xxlxor    vs3,vs3,vs3     
.endm
 
.macro KERNEL2x1
  KERNEL2x1_1 AO,BO, 0, 0,0,0
.endm
.macro KERNEL2x1_4 OffsetA,OffsetB, Index,IsLast  
  KERNEL2x1_I_4 AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast
.endm

.macro KERNEL2x1_2 OffsetA,OffsetB, Index,IsLast  
  KERNEL2x1_I_2 AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast
.endm
 /*
   we will calculate 1 alone then will add it to batched ones
 */
.macro KERNEL2x1_1 AREG,BREG,First,OffsetA,OffsetB,Index


    lxssp v3,   DISP2(\Index, 0+\OffsetB)(\BREG)
    lxssp v4,   DISP2(\Index, 4+\OffsetB)(\BREG) 
    lxssp v5,   DISP1(\Index, 0+\OffsetA)(\AREG)        
 
 
.if \First==1
    xvmulsp      vs2,   vs37,   vs35 
    xvmulsp      vs3,   vs37,   vs36     
     
.else 
    xsmaddadp     vs2,   vs37,   vs35
    xsmaddadp      vs3,   vs37,   vs36
 .endif
   
    addi        \BREG, \BREG, DISP2(\Index,8)
    addi        \AREG, \AREG, DISP1(\Index,4)  
 
.endm




.macro KERNEL2x1_I_4  AREG,BREG, OffsetA,OffsetB, Index,IsLast  

    lxv vs8,    DISP8(\Index, 0+\OffsetB)(\BREG)
    lxv vs10,   DISP8(\Index, 16+\OffsetB)(\BREG) 

    lxv vs26,   DISP4(\Index, 0+\OffsetA)(\AREG)
   
    xxmrglw   vs5, vs26,vs26
    xxmrghw   vs6, vs26,vs26 
 
    xvmaddasp      vs0,   vs8,   vs5
    xvmaddasp      vs1,   vs10,   vs6 
 
 
.if \IsLast==1   
    addi        \BREG, \BREG, DISP8(\Index,32)  
    addi        \AREG, \AREG, DISP4(\Index,16)
.endif 
  
.endm

.macro KERNEL2x1_I_2  AREG,BREG, OffsetA,OffsetB, Index,IsLast  

    lxssp v3,   DISP4(\Index, 0+\OffsetB)(\BREG)
    lxssp v4,   DISP4(\Index, 4+\OffsetB)(\BREG) 
    lxssp v7,   DISP4(\Index, 8+\OffsetB)(\BREG)
    lxssp v8,   DISP4(\Index, 12+\OffsetB)(\BREG)    
    lxssp v5,   DISP2(\Index, 0+\OffsetA)(\AREG)        
    lxssp v6,   DISP2(\Index, 4+\OffsetA)(\AREG)  
 
 
    xsmaddadp      vs2,   vs37,   vs35
    xsmaddadp      vs3,   vs37,   vs36

    xsmaddadp      vs2,   vs38,   vs39 
    xsmaddadp      vs3,   vs38,   vs40      
 
   
    addi        \BREG, \BREG, DISP4(\Index,16)
    addi        \AREG, \AREG, DISP2(\Index,8) 
.endm


.macro SAVE2x1

#ifndef TRMMKERNEL    
    lxssp v4   , 0(CO)     
#endif
  add     T1, CO, LDC 
#ifndef TRMMKERNEL    
    lxssp v5   , 0(T1) 
    
#endif

  /*convert alpha_r for multiply*/
  xscvspdp  vs16,alpha_r

 /*aggregate vectors 2x2_4   */ 
      xxpermdi         vs4,vs0,vs0,2
      xxpermdi         vs5,vs1,vs1,2  
      xvaddsp          vs0,vs0,vs4
      xvaddsp         vs1,vs1,vs5 
      xvaddsp         vs0,vs0,vs1 
/*aggregate vectors 2x1_2 and 2x1_1 into 2x2_4*/
  xscvspdp  vs5, vs0
  xxspltw   vs6, vs0, 1  
  xscvspdp  vs6,vs6 
  xsadddp  vs2,vs2,vs6
  xsadddp  vs3,vs3,vs5  

  /**** store last two words*/
#if defined(TRMMKERNEL) 
  xsmuldp  vs36,vs2, vs16 
  xsmuldp  vs37,vs3, vs16  
 
#else
  xsmaddadp  vs36,vs2, vs16 
  xsmaddadp  vs37,vs3, vs16 
#endif  

  stxssp       v4, 0(CO) 
  stxssp        v5, 0(T1)  

  addi CO,CO,4

.endm



/****************************N=1 section*****************/

.macro KERNEL1x16_2   OffsetA,OffsetB, Index,IsLast
  KERNEL1x16_I_2 AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast
.endm

 
.macro Zero1x16
    xxlxor      vs0,   vs0,   vs0
    xxlxor      vs1,   vs1,   vs1 
    xxlxor      vs2,   vs2,   vs2
    xxlxor      vs3,   vs3,   vs3       
.endm
 
.macro KERNEL1x16
  KERNEL1x16_1 AO,BO, 0, 0,0,0
.endm
.macro KERNEL1x16_4 OffsetA,OffsetB, Index,IsLast  
  KERNEL1x16_I_4 AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast
.endm
 
.macro KERNEL1x16_1 AREG,BREG,First,OffsetA,OffsetB,Index


    lxssp v4,   DISP1(\Index, 0+\OffsetB)(\BREG)
    xscvdpspn   vs36,vs36
    xxspltw     vs8,  vs36, 0
    lxv vs26,   DISP16(\Index, 0+\OffsetA)(\AREG)
    lxv vs27,   DISP16(\Index,16+\OffsetA)(\AREG)   
    lxv vs28,   DISP16(\Index, 32+\OffsetA)(\AREG)
    lxv vs29,   DISP16(\Index,48+\OffsetA)(\AREG)        
 
 
.if \First==1
    xvmulsp      vs0,   vs26,   vs8
    xvmulsp      vs1,   vs27,   vs8 
    xvmulsp      vs2,   vs28,   vs8
    xvmulsp      vs3,   vs29,   vs8 
  
     
.else 
    xvmaddasp      vs0,   vs26,   vs8
    xvmaddasp      vs1,   vs27,   vs8 
    xvmaddasp      vs2,   vs28,   vs8
    xvmaddasp      vs3,   vs29,   vs8 
 
 .endif
   
    addi        \BREG, \BREG, DISP1(\Index,4)
    addi        \AREG, \AREG, DISP16(\Index,64)  
 
.endm




.macro KERNEL1x16_I_4  AREG,BREG, OffsetA,OffsetB, Index,IsLast  

    lxv vs38,    DISP4(\Index, 0+\OffsetB)(\BREG) 

    lxv vs26,   DISP64(\Index, 0+\OffsetA)(\AREG)
    lxv vs27,   DISP64(\Index,16+\OffsetA)(\AREG)
    lxv vs28,   DISP64(\Index,32+\OffsetA)(\AREG)
    lxv vs29,   DISP64(\Index,48+\OffsetA)(\AREG)  

    lxv vs16,   DISP64(\Index,64+ 0+\OffsetA)(\AREG)
    lxv vs17,   DISP64(\Index,64+ 16+\OffsetA)(\AREG)
    lxv vs18,   DISP64(\Index,64+ 32+\OffsetA)(\AREG)
    lxv vs19,   DISP64(\Index,64+ 48+\OffsetA)(\AREG) 

    xxspltw   vs8,  vs38, 3  
    xxspltw   vs9,  vs38, 2 

    lxv vs30,   DISP64(\Index,128+ 0+\OffsetA)(\AREG)
    lxv vs31,   DISP64(\Index,128+ 16+\OffsetA)(\AREG)
    lxv vs32,   DISP64(\Index,128+ 32+\OffsetA)(\AREG)
    lxv vs33,   DISP64(\Index,128+ 48+\OffsetA)(\AREG)  
        
    lxv vs34,   DISP64(\Index,128+ 64+ 0+\OffsetA)(\AREG)
    lxv vs35,   DISP64(\Index,128+ 64+ 16+\OffsetA)(\AREG)
    lxv vs36,   DISP64(\Index,128+ 64+ 32+\OffsetA)(\AREG)
    lxv vs37,   DISP64(\Index,128+ 64+ 48+\OffsetA)(\AREG) 

    xxspltw   vs10, vs38, 1 
    xxspltw   vs11, vs38, 0    

 
    xvmaddasp      vs0,   vs26,   vs8
    xvmaddasp      vs1,   vs27,   vs8 
    xvmaddasp      vs2,   vs28,   vs8
    xvmaddasp      vs3,   vs29,   vs8 
 

    xvmaddasp      vs0,   vs16,   vs9
    xvmaddasp      vs1,   vs17,   vs9 
    xvmaddasp      vs2,   vs18,   vs9
    xvmaddasp      vs3,   vs19,   vs9 
 

    xvmaddasp      vs0,   vs30,   vs10
    xvmaddasp      vs1,   vs31,   vs10 
    xvmaddasp      vs2,   vs32,   vs10
    xvmaddasp      vs3,   vs33,   vs10 
 

    xvmaddasp      vs0,   vs34,   vs11
    xvmaddasp      vs1,   vs35,   vs11 
    xvmaddasp      vs2,   vs36,   vs11
    xvmaddasp      vs3,   vs37,   vs11 

 
 
 
.if \IsLast==1   
    addi        \BREG, \BREG, DISP4(\Index,16)  
    addi        \AREG, \AREG, DISP64(\Index,256)
.endif 
  
.endm

.macro KERNEL1x16_I_2  AREG,BREG, OffsetA,OffsetB, Index,IsLast  

    lxsd v4,    DISP2(\Index, 0+\OffsetB)(\BREG)
    xxspltw   vs8,  vs36, 1  
    xxspltw   vs9,  vs36, 0      
    lxv vs26,   DISP32(\Index, 0+\OffsetA)(\AREG)
    lxv vs27,   DISP32(\Index,16+\OffsetA)(\AREG)
    lxv vs28,   DISP32(\Index,32+\OffsetA)(\AREG)
    lxv vs29,   DISP32(\Index,48+\OffsetA)(\AREG)      
    lxv vs16,   DISP32(\Index,64+ 0+\OffsetA)(\AREG)
    lxv vs17,   DISP32(\Index,64+ 16+\OffsetA)(\AREG)
    lxv vs18,   DISP32(\Index,64+ 32+\OffsetA)(\AREG)
    lxv vs19,   DISP32(\Index,64+ 48+\OffsetA)(\AREG) 
 
 
    xvmaddasp      vs0,   vs26,   vs8
    xvmaddasp      vs1,   vs27,   vs8 
    xvmaddasp      vs2,   vs28,   vs8
    xvmaddasp      vs3,   vs29,   vs8 
 

    xvmaddasp      vs0,   vs16,   vs9
    xvmaddasp      vs1,   vs17,   vs9 
    xvmaddasp      vs2,   vs18,   vs9
    xvmaddasp      vs3,   vs19,   vs9 
  
 
.if \IsLast==1   
    addi        \BREG, \BREG, DISP2(\Index,8)  
    addi        \AREG, \AREG, DISP32(\Index,128)
.endif 
  
.endm


.macro SAVE1x16

#ifndef TRMMKERNEL    
    lxv        vs16, 0(CO)
    lxv        vs17, 16(CO) 
    lxv        vs18, 32(CO)  
    lxv        vs19, 48(CO)      
#endif
 

#if defined(TRMMKERNEL)
  xvmulsp        vs16, vs0, alpha_r
  xvmulsp        vs17, vs1, alpha_r 
  xvmulsp        vs18, vs2, alpha_r
  xvmulsp        vs19, vs3, alpha_r   
#else
  xvmaddasp        vs16, vs0, alpha_r
  xvmaddasp        vs17, vs1, alpha_r 
  xvmaddasp        vs18, vs2, alpha_r
  xvmaddasp        vs19, vs3, alpha_r   
#endif
    stxv        vs16, 0(CO)
    stxv        vs17, 16(CO) 
    stxv        vs18, 32(CO)  
    stxv        vs19, 48(CO)      
    
  addi CO,CO,64

.endm

/*       M=8 N=1 */

.macro KERNEL1x8_2   OffsetA,OffsetB, Index,IsLast
  KERNEL1x8_I_2 AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast
.endm

 
.macro Zero1x8
    xxlxor      vs0,   vs0,   vs0
    xxlxor      vs1,   vs1,   vs1  
    xxlxor      vs2,   vs2,   vs2
    xxlxor      vs3,   vs3,   vs3          
.endm
 
.macro KERNEL1x8
  KERNEL1x8_1 AO,BO, 0, 0,0,0
.endm
.macro KERNEL1x8_4 OffsetA,OffsetB, Index,IsLast  
  KERNEL1x8_I_4 AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast
.endm
 
.macro KERNEL1x8_1 AREG,BREG,First,OffsetA,OffsetB,Index


    lxssp v4,   DISP1(\Index, 0+\OffsetB)(\BREG)
    xscvdpspn   vs36,vs36
    xxspltw     vs8,  vs36, 0
    lxv vs26,   DISP8(\Index, 0+\OffsetA)(\AREG)
    lxv vs27,   DISP8(\Index,16+\OffsetA)(\AREG)         
 
 
.if \First==1
    xvmulsp      vs0,   vs26,   vs8
    xvmulsp      vs1,   vs27,   vs8  
  
     
.else 
    xvmaddasp      vs0,   vs26,   vs8
    xvmaddasp      vs1,   vs27,   vs8  
 
 .endif
   
    addi        \BREG, \BREG, DISP1(\Index,4)
    addi        \AREG, \AREG, DISP8(\Index,32)  
 
.endm




.macro KERNEL1x8_I_4  AREG,BREG, OffsetA,OffsetB, Index,IsLast  

    lxv vs38,    DISP4(\Index, 0+\OffsetB)(\BREG) 

    lxv vs26,   DISP32(\Index, 0+\OffsetA)(\AREG)
    lxv vs27,   DISP32(\Index,16+\OffsetA)(\AREG) 

    lxv vs16,   DISP32(\Index,32+ 0+\OffsetA)(\AREG)
    lxv vs17,   DISP32(\Index,32+ 16+\OffsetA)(\AREG) 

    xxspltw   vs8,  vs38, 3  
    xxspltw   vs9,  vs38, 2 

    lxv vs30,   DISP32(\Index,64+ 0+\OffsetA)(\AREG)
    lxv vs31,   DISP32(\Index,64+ 16+\OffsetA)(\AREG)   
        
    lxv vs34,   DISP32(\Index,64+ 32+ 0+\OffsetA)(\AREG)
    lxv vs35,   DISP32(\Index,64+ 32+ 16+\OffsetA)(\AREG)  

    xxspltw   vs10, vs38, 1 
    xxspltw   vs11, vs38, 0    

 
    xvmaddasp      vs0,   vs26,   vs8
    xvmaddasp      vs1,   vs27,   vs8  
 

    xvmaddasp      vs2,   vs16,   vs9
    xvmaddasp      vs3,   vs17,   vs9  
 

    xvmaddasp      vs0,   vs30,   vs10
    xvmaddasp      vs1,   vs31,   vs10  
 

    xvmaddasp      vs2,   vs34,   vs11
    xvmaddasp      vs3,   vs35,   vs11  

 
 
 
.if \IsLast==1   
    addi        \BREG, \BREG, DISP4(\Index,16)  
    addi        \AREG, \AREG, DISP32(\Index,128)
.endif 
  
.endm

.macro KERNEL1x8_I_2  AREG,BREG, OffsetA,OffsetB, Index,IsLast  

    lxsd v4,    DISP2(\Index, 0+\OffsetB)(\BREG)
    xxspltw   vs8,  vs36, 1  
    xxspltw   vs9,  vs36, 0      
    lxv vs26,   DISP16(\Index, 0+\OffsetA)(\AREG)
    lxv vs27,   DISP16(\Index,16+\OffsetA)(\AREG)     
    lxv vs16,   DISP16(\Index,32+ 0+\OffsetA)(\AREG)
    lxv vs17,   DISP16(\Index,32+ 16+\OffsetA)(\AREG) 
 
 
    xvmaddasp      vs0,   vs26,   vs8
    xvmaddasp      vs1,   vs27,   vs8  
 

    xvmaddasp      vs2,   vs16,   vs9
    xvmaddasp      vs3,   vs17,   vs9   
  
 
.if \IsLast==1   
    addi        \BREG, \BREG, DISP2(\Index,8)  
    addi        \AREG, \AREG, DISP16(\Index,64)
.endif 
  
.endm


.macro SAVE1x8

#ifndef TRMMKERNEL    
    lxv        vs16, 0(CO)
    lxv        vs17, 16(CO)       
#endif
   /* aggregate vs0 vs2 and vs1 vs3*/
  xvaddsp vs0,vs0,vs2
  xvaddsp  vs1,vs1,vs3
#if defined(TRMMKERNEL)
  xvmulsp        vs16, vs0, alpha_r
  xvmulsp        vs17, vs1, alpha_r     
#else
  xvmaddasp        vs16, vs0, alpha_r
  xvmaddasp        vs17, vs1, alpha_r  
#endif
    stxv        vs16, 0(CO)
    stxv        vs17, 16(CO)      
    
  addi CO,CO,32

.endm
/*M=4*/

.macro KERNEL1x4_2   OffsetA,OffsetB, Index,IsLast
  KERNEL1x4_I_2 AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast
.endm

 
.macro Zero1x4
    xxlxor      vs0,   vs0,   vs0
    xxlxor      vs1,   vs1,   vs1  
    xxlxor      vs2,   vs2,   vs2
    xxlxor      vs3,   vs3,   vs3          
.endm
 
.macro KERNEL1x4
  KERNEL1x4_1 AO,BO, 0, 0,0,0
.endm
.macro KERNEL1x4_4 OffsetA,OffsetB, Index,IsLast  
  KERNEL1x4_I_4 AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast
.endm
 
.macro KERNEL1x4_1 AREG,BREG,First,OffsetA,OffsetB,Index


    lxssp v4,   DISP1(\Index, 0+\OffsetB)(\BREG)
    xscvdpspn   vs36,vs36
    xxspltw     vs8,  vs36, 0
    lxv vs26,   DISP4(\Index, 0+\OffsetA)(\AREG)         
 
 
.if \First==1
    xvmulsp      vs0,   vs26,   vs8 
.else 
    xvmaddasp      vs0,   vs26,   vs8 
 
 .endif
   
    addi        \BREG, \BREG, DISP1(\Index,4)
    addi        \AREG, \AREG, DISP4(\Index,16)  
 
.endm




.macro KERNEL1x4_I_4  AREG,BREG, OffsetA,OffsetB, Index,IsLast  

    lxv vs38,    DISP4(\Index, 0+\OffsetB)(\BREG) 

    lxv vs26,   DISP16(\Index, 0+\OffsetA)(\AREG)
    lxv vs27,   DISP16(\Index,16+\OffsetA)(\AREG) 
 

    xxspltw   vs8,  vs38, 3  
    xxspltw   vs9,  vs38, 2 

    lxv vs30,   DISP16(\Index,32+ 0+\OffsetA)(\AREG)
    lxv vs31,   DISP16(\Index,32+ 16+\OffsetA)(\AREG)   
          

    xxspltw   vs10, vs38, 1 
    xxspltw   vs11, vs38, 0    

 
    xvmaddasp      vs0,   vs26,   vs8 

    xvmaddasp      vs1,   vs27,   vs9 

    xvmaddasp      vs2,   vs30,   vs10   
 

    xvmaddasp      vs3,   vs31,   vs11   

 
 
 
.if \IsLast==1   
    addi        \BREG, \BREG, DISP4(\Index,16)  
    addi        \AREG, \AREG, DISP16(\Index,64)
.endif 
  
.endm

.macro KERNEL1x4_I_2  AREG,BREG, OffsetA,OffsetB, Index,IsLast  

    lxsd v4,    DISP2(\Index, 0+\OffsetB)(\BREG)
    xxspltw   vs8,  vs36, 1  
    xxspltw   vs9,  vs36, 0      
    lxv vs26,   DISP8(\Index, 0+\OffsetA)(\AREG)
    lxv vs27,   DISP8(\Index,16+\OffsetA)(\AREG)      
 
 
    xvmaddasp      vs0,   vs26,   vs8
    xvmaddasp      vs1,   vs27,   vs9
  
 
.if \IsLast==1   
    addi        \BREG, \BREG, DISP2(\Index,8)  
    addi        \AREG, \AREG, DISP8(\Index,32)
.endif 
  
.endm


.macro SAVE1x4

#ifndef TRMMKERNEL    
    lxv        vs16, 0(CO)       
#endif
   /* aggregate */
  xvaddsp vs0,vs0,vs2
  xvaddsp  vs1,vs1,vs3
  xvaddsp  vs0,vs1,vs0
#if defined(TRMMKERNEL)
  xvmulsp        vs16, vs0, alpha_r     
#else
  xvmaddasp        vs16, vs0, alpha_r  
#endif
    stxv        vs16, 0(CO)      
    
  addi CO,CO,16

.endm

/* M=2 N=1*/ 
.macro Zero1x2
    xxlxor      vs0,   vs0,   vs0
    xxlxor      vs1,   vs1,   vs1 
    xxlxor    vs2,vs2,vs2 
    xxlxor    vs3,vs3,vs3     
.endm
 
.macro KERNEL1x2
  KERNEL1x2_1 AO,BO, 0, 0,0,0
.endm
.macro KERNEL1x2_4 OffsetA,OffsetB, Index,IsLast  
  KERNEL1x2_I_4 AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast
.endm

.macro KERNEL1x2_2 OffsetA,OffsetB, Index,IsLast  
  KERNEL1x2_I_2 AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast
.endm
 /*
   we will calculate 1 alone then will add it to batched ones
 */
.macro KERNEL1x2_1 AREG,BREG,First,OffsetA,OffsetB,Index


    lxssp v3,   DISP2(\Index, 0+\OffsetB)(\AREG)
    lxssp v4,   DISP2(\Index, 4+\OffsetB)(\AREG) 
    lxssp v5,   DISP1(\Index, 0+\OffsetA)(\BREG)        
 
 
.if \First==1
    xvmuldp      vs2,   vs37,   vs35 
    xvmuldp      vs3,   vs37,   vs36     
     
.else 
    xsmaddadp     vs2,   vs37,   vs35
    xsmaddadp      vs3,   vs37,   vs36
 .endif
   
    addi        \AREG, \AREG,  DISP2(\Index,8) 
    addi        \BREG, \BREG, DISP1(\Index,4) 
 
.endm




.macro KERNEL1x2_I_4  AREG,BREG, OffsetA,OffsetB, Index,IsLast  

    lxv vs8,    DISP8(\Index, 0+\OffsetB)(\AREG)
    lxv vs10,   DISP8(\Index, 16+\OffsetB)(\AREG) 

    lxv vs26,   DISP4(\Index, 0+\OffsetA)(\BREG)
   
    xxmrglw   vs5, vs26,vs26
    xxmrghw   vs6, vs26,vs26 
 
    xvmaddasp      vs0,   vs8,   vs5
    xvmaddasp      vs1,   vs10,   vs6 
 
 
.if \IsLast==1   
    addi        \AREG, \AREG, DISP8(\Index,32)
    addi        \BREG, \BREG,  DISP4(\Index,16)  
.endif 
  
.endm

.macro KERNEL1x2_I_2  AREG,BREG, OffsetA,OffsetB, Index,IsLast  

    lxssp v3,   DISP4(\Index, 0+\OffsetB)(\AREG)
    lxssp v4,   DISP4(\Index, 4+\OffsetB)(\AREG) 
    lxssp v7,   DISP4(\Index, 8+\OffsetB)(\AREG)
    lxssp v8,   DISP4(\Index, 12+\OffsetB)(\AREG)    
    lxssp v5,   DISP2(\Index, 0+\OffsetA)(\BREG)        
    lxssp v6,   DISP2(\Index, 4+\OffsetA)(\BREG)  
 
 
    xsmaddadp      vs2,   vs37,   vs35
    xsmaddadp      vs3,   vs37,   vs36

    xsmaddadp      vs2,   vs38,   vs39 
    xsmaddadp      vs3,   vs38,   vs40      
 
   
    addi        \AREG, \AREG, DISP4(\Index,16)
    addi        \BREG, \BREG, DISP2(\Index,8) 
.endm


.macro SAVE1x2

#ifndef TRMMKERNEL    
    lxssp v4   , 0(CO)      
    lxssp v5   , 4(CO) 
    
#endif

  /*convert alpha_r for multiply*/
  xscvspdp  vs16,alpha_r

 /*aggregate vectors 1x2_4   */ 
      xxpermdi         vs4,vs0,vs0,2
      xxpermdi         vs5,vs1,vs1,2  
      xvaddsp          vs0,vs0,vs4
      xvaddsp         vs1,vs1,vs5 
      xvaddsp         vs0,vs0,vs1 
/*aggregate vectors 1x1_2 and 1x1_1 into 1x2_4*/
  xscvspdp  vs5, vs0
  xxspltw   vs6, vs0, 1  
  xscvspdp  vs6,vs6 
  xsadddp  vs2,vs2,vs6
  xsadddp  vs3,vs3,vs5  

  /**** store last two words*/
#if defined(TRMMKERNEL) 
  xsmuldp  vs36,vs2, vs16 
  xsmuldp  vs37,vs3, vs16  
 
#else
  xsmaddadp  vs36,vs2, vs16 
  xsmaddadp  vs37,vs3, vs16 
#endif  

  stxssp       v4, 0(CO) 
  stxssp        v5, 4(CO)  

  addi CO,CO,8

.endm
/*///////////////// N=1 M=1 //////////////////*/
.macro Zero1x1
    xxlxor      vs0,   vs0,   vs0
    xxlxor      vs1,   vs1,   vs1 
    xxlxor      vs2, vs2,vs2 
    xxlxor      vs3,vs3,vs3 
    xxlxor      vs4,vs4,vs4       
.endm
 
.macro KERNEL1x1
  KERNEL1x1_1 AO,BO, 1, 0,0,0
.endm

.macro KERNEL1x1_16 OffsetA,OffsetB, Index,IsLast  
  KERNEL1x1_I_16 AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast
.endm

.macro KERNEL1x1_8 OffsetA,OffsetB, Index,IsLast  
  KERNEL1x1_I_8 AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast
.endm

.macro KERNEL1x1_4 OffsetA,OffsetB, Index,IsLast  
  KERNEL1x1_I_4 AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast
.endm

.macro KERNEL1x1_2 OffsetA,OffsetB, Index,IsLast  
  KERNEL1x1_I_2 AO,BO,  \OffsetA,\OffsetB,\Index,\IsLast
.endm
 /*
   we will calculate 1 alone ( FIRST==1 to zero vs4) 
 */
.macro KERNEL1x1_1 AREG,BREG,First,OffsetA,OffsetB,Index


    lxssp v3,   DISP1(\Index, 0+\OffsetB)(\AREG) 
    lxssp v5,   DISP1(\Index, 0+\OffsetA)(\BREG)        
 
 
.if \First==1
    xvmuldp      vs4,   vs37,   vs35       
     
.else 
    xsmaddadp     vs4,   vs37,   vs35 
 .endif
   
    addi        \AREG, \AREG,  DISP1(\Index,4) 
    addi        \BREG, \BREG, DISP1(\Index,4) 
 
.endm


.macro KERNEL1x1_I_16 AREG,BREG, OffsetA,OffsetB, Index,IsLast  

    lxv vs8,    DISP16(\Index, 0+\OffsetB)(\AREG) 
    lxv vs9,    DISP16(\Index, 16+\OffsetB)(\AREG) 
    lxv vs10,   DISP16(\Index, 32+0+\OffsetB)(\AREG) 
    lxv vs11,   DISP16(\Index, 32+ 16+\OffsetB)(\AREG)        
    lxv vs26,   DISP16(\Index, 0+\OffsetA)(\BREG) 
    lxv vs16,   DISP16(\Index, 16+\OffsetA)(\BREG) 
    lxv vs17,   DISP16(\Index, 32+0+\OffsetA)(\BREG) 
    lxv vs18,   DISP16(\Index, 32+16+\OffsetA)(\BREG)     
    xvmaddasp      vs0,   vs8,   vs26 
    xvmaddasp      vs1,   vs9,   vs16  
    xvmaddasp      vs2,   vs10,  vs17 
    xvmaddasp      vs3,   vs11,  vs18
.if \IsLast==1   
    addi        \AREG, \AREG, DISP16(\Index,64)
    addi        \BREG, \BREG,  DISP16(\Index,64)  
.endif 
  
.endm

.macro KERNEL1x1_I_8  AREG,BREG, OffsetA,OffsetB, Index,IsLast  

    lxv vs8,    DISP8(\Index, 0+\OffsetB)(\AREG) 
    lxv vs9,    DISP8(\Index, 16+\OffsetB)(\AREG)     
    lxv vs26,   DISP8(\Index, 0+\OffsetA)(\BREG) 
    lxv vs16,   DISP8(\Index, 16+\OffsetA)(\BREG) 
    xvmaddasp      vs0,   vs8,   vs26 
    xvmaddasp      vs1,   vs9,   vs16 
 
.if \IsLast==1   
    addi        \AREG, \AREG, DISP8(\Index,32)
    addi        \BREG, \BREG,  DISP8(\Index,32)  
.endif 
  
.endm


.macro KERNEL1x1_I_4  AREG,BREG, OffsetA,OffsetB, Index,IsLast  

    lxv vs8,    DISP4(\Index, 0+\OffsetB)(\AREG) 
    lxv vs26,   DISP4(\Index, 0+\OffsetA)(\BREG) 
 
    xvmaddasp      vs0,   vs8,   vs26 
 
 
.if \IsLast==1   
    addi        \AREG, \AREG, DISP4(\Index,16)
    addi        \BREG, \BREG,  DISP4(\Index,16)  
.endif 
  
.endm

.macro KERNEL1x1_I_2  AREG,BREG, OffsetA,OffsetB, Index,IsLast  

    lxsd v4,    DISP2(\Index, 0+\OffsetB)(\AREG) 
    lxsd v5,   DISP2(\Index, 0+\OffsetA)(\BREG) 
 
    xvmaddasp      vs0,   vs36,   vs37 
 
    addi        \AREG, \AREG, DISP2(\Index,8)
    addi        \BREG, \BREG, DISP2(\Index,8) 
.endm


.macro SAVE1x1

#ifndef TRMMKERNEL    
    lxssp v4   , 0(CO)    
    
#endif

  /*convert alpha_r for multiply*/
  xscvspdp  vs16,alpha_r

 /*aggregate vectors   */ 
      xvaddsp          vs0,vs0,vs1
      xvaddsp          vs2,vs2,vs3
      xvaddsp          vs0,vs0,vs2

      xxpermdi         vs7,vs0,vs0,2   
      xvaddsp          vs0,vs0,vs7 
/*aggregate vectors 1x1_2 and 1x1_1 into 1x1_4*/
  xscvspdp  vs5, vs0
  xxspltw   vs6, vs0, 1  
  xscvspdp  vs6,vs6 
  xsadddp  vs7,vs5,vs6
  xsadddp  vs4,vs4,vs7  

  /**** store last two words*/
#if defined(TRMMKERNEL) 
  xsmuldp  vs36,vs4, vs16   
 
#else
  xsmaddadp  vs36,vs4, vs16   
#endif  

  stxssp       v4, 0(CO)    

  addi CO,CO,4

.endm




/****************************TRMM POINTER REFRESH MACROSES*************************/

.macro SHIFT_REG  REG1,REG2,SHIFT_VAL
		.if \SHIFT_VAL==16 
			slwi		\REG1,	\REG2,	6			
		.elseif \SHIFT_VAL==8  
			slwi		\REG1,	\REG2,	5			 
		.elseif \SHIFT_VAL==4
			slwi		\REG1,	\REG2,	4			  
		.elseif \SHIFT_VAL==2
			slwi		\REG1,	\REG2,	3			 
		.elseif \SHIFT_VAL==1
			slwi		\REG1,	\REG2,	2			 
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