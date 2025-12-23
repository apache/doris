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

#define MY_ALIGN .align 3
b CGEMM_L4
/*                MINI SUBROUTINES                            */      
/*                4x8 MAIN 128x+2 LOOP                     */      


CGEMM_L4x8_LMAIN_SUB:
/*----------------------------------------*/   
    mtctr   T8
    LOAD4x8_2 
    MY_ALIGN
CGEMM_L4x8_LOOP:
/*----------------------------------------*/   
    dcbt    AO, PRE
    dcbt    BO, PRE
    KERNEL4x8_L2 128,64,0,0 
CGEMM_L4x8_K128:
/*----------------------------------------*/   
    KERNEL4x8_L2 128,64,1,0
    dcbt    AO, T2  
    KERNEL4x8_L2 128,64,2,0
    KERNEL4x8_L2 128,64,3,0 
    dcbt    AO, T3
    dcbt    BO, T2
    KERNEL4x8_L2 128,64,4,0
    KERNEL4x8_L2 128,64,5,0
    dcbt    AO, T4  
    KERNEL4x8_L2 128,64,6,0
    KERNEL4x8_L2 128,64,7,0  
    dcbt    AO, T5  
    dcbt    BO, T3
    KERNEL4x8_L2 128,64,8,0
    KERNEL4x8_L2 128,64,9,0
    KERNEL4x8_L2 128,64,10,0
    KERNEL4x8_L2 128,64,11,0  
    dcbt    BO, T4
    KERNEL4x8_L2 128,64,12,0
    KERNEL4x8_L2 128,64,13,0
    KERNEL4x8_L2 128,64,14,0
    KERNEL4x8_L2 128,64,15,0  
    KERNEL4x8_L2 128,64,16,0
    KERNEL4x8_L2 128,64,17,0 
    KERNEL4x8_L2 128,64,18,0
    KERNEL4x8_L2 128,64,19,0  
    KERNEL4x8_L2 128,64,20,0
    KERNEL4x8_L2 128,64,21,0 
    KERNEL4x8_L2 128,64,22,0
    KERNEL4x8_L2 128,64,23,0   
    KERNEL4x8_L2 128,64,24,0
    KERNEL4x8_L2 128,64,25,0
    KERNEL4x8_L2 128,64,26,0
    KERNEL4x8_L2 128,64,27,0  
    KERNEL4x8_L2 128,64,28,0
    KERNEL4x8_L2 128,64,29,0
    KERNEL4x8_L2 128,64,30,0
    KERNEL4x8_L2 128,64,31,0 
    KERNEL4x8_L2 128,64,32,0
    KERNEL4x8_L2 128,64,33,0
    KERNEL4x8_L2 128,64,34,0
    KERNEL4x8_L2 128,64,35,0 
    KERNEL4x8_L2 128,64,36,0
    KERNEL4x8_L2 128,64,37,0
    KERNEL4x8_L2 128,64,38,0
    KERNEL4x8_L2 128,64,39,0  
    KERNEL4x8_L2 128,64,40,0
    KERNEL4x8_L2 128,64,41,0
    KERNEL4x8_L2 128,64,42,0
    KERNEL4x8_L2 128,64,43,0  
    KERNEL4x8_L2 128,64,44,0
    KERNEL4x8_L2 128,64,45,0
    KERNEL4x8_L2 128,64,46,0
    KERNEL4x8_L2 128,64,47,0 
    KERNEL4x8_L2 128,64,48,0
    KERNEL4x8_L2 128,64,49,0 
    KERNEL4x8_L2 128,64,50,0
    KERNEL4x8_L2 128,64,51,0  
    KERNEL4x8_L2 128,64,52,0
    KERNEL4x8_L2 128,64,53,0 
    KERNEL4x8_L2 128,64,54,0
    KERNEL4x8_L2 128,64,55,0  
    KERNEL4x8_L2 128,64,56,0
    KERNEL4x8_L2 128,64,57,0
    KERNEL4x8_L2 128,64,58,0
    KERNEL4x8_L2 128,64,59,0  
    KERNEL4x8_L2 128,64,60,0
    KERNEL4x8_L2 128,64,61,0
    KERNEL4x8_L2 128,64,62,0 
    KERNEL4x8_L2 128,64,63,1  
    bdnz    CGEMM_L4x8_LOOP
    MY_ALIGN  
CGEMM_L4x8_LOOP_END:
/*----------------------------------------*/   
    END4x8_2
    blr
    MY_ALIGN


CGEMM_4x8_L64_SUB:
/*----------------------------------------*/   
    LOAD4x8_2  
    dcbt    AO, PRE
    dcbt    BO, PRE
    KERNEL4x8_L2 128,64,0,0 
    KERNEL4x8_L2 128,64,1,0
    dcbt    AO, T2  
    KERNEL4x8_L2 128,64,2,0
    KERNEL4x8_L2 128,64,3,0 
    dcbt    AO, T3
    dcbt    BO, T2
    KERNEL4x8_L2 128,64,4,0
    KERNEL4x8_L2 128,64,5,0
    dcbt    AO, T4  
    KERNEL4x8_L2 128,64,6,0
    KERNEL4x8_L2 128,64,7,0  
    dcbt    AO, T5  
    dcbt    BO, T3
    KERNEL4x8_L2 128,64,8,0
    KERNEL4x8_L2 128,64,9,0
    KERNEL4x8_L2 128,64,10,0
    KERNEL4x8_L2 128,64,11,0  
    dcbt    BO, T4
    KERNEL4x8_L2 128,64,12,0
    KERNEL4x8_L2 128,64,13,0
    KERNEL4x8_L2 128,64,14,0
    KERNEL4x8_L2 128,64,15,0  
    KERNEL4x8_L2 128,64,16,0
    KERNEL4x8_L2 128,64,17,0 
    KERNEL4x8_L2 128,64,18,0
    KERNEL4x8_L2 128,64,19,0  
    KERNEL4x8_L2 128,64,20,0
    KERNEL4x8_L2 128,64,21,0 
    KERNEL4x8_L2 128,64,22,0
    KERNEL4x8_L2 128,64,23,0   
    KERNEL4x8_L2 128,64,24,0
    KERNEL4x8_L2 128,64,25,0
    KERNEL4x8_L2 128,64,26,0
    KERNEL4x8_L2 128,64,27,0  
    KERNEL4x8_L2 128,64,28,0
    KERNEL4x8_L2 128,64,29,0
    KERNEL4x8_L2 128,64,30,0
    KERNEL4x8_E2 128,64,31,1
    blr
    MY_ALIGN


CGEMM_4x8_L32_SUB:
/*----------------------------------------*/   
    LOAD4x8_2  
    dcbt    AO, PRE
    dcbt    BO, PRE
    KERNEL4x8_L2 128,64,0,0 
    KERNEL4x8_L2 128,64,1,0
    dcbt    AO, T2  
    KERNEL4x8_L2 128,64,2,0
    KERNEL4x8_L2 128,64,3,0 
    dcbt    AO, T3
    dcbt    BO, T2
    KERNEL4x8_L2 128,64,4,0
    KERNEL4x8_L2 128,64,5,0
    dcbt    AO, T4  
    KERNEL4x8_L2 128,64,6,0
    KERNEL4x8_L2 128,64,7,0  
    dcbt    AO, T5  
    dcbt    BO, T3
    KERNEL4x8_L2 128,64,8,0
    KERNEL4x8_L2 128,64,9,0
    KERNEL4x8_L2 128,64,10,0
    KERNEL4x8_L2 128,64,11,0  
    dcbt    BO, T4
    KERNEL4x8_L2 128,64,12,0
    KERNEL4x8_L2 128,64,13,0
    KERNEL4x8_L2 128,64,14,0
    KERNEL4x8_E2 128,64,15,1
    blr
    MY_ALIGN


CGEMM_4x8_L16_SUB:
/*----------------------------------------*/   
    LOAD4x8_2 
    dcbt    AO, PRE
    dcbt    BO, PRE
    KERNEL4x8_L2 128,64,0,0 
    KERNEL4x8_L2 128,64,1,0
    dcbt    AO, T2  
    KERNEL4x8_L2 128,64,2,0
    KERNEL4x8_L2 128,64,3,0 
    dcbt    AO, T3
    dcbt    BO, T2
    KERNEL4x8_L2 128,64,4,0
    KERNEL4x8_L2 128,64,5,0
    dcbt    AO, T4  
    KERNEL4x8_L2 128,64,6,0
    KERNEL4x8_E2 128,64,7,1
    blr
    MY_ALIGN


CGEMM_4x4_LMAIN_SUB:
/*----------------------------------------*/   
    mtctr   T8
    LOAD4x4_2  
    MY_ALIGN
CGEMM_L4x4_LOOP:
/*----------------------------------------*/   
    KERNEL4x4_L2 64,64,0,0
CGEMM_L4x4_K32:
/*----------------------------------------*/   
    KERNEL4x4_L2 64,64,1,0   
    KERNEL4x4_L2 64,64,2,0
    KERNEL4x4_L2 64,64,3,0  
    KERNEL4x4_L2 64,64,4,0
    KERNEL4x4_L2 64,64,5,0 
    KERNEL4x4_L2 64,64,6,0
    KERNEL4x4_L2 64,64,7,0
    KERNEL4x4_L2 64,64,8,0
    KERNEL4x4_L2 64,64,9,0   
    KERNEL4x4_L2 64,64,10,0
    KERNEL4x4_L2 64,64,11,0  
    KERNEL4x4_L2 64,64,12,0
    KERNEL4x4_L2 64,64,13,0 
    KERNEL4x4_L2 64,64,14,0
    KERNEL4x4_L2 64,64,15,1    
    bdnz    CGEMM_L4x4_LOOP
    MY_ALIGN  
CGEMM_L4x4_LOOP_END:
/*----------------------------------------*/   
    END4x4_2 
    blr
    MY_ALIGN


CGEMM_4x4_L16_SUB:
/*----------------------------------------*/   
    LOAD4x4_2
    KERNEL4x4_L2 64,64,0,0
    KERNEL4x4_L2 64,64,1,0   
    KERNEL4x4_L2 64,64,2,0
    KERNEL4x4_L2 64,64,3,0  
    KERNEL4x4_L2 64,64,4,0
    KERNEL4x4_L2 64,64,5,0 
    KERNEL4x4_L2 64,64,6,0
    KERNEL4x4_E2 64,64,7,1
    blr
    MY_ALIGN


CGEMM_4x4_L8_SUB:
/*----------------------------------------*/   
    LOAD4x4_2
    KERNEL4x4_L2 64,64,0,0
    KERNEL4x4_L2 64,64,1,0   
    KERNEL4x4_L2 64,64,2,0
    KERNEL4x4_E2 64,64,3,1 
    blr


CGEMM_4x2_LMAIN_SUB:
/*----------------------------------------*/   
    mtctr   T8
    LOAD4x2_2  
    MY_ALIGN 
CGEMM_L4x2_LOOP:
/*----------------------------------------*/   
    KERNEL4x2_L2 32,64,0,0 
CGEMM_L4x2_K32:
/*----------------------------------------*/   
    KERNEL4x2_L2 32,64,1,0  
    KERNEL4x2_L2 32,64,2,0
    KERNEL4x2_L2 32,64,3,0  
    KERNEL4x2_L2 32,64,4,0
    KERNEL4x2_L2 32,64,5,0 
    KERNEL4x2_L2 32,64,6,0
    KERNEL4x2_L2 32,64,7,0
    KERNEL4x2_L2 32,64,8,0
    KERNEL4x2_L2 32,64,9,0  
    KERNEL4x2_L2 32,64,10,0
    KERNEL4x2_L2 32,64,11,0  
    KERNEL4x2_L2 32,64,12,0
    KERNEL4x2_L2 32,64,13,0 
    KERNEL4x2_L2 32,64,14,0
    KERNEL4x2_L2 32,64,15,1   
    bdnz    CGEMM_L4x2_LOOP
    MY_ALIGN  


CGEMM_L4x2_LOOP_END:
/*----------------------------------------*/   
    END4x2_2 
    blr
    MY_ALIGN
CGEMM_4x2_L16_SUB:
/*----------------------------------------*/   
    LOAD4x2_2
    KERNEL4x2_L2 32,64,0,0
    KERNEL4x2_L2 32,64,1,0  
    KERNEL4x2_L2 32,64,2,0
    KERNEL4x2_L2 32,64,3,0  
    KERNEL4x2_L2 32,64,4,0
    KERNEL4x2_L2 32,64,5,0 
    KERNEL4x2_L2 32,64,6,0
    KERNEL4x2_E2 32,64,7,1
    blr
    MY_ALIGN
CGEMM_4x2_L8_SUB:
/*----------------------------------------*/   
    LOAD4x2_2
    KERNEL4x2_L2 32,64,0,0
    KERNEL4x2_L2 32,64,1,0  
    KERNEL4x2_L2 32,64,2,0
    KERNEL4x2_E2 32,64,3,1  
    blr


CGEMM_4x1_LMAIN_SUB:
/*----------------------------------------*/   
    mtctr   T8
    LOAD4x1_2  
    MY_ALIGN
CGEMM_L4x1_LOOP:
/*----------------------------------------*/   
    KERNEL4x1_L2 16,64,0,0 
CGEMM_L4x1_K32:
/*----------------------------------------*/   
    KERNEL4x1_L2 16,64,1,0  
    KERNEL4x1_L2 16,64,2,0
    KERNEL4x1_L2 16,64,3,0  
    KERNEL4x1_L2 16,64,4,0
    KERNEL4x1_L2 16,64,5,0 
    KERNEL4x1_L2 16,64,6,0
    KERNEL4x1_L2 16,64,7,0
    KERNEL4x1_L2 16,64,8,0
    KERNEL4x1_L2 16,64,9,0  
    KERNEL4x1_L2 16,64,10,0
    KERNEL4x1_L2 16,64,11,0  
    KERNEL4x1_L2 16,64,12,0
    KERNEL4x1_L2 16,64,13,0 
    KERNEL4x1_L2 16,64,14,0
    KERNEL4x1_L2 16,64,15,1   
    bdnz    CGEMM_L4x1_LOOP
    MY_ALIGN  
CGEMM_L4x1_LOOP_END:
/*----------------------------------------*/   
    END4x1_2 
    blr

    MY_ALIGN
CGEMM_4x1_L16_SUB:
/*----------------------------------------*/   
    LOAD4x1_2
    KERNEL4x1_L2 16,64,0,0
    KERNEL4x1_L2 16,64,1,0  
    KERNEL4x1_L2 16,64,2,0
    KERNEL4x1_L2 16,64,3,0  
    KERNEL4x1_L2 16,64,4,0
    KERNEL4x1_L2 16,64,5,0 
    KERNEL4x1_L2 16,64,6,0
    KERNEL4x1_E2 16,64,7,1
    blr
    MY_ALIGN


CGEMM_4x1_L8_SUB:
/*----------------------------------------*/   
    LOAD4x1_2
    KERNEL4x1_L2 16,64,0,0
    KERNEL4x1_L2 16,64,1,0  
    KERNEL4x1_L2 16,64,2,0
    KERNEL4x1_E2 16,64,3,1  
    blr



/*             MAIN LOOP BEGINS               */   
    MY_ALIGN


CGEMM_L4:
/*----------------------------------------*/   
#if defined(TRMMKERNEL) && !defined(LEFT)   
    neg TEMP_REG, OFFSET 
#endif   
    /* Pre set value in vs57 as 0xffff0000ffff0000 for masking */
    vspltisb v24, -1
    vspltisb v25, 0
    xxsldwi vs57, vs56, vs57, 1
    xxpermdi vs57, vs57, vs57, 3
    srawi.    J,  N,  2
    ble   CGEMM_L4_END


CGEMM_L4_BEGIN:
/*----------------------------------------*/   
    mr    CO, C
    slwi    T1, LDC , 2     
    add     T2,C,LDC    
    mr    AO, A  
    add   C,  C,  T1
#if defined(TRMMKERNEL) && defined(LEFT)   
    mr TEMP_REG, OFFSET  /*off = offset;*/
#endif     
    srawi.    I,  M,  3
    ble   CGEMM_L4x8_END
    dcbt    CO,r0  /*just prefetch*/
    dcbt    T2,r0    


CGEMM_L4x8_BEGIN:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)   
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,8,4
#else    
    mr    BO, B  
    dcbt    B,  r0  
#endif     
    dcbt    AO, r0
#if defined(TRMMKERNEL)   
    REFRESH_TEMP_BK T6,K,TEMP_REG,8,4
    mr T1, T6
/* TEMPS FOR PREFETCH */   
    li T2, 1024
    li T3, 1024+512
    addi T1,T1, -2
/* TEMPS FOR PREFETCH */     
    li T4, 2048
    li T5, 2048+512   
    srawi.   T8, T1, 7 /**(T1-2) % 128x */
#else   
    mr T1, K
/* TEMPS FOR PREFETCH */   
    li T2, 1024
    li T3, 1024+512
    addi T1,T1, -2
/* TEMPS FOR PREFETCH */     
    li T4, 2048
    li T5, 2048+512 
    srawi.   T8, T1, 7 /**(K-2) % 128x */
#endif   
    ZERO4x8  
    ble   CGEMM_L4x8_SUB0
    bl CGEMM_L4x8_LMAIN_SUB
    andi.   L,  T1, 127
    ble   CGEMM_L4x8_SAVE
    b   CGEMM_L4x8_SUB2


CGEMM_L4x8_SUB0:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)   
    andi.   L,  T6, 255
    cmpwi   T6,129
#else   
    andi.   L,  K,  255
    cmpwi   K,129
#endif       
    li T8,1
    bne CMP4x8_128K
    addi BO,BO,-32
    addi AO,AO,-64 
    LOAD4x8O 64,32 
    END4x8_WITHOUT_ADD   
    LOAD4x8_2O  128, 64 
    mtctr   T8    
    bl CGEMM_L4x8_K128   
    b CGEMM_L4x8_SAVE  
    CMP4x8_128K:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)    
    cmpwi   T6,128
#else    
    cmpwi   K,128
#endif        
    bne CGEMM_L4x8_SUB2 
    MY_ALIGN   
    mtctr   T8
    addi BO,BO,-64
    addi AO,AO,-128   
    LOAD4x8_2O 128,64
    bl CGEMM_L4x8_K128   
    b CGEMM_L4x8_SAVE 
    MY_ALIGN


CGEMM_L4x8_SUB2:
/*----------------------------------------*/   
    andi.      T1,L, 64
    ble CGEMM_L4x8_SUB2_32
    bl  CGEMM_4x8_L64_SUB
    MY_ALIGN


CGEMM_L4x8_SUB2_32:
/*----------------------------------------*/   
    andi.      T1,L, 32
    ble CGEMM_L4x8_SUB2_16    
    bl  CGEMM_4x8_L32_SUB
    MY_ALIGN 


CGEMM_L4x8_SUB2_16:
/*----------------------------------------*/   
    andi.      T1,L, 16
    ble CGEMM_L4x8_SUB2_8
    bl  CGEMM_4x8_L16_SUB  
    MY_ALIGN    


CGEMM_L4x8_SUB2_8:
/*----------------------------------------*/   
    andi.      T1,L, 8
    ble CGEMM_L4x8_SUB2_4
    LOAD4x8_2
    KERNEL4x8_L2  128,64, 0,0
    KERNEL4x8_L2  128,64, 1,0
    KERNEL4x8_L2  128,64, 2,0
    KERNEL4x8_E2  128,64, 3,1
    MY_ALIGN   


CGEMM_L4x8_SUB2_4:
/*----------------------------------------*/   
    andi.      T1,L, 4
    ble CGEMM_L4x8_SUB2_2
    LOAD4x8_2
    KERNEL4x8_L2  128,64, 0,0
    KERNEL4x8_E2  128,64, 1,1
    MY_ALIGN


CGEMM_L4x8_SUB2_2:
/*----------------------------------------*/   
    andi.      T1,L, 2
    ble CGEMM_L4x8_SUB2_1
    LOAD4x8_2 
    KERNEL4x8_E2  128,64, 0,1
    MY_ALIGN    


CGEMM_L4x8_SUB2_1:
/*----------------------------------------*/   
    andi.      T1,L, 1
    ble CGEMM_L4x8_SAVE 
    KERNEL4x8

    MY_ALIGN
CGEMM_L4x8_SAVE:
/*----------------------------------------*/   
    addic.    I,  I,  -1
    MY_ALIGN
    SAVE4x8
#if defined(TRMMKERNEL)    
    REFRESH_AFTER_SAVE T6,K,TEMP_REG,BO,AO,8,4
#endif     
    bgt   CGEMM_L4x8_BEGIN
    andi.   T2, M,  7
    ble   CGEMM_L4x1_END
    andi.   T1, M,  4
    ble   CGEMM_L4x4_END
    b   CGEMM_L4x4_BEGIN
    MY_ALIGN 


CGEMM_L4x8_END:
/*----------------------------------------*/   


CGEMM_L4x4_BEGIN:
/*----------------------------------------*/   
    andi.   T2, M,  7
    ble   CGEMM_L4x1_END
    andi.   T1, M,  4
    ble   CGEMM_L4x4_END
#if defined(TRMMKERNEL)   
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,4,4
#else    
    mr    BO, B   
#endif        
#if defined(TRMMKERNEL)   
    REFRESH_TEMP_BK T6,K,TEMP_REG,4,4
    mr T1, T6 
    addi T1,T1, -2 
    srawi.   T8, T1, 5 /**(T1-2) % 32x */
#else   
    mr T1, K 
    addi T1,T1, -2
    srawi.   T8, T1, 5 /**(K-2) % 32x */
#endif     
    ZERO4x4
    ble   CGEMM_L4x4_SUB0 
    bl CGEMM_4x4_LMAIN_SUB
    andi.   L,  T1, 31
    ble   CGEMM_L4x4_SAVE
    b    CGEMM_L4x4_SUB2


CGEMM_L4x4_SUB0:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)   
    andi.   L,  T6, 63
    cmpwi   T6,33
#else   
    andi.   L,  K,  63
    cmpwi   K,33
#endif       
    li T8,1
    bne CMP4x4_32K
    addi BO,BO,-32
    addi AO,AO,-32  
    LOAD4x4O 32,32 
    END4x4_WITHOUT_ADD   
    LOAD4x4_2O  64, 64 
    mtctr   T8    
    bl CGEMM_L4x4_K32   
    b CGEMM_L4x4_SAVE  
    CMP4x4_32K:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)    
    cmpwi   T6,32
#else    
    cmpwi   K,32
#endif        
    bne CGEMM_L4x4_SUB2 
    MY_ALIGN   
    mtctr   T8
    addi BO,BO,-64
    addi AO,AO,-64   
    LOAD4x4_2O 64,64
    bl CGEMM_L4x4_K32   
    b CGEMM_L4x4_SAVE 
    MY_ALIGN 
    MY_ALIGN 


CGEMM_L4x4_SUB2:
/*----------------------------------------*/   
    andi.      T1,L, 16
    ble CGEMM_L4x4_SUB2_8
    bl  CGEMM_4x4_L16_SUB  
    MY_ALIGN


CGEMM_L4x4_SUB2_8:
/*----------------------------------------*/   
    andi.      T1,L, 8
    ble CGEMM_L4x4_SUB2_4
    bl CGEMM_4x4_L8_SUB
    MY_ALIGN  


CGEMM_L4x4_SUB2_4:
/*----------------------------------------*/   
    andi.      T1,L, 4
    ble CGEMM_L4x4_SUB2_2
    LOAD4x4_2
    KERNEL4x4_L2  64,64, 0,0
    KERNEL4x4_E2  64,64, 1,1
    MY_ALIGN


CGEMM_L4x4_SUB2_2:
/*----------------------------------------*/   
    andi.      T1,L, 2
    ble CGEMM_L4x4_SUB2_1
    LOAD4x4_2
    KERNEL4x4_E2  64,64, 0,1
    MY_ALIGN    


CGEMM_L4x4_SUB2_1:
/*----------------------------------------*/   
    andi.      T1,L, 1
    ble CGEMM_L4x4_SAVE 
    KERNEL4x4


CGEMM_L4x4_SAVE:
/*----------------------------------------*/   
    SAVE4x4
#if defined(TRMMKERNEL)    
    REFRESH_AFTER_SAVE T6,K,TEMP_REG,BO,AO,4,4
#endif     


CGEMM_L4x4_END:
/*----------------------------------------*/   


CGEMM_L4x2_BEGIN:
/*----------------------------------------*/   
    andi.   T1, M,  2
    ble   CGEMM_L4x2_END
#if defined(TRMMKERNEL)   
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,2,4
#else    
    mr    BO, B   
#endif        
#if defined(TRMMKERNEL)   
    REFRESH_TEMP_BK T6,K,TEMP_REG,2,4
    mr T1, T6 
    addi T1,T1, -2 
    srawi.   T8, T1, 5 /**(T1-2) % 32x */
#else   
    mr T1, K 
    addi T1,T1, -2
    srawi.   T8, T1, 5 /**(K-2) % 32x */
#endif     
    ZERO4x2
    ble   CGEMM_L4x2_SUB0 
    bl CGEMM_4x2_LMAIN_SUB
    andi.   L,  T1, 31
    ble   CGEMM_L4x2_SAVE
    b   CGEMM_L4x2_SUB2


CGEMM_L4x2_SUB0:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)   
    andi.   L,  T6, 63
    cmpwi   T6,33
#else   
    andi.   L,  K,  63
    cmpwi   K,33
#endif       
    li T8,1
    bne CMP4x2_32K
    addi BO,BO,-32
    addi AO,AO,-16  
    LOAD4x2O 16,32 
    END4x2_WITHOUT_ADD   
    LOAD4x2_2O  32, 64  
    mtctr   T8    
    bl CGEMM_L4x2_K32   
    b CGEMM_L4x2_SAVE  
    CMP4x2_32K:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)    
    cmpwi   T6,32
#else    
    cmpwi   K,32
#endif        
    bne CGEMM_L4x2_SUB2 
    MY_ALIGN   
    mtctr   T8
    addi BO,BO,-64
    addi AO,AO,-32   
    LOAD4x2_2O 32,64
    bl CGEMM_L4x2_K32   
    b CGEMM_L4x2_SAVE 
    MY_ALIGN 
    MY_ALIGN 


CGEMM_L4x2_SUB2:
/*----------------------------------------*/   
    andi.      T1,L, 16
    ble CGEMM_L4x2_SUB2_8
    bl CGEMM_4x2_L16_SUB  
    MY_ALIGN


CGEMM_L4x2_SUB2_8:
/*----------------------------------------*/   
    andi.      T1,L, 8
    ble CGEMM_L4x2_SUB2_4
    bl CGEMM_4x2_L8_SUB
    MY_ALIGN  


CGEMM_L4x2_SUB2_4:
/*----------------------------------------*/   
    andi.      T1,L, 4
    ble CGEMM_L4x2_SUB2_2
    LOAD4x2_2
    KERNEL4x2_L2  32,64, 0,0
    KERNEL4x2_E2  32,64, 1,1
    MY_ALIGN


CGEMM_L4x2_SUB2_2:
/*----------------------------------------*/   
    andi.      T1,L, 2
    ble CGEMM_L4x2_SUB2_1
    LOAD4x2_2
    KERNEL4x2_E2  32,64, 0,1
    MY_ALIGN    


CGEMM_L4x2_SUB2_1:
/*----------------------------------------*/   
    andi.      T1,L, 1
    ble CGEMM_L4x2_SAVE 
    KERNEL4x2

    MY_ALIGN
CGEMM_L4x2_SAVE:
/*----------------------------------------*/   
    SAVE4x2
#if defined(TRMMKERNEL)    
    REFRESH_AFTER_SAVE T6,K,TEMP_REG,BO,AO,2,4
#endif     


CGEMM_L4x2_END:
/*----------------------------------------*/   


CGEMM_L4x1_BEGIN:
/*----------------------------------------*/   
    andi.   T1, M,  1
    ble   CGEMM_L4x1_END
#if defined(TRMMKERNEL)   
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,1,4
#else    
    mr    BO, B   
#endif        
#if defined(TRMMKERNEL)   
    REFRESH_TEMP_BK T6,K,TEMP_REG,1,4
    mr T1, T6 
    addi T1,T1, -2 
    srawi.   T8, T1, 5 /**(T1-2) % 32x */
#else   
    mr T1, K 
    addi T1,T1, -2
    srawi.   T8, T1, 5 /**(K-2) % 32x */
#endif     
    ZERO4x1
    ble   CGEMM_L4x1_SUB0 
    bl CGEMM_4x1_LMAIN_SUB
    andi.   L,  T1, 31
    ble   CGEMM_L4x1_SAVE
    b   CGEMM_L4x1_SUB2


CGEMM_L4x1_SUB0:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)   
    andi.   L,  T6, 63
    cmpwi   T6,33
#else   
    andi.   L,  K,  63
    cmpwi   K,33
#endif       
    li T8,1
    bne CMP4x1_32K
    addi BO,BO,-32
    addi AO,AO,-8  
    LOAD4x1O 8,32 
    END4x1_WITHOUT_ADD   
    LOAD4x1_2O  16, 64  
    mtctr   T8    
    bl CGEMM_L4x1_K32   
    b CGEMM_L4x1_SAVE  
    CMP4x1_32K:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)    
    cmpwi   T6,32
#else    
    cmpwi   K,32
#endif        
    bne CGEMM_L4x1_SUB2 
    MY_ALIGN   
    mtctr   T8
    addi BO,BO,-64
    addi AO,AO,-16   
    LOAD4x1_2O 16,64
    bl CGEMM_L4x1_K32   
    b CGEMM_L4x1_SAVE 
    MY_ALIGN 
    MY_ALIGN 


CGEMM_L4x1_SUB2:
/*----------------------------------------*/   
    andi.      T1,L, 16
    ble CGEMM_L4x1_SUB2_8
    bl CGEMM_4x1_L16_SUB  
    MY_ALIGN


CGEMM_L4x1_SUB2_8:
/*----------------------------------------*/   
    andi.      T1,L, 8
    ble CGEMM_L4x1_SUB2_4
    bl CGEMM_4x1_L8_SUB
    MY_ALIGN  


CGEMM_L4x1_SUB2_4:
/*----------------------------------------*/   
    andi.      T1,L, 4
    ble CGEMM_L4x1_SUB2_2
    LOAD4x1_2
    KERNEL4x1_L2  16,64, 0,0
    KERNEL4x1_E2  16,64, 1,1
    MY_ALIGN


CGEMM_L4x1_SUB2_2:
/*----------------------------------------*/   
    andi.      T1,L, 2
    ble CGEMM_L4x1_SUB2_1
    LOAD4x1_2
    KERNEL4x1_E2  16,64, 0,1
    MY_ALIGN    


CGEMM_L4x1_SUB2_1:
/*----------------------------------------*/   
    andi.      T1,L, 1
    ble CGEMM_L4x1_SAVE 
    KERNEL4x1

    MY_ALIGN
CGEMM_L4x1_SAVE:
/*----------------------------------------*/  
     
    SAVE4x1
#if defined(TRMMKERNEL)    
    REFRESH_AFTER_SAVE T6,K,TEMP_REG,BO,AO,1,4
#endif   


CGEMM_L4x1_END:
/*----------------------------------------*/   
    slwi    T1, K,  5
    addic.    J,  J,  -1
    add   B,  B,  T1
#if defined(TRMMKERNEL) && !defined(LEFT)   
    addi TEMP_REG, TEMP_REG, 4
#endif   
    bgt   CGEMM_L4_BEGIN


CGEMM_L4_END:

b CGEMM_L2
/*                MINI SUBROUTINES                            */      
/*                2x8 MAIN 128x+2 LOOP                     */


CGEMM_L2x8_LMAIN_SUB:
/*----------------------------------------*/   
    mtctr   T8
    LOAD2x8_2 
    MY_ALIGN
CGEMM_L2x8_LOOP:
/*----------------------------------------*/   
    dcbt    AO, PRE
    dcbt    BO, PRE
    KERNEL2x8_L2 128,32,0,0 
CGEMM_L2x8_K128:
/*----------------------------------------*/   
    KERNEL2x8_L2 128,32,1,0
    dcbt    AO, T2  
    KERNEL2x8_L2 128,32,2,0
    KERNEL2x8_L2 128,32,3,0 
    dcbt    AO, T3
    dcbt    BO, T2
    KERNEL2x8_L2 128,32,4,0
    KERNEL2x8_L2 128,32,5,0
    dcbt    AO, T4  
    KERNEL2x8_L2 128,32,6,0
    KERNEL2x8_L2 128,32,7,0  
    dcbt    AO, T5  
    dcbt    BO, T3
    KERNEL2x8_L2 128,32,8,0
    KERNEL2x8_L2 128,32,9,0
    KERNEL2x8_L2 128,32,10,0
    KERNEL2x8_L2 128,32,11,0  
    dcbt    BO, T4
    KERNEL2x8_L2 128,32,12,0
    KERNEL2x8_L2 128,32,13,0
    KERNEL2x8_L2 128,32,14,0
    KERNEL2x8_L2 128,32,15,0  
    KERNEL2x8_L2 128,32,16,0
    KERNEL2x8_L2 128,32,17,0 
    KERNEL2x8_L2 128,32,18,0
    KERNEL2x8_L2 128,32,19,0  
    KERNEL2x8_L2 128,32,20,0
    KERNEL2x8_L2 128,32,21,0 
    KERNEL2x8_L2 128,32,22,0
    KERNEL2x8_L2 128,32,23,0   
    KERNEL2x8_L2 128,32,24,0
    KERNEL2x8_L2 128,32,25,0
    KERNEL2x8_L2 128,32,26,0
    KERNEL2x8_L2 128,32,27,0  
    KERNEL2x8_L2 128,32,28,0
    KERNEL2x8_L2 128,32,29,0
    KERNEL2x8_L2 128,32,30,0
    KERNEL2x8_L2 128,32,31,0 
    KERNEL2x8_L2 128,32,32,0
    KERNEL2x8_L2 128,32,33,0
    KERNEL2x8_L2 128,32,34,0
    KERNEL2x8_L2 128,32,35,0 
    KERNEL2x8_L2 128,32,36,0
    KERNEL2x8_L2 128,32,37,0
    KERNEL2x8_L2 128,32,38,0
    KERNEL2x8_L2 128,32,39,0  
    KERNEL2x8_L2 128,32,40,0
    KERNEL2x8_L2 128,32,41,0
    KERNEL2x8_L2 128,32,42,0
    KERNEL2x8_L2 128,32,43,0  
    KERNEL2x8_L2 128,32,44,0
    KERNEL2x8_L2 128,32,45,0
    KERNEL2x8_L2 128,32,46,0
    KERNEL2x8_L2 128,32,47,0 
    KERNEL2x8_L2 128,32,48,0
    KERNEL2x8_L2 128,32,49,0 
    KERNEL2x8_L2 128,32,50,0
    KERNEL2x8_L2 128,32,51,0  
    KERNEL2x8_L2 128,32,52,0
    KERNEL2x8_L2 128,32,53,0 
    KERNEL2x8_L2 128,32,54,0
    KERNEL2x8_L2 128,32,55,0  
    KERNEL2x8_L2 128,32,56,0
    KERNEL2x8_L2 128,32,57,0
    KERNEL2x8_L2 128,32,58,0
    KERNEL2x8_L2 128,32,59,0  
    KERNEL2x8_L2 128,32,60,0
    KERNEL2x8_L2 128,32,61,0
    KERNEL2x8_L2 128,32,62,0 
    KERNEL2x8_L2 128,32,63,1  
    bdnz    CGEMM_L2x8_LOOP
    MY_ALIGN  
CGEMM_L2x8_LOOP_END:
/*----------------------------------------*/   
    END2x8_2
    blr
    MY_ALIGN


CGEMM_2x8_L64_SUB:
/*----------------------------------------*/   
    LOAD2x8_2  
    dcbt    AO, PRE
    dcbt    BO, PRE
    KERNEL2x8_L2 128,32,0,0 
    KERNEL2x8_L2 128,32,1,0
    dcbt    AO, T2  
    KERNEL2x8_L2 128,32,2,0
    KERNEL2x8_L2 128,32,3,0 
    dcbt    AO, T3
    dcbt    BO, T2
    KERNEL2x8_L2 128,32,4,0
    KERNEL2x8_L2 128,32,5,0
    dcbt    AO, T4  
    KERNEL2x8_L2 128,32,6,0
    KERNEL2x8_L2 128,32,7,0  
    dcbt    AO, T5  
    dcbt    BO, T3
    KERNEL2x8_L2 128,32,8,0
    KERNEL2x8_L2 128,32,9,0
    KERNEL2x8_L2 128,32,10,0
    KERNEL2x8_L2 128,32,11,0  
    dcbt    BO, T4
    KERNEL2x8_L2 128,32,12,0
    KERNEL2x8_L2 128,32,13,0
    KERNEL2x8_L2 128,32,14,0
    KERNEL2x8_L2 128,32,15,0  
    KERNEL2x8_L2 128,32,16,0
    KERNEL2x8_L2 128,32,17,0 
    KERNEL2x8_L2 128,32,18,0
    KERNEL2x8_L2 128,32,19,0  
    KERNEL2x8_L2 128,32,20,0
    KERNEL2x8_L2 128,32,21,0 
    KERNEL2x8_L2 128,32,22,0
    KERNEL2x8_L2 128,32,23,0   
    KERNEL2x8_L2 128,32,24,0
    KERNEL2x8_L2 128,32,25,0
    KERNEL2x8_L2 128,32,26,0
    KERNEL2x8_L2 128,32,27,0  
    KERNEL2x8_L2 128,32,28,0
    KERNEL2x8_L2 128,32,29,0
    KERNEL2x8_L2 128,32,30,0
    KERNEL2x8_E2 128,32,31,1
    blr
    MY_ALIGN


CGEMM_2x8_L32_SUB:
/*----------------------------------------*/   
    LOAD2x8_2  
    dcbt    AO, PRE
    dcbt    BO, PRE
    KERNEL2x8_L2 128,32,0,0 
    KERNEL2x8_L2 128,32,1,0
    dcbt    AO, T2  
    KERNEL2x8_L2 128,32,2,0
    KERNEL2x8_L2 128,32,3,0 
    dcbt    AO, T3
    dcbt    BO, T2
    KERNEL2x8_L2 128,32,4,0
    KERNEL2x8_L2 128,32,5,0
    dcbt    AO, T4  
    KERNEL2x8_L2 128,32,6,0
    KERNEL2x8_L2 128,32,7,0  
    dcbt    AO, T5  
    dcbt    BO, T3
    KERNEL2x8_L2 128,32,8,0
    KERNEL2x8_L2 128,32,9,0
    KERNEL2x8_L2 128,32,10,0
    KERNEL2x8_L2 128,32,11,0  
    dcbt    BO, T4
    KERNEL2x8_L2 128,32,12,0
    KERNEL2x8_L2 128,32,13,0
    KERNEL2x8_L2 128,32,14,0
    KERNEL2x8_E2 128,32,15,1
    blr
    MY_ALIGN


CGEMM_2x8_L16_SUB:
/*----------------------------------------*/   
    LOAD2x8_2 
    dcbt    AO, PRE
    dcbt    BO, PRE
    KERNEL2x8_L2 128,32,0,0 
    KERNEL2x8_L2 128,32,1,0
    dcbt    AO, T2  
    KERNEL2x8_L2 128,32,2,0
    KERNEL2x8_L2 128,32,3,0 
    dcbt    AO, T3
    dcbt    BO, T2
    KERNEL2x8_L2 128,32,4,0
    KERNEL2x8_L2 128,32,5,0
    dcbt    AO, T4  
    KERNEL2x8_L2 128,32,6,0
    KERNEL2x8_E2 128,32,7,1
    blr
    MY_ALIGN


CGEMM_2x4_LMAIN_SUB:
/*----------------------------------------*/   
    mtctr   T8
    LOAD2x4_2  
    MY_ALIGN
CGEMM_L2x4_LOOP:
/*----------------------------------------*/   
    KERNEL2x4_L2 64,32,0,0
CGEMM_L2x4_K32:
/*----------------------------------------*/   
    KERNEL2x4_L2 64,32,1,0   
    KERNEL2x4_L2 64,32,2,0
    KERNEL2x4_L2 64,32,3,0  
    KERNEL2x4_L2 64,32,4,0
    KERNEL2x4_L2 64,32,5,0 
    KERNEL2x4_L2 64,32,6,0
    KERNEL2x4_L2 64,32,7,0
    KERNEL2x4_L2 64,32,8,0
    KERNEL2x4_L2 64,32,9,0   
    KERNEL2x4_L2 64,32,10,0
    KERNEL2x4_L2 64,32,11,0  
    KERNEL2x4_L2 64,32,12,0
    KERNEL2x4_L2 64,32,13,0 
    KERNEL2x4_L2 64,32,14,0
    KERNEL2x4_L2 64,32,15,1    
    bdnz    CGEMM_L2x4_LOOP
    MY_ALIGN  
CGEMM_L2x4_LOOP_END:
/*----------------------------------------*/   
    END2x4_2 
    blr
    MY_ALIGN


CGEMM_2x4_L16_SUB:
/*----------------------------------------*/   
    LOAD2x4_2
    KERNEL2x4_L2 64,32,0,0
    KERNEL2x4_L2 64,32,1,0   
    KERNEL2x4_L2 64,32,2,0
    KERNEL2x4_L2 64,32,3,0  
    KERNEL2x4_L2 64,32,4,0
    KERNEL2x4_L2 64,32,5,0 
    KERNEL2x4_L2 64,32,6,0
    KERNEL2x4_E2 64,32,7,1
    blr
    MY_ALIGN


CGEMM_2x4_L8_SUB:
/*----------------------------------------*/   
    LOAD2x4_2
    KERNEL2x4_L2 64,32,0,0
    KERNEL2x4_L2 64,32,1,0   
    KERNEL2x4_L2 64,32,2,0
    KERNEL2x4_E2 64,32,3,1 
    blr


CGEMM_2x2_LMAIN_SUB:
/*----------------------------------------*/   
    mtctr   T8
    LOAD2x2_2  
    MY_ALIGN 
CGEMM_L2x2_LOOP:
/*----------------------------------------*/   
    KERNEL2x2_L2 32,32,0,0 
CGEMM_L2x2_K32:
/*----------------------------------------*/   
    KERNEL2x2_L2 32,32,1,0  
    KERNEL2x2_L2 32,32,2,0
    KERNEL2x2_L2 32,32,3,0  
    KERNEL2x2_L2 32,32,4,0
    KERNEL2x2_L2 32,32,5,0 
    KERNEL2x2_L2 32,32,6,0
    KERNEL2x2_L2 32,32,7,0
    KERNEL2x2_L2 32,32,8,0
    KERNEL2x2_L2 32,32,9,0  
    KERNEL2x2_L2 32,32,10,0
    KERNEL2x2_L2 32,32,11,0  
    KERNEL2x2_L2 32,32,12,0
    KERNEL2x2_L2 32,32,13,0 
    KERNEL2x2_L2 32,32,14,0
    KERNEL2x2_L2 32,32,15,1   
    bdnz    CGEMM_L2x2_LOOP
    MY_ALIGN  


CGEMM_L2x2_LOOP_END:
/*----------------------------------------*/   
    END2x2_2 
    blr
    MY_ALIGN
CGEMM_2x2_L16_SUB:
/*----------------------------------------*/   
    LOAD2x2_2
    KERNEL2x2_L2 32,32,0,0
    KERNEL2x2_L2 32,32,1,0  
    KERNEL2x2_L2 32,32,2,0
    KERNEL2x2_L2 32,32,3,0  
    KERNEL2x2_L2 32,32,4,0
    KERNEL2x2_L2 32,32,5,0 
    KERNEL2x2_L2 32,32,6,0
    KERNEL2x2_E2 32,32,7,1
    blr
    MY_ALIGN
CGEMM_2x2_L8_SUB:
/*----------------------------------------*/   
    LOAD2x2_2
    KERNEL2x2_L2 32,32,0,0
    KERNEL2x2_L2 32,32,1,0  
    KERNEL2x2_L2 32,32,2,0
    KERNEL2x2_E2 32,32,3,1  
    blr


CGEMM_2x1_LMAIN_SUB:
/*----------------------------------------*/   
    mtctr   T8
    LOAD2x1_2  
    MY_ALIGN
CGEMM_L2x1_LOOP:
/*----------------------------------------*/   
    KERNEL2x1_L2 16,32,0,0 
CGEMM_L2x1_K32:
/*----------------------------------------*/   
    KERNEL2x1_L2 16,32,1,0  
    KERNEL2x1_L2 16,32,2,0
    KERNEL2x1_L2 16,32,3,0  
    KERNEL2x1_L2 16,32,4,0
    KERNEL2x1_L2 16,32,5,0 
    KERNEL2x1_L2 16,32,6,0
    KERNEL2x1_L2 16,32,7,0
    KERNEL2x1_L2 16,32,8,0
    KERNEL2x1_L2 16,32,9,0  
    KERNEL2x1_L2 16,32,10,0
    KERNEL2x1_L2 16,32,11,0  
    KERNEL2x1_L2 16,32,12,0
    KERNEL2x1_L2 16,32,13,0 
    KERNEL2x1_L2 16,32,14,0
    KERNEL2x1_L2 16,32,15,1   
    bdnz    CGEMM_L2x1_LOOP
    MY_ALIGN  
CGEMM_L2x1_LOOP_END:
/*----------------------------------------*/   
    END2x1_2 
    blr

    MY_ALIGN
CGEMM_2x1_L16_SUB:
/*----------------------------------------*/   
    LOAD2x1_2
    KERNEL2x1_L2 16,32,0,0
    KERNEL2x1_L2 16,32,1,0  
    KERNEL2x1_L2 16,32,2,0
    KERNEL2x1_L2 16,32,3,0  
    KERNEL2x1_L2 16,32,4,0
    KERNEL2x1_L2 16,32,5,0 
    KERNEL2x1_L2 16,32,6,0
    KERNEL2x1_E2 16,32,7,1
    blr
    MY_ALIGN


CGEMM_2x1_L8_SUB:
/*----------------------------------------*/   
    LOAD2x1_2
    KERNEL2x1_L2 16,32,0,0
    KERNEL2x1_L2 16,32,1,0  
    KERNEL2x1_L2 16,32,2,0
    KERNEL2x1_E2 16,32,3,1  
    blr



/*             MAIN LOOP BEGINS               */   
    MY_ALIGN


CGEMM_L2:
/*----------------------------------------*/   

    andi.    J,  N,  2
    ble   CGEMM_L2_END


CGEMM_L2_BEGIN:
/*----------------------------------------*/   
    mr    CO, C
    slwi    T1, LDC , 1     
    add     T2,C,LDC    
    mr    AO, A  
    add   C,  C,  T1
#if defined(TRMMKERNEL) && defined(LEFT)   
    mr TEMP_REG, OFFSET  /*off = offset;*/
#endif     
    srawi.    I,  M, 3
    ble   CGEMM_L2x8_END
    dcbt    CO,r0  /*just prefetch*/
    dcbt    T2,r0    


CGEMM_L2x8_BEGIN:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)   
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,8,2
#else    
    mr    BO, B  
    dcbt    B,  r0  
#endif     
    dcbt    AO, r0
#if defined(TRMMKERNEL)   
    REFRESH_TEMP_BK T6,K,TEMP_REG,8,2
    mr T1, T6
/* TEMPS FOR PREFETCH */   
    li T2, 1024
    li T3, 1024+512
    addi T1,T1, -2
/* TEMPS FOR PREFETCH */     
    li T4, 2048
    li T5, 2048+512   
    srawi.   T8, T1, 7 /**(T1-2) % 128x */
#else   
    mr T1, K
/* TEMPS FOR PREFETCH */   
    li T2, 1024
    li T3, 1024+512
    addi T1,T1, -2
/* TEMPS FOR PREFETCH */     
    li T4, 2048
    li T5, 2048+512 
    srawi.   T8, T1, 7 /**(K-2) % 128x */
#endif   
    ZERO2x8  
    ble   CGEMM_L2x8_SUB0
    bl CGEMM_L2x8_LMAIN_SUB
    andi.   L,  T1, 127
    ble   CGEMM_L2x8_SAVE
    b   CGEMM_L2x8_SUB2


CGEMM_L2x8_SUB0:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)   
    andi.   L,  T6, 255
    cmpwi   T6,129
#else   
    andi.   L,  K,  255
    cmpwi   K,129
#endif       
    li T8,1
    bne CMP2x8_128K
    addi BO,BO,-16
    addi AO,AO,-64 
    LOAD2x8O 64,16 
    END2x8_WITHOUT_ADD   
    LOAD2x8_2O  128, 32 
    mtctr   T8    
    bl CGEMM_L2x8_K128   
    b CGEMM_L2x8_SAVE  
    CMP2x8_128K:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)    
    cmpwi   T6,128
#else    
    cmpwi   K,128
#endif        
    bne CGEMM_L2x8_SUB2 
    MY_ALIGN   
    mtctr   T8
    addi BO,BO,-32
    addi AO,AO,-128   
    LOAD2x8_2O 128,32
    bl CGEMM_L2x8_K128   
    b CGEMM_L2x8_SAVE 
    MY_ALIGN


CGEMM_L2x8_SUB2:
/*----------------------------------------*/   
    andi.      T1,L, 64
    ble CGEMM_L2x8_SUB2_32
    bl  CGEMM_2x8_L64_SUB
    MY_ALIGN


CGEMM_L2x8_SUB2_32:
/*----------------------------------------*/   
    andi.      T1,L, 32
    ble CGEMM_L2x8_SUB2_16    
    bl  CGEMM_2x8_L32_SUB
    MY_ALIGN 


CGEMM_L2x8_SUB2_16:
/*----------------------------------------*/   
    andi.      T1,L, 16
    ble CGEMM_L2x8_SUB2_8
    bl  CGEMM_2x8_L16_SUB  
    MY_ALIGN    


CGEMM_L2x8_SUB2_8:
/*----------------------------------------*/   
    andi.      T1,L, 8
    ble CGEMM_L2x8_SUB2_4
    LOAD2x8_2
    KERNEL2x8_L2  128,32, 0,0
    KERNEL2x8_L2  128,32, 1,0
    KERNEL2x8_L2  128,32, 2,0
    KERNEL2x8_E2  128,32, 3,1
    MY_ALIGN   


CGEMM_L2x8_SUB2_4:
/*----------------------------------------*/   
    andi.      T1,L, 4
    ble CGEMM_L2x8_SUB2_2
    LOAD2x8_2
    KERNEL2x8_L2  128,32, 0,0
    KERNEL2x8_E2  128,32, 1,1
    MY_ALIGN


CGEMM_L2x8_SUB2_2:
/*----------------------------------------*/   
    andi.      T1,L, 2
    ble CGEMM_L2x8_SUB2_1
    LOAD2x8_2 
    KERNEL2x8_E2  128,32, 0,1
    MY_ALIGN    


CGEMM_L2x8_SUB2_1:
/*----------------------------------------*/   
    andi.      T1,L, 1
    ble CGEMM_L2x8_SAVE 
    KERNEL2x8

    MY_ALIGN
CGEMM_L2x8_SAVE:
/*----------------------------------------*/   
    addic.    I,  I,  -1
    MY_ALIGN
    SAVE2x8
#if defined(TRMMKERNEL)    
    REFRESH_AFTER_SAVE T6,K,TEMP_REG,BO,AO,8,2
#endif     
    bgt   CGEMM_L2x8_BEGIN
    andi.   T2, M,  7
    ble   CGEMM_L2x1_END
    andi.   T1, M,  4
    ble   CGEMM_L2x4_END
    b   CGEMM_L2x4_BEGIN
    MY_ALIGN 


CGEMM_L2x8_END:
/*----------------------------------------*/   


CGEMM_L2x4_BEGIN:
/*----------------------------------------*/   
    andi.   T2, M,  7
    ble   CGEMM_L2x1_END
    andi.   T1, M,  4
    ble   CGEMM_L2x4_END
#if defined(TRMMKERNEL)   
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,4,2
#else    
    mr    BO, B   
#endif        
#if defined(TRMMKERNEL)   
    REFRESH_TEMP_BK T6,K,TEMP_REG,4,2
    mr T1, T6 
    addi T1,T1, -2 
    srawi.   T8, T1, 5 /**(T1-2) % 32x */
#else   
    mr T1, K 
    addi T1,T1, -2
    srawi.   T8, T1, 5 /**(K-2) % 32x */
#endif     
    ZERO2x4
    ble   CGEMM_L2x4_SUB0 
    bl CGEMM_2x4_LMAIN_SUB
    andi.   L,  T1, 31
    ble   CGEMM_L2x4_SAVE
    b    CGEMM_L2x4_SUB2


CGEMM_L2x4_SUB0:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)   
    andi.   L,  T6, 63
    cmpwi   T6,33
#else   
    andi.   L,  K,  63
    cmpwi   K,33
#endif       
    li T8,1
    bne CMP2x4_32K
    addi BO,BO,-16
    addi AO,AO,-32  
    LOAD2x4O 32,16 
    END2x4_WITHOUT_ADD   
    LOAD2x4_2O  64, 32 
    mtctr   T8    
    bl CGEMM_L2x4_K32   
    b CGEMM_L2x4_SAVE  
    CMP2x4_32K:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)    
    cmpwi   T6,32
#else    
    cmpwi   K,32
#endif        
    bne CGEMM_L2x4_SUB2 
    MY_ALIGN   
    mtctr   T8
    addi BO,BO,-32
    addi AO,AO,-64   
    LOAD2x4_2O 64,32
    bl CGEMM_L2x4_K32   
    b CGEMM_L2x4_SAVE 
    MY_ALIGN 
    MY_ALIGN 


CGEMM_L2x4_SUB2:
/*----------------------------------------*/   
    andi.      T1,L, 16
    ble CGEMM_L2x4_SUB2_8
    bl  CGEMM_2x4_L16_SUB  
    MY_ALIGN


CGEMM_L2x4_SUB2_8:
/*----------------------------------------*/   
    andi.      T1,L, 8
    ble CGEMM_L2x4_SUB2_4
    bl CGEMM_2x4_L8_SUB
    MY_ALIGN  


CGEMM_L2x4_SUB2_4:
/*----------------------------------------*/   
    andi.      T1,L, 4
    ble CGEMM_L2x4_SUB2_2
    LOAD2x4_2
    KERNEL2x4_L2  64,32, 0,0
    KERNEL2x4_E2  64,32, 1,1
    MY_ALIGN


CGEMM_L2x4_SUB2_2:
/*----------------------------------------*/   
    andi.      T1,L, 2
    ble CGEMM_L2x4_SUB2_1
    LOAD2x4_2
    KERNEL2x4_E2  64,32, 0,1
    MY_ALIGN    


CGEMM_L2x4_SUB2_1:
/*----------------------------------------*/   
    andi.      T1,L, 1
    ble CGEMM_L2x4_SAVE 
    KERNEL2x4


CGEMM_L2x4_SAVE:
/*----------------------------------------*/   
    SAVE2x4
#if defined(TRMMKERNEL)    
    REFRESH_AFTER_SAVE T6,K,TEMP_REG,BO,AO,4,2
#endif     


CGEMM_L2x4_END:
/*----------------------------------------*/   


CGEMM_L2x2_BEGIN:
/*----------------------------------------*/   
    andi.   T1, M,  2
    ble   CGEMM_L2x2_END
#if defined(TRMMKERNEL)   
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,2,2
#else    
    mr    BO, B   
#endif        
#if defined(TRMMKERNEL)   
    REFRESH_TEMP_BK T6,K,TEMP_REG,2,2
    mr T1, T6 
    addi T1,T1, -2 
    srawi.   T8, T1, 5 /**(T1-2) % 32x */
#else   
    mr T1, K 
    addi T1,T1, -2
    srawi.   T8, T1, 5 /**(K-2) % 32x */
#endif     
    ZERO2x2
    ble   CGEMM_L2x2_SUB0 
    bl CGEMM_2x2_LMAIN_SUB
    andi.   L,  T1, 31
    ble   CGEMM_L2x2_SAVE
    b   CGEMM_L2x2_SUB2


CGEMM_L2x2_SUB0:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)   
    andi.   L,  T6, 63
    cmpwi   T6,33
#else   
    andi.   L,  K,  63
    cmpwi   K,33
#endif       
    li T8,1
    bne CMP2x2_32K
    addi BO,BO,-16
    addi AO,AO,-16  
    LOAD2x2O 16,16 
    END2x2_WITHOUT_ADD   
    LOAD2x2_2O  32, 32  
    mtctr   T8    
    bl CGEMM_L2x2_K32   
    b CGEMM_L2x2_SAVE  
    CMP2x2_32K:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)    
    cmpwi   T6,32
#else    
    cmpwi   K,32
#endif        
    bne CGEMM_L2x2_SUB2 
    MY_ALIGN   
    mtctr   T8
    addi BO,BO,-32
    addi AO,AO,-32   
    LOAD2x2_2O 32,32
    bl CGEMM_L2x2_K32   
    b CGEMM_L2x2_SAVE 
    MY_ALIGN 
    MY_ALIGN 


CGEMM_L2x2_SUB2:
/*----------------------------------------*/   
    andi.      T1,L, 16
    ble CGEMM_L2x2_SUB2_8
    bl CGEMM_2x2_L16_SUB  
    MY_ALIGN


CGEMM_L2x2_SUB2_8:
/*----------------------------------------*/   
    andi.      T1,L, 8
    ble CGEMM_L2x2_SUB2_4
    bl CGEMM_2x2_L8_SUB
    MY_ALIGN  


CGEMM_L2x2_SUB2_4:
/*----------------------------------------*/   
    andi.      T1,L, 4
    ble CGEMM_L2x2_SUB2_2
    LOAD2x2_2
    KERNEL2x2_L2  32,32, 0,0
    KERNEL2x2_E2  32,32, 1,1
    MY_ALIGN


CGEMM_L2x2_SUB2_2:
/*----------------------------------------*/   
    andi.      T1,L, 2
    ble CGEMM_L2x2_SUB2_1
    LOAD2x2_2
    KERNEL2x2_E2  32,32, 0,1
    MY_ALIGN    


CGEMM_L2x2_SUB2_1:
/*----------------------------------------*/   
    andi.      T1,L, 1
    ble CGEMM_L2x2_SAVE 
    KERNEL2x2

    MY_ALIGN
CGEMM_L2x2_SAVE:
/*----------------------------------------*/   
    SAVE2x2
#if defined(TRMMKERNEL)    
    REFRESH_AFTER_SAVE T6,K,TEMP_REG,BO,AO,2,2
#endif     


CGEMM_L2x2_END:
/*----------------------------------------*/   


CGEMM_L2x1_BEGIN:
/*----------------------------------------*/   
    andi.   T1, M,  1
    ble   CGEMM_L2x1_END
#if defined(TRMMKERNEL)   
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,1,2
#else    
    mr    BO, B   
#endif        
#if defined(TRMMKERNEL)   
    REFRESH_TEMP_BK T6,K,TEMP_REG,1,2
    mr T1, T6 
    addi T1,T1, -2 
    srawi.   T8, T1, 5 /**(T1-2) % 32x */
#else   
    mr T1, K 
    addi T1,T1, -2
    srawi.   T8, T1, 5 /**(K-2) % 32x */
#endif     
    ZERO2x1
    ble   CGEMM_L2x1_SUB0 
    bl CGEMM_2x1_LMAIN_SUB
    andi.   L,  T1, 31
    ble   CGEMM_L2x1_SAVE
    b   CGEMM_L2x1_SUB2


CGEMM_L2x1_SUB0:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)   
    andi.   L,  T6, 63
    cmpwi   T6,33
#else   
    andi.   L,  K,  63
    cmpwi   K,33
#endif       
    li T8,1
    bne CMP2x1_32K
    addi BO,BO,-16
    addi AO,AO,-8  
    LOAD2x1O 8,16 
    END2x1_WITHOUT_ADD   
    LOAD2x1_2O  16, 32  
    mtctr   T8    
    bl CGEMM_L2x1_K32   
    b CGEMM_L2x1_SAVE  
    CMP2x1_32K:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)    
    cmpwi   T6,32
#else    
    cmpwi   K,32
#endif        
    bne CGEMM_L2x1_SUB2 
    MY_ALIGN   
    mtctr   T8
    addi BO,BO,-32
    addi AO,AO,-16   
    LOAD2x1_2O 16,32
    bl CGEMM_L2x1_K32   
    b CGEMM_L2x1_SAVE 
    MY_ALIGN 
    MY_ALIGN 


CGEMM_L2x1_SUB2:
/*----------------------------------------*/   
    andi.      T1,L, 16
    ble CGEMM_L2x1_SUB2_8
    bl CGEMM_2x1_L16_SUB  
    MY_ALIGN


CGEMM_L2x1_SUB2_8:
/*----------------------------------------*/   
    andi.      T1,L, 8
    ble CGEMM_L2x1_SUB2_4
    bl CGEMM_2x1_L8_SUB
    MY_ALIGN  


CGEMM_L2x1_SUB2_4:
/*----------------------------------------*/   
    andi.      T1,L, 4
    ble CGEMM_L2x1_SUB2_2
    LOAD2x1_2
    KERNEL2x1_L2  16,32, 0,0
    KERNEL2x1_E2  16,32, 1,1
    MY_ALIGN


CGEMM_L2x1_SUB2_2:
/*----------------------------------------*/   
    andi.      T1,L, 2
    ble CGEMM_L2x1_SUB2_1
    LOAD2x1_2
    KERNEL2x1_E2  16,32, 0,1
    MY_ALIGN    


CGEMM_L2x1_SUB2_1:
/*----------------------------------------*/   
    andi.      T1,L, 1
    ble CGEMM_L2x1_SAVE 
    KERNEL2x1

    MY_ALIGN
CGEMM_L2x1_SAVE:
/*----------------------------------------*/  
     
    SAVE2x1
#if defined(TRMMKERNEL)    
    REFRESH_AFTER_SAVE T6,K,TEMP_REG,BO,AO,1,2
#endif   


CGEMM_L2x1_END:
/*----------------------------------------*/   
    slwi    T1, K,  4

    add   B,  B,  T1
#if defined(TRMMKERNEL) && !defined(LEFT)   
    addi TEMP_REG, TEMP_REG, 2
#endif   

CGEMM_L2_END:


b CGEMM_L1
/*                MINI SUBROUTINES                            */      
/*                1x8 MAIN 128x+2 LOOP                     */      


CGEMM_L1x8_LMAIN_SUB:
/*----------------------------------------*/   
    mtctr   T8
    LOAD1x8_2 
    MY_ALIGN
CGEMM_L1x8_LOOP:
/*----------------------------------------*/   
    dcbt    AO, PRE
    dcbt    BO, PRE
    KERNEL1x8_L2 128,16,0,0 
CGEMM_L1x8_K128:
/*----------------------------------------*/   
    KERNEL1x8_L2 128,16,1,0
    dcbt    AO, T2  
    KERNEL1x8_L2 128,16,2,0
    KERNEL1x8_L2 128,16,3,0 
    dcbt    AO, T3
    dcbt    BO, T2
    KERNEL1x8_L2 128,16,4,0
    KERNEL1x8_L2 128,16,5,0
    dcbt    AO, T4  
    KERNEL1x8_L2 128,16,6,0
    KERNEL1x8_L2 128,16,7,0  
    dcbt    AO, T5  
    dcbt    BO, T3
    KERNEL1x8_L2 128,16,8,0
    KERNEL1x8_L2 128,16,9,0
    KERNEL1x8_L2 128,16,10,0
    KERNEL1x8_L2 128,16,11,0  
    dcbt    BO, T4
    KERNEL1x8_L2 128,16,12,0
    KERNEL1x8_L2 128,16,13,0
    KERNEL1x8_L2 128,16,14,0
    KERNEL1x8_L2 128,16,15,0  
    KERNEL1x8_L2 128,16,16,0
    KERNEL1x8_L2 128,16,17,0 
    KERNEL1x8_L2 128,16,18,0
    KERNEL1x8_L2 128,16,19,0  
    KERNEL1x8_L2 128,16,20,0
    KERNEL1x8_L2 128,16,21,0 
    KERNEL1x8_L2 128,16,22,0
    KERNEL1x8_L2 128,16,23,0   
    KERNEL1x8_L2 128,16,24,0
    KERNEL1x8_L2 128,16,25,0
    KERNEL1x8_L2 128,16,26,0
    KERNEL1x8_L2 128,16,27,0  
    KERNEL1x8_L2 128,16,28,0
    KERNEL1x8_L2 128,16,29,0
    KERNEL1x8_L2 128,16,30,0
    KERNEL1x8_L2 128,16,31,0 
    KERNEL1x8_L2 128,16,32,0
    KERNEL1x8_L2 128,16,33,0
    KERNEL1x8_L2 128,16,34,0
    KERNEL1x8_L2 128,16,35,0 
    KERNEL1x8_L2 128,16,36,0
    KERNEL1x8_L2 128,16,37,0
    KERNEL1x8_L2 128,16,38,0
    KERNEL1x8_L2 128,16,39,0  
    KERNEL1x8_L2 128,16,40,0
    KERNEL1x8_L2 128,16,41,0
    KERNEL1x8_L2 128,16,42,0
    KERNEL1x8_L2 128,16,43,0  
    KERNEL1x8_L2 128,16,44,0
    KERNEL1x8_L2 128,16,45,0
    KERNEL1x8_L2 128,16,46,0
    KERNEL1x8_L2 128,16,47,0 
    KERNEL1x8_L2 128,16,48,0
    KERNEL1x8_L2 128,16,49,0 
    KERNEL1x8_L2 128,16,50,0
    KERNEL1x8_L2 128,16,51,0  
    KERNEL1x8_L2 128,16,52,0
    KERNEL1x8_L2 128,16,53,0 
    KERNEL1x8_L2 128,16,54,0
    KERNEL1x8_L2 128,16,55,0  
    KERNEL1x8_L2 128,16,56,0
    KERNEL1x8_L2 128,16,57,0
    KERNEL1x8_L2 128,16,58,0
    KERNEL1x8_L2 128,16,59,0  
    KERNEL1x8_L2 128,16,60,0
    KERNEL1x8_L2 128,16,61,0
    KERNEL1x8_L2 128,16,62,0 
    KERNEL1x8_L2 128,16,63,1  
    bdnz    CGEMM_L1x8_LOOP
    MY_ALIGN  
CGEMM_L1x8_LOOP_END:
/*----------------------------------------*/   
    END1x8_2
    blr
    MY_ALIGN


CGEMM_1x8_L64_SUB:
/*----------------------------------------*/   
    LOAD1x8_2  
    dcbt    AO, PRE
    dcbt    BO, PRE
    KERNEL1x8_L2 128,16,0,0 
    KERNEL1x8_L2 128,16,1,0
    dcbt    AO, T2  
    KERNEL1x8_L2 128,16,2,0
    KERNEL1x8_L2 128,16,3,0 
    dcbt    AO, T3
    dcbt    BO, T2
    KERNEL1x8_L2 128,16,4,0
    KERNEL1x8_L2 128,16,5,0
    dcbt    AO, T4  
    KERNEL1x8_L2 128,16,6,0
    KERNEL1x8_L2 128,16,7,0  
    dcbt    AO, T5  
    dcbt    BO, T3
    KERNEL1x8_L2 128,16,8,0
    KERNEL1x8_L2 128,16,9,0
    KERNEL1x8_L2 128,16,10,0
    KERNEL1x8_L2 128,16,11,0  
    dcbt    BO, T4
    KERNEL1x8_L2 128,16,12,0
    KERNEL1x8_L2 128,16,13,0
    KERNEL1x8_L2 128,16,14,0
    KERNEL1x8_L2 128,16,15,0  
    KERNEL1x8_L2 128,16,16,0
    KERNEL1x8_L2 128,16,17,0 
    KERNEL1x8_L2 128,16,18,0
    KERNEL1x8_L2 128,16,19,0  
    KERNEL1x8_L2 128,16,20,0
    KERNEL1x8_L2 128,16,21,0 
    KERNEL1x8_L2 128,16,22,0
    KERNEL1x8_L2 128,16,23,0   
    KERNEL1x8_L2 128,16,24,0
    KERNEL1x8_L2 128,16,25,0
    KERNEL1x8_L2 128,16,26,0
    KERNEL1x8_L2 128,16,27,0  
    KERNEL1x8_L2 128,16,28,0
    KERNEL1x8_L2 128,16,29,0
    KERNEL1x8_L2 128,16,30,0
    KERNEL1x8_E2 128,16,31,1
    blr
    MY_ALIGN


CGEMM_1x8_L32_SUB:
/*----------------------------------------*/   
    LOAD1x8_2  
    dcbt    AO, PRE
    dcbt    BO, PRE
    KERNEL1x8_L2 128,16,0,0 
    KERNEL1x8_L2 128,16,1,0
    dcbt    AO, T2  
    KERNEL1x8_L2 128,16,2,0
    KERNEL1x8_L2 128,16,3,0 
    dcbt    AO, T3
    dcbt    BO, T2
    KERNEL1x8_L2 128,16,4,0
    KERNEL1x8_L2 128,16,5,0
    dcbt    AO, T4  
    KERNEL1x8_L2 128,16,6,0
    KERNEL1x8_L2 128,16,7,0  
    dcbt    AO, T5  
    dcbt    BO, T3
    KERNEL1x8_L2 128,16,8,0
    KERNEL1x8_L2 128,16,9,0
    KERNEL1x8_L2 128,16,10,0
    KERNEL1x8_L2 128,16,11,0  
    dcbt    BO, T4
    KERNEL1x8_L2 128,16,12,0
    KERNEL1x8_L2 128,16,13,0
    KERNEL1x8_L2 128,16,14,0
    KERNEL1x8_E2 128,16,15,1
    blr
    MY_ALIGN


CGEMM_1x8_L16_SUB:
/*----------------------------------------*/   
    LOAD1x8_2 
    dcbt    AO, PRE
    dcbt    BO, PRE
    KERNEL1x8_L2 128,16,0,0 
    KERNEL1x8_L2 128,16,1,0
    dcbt    AO, T2  
    KERNEL1x8_L2 128,16,2,0
    KERNEL1x8_L2 128,16,3,0 
    dcbt    AO, T3
    dcbt    BO, T2
    KERNEL1x8_L2 128,16,4,0
    KERNEL1x8_L2 128,16,5,0
    dcbt    AO, T4  
    KERNEL1x8_L2 128,16,6,0
    KERNEL1x8_E2 128,16,7,1
    blr
    MY_ALIGN


CGEMM_1x4_LMAIN_SUB:
/*----------------------------------------*/   
    mtctr   T8
    LOAD1x4_2  
    MY_ALIGN
CGEMM_L1x4_LOOP:
/*----------------------------------------*/   
    KERNEL1x4_L2 64,16,0,0
CGEMM_L1x4_K32:
/*----------------------------------------*/   
    KERNEL1x4_L2 64,16,1,0   
    KERNEL1x4_L2 64,16,2,0
    KERNEL1x4_L2 64,16,3,0  
    KERNEL1x4_L2 64,16,4,0
    KERNEL1x4_L2 64,16,5,0 
    KERNEL1x4_L2 64,16,6,0
    KERNEL1x4_L2 64,16,7,0
    KERNEL1x4_L2 64,16,8,0
    KERNEL1x4_L2 64,16,9,0   
    KERNEL1x4_L2 64,16,10,0
    KERNEL1x4_L2 64,16,11,0  
    KERNEL1x4_L2 64,16,12,0
    KERNEL1x4_L2 64,16,13,0 
    KERNEL1x4_L2 64,16,14,0
    KERNEL1x4_L2 64,16,15,1    
    bdnz    CGEMM_L1x4_LOOP
    MY_ALIGN  
CGEMM_L1x4_LOOP_END:
/*----------------------------------------*/   
    END1x4_2 
    blr
    MY_ALIGN


CGEMM_1x4_L16_SUB:
/*----------------------------------------*/   
    LOAD1x4_2
    KERNEL1x4_L2 64,16,0,0
    KERNEL1x4_L2 64,16,1,0   
    KERNEL1x4_L2 64,16,2,0
    KERNEL1x4_L2 64,16,3,0  
    KERNEL1x4_L2 64,16,4,0
    KERNEL1x4_L2 64,16,5,0 
    KERNEL1x4_L2 64,16,6,0
    KERNEL1x4_E2 64,16,7,1
    blr
    MY_ALIGN


CGEMM_1x4_L8_SUB:
/*----------------------------------------*/   
    LOAD1x4_2
    KERNEL1x4_L2 64,16,0,0
    KERNEL1x4_L2 64,16,1,0   
    KERNEL1x4_L2 64,16,2,0
    KERNEL1x4_E2 64,16,3,1 
    blr


CGEMM_1x2_LMAIN_SUB:
/*----------------------------------------*/   
    mtctr   T8
    LOAD1x2_2  
    MY_ALIGN 
CGEMM_L1x2_LOOP:
/*----------------------------------------*/   
    KERNEL1x2_L2 32,16,0,0 
CGEMM_L1x2_K32:
/*----------------------------------------*/   
    KERNEL1x2_L2 32,16,1,0  
    KERNEL1x2_L2 32,16,2,0
    KERNEL1x2_L2 32,16,3,0  
    KERNEL1x2_L2 32,16,4,0
    KERNEL1x2_L2 32,16,5,0 
    KERNEL1x2_L2 32,16,6,0
    KERNEL1x2_L2 32,16,7,0
    KERNEL1x2_L2 32,16,8,0
    KERNEL1x2_L2 32,16,9,0  
    KERNEL1x2_L2 32,16,10,0
    KERNEL1x2_L2 32,16,11,0  
    KERNEL1x2_L2 32,16,12,0
    KERNEL1x2_L2 32,16,13,0 
    KERNEL1x2_L2 32,16,14,0
    KERNEL1x2_L2 32,16,15,1   
    bdnz    CGEMM_L1x2_LOOP
    MY_ALIGN  


CGEMM_L1x2_LOOP_END:
/*----------------------------------------*/   
    END1x2_2 
    blr
    MY_ALIGN
CGEMM_1x2_L16_SUB:
/*----------------------------------------*/   
    LOAD1x2_2
    KERNEL1x2_L2 32,16,0,0
    KERNEL1x2_L2 32,16,1,0  
    KERNEL1x2_L2 32,16,2,0
    KERNEL1x2_L2 32,16,3,0  
    KERNEL1x2_L2 32,16,4,0
    KERNEL1x2_L2 32,16,5,0 
    KERNEL1x2_L2 32,16,6,0
    KERNEL1x2_E2 32,16,7,1
    blr
    MY_ALIGN
CGEMM_1x2_L8_SUB:
/*----------------------------------------*/   
    LOAD1x2_2
    KERNEL1x2_L2 32,16,0,0
    KERNEL1x2_L2 32,16,1,0  
    KERNEL1x2_L2 32,16,2,0
    KERNEL1x2_E2 32,16,3,1  
    blr


CGEMM_1x1_LMAIN_SUB:
/*----------------------------------------*/   
    mtctr   T8
    LOAD1x1_2  
    MY_ALIGN
CGEMM_L1x1_LOOP:
/*----------------------------------------*/   
    KERNEL1x1_L2 16,16,0,0 
CGEMM_L1x1_K32:
/*----------------------------------------*/   
    KERNEL1x1_L2 16,16,1,0  
    KERNEL1x1_L2 16,16,2,0
    KERNEL1x1_L2 16,16,3,0  
    KERNEL1x1_L2 16,16,4,0
    KERNEL1x1_L2 16,16,5,0 
    KERNEL1x1_L2 16,16,6,0
    KERNEL1x1_L2 16,16,7,0
    KERNEL1x1_L2 16,16,8,0
    KERNEL1x1_L2 16,16,9,0  
    KERNEL1x1_L2 16,16,10,0
    KERNEL1x1_L2 16,16,11,0  
    KERNEL1x1_L2 16,16,12,0
    KERNEL1x1_L2 16,16,13,0 
    KERNEL1x1_L2 16,16,14,0
    KERNEL1x1_L2 16,16,15,1   
    bdnz    CGEMM_L1x1_LOOP
    MY_ALIGN  
CGEMM_L1x1_LOOP_END:
/*----------------------------------------*/   
    END1x1_2 
    blr

    MY_ALIGN
CGEMM_1x1_L16_SUB:
/*----------------------------------------*/   
    LOAD1x1_2
    KERNEL1x1_L2 16,16,0,0
    KERNEL1x1_L2 16,16,1,0  
    KERNEL1x1_L2 16,16,2,0
    KERNEL1x1_L2 16,16,3,0  
    KERNEL1x1_L2 16,16,4,0
    KERNEL1x1_L2 16,16,5,0 
    KERNEL1x1_L2 16,16,6,0
    KERNEL1x1_E2 16,16,7,1
    blr
    MY_ALIGN


CGEMM_1x1_L8_SUB:
/*----------------------------------------*/   
    LOAD1x1_2
    KERNEL1x1_L2 16,16,0,0
    KERNEL1x1_L2 16,16,1,0  
    KERNEL1x1_L2 16,16,2,0
    KERNEL1x1_E2 16,16,3,1  
    blr



/*             MAIN LOOP BEGINS               */   
    MY_ALIGN


CGEMM_L1:
/*----------------------------------------*/   

    andi.    J,  N,  1
    ble   CGEMM_L1_END

CGEMM_L1_BEGIN:
/*----------------------------------------*/   
    mr    CO, C  
    add     T2,C,LDC    
    mr    AO, A  
    add   C,  C,  T1
#if defined(TRMMKERNEL) && defined(LEFT)   
    mr TEMP_REG, OFFSET  /*off = offset;*/
#endif     
    srawi.    I,  M,  3
    ble   CGEMM_L1x8_END
    dcbt    CO,r0  /*just prefetch*/
    dcbt    T2,r0    


CGEMM_L1x8_BEGIN:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)   
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,8,1
#else    
    mr    BO, B  
    dcbt    B,  r0  
#endif     
    dcbt    AO, r0
#if defined(TRMMKERNEL)   
    REFRESH_TEMP_BK T6,K,TEMP_REG,8,1
    mr T1, T6
/* TEMPS FOR PREFETCH */   
    li T2, 1024
    li T3, 1024+512
    addi T1,T1, -2
/* TEMPS FOR PREFETCH */     
    li T4, 2048
    li T5, 2048+512   
    srawi.   T8, T1, 7 /**(T1-2) % 128x */
#else   
    mr T1, K
/* TEMPS FOR PREFETCH */   
    li T2, 1024
    li T3, 1024+512
    addi T1,T1, -2
/* TEMPS FOR PREFETCH */     
    li T4, 2048
    li T5, 2048+512 
    srawi.   T8, T1, 7 /**(K-2) % 128x */
#endif   
    ZERO1x8  
    ble   CGEMM_L1x8_SUB0
    bl CGEMM_L1x8_LMAIN_SUB
    andi.   L,  T1, 127
    ble   CGEMM_L1x8_SAVE
    b   CGEMM_L1x8_SUB2


CGEMM_L1x8_SUB0:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)   
    andi.   L,  T6, 255
    cmpwi   T6,129
#else   
    andi.   L,  K,  255
    cmpwi   K,129
#endif       
    li T8,1
    bne CMP1x8_128K
    addi BO,BO,-8
    addi AO,AO,-64 
    LOAD1x8O 64,8 
    END1x8_WITHOUT_ADD   
    LOAD1x8_2O  128, 16 
    mtctr   T8    
    bl CGEMM_L1x8_K128   
    b CGEMM_L1x8_SAVE  
    CMP1x8_128K:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)    
    cmpwi   T6,128
#else    
    cmpwi   K,128
#endif        
    bne CGEMM_L1x8_SUB2 
    MY_ALIGN   
    mtctr   T8
    addi BO,BO,-16
    addi AO,AO,-128   
    LOAD1x8_2O 128,16
    bl CGEMM_L1x8_K128   
    b CGEMM_L1x8_SAVE 
    MY_ALIGN


CGEMM_L1x8_SUB2:
/*----------------------------------------*/   
    andi.      T1,L, 64
    ble CGEMM_L1x8_SUB2_32
    bl  CGEMM_1x8_L64_SUB
    MY_ALIGN


CGEMM_L1x8_SUB2_32:
/*----------------------------------------*/   
    andi.      T1,L, 32
    ble CGEMM_L1x8_SUB2_16    
    bl  CGEMM_1x8_L32_SUB
    MY_ALIGN 


CGEMM_L1x8_SUB2_16:
/*----------------------------------------*/   
    andi.      T1,L, 16
    ble CGEMM_L1x8_SUB2_8
    bl  CGEMM_1x8_L16_SUB  
    MY_ALIGN    


CGEMM_L1x8_SUB2_8:
/*----------------------------------------*/   
    andi.      T1,L, 8
    ble CGEMM_L1x8_SUB2_4
    LOAD1x8_2
    KERNEL1x8_L2  128,16, 0,0
    KERNEL1x8_L2  128,16, 1,0
    KERNEL1x8_L2  128,16, 2,0
    KERNEL1x8_E2  128,16, 3,1
    MY_ALIGN   


CGEMM_L1x8_SUB2_4:
/*----------------------------------------*/   
    andi.      T1,L, 4
    ble CGEMM_L1x8_SUB2_2
    LOAD1x8_2
    KERNEL1x8_L2  128,16, 0,0
    KERNEL1x8_E2  128,16, 1,1
    MY_ALIGN


CGEMM_L1x8_SUB2_2:
/*----------------------------------------*/   
    andi.      T1,L, 2
    ble CGEMM_L1x8_SUB2_1
    LOAD1x8_2 
    KERNEL1x8_E2  128,16, 0,1
    MY_ALIGN    


CGEMM_L1x8_SUB2_1:
/*----------------------------------------*/   
    andi.      T1,L, 1
    ble CGEMM_L1x8_SAVE 
    KERNEL1x8

    MY_ALIGN
CGEMM_L1x8_SAVE:
/*----------------------------------------*/   
    addic.    I,  I,  -1
    MY_ALIGN
    SAVE1x8
#if defined(TRMMKERNEL)    
    REFRESH_AFTER_SAVE T6,K,TEMP_REG,BO,AO,8,1
#endif     
    bgt   CGEMM_L1x8_BEGIN
    andi.   T2, M,  7
    ble   CGEMM_L1x1_END
    andi.   T1, M,  4
    ble   CGEMM_L1x4_END
    b   CGEMM_L1x4_BEGIN
    MY_ALIGN 


CGEMM_L1x8_END:
/*----------------------------------------*/   


CGEMM_L1x4_BEGIN:
/*----------------------------------------*/   
    andi.   T2, M,  7
    ble   CGEMM_L1x1_END
    andi.   T1, M,  4
    ble   CGEMM_L1x4_END
#if defined(TRMMKERNEL)   
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,4,1
#else    
    mr    BO, B   
#endif        
#if defined(TRMMKERNEL)   
    REFRESH_TEMP_BK T6,K,TEMP_REG,4,1
    mr T1, T6 
    addi T1,T1, -2 
    srawi.   T8, T1, 5 /**(T1-2) % 31x */
#else   
    mr T1, K 
    addi T1,T1, -2
    srawi.   T8, T1, 5 /**(K-2) % 31x */
#endif     
    ZERO1x4
    ble   CGEMM_L1x4_SUB0 
    bl CGEMM_1x4_LMAIN_SUB
    andi.   L,  T1, 31
    ble   CGEMM_L1x4_SAVE
    b    CGEMM_L1x4_SUB2


CGEMM_L1x4_SUB0:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)   
    andi.   L,  T6, 63
    cmpwi   T6,33
#else   
    andi.   L,  K,  63
    cmpwi   K,33
#endif       
    li T8,1
    bne CMP1x4_32K
    addi BO,BO,-8
    addi AO,AO,-32  
    LOAD1x4O 32,8 
    END1x4_WITHOUT_ADD   
    LOAD1x4_2O  64, 16 
    mtctr   T8    
    bl CGEMM_L1x4_K32   
    b CGEMM_L1x4_SAVE  
    CMP1x4_32K:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)    
    cmpwi   T6,32
#else    
    cmpwi   K,32
#endif        
    bne CGEMM_L1x4_SUB2 
    MY_ALIGN   
    mtctr   T8
    addi BO,BO,-16
    addi AO,AO,-64   
    LOAD1x4_2O 64,16
    bl CGEMM_L1x4_K32   
    b CGEMM_L1x4_SAVE 
    MY_ALIGN 
    MY_ALIGN 


CGEMM_L1x4_SUB2:
/*----------------------------------------*/   
    andi.      T1,L, 16
    ble CGEMM_L1x4_SUB2_8
    bl  CGEMM_1x4_L16_SUB  
    MY_ALIGN


CGEMM_L1x4_SUB2_8:
/*----------------------------------------*/   
    andi.      T1,L, 8
    ble CGEMM_L1x4_SUB2_4
    bl CGEMM_1x4_L8_SUB
    MY_ALIGN  


CGEMM_L1x4_SUB2_4:
/*----------------------------------------*/   
    andi.      T1,L, 4
    ble CGEMM_L1x4_SUB2_2
    LOAD1x4_2
    KERNEL1x4_L2  64,16, 0,0
    KERNEL1x4_E2  64,16, 1,1
    MY_ALIGN


CGEMM_L1x4_SUB2_2:
/*----------------------------------------*/   
    andi.      T1,L, 2
    ble CGEMM_L1x4_SUB2_1
    LOAD1x4_2
    KERNEL1x4_E2  64,16, 0,1
    MY_ALIGN    


CGEMM_L1x4_SUB2_1:
/*----------------------------------------*/   
    andi.      T1,L, 1
    ble CGEMM_L1x4_SAVE 
    KERNEL1x4


CGEMM_L1x4_SAVE:
/*----------------------------------------*/   
    SAVE1x4
#if defined(TRMMKERNEL)    
    REFRESH_AFTER_SAVE T6,K,TEMP_REG,BO,AO,4,1
#endif     


CGEMM_L1x4_END:
/*----------------------------------------*/   


CGEMM_L1x2_BEGIN:
/*----------------------------------------*/   
    andi.   T1, M,  2
    ble   CGEMM_L1x2_END
#if defined(TRMMKERNEL)   
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,2,1
#else    
    mr    BO, B   
#endif        
#if defined(TRMMKERNEL)   
    REFRESH_TEMP_BK T6,K,TEMP_REG,2,1
    mr T1, T6 
    addi T1,T1, -2 
    srawi.   T8, T1, 5 /**(T1-2) % 31x */
#else   
    mr T1, K 
    addi T1,T1, -2
    srawi.   T8, T1, 5 /**(K-2) % 31x */
#endif     
    ZERO1x2
    ble   CGEMM_L1x2_SUB0 
    bl CGEMM_1x2_LMAIN_SUB
    andi.   L,  T1, 31
    ble   CGEMM_L1x2_SAVE
    b   CGEMM_L1x2_SUB2


CGEMM_L1x2_SUB0:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)   
    andi.   L,  T6, 63
    cmpwi   T6,33
#else   
    andi.   L,  K,  63
    cmpwi   K,33
#endif       
    li T8,1
    bne CMP1x2_32K
    addi BO,BO,-8
    addi AO,AO,-16  
    LOAD1x2O 16,8 
    END1x2_WITHOUT_ADD   
    LOAD1x2_2O  32, 16  
    mtctr   T8    
    bl CGEMM_L1x2_K32   
    b CGEMM_L1x2_SAVE  
    CMP1x2_32K:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)    
    cmpwi   T6,32
#else    
    cmpwi   K,32
#endif        
    bne CGEMM_L1x2_SUB2 
    MY_ALIGN   
    mtctr   T8
    addi BO,BO,-16
    addi AO,AO,-32   
    LOAD1x2_2O 32,16
    bl CGEMM_L1x2_K32   
    b CGEMM_L1x2_SAVE 
    MY_ALIGN 
    MY_ALIGN 


CGEMM_L1x2_SUB2:
/*----------------------------------------*/   
    andi.      T1,L, 16
    ble CGEMM_L1x2_SUB2_8
    bl CGEMM_1x2_L16_SUB  
    MY_ALIGN


CGEMM_L1x2_SUB2_8:
/*----------------------------------------*/   
    andi.      T1,L, 8
    ble CGEMM_L1x2_SUB2_4
    bl CGEMM_1x2_L8_SUB
    MY_ALIGN  


CGEMM_L1x2_SUB2_4:
/*----------------------------------------*/   
    andi.      T1,L, 4
    ble CGEMM_L1x2_SUB2_2
    LOAD1x2_2
    KERNEL1x2_L2  32,16, 0,0
    KERNEL1x2_E2  32,16, 1,1
    MY_ALIGN


CGEMM_L1x2_SUB2_2:
/*----------------------------------------*/   
    andi.      T1,L, 2
    ble CGEMM_L1x2_SUB2_1
    LOAD1x2_2
    KERNEL1x2_E2  32,16, 0,1
    MY_ALIGN    


CGEMM_L1x2_SUB2_1:
/*----------------------------------------*/   
    andi.      T1,L, 1
    ble CGEMM_L1x2_SAVE 
    KERNEL1x2

    MY_ALIGN
CGEMM_L1x2_SAVE:
/*----------------------------------------*/   
    SAVE1x2
#if defined(TRMMKERNEL)    
    REFRESH_AFTER_SAVE T6,K,TEMP_REG,BO,AO,2,1
#endif     


CGEMM_L1x2_END:
/*----------------------------------------*/   


CGEMM_L1x1_BEGIN:
/*----------------------------------------*/   
    andi.   T1, M,  1
    ble   CGEMM_L1x1_END
#if defined(TRMMKERNEL)   
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,1,1
#else    
    mr    BO, B   
#endif        
#if defined(TRMMKERNEL)   
    REFRESH_TEMP_BK T6,K,TEMP_REG,1,1
    mr T1, T6 
    addi T1,T1, -2 
    srawi.   T8, T1, 5 /**(T1-2) % 31x */
#else   
    mr T1, K 
    addi T1,T1, -2
    srawi.   T8, T1, 5 /**(K-2) % 31x */
#endif     
    ZERO1x1
    ble   CGEMM_L1x1_SUB0 
    bl CGEMM_1x1_LMAIN_SUB
    andi.   L,  T1, 31
    ble   CGEMM_L1x1_SAVE
    b   CGEMM_L1x1_SUB2


CGEMM_L1x1_SUB0:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)   
    andi.   L,  T6, 63
    cmpwi   T6,33
#else   
    andi.   L,  K,  63
    cmpwi   K,33
#endif       
    li T8,1
    bne CMP1x1_32K
    addi BO,BO,-8
    addi AO,AO,-8  
    LOAD1x1O 8,8 
    END1x1_WITHOUT_ADD   
    LOAD1x1_2O  16, 16  
    mtctr   T8    
    bl CGEMM_L1x1_K32   
    b CGEMM_L1x1_SAVE  
    CMP1x1_32K:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)    
    cmpwi   T6,32
#else    
    cmpwi   K,32
#endif        
    bne CGEMM_L1x1_SUB2 
    MY_ALIGN   
    mtctr   T8
    addi BO,BO,-16
    addi AO,AO,-16   
    LOAD1x1_2O 16,16
    bl CGEMM_L1x1_K32   
    b CGEMM_L1x1_SAVE 
    MY_ALIGN 
    MY_ALIGN 


CGEMM_L1x1_SUB2:
/*----------------------------------------*/   
    andi.      T1,L, 16
    ble CGEMM_L1x1_SUB2_8
    bl CGEMM_1x1_L16_SUB  
    MY_ALIGN


CGEMM_L1x1_SUB2_8:
/*----------------------------------------*/   
    andi.      T1,L, 8
    ble CGEMM_L1x1_SUB2_4
    bl CGEMM_1x1_L8_SUB
    MY_ALIGN  


CGEMM_L1x1_SUB2_4:
/*----------------------------------------*/   
    andi.      T1,L, 4
    ble CGEMM_L1x1_SUB2_2
    LOAD1x1_2
    KERNEL1x1_L2  16,16, 0,0
    KERNEL1x1_E2  16,16, 1,1
    MY_ALIGN


CGEMM_L1x1_SUB2_2:
/*----------------------------------------*/   
    andi.      T1,L, 2
    ble CGEMM_L1x1_SUB2_1
    LOAD1x1_2
    KERNEL1x1_E2  16,16, 0,1
    MY_ALIGN    


CGEMM_L1x1_SUB2_1:
/*----------------------------------------*/   
    andi.      T1,L, 1
    ble CGEMM_L1x1_SAVE 
    KERNEL1x1

    MY_ALIGN
CGEMM_L1x1_SAVE:
/*----------------------------------------*/  
     
    SAVE1x1
#if defined(TRMMKERNEL)    
    REFRESH_AFTER_SAVE T6,K,TEMP_REG,BO,AO,1,1
#endif   


CGEMM_L1x1_END:
/*----------------------------------------*/   
    slwi    T1, K,  3

    add   B,  B,  T1
#if defined(TRMMKERNEL) && !defined(LEFT)   
    addi TEMP_REG, TEMP_REG, 1
#endif   

CGEMM_L1_END:




