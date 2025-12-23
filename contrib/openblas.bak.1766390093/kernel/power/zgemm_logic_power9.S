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
#define MY_ALIGN .align 3
b ZGEMM_L2
/*                MINI SUBROUTINES                            */      
/*                2x8 MAIN 128x+2 LOOP                     */      


ZGEMM_L2x8_LMAIN_SUB:
/*----------------------------------------*/   
    mtctr   T8
    LOAD2x8_2 
    MY_ALIGN
ZGEMM_L2x8_LOOP:
/*----------------------------------------*/   
    dcbt    AO, PRE
    dcbt    BO, PRE
    KERNEL2x8_L2 256,64,0,0 
ZGEMM_L2x8_K128:
/*----------------------------------------*/   
    KERNEL2x8_L2 256,64,1,0
    dcbt    AO, T2  
    KERNEL2x8_L2 256,64,2,0
    KERNEL2x8_L2 256,64,3,0 
    dcbt    AO, T3
    dcbt    BO, T2
    KERNEL2x8_L2 256,64,4,0
    KERNEL2x8_L2 256,64,5,0
    dcbt    AO, T4  
    KERNEL2x8_L2 256,64,6,0
    KERNEL2x8_L2 256,64,7,0  
    dcbt    AO, T5  
    dcbt    BO, T3
    KERNEL2x8_L2 256,64,8,0
    KERNEL2x8_L2 256,64,9,0
    KERNEL2x8_L2 256,64,10,0
    KERNEL2x8_L2 256,64,11,0  
    dcbt    BO, T4
    KERNEL2x8_L2 256,64,12,0
    KERNEL2x8_L2 256,64,13,0
    KERNEL2x8_L2 256,64,14,0
    KERNEL2x8_L2 256,64,15,0  
    KERNEL2x8_L2 256,64,16,0
    KERNEL2x8_L2 256,64,17,0 
    KERNEL2x8_L2 256,64,18,0
    KERNEL2x8_L2 256,64,19,0  
    KERNEL2x8_L2 256,64,20,0
    KERNEL2x8_L2 256,64,21,0 
    KERNEL2x8_L2 256,64,22,0
    KERNEL2x8_L2 256,64,23,0   
    KERNEL2x8_L2 256,64,24,0
    KERNEL2x8_L2 256,64,25,0
    KERNEL2x8_L2 256,64,26,0
    KERNEL2x8_L2 256,64,27,0  
    KERNEL2x8_L2 256,64,28,0
    KERNEL2x8_L2 256,64,29,0
    KERNEL2x8_L2 256,64,30,0
    KERNEL2x8_L2 256,64,31,0 
    KERNEL2x8_L2 256,64,32,0
    KERNEL2x8_L2 256,64,33,0
    KERNEL2x8_L2 256,64,34,0
    KERNEL2x8_L2 256,64,35,0 
    KERNEL2x8_L2 256,64,36,0
    KERNEL2x8_L2 256,64,37,0
    KERNEL2x8_L2 256,64,38,0
    KERNEL2x8_L2 256,64,39,0  
    KERNEL2x8_L2 256,64,40,0
    KERNEL2x8_L2 256,64,41,0
    KERNEL2x8_L2 256,64,42,0
    KERNEL2x8_L2 256,64,43,0  
    KERNEL2x8_L2 256,64,44,0
    KERNEL2x8_L2 256,64,45,0
    KERNEL2x8_L2 256,64,46,0
    KERNEL2x8_L2 256,64,47,0 
    KERNEL2x8_L2 256,64,48,0
    KERNEL2x8_L2 256,64,49,0 
    KERNEL2x8_L2 256,64,50,0
    KERNEL2x8_L2 256,64,51,0  
    KERNEL2x8_L2 256,64,52,0
    KERNEL2x8_L2 256,64,53,0 
    KERNEL2x8_L2 256,64,54,0
    KERNEL2x8_L2 256,64,55,0  
    KERNEL2x8_L2 256,64,56,0
    KERNEL2x8_L2 256,64,57,0
    KERNEL2x8_L2 256,64,58,0
    KERNEL2x8_L2 256,64,59,0  
    KERNEL2x8_L2 256,64,60,0
    KERNEL2x8_L2 256,64,61,0
    KERNEL2x8_L2 256,64,62,0 
    KERNEL2x8_L2 256,64,63,1  
    bdnz    ZGEMM_L2x8_LOOP
    MY_ALIGN  
ZGEMM_L2x8_LOOP_END:
/*----------------------------------------*/   
    END2x8_2
    blr
    MY_ALIGN


ZGEMM_2x8_L64_SUB:
/*----------------------------------------*/   
    LOAD2x8_2  
    dcbt    AO, PRE
    dcbt    BO, PRE
    KERNEL2x8_L2 256,64,0,0 
    KERNEL2x8_L2 256,64,1,0
    dcbt    AO, T2  
    KERNEL2x8_L2 256,64,2,0
    KERNEL2x8_L2 256,64,3,0 
    dcbt    AO, T3
    dcbt    BO, T2
    KERNEL2x8_L2 256,64,4,0
    KERNEL2x8_L2 256,64,5,0
    dcbt    AO, T4  
    KERNEL2x8_L2 256,64,6,0
    KERNEL2x8_L2 256,64,7,0  
    dcbt    AO, T5  
    dcbt    BO, T3
    KERNEL2x8_L2 256,64,8,0
    KERNEL2x8_L2 256,64,9,0
    KERNEL2x8_L2 256,64,10,0
    KERNEL2x8_L2 256,64,11,0  
    dcbt    BO, T4
    KERNEL2x8_L2 256,64,12,0
    KERNEL2x8_L2 256,64,13,0
    KERNEL2x8_L2 256,64,14,0
    KERNEL2x8_L2 256,64,15,0  
    KERNEL2x8_L2 256,64,16,0
    KERNEL2x8_L2 256,64,17,0 
    KERNEL2x8_L2 256,64,18,0
    KERNEL2x8_L2 256,64,19,0  
    KERNEL2x8_L2 256,64,20,0
    KERNEL2x8_L2 256,64,21,0 
    KERNEL2x8_L2 256,64,22,0
    KERNEL2x8_L2 256,64,23,0   
    KERNEL2x8_L2 256,64,24,0
    KERNEL2x8_L2 256,64,25,0
    KERNEL2x8_L2 256,64,26,0
    KERNEL2x8_L2 256,64,27,0  
    KERNEL2x8_L2 256,64,28,0
    KERNEL2x8_L2 256,64,29,0
    KERNEL2x8_L2 256,64,30,0
    KERNEL2x8_E2 256,64,31,1
    blr
    MY_ALIGN


ZGEMM_2x8_L32_SUB:
/*----------------------------------------*/   
    LOAD2x8_2  
    dcbt    AO, PRE
    dcbt    BO, PRE
    KERNEL2x8_L2 256,64,0,0 
    KERNEL2x8_L2 256,64,1,0
    dcbt    AO, T2  
    KERNEL2x8_L2 256,64,2,0
    KERNEL2x8_L2 256,64,3,0 
    dcbt    AO, T3
    dcbt    BO, T2
    KERNEL2x8_L2 256,64,4,0
    KERNEL2x8_L2 256,64,5,0
    dcbt    AO, T4  
    KERNEL2x8_L2 256,64,6,0
    KERNEL2x8_L2 256,64,7,0  
    dcbt    AO, T5  
    dcbt    BO, T3
    KERNEL2x8_L2 256,64,8,0
    KERNEL2x8_L2 256,64,9,0
    KERNEL2x8_L2 256,64,10,0
    KERNEL2x8_L2 256,64,11,0  
    dcbt    BO, T4
    KERNEL2x8_L2 256,64,12,0
    KERNEL2x8_L2 256,64,13,0
    KERNEL2x8_L2 256,64,14,0
    KERNEL2x8_E2 256,64,15,1
    blr
    MY_ALIGN


ZGEMM_2x8_L16_SUB:
/*----------------------------------------*/   
    LOAD2x8_2 
    dcbt    AO, PRE
    dcbt    BO, PRE
    KERNEL2x8_L2 256,64,0,0 
    KERNEL2x8_L2 256,64,1,0
    dcbt    AO, T2  
    KERNEL2x8_L2 256,64,2,0
    KERNEL2x8_L2 256,64,3,0 
    dcbt    AO, T3
    dcbt    BO, T2
    KERNEL2x8_L2 256,64,4,0
    KERNEL2x8_L2 256,64,5,0
    dcbt    AO, T4  
    KERNEL2x8_L2 256,64,6,0
    KERNEL2x8_E2 256,64,7,1
    blr
    MY_ALIGN


ZGEMM_2x4_LMAIN_SUB:
/*----------------------------------------*/   
    mtctr   T8
    LOAD2x4_2  
    MY_ALIGN
ZGEMM_L2x4_LOOP:
/*----------------------------------------*/   
    KERNEL2x4_L2 128,64,0,0
ZGEMM_L2x4_K32:
/*----------------------------------------*/   
    KERNEL2x4_L2 128,64,1,0   
    KERNEL2x4_L2 128,64,2,0
    KERNEL2x4_L2 128,64,3,0  
    KERNEL2x4_L2 128,64,4,0
    KERNEL2x4_L2 128,64,5,0 
    KERNEL2x4_L2 128,64,6,0
    KERNEL2x4_L2 128,64,7,0
    KERNEL2x4_L2 128,64,8,0
    KERNEL2x4_L2 128,64,9,0   
    KERNEL2x4_L2 128,64,10,0
    KERNEL2x4_L2 128,64,11,0  
    KERNEL2x4_L2 128,64,12,0
    KERNEL2x4_L2 128,64,13,0 
    KERNEL2x4_L2 128,64,14,0
    KERNEL2x4_L2 128,64,15,1    
    bdnz    ZGEMM_L2x4_LOOP
    MY_ALIGN  
ZGEMM_L2x4_LOOP_END:
/*----------------------------------------*/   
    END2x4_2 
    blr
    MY_ALIGN


ZGEMM_2x4_L16_SUB:
/*----------------------------------------*/   
    LOAD2x4_2
    KERNEL2x4_L2 128,64,0,0
    KERNEL2x4_L2 128,64,1,0   
    KERNEL2x4_L2 128,64,2,0
    KERNEL2x4_L2 128,64,3,0  
    KERNEL2x4_L2 128,64,4,0
    KERNEL2x4_L2 128,64,5,0 
    KERNEL2x4_L2 128,64,6,0
    KERNEL2x4_E2 128,64,7,1
    blr
    MY_ALIGN


ZGEMM_2x4_L8_SUB:
/*----------------------------------------*/   
    LOAD2x4_2
    KERNEL2x4_L2 128,64,0,0
    KERNEL2x4_L2 128,64,1,0   
    KERNEL2x4_L2 128,64,2,0
    KERNEL2x4_E2 128,64,3,1 
    blr


ZGEMM_2x2_LMAIN_SUB:
/*----------------------------------------*/   
    mtctr   T8
    LOAD2x2_2  
    MY_ALIGN 
ZGEMM_L2x2_LOOP:
/*----------------------------------------*/   
    KERNEL2x2_L2 64,64,0,0 
ZGEMM_L2x2_K32:
/*----------------------------------------*/   
    KERNEL2x2_L2 64,64,1,0  
    KERNEL2x2_L2 64,64,2,0
    KERNEL2x2_L2 64,64,3,0  
    KERNEL2x2_L2 64,64,4,0
    KERNEL2x2_L2 64,64,5,0 
    KERNEL2x2_L2 64,64,6,0
    KERNEL2x2_L2 64,64,7,0
    KERNEL2x2_L2 64,64,8,0
    KERNEL2x2_L2 64,64,9,0  
    KERNEL2x2_L2 64,64,10,0
    KERNEL2x2_L2 64,64,11,0  
    KERNEL2x2_L2 64,64,12,0
    KERNEL2x2_L2 64,64,13,0 
    KERNEL2x2_L2 64,64,14,0
    KERNEL2x2_L2 64,64,15,1   
    bdnz    ZGEMM_L2x2_LOOP
    MY_ALIGN  


ZGEMM_L2x2_LOOP_END:
/*----------------------------------------*/   
    END2x2_2 
    blr
    MY_ALIGN
ZGEMM_2x2_L16_SUB:
/*----------------------------------------*/   
    LOAD2x2_2
    KERNEL2x2_L2 64,64,0,0
    KERNEL2x2_L2 64,64,1,0  
    KERNEL2x2_L2 64,64,2,0
    KERNEL2x2_L2 64,64,3,0  
    KERNEL2x2_L2 64,64,4,0
    KERNEL2x2_L2 64,64,5,0 
    KERNEL2x2_L2 64,64,6,0
    KERNEL2x2_E2 64,64,7,1
    blr
    MY_ALIGN
ZGEMM_2x2_L8_SUB:
/*----------------------------------------*/   
    LOAD2x2_2
    KERNEL2x2_L2 64,64,0,0
    KERNEL2x2_L2 64,64,1,0  
    KERNEL2x2_L2 64,64,2,0
    KERNEL2x2_E2 64,64,3,1  
    blr


ZGEMM_2x1_LMAIN_SUB:
/*----------------------------------------*/   
    mtctr   T8
    LOAD2x1_2  
    MY_ALIGN
ZGEMM_L2x1_LOOP:
/*----------------------------------------*/   
    KERNEL2x1_L2 32,64,0,0 
ZGEMM_L2x1_K32:
/*----------------------------------------*/   
    KERNEL2x1_L2 32,64,1,0  
    KERNEL2x1_L2 32,64,2,0
    KERNEL2x1_L2 32,64,3,0  
    KERNEL2x1_L2 32,64,4,0
    KERNEL2x1_L2 32,64,5,0 
    KERNEL2x1_L2 32,64,6,0
    KERNEL2x1_L2 32,64,7,0
    KERNEL2x1_L2 32,64,8,0
    KERNEL2x1_L2 32,64,9,0  
    KERNEL2x1_L2 32,64,10,0
    KERNEL2x1_L2 32,64,11,0  
    KERNEL2x1_L2 32,64,12,0
    KERNEL2x1_L2 32,64,13,0 
    KERNEL2x1_L2 32,64,14,0
    KERNEL2x1_L2 32,64,15,1   
    bdnz    ZGEMM_L2x1_LOOP
    MY_ALIGN  
ZGEMM_L2x1_LOOP_END:
/*----------------------------------------*/   
    END2x1_2 
    blr

    MY_ALIGN
ZGEMM_2x1_L16_SUB:
/*----------------------------------------*/   
    LOAD2x1_2
    KERNEL2x1_L2 32,64,0,0
    KERNEL2x1_L2 32,64,1,0  
    KERNEL2x1_L2 32,64,2,0
    KERNEL2x1_L2 32,64,3,0  
    KERNEL2x1_L2 32,64,4,0
    KERNEL2x1_L2 32,64,5,0 
    KERNEL2x1_L2 32,64,6,0
    KERNEL2x1_E2 32,64,7,1
    blr
    MY_ALIGN


ZGEMM_2x1_L8_SUB:
/*----------------------------------------*/   
    LOAD2x1_2
    KERNEL2x1_L2 32,64,0,0
    KERNEL2x1_L2 32,64,1,0  
    KERNEL2x1_L2 32,64,2,0
    KERNEL2x1_E2 32,64,3,1  
    blr



/*             MAIN LOOP BEGINS               */   
    MY_ALIGN


ZGEMM_L2:
/*----------------------------------------*/   
#if defined(TRMMKERNEL) && !defined(LEFT)   
    neg TEMP_REG, OFFSET 
#endif   
    srawi.    J,  N,  1
    ble   ZGEMM_L2_END


ZGEMM_L2_BEGIN:
/*----------------------------------------*/   
    mr    CO, C
    slwi    T1, LDC , 1     
    add     T2,C,LDC    
    mr    AO, A  
    add   C,  C,  T1
#if defined(TRMMKERNEL) && defined(LEFT)   
    mr TEMP_REG, OFFSET  /*off = offset;*/
#endif     
    srawi.    I,  M,  3
    ble   ZGEMM_L2x8_END
    dcbt    CO,r0  /*just prefetch*/
    dcbt    T2,r0    


ZGEMM_L2x8_BEGIN:
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
    srawi.   T8, T1, 7 /**(T11-2) % 128x */
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
    ble   ZGEMM_L2x8_SUB0
    bl ZGEMM_L2x8_LMAIN_SUB
    andi.   L,  T1, 127
    ble   ZGEMM_L2x8_SAVE
    b   ZGEMM_L2x8_SUB2


ZGEMM_L2x8_SUB0:
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
    addi BO,BO,-32
    addi AO,AO,-128 
    LOAD2x8O 128,32 
    END2x8_WITHOUT_ADD   
    LOAD2x8_2O  256, 64 
    mtctr   T8    
    bl ZGEMM_L2x8_K128   
    b ZGEMM_L2x8_SAVE  
    CMP2x8_128K:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)    
    cmpwi   T6,128
#else    
    cmpwi   K,128
#endif        
    bne ZGEMM_L2x8_SUB2 
    MY_ALIGN   
    mtctr   T8
    addi BO,BO,-64
    addi AO,AO,-256   
    LOAD2x8_2O 256,64
    bl ZGEMM_L2x8_K128   
    b ZGEMM_L2x8_SAVE 
    MY_ALIGN


ZGEMM_L2x8_SUB2:
/*----------------------------------------*/   
    andi.      T1,L, 64
    ble ZGEMM_L2x8_SUB2_32
    bl  ZGEMM_2x8_L64_SUB
    MY_ALIGN


ZGEMM_L2x8_SUB2_32:
/*----------------------------------------*/   
    andi.      T1,L, 32
    ble ZGEMM_L2x8_SUB2_16    
    bl  ZGEMM_2x8_L32_SUB
    MY_ALIGN 


ZGEMM_L2x8_SUB2_16:
/*----------------------------------------*/   
    andi.      T1,L, 16
    ble ZGEMM_L2x8_SUB2_8
    bl  ZGEMM_2x8_L16_SUB  
    MY_ALIGN    


ZGEMM_L2x8_SUB2_8:
/*----------------------------------------*/   
    andi.      T1,L, 8
    ble ZGEMM_L2x8_SUB2_4
    LOAD2x8_2
    KERNEL2x8_L2  256,64, 0,0
    KERNEL2x8_L2  256,64, 1,0
    KERNEL2x8_L2  256,64, 2,0
    KERNEL2x8_E2  256,64, 3,1
    MY_ALIGN   


ZGEMM_L2x8_SUB2_4:
/*----------------------------------------*/   
    andi.      T1,L, 4
    ble ZGEMM_L2x8_SUB2_2
    LOAD2x8_2
    KERNEL2x8_L2  256,64, 0,0
    KERNEL2x8_E2  256,64, 1,1
    MY_ALIGN


ZGEMM_L2x8_SUB2_2:
/*----------------------------------------*/   
    andi.      T1,L, 2
    ble ZGEMM_L2x8_SUB2_1
    LOAD2x8_2 
    KERNEL2x8_E2  256,64, 0,1
    MY_ALIGN    


ZGEMM_L2x8_SUB2_1:
/*----------------------------------------*/   
    andi.      T1,L, 1
    ble ZGEMM_L2x8_SAVE 
    KERNEL2x8


ZGEMM_L2x8_SAVE:
/*----------------------------------------*/   
    addic.    I,  I,  -1
    SAVE2x8
#if defined(TRMMKERNEL)    
    REFRESH_AFTER_SAVE T6,K,TEMP_REG,BO,AO,8,2
#endif     
    bgt   ZGEMM_L2x8_BEGIN
    andi.   T2, M,  7
    ble   ZGEMM_L2x1_END
    andi.   T1, M,  4
    ble   ZGEMM_L2x4_END
    b   ZGEMM_L2x4_BEGIN
    MY_ALIGN 


ZGEMM_L2x8_END:
/*----------------------------------------*/   


ZGEMM_L2x4_BEGIN:
/*----------------------------------------*/   
    andi.   T2, M,  7
    ble   ZGEMM_L2x1_END
    andi.   T1, M,  4
    ble   ZGEMM_L2x4_END
#if defined(TRMMKERNEL)   
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,4,2
#else    
    mr    BO, B   
#endif        
#if defined(TRMMKERNEL)   
    REFRESH_TEMP_BK T6,K,TEMP_REG,4,2
    mr T1, T6 
    addi T1,T1, -2 
    srawi.   T8, T1, 5 /**(T11-2) % 32x */
#else   
    mr T1, K 
    addi T1,T1, -2
    srawi.   T8, T1, 5 /**(K-2) % 32x */
#endif     
    ZERO2x4
    ble   ZGEMM_L2x4_SUB0 
    bl ZGEMM_2x4_LMAIN_SUB
    andi.   L,  T1, 31
    ble   ZGEMM_L2x4_SAVE
    b    ZGEMM_L2x4_SUB2


ZGEMM_L2x4_SUB0:
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
    addi BO,BO,-32
    addi AO,AO,-64  
    LOAD2x4O 64,32 
    END2x4_WITHOUT_ADD   
    LOAD2x4_2O  128, 64 
    mtctr   T8    
    bl ZGEMM_L2x4_K32   
    b ZGEMM_L2x4_SAVE  
    CMP2x4_32K:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)    
    cmpwi   T6,32
#else    
    cmpwi   K,32
#endif        
    bne ZGEMM_L2x4_SUB2 
    MY_ALIGN   
    mtctr   T8
    addi BO,BO,-64
    addi AO,AO,-128   
    LOAD2x4_2O 128,64
    bl ZGEMM_L2x4_K32   
    b ZGEMM_L2x4_SAVE 
    MY_ALIGN 
    MY_ALIGN 


ZGEMM_L2x4_SUB2:
/*----------------------------------------*/   
    andi.      T1,L, 16
    ble ZGEMM_L2x4_SUB2_8
    bl  ZGEMM_2x4_L16_SUB  
    MY_ALIGN


ZGEMM_L2x4_SUB2_8:
/*----------------------------------------*/   
    andi.      T1,L, 8
    ble ZGEMM_L2x4_SUB2_4
    bl ZGEMM_2x4_L8_SUB
    MY_ALIGN  


ZGEMM_L2x4_SUB2_4:
/*----------------------------------------*/   
    andi.      T1,L, 4
    ble ZGEMM_L2x4_SUB2_2
    LOAD2x4_2
    KERNEL2x4_L2  128,64, 0,0
    KERNEL2x4_E2  128,64, 1,1
    MY_ALIGN


ZGEMM_L2x4_SUB2_2:
/*----------------------------------------*/   
    andi.      T1,L, 2
    ble ZGEMM_L2x4_SUB2_1
    LOAD2x4_2
    KERNEL2x4_E2  128,64, 0,1
    MY_ALIGN    


ZGEMM_L2x4_SUB2_1:
/*----------------------------------------*/   
    andi.      T1,L, 1
    ble ZGEMM_L2x4_SAVE 
    KERNEL2x4


ZGEMM_L2x4_SAVE:
/*----------------------------------------*/   
    SAVE2x4
#if defined(TRMMKERNEL)    
    REFRESH_AFTER_SAVE T6,K,TEMP_REG,BO,AO,4,2
#endif     


ZGEMM_L2x4_END:
/*----------------------------------------*/   


ZGEMM_L2x2_BEGIN:
/*----------------------------------------*/   
    andi.   T1, M,  2
    ble   ZGEMM_L2x2_END
#if defined(TRMMKERNEL)   
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,2,2
#else    
    mr    BO, B   
#endif        
#if defined(TRMMKERNEL)   
    REFRESH_TEMP_BK T6,K,TEMP_REG,2,2
    mr T1, T6 
    addi T1,T1, -2 
    srawi.   T8, T1, 5 /**(T11-2) % 32x */
#else   
    mr T1, K 
    addi T1,T1, -2
    srawi.   T8, T1, 5 /**(K-2) % 32x */
#endif     
    ZERO2x2
    ble   ZGEMM_L2x2_SUB0 
    bl ZGEMM_2x2_LMAIN_SUB
    andi.   L,  T1, 31
    ble   ZGEMM_L2x2_SAVE
    b   ZGEMM_L2x2_SUB2


ZGEMM_L2x2_SUB0:
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
    addi BO,BO,-32
    addi AO,AO,-32  
    LOAD2x2O 32,32 
    END2x2_WITHOUT_ADD   
    LOAD2x2_2O  64, 64  
    mtctr   T8    
    bl ZGEMM_L2x2_K32   
    b ZGEMM_L2x2_SAVE  
    CMP2x2_32K:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)    
    cmpwi   T6,32
#else    
    cmpwi   K,32
#endif        
    bne ZGEMM_L2x2_SUB2 
    MY_ALIGN   
    mtctr   T8
    addi BO,BO,-64
    addi AO,AO,-64   
    LOAD2x2_2O 64,64
    bl ZGEMM_L2x2_K32   
    b ZGEMM_L2x2_SAVE 
    MY_ALIGN 
    MY_ALIGN 


ZGEMM_L2x2_SUB2:
/*----------------------------------------*/   
    andi.      T1,L, 16
    ble ZGEMM_L2x2_SUB2_8
    bl ZGEMM_2x2_L16_SUB  
    MY_ALIGN


ZGEMM_L2x2_SUB2_8:
/*----------------------------------------*/   
    andi.      T1,L, 8
    ble ZGEMM_L2x2_SUB2_4
    bl ZGEMM_2x2_L8_SUB
    MY_ALIGN  


ZGEMM_L2x2_SUB2_4:
/*----------------------------------------*/   
    andi.      T1,L, 4
    ble ZGEMM_L2x2_SUB2_2
    LOAD2x2_2
    KERNEL2x2_L2  64,64, 0,0
    KERNEL2x2_E2  64,64, 1,1
    MY_ALIGN


ZGEMM_L2x2_SUB2_2:
/*----------------------------------------*/   
    andi.      T1,L, 2
    ble ZGEMM_L2x2_SUB2_1
    LOAD2x2_2
    KERNEL2x2_E2  64,64, 0,1
    MY_ALIGN    


ZGEMM_L2x2_SUB2_1:
/*----------------------------------------*/   
    andi.      T1,L, 1
    ble ZGEMM_L2x2_SAVE 
    KERNEL2x2


ZGEMM_L2x2_SAVE:
/*----------------------------------------*/   
    SAVE2x2
#if defined(TRMMKERNEL)    
    REFRESH_AFTER_SAVE T6,K,TEMP_REG,BO,AO,2,2
#endif     


ZGEMM_L2x2_END:
/*----------------------------------------*/   


ZGEMM_L2x1_BEGIN:
/*----------------------------------------*/   
    andi.   T1, M,  1
    ble   ZGEMM_L2x1_END
#if defined(TRMMKERNEL)   
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,1,2
#else    
    mr    BO, B   
#endif        
#if defined(TRMMKERNEL)   
    REFRESH_TEMP_BK T6,K,TEMP_REG,1,2
    mr T1, T6 
    addi T1,T1, -2 
    srawi.   T8, T1, 5 /**(T11-2) % 32x */
#else   
    mr T1, K 
    addi T1,T1, -2
    srawi.   T8, T1, 5 /**(K-2) % 32x */
#endif     
    ZERO2x1
    ble   ZGEMM_L2x1_SUB0 
    bl ZGEMM_2x1_LMAIN_SUB
    andi.   L,  T1, 31
    ble   ZGEMM_L2x1_SAVE
    b   ZGEMM_L2x1_SUB2


ZGEMM_L2x1_SUB0:
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
    addi BO,BO,-32
    addi AO,AO,-16  
    LOAD2x1O 16,32 
    END2x1_WITHOUT_ADD   
    LOAD2x1_2O  32, 64  
    mtctr   T8    
    bl ZGEMM_L2x1_K32   
    b ZGEMM_L2x1_SAVE  
    CMP2x1_32K:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)    
    cmpwi   T6,32
#else    
    cmpwi   K,32
#endif        
    bne ZGEMM_L2x1_SUB2 
    MY_ALIGN   
    mtctr   T8
    addi BO,BO,-64
    addi AO,AO,-32   
    LOAD2x1_2O 32,64
    bl ZGEMM_L2x1_K32   
    b ZGEMM_L2x1_SAVE 
    MY_ALIGN 
    MY_ALIGN 


ZGEMM_L2x1_SUB2:
/*----------------------------------------*/   
    andi.      T1,L, 16
    ble ZGEMM_L2x1_SUB2_8
    bl ZGEMM_2x1_L16_SUB  
    MY_ALIGN


ZGEMM_L2x1_SUB2_8:
/*----------------------------------------*/   
    andi.      T1,L, 8
    ble ZGEMM_L2x1_SUB2_4
    bl ZGEMM_2x1_L8_SUB
    MY_ALIGN  


ZGEMM_L2x1_SUB2_4:
/*----------------------------------------*/   
    andi.      T1,L, 4
    ble ZGEMM_L2x1_SUB2_2
    LOAD2x1_2
    KERNEL2x1_L2  32,64, 0,0
    KERNEL2x1_E2  32,64, 1,1
    MY_ALIGN


ZGEMM_L2x1_SUB2_2:
/*----------------------------------------*/   
    andi.      T1,L, 2
    ble ZGEMM_L2x1_SUB2_1
    LOAD2x1_2
    KERNEL2x1_E2  32,64, 0,1
    MY_ALIGN    


ZGEMM_L2x1_SUB2_1:
/*----------------------------------------*/   
    andi.      T1,L, 1
    ble ZGEMM_L2x1_SAVE 
    KERNEL2x1


ZGEMM_L2x1_SAVE:
/*----------------------------------------*/   
    SAVE2x1
#if defined(TRMMKERNEL)    
    REFRESH_AFTER_SAVE T6,K,TEMP_REG,BO,AO,1,2
#endif   


ZGEMM_L2x1_END:
/*----------------------------------------*/   
    slwi    T1, K,  5
    addic.    J,  J,  -1
    add   B,  B,  T1
#if defined(TRMMKERNEL) && !defined(LEFT)   
    addi TEMP_REG, TEMP_REG, 2
#endif   
    bgt   ZGEMM_L2_BEGIN


ZGEMM_L2_END:

b ZGEMM_L1
/*                MINI SUBROUTINES                            */      
/*                1x8 MAIN 128x+2 LOOP                     */      


ZGEMM_L1x8_LMAIN_SUB:
/*----------------------------------------*/   
    mtctr   T8
    LOAD1x8_2 
    MY_ALIGN
ZGEMM_L1x8_LOOP:
/*----------------------------------------*/   
    dcbt    AO, PRE
    dcbt    BO, PRE
    KERNEL1x8_L2 256,32,0,0 
ZGEMM_L1x8_K128:
/*----------------------------------------*/   
    KERNEL1x8_L2 256,32,1,0
    dcbt    AO, T2  
    KERNEL1x8_L2 256,32,2,0
    KERNEL1x8_L2 256,32,3,0 
    dcbt    AO, T3
    dcbt    BO, T2
    KERNEL1x8_L2 256,32,4,0
    KERNEL1x8_L2 256,32,5,0
    dcbt    AO, T4  
    KERNEL1x8_L2 256,32,6,0
    KERNEL1x8_L2 256,32,7,0  
    dcbt    AO, T5  
    dcbt    BO, T3
    KERNEL1x8_L2 256,32,8,0
    KERNEL1x8_L2 256,32,9,0
    KERNEL1x8_L2 256,32,10,0
    KERNEL1x8_L2 256,32,11,0  
    dcbt    BO, T4
    KERNEL1x8_L2 256,32,12,0
    KERNEL1x8_L2 256,32,13,0
    KERNEL1x8_L2 256,32,14,0
    KERNEL1x8_L2 256,32,15,0  
    KERNEL1x8_L2 256,32,16,0
    KERNEL1x8_L2 256,32,17,0 
    KERNEL1x8_L2 256,32,18,0
    KERNEL1x8_L2 256,32,19,0  
    KERNEL1x8_L2 256,32,20,0
    KERNEL1x8_L2 256,32,21,0 
    KERNEL1x8_L2 256,32,22,0
    KERNEL1x8_L2 256,32,23,0   
    KERNEL1x8_L2 256,32,24,0
    KERNEL1x8_L2 256,32,25,0
    KERNEL1x8_L2 256,32,26,0
    KERNEL1x8_L2 256,32,27,0  
    KERNEL1x8_L2 256,32,28,0
    KERNEL1x8_L2 256,32,29,0
    KERNEL1x8_L2 256,32,30,0
    KERNEL1x8_L2 256,32,31,0 
    KERNEL1x8_L2 256,32,32,0
    KERNEL1x8_L2 256,32,33,0
    KERNEL1x8_L2 256,32,34,0
    KERNEL1x8_L2 256,32,35,0 
    KERNEL1x8_L2 256,32,36,0
    KERNEL1x8_L2 256,32,37,0
    KERNEL1x8_L2 256,32,38,0
    KERNEL1x8_L2 256,32,39,0  
    KERNEL1x8_L2 256,32,40,0
    KERNEL1x8_L2 256,32,41,0
    KERNEL1x8_L2 256,32,42,0
    KERNEL1x8_L2 256,32,43,0  
    KERNEL1x8_L2 256,32,44,0
    KERNEL1x8_L2 256,32,45,0
    KERNEL1x8_L2 256,32,46,0
    KERNEL1x8_L2 256,32,47,0 
    KERNEL1x8_L2 256,32,48,0
    KERNEL1x8_L2 256,32,49,0 
    KERNEL1x8_L2 256,32,50,0
    KERNEL1x8_L2 256,32,51,0  
    KERNEL1x8_L2 256,32,52,0
    KERNEL1x8_L2 256,32,53,0 
    KERNEL1x8_L2 256,32,54,0
    KERNEL1x8_L2 256,32,55,0  
    KERNEL1x8_L2 256,32,56,0
    KERNEL1x8_L2 256,32,57,0
    KERNEL1x8_L2 256,32,58,0
    KERNEL1x8_L2 256,32,59,0  
    KERNEL1x8_L2 256,32,60,0
    KERNEL1x8_L2 256,32,61,0
    KERNEL1x8_L2 256,32,62,0 
    KERNEL1x8_L2 256,32,63,1  
    bdnz    ZGEMM_L1x8_LOOP
    MY_ALIGN  
ZGEMM_L1x8_LOOP_END:
/*----------------------------------------*/   
    END1x8_2
    blr
    MY_ALIGN


ZGEMM_1x8_L64_SUB:
/*----------------------------------------*/   
    LOAD1x8_2  
    dcbt    AO, PRE
    dcbt    BO, PRE
    KERNEL1x8_L2 256,32,0,0 
    KERNEL1x8_L2 256,32,1,0
    dcbt    AO, T2  
    KERNEL1x8_L2 256,32,2,0
    KERNEL1x8_L2 256,32,3,0 
    dcbt    AO, T3
    dcbt    BO, T2
    KERNEL1x8_L2 256,32,4,0
    KERNEL1x8_L2 256,32,5,0
    dcbt    AO, T4  
    KERNEL1x8_L2 256,32,6,0
    KERNEL1x8_L2 256,32,7,0  
    dcbt    AO, T5  
    dcbt    BO, T3
    KERNEL1x8_L2 256,32,8,0
    KERNEL1x8_L2 256,32,9,0
    KERNEL1x8_L2 256,32,10,0
    KERNEL1x8_L2 256,32,11,0  
    dcbt    BO, T4
    KERNEL1x8_L2 256,32,12,0
    KERNEL1x8_L2 256,32,13,0
    KERNEL1x8_L2 256,32,14,0
    KERNEL1x8_L2 256,32,15,0  
    KERNEL1x8_L2 256,32,16,0
    KERNEL1x8_L2 256,32,17,0 
    KERNEL1x8_L2 256,32,18,0
    KERNEL1x8_L2 256,32,19,0  
    KERNEL1x8_L2 256,32,20,0
    KERNEL1x8_L2 256,32,21,0 
    KERNEL1x8_L2 256,32,22,0
    KERNEL1x8_L2 256,32,23,0   
    KERNEL1x8_L2 256,32,24,0
    KERNEL1x8_L2 256,32,25,0
    KERNEL1x8_L2 256,32,26,0
    KERNEL1x8_L2 256,32,27,0  
    KERNEL1x8_L2 256,32,28,0
    KERNEL1x8_L2 256,32,29,0
    KERNEL1x8_L2 256,32,30,0
    KERNEL1x8_E2 256,32,31,1
    blr
    MY_ALIGN


ZGEMM_1x8_L32_SUB:
/*----------------------------------------*/   
    LOAD1x8_2  
    dcbt    AO, PRE
    dcbt    BO, PRE
    KERNEL1x8_L2 256,32,0,0 
    KERNEL1x8_L2 256,32,1,0
    dcbt    AO, T2  
    KERNEL1x8_L2 256,32,2,0
    KERNEL1x8_L2 256,32,3,0 
    dcbt    AO, T3
    dcbt    BO, T2
    KERNEL1x8_L2 256,32,4,0
    KERNEL1x8_L2 256,32,5,0
    dcbt    AO, T4  
    KERNEL1x8_L2 256,32,6,0
    KERNEL1x8_L2 256,32,7,0  
    dcbt    AO, T5  
    dcbt    BO, T3
    KERNEL1x8_L2 256,32,8,0
    KERNEL1x8_L2 256,32,9,0
    KERNEL1x8_L2 256,32,10,0
    KERNEL1x8_L2 256,32,11,0  
    dcbt    BO, T4
    KERNEL1x8_L2 256,32,12,0
    KERNEL1x8_L2 256,32,13,0
    KERNEL1x8_L2 256,32,14,0
    KERNEL1x8_E2 256,32,15,1
    blr
    MY_ALIGN


ZGEMM_1x8_L16_SUB:
/*----------------------------------------*/   
    LOAD1x8_2 
    dcbt    AO, PRE
    dcbt    BO, PRE
    KERNEL1x8_L2 256,32,0,0 
    KERNEL1x8_L2 256,32,1,0
    dcbt    AO, T2  
    KERNEL1x8_L2 256,32,2,0
    KERNEL1x8_L2 256,32,3,0 
    dcbt    AO, T3
    dcbt    BO, T2
    KERNEL1x8_L2 256,32,4,0
    KERNEL1x8_L2 256,32,5,0
    dcbt    AO, T4  
    KERNEL1x8_L2 256,32,6,0
    KERNEL1x8_E2 256,32,7,1
    blr
    MY_ALIGN


ZGEMM_1x4_LMAIN_SUB:
/*----------------------------------------*/   
    mtctr   T8
    LOAD1x4_2  
    MY_ALIGN


ZGEMM_L1x4_LOOP:
/*----------------------------------------*/   
    KERNEL1x4_L2 128,32,0,0


ZGEMM_L1x4_K32:
/*----------------------------------------*/   
    KERNEL1x4_L2 128,32,1,0   
    KERNEL1x4_L2 128,32,2,0
    KERNEL1x4_L2 128,32,3,0  
    KERNEL1x4_L2 128,32,4,0
    KERNEL1x4_L2 128,32,5,0 
    KERNEL1x4_L2 128,32,6,0
    KERNEL1x4_L2 128,32,7,0
    KERNEL1x4_L2 128,32,8,0
    KERNEL1x4_L2 128,32,9,0   
    KERNEL1x4_L2 128,32,10,0
    KERNEL1x4_L2 128,32,11,0  
    KERNEL1x4_L2 128,32,12,0
    KERNEL1x4_L2 128,32,13,0 
    KERNEL1x4_L2 128,32,14,0
    KERNEL1x4_L2 128,32,15,1    
    bdnz    ZGEMM_L1x4_LOOP
    MY_ALIGN  


ZGEMM_L1x4_LOOP_END:
/*----------------------------------------*/   
    END1x4_2 
    blr
    MY_ALIGN


ZGEMM_1x4_L16_SUB:
/*----------------------------------------*/   
    LOAD1x4_2
    KERNEL1x4_L2 128,32,0,0
    KERNEL1x4_L2 128,32,1,0   
    KERNEL1x4_L2 128,32,2,0
    KERNEL1x4_L2 128,32,3,0  
    KERNEL1x4_L2 128,32,4,0
    KERNEL1x4_L2 128,32,5,0 
    KERNEL1x4_L2 128,32,6,0
    KERNEL1x4_E2 128,32,7,1
    blr
    MY_ALIGN


ZGEMM_1x4_L8_SUB:
/*----------------------------------------*/   
    LOAD1x4_2
    KERNEL1x4_L2 128,32,0,0
    KERNEL1x4_L2 128,32,1,0   
    KERNEL1x4_L2 128,32,2,0
    KERNEL1x4_E2 128,32,3,1  
    blr


ZGEMM_1x2_LMAIN_SUB:
/*----------------------------------------*/   
    mtctr   T8
    LOAD1x2_2  
    MY_ALIGN


ZGEMM_L1x2_LOOP:
/*----------------------------------------*/   
    KERNEL1x2_L2 64,32,0,0


ZGEMM_L1x2_K32:
/*----------------------------------------*/   
    KERNEL1x2_L2 64,32,1,0  
    KERNEL1x2_L2 64,32,2,0
    KERNEL1x2_L2 64,32,3,0  
    KERNEL1x2_L2 64,32,4,0
    KERNEL1x2_L2 64,32,5,0 
    KERNEL1x2_L2 64,32,6,0
    KERNEL1x2_L2 64,32,7,0
    KERNEL1x2_L2 64,32,8,0
    KERNEL1x2_L2 64,32,9,0  
    KERNEL1x2_L2 64,32,10,0
    KERNEL1x2_L2 64,32,11,0  
    KERNEL1x2_L2 64,32,12,0
    KERNEL1x2_L2 64,32,13,0 
    KERNEL1x2_L2 64,32,14,0
    KERNEL1x2_L2 64,32,15,1   
    bdnz    ZGEMM_L1x2_LOOP
    MY_ALIGN  


ZGEMM_L1x2_LOOP_END:
/*----------------------------------------*/   
    END1x2_2 
    blr
    MY_ALIGN


ZGEMM_1x2_L16_SUB:
/*----------------------------------------*/   
    LOAD1x2_2
    KERNEL1x2_L2 64,32,0,0
    KERNEL1x2_L2 64,32,1,0  
    KERNEL1x2_L2 64,32,2,0
    KERNEL1x2_L2 64,32,3,0  
    KERNEL1x2_L2 64,32,4,0
    KERNEL1x2_L2 64,32,5,0 
    KERNEL1x2_L2 64,32,6,0
    KERNEL1x2_E2 64,32,7,1
    blr
    MY_ALIGN


ZGEMM_1x2_L8_SUB:
/*----------------------------------------*/   
    LOAD1x2_2
    KERNEL1x2_L2 64,32,0,0
    KERNEL1x2_L2 64,32,1,0  
    KERNEL1x2_L2 64,32,2,0
    KERNEL1x2_E2 64,32,3,1  
    blr


ZGEMM_1x1_LMAIN_SUB:
/*----------------------------------------*/   
    mtctr   T8
    LOAD1x1_2  
    MY_ALIGN


ZGEMM_L1x1_LOOP:
/*----------------------------------------*/   
    KERNEL1x1_L2 32,32,0,0


ZGEMM_L1x1_K32:
/*----------------------------------------*/   
    KERNEL1x1_L2 32,32,1,0  
    KERNEL1x1_L2 32,32,2,0
    KERNEL1x1_L2 32,32,3,0  
    KERNEL1x1_L2 32,32,4,0
    KERNEL1x1_L2 32,32,5,0 
    KERNEL1x1_L2 32,32,6,0
    KERNEL1x1_L2 32,32,7,0
    KERNEL1x1_L2 32,32,8,0
    KERNEL1x1_L2 32,32,9,0  
    KERNEL1x1_L2 32,32,10,0
    KERNEL1x1_L2 32,32,11,0  
    KERNEL1x1_L2 32,32,12,0
    KERNEL1x1_L2 32,32,13,0 
    KERNEL1x1_L2 32,32,14,0
    KERNEL1x1_L2 32,32,15,1   
    bdnz    ZGEMM_L1x1_LOOP
    MY_ALIGN  


ZGEMM_L1x1_LOOP_END:
/*----------------------------------------*/   
    END1x1_2 
    blr
    MY_ALIGN


ZGEMM_1x1_L16_SUB:
/*----------------------------------------*/   
    LOAD1x1_2
    KERNEL1x1_L2 32,32,0,0
    KERNEL1x1_L2 32,32,1,0  
    KERNEL1x1_L2 32,32,2,0
    KERNEL1x1_L2 32,32,3,0  
    KERNEL1x1_L2 32,32,4,0
    KERNEL1x1_L2 32,32,5,0 
    KERNEL1x1_L2 32,32,6,0
    KERNEL1x1_E2 32,32,7,1
    blr
    MY_ALIGN


ZGEMM_1x1_L8_SUB:
/*----------------------------------------*/   
    LOAD1x1_2
    KERNEL1x1_L2 32,32,0,0
    KERNEL1x1_L2 32,32,1,0  
    KERNEL1x1_L2 32,32,2,0
    KERNEL1x1_E2 32,32,3,1  
    blr


/*----------------------N1 BEGINS---------*/
ZGEMM_L1:
/*----------------------------------------*/   
    andi.   T1, N,  1
    ble   ZGEMM_L1_END
		
ZGEMM_L1_BEGIN:
/*----------------------------------------*/   
    mr    CO, C
   
    add     T2,C,LDC    
    mr    AO, A  
    add   C,  C,  T1
#if defined(TRMMKERNEL) && defined(LEFT)   
    mr TEMP_REG, OFFSET  /*off = offset;*/
#endif     
    srawi.    I,  M,  3
    ble   ZGEMM_L1x8_END
    dcbt    CO,r0  /*just prefetch*/
    dcbt    T2,r0    


ZGEMM_L1x8_BEGIN:
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
    srawi.   T8, T1, 7 /**(T11-2) % 128x */
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
    ble   ZGEMM_L1x8_SUB0
    bl ZGEMM_L1x8_LMAIN_SUB
    andi.   L,  T1, 127
    ble   ZGEMM_L1x8_SAVE
    b   ZGEMM_L1x8_SUB2


ZGEMM_L1x8_SUB0:
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
    addi BO,BO,-16
    addi AO,AO,-128 
    LOAD1x8O 128,16 
    END1x8_WITHOUT_ADD   
    LOAD1x8_2O  256, 32 
    mtctr   T8    
    bl ZGEMM_L1x8_K128   
    b ZGEMM_L1x8_SAVE  
    CMP1x8_128K:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)    
    cmpwi   T6,128
#else    
    cmpwi   K,128
#endif        
    bne ZGEMM_L1x8_SUB2 
    MY_ALIGN   
    mtctr   T8
    addi BO,BO,-32
    addi AO,AO,-256   
    LOAD1x8_2O 256,32
    bl ZGEMM_L1x8_K128   
    b ZGEMM_L1x8_SAVE 
    MY_ALIGN


ZGEMM_L1x8_SUB2:
/*----------------------------------------*/   
    andi.      T1,L, 64
    ble ZGEMM_L1x8_SUB2_32
    bl ZGEMM_1x8_L64_SUB
    MY_ALIGN


ZGEMM_L1x8_SUB2_32:
/*----------------------------------------*/   
    andi.      T1,L, 32
    ble ZGEMM_L1x8_SUB2_16    
    bl ZGEMM_1x8_L32_SUB
    MY_ALIGN 


ZGEMM_L1x8_SUB2_16:
/*----------------------------------------*/   
    andi.      T1,L, 16
    ble ZGEMM_L1x8_SUB2_8
    bl ZGEMM_1x8_L16_SUB  
    MY_ALIGN    


ZGEMM_L1x8_SUB2_8:
/*----------------------------------------*/   
    andi.      T1,L, 8
    ble ZGEMM_L1x8_SUB2_4
    LOAD1x8_2
    KERNEL1x8_L2  256,32, 0,0
    KERNEL1x8_L2  256,32, 1,0
    KERNEL1x8_L2  256,32, 2,0
    KERNEL1x8_E2  256,32, 3,1
    MY_ALIGN   


ZGEMM_L1x8_SUB2_4:
/*----------------------------------------*/   
    andi.      T1,L, 4
    ble ZGEMM_L1x8_SUB2_2
    LOAD1x8_2
    KERNEL1x8_L2  256,32, 0,0
    KERNEL1x8_E2  256,32, 1,1
    MY_ALIGN


ZGEMM_L1x8_SUB2_2:
/*----------------------------------------*/   
    andi.      T1,L, 2
    ble ZGEMM_L1x8_SUB2_1
    LOAD1x8_2 
    KERNEL1x8_E2  256,32, 0,1
    MY_ALIGN    


ZGEMM_L1x8_SUB2_1:
/*----------------------------------------*/   
    andi.      T1,L, 1
    ble ZGEMM_L1x8_SAVE 
    KERNEL1x8


ZGEMM_L1x8_SAVE:
/*----------------------------------------*/   
    addic.    I,  I,  -1
    SAVE1x8
#if defined(TRMMKERNEL)    
    REFRESH_AFTER_SAVE T6,K,TEMP_REG,BO,AO,8,1
#endif     
    bgt   ZGEMM_L1x8_BEGIN
    andi.   T2, M,  7
    ble   ZGEMM_L1x1_END
    andi.   T1, M,  4
    ble   ZGEMM_L1x4_END
    b   ZGEMM_L1x4_BEGIN
    MY_ALIGN 


ZGEMM_L1x8_END:
/*----------------------------------------*/   


ZGEMM_L1x4_BEGIN:
/*----------------------------------------*/   
    andi.   T2, M,  7
    ble   ZGEMM_L1x1_END
    andi.   T1, M,  4
    ble   ZGEMM_L1x4_END
#if defined(TRMMKERNEL)   
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,4,1
#else    
    mr    BO, B   
#endif        
#if defined(TRMMKERNEL)   
    REFRESH_TEMP_BK T6,K,TEMP_REG,4,1
    mr T1, T6 
    addi T1,T1, -2 
    srawi.   T8, T1, 5 /**(T11-2) % 32x */
#else   
    mr T1, K 
    addi T1,T1, -2
    srawi.   T8, T1, 5 /**(K-2) % 32x */
#endif     
    ZERO1x4
    ble   ZGEMM_L1x4_SUB0 
    bl ZGEMM_1x4_LMAIN_SUB
    andi.   L,  T1, 31
    ble   ZGEMM_L1x4_SAVE
    b   ZGEMM_L1x4_SUB2


ZGEMM_L1x4_SUB0:
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
    addi BO,BO,-16
    addi AO,AO,-64  
    LOAD1x4O 64,16 
    END1x4_WITHOUT_ADD   
    LOAD1x4_2O  128, 32 
    mtctr   T8    
    bl ZGEMM_L1x4_K32   
    b ZGEMM_L1x4_SAVE  
    CMP1x4_32K:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)    
    cmpwi   T6,32
#else    
    cmpwi   K,32
#endif        
    bne ZGEMM_L1x4_SUB2 
    MY_ALIGN   
    mtctr   T8
    addi BO,BO,-32
    addi AO,AO,-128   
    LOAD1x4_2O 128,32
    bl ZGEMM_L1x4_K32   
    b ZGEMM_L1x4_SAVE 
    MY_ALIGN 
    MY_ALIGN 


ZGEMM_L1x4_SUB2:
/*----------------------------------------*/   
    andi.      T1,L, 16
    ble ZGEMM_L1x4_SUB2_8
    bl ZGEMM_1x4_L16_SUB  
    MY_ALIGN


ZGEMM_L1x4_SUB2_8:
/*----------------------------------------*/   
    andi.      T1,L, 8
    ble ZGEMM_L1x4_SUB2_4
    bl ZGEMM_1x4_L8_SUB
    MY_ALIGN  


ZGEMM_L1x4_SUB2_4:
/*----------------------------------------*/   
    andi.      T1,L, 4
    ble ZGEMM_L1x4_SUB2_2
    LOAD1x4_2
    KERNEL1x4_L2  128,32, 0,0
    KERNEL1x4_E2  128,32, 1,1
    MY_ALIGN


ZGEMM_L1x4_SUB2_2:
/*----------------------------------------*/   
    andi.      T1,L, 2
    ble ZGEMM_L1x4_SUB2_1
    LOAD1x4_2
    KERNEL1x4_E2  128,32, 0,1
    MY_ALIGN    


ZGEMM_L1x4_SUB2_1:
/*----------------------------------------*/   
    andi.      T1,L, 1
    ble ZGEMM_L1x4_SAVE 
    KERNEL1x4


ZGEMM_L1x4_SAVE:
/*----------------------------------------*/   
    SAVE1x4
#if defined(TRMMKERNEL)    
    REFRESH_AFTER_SAVE T6,K,TEMP_REG,BO,AO,4,1
#endif     


ZGEMM_L1x4_END:
/*----------------------------------------*/   


ZGEMM_L1x2_BEGIN:
/*----------------------------------------*/   
    andi.   T1, M,  2
    ble   ZGEMM_L1x2_END
#if defined(TRMMKERNEL)   
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,2,1
#else    
    mr    BO, B   
#endif        
#if defined(TRMMKERNEL)   
    REFRESH_TEMP_BK T6,K,TEMP_REG,2,1
    mr T1, T6 
    addi T1,T1, -2 
    srawi.   T8, T1, 5 /**(T11-2) % 32x */
#else   
    mr T1, K 
    addi T1,T1, -2
    srawi.   T8, T1, 5 /**(K-2) % 32x */
#endif     
    ZERO1x2
    ble   ZGEMM_L1x2_SUB0 
    bl ZGEMM_1x2_LMAIN_SUB
    andi.   L,  T1, 31
    ble   ZGEMM_L1x2_SAVE
    b   ZGEMM_L1x2_SUB2


ZGEMM_L1x2_SUB0:
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
    addi BO,BO,-16
    addi AO,AO,-32  
    LOAD1x2O 32,16 
    END1x2_WITHOUT_ADD   
    LOAD1x2_2O  64, 32  
    mtctr   T8    
    bl ZGEMM_L1x2_K32   
    b ZGEMM_L1x2_SAVE  
    CMP1x2_32K:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)    
    cmpwi   T6,32
#else    
    cmpwi   K,32
#endif        
    bne ZGEMM_L1x2_SUB2 
    MY_ALIGN   
    mtctr   T8
    addi BO,BO,-32
    addi AO,AO,-64   
    LOAD1x2_2O 64,32
    bl ZGEMM_L1x2_K32   
    b ZGEMM_L1x2_SAVE 
    MY_ALIGN 
    MY_ALIGN 


ZGEMM_L1x2_SUB2:
/*----------------------------------------*/   
    andi.      T1,L, 16
    ble ZGEMM_L1x2_SUB2_8
    bl ZGEMM_1x2_L16_SUB  
    MY_ALIGN


ZGEMM_L1x2_SUB2_8:
/*----------------------------------------*/   
    andi.      T1,L, 8
    ble ZGEMM_L1x2_SUB2_4
    bl ZGEMM_1x2_L8_SUB
    MY_ALIGN  


ZGEMM_L1x2_SUB2_4:
/*----------------------------------------*/   
    andi.      T1,L, 4
    ble ZGEMM_L1x2_SUB2_2
    LOAD1x2_2
    KERNEL1x2_L2  64,32, 0,0
    KERNEL1x2_E2  64,32, 1,1
    MY_ALIGN


ZGEMM_L1x2_SUB2_2:
/*----------------------------------------*/   
    andi.      T1,L, 2
    ble ZGEMM_L1x2_SUB2_1
    LOAD1x2_2
    KERNEL1x2_E2  64,32, 0,1
    MY_ALIGN    


ZGEMM_L1x2_SUB2_1:
/*----------------------------------------*/   
    andi.      T1,L, 1
    ble ZGEMM_L1x2_SAVE 
    KERNEL1x2


ZGEMM_L1x2_SAVE:
/*----------------------------------------*/   
    SAVE1x2
#if defined(TRMMKERNEL)    
    REFRESH_AFTER_SAVE T6,K,TEMP_REG,BO,AO,2,1
#endif     


ZGEMM_L1x2_END:
/*----------------------------------------*/   


ZGEMM_L1x1_BEGIN:
/*----------------------------------------*/   
    andi.   T1, M,  1
    ble   ZGEMM_L1x1_END
#if defined(TRMMKERNEL)   
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,1,1
#else    
    mr    BO, B   
#endif        
#if defined(TRMMKERNEL)   
    REFRESH_TEMP_BK T6,K,TEMP_REG,1,1
    mr T1, T6 
    addi T1,T1, -2 
    srawi.   T8, T1, 5 /**(T11-2) % 32x */
#else   
    mr T1, K 
    addi T1,T1, -2
    srawi.   T8, T1, 5 /**(K-2) % 32x */
#endif     
    ZERO1x1
    ble   ZGEMM_L1x1_SUB0 
    bl ZGEMM_1x1_LMAIN_SUB
    andi.   L,  T1, 31
    ble   ZGEMM_L1x1_SAVE
    b   ZGEMM_L1x1_SUB2


ZGEMM_L1x1_SUB0:
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
    addi BO,BO,-16
    addi AO,AO,-16  
    LOAD1x1O 16,16 
    END1x1_WITHOUT_ADD   
    LOAD1x1_2O  32, 32  
    mtctr   T8    
    bl ZGEMM_L1x1_K32   
    b ZGEMM_L1x1_SAVE  
    CMP1x1_32K:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)    
    cmpwi   T6,32
#else    
    cmpwi   K,32
#endif        
    bne ZGEMM_L1x1_SUB2 
    MY_ALIGN   
    mtctr   T8
    addi BO,BO,-32
    addi AO,AO,-32   
    LOAD1x1_2O 32,32
    bl ZGEMM_L1x1_K32   
    b ZGEMM_L1x1_SAVE 
    MY_ALIGN 
    MY_ALIGN 


ZGEMM_L1x1_SUB2:
/*----------------------------------------*/   
    andi.      T1,L, 16
    ble ZGEMM_L1x1_SUB2_8
    bl ZGEMM_1x1_L16_SUB  
    MY_ALIGN


ZGEMM_L1x1_SUB2_8:
/*----------------------------------------*/   
    andi.      T1,L, 8
    ble ZGEMM_L1x1_SUB2_4
    bl ZGEMM_1x1_L8_SUB
    MY_ALIGN  


ZGEMM_L1x1_SUB2_4:
/*----------------------------------------*/   
    andi.      T1,L, 4
    ble ZGEMM_L1x1_SUB2_2
    LOAD1x1_2
    KERNEL1x1_L2  32,32, 0,0
    KERNEL1x1_E2  32,32, 1,1
    MY_ALIGN


ZGEMM_L1x1_SUB2_2:
/*----------------------------------------*/   
    andi.      T1,L, 2
    ble ZGEMM_L1x1_SUB2_1
    LOAD1x1_2
    KERNEL1x1_E2  32,32, 0,1
    MY_ALIGN    


ZGEMM_L1x1_SUB2_1:
/*----------------------------------------*/   
    andi.      T1,L, 1
    ble ZGEMM_L1x1_SAVE 
    KERNEL1x1


ZGEMM_L1x1_SAVE:
/*----------------------------------------*/   
    SAVE1x1
#if defined(TRMMKERNEL)    
    REFRESH_AFTER_SAVE T6,K,TEMP_REG,BO,AO,1,1
#endif   


ZGEMM_L1x1_END:
/*----------------------------------------*/   
#if defined(TRMMKERNEL) && !defined(LEFT)   
    addi TEMP_REG, TEMP_REG, 1
#endif   


ZGEMM_L1_END:
/*----------------------------------------*/   
    