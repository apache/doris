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
b ZGEMM_L2
/*                MINI SUBROUTINES                            */      
/*                2x8 MAIN 128x+2 LOOP                     */      


ZGEMM_L2x8_LMAIN_SUB:
/*----------------------------------------*/   
    mtctr   T8
    MY_ALIGN
ZGEMM_L2x8_LOOP:
/*----------------------------------------*/   
    dcbt    AO, PRE
    dcbt    BO, PRE
    KERNEL2x8_2 0, 0
ZGEMM_L2x8_K128:
/*----------------------------------------*/   
    KERNEL2x8_2 1, 0
    dcbt    AO, T2  
    KERNEL2x8_2 2, 0
    KERNEL2x8_2 3, 0
    dcbt    AO, T3
    dcbt    BO, T2
    KERNEL2x8_2 4, 0
    KERNEL2x8_2 5, 0
    dcbt    AO, T4  
    KERNEL2x8_2 6, 0
    KERNEL2x8_2 7, 0
    dcbt    AO, T5  
    dcbt    BO, T3
    KERNEL2x8_2 8, 0
    KERNEL2x8_2 9, 0
    KERNEL2x8_2 10, 0
    KERNEL2x8_2 11, 0
    dcbt    BO, T4
    KERNEL2x8_2 12, 0
    KERNEL2x8_2 13, 0
    KERNEL2x8_2 14, 0
    KERNEL2x8_2 15, 0
    KERNEL2x8_2 16, 0
    KERNEL2x8_2 17, 0
    KERNEL2x8_2 18, 0
    KERNEL2x8_2 19, 0
    KERNEL2x8_2 20, 0
    KERNEL2x8_2 21, 0
    KERNEL2x8_2 22, 0
    KERNEL2x8_2 23, 0
    KERNEL2x8_2 24, 0
    KERNEL2x8_2 25, 0
    KERNEL2x8_2 26, 0
    KERNEL2x8_2 27, 0
    KERNEL2x8_2 28, 0
    KERNEL2x8_2 29, 0
    KERNEL2x8_2 30, 0
    KERNEL2x8_2 31, 0
    KERNEL2x8_2 32, 0
    KERNEL2x8_2 33, 0
    KERNEL2x8_2 34, 0
    KERNEL2x8_2 35, 0
    KERNEL2x8_2 36, 0
    KERNEL2x8_2 37, 0
    KERNEL2x8_2 38, 0
    KERNEL2x8_2 39, 0
    KERNEL2x8_2 40, 0
    KERNEL2x8_2 41, 0
    KERNEL2x8_2 42, 0
    KERNEL2x8_2 43, 0
    KERNEL2x8_2 44, 0
    KERNEL2x8_2 45, 0
    KERNEL2x8_2 46, 0
    KERNEL2x8_2 47, 0
    KERNEL2x8_2 48, 0
    KERNEL2x8_2 49, 0
    KERNEL2x8_2 50, 0
    KERNEL2x8_2 51, 0
    KERNEL2x8_2 52, 0
    KERNEL2x8_2 53, 0
    KERNEL2x8_2 54, 0
    KERNEL2x8_2 55, 0
    KERNEL2x8_2 56, 0
    KERNEL2x8_2 57, 0
    KERNEL2x8_2 58, 0
    KERNEL2x8_2 59, 0
    KERNEL2x8_2 60, 0
    KERNEL2x8_2 61, 0
    KERNEL2x8_2 62, 0
    KERNEL2x8_2 63, 1
    bdz     ZGEMM_L2x8_LOOP_END
    b       ZGEMM_L2x8_LOOP
    MY_ALIGN  

ZGEMM_L2x8_LOOP_END:
/*----------------------------------------*/   
    KERNEL2x8_2 0, 1
    blr
    MY_ALIGN


ZGEMM_2x4_LMAIN_SUB:
/*----------------------------------------*/   
    mtctr   T8
    MY_ALIGN
ZGEMM_L2x4_LOOP:
/*----------------------------------------*/   
    KERNEL2x4_2 0, 0
ZGEMM_L2x4_K32:
/*----------------------------------------*/   
    KERNEL2x4_2 1, 0
    KERNEL2x4_2 2, 0
    KERNEL2x4_2 3, 0
    KERNEL2x4_2 4, 0
    KERNEL2x4_2 5, 0
    KERNEL2x4_2 6, 0
    KERNEL2x4_2 7, 0
    KERNEL2x4_2 8, 0
    KERNEL2x4_2 9, 0
    KERNEL2x4_2 10, 0
    KERNEL2x4_2 11, 0
    KERNEL2x4_2 12, 0
    KERNEL2x4_2 13, 0
    KERNEL2x4_2 14, 0
    KERNEL2x4_2 15, 1
    bdnz    ZGEMM_L2x4_LOOP
    MY_ALIGN  
ZGEMM_L2x4_LOOP_END:
/*----------------------------------------*/   
    KERNEL2x4_2 0, 1
    blr
    MY_ALIGN


ZGEMM_2x2_LMAIN_SUB:
/*----------------------------------------*/   
    mtctr   T8
    MY_ALIGN 
ZGEMM_L2x2_LOOP:
/*----------------------------------------*/   
    KERNEL2x2_2 0, 0 
ZGEMM_L2x2_K32:
/*----------------------------------------*/   
    KERNEL2x2_2 1, 0  
    KERNEL2x2_2 2, 0
    KERNEL2x2_2 3, 0  
    KERNEL2x2_2 4, 0
    KERNEL2x2_2 5, 0 
    KERNEL2x2_2 6, 0
    KERNEL2x2_2 7, 0
    KERNEL2x2_2 8, 0
    KERNEL2x2_2 9, 0  
    KERNEL2x2_2 10, 0
    KERNEL2x2_2 11, 0  
    KERNEL2x2_2 12, 0
    KERNEL2x2_2 13, 0 
    KERNEL2x2_2 14, 0
    KERNEL2x2_2 15, 1   
    bdnz    ZGEMM_L2x2_LOOP
    MY_ALIGN  


ZGEMM_L2x2_LOOP_END:
/*----------------------------------------*/   
    KERNEL2x2_2 0, 1
    blr
    MY_ALIGN

ZGEMM_2x1_LMAIN_SUB:
/*----------------------------------------*/   
    mtctr   T8
    LOAD2x1_2  
    MY_ALIGN
ZGEMM_L2x1_LOOP:
/*----------------------------------------*/   
    KERNEL2x1_L2 32, 64, 0, 0 
ZGEMM_L2x1_K32:
/*----------------------------------------*/   
    KERNEL2x1_L2 32, 64, 1, 0  
    KERNEL2x1_L2 32, 64, 2, 0
    KERNEL2x1_L2 32, 64, 3, 0  
    KERNEL2x1_L2 32, 64, 4, 0
    KERNEL2x1_L2 32, 64, 5, 0 
    KERNEL2x1_L2 32, 64, 6, 0
    KERNEL2x1_L2 32, 64, 7, 0
    KERNEL2x1_L2 32, 64, 8, 0
    KERNEL2x1_L2 32, 64, 9, 0  
    KERNEL2x1_L2 32, 64, 10, 0
    KERNEL2x1_L2 32, 64, 11, 0  
    KERNEL2x1_L2 32, 64, 12, 0
    KERNEL2x1_L2 32, 64, 13, 0 
    KERNEL2x1_L2 32, 64, 14, 0
    KERNEL2x1_L2 32, 64, 15, 1   
    bdnz    ZGEMM_L2x1_LOOP
    MY_ALIGN  
ZGEMM_L2x1_LOOP_END:
/*----------------------------------------*/   
    END2x1_2 
    blr

    MY_ALIGN


/*             MAIN LOOP BEGINS               */   
    MY_ALIGN


ZGEMM_L2:
/*----------------------------------------*/   
#if defined(TRMMKERNEL) && !defined(LEFT)   
    neg TEMP_REG, OFFSET 
#endif   
    srawi.    J, N, 1
    bgt   ZGEMM_L2_BEGIN
    b     ZGEMM_L2_END

ZGEMM_L2_BEGIN:
/*----------------------------------------*/   
    mr    CO, C
    slwi    T1, LDC, 1     
    add     T2,C,LDC    
    mr    AO, A  
    add   C, C, T1
#if defined(TRMMKERNEL) && defined(LEFT)   
    mr TEMP_REG, OFFSET  /*off = offset;*/
#endif     
    srawi.    I, M, 3
    bgt   ZGEMM_L2_BEGIN_CONTINUE
    b     ZGEMM_L2x8_END

ZGEMM_L2_BEGIN_CONTINUE:
    dcbt    CO,r0  /*just prefetch*/
    dcbt    T2,r0


ZGEMM_L2x8_BEGIN:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)   
    REFRESH_POINTERS  AO, BO,TEMP_REG, B, 8, 2
#else    
    mr    BO, B  
    dcbt    B, r0  
#endif     
    dcbt    AO, r0
#if defined(TRMMKERNEL)   
    REFRESH_TEMP_BK T6,K,TEMP_REG, 8, 2
    mr T1, T6
#else   
    mr T1, K
#endif   
/* TEMPS FOR PREFETCH */   
    li T2, 1024
    li T3, 1024+512
    addi T1,T1, -2
/* TEMPS FOR PREFETCH */     
    li T4, 2048
    li T5, 2048+512 
    srawi.   T8, T1, 7 /* T8 <- T1 % 128 */

    KERNEL2x8_PRELOAD
    KERNEL2x8_ZERO_AND_PRIME_MMA
    ble   ZGEMM_L2x8_SUB0
    bl ZGEMM_L2x8_LMAIN_SUB
    andi.   L, T1, 127

    bgt   ZGEMM_L2x8_BEGIN_CONTINUE
    b     ZGEMM_L2x8_SAVE

ZGEMM_L2x8_BEGIN_CONTINUE:
    b   ZGEMM_L2x8_SUB2


ZGEMM_L2x8_SUB0:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)   
    andi.   L, T6, 255
    cmpwi   T6, 129
#else   
    andi.   L, K, 255
    cmpwi   K, 129
#endif       
    li T8, 1
    bne CMP2x8_128K
    LOAD_END_2x8 128, 32
    KERNEL2x8_PRELOAD
    addi BO, BO, -64
    addi AO,AO, -256   
    mtctr   T8    
    bl ZGEMM_L2x8_K128
    b ZGEMM_L2x8_SAVE  

CMP2x8_128K:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)    
    cmpwi   T6, 128
#else    
    cmpwi   K, 128
#endif        
    bne ZGEMM_L2x8_SUB2 
    MY_ALIGN   
    mtctr   T8
    addi BO, BO, -64
    addi AO,AO, -256   
    bl ZGEMM_L2x8_K128
    b ZGEMM_L2x8_SAVE 
    MY_ALIGN


ZGEMM_L2x8_SUB2:
/*----------------------------------------*/   
    andi.      T1,L, 64
    ble ZGEMM_L2x8_SUB2_32
    dcbt    AO, PRE
    dcbt    BO, PRE
    KERNEL2x8_2 0, 0
    KERNEL2x8_2 1, 0
    dcbt    AO, T2  
    KERNEL2x8_2 2, 0
    KERNEL2x8_2 3, 0
    dcbt    AO, T3
    dcbt    BO, T2
    KERNEL2x8_2 4, 0
    KERNEL2x8_2 5, 0
    dcbt    AO, T4  
    KERNEL2x8_2 6, 0
    KERNEL2x8_2 7, 0
    dcbt    AO, T5  
    dcbt    BO, T3
    KERNEL2x8_2 8, 0
    KERNEL2x8_2 9, 0
    KERNEL2x8_2 10, 0
    KERNEL2x8_2 11, 0
    dcbt    BO, T4
    KERNEL2x8_2 12, 0
    KERNEL2x8_2 13, 0
    KERNEL2x8_2 14, 0
    KERNEL2x8_2 15, 0
    KERNEL2x8_2 16, 0
    KERNEL2x8_2 17, 0
    KERNEL2x8_2 18, 0
    KERNEL2x8_2 19, 0
    KERNEL2x8_2 20, 0
    KERNEL2x8_2 21, 0
    KERNEL2x8_2 22, 0
    KERNEL2x8_2 23, 0
    KERNEL2x8_2 24, 0
    KERNEL2x8_2 25, 0
    KERNEL2x8_2 26, 0
    KERNEL2x8_2 27, 0
    KERNEL2x8_2 28, 0
    KERNEL2x8_2 29, 0
    KERNEL2x8_2 30, 0
    KERNEL2x8_2 31, 1
    MY_ALIGN


ZGEMM_L2x8_SUB2_32:
/*----------------------------------------*/   
    andi.      T1,L, 32
    ble ZGEMM_L2x8_SUB2_16    
    dcbt    AO, PRE
    dcbt    BO, PRE
    KERNEL2x8_2 0, 0
    KERNEL2x8_2 1, 0
    dcbt    AO, T2  
    KERNEL2x8_2 2, 0
    KERNEL2x8_2 3, 0
    dcbt    AO, T3
    dcbt    BO, T2
    KERNEL2x8_2 4, 0
    KERNEL2x8_2 5, 0
    dcbt    AO, T4  
    KERNEL2x8_2 6, 0
    KERNEL2x8_2 7, 0
    dcbt    AO, T5  
    dcbt    BO, T3
    KERNEL2x8_2 8, 0
    KERNEL2x8_2 9, 0
    KERNEL2x8_2 10, 0
    KERNEL2x8_2 11, 0
    dcbt    BO, T4
    KERNEL2x8_2 12, 0
    KERNEL2x8_2 13, 0
    KERNEL2x8_2 14, 0
    KERNEL2x8_2 15, 1
    MY_ALIGN 


ZGEMM_L2x8_SUB2_16:
/*----------------------------------------*/   
    andi.      T1,L, 16
    ble ZGEMM_L2x8_SUB2_8
    dcbt    AO, PRE
    dcbt    BO, PRE
    KERNEL2x8_2 0, 0
    KERNEL2x8_2 1, 0
    dcbt    AO, T2  
    KERNEL2x8_2 2, 0
    KERNEL2x8_2 3, 0
    dcbt    AO, T3
    dcbt    BO, T2
    KERNEL2x8_2 4, 0
    KERNEL2x8_2 5, 0
    dcbt    AO, T4  
    KERNEL2x8_2 6, 0
    KERNEL2x8_2 7, 1
    MY_ALIGN    


ZGEMM_L2x8_SUB2_8:
/*----------------------------------------*/   
    andi.      T1,L, 8
    ble ZGEMM_L2x8_SUB2_4
    KERNEL2x8_2 0, 0
    KERNEL2x8_2 1, 0
    KERNEL2x8_2 2, 0
    KERNEL2x8_2 3, 1
    MY_ALIGN   


ZGEMM_L2x8_SUB2_4:
/*----------------------------------------*/   
    andi.      T1,L, 4
    ble ZGEMM_L2x8_SUB2_2
    KERNEL2x8_2 0, 0
    KERNEL2x8_2 1, 1
    MY_ALIGN


ZGEMM_L2x8_SUB2_2:
/*----------------------------------------*/   
    andi.      T1,L, 2
    ble ZGEMM_L2x8_SUB2_1
    KERNEL2x8_2 0, 1
    MY_ALIGN    


ZGEMM_L2x8_SUB2_1:
/*----------------------------------------*/   
    andi.      T1,L, 1
    ble ZGEMM_L2x8_SAVE 
    LOAD_END_2x8 128, 32


ZGEMM_L2x8_SAVE:
/*----------------------------------------*/   
    addic.    I, I, -1
    KERNEL2x8_UNPRIME_MMA
    SAVE2x8
#if defined(TRMMKERNEL)    
    REFRESH_AFTER_SAVE T6,K,TEMP_REG, BO,AO, 8, 2
#endif     

    ble   ZGEMM_L2x8_SAVE_CONTINUE
    b     ZGEMM_L2x8_BEGIN

ZGEMM_L2x8_SAVE_CONTINUE:
    andi.   T2, M, 7
    ble   ZGEMM_L2x1_END
    andi.   T1, M, 4
    ble   ZGEMM_L2x4_END
    b   ZGEMM_L2x4_BEGIN
    MY_ALIGN 


ZGEMM_L2x8_END:
/*----------------------------------------*/   


ZGEMM_L2x4_BEGIN:
/*----------------------------------------*/   
    andi.   T2, M, 7
    ble   ZGEMM_L2x1_END
    andi.   T1, M, 4
    ble   ZGEMM_L2x4_END
#if defined(TRMMKERNEL)   
    REFRESH_POINTERS  AO, BO,TEMP_REG, B, 4, 2
#else    
    mr    BO, B   
#endif        
#if defined(TRMMKERNEL)   
    REFRESH_TEMP_BK T6,K,TEMP_REG, 4, 2
    mr T1, T6 
    addi T1,T1, -2 
    srawi.   T8, T1, 5 /**(T11-2) % 32x */
#else   
    mr T1, K 
    addi T1,T1, -2
    srawi.   T8, T1, 5 /**(K-2) % 32x */
#endif     
    KERNEL2x4_PRELOAD
    KERNEL2x4_ZERO_AND_PRIME_MMA
    ble   ZGEMM_L2x4_SUB0 
    bl ZGEMM_2x4_LMAIN_SUB
    andi.   L, T1, 31
    ble   ZGEMM_L2x4_SAVE
    b    ZGEMM_L2x4_SUB2


ZGEMM_L2x4_SUB0:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)   
    andi.   L, T6, 63
    cmpwi   T6, 33
#else   
    andi.   L, K, 63
    cmpwi   K, 33
#endif       
    li T8, 1
    bne CMP2x4_32K
    LOAD_END_2x4 64, 32
    KERNEL2x4_PRELOAD
    addi BO, BO, -64
    addi AO,AO, -128
    mtctr   T8    
    bl ZGEMM_L2x4_K32   
    b ZGEMM_L2x4_SAVE  
    CMP2x4_32K:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)    
    cmpwi   T6, 32
#else    
    cmpwi   K, 32
#endif        
    bne ZGEMM_L2x4_SUB2 
    MY_ALIGN   
    mtctr   T8
    addi BO, BO, -64
    addi AO,AO, -128
    bl ZGEMM_L2x4_K32   
    b ZGEMM_L2x4_SAVE 
    MY_ALIGN 
    MY_ALIGN 


ZGEMM_L2x4_SUB2:
/*----------------------------------------*/   
    andi.      T1,L, 16
    ble ZGEMM_L2x4_SUB2_8
    KERNEL2x4_2 0, 0
    KERNEL2x4_2 1, 0
    KERNEL2x4_2 2, 0
    KERNEL2x4_2 3, 0
    KERNEL2x4_2 4, 0
    KERNEL2x4_2 5, 0
    KERNEL2x4_2 6, 0
    KERNEL2x4_2 7, 1
    MY_ALIGN


ZGEMM_L2x4_SUB2_8:
/*----------------------------------------*/   
    andi.      T1,L, 8
    ble ZGEMM_L2x4_SUB2_4
    KERNEL2x4_2 0, 0
    KERNEL2x4_2 1, 0
    KERNEL2x4_2 2, 0
    KERNEL2x4_2 3, 1
    MY_ALIGN  


ZGEMM_L2x4_SUB2_4:
/*----------------------------------------*/   
    andi.      T1,L, 4
    ble ZGEMM_L2x4_SUB2_2
    KERNEL2x4_2 0, 0
    KERNEL2x4_2 1, 1
    MY_ALIGN


ZGEMM_L2x4_SUB2_2:
/*----------------------------------------*/   
    andi.      T1,L, 2
    ble ZGEMM_L2x4_SUB2_1
    KERNEL2x4_2 0, 1
    MY_ALIGN    


ZGEMM_L2x4_SUB2_1:
/*----------------------------------------*/   
    andi.      T1,L, 1
    ble ZGEMM_L2x4_SAVE 
    LOAD_END_2x4 64, 32


ZGEMM_L2x4_SAVE:
/*----------------------------------------*/   
    KERNEL2x4_UNPRIME_MMA
    SAVE2x4
#if defined(TRMMKERNEL)    
    REFRESH_AFTER_SAVE T6,K,TEMP_REG, BO,AO, 4, 2
#endif     


ZGEMM_L2x4_END:
/*----------------------------------------*/   


ZGEMM_L2x2_BEGIN:
/*----------------------------------------*/   
    andi.   T1, M, 2
    ble   ZGEMM_L2x2_END
#if defined(TRMMKERNEL)   
    REFRESH_POINTERS  AO, BO,TEMP_REG, B, 2, 2
#else    
    mr    BO, B   
#endif        
#if defined(TRMMKERNEL)   
    REFRESH_TEMP_BK T6,K,TEMP_REG, 2, 2
    mr T1, T6 
    addi T1,T1, -2 
    srawi.   T8, T1, 5 /**(T11-2) % 32x */
#else   
    mr T1, K 
    addi T1,T1, -2
    srawi.   T8, T1, 5 /**(K-2) % 32x */
#endif     
    KERNEL2x2_PRELOAD
    KERNEL2x2_ZERO_AND_PRIME_MMA
    ble   ZGEMM_L2x2_SUB0 
    bl ZGEMM_2x2_LMAIN_SUB
    andi.   L, T1, 31
    ble   ZGEMM_L2x2_SAVE
    b   ZGEMM_L2x2_SUB2


ZGEMM_L2x2_SUB0:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)   
    andi.   L, T6, 63
    cmpwi   T6, 33
#else   
    andi.   L, K, 63
    cmpwi   K, 33
#endif       
    li T8, 1
    bne CMP2x2_32K
    LOAD_END_2x2 32, 32
    KERNEL2x2_PRELOAD
    addi BO, BO, -64
    addi AO,AO, -64
    mtctr   T8    
    bl ZGEMM_L2x2_K32   
    b ZGEMM_L2x2_SAVE  
    CMP2x2_32K:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)    
    cmpwi   T6, 32
#else    
    cmpwi   K, 32
#endif        
    bne ZGEMM_L2x2_SUB2 
    MY_ALIGN   
    mtctr   T8
    addi BO, BO, -64
    addi AO,AO, -64
    bl ZGEMM_L2x2_K32   
    b ZGEMM_L2x2_SAVE 
    MY_ALIGN 
    MY_ALIGN 


ZGEMM_L2x2_SUB2:
/*----------------------------------------*/   
    andi.      T1,L, 16
    ble ZGEMM_L2x2_SUB2_8
    KERNEL2x2_2 0, 0
    KERNEL2x2_2 1, 0  
    KERNEL2x2_2 2, 0
    KERNEL2x2_2 3, 0  
    KERNEL2x2_2 4, 0
    KERNEL2x2_2 5, 0 
    KERNEL2x2_2 6, 0
    KERNEL2x2_2 7, 1
    MY_ALIGN


ZGEMM_L2x2_SUB2_8:
/*----------------------------------------*/   
    andi.      T1,L, 8
    ble ZGEMM_L2x2_SUB2_4
    KERNEL2x2_2 0, 0
    KERNEL2x2_2 1, 0  
    KERNEL2x2_2 2, 0
    KERNEL2x2_2 3, 1  
    MY_ALIGN  


ZGEMM_L2x2_SUB2_4:
/*----------------------------------------*/   
    andi.      T1,L, 4
    ble ZGEMM_L2x2_SUB2_2
    KERNEL2x2_2 0, 0
    KERNEL2x2_2 1, 1
    MY_ALIGN


ZGEMM_L2x2_SUB2_2:
/*----------------------------------------*/   
    andi.      T1,L, 2
    ble ZGEMM_L2x2_SUB2_1
    KERNEL2x2_2 0, 1
    MY_ALIGN    


ZGEMM_L2x2_SUB2_1:
/*----------------------------------------*/   
    andi.      T1,L, 1
    ble ZGEMM_L2x2_SAVE 
    LOAD_END_2x2 32, 32


ZGEMM_L2x2_SAVE:
/*----------------------------------------*/   
    KERNEL2x2_UNPRIME_MMA
    SAVE2x2
#if defined(TRMMKERNEL)    
    REFRESH_AFTER_SAVE T6,K,TEMP_REG, BO,AO, 2, 2
#endif     


ZGEMM_L2x2_END:
/*----------------------------------------*/   


ZGEMM_L2x1_BEGIN:
/*----------------------------------------*/   
    andi.   T1, M, 1
    ble   ZGEMM_L2x1_END
#if defined(TRMMKERNEL)   
    REFRESH_POINTERS  AO, BO,TEMP_REG, B, 1, 2
#else    
    mr    BO, B   
#endif        
#if defined(TRMMKERNEL)   
    REFRESH_TEMP_BK T6,K,TEMP_REG, 1, 2
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
    andi.   L, T1, 31
    ble   ZGEMM_L2x1_SAVE
    b   ZGEMM_L2x1_SUB2


ZGEMM_L2x1_SUB0:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)   
    andi.   L, T6, 63
    cmpwi   T6, 33
#else   
    andi.   L, K, 63
    cmpwi   K, 33
#endif       
    li T8, 1
    bne CMP2x1_32K
    addi BO, BO, -32
    addi AO,AO, -16  
    LOAD2x1O 16, 32 
    END2x1_WITHOUT_ADD   
    LOAD2x1_2O  32, 64  
    mtctr   T8    
    bl ZGEMM_L2x1_K32   
    b ZGEMM_L2x1_SAVE  
    CMP2x1_32K:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)    
    cmpwi   T6, 32
#else    
    cmpwi   K, 32
#endif        
    bne ZGEMM_L2x1_SUB2 
    MY_ALIGN   
    mtctr   T8
    addi BO, BO, -64
    addi AO,AO, -32   
    LOAD2x1_2O 32, 64
    bl ZGEMM_L2x1_K32   
    b ZGEMM_L2x1_SAVE 
    MY_ALIGN 
    MY_ALIGN 


ZGEMM_L2x1_SUB2:
/*----------------------------------------*/   
    andi.      T1,L, 16
    ble ZGEMM_L2x1_SUB2_8
    LOAD2x1_2
    KERNEL2x1_L2 32, 64, 0, 0
    KERNEL2x1_L2 32, 64, 1, 0  
    KERNEL2x1_L2 32, 64, 2, 0
    KERNEL2x1_L2 32, 64, 3, 0  
    KERNEL2x1_L2 32, 64, 4, 0
    KERNEL2x1_L2 32, 64, 5, 0 
    KERNEL2x1_L2 32, 64, 6, 0
    KERNEL2x1_E2 32, 64, 7, 1
    MY_ALIGN


ZGEMM_L2x1_SUB2_8:
/*----------------------------------------*/   
    andi.      T1,L, 8
    ble ZGEMM_L2x1_SUB2_4
    LOAD2x1_2
    KERNEL2x1_L2 32, 64, 0, 0
    KERNEL2x1_L2 32, 64, 1, 0  
    KERNEL2x1_L2 32, 64, 2, 0
    KERNEL2x1_E2 32, 64, 3, 1  
    MY_ALIGN  


ZGEMM_L2x1_SUB2_4:
/*----------------------------------------*/   
    andi.      T1,L, 4
    ble ZGEMM_L2x1_SUB2_2
    LOAD2x1_2
    KERNEL2x1_L2  32, 64, 0, 0
    KERNEL2x1_E2  32, 64, 1, 1
    MY_ALIGN


ZGEMM_L2x1_SUB2_2:
/*----------------------------------------*/   
    andi.      T1,L, 2
    ble ZGEMM_L2x1_SUB2_1
    LOAD2x1_2
    KERNEL2x1_E2  32, 64, 0, 1
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
    REFRESH_AFTER_SAVE T6,K,TEMP_REG, BO,AO, 1, 2
#endif   


ZGEMM_L2x1_END:
/*----------------------------------------*/   
    slwi    T1, K, 5
    addic.    J, J, -1
    add   B, B, T1
#if defined(TRMMKERNEL) && !defined(LEFT)   
    addi TEMP_REG, TEMP_REG, 2 
#endif   
    ble   ZGEMM_L2_END 
    b     ZGEMM_L2_BEGIN

ZGEMM_L2_END:

b ZGEMM_L1
/*                MINI SUBROUTINES                            */      
/*                1x8 MAIN 128x+2 LOOP                     */      


ZGEMM_L1x8_LMAIN_SUB:
/*----------------------------------------*/   
    mtctr   T8
    MY_ALIGN
ZGEMM_L1x8_LOOP:
/*----------------------------------------*/   
    dcbt    AO, PRE
    dcbt    BO, PRE
    KERNEL1x8_2 0, 0
ZGEMM_L1x8_K128:
/*----------------------------------------*/   
    KERNEL1x8_2 1, 0
    dcbt    AO, T2  
    KERNEL1x8_2 2, 0
    KERNEL1x8_2 3, 0
    dcbt    AO, T3
    dcbt    BO, T2
    KERNEL1x8_2 4, 0
    KERNEL1x8_2 5, 0
    dcbt    AO, T4  
    KERNEL1x8_2 6, 0
    KERNEL1x8_2 7, 0
    dcbt    AO, T5  
    dcbt    BO, T3
    KERNEL1x8_2 8, 0
    KERNEL1x8_2 9, 0
    KERNEL1x8_2 10, 0
    KERNEL1x8_2 11, 0
    dcbt    BO, T4
    KERNEL1x8_2 12, 0
    KERNEL1x8_2 13, 0
    KERNEL1x8_2 14, 0
    KERNEL1x8_2 15, 0
    KERNEL1x8_2 16, 0
    KERNEL1x8_2 17, 0
    KERNEL1x8_2 18, 0
    KERNEL1x8_2 19, 0
    KERNEL1x8_2 20, 0
    KERNEL1x8_2 21, 0
    KERNEL1x8_2 22, 0
    KERNEL1x8_2 23, 0
    KERNEL1x8_2 24, 0
    KERNEL1x8_2 25, 0
    KERNEL1x8_2 26, 0
    KERNEL1x8_2 27, 0
    KERNEL1x8_2 28, 0
    KERNEL1x8_2 29, 0
    KERNEL1x8_2 30, 0
    KERNEL1x8_2 31, 0
    KERNEL1x8_2 32, 0
    KERNEL1x8_2 33, 0
    KERNEL1x8_2 34, 0
    KERNEL1x8_2 35, 0
    KERNEL1x8_2 36, 0
    KERNEL1x8_2 37, 0
    KERNEL1x8_2 38, 0
    KERNEL1x8_2 39, 0
    KERNEL1x8_2 40, 0
    KERNEL1x8_2 41, 0
    KERNEL1x8_2 42, 0
    KERNEL1x8_2 43, 0
    KERNEL1x8_2 44, 0
    KERNEL1x8_2 45, 0
    KERNEL1x8_2 46, 0
    KERNEL1x8_2 47, 0
    KERNEL1x8_2 48, 0
    KERNEL1x8_2 49, 0
    KERNEL1x8_2 50, 0
    KERNEL1x8_2 51, 0
    KERNEL1x8_2 52, 0
    KERNEL1x8_2 53, 0
    KERNEL1x8_2 54, 0
    KERNEL1x8_2 55, 0
    KERNEL1x8_2 56, 0
    KERNEL1x8_2 57, 0
    KERNEL1x8_2 58, 0
    KERNEL1x8_2 59, 0
    KERNEL1x8_2 60, 0
    KERNEL1x8_2 61, 0
    KERNEL1x8_2 62, 0
    KERNEL1x8_2 63, 1
    bdnz    ZGEMM_L1x8_LOOP
    MY_ALIGN  
ZGEMM_L1x8_LOOP_END:
/*----------------------------------------*/   
    KERNEL1x8_2 0, 1
    blr
    MY_ALIGN


ZGEMM_1x4_LMAIN_SUB:
/*----------------------------------------*/   
    mtctr   T8
    MY_ALIGN


ZGEMM_L1x4_LOOP:
/*----------------------------------------*/   
    KERNEL1x4_2 0, 0


ZGEMM_L1x4_K32:
/*----------------------------------------*/   
    KERNEL1x4_2 1, 0
    KERNEL1x4_2 2, 0
    KERNEL1x4_2 3, 0
    KERNEL1x4_2 4, 0
    KERNEL1x4_2 5, 0
    KERNEL1x4_2 6, 0
    KERNEL1x4_2 7, 0
    KERNEL1x4_2 8, 0
    KERNEL1x4_2 9, 0
    KERNEL1x4_2 10, 0
    KERNEL1x4_2 11, 0
    KERNEL1x4_2 12, 0
    KERNEL1x4_2 13, 0
    KERNEL1x4_2 14, 0
    KERNEL1x4_2 15, 1
    bdnz    ZGEMM_L1x4_LOOP
    MY_ALIGN  


ZGEMM_L1x4_LOOP_END:
/*----------------------------------------*/   
    KERNEL1x4_2 0, 1
    blr
    MY_ALIGN


ZGEMM_1x2_LMAIN_SUB:
/*----------------------------------------*/   
    mtctr   T8
    MY_ALIGN


ZGEMM_L1x2_LOOP:
/*----------------------------------------*/   
    KERNEL1x2_2 0, 0


ZGEMM_L1x2_K32:
/*----------------------------------------*/   
    KERNEL1x2_2 1, 0
    KERNEL1x2_2 2, 0
    KERNEL1x2_2 3, 0
    KERNEL1x2_2 4, 0
    KERNEL1x2_2 5, 0
    KERNEL1x2_2 6, 0
    KERNEL1x2_2 7, 0
    KERNEL1x2_2 8, 0
    KERNEL1x2_2 9, 0
    KERNEL1x2_2 10, 0
    KERNEL1x2_2 11, 0
    KERNEL1x2_2 12, 0
    KERNEL1x2_2 13, 0
    KERNEL1x2_2 14, 0
    KERNEL1x2_2 15, 1
    bdnz    ZGEMM_L1x2_LOOP
    MY_ALIGN  


ZGEMM_L1x2_LOOP_END:
/*----------------------------------------*/   
    KERNEL1x2_2 0, 1
    blr
    MY_ALIGN


ZGEMM_1x1_LMAIN_SUB:
/*----------------------------------------*/   
    mtctr   T8
    LOAD1x1_2  
    MY_ALIGN


ZGEMM_L1x1_LOOP:
/*----------------------------------------*/   
    KERNEL1x1_L2 32, 32, 0, 0


ZGEMM_L1x1_K32:
/*----------------------------------------*/   
    KERNEL1x1_L2 32, 32, 1, 0  
    KERNEL1x1_L2 32, 32, 2, 0
    KERNEL1x1_L2 32, 32, 3, 0  
    KERNEL1x1_L2 32, 32, 4, 0
    KERNEL1x1_L2 32, 32, 5, 0 
    KERNEL1x1_L2 32, 32, 6, 0
    KERNEL1x1_L2 32, 32, 7, 0
    KERNEL1x1_L2 32, 32, 8, 0
    KERNEL1x1_L2 32, 32, 9, 0  
    KERNEL1x1_L2 32, 32, 10, 0
    KERNEL1x1_L2 32, 32, 11, 0  
    KERNEL1x1_L2 32, 32, 12, 0
    KERNEL1x1_L2 32, 32, 13, 0 
    KERNEL1x1_L2 32, 32, 14, 0
    KERNEL1x1_L2 32, 32, 15, 1   
    bdnz    ZGEMM_L1x1_LOOP
    MY_ALIGN  


ZGEMM_L1x1_LOOP_END:
/*----------------------------------------*/   
    END1x1_2 
    blr
    MY_ALIGN


/*----------------------N1 BEGINS---------*/
ZGEMM_L1:
/*----------------------------------------*/   
    andi.   T1, N, 1
    ble   ZGEMM_L1_END
		
ZGEMM_L1_BEGIN:
/*----------------------------------------*/   
    mr    CO, C
   
    add     T2,C,LDC    
    mr    AO, A  
    add   C, C, T1
#if defined(TRMMKERNEL) && defined(LEFT)   
    mr TEMP_REG, OFFSET  /*off = offset;*/
#endif     
    srawi.    I, M, 3
    ble   ZGEMM_L1x8_END
    dcbt    CO,r0  /*just prefetch*/
    dcbt    T2,r0    


ZGEMM_L1x8_BEGIN:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)   
    REFRESH_POINTERS  AO, BO,TEMP_REG, B, 8, 1
#else    
    mr    BO, B  
    dcbt    B, r0  
#endif     
    dcbt    AO, r0
#if defined(TRMMKERNEL)   
    REFRESH_TEMP_BK T6,K,TEMP_REG, 8, 1
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
    KERNEL1x8_ZERO_AND_PRIME_MMA
    ble   ZGEMM_L1x8_SUB0
    bl ZGEMM_L1x8_LMAIN_SUB
    andi.   L, T1, 127
    ble   ZGEMM_L1x8_SAVE
    b   ZGEMM_L1x8_SUB2


ZGEMM_L1x8_SUB0:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)   
    andi.   L, T6, 255
    cmpwi   T6, 129
#else   
    andi.   L, K, 255
    cmpwi   K, 129
#endif       
    li T8, 1
    bne CMP1x8_128K
    LOAD_END_1x8 -128, -16
    mtctr   T8    
    bl ZGEMM_L1x8_K128   
    b ZGEMM_L1x8_SAVE  
    CMP1x8_128K:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)    
    cmpwi   T6, 128
#else    
    cmpwi   K, 128
#endif        
    bne ZGEMM_L1x8_SUB2 
    MY_ALIGN   
    mtctr   T8
    addi BO, BO, -32
    addi AO,AO, -256   
    bl ZGEMM_L1x8_K128   
    b ZGEMM_L1x8_SAVE 
    MY_ALIGN


ZGEMM_L1x8_SUB2:
/*----------------------------------------*/   
    andi.      T1,L, 64
    ble ZGEMM_L1x8_SUB2_32
    dcbt    AO, PRE
    dcbt    BO, PRE
    KERNEL1x8_2 0, 0
    KERNEL1x8_2 1, 0
    dcbt    AO, T2  
    KERNEL1x8_2 2, 0
    KERNEL1x8_2 3, 0
    dcbt    AO, T3
    dcbt    BO, T2
    KERNEL1x8_2 4, 0
    KERNEL1x8_2 5, 0
    dcbt    AO, T4  
    KERNEL1x8_2 6, 0
    KERNEL1x8_2 7, 0
    dcbt    AO, T5  
    dcbt    BO, T3
    KERNEL1x8_2 8, 0
    KERNEL1x8_2 9, 0
    KERNEL1x8_2 10, 0
    KERNEL1x8_2 11, 0
    dcbt    BO, T4
    KERNEL1x8_2 12, 0
    KERNEL1x8_2 13, 0
    KERNEL1x8_2 14, 0
    KERNEL1x8_2 15, 0
    KERNEL1x8_2 16, 0
    KERNEL1x8_2 17, 0
    KERNEL1x8_2 18, 0
    KERNEL1x8_2 19, 0
    KERNEL1x8_2 20, 0
    KERNEL1x8_2 21, 0
    KERNEL1x8_2 22, 0
    KERNEL1x8_2 23, 0
    KERNEL1x8_2 24, 0
    KERNEL1x8_2 25, 0
    KERNEL1x8_2 26, 0
    KERNEL1x8_2 27, 0
    KERNEL1x8_2 28, 0
    KERNEL1x8_2 29, 0
    KERNEL1x8_2 30, 0
    KERNEL1x8_2 31, 1
    MY_ALIGN


ZGEMM_L1x8_SUB2_32:
/*----------------------------------------*/   
    andi.      T1,L, 32
    ble ZGEMM_L1x8_SUB2_16    
    dcbt    AO, PRE
    dcbt    BO, PRE
    KERNEL1x8_2 0, 0
    KERNEL1x8_2 1, 0
    dcbt    AO, T2  
    KERNEL1x8_2 2, 0
    KERNEL1x8_2 3, 0
    dcbt    AO, T3
    dcbt    BO, T2
    KERNEL1x8_2 4, 0
    KERNEL1x8_2 5, 0
    dcbt    AO, T4  
    KERNEL1x8_2 6, 0
    KERNEL1x8_2 7, 0
    dcbt    AO, T5  
    dcbt    BO, T3
    KERNEL1x8_2 8, 0
    KERNEL1x8_2 9, 0
    KERNEL1x8_2 10, 0
    KERNEL1x8_2 11, 0
    dcbt    BO, T4
    KERNEL1x8_2 12, 0
    KERNEL1x8_2 13, 0
    KERNEL1x8_2 14, 0
    KERNEL1x8_2 15, 1
    MY_ALIGN 


ZGEMM_L1x8_SUB2_16:
/*----------------------------------------*/   
    andi.      T1,L, 16
    ble ZGEMM_L1x8_SUB2_8
    dcbt    AO, PRE
    dcbt    BO, PRE
    KERNEL1x8_2 0, 0
    KERNEL1x8_2 1, 0
    dcbt    AO, T2  
    KERNEL1x8_2 2, 0
    KERNEL1x8_2 3, 0
    dcbt    AO, T3
    dcbt    BO, T2
    KERNEL1x8_2 4, 0
    KERNEL1x8_2 5, 0
    dcbt    AO, T4  
    KERNEL1x8_2 6, 0
    KERNEL1x8_2 7, 1
    MY_ALIGN    


ZGEMM_L1x8_SUB2_8:
/*----------------------------------------*/   
    andi.      T1,L, 8
    ble ZGEMM_L1x8_SUB2_4
    KERNEL1x8_2 0, 0
    KERNEL1x8_2 1, 0
    KERNEL1x8_2 2, 0
    KERNEL1x8_2 3, 1
    MY_ALIGN   


ZGEMM_L1x8_SUB2_4:
/*----------------------------------------*/   
    andi.      T1,L, 4
    ble ZGEMM_L1x8_SUB2_2
    KERNEL1x8_2 0, 0
    KERNEL1x8_2 1, 1
    MY_ALIGN


ZGEMM_L1x8_SUB2_2:
/*----------------------------------------*/   
    andi.      T1,L, 2
    ble ZGEMM_L1x8_SUB2_1
    KERNEL1x8_2 0, 1
    MY_ALIGN    


ZGEMM_L1x8_SUB2_1:
/*----------------------------------------*/   
    andi.      T1,L, 1
    ble ZGEMM_L1x8_SAVE 
    LOAD_END_1x8 128, 16


ZGEMM_L1x8_SAVE:
/*----------------------------------------*/   
    addic.    I, I, -1
    KERNEL1x8_UNPRIME_MMA
    SAVE1x8
#if defined(TRMMKERNEL)    
    REFRESH_AFTER_SAVE T6,K,TEMP_REG, BO,AO, 8, 1
#endif     
    bgt   ZGEMM_L1x8_BEGIN
    andi.   T2, M, 7
    ble   ZGEMM_L1x1_END
    andi.   T1, M, 4
    ble   ZGEMM_L1x4_END
    b   ZGEMM_L1x4_BEGIN
    MY_ALIGN 


ZGEMM_L1x8_END:
/*----------------------------------------*/   


ZGEMM_L1x4_BEGIN:
/*----------------------------------------*/   
    andi.   T2, M, 7
    ble   ZGEMM_L1x1_END
    andi.   T1, M, 4
    ble   ZGEMM_L1x4_END
#if defined(TRMMKERNEL)   
    REFRESH_POINTERS  AO, BO,TEMP_REG, B, 4, 1
#else    
    mr    BO, B   
#endif        
#if defined(TRMMKERNEL)   
    REFRESH_TEMP_BK T6,K,TEMP_REG, 4, 1
    mr T1, T6 
    addi T1,T1, -2 
    srawi.   T8, T1, 5 /**(T11-2) % 32x */
#else   
    mr T1, K 
    addi T1,T1, -2
    srawi.   T8, T1, 5 /**(K-2) % 32x */
#endif     
    KERNEL1x4_ZERO_AND_PRIME_MMA
    ble   ZGEMM_L1x4_SUB0 
    bl ZGEMM_1x4_LMAIN_SUB
    andi.   L, T1, 31
    ble   ZGEMM_L1x4_SAVE
    b   ZGEMM_L1x4_SUB2


ZGEMM_L1x4_SUB0:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)   
    andi.   L, T6, 63
    cmpwi   T6, 33
#else   
    andi.   L, K, 63
    cmpwi   K, 33
#endif       
    li T8, 1
    bne CMP1x4_32K
    LOAD_END_1x4 -64, -16 
    mtctr   T8    
    bl ZGEMM_L1x4_K32   
    b ZGEMM_L1x4_SAVE  
    CMP1x4_32K:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)    
    cmpwi   T6, 32
#else    
    cmpwi   K, 32
#endif        
    bne ZGEMM_L1x4_SUB2 
    MY_ALIGN   
    mtctr   T8
    addi BO, BO, -32
    addi AO,AO, -128   
    bl ZGEMM_L1x4_K32   
    b ZGEMM_L1x4_SAVE 
    MY_ALIGN 
    MY_ALIGN 


ZGEMM_L1x4_SUB2:
/*----------------------------------------*/   
    andi.      T1,L, 16
    ble ZGEMM_L1x4_SUB2_8
    KERNEL1x4_2 0, 0
    KERNEL1x4_2 1, 0
    KERNEL1x4_2 2, 0
    KERNEL1x4_2 3, 0
    KERNEL1x4_2 4, 0
    KERNEL1x4_2 5, 0
    KERNEL1x4_2 6, 0
    KERNEL1x4_2 7, 1
    MY_ALIGN


ZGEMM_L1x4_SUB2_8:
/*----------------------------------------*/   
    andi.      T1,L, 8
    ble ZGEMM_L1x4_SUB2_4
    KERNEL1x4_2 0, 0
    KERNEL1x4_2 1, 0
    KERNEL1x4_2 2, 0
    KERNEL1x4_2 3, 1
    MY_ALIGN  


ZGEMM_L1x4_SUB2_4:
/*----------------------------------------*/   
    andi.      T1,L, 4
    ble ZGEMM_L1x4_SUB2_2
    KERNEL1x4_2 0, 0
    KERNEL1x4_2 1, 1
    MY_ALIGN


ZGEMM_L1x4_SUB2_2:
/*----------------------------------------*/   
    andi.      T1,L, 2
    ble ZGEMM_L1x4_SUB2_1
    KERNEL1x4_2 0, 1
    MY_ALIGN    


ZGEMM_L1x4_SUB2_1:
/*----------------------------------------*/   
    andi.      T1,L, 1
    ble ZGEMM_L1x4_SAVE 
    LOAD_END_1x4 64,16



ZGEMM_L1x4_SAVE:
/*----------------------------------------*/   
    KERNEL1x4_UNPRIME_MMA
    SAVE1x4
#if defined(TRMMKERNEL)    
    REFRESH_AFTER_SAVE T6,K,TEMP_REG, BO,AO, 4, 1
#endif     


ZGEMM_L1x4_END:
/*----------------------------------------*/   


ZGEMM_L1x2_BEGIN:
/*----------------------------------------*/   
    andi.   T1, M, 2
    ble   ZGEMM_L1x2_END
#if defined(TRMMKERNEL)   
    REFRESH_POINTERS  AO, BO,TEMP_REG, B, 2, 1
#else    
    mr    BO, B   
#endif        
#if defined(TRMMKERNEL)   
    REFRESH_TEMP_BK T6,K,TEMP_REG, 2, 1
    mr T1, T6 
    addi T1,T1, -2 
    srawi.   T8, T1, 5 /**(T11-2) % 32x */
#else   
    mr T1, K 
    addi T1,T1, -2
    srawi.   T8, T1, 5 /**(K-2) % 32x */
#endif     
    KERNEL1x2_ZERO_AND_PRIME_MMA
    ble   ZGEMM_L1x2_SUB0 
    bl ZGEMM_1x2_LMAIN_SUB
    andi.   L, T1, 31
    ble   ZGEMM_L1x2_SAVE
    b   ZGEMM_L1x2_SUB2


ZGEMM_L1x2_SUB0:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)   
    andi.   L, T6, 63
    cmpwi   T6, 33
#else   
    andi.   L, K, 63
    cmpwi   K, 33
#endif       
    li T8, 1
    bne CMP1x2_32K
    LOAD_END_1x2 -32, -16 
    mtctr   T8    
    bl ZGEMM_L1x2_K32   
    b ZGEMM_L1x2_SAVE  
    CMP1x2_32K:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)    
    cmpwi   T6, 32
#else    
    cmpwi   K, 32
#endif        
    bne ZGEMM_L1x2_SUB2 
    MY_ALIGN   
    mtctr   T8
    addi BO, BO, -32
    addi AO,AO, -64   
    bl ZGEMM_L1x2_K32   
    b ZGEMM_L1x2_SAVE 
    MY_ALIGN 
    MY_ALIGN 


ZGEMM_L1x2_SUB2:
/*----------------------------------------*/   
    andi.      T1,L, 16
    ble ZGEMM_L1x2_SUB2_8
    KERNEL1x2_2 0, 0
    KERNEL1x2_2 1, 0
    KERNEL1x2_2 2, 0
    KERNEL1x2_2 3, 0
    KERNEL1x2_2 4, 0
    KERNEL1x2_2 5, 0
    KERNEL1x2_2 6, 0
    KERNEL1x2_2 7, 1
    MY_ALIGN


ZGEMM_L1x2_SUB2_8:
/*----------------------------------------*/   
    andi.      T1,L, 8
    ble ZGEMM_L1x2_SUB2_4
    KERNEL1x2_2 0, 0
    KERNEL1x2_2 1, 0
    KERNEL1x2_2 2, 0
    KERNEL1x2_2 3, 1
    MY_ALIGN  


ZGEMM_L1x2_SUB2_4:
/*----------------------------------------*/   
    andi.      T1,L, 4
    ble ZGEMM_L1x2_SUB2_2
    KERNEL1x2_2 0, 0
    KERNEL1x2_2 1, 1
    MY_ALIGN


ZGEMM_L1x2_SUB2_2:
/*----------------------------------------*/   
    andi.      T1,L, 2
    ble ZGEMM_L1x2_SUB2_1
    KERNEL1x2_2 0, 1
    MY_ALIGN    


ZGEMM_L1x2_SUB2_1:
/*----------------------------------------*/   
    andi.      T1,L, 1
    ble ZGEMM_L1x2_SAVE 
    LOAD_END_1x2 32,16


ZGEMM_L1x2_SAVE:
/*----------------------------------------*/   
    KERNEL1x2_UNPRIME_MMA
    SAVE1x2
#if defined(TRMMKERNEL)    
    REFRESH_AFTER_SAVE T6,K,TEMP_REG, BO,AO, 2, 1
#endif     


ZGEMM_L1x2_END:
/*----------------------------------------*/   


ZGEMM_L1x1_BEGIN:
/*----------------------------------------*/   
    andi.   T1, M, 1
    ble   ZGEMM_L1x1_END
#if defined(TRMMKERNEL)   
    REFRESH_POINTERS  AO, BO,TEMP_REG, B, 1, 1
#else    
    mr    BO, B   
#endif        
#if defined(TRMMKERNEL)   
    REFRESH_TEMP_BK T6,K,TEMP_REG, 1, 1
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
    andi.   L, T1, 31
    ble   ZGEMM_L1x1_SAVE
    b   ZGEMM_L1x1_SUB2


ZGEMM_L1x1_SUB0:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)   
    andi.   L, T6, 63
    cmpwi   T6, 33
#else   
    andi.   L, K, 63
    cmpwi   K, 33
#endif       
    li T8, 1
    bne CMP1x1_32K
    addi BO, BO, -16
    addi AO,AO, -16  
    LOAD1x1O 16, 16 
    END1x1_WITHOUT_ADD   
    LOAD1x1_2O  32, 32  
    mtctr   T8    
    bl ZGEMM_L1x1_K32   
    b ZGEMM_L1x1_SAVE  
    CMP1x1_32K:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)    
    cmpwi   T6, 32
#else    
    cmpwi   K, 32
#endif        
    bne ZGEMM_L1x1_SUB2 
    MY_ALIGN   
    mtctr   T8
    addi BO, BO, -32
    addi AO,AO, -32   
    LOAD1x1_2O 32, 32
    bl ZGEMM_L1x1_K32   
    b ZGEMM_L1x1_SAVE 
    MY_ALIGN 


ZGEMM_L1x1_SUB2:
/*----------------------------------------*/   
    andi.      T1, L, 16
    ble ZGEMM_L1x1_SUB2_8
    LOAD1x1_2
    KERNEL1x1_L2 32, 32, 0, 0
    KERNEL1x1_L2 32, 32, 1, 0  
    KERNEL1x1_L2 32, 32, 2, 0
    KERNEL1x1_L2 32, 32, 3, 0  
    KERNEL1x1_L2 32, 32, 4, 0
    KERNEL1x1_L2 32, 32, 5, 0 
    KERNEL1x1_L2 32, 32, 6, 0
    KERNEL1x1_E2 32, 32, 7, 1
    MY_ALIGN


ZGEMM_L1x1_SUB2_8:
/*----------------------------------------*/   
    andi.      T1, L, 8
    ble ZGEMM_L1x1_SUB2_4
    LOAD1x1_2
    KERNEL1x1_L2 32, 32, 0, 0
    KERNEL1x1_L2 32, 32, 1, 0  
    KERNEL1x1_L2 32, 32, 2, 0
    KERNEL1x1_E2 32, 32, 3, 1  
    MY_ALIGN  


ZGEMM_L1x1_SUB2_4:
/*----------------------------------------*/   
    andi.      T1,L, 4
    ble ZGEMM_L1x1_SUB2_2
    LOAD1x1_2
    KERNEL1x1_L2  32, 32, 0, 0
    KERNEL1x1_E2  32, 32, 1, 1
    MY_ALIGN


ZGEMM_L1x1_SUB2_2:
/*----------------------------------------*/   
    andi.      T1,L, 2
    ble ZGEMM_L1x1_SUB2_1
    LOAD1x1_2
    KERNEL1x1_E2  32, 32, 0, 1
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
    REFRESH_AFTER_SAVE T6,K,TEMP_REG, BO,AO, 1, 1
#endif   


ZGEMM_L1x1_END:
/*----------------------------------------*/   
#if defined(TRMMKERNEL) && !defined(LEFT)   
    addi TEMP_REG, TEMP_REG, 1
#endif   


ZGEMM_L1_END:
/*----------------------------------------*/   
