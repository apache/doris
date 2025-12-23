#define MY_ALIGN .align 3
b L8

	MY_ALIGN
LSGEMM_L8x16_LMAIN_SUB: 
	LOAD8x16_2    
	MY_ALIGN

LSGEMM_L8x16_LOOP:
    KERNEL8x16_L2 128,64,0,0 
LSGEMM_L8x16_K128:
    KERNEL8x16_L2 128,64,1,0 
    KERNEL8x16_I1_L4_2  128,64, 1,0
    KERNEL8x16_I1_L4_2  128,64, 2,0
    KERNEL8x16_I1_L4_2  128,64, 3,0
    KERNEL8x16_I1_L4_2  128,64, 4,0
    KERNEL8x16_I1_L4_2  128,64, 5,0        
    KERNEL8x16_I1_L4_2  128,64, 6,0
    KERNEL8x16_I1_L4_2  128,64, 7,0  
    KERNEL8x16_I1_L4_2  128,64, 8,0      
    KERNEL8x16_I1_L4_2  128,64, 9,0
    KERNEL8x16_I1_L4_2  128,64, 10,0
    KERNEL8x16_I1_L4_2  128,64, 11,0
    KERNEL8x16_I1_L4_2  128,64, 12,0
    KERNEL8x16_I1_L4_2  128,64, 13,0    
    KERNEL8x16_I1_L4_2  128,64, 14,0    
    KERNEL8x16_I1_L4_2  128,64, 15,0  	
    KERNEL8x16_I1_L4_2  128,64, 16,0
    KERNEL8x16_I1_L4_2  128,64, 17,0
    KERNEL8x16_I1_L4_2  128,64, 18,0
    KERNEL8x16_I1_L4_2  128,64, 19,0
    KERNEL8x16_I1_L4_2  128,64, 20,0
    KERNEL8x16_I1_L4_2  128,64, 21,0        
    KERNEL8x16_I1_L4_2  128,64, 22,0
    KERNEL8x16_I1_L4_2  128,64, 23,0  
    KERNEL8x16_I1_L4_2  128,64, 24,0      
    KERNEL8x16_I1_L4_2  128,64, 25,0
    KERNEL8x16_I1_L4_2  128,64, 26,0
    KERNEL8x16_I1_L4_2  128,64, 27,0
    KERNEL8x16_I1_L4_2  128,64, 28,0
    KERNEL8x16_I1_L4_2  128,64, 29,0    
    KERNEL8x16_I1_L4_2  128,64, 30,0    
    KERNEL8x16_I1_L4_2  128,64, 31,1 
	bdnz		LSGEMM_L8x16_LOOP

	MY_ALIGN
LSGEMM_L8x16_LOOP_END: 
    END8x16_2
    blr  

	MY_ALIGN
LSGEMM_L8x16_L64_SUB: 
	LOAD8x16_2     
    KERNEL8x16_I1_L4_2  128,64, 0,0
    KERNEL8x16_I1_L4_2  128,64, 1,0
    KERNEL8x16_I1_L4_2  128,64, 2,0
    KERNEL8x16_I1_L4_2  128,64,3,0
    KERNEL8x16_I1_L4_2  128,64,4,0
    KERNEL8x16_I1_L4_2  128,64,5,0        
    KERNEL8x16_I1_L4_2  128,64,6,0
    KERNEL8x16_I1_L4_2  128,64,7,0  
    KERNEL8x16_I1_L4_2  128,64,8,0      
    KERNEL8x16_I1_L4_2  128,64,9,0
    KERNEL8x16_I1_L4_2  128,64,10,0
    KERNEL8x16_I1_L4_2  128,64,11,0
    KERNEL8x16_I1_L4_2  128,64,12,0
    KERNEL8x16_I1_L4_2  128,64,13,0    
    KERNEL8x16_I1_L4_2  128,64,14,0    
    KERNEL8x16_I1_L4_3  128,64,15,1 
    blr	
LSGEMM_L8x16_L32_SUB: 
	LOAD8x16_2     
    KERNEL8x16_I1_L4_2  128,64,0,0
    KERNEL8x16_I1_L4_2  128,64,1,0
    KERNEL8x16_I1_L4_2  128,64,2,0
    KERNEL8x16_I1_L4_2  128,64,3,0
    KERNEL8x16_I1_L4_2  128,64,4,0
    KERNEL8x16_I1_L4_2  128,64,5,0        
    KERNEL8x16_I1_L4_2  128,64,6,0
    KERNEL8x16_I1_L4_3  128,64,7,1
    blr	

LSGEMM_L8x16_L16_SUB: 
	LOAD8x16_2     
    KERNEL8x16_I1_L4_2  128,64,0,0
    KERNEL8x16_I1_L4_2  128,64,1,0
    KERNEL8x16_I1_L4_2  128,64,2,0
    KERNEL8x16_I1_L4_3  128,64,3,1
    blr	

L8:
#if defined(TRMMKERNEL) && !defined(LEFT)
   neg TEMP_REG, OFFSET 
#endif

	srawi.		J,	N,	3

	ble		LSGEMM_L8_END

LSGEMM_L8_BEGIN:

	li		T1,	128
	li		T2,	256
 
	mr		AO,	A
	mr		CO,	C
	slwi		T3,	LDC	,	3
	add		C,	C,	T3

	dcbt		A,	T1
	dcbt		A,	T2
#if defined(TRMMKERNEL) && defined(LEFT)
	mr TEMP_REG, OFFSET	 /*off = offset;*/
#endif 
	srawi.		I,	M,	4
	ble		LSGEMM_L8x16_END

	MY_ALIGN
LSGEMM_L8x16_BEGIN:

#if defined(TRMMKERNEL)
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,16,8
#else
	mr		BO,	B
#endif	

#if defined(TRMMKERNEL)
   REFRESH_TEMP_BK T11,K,TEMP_REG,16,8
   mr T12, T11
   addi T12,T12, -2
   srawi.		L, T12,	7 /**(T11-2) % 128x */
#else
   mr T12, K
   addi T12,T12, -2
   srawi.		L,	T12,	7 /**(K-2) % 128x */
#endif 
 
    ZERO8x16 
	ble		LSGEMM_L8x16_SUB0
	mtctr		L 
    bl      LSGEMM_L8x16_LMAIN_SUB
	andi.		L,	T12,	127
	ble		LSGEMM_L8x16_SAVE
	b		LSGEMM_L8x16_SUB2   
	MY_ALIGN
LSGEMM_L8x16_SUB0:
#if defined(TRMMKERNEL)
	andi.		L,	T11,	255
    cmpwi   T11,129
#else
	andi.		L,	K,	255
    cmpwi   K,129
#endif       
    li T10,1
    bne CMP8x16_128K
    addi BO,BO,-32
    addi AO,AO,-64 
    LOAD8x16 64,32 
    END8x16_WITHOUT_ADD   
    LOAD8x16_2O AO,BO,  128, 64 
    mtctr   T10   
    bl LSGEMM_L8x16_K128   
    b LSGEMM_L8x16_SAVE  
CMP8x16_128K:
/*----------------------------------------*/   
#if defined(TRMMKERNEL)    
    cmpwi   T11,128
#else    
    cmpwi   K,128
#endif        
    bne LSGEMM_L8x16_SUB2 
    MY_ALIGN   
    mtctr   T10
    addi BO,BO,-64
    addi AO,AO,-128   
    LOAD8x16_2O  AO,BO,  128,64
    bl LSGEMM_L8x16_K128   
    b LSGEMM_L8x16_SAVE
	MY_ALIGN
LSGEMM_L8x16_SUB2:
    andi.   T10,L,64
    ble   LSGEMM_L8x16_SUB2_32
    bl   LSGEMM_L8x16_L64_SUB
    MY_ALIGN 
LSGEMM_L8x16_SUB2_32:
    andi.      T10,L, 32
    ble LSGEMM_L8x16_SUB2_16
    bl   LSGEMM_L8x16_L32_SUB
    MY_ALIGN                
LSGEMM_L8x16_SUB2_16:
    andi.      T10,L, 16
    ble LSGEMM_L8x16_SUB2_8
	bl  LSGEMM_L8x16_L16_SUB
    MY_ALIGN 
LSGEMM_L8x16_SUB2_8:
    andi.      T10,L, 8
    ble LSGEMM_L8x16_SUB2_4 
	LOAD8x16_2
    KERNEL8x16_I1_L4_2  128,64, 0,0
    KERNEL8x16_I1_L4_3  128,64, 1,1
	MY_ALIGN	
LSGEMM_L8x16_SUB2_4:
    andi.      T10,L, 4
    ble LSGEMM_L8x16_SUB2_2
    LOAD8x16_2
    KERNEL8x16_I1_L4_3  128,64, 0,1
    MY_ALIGN
LSGEMM_L8x16_SUB2_2:
    andi.      T10,L, 2
    ble LSGEMM_L8x16_SUB2_1
    LOAD8x16_2
    KERNEL8x16_E2  128,64, 0,1
    MY_ALIGN    
LSGEMM_L8x16_SUB2_1:
    andi.      T10,L, 1
    ble LSGEMM_L8x16_SAVE	
    KERNEL8x16 0


	MY_ALIGN
LSGEMM_L8x16_SAVE:
	SAVE8x16
#if defined(TRMMKERNEL)	
	REFRESH_AFTER_SAVE T11,K,TEMP_REG,BO,AO,16,8
#endif	
	addic.		I,	I,	-1
	bgt+		LSGEMM_L8x16_BEGIN
    MY_ALIGN
LSGEMM_L8x16_END:
LSGEMM_L8x8_BEGIN:
    andi.       T2, M,  15
    ble     LSGEMM_L8x1_END

    andi.       T1, M,  8
    ble     LSGEMM_L8x8_END

#if defined(TRMMKERNEL)
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,8,8
#else
    mr      BO, B
#endif  

#if defined(TRMMKERNEL)
   REFRESH_TEMP_BK T11,K,TEMP_REG,8,8
   mr T12, T11
   addi T12,T12, -1
   srawi.       L, T12, 4 /**(T11-1) % 16x */
#else
   mr T12, K
   addi T12,T12, -1
   srawi.       L,  T12,    4 /**(K-1) % 16x */
#endif 
    
    ZERO8x8
    ble     LSGEMM_L8x8_SUB0

    MY_ALIGN
LSGEMM_L8x8_LOOP_START:
 
    LOAD8x8_0  /*we already zeroed */ 
    mtctr       L

    MY_ALIGN

LSGEMM_L8x8_LOOP:

    KERNEL8x8_I1_L4_2  32,32, 0,0
    KERNEL8x8_I1_L4_2  32,32, 1,0
    KERNEL8x8_I1_L4_2  32,32, 2,0
    KERNEL8x8_I1_L4_2  32,32, 3,1    

    bdnz        LSGEMM_L8x8_LOOP

    MY_ALIGN
LSGEMM_L8x8_LOOP_END:

    END8x8 0, AO, BO, 32, 32    

    b       LSGEMM_L8x8_SUB1 
    MY_ALIGN
LSGEMM_L8x8_SUB0:
#if defined(TRMMKERNEL)
    andi.       L,  T11,    31
#else
    andi.       L,  K,  31
#endif   
    b       LSGEMM_L8x8_SUB2
    MY_ALIGN
LSGEMM_L8x8_SUB1:
#if defined(TRMMKERNEL)
    andi.       L,  T12,    15
#else
    andi.       L,  T12,    15
#endif  
    ble     LSGEMM_L8x8_SAVE
    MY_ALIGN
LSGEMM_L8x8_SUB2:
 
    srawi.      T1,L, 3
    ble LSGEMM_L8x8_SUB2_4 
    mtctr		T1
    MY_ALIGN
LSGEMM_L8x8_SUB2_LOOP:    
    LOAD8x8_0
    KERNEL8x8_I1_L4_2  32,32, 0,0
    KERNEL8x8_I1_L4_3  32,32, 1,1
    bdnz LSGEMM_L8x8_SUB2_LOOP
    MY_ALIGN    
LSGEMM_L8x8_SUB2_4:
    andi.      T1,L, 4
    ble LSGEMM_L8x8_SUB2_2
    LOAD8x8_0
    KERNEL8x8_I1_L4_3  32,32, 0,1
    MY_ALIGN
LSGEMM_L8x8_SUB2_2:
    andi.      T1,L, 2
    ble LSGEMM_L8x8_SUB2_1
    LOAD8x8_0
    KERNEL8x8_I1_L2_3  32,32, 0,1
    MY_ALIGN    
LSGEMM_L8x8_SUB2_1:
    andi.      T1,L, 1
    ble LSGEMM_L8x8_SAVE   
    KERNEL8x8 0
 

    MY_ALIGN
LSGEMM_L8x8_SAVE:
    SAVE8x8
#if defined(TRMMKERNEL) 
    REFRESH_AFTER_SAVE T11,K,TEMP_REG,BO,AO,8,8
#endif  
    MY_ALIGN  
LSGEMM_L8x8_END:
LSGEMM_L8x4_BEGIN:
    andi.       T2, M,  15
    ble     LSGEMM_L8x1_END

    andi.       T1, M,  4
    ble     LSGEMM_L8x4_END

#if defined(TRMMKERNEL)
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,4,8
#else
    mr      BO, B
#endif  

#if defined(TRMMKERNEL)
   REFRESH_TEMP_BK T11,K,TEMP_REG,4,8
   mr T12, T11
   addi T12,T12, -1
   srawi.       L, T12, 4 /**(T11-1) % 16x */
#else
   mr T12, K
   addi T12,T12, -1
   srawi.       L,  T12,    4 /**(K-1) % 16x */
#endif 
    
    ZERO8x4
    ble     LSGEMM_L8x4_SUB0

    MY_ALIGN
LSGEMM_L8x4_LOOP_START:
 
    LOAD8x4_0  /*we already zeroed */ 
    mtctr       L

    MY_ALIGN

LSGEMM_L8x4_LOOP:

    KERNEL8x4_I1_L4_2  16,32, 0,0
    KERNEL8x4_I1_L4_2  16,32, 1,0
    KERNEL8x4_I1_L4_2  16,32, 2,0
    KERNEL8x4_I1_L4_2  16,32, 3,1    

    bdnz        LSGEMM_L8x4_LOOP

    MY_ALIGN
LSGEMM_L8x4_LOOP_END:

    END8x4 0, AO, BO, 16, 32    

    b       LSGEMM_L8x4_SUB1 
    MY_ALIGN
LSGEMM_L8x4_SUB0:
#if defined(TRMMKERNEL)
    andi.       L,  T11,    31
#else
    andi.       L,  K,  31
#endif   
    b       LSGEMM_L8x4_SUB2
    MY_ALIGN
LSGEMM_L8x4_SUB1:
#if defined(TRMMKERNEL)
    andi.       L,  T12,    15
#else
    andi.       L,  T12,    15
#endif  
    ble     LSGEMM_L8x4_SAVE
    MY_ALIGN
LSGEMM_L8x4_SUB2:

    srawi.      T1,L, 3
    ble LSGEMM_L8x4_SUB2_4 
    mtctr		T1
    MY_ALIGN
LSGEMM_L8x4_SUB2_LOOP:      
    LOAD8x4_0
    KERNEL8x4_I1_L4_2  16,32, 0,0
    KERNEL8x4_I1_L4_3  16,32, 1,1
    bdnz LSGEMM_L8x4_SUB2_LOOP
    MY_ALIGN    
LSGEMM_L8x4_SUB2_4:
    andi.      T1,L, 4
    ble LSGEMM_L8x4_SUB2_2
    LOAD8x4_0
    KERNEL8x4_I1_L4_3  16,32, 0,1
    MY_ALIGN
LSGEMM_L8x4_SUB2_2:
    andi.      T1,L, 2
    ble LSGEMM_L8x4_SUB2_1
    LOAD8x4_0
    KERNEL8x4_I1_L2_3  16,32, 0,1
    MY_ALIGN    
LSGEMM_L8x4_SUB2_1:
    andi.      T1,L, 1
    ble LSGEMM_L8x4_SAVE   
    KERNEL8x4 0
 

    MY_ALIGN
LSGEMM_L8x4_SAVE:
    SAVE8x4
#if defined(TRMMKERNEL) 
    REFRESH_AFTER_SAVE T11,K,TEMP_REG,BO,AO,4,8
#endif  
    MY_ALIGN  
LSGEMM_L8x4_END:
LSGEMM_L8x2_BEGIN:
    andi.       T1, M,  2
    ble     LSGEMM_L8x2_END

#if defined(TRMMKERNEL)
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,2,8
#else
    mr      BO, B
#endif  

#if defined(TRMMKERNEL)
   REFRESH_TEMP_BK T11,K,TEMP_REG,2,8 
   srawi.       L, T11, 3 /**(T11) % 8x */
#else
   srawi.       L,  K,    3 /**(K) % 8x */
#endif 
    
    ZERO8x2
    ble     LSGEMM_L8x2_SUB0

    MY_ALIGN
LSGEMM_L8x2_LOOP_START: 
    mtctr       L

    MY_ALIGN

LSGEMM_L8x2_LOOP:

    KERNEL8x2_2  0,0, 0,0
    KERNEL8x2_2  0,0, 1,0
    KERNEL8x2_2  0,0, 2,0
    KERNEL8x2_2  0,0, 3,1    

    bdnz        LSGEMM_L8x2_LOOP

    MY_ALIGN
LSGEMM_L8x2_LOOP_END:   
 
LSGEMM_L8x2_SUB0:
#if defined(TRMMKERNEL)
    andi.       L,  T11,    7
#else
    andi.       L,  K,  7
#endif    
    ble     LSGEMM_L8x2_SAVE
    MY_ALIGN
LSGEMM_L8x2_SUB2:
    andi.      T1,L, 4
    ble LSGEMM_L8x2_SUB2_2
    KERNEL8x2_2  0,0, 0,0
    KERNEL8x2_2  0,0, 1,1
    MY_ALIGN
LSGEMM_L8x2_SUB2_2:
    andi.      T1,L, 2
    ble LSGEMM_L8x2_SUB2_1
    KERNEL8x2_2  0,0, 0,1 
    MY_ALIGN    
LSGEMM_L8x2_SUB2_1:
    andi.      T1,L, 1
    ble LSGEMM_L8x2_SAVE   
    KERNEL8x2
  
    MY_ALIGN
LSGEMM_L8x2_SAVE:
    SAVE8x2
#if defined(TRMMKERNEL) 
    REFRESH_AFTER_SAVE T11,K,TEMP_REG,BO,AO,2,8
#endif  
    MY_ALIGN  
LSGEMM_L8x2_END:
LSGEMM_L8x1_BEGIN: 
    andi.       T1, M,  1
    ble     LSGEMM_L8x1_END
#if defined(TRMMKERNEL)
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,1,8
#else
    mr      BO, B
#endif  

#if defined(TRMMKERNEL)
   REFRESH_TEMP_BK T11,K,TEMP_REG,1,8 
   srawi.       L, T11, 3 /**(T11) % 8x */
#else
   srawi.       L,  K,    3 /**(K) % 8x */
#endif 
    
    ZERO8x1
    ble     LSGEMM_L8x1_SUB0

    MY_ALIGN
LSGEMM_L8x1_LOOP_START: 
    mtctr       L

    MY_ALIGN

LSGEMM_L8x1_LOOP:

    KERNEL8x1_4  0,0, 0,0
    KERNEL8x1_4  0,0, 1,1     

    bdnz        LSGEMM_L8x1_LOOP

    MY_ALIGN
LSGEMM_L8x1_LOOP_END:   
 
LSGEMM_L8x1_SUB0:
#if defined(TRMMKERNEL)
    andi.       L,  T11,    7
#else
    andi.       L,  K,  7
#endif    
    ble     LSGEMM_L8x1_SAVE
    MY_ALIGN
LSGEMM_L8x1_SUB2:
    andi.      T1,L, 4
    ble LSGEMM_L8x1_SUB2_2
    KERNEL8x1_4  0,0, 0,1 
    MY_ALIGN
LSGEMM_L8x1_SUB2_2:
    andi.      T1,L, 2
    ble LSGEMM_L8x1_SUB2_1
    KERNEL8x1_2 
    MY_ALIGN    
LSGEMM_L8x1_SUB2_1:
    andi.      T1,L, 1
    ble LSGEMM_L8x1_SAVE   
    KERNEL8x1
  
    MY_ALIGN
LSGEMM_L8x1_SAVE:
    SAVE8x1
#if defined(TRMMKERNEL) 
    REFRESH_AFTER_SAVE T11,K,TEMP_REG,BO,AO,1,8
#endif  
    MY_ALIGN  
LSGEMM_L8x1_END:

	slwi		T1,	K,	5
	add		B,	B,	T1
#if defined(TRMMKERNEL) && !defined(LEFT)
    addi TEMP_REG, TEMP_REG, 8
#endif
	addic.		J,	J,	-1
	bgt		LSGEMM_L8_BEGIN
 

LSGEMM_L8_END:

/*	b		LSGEMM_L4_BEGIN*/
    andi.       T1, N,  4
    ble     LSGEMM_L4_END
LSGEMM_L4_BEGIN:
  

	mr		AO,	A
	mr		CO,	C
	slwi		T3,	LDC	,	2
	add		C,	C,	T3
 
#if defined(TRMMKERNEL) && defined(LEFT)
	mr TEMP_REG, OFFSET	 /*off = offset;*/
#endif 
	srawi.		I,	M,	4
	ble		LSGEMM_L4x16_END

	MY_ALIGN
LSGEMM_L4x16_BEGIN:

#if defined(TRMMKERNEL)
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,16,4
#else
	mr		BO,	B
#endif	

#if defined(TRMMKERNEL)
   REFRESH_TEMP_BK T11,K,TEMP_REG,16,4
   mr T12, T11
   addi T12,T12, -1
   srawi.		L, T12,	6 /**(T11-1) % 64x */
#else
   mr T12, K
   addi T12,T12, -1
   srawi.		L,	T12,	6 /**(K-1) % 64x */
#endif 
 
    ZERO4x16
	ble		LSGEMM_L4x16_SUB0

	MY_ALIGN
LSGEMM_L4x16_LOOP_START:
 
	LOAD4x16_0  /*we already zeroed */
    ##OffsetA=64 OffsetB=16
    addi AO,AO,2112
    addi BO,BO,16  

	mtctr		L

	MY_ALIGN

LSGEMM_L4x16_LOOP:

    KERNEL4x16_I1_L4_2  -2048,0, 0,0
    KERNEL4x16_I1_L4_2  -2048,0, 1,0
    KERNEL4x16_I1_L4_2  -2048,0, 2,0
    KERNEL4x16_I1_L4_2  -2048,0, 3,0
    KERNEL4x16_I1_L4_2  -2048,0, 4,0
    KERNEL4x16_I1_L4_2  -2048,0, 5,0        
    KERNEL4x16_I1_L4_2  -2048,0, 6,0
    KERNEL4x16_I1_L4_2  -2048,0, 7,0  
    KERNEL4x16_I1_L4_2  -2048,0, 8,0      
    KERNEL4x16_I1_L4_2  -2048,0, 9,0
    KERNEL4x16_I1_L4_2  -2048,0, 10,0
    KERNEL4x16_I1_L4_2  -2048,0, 11,0
    KERNEL4x16_I1_L4_2  -2048,0, 12,0
    KERNEL4x16_I1_L4_2  -2048,0, 13,0    
    KERNEL4x16_I1_L4_2  -2048,0, 14,0    
    KERNEL4x16_I1_L4_2  -2048,0, 15,1  	

	bdnz		LSGEMM_L4x16_LOOP

	MY_ALIGN
LSGEMM_L4x16_LOOP_END:

    END4x16 0, AO, BO, -2048, 0    

	b		LSGEMM_L4x16_SUB1 
	MY_ALIGN
LSGEMM_L4x16_SUB0:
#if defined(TRMMKERNEL)
	andi.		L,	T11,	127
#else
	andi.		L,	K,	127
#endif   
	b		LSGEMM_L4x16_SUB2
	MY_ALIGN
LSGEMM_L4x16_SUB1:
#if defined(TRMMKERNEL)
	andi.		L,	T12,	63
#else
	andi.		L,  T12,	63
#endif	
	ble		LSGEMM_L4x16_SAVE
	MY_ALIGN
LSGEMM_L4x16_SUB2:

    srawi.      T10,L, 5
    ble LSGEMM_L4x16_SUB2_16
    mtctr		T10
    MY_ALIGN
LSGEMM_L4x16_SUB2_LOOP:
	LOAD4x16_0 
    KERNEL4x16_I1_L4_2  64,16, 0,0
    KERNEL4x16_I1_L4_2  64,16, 1,0
    KERNEL4x16_I1_L4_2  64,16, 2,0
    KERNEL4x16_I1_L4_2  64,16, 3,0
    KERNEL4x16_I1_L4_2  64,16, 4,0
    KERNEL4x16_I1_L4_2  64,16, 5,0
    KERNEL4x16_I1_L4_2  64,16, 6,0
    KERNEL4x16_I1_L4_3  64,16, 7,1
    bdnz LSGEMM_L4x16_SUB2_LOOP 
    MY_ALIGN        
LSGEMM_L4x16_SUB2_16:
    andi.      T10,L, 16
    ble LSGEMM_L4x16_SUB2_8
	LOAD4x16_0 
    KERNEL4x16_I1_L4_2  64,16, 0,0
    KERNEL4x16_I1_L4_2  64,16, 1,0
    KERNEL4x16_I1_L4_2  64,16, 2,0
    KERNEL4x16_I1_L4_3  64,16, 3,1
    MY_ALIGN 
LSGEMM_L4x16_SUB2_8:
    andi.      T10,L, 8
    ble LSGEMM_L4x16_SUB2_4 
	LOAD4x16_0
    KERNEL4x16_I1_L4_2  64,16, 0,0
    KERNEL4x16_I1_L4_3  64,16, 1,1
	MY_ALIGN	
LSGEMM_L4x16_SUB2_4:
    andi.      T10,L, 4
    ble LSGEMM_L4x16_SUB2_2
    LOAD4x16_0
    KERNEL4x16_I1_L4_3  64,16, 0,1
    MY_ALIGN
LSGEMM_L4x16_SUB2_2:
    andi.      T10,L, 2
    ble LSGEMM_L4x16_SUB2_1
    LOAD4x16_0
    KERNEL4x16_I1_L2_3  64,16, 0,1
    MY_ALIGN    
LSGEMM_L4x16_SUB2_1:
    andi.      T10,L, 1
    ble LSGEMM_L4x16_SAVE	
    KERNEL4x16 0
#	addic.		L,	L,	-1
#	bgt		LSGEMM_L4x16_SUB2

	MY_ALIGN
LSGEMM_L4x16_SAVE:
	SAVE4x16
#if defined(TRMMKERNEL)	
	REFRESH_AFTER_SAVE T11,K,TEMP_REG,BO,AO,16,4
#endif	
	addic.		I,	I,	-1
	bgt+		LSGEMM_L4x16_BEGIN
    MY_ALIGN
LSGEMM_L4x16_END:
LSGEMM_L4x8_BEGIN:
    andi.       T2, M,  15
    ble     LSGEMM_L4x1_END

    andi.       T1, M,  8
    ble     LSGEMM_L4x8_END

#if defined(TRMMKERNEL)
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,8,4
#else
    mr      BO, B
#endif  

#if defined(TRMMKERNEL)
   REFRESH_TEMP_BK T11,K,TEMP_REG,8,4
   mr T12, T11
   addi T12,T12, -1
   srawi.       L, T12, 4 /**(T11-1) % 16x */
#else
   mr T12, K
   addi T12,T12, -1
   srawi.       L,  T12,    4 /**(K-1) % 16x */
#endif 
    
    ZERO4x8
    ble     LSGEMM_L4x8_SUB0

    MY_ALIGN
LSGEMM_L4x8_LOOP_START:
 
    LOAD4x8_0  /*we already zeroed */ 
    mtctr       L

    MY_ALIGN

LSGEMM_L4x8_LOOP:

    KERNEL4x8_I1_L4_2  32,16, 0,0
    KERNEL4x8_I1_L4_2  32,16, 1,0
    KERNEL4x8_I1_L4_2  32,16, 2,0
    KERNEL4x8_I1_L4_2  32,16, 3,1    

    bdnz        LSGEMM_L4x8_LOOP

    MY_ALIGN
LSGEMM_L4x8_LOOP_END:

    END4x8 0, AO, BO, 32, 16    

    b       LSGEMM_L4x8_SUB1 
    MY_ALIGN
LSGEMM_L4x8_SUB0:
#if defined(TRMMKERNEL)
    andi.       L,  T11,    31
#else
    andi.       L,  K,  31
#endif   
    b       LSGEMM_L4x8_SUB2
    MY_ALIGN
LSGEMM_L4x8_SUB1:
#if defined(TRMMKERNEL)
    andi.       L,  T12,    15
#else
    andi.       L,  T12,    15
#endif  
    ble     LSGEMM_L4x8_SAVE
    MY_ALIGN
LSGEMM_L4x8_SUB2:
 
    srawi.      T1,L, 3
    ble LSGEMM_L4x8_SUB2_4 
    mtctr		T1
    MY_ALIGN
LSGEMM_L4x8_SUB2_LOOP:    
    LOAD4x8_0
    KERNEL4x8_I1_L4_2  32,16, 0,0
    KERNEL4x8_I1_L4_3  32,16, 1,1
    bdnz LSGEMM_L4x8_SUB2_LOOP
    MY_ALIGN    
LSGEMM_L4x8_SUB2_4:
    andi.      T1,L, 4
    ble LSGEMM_L4x8_SUB2_2
    LOAD4x8_0
    KERNEL4x8_I1_L4_3  32,16, 0,1
    MY_ALIGN
LSGEMM_L4x8_SUB2_2:
    andi.      T1,L, 2
    ble LSGEMM_L4x8_SUB2_1
    LOAD4x8_0
    KERNEL4x8_I1_L2_3  32,16, 0,1
    MY_ALIGN    
LSGEMM_L4x8_SUB2_1:
    andi.      T1,L, 1
    ble LSGEMM_L4x8_SAVE   
    KERNEL4x8 0
 

    MY_ALIGN
LSGEMM_L4x8_SAVE:
    SAVE4x8
#if defined(TRMMKERNEL) 
    REFRESH_AFTER_SAVE T11,K,TEMP_REG,BO,AO,8,4
#endif  
    MY_ALIGN  
LSGEMM_L4x8_END:
LSGEMM_L4x4_BEGIN:
    andi.       T2, M,  15
    ble     LSGEMM_L4x1_END

    andi.       T1, M,  4
    ble     LSGEMM_L4x4_END

#if defined(TRMMKERNEL)
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,4,4
#else
    mr      BO, B
#endif  

#if defined(TRMMKERNEL)
   REFRESH_TEMP_BK T11,K,TEMP_REG,4,4
   mr T12, T11
   addi T12,T12, -1
   srawi.       L, T12, 4 /**(T11-1) % 16x */
#else
   mr T12, K
   addi T12,T12, -1
   srawi.       L,  T12,    4 /**(K-1) % 16x */
#endif 
    
    ZERO4x4
    ble     LSGEMM_L4x4_SUB0

    MY_ALIGN
LSGEMM_L4x4_LOOP_START:
 
    LOAD4x4_0  /*we already zeroed */ 
    mtctr       L

    MY_ALIGN

LSGEMM_L4x4_LOOP:

    KERNEL4x4_I1_L4_2  16,16, 0,0
    KERNEL4x4_I1_L4_2  16,16, 1,0
    KERNEL4x4_I1_L4_2  16,16, 2,0
    KERNEL4x4_I1_L4_2  16,16, 3,1    

    bdnz        LSGEMM_L4x4_LOOP

    MY_ALIGN
LSGEMM_L4x4_LOOP_END:

    END4x4 0, AO, BO, 16, 16    

    b       LSGEMM_L4x4_SUB1 
    MY_ALIGN
LSGEMM_L4x4_SUB0:
#if defined(TRMMKERNEL)
    andi.       L,  T11,    31
#else
    andi.       L,  K,  31
#endif   
    b       LSGEMM_L4x4_SUB2
    MY_ALIGN
LSGEMM_L4x4_SUB1:
#if defined(TRMMKERNEL)
    andi.       L,  T12,    15
#else
    andi.       L,  T12,    15
#endif  
    ble     LSGEMM_L4x4_SAVE
    MY_ALIGN
LSGEMM_L4x4_SUB2:

    srawi.      T1,L, 3 
    ble LSGEMM_L4x4_SUB2_4  
    mtctr		T1
    MY_ALIGN
LSGEMM_L4x4_SUB2_LOOP:     
    LOAD4x4_0
    KERNEL4x4_I1_L4_2  16,16, 0,0
    KERNEL4x4_I1_L4_3  16,16, 1,1
    bdnz LSGEMM_L4x4_SUB2_LOOP
    MY_ALIGN    
LSGEMM_L4x4_SUB2_4:
    andi.      T1,L, 4
    ble LSGEMM_L4x4_SUB2_2
    LOAD4x4_0
    KERNEL4x4_I1_L4_3  16,16, 0,1
    MY_ALIGN
LSGEMM_L4x4_SUB2_2:
    andi.      T1,L, 2
    ble LSGEMM_L4x4_SUB2_1
    LOAD4x4_0
    KERNEL4x4_I1_L2_3  16,16, 0,1
    MY_ALIGN    
LSGEMM_L4x4_SUB2_1:
    andi.      T1,L, 1
    ble LSGEMM_L4x4_SAVE   
    KERNEL4x4 0
 

    MY_ALIGN
LSGEMM_L4x4_SAVE:
    SAVE4x4
#if defined(TRMMKERNEL) 
    REFRESH_AFTER_SAVE T11,K,TEMP_REG,BO,AO,4,4
#endif  
    MY_ALIGN  
LSGEMM_L4x4_END:
LSGEMM_L4x2_BEGIN:
    andi.       T1, M,  2
    ble     LSGEMM_L4x2_END

#if defined(TRMMKERNEL)
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,2,4
#else
    mr      BO, B
#endif  

#if defined(TRMMKERNEL)
   REFRESH_TEMP_BK T11,K,TEMP_REG,2,4 
   srawi.       L, T11, 3 /**(T11) % 8x */
#else
   srawi.       L,  K,    3 /**(K) % 8x */
#endif 
    
    ZERO4x2
    ble     LSGEMM_L4x2_SUB0

    MY_ALIGN
LSGEMM_L4x2_LOOP_START: 
    mtctr       L

    MY_ALIGN

LSGEMM_L4x2_LOOP:

    KERNEL4x2_2  0,0, 0,0
    KERNEL4x2_2  0,0, 1,0
    KERNEL4x2_2  0,0, 2,0
    KERNEL4x2_2  0,0, 3,1    

    bdnz        LSGEMM_L4x2_LOOP

    MY_ALIGN
LSGEMM_L4x2_LOOP_END:   
 
LSGEMM_L4x2_SUB0:
#if defined(TRMMKERNEL)
    andi.       L,  T11,    7
#else
    andi.       L,  K,  7
#endif    
    ble     LSGEMM_L4x2_SAVE
    MY_ALIGN
LSGEMM_L4x2_SUB2:
    andi.      T1,L, 4
    ble LSGEMM_L4x2_SUB2_2
    KERNEL4x2_2  0,0, 0,0
    KERNEL4x2_2  0,0, 1,1
    MY_ALIGN
LSGEMM_L4x2_SUB2_2:
    andi.      T1,L, 2
    ble LSGEMM_L4x2_SUB2_1
    KERNEL4x2_2  0,0, 0,1 
    MY_ALIGN    
LSGEMM_L4x2_SUB2_1:
    andi.      T1,L, 1
    ble LSGEMM_L4x2_SAVE   
    KERNEL4x2
  
    MY_ALIGN
LSGEMM_L4x2_SAVE:
    SAVE4x2
#if defined(TRMMKERNEL) 
    REFRESH_AFTER_SAVE T11,K,TEMP_REG,BO,AO,2,4
#endif  
    MY_ALIGN  
LSGEMM_L4x2_END:
LSGEMM_L4x1_BEGIN: 
    andi.       T1, M,  1
    ble     LSGEMM_L4x1_END
#if defined(TRMMKERNEL)
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,1,4
#else
    mr      BO, B
#endif  

#if defined(TRMMKERNEL)
   REFRESH_TEMP_BK T11,K,TEMP_REG,1,4 
   srawi.       L, T11, 3 /**(T11) % 8x */
#else
   srawi.       L,  K,    3 /**(K) % 8x */
#endif 
    
    ZERO4x1
    ble     LSGEMM_L4x1_SUB0

    MY_ALIGN
LSGEMM_L4x1_LOOP_START: 
    mtctr       L

    MY_ALIGN

LSGEMM_L4x1_LOOP:

    KERNEL4x1_4  0,0, 0,0
    KERNEL4x1_4  0,0, 1,1     

    bdnz        LSGEMM_L4x1_LOOP

    MY_ALIGN
LSGEMM_L4x1_LOOP_END:   
 
LSGEMM_L4x1_SUB0:
#if defined(TRMMKERNEL)
    andi.       L,  T11,    7
#else
    andi.       L,  K,  7
#endif    
    ble     LSGEMM_L4x1_SAVE
    MY_ALIGN
LSGEMM_L4x1_SUB2:
    andi.      T1,L, 4
    ble LSGEMM_L4x1_SUB2_2
    KERNEL4x1_4  0,0, 0,1 
    MY_ALIGN
LSGEMM_L4x1_SUB2_2:
    andi.      T1,L, 2
    ble LSGEMM_L4x1_SUB2_1
    KERNEL4x1_2 
    MY_ALIGN    
LSGEMM_L4x1_SUB2_1:
    andi.      T1,L, 1
    ble LSGEMM_L4x1_SAVE   
    KERNEL4x1
  
    MY_ALIGN
LSGEMM_L4x1_SAVE:
    SAVE4x1
#if defined(TRMMKERNEL) 
    REFRESH_AFTER_SAVE T11,K,TEMP_REG,BO,AO,1,4
#endif  
    MY_ALIGN  
LSGEMM_L4x1_END:

	slwi		T1,	K,	4
	add		B,	B,	T1
#if defined(TRMMKERNEL) && !defined(LEFT)
    addi TEMP_REG, TEMP_REG, 4
#endif

	andi.		T2,	N,	3
	ble		.L999

LSGEMM_L4_END:
    andi.       T1, N,  2
    ble     LSGEMM_L2_END
LSGEMM_L2_BEGIN:
  

	mr		AO,	A
	mr		CO,	C
	slwi		T3,	LDC	,	1
	add		C,	C,	T3
 
#if defined(TRMMKERNEL) && defined(LEFT)
	mr TEMP_REG, OFFSET	 /*off = offset;*/
#endif 
	srawi.		I,	M,	4
	ble		LSGEMM_L2x16_END

	MY_ALIGN
LSGEMM_L2x16_BEGIN:

#if defined(TRMMKERNEL)
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,16,2
#else
	mr		BO,	B
#endif	

#if defined(TRMMKERNEL)
   REFRESH_TEMP_BK T11,K,TEMP_REG,16,2 
   srawi.		L, T11,	6 /**(T11 ) % 64x */
#else 
   srawi.		L,	K,	6 /**(K ) % 64x */
#endif 
 
    ZERO2x16
	ble		LSGEMM_L2x16_SUB0
    addi AO,AO,2048
  
	mtctr		L

	MY_ALIGN

LSGEMM_L2x16_LOOP:

    KERNEL2x16_4  -2048,0, 0,0
    KERNEL2x16_4  -2048,0, 1,0
    KERNEL2x16_4  -2048,0, 2,0
    KERNEL2x16_4  -2048,0, 3,0
    KERNEL2x16_4  -2048,0, 4,0
    KERNEL2x16_4  -2048,0, 5,0        
    KERNEL2x16_4  -2048,0, 6,0
    KERNEL2x16_4  -2048,0, 7,0  
    KERNEL2x16_4  -2048,0, 8,0      
    KERNEL2x16_4  -2048,0, 9,0
    KERNEL2x16_4  -2048,0, 10,0
    KERNEL2x16_4  -2048,0, 11,0
    KERNEL2x16_4  -2048,0, 12,0
    KERNEL2x16_4  -2048,0, 13,0    
    KERNEL2x16_4  -2048,0, 14,0    
    KERNEL2x16_4  -2048,0, 15,1  	

	bdnz		LSGEMM_L2x16_LOOP
    MY_ALIGN
    addi AO,AO, -2048
	MY_ALIGN
LSGEMM_L2x16_SUB0: 
#if defined(TRMMKERNEL)
	andi.		L,	T11,	63
#else
	andi.		L,  K,	63
#endif	
	ble		LSGEMM_L2x16_SAVE
	MY_ALIGN
LSGEMM_L2x16_SUB2:
    andi.      T10,L, 32
    ble LSGEMM_L2x16_SUB2_16 
    KERNEL2x16_4  0,0, 0,0
    KERNEL2x16_4  0,0, 1,0
    KERNEL2x16_4  0,0, 2,0
    KERNEL2x16_4  0,0, 3,0
    KERNEL2x16_4  0,0, 4,0
    KERNEL2x16_4  0,0, 5,0
    KERNEL2x16_4  0,0, 6,0
    KERNEL2x16_4  0,0, 7,1 
    MY_ALIGN        
LSGEMM_L2x16_SUB2_16:
    andi.      T10,L, 16
    ble LSGEMM_L2x16_SUB2_8 
    KERNEL2x16_4  0,0, 0,0
    KERNEL2x16_4  0,0, 1,0
    KERNEL2x16_4  0,0, 2,0
    KERNEL2x16_4  0,0, 3,1
    MY_ALIGN 
LSGEMM_L2x16_SUB2_8:
    andi.      T10,L, 8
    ble LSGEMM_L2x16_SUB2_4  
    KERNEL2x16_4  0,0, 0,0
    KERNEL2x16_4  0,0, 1,1
	MY_ALIGN	
LSGEMM_L2x16_SUB2_4:
    andi.      T10,L, 4
    ble LSGEMM_L2x16_SUB2_2 
    KERNEL2x16_4  0,0, 0,1
    MY_ALIGN
LSGEMM_L2x16_SUB2_2:
    andi.      T10,L, 2
    ble LSGEMM_L2x16_SUB2_1 
    KERNEL2x16_2  0,0, 0,1
    MY_ALIGN    
LSGEMM_L2x16_SUB2_1:
    andi.      T10,L, 1
    ble LSGEMM_L2x16_SAVE	
    KERNEL2x16

	MY_ALIGN
LSGEMM_L2x16_SAVE:
	SAVE2x16
#if defined(TRMMKERNEL)	
	REFRESH_AFTER_SAVE T11,K,TEMP_REG,BO,AO,16,2
#endif	
	addic.		I,	I,	-1
	bgt+		LSGEMM_L2x16_BEGIN
    MY_ALIGN
LSGEMM_L2x16_END:
	andi.		I,	M,	8
	ble		LSGEMM_L2x8_END

	MY_ALIGN
LSGEMM_L2x8_BEGIN:

#if defined(TRMMKERNEL)
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,8,2
#else
	mr		BO,	B
#endif	

#if defined(TRMMKERNEL)
   REFRESH_TEMP_BK T11,K,TEMP_REG,8,2 
   srawi.		L, T11,	6 /**(T11 ) % 64x */
#else 
   srawi.		L,	K,	6 /**(K ) % 64x */
#endif 
 
    ZERO2x8
	ble		LSGEMM_L2x8_SUB0
    addi AO,AO,2048
  
	mtctr		L

	MY_ALIGN

LSGEMM_L2x8_LOOP:

    KERNEL2x8_4  -2048,0, 0,0
    KERNEL2x8_4  -2048,0, 1,0
    KERNEL2x8_4  -2048,0, 2,0
    KERNEL2x8_4  -2048,0, 3,0
    KERNEL2x8_4  -2048,0, 4,0
    KERNEL2x8_4  -2048,0, 5,0        
    KERNEL2x8_4  -2048,0, 6,0
    KERNEL2x8_4  -2048,0, 7,0  
    KERNEL2x8_4  -2048,0, 8,0      
    KERNEL2x8_4  -2048,0, 9,0
    KERNEL2x8_4  -2048,0, 10,0
    KERNEL2x8_4  -2048,0, 11,0
    KERNEL2x8_4  -2048,0, 12,0
    KERNEL2x8_4  -2048,0, 13,0    
    KERNEL2x8_4  -2048,0, 14,0    
    KERNEL2x8_4  -2048,0, 15,1  	

	bdnz		LSGEMM_L2x8_LOOP
    MY_ALIGN
    addi AO,AO, -2048
	MY_ALIGN
LSGEMM_L2x8_SUB0: 
#if defined(TRMMKERNEL)
	andi.		L,	T11,	63
#else
	andi.		L,  K,	63
#endif	
	ble		LSGEMM_L2x8_SAVE
	MY_ALIGN
LSGEMM_L2x8_SUB2:
    andi.      T10,L, 32
    ble LSGEMM_L2x8_SUB2_16 
    KERNEL2x8_4  0,0, 0,0
    KERNEL2x8_4  0,0, 1,0
    KERNEL2x8_4  0,0, 2,0
    KERNEL2x8_4  0,0, 3,0
    KERNEL2x8_4  0,0, 4,0
    KERNEL2x8_4  0,0, 5,0
    KERNEL2x8_4  0,0, 6,0
    KERNEL2x8_4  0,0, 7,1 
    MY_ALIGN        
LSGEMM_L2x8_SUB2_16:
    andi.      T10,L, 16
    ble LSGEMM_L2x8_SUB2_8 
    KERNEL2x8_4  0,0, 0,0
    KERNEL2x8_4  0,0, 1,0
    KERNEL2x8_4  0,0, 2,0
    KERNEL2x8_4  0,0, 3,1
    MY_ALIGN 
LSGEMM_L2x8_SUB2_8:
    andi.      T10,L, 8
    ble LSGEMM_L2x8_SUB2_4  
    KERNEL2x8_4  0,0, 0,0
    KERNEL2x8_4  0,0, 1,1
	MY_ALIGN	
LSGEMM_L2x8_SUB2_4:
    andi.      T10,L, 4
    ble LSGEMM_L2x8_SUB2_2 
    KERNEL2x8_4  0,0, 0,1
    MY_ALIGN
LSGEMM_L2x8_SUB2_2:
    andi.      T10,L, 2
    ble LSGEMM_L2x8_SUB2_1 
    KERNEL2x8_2  0,0, 0,1
    MY_ALIGN    
LSGEMM_L2x8_SUB2_1:
    andi.      T10,L, 1
    ble LSGEMM_L2x8_SAVE	
    KERNEL2x8

	MY_ALIGN
LSGEMM_L2x8_SAVE:
	SAVE2x8
#if defined(TRMMKERNEL)	
	REFRESH_AFTER_SAVE T11,K,TEMP_REG,BO,AO,8,2
#endif	 
    MY_ALIGN
LSGEMM_L2x8_END:
	andi.		I,	M,	4
	ble		LSGEMM_L2x4_END

	MY_ALIGN
LSGEMM_L2x4_BEGIN:

#if defined(TRMMKERNEL)
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,4,2
#else
	mr		BO,	B
#endif	

#if defined(TRMMKERNEL)
   REFRESH_TEMP_BK T11,K,TEMP_REG,4,2 
   srawi.		L, T11,	6 /**(T11 ) % 64x */
#else 
   srawi.		L,	K,	6 /**(K ) % 64x */
#endif 
 
    ZERO2x4
	ble		LSGEMM_L2x4_SUB0
 
  
	mtctr		L

	MY_ALIGN

LSGEMM_L2x4_LOOP:

    KERNEL2x4_4  0,0, 0,0
    KERNEL2x4_4  0,0, 1,0
    KERNEL2x4_4  0,0, 2,0
    KERNEL2x4_4  0,0, 3,0
    KERNEL2x4_4  0,0, 4,0
    KERNEL2x4_4  0,0, 5,0        
    KERNEL2x4_4  0,0, 6,0
    KERNEL2x4_4  0,0, 7,0  
    KERNEL2x4_4  0,0, 8,0      
    KERNEL2x4_4  0,0, 9,0
    KERNEL2x4_4  0,0, 10,0
    KERNEL2x4_4  0,0, 11,0
    KERNEL2x4_4  0,0, 12,0
    KERNEL2x4_4  0,0, 13,0    
    KERNEL2x4_4  0,0, 14,0    
    KERNEL2x4_4  0,0, 15,1  	

	bdnz		LSGEMM_L2x4_LOOP
    MY_ALIGN
 
	MY_ALIGN
LSGEMM_L2x4_SUB0: 
#if defined(TRMMKERNEL)
	andi.		L,	T11,	63
#else
	andi.		L,  K,	63
#endif	
	ble		LSGEMM_L2x4_SAVE
	MY_ALIGN
LSGEMM_L2x4_SUB2:
    andi.      T10,L, 32
    ble LSGEMM_L2x4_SUB2_16 
    KERNEL2x4_4  0,0, 0,0
    KERNEL2x4_4  0,0, 1,0
    KERNEL2x4_4  0,0, 2,0
    KERNEL2x4_4  0,0, 3,0
    KERNEL2x4_4  0,0, 4,0
    KERNEL2x4_4  0,0, 5,0
    KERNEL2x4_4  0,0, 6,0
    KERNEL2x4_4  0,0, 7,1 
    MY_ALIGN        
LSGEMM_L2x4_SUB2_16:
    andi.      T10,L, 16
    ble LSGEMM_L2x4_SUB2_8 
    KERNEL2x4_4  0,0, 0,0
    KERNEL2x4_4  0,0, 1,0
    KERNEL2x4_4  0,0, 2,0
    KERNEL2x4_4  0,0, 3,1
    MY_ALIGN 
LSGEMM_L2x4_SUB2_8:
    andi.      T10,L, 8
    ble LSGEMM_L2x4_SUB2_4  
    KERNEL2x4_4  0,0, 0,0
    KERNEL2x4_4  0,0, 1,1
	MY_ALIGN	
LSGEMM_L2x4_SUB2_4:
    andi.      T10,L, 4
    ble LSGEMM_L2x4_SUB2_2 
    KERNEL2x4_4  0,0, 0,1
    MY_ALIGN
LSGEMM_L2x4_SUB2_2:
    andi.      T10,L, 2
    ble LSGEMM_L2x4_SUB2_1 
    KERNEL2x4_2  0,0, 0,1
    MY_ALIGN    
LSGEMM_L2x4_SUB2_1:
    andi.      T10,L, 1
    ble LSGEMM_L2x4_SAVE	
    KERNEL2x4

	MY_ALIGN
LSGEMM_L2x4_SAVE:
	SAVE2x4
#if defined(TRMMKERNEL)	
	REFRESH_AFTER_SAVE T11,K,TEMP_REG,BO,AO,4,2
#endif	 
    MY_ALIGN
LSGEMM_L2x4_END:
	andi.		I,	M,	2
	ble		LSGEMM_L2x2_END

	MY_ALIGN
LSGEMM_L2x2_BEGIN:

#if defined(TRMMKERNEL)
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,2,2
#else
	mr		BO,	B
#endif	

#if defined(TRMMKERNEL)
   REFRESH_TEMP_BK T11,K,TEMP_REG,2,2 
   srawi.		L, T11,	6 /**(T11 ) % 64x */
#else 
   srawi.		L,	K,	6 /**(K ) % 64x */
#endif 
 
    ZERO2x2
	ble		LSGEMM_L2x2_SUB0
 
  
	mtctr		L

	MY_ALIGN

LSGEMM_L2x2_LOOP:

    KERNEL2x2_4  0,0, 0,0
    KERNEL2x2_4  0,0, 1,0
    KERNEL2x2_4  0,0, 2,0
    KERNEL2x2_4  0,0, 3,0
    KERNEL2x2_4  0,0, 4,0
    KERNEL2x2_4  0,0, 5,0        
    KERNEL2x2_4  0,0, 6,0
    KERNEL2x2_4  0,0, 7,0  
    KERNEL2x2_4  0,0, 8,0      
    KERNEL2x2_4  0,0, 9,0
    KERNEL2x2_4  0,0, 10,0
    KERNEL2x2_4  0,0, 11,0
    KERNEL2x2_4  0,0, 12,0
    KERNEL2x2_4  0,0, 13,0    
    KERNEL2x2_4  0,0, 14,0    
    KERNEL2x2_4  0,0, 15,1  	

	bdnz		LSGEMM_L2x2_LOOP
    MY_ALIGN
 
	MY_ALIGN
LSGEMM_L2x2_SUB0: 
#if defined(TRMMKERNEL)
	andi.		L,	T11,	63
#else
	andi.		L,  K,	63
#endif	
	ble		LSGEMM_L2x2_SAVE
	MY_ALIGN
LSGEMM_L2x2_SUB2:
    andi.      T10,L, 32
    ble LSGEMM_L2x2_SUB2_16 
    KERNEL2x2_4  0,0, 0,0
    KERNEL2x2_4  0,0, 1,0
    KERNEL2x2_4  0,0, 2,0
    KERNEL2x2_4  0,0, 3,0
    KERNEL2x2_4  0,0, 4,0
    KERNEL2x2_4  0,0, 5,0
    KERNEL2x2_4  0,0, 6,0
    KERNEL2x2_4  0,0, 7,1 
    MY_ALIGN        
LSGEMM_L2x2_SUB2_16:
    andi.      T10,L, 16
    ble LSGEMM_L2x2_SUB2_8 
    KERNEL2x2_4  0,0, 0,0
    KERNEL2x2_4  0,0, 1,0
    KERNEL2x2_4  0,0, 2,0
    KERNEL2x2_4  0,0, 3,1
    MY_ALIGN 
LSGEMM_L2x2_SUB2_8:
    andi.      T10,L, 8
    ble LSGEMM_L2x2_SUB2_4  
    KERNEL2x2_4  0,0, 0,0
    KERNEL2x2_4  0,0, 1,1
	MY_ALIGN	
LSGEMM_L2x2_SUB2_4:
    andi.      T10,L, 4
    ble LSGEMM_L2x2_SUB2_2 
    KERNEL2x2_4  0,0, 0,1
    MY_ALIGN
LSGEMM_L2x2_SUB2_2:
    andi.      T10,L, 2
    ble LSGEMM_L2x2_SUB2_1 
    KERNEL2x2_2  0,0, 0,1
    MY_ALIGN    
LSGEMM_L2x2_SUB2_1:
    andi.      T10,L, 1
    ble LSGEMM_L2x2_SAVE	
    KERNEL2x2

	MY_ALIGN
LSGEMM_L2x2_SAVE:
	SAVE2x2
#if defined(TRMMKERNEL)	
	REFRESH_AFTER_SAVE T11,K,TEMP_REG,BO,AO,2,2
#endif	 
    MY_ALIGN
LSGEMM_L2x2_END:
	andi.		I,	M,	1
	ble		LSGEMM_L2x1_END

	MY_ALIGN
LSGEMM_L2x1_BEGIN:

#if defined(TRMMKERNEL)
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,1,2
#else
	mr		BO,	B
#endif	

#if defined(TRMMKERNEL)
   REFRESH_TEMP_BK T11,K,TEMP_REG,1,2 
   srawi.		L, T11,	6 /**(T11 ) % 64x */
#else 
   srawi.		L,	K,	6 /**(K ) % 64x */
#endif 
 
    ZERO2x1
	ble		LSGEMM_L2x1_SUB0
 
  
	mtctr		L

	MY_ALIGN

LSGEMM_L2x1_LOOP:

    KERNEL2x1_4  0,0, 0,0
    KERNEL2x1_4  0,0, 1,0
    KERNEL2x1_4  0,0, 2,0
    KERNEL2x1_4  0,0, 3,0
    KERNEL2x1_4  0,0, 4,0
    KERNEL2x1_4  0,0, 5,0        
    KERNEL2x1_4  0,0, 6,0
    KERNEL2x1_4  0,0, 7,0  
    KERNEL2x1_4  0,0, 8,0      
    KERNEL2x1_4  0,0, 9,0
    KERNEL2x1_4  0,0, 10,0
    KERNEL2x1_4  0,0, 11,0
    KERNEL2x1_4  0,0, 12,0
    KERNEL2x1_4  0,0, 13,0    
    KERNEL2x1_4  0,0, 14,0    
    KERNEL2x1_4  0,0, 15,1  	

	bdnz		LSGEMM_L2x1_LOOP
    MY_ALIGN
 
	MY_ALIGN
LSGEMM_L2x1_SUB0: 
#if defined(TRMMKERNEL)
	andi.		L,	T11,	63
#else
	andi.		L,  K,	63
#endif	
	ble		LSGEMM_L2x1_SAVE
	MY_ALIGN
LSGEMM_L2x1_SUB2:
    andi.      T10,L, 32
    ble LSGEMM_L2x1_SUB2_16 
    KERNEL2x1_4  0,0, 0,0
    KERNEL2x1_4  0,0, 1,0
    KERNEL2x1_4  0,0, 2,0
    KERNEL2x1_4  0,0, 3,0
    KERNEL2x1_4  0,0, 4,0
    KERNEL2x1_4  0,0, 5,0
    KERNEL2x1_4  0,0, 6,0
    KERNEL2x1_4  0,0, 7,1 
    MY_ALIGN        
LSGEMM_L2x1_SUB2_16:
    andi.      T10,L, 16
    ble LSGEMM_L2x1_SUB2_8 
    KERNEL2x1_4  0,0, 0,0
    KERNEL2x1_4  0,0, 1,0
    KERNEL2x1_4  0,0, 2,0
    KERNEL2x1_4  0,0, 3,1
    MY_ALIGN 
LSGEMM_L2x1_SUB2_8:
    andi.      T10,L, 8
    ble LSGEMM_L2x1_SUB2_4  
    KERNEL2x1_4  0,0, 0,0
    KERNEL2x1_4  0,0, 1,1
	MY_ALIGN	
LSGEMM_L2x1_SUB2_4:
    andi.      T10,L, 4
    ble LSGEMM_L2x1_SUB2_2 
    KERNEL2x1_4  0,0, 0,1
    MY_ALIGN
LSGEMM_L2x1_SUB2_2:
    andi.      T10,L, 2
    ble LSGEMM_L2x1_SUB2_1 
    KERNEL2x1_2  0,0, 0,1
    MY_ALIGN    
LSGEMM_L2x1_SUB2_1:
    andi.      T10,L, 1
    ble LSGEMM_L2x1_SAVE	
    KERNEL2x1

	MY_ALIGN
LSGEMM_L2x1_SAVE:
	SAVE2x1
#if defined(TRMMKERNEL)	
	REFRESH_AFTER_SAVE T11,K,TEMP_REG,BO,AO,1,2
#endif	 
    MY_ALIGN
LSGEMM_L2x1_END:
	slwi		T1,	K,	3
	add		B,	B,	T1
#if defined(TRMMKERNEL) && !defined(LEFT)
    addi TEMP_REG, TEMP_REG, 2
#endif 
LSGEMM_L2_END:
   andi.       T1, N,  1
   ble     LSGEMM_END
LSGEMM_1_BEGIN:
  

	mr		AO,	A
	mr		CO,	C 
	add		C,	C,	LDC
 
#if defined(TRMMKERNEL) && defined(LEFT)
	mr TEMP_REG, OFFSET	 /*off = offset;*/
#endif 
	srawi.		I,	M,	4
	ble		LSGEMM_1x16_END

	MY_ALIGN
LSGEMM_1x16_BEGIN:

#if defined(TRMMKERNEL)
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,16,1
#else
	mr		BO,	B
#endif	

#if defined(TRMMKERNEL)
   REFRESH_TEMP_BK T11,K,TEMP_REG,16,1 
   srawi.		L, T11,	6 /**(T11 ) % 64x */
#else 
   srawi.		L,	K,	6 /**(K ) % 64x */
#endif 
 
    ZERO1x16
	ble		LSGEMM_1x16_SUB0
    addi AO,AO,2048
  
	mtctr		L

	MY_ALIGN

LSGEMM_1x16_LOOP:

    KERNEL1x16_4  -2048,0, 0,0
    KERNEL1x16_4  -2048,0, 1,0
    KERNEL1x16_4  -2048,0, 2,0
    KERNEL1x16_4  -2048,0, 3,0
    KERNEL1x16_4  -2048,0, 4,0
    KERNEL1x16_4  -2048,0, 5,0        
    KERNEL1x16_4  -2048,0, 6,0
    KERNEL1x16_4  -2048,0, 7,0  
    KERNEL1x16_4  -2048,0, 8,0      
    KERNEL1x16_4  -2048,0, 9,0
    KERNEL1x16_4  -2048,0, 10,0
    KERNEL1x16_4  -2048,0, 11,0
    KERNEL1x16_4  -2048,0, 12,0
    KERNEL1x16_4  -2048,0, 13,0    
    KERNEL1x16_4  -2048,0, 14,0    
    KERNEL1x16_4  -2048,0, 15,1  	

	bdnz		LSGEMM_1x16_LOOP
    MY_ALIGN
    addi AO,AO, -2048
	MY_ALIGN
LSGEMM_1x16_SUB0: 
#if defined(TRMMKERNEL)
	andi.		L,	T11,	63
#else
	andi.		L,  K,	63
#endif	
	ble		LSGEMM_1x16_SAVE
	MY_ALIGN
LSGEMM_1x16_SUB2:
    andi.      T10,L, 32
    ble LSGEMM_1x16_SUB2_16 
    KERNEL1x16_4  0,0, 0,0
    KERNEL1x16_4  0,0, 1,0
    KERNEL1x16_4  0,0, 2,0
    KERNEL1x16_4  0,0, 3,0
    KERNEL1x16_4  0,0, 4,0
    KERNEL1x16_4  0,0, 5,0
    KERNEL1x16_4  0,0, 6,0
    KERNEL1x16_4  0,0, 7,1 
    MY_ALIGN        
LSGEMM_1x16_SUB2_16:
    andi.      T10,L, 16
    ble LSGEMM_1x16_SUB2_8 
    KERNEL1x16_4  0,0, 0,0
    KERNEL1x16_4  0,0, 1,0
    KERNEL1x16_4  0,0, 2,0
    KERNEL1x16_4  0,0, 3,1
    MY_ALIGN 
LSGEMM_1x16_SUB2_8:
    andi.      T10,L, 8
    ble LSGEMM_1x16_SUB2_4  
    KERNEL1x16_4  0,0, 0,0
    KERNEL1x16_4  0,0, 1,1
	MY_ALIGN	
LSGEMM_1x16_SUB2_4:
    andi.      T10,L, 4
    ble LSGEMM_1x16_SUB2_2 
    KERNEL1x16_4  0,0, 0,1
    MY_ALIGN
LSGEMM_1x16_SUB2_2:
    andi.      T10,L, 2
    ble LSGEMM_1x16_SUB2_1 
    KERNEL1x16_2  0,0, 0,1
    MY_ALIGN    
LSGEMM_1x16_SUB2_1:
    andi.      T10,L, 1
    ble LSGEMM_1x16_SAVE	
    KERNEL1x16

	MY_ALIGN
LSGEMM_1x16_SAVE:
	SAVE1x16
#if defined(TRMMKERNEL)	
	REFRESH_AFTER_SAVE T11,K,TEMP_REG,BO,AO,16,1
#endif	
	addic.		I,	I,	-1
	bgt+		LSGEMM_1x16_BEGIN
    MY_ALIGN
LSGEMM_1x16_END:
	andi.		I,	M,	8
	ble		LSGEMM_1x8_END

	MY_ALIGN
LSGEMM_1x8_BEGIN:

#if defined(TRMMKERNEL)
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,8,1
#else
	mr		BO,	B
#endif	

#if defined(TRMMKERNEL)
   REFRESH_TEMP_BK T11,K,TEMP_REG,8,1 
   srawi.		L, T11,	6 /**(T11 ) % 64x */
#else 
   srawi.		L,	K,	6 /**(K ) % 64x */
#endif 
 
    ZERO1x8
	ble		LSGEMM_1x8_SUB0
    addi AO,AO,2048
  
	mtctr		L

	MY_ALIGN

LSGEMM_1x8_LOOP:

    KERNEL1x8_4  -2048,0, 0,0
    KERNEL1x8_4  -2048,0, 1,0
    KERNEL1x8_4  -2048,0, 2,0
    KERNEL1x8_4  -2048,0, 3,0
    KERNEL1x8_4  -2048,0, 4,0
    KERNEL1x8_4  -2048,0, 5,0        
    KERNEL1x8_4  -2048,0, 6,0
    KERNEL1x8_4  -2048,0, 7,0  
    KERNEL1x8_4  -2048,0, 8,0      
    KERNEL1x8_4  -2048,0, 9,0
    KERNEL1x8_4  -2048,0, 10,0
    KERNEL1x8_4  -2048,0, 11,0
    KERNEL1x8_4  -2048,0, 12,0
    KERNEL1x8_4  -2048,0, 13,0    
    KERNEL1x8_4  -2048,0, 14,0    
    KERNEL1x8_4  -2048,0, 15,1  	

	bdnz		LSGEMM_1x8_LOOP
    MY_ALIGN
    addi AO,AO, -2048
	MY_ALIGN
LSGEMM_1x8_SUB0: 
#if defined(TRMMKERNEL)
	andi.		L,	T11,	63
#else
	andi.		L,  K,	63
#endif	
	ble		LSGEMM_1x8_SAVE
	MY_ALIGN
LSGEMM_1x8_SUB2:
    andi.      T10,L, 32
    ble LSGEMM_1x8_SUB2_16 
    KERNEL1x8_4  0,0, 0,0
    KERNEL1x8_4  0,0, 1,0
    KERNEL1x8_4  0,0, 2,0
    KERNEL1x8_4  0,0, 3,0
    KERNEL1x8_4  0,0, 4,0
    KERNEL1x8_4  0,0, 5,0
    KERNEL1x8_4  0,0, 6,0
    KERNEL1x8_4  0,0, 7,1 
    MY_ALIGN        
LSGEMM_1x8_SUB2_16:
    andi.      T10,L, 16
    ble LSGEMM_1x8_SUB2_8 
    KERNEL1x8_4  0,0, 0,0
    KERNEL1x8_4  0,0, 1,0
    KERNEL1x8_4  0,0, 2,0
    KERNEL1x8_4  0,0, 3,1
    MY_ALIGN 
LSGEMM_1x8_SUB2_8:
    andi.      T10,L, 8
    ble LSGEMM_1x8_SUB2_4  
    KERNEL1x8_4  0,0, 0,0
    KERNEL1x8_4  0,0, 1,1
	MY_ALIGN	
LSGEMM_1x8_SUB2_4:
    andi.      T10,L, 4
    ble LSGEMM_1x8_SUB2_2 
    KERNEL1x8_4  0,0, 0,1
    MY_ALIGN
LSGEMM_1x8_SUB2_2:
    andi.      T10,L, 2
    ble LSGEMM_1x8_SUB2_1 
    KERNEL1x8_2  0,0, 0,1
    MY_ALIGN    
LSGEMM_1x8_SUB2_1:
    andi.      T10,L, 1
    ble LSGEMM_1x8_SAVE	
    KERNEL1x8

	MY_ALIGN
LSGEMM_1x8_SAVE:
	SAVE1x8
#if defined(TRMMKERNEL)	
	REFRESH_AFTER_SAVE T11,K,TEMP_REG,BO,AO,8,1
#endif	 
    MY_ALIGN
LSGEMM_1x8_END:
	andi.		I,	M,	4
	ble		LSGEMM_1x4_END

	MY_ALIGN
LSGEMM_1x4_BEGIN:

#if defined(TRMMKERNEL)
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,4,1
#else
	mr		BO,	B
#endif	

#if defined(TRMMKERNEL)
   REFRESH_TEMP_BK T11,K,TEMP_REG,4,1 
   srawi.		L, T11,	6 /**(T11 ) % 64x */
#else 
   srawi.		L,	K,	6 /**(K ) % 64x */
#endif 
 
    ZERO1x4
	ble		LSGEMM_1x4_SUB0
 
  
	mtctr		L

	MY_ALIGN

LSGEMM_1x4_LOOP:

    KERNEL1x4_4  0,0, 0,0
    KERNEL1x4_4  0,0, 1,0
    KERNEL1x4_4  0,0, 2,0
    KERNEL1x4_4  0,0, 3,0
    KERNEL1x4_4  0,0, 4,0
    KERNEL1x4_4  0,0, 5,0        
    KERNEL1x4_4  0,0, 6,0
    KERNEL1x4_4  0,0, 7,0  
    KERNEL1x4_4  0,0, 8,0      
    KERNEL1x4_4  0,0, 9,0
    KERNEL1x4_4  0,0, 10,0
    KERNEL1x4_4  0,0, 11,0
    KERNEL1x4_4  0,0, 12,0
    KERNEL1x4_4  0,0, 13,0    
    KERNEL1x4_4  0,0, 14,0    
    KERNEL1x4_4  0,0, 15,1  	

	bdnz		LSGEMM_1x4_LOOP
    MY_ALIGN
 
	MY_ALIGN
LSGEMM_1x4_SUB0: 
#if defined(TRMMKERNEL)
	andi.		L,	T11,	63
#else
	andi.		L,  K,	63
#endif	
	ble		LSGEMM_1x4_SAVE
	MY_ALIGN
LSGEMM_1x4_SUB2:
    andi.      T10,L, 32
    ble LSGEMM_1x4_SUB2_16 
    KERNEL1x4_4  0,0, 0,0
    KERNEL1x4_4  0,0, 1,0
    KERNEL1x4_4  0,0, 2,0
    KERNEL1x4_4  0,0, 3,0
    KERNEL1x4_4  0,0, 4,0
    KERNEL1x4_4  0,0, 5,0
    KERNEL1x4_4  0,0, 6,0
    KERNEL1x4_4  0,0, 7,1 
    MY_ALIGN        
LSGEMM_1x4_SUB2_16:
    andi.      T10,L, 16
    ble LSGEMM_1x4_SUB2_8 
    KERNEL1x4_4  0,0, 0,0
    KERNEL1x4_4  0,0, 1,0
    KERNEL1x4_4  0,0, 2,0
    KERNEL1x4_4  0,0, 3,1
    MY_ALIGN 
LSGEMM_1x4_SUB2_8:
    andi.      T10,L, 8
    ble LSGEMM_1x4_SUB2_4  
    KERNEL1x4_4  0,0, 0,0
    KERNEL1x4_4  0,0, 1,1
	MY_ALIGN	
LSGEMM_1x4_SUB2_4:
    andi.      T10,L, 4
    ble LSGEMM_1x4_SUB2_2 
    KERNEL1x4_4  0,0, 0,1
    MY_ALIGN
LSGEMM_1x4_SUB2_2:
    andi.      T10,L, 2
    ble LSGEMM_1x4_SUB2_1 
    KERNEL1x4_2  0,0, 0,1
    MY_ALIGN    
LSGEMM_1x4_SUB2_1:
    andi.      T10,L, 1
    ble LSGEMM_1x4_SAVE	
    KERNEL1x4

	MY_ALIGN
LSGEMM_1x4_SAVE:
	SAVE1x4
#if defined(TRMMKERNEL)	
	REFRESH_AFTER_SAVE T11,K,TEMP_REG,BO,AO,4,1
#endif	 
    MY_ALIGN
LSGEMM_1x4_END:
	andi.		I,	M,	2
	ble		LSGEMM_1x2_END

	MY_ALIGN
LSGEMM_1x2_BEGIN:

#if defined(TRMMKERNEL)
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,2,1
#else
	mr		BO,	B
#endif	

#if defined(TRMMKERNEL)
   REFRESH_TEMP_BK T11,K,TEMP_REG,2,1 
   srawi.		L, T11,	6 /**(T11 ) % 64x */
#else 
   srawi.		L,	K,	6 /**(K ) % 64x */
#endif 
 
    ZERO1x2
	ble		LSGEMM_1x2_SUB0
 
  
	mtctr		L

	MY_ALIGN

LSGEMM_1x2_LOOP:

    KERNEL1x2_4  0,0, 0,0
    KERNEL1x2_4  0,0, 1,0
    KERNEL1x2_4  0,0, 2,0
    KERNEL1x2_4  0,0, 3,0
    KERNEL1x2_4  0,0, 4,0
    KERNEL1x2_4  0,0, 5,0        
    KERNEL1x2_4  0,0, 6,0
    KERNEL1x2_4  0,0, 7,0  
    KERNEL1x2_4  0,0, 8,0      
    KERNEL1x2_4  0,0, 9,0
    KERNEL1x2_4  0,0, 10,0
    KERNEL1x2_4  0,0, 11,0
    KERNEL1x2_4  0,0, 12,0
    KERNEL1x2_4  0,0, 13,0    
    KERNEL1x2_4  0,0, 14,0    
    KERNEL1x2_4  0,0, 15,1  	

	bdnz		LSGEMM_1x2_LOOP
    MY_ALIGN
 
	MY_ALIGN
LSGEMM_1x2_SUB0: 
#if defined(TRMMKERNEL)
	andi.		L,	T11,	63
#else
	andi.		L,  K,	63
#endif	
	ble		LSGEMM_1x2_SAVE
	MY_ALIGN
LSGEMM_1x2_SUB2:
    andi.      T10,L, 32
    ble LSGEMM_1x2_SUB2_16 
    KERNEL1x2_4  0,0, 0,0
    KERNEL1x2_4  0,0, 1,0
    KERNEL1x2_4  0,0, 2,0
    KERNEL1x2_4  0,0, 3,0
    KERNEL1x2_4  0,0, 4,0
    KERNEL1x2_4  0,0, 5,0
    KERNEL1x2_4  0,0, 6,0
    KERNEL1x2_4  0,0, 7,1 
    MY_ALIGN        
LSGEMM_1x2_SUB2_16:
    andi.      T10,L, 16
    ble LSGEMM_1x2_SUB2_8 
    KERNEL1x2_4  0,0, 0,0
    KERNEL1x2_4  0,0, 1,0
    KERNEL1x2_4  0,0, 2,0
    KERNEL1x2_4  0,0, 3,1
    MY_ALIGN 
LSGEMM_1x2_SUB2_8:
    andi.      T10,L, 8
    ble LSGEMM_1x2_SUB2_4  
    KERNEL1x2_4  0,0, 0,0
    KERNEL1x2_4  0,0, 1,1
	MY_ALIGN	
LSGEMM_1x2_SUB2_4:
    andi.      T10,L, 4
    ble LSGEMM_1x2_SUB2_2 
    KERNEL1x2_4  0,0, 0,1
    MY_ALIGN
LSGEMM_1x2_SUB2_2:
    andi.      T10,L, 2
    ble LSGEMM_1x2_SUB2_1 
    KERNEL1x2_2  0,0, 0,1
    MY_ALIGN    
LSGEMM_1x2_SUB2_1:
    andi.      T10,L, 1
    ble LSGEMM_1x2_SAVE	
    KERNEL1x2

	MY_ALIGN
LSGEMM_1x2_SAVE:
	SAVE1x2
#if defined(TRMMKERNEL)	
	REFRESH_AFTER_SAVE T11,K,TEMP_REG,BO,AO,2,1
#endif	 
    MY_ALIGN
LSGEMM_1x2_END:
    andi.		I,	M,	1
	ble		LSGEMM_1x1_END

	MY_ALIGN
LSGEMM_1x1_BEGIN:

#if defined(TRMMKERNEL)
    REFRESH_POINTERS  AO,BO,TEMP_REG,B,1,1
#else
	mr		BO,	B
#endif	

#if defined(TRMMKERNEL)
   REFRESH_TEMP_BK T11,K,TEMP_REG,1,1 
   srawi.		L, T11,	6 /**(T11 ) % 64x */
#else 
   srawi.		L,	K,	6 /**(K ) % 64x */
#endif 
 
    ZERO1x1
	ble		LSGEMM_1x1_SUB0
 
  
	mtctr		L

	MY_ALIGN

LSGEMM_1x1_LOOP:

    KERNEL1x1_16  0,0, 0,0
    KERNEL1x1_16  0,0, 1,0
    KERNEL1x1_16  0,0, 2,0
    KERNEL1x1_16  0,0, 3,1 	

	bdnz		LSGEMM_1x1_LOOP
    MY_ALIGN
 
	MY_ALIGN
LSGEMM_1x1_SUB0: 
#if defined(TRMMKERNEL)
	andi.		L,	T11,	63
#else
	andi.		L,  K,	63
#endif	
	ble		LSGEMM_1x1_SAVE
	MY_ALIGN
LSGEMM_1x1_SUB2:
    andi.      T10,L, 32
    ble LSGEMM_1x1_SUB2_16 
    KERNEL1x1_16  0,0, 0,0
    KERNEL1x1_16  0,0, 1,1 
    MY_ALIGN        
LSGEMM_1x1_SUB2_16:
    andi.      T10,L, 16
    ble LSGEMM_1x1_SUB2_8 
    KERNEL1x1_16  0,0, 0,1
    MY_ALIGN 
LSGEMM_1x1_SUB2_8:
    andi.      T10,L, 8
    ble LSGEMM_1x1_SUB2_4  
    KERNEL1x1_8  0,0, 0,1
	MY_ALIGN	
LSGEMM_1x1_SUB2_4:
    andi.      T10,L, 4
    ble LSGEMM_1x1_SUB2_2 
    KERNEL1x1_4  0,0, 0,1
    MY_ALIGN
LSGEMM_1x1_SUB2_2:
    andi.      T10,L, 2
    ble LSGEMM_1x1_SUB2_1 
    KERNEL1x1_2  0,0, 0,1
    MY_ALIGN    
LSGEMM_1x1_SUB2_1:
    andi.      T10,L, 1
    ble LSGEMM_1x1_SAVE	
    KERNEL1x1

	MY_ALIGN
LSGEMM_1x1_SAVE:
	SAVE1x1
#if defined(TRMMKERNEL)	
	REFRESH_AFTER_SAVE T11,K,TEMP_REG,BO,AO,1,1
#endif	 
    MY_ALIGN
LSGEMM_1x1_END:
	slwi		T1,	K,	2
	add		B,	B,	T1
#if defined(TRMMKERNEL) && !defined(LEFT)
    addi TEMP_REG, TEMP_REG, 1
#endif 
LSGEMM_END: