/*
 Copyright (c) 2025 Qualcomm Innovation Center, Inc. All rights reserved.
 SPDX-License-Identifier: BSD-3-Clause-Clear
*/

/*----------------------------------------------------------------------------
 * This function is used to re-arrange the elements of input matrix to
 * make it suitable for matrix outer product computation using SME for matrix
 * multiplication. It should be used to pre-process the leftmatrix(A) in the
 * matrix muliplication (C= A*B) using sgemm_direct_sme1_2VLx2VL()
 *
 * The pre-processing transposes a block of SVLs rows of the input matrix and
 * stores it contiguously. The same is applied to remaining blocks of SVLs
 * rows. The last block of SVLs rows is zero-padded to SVLs rows if needed.
 *  
 * Usage of function:
 *  sgemm_direct_sme1_preprocess(uint64_t nrow, uint64_t ncol, \
 *                               const float * restrict mat, float * mat_mod);
 *
 ----------------------------------------------------------------------------*/


#define nrow x0              //Number of rows of input matrix
#define ncol x1              //Number of coulumns of input matrix
#define mat x2               //Input matrix base address
#define mat_mod x3           //Output matrix (re-arranged matrix) base address
#define mat_mod_ptr x4       //Pointer to output matrix
#define mat_ptr0 x5          //Pointer to input matrix
#define mat_ptr1 x6          //2nd pointer to input matrix
#define outer_loop_cntr x7   //Outer loop counter
#define inner_loop_exit x8   //Inner loop exit condition
#define C1 x9                //Constant1: SVLs - No. of 32-bit elements
#define C2 x10               //Constant2: 3*SVLs
#define C3 x11               //Constant3: ncol*SVLs
#define C4 x13               //Constant4: 2*SVLs
#define C5 x14               //Constant5: 2*ncol
#define C6 x15               //Constant6: 3*ncol

    .text
    .global sgemm_direct_sme1_preprocess

    sgemm_direct_sme1_preprocess:

    stp     x19, x20, [sp, #-48]!
    stp     x21, x22, [sp, #16]
    stp     x23, x24, [sp, #32]

    smstart

    cntw    C1                    //SVLs
    mul     C3, C1, ncol          //SVLs*ncol
    lsl     C5, ncol, #1          //2*ncol
    add     C6, C5, ncol          //3*ncol
    cnth    C4                    //2*SVLs
    add     C2, C1, C1, lsl #1    //3*SVLs

    mov     outer_loop_cntr, #0
    //Tile predicate (M dimension)
    whilelt p0.s, outer_loop_cntr, nrow
    //Predicate for stores
    ptrue   p9.s

.M_Loop:
    mov     mat_ptr0, mat                       //Load base address of mat
    mov     mat_mod_ptr, mat_mod                //a_mod store base address
    add     inner_loop_exit,  mat, ncol, lsl #2 //Exit condition for inner loop
    whilelt p8.b, mat_ptr0, inner_loop_exit     //Tile predicate (K dimension)

.Loop_process:
    mov     mat_ptr1, mat_ptr0
    //Load_to_tile loop counter
    mov     w12, #0

.Load_to_tile:
    psel    p2, p8, p0.s[w12, 0]
    psel    p3, p8, p0.s[w12, 1]
    psel    p4, p8, p0.s[w12, 2]
    psel    p5, p8, p0.s[w12, 3]
    //Load 1st row from mat_ptr1
    ld1w    {za0h.s[w12, #0]},  p2/z,   [mat_ptr1]
    //Load 2nd row from mat_ptr1 + ncol
    ld1w    {za0h.s[w12, #1]},  p3/z,   [mat_ptr1, ncol,  lsl #2]
    //Load 3rd row from mat_ptr1 + 2*ncol
    ld1w    {za0h.s[w12, #2]},  p4/z,   [mat_ptr1, C5, lsl #2]
    //Load 4th row from mat_ptr1 + 3*ncol
    ld1w    {za0h.s[w12, #3]},  p5/z,   [mat_ptr1, C6, lsl #2]
    //mat_ptr1+=4*ncol FP32 elements
    add     mat_ptr1, mat_ptr1, ncol, lsl #4
    //Increment counter
    add     w12, w12, #4
    cmp     w12, w9
    b.mi    .Load_to_tile
    // Store_from_tile loop counter
    mov     w12, #0

.Store_from_tile:
    psel    p2, p9, p8.s[w12, 0]
    psel    p3, p9, p8.s[w12, 1]
    psel    p4, p9, p8.s[w12, 2]
    psel    p5, p9, p8.s[w12, 3]
    //Store 1st col to mat_mod
    st1w    {za0v.s[w12, #0]},  p2,   [mat_mod_ptr]
    //Store 2nd col to mat_mod + SVLs
    st1w    {za0v.s[w12, #1]},  p3,   [mat_mod_ptr, C1,  lsl #2]
    //Store 3rd col to mat_mod + 2*SVLs
    st1w    {za0v.s[w12, #2]},  p4,   [mat_mod_ptr, C4, lsl #2]
    //Store 4th col to mat_mod + 3*SVLs
    st1w    {za0v.s[w12, #3]},  p5,   [mat_mod_ptr, C2, lsl #2]

    addvl   mat_mod_ptr, mat_mod_ptr, #4    //mat_mod_ptr += 4*SVLb
    add     w12, w12, #4                    //Increment counter
    cmp     w12, w9
    b.mi    .Store_from_tile

    addvl   mat_ptr0, mat_ptr0, #1          //mat_ptr0 += SVLb
    whilelt p8.b, mat_ptr0, inner_loop_exit
    b.first .Loop_process

    add     mat_mod, mat_mod, C3, lsl #2    //mat_mod+=SVLs*nbc FP32 elements
    add     mat, mat, C3, lsl #2            //mat+=SVLs*nbc FP32 elements
    incw    outer_loop_cntr

    whilelt p0.s, outer_loop_cntr, nrow
    b.first .M_Loop

    smstop

    ldp     x23, x24, [sp, #32]
    ldp     x21, x22, [sp, #16]
    ldp     x19, x20, [sp], #48

    ret

