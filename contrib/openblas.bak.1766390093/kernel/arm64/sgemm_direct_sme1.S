/*
 Copyright (c) 2025 Qualcomm Innovation Center, Inc. All rights reserved.
 SPDX-License-Identifier: BSD-3-Clause-Clear
*/

/*--------------------------------------------------------------------------
 * SME1 based Matrix multiplication code for FP32 input matrices to FP32
 * output matrix
 * C = A*B
 * A: Left input matrix of dimension M x K
 * B: Right input matrix of dimension K x N
 * C: Result matrix of dimension M x N
 *
 * Usage of function:
 *  sgemm_direct_sme1_2VLx2VL( uint64_t M , uint64_t K, uint64_t N,\
                               const float * restrict A_base,\
                               const float * restrict B_base,\
                               const float * restrict C_base);
----------------------------------------------------------------------------*/

#define M            x0  //M dimension
#define K            x1  //K dimension
#define N            x2  //N dimension
#define A_base       x3  //Pointer to left matrix(A)
#define B_base       x4  //Pointer to right matrix(B)
#define C_base       x5  //Pointer to result matrix(C)
#define Aptr         x6  //Pointer to traverse A
#define Aptr_end     x7  //Pointer to end of row of A
#define Cptr         x8  //Pointer to traverse C
#define Cptr0        x9  //2nd Pointer to traverse C
#define Cptr1        x10 //3rd Pointer to traverse C
#define Bptr         x11 //Pointer to traverse B
#define Bptr0        x12 //2nd Pointer to traverse B
#define N_exit       x14 //Exit condition for N loop
#define K_exit       x15 //Exit condition for K loop
#define M_cntr       x16 //M loop counter
#define C1           x17 //Constant1: N*(SVLs+1);SVLs-No. of 32-bit elements
#define C2           x18 //Constant2: N + SVLs
#define C3           x19 //Constant3: K*SVLs + SVLs
#define C4           x20 //Constant4: SVLs-2
#define C5           x21 //Constant5: K*SVLs
#define C6           x22 //Constant6: N*SVLs

    .text
    .global sgemm_direct_sme1_2VLx2VL

    sgemm_direct_sme1_2VLx2VL:

    stp     x19, x20, [sp, #-48]!
    stp     x21, x22, [sp, #16]
    stp     x23, x24, [sp, #32]

    smstart

    cntw    C4                         //SVLs
    mul     C5, C4, K                  //K*SVLs
    mul     C6, C4, N                  //N*SVLs
    add     C1, C6, N                  //N*SVLs + N
    add     N_exit, B_base, N, lsl #2  //N_Loop exit conditon
    mov     M_cntr, #0
    add     C2, N, C4                  //N + SVLs
    add     C3, C5, C4                 //K*SVLs + SVLs
    whilelt p2.s, M_cntr, M            //Tile 0,1 predicate (M dimension)
    sub     w20, w20, #2               //SVLs-2

.M_Loop:
    incw    M_cntr
    whilelt p3.s, M_cntr, M            //Tile 2,3 predicate (M dimension)
    mov     Bptr, B_base               //B_base
    mov     Cptr, C_base               //C_base
    whilelt p0.b, Bptr, N_exit         //Tile 0/2 predicate (N dimension)

.N_Loop:
    mov     Aptr, A_base               //Aptr = A_base
    mov     Bptr0, Bptr                //Bptr = B_base
    mov     Cptr0, Cptr                //Cptr0 = C_base
    addvl   Cptr1, Cptr, #1            //Cptr1 = C_base + SVLb
    addvl   Bptr, Bptr, #1
    whilelt p1.b, Bptr, N_exit             //Tile 1,3 predicate (N dimension)
    add     Aptr_end, A_base, C5, lsl #2   //A_base + K*SVLs
    addvl   K_exit, Aptr_end, #-1          //Exit condition for K loop
    //Load 1st vector from Aptr
    ld1w    {z1.s},  p2/z,   [Aptr]
    zero    {za}
    // Load 1st vector from Bptr
    ld1w    {z2.s},  p0/z,   [Bptr0]
    // ZA0 += 1st Aptr vector OP 1st Bptr vector
    fmopa   za0.s,  p2/m,   p0/m,   z1.s,   z2.s
    // Load 2nd vector from Aptr
    ld1w    {z5.s},  p3/z,   [Aptr, C5, lsl #2]
    // Aptr += SVLb
    addvl   Aptr, Aptr, #1

.K_Loop:
    // ZA2 += 2nd Aptr vector OP 1st Bptr vector
    fmopa   za2.s,  p3/m,   p0/m,   z5.s,   z2.s
    // Load 2nd vector from Bptr
    ld1w    {z3.s},  p1/z,   [Bptr0, #1, MUL VL]
    // ZA1 += 1st Aptr vector OP 2nd Bptr vector
    fmopa   za1.s,  p2/m,   p1/m,   z1.s,   z3.s
    // Load next 1st vector from Aptr
    ld1w    {z0.s},  p2/z,   [Aptr]
    // ZA3 += 2nd Aptr vector OP 2nd Bptr vector
    fmopa   za3.s,  p3/m,   p1/m,   z5.s,   z3.s
    cmp K, #2
    b.le process_K_less_than_equal_2
    // Load next 1st vector from Bptr
    ld1w    {z6.s},  p0/z,   [Bptr0, N, lsl #2]
    // ZA0 += 1st Aptr vector OP 1st Bptr vector
    fmopa   za0.s,  p2/m,   p0/m,   z0.s,   z6.s
    // Load next 2nd vector from Aptr
    ld1w    {z4.s},  p3/z,   [Aptr, C5, lsl #2]
    // ZA2 += 2nd Aptr vector OP 1st Bptr vector
    fmopa   za2.s,  p3/m,   p0/m,   z4.s,   z6.s
    // Load next 2nd vector from Bptr
    ld1w    {z7.s},  p1/z,   [Bptr0, C2, lsl #2]
    // Bptr += 2*ldb FP32 elms [Bytes]
    add     Bptr0, Bptr0, N, lsl #3
    // ZA1 += 1st Aptr vector OP 2nd Bptr vector
    fmopa   za1.s,  p2/m,   p1/m,   z0.s,   z7.s
    // Load next 2nd vector from Aptr
    ld1w    {z1.s},  p2/z,   [Aptr, #1, MUL VL]
    // ZA3 += 2nd Aptr vector OP 2nd Bptr vector
    fmopa   za3.s,  p3/m,   p1/m,   z4.s,   z7.s
    // Load next 1st vector from Bptr
    ld1w    {z2.s},  p0/z,   [Bptr0]
    // ZA0 += 1st Aptr vector OP 1st Bptr vector
    fmopa   za0.s,  p2/m,   p0/m,   z1.s,   z2.s
    // Load next 2nd vector from Aptr
    ld1w    {z5.s},  p3/z,   [Aptr, C3, lsl #2]
    // Aptr += 2*SVLb [Bytes]
    addvl   Aptr, Aptr, #2
    cmp     Aptr, K_exit
    b.mi    .K_Loop
    // ZA2 += 2nd Aptr vector OP 1st Bptr vector
    fmopa   za2.s,  p3/m,   p0/m,   z5.s,   z2.s
    // Load next 2nd vector from Bptr
    ld1w    {z3.s},  p1/z,   [Bptr0, #1, MUL VL]
    // ZA1 += 1st Aptr vector OP 2nd Bptr vector
    fmopa   za1.s,  p2/m,   p1/m,   z1.s,   z3.s
    // ZA3 += 2nd Aptr vector OP 2nd Bptr vector
    fmopa   za3.s,  p3/m,   p1/m,   z5.s,   z3.s

process_K_less_than_equal_2:
    // Bptr += 2*ldb FP32 elements
    add     Bptr0, Bptr0, N, lsl #2
    cmp     Aptr, Aptr_end
    b.pl    .Ktail_end

.Ktail_start:
    ld1w    {z1.s},  p2/z,   [Aptr]
    ld1w    {z2.s},  p0/z,   [Bptr0]
    ld1w    {z3.s},  p1/z,   [Bptr0, #1, MUL VL]
    fmopa   za0.s,  p2/m,   p0/m,   z1.s,   z2.s
    ld1w    {z5.s},  p3/z,   [Aptr, C5, lsl #2]
    fmopa   za2.s,  p3/m,   p0/m,   z5.s,   z2.s
    fmopa   za1.s,  p2/m,   p1/m,   z1.s,   z3.s
    fmopa   za3.s,  p3/m,   p1/m,   z5.s,   z3.s

.Ktail_end:
    mov     w13, #0
    psel    p4, p0, p2.s[w13, 0]
    psel    p5, p1, p2.s[w13, 0]
    psel    p6, p0, p3.s[w13, 0]
    psel    p7, p1, p3.s[w13, 0]
    // Store to Cptr0
    st1w    {za0h.s[w13, #0]},  p4, [Cptr0]
    // Store to Cptr1
    st1w    {za1h.s[w13, #0]},  p5, [Cptr1]
    // Store to Cptr0 + N*SVLs
    st1w    {za2h.s[w13, #0]},  p6, [Cptr0, C6, lsl #2]
    // Store to Cptr1 + N*SVLs
    st1w    {za3h.s[w13, #0]},  p7, [Cptr1, C6, lsl #2]

.Loop_store_ZA:
    psel    p4, p0, p2.s[w13, 1]
    psel    p5, p1, p2.s[w13, 1]
    psel    p6, p0, p3.s[w13, 1]
    psel    p7, p1, p3.s[w13, 1]
    // Store to Cptr0 + N
    st1w    {za0h.s[w13, #1]},  p4, [Cptr0, N,  lsl #2]
    // Store to Cptr1 + N
    st1w    {za1h.s[w13, #1]},  p5, [Cptr1, N,  lsl #2]
    // Store to Cptr0 + N*(SVLs+1)
    st1w    {za2h.s[w13, #1]},  p6, [Cptr0, C1,  lsl #2]
    // Store to Cptr1 + N*(SVLs+1)
    st1w    {za3h.s[w13, #1]},  p7, [Cptr1, C1,  lsl #2]

    add     Cptr0, Cptr0, N, lsl #3    //Cptr0 += 2*N FP32 elements
    add     Cptr1, Cptr1, N, lsl #3    //Cptr1 += 2*N FP32 elements
    add     w13, w13, #2

    psel    p4, p0, p2.s[w13, 0]
    psel    p5, p1, p2.s[w13, 0]
    psel    p6, p0, p3.s[w13, 0]
    psel    p7, p1, p3.s[w13, 0]
    st1w    {za0h.s[w13, #0]},  p4, [Cptr0]
    st1w    {za1h.s[w13, #0]},  p5, [Cptr1]
    st1w    {za2h.s[w13, #0]},  p6, [Cptr0, C6, lsl #2]
    st1w    {za3h.s[w13, #0]},  p7, [Cptr1, C6, lsl #2]
    cmp     w13, w20
    b.mi    .Loop_store_ZA
    psel    p4, p0, p2.s[w13, 1]
    psel    p5, p1, p2.s[w13, 1]
    psel    p6, p0, p3.s[w13, 1]
    psel    p7, p1, p3.s[w13, 1]
    st1w    {za0h.s[w13, #1]},  p4, [Cptr0, N,  lsl #2]
    st1w    {za1h.s[w13, #1]},  p5, [Cptr1, N,  lsl #2]
    st1w    {za2h.s[w13, #1]},  p6, [Cptr0, C1,  lsl #2]
    st1w    {za3h.s[w13, #1]},  p7, [Cptr1, C1,  lsl #2]
    addvl   Cptr, Cptr, #2
    addvl   Bptr, Bptr, #1
    whilelt p0.b, Bptr, N_exit           //1st Tile predicate (N dimension)
    b.first .N_Loop
    add     A_base, A_base, C5, lsl #3   //A_base += 2*K*SVLs FP32 elements
    add     C_base, C_base, C6, lsl #3   //C_base += 2*N*SVLs FP32 elements
    incw    M_cntr
    whilelt p2.s, M_cntr, M              //1st Tile predicate (M dimension)
    b.first    .M_Loop

    smstop

    ldp     x23, x24, [sp, #32]
    ldp     x21, x22, [sp, #16]
    ldp     x19, x20, [sp], #48

    ret

