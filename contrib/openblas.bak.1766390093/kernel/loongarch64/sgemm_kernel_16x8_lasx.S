/*******************************************************************************
Copyright (c) 2023, The OpenBLAS Project
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
*******************************************************************************/
#define ASSEMBLER

#include "common.h"
#include "loongarch64_asm.S"

/*********************************************************************
* 2023/08/23 guxiwei
*        UTEST                  : OK
*        CTEST                  : OK
*        TEST                   : OK
*
*
* 2023/08/23 guxiwei
* Parameter:
*       SGEMM_DEFAULT_UNROLL_N  8
*       SGEMM_DEFAULT_UNROLL_M  16
*       SGEMM_DEFAULT_P         256
*       SGEMM_DEFAULT_Q         256
*       SGEMM_DEFAULT_R         1024
*       A_PRE                   1024
*       B_PRE                   256    // Enable prefetching for B results in a performance decrease, temporarily disabled.
*
*
* Performance at Loongson 3A5000 2.5GHz with 5000x5000x5000:
*       1 thread:       71.7 GFLOPS
*       2 threads:     142.6 GFLOPS
*       3 threads:     211.5 GFLOPS
*       4 threads:     265.0 GFLOPS
*********************************************************************/

/* Function parameters */
#define M      $r4   // param 1: bm
#define N      $r5   // param 2: bn
#define K      $r6   // param 3: bk
#define ALPHA  $f0   // param 4: alpha
#define A      $r7   // param 5: ba
#define B      $r8   // param 6: bb
#define C      $r9   // param 7: bc
#define LDC    $r10  // param 8: ldc

#ifdef TRMMKERNEL
#define OFFSET $r11  // param 9: offset
#endif
#define OFF    $r12

/* Cycle control parameters */
#define I      $r13
#define J      $r14
#define L      $r15
#define TL     $r16
/* Matrix address */
#define A0     $r17
#define B0     $r18
#define C0     $r19
#define C1     $r20
#define C2     $r23
#define C3     $r24
#define C4     $r25
#define C5     $r26
#define C6     $r27
#define C7     $r28
#define T0     $r29
#define T1     $r30
#undef  ZERO
#define ZERO   $r0

/* LASX Vectors
 * Store 16 sets of 32-bit data in A using UO and U1, with each register holding 8 data.
 * Use X0 through X7 to store 8 sets of 32-bit data in B, with each register holding a broadcast value of a single data.
 * Use D0 to D15 to store intermediate values of the computation.
 * Use VALPHA to store the broadcast value of alpha
 */
#define U0     $xr0
#define U1     $xr1
#define X0     $xr2
#define X1     $xr3
#define X2     $xr4
#define X3     $xr5
#define X4     $xr6
#define X5     $xr7
#define X6     $xr8
#define X7     $xr9
#define D0     $xr10
#define D1     $xr11
#define D2     $xr12
#define D3     $xr13
#define D4     $xr14
#define D5     $xr15
#define D6     $xr16
#define D7     $xr17
#define D8     $xr18
#define D9     $xr19
#define D10    $xr20
#define D11    $xr21
#define D12    $xr22
#define D13    $xr23
#define D14    $xr24
#define D15    $xr25
#define VALPHA $xr26

/* Prefetch interval */
#define A_PRE  0x400
#define B_PRE  0x100

// Loops outline:
// .L_N8  <--------------------------------------------------------------------------------------------    /* if N >> 3 == 0, goto .L_N7; else, enter .L_N8. */
// |    .L_M16 <---------------------                                                                  |   /* if M >> 4 == 0, goto .L_M8; Otherwise, enter .L_M16. */
// |    |  .L_M16_TL1               |                                                                  |
// |    |  .L_M16_L7                | The entire core loop of the function, KERNEK16x8                 |
// |    |  .L_M16_L71               |                                                                  |
// |    |  .L_M16_L0 ----------------                                                                  |
// |    .L_M8                                                                                          |
// |    |  .L_M8_TL1                |                                                                  |
// |    |  .L_M8_L7                 | KERNEK8x8                                                        |
// |    |  .L_M8_L71                |                                                                  |
// |    |  .L_M8_L0                 |                                                                  |
// |    .L_M4                                                                                          |
// |    |  .L_M4_TL1                |                                                                  |
// |    |  .L_M4_L7                 | KERNEK4x8                                                        |
// |    |  .L_M4_L71                |                                                                  |
// |    |  .L_M4_L0                 |                                                                  |
// |    .L_M2                                                                                          |
// |    |  .L_M2_TL1                |                                                                  |
// |    |  .L_M2_L7                 | KERNEK2x8                                                        |
// |    |  .L_M2_L71                |                                                                  |
// |    |  .L_M2_L0                 |                                                                  |
// |    .L_M1                                                                                          |
// |    |  .L_M1_TL1                |                                                                  |
// |    |  .L_M1_L7                 | KERNEK1x8                                                        |
// |    |  .L_M1_L71                |                                                                  |
// |    |  .L_M1_L0                 |                                                                  |
// |    .L_M0------------------------------------------------------------------------------------------
// .L_N7 /* if N & 7 == 0, goto .L_N0; else, enter .L_N4 */
// .L_N4
// |    .L_N4_M16 <---------------------
// |    |  .L_N4_M16_TL1               |
// |    |  .L_N4_M16_L7                |  KERNEL16x4
// |    |  .L_N4_M16_L71               |
// |    |  .L_N4_M16_L0 ----------------
// |    .L_N4_M8
// |    |  .L_N4_M8_TL1                |
// |    |  .L_N4_M8_L7                 | KERNEL8x4
// |    |  .L_N4_M8_L71                |
// |    |  .L_N4_M8_L0                 |
// |    .L_N4_M4
// |    |  .L_N4_M4_TL1                |
// |    |  .L_N4_M4_L7                 | KERNEL4x4
// |    |  .L_N4_M4_L71                |
// |    |  .L_N4_M4_L0                 |
// |    .L_N4_M2
// |    |  .L_N4_M2_TL1                |
// |    |  .L_N4_M2_L7                 | KERNEL2x4
// |    |  .L_N4_M2_L71                |
// |    |  .L_N4_M2_L0                 |
// |    .L_N4_M1
// |    |  .L_N4_M1_TL1                |
// |    |  .L_N4_M1_L7                 | KERNEL1x4
// |    |  .L_N4_M1_L71                |
// |    |  .L_N4_M1_L0                 |
// |    .L_N4_M0
// .L_N3     /* if N & 2 == 0, goto .L_N1; else enter .L_N2 */
// .L_N2
// |    .L_N2_M16 <---------------------
// |    |  .L_N2_M16_TL1               |
// |    |  .L_N2_M16_L7                | KERNEL16x2
// |    |  .L_N2_M16_L71               |
// |    |  .L_N2_M16_L0 ----------------
// |    .L_N2_M8
// |    |  .L_N2_M8_TL1                 |
// |    |  .L_N2_M8_L7                  | KERNEL8x2
// |    |  .L_N2_M8_L71                 |
// |    |  .L_N2_M8_L0                  |
// |    .L_N2_M4
// |    |  .L_N2_M4_TL1                 |
// |    |  .L_N2_M4_L7                  | KERNEL4x2
// |    |  .L_N2_M4_L71                 |
// |    |  .L_N2_M4_L0                  |
// |    .L_N2_M2
// |    |  .L_N2_M2_TL1                 |
// |    |  .L_N2_M2_L7                  | KERNEL2x2
// |    |  .L_N2_M2_L71                 |
// |    |  .L_N2_M2_L0                  |
// |    .L_N2_M1
// |    |  .L_N2_M1_TL1                 |
// |    |  .L_N2_M1_L7                  | KERNEL1x2
// |    |  .L_N2_M1_L71                 |
// |    |  .L_N2_M1_L0                  |
// |    .L_N2_M0
// .L_N1
// |    .L_N1_M16 <---------------------
// |    |  .L_N1_M16_TL1               |
// |    |  .L_N1_M16_L7                | KERNEL16x1
// |    |  .L_N1_M16_L71               |
// |    |  .L_N1_M16_L0 ----------------
// |    .L_N1_M8
// |    |  .L_N1_M8_TL1                |
// |    |  .L_N1_M8_L7                 | KERNEL8x1
// |    |  .L_N1_M8_L71                |
// |    |  .L_N1_M8_L0                 |
// |    .L_N1_M4
// |    |  .L_N1_M4_TL1                |
// |    |  .L_N1_M4_L7                 | KERNEL4x1
// |    |  .L_N1_M4_L71                |
// |    |  .L_N1_M4_L0                 |
// |    .L_N1_M2
// |    |  .L_N1_M2_TL1                |
// |    |  .L_N1_M2_L7                 | KERNEL2x1
// |    |  .L_N1_M2_L71                |
// |    |  .L_N1_M2_L0                 |
// |    .L_N1_M1
// |    |  .L_N1_M1_TL1                |
// |    |  .L_N1_M1_L7                 | KERNEL1x1
// |    |  .L_N1_M1_L71                |
// |    |  .L_N1_M1_L0                 |
// |    .L_N1_M0
// .L_N0

/*************** sgemm_kernel_macros ***************/
.macro KERNEL1x16x8_START
    GLD xv, , U0, A0, 0x00, U1, A0, 0x20

    GLDREPL xv, w, X0, B0, 0x00, X1, B0, 0x04, X2, B0, 0x08, X3, B0, 0x0C
    GMUL xvf, s, D0, U0, X0, D1, U1, X0
    preld   0,  C0, 0x00
    GMUL xvf, s, D2, U0, X1, D3, U1, X1
    preld   0,  C1, 0x00
    GMUL xvf, s, D4, U0, X2, D5, U1, X2
    preld   0,  C2, 0x00
    GMUL xvf, s, D6, U0, X3, D7, U1, X3
    preld   0,  C3, 0x00
    GLDREPL xv, w, X4, B0, 0x10, X5, B0, 0x14, X6, B0, 0x18, X7, B0, 0x1C
    GMUL xvf, s, D8,  U0, X4, D9,  U1, X4
    preld   0,  C4, 0x00
    GMUL xvf, s, D10, U0, X5, D11, U1, X5
    preld   0,  C5, 0x00
    GMUL xvf, s, D12, U0, X6, D13, U1, X6
    preld   0,  C6, 0x00
    GMUL xvf, s, D14, U0, X7, D15, U1, X7
    preld   0,  C7, 0x00
    PTR_ADDI   A0,     A0,     0x40
    PTR_ADDI   B0,     B0,     0x20
.endm

.macro KERNEL1x16x8
    GLD xv, , U0, A0, 0x00, U1, A0, 0x20

    GLDREPL xv, w, X0, B0, 0x00, X1, B0, 0x04, X2, B0, 0x08, X3, B0, 0x0C
    GMADD xvf, s, D0, U0, X0, D0, D1, U1, X0, D1, \
                  D2, U0, X1, D2, D3, U1, X1, D3
    preld      0,   A0, A_PRE
    GMADD xvf, s, D4, U0, X2, D4, D5, U1, X2, D5, \
                  D6, U0, X3, D6, D7, U1, X3  D7
    GLDREPL xv, w, X4, B0, 0x10, X5, B0, 0x14, X6, B0, 0x18, X7, B0, 0x1C
    GMADD xvf, s, D8,  U0, X4, D8,  D9,  U1, X4, D9,  \
                  D10, U0, X5, D10, D11, U1, X5, D11
    //preld      0,   B0, B_PRE
    GMADD xvf, s, D12, U0, X6, D12, D13, U1, X6, D13, \
                  D14, U0, X7, D14, D15, U1, X7  D15
    PTR_ADDI   A0,     A0,     0x40
    PTR_ADDI   B0,     B0,     0x20
.endm

.macro KERNEL8x16x8
.rept 8
    KERNEL1x16x8
.endr
.endm

.macro SAVE16x8
#if defined(TRMMKERNEL)
    GMUL xvf, s, D0,  D0,  VALPHA, D1,  D1,  VALPHA, D2,  D2,  VALPHA, D3,  D3,  VALPHA, \
                 D4,  D4,  VALPHA, D5,  D5,  VALPHA, D6,  D6,  VALPHA, D7,  D7,  VALPHA, \
                 D8,  D8,  VALPHA, D9,  D9,  VALPHA, D10, D10, VALPHA, D11, D11, VALPHA, \
                 D12, D12, VALPHA, D13, D13, VALPHA, D14, D14, VALPHA, D15, D15, VALPHA
#else
    /* Load C0  */
    GLD xv, , X0, C0, 0x00, X1, C0, 0x20
    GMADD xvf, s, D0, D0, VALPHA, X0, D1, D1, VALPHA, X1
    /* Load C1  */
    GLD xv, , X2, C1, 0x00, X3, C1, 0x20
    GMADD xvf, s, D2, D2, VALPHA, X2, D3, D3, VALPHA, X3
    /* Load C2  */
    GLD xv, , X4, C2, 0x00, X5, C2, 0x20
    GMADD xvf, s, D4, D4, VALPHA, X4, D5, D5, VALPHA, X5
    /* Load C3  */
    GLD xv, , X6, C3, 0x00, X7, C3, 0x20
    GMADD xvf, s, D6, D6, VALPHA, X6, D7, D7, VALPHA, X7
    /* Load C4  */
    GLD xv, , X0, C4, 0x00, X1, C4, 0x20
    GMADD xvf, s, D8, D8, VALPHA, X0, D9, D9, VALPHA, X1
    /* Load C5  */
    GLD xv, , X2, C5, 0x00, X3, C5, 0x20
    GMADD xvf, s, D10, D10, VALPHA, X2, D11, D11, VALPHA, X3
    /* Load C6  */
    GLD xv, , X4, C6, 0x00, X5, C6, 0x20
    GMADD xvf, s, D12, D12, VALPHA, X4, D13, D13, VALPHA, X5
    /* Load C7  */
    GLD xv, , X6, C7, 0x00, X7, C7, 0x20
    GMADD xvf, s, D14, D14, VALPHA, X6, D15, D15, VALPHA, X7
#endif // #if defined(TRMMKERNEL)
    GST xv, , D0,  C0, 0x00, D1,  C0, 0x20, \
              D2,  C1, 0x00, D3,  C1, 0x20, \
              D4,  C2, 0x00, D5,  C2, 0x20, \
              D6,  C3, 0x00, D7,  C3, 0x20, \
              D8,  C4, 0x00, D9,  C4, 0x20, \
              D10, C5, 0x00, D11, C5, 0x20, \
              D12, C6, 0x00, D13, C6, 0x20, \
              D14, C7, 0x00, D15, C7, 0x20
#if __loongarch_grlen == 64
    GADDI , d, C0, C0, 0x40, C1, C1, 0x40, C2, C2, 0x40, C3, C3, 0x40, \
               C4, C4, 0x40, C5, C5, 0x40, C6, C6, 0x40, C7, C7, 0x40
#elif __loongarch_grlen == 32
    GADDI , w, C0, C0, 0x40, C1, C1, 0x40, C2, C2, 0x40, C3, C3, 0x40, \
               C4, C4, 0x40, C5, C5, 0x40, C6, C6, 0x40, C7, C7, 0x40
#else
    GADDI , d, C0, C0, 0x40, C1, C1, 0x40, C2, C2, 0x40, C3, C3, 0x40, \
               C4, C4, 0x40, C5, C5, 0x40, C6, C6, 0x40, C7, C7, 0x40
#endif
.endm

// m = 8, 4, 2, 1
// stride = 0x20, 0x10, 0x08, 0x04
.macro KERNEL1xMx8_START m, stride
.if \m == 8
    GLD xv, , U0, A0, 0x00
.elseif \m == 4
    GLD v, , $vr0, A0, 0x00
.elseif \m ==2
    GLD f, d, $f0, A0, 0x00
.elseif \m ==1
    GLD f, s, $f0, A0, 0x00
.endif
    GLDREPL xv, w, X0, B0, 0x00, X1, B0, 0x04, X2, B0, 0x08, X3, B0, 0x0C
    GMUL xvf, s, D0, U0, X0, D2, U0, X1, \
                 D4, U0, X2, D6, U0, X3
    GLDREPL xv, w, X4, B0, 0x10, X5, B0, 0x14, X6, B0, 0x18, X7, B0, 0x1C
    GMUL xvf, s, D8,  U0, X4, D10, U0, X5, \
                 D12, U0, X6, D14, U0, X7
   PTR_ADDI   A0,     A0,     \stride
   PTR_ADDI   B0,     B0,     0x20
.endm

.macro KERNEL1xMx8 m, stride
.if \m == 8
    GLD xv, , U0, A0, 0x00
.elseif \m == 4
    GLD v, , $vr0, A0, 0x00
.elseif \m ==2
    GLD f, d, $f0, A0, 0x00
.elseif \m ==1
    GLD f, s, $f0, A0, 0x00
.endif

    GLDREPL xv, w, X0, B0, 0x00, X1, B0, 0x04, X2, B0, 0x08, X3, B0, 0x0C
    GMADD xvf, s, D0, U0, X0, D0, D2, U0, X1, D2, \
                  D4, U0, X2, D4, D6, U0, X3, D6
    GLDREPL xv, w, X4, B0, 0x10, X5, B0, 0x14, X6, B0, 0x18, X7, B0, 0x1C
    GMADD xvf, s, D8,  U0, X4, D8,  D10, U0, X5, D10, \
                  D12, U0, X6, D12, D14, U0, X7, D14
   PTR_ADDI   A0,     A0,     \stride
   PTR_ADDI   B0,     B0,     0x20
.endm

.macro KERNEL8xMx8 m, stride
.rept 8
    KERNEL1xMx8 \m, \stride
.endr
.endm

.macro SAVEMx8 m, stride
#if defined(TRMMKERNEL)
    GMUL xvf, s, D0,  D0,  VALPHA, D2,  D2,  VALPHA, \
                 D4,  D4,  VALPHA, D6,  D6,  VALPHA, \
                 D8,  D8,  VALPHA, D10, D10, VALPHA, \
                 D12, D12, VALPHA, D14, D14, VALPHA
#else
    /* Load C0, C1, C2, C3, C4, C5, C6, C7 */
 .if \m == 8
    GLD xv, , X0, C0, 0x00, X2, C1, 0x00, X4, C2, 0x00, X6, C3, 0x00
 .elseif \m == 4
    GLD v, , $vr2, C0, 0x00, $vr4, C1, 0x00, $vr6, C2, 0x00, $vr8, C3, 0x00
.elseif \m == 2
    GLD f, d, $f2, C0, 0x00, $f4, C1, 0x00, $f6, C2, 0x00, $f8, C3, 0x00
.elseif \m == 1
    GLD f, s, $f2, C0, 0x00, $f4, C1, 0x00, $f6, C2, 0x00, $f8, C3, 0x00
 .endif
    GMADD xvf, s, D0, D0, VALPHA, X0, D2, D2, VALPHA, X2, \
                  D4, D4, VALPHA, X4, D6, D6, VALPHA, X6
.if \m == 8
    GLD xv, , X0, C4, 0x00, X2, C5, 0x00, X4, C6, 0x00, X6, C7, 0x00
.elseif \m == 4
    GLD v, , $vr2, C4, 0x00, $vr4, C5, 0x00, $vr6, C6, 0x00, $vr8, C7, 0x00
.elseif \m == 2
    GLD f, d, $f2, C4, 0x00, $f4, C5, 0x00, $f6, C6, 0x00, $f8, C7, 0x00
.elseif \m == 1
    GLD f, s, $f2, C4, 0x00, $f4, C5, 0x00, $f6, C6, 0x00, $f8, C7, 0x00
.endif
    GMADD xvf, s, D8,  D8,  VALPHA, X0, D10, D10, VALPHA, X2, \
                  D12, D12, VALPHA, X4, D14, D14, VALPHA, X6
#endif // #if defined(TRMMKERNEL)
.if \m == 8
    GST xv, , D0,  C0, 0x00, D2,  C1, 0x00, \
              D4,  C2, 0x00, D6,  C3, 0x00, \
              D8,  C4, 0x00, D10, C5, 0x00, \
              D12, C6, 0x00, D14, C7, 0x00
.elseif \m == 4
    GST v, , $vr10, C0, 0x00, $vr12, C1, 0x00, \
             $vr14, C2, 0x00, $vr16, C3, 0x00, \
             $vr18, C4, 0x00, $vr20, C5, 0x00, \
             $vr22, C6, 0x00, $vr24, C7, 0x00
.elseif \m == 2
    GST f, d, $f10, C0, 0x00, $f12, C1, 0x00, \
              $f14, C2, 0x00, $f16, C3, 0x00, \
              $f18, C4, 0x00, $f20, C5, 0x00, \
              $f22, C6, 0x00, $f24, C7, 0x00
.elseif \m == 1
    GST f, s, $f10, C0, 0x00, $f12, C1, 0x00, \
              $f14, C2, 0x00, $f16, C3, 0x00, \
              $f18, C4, 0x00, $f20, C5, 0x00, \
              $f22, C6, 0x00, $f24, C7, 0x00
.endif
#if __loongarch_grlen == 64
    GADDI , d, C0, C0, \stride, C1, C1, \stride, C2, C2, \stride, C3, C3, \stride, \
               C4, C4, \stride, C5, C5, \stride, C6, C6, \stride, C7, C7, \stride
#elif __loongarch_grlen == 32
    GADDI , w, C0, C0, \stride, C1, C1, \stride, C2, C2, \stride, C3, C3, \stride, \
               C4, C4, \stride, C5, C5, \stride, C6, C6, \stride, C7, C7, \stride
#else
    GADDI , d, C0, C0, \stride, C1, C1, \stride, C2, C2, \stride, C3, C3, \stride, \
               C4, C4, \stride, C5, C5, \stride, C6, C6, \stride, C7, C7, \stride
#endif
.endm

.macro KERNEL1x16x4_START
    GLD xv, , U0, A0, 0x00, U1, A0, 0x20

    GLDREPL xv, w, X0, B0, 0x00, X1, B0, 0x04, X2, B0, 0x08, X3, B0, 0x0C
    GMUL xvf, s, D0, U0, X0, D1, U1, X0, \
                 D2, U0, X1, D3, U1, X1, \
                 D4, U0, X2, D5, U1, X2, \
                 D6, U0, X3, D7, U1, X3
   PTR_ADDI   A0,     A0,     0x40
   PTR_ADDI   B0,     B0,     0x10
.endm

.macro KERNEL1x16x4
    GLD xv, , U0, A0, 0x00, U1, A0, 0x20

    GLDREPL xv, w, X0, B0, 0x00, X1, B0, 0x04, X2, B0, 0x08, X3, B0, 0x0C
    GMADD xvf, s, D0, U0, X0, D0, D1, U1, X0, D1, \
                  D2, U0, X1, D2, D3, U1, X1, D3, \
                  D4, U0, X2, D4, D5, U1, X2, D5, \
                  D6, U0, X3, D6, D7, U1, X3  D7
    PTR_ADDI   A0,     A0,     0x40
    PTR_ADDI   B0,     B0,     0x10
.endm

.macro KERNEL8x16x4
.rept 8
    KERNEL1x16x4
.endr
.endm

.macro SAVE16x4
#if defined(TRMMKERNEL)
    GMUL xvf, s, D0,  D0,  VALPHA, D1,  D1,  VALPHA, D2,  D2,  VALPHA, D3,  D3,  VALPHA, \
                 D4,  D4,  VALPHA, D5,  D5,  VALPHA, D6,  D6,  VALPHA, D7,  D7,  VALPHA
#else
    /* Load C0  */
    GLD xv, , X0, C0, 0x00, X1, C0, 0x20
    GMADD xvf, s, D0, D0, VALPHA, X0, D1, D1, VALPHA, X1
    /* Load C1  */
    GLD xv, , X2, C1, 0x00, X3, C1, 0x20
    GMADD xvf, s, D2, D2, VALPHA, X2, D3, D3, VALPHA, X3
    /* Load C2  */
    GLD xv, , X4, C2, 0x00, X5, C2, 0x20
    GMADD xvf, s, D4, D4, VALPHA, X4, D5, D5, VALPHA, X5
    /* Load C3  */
    GLD xv, , X6, C3, 0x00, X7, C3, 0x20
    GMADD xvf, s, D6, D6, VALPHA, X6, D7, D7, VALPHA, X7
#endif // #if defined(TRMMKERNEL)
    GST xv, , D0,  C0, 0x00, D1,  C0, 0x20, \
              D2,  C1, 0x00, D3,  C1, 0x20, \
              D4,  C2, 0x00, D5,  C2, 0x20, \
              D6,  C3, 0x00, D7,  C3, 0x20
#if __loongarch_grlen == 64
    GADDI , d, C0, C0, 0x40, C1, C1, 0x40, C2, C2, 0x40, C3, C3, 0x40
#elif __loongarch_grlen == 32
    GADDI , w, C0, C0, 0x40, C1, C1, 0x40, C2, C2, 0x40, C3, C3, 0x40
#else
    GADDI , d, C0, C0, 0x40, C1, C1, 0x40, C2, C2, 0x40, C3, C3, 0x40
#endif
.endm

// m = 8, 4, 2, 1
// stride = 0x20, 0x10, 0x08, 0x04
.macro KERNEL1xMx4_START m, stride
.if \m == 8
    GLD xv, , U0, A0, 0x00
.elseif \m == 4
    GLD v, , $vr0, A0, 0x00
.elseif \m ==2
    GLD f, d, $f0, A0, 0x00
.elseif \m ==1
    GLD f, s, $f0, A0, 0x00
.endif
    GLDREPL xv, w, X0, B0, 0x00, X1, B0, 0x04, X2, B0, 0x08, X3, B0, 0x0C
    GMUL xvf, s, D0, U0, X0, D2, U0, X1, \
                 D4, U0, X2, D6, U0, X3
   PTR_ADDI   A0,     A0,     \stride
   PTR_ADDI   B0,     B0,     0x10
.endm

.macro KERNEL1xMx4 m, stride
.if \m == 8
    GLD xv, , U0, A0, 0x00
.elseif \m == 4
    GLD v, , $vr0, A0, 0x00
.elseif \m ==2
    GLD f, d, $f0, A0, 0x00
.elseif \m ==1
    GLD f, s, $f0, A0, 0x00
.endif
    GLDREPL xv, w, X0, B0, 0x00, X1, B0, 0x04, X2, B0, 0x08, X3, B0, 0x0C
    GMADD xvf, s, D0, U0, X0, D0, D2, U0, X1, D2, \
                  D4, U0, X2, D4, D6, U0, X3, D6
   PTR_ADDI   A0,     A0,     \stride
   PTR_ADDI   B0,     B0,     0x10
.endm

.macro KERNEL8xMx4 m, stride
.rept 8
    KERNEL1xMx4 \m, \stride
.endr
.endm

.macro SAVEMx4 m, stride
#if defined(TRMMKERNEL)
    GMUL xvf, s, D0,  D0,  VALPHA, D2,  D2,  VALPHA, \
                 D4,  D4,  VALPHA, D6,  D6,  VALPHA
#else
    /* Load C0, C1, C2, C3 */
 .if \m == 8
    GLD xv, , X0, C0, 0x00, X2, C1, 0x00, X4, C2, 0x00, X6, C3, 0x00
 .elseif \m == 4
    GLD v, , $vr2, C0, 0x00, $vr4, C1, 0x00, $vr6, C2, 0x00, $vr8, C3, 0x00
.elseif \m == 2
    GLD f, d, $f2, C0, 0x00, $f4, C1, 0x00, $f6, C2, 0x00, $f8, C3, 0x00
.elseif \m == 1
    GLD f, s, $f2, C0, 0x00, $f4, C1, 0x00, $f6, C2, 0x00, $f8, C3, 0x00
 .endif
    GMADD xvf, s, D0, D0, VALPHA, X0, D2, D2, VALPHA, X2, \
                  D4, D4, VALPHA, X4, D6, D6, VALPHA, X6
#endif // #if defined(TRMMKERNEL)
.if \m == 8
    GST xv, , D0,  C0, 0x00, D2,  C1, 0x00, \
              D4,  C2, 0x00, D6,  C3, 0x00
.elseif \m == 4
    GST v, , $vr10, C0, 0x00, $vr12, C1, 0x00, \
             $vr14, C2, 0x00, $vr16, C3, 0x00
.elseif \m == 2
    GST f, d, $f10, C0, 0x00, $f12, C1, 0x00, \
              $f14, C2, 0x00, $f16, C3, 0x00
.elseif \m == 1
    GST f, s, $f10, C0, 0x00, $f12, C1, 0x00, \
              $f14, C2, 0x00, $f16, C3, 0x00
.endif
#if __loongarch_grlen == 64
    GADDI , d, C0, C0, \stride, C1, C1, \stride, C2, C2, \stride, C3, C3, \stride
#elif __loongarch_grlen == 32
    GADDI , w, C0, C0, \stride, C1, C1, \stride, C2, C2, \stride, C3, C3, \stride
#else
    GADDI , d, C0, C0, \stride, C1, C1, \stride, C2, C2, \stride, C3, C3, \stride
#endif
.endm

.macro KERNEL1x16x2_START
    GLD xv, , U0, A0, 0x00, U1, A0, 0x20

    GLDREPL xv, w, X0, B0, 0x00, X1, B0, 0x04
    GMUL xvf, s, D0, U0, X0, D1, U1, X0, \
                 D2, U0, X1, D3, U1, X1
   PTR_ADDI   A0,     A0,     0x40
   PTR_ADDI   B0,     B0,     0x08
.endm

.macro KERNEL1x16x2
    GLD xv, , U0, A0, 0x00, U1, A0, 0x20

    GLDREPL xv, w, X0, B0, 0x00, X1, B0, 0x04
    GMADD xvf, s, D0, U0, X0, D0, D1, U1, X0, D1, \
                  D2, U0, X1, D2, D3, U1, X1, D3
   PTR_ADDI   A0,     A0,     0x40
   PTR_ADDI   B0,     B0,     0x08
.endm

.macro KERNEL8x16x2
.rept 8
    KERNEL1x16x2
.endr
.endm

.macro SAVE16x2
#if defined(TRMMKERNEL)
    GMUL xvf, s, D0,  D0,  VALPHA, D1,  D1,  VALPHA, D2,  D2,  VALPHA, D3,  D3,  VALPHA
#else
    /* Load C0  */
    GLD xv, , X0, C0, 0x00, X1, C0, 0x20
    GMADD xvf, s, D0, D0, VALPHA, X0, D1, D1, VALPHA, X1
    /* Load C1  */
    GLD xv, , X2, C1, 0x00, X3, C1, 0x20
    GMADD xvf, s, D2, D2, VALPHA, X2, D3, D3, VALPHA, X3
#endif // #if defined(TRMMKERNEL)
    GST xv, , D0,  C0, 0x00, D1,  C0, 0x20, \
              D2,  C1, 0x00, D3,  C1, 0x20
#if __loongarch_grlen == 64
    GADDI , d, C0, C0, 0x40, C1, C1, 0x40
#elif __loongarch_grlen == 32
    GADDI , w, C0, C0, 0x40, C1, C1, 0x40
#else
    GADDI , d, C0, C0, 0x40, C1, C1, 0x40
#endif
.endm

// m = 8, 4, 2, 1
// stride = 0x20, 0x10, 0x08, 0x04
.macro KERNEL1xMx2_START m, stride
.if \m == 8
    GLD xv, , U0, A0, 0x00
.elseif \m == 4
    GLD v, , $vr0, A0, 0x00
.elseif \m ==2
    GLD f, d, $f0, A0, 0x00
.elseif \m ==1
    GLD f, s, $f0, A0, 0x00
.endif
    GLDREPL xv, w, X0, B0, 0x00, X1, B0, 0x04
    GMUL xvf, s, D0, U0, X0, D2, U0, X1
    PTR_ADDI   A0,     A0,     \stride
    PTR_ADDI   B0,     B0,     0x08
.endm

.macro KERNEL1xMx2 m, stride
.if \m == 8
    GLD xv, , U0, A0, 0x00
.elseif \m == 4
    GLD v, , $vr0, A0, 0x00
.elseif \m ==2
    GLD f, d, $f0, A0, 0x00
.elseif \m ==1
    GLD f, s, $f0, A0, 0x00
.endif
    GLDREPL xv, w, X0, B0, 0x00, X1, B0, 0x04
    GMADD xvf, s, D0, U0, X0, D0, D2, U0, X1, D2
    PTR_ADDI   A0,     A0,     \stride
    PTR_ADDI   B0,     B0,     0x08
.endm

.macro KERNEL8xMx2 m, stride
.rept 8
    KERNEL1xMx2 \m, \stride
.endr
.endm

.macro SAVEMx2 m, stride
#if defined(TRMMKERNEL)
    GMUL xvf, s, D0,  D0,  VALPHA, D2,  D2,  VALPHA
#else
    /* Load C0, C1 */
 .if \m == 8
    GLD xv, , X0, C0, 0x00, X2, C1, 0x00
 .elseif \m == 4
    GLD v, , $vr2, C0, 0x00, $vr4, C1, 0x00
.elseif \m == 2
    GLD f, d, $f2, C0, 0x00, $f4, C1, 0x00
.elseif \m == 1
    GLD f, s, $f2, C0, 0x00, $f4, C1, 0x00
 .endif
    GMADD xvf, s, D0, D0, VALPHA, X0, D2, D2, VALPHA, X2
#endif // #if defined(TRMMKERNEL)
.if \m == 8
    GST xv, , D0,  C0, 0x00, D2,  C1, 0x00
.elseif \m == 4
    GST v, , $vr10, C0, 0x00, $vr12, C1, 0x00
.elseif \m == 2
    GST f, d, $f10, C0, 0x00, $f12, C1, 0x00
.elseif \m == 1
    GST f, s, $f10, C0, 0x00, $f12, C1, 0x00
.endif
#if __loongarch_grlen == 64
    GADDI , d, C0, C0, \stride, C1, C1, \stride
#elif __loongarch_grlen == 32
    GADDI , w, C0, C0, \stride, C1, C1, \stride
#else
    GADDI , d, C0, C0, \stride, C1, C1, \stride
#endif
.endm

.macro KERNEL1x16x1_START
    GLD xv, , U0, A0, 0x00, U1, A0, 0x20
    GLDREPL xv, w, X0, B0, 0x00
    GMUL xvf, s, D0, U0, X0, D1, U1, X0
    PTR_ADDI   A0,     A0,     0x40
    PTR_ADDI   B0,     B0,     0x04
.endm

.macro KERNEL1x16x1
    GLD xv, , U0, A0, 0x00, U1, A0, 0x20
    GLDREPL xv, w, X0, B0, 0x00
    GMADD xvf, s, D0, U0, X0, D0, D1, U1, X0, D1
    PTR_ADDI   A0,     A0,     0x40
    PTR_ADDI   B0,     B0,     0x04
.endm

.macro KERNEL8x16x1
.rept 8
    KERNEL1x16x1
.endr
.endm

.macro SAVE16x1
#if defined(TRMMKERNEL)
    GMUL xvf, s, D0,  D0,  VALPHA, D1,  D1,  VALPHA
#else
    /* Load C0  */
    GLD xv, , X0, C0, 0x00, X1, C0, 0x20
    GMADD xvf, s, D0, D0, VALPHA, X0, D1, D1, VALPHA, X1
#endif // #if defined(TRMMKERNEL)
    GST xv, , D0,  C0, 0x00, D1,  C0, 0x20
#if __loongarch_grlen == 64
    GADDI , d, C0, C0, 0x40
#elif __loongarch_grlen == 32
    GADDI , w, C0, C0, 0x40
#else
    GADDI , d, C0, C0, 0x40
#endif
.endm

// m = 8, 4, 2, 1
// stride = 0x20, 0x10, 0x08, 0x04
.macro KERNEL1xMx1_START m, stride
.if \m == 8
    GLD xv, , U0, A0, 0x00
.elseif \m == 4
    GLD v, , $vr0, A0, 0x00
.elseif \m ==2
    GLD f, d, $f0, A0, 0x00
.elseif \m ==1
    GLD f, s, $f0, A0, 0x00
.endif
    GLDREPL xv, w, X0, B0, 0x00
    GMUL xvf, s, D0, U0, X0
    PTR_ADDI   A0,     A0,     \stride
    PTR_ADDI   B0,     B0,     0x04
.endm

.macro KERNEL1xMx1 m, stride
.if \m == 8
    GLD xv, , U0, A0, 0x00
.elseif \m == 4
    GLD v, , $vr0, A0, 0x00
.elseif \m ==2
    GLD f, d, $f0, A0, 0x00
.elseif \m ==1
    GLD f, s, $f0, A0, 0x00
.endif
    GLDREPL xv, w, X0, B0, 0x00
    GMADD xvf, s, D0, U0, X0, D0
    PTR_ADDI   A0,     A0,     \stride
    PTR_ADDI   B0,     B0,     0x04
.endm

.macro KERNEL8xMx1 m, stride
.rept 8
    KERNEL1xMx1 \m, \stride
.endr
.endm

.macro SAVEMx1 m, stride
#if defined(TRMMKERNEL)
    GMUL xvf, s, D0,  D0,  VALPHA
#else
    /* Load C0, C1 */
 .if \m == 8
    GLD xv, , X0, C0, 0x00
 .elseif \m == 4
    GLD v, , $vr2, C0, 0x00
.elseif \m == 2
    GLD f, d, $f2, C0, 0x00
.elseif \m == 1
    GLD f, s, $f2, C0, 0x00
 .endif
    GMADD xvf, s, D0, D0, VALPHA, X0
#endif // #if defined(TRMMKERNEL)
.if \m == 8
    GST xv, , D0,  C0, 0x00
.elseif \m == 4
    GST v, , $vr10, C0, 0x00
.elseif \m == 2
    GST f, d, $f10, C0, 0x00
.elseif \m == 1
    GST f, s, $f10, C0, 0x00
.endif
#if __loongarch_grlen == 64
    GADDI , d, C0, C0, \stride
#elif __loongarch_grlen == 32
    GADDI , w, C0, C0, \stride
#else
    GADDI , d, C0, C0, \stride
#endif
.endm

    PROLOGUE
    push_if_used 9, 8
    xvreplve0.w   VALPHA,     $xr0
#if defined (TRMMKERNEL) && !defined(LEFT)
    PTR_SUB   OFF,   ZERO,  OFFSET
#else
    xor     OFF,   OFF,   OFF
#endif
    /* if (!(N >> 3)) goto L_N7 */
    PTR_SRAI   J,     N,     3     /* J = bn >> 3 */
    andi     N,     N,     0x07
    beq      ZERO,  J,     .L_N7
.L_N8: /* J -- */
    move     C0,    C
    move     A0,    A
    PTR_SLLI   T0,    LDC,   2
    PTR_ADDI   J,     J,     -1   /* J-- */
#if __loongarch_grlen == 64
    GADD , d, C1, C0, T0, C2, C1, T0, C3, C2, T0, C4, C3, T0, C5, C4, T0, \
              C6, C5, T0, C7, C6, T0
#elif __loongarch_grlen == 32
    GADD , w, C1, C0, T0, C2, C1, T0, C3, C2, T0, C4, C3, T0, C5, C4, T0, \
              C6, C5, T0, C7, C6, T0
#else
    GADD , d, C1, C0, T0, C2, C1, T0, C3, C2, T0, C4, C3, T0, C5, C4, T0, \
              C6, C5, T0, C7, C6, T0
#endif
#if defined(TRMMKERNEL) && defined(LEFT)
    move     OFF,   OFFSET
#endif
    /* if (!(M >> 4)) goto L_M8 */
    PTR_SRAI   I,     M,     4     /* I = bm >> 4 */
    beq      ZERO,  I,     .L_M8
.align 5
.L_M16: /* I-- */
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    PTR_SLLI   T0,    OFF,  0x06
    PTR_ADD    A0,    A0,   T0 /* A0 += 16 * OFF */
    PTR_SLLI   T0,    OFF,  0x05
    PTR_ADD    B0,    B,    T0 /* B0 = B + 8 * OFF */
#endif
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    PTR_SUB    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    PTR_ADDI   L,     OFF,  16
#else
    /* number of values in B */
    PTR_ADDI   L,     OFF,  8
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif
    KERNEL1x16x8_START
    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_M16_L7 */
    beq       ZERO,TL, .L_M16_L7
.align 5
.L_M16_TL1:
    KERNEL8x16x8
    PTR_ADDI    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_M16_TL1
.L_M16_L7:
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_M16_L0
.align 5
.L_M16_L71:
    KERNEL1x16x8
    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_M16_L71
.L_M16_L0:
    SAVE16x8

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    PTR_SUB     L,   K,   OFF
#ifdef LEFT
    /* number of values  in A */
    PTR_ADDI    L,   L,   -16
#else
    /* number of values in B */
    PTR_ADDI    L,   L,   -8
#endif
    PTR_SLLI    T0,  L,  0x06
    PTR_ADD     A0,  A0, T0
    PTR_SLLI    T0,  L,  0x05
    PTR_ADD     B0,  B0, T0
#endif

#ifdef LEFT
    PTR_ADDI    OFF, OFF, 0x10 /* number of values in A */
#endif
#endif   // #if defined(TRMMKERNEL)

    PTR_ADDI    I,   I,   -1  /* I-- */
    blt       ZERO,I,   .L_M16
.L_M8:
    /* We have done M & 16, considering M=8/4/2/1 */
    andi      I,   M,   15
    beq       ZERO,I,   .L_M0

    andi      I,   M,   8
    beq       ZERO,I,   .L_M4
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    PTR_SLLI   T0,    OFF,  0x05
    PTR_ADD    A0,    A0,   T0 /* A0 += 8 * OFF */
    PTR_SLLI   T0,    OFF,  0x05
    PTR_ADD    B0,    B,    T0 /* B0 = B + 8 * OFF */
#endif
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    PTR_SUB    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    PTR_ADDI   L,     OFF,  8
#else
    /* number of values in B */
    PTR_ADDI   L,     OFF,  8
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif  // #if defined(TRMMKERNEL)
    KERNEL1xMx8_START 8, 0x20
    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_M8_L7 */
    beq       ZERO,TL, .L_M8_L7
.align 5
.L_M8_TL1:
    KERNEL8xMx8 8, 0x20
    PTR_ADDI    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_M8_TL1
.L_M8_L7:
    /* if (!(L & 7)) goto L_M8_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_M8_L0
.align 5
.L_M8_L71:
    KERNEL1xMx8 8, 0x20
    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_M8_L71
.L_M8_L0:
    SAVEMx8 8, 0x20
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    PTR_SUB     L,   K,   OFF
#ifdef LEFT
    /* number of values in A */
    PTR_ADDI    L,   L,   -8
#else
    /* number of values in B */
    PTR_ADDI    L,   L,   -8
#endif
    PTR_SLLI    T0,  L,  0x05
    PTR_ADD     A0,  A0, T0
    PTR_SLLI    T0,  L,  0x05
    PTR_ADD     B0,  B0, T0
#endif

#ifdef LEFT
    /* number of values in A */
    PTR_ADDI    OFF,   OFF,  0x08
#endif
#endif   // #if defined(TRMMKERNEL)
.L_M4:
    andi      I,   M,   4
    beq       ZERO,I,   .L_M2
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    PTR_SLLI   T0,    OFF,  0x04
    PTR_ADD    A0,    A0,   T0 /* A0 += 4 * OFF */
    PTR_SLLI   T0,    OFF,  0x05
    PTR_ADD    B0,    B,    T0 /* B0 = B + 8 * OFF */
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    PTR_SUB    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    PTR_ADDI   L,     OFF,  4
#else
    /* number of values in B */
    PTR_ADDI   L,     OFF,  8
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif
    KERNEL1xMx8_START 4, 0x10
    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_M4_L7 */
    beq       ZERO,TL, .L_M4_L7
.align 5
.L_M4_TL1:
    KERNEL8xMx8 4, 0x10
    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_M4_TL1
.L_M4_L7:
    /* if (!(L & 7)) goto L_M4_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_M4_L0
.L_M4_L71:
    KERNEL1xMx8 4, 0x10
    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_M4_L71
.L_M4_L0:
    SAVEMx8 4, 0x10
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    PTR_SUB     L,   K,   OFF
#ifdef LEFT
    /* number of values in A */
    PTR_ADDI    L,   L,   -4
#else
    /* number of values in B */
    PTR_ADDI    L,   L,   -8
#endif
    PTR_SLLI    T0,  L,  0x04
    PTR_ADD     A0,  A0, T0
    PTR_SLLI    T0,  L,  0x05
    PTR_ADD     B0,  B0, T0
#endif

#ifdef LEFT
    /* number of values in A */
    PTR_ADDI    OFF,   OFF,  0x04
#endif
#endif   // #if defined(TRMMKERNEL)
.L_M2:
    andi      I,   M,   2
    beq       ZERO,I,   .L_M1

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    PTR_SLLI   T0,    OFF,  0x03
    PTR_ADD    A0,    A0,   T0
    PTR_SLLI   T0,    OFF,  0x05
    PTR_ADD    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    PTR_SUB    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    PTR_ADDI   L,     OFF,  2
#else
    /* number of values in B */
    PTR_ADDI   L,     OFF,  8
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif
    KERNEL1xMx8_START 2, 0x08

    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_M2_L7 */
    beq       ZERO,TL, .L_M2_L7
.align 5
.L_M2_TL1:
    KERNEL8xMx8 2, 0x08
    PTR_ADDI    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_M2_TL1
.L_M2_L7:
    /* if (!(L & 7)) goto L_M2_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_M2_L0
.align 5
.L_M2_L71:
    KERNEL1xMx8 2, 0x08
    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_M2_L71
.L_M2_L0:
    SAVEMx8 2, 0x08
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    PTR_SUB     L,   K,   OFF
#ifdef LEFT
    /* number of values in A */
    PTR_ADDI    L,   L,   -2
#else
    /* number of values in B */
    PTR_ADDI    L,   L,   -8
#endif
    PTR_SLLI    T0,  L,  0x03
    PTR_ADD     A0,  A0, T0
    PTR_SLLI    T0,  L,  0x05
    PTR_ADD     B0,  B0, T0
#endif

#ifdef LEFT
    /* number of values in A */
    PTR_ADDI    OFF,   OFF,  0x02
#endif
#endif   // #if defined(TRMMKERNEL)
.L_M1:
    andi      I,   M,   1
    beq       ZERO,I,   .L_M0

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    PTR_SLLI   T0,    OFF,  0x02
    PTR_ADD    A0,    A0,   T0
    PTR_SLLI   T0,    OFF,  0x05
    PTR_ADD    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    PTR_SUB    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    PTR_ADDI   L,     OFF,  1
#else
    /* number of values in B */
    PTR_ADDI   L,     OFF,  8
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif
    KERNEL1xMx8_START 1, 0x04
    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_M1_L7 */
    beq       ZERO,TL, .L_M1_L7
.align 5
.L_M1_TL1:
    KERNEL8xMx8 1, 0x04
    PTR_ADDI    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_M1_TL1
.L_M1_L7:
    /* if (!(L & 7)) goto L_M1_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_M1_L0
.align 5
.L_M1_L71:
    KERNEL1xMx8 1, 0x04
    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_M1_L71
.L_M1_L0:
    SAVEMx8 1, 0x04
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    PTR_SUB     L,   K,   OFF
#ifdef LEFT
    /* number of values in A */
    PTR_ADDI    L,   L,   -1
#else
    /* number of values in B */
    PTR_ADDI    L,   L,   -8
#endif
    PTR_SLLI    T0,  L,  0x02
    PTR_ADD     A0,  A0, T0
    PTR_SLLI    T0,  L,  0x05
    PTR_ADD     B0,  B0, T0
#endif

#ifdef LEFT
    /* number of values in A */
    PTR_ADDI    OFF,   OFF,  0x01
#endif
#endif   // #if defined(TRMMKERNEL)

.L_M0:
    /* Add stride for B and C
     * B += (K * 32)
     * C += (LDC * 32)
     */
    PTR_SLLI    T0,   K,   5
    PTR_SLLI    T1,   LDC, 5
    PTR_ADD     B,    B,   T0
    PTR_ADD     C,    C,   T1
#if defined(TRMMKERNEL) && !defined(LEFT)
    PTR_ADDI    OFF,  OFF, 0x08 /* number of values in B */
#endif
    blt      ZERO,  J,   .L_N8

.L_N7:
    andi     J,    N,   4
    beq      ZERO, J,   .L_N3
.L_N4:
    move     C0,    C
    move     A0,    A
    PTR_SLLI   T0,    LDC,   2
#if __loongarch_grlen == 64
    GADD , d, C1, C0, T0, C2, C1, T0, C3, C2, T0
#elif __loongarch_grlen == 32
    GADD , w, C1, C0, T0, C2, C1, T0, C3, C2, T0
#else
    GADD , d, C1, C0, T0, C2, C1, T0, C3, C2, T0
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
    move     OFF,   OFFSET
#endif

    /* if (!(M >> 4)) goto L_N4_M8 */
    PTR_SRAI   I,     M,     4     /* I = bm >> 4 */
    beq      ZERO,  I,     .L_N4_M8
.align 5
.L_N4_M16:
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    PTR_SLLI   T0,    OFF,  0x06
    PTR_ADD    A0,    A0,   T0 /* A0 += 16 * OFF */
    PTR_SLLI   T0,    OFF,  0x04
    PTR_ADD    B0,    B,    T0 /* B0 += 4 * OFF */
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    PTR_SUB    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    PTR_ADDI   L,     OFF,  16
#else
    /* number of values in B */
    PTR_ADDI   L,     OFF,  4
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif
    KERNEL1x16x4_START

    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N4_L7 */
    beq       ZERO,TL, .L_N4_M16_L7
.align 5
.L_N4_M16_TL1: /* TL-- */
    KERNEL8x16x4

    PTR_ADDI    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N4_M16_TL1
.L_N4_M16_L7:
    /* if (!(L & 7)) goto L_N4_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N4_M16_L0
.align 5
.L_N4_M16_L71:
    KERNEL1x16x4
    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_N4_M16_L71
.L_N4_M16_L0:
    SAVE16x4
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    PTR_SUB     L,   K,   OFF
#ifdef LEFT
    PTR_ADDI    L,   L,   -16
#else
    PTR_ADDI    L,   L,   -4
#endif
    PTR_SLLI    T0,  L,  0x06
    PTR_ADD     A0,  A0, T0
    PTR_SLLI    T0,  L,  0x04
    PTR_ADD     B0,  B0, T0
#endif

#ifdef LEFT
    PTR_ADDI    OFF,   OFF,  0x10
#endif
#endif   // #if defined(TRMMKERNEL)

    PTR_ADDI    I,   I,   -1  /* I-- */
    blt       ZERO,I,   .L_N4_M16
.L_N4_M8:
    /* We have done M & 16, considering M=8/4/2/1 */
    andi      I,   M,   15
    beq       ZERO,I,   .L_N4_M0

    andi      I,   M,   8
    beq       ZERO,I,   .L_N4_M4

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    PTR_SLLI   T0,    OFF,  0x05
    PTR_ADD    A0,    A0,   T0
    PTR_SLLI   T0,    OFF,  0x04
    PTR_ADD    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    PTR_SUB    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    PTR_ADDI   L,     OFF,  8
#else
    /* number of values in B */
    PTR_ADDI   L,     OFF,  4
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif
    KERNEL1xMx4_START 8, 0x20
    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N4_M8_L7 */
    beq       ZERO,TL, .L_N4_M8_L7
.align 5
.L_N4_M8_TL1: /* TL-- */
    KERNEL8xMx4 8, 0x20

    PTR_ADDI    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N4_M8_TL1
.L_N4_M8_L7:
    /* if (!(L & 7)) goto L_N4_M8_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N4_M8_L0
.align 5
.L_N4_M8_L71:
    KERNEL1xMx4 8, 0x20
    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_N4_M8_L71
.L_N4_M8_L0:
    SAVEMx4 8, 0x20
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    PTR_SUB     L,   K,   OFF
#ifdef LEFT
    PTR_ADDI    L,   L,   -8
#else
    PTR_ADDI    L,   L,   -4
#endif
    PTR_SLLI    T0,  L,  0x05
    PTR_ADD     A0,  A0, T0
    PTR_SLLI    T0,  L,  0x04
    PTR_ADD     B0,  B0, T0
#endif

#ifdef LEFT
    PTR_ADDI    OFF,   OFF,  0x08
#endif
#endif   // #if defined(TRMMKERNEL)
.L_N4_M4:
    andi      I,   M,   4
    beq       ZERO,I,   .L_N4_M2

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    PTR_SLLI   T0,    OFF,  0x04
    PTR_ADD    A0,    A0,   T0
    PTR_SLLI   T0,    OFF,  0x04
    PTR_ADD    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    PTR_SUB    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    PTR_ADDI   L,     OFF,  4
#else
    /* number of values in B */
    PTR_ADDI   L,     OFF,  4
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif
    KERNEL1xMx4_START 4, 0x10
    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N4_M4_L7 */
    beq       ZERO,TL, .L_N4_M4_L7
.align 5
.L_N4_M4_TL1: /* TL-- */
    KERNEL8xMx4 4, 0x10

    PTR_ADDI    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N4_M4_TL1
.L_N4_M4_L7:
    /* if (!(L & 7)) goto L_N4_M4_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N4_M4_L0
.align 5
.L_N4_M4_L71:
    KERNEL1xMx4 4, 0x10

    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_N4_M4_L71
.L_N4_M4_L0:
    SAVEMx4 4, 0x10
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    PTR_SUB     L,   K,   OFF
#ifdef LEFT
    PTR_ADDI    L,   L,   -4
#else
    PTR_ADDI    L,   L,   -4
#endif
    PTR_SLLI    T0,  L,  0x04
    PTR_ADD     A0,  A0, T0
    PTR_SLLI    T0,  L,  0x04
    PTR_ADD     B0,  B0, T0
#endif

#ifdef LEFT
    PTR_ADDI    OFF,   OFF,  0x04
#endif
#endif   // #if defined(TRMMKERNEL)
.L_N4_M2:
    andi      I,   M,   2
    beq       ZERO,I,   .L_N4_M1

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    PTR_SLLI   T0,    OFF,  0x03
    PTR_ADD    A0,    A0,   T0
    PTR_SLLI   T0,    OFF,  0x04
    PTR_ADD    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    PTR_SUB    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    PTR_ADDI   L,     OFF,  2
#else
    /* number of values in B */
    PTR_ADDI   L,     OFF,  4
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif
    KERNEL1xMx4_START 2, 0x08
    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N4_M2_L7 */
    beq       ZERO,TL, .L_N4_M2_L7
.align 5
.L_N4_M2_TL1: /* TL-- */
    KERNEL8xMx4 2, 0x08

    PTR_ADDI    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N4_M2_TL1
.L_N4_M2_L7:
    /* if (!(L & 7)) goto L_N4_M2_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N4_M2_L0
.align 5
.L_N4_M2_L71:
    KERNEL1xMx4 2, 0x08
    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_N4_M2_L71
.L_N4_M2_L0:
    SAVEMx4 2, 0x08

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    PTR_SUB     L,   K,   OFF
#ifdef LEFT
    PTR_ADDI    L,   L,   -2
#else
    PTR_ADDI    L,   L,   -4
#endif
    PTR_SLLI    T0,  L,  0x03
    PTR_ADD     A0,  A0, T0
    PTR_SLLI    T0,  L,  0x04
    PTR_ADD     B0,  B0, T0
#endif

#ifdef LEFT
    PTR_ADDI    OFF,   OFF,  0x02
#endif
#endif   // #if defined(TRMMKERNEL)
.L_N4_M1:
    andi      I,   M,   1
    beq       ZERO,I,   .L_N4_M0

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    PTR_SLLI   T0,    OFF,  0x02
    PTR_ADD    A0,    A0,   T0
    PTR_SLLI   T0,    OFF,  0x04
    PTR_ADD    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    PTR_SUB    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    PTR_ADDI   L,     OFF,  1
#else
    /* number of values in B */
    PTR_ADDI   L,     OFF,  4
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif
    KERNEL1xMx4_START 1, 0x04
    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N4_M1_L7 */
    beq       ZERO,TL, .L_N4_M1_L7
.align 5
.L_N4_M1_TL1: /* TL-- */
    KERNEL8xMx4 1, 0x04

    PTR_ADDI    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N4_M1_TL1
.L_N4_M1_L7:
    /* if (!(L & 7)) goto L_N4_M1_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N4_M1_L0
.align 5
.L_N4_M1_L71:
    KERNEL1xMx4 1, 0x04
    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_N4_M1_L71
.L_N4_M1_L0:
    SAVEMx4 1, 0x04
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    PTR_SUB     L,   K,   OFF
#ifdef LEFT
    PTR_ADDI    L,   L,   -1
#else
    PTR_ADDI    L,   L,   -4
#endif
    PTR_SLLI    T0,  L,  0x02
    PTR_ADD     A0,  A0, T0
    PTR_SLLI    T0,  L,  0x04
    PTR_ADD     B0,  B0, T0
#endif

#ifdef LEFT
    PTR_ADDI    OFF,   OFF,  0x01
#endif
#endif   // #if defined(TRMMKERNEL)
.L_N4_M0:
    /* Add stride for B and C
     * B += 4 * K
     * C += 4 * LDC
     */
    PTR_SLLI    T0,   K,   4
    PTR_SLLI    T1,   LDC, 4
    PTR_ADD     B,    B,   T0
    PTR_ADD     C,    C,   T1

#if defined(TRMMKERNEL) && !defined(LEFT)
    PTR_ADDI    OFF,  OFF, 0x04
#endif
    /* We must reinit I */
    PTR_SRAI   I,     M,   4     /* I = bm >> 4 */
.L_N3:
    andi     J,    N,   2
    beq      ZERO, J,   .L_N1

.L_N2:
    move     C0,    C
    move     A0,    A
    PTR_SLLI   T0,    LDC,   2
    PTR_ADD    C1,    C0,    T0

#if defined(TRMMKERNEL) && defined(LEFT)
    move     OFF,   OFFSET
#endif

    /* if (!(M >> 4)) goto L_N2_M8 */
    PTR_SRAI   I,     M,     4     /* I = bm >> 4 */
    beq      ZERO,  I,     .L_N2_M8
.align 5
.L_N2_M16:
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    PTR_SLLI   T0,    OFF,  0x06
    PTR_ADD    A0,    A0,   T0
    PTR_SLLI   T0,    OFF,  0x03
    PTR_ADD    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    PTR_SUB    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    PTR_ADDI   L,     OFF,  16
#else
    /* number of values in B */
    PTR_ADDI   L,     OFF,  2
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif
    KERNEL1x16x2_START

    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N2_M16_L7 */
    beq       ZERO,TL, .L_N2_M16_L7
.align 5
.L_N2_M16_TL1: /* TL-- */
    KERNEL8x16x2

    PTR_ADDI    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N2_M16_TL1
.L_N2_M16_L7:
    /* if (!(L & 7)) goto L_N2_M16_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N2_M16_L0
.align 5
.L_N2_M16_L71:
    KERNEL1x16x2
    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_N2_M16_L71
.L_N2_M16_L0:
    SAVE16x2
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    PTR_SUB     L,   K,   OFF
#ifdef LEFT
    PTR_ADDI    L,   L,   -16
#else
    PTR_ADDI    L,   L,   -2
#endif
    PTR_SLLI    T0,  L,  0x06
    PTR_ADD     A0,  A0, T0
    PTR_SLLI    T0,  L,  0x03
    PTR_ADD     B0,  B0, T0
#endif

#ifdef LEFT
    PTR_ADDI    OFF,   OFF,  0x10
#endif
#endif   // #if defined(TRMMKERNEL)

    PTR_ADDI    I,   I,   -1  /* I-- */
    blt       ZERO,I,   .L_N2_M16
.L_N2_M8:
    /* We have done M & 16, considering M=8/4/2/1 */
    andi      I,   M,   15
    beq       ZERO,I,   .L_N2_M0

    andi      I,   M,   8
    beq       ZERO,I,   .L_N2_M4

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    PTR_SLLI   T0,    OFF,  0x05
    PTR_ADD    A0,    A0,   T0
    PTR_SLLI   T0,    OFF,  0x03
    PTR_ADD    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    PTR_SUB    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    PTR_ADDI   L,     OFF,  8
#else
    /* number of values in B */
    PTR_ADDI   L,     OFF,  2
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif
    KERNEL1xMx2_START 8, 0x20
    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N2_M8_L7 */
    beq       ZERO,TL, .L_N2_M8_L7
.align 5
.L_N2_M8_TL1: /* TL-- */
    KERNEL8xMx2 8, 0x20
    PTR_ADDI    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N2_M8_TL1
.L_N2_M8_L7:
    /* if (!(L & 7)) goto L_N2_M8_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N2_M8_L0
.align 5
.L_N2_M8_L71:
    KERNEL1xMx2 8, 0x20
    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_N2_M8_L71
.L_N2_M8_L0:
    SAVEMx2 8, 0x20
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    PTR_SUB     L,   K,   OFF
#ifdef LEFT
    PTR_ADDI    L,   L,   -8
#else
    PTR_ADDI    L,   L,   -2
#endif
    PTR_SLLI    T0,  L,  0x05
    PTR_ADD     A0,  A0, T0
    PTR_SLLI    T0,  L,  0x03
    PTR_ADD     B0,  B0, T0
#endif

#ifdef LEFT
    PTR_ADDI    OFF,   OFF,  0x08
#endif
#endif   // #if defined(TRMMKERNEL)
.L_N2_M4:
    andi      I,   M,   4
    beq       ZERO,I,   .L_N2_M2

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    PTR_SLLI   T0,    OFF,  0x04
    PTR_ADD    A0,    A0,   T0
    PTR_SLLI   T0,    OFF,  0x03
    PTR_ADD    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    PTR_SUB    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    PTR_ADDI   L,     OFF,  4
#else
    /* number of values in B */
    PTR_ADDI   L,     OFF,  2
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif
    KERNEL1xMx2_START 4, 0x10
    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N2_M4_L7 */
    beq       ZERO,TL, .L_N2_M4_L7
.align 5
.L_N2_M4_TL1: /* TL-- */
    KERNEL8xMx2 4, 0x10
    PTR_ADDI    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N2_M4_TL1
.L_N2_M4_L7:
    /* if (!(L & 7)) goto L_N2_M4_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N2_M4_L0
.align 5
.L_N2_M4_L71:
    KERNEL1xMx2 4, 0x10
    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_N2_M4_L71
.L_N2_M4_L0:
    SAVEMx2 4, 0x10
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    PTR_SUB     L,   K,   OFF
#ifdef LEFT
    PTR_ADDI    L,   L,   -4
#else
    PTR_ADDI    L,   L,   -2
#endif
    PTR_SLLI    T0,  L,  0x04
    PTR_ADD     A0,  A0, T0
    PTR_SLLI    T0,  L,  0x03
    PTR_ADD     B0,  B0, T0
#endif

#ifdef LEFT
    PTR_ADDI    OFF,   OFF,  0x04
#endif
#endif   // #if defined(TRMMKERNEL)
.L_N2_M2:
    andi      I,   M,   2
    beq       ZERO,I,   .L_N2_M1

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    PTR_SLLI   T0,    OFF,  0x03
    PTR_ADD    A0,    A0,   T0
    PTR_ADD    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    PTR_SUB    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    PTR_ADDI   L,     OFF,  2
#else
    /* number of values in B */
    PTR_ADDI   L,     OFF,  2
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif
    KERNEL1xMx2_START 2, 0x08
    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N2_M2_L7 */
    beq       ZERO,TL, .L_N2_M2_L7
.align 5
.L_N2_M2_TL1: /* TL-- */
    KERNEL8xMx2 2, 0x08
    PTR_ADDI    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N2_M2_TL1
.L_N2_M2_L7:
    /* if (!(L & 7)) goto L_N2_M2_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N2_M2_L0
.align 5
.L_N2_M2_L71:
    KERNEL1xMx2 2, 0x08
    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_N2_M2_L71
.L_N2_M2_L0:
    SAVEMx2 2, 0x08
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    PTR_SUB     L,   K,   OFF
#ifdef LEFT
    PTR_ADDI    L,   L,   -2
#else
    PTR_ADDI    L,   L,   -2
#endif
    PTR_SLLI    T0,  L,  0x03
    PTR_ADD     A0,  A0, T0
    PTR_ADD     B0,  B0, T0
#endif

#ifdef LEFT
    PTR_ADDI    OFF,   OFF,  0x02
#endif
#endif   // #if defined(TRMMKERNEL)
.L_N2_M1:
    andi      I,   M,   1
    beq       ZERO,I,   .L_N2_M0

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    PTR_SLLI   T0,    OFF,  0x02
    PTR_ADD    A0,    A0,   T0
    PTR_SLLI   T0,    OFF,  0x03
    PTR_ADD    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    PTR_SUB    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    PTR_ADDI   L,     OFF,  1
#else
    /* number of values in B */
    PTR_ADDI   L,     OFF,  2
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif
    KERNEL1xMx2_START 1, 0x04
    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N2_M1_L7 */
    beq       ZERO,TL, .L_N2_M1_L7
.align 5
.L_N2_M1_TL1: /* TL-- */
    KERNEL8xMx2 1, 0x04
    PTR_ADDI    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N2_M1_TL1
.L_N2_M1_L7:
    /* if (!(L & 7)) goto L_N2_M1_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N2_M1_L0
.align 5
.L_N2_M1_L71:
    KERNEL1xMx2 1, 0x04
    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_N2_M1_L71
.L_N2_M1_L0:
    SAVEMx2 1, 0x04
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    PTR_SUB     L,   K,   OFF
#ifdef LEFT
    PTR_ADDI    L,   L,   -1
#else
    PTR_ADDI    L,   L,   -2
#endif
    PTR_SLLI    T0,  L,  0x02
    PTR_ADD     A0,  A0, T0
    PTR_SLLI    T0,  L,  0x03
    PTR_ADD     B0,  B0, T0
#endif

#ifdef LEFT
    PTR_ADDI    OFF,   OFF,  0x01
#endif
#endif   // #if defined(TRMMKERNEL)
.L_N2_M0:
    /* Add stride for B and C
     * B += 2 * K
     * C += 2 * LDC
     */
    PTR_SLLI    T0,   K,   3
    PTR_SLLI    T1,   LDC, 3
    PTR_ADD     B,    B,   T0
    PTR_ADD     C,    C,   T1
#if defined(TRMMKERNEL) && !defined(LEFT)
    PTR_ADDI    OFF,  OFF, 0x02
#endif
    /* We must reinit I */
    PTR_SRAI   I,     M,   4     /* I = bm >> 4 */
.L_N1:
    andi     J,    N,   1
    beq      ZERO, J,   .L_N0
    move     C0,    C
    move     A0,    A

#if defined(TRMMKERNEL) && defined(LEFT)
    move     OFF,   OFFSET
#endif
    /* if (!(M >> 4)) goto L_N1_M8 */
    PTR_SRAI   I,     M,     4     /* I = bm >> 4 */
    beq      ZERO,  I,     .L_N1_M8
.L_N1_M16:
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    PTR_SLLI   T0,    OFF,  0x06
    PTR_ADD    A0,    A0,   T0
    PTR_SLLI   T0,    OFF,  0x02
    PTR_ADD    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    PTR_SUB    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    PTR_ADDI   L,     OFF,  16
#else
    /* number of values in B */
    PTR_ADDI   L,     OFF,  1
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif
    KERNEL1x16x1_START
    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N1_M16_L7 */
    beq       ZERO,TL, .L_N1_M16_L7
.align 5
.L_N1_M16_TL1: /* TL-- */
    KERNEL8x16x1
    PTR_ADDI    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N1_M16_TL1
.L_N1_M16_L7:
    /* if (!(L & 7)) goto L_N1_M16_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N1_M16_L0
.align 5
.L_N1_M16_L71:
    KERNEL1x16x1
    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_N1_M16_L71
.L_N1_M16_L0:
    SAVE16x1
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    PTR_SUB     L,   K,   OFF
#ifdef LEFT
    PTR_ADDI    L,   L,   -16
#else
    PTR_ADDI    L,   L,   -1
#endif
    PTR_SLLI    T0,  L,  0x06
    PTR_ADD     A0,  A0, T0
    PTR_SLLI    T0,  L,  0x02
    PTR_ADD     B0,  B0, T0
#endif

#ifdef LEFT
    PTR_ADDI    OFF,   OFF,  0x10
#endif
#endif   // #if defined(TRMMKERNEL)

    PTR_ADDI    I,   I,   -1  /* I-- */
    blt       ZERO,I,   .L_N1_M16
.L_N1_M8:
    /* We have done M & 16, considering M=8/4/2/1 */
    andi      I,   M,   15
    beq       ZERO,I,   .L_N1_M0

    andi      I,   M,   8
    beq       ZERO,I,   .L_N1_M4
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    PTR_SLLI   T0,    OFF,  0x05
    PTR_ADD    A0,    A0,   T0
    PTR_SLLI   T0,    OFF,  0x02
    PTR_ADD    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    PTR_SUB    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    PTR_ADDI   L,     OFF,  8
#else
    /* number of values in B */
    PTR_ADDI   L,     OFF,  1
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif
    KERNEL1xMx1_START 8, 0x20
    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N1_M8_L7 */
    beq       ZERO,TL, .L_N1_M8_L7
.align 5
.L_N1_M8_TL1: /* TL-- */
    KERNEL8xMx1 8, 0x20
    PTR_ADDI    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N1_M8_TL1
.L_N1_M8_L7:
    /* if (!(L & 7)) goto L_N1_M8_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N1_M8_L0
.align 5
.L_N1_M8_L71:
    KERNEL1xMx1 8, 0x20
    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_N1_M8_L71
.L_N1_M8_L0:
    SAVEMx1 8, 0x20
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    PTR_SUB     L,   K,   OFF
#ifdef LEFT
    PTR_ADDI    L,   L,   -8
#else
    PTR_ADDI    L,   L,   -1
#endif
    PTR_SLLI    T0,  L,  0x05
    PTR_ADD     A0,  A0, T0
    PTR_SLLI    T0,  L,  0x02
    PTR_ADD     B0,  B0, T0
#endif

#ifdef LEFT
    PTR_ADDI    OFF,   OFF,  0x08
#endif
#endif   // #if defined(TRMMKERNEL)
.L_N1_M4:
    andi      I,   M,   4
    beq       ZERO,I,   .L_N1_M2

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    PTR_SLLI   T0,    OFF,  0x04
    PTR_ADD    A0,    A0,   T0
    PTR_SLLI   T0,    OFF,  0x02
    PTR_ADD    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    PTR_SUB    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    PTR_ADDI   L,     OFF,  4
#else
    /* number of values in B */
    PTR_ADDI   L,     OFF,  1
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif
    KERNEL1xMx1_START 4, 0x10
    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N1_M4_L7 */
    beq       ZERO,TL, .L_N1_M4_L7
.align 5
.L_N1_M4_TL1: /* TL-- */
    KERNEL8xMx1 4, 0x10

    PTR_ADDI    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N1_M4_TL1
.L_N1_M4_L7:
    /* if (!(L & 7)) goto L_N1_M4_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N1_M4_L0
.align 5
.L_N1_M4_L71:
    KERNEL1xMx1 4, 0x10
    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_N1_M4_L71
.L_N1_M4_L0:
    SAVEMx1 4, 0x10
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    PTR_SUB     L,   K,   OFF
#ifdef LEFT
    PTR_ADDI    L,   L,   -4
#else
    PTR_ADDI    L,   L,   -1
#endif
    PTR_SLLI    T0,  L,  0x04
    PTR_ADD     A0,  A0, T0
    PTR_SLLI    T0,  L,  0x02
    PTR_ADD     B0,  B0, T0
#endif

#ifdef LEFT
    PTR_ADDI    OFF,   OFF,  0x04
#endif
#endif   // #if defined(TRMMKERNEL)
.L_N1_M2:
    andi      I,   M,   2
    beq       ZERO,I,   .L_N1_M1
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    PTR_SLLI   T0,    OFF,  0x03
    PTR_ADD    A0,    A0,   T0
    PTR_SLLI   T0,    OFF,  0x02
    PTR_ADD    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    PTR_SUB    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    PTR_ADDI   L,     OFF,  2
#else
    /* number of values in B */
    PTR_ADDI   L,     OFF,  1
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif
    KERNEL1xMx1_START 2, 0x08
    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N1_M2_L7 */
    beq       ZERO,TL, .L_N1_M2_L7
.align 5
.L_N1_M2_TL1: /* TL-- */
    KERNEL8xMx1 2, 0x08

    PTR_ADDI    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N1_M2_TL1
.L_N1_M2_L7:
    /* if (!(L & 7)) goto L_N1_M2_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N1_M2_L0
.align 5
.L_N1_M2_L71:
    KERNEL1xMx1 2, 0x08
    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_N1_M2_L71
.L_N1_M2_L0:
    SAVEMx1 2, 0x08
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    PTR_SUB     L,   K,   OFF
#ifdef LEFT
    PTR_ADDI    L,   L,   -2
#else
    PTR_ADDI    L,   L,   -1
#endif
    PTR_SLLI    T0,  L,  0x03
    PTR_ADD     A0,  A0, T0
    PTR_SLLI    T0,  L,  0x02
    PTR_ADD     B0,  B0, T0
#endif

#ifdef LEFT
    PTR_ADDI    OFF,   OFF,  0x02
#endif
#endif   // #if defined(TRMMKERNEL)

.L_N1_M1:
    andi      I,   M,   1
    beq       ZERO,I,   .L_N1_M0

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    PTR_SLLI   T0,    OFF,  0x02
    PTR_ADD    A0,    A0,   T0
    PTR_ADD    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    PTR_SUB    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    PTR_ADDI   L,     OFF,  1
#else
    /* number of values in B */
    PTR_ADDI   L,     OFF,  1
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif
    KERNEL1xMx1_START 1, 0x04
    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N1_M1_L7 */
    beq       ZERO,TL, .L_N1_M1_L7
.align 5
.L_N1_M1_TL1: /* TL-- */
    KERNEL8xMx1 1, 0x04

    PTR_ADDI    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N1_M1_TL1
.L_N1_M1_L7:
    /* if (!(L & 7)) goto L_N1_M1_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N1_M1_L0
.align 5
.L_N1_M1_L71:
    KERNEL1xMx1 1, 0x04
    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_N1_M1_L71
.L_N1_M1_L0:
    SAVEMx1 1, 0x04
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    PTR_SUB     L,   K,   OFF
#ifdef LEFT
    PTR_ADDI    L,   L,   -1
#else
    PTR_ADDI    L,   L,   -1
#endif
    PTR_SLLI    T0,  L,  0x02
    PTR_ADD     A0,  A0, T0
    PTR_ADD     B0,  B0, T0
#endif

#ifdef LEFT
    PTR_ADDI    OFF,   OFF,  0x01
#endif
#endif   // #if defined(TRMMKERNEL)
.L_N1_M0:
.L_N0:
    pop_if_used 9, 8
    jirl    $r0, $r1, 0x0
    EPILOGUE
