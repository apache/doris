/***************************************************************************
Copyright (c) 2023, The OpenBLAS Project
Copyright (c) 2022, Arm Ltd
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
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*****************************************************************************/

#include "common.h"

#include <arm_sve.h>

#ifdef DOUBLE
#define DTYPE "d"
#define WIDTH "d"
#define SHIFT "3"
#else
#define DTYPE "s"
#define WIDTH "w"
#define SHIFT "2"
#endif

#define COUNT \
"        cnt"WIDTH"    x9                                   \n"
#define SETUP_TRUE \
"        ptrue   p0."DTYPE"                              \n"
#define OFFSET_INPUTS                                     \
"        add     x12, %[X_], x9, lsl #"SHIFT"               \n" \
"        add     x13, %[Y_], x9, lsl #"SHIFT"               \n"
#define TAIL_WHILE                                        \
"        whilelo p1."DTYPE", x8, x0                         \n"
#define UPDATE(pg, x,y,out)                               \
"        ld1"WIDTH"    { z2."DTYPE" }, "pg"/z, ["x", x8, lsl #"SHIFT"]  \n" \
"        ld1"WIDTH"    { z3."DTYPE" }, "pg"/z, ["y", x8, lsl #"SHIFT"]  \n" \
"        fmla    "out"."DTYPE", "pg"/m, z2."DTYPE", z3."DTYPE"      \n"
#define SUM_VECTOR(v) \
"        faddv   "DTYPE""v", p0, z"v"."DTYPE"                     \n"
#define RET \
"        fadd    %"DTYPE"[RET_], "DTYPE"1, "DTYPE"0                     \n"

#define DOT_KERNEL                                        \
        COUNT                                             \
"        mov     z1.d, #0                             \n" \
"        mov     z0.d, #0                             \n" \
"        mov     x8, #0                               \n" \
"        movi    d1, #0x0                             \n" \
        SETUP_TRUE                                        \
"        neg     x10, x9, lsl #1                      \n" \
"        ands    x11, x10, x0                         \n" \
"        b.eq    2f // skip_2x                        \n" \
        OFFSET_INPUTS                                     \
"1: // vector_2x                                      \n" \
        UPDATE("p0", "%[X_]", "%[Y_]", "z1") \
        UPDATE("p0", "x12", "x13", "z0") \
"        sub     x8, x8, x10                          \n" \
"        cmp     x8, x11                              \n" \
"        b.lo    1b // vector_2x                      \n" \
        SUM_VECTOR("1") \
"2: // skip_2x                                        \n" \
"        neg     x10, x9                              \n" \
"        and     x10, x10, x0                         \n" \
"        cmp     x8, x10                              \n" \
"        b.hs    4f // tail                           \n" \
"3: // vector_1x                                      \n" \
        UPDATE("p0", "%[X_]", "%[Y_]", "z0")              \
"        add     x8, x8, x9                           \n" \
"        cmp     x8, x10                              \n" \
"        b.lo    3b // vector_1x                      \n" \
"4: // tail                                           \n" \
"        cmp     x10, x0                              \n" \
"        b.eq    5f // end                            \n" \
        TAIL_WHILE                                        \
        UPDATE("p1", "%[X_]", "%[Y_]", "z0")              \
"5: // end                                            \n" \
        SUM_VECTOR("0") \
        RET

static
FLOAT
dot_kernel_sve(BLASLONG n, FLOAT* x, FLOAT* y)
{
  FLOAT ret;

  asm(DOT_KERNEL
      :
        [RET_] "=&w" (ret)
      :
        [N_] "r" (n),
        [X_] "r" (x),
        [Y_] "r" (y)
      : "cc",
        "memory",
        "x0", "x1", "x2", "x3", "x4", "x5", "x6", "x7",
        "x8", "x9", "x10", "x11", "x12", "x13", "d1", 
        "z0", "z1"
  );

  return ret;
}
