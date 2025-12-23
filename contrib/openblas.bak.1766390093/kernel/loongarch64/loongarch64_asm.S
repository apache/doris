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

#if __loongarch_grlen == 64
#define LA_REG    int64_t
#define REG_SIZE  8
#define REG_LOG   3
#define PTR_ADDI  addi.d
#define PTR_ADD   add.d
#define PTR_SUB   sub.d
#define PTR_LD    ld.d
#define PTR_ST    st.d
#define PTR_SLLI  slli.d
#define PTR_SRLI  srli.d
#define PTR_SRAI  srai.d
#define PTR_MUL   mul.d
#define PTR_ALSL  alsl.d
#elif __loongarch_grlen == 32
#define LA_REG    int32_t
#define REG_SIZE  4
#define REG_LOG   2
#define PTR_ADDI  addi.w
#define PTR_ADD   add.w
#define PTR_SUB   sub.w
#define PTR_LD    ld.w
#define PTR_ST    st.w
#define PTR_SLLI  slli.w
#define PTR_SRLI  srli.w
#define PTR_SRAI  srai.w
#define PTR_MUL   mul.w
#define PTR_ALSL  alsl.w
#else
// If neither of the above two conditions is supported, it means this is an early
// internal toolchain. To ensure maximum compatibility, the following approach is taken:
#define LA_REG    int64_t
#define REG_SIZE  8
#define REG_LOG   3
#define PTR_ADDI  addi.d
#define PTR_ADD   add.d
#define PTR_SUB   sub.d
#define PTR_LD    ld.d
#define PTR_ST    st.d
#define PTR_SLLI  slli.d
#define PTR_SRLI  srli.d
#define PTR_SRAI  srai.d
#define PTR_MUL   mul.d
#define PTR_ALSL  alsl.d
#endif

#if __loongarch_frlen == 64
#define FREG_SIZE 8
#define FREG_LOG  3
#define PTR_FLD   fld.d
#define PTR_FST   fst.d
#elif __loongarch_frlen == 32
#define FREG_SIZE 4
#define FREG_LOG  2
#define PTR_FLD   fld.s
#define PTR_FST   fst.s
#else
// If neither of the above two conditions is supported, it means this is an early
// internal toolchain. To ensure maximum compatibility, the following approach is taken:
#define FREG_SIZE 8
#define FREG_LOG  3
#define PTR_FLD   fld.d
#define PTR_FST   fst.d
#endif

.altmacro // Enable alternate macro mode

/*
 * Pushing and popping static registers into/from the stack.
 * regs : number of static general-purpose registers, greater than or equal to 0, less than or equal to 9
 * fregs: number of static floating-point registers, greater than or equal to 0, less than or equal to 8
 */
.macro push_if_used regs, fregs
.if \regs > 0
    PTR_ADDI    $sp,    $sp,    -(\regs << REG_LOG)
    push_regs 0, \regs - 1
.endif
.if \fregs > 0
    PTR_ADDI     $sp,    $sp,    -(\fregs << FREG_LOG)
    push_fregs 0, \fregs - 1
.endif
.endm // End push_if_used

.macro pop_if_used regs, fregs
.if \fregs > 0
    pop_fregs 0, \fregs - 1
    PTR_ADDI    $sp,    $sp,     \fregs << FREG_LOG
.endif
.if \regs > 0
    pop_regs 0, \regs - 1
    PTR_ADDI   $sp,    $sp,      \regs << REG_LOG
.endif
.endm // End pop_if_used

.macro push_regs from, to
#ifdef __clang__
.if \to >= 0
    PTR_ST    $s0, $sp, 0 << REG_LOG
.endif
.if \to >= 1
    PTR_ST    $s1, $sp, 1 << REG_LOG
.endif
.if \to >= 2
    PTR_ST    $s2, $sp, 2 << REG_LOG
.endif
.if \to >= 3
    PTR_ST    $s3, $sp, 3 << REG_LOG
.endif
.if \to >= 4
    PTR_ST    $s4, $sp, 4 << REG_LOG
.endif
.if \to >= 5
    PTR_ST    $s5, $sp, 5 << REG_LOG
.endif
.if \to >= 6
    PTR_ST    $s6, $sp, 6 << REG_LOG
.endif
.if \to >= 7
    PTR_ST    $s7, $sp, 7 << REG_LOG
.endif
.if \to >= 8
    PTR_ST    $s8, $sp, 8 << REG_LOG
.endif
#else
    PTR_ST    $s\()\from,       $sp,    \from << REG_LOG
.if \to - \from
    push_regs %from + 1, \to
.endif
#endif
.endm // End push_regs

.macro pop_regs from, to
#ifdef __clang__
.if \to >= 0
    PTR_LD    $s0, $sp, 0 << REG_LOG
.endif
.if \to >= 1
    PTR_LD    $s1, $sp, 1 << REG_LOG
.endif
.if \to >= 2
    PTR_LD    $s2, $sp, 2 << REG_LOG
.endif
.if \to >= 3
    PTR_LD    $s3, $sp, 3 << REG_LOG
.endif
.if \to >= 4
    PTR_LD    $s4, $sp, 4 << REG_LOG
.endif
.if \to >= 5
    PTR_LD    $s5, $sp, 5 << REG_LOG
.endif
.if \to >= 6
    PTR_LD    $s6, $sp, 6 << REG_LOG
.endif
.if \to >= 7
    PTR_LD    $s7, $sp, 7 << REG_LOG
.endif
.if \to >= 8
    PTR_LD    $s8, $sp, 8 << REG_LOG
.endif
#else
    PTR_LD    $s\()\from,     $sp,    \from << REG_LOG
.if \to - \from
    pop_regs %from + 1, \to
.endif
#endif
.endm // End pop_regs

.macro push_fregs from, to
#ifdef __clang__
.if \to >= 0
    PTR_FST    $fs0, $sp, 0 << FREG_LOG
.endif
.if \to >= 1
    PTR_FST    $fs1, $sp, 1 << FREG_LOG
.endif
.if \to >= 2
    PTR_FST    $fs2, $sp, 2 << FREG_LOG
.endif
.if \to >= 3
    PTR_FST    $fs3, $sp, 3 << FREG_LOG
.endif
.if \to >= 4
    PTR_FST    $fs4, $sp, 4 << FREG_LOG
.endif
.if \to >= 5
    PTR_FST    $fs5, $sp, 5 << FREG_LOG
.endif
.if \to >= 6
    PTR_FST    $fs6, $sp, 6 << FREG_LOG
.endif
.if \to >= 7
    PTR_FST    $fs7, $sp, 7 << FREG_LOG
.endif
#else
    PTR_FST   $fs\()\from,    $sp,    \from << FREG_LOG
.if \to - \from
    push_fregs %from + 1, \to
.endif
#endif
.endm // End push_fregs

.macro pop_fregs from, to
#ifdef __clang__
.if \to >= 0
    PTR_FLD    $fs0, $sp, 0 << FREG_LOG
.endif
.if \to >= 1
    PTR_FLD    $fs1, $sp, 1 << FREG_LOG
.endif
.if \to >= 2
    PTR_FLD    $fs2, $sp, 2 << FREG_LOG
.endif
.if \to >= 3
    PTR_FLD    $fs3, $sp, 3 << FREG_LOG
.endif
.if \to >= 4
    PTR_FLD    $fs4, $sp, 4 << FREG_LOG
.endif
.if \to >= 5
    PTR_FLD    $fs5, $sp, 5 << FREG_LOG
.endif
.if \to >= 6
    PTR_FLD    $fs6, $sp, 6 << FREG_LOG
.endif
.if \to >= 7
    PTR_FLD    $fs7, $sp, 7 << FREG_LOG
.endif
#else
    PTR_FLD   $fs\()\from,    $sp,    \from << FREG_LOG
.if \to - \from
    pop_fregs %from + 1, \to
.endif
#endif
.endm // End pop_fregs

//
// Instruction Related Macros
//
// GLD
//
.macro GLD pre_op:req, suf_op=0, out:req, src:req, offset:req/* imm */, more:vararg
.ifeqs "\suf_op", "0"
    \pre_op\()ld    \out,   \src,   \offset
.else
    \pre_op\()ld.\suf_op    \out,   \src,   \offset
.endif
.ifnb \more
    GLD \pre_op, \suf_op, \more
.endif
.endm

//
// GLD_INC
//
.macro GLD_INC pre_op:req, suf_op=0, inc:req, out:req, src:req, offset:req/* imm */, more:vararg
.ifeqs "\suf_op", "0"
    \pre_op\()ld    \out,   \src,   \offset
.else
    \pre_op\()ld.\suf_op    \out,   \src,   \offset
.endif
    PTR_ADDI  \src,   \src,   \inc
.ifnb \more
    GLD_INC \pre_op, \suf_op, \inc, \more
.endif
.endm
//
// GLDX is same as GLD except the stride is a register
//
.macro GLDX pre_op:req, suf_op=0, out:req, src:req, offset:req/* reg */, more:vararg
.ifeqs "\suf_op", "0"
    \pre_op\()ldx    \out,   \src,   \offset
.else
    \pre_op\()ldx.\suf_op    \out,   \src,   \offset
.endif
.ifnb \more
    GLDX \pre_op, \suf_op, \more
.endif
.endm
//
// GLDREPL
//
.macro GLDREPL pre_op:req, suf_op:req, out:req, src:req, offset:req/* imm */, more:vararg
    \pre_op\()ldrepl.\suf_op    \out,   \src,   \offset
.ifnb \more
    GLDREPL  \pre_op, \suf_op, \more
.endif
.endm
//
// GST
//
.macro GST pre_op:req, suf_op=0, src:req, dst:req, offset:req/* imm */, more:vararg
.ifeqs "\suf_op", "0"
    \pre_op\()st    \src,   \dst,   \offset
.else
    \pre_op\()st.\suf_op \src,  \dst, \offset
.endif
.ifnb \more
    GST \pre_op, \suf_op, \more
.endif
.endm
//
// GMUL
//
.macro GMUL pre_op, suf_op:req, out:req, in0:req, in1:req, more:vararg
    \pre_op\()mul.\suf_op   \out,   \in0,   \in1
.ifnb \more
    GMUL \pre_op, \suf_op, \more
.endif
.endm
//
// GMADD
//
.macro GMADD pre_op, suf_op:req, out:req, in0:req, in1:req, in2:req, more:vararg
    \pre_op\()madd.\suf_op \out, \in0, \in1, \in2
.ifnb \more
    GMADD \pre_op, \suf_op, \more
.endif
.endm
//
// GADD
//
.macro GADD pre_op, suf_op:req, out:req, in0:req, in1:req, more:vararg
    \pre_op\()add.\suf_op \out, \in0, \in1
.ifnb \more
    GADD \pre_op, \suf_op, \more
.endif
.endm
//
// GADDI
//
.macro GADDI pre_op, suf_op:req, out:req, in0:req, in1:req, more:vararg
    \pre_op\()addi.\suf_op  \out,   \in0,   \in1
.ifnb \more
    GADDI \pre_op, \suf_op, \more
.endif
.endm
//
// GSUB
//
.macro GSUB pre_op, suf_op:req, out:req, in0:req, in1:req, more:vararg
    \pre_op\()sub.\suf_op \out, \in0, \in1
.ifnb \more
    GSUB \pre_op, \suf_op, \more
.endif
.endm
//
// GSLLI
//
.macro GSLLI pre_op, suf_op:req, out:req, in0:req, in1:req, more:vararg
    \pre_op\()slli.\suf_op  \out,   \in0,   \in1
.ifnb \more
    GSLLI \pre_op, \suf_op, \more
.endif
.endm
//
// GINSVE0
//
.macro GINSVE0 pre_op:req, suf_op:req, out:req, in0:req, in1:req, more:vararg
    \pre_op\()insve0.\suf_op    \out,   \in0,   \in1
.ifnb \more
    GINSVE0 \pre_op, \suf_op, \more
.endif
.endm
//
// GXOR
//
.macro GXOR pre_op:req, suf_op:req, out:req, in0:req, in1:req, more:vararg
.ifnb \pre_op
    \pre_op\()xor.v    \out,   \in0,   \in1
.else
    xor.\suf_op        \out,   \in0,   \in1
.endif
.ifnb \more
    GXOR \pre_op, \suf_op, \more
.endif
.endm
//
// GPERMI
//
.macro GPERMI pre_op:req, suf_op:req, out:req, in0:req, in1:req, more:vararg
    \pre_op\()permi.\suf_op     \out,   \in0,   \in1
.ifnb \more
    GPERMI \pre_op, \suf_op, \more
.endif
.endm
//
// GNMSUB
//
.macro GNMSUB pre_op:req, suf_op:req, out:req, in0:req, in1:req, in2:req, more:vararg
    \pre_op\()nmsub.\suf_op     \out,   \in0,   \in1,   \in2
.ifnb \more
    GNMSUB \pre_op, \suf_op, \more
.endif
.endm
//
// GPRELD
//
.macro GPRELD in0:req, in1:req, in2:req, more:vararg
    preld \in0, \in1, \in2
.ifnb \more
    GPRELD \more
.endif
.endm
//
// GPACKEV
//
.macro GPACKEV pre_op:req, suf_op:req, out:req, in0:req, in1:req, more:vararg
    \pre_op\()packev.\suf_op   \out,  \in0,  \in1
.ifnb \more
    GPACKEV \pre_op, \suf_op, \more
.endif
.endm
//
// GPACKOD
//
.macro GPACKOD pre_op:req, suf_op:req, out:req, in0:req, in1:req, more:vararg
    \pre_op\()packod.\suf_op   \out,  \in0,  \in1
.ifnb \more
    GPACKOD \pre_op, \suf_op, \more
.endif
.endm
//
// GSHUF4I
//
.macro GSHUF4I pre_op:req, suf_op:req, out:req, in0:req, in1:req /* imm */, more:vararg
    \pre_op\()shuf4i.\suf_op  \out,  \in0,  \in1
.ifnb \more
    GSHUF4I \pre_op, \suf_op, \more
.endif
.endm

.macro TRANSF2G name, pre_op:req, suf_op:req, more:vararg
.ifeqs "\pre_op\()\suf_op", "vfs"
    \name v, w, \more
.endif
.ifeqs "\pre_op\()\suf_op", "vfd"
    \name v, d, \more
.endif
.ifeqs "\pre_op\()\suf_op", "xvfs"
    \name  xv, w, \more
.endif
.ifeqs "\pre_op\()\suf_op", "xvfd"
    \name xv, d, \more
.endif
.endm

//
// Compound instructions
//
// GACC: Accumulate the values of vector registers
//
.macro GACC pre_op:req, suf_op:req, out:req, in:req, more:vararg
.ifeqs "\pre_op\()\suf_op", "xvfd"
    xvpermi.q              \out,   \in,    0x01
    \pre_op\()add.\suf_op  \in,    \out,   \in
    xvpackod.d             \out,   \in,    \in
    \pre_op\()add.\suf_op  \out,   \out,   \in
.endif
.ifeqs "\pre_op\()\suf_op", "xvfs"
    xvpermi.q              \out,   \in,    0x01
    \pre_op\()add.\suf_op  \in,    \out,   \in
    xvpackod.d             \out,   \in,    \in
    \pre_op\()add.\suf_op  \out,   \out,   \in
    xvpackod.w             \in,    \out,   \out
    \pre_op\()add.\suf_op  \out,   \out,   \in
.endif
.ifeqs "\pre_op\()\suf_op", "vfd"
    vpackod.d              \out,   \in,    \in
    \pre_op\()add.\suf_op  \out,   \out,   \in
.endif
.ifeqs "\pre_op\()\suf_op", "vfs"
    vpackod.d              \out,   \in,    \in
    \pre_op\()add.\suf_op  \out,   \out,   \in
    vpackod.w              \in,    \out,   \out
    \pre_op\()add.\suf_op  \out,   \out,   \in
.endif

.ifeqs "\pre_op\()\suf_op", "xvd"
    xvpermi.q              \out,   \in,    0x01
    \pre_op\()add.\suf_op  \in,    \out,   \in
    xvpackod.d             \out,   \in,    \in
    \pre_op\()add.\suf_op  \out,   \out,   \in
.endif
.ifeqs "\pre_op\()\suf_op", "xvw"
    xvpermi.q              \out,   \in,    0x01
    \pre_op\()add.\suf_op  \in,    \out,   \in
    xvpackod.d             \out,   \in,    \in
    \pre_op\()add.\suf_op  \out,   \out,   \in
    xvpackod.w             \in,    \out,   \out
    \pre_op\()add.\suf_op  \out,   \out,   \in
.endif
.ifeqs "\pre_op\()\suf_op", "xvh"
    xvpermi.q              \out,   \in,    0x01
    \pre_op\()add.\suf_op  \in,    \out,   \in
    xvpackod.d             \out,   \in,    \in
    \pre_op\()add.\suf_op  \out,   \out,   \in
    xvpackod.w             \in,    \out,   \out
    \pre_op\()add.\suf_op  \out,   \out,   \in
    xvpackod.h             \in,    \out,   \out
    \pre_op\()add.\suf_op  \out,   \out,   \in
.endif
.ifeqs "\pre_op\()\suf_op", "xvb"
    xvpermi.q              \out,   \in,    0x01
    \pre_op\()add.\suf_op  \in,    \out,   \in
    xvpackod.d             \out,   \in,    \in
    \pre_op\()add.\suf_op  \out,   \out,   \in
    xvpackod.w             \in,    \out,   \out
    \pre_op\()add.\suf_op  \out,   \out,   \in
    xvpackod.h             \in,    \out,   \out
    \pre_op\()add.\suf_op  \out,   \out,   \in
    xvpackod.b             \in,    \out,   \out
    \pre_op\()add.\suf_op  \out,   \out,   \in
.endif

.ifeqs "\pre_op\()\suf_op", "vd"
    vpackod.d              \out,   \in,    \in
    \pre_op\()add.\suf_op  \out,   \out,   \in
.endif
.ifeqs "\pre_op\()\suf_op", "vw"
    vpackod.d              \out,   \in,    \in
    \pre_op\()add.\suf_op  \out,   \out,   \in
    vpackod.w              \in,    \out,   \out
    \pre_op\()add.\suf_op  \out,   \out,   \in
.endif
.ifeqs "\pre_op\()\suf_op", "vh"
    vpackod.d              \out,   \in,    \in
    \pre_op\()add.\suf_op  \out,   \out,   \in
    vpackod.w              \in,    \out,   \out
    \pre_op\()add.\suf_op  \out,   \out,   \in
    vpackod.h              \in,    \out,   \out
    \pre_op\()add.\suf_op  \out,   \out,   \in
.endif
.ifeqs "\pre_op\()\suf_op", "vb"
    vpackod.d              \out,   \in,    \in
    \pre_op\()add.\suf_op  \out,   \out,   \in
    vpackod.w              \in,    \out,   \out
    \pre_op\()add.\suf_op  \out,   \out,   \in
    vpackod.h              \in,    \out,   \out
    \pre_op\()add.\suf_op  \out,   \out,   \in
    vpackod.b              \in,    \out,   \out
    \pre_op\()add.\suf_op  \out,   \out,   \in
.endif

.ifnb \more
    GACC \pre_op, \suf_op, \more
.endif
.endm
//
// GMOV
//
.macro GMOV pre_op:req, out:req, in:req, more:vararg
    \pre_op\()or.v  \out, \in, \in
.ifnb \more
    GMOV \pre_op, \more
.endif
.endm

//
// GCOMPLEXACC: Complex accumulate the values of vector registers
// pre_op: xvf or vf, differentiate between LSX or LASX instruction
// suf_op: s or d, differentiate between single precision or double precision complex numbers
// Note: When "pre_op = xvf && suf_op = s", in will be modified.
//
.macro GCOMPLEXACC pre_op:req, suf_op:req, out:req, in:req, more:vararg
.ifeqs "\pre_op\()\suf_op", "xvfd"
    xvpermi.q              \out,   \in,    0x01
    \pre_op\()add.\suf_op  \out,   \out,   \in
.endif

.ifeqs "\pre_op\()\suf_op", "xvfs"
    xvpermi.q              \out,   \in,    0x01
    \pre_op\()add.\suf_op  \in,    \out,   \in
    xvpackod.d             \out,   \in,    \in
    \pre_op\()add.\suf_op  \out,   \out,   \in
.endif

.ifeqs "\pre_op\()\suf_op", "vfd"
    vor.v \out, \in, \in
.endif

.ifeqs "\pre_op\()\suf_op", "vfs"
    vpackod.d              \out,   \in,    \in
    \pre_op\()add.\suf_op  \out,   \out,   \in
.endif

.ifnb \more
    GCOMPLEXACC \pre_op, \suf_op, \more
.endif
.endm

//
// GCOMPLEXMUL: Complex multiplication, out = in0 * in1
// xconj: default value 0.
// if !(xconj)
//     out_r = in0_r * in1_r - in0_i * in1_i;
//     out_i = in0_r * in1_i + in0_i * in1_r;
// else
//     out_r = in0_r * in1_r + in0_i * in1_i;
//     out_i = in0_r * in1_i - in0_i * in1_r;
// pre_op: xvf or vf, differentiate between LSX or LASX instruction
// suf_op: s or d, differentiate between single precision or double precision complex numbers
//
.macro GCOMPLEXMUL xconj=0, pre_op:req, suf_op:req, out:req, in0:req, in1:req, tmp0:req, tmp1:req, tmp2:req, more:vararg
    TRANSF2G GXOR, \pre_op, s, \tmp1, \tmp1, \tmp1
    TRANSF2G GPACKEV, \pre_op, \suf_op, \tmp0, \in0, \in0

    \pre_op\()sub.\suf_op    \tmp1,     \tmp1,     \in0

.ifeqs "\xconj", "0"
    TRANSF2G GPACKOD, \pre_op, \suf_op, \tmp1, \in0, \tmp1
.else
    TRANSF2G GPACKOD, \pre_op, \suf_op, \tmp1, \tmp1, \in0
.endif

.ifeqs "\suf_op", "s"
    TRANSF2G GSHUF4I, \pre_op, \suf_op, \tmp2, \in1, 0xb1
.else
    TRANSF2G GSHUF4I, \pre_op, \suf_op, \tmp2, \in1, 0x0b
.endif

    \pre_op\()mul.\suf_op    \out,      \tmp0,     \in1
    \pre_op\()madd.\suf_op   \out,      \tmp1,     \tmp2,     \out

.ifnb \more
    GCOMPLEXMUL \xconj, \pre_op, \suf_op, \more
.endif
.endm

//
// GCOMPLEXMADD: Complex multiply-accumulate, out = in0 * in1 + in2
// xconj: default value 0
// conj: default value 0
// if !(CONJ)
// if !(XCONJ)
//     out_r = in0_r * in1_r - in0_i * in1_i + in2_r;
//     out_i = in0_r * in1_i + in0_i * in1_r + in2_i;
// else
//     out_r = in0_r * in1_r + in0_i * in1_i + in2_r;
//     out_i = in0_r * in1_i - in0_i * in1_r + in2_i;
// else
// if !(XCONJ)
//     out_r = in0_r * in1_r + in0_i * in1_i + in2_r;
//     out_i = in2_i - (in0_r * in1_i - in0_i * in1_r);
// else
//     out_r = in0_r * in1_r   - in0_i * in1_i + in2_r;
//     out_i = in2_i - (in0_r * in1_i + in0_i * in1_r);
// pre_op: xvf or vf, differentiate between LSX or LASX instruction
// suf_op: s or d, differentiate between single precision or double precision complex numbers
//
.macro GCOMPLEXMADD xconj=0, conj=0, pre_op:req, suf_op:req, out:req, in0:req, in1:req, in2:req, tmp0:req, tmp1:req, tmp2:req, more:vararg
    TRANSF2G GXOR, \pre_op, s, \tmp1, \tmp1, \tmp1
    TRANSF2G GPACKEV, \pre_op, \suf_op, \tmp0, \in0, \in0

    \pre_op\()madd.\suf_op   \tmp2,      \tmp0,     \in1,      \in2

.ifeqs "\conj\()\suf_op", "1s"
    \pre_op\()nmsub.\suf_op  \tmp0,      \tmp0,     \in1,      \in2
    TRANSF2G GSHUF4I, \pre_op, \suf_op, \tmp0, \tmp0, 0xb1
    TRANSF2G GPACKEV, \pre_op, \suf_op, \out, \tmp0, \tmp2
.endif
.ifeqs "\conj\()\suf_op", "1d"
    \pre_op\()nmsub.\suf_op  \tmp0,      \tmp0,     \in1,      \in2
    TRANSF2G GSHUF4I, \pre_op, \suf_op, \tmp0, \tmp0, 0x0b
    TRANSF2G GPACKEV, \pre_op, \suf_op, \out, \tmp0, \tmp2
.endif
.ifeqs "\conj", "0"
    \pre_op\()add.\suf_op    \out,     \tmp2,     \tmp1
.endif

    \pre_op\()sub.\suf_op    \tmp1,     \tmp1,     \in0

.ifeqs "\xconj\()\conj\()\suf_op", "00s"
    TRANSF2G GPACKOD, \pre_op, \suf_op, \tmp1, \in0, \tmp1
    TRANSF2G GSHUF4I, \pre_op, \suf_op, \tmp2, \in1, 0xb1
.endif
.ifeqs "\xconj\()\conj\()\suf_op", "10s"
    TRANSF2G GPACKOD, \pre_op, \suf_op, \tmp1, \tmp1, \in0
    TRANSF2G GSHUF4I, \pre_op, \suf_op, \tmp2, \in1, 0xb1
.endif
.ifeqs "\xconj\()\conj\()\suf_op", "01s"
    TRANSF2G GPACKOD, \pre_op, \suf_op, \tmp1, \in0, \in0
    TRANSF2G GSHUF4I, \pre_op, \suf_op, \tmp2, \in1, 0xb1
.endif
.ifeqs "\xconj\()\conj\()\suf_op", "11s"
    TRANSF2G GPACKOD, \pre_op, \suf_op, \tmp1, \tmp1, \tmp1
    TRANSF2G GSHUF4I, \pre_op, \suf_op, \tmp2, \in1, 0xb1
.endif
.ifeqs "\xconj\()\conj\()\suf_op", "00d"
    TRANSF2G GPACKOD, \pre_op, \suf_op, \tmp1, \in0, \tmp1
    TRANSF2G GSHUF4I, \pre_op, \suf_op, \tmp2, \in1, 0x0b
.endif
.ifeqs "\xconj\()\conj\()\suf_op", "10d"
    TRANSF2G GPACKOD, \pre_op, \suf_op, \tmp1, \tmp1, \in0
    TRANSF2G GSHUF4I, \pre_op, \suf_op, \tmp2, \in1, 0x0b
.endif
.ifeqs "\xconj\()\conj\()\suf_op", "01d"
    TRANSF2G GPACKOD, \pre_op, \suf_op, \tmp1, \in0, \in0
    TRANSF2G GSHUF4I, \pre_op, \suf_op, \tmp2, \in1, 0x0b
.endif
.ifeqs "\xconj\()\conj\()\suf_op", "11d"
    TRANSF2G GPACKOD, \pre_op, \suf_op, \tmp1, \tmp1, \tmp1
    TRANSF2G GSHUF4I, \pre_op, \suf_op, \tmp2, \in1, 0x0b
.endif

    \pre_op\()madd.\suf_op   \out,      \tmp1,     \tmp2,     \out

.ifnb \more
    GCOMPLEXMADD \xconj, \conj, \pre_op, \suf_op, \more
.endif
.endm

//
// Media Related Macros
//
.macro GSBUTTERFLY pre_op, suf_op, out0, out1, in0, in1
    \pre_op\()ilvl.\suf_op \out0, \in0, \in1
    \pre_op\()ilvh.\suf_op \out1, \in0, \in1
.endm
.macro GINTERLACE pre_op, suf_op, out0, out1, in0, in1
    \pre_op\()pickev.\suf_op \out0, \in0, \in1
    \pre_op\()pickod.\suf_op \out1, \in0, \in1
.endm

//
// TRANSPOSE4x4_D: Transpose 4x4 block with double-word elements in vectors,
// has no pre_op param. 128-bit vector instructions are not supported.
//
.macro GTRANSPOSE4x4_D in0, in1, in2, in3, out0, out1, out2, out3, \
                       vt0, vt1
    GSBUTTERFLY xv, d, \vt0, \out1, \in1, \in0
    GSBUTTERFLY xv, d, \vt1, \out3, \in3, \in2
    GMOV xv, \out0, \vt0, \out2, \vt1, \vt1, \out3
    GPERMI xv, q, \out0, \out2, 0x02, \out2, \vt0, 0x31, \out3, \out1, 0x31, \out1, \vt1, 0x02
.endm

.macro GTRANSPOSE8x8_W out0, out1, out2, out3, out4, out5, out6, out7, \
                       in0, in1, in2, in3, in4, in5, in6, in7, \
                       tmp0, tmp1, tmp2, tmp3
    GSBUTTERFLY xv, w, \tmp0, \tmp2, \in2, \in0
    GSBUTTERFLY xv, w, \tmp1, \tmp3, \in3, \in1
    GSBUTTERFLY xv, w, \out0, \out1, \tmp1, \tmp0
    GSBUTTERFLY xv, w, \out2, \out3, \tmp3, \tmp2

    GSBUTTERFLY xv, w, \tmp0, \tmp2, \in6, \in4
    GSBUTTERFLY xv, w, \tmp1, \tmp3, \in7, \in5
    GSBUTTERFLY xv, w, \out4, \out5, \tmp1, \tmp0
    GSBUTTERFLY xv, w, \out6, \out7, \tmp3, \tmp2

    GMOV xv, \tmp0, \out0, \tmp1, \out1, \tmp2, \out2, \tmp3, \out3

    GPERMI xv, q, \out0, \out4, 0x02, \out1, \out5, 0x02, \
                  \out2, \out6, 0x02, \out3, \out7, 0x02, \
                  \out4, \tmp0, 0x31, \out5, \tmp1, 0x31, \
                  \out6, \tmp2, 0x31, \out7, \tmp3, 0x31
.endm
