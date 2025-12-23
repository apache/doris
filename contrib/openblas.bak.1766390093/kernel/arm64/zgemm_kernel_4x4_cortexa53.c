/***************************************************************************
Copyright (c) 2021, The OpenBLAS Project
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
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A00 PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE OPENBLAS PROJECT OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*****************************************************************************/

#include "common.h"
#include <arm_neon.h>

/*******************************************************************************
  The complex GEMM kernels in OpenBLAS use static configuration of conjugation
modes via specific macros:

  MACRO_NAME | conjugation on matrix A | conjugation on matrix B |
  ---------- | ----------------------- | ----------------------- |
 NN/NT/TN/TT |            No           |            No           |
 NR/NC/TR/TC |            No           |           Yes           |
 RN/RT/CN/CT |           Yes           |            No           |
 RR/RC/CR/CC |           Yes           |           Yes           |

  "conjugation on matrix A" means the complex conjugates of elements from
matrix A are used for matmul (rather than the original elements). "conjugation
on matrix B" means the complex conjugate of each element from matrix B is taken
for matrix multiplication, respectively.

  Complex numbers in arrays or matrices are usually packed together as an
array of struct (without padding):
  struct complex_number {
    FLOAT real_part;
    FLOAT imag_part;
  };

  For a double complex array ARR[] which is usually DEFINED AS AN ARRAY OF
DOUBLE, the real part of its Kth complex number can be accessed as
ARR[K * 2], the imaginary part of the Kth complex number is ARR[2 * K + 1].

  This file uses 2 ways to vectorize matrix multiplication of complex numbers:

(1) Expanded-form

  During accumulation along direction K:

                                        Σk(a[0][k].real b[k][n].real)
                  accumulate            Σk(a[0][k].imag b[k][n].real)
             ------------------->                     .
             |   * b[k][n].real                       .
             |     (broadcasted)                      .
    a[0][k].real                        Σk(a[v-1][k].real b[k][n].real)
    a[0][k].imag                        Σk(a[v-1][k].imag b[k][n].real)
         .                                         VECTOR I
(vec_a)  .
         .
    a[v-1][k].real                      Σk(a[0][k].real b[k][n].imag)
    a[v-1][k].imag                      Σk(a[0][k].imag b[k][n].imag)
             |                                        .
             |   accumulate                           .
             ------------------->                     .
                 * b[k][n].imag         Σk(a[v-1][k].real b[k][n].imag)
                  (broadcasted)         Σk(a[v-1][k].imag b[k][n].imag)
                                                   VECTOR II

  After accumulation, prior to storage:

                                    -1     -Σk(a[0][k].imag b[k][n].imag)
                                     1      Σk(a[0][k].real b[k][n].imag)
                                     .                 .
    VECTOR II permute and multiply   .  to get         .
                                     .                 .
                                    -1     -Σk(a[v-1][k].imag b[k][n].imag)
                                     1      Σk(a[v-1][k].real b[k][n].imag)

    then add with VECTOR I to get the result vector of elements of C.

  2 vector registers are needed for every v elements of C, with
v == sizeof(vector) / sizeof(complex)

(2) Contracted-form

  During accumulation along direction K:

   (the K coordinate is not shown, since the operation is identical for each k)

        (load vector in mem)                       (load vector in mem)
  a[0].r a[0].i ... a[v-1].r a[v-1].i     a[v].r a[v].i ... a[2v-1].r a[2v-1]i
              |                                                   |
              |      unzip operation (or VLD2 in arm neon)        |
              -----------------------------------------------------
                                            |
                                            |
                --------------------------------------------------
                |                                                |
                |                                                |
                v                                                v
   a[0].real ... a[2v-1].real                     a[0].imag ... a[2v-1].imag
               |         |                            |          | 
               |         | * b[i].imag(broadcast)     |          |
   * b[i].real |         -----------------------------|----      | * b[i].real
   (broadcast) |                                      |   |      | (broadcast)
               |        ------------------------------    |      |
             + |      - |       * b[i].imag(broadcast)  + |    + |
               v        v                                 v      v
              (accumulate)                              (accumulate)
        c[0].real ... c[2v-1].real                 c[0].imag ... c[2v-1].imag
               VECTOR_REAL                                VECTOR_IMAG

  After accumulation, VECTOR_REAL and VECTOR_IMAG are zipped (interleaved)
then stored to matrix C directly.

  For 2v elements of C, only 2 vector registers are needed, while
4 registers are required for expanded-form.
(v == sizeof(vector) / sizeof(complex))

  For AArch64 zgemm, 4x4 kernel needs 32 128-bit NEON registers
to store elements of C when using expanded-form calculation, where
the register spilling will occur. So contracted-form operation is
selected for 4x4 kernel. As for all other combinations of unroll parameters
(2x4, 4x2, 2x2, and so on), expanded-form mode is used to bring more
NEON registers into usage to hide latency of multiply-add instructions.
******************************************************************************/
 
static inline float64x2_t set_f64x2(double lo, double hi) {
  float64x2_t ret = vdupq_n_f64(0);
  ret = vsetq_lane_f64(lo, ret, 0);
  ret = vsetq_lane_f64(hi, ret, 1);
  return ret;
}

static inline float64x2x2_t expand_alpha(double alpha_r, double alpha_i) {
  float64x2x2_t ret = {{ set_f64x2(alpha_r, alpha_i), set_f64x2(-alpha_i, alpha_r) }};
  return ret;
}

/*****************************************************************
 * operation: *c += alpha * c_value //complex multiplication
 * expanded_alpha: { { alpha_r, alpha_i }, { -alpha_i, alpha_r }
 * expanded_c: {{ arbr, aibr }, { arbi, aibi }}
 ****************************************************************/
static inline void store_1c(double *c, float64x2x2_t expanded_c,
  float64x2x2_t expanded_alpha) {
  float64x2_t ld = vld1q_f64(c);
#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
  double real = vgetq_lane_f64(expanded_c.val[0], 0) - vgetq_lane_f64(expanded_c.val[1], 1);
  double imag = vgetq_lane_f64(expanded_c.val[0], 1) + vgetq_lane_f64(expanded_c.val[1], 0);
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
  double real = vgetq_lane_f64(expanded_c.val[0], 0) + vgetq_lane_f64(expanded_c.val[1], 1);
  double imag = vgetq_lane_f64(expanded_c.val[0], 1) - vgetq_lane_f64(expanded_c.val[1], 0);
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
  double real = vgetq_lane_f64(expanded_c.val[0], 0) + vgetq_lane_f64(expanded_c.val[1], 1);
  double imag = -vgetq_lane_f64(expanded_c.val[0], 1) + vgetq_lane_f64(expanded_c.val[1], 0);
#else
  double real = vgetq_lane_f64(expanded_c.val[0], 0) - vgetq_lane_f64(expanded_c.val[1], 1);
  double imag = -vgetq_lane_f64(expanded_c.val[0], 1) - vgetq_lane_f64(expanded_c.val[1], 0);
#endif
  ld = vfmaq_n_f64(ld, expanded_alpha.val[0], real);
  vst1q_f64(c, vfmaq_n_f64(ld, expanded_alpha.val[1], imag));
}

static inline void pref_c_4(const double *c) {
  __asm__ __volatile__("prfm pstl1keep,[%0]; prfm pstl1keep,[%0,#56]\n\t"::"r"(c):);
}

static inline float64x2x2_t add_ec(float64x2x2_t ec1, float64x2x2_t ec2) {
  float64x2x2_t ret = {{ vaddq_f64(ec1.val[0], ec2.val[0]),
    vaddq_f64(ec1.val[1], ec2.val[1]) }};
  return ret;
}

static inline float64x2x2_t update_ec(float64x2x2_t ec, float64x2_t a, float64x2_t b) {
  float64x2x2_t ret = {{ vfmaq_laneq_f64(ec.val[0], a, b, 0), vfmaq_laneq_f64(ec.val[1], a, b, 1) }};
  return ret;
}

static inline float64x2x2_t init() {
  float64x2x2_t ret = {{ vdupq_n_f64(0), vdupq_n_f64(0) }};
  return ret;
}

static inline void kernel_1x1(const double *sa, const double *sb, double *C,
  BLASLONG K, double alphar, double alphai) {

  const float64x2x2_t expanded_alpha = expand_alpha(alphar, alphai);
  float64x2x2_t c1, c2, c3, c4;
  c1 = c2 = c3 = c4 = init();

  for (; K > 3; K -= 4) {
    float64x2_t a1 = vld1q_f64(sa), a2 = vld1q_f64(sa + 2),
      a3 = vld1q_f64(sa + 4), a4 = vld1q_f64(sa + 6); sa += 8;
    float64x2_t b1 = vld1q_f64(sb), b2 = vld1q_f64(sb + 2),
      b3 = vld1q_f64(sb + 4), b4 = vld1q_f64(sb + 6); sb += 8;
    c1 = update_ec(c1, a1, b1);
    c2 = update_ec(c2, a2, b2);
    c3 = update_ec(c3, a3, b3);
    c4 = update_ec(c4, a4, b4);
  }
  c1 = add_ec(c1, c2);
  c3 = add_ec(c3, c4);
  c1 = add_ec(c1, c3);
  for (; K; K--) {
    c1 = update_ec(c1, vld1q_f64(sa), vld1q_f64(sb)); sa += 2; sb += 2;
  }
  store_1c(C, c1, expanded_alpha);
}

static inline void kernel_2x1(const double *sa, const double *sb, double *C,
  BLASLONG K, double alphar, double alphai) {

  const float64x2x2_t expanded_alpha = expand_alpha(alphar, alphai);
  float64x2x2_t c1, c2, c3, c4;
  c1 = c2 = c3 = c4 = init();

  for (; K > 1; K -= 2) {
    float64x2_t a1 = vld1q_f64(sa), a2 = vld1q_f64(sa + 2),
      a3 = vld1q_f64(sa + 4), a4 = vld1q_f64(sa + 6); sa += 8;
    float64x2_t b1 = vld1q_f64(sb), b2 = vld1q_f64(sb + 2); sb += 4;
    c1 = update_ec(c1, a1, b1);
    c2 = update_ec(c2, a2, b1);
    c3 = update_ec(c3, a3, b2);
    c4 = update_ec(c4, a4, b2);
  }
  c1 = add_ec(c1, c3);
  c2 = add_ec(c2, c4);
  if (K) {
    float64x2_t b1 = vld1q_f64(sb);
    c1 = update_ec(c1, vld1q_f64(sa), b1);
    c2 = update_ec(c2, vld1q_f64(sa + 2), b1);
  }
  store_1c(C, c1, expanded_alpha);
  store_1c(C + 2, c2, expanded_alpha);
}

static inline void kernel_1x2(const double *sa, const double *sb, double *C,
  BLASLONG LDC, BLASLONG K, double alphar, double alphai) {

  const float64x2x2_t expanded_alpha = expand_alpha(alphar, alphai);
  float64x2x2_t c1, c2, c3, c4;
  c1 = c2 = c3 = c4 = init();

  for (; K > 1; K -= 2) {
    float64x2_t a1 = vld1q_f64(sa), a2 = vld1q_f64(sa + 2); sa += 4;
    float64x2_t b1 = vld1q_f64(sb), b2 = vld1q_f64(sb + 2),
      b3 = vld1q_f64(sb + 4), b4 = vld1q_f64(sb + 6); sb += 8;
    c1 = update_ec(c1, a1, b1);
    c2 = update_ec(c2, a1, b2);
    c3 = update_ec(c3, a2, b3);
    c4 = update_ec(c4, a2, b4);
  }
  c1 = add_ec(c1, c3);
  c2 = add_ec(c2, c4);
  if (K) {
    float64x2_t a1 = vld1q_f64(sa);
    c1 = update_ec(c1, a1, vld1q_f64(sb));
    c2 = update_ec(c2, a1, vld1q_f64(sb + 2));
  }
  store_1c(C, c1, expanded_alpha);
  store_1c(C + LDC * 2, c2, expanded_alpha);
}

static inline void kernel_2x2(const double *sa, const double *sb, double *C,
  BLASLONG LDC, BLASLONG K, double alphar, double alphai) {

  const float64x2x2_t expanded_alpha = expand_alpha(alphar, alphai);
  float64x2x2_t c1, c2, c3, c4;
  c1 = c2 = c3 = c4 = init();

  for (; K; K--) {
    float64x2_t a1 = vld1q_f64(sa), a2 = vld1q_f64(sa + 2); sa += 4;
    float64x2_t b1 = vld1q_f64(sb), b2 = vld1q_f64(sb + 2); sb += 4;
    c1 = update_ec(c1, a1, b1);
    c2 = update_ec(c2, a2, b1);
    c3 = update_ec(c3, a1, b2);
    c4 = update_ec(c4, a2, b2);
  }
  store_1c(C, c1, expanded_alpha);
  store_1c(C + 2, c2, expanded_alpha); C += LDC * 2;
  store_1c(C, c3, expanded_alpha);
  store_1c(C + 2, c4, expanded_alpha);
}

static inline void kernel_4x1(const double *sa, const double *sb, double *C,
  BLASLONG K, double alphar, double alphai) {

  const float64x2x2_t expanded_alpha = expand_alpha(alphar, alphai);
  float64x2x2_t c1, c2, c3, c4;
  c1 = c2 = c3 = c4 = init();
  pref_c_4(C);

  for (; K; K--) {
    float64x2_t b1 = vld1q_f64(sb); sb += 2;
    c1 = update_ec(c1, vld1q_f64(sa), b1);
    c2 = update_ec(c2, vld1q_f64(sa + 2), b1);
    c3 = update_ec(c3, vld1q_f64(sa + 4), b1);
    c4 = update_ec(c4, vld1q_f64(sa + 6), b1);
    sa += 8;
  }
  store_1c(C, c1, expanded_alpha);
  store_1c(C + 2, c2, expanded_alpha);
  store_1c(C + 4, c3, expanded_alpha);
  store_1c(C + 6, c4, expanded_alpha);
}

static inline void kernel_4x2(const double *sa, const double *sb, double *C,
  BLASLONG LDC, BLASLONG K, double alphar, double alphai) {

  const float64x2x2_t expanded_alpha = expand_alpha(alphar, alphai);
  float64x2x2_t c1, c2, c3, c4, c5, c6, c7, c8;
  c1 = c2 = c3 = c4 = c5 = c6 = c7 = c8 = init();
  pref_c_4(C);
  pref_c_4(C + LDC * 2);

  for (; K; K--) {
    float64x2_t b1 = vld1q_f64(sb), b2 = vld1q_f64(sb + 2); sb += 4;
    float64x2_t a1 = vld1q_f64(sa), a2 = vld1q_f64(sa + 2),
      a3 = vld1q_f64(sa + 4), a4 = vld1q_f64(sa + 6); sa += 8;
    c1 = update_ec(c1, a1, b1);
    c2 = update_ec(c2, a2, b1);
    c3 = update_ec(c3, a3, b1);
    c4 = update_ec(c4, a4, b1);
    c5 = update_ec(c5, a1, b2);
    c6 = update_ec(c6, a2, b2);
    c7 = update_ec(c7, a3, b2);
    c8 = update_ec(c8, a4, b2);
  }
  store_1c(C, c1, expanded_alpha);
  store_1c(C + 2, c2, expanded_alpha);
  store_1c(C + 4, c3, expanded_alpha);
  store_1c(C + 6, c4, expanded_alpha); C += LDC * 2;
  store_1c(C, c5, expanded_alpha);
  store_1c(C + 2, c6, expanded_alpha);
  store_1c(C + 4, c7, expanded_alpha);
  store_1c(C + 6, c8, expanded_alpha);
}

static inline void kernel_1x4(const double *sa, const double *sb, double *C,
  BLASLONG LDC, BLASLONG K, double alphar, double alphai) {

  const float64x2x2_t expanded_alpha = expand_alpha(alphar, alphai);
  float64x2x2_t c1, c2, c3, c4;
  c1 = c2 = c3 = c4 = init();

  for (; K; K--) {
    float64x2_t a1 = vld1q_f64(sa); sa += 2;
    c1 = update_ec(c1, a1, vld1q_f64(sb));
    c2 = update_ec(c2, a1, vld1q_f64(sb + 2));
    c3 = update_ec(c3, a1, vld1q_f64(sb + 4));
    c4 = update_ec(c4, a1, vld1q_f64(sb + 6));
    sb += 8;
  }
  store_1c(C, c1, expanded_alpha); C += LDC * 2;
  store_1c(C, c2, expanded_alpha); C += LDC * 2;
  store_1c(C, c3, expanded_alpha); C += LDC * 2;
  store_1c(C, c4, expanded_alpha);
}

static inline void kernel_2x4(const double *sa, const double *sb, double *C,
  BLASLONG LDC, BLASLONG K, double alphar, double alphai) {

  const float64x2x2_t expanded_alpha = expand_alpha(alphar, alphai);
  float64x2x2_t c1, c2, c3, c4, c5, c6, c7, c8;
  c1 = c2 = c3 = c4 = c5 = c6 = c7 = c8 = init();

  for (; K; K--) {
    float64x2_t a1 = vld1q_f64(sa), a2 = vld1q_f64(sa + 2); sa += 4;
    float64x2_t b1 = vld1q_f64(sb), b2 = vld1q_f64(sb + 2),
      b3 = vld1q_f64(sb + 4), b4 = vld1q_f64(sb + 6); sb += 8;
    c1 = update_ec(c1, a1, b1);
    c2 = update_ec(c2, a2, b1);
    c3 = update_ec(c3, a1, b2);
    c4 = update_ec(c4, a2, b2);
    c5 = update_ec(c5, a1, b3);
    c6 = update_ec(c6, a2, b3);
    c7 = update_ec(c7, a1, b4);
    c8 = update_ec(c8, a2, b4);
  }
  store_1c(C, c1, expanded_alpha);
  store_1c(C + 2, c2, expanded_alpha); C += LDC * 2;
  store_1c(C, c3, expanded_alpha);
  store_1c(C + 2, c4, expanded_alpha); C += LDC * 2;
  store_1c(C, c5, expanded_alpha);
  store_1c(C + 2, c6, expanded_alpha); C += LDC * 2;
  store_1c(C, c7, expanded_alpha);
  store_1c(C + 2, c8, expanded_alpha);
}

#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define FMLA_RI "fmla "
#define FMLA_IR "fmla "
#define FMLA_II "fmls "
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define FMLA_RI "fmls "
#define FMLA_IR "fmla "
#define FMLA_II "fmla "
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define FMLA_RI "fmla "
#define FMLA_IR "fmls "
#define FMLA_II "fmla "
#else
#define FMLA_RI "fmls "
#define FMLA_IR "fmls "
#define FMLA_II "fmls "
#endif
#define FMLA_RR "fmla "

static inline void store_4c(double *C, float64x2_t up_r, float64x2_t up_i,
  float64x2_t lo_r, float64x2_t lo_i, double alphar, double alphai) {
  float64x2x2_t up = vld2q_f64(C), lo = vld2q_f64(C + 4);
  up.val[0] = vfmaq_n_f64(up.val[0], up_r, alphar);
  up.val[1] = vfmaq_n_f64(up.val[1], up_r, alphai);
  lo.val[0] = vfmaq_n_f64(lo.val[0], lo_r, alphar);
  lo.val[1] = vfmaq_n_f64(lo.val[1], lo_r, alphai);
  up.val[0] = vfmsq_n_f64(up.val[0], up_i, alphai);
  up.val[1] = vfmaq_n_f64(up.val[1], up_i, alphar);
  lo.val[0] = vfmsq_n_f64(lo.val[0], lo_i, alphai);
  lo.val[1] = vfmaq_n_f64(lo.val[1], lo_i, alphar);
  vst2q_f64(C, up);
  vst2q_f64(C + 4, lo);
}

static inline void kernel_4x4(const double *sa, const double *sb, double *C,
  BLASLONG LDC, BLASLONG K, double alphar, double alphai) {

  float64x2_t c1r, c1i, c2r, c2i;
  float64x2_t c3r, c3i, c4r, c4i;
  float64x2_t c5r, c5i, c6r, c6i;
  float64x2_t c7r, c7i, c8r, c8i;

  const double *pref_ = C;
  pref_c_4(pref_); pref_ += LDC * 2;
  pref_c_4(pref_); pref_ += LDC * 2;
  pref_c_4(pref_); pref_ += LDC * 2;
  pref_c_4(pref_);

  __asm__ __volatile__(
    "cmp %[K],#0\n\t"
    "movi %[c1r].16b,#0; movi %[c1i].16b,#0; movi %[c2r].16b,#0; movi %[c2i].16b,#0\n\t"
    "movi %[c3r].16b,#0; movi %[c3i].16b,#0; movi %[c4r].16b,#0; movi %[c4i].16b,#0\n\t"
    "movi %[c5r].16b,#0; movi %[c5i].16b,#0; movi %[c6r].16b,#0; movi %[c6i].16b,#0\n\t"
    "movi %[c7r].16b,#0; movi %[c7i].16b,#0; movi %[c8r].16b,#0; movi %[c8i].16b,#0\n\t"
    "beq 4f; cmp %[K],#2\n\t"
    "ld2 {v0.2d,v1.2d},[%[sa]],#32; ldp q4,q5,[%[sb]],#32\n\t"
    "ld2 {v2.2d,v3.2d},[%[sa]],#32; ldr q6,[%[sb]]; ldr d7,[%[sb],#16]\n\t"
    "ldr x0,[%[sb],#24]; add %[sb],%[sb],#32\n\t"
    "beq 2f; blt 3f\n\t"
    "1:\n\t"
    "fmov v7.d[1],x0; ldr d8,[%[sa]]\n\t"
    FMLA_RR "%[c1r].2d,v0.2d,v4.d[0]; ldr x0,[%[sa],#16]\n\t"
    FMLA_RR "%[c2r].2d,v2.2d,v4.d[0]\n\t"
    FMLA_RI "%[c1i].2d,v0.2d,v4.d[1]\n\t"
    "fmov v8.d[1],x0; ldr d9,[%[sa],#8]\n\t"
    FMLA_RI "%[c2i].2d,v2.2d,v4.d[1]; ldr x0,[%[sa],#24]\n\t"
    FMLA_II "%[c1r].2d,v1.2d,v4.d[1]\n\t"
    FMLA_II "%[c2r].2d,v3.2d,v4.d[1]\n\t"
    "fmov v9.d[1],x0; ldr d10,[%[sa],#32]\n\t"
    FMLA_IR "%[c1i].2d,v1.2d,v4.d[0]; ldr x0,[%[sa],#48]\n\t"
    FMLA_IR "%[c2i].2d,v3.2d,v4.d[0]\n\t"
    FMLA_RR "%[c3r].2d,v0.2d,v5.d[0]\n\t"
    "fmov v10.d[1],x0; ldr d11,[%[sa],#40]\n\t"
    FMLA_RR "%[c4r].2d,v2.2d,v5.d[0]; ldr x0,[%[sa],#56]\n\t"
    FMLA_RI "%[c3i].2d,v0.2d,v5.d[1]\n\t"
    FMLA_RI "%[c4i].2d,v2.2d,v5.d[1]\n\t"
    "fmov v11.d[1],x0; ldr d12,[%[sb]]\n\t"
    FMLA_II "%[c3r].2d,v1.2d,v5.d[1]; ldr x0,[%[sb],#8]\n\t"
    FMLA_II "%[c4r].2d,v3.2d,v5.d[1]\n\t"
    FMLA_IR "%[c3i].2d,v1.2d,v5.d[0]\n\t"
    "fmov v12.d[1],x0; ldr d13,[%[sb],#16]\n\t"
    FMLA_IR "%[c4i].2d,v3.2d,v5.d[0]; ldr x0,[%[sb],#24]\n\t"
    FMLA_RR "%[c5r].2d,v0.2d,v6.d[0]\n\t"
    FMLA_RR "%[c6r].2d,v2.2d,v6.d[0]\n\t"
    "fmov v13.d[1],x0; ldr d14,[%[sb],#32]\n\t"
    FMLA_RI "%[c5i].2d,v0.2d,v6.d[1]; ldr x0,[%[sb],#40]\n\t"
    FMLA_RI "%[c6i].2d,v2.2d,v6.d[1]\n\t"
    FMLA_II "%[c5r].2d,v1.2d,v6.d[1]\n\t"
    "fmov v14.d[1],x0; ldr d15,[%[sb],#48]\n\t"
    FMLA_II "%[c6r].2d,v3.2d,v6.d[1]; ldr x0,[%[sb],#56]\n\t"
    FMLA_IR "%[c5i].2d,v1.2d,v6.d[0]\n\t"
    FMLA_IR "%[c6i].2d,v3.2d,v6.d[0]\n\t"
    "fmov v15.d[1],x0; ldr d4,[%[sb],#64]\n\t"
    FMLA_RR "%[c7r].2d,v0.2d,v7.d[0]; ldr x0,[%[sb],#72]\n\t"
    FMLA_RR "%[c8r].2d,v2.2d,v7.d[0]\n\t"
    FMLA_RI "%[c7i].2d,v0.2d,v7.d[1]\n\t"
    "fmov v4.d[1],x0; ldr d5,[%[sb],#80]\n\t"
    FMLA_RI "%[c8i].2d,v2.2d,v7.d[1]; ldr x0,[%[sb],#88]\n\t"
    FMLA_II "%[c7r].2d,v1.2d,v7.d[1]\n\t"
    FMLA_II "%[c8r].2d,v3.2d,v7.d[1]\n\t"
    "fmov v5.d[1],x0; ldr d0,[%[sa],#64]\n\t"
    FMLA_IR "%[c7i].2d,v1.2d,v7.d[0]; ldr x0,[%[sa],#80]\n\t"
    FMLA_IR "%[c8i].2d,v3.2d,v7.d[0]\n\t"
    FMLA_RR "%[c1r].2d,v8.2d,v12.d[0]\n\t"
    "fmov v0.d[1],x0; ldr d1,[%[sa],#72]\n\t"
    FMLA_RR "%[c2r].2d,v10.2d,v12.d[0]; ldr x0,[%[sa],#88]\n\t"
    FMLA_RI "%[c1i].2d,v8.2d,v12.d[1]\n\t"
    FMLA_RI "%[c2i].2d,v10.2d,v12.d[1]\n\t"
    "fmov v1.d[1],x0; ldr d2,[%[sa],#96]\n\t"
    FMLA_II "%[c1r].2d,v9.2d,v12.d[1]; ldr x0,[%[sa],#112]\n\t"
    FMLA_II "%[c2r].2d,v11.2d,v12.d[1]\n\t"
    FMLA_IR "%[c1i].2d,v9.2d,v12.d[0]\n\t"
    "fmov v2.d[1],x0; ldr d3,[%[sa],#104]\n\t"
    FMLA_IR "%[c2i].2d,v11.2d,v12.d[0]; ldr x0,[%[sa],#120]\n\t"
    FMLA_RR "%[c3r].2d,v8.2d,v13.d[0]\n\t"
    FMLA_RR "%[c4r].2d,v10.2d,v13.d[0]\n\t"
    "fmov v3.d[1],x0; ldr d6,[%[sb],#96]\n\t"
    FMLA_RI "%[c3i].2d,v8.2d,v13.d[1]; ldr x0,[%[sb],#104]\n\t"
    FMLA_RI "%[c4i].2d,v10.2d,v13.d[1]\n\t"
    FMLA_II "%[c3r].2d,v9.2d,v13.d[1]\n\t"
    "fmov v6.d[1],x0; ldr d7,[%[sb],#112]\n\t"
    FMLA_II "%[c4r].2d,v11.2d,v13.d[1]; ldr x0,[%[sb],#120]\n\t"
    FMLA_IR "%[c3i].2d,v9.2d,v13.d[0]\n\t"
    FMLA_IR "%[c4i].2d,v11.2d,v13.d[0]; prfm pldl1keep,[%[sa],#256]\n\t"
    FMLA_RR "%[c5r].2d,v8.2d,v14.d[0]\n\t"
    FMLA_RR "%[c6r].2d,v10.2d,v14.d[0]; prfm pldl1keep,[%[sa],#320]\n\t"
    FMLA_RI "%[c5i].2d,v8.2d,v14.d[1]\n\t"
    FMLA_RI "%[c6i].2d,v10.2d,v14.d[1]; prfm pldl1keep,[%[sb],#256]\n\t"
    FMLA_II "%[c5r].2d,v9.2d,v14.d[1]\n\t"
    FMLA_II "%[c6r].2d,v11.2d,v14.d[1]; prfm pldl1keep,[%[sb],#320]\n\t"
    FMLA_IR "%[c5i].2d,v9.2d,v14.d[0]\n\t"
    FMLA_IR "%[c6i].2d,v11.2d,v14.d[0]; add %[sa],%[sa],#128\n\t"
    FMLA_RR "%[c7r].2d,v8.2d,v15.d[0]\n\t"
    FMLA_RR "%[c8r].2d,v10.2d,v15.d[0]; add %[sb],%[sb],#128\n\t"
    FMLA_RI "%[c7i].2d,v8.2d,v15.d[1]\n\t"
    FMLA_RI "%[c8i].2d,v10.2d,v15.d[1]; sub %[K],%[K],#2\n\t"
    FMLA_II "%[c7r].2d,v9.2d,v15.d[1]\n\t"
    FMLA_II "%[c8r].2d,v11.2d,v15.d[1]; cmp %[K],#2\n\t"
    FMLA_IR "%[c7i].2d,v9.2d,v15.d[0]\n\t"
    FMLA_IR "%[c8i].2d,v11.2d,v15.d[0]; bgt 1b; blt 3f\n\t"
    "2:\n\t"
    "fmov v7.d[1],x0; ldr d8,[%[sa]]\n\t"
    FMLA_RR "%[c1r].2d,v0.2d,v4.d[0]; ldr x0,[%[sa],#16]\n\t"
    FMLA_RR "%[c2r].2d,v2.2d,v4.d[0]\n\t"
    FMLA_RI "%[c1i].2d,v0.2d,v4.d[1]\n\t"
    "fmov v8.d[1],x0; ldr d9,[%[sa],#8]\n\t"
    FMLA_RI "%[c2i].2d,v2.2d,v4.d[1]; ldr x0,[%[sa],#24]\n\t"
    FMLA_II "%[c1r].2d,v1.2d,v4.d[1]\n\t"
    FMLA_II "%[c2r].2d,v3.2d,v4.d[1]\n\t"
    "fmov v9.d[1],x0; ldr d10,[%[sa],#32]\n\t"
    FMLA_IR "%[c1i].2d,v1.2d,v4.d[0]; ldr x0,[%[sa],#48]\n\t"
    FMLA_IR "%[c2i].2d,v3.2d,v4.d[0]\n\t"
    FMLA_RR "%[c3r].2d,v0.2d,v5.d[0]\n\t"
    "fmov v10.d[1],x0; ldr d11,[%[sa],#40]\n\t"
    FMLA_RR "%[c4r].2d,v2.2d,v5.d[0]; ldr x0,[%[sa],#56]\n\t"
    FMLA_RI "%[c3i].2d,v0.2d,v5.d[1]\n\t"
    FMLA_RI "%[c4i].2d,v2.2d,v5.d[1]\n\t"
    "fmov v11.d[1],x0; ldr d12,[%[sb]]\n\t"
    FMLA_II "%[c3r].2d,v1.2d,v5.d[1]; ldr x0,[%[sb],#8]\n\t"
    FMLA_II "%[c4r].2d,v3.2d,v5.d[1]\n\t"
    FMLA_IR "%[c3i].2d,v1.2d,v5.d[0]\n\t"
    "fmov v12.d[1],x0; ldr d13,[%[sb],#16]\n\t"
    FMLA_IR "%[c4i].2d,v3.2d,v5.d[0]; ldr x0,[%[sb],#24]\n\t"
    FMLA_RR "%[c5r].2d,v0.2d,v6.d[0]\n\t"
    FMLA_RR "%[c6r].2d,v2.2d,v6.d[0]\n\t"
    "fmov v13.d[1],x0; ldr d14,[%[sb],#32]\n\t"
    FMLA_RI "%[c5i].2d,v0.2d,v6.d[1]; ldr x0,[%[sb],#40]\n\t"
    FMLA_RI "%[c6i].2d,v2.2d,v6.d[1]\n\t"
    FMLA_II "%[c5r].2d,v1.2d,v6.d[1]\n\t"
    "fmov v14.d[1],x0; ldr d15,[%[sb],#48]\n\t"
    FMLA_II "%[c6r].2d,v3.2d,v6.d[1]; ldr x0,[%[sb],#56]\n\t"
    FMLA_IR "%[c5i].2d,v1.2d,v6.d[0]\n\t"
    FMLA_IR "%[c6i].2d,v3.2d,v6.d[0]\n\t"
    "fmov v15.d[1],x0\n\t"
    FMLA_RR "%[c7r].2d,v0.2d,v7.d[0]\n\t"
    FMLA_RR "%[c8r].2d,v2.2d,v7.d[0]\n\t"
    FMLA_RI "%[c7i].2d,v0.2d,v7.d[1]\n\t"
    FMLA_RI "%[c8i].2d,v2.2d,v7.d[1]\n\t"
    FMLA_II "%[c7r].2d,v1.2d,v7.d[1]\n\t"
    FMLA_II "%[c8r].2d,v3.2d,v7.d[1]\n\t"
    FMLA_IR "%[c7i].2d,v1.2d,v7.d[0]\n\t"
    FMLA_IR "%[c8i].2d,v3.2d,v7.d[0]\n\t"
    FMLA_RR "%[c1r].2d,v8.2d,v12.d[0]\n\t"
    FMLA_RR "%[c2r].2d,v10.2d,v12.d[0]\n\t"
    FMLA_RI "%[c1i].2d,v8.2d,v12.d[1]\n\t"
    FMLA_RI "%[c2i].2d,v10.2d,v12.d[1]\n\t"
    FMLA_II "%[c1r].2d,v9.2d,v12.d[1]\n\t"
    FMLA_II "%[c2r].2d,v11.2d,v12.d[1]\n\t"
    FMLA_IR "%[c1i].2d,v9.2d,v12.d[0]\n\t"
    FMLA_IR "%[c2i].2d,v11.2d,v12.d[0]\n\t"
    FMLA_RR "%[c3r].2d,v8.2d,v13.d[0]\n\t"
    FMLA_RR "%[c4r].2d,v10.2d,v13.d[0]\n\t"
    FMLA_RI "%[c3i].2d,v8.2d,v13.d[1]\n\t"
    FMLA_RI "%[c4i].2d,v10.2d,v13.d[1]\n\t"
    FMLA_II "%[c3r].2d,v9.2d,v13.d[1]\n\t"
    FMLA_II "%[c4r].2d,v11.2d,v13.d[1]\n\t"
    FMLA_IR "%[c3i].2d,v9.2d,v13.d[0]\n\t"
    FMLA_IR "%[c4i].2d,v11.2d,v13.d[0]\n\t"
    FMLA_RR "%[c5r].2d,v8.2d,v14.d[0]\n\t"
    FMLA_RR "%[c6r].2d,v10.2d,v14.d[0]\n\t"
    FMLA_RI "%[c5i].2d,v8.2d,v14.d[1]\n\t"
    FMLA_RI "%[c6i].2d,v10.2d,v14.d[1]\n\t"
    FMLA_II "%[c5r].2d,v9.2d,v14.d[1]\n\t"
    FMLA_II "%[c6r].2d,v11.2d,v14.d[1]\n\t"
    FMLA_IR "%[c5i].2d,v9.2d,v14.d[0]\n\t"
    FMLA_IR "%[c6i].2d,v11.2d,v14.d[0]; add %[sa],%[sa],#64\n\t"
    FMLA_RR "%[c7r].2d,v8.2d,v15.d[0]\n\t"
    FMLA_RR "%[c8r].2d,v10.2d,v15.d[0]; add %[sb],%[sb],#64\n\t"
    FMLA_RI "%[c7i].2d,v8.2d,v15.d[1]\n\t"
    FMLA_RI "%[c8i].2d,v10.2d,v15.d[1]; sub %[K],%[K],#2\n\t"
    FMLA_II "%[c7r].2d,v9.2d,v15.d[1]\n\t"
    FMLA_II "%[c8r].2d,v11.2d,v15.d[1]\n\t"
    FMLA_IR "%[c7i].2d,v9.2d,v15.d[0]\n\t"
    FMLA_IR "%[c8i].2d,v11.2d,v15.d[0]; b 4f\n\t"
    "3:\n\t"
    "fmov v7.d[1],x0\n\t"
    FMLA_RR "%[c1r].2d,v0.2d,v4.d[0]\n\t"
    FMLA_RR "%[c2r].2d,v2.2d,v4.d[0]\n\t"
    FMLA_RI "%[c1i].2d,v0.2d,v4.d[1]\n\t"
    FMLA_RI "%[c2i].2d,v2.2d,v4.d[1]\n\t"
    FMLA_II "%[c1r].2d,v1.2d,v4.d[1]\n\t"
    FMLA_II "%[c2r].2d,v3.2d,v4.d[1]\n\t"
    FMLA_IR "%[c1i].2d,v1.2d,v4.d[0]\n\t"
    FMLA_IR "%[c2i].2d,v3.2d,v4.d[0]\n\t"
    FMLA_RR "%[c3r].2d,v0.2d,v5.d[0]\n\t"
    FMLA_RR "%[c4r].2d,v2.2d,v5.d[0]\n\t"
    FMLA_RI "%[c3i].2d,v0.2d,v5.d[1]\n\t"
    FMLA_RI "%[c4i].2d,v2.2d,v5.d[1]\n\t"
    FMLA_II "%[c3r].2d,v1.2d,v5.d[1]\n\t"
    FMLA_II "%[c4r].2d,v3.2d,v5.d[1]\n\t"
    FMLA_IR "%[c3i].2d,v1.2d,v5.d[0]\n\t"
    FMLA_IR "%[c4i].2d,v3.2d,v5.d[0]\n\t"
    FMLA_RR "%[c5r].2d,v0.2d,v6.d[0]\n\t"
    FMLA_RR "%[c6r].2d,v2.2d,v6.d[0]\n\t"
    FMLA_RI "%[c5i].2d,v0.2d,v6.d[1]\n\t"
    FMLA_RI "%[c6i].2d,v2.2d,v6.d[1]\n\t"
    FMLA_II "%[c5r].2d,v1.2d,v6.d[1]\n\t"
    FMLA_II "%[c6r].2d,v3.2d,v6.d[1]\n\t"
    FMLA_IR "%[c5i].2d,v1.2d,v6.d[0]\n\t"
    FMLA_IR "%[c6i].2d,v3.2d,v6.d[0]\n\t"
    FMLA_RR "%[c7r].2d,v0.2d,v7.d[0]\n\t"
    FMLA_RR "%[c8r].2d,v2.2d,v7.d[0]\n\t"
    FMLA_RI "%[c7i].2d,v0.2d,v7.d[1]\n\t"
    FMLA_RI "%[c8i].2d,v2.2d,v7.d[1]\n\t"
    FMLA_II "%[c7r].2d,v1.2d,v7.d[1]\n\t"
    FMLA_II "%[c8r].2d,v3.2d,v7.d[1]\n\t"
    FMLA_IR "%[c7i].2d,v1.2d,v7.d[0]\n\t"
    FMLA_IR "%[c8i].2d,v3.2d,v7.d[0]; sub %[K],%[K],#1\n\t"
    "4:\n\t"
   :[c1r]"=w"(c1r), [c1i]"=w"(c1i), [c2r]"=w"(c2r), [c2i]"=w"(c2i),
    [c3r]"=w"(c3r), [c3i]"=w"(c3i), [c4r]"=w"(c4r), [c4i]"=w"(c4i),
    [c5r]"=w"(c5r), [c5i]"=w"(c5i), [c6r]"=w"(c6r), [c6i]"=w"(c6i),
    [c7r]"=w"(c7r), [c7i]"=w"(c7i), [c8r]"=w"(c8r), [c8i]"=w"(c8i),
    [K]"+r"(K), [sa]"+r"(sa), [sb]"+r"(sb)
   ::"cc", "memory", "x0", "v0", "v1", "v2", "v3", "v4", "v5", "v6", "v7",
     "v8", "v9", "v10", "v11", "v12", "v13", "v14", "v15");

  store_4c(C, c1r, c1i, c2r, c2i, alphar, alphai); C += LDC * 2;
  store_4c(C, c3r, c3i, c4r, c4i, alphar, alphai); C += LDC * 2;
  store_4c(C, c5r, c5i, c6r, c6i, alphar, alphai); C += LDC * 2;
  store_4c(C, c7r, c7i, c8r, c8i, alphar, alphai);
}

int CNAME(BLASLONG M, BLASLONG N, BLASLONG K, FLOAT alphar, FLOAT alphai,
  FLOAT *sa, FLOAT *sb, FLOAT *C, BLASLONG LDC) {

  BLASLONG n_left = N;
  for (; n_left >= 4; n_left -= 4) {
    const FLOAT *a_ = sa;
    FLOAT *c_ = C;
    BLASLONG m_left = M;
    for (; m_left >= 4; m_left -= 4) {
      kernel_4x4(a_, sb, c_, LDC, K, alphar, alphai);
      a_ += 8 * K;
      c_ += 8;
    }
    if (m_left >= 2) {
      m_left -= 2;
      kernel_2x4(a_, sb, c_, LDC, K, alphar, alphai);
      a_ += 4 * K;
      c_ += 4;
    }
    if (m_left) {
      kernel_1x4(a_, sb, c_, LDC, K, alphar, alphai);
    }
    sb += 8 * K;
    C += 8 * LDC;
  }
  if (n_left >= 2) {
    n_left -= 2;
    const FLOAT *a_ = sa;
    FLOAT *c_ = C;
    BLASLONG m_left = M;
    for (; m_left >= 4; m_left -= 4) {
      kernel_4x2(a_, sb, c_, LDC, K, alphar, alphai);
      a_ += 8 * K;
      c_ += 8;
    }
    if (m_left >= 2) {
      m_left -= 2;
      kernel_2x2(a_, sb, c_, LDC, K, alphar, alphai);
      a_ += 4 * K;
      c_ += 4;
    }
    if (m_left) {
      kernel_1x2(a_, sb, c_, LDC, K, alphar, alphai);
    }
    sb += 4 * K;
    C += 4 * LDC;
  }
  if (n_left) {
    const FLOAT *a_ = sa;
    FLOAT *c_ = C;
    BLASLONG m_left = M;
    for (; m_left >= 4; m_left -= 4) {
      kernel_4x1(a_, sb, c_, K, alphar, alphai);
      a_ += 8 * K;
      c_ += 8;
    }
    if (m_left >= 2) {
      m_left -= 2;
      kernel_2x1(a_, sb, c_, K, alphar, alphai);
      a_ += 4 * K;
      c_ += 4;
    }
    if (m_left) {
      kernel_1x1(a_, sb, c_, K, alphar, alphai);
    }
  }
  return 0;
}

