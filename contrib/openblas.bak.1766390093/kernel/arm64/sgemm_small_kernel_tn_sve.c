/***************************************************************************
Copyright (c) 2024, The OpenBLAS Project
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

#include <arm_neon.h>
#include <arm_sve.h>
#if defined(__ARM_NEON_SVE_BRIDGE) && defined(__has_include) &&                \
  __has_include(<arm_neon_sve_bridge.h>)
#include <arm_neon_sve_bridge.h>
#else
#define svdup_neonq_f32(fixed_reg)                                             \
  ({                                                                           \
    svfloat32_t scalable_reg;                                                  \
    asm("mov %0.q, %q1" : "=w"(scalable_reg) : "w"(fixed_reg) :);              \
    scalable_reg;                                                              \
  })
#define svdup_neonq_f64(fixed_reg)                                             \
  ({                                                                           \
    svfloat64_t scalable_reg;                                                  \
    asm("mov %0.q, %q1" : "=w"(scalable_reg) : "w"(fixed_reg) :);              \
    scalable_reg;                                                              \
  })
#endif

#define RESET_A_POINTER() a_offset = A;

#define CREATE_A_POINTER(m, scale) FLOAT* a_offset##m = a_offset + scale * lda;
#define UPDATE_A_POINTER(scale) a_offset = a_offset + scale * lda;
#define A_ELEMENT_K(m, offset_k) *(a_offset##m + (k + offset_k))
#define A_ELEMENT(m) A_ELEMENT_K(m, 0)

#define RESET_B_POINTER() b_offset = B;

#define CREATE_B_POINTER(n, scale) FLOAT* b_offset##n = b_offset + scale * ldb;
#define UPDATE_B_POINTER(scale) b_offset = b_offset + scale * ldb;
#define B_ELEMENT_K(n, offset_k) *(b_offset##n + (k + offset_k))
#define B_ELEMENT(n) B_ELEMENT_K(n, 0)

#define CREATE_C_POINTER(m, scale) FLOAT* c_offset##m = c_offset + scale;
#define INCR_C_POINTER(m, incr) // c_offset ## m += incr * ldc;
#define UPDATE_C_POINTER(scale) c_offset += scale;
#define C_ELEMENT(m, n)                                                        \
  *(c_offset##m + ((j + n) * ldc)) // C[(i+(m))+(j+(n))*ldc]

// #undef C_ELEMENT
// #define C_ELEMENT(m, n)             C[(i+(m))+(j+(n))*ldc]

#define PACK_ELEMENT_K(m, offset_k) packed_a[(k + offset_k) * v_size2 + m]
#define PACK_ELEMENT(m) PACK_ELEMENT_K(m, 0)

// ASIMD
#define DECLARE_RESULT_VECTOR4(m, n)                                           \
  float32x4_t result##m##n = vdupq_n_f32(0.0);
#define DECLARE_RESULT(m, n) float32_t result##m##n = 0.0;
#define BROADCAST_LOAD_A4(m, offset_k)                                         \
  float32x4_t a##m##_k##offset_k = vld1q_dup_f32(&A_ELEMENT_K(m, offset_k));
#define LOAD_A1(m, offset_k)                                                   \
  float32_t a##m##_k##offset_k = A_ELEMENT_K(m, offset_k);
#define GATHER_LOAD_B4(n, offset_k)                                            \
  float32x4_t b##n##_k##offset_k = vdupq_n_f32(B_ELEMENT_K(n, offset_k));      \
  b##n##_k##offset_k =                                                         \
    vsetq_lane_f32(B_ELEMENT_K(n + 1, offset_k), b##n##_k##offset_k, 1);       \
  b##n##_k##offset_k =                                                         \
    vsetq_lane_f32(B_ELEMENT_K(n + 2, offset_k), b##n##_k##offset_k, 2);       \
  b##n##_k##offset_k =                                                         \
    vsetq_lane_f32(B_ELEMENT_K(n + 3, offset_k), b##n##_k##offset_k, 3);
#define VECTOR_UNPACK_B4(n, offset_k)                                          \
  float32x4_t b##n##_k##offset_k = vld1q_f32(&PACK_ELEMENT_K(n, offset_k));
#define PACK_B0(n, offset_k)                                                   \
  PACK_ELEMENT_K(n, offset_k) = vget_lane_f32(b##n##_k##offset_k, 0);
#define UPDATE_RESULT_VECTOR4(m, n, offset_k)                                  \
  result##m##n =                                                               \
    vfmaq_f32(result##m##n, a##m##_k##offset_k, b##n##_k##offset_k);
#define UPDATE_RESULT(m, n, offset_k)                                          \
  result##m##n = result##m##n + a##m##_k##offset_k * b##n##_k##offset_k;
#ifdef B0
#define SCATTER_STORE4(m, n)                                                   \
  result##m##n = vmulq_f32(result##m##n, vdupq_n_f32(alpha));                  \
  C_ELEMENT(m, n + 0) = vgetq_lane_f32(result##m##n, 0);                       \
  C_ELEMENT(m, n + 1) = vgetq_lane_f32(result##m##n, 1);                       \
  C_ELEMENT(m, n + 2) = vgetq_lane_f32(result##m##n, 2);                       \
  C_ELEMENT(m, n + 3) = vgetq_lane_f32(result##m##n, 3);
#else
#define SCATTER_STORE4(m, n)                                                   \
  result##m##n = vmulq_f32(result##m##n, vdupq_n_f32(alpha));                  \
  C_ELEMENT(m, n + 0) =                                                        \
    C_ELEMENT(m, n + 0) * beta + vgetq_lane_f32(result##m##n, 0);              \
  C_ELEMENT(m, n + 1) =                                                        \
    C_ELEMENT(m, n + 1) * beta + vgetq_lane_f32(result##m##n, 1);              \
  C_ELEMENT(m, n + 2) =                                                        \
    C_ELEMENT(m, n + 2) * beta + vgetq_lane_f32(result##m##n, 2);              \
  C_ELEMENT(m, n + 3) =                                                        \
    C_ELEMENT(m, n + 3) * beta + vgetq_lane_f32(result##m##n, 3);
#endif

// SVE
#define DECLARE_RESULT_VECTOR(m, n) svfloat32_t result##m##n = svdup_f32(0.0);
#define BROADCAST_LOAD_A(m, offset_k)                                          \
  svfloat32_t a##s##m##_k##offset_k = svdup_f32(A_ELEMENT_K(m, offset_k));
#define BROADCAST_LOAD_B(n, offset_k)                                          \
  svfloat32_t b##s##n##_k##offset_k = svdup_f32(B_ELEMENT_K(n, offset_k));
#define VECTOR_LOAD_A(pg, m, offset_k)                                         \
  svfloat32_t a##s##m##_k##offset_k = svld1(pg, &A_ELEMENT_K(m, offset_k));
#define GATHER_LOAD_A(pg, m, offset_k)                                         \
  svfloat32_t a##s##m##_k##offset_k =                                          \
    svld1_gather_index(pg, &A_ELEMENT_K(m, offset_k), lda_vec);
#define PACK_A(m, offset_k)                                                    \
  svst1(pg_first, &PACK_ELEMENT_K(m, offset_k), a##s##m##_k##offset_k);
#define VECTOR_PACK_A(m, offset_k)                                             \
  svst1(pg_true, &PACK_ELEMENT_K(m* v_size, offset_k), a##s##m##_k##offset_k);
#define QUADWORD_PACK_A(m, offset_k)                                           \
  svst1(pg_quad, &PACK_ELEMENT_K(m, offset_k), a##s##m##_k##offset_k);
#define UNPACK_VECTOR_A(m, offset_k)                                           \
  svfloat32_t a##s##m##_k##offset_k =                                          \
    svld1(pg_true, &PACK_ELEMENT_K(m * v_size, offset_k));
#define UNPACK_BROADCAST_A(m, offset_k)                                        \
  svfloat32_t a##s##m##_k##offset_k = svdup_f32(PACK_ELEMENT_K(m, offset_k));
#define UNPACK_QUADWORD_A(m, offset_k)                                         \
  svfloat32_t a##s##m##_k##offset_k =                                          \
    svld1rq(pg_true, &PACK_ELEMENT_K(m, offset_k));
#define UPDATE_RESULT_VECTOR(pg, m, n, offset_k)                               \
  result##m##n =                                                               \
    svmla_m(pg, result##m##n, a##s##m##_k##offset_k, b##s##n##_k##offset_k);
#define UPDATE_RESULT_VECTOR_QUADWORD(m, n, outer, lane, offset_k)             \
  result##m##n = svmla_lane(                                                   \
    result##m##n, a##s##m##_k##offset_k, b##s##outer##_k##offset_k, lane);
#ifdef B0
#define VECTOR_STORE(pg, m, n)                                                 \
  result##m##n = svmul_m(pg, result##m##n, alpha_vec);                         \
  svst1(pg, &C_ELEMENT(m, n), result##m##n);
#define SCATTER_STORE(pg, m, n)                                                \
  result##m##n = svmul_m(pg, result##m##n, alpha_vec);                         \
  svst1_scatter_index(pg, &C_ELEMENT(m, n), ldc_vec, result##m##n);
#else
#define VECTOR_STORE(pg, m, n)                                                 \
  result##m##n = svmul_m(pg, result##m##n, alpha_vec);                         \
  result##m##n =                                                               \
    svmla_m(pg, result##m##n, svld1(pg, &C_ELEMENT(m, n)), beta_vec);          \
  svst1(pg, &C_ELEMENT(m, n), result##m##n);
#define SCATTER_STORE(pg, m, n)                                                \
  result##m##n = svmul_m(pg, result##m##n, alpha_vec);                         \
  result##m##n = svmla_m(pg,                                                   \
                         result##m##n,                                         \
                         svld1_gather_index(pg, &C_ELEMENT(m, n), ldc_vec),    \
                         beta_vec);                                            \
  svst1_scatter_index(pg, &C_ELEMENT(m, n), ldc_vec, result##m##n);
#endif

#ifndef LIKELY
#ifdef __GNUC__
#define LIKELY(x) __builtin_expect(!!(x), 1)
#else
#define LIKELY(x) (x)
#endif
#endif

#ifdef B0
int
CNAME(BLASLONG M,
      BLASLONG N,
      BLASLONG K,
      IFLOAT* A,
      BLASLONG lda,
      FLOAT alpha,
      IFLOAT* B,
      BLASLONG ldb,
      FLOAT* C,
      BLASLONG ldc)
#else
int
CNAME(BLASLONG M,
      BLASLONG N,
      BLASLONG K,
      IFLOAT* A,
      BLASLONG lda,
      FLOAT alpha,
      IFLOAT* B,
      BLASLONG ldb,
      FLOAT beta,
      FLOAT* C,
      BLASLONG ldc)
#endif
{
  const uint64_t v_size = svcntw();
  const uint64_t v_size2 = v_size * 2;
  const svbool_t pg_true = svptrue_b32();
  const svbool_t pg_quad = svwhilelt_b32(0, 4);
  const svbool_t pg_first = svwhilelt_b32(0, 1);
  const svfloat32_t alpha_vec = svdup_f32(alpha);
#ifndef B0
  const svfloat32_t beta_vec = svdup_f32(beta);
#endif
  const svuint32_t lda_vec = svindex_u32(0LL, lda);

  const BLASLONG v_m2 = M & -v_size2;
  const BLASLONG v_m1 = M & -v_size;
  const BLASLONG n8 = N & -8;
  const BLASLONG n4 = N & -4;

  const int pack_a = M >= v_size2 && N >= 8 ? 1 : 0;
  FLOAT* packed_a =
    (pack_a) ? packed_a = (FLOAT*)malloc(K * v_size2 * sizeof(FLOAT)) : NULL;

  FLOAT* a_offset = A;
  FLOAT* b_offset = B;
  FLOAT* c_offset = C;

  BLASLONG i = 0;
  for (; i < v_m2; i += v_size2) {

    CREATE_C_POINTER(0, 0);
    CREATE_C_POINTER(1, v_size);
    CREATE_A_POINTER(0, 0);
    CREATE_A_POINTER(1, v_size);

    BLASLONG j = 0;
    for (; j < n8; j += 8) {

      CREATE_B_POINTER(0, 0);
      CREATE_B_POINTER(1, 1);
      CREATE_B_POINTER(2, 2);
      CREATE_B_POINTER(3, 3);
      CREATE_B_POINTER(4, 4);
      CREATE_B_POINTER(5, 5);
      CREATE_B_POINTER(6, 6);
      CREATE_B_POINTER(7, 7);
      UPDATE_B_POINTER(8);

      BLASLONG k = 0;
      DECLARE_RESULT_VECTOR(0, 0);
      DECLARE_RESULT_VECTOR(0, 1);
      DECLARE_RESULT_VECTOR(0, 2);
      DECLARE_RESULT_VECTOR(0, 3);
      DECLARE_RESULT_VECTOR(0, 4);
      DECLARE_RESULT_VECTOR(0, 5);
      DECLARE_RESULT_VECTOR(0, 6);
      DECLARE_RESULT_VECTOR(0, 7);
      DECLARE_RESULT_VECTOR(1, 0);
      DECLARE_RESULT_VECTOR(1, 1);
      DECLARE_RESULT_VECTOR(1, 2);
      DECLARE_RESULT_VECTOR(1, 3);
      DECLARE_RESULT_VECTOR(1, 4);
      DECLARE_RESULT_VECTOR(1, 5);
      DECLARE_RESULT_VECTOR(1, 6);
      DECLARE_RESULT_VECTOR(1, 7);

      if (LIKELY(packed_a != NULL)) {
        if (j == 0) {
          for (; k < K; k++) {

            BROADCAST_LOAD_B(0, 0);
            GATHER_LOAD_A(pg_true, 0, 0);
            VECTOR_PACK_A(0, 0);
            UPDATE_RESULT_VECTOR(pg_true, 0, 0, 0);
            BROADCAST_LOAD_B(1, 0);
            UPDATE_RESULT_VECTOR(pg_true, 0, 1, 0);
            GATHER_LOAD_A(pg_true, 1, 0);
            VECTOR_PACK_A(1, 0);
            UPDATE_RESULT_VECTOR(pg_true, 1, 0, 0);
            UPDATE_RESULT_VECTOR(pg_true, 1, 1, 0);
            BROADCAST_LOAD_B(2, 0);
            UPDATE_RESULT_VECTOR(pg_true, 0, 2, 0);
            UPDATE_RESULT_VECTOR(pg_true, 1, 2, 0);
            BROADCAST_LOAD_B(3, 0);
            UPDATE_RESULT_VECTOR(pg_true, 0, 3, 0);
            UPDATE_RESULT_VECTOR(pg_true, 1, 3, 0);
            BROADCAST_LOAD_B(4, 0);
            UPDATE_RESULT_VECTOR(pg_true, 0, 4, 0);
            UPDATE_RESULT_VECTOR(pg_true, 1, 4, 0);
            BROADCAST_LOAD_B(5, 0);
            UPDATE_RESULT_VECTOR(pg_true, 0, 5, 0);
            UPDATE_RESULT_VECTOR(pg_true, 1, 5, 0);
            BROADCAST_LOAD_B(6, 0);
            UPDATE_RESULT_VECTOR(pg_true, 0, 6, 0);
            UPDATE_RESULT_VECTOR(pg_true, 1, 6, 0);
            BROADCAST_LOAD_B(7, 0);
            UPDATE_RESULT_VECTOR(pg_true, 0, 7, 0);
            UPDATE_RESULT_VECTOR(pg_true, 1, 7, 0);
          }
        } else {
          for (; k < K; k++) {

            BROADCAST_LOAD_B(0, 0);
            UNPACK_VECTOR_A(0, 0);
            UPDATE_RESULT_VECTOR(pg_true, 0, 0, 0);
            BROADCAST_LOAD_B(1, 0);
            UPDATE_RESULT_VECTOR(pg_true, 0, 1, 0);
            UNPACK_VECTOR_A(1, 0);
            UPDATE_RESULT_VECTOR(pg_true, 1, 0, 0);
            UPDATE_RESULT_VECTOR(pg_true, 1, 1, 0);
            BROADCAST_LOAD_B(2, 0);
            UPDATE_RESULT_VECTOR(pg_true, 0, 2, 0);
            UPDATE_RESULT_VECTOR(pg_true, 1, 2, 0);
            BROADCAST_LOAD_B(3, 0);
            UPDATE_RESULT_VECTOR(pg_true, 0, 3, 0);
            UPDATE_RESULT_VECTOR(pg_true, 1, 3, 0);
            BROADCAST_LOAD_B(4, 0);
            UPDATE_RESULT_VECTOR(pg_true, 0, 4, 0);
            UPDATE_RESULT_VECTOR(pg_true, 1, 4, 0);
            BROADCAST_LOAD_B(5, 0);
            UPDATE_RESULT_VECTOR(pg_true, 0, 5, 0);
            UPDATE_RESULT_VECTOR(pg_true, 1, 5, 0);
            BROADCAST_LOAD_B(6, 0);
            UPDATE_RESULT_VECTOR(pg_true, 0, 6, 0);
            UPDATE_RESULT_VECTOR(pg_true, 1, 6, 0);
            BROADCAST_LOAD_B(7, 0);
            UPDATE_RESULT_VECTOR(pg_true, 0, 7, 0);
            UPDATE_RESULT_VECTOR(pg_true, 1, 7, 0);
          }
        }
      } else {
        for (; k < K; k++) {

          BROADCAST_LOAD_B(0, 0);
          GATHER_LOAD_A(pg_true, 0, 0);
          UPDATE_RESULT_VECTOR(pg_true, 0, 0, 0);
          BROADCAST_LOAD_B(1, 0);
          UPDATE_RESULT_VECTOR(pg_true, 0, 1, 0);
          GATHER_LOAD_A(pg_true, 1, 0);
          UPDATE_RESULT_VECTOR(pg_true, 1, 0, 0);
          UPDATE_RESULT_VECTOR(pg_true, 1, 1, 0);
          BROADCAST_LOAD_B(2, 0);
          UPDATE_RESULT_VECTOR(pg_true, 0, 2, 0);
          UPDATE_RESULT_VECTOR(pg_true, 1, 2, 0);
          BROADCAST_LOAD_B(3, 0);
          UPDATE_RESULT_VECTOR(pg_true, 0, 3, 0);
          UPDATE_RESULT_VECTOR(pg_true, 1, 3, 0);
          BROADCAST_LOAD_B(4, 0);
          UPDATE_RESULT_VECTOR(pg_true, 0, 4, 0);
          UPDATE_RESULT_VECTOR(pg_true, 1, 4, 0);
          BROADCAST_LOAD_B(5, 0);
          UPDATE_RESULT_VECTOR(pg_true, 0, 5, 0);
          UPDATE_RESULT_VECTOR(pg_true, 1, 5, 0);
          BROADCAST_LOAD_B(6, 0);
          UPDATE_RESULT_VECTOR(pg_true, 0, 6, 0);
          UPDATE_RESULT_VECTOR(pg_true, 1, 6, 0);
          BROADCAST_LOAD_B(7, 0);
          UPDATE_RESULT_VECTOR(pg_true, 0, 7, 0);
          UPDATE_RESULT_VECTOR(pg_true, 1, 7, 0);
        }
      }
      VECTOR_STORE(pg_true, 0, 0);
      VECTOR_STORE(pg_true, 0, 1);
      VECTOR_STORE(pg_true, 0, 2);
      VECTOR_STORE(pg_true, 0, 3);
      VECTOR_STORE(pg_true, 0, 4);
      VECTOR_STORE(pg_true, 0, 5);
      VECTOR_STORE(pg_true, 0, 6);
      VECTOR_STORE(pg_true, 0, 7);
      VECTOR_STORE(pg_true, 1, 0);
      VECTOR_STORE(pg_true, 1, 1);
      VECTOR_STORE(pg_true, 1, 2);
      VECTOR_STORE(pg_true, 1, 3);
      VECTOR_STORE(pg_true, 1, 4);
      VECTOR_STORE(pg_true, 1, 5);
      VECTOR_STORE(pg_true, 1, 6);
      VECTOR_STORE(pg_true, 1, 7);
      INCR_C_POINTER(0, 8);
      INCR_C_POINTER(1, 8);
    }
    for (; j < n4; j += 4) {

      CREATE_B_POINTER(0, 0);
      CREATE_B_POINTER(1, 1);
      CREATE_B_POINTER(2, 2);
      CREATE_B_POINTER(3, 3);
      UPDATE_B_POINTER(4);

      BLASLONG k = 0;
      DECLARE_RESULT_VECTOR(0, 0);
      DECLARE_RESULT_VECTOR(0, 1);
      DECLARE_RESULT_VECTOR(0, 2);
      DECLARE_RESULT_VECTOR(0, 3);
      DECLARE_RESULT_VECTOR(1, 0);
      DECLARE_RESULT_VECTOR(1, 1);
      DECLARE_RESULT_VECTOR(1, 2);
      DECLARE_RESULT_VECTOR(1, 3);

      if (LIKELY(packed_a != NULL)) {
        for (; k < K; k++) {

          BROADCAST_LOAD_B(0, 0);
          UNPACK_VECTOR_A(0, 0);
          UPDATE_RESULT_VECTOR(pg_true, 0, 0, 0);
          BROADCAST_LOAD_B(1, 0);
          UPDATE_RESULT_VECTOR(pg_true, 0, 1, 0);
          UNPACK_VECTOR_A(1, 0);
          UPDATE_RESULT_VECTOR(pg_true, 1, 0, 0);
          UPDATE_RESULT_VECTOR(pg_true, 1, 1, 0);
          BROADCAST_LOAD_B(2, 0);
          UPDATE_RESULT_VECTOR(pg_true, 0, 2, 0);
          UPDATE_RESULT_VECTOR(pg_true, 1, 2, 0);
          BROADCAST_LOAD_B(3, 0);
          UPDATE_RESULT_VECTOR(pg_true, 0, 3, 0);
          UPDATE_RESULT_VECTOR(pg_true, 1, 3, 0);
        }
      } else {
        for (; k < K; k++) {

          BROADCAST_LOAD_B(0, 0);
          GATHER_LOAD_A(pg_true, 0, 0);
          UPDATE_RESULT_VECTOR(pg_true, 0, 0, 0);
          BROADCAST_LOAD_B(1, 0);
          UPDATE_RESULT_VECTOR(pg_true, 0, 1, 0);
          GATHER_LOAD_A(pg_true, 1, 0);
          UPDATE_RESULT_VECTOR(pg_true, 1, 0, 0);
          UPDATE_RESULT_VECTOR(pg_true, 1, 1, 0);
          BROADCAST_LOAD_B(2, 0);
          UPDATE_RESULT_VECTOR(pg_true, 0, 2, 0);
          UPDATE_RESULT_VECTOR(pg_true, 1, 2, 0);
          BROADCAST_LOAD_B(3, 0);
          UPDATE_RESULT_VECTOR(pg_true, 0, 3, 0);
          UPDATE_RESULT_VECTOR(pg_true, 1, 3, 0);
        }
      }
      VECTOR_STORE(pg_true, 0, 0);
      VECTOR_STORE(pg_true, 0, 1);
      VECTOR_STORE(pg_true, 0, 2);
      VECTOR_STORE(pg_true, 0, 3);
      VECTOR_STORE(pg_true, 1, 0);
      VECTOR_STORE(pg_true, 1, 1);
      VECTOR_STORE(pg_true, 1, 2);
      VECTOR_STORE(pg_true, 1, 3);
      INCR_C_POINTER(0, 4);
      INCR_C_POINTER(1, 4);
    }
    for (; j < N; j++) {

      CREATE_B_POINTER(0, 0);
      UPDATE_B_POINTER(1);

      BLASLONG k = 0;
      DECLARE_RESULT_VECTOR(0, 0);
      DECLARE_RESULT_VECTOR(1, 0);

      if (LIKELY(packed_a != NULL)) {
        for (; k < K; k++) {

          BROADCAST_LOAD_B(0, 0);
          UNPACK_VECTOR_A(0, 0);
          UPDATE_RESULT_VECTOR(pg_true, 0, 0, 0);
          UNPACK_VECTOR_A(1, 0);
          UPDATE_RESULT_VECTOR(pg_true, 1, 0, 0);
        }
      } else {
        for (; k < K; k++) {

          BROADCAST_LOAD_B(0, 0);
          GATHER_LOAD_A(pg_true, 0, 0);
          UPDATE_RESULT_VECTOR(pg_true, 0, 0, 0);
          GATHER_LOAD_A(pg_true, 1, 0);
          UPDATE_RESULT_VECTOR(pg_true, 1, 0, 0);
        }
      }
      VECTOR_STORE(pg_true, 0, 0);
      VECTOR_STORE(pg_true, 1, 0);
      INCR_C_POINTER(0, 1);
      INCR_C_POINTER(1, 1);
    }

    UPDATE_A_POINTER(v_size2);
    RESET_B_POINTER();
    UPDATE_C_POINTER(v_size2);
  }
  for (; i < v_m1; i += v_size) {

    CREATE_C_POINTER(0, 0);
    CREATE_A_POINTER(0, 0);

    BLASLONG j = 0;
    for (; j < n8; j += 8) {

      CREATE_B_POINTER(0, 0);
      CREATE_B_POINTER(1, 1);
      CREATE_B_POINTER(2, 2);
      CREATE_B_POINTER(3, 3);
      CREATE_B_POINTER(4, 4);
      CREATE_B_POINTER(5, 5);
      CREATE_B_POINTER(6, 6);
      CREATE_B_POINTER(7, 7);
      UPDATE_B_POINTER(8);

      BLASLONG k = 0;
      DECLARE_RESULT_VECTOR(0, 0);
      DECLARE_RESULT_VECTOR(0, 1);
      DECLARE_RESULT_VECTOR(0, 2);
      DECLARE_RESULT_VECTOR(0, 3);
      DECLARE_RESULT_VECTOR(0, 4);
      DECLARE_RESULT_VECTOR(0, 5);
      DECLARE_RESULT_VECTOR(0, 6);
      DECLARE_RESULT_VECTOR(0, 7);

      for (; k < K; k++) {

        BROADCAST_LOAD_B(0, 0);
        GATHER_LOAD_A(pg_true, 0, 0);
        UPDATE_RESULT_VECTOR(pg_true, 0, 0, 0);
        BROADCAST_LOAD_B(1, 0);
        UPDATE_RESULT_VECTOR(pg_true, 0, 1, 0);
        BROADCAST_LOAD_B(2, 0);
        UPDATE_RESULT_VECTOR(pg_true, 0, 2, 0);
        BROADCAST_LOAD_B(3, 0);
        UPDATE_RESULT_VECTOR(pg_true, 0, 3, 0);
        BROADCAST_LOAD_B(4, 0);
        UPDATE_RESULT_VECTOR(pg_true, 0, 4, 0);
        BROADCAST_LOAD_B(5, 0);
        UPDATE_RESULT_VECTOR(pg_true, 0, 5, 0);
        BROADCAST_LOAD_B(6, 0);
        UPDATE_RESULT_VECTOR(pg_true, 0, 6, 0);
        BROADCAST_LOAD_B(7, 0);
        UPDATE_RESULT_VECTOR(pg_true, 0, 7, 0);
      }
      VECTOR_STORE(pg_true, 0, 0);
      VECTOR_STORE(pg_true, 0, 1);
      VECTOR_STORE(pg_true, 0, 2);
      VECTOR_STORE(pg_true, 0, 3);
      VECTOR_STORE(pg_true, 0, 4);
      VECTOR_STORE(pg_true, 0, 5);
      VECTOR_STORE(pg_true, 0, 6);
      VECTOR_STORE(pg_true, 0, 7);
      INCR_C_POINTER(0, 8);
    }
    for (; j < n4; j += 4) {

      CREATE_B_POINTER(0, 0);
      CREATE_B_POINTER(1, 1);
      CREATE_B_POINTER(2, 2);
      CREATE_B_POINTER(3, 3);
      UPDATE_B_POINTER(4);

      BLASLONG k = 0;
      DECLARE_RESULT_VECTOR(0, 0);
      DECLARE_RESULT_VECTOR(0, 1);
      DECLARE_RESULT_VECTOR(0, 2);
      DECLARE_RESULT_VECTOR(0, 3);

      for (; k < K; k++) {

        BROADCAST_LOAD_B(0, 0);
        GATHER_LOAD_A(pg_true, 0, 0);
        UPDATE_RESULT_VECTOR(pg_true, 0, 0, 0);
        BROADCAST_LOAD_B(1, 0);
        UPDATE_RESULT_VECTOR(pg_true, 0, 1, 0);
        BROADCAST_LOAD_B(2, 0);
        UPDATE_RESULT_VECTOR(pg_true, 0, 2, 0);
        BROADCAST_LOAD_B(3, 0);
        UPDATE_RESULT_VECTOR(pg_true, 0, 3, 0);
      }
      VECTOR_STORE(pg_true, 0, 0);
      VECTOR_STORE(pg_true, 0, 1);
      VECTOR_STORE(pg_true, 0, 2);
      VECTOR_STORE(pg_true, 0, 3);
      INCR_C_POINTER(0, 4);
    }
    for (; j < N; j++) {

      CREATE_B_POINTER(0, 0);
      UPDATE_B_POINTER(1);

      BLASLONG k = 0;
      DECLARE_RESULT_VECTOR(0, 0);

      for (; k < K; k++) {

        BROADCAST_LOAD_B(0, 0);
        GATHER_LOAD_A(pg_true, 0, 0);
        UPDATE_RESULT_VECTOR(pg_true, 0, 0, 0);
      }
      VECTOR_STORE(pg_true, 0, 0);
      INCR_C_POINTER(0, 1);
    }

    UPDATE_A_POINTER(v_size);
    RESET_B_POINTER();
    UPDATE_C_POINTER(v_size);
  }
  for (; i < M; i += v_size) {
    const svbool_t pg_tail = svwhilelt_b32((uint32_t)i, (uint32_t)(M));
    CREATE_C_POINTER(0, 0);
    CREATE_A_POINTER(0, 0);

    BLASLONG j = 0;
    for (; j < n8; j += 8) {

      CREATE_B_POINTER(0, 0);
      CREATE_B_POINTER(1, 1);
      CREATE_B_POINTER(2, 2);
      CREATE_B_POINTER(3, 3);
      CREATE_B_POINTER(4, 4);
      CREATE_B_POINTER(5, 5);
      CREATE_B_POINTER(6, 6);
      CREATE_B_POINTER(7, 7);
      UPDATE_B_POINTER(8);

      BLASLONG k = 0;
      DECLARE_RESULT_VECTOR(0, 0);
      DECLARE_RESULT_VECTOR(0, 1);
      DECLARE_RESULT_VECTOR(0, 2);
      DECLARE_RESULT_VECTOR(0, 3);
      DECLARE_RESULT_VECTOR(0, 4);
      DECLARE_RESULT_VECTOR(0, 5);
      DECLARE_RESULT_VECTOR(0, 6);
      DECLARE_RESULT_VECTOR(0, 7);

      for (; k < K; k++) {

        BROADCAST_LOAD_B(0, 0);
        GATHER_LOAD_A(pg_tail, 0, 0);
        UPDATE_RESULT_VECTOR(pg_tail, 0, 0, 0);
        BROADCAST_LOAD_B(1, 0);
        UPDATE_RESULT_VECTOR(pg_tail, 0, 1, 0);
        BROADCAST_LOAD_B(2, 0);
        UPDATE_RESULT_VECTOR(pg_tail, 0, 2, 0);
        BROADCAST_LOAD_B(3, 0);
        UPDATE_RESULT_VECTOR(pg_tail, 0, 3, 0);
        BROADCAST_LOAD_B(4, 0);
        UPDATE_RESULT_VECTOR(pg_tail, 0, 4, 0);
        BROADCAST_LOAD_B(5, 0);
        UPDATE_RESULT_VECTOR(pg_tail, 0, 5, 0);
        BROADCAST_LOAD_B(6, 0);
        UPDATE_RESULT_VECTOR(pg_tail, 0, 6, 0);
        BROADCAST_LOAD_B(7, 0);
        UPDATE_RESULT_VECTOR(pg_tail, 0, 7, 0);
      }
      VECTOR_STORE(pg_tail, 0, 0);
      VECTOR_STORE(pg_tail, 0, 1);
      VECTOR_STORE(pg_tail, 0, 2);
      VECTOR_STORE(pg_tail, 0, 3);
      VECTOR_STORE(pg_tail, 0, 4);
      VECTOR_STORE(pg_tail, 0, 5);
      VECTOR_STORE(pg_tail, 0, 6);
      VECTOR_STORE(pg_tail, 0, 7);
      INCR_C_POINTER(0, 8);
    }
    for (; j < n4; j += 4) {

      CREATE_B_POINTER(0, 0);
      CREATE_B_POINTER(1, 1);
      CREATE_B_POINTER(2, 2);
      CREATE_B_POINTER(3, 3);
      UPDATE_B_POINTER(4);

      BLASLONG k = 0;
      DECLARE_RESULT_VECTOR(0, 0);
      DECLARE_RESULT_VECTOR(0, 1);
      DECLARE_RESULT_VECTOR(0, 2);
      DECLARE_RESULT_VECTOR(0, 3);

      for (; k < K; k++) {

        BROADCAST_LOAD_B(0, 0);
        GATHER_LOAD_A(pg_tail, 0, 0);
        UPDATE_RESULT_VECTOR(pg_tail, 0, 0, 0);
        BROADCAST_LOAD_B(1, 0);
        UPDATE_RESULT_VECTOR(pg_tail, 0, 1, 0);
        BROADCAST_LOAD_B(2, 0);
        UPDATE_RESULT_VECTOR(pg_tail, 0, 2, 0);
        BROADCAST_LOAD_B(3, 0);
        UPDATE_RESULT_VECTOR(pg_tail, 0, 3, 0);
      }
      VECTOR_STORE(pg_tail, 0, 0);
      VECTOR_STORE(pg_tail, 0, 1);
      VECTOR_STORE(pg_tail, 0, 2);
      VECTOR_STORE(pg_tail, 0, 3);
      INCR_C_POINTER(0, 4);
    }
    for (; j < N; j++) {

      CREATE_B_POINTER(0, 0);
      UPDATE_B_POINTER(1);

      BLASLONG k = 0;
      DECLARE_RESULT_VECTOR(0, 0);

      for (; k < K; k++) {

        BROADCAST_LOAD_B(0, 0);
        GATHER_LOAD_A(pg_tail, 0, 0);
        UPDATE_RESULT_VECTOR(pg_tail, 0, 0, 0);
      }
      VECTOR_STORE(pg_tail, 0, 0);
      INCR_C_POINTER(0, 1);
    }

    UPDATE_A_POINTER(0);
    RESET_B_POINTER();
    UPDATE_C_POINTER(0);
  }

  if (pack_a)
    free(packed_a);

  return 0;
}