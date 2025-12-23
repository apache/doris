/***************************************************************************
Copyright (c) 2022, The OpenBLAS Project
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

#include "common.h"

#if !defined(DOUBLE)
#define VSETVL(n)               __riscv_vsetvl_e32m2(n)
#define VSETVL_MAX              __riscv_vsetvlmax_e32m2()
#define FLOAT_V_T               vfloat32m2_t
#define FLOAT_VX2_T             vfloat32m2x2_t
#define VGET_VX2                __riscv_vget_v_f32m2x2_f32m2
#define VSET_VX2                __riscv_vset_v_f32m2_f32m2x2
#define VLSEV_FLOAT             __riscv_vlse32_v_f32m2
#define VSSEV_FLOAT             __riscv_vsse32_v_f32m2
#define VSEV_FLOAT              __riscv_vse32_v_f32m2
#define VSSEG2_FLOAT            __riscv_vsseg2e32_v_f32m2x2
#define VLSSEG2_FLOAT           __riscv_vlsseg2e32_v_f32m2x2
#define VSSSEG2_FLOAT           __riscv_vssseg2e32_v_f32m2x2
#define VFMACCVF_FLOAT          __riscv_vfmacc_vf_f32m2
#define VFNMSACVF_FLOAT         __riscv_vfnmsac_vf_f32m2
#define VFMULVF_FLOAT           __riscv_vfmul_vf_f32m2
#else
#define VSETVL(n)               __riscv_vsetvl_e64m2(n)
#define VSETVL_MAX              __riscv_vsetvlmax_e64m2()
#define FLOAT_V_T               vfloat64m2_t
#define FLOAT_VX2_T             vfloat64m2x2_t
#define VGET_VX2                __riscv_vget_v_f64m2x2_f64m2
#define VSET_VX2                __riscv_vset_v_f64m2_f64m2x2
#define VLSEV_FLOAT             __riscv_vlse64_v_f64m2
#define VSSEV_FLOAT             __riscv_vsse64_v_f64m2
#define VSEV_FLOAT              __riscv_vse64_v_f64m2
#define VSSEG2_FLOAT            __riscv_vsseg2e64_v_f64m2x2
#define VLSSEG2_FLOAT           __riscv_vlsseg2e64_v_f64m2x2
#define VSSSEG2_FLOAT           __riscv_vssseg2e64_v_f64m2x2
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f64m2
#define VFMACCVF_FLOAT          __riscv_vfmacc_vf_f64m2
#define VFNMSACVF_FLOAT         __riscv_vfnmsac_vf_f64m2
#define VFMULVF_FLOAT           __riscv_vfmul_vf_f64m2
#endif


static FLOAT dm1 = -1.;

#ifdef CONJ
#define GEMM_KERNEL   GEMM_KERNEL_L
#else
#define GEMM_KERNEL   GEMM_KERNEL_N
#endif

#if GEMM_DEFAULT_UNROLL_N == 1
#define GEMM_UNROLL_N_SHIFT 0
#endif

#if GEMM_DEFAULT_UNROLL_N == 2
#define GEMM_UNROLL_N_SHIFT 1
#endif

#if GEMM_DEFAULT_UNROLL_N == 4
#define GEMM_UNROLL_N_SHIFT 2
#endif

#if GEMM_DEFAULT_UNROLL_N == 8
#define GEMM_UNROLL_N_SHIFT 3
#endif

#if GEMM_DEFAULT_UNROLL_N == 16
#define GEMM_UNROLL_N_SHIFT 4
#endif

// Optimizes the implementation in ../arm64/trsm_kernel_LT_sve.c

#ifndef COMPLEX

static inline void solve(BLASLONG m, BLASLONG n, FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc) {

    FLOAT aa;
    FLOAT* pc;

    int i, j, k;

    BLASLONG stride_ldc = sizeof(FLOAT) * ldc;

    FLOAT_V_T vb, vc;

    size_t vl;

    for (i = 0; i < m; i++) {

        aa = *(a + i);
        pc  = c;
        for (j = n; j > 0; j -= vl) {
            vl = VSETVL(j);
            vb = VLSEV_FLOAT(pc + i, stride_ldc, vl);
            vb = VFMULVF_FLOAT(vb, aa, vl);
            VSEV_FLOAT(b, vb, vl);
            VSSEV_FLOAT(pc + i, stride_ldc, vb, vl);
            b   += vl;

            for (k = i + 1; k < m; k++) {
                vc = VLSEV_FLOAT(pc + k, stride_ldc, vl);
                vc = VFNMSACVF_FLOAT(vc, *(a + k), vb, vl);
                VSSEV_FLOAT(pc + k, stride_ldc, vc, vl);
            }
            pc  += vl * ldc;
        }
        a += m;
    }
}

#else

static inline void solve(BLASLONG m, BLASLONG n, FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc) {

    FLOAT aa1, aa2;
    FLOAT *pc;
    int i, j, k;

    BLASLONG stride_ldc = sizeof(FLOAT) * ldc * 2;

    FLOAT_VX2_T vbx2, vsx2, vcx2;
    FLOAT_V_T vb1, vb2, vc1, vc2, vs1, vs2;
    size_t vl;

    ldc *= 2;

    for (i = 0; i < m; i++) {
        aa1 = *(a + i * 2 + 0);
        aa2 = *(a + i * 2 + 1);
        pc  = c;

        for (j = n; j > 0; j -= vl) {
            vl = VSETVL(j);
            vbx2 = VLSSEG2_FLOAT(pc + i * 2, stride_ldc, vl);
            vb1 = VGET_VX2(vbx2, 0);
            vb2 = VGET_VX2(vbx2, 1);
#ifndef CONJ
            vs1 =   VFMULVF_FLOAT(vb1, aa1, vl);
            vs1 = VFNMSACVF_FLOAT(vs1, aa2, vb2, vl);
            vs2 =   VFMULVF_FLOAT(vb2, aa1, vl);
            vs2 =  VFMACCVF_FLOAT(vs2, aa2, vb1, vl);
#else
            vs1 =   VFMULVF_FLOAT(vb1, aa1, vl);
            vs1 =  VFMACCVF_FLOAT(vs1, aa2, vb2, vl);
            vs2 =   VFMULVF_FLOAT(vb2, aa1, vl);
            vs2 = VFNMSACVF_FLOAT(vs2, aa2, vb1, vl);
#endif
            vsx2 = VSET_VX2(vsx2, 0, vs1);
            vsx2 = VSET_VX2(vsx2, 1, vs2);
            VSSEG2_FLOAT(b, vsx2, vl);
            VSSSEG2_FLOAT(pc + i * 2, stride_ldc, vsx2, vl);
            b   += vl * 2;

            for (k = i + 1; k < m; k++) {
                vcx2 = VLSSEG2_FLOAT(pc + k * 2, stride_ldc, vl);
                vc1 = VGET_VX2(vcx2, 0);
                vc2 = VGET_VX2(vcx2, 1);
#ifndef CONJ
                vc1 =  VFMACCVF_FLOAT(vc1, *(a + k * 2 + 1), vs2, vl);
                vc1 = VFNMSACVF_FLOAT(vc1, *(a + k * 2 + 0), vs1, vl);
                vc2 = VFNMSACVF_FLOAT(vc2, *(a + k * 2 + 1), vs1, vl);
                vc2 = VFNMSACVF_FLOAT(vc2, *(a + k * 2 + 0), vs2, vl);
#else                                                        
                vc1 = VFNMSACVF_FLOAT(vc1, *(a + k * 2 + 1), vs2, vl);
                vc1 = VFNMSACVF_FLOAT(vc1, *(a + k * 2 + 0), vs1, vl);
                vc2 =  VFMACCVF_FLOAT(vc2, *(a + k * 2 + 1), vs1, vl);
                vc2 = VFNMSACVF_FLOAT(vc2, *(a + k * 2 + 0), vs2, vl);
#endif
                vcx2 = VSET_VX2(vcx2, 0, vc1);
                vcx2 = VSET_VX2(vcx2, 1, vc2);
                VSSSEG2_FLOAT(pc + k * 2, stride_ldc, vcx2, vl);
            }
            pc  += vl * ldc * 2;
        }

        a += m * 2;
    }
}

#endif


int CNAME(BLASLONG m, BLASLONG n, BLASLONG k, FLOAT dummy1,
#ifdef COMPLEX
	   FLOAT dummy2,
#endif
	   FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG offset){

  FLOAT *aa, *cc;
  BLASLONG  kk;
  BLASLONG i, j;

  size_t vl = VSETVL_MAX;

    //fprintf(stderr, "%s , %s, m = %4ld  n = %4ld  k = %4ld offset = %4ld\n", __FILE__, __FUNCTION__, m, n, k, offset); // Debug

  j = (n >> GEMM_UNROLL_N_SHIFT);

  while (j > 0) {

    kk = offset;
    aa = a;
    cc = c;

    i = vl;

    while (i <= m) {

      if (kk > 0) {
        GEMM_KERNEL(vl, GEMM_UNROLL_N, kk, dm1,
#ifdef COMPLEX
            ZERO,
#endif
            aa, b, cc, ldc);
      }

      solve(vl, GEMM_UNROLL_N,
          aa + kk * vl * COMPSIZE,
          b  + kk * GEMM_UNROLL_N * COMPSIZE,
          cc, ldc);

      aa += vl * k * COMPSIZE;
      cc += vl     * COMPSIZE;
      kk += vl;
      i += vl;
    }

    i = m % vl;
    if (i) {
      if (kk > 0) {
        GEMM_KERNEL(i, GEMM_UNROLL_N, kk, dm1,
#ifdef COMPLEX
            ZERO,
#endif
            aa, b, cc, ldc);
      }
      solve(i, GEMM_UNROLL_N,
          aa + kk * i             * COMPSIZE,
          b  + kk * GEMM_UNROLL_N * COMPSIZE,
          cc, ldc);

      aa += i * k * COMPSIZE;
      cc += i     * COMPSIZE;
      kk += i;

    }

    b += GEMM_UNROLL_N * k   * COMPSIZE;
    c += GEMM_UNROLL_N * ldc * COMPSIZE;
    j --;
  }

  if (n & (GEMM_UNROLL_N - 1)) {

    j = (GEMM_UNROLL_N >> 1);
    while (j > 0) {
      if (n & j) {

        kk = offset;
        aa = a;
        cc = c;

        i = vl;

        while (i <= m) {
          if (kk > 0) {
            GEMM_KERNEL(vl, j, kk, dm1,
#ifdef COMPLEX
                ZERO,
#endif
                aa,
                b,
                cc,
                ldc);
          }

          solve(vl, j,
              aa + kk * vl * COMPSIZE,
              b  + kk * j             * COMPSIZE, cc, ldc);

          aa += vl * k * COMPSIZE;
          cc += vl     * COMPSIZE;
          kk += vl;
          i += vl;
        }

        i = m % vl;
        if (i) {
          if (kk > 0) {
            GEMM_KERNEL(i, j, kk, dm1,
#ifdef COMPLEX
                ZERO,
#endif
                aa,
                b,
                cc,
                ldc);
          }

          solve(i, j,
              aa + kk * i * COMPSIZE,
              b  + kk * j * COMPSIZE, cc, ldc);

          aa += i * k * COMPSIZE;
          cc += i     * COMPSIZE;
          kk += i;

        }

        b += j * k   * COMPSIZE;
        c += j * ldc * COMPSIZE;
      }
      j >>= 1;
    }
  }

  return 0;
}
