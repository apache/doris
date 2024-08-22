// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#ifdef __SSE3__
#include <immintrin.h>
#endif

namespace doris {
namespace simd {

float fvec_L2sqr_ref(const float* x, const float* y, size_t d) {
    size_t i;
    float res = 0;
    for (i = 0; i < d; i++) {
        const float tmp = x[i] - y[i];
        res += tmp * tmp;
    }
    return res;
}

float fvec_inner_product_ref(const float* x, const float* y, size_t d) {
    size_t i;
    float res = 0;
    for (i = 0; i < d; i++)
        res += x[i] * y[i];
    return res;
}

float fvec_norm_L2sqr_ref(const float* x, size_t d) {
    size_t i;
    double res = 0;
    for (i = 0; i < d; i++)
        res += x[i] * x[i];
    return res;
}

#ifdef __SSE3__

// reads 0 <= d < 4 floats as __m128
static inline __m128 masked_read(int d, const float* x) {
    assert(0 <= d && d < 4);
    ALIGNED(16) float buf[4] = {0, 0, 0, 0};
    switch (d) {
    case 3:
        buf[2] = x[2];
    case 2:
        buf[1] = x[1];
    case 1:
        buf[0] = x[0];
    }
    return _mm_load_ps(buf);
    // cannot use AVX2 _mm_mask_set1_epi32
}

float fvec_norm_L2sqr(const float* x, size_t d) {
    __m128 mx;
    __m128 msum1 = _mm_setzero_ps();

    while (d >= 4) {
        mx = _mm_loadu_ps(x);
        x += 4;
        msum1 = _mm_add_ps(msum1, _mm_mul_ps(mx, mx));
        d -= 4;
    }

    mx = masked_read(d, x);
    msum1 = _mm_add_ps(msum1, _mm_mul_ps(mx, mx));

    msum1 = _mm_hadd_ps(msum1, msum1);
    msum1 = _mm_hadd_ps(msum1, msum1);
    return _mm_cvtss_f32(msum1);
}

#endif

#if defined(__AVX2__)

float fvec_inner_product(const float* x, const float* y, size_t d) {
    __m256 msum1 = _mm256_setzero_ps();

    while (d >= 8) {
        __m256 mx = _mm256_loadu_ps(x);
        x += 8;
        __m256 my = _mm256_loadu_ps(y);
        y += 8;
        msum1 = _mm256_add_ps(msum1, _mm256_mul_ps(mx, my));
        d -= 8;
    }

    __m128 msum2 = _mm256_extractf128_ps(msum1, 1);
    msum2 = _mm_add_ps(msum2, _mm256_extractf128_ps(msum1, 0));

    if (d >= 4) {
        __m128 mx = _mm_loadu_ps(x);
        x += 4;
        __m128 my = _mm_loadu_ps(y);
        y += 4;
        msum2 = _mm_add_ps(msum2, _mm_mul_ps(mx, my));
        d -= 4;
    }

    if (d > 0) {
        __m128 mx = masked_read(d, x);
        __m128 my = masked_read(d, y);
        msum2 = _mm_add_ps(msum2, _mm_mul_ps(mx, my));
    }

    msum2 = _mm_hadd_ps(msum2, msum2);
    msum2 = _mm_hadd_ps(msum2, msum2);
    return _mm_cvtss_f32(msum2);
}

float fvec_L2sqr(const float* x, const float* y, size_t d) {
    __m256 msum1 = _mm256_setzero_ps();

    while (d >= 8) {
        __m256 mx = _mm256_loadu_ps(x);
        x += 8;
        __m256 my = _mm256_loadu_ps(y);
        y += 8;
        const __m256 a_m_b1 = _mm256_sub_ps(mx, my);
        msum1 = _mm256_add_ps(msum1, _mm256_mul_ps(a_m_b1, a_m_b1));
        d -= 8;
    }

    __m128 msum2 = _mm256_extractf128_ps(msum1, 1);
    msum2 = _mm_add_ps(msum2, _mm256_extractf128_ps(msum1, 0));

    if (d >= 4) {
        __m128 mx = _mm_loadu_ps(x);
        x += 4;
        __m128 my = _mm_loadu_ps(y);
        y += 4;
        const __m128 a_m_b1 = _mm_sub_ps(mx, my);
        msum2 = _mm_add_ps(msum2, _mm_mul_ps(a_m_b1, a_m_b1));
        d -= 4;
    }

    if (d > 0) {
        __m128 mx = masked_read(d, x);
        __m128 my = masked_read(d, y);
        __m128 a_m_b1 = _mm_sub_ps(mx, my);
        msum2 = _mm_add_ps(msum2, _mm_mul_ps(a_m_b1, a_m_b1));
    }

    msum2 = _mm_hadd_ps(msum2, msum2);
    msum2 = _mm_hadd_ps(msum2, msum2);
    return _mm_cvtss_f32(msum2);
}

#elif defined(__SSE3__)

float fvec_L2sqr(const float* x, const float* y, size_t d) {
    __m128 msum1 = _mm_setzero_ps();

    while (d >= 4) {
        __m128 mx = _mm_loadu_ps(x);
        x += 4;
        __m128 my = _mm_loadu_ps(y);
        y += 4;
        const __m128 a_m_b1 = _mm_sub_ps(mx, my);
        msum1 = _mm_add_ps(msum1, _mm_mul_ps(a_m_b1, a_m_b1));
        d -= 4;
    }

    if (d > 0) {
        // add the last 1, 2 or 3 values
        __m128 mx = masked_read(d, x);
        __m128 my = masked_read(d, y);
        __m128 a_m_b1 = _mm_sub_ps(mx, my);
        msum1 = _mm_add_ps(msum1, _mm_mul_ps(a_m_b1, a_m_b1));
    }

    msum1 = _mm_hadd_ps(msum1, msum1);
    msum1 = _mm_hadd_ps(msum1, msum1);
    return _mm_cvtss_f32(msum1);
}

float fvec_inner_product(const float* x, const float* y, size_t d) {
    __m128 mx, my;
    __m128 msum1 = _mm_setzero_ps();

    while (d >= 4) {
        mx = _mm_loadu_ps(x);
        x += 4;
        my = _mm_loadu_ps(y);
        y += 4;
        msum1 = _mm_add_ps(msum1, _mm_mul_ps(mx, my));
        d -= 4;
    }

    // add the last 1, 2, or 3 values
    mx = masked_read(d, x);
    my = masked_read(d, y);
    __m128 prod = _mm_mul_ps(mx, my);

    msum1 = _mm_add_ps(msum1, prod);

    msum1 = _mm_hadd_ps(msum1, msum1);
    msum1 = _mm_hadd_ps(msum1, msum1);
    return _mm_cvtss_f32(msum1);
}

#else

float fvec_norm_L2sqr(const float* x, size_t d) {
    return fvec_norm_L2sqr_ref(x, d);
}

float fvec_L2sqr(const float* x, const float* y, size_t d) {
    return fvec_L2sqr_ref(x, y, d);
}

float fvec_inner_product(const float* x, const float* y, size_t d) {
    return fvec_inner_product_ref(x, y, d);
}

#endif

void fvec_renorm_L2(size_t d, size_t nx, float* __restrict x) {
    for (int64_t i = 0; i < nx; i++) {
        float* __restrict xi = x + i * d;

        float nr = fvec_norm_L2sqr(xi, d);

        if (nr > 0) {
            size_t j;
            const float inv_nr = 1.0 / sqrtf(nr);
            for (j = 0; j < d; j++)
                xi[j] *= inv_nr;
        }
    }
}

} // namespace simd
} // namespace doris
