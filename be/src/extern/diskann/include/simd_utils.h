#pragma once

#ifdef _WINDOWS
#include <immintrin.h>
#include <smmintrin.h>
#include <tmmintrin.h>
#include <intrin.h>
#else
#include <immintrin.h>
#endif

namespace diskann
{
static inline __m256 _mm256_mul_epi8(__m256i X)
{
    __m256i zero = _mm256_setzero_si256();

    __m256i sign_x = _mm256_cmpgt_epi8(zero, X);

    __m256i xlo = _mm256_unpacklo_epi8(X, sign_x);
    __m256i xhi = _mm256_unpackhi_epi8(X, sign_x);

    return _mm256_cvtepi32_ps(_mm256_add_epi32(_mm256_madd_epi16(xlo, xlo), _mm256_madd_epi16(xhi, xhi)));
}

static inline __m128 _mm_mulhi_epi8(__m128i X)
{
    __m128i zero = _mm_setzero_si128();
    __m128i sign_x = _mm_cmplt_epi8(X, zero);
    __m128i xhi = _mm_unpackhi_epi8(X, sign_x);

    return _mm_cvtepi32_ps(_mm_add_epi32(_mm_setzero_si128(), _mm_madd_epi16(xhi, xhi)));
}

static inline __m128 _mm_mulhi_epi8_shift32(__m128i X)
{
    __m128i zero = _mm_setzero_si128();
    X = _mm_srli_epi64(X, 32);
    __m128i sign_x = _mm_cmplt_epi8(X, zero);
    __m128i xhi = _mm_unpackhi_epi8(X, sign_x);

    return _mm_cvtepi32_ps(_mm_add_epi32(_mm_setzero_si128(), _mm_madd_epi16(xhi, xhi)));
}
static inline __m128 _mm_mul_epi8(__m128i X, __m128i Y)
{
    __m128i zero = _mm_setzero_si128();

    __m128i sign_x = _mm_cmplt_epi8(X, zero);
    __m128i sign_y = _mm_cmplt_epi8(Y, zero);

    __m128i xlo = _mm_unpacklo_epi8(X, sign_x);
    __m128i xhi = _mm_unpackhi_epi8(X, sign_x);
    __m128i ylo = _mm_unpacklo_epi8(Y, sign_y);
    __m128i yhi = _mm_unpackhi_epi8(Y, sign_y);

    return _mm_cvtepi32_ps(_mm_add_epi32(_mm_madd_epi16(xlo, ylo), _mm_madd_epi16(xhi, yhi)));
}
static inline __m128 _mm_mul_epi8(__m128i X)
{
    __m128i zero = _mm_setzero_si128();
    __m128i sign_x = _mm_cmplt_epi8(X, zero);
    __m128i xlo = _mm_unpacklo_epi8(X, sign_x);
    __m128i xhi = _mm_unpackhi_epi8(X, sign_x);

    return _mm_cvtepi32_ps(_mm_add_epi32(_mm_madd_epi16(xlo, xlo), _mm_madd_epi16(xhi, xhi)));
}

static inline __m128 _mm_mul32_pi8(__m128i X, __m128i Y)
{
    __m128i xlo = _mm_cvtepi8_epi16(X), ylo = _mm_cvtepi8_epi16(Y);
    return _mm_cvtepi32_ps(_mm_unpacklo_epi32(_mm_madd_epi16(xlo, ylo), _mm_setzero_si128()));
}

static inline __m256 _mm256_mul_epi8(__m256i X, __m256i Y)
{
    __m256i zero = _mm256_setzero_si256();

    __m256i sign_x = _mm256_cmpgt_epi8(zero, X);
    __m256i sign_y = _mm256_cmpgt_epi8(zero, Y);

    __m256i xlo = _mm256_unpacklo_epi8(X, sign_x);
    __m256i xhi = _mm256_unpackhi_epi8(X, sign_x);
    __m256i ylo = _mm256_unpacklo_epi8(Y, sign_y);
    __m256i yhi = _mm256_unpackhi_epi8(Y, sign_y);

    return _mm256_cvtepi32_ps(_mm256_add_epi32(_mm256_madd_epi16(xlo, ylo), _mm256_madd_epi16(xhi, yhi)));
}

static inline __m256 _mm256_mul32_pi8(__m128i X, __m128i Y)
{
    __m256i xlo = _mm256_cvtepi8_epi16(X), ylo = _mm256_cvtepi8_epi16(Y);
    return _mm256_blend_ps(_mm256_cvtepi32_ps(_mm256_madd_epi16(xlo, ylo)), _mm256_setzero_ps(), 252);
}

static inline float _mm256_reduce_add_ps(__m256 x)
{
    /* ( x3+x7, x2+x6, x1+x5, x0+x4 ) */
    const __m128 x128 = _mm_add_ps(_mm256_extractf128_ps(x, 1), _mm256_castps256_ps128(x));
    /* ( -, -, x1+x3+x5+x7, x0+x2+x4+x6 ) */
    const __m128 x64 = _mm_add_ps(x128, _mm_movehl_ps(x128, x128));
    /* ( -, -, -, x0+x1+x2+x3+x4+x5+x6+x7 ) */
    const __m128 x32 = _mm_add_ss(x64, _mm_shuffle_ps(x64, x64, 0x55));
    /* Conversion to float is a no-op on x86-64 */
    return _mm_cvtss_f32(x32);
}
} // namespace diskann
