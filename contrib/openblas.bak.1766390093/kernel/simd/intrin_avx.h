#define V_SIMD 256
#define V_SIMD_F64 1
/***************************
 * Data Type
 ***************************/
typedef __m256  v_f32;
typedef __m256d  v_f64;
#define v_nlanes_f32 8
#define v_nlanes_f64 4
/***************************
 * Arithmetic
 ***************************/
#define v_add_f32 _mm256_add_ps
#define v_add_f64 _mm256_add_pd
#define v_sub_f32 _mm256_sub_ps
#define v_sub_f64 _mm256_sub_pd
#define v_mul_f32 _mm256_mul_ps
#define v_mul_f64 _mm256_mul_pd

#ifdef HAVE_FMA3
    // multiply and add, a*b + c
    #define v_muladd_f32 _mm256_fmadd_ps
    #define v_muladd_f64 _mm256_fmadd_pd
    // multiply and subtract, a*b - c
    #define v_mulsub_f32 _mm256_fmsub_ps
    #define v_mulsub_f64 _mm256_fmsub_pd
#else
    // multiply and add, a*b + c
    BLAS_FINLINE v_f32 v_muladd_f32(v_f32 a, v_f32 b, v_f32 c)
    { return v_add_f32(v_mul_f32(a, b), c); }
    BLAS_FINLINE v_f64 v_muladd_f64(v_f64 a, v_f64 b, v_f64 c)
    { return v_add_f64(v_mul_f64(a, b), c); }
    // multiply and subtract, a*b - c
    BLAS_FINLINE v_f32 v_mulsub_f32(v_f32 a, v_f32 b, v_f32 c)
    { return v_sub_f32(v_mul_f32(a, b), c); }
    BLAS_FINLINE v_f64 v_mulsub_f64(v_f64 a, v_f64 b, v_f64 c)
    { return v_sub_f64(v_mul_f64(a, b), c); }
#endif // !HAVE_FMA3

// Horizontal add: Calculates the sum of all vector elements.
BLAS_FINLINE float v_sum_f32(__m256 a)
{
    __m256 sum_halves = _mm256_hadd_ps(a, a);
    sum_halves = _mm256_hadd_ps(sum_halves, sum_halves);
    __m128 lo = _mm256_castps256_ps128(sum_halves);
    __m128 hi = _mm256_extractf128_ps(sum_halves, 1);
    __m128 sum = _mm_add_ps(lo, hi);
    return _mm_cvtss_f32(sum);
}

BLAS_FINLINE double v_sum_f64(__m256d a)
{
    __m256d sum_halves = _mm256_hadd_pd(a, a);
    __m128d lo = _mm256_castpd256_pd128(sum_halves);
    __m128d hi = _mm256_extractf128_pd(sum_halves, 1);
    __m128d sum = _mm_add_pd(lo, hi);
    return _mm_cvtsd_f64(sum);
}
/***************************
 * memory
 ***************************/
// unaligned load
#define v_loadu_f32 _mm256_loadu_ps
#define v_loadu_f64 _mm256_loadu_pd
#define v_storeu_f32 _mm256_storeu_ps
#define v_storeu_f64 _mm256_storeu_pd
#define v_setall_f32(VAL) _mm256_set1_ps(VAL)
#define v_setall_f64(VAL) _mm256_set1_pd(VAL)
#define v_zero_f32 _mm256_setzero_ps
#define v_zero_f64 _mm256_setzero_pd