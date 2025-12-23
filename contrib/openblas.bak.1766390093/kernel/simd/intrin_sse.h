#define V_SIMD 128
#define V_SIMD_F64 1
/***************************
 * Data Type
 ***************************/
typedef __m128  v_f32;
typedef __m128d  v_f64;
#define v_nlanes_f32 4
#define v_nlanes_f64 2
/***************************
 * Arithmetic
 ***************************/
#define v_add_f32 _mm_add_ps
#define v_add_f64 _mm_add_pd
#define v_sub_f32 _mm_sub_ps
#define v_sub_f64 _mm_sub_pd
#define v_mul_f32 _mm_mul_ps
#define v_mul_f64 _mm_mul_pd
#ifdef HAVE_FMA3
    // multiply and add, a*b + c
    #define v_muladd_f32 _mm_fmadd_ps
    #define v_muladd_f64 _mm_fmadd_pd
    // multiply and subtract, a*b - c
    #define v_mulsub_f32 _mm_fmsub_ps
    #define v_mulsub_f64 _mm_fmsub_pd
#elif defined(HAVE_FMA4)
    // multiply and add, a*b + c
    #define v_muladd_f32 _mm_macc_ps
    #define v_muladd_f64 _mm_macc_pd
    // multiply and subtract, a*b - c
    #define v_mulsub_f32 _mm_msub_ps
    #define v_mulsub_f64 _mm_msub_pd
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
#endif // HAVE_FMA3

// Horizontal add: Calculates the sum of all vector elements.
BLAS_FINLINE float v_sum_f32(__m128 a)
{
#ifdef HAVE_SSE3
    __m128 sum_halves = _mm_hadd_ps(a, a);
    return _mm_cvtss_f32(_mm_hadd_ps(sum_halves, sum_halves));
#else
    __m128 t1 = _mm_movehl_ps(a, a);
    __m128 t2 = _mm_add_ps(a, t1);
    __m128 t3 = _mm_shuffle_ps(t2, t2, 1);
    __m128 t4 = _mm_add_ss(t2, t3);
    return _mm_cvtss_f32(t4);
#endif
}

BLAS_FINLINE double v_sum_f64(__m128d a)
{
#ifdef HAVE_SSE3
    return _mm_cvtsd_f64(_mm_hadd_pd(a, a));
#else
    return _mm_cvtsd_f64(_mm_add_pd(a, _mm_unpackhi_pd(a, a)));
#endif
}
/***************************
 * memory
 ***************************/
// unaligned load
#define v_loadu_f32 _mm_loadu_ps
#define v_loadu_f64 _mm_loadu_pd
#define v_storeu_f32 _mm_storeu_ps
#define v_storeu_f64 _mm_storeu_pd
#define v_setall_f32(VAL) _mm_set1_ps(VAL)
#define v_setall_f64(VAL) _mm_set1_pd(VAL)
#define v_zero_f32 _mm_setzero_ps
#define v_zero_f64 _mm_setzero_pd