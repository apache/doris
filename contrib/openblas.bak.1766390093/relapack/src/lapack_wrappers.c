#include "relapack.h"

////////////
// XLAUUM //
////////////

#if INCLUDE_SLAUUM
void LAPACK(slauum)(
    const char *uplo, const blasint *n,
    float *A, const blasint *ldA,
    blasint *info
) {
    RELAPACK_slauum(uplo, n, A, ldA, info);
}
#endif

#if INCLUDE_DLAUUM
void LAPACK(dlauum)(
    const char *uplo, const blasint *n,
    double *A, const blasint *ldA,
    blasint *info
) {
    RELAPACK_dlauum(uplo, n, A, ldA, info);
}
#endif

#if INCLUDE_CLAUUM
void LAPACK(clauum)(
    const char *uplo, const blasint *n,
    float *A, const blasint *ldA,
    blasint *info
) {
    RELAPACK_clauum(uplo, n, A, ldA, info);
}
#endif

#if INCLUDE_ZLAUUM
void LAPACK(zlauum)(
    const char *uplo, const blasint *n,
    double *A, const blasint *ldA,
    blasint *info
) {
    RELAPACK_zlauum(uplo, n, A, ldA, info);
}
#endif


////////////
// XSYGST //
////////////

#if INCLUDE_SSYGST
void LAPACK(ssygst)(
    const blasint *itype, const char *uplo, const blasint *n,
    float *A, const blasint *ldA, const float *B, const blasint *ldB,
    blasint *info
) {
    RELAPACK_ssygst(itype, uplo, n, A, ldA, B, ldB, info);
}
#endif

#if INCLUDE_DSYGST
void LAPACK(dsygst)(
    const blasint *itype, const char *uplo, const blasint *n,
    double *A, const blasint *ldA, const double *B, const blasint *ldB,
    blasint *info
) {
    RELAPACK_dsygst(itype, uplo, n, A, ldA, B, ldB, info);
}
#endif

#if INCLUDE_CHEGST
void LAPACK(chegst)(
    const blasint *itype, const char *uplo, const blasint *n,
    float *A, const blasint *ldA, const float *B, const blasint *ldB,
    blasint *info
) {
    RELAPACK_chegst(itype, uplo, n, A, ldA, B, ldB, info);
}
#endif

#if INCLUDE_ZHEGST
void LAPACK(zhegst)(
    const blasint *itype, const char *uplo, const blasint *n,
    double *A, const blasint *ldA, const double *B, const blasint *ldB,
    blasint *info
) {
    RELAPACK_zhegst(itype, uplo, n, A, ldA, B, ldB, info);
}
#endif


////////////
// XTRTRI //
////////////

#if INCLUDE_STRTRI
void LAPACK(strtri)(
    const char *uplo, const char *diag, const blasint *n,
    float *A, const blasint *ldA,
    blasint *info
) {
    RELAPACK_strtri(uplo, diag, n, A, ldA, info);
}
#endif

#if INCLUDE_DTRTRI
void LAPACK(dtrtri)(
    const char *uplo, const char *diag, const blasint *n,
    double *A, const blasint *ldA,
    blasint *info
) {
    RELAPACK_dtrtri(uplo, diag, n, A, ldA, info);
}
#endif

#if INCLUDE_CTRTRI
void LAPACK(ctrtri)(
    const char *uplo, const char *diag, const blasint *n,
    float *A, const blasint *ldA,
    blasint *info
) {
    RELAPACK_ctrtri(uplo, diag, n, A, ldA, info);
}
#endif

#if INCLUDE_ZTRTRI
void LAPACK(ztrtri)(
    const char *uplo, const char *diag, const blasint *n,
    double *A, const blasint *ldA,
    blasint *info
) {
    RELAPACK_ztrtri(uplo, diag, n, A, ldA, info);
}
#endif


////////////
// XPOTRF //
////////////

#if INCLUDE_SPOTRF
void LAPACK(spotrf)(
    const char *uplo, const blasint *n,
    float *A, const blasint *ldA,
    blasint *info
) {
    RELAPACK_spotrf(uplo, n, A, ldA, info);
}
#endif

#if INCLUDE_DPOTRF
void LAPACK(dpotrf)(
    const char *uplo, const blasint *n,
    double *A, const blasint *ldA,
    blasint *info
) {
    RELAPACK_dpotrf(uplo, n, A, ldA, info);
}
#endif

#if INCLUDE_CPOTRF
void LAPACK(cpotrf)(
    const char *uplo, const blasint *n,
    float *A, const blasint *ldA,
    blasint *info
) {
    RELAPACK_cpotrf(uplo, n, A, ldA, info);
}
#endif

#if INCLUDE_ZPOTRF
void LAPACK(zpotrf)(
    const char *uplo, const blasint *n,
    double *A, const blasint *ldA,
    blasint *info
) {
    RELAPACK_zpotrf(uplo, n, A, ldA, info);
}
#endif


////////////
// XPBTRF //
////////////

#if INCLUDE_SPBTRF
void LAPACK(spbtrf)(
    const char *uplo, const blasint *n, const blasint *kd,
    float *Ab, const blasint *ldAb,
    blasint *info
) {
    RELAPACK_spbtrf(uplo, n, kd, Ab, ldAb, info);
}
#endif

#if INCLUDE_DPBTRF
void LAPACK(dpbtrf)(
    const char *uplo, const blasint *n, const blasint *kd,
    double *Ab, const blasint *ldAb,
    blasint *info
) {
    RELAPACK_dpbtrf(uplo, n, kd, Ab, ldAb, info);
}
#endif

#if INCLUDE_CPBTRF
void LAPACK(cpbtrf)(
    const char *uplo, const blasint *n, const blasint *kd,
    float *Ab, const blasint *ldAb,
    blasint *info
) {
    RELAPACK_cpbtrf(uplo, n, kd, Ab, ldAb, info);
}
#endif

#if INCLUDE_ZPBTRF
void LAPACK(zpbtrf)(
    const char *uplo, const blasint *n, const blasint *kd,
    double *Ab, const blasint *ldAb,
    blasint *info
) {
    RELAPACK_zpbtrf(uplo, n, kd, Ab, ldAb, info);
}
#endif


////////////
// XSYTRF //
////////////

#if INCLUDE_SSYTRF
void LAPACK(ssytrf)(
    const char *uplo, const blasint *n,
    float *A, const blasint *ldA, blasint *ipiv,
    float *Work, const blasint *lWork, blasint *info
) {
    RELAPACK_ssytrf(uplo, n, A, ldA, ipiv, Work, lWork, info);
}
#endif

#if INCLUDE_DSYTRF
void LAPACK(dsytrf)(
    const char *uplo, const blasint *n,
    double *A, const blasint *ldA, blasint *ipiv,
    double *Work, const blasint *lWork, blasint *info
) {
    RELAPACK_dsytrf(uplo, n, A, ldA, ipiv, Work, lWork, info);
}
#endif

#if INCLUDE_CSYTRF
void LAPACK(csytrf)(
    const char *uplo, const blasint *n,
    float *A, const blasint *ldA, blasint *ipiv,
    float *Work, const blasint *lWork, blasint *info
) {
    RELAPACK_csytrf(uplo, n, A, ldA, ipiv, Work, lWork, info);
}
#endif

#if INCLUDE_ZSYTRF
void LAPACK(zsytrf)(
    const char *uplo, const blasint *n,
    double *A, const blasint *ldA, blasint *ipiv,
    double *Work, const blasint *lWork, blasint *info
) {
    RELAPACK_zsytrf(uplo, n, A, ldA, ipiv, Work, lWork, info);
}
#endif

#if INCLUDE_CHETRF
void LAPACK(chetrf)(
    const char *uplo, const blasint *n,
    float *A, const blasint *ldA, blasint *ipiv,
    float *Work, const blasint *lWork, blasint *info
) {
    RELAPACK_chetrf(uplo, n, A, ldA, ipiv, Work, lWork, info);
}
#endif

#if INCLUDE_ZHETRF
void LAPACK(zhetrf)(
    const char *uplo, const blasint *n,
    double *A, const blasint *ldA, blasint *ipiv,
    double *Work, const blasint *lWork, blasint *info
) {
    RELAPACK_zhetrf(uplo, n, A, ldA, ipiv, Work, lWork, info);
}
#endif

#if INCLUDE_SSYTRF_ROOK
void LAPACK(ssytrf_rook)(
    const char *uplo, const blasint *n,
    float *A, const blasint *ldA, blasint *ipiv,
    float *Work, const blasint *lWork, blasint *info
) {
    RELAPACK_ssytrf_rook(uplo, n, A, ldA, ipiv, Work, lWork, info);
}
#endif

#if INCLUDE_DSYTRF_ROOK
void LAPACK(dsytrf_rook)(
    const char *uplo, const blasint *n,
    double *A, const blasint *ldA, blasint *ipiv,
    double *Work, const blasint *lWork, blasint *info
) {
    RELAPACK_dsytrf_rook(uplo, n, A, ldA, ipiv, Work, lWork, info);
}
#endif

#if INCLUDE_CSYTRF_ROOK
void LAPACK(csytrf_rook)(
    const char *uplo, const blasint *n,
    float *A, const blasint *ldA, blasint *ipiv,
    float *Work, const blasint *lWork, blasint *info
) {
    RELAPACK_csytrf_rook(uplo, n, A, ldA, ipiv, Work, lWork, info);
}
#endif

#if INCLUDE_ZSYTRF_ROOK
void LAPACK(zsytrf_rook)(
    const char *uplo, const blasint *n,
    double *A, const blasint *ldA, blasint *ipiv,
    double *Work, const blasint *lWork, blasint *info
) {
    RELAPACK_zsytrf_rook(uplo, n, A, ldA, ipiv, Work, lWork, info);
}
#endif

#if INCLUDE_CHETRF_ROOK
void LAPACK(chetrf_rook)(
    const char *uplo, const blasint *n,
    float *A, const blasint *ldA, blasint *ipiv,
    float *Work, const blasint *lWork, blasint *info
) {
    RELAPACK_chetrf_rook(uplo, n, A, ldA, ipiv, Work, lWork, info);
}
#endif

#if INCLUDE_ZHETRF_ROOK
void LAPACK(zhetrf_rook)(
    const char *uplo, const blasint *n,
    double *A, const blasint *ldA, blasint *ipiv,
    double *Work, const blasint *lWork, blasint *info
) {
    RELAPACK_zhetrf_rook(uplo, n, A, ldA, ipiv, Work, lWork, info);
}
#endif


////////////
// XGETRF //
////////////

#if INCLUDE_SGETRF
void LAPACK(sgetrf)(
    const blasint *m, const blasint *n,
    float *A, const blasint *ldA, blasint *ipiv,
    blasint *info
) {
    RELAPACK_sgetrf(m, n, A, ldA, ipiv, info);
}
#endif

#if INCLUDE_DGETRF
void LAPACK(dgetrf)(
    const blasint *m, const blasint *n,
    double *A, const blasint *ldA, blasint *ipiv,
    blasint *info
) {
    RELAPACK_dgetrf(m, n, A, ldA, ipiv, info);
}
#endif

#if INCLUDE_CGETRF
void LAPACK(cgetrf)(
    const blasint *m, const blasint *n,
    float *A, const blasint *ldA, blasint *ipiv,
    blasint *info
) {
    RELAPACK_cgetrf(m, n, A, ldA, ipiv, info);
}
#endif

#if INCLUDE_ZGETRF
void LAPACK(zgetrf)(
    const blasint *m, const blasint *n,
    double *A, const blasint *ldA, blasint *ipiv,
    blasint *info
) {
    RELAPACK_zgetrf(m, n, A, ldA, ipiv, info);
}
#endif


////////////
// XGBTRF //
////////////

#if INCLUDE_SGBTRF
void LAPACK(sgbtrf)(
    const blasint *m, const blasint *n, const blasint *kl, const blasint *ku,
    float *Ab, const blasint *ldAb, blasint *ipiv,
    blasint *info
) {
    RELAPACK_sgbtrf(m, n, kl, ku, Ab, ldAb, ipiv, info);
}
#endif

#if INCLUDE_DGBTRF
void LAPACK(dgbtrf)(
    const blasint *m, const blasint *n, const blasint *kl, const blasint *ku,
    double *Ab, const blasint *ldAb, blasint *ipiv,
    blasint *info
) {
    RELAPACK_dgbtrf(m, n, kl, ku, Ab, ldAb, ipiv, info);
}
#endif

#if INCLUDE_CGBTRF
void LAPACK(cgbtrf)(
    const blasint *m, const blasint *n, const blasint *kl, const blasint *ku,
    float *Ab, const blasint *ldAb, blasint *ipiv,
    blasint *info
) {
    RELAPACK_cgbtrf(m, n, kl, ku, Ab, ldAb, ipiv, info);
}
#endif

#if INCLUDE_ZGBTRF
void LAPACK(zgbtrf)(
    const blasint *m, const blasint *n, const blasint *kl, const blasint *ku,
    double *Ab, const blasint *ldAb, blasint *ipiv,
    blasint *info
) {
    RELAPACK_zgbtrf(m, n, kl, ku, Ab, ldAb, ipiv, info);
}
#endif


////////////
// XTRSYL //
////////////

#if INCLUDE_STRSYL
void LAPACK(strsyl)(
    const char *tranA, const char *tranB, const blasint *isgn,
    const blasint *m, const blasint *n,
    const float *A, const blasint *ldA, const float *B, const blasint *ldB,
    float *C, const blasint *ldC, float *scale,
    blasint *info
) {
    RELAPACK_strsyl(tranA, tranB, isgn, m, n, A, ldA, B, ldB, C, ldC, scale, info);
}
#endif

#if INCLUDE_DTRSYL
void LAPACK(dtrsyl)(
    const char *tranA, const char *tranB, const blasint *isgn,
    const blasint *m, const blasint *n,
    const double *A, const blasint *ldA, const double *B, const blasint *ldB,
    double *C, const blasint *ldC, double *scale,
    blasint *info
) {
    RELAPACK_dtrsyl(tranA, tranB, isgn, m, n, A, ldA, B, ldB, C, ldC, scale, info);
}
#endif

#if INCLUDE_CTRSYL
void LAPACK(ctrsyl)(
    const char *tranA, const char *tranB, const blasint *isgn,
    const blasint *m, const blasint *n,
    const float *A, const blasint *ldA, const float *B, const blasint *ldB,
    float *C, const blasint *ldC, float *scale,
    blasint *info
) {
    RELAPACK_ctrsyl(tranA, tranB, isgn, m, n, A, ldA, B, ldB, C, ldC, scale, info);
}
#endif

#if INCLUDE_ZTRSYL
void LAPACK(ztrsyl)(
    const char *tranA, const char *tranB, const blasint *isgn,
    const blasint *m, const blasint *n,
    const double *A, const blasint *ldA, const double *B, const blasint *ldB,
    double *C, const blasint *ldC, double *scale,
    blasint *info
) {
    RELAPACK_ztrsyl(tranA, tranB, isgn, m, n, A, ldA, B, ldB, C, ldC, scale, info);
}
#endif


////////////
// XTGSYL //
////////////

#if INCLUDE_STGSYL
void LAPACK(stgsyl)(
    const char *trans, const blasint *ijob, const blasint *m, const blasint *n,
    const float *A, const blasint *ldA, const float *B, const blasint *ldB,
    float *C, const blasint *ldC,
    const float *D, const blasint *ldD, const float *E, const blasint *ldE,
    float *F, const blasint *ldF,
    float *scale, float *dif,
    float *Work, const blasint *lWork, blasint *iWork, blasint *info
) {
    RELAPACK_stgsyl(trans, ijob, m, n, A, ldA, B, ldB, C, ldC, D, ldD, E, ldE, F, ldF, scale, dif, Work, lWork, iWork, info);
}
#endif

#if INCLUDE_DTGSYL
void LAPACK(dtgsyl)(
    const char *trans, const blasint *ijob, const blasint *m, const blasint *n,
    const double *A, const blasint *ldA, const double *B, const blasint *ldB,
    double *C, const blasint *ldC,
    const double *D, const blasint *ldD, const double *E, const blasint *ldE,
    double *F, const blasint *ldF,
    double *scale, double *dif,
    double *Work, const blasint *lWork, blasint *iWork, blasint *info
) {
    RELAPACK_dtgsyl(trans, ijob, m, n, A, ldA, B, ldB, C, ldC, D, ldD, E, ldE, F, ldF, scale, dif, Work, lWork, iWork, info);
}
#endif

#if INCLUDE_CTGSYL
void LAPACK(ctgsyl)(
    const char *trans, const blasint *ijob, const blasint *m, const blasint *n,
    const float *A, const blasint *ldA, const float *B, const blasint *ldB,
    float *C, const blasint *ldC,
    const float *D, const blasint *ldD, const float *E, const blasint *ldE,
    float *F, const blasint *ldF,
    float *scale, float *dif,
    float *Work, const blasint *lWork, blasint *iWork, blasint *info
) {
    RELAPACK_ctgsyl(trans, ijob, m, n, A, ldA, B, ldB, C, ldC, D, ldD, E, ldE, F, ldF, scale, dif, Work, lWork, iWork, info);
}
#endif

#if INCLUDE_ZTGSYL
void LAPACK(ztgsyl)(
    const char *trans, const blasint *ijob, const blasint *m, const blasint *n,
    const double *A, const blasint *ldA, const double *B, const blasint *ldB,
    double *C, const blasint *ldC,
    const double *D, const blasint *ldD, const double *E, const blasint *ldE,
    double *F, const blasint *ldF,
    double *scale, double *dif,
    double *Work, const blasint *lWork, blasint *iWork, blasint *info
) {
    RELAPACK_ztgsyl(trans, ijob, m, n, A, ldA, B, ldB, C, ldC, D, ldD, E, ldE, F, ldF, scale, dif, Work, lWork, iWork, info);
}
#endif


////////////
// XGEMMT //
////////////

#if INCLUDE_SGEMMT
void LAPACK(sgemmt)(
    const char *uplo, const char *transA, const char *transB,
    const blasint *n, const blasint *k,
    const float *alpha, const float *A, const blasint *ldA,
    const float *B, const blasint *ldB,
    const float *beta, float *C, const blasint *ldC
) {
    RELAPACK_sgemmt(uplo, transA, transB, n, k, alpha, A, ldA, B, ldB, beta, C, ldC);
}
#endif

#if INCLUDE_DGEMMT
void LAPACK(dgemmt)(
    const char *uplo, const char *transA, const char *transB,
    const blasint *n, const blasint *k,
    const double *alpha, const double *A, const blasint *ldA,
    const double *B, const blasint *ldB,
    const double *beta, double *C, const blasint *ldC
) {
    RELAPACK_dgemmt(uplo, transA, transB, n, k, alpha, A, ldA, B, ldB, beta, C, ldC);
}
#endif

#if INCLUDE_CGEMMT
void LAPACK(cgemmt)(
    const char *uplo, const char *transA, const char *transB,
    const blasint *n, const blasint *k,
    const float *alpha, const float *A, const blasint *ldA,
    const float *B, const blasint *ldB,
    const float *beta, float *C, const blasint *ldC
) {
    RELAPACK_cgemmt(uplo, transA, transB, n, k, alpha, A, ldA, B, ldB, beta, C, ldC);
}
#endif

#if INCLUDE_ZGEMMT
void LAPACK(zgemmt)(
    const char *uplo, const char *transA, const char *transB,
    const blasint *n, const blasint *k,
    const double *alpha, const double *A, const blasint *ldA,
    const double *B, const blasint *ldB,
    const double *beta, double *C, const blasint *ldC
) {
    RELAPACK_zgemmt(uplo, transA, transB, n, k, alpha, A, ldA, B, ldB, beta, C, ldC);
}
#endif
