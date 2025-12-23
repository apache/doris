#include "relapack.h"

static void RELAPACK_dgemmt_rec(const char *, const char *, const char *,
    const blasint *, const blasint *, const double *, const double *, const blasint *,
    const double *, const blasint *, const double *, double *, const blasint *);

static void RELAPACK_dgemmt_rec2(const char *, const char *, const char *,
    const blasint *, const blasint *, const double *, const double *, const blasint *,
    const double *, const blasint *, const double *, double *, const blasint *);


/** DGEMMT computes a matrix-matrix product with general matrices but updates
 * only the upper or lower triangular part of the result matrix.
 *
 * This routine performs the same operation as the BLAS routine
 * dgemm(transA, transB, n, n, k, alpha, A, ldA, B, ldB, beta, C, ldC)
 * but only updates the triangular part of C specified by uplo:
 * If (*uplo == 'L'), only the lower triangular part of C is updated,
 * otherwise the upper triangular part is updated.
 * */
void RELAPACK_dgemmt(
    const char *uplo, const char *transA, const char *transB,
    const blasint *n, const blasint *k,
    const double *alpha, const double *A, const blasint *ldA,
    const double *B, const blasint *ldB,
    const double *beta, double *C, const blasint *ldC
) {

#if HAVE_XGEMMT
    BLAS(dgemmt)(uplo, transA, transB, n, k, alpha, A, ldA, B, ldB, beta, C, ldC);
    return;
#else

    // Check arguments
    const blasint lower = LAPACK(lsame)(uplo, "L");
    const blasint upper = LAPACK(lsame)(uplo, "U");
    const blasint notransA = LAPACK(lsame)(transA, "N");
    const blasint tranA = LAPACK(lsame)(transA, "T");
    const blasint notransB = LAPACK(lsame)(transB, "N");
    const blasint tranB = LAPACK(lsame)(transB, "T");
    blasint info = 0;
    if (!lower && !upper)
        info = 1;
    else if (!tranA && !notransA)
        info = 2;
    else if (!tranB && !notransB)
        info = 3;
    else if (*n < 0)
        info = 4;
    else if (*k < 0)
        info = 5;
    else if (*ldA < MAX(1, notransA ? *n : *k))
        info = 8;
    else if (*ldB < MAX(1, notransB ? *k : *n))
        info = 10;
    else if (*ldC < MAX(1, *n))
        info = 13;
    if (info) {
        LAPACK(xerbla)("DGEMMT", &info, strlen("DGEMMT"));
        return;
    }

    // Clean char * arguments
    const char cleanuplo = lower ? 'L' : 'U';
    const char cleantransA = notransA ? 'N' : 'T';
    const char cleantransB = notransB ? 'N' : 'T';

    // Recursive kernel
    RELAPACK_dgemmt_rec(&cleanuplo, &cleantransA, &cleantransB, n, k, alpha, A, ldA, B, ldB, beta, C, ldC);
#endif
}


/** dgemmt's recursive compute kernel */
static void RELAPACK_dgemmt_rec(
    const char *uplo, const char *transA, const char *transB,
    const blasint *n, const blasint *k,
    const double *alpha, const double *A, const blasint *ldA,
    const double *B, const blasint *ldB,
    const double *beta, double *C, const blasint *ldC
) {

    if (*n <= MAX(CROSSOVER_DGEMMT, 1)) {
        // Unblocked
        RELAPACK_dgemmt_rec2(uplo, transA, transB, n, k, alpha, A, ldA, B, ldB, beta, C, ldC);
        return;
    }

    // Splitting
    const blasint n1 = DREC_SPLIT(*n);
    const blasint n2 = *n - n1;

    // A_T
    // A_B
    const double *const A_T = A;
    const double *const A_B = A + ((*transA == 'N') ? n1 : *ldA * n1);

    // B_L B_R
    const double *const B_L = B;
    const double *const B_R = B + ((*transB == 'N') ? *ldB * n1 : n1);

    // C_TL C_TR
    // C_BL C_BR
    double *const C_TL = C;
    double *const C_TR = C + *ldC * n1;
    double *const C_BL = C             + n1;
    double *const C_BR = C + *ldC * n1 + n1;

    // recursion(C_TL)
    RELAPACK_dgemmt_rec(uplo, transA, transB, &n1, k, alpha, A_T, ldA, B_L, ldB, beta, C_TL, ldC);

    if (*uplo == 'L')
        // C_BL = alpha A_B B_L + beta C_BL
        BLAS(dgemm)(transA, transB, &n2, &n1, k, alpha, A_B, ldA, B_L, ldB, beta, C_BL, ldC);
    else
        // C_TR = alpha A_T B_R + beta C_TR
        BLAS(dgemm)(transA, transB, &n1, &n2, k, alpha, A_T, ldA, B_R, ldB, beta, C_TR, ldC);

    // recursion(C_BR)
    RELAPACK_dgemmt_rec(uplo, transA, transB, &n2, k, alpha, A_B, ldA, B_R, ldB, beta, C_BR, ldC);
}


/** dgemmt's unblocked compute kernel */
static void RELAPACK_dgemmt_rec2(
    const char *uplo, const char *transA, const char *transB,
    const blasint *n, const blasint *k,
    const double *alpha, const double *A, const blasint *ldA,
    const double *B, const blasint *ldB,
    const double *beta, double *C, const blasint *ldC
) {

    const blasint incB = (*transB == 'N') ? 1 : *ldB;
    const blasint incC = 1;

    blasint i;
    for (i = 0; i < *n; i++) {
        // A_0
        // A_i
        const double *const A_0 = A;
        const double *const A_i = A + ((*transA == 'N') ? i : *ldA * i);

        // * B_i *
        const double *const B_i = B + ((*transB == 'N') ? *ldB * i : i);

        // * C_0i *
        // * C_ii *
        double *const C_0i = C + *ldC * i;
        double *const C_ii = C + *ldC * i + i;

        if (*uplo == 'L') {
            const blasint nmi = *n - i;
            if (*transA == 'N')
                BLAS(dgemv)(transA, &nmi, k, alpha, A_i, ldA, B_i, &incB, beta, C_ii, &incC);
            else
                BLAS(dgemv)(transA, k, &nmi, alpha, A_i, ldA, B_i, &incB, beta, C_ii, &incC);
        } else {
            const blasint ip1 = i + 1;
            if (*transA == 'N')
                BLAS(dgemv)(transA, &ip1, k, alpha, A_0, ldA, B_i, &incB, beta, C_0i, &incC);
            else
                BLAS(dgemv)(transA, k, &ip1, alpha, A_0, ldA, B_i, &incB, beta, C_0i, &incC);
        }
    }
}
