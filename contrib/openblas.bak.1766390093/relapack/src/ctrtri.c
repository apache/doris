#include "relapack.h"

static void RELAPACK_ctrtri_rec(const char *, const char *, const blasint *,
    float *, const blasint *, blasint *);


/** CTRTRI computes the inverse of a complex upper or lower triangular matrix A.
 *
 * This routine is functionally equivalent to LAPACK's ctrtri.
 * For details on its interface, see
 * http://www.netlib.org/lapack/explore-html/df/df8/ctrtri_8f.html
 * */
void RELAPACK_ctrtri(
    const char *uplo, const char *diag, const blasint *n,
    float *A, const blasint *ldA,
    blasint *info
) {

    // Check arguments
    const blasint lower = LAPACK(lsame)(uplo, "L");
    const blasint upper = LAPACK(lsame)(uplo, "U");
    const blasint nounit = LAPACK(lsame)(diag, "N");
    const blasint unit = LAPACK(lsame)(diag, "U");
    *info = 0;
    if (!lower && !upper)
        *info = -1;
    else if (!nounit && !unit)
        *info = -2;
    else if (*n < 0)
        *info = -3;
    else if (*ldA < MAX(1, *n))
        *info = -5;
    if (*info) {
        const blasint minfo = -*info;
        LAPACK(xerbla)("CTRTRI", &minfo, strlen("CTRTRI"));
        return;
    }

    if (*n == 0) return;

    // Clean char * arguments
    const char cleanuplo = lower  ? 'L' : 'U';
    const char cleandiag = nounit ? 'N' : 'U';

    // check for singularity
    if (nounit) {
        blasint i;
        for (i = 0; i < *n; i++)
            if (A[2 * (i + *ldA * i)] == 0 && A[2 * (i + *ldA * i) + 1] == 0) {
                *info = i;
                return;
            }
    }

    // Recursive kernel
    RELAPACK_ctrtri_rec(&cleanuplo, &cleandiag, n, A, ldA, info);
}


/** ctrtri's recursive compute kernel */
static void RELAPACK_ctrtri_rec(
    const char *uplo, const char *diag, const blasint *n,
    float *A, const blasint *ldA,
    blasint *info
){

    if (*n <= MAX(CROSSOVER_CTRTRI, 1)) {
        // Unblocked
        LAPACK(ctrti2)(uplo, diag, n, A, ldA, info);
        return;
    }

    // Constants
    const float ONE[]  = { 1., 0. };
    const float MONE[] = { -1., 0. };

    // Splitting
    const blasint n1 = CREC_SPLIT(*n);
    const blasint n2 = *n - n1;

    // A_TL A_TR
    // A_BL A_BR
    float *const A_TL = A;
    float *const A_TR = A + 2 * *ldA * n1;
    float *const A_BL = A                 + 2 * n1;
    float *const A_BR = A + 2 * *ldA * n1 + 2 * n1;

    // recursion(A_TL)
    RELAPACK_ctrtri_rec(uplo, diag, &n1, A_TL, ldA, info);
    if (*info)
        return;

    if (*uplo == 'L') {
        // A_BL = - A_BL * A_TL
        BLAS(ctrmm)("R", "L", "N", diag, &n2, &n1, MONE, A_TL, ldA, A_BL, ldA);
        // A_BL = A_BR \ A_BL
        BLAS(ctrsm)("L", "L", "N", diag, &n2, &n1, ONE, A_BR, ldA, A_BL, ldA);
    } else {
        // A_TR = - A_TL * A_TR
        BLAS(ctrmm)("L", "U", "N", diag, &n1, &n2, MONE, A_TL, ldA, A_TR, ldA);
        // A_TR = A_TR / A_BR
        BLAS(ctrsm)("R", "U", "N", diag, &n1, &n2, ONE, A_BR, ldA, A_TR, ldA);
    }

    // recursion(A_BR)
    RELAPACK_ctrtri_rec(uplo, diag, &n2, A_BR, ldA, info);
    if (*info)
        *info += n1;
}
