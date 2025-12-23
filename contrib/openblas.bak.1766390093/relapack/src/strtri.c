#include "relapack.h"

static void RELAPACK_strtri_rec(const char *, const char *, const blasint *,
    float *, const blasint *, blasint *);


/** CTRTRI computes the inverse of a real upper or lower triangular matrix A.
 *
 * This routine is functionally equivalent to LAPACK's strtri.
 * For details on its interface, see
 * http://www.netlib.org/lapack/explore-html/de/d76/strtri_8f.html
 * */
void RELAPACK_strtri(
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
        LAPACK(xerbla)("STRTRI", &minfo, strlen("STRTRI"));
        return;
    }

    // Clean char * arguments
    const char cleanuplo = lower  ? 'L' : 'U';
    const char cleandiag = nounit ? 'N' : 'U';

    // check for singularity
    if (nounit) {
        blasint i;
        for (i = 0; i < *n; i++)
            if (A[i + *ldA * i] == 0) {
                *info = i;
                return;
            }
    }

    // Recursive kernel
    RELAPACK_strtri_rec(&cleanuplo, &cleandiag, n, A, ldA, info);
}


/** strtri's recursive compute kernel */
static void RELAPACK_strtri_rec(
    const char *uplo, const char *diag, const blasint *n,
    float *A, const blasint *ldA,
    blasint *info
){

    if (*n <= MAX(CROSSOVER_STRTRI, 1)) {
        // Unblocked
        LAPACK(strti2)(uplo, diag, n, A, ldA, info);
        return;
    }

    // Constants
    const float ONE[]  = { 1. };
    const float MONE[] = { -1. };

    // Splitting
    const blasint n1 = SREC_SPLIT(*n);
    const blasint n2 = *n - n1;

    // A_TL A_TR
    // A_BL A_BR
    float *const A_TL = A;
    float *const A_TR = A + *ldA * n1;
    float *const A_BL = A             + n1;
    float *const A_BR = A + *ldA * n1 + n1;

    // recursion(A_TL)
    RELAPACK_strtri_rec(uplo, diag, &n1, A_TL, ldA, info);
    if (*info)
        return;

    if (*uplo == 'L') {
        // A_BL = - A_BL * A_TL
        BLAS(strmm)("R", "L", "N", diag, &n2, &n1, MONE, A_TL, ldA, A_BL, ldA);
        // A_BL = A_BR \ A_BL
        BLAS(strsm)("L", "L", "N", diag, &n2, &n1, ONE, A_BR, ldA, A_BL, ldA);
    } else {
        // A_TR = - A_TL * A_TR
        BLAS(strmm)("L", "U", "N", diag, &n1, &n2, MONE, A_TL, ldA, A_TR, ldA);
        // A_TR = A_TR / A_BR
        BLAS(strsm)("R", "U", "N", diag, &n1, &n2, ONE, A_BR, ldA, A_TR, ldA);
    }

    // recursion(A_BR)
    RELAPACK_strtri_rec(uplo, diag, &n2, A_BR, ldA, info);
    if (*info)
        *info += n1;
}
