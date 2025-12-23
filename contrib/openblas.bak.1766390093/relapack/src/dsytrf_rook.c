#include "relapack.h"
#if XSYTRF_ALLOW_MALLOC
#include <stdlib.h>
#endif

static void RELAPACK_dsytrf_rook_rec(const char *, const blasint *, const blasint *, blasint *,
    double *, const blasint *, blasint *, double *, const blasint *, blasint *);


/** DSYTRF_ROOK computes the factorization of a real symmetric matrix A using the bounded Bunch-Kaufman ("rook") diagonal pivoting method.
 *
 * This routine is functionally equivalent to LAPACK's dsytrf_rook.
 * For details on its interface, see
 * http://www.netlib.org/lapack/explore-html/db/df4/dsytrf__rook_8f.html
 * */
void RELAPACK_dsytrf_rook(
    const char *uplo, const blasint *n,
    double *A, const blasint *ldA, blasint *ipiv,
    double *Work, const blasint *lWork, blasint *info
) {

    // Required work size
    const blasint cleanlWork = *n * (*n / 2);
    blasint minlWork = cleanlWork;
#if XSYTRF_ALLOW_MALLOC
    minlWork = 1;
#endif

    // Check arguments
    const blasint lower = LAPACK(lsame)(uplo, "L");
    const blasint upper = LAPACK(lsame)(uplo, "U");
    *info = 0;
    if (!lower && !upper)
        *info = -1;
    else if (*n < 0)
        *info = -2;
    else if (*ldA < MAX(1, *n))
        *info = -4;
    else if ((*lWork <1 || *lWork < minlWork) && *lWork != -1)
        *info = -7;
    else if (*lWork == -1) {
        // Work size query
        *Work = cleanlWork;
        return;
    }

    // Ensure Work size
    double *cleanWork = Work;
#if XSYTRF_ALLOW_MALLOC
    if (!*info && *lWork < cleanlWork) {
        cleanWork = malloc(cleanlWork * sizeof(double));
        if (!cleanWork)
            *info = -7;
    }
#endif

    if (*info) {
        const blasint minfo = -*info;
        LAPACK(xerbla)("DSYTRF_ROOK", &minfo, strlen("DSYTRF_ROOK"));
        return;
    }

    // Clean char * arguments
    const char cleanuplo = lower ? 'L' : 'U';

    // Dummy argument
    blasint nout;

    // Recursive kernel
    RELAPACK_dsytrf_rook_rec(&cleanuplo, n, n, &nout, A, ldA, ipiv, cleanWork, n, info);

#if XSYTRF_ALLOW_MALLOC
    if (cleanWork != Work)
        free(cleanWork);
#endif
}


/** dsytrf_rook's recursive compute kernel */
static void RELAPACK_dsytrf_rook_rec(
    const char *uplo, const blasint *n_full, const blasint *n, blasint *n_out,
    double *A, const blasint *ldA, blasint *ipiv,
    double *Work, const blasint *ldWork, blasint *info
) {

    // top recursion level?
    const blasint top = *n_full == *n;

    if (*n <= MAX(CROSSOVER_DSYTRF_ROOK, 3)) {
        // Unblocked
        if (top) {
            LAPACK(dsytf2)(uplo, n, A, ldA, ipiv, info);
            *n_out = *n;
        } else
            RELAPACK_dsytrf_rook_rec2(uplo, n_full, n, n_out, A, ldA, ipiv, Work, ldWork, info);
        return;
    }

    blasint info1, info2;

    // Constants
    const double ONE[]  = { 1. };
    const double MONE[] = { -1. };
    const blasint    iONE[] = { 1 };

    const blasint n_rest = *n_full - *n;

    if (*uplo == 'L') {
        // Splitting (setup)
        blasint n1 = DREC_SPLIT(*n);
        blasint n2 = *n - n1;

        // Work_L *
        double *const Work_L = Work;

        // recursion(A_L)
        blasint n1_out;
        RELAPACK_dsytrf_rook_rec(uplo, n_full, &n1, &n1_out, A, ldA, ipiv, Work_L, ldWork, &info1);
        n1 = n1_out;

        // Splitting (continued)
        n2 = *n - n1;
        const blasint n_full2   = *n_full - n1;

        // *      *
        // A_BL   A_BR
        // A_BL_B A_BR_B
        double *const A_BL   = A             + n1;
        double *const A_BR   = A + *ldA * n1 + n1;
        double *const A_BL_B = A             + *n;
        double *const A_BR_B = A + *ldA * n1 + *n;

        // *        *
        // Work_BL Work_BR
        // *       *
        // (top recursion level: use Work as Work_BR)
        double *const Work_BL =              Work                + n1;
        double *const Work_BR = top ? Work : Work + *ldWork * n1 + n1;
        const blasint ldWork_BR = top ? n2 : *ldWork;

        // ipiv_T
        // ipiv_B
        blasint *const ipiv_B = ipiv + n1;

        // A_BR = A_BR - A_BL Work_BL'
        RELAPACK_dgemmt(uplo, "N", "T", &n2, &n1, MONE, A_BL, ldA, Work_BL, ldWork, ONE, A_BR, ldA);
        BLAS(dgemm)("N", "T", &n_rest, &n2, &n1, MONE, A_BL_B, ldA, Work_BL, ldWork, ONE, A_BR_B, ldA);

        // recursion(A_BR)
        blasint n2_out;
        RELAPACK_dsytrf_rook_rec(uplo, &n_full2, &n2, &n2_out, A_BR, ldA, ipiv_B, Work_BR, &ldWork_BR, &info2);

        if (n2_out != n2) {
            // undo 1 column of updates
            const blasint n_restp1 = n_rest + 1;

            // last column of A_BR
            double *const A_BR_r = A_BR + *ldA * n2_out + n2_out;

            // last row of A_BL
            double *const A_BL_b = A_BL + n2_out;

            // last row of Work_BL
            double *const Work_BL_b = Work_BL + n2_out;

            // A_BR_r = A_BR_r + A_BL_b Work_BL_b'
            BLAS(dgemv)("N", &n_restp1, &n1, ONE, A_BL_b, ldA, Work_BL_b, ldWork, ONE, A_BR_r, iONE);
        }
        n2 = n2_out;

        // shift pivots
        blasint i;
        for (i = 0; i < n2; i++)
            if (ipiv_B[i] > 0)
                ipiv_B[i] += n1;
            else
                ipiv_B[i] -= n1;

        *info  = info1 || info2;
        *n_out = n1 + n2;
    } else {
        // Splitting (setup)
        blasint n2 = DREC_SPLIT(*n);
        blasint n1 = *n - n2;

        // * Work_R
        // (top recursion level: use Work as Work_R)
        double *const Work_R = top ? Work : Work + *ldWork * n1;

        // recursion(A_R)
        blasint n2_out;
        RELAPACK_dsytrf_rook_rec(uplo, n_full, &n2, &n2_out, A, ldA, ipiv, Work_R, ldWork, &info2);
        const blasint n2_diff = n2 - n2_out;
        n2 = n2_out;

        // Splitting (continued)
        n1 = *n - n2;
        const blasint n_full1 = *n_full - n2;

        // * A_TL_T A_TR_T
        // * A_TL   A_TR
        // * *      *
        double *const A_TL_T = A + *ldA * n_rest;
        double *const A_TR_T = A + *ldA * (n_rest + n1);
        double *const A_TL   = A + *ldA * n_rest        + n_rest;
        double *const A_TR   = A + *ldA * (n_rest + n1) + n_rest;

        // Work_L *
        // *      Work_TR
        // *      *
        // (top recursion level: Work_R was Work)
        double *const Work_L  = Work;
        double *const Work_TR = Work + *ldWork * (top ? n2_diff : n1) + n_rest;
        const blasint ldWork_L = top ? n1 : *ldWork;

        // A_TL = A_TL - A_TR Work_TR'
        RELAPACK_dgemmt(uplo, "N", "T", &n1, &n2, MONE, A_TR, ldA, Work_TR, ldWork, ONE, A_TL, ldA);
        BLAS(dgemm)("N", "T", &n_rest, &n1, &n2, MONE, A_TR_T, ldA, Work_TR, ldWork, ONE, A_TL_T, ldA);

        // recursion(A_TL)
        blasint n1_out;
        RELAPACK_dsytrf_rook_rec(uplo, &n_full1, &n1, &n1_out, A, ldA, ipiv, Work_L, &ldWork_L, &info1);

        if (n1_out != n1) {
            // undo 1 column of updates
            const blasint n_restp1 = n_rest + 1;

            // A_TL_T_l = A_TL_T_l + A_TR_T Work_TR_t'
            BLAS(dgemv)("N", &n_restp1, &n2, ONE, A_TR_T, ldA, Work_TR, ldWork, ONE, A_TL_T, iONE);
        }
        n1 = n1_out;

        *info  = info2 || info1;
        *n_out = n1 + n2;
    }
}
