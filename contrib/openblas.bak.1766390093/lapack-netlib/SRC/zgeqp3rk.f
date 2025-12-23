*> \brief \b ZGEQP3RK computes a truncated Householder QR factorization with column pivoting of a complex m-by-n matrix A by using Level 3 BLAS and overwrites m-by-nrhs matrix B with Q**H * B.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZGEQP3RK + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zgeqp3rk.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zgeqp3rk.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zgeqp3rk.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZGEQP3RK( M, N, NRHS, KMAX, ABSTOL, RELTOL, A, LDA,
*      $                     K, MAXC2NRMK, RELMAXC2NRMK, JPIV, TAU,
*      $                     WORK, LWORK, RWORK, IWORK, INFO )
*       IMPLICIT NONE
*
*      .. Scalar Arguments ..
*       INTEGER            INFO, K, KMAX, LDA, LWORK, M, N, NRHS
*       DOUBLE PRECISION   ABSTOL, MAXC2NRMK, RELMAXC2NRMK, RELTOL
*      ..
*      .. Array Arguments ..
*       INTEGER            IWORK( * ), JPIV( * )
*       DOUBLE PRECISION   RWORK( * )
*       COMPLEX*16         A( LDA, * ), TAU( * ), WORK( * )
*      ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZGEQP3RK performs two tasks simultaneously:
*>
*> Task 1: The routine computes a truncated (rank K) or full rank
*> Householder QR factorization with column pivoting of a complex
*> M-by-N matrix A using Level 3 BLAS. K is the number of columns
*> that were factorized, i.e. factorization rank of the
*> factor R, K <= min(M,N).
*>
*>  A * P(K) = Q(K) * R(K)  =
*>
*>        = Q(K) * ( R11(K) R12(K) ) = Q(K) * (   R(K)_approx    )
*>                 ( 0      R22(K) )          ( 0  R(K)_residual ),
*>
*> where:
*>
*>  P(K)            is an N-by-N permutation matrix;
*>  Q(K)            is an M-by-M unitary matrix;
*>  R(K)_approx   = ( R11(K), R12(K) ) is a rank K approximation of the
*>                    full rank factor R with K-by-K upper-triangular
*>                    R11(K) and K-by-N rectangular R12(K). The diagonal
*>                    entries of R11(K) appear in non-increasing order
*>                    of absolute value, and absolute values of all of
*>                    them exceed the maximum column 2-norm of R22(K)
*>                    up to roundoff error.
*>  R(K)_residual = R22(K) is the residual of a rank K approximation
*>                    of the full rank factor R. It is a
*>                    an (M-K)-by-(N-K) rectangular matrix;
*>  0               is a an (M-K)-by-K zero matrix.
*>
*> Task 2: At the same time, the routine overwrites a complex M-by-NRHS
*> matrix B with  Q(K)**H * B  using Level 3 BLAS.
*>
*> =====================================================================
*>
*> The matrices A and B are stored on input in the array A as
*> the left and right blocks A(1:M,1:N) and A(1:M, N+1:N+NRHS)
*> respectively.
*>
*>                                  N     NRHS
*>             array_A   =   M  [ mat_A, mat_B ]
*>
*> The truncation criteria (i.e. when to stop the factorization)
*> can be any of the following:
*>
*>   1) The input parameter KMAX, the maximum number of columns
*>      KMAX to factorize, i.e. the factorization rank is limited
*>      to KMAX. If KMAX >= min(M,N), the criterion is not used.
*>
*>   2) The input parameter ABSTOL, the absolute tolerance for
*>      the maximum column 2-norm of the residual matrix R22(K). This
*>      means that the factorization stops if this norm is less or
*>      equal to ABSTOL. If ABSTOL < 0.0, the criterion is not used.
*>
*>   3) The input parameter RELTOL, the tolerance for the maximum
*>      column 2-norm matrix of the residual matrix R22(K) divided
*>      by the maximum column 2-norm of the original matrix A, which
*>      is equal to abs(R(1,1)). This means that the factorization stops
*>      when the ratio of the maximum column 2-norm of R22(K) to
*>      the maximum column 2-norm of A is less than or equal to RELTOL.
*>      If RELTOL < 0.0, the criterion is not used.
*>
*>   4) In case both stopping criteria ABSTOL or RELTOL are not used,
*>      and when the residual matrix R22(K) is a zero matrix in some
*>      factorization step K. ( This stopping criterion is implicit. )
*>
*>  The algorithm stops when any of these conditions is first
*>  satisfied, otherwise the whole matrix A is factorized.
*>
*>  To factorize the whole matrix A, use the values
*>  KMAX >= min(M,N), ABSTOL < 0.0 and RELTOL < 0.0.
*>
*>  The routine returns:
*>     a) Q(K), R(K)_approx = ( R11(K), R12(K) ),
*>        R(K)_residual = R22(K), P(K), i.e. the resulting matrices
*>        of the factorization; P(K) is represented by JPIV,
*>        ( if K = min(M,N), R(K)_approx is the full factor R,
*>        and there is no residual matrix R(K)_residual);
*>     b) K, the number of columns that were factorized,
*>        i.e. factorization rank;
*>     c) MAXC2NRMK, the maximum column 2-norm of the residual
*>        matrix R(K)_residual = R22(K),
*>        ( if K = min(M,N), MAXC2NRMK = 0.0 );
*>     d) RELMAXC2NRMK equals MAXC2NRMK divided by MAXC2NRM, the maximum
*>        column 2-norm of the original matrix A, which is equal
*>        to abs(R(1,1)), ( if K = min(M,N), RELMAXC2NRMK = 0.0 );
*>     e) Q(K)**H * B, the matrix B with the unitary
*>        transformation Q(K)**H applied on the left.
*>
*> The N-by-N permutation matrix P(K) is stored in a compact form in
*> the integer array JPIV. For 1 <= j <= N, column j
*> of the matrix A was interchanged with column JPIV(j).
*>
*> The M-by-M unitary matrix Q is represented as a product
*> of elementary Householder reflectors
*>
*>     Q(K) = H(1) *  H(2) * . . . * H(K),
*>
*> where K is the number of columns that were factorized.
*>
*> Each H(j) has the form
*>
*>     H(j) = I - tau * v * v**H,
*>
*> where 1 <= j <= K and
*>   I    is an M-by-M identity matrix,
*>   tau  is a complex scalar,
*>   v    is a complex vector with v(1:j-1) = 0 and v(j) = 1.
*>
*> v(j+1:M) is stored on exit in A(j+1:M,j) and tau in TAU(j).
*>
*> See the Further Details section for more information.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the matrix A. M >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrix A. N >= 0.
*> \endverbatim
*>
*> \param[in] NRHS
*> \verbatim
*>          NRHS is INTEGER
*>          The number of right hand sides, i.e. the number of
*>          columns of the matrix B. NRHS >= 0.
*> \endverbatim
*>
*> \param[in] KMAX
*> \verbatim
*>          KMAX is INTEGER
*>
*>          The first factorization stopping criterion. KMAX >= 0.
*>
*>          The maximum number of columns of the matrix A to factorize,
*>          i.e. the maximum factorization rank.
*>
*>          a) If KMAX >= min(M,N), then this stopping criterion
*>                is not used, the routine factorizes columns
*>                depending on ABSTOL and RELTOL.
*>
*>          b) If KMAX = 0, then this stopping criterion is
*>                satisfied on input and the routine exits immediately.
*>                This means that the factorization is not performed,
*>                the matrices A and B are not modified, and
*>                the matrix A is itself the residual.
*> \endverbatim
*>
*> \param[in] ABSTOL
*> \verbatim
*>          ABSTOL is DOUBLE PRECISION
*>
*>          The second factorization stopping criterion, cannot be NaN.
*>
*>          The absolute tolerance (stopping threshold) for
*>          maximum column 2-norm of the residual matrix R22(K).
*>          The algorithm converges (stops the factorization) when
*>          the maximum column 2-norm of the residual matrix R22(K)
*>          is less than or equal to ABSTOL. Let SAFMIN = DLAMCH('S').
*>
*>          a) If ABSTOL is NaN, then no computation is performed
*>                and an error message ( INFO = -5 ) is issued
*>                by XERBLA.
*>
*>          b) If ABSTOL < 0.0, then this stopping criterion is not
*>                used, the routine factorizes columns depending
*>                on KMAX and RELTOL.
*>                This includes the case ABSTOL = -Inf.
*>
*>          c) If 0.0 <= ABSTOL < 2*SAFMIN, then ABSTOL = 2*SAFMIN
*>                is used. This includes the case ABSTOL = -0.0.
*>
*>          d) If 2*SAFMIN <= ABSTOL then the input value
*>                of ABSTOL is used.
*>
*>          Let MAXC2NRM be the maximum column 2-norm of the
*>          whole original matrix A.
*>          If ABSTOL chosen above is >= MAXC2NRM, then this
*>          stopping criterion is satisfied on input and routine exits
*>          immediately after MAXC2NRM is computed. The routine
*>          returns MAXC2NRM in MAXC2NORMK,
*>          and 1.0 in RELMAXC2NORMK.
*>          This includes the case ABSTOL = +Inf. This means that the
*>          factorization is not performed, the matrices A and B are not
*>          modified, and the matrix A is itself the residual.
*> \endverbatim
*>
*> \param[in] RELTOL
*> \verbatim
*>          RELTOL is DOUBLE PRECISION
*>
*>          The third factorization stopping criterion, cannot be NaN.
*>
*>          The tolerance (stopping threshold) for the ratio
*>          abs(R(K+1,K+1))/abs(R(1,1)) of the maximum column 2-norm of
*>          the residual matrix R22(K) to the maximum column 2-norm of
*>          the original matrix A. The algorithm converges (stops the
*>          factorization), when abs(R(K+1,K+1))/abs(R(1,1)) A is less
*>          than or equal to RELTOL. Let EPS = DLAMCH('E').
*>
*>          a) If RELTOL is NaN, then no computation is performed
*>                and an error message ( INFO = -6 ) is issued
*>                by XERBLA.
*>
*>          b) If RELTOL < 0.0, then this stopping criterion is not
*>                used, the routine factorizes columns depending
*>                on KMAX and ABSTOL.
*>                This includes the case RELTOL = -Inf.
*>
*>          c) If 0.0 <= RELTOL < EPS, then RELTOL = EPS is used.
*>                This includes the case RELTOL = -0.0.
*>
*>          d) If EPS <= RELTOL then the input value of RELTOL
*>                is used.
*>
*>          Let MAXC2NRM be the maximum column 2-norm of the
*>          whole original matrix A.
*>          If RELTOL chosen above is >= 1.0, then this stopping
*>          criterion is satisfied on input and routine exits
*>          immediately after MAXC2NRM is computed.
*>          The routine returns MAXC2NRM in MAXC2NORMK,
*>          and 1.0 in RELMAXC2NORMK.
*>          This includes the case RELTOL = +Inf. This means that the
*>          factorization is not performed, the matrices A and B are not
*>          modified, and the matrix A is itself the residual.
*>
*>          NOTE: We recommend that RELTOL satisfy
*>                min( 10*max(M,N)*EPS, sqrt(EPS) ) <= RELTOL
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA,N+NRHS)
*>
*>          On entry:
*>
*>          a) The subarray A(1:M,1:N) contains the M-by-N matrix A.
*>          b) The subarray A(1:M,N+1:N+NRHS) contains the M-by-NRHS
*>             matrix B.
*>
*>                                  N     NRHS
*>              array_A   =   M  [ mat_A, mat_B ]
*>
*>          On exit:
*>
*>          a) The subarray A(1:M,1:N) contains parts of the factors
*>             of the matrix A:
*>
*>            1) If K = 0, A(1:M,1:N) contains the original matrix A.
*>            2) If K > 0, A(1:M,1:N) contains parts of the
*>            factors:
*>
*>              1. The elements below the diagonal of the subarray
*>                 A(1:M,1:K) together with TAU(1:K) represent the
*>                 unitary matrix Q(K) as a product of K Householder
*>                 elementary reflectors.
*>
*>              2. The elements on and above the diagonal of
*>                 the subarray A(1:K,1:N) contain K-by-N
*>                 upper-trapezoidal matrix
*>                 R(K)_approx = ( R11(K), R12(K) ).
*>                 NOTE: If K=min(M,N), i.e. full rank factorization,
*>                       then R_approx(K) is the full factor R which
*>                       is upper-trapezoidal. If, in addition, M>=N,
*>                       then R is upper-triangular.
*>
*>              3. The subarray A(K+1:M,K+1:N) contains (M-K)-by-(N-K)
*>                 rectangular matrix R(K)_residual = R22(K).
*>
*>          b) If NRHS > 0, the subarray A(1:M,N+1:N+NRHS) contains
*>             the M-by-NRHS product Q(K)**H * B.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A. LDA >= max(1,M).
*>          This is the leading dimension for both matrices, A and B.
*> \endverbatim
*>
*> \param[out] K
*> \verbatim
*>          K is INTEGER
*>          Factorization rank of the matrix A, i.e. the rank of
*>          the factor R, which is the same as the number of non-zero
*>          rows of the factor R. 0 <= K <= min(M,KMAX,N).
*>
*>          K also represents the number of non-zero Householder
*>          vectors.
*>
*>          NOTE: If K = 0, a) the arrays A and B are not modified;
*>                          b) the array TAU(1:min(M,N)) is set to ZERO,
*>                             if the matrix A does not contain NaN,
*>                             otherwise the elements TAU(1:min(M,N))
*>                             are undefined;
*>                          c) the elements of the array JPIV are set
*>                             as follows: for j = 1:N, JPIV(j) = j.
*> \endverbatim
*>
*> \param[out] MAXC2NRMK
*> \verbatim
*>          MAXC2NRMK is DOUBLE PRECISION
*>          The maximum column 2-norm of the residual matrix R22(K),
*>          when the factorization stopped at rank K. MAXC2NRMK >= 0.
*>
*>          a) If K = 0, i.e. the factorization was not performed,
*>             the matrix A was not modified and is itself a residual
*>             matrix, then MAXC2NRMK equals the maximum column 2-norm
*>             of the original matrix A.
*>
*>          b) If 0 < K < min(M,N), then MAXC2NRMK is returned.
*>
*>          c) If K = min(M,N), i.e. the whole matrix A was
*>             factorized and there is no residual matrix,
*>             then MAXC2NRMK = 0.0.
*>
*>          NOTE: MAXC2NRMK in the factorization step K would equal
*>                R(K+1,K+1) in the next factorization step K+1.
*> \endverbatim
*>
*> \param[out] RELMAXC2NRMK
*> \verbatim
*>          RELMAXC2NRMK is DOUBLE PRECISION
*>          The ratio MAXC2NRMK / MAXC2NRM of the maximum column
*>          2-norm of the residual matrix R22(K) (when the factorization
*>          stopped at rank K) to the maximum column 2-norm of the
*>          whole original matrix A. RELMAXC2NRMK >= 0.
*>
*>          a) If K = 0, i.e. the factorization was not performed,
*>             the matrix A was not modified and is itself a residual
*>             matrix, then RELMAXC2NRMK = 1.0.
*>
*>          b) If 0 < K < min(M,N), then
*>                RELMAXC2NRMK = MAXC2NRMK / MAXC2NRM is returned.
*>
*>          c) If K = min(M,N), i.e. the whole matrix A was
*>             factorized and there is no residual matrix,
*>             then RELMAXC2NRMK = 0.0.
*>
*>         NOTE: RELMAXC2NRMK in the factorization step K would equal
*>               abs(R(K+1,K+1))/abs(R(1,1)) in the next factorization
*>               step K+1.
*> \endverbatim
*>
*> \param[out] JPIV
*> \verbatim
*>          JPIV is INTEGER array, dimension (N)
*>          Column pivot indices. For 1 <= j <= N, column j
*>          of the matrix A was interchanged with column JPIV(j).
*>
*>          The elements of the array JPIV(1:N) are always set
*>          by the routine, for example, even  when no columns
*>          were factorized, i.e. when K = 0, the elements are
*>          set as JPIV(j) = j for j = 1:N.
*> \endverbatim
*>
*> \param[out] TAU
*> \verbatim
*>          TAU is COMPLEX*16 array, dimension (min(M,N))
*>          The scalar factors of the elementary reflectors.
*>
*>          If 0 < K <= min(M,N), only the elements TAU(1:K) of
*>          the array TAU are modified by the factorization.
*>          After the factorization computed, if no NaN was found
*>          during the factorization, the remaining elements
*>          TAU(K+1:min(M,N)) are set to zero, otherwise the
*>          elements TAU(K+1:min(M,N)) are not set and therefore
*>          undefined.
*>          ( If K = 0, all elements of TAU are set to zero, if
*>          the matrix A does not contain NaN. )
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension (MAX(1,LWORK))
*>          On exit, if INFO = 0, WORK(1) returns the optimal LWORK.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the array WORK.
*>          LWORK >= 1, if MIN(M,N) = 0, and
*>          LWORK >= N+NRHS-1, otherwise.
*>          For optimal performance LWORK >= NB*( N+NRHS+1 ),
*>          where NB is the optimal block size for ZGEQP3RK returned
*>          by ILAENV. Minimal block size MINNB=2.
*>
*>          NOTE: The decision, whether to use unblocked BLAS 2
*>          or blocked BLAS 3 code is based not only on the dimension
*>          LWORK of the availbale workspace WORK, but also also on the
*>          matrix A dimension N via crossover point NX returned
*>          by ILAENV. (For N less than NX, unblocked code should be
*>          used.)
*>
*>          If LWORK = -1, then a workspace query is assumed;
*>          the routine only calculates the optimal size of the WORK
*>          array, returns this value as the first entry of the WORK
*>          array, and no error message related to LWORK is issued
*>          by XERBLA.
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array, dimension (2*N)
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (N-1).
*>          Is a work array. ( IWORK is used to store indices
*>          of "bad" columns for norm downdating in the residual
*>          matrix in the blocked step auxiliary subroutine ZLAQP3RK ).
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          1) INFO = 0: successful exit.
*>          2) INFO < 0: if INFO = -i, the i-th argument had an
*>                       illegal value.
*>          3) If INFO = j_1, where 1 <= j_1 <= N, then NaN was
*>             detected and the routine stops the computation.
*>             The j_1-th column of the matrix A or the j_1-th
*>             element of array TAU contains the first occurrence
*>             of NaN in the factorization step K+1 ( when K columns
*>             have been factorized ).
*>
*>             On exit:
*>             K                  is set to the number of
*>                                   factorized columns without
*>                                   exception.
*>             MAXC2NRMK          is set to NaN.
*>             RELMAXC2NRMK       is set to NaN.
*>             TAU(K+1:min(M,N))  is not set and contains undefined
*>                                   elements. If j_1=K+1, TAU(K+1)
*>                                   may contain NaN.
*>          4) If INFO = j_2, where N+1 <= j_2 <= 2*N, then no NaN
*>             was detected, but +Inf (or -Inf) was detected and
*>             the routine continues the computation until completion.
*>             The (j_2-N)-th column of the matrix A contains the first
*>             occurrence of +Inf (or -Inf) in the factorization
*>             step K+1 ( when K columns have been factorized ).
*> \endverbatim
*
*  Authors:
*  ========
*
*> \author Univ. of Tennessee
*> \author Univ. of California Berkeley
*> \author Univ. of Colorado Denver
*> \author NAG Ltd.
*
*> \ingroup geqp3rk
*
*> \par Further Details:
*  =====================
*
*> \verbatim
*> ZGEQP3RK is based on the same BLAS3 Householder QR factorization
*> algorithm with column pivoting as in ZGEQP3 routine which uses
*> ZLARFG routine to generate Householder reflectors
*> for QR factorization.
*>
*> We can also write:
*>
*>   A = A_approx(K) + A_residual(K)
*>
*> The low rank approximation matrix A(K)_approx from
*> the truncated QR factorization of rank K of the matrix A is:
*>
*>   A(K)_approx = Q(K) * ( R(K)_approx ) * P(K)**T
*>                        (     0     0 )
*>
*>               = Q(K) * ( R11(K) R12(K) ) * P(K)**T
*>                        (      0      0 )
*>
*> The residual A_residual(K) of the matrix A is:
*>
*>   A_residual(K) = Q(K) * ( 0              0 ) * P(K)**T =
*>                          ( 0  R(K)_residual )
*>
*>                 = Q(K) * ( 0        0 ) * P(K)**T
*>                          ( 0   R22(K) )
*>
*> The truncated (rank K) factorization guarantees that
*> the maximum column 2-norm of A_residual(K) is less than
*> or equal to MAXC2NRMK up to roundoff error.
*>
*> NOTE: An approximation of the null vectors
*>       of A can be easily computed from R11(K)
*>       and R12(K):
*>
*>       Null( A(K) )_approx = P * ( inv(R11(K)) * R12(K) )
*>                                 (         -I           )
*>
*> \endverbatim
*
*> \par References:
*  ================
*> [1] A Level 3 BLAS QR factorization algorithm with column pivoting developed in 1996.
*> G. Quintana-Orti, Depto. de Informatica, Universidad Jaime I, Spain.
*> X. Sun, Computer Science Dept., Duke University, USA.
*> C. H. Bischof, Math. and Comp. Sci. Div., Argonne National Lab, USA.
*> A BLAS-3 version of the QR factorization with column pivoting.
*> LAPACK Working Note 114
*> \htmlonly
*> <a href="https://www.netlib.org/lapack/lawnspdf/lawn114.pdf">https://www.netlib.org/lapack/lawnspdf/lawn114.pdf</a>
*> \endhtmlonly
*> and in
*> SIAM J. Sci. Comput., 19(5):1486-1494, Sept. 1998.
*> \htmlonly
*> <a href="https://doi.org/10.1137/S1064827595296732">https://doi.org/10.1137/S1064827595296732</a>
*> \endhtmlonly
*>
*> [2] A partial column norm updating strategy developed in 2006.
*> Z. Drmac and Z. Bujanovic, Dept. of Math., University of Zagreb, Croatia.
*> On the failure of rank revealing QR factorization software – a case study.
*> LAPACK Working Note 176.
*> \htmlonly
*> <a href="http://www.netlib.org/lapack/lawnspdf/lawn176.pdf">http://www.netlib.org/lapack/lawnspdf/lawn176.pdf</a>
*> \endhtmlonly
*> and in
*> ACM Trans. Math. Softw. 35, 2, Article 12 (July 2008), 28 pages.
*> \htmlonly
*> <a href="https://doi.org/10.1145/1377612.1377616">https://doi.org/10.1145/1377612.1377616</a>
*> \endhtmlonly
*
*> \par Contributors:
*  ==================
*>
*> \verbatim
*>
*>  November  2023, Igor Kozachenko, James Demmel,
*>                  EECS Department,
*>                  University of California, Berkeley, USA.
*>
*> \endverbatim
*
*  =====================================================================
      SUBROUTINE ZGEQP3RK( M, N, NRHS, KMAX, ABSTOL, RELTOL, A, LDA,
     $                     K, MAXC2NRMK, RELMAXC2NRMK, JPIV, TAU,
     $                     WORK, LWORK, RWORK, IWORK, INFO )
      IMPLICIT NONE
*
*  -- LAPACK computational routine --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*
*     .. Scalar Arguments ..
      INTEGER            INFO, K, KF, KMAX, LDA, LWORK, M, N, NRHS
      DOUBLE PRECISION   ABSTOL,  MAXC2NRMK, RELMAXC2NRMK, RELTOL
*     ..
*     .. Array Arguments ..
      INTEGER            IWORK( * ), JPIV( * )
      DOUBLE PRECISION   RWORK( * )
      COMPLEX*16         A( LDA, * ), TAU( * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      INTEGER            INB, INBMIN, IXOVER
      PARAMETER          ( INB = 1, INBMIN = 2, IXOVER = 3 )
      DOUBLE PRECISION   ZERO, ONE, TWO
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0, TWO = 2.0D+0 )
      COMPLEX*16         CZERO
      PARAMETER          ( CZERO = ( 0.0D+0, 0.0D+0 ) )
*     ..
*     .. Local Scalars ..
      LOGICAL            LQUERY, DONE
      INTEGER            IINFO, IOFFSET, IWS, J, JB, JBF, JMAXB, JMAX,
     $                   JMAXC2NRM, KP1, LWKOPT, MINMN, N_SUB, NB,
     $                   NBMIN, NX
      DOUBLE PRECISION   EPS, HUGEVAL, MAXC2NRM, SAFMIN
*     ..
*     .. External Subroutines ..
      EXTERNAL           ZLAQP2RK, ZLAQP3RK, XERBLA
*     ..
*     .. External Functions ..
      LOGICAL            DISNAN
      INTEGER            IDAMAX, ILAENV
      DOUBLE PRECISION   DLAMCH, DZNRM2
      EXTERNAL           DISNAN, DLAMCH, DZNRM2, IDAMAX, ILAENV
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DCMPLX, MAX, MIN
*     ..
*     .. Executable Statements ..
*
*     Test input arguments
*     ====================
*
      INFO = 0
      LQUERY = ( LWORK.EQ.-1 )
      IF( M.LT.0 ) THEN
         INFO = -1
      ELSE IF( N.LT.0 ) THEN
         INFO = -2
      ELSE IF( NRHS.LT.0 ) THEN
         INFO = -3
      ELSE IF( KMAX.LT.0 ) THEN
         INFO = -4
      ELSE IF( DISNAN( ABSTOL ) ) THEN
         INFO = -5
      ELSE IF( DISNAN( RELTOL ) ) THEN
         INFO = -6
      ELSE IF( LDA.LT.MAX( 1, M ) ) THEN
         INFO = -8
      END IF
*
*     If the input parameters M, N, NRHS, KMAX, LDA are valid:
*       a) Test the input workspace size LWORK for the minimum
*          size requirement IWS.
*       b) Determine the optimal block size NB and optimal
*          workspace size LWKOPT to be returned in WORK(1)
*          in case of (1) LWORK < IWS, (2) LQUERY = .TRUE.,
*          (3) when routine exits.
*     Here, IWS is the miminum workspace required for unblocked
*     code.
*
      IF( INFO.EQ.0 ) THEN
         MINMN = MIN( M, N )
         IF( MINMN.EQ.0 ) THEN
            IWS = 1
            LWKOPT = 1
         ELSE
*
*           Minimal workspace size in case of using only unblocked
*           BLAS 2 code in ZLAQP2RK.
*           1) ZLAQP2RK: N+NRHS-1 to use in WORK array that is used
*              in ZLARF subroutine inside ZLAQP2RK to apply an
*              elementary reflector from the left.
*           TOTAL_WORK_SIZE = 3*N + NRHS - 1
*
            IWS = N + NRHS - 1
*
*           Assign to NB optimal block size.
*
            NB = ILAENV( INB, 'ZGEQP3RK', ' ', M, N, -1, -1 )
*
*           A formula for the optimal workspace size in case of using
*           both unblocked BLAS 2 in ZLAQP2RK and blocked BLAS 3 code
*           in ZLAQP3RK.
*           1) ZGEQP3RK, ZLAQP2RK, ZLAQP3RK: 2*N to store full and
*              partial column 2-norms.
*           2) ZLAQP2RK: N+NRHS-1 to use in WORK array that is used
*              in ZLARF subroutine to apply an elementary reflector
*              from the left.
*           3) ZLAQP3RK: NB*(N+NRHS) to use in the work array F that
*              is used to apply a block reflector from
*              the left.
*           4) ZLAQP3RK: NB to use in the auxilixary array AUX.
*           Sizes (2) and ((3) + (4)) should intersect, therefore
*           TOTAL_WORK_SIZE = 2*N + NB*( N+NRHS+1 ), given NBMIN=2.
*
            LWKOPT = 2*N + NB*( N+NRHS+1 )
         END IF
         WORK( 1 ) = DCMPLX( LWKOPT )
*
         IF( ( LWORK.LT.IWS ) .AND. .NOT.LQUERY ) THEN
            INFO = -15
         END IF
      END IF
*
*      NOTE: The optimal workspace size is returned in WORK(1), if
*            the input parameters M, N, NRHS, KMAX, LDA are valid.
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZGEQP3RK', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         RETURN
      END IF
*
*     Quick return if possible for M=0 or N=0.
*
      IF( MINMN.EQ.0 ) THEN
         K = 0
         MAXC2NRMK = ZERO
         RELMAXC2NRMK = ZERO
         WORK( 1 ) = DCMPLX( LWKOPT )
         RETURN
      END IF
*
*     ==================================================================
*
*     Initialize column pivot array JPIV.
*
      DO J = 1, N
         JPIV( J ) = J
      END DO
*
*     ==================================================================
*
*     Initialize storage for partial and exact column 2-norms.
*     a) The elements WORK(1:N) are used to store partial column
*        2-norms of the matrix A, and may decrease in each computation
*        step; initialize to the values of complete columns 2-norms.
*     b) The elements WORK(N+1:2*N) are used to store complete column
*        2-norms of the matrix A, they are not changed during the
*        computation; initialize the values of complete columns 2-norms.
*
      DO J = 1, N
         RWORK( J ) = DZNRM2( M, A( 1, J ), 1 )
         RWORK( N+J ) = RWORK( J )
      END DO
*
*     ==================================================================
*
*     Compute the pivot column index and the maximum column 2-norm
*     for the whole original matrix stored in A(1:M,1:N).
*
      KP1 = IDAMAX( N, RWORK( 1 ), 1 )
      MAXC2NRM = RWORK( KP1 )
*
*     ==================================================================.
*
      IF( DISNAN( MAXC2NRM ) ) THEN
*
*        Check if the matrix A contains NaN, set INFO parameter
*        to the column number where the first NaN is found and return
*        from the routine.
*
         K = 0
         INFO = KP1
*
*        Set MAXC2NRMK and  RELMAXC2NRMK to NaN.
*
         MAXC2NRMK = MAXC2NRM
         RELMAXC2NRMK = MAXC2NRM
*
*        Array TAU is not set and contains undefined elements.
*
         WORK( 1 ) = DCMPLX( LWKOPT )
         RETURN
      END IF
*
*     ===================================================================
*
      IF( MAXC2NRM.EQ.ZERO ) THEN
*
*        Check is the matrix A is a zero matrix, set array TAU and
*        return from the routine.
*
         K = 0
         MAXC2NRMK = ZERO
         RELMAXC2NRMK = ZERO
*
         DO J = 1, MINMN
            TAU( J ) = CZERO
         END DO
*
         WORK( 1 ) = DCMPLX( LWKOPT )
         RETURN
*
      END IF
*
*     ===================================================================
*
      HUGEVAL = DLAMCH( 'Overflow' )
*
      IF( MAXC2NRM.GT.HUGEVAL ) THEN
*
*        Check if the matrix A contains +Inf or -Inf, set INFO parameter
*        to the column number, where the first +/-Inf  is found plus N,
*        and continue the computation.
*
         INFO = N + KP1
*
      END IF
*
*     ==================================================================
*
*     Quick return if possible for the case when the first
*     stopping criterion is satisfied, i.e. KMAX = 0.
*
      IF( KMAX.EQ.0 ) THEN
         K = 0
         MAXC2NRMK = MAXC2NRM
         RELMAXC2NRMK = ONE
         DO J = 1, MINMN
            TAU( J ) = CZERO
         END DO
         WORK( 1 ) = DCMPLX( LWKOPT )
         RETURN
      END IF
*
*     ==================================================================
*
      EPS = DLAMCH('Epsilon')
*
*     Adjust ABSTOL
*
      IF( ABSTOL.GE.ZERO ) THEN
         SAFMIN = DLAMCH('Safe minimum')
         ABSTOL = MAX( ABSTOL, TWO*SAFMIN )
      END IF
*
*     Adjust RELTOL
*
      IF( RELTOL.GE.ZERO ) THEN
         RELTOL = MAX( RELTOL, EPS )
      END IF
*
*     ===================================================================
*
*     JMAX is the maximum index of the column to be factorized,
*     which is also limited by the first stopping criterion KMAX.
*
      JMAX = MIN( KMAX, MINMN )
*
*     ===================================================================
*
*     Quick return if possible for the case when the second or third
*     stopping criterion for the whole original matrix is satified,
*     i.e. MAXC2NRM <= ABSTOL or RELMAXC2NRM <= RELTOL
*     (which is ONE <= RELTOL).
*
      IF( MAXC2NRM.LE.ABSTOL .OR. ONE.LE.RELTOL ) THEN
*
         K = 0
         MAXC2NRMK = MAXC2NRM
         RELMAXC2NRMK = ONE
*
         DO J = 1, MINMN
            TAU( J ) = CZERO
         END DO
*
         WORK( 1 ) = DCMPLX( LWKOPT )
         RETURN
      END IF
*
*     ==================================================================
*     Factorize columns
*     ==================================================================
*
*     Determine the block size.
*
      NBMIN = 2
      NX = 0
*
      IF( ( NB.GT.1 ) .AND. ( NB.LT.MINMN ) ) THEN
*
*        Determine when to cross over from blocked to unblocked code.
*        (for N less than NX, unblocked code should be used).
*
         NX = MAX( 0, ILAENV( IXOVER, 'ZGEQP3RK', ' ', M, N, -1, -1 ) )
*
         IF( NX.LT.MINMN ) THEN
*
*           Determine if workspace is large enough for blocked code.
*
            IF( LWORK.LT.LWKOPT ) THEN
*
*              Not enough workspace to use optimal block size that
*              is currently stored in NB.
*              Reduce NB and determine the minimum value of NB.
*
               NB = ( LWORK-2*N ) / ( N+1 )
               NBMIN = MAX( 2, ILAENV( INBMIN, 'ZGEQP3RK', ' ', M, N,
     $                 -1, -1 ) )
*
            END IF
         END IF
      END IF
*
*     ==================================================================
*
*     DONE is the boolean flag to rerpresent the case when the
*     factorization completed in the block factorization routine,
*     before the end of the block.
*
      DONE = .FALSE.
*
*     J is the column index.
*
      J = 1
*
*     (1) Use blocked code initially.
*
*     JMAXB is the maximum column index of the block, when the
*     blocked code is used, is also limited by the first stopping
*     criterion KMAX.
*
      JMAXB = MIN( KMAX, MINMN - NX )
*
      IF( NB.GE.NBMIN .AND. NB.LT.JMAX .AND. JMAXB.GT.0 ) THEN
*
*        Loop over the column blocks of the matrix A(1:M,1:JMAXB). Here:
*        J   is the column index of a column block;
*        JB  is the column block size to pass to block factorization
*            routine in a loop step;
*        JBF is the number of columns that were actually factorized
*            that was returned by the block factorization routine
*            in a loop step, JBF <= JB;
*        N_SUB is the number of columns in the submatrix;
*        IOFFSET is the number of rows that should not be factorized.
*
         DO WHILE( J.LE.JMAXB )
*
            JB = MIN( NB, JMAXB-J+1 )
            N_SUB = N-J+1
            IOFFSET = J-1
*
*           Factorize JB columns among the columns A(J:N).
*
            CALL ZLAQP3RK( M, N_SUB, NRHS, IOFFSET, JB, ABSTOL,
     $                     RELTOL, KP1, MAXC2NRM, A( 1, J ), LDA,
     $                     DONE, JBF, MAXC2NRMK, RELMAXC2NRMK,
     $                     JPIV( J ), TAU( J ),
     $                     RWORK( J ), RWORK( N+J ),
     $                     WORK( 1 ), WORK( JB+1 ),
     $                     N+NRHS-J+1, IWORK, IINFO )
*
*           Set INFO on the first occurence of Inf.
*
            IF( IINFO.GT.N_SUB .AND. INFO.EQ.0 ) THEN
               INFO = 2*IOFFSET + IINFO
            END IF
*
            IF( DONE ) THEN
*
*              Either the submatrix is zero before the end of the
*              column block, or ABSTOL or RELTOL criterion is
*              satisfied before the end of the column block, we can
*              return from the routine. Perform the following before
*              returning:
*                a) Set the number of factorized columns K,
*                   K = IOFFSET + JBF from the last call of blocked
*                   routine.
*                NOTE: 1) MAXC2NRMK and RELMAXC2NRMK are returned
*                         by the block factorization routine;
*                      2) The remaining TAUs are set to ZERO by the
*                         block factorization routine.
*
               K = IOFFSET + JBF
*
*              Set INFO on the first occurrence of NaN, NaN takes
*              prcedence over Inf.
*
               IF( IINFO.LE.N_SUB .AND. IINFO.GT.0 ) THEN
                  INFO = IOFFSET + IINFO
               END IF
*
*              Return from the routine.
*
               WORK( 1 ) = DCMPLX( LWKOPT )
*
               RETURN
*
            END IF
*
            J = J + JBF
*
         END DO
*
      END IF
*
*     Use unblocked code to factor the last or only block.
*     J = JMAX+1 means we factorized the maximum possible number of
*     columns, that is in ELSE clause we need to compute
*     the MAXC2NORM and RELMAXC2NORM to return after we processed
*     the blocks.
*
      IF( J.LE.JMAX ) THEN
*
*        N_SUB is the number of columns in the submatrix;
*        IOFFSET is the number of rows that should not be factorized.
*
         N_SUB = N-J+1
         IOFFSET = J-1
*
         CALL ZLAQP2RK( M, N_SUB, NRHS, IOFFSET, JMAX-J+1,
     $                  ABSTOL, RELTOL, KP1, MAXC2NRM, A( 1, J ), LDA,
     $                  KF, MAXC2NRMK, RELMAXC2NRMK, JPIV( J ),
     $                  TAU( J ), RWORK( J ), RWORK( N+J ),
     $                  WORK( 1 ), IINFO )
*
*        ABSTOL or RELTOL criterion is satisfied when the number of
*        the factorized columns KF is smaller then the  number
*        of columns JMAX-J+1 supplied to be factorized by the
*        unblocked routine, we can return from
*        the routine. Perform the following before returning:
*           a) Set the number of factorized columns K,
*           b) MAXC2NRMK and RELMAXC2NRMK are returned by the
*              unblocked factorization routine above.
*
         K = J - 1 + KF
*
*        Set INFO on the first exception occurence.
*
*        Set INFO on the first exception occurence of Inf or NaN,
*        (NaN takes precedence over Inf).
*
         IF( IINFO.GT.N_SUB .AND. INFO.EQ.0 ) THEN
            INFO = 2*IOFFSET + IINFO
         ELSE IF( IINFO.LE.N_SUB .AND. IINFO.GT.0 ) THEN
            INFO = IOFFSET + IINFO
         END IF
*
      ELSE
*
*        Compute the return values for blocked code.
*
*        Set the number of factorized columns if the unblocked routine
*        was not called.
*
            K = JMAX
*
*        If there exits a residual matrix after the blocked code:
*           1) compute the values of MAXC2NRMK, RELMAXC2NRMK of the
*              residual matrix, otherwise set them to ZERO;
*           2) Set TAU(K+1:MINMN) to ZERO.
*
         IF( K.LT.MINMN ) THEN
            JMAXC2NRM = K + IDAMAX( N-K, RWORK( K+1 ), 1 )
            MAXC2NRMK = RWORK( JMAXC2NRM )
            IF( K.EQ.0 ) THEN
               RELMAXC2NRMK = ONE
            ELSE
               RELMAXC2NRMK = MAXC2NRMK / MAXC2NRM
            END IF
*
            DO J = K + 1, MINMN
               TAU( J ) = CZERO
            END DO
*
         ELSE
            MAXC2NRMK = ZERO
            RELMAXC2NRMK = ZERO
*
         END IF
*
*     END IF( J.LE.JMAX ) THEN
*
      END IF
*
      WORK( 1 ) = DCMPLX( LWKOPT )
*
      RETURN
*
*     End of ZGEQP3RK
*
      END
