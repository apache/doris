*> \brief \b CLAQP2RK computes truncated QR factorization with column pivoting of a complex matrix block using Level 2 BLAS and overwrites a complex m-by-nrhs matrix B with Q**H * B.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CLAQP2RK + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/claqp2rk.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/claqp2rk.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/claqp2rk.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*      SUBROUTINE CLAQP2RK( M, N, NRHS, IOFFSET, KMAX, ABSTOL, RELTOL,
*     $                     KP1, MAXC2NRM, A, LDA, K, MAXC2NRMK,
*     $                     RELMAXC2NRMK, JPIV, TAU, VN1, VN2, WORK,
*     $                     INFO )
*      IMPLICIT NONE
*
*     .. Scalar Arguments ..
*      INTEGER            INFO, IOFFSET, KP1, K, KMAX, LDA, M, N, NRHS
*      REAL               ABSTOL, MAXC2NRM, MAXC2NRMK, RELMAXC2NRMK,
*     $                   RELTOL
*     ..
*     .. Array Arguments ..
*      INTEGER            JPIV( * )
*      REAL               VN1( * ), VN2( * )
*      COMPLEX            A( LDA, * ), TAU( * ), WORK( * )
*     $
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CLAQP2RK computes a truncated (rank K) or full rank Householder QR
*> factorization with column pivoting of the complex matrix
*> block A(IOFFSET+1:M,1:N) as
*>
*>   A * P(K) = Q(K) * R(K).
*>
*> The routine uses Level 2 BLAS. The block A(1:IOFFSET,1:N)
*> is accordingly pivoted, but not factorized.
*>
*> The routine also overwrites the right-hand-sides matrix block B
*> stored in A(IOFFSET+1:M,N+1:N+NRHS) with Q(K)**H * B.
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
*>          The number of right hand sides, i.e., the number of
*>          columns of the matrix B. NRHS >= 0.
*> \endverbatim
*>
*> \param[in] IOFFSET
*> \verbatim
*>          IOFFSET is INTEGER
*>          The number of rows of the matrix A that must be pivoted
*>          but not factorized. IOFFSET >= 0.
*>
*>          IOFFSET also represents the number of columns of the whole
*>          original matrix A_orig that have been factorized
*>          in the previous steps.
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
*>          a) If KMAX >= min(M-IOFFSET,N), then this stopping
*>                criterion is not used, factorize columns
*>                depending on ABSTOL and RELTOL.
*>
*>          b) If KMAX = 0, then this stopping criterion is
*>             satisfied on input and the routine exits immediately.
*>             This means that the factorization is not performed,
*>             the matrices A and B and the arrays TAU, IPIV
*>             are not modified.
*> \endverbatim
*>
*> \param[in] ABSTOL
*> \verbatim
*>          ABSTOL is REAL, cannot be NaN.
*>
*>          The second factorization stopping criterion.
*>
*>          The absolute tolerance (stopping threshold) for
*>          maximum column 2-norm of the residual matrix.
*>          The algorithm converges (stops the factorization) when
*>          the maximum column 2-norm of the residual matrix
*>          is less than or equal to ABSTOL.
*>
*>          a) If ABSTOL < 0.0, then this stopping criterion is not
*>                used, the routine factorizes columns depending
*>                on KMAX and RELTOL.
*>                This includes the case ABSTOL = -Inf.
*>
*>          b) If 0.0 <= ABSTOL then the input value
*>                of ABSTOL is used.
*> \endverbatim
*>
*> \param[in] RELTOL
*> \verbatim
*>          RELTOL is REAL, cannot be NaN.
*>
*>          The third factorization stopping criterion.
*>
*>          The tolerance (stopping threshold) for the ratio of the
*>          maximum column 2-norm of the residual matrix to the maximum
*>          column 2-norm of the original matrix A_orig. The algorithm
*>          converges (stops the factorization), when this ratio is
*>          less than or equal to RELTOL.
*>
*>          a) If RELTOL < 0.0, then this stopping criterion is not
*>                used, the routine factorizes columns depending
*>                on KMAX and ABSTOL.
*>                This includes the case RELTOL = -Inf.
*>
*>          d) If 0.0 <= RELTOL then the input value of RELTOL
*>                is used.
*> \endverbatim
*>
*> \param[in] KP1
*> \verbatim
*>          KP1 is INTEGER
*>          The index of the column with the maximum 2-norm in
*>          the whole original matrix A_orig determined in the
*>          main routine CGEQP3RK. 1 <= KP1 <= N_orig_mat.
*> \endverbatim
*>
*> \param[in] MAXC2NRM
*> \verbatim
*>          MAXC2NRM is REAL
*>          The maximum column 2-norm of the whole original
*>          matrix A_orig computed in the main routine CGEQP3RK.
*>          MAXC2NRM >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA,N+NRHS)
*>          On entry:
*>              the M-by-N matrix A and M-by-NRHS matrix B, as in
*>
*>                                  N     NRHS
*>              array_A   =   M  [ mat_A, mat_B ]
*>
*>          On exit:
*>          1. The elements in block A(IOFFSET+1:M,1:K) below
*>             the diagonal together with the array TAU represent
*>             the unitary matrix Q(K) as a product of elementary
*>             reflectors.
*>          2. The upper triangular block of the matrix A stored
*>             in A(IOFFSET+1:M,1:K) is the triangular factor obtained.
*>          3. The block of the matrix A stored in A(1:IOFFSET,1:N)
*>             has been accordingly pivoted, but not factorized.
*>          4. The rest of the array A, block A(IOFFSET+1:M,K+1:N+NRHS).
*>             The left part A(IOFFSET+1:M,K+1:N) of this block
*>             contains the residual of the matrix A, and,
*>             if NRHS > 0, the right part of the block
*>             A(IOFFSET+1:M,N+1:N+NRHS) contains the block of
*>             the right-hand-side matrix B. Both these blocks have been
*>             updated by multiplication from the left by Q(K)**H.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A. LDA >= max(1,M).
*> \endverbatim
*>
*> \param[out] K
*> \verbatim
*>          K is INTEGER
*>          Factorization rank of the matrix A, i.e. the rank of
*>          the factor R, which is the same as the number of non-zero
*>          rows of the factor R. 0 <= K <= min(M-IOFFSET,KMAX,N).
*>
*>          K also represents the number of non-zero Householder
*>          vectors.
*> \endverbatim
*>
*> \param[out] MAXC2NRMK
*> \verbatim
*>          MAXC2NRMK is REAL
*>          The maximum column 2-norm of the residual matrix,
*>          when the factorization stopped at rank K. MAXC2NRMK >= 0.
*> \endverbatim
*>
*> \param[out] RELMAXC2NRMK
*> \verbatim
*>          RELMAXC2NRMK is REAL
*>          The ratio MAXC2NRMK / MAXC2NRM of the maximum column
*>          2-norm of the residual matrix (when the factorization
*>          stopped at rank K) to the maximum column 2-norm of the
*>          whole original matrix A. RELMAXC2NRMK >= 0.
*> \endverbatim
*>
*> \param[out] JPIV
*> \verbatim
*>          JPIV is INTEGER array, dimension (N)
*>          Column pivot indices, for 1 <= j <= N, column j
*>          of the matrix A was interchanged with column JPIV(j).
*> \endverbatim
*>
*> \param[out] TAU
*> \verbatim
*>          TAU is COMPLEX array, dimension (min(M-IOFFSET,N))
*>          The scalar factors of the elementary reflectors.
*> \endverbatim
*>
*> \param[in,out] VN1
*> \verbatim
*>          VN1 is REAL array, dimension (N)
*>          The vector with the partial column norms.
*> \endverbatim
*>
*> \param[in,out] VN2
*> \verbatim
*>          VN2 is REAL array, dimension (N)
*>          The vector with the exact column norms.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX array, dimension (N-1)
*>          Used in CLARF subroutine to apply an elementary
*>          reflector from the left.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          1) INFO = 0: successful exit.
*>          2) If INFO = j_1, where 1 <= j_1 <= N, then NaN was
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
*>          3) If INFO = j_2, where N+1 <= j_2 <= 2*N, then no NaN
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
*> \ingroup laqp2rk
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
      SUBROUTINE CLAQP2RK( M, N, NRHS, IOFFSET, KMAX, ABSTOL, RELTOL,
     $                     KP1, MAXC2NRM, A, LDA, K, MAXC2NRMK,
     $                     RELMAXC2NRMK, JPIV, TAU, VN1, VN2, WORK,
     $                     INFO )
      IMPLICIT NONE
*
*  -- LAPACK auxiliary routine --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*
*     .. Scalar Arguments ..
      INTEGER            INFO, IOFFSET, KP1, K, KMAX, LDA, M, N, NRHS
      REAL               ABSTOL, MAXC2NRM, MAXC2NRMK, RELMAXC2NRMK,
     $                   RELTOL
*     ..
*     .. Array Arguments ..
      INTEGER            JPIV( * )
      REAL               VN1( * ), VN2( * )
      COMPLEX            A( LDA, * ), TAU( * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
      COMPLEX            CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0E+0, 0.0E+0 ),
     $                   CONE = ( 1.0E+0, 0.0E+0 ) )
*     ..
*     .. Local Scalars ..
      INTEGER            I, ITEMP, J, JMAXC2NRM, KK, KP, MINMNFACT,
     $                   MINMNUPDT
      REAL               HUGEVAL, TAUNAN, TEMP, TEMP2, TOL3Z
      COMPLEX            AIKK
*     ..
*     .. External Subroutines ..
      EXTERNAL           CLARF, CLARFG, CSWAP
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, REAL, CONJG, AIMAG, MAX, MIN, SQRT
*     ..
*     .. External Functions ..
      LOGICAL            SISNAN
      INTEGER            ISAMAX
      REAL               SLAMCH, SCNRM2
      EXTERNAL           SISNAN, SLAMCH, ISAMAX, SCNRM2
*     ..
*     .. Executable Statements ..
*
*     Initialize INFO
*
      INFO = 0
*
*     MINMNFACT in the smallest dimension of the submatrix
*     A(IOFFSET+1:M,1:N) to be factorized.
*
*     MINMNUPDT is the smallest dimension
*     of the subarray A(IOFFSET+1:M,1:N+NRHS) to be udated, which
*     contains the submatrices A(IOFFSET+1:M,1:N) and
*     B(IOFFSET+1:M,1:NRHS) as column blocks.
*
      MINMNFACT = MIN( M-IOFFSET, N )
      MINMNUPDT = MIN( M-IOFFSET, N+NRHS )
      KMAX = MIN( KMAX, MINMNFACT )
      TOL3Z = SQRT( SLAMCH( 'Epsilon' ) )
      HUGEVAL = SLAMCH( 'Overflow' )
*
*     Compute the factorization, KK is the lomn loop index.
*
      DO KK = 1, KMAX
*
         I = IOFFSET + KK
*
         IF( I.EQ.1 ) THEN
*
*           ============================================================
*
*           We are at the first column of the original whole matrix A,
*           therefore we use the computed KP1 and MAXC2NRM from the
*           main routine.
*
            KP = KP1
*
*           ============================================================
*
         ELSE
*
*           ============================================================
*
*           Determine the pivot column in KK-th step, i.e. the index
*           of the column with the maximum 2-norm in the
*           submatrix A(I:M,K:N).
*
            KP = ( KK-1 ) + ISAMAX( N-KK+1, VN1( KK ), 1 )
*
*           Determine the maximum column 2-norm and the relative maximum
*           column 2-norm of the submatrix A(I:M,KK:N) in step KK.
*           RELMAXC2NRMK  will be computed later, after somecondition
*           checks on MAXC2NRMK.
*
            MAXC2NRMK = VN1( KP )
*
*           ============================================================
*
*           Check if the submatrix A(I:M,KK:N) contains NaN, and set
*           INFO parameter to the column number, where the first NaN
*           is found and return from the routine.
*           We need to check the condition only if the
*           column index (same as row index) of the original whole
*           matrix is larger than 1, since the condition for whole
*           original matrix is checked in the main routine.
*
            IF( SISNAN( MAXC2NRMK ) ) THEN
*
*              Set K, the number of factorized columns.
*              that are not zero.
*
                K = KK - 1
                INFO = K + KP
*
*               Set RELMAXC2NRMK to NaN.
*
                RELMAXC2NRMK = MAXC2NRMK
*
*               Array TAU(K+1:MINMNFACT) is not set and contains
*               undefined elements.
*
               RETURN
            END IF
*
*           ============================================================
*
*           Quick return, if the submatrix A(I:M,KK:N) is
*           a zero matrix.
*           We need to check the condition only if the
*           column index (same as row index) of the original whole
*           matrix is larger than 1, since the condition for whole
*           original matrix is checked in the main routine.
*
            IF( MAXC2NRMK.EQ.ZERO ) THEN
*
*              Set K, the number of factorized columns.
*              that are not zero.
*
               K = KK - 1
               RELMAXC2NRMK = ZERO
*
*              Set TAUs corresponding to the columns that were not
*              factorized to ZERO, i.e. set TAU(KK:MINMNFACT) to CZERO.
*
               DO J = KK, MINMNFACT
                  TAU( J ) = CZERO
               END DO
*
*              Return from the routine.
*
               RETURN
*
            END IF
*
*           ============================================================
*
*           Check if the submatrix A(I:M,KK:N) contains Inf,
*           set INFO parameter to the column number, where
*           the first Inf is found plus N, and continue
*           the computation.
*           We need to check the condition only if the
*           column index (same as row index) of the original whole
*           matrix is larger than 1, since the condition for whole
*           original matrix is checked in the main routine.
*
            IF( INFO.EQ.0 .AND. MAXC2NRMK.GT.HUGEVAL ) THEN
               INFO = N + KK - 1 + KP
            END IF
*
*           ============================================================
*
*           Test for the second and third stopping criteria.
*           NOTE: There is no need to test for ABSTOL >= ZERO, since
*           MAXC2NRMK is non-negative. Similarly, there is no need
*           to test for RELTOL >= ZERO, since RELMAXC2NRMK is
*           non-negative.
*           We need to check the condition only if the
*           column index (same as row index) of the original whole
*           matrix is larger than 1, since the condition for whole
*           original matrix is checked in the main routine.

            RELMAXC2NRMK =  MAXC2NRMK / MAXC2NRM
*
            IF( MAXC2NRMK.LE.ABSTOL .OR. RELMAXC2NRMK.LE.RELTOL ) THEN
*
*              Set K, the number of factorized columns.
*
               K = KK - 1
*
*              Set TAUs corresponding to the columns that were not
*              factorized to ZERO, i.e. set TAU(KK:MINMNFACT) to CZERO.
*
               DO J = KK, MINMNFACT
                  TAU( J ) = CZERO
               END DO
*
*              Return from the routine.
*
               RETURN
*
            END IF
*
*           ============================================================
*
*           End ELSE of IF(I.EQ.1)
*
         END IF
*
*        ===============================================================
*
*        If the pivot column is not the first column of the
*        subblock A(1:M,KK:N):
*        1) swap the KK-th column and the KP-th pivot column
*           in A(1:M,1:N);
*        2) copy the KK-th element into the KP-th element of the partial
*           and exact 2-norm vectors VN1 and VN2. ( Swap is not needed
*           for VN1 and VN2 since we use the element with the index
*           larger than KK in the next loop step.)
*        3) Save the pivot interchange with the indices relative to the
*           the original matrix A, not the block A(1:M,1:N).
*
         IF( KP.NE.KK ) THEN
            CALL CSWAP( M, A( 1, KP ), 1, A( 1, KK ), 1 )
            VN1( KP ) = VN1( KK )
            VN2( KP ) = VN2( KK )
            ITEMP = JPIV( KP )
            JPIV( KP ) = JPIV( KK )
            JPIV( KK ) = ITEMP
         END IF
*
*        Generate elementary reflector H(KK) using the column A(I:M,KK),
*        if the column has more than one element, otherwise
*        the elementary reflector would be an identity matrix,
*        and TAU(KK) = CZERO.
*
         IF( I.LT.M ) THEN
            CALL CLARFG( M-I+1, A( I, KK ), A( I+1, KK ), 1,
     $                   TAU( KK ) )
         ELSE
            TAU( KK ) = CZERO
         END IF
*
*        Check if TAU(KK) contains NaN, set INFO parameter
*        to the column number where NaN is found and return from
*        the routine.
*        NOTE: There is no need to check TAU(KK) for Inf,
*        since CLARFG cannot produce TAU(KK) or Householder vector
*        below the diagonal containing Inf. Only BETA on the diagonal,
*        returned by CLARFG can contain Inf, which requires
*        TAU(KK) to contain NaN. Therefore, this case of generating Inf
*        by CLARFG is covered by checking TAU(KK) for NaN.
*
         IF( SISNAN( REAL( TAU(KK) ) ) ) THEN
            TAUNAN = REAL( TAU(KK) )
         ELSE IF( SISNAN( AIMAG( TAU(KK) ) ) ) THEN
            TAUNAN = AIMAG( TAU(KK) )
         ELSE
            TAUNAN = ZERO
         END IF
*
         IF( SISNAN( TAUNAN ) ) THEN
            K = KK - 1
            INFO = KK
*
*           Set MAXC2NRMK and  RELMAXC2NRMK to NaN.
*
            MAXC2NRMK = TAUNAN
            RELMAXC2NRMK = TAUNAN
*
*           Array TAU(KK:MINMNFACT) is not set and contains
*           undefined elements, except the first element TAU(KK) = NaN.
*
            RETURN
         END IF
*
*        Apply H(KK)**H to A(I:M,KK+1:N+NRHS) from the left.
*        ( If M >= N, then at KK = N there is no residual matrix,
*         i.e. no columns of A to update, only columns of B.
*         If M < N, then at KK = M-IOFFSET, I = M and we have a
*         one-row residual matrix in A and the elementary
*         reflector is a unit matrix, TAU(KK) = CZERO, i.e. no update
*         is needed for the residual matrix in A and the
*         right-hand-side-matrix in B.
*         Therefore, we update only if
*         KK < MINMNUPDT = min(M-IOFFSET, N+NRHS)
*         condition is satisfied, not only KK < N+NRHS )
*
         IF( KK.LT.MINMNUPDT ) THEN
            AIKK = A( I, KK )
            A( I, KK ) = CONE
            CALL CLARF( 'Left', M-I+1, N+NRHS-KK, A( I, KK ), 1,
     $                  CONJG( TAU( KK ) ), A( I, KK+1 ), LDA,
     $                  WORK( 1 ) )
            A( I, KK ) = AIKK
         END IF
*
         IF( KK.LT.MINMNFACT ) THEN
*
*           Update the partial column 2-norms for the residual matrix,
*           only if the residual matrix A(I+1:M,KK+1:N) exists, i.e.
*           when KK < min(M-IOFFSET, N).
*
            DO J = KK + 1, N
               IF( VN1( J ).NE.ZERO ) THEN
*
*                 NOTE: The following lines follow from the analysis in
*                 Lapack Working Note 176.
*
                  TEMP = ONE - ( ABS( A( I, J ) ) / VN1( J ) )**2
                  TEMP = MAX( TEMP, ZERO )
                  TEMP2 = TEMP*( VN1( J ) / VN2( J ) )**2
                  IF( TEMP2 .LE. TOL3Z ) THEN
*
*                    Compute the column 2-norm for the partial
*                    column A(I+1:M,J) by explicitly computing it,
*                    and store it in both partial 2-norm vector VN1
*                    and exact column 2-norm vector VN2.
*
                     VN1( J ) = SCNRM2( M-I, A( I+1, J ), 1 )
                     VN2( J ) = VN1( J )
*
                  ELSE
*
*                    Update the column 2-norm for the partial
*                    column A(I+1:M,J) by removing one
*                    element A(I,J) and store it in partial
*                    2-norm vector VN1.
*
                     VN1( J ) = VN1( J )*SQRT( TEMP )
*
                  END IF
               END IF
            END DO
*
         END IF
*
*     End factorization loop
*
      END DO
*
*     If we reached this point, all colunms have been factorized,
*     i.e. no condition was triggered to exit the routine.
*     Set the number of factorized columns.
*
      K = KMAX
*
*     We reached the end of the loop, i.e. all KMAX columns were
*     factorized, we need to set MAXC2NRMK and RELMAXC2NRMK before
*     we return.
*
      IF( K.LT.MINMNFACT ) THEN
*
         JMAXC2NRM = K + ISAMAX( N-K, VN1( K+1 ), 1 )
         MAXC2NRMK = VN1( JMAXC2NRM )
*
         IF( K.EQ.0 ) THEN
            RELMAXC2NRMK = ONE
         ELSE
            RELMAXC2NRMK = MAXC2NRMK / MAXC2NRM
         END IF
*
      ELSE
         MAXC2NRMK = ZERO
         RELMAXC2NRMK = ZERO
      END IF
*
*     We reached the end of the loop, i.e. all KMAX columns were
*     factorized, set TAUs corresponding to the columns that were
*     not factorized to ZERO, i.e. TAU(K+1:MINMNFACT) set to CZERO.
*
      DO J = K + 1, MINMNFACT
         TAU( J ) = CZERO
      END DO
*
      RETURN
*
*     End of CLAQP2RK
*
      END
