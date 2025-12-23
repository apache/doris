*> \brief \b DQPT01
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       DOUBLE PRECISION FUNCTION DQPT01( M, N, K, A, AF, LDA, TAU, JPVT,
*                        WORK, LWORK )
*
*       .. Scalar Arguments ..
*       INTEGER            K, LDA, LWORK, M, N
*       ..
*       .. Array Arguments ..
*       INTEGER            JPVT( * )
*       DOUBLE PRECISION   A( LDA, * ), AF( LDA, * ), TAU( * ),
*      $                   WORK( LWORK )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DQPT01 tests the QR-factorization with pivoting of a matrix A.  The
*> array AF contains the (possibly partial) QR-factorization of A, where
*> the upper triangle of AF(1:K,1:K) is a partial triangular factor,
*> the entries below the diagonal in the first K columns are the
*> Householder vectors, and the rest of AF contains a partially updated
*> matrix.
*>
*> This function returns ||A*P - Q*R|| / ( ||norm(A)||*eps*max(M,N) ),
*> where || . || is matrix one norm.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the matrices A and AF.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrices A and AF.
*> \endverbatim
*>
*> \param[in] K
*> \verbatim
*>          K is INTEGER
*>          The number of columns of AF that have been reduced
*>          to upper triangular form.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is DOUBLE PRECISION array, dimension (LDA, N)
*>          The original matrix A.
*> \endverbatim
*>
*> \param[in] AF
*> \verbatim
*>          AF is DOUBLE PRECISION array, dimension (LDA,N)
*>          The (possibly partial) output of DGEQPF.  The upper triangle
*>          of AF(1:k,1:k) is a partial triangular factor, the entries
*>          below the diagonal in the first k columns are the Householder
*>          vectors, and the rest of AF contains a partially updated
*>          matrix.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the arrays A and AF.
*> \endverbatim
*>
*> \param[in] TAU
*> \verbatim
*>          TAU is DOUBLE PRECISION array, dimension (K)
*>          Details of the Householder transformations as returned by
*>          DGEQPF.
*> \endverbatim
*>
*> \param[in] JPVT
*> \verbatim
*>          JPVT is INTEGER array, dimension (N)
*>          Pivot information as returned by DGEQPF.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (LWORK)
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The length of the array WORK.  LWORK >= M*N+N.
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
*> \ingroup double_lin
*
*  =====================================================================
      DOUBLE PRECISION FUNCTION DQPT01( M, N, K, A, AF, LDA, TAU, JPVT,
     $                 WORK, LWORK )
*
*  -- LAPACK test routine --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*
*     .. Scalar Arguments ..
      INTEGER            K, LDA, LWORK, M, N
*     ..
*     .. Array Arguments ..
      INTEGER            JPVT( * )
      DOUBLE PRECISION   A( LDA, * ), AF( LDA, * ), TAU( * ),
     $                   WORK( LWORK )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D0, ONE = 1.0D0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, INFO, J
      DOUBLE PRECISION   NORMA
*     ..
*     .. Local Arrays ..
      DOUBLE PRECISION   RWORK( 1 )
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH, DLANGE
      EXTERNAL           DLAMCH, DLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           DAXPY, DCOPY, DORMQR, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DBLE, MAX, MIN
*     ..
*     .. Executable Statements ..
*
      DQPT01 = ZERO
*
*     Test if there is enough workspace
*
      IF( LWORK.LT.M*N+N ) THEN
         CALL XERBLA( 'DQPT01', 10 )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( M.LE.0 .OR. N.LE.0 )
     $   RETURN
*
      NORMA = DLANGE( 'One-norm', M, N, A, LDA, RWORK )
*
      DO J = 1, K
*
*        Copy the upper triangular part of the factor R stored
*        in AF(1:K,1:K) into the work array WORK.
*
         DO I = 1, MIN( J, M )
            WORK( ( J-1 )*M+I ) = AF( I, J )
         END DO
*
*        Zero out the elements below the diagonal in the work array.
*
         DO I = J + 1, M
            WORK( ( J-1 )*M+I ) = ZERO
         END DO
      END DO
*
*     Copy columns (K+1,N) from AF into the work array WORK.
*     AF(1:K,K+1:N) contains the rectangular block of the upper trapezoidal
*     factor R, AF(K+1:M,K+1:N) contains the partially updated residual
*     matrix of R.
*
      DO J = K + 1, N
         CALL DCOPY( M, AF( 1, J ), 1, WORK( ( J-1 )*M+1 ), 1 )
      END DO
*
      CALL DORMQR( 'Left', 'No transpose', M, N, K, AF, LDA, TAU, WORK,
     $             M, WORK( M*N+1 ), LWORK-M*N, INFO )
*
      DO J = 1, N
*
*        Compare J-th column of QR and JPVT(J)-th column of A.
*
         CALL DAXPY( M, -ONE, A( 1, JPVT( J ) ), 1, WORK( ( J-1 )*M+1 ),
     $               1 )
      END DO
*
      DQPT01 = DLANGE( 'One-norm', M, N, WORK, M, RWORK ) /
     $         ( DBLE( MAX( M, N ) )*DLAMCH( 'Epsilon' ) )
      IF( NORMA.NE.ZERO )
     $   DQPT01 = DQPT01 / NORMA
*
      RETURN
*
*     End of DQPT01
*
      END
