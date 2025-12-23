*> \brief \b DGBT02
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DGBT02( TRANS, M, N, KL, KU, NRHS, A, LDA, X, LDX, B,
*                          LDB, RWORK, RESID )
*
*       .. Scalar Arguments ..
*       CHARACTER          TRANS
*       INTEGER            KL, KU, LDA, LDB, LDX, M, N, NRHS
*       DOUBLE PRECISION   RESID
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   A( LDA, * ), B( LDB, * ), X( LDX, * ),
*                          RWORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DGBT02 computes the residual for a solution of a banded system of
*> equations op(A)*X = B:
*>    RESID = norm(B - op(A)*X) / ( norm(op(A)) * norm(X) * EPS ),
*> where op(A) = A or A**T, depending on TRANS, and EPS is the
*> machine epsilon.
*> The norm used is the 1-norm.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] TRANS
*> \verbatim
*>          TRANS is CHARACTER*1
*>          Specifies the form of the system of equations:
*>          = 'N':  A    * X = B  (No transpose)
*>          = 'T':  A**T * X = B  (Transpose)
*>          = 'C':  A**H * X = B  (Conjugate transpose = Transpose)
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the matrix A.  M >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] KL
*> \verbatim
*>          KL is INTEGER
*>          The number of subdiagonals within the band of A.  KL >= 0.
*> \endverbatim
*>
*> \param[in] KU
*> \verbatim
*>          KU is INTEGER
*>          The number of superdiagonals within the band of A.  KU >= 0.
*> \endverbatim
*>
*> \param[in] NRHS
*> \verbatim
*>          NRHS is INTEGER
*>          The number of columns of B.  NRHS >= 0.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is DOUBLE PRECISION array, dimension (LDA,N)
*>          The original matrix A in band storage, stored in rows 1 to
*>          KL+KU+1.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,KL+KU+1).
*> \endverbatim
*>
*> \param[in] X
*> \verbatim
*>          X is DOUBLE PRECISION array, dimension (LDX,NRHS)
*>          The computed solution vectors for the system of linear
*>          equations.
*> \endverbatim
*>
*> \param[in] LDX
*> \verbatim
*>          LDX is INTEGER
*>          The leading dimension of the array X.  If TRANS = 'N',
*>          LDX >= max(1,N); if TRANS = 'T' or 'C', LDX >= max(1,M).
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is DOUBLE PRECISION array, dimension (LDB,NRHS)
*>          On entry, the right hand side vectors for the system of
*>          linear equations.
*>          On exit, B is overwritten with the difference B - A*X.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B.  IF TRANS = 'N',
*>          LDB >= max(1,M); if TRANS = 'T' or 'C', LDB >= max(1,N).
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array, dimension (MAX(1,LRWORK)),
*>          where LRWORK >= M when TRANS = 'T' or 'C'; otherwise, RWORK
*>          is not referenced.
*> \endverbatim
*
*> \param[out] RESID
*> \verbatim
*>          RESID is DOUBLE PRECISION
*>          The maximum over the number of right hand sides of
*>          norm(B - op(A)*X) / ( norm(op(A)) * norm(X) * EPS ).
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
      SUBROUTINE DGBT02( TRANS, M, N, KL, KU, NRHS, A, LDA, X, LDX, B,
     $                   LDB, RWORK, RESID )
*
*  -- LAPACK test routine --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*
*     .. Scalar Arguments ..
      CHARACTER          TRANS
      INTEGER            KL, KU, LDA, LDB, LDX, M, N, NRHS
      DOUBLE PRECISION   RESID
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   A( LDA, * ), B( LDB, * ), X( LDX, * ),
     $                   RWORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I1, I2, J, KD, N1
      DOUBLE PRECISION   ANORM, BNORM, EPS, TEMP, XNORM
*     ..
*     .. External Functions ..
      LOGICAL            DISNAN, LSAME
      DOUBLE PRECISION   DASUM, DLAMCH
      EXTERNAL           DASUM, DISNAN, DLAMCH, LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           DGBMV
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, MIN
*     ..
*     .. Executable Statements ..
*
*     Quick return if N = 0 pr NRHS = 0
*
      IF( M.LE.0 .OR. N.LE.0 .OR. NRHS.LE.0 ) THEN
         RESID = ZERO
         RETURN
      END IF
*
*     Exit with RESID = 1/EPS if ANORM = 0.
*
      EPS = DLAMCH( 'Epsilon' )
      ANORM = ZERO
      IF( LSAME( TRANS, 'N' ) ) THEN
*
*        Find norm1(A).
*
         KD = KU + 1
      DO 10 J = 1, N
         I1 = MAX( KD+1-J, 1 )
         I2 = MIN( KD+M-J, KL+KD )
            IF( I2.GE.I1 ) THEN
               TEMP = DASUM( I2-I1+1, A( I1, J ), 1 )
               IF( ANORM.LT.TEMP .OR. DISNAN( TEMP ) ) ANORM = TEMP
            END IF
   10 CONTINUE
      ELSE
*
*        Find normI(A).
*
         DO 12 I1 = 1, M
            RWORK( I1 ) = ZERO
   12    CONTINUE
         DO 16 J = 1, N
            KD = KU + 1 - J
            DO 14 I1 = MAX( 1, J-KU ), MIN( M, J+KL )
               RWORK( I1 ) = RWORK( I1 ) + ABS( A( KD+I1, J ) )
   14       CONTINUE
   16    CONTINUE
         DO 18 I1 = 1, M
            TEMP = RWORK( I1 )
            IF( ANORM.LT.TEMP .OR. DISNAN( TEMP ) ) ANORM = TEMP
   18    CONTINUE
      END IF
      IF( ANORM.LE.ZERO ) THEN
         RESID = ONE / EPS
         RETURN
      END IF
*
      IF( LSAME( TRANS, 'T' ) .OR. LSAME( TRANS, 'C' ) ) THEN
         N1 = N
      ELSE
         N1 = M
      END IF
*
*     Compute B - op(A)*X
*
      DO 20 J = 1, NRHS
         CALL DGBMV( TRANS, M, N, KL, KU, -ONE, A, LDA, X( 1, J ), 1,
     $               ONE, B( 1, J ), 1 )
   20 CONTINUE
*
*     Compute the maximum over the number of right hand sides of
*        norm(B - op(A)*X) / ( norm(op(A)) * norm(X) * EPS ).
*
      RESID = ZERO
      DO 30 J = 1, NRHS
         BNORM = DASUM( N1, B( 1, J ), 1 )
         XNORM = DASUM( N1, X( 1, J ), 1 )
         IF( XNORM.LE.ZERO ) THEN
            RESID = ONE / EPS
         ELSE
            RESID = MAX( RESID, ( ( BNORM / ANORM ) / XNORM ) / EPS )
         END IF
   30 CONTINUE
*
      RETURN
*
*     End of DGBT02
*
      END
