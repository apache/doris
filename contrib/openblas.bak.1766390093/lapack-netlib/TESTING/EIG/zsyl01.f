*> \brief \b ZSYL01
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZSYL01( THRESH, NFAIL, RMAX, NINFO, KNT )
*
*     .. Scalar Arguments ..
*     INTEGER            KNT
*     DOUBLE PRECISION   THRESH
*     ..
*     .. Array Arguments ..
*     INTEGER            NFAIL( 3 ), NINFO( 2 )
*     DOUBLE PRECISION   RMAX( 2 )
*     ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZSYL01 tests ZTRSYL and ZTRSYL3, routines for solving the Sylvester matrix
*> equation
*>
*>    op(A)*X + ISGN*X*op(B) = scale*C,
*>
*> where op(A) and op(B) are both upper triangular form, op() represents an
*> optional conjugate transpose, and ISGN can be -1 or +1. Scale is an output
*> less than or equal to 1, chosen to avoid overflow in X.
*>
*> The test code verifies that the following residual does not exceed
*> the provided threshold:
*>
*>    norm(op(A)*X + ISGN*X*op(B) - scale*C) /
*>        (EPS*max(norm(A),norm(B))*norm(X))
*>
*> This routine complements ZGET35 by testing with larger,
*> random matrices, of which some require rescaling of X to avoid overflow.
*>
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] THRESH
*> \verbatim
*>          THRESH is DOUBLE PRECISION
*>          A test will count as "failed" if the residual, computed as
*>          described above, exceeds THRESH.
*> \endverbatim
*>
*> \param[out] NFAIL
*> \verbatim
*>          NFAIL is INTEGER array, dimension (3)
*>          NFAIL(1) = No. of times residual ZTRSYL exceeds threshold THRESH
*>          NFAIL(2) = No. of times residual ZTRSYL3 exceeds threshold THRESH
*>          NFAIL(3) = No. of times ZTRSYL3 and ZTRSYL deviate
*> \endverbatim
*>
*> \param[out] RMAX
*> \verbatim
*>          RMAX is DOUBLE PRECISION array, dimension (2)
*>          RMAX(1) = Value of the largest test ratio of ZTRSYL
*>          RMAX(2) = Value of the largest test ratio of ZTRSYL3
*> \endverbatim
*>
*> \param[out] NINFO
*> \verbatim
*>          NINFO is INTEGER array, dimension (2)
*>          NINFO(1) = No. of times ZTRSYL returns an expected INFO
*>          NINFO(2) = No. of times ZTRSYL3 returns an expected INFO
*> \endverbatim
*>
*> \param[out] KNT
*> \verbatim
*>          KNT is INTEGER
*>          Total number of examples tested.
*> \endverbatim

*
*  -- LAPACK test routine --
      SUBROUTINE ZSYL01( THRESH, NFAIL, RMAX, NINFO, KNT )
      IMPLICIT NONE
*
*  -- LAPACK test routine --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*
*     .. Scalar Arguments ..
      INTEGER            KNT
      DOUBLE PRECISION   THRESH
*     ..
*     .. Array Arguments ..
      INTEGER            NFAIL( 3 ), NINFO( 2 )
      DOUBLE PRECISION   RMAX( 2 )
*     ..
*
*  =====================================================================
*     ..
*     .. Parameters ..
      COMPLEX*16         CONE
      PARAMETER          ( CONE = ( 1.0D0, 0.0D+0 ) )
      DOUBLE PRECISION   ONE, ZERO
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
      INTEGER            MAXM, MAXN, LDSWORK
      PARAMETER          ( MAXM = 185, MAXN = 192, LDSWORK = 36 )
*     ..
*     .. Local Scalars ..
      CHARACTER          TRANA, TRANB
      INTEGER            I, INFO, IINFO, ISGN, ITRANA, ITRANB, J, KLA,
     $                   KUA, KLB, KUB, M, N
      DOUBLE PRECISION   ANRM, BNRM, BIGNUM, EPS, RES, RES1,
     $                   SCALE, SCALE3, SMLNUM, TNRM, XNRM
      COMPLEX*16         RMUL
*     ..
*     .. Local Arrays ..
      COMPLEX*16         DUML( MAXM ), DUMR( MAXN ),
     $                   D( MAX( MAXM, MAXN ) )
      DOUBLE PRECISION   DUM( MAXN ), VM( 2 )
      INTEGER            ISEED( 4 ), IWORK( MAXM + MAXN + 2 )
*     ..
*     .. Allocatable Arrays ..
      INTEGER            AllocateStatus
      COMPLEX*16,       DIMENSION(:,:), ALLOCATABLE :: A, B, C, CC, X
      DOUBLE PRECISION, DIMENSION(:,:), ALLOCATABLE :: SWORK
*     ..
*     .. External Functions ..
      LOGICAL            DISNAN
      DOUBLE PRECISION   DLAMCH, ZLANGE
      EXTERNAL           DISNAN, DLAMCH, ZLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           ZLATMR, ZLACPY, ZGEMM, ZTRSYL, ZTRSYL3
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, MAX, SQRT
*     ..
*     .. Allocate memory dynamically ..
      ALLOCATE ( A( MAXM, MAXM ), STAT = AllocateStatus )
      IF( AllocateStatus /= 0 ) STOP "*** Not enough memory ***"
      ALLOCATE ( B( MAXN, MAXN ), STAT = AllocateStatus )
      IF( AllocateStatus /= 0 ) STOP "*** Not enough memory ***"
      ALLOCATE ( C( MAXM, MAXN ), STAT = AllocateStatus )
      IF( AllocateStatus /= 0 ) STOP "*** Not enough memory ***"
      ALLOCATE ( CC( MAXM, MAXN ), STAT = AllocateStatus )
      IF( AllocateStatus /= 0 ) STOP "*** Not enough memory ***"
      ALLOCATE ( X( MAXM, MAXN ), STAT = AllocateStatus )
      IF( AllocateStatus /= 0 ) STOP "*** Not enough memory ***"
      ALLOCATE ( SWORK( LDSWORK, 103 ), STAT = AllocateStatus )
      IF( AllocateStatus /= 0 ) STOP "*** Not enough memory ***"
*     ..
*     .. Executable Statements ..
*
*     Get machine parameters
*
      EPS = DLAMCH( 'P' )
      SMLNUM = DLAMCH( 'S' ) / EPS
      BIGNUM = ONE / SMLNUM
*
*     Expect INFO = 0
      VM( 1 ) = ONE
*     Expect INFO = 1
      VM( 2 ) = 0.05D+0
*
*     Begin test loop
*
      NINFO( 1 ) = 0
      NINFO( 2 ) = 0
      NFAIL( 1 ) = 0
      NFAIL( 2 ) = 0
      NFAIL( 3 ) = 0
      RMAX( 1 ) = ZERO
      RMAX( 2 ) = ZERO
      KNT = 0
      ISEED( 1 ) = 1
      ISEED( 2 ) = 1
      ISEED( 3 ) = 1
      ISEED( 4 ) = 1
      SCALE = ONE
      SCALE3 = ONE
      DO J = 1, 2
         DO ISGN = -1, 1, 2
*           Reset seed (overwritten by LATMR)
            ISEED( 1 ) = 1
            ISEED( 2 ) = 1
            ISEED( 3 ) = 1
            ISEED( 4 ) = 1
            DO M = 32, MAXM, 51
               KLA = 0
               KUA = M - 1
               CALL ZLATMR( M, M, 'S', ISEED, 'N', D,
     $                      6, ONE, CONE, 'T', 'N',
     $                      DUML, 1, ONE, DUMR, 1, ONE,
     $                      'N', IWORK, KLA, KUA, ZERO,
     $                      ONE, 'NO', A, MAXM, IWORK,
     $                      IINFO )
               DO I = 1, M
                  A( I, I ) = A( I, I ) * VM( J )
               END DO
               ANRM = ZLANGE( 'M', M, M, A, MAXM, DUM )
               DO N = 51, MAXN, 47
                  KLB = 0
                  KUB = N - 1
                  CALL ZLATMR( N, N, 'S', ISEED, 'N', D,
     $                         6, ONE, CONE, 'T', 'N',
     $                         DUML, 1, ONE, DUMR, 1, ONE,
     $                         'N', IWORK, KLB, KUB, ZERO,
     $                         ONE, 'NO', B, MAXN, IWORK,
     $                         IINFO )
                  DO I = 1, N
                     B( I, I ) = B( I, I ) * VM ( J )
                  END DO
                  BNRM = ZLANGE( 'M', N, N, B, MAXN, DUM )
                  TNRM = MAX( ANRM, BNRM )
                  CALL ZLATMR( M, N, 'S', ISEED, 'N', D,
     $                         6, ONE, CONE, 'T', 'N',
     $                         DUML, 1, ONE, DUMR, 1, ONE,
     $                         'N', IWORK, M, N, ZERO, ONE,
     $                         'NO', C, MAXM, IWORK, IINFO )
                  DO ITRANA = 1, 2
                     IF( ITRANA.EQ.1 )
     $                   TRANA = 'N'
                     IF( ITRANA.EQ.2 )
     $                   TRANA = 'C'
                     DO ITRANB = 1, 2
                        IF( ITRANB.EQ.1 )
     $                     TRANB = 'N'
                        IF( ITRANB.EQ.2 )
     $                     TRANB = 'C'
                        KNT = KNT + 1
*
                        CALL ZLACPY( 'All', M, N, C, MAXM, X, MAXM)
                        CALL ZLACPY( 'All', M, N, C, MAXM, CC, MAXM)
                        CALL ZTRSYL( TRANA, TRANB, ISGN, M, N, 
     $                               A, MAXM, B, MAXN, X, MAXM,
     $                               SCALE, IINFO )
                        IF( IINFO.NE.0 )
     $                     NINFO( 1 ) = NINFO( 1 ) + 1
                        XNRM = ZLANGE( 'M', M, N, X, MAXM, DUM )
                        RMUL = CONE
                        IF( XNRM.GT.ONE .AND. TNRM.GT.ONE ) THEN
                           IF( XNRM.GT.BIGNUM / TNRM ) THEN
                              RMUL = CONE / MAX( XNRM, TNRM )
                           END IF
                        END IF
                        CALL ZGEMM( TRANA, 'N', M, N, M, RMUL,
     $                              A, MAXM, X, MAXM, -SCALE*RMUL,
     $                              CC, MAXM )
                        CALL ZGEMM( 'N', TRANB, M, N, N,
     $                              DBLE( ISGN )*RMUL, X, MAXM, B,
     $                              MAXN, CONE, CC, MAXM )
                        RES1 = ZLANGE( 'M', M, N, CC, MAXM, DUM )
                        RES = RES1 / MAX( SMLNUM, SMLNUM*XNRM,
     $                        ( ( ABS( RMUL )*TNRM )*EPS )*XNRM )
                        IF( RES.GT.THRESH )
     $                     NFAIL( 1 ) = NFAIL( 1 ) + 1
                        IF( RES.GT.RMAX( 1 ) )
     $                     RMAX( 1 ) = RES
*
                        CALL ZLACPY( 'All', M, N, C, MAXM, X, MAXM )
                        CALL ZLACPY( 'All', M, N, C, MAXM, CC, MAXM )
                        CALL ZTRSYL3( TRANA, TRANB, ISGN, M, N,
     $                                A, MAXM, B, MAXN, X, MAXM,
     $                                SCALE3, SWORK, LDSWORK, INFO)
                        IF( INFO.NE.0 )
     $                     NINFO( 2 ) = NINFO( 2 ) + 1
                        XNRM = ZLANGE( 'M', M, N, X, MAXM, DUM )
                        RMUL = CONE
                        IF( XNRM.GT.ONE .AND. TNRM.GT.ONE ) THEN
                           IF( XNRM.GT.BIGNUM / TNRM ) THEN
                              RMUL = CONE / MAX( XNRM, TNRM )
                           END IF
                        END IF
                        CALL ZGEMM( TRANA, 'N', M, N, M, RMUL,
     $                              A, MAXM, X, MAXM, -SCALE3*RMUL,
     $                              CC, MAXM )
                        CALL ZGEMM( 'N', TRANB, M, N, N,
     $                              DBLE( ISGN )*RMUL, X, MAXM, B,
     $                              MAXN, CONE, CC, MAXM )
                        RES1 = ZLANGE( 'M', M, N, CC, MAXM, DUM )
                        RES = RES1 / MAX( SMLNUM, SMLNUM*XNRM,
     $                             ( ( ABS( RMUL )*TNRM )*EPS )*XNRM )
*                       Verify that TRSYL3 only flushes if TRSYL flushes (but
*                       there may be cases where TRSYL3 avoid flushing).
                        IF( SCALE3.EQ.ZERO .AND. SCALE.GT.ZERO .OR.
     $                      IINFO.NE.INFO ) THEN
                           NFAIL( 3 ) = NFAIL( 3 ) + 1
                        END IF
                        IF( RES.GT.THRESH .OR. DISNAN( RES ) )
     $                     NFAIL( 2 ) = NFAIL( 2 ) + 1
                        IF( RES.GT.RMAX( 2 ) )
     $                     RMAX( 2 ) = RES
                     END DO
                  END DO
               END DO
            END DO
         END DO
      END DO
*
      DEALLOCATE (A, STAT = AllocateStatus)
      DEALLOCATE (B, STAT = AllocateStatus)
      DEALLOCATE (C, STAT = AllocateStatus)
      DEALLOCATE (CC, STAT = AllocateStatus)
      DEALLOCATE (X, STAT = AllocateStatus)
      DEALLOCATE (SWORK, STAT = AllocateStatus)
*
      RETURN
*
*     End of ZSYL01
*
      END
