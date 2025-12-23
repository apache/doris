*> \brief \b DSYL01
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DSYL01( THRESH, NFAIL, RMAX, NINFO, KNT )
*
*       .. Scalar Arguments ..
*       INTEGER            KNT
*       DOUBLE PRECISION   THRESH
*       ..
*       .. Array Arguments ..
*       INTEGER            NFAIL( 3 ), NINFO( 2 )
*       DOUBLE PRECISION   RMAX( 2 )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DSYL01 tests DTRSYL and DTRSYL3, routines for solving the Sylvester matrix
*> equation
*>
*>    op(A)*X + ISGN*X*op(B) = scale*C,
*>
*> A and B are assumed to be in Schur canonical form, op() represents an
*> optional transpose, and ISGN can be -1 or +1.  Scale is an output
*> less than or equal to 1, chosen to avoid overflow in X.
*>
*> The test code verifies that the following residual does not exceed
*> the provided threshold:
*>
*>    norm(op(A)*X + ISGN*X*op(B) - scale*C) /
*>        (EPS*max(norm(A),norm(B))*norm(X))
*>
*> This routine complements DGET35 by testing with larger,
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
*>          NFAIL(1) = No. of times residual DTRSYL exceeds threshold THRESH
*>          NFAIL(2) = No. of times residual DTRSYL3 exceeds threshold THRESH
*>          NFAIL(3) = No. of times DTRSYL3 and DTRSYL deviate
*> \endverbatim
*>
*> \param[out] RMAX
*> \verbatim
*>          RMAX is DOUBLE PRECISION, dimension (2)
*>          RMAX(1) = Value of the largest test ratio of DTRSYL
*>          RMAX(2) = Value of the largest test ratio of DTRSYL3
*> \endverbatim
*>
*> \param[out] NINFO
*> \verbatim
*>          NINFO is INTEGER array, dimension (2)
*>          NINFO(1) = No. of times DTRSYL returns an expected INFO
*>          NINFO(2) = No. of times DTRSYL3 returns an expected INFO
*> \endverbatim
*>
*> \param[out] KNT
*> \verbatim
*>          KNT is INTEGER
*>          Total number of examples tested.
*> \endverbatim

*
*  -- LAPACK test routine --
      SUBROUTINE DSYL01( THRESH, NFAIL, RMAX, NINFO, KNT )
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
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D0, ONE = 1.0D0 )
      INTEGER            MAXM, MAXN, LDSWORK
      PARAMETER          ( MAXM = 245, MAXN = 192, LDSWORK = 36 )
*     ..
*     .. Local Scalars ..
      CHARACTER          TRANA, TRANB
      INTEGER            I, INFO, IINFO, ISGN, ITRANA, ITRANB, J, KLA,
     $                   KUA, KLB, KUB, LIWORK, M, N
      DOUBLE PRECISION   ANRM, BNRM, BIGNUM, EPS, RES, RES1, RMUL,
     $                   SCALE, SCALE3, SMLNUM, TNRM, XNRM
*     ..
*     .. Local Arrays ..
      DOUBLE PRECISION   DUML( MAXM ), DUMR( MAXN ),
     $                   D( MAX( MAXM, MAXN ) ), DUM( MAXN ),
     $                   VM( 2 )
      INTEGER            ISEED( 4 ), IWORK( MAXM + MAXN + 2 )
*     ..
*     .. Allocatable Arrays ..
      INTEGER            AllocateStatus
      DOUBLE PRECISION, DIMENSION(:,:), ALLOCATABLE :: A, B, C, CC, X,
     $                   SWORK
*     ..
*     .. External Functions ..
      LOGICAL            DISNAN
      DOUBLE PRECISION   DLAMCH, DLANGE
      EXTERNAL           DLAMCH, DLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           DLATMR, DLACPY, DGEMM, DTRSYL, DTRSYL3
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, MAX
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
      ALLOCATE ( SWORK( LDSWORK, 126 ), STAT = AllocateStatus )
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
      VM( 1 ) = ONE
      VM( 2 ) = 0.000001D+0
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
      DO I = 1, 4
         ISEED( I ) = 1
      END DO
      SCALE = ONE
      SCALE3 = ONE
      LIWORK = MAXM + MAXN + 2
      DO J = 1, 2
         DO ISGN = -1, 1, 2
*           Reset seed (overwritten by LATMR)
            DO I = 1, 4
               ISEED( I ) = 1
            END DO
            DO M = 32, MAXM, 71
               KLA = 0
               KUA = M - 1
               CALL DLATMR( M, M, 'S', ISEED, 'N', D,
     $                      6, ONE, ONE, 'T', 'N',
     $                      DUML, 1, ONE, DUMR, 1, ONE,
     $                      'N', IWORK, KLA, KUA, ZERO,
     $                      ONE, 'NO', A, MAXM, IWORK, IINFO )
               DO I = 1, M
                  A( I, I ) = A( I, I ) * VM( J )
               END DO
               ANRM = DLANGE( 'M', M, M, A, MAXM, DUM )
               DO N = 51, MAXN, 47
                  KLB = 0
                  KUB = N - 1
                  CALL DLATMR( N, N, 'S', ISEED, 'N', D,
     $                         6, ONE, ONE, 'T', 'N',
     $                         DUML, 1, ONE, DUMR, 1, ONE,
     $                         'N', IWORK, KLB, KUB, ZERO,
     $                         ONE, 'NO', B, MAXN, IWORK, IINFO )
                  BNRM = DLANGE( 'M', N, N, B, MAXN, DUM )
                  TNRM = MAX( ANRM, BNRM )
                  CALL DLATMR( M, N, 'S', ISEED, 'N', D,
     $                         6, ONE, ONE, 'T', 'N',
     $                         DUML, 1, ONE, DUMR, 1, ONE,
     $                         'N', IWORK, M, N, ZERO, ONE,
     $                         'NO', C, MAXM, IWORK, IINFO )
                  DO ITRANA = 1, 2
                     IF( ITRANA.EQ.1 ) THEN
                        TRANA = 'N'
                     END IF
                     IF( ITRANA.EQ.2 ) THEN
                        TRANA = 'T'
                     END IF
                     DO ITRANB = 1, 2
                        IF( ITRANB.EQ.1 ) THEN
                           TRANB = 'N'
                        END IF
                        IF( ITRANB.EQ.2 ) THEN
                           TRANB = 'T'
                        END IF
                        KNT = KNT + 1
*
                        CALL DLACPY( 'All', M, N, C, MAXM, X, MAXM)
                        CALL DLACPY( 'All', M, N, C, MAXM, CC, MAXM)
                        CALL DTRSYL( TRANA, TRANB, ISGN, M, N, 
     $                               A, MAXM, B, MAXN, X, MAXM,
     $                               SCALE, IINFO )
                        IF( IINFO.NE.0 )
     $                     NINFO( 1 ) = NINFO( 1 ) + 1
                        XNRM = DLANGE( 'M', M, N, X, MAXM, DUM )
                        RMUL = ONE
                        IF( XNRM.GT.ONE .AND. TNRM.GT.ONE ) THEN
                           IF( XNRM.GT.BIGNUM / TNRM ) THEN
                              RMUL = ONE / MAX( XNRM, TNRM )
                           END IF
                        END IF
                        CALL DGEMM( TRANA, 'N', M, N, M, RMUL,
     $                              A, MAXM, X, MAXM, -SCALE*RMUL,
     $                              CC, MAXM )
                        CALL DGEMM( 'N', TRANB, M, N, N,
     $                               DBLE( ISGN )*RMUL, X, MAXM, B,
     $                               MAXN, ONE, CC, MAXM )
                        RES1 = DLANGE( 'M', M, N, CC, MAXM, DUM )
                        RES = RES1 / MAX( SMLNUM, SMLNUM*XNRM,
     $                              ( ( RMUL*TNRM )*EPS )*XNRM )
                        IF( RES.GT.THRESH )
     $                     NFAIL( 1 ) = NFAIL( 1 ) + 1
                        IF( RES.GT.RMAX( 1 ) )
     $                     RMAX( 1 ) = RES
*
                        CALL DLACPY( 'All', M, N, C, MAXM, X, MAXM )
                        CALL DLACPY( 'All', M, N, C, MAXM, CC, MAXM )
                        CALL DTRSYL3( TRANA, TRANB, ISGN, M, N,
     $                                A, MAXM, B, MAXN, X, MAXM,
     $                                SCALE3, IWORK, LIWORK,
     $                                SWORK, LDSWORK, INFO)
                        IF( INFO.NE.0 )
     $                     NINFO( 2 ) = NINFO( 2 ) + 1
                        XNRM = DLANGE( 'M', M, N, X, MAXM, DUM )
                        RMUL = ONE
                        IF( XNRM.GT.ONE .AND. TNRM.GT.ONE ) THEN
                           IF( XNRM.GT.BIGNUM / TNRM ) THEN
                              RMUL = ONE / MAX( XNRM, TNRM )
                           END IF
                        END IF
                        CALL DGEMM( TRANA, 'N', M, N, M, RMUL,
     $                              A, MAXM, X, MAXM, -SCALE3*RMUL,
     $                              CC, MAXM )
                        CALL DGEMM( 'N', TRANB, M, N, N,
     $                              DBLE( ISGN )*RMUL, X, MAXM, B,
     $                              MAXN, ONE, CC, MAXM )
                        RES1 = DLANGE( 'M', M, N, CC, MAXM, DUM )
                        RES = RES1 / MAX( SMLNUM, SMLNUM*XNRM,
     $                             ( ( RMUL*TNRM )*EPS )*XNRM )
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
*     End of DSYL01
*
      END
