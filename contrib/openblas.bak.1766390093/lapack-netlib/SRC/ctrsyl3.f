*> \brief \b CTRSYL3
*
* Definition:
* ===========
*
*
*>  \par Purpose
*  =============
*>
*> \verbatim
*>
*>  CTRSYL3 solves the complex Sylvester matrix equation:
*>
*>     op(A)*X + X*op(B) = scale*C or
*>     op(A)*X - X*op(B) = scale*C,
*>
*>  where op(A) = A or A**H, and  A and B are both upper triangular. A is
*>  M-by-M and B is N-by-N; the right hand side C and the solution X are
*>  M-by-N; and scale is an output scale factor, set <= 1 to avoid
*>  overflow in X.
*>
*>  This is the block version of the algorithm.
*> \endverbatim
*
*  Arguments
*  =========
*
*> \param[in] TRANA
*> \verbatim
*>          TRANA is CHARACTER*1
*>          Specifies the option op(A):
*>          = 'N': op(A) = A    (No transpose)
*>          = 'C': op(A) = A**H (Conjugate transpose)
*> \endverbatim
*>
*> \param[in] TRANB
*> \verbatim
*>          TRANB is CHARACTER*1
*>          Specifies the option op(B):
*>          = 'N': op(B) = B    (No transpose)
*>          = 'C': op(B) = B**H (Conjugate transpose)
*> \endverbatim
*>
*> \param[in] ISGN
*> \verbatim
*>          ISGN is INTEGER
*>          Specifies the sign in the equation:
*>          = +1: solve op(A)*X + X*op(B) = scale*C
*>          = -1: solve op(A)*X - X*op(B) = scale*C
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The order of the matrix A, and the number of rows in the
*>          matrices X and C. M >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix B, and the number of columns in the
*>          matrices X and C. N >= 0.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA,M)
*>          The upper triangular matrix A.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A. LDA >= max(1,M).
*> \endverbatim
*>
*> \param[in] B
*> \verbatim
*>          B is COMPLEX array, dimension (LDB,N)
*>          The upper triangular matrix B.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B. LDB >= max(1,N).
*> \endverbatim
*>
*> \param[in,out] C
*> \verbatim
*>          C is COMPLEX array, dimension (LDC,N)
*>          On entry, the M-by-N right hand side matrix C.
*>          On exit, C is overwritten by the solution matrix X.
*> \endverbatim
*>
*> \param[in] LDC
*> \verbatim
*>          LDC is INTEGER
*>          The leading dimension of the array C. LDC >= max(1,M)
*> \endverbatim
*>
*> \param[out] SCALE
*> \verbatim
*>          SCALE is REAL
*>          The scale factor, scale, set <= 1 to avoid overflow in X.
*> \endverbatim
*>
*> \param[out] SWORK
*> \verbatim
*>          SWORK is REAL array, dimension (MAX(2, ROWS), MAX(1,COLS)).
*>          On exit, if INFO = 0, SWORK(1) returns the optimal value ROWS
*>          and SWORK(2) returns the optimal COLS.
*> \endverbatim
*>
*> \param[in] LDSWORK
*> \verbatim
*>          LDSWORK is INTEGER
*>          LDSWORK >= MAX(2,ROWS), where ROWS = ((M + NB - 1) / NB + 1)
*>          and NB is the optimal block size.
*>
*>          If LDSWORK = -1, then a workspace query is assumed; the routine
*>          only calculates the optimal dimensions of the SWORK matrix,
*>          returns these values as the first and second entry of the SWORK
*>          matrix, and no error message related LWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0: successful exit
*>          < 0: if INFO = -i, the i-th argument had an illegal value
*>          = 1: A and B have common or very close eigenvalues; perturbed
*>               values were used to solve the equation (but the matrices
*>               A and B are unchanged).
*> \endverbatim
*
*> \ingroup complexSYcomputational
*
*  =====================================================================
*  References:
*   E. S. Quintana-Orti and R. A. Van De Geijn (2003). Formal derivation of
*   algorithms: The triangular Sylvester equation, ACM Transactions
*   on Mathematical Software (TOMS), volume 29, pages 218--243.
*
*   A. Schwarz and C. C. Kjelgaard Mikkelsen (2020). Robust Task-Parallel
*   Solution of the Triangular Sylvester Equation. Lecture Notes in
*   Computer Science, vol 12043, pages 82--92, Springer.
*
*  Contributor:
*   Angelika Schwarz, Umea University, Sweden.
*
*  =====================================================================
      SUBROUTINE CTRSYL3( TRANA, TRANB, ISGN, M, N, A, LDA, B, LDB, C,
     $                    LDC, SCALE, SWORK, LDSWORK, INFO )
      IMPLICIT NONE
*
*     .. Scalar Arguments ..
      CHARACTER          TRANA, TRANB
      INTEGER            INFO, ISGN, LDA, LDB, LDC, LDSWORK, M, N
      REAL               SCALE
*     ..
*     .. Array Arguments ..
      COMPLEX            A( LDA, * ), B( LDB, * ), C( LDC, * )
      REAL               SWORK( LDSWORK, * )
*     ..
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
      COMPLEX            CONE
      PARAMETER          ( CONE = ( 1.0E+0, 0.0E+0 ) )
*     ..
*     .. Local Scalars ..
      LOGICAL            NOTRNA, NOTRNB, LQUERY
      INTEGER            AWRK, BWRK, I, I1, I2, IINFO, J, J1, J2, JJ,
     $                   K, K1, K2, L, L1, L2, LL, NBA, NB, NBB
      REAL               ANRM, BIGNUM, BNRM, CNRM, SCAL, SCALOC,
     $                   SCAMIN, SGN, XNRM, BUF, SMLNUM
      COMPLEX            CSGN
*     ..
*     .. Local Arrays ..
      REAL               WNRM( MAX( M, N ) )
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ILAENV
      REAL               CLANGE, SLAMCH, SLARMM
      EXTERNAL           CLANGE, ILAENV, LSAME, SLAMCH, SLARMM
*     ..
*     .. External Subroutines ..
      EXTERNAL           CSSCAL, CGEMM, CLASCL, CTRSYL, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, AIMAG, EXPONENT, MAX, MIN, REAL
*     ..
*     .. Executable Statements ..
*
*     Decode and Test input parameters
*
      NOTRNA = LSAME( TRANA, 'N' )
      NOTRNB = LSAME( TRANB, 'N' )
*
*     Use the same block size for all matrices.
*
      NB = MAX( 8, ILAENV( 1, 'CTRSYL', '', M, N, -1, -1) )
*
*     Compute number of blocks in A and B
*
      NBA = MAX( 1, (M + NB - 1) / NB )
      NBB = MAX( 1, (N + NB - 1) / NB )
*
*     Compute workspace
*
      INFO = 0
      LQUERY = ( LDSWORK.EQ.-1 )
      IF( LQUERY ) THEN
         LDSWORK = 2
         SWORK(1,1) = MAX( NBA, NBB )
         SWORK(2,1) = 2 * NBB + NBA
      END IF
*
*     Test the input arguments
*
      IF( .NOT.NOTRNA .AND. .NOT. LSAME( TRANA, 'C' ) ) THEN
         INFO = -1
      ELSE IF( .NOT.NOTRNB .AND. .NOT. LSAME( TRANB, 'C' ) ) THEN
         INFO = -2
      ELSE IF( ISGN.NE.1 .AND. ISGN.NE.-1 ) THEN
         INFO = -3
      ELSE IF( M.LT.0 ) THEN
         INFO = -4
      ELSE IF( N.LT.0 ) THEN
         INFO = -5
      ELSE IF( LDA.LT.MAX( 1, M ) ) THEN
         INFO = -7
      ELSE IF( LDB.LT.MAX( 1, N ) ) THEN
         INFO = -9
      ELSE IF( LDC.LT.MAX( 1, M ) ) THEN
         INFO = -11
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'CTRSYL3', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         RETURN
      END IF
*
*     Quick return if possible
*
      SCALE = ONE
      IF( M.EQ.0 .OR. N.EQ.0 )
     $   RETURN
*
*     Use unblocked code for small problems or if insufficient
*     workspace is provided
*
      IF( MIN( NBA, NBB ).EQ.1 .OR. LDSWORK.LT.MAX( NBA, NBB ) ) THEN
        CALL CTRSYL( TRANA, TRANB, ISGN, M, N, A, LDA, B, LDB,
     $               C, LDC, SCALE, INFO )
        RETURN
      END IF
*
*     Set constants to control overflow
*
      SMLNUM = SLAMCH( 'S' )
      BIGNUM = ONE / SMLNUM
*
*     Set local scaling factors.
*
      DO L = 1, NBB
         DO K = 1, NBA
            SWORK( K, L ) = ONE
         END DO
      END DO
*
*     Fallback scaling factor to prevent flushing of SWORK( K, L ) to zero.
*     This scaling is to ensure compatibility with TRSYL and may get flushed.
*
      BUF = ONE
*
*      Compute upper bounds of blocks of A and B
*
      AWRK = NBB
      DO K = 1, NBA
         K1 = (K - 1) * NB + 1
         K2 = MIN( K * NB, M ) + 1
         DO L = K, NBA
            L1 = (L - 1) * NB + 1
            L2 = MIN( L * NB, M ) + 1
            IF( NOTRNA ) THEN
               SWORK( K, AWRK + L ) = CLANGE( 'I', K2-K1, L2-L1,
     $                                        A( K1, L1 ), LDA, WNRM )
            ELSE
               SWORK( L, AWRK + K ) = CLANGE( '1', K2-K1, L2-L1,
     $                                        A( K1, L1 ), LDA, WNRM )
            END IF
         END DO
      END DO
      BWRK = NBB + NBA
      DO K = 1, NBB
         K1 = (K - 1) * NB + 1
         K2 = MIN( K * NB, N ) + 1
         DO L = K, NBB
            L1 = (L - 1) * NB + 1
            L2 = MIN( L * NB, N ) + 1
            IF( NOTRNB ) THEN
               SWORK( K, BWRK + L ) = CLANGE( 'I', K2-K1, L2-L1,
     $                                        B( K1, L1 ), LDB, WNRM )
            ELSE
               SWORK( L, BWRK + K ) = CLANGE( '1', K2-K1, L2-L1,
     $                                        B( K1, L1 ), LDB, WNRM )
            END IF
         END DO
      END DO
*
      SGN = REAL( ISGN )
      CSGN = CMPLX( SGN, ZERO )
*
      IF( NOTRNA .AND. NOTRNB ) THEN
*
*        Solve    A*X + ISGN*X*B = scale*C.
*
*        The (K,L)th block of X is determined starting from
*        bottom-left corner column by column by
*
*         A(K,K)*X(K,L) + ISGN*X(K,L)*B(L,L) = C(K,L) - R(K,L)
*
*        Where
*                  M                         L-1
*        R(K,L) = SUM [A(K,I)*X(I,L)] + ISGN*SUM [X(K,J)*B(J,L)].
*                I=K+1                       J=1
*
*        Start loop over block rows (index = K) and block columns (index = L)
*
         DO K = NBA, 1, -1
*
*           K1: row index of the first row in X( K, L )
*           K2: row index of the first row in X( K+1, L )
*           so the K2 - K1 is the column count of the block X( K, L )
*
            K1 = (K - 1) * NB + 1
            K2 = MIN( K * NB, M ) + 1
            DO L = 1, NBB
*
*              L1: column index of the first column in X( K, L )
*              L2: column index of the first column in X( K, L + 1)
*              so that L2 - L1 is the row count of the block X( K, L )
*
               L1 = (L - 1) * NB + 1
               L2 = MIN( L * NB, N ) + 1
*
               CALL CTRSYL( TRANA, TRANB, ISGN, K2-K1, L2-L1,
     $                      A( K1, K1 ), LDA,
     $                      B( L1, L1 ), LDB,
     $                      C( K1, L1 ), LDC, SCALOC, IINFO )
               INFO = MAX( INFO, IINFO )
*
               IF( SCALOC * SWORK( K, L ) .EQ. ZERO ) THEN
                  IF( SCALOC .EQ. ZERO ) THEN
*                    The magnitude of the largest entry of X(K1:K2-1, L1:L2-1)
*                    is larger than the product of BIGNUM**2 and cannot be
*                    represented in the form (1/SCALE)*X(K1:K2-1, L1:L2-1).
*                    Mark the computation as pointless.
                     BUF = ZERO
                  ELSE
*                    Use second scaling factor to prevent flushing to zero.
                     BUF = BUF*2.E0**EXPONENT( SCALOC )
                  END IF
                  DO JJ = 1, NBB
                     DO LL = 1, NBA
*                       Bound by BIGNUM to not introduce Inf. The value
*                       is irrelevant; corresponding entries of the
*                       solution will be flushed in consistency scaling.
                        SWORK( LL, JJ ) = MIN( BIGNUM,
     $                     SWORK( LL, JJ ) / 2.E0**EXPONENT( SCALOC ) )
                     END DO
                  END DO
               END IF
               SWORK( K, L ) = SCALOC * SWORK( K, L )
               XNRM = CLANGE( 'I', K2-K1, L2-L1, C( K1, L1 ), LDC,
     $                        WNRM )
*
               DO I = K - 1, 1, -1
*
*                 C( I, L ) := C( I, L ) - A( I, K ) * C( K, L )
*
                  I1 = (I - 1) * NB + 1
                  I2 = MIN( I * NB, M ) + 1
*
*                 Compute scaling factor to survive the linear update
*                 simulating consistent scaling.
*
                  CNRM = CLANGE( 'I', I2-I1, L2-L1, C( I1, L1 ),
     $                           LDC, WNRM )
                  SCAMIN = MIN( SWORK( I, L ), SWORK( K, L ) )
                  CNRM = CNRM * ( SCAMIN / SWORK( I, L ) )
                  XNRM = XNRM * ( SCAMIN / SWORK( K, L ) )
                  ANRM = SWORK( I, AWRK + K )
                  SCALOC = SLARMM( ANRM, XNRM, CNRM )
                  IF( SCALOC * SCAMIN .EQ. ZERO ) THEN
*                    Use second scaling factor to prevent flushing to zero.
                     BUF = BUF*2.E0**EXPONENT( SCALOC )
                     DO JJ = 1, NBB
                        DO LL = 1, NBA
                        SWORK( LL, JJ ) = MIN( BIGNUM,
     $                     SWORK( LL, JJ ) / 2.E0**EXPONENT( SCALOC ) )
                        END DO
                     END DO
                     SCAMIN = SCAMIN / 2.E0**EXPONENT( SCALOC )
                     SCALOC = SCALOC / 2.E0**EXPONENT( SCALOC )
                  END IF
                  CNRM = CNRM * SCALOC
                  XNRM = XNRM * SCALOC
*
*                 Simultaneously apply the robust update factor and the
*                 consistency scaling factor to C( I, L ) and C( K, L ).
*
                  SCAL = ( SCAMIN / SWORK( K, L ) ) * SCALOC
                  IF( SCAL.NE.ONE ) THEN
                      DO JJ = L1, L2-1
                         CALL CSSCAL( K2-K1, SCAL, C( K1, JJ ), 1)
                      END DO
                  ENDIF
*
                  SCAL = ( SCAMIN / SWORK( I, L ) ) * SCALOC
                  IF( SCAL.NE.ONE ) THEN
                      DO LL = L1, L2-1
                         CALL CSSCAL( I2-I1, SCAL, C( I1, LL ), 1)
                      END DO
                  ENDIF
*
*                 Record current scaling factor
*
                  SWORK( K, L ) = SCAMIN * SCALOC
                  SWORK( I, L ) = SCAMIN * SCALOC
*
                  CALL CGEMM( 'N', 'N', I2-I1, L2-L1, K2-K1, -CONE,
     $                        A( I1, K1 ), LDA, C( K1, L1 ), LDC,
     $                        CONE, C( I1, L1 ), LDC )
*
               END DO
*
               DO J = L + 1, NBB
*
*                 C( K, J ) := C( K, J ) - SGN * C( K, L ) * B( L, J )
*
                  J1 = (J - 1) * NB + 1
                  J2 = MIN( J * NB, N ) + 1
*
*                 Compute scaling factor to survive the linear update
*                 simulating consistent scaling.
*
                  CNRM = CLANGE( 'I', K2-K1, J2-J1, C( K1, J1 ),
     $                           LDC, WNRM )
                  SCAMIN = MIN( SWORK( K, J ), SWORK( K, L ) )
                  CNRM = CNRM * ( SCAMIN / SWORK( K, J ) )
                  XNRM = XNRM * ( SCAMIN / SWORK( K, L ) )
                  BNRM = SWORK(L, BWRK + J)
                  SCALOC = SLARMM( BNRM, XNRM, CNRM )
                  IF( SCALOC * SCAMIN .EQ. ZERO ) THEN
*                    Use second scaling factor to prevent flushing to zero.
                     BUF = BUF*2.E0**EXPONENT( SCALOC )
                     DO JJ = 1, NBB
                        DO LL = 1, NBA
                        SWORK( LL, JJ ) = MIN( BIGNUM,
     $                     SWORK( LL, JJ ) / 2.E0**EXPONENT( SCALOC ) )
                        END DO
                     END DO
                     SCAMIN = SCAMIN / 2.E0**EXPONENT( SCALOC )
                     SCALOC = SCALOC / 2.E0**EXPONENT( SCALOC )
                  END IF
                  CNRM = CNRM * SCALOC
                  XNRM = XNRM * SCALOC
*
*                 Simultaneously apply the robust update factor and the
*                 consistency scaling factor to C( K, J ) and C( K, L).
*
                  SCAL = ( SCAMIN / SWORK( K, L ) ) * SCALOC
                  IF( SCAL .NE. ONE ) THEN
                     DO LL = L1, L2-1
                        CALL CSSCAL( K2-K1, SCAL, C( K1, LL ), 1 )
                     END DO
                  ENDIF
*
                  SCAL = ( SCAMIN / SWORK( K, J ) ) * SCALOC
                  IF( SCAL .NE. ONE ) THEN
                      DO JJ = J1, J2-1
                         CALL CSSCAL( K2-K1, SCAL, C( K1, JJ ), 1 )
                      END DO
                  ENDIF
*
*                 Record current scaling factor
*
                  SWORK( K, L ) = SCAMIN * SCALOC
                  SWORK( K, J ) = SCAMIN * SCALOC
*
                  CALL CGEMM( 'N', 'N', K2-K1, J2-J1, L2-L1, -CSGN,
     $                        C( K1, L1 ), LDC, B( L1, J1 ), LDB,
     $                        CONE, C( K1, J1 ), LDC )
               END DO
            END DO
         END DO
      ELSE IF( .NOT.NOTRNA .AND. NOTRNB ) THEN
*
*        Solve    A**H *X + ISGN*X*B = scale*C.
*
*        The (K,L)th block of X is determined starting from
*        upper-left corner column by column by
*
*          A(K,K)**H*X(K,L) + ISGN*X(K,L)*B(L,L) = C(K,L) - R(K,L)
*
*        Where
*                   K-1                        L-1
*          R(K,L) = SUM [A(I,K)**H*X(I,L)] +ISGN*SUM [X(K,J)*B(J,L)]
*                   I=1                        J=1
*
*        Start loop over block rows (index = K) and block columns (index = L)
*
         DO K = 1, NBA
*
*           K1: row index of the first row in X( K, L )
*           K2: row index of the first row in X( K+1, L )
*           so the K2 - K1 is the column count of the block X( K, L )
*
            K1 = (K - 1) * NB + 1
            K2 = MIN( K * NB, M ) + 1
            DO L = 1, NBB
*
*              L1: column index of the first column in X( K, L )
*              L2: column index of the first column in X( K, L + 1)
*              so that L2 - L1 is the row count of the block X( K, L )
*
               L1 = (L - 1) * NB + 1
               L2 = MIN( L * NB, N ) + 1
*
               CALL CTRSYL( TRANA, TRANB, ISGN, K2-K1, L2-L1,
     $                      A( K1, K1 ), LDA,
     $                      B( L1, L1 ), LDB,
     $                      C( K1, L1 ), LDC, SCALOC, IINFO )
               INFO = MAX( INFO, IINFO )
*
               IF( SCALOC * SWORK( K, L ) .EQ. ZERO ) THEN
                  IF( SCALOC .EQ. ZERO ) THEN
*                    The magnitude of the largest entry of X(K1:K2-1, L1:L2-1)
*                    is larger than the product of BIGNUM**2 and cannot be
*                    represented in the form (1/SCALE)*X(K1:K2-1, L1:L2-1).
*                    Mark the computation as pointless.
                     BUF = ZERO
                  ELSE
*                    Use second scaling factor to prevent flushing to zero.
                     BUF = BUF*2.E0**EXPONENT( SCALOC )
                  END IF
                  DO JJ = 1, NBB
                     DO LL = 1, NBA
*                       Bound by BIGNUM to not introduce Inf. The value
*                       is irrelevant; corresponding entries of the
*                       solution will be flushed in consistency scaling.
                        SWORK( LL, JJ ) = MIN( BIGNUM,
     $                     SWORK( LL, JJ ) / 2.E0**EXPONENT( SCALOC ) )
                     END DO
                  END DO
               END IF
               SWORK( K, L ) = SCALOC * SWORK( K, L )
               XNRM = CLANGE( 'I', K2-K1, L2-L1, C( K1, L1 ), LDC,
     $                        WNRM )
*
               DO I = K + 1, NBA
*
*                 C( I, L ) := C( I, L ) - A( K, I )**H * C( K, L )
*
                  I1 = (I - 1) * NB + 1
                  I2 = MIN( I * NB, M ) + 1
*
*                 Compute scaling factor to survive the linear update
*                 simulating consistent scaling.
*
                  CNRM = CLANGE( 'I', I2-I1, L2-L1, C( I1, L1 ),
     $                           LDC, WNRM )
                  SCAMIN = MIN( SWORK( I, L ), SWORK( K, L ) )
                  CNRM = CNRM * ( SCAMIN / SWORK( I, L ) )
                  XNRM = XNRM * ( SCAMIN / SWORK( K, L ) )
                  ANRM = SWORK( I, AWRK + K )
                  SCALOC = SLARMM( ANRM, XNRM, CNRM )
                  IF( SCALOC * SCAMIN .EQ. ZERO ) THEN
*                    Use second scaling factor to prevent flushing to zero.
                     BUF = BUF*2.E0**EXPONENT( SCALOC )
                     DO JJ = 1, NBB
                        DO LL = 1, NBA
                        SWORK( LL, JJ ) = MIN( BIGNUM,
     $                     SWORK( LL, JJ ) / 2.E0**EXPONENT( SCALOC ) )
                        END DO
                     END DO
                     SCAMIN = SCAMIN / 2.E0**EXPONENT( SCALOC )
                     SCALOC = SCALOC / 2.E0**EXPONENT( SCALOC )
                  END IF
                  CNRM = CNRM * SCALOC
                  XNRM = XNRM * SCALOC
*
*                 Simultaneously apply the robust update factor and the
*                 consistency scaling factor to to C( I, L ) and C( K, L).
*
                  SCAL = ( SCAMIN / SWORK( K, L ) ) * SCALOC
                  IF( SCAL .NE. ONE ) THEN
                     DO LL = L1, L2-1
                        CALL CSSCAL( K2-K1, SCAL, C( K1, LL ), 1 )
                     END DO
                  ENDIF
*
                  SCAL = ( SCAMIN / SWORK( I, L ) ) * SCALOC
                  IF( SCAL .NE. ONE ) THEN
                     DO LL = L1, L2-1
                        CALL CSSCAL( I2-I1, SCAL, C( I1, LL ), 1 )
                     END DO
                  ENDIF
*
*                 Record current scaling factor
*
                  SWORK( K, L ) = SCAMIN * SCALOC
                  SWORK( I, L ) = SCAMIN * SCALOC
*
                  CALL CGEMM( 'C', 'N', I2-I1, L2-L1, K2-K1, -CONE,
     $                        A( K1, I1 ), LDA, C( K1, L1 ), LDC,
     $                        CONE, C( I1, L1 ), LDC )
               END DO
*
               DO J = L + 1, NBB
*
*                 C( K, J ) := C( K, J ) - SGN * C( K, L ) * B( L, J )
*
                  J1 = (J - 1) * NB + 1
                  J2 = MIN( J * NB, N ) + 1
*
*                 Compute scaling factor to survive the linear update
*                 simulating consistent scaling.
*
                  CNRM = CLANGE( 'I', K2-K1, J2-J1, C( K1, J1 ),
     $                           LDC, WNRM )
                  SCAMIN = MIN( SWORK( K, J ), SWORK( K, L ) )
                  CNRM = CNRM * ( SCAMIN / SWORK( K, J ) )
                  XNRM = XNRM * ( SCAMIN / SWORK( K, L ) )
                  BNRM = SWORK( L, BWRK + J )
                  SCALOC = SLARMM( BNRM, XNRM, CNRM )
                  IF( SCALOC * SCAMIN .EQ. ZERO ) THEN
*                    Use second scaling factor to prevent flushing to zero.
                     BUF = BUF*2.E0**EXPONENT( SCALOC )
                     DO JJ = 1, NBB
                        DO LL = 1, NBA
                        SWORK( LL, JJ ) = MIN( BIGNUM,
     $                     SWORK( LL, JJ ) / 2.E0**EXPONENT( SCALOC ) )
                        END DO
                     END DO
                     SCAMIN = SCAMIN / 2.E0**EXPONENT( SCALOC )
                     SCALOC = SCALOC / 2.E0**EXPONENT( SCALOC )
                  END IF
                  CNRM = CNRM * SCALOC
                  XNRM = XNRM * SCALOC
*
*                 Simultaneously apply the robust update factor and the
*                 consistency scaling factor to to C( K, J ) and C( K, L).
*
                  SCAL = ( SCAMIN / SWORK( K, L ) ) * SCALOC
                  IF( SCAL .NE. ONE ) THEN
                      DO LL = L1, L2-1
                         CALL CSSCAL( K2-K1, SCAL, C( K1, LL ), 1 )
                      END DO
                  ENDIF
*
                  SCAL = ( SCAMIN / SWORK( K, J ) ) * SCALOC
                  IF( SCAL .NE. ONE ) THEN
                     DO JJ = J1, J2-1
                        CALL CSSCAL( K2-K1, SCAL, C( K1, JJ ), 1 )
                     END DO
                  ENDIF
*
*                 Record current scaling factor
*
                  SWORK( K, L ) = SCAMIN * SCALOC
                  SWORK( K, J ) = SCAMIN * SCALOC
*
                  CALL CGEMM( 'N', 'N', K2-K1, J2-J1, L2-L1, -CSGN,
     $                        C( K1, L1 ), LDC, B( L1, J1 ), LDB,
     $                        CONE, C( K1, J1 ), LDC )
               END DO
            END DO
         END DO
      ELSE IF( .NOT.NOTRNA .AND. .NOT.NOTRNB ) THEN
*
*        Solve    A**H *X + ISGN*X*B**H = scale*C.
*
*        The (K,L)th block of X is determined starting from
*        top-right corner column by column by
*
*           A(K,K)**H*X(K,L) + ISGN*X(K,L)*B(L,L)**H = C(K,L) - R(K,L)
*
*        Where
*                     K-1                          N
*            R(K,L) = SUM [A(I,K)**H*X(I,L)] + ISGN*SUM [X(K,J)*B(L,J)**H].
*                     I=1                        J=L+1
*
*        Start loop over block rows (index = K) and block columns (index = L)
*
         DO K = 1, NBA
*
*           K1: row index of the first row in X( K, L )
*           K2: row index of the first row in X( K+1, L )
*           so the K2 - K1 is the column count of the block X( K, L )
*
            K1 = (K - 1) * NB + 1
            K2 = MIN( K * NB, M ) + 1
            DO L = NBB, 1, -1
*
*              L1: column index of the first column in X( K, L )
*              L2: column index of the first column in X( K, L + 1)
*              so that L2 - L1 is the row count of the block X( K, L )
*
               L1 = (L - 1) * NB + 1
               L2 = MIN( L * NB, N ) + 1
*
               CALL CTRSYL( TRANA, TRANB, ISGN, K2-K1, L2-L1,
     $                      A( K1, K1 ), LDA,
     $                      B( L1, L1 ), LDB,
     $                      C( K1, L1 ), LDC, SCALOC, IINFO )
               INFO = MAX( INFO, IINFO )
*
               IF( SCALOC * SWORK( K, L ) .EQ. ZERO ) THEN
                  IF( SCALOC .EQ. ZERO ) THEN
*                    The magnitude of the largest entry of X(K1:K2-1, L1:L2-1)
*                    is larger than the product of BIGNUM**2 and cannot be
*                    represented in the form (1/SCALE)*X(K1:K2-1, L1:L2-1).
*                    Mark the computation as pointless.
                     BUF = ZERO
                  ELSE
*                    Use second scaling factor to prevent flushing to zero.
                     BUF = BUF*2.E0**EXPONENT( SCALOC )
                  END IF
                  DO JJ = 1, NBB
                     DO LL = 1, NBA
*                       Bound by BIGNUM to not introduce Inf. The value
*                       is irrelevant; corresponding entries of the
*                       solution will be flushed in consistency scaling.
                        SWORK( LL, JJ ) = MIN( BIGNUM,
     $                     SWORK( LL, JJ ) / 2.E0**EXPONENT( SCALOC ) )
                     END DO
                  END DO
               END IF
               SWORK( K, L ) = SCALOC * SWORK( K, L )
               XNRM = CLANGE( 'I', K2-K1, L2-L1, C( K1, L1 ), LDC,
     $                        WNRM )
*
               DO I = K + 1, NBA
*
*                 C( I, L ) := C( I, L ) - A( K, I )**H * C( K, L )
*
                  I1 = (I - 1) * NB + 1
                  I2 = MIN( I * NB, M ) + 1
*
*                 Compute scaling factor to survive the linear update
*                 simulating consistent scaling.
*
                  CNRM = CLANGE( 'I', I2-I1, L2-L1, C( I1, L1 ),
     $                           LDC, WNRM )
                  SCAMIN = MIN( SWORK( I, L ), SWORK( K, L ) )
                  CNRM = CNRM * ( SCAMIN / SWORK( I, L ) )
                  XNRM = XNRM * ( SCAMIN / SWORK( K, L ) )
                  ANRM = SWORK( I, AWRK + K )
                  SCALOC = SLARMM( ANRM, XNRM, CNRM )
                  IF( SCALOC * SCAMIN .EQ. ZERO ) THEN
*                    Use second scaling factor to prevent flushing to zero.
                     BUF = BUF*2.E0**EXPONENT( SCALOC )
                     DO JJ = 1, NBB
                        DO LL = 1, NBA
                        SWORK( LL, JJ ) = MIN( BIGNUM,
     $                     SWORK( LL, JJ ) / 2.E0**EXPONENT( SCALOC ) )
                        END DO
                     END DO
                     SCAMIN = SCAMIN / 2.E0**EXPONENT( SCALOC )
                     SCALOC = SCALOC / 2.E0**EXPONENT( SCALOC )
                  END IF
                  CNRM = CNRM * SCALOC
                  XNRM = XNRM * SCALOC
*
*                 Simultaneously apply the robust update factor and the
*                 consistency scaling factor to C( I, L ) and C( K, L).
*
                  SCAL = ( SCAMIN / SWORK( K, L ) ) * SCALOC
                  IF( SCAL .NE. ONE ) THEN
                     DO LL = L1, L2-1
                        CALL CSSCAL( K2-K1, SCAL, C( K1, LL ), 1 )
                     END DO
                  ENDIF
*
                  SCAL = ( SCAMIN / SWORK( I, L ) ) * SCALOC
                  IF( SCAL .NE. ONE ) THEN
                     DO LL = L1, L2-1
                        CALL CSSCAL( I2-I1, SCAL, C( I1, LL ), 1 )
                     END DO
                  ENDIF
*
*                 Record current scaling factor
*
                  SWORK( K, L ) = SCAMIN * SCALOC
                  SWORK( I, L ) = SCAMIN * SCALOC
*
                  CALL CGEMM( 'C', 'N', I2-I1, L2-L1, K2-K1, -CONE,
     $                        A( K1, I1 ), LDA, C( K1, L1 ), LDC,
     $                        CONE, C( I1, L1 ), LDC )
               END DO
*
               DO J = 1, L - 1
*
*                 C( K, J ) := C( K, J ) - SGN * C( K, L ) * B( J, L )**H
*
                  J1 = (J - 1) * NB + 1
                  J2 = MIN( J * NB, N ) + 1
*
*                 Compute scaling factor to survive the linear update
*                 simulating consistent scaling.
*
                  CNRM = CLANGE( 'I', K2-K1, J2-J1, C( K1, J1 ),
     $                           LDC, WNRM )
                  SCAMIN = MIN( SWORK( K, J ), SWORK( K, L ) )
                  CNRM = CNRM * ( SCAMIN / SWORK( K, J ) )
                  XNRM = XNRM * ( SCAMIN / SWORK( K, L ) )
                  BNRM = SWORK( L, BWRK + J )
                  SCALOC = SLARMM( BNRM, XNRM, CNRM )
                  IF( SCALOC * SCAMIN .EQ. ZERO ) THEN
*                    Use second scaling factor to prevent flushing to zero.
                     BUF = BUF*2.E0**EXPONENT( SCALOC )
                     DO JJ = 1, NBB
                        DO LL = 1, NBA
                        SWORK( LL, JJ ) = MIN( BIGNUM,
     $                     SWORK( LL, JJ ) / 2.E0**EXPONENT( SCALOC ) )
                        END DO
                     END DO
                     SCAMIN = SCAMIN / 2.E0**EXPONENT( SCALOC )
                     SCALOC = SCALOC / 2.E0**EXPONENT( SCALOC )
                  END IF
                  CNRM = CNRM * SCALOC
                  XNRM = XNRM * SCALOC
*
*                 Simultaneously apply the robust update factor and the
*                 consistency scaling factor to C( K, J ) and C( K, L).
*
                  SCAL = ( SCAMIN / SWORK( K, L ) ) * SCALOC
                  IF( SCAL .NE. ONE ) THEN
                     DO LL = L1, L2-1
                        CALL CSSCAL( K2-K1, SCAL, C( K1, LL ), 1)
                     END DO
                  ENDIF
*
                  SCAL = ( SCAMIN / SWORK( K, J ) ) * SCALOC
                  IF( SCAL .NE. ONE ) THEN
                     DO JJ = J1, J2-1
                        CALL CSSCAL( K2-K1, SCAL, C( K1, JJ ), 1 )
                     END DO
                  ENDIF
*
*                 Record current scaling factor
*
                  SWORK( K, L ) = SCAMIN * SCALOC
                  SWORK( K, J ) = SCAMIN * SCALOC
*
                  CALL CGEMM( 'N', 'C', K2-K1, J2-J1, L2-L1, -CSGN,
     $                        C( K1, L1 ), LDC, B( J1, L1 ), LDB,
     $                        CONE, C( K1, J1 ), LDC )
               END DO
            END DO
         END DO
      ELSE IF( NOTRNA .AND. .NOT.NOTRNB ) THEN
*
*        Solve    A*X + ISGN*X*B**H = scale*C.
*
*        The (K,L)th block of X is determined starting from
*        bottom-right corner column by column by
*
*            A(K,K)*X(K,L) + ISGN*X(K,L)*B(L,L)**H = C(K,L) - R(K,L)
*
*        Where
*                      M                          N
*            R(K,L) = SUM [A(K,I)*X(I,L)] + ISGN*SUM [X(K,J)*B(L,J)**H].
*                    I=K+1                      J=L+1
*
*        Start loop over block rows (index = K) and block columns (index = L)
*
         DO K = NBA, 1, -1
*
*           K1: row index of the first row in X( K, L )
*           K2: row index of the first row in X( K+1, L )
*           so the K2 - K1 is the column count of the block X( K, L )
*
            K1 = (K - 1) * NB + 1
            K2 = MIN( K * NB, M ) + 1
            DO L = NBB, 1, -1
*
*              L1: column index of the first column in X( K, L )
*              L2: column index of the first column in X( K, L + 1)
*              so that L2 - L1 is the row count of the block X( K, L )
*
               L1 = (L - 1) * NB + 1
               L2 = MIN( L * NB, N ) + 1
*
               CALL CTRSYL( TRANA, TRANB, ISGN, K2-K1, L2-L1,
     $                      A( K1, K1 ), LDA,
     $                      B( L1, L1 ), LDB,
     $                      C( K1, L1 ), LDC, SCALOC, IINFO )
               INFO = MAX( INFO, IINFO )
*
               IF( SCALOC * SWORK( K, L ) .EQ. ZERO ) THEN
                  IF( SCALOC .EQ. ZERO ) THEN
*                    The magnitude of the largest entry of X(K1:K2-1, L1:L2-1)
*                    is larger than the product of BIGNUM**2 and cannot be
*                    represented in the form (1/SCALE)*X(K1:K2-1, L1:L2-1).
*                    Mark the computation as pointless.
                     BUF = ZERO
                  ELSE
*                    Use second scaling factor to prevent flushing to zero.
                     BUF = BUF*2.E0**EXPONENT( SCALOC )
                  END IF
                  DO JJ = 1, NBB
                     DO LL = 1, NBA
*                       Bound by BIGNUM to not introduce Inf. The value
*                       is irrelevant; corresponding entries of the
*                       solution will be flushed in consistency scaling.
                        SWORK( LL, JJ ) = MIN( BIGNUM,
     $                     SWORK( LL, JJ ) / 2.E0**EXPONENT( SCALOC ) )
                     END DO
                  END DO
               END IF
               SWORK( K, L ) = SCALOC * SWORK( K, L )
               XNRM = CLANGE( 'I', K2-K1, L2-L1, C( K1, L1 ), LDC,
     $                        WNRM )
*
               DO I = 1, K - 1
*
*                 C( I, L ) := C( I, L ) - A( I, K ) * C( K, L )
*
                  I1 = (I - 1) * NB + 1
                  I2 = MIN( I * NB, M ) + 1
*
*                 Compute scaling factor to survive the linear update
*                 simulating consistent scaling.
*
                  CNRM = CLANGE( 'I', I2-I1, L2-L1, C( I1, L1 ),
     $                           LDC, WNRM )
                  SCAMIN = MIN( SWORK( I, L ), SWORK( K, L ) )
                  CNRM = CNRM * ( SCAMIN / SWORK( I, L ) )
                  XNRM = XNRM * ( SCAMIN / SWORK( K, L ) )
                  ANRM = SWORK( I, AWRK + K )
                  SCALOC = SLARMM( ANRM, XNRM, CNRM )
                  IF( SCALOC * SCAMIN .EQ. ZERO ) THEN
*                    Use second scaling factor to prevent flushing to zero.
                     BUF = BUF*2.E0**EXPONENT( SCALOC )
                     DO JJ = 1, NBB
                        DO LL = 1, NBA
                        SWORK( LL, JJ ) = MIN( BIGNUM,
     $                     SWORK( LL, JJ ) / 2.E0**EXPONENT( SCALOC ) )
                        END DO
                     END DO
                     SCAMIN = SCAMIN / 2.E0**EXPONENT( SCALOC )
                     SCALOC = SCALOC / 2.E0**EXPONENT( SCALOC )
                  END IF
                  CNRM = CNRM * SCALOC
                  XNRM = XNRM * SCALOC
*
*                 Simultaneously apply the robust update factor and the
*                 consistency scaling factor to C( I, L ) and C( K, L).
*
                  SCAL = ( SCAMIN / SWORK( K, L ) ) * SCALOC
                  IF( SCAL .NE. ONE ) THEN
                     DO LL = L1, L2-1
                        CALL CSSCAL( K2-K1, SCAL, C( K1, LL ), 1 )
                     END DO
                  ENDIF
*
                  SCAL = ( SCAMIN / SWORK( I, L ) ) * SCALOC
                  IF( SCAL .NE. ONE ) THEN
                     DO LL = L1, L2-1
                        CALL CSSCAL( I2-I1, SCAL, C( I1, LL ), 1 )
                     END DO
                  ENDIF
*
*                 Record current scaling factor
*
                  SWORK( K, L ) = SCAMIN * SCALOC
                  SWORK( I, L ) = SCAMIN * SCALOC
*
                  CALL CGEMM( 'N', 'N', I2-I1, L2-L1, K2-K1, -CONE,
     $                        A( I1, K1 ), LDA, C( K1, L1 ), LDC,
     $                        CONE, C( I1, L1 ), LDC )
*
               END DO
*
               DO J = 1, L - 1
*
*                 C( K, J ) := C( K, J ) - SGN * C( K, L ) * B( J, L )**H
*
                  J1 = (J - 1) * NB + 1
                  J2 = MIN( J * NB, N ) + 1
*
*                 Compute scaling factor to survive the linear update
*                 simulating consistent scaling.
*
                  CNRM = CLANGE( 'I', K2-K1, J2-J1, C( K1, J1 ),
     $                           LDC, WNRM )
                  SCAMIN = MIN( SWORK( K, J ), SWORK( K, L ) )
                  CNRM = CNRM * ( SCAMIN / SWORK( K, J ) )
                  XNRM = XNRM * ( SCAMIN / SWORK( K, L ) )
                  BNRM = SWORK( L, BWRK + J )
                  SCALOC = SLARMM( BNRM, XNRM, CNRM )
                  IF( SCALOC * SCAMIN .EQ. ZERO ) THEN
*                    Use second scaling factor to prevent flushing to zero.
                     BUF = BUF*2.E0**EXPONENT( SCALOC )
                     DO JJ = 1, NBB
                        DO LL = 1, NBA
                        SWORK( LL, JJ ) = MIN( BIGNUM,
     $                     SWORK( LL, JJ ) / 2.E0**EXPONENT( SCALOC ) )
                        END DO
                     END DO
                     SCAMIN = SCAMIN / 2.E0**EXPONENT( SCALOC )
                     SCALOC = SCALOC / 2.E0**EXPONENT( SCALOC )
                  END IF
                  CNRM = CNRM * SCALOC 
                  XNRM = XNRM * SCALOC
*
*                 Simultaneously apply the robust update factor and the
*                 consistency scaling factor to C( K, J ) and C( K, L).
*
                  SCAL = ( SCAMIN / SWORK( K, L ) ) * SCALOC
                  IF( SCAL .NE. ONE ) THEN
                     DO JJ = L1, L2-1
                        CALL CSSCAL( K2-K1, SCAL, C( K1, JJ ), 1 )
                     END DO
                  ENDIF
*
                  SCAL = ( SCAMIN / SWORK( K, J ) ) * SCALOC
                  IF( SCAL .NE. ONE ) THEN
                     DO JJ = J1, J2-1
                        CALL CSSCAL( K2-K1, SCAL, C( K1, JJ ), 1 )
                     END DO
                  ENDIF
*
*                 Record current scaling factor
*
                  SWORK( K, L ) = SCAMIN * SCALOC
                  SWORK( K, J ) = SCAMIN * SCALOC
*
                  CALL CGEMM( 'N', 'C', K2-K1, J2-J1, L2-L1, -CSGN,
     $                        C( K1, L1 ), LDC, B( J1, L1 ), LDB,
     $                        CONE, C( K1, J1 ), LDC )
               END DO
            END DO
         END DO
*
      END IF
*
*     Reduce local scaling factors
*
      SCALE = SWORK( 1, 1 )
      DO K = 1, NBA
         DO L = 1, NBB
            SCALE = MIN( SCALE, SWORK( K, L ) )
         END DO
      END DO
      IF( SCALE .EQ. ZERO ) THEN
*
*        The magnitude of the largest entry of the solution is larger
*        than the product of BIGNUM**2 and cannot be represented in the
*        form (1/SCALE)*X if SCALE is REAL. Set SCALE to
*        zero and give up.
*
         SWORK(1,1) = MAX( NBA, NBB )
         SWORK(2,1) = 2 * NBB + NBA
         RETURN
      END IF
*
*     Realize consistent scaling
*
      DO K = 1, NBA
         K1 = (K - 1) * NB + 1
         K2 = MIN( K * NB, M ) + 1
         DO L = 1, NBB
            L1 = (L - 1) * NB + 1
            L2 = MIN( L * NB, N ) + 1
            SCAL = SCALE / SWORK( K, L )
            IF( SCAL .NE. ONE ) THEN
               DO LL = L1, L2-1
                  CALL CSSCAL( K2-K1, SCAL, C( K1, LL ), 1 )
               END DO
            ENDIF
         END DO
      END DO
*
      IF( BUF .NE. ONE .AND. BUF.GT.ZERO ) THEN
*
*        Decrease SCALE as much as possible.
*
         SCALOC = MIN( SCALE / SMLNUM, ONE / BUF )
         BUF = BUF * SCALOC
         SCALE = SCALE / SCALOC
      END IF
*
      IF( BUF.NE.ONE .AND. BUF.GT.ZERO ) THEN
*
*        In case of overly aggressive scaling during the computation,
*        flushing of the global scale factor may be prevented by
*        undoing some of the scaling. This step is to ensure that
*        this routine flushes only scale factors that TRSYL also
*        flushes and be usable as a drop-in replacement.
*
*        How much can the normwise largest entry be upscaled?
*
         SCAL = MAX( ABS( REAL( C( 1, 1 ) ) ),
     $               ABS( AIMAG( C ( 1, 1 ) ) ) )
         DO K = 1, M
            DO L = 1, N
               SCAL = MAX( SCAL, ABS( REAL ( C( K, L ) ) ),
     $                     ABS( AIMAG ( C( K, L ) ) ) )
            END DO
         END DO
*
*        Increase BUF as close to 1 as possible and apply scaling.
*
         SCALOC = MIN( BIGNUM / SCAL, ONE / BUF )
         BUF = BUF * SCALOC
         CALL CLASCL( 'G', -1, -1, ONE, SCALOC, M, N, C, LDC, IINFO )
      END IF
*
*     Combine with buffer scaling factor. SCALE will be flushed if
*     BUF is less than one here.
*
      SCALE = SCALE * BUF
*
*     Restore workspace dimensions
*
      SWORK(1,1) = MAX( NBA, NBB )
      SWORK(2,1) = 2 * NBB + NBA
*
      RETURN
*
*     End of CTRSYL3
*
      END
