*> \brief \b DTRSYL3
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
*>  DTRSYL3 solves the real Sylvester matrix equation:
*>
*>     op(A)*X + X*op(B) = scale*C or
*>     op(A)*X - X*op(B) = scale*C,
*>
*>  where op(A) = A or A**T, and  A and B are both upper quasi-
*>  triangular. A is M-by-M and B is N-by-N; the right hand side C and
*>  the solution X are M-by-N; and scale is an output scale factor, set
*>  <= 1 to avoid overflow in X.
*>
*>  A and B must be in Schur canonical form (as returned by DHSEQR), that
*>  is, block upper triangular with 1-by-1 and 2-by-2 diagonal blocks;
*>  each 2-by-2 diagonal block has its diagonal elements equal and its
*>  off-diagonal elements of opposite sign.
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
*>          = 'T': op(A) = A**T (Transpose)
*>          = 'C': op(A) = A**H (Conjugate transpose = Transpose)
*> \endverbatim
*>
*> \param[in] TRANB
*> \verbatim
*>          TRANB is CHARACTER*1
*>          Specifies the option op(B):
*>          = 'N': op(B) = B    (No transpose)
*>          = 'T': op(B) = B**T (Transpose)
*>          = 'C': op(B) = B**H (Conjugate transpose = Transpose)
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
*>          A is DOUBLE PRECISION array, dimension (LDA,M)
*>          The upper quasi-triangular matrix A, in Schur canonical form.
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
*>          B is DOUBLE PRECISION array, dimension (LDB,N)
*>          The upper quasi-triangular matrix B, in Schur canonical form.
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
*>          C is DOUBLE PRECISION array, dimension (LDC,N)
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
*>          SCALE is DOUBLE PRECISION
*>          The scale factor, scale, set <= 1 to avoid overflow in X.
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (MAX(1,LIWORK))
*>          On exit, if INFO = 0, IWORK(1) returns the optimal LIWORK.
*> \endverbatim
*>
*> \param[in] LIWORK
*> \verbatim
*>          IWORK is INTEGER
*>          The dimension of the array IWORK. LIWORK >=  ((M + NB - 1) / NB + 1)
*>          + ((N + NB - 1) / NB + 1), where NB is the optimal block size.
*>
*>          If LIWORK = -1, then a workspace query is assumed; the routine
*>          only calculates the optimal dimension of the IWORK array,
*>          returns this value as the first entry of the IWORK array, and
*>          no error message related to LIWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] SWORK
*> \verbatim
*>          SWORK is DOUBLE PRECISION array, dimension (MAX(2, ROWS),
*>          MAX(1,COLS)).
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
      SUBROUTINE DTRSYL3( TRANA, TRANB, ISGN, M, N, A, LDA, B, LDB, C,
     $                    LDC, SCALE, IWORK, LIWORK, SWORK, LDSWORK,
     $                    INFO )
      IMPLICIT NONE
*
*     .. Scalar Arguments ..
      CHARACTER          TRANA, TRANB
      INTEGER            INFO, ISGN, LDA, LDB, LDC, M, N,
     $                   LIWORK, LDSWORK
      DOUBLE PRECISION   SCALE
*     ..
*     .. Array Arguments ..
      INTEGER            IWORK( * )
      DOUBLE PRECISION   A( LDA, * ), B( LDB, * ), C( LDC, * ),
     $                   SWORK( LDSWORK, * )
*     ..
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            NOTRNA, NOTRNB, LQUERY, SKIP
      INTEGER            AWRK, BWRK, I, I1, I2, IINFO, J, J1, J2, JJ,
     $                   K, K1, K2, L, L1, L2, LL, NBA, NB, NBB, PC
      DOUBLE PRECISION   ANRM, BIGNUM, BNRM, CNRM, SCAL, SCALOC,
     $                   SCAMIN, SGN, XNRM, BUF, SMLNUM
*     ..
*     .. Local Arrays ..
      DOUBLE PRECISION   WNRM( MAX( M, N ) )
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ILAENV
      DOUBLE PRECISION   DLANGE, DLAMCH, DLARMM
      EXTERNAL           DLANGE, DLAMCH, DLARMM, ILAENV, LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           DGEMM, DLASCL, DSCAL, DTRSYL, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, EXPONENT, MAX, MIN
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
      NB = MAX(8, ILAENV( 1, 'DTRSYL', '', M, N, -1, -1) )
*
*     Compute number of blocks in A and B
*
      NBA = MAX( 1, (M + NB - 1) / NB )
      NBB = MAX( 1, (N + NB - 1) / NB )
*
*     Compute workspace
*
      INFO = 0
      LQUERY = ( LIWORK.EQ.-1 .OR. LDSWORK.EQ.-1 )
      IWORK( 1 ) = NBA + NBB + 2
      IF( LQUERY ) THEN
         LDSWORK = 2
         SWORK( 1, 1 ) = MAX( NBA, NBB )
         SWORK( 2, 1 ) = 2 * NBB + NBA
      END IF
*
*     Test the input arguments
*
      IF( .NOT.NOTRNA .AND. .NOT.LSAME( TRANA, 'T' ) .AND. .NOT.
     $    LSAME( TRANA, 'C' ) ) THEN
         INFO = -1
      ELSE IF( .NOT.NOTRNB .AND. .NOT.LSAME( TRANB, 'T' ) .AND. .NOT.
     $         LSAME( TRANB, 'C' ) ) THEN
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
         CALL XERBLA( 'DTRSYL3', -INFO )
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
*     workspaces are provided
*
      IF( MIN( NBA, NBB ).EQ.1 .OR. LDSWORK.LT.MAX( NBA, NBB ) .OR.
     $    LIWORK.LT.IWORK(1) ) THEN
        CALL DTRSYL( TRANA, TRANB, ISGN, M, N, A, LDA, B, LDB,
     $               C, LDC, SCALE, INFO )
        RETURN
      END IF
*
*     Set constants to control overflow
*
      SMLNUM = DLAMCH( 'S' )
      BIGNUM = ONE / SMLNUM
*
*      Partition A such that 2-by-2 blocks on the diagonal are not split
*
       SKIP = .FALSE.
       DO I = 1, NBA
          IWORK( I ) = ( I - 1 ) * NB + 1
       END DO
       IWORK( NBA + 1 ) = M + 1
       DO K = 1, NBA
          L1 = IWORK( K )
          L2 = IWORK( K + 1 ) - 1
          DO L = L1, L2
             IF( SKIP ) THEN
                SKIP = .FALSE.
                CYCLE
             END IF
             IF( L.GE.M ) THEN
*               A( M, M ) is a 1-by-1 block
                CYCLE
             END IF
             IF( A( L, L+1 ).NE.ZERO .AND. A( L+1, L ).NE.ZERO ) THEN
*               Check if 2-by-2 block is split
                IF( L + 1 .EQ. IWORK( K + 1 ) ) THEN
                   IWORK( K + 1 ) = IWORK( K + 1 ) + 1
                   CYCLE
                END IF
                SKIP = .TRUE.
             END IF
          END DO
       END DO
       IWORK( NBA + 1 ) = M + 1
       IF( IWORK( NBA ).GE.IWORK( NBA + 1 ) ) THEN
          IWORK( NBA ) = IWORK( NBA + 1 )
          NBA = NBA - 1
       END IF
*
*      Partition B such that 2-by-2 blocks on the diagonal are not split
*
       PC = NBA + 1
       SKIP = .FALSE.
       DO I = 1, NBB
          IWORK( PC + I ) = ( I - 1 ) * NB + 1
       END DO
       IWORK( PC + NBB + 1 ) = N + 1
       DO K = 1, NBB
          L1 = IWORK( PC + K )
          L2 = IWORK( PC + K + 1 ) - 1
          DO L = L1, L2
             IF( SKIP ) THEN
                SKIP = .FALSE.
                CYCLE
             END IF
             IF( L.GE.N ) THEN
*               B( N, N ) is a 1-by-1 block
                CYCLE
             END IF
             IF( B( L, L+1 ).NE.ZERO .AND. B( L+1, L ).NE.ZERO ) THEN
*               Check if 2-by-2 block is split
                IF( L + 1 .EQ. IWORK( PC + K + 1 ) ) THEN
                   IWORK( PC + K + 1 ) = IWORK( PC + K + 1 ) + 1
                   CYCLE
                END IF
                SKIP = .TRUE.
             END IF
          END DO
       END DO
       IWORK( PC + NBB + 1 ) = N + 1
       IF( IWORK( PC + NBB ).GE.IWORK( PC + NBB + 1 ) ) THEN
          IWORK( PC + NBB ) = IWORK( PC + NBB + 1 )
          NBB = NBB - 1
       END IF
*
*     Set local scaling factors - must never attain zero.
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
*     Compute upper bounds of blocks of A and B
*
      AWRK = NBB
      DO K = 1, NBA
         K1 = IWORK( K )
         K2 = IWORK( K + 1 )
         DO L = K, NBA
            L1 = IWORK( L )
            L2 = IWORK( L + 1 )
            IF( NOTRNA ) THEN
               SWORK( K, AWRK + L ) = DLANGE( 'I', K2-K1, L2-L1,
     $                                        A( K1, L1 ), LDA, WNRM )
            ELSE
               SWORK( L, AWRK + K ) = DLANGE( '1', K2-K1, L2-L1,
     $                                        A( K1, L1 ), LDA, WNRM )
            END IF
         END DO
      END DO
      BWRK = NBB + NBA
      DO K = 1, NBB
         K1 = IWORK( PC + K )
         K2 = IWORK( PC + K + 1 )
         DO L = K, NBB
            L1 = IWORK( PC + L )
            L2 = IWORK( PC + L + 1 )
            IF( NOTRNB ) THEN
               SWORK( K, BWRK + L ) = DLANGE( 'I', K2-K1, L2-L1,
     $                                        B( K1, L1 ), LDB, WNRM )
            ELSE
               SWORK( L, BWRK + K ) = DLANGE( '1', K2-K1, L2-L1,
     $                                        B( K1, L1 ), LDB, WNRM )
            END IF
         END DO
      END DO
*
      SGN = DBLE( ISGN )
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
            K1 = IWORK( K )
            K2 = IWORK( K + 1 )
            DO L = 1, NBB
*
*              L1: column index of the first column in X( K, L )
*              L2: column index of the first column in X( K, L + 1)
*              so that L2 - L1 is the row count of the block X( K, L )
*
               L1 = IWORK( PC + L )
               L2 = IWORK( PC + L + 1 )
*
               CALL DTRSYL( TRANA, TRANB, ISGN, K2-K1, L2-L1,
     $                      A( K1, K1 ), LDA,
     $                      B( L1, L1 ), LDB,
     $                      C( K1, L1 ), LDC, SCALOC, IINFO )
               INFO = MAX( INFO, IINFO )
*
               IF ( SCALOC * SWORK( K, L ) .EQ. ZERO ) THEN
                  IF( SCALOC .EQ. ZERO ) THEN
*                    The magnitude of the largest entry of X(K1:K2-1, L1:L2-1)
*                    is larger than the product of BIGNUM**2 and cannot be
*                    represented in the form (1/SCALE)*X(K1:K2-1, L1:L2-1).
*                    Mark the computation as pointless.
                     BUF = ZERO
                  ELSE
*                    Use second scaling factor to prevent flushing to zero.
                     BUF = BUF*2.D0**EXPONENT( SCALOC )
                  END IF
                  DO JJ = 1, NBB
                     DO LL = 1, NBA
*                       Bound by BIGNUM to not introduce Inf. The value
*                       is irrelevant; corresponding entries of the
*                       solution will be flushed in consistency scaling.
                        SWORK( LL, JJ ) = MIN( BIGNUM,
     $                     SWORK( LL, JJ ) / 2.D0**EXPONENT( SCALOC ) )
                     END DO
                  END DO
               END IF
               SWORK( K, L ) = SCALOC * SWORK( K, L )
               XNRM = DLANGE( 'I', K2-K1, L2-L1, C( K1, L1 ), LDC,
     $                        WNRM )
*
               DO I = K - 1, 1, -1
*
*                 C( I, L ) := C( I, L ) - A( I, K ) * C( K, L )
*
                  I1 = IWORK( I )
                  I2 = IWORK( I + 1 )
*
*                 Compute scaling factor to survive the linear update
*                 simulating consistent scaling.
*
                  CNRM = DLANGE( 'I', I2-I1, L2-L1, C( I1, L1 ),
     $                           LDC, WNRM )
                  SCAMIN = MIN( SWORK( I, L ), SWORK( K, L ) )
                  CNRM = CNRM * ( SCAMIN / SWORK( I, L ) )
                  XNRM = XNRM * ( SCAMIN / SWORK( K, L ) )
                  ANRM = SWORK( I, AWRK + K )
                  SCALOC = DLARMM( ANRM, XNRM, CNRM )
                  IF( SCALOC * SCAMIN .EQ. ZERO ) THEN
*                    Use second scaling factor to prevent flushing to zero.
                     BUF = BUF*2.D0**EXPONENT( SCALOC )
                     DO JJ = 1, NBB
                        DO LL = 1, NBA
                        SWORK( LL, JJ ) = MIN( BIGNUM,
     $                     SWORK( LL, JJ ) / 2.D0**EXPONENT( SCALOC ) )
                        END DO
                     END DO
                     SCAMIN = SCAMIN / 2.D0**EXPONENT( SCALOC )
                     SCALOC = SCALOC / 2.D0**EXPONENT( SCALOC )
                  END IF
                  CNRM = CNRM * SCALOC
                  XNRM = XNRM * SCALOC
*
*                 Simultaneously apply the robust update factor and the
*                 consistency scaling factor to C( I, L ) and C( K, L ).
*
                  SCAL = ( SCAMIN / SWORK( K, L ) ) * SCALOC
                  IF (SCAL .NE. ONE) THEN
                      DO JJ = L1, L2-1
                         CALL DSCAL( K2-K1, SCAL, C( K1, JJ ), 1)
                      END DO
                  ENDIF
*
                  SCAL = ( SCAMIN / SWORK( I, L ) ) * SCALOC
                  IF (SCAL .NE. ONE) THEN
                      DO LL = L1, L2-1
                         CALL DSCAL( I2-I1, SCAL, C( I1, LL ), 1)
                      END DO
                  ENDIF
*
*                 Record current scaling factor
*
                  SWORK( K, L ) = SCAMIN * SCALOC
                  SWORK( I, L ) = SCAMIN * SCALOC
*
                  CALL DGEMM( 'N', 'N', I2-I1, L2-L1, K2-K1, -ONE,
     $                        A( I1, K1 ), LDA, C( K1, L1 ), LDC,
     $                        ONE, C( I1, L1 ), LDC )
*
               END DO
*
               DO J = L + 1, NBB
*
*                 C( K, J ) := C( K, J ) - SGN * C( K, L ) * B( L, J )
*
                  J1 = IWORK( PC + J )
                  J2 = IWORK( PC + J + 1 )
*
*                 Compute scaling factor to survive the linear update
*                 simulating consistent scaling.
*
                  CNRM = DLANGE( 'I', K2-K1, J2-J1, C( K1, J1 ),
     $                           LDC, WNRM )
                  SCAMIN = MIN( SWORK( K, J ), SWORK( K, L ) )
                  CNRM = CNRM * ( SCAMIN / SWORK( K, J ) )
                  XNRM = XNRM * ( SCAMIN / SWORK( K, L ) )
                  BNRM = SWORK(L, BWRK + J)
                  SCALOC = DLARMM( BNRM, XNRM, CNRM )
                  IF( SCALOC * SCAMIN .EQ. ZERO ) THEN
*                    Use second scaling factor to prevent flushing to zero.
                     BUF = BUF*2.D0**EXPONENT( SCALOC )
                     DO JJ = 1, NBB
                        DO LL = 1, NBA
                        SWORK( LL, JJ ) = MIN( BIGNUM,
     $                     SWORK( LL, JJ ) / 2.D0**EXPONENT( SCALOC ) )
                        END DO
                     END DO
                     SCAMIN = SCAMIN / 2.D0**EXPONENT( SCALOC )
                     SCALOC = SCALOC / 2.D0**EXPONENT( SCALOC )
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
                        CALL DSCAL( K2-K1, SCAL, C( K1, LL ), 1 )
                     END DO
                  ENDIF
*
                  SCAL = ( SCAMIN / SWORK( K, J ) ) * SCALOC
                  IF( SCAL .NE. ONE ) THEN
                      DO JJ = J1, J2-1
                         CALL DSCAL( K2-K1, SCAL, C( K1, JJ ), 1 )
                      END DO
                  ENDIF
*
*                 Record current scaling factor
*
                  SWORK( K, L ) = SCAMIN * SCALOC
                  SWORK( K, J ) = SCAMIN * SCALOC
*
                  CALL DGEMM( 'N', 'N', K2-K1, J2-J1, L2-L1, -SGN,
     $                        C( K1, L1 ), LDC, B( L1, J1 ), LDB,
     $                        ONE, C( K1, J1 ), LDC )
               END DO
            END DO
         END DO
      ELSE IF( .NOT.NOTRNA .AND. NOTRNB ) THEN
*
*        Solve    A**T*X + ISGN*X*B = scale*C.
*
*        The (K,L)th block of X is determined starting from
*        upper-left corner column by column by
*
*          A(K,K)**T*X(K,L) + ISGN*X(K,L)*B(L,L) = C(K,L) - R(K,L)
*
*        Where
*                   K-1                        L-1
*          R(K,L) = SUM [A(I,K)**T*X(I,L)] +ISGN*SUM [X(K,J)*B(J,L)]
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
            K1 = IWORK( K )
            K2 = IWORK( K + 1 )
            DO L = 1, NBB
*
*              L1: column index of the first column in X( K, L )
*              L2: column index of the first column in X( K, L + 1)
*              so that L2 - L1 is the row count of the block X( K, L )
*
               L1 = IWORK( PC + L )
               L2 = IWORK( PC + L + 1 )
*
               CALL DTRSYL( TRANA, TRANB, ISGN, K2-K1, L2-L1,
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
                     BUF = BUF*2.D0**EXPONENT( SCALOC )
                  END IF
                  DO JJ = 1, NBB
                     DO LL = 1, NBA
*                       Bound by BIGNUM to not introduce Inf. The value
*                       is irrelevant; corresponding entries of the
*                       solution will be flushed in consistency scaling.
                        SWORK( LL, JJ ) = MIN( BIGNUM,
     $                     SWORK( LL, JJ ) / 2.D0**EXPONENT( SCALOC ) )
                     END DO
                  END DO
               END IF
               SWORK( K, L ) = SCALOC * SWORK( K, L )
               XNRM = DLANGE( 'I', K2-K1, L2-L1, C( K1, L1 ), LDC,
     $                        WNRM )
*
               DO I = K + 1, NBA
*
*                 C( I, L ) := C( I, L ) - A( K, I )**T * C( K, L )
*
                  I1 = IWORK( I )
                  I2 = IWORK( I + 1 )
*
*                 Compute scaling factor to survive the linear update
*                 simulating consistent scaling.
*
                  CNRM = DLANGE( 'I', I2-I1, L2-L1, C( I1, L1 ),
     $                           LDC, WNRM )
                  SCAMIN = MIN( SWORK( I, L ), SWORK( K, L ) )
                  CNRM = CNRM * ( SCAMIN / SWORK( I, L ) )
                  XNRM = XNRM * ( SCAMIN / SWORK( K, L ) )
                  ANRM = SWORK( I, AWRK + K )
                  SCALOC = DLARMM( ANRM, XNRM, CNRM )
                  IF( SCALOC * SCAMIN .EQ. ZERO ) THEN
*                    Use second scaling factor to prevent flushing to zero.
                     BUF = BUF*2.D0**EXPONENT( SCALOC )
                     DO JJ = 1, NBB
                        DO LL = 1, NBA
                        SWORK( LL, JJ ) = MIN( BIGNUM,
     $                     SWORK( LL, JJ ) / 2.D0**EXPONENT( SCALOC ) )
                        END DO
                     END DO
                     SCAMIN = SCAMIN / 2.D0**EXPONENT( SCALOC )
                     SCALOC = SCALOC / 2.D0**EXPONENT( SCALOC )
                  END IF
                  CNRM = CNRM * SCALOC
                  XNRM = XNRM * SCALOC
*
*                 Simultaneously apply the robust update factor and the
*                 consistency scaling factor to to C( I, L ) and C( K, L ).
*
                  SCAL = ( SCAMIN / SWORK( K, L ) ) * SCALOC
                  IF (SCAL .NE. ONE) THEN
                     DO LL = L1, L2-1
                        CALL DSCAL( K2-K1, SCAL, C( K1, LL ), 1 )
                     END DO
                  ENDIF
*
                  SCAL = ( SCAMIN / SWORK( I, L ) ) * SCALOC
                  IF (SCAL .NE. ONE) THEN
                     DO LL = L1, L2-1
                        CALL DSCAL( I2-I1, SCAL, C( I1, LL ), 1 )
                     END DO
                  ENDIF
*
*                 Record current scaling factor
*
                  SWORK( K, L ) = SCAMIN * SCALOC
                  SWORK( I, L ) = SCAMIN * SCALOC
*
                  CALL DGEMM( 'T', 'N', I2-I1, L2-L1, K2-K1, -ONE,
     $                        A( K1, I1 ), LDA, C( K1, L1 ), LDC,
     $                        ONE, C( I1, L1 ), LDC )
               END DO
*
               DO J = L + 1, NBB
*
*                 C( K, J ) := C( K, J ) - SGN * C( K, L ) * B( L, J )
*
                  J1 = IWORK( PC + J )
                  J2 = IWORK( PC + J + 1 )
*
*                 Compute scaling factor to survive the linear update
*                 simulating consistent scaling.
*
                  CNRM = DLANGE( 'I', K2-K1, J2-J1, C( K1, J1 ),
     $                           LDC, WNRM )
                  SCAMIN = MIN( SWORK( K, J ), SWORK( K, L ) )
                  CNRM = CNRM * ( SCAMIN / SWORK( K, J ) )
                  XNRM = XNRM * ( SCAMIN / SWORK( K, L ) )
                  BNRM = SWORK( L, BWRK + J )
                  SCALOC = DLARMM( BNRM, XNRM, CNRM )
                  IF( SCALOC * SCAMIN .EQ. ZERO ) THEN
*                    Use second scaling factor to prevent flushing to zero.
                     BUF = BUF*2.D0**EXPONENT( SCALOC )
                     DO JJ = 1, NBB
                        DO LL = 1, NBA
                        SWORK( LL, JJ ) = MIN( BIGNUM,
     $                     SWORK( LL, JJ ) / 2.D0**EXPONENT( SCALOC ) )
                        END DO
                     END DO
                     SCAMIN = SCAMIN / 2.D0**EXPONENT( SCALOC )
                     SCALOC = SCALOC / 2.D0**EXPONENT( SCALOC )
                  END IF
                  CNRM = CNRM * SCALOC
                  XNRM = XNRM * SCALOC
*
*                 Simultaneously apply the robust update factor and the
*                 consistency scaling factor to to C( K, J ) and C( K, L ).
*
                  SCAL = ( SCAMIN / SWORK( K, L ) ) * SCALOC
                  IF( SCAL .NE. ONE ) THEN
                      DO LL = L1, L2-1
                         CALL DSCAL( K2-K1, SCAL, C( K1, LL ), 1 )
                      END DO
                  ENDIF
*
                  SCAL = ( SCAMIN / SWORK( K, J ) ) * SCALOC
                  IF( SCAL .NE. ONE ) THEN
                     DO JJ = J1, J2-1
                        CALL DSCAL( K2-K1, SCAL, C( K1, JJ ), 1 )
                     END DO
                  ENDIF
*
*                 Record current scaling factor
*
                  SWORK( K, L ) = SCAMIN * SCALOC
                  SWORK( K, J ) = SCAMIN * SCALOC
*
                  CALL DGEMM( 'N', 'N', K2-K1, J2-J1, L2-L1, -SGN,
     $                        C( K1, L1 ), LDC, B( L1, J1 ), LDB,
     $                        ONE, C( K1, J1 ), LDC )
               END DO
            END DO
         END DO
      ELSE IF( .NOT.NOTRNA .AND. .NOT.NOTRNB ) THEN
*
*        Solve    A**T*X + ISGN*X*B**T = scale*C.
*
*        The (K,L)th block of X is determined starting from
*        top-right corner column by column by
*
*           A(K,K)**T*X(K,L) + ISGN*X(K,L)*B(L,L)**T = C(K,L) - R(K,L)
*
*        Where
*                     K-1                          N
*            R(K,L) = SUM [A(I,K)**T*X(I,L)] + ISGN*SUM [X(K,J)*B(L,J)**T].
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
            K1 = IWORK( K )
            K2 = IWORK( K + 1 )
            DO L = NBB, 1, -1
*
*              L1: column index of the first column in X( K, L )
*              L2: column index of the first column in X( K, L + 1)
*              so that L2 - L1 is the row count of the block X( K, L )
*
               L1 = IWORK( PC + L )
               L2 = IWORK( PC + L + 1 )
*
               CALL DTRSYL( TRANA, TRANB, ISGN, K2-K1, L2-L1,
     $                      A( K1, K1 ), LDA,
     $                      B( L1, L1 ), LDB,
     $                      C( K1, L1 ), LDC, SCALOC, IINFO )
               INFO = MAX( INFO, IINFO )
*
               SWORK( K, L ) = SCALOC * SWORK( K, L )
               IF( SCALOC * SWORK( K, L ) .EQ. ZERO ) THEN
                  IF( SCALOC .EQ. ZERO ) THEN
*                    The magnitude of the largest entry of X(K1:K2-1, L1:L2-1)
*                    is larger than the product of BIGNUM**2 and cannot be
*                    represented in the form (1/SCALE)*X(K1:K2-1, L1:L2-1).
*                    Mark the computation as pointless.
                     BUF = ZERO
                  ELSE
*                    Use second scaling factor to prevent flushing to zero.
                     BUF = BUF*2.D0**EXPONENT( SCALOC )
                  END IF
                  DO JJ = 1, NBB
                     DO LL = 1, NBA
*                       Bound by BIGNUM to not introduce Inf. The value
*                       is irrelevant; corresponding entries of the
*                       solution will be flushed in consistency scaling.
                        SWORK( LL, JJ ) = MIN( BIGNUM,
     $                     SWORK( LL, JJ ) / 2.D0**EXPONENT( SCALOC ) )
                     END DO
                  END DO
               END IF
               XNRM = DLANGE( 'I', K2-K1, L2-L1, C( K1, L1 ), LDC,
     $                        WNRM )
*
               DO I = K + 1, NBA
*
*                 C( I, L ) := C( I, L ) - A( K, I )**T * C( K, L )
*
                  I1 = IWORK( I )
                  I2 = IWORK( I + 1 )
*
*                 Compute scaling factor to survive the linear update
*                 simulating consistent scaling.
*
                  CNRM = DLANGE( 'I', I2-I1, L2-L1, C( I1, L1 ),
     $                           LDC, WNRM )
                  SCAMIN = MIN( SWORK( I, L ), SWORK( K, L ) )
                  CNRM = CNRM * ( SCAMIN / SWORK( I, L ) )
                  XNRM = XNRM * ( SCAMIN / SWORK( K, L ) )
                  ANRM = SWORK( I, AWRK + K )
                  SCALOC = DLARMM( ANRM, XNRM, CNRM )
                  IF( SCALOC * SCAMIN .EQ. ZERO ) THEN
*                    Use second scaling factor to prevent flushing to zero.
                     BUF = BUF*2.D0**EXPONENT( SCALOC )
                     DO JJ = 1, NBB
                        DO LL = 1, NBA
                        SWORK( LL, JJ ) = MIN( BIGNUM,
     $                     SWORK( LL, JJ ) / 2.D0**EXPONENT( SCALOC ) )
                        END DO
                     END DO
                     SCAMIN = SCAMIN / 2.D0**EXPONENT( SCALOC )
                     SCALOC = SCALOC / 2.D0**EXPONENT( SCALOC )
                  END IF
                  CNRM = CNRM * SCALOC
                  XNRM = XNRM * SCALOC
*
*                 Simultaneously apply the robust update factor and the
*                 consistency scaling factor to C( I, L ) and C( K, L ).
*
                  SCAL = ( SCAMIN / SWORK( K, L ) ) * SCALOC
                  IF (SCAL .NE. ONE) THEN
                     DO LL = L1, L2-1
                        CALL DSCAL( K2-K1, SCAL, C( K1, LL ), 1 )
                     END DO
                  ENDIF
*
                  SCAL = ( SCAMIN / SWORK( I, L ) ) * SCALOC
                  IF (SCAL .NE. ONE) THEN
                     DO LL = L1, L2-1
                        CALL DSCAL( I2-I1, SCAL, C( I1, LL ), 1 )
                     END DO
                  ENDIF
*
*                 Record current scaling factor
*
                  SWORK( K, L ) = SCAMIN * SCALOC
                  SWORK( I, L ) = SCAMIN * SCALOC
*
                  CALL DGEMM( 'T', 'N', I2-I1, L2-L1, K2-K1, -ONE,
     $                        A( K1, I1 ), LDA, C( K1, L1 ), LDC,
     $                        ONE, C( I1, L1 ), LDC )
               END DO
*
               DO J = 1, L - 1
*
*                 C( K, J ) := C( K, J ) - SGN * C( K, L ) * B( J, L )**T
*
                  J1 = IWORK( PC + J )
                  J2 = IWORK( PC + J + 1 )
*
*                 Compute scaling factor to survive the linear update
*                 simulating consistent scaling.
*
                  CNRM = DLANGE( 'I', K2-K1, J2-J1, C( K1, J1 ),
     $                           LDC, WNRM )
                  SCAMIN = MIN( SWORK( K, J ), SWORK( K, L ) )
                  CNRM = CNRM * ( SCAMIN / SWORK( K, J ) )
                  XNRM = XNRM * ( SCAMIN / SWORK( K, L ) )
                  BNRM = SWORK( L, BWRK + J )
                  SCALOC = DLARMM( BNRM, XNRM, CNRM )
                  IF( SCALOC * SCAMIN .EQ. ZERO ) THEN
*                    Use second scaling factor to prevent flushing to zero.
                     BUF = BUF*2.D0**EXPONENT( SCALOC )
                     DO JJ = 1, NBB
                        DO LL = 1, NBA
                        SWORK( LL, JJ ) = MIN( BIGNUM,
     $                     SWORK( LL, JJ ) / 2.D0**EXPONENT( SCALOC ) )
                        END DO
                     END DO
                     SCAMIN = SCAMIN / 2.D0**EXPONENT( SCALOC )
                     SCALOC = SCALOC / 2.D0**EXPONENT( SCALOC )
                  END IF
                  CNRM = CNRM * SCALOC
                  XNRM = XNRM * SCALOC
*
*                 Simultaneously apply the robust update factor and the
*                 consistency scaling factor to C( K, J ) and C( K, L ).
*
                  SCAL = ( SCAMIN / SWORK( K, L ) ) * SCALOC
                  IF( SCAL .NE. ONE ) THEN
                     DO LL = L1, L2-1
                        CALL DSCAL( K2-K1, SCAL, C( K1, LL ), 1)
                     END DO
                  ENDIF
*
                  SCAL = ( SCAMIN / SWORK( K, J ) ) * SCALOC
                  IF( SCAL .NE. ONE ) THEN
                     DO JJ = J1, J2-1
                        CALL DSCAL( K2-K1, SCAL, C( K1, JJ ), 1 )
                     END DO
                  ENDIF
*
*                 Record current scaling factor
*
                  SWORK( K, L ) = SCAMIN * SCALOC
                  SWORK( K, J ) = SCAMIN * SCALOC
*
                  CALL DGEMM( 'N', 'T', K2-K1, J2-J1, L2-L1, -SGN,
     $                        C( K1, L1 ), LDC, B( J1, L1 ), LDB,
     $                        ONE, C( K1, J1 ), LDC )
               END DO
            END DO
         END DO
      ELSE IF( NOTRNA .AND. .NOT.NOTRNB ) THEN
*
*        Solve    A*X + ISGN*X*B**T = scale*C.
*
*        The (K,L)th block of X is determined starting from
*        bottom-right corner column by column by
*
*            A(K,K)*X(K,L) + ISGN*X(K,L)*B(L,L)**T = C(K,L) - R(K,L)
*
*        Where
*                      M                          N
*            R(K,L) = SUM [A(K,I)*X(I,L)] + ISGN*SUM [X(K,J)*B(L,J)**T].
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
            K1 = IWORK( K )
            K2 = IWORK( K + 1 )
            DO L = NBB, 1, -1
*
*              L1: column index of the first column in X( K, L )
*              L2: column index of the first column in X( K, L + 1)
*              so that L2 - L1 is the row count of the block X( K, L )
*
               L1 = IWORK( PC + L )
               L2 = IWORK( PC + L + 1 )
*
               CALL DTRSYL( TRANA, TRANB, ISGN, K2-K1, L2-L1,
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
                     BUF = BUF*2.D0**EXPONENT( SCALOC )
                  END IF
                  DO JJ = 1, NBB
                     DO LL = 1, NBA
*                       Bound by BIGNUM to not introduce Inf. The value
*                       is irrelevant; corresponding entries of the
*                       solution will be flushed in consistency scaling.
                        SWORK( LL, JJ ) = MIN( BIGNUM,
     $                     SWORK( LL, JJ ) / 2.D0**EXPONENT( SCALOC ) )
                     END DO
                  END DO
               END IF
               SWORK( K, L ) = SCALOC * SWORK( K, L )
               XNRM = DLANGE( 'I', K2-K1, L2-L1, C( K1, L1 ), LDC,
     $                        WNRM )
*
               DO I = 1, K - 1
*
*                 C( I, L ) := C( I, L ) - A( I, K ) * C( K, L )
*
                  I1 = IWORK( I )
                  I2 = IWORK( I + 1 )
*
*                 Compute scaling factor to survive the linear update
*                 simulating consistent scaling.
*
                  CNRM = DLANGE( 'I', I2-I1, L2-L1, C( I1, L1 ),
     $                           LDC, WNRM )
                  SCAMIN = MIN( SWORK( I, L ), SWORK( K, L ) )
                  CNRM = CNRM * ( SCAMIN / SWORK( I, L ) )
                  XNRM = XNRM * ( SCAMIN / SWORK( K, L ) )
                  ANRM = SWORK( I, AWRK + K )
                  SCALOC = DLARMM( ANRM, XNRM, CNRM )
                  IF( SCALOC * SCAMIN .EQ. ZERO ) THEN
*                    Use second scaling factor to prevent flushing to zero.
                     BUF = BUF*2.D0**EXPONENT( SCALOC )
                     DO JJ = 1, NBB
                        DO LL = 1, NBA
                        SWORK( LL, JJ ) = MIN( BIGNUM,
     $                     SWORK( LL, JJ ) / 2.D0**EXPONENT( SCALOC ) )
                        END DO
                     END DO
                     SCAMIN = SCAMIN / 2.D0**EXPONENT( SCALOC )
                     SCALOC = SCALOC / 2.D0**EXPONENT( SCALOC )
                  END IF
                  CNRM = CNRM * SCALOC
                  XNRM = XNRM * SCALOC
*
*                 Simultaneously apply the robust update factor and the
*                 consistency scaling factor to C( I, L ) and C( K, L ).
*
                  SCAL = ( SCAMIN / SWORK( K, L ) ) * SCALOC
                  IF (SCAL .NE. ONE) THEN
                     DO LL = L1, L2-1
                        CALL DSCAL( K2-K1, SCAL, C( K1, LL ), 1 )
                     END DO
                  ENDIF
*
                  SCAL = ( SCAMIN / SWORK( I, L ) ) * SCALOC
                  IF (SCAL .NE. ONE) THEN
                     DO LL = L1, L2-1
                        CALL DSCAL( I2-I1, SCAL, C( I1, LL ), 1 )
                     END DO
                  ENDIF
*
*                 Record current scaling factor
*
                  SWORK( K, L ) = SCAMIN * SCALOC
                  SWORK( I, L ) = SCAMIN * SCALOC
*
                  CALL DGEMM( 'N', 'N', I2-I1, L2-L1, K2-K1, -ONE,
     $                        A( I1, K1 ), LDA, C( K1, L1 ), LDC,
     $                        ONE, C( I1, L1 ), LDC )
*
               END DO
*
               DO J = 1, L - 1
*
*                 C( K, J ) := C( K, J ) - SGN * C( K, L ) * B( J, L )**T
*
                  J1 = IWORK( PC + J )
                  J2 = IWORK( PC + J + 1 )
*
*                 Compute scaling factor to survive the linear update
*                 simulating consistent scaling.
*
                  CNRM = DLANGE( 'I', K2-K1, J2-J1, C( K1, J1 ),
     $                           LDC, WNRM )
                  SCAMIN = MIN( SWORK( K, J ), SWORK( K, L ) )
                  CNRM = CNRM * ( SCAMIN / SWORK( K, J ) )
                  XNRM = XNRM * ( SCAMIN / SWORK( K, L ) )
                  BNRM = SWORK( L, BWRK + J )
                  SCALOC = DLARMM( BNRM, XNRM, CNRM )
                  IF( SCALOC * SCAMIN .EQ. ZERO ) THEN
*                    Use second scaling factor to prevent flushing to zero.
                     BUF = BUF*2.D0**EXPONENT( SCALOC )
                     DO JJ = 1, NBB
                        DO LL = 1, NBA
                        SWORK( LL, JJ ) = MIN( BIGNUM,
     $                     SWORK( LL, JJ ) / 2.D0**EXPONENT( SCALOC ) )
                        END DO
                     END DO
                     SCAMIN = SCAMIN / 2.D0**EXPONENT( SCALOC )
                     SCALOC = SCALOC / 2.D0**EXPONENT( SCALOC )
                  END IF
                  CNRM = CNRM * SCALOC
                  XNRM = XNRM * SCALOC
*
*                 Simultaneously apply the robust update factor and the
*                 consistency scaling factor to C( K, J ) and C( K, L ).
*
                  SCAL = ( SCAMIN / SWORK( K, L ) ) * SCALOC
                  IF( SCAL .NE. ONE ) THEN
                     DO JJ = L1, L2-1
                        CALL DSCAL( K2-K1, SCAL, C( K1, JJ ), 1 )
                     END DO
                  ENDIF
*
                  SCAL = ( SCAMIN / SWORK( K, J ) ) * SCALOC
                  IF( SCAL .NE. ONE ) THEN
                     DO JJ = J1, J2-1
                        CALL DSCAL( K2-K1, SCAL, C( K1, JJ ), 1 )
                     END DO
                  ENDIF
*
*                 Record current scaling factor
*
                  SWORK( K, L ) = SCAMIN * SCALOC
                  SWORK( K, J ) = SCAMIN * SCALOC
*
                  CALL DGEMM( 'N', 'T', K2-K1, J2-J1, L2-L1, -SGN,
     $                        C( K1, L1 ), LDC, B( J1, L1 ), LDB,
     $                        ONE, C( K1, J1 ), LDC )
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
*
      IF( SCALE .EQ. ZERO ) THEN
*
*        The magnitude of the largest entry of the solution is larger
*        than the product of BIGNUM**2 and cannot be represented in the
*        form (1/SCALE)*X if SCALE is DOUBLE PRECISION. Set SCALE to
*        zero and give up.
*
         IWORK(1) = NBA + NBB + 2
         SWORK(1,1) = MAX( NBA, NBB )
         SWORK(2,1) = 2 * NBB + NBA
         RETURN
      END IF
*
*     Realize consistent scaling
*
      DO K = 1, NBA
         K1 = IWORK( K )
         K2 = IWORK( K + 1 )
         DO L = 1, NBB
            L1 = IWORK( PC + L )
            L2 = IWORK( PC + L + 1 )
            SCAL = SCALE / SWORK( K, L )
            IF( SCAL .NE. ONE ) THEN
               DO LL = L1, L2-1
                  CALL DSCAL( K2-K1, SCAL, C( K1, LL ), 1 )
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
         SCAL = C( 1, 1 )
         DO K = 1, M
            DO L = 1, N
               SCAL = MAX( SCAL, ABS( C( K, L ) ) )
            END DO
         END DO
*
*        Increase BUF as close to 1 as possible and apply scaling.
*
         SCALOC = MIN( BIGNUM / SCAL, ONE / BUF )
         BUF = BUF * SCALOC
         CALL DLASCL( 'G', -1, -1, ONE, SCALOC, M, N, C, LDC, IWORK(1) )
      END IF
*
*     Combine with buffer scaling factor. SCALE will be flushed if
*     BUF is less than one here.
*
      SCALE = SCALE * BUF
*
*     Restore workspace dimensions
*
      IWORK(1) = NBA + NBB + 2
      SWORK(1,1) = MAX( NBA, NBB )
      SWORK(2,1) = 2 * NBB + NBA
*
      RETURN
*
*     End of DTRSYL3
*
      END
