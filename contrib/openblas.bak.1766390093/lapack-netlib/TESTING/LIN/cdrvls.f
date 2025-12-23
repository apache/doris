*> \brief \b CDRVLS
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CDRVLS( DOTYPE, NM, MVAL, NN, NVAL, NNS, NSVAL, NNB,
*                          NBVAL, NXVAL, THRESH, TSTERR, A, COPYA, B,
*                          COPYB, C, S, COPYS, NOUT )
*
*       .. Scalar Arguments ..
*       LOGICAL            TSTERR
*       INTEGER            NM, NN, NNB, NNS, NOUT
*       REAL               THRESH
*       ..
*       .. Array Arguments ..
*       LOGICAL            DOTYPE( * )
*       INTEGER            MVAL( * ), NBVAL( * ), NSVAL( * ),
*      $                   NVAL( * ), NXVAL( * )
*       REAL               COPYS( * ), S( * )
*       COMPLEX            A( * ), B( * ), C( * ), COPYA( * ), COPYB( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CDRVLS tests the least squares driver routines CGELS, CGELST,
*> CGETSLS, CGELSS, CGELSY
*> and CGELSD.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] DOTYPE
*> \verbatim
*>          DOTYPE is LOGICAL array, dimension (NTYPES)
*>          The matrix types to be used for testing.  Matrices of type j
*>          (for 1 <= j <= NTYPES) are used for testing if DOTYPE(j) =
*>          .TRUE.; if DOTYPE(j) = .FALSE., then type j is not used.
*>          The matrix of type j is generated as follows:
*>          j=1: A = U*D*V where U and V are random unitary matrices
*>               and D has random entries (> 0.1) taken from a uniform
*>               distribution (0,1). A is full rank.
*>          j=2: The same of 1, but A is scaled up.
*>          j=3: The same of 1, but A is scaled down.
*>          j=4: A = U*D*V where U and V are random unitary matrices
*>               and D has 3*min(M,N)/4 random entries (> 0.1) taken
*>               from a uniform distribution (0,1) and the remaining
*>               entries set to 0. A is rank-deficient.
*>          j=5: The same of 4, but A is scaled up.
*>          j=6: The same of 5, but A is scaled down.
*> \endverbatim
*>
*> \param[in] NM
*> \verbatim
*>          NM is INTEGER
*>          The number of values of M contained in the vector MVAL.
*> \endverbatim
*>
*> \param[in] MVAL
*> \verbatim
*>          MVAL is INTEGER array, dimension (NM)
*>          The values of the matrix row dimension M.
*> \endverbatim
*>
*> \param[in] NN
*> \verbatim
*>          NN is INTEGER
*>          The number of values of N contained in the vector NVAL.
*> \endverbatim
*>
*> \param[in] NVAL
*> \verbatim
*>          NVAL is INTEGER array, dimension (NN)
*>          The values of the matrix column dimension N.
*> \endverbatim
*>
*> \param[in] NNB
*> \verbatim
*>          NNB is INTEGER
*>          The number of values of NB and NX contained in the
*>          vectors NBVAL and NXVAL.  The blocking parameters are used
*>          in pairs (NB,NX).
*> \endverbatim
*>
*> \param[in] NBVAL
*> \verbatim
*>          NBVAL is INTEGER array, dimension (NNB)
*>          The values of the blocksize NB.
*> \endverbatim
*>
*> \param[in] NXVAL
*> \verbatim
*>          NXVAL is INTEGER array, dimension (NNB)
*>          The values of the crossover point NX.
*> \endverbatim
*>
*> \param[in] NNS
*> \verbatim
*>          NNS is INTEGER
*>          The number of values of NRHS contained in the vector NSVAL.
*> \endverbatim
*>
*> \param[in] NSVAL
*> \verbatim
*>          NSVAL is INTEGER array, dimension (NNS)
*>          The values of the number of right hand sides NRHS.
*> \endverbatim
*>
*> \param[in] THRESH
*> \verbatim
*>          THRESH is REAL
*>          The threshold value for the test ratios.  A result is
*>          included in the output file if RESULT >= THRESH.  To have
*>          every test ratio printed, use THRESH = 0.
*> \endverbatim
*>
*> \param[in] TSTERR
*> \verbatim
*>          TSTERR is LOGICAL
*>          Flag that indicates whether error exits are to be tested.
*> \endverbatim
*>
*> \param[out] A
*> \verbatim
*>          A is COMPLEX array, dimension (MMAX*NMAX)
*>          where MMAX is the maximum value of M in MVAL and NMAX is the
*>          maximum value of N in NVAL.
*> \endverbatim
*>
*> \param[out] COPYA
*> \verbatim
*>          COPYA is COMPLEX array, dimension (MMAX*NMAX)
*> \endverbatim
*>
*> \param[out] B
*> \verbatim
*>          B is COMPLEX array, dimension (MMAX*NSMAX)
*>          where MMAX is the maximum value of M in MVAL and NSMAX is the
*>          maximum value of NRHS in NSVAL.
*> \endverbatim
*>
*> \param[out] COPYB
*> \verbatim
*>          COPYB is COMPLEX array, dimension (MMAX*NSMAX)
*> \endverbatim
*>
*> \param[out] C
*> \verbatim
*>          C is COMPLEX array, dimension (MMAX*NSMAX)
*> \endverbatim
*>
*> \param[out] S
*> \verbatim
*>          S is REAL array, dimension
*>                      (min(MMAX,NMAX))
*> \endverbatim
*>
*> \param[out] COPYS
*> \verbatim
*>          COPYS is REAL array, dimension
*>                      (min(MMAX,NMAX))
*> \endverbatim
*>
*> \param[in] NOUT
*> \verbatim
*>          NOUT is INTEGER
*>          The unit number for output.
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
*> \ingroup complex_lin
*
*  =====================================================================
      SUBROUTINE CDRVLS( DOTYPE, NM, MVAL, NN, NVAL, NNS, NSVAL, NNB,
     $                   NBVAL, NXVAL, THRESH, TSTERR, A, COPYA, B,
     $                   COPYB, C, S, COPYS, NOUT )
*
*  -- LAPACK test routine --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*
*     .. Scalar Arguments ..
      LOGICAL            TSTERR
      INTEGER            NM, NN, NNB, NNS, NOUT
      REAL               THRESH
*     ..
*     .. Array Arguments ..
      LOGICAL            DOTYPE( * )
      INTEGER            MVAL( * ), NBVAL( * ), NSVAL( * ),
     $                   NVAL( * ), NXVAL( * )
      REAL               COPYS( * ), S( * )
      COMPLEX            A( * ), B( * ), C( * ), COPYA( * ), COPYB( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      INTEGER            NTESTS
      PARAMETER          ( NTESTS = 18 )
      INTEGER            SMLSIZ
      PARAMETER          ( SMLSIZ = 25 )
      REAL               ONE, ZERO
      PARAMETER          ( ONE = 1.0E+0, ZERO = 0.0E+0 )
      COMPLEX            CONE, CZERO
      PARAMETER          ( CONE = ( 1.0E+0, 0.0E+0 ),
     $                   CZERO = ( 0.0E+0, 0.0E+0 ) )
*     ..
*     .. Local Scalars ..
      CHARACTER          TRANS
      CHARACTER*3        PATH
      INTEGER            CRANK, I, IM, IMB, IN, INB, INFO, INS, IRANK,
     $                   ISCALE, ITRAN, ITYPE, J, K, LDA, LDB, LDWORK,
     $                   LWLSY, LWORK, M, MNMIN, N, NB, NCOLS, NERRS,
     $                   NFAIL, NRHS, NROWS, NRUN, RANK, MB,
     $                   MMAX, NMAX, NSMAX, LIWORK, LRWORK,
     $                   LWORK_CGELS, LWORK_CGELST, LWORK_CGETSLS,
     $                   LWORK_CGELSS, LWORK_CGELSY,  LWORK_CGELSD,
     $                   LRWORK_CGELSY, LRWORK_CGELSS, LRWORK_CGELSD
      REAL               EPS, NORMA, NORMB, RCOND
*     ..
*     .. Local Arrays ..
      INTEGER            ISEED( 4 ), ISEEDY( 4 ), IWQ( 1 )
      REAL               RESULT( NTESTS ), RWQ( 1 )
      COMPLEX            WQ( 1 )
*     ..
*     .. Allocatable Arrays ..
      COMPLEX, ALLOCATABLE :: WORK (:)
      REAL, ALLOCATABLE :: RWORK (:), WORK2 (:)
      INTEGER, ALLOCATABLE :: IWORK (:)
*     ..
*     .. External Functions ..
      REAL               CQRT12, CQRT14, CQRT17, SASUM, SLAMCH
      EXTERNAL           CQRT12, CQRT14, CQRT17, SASUM, SLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAERH, ALAHD, ALASVM, CERRLS, CGELS, CGELSD,
     $                   CGELSS, CGELST, CGELSY, CGEMM, CGETSLS, CLACPY,
     $                   CLARNV, CQRT13, CQRT15, CQRT16, CSSCAL,
     $                   SAXPY, XLAENV
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN, INT, REAL, SQRT
*     ..
*     .. Scalars in Common ..
      LOGICAL            LERR, OK
      CHARACTER*32       SRNAMT
      INTEGER            INFOT, IOUNIT
*     ..
*     .. Common blocks ..
      COMMON             / INFOC / INFOT, IOUNIT, OK, LERR
      COMMON             / SRNAMC / SRNAMT
*     ..
*     .. Data statements ..
      DATA               ISEEDY / 1988, 1989, 1990, 1991 /
*     ..
*     .. Executable Statements ..
*
*     Initialize constants and the random number seed.
*
      PATH( 1: 1 ) = 'Complex precision'
      PATH( 2: 3 ) = 'LS'
      NRUN = 0
      NFAIL = 0
      NERRS = 0
      DO 10 I = 1, 4
         ISEED( I ) = ISEEDY( I )
   10 CONTINUE
      EPS = SLAMCH( 'Epsilon' )
*
*     Threshold for rank estimation
*
      RCOND = SQRT( EPS ) - ( SQRT( EPS )-EPS ) / 2
*
*     Test the error exits
*
      CALL XLAENV( 9, SMLSIZ )
      IF( TSTERR )
     $   CALL CERRLS( PATH, NOUT )
*
*     Print the header if NM = 0 or NN = 0 and THRESH = 0.
*
      IF( ( NM.EQ.0 .OR. NN.EQ.0 ) .AND. THRESH.EQ.ZERO )
     $   CALL ALAHD( NOUT, PATH )
      INFOT = 0
*
*     Compute maximal workspace needed for all routines
*
      NMAX = 0
      MMAX = 0
      NSMAX = 0
      DO I = 1, NM
         IF ( MVAL( I ).GT.MMAX ) THEN
            MMAX = MVAL( I )
         END IF
      ENDDO
      DO I = 1, NN
         IF ( NVAL( I ).GT.NMAX ) THEN
            NMAX = NVAL( I )
         END IF
      ENDDO
      DO I = 1, NNS
         IF ( NSVAL( I ).GT.NSMAX ) THEN
            NSMAX = NSVAL( I )
         END IF
      ENDDO
      M = MMAX
      N = NMAX
      NRHS = NSMAX
      MNMIN = MAX( MIN( M, N ), 1 )
*
*     Compute workspace needed for routines
*     CQRT14, CQRT17 (two side cases), CQRT15 and CQRT12
*
      LWORK = MAX( 1, ( M+N )*NRHS,
     $      ( N+NRHS )*( M+2 ), ( M+NRHS )*( N+2 ),
     $      MAX( M+MNMIN, NRHS*MNMIN,2*N+M ),
     $      MAX( M*N+4*MNMIN+MAX(M,N), M*N+2*MNMIN+4*N ) )
      LRWORK = 1
      LIWORK = 1
*
*     Iterate through all test cases and compute necessary workspace
*     sizes for ?GELS, ?GELST, ?GETSLS, ?GELSY, ?GELSS and ?GELSD
*     routines.
*
      DO IM = 1, NM
         M = MVAL( IM )
         LDA = MAX( 1, M )
         DO IN = 1, NN
            N = NVAL( IN )
            MNMIN = MAX(MIN( M, N ),1)
            LDB = MAX( 1, M, N )
            DO INS = 1, NNS
               NRHS = NSVAL( INS )
               DO IRANK = 1, 2
                  DO ISCALE = 1, 3
                     ITYPE = ( IRANK-1 )*3 + ISCALE
                     IF( DOTYPE( ITYPE ) ) THEN
                        IF( IRANK.EQ.1 ) THEN
                           DO ITRAN = 1, 2
                              IF( ITRAN.EQ.1 ) THEN
                                 TRANS = 'N'
                              ELSE
                                 TRANS = 'C'
                              END IF
*
*                             Compute workspace needed for CGELS
                              CALL CGELS( TRANS, M, N, NRHS, A, LDA,
     $                                    B, LDB, WQ, -1, INFO )
                              LWORK_CGELS = INT( WQ( 1 ) )
*                             Compute workspace needed for CGELST
                              CALL CGELST( TRANS, M, N, NRHS, A, LDA,
     $                                    B, LDB, WQ, -1, INFO )
                              LWORK_CGELST = INT ( WQ ( 1 ) )
*                             Compute workspace needed for CGETSLS
                              CALL CGETSLS( TRANS, M, N, NRHS, A, LDA,
     $                                      B, LDB, WQ, -1, INFO )
                              LWORK_CGETSLS = INT( WQ( 1 ) )
                           ENDDO
                        END IF
*                       Compute workspace needed for CGELSY
                        CALL CGELSY( M, N, NRHS, A, LDA, B, LDB,
     $                               IWQ, RCOND, CRANK, WQ, -1, RWQ,
     $                               INFO )
                        LWORK_CGELSY = INT( WQ( 1 ) )
                        LRWORK_CGELSY = 2*N
*                       Compute workspace needed for CGELSS
                        CALL CGELSS( M, N, NRHS, A, LDA, B, LDB, S,
     $                               RCOND, CRANK, WQ, -1, RWQ, INFO )
                        LWORK_CGELSS = INT( WQ( 1 ) )
                        LRWORK_CGELSS = 5*MNMIN
*                       Compute workspace needed for CGELSD
                        CALL CGELSD( M, N, NRHS, A, LDA, B, LDB, S,
     $                               RCOND, CRANK, WQ, -1, RWQ, IWQ,
     $                               INFO )
                        LWORK_CGELSD = INT( WQ( 1 ) )
                        LRWORK_CGELSD = INT( RWQ ( 1 ) )
*                       Compute LIWORK workspace needed for CGELSY and CGELSD
                        LIWORK = MAX( LIWORK, N, IWQ ( 1 ) )
*                       Compute LRWORK workspace needed for CGELSY, CGELSS and CGELSD
                        LRWORK = MAX( LRWORK, LRWORK_CGELSY,
     $                                LRWORK_CGELSS, LRWORK_CGELSD )
*                       Compute LWORK workspace needed for all functions
                        LWORK = MAX( LWORK, LWORK_CGELS, LWORK_CGETSLS,
     $                               LWORK_CGELSY, LWORK_CGELSS,
     $                               LWORK_CGELSD )
                     END IF
                  ENDDO
               ENDDO
            ENDDO
         ENDDO
      ENDDO
*
      LWLSY = LWORK
*
      ALLOCATE( WORK( LWORK ) )
      ALLOCATE( IWORK( LIWORK ) )
      ALLOCATE( RWORK( LRWORK ) )
      ALLOCATE( WORK2( 2 * LWORK ) )
*
      DO 140 IM = 1, NM
         M = MVAL( IM )
         LDA = MAX( 1, M )
*
         DO 130 IN = 1, NN
            N = NVAL( IN )
            MNMIN = MAX(MIN( M, N ),1)
            LDB = MAX( 1, M, N )
            MB = (MNMIN+1)
*
            DO 120 INS = 1, NNS
               NRHS = NSVAL( INS )
*
               DO 110 IRANK = 1, 2
                  DO 100 ISCALE = 1, 3
                     ITYPE = ( IRANK-1 )*3 + ISCALE
                     IF( .NOT.DOTYPE( ITYPE ) )
     $                  GO TO 100
*                 =====================================================
*                       Begin test CGELS
*                 =====================================================
                     IF( IRANK.EQ.1 ) THEN
*
*                       Generate a matrix of scaling type ISCALE
*
                        CALL CQRT13( ISCALE, M, N, COPYA, LDA, NORMA,
     $                               ISEED )
*
*                       Loop for testing different block sizes.
*
                        DO INB = 1, NNB
                           NB = NBVAL( INB )
                           CALL XLAENV( 1, NB )
                           CALL XLAENV( 3, NXVAL( INB ) )
*
*                          Loop for testing non-transposed and transposed.
*
                           DO ITRAN = 1, 2
                              IF( ITRAN.EQ.1 ) THEN
                                 TRANS = 'N'
                                 NROWS = M
                                 NCOLS = N
                              ELSE
                                 TRANS = 'C'
                                 NROWS = N
                                 NCOLS = M
                              END IF
                              LDWORK = MAX( 1, NCOLS )
*
*                             Set up a consistent rhs
*
                              IF( NCOLS.GT.0 ) THEN
                                 CALL CLARNV( 2, ISEED, NCOLS*NRHS,
     $                                        WORK )
                                 CALL CSSCAL( NCOLS*NRHS,
     $                                        ONE / REAL( NCOLS ), WORK,
     $                                        1 )
                              END IF
                              CALL CGEMM( TRANS, 'No transpose', NROWS,
     $                                    NRHS, NCOLS, CONE, COPYA, LDA,
     $                                    WORK, LDWORK, CZERO, B, LDB )
                              CALL CLACPY( 'Full', NROWS, NRHS, B, LDB,
     $                                     COPYB, LDB )
*
*                             Solve LS or overdetermined system
*
                              IF( M.GT.0 .AND. N.GT.0 ) THEN
                                 CALL CLACPY( 'Full', M, N, COPYA, LDA,
     $                                        A, LDA )
                                 CALL CLACPY( 'Full', NROWS, NRHS,
     $                                        COPYB, LDB, B, LDB )
                              END IF
                              SRNAMT = 'CGELS '
                              CALL CGELS( TRANS, M, N, NRHS, A, LDA, B,
     $                                    LDB, WORK, LWORK, INFO )
*
                              IF( INFO.NE.0 )
     $                           CALL ALAERH( PATH, 'CGELS ', INFO, 0,
     $                                        TRANS, M, N, NRHS, -1, NB,
     $                                        ITYPE, NFAIL, NERRS,
     $                                        NOUT )
*
*                             Test 1: Check correctness of results
*                             for CGELS, compute the residual:
*                             RESID = norm(B - A*X) /
*                             / ( max(m,n) * norm(A) * norm(X) * EPS )
*
                              IF( NROWS.GT.0 .AND. NRHS.GT.0 )
     $                           CALL CLACPY( 'Full', NROWS, NRHS,
     $                                        COPYB, LDB, C, LDB )
                              CALL CQRT16( TRANS, M, N, NRHS, COPYA,
     $                                     LDA, B, LDB, C, LDB, RWORK,
     $                                     RESULT( 1 ) )
*
*                             Test 2: Check correctness of results
*                             for CGELS.
*
                              IF( ( ITRAN.EQ.1 .AND. M.GE.N ) .OR.
     $                            ( ITRAN.EQ.2 .AND. M.LT.N ) ) THEN
*
*                                Solving LS system
*
                                 RESULT( 2 ) = CQRT17( TRANS, 1, M, N,
     $                                         NRHS, COPYA, LDA, B, LDB,
     $                                         COPYB, LDB, C, WORK,
     $                                         LWORK )
                              ELSE
*
*                                Solving overdetermined system
*
                                 RESULT( 2 ) = CQRT14( TRANS, M, N,
     $                                         NRHS, COPYA, LDA, B, LDB,
     $                                         WORK, LWORK )
                              END IF
*
*                             Print information about the tests that
*                             did not pass the threshold.
*
                              DO K = 1, 2
                                 IF( RESULT( K ).GE.THRESH ) THEN
                                    IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 )
     $                                 CALL ALAHD( NOUT, PATH )
                                    WRITE( NOUT, FMT = 9999 )TRANS, M,
     $                                 N, NRHS, NB, ITYPE, K,
     $                                 RESULT( K )
                                    NFAIL = NFAIL + 1
                                 END IF
                              END DO
                              NRUN = NRUN + 2
                           END DO
                        END DO
                     END IF
*                 =====================================================
*                       End test CGELS
*                 =====================================================
*                 =====================================================
*                       Begin test CGELST
*                 =====================================================
                     IF( IRANK.EQ.1 ) THEN
*
*                       Generate a matrix of scaling type ISCALE
*
                        CALL CQRT13( ISCALE, M, N, COPYA, LDA, NORMA,
     $                               ISEED )
*
*                       Loop for testing different block sizes.
*
                        DO INB = 1, NNB
                           NB = NBVAL( INB )
                           CALL XLAENV( 1, NB )
                           CALL XLAENV( 3, NXVAL( INB ) )
*
*                          Loop for testing non-transposed and transposed.
*
                           DO ITRAN = 1, 2
                              IF( ITRAN.EQ.1 ) THEN
                                 TRANS = 'N'
                                 NROWS = M
                                 NCOLS = N
                              ELSE
                                 TRANS = 'C'
                                 NROWS = N
                                 NCOLS = M
                              END IF
                              LDWORK = MAX( 1, NCOLS )
*
*                             Set up a consistent rhs
*
                              IF( NCOLS.GT.0 ) THEN
                                 CALL CLARNV( 2, ISEED, NCOLS*NRHS,
     $                                        WORK )
                                 CALL CSSCAL( NCOLS*NRHS,
     $                                        ONE / REAL( NCOLS ), WORK,
     $                                        1 )
                              END IF
                              CALL CGEMM( TRANS, 'No transpose', NROWS,
     $                                    NRHS, NCOLS, CONE, COPYA, LDA,
     $                                    WORK, LDWORK, CZERO, B, LDB )
                              CALL CLACPY( 'Full', NROWS, NRHS, B, LDB,
     $                                     COPYB, LDB )
*
*                             Solve LS or overdetermined system
*
                              IF( M.GT.0 .AND. N.GT.0 ) THEN
                                 CALL CLACPY( 'Full', M, N, COPYA, LDA,
     $                                        A, LDA )
                                 CALL CLACPY( 'Full', NROWS, NRHS,
     $                                        COPYB, LDB, B, LDB )
                              END IF
                              SRNAMT = 'CGELST'
                              CALL CGELST( TRANS, M, N, NRHS, A, LDA, B,
     $                                    LDB, WORK, LWORK, INFO )
*
                              IF( INFO.NE.0 )
     $                           CALL ALAERH( PATH, 'CGELST', INFO, 0,
     $                                        TRANS, M, N, NRHS, -1, NB,
     $                                        ITYPE, NFAIL, NERRS,
     $                                        NOUT )
*
*                             Test 3: Check correctness of results
*                             for CGELST, compute the residual:
*                             RESID = norm(B - A*X) /
*                             / ( max(m,n) * norm(A) * norm(X) * EPS )
*
                              IF( NROWS.GT.0 .AND. NRHS.GT.0 )
     $                           CALL CLACPY( 'Full', NROWS, NRHS,
     $                                        COPYB, LDB, C, LDB )
                              CALL CQRT16( TRANS, M, N, NRHS, COPYA,
     $                                     LDA, B, LDB, C, LDB, RWORK,
     $                                     RESULT( 3 ) )
*
*                             Test 4: Check correctness of results
*                             for CGELST.
*
                              IF( ( ITRAN.EQ.1 .AND. M.GE.N ) .OR.
     $                            ( ITRAN.EQ.2 .AND. M.LT.N ) ) THEN
*
*                                Solving LS system
*
                                 RESULT( 4 ) = CQRT17( TRANS, 1, M, N,
     $                                         NRHS, COPYA, LDA, B, LDB,
     $                                         COPYB, LDB, C, WORK,
     $                                         LWORK )
                              ELSE
*
*                                Solving overdetermined system
*
                                 RESULT( 4 ) = CQRT14( TRANS, M, N,
     $                                         NRHS, COPYA, LDA, B, LDB,
     $                                         WORK, LWORK )
                              END IF
*
*                             Print information about the tests that
*                             did not pass the threshold.
*
                              DO K = 3, 4
                                 IF( RESULT( K ).GE.THRESH ) THEN
                                    IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 )
     $                                 CALL ALAHD( NOUT, PATH )
                                    WRITE( NOUT, FMT = 9999 )TRANS, M,
     $                                 N, NRHS, NB, ITYPE, K,
     $                                 RESULT( K )
                                    NFAIL = NFAIL + 1
                                 END IF
                              END DO
                              NRUN = NRUN + 2
                           END DO
                        END DO
                     END IF
*                 =====================================================
*                       End test CGELST
*                 =====================================================
*                 =====================================================
*                       Begin test CGELSTSLS
*                 =====================================================
                     IF( IRANK.EQ.1 ) THEN
*
*                       Generate a matrix of scaling type ISCALE
*
                        CALL CQRT13( ISCALE, M, N, COPYA, LDA, NORMA,
     $                               ISEED )
*
*                       Loop for testing different block sizes MB.
*
                        DO INB = 1, NNB
                           MB = NBVAL( INB )
                           CALL XLAENV( 1, MB )
*
*                          Loop for testing different block sizes NB.
*
                           DO IMB = 1, NNB
                              NB = NBVAL( IMB )
                              CALL XLAENV( 2, NB )
*
*                             Loop for testing non-transposed
*                             and transposed.
*
                              DO ITRAN = 1, 2
                                 IF( ITRAN.EQ.1 ) THEN
                                    TRANS = 'N'
                                    NROWS = M
                                    NCOLS = N
                                 ELSE
                                    TRANS = 'C'
                                    NROWS = N
                                    NCOLS = M
                                 END IF
                                 LDWORK = MAX( 1, NCOLS )
*
*                                Set up a consistent rhs
*
                                 IF( NCOLS.GT.0 ) THEN
                                    CALL CLARNV( 2, ISEED, NCOLS*NRHS,
     $                                           WORK )
                                    CALL CSCAL( NCOLS*NRHS,
     $                                          CONE / REAL( NCOLS ),
     $                                          WORK, 1 )
                                 END IF
                                 CALL CGEMM( TRANS, 'No transpose',
     $                                       NROWS, NRHS, NCOLS, CONE,
     $                                       COPYA, LDA, WORK, LDWORK,
     $                                       CZERO, B, LDB )
                                 CALL CLACPY( 'Full', NROWS, NRHS,
     $                                        B, LDB, COPYB, LDB )
*
*                                Solve LS or overdetermined system
*
                                 IF( M.GT.0 .AND. N.GT.0 ) THEN
                                    CALL CLACPY( 'Full', M, N,
     $                                           COPYA, LDA, A, LDA )
                                    CALL CLACPY( 'Full', NROWS, NRHS,
     $                                           COPYB, LDB, B, LDB )
                                 END IF
                                 SRNAMT = 'CGETSLS '
                                 CALL CGETSLS( TRANS, M, N, NRHS, A,
     $                                    LDA, B, LDB, WORK, LWORK,
     $                                    INFO )
                                 IF( INFO.NE.0 )
     $                              CALL ALAERH( PATH, 'CGETSLS ', INFO,
     $                                           0, TRANS, M, N, NRHS,
     $                                           -1, NB, ITYPE, NFAIL,
     $                                           NERRS, NOUT )
*
*                             Test 5: Check correctness of results
*                             for CGETSLS, compute the residual:
*                             RESID = norm(B - A*X) /
*                             / ( max(m,n) * norm(A) * norm(X) * EPS )
*
                                 IF( NROWS.GT.0 .AND. NRHS.GT.0 )
     $                              CALL CLACPY( 'Full', NROWS, NRHS,
     $                                           COPYB, LDB, C, LDB )
                                 CALL CQRT16( TRANS, M, N, NRHS,
     $                                        COPYA, LDA, B, LDB,
     $                                        C, LDB, WORK2,
     $                                        RESULT( 5 ) )
*
*                             Test 6: Check correctness of results
*                             for CGETSLS.
*
                                 IF( ( ITRAN.EQ.1 .AND. M.GE.N ) .OR.
     $                               ( ITRAN.EQ.2 .AND. M.LT.N ) ) THEN
*
*                                   Solving LS system, compute:
*                                   r = norm((B- A*X)**T * A) /
*                                 / (norm(A)*norm(B)*max(M,N,NRHS)*EPS)
*
                                    RESULT( 6 ) = CQRT17( TRANS, 1, M,
     $                                             N, NRHS, COPYA, LDA,
     $                                             B, LDB, COPYB, LDB,
     $                                             C, WORK, LWORK )
                                 ELSE
*
*                                   Solving overdetermined system
*
                                    RESULT( 6 ) = CQRT14( TRANS, M, N,
     $                                             NRHS, COPYA, LDA, B,
     $                                             LDB, WORK, LWORK )
                                 END IF
*
*                                Print information about the tests that
*                                did not pass the threshold.
*
                                 DO K = 5, 6
                                    IF( RESULT( K ).GE.THRESH ) THEN
                                       IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 )
     $                                    CALL ALAHD( NOUT, PATH )
                                       WRITE( NOUT, FMT = 9997 )TRANS,
     $                                    M, N, NRHS, MB, NB, ITYPE, K,
     $                                    RESULT( K )
                                          NFAIL = NFAIL + 1
                                    END IF
                                 END DO
                                 NRUN = NRUN + 2
                              END DO
                           END DO
                        END DO
                     END IF
*                 =====================================================
*                       End test CGELSTSLS
*                 ====================================================
*
*                    Generate a matrix of scaling type ISCALE and rank
*                    type IRANK.
*
                     CALL CQRT15( ISCALE, IRANK, M, N, NRHS, COPYA, LDA,
     $                            COPYB, LDB, COPYS, RANK, NORMA, NORMB,
     $                            ISEED, WORK, LWORK )
*
*                    workspace used: MAX(M+MIN(M,N),NRHS*MIN(M,N),2*N+M)
*
                     LDWORK = MAX( 1, M )
*
*                    Loop for testing different block sizes.
*
                     DO 90 INB = 1, NNB
                        NB = NBVAL( INB )
                        CALL XLAENV( 1, NB )
                        CALL XLAENV( 3, NXVAL( INB ) )
*
*                       Test CGELSY
*
*                       CGELSY:  Compute the minimum-norm solution
*                       X to min( norm( A * X - B ) )
*                       using the rank-revealing orthogonal
*                       factorization.
*
                        CALL CLACPY( 'Full', M, N, COPYA, LDA, A, LDA )
                        CALL CLACPY( 'Full', M, NRHS, COPYB, LDB, B,
     $                               LDB )
*
*                       Initialize vector IWORK.
*
                        DO 70 J = 1, N
                           IWORK( J ) = 0
   70                   CONTINUE
*
                        SRNAMT = 'CGELSY'
                        CALL CGELSY( M, N, NRHS, A, LDA, B, LDB, IWORK,
     $                               RCOND, CRANK, WORK, LWLSY, RWORK,
     $                               INFO )
                        IF( INFO.NE.0 )
     $                     CALL ALAERH( PATH, 'CGELSY', INFO, 0, ' ', M,
     $                                  N, NRHS, -1, NB, ITYPE, NFAIL,
     $                                  NERRS, NOUT )
*
*                       workspace used: 2*MNMIN+NB*NB+NB*MAX(N,NRHS)
*
*                       Test 7:  Compute relative error in svd
*                                workspace: M*N + 4*MIN(M,N) + MAX(M,N)
*
                        RESULT( 7 ) = CQRT12( CRANK, CRANK, A, LDA,
     $                                COPYS, WORK, LWORK, RWORK )
*
*                       Test 8:  Compute error in solution
*                                workspace:  M*NRHS + M
*
                        CALL CLACPY( 'Full', M, NRHS, COPYB, LDB, WORK,
     $                               LDWORK )
                        CALL CQRT16( 'No transpose', M, N, NRHS, COPYA,
     $                               LDA, B, LDB, WORK, LDWORK, RWORK,
     $                               RESULT( 8 ) )
*
*                       Test 9:  Check norm of r'*A
*                                workspace: NRHS*(M+N)
*
                        RESULT( 9 ) = ZERO
                        IF( M.GT.CRANK )
     $                     RESULT( 9 ) = CQRT17( 'No transpose', 1, M,
     $                                   N, NRHS, COPYA, LDA, B, LDB,
     $                                   COPYB, LDB, C, WORK, LWORK )
*
*                       Test 10:  Check if x is in the rowspace of A
*                                workspace: (M+NRHS)*(N+2)
*
                        RESULT( 10 ) = ZERO
*
                        IF( N.GT.CRANK )
     $                     RESULT( 10 ) = CQRT14( 'No transpose', M, N,
     $                                   NRHS, COPYA, LDA, B, LDB,
     $                                   WORK, LWORK )
*
*                       Test CGELSS
*
*                       CGELSS:  Compute the minimum-norm solution
*                       X to min( norm( A * X - B ) )
*                       using the SVD.
*
                        CALL CLACPY( 'Full', M, N, COPYA, LDA, A, LDA )
                        CALL CLACPY( 'Full', M, NRHS, COPYB, LDB, B,
     $                               LDB )
                        SRNAMT = 'CGELSS'
                        CALL CGELSS( M, N, NRHS, A, LDA, B, LDB, S,
     $                               RCOND, CRANK, WORK, LWORK, RWORK,
     $                               INFO )
*
                        IF( INFO.NE.0 )
     $                     CALL ALAERH( PATH, 'CGELSS', INFO, 0, ' ', M,
     $                                  N, NRHS, -1, NB, ITYPE, NFAIL,
     $                                  NERRS, NOUT )
*
*                       workspace used: 3*min(m,n) +
*                                       max(2*min(m,n),nrhs,max(m,n))
*
*                       Test 11:  Compute relative error in svd
*
                        IF( RANK.GT.0 ) THEN
                           CALL SAXPY( MNMIN, -ONE, COPYS, 1, S, 1 )
                           RESULT( 11 ) = SASUM( MNMIN, S, 1 ) /
     $                                    SASUM( MNMIN, COPYS, 1 ) /
     $                                    ( EPS*REAL( MNMIN ) )
                        ELSE
                           RESULT( 11 ) = ZERO
                        END IF
*
*                       Test 12:  Compute error in solution
*
                        CALL CLACPY( 'Full', M, NRHS, COPYB, LDB, WORK,
     $                               LDWORK )
                        CALL CQRT16( 'No transpose', M, N, NRHS, COPYA,
     $                               LDA, B, LDB, WORK, LDWORK, RWORK,
     $                               RESULT( 12 ) )
*
*                       Test 13:  Check norm of r'*A
*
                        RESULT( 13 ) = ZERO
                        IF( M.GT.CRANK )
     $                     RESULT( 13 ) = CQRT17( 'No transpose', 1, M,
     $                                    N, NRHS, COPYA, LDA, B, LDB,
     $                                    COPYB, LDB, C, WORK, LWORK )
*
*                       Test 14:  Check if x is in the rowspace of A
*
                        RESULT( 14 ) = ZERO
                        IF( N.GT.CRANK )
     $                     RESULT( 14 ) = CQRT14( 'No transpose', M, N,
     $                                    NRHS, COPYA, LDA, B, LDB,
     $                                    WORK, LWORK )
*
*                       Test CGELSD
*
*                       CGELSD:  Compute the minimum-norm solution X
*                       to min( norm( A * X - B ) ) using a
*                       divide and conquer SVD.
*
                        CALL XLAENV( 9, 25 )
*
                        CALL CLACPY( 'Full', M, N, COPYA, LDA, A, LDA )
                        CALL CLACPY( 'Full', M, NRHS, COPYB, LDB, B,
     $                               LDB )
*
                        SRNAMT = 'CGELSD'
                        CALL CGELSD( M, N, NRHS, A, LDA, B, LDB, S,
     $                               RCOND, CRANK, WORK, LWORK, RWORK,
     $                               IWORK, INFO )
                        IF( INFO.NE.0 )
     $                     CALL ALAERH( PATH, 'CGELSD', INFO, 0, ' ', M,
     $                                  N, NRHS, -1, NB, ITYPE, NFAIL,
     $                                  NERRS, NOUT )
*
*                       Test 15:  Compute relative error in svd
*
                        IF( RANK.GT.0 ) THEN
                           CALL SAXPY( MNMIN, -ONE, COPYS, 1, S, 1 )
                           RESULT( 15 ) = SASUM( MNMIN, S, 1 ) /
     $                                    SASUM( MNMIN, COPYS, 1 ) /
     $                                    ( EPS*REAL( MNMIN ) )
                        ELSE
                           RESULT( 15 ) = ZERO
                        END IF
*
*                       Test 16:  Compute error in solution
*
                        CALL CLACPY( 'Full', M, NRHS, COPYB, LDB, WORK,
     $                               LDWORK )
                        CALL CQRT16( 'No transpose', M, N, NRHS, COPYA,
     $                               LDA, B, LDB, WORK, LDWORK, RWORK,
     $                               RESULT( 16 ) )
*
*                       Test 17:  Check norm of r'*A
*
                        RESULT( 17 ) = ZERO
                        IF( M.GT.CRANK )
     $                     RESULT( 17 ) = CQRT17( 'No transpose', 1, M,
     $                                    N, NRHS, COPYA, LDA, B, LDB,
     $                                    COPYB, LDB, C, WORK, LWORK )
*
*                       Test 18:  Check if x is in the rowspace of A
*
                        RESULT( 18 ) = ZERO
                        IF( N.GT.CRANK )
     $                     RESULT( 18 ) = CQRT14( 'No transpose', M, N,
     $                                    NRHS, COPYA, LDA, B, LDB,
     $                                    WORK, LWORK )
*
*                       Print information about the tests that did not
*                       pass the threshold.
*
                        DO 80 K = 7, 18
                           IF( RESULT( K ).GE.THRESH ) THEN
                              IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 )
     $                           CALL ALAHD( NOUT, PATH )
                              WRITE( NOUT, FMT = 9998 )M, N, NRHS, NB,
     $                           ITYPE, K, RESULT( K )
                              NFAIL = NFAIL + 1
                           END IF
   80                   CONTINUE
                        NRUN = NRUN + 12
*
   90                CONTINUE
  100             CONTINUE
  110          CONTINUE
  120       CONTINUE
  130    CONTINUE
  140 CONTINUE
*
*     Print a summary of the results.
*
      CALL ALASVM( PATH, NOUT, NFAIL, NRUN, NERRS )
*
 9999 FORMAT( ' TRANS=''', A1, ''', M=', I5, ', N=', I5, ', NRHS=', I4,
     $      ', NB=', I4, ', type', I2, ', test(', I2, ')=', G12.5 )
 9998 FORMAT( ' M=', I5, ', N=', I5, ', NRHS=', I4, ', NB=', I4,
     $      ', type', I2, ', test(', I2, ')=', G12.5 )
 9997 FORMAT( ' TRANS=''', A1,' M=', I5, ', N=', I5, ', NRHS=', I4,
     $      ', MB=', I4,', NB=', I4,', type', I2,
     $      ', test(', I2, ')=', G12.5 )
*
      DEALLOCATE( WORK )
      DEALLOCATE( RWORK )
      DEALLOCATE( IWORK )
      RETURN
*
*     End of CDRVLS
*
      END
