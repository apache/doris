*> \brief \b DCHKQP3RK
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*      SUBROUTINE DCHKQP3RK( DOTYPE, NM, MVAL, NN, NVAL, NNS, NSVAL,
*     $                      NNB, NBVAL, NXVAL, THRESH, A, COPYA,
*     $                      B, COPYB, S, TAU,
*     $                      WORK, IWORK, NOUT )
*      IMPLICIT NONE
*
*  -- LAPACK test routine --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*
*     .. Scalar Arguments ..
*      INTEGER            NM, NN, NNS, NNB, NOUT
*      DOUBLE PRECISION   THRESH
*     ..
*     .. Array Arguments ..
*      LOGICAL            DOTYPE( * )
*      INTEGER            IWORK( * ), MVAL( * ), NBVAL( * ), NSVAL( * ),
*     $                   NVAL( * ), NXVAL( * )
*      DOUBLE PRECISION   A( * ), COPYA( * ), B( * ), COPYB( * ),
*     $                   S( * ), TAU( * ), WORK( * )
*     ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DCHKQP3RK tests DGEQP3RK.
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
*> \param[in] THRESH
*> \verbatim
*>          THRESH is DOUBLE PRECISION
*>          The threshold value for the test ratios.  A result is
*>          included in the output file if RESULT >= THRESH.  To have
*>          every test ratio printed, use THRESH = 0.
*> \endverbatim
*>
*> \param[out] A
*> \verbatim
*>          A is DOUBLE PRECISION array, dimension (MMAX*NMAX)
*>          where MMAX is the maximum value of M in MVAL and NMAX is the
*>          maximum value of N in NVAL.
*> \endverbatim
*>
*> \param[out] COPYA
*> \verbatim
*>          COPYA is DOUBLE PRECISION array, dimension (MMAX*NMAX)
*> \endverbatim
*>
*> \param[out] B
*> \verbatim
*>          B is DOUBLE PRECISION array, dimension (MMAX*NSMAX)
*>          where MMAX is the maximum value of M in MVAL and NSMAX is the
*>          maximum value of NRHS in NSVAL.
*> \endverbatim
*>
*> \param[out] COPYB
*> \verbatim
*>          COPYB is DOUBLE PRECISION array, dimension (MMAX*NSMAX)
*> \endverbatim
*>
*> \param[out] S
*> \verbatim
*>          S is DOUBLE PRECISION array, dimension
*>                      (min(MMAX,NMAX))
*> \endverbatim
*>
*> \param[out] TAU
*> \verbatim
*>          TAU is DOUBLE PRECISION array, dimension (MMAX)
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension
*>                      (MMAX*NMAX + 4*NMAX + MMAX)
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (2*NMAX)
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
*> \ingroup double_lin
*
*  =====================================================================
      SUBROUTINE DCHKQP3RK( DOTYPE, NM, MVAL, NN, NVAL, NNS, NSVAL,
     $                      NNB, NBVAL, NXVAL, THRESH, A, COPYA,
     $                      B, COPYB, S, TAU,
     $                      WORK, IWORK, NOUT )
      IMPLICIT NONE
*
*  -- LAPACK test routine --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*
*     .. Scalar Arguments ..
      INTEGER            NM, NN, NNB, NNS, NOUT
      DOUBLE PRECISION   THRESH
*     ..
*     .. Array Arguments ..
      LOGICAL            DOTYPE( * )
      INTEGER            IWORK( * ), NBVAL( * ), MVAL( * ), NVAL( * ),
     $                   NSVAL( * ), NXVAL( * )
      DOUBLE PRECISION   A( * ), COPYA( * ), B( * ), COPYB( * ),
     $                   S( * ), TAU( * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      INTEGER            NTYPES
      PARAMETER          ( NTYPES = 19 )
      INTEGER            NTESTS
      PARAMETER          ( NTESTS = 5 )
      DOUBLE PRECISION   ONE, ZERO, BIGNUM
      PARAMETER          ( ONE = 1.0D+0, ZERO = 0.0D+0,
     $                     BIGNUM = 1.0D+38 )
*     ..
*     .. Local Scalars ..
      CHARACTER          DIST, TYPE
      CHARACTER*3        PATH
      INTEGER            I, IHIGH, ILOW, IM, IMAT, IN, INC_ZERO,
     $                   INB, IND_OFFSET_GEN,
     $                   IND_IN, IND_OUT, INS, INFO,
     $                   ISTEP, J, J_INC, J_FIRST_NZ, JB_ZERO,
     $                   KFACT, KL, KMAX, KU, LDA, LW, LWORK,
     $                   LWORK_MQR, M, MINMN, MINMNB_GEN, MODE, N,
     $                   NB, NB_ZERO, NERRS, NFAIL, NB_GEN, NRHS,
     $                   NRUN, NX, T
      DOUBLE PRECISION   ANORM, CNDNUM, EPS, ABSTOL, RELTOL,
     $                   DTEMP, MAXC2NRMK, RELMAXC2NRMK
*     ..
*     .. Local Arrays ..
      INTEGER            ISEED( 4 ), ISEEDY( 4 )
      DOUBLE PRECISION   RESULT( NTESTS ), RDUMMY( 1 )
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH, DQPT01, DQRT11, DQRT12, DLANGE,
     $                   DLAPY2
      EXTERNAL           DLAMCH, DQPT01, DQRT11, DQRT12, DLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAERH, ALAHD, ALASUM, DAXPY, DGEQP3RK,
     $                   DLACPY, DLAORD, DLASET, DLATB4, DLATMS,
     $                   DORMQR, DSWAP, ICOPY, XLAENV
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, MAX, MIN, MOD
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
      PATH( 1: 1 ) = 'Double precision'
      PATH( 2: 3 ) = 'QK'
      NRUN = 0
      NFAIL = 0
      NERRS = 0
      DO I = 1, 4
         ISEED( I ) = ISEEDY( I )
      END DO
      EPS = DLAMCH( 'Epsilon' )
      INFOT = 0
*
      DO IM = 1, NM
*
*        Do for each value of M in MVAL.
*
         M = MVAL( IM )
         LDA = MAX( 1, M )
*
         DO IN = 1, NN
*
*           Do for each value of N in NVAL.
*
            N = NVAL( IN )
            MINMN = MIN( M, N )
            LWORK = MAX( 1, M*MAX( M, N )+4*MINMN+MAX( M, N ),
     $                   M*N + 2*MINMN + 4*N )
*
            DO INS = 1, NNS
               NRHS = NSVAL( INS )
*
*                 Set up parameters with DLATB4 and generate
*                 M-by-NRHS B matrix with DLATMS.
*                 IMAT = 14:
*                 Random matrix, CNDNUM = 2, NORM = ONE,
*                 MODE = 3 (geometric distribution of singular values).
*
                  CALL DLATB4( PATH, 14, M, NRHS, TYPE, KL, KU, ANORM,
     $                         MODE, CNDNUM, DIST )
*
                  SRNAMT = 'DLATMS'
                  CALL DLATMS( M, NRHS, DIST, ISEED, TYPE, S, MODE,
     $                         CNDNUM, ANORM, KL, KU, 'No packing',
     $                         COPYB, LDA, WORK, INFO )


*
*                 Check error code from DLATMS.
*
                  IF( INFO.NE.0 ) THEN
                     CALL ALAERH( PATH, 'DLATMS', INFO, 0, ' ', M,
     $                            NRHS, -1, -1, -1, 6, NFAIL, NERRS,
     $                            NOUT )
                     CYCLE
                  END IF
*
               DO IMAT = 1, NTYPES
*
*              Do the tests only if DOTYPE( IMAT ) is true.
*
               IF( .NOT.DOTYPE( IMAT ) )
     $            CYCLE
*
*              The type of distribution used to generate the random
*              eigen-/singular values:
*              ( 'S' for symmetric distribution ) => UNIFORM( -1, 1 )
*
*           Do for each type of NON-SYMMETRIC matrix:                               CNDNUM                     NORM                                     MODE
*            1. Zero matrix
*            2. Random, Diagonal, CNDNUM = 2                                        CNDNUM = 2                      ONE                                      3 ( geometric distribution of singular values )
*            3. Random, Upper triangular, CNDNUM = 2                                CNDNUM = 2                      ONE                                      3 ( geometric distribution of singular values )
*            4. Random, Lower triangular, CNDNUM = 2                                CNDNUM = 2                      ONE                                      3 ( geometric distribution of singular values )
*            5. Random, First column is zero, CNDNUM = 2                            CNDNUM = 2                      ONE                                      3 ( geometric distribution of singular values )
*            6. Random, Last MINMN column is zero, CNDNUM = 2                       CNDNUM = 2                      ONE                                      3 ( geometric distribution of singular values )
*            7. Random, Last N column is zero, CNDNUM = 2                           CNDNUM = 2                      ONE                                      3 ( geometric distribution of singular values )
*            8. Random, Middle column in MINMN is zero, CNDNUM = 2                  CNDNUM = 2                      ONE                                      3 ( geometric distribution of singular values )
*            9. Random, First half of MINMN columns are zero, CNDNUM = 2            CNDNUM = 2                      ONE                                      3 ( geometric distribution of singular values )
*           10. Random, Last columns are zero starting from MINMN/2+1, CNDNUM = 2   CNDNUM = 2                      ONE                                      3 ( geometric distribution of singular values )
*           11. Random, Half MINMN columns in the middle are zero starting
*                  from  MINMN/2-(MINMN/2)/2+1, CNDNUM = 2                          CNDNUM = 2                      ONE                                      3 ( geometric distribution of singular values )
*           12. Random, Odd columns are ZERO, CNDNUM = 2                            CNDNUM = 2                      ONE                                      3 ( geometric distribution of singular values )
*           13. Random, Even columns are ZERO, CNDNUM = 2                           CNDNUM = 2                      ONE                                      3 ( geometric distribution of singular values )
*           14. Random, CNDNUM = 2                                                  CNDNUM = 2                      ONE                                      3 ( geometric distribution of singular values )
*           15. Random, CNDNUM = sqrt(0.1/EPS)                                      CNDNUM = BADC1 = sqrt(0.1/EPS)  ONE                                      3 ( geometric distribution of singular values )
*           16. Random, CNDNUM = 0.1/EPS                                            CNDNUM = BADC2 = 0.1/EPS        ONE                                      3 ( geometric distribution of singular values )
*           17. Random, CNDNUM = 0.1/EPS,                                           CNDNUM = BADC2 = 0.1/EPS        ONE                                      2 ( one small singular value, S(N)=1/CNDNUM )
*                 one small singular value S(N)=1/CNDNUM
*           18. Random, CNDNUM = 2, scaled near underflow                           CNDNUM = 2                      SMALL = SAFMIN
*           19. Random, CNDNUM = 2, scaled near overflow                            CNDNUM = 2                      LARGE = 1.0/( 0.25 * ( SAFMIN / EPS ) )  3 ( geometric distribution of singular values )
*
               IF( IMAT.EQ.1 ) THEN
*
*                 Matrix 1: Zero matrix
*
                  CALL DLASET( 'Full', M, N, ZERO, ZERO, COPYA, LDA )
                  DO I = 1, MINMN
                     S( I ) = ZERO
                  END DO
*
               ELSE IF( (IMAT.GE.2 .AND. IMAT.LE.4 )
     $                  .OR. (IMAT.GE.14 .AND. IMAT.LE.19 ) ) THEN
*
*                 Matrices 2-5.
*
*                 Set up parameters with DLATB4 and generate a test
*                 matrix with DLATMS.
*
                  CALL DLATB4( PATH, IMAT, M, N, TYPE, KL, KU, ANORM,
     $                         MODE, CNDNUM, DIST )
*
                  SRNAMT = 'DLATMS'
                  CALL DLATMS( M, N, DIST, ISEED, TYPE, S, MODE,
     $                         CNDNUM, ANORM, KL, KU, 'No packing',
     $                         COPYA, LDA, WORK, INFO )
*
*                 Check error code from DLATMS.
*
                  IF( INFO.NE.0 ) THEN
                     CALL ALAERH( PATH, 'DLATMS', INFO, 0, ' ', M, N,
     $                            -1, -1, -1, IMAT, NFAIL, NERRS,
     $                            NOUT )
                     CYCLE
                  END IF
*
                  CALL DLAORD( 'Decreasing', MINMN, S, 1 )
*
               ELSE IF( MINMN.GE.2
     $                  .AND. IMAT.GE.5 .AND. IMAT.LE.13 ) THEN
*
*                 Rectangular matrices 5-13 that contain zero columns,
*                 only for matrices MINMN >=2.
*
*                 JB_ZERO is the column index of ZERO block.
*                 NB_ZERO is the column block size of ZERO block.
*                 NB_GEN is the column blcok size of the
*                 generated block.
*                 J_INC in the non_zero column index increment
*                 for matrix 12 and 13.
*                 J_FIRS_NZ is the index of the first non-zero
*                 column.
*
                  IF( IMAT.EQ.5 ) THEN
*
*                    First column is zero.
*
                     JB_ZERO = 1
                     NB_ZERO = 1
                     NB_GEN = N - NB_ZERO
*
                  ELSE IF( IMAT.EQ.6 ) THEN
*
*                    Last column MINMN is zero.
*
                     JB_ZERO = MINMN
                     NB_ZERO = 1
                     NB_GEN = N - NB_ZERO
*
                  ELSE IF( IMAT.EQ.7 ) THEN
*
*                    Last column N is zero.
*
                     JB_ZERO = N
                     NB_ZERO = 1
                     NB_GEN = N - NB_ZERO
*
                  ELSE IF( IMAT.EQ.8 ) THEN
*
*                    Middle column in MINMN is zero.
*
                     JB_ZERO = MINMN / 2 + 1
                     NB_ZERO = 1
                     NB_GEN = N - NB_ZERO
*
                  ELSE IF( IMAT.EQ.9 ) THEN
*
*                    First half of MINMN columns is zero.
*
                     JB_ZERO = 1
                     NB_ZERO = MINMN / 2
                     NB_GEN = N - NB_ZERO
*
                  ELSE IF( IMAT.EQ.10 ) THEN
*
*                    Last columns are zero columns,
*                    starting from (MINMN / 2 + 1) column.
*
                     JB_ZERO = MINMN / 2 + 1
                     NB_ZERO = N - JB_ZERO + 1
                     NB_GEN = N - NB_ZERO
*
                  ELSE IF( IMAT.EQ.11 ) THEN
*
*                    Half of the columns in the middle of MINMN
*                    columns is zero, starting from
*                    MINMN/2 - (MINMN/2)/2 + 1 column.
*
                     JB_ZERO = MINMN / 2 - (MINMN / 2) / 2 + 1
                     NB_ZERO = MINMN / 2
                     NB_GEN = N - NB_ZERO
*
                  ELSE IF( IMAT.EQ.12 ) THEN
*
*                    Odd-numbered columns are zero,
*
                     NB_GEN = N / 2
                     NB_ZERO = N - NB_GEN
                     J_INC = 2
                     J_FIRST_NZ = 2
*
                  ELSE IF( IMAT.EQ.13 ) THEN
*
*                    Even-numbered columns are zero.
*
                     NB_ZERO = N / 2
                     NB_GEN = N - NB_ZERO
                     J_INC = 2
                     J_FIRST_NZ = 1
*
                  END IF
*
*
*                 1) Set the first NB_ZERO columns in COPYA(1:M,1:N)
*                    to zero.
*
                  CALL DLASET( 'Full', M, NB_ZERO, ZERO, ZERO,
     $                         COPYA, LDA )
*
*                    2) Generate an M-by-(N-NB_ZERO) matrix with the
*                       chosen singular value distribution
*                       in COPYA(1:M,NB_ZERO+1:N).
*
                  CALL DLATB4( PATH, IMAT, M, NB_GEN, TYPE, KL, KU,
     $                         ANORM, MODE, CNDNUM, DIST )
*
                  SRNAMT = 'DLATMS'
*
                  IND_OFFSET_GEN = NB_ZERO * LDA
*
                  CALL DLATMS( M, NB_GEN, DIST, ISEED, TYPE, S, MODE,
     $                        CNDNUM, ANORM, KL, KU, 'No packing',
     $                        COPYA( IND_OFFSET_GEN + 1 ), LDA,
     $                        WORK, INFO )
*
*                 Check error code from DLATMS.
*
                  IF( INFO.NE.0 ) THEN
                     CALL ALAERH( PATH, 'DLATMS', INFO, 0, ' ', M,
     $                            NB_GEN, -1, -1, -1, IMAT, NFAIL,
     $                            NERRS, NOUT )
                     CYCLE
                  END IF
*
*                 3) Swap the gererated colums from the right side
*                 NB_GEN-size block in COPYA into correct column
*                 positions.
*
                  IF( IMAT.EQ.6
     $                    .OR. IMAT.EQ.7
     $                    .OR. IMAT.EQ.8
     $                    .OR. IMAT.EQ.10
     $                    .OR. IMAT.EQ.11 ) THEN
*
*                    Move by swapping the generated columns
*                    from the right NB_GEN-size block from
*                    (NB_ZERO+1:NB_ZERO+JB_ZERO)
*                    into columns (1:JB_ZERO-1).
*
                     DO J = 1, JB_ZERO-1, 1
                        CALL DSWAP( M,
     $                        COPYA( ( NB_ZERO+J-1)*LDA+1), 1,
     $                        COPYA( (J-1)*LDA + 1 ), 1 )
                     END DO
*
                  ELSE IF( IMAT.EQ.12 .OR. IMAT.EQ.13 ) THEN
*
*                    ( IMAT = 12, Odd-numbered ZERO columns. )
*                    Swap the generated columns from the right
*                    NB_GEN-size block into the even zero colums in the
*                    left NB_ZERO-size block.
*
*                    ( IMAT = 13, Even-numbered ZERO columns. )
*                    Swap the generated columns from the right
*                    NB_GEN-size block into the odd zero colums in the
*                    left NB_ZERO-size block.
*
                     DO J = 1, NB_GEN, 1
                        IND_OUT = ( NB_ZERO+J-1 )*LDA + 1
                        IND_IN = ( J_INC*(J-1)+(J_FIRST_NZ-1) )*LDA
     $                            + 1
                        CALL DSWAP( M,
     $                              COPYA( IND_OUT ), 1,
     $                              COPYA( IND_IN), 1 )
                        END DO
*
                  END IF
*
*                 5) Order the singular values generated by
*                    DLAMTS in decreasing order and add trailing zeros
*                    that correspond to zero columns.
*                    The total number of singular values is MINMN.
*
                  MINMNB_GEN = MIN( M, NB_GEN )
*
                  DO I = MINMNB_GEN+1, MINMN
                     S( I ) = ZERO
                  END DO
*
               ELSE
*
*                    IF(MINMN.LT.2) skip this size for this matrix type.
*
                     CYCLE
               END IF
*
*              Initialize a copy array for a pivot array for DGEQP3RK.
*
               DO I = 1, N
                  IWORK( I ) = 0
               END DO
*
               DO INB = 1, NNB
*
*                 Do for each pair of values (NB,NX) in NBVAL and NXVAL.
*
                  NB = NBVAL( INB )
                  CALL XLAENV( 1, NB )
                  NX = NXVAL( INB )
                  CALL XLAENV( 3, NX )
*
*                 We do MIN(M,N)+1 because we need a test for KMAX > N,
*                 when KMAX is larger than MIN(M,N), KMAX should be
*                 KMAX = MIN(M,N)
*
                  DO KMAX = 0, MIN(M,N)+1
*
*                 Get a working copy of COPYA into A( 1:M,1:N ).
*                 Get a working copy of COPYB into A( 1:M, (N+1):NRHS ).
*                 Get a working copy of COPYB into into B( 1:M, 1:NRHS ).
*                 Get a working copy of IWORK(1:N) awith zeroes into
*                 which is going to be used as pivot array IWORK( N+1:2N ).
*                 NOTE: IWORK(2N+1:3N) is going to be used as a WORK array
*                 for the routine.
*
                  CALL DLACPY( 'All', M, N, COPYA, LDA, A, LDA )
                  CALL DLACPY( 'All', M, NRHS, COPYB, LDA,
     $                         A( LDA*N + 1 ),  LDA )
                  CALL DLACPY( 'All', M, NRHS, COPYB, LDA,
     $                         B,  LDA )
                  CALL ICOPY( N, IWORK( 1 ), 1, IWORK( N+1 ), 1 )
                   DO I = 1, NTESTS
                     RESULT( I ) = ZERO
                   END DO
*
                  ABSTOL = -1.0
                  RELTOL = -1.0
*
*                 Compute the QR factorization with pivoting of A
*
                  LW = MAX( 1, MAX( 2*N + NB*( N+NRHS+1 ),
     $                              3*N + NRHS - 1 ) )
*
*                 Compute DGEQP3RK factorization of A.
*
                  SRNAMT = 'DGEQP3RK'
                  CALL DGEQP3RK( M, N, NRHS, KMAX, ABSTOL, RELTOL,
     $                           A, LDA, KFACT, MAXC2NRMK,
     $                           RELMAXC2NRMK, IWORK( N+1 ), TAU,
     $                           WORK, LW, IWORK( 2*N+1 ), INFO )
*
*                 Check error code from DGEQP3RK.
*
                  IF( INFO.LT.0 )
     $               CALL ALAERH( PATH, 'DGEQP3RK', INFO, 0, ' ',
     $                            M, N, NX, -1, NB, IMAT,
     $                            NFAIL, NERRS, NOUT )
*
*                 Compute test 1:
*
*                 This test in only for the full rank factorization of
*                 the matrix A.
*
*                 Array S(1:min(M,N)) contains svd(A) the sigular values
*                 of the original matrix A in decreasing absolute value
*                 order. The test computes svd(R), the vector sigular
*                 values of the upper trapezoid of A(1:M,1:N) that
*                 contains the factor R, in decreasing order. The test
*                 returns the ratio:
*
*                 2-norm(svd(R) - svd(A)) / ( max(M,N) * 2-norm(svd(A)) * EPS )
*
                  IF( KFACT.EQ.MINMN ) THEN
*
                     RESULT( 1 ) = DQRT12( M, N, A, LDA, S, WORK,
     $                                     LWORK )
*
                     NRUN = NRUN + 1
*
*                   End test 1
*
                  END IF
*
*                 Compute test 2:
*
*                 The test returns the ratio:
*
*                 1-norm( A*P - Q*R ) / ( max(M,N) * 1-norm(A) * EPS )
*
                  RESULT( 2 ) = DQPT01( M, N, KFACT, COPYA, A, LDA, TAU,
     $                                  IWORK( N+1 ), WORK, LWORK )
*
*                 Compute test 3:
*
*                 The test returns the ratio:
*
*                 1-norm( Q**T * Q - I ) / ( M * EPS )
*
                  RESULT( 3 ) = DQRT11( M, KFACT, A, LDA, TAU, WORK,
     $                                  LWORK )
*
                  NRUN = NRUN + 2
*
*                 Compute test 4:
*
*                 This test is only for the factorizations with the
*                 rank greater than 2.
*                 The elements on the diagonal of R should be non-
*                 increasing.
*
*                 The test returns the ratio:
*
*                 Returns 1.0D+100 if abs(R(K+1,K+1)) > abs(R(K,K)),
*                 K=1:KFACT-1
*
                  IF( MIN(KFACT, MINMN).GE.2 ) THEN
*
                     DO J = 1, KFACT-1, 1

                        DTEMP = (( ABS( A( (J-1)*LDA+J ) ) -
     $                          ABS( A( (J)*LDA+J+1 ) ) ) /
     $                          ABS( A(1) ) )
*
                        IF( DTEMP.LT.ZERO ) THEN
                           RESULT( 4 ) = BIGNUM
                        END IF
*
                     END DO
*
                     NRUN = NRUN + 1
*
*                    End test 4.
*
                  END IF
*
*                 Compute test 5:
*
*                 This test in only for matrix A with min(M,N) > 0.
*
*                 The test returns the ratio:
*
*                 1-norm(Q**T * B - Q**T * B ) /
*                       ( M * EPS )
*
*                 (1) Compute B:=Q**T * B in the matrix B.
*
                  IF( MINMN.GT.0 ) THEN
*
                     LWORK_MQR = MAX(1, NRHS)
                     CALL DORMQR( 'Left', 'Transpose',
     $                            M, NRHS, KFACT, A, LDA, TAU, B, LDA,
     $                            WORK, LWORK_MQR, INFO )
*
                     DO I = 1, NRHS
*
*                       Compare N+J-th column of A and J-column of B.
*
                        CALL DAXPY( M, -ONE, A( ( N+I-1 )*LDA+1 ), 1,
     $                              B( ( I-1 )*LDA+1 ), 1 )
                     END DO
*
                     RESULT( 5 ) = ABS(
     $                  DLANGE( 'One-norm', M, NRHS, B, LDA, RDUMMY ) /
     $                  ( DBLE( M )*DLAMCH( 'Epsilon' ) ) )
*
                     NRUN = NRUN + 1
*
*                    End compute test 5.
*
                  END IF
*
*                 Print information about the tests that did not
*                 pass the threshold.
*
                  DO T = 1, NTESTS
                     IF( RESULT( T ).GE.THRESH ) THEN
                        IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 )
     $                     CALL ALAHD( NOUT, PATH )
                        WRITE( NOUT, FMT = 9999 ) 'DGEQP3RK', M, N,
     $                     NRHS, KMAX, ABSTOL, RELTOL, NB, NX,
     $                     IMAT, T, RESULT( T )
                        NFAIL = NFAIL + 1
                     END IF
                  END DO
*
*                 END DO KMAX = 1, MIN(M,N)+1
*
                  END DO
*
*                 END DO for INB = 1, NNB
*
               END DO
*
*              END DO  for IMAT = 1, NTYPES
*
               END DO
*
*              END DO for INS = 1, NNS
*
            END DO
*
*           END DO for IN = 1, NN
*
         END DO
*
*        END DO for IM = 1, NM
*
      END DO
*
*     Print a summary of the results.
*
      CALL ALASUM( PATH, NOUT, NFAIL, NRUN, NERRS )
*
 9999 FORMAT( 1X, A, ' M =', I5, ', N =', I5, ', NRHS =', I5,
     $        ', KMAX =', I5, ', ABSTOL =', G12.5,
     $        ', RELTOL =', G12.5, ', NB =', I4, ', NX =', I4,
     $        ', type ', I2, ', test ', I2, ', ratio =', G12.5 )
*
*     End of DCHKQP3RK
*
      END
