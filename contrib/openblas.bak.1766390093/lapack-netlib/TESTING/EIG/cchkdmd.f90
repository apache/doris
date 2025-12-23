!   This is a test program for checking the implementations of
!   the implementations of the following subroutines
!
!   CGEDMD,  for computation of the
!            Dynamic Mode Decomposition (DMD)
!   CGEDMDQ, for computation of a
!            QR factorization based compressed DMD
!
!   Developed and supported by:
!   ===========================
!   Developed and coded by Zlatko Drmac, Faculty of Science,
!   University of Zagreb;  drmac@math.hr
!   In cooperation with
!   AIMdyn Inc., Santa Barbara, CA.
!   ========================================================
!   How to run the code (compiler, link info)
!   ========================================================
!   Compile as FORTRAN 90 (or later) and link with BLAS and
!   LAPACK libraries.
!   NOTE: The code is developed and tested on top of the
!   Intel MKL library (versions 2022.0.3 and 2022.2.0),
!   using the Intel Fortran compiler.
!
!   For developers of the C++ implementation
!   ========================================================
!   See the LAPACK++ and Template Numerical Toolkit (TNT)
!
!   Note on a development of the GPU HP implementation
!   ========================================================
!   Work in progress. See CUDA, MAGMA, SLATE.
!   NOTE: The four SVD subroutines used in this code are
!   included as a part of R&D and for the completeness.
!   This was also an opportunity to test those SVD codes.
!   If the scaling option is used all four are essentially
!   equally good. For implementations on HP platforms,
!   one can use whichever SVD is available.
!............................................................

!............................................................
!............................................................
!
      PROGRAM DMD_TEST

      use iso_fortran_env
      IMPLICIT NONE
      integer, parameter :: WP = real32
!............................................................
      REAL(KIND=WP), PARAMETER ::  ONE = 1.0_WP
      REAL(KIND=WP), PARAMETER :: ZERO = 0.0_WP

      COMPLEX(KIND=WP), PARAMETER ::  CONE = ( 1.0_WP, 0.0_WP )
      COMPLEX(KIND=WP), PARAMETER :: CZERO = ( 0.0_WP, 0.0_WP )
!............................................................
      REAL(KIND=WP), ALLOCATABLE, DIMENSION(:)   :: RES, &
                     RES1, RESEX, SINGVX, SINGVQX, WORK
      INTEGER      , ALLOCATABLE, DIMENSION(:)   ::   IWORK
      REAL(KIND=WP) :: WDUMMY(2)
      INTEGER       :: IDUMMY(4), ISEED(4)
      REAL(KIND=WP) :: ANORM, COND, CONDL, CONDR, EPS,       &
                       TOL, TOL2, SVDIFF, TMP, TMP_AU,       &
                       TMP_FQR, TMP_REZ, TMP_REZQ,  TMP_XW, &
                       TMP_EX
!............................................................
      COMPLEX(KIND=WP) :: CMAX
      INTEGER :: LCWORK
      COMPLEX(KIND=WP), ALLOCATABLE, DIMENSION(:,:) ::  A, AC,  &
                                 AU, F, F0, F1, S, W,  &
                                 X, X0, Y, Y0, Y1, Z, Z1
      COMPLEX(KIND=WP), ALLOCATABLE, DIMENSION(:)   ::  CDA, CDR, &
                                       CDL, CEIGS, CEIGSA, CWORK
      COMPLEX(KIND=WP) ::  CDUMMY(22), CDUM2X2(2,2)
!............................................................
      INTEGER :: K, KQ, LDF, LDS, LDA, LDAU, LDW, LDX, LDY,  &
                 LDZ, LIWORK, LWORK, M, N, LLOOP, NRNK
      INTEGER :: i, iJOBREF, iJOBZ, iSCALE, INFO, j,     &
                 NFAIL, NFAIL_AU, NFAIL_F_QR, NFAIL_REZ,     &
                 NFAIL_REZQ, NFAIL_SVDIFF, NFAIL_TOTAL, NFAILQ_TOTAL,  &
                 NFAIL_Z_XV,  MODE, MODEL, MODER, WHTSVD
      INTEGER :: iNRNK, iWHTSVD,  K_traj, LWMINOPT
      CHARACTER :: GRADE, JOBREF, JOBZ, PIVTNG, RSIGN,   &
                   SCALE, RESIDS, WANTQ, WANTR
      LOGICAL :: TEST_QRDMD

!..... external subroutines (BLAS and LAPACK)
      EXTERNAL CAXPY, CGEEV, CGEMM, CGEMV, CLASCL
!.....external subroutines DMD package
!     subroutines under test
      EXTERNAL CGEDMD, CGEDMDQ
!..... external functions (BLAS and LAPACK)
      EXTERNAL         SCNRM2, SLAMCH
      REAL(KIND=WP) :: SCNRM2, SLAMCH
      EXTERNAL         CLANGE
      REAL(KIND=WP) :: CLANGE
      EXTERNAL ICAMAX
      INTEGER  ICAMAX
      EXTERNAL LSAME
      LOGICAL  LSAME

      INTRINSIC ABS, INT, MIN, MAX, SIGN
!............................................................


      WRITE(*,*) 'COMPLEX CODE TESTING'

      ! The test is always in pairs : ( CGEDMD and CGEDMDQ)
      ! because the test includes comparing the results (in pairs).
!.....................................................................................
      ! This code by default performs tests on CGEDMDQ
      ! Since the QR factorizations based algorithm is designed for
      ! single trajectory data, only single trajectory tests will
      ! be performed with xGEDMDQ.

      WANTQ = 'Q'
      WANTR = 'R'
!.................................................................................

      EPS = SLAMCH( 'P' )  ! machine precision WP

      ! Global counters of failures of some particular tests
      NFAIL      = 0
      NFAIL_REZ  = 0
      NFAIL_REZQ = 0
      NFAIL_Z_XV = 0
      NFAIL_F_QR = 0
      NFAIL_AU   = 0
      NFAIL_SVDIFF = 0
      NFAIL_TOTAL  = 0
      NFAILQ_TOTAL = 0

      DO LLOOP = 1, 4

      WRITE(*,*) 'L Loop Index = ', LLOOP

      ! Set the dimensions of the problem ...
      READ(*,*) M
      WRITE(*,*) 'M = ', M
      ! ... and the number of snapshots.
      READ(*,*) N
      WRITE(*,*) 'N = ', N

      ! Test the dimensions
      IF ( ( MIN(M,N) == 0 ) .OR. ( M < N )  ) THEN
          WRITE(*,*) 'Bad dimensions. Required: M >= N > 0.'
          STOP
      END IF
!.............
      ! The seed inside the LLOOP so that each pass can be reproduced easily.
      ISEED(1) = 4
      ISEED(2) = 3
      ISEED(3) = 2
      ISEED(4) = 1

      LDA  = M
      LDF  = M
      LDX  = M
      LDY  = M
      LDW  = N
      LDZ  = M
      LDAU = M
      LDS  = N

      TMP_XW  = ZERO
      TMP_AU   = ZERO
      TMP_REZ  = ZERO
      TMP_REZQ = ZERO
      SVDIFF   = ZERO
      TMP_EX   = ZERO

      ALLOCATE( A(LDA,M) )
      ALLOCATE( AC(LDA,M) )
      ALLOCATE( F(LDF,N+1) )
      ALLOCATE( F0(LDF,N+1) )
      ALLOCATE( F1(LDF,N+1) )
      ALLOCATE( X(LDX,N) )
      ALLOCATE( X0(LDX,N) )
      ALLOCATE( Y(LDY,N+1) )
      ALLOCATE( Y0(LDY,N+1) )
      ALLOCATE( Y1(LDY,N+1) )
      ALLOCATE( AU(LDAU,N) )
      ALLOCATE( W(LDW,N) )
      ALLOCATE( S(LDS,N) )
      ALLOCATE( Z(LDZ,N) )
      ALLOCATE( Z1(LDZ,N) )
      ALLOCATE( RES(N) )
      ALLOCATE( RES1(N) )
      ALLOCATE( RESEX(N) )
      ALLOCATE( CEIGS(N) )
      ALLOCATE( SINGVX(N) )
      ALLOCATE( SINGVQX(N) )

      TOL  = 10*M*EPS
      TOL2 = 10*M*N*EPS

!.............

      DO K_traj = 1, 2
      !  Number of intial conditions in the simulation/trajectories (1 or 2)

      COND   = 1.0D4
      CMAX   = (1.0D1,1.0D1)
      RSIGN  = 'F'
      GRADE  = 'N'
      MODEL  = 6
      CONDL  = 1.0D1
      MODER  = 6
      CONDR  = 1.0D1
      PIVTNG = 'N'
      ! Loop over all parameter MODE values for CLATMR (+-1,..,+-6)

      DO MODE = 1, 6

      ALLOCATE( IWORK(2*M) )
      ALLOCATE( CDA(M) )
      ALLOCATE( CDL(M) )
      ALLOCATE( CDR(M) )

      CALL CLATMR( M, M, 'N', ISEED, 'N', CDA, MODE, COND, &
                   CMAX, RSIGN, GRADE, CDL, MODEL,  CONDL, &
                   CDR, MODER, CONDR, PIVTNG, IWORK, M, M, &
                   ZERO, -ONE, 'N', A, LDA, IWORK(M+1), INFO )
      DEALLOCATE( CDR )
      DEALLOCATE( CDL )
      DEALLOCATE( CDA )
      DEALLOCATE( IWORK )

      LCWORK = MAX(1,2*M)
      ALLOCATE( CEIGSA(M) )
      ALLOCATE( CWORK(LCWORK) )
      ALLOCATE( WORK(2*M) )
      AC(1:M,1:M) = A(1:M,1:M)
      CALL CGEEV( 'N','N', M, AC, LDA, CEIGSA, CDUM2X2, 2, &
                  CDUM2X2, 2, CWORK, LCWORK, WORK, INFO ) ! LAPACK CALL
      DEALLOCATE(WORK)
      DEALLOCATE(CWORK)

      TMP = ABS(CEIGSA(ICAMAX(M, CEIGSA, 1))) ! The spectral radius of A
      ! Scale the matrix A to have unit spectral radius.
      CALL CLASCL( 'G',0, 0, TMP, ONE, M, M, &
                   A, LDA, INFO )
      CALL CLASCL( 'G',0, 0, TMP, ONE, M, 1, &
                   CEIGSA, M, INFO )
      ANORM = CLANGE( 'F', M, M, A, LDA, WDUMMY )

      IF ( K_traj == 2 ) THEN
          ! generate data as two trajectories
          ! with two inital conditions
          CALL CLARNV(2, ISEED, M, F(1,1) )
          DO i = 1, N/2
             CALL CGEMV( 'N', M, M, CONE, A, LDA, F(1,i), 1,  &
                  CZERO, F(1,i+1), 1 )
          END DO
          X0(1:M,1:N/2) = F(1:M,1:N/2)
          Y0(1:M,1:N/2) = F(1:M,2:N/2+1)

          CALL CLARNV(2, ISEED, M, F(1,1) )
          DO i = 1, N-N/2
             CALL CGEMV( 'N', M, M, CONE, A, LDA, F(1,i), 1,  &
                  CZERO, F(1,i+1), 1 )
          END DO
          X0(1:M,N/2+1:N) = F(1:M,1:N-N/2)
          Y0(1:M,N/2+1:N) = F(1:M,2:N-N/2+1)
      ELSE
          CALL CLARNV(2, ISEED, M, F(1,1) )
          DO i = 1, N
             CALL CGEMV( 'N', M, M, CONE, A, M, F(1,i), 1,  &
                  CZERO, F(1,i+1), 1 )
          END DO
          F0(1:M,1:N+1) = F(1:M,1:N+1)
          X0(1:M,1:N) = F0(1:M,1:N)
          Y0(1:M,1:N) = F0(1:M,2:N+1)
      END IF

      DEALLOCATE( CEIGSA )
!........................................................................

      DO iJOBZ = 1, 4

          SELECT CASE ( iJOBZ )
          CASE(1)
              JOBZ   = 'V'
              RESIDS = 'R'
          CASE(2)
              JOBZ   = 'V'
              RESIDS = 'N'
          CASE(3)
              JOBZ   = 'F'
              RESIDS = 'N'
          CASE(4)
              JOBZ   = 'N'
              RESIDS = 'N'
          END SELECT

      DO iJOBREF = 1, 3

          SELECT CASE ( iJOBREF )
          CASE(1)
              JOBREF = 'R'
          CASE(2)
              JOBREF = 'E'
          CASE(3)
              JOBREF = 'N'
          END SELECT

      DO iSCALE = 1, 4

          SELECT CASE ( iSCALE )
          CASE(1)
              SCALE = 'S'
          CASE(2)
              SCALE = 'C'
          CASE(3)
              SCALE = 'Y'
          CASE(4)
              SCALE = 'N'
          END SELECT

      DO iNRNK = -1, -2, -1
          NRNK   = iNRNK

      DO iWHTSVD = 1,  3
         ! Check all four options to compute the POD basis
         ! via the SVD.
         WHTSVD   = iWHTSVD

      DO LWMINOPT = 1, 2
         ! Workspace query for the minimal (1) and for the optimal
         ! (2) workspace lengths determined by workspace query.

      ! CGEDMD is always tested and its results are also used for
      ! comparisons with CGEDMDQ.

      X(1:M,1:N) = X0(1:M,1:N)
      Y(1:M,1:N) = Y0(1:M,1:N)

      CALL CGEDMD( SCALE, JOBZ, RESIDS, JOBREF, WHTSVD,  &
                M,  N, X, LDX, Y, LDY, NRNK, TOL,  &
                K, CEIGS, Z, LDZ,  RES,  &
                AU, LDAU, W,  LDW,   S, LDS,        &
                CDUMMY, -1, WDUMMY, -1, IDUMMY, -1, INFO )

      IF ( (INFO .EQ. 2) .OR. ( INFO .EQ. 3 ) &
                       .OR. ( INFO < 0 ) ) THEN
        WRITE(*,*) 'Call to CGEDMD workspace query failed. &
                   &Check the calling sequence and the code.'
        WRITE(*,*) 'The error code is ', INFO
        WRITE(*,*) 'The input parameters were ',      &
        SCALE, JOBZ, RESIDS, JOBREF, WHTSVD,          &
        M, N, LDX, LDY, NRNK, TOL, LDZ, LDAU, LDW, LDS
        STOP
      ELSE
        !WRITE(*,*) '... done. Workspace length computed.'
      END IF

      LCWORK = INT(CDUMMY(LWMINOPT))
      ALLOCATE(CWORK(LCWORK))
      LIWORK = IDUMMY(1)
      ALLOCATE(IWORK(LIWORK))
      LWORK = INT(WDUMMY(1))
      ALLOCATE(WORK(LWORK))

      CALL CGEDMD( SCALE, JOBZ, RESIDS, JOBREF, WHTSVD,  &
                   M,  N, X, LDX, Y, LDY, NRNK, TOL,  &
                   K, CEIGS, Z, LDZ,  RES,  &
                   AU, LDAU, W,  LDW,   S, LDS,        &
                   CWORK, LCWORK, WORK, LWORK, IWORK, LIWORK, INFO )
      IF ( INFO /= 0 ) THEN
           WRITE(*,*) 'Call to CGEDMD failed. &
           &Check the calling sequence and the code.'
           WRITE(*,*) 'The error code is ', INFO
           WRITE(*,*) 'The input parameters were ',&
           SCALE, JOBZ, RESIDS, JOBREF, WHTSVD, &
           M, N, LDX, LDY, NRNK, TOL
           STOP
      END IF
      SINGVX(1:N) = WORK(1:N)

      !...... CGEDMD check point
      IF ( LSAME(JOBZ,'V')  ) THEN
          ! Check that Z = X*W, on return from CGEDMD
          ! This checks that the returned eigenvectors in Z are
          ! the product of the SVD'POD basis returned in X
          ! and the eigenvectors of the Rayleigh quotient
          ! returned in W
          CALL CGEMM( 'N', 'N', M, K, K, CONE, X, LDX, W, LDW, &
                      CZERO, Z1, LDZ )
          TMP = ZERO
          DO i = 1, K
             CALL CAXPY( M, -CONE, Z(1,i), 1, Z1(1,i), 1)
             TMP = MAX(TMP, SCNRM2( M, Z1(1,i), 1 ) )
          END DO
          TMP_XW = MAX(TMP_XW, TMP )
          IF ( TMP_XW <= TOL ) THEN
              !WRITE(*,*) ' :) .... OK .........CGEDMD PASSED.'
          ELSE
              NFAIL_Z_XV = NFAIL_Z_XV + 1
              WRITE(*,*) ':( .................CGEDMD FAILED!', &
                  'Check the code for implementation errors.'
              WRITE(*,*) 'The input parameters were ',&
                 SCALE, JOBZ, RESIDS, JOBREF, WHTSVD, &
                 M, N, LDX, LDY, NRNK, TOL
          END IF
      END IF
      !...... CGEDMD check point

      IF ( LSAME(JOBREF,'R') ) THEN
           ! The matrix A*U is returned for computing refined Ritz vectors.
           ! Check that A*U is computed correctly using the formula
           ! A*U = Y * V * inv(SIGMA). This depends on the
           ! accuracy in the computed singular values and vectors of X.
           ! See the paper for an error analysis.
           ! Note that the left singular vectors of the input matrix X
           ! are returned in the array X.
           CALL CGEMM( 'N', 'N', M, K, M, CONE, A, LDA, X, LDX, &
                      CZERO, Z1, LDZ )
          TMP = ZERO
          DO i = 1, K
             CALL CAXPY( M, -CONE, AU(1,i), 1, Z1(1,i), 1)
             TMP = MAX( TMP, SCNRM2( M, Z1(1,i),1 ) * &
                     SINGVX(K)/(ANORM*SINGVX(1)) )
          END DO
          TMP_AU = MAX( TMP_AU, TMP )
          IF ( TMP <= TOL2 ) THEN
              !WRITE(*,*) ':) .... OK .........CGEDMD PASSED.'
          ELSE
              NFAIL_AU = NFAIL_AU + 1
              WRITE(*,*) ':( .................CGEDMD FAILED!', &
                  'Check the code for implementation errors.'
              WRITE(*,*) 'The input parameters were ',&
                 SCALE, JOBZ, RESIDS, JOBREF, WHTSVD, &
                 M, N, LDX, LDY, NRNK, TOL2
          END IF
      ELSEIF ( LSAME(JOBREF,'E') ) THEN
          ! The unscaled vectors of the Exact DMD are computed.
          ! This option is included for the sake of completeness,
          ! for users who prefer the Exact DMD vectors. The
          ! returned vectors are in the real form, in the same way
          ! as the Ritz vectors. Here we just save the vectors
          ! and test them separately using a Matlab script.
          CALL CGEMM( 'N', 'N', M, K, M, CONE, A, LDA, AU, LDAU, CZERO, Y1, LDY )

          DO i=1, K
             CALL CAXPY( M, -CEIGS(i), AU(1,i), 1, Y1(1,i), 1 )
             RESEX(i) = SCNRM2( M, Y1(1,i), 1) / SCNRM2(M,AU(1,i),1)
          END DO
      END IF
      !...... CGEDMD check point

      IF ( LSAME(RESIDS, 'R') ) THEN
          ! Compare the residuals returned by CGEDMD with the
          ! explicitly computed residuals using the matrix A.
          ! Compute explicitly Y1 = A*Z
          CALL CGEMM( 'N', 'N', M, K, M, CONE, A, LDA, Z, LDZ, CZERO, Y1, LDY )
          ! ... and then A*Z(:,i) - LAMBDA(i)*Z(:,i), using the real forms
          ! of the invariant subspaces that correspond to complex conjugate
          ! pairs of eigencalues. (See the description of Z in CGEDMD,)

          DO i=1, K
                ! have a real eigenvalue with real eigenvector
                CALL CAXPY( M, -CEIGS(i), Z(1,i), 1, Y1(1,i), 1 )
                RES1(i) = SCNRM2( M, Y1(1,i), 1)
          END DO
          TMP = ZERO
          DO i = 1, K
          TMP = MAX( TMP, ABS(RES(i) - RES1(i)) * &
                    SINGVX(K)/(ANORM*SINGVX(1)) )
          END DO
          TMP_REZ = MAX( TMP_REZ, TMP )
          IF ( TMP <= TOL2 ) THEN
              !WRITE(*,*) ':) .... OK ..........CGEDMD PASSED.'
          ELSE
              NFAIL_REZ = NFAIL_REZ + 1
              WRITE(*,*) ':( ..................CGEDMD FAILED!', &
                  'Check the code for implementation errors.'
              WRITE(*,*) 'The input parameters were ',&
                 SCALE, JOBZ, RESIDS, JOBREF, WHTSVD, &
                 M, N, LDX, LDY, NRNK, TOL
          END IF


         IF ( LSAME(JOBREF,'E') ) THEN
            TMP = ZERO
          DO i = 1, K
          TMP = MAX( TMP, ABS(RES1(i) - RESEX(i))/(RES1(i)+RESEX(i)) )
          END DO
          TMP_EX = MAX(TMP_EX,TMP)
         END IF

      END IF

      DEALLOCATE(CWORK)
      DEALLOCATE(WORK)
      DEALLOCATE(IWORK)

!.......................................................................................................

      IF ( K_traj == 1 ) THEN

          F(1:M,1:N+1) = F0(1:M,1:N+1)
          CALL CGEDMDQ( SCALE, JOBZ, RESIDS, WANTQ, WANTR, JOBREF, &
                    WHTSVD, M, N+1, F, LDF,  X, LDX,  Y, LDY,  &
                    NRNK,  TOL, K, CEIGS, Z, LDZ, RES,  AU,  &
                    LDAU, W, LDW, S, LDS, CDUMMY, -1,   &
                    WDUMMY,  -1, IDUMMY, -1, INFO )

          LCWORK = INT(CDUMMY(LWMINOPT))
          ALLOCATE(CWORK(LCWORK))
          LIWORK = IDUMMY(1)
          ALLOCATE(IWORK(LIWORK))
          LWORK = INT(WDUMMY(1))
          ALLOCATE(WORK(LWORK))

          CALL CGEDMDQ( SCALE, JOBZ, RESIDS, WANTQ, WANTR, JOBREF, &
                        WHTSVD, M, N+1, F, LDF,  X, LDX,  Y, LDY,  &
                        NRNK,  TOL, KQ, CEIGS, Z, LDZ, RES,  AU,  &
                        LDAU, W, LDW, S, LDS, CWORK, LCWORK,   &
                        WORK,  LWORK, IWORK, LIWORK, INFO )
          IF ( INFO /= 0 ) THEN
                 WRITE(*,*) 'Call to CGEDMDQ failed. &
                 &Check the calling sequence and the code.'
                 WRITE(*,*) 'The error code is ', INFO
                 WRITE(*,*) 'The input parameters were ',&
                 SCALE, JOBZ, RESIDS, WANTQ, WANTR, WHTSVD, &
                 M, N, LDX, LDY, NRNK, TOL
                 STOP
          END IF
          SINGVQX(1:N) =WORK(1:N)

          !..... ZGEDMDQ check point

          TMP = ZERO
          DO i = 1, MIN(K, KQ)
             TMP = MAX(TMP, ABS(SINGVX(i)-SINGVQX(i)) / &
                                   SINGVX(1) )
          END DO
          SVDIFF = MAX( SVDIFF, TMP )
          IF ( TMP > TOL2 ) THEN
               WRITE(*,*) 'FAILED! Something was wrong with the run.'
             NFAIL_SVDIFF = NFAIL_SVDIFF + 1
          END IF
          !..... CGEDMDQ check point

          !..... CGEDMDQ check point
          IF ( LSAME(WANTQ,'Q') .AND. LSAME(WANTR,'R') ) THEN
             ! Check that the QR factors are computed and returned
             ! as requested. The residual ||F-Q*R||_F / ||F||_F
             ! is compared to M*N*EPS.
             F1(1:M,1:N+1) = F0(1:M,1:N+1)
             CALL CGEMM( 'N', 'N', M, N+1, MIN(M,N+1), -CONE, F, &
                         LDF, Y, LDY, CONE, F1, LDF )
             TMP_FQR = CLANGE( 'F', M, N+1, F1, LDF, WORK ) / &
                   CLANGE( 'F', M, N+1, F0,  LDF, WORK )
             IF ( TMP_FQR <= TOL2 ) THEN
                !WRITE(*,*) ':) CGEDMDQ ........ PASSED.'
             ELSE
                WRITE(*,*) ':( CGEDMDQ ........ FAILED.'
                NFAIL_F_QR = NFAIL_F_QR + 1
             END IF
          END IF
          !..... ZGEDMDQ checkpoint
                 !..... ZGEDMDQ checkpoint
          IF ( LSAME(RESIDS, 'R') ) THEN
              ! Compare the residuals returned by ZGEDMDQ with the
              ! explicitly computed residuals using the matrix A.
              ! Compute explicitly Y1 = A*Z
              CALL CGEMM( 'N', 'N', M, KQ, M, CONE, A, LDA, Z, LDZ, CZERO, Y1, LDY )
              ! ... and then A*Z(:,i) - LAMBDA(i)*Z(:,i), using the real forms
              ! of the invariant subspaces that correspond to complex conjugate
              ! pairs of eigencalues. (See the description of Z in ZGEDMDQ)
              DO i = 1, KQ
                    ! have a real eigenvalue with real eigenvector
                    CALL CAXPY( M, -CEIGS(i), Z(1,i), 1, Y1(1,i), 1 )
                    ! Y(1:M,i) = Y(1:M,i) - REIG(i)*Z(1:M,i)
                    RES1(i) = SCNRM2( M, Y1(1,i), 1)
              END DO
              TMP = ZERO
              DO i = 1, KQ
              TMP = MAX( TMP, ABS(RES(i) - RES1(i)) * &
                  SINGVQX(KQ)/(ANORM*SINGVQX(1)) )
              END DO
              TMP_REZQ = MAX( TMP_REZQ, TMP )
              IF ( TMP <= TOL2 ) THEN
                  !WRITE(*,*) '.... OK ........ CGEDMDQ PASSED.'
              ELSE
                  NFAIL_REZQ = NFAIL_REZQ + 1
                  WRITE(*,*) '................ CGEDMDQ FAILED!', &
                      'Check the code for implementation errors.'
              END IF
          END IF

          DEALLOCATE(CWORK)
          DEALLOCATE(WORK)
          DEALLOCATE(IWORK)

      END IF

      END DO   ! LWMINOPT
      !write(*,*) 'LWMINOPT loop completed'
      END DO   ! iWHTSVD
      !write(*,*) 'WHTSVD loop completed'
      END DO   ! iNRNK  -2:-1
      !write(*,*) 'NRNK loop completed'
      END DO   ! iSCALE  1:4
      !write(*,*) 'SCALE loop completed'
      END DO
      !write(*,*) 'JOBREF loop completed'
      END DO   ! iJOBZ
      !write(*,*) 'JOBZ loop completed'

      END DO ! MODE -6:6
      !write(*,*) 'MODE loop completed'
      END DO ! 1 or 2 trajectories
      !write(*,*) 'trajectories  loop completed'

      DEALLOCATE( A )
      DEALLOCATE( AC )
      DEALLOCATE( Z )
      DEALLOCATE( F )
      DEALLOCATE( F0 )
      DEALLOCATE( F1 )
      DEALLOCATE( X )
      DEALLOCATE( X0 )
      DEALLOCATE( Y )
      DEALLOCATE( Y0 )
      DEALLOCATE( Y1 )
      DEALLOCATE( AU )
      DEALLOCATE( W )
      DEALLOCATE( S )
      DEALLOCATE( Z1 )
      DEALLOCATE( RES )
      DEALLOCATE( RES1 )
      DEALLOCATE( RESEX )
      DEALLOCATE( CEIGS )
      DEALLOCATE( SINGVX )
      DEALLOCATE( SINGVQX )

      END DO ! LLOOP

      WRITE(*,*)
      WRITE(*,*) '>>>>>>>>>>>>>>>>>>>>>>>>>>'
      WRITE(*,*) ' Test summary for CGEDMD :'
      WRITE(*,*) '>>>>>>>>>>>>>>>>>>>>>>>>>>'
      WRITE(*,*)
      IF ( NFAIL_Z_XV == 0 ) THEN
          WRITE(*,*) '>>>> Z - U*V test PASSED.'
      ELSE
          WRITE(*,*) 'Z - U*V test FAILED ', NFAIL_Z_XV, ' time(s)'
          WRITE(*,*) 'Max error ||Z-U*V||_F was ', TMP_XW
          NFAIL_TOTAL = NFAIL_TOTAL + NFAIL_z_XV
      END IF

      IF ( NFAIL_AU == 0 ) THEN
          WRITE(*,*) '>>>> A*U test PASSED. '
      ELSE
          WRITE(*,*) 'A*U test FAILED ', NFAIL_AU, ' time(s)'
          WRITE(*,*) 'Max A*U test adjusted error measure was ', TMP_AU
          WRITE(*,*) 'It should be up to O(M*N) times EPS, EPS = ', EPS
          NFAIL_TOTAL = NFAIL_TOTAL + NFAIL_AU
      END IF


      IF ( NFAIL_REZ == 0 ) THEN
         WRITE(*,*) '>>>> Rezidual computation test PASSED.'
      ELSE
        WRITE(*,*) 'Rezidual computation test FAILED ', NFAIL_REZ, 'time(s)'
        WRITE(*,*) 'Max residual computing test adjusted error measure was ', TMP_REZ
        WRITE(*,*) 'It should be up to O(M*N) times EPS, EPS = ', EPS
        NFAIL_TOTAL = NFAIL_TOTAL + NFAIL_REZ
      END IF
      IF ( NFAIL_TOTAL == 0 ) THEN
        WRITE(*,*) '>>>> CGEDMD :: ALL TESTS PASSED.'
      ELSE
        WRITE(*,*) NFAIL_TOTAL, 'FAILURES!'
        WRITE(*,*) '>>>>>>>>>>>>>> CGEDMD :: TESTS FAILED. CHECK THE IMPLEMENTATION.'
      END IF

      WRITE(*,*)
      WRITE(*,*) '>>>>>>>>>>>>>>>>>>>>>>>>>>'
      WRITE(*,*) ' Test summary for CGEDMDQ :'
      WRITE(*,*) '>>>>>>>>>>>>>>>>>>>>>>>>>>'
      WRITE(*,*)

      IF ( NFAIL_SVDIFF == 0 ) THEN
        WRITE(*,*) '>>>> CGEDMD and CGEDMDQ computed singular &
           &values test PASSED.'
      ELSE
        WRITE(*,*) 'ZGEDMD and ZGEDMDQ discrepancies in &
            &the singular values unacceptable ', &
            NFAIL_SVDIFF, ' times. Test FAILED.'
        WRITE(*,*) 'The maximal discrepancy in the singular values (relative to the norm) was ', SVDIFF
        WRITE(*,*) 'It should be up to O(M*N) times EPS, EPS = ', EPS
        NFAILQ_TOTAL = NFAILQ_TOTAL + NFAIL_SVDIFF
      END IF
      IF ( NFAIL_F_QR == 0 ) THEN
        WRITE(*,*) '>>>> F - Q*R test PASSED.'
      ELSE
        WRITE(*,*) 'F - Q*R test FAILED ', NFAIL_F_QR, ' time(s)'
        WRITE(*,*) 'The largest relative residual was ', TMP_FQR
        WRITE(*,*) 'It should be up to O(M*N) times EPS, EPS = ', EPS
        NFAILQ_TOTAL = NFAILQ_TOTAL + NFAIL_F_QR
      END IF

      IF ( NFAIL_REZQ == 0 ) THEN
        WRITE(*,*) '>>>> Rezidual computation test PASSED.'
      ELSE
        WRITE(*,*) 'Rezidual computation test FAILED ', NFAIL_REZQ, 'time(s)'
        WRITE(*,*) 'Max residual computing test adjusted error measure was ', TMP_REZQ
        WRITE(*,*) 'It should be up to O(M*N) times EPS, EPS = ', EPS
        NFAILQ_TOTAL = NFAILQ_TOTAL + NFAIL_REZQ
      END IF

      IF ( NFAILQ_TOTAL == 0 ) THEN
        WRITE(*,*) '>>>>>>> CGEDMDQ :: ALL TESTS PASSED.'
      ELSE
        WRITE(*,*) NFAILQ_TOTAL, 'FAILURES!'
        WRITE(*,*) '>>>>>>> CGEDMDQ :: TESTS FAILED. CHECK THE IMPLEMENTATION.'
      END IF

      WRITE(*,*)
      WRITE(*,*) 'Test completed.'
      STOP
      END
