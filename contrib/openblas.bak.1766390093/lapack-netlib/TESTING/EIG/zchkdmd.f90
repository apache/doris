!     This is a test program for checking the implementations of
!     the implementations of the following subroutines
!
!     ZGEDMD,  for computation of the
!              Dynamic Mode Decomposition (DMD)
!     ZGEDMDQ, for computation of a
!              QR factorization based compressed DMD
!
!     Developed and supported by:
!     ===========================
!     Developed and coded by Zlatko Drmac, Faculty of Science,
!     University of Zagreb;  drmac@math.hr
!     In cooperation with
!     AIMdyn Inc., Santa Barbara, CA.
!     ========================================================
!     How to run the code (compiler, link info)
!     ========================================================
!     Compile as FORTRAN 90 (or later) and link with BLAS and
!     LAPACK libraries.
!     NOTE: The code is developed and tested on top of the
!     Intel MKL library (versions 2022.0.3 and 2022.2.0),
!     using the Intel Fortran compiler.
!
!     For developers of the C++ implementation
!     ========================================================
!     See the LAPACK++ and Template Numerical Toolkit (TNT)
!
!     Note on a development of the GPU HP implementation
!     ========================================================
!     Work in progress. See CUDA, MAGMA, SLATE.
!     NOTE: The four SVD subroutines used in this code are
!     included as a part of R&D and for the completeness.
!     This was also an opportunity to test those SVD codes.
!     If the scaling option is used all four are essentially
!     equally good. For implementations on HP platforms,
!     one can use whichever SVD is available.
!............................................................

!............................................................
!............................................................
!
      PROGRAM DMD_TEST
      use iso_fortran_env, only: real64
      IMPLICIT NONE
      integer, parameter :: WP = real64

!............................................................
      REAL(KIND=WP), PARAMETER ::  ONE = 1.0_WP
      REAL(KIND=WP), PARAMETER :: ZERO = 0.0_WP

      COMPLEX(KIND=WP), PARAMETER ::  ZONE = ( 1.0_WP, 0.0_WP )
      COMPLEX(KIND=WP), PARAMETER :: ZZERO = ( 0.0_WP, 0.0_WP )
!............................................................
      REAL(KIND=WP), ALLOCATABLE, DIMENSION(:)   :: RES, &
                     RES1, RESEX, SINGVX, SINGVQX, WORK
      INTEGER      , ALLOCATABLE, DIMENSION(:)   :: IWORK
      REAL(KIND=WP) :: WDUMMY(2)
      INTEGER       :: IDUMMY(4), ISEED(4)
      REAL(KIND=WP) :: ANORM, COND, CONDL, CONDR, EPS,       &
                       TOL, TOL2, SVDIFF, TMP, TMP_AU,       &
                       TMP_FQR, TMP_REZ, TMP_REZQ,  TMP_ZXW, &
                       TMP_EX

!............................................................
      COMPLEX(KIND=WP) :: ZMAX
      INTEGER :: LZWORK
      COMPLEX(KIND=WP), ALLOCATABLE, DIMENSION(:,:) ::  ZA, ZAC,  &
                                 ZAU, ZF, ZF0, ZF1, ZS, ZW,       &
                                 ZX, ZX0, ZY, ZY0, ZY1, ZZ, ZZ1
      COMPLEX(KIND=WP), ALLOCATABLE, DIMENSION(:)   ::  ZDA, ZDR, &
                                       ZDL, ZEIGS, ZEIGSA, ZWORK
      COMPLEX(KIND=WP) ::  ZDUMMY(22), ZDUM2X2(2,2)
!............................................................
      INTEGER :: K, KQ, LDF, LDS, LDA, LDAU, LDW, LDX, LDY,  &
                 LDZ, LIWORK, LWORK, M, N, LLOOP, NRNK, NRNKsp
      INTEGER :: i, iJOBREF, iJOBZ, iSCALE, INFO, j,     &
                 NFAIL, NFAIL_AU, NFAIL_F_QR, NFAIL_REZ,     &
                 NFAIL_REZQ, NFAIL_SVDIFF, NFAIL_TOTAL, NFAILQ_TOTAL,  &
                 NFAIL_Z_XV,  MODE, MODEL, MODER, WHTSVD,     &
                 WHTSVDsp
      INTEGER :: iNRNK, iWHTSVD,  K_TRAJ, LWMINOPT
      CHARACTER :: GRADE, JOBREF, JOBZ, PIVTNG, RSIGN,   &
                       SCALE, RESIDS, WANTQ, WANTR
      LOGICAL :: TEST_QRDMD

!.....external subroutines (BLAS and LAPACK)
      EXTERNAL DAXPY,  DGEEV, DGEMM, DGEMV, DLACPY, DLASCL
      EXTERNAL ZGEEV,  ZGEMV, ZLASCL
      EXTERNAL ZLARNV, ZLATMR
      EXTERNAL ZAXPY,  ZGEMM
!.....external subroutines DMD package, part 1
!     subroutines under test
      EXTERNAL ZGEDMD, ZGEDMDQ
!.....external functions (BLAS and LAPACK)
      EXTERNAL         DLAMCH,  DZNRM2
      REAL(KIND=WP) :: DLAMCH,  DZNRM2
      REAL(KIND=WP) ::          ZLANGE
      EXTERNAL IZAMAX
      INTEGER  IZAMAX
      EXTERNAL LSAME
      LOGICAL  LSAME

      INTRINSIC ABS, INT, MIN, MAX, SIGN
!............................................................

      ! The test is always in pairs : ( ZGEDMD and ZGEDMDQ )
      ! because the test includes comparing the results (in pairs).
!.....................................................................................
      TEST_QRDMD = .TRUE. ! This code by default performs tests on ZGEDMDQ
                          ! Since the QR factorizations based algorithm is designed for
                          ! single trajectory data, only single trajectory tests will
                          ! be performed with xGEDMDQ.
      WANTQ = 'Q'
      WANTR = 'R'
!.................................................................................

      EPS = DLAMCH( 'P' )  ! machine precision DP

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
      WRITE(*,*) 'M = '
      READ(*,*) M
      WRITE(*,*) M
      ! ... and the number of snapshots.
      WRITE(*,*) 'N = '
      READ(*,*) N
      WRITE(*,*) N

      ! ... Test the dimensions
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

      TMP_ZXW  = ZERO
      TMP_AU   = ZERO
      TMP_REZ  = ZERO
      TMP_REZQ = ZERO
      SVDIFF   = ZERO
      TMP_EX   = ZERO

      ALLOCATE( ZA(LDA,M) )
      ALLOCATE( ZAC(LDA,M) )
      ALLOCATE( ZF(LDF,N+1) )
      ALLOCATE( ZF0(LDF,N+1) )
      ALLOCATE( ZF1(LDF,N+1) )
      ALLOCATE( ZX(LDX,N) )
      ALLOCATE( ZX0(LDX,N) )
      ALLOCATE( ZY(LDY,N+1) )
      ALLOCATE( ZY0(LDY,N+1) )
      ALLOCATE( ZY1(LDY,N+1) )
      ALLOCATE( ZAU(LDAU,N) )
      ALLOCATE( ZW(LDW,N) )
      ALLOCATE( ZS(LDS,N) )
      ALLOCATE( ZZ(LDZ,N) )
      ALLOCATE( ZZ1(LDZ,N) )
      ALLOCATE( RES(N) )
      ALLOCATE( RES1(N) )
      ALLOCATE( RESEX(N) )
      ALLOCATE( ZEIGS(N) )
      ALLOCATE( SINGVX(N) )
      ALLOCATE( SINGVQX(N) )

      TOL  = M*EPS
      ! This mimics O(M*N)*EPS bound for accumulated roundoff error.
      ! The factor 10 is somewhat arbitrary.
      TOL2 = 10*M*N*EPS

!.............

      DO K_TRAJ = 1, 2
      !  Number of intial conditions in the simulation/trajectories (1 or 2)

      COND = 1.0D4
      ZMAX = (1.0D1,1.0D1)
      RSIGN = 'F'
      GRADE = 'N'
      MODEL = 6
      CONDL = 1.0D1
      MODER = 6
      CONDR = 1.0D1
      PIVTNG = 'N'

      ! Loop over all parameter MODE values for ZLATMR (+1,..,+6)
      DO MODE = 1, 6

      ALLOCATE( IWORK(2*M) )
      ALLOCATE( ZDA(M) )
      ALLOCATE( ZDL(M) )
      ALLOCATE( ZDR(M) )

      CALL ZLATMR( M, M, 'N', ISEED, 'N', ZDA, MODE, COND, &
                   ZMAX, RSIGN, GRADE, ZDL, MODEL,  CONDL, &
                   ZDR, MODER, CONDR, PIVTNG, IWORK, M, M, &
                   ZERO, -ONE, 'N', ZA, LDA, IWORK(M+1), INFO )
      DEALLOCATE( ZDR )
      DEALLOCATE( ZDL )
      DEALLOCATE( ZDA )
      DEALLOCATE( IWORK )

      LZWORK = MAX(1,2*M)
      ALLOCATE( ZEIGSA(M) )
      ALLOCATE( ZWORK(LZWORK) )
      ALLOCATE( WORK(2*M) )
      ZAC(1:M,1:M) = ZA(1:M,1:M)
      CALL ZGEEV( 'N','N', M, ZAC, LDA, ZEIGSA, ZDUM2X2, 2, &
                  ZDUM2X2, 2, ZWORK, LZWORK, WORK, INFO ) ! LAPACK CALL
      DEALLOCATE(WORK)
      DEALLOCATE(ZWORK)

      TMP = ABS(ZEIGSA(IZAMAX(M, ZEIGSA, 1))) ! The spectral radius of ZA
      ! Scale the matrix ZA to have unit spectral radius.
      CALL ZLASCL( 'G',0, 0, TMP, ONE, M, M, &
                   ZA, LDA, INFO )
      CALL ZLASCL( 'G',0, 0, TMP, ONE, M, 1, &
                   ZEIGSA, M, INFO )
      ANORM = ZLANGE( 'F', M, M, ZA, LDA, WDUMMY )

      IF ( K_TRAJ == 2 ) THEN
          ! generate data as two trajectories
          ! with two inital conditions
          CALL ZLARNV(2, ISEED, M, ZF(1,1) )
          DO i = 1, N/2
             CALL ZGEMV( 'N', M, M, ZONE, ZA, LDA, ZF(1,i), 1,  &
                  ZZERO, ZF(1,i+1), 1 )
          END DO
          ZX0(1:M,1:N/2) = ZF(1:M,1:N/2)
          ZY0(1:M,1:N/2) = ZF(1:M,2:N/2+1)

          CALL ZLARNV(2, ISEED, M, ZF(1,1) )
          DO i = 1, N-N/2
             CALL ZGEMV( 'N', M, M, ZONE, ZA, LDA, ZF(1,i), 1,  &
                  ZZERO, ZF(1,i+1), 1 )
          END DO
          ZX0(1:M,N/2+1:N) = ZF(1:M,1:N-N/2)
          ZY0(1:M,N/2+1:N) = ZF(1:M,2:N-N/2+1)
      ELSE
          CALL ZLARNV(2, ISEED, M, ZF(1,1) )
          DO i = 1, N
             CALL ZGEMV( 'N', M, M, ZONE, ZA, M, ZF(1,i), 1,  &
                  ZZERO, ZF(1,i+1), 1 )
          END DO
          ZF0(1:M,1:N+1) = ZF(1:M,1:N+1)
          ZX0(1:M,1:N) = ZF0(1:M,1:N)
          ZY0(1:M,1:N) = ZF0(1:M,2:N+1)
      END IF

      DEALLOCATE( ZEIGSA )
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
         NRNKsp = iNRNK

      DO iWHTSVD = 1,  3
         ! Check all four options to compute the POD basis
         ! via the SVD.
         WHTSVD   = iWHTSVD
         WHTSVDsp = iWHTSVD

      DO LWMINOPT = 1, 2
         ! Workspace query for the minimal (1) and for the optimal
         ! (2) workspace lengths determined by workspace query.

      ! ZGEDMD is always tested and its results are also used for
      ! comparisons with ZGEDMDQ.

      ZX(1:M,1:N) = ZX0(1:M,1:N)
      ZY(1:M,1:N) = ZY0(1:M,1:N)

      CALL ZGEDMD( SCALE, JOBZ, RESIDS, JOBREF, WHTSVD,   &
                   M,  N, ZX, LDX, ZY, LDY, NRNK, TOL,    &
                   K, ZEIGS, ZZ, LDZ,  RES, ZAU, LDAU,    &
                   ZW,  LDW, ZS, LDS,  ZDUMMY, -1,        &
                   WDUMMY, -1, IDUMMY, -1, INFO )
      IF ( (INFO .EQ. 2) .OR. ( INFO .EQ. 3 ) &
                          .OR. ( INFO < 0 ) ) THEN
           WRITE(*,*) 'Call to ZGEDMD workspace query failed. &
                      &Check the calling sequence and the code.'
           WRITE(*,*) 'The error code is ', INFO
           WRITE(*,*) 'The input parameters were ',      &
           SCALE, JOBZ, RESIDS, JOBREF, WHTSVD,          &
           M, N, LDX, LDY, NRNK, TOL, LDZ, LDAU, LDW, LDS
           STOP
      END IF

      LZWORK = INT(ZDUMMY(LWMINOPT))
      LWORK  = INT(WDUMMY(1))
      LIWORK = IDUMMY(1)

      ALLOCATE(ZWORK(LZWORK))
      ALLOCATE( WORK(LWORK))
      ALLOCATE(IWORK(LIWORK))

      CALL ZGEDMD( SCALE, JOBZ, RESIDS, JOBREF, WHTSVD,  &
                   M,  N, ZX, LDX, ZY, LDY, NRNK, TOL,   &
                   K, ZEIGS, ZZ, LDZ,  RES, ZAU, LDAU,   &
                   ZW,  LDW,  ZS, LDS, ZWORK,  LZWORK,   &
                   WORK, LWORK, IWORK, LIWORK, INFO )

      IF ( INFO /= 0 ) THEN
           WRITE(*,*) 'Call to ZGEDMD failed. &
           &Check the calling sequence and the code.'
           WRITE(*,*) 'The error code is ', INFO
           WRITE(*,*) 'The input parameters were ',&
           SCALE, JOBZ, RESIDS, JOBREF, WHTSVD, &
           M, N, LDX, LDY, NRNK, TOL
           STOP
      END IF

      SINGVX(1:N) = WORK(1:N)

      !...... ZGEDMD check point
      IF ( LSAME(JOBZ,'V')  ) THEN
          ! Check that Z = X*W, on return from ZGEDMD
          ! This checks that the returned eigenvectors in Z are
          ! the product of the SVD'POD basis returned in X
          ! and the eigenvectors of the rayleigh quotient
          ! returned in W
          CALL ZGEMM( 'N', 'N', M, K, K, ZONE, ZX, LDX, ZW, LDW, &
                      ZZERO, ZZ1, LDZ )
          TMP = ZERO
          DO i = 1, K
             CALL ZAXPY( M, -ZONE, ZZ(1,i), 1, ZZ1(1,i), 1)
             TMP = MAX(TMP, DZNRM2( M, ZZ1(1,i), 1 ) )
          END DO
          TMP_ZXW = MAX(TMP_ZXW, TMP )
          IF ( TMP_ZXW <= 10*M*EPS ) THEN
              !WRITE(*,*) ' :) .... OK .........ZGEDMD PASSED.'
          ELSE
              NFAIL_Z_XV = NFAIL_Z_XV + 1
              WRITE(*,*) ':( .................ZGEDMD FAILED!', &
                  'Check the code for implementation errors.'
              WRITE(*,*) 'The input parameters were ',&
                 SCALE, JOBZ, RESIDS, JOBREF, WHTSVD, &
                 M, N, LDX, LDY, NRNK, TOL
          END IF
      END IF


      !...... ZGEDMD check point
      IF ( LSAME(JOBREF,'R') ) THEN
           ! The matrix A*U is returned for computing refined Ritz vectors.
           ! Check that A*U is computed correctly using the formula
           ! A*U = Y * V * inv(SIGMA). This depends on the
           ! accuracy in the computed singular values and vectors of X.
           ! See the paper for an error analysis.
           ! Note that the left singular vectors of the input matrix X
           ! are returned in the array X.
           CALL ZGEMM( 'N', 'N', M, K, M, ZONE, ZA, LDA, ZX, LDX, &
                      ZZERO, ZZ1, LDZ )
          TMP = ZERO
          DO i = 1, K
            CALL ZAXPY( M, -ZONE, ZAU(1,i), 1, ZZ1(1,i), 1)
            TMP = MAX( TMP, DZNRM2( M, ZZ1(1,i),1 ) * &
                     SINGVX(K)/(ANORM*SINGVX(1)) )
          END DO
          TMP_AU = MAX( TMP_AU, TMP )
          IF ( TMP <= TOL2 ) THEN
              !WRITE(*,*) ':) .... OK .........ZGEDMD PASSED.'
          ELSE
              NFAIL_AU = NFAIL_AU + 1
              WRITE(*,*) ':( .................ZGEDMD FAILED!', &
                  'Check the code for implementation errors.'
              WRITE(*,*) 'The input parameters were ',&
                 SCALE, JOBZ, RESIDS, JOBREF, WHTSVD, &
                 M, N, LDX, LDY, NRNK, TOL
          END IF
      ELSEIF ( LSAME(JOBREF,'E') ) THEN
       ! The unscaled vectors of the Exact DMD are computed.
       ! This option is included for the sake of completeness,
       ! for users who prefer the Exact DMD vectors. The
       ! returned vectors are in the real form, in the same way
       ! as the Ritz vectors. Here we just save the vectors
       ! and test them separately using a Matlab script.


       CALL ZGEMM( 'N', 'N', M, K, M, ZONE, ZA, LDA, ZAU, LDAU, ZZERO, ZY1, LDY )

               DO i=1, K
                  ! have a real eigenvalue with real eigenvector
                CALL ZAXPY( M, -ZEIGS(i), ZAU(1,i), 1, ZY1(1,i), 1 )
                RESEX(i) = DZNRM2( M, ZY1(1,i), 1) / DZNRM2(M,ZAU(1,i),1)
               END DO
      END IF
      !...... ZGEDMD check point

      IF ( LSAME(RESIDS, 'R') ) THEN
          ! Compare the residuals returned by ZGEDMD with the
          ! explicitly computed residuals using the matrix A.
          ! Compute explicitly Y1 = A*Z
          CALL ZGEMM( 'N', 'N', M, K, M, ZONE, ZA, LDA, ZZ, LDZ, ZZERO, ZY1, LDY )
          ! ... and then A*Z(:,i) - LAMBDA(i)*Z(:,i), using the real forms
          ! of the invariant subspaces that correspond to complex conjugate
          ! pairs of eigencalues. (See the description of Z in ZGEDMD,)

          DO i=1, K
                ! have a real eigenvalue with real eigenvector
                CALL ZAXPY( M, -ZEIGS(i), ZZ(1,i), 1, ZY1(1,i), 1 )
                RES1(i) = DZNRM2( M, ZY1(1,i), 1)
          END DO
          TMP = ZERO
          DO i = 1, K
          TMP = MAX( TMP, ABS(RES(i) - RES1(i)) * &
                    SINGVX(K)/(ANORM*SINGVX(1)) )
          END DO
          TMP_REZ = MAX( TMP_REZ, TMP )
          IF ( TMP <= TOL2 ) THEN
              !WRITE(*,*) ':) .... OK ..........ZGEDMD PASSED.'
          ELSE
              NFAIL_REZ = NFAIL_REZ + 1
              WRITE(*,*) ':( ..................ZGEDMD FAILED!', &
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

      DEALLOCATE(ZWORK)
      DEALLOCATE(WORK)
      DEALLOCATE(IWORK)

      IF ( TEST_QRDMD .AND. (K_TRAJ == 1) ) THEN

      ZF(1:M,1:N+1) = ZF0(1:M,1:N+1)

      CALL ZGEDMDQ( SCALE, JOBZ, RESIDS, WANTQ, WANTR, JOBREF, &
                    WHTSVD, M, N+1, ZF, LDF,  ZX, LDX,  ZY, LDY,  &
                    NRNK,  TOL, K, ZEIGS, ZZ, LDZ, RES,  ZAU,  &
                    LDAU, ZW, LDW, ZS, LDS, ZDUMMY, -1,   &
                    WDUMMY,  -1, IDUMMY, -1, INFO )

      LZWORK = INT(ZDUMMY(LWMINOPT))
      ALLOCATE( ZWORK(LZWORK) )
      LIWORK = IDUMMY(1)
      ALLOCATE(IWORK(LIWORK))
      LWORK = INT(WDUMMY(1))
      ALLOCATE(WORK(LWORK))

      CALL ZGEDMDQ( SCALE, JOBZ, RESIDS, WANTQ, WANTR, JOBREF, &
                    WHTSVD, M, N+1, ZF, LDF,  ZX, LDX,  ZY, LDY,  &
                    NRNK,  TOL, KQ, ZEIGS, ZZ, LDZ, RES,  ZAU,  &
                    LDAU, ZW, LDW, ZS, LDS, ZWORK, LZWORK,   &
                    WORK,  LWORK, IWORK, LIWORK, INFO )

      IF ( INFO /= 0 ) THEN
             WRITE(*,*) 'Call to ZGEDMDQ failed. &
             &Check the calling sequence and the code.'
             WRITE(*,*) 'The error code is ', INFO
             WRITE(*,*) 'The input parameters were ',&
             SCALE, JOBZ, RESIDS, WANTQ, WANTR, WHTSVD, &
             M, N, LDX, LDY, NRNK, TOL
             STOP
      END IF
      SINGVQX(1:N) = WORK(1:N)

      !..... ZGEDMDQ check point

          IF ( 1 == 0 ) THEN
              ! Comparison of ZGEDMD and ZGEDMDQ singular values disabled
          TMP = ZERO
          DO i = 1, MIN(K, KQ)
             TMP = MAX(TMP, ABS(SINGVX(i)-SINGVQX(i)) / &
                                   SINGVX(1) )
          END DO
          SVDIFF = MAX( SVDIFF, TMP )
          IF ( TMP > M*N*EPS ) THEN
             WRITE(*,*) 'FAILED! Something was wrong with the run.'
             NFAIL_SVDIFF = NFAIL_SVDIFF + 1
             DO j =1, 3
                 write(*,*) j, SINGVX(j), SINGVQX(j)
                 read(*,*)
             END DO

          END IF
          END IF

      !..... ZGEDMDQ check point
      IF ( LSAME(WANTQ,'Q') .AND. LSAME(WANTR,'R') ) THEN
         ! Check that the QR factors are computed and returned
         ! as requested. The residual ||F-Q*R||_F / ||F||_F
         ! is compared to M*N*EPS.
         ZF1(1:M,1:N+1) = ZF0(1:M,1:N+1)
         CALL ZGEMM( 'N', 'N', M, N+1, MIN(M,N+1), -ZONE, ZF, &
                     LDF, ZY, LDY, ZONE, ZF1, LDF )
         TMP_FQR = ZLANGE( 'F', M, N+1, ZF1, LDF, WORK ) / &
               ZLANGE( 'F', M, N+1, ZF0,  LDF, WORK )
         IF ( TMP_FQR > TOL2 ) THEN
              WRITE(*,*) 'FAILED! Something was wrong with the run.'
             NFAIL_F_QR = NFAIL_F_QR + 1
         ELSE
             !WRITE(*,*) '........ PASSED.'
         END IF
      END IF

      !..... ZGEDMDQ check point
      IF ( LSAME(RESIDS, 'R') ) THEN
          ! Compare the residuals returned by ZGEDMDQ with the
          ! explicitly computed residuals using the matrix A.
          ! Compute explicitly Y1 = A*Z
          CALL ZGEMM( 'N', 'N', M, KQ, M, ZONE, ZA, LDA, ZZ, LDZ, ZZERO, ZY1, LDY )
          ! ... and then A*Z(:,i) - LAMBDA(i)*Z(:,i), using the real forms
          ! of the invariant subspaces that correspond to complex conjugate
          ! pairs of eigencalues. (See the description of Z in ZGEDMDQ)

          DO i=1, KQ
                ! have a real eigenvalue with real eigenvector
                CALL ZAXPY( M, -ZEIGS(i), ZZ(1,i), 1, ZY1(1,i), 1 )
                ! Y(1:M,i) = Y(1:M,i) - REIG(i)*Z(1:M,i)
                RES1(i) = DZNRM2( M, ZY1(1,i), 1)
          END DO
          TMP = ZERO
          DO i = 1, KQ
          TMP = MAX( TMP, ABS(RES(i) - RES1(i)) * &
              SINGVQX(KQ)/(ANORM*SINGVQX(1)) )
          END DO
          TMP_REZQ = MAX( TMP_REZQ, TMP )
          IF ( TMP <= TOL2 ) THEN
              !WRITE(*,*) '.... OK ........ ZGEDMDQ PASSED.'
          ELSE
              NFAIL_REZQ = NFAIL_REZQ + 1
              WRITE(*,*) '................ ZGEDMDQ FAILED!', &
                  'Check the code for implementation errors.'
              STOP
          END IF

      END IF

      DEALLOCATE( ZWORK )
      DEALLOCATE( WORK  )
      DEALLOCATE( IWORK )

      END IF ! ZGEDMDQ

!.......................................................................................................

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

      DEALLOCATE( ZA )
      DEALLOCATE( ZAC )
      DEALLOCATE( ZZ )
      DEALLOCATE( ZF )
      DEALLOCATE( ZF0 )
      DEALLOCATE( ZF1 )
      DEALLOCATE( ZX )
      DEALLOCATE( ZX0 )
      DEALLOCATE( ZY )
      DEALLOCATE( ZY0 )
      DEALLOCATE( ZY1 )
      DEALLOCATE( ZAU )
      DEALLOCATE( ZW )
      DEALLOCATE( ZS )
      DEALLOCATE( ZZ1 )
      DEALLOCATE( RES )
      DEALLOCATE( RES1 )
      DEALLOCATE( RESEX )
      DEALLOCATE( ZEIGS )
      DEALLOCATE( SINGVX )
      DEALLOCATE( SINGVQX )

      END DO ! LLOOP

      WRITE(*,*) '>>>>>>>>>>>>>>>>>>>>>>>>>>'
      WRITE(*,*) ' Test summary for ZGEDMD :'
      WRITE(*,*) '>>>>>>>>>>>>>>>>>>>>>>>>>>'
      WRITE(*,*)
      IF ( NFAIL_Z_XV == 0 ) THEN
         WRITE(*,*) '>>>> Z - U*V test PASSED.'
      ELSE
         WRITE(*,*) 'Z - U*V test FAILED ', NFAIL_Z_XV, ' time(s)'
         WRITE(*,*) 'Max error ||Z-U*V||_F was ', TMP_ZXW
         NFAIL_TOTAL = NFAIL_TOTAL + NFAIL_Z_XV
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
        WRITE(*,*) '>>>> ZGEDMD :: ALL TESTS PASSED.'
      ELSE
        WRITE(*,*) NFAIL_TOTAL, 'FAILURES!'
        WRITE(*,*) '>>>>>>>>>>>>>> ZGEDMD :: TESTS FAILED. CHECK THE IMPLEMENTATION.'
      END IF

      IF ( TEST_QRDMD ) THEN
      WRITE(*,*)
      WRITE(*,*) '>>>>>>>>>>>>>>>>>>>>>>>>>>'
      WRITE(*,*) ' Test summary for ZGEDMDQ :'
      WRITE(*,*) '>>>>>>>>>>>>>>>>>>>>>>>>>>'
      WRITE(*,*)

      IF ( NFAIL_SVDIFF == 0 ) THEN
          WRITE(*,*) '>>>> ZGEDMD and ZGEDMDQ computed singular &
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
          WRITE(*,*) '>>>>>>> ZGEDMDQ :: ALL TESTS PASSED.'
      ELSE
         WRITE(*,*) NFAILQ_TOTAL, 'FAILURES!'
         WRITE(*,*) '>>>>>>> ZGEDMDQ :: TESTS FAILED. CHECK THE IMPLEMENTATION.'
      END IF

      END IF

      WRITE(*,*)
      WRITE(*,*) 'Test completed.'
      STOP
      END
