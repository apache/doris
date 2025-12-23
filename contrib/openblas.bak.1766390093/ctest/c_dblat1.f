      PROGRAM DCBLAT1
*     Test program for the DOUBLE PRECISION Level 1 CBLAS.
*     Based upon the original CBLAS test routine together with:
*     F06EAF Example Program Text
*     .. Parameters ..
      INTEGER          NOUT
      PARAMETER        (NOUT=6)
*     .. Scalars in Common ..
      INTEGER          ICASE, INCX, INCY, MODE, N
      LOGICAL          PASS
*     .. Local Scalars ..
      DOUBLE PRECISION SFAC
      INTEGER          IC
*     .. External Subroutines ..
      EXTERNAL         CHECK0, CHECK1, CHECK2, CHECK3, HEADER
*     .. Common blocks ..
      COMMON           /COMBLA/ICASE, N, INCX, INCY, MODE, PASS
*     .. Data statements ..
      DATA             SFAC/9.765625D-4/
*     .. Executable Statements ..
      WRITE (NOUT,99999)
      DO 20 IC = 1, 11
         ICASE = IC
         CALL HEADER
*
*        .. Initialize  PASS,  INCX,  INCY, and MODE for a new case. ..
*        .. the value 9999 for INCX, INCY or MODE will appear in the ..
*        .. detailed  output, if any, for cases  that do not involve ..
*        .. these parameters ..
*
         PASS = .TRUE.
         INCX = 9999
         INCY = 9999
         MODE = 9999
         IF (ICASE.EQ.3) THEN
            CALL CHECK0(SFAC)
         ELSE IF (ICASE.EQ.7 .OR. ICASE.EQ.8 .OR. ICASE.EQ.9 .OR.
     +            ICASE.EQ.10) THEN
            CALL CHECK1(SFAC)
         ELSE IF (ICASE.EQ.1 .OR. ICASE.EQ.2 .OR. ICASE.EQ.5 .OR.
     +            ICASE.EQ.6) THEN
            CALL CHECK2(SFAC)
         ELSE IF (ICASE.EQ.4 .OR. ICASE.EQ.11) THEN
            CALL CHECK3(SFAC)
         END IF
*        -- Print
         IF (PASS) THEN
            WRITE (NOUT,99998)
         ELSE
            ERROR STOP
        END IF
   20 CONTINUE
*
99999 FORMAT (' Real CBLAS Test Program Results',/1X)
99998 FORMAT ('                                    ----- PASS -----')
      END
      SUBROUTINE HEADER
*     .. Parameters ..
      INTEGER          NOUT
      PARAMETER        (NOUT=6)
*     .. Scalars in Common ..
      INTEGER          ICASE, INCX, INCY, MODE, N
      LOGICAL          PASS
*     .. Local Arrays ..
      CHARACTER*15      L(11)
*     .. Common blocks ..
      COMMON           /COMBLA/ICASE, N, INCX, INCY, MODE, PASS
*     .. Data statements ..
      DATA             L(1)/'CBLAS_DDOT'/
      DATA             L(2)/'CBLAS_DAXPY '/
      DATA             L(3)/'CBLAS_DROTG '/
      DATA             L(4)/'CBLAS_DROT '/
      DATA             L(5)/'CBLAS_DCOPY '/
      DATA             L(6)/'CBLAS_DSWAP '/
      DATA             L(7)/'CBLAS_DNRM2 '/
      DATA             L(8)/'CBLAS_DASUM '/
      DATA             L(9)/'CBLAS_DSCAL '/
      DATA             L(10)/'CBLAS_IDAMAX'/
      DATA             L(11)/'CBLAS_DROTM'/
*     .. Executable Statements ..
      WRITE (NOUT,99999) ICASE, L(ICASE)
      RETURN
*
99999 FORMAT (/' Test of subprogram number',I3,9X,A15)
      END
      SUBROUTINE CHECK0(SFAC)
*     .. Parameters ..
      INTEGER           NOUT
      PARAMETER         (NOUT=6)
*     .. Scalar Arguments ..
      DOUBLE PRECISION  SFAC
*     .. Scalars in Common ..
      INTEGER           ICASE, INCX, INCY, MODE, N
      LOGICAL           PASS
*     .. Local Scalars ..
      DOUBLE PRECISION  SA, SB, SC, SS
      INTEGER           K
*     .. Local Arrays ..
      DOUBLE PRECISION  DA1(8), DATRUE(8), DB1(8), DBTRUE(8), DC1(8),
     +                  DS1(8)
*     .. External Subroutines ..
      EXTERNAL          DROTGTEST, STEST1
*     .. Common blocks ..
      COMMON            /COMBLA/ICASE, N, INCX, INCY, MODE, PASS
*     .. Data statements ..
      DATA              DA1/0.3D0, 0.4D0, -0.3D0, -0.4D0, -0.3D0, 0.0D0,
     +                  0.0D0, 1.0D0/
      DATA              DB1/0.4D0, 0.3D0, 0.4D0, 0.3D0, -0.4D0, 0.0D0,
     +                  1.0D0, 0.0D0/
      DATA              DC1/0.6D0, 0.8D0, -0.6D0, 0.8D0, 0.6D0, 1.0D0,
     +                  0.0D0, 1.0D0/
      DATA              DS1/0.8D0, 0.6D0, 0.8D0, -0.6D0, 0.8D0, 0.0D0,
     +                  1.0D0, 0.0D0/
      DATA              DATRUE/0.5D0, 0.5D0, 0.5D0, -0.5D0, -0.5D0,
     +                  0.0D0, 1.0D0, 1.0D0/
      DATA              DBTRUE/0.0D0, 0.6D0, 0.0D0, -0.6D0, 0.0D0,
     +                  0.0D0, 1.0D0, 0.0D0/
*     .. Executable Statements ..
*
*     Compute true values which cannot be prestored
*     in decimal notation
*
      DBTRUE(1) = 1.0D0/0.6D0
      DBTRUE(3) = -1.0D0/0.6D0
      DBTRUE(5) = 1.0D0/0.6D0
*
      DO 20 K = 1, 8
*        .. Set N=K for identification in output if any ..
         N = K
         IF (ICASE.EQ.3) THEN
*           .. DROTGTEST ..
            IF (K.GT.8) GO TO 40
            SA = DA1(K)
            SB = DB1(K)
            CALL DROTGTEST(SA,SB,SC,SS)
            CALL STEST1(SA,DATRUE(K),DATRUE(K),SFAC)
            CALL STEST1(SB,DBTRUE(K),DBTRUE(K),SFAC)
            CALL STEST1(SC,DC1(K),DC1(K),SFAC)
            CALL STEST1(SS,DS1(K),DS1(K),SFAC)
         ELSE
            WRITE (NOUT,*) ' Shouldn''t be here in CHECK0'
            ERROR STOP
         END IF
   20 CONTINUE
   40 RETURN
      END
      SUBROUTINE CHECK1(SFAC)
*     .. Parameters ..
      INTEGER           NOUT
      PARAMETER         (NOUT=6)
*     .. Scalar Arguments ..
      DOUBLE PRECISION  SFAC
*     .. Scalars in Common ..
      INTEGER           ICASE, INCX, INCY, MODE, N
      LOGICAL           PASS
*     .. Local Scalars ..
      INTEGER           I, LEN, NP1
*     .. Local Arrays ..
      DOUBLE PRECISION  DTRUE1(5), DTRUE3(5), DTRUE5(8,5,2), DV(8,5,2),
     +                  SA(10), STEMP(1), STRUE(8), SX(8)
      INTEGER           ITRUE2(5)
*     .. External Functions ..
      DOUBLE PRECISION  DASUMTEST, DNRM2TEST
      INTEGER           IDAMAXTEST
      EXTERNAL          DASUMTEST, DNRM2TEST, IDAMAXTEST
*     .. External Subroutines ..
      EXTERNAL          ITEST1, DSCALTEST, STEST, STEST1
*     .. Intrinsic Functions ..
      INTRINSIC         MAX
*     .. Common blocks ..
      COMMON            /COMBLA/ICASE, N, INCX, INCY, MODE, PASS
*     .. Data statements ..
      DATA              SA/0.3D0, -1.0D0, 0.0D0, 1.0D0, 0.3D0, 0.3D0,
     +                  0.3D0, 0.3D0, 0.3D0, 0.3D0/
      DATA              DV/0.1D0, 2.0D0, 2.0D0, 2.0D0, 2.0D0, 2.0D0,
     +                  2.0D0, 2.0D0, 0.3D0, 3.0D0, 3.0D0, 3.0D0, 3.0D0,
     +                  3.0D0, 3.0D0, 3.0D0, 0.3D0, -0.4D0, 4.0D0,
     +                  4.0D0, 4.0D0, 4.0D0, 4.0D0, 4.0D0, 0.2D0,
     +                  -0.6D0, 0.3D0, 5.0D0, 5.0D0, 5.0D0, 5.0D0,
     +                  5.0D0, 0.1D0, -0.3D0, 0.5D0, -0.1D0, 6.0D0,
     +                  6.0D0, 6.0D0, 6.0D0, 0.1D0, 8.0D0, 8.0D0, 8.0D0,
     +                  8.0D0, 8.0D0, 8.0D0, 8.0D0, 0.3D0, 9.0D0, 9.0D0,
     +                  9.0D0, 9.0D0, 9.0D0, 9.0D0, 9.0D0, 0.3D0, 2.0D0,
     +                  -0.4D0, 2.0D0, 2.0D0, 2.0D0, 2.0D0, 2.0D0,
     +                  0.2D0, 3.0D0, -0.6D0, 5.0D0, 0.3D0, 2.0D0,
     +                  2.0D0, 2.0D0, 0.1D0, 4.0D0, -0.3D0, 6.0D0,
     +                  -0.5D0, 7.0D0, -0.1D0, 3.0D0/
      DATA              DTRUE1/0.0D0, 0.3D0, 0.5D0, 0.7D0, 0.6D0/
      DATA              DTRUE3/0.0D0, 0.3D0, 0.7D0, 1.1D0, 1.0D0/
      DATA              DTRUE5/0.10D0, 2.0D0, 2.0D0, 2.0D0, 2.0D0,
     +                  2.0D0, 2.0D0, 2.0D0, -0.3D0, 3.0D0, 3.0D0,
     +                  3.0D0, 3.0D0, 3.0D0, 3.0D0, 3.0D0, 0.0D0, 0.0D0,
     +                  4.0D0, 4.0D0, 4.0D0, 4.0D0, 4.0D0, 4.0D0,
     +                  0.20D0, -0.60D0, 0.30D0, 5.0D0, 5.0D0, 5.0D0,
     +                  5.0D0, 5.0D0, 0.03D0, -0.09D0, 0.15D0, -0.03D0,
     +                  6.0D0, 6.0D0, 6.0D0, 6.0D0, 0.10D0, 8.0D0,
     +                  8.0D0, 8.0D0, 8.0D0, 8.0D0, 8.0D0, 8.0D0,
     +                  0.09D0, 9.0D0, 9.0D0, 9.0D0, 9.0D0, 9.0D0,
     +                  9.0D0, 9.0D0, 0.09D0, 2.0D0, -0.12D0, 2.0D0,
     +                  2.0D0, 2.0D0, 2.0D0, 2.0D0, 0.06D0, 3.0D0,
     +                  -0.18D0, 5.0D0, 0.09D0, 2.0D0, 2.0D0, 2.0D0,
     +                  0.03D0, 4.0D0, -0.09D0, 6.0D0, -0.15D0, 7.0D0,
     +                  -0.03D0, 3.0D0/
      DATA              ITRUE2/0, 1, 2, 2, 3/
*     .. Executable Statements ..
      DO 80 INCX = 1, 2
         DO 60 NP1 = 1, 5
            N = NP1 - 1
            LEN = 2*MAX(N,1)
*           .. Set vector arguments ..
            DO 20 I = 1, LEN
               SX(I) = DV(I,NP1,INCX)
   20       CONTINUE
*
            IF (ICASE.EQ.7) THEN
*              .. DNRM2TEST ..
               STEMP(1) = DTRUE1(NP1)
               CALL STEST1(DNRM2TEST(N,SX,INCX),STEMP(1),STEMP,SFAC)
            ELSE IF (ICASE.EQ.8) THEN
*              .. DASUMTEST ..
               STEMP(1) = DTRUE3(NP1)
               CALL STEST1(DASUMTEST(N,SX,INCX),STEMP(1),STEMP,SFAC)
            ELSE IF (ICASE.EQ.9) THEN
*              .. DSCALTEST ..
               CALL DSCALTEST(N,SA((INCX-1)*5+NP1),SX,INCX)
               DO 40 I = 1, LEN
                  STRUE(I) = DTRUE5(I,NP1,INCX)
   40          CONTINUE
               CALL STEST(LEN,SX,STRUE,STRUE,SFAC)
            ELSE IF (ICASE.EQ.10) THEN
*              .. IDAMAXTEST ..
               CALL ITEST1(IDAMAXTEST(N,SX,INCX),ITRUE2(NP1))
            ELSE
               WRITE (NOUT,*) ' Shouldn''t be here in CHECK1'
               ERROR STOP
            END IF
   60    CONTINUE
   80 CONTINUE
      RETURN
      END
      SUBROUTINE CHECK2(SFAC)
*     .. Parameters ..
      INTEGER           NOUT
      PARAMETER         (NOUT=6)
*     .. Scalar Arguments ..
      DOUBLE PRECISION  SFAC
*     .. Scalars in Common ..
      INTEGER           ICASE, INCX, INCY, MODE, N
      LOGICAL           PASS
*     .. Local Scalars ..
      DOUBLE PRECISION  SA
      INTEGER           I, J, KI, KN, KSIZE, LENX, LENY, MX, MY
*     .. Local Arrays ..
      DOUBLE PRECISION  DT10X(7,4,4), DT10Y(7,4,4), DT7(4,4),
     +                  DT8(7,4,4), DX1(7),
     +                  DY1(7), SSIZE1(4), SSIZE2(14,2), STX(7), STY(7),
     +                  SX(7), SY(7)
      INTEGER           INCXS(4), INCYS(4), LENS(4,2), NS(4)
*     .. External Functions ..
      EXTERNAL          DDOTTEST
      DOUBLE PRECISION  DDOTTEST
*     .. External Subroutines ..
      EXTERNAL          DAXPYTEST, DCOPYTEST, DSWAPTEST, STEST, STEST1
*     .. Intrinsic Functions ..
      INTRINSIC         ABS, MIN
*     .. Common blocks ..
      COMMON            /COMBLA/ICASE, N, INCX, INCY, MODE, PASS
*     .. Data statements ..
      DATA              SA/0.3D0/
      DATA              INCXS/1, 2, -2, -1/
      DATA              INCYS/1, -2, 1, -2/
      DATA              LENS/1, 1, 2, 4, 1, 1, 3, 7/
      DATA              NS/0, 1, 2, 4/
      DATA              DX1/0.6D0, 0.1D0, -0.5D0, 0.8D0, 0.9D0, -0.3D0,
     +                  -0.4D0/
      DATA              DY1/0.5D0, -0.9D0, 0.3D0, 0.7D0, -0.6D0, 0.2D0,
     +                  0.8D0/
      DATA              DT7/0.0D0, 0.30D0, 0.21D0, 0.62D0, 0.0D0,
     +                  0.30D0, -0.07D0, 0.85D0, 0.0D0, 0.30D0, -0.79D0,
     +                  -0.74D0, 0.0D0, 0.30D0, 0.33D0, 1.27D0/
      DATA              DT8/0.5D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0,
     +                  0.0D0, 0.68D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0,
     +                  0.0D0, 0.0D0, 0.68D0, -0.87D0, 0.0D0, 0.0D0,
     +                  0.0D0, 0.0D0, 0.0D0, 0.68D0, -0.87D0, 0.15D0,
     +                  0.94D0, 0.0D0, 0.0D0, 0.0D0, 0.5D0, 0.0D0,
     +                  0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.68D0,
     +                  0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0,
     +                  0.35D0, -0.9D0, 0.48D0, 0.0D0, 0.0D0, 0.0D0,
     +                  0.0D0, 0.38D0, -0.9D0, 0.57D0, 0.7D0, -0.75D0,
     +                  0.2D0, 0.98D0, 0.5D0, 0.0D0, 0.0D0, 0.0D0,
     +                  0.0D0, 0.0D0, 0.0D0, 0.68D0, 0.0D0, 0.0D0,
     +                  0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.35D0, -0.72D0,
     +                  0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.38D0,
     +                  -0.63D0, 0.15D0, 0.88D0, 0.0D0, 0.0D0, 0.0D0,
     +                  0.5D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0,
     +                  0.68D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0,
     +                  0.0D0, 0.68D0, -0.9D0, 0.33D0, 0.0D0, 0.0D0,
     +                  0.0D0, 0.0D0, 0.68D0, -0.9D0, 0.33D0, 0.7D0,
     +                  -0.75D0, 0.2D0, 1.04D0/
      DATA              DT10X/0.6D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0,
     +                  0.0D0, 0.5D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0,
     +                  0.0D0, 0.5D0, -0.9D0, 0.0D0, 0.0D0, 0.0D0,
     +                  0.0D0, 0.0D0, 0.5D0, -0.9D0, 0.3D0, 0.7D0,
     +                  0.0D0, 0.0D0, 0.0D0, 0.6D0, 0.0D0, 0.0D0, 0.0D0,
     +                  0.0D0, 0.0D0, 0.0D0, 0.5D0, 0.0D0, 0.0D0, 0.0D0,
     +                  0.0D0, 0.0D0, 0.0D0, 0.3D0, 0.1D0, 0.5D0, 0.0D0,
     +                  0.0D0, 0.0D0, 0.0D0, 0.8D0, 0.1D0, -0.6D0,
     +                  0.8D0, 0.3D0, -0.3D0, 0.5D0, 0.6D0, 0.0D0,
     +                  0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.5D0, 0.0D0,
     +                  0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0, -0.9D0,
     +                  0.1D0, 0.5D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.7D0,
     +                  0.1D0, 0.3D0, 0.8D0, -0.9D0, -0.3D0, 0.5D0,
     +                  0.6D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0,
     +                  0.5D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0,
     +                  0.5D0, 0.3D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0,
     +                  0.5D0, 0.3D0, -0.6D0, 0.8D0, 0.0D0, 0.0D0,
     +                  0.0D0/
      DATA              DT10Y/0.5D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0,
     +                  0.0D0, 0.6D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0,
     +                  0.0D0, 0.6D0, 0.1D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0,
     +                  0.0D0, 0.6D0, 0.1D0, -0.5D0, 0.8D0, 0.0D0,
     +                  0.0D0, 0.0D0, 0.5D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0,
     +                  0.0D0, 0.0D0, 0.6D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0,
     +                  0.0D0, 0.0D0, -0.5D0, -0.9D0, 0.6D0, 0.0D0,
     +                  0.0D0, 0.0D0, 0.0D0, -0.4D0, -0.9D0, 0.9D0,
     +                  0.7D0, -0.5D0, 0.2D0, 0.6D0, 0.5D0, 0.0D0,
     +                  0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.6D0, 0.0D0,
     +                  0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0, -0.5D0,
     +                  0.6D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0,
     +                  -0.4D0, 0.9D0, -0.5D0, 0.6D0, 0.0D0, 0.0D0,
     +                  0.0D0, 0.5D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0,
     +                  0.0D0, 0.6D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0,
     +                  0.0D0, 0.6D0, -0.9D0, 0.1D0, 0.0D0, 0.0D0,
     +                  0.0D0, 0.0D0, 0.6D0, -0.9D0, 0.1D0, 0.7D0,
     +                  -0.5D0, 0.2D0, 0.8D0/
      DATA              SSIZE1/0.0D0, 0.3D0, 1.6D0, 3.2D0/
      DATA              SSIZE2/0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0,
     +                  0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0,
     +                  0.0D0, 1.17D0, 1.17D0, 1.17D0, 1.17D0, 1.17D0,
     +                  1.17D0, 1.17D0, 1.17D0, 1.17D0, 1.17D0, 1.17D0,
     +                  1.17D0, 1.17D0, 1.17D0/
*     .. Executable Statements ..
*
      DO 120 KI = 1, 4
         INCX = INCXS(KI)
         INCY = INCYS(KI)
         MX = ABS(INCX)
         MY = ABS(INCY)
*
         DO 100 KN = 1, 4
            N = NS(KN)
            KSIZE = MIN(2,KN)
            LENX = LENS(KN,MX)
            LENY = LENS(KN,MY)
*           .. Initialize all argument arrays ..
            DO 20 I = 1, 7
               SX(I) = DX1(I)
               SY(I) = DY1(I)
   20       CONTINUE
*
            IF (ICASE.EQ.1) THEN
*              .. DDOTTEST ..
               CALL STEST1(DDOTTEST(N,SX,INCX,SY,INCY),DT7(KN,KI),
     +                     SSIZE1(KN),SFAC)
            ELSE IF (ICASE.EQ.2) THEN
*              .. DAXPYTEST ..
               CALL DAXPYTEST(N,SA,SX,INCX,SY,INCY)
               DO 40 J = 1, LENY
                  STY(J) = DT8(J,KN,KI)
   40          CONTINUE
               CALL STEST(LENY,SY,STY,SSIZE2(1,KSIZE),SFAC)
            ELSE IF (ICASE.EQ.5) THEN
*              .. DCOPYTEST ..
               DO 60 I = 1, 7
                  STY(I) = DT10Y(I,KN,KI)
   60          CONTINUE
               CALL DCOPYTEST(N,SX,INCX,SY,INCY)
               CALL STEST(LENY,SY,STY,SSIZE2(1,1),1.0D0)
            ELSE IF (ICASE.EQ.6) THEN
*              .. DSWAPTEST ..
               CALL DSWAPTEST(N,SX,INCX,SY,INCY)
               DO 80 I = 1, 7
                  STX(I) = DT10X(I,KN,KI)
                  STY(I) = DT10Y(I,KN,KI)
   80          CONTINUE
               CALL STEST(LENX,SX,STX,SSIZE2(1,1),1.0D0)
               CALL STEST(LENY,SY,STY,SSIZE2(1,1),1.0D0)
            ELSE
               WRITE (NOUT,*) ' Shouldn''t be here in CHECK2'
               ERROR STOP
            END IF
  100    CONTINUE
  120 CONTINUE
      RETURN
      END
      SUBROUTINE CHECK3(SFAC)
*     .. Parameters ..
      INTEGER           NOUT
      PARAMETER         (NOUT=6)
*     .. Scalar Arguments ..
      DOUBLE PRECISION  SFAC
*     .. Scalars in Common ..
      INTEGER           ICASE, INCX, INCY, MODE, N
      LOGICAL           PASS
*     .. Local Scalars ..
      DOUBLE PRECISION  SC, SS
      INTEGER           I, KI, KN, KSIZE, LEN
*     .. Local Arrays ..
      DOUBLE PRECISION  DX(10), DY(10), SSIZE2(10,2), STX(10), 
     +                  STY(10), SX(10), SY(10),
     +                  PARAM(5, 4), DPARAM(5) 
      INTEGER           INCXS(7), INCYS(7), NS(5)
*     .. External Subroutines ..
      EXTERNAL          STEST, DROTTEST, DROT
*     .. Intrinsic Functions ..
      INTRINSIC         MIN
*     .. Common blocks ..
      COMMON            /COMBLA/ICASE, N, INCX, INCY, MODE, PASS
*     .. Data statements ..
      DATA              INCXS/1, 1, 2, 2, -2, -1, -2/
      DATA              INCYS/1, 2, 2, -2, 1, -2, -2/
      DATA              NS/0, 1, 2, 4, 5/
      DATA              DX/0.6D0, 0.1D0, -0.5D0, 0.8D0, 0.9D0, -0.3D0,
     +                  -0.4D0, 0.7D0, 0.5D0, 0.2D0/
      DATA              DY/0.5D0, -0.9D0, 0.3D0, 0.7D0, -0.6D0, 0.2D0,
     +                  0.8D0, -0.5D0, 0.1D0, -0.3D0/
      DATA              SC, SS/0.8D0, 0.6D0/
      DATA              LEN/10/
      DATA              PARAM/-2.0D0, 1.0D0, 0.0D0, 0.0D0, 1.0D0,
     +                  -1.0D0, 0.2D0, 0.3D0, 0.4D0, 0.5D0,
     +                  0.0D0, 1.0D0, 0.3D0, 0.4D0, 1.0D0,
     +                  1.0D0, 0.2D0, -1.0D0, 1.0D0, 0.5D0/
      DATA              SSIZE2/0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0, 0.0D0,
     +                  0.0D0, 0.0D0, 0.0D0, 0.0D0, 1.17D0, 1.17D0, 
     +                  1.17D0, 1.17D0, 1.17D0, 1.17D0, 1.17D0, 1.17D0,
     +                  1.17D0, 1.17D0/
*     .. Executable Statements ..
*
      DO 60 KI = 1, 7
         INCX = INCXS(KI)
         INCY = INCYS(KI)
*
         DO 40 KN = 1, 5
            N = NS(KN)
            KSIZE = MIN(2,KN)
*
            IF (ICASE.EQ.4) THEN
*              .. DROTTEST ..
               DO 20 I = 1, 10
                  SX(I) = DX(I)
                  SY(I) = DY(I)
                  STX(I) = DX(I)
                  STY(I) = DY(I)
   20          CONTINUE
               CALL DROTTEST(N,SX,INCX,SY,INCY,SC,SS)
               CALL DROT(N,STX,INCX,STY,INCY,SC,SS)
               CALL STEST(LEN,SX,STX,SSIZE2(1,KSIZE),SFAC)
               CALL STEST(LEN,SY,STY,SSIZE2(1,KSIZE),SFAC)
            ELSE IF (ICASE.EQ.11) THEN
*              .. DROTMTEST ..
               DO 90 I = 1, 10
                  SX(I) = DX(I)
                  SY(I) = DY(I)
                  STX(I) = DX(I)
                  STY(I) = DY(I)
   90          CONTINUE
               DO 70 I = 1, 4
                  DO 80 K = 1, 5
                     DPARAM(K) = PARAM(K,I)
   80             CONTINUE
                  CALL DROTMTEST(N,SX,INCX,SY,INCY,DPARAM)
                  CALL DROTM(N,STX,INCX,STY,INCY,DPARAM)
                  CALL STEST(LEN,SX,STX,SSIZE2(1,KSIZE),SFAC)
                  CALL STEST(LEN,SY,STY,SSIZE2(1,KSIZE),SFAC)
   70          CONTINUE      
               ELSE
               WRITE (NOUT,*) ' Shouldn''t be here in CHECK3'
               ERROR STOP
            END IF
   40    CONTINUE
   60 CONTINUE
      RETURN
      END
      SUBROUTINE STEST(LEN,SCOMP,STRUE,SSIZE,SFAC)
*     ********************************* STEST **************************
*
*     THIS SUBR COMPARES ARRAYS  SCOMP() AND STRUE() OF LENGTH LEN TO
*     SEE IF THE TERM BY TERM DIFFERENCES, MULTIPLIED BY SFAC, ARE
*     NEGLIGIBLE.
*
*     C. L. LAWSON, JPL, 1974 DEC 10
*
*     .. Parameters ..
      INTEGER          NOUT
      PARAMETER        (NOUT=6)
*     .. Scalar Arguments ..
      DOUBLE PRECISION SFAC
      INTEGER          LEN
*     .. Array Arguments ..
      DOUBLE PRECISION SCOMP(LEN), SSIZE(LEN), STRUE(LEN)
*     .. Scalars in Common ..
      INTEGER          ICASE, INCX, INCY, MODE, N
      LOGICAL          PASS
*     .. Local Scalars ..
      DOUBLE PRECISION SD
      INTEGER          I
*     .. External Functions ..
      DOUBLE PRECISION SDIFF
      EXTERNAL         SDIFF
*     .. Intrinsic Functions ..
      INTRINSIC        ABS
*     .. Common blocks ..
      COMMON           /COMBLA/ICASE, N, INCX, INCY, MODE, PASS
*     .. Executable Statements ..
*
      DO 40 I = 1, LEN
         SD = SCOMP(I) - STRUE(I)
         IF (SDIFF(ABS(SSIZE(I))+ABS(SFAC*SD),ABS(SSIZE(I))).EQ.0.0D0)
     +       GO TO 40
*
*                             HERE    SCOMP(I) IS NOT CLOSE TO STRUE(I).
*
         IF ( .NOT. PASS) GO TO 20
*                             PRINT FAIL MESSAGE AND HEADER.
         PASS = .FALSE.
         WRITE (NOUT,99999)
         WRITE (NOUT,99998)
   20    WRITE (NOUT,99997) ICASE, N, INCX, INCY, MODE, I, SCOMP(I),
     +     STRUE(I), SD, SSIZE(I)
   40 CONTINUE
      RETURN
*
99999 FORMAT ('                                       FAIL')
99998 FORMAT (/' CASE  N INCX INCY MODE  I                            ',
     +       ' COMP(I)                             TRUE(I)  DIFFERENCE',
     +       '     SIZE(I)',/1X)
99997 FORMAT (1X,I4,I3,3I5,I3,2D36.8,2D12.4)
      END
      SUBROUTINE STEST1(SCOMP1,STRUE1,SSIZE,SFAC)
*     ************************* STEST1 *****************************
*
*     THIS IS AN INTERFACE SUBROUTINE TO ACCOMMODATE THE FORTRAN
*     REQUIREMENT THAT WHEN A DUMMY ARGUMENT IS AN ARRAY, THE
*     ACTUAL ARGUMENT MUST ALSO BE AN ARRAY OR AN ARRAY ELEMENT.
*
*     C.L. LAWSON, JPL, 1978 DEC 6
*
*     .. Scalar Arguments ..
      DOUBLE PRECISION  SCOMP1, SFAC, STRUE1
*     .. Array Arguments ..
      DOUBLE PRECISION  SSIZE(*)
*     .. Local Arrays ..
      DOUBLE PRECISION  SCOMP(1), STRUE(1)
*     .. External Subroutines ..
      EXTERNAL          STEST
*     .. Executable Statements ..
*
      SCOMP(1) = SCOMP1
      STRUE(1) = STRUE1
      CALL STEST(1,SCOMP,STRUE,SSIZE,SFAC)
*
      RETURN
      END
      DOUBLE PRECISION FUNCTION SDIFF(SA,SB)
*     ********************************* SDIFF **************************
*     COMPUTES DIFFERENCE OF TWO NUMBERS.  C. L. LAWSON, JPL 1974 FEB 15
*
*     .. Scalar Arguments ..
      DOUBLE PRECISION                SA, SB
*     .. Executable Statements ..
      SDIFF = SA - SB
      RETURN
      END
      SUBROUTINE ITEST1(ICOMP,ITRUE)
*     ********************************* ITEST1 *************************
*
*     THIS SUBROUTINE COMPARES THE VARIABLES ICOMP AND ITRUE FOR
*     EQUALITY.
*     C. L. LAWSON, JPL, 1974 DEC 10
*
*     .. Parameters ..
      INTEGER           NOUT
      PARAMETER         (NOUT=6)
*     .. Scalar Arguments ..
      INTEGER           ICOMP, ITRUE
*     .. Scalars in Common ..
      INTEGER           ICASE, INCX, INCY, MODE, N
      LOGICAL           PASS
*     .. Local Scalars ..
      INTEGER           ID
*     .. Common blocks ..
      COMMON            /COMBLA/ICASE, N, INCX, INCY, MODE, PASS
*     .. Executable Statements ..
*
      IF (ICOMP.EQ.ITRUE) GO TO 40
*
*                            HERE ICOMP IS NOT EQUAL TO ITRUE.
*
      IF ( .NOT. PASS) GO TO 20
*                             PRINT FAIL MESSAGE AND HEADER.
      PASS = .FALSE.
      WRITE (NOUT,99999)
      WRITE (NOUT,99998)
   20 ID = ICOMP - ITRUE
      WRITE (NOUT,99997) ICASE, N, INCX, INCY, MODE, ICOMP, ITRUE, ID
   40 CONTINUE
      RETURN
*
99999 FORMAT ('                                       FAIL')
99998 FORMAT (/' CASE  N INCX INCY MODE                               ',
     +       ' COMP                                TRUE     DIFFERENCE',
     +       /1X)
99997 FORMAT (1X,I4,I3,3I5,2I36,I12)
      END
      SUBROUTINE DROT(N,DX,INCX,DY,INCY,C,S)
*     .. Scalar Arguments ..
      DOUBLE PRECISION C,S
      INTEGER INCX,INCY,N
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION DX(*),DY(*)
*     ..
*     applies a plane rotation.
*     jack dongarra, linpack, 3/11/78.
*     modified 12/3/93, array(1) declarations changed to array(*)
*
*     .. Local Scalars ..
      DOUBLE PRECISION DTEMP
      INTEGER I,IX,IY
*     ..
      IF (N.LE.0) RETURN
      IF (INCX.EQ.1 .AND. INCY.EQ.1) GO TO 20
      IX = 1
      IY = 1
      IF (INCX.LT.0) IX = (-N+1)*INCX + 1
      IF (INCY.LT.0) IY = (-N+1)*INCY + 1
      DO 10 I = 1,N
          DTEMP = C*DX(IX) + S*DY(IY)
          DY(IY) = C*DY(IY) - S*DX(IX)
          DX(IX) = DTEMP
          IX = IX + INCX
          IY = IY + INCY
   10 CONTINUE
      RETURN
   20 DO 30 I = 1,N
          DTEMP = C*DX(I) + S*DY(I)
          DY(I) = C*DY(I) - S*DX(I)
          DX(I) = DTEMP
   30 CONTINUE
      RETURN
      END
      SUBROUTINE drotm(N,DX,INCX,DY,INCY,DPARAM)
*
*  -- Reference BLAS level1 routine (version 3.8.0) --
*  -- Reference BLAS is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     November 2017
*
*     .. Scalar Arguments ..
      INTEGER INCX,INCY,N
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION DPARAM(5),DX(*),DY(*)
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      DOUBLE PRECISION DFLAG,DH11,DH12,DH21,DH22,TWO,W,Z,ZERO
      INTEGER I,KX,KY,NSTEPS
*     ..
*     .. Data statements ..
      DATA zero,two/0.d0,2.d0/
*     ..
*
      dflag = dparam(1)
      IF (n.LE.0 .OR. (dflag+two.EQ.zero)) RETURN
      IF (incx.EQ.incy.AND.incx.GT.0) THEN
*
         nsteps = n*incx
         IF (dflag.LT.zero) THEN
            dh11 = dparam(2)
            dh12 = dparam(4)
            dh21 = dparam(3)
            dh22 = dparam(5)
            DO i = 1,nsteps,incx
               w = dx(i)
               z = dy(i)
               dx(i) = w*dh11 + z*dh12
               dy(i) = w*dh21 + z*dh22
            END DO
         ELSE IF (dflag.EQ.zero) THEN
            dh12 = dparam(4)
            dh21 = dparam(3)
            DO i = 1,nsteps,incx
               w = dx(i)
               z = dy(i)
               dx(i) = w + z*dh12
               dy(i) = w*dh21 + z
            END DO
         ELSE
            dh11 = dparam(2)
            dh22 = dparam(5)
            DO i = 1,nsteps,incx
               w = dx(i)
               z = dy(i)
               dx(i) = w*dh11 + z
               dy(i) = -w + dh22*z
            END DO
         END IF
      ELSE
         kx = 1
         ky = 1
         IF (incx.LT.0) kx = 1 + (1-n)*incx
         IF (incy.LT.0) ky = 1 + (1-n)*incy
*
         IF (dflag.LT.zero) THEN
            dh11 = dparam(2)
            dh12 = dparam(4)
            dh21 = dparam(3)
            dh22 = dparam(5)
            DO i = 1,n
               w = dx(kx)
               z = dy(ky)
               dx(kx) = w*dh11 + z*dh12
               dy(ky) = w*dh21 + z*dh22
               kx = kx + incx
               ky = ky + incy
            END DO
         ELSE IF (dflag.EQ.zero) THEN
            dh12 = dparam(4)
            dh21 = dparam(3)
            DO i = 1,n
               w = dx(kx)
               z = dy(ky)
               dx(kx) = w + z*dh12
               dy(ky) = w*dh21 + z
               kx = kx + incx
               ky = ky + incy
            END DO
         ELSE
             dh11 = dparam(2)
             dh22 = dparam(5)
             DO i = 1,n
                w = dx(kx)
                z = dy(ky)
                dx(kx) = w*dh11 + z
                dy(ky) = -w + dh22*z
                kx = kx + incx
                ky = ky + incy
            END DO
         END IF
      END IF
      RETURN
      END
