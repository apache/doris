      PROGRAM SCBLAT1
*     Test program for the REAL             Level 1 CBLAS.
*     Based upon the original CBLAS test routine together with:
*     F06EAF Example Program Text
*     .. Parameters ..
      INTEGER          NOUT
      PARAMETER        (NOUT=6)
*     .. Scalars in Common ..
      INTEGER          ICASE, INCX, INCY, MODE, N
      LOGICAL          PASS
*     .. Local Scalars ..
      REAL             SFAC
      INTEGER          IC
*     .. External Subroutines ..
      EXTERNAL         CHECK0, CHECK1, CHECK2, CHECK3, HEADER
*     .. Common blocks ..
      COMMON           /COMBLA/ICASE, N, INCX, INCY, MODE, PASS
*     .. Data statements ..
      DATA             SFAC/9.765625E-4/
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
      DATA             L(1)/'CBLAS_SDOT '/
      DATA             L(2)/'CBLAS_SAXPY '/
      DATA             L(3)/'CBLAS_SROTG '/
      DATA             L(4)/'CBLAS_SROT '/
      DATA             L(5)/'CBLAS_SCOPY '/
      DATA             L(6)/'CBLAS_SSWAP '/
      DATA             L(7)/'CBLAS_SNRM2 '/
      DATA             L(8)/'CBLAS_SASUM '/
      DATA             L(9)/'CBLAS_SSCAL '/
      DATA             L(10)/'CBLAS_ISAMAX'/
      DATA             L(11)/'CBLAS_SROTM'/
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
      REAL              SFAC
*     .. Scalars in Common ..
      INTEGER           ICASE, INCX, INCY, MODE, N
      LOGICAL           PASS
*     .. Local Scalars ..
      REAL              SA, SB, SC, SS
      INTEGER           K
*     .. Local Arrays ..
      REAL              DA1(8), DATRUE(8), DB1(8), DBTRUE(8), DC1(8),
     +                  DS1(8)
*     .. External Subroutines ..
      EXTERNAL          SROTGTEST, STEST1
*     .. Common blocks ..
      COMMON            /COMBLA/ICASE, N, INCX, INCY, MODE, PASS
*     .. Data statements ..
      DATA              DA1/0.3E0, 0.4E0, -0.3E0, -0.4E0, -0.3E0, 0.0E0,
     +                  0.0E0, 1.0E0/
      DATA              DB1/0.4E0, 0.3E0, 0.4E0, 0.3E0, -0.4E0, 0.0E0,
     +                  1.0E0, 0.0E0/
      DATA              DC1/0.6E0, 0.8E0, -0.6E0, 0.8E0, 0.6E0, 1.0E0,
     +                  0.0E0, 1.0E0/
      DATA              DS1/0.8E0, 0.6E0, 0.8E0, -0.6E0, 0.8E0, 0.0E0,
     +                  1.0E0, 0.0E0/
      DATA              DATRUE/0.5E0, 0.5E0, 0.5E0, -0.5E0, -0.5E0,
     +                  0.0E0, 1.0E0, 1.0E0/
      DATA              DBTRUE/0.0E0, 0.6E0, 0.0E0, -0.6E0, 0.0E0,
     +                  0.0E0, 1.0E0, 0.0E0/
*     .. Executable Statements ..
*
*     Compute true values which cannot be prestored
*     in decimal notation
*
      DBTRUE(1) = 1.0E0/0.6E0
      DBTRUE(3) = -1.0E0/0.6E0
      DBTRUE(5) = 1.0E0/0.6E0
*
      DO 20 K = 1, 8
*        .. Set N=K for identification in output if any ..
         N = K
         IF (ICASE.EQ.3) THEN
*           .. SROTGTEST ..
            IF (K.GT.8) GO TO 40
            SA = DA1(K)
            SB = DB1(K)
            CALL SROTGTEST(SA,SB,SC,SS)
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
      REAL              SFAC
*     .. Scalars in Common ..
      INTEGER           ICASE, INCX, INCY, MODE, N
      LOGICAL           PASS
*     .. Local Scalars ..
      INTEGER           I, LEN, NP1
*     .. Local Arrays ..
      REAL              DTRUE1(5), DTRUE3(5), DTRUE5(8,5,2), DV(8,5,2),
     +                  SA(10), STEMP(1), STRUE(8), SX(8)
      INTEGER           ITRUE2(5)
*     .. External Functions ..
      REAL              SASUMTEST, SNRM2TEST
      INTEGER           ISAMAXTEST
      EXTERNAL          SASUMTEST, SNRM2TEST, ISAMAXTEST
*     .. External Subroutines ..
      EXTERNAL          ITEST1, SSCALTEST, STEST, STEST1
*     .. Intrinsic Functions ..
      INTRINSIC         MAX
*     .. Common blocks ..
      COMMON            /COMBLA/ICASE, N, INCX, INCY, MODE, PASS
*     .. Data statements ..
      DATA              SA/0.3E0, -1.0E0, 0.0E0, 1.0E0, 0.3E0, 0.3E0,
     +                  0.3E0, 0.3E0, 0.3E0, 0.3E0/
      DATA              DV/0.1E0, 2.0E0, 2.0E0, 2.0E0, 2.0E0, 2.0E0,
     +                  2.0E0, 2.0E0, 0.3E0, 3.0E0, 3.0E0, 3.0E0, 3.0E0,
     +                  3.0E0, 3.0E0, 3.0E0, 0.3E0, -0.4E0, 4.0E0,
     +                  4.0E0, 4.0E0, 4.0E0, 4.0E0, 4.0E0, 0.2E0,
     +                  -0.6E0, 0.3E0, 5.0E0, 5.0E0, 5.0E0, 5.0E0,
     +                  5.0E0, 0.1E0, -0.3E0, 0.5E0, -0.1E0, 6.0E0,
     +                  6.0E0, 6.0E0, 6.0E0, 0.1E0, 8.0E0, 8.0E0, 8.0E0,
     +                  8.0E0, 8.0E0, 8.0E0, 8.0E0, 0.3E0, 9.0E0, 9.0E0,
     +                  9.0E0, 9.0E0, 9.0E0, 9.0E0, 9.0E0, 0.3E0, 2.0E0,
     +                  -0.4E0, 2.0E0, 2.0E0, 2.0E0, 2.0E0, 2.0E0,
     +                  0.2E0, 3.0E0, -0.6E0, 5.0E0, 0.3E0, 2.0E0,
     +                  2.0E0, 2.0E0, 0.1E0, 4.0E0, -0.3E0, 6.0E0,
     +                  -0.5E0, 7.0E0, -0.1E0, 3.0E0/
      DATA              DTRUE1/0.0E0, 0.3E0, 0.5E0, 0.7E0, 0.6E0/
      DATA              DTRUE3/0.0E0, 0.3E0, 0.7E0, 1.1E0, 1.0E0/
      DATA              DTRUE5/0.10E0, 2.0E0, 2.0E0, 2.0E0, 2.0E0,
     +                  2.0E0, 2.0E0, 2.0E0, -0.3E0, 3.0E0, 3.0E0,
     +                  3.0E0, 3.0E0, 3.0E0, 3.0E0, 3.0E0, 0.0E0, 0.0E0,
     +                  4.0E0, 4.0E0, 4.0E0, 4.0E0, 4.0E0, 4.0E0,
     +                  0.20E0, -0.60E0, 0.30E0, 5.0E0, 5.0E0, 5.0E0,
     +                  5.0E0, 5.0E0, 0.03E0, -0.09E0, 0.15E0, -0.03E0,
     +                  6.0E0, 6.0E0, 6.0E0, 6.0E0, 0.10E0, 8.0E0,
     +                  8.0E0, 8.0E0, 8.0E0, 8.0E0, 8.0E0, 8.0E0,
     +                  0.09E0, 9.0E0, 9.0E0, 9.0E0, 9.0E0, 9.0E0,
     +                  9.0E0, 9.0E0, 0.09E0, 2.0E0, -0.12E0, 2.0E0,
     +                  2.0E0, 2.0E0, 2.0E0, 2.0E0, 0.06E0, 3.0E0,
     +                  -0.18E0, 5.0E0, 0.09E0, 2.0E0, 2.0E0, 2.0E0,
     +                  0.03E0, 4.0E0, -0.09E0, 6.0E0, -0.15E0, 7.0E0,
     +                  -0.03E0, 3.0E0/
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
*              .. SNRM2TEST ..
               STEMP(1) = DTRUE1(NP1)
               CALL STEST1(SNRM2TEST(N,SX,INCX),STEMP(1),STEMP,SFAC)
            ELSE IF (ICASE.EQ.8) THEN
*              .. SASUMTEST ..
               STEMP(1) = DTRUE3(NP1)
               CALL STEST1(SASUMTEST(N,SX,INCX),STEMP(1),STEMP,SFAC)
            ELSE IF (ICASE.EQ.9) THEN
*              .. SSCALTEST ..
               CALL SSCALTEST(N,SA((INCX-1)*5+NP1),SX,INCX)
               DO 40 I = 1, LEN
                  STRUE(I) = DTRUE5(I,NP1,INCX)
   40          CONTINUE
               CALL STEST(LEN,SX,STRUE,STRUE,SFAC)
            ELSE IF (ICASE.EQ.10) THEN
*              .. ISAMAXTEST ..
               CALL ITEST1(ISAMAXTEST(N,SX,INCX),ITRUE2(NP1))
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
      REAL              SFAC
*     .. Scalars in Common ..
      INTEGER           ICASE, INCX, INCY, MODE, N
      LOGICAL           PASS
*     .. Local Scalars ..
      REAL              SA
      INTEGER           I, J, KI, KN, KSIZE, LENX, LENY, MX, MY
*     .. Local Arrays ..
      REAL              DT10X(7,4,4), DT10Y(7,4,4), DT7(4,4),
     +                  DT8(7,4,4), DX1(7),
     +                  DY1(7), SSIZE1(4), SSIZE2(14,2), STX(7), STY(7),
     +                  SX(7), SY(7)
      INTEGER           INCXS(4), INCYS(4), LENS(4,2), NS(4)
*     .. External Functions ..
      REAL              SDOTTEST
      EXTERNAL          SDOTTEST
*     .. External Subroutines ..
      EXTERNAL          SAXPYTEST, SCOPYTEST, SSWAPTEST, STEST, STEST1
*     .. Intrinsic Functions ..
      INTRINSIC         ABS, MIN
*     .. Common blocks ..
      COMMON            /COMBLA/ICASE, N, INCX, INCY, MODE, PASS
*     .. Data statements ..
      DATA              SA/0.3E0/
      DATA              INCXS/1, 2, -2, -1/
      DATA              INCYS/1, -2, 1, -2/
      DATA              LENS/1, 1, 2, 4, 1, 1, 3, 7/
      DATA              NS/0, 1, 2, 4/
      DATA              DX1/0.6E0, 0.1E0, -0.5E0, 0.8E0, 0.9E0, -0.3E0,
     +                  -0.4E0/
      DATA              DY1/0.5E0, -0.9E0, 0.3E0, 0.7E0, -0.6E0, 0.2E0,
     +                  0.8E0/
      DATA              DT7/0.0E0, 0.30E0, 0.21E0, 0.62E0, 0.0E0,
     +                  0.30E0, -0.07E0, 0.85E0, 0.0E0, 0.30E0, -0.79E0,
     +                  -0.74E0, 0.0E0, 0.30E0, 0.33E0, 1.27E0/
      DATA              DT8/0.5E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.68E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.68E0, -0.87E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.68E0, -0.87E0, 0.15E0,
     +                  0.94E0, 0.0E0, 0.0E0, 0.0E0, 0.5E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.68E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.35E0, -0.9E0, 0.48E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.38E0, -0.9E0, 0.57E0, 0.7E0, -0.75E0,
     +                  0.2E0, 0.98E0, 0.5E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.68E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.35E0, -0.72E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.38E0,
     +                  -0.63E0, 0.15E0, 0.88E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.5E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.68E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.68E0, -0.9E0, 0.33E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.68E0, -0.9E0, 0.33E0, 0.7E0,
     +                  -0.75E0, 0.2E0, 1.04E0/
      DATA              DT10X/0.6E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.5E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.5E0, -0.9E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.5E0, -0.9E0, 0.3E0, 0.7E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.6E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.5E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.3E0, 0.1E0, 0.5E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.8E0, 0.1E0, -0.6E0,
     +                  0.8E0, 0.3E0, -0.3E0, 0.5E0, 0.6E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.5E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, -0.9E0,
     +                  0.1E0, 0.5E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.7E0,
     +                  0.1E0, 0.3E0, 0.8E0, -0.9E0, -0.3E0, 0.5E0,
     +                  0.6E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.5E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.5E0, 0.3E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.5E0, 0.3E0, -0.6E0, 0.8E0, 0.0E0, 0.0E0,
     +                  0.0E0/
      DATA              DT10Y/0.5E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.6E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.6E0, 0.1E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.6E0, 0.1E0, -0.5E0, 0.8E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.5E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.6E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, -0.5E0, -0.9E0, 0.6E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, -0.4E0, -0.9E0, 0.9E0,
     +                  0.7E0, -0.5E0, 0.2E0, 0.6E0, 0.5E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.6E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, -0.5E0,
     +                  0.6E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  -0.4E0, 0.9E0, -0.5E0, 0.6E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.5E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.6E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.6E0, -0.9E0, 0.1E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.6E0, -0.9E0, 0.1E0, 0.7E0,
     +                  -0.5E0, 0.2E0, 0.8E0/
      DATA              SSIZE1/0.0E0, 0.3E0, 1.6E0, 3.2E0/
      DATA              SSIZE2/0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 1.17E0, 1.17E0, 1.17E0, 1.17E0, 1.17E0,
     +                  1.17E0, 1.17E0, 1.17E0, 1.17E0, 1.17E0, 1.17E0,
     +                  1.17E0, 1.17E0, 1.17E0/
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
*              .. SDOTTEST ..
               CALL STEST1(SDOTTEST(N,SX,INCX,SY,INCY),DT7(KN,KI),
     +                     SSIZE1(KN),SFAC)
            ELSE IF (ICASE.EQ.2) THEN
*              .. SAXPYTEST ..
               CALL SAXPYTEST(N,SA,SX,INCX,SY,INCY)
               DO 40 J = 1, LENY
                  STY(J) = DT8(J,KN,KI)
   40          CONTINUE
               CALL STEST(LENY,SY,STY,SSIZE2(1,KSIZE),SFAC)
            ELSE IF (ICASE.EQ.5) THEN
*              .. SCOPYTEST ..
               DO 60 I = 1, 7
                  STY(I) = DT10Y(I,KN,KI)
   60          CONTINUE
               CALL SCOPYTEST(N,SX,INCX,SY,INCY)
               CALL STEST(LENY,SY,STY,SSIZE2(1,1),1.0E0)
            ELSE IF (ICASE.EQ.6) THEN
*              .. SSWAPTEST ..
               CALL SSWAPTEST(N,SX,INCX,SY,INCY)
               DO 80 I = 1, 7
                  STX(I) = DT10X(I,KN,KI)
                  STY(I) = DT10Y(I,KN,KI)
   80          CONTINUE
               CALL STEST(LENX,SX,STX,SSIZE2(1,1),1.0E0)
               CALL STEST(LENY,SY,STY,SSIZE2(1,1),1.0E0)
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
      REAL              SFAC
*     .. Scalars in Common ..
      INTEGER           ICASE, INCX, INCY, N
      LOGICAL           PASS
*     .. Local Scalars ..
      REAL              SC, SS
      INTEGER           I, K, KI, KN, KSIZE, LEN
*     .. Local Arrays ..
      REAL              DX(19), DY(19),
     +                  SSIZE2(19,2), STX(19), STY(19), SX(19), SY(19),
     +                  PARAM(5, 4), SPARAM(5) 
      INTEGER           INCXS(7), INCYS(7), NS(7)
*     .. External Subroutines ..
      EXTERNAL          SROTMTEST, SROTM
*     .. Intrinsic Functions ..
      INTRINSIC         MIN
*     .. Common blocks ..
      COMMON            /COMBLA/ICASE, N, INCX, INCY, MODE, PASS
*     .. Data statements ..
      DATA              INCXS/1, 1, 2, 2, -2, -1, -2/
      DATA              INCYS/1, 2, 2, -2, 1, -2, -2/
      DATA              NS/0, 1, 2, 4, 5, 8, 9/
      DATA              DX/0.6E0, 0.1E0, -0.5E0, 0.8E0, 0.9E0, -0.3E0,
     +                  -0.4E0, 0.5E0, -0.9E0, 0.3E0, 0.7E0, -0.6E0,
     +                  0.2E0, 0.8E0, -0.46E0, 0.78E0, -0.46E0, -0.22E0,
     +                  1.06E0/
      DATA              DY/0.5E0, -0.9E0, 0.3E0, 0.7E0, -0.6E0, 0.2E0,
     +                  0.6E0, 0.1E0, -0.5E0, 0.8E0, 0.9E0, -0.3E0,
     +                  0.96E0, 0.1E0, -0.76E0, 0.8E0, 0.90E0, 0.66E0,
     +                  0.8E0/
      DATA              SC, SS/0.8E0, 0.6E0/
      DATA              PARAM/-2.0E0, 1.0E0, 0.0E0, 0.0E0, 1.0E0,
     +                  -1.0E0, 0.2E0, 0.3E0, 0.4E0, 0.5E0,
     +                  0.0E0, 1.0E0, 0.3E0, 0.4E0, 1.0E0,
     +                  1.0E0, 0.2E0, -1.0E0, 1.0E0, 0.5E0/
      DATA              LEN/19/
      DATA              SSIZE2/0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  1.17E0, 1.17E0, 1.17E0, 1.17E0, 1.17E0, 1.17E0,
     +                  1.17E0, 1.17E0, 1.17E0, 1.17E0, 1.17E0, 1.17E0,
     +                  1.17E0, 1.17E0, 1.17E0, 1.17E0, 1.17E0, 1.17E0,
     +                  1.17E0/
*     .. Executable Statements ..
*
      DO 60 KI = 1, 7
         INCX = INCXS(KI)
         INCY = INCYS(KI)
*
         DO 40 KN = 1, 7
            N = NS(KN)
            KSIZE = MIN(2,KN)
*
            IF (ICASE.EQ.4) THEN
*              .. SROTTEST ..
               DO 20 I = 1, 19
                  SX(I) = DX(I)
                  SY(I) = DY(I)
                  STX(I) = DX(I)
                  STY(I) = DY(I)
   20          CONTINUE
               CALL SROTTEST(N,SX,INCX,SY,INCY,SC,SS)
               CALL SROT(N,STX,INCX,STY,INCY,SC,SS)
               CALL STEST(LEN,SX,STX,SSIZE2(1,KSIZE),SFAC)
               CALL STEST(LEN,SY,STY,SSIZE2(1,KSIZE),SFAC)
            ELSE IF (ICASE.EQ.11) THEN
*              .. SROTMTEST ..
               DO 90 I = 1, 19
                  SX(I) = DX(I)
                  SY(I) = DY(I)
                  STX(I) = DX(I)
                  STY(I) = DY(I)
   90          CONTINUE
               DO 70 I = 1, 4
                  DO 80 K = 1, 5
                     SPARAM(K) = PARAM(K,I)
   80             CONTINUE
                  CALL SROTMTEST(N,SX,INCX,SY,INCY,SPARAM)
                  CALL SROTM(N,STX,INCX,STY,INCY,SPARAM)
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
      REAL             SFAC
      INTEGER          LEN
*     .. Array Arguments ..
      REAL             SCOMP(LEN), SSIZE(LEN), STRUE(LEN)
*     .. Scalars in Common ..
      INTEGER          ICASE, INCX, INCY, MODE, N
      LOGICAL          PASS
*     .. Local Scalars ..
      REAL             SD
      INTEGER          I
*     .. External Functions ..
      REAL             SDIFF
      EXTERNAL         SDIFF
*     .. Intrinsic Functions ..
      INTRINSIC        ABS
*     .. Common blocks ..
      COMMON           /COMBLA/ICASE, N, INCX, INCY, MODE, PASS
*     .. Executable Statements ..
*
      DO 40 I = 1, LEN
         SD = SCOMP(I) - STRUE(I)
         IF (SDIFF(ABS(SSIZE(I))+ABS(SFAC*SD),ABS(SSIZE(I))).EQ.0.0E0)
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
99997 FORMAT (1X,I4,I3,3I5,I3,2E36.8,2E12.4)
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
      REAL              SCOMP1, SFAC, STRUE1
*     .. Array Arguments ..
      REAL              SSIZE(*)
*     .. Local Arrays ..
      REAL              SCOMP(1), STRUE(1)
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
      REAL             FUNCTION SDIFF(SA,SB)
*     ********************************* SDIFF **************************
*     COMPUTES DIFFERENCE OF TWO NUMBERS.  C. L. LAWSON, JPL 1974 FEB 15
*
*     .. Scalar Arguments ..
      REAL                            SA, SB
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
      SUBROUTINE SROT(N,SX,INCX,SY,INCY,C,S)
*
*   --Reference BLAS level1 routine (version 3.8.0) --
*   --Reference BLAS is a software package provided by Univ. of Tennessee,    --
*   --Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     November 2017
* 
*     .. Scalar Arguments ..
      REAL C,S
      INTEGER INCX,INCY,N
*     ..
*     .. Array Arguments ..
      REAL SX(*),SY(*)
*     ..
*     .. Local Scalars ..
      REAL STEMP
      INTEGER I,IX,IY
*     ..
      IF (n.LE.0) RETURN
      IF (incx.EQ.1 .AND. incy.EQ.1) THEN
         DO i = 1,n
            stemp = c*sx(i) + s*sy(i)
            sy(i) = c*sy(i) - s*sx(i)
            sx(i) = stemp
         END DO
      ELSE
         ix = 1
         iy = 1
         IF (incx.LT.0) ix = (-n+1)*incx + 1
         IF (incy.LT.0) iy = (-n+1)*incy + 1
         DO i = 1,n
            stemp = c*sx(ix) + s*sy(iy)
            sy(iy) = c*sy(iy) - s*sx(ix)
            sx(ix) = stemp
            ix = ix + incx
            iy = iy + incy
         END DO
      END IF
      RETURN
      END
      SUBROUTINE srotm(N,SX,INCX,SY,INCY,SPARAM)
* 
*   --Reference BLAS level1 routine (version 3.8.0) --
*   --Reference BLAS is a software package provided by Univ. of Tennessee,    --
*   --Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     November 2017
* 
*     .. Scalar Arguments ..
      INTEGER INCX,INCY,N
*     ..
*     .. Array Arguments ..
      REAL SPARAM(5),SX(*),SY(*)
*     ..
* 
*   ====================================================================
* 
*     .. Local Scalars ..
      REAL SFLAG,SH11,SH12,SH21,SH22,TWO,W,Z,ZERO
      INTEGER I,KX,KY,NSTEPS
*     ..
*     .. Data statements ..
      DATA zero,two/0.e0,2.e0/
*     ..
* 
      sflag = sparam(1)
      IF (n.LE.0 .OR. (sflag+two.EQ.zero)) RETURN
      IF (incx.EQ.incy.AND.incx.GT.0) THEN
* 
         nsteps = n*incx
         IF (sflag.LT.zero) THEN
            sh11 = sparam(2)
            sh12 = sparam(4)
            sh21 = sparam(3)
            sh22 = sparam(5)
            DO i = 1,nsteps,incx
               w = sx(i)
               z = sy(i)
               sx(i) = w*sh11 + z*sh12
               sy(i) = w*sh21 + z*sh22
            END DO
         ELSE IF (sflag.EQ.zero) THEN
            sh12 = sparam(4)
            sh21 = sparam(3)
            DO i = 1,nsteps,incx
               w = sx(i)
               z = sy(i)
               sx(i) = w + z*sh12
               sy(i) = w*sh21 + z
            END DO
         ELSE
            sh11 = sparam(2)
            sh22 = sparam(5)
            DO i = 1,nsteps,incx
               w = sx(i)
               z = sy(i)
               sx(i) = w*sh11 + z
               sy(i) = -w + sh22*z
            END DO
         END IF
      ELSE
         kx = 1
         ky = 1
         IF (incx.LT.0) kx = 1 + (1-n)*incx
         IF (incy.LT.0) ky = 1 + (1-n)*incy
* 
         IF (sflag.LT.zero) THEN
            sh11 = sparam(2)
            sh12 = sparam(4)
            sh21 = sparam(3)
            sh22 = sparam(5)
            DO i = 1,n
               w = sx(kx)
               z = sy(ky)
               sx(kx) = w*sh11 + z*sh12
               sy(ky) = w*sh21 + z*sh22
               kx = kx + incx
               ky = ky + incy
            END DO
         ELSE IF (sflag.EQ.zero) THEN
            sh12 = sparam(4)
            sh21 = sparam(3)
            DO i = 1,n
               w = sx(kx)
               z = sy(ky)
               sx(kx) = w + z*sh12
               sy(ky) = w*sh21 + z
               kx = kx + incx
               ky = ky + incy
            END DO
         ELSE
             sh11 = sparam(2)
             sh22 = sparam(5)
             DO i = 1,n
                w = sx(kx)
                z = sy(ky)
                sx(kx) = w*sh11 + z
                sy(ky) = -w + sh22*z
                kx = kx + incx
                ky = ky + incy
            END DO
         END IF
      END IF
      RETURN
      END
