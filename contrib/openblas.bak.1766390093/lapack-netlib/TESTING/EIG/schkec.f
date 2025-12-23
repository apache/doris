*> \brief \b SCHKEC
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SCHKEC( THRESH, TSTERR, NIN, NOUT )
*
*       .. Scalar Arguments ..
*       LOGICAL            TSTERR
*       INTEGER            NIN, NOUT
*       REAL               THRESH
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SCHKEC tests eigen- condition estimation routines
*>        SLALN2, SLASY2, SLANV2, SLAQTR, SLAEXC,
*>        STRSYL, STREXC, STRSNA, STRSEN, STGEXC
*>
*> In all cases, the routine runs through a fixed set of numerical
*> examples, subjects them to various tests, and compares the test
*> results to a threshold THRESH. In addition, STREXC, STRSNA and STRSEN
*> are tested by reading in precomputed examples from a file (on input
*> unit NIN).  Output is written to output unit NOUT.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] THRESH
*> \verbatim
*>          THRESH is REAL
*>          Threshold for residual tests.  A computed test ratio passes
*>          the threshold if it is less than THRESH.
*> \endverbatim
*>
*> \param[in] TSTERR
*> \verbatim
*>          TSTERR is LOGICAL
*>          Flag that indicates whether error exits are to be tested.
*> \endverbatim
*>
*> \param[in] NIN
*> \verbatim
*>          NIN is INTEGER
*>          The logical unit number for input.
*> \endverbatim
*>
*> \param[in] NOUT
*> \verbatim
*>          NOUT is INTEGER
*>          The logical unit number for output.
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
*> \ingroup single_eig
*
*  =====================================================================
      SUBROUTINE SCHKEC( THRESH, TSTERR, NIN, NOUT )
*
*  -- LAPACK test routine --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*
*     .. Scalar Arguments ..
      LOGICAL            TSTERR
      INTEGER            NIN, NOUT
      REAL               THRESH
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      LOGICAL            OK
      CHARACTER*3        PATH
      INTEGER            KLAEXC, KLALN2, KLANV2, KLAQTR, KLASY2, KTREXC,
     $                   KTRSEN, KTRSNA, KTRSYL, KTRSYL3, LLAEXC,
     $                   LLALN2, LLANV2, LLAQTR, LLASY2, LTREXC, LTRSYL,
     $                   NLANV2, NLAQTR, NLASY2, NTESTS, NTRSYL, KTGEXC,
     $                   LTGEXC
      REAL               EPS, RLAEXC, RLALN2, RLANV2, RLAQTR, RLASY2,
     $                   RTREXC, SFMIN, RTGEXC
*     ..
*     .. Local Arrays ..
      INTEGER            FTRSYL( 3 ), ITRSYL( 2 ), LTRSEN( 3 ),
     $                   LTRSNA( 3 ), NLAEXC( 2 ), NLALN2( 2 ),
     $                   NTGEXC( 2 ), NTREXC( 3 ), NTRSEN( 3 ),
     $                   NTRSNA( 3 )
      REAL               RTRSEN( 3 ), RTRSNA( 3 ), RTRSYL( 2 )
*     ..
*     .. External Subroutines ..
      EXTERNAL           SERREC, SGET31, SGET32, SGET33, SGET34, SGET35,
     $                   SGET36, SGET37, SGET38, SGET39, SGET40, SSYL01
*     ..
*     .. External Functions ..
      REAL               SLAMCH
      EXTERNAL           SLAMCH
*     ..
*     .. Executable Statements ..
*
      PATH( 1: 1 ) = 'Single precision'
      PATH( 2: 3 ) = 'EC'
      EPS = SLAMCH( 'P' )
      SFMIN = SLAMCH( 'S' )
*
*     Print header information
*
      WRITE( NOUT, FMT = 9989 )
      WRITE( NOUT, FMT = 9988 )EPS, SFMIN
      WRITE( NOUT, FMT = 9987 )THRESH
*
*     Test error exits if TSTERR is .TRUE.
*
      IF( TSTERR )
     $   CALL SERREC( PATH, NOUT )
*
      OK = .TRUE.
      CALL SGET31( RLALN2, LLALN2, NLALN2, KLALN2 )
      IF( RLALN2.GT.THRESH .OR. NLALN2( 1 ).NE.0 ) THEN
         OK = .FALSE.
         WRITE( NOUT, FMT = 9999 )RLALN2, LLALN2, NLALN2, KLALN2
      END IF
*
      CALL SGET32( RLASY2, LLASY2, NLASY2, KLASY2 )
      IF( RLASY2.GT.THRESH ) THEN
         OK = .FALSE.
         WRITE( NOUT, FMT = 9998 )RLASY2, LLASY2, NLASY2, KLASY2
      END IF
*
      CALL SGET33( RLANV2, LLANV2, NLANV2, KLANV2 )
      IF( RLANV2.GT.THRESH .OR. NLANV2.NE.0 ) THEN
         OK = .FALSE.
         WRITE( NOUT, FMT = 9997 )RLANV2, LLANV2, NLANV2, KLANV2
      END IF
*
      CALL SGET34( RLAEXC, LLAEXC, NLAEXC, KLAEXC )
      IF( RLAEXC.GT.THRESH .OR. NLAEXC( 2 ).NE.0 ) THEN
         OK = .FALSE.
         WRITE( NOUT, FMT = 9996 )RLAEXC, LLAEXC, NLAEXC, KLAEXC
      END IF
*
      CALL SGET35( RTRSYL( 1 ), LTRSYL, NTRSYL, KTRSYL )
      IF( RTRSYL( 1 ).GT.THRESH ) THEN
         OK = .FALSE.
         WRITE( NOUT, FMT = 9995 )RTRSYL( 1 ), LTRSYL, NTRSYL, KTRSYL
      END IF
*
      CALL SSYL01( THRESH, FTRSYL, RTRSYL, ITRSYL, KTRSYL3 )
      IF( FTRSYL( 1 ).GT.0 ) THEN
         OK = .FALSE.
         WRITE( NOUT, FMT = 9970 )FTRSYL( 1 ), RTRSYL( 1 ), THRESH
      END IF
      IF( FTRSYL( 2 ).GT.0 ) THEN
         OK = .FALSE.
         WRITE( NOUT, FMT = 9971 )FTRSYL( 2 ), RTRSYL( 2 ), THRESH
      END IF
      IF( FTRSYL( 3 ).GT.0 ) THEN
         OK = .FALSE.
         WRITE( NOUT, FMT = 9972 )FTRSYL( 3 )
      END IF
*
      CALL SGET36( RTREXC, LTREXC, NTREXC, KTREXC, NIN )
      IF( RTREXC.GT.THRESH .OR. NTREXC( 3 ).GT.0 ) THEN
         OK = .FALSE.
         WRITE( NOUT, FMT = 9994 )RTREXC, LTREXC, NTREXC, KTREXC
      END IF
*
      CALL SGET37( RTRSNA, LTRSNA, NTRSNA, KTRSNA, NIN )
      IF( RTRSNA( 1 ).GT.THRESH .OR. RTRSNA( 2 ).GT.THRESH .OR.
     $    NTRSNA( 1 ).NE.0 .OR. NTRSNA( 2 ).NE.0 .OR. NTRSNA( 3 ).NE.0 )
     $     THEN
         OK = .FALSE.
         WRITE( NOUT, FMT = 9993 )RTRSNA, LTRSNA, NTRSNA, KTRSNA
      END IF
*
      CALL SGET38( RTRSEN, LTRSEN, NTRSEN, KTRSEN, NIN )
      IF( RTRSEN( 1 ).GT.THRESH .OR. RTRSEN( 2 ).GT.THRESH .OR.
     $    NTRSEN( 1 ).NE.0 .OR. NTRSEN( 2 ).NE.0 .OR. NTRSEN( 3 ).NE.0 )
     $     THEN
         OK = .FALSE.
         WRITE( NOUT, FMT = 9992 )RTRSEN, LTRSEN, NTRSEN, KTRSEN
      END IF
*
      CALL SGET39( RLAQTR, LLAQTR, NLAQTR, KLAQTR )
      IF( RLAQTR.GT.THRESH ) THEN
         OK = .FALSE.
         WRITE( NOUT, FMT = 9991 )RLAQTR, LLAQTR, NLAQTR, KLAQTR
      END IF
*
      CALL SGET40( RTGEXC, LTGEXC, NTGEXC, KTGEXC, NIN )
      IF( RTGEXC.GT.THRESH ) THEN
         OK = .FALSE.
         WRITE( NOUT, FMT = 9986 )RTGEXC, LTGEXC, NTGEXC, KTGEXC
      END IF
*
      NTESTS = KLALN2 + KLASY2 + KLANV2 + KLAEXC + KTRSYL + KTREXC +
     $         KTRSNA + KTRSEN + KLAQTR
      IF( OK )
     $   WRITE( NOUT, FMT = 9990 )PATH, NTESTS
*
      RETURN
 9999 FORMAT( ' Error in SLALN2: RMAX =', E12.3, / ' LMAX = ', I8, ' N',
     $      'INFO=', 2I8, ' KNT=', I8 )
 9998 FORMAT( ' Error in SLASY2: RMAX =', E12.3, / ' LMAX = ', I8, ' N',
     $      'INFO=', I8, ' KNT=', I8 )
 9997 FORMAT( ' Error in SLANV2: RMAX =', E12.3, / ' LMAX = ', I8, ' N',
     $      'INFO=', I8, ' KNT=', I8 )
 9996 FORMAT( ' Error in SLAEXC: RMAX =', E12.3, / ' LMAX = ', I8, ' N',
     $      'INFO=', 2I8, ' KNT=', I8 )
 9995 FORMAT( ' Error in STRSYL: RMAX =', E12.3, / ' LMAX = ', I8, ' N',
     $      'INFO=', I8, ' KNT=', I8 )
 9994 FORMAT( ' Error in STREXC: RMAX =', E12.3, / ' LMAX = ', I8, ' N',
     $      'INFO=', 3I8, ' KNT=', I8 )
 9993 FORMAT( ' Error in STRSNA: RMAX =', 3E12.3, / ' LMAX = ', 3I8,
     $      ' NINFO=', 3I8, ' KNT=', I8 )
 9992 FORMAT( ' Error in STRSEN: RMAX =', 3E12.3, / ' LMAX = ', 3I8,
     $      ' NINFO=', 3I8, ' KNT=', I8 )
 9991 FORMAT( ' Error in SLAQTR: RMAX =', E12.3, / ' LMAX = ', I8, ' N',
     $      'INFO=', I8, ' KNT=', I8 )
 9990 FORMAT( / 1X, 'All tests for ', A3, ' routines passed the thresh',
     $      'old ( ', I6, ' tests run)' )
 9989 FORMAT( ' Tests of the Nonsymmetric eigenproblem condition estim',
     $      'ation routines', / ' SLALN2, SLASY2, SLANV2, SLAEXC, STRS',
     $      'YL, STREXC, STRSNA, STRSEN, SLAQTR', / )
 9988 FORMAT( ' Relative machine precision (EPS) = ', E16.6, / ' Safe ',
     $      'minimum (SFMIN)             = ', E16.6, / )
 9987 FORMAT( ' Routines pass computational tests if test ratio is les',
     $      's than', F8.2, / / )
 9986 FORMAT( ' Error in STGEXC: RMAX =', E12.3, / ' LMAX = ', I8, ' N',
     $      'INFO=', 2I8, ' KNT=', I8 )
 9972 FORMAT( 'STRSYL and STRSYL3 compute an inconsistent result ',
     $      'factor in ', I8, ' tests.')
 9971 FORMAT( 'Error in STRSYL3: ', I8, ' tests fail the threshold.', /
     $      'Maximum test ratio =', D12.3, ' threshold =', D12.3 )
 9970 FORMAT( 'Error in STRSYL: ', I8, ' tests fail the threshold.', /
     $      'Maximum test ratio =', D12.3, ' threshold =', D12.3 )
*
*     End of SCHKEC
*
      END
