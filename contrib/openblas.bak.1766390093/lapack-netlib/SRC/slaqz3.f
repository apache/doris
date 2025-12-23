*> \brief \b SLAQZ3
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLAQZ3 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slaqz3.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slaqz3.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slaqz3.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*      SUBROUTINE SLAQZ3( ILSCHUR, ILQ, ILZ, N, ILO, IHI, NW, A, LDA, B,
*     $    LDB, Q, LDQ, Z, LDZ, NS, ND, ALPHAR, ALPHAI, BETA, QC, LDQC,
*     $    ZC, LDZC, WORK, LWORK, REC, INFO )
*      IMPLICIT NONE
*
*      Arguments
*      LOGICAL, INTENT( IN ) :: ILSCHUR, ILQ, ILZ
*      INTEGER, INTENT( IN ) :: N, ILO, IHI, NW, LDA, LDB, LDQ, LDZ,
*     $    LDQC, LDZC, LWORK, REC
*
*      REAL, INTENT( INOUT ) :: A( LDA, * ), B( LDB, * ), Q( LDQ, * ),
*     $    Z( LDZ, * ), ALPHAR( * ), ALPHAI( * ), BETA( * )
*      INTEGER, INTENT( OUT ) :: NS, ND, INFO
*      REAL :: QC( LDQC, * ), ZC( LDZC, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SLAQZ3 performs AED
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] ILSCHUR
*> \verbatim
*>          ILSCHUR is LOGICAL
*>              Determines whether or not to update the full Schur form
*> \endverbatim
*> \param[in] ILQ
*> \verbatim
*>          ILQ is LOGICAL
*>              Determines whether or not to update the matrix Q
*> \endverbatim
*>
*> \param[in] ILZ
*> \verbatim
*>          ILZ is LOGICAL
*>              Determines whether or not to update the matrix Z
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrices A, B, Q, and Z.  N >= 0.
*> \endverbatim
*>
*> \param[in] ILO
*> \verbatim
*>          ILO is INTEGER
*> \endverbatim
*>
*> \param[in] IHI
*> \verbatim
*>          IHI is INTEGER
*>          ILO and IHI mark the rows and columns of (A,B) which
*>          are to be normalized
*> \endverbatim
*>
*> \param[in] NW
*> \verbatim
*>          NW is INTEGER
*>          The desired size of the deflation window.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is REAL array, dimension (LDA, N)
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max( 1, N ).
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is REAL array, dimension (LDB, N)
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B.  LDB >= max( 1, N ).
*> \endverbatim
*>
*> \param[in,out] Q
*> \verbatim
*>          Q is REAL array, dimension (LDQ, N)
*> \endverbatim
*>
*> \param[in] LDQ
*> \verbatim
*>          LDQ is INTEGER
*> \endverbatim
*>
*> \param[in,out] Z
*> \verbatim
*>          Z is REAL array, dimension (LDZ, N)
*> \endverbatim
*>
*> \param[in] LDZ
*> \verbatim
*>          LDZ is INTEGER
*> \endverbatim
*>
*> \param[out] NS
*> \verbatim
*>          NS is INTEGER
*>          The number of unconverged eigenvalues available to
*>          use as shifts.
*> \endverbatim
*>
*> \param[out] ND
*> \verbatim
*>          ND is INTEGER
*>          The number of converged eigenvalues found.
*> \endverbatim
*>
*> \param[out] ALPHAR
*> \verbatim
*>          ALPHAR is REAL array, dimension (N)
*>          The real parts of each scalar alpha defining an eigenvalue
*>          of GNEP.
*> \endverbatim
*>
*> \param[out] ALPHAI
*> \verbatim
*>          ALPHAI is REAL array, dimension (N)
*>          The imaginary parts of each scalar alpha defining an
*>          eigenvalue of GNEP.
*>          If ALPHAI(j) is zero, then the j-th eigenvalue is real; if
*>          positive, then the j-th and (j+1)-st eigenvalues are a
*>          complex conjugate pair, with ALPHAI(j+1) = -ALPHAI(j).
*> \endverbatim
*>
*> \param[out] BETA
*> \verbatim
*>          BETA is REAL array, dimension (N)
*>          The scalars beta that define the eigenvalues of GNEP.
*>          Together, the quantities alpha = (ALPHAR(j),ALPHAI(j)) and
*>          beta = BETA(j) represent the j-th eigenvalue of the matrix
*>          pair (A,B), in one of the forms lambda = alpha/beta or
*>          mu = beta/alpha.  Since either lambda or mu may overflow,
*>          they should not, in general, be computed.
*> \endverbatim
*>
*> \param[in,out] QC
*> \verbatim
*>          QC is REAL array, dimension (LDQC, NW)
*> \endverbatim
*>
*> \param[in] LDQC
*> \verbatim
*>          LDQC is INTEGER
*> \endverbatim
*>
*> \param[in,out] ZC
*> \verbatim
*>          ZC is REAL array, dimension (LDZC, NW)
*> \endverbatim
*>
*> \param[in] LDZC
*> \verbatim
*>          LDZ is INTEGER
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension (MAX(1,LWORK))
*>          On exit, if INFO >= 0, WORK(1) returns the optimal LWORK.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the array WORK.  LWORK >= max(1,N).
*>
*>          If LWORK = -1, then a workspace query is assumed; the routine
*>          only calculates the optimal size of the WORK array, returns
*>          this value as the first entry of the WORK array, and no error
*>          message related to LWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[in] REC
*> \verbatim
*>          REC is INTEGER
*>             REC indicates the current recursion level. Should be set
*>             to 0 on first call.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0: successful exit
*>          < 0: if INFO = -i, the i-th argument had an illegal value
*> \endverbatim
*
*  Authors:
*  ========
*
*> \author Thijs Steel, KU Leuven
*
*> \date May 2020
*
*> \ingroup laqz3
*>
*  =====================================================================
      RECURSIVE SUBROUTINE SLAQZ3( ILSCHUR, ILQ, ILZ, N, ILO, IHI, NW,
     $                             A, LDA, B, LDB, Q, LDQ, Z, LDZ, NS,
     $                             ND, ALPHAR, ALPHAI, BETA, QC, LDQC,
     $                             ZC, LDZC, WORK, LWORK, REC, INFO )
      IMPLICIT NONE

*     Arguments
      LOGICAL, INTENT( IN ) :: ILSCHUR, ILQ, ILZ
      INTEGER, INTENT( IN ) :: N, ILO, IHI, NW, LDA, LDB, LDQ, LDZ,
     $         LDQC, LDZC, LWORK, REC

      REAL, INTENT( INOUT ) :: A( LDA, * ), B( LDB, * ), Q( LDQ, * ),
     $   Z( LDZ, * ), ALPHAR( * ), ALPHAI( * ), BETA( * )
      INTEGER, INTENT( OUT ) :: NS, ND, INFO
      REAL :: QC( LDQC, * ), ZC( LDZC, * ), WORK( * )

*     Parameters
      REAL :: ZERO, ONE, HALF
      PARAMETER( ZERO = 0.0, ONE = 1.0, HALF = 0.5 )

*     Local Scalars
      LOGICAL :: BULGE
      INTEGER :: JW, KWTOP, KWBOT, ISTOPM, ISTARTM, K, K2, STGEXC_INFO,
     $           IFST, ILST, LWORKREQ, QZ_SMALL_INFO
      REAL :: S, SMLNUM, ULP, SAFMIN, SAFMAX, C1, S1, TEMP

*     External Functions
      EXTERNAL :: XERBLA, STGEXC, SLAQZ0, SLACPY, SLASET,
     $            SLAQZ2, SROT, SLARTG, SLAG2, SGEMM
      REAL, EXTERNAL :: SLAMCH, SROUNDUP_LWORK

      INFO = 0

*     Set up deflation window
      JW = MIN( NW, IHI-ILO+1 )
      KWTOP = IHI-JW+1
      IF ( KWTOP .EQ. ILO ) THEN
         S = ZERO
      ELSE
         S = A( KWTOP, KWTOP-1 )
      END IF

*     Determine required workspace
      IFST = 1
      ILST = JW
      CALL STGEXC( .TRUE., .TRUE., JW, A, LDA, B, LDB, QC, LDQC, ZC,
     $             LDZC, IFST, ILST, WORK, -1, STGEXC_INFO )
      LWORKREQ = INT( WORK( 1 ) )
      CALL SLAQZ0( 'S', 'V', 'V', JW, 1, JW, A( KWTOP, KWTOP ), LDA,
     $             B( KWTOP, KWTOP ), LDB, ALPHAR, ALPHAI, BETA, QC,
     $             LDQC, ZC, LDZC, WORK, -1, REC+1, QZ_SMALL_INFO )
      LWORKREQ = MAX( LWORKREQ, INT( WORK( 1 ) )+2*JW**2 )
      LWORKREQ = MAX( LWORKREQ, N*NW, 2*NW**2+N )
      IF ( LWORK .EQ.-1 ) THEN
*        workspace query, quick return
         WORK( 1 ) = SROUNDUP_LWORK(LWORKREQ)
         RETURN
      ELSE IF ( LWORK .LT. LWORKREQ ) THEN
         INFO = -26
      END IF

      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'SLAQZ3', -INFO )
         RETURN
      END IF

*     Get machine constants
      SAFMIN = SLAMCH( 'SAFE MINIMUM' )
      SAFMAX = ONE/SAFMIN
      ULP = SLAMCH( 'PRECISION' )
      SMLNUM = SAFMIN*( REAL( N )/ULP )

      IF ( IHI .EQ. KWTOP ) THEN
*        1 by 1 deflation window, just try a regular deflation
         ALPHAR( KWTOP ) = A( KWTOP, KWTOP )
         ALPHAI( KWTOP ) = ZERO
         BETA( KWTOP ) = B( KWTOP, KWTOP )
         NS = 1
         ND = 0
         IF ( ABS( S ) .LE. MAX( SMLNUM, ULP*ABS( A( KWTOP,
     $      KWTOP ) ) ) ) THEN
            NS = 0
            ND = 1
            IF ( KWTOP .GT. ILO ) THEN
               A( KWTOP, KWTOP-1 ) = ZERO
            END IF
         END IF
      END IF


*     Store window in case of convergence failure
      CALL SLACPY( 'ALL', JW, JW, A( KWTOP, KWTOP ), LDA, WORK, JW )
      CALL SLACPY( 'ALL', JW, JW, B( KWTOP, KWTOP ), LDB, WORK( JW**2+
     $             1 ), JW )

*     Transform window to real schur form
      CALL SLASET( 'FULL', JW, JW, ZERO, ONE, QC, LDQC )
      CALL SLASET( 'FULL', JW, JW, ZERO, ONE, ZC, LDZC )
      CALL SLAQZ0( 'S', 'V', 'V', JW, 1, JW, A( KWTOP, KWTOP ), LDA,
     $             B( KWTOP, KWTOP ), LDB, ALPHAR, ALPHAI, BETA, QC,
     $             LDQC, ZC, LDZC, WORK( 2*JW**2+1 ), LWORK-2*JW**2,
     $             REC+1, QZ_SMALL_INFO )

      IF( QZ_SMALL_INFO .NE. 0 ) THEN
*        Convergence failure, restore the window and exit
         ND = 0
         NS = JW-QZ_SMALL_INFO
         CALL SLACPY( 'ALL', JW, JW, WORK, JW, A( KWTOP, KWTOP ), LDA )
         CALL SLACPY( 'ALL', JW, JW, WORK( JW**2+1 ), JW, B( KWTOP,
     $                KWTOP ), LDB )
         RETURN
      END IF

*     Deflation detection loop
      IF ( KWTOP .EQ. ILO .OR. S .EQ. ZERO ) THEN
         KWBOT = KWTOP-1
      ELSE
         KWBOT = IHI
         K = 1
         K2 = 1
         DO WHILE ( K .LE. JW )
            BULGE = .FALSE.
            IF ( KWBOT-KWTOP+1 .GE. 2 ) THEN
               BULGE = A( KWBOT, KWBOT-1 ) .NE. ZERO
            END IF
            IF ( BULGE ) THEN

*              Try to deflate complex conjugate eigenvalue pair
               TEMP = ABS( A( KWBOT, KWBOT ) )+SQRT( ABS( A( KWBOT,
     $            KWBOT-1 ) ) )*SQRT( ABS( A( KWBOT-1, KWBOT ) ) )
               IF( TEMP .EQ. ZERO )THEN
                  TEMP = ABS( S )
               END IF
               IF ( MAX( ABS( S*QC( 1, KWBOT-KWTOP ) ), ABS( S*QC( 1,
     $            KWBOT-KWTOP+1 ) ) ) .LE. MAX( SMLNUM,
     $            ULP*TEMP ) ) THEN
*                 Deflatable
                  KWBOT = KWBOT-2
               ELSE
*                 Not deflatable, move out of the way
                  IFST = KWBOT-KWTOP+1
                  ILST = K2
                  CALL STGEXC( .TRUE., .TRUE., JW, A( KWTOP, KWTOP ),
     $                         LDA, B( KWTOP, KWTOP ), LDB, QC, LDQC,
     $                         ZC, LDZC, IFST, ILST, WORK, LWORK,
     $                         STGEXC_INFO )
                  K2 = K2+2
               END IF
               K = K+2
            ELSE

*              Try to deflate real eigenvalue
               TEMP = ABS( A( KWBOT, KWBOT ) )
               IF( TEMP .EQ. ZERO ) THEN
                  TEMP = ABS( S )
               END IF
               IF ( ( ABS( S*QC( 1, KWBOT-KWTOP+1 ) ) ) .LE. MAX( ULP*
     $            TEMP, SMLNUM ) ) THEN
*                 Deflatable
                  KWBOT = KWBOT-1
               ELSE
*                 Not deflatable, move out of the way
                  IFST = KWBOT-KWTOP+1
                  ILST = K2
                  CALL STGEXC( .TRUE., .TRUE., JW, A( KWTOP, KWTOP ),
     $                         LDA, B( KWTOP, KWTOP ), LDB, QC, LDQC,
     $                         ZC, LDZC, IFST, ILST, WORK, LWORK,
     $                         STGEXC_INFO )
                  K2 = K2+1
               END IF

               K = K+1

            END IF
         END DO
      END IF

*     Store eigenvalues
      ND = IHI-KWBOT
      NS = JW-ND
      K = KWTOP
      DO WHILE ( K .LE. IHI )
         BULGE = .FALSE.
         IF ( K .LT. IHI ) THEN
            IF ( A( K+1, K ) .NE. ZERO ) THEN
               BULGE = .TRUE.
            END IF
         END IF
         IF ( BULGE ) THEN
*           2x2 eigenvalue block
            CALL SLAG2( A( K, K ), LDA, B( K, K ), LDB, SAFMIN,
     $                  BETA( K ), BETA( K+1 ), ALPHAR( K ),
     $                  ALPHAR( K+1 ), ALPHAI( K ) )
            ALPHAI( K+1 ) = -ALPHAI( K )
            K = K+2
         ELSE
*           1x1 eigenvalue block
            ALPHAR( K ) = A( K, K )
            ALPHAI( K ) = ZERO
            BETA( K ) = B( K, K )
            K = K+1
         END IF
      END DO

      IF ( KWTOP .NE. ILO .AND. S .NE. ZERO ) THEN
*        Reflect spike back, this will create optimally packed bulges
         A( KWTOP:KWBOT, KWTOP-1 ) = A( KWTOP, KWTOP-1 )*QC( 1,
     $      1:JW-ND )
         DO K = KWBOT-1, KWTOP, -1
            CALL SLARTG( A( K, KWTOP-1 ), A( K+1, KWTOP-1 ), C1, S1,
     $                   TEMP )
            A( K, KWTOP-1 ) = TEMP
            A( K+1, KWTOP-1 ) = ZERO
            K2 = MAX( KWTOP, K-1 )
            CALL SROT( IHI-K2+1, A( K, K2 ), LDA, A( K+1, K2 ), LDA, C1,
     $                 S1 )
            CALL SROT( IHI-( K-1 )+1, B( K, K-1 ), LDB, B( K+1, K-1 ),
     $                 LDB, C1, S1 )
            CALL SROT( JW, QC( 1, K-KWTOP+1 ), 1, QC( 1, K+1-KWTOP+1 ),
     $                 1, C1, S1 )
         END DO

*        Chase bulges down
         ISTARTM = KWTOP
         ISTOPM = IHI
         K = KWBOT-1
         DO WHILE ( K .GE. KWTOP )
            IF ( ( K .GE. KWTOP+1 ) .AND. A( K+1, K-1 ) .NE. ZERO ) THEN

*              Move double pole block down and remove it
               DO K2 = K-1, KWBOT-2
                  CALL SLAQZ2( .TRUE., .TRUE., K2, KWTOP, KWTOP+JW-1,
     $                         KWBOT, A, LDA, B, LDB, JW, KWTOP, QC,
     $                         LDQC, JW, KWTOP, ZC, LDZC )
               END DO

               K = K-2
            ELSE

*              k points to single shift
               DO K2 = K, KWBOT-2

*                 Move shift down
                  CALL SLARTG( B( K2+1, K2+1 ), B( K2+1, K2 ), C1, S1,
     $                         TEMP )
                  B( K2+1, K2+1 ) = TEMP
                  B( K2+1, K2 ) = ZERO
                  CALL SROT( K2+2-ISTARTM+1, A( ISTARTM, K2+1 ), 1,
     $                       A( ISTARTM, K2 ), 1, C1, S1 )
                  CALL SROT( K2-ISTARTM+1, B( ISTARTM, K2+1 ), 1,
     $                       B( ISTARTM, K2 ), 1, C1, S1 )
                  CALL SROT( JW, ZC( 1, K2+1-KWTOP+1 ), 1, ZC( 1,
     $                       K2-KWTOP+1 ), 1, C1, S1 )
            
                  CALL SLARTG( A( K2+1, K2 ), A( K2+2, K2 ), C1, S1,
     $                         TEMP )
                  A( K2+1, K2 ) = TEMP
                  A( K2+2, K2 ) = ZERO
                  CALL SROT( ISTOPM-K2, A( K2+1, K2+1 ), LDA, A( K2+2,
     $                       K2+1 ), LDA, C1, S1 )
                  CALL SROT( ISTOPM-K2, B( K2+1, K2+1 ), LDB, B( K2+2,
     $                       K2+1 ), LDB, C1, S1 )
                  CALL SROT( JW, QC( 1, K2+1-KWTOP+1 ), 1, QC( 1,
     $                       K2+2-KWTOP+1 ), 1, C1, S1 )

               END DO

*              Remove the shift
               CALL SLARTG( B( KWBOT, KWBOT ), B( KWBOT, KWBOT-1 ), C1,
     $                      S1, TEMP )
               B( KWBOT, KWBOT ) = TEMP
               B( KWBOT, KWBOT-1 ) = ZERO
               CALL SROT( KWBOT-ISTARTM, B( ISTARTM, KWBOT ), 1,
     $                    B( ISTARTM, KWBOT-1 ), 1, C1, S1 )
               CALL SROT( KWBOT-ISTARTM+1, A( ISTARTM, KWBOT ), 1,
     $                    A( ISTARTM, KWBOT-1 ), 1, C1, S1 )
               CALL SROT( JW, ZC( 1, KWBOT-KWTOP+1 ), 1, ZC( 1,
     $                    KWBOT-1-KWTOP+1 ), 1, C1, S1 )

               K = K-1
            END IF
         END DO

      END IF

*     Apply Qc and Zc to rest of the matrix
      IF ( ILSCHUR ) THEN
         ISTARTM = 1
         ISTOPM = N
      ELSE
         ISTARTM = ILO
         ISTOPM = IHI
      END IF

      IF ( ISTOPM-IHI > 0 ) THEN
         CALL SGEMM( 'T', 'N', JW, ISTOPM-IHI, JW, ONE, QC, LDQC,
     $               A( KWTOP, IHI+1 ), LDA, ZERO, WORK, JW )
         CALL SLACPY( 'ALL', JW, ISTOPM-IHI, WORK, JW, A( KWTOP,
     $                IHI+1 ), LDA )
         CALL SGEMM( 'T', 'N', JW, ISTOPM-IHI, JW, ONE, QC, LDQC,
     $               B( KWTOP, IHI+1 ), LDB, ZERO, WORK, JW )
         CALL SLACPY( 'ALL', JW, ISTOPM-IHI, WORK, JW, B( KWTOP,
     $                IHI+1 ), LDB )
      END IF
      IF ( ILQ ) THEN
         CALL SGEMM( 'N', 'N', N, JW, JW, ONE, Q( 1, KWTOP ), LDQ, QC,
     $               LDQC, ZERO, WORK, N )
         CALL SLACPY( 'ALL', N, JW, WORK, N, Q( 1, KWTOP ), LDQ )
      END IF

      IF ( KWTOP-1-ISTARTM+1 > 0 ) THEN
         CALL SGEMM( 'N', 'N', KWTOP-ISTARTM, JW, JW, ONE, A( ISTARTM,
     $               KWTOP ), LDA, ZC, LDZC, ZERO, WORK,
     $               KWTOP-ISTARTM )
         CALL SLACPY( 'ALL', KWTOP-ISTARTM, JW, WORK, KWTOP-ISTARTM,
     $                A( ISTARTM, KWTOP ), LDA )
         CALL SGEMM( 'N', 'N', KWTOP-ISTARTM, JW, JW, ONE, B( ISTARTM,
     $               KWTOP ), LDB, ZC, LDZC, ZERO, WORK,
     $               KWTOP-ISTARTM )
         CALL SLACPY( 'ALL', KWTOP-ISTARTM, JW, WORK, KWTOP-ISTARTM,
     $                B( ISTARTM, KWTOP ), LDB )
      END IF
      IF ( ILZ ) THEN
         CALL SGEMM( 'N', 'N', N, JW, JW, ONE, Z( 1, KWTOP ), LDZ, ZC,
     $               LDZC, ZERO, WORK, N )
         CALL SLACPY( 'ALL', N, JW, WORK, N, Z( 1, KWTOP ), LDZ )
      END IF

      END SUBROUTINE
