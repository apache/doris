*> \brief \b DLAQZ0
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DLAQZ0 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dlaqz0.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dlaqz0.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dlaqz0.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*     RECURSIVE SUBROUTINE DLAQZ0( WANTS, WANTQ, WANTZ, N, ILO, IHI, A,
*    $                             LDA, B, LDB, ALPHAR, ALPHAI, BETA,
*    $                             Q, LDQ, Z, LDZ, WORK, LWORK, REC,
*    $                             INFO )
*     IMPLICIT NONE
*
*     Arguments
*     CHARACTER, INTENT( IN ) :: WANTS, WANTQ, WANTZ
*     INTEGER, INTENT( IN ) :: N, ILO, IHI, LDA, LDB, LDQ, LDZ, LWORK,
*    $         REC
*
*     INTEGER, INTENT( OUT ) :: INFO
*
*     DOUBLE PRECISION, INTENT( INOUT ) :: A( LDA, * ), B( LDB, * ),
*    $                  Q( LDQ, * ), Z( LDZ, * ), ALPHAR( * ),
*    $                  ALPHAI( * ), BETA( * ), WORK( * )
*      ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DLAQZ0 computes the eigenvalues of a real matrix pair (H,T),
*> where H is an upper Hessenberg matrix and T is upper triangular,
*> using the double-shift QZ method.
*> Matrix pairs of this type are produced by the reduction to
*> generalized upper Hessenberg form of a real matrix pair (A,B):
*>
*>    A = Q1*H*Z1**T,  B = Q1*T*Z1**T,
*>
*> as computed by DGGHRD.
*>
*> If JOB='S', then the Hessenberg-triangular pair (H,T) is
*> also reduced to generalized Schur form,
*>
*>    H = Q*S*Z**T,  T = Q*P*Z**T,
*>
*> where Q and Z are orthogonal matrices, P is an upper triangular
*> matrix, and S is a quasi-triangular matrix with 1-by-1 and 2-by-2
*> diagonal blocks.
*>
*> The 1-by-1 blocks correspond to real eigenvalues of the matrix pair
*> (H,T) and the 2-by-2 blocks correspond to complex conjugate pairs of
*> eigenvalues.
*>
*> Additionally, the 2-by-2 upper triangular diagonal blocks of P
*> corresponding to 2-by-2 blocks of S are reduced to positive diagonal
*> form, i.e., if S(j+1,j) is non-zero, then P(j+1,j) = P(j,j+1) = 0,
*> P(j,j) > 0, and P(j+1,j+1) > 0.
*>
*> Optionally, the orthogonal matrix Q from the generalized Schur
*> factorization may be postmultiplied into an input matrix Q1, and the
*> orthogonal matrix Z may be postmultiplied into an input matrix Z1.
*> If Q1 and Z1 are the orthogonal matrices from DGGHRD that reduced
*> the matrix pair (A,B) to generalized upper Hessenberg form, then the
*> output matrices Q1*Q and Z1*Z are the orthogonal factors from the
*> generalized Schur factorization of (A,B):
*>
*>    A = (Q1*Q)*S*(Z1*Z)**T,  B = (Q1*Q)*P*(Z1*Z)**T.
*>
*> To avoid overflow, eigenvalues of the matrix pair (H,T) (equivalently,
*> of (A,B)) are computed as a pair of values (alpha,beta), where alpha is
*> complex and beta real.
*> If beta is nonzero, lambda = alpha / beta is an eigenvalue of the
*> generalized nonsymmetric eigenvalue problem (GNEP)
*>    A*x = lambda*B*x
*> and if alpha is nonzero, mu = beta / alpha is an eigenvalue of the
*> alternate form of the GNEP
*>    mu*A*y = B*y.
*> Real eigenvalues can be read directly from the generalized Schur
*> form:
*>   alpha = S(i,i), beta = P(i,i).
*>
*> Ref: C.B. Moler & G.W. Stewart, "An Algorithm for Generalized Matrix
*>      Eigenvalue Problems", SIAM J. Numer. Anal., 10(1973),
*>      pp. 241--256.
*>
*> Ref: B. Kagstrom, D. Kressner, "Multishift Variants of the QZ
*>      Algorithm with Aggressive Early Deflation", SIAM J. Numer.
*>      Anal., 29(2006), pp. 199--227.
*>
*> Ref: T. Steel, D. Camps, K. Meerbergen, R. Vandebril "A multishift,
*>      multipole rational QZ method with aggressive early deflation"
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] WANTS
*> \verbatim
*>          WANTS is CHARACTER*1
*>          = 'E': Compute eigenvalues only;
*>          = 'S': Compute eigenvalues and the Schur form.
*> \endverbatim
*>
*> \param[in] WANTQ
*> \verbatim
*>          WANTQ is CHARACTER*1
*>          = 'N': Left Schur vectors (Q) are not computed;
*>          = 'I': Q is initialized to the unit matrix and the matrix Q
*>                 of left Schur vectors of (A,B) is returned;
*>          = 'V': Q must contain an orthogonal matrix Q1 on entry and
*>                 the product Q1*Q is returned.
*> \endverbatim
*>
*> \param[in] WANTZ
*> \verbatim
*>          WANTZ is CHARACTER*1
*>          = 'N': Right Schur vectors (Z) are not computed;
*>          = 'I': Z is initialized to the unit matrix and the matrix Z
*>                 of right Schur vectors of (A,B) is returned;
*>          = 'V': Z must contain an orthogonal matrix Z1 on entry and
*>                 the product Z1*Z is returned.
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
*>          ILO and IHI mark the rows and columns of A which are in
*>          Hessenberg form.  It is assumed that A is already upper
*>          triangular in rows and columns 1:ILO-1 and IHI+1:N.
*>          If N > 0, 1 <= ILO <= IHI <= N; if N = 0, ILO=1 and IHI=0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is DOUBLE PRECISION array, dimension (LDA, N)
*>          On entry, the N-by-N upper Hessenberg matrix A.
*>          On exit, if JOB = 'S', A contains the upper quasi-triangular
*>          matrix S from the generalized Schur factorization.
*>          If JOB = 'E', the diagonal blocks of A match those of S, but
*>          the rest of A is unspecified.
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
*>          B is DOUBLE PRECISION array, dimension (LDB, N)
*>          On entry, the N-by-N upper triangular matrix B.
*>          On exit, if JOB = 'S', B contains the upper triangular
*>          matrix P from the generalized Schur factorization;
*>          2-by-2 diagonal blocks of P corresponding to 2-by-2 blocks of S
*>          are reduced to positive diagonal form, i.e., if A(j+1,j) is
*>          non-zero, then B(j+1,j) = B(j,j+1) = 0, B(j,j) > 0, and
*>          B(j+1,j+1) > 0.
*>          If JOB = 'E', the diagonal blocks of B match those of P, but
*>          the rest of B is unspecified.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B.  LDB >= max( 1, N ).
*> \endverbatim
*>
*> \param[out] ALPHAR
*> \verbatim
*>          ALPHAR is DOUBLE PRECISION array, dimension (N)
*>          The real parts of each scalar alpha defining an eigenvalue
*>          of GNEP.
*> \endverbatim
*>
*> \param[out] ALPHAI
*> \verbatim
*>          ALPHAI is DOUBLE PRECISION array, dimension (N)
*>          The imaginary parts of each scalar alpha defining an
*>          eigenvalue of GNEP.
*>          If ALPHAI(j) is zero, then the j-th eigenvalue is real; if
*>          positive, then the j-th and (j+1)-st eigenvalues are a
*>          complex conjugate pair, with ALPHAI(j+1) = -ALPHAI(j).
*> \endverbatim
*>
*> \param[out] BETA
*> \verbatim
*>          BETA is DOUBLE PRECISION array, dimension (N)
*>          The scalars beta that define the eigenvalues of GNEP.
*>          Together, the quantities alpha = (ALPHAR(j),ALPHAI(j)) and
*>          beta = BETA(j) represent the j-th eigenvalue of the matrix
*>          pair (A,B), in one of the forms lambda = alpha/beta or
*>          mu = beta/alpha.  Since either lambda or mu may overflow,
*>          they should not, in general, be computed.
*> \endverbatim
*>
*> \param[in,out] Q
*> \verbatim
*>          Q is DOUBLE PRECISION array, dimension (LDQ, N)
*>          On entry, if COMPQ = 'V', the orthogonal matrix Q1 used in
*>          the reduction of (A,B) to generalized Hessenberg form.
*>          On exit, if COMPQ = 'I', the orthogonal matrix of left Schur
*>          vectors of (A,B), and if COMPQ = 'V', the orthogonal matrix
*>          of left Schur vectors of (A,B).
*>          Not referenced if COMPQ = 'N'.
*> \endverbatim
*>
*> \param[in] LDQ
*> \verbatim
*>          LDQ is INTEGER
*>          The leading dimension of the array Q.  LDQ >= 1.
*>          If COMPQ='V' or 'I', then LDQ >= N.
*> \endverbatim
*>
*> \param[in,out] Z
*> \verbatim
*>          Z is DOUBLE PRECISION array, dimension (LDZ, N)
*>          On entry, if COMPZ = 'V', the orthogonal matrix Z1 used in
*>          the reduction of (A,B) to generalized Hessenberg form.
*>          On exit, if COMPZ = 'I', the orthogonal matrix of
*>          right Schur vectors of (H,T), and if COMPZ = 'V', the
*>          orthogonal matrix of right Schur vectors of (A,B).
*>          Not referenced if COMPZ = 'N'.
*> \endverbatim
*>
*> \param[in] LDZ
*> \verbatim
*>          LDZ is INTEGER
*>          The leading dimension of the array Z.  LDZ >= 1.
*>          If COMPZ='V' or 'I', then LDZ >= N.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (MAX(1,LWORK))
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
*>          = 1,...,N: the QZ iteration did not converge.  (A,B) is not
*>                     in Schur form, but ALPHAR(i), ALPHAI(i), and
*>                     BETA(i), i=INFO+1,...,N should be correct.
*> \endverbatim
*
*  Authors:
*  ========
*
*> \author Thijs Steel, KU Leuven
*
*> \date May 2020
*
*> \ingroup doubleGEcomputational
*>
*  =====================================================================
      RECURSIVE SUBROUTINE DLAQZ0( WANTS, WANTQ, WANTZ, N, ILO, IHI, A,
     $                             LDA, B, LDB, ALPHAR, ALPHAI, BETA,
     $                             Q, LDQ, Z, LDZ, WORK, LWORK, REC,
     $                             INFO )
      IMPLICIT NONE

*     Arguments
      CHARACTER, INTENT( IN ) :: WANTS, WANTQ, WANTZ
      INTEGER, INTENT( IN ) :: N, ILO, IHI, LDA, LDB, LDQ, LDZ, LWORK,
     $         REC

      INTEGER, INTENT( OUT ) :: INFO

      DOUBLE PRECISION, INTENT( INOUT ) :: A( LDA, * ), B( LDB, * ),
     $                  Q( LDQ, * ), Z( LDZ, * ), ALPHAR( * ),
     $                  ALPHAI( * ), BETA( * ), WORK( * )

*     Parameters
      DOUBLE PRECISION :: ZERO, ONE, HALF
      PARAMETER( ZERO = 0.0D0, ONE = 1.0D0, HALF = 0.5D0 )

*     Local scalars
      DOUBLE PRECISION :: SMLNUM, ULP, ESHIFT, SAFMIN, SAFMAX, C1, S1,
     $                    TEMP, SWAP, BNORM, BTOL
      INTEGER :: ISTART, ISTOP, IITER, MAXIT, ISTART2, K, LD, NSHIFTS,
     $           NBLOCK, NW, NMIN, NIBBLE, N_UNDEFLATED, N_DEFLATED,
     $           NS, SWEEP_INFO, SHIFTPOS, LWORKREQ, K2, ISTARTM,
     $           ISTOPM, IWANTS, IWANTQ, IWANTZ, NORM_INFO, AED_INFO,
     $           NWR, NBR, NSR, ITEMP1, ITEMP2, RCOST, I
      LOGICAL :: ILSCHUR, ILQ, ILZ
      CHARACTER :: JBCMPZ*3

*     External Functions
      EXTERNAL :: XERBLA, DHGEQZ, DLASET, DLAQZ3, DLAQZ4,
     $            DLARTG, DROT
      DOUBLE PRECISION, EXTERNAL :: DLAMCH, DLANHS
      LOGICAL, EXTERNAL :: LSAME
      INTEGER, EXTERNAL :: ILAENV

*
*     Decode wantS,wantQ,wantZ
*      
      IF( LSAME( WANTS, 'E' ) ) THEN
         ILSCHUR = .FALSE.
         IWANTS = 1
      ELSE IF( LSAME( WANTS, 'S' ) ) THEN
         ILSCHUR = .TRUE.
         IWANTS = 2
      ELSE
         IWANTS = 0
      END IF

      IF( LSAME( WANTQ, 'N' ) ) THEN
         ILQ = .FALSE.
         IWANTQ = 1
      ELSE IF( LSAME( WANTQ, 'V' ) ) THEN
         ILQ = .TRUE.
         IWANTQ = 2
      ELSE IF( LSAME( WANTQ, 'I' ) ) THEN
         ILQ = .TRUE.
         IWANTQ = 3
      ELSE
         IWANTQ = 0
      END IF

      IF( LSAME( WANTZ, 'N' ) ) THEN
         ILZ = .FALSE.
         IWANTZ = 1
      ELSE IF( LSAME( WANTZ, 'V' ) ) THEN
         ILZ = .TRUE.
         IWANTZ = 2
      ELSE IF( LSAME( WANTZ, 'I' ) ) THEN
         ILZ = .TRUE.
         IWANTZ = 3
      ELSE
         IWANTZ = 0
      END IF
*
*     Check Argument Values
*
      INFO = 0
      IF( IWANTS.EQ.0 ) THEN
         INFO = -1
      ELSE IF( IWANTQ.EQ.0 ) THEN
         INFO = -2
      ELSE IF( IWANTZ.EQ.0 ) THEN
         INFO = -3
      ELSE IF( N.LT.0 ) THEN
         INFO = -4
      ELSE IF( ILO.LT.1 ) THEN
         INFO = -5
      ELSE IF( IHI.GT.N .OR. IHI.LT.ILO-1 ) THEN
         INFO = -6
      ELSE IF( LDA.LT.N ) THEN
         INFO = -8
      ELSE IF( LDB.LT.N ) THEN
         INFO = -10
      ELSE IF( LDQ.LT.1 .OR. ( ILQ .AND. LDQ.LT.N ) ) THEN
         INFO = -15
      ELSE IF( LDZ.LT.1 .OR. ( ILZ .AND. LDZ.LT.N ) ) THEN
         INFO = -17
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'DLAQZ0', -INFO )
         RETURN
      END IF
   
*
*     Quick return if possible
*
      IF( N.LE.0 ) THEN
         WORK( 1 ) = DBLE( 1 )
         RETURN
      END IF

*
*     Get the parameters
*
      JBCMPZ( 1:1 ) = WANTS
      JBCMPZ( 2:2 ) = WANTQ
      JBCMPZ( 3:3 ) = WANTZ

      NMIN = ILAENV( 12, 'DLAQZ0', JBCMPZ, N, ILO, IHI, LWORK )

      NWR = ILAENV( 13, 'DLAQZ0', JBCMPZ, N, ILO, IHI, LWORK )
      NWR = MAX( 2, NWR )
      NWR = MIN( IHI-ILO+1, ( N-1 ) / 3, NWR )

      NIBBLE = ILAENV( 14, 'DLAQZ0', JBCMPZ, N, ILO, IHI, LWORK )
      
      NSR = ILAENV( 15, 'DLAQZ0', JBCMPZ, N, ILO, IHI, LWORK )
      NSR = MIN( NSR, ( N+6 ) / 9, IHI-ILO )
      NSR = MAX( 2, NSR-MOD( NSR, 2 ) )

      RCOST = ILAENV( 17, 'DLAQZ0', JBCMPZ, N, ILO, IHI, LWORK )
      ITEMP1 = INT( NSR/SQRT( 1+2*NSR/( DBLE( RCOST )/100*N ) ) )
      ITEMP1 = ( ( ITEMP1-1 )/4 )*4+4
      NBR = NSR+ITEMP1

      IF( N .LT. NMIN .OR. REC .GE. 2 ) THEN
         CALL DHGEQZ( WANTS, WANTQ, WANTZ, N, ILO, IHI, A, LDA, B, LDB,
     $                ALPHAR, ALPHAI, BETA, Q, LDQ, Z, LDZ, WORK,
     $                LWORK, INFO )
         RETURN
      END IF

*
*     Find out required workspace
*

*     Workspace query to dlaqz3
      NW = MAX( NWR, NMIN )
      CALL DLAQZ3( ILSCHUR, ILQ, ILZ, N, ILO, IHI, NW, A, LDA, B, LDB,
     $             Q, LDQ, Z, LDZ, N_UNDEFLATED, N_DEFLATED, ALPHAR,
     $             ALPHAI, BETA, WORK, NW, WORK, NW, WORK, -1, REC,
     $             AED_INFO )
      ITEMP1 = INT( WORK( 1 ) )
*     Workspace query to dlaqz4
      CALL DLAQZ4( ILSCHUR, ILQ, ILZ, N, ILO, IHI, NSR, NBR, ALPHAR,
     $             ALPHAI, BETA, A, LDA, B, LDB, Q, LDQ, Z, LDZ, WORK,
     $             NBR, WORK, NBR, WORK, -1, SWEEP_INFO )
      ITEMP2 = INT( WORK( 1 ) )

      LWORKREQ = MAX( ITEMP1+2*NW**2, ITEMP2+2*NBR**2 )
      IF ( LWORK .EQ.-1 ) THEN
         WORK( 1 ) = DBLE( LWORKREQ )
         RETURN
      ELSE IF ( LWORK .LT. LWORKREQ ) THEN
         INFO = -19
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'DLAQZ0', INFO )
         RETURN
      END IF
*
*     Initialize Q and Z
*
      IF( IWANTQ.EQ.3 ) CALL DLASET( 'FULL', N, N, ZERO, ONE, Q, LDQ )
      IF( IWANTZ.EQ.3 ) CALL DLASET( 'FULL', N, N, ZERO, ONE, Z, LDZ )

*     Get machine constants
      SAFMIN = DLAMCH( 'SAFE MINIMUM' )
      SAFMAX = ONE/SAFMIN
      ULP = DLAMCH( 'PRECISION' )
      SMLNUM = SAFMIN*( DBLE( N )/ULP )

      BNORM = DLANHS( 'F', IHI-ILO+1, B( ILO, ILO ), LDB, WORK )
      BTOL = MAX( SAFMIN, ULP*BNORM )

      ISTART = ILO
      ISTOP = IHI
      MAXIT = 3*( IHI-ILO+1 )
      LD = 0
 
      DO IITER = 1, MAXIT
         IF( IITER .GE. MAXIT ) THEN
            INFO = ISTOP+1
            GOTO 80
         END IF
         IF ( ISTART+1 .GE. ISTOP ) THEN
            ISTOP = ISTART
            EXIT
         END IF

*        Check deflations at the end
         IF ( ABS( A( ISTOP-1, ISTOP-2 ) ) .LE. MAX( SMLNUM,
     $      ULP*( ABS( A( ISTOP-1, ISTOP-1 ) )+ABS( A( ISTOP-2,
     $      ISTOP-2 ) ) ) ) ) THEN
            A( ISTOP-1, ISTOP-2 ) = ZERO
            ISTOP = ISTOP-2
            LD = 0
            ESHIFT = ZERO
         ELSE IF ( ABS( A( ISTOP, ISTOP-1 ) ) .LE. MAX( SMLNUM,
     $      ULP*( ABS( A( ISTOP, ISTOP ) )+ABS( A( ISTOP-1,
     $      ISTOP-1 ) ) ) ) ) THEN
            A( ISTOP, ISTOP-1 ) = ZERO
            ISTOP = ISTOP-1
            LD = 0
            ESHIFT = ZERO
         END IF
*        Check deflations at the start
         IF ( ABS( A( ISTART+2, ISTART+1 ) ) .LE. MAX( SMLNUM,
     $      ULP*( ABS( A( ISTART+1, ISTART+1 ) )+ABS( A( ISTART+2,
     $      ISTART+2 ) ) ) ) ) THEN
            A( ISTART+2, ISTART+1 ) = ZERO
            ISTART = ISTART+2
            LD = 0
            ESHIFT = ZERO
         ELSE IF ( ABS( A( ISTART+1, ISTART ) ) .LE. MAX( SMLNUM,
     $      ULP*( ABS( A( ISTART, ISTART ) )+ABS( A( ISTART+1,
     $      ISTART+1 ) ) ) ) ) THEN
            A( ISTART+1, ISTART ) = ZERO
            ISTART = ISTART+1
            LD = 0
            ESHIFT = ZERO
         END IF

         IF ( ISTART+1 .GE. ISTOP ) THEN
            EXIT
         END IF

*        Check interior deflations
         ISTART2 = ISTART
         DO K = ISTOP, ISTART+1, -1
            IF ( ABS( A( K, K-1 ) ) .LE. MAX( SMLNUM, ULP*( ABS( A( K,
     $         K ) )+ABS( A( K-1, K-1 ) ) ) ) ) THEN
               A( K, K-1 ) = ZERO
               ISTART2 = K
               EXIT
            END IF
         END DO

*        Get range to apply rotations to
         IF ( ILSCHUR ) THEN
            ISTARTM = 1
            ISTOPM = N
         ELSE
            ISTARTM = ISTART2
            ISTOPM = ISTOP
         END IF

*        Check infinite eigenvalues, this is done without blocking so might
*        slow down the method when many infinite eigenvalues are present
         K = ISTOP
         DO WHILE ( K.GE.ISTART2 )

            IF( ABS( B( K, K ) ) .LT. BTOL ) THEN
*              A diagonal element of B is negligible, move it
*              to the top and deflate it
               
               DO K2 = K, ISTART2+1, -1
                  CALL DLARTG( B( K2-1, K2 ), B( K2-1, K2-1 ), C1, S1,
     $                         TEMP )
                  B( K2-1, K2 ) = TEMP
                  B( K2-1, K2-1 ) = ZERO

                  CALL DROT( K2-2-ISTARTM+1, B( ISTARTM, K2 ), 1,
     $                       B( ISTARTM, K2-1 ), 1, C1, S1 )
                  CALL DROT( MIN( K2+1, ISTOP )-ISTARTM+1, A( ISTARTM,
     $                       K2 ), 1, A( ISTARTM, K2-1 ), 1, C1, S1 )
                  IF ( ILZ ) THEN
                     CALL DROT( N, Z( 1, K2 ), 1, Z( 1, K2-1 ), 1, C1,
     $                          S1 )
                  END IF

                  IF( K2.LT.ISTOP ) THEN
                     CALL DLARTG( A( K2, K2-1 ), A( K2+1, K2-1 ), C1,
     $                            S1, TEMP )
                     A( K2, K2-1 ) = TEMP
                     A( K2+1, K2-1 ) = ZERO

                     CALL DROT( ISTOPM-K2+1, A( K2, K2 ), LDA, A( K2+1,
     $                          K2 ), LDA, C1, S1 )
                     CALL DROT( ISTOPM-K2+1, B( K2, K2 ), LDB, B( K2+1,
     $                          K2 ), LDB, C1, S1 )
                     IF( ILQ ) THEN
                        CALL DROT( N, Q( 1, K2 ), 1, Q( 1, K2+1 ), 1,
     $                             C1, S1 )
                     END IF
                  END IF

               END DO

               IF( ISTART2.LT.ISTOP )THEN
                  CALL DLARTG( A( ISTART2, ISTART2 ), A( ISTART2+1,
     $                         ISTART2 ), C1, S1, TEMP )
                  A( ISTART2, ISTART2 ) = TEMP
                  A( ISTART2+1, ISTART2 ) = ZERO

                  CALL DROT( ISTOPM-( ISTART2+1 )+1, A( ISTART2,
     $                       ISTART2+1 ), LDA, A( ISTART2+1,
     $                       ISTART2+1 ), LDA, C1, S1 )
                  CALL DROT( ISTOPM-( ISTART2+1 )+1, B( ISTART2,
     $                       ISTART2+1 ), LDB, B( ISTART2+1,
     $                       ISTART2+1 ), LDB, C1, S1 )
                  IF( ILQ ) THEN
                     CALL DROT( N, Q( 1, ISTART2 ), 1, Q( 1,
     $                          ISTART2+1 ), 1, C1, S1 )
                  END IF
               END IF

               ISTART2 = ISTART2+1
   
            END IF
            K = K-1
         END DO

*        istart2 now points to the top of the bottom right
*        unreduced Hessenberg block
         IF ( ISTART2 .GE. ISTOP ) THEN
            ISTOP = ISTART2-1
            LD = 0
            ESHIFT = ZERO
            CYCLE
         END IF

         NW = NWR
         NSHIFTS = NSR
         NBLOCK = NBR

         IF ( ISTOP-ISTART2+1 .LT. NMIN ) THEN
*           Setting nw to the size of the subblock will make AED deflate
*           all the eigenvalues. This is slightly more efficient than just
*           using DHGEQZ because the off diagonal part gets updated via BLAS.
            IF ( ISTOP-ISTART+1 .LT. NMIN ) THEN
               NW = ISTOP-ISTART+1
               ISTART2 = ISTART
            ELSE
               NW = ISTOP-ISTART2+1
            END IF
         END IF

*
*        Time for AED
*
         CALL DLAQZ3( ILSCHUR, ILQ, ILZ, N, ISTART2, ISTOP, NW, A, LDA,
     $                B, LDB, Q, LDQ, Z, LDZ, N_UNDEFLATED, N_DEFLATED,
     $                ALPHAR, ALPHAI, BETA, WORK, NW, WORK( NW**2+1 ),
     $                NW, WORK( 2*NW**2+1 ), LWORK-2*NW**2, REC,
     $                AED_INFO )

         IF ( N_DEFLATED > 0 ) THEN
            ISTOP = ISTOP-N_DEFLATED
            LD = 0
            ESHIFT = ZERO
         END IF

         IF ( 100*N_DEFLATED > NIBBLE*( N_DEFLATED+N_UNDEFLATED ) .OR.
     $      ISTOP-ISTART2+1 .LT. NMIN ) THEN
*           AED has uncovered many eigenvalues. Skip a QZ sweep and run
*           AED again.
            CYCLE
         END IF

         LD = LD+1

         NS = MIN( NSHIFTS, ISTOP-ISTART2 )
         NS = MIN( NS, N_UNDEFLATED )
         SHIFTPOS = ISTOP-N_UNDEFLATED+1
*
*        Shuffle shifts to put double shifts in front
*        This ensures that we don't split up a double shift
*
         DO I = SHIFTPOS, SHIFTPOS+N_UNDEFLATED-1, 2
            IF( ALPHAI( I ).NE.-ALPHAI( I+1 ) ) THEN
*
               SWAP = ALPHAR( I )
               ALPHAR( I ) = ALPHAR( I+1 )
               ALPHAR( I+1 ) = ALPHAR( I+2 )
               ALPHAR( I+2 ) = SWAP

               SWAP = ALPHAI( I )
               ALPHAI( I ) = ALPHAI( I+1 )
               ALPHAI( I+1 ) = ALPHAI( I+2 )
               ALPHAI( I+2 ) = SWAP
               
               SWAP = BETA( I )
               BETA( I ) = BETA( I+1 )
               BETA( I+1 ) = BETA( I+2 )
               BETA( I+2 ) = SWAP
            END IF
         END DO

         IF ( MOD( LD, 6 ) .EQ. 0 ) THEN
* 
*           Exceptional shift.  Chosen for no particularly good reason.
*
            IF( ( DBLE( MAXIT )*SAFMIN )*ABS( A( ISTOP,
     $         ISTOP-1 ) ).LT.ABS( A( ISTOP-1, ISTOP-1 ) ) ) THEN
               ESHIFT = A( ISTOP, ISTOP-1 )/B( ISTOP-1, ISTOP-1 )
            ELSE
               ESHIFT = ESHIFT+ONE/( SAFMIN*DBLE( MAXIT ) )
            END IF
            ALPHAR( SHIFTPOS ) = ONE
            ALPHAR( SHIFTPOS+1 ) = ZERO
            ALPHAI( SHIFTPOS ) = ZERO
            ALPHAI( SHIFTPOS+1 ) = ZERO
            BETA( SHIFTPOS ) = ESHIFT
            BETA( SHIFTPOS+1 ) = ESHIFT
            NS = 2
         END IF

*
*        Time for a QZ sweep
*
         CALL DLAQZ4( ILSCHUR, ILQ, ILZ, N, ISTART2, ISTOP, NS, NBLOCK,
     $                ALPHAR( SHIFTPOS ), ALPHAI( SHIFTPOS ),
     $                BETA( SHIFTPOS ), A, LDA, B, LDB, Q, LDQ, Z, LDZ,
     $                WORK, NBLOCK, WORK( NBLOCK**2+1 ), NBLOCK,
     $                WORK( 2*NBLOCK**2+1 ), LWORK-2*NBLOCK**2,
     $                SWEEP_INFO )

      END DO

*
*     Call DHGEQZ to normalize the eigenvalue blocks and set the eigenvalues
*     If all the eigenvalues have been found, DHGEQZ will not do any iterations
*     and only normalize the blocks. In case of a rare convergence failure,
*     the single shift might perform better.
*
   80 CALL DHGEQZ( WANTS, WANTQ, WANTZ, N, ILO, IHI, A, LDA, B, LDB,
     $             ALPHAR, ALPHAI, BETA, Q, LDQ, Z, LDZ, WORK, LWORK,
     $             NORM_INFO )
      
      INFO = NORM_INFO

      END SUBROUTINE
