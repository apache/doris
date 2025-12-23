*> \brief \b DLAQR5 performs a single small-bulge multi-shift QR sweep.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DLAQR5 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dlaqr5.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dlaqr5.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dlaqr5.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DLAQR5( WANTT, WANTZ, KACC22, N, KTOP, KBOT, NSHFTS,
*                          SR, SI, H, LDH, ILOZ, IHIZ, Z, LDZ, V, LDV, U,
*                          LDU, NV, WV, LDWV, NH, WH, LDWH )
*
*       .. Scalar Arguments ..
*       INTEGER            IHIZ, ILOZ, KACC22, KBOT, KTOP, LDH, LDU, LDV,
*      $                   LDWH, LDWV, LDZ, N, NH, NSHFTS, NV
*       LOGICAL            WANTT, WANTZ
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   H( LDH, * ), SI( * ), SR( * ), U( LDU, * ),
*      $                   V( LDV, * ), WH( LDWH, * ), WV( LDWV, * ),
*      $                   Z( LDZ, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    DLAQR5, called by DLAQR0, performs a
*>    single small-bulge multi-shift QR sweep.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] WANTT
*> \verbatim
*>          WANTT is LOGICAL
*>             WANTT = .true. if the quasi-triangular Schur factor
*>             is being computed.  WANTT is set to .false. otherwise.
*> \endverbatim
*>
*> \param[in] WANTZ
*> \verbatim
*>          WANTZ is LOGICAL
*>             WANTZ = .true. if the orthogonal Schur factor is being
*>             computed.  WANTZ is set to .false. otherwise.
*> \endverbatim
*>
*> \param[in] KACC22
*> \verbatim
*>          KACC22 is INTEGER with value 0, 1, or 2.
*>             Specifies the computation mode of far-from-diagonal
*>             orthogonal updates.
*>        = 0: DLAQR5 does not accumulate reflections and does not
*>             use matrix-matrix multiply to update far-from-diagonal
*>             matrix entries.
*>        = 1: DLAQR5 accumulates reflections and uses matrix-matrix
*>             multiply to update the far-from-diagonal matrix entries.
*>        = 2: Same as KACC22 = 1. This option used to enable exploiting
*>             the 2-by-2 structure during matrix multiplications, but
*>             this is no longer supported.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>             N is the order of the Hessenberg matrix H upon which this
*>             subroutine operates.
*> \endverbatim
*>
*> \param[in] KTOP
*> \verbatim
*>          KTOP is INTEGER
*> \endverbatim
*>
*> \param[in] KBOT
*> \verbatim
*>          KBOT is INTEGER
*>             These are the first and last rows and columns of an
*>             isolated diagonal block upon which the QR sweep is to be
*>             applied. It is assumed without a check that
*>                       either KTOP = 1  or   H(KTOP,KTOP-1) = 0
*>             and
*>                       either KBOT = N  or   H(KBOT+1,KBOT) = 0.
*> \endverbatim
*>
*> \param[in] NSHFTS
*> \verbatim
*>          NSHFTS is INTEGER
*>             NSHFTS gives the number of simultaneous shifts.  NSHFTS
*>             must be positive and even.
*> \endverbatim
*>
*> \param[in,out] SR
*> \verbatim
*>          SR is DOUBLE PRECISION array, dimension (NSHFTS)
*> \endverbatim
*>
*> \param[in,out] SI
*> \verbatim
*>          SI is DOUBLE PRECISION array, dimension (NSHFTS)
*>             SR contains the real parts and SI contains the imaginary
*>             parts of the NSHFTS shifts of origin that define the
*>             multi-shift QR sweep.  On output SR and SI may be
*>             reordered.
*> \endverbatim
*>
*> \param[in,out] H
*> \verbatim
*>          H is DOUBLE PRECISION array, dimension (LDH,N)
*>             On input H contains a Hessenberg matrix.  On output a
*>             multi-shift QR sweep with shifts SR(J)+i*SI(J) is applied
*>             to the isolated diagonal block in rows and columns KTOP
*>             through KBOT.
*> \endverbatim
*>
*> \param[in] LDH
*> \verbatim
*>          LDH is INTEGER
*>             LDH is the leading dimension of H just as declared in the
*>             calling procedure.  LDH >= MAX(1,N).
*> \endverbatim
*>
*> \param[in] ILOZ
*> \verbatim
*>          ILOZ is INTEGER
*> \endverbatim
*>
*> \param[in] IHIZ
*> \verbatim
*>          IHIZ is INTEGER
*>             Specify the rows of Z to which transformations must be
*>             applied if WANTZ is .TRUE.. 1 <= ILOZ <= IHIZ <= N
*> \endverbatim
*>
*> \param[in,out] Z
*> \verbatim
*>          Z is DOUBLE PRECISION array, dimension (LDZ,IHIZ)
*>             If WANTZ = .TRUE., then the QR Sweep orthogonal
*>             similarity transformation is accumulated into
*>             Z(ILOZ:IHIZ,ILOZ:IHIZ) from the right.
*>             If WANTZ = .FALSE., then Z is unreferenced.
*> \endverbatim
*>
*> \param[in] LDZ
*> \verbatim
*>          LDZ is INTEGER
*>             LDA is the leading dimension of Z just as declared in
*>             the calling procedure. LDZ >= N.
*> \endverbatim
*>
*> \param[out] V
*> \verbatim
*>          V is DOUBLE PRECISION array, dimension (LDV,NSHFTS/2)
*> \endverbatim
*>
*> \param[in] LDV
*> \verbatim
*>          LDV is INTEGER
*>             LDV is the leading dimension of V as declared in the
*>             calling procedure.  LDV >= 3.
*> \endverbatim
*>
*> \param[out] U
*> \verbatim
*>          U is DOUBLE PRECISION array, dimension (LDU,2*NSHFTS)
*> \endverbatim
*>
*> \param[in] LDU
*> \verbatim
*>          LDU is INTEGER
*>             LDU is the leading dimension of U just as declared in the
*>             in the calling subroutine.  LDU >= 2*NSHFTS.
*> \endverbatim
*>
*> \param[in] NV
*> \verbatim
*>          NV is INTEGER
*>             NV is the number of rows in WV agailable for workspace.
*>             NV >= 1.
*> \endverbatim
*>
*> \param[out] WV
*> \verbatim
*>          WV is DOUBLE PRECISION array, dimension (LDWV,2*NSHFTS)
*> \endverbatim
*>
*> \param[in] LDWV
*> \verbatim
*>          LDWV is INTEGER
*>             LDWV is the leading dimension of WV as declared in the
*>             in the calling subroutine.  LDWV >= NV.
*> \endverbatim
*
*> \param[in] NH
*> \verbatim
*>          NH is INTEGER
*>             NH is the number of columns in array WH available for
*>             workspace. NH >= 1.
*> \endverbatim
*>
*> \param[out] WH
*> \verbatim
*>          WH is DOUBLE PRECISION array, dimension (LDWH,NH)
*> \endverbatim
*>
*> \param[in] LDWH
*> \verbatim
*>          LDWH is INTEGER
*>             Leading dimension of WH just as declared in the
*>             calling procedure.  LDWH >= 2*NSHFTS.
*> \endverbatim
*>
*  Authors:
*  ========
*
*> \author Univ. of Tennessee
*> \author Univ. of California Berkeley
*> \author Univ. of Colorado Denver
*> \author NAG Ltd.
*
*> \ingroup doubleOTHERauxiliary
*
*> \par Contributors:
*  ==================
*>
*>       Karen Braman and Ralph Byers, Department of Mathematics,
*>       University of Kansas, USA
*>
*>       Lars Karlsson, Daniel Kressner, and Bruno Lang
*>
*>       Thijs Steel, Department of Computer science,
*>       KU Leuven, Belgium
*
*> \par References:
*  ================
*>
*>       K. Braman, R. Byers and R. Mathias, The Multi-Shift QR
*>       Algorithm Part I: Maintaining Well Focused Shifts, and Level 3
*>       Performance, SIAM Journal of Matrix Analysis, volume 23, pages
*>       929--947, 2002.
*>
*>       Lars Karlsson, Daniel Kressner, and Bruno Lang, Optimally packed
*>       chains of bulges in multishift QR algorithms.
*>       ACM Trans. Math. Softw. 40, 2, Article 12 (February 2014).
*>
*  =====================================================================
      SUBROUTINE DLAQR5( WANTT, WANTZ, KACC22, N, KTOP, KBOT, NSHFTS,
     $                   SR, SI, H, LDH, ILOZ, IHIZ, Z, LDZ, V, LDV, U,
     $                   LDU, NV, WV, LDWV, NH, WH, LDWH )
      IMPLICIT NONE
*
*  -- LAPACK auxiliary routine --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*
*     .. Scalar Arguments ..
      INTEGER            IHIZ, ILOZ, KACC22, KBOT, KTOP, LDH, LDU, LDV,
     $                   LDWH, LDWV, LDZ, N, NH, NSHFTS, NV
      LOGICAL            WANTT, WANTZ
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   H( LDH, * ), SI( * ), SR( * ), U( LDU, * ),
     $                   V( LDV, * ), WH( LDWH, * ), WV( LDWV, * ),
     $                   Z( LDZ, * )
*     ..
*
*  ================================================================
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0d0, ONE = 1.0d0 )
*     ..
*     .. Local Scalars ..
      DOUBLE PRECISION   ALPHA, BETA, H11, H12, H21, H22, REFSUM,
     $                   SAFMAX, SAFMIN, SCL, SMLNUM, SWAP, T1, T2,
     $                   T3, TST1, TST2, ULP
      INTEGER            I, I2, I4, INCOL, J, JBOT, JCOL, JLEN,
     $                   JROW, JTOP, K, K1, KDU, KMS, KRCOL,
     $                   M, M22, MBOT, MTOP, NBMPS, NDCOL,
     $                   NS, NU
      LOGICAL            ACCUM, BMP22
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH
      EXTERNAL           DLAMCH
*     ..
*     .. Intrinsic Functions ..
*
      INTRINSIC          ABS, DBLE, MAX, MIN, MOD
*     ..
*     .. Local Arrays ..
      DOUBLE PRECISION   VT( 3 )
*     ..
*     .. External Subroutines ..
      EXTERNAL           DGEMM, DLABAD, DLACPY, DLAQR1, DLARFG, DLASET,
     $                   DTRMM
*     ..
*     .. Executable Statements ..
*
*     ==== If there are no shifts, then there is nothing to do. ====
*
      IF( NSHFTS.LT.2 )
     $   RETURN
*
*     ==== If the active block is empty or 1-by-1, then there
*     .    is nothing to do. ====
*
      IF( KTOP.GE.KBOT )
     $   RETURN
*
*     ==== Shuffle shifts into pairs of real shifts and pairs
*     .    of complex conjugate shifts assuming complex
*     .    conjugate shifts are already adjacent to one
*     .    another. ====
*
      DO 10 I = 1, NSHFTS - 2, 2
         IF( SI( I ).NE.-SI( I+1 ) ) THEN
*
            SWAP = SR( I )
            SR( I ) = SR( I+1 )
            SR( I+1 ) = SR( I+2 )
            SR( I+2 ) = SWAP
*
            SWAP = SI( I )
            SI( I ) = SI( I+1 )
            SI( I+1 ) = SI( I+2 )
            SI( I+2 ) = SWAP
         END IF
   10 CONTINUE
*
*     ==== NSHFTS is supposed to be even, but if it is odd,
*     .    then simply reduce it by one.  The shuffle above
*     .    ensures that the dropped shift is real and that
*     .    the remaining shifts are paired. ====
*
      NS = NSHFTS - MOD( NSHFTS, 2 )
*
*     ==== Machine constants for deflation ====
*
      SAFMIN = DLAMCH( 'SAFE MINIMUM' )
      SAFMAX = ONE / SAFMIN
      CALL DLABAD( SAFMIN, SAFMAX )
      ULP = DLAMCH( 'PRECISION' )
      SMLNUM = SAFMIN*( DBLE( N ) / ULP )
*
*     ==== Use accumulated reflections to update far-from-diagonal
*     .    entries ? ====
*
      ACCUM = ( KACC22.EQ.1 ) .OR. ( KACC22.EQ.2 )
*
*     ==== clear trash ====
*
      IF( KTOP+2.LE.KBOT )
     $   H( KTOP+2, KTOP ) = ZERO
*
*     ==== NBMPS = number of 2-shift bulges in the chain ====
*
      NBMPS = NS / 2
*
*     ==== KDU = width of slab ====
*
      KDU = 4*NBMPS
*
*     ==== Create and chase chains of NBMPS bulges ====
*
      DO 180 INCOL = KTOP - 2*NBMPS + 1, KBOT - 2, 2*NBMPS
*
*        JTOP = Index from which updates from the right start.
*
         IF( ACCUM ) THEN
            JTOP = MAX( KTOP, INCOL )
         ELSE IF( WANTT ) THEN
            JTOP = 1
         ELSE
            JTOP = KTOP
         END IF
*
         NDCOL = INCOL + KDU
         IF( ACCUM )
     $      CALL DLASET( 'ALL', KDU, KDU, ZERO, ONE, U, LDU )
*
*        ==== Near-the-diagonal bulge chase.  The following loop
*        .    performs the near-the-diagonal part of a small bulge
*        .    multi-shift QR sweep.  Each 4*NBMPS column diagonal
*        .    chunk extends from column INCOL to column NDCOL
*        .    (including both column INCOL and column NDCOL). The
*        .    following loop chases a 2*NBMPS+1 column long chain of
*        .    NBMPS bulges 2*NBMPS columns to the right.  (INCOL
*        .    may be less than KTOP and and NDCOL may be greater than
*        .    KBOT indicating phantom columns from which to chase
*        .    bulges before they are actually introduced or to which
*        .    to chase bulges beyond column KBOT.)  ====
*
         DO 145 KRCOL = INCOL, MIN( INCOL+2*NBMPS-1, KBOT-2 )
*
*           ==== Bulges number MTOP to MBOT are active double implicit
*           .    shift bulges.  There may or may not also be small
*           .    2-by-2 bulge, if there is room.  The inactive bulges
*           .    (if any) must wait until the active bulges have moved
*           .    down the diagonal to make room.  The phantom matrix
*           .    paradigm described above helps keep track.  ====
*
            MTOP = MAX( 1, ( KTOP-KRCOL ) / 2+1 )
            MBOT = MIN( NBMPS, ( KBOT-KRCOL-1 ) / 2 )
            M22 = MBOT + 1
            BMP22 = ( MBOT.LT.NBMPS ) .AND. ( KRCOL+2*( M22-1 ) ).EQ.
     $              ( KBOT-2 )
*
*           ==== Generate reflections to chase the chain right
*           .    one column.  (The minimum value of K is KTOP-1.) ====
*
            IF ( BMP22 ) THEN
*
*              ==== Special case: 2-by-2 reflection at bottom treated
*              .    separately ====
*
               K = KRCOL + 2*( M22-1 )
               IF( K.EQ.KTOP-1 ) THEN
                  CALL DLAQR1( 2, H( K+1, K+1 ), LDH, SR( 2*M22-1 ),
     $                         SI( 2*M22-1 ), SR( 2*M22 ), SI( 2*M22 ),
     $                         V( 1, M22 ) )
                  BETA = V( 1, M22 )
                  CALL DLARFG( 2, BETA, V( 2, M22 ), 1, V( 1, M22 ) )
               ELSE
                  BETA = H( K+1, K )
                  V( 2, M22 ) = H( K+2, K )
                  CALL DLARFG( 2, BETA, V( 2, M22 ), 1, V( 1, M22 ) )
                  H( K+1, K ) = BETA
                  H( K+2, K ) = ZERO
               END IF

*
*              ==== Perform update from right within 
*              .    computational window. ====
*
               T1 = V( 1, M22 )
               T2 = T1*V( 2, M22 )
               DO 30 J = JTOP, MIN( KBOT, K+3 )
                  REFSUM = H( J, K+1 ) + V( 2, M22 )*H( J, K+2 )
                  H( J, K+1 ) = H( J, K+1 ) - REFSUM*T1
                  H( J, K+2 ) = H( J, K+2 ) - REFSUM*T2
   30          CONTINUE
*
*              ==== Perform update from left within 
*              .    computational window. ====
*
               IF( ACCUM ) THEN
                  JBOT = MIN( NDCOL, KBOT )
               ELSE IF( WANTT ) THEN
                  JBOT = N
               ELSE
                  JBOT = KBOT
               END IF
               T1 = V( 1, M22 )
               T2 = T1*V( 2, M22 )
               DO 40 J = K+1, JBOT
                  REFSUM = H( K+1, J ) + V( 2, M22 )*H( K+2, J )
                  H( K+1, J ) = H( K+1, J ) - REFSUM*T1
                  H( K+2, J ) = H( K+2, J ) - REFSUM*T2
   40          CONTINUE
*
*              ==== The following convergence test requires that
*              .    the tradition small-compared-to-nearby-diagonals
*              .    criterion and the Ahues & Tisseur (LAWN 122, 1997)
*              .    criteria both be satisfied.  The latter improves
*              .    accuracy in some examples. Falling back on an
*              .    alternate convergence criterion when TST1 or TST2
*              .    is zero (as done here) is traditional but probably
*              .    unnecessary. ====
*
               IF( K.GE.KTOP ) THEN
                  IF( H( K+1, K ).NE.ZERO ) THEN
                     TST1 = ABS( H( K, K ) ) + ABS( H( K+1, K+1 ) )
                     IF( TST1.EQ.ZERO ) THEN
                        IF( K.GE.KTOP+1 )
     $                     TST1 = TST1 + ABS( H( K, K-1 ) )
                        IF( K.GE.KTOP+2 )
     $                     TST1 = TST1 + ABS( H( K, K-2 ) )
                        IF( K.GE.KTOP+3 )
     $                     TST1 = TST1 + ABS( H( K, K-3 ) )
                        IF( K.LE.KBOT-2 )
     $                     TST1 = TST1 + ABS( H( K+2, K+1 ) )
                        IF( K.LE.KBOT-3 )
     $                     TST1 = TST1 + ABS( H( K+3, K+1 ) )
                        IF( K.LE.KBOT-4 )
     $                     TST1 = TST1 + ABS( H( K+4, K+1 ) )
                     END IF
                     IF( ABS( H( K+1, K ) )
     $                   .LE.MAX( SMLNUM, ULP*TST1 ) ) THEN
                        H12 = MAX( ABS( H( K+1, K ) ),
     $                             ABS( H( K, K+1 ) ) )
                        H21 = MIN( ABS( H( K+1, K ) ),
     $                             ABS( H( K, K+1 ) ) )
                        H11 = MAX( ABS( H( K+1, K+1 ) ),
     $                             ABS( H( K, K )-H( K+1, K+1 ) ) )
                        H22 = MIN( ABS( H( K+1, K+1 ) ),
     $                        ABS( H( K, K )-H( K+1, K+1 ) ) )
                        SCL = H11 + H12
                        TST2 = H22*( H11 / SCL )
*
                        IF( TST2.EQ.ZERO .OR. H21*( H12 / SCL ).LE.
     $                      MAX( SMLNUM, ULP*TST2 ) ) THEN
                           H( K+1, K ) = ZERO
                        END IF
                     END IF
                  END IF
               END IF
*
*              ==== Accumulate orthogonal transformations. ====
*
               IF( ACCUM ) THEN
                  KMS = K - INCOL
                  T1 = V( 1, M22 )
                  T2 = T1*V( 2, M22 )
                  DO 50 J = MAX( 1, KTOP-INCOL ), KDU
                     REFSUM = U( J, KMS+1 ) + V( 2, M22 )*U( J, KMS+2 )
                     U( J, KMS+1 ) = U( J, KMS+1 ) - REFSUM*T1
                     U( J, KMS+2 ) = U( J, KMS+2 ) - REFSUM*T2
  50                 CONTINUE
               ELSE IF( WANTZ ) THEN
                  T1 = V( 1, M22 )
                  T2 = T1*V( 2, M22 )
                  DO 60 J = ILOZ, IHIZ
                     REFSUM = Z( J, K+1 )+V( 2, M22 )*Z( J, K+2 )
                     Z( J, K+1 ) = Z( J, K+1 ) - REFSUM*T1
                     Z( J, K+2 ) = Z( J, K+2 ) - REFSUM*T2
  60              CONTINUE
               END IF
            END IF
*
*           ==== Normal case: Chain of 3-by-3 reflections ====
*
            DO 80 M = MBOT, MTOP, -1
               K = KRCOL + 2*( M-1 )
               IF( K.EQ.KTOP-1 ) THEN
                  CALL DLAQR1( 3, H( KTOP, KTOP ), LDH, SR( 2*M-1 ),
     $                         SI( 2*M-1 ), SR( 2*M ), SI( 2*M ),
     $                         V( 1, M ) )
                  ALPHA = V( 1, M )
                  CALL DLARFG( 3, ALPHA, V( 2, M ), 1, V( 1, M ) )
               ELSE
*
*                 ==== Perform delayed transformation of row below
*                 .    Mth bulge. Exploit fact that first two elements
*                 .    of row are actually zero. ====
*
                  T1 = V( 1, M )
                  T2 = T1*V( 2, M )
                  T3 = T1*V( 3, M )
                  REFSUM = V( 3, M )*H( K+3, K+2 )
                  H( K+3, K   ) = -REFSUM*T1
                  H( K+3, K+1 ) = -REFSUM*T2
                  H( K+3, K+2 ) = H( K+3, K+2 ) - REFSUM*T3
*
*                 ==== Calculate reflection to move
*                 .    Mth bulge one step. ====
*
                  BETA      = H( K+1, K )
                  V( 2, M ) = H( K+2, K )
                  V( 3, M ) = H( K+3, K )
                  CALL DLARFG( 3, BETA, V( 2, M ), 1, V( 1, M ) )
*
*                 ==== A Bulge may collapse because of vigilant
*                 .    deflation or destructive underflow.  In the
*                 .    underflow case, try the two-small-subdiagonals
*                 .    trick to try to reinflate the bulge.  ====
*
                  IF( H( K+3, K ).NE.ZERO .OR. H( K+3, K+1 ).NE.
     $                ZERO .OR. H( K+3, K+2 ).EQ.ZERO ) THEN
*
*                    ==== Typical case: not collapsed (yet). ====
*
                     H( K+1, K ) = BETA
                     H( K+2, K ) = ZERO
                     H( K+3, K ) = ZERO
                  ELSE
*
*                    ==== Atypical case: collapsed.  Attempt to
*                    .    reintroduce ignoring H(K+1,K) and H(K+2,K).
*                    .    If the fill resulting from the new
*                    .    reflector is too large, then abandon it.
*                    .    Otherwise, use the new one. ====
*
                     CALL DLAQR1( 3, H( K+1, K+1 ), LDH, SR( 2*M-1 ),
     $                            SI( 2*M-1 ), SR( 2*M ), SI( 2*M ),
     $                            VT )
                     ALPHA = VT( 1 )
                     CALL DLARFG( 3, ALPHA, VT( 2 ), 1, VT( 1 ) )
                     T1 = VT( 1 )
                     T2 = T1*VT( 2 )
                     T3 = T1*VT( 3 )
                     REFSUM = H( K+1, K ) + VT( 2 )*H( K+2, K )
*
                     IF( ABS( H( K+2, K )-REFSUM*T2 )+
     $                   ABS( REFSUM*T3 ).GT.ULP*
     $                   ( ABS( H( K, K ) )+ABS( H( K+1,
     $                   K+1 ) )+ABS( H( K+2, K+2 ) ) ) ) THEN
*
*                       ==== Starting a new bulge here would
*                       .    create non-negligible fill.  Use
*                       .    the old one with trepidation. ====
*
                        H( K+1, K ) = BETA
                        H( K+2, K ) = ZERO
                        H( K+3, K ) = ZERO
                     ELSE
*
*                       ==== Starting a new bulge here would
*                       .    create only negligible fill.
*                       .    Replace the old reflector with
*                       .    the new one. ====
*
                        H( K+1, K ) = H( K+1, K ) - REFSUM*T1
                        H( K+2, K ) = ZERO
                        H( K+3, K ) = ZERO
                        V( 1, M ) = VT( 1 )
                        V( 2, M ) = VT( 2 )
                        V( 3, M ) = VT( 3 )
                     END IF
                  END IF
               END IF
*
*              ====  Apply reflection from the right and
*              .     the first column of update from the left.
*              .     These updates are required for the vigilant
*              .     deflation check. We still delay most of the
*              .     updates from the left for efficiency. ====      
*
               T1 = V( 1, M )
               T2 = T1*V( 2, M )
               T3 = T1*V( 3, M )
               DO 70 J = JTOP, MIN( KBOT, K+3 )
                  REFSUM = H( J, K+1 ) + V( 2, M )*H( J, K+2 )
     $                     + V( 3, M )*H( J, K+3 )
                  H( J, K+1 ) = H( J, K+1 ) - REFSUM*T1
                  H( J, K+2 ) = H( J, K+2 ) - REFSUM*T2
                  H( J, K+3 ) = H( J, K+3 ) - REFSUM*T3
   70          CONTINUE
*
*              ==== Perform update from left for subsequent
*              .    column. ====
*
               REFSUM = H( K+1, K+1 ) + V( 2, M )*H( K+2, K+1 )
     $                  + V( 3, M )*H( K+3, K+1 )
               H( K+1, K+1 ) = H( K+1, K+1 ) - REFSUM*T1
               H( K+2, K+1 ) = H( K+2, K+1 ) - REFSUM*T2
               H( K+3, K+1 ) = H( K+3, K+1 ) - REFSUM*T3
*
*              ==== The following convergence test requires that
*              .    the tradition small-compared-to-nearby-diagonals
*              .    criterion and the Ahues & Tisseur (LAWN 122, 1997)
*              .    criteria both be satisfied.  The latter improves
*              .    accuracy in some examples. Falling back on an
*              .    alternate convergence criterion when TST1 or TST2
*              .    is zero (as done here) is traditional but probably
*              .    unnecessary. ====
*
               IF( K.LT.KTOP)
     $              CYCLE
               IF( H( K+1, K ).NE.ZERO ) THEN
                  TST1 = ABS( H( K, K ) ) + ABS( H( K+1, K+1 ) )
                  IF( TST1.EQ.ZERO ) THEN
                     IF( K.GE.KTOP+1 )
     $                  TST1 = TST1 + ABS( H( K, K-1 ) )
                     IF( K.GE.KTOP+2 )
     $                  TST1 = TST1 + ABS( H( K, K-2 ) )
                     IF( K.GE.KTOP+3 )
     $                  TST1 = TST1 + ABS( H( K, K-3 ) )
                     IF( K.LE.KBOT-2 )
     $                  TST1 = TST1 + ABS( H( K+2, K+1 ) )
                     IF( K.LE.KBOT-3 )
     $                  TST1 = TST1 + ABS( H( K+3, K+1 ) )
                     IF( K.LE.KBOT-4 )
     $                  TST1 = TST1 + ABS( H( K+4, K+1 ) )
                  END IF
                  IF( ABS( H( K+1, K ) ).LE.MAX( SMLNUM, ULP*TST1 ) )
     $                 THEN
                     H12 = MAX( ABS( H( K+1, K ) ), ABS( H( K, K+1 ) ) )
                     H21 = MIN( ABS( H( K+1, K ) ), ABS( H( K, K+1 ) ) )
                     H11 = MAX( ABS( H( K+1, K+1 ) ),
     $                     ABS( H( K, K )-H( K+1, K+1 ) ) )
                     H22 = MIN( ABS( H( K+1, K+1 ) ),
     $                     ABS( H( K, K )-H( K+1, K+1 ) ) )
                     SCL = H11 + H12
                     TST2 = H22*( H11 / SCL )
*
                     IF( TST2.EQ.ZERO .OR. H21*( H12 / SCL ).LE.
     $                   MAX( SMLNUM, ULP*TST2 ) ) THEN
                        H( K+1, K ) = ZERO
                     END IF
                  END IF
               END IF
   80       CONTINUE
*
*           ==== Multiply H by reflections from the left ====
*
            IF( ACCUM ) THEN
               JBOT = MIN( NDCOL, KBOT )
            ELSE IF( WANTT ) THEN
               JBOT = N
            ELSE
               JBOT = KBOT
            END IF
*
            DO 100 M = MBOT, MTOP, -1
               K = KRCOL + 2*( M-1 )
               T1 = V( 1, M )
               T2 = T1*V( 2, M )
               T3 = T1*V( 3, M )
               DO 90 J = MAX( KTOP, KRCOL + 2*M ), JBOT
                  REFSUM = H( K+1, J ) + V( 2, M )*H( K+2, J )
     $                     + V( 3, M )*H( K+3, J )
                  H( K+1, J ) = H( K+1, J ) - REFSUM*T1
                  H( K+2, J ) = H( K+2, J ) - REFSUM*T2
                  H( K+3, J ) = H( K+3, J ) - REFSUM*T3
   90          CONTINUE
  100       CONTINUE
*
*           ==== Accumulate orthogonal transformations. ====
*
            IF( ACCUM ) THEN
*
*              ==== Accumulate U. (If needed, update Z later
*              .    with an efficient matrix-matrix
*              .    multiply.) ====
*
               DO 120 M = MBOT, MTOP, -1
                  K = KRCOL + 2*( M-1 )
                  KMS = K - INCOL
                  I2 = MAX( 1, KTOP-INCOL )
                  I2 = MAX( I2, KMS-(KRCOL-INCOL)+1 )
                  I4 = MIN( KDU, KRCOL + 2*( MBOT-1 ) - INCOL + 5 )
                  T1 = V( 1, M )
                  T2 = T1*V( 2, M )
                  T3 = T1*V( 3, M )
                  DO 110 J = I2, I4
                     REFSUM = U( J, KMS+1 ) + V( 2, M )*U( J, KMS+2 )
     $                        + V( 3, M )*U( J, KMS+3 )
                     U( J, KMS+1 ) = U( J, KMS+1 ) - REFSUM*T1
                     U( J, KMS+2 ) = U( J, KMS+2 ) - REFSUM*T2
                     U( J, KMS+3 ) = U( J, KMS+3 ) - REFSUM*T3
  110             CONTINUE
  120          CONTINUE
            ELSE IF( WANTZ ) THEN
*
*              ==== U is not accumulated, so update Z
*              .    now by multiplying by reflections
*              .    from the right. ====
*
               DO 140 M = MBOT, MTOP, -1
                  K = KRCOL + 2*( M-1 )
                  T1 = V( 1, M )
                  T2 = T1*V( 2, M )
                  T3 = T1*V( 3, M )
                  DO 130 J = ILOZ, IHIZ
                     REFSUM = Z( J, K+1 ) + V( 2, M )*Z( J, K+2 )
     $                        + V( 3, M )*Z( J, K+3 )
                     Z( J, K+1 ) = Z( J, K+1 ) - REFSUM*T1
                     Z( J, K+2 ) = Z( J, K+2 ) - REFSUM*T2
                     Z( J, K+3 ) = Z( J, K+3 ) - REFSUM*T3
  130             CONTINUE
  140          CONTINUE
            END IF
*
*           ==== End of near-the-diagonal bulge chase. ====
*
  145    CONTINUE
*
*        ==== Use U (if accumulated) to update far-from-diagonal
*        .    entries in H.  If required, use U to update Z as
*        .    well. ====
*
         IF( ACCUM ) THEN
            IF( WANTT ) THEN
               JTOP = 1
               JBOT = N
            ELSE
               JTOP = KTOP
               JBOT = KBOT
            END IF
            K1 = MAX( 1, KTOP-INCOL )
            NU = ( KDU-MAX( 0, NDCOL-KBOT ) ) - K1 + 1
*
*           ==== Horizontal Multiply ====
*
            DO 150 JCOL = MIN( NDCOL, KBOT ) + 1, JBOT, NH
               JLEN = MIN( NH, JBOT-JCOL+1 )
               CALL DGEMM( 'C', 'N', NU, JLEN, NU, ONE, U( K1, K1 ),
     $                        LDU, H( INCOL+K1, JCOL ), LDH, ZERO, WH,
     $                        LDWH )
               CALL DLACPY( 'ALL', NU, JLEN, WH, LDWH,
     $                         H( INCOL+K1, JCOL ), LDH )
  150       CONTINUE
*
*           ==== Vertical multiply ====
*
            DO 160 JROW = JTOP, MAX( KTOP, INCOL ) - 1, NV
               JLEN = MIN( NV, MAX( KTOP, INCOL )-JROW )
               CALL DGEMM( 'N', 'N', JLEN, NU, NU, ONE,
     $                     H( JROW, INCOL+K1 ), LDH, U( K1, K1 ),
     $                     LDU, ZERO, WV, LDWV )
               CALL DLACPY( 'ALL', JLEN, NU, WV, LDWV,
     $                      H( JROW, INCOL+K1 ), LDH )
  160       CONTINUE
*
*           ==== Z multiply (also vertical) ====
*
            IF( WANTZ ) THEN
               DO 170 JROW = ILOZ, IHIZ, NV
                  JLEN = MIN( NV, IHIZ-JROW+1 )
                  CALL DGEMM( 'N', 'N', JLEN, NU, NU, ONE,
     $                        Z( JROW, INCOL+K1 ), LDZ, U( K1, K1 ),
     $                        LDU, ZERO, WV, LDWV )
                  CALL DLACPY( 'ALL', JLEN, NU, WV, LDWV,
     $                         Z( JROW, INCOL+K1 ), LDZ )
  170          CONTINUE
            END IF
         END IF
  180 CONTINUE
*
*     ==== End of DLAQR5 ====
*
      END
