*> \brief \b ZDRSCL multiplies a vector by the reciprocal of a real scalar.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZDRSCL + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zdrscl.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zdrscl.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zdrscl.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZRSCL( N, A, X, INCX )
*
*       .. Scalar Arguments ..
*       INTEGER            INCX, N
*       COMPLEX*16         A
*       ..
*       .. Array Arguments ..
*       COMPLEX*16         X( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZRSCL multiplies an n-element complex vector x by the complex scalar
*> 1/a.  This is done without overflow or underflow as long as
*> the final result x/a does not overflow or underflow.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of components of the vector x.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is COMPLEX*16
*>          The scalar a which is used to divide each component of x.
*>          A must not be 0, or the subroutine will divide by zero.
*> \endverbatim
*>
*> \param[in,out] X
*> \verbatim
*>          X is COMPLEX*16 array, dimension
*>                         (1+(N-1)*abs(INCX))
*>          The n-element vector x.
*> \endverbatim
*>
*> \param[in] INCX
*> \verbatim
*>          INCX is INTEGER
*>          The increment between successive values of the vector SX.
*>          > 0:  SX(1) = X(1) and SX(1+(i-1)*INCX) = x(i),     1< i<= n
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
*> \ingroup complex16OTHERauxiliary
*
*  =====================================================================
      SUBROUTINE ZRSCL( N, A, X, INCX )
*
*  -- LAPACK auxiliary routine --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*
*     .. Scalar Arguments ..
      INTEGER            INCX, N
      COMPLEX*16         A
*     ..
*     .. Array Arguments ..
      COMPLEX*16         X( * )
*     ..
*
* =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
*     ..
*     .. Local Scalars ..
      DOUBLE PRECISION   SAFMAX, SAFMIN, OV, AR, AI, ABSR, ABSI, UR, UI
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH
      COMPLEX*16         ZLADIV
      EXTERNAL           DLAMCH, ZLADIV
*     ..
*     .. External Subroutines ..
      EXTERNAL           DSCAL, ZDSCAL, ZDRSCL
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS
*     ..
*     .. Executable Statements ..
*
*     Quick return if possible
*
      IF( N.LE.0 )
     $   RETURN
*
*     Get machine parameters
*
      SAFMIN = DLAMCH( 'S' )
      SAFMAX = ONE / SAFMIN
      OV   = DLAMCH( 'O' )
*
*     Initialize constants related to A.
*
      AR = DBLE( A )
      AI = DIMAG( A )
      ABSR = ABS( AR )
      ABSI = ABS( AI )
*
      IF( AI.EQ.ZERO ) THEN
*        If alpha is real, then we can use csrscl
         CALL ZDRSCL( N, AR, X, INCX )
*
      ELSE IF( AR.EQ.ZERO ) THEN
*        If alpha has a zero real part, then we follow the same rules as if
*        alpha were real.
         IF( ABSI.GT.SAFMAX ) THEN
            CALL ZDSCAL( N, SAFMIN, X, INCX )
            CALL ZSCAL( N, DCMPLX( ZERO, -SAFMAX / AI ), X, INCX )
         ELSE IF( ABSI.LT.SAFMIN ) THEN
            CALL ZSCAL( N, DCMPLX( ZERO, -SAFMIN / AI ), X, INCX )
            CALL ZDSCAL( N, SAFMAX, X, INCX )
         ELSE
            CALL ZSCAL( N, DCMPLX( ZERO, -ONE / AI ), X, INCX )
         END IF
*
      ELSE
*        The following numbers can be computed.
*        They are the inverse of the real and imaginary parts of 1/alpha.
*        Note that a and b are always different from zero.
*        NaNs are only possible if either:
*        1. alphaR or alphaI is NaN.
*        2. alphaR and alphaI are both infinite, in which case it makes sense
*        to propagate a NaN.
         UR = AR + AI * ( AI / AR )
         UI = AI + AR * ( AR / AI )
*
         IF( (ABS( UR ).LT.SAFMIN).OR.(ABS( UI ).LT.SAFMIN) ) THEN
*           This means that both alphaR and alphaI are very small.
            CALL ZSCAL( N, DCMPLX( SAFMIN / UR, -SAFMIN / UI ), X,
     $                  INCX )
            CALL ZDSCAL( N, SAFMAX, X, INCX )
         ELSE IF( (ABS( UR ).GT.SAFMAX).OR.(ABS( UI ).GT.SAFMAX) ) THEN
            IF( (ABSR.GT.OV).OR.(ABSI.GT.OV) ) THEN
*              This means that a and b are both Inf. No need for scaling.
               CALL ZSCAL( N, DCMPLX( ONE / UR, -ONE / UI ), X, INCX )
            ELSE
               CALL ZDSCAL( N, SAFMIN, X, INCX )
               IF( (ABS( UR ).GT.OV).OR.(ABS( UI ).GT.OV) ) THEN
*                 Infs were generated. We do proper scaling to avoid them.
                  IF( ABSR.GE.ABSI ) THEN
*                    ABS( UR ) <= ABS( UI )
                     UR = (SAFMIN * AR) + SAFMIN * (AI * ( AI / AR ))
                     UI = (SAFMIN * AI) + AR * ( (SAFMIN * AR) / AI )
                  ELSE
*                    ABS( UR ) > ABS( UI )
                     UR = (SAFMIN * AR) + AI * ( (SAFMIN * AI) / AR )
                     UI = (SAFMIN * AI) + SAFMIN * (AR * ( AR / AI ))
                  END IF
                  CALL ZSCAL( N, DCMPLX( ONE / UR, -ONE / UI ), X,
     $                        INCX )
               ELSE
                  CALL ZSCAL( N, DCMPLX( SAFMAX / UR, -SAFMAX / UI ),
     $                        X, INCX )
               END IF
            END IF
         ELSE
            CALL ZSCAL( N, DCMPLX( ONE / UR, -ONE / UI ), X, INCX )
         END IF
      END IF
*
      RETURN
*
*     End of ZRSCL
*
      END
