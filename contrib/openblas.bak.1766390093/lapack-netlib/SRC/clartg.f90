!> \brief \b CLARTG generates a plane rotation with real cosine and complex sine.
!
!  =========== DOCUMENTATION ===========
!
! Online html documentation available at
!            http://www.netlib.org/lapack/explore-html/
!
!  Definition:
!  ===========
!
!       SUBROUTINE CLARTG( F, G, C, S, R )
!
!       .. Scalar Arguments ..
!       REAL(wp)              C
!       COMPLEX(wp)           F, G, R, S
!       ..
!
!> \par Purpose:
!  =============
!>
!> \verbatim
!>
!> CLARTG generates a plane rotation so that
!>
!>    [  C         S  ] . [ F ]  =  [ R ]
!>    [ -conjg(S)  C  ]   [ G ]     [ 0 ]
!>
!> where C is real and C**2 + |S|**2 = 1.
!>
!> The mathematical formulas used for C and S are
!>
!>    sgn(x) = {  x / |x|,   x != 0
!>             {  1,         x  = 0
!>
!>    R = sgn(F) * sqrt(|F|**2 + |G|**2)
!>
!>    C = |F| / sqrt(|F|**2 + |G|**2)
!>
!>    S = sgn(F) * conjg(G) / sqrt(|F|**2 + |G|**2)
!>
!> Special conditions:
!>    If G=0, then C=1 and S=0.
!>    If F=0, then C=0 and S is chosen so that R is real.
!>
!> When F and G are real, the formulas simplify to C = F/R and
!> S = G/R, and the returned values of C, S, and R should be
!> identical to those returned by SLARTG.
!>
!> The algorithm used to compute these quantities incorporates scaling
!> to avoid overflow or underflow in computing the square root of the
!> sum of squares.
!>
!> This is the same routine CROTG fom BLAS1, except that
!> F and G are unchanged on return.
!>
!> Below, wp=>sp stands for single precision from LA_CONSTANTS module.
!> \endverbatim
!
!  Arguments:
!  ==========
!
!> \param[in] F
!> \verbatim
!>          F is COMPLEX(wp)
!>          The first component of vector to be rotated.
!> \endverbatim
!>
!> \param[in] G
!> \verbatim
!>          G is COMPLEX(wp)
!>          The second component of vector to be rotated.
!> \endverbatim
!>
!> \param[out] C
!> \verbatim
!>          C is REAL(wp)
!>          The cosine of the rotation.
!> \endverbatim
!>
!> \param[out] S
!> \verbatim
!>          S is COMPLEX(wp)
!>          The sine of the rotation.
!> \endverbatim
!>
!> \param[out] R
!> \verbatim
!>          R is COMPLEX(wp)
!>          The nonzero component of the rotated vector.
!> \endverbatim
!
!  Authors:
!  ========
!
!> \author Weslley Pereira, University of Colorado Denver, USA
!
!> \date December 2021
!
!> \ingroup OTHERauxiliary
!
!> \par Further Details:
!  =====================
!>
!> \verbatim
!>
!> Based on the algorithm from
!>
!>  Anderson E. (2017)
!>  Algorithm 978: Safe Scaling in the Level 1 BLAS
!>  ACM Trans Math Softw 44:1--28
!>  https://doi.org/10.1145/3061665
!>
!> \endverbatim
!
subroutine CLARTG( f, g, c, s, r )
   use LA_CONSTANTS, &
   only: wp=>sp, zero=>szero, one=>sone, two=>stwo, czero, &
         safmin=>ssafmin, safmax=>ssafmax
!
!  -- LAPACK auxiliary routine --
!  -- LAPACK is a software package provided by Univ. of Tennessee,    --
!  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
!     February 2021
!
!  .. Scalar Arguments ..
   real(wp)           c
   complex(wp)        f, g, r, s
!  ..
!  .. Local Scalars ..
   real(wp) :: d, f1, f2, g1, g2, h2, u, v, w, rtmin, rtmax
   complex(wp) :: fs, gs, t
!  ..
!  .. Intrinsic Functions ..
   intrinsic :: abs, aimag, conjg, max, min, real, sqrt
!  ..
!  .. Statement Functions ..
   real(wp) :: ABSSQ
!  ..
!  .. Statement Function definitions ..
   ABSSQ( t ) = real( t )**2 + aimag( t )**2
!  ..
!  .. Constants ..
   rtmin = sqrt( safmin )
!  ..
!  .. Executable Statements ..
!
   if( g == czero ) then
      c = one
      s = czero
      r = f
   else if( f == czero ) then
      c = zero
      if( real(g) == zero ) then
         r = abs(aimag(g))
         s = conjg( g ) / r
      elseif( aimag(g) == zero ) then
         r = abs(real(g))
         s = conjg( g ) / r
      else
         g1 = max( abs(real(g)), abs(aimag(g)) )
         rtmax = sqrt( safmax/2 )
         if( g1 > rtmin .and. g1 < rtmax ) then
!
!        Use unscaled algorithm
!
!           The following two lines can be replaced by `d = abs( g )`.
!           This algorithm do not use the intrinsic complex abs.
            g2 = ABSSQ( g )
            d = sqrt( g2 )
            s = conjg( g ) / d
            r = d
         else
!
!        Use scaled algorithm
!
            u = min( safmax, max( safmin, g1 ) )
            gs = g / u
!           The following two lines can be replaced by `d = abs( gs )`.
!           This algorithm do not use the intrinsic complex abs.
            g2 = ABSSQ( gs )
            d = sqrt( g2 )
            s = conjg( gs ) / d
            r = d*u
         end if
      end if
   else
      f1 = max( abs(real(f)), abs(aimag(f)) )
      g1 = max( abs(real(g)), abs(aimag(g)) )
      rtmax = sqrt( safmax/4 )
      if( f1 > rtmin .and. f1 < rtmax .and. &
          g1 > rtmin .and. g1 < rtmax ) then
!
!        Use unscaled algorithm
!
         f2 = ABSSQ( f )
         g2 = ABSSQ( g )
         h2 = f2 + g2
         ! safmin <= f2 <= h2 <= safmax 
         if( f2 >= h2 * safmin ) then
            ! safmin <= f2/h2 <= 1, and h2/f2 is finite
            c = sqrt( f2 / h2 )
            r = f / c
            rtmax = rtmax * 2
            if( f2 > rtmin .and. h2 < rtmax ) then
               ! safmin <= sqrt( f2*h2 ) <= safmax
               s = conjg( g ) * ( f / sqrt( f2*h2 ) )
            else
               s = conjg( g ) * ( r / h2 )
            end if
         else
            ! f2/h2 <= safmin may be subnormal, and h2/f2 may overflow.
            ! Moreover,
            !  safmin <= f2*f2 * safmax < f2 * h2 < h2*h2 * safmin <= safmax,
            !  sqrt(safmin) <= sqrt(f2 * h2) <= sqrt(safmax).
            ! Also,
            !  g2 >> f2, which means that h2 = g2.
            d = sqrt( f2 * h2 )
            c = f2 / d
            if( c >= safmin ) then
               r = f / c
            else
               ! f2 / sqrt(f2 * h2) < safmin, then
               !  sqrt(safmin) <= f2 * sqrt(safmax) <= h2 / sqrt(f2 * h2) <= h2 * (safmin / f2) <= h2 <= safmax
               r = f * ( h2 / d )
            end if
            s = conjg( g ) * ( f / d )
         end if
      else
!
!        Use scaled algorithm
!
         u = min( safmax, max( safmin, f1, g1 ) )
         gs = g / u
         g2 = ABSSQ( gs )
         if( f1 / u < rtmin ) then
!
!           f is not well-scaled when scaled by g1.
!           Use a different scaling for f.
!
            v = min( safmax, max( safmin, f1 ) )
            w = v / u
            fs = f / v
            f2 = ABSSQ( fs )
            h2 = f2*w**2 + g2
         else
!
!           Otherwise use the same scaling for f and g.
!
            w = one
            fs = f / u
            f2 = ABSSQ( fs )
            h2 = f2 + g2
         end if
         ! safmin <= f2 <= h2 <= safmax 
         if( f2 >= h2 * safmin ) then
            ! safmin <= f2/h2 <= 1, and h2/f2 is finite
            c = sqrt( f2 / h2 )
            r = fs / c
            rtmax = rtmax * 2
            if( f2 > rtmin .and. h2 < rtmax ) then
               ! safmin <= sqrt( f2*h2 ) <= safmax
               s = conjg( gs ) * ( fs / sqrt( f2*h2 ) )
            else
               s = conjg( gs ) * ( r / h2 )
            end if
         else
            ! f2/h2 <= safmin may be subnormal, and h2/f2 may overflow.
            ! Moreover,
            !  safmin <= f2*f2 * safmax < f2 * h2 < h2*h2 * safmin <= safmax,
            !  sqrt(safmin) <= sqrt(f2 * h2) <= sqrt(safmax).
            ! Also,
            !  g2 >> f2, which means that h2 = g2.
            d = sqrt( f2 * h2 )
            c = f2 / d
            if( c >= safmin ) then
               r = fs / c
            else
               ! f2 / sqrt(f2 * h2) < safmin, then
               !  sqrt(safmin) <= f2 * sqrt(safmax) <= h2 / sqrt(f2 * h2) <= h2 * (safmin / f2) <= h2 <= safmax
               r = fs * ( h2 / d )
            end if
            s = conjg( gs ) * ( fs / d )
         end if
         ! Rescale c and r
         c = c * w
         r = r * u
      end if
   end if
   return
end subroutine
