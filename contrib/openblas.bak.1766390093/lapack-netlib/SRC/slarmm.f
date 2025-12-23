*> \brief \b SLARMM
*
* Definition:
* ===========
*
*      REAL FUNCTION SLARMM( ANORM, BNORM, CNORM )
*
*     .. Scalar Arguments ..
*      REAL               ANORM, BNORM, CNORM
*     ..
*
*>  \par Purpose:
*  =======
*>
*> \verbatim
*>
*> SLARMM returns a factor s in (0, 1] such that the linear updates
*>
*>    (s * C) - A * (s * B)  and  (s * C) - (s * A) * B
*>
*> cannot overflow, where A, B, and C are matrices of conforming
*> dimensions.
*>
*> This is an auxiliary routine so there is no argument checking.
*> \endverbatim
*
*  Arguments:
*  =========
*
*> \param[in] ANORM
*> \verbatim
*>          ANORM is REAL
*>          The infinity norm of A. ANORM >= 0.
*>          The number of rows of the matrix A.  M >= 0.
*> \endverbatim
*>
*> \param[in] BNORM
*> \verbatim
*>          BNORM is REAL
*>          The infinity norm of B. BNORM >= 0.
*> \endverbatim
*>
*> \param[in] CNORM
*> \verbatim
*>          CNORM is REAL
*>          The infinity norm of C. CNORM >= 0.
*> \endverbatim
*>
*>
*  =====================================================================
*>  References:
*>    C. C. Kjelgaard Mikkelsen and L. Karlsson, Blocked Algorithms for
*>    Robust Solution of Triangular Linear Systems. In: International
*>    Conference on Parallel Processing and Applied Mathematics, pages
*>    68--78. Springer, 2017.
*>
*> \ingroup OTHERauxiliary
*  =====================================================================

      REAL FUNCTION SLARMM( ANORM, BNORM, CNORM )
      IMPLICIT NONE
*     .. Scalar Arguments ..
      REAL               ANORM, BNORM, CNORM
*     .. Parameters ..
      REAL               ONE, HALF, FOUR
      PARAMETER          ( ONE = 1.0E0, HALF = 0.5E+0, FOUR = 4.0E+0 )
*     ..
*     .. Local Scalars ..
      REAL               BIGNUM, SMLNUM
*     ..
*     .. External Functions ..
      REAL               SLAMCH
      EXTERNAL           SLAMCH
*     ..
*     .. Executable Statements ..
*
*
*     Determine machine dependent parameters to control overflow.
*
      SMLNUM = SLAMCH( 'Safe minimum' ) / SLAMCH( 'Precision' )
      BIGNUM = ( ONE / SMLNUM ) / FOUR
*
*     Compute a scale factor.
*
      SLARMM = ONE
      IF( BNORM .LE. ONE ) THEN
         IF( ANORM * BNORM .GT. BIGNUM - CNORM ) THEN
            SLARMM = HALF
         END IF
      ELSE
         IF( ANORM .GT. (BIGNUM - CNORM) / BNORM ) THEN
            SLARMM = HALF / BNORM
         END IF
      END IF
      RETURN
*
*     ==== End of SLARMM ====
*
      END
