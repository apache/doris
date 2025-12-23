*> \brief \b DLARMM
*
* Definition:
* ===========
*
*      DOUBLE PRECISION FUNCTION DLARMM( ANORM, BNORM, CNORM )
*
*     .. Scalar Arguments ..
*      DOUBLE PRECISION   ANORM, BNORM, CNORM
*     ..
*
*>  \par Purpose:
*  =======
*>
*> \verbatim
*>
*> DLARMM returns a factor s in (0, 1] such that the linear updates
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
*>          ANORM is DOUBLE PRECISION
*>          The infinity norm of A. ANORM >= 0.
*>          The number of rows of the matrix A.  M >= 0.
*> \endverbatim
*>
*> \param[in] BNORM
*> \verbatim
*>          BNORM is DOUBLE PRECISION
*>          The infinity norm of B. BNORM >= 0.
*> \endverbatim
*>
*> \param[in] CNORM
*> \verbatim
*>          CNORM is DOUBLE PRECISION
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

      DOUBLE PRECISION FUNCTION DLARMM( ANORM, BNORM, CNORM )
      IMPLICIT NONE
*     .. Scalar Arguments ..
      DOUBLE PRECISION   ANORM, BNORM, CNORM
*     .. Parameters ..
      DOUBLE PRECISION   ONE, HALF, FOUR
      PARAMETER          ( ONE = 1.0D0, HALF = 0.5D+0, FOUR = 4.0D0 )
*     ..
*     .. Local Scalars ..
       DOUBLE PRECISION   BIGNUM, SMLNUM
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH
      EXTERNAL           DLAMCH
*     ..
*     .. Executable Statements ..
*
*
*     Determine machine dependent parameters to control overflow.
*
      SMLNUM = DLAMCH( 'Safe minimum' ) / DLAMCH( 'Precision' )
      BIGNUM = ( ONE / SMLNUM ) / FOUR
*
*     Compute a scale factor.
*
      DLARMM = ONE
      IF( BNORM .LE. ONE ) THEN
         IF( ANORM * BNORM .GT. BIGNUM - CNORM ) THEN
            DLARMM = HALF
         END IF
      ELSE
         IF( ANORM .GT. (BIGNUM - CNORM) / BNORM ) THEN
            DLARMM = HALF / BNORM
         END IF
      END IF
      RETURN
*
*     ==== End of DLARMM ====
*
      END
