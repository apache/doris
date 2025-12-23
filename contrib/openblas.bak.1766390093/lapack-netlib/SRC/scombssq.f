*> \brief \b SCOMBSSQ adds two scaled sum of squares quantities
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*
*  Definition:
*  ===========
*
*       SUBROUTINE SCOMBSSQ( V1, V2 )
*
*       .. Array Arguments ..
*       REAL               V1( 2 ), V2( 2 )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SCOMBSSQ adds two scaled sum of squares quantities, V1 := V1 + V2.
*> That is,
*>
*>    V1_scale**2 * V1_sumsq := V1_scale**2 * V1_sumsq
*>                            + V2_scale**2 * V2_sumsq
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in,out] V1
*> \verbatim
*>          V1 is REAL array, dimension (2).
*>          The first scaled sum.
*>          V1(1) = V1_scale, V1(2) = V1_sumsq.
*> \endverbatim
*>
*> \param[in] V2
*> \verbatim
*>          V2 is REAL array, dimension (2).
*>          The second scaled sum.
*>          V2(1) = V2_scale, V2(2) = V2_sumsq.
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
*> \date November 2018
*
*> \ingroup OTHERauxiliary
*
*  =====================================================================
      SUBROUTINE SCOMBSSQ( V1, V2 )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     November 2018
*
*     .. Array Arguments ..
      REAL               V1( 2 ), V2( 2 )
*     ..
*
* =====================================================================
*
*     .. Parameters ..
      REAL               ZERO
      PARAMETER          ( ZERO = 0.0D+0 )
*     ..
*     .. Executable Statements ..
*
      IF( V1( 1 ).GE.V2( 1 ) ) THEN
         IF( V1( 1 ).NE.ZERO ) THEN
            V1( 2 ) = V1( 2 ) + ( V2( 1 ) / V1( 1 ) )**2 * V2( 2 )
         ELSE
            V1( 2 ) = V1( 2 ) + V2( 2 )
         END IF
      ELSE
         V1( 2 ) = V2( 2 ) + ( V1( 1 ) / V2( 1 ) )**2 * V1( 2 )
         V1( 1 ) = V2( 1 )
      END IF
      RETURN
*
*     End of SCOMBSSQ
*
      END
