*> \brief \b DROUNDUP_LWORK
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*      DOUBLE PRECISION FUNCTION DROUNDUP_LWORK( LWORK )
*
*     .. Scalar Arguments ..
*      INTEGER          LWORK
*     ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DROUNDUP_LWORK deals with a subtle bug with returning LWORK as a Float.
*> This routine guarantees it is rounded up instead of down by
*> multiplying LWORK by 1+eps when it is necessary, where eps is the relative machine precision.
*> E.g.,
*>
*>        float( 9007199254740993            ) == 9007199254740992
*>        float( 9007199254740993 ) * (1.+eps) == 9007199254740994
*>
*> \return DROUNDUP_LWORK
*> \verbatim
*>         DROUNDUP_LWORK >= LWORK.
*>         DROUNDUP_LWORK is guaranteed to have zero decimal part.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] LWORK Workspace size.
*
*  Authors:
*  ========
*
*> \author Weslley Pereira, University of Colorado Denver, USA
*
*> \ingroup auxOTHERauxiliary
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>  This routine was inspired in the method `magma_zmake_lwork` from MAGMA.
*>  \see https://bitbucket.org/icl/magma/src/master/control/magma_zauxiliary.cpp
*> \endverbatim
*
*  =====================================================================
      DOUBLE PRECISION FUNCTION DROUNDUP_LWORK( LWORK )
*
*  -- LAPACK auxiliary routine --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*
*     .. Scalar Arguments ..
      INTEGER          LWORK
*     ..
*
* =====================================================================
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC         EPSILON, DBLE, INT
*     ..
*     .. Executable Statements ..
*     ..
      DROUNDUP_LWORK = DBLE( LWORK )
*
      IF( INT( DROUNDUP_LWORK ) .LT. LWORK ) THEN
*         Force round up of LWORK
          DROUNDUP_LWORK = DROUNDUP_LWORK * ( 1.0D+0 + EPSILON(0.0D+0) )
      ENDIF
*
      RETURN
*
*     End of DROUNDUP_LWORK
*
      END
