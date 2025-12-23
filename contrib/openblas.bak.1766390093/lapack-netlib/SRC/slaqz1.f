*> \brief \b SLAQZ1
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLAQZ1 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slaqz1.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slaqz1.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slaqz1.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*      SUBROUTINE SLAQZ1( A, LDA, B, LDB, SR1, SR2, SI, BETA1, BETA2,
*     $    V )
*      IMPLICIT NONE
*
*      Arguments
*      INTEGER, INTENT( IN ) :: LDA, LDB
*      REAL, INTENT( IN ) :: A( LDA, * ), B( LDB, * ), SR1, SR2, SI,
*     $   BETA1, BETA2
*      REAL, INTENT( OUT ) :: V( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>      Given a 3-by-3 matrix pencil (A,B), SLAQZ1 sets v to a
*>      scalar multiple of the first column of the product
*>
*>      (*)  K = (A - (beta2*sr2 - i*si)*B)*B^(-1)*(beta1*A - (sr2 + i*si2)*B)*B^(-1).
*>
*>      It is assumed that either
*>
*>              1) sr1 = sr2
*>          or
*>              2) si = 0.
*>
*>      This is useful for starting double implicit shift bulges
*>      in the QZ algorithm.
*> \endverbatim
*
*
*  Arguments:
*  ==========
*
*> \param[in] A
*> \verbatim
*>          A is REAL array, dimension (LDA,N)
*>              The 3-by-3 matrix A in (*).
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>              The leading dimension of A as declared in
*>              the calling procedure.
*> \endverbatim
*
*> \param[in] B
*> \verbatim
*>          B is REAL array, dimension (LDB,N)
*>              The 3-by-3 matrix B in (*).
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>              The leading dimension of B as declared in
*>              the calling procedure.
*> \endverbatim
*>
*> \param[in] SR1
*> \verbatim
*>          SR1 is REAL
*> \endverbatim
*>
*> \param[in] SR2
*> \verbatim
*>          SR2 is REAL
*> \endverbatim
*>
*> \param[in] SI
*> \verbatim
*>          SI is REAL
*> \endverbatim
*>
*> \param[in] BETA1
*> \verbatim
*>          BETA1 is REAL
*> \endverbatim
*>
*> \param[in] BETA2
*> \verbatim
*>          BETA2 is REAL
*> \endverbatim
*>
*> \param[out] V
*> \verbatim
*>          V is REAL array, dimension (N)
*>              A scalar multiple of the first column of the
*>              matrix K in (*).
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
      SUBROUTINE SLAQZ1( A, LDA, B, LDB, SR1, SR2, SI, BETA1, BETA2,
     $                   V )
      IMPLICIT NONE
*
*     Arguments
      INTEGER, INTENT( IN ) :: LDA, LDB
      REAL, INTENT( IN ) :: A( LDA, * ), B( LDB, * ), SR1, SR2, SI,
     $   BETA1, BETA2
      REAL, INTENT( OUT ) :: V( * )
*
*     Parameters
      REAL :: ZERO, ONE, HALF
      PARAMETER( ZERO = 0.0, ONE = 1.0, HALF = 0.5 )
*
*     Local scalars
      REAL :: W( 2 ), SAFMIN, SAFMAX, SCALE1, SCALE2
*
*     External Functions
      REAL, EXTERNAL :: SLAMCH
      LOGICAL, EXTERNAL :: SISNAN
*
      SAFMIN = SLAMCH( 'SAFE MINIMUM' )
      SAFMAX = ONE/SAFMIN
*
*     Calculate first shifted vector
*
      W( 1 ) = BETA1*A( 1, 1 )-SR1*B( 1, 1 )
      W( 2 ) = BETA1*A( 2, 1 )-SR1*B( 2, 1 )
      SCALE1 = SQRT( ABS( W( 1 ) ) ) * SQRT( ABS( W( 2 ) ) )
      IF( SCALE1 .GE. SAFMIN .AND. SCALE1 .LE. SAFMAX ) THEN
         W( 1 ) = W( 1 )/SCALE1
         W( 2 ) = W( 2 )/SCALE1
      END IF
*
*     Solve linear system
*
      W( 2 ) = W( 2 )/B( 2, 2 )
      W( 1 ) = ( W( 1 )-B( 1, 2 )*W( 2 ) )/B( 1, 1 )
      SCALE2 = SQRT( ABS( W( 1 ) ) ) * SQRT( ABS( W( 2 ) ) )
      IF( SCALE2 .GE. SAFMIN .AND. SCALE2 .LE. SAFMAX ) THEN
         W( 1 ) = W( 1 )/SCALE2
         W( 2 ) = W( 2 )/SCALE2
      END IF
*
*     Apply second shift
*
      V( 1 ) = BETA2*( A( 1, 1 )*W( 1 )+A( 1, 2 )*W( 2 ) )-SR2*( B( 1,
     $   1 )*W( 1 )+B( 1, 2 )*W( 2 ) )
      V( 2 ) = BETA2*( A( 2, 1 )*W( 1 )+A( 2, 2 )*W( 2 ) )-SR2*( B( 2,
     $   1 )*W( 1 )+B( 2, 2 )*W( 2 ) )
      V( 3 ) = BETA2*( A( 3, 1 )*W( 1 )+A( 3, 2 )*W( 2 ) )-SR2*( B( 3,
     $   1 )*W( 1 )+B( 3, 2 )*W( 2 ) )
*
*     Account for imaginary part
*
      V( 1 ) = V( 1 )+SI*SI*B( 1, 1 )/SCALE1/SCALE2
*
*     Check for overflow
*
      IF( ABS( V( 1 ) ).GT.SAFMAX .OR. ABS( V( 2 ) ) .GT. SAFMAX .OR.
     $   ABS( V( 3 ) ).GT.SAFMAX .OR. SISNAN( V( 1 ) ) .OR.
     $   SISNAN( V( 2 ) ) .OR. SISNAN( V( 3 ) ) ) THEN
         V( 1 ) = ZERO
         V( 2 ) = ZERO
         V( 3 ) = ZERO
      END IF
*
*     End of SLAQZ1
*
      END SUBROUTINE
