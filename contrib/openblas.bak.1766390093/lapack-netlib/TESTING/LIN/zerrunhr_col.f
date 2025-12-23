*> \brief \b ZERRUNHR_COL
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZERRUNHR_COL( PATH, NUNIT )
*
*       .. Scalar Arguments ..
*       CHARACTER*3        PATH
*       INTEGER            NUNIT
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZERRUNHR_COL tests the error exits for ZUNHR_COL that does
*> Householder reconstruction from the output of tall-skinny
*> factorization ZLATSQR.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] PATH
*> \verbatim
*>          PATH is CHARACTER*3
*>          The LAPACK path name for the routines to be tested.
*> \endverbatim
*>
*> \param[in] NUNIT
*> \verbatim
*>          NUNIT is INTEGER
*>          The unit number for output.
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
*> \ingroup complex16_lin
*
*  =====================================================================
      SUBROUTINE ZERRUNHR_COL( PATH, NUNIT )
      IMPLICIT NONE
*
*  -- LAPACK test routine --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*
*     .. Scalar Arguments ..
      CHARACTER(LEN=3)   PATH
      INTEGER            NUNIT
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      INTEGER            NMAX
      PARAMETER          ( NMAX = 2 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, INFO, J
*     ..
*     .. Local Arrays ..
      COMPLEX*16         A( NMAX, NMAX ), T( NMAX, NMAX ), D(NMAX)
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, ZUNHR_COL
*     ..
*     .. Scalars in Common ..
      LOGICAL            LERR, OK
      CHARACTER(LEN=32)  SRNAMT
      INTEGER            INFOT, NOUT
*     ..
*     .. Common blocks ..
      COMMON             / INFOC / INFOT, NOUT, OK, LERR
      COMMON             / SRNAMC / SRNAMT
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DBLE, DCMPLX
*     ..
*     .. Executable Statements ..
*
      NOUT = NUNIT
      WRITE( NOUT, FMT = * )
*
*     Set the variables to innocuous values.
*
      DO J = 1, NMAX
         DO I = 1, NMAX
            A( I, J ) = DCMPLX( 1.D+0 / DBLE( I+J ) )
            T( I, J ) = DCMPLX( 1.D+0 / DBLE( I+J ) )
         END DO
         D( J ) = ( 0.D+0, 0.D+0 )
      END DO
      OK = .TRUE.
*
*     Error exits for Householder reconstruction
*
*     ZUNHR_COL
*
      SRNAMT = 'ZUNHR_COL'
*
      INFOT = 1
      CALL ZUNHR_COL( -1, 0, 1, A, 1, T, 1, D, INFO )
      CALL CHKXER( 'ZUNHR_COL', INFOT, NOUT, LERR, OK )
*
      INFOT = 2
      CALL ZUNHR_COL( 0, -1, 1, A, 1, T, 1, D, INFO )
      CALL CHKXER( 'ZUNHR_COL', INFOT, NOUT, LERR, OK )
      CALL ZUNHR_COL( 1, 2, 1, A, 1, T, 1, D, INFO )
      CALL CHKXER( 'ZUNHR_COL', INFOT, NOUT, LERR, OK )
*
      INFOT = 3
      CALL ZUNHR_COL( 0, 0, -1, A, 1, T, 1, D, INFO )
      CALL CHKXER( 'ZUNHR_COL', INFOT, NOUT, LERR, OK )
*
      CALL ZUNHR_COL( 0, 0, 0, A, 1, T, 1, D, INFO )
      CALL CHKXER( 'ZUNHR_COL', INFOT, NOUT, LERR, OK )
*
      INFOT = 5
      CALL ZUNHR_COL( 0, 0, 1, A, -1, T, 1, D, INFO )
      CALL CHKXER( 'ZUNHR_COL', INFOT, NOUT, LERR, OK )
*
      CALL ZUNHR_COL( 0, 0, 1, A, 0, T, 1, D, INFO )
      CALL CHKXER( 'ZUNHR_COL', INFOT, NOUT, LERR, OK )
*
      CALL ZUNHR_COL( 2, 0, 1, A, 1, T, 1, D, INFO )
      CALL CHKXER( 'ZUNHR_COL', INFOT, NOUT, LERR, OK )
*
      INFOT = 7
      CALL ZUNHR_COL( 0, 0, 1, A, 1, T, -1, D, INFO )
      CALL CHKXER( 'ZUNHR_COL', INFOT, NOUT, LERR, OK )
*
      CALL ZUNHR_COL( 0, 0, 1, A, 1, T, 0, D, INFO )
      CALL CHKXER( 'ZUNHR_COL', INFOT, NOUT, LERR, OK )
*
      CALL ZUNHR_COL( 4, 3, 2, A, 4, T, 1, D, INFO )
      CALL CHKXER( 'ZUNHR_COL', INFOT, NOUT, LERR, OK )
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of ZERRUNHR_COL
*
      END
