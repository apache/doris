*> \brief \b CERRUNHR_COL
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CERRUNHR_COL( PATH, NUNIT )
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
*> CERRUNHR_COL tests the error exits for CUNHR_COL that does
*> Householder reconstruction from the output of tall-skinny
*> factorization CLATSQR.
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
*> \ingroup complex_lin
*
*  =====================================================================
      SUBROUTINE CERRUNHR_COL( PATH, NUNIT )
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
      COMPLEX            A( NMAX, NMAX ), T( NMAX, NMAX ), D(NMAX)
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, CUNHR_COL
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
      INTRINSIC          REAL, CMPLX
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
            A( I, J ) = CMPLX( 1.E+0 / REAL( I+J ) )
            T( I, J ) = CMPLX( 1.E+0 / REAL( I+J ) )
         END DO
         D( J ) = ( 0.E+0, 0.E+0 )
      END DO
      OK = .TRUE.
*
*     Error exits for Householder reconstruction
*
*     CUNHR_COL
*
      SRNAMT = 'CUNHR_COL'
*
      INFOT = 1
      CALL CUNHR_COL( -1, 0, 1, A, 1, T, 1, D, INFO )
      CALL CHKXER( 'CUNHR_COL', INFOT, NOUT, LERR, OK )
*
      INFOT = 2
      CALL CUNHR_COL( 0, -1, 1, A, 1, T, 1, D, INFO )
      CALL CHKXER( 'CUNHR_COL', INFOT, NOUT, LERR, OK )
      CALL CUNHR_COL( 1, 2, 1, A, 1, T, 1, D, INFO )
      CALL CHKXER( 'CUNHR_COL', INFOT, NOUT, LERR, OK )
*
      INFOT = 3
      CALL CUNHR_COL( 0, 0, -1, A, 1, T, 1, D, INFO )
      CALL CHKXER( 'CUNHR_COL', INFOT, NOUT, LERR, OK )
*
      CALL CUNHR_COL( 0, 0, 0, A, 1, T, 1, D, INFO )
      CALL CHKXER( 'CUNHR_COL', INFOT, NOUT, LERR, OK )
*
      INFOT = 5
      CALL CUNHR_COL( 0, 0, 1, A, -1, T, 1, D, INFO )
      CALL CHKXER( 'CUNHR_COL', INFOT, NOUT, LERR, OK )
*
      CALL CUNHR_COL( 0, 0, 1, A, 0, T, 1, D, INFO )
      CALL CHKXER( 'CUNHR_COL', INFOT, NOUT, LERR, OK )
*
      CALL CUNHR_COL( 2, 0, 1, A, 1, T, 1, D, INFO )
      CALL CHKXER( 'CUNHR_COL', INFOT, NOUT, LERR, OK )
*
      INFOT = 7
      CALL CUNHR_COL( 0, 0, 1, A, 1, T, -1, D, INFO )
      CALL CHKXER( 'CUNHR_COL', INFOT, NOUT, LERR, OK )
*
      CALL CUNHR_COL( 0, 0, 1, A, 1, T, 0, D, INFO )
      CALL CHKXER( 'CUNHR_COL', INFOT, NOUT, LERR, OK )
*
      CALL CUNHR_COL( 4, 3, 2, A, 4, T, 1, D, INFO )
      CALL CHKXER( 'CUNHR_COL', INFOT, NOUT, LERR, OK )
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of CERRUNHR_COL
*
      END
