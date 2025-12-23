*> \brief \b CLAQZ1
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CLAQZ1 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/CLAQZ1.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/CLAQZ1.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/CLAQZ1.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*      SUBROUTINE CLAQZ1( ILQ, ILZ, K, ISTARTM, ISTOPM, IHI, A, LDA, B,
*     $    LDB, NQ, QSTART, Q, LDQ, NZ, ZSTART, Z, LDZ )
*      IMPLICIT NONE
*
*      Arguments
*      LOGICAL, INTENT( IN ) :: ILQ, ILZ
*      INTEGER, INTENT( IN ) :: K, LDA, LDB, LDQ, LDZ, ISTARTM, ISTOPM,
*     $    NQ, NZ, QSTART, ZSTART, IHI
*      COMPLEX :: A( LDA, * ), B( LDB, * ), Q( LDQ, * ), Z( LDZ, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>      CLAQZ1 chases a 1x1 shift bulge in a matrix pencil down a single position
*> \endverbatim
*
*
*  Arguments:
*  ==========
*
*>
*> \param[in] ILQ
*> \verbatim
*>          ILQ is LOGICAL
*>              Determines whether or not to update the matrix Q
*> \endverbatim
*>
*> \param[in] ILZ
*> \verbatim
*>          ILZ is LOGICAL
*>              Determines whether or not to update the matrix Z
*> \endverbatim
*>
*> \param[in] K
*> \verbatim
*>          K is INTEGER
*>              Index indicating the position of the bulge.
*>              On entry, the bulge is located in
*>              (A(k+1,k),B(k+1,k)).
*>              On exit, the bulge is located in
*>              (A(k+2,k+1),B(k+2,k+1)).
*> \endverbatim
*>
*> \param[in] ISTARTM
*> \verbatim
*>          ISTARTM is INTEGER
*> \endverbatim
*>
*> \param[in] ISTOPM
*> \verbatim
*>          ISTOPM is INTEGER
*>              Updates to (A,B) are restricted to
*>              (istartm:k+2,k:istopm). It is assumed
*>              without checking that istartm <= k+1 and
*>              k+2 <= istopm
*> \endverbatim
*>
*> \param[in] IHI
*> \verbatim
*>          IHI is INTEGER
*> \endverbatim
*>
*> \param[inout] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA,N)
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>              The leading dimension of A as declared in
*>              the calling procedure.
*> \endverbatim
*
*> \param[inout] B
*> \verbatim
*>          B is COMPLEX array, dimension (LDB,N)
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>              The leading dimension of B as declared in
*>              the calling procedure.
*> \endverbatim
*>
*> \param[in] NQ
*> \verbatim
*>          NQ is INTEGER
*>              The order of the matrix Q
*> \endverbatim
*>
*> \param[in] QSTART
*> \verbatim
*>          QSTART is INTEGER
*>              Start index of the matrix Q. Rotations are applied
*>              To columns k+2-qStart:k+3-qStart of Q.
*> \endverbatim
*
*> \param[inout] Q
*> \verbatim
*>          Q is COMPLEX array, dimension (LDQ,NQ)
*> \endverbatim
*>
*> \param[in] LDQ
*> \verbatim
*>          LDQ is INTEGER
*>              The leading dimension of Q as declared in
*>              the calling procedure.
*> \endverbatim
*>
*> \param[in] NZ
*> \verbatim
*>          NZ is INTEGER
*>              The order of the matrix Z
*> \endverbatim
*>
*> \param[in] ZSTART
*> \verbatim
*>          ZSTART is INTEGER
*>              Start index of the matrix Z. Rotations are applied
*>              To columns k+1-qStart:k+2-qStart of Z.
*> \endverbatim
*
*> \param[inout] Z
*> \verbatim
*>          Z is COMPLEX array, dimension (LDZ,NZ)
*> \endverbatim
*>
*> \param[in] LDZ
*> \verbatim
*>          LDZ is INTEGER
*>              The leading dimension of Q as declared in
*>              the calling procedure.
*> \endverbatim
*
*  Authors:
*  ========
*
*> \author Thijs Steel, KU Leuven
*
*> \date May 2020
*
*> \ingroup complexGEcomputational
*>
*  =====================================================================
      SUBROUTINE CLAQZ1( ILQ, ILZ, K, ISTARTM, ISTOPM, IHI, A, LDA, B,
     $                   LDB, NQ, QSTART, Q, LDQ, NZ, ZSTART, Z, LDZ )
      IMPLICIT NONE
*
*     Arguments
      LOGICAL, INTENT( IN ) :: ILQ, ILZ
      INTEGER, INTENT( IN ) :: K, LDA, LDB, LDQ, LDZ, ISTARTM, ISTOPM,
     $         NQ, NZ, QSTART, ZSTART, IHI
      COMPLEX :: A( LDA, * ), B( LDB, * ), Q( LDQ, * ), Z( LDZ, * )
*
*     Parameters
      COMPLEX         CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0, 0.0 ), CONE = ( 1.0, 0.0 ) )
      REAL :: ZERO, ONE, HALF
      PARAMETER( ZERO = 0.0, ONE = 1.0, HALF = 0.5 )
*
*     Local variables
      REAL :: C
      COMPLEX :: S, TEMP
*
*     External Functions
      EXTERNAL :: CLARTG, CROT
*
      IF( K+1 .EQ. IHI ) THEN
*
*        Shift is located on the edge of the matrix, remove it
*
         CALL CLARTG( B( IHI, IHI ), B( IHI, IHI-1 ), C, S, TEMP )
         B( IHI, IHI ) = TEMP
         B( IHI, IHI-1 ) = CZERO
         CALL CROT( IHI-ISTARTM, B( ISTARTM, IHI ), 1, B( ISTARTM,
     $              IHI-1 ), 1, C, S )
         CALL CROT( IHI-ISTARTM+1, A( ISTARTM, IHI ), 1, A( ISTARTM,
     $              IHI-1 ), 1, C, S )
         IF ( ILZ ) THEN
            CALL CROT( NZ, Z( 1, IHI-ZSTART+1 ), 1, Z( 1, IHI-1-ZSTART+
     $                 1 ), 1, C, S )
         END IF
*
      ELSE
*
*        Normal operation, move bulge down
*
*
*        Apply transformation from the right
*
         CALL CLARTG( B( K+1, K+1 ), B( K+1, K ), C, S, TEMP )
         B( K+1, K+1 ) = TEMP
         B( K+1, K ) = CZERO
         CALL CROT( K+2-ISTARTM+1, A( ISTARTM, K+1 ), 1, A( ISTARTM,
     $              K ), 1, C, S )
         CALL CROT( K-ISTARTM+1, B( ISTARTM, K+1 ), 1, B( ISTARTM, K ),
     $              1, C, S )
         IF ( ILZ ) THEN
            CALL CROT( NZ, Z( 1, K+1-ZSTART+1 ), 1, Z( 1, K-ZSTART+1 ),
     $                 1, C, S )
         END IF
*
*        Apply transformation from the left
*
         CALL CLARTG( A( K+1, K ), A( K+2, K ), C, S, TEMP )
         A( K+1, K ) = TEMP
         A( K+2, K ) = CZERO
         CALL CROT( ISTOPM-K, A( K+1, K+1 ), LDA, A( K+2, K+1 ), LDA, C,
     $              S )
         CALL CROT( ISTOPM-K, B( K+1, K+1 ), LDB, B( K+2, K+1 ), LDB, C,
     $              S )
         IF ( ILQ ) THEN
            CALL CROT( NQ, Q( 1, K+1-QSTART+1 ), 1, Q( 1, K+2-QSTART+
     $                 1 ), 1, C, CONJG( S ) )
         END IF
*
      END IF
*
*     End of CLAQZ1
*
      END SUBROUTINE