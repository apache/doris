*> \brief \b DLAQZ2
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DLAQZ2 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dlaqz2.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dlaqz2.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dlaqz2.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*      SUBROUTINE DLAQZ2( ILQ, ILZ, K, ISTARTM, ISTOPM, IHI, A, LDA, B,
*     $    LDB, NQ, QSTART, Q, LDQ, NZ, ZSTART, Z, LDZ )
*      IMPLICIT NONE
*
*      Arguments
*      LOGICAL, INTENT( IN ) :: ILQ, ILZ
*      INTEGER, INTENT( IN ) :: K, LDA, LDB, LDQ, LDZ, ISTARTM, ISTOPM,
*     $    NQ, NZ, QSTART, ZSTART, IHI
*      DOUBLE PRECISION :: A( LDA, * ), B( LDB, * ), Q( LDQ, * ), Z( LDZ,
*     $    * )
*      ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>      DLAQZ2 chases a 2x2 shift bulge in a matrix pencil down a single position
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
*>              (A(k+1:k+2,k:k+1),B(k+1:k+2,k:k+1)).
*>              On exit, the bulge is located in
*>              (A(k+2:k+3,k+1:k+2),B(k+2:k+3,k+1:k+2)).
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
*>              (istartm:k+3,k:istopm). It is assumed
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
*>          A is DOUBLE PRECISION array, dimension (LDA,N)
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
*>          B is DOUBLE PRECISION array, dimension (LDB,N)
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
*>              To columns k+2-qStart:k+4-qStart of Q.
*> \endverbatim
*
*> \param[inout] Q
*> \verbatim
*>          Q is DOUBLE PRECISION array, dimension (LDQ,NQ)
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
*>              To columns k+1-qStart:k+3-qStart of Z.
*> \endverbatim
*
*> \param[inout] Z
*> \verbatim
*>          Z is DOUBLE PRECISION array, dimension (LDZ,NZ)
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
*> \ingroup doubleGEcomputational
*>
*  =====================================================================
      SUBROUTINE DLAQZ2( ILQ, ILZ, K, ISTARTM, ISTOPM, IHI, A, LDA, B,
     $                   LDB, NQ, QSTART, Q, LDQ, NZ, ZSTART, Z, LDZ )
      IMPLICIT NONE
*
*     Arguments
      LOGICAL, INTENT( IN ) :: ILQ, ILZ
      INTEGER, INTENT( IN ) :: K, LDA, LDB, LDQ, LDZ, ISTARTM, ISTOPM,
     $         NQ, NZ, QSTART, ZSTART, IHI
      DOUBLE PRECISION :: A( LDA, * ), B( LDB, * ), Q( LDQ, * ), Z( LDZ,
     $                    * )
*
*     Parameters
      DOUBLE PRECISION :: ZERO, ONE, HALF
      PARAMETER( ZERO = 0.0D0, ONE = 1.0D0, HALF = 0.5D0 )
*
*     Local variables
      DOUBLE PRECISION :: H( 2, 3 ), C1, S1, C2, S2, TEMP
*
*     External functions
      EXTERNAL :: DLARTG, DROT
*
      IF( K+2 .EQ. IHI ) THEN
*        Shift is located on the edge of the matrix, remove it
         H = B( IHI-1:IHI, IHI-2:IHI )
*        Make H upper triangular
         CALL DLARTG( H( 1, 1 ), H( 2, 1 ), C1, S1, TEMP )
         H( 2, 1 ) = ZERO
         H( 1, 1 ) = TEMP
         CALL DROT( 2, H( 1, 2 ), 2, H( 2, 2 ), 2, C1, S1 )
*
         CALL DLARTG( H( 2, 3 ), H( 2, 2 ), C1, S1, TEMP )
         CALL DROT( 1, H( 1, 3 ), 1, H( 1, 2 ), 1, C1, S1 )
         CALL DLARTG( H( 1, 2 ), H( 1, 1 ), C2, S2, TEMP )
*
         CALL DROT( IHI-ISTARTM+1, B( ISTARTM, IHI ), 1, B( ISTARTM,
     $              IHI-1 ), 1, C1, S1 )
         CALL DROT( IHI-ISTARTM+1, B( ISTARTM, IHI-1 ), 1, B( ISTARTM,
     $              IHI-2 ), 1, C2, S2 )
         B( IHI-1, IHI-2 ) = ZERO
         B( IHI, IHI-2 ) = ZERO
         CALL DROT( IHI-ISTARTM+1, A( ISTARTM, IHI ), 1, A( ISTARTM,
     $              IHI-1 ), 1, C1, S1 )
         CALL DROT( IHI-ISTARTM+1, A( ISTARTM, IHI-1 ), 1, A( ISTARTM,
     $              IHI-2 ), 1, C2, S2 )
         IF ( ILZ ) THEN
            CALL DROT( NZ, Z( 1, IHI-ZSTART+1 ), 1, Z( 1, IHI-1-ZSTART+
     $                 1 ), 1, C1, S1 )
            CALL DROT( NZ, Z( 1, IHI-1-ZSTART+1 ), 1, Z( 1,
     $                 IHI-2-ZSTART+1 ), 1, C2, S2 )
         END IF
*
         CALL DLARTG( A( IHI-1, IHI-2 ), A( IHI, IHI-2 ), C1, S1,
     $                TEMP )
         A( IHI-1, IHI-2 ) = TEMP
         A( IHI, IHI-2 ) = ZERO
         CALL DROT( ISTOPM-IHI+2, A( IHI-1, IHI-1 ), LDA, A( IHI,
     $              IHI-1 ), LDA, C1, S1 )
         CALL DROT( ISTOPM-IHI+2, B( IHI-1, IHI-1 ), LDB, B( IHI,
     $              IHI-1 ), LDB, C1, S1 )
         IF ( ILQ ) THEN
            CALL DROT( NQ, Q( 1, IHI-1-QSTART+1 ), 1, Q( 1, IHI-QSTART+
     $                 1 ), 1, C1, S1 )
         END IF
*
         CALL DLARTG( B( IHI, IHI ), B( IHI, IHI-1 ), C1, S1, TEMP )
         B( IHI, IHI ) = TEMP
         B( IHI, IHI-1 ) = ZERO
         CALL DROT( IHI-ISTARTM, B( ISTARTM, IHI ), 1, B( ISTARTM,
     $              IHI-1 ), 1, C1, S1 )
         CALL DROT( IHI-ISTARTM+1, A( ISTARTM, IHI ), 1, A( ISTARTM,
     $              IHI-1 ), 1, C1, S1 )
         IF ( ILZ ) THEN
            CALL DROT( NZ, Z( 1, IHI-ZSTART+1 ), 1, Z( 1, IHI-1-ZSTART+
     $                 1 ), 1, C1, S1 )
         END IF
*
      ELSE
*
*        Normal operation, move bulge down
*
         H = B( K+1:K+2, K:K+2 )
*
*        Make H upper triangular
*
         CALL DLARTG( H( 1, 1 ), H( 2, 1 ), C1, S1, TEMP )
         H( 2, 1 ) = ZERO
         H( 1, 1 ) = TEMP
         CALL DROT( 2, H( 1, 2 ), 2, H( 2, 2 ), 2, C1, S1 )
*
*        Calculate Z1 and Z2
*
         CALL DLARTG( H( 2, 3 ), H( 2, 2 ), C1, S1, TEMP )
         CALL DROT( 1, H( 1, 3 ), 1, H( 1, 2 ), 1, C1, S1 )
         CALL DLARTG( H( 1, 2 ), H( 1, 1 ), C2, S2, TEMP )
*
*        Apply transformations from the right
*
         CALL DROT( K+3-ISTARTM+1, A( ISTARTM, K+2 ), 1, A( ISTARTM,
     $              K+1 ), 1, C1, S1 )
         CALL DROT( K+3-ISTARTM+1, A( ISTARTM, K+1 ), 1, A( ISTARTM,
     $              K ), 1, C2, S2 )
         CALL DROT( K+2-ISTARTM+1, B( ISTARTM, K+2 ), 1, B( ISTARTM,
     $              K+1 ), 1, C1, S1 )
         CALL DROT( K+2-ISTARTM+1, B( ISTARTM, K+1 ), 1, B( ISTARTM,
     $              K ), 1, C2, S2 )
         IF ( ILZ ) THEN
            CALL DROT( NZ, Z( 1, K+2-ZSTART+1 ), 1, Z( 1, K+1-ZSTART+
     $                 1 ), 1, C1, S1 )
            CALL DROT( NZ, Z( 1, K+1-ZSTART+1 ), 1, Z( 1, K-ZSTART+1 ),
     $                 1, C2, S2 )
         END IF
         B( K+1, K ) = ZERO
         B( K+2, K ) = ZERO
*
*        Calculate Q1 and Q2
*
         CALL DLARTG( A( K+2, K ), A( K+3, K ), C1, S1, TEMP )
         A( K+2, K ) = TEMP
         A( K+3, K ) = ZERO
         CALL DLARTG( A( K+1, K ), A( K+2, K ), C2, S2, TEMP )
         A( K+1, K ) = TEMP
         A( K+2, K ) = ZERO
*
*        Apply transformations from the left
*
         CALL DROT( ISTOPM-K, A( K+2, K+1 ), LDA, A( K+3, K+1 ), LDA,
     $              C1, S1 )
         CALL DROT( ISTOPM-K, A( K+1, K+1 ), LDA, A( K+2, K+1 ), LDA,
     $              C2, S2 )
*
         CALL DROT( ISTOPM-K, B( K+2, K+1 ), LDB, B( K+3, K+1 ), LDB,
     $              C1, S1 )
         CALL DROT( ISTOPM-K, B( K+1, K+1 ), LDB, B( K+2, K+1 ), LDB,
     $              C2, S2 )
         IF ( ILQ ) THEN
            CALL DROT( NQ, Q( 1, K+2-QSTART+1 ), 1, Q( 1, K+3-QSTART+
     $                 1 ), 1, C1, S1 )
            CALL DROT( NQ, Q( 1, K+1-QSTART+1 ), 1, Q( 1, K+2-QSTART+
     $                 1 ), 1, C2, S2 )
         END IF
*
      END IF
*
*     End of DLAQZ2
*
      END SUBROUTINE