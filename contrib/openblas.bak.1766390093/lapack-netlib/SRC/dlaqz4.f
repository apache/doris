*> \brief \b DLAQZ4
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DLAQZ4 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dlaqz4.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dlaqz4.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dlaqz4.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*      SUBROUTINE DLAQZ4( ILSCHUR, ILQ, ILZ, N, ILO, IHI, NSHIFTS,
*     $    NBLOCK_DESIRED, SR, SI, SS, A, LDA, B, LDB, Q, LDQ, Z, LDZ,
*     $    QC, LDQC, ZC, LDZC, WORK, LWORK, INFO )
*      IMPLICIT NONE
*
*      Function arguments
*      LOGICAL, INTENT( IN ) :: ILSCHUR, ILQ, ILZ
*      INTEGER, INTENT( IN ) :: N, ILO, IHI, LDA, LDB, LDQ, LDZ, LWORK,
*     $    NSHIFTS, NBLOCK_DESIRED, LDQC, LDZC
*
*      DOUBLE PRECISION, INTENT( INOUT ) :: A( LDA, * ), B( LDB, * ),
*     $    Q( LDQ, * ), Z( LDZ, * ), QC( LDQC, * ), ZC( LDZC, * ),
*     $    WORK( * ), SR( * ), SI( * ), SS( * )
*
*      INTEGER, INTENT( OUT ) :: INFO
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DLAQZ4 Executes a single multishift QZ sweep
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] ILSCHUR
*> \verbatim
*>          ILSCHUR is LOGICAL
*>              Determines whether or not to update the full Schur form
*> \endverbatim
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
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrices A, B, Q, and Z.  N >= 0.
*> \endverbatim
*>
*> \param[in] ILO
*> \verbatim
*>          ILO is INTEGER
*> \endverbatim
*>
*> \param[in] IHI
*> \verbatim
*>          IHI is INTEGER
*> \endverbatim
*>
*> \param[in] NSHIFTS
*> \verbatim
*>          NSHIFTS is INTEGER
*>          The desired number of shifts to use
*> \endverbatim
*>
*> \param[in] NBLOCK_DESIRED
*> \verbatim
*>          NBLOCK_DESIRED is INTEGER
*>          The desired size of the computational windows
*> \endverbatim
*>
*> \param[in] SR
*> \verbatim
*>          SR is DOUBLE PRECISION array. SR contains
*>          the real parts of the shifts to use.
*> \endverbatim
*>
*> \param[in] SI
*> \verbatim
*>          SI is DOUBLE PRECISION array. SI contains
*>          the imaginary parts of the shifts to use.
*> \endverbatim
*>
*> \param[in] SS
*> \verbatim
*>          SS is DOUBLE PRECISION array. SS contains
*>          the scale of the shifts to use.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is DOUBLE PRECISION array, dimension (LDA, N)
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max( 1, N ).
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is DOUBLE PRECISION array, dimension (LDB, N)
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B.  LDB >= max( 1, N ).
*> \endverbatim
*>
*> \param[in,out] Q
*> \verbatim
*>          Q is DOUBLE PRECISION array, dimension (LDQ, N)
*> \endverbatim
*>
*> \param[in] LDQ
*> \verbatim
*>          LDQ is INTEGER
*> \endverbatim
*>
*> \param[in,out] Z
*> \verbatim
*>          Z is DOUBLE PRECISION array, dimension (LDZ, N)
*> \endverbatim
*>
*> \param[in] LDZ
*> \verbatim
*>          LDZ is INTEGER
*> \endverbatim
*>
*> \param[in,out] QC
*> \verbatim
*>          QC is DOUBLE PRECISION array, dimension (LDQC, NBLOCK_DESIRED)
*> \endverbatim
*>
*> \param[in] LDQC
*> \verbatim
*>          LDQC is INTEGER
*> \endverbatim
*>
*> \param[in,out] ZC
*> \verbatim
*>          ZC is DOUBLE PRECISION array, dimension (LDZC, NBLOCK_DESIRED)
*> \endverbatim
*>
*> \param[in] LDZC
*> \verbatim
*>          LDZ is INTEGER
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (MAX(1,LWORK))
*>          On exit, if INFO >= 0, WORK(1) returns the optimal LWORK.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the array WORK.  LWORK >= max(1,N).
*>
*>          If LWORK = -1, then a workspace query is assumed; the routine
*>          only calculates the optimal size of the WORK array, returns
*>          this value as the first entry of the WORK array, and no error
*>          message related to LWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0: successful exit
*>          < 0: if INFO = -i, the i-th argument had an illegal value
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
      SUBROUTINE DLAQZ4( ILSCHUR, ILQ, ILZ, N, ILO, IHI, NSHIFTS,
     $                   NBLOCK_DESIRED, SR, SI, SS, A, LDA, B, LDB, Q,
     $                   LDQ, Z, LDZ, QC, LDQC, ZC, LDZC, WORK, LWORK,
     $                   INFO )
      IMPLICIT NONE

*     Function arguments
      LOGICAL, INTENT( IN ) :: ILSCHUR, ILQ, ILZ
      INTEGER, INTENT( IN ) :: N, ILO, IHI, LDA, LDB, LDQ, LDZ, LWORK,
     $         NSHIFTS, NBLOCK_DESIRED, LDQC, LDZC

      DOUBLE PRECISION, INTENT( INOUT ) :: A( LDA, * ), B( LDB, * ),
     $                  Q( LDQ, * ), Z( LDZ, * ), QC( LDQC, * ),
     $                  ZC( LDZC, * ), WORK( * ), SR( * ), SI( * ),
     $                  SS( * )

      INTEGER, INTENT( OUT ) :: INFO

*     Parameters
      DOUBLE PRECISION :: ZERO, ONE, HALF
      PARAMETER( ZERO = 0.0D0, ONE = 1.0D0, HALF = 0.5D0 )

*     Local scalars
      INTEGER :: I, J, NS, ISTARTM, ISTOPM, SHEIGHT, SWIDTH, K, NP,
     $           ISTARTB, ISTOPB, ISHIFT, NBLOCK, NPOS
      DOUBLE PRECISION :: TEMP, V( 3 ), C1, S1, C2, S2, SWAP
*
*     External functions
      EXTERNAL :: XERBLA, DGEMM, DLAQZ1, DLAQZ2, DLASET, DLARTG, DROT,
     $            DLACPY

      INFO = 0
      IF ( NBLOCK_DESIRED .LT. NSHIFTS+1 ) THEN
         INFO = -8
      END IF
      IF ( LWORK .EQ.-1 ) THEN
*        workspace query, quick return
         WORK( 1 ) = N*NBLOCK_DESIRED
         RETURN
      ELSE IF ( LWORK .LT. N*NBLOCK_DESIRED ) THEN
         INFO = -25
      END IF

      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'DLAQZ4', -INFO )
         RETURN
      END IF

*     Executable statements

      IF ( NSHIFTS .LT. 2 ) THEN
         RETURN
      END IF

      IF ( ILO .GE. IHI ) THEN
         RETURN
      END IF

      IF ( ILSCHUR ) THEN
         ISTARTM = 1
         ISTOPM = N
      ELSE
         ISTARTM = ILO
         ISTOPM = IHI
      END IF

*     Shuffle shifts into pairs of real shifts and pairs
*     of complex conjugate shifts assuming complex
*     conjugate shifts are already adjacent to one
*     another

      DO I = 1, NSHIFTS-2, 2
         IF( SI( I ).NE.-SI( I+1 ) ) THEN
*
            SWAP = SR( I )
            SR( I ) = SR( I+1 )
            SR( I+1 ) = SR( I+2 )
            SR( I+2 ) = SWAP

            SWAP = SI( I )
            SI( I ) = SI( I+1 )
            SI( I+1 ) = SI( I+2 )
            SI( I+2 ) = SWAP
            
            SWAP = SS( I )
            SS( I ) = SS( I+1 )
            SS( I+1 ) = SS( I+2 )
            SS( I+2 ) = SWAP
         END IF
      END DO

*     NSHFTS is supposed to be even, but if it is odd,
*     then simply reduce it by one.  The shuffle above
*     ensures that the dropped shift is real and that
*     the remaining shifts are paired.

      NS = NSHIFTS-MOD( NSHIFTS, 2 )
      NPOS = MAX( NBLOCK_DESIRED-NS, 1 )

*     The following block introduces the shifts and chases
*     them down one by one just enough to make space for
*     the other shifts. The near-the-diagonal block is
*     of size (ns+1) x ns.

      CALL DLASET( 'FULL', NS+1, NS+1, ZERO, ONE, QC, LDQC )
      CALL DLASET( 'FULL', NS, NS, ZERO, ONE, ZC, LDZC )

      DO I = 1, NS, 2
*        Introduce the shift
         CALL DLAQZ1( A( ILO, ILO ), LDA, B( ILO, ILO ), LDB, SR( I ),
     $                SR( I+1 ), SI( I ), SS( I ), SS( I+1 ), V )

         TEMP = V( 2 )
         CALL DLARTG( TEMP, V( 3 ), C1, S1, V( 2 ) )
         CALL DLARTG( V( 1 ), V( 2 ), C2, S2, TEMP )

         CALL DROT( NS, A( ILO+1, ILO ), LDA, A( ILO+2, ILO ), LDA, C1,
     $              S1 )
         CALL DROT( NS, A( ILO, ILO ), LDA, A( ILO+1, ILO ), LDA, C2,
     $              S2 )
         CALL DROT( NS, B( ILO+1, ILO ), LDB, B( ILO+2, ILO ), LDB, C1,
     $              S1 )
         CALL DROT( NS, B( ILO, ILO ), LDB, B( ILO+1, ILO ), LDB, C2,
     $              S2 )
         CALL DROT( NS+1, QC( 1, 2 ), 1, QC( 1, 3 ), 1, C1, S1 )
         CALL DROT( NS+1, QC( 1, 1 ), 1, QC( 1, 2 ), 1, C2, S2 )

*        Chase the shift down
         DO J = 1, NS-1-I

            CALL DLAQZ2( .TRUE., .TRUE., J, 1, NS, IHI-ILO+1, A( ILO,
     $                   ILO ), LDA, B( ILO, ILO ), LDB, NS+1, 1, QC,
     $                   LDQC, NS, 1, ZC, LDZC )

         END DO

      END DO

*     Update the rest of the pencil

*     Update A(ilo:ilo+ns,ilo+ns:istopm) and B(ilo:ilo+ns,ilo+ns:istopm)
*     from the left with Qc(1:ns+1,1:ns+1)'
      SHEIGHT = NS+1
      SWIDTH = ISTOPM-( ILO+NS )+1
      IF ( SWIDTH > 0 ) THEN
         CALL DGEMM( 'T', 'N', SHEIGHT, SWIDTH, SHEIGHT, ONE, QC, LDQC,
     $               A( ILO, ILO+NS ), LDA, ZERO, WORK, SHEIGHT )
         CALL DLACPY( 'ALL', SHEIGHT, SWIDTH, WORK, SHEIGHT, A( ILO,
     $                ILO+NS ), LDA )
         CALL DGEMM( 'T', 'N', SHEIGHT, SWIDTH, SHEIGHT, ONE, QC, LDQC,
     $               B( ILO, ILO+NS ), LDB, ZERO, WORK, SHEIGHT )
         CALL DLACPY( 'ALL', SHEIGHT, SWIDTH, WORK, SHEIGHT, B( ILO,
     $                ILO+NS ), LDB )
      END IF
      IF ( ILQ ) THEN
         CALL DGEMM( 'N', 'N', N, SHEIGHT, SHEIGHT, ONE, Q( 1, ILO ),
     $               LDQ, QC, LDQC, ZERO, WORK, N )
         CALL DLACPY( 'ALL', N, SHEIGHT, WORK, N, Q( 1, ILO ), LDQ )
      END IF

*     Update A(istartm:ilo-1,ilo:ilo+ns-1) and B(istartm:ilo-1,ilo:ilo+ns-1)
*     from the right with Zc(1:ns,1:ns)
      SHEIGHT = ILO-1-ISTARTM+1
      SWIDTH = NS
      IF ( SHEIGHT > 0 ) THEN
         CALL DGEMM( 'N', 'N', SHEIGHT, SWIDTH, SWIDTH, ONE, A( ISTARTM,
     $               ILO ), LDA, ZC, LDZC, ZERO, WORK, SHEIGHT )
         CALL DLACPY( 'ALL', SHEIGHT, SWIDTH, WORK, SHEIGHT, A( ISTARTM,
     $                ILO ), LDA )
         CALL DGEMM( 'N', 'N', SHEIGHT, SWIDTH, SWIDTH, ONE, B( ISTARTM,
     $               ILO ), LDB, ZC, LDZC, ZERO, WORK, SHEIGHT )
         CALL DLACPY( 'ALL', SHEIGHT, SWIDTH, WORK, SHEIGHT, B( ISTARTM,
     $                ILO ), LDB )
      END IF
      IF ( ILZ ) THEN
         CALL DGEMM( 'N', 'N', N, SWIDTH, SWIDTH, ONE, Z( 1, ILO ), LDZ,
     $               ZC, LDZC, ZERO, WORK, N )
         CALL DLACPY( 'ALL', N, SWIDTH, WORK, N, Z( 1, ILO ), LDZ )
      END IF

*     The following block chases the shifts down to the bottom
*     right block. If possible, a shift is moved down npos
*     positions at a time

      K = ILO
      DO WHILE ( K < IHI-NS )
         NP = MIN( IHI-NS-K, NPOS )
*        Size of the near-the-diagonal block
         NBLOCK = NS+NP
*        istartb points to the first row we will be updating
         ISTARTB = K+1
*        istopb points to the last column we will be updating
         ISTOPB = K+NBLOCK-1

         CALL DLASET( 'FULL', NS+NP, NS+NP, ZERO, ONE, QC, LDQC )
         CALL DLASET( 'FULL', NS+NP, NS+NP, ZERO, ONE, ZC, LDZC )

*        Near the diagonal shift chase
         DO I = NS-1, 0, -2
            DO J = 0, NP-1
*              Move down the block with index k+i+j-1, updating
*              the (ns+np x ns+np) block:
*              (k:k+ns+np,k:k+ns+np-1)
               CALL DLAQZ2( .TRUE., .TRUE., K+I+J-1, ISTARTB, ISTOPB,
     $                      IHI, A, LDA, B, LDB, NBLOCK, K+1, QC, LDQC,
     $                      NBLOCK, K, ZC, LDZC )
            END DO
         END DO

*        Update rest of the pencil

*        Update A(k+1:k+ns+np, k+ns+np:istopm) and
*        B(k+1:k+ns+np, k+ns+np:istopm)
*        from the left with Qc(1:ns+np,1:ns+np)'
         SHEIGHT = NS+NP
         SWIDTH = ISTOPM-( K+NS+NP )+1
         IF ( SWIDTH > 0 ) THEN
            CALL DGEMM( 'T', 'N', SHEIGHT, SWIDTH, SHEIGHT, ONE, QC,
     $                  LDQC, A( K+1, K+NS+NP ), LDA, ZERO, WORK,
     $                  SHEIGHT )
            CALL DLACPY( 'ALL', SHEIGHT, SWIDTH, WORK, SHEIGHT, A( K+1,
     $                   K+NS+NP ), LDA )
            CALL DGEMM( 'T', 'N', SHEIGHT, SWIDTH, SHEIGHT, ONE, QC,
     $                  LDQC, B( K+1, K+NS+NP ), LDB, ZERO, WORK,
     $                  SHEIGHT )
            CALL DLACPY( 'ALL', SHEIGHT, SWIDTH, WORK, SHEIGHT, B( K+1,
     $                   K+NS+NP ), LDB )
         END IF
         IF ( ILQ ) THEN
            CALL DGEMM( 'N', 'N', N, NBLOCK, NBLOCK, ONE, Q( 1, K+1 ),
     $                  LDQ, QC, LDQC, ZERO, WORK, N )
            CALL DLACPY( 'ALL', N, NBLOCK, WORK, N, Q( 1, K+1 ), LDQ )
         END IF

*        Update A(istartm:k,k:k+ns+npos-1) and B(istartm:k,k:k+ns+npos-1)
*        from the right with Zc(1:ns+np,1:ns+np)
         SHEIGHT = K-ISTARTM+1
         SWIDTH = NBLOCK
         IF ( SHEIGHT > 0 ) THEN
            CALL DGEMM( 'N', 'N', SHEIGHT, SWIDTH, SWIDTH, ONE,
     $                  A( ISTARTM, K ), LDA, ZC, LDZC, ZERO, WORK,
     $                  SHEIGHT )
            CALL DLACPY( 'ALL', SHEIGHT, SWIDTH, WORK, SHEIGHT,
     $                   A( ISTARTM, K ), LDA )
            CALL DGEMM( 'N', 'N', SHEIGHT, SWIDTH, SWIDTH, ONE,
     $                  B( ISTARTM, K ), LDB, ZC, LDZC, ZERO, WORK,
     $                  SHEIGHT )
            CALL DLACPY( 'ALL', SHEIGHT, SWIDTH, WORK, SHEIGHT,
     $                   B( ISTARTM, K ), LDB )
         END IF
         IF ( ILZ ) THEN
            CALL DGEMM( 'N', 'N', N, NBLOCK, NBLOCK, ONE, Z( 1, K ),
     $                  LDZ, ZC, LDZC, ZERO, WORK, N )
            CALL DLACPY( 'ALL', N, NBLOCK, WORK, N, Z( 1, K ), LDZ )
         END IF

         K = K+NP

      END DO

*     The following block removes the shifts from the bottom right corner
*     one by one. Updates are initially applied to A(ihi-ns+1:ihi,ihi-ns:ihi).

      CALL DLASET( 'FULL', NS, NS, ZERO, ONE, QC, LDQC )
      CALL DLASET( 'FULL', NS+1, NS+1, ZERO, ONE, ZC, LDZC )

*     istartb points to the first row we will be updating
      ISTARTB = IHI-NS+1
*     istopb points to the last column we will be updating
      ISTOPB = IHI

      DO I = 1, NS, 2
*        Chase the shift down to the bottom right corner
         DO ISHIFT = IHI-I-1, IHI-2
            CALL DLAQZ2( .TRUE., .TRUE., ISHIFT, ISTARTB, ISTOPB, IHI,
     $                   A, LDA, B, LDB, NS, IHI-NS+1, QC, LDQC, NS+1,
     $                   IHI-NS, ZC, LDZC )
         END DO
         
      END DO

*     Update rest of the pencil

*     Update A(ihi-ns+1:ihi, ihi+1:istopm)
*     from the left with Qc(1:ns,1:ns)'
      SHEIGHT = NS
      SWIDTH = ISTOPM-( IHI+1 )+1
      IF ( SWIDTH > 0 ) THEN
         CALL DGEMM( 'T', 'N', SHEIGHT, SWIDTH, SHEIGHT, ONE, QC, LDQC,
     $               A( IHI-NS+1, IHI+1 ), LDA, ZERO, WORK, SHEIGHT )
         CALL DLACPY( 'ALL', SHEIGHT, SWIDTH, WORK, SHEIGHT,
     $                A( IHI-NS+1, IHI+1 ), LDA )
         CALL DGEMM( 'T', 'N', SHEIGHT, SWIDTH, SHEIGHT, ONE, QC, LDQC,
     $               B( IHI-NS+1, IHI+1 ), LDB, ZERO, WORK, SHEIGHT )
         CALL DLACPY( 'ALL', SHEIGHT, SWIDTH, WORK, SHEIGHT,
     $                B( IHI-NS+1, IHI+1 ), LDB )
      END IF
      IF ( ILQ ) THEN
         CALL DGEMM( 'N', 'N', N, NS, NS, ONE, Q( 1, IHI-NS+1 ), LDQ,
     $               QC, LDQC, ZERO, WORK, N )
         CALL DLACPY( 'ALL', N, NS, WORK, N, Q( 1, IHI-NS+1 ), LDQ )
      END IF

*     Update A(istartm:ihi-ns,ihi-ns:ihi)
*     from the right with Zc(1:ns+1,1:ns+1)
      SHEIGHT = IHI-NS-ISTARTM+1
      SWIDTH = NS+1
      IF ( SHEIGHT > 0 ) THEN
         CALL DGEMM( 'N', 'N', SHEIGHT, SWIDTH, SWIDTH, ONE, A( ISTARTM,
     $               IHI-NS ), LDA, ZC, LDZC, ZERO, WORK, SHEIGHT )
         CALL DLACPY( 'ALL', SHEIGHT, SWIDTH, WORK, SHEIGHT, A( ISTARTM,
     $                IHI-NS ), LDA )
         CALL DGEMM( 'N', 'N', SHEIGHT, SWIDTH, SWIDTH, ONE, B( ISTARTM,
     $               IHI-NS ), LDB, ZC, LDZC, ZERO, WORK, SHEIGHT )
         CALL DLACPY( 'ALL', SHEIGHT, SWIDTH, WORK, SHEIGHT, B( ISTARTM,
     $                IHI-NS ), LDB )
      END IF
      IF ( ILZ ) THEN
         CALL DGEMM( 'N', 'N', N, NS+1, NS+1, ONE, Z( 1, IHI-NS ), LDZ,
     $               ZC, LDZC, ZERO, WORK, N )
         CALL DLACPY( 'ALL', N, NS+1, WORK, N, Z( 1, IHI-NS ), LDZ )
      END IF

      END SUBROUTINE
