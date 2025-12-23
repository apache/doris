*> \brief \b CHETRS_AA
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CHETRS_AA + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/chetrs_aa.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/chetrs_aa.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/chetrs_aa.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CHETRS_AA( UPLO, N, NRHS, A, LDA, IPIV, B, LDB,
*                             WORK, LWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            N, NRHS, LDA, LDB, LWORK, INFO
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * )
*       COMPLEX            A( LDA, * ), B( LDB, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CHETRS_AA solves a system of linear equations A*X = B with a complex
*> hermitian matrix A using the factorization A = U**H*T*U or
*> A = L*T*L**H computed by CHETRF_AA.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          Specifies whether the details of the factorization are stored
*>          as an upper or lower triangular matrix.
*>          = 'U':  Upper triangular, form is A = U**H*T*U;
*>          = 'L':  Lower triangular, form is A = L*T*L**H.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] NRHS
*> \verbatim
*>          NRHS is INTEGER
*>          The number of right hand sides, i.e., the number of columns
*>          of the matrix B.  NRHS >= 0.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA,N)
*>          Details of factors computed by CHETRF_AA.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[in] IPIV
*> \verbatim
*>          IPIV is INTEGER array, dimension (N)
*>          Details of the interchanges as computed by CHETRF_AA.
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is COMPLEX array, dimension (LDB,NRHS)
*>          On entry, the right hand side matrix B.
*>          On exit, the solution matrix X.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B.  LDB >= max(1,N).
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX array, dimension (MAX(1,LWORK))
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the array WORK.
*>          If MIN(N,NRHS) = 0, LWORK >= 1, else LWORK >= 3*N-2.
*>
*>          If LWORK = -1, then a workspace query is assumed; the routine
*>          only calculates the minimal size of the WORK array, returns
*>          this value as the first entry of the WORK array, and no error
*>          message related to LWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value
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
*> \ingroup hetrs_aa
*
*  =====================================================================
      SUBROUTINE CHETRS_AA( UPLO, N, NRHS, A, LDA, IPIV, B, LDB,
     $                      WORK, LWORK, INFO )
*
*  -- LAPACK computational routine --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*
      IMPLICIT NONE
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
      INTEGER            N, NRHS, LDA, LDB, LWORK, INFO
*     ..
*     .. Array Arguments ..
      INTEGER            IPIV( * )
      COMPLEX            A( LDA, * ), B( LDB, * ), WORK( * )
*     ..
*
*  =====================================================================
*
      COMPLEX            ONE
      PARAMETER          ( ONE = 1.0E+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            LQUERY, UPPER
      INTEGER            K, KP, LWKMIN
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      REAL               SROUNDUP_LWORK
      EXTERNAL           LSAME, SROUNDUP_LWORK
*     ..
*     .. External Subroutines ..
      EXTERNAL           CLACPY, CLACGV, CGTSV, CSWAP, CTRSM, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MIN, MAX
*     ..
*     .. Executable Statements ..
*
      INFO = 0
      UPPER = LSAME( UPLO, 'U' )
      LQUERY = ( LWORK.EQ.-1 )
      IF( MIN( N, NRHS ).EQ.0 ) THEN
         LWKMIN = 1
      ELSE
         LWKMIN = 3*N-2
      END IF
*
      IF( .NOT.UPPER .AND. .NOT.LSAME( UPLO, 'L' ) ) THEN
         INFO = -1
      ELSE IF( N.LT.0 ) THEN
         INFO = -2
      ELSE IF( NRHS.LT.0 ) THEN
         INFO = -3
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -5
      ELSE IF( LDB.LT.MAX( 1, N ) ) THEN
         INFO = -8
      ELSE IF( LWORK.LT.LWKMIN .AND. .NOT.LQUERY ) THEN
         INFO = -10
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'CHETRS_AA', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         WORK( 1 ) = SROUNDUP_LWORK( LWKMIN )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( MIN( N, NRHS ).EQ.0 )
     $   RETURN
*
      IF( UPPER ) THEN
*
*        Solve A*X = B, where A = U**H*T*U.
*
*        1) Forward substitution with U**H
*
         IF( N.GT.1 ) THEN
*
*           Pivot, P**T * B -> B
*
            K = 1
            DO WHILE ( K.LE.N )
               KP = IPIV( K )
               IF( KP.NE.K )
     $            CALL CSWAP( NRHS, B( K, 1 ), LDB, B( KP, 1 ), LDB )
               K = K + 1
            END DO
*
*           Compute U**H \ B -> B    [ (U**H \P**T * B) ]
*
            CALL CTRSM( 'L', 'U', 'C', 'U', N-1, NRHS, ONE, A( 1, 2 ),
     $                  LDA, B( 2, 1 ), LDB)
         END IF
*
*        2) Solve with triangular matrix T
*
*        Compute T \ B -> B   [ T \ (U**H \P**T * B) ]
*
         CALL CLACPY( 'F', 1, N, A(1, 1), LDA+1, WORK(N), 1)
         IF( N.GT.1 ) THEN
             CALL CLACPY( 'F', 1, N-1, A( 1, 2 ), LDA+1, WORK( 2*N ), 1)
             CALL CLACPY( 'F', 1, N-1, A( 1, 2 ), LDA+1, WORK( 1 ), 1)
             CALL CLACGV( N-1, WORK( 1 ), 1 )
         END IF
         CALL CGTSV(N, NRHS, WORK(1), WORK(N), WORK(2*N), B, LDB,
     $              INFO)
*
*        3) Backward substitution with U
*
         IF( N.GT.1 ) THEN
*
*           Compute U \ B -> B   [ U \ (T \ (U**H \P**T * B) ) ]
*
            CALL CTRSM( 'L', 'U', 'N', 'U', N-1, NRHS, ONE, A( 1, 2 ),
     $                  LDA, B(2, 1), LDB)
*
*           Pivot, P * B  -> B [ P * (U \ (T \ (U**H \P**T * B) )) ]
*
            K = N
            DO WHILE ( K.GE.1 )
               KP = IPIV( K )
               IF( KP.NE.K )
     $            CALL CSWAP( NRHS, B( K, 1 ), LDB, B( KP, 1 ), LDB )
               K = K - 1
            END DO
         END IF
*
      ELSE
*
*        Solve A*X = B, where A = L*T*L**H.
*
*        1) Forward substitution with L
*
         IF( N.GT.1 ) THEN
*
*           Pivot, P**T * B -> B
*
            K = 1
            DO WHILE ( K.LE.N )
               KP = IPIV( K )
               IF( KP.NE.K )
     $            CALL CSWAP( NRHS, B( K, 1 ), LDB, B( KP, 1 ), LDB )
               K = K + 1
            END DO
*
*           Compute L \ B -> B    [ (L \P**T * B) ]
*
            CALL CTRSM( 'L', 'L', 'N', 'U', N-1, NRHS, ONE, A( 2, 1),
     $                  LDA, B(2, 1), LDB )
         END IF
*
*        2) Solve with triangular matrix T
*
*        Compute T \ B -> B   [ T \ (L \P**T * B) ]
*
         CALL CLACPY( 'F', 1, N, A(1, 1), LDA+1, WORK(N), 1)
         IF( N.GT.1 ) THEN
             CALL CLACPY( 'F', 1, N-1, A( 2, 1 ), LDA+1, WORK( 1 ), 1 )
             CALL CLACPY( 'F', 1, N-1, A( 2, 1 ), LDA+1, WORK( 2*N ), 1)
             CALL CLACGV( N-1, WORK( 2*N ), 1 )
         END IF
         CALL CGTSV(N, NRHS, WORK(1), WORK(N), WORK(2*N), B, LDB,
     $              INFO)
*
*        3) Backward substitution with L**H
*
         IF( N.GT.1 ) THEN
*
*           Compute (L**H \ B) -> B   [ L**H \ (T \ (L \P**T * B) ) ]
*
            CALL CTRSM( 'L', 'L', 'C', 'U', N-1, NRHS, ONE, A( 2, 1 ),
     $                  LDA, B( 2, 1 ), LDB )
*
*           Pivot, P * B -> B  [ P * (L**H \ (T \ (L \P**T * B) )) ]
*
            K = N
            DO WHILE ( K.GE.1 )
               KP = IPIV( K )
               IF( KP.NE.K )
     $            CALL CSWAP( NRHS, B( K, 1 ), LDB, B( KP, 1 ), LDB )
               K = K - 1
            END DO
         END IF
*
      END IF
*
      RETURN
*
*     End of CHETRS_AA
*
      END
