*> \brief <b> ZGELST solves overdetermined or underdetermined systems for GE matrices using QR or LQ factorization with compact WY representation of Q.</b>
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZGELST + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zgelst.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zgelst.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zgelst.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZGELST( TRANS, M, N, NRHS, A, LDA, B, LDB, WORK, LWORK,
*                          INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          TRANS
*       INTEGER            INFO, LDA, LDB, LWORK, M, N, NRHS
*       ..
*       .. Array Arguments ..
*       COMPLEX*16         A( LDA, * ), B( LDB, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZGELST solves overdetermined or underdetermined real linear systems
*> involving an M-by-N matrix A, or its conjugate-transpose, using a QR
*> or LQ factorization of A with compact WY representation of Q.
*> It is assumed that A has full rank.
*>
*> The following options are provided:
*>
*> 1. If TRANS = 'N' and m >= n:  find the least squares solution of
*>    an overdetermined system, i.e., solve the least squares problem
*>                 minimize || B - A*X ||.
*>
*> 2. If TRANS = 'N' and m < n:  find the minimum norm solution of
*>    an underdetermined system A * X = B.
*>
*> 3. If TRANS = 'C' and m >= n:  find the minimum norm solution of
*>    an underdetermined system A**T * X = B.
*>
*> 4. If TRANS = 'C' and m < n:  find the least squares solution of
*>    an overdetermined system, i.e., solve the least squares problem
*>                 minimize || B - A**T * X ||.
*>
*> Several right hand side vectors b and solution vectors x can be
*> handled in a single call; they are stored as the columns of the
*> M-by-NRHS right hand side matrix B and the N-by-NRHS solution
*> matrix X.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] TRANS
*> \verbatim
*>          TRANS is CHARACTER*1
*>          = 'N': the linear system involves A;
*>          = 'C': the linear system involves A**H.
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the matrix A.  M >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] NRHS
*> \verbatim
*>          NRHS is INTEGER
*>          The number of right hand sides, i.e., the number of
*>          columns of the matrices B and X. NRHS >=0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA,N)
*>          On entry, the M-by-N matrix A.
*>          On exit,
*>            if M >= N, A is overwritten by details of its QR
*>                       factorization as returned by ZGEQRT;
*>            if M <  N, A is overwritten by details of its LQ
*>                       factorization as returned by ZGELQT.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,M).
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is COMPLEX*16 array, dimension (LDB,NRHS)
*>          On entry, the matrix B of right hand side vectors, stored
*>          columnwise; B is M-by-NRHS if TRANS = 'N', or N-by-NRHS
*>          if TRANS = 'C'.
*>          On exit, if INFO = 0, B is overwritten by the solution
*>          vectors, stored columnwise:
*>          if TRANS = 'N' and m >= n, rows 1 to n of B contain the least
*>          squares solution vectors; the residual sum of squares for the
*>          solution in each column is given by the sum of squares of
*>          modulus of elements N+1 to M in that column;
*>          if TRANS = 'N' and m < n, rows 1 to N of B contain the
*>          minimum norm solution vectors;
*>          if TRANS = 'C' and m >= n, rows 1 to M of B contain the
*>          minimum norm solution vectors;
*>          if TRANS = 'C' and m < n, rows 1 to M of B contain the
*>          least squares solution vectors; the residual sum of squares
*>          for the solution in each column is given by the sum of
*>          squares of the modulus of elements M+1 to N in that column.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B. LDB >= MAX(1,M,N).
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension (MAX(1,LWORK))
*>          On exit, if INFO = 0, WORK(1) returns the optimal LWORK.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the array WORK.
*>          LWORK >= max( 1, MN + max( MN, NRHS ) ).
*>          For optimal performance,
*>          LWORK >= max( 1, (MN + max( MN, NRHS ))*NB ).
*>          where MN = min(M,N) and NB is the optimum block size.
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
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value
*>          > 0:  if INFO =  i, the i-th diagonal element of the
*>                triangular factor of A is zero, so that A does not have
*>                full rank; the least squares solution could not be
*>                computed.
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
*> \ingroup complex16GEsolve
*
*> \par Contributors:
*  ==================
*>
*> \verbatim
*>
*>  November 2022,  Igor Kozachenko,
*>                  Computer Science Division,
*>                  University of California, Berkeley
*> \endverbatim
*
*  =====================================================================
      SUBROUTINE ZGELST( TRANS, M, N, NRHS, A, LDA, B, LDB, WORK, LWORK,
     $                   INFO )
*
*  -- LAPACK driver routine --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*
*     .. Scalar Arguments ..
      CHARACTER          TRANS
      INTEGER            INFO, LDA, LDB, LWORK, M, N, NRHS
*     ..
*     .. Array Arguments ..
      COMPLEX*16         A( LDA, * ), B( LDB, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
      COMPLEX*16         CZERO
      PARAMETER          ( CZERO = ( 0.0D+0, 0.0D+0 ) )
*     ..
*     .. Local Scalars ..
      LOGICAL            LQUERY, TPSD
      INTEGER            BROW, I, IASCL, IBSCL, J, LWOPT, MN, MNNRHS,
     $                   NB, NBMIN, SCLLEN
      DOUBLE PRECISION   ANRM, BIGNUM, BNRM, SMLNUM
*     ..
*     .. Local Arrays ..
      DOUBLE PRECISION   RWORK( 1 )
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ILAENV
      DOUBLE PRECISION   DLAMCH, ZLANGE
      EXTERNAL           LSAME, ILAENV, DLAMCH, ZLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           ZGELQT, ZGEQRT, ZGEMLQT, ZGEMQRT, DLABAD,
     $                   ZLASCL, ZLASET, ZTRTRS, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DBLE, MAX, MIN
*     ..
*     .. Executable Statements ..
*
*     Test the input arguments.
*
      INFO = 0
      MN = MIN( M, N )
      LQUERY = ( LWORK.EQ.-1 )
      IF( .NOT.( LSAME( TRANS, 'N' ) .OR. LSAME( TRANS, 'C' ) ) ) THEN
         INFO = -1
      ELSE IF( M.LT.0 ) THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -3
      ELSE IF( NRHS.LT.0 ) THEN
         INFO = -4
      ELSE IF( LDA.LT.MAX( 1, M ) ) THEN
         INFO = -6
      ELSE IF( LDB.LT.MAX( 1, M, N ) ) THEN
         INFO = -8
      ELSE IF( LWORK.LT.MAX( 1, MN+MAX( MN, NRHS ) ) .AND. .NOT.LQUERY )
     $          THEN
         INFO = -10
      END IF
*
*     Figure out optimal block size and optimal workspace size
*
      IF( INFO.EQ.0 .OR. INFO.EQ.-10 ) THEN
*
         TPSD = .TRUE.
         IF( LSAME( TRANS, 'N' ) )
     $      TPSD = .FALSE.
*
         NB = ILAENV( 1, 'ZGELST', ' ', M, N, -1, -1 )
*
         MNNRHS = MAX( MN, NRHS )
         LWOPT = MAX( 1, (MN+MNNRHS)*NB )
         WORK( 1 ) = DBLE( LWOPT )
*
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZGELST ', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( MIN( M, N, NRHS ).EQ.0 ) THEN
         CALL ZLASET( 'Full', MAX( M, N ), NRHS, CZERO, CZERO, B, LDB )
         WORK( 1 ) = DBLE( LWOPT )
         RETURN
      END IF
*
*     *GEQRT and *GELQT routines cannot accept NB larger than min(M,N)
*
      IF( NB.GT.MN ) NB = MN
*
*     Determine the block size from the supplied LWORK
*     ( at this stage we know that LWORK >= (minimum required workspace,
*     but it may be less than optimal)
*
      NB = MIN( NB, LWORK/( MN + MNNRHS ) )
*
*     The minimum value of NB, when blocked code is used
*
      NBMIN = MAX( 2, ILAENV( 2, 'ZGELST', ' ', M, N, -1, -1 ) )
*
      IF( NB.LT.NBMIN ) THEN
         NB = 1
      END IF
*
*     Get machine parameters
*
      SMLNUM = DLAMCH( 'S' ) / DLAMCH( 'P' )
      BIGNUM = ONE / SMLNUM
      CALL DLABAD( SMLNUM, BIGNUM )
*
*     Scale A, B if max element outside range [SMLNUM,BIGNUM]
*
      ANRM = ZLANGE( 'M', M, N, A, LDA, RWORK )
      IASCL = 0
      IF( ANRM.GT.ZERO .AND. ANRM.LT.SMLNUM ) THEN
*
*        Scale matrix norm up to SMLNUM
*
         CALL ZLASCL( 'G', 0, 0, ANRM, SMLNUM, M, N, A, LDA, INFO )
         IASCL = 1
      ELSE IF( ANRM.GT.BIGNUM ) THEN
*
*        Scale matrix norm down to BIGNUM
*
         CALL ZLASCL( 'G', 0, 0, ANRM, BIGNUM, M, N, A, LDA, INFO )
         IASCL = 2
      ELSE IF( ANRM.EQ.ZERO ) THEN
*
*        Matrix all zero. Return zero solution.
*
         CALL ZLASET( 'Full', MAX( M, N ), NRHS, CZERO, CZERO, B, LDB )
         WORK( 1 ) = DBLE( LWOPT )
         RETURN
      END IF
*
      BROW = M
      IF( TPSD )
     $   BROW = N
      BNRM = ZLANGE( 'M', BROW, NRHS, B, LDB, RWORK )
      IBSCL = 0
      IF( BNRM.GT.ZERO .AND. BNRM.LT.SMLNUM ) THEN
*
*        Scale matrix norm up to SMLNUM
*
         CALL ZLASCL( 'G', 0, 0, BNRM, SMLNUM, BROW, NRHS, B, LDB,
     $                INFO )
         IBSCL = 1
      ELSE IF( BNRM.GT.BIGNUM ) THEN
*
*        Scale matrix norm down to BIGNUM
*
         CALL ZLASCL( 'G', 0, 0, BNRM, BIGNUM, BROW, NRHS, B, LDB,
     $                INFO )
         IBSCL = 2
      END IF
*
      IF( M.GE.N ) THEN
*
*        M > N:
*        Compute the blocked QR factorization of A,
*        using the compact WY representation of Q,
*        workspace at least N, optimally N*NB.
*
         CALL ZGEQRT( M, N, NB, A, LDA, WORK( 1 ), NB,
     $                WORK( MN*NB+1 ), INFO )
*
         IF( .NOT.TPSD ) THEN
*
*           M > N, A is not transposed:
*           Overdetermined system of equations,
*           least-squares problem, min || A * X - B ||.
*
*           Compute B(1:M,1:NRHS) := Q**T * B(1:M,1:NRHS),
*           using the compact WY representation of Q,
*           workspace at least NRHS, optimally NRHS*NB.
*
            CALL ZGEMQRT( 'Left', 'Conjugate transpose', M, NRHS, N, NB,
     $                    A, LDA, WORK( 1 ), NB, B, LDB,
     $                    WORK( MN*NB+1 ), INFO )
*
*           Compute B(1:N,1:NRHS) := inv(R) * B(1:N,1:NRHS)
*
            CALL ZTRTRS( 'Upper', 'No transpose', 'Non-unit', N, NRHS,
     $                   A, LDA, B, LDB, INFO )
*
            IF( INFO.GT.0 ) THEN
               RETURN
            END IF
*
            SCLLEN = N
*
         ELSE
*
*           M > N, A is transposed:
*           Underdetermined system of equations,
*           minimum norm solution of A**T * X = B.
*
*           Compute B := inv(R**T) * B in two row blocks of B.
*
*           Block 1: B(1:N,1:NRHS) := inv(R**T) * B(1:N,1:NRHS)
*
            CALL ZTRTRS( 'Upper', 'Conjugate transpose', 'Non-unit',
     $                   N, NRHS, A, LDA, B, LDB, INFO )
*
            IF( INFO.GT.0 ) THEN
               RETURN
            END IF
*
*           Block 2: Zero out all rows below the N-th row in B:
*           B(N+1:M,1:NRHS) = ZERO
*
            DO  J = 1, NRHS
               DO I = N + 1, M
                  B( I, J ) = ZERO
               END DO
            END DO
*
*           Compute B(1:M,1:NRHS) := Q(1:N,:) * B(1:N,1:NRHS),
*           using the compact WY representation of Q,
*           workspace at least NRHS, optimally NRHS*NB.
*
            CALL ZGEMQRT( 'Left', 'No transpose', M, NRHS, N, NB,
     $                    A, LDA, WORK( 1 ), NB, B, LDB,
     $                    WORK( MN*NB+1 ), INFO )
*
            SCLLEN = M
*
         END IF
*
      ELSE
*
*        M < N:
*        Compute the blocked LQ factorization of A,
*        using the compact WY representation of Q,
*        workspace at least M, optimally M*NB.
*
         CALL ZGELQT( M, N, NB, A, LDA, WORK( 1 ), NB,
     $                WORK( MN*NB+1 ), INFO )
*
         IF( .NOT.TPSD ) THEN
*
*           M < N, A is not transposed:
*           Underdetermined system of equations,
*           minimum norm solution of A * X = B.
*
*           Compute B := inv(L) * B in two row blocks of B.
*
*           Block 1: B(1:M,1:NRHS) := inv(L) * B(1:M,1:NRHS)
*
            CALL ZTRTRS( 'Lower', 'No transpose', 'Non-unit', M, NRHS,
     $                   A, LDA, B, LDB, INFO )
*
            IF( INFO.GT.0 ) THEN
               RETURN
            END IF
*
*           Block 2: Zero out all rows below the M-th row in B:
*           B(M+1:N,1:NRHS) = ZERO
*
            DO J = 1, NRHS
               DO I = M + 1, N
                  B( I, J ) = ZERO
               END DO
            END DO
*
*           Compute B(1:N,1:NRHS) := Q(1:N,:)**T * B(1:M,1:NRHS),
*           using the compact WY representation of Q,
*           workspace at least NRHS, optimally NRHS*NB.
*
            CALL ZGEMLQT( 'Left', 'Conjugate transpose', N, NRHS, M, NB,
     $                   A, LDA, WORK( 1 ), NB, B, LDB,
     $                   WORK( MN*NB+1 ), INFO )
*
            SCLLEN = N
*
         ELSE
*
*           M < N, A is transposed:
*           Overdetermined system of equations,
*           least-squares problem, min || A**T * X - B ||.
*
*           Compute B(1:N,1:NRHS) := Q * B(1:N,1:NRHS),
*           using the compact WY representation of Q,
*           workspace at least NRHS, optimally NRHS*NB.
*
            CALL ZGEMLQT( 'Left', 'No transpose', N, NRHS, M, NB,
     $                    A, LDA, WORK( 1 ), NB, B, LDB,
     $                    WORK( MN*NB+1), INFO )
*
*           Compute B(1:M,1:NRHS) := inv(L**T) * B(1:M,1:NRHS)
*
            CALL ZTRTRS( 'Lower', 'Conjugate transpose', 'Non-unit',
     $                   M, NRHS, A, LDA, B, LDB, INFO )
*
            IF( INFO.GT.0 ) THEN
               RETURN
            END IF
*
            SCLLEN = M
*
         END IF
*
      END IF
*
*     Undo scaling
*
      IF( IASCL.EQ.1 ) THEN
         CALL ZLASCL( 'G', 0, 0, ANRM, SMLNUM, SCLLEN, NRHS, B, LDB,
     $                INFO )
      ELSE IF( IASCL.EQ.2 ) THEN
         CALL ZLASCL( 'G', 0, 0, ANRM, BIGNUM, SCLLEN, NRHS, B, LDB,
     $                INFO )
      END IF
      IF( IBSCL.EQ.1 ) THEN
         CALL ZLASCL( 'G', 0, 0, SMLNUM, BNRM, SCLLEN, NRHS, B, LDB,
     $                INFO )
      ELSE IF( IBSCL.EQ.2 ) THEN
         CALL ZLASCL( 'G', 0, 0, BIGNUM, BNRM, SCLLEN, NRHS, B, LDB,
     $                INFO )
      END IF
*
      WORK( 1 ) = DBLE( LWOPT )
*
      RETURN
*
*     End of ZGELST
*
      END
