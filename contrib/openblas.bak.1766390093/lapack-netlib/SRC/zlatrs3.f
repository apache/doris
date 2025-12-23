*> \brief \b ZLATRS3 solves a triangular system of equations with the scale factors set to prevent overflow.
*
*  Definition:
*  ===========
*
*      SUBROUTINE ZLATRS3( UPLO, TRANS, DIAG, NORMIN, N, NRHS, A, LDA,
*                          X, LDX, SCALE, CNORM, WORK, LWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          DIAG, NORMIN, TRANS, UPLO
*       INTEGER            INFO, LDA, LWORK, LDX, N, NRHS
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   CNORM( * ), SCALE( * ), WORK( * )
*       COMPLEX*16         A( LDA, * ), X( LDX, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZLATRS3 solves one of the triangular systems
*>
*>    A * X = B * diag(scale),  A**T * X = B * diag(scale), or
*>    A**H * X = B * diag(scale)
*>
*> with scaling to prevent overflow.  Here A is an upper or lower
*> triangular matrix, A**T denotes the transpose of A, A**H denotes the
*> conjugate transpose of A. X and B are n-by-nrhs matrices and scale
*> is an nrhs-element vector of scaling factors. A scaling factor scale(j)
*> is usually less than or equal to 1, chosen such that X(:,j) is less
*> than the overflow threshold. If the matrix A is singular (A(j,j) = 0
*> for some j), then a non-trivial solution to A*X = 0 is returned. If
*> the system is so badly scaled that the solution cannot be represented
*> as (1/scale(k))*X(:,k), then x(:,k) = 0 and scale(k) is returned.
*>
*> This is a BLAS-3 version of LATRS for solving several right
*> hand sides simultaneously.
*>
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          Specifies whether the matrix A is upper or lower triangular.
*>          = 'U':  Upper triangular
*>          = 'L':  Lower triangular
*> \endverbatim
*>
*> \param[in] TRANS
*> \verbatim
*>          TRANS is CHARACTER*1
*>          Specifies the operation applied to A.
*>          = 'N':  Solve A * x = s*b  (No transpose)
*>          = 'T':  Solve A**T* x = s*b  (Transpose)
*>          = 'C':  Solve A**T* x = s*b  (Conjugate transpose)
*> \endverbatim
*>
*> \param[in] DIAG
*> \verbatim
*>          DIAG is CHARACTER*1
*>          Specifies whether or not the matrix A is unit triangular.
*>          = 'N':  Non-unit triangular
*>          = 'U':  Unit triangular
*> \endverbatim
*>
*> \param[in] NORMIN
*> \verbatim
*>          NORMIN is CHARACTER*1
*>          Specifies whether CNORM has been set or not.
*>          = 'Y':  CNORM contains the column norms on entry
*>          = 'N':  CNORM is not set on entry.  On exit, the norms will
*>                  be computed and stored in CNORM.
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
*>          The number of columns of X.  NRHS >= 0.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA,N)
*>          The triangular matrix A.  If UPLO = 'U', the leading n by n
*>          upper triangular part of the array A contains the upper
*>          triangular matrix, and the strictly lower triangular part of
*>          A is not referenced.  If UPLO = 'L', the leading n by n lower
*>          triangular part of the array A contains the lower triangular
*>          matrix, and the strictly upper triangular part of A is not
*>          referenced.  If DIAG = 'U', the diagonal elements of A are
*>          also not referenced and are assumed to be 1.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max (1,N).
*> \endverbatim
*>
*> \param[in,out] X
*> \verbatim
*>          X is COMPLEX*16 array, dimension (LDX,NRHS)
*>          On entry, the right hand side B of the triangular system.
*>          On exit, X is overwritten by the solution matrix X.
*> \endverbatim
*>
*> \param[in] LDX
*> \verbatim
*>          LDX is INTEGER
*>          The leading dimension of the array X.  LDX >= max (1,N).
*> \endverbatim
*>
*> \param[out] SCALE
*> \verbatim
*>          SCALE is DOUBLE PRECISION array, dimension (NRHS)
*>          The scaling factor s(k) is for the triangular system
*>          A * x(:,k) = s(k)*b(:,k)  or  A**T* x(:,k) = s(k)*b(:,k).
*>          If SCALE = 0, the matrix A is singular or badly scaled.
*>          If A(j,j) = 0 is encountered, a non-trivial vector x(:,k)
*>          that is an exact or approximate solution to A*x(:,k) = 0
*>          is returned. If the system so badly scaled that solution
*>          cannot be presented as x(:,k) * 1/s(k), then x(:,k) = 0
*>          is returned.
*> \endverbatim
*>
*> \param[in,out] CNORM
*> \verbatim
*>          CNORM is DOUBLE PRECISION array, dimension (N)
*>
*>          If NORMIN = 'Y', CNORM is an input argument and CNORM(j)
*>          contains the norm of the off-diagonal part of the j-th column
*>          of A.  If TRANS = 'N', CNORM(j) must be greater than or equal
*>          to the infinity-norm, and if TRANS = 'T' or 'C', CNORM(j)
*>          must be greater than or equal to the 1-norm.
*>
*>          If NORMIN = 'N', CNORM is an output argument and CNORM(j)
*>          returns the 1-norm of the offdiagonal part of the j-th column
*>          of A.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (LWORK).
*>          On exit, if INFO = 0, WORK(1) returns the optimal size of
*>          WORK.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the array WORK.
*>
*>          If MIN(N,NRHS) = 0, LWORK >= 1, else
*>          LWORK >= MAX(1, 2*NBA * MAX(NBA, MIN(NRHS, 32)), where
*>          NBA = (N + NB - 1)/NB and NB is the optimal block size.
*>
*>          If LWORK = -1, then a workspace query is assumed; the routine
*>          only calculates the optimal dimensions of the WORK array, returns
*>          this value as the first entry of the WORK array, and no error
*>          message related to LWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -k, the k-th argument had an illegal value
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
*> \ingroup latrs3
*> \par Further Details:
*  =====================
*  \verbatim
*  The algorithm follows the structure of a block triangular solve.
*  The diagonal block is solved with a call to the robust the triangular
*  solver LATRS for every right-hand side RHS = 1, ..., NRHS
*     op(A( J, J )) * x( J, RHS ) = SCALOC * b( J, RHS ),
*  where op( A ) = A or op( A ) = A**T or op( A ) = A**H.
*  The linear block updates operate on block columns of X,
*     B( I, K ) - op(A( I, J )) * X( J, K )
*  and use GEMM. To avoid overflow in the linear block update, the worst case
*  growth is estimated. For every RHS, a scale factor s <= 1.0 is computed
*  such that
*     || s * B( I, RHS )||_oo
*   + || op(A( I, J )) ||_oo * || s *  X( J, RHS ) ||_oo <= Overflow threshold
*
*  Once all columns of a block column have been rescaled (BLAS-1), the linear
*  update is executed with GEMM without overflow.
*
*  To limit rescaling, local scale factors track the scaling of column segments.
*  There is one local scale factor s( I, RHS ) per block row I = 1, ..., NBA
*  per right-hand side column RHS = 1, ..., NRHS. The global scale factor
*  SCALE( RHS ) is chosen as the smallest local scale factor s( I, RHS )
*  I = 1, ..., NBA.
*  A triangular solve op(A( J, J )) * x( J, RHS ) = SCALOC * b( J, RHS )
*  updates the local scale factor s( J, RHS ) := s( J, RHS ) * SCALOC. The
*  linear update of potentially inconsistently scaled vector segments
*     s( I, RHS ) * b( I, RHS ) - op(A( I, J )) * ( s( J, RHS )* x( J, RHS ) )
*  computes a consistent scaling SCAMIN = MIN( s(I, RHS ), s(J, RHS) ) and,
*  if necessary, rescales the blocks prior to calling GEMM.
*
*  \endverbatim
*  =====================================================================
*  References:
*  C. C. Kjelgaard Mikkelsen, A. B. Schwarz and L. Karlsson (2019).
*  Parallel robust solution of triangular linear systems. Concurrency
*  and Computation: Practice and Experience, 31(19), e5064.
*
*  Contributor:
*   Angelika Schwarz, Umea University, Sweden.
*
*  =====================================================================
      SUBROUTINE ZLATRS3( UPLO, TRANS, DIAG, NORMIN, N, NRHS, A, LDA,
     $                    X, LDX, SCALE, CNORM, WORK, LWORK, INFO )
      IMPLICIT NONE
*
*     .. Scalar Arguments ..
      CHARACTER          DIAG, TRANS, NORMIN, UPLO
      INTEGER            INFO, LDA, LWORK, LDX, N, NRHS
*     ..
*     .. Array Arguments ..
      COMPLEX*16         A( LDA, * ), X( LDX, * )
      DOUBLE PRECISION   CNORM( * ), SCALE( * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
      COMPLEX*16         CZERO, CONE
      PARAMETER          ( CONE = ( 1.0D+0, 0.0D+0 ) )
      PARAMETER          ( CZERO = ( 0.0D+0, 0.0D+0 ) )
      INTEGER            NBMAX, NBMIN, NBRHS, NRHSMIN
      PARAMETER          ( NRHSMIN = 2, NBRHS = 32 )
      PARAMETER          ( NBMIN = 8, NBMAX = 64 )
*     ..
*     .. Local Arrays ..
      DOUBLE PRECISION   W( NBMAX ), XNRM( NBRHS )
*     ..
*     .. Local Scalars ..
      LOGICAL            LQUERY, NOTRAN, NOUNIT, UPPER
      INTEGER            AWRK, I, IFIRST, IINC, ILAST, II, I1, I2, J,
     $                   JFIRST, JINC, JLAST, J1, J2, K, KK, K1, K2,
     $                   LANRM, LDS, LSCALE, NB, NBA, NBX, RHS, LWMIN
      DOUBLE PRECISION   ANRM, BIGNUM, BNRM, RSCAL, SCAL, SCALOC,
     $                   SCAMIN, SMLNUM, TMAX
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ILAENV
      DOUBLE PRECISION   DLAMCH, ZLANGE, DLARMM
      EXTERNAL           ILAENV, LSAME, DLAMCH, ZLANGE, DLARMM
*     ..
*     .. External Subroutines ..
      EXTERNAL           ZLATRS, ZDSCAL, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, MIN
*     ..
*     .. Executable Statements ..
*
      INFO = 0
      UPPER = LSAME( UPLO, 'U' )
      NOTRAN = LSAME( TRANS, 'N' )
      NOUNIT = LSAME( DIAG, 'N' )
      LQUERY = ( LWORK.EQ.-1 )
*
*     Partition A and X into blocks.
*
      NB = MAX( NBMIN, ILAENV( 1, 'ZLATRS', '', N, N, -1, -1 ) )
      NB = MIN( NBMAX, NB )
      NBA = MAX( 1, (N + NB - 1) / NB )
      NBX = MAX( 1, (NRHS + NBRHS - 1) / NBRHS )
*
*     Compute the workspace
*
*     The workspace comprises two parts.
*     The first part stores the local scale factors. Each simultaneously
*     computed right-hand side requires one local scale factor per block
*     row. WORK( I + KK * LDS ) is the scale factor of the vector
*     segment associated with the I-th block row and the KK-th vector
*     in the block column.
*
      LSCALE = NBA * MAX( NBA, MIN( NRHS, NBRHS ) )
      LDS = NBA
*
*     The second part stores upper bounds of the triangular A. There are
*     a total of NBA x NBA blocks, of which only the upper triangular
*     part or the lower triangular part is referenced. The upper bound of
*     the block A( I, J ) is stored as WORK( AWRK + I + J * NBA ).
*
      LANRM = NBA * NBA
      AWRK = LSCALE
*
      IF( MIN( N, NRHS ).EQ.0 ) THEN
         LWMIN = 1
      ELSE
         LWMIN = LSCALE + LANRM
      END IF
      WORK( 1 ) = LWMIN
*
*     Test the input parameters.
*
      IF( .NOT.UPPER .AND. .NOT.LSAME( UPLO, 'L' ) ) THEN
         INFO = -1
      ELSE IF( .NOT.NOTRAN .AND. .NOT.LSAME( TRANS, 'T' ) .AND. .NOT.
     $         LSAME( TRANS, 'C' ) ) THEN
         INFO = -2
      ELSE IF( .NOT.NOUNIT .AND. .NOT.LSAME( DIAG, 'U' ) ) THEN
         INFO = -3
      ELSE IF( .NOT.LSAME( NORMIN, 'Y' ) .AND. .NOT.
     $         LSAME( NORMIN, 'N' ) ) THEN
         INFO = -4
      ELSE IF( N.LT.0 ) THEN
         INFO = -5
      ELSE IF( NRHS.LT.0 ) THEN
         INFO = -6
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -8
      ELSE IF( LDX.LT.MAX( 1, N ) ) THEN
         INFO = -10
      ELSE IF( .NOT.LQUERY .AND. LWORK.LT.LWMIN ) THEN
         INFO = -14
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZLATRS3', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         RETURN
      END IF
*
*     Initialize scaling factors
*
      DO KK = 1, NRHS
         SCALE( KK ) = ONE
      END DO
*
*     Quick return if possible
*
      IF( MIN( N, NRHS ).EQ.0 )
     $   RETURN
*
*     Determine machine dependent constant to control overflow.
*
      BIGNUM = DLAMCH( 'Overflow' )
      SMLNUM = DLAMCH( 'Safe Minimum' )
*
*     Use unblocked code for small problems
*
      IF( NRHS.LT.NRHSMIN ) THEN
         CALL ZLATRS( UPLO, TRANS, DIAG, NORMIN, N, A, LDA, X( 1, 1),
     $                SCALE( 1 ), CNORM, INFO )
         DO K = 2, NRHS
            CALL ZLATRS( UPLO, TRANS, DIAG, 'Y', N, A, LDA, X( 1, K ),
     $                   SCALE( K ), CNORM, INFO )
         END DO
         RETURN
      END IF
*
*     Compute norms of blocks of A excluding diagonal blocks and find
*     the block with the largest norm TMAX.
*
      TMAX = ZERO
      DO J = 1, NBA
         J1 = (J-1)*NB + 1
         J2 = MIN( J*NB, N ) + 1
         IF ( UPPER ) THEN
            IFIRST = 1
            ILAST = J - 1
         ELSE
            IFIRST = J + 1
            ILAST = NBA
         END IF
         DO I = IFIRST, ILAST
            I1 = (I-1)*NB + 1
            I2 = MIN( I*NB, N ) + 1
*
*           Compute upper bound of A( I1:I2-1, J1:J2-1 ).
*
            IF( NOTRAN ) THEN
               ANRM = ZLANGE( 'I', I2-I1, J2-J1, A( I1, J1 ), LDA, W )
               WORK( AWRK + I+(J-1)*NBA ) = ANRM
            ELSE
               ANRM = ZLANGE( '1', I2-I1, J2-J1, A( I1, J1 ), LDA, W )
               WORK( AWRK + J+(I-1) * NBA ) = ANRM
            END IF
            TMAX = MAX( TMAX, ANRM )
         END DO
      END DO
*
      IF( .NOT. TMAX.LE.DLAMCH('Overflow') ) THEN
*
*        Some matrix entries have huge absolute value. At least one upper
*        bound norm( A(I1:I2-1, J1:J2-1), 'I') is not a valid floating-point
*        number, either due to overflow in LANGE or due to Inf in A.
*        Fall back to LATRS. Set normin = 'N' for every right-hand side to
*        force computation of TSCAL in LATRS to avoid the likely overflow
*        in the computation of the column norms CNORM.
*
         DO K = 1, NRHS
            CALL ZLATRS( UPLO, TRANS, DIAG, 'N', N, A, LDA, X( 1, K ),
     $                   SCALE( K ), CNORM, INFO )
         END DO
         RETURN
      END IF
*
*     Every right-hand side requires workspace to store NBA local scale
*     factors. To save workspace, X is computed successively in block columns
*     of width NBRHS, requiring a total of NBA x NBRHS space. If sufficient
*     workspace is available, larger values of NBRHS or NBRHS = NRHS are viable.
      DO K = 1, NBX
*        Loop over block columns (index = K) of X and, for column-wise scalings,
*        over individual columns (index = KK).
*        K1: column index of the first column in X( J, K )
*        K2: column index of the first column in X( J, K+1 )
*        so the K2 - K1 is the column count of the block X( J, K )
         K1 = (K-1)*NBRHS + 1
         K2 = MIN( K*NBRHS, NRHS ) + 1
*
*        Initialize local scaling factors of current block column X( J, K )
*
         DO KK = 1, K2 - K1
            DO I = 1, NBA
               WORK( I+KK*LDS ) = ONE
            END DO
         END DO
*
         IF( NOTRAN ) THEN
*
*           Solve A * X(:, K1:K2-1) = B * diag(scale(K1:K2-1))
*
            IF( UPPER ) THEN
               JFIRST = NBA
               JLAST = 1
               JINC = -1
            ELSE
               JFIRST = 1
               JLAST = NBA
               JINC = 1
            END IF
         ELSE
*
*           Solve op(A) * X(:, K1:K2-1) = B * diag(scale(K1:K2-1))
*           where op(A) = A**T or op(A) = A**H
*
            IF( UPPER ) THEN
               JFIRST = 1
               JLAST = NBA
               JINC = 1
            ELSE
               JFIRST = NBA
               JLAST = 1
               JINC = -1
            END IF
         END IF

         DO J = JFIRST, JLAST, JINC
*           J1: row index of the first row in A( J, J )
*           J2: row index of the first row in A( J+1, J+1 )
*           so that J2 - J1 is the row count of the block A( J, J )
            J1 = (J-1)*NB + 1
            J2 = MIN( J*NB, N ) + 1
*
*           Solve op(A( J, J )) * X( J, RHS ) = SCALOC * B( J, RHS )
*
            DO KK = 1, K2 - K1
               RHS = K1 + KK - 1
               IF( KK.EQ.1 ) THEN
                  CALL ZLATRS( UPLO, TRANS, DIAG, 'N', J2-J1,
     $                         A( J1, J1 ), LDA, X( J1, RHS ),
     $                         SCALOC, CNORM, INFO )
               ELSE
                  CALL ZLATRS( UPLO, TRANS, DIAG, 'Y', J2-J1,
     $                         A( J1, J1 ), LDA, X( J1, RHS ),
     $                         SCALOC, CNORM, INFO )
               END IF
*              Find largest absolute value entry in the vector segment
*              X( J1:J2-1, RHS ) as an upper bound for the worst case
*              growth in the linear updates.
               XNRM( KK ) = ZLANGE( 'I', J2-J1, 1, X( J1, RHS ),
     $                              LDX, W )
*
               IF( SCALOC .EQ. ZERO ) THEN
*                 LATRS found that A is singular through A(j,j) = 0.
*                 Reset the computation x(1:n) = 0, x(j) = 1, SCALE = 0
*                 and compute op(A)*x = 0. Note that X(J1:J2-1, KK) is
*                 set by LATRS.
                  SCALE( RHS ) = ZERO
                  DO II = 1, J1-1
                     X( II, KK ) = CZERO
                  END DO
                  DO II = J2, N
                     X( II, KK ) = CZERO
                  END DO
*                 Discard the local scale factors.
                  DO II = 1, NBA
                     WORK( II+KK*LDS ) = ONE
                  END DO
                  SCALOC = ONE
               ELSE IF( SCALOC*WORK( J+KK*LDS ) .EQ. ZERO ) THEN
*                 LATRS computed a valid scale factor, but combined with
*                 the current scaling the solution does not have a
*                 scale factor > 0.
*
*                 Set WORK( J+KK*LDS ) to smallest valid scale
*                 factor and increase SCALOC accordingly.
                  SCAL = WORK( J+KK*LDS ) / SMLNUM
                  SCALOC = SCALOC * SCAL
                  WORK( J+KK*LDS ) = SMLNUM
*                 If LATRS overestimated the growth, x may be
*                 rescaled to preserve a valid combined scale
*                 factor WORK( J, KK ) > 0.
                  RSCAL = ONE / SCALOC
                  IF( XNRM( KK )*RSCAL .LE. BIGNUM ) THEN
                     XNRM( KK ) = XNRM( KK ) * RSCAL
                     CALL ZDSCAL( J2-J1, RSCAL, X( J1, RHS ), 1 )
                     SCALOC = ONE
                  ELSE
*                    The system op(A) * x = b is badly scaled and its
*                    solution cannot be represented as (1/scale) * x.
*                    Set x to zero. This approach deviates from LATRS
*                    where a completely meaningless non-zero vector
*                    is returned that is not a solution to op(A) * x = b.
                     SCALE( RHS ) = ZERO
                     DO II = 1, N
                        X( II, KK ) = CZERO
                     END DO
*                    Discard the local scale factors.
                     DO II = 1, NBA
                        WORK( II+KK*LDS ) = ONE
                     END DO
                     SCALOC = ONE
                  END IF
               END IF
               SCALOC = SCALOC * WORK( J+KK*LDS )
               WORK( J+KK*LDS ) = SCALOC
            END DO
*
*           Linear block updates
*
            IF( NOTRAN ) THEN
               IF( UPPER ) THEN
                  IFIRST = J - 1
                  ILAST = 1
                  IINC = -1
               ELSE
                  IFIRST = J + 1
                  ILAST = NBA
                  IINC = 1
               END IF
            ELSE
               IF( UPPER ) THEN
                  IFIRST = J + 1
                  ILAST = NBA
                  IINC = 1
               ELSE
                  IFIRST = J - 1
                  ILAST = 1
                  IINC = -1
               END IF
            END IF
*
            DO I = IFIRST, ILAST, IINC
*              I1: row index of the first column in X( I, K )
*              I2: row index of the first column in X( I+1, K )
*              so the I2 - I1 is the row count of the block X( I, K )
               I1 = (I-1)*NB + 1
               I2 = MIN( I*NB, N ) + 1
*
*              Prepare the linear update to be executed with GEMM.
*              For each column, compute a consistent scaling, a
*              scaling factor to survive the linear update, and
*              rescale the column segments, if necessary. Then
*              the linear update is safely executed.
*
               DO KK = 1, K2 - K1
                  RHS = K1 + KK - 1
*                 Compute consistent scaling
                  SCAMIN = MIN( WORK( I+KK*LDS), WORK( J+KK*LDS ) )
*
*                 Compute scaling factor to survive the linear update
*                 simulating consistent scaling.
*
                  BNRM = ZLANGE( 'I', I2-I1, 1, X( I1, RHS ), LDX, W )
                  BNRM = BNRM*( SCAMIN / WORK( I+KK*LDS ) )
                  XNRM( KK ) = XNRM( KK )*( SCAMIN / WORK( J+KK*LDS) )
                  ANRM = WORK( AWRK + I+(J-1)*NBA )
                  SCALOC = DLARMM( ANRM, XNRM( KK ), BNRM )
*
*                 Simultaneously apply the robust update factor and the
*                 consistency scaling factor to X( I, KK ) and X( J, KK ).
*
                  SCAL = ( SCAMIN / WORK( I+KK*LDS) )*SCALOC
                  IF( SCAL.NE.ONE ) THEN
                     CALL ZDSCAL( I2-I1, SCAL, X( I1, RHS ), 1 )
                     WORK( I+KK*LDS ) = SCAMIN*SCALOC
                  END IF
*
                  SCAL = ( SCAMIN / WORK( J+KK*LDS ) )*SCALOC
                  IF( SCAL.NE.ONE ) THEN
                     CALL ZDSCAL( J2-J1, SCAL, X( J1, RHS ), 1 )
                     WORK( J+KK*LDS ) = SCAMIN*SCALOC
                  END IF
               END DO
*
               IF( NOTRAN ) THEN
*
*                 B( I, K ) := B( I, K ) - A( I, J ) * X( J, K )
*
                  CALL ZGEMM( 'N', 'N', I2-I1, K2-K1, J2-J1, -CONE,
     $                        A( I1, J1 ), LDA, X( J1, K1 ), LDX,
     $                        CONE, X( I1, K1 ), LDX )
               ELSE IF( LSAME( TRANS, 'T' ) ) THEN
*
*                 B( I, K ) := B( I, K ) - A( I, J )**T * X( J, K )
*
                  CALL ZGEMM( 'T', 'N', I2-I1, K2-K1, J2-J1, -CONE,
     $                        A( J1, I1 ), LDA, X( J1, K1 ), LDX,
     $                        CONE, X( I1, K1 ), LDX )
               ELSE
*
*                 B( I, K ) := B( I, K ) - A( I, J )**H * X( J, K )
*
                  CALL ZGEMM( 'C', 'N', I2-I1, K2-K1, J2-J1, -CONE,
     $                        A( J1, I1 ), LDA, X( J1, K1 ), LDX,
     $                        CONE, X( I1, K1 ), LDX )
               END IF
            END DO
         END DO

*
*        Reduce local scaling factors
*
         DO KK = 1, K2 - K1
            RHS = K1 + KK - 1
            DO I = 1, NBA
               SCALE( RHS ) = MIN( SCALE( RHS ), WORK( I+KK*LDS ) )
            END DO
         END DO
*
*        Realize consistent scaling
*
         DO KK = 1, K2 - K1
            RHS = K1 + KK - 1
            IF( SCALE( RHS ).NE.ONE .AND. SCALE( RHS ).NE. ZERO ) THEN
               DO I = 1, NBA
                  I1 = (I - 1) * NB + 1
                  I2 = MIN( I * NB, N ) + 1
                  SCAL = SCALE( RHS ) / WORK( I+KK*LDS )
                  IF( SCAL.NE.ONE )
     $               CALL ZDSCAL( I2-I1, SCAL, X( I1, RHS ), 1 )
               END DO
            END IF
         END DO
      END DO
      RETURN
*
*     End of ZLATRS3
*
      END
