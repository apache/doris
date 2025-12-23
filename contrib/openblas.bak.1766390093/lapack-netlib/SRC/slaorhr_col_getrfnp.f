*> \brief \b SLAORHR_COL_GETRFNP
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLAORHR_COL_GETRFNP + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slaorhr_col_getrfnp.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slaorhr_col_getrfnp.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slaorhr_col_getrfnp.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLAORHR_COL_GETRFNP( M, N, A, LDA, D, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, LDA, M, N
*       ..
*       .. Array Arguments ..
*       REAL               A( LDA, * ), D( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SLAORHR_COL_GETRFNP computes the modified LU factorization without
*> pivoting of a real general M-by-N matrix A. The factorization has
*> the form:
*>
*>     A - S = L * U,
*>
*> where:
*>    S is a m-by-n diagonal sign matrix with the diagonal D, so that
*>    D(i) = S(i,i), 1 <= i <= min(M,N). The diagonal D is constructed
*>    as D(i)=-SIGN(A(i,i)), where A(i,i) is the value after performing
*>    i-1 steps of Gaussian elimination. This means that the diagonal
*>    element at each step of "modified" Gaussian elimination is
*>    at least one in absolute value (so that division-by-zero not
*>    not possible during the division by the diagonal element);
*>
*>    L is a M-by-N lower triangular matrix with unit diagonal elements
*>    (lower trapezoidal if M > N);
*>
*>    and U is a M-by-N upper triangular matrix
*>    (upper trapezoidal if M < N).
*>
*> This routine is an auxiliary routine used in the Householder
*> reconstruction routine SORHR_COL. In SORHR_COL, this routine is
*> applied to an M-by-N matrix A with orthonormal columns, where each
*> element is bounded by one in absolute value. With the choice of
*> the matrix S above, one can show that the diagonal element at each
*> step of Gaussian elimination is the largest (in absolute value) in
*> the column on or below the diagonal, so that no pivoting is required
*> for numerical stability [1].
*>
*> For more details on the Householder reconstruction algorithm,
*> including the modified LU factorization, see [1].
*>
*> This is the blocked right-looking version of the algorithm,
*> calling Level 3 BLAS to update the submatrix. To factorize a block,
*> this routine calls the recursive routine SLAORHR_COL_GETRFNP2.
*>
*> [1] "Reconstructing Householder vectors from tall-skinny QR",
*>     G. Ballard, J. Demmel, L. Grigori, M. Jacquelin, H.D. Nguyen,
*>     E. Solomonik, J. Parallel Distrib. Comput.,
*>     vol. 85, pp. 3-31, 2015.
*> \endverbatim
*
*  Arguments:
*  ==========
*
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
*> \param[in,out] A
*> \verbatim
*>          A is REAL array, dimension (LDA,N)
*>          On entry, the M-by-N matrix to be factored.
*>          On exit, the factors L and U from the factorization
*>          A-S=L*U; the unit diagonal elements of L are not stored.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,M).
*> \endverbatim
*>
*> \param[out] D
*> \verbatim
*>          D is REAL array, dimension min(M,N)
*>          The diagonal elements of the diagonal M-by-N sign matrix S,
*>          D(i) = S(i,i), where 1 <= i <= min(M,N). The elements can
*>          be only plus or minus one.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value
*> \endverbatim
*>
*  Authors:
*  ========
*
*> \author Univ. of Tennessee
*> \author Univ. of California Berkeley
*> \author Univ. of Colorado Denver
*> \author NAG Ltd.
*
*> \ingroup realGEcomputational
*
*> \par Contributors:
*  ==================
*>
*> \verbatim
*>
*> November 2019, Igor Kozachenko,
*>                Computer Science Division,
*>                University of California, Berkeley
*>
*> \endverbatim
*
*  =====================================================================
      SUBROUTINE SLAORHR_COL_GETRFNP( M, N, A, LDA, D, INFO )
      IMPLICIT NONE
*
*  -- LAPACK computational routine --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*
*     .. Scalar Arguments ..
      INTEGER            INFO, LDA, M, N
*     ..
*     .. Array Arguments ..
      REAL               A( LDA, * ), D( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ONE
      PARAMETER          ( ONE = 1.0E+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            IINFO, J, JB, NB
*     ..
*     .. External Subroutines ..
      EXTERNAL           SGEMM, SLAORHR_COL_GETRFNP2, STRSM, XERBLA
*     ..
*     .. External Functions ..
      INTEGER            ILAENV
      EXTERNAL           ILAENV
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
      IF( M.LT.0 ) THEN
         INFO = -1
      ELSE IF( N.LT.0 ) THEN
         INFO = -2
      ELSE IF( LDA.LT.MAX( 1, M ) ) THEN
         INFO = -4
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'SLAORHR_COL_GETRFNP', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( MIN( M, N ).EQ.0 )
     $   RETURN
*
*     Determine the block size for this environment.
*

      NB = ILAENV( 1, 'SLAORHR_COL_GETRFNP', ' ', M, N, -1, -1 )

      IF( NB.LE.1 .OR. NB.GE.MIN( M, N ) ) THEN
*
*        Use unblocked code.
*
         CALL SLAORHR_COL_GETRFNP2( M, N, A, LDA, D, INFO )
      ELSE
*
*        Use blocked code.
*
         DO J = 1, MIN( M, N ), NB
            JB = MIN( MIN( M, N )-J+1, NB )
*
*           Factor diagonal and subdiagonal blocks.
*
            CALL SLAORHR_COL_GETRFNP2( M-J+1, JB, A( J, J ), LDA,
     $                                 D( J ), IINFO )
*
            IF( J+JB.LE.N ) THEN
*
*              Compute block row of U.
*
               CALL STRSM( 'Left', 'Lower', 'No transpose', 'Unit', JB,
     $                     N-J-JB+1, ONE, A( J, J ), LDA, A( J, J+JB ),
     $                     LDA )
               IF( J+JB.LE.M ) THEN
*
*                 Update trailing submatrix.
*
                  CALL SGEMM( 'No transpose', 'No transpose', M-J-JB+1,
     $                        N-J-JB+1, JB, -ONE, A( J+JB, J ), LDA,
     $                        A( J, J+JB ), LDA, ONE, A( J+JB, J+JB ),
     $                        LDA )
               END IF
            END IF
         END DO
      END IF
      RETURN
*
*     End of SLAORHR_COL_GETRFNP
*
      END