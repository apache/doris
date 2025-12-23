*> \brief \b SLAORHR_COL_GETRFNP2
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DLAORHR_GETRF2NP + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slaorhr_col_getrfnp2.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slaorhr_col_getrfnp2.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slaorhr_col_getrfnp2.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       RECURSIVE SUBROUTINE SLAORHR_COL_GETRFNP2( M, N, A, LDA, D, INFO )
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
*> SLAORHR_COL_GETRFNP2 computes the modified LU factorization without
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
*>    element at each step of "modified" Gaussian elimination is at
*>    least one in absolute value (so that division-by-zero not
*>    possible during the division by the diagonal element);
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
*> This is the recursive version of the LU factorization algorithm.
*> Denote A - S by B. The algorithm divides the matrix B into four
*> submatrices:
*>
*>        [  B11 | B12  ]  where B11 is n1 by n1,
*>    B = [ -----|----- ]        B21 is (m-n1) by n1,
*>        [  B21 | B22  ]        B12 is n1 by n2,
*>                               B22 is (m-n1) by n2,
*>                               with n1 = min(m,n)/2, n2 = n-n1.
*>
*>
*> The subroutine calls itself to factor B11, solves for B21,
*> solves for B12, updates B22, then calls itself to factor B22.
*>
*> For more details on the recursive LU algorithm, see [2].
*>
*> SLAORHR_COL_GETRFNP2 is called to factorize a block by the blocked
*> routine SLAORHR_COL_GETRFNP, which uses blocked code calling
*> Level 3 BLAS to update the submatrix. However, SLAORHR_COL_GETRFNP2
*> is self-sufficient and can be used without SLAORHR_COL_GETRFNP.
*>
*> [1] "Reconstructing Householder vectors from tall-skinny QR",
*>     G. Ballard, J. Demmel, L. Grigori, M. Jacquelin, H.D. Nguyen,
*>     E. Solomonik, J. Parallel Distrib. Comput.,
*>     vol. 85, pp. 3-31, 2015.
*>
*> [2] "Recursion leads to automatic variable blocking for dense linear
*>     algebra algorithms", F. Gustavson, IBM J. of Res. and Dev.,
*>     vol. 41, no. 6, pp. 737-755, 1997.
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
      RECURSIVE SUBROUTINE SLAORHR_COL_GETRFNP2( M, N, A, LDA, D, INFO )
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
      REAL               SFMIN
      INTEGER            I, IINFO, N1, N2
*     ..
*     .. External Functions ..
      REAL               SLAMCH
      EXTERNAL           SLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           SGEMM, SSCAL, STRSM, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, SIGN, MAX, MIN
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters
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
         CALL XERBLA( 'SLAORHR_COL_GETRFNP2', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( MIN( M, N ).EQ.0 )
     $   RETURN

      IF ( M.EQ.1 ) THEN
*
*        One row case, (also recursion termination case),
*        use unblocked code
*
*        Transfer the sign
*
         D( 1 ) = -SIGN( ONE, A( 1, 1 ) )
*
*        Construct the row of U
*
         A( 1, 1 ) = A( 1, 1 ) - D( 1 )
*
      ELSE IF( N.EQ.1 ) THEN
*
*        One column case, (also recursion termination case),
*        use unblocked code
*
*        Transfer the sign
*
         D( 1 ) = -SIGN( ONE, A( 1, 1 ) )
*
*        Construct the row of U
*
         A( 1, 1 ) = A( 1, 1 ) - D( 1 )
*
*        Scale the elements 2:M of the column
*
*        Determine machine safe minimum
*
         SFMIN = SLAMCH('S')
*
*        Construct the subdiagonal elements of L
*
         IF( ABS( A( 1, 1 ) ) .GE. SFMIN ) THEN
            CALL SSCAL( M-1, ONE / A( 1, 1 ), A( 2, 1 ), 1 )
         ELSE
            DO I = 2, M
               A( I, 1 ) = A( I, 1 ) / A( 1, 1 )
            END DO
         END IF
*
      ELSE
*
*        Divide the matrix B into four submatrices
*
         N1 = MIN( M, N ) / 2
         N2 = N-N1

*
*        Factor B11, recursive call
*
         CALL SLAORHR_COL_GETRFNP2( N1, N1, A, LDA, D, IINFO )
*
*        Solve for B21
*
         CALL STRSM( 'R', 'U', 'N', 'N', M-N1, N1, ONE, A, LDA,
     $               A( N1+1, 1 ), LDA )
*
*        Solve for B12
*
         CALL STRSM( 'L', 'L', 'N', 'U', N1, N2, ONE, A, LDA,
     $               A( 1, N1+1 ), LDA )
*
*        Update B22, i.e. compute the Schur complement
*        B22 := B22 - B21*B12
*
         CALL SGEMM( 'N', 'N', M-N1, N2, N1, -ONE, A( N1+1, 1 ), LDA,
     $               A( 1, N1+1 ), LDA, ONE, A( N1+1, N1+1 ), LDA )
*
*        Factor B22, recursive call
*
         CALL SLAORHR_COL_GETRFNP2( M-N1, N2, A( N1+1, N1+1 ), LDA,
     $                          D( N1+1 ), IINFO )
*
      END IF
      RETURN
*
*     End of SLAORHR_COL_GETRFNP2
*
      END
