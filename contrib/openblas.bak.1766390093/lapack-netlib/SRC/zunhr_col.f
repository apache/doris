*> \brief \b ZUNHR_COL
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZUNHR_COL + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zunhr_col.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zunhr_col.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zunhr_col.f">
*> [TXT]</a
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZUNHR_COL( M, N, NB, A, LDA, T, LDT, D, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER           INFO, LDA, LDT, M, N, NB
*       ..
*       .. Array Arguments ..
*       COMPLEX*16        A( LDA, * ), D( * ), T( LDT, * )
*       ..
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>  ZUNHR_COL takes an M-by-N complex matrix Q_in with orthonormal columns
*>  as input, stored in A, and performs Householder Reconstruction (HR),
*>  i.e. reconstructs Householder vectors V(i) implicitly representing
*>  another M-by-N matrix Q_out, with the property that Q_in = Q_out*S,
*>  where S is an N-by-N diagonal matrix with diagonal entries
*>  equal to +1 or -1. The Householder vectors (columns V(i) of V) are
*>  stored in A on output, and the diagonal entries of S are stored in D.
*>  Block reflectors are also returned in T
*>  (same output format as ZGEQRT).
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the matrix A. M >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrix A. M >= N >= 0.
*> \endverbatim
*>
*> \param[in] NB
*> \verbatim
*>          NB is INTEGER
*>          The column block size to be used in the reconstruction
*>          of Householder column vector blocks in the array A and
*>          corresponding block reflectors in the array T. NB >= 1.
*>          (Note that if NB > N, then N is used instead of NB
*>          as the column block size.)
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA,N)
*>
*>          On entry:
*>
*>             The array A contains an M-by-N orthonormal matrix Q_in,
*>             i.e the columns of A are orthogonal unit vectors.
*>
*>          On exit:
*>
*>             The elements below the diagonal of A represent the unit
*>             lower-trapezoidal matrix V of Householder column vectors
*>             V(i). The unit diagonal entries of V are not stored
*>             (same format as the output below the diagonal in A from
*>             ZGEQRT). The matrix T and the matrix V stored on output
*>             in A implicitly define Q_out.
*>
*>             The elements above the diagonal contain the factor U
*>             of the "modified" LU-decomposition:
*>                Q_in - ( S ) = V * U
*>                       ( 0 )
*>             where 0 is a (M-N)-by-(M-N) zero matrix.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,M).
*> \endverbatim
*>
*> \param[out] T
*> \verbatim
*>          T is COMPLEX*16 array,
*>          dimension (LDT, N)
*>
*>          Let NOCB = Number_of_output_col_blocks
*>                   = CEIL(N/NB)
*>
*>          On exit, T(1:NB, 1:N) contains NOCB upper-triangular
*>          block reflectors used to define Q_out stored in compact
*>          form as a sequence of upper-triangular NB-by-NB column
*>          blocks (same format as the output T in ZGEQRT).
*>          The matrix T and the matrix V stored on output in A
*>          implicitly define Q_out. NOTE: The lower triangles
*>          below the upper-triangular blocks will be filled with
*>          zeros. See Further Details.
*> \endverbatim
*>
*> \param[in] LDT
*> \verbatim
*>          LDT is INTEGER
*>          The leading dimension of the array T.
*>          LDT >= max(1,min(NB,N)).
*> \endverbatim
*>
*> \param[out] D
*> \verbatim
*>          D is COMPLEX*16 array, dimension min(M,N).
*>          The elements can be only plus or minus one.
*>
*>          D(i) is constructed as D(i) = -SIGN(Q_in_i(i,i)), where
*>          1 <= i <= min(M,N), and Q_in_i is Q_in after performing
*>          i-1 steps of “modified” Gaussian elimination.
*>          See Further Details.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value
*> \endverbatim
*>
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*> The computed M-by-M unitary factor Q_out is defined implicitly as
*> a product of unitary matrices Q_out(i). Each Q_out(i) is stored in
*> the compact WY-representation format in the corresponding blocks of
*> matrices V (stored in A) and T.
*>
*> The M-by-N unit lower-trapezoidal matrix V stored in the M-by-N
*> matrix A contains the column vectors V(i) in NB-size column
*> blocks VB(j). For example, VB(1) contains the columns
*> V(1), V(2), ... V(NB). NOTE: The unit entries on
*> the diagonal of Y are not stored in A.
*>
*> The number of column blocks is
*>
*>     NOCB = Number_of_output_col_blocks = CEIL(N/NB)
*>
*> where each block is of order NB except for the last block, which
*> is of order LAST_NB = N - (NOCB-1)*NB.
*>
*> For example, if M=6,  N=5 and NB=2, the matrix V is
*>
*>
*>     V = (    VB(1),   VB(2), VB(3) ) =
*>
*>       = (   1                      )
*>         ( v21    1                 )
*>         ( v31  v32    1            )
*>         ( v41  v42  v43   1        )
*>         ( v51  v52  v53  v54    1  )
*>         ( v61  v62  v63  v54   v65 )
*>
*>
*> For each of the column blocks VB(i), an upper-triangular block
*> reflector TB(i) is computed. These blocks are stored as
*> a sequence of upper-triangular column blocks in the NB-by-N
*> matrix T. The size of each TB(i) block is NB-by-NB, except
*> for the last block, whose size is LAST_NB-by-LAST_NB.
*>
*> For example, if M=6,  N=5 and NB=2, the matrix T is
*>
*>     T  = (    TB(1),    TB(2), TB(3) ) =
*>
*>        = ( t11  t12  t13  t14   t15  )
*>          (      t22       t24        )
*>
*>
*> The M-by-M factor Q_out is given as a product of NOCB
*> unitary M-by-M matrices Q_out(i).
*>
*>     Q_out = Q_out(1) * Q_out(2) * ... * Q_out(NOCB),
*>
*> where each matrix Q_out(i) is given by the WY-representation
*> using corresponding blocks from the matrices V and T:
*>
*>     Q_out(i) = I - VB(i) * TB(i) * (VB(i))**T,
*>
*> where I is the identity matrix. Here is the formula with matrix
*> dimensions:
*>
*>  Q(i){M-by-M} = I{M-by-M} -
*>    VB(i){M-by-INB} * TB(i){INB-by-INB} * (VB(i))**T {INB-by-M},
*>
*> where INB = NB, except for the last block NOCB
*> for which INB=LAST_NB.
*>
*> =====
*> NOTE:
*> =====
*>
*> If Q_in is the result of doing a QR factorization
*> B = Q_in * R_in, then:
*>
*> B = (Q_out*S) * R_in = Q_out * (S * R_in) = Q_out * R_out.
*>
*> So if one wants to interpret Q_out as the result
*> of the QR factorization of B, then the corresponding R_out
*> should be equal to R_out = S * R_in, i.e. some rows of R_in
*> should be multiplied by -1.
*>
*> For the details of the algorithm, see [1].
*>
*> [1] "Reconstructing Householder vectors from tall-skinny QR",
*>     G. Ballard, J. Demmel, L. Grigori, M. Jacquelin, H.D. Nguyen,
*>     E. Solomonik, J. Parallel Distrib. Comput.,
*>     vol. 85, pp. 3-31, 2015.
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
*> \ingroup complex16OTHERcomputational
*
*> \par Contributors:
*  ==================
*>
*> \verbatim
*>
*> November   2019, Igor Kozachenko,
*>            Computer Science Division,
*>            University of California, Berkeley
*>
*> \endverbatim
*
*  =====================================================================
      SUBROUTINE ZUNHR_COL( M, N, NB, A, LDA, T, LDT, D, INFO )
      IMPLICIT NONE
*
*  -- LAPACK computational routine --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*
*     .. Scalar Arguments ..
      INTEGER           INFO, LDA, LDT, M, N, NB
*     ..
*     .. Array Arguments ..
      COMPLEX*16        A( LDA, * ), D( * ), T( LDT, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      COMPLEX*16         CONE, CZERO
      PARAMETER          ( CONE = ( 1.0D+0, 0.0D+0 ),
     $                     CZERO = ( 0.0D+0, 0.0D+0 ) )
*     ..
*     .. Local Scalars ..
      INTEGER            I, IINFO, J, JB, JBTEMP1, JBTEMP2, JNB,
     $                   NPLUSONE
*     ..
*     .. External Subroutines ..
      EXTERNAL           ZCOPY, ZLAUNHR_COL_GETRFNP, ZSCAL, ZTRSM,
     $                   XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters
*
      INFO = 0
      IF( M.LT.0 ) THEN
         INFO = -1
      ELSE IF( N.LT.0 .OR. N.GT.M ) THEN
         INFO = -2
      ELSE IF( NB.LT.1 ) THEN
         INFO = -3
      ELSE IF( LDA.LT.MAX( 1, M ) ) THEN
         INFO = -5
      ELSE IF( LDT.LT.MAX( 1, MIN( NB, N ) ) ) THEN
         INFO = -7
      END IF
*
*     Handle error in the input parameters.
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZUNHR_COL', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( MIN( M, N ).EQ.0 ) THEN
         RETURN
      END IF
*
*     On input, the M-by-N matrix A contains the unitary
*     M-by-N matrix Q_in.
*
*     (1) Compute the unit lower-trapezoidal V (ones on the diagonal
*     are not stored) by performing the "modified" LU-decomposition.
*
*     Q_in - ( S ) = V * U = ( V1 ) * U,
*            ( 0 )           ( V2 )
*
*     where 0 is an (M-N)-by-N zero matrix.
*
*     (1-1) Factor V1 and U.

      CALL ZLAUNHR_COL_GETRFNP( N, N, A, LDA, D, IINFO )
*
*     (1-2) Solve for V2.
*
      IF( M.GT.N ) THEN
         CALL ZTRSM( 'R', 'U', 'N', 'N', M-N, N, CONE, A, LDA,
     $               A( N+1, 1 ), LDA )
      END IF
*
*     (2) Reconstruct the block reflector T stored in T(1:NB, 1:N)
*     as a sequence of upper-triangular blocks with NB-size column
*     blocking.
*
*     Loop over the column blocks of size NB of the array A(1:M,1:N)
*     and the array T(1:NB,1:N), JB is the column index of a column
*     block, JNB is the column block size at each step JB.
*
      NPLUSONE = N + 1
      DO JB = 1, N, NB
*
*        (2-0) Determine the column block size JNB.
*
         JNB = MIN( NPLUSONE-JB, NB )
*
*        (2-1) Copy the upper-triangular part of the current JNB-by-JNB
*        diagonal block U(JB) (of the N-by-N matrix U) stored
*        in A(JB:JB+JNB-1,JB:JB+JNB-1) into the upper-triangular part
*        of the current JNB-by-JNB block T(1:JNB,JB:JB+JNB-1)
*        column-by-column, total JNB*(JNB+1)/2 elements.
*
         JBTEMP1 = JB - 1
         DO J = JB, JB+JNB-1
            CALL ZCOPY( J-JBTEMP1, A( JB, J ), 1, T( 1, J ), 1 )
         END DO
*
*        (2-2) Perform on the upper-triangular part of the current
*        JNB-by-JNB diagonal block U(JB) (of the N-by-N matrix U) stored
*        in T(1:JNB,JB:JB+JNB-1) the following operation in place:
*        (-1)*U(JB)*S(JB), i.e the result will be stored in the upper-
*        triangular part of T(1:JNB,JB:JB+JNB-1). This multiplication
*        of the JNB-by-JNB diagonal block U(JB) by the JNB-by-JNB
*        diagonal block S(JB) of the N-by-N sign matrix S from the
*        right means changing the sign of each J-th column of the block
*        U(JB) according to the sign of the diagonal element of the block
*        S(JB), i.e. S(J,J) that is stored in the array element D(J).
*
         DO J = JB, JB+JNB-1
            IF( D( J ).EQ.CONE ) THEN
               CALL ZSCAL( J-JBTEMP1, -CONE, T( 1, J ), 1 )
            END IF
         END DO
*
*        (2-3) Perform the triangular solve for the current block
*        matrix X(JB):
*
*               X(JB) * (A(JB)**T) = B(JB), where:
*
*               A(JB)**T  is a JNB-by-JNB unit upper-triangular
*                         coefficient block, and A(JB)=V1(JB), which
*                         is a JNB-by-JNB unit lower-triangular block
*                         stored in A(JB:JB+JNB-1,JB:JB+JNB-1).
*                         The N-by-N matrix V1 is the upper part
*                         of the M-by-N lower-trapezoidal matrix V
*                         stored in A(1:M,1:N);
*
*               B(JB)     is a JNB-by-JNB  upper-triangular right-hand
*                         side block, B(JB) = (-1)*U(JB)*S(JB), and
*                         B(JB) is stored in T(1:JNB,JB:JB+JNB-1);
*
*               X(JB)     is a JNB-by-JNB upper-triangular solution
*                         block, X(JB) is the upper-triangular block
*                         reflector T(JB), and X(JB) is stored
*                         in T(1:JNB,JB:JB+JNB-1).
*
*             In other words, we perform the triangular solve for the
*             upper-triangular block T(JB):
*
*               T(JB) * (V1(JB)**T) = (-1)*U(JB)*S(JB).
*
*             Even though the blocks X(JB) and B(JB) are upper-
*             triangular, the routine ZTRSM will access all JNB**2
*             elements of the square T(1:JNB,JB:JB+JNB-1). Therefore,
*             we need to set to zero the elements of the block
*             T(1:JNB,JB:JB+JNB-1) below the diagonal before the call
*             to ZTRSM.
*
*        (2-3a) Set the elements to zero.
*
         JBTEMP2 = JB - 2
         DO J = JB, JB+JNB-2
            DO I = J-JBTEMP2, MIN( NB, N )
               T( I, J ) = CZERO
            END DO
         END DO
*
*        (2-3b) Perform the triangular solve.
*
         CALL ZTRSM( 'R', 'L', 'C', 'U', JNB, JNB, CONE,
     $               A( JB, JB ), LDA, T( 1, JB ), LDT )
*
      END DO
*
      RETURN
*
*     End of ZUNHR_COL
*
      END
