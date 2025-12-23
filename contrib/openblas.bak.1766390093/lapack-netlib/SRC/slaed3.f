*> \brief \b SLAED3 used by SSTEDC. Finds the roots of the secular equation and updates the eigenvectors. Used when the original matrix is tridiagonal.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLAED3 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slaed3.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slaed3.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slaed3.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLAED3( K, N, N1, D, Q, LDQ, RHO, DLAMBDA, Q2, INDX,
*                          CTOT, W, S, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, K, LDQ, N, N1
*       REAL               RHO
*       ..
*       .. Array Arguments ..
*       INTEGER            CTOT( * ), INDX( * )
*       REAL               D( * ), DLAMBDA( * ), Q( LDQ, * ), Q2( * ),
*      $                   S( * ), W( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SLAED3 finds the roots of the secular equation, as defined by the
*> values in D, W, and RHO, between 1 and K.  It makes the
*> appropriate calls to SLAED4 and then updates the eigenvectors by
*> multiplying the matrix of eigenvectors of the pair of eigensystems
*> being combined by the matrix of eigenvectors of the K-by-K system
*> which is solved here.
*>
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] K
*> \verbatim
*>          K is INTEGER
*>          The number of terms in the rational function to be solved by
*>          SLAED4.  K >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of rows and columns in the Q matrix.
*>          N >= K (deflation may result in N>K).
*> \endverbatim
*>
*> \param[in] N1
*> \verbatim
*>          N1 is INTEGER
*>          The location of the last eigenvalue in the leading submatrix.
*>          min(1,N) <= N1 <= N/2.
*> \endverbatim
*>
*> \param[out] D
*> \verbatim
*>          D is REAL array, dimension (N)
*>          D(I) contains the updated eigenvalues for
*>          1 <= I <= K.
*> \endverbatim
*>
*> \param[out] Q
*> \verbatim
*>          Q is REAL array, dimension (LDQ,N)
*>          Initially the first K columns are used as workspace.
*>          On output the columns 1 to K contain
*>          the updated eigenvectors.
*> \endverbatim
*>
*> \param[in] LDQ
*> \verbatim
*>          LDQ is INTEGER
*>          The leading dimension of the array Q.  LDQ >= max(1,N).
*> \endverbatim
*>
*> \param[in] RHO
*> \verbatim
*>          RHO is REAL
*>          The value of the parameter in the rank one update equation.
*>          RHO >= 0 required.
*> \endverbatim
*>
*> \param[in] DLAMBDA
*> \verbatim
*>          DLAMBDA is REAL array, dimension (K)
*>          The first K elements of this array contain the old roots
*>          of the deflated updating problem.  These are the poles
*>          of the secular equation.
*> \endverbatim
*>
*> \param[in] Q2
*> \verbatim
*>          Q2 is REAL array, dimension (LDQ2*N)
*>          The first K columns of this matrix contain the non-deflated
*>          eigenvectors for the split problem.
*> \endverbatim
*>
*> \param[in] INDX
*> \verbatim
*>          INDX is INTEGER array, dimension (N)
*>          The permutation used to arrange the columns of the deflated
*>          Q matrix into three groups (see SLAED2).
*>          The rows of the eigenvectors found by SLAED4 must be likewise
*>          permuted before the matrix multiply can take place.
*> \endverbatim
*>
*> \param[in] CTOT
*> \verbatim
*>          CTOT is INTEGER array, dimension (4)
*>          A count of the total number of the various types of columns
*>          in Q, as described in INDX.  The fourth column type is any
*>          column which has been deflated.
*> \endverbatim
*>
*> \param[in,out] W
*> \verbatim
*>          W is REAL array, dimension (K)
*>          The first K elements of this array contain the components
*>          of the deflation-adjusted updating vector. Destroyed on
*>          output.
*> \endverbatim
*>
*> \param[out] S
*> \verbatim
*>          S is REAL array, dimension (N1 + 1)*K
*>          Will contain the eigenvectors of the repaired matrix which
*>          will be multiplied by the previously accumulated eigenvectors
*>          to update the system.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit.
*>          < 0:  if INFO = -i, the i-th argument had an illegal value.
*>          > 0:  if INFO = 1, an eigenvalue did not converge
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
*> \ingroup auxOTHERcomputational
*
*> \par Contributors:
*  ==================
*>
*> Jeff Rutter, Computer Science Division, University of California
*> at Berkeley, USA \n
*>  Modified by Francoise Tisseur, University of Tennessee
*>
*  =====================================================================
      SUBROUTINE SLAED3( K, N, N1, D, Q, LDQ, RHO, DLAMBDA, Q2, INDX,
     $                   CTOT, W, S, INFO )
*
*  -- LAPACK computational routine --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*
*     .. Scalar Arguments ..
      INTEGER            INFO, K, LDQ, N, N1
      REAL               RHO
*     ..
*     .. Array Arguments ..
      INTEGER            CTOT( * ), INDX( * )
      REAL               D( * ), DLAMBDA( * ), Q( LDQ, * ), Q2( * ),
     $                   S( * ), W( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ONE, ZERO
      PARAMETER          ( ONE = 1.0E0, ZERO = 0.0E0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, II, IQ2, J, N12, N2, N23
      REAL               TEMP
*     ..
*     .. External Functions ..
      REAL               SNRM2
      EXTERNAL           SNRM2
*     ..
*     .. External Subroutines ..
      EXTERNAL           SCOPY, SGEMM, SLACPY, SLAED4, SLASET, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, SIGN, SQRT
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
*
      IF( K.LT.0 ) THEN
         INFO = -1
      ELSE IF( N.LT.K ) THEN
         INFO = -2
      ELSE IF( LDQ.LT.MAX( 1, N ) ) THEN
         INFO = -6
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'SLAED3', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( K.EQ.0 )
     $   RETURN
*
      DO 20 J = 1, K
         CALL SLAED4( K, J, DLAMBDA, W, Q( 1, J ), RHO, D( J ), INFO )
*
*        If the zero finder fails, the computation is terminated.
*
         IF( INFO.NE.0 )
     $      GO TO 120
   20 CONTINUE
*
      IF( K.EQ.1 )
     $   GO TO 110
      IF( K.EQ.2 ) THEN
         DO 30 J = 1, K
            W( 1 ) = Q( 1, J )
            W( 2 ) = Q( 2, J )
            II = INDX( 1 )
            Q( 1, J ) = W( II )
            II = INDX( 2 )
            Q( 2, J ) = W( II )
   30    CONTINUE
         GO TO 110
      END IF
*
*     Compute updated W.
*
      CALL SCOPY( K, W, 1, S, 1 )
*
*     Initialize W(I) = Q(I,I)
*
      CALL SCOPY( K, Q, LDQ+1, W, 1 )
      DO 60 J = 1, K
         DO 40 I = 1, J - 1
            W( I ) = W( I )*( Q( I, J )/( DLAMBDA( I )-DLAMBDA( J ) ) )
   40    CONTINUE
         DO 50 I = J + 1, K
            W( I ) = W( I )*( Q( I, J )/( DLAMBDA( I )-DLAMBDA( J ) ) )
   50    CONTINUE
   60 CONTINUE
      DO 70 I = 1, K
         W( I ) = SIGN( SQRT( -W( I ) ), S( I ) )
   70 CONTINUE
*
*     Compute eigenvectors of the modified rank-1 modification.
*
      DO 100 J = 1, K
         DO 80 I = 1, K
            S( I ) = W( I ) / Q( I, J )
   80    CONTINUE
         TEMP = SNRM2( K, S, 1 )
         DO 90 I = 1, K
            II = INDX( I )
            Q( I, J ) = S( II ) / TEMP
   90    CONTINUE
  100 CONTINUE
*
*     Compute the updated eigenvectors.
*
  110 CONTINUE
*
      N2 = N - N1
      N12 = CTOT( 1 ) + CTOT( 2 )
      N23 = CTOT( 2 ) + CTOT( 3 )
*
      CALL SLACPY( 'A', N23, K, Q( CTOT( 1 )+1, 1 ), LDQ, S, N23 )
      IQ2 = N1*N12 + 1
      IF( N23.NE.0 ) THEN
         CALL SGEMM( 'N', 'N', N2, K, N23, ONE, Q2( IQ2 ), N2, S, N23,
     $               ZERO, Q( N1+1, 1 ), LDQ )
      ELSE
         CALL SLASET( 'A', N2, K, ZERO, ZERO, Q( N1+1, 1 ), LDQ )
      END IF
*
      CALL SLACPY( 'A', N12, K, Q, LDQ, S, N12 )
      IF( N12.NE.0 ) THEN
         CALL SGEMM( 'N', 'N', N1, K, N12, ONE, Q2, N1, S, N12, ZERO, Q,
     $               LDQ )
      ELSE
         CALL SLASET( 'A', N1, K, ZERO, ZERO, Q( 1, 1 ), LDQ )
      END IF
*
*
  120 CONTINUE
      RETURN
*
*     End of SLAED3
*
      END
