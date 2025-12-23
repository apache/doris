*> \brief \b ZGETSQRHRT
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZGETSQRHRT + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zgetsqrhrt.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zgetsqrhrt.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zgetsqrhrt.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZGETSQRHRT( M, N, MB1, NB1, NB2, A, LDA, T, LDT, WORK,
*      $                       LWORK, INFO )
*       IMPLICIT NONE
*
*       .. Scalar Arguments ..
*       INTEGER           INFO, LDA, LDT, LWORK, M, N, NB1, NB2, MB1
*       ..
*       .. Array Arguments ..
*       COMPLEX*16        A( LDA, * ), T( LDT, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZGETSQRHRT computes a NB2-sized column blocked QR-factorization
*> of a complex M-by-N matrix A with M >= N,
*>
*>    A = Q * R.
*>
*> The routine uses internally a NB1-sized column blocked and MB1-sized
*> row blocked TSQR-factorization and perfors the reconstruction
*> of the Householder vectors from the TSQR output. The routine also
*> converts the R_tsqr factor from the TSQR-factorization output into
*> the R factor that corresponds to the Householder QR-factorization,
*>
*>    A = Q_tsqr * R_tsqr = Q * R.
*>
*> The output Q and R factors are stored in the same format as in ZGEQRT
*> (Q is in blocked compact WY-representation). See the documentation
*> of ZGEQRT for more details on the format.
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
*>          The number of columns of the matrix A. M >= N >= 0.
*> \endverbatim
*>
*> \param[in] MB1
*> \verbatim
*>          MB1 is INTEGER
*>          The row block size to be used in the blocked TSQR.
*>          MB1 > N.
*> \endverbatim
*>
*> \param[in] NB1
*> \verbatim
*>          NB1 is INTEGER
*>          The column block size to be used in the blocked TSQR.
*>          N >= NB1 >= 1.
*> \endverbatim
*>
*> \param[in] NB2
*> \verbatim
*>          NB2 is INTEGER
*>          The block size to be used in the blocked QR that is
*>          output. NB2 >= 1.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA,N)
*>
*>          On entry: an M-by-N matrix A.
*>
*>          On exit:
*>           a) the elements on and above the diagonal
*>              of the array contain the N-by-N upper-triangular
*>              matrix R corresponding to the Householder QR;
*>           b) the elements below the diagonal represent Q by
*>              the columns of blocked V (compact WY-representation).
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
*>          T is COMPLEX*16 array, dimension (LDT,N))
*>          The upper triangular block reflectors stored in compact form
*>          as a sequence of upper triangular blocks.
*> \endverbatim
*>
*> \param[in] LDT
*> \verbatim
*>          LDT is INTEGER
*>          The leading dimension of the array T.  LDT >= NB2.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          (workspace) COMPLEX*16 array, dimension (MAX(1,LWORK))
*>          On exit, if INFO = 0, WORK(1) returns the optimal LWORK.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          The dimension of the array WORK.
*>          If MIN(M,N) = 0, LWORK >= 1, else
*>          LWORK >= MAX( 1, LWT + LW1, MAX( LWT+N*N+LW2, LWT+N*N+N ) ),
*>          where
*>             NUM_ALL_ROW_BLOCKS = CEIL((M-N)/(MB1-N)),
*>             NB1LOCAL = MIN(NB1,N).
*>             LWT = NUM_ALL_ROW_BLOCKS * N * NB1LOCAL,
*>             LW1 = NB1LOCAL * N,
*>             LW2 = NB1LOCAL * MAX( NB1LOCAL, ( N - NB1LOCAL ) ).
*>
*>          If LWORK = -1, then a workspace query is assumed.
*>          The routine only calculates the optimal size of the WORK
*>          array, returns this value as the first entry of the WORK
*>          array, and no error message related to LWORK is issued
*>          by XERBLA.
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
*> \ingroup getsqrhrt
*
*> \par Contributors:
*  ==================
*>
*> \verbatim
*>
*> November 2020, Igor Kozachenko,
*>                Computer Science Division,
*>                University of California, Berkeley
*>
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE ZGETSQRHRT( M, N, MB1, NB1, NB2, A, LDA, T, LDT, WORK,
     $                       LWORK, INFO )
      IMPLICIT NONE
*
*  -- LAPACK computational routine --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*
*     .. Scalar Arguments ..
      INTEGER           INFO, LDA, LDT, LWORK, M, N, NB1, NB2, MB1
*     ..
*     .. Array Arguments ..
      COMPLEX*16        A( LDA, * ), T( LDT, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      COMPLEX*16         CONE
      PARAMETER          ( CONE = ( 1.0D+0, 0.0D+0 ) )
*     ..
*     .. Local Scalars ..
      LOGICAL            LQUERY
      INTEGER            I, IINFO, J, LW1, LW2, LWT, LDWT, LWORKOPT,
     $                   NB1LOCAL, NB2LOCAL, NUM_ALL_ROW_BLOCKS
*     ..
*     .. External Subroutines ..
      EXTERNAL           ZCOPY, ZLATSQR, ZUNGTSQR_ROW, ZUNHR_COL,
     $                   XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          CEILING, DBLE, DCMPLX, MAX, MIN
*     ..
*     .. Executable Statements ..
*
*     Test the input arguments
*
      INFO = 0
      LQUERY = ( LWORK.EQ.-1 )
      IF( M.LT.0 ) THEN
         INFO = -1
      ELSE IF( N.LT.0 .OR. M.LT.N ) THEN
         INFO = -2
      ELSE IF( MB1.LE.N ) THEN
         INFO = -3
      ELSE IF( NB1.LT.1 ) THEN
         INFO = -4
      ELSE IF( NB2.LT.1 ) THEN
         INFO = -5
      ELSE IF( LDA.LT.MAX( 1, M ) ) THEN
         INFO = -7
      ELSE IF( LDT.LT.MAX( 1, MIN( NB2, N ) ) ) THEN
         INFO = -9
      ELSE
*
*        Test the input LWORK for the dimension of the array WORK.
*        This workspace is used to store array:
*        a) Matrix T and WORK for ZLATSQR;
*        b) N-by-N upper-triangular factor R_tsqr;
*        c) Matrix T and array WORK for ZUNGTSQR_ROW;
*        d) Diagonal D for ZUNHR_COL.
*
         IF( LWORK.LT.N*N+1 .AND. .NOT.LQUERY ) THEN
            INFO = -11
         ELSE
*
*           Set block size for column blocks
*
            NB1LOCAL = MIN( NB1, N )
*
            NUM_ALL_ROW_BLOCKS = MAX( 1,
     $                   CEILING( DBLE( M - N ) / DBLE( MB1 - N ) ) )
*
*           Length and leading dimension of WORK array to place
*           T array in TSQR.
*
            LWT = NUM_ALL_ROW_BLOCKS * N * NB1LOCAL

            LDWT = NB1LOCAL
*
*           Length of TSQR work array
*
            LW1 = NB1LOCAL * N
*
*           Length of ZUNGTSQR_ROW work array.
*
            LW2 = NB1LOCAL * MAX( NB1LOCAL, ( N - NB1LOCAL ) )
*
            LWORKOPT = MAX( LWT + LW1, MAX( LWT+N*N+LW2, LWT+N*N+N ) )
            LWORKOPT = MAX( 1, LWORKOPT )
*
            IF( LWORK.LT.LWORKOPT .AND. .NOT.LQUERY ) THEN
               INFO = -11
            END IF
*
         END IF
      END IF
*
*     Handle error in the input parameters and return workspace query.
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZGETSQRHRT', -INFO )
         RETURN
      ELSE IF ( LQUERY ) THEN
         WORK( 1 ) = DCMPLX( LWORKOPT )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( MIN( M, N ).EQ.0 ) THEN
         WORK( 1 ) = DCMPLX( LWORKOPT )
         RETURN
      END IF
*
      NB2LOCAL = MIN( NB2, N )
*
*
*     (1) Perform TSQR-factorization of the M-by-N matrix A.
*
      CALL ZLATSQR( M, N, MB1, NB1LOCAL, A, LDA, WORK, LDWT,
     $              WORK(LWT+1), LW1, IINFO )
*
*     (2) Copy the factor R_tsqr stored in the upper-triangular part
*         of A into the square matrix in the work array
*         WORK(LWT+1:LWT+N*N) column-by-column.
*
      DO J = 1, N
         CALL ZCOPY( J, A( 1, J ), 1, WORK( LWT + N*(J-1)+1 ), 1 )
      END DO
*
*     (3) Generate a M-by-N matrix Q with orthonormal columns from
*     the result stored below the diagonal in the array A in place.
*

      CALL ZUNGTSQR_ROW( M, N, MB1, NB1LOCAL, A, LDA, WORK, LDWT,
     $                   WORK( LWT+N*N+1 ), LW2, IINFO )
*
*     (4) Perform the reconstruction of Householder vectors from
*     the matrix Q (stored in A) in place.
*
      CALL ZUNHR_COL( M, N, NB2LOCAL, A, LDA, T, LDT,
     $                WORK( LWT+N*N+1 ), IINFO )
*
*     (5) Copy the factor R_tsqr stored in the square matrix in the
*     work array WORK(LWT+1:LWT+N*N) into the upper-triangular
*     part of A.
*
*     (6) Compute from R_tsqr the factor R_hr corresponding to
*     the reconstructed Householder vectors, i.e. R_hr = S * R_tsqr.
*     This multiplication by the sign matrix S on the left means
*     changing the sign of I-th row of the matrix R_tsqr according
*     to sign of the I-th diagonal element DIAG(I) of the matrix S.
*     DIAG is stored in WORK( LWT+N*N+1 ) from the ZUNHR_COL output.
*
*     (5) and (6) can be combined in a single loop, so the rows in A
*     are accessed only once.
*
      DO I = 1, N
         IF( WORK( LWT+N*N+I ).EQ.-CONE ) THEN
            DO J = I, N
               A( I, J ) = -CONE * WORK( LWT+N*(J-1)+I )
            END DO
         ELSE
            CALL ZCOPY( N-I+1, WORK(LWT+N*(I-1)+I), N, A( I, I ), LDA )
         END IF
      END DO
*
      WORK( 1 ) = DCMPLX( LWORKOPT )
      RETURN
*
*     End of ZGETSQRHRT
*
      END