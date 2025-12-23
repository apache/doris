*> \brief \b CUNGTSQR_ROW
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CUNGTSQR_ROW + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/cunrgtsqr_row.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/cunrgtsqr_row.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/cunrgtsqr_row.f">
*> [TXT]</a>
*> \endhtmlonly
*>
*  Definition:
*  ===========
*
*       SUBROUTINE CUNGTSQR_ROW( M, N, MB, NB, A, LDA, T, LDT, WORK,
*      $                         LWORK, INFO )
*       IMPLICIT NONE
*
*       .. Scalar Arguments ..
*       INTEGER           INFO, LDA, LDT, LWORK, M, N, MB, NB
*       ..
*       .. Array Arguments ..
*       COMPLEX           A( LDA, * ), T( LDT, * ), WORK( * )
*       ..
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CUNGTSQR_ROW generates an M-by-N complex matrix Q_out with
*> orthonormal columns from the output of CLATSQR. These N orthonormal
*> columns are the first N columns of a product of complex unitary
*> matrices Q(k)_in of order M, which are returned by CLATSQR in
*> a special format.
*>
*>      Q_out = first_N_columns_of( Q(1)_in * Q(2)_in * ... * Q(k)_in ).
*>
*> The input matrices Q(k)_in are stored in row and column blocks in A.
*> See the documentation of CLATSQR for more details on the format of
*> Q(k)_in, where each Q(k)_in is represented by block Householder
*> transformations. This routine calls an auxiliary routine CLARFB_GETT,
*> where the computation is performed on each individual block. The
*> algorithm first sweeps NB-sized column blocks from the right to left
*> starting in the bottom row block and continues to the top row block
*> (hence _ROW in the routine name). This sweep is in reverse order of
*> the order in which CLATSQR generates the output blocks.
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
*> \param[in] MB
*> \verbatim
*>          MB is INTEGER
*>          The row block size used by CLATSQR to return
*>          arrays A and T. MB > N.
*>          (Note that if MB > M, then M is used instead of MB
*>          as the row block size).
*> \endverbatim
*>
*> \param[in] NB
*> \verbatim
*>          NB is INTEGER
*>          The column block size used by CLATSQR to return
*>          arrays A and T. NB >= 1.
*>          (Note that if NB > N, then N is used instead of NB
*>          as the column block size).
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA,N)
*>
*>          On entry:
*>
*>             The elements on and above the diagonal are not used as
*>             input. The elements below the diagonal represent the unit
*>             lower-trapezoidal blocked matrix V computed by CLATSQR
*>             that defines the input matrices Q_in(k) (ones on the
*>             diagonal are not stored). See CLATSQR for more details.
*>
*>          On exit:
*>
*>             The array A contains an M-by-N orthonormal matrix Q_out,
*>             i.e the columns of A are orthogonal unit vectors.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,M).
*> \endverbatim
*>
*> \param[in] T
*> \verbatim
*>          T is COMPLEX array,
*>          dimension (LDT, N * NIRB)
*>          where NIRB = Number_of_input_row_blocks
*>                     = MAX( 1, CEIL((M-N)/(MB-N)) )
*>          Let NICB = Number_of_input_col_blocks
*>                   = CEIL(N/NB)
*>
*>          The upper-triangular block reflectors used to define the
*>          input matrices Q_in(k), k=(1:NIRB*NICB). The block
*>          reflectors are stored in compact form in NIRB block
*>          reflector sequences. Each of the NIRB block reflector
*>          sequences is stored in a larger NB-by-N column block of T
*>          and consists of NICB smaller NB-by-NB upper-triangular
*>          column blocks. See CLATSQR for more details on the format
*>          of T.
*> \endverbatim
*>
*> \param[in] LDT
*> \verbatim
*>          LDT is INTEGER
*>          The leading dimension of the array T.
*>          LDT >= max(1,min(NB,N)).
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          (workspace) COMPLEX array, dimension (MAX(1,LWORK))
*>          On exit, if INFO = 0, WORK(1) returns the optimal LWORK.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          The dimension of the array WORK.
*>          LWORK >= NBLOCAL * MAX(NBLOCAL,(N-NBLOCAL)),
*>          where NBLOCAL=MIN(NB,N).
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
*>
*  Authors:
*  ========
*
*> \author Univ. of Tennessee
*> \author Univ. of California Berkeley
*> \author Univ. of Colorado Denver
*> \author NAG Ltd.
*
*> \ingroup complexOTHERcomputational
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
      SUBROUTINE CUNGTSQR_ROW( M, N, MB, NB, A, LDA, T, LDT, WORK,
     $                         LWORK, INFO )
      IMPLICIT NONE
*
*  -- LAPACK computational routine --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*
*     .. Scalar Arguments ..
      INTEGER           INFO, LDA, LDT, LWORK, M, N, MB, NB
*     ..
*     .. Array Arguments ..
      COMPLEX           A( LDA, * ), T( LDT, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      COMPLEX            CONE, CZERO
      PARAMETER          ( CONE = ( 1.0E+0, 0.0E+0 ),
     $                     CZERO = ( 0.0E+0, 0.0E+0 ) )
*     ..
*     .. Local Scalars ..
      LOGICAL            LQUERY
      INTEGER            NBLOCAL, MB2, M_PLUS_ONE, ITMP, IB_BOTTOM,
     $                   LWORKOPT, NUM_ALL_ROW_BLOCKS, JB_T, IB, IMB,
     $                   KB, KB_LAST, KNB, MB1
*     ..
*     .. Local Arrays ..
      COMPLEX            DUMMY( 1, 1 )
*     ..
*     .. External Subroutines ..
      EXTERNAL           CLARFB_GETT, CLASET, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          CMPLX, MAX, MIN
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters
*
      INFO = 0
      LQUERY  = LWORK.EQ.-1
      IF( M.LT.0 ) THEN
         INFO = -1
      ELSE IF( N.LT.0 .OR. M.LT.N ) THEN
         INFO = -2
      ELSE IF( MB.LE.N ) THEN
         INFO = -3
      ELSE IF( NB.LT.1 ) THEN
         INFO = -4
      ELSE IF( LDA.LT.MAX( 1, M ) ) THEN
         INFO = -6
      ELSE IF( LDT.LT.MAX( 1, MIN( NB, N ) ) ) THEN
         INFO = -8
      ELSE IF( LWORK.LT.1 .AND. .NOT.LQUERY ) THEN
         INFO = -10
      END IF
*
      NBLOCAL = MIN( NB, N )
*
*     Determine the workspace size.
*
      IF( INFO.EQ.0 ) THEN
         LWORKOPT = NBLOCAL * MAX( NBLOCAL, ( N - NBLOCAL ) )
      END IF
*
*     Handle error in the input parameters and handle the workspace query.
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'CUNGTSQR_ROW', -INFO )
         RETURN
      ELSE IF ( LQUERY ) THEN
         WORK( 1 ) = CMPLX( LWORKOPT )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( MIN( M, N ).EQ.0 ) THEN
         WORK( 1 ) = CMPLX( LWORKOPT )
         RETURN
      END IF
*
*     (0) Set the upper-triangular part of the matrix A to zero and
*     its diagonal elements to one.
*
      CALL CLASET('U', M, N, CZERO, CONE, A, LDA )
*
*     KB_LAST is the column index of the last column block reflector
*     in the matrices T and V.
*
      KB_LAST = ( ( N-1 ) / NBLOCAL ) * NBLOCAL + 1
*
*
*     (1) Bottom-up loop over row blocks of A, except the top row block.
*     NOTE: If MB>=M, then the loop is never executed.
*
      IF ( MB.LT.M ) THEN
*
*        MB2 is the row blocking size for the row blocks before the
*        first top row block in the matrix A. IB is the row index for
*        the row blocks in the matrix A before the first top row block.
*        IB_BOTTOM is the row index for the last bottom row block
*        in the matrix A. JB_T is the column index of the corresponding
*        column block in the matrix T.
*
*        Initialize variables.
*
*        NUM_ALL_ROW_BLOCKS is the number of row blocks in the matrix A
*        including the first row block.
*
         MB2 = MB - N
         M_PLUS_ONE = M + 1
         ITMP = ( M - MB - 1 ) / MB2
         IB_BOTTOM = ITMP * MB2 + MB + 1
         NUM_ALL_ROW_BLOCKS = ITMP + 2
         JB_T = NUM_ALL_ROW_BLOCKS * N + 1
*
         DO IB = IB_BOTTOM, MB+1, -MB2
*
*           Determine the block size IMB for the current row block
*           in the matrix A.
*
            IMB = MIN( M_PLUS_ONE - IB, MB2 )
*
*           Determine the column index JB_T for the current column block
*           in the matrix T.
*
            JB_T = JB_T - N
*
*           Apply column blocks of H in the row block from right to left.
*
*           KB is the column index of the current column block reflector
*           in the matrices T and V.
*
            DO KB = KB_LAST, 1, -NBLOCAL
*
*              Determine the size of the current column block KNB in
*              the matrices T and V.
*
               KNB = MIN( NBLOCAL, N - KB + 1 )
*
               CALL CLARFB_GETT( 'I', IMB, N-KB+1, KNB,
     $                     T( 1, JB_T+KB-1 ), LDT, A( KB, KB ), LDA,
     $                     A( IB, KB ), LDA, WORK, KNB )
*
            END DO
*
         END DO
*
      END IF
*
*     (2) Top row block of A.
*     NOTE: If MB>=M, then we have only one row block of A of size M
*     and we work on the entire matrix A.
*
      MB1 = MIN( MB, M )
*
*     Apply column blocks of H in the top row block from right to left.
*
*     KB is the column index of the current block reflector in
*     the matrices T and V.
*
      DO KB = KB_LAST, 1, -NBLOCAL
*
*        Determine the size of the current column block KNB in
*        the matrices T and V.
*
         KNB = MIN( NBLOCAL, N - KB + 1 )
*
         IF( MB1-KB-KNB+1.EQ.0 ) THEN
*
*           In SLARFB_GETT parameters, when M=0, then the matrix B
*           does not exist, hence we need to pass a dummy array
*           reference DUMMY(1,1) to B with LDDUMMY=1.
*
            CALL CLARFB_GETT( 'N', 0, N-KB+1, KNB,
     $                        T( 1, KB ), LDT, A( KB, KB ), LDA,
     $                        DUMMY( 1, 1 ), 1, WORK, KNB )
         ELSE
            CALL CLARFB_GETT( 'N', MB1-KB-KNB+1, N-KB+1, KNB,
     $                        T( 1, KB ), LDT, A( KB, KB ), LDA,
     $                        A( KB+KNB, KB), LDA, WORK, KNB )

         END IF
*
      END DO
*
      WORK( 1 ) = CMPLX( LWORKOPT )
      RETURN
*
*     End of CUNGTSQR_ROW
*
      END
