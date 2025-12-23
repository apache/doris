*> \brief \b SLARFB_GETT
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLARFB_GETT + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slarfb_gett.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slarfb_gett.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slarfb_gett.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLARFB_GETT( IDENT, M, N, K, T, LDT, A, LDA, B, LDB,
*      $                        WORK, LDWORK )
*       IMPLICIT NONE
*
*       .. Scalar Arguments ..
*       CHARACTER          IDENT
*       INTEGER            K, LDA, LDB, LDT, LDWORK, M, N
*       ..
*       .. Array Arguments ..
*       REAL               A( LDA, * ), B( LDB, * ), T( LDT, * ),
*      $                   WORK( LDWORK, * )
*       ..
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SLARFB_GETT applies a real Householder block reflector H from the
*> left to a real (K+M)-by-N  "triangular-pentagonal" matrix
*> composed of two block matrices: an upper trapezoidal K-by-N matrix A
*> stored in the array A, and a rectangular M-by-(N-K) matrix B, stored
*> in the array B. The block reflector H is stored in a compact
*> WY-representation, where the elementary reflectors are in the
*> arrays A, B and T. See Further Details section.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] IDENT
*> \verbatim
*>          IDENT is CHARACTER*1
*>          If IDENT = not 'I', or not 'i', then V1 is unit
*>             lower-triangular and stored in the left K-by-K block of
*>             the input matrix A,
*>          If IDENT = 'I' or 'i', then  V1 is an identity matrix and
*>             not stored.
*>          See Further Details section.
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the matrix B.
*>          M >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrices A and B.
*>          N >= 0.
*> \endverbatim
*>
*> \param[in] K
*> \verbatim
*>          K is INTEGER
*>          The number or rows of the matrix A.
*>          K is also order of the matrix T, i.e. the number of
*>          elementary reflectors whose product defines the block
*>          reflector. 0 <= K <= N.
*> \endverbatim
*>
*> \param[in] T
*> \verbatim
*>          T is REAL array, dimension (LDT,K)
*>          The upper-triangular K-by-K matrix T in the representation
*>          of the block reflector.
*> \endverbatim
*>
*> \param[in] LDT
*> \verbatim
*>          LDT is INTEGER
*>          The leading dimension of the array T. LDT >= K.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is REAL array, dimension (LDA,N)
*>
*>          On entry:
*>           a) In the K-by-N upper-trapezoidal part A: input matrix A.
*>           b) In the columns below the diagonal: columns of V1
*>              (ones are not stored on the diagonal).
*>
*>          On exit:
*>            A is overwritten by rectangular K-by-N product H*A.
*>
*>          See Further Details section.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array A. LDA >= max(1,K).
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is REAL array, dimension (LDB,N)
*>
*>          On entry:
*>            a) In the M-by-(N-K) right block: input matrix B.
*>            b) In the M-by-N left block: columns of V2.
*>
*>          On exit:
*>            B is overwritten by rectangular M-by-N product H*B.
*>
*>          See Further Details section.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B. LDB >= max(1,M).
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array,
*>          dimension (LDWORK,max(K,N-K))
*> \endverbatim
*>
*> \param[in] LDWORK
*> \verbatim
*>          LDWORK is INTEGER
*>          The leading dimension of the array WORK. LDWORK>=max(1,K).
*>
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
*> \ingroup singleOTHERauxiliary
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
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>    (1) Description of the Algebraic Operation.
*>
*>    The matrix A is a K-by-N matrix composed of two column block
*>    matrices, A1, which is K-by-K, and A2, which is K-by-(N-K):
*>    A = ( A1, A2 ).
*>    The matrix B is an M-by-N matrix composed of two column block
*>    matrices, B1, which is M-by-K, and B2, which is M-by-(N-K):
*>    B = ( B1, B2 ).
*>
*>    Perform the operation:
*>
*>       ( A_out ) := H * ( A_in ) = ( I - V * T * V**T ) * ( A_in ) =
*>       ( B_out )        ( B_in )                          ( B_in )
*>                  = ( I - ( V1 ) * T * ( V1**T, V2**T ) ) * ( A_in )
*>                          ( V2 )                            ( B_in )
*>     On input:
*>
*>    a) ( A_in )  consists of two block columns:
*>       ( B_in )
*>
*>       ( A_in ) = (( A1_in ) ( A2_in )) = (( A1_in ) ( A2_in ))
*>       ( B_in )   (( B1_in ) ( B2_in ))   ((     0 ) ( B2_in )),
*>
*>       where the column blocks are:
*>
*>       (  A1_in )  is a K-by-K upper-triangular matrix stored in the
*>                   upper triangular part of the array A(1:K,1:K).
*>       (  B1_in )  is an M-by-K rectangular ZERO matrix and not stored.
*>
*>       ( A2_in )  is a K-by-(N-K) rectangular matrix stored
*>                  in the array A(1:K,K+1:N).
*>       ( B2_in )  is an M-by-(N-K) rectangular matrix stored
*>                  in the array B(1:M,K+1:N).
*>
*>    b) V = ( V1 )
*>           ( V2 )
*>
*>       where:
*>       1) if IDENT == 'I',V1 is a K-by-K identity matrix, not stored;
*>       2) if IDENT != 'I',V1 is a K-by-K unit lower-triangular matrix,
*>          stored in the lower-triangular part of the array
*>          A(1:K,1:K) (ones are not stored),
*>       and V2 is an M-by-K rectangular stored the array B(1:M,1:K),
*>                 (because on input B1_in is a rectangular zero
*>                  matrix that is not stored and the space is
*>                  used to store V2).
*>
*>    c) T is a K-by-K upper-triangular matrix stored
*>       in the array T(1:K,1:K).
*>
*>    On output:
*>
*>    a) ( A_out ) consists of two  block columns:
*>       ( B_out )
*>
*>       ( A_out ) = (( A1_out ) ( A2_out ))
*>       ( B_out )   (( B1_out ) ( B2_out )),
*>
*>       where the column blocks are:
*>
*>       ( A1_out )  is a K-by-K square matrix, or a K-by-K
*>                   upper-triangular matrix, if V1 is an
*>                   identity matrix. AiOut is stored in
*>                   the array A(1:K,1:K).
*>       ( B1_out )  is an M-by-K rectangular matrix stored
*>                   in the array B(1:M,K:N).
*>
*>       ( A2_out )  is a K-by-(N-K) rectangular matrix stored
*>                   in the array A(1:K,K+1:N).
*>       ( B2_out )  is an M-by-(N-K) rectangular matrix stored
*>                   in the array B(1:M,K+1:N).
*>
*>
*>    The operation above can be represented as the same operation
*>    on each block column:
*>
*>       ( A1_out ) := H * ( A1_in ) = ( I - V * T * V**T ) * ( A1_in )
*>       ( B1_out )        (     0 )                          (     0 )
*>
*>       ( A2_out ) := H * ( A2_in ) = ( I - V * T * V**T ) * ( A2_in )
*>       ( B2_out )        ( B2_in )                          ( B2_in )
*>
*>    If IDENT != 'I':
*>
*>       The computation for column block 1:
*>
*>       A1_out: = A1_in - V1*T*(V1**T)*A1_in
*>
*>       B1_out: = - V2*T*(V1**T)*A1_in
*>
*>       The computation for column block 2, which exists if N > K:
*>
*>       A2_out: = A2_in - V1*T*( (V1**T)*A2_in + (V2**T)*B2_in )
*>
*>       B2_out: = B2_in - V2*T*( (V1**T)*A2_in + (V2**T)*B2_in )
*>
*>    If IDENT == 'I':
*>
*>       The operation for column block 1:
*>
*>       A1_out: = A1_in - V1*T**A1_in
*>
*>       B1_out: = - V2*T**A1_in
*>
*>       The computation for column block 2, which exists if N > K:
*>
*>       A2_out: = A2_in - T*( A2_in + (V2**T)*B2_in )
*>
*>       B2_out: = B2_in - V2*T*( A2_in + (V2**T)*B2_in )
*>
*>    (2) Description of the Algorithmic Computation.
*>
*>    In the first step, we compute column block 2, i.e. A2 and B2.
*>    Here, we need to use the K-by-(N-K) rectangular workspace
*>    matrix W2 that is of the same size as the matrix A2.
*>    W2 is stored in the array WORK(1:K,1:(N-K)).
*>
*>    In the second step, we compute column block 1, i.e. A1 and B1.
*>    Here, we need to use the K-by-K square workspace matrix W1
*>    that is of the same size as the as the matrix A1.
*>    W1 is stored in the array WORK(1:K,1:K).
*>
*>    NOTE: Hence, in this routine, we need the workspace array WORK
*>    only of size WORK(1:K,1:max(K,N-K)) so it can hold both W2 from
*>    the first step and W1 from the second step.
*>
*>    Case (A), when V1 is unit lower-triangular, i.e. IDENT != 'I',
*>    more computations than in the Case (B).
*>
*>    if( IDENT != 'I' ) then
*>     if ( N > K ) then
*>       (First Step - column block 2)
*>       col2_(1) W2: = A2
*>       col2_(2) W2: = (V1**T) * W2 = (unit_lower_tr_of_(A1)**T) * W2
*>       col2_(3) W2: = W2 + (V2**T) * B2 = W2 + (B1**T) * B2
*>       col2_(4) W2: = T * W2
*>       col2_(5) B2: = B2 - V2 * W2 = B2 - B1 * W2
*>       col2_(6) W2: = V1 * W2 = unit_lower_tr_of_(A1) * W2
*>       col2_(7) A2: = A2 - W2
*>     else
*>       (Second Step - column block 1)
*>       col1_(1) W1: = A1
*>       col1_(2) W1: = (V1**T) * W1 = (unit_lower_tr_of_(A1)**T) * W1
*>       col1_(3) W1: = T * W1
*>       col1_(4) B1: = - V2 * W1 = - B1 * W1
*>       col1_(5) square W1: = V1 * W1 = unit_lower_tr_of_(A1) * W1
*>       col1_(6) square A1: = A1 - W1
*>     end if
*>    end if
*>
*>    Case (B), when V1 is an identity matrix, i.e. IDENT == 'I',
*>    less computations than in the Case (A)
*>
*>    if( IDENT == 'I' ) then
*>     if ( N > K ) then
*>       (First Step - column block 2)
*>       col2_(1) W2: = A2
*>       col2_(3) W2: = W2 + (V2**T) * B2 = W2 + (B1**T) * B2
*>       col2_(4) W2: = T * W2
*>       col2_(5) B2: = B2 - V2 * W2 = B2 - B1 * W2
*>       col2_(7) A2: = A2 - W2
*>     else
*>       (Second Step - column block 1)
*>       col1_(1) W1: = A1
*>       col1_(3) W1: = T * W1
*>       col1_(4) B1: = - V2 * W1 = - B1 * W1
*>       col1_(6) upper-triangular_of_(A1): = A1 - W1
*>     end if
*>    end if
*>
*>    Combine these cases (A) and (B) together, this is the resulting
*>    algorithm:
*>
*>    if ( N > K ) then
*>
*>      (First Step - column block 2)
*>
*>      col2_(1)  W2: = A2
*>      if( IDENT != 'I' ) then
*>        col2_(2)  W2: = (V1**T) * W2
*>                      = (unit_lower_tr_of_(A1)**T) * W2
*>      end if
*>      col2_(3)  W2: = W2 + (V2**T) * B2 = W2 + (B1**T) * B2]
*>      col2_(4)  W2: = T * W2
*>      col2_(5)  B2: = B2 - V2 * W2 = B2 - B1 * W2
*>      if( IDENT != 'I' ) then
*>        col2_(6)    W2: = V1 * W2 = unit_lower_tr_of_(A1) * W2
*>      end if
*>      col2_(7) A2: = A2 - W2
*>
*>    else
*>
*>    (Second Step - column block 1)
*>
*>      col1_(1) W1: = A1
*>      if( IDENT != 'I' ) then
*>        col1_(2) W1: = (V1**T) * W1
*>                    = (unit_lower_tr_of_(A1)**T) * W1
*>      end if
*>      col1_(3) W1: = T * W1
*>      col1_(4) B1: = - V2 * W1 = - B1 * W1
*>      if( IDENT != 'I' ) then
*>        col1_(5) square W1: = V1 * W1 = unit_lower_tr_of_(A1) * W1
*>        col1_(6_a) below_diag_of_(A1): =  - below_diag_of_(W1)
*>      end if
*>      col1_(6_b) up_tr_of_(A1): = up_tr_of_(A1) - up_tr_of_(W1)
*>
*>    end if
*>
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE SLARFB_GETT( IDENT, M, N, K, T, LDT, A, LDA, B, LDB,
     $                        WORK, LDWORK )
      IMPLICIT NONE
*
*  -- LAPACK auxiliary routine --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*
*     .. Scalar Arguments ..
      CHARACTER          IDENT
      INTEGER            K, LDA, LDB, LDT, LDWORK, M, N
*     ..
*     .. Array Arguments ..
      REAL               A( LDA, * ), B( LDB, * ), T( LDT, * ),
     $                   WORK( LDWORK, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ONE, ZERO
      PARAMETER          ( ONE = 1.0E+0, ZERO = 0.0E+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            LNOTIDENT
      INTEGER            I, J
*     ..
*     .. EXTERNAL FUNCTIONS ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           SCOPY, SGEMM, STRMM
*     ..
*     .. Executable Statements ..
*
*     Quick return if possible
*
      IF( M.LT.0 .OR. N.LE.0 .OR. K.EQ.0 .OR. K.GT.N )
     $   RETURN
*
      LNOTIDENT = .NOT.LSAME( IDENT, 'I' )
*
*     ------------------------------------------------------------------
*
*     First Step. Computation of the Column Block 2:
*
*        ( A2 ) := H * ( A2 )
*        ( B2 )        ( B2 )
*
*     ------------------------------------------------------------------
*
      IF( N.GT.K ) THEN
*
*        col2_(1) Compute W2: = A2. Therefore, copy A2 = A(1:K, K+1:N)
*        into W2=WORK(1:K, 1:N-K) column-by-column.
*
         DO J = 1, N-K
            CALL SCOPY( K, A( 1, K+J ), 1, WORK( 1, J ), 1 )
         END DO

         IF( LNOTIDENT ) THEN
*
*           col2_(2) Compute W2: = (V1**T) * W2 = (A1**T) * W2,
*           V1 is not an identity matrix, but unit lower-triangular
*           V1 stored in A1 (diagonal ones are not stored).
*
*
            CALL STRMM( 'L', 'L', 'T', 'U', K, N-K, ONE, A, LDA,
     $                  WORK, LDWORK )
         END IF
*
*        col2_(3) Compute W2: = W2 + (V2**T) * B2 = W2 + (B1**T) * B2
*        V2 stored in B1.
*
         IF( M.GT.0 ) THEN
            CALL SGEMM( 'T', 'N', K, N-K, M, ONE, B, LDB,
     $                  B( 1, K+1 ), LDB, ONE, WORK, LDWORK )
         END IF
*
*        col2_(4) Compute W2: = T * W2,
*        T is upper-triangular.
*
         CALL STRMM( 'L', 'U', 'N', 'N', K, N-K, ONE, T, LDT,
     $               WORK, LDWORK )
*
*        col2_(5) Compute B2: = B2 - V2 * W2 = B2 - B1 * W2,
*        V2 stored in B1.
*
         IF( M.GT.0 ) THEN
            CALL SGEMM( 'N', 'N', M, N-K, K, -ONE, B, LDB,
     $                   WORK, LDWORK, ONE, B( 1, K+1 ), LDB )
         END IF
*
         IF( LNOTIDENT ) THEN
*
*           col2_(6) Compute W2: = V1 * W2 = A1 * W2,
*           V1 is not an identity matrix, but unit lower-triangular,
*           V1 stored in A1 (diagonal ones are not stored).
*
            CALL STRMM( 'L', 'L', 'N', 'U', K, N-K, ONE, A, LDA,
     $                  WORK, LDWORK )
         END IF
*
*        col2_(7) Compute A2: = A2 - W2 =
*                             = A(1:K, K+1:N-K) - WORK(1:K, 1:N-K),
*        column-by-column.
*
         DO J = 1, N-K
            DO I = 1, K
               A( I, K+J ) = A( I, K+J ) - WORK( I, J )
            END DO
         END DO
*
      END IF
*
*     ------------------------------------------------------------------
*
*     Second Step. Computation of the Column Block 1:
*
*        ( A1 ) := H * ( A1 )
*        ( B1 )        (  0 )
*
*     ------------------------------------------------------------------
*
*     col1_(1) Compute W1: = A1. Copy the upper-triangular
*     A1 = A(1:K, 1:K) into the upper-triangular
*     W1 = WORK(1:K, 1:K) column-by-column.
*
      DO J = 1, K
         CALL SCOPY( J, A( 1, J ), 1, WORK( 1, J ), 1 )
      END DO
*
*     Set the subdiagonal elements of W1 to zero column-by-column.
*
      DO J = 1, K - 1
         DO I = J + 1, K
            WORK( I, J ) = ZERO
         END DO
      END DO
*
      IF( LNOTIDENT ) THEN
*
*        col1_(2) Compute W1: = (V1**T) * W1 = (A1**T) * W1,
*        V1 is not an identity matrix, but unit lower-triangular
*        V1 stored in A1 (diagonal ones are not stored),
*        W1 is upper-triangular with zeroes below the diagonal.
*
         CALL STRMM( 'L', 'L', 'T', 'U', K, K, ONE, A, LDA,
     $               WORK, LDWORK )
      END IF
*
*     col1_(3) Compute W1: = T * W1,
*     T is upper-triangular,
*     W1 is upper-triangular with zeroes below the diagonal.
*
      CALL STRMM( 'L', 'U', 'N', 'N', K, K, ONE, T, LDT,
     $            WORK, LDWORK )
*
*     col1_(4) Compute B1: = - V2 * W1 = - B1 * W1,
*     V2 = B1, W1 is upper-triangular with zeroes below the diagonal.
*
      IF( M.GT.0 ) THEN
         CALL STRMM( 'R', 'U', 'N', 'N', M, K, -ONE, WORK, LDWORK,
     $               B, LDB )
      END IF
*
      IF( LNOTIDENT ) THEN
*
*        col1_(5) Compute W1: = V1 * W1 = A1 * W1,
*        V1 is not an identity matrix, but unit lower-triangular
*        V1 stored in A1 (diagonal ones are not stored),
*        W1 is upper-triangular on input with zeroes below the diagonal,
*        and square on output.
*
         CALL STRMM( 'L', 'L', 'N', 'U', K, K, ONE, A, LDA,
     $               WORK, LDWORK )
*
*        col1_(6) Compute A1: = A1 - W1 = A(1:K, 1:K) - WORK(1:K, 1:K)
*        column-by-column. A1 is upper-triangular on input.
*        If IDENT, A1 is square on output, and W1 is square,
*        if NOT IDENT, A1 is upper-triangular on output,
*        W1 is upper-triangular.
*
*        col1_(6)_a Compute elements of A1 below the diagonal.
*
         DO J = 1, K - 1
            DO I = J + 1, K
               A( I, J ) = - WORK( I, J )
            END DO
         END DO
*
      END IF
*
*     col1_(6)_b Compute elements of A1 on and above the diagonal.
*
      DO J = 1, K
         DO I = 1, J
            A( I, J ) = A( I, J ) - WORK( I, J )
         END DO
      END DO
*
      RETURN
*
*     End of SLARFB_GETT
*
      END
