*> \brief \b SLARFT forms the triangular factor T of a block reflector H = I - vtvH
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLARFT + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slarft.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slarft.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slarft.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       RECURSIVE SUBROUTINE SLARFT( DIRECT, STOREV, N, K, V, LDV, TAU, T, LDT )
*
*       .. Scalar Arguments ..
*       CHARACTER          DIRECT, STOREV
*       INTEGER            K, LDT, LDV, N
*       ..
*       .. Array Arguments ..
*       REAL               T( LDT, * ), TAU( * ), V( LDV, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SLARFT forms the triangular factor T of a real block reflector H
*> of order n, which is defined as a product of k elementary reflectors.
*>
*> If DIRECT = 'F', H = H(1) H(2) . . . H(k) and T is upper triangular;
*>
*> If DIRECT = 'B', H = H(k) . . . H(2) H(1) and T is lower triangular.
*>
*> If STOREV = 'C', the vector which defines the elementary reflector
*> H(i) is stored in the i-th column of the array V, and
*>
*>    H  =  I - V * T * V**T
*>
*> If STOREV = 'R', the vector which defines the elementary reflector
*> H(i) is stored in the i-th row of the array V, and
*>
*>    H  =  I - V**T * T * V
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] DIRECT
*> \verbatim
*>          DIRECT is CHARACTER*1
*>          Specifies the order in which the elementary reflectors are
*>          multiplied to form the block reflector:
*>          = 'F': H = H(1) H(2) . . . H(k) (Forward)
*>          = 'B': H = H(k) . . . H(2) H(1) (Backward)
*> \endverbatim
*>
*> \param[in] STOREV
*> \verbatim
*>          STOREV is CHARACTER*1
*>          Specifies how the vectors which define the elementary
*>          reflectors are stored (see also Further Details):
*>          = 'C': columnwise
*>          = 'R': rowwise
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the block reflector H. N >= 0.
*> \endverbatim
*>
*> \param[in] K
*> \verbatim
*>          K is INTEGER
*>          The order of the triangular factor T (= the number of
*>          elementary reflectors). K >= 1.
*> \endverbatim
*>
*> \param[in] V
*> \verbatim
*>          V is REAL array, dimension
*>                               (LDV,K) if STOREV = 'C'
*>                               (LDV,N) if STOREV = 'R'
*>          The matrix V. See further details.
*> \endverbatim
*>
*> \param[in] LDV
*> \verbatim
*>          LDV is INTEGER
*>          The leading dimension of the array V.
*>          If STOREV = 'C', LDV >= max(1,N); if STOREV = 'R', LDV >= K.
*> \endverbatim
*>
*> \param[in] TAU
*> \verbatim
*>          TAU is REAL array, dimension (K)
*>          TAU(i) must contain the scalar factor of the elementary
*>          reflector H(i).
*> \endverbatim
*>
*> \param[out] T
*> \verbatim
*>          T is REAL array, dimension (LDT,K)
*>          The k by k triangular factor T of the block reflector.
*>          If DIRECT = 'F', T is upper triangular; if DIRECT = 'B', T is
*>          lower triangular. The rest of the array is not used.
*> \endverbatim
*>
*> \param[in] LDT
*> \verbatim
*>          LDT is INTEGER
*>          The leading dimension of the array T. LDT >= K.
*> \endverbatim
*
*  Authors:
*  ========
*
*> \author Univ. of Tennessee
*> \author Univ. of California Berkeley
*> \author Johnathan Rhyne, Univ. of Colorado Denver (original author, 2024)
*> \author NAG Ltd.
*
*> \ingroup larft
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  The shape of the matrix V and the storage of the vectors which define
*>  the H(i) is best illustrated by the following example with n = 5 and
*>  k = 3. The elements equal to 1 are not stored.
*>
*>  DIRECT = 'F' and STOREV = 'C':         DIRECT = 'F' and STOREV = 'R':
*>
*>               V = (  1       )                 V = (  1 v1 v1 v1 v1 )
*>                   ( v1  1    )                     (     1 v2 v2 v2 )
*>                   ( v1 v2  1 )                     (        1 v3 v3 )
*>                   ( v1 v2 v3 )
*>                   ( v1 v2 v3 )
*>
*>  DIRECT = 'B' and STOREV = 'C':         DIRECT = 'B' and STOREV = 'R':
*>
*>               V = ( v1 v2 v3 )                 V = ( v1 v1  1       )
*>                   ( v1 v2 v3 )                     ( v2 v2 v2  1    )
*>                   (  1 v2 v3 )                     ( v3 v3 v3 v3  1 )
*>                   (     1 v3 )
*>                   (        1 )
*> \endverbatim
*>
*  =====================================================================
      RECURSIVE SUBROUTINE SLARFT( DIRECT, STOREV, N, K, V, LDV,
     $                             TAU, T, LDT )
*
*  -- LAPACK auxiliary routine --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*
*        .. Scalar Arguments
*
      CHARACTER          DIRECT, STOREV
      INTEGER            K, LDT, LDV, N
*     ..
*     .. Array Arguments ..
*
      REAL               T( LDT, * ), TAU( * ), V( LDV, * )
*     ..
*
*     .. Parameters ..
*
      REAL               ONE, NEG_ONE, ZERO
      PARAMETER(ONE=1.0E+0, ZERO = 0.0E+0, NEG_ONE=-1.0E+0)
*
*     .. Local Scalars ..
*
      INTEGER            I,J,L
      LOGICAL            QR,LQ,QL,DIRF,COLV
*
*     .. External Subroutines ..
*
      EXTERNAL           STRMM,SGEMM,SLACPY
*
*     .. External Functions..
*
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     
*     The general scheme used is inspired by the approach inside DGEQRT3
*     which was (at the time of writing this code):
*     Based on the algorithm of Elmroth and Gustavson,
*     IBM J. Res. Develop. Vol 44 No. 4 July 2000.
*     ..
*     .. Executable Statements ..
*
*     Quick return if possible
*
      IF(N.EQ.0.OR.K.EQ.0) THEN
         RETURN
      END IF
*
*     Base case
*
      IF(N.EQ.1.OR.K.EQ.1) THEN
         T(1,1) = TAU(1)
         RETURN
      END IF
*
*     Beginning of executable statements
*
      L = K / 2
*
*     Determine what kind of Q we need to compute
*     We assume that if the user doesn't provide 'F' for DIRECT,
*     then they meant to provide 'B' and if they don't provide
*     'C' for STOREV, then they meant to provide 'R'
*
      DIRF = LSAME(DIRECT,'F')
      COLV = LSAME(STOREV,'C')
*
*     QR happens when we have forward direction in column storage
*
      QR = DIRF.AND.COLV
*
*     LQ happens when we have forward direction in row storage
*
      LQ = DIRF.AND.(.NOT.COLV)
*
*     QL happens when we have backward direction in column storage
*
      QL = (.NOT.DIRF).AND.COLV
*
*     The last case is RQ. Due to how we structured this, if the
*     above 3 are false, then RQ must be true, so we never store 
*     this
*     RQ happens when we have backward direction in row storage
*     RQ = (.NOT.DIRF).AND.(.NOT.COLV)
*
      IF(QR) THEN
*
*        Break V apart into 6 components
*
*        V = |---------------|
*            |V_{1,1} 0      |
*            |V_{2,1} V_{2,2}|
*            |V_{3,1} V_{3,2}|
*            |---------------|
*
*        V_{1,1}\in\R^{l,l}      unit lower triangular
*        V_{2,1}\in\R^{k-l,l}    rectangular
*        V_{3,1}\in\R^{n-k,l}    rectangular
*        
*        V_{2,2}\in\R^{k-l,k-l}  unit lower triangular
*        V_{3,2}\in\R^{n-k,k-l}  rectangular
*
*        We will construct the T matrix 
*        T = |---------------|
*            |T_{1,1} T_{1,2}|
*            |0       T_{2,2}|
*            |---------------|
*
*        T is the triangular factor obtained from block reflectors. 
*        To motivate the structure, assume we have already computed T_{1,1}
*        and T_{2,2}. Then collect the associated reflectors in V_1 and V_2
*
*        T_{1,1}\in\R^{l, l}     upper triangular
*        T_{2,2}\in\R^{k-l, k-l} upper triangular
*        T_{1,2}\in\R^{l, k-l}   rectangular
*
*        Where l = floor(k/2)
*
*        Then, consider the product:
*        
*        (I - V_1*T_{1,1}*V_1')*(I - V_2*T_{2,2}*V_2')
*        = I - V_1*T_{1,1}*V_1' - V_2*T_{2,2}*V_2' + V_1*T_{1,1}*V_1'*V_2*T_{2,2}*V_2'
*        
*        Define T_{1,2} = -T_{1,1}*V_1'*V_2*T_{2,2}
*        
*        Then, we can define the matrix V as 
*        V = |-------|
*            |V_1 V_2|
*            |-------|
*        
*        So, our product is equivalent to the matrix product
*        I - V*T*V'
*        This means, we can compute T_{1,1} and T_{2,2}, then use this information
*        to compute T_{1,2}
*
*        Compute T_{1,1} recursively
*
         CALL SLARFT(DIRECT, STOREV, N, L, V, LDV, TAU, T, LDT)
*
*        Compute T_{2,2} recursively
*
         CALL SLARFT(DIRECT, STOREV, N-L, K-L, V(L+1, L+1), LDV, 
     $               TAU(L+1), T(L+1, L+1), LDT)
*
*        Compute T_{1,2} 
*        T_{1,2} = V_{2,1}'
*
         DO J = 1, L
            DO I = 1, K-L
               T(J, L+I) = V(L+I, J)
            END DO
         END DO
*
*        T_{1,2} = T_{1,2}*V_{2,2}
*
         CALL STRMM('Right', 'Lower', 'No transpose', 'Unit', L,
     $               K-L, ONE, V(L+1, L+1), LDV, T(1, L+1), LDT)

*
*        T_{1,2} = V_{3,1}'*V_{3,2} + T_{1,2}
*        Note: We assume K <= N, and GEMM will do nothing if N=K
*
         CALL SGEMM('Transpose', 'No transpose', L, K-L, N-K, ONE, 
     $               V(K+1, 1), LDV, V(K+1, L+1), LDV, ONE, 
     $               T(1, L+1), LDT)
*
*        At this point, we have that T_{1,2} = V_1'*V_2
*        All that is left is to pre and post multiply by -T_{1,1} and T_{2,2}
*        respectively.
*
*        T_{1,2} = -T_{1,1}*T_{1,2}
*
         CALL STRMM('Left', 'Upper', 'No transpose', 'Non-unit', L,
     $               K-L, NEG_ONE, T, LDT, T(1, L+1), LDT)
*
*        T_{1,2} = T_{1,2}*T_{2,2}
*
         CALL STRMM('Right', 'Upper', 'No transpose', 'Non-unit', L, 
     $               K-L, ONE, T(L+1, L+1), LDT, T(1, L+1), LDT)

      ELSE IF(LQ) THEN
*
*        Break V apart into 6 components
*
*        V = |----------------------|
*            |V_{1,1} V_{1,2} V{1,3}|
*            |0       V_{2,2} V{2,3}|
*            |----------------------|
*
*        V_{1,1}\in\R^{l,l}      unit upper triangular
*        V_{1,2}\in\R^{l,k-l}    rectangular
*        V_{1,3}\in\R^{l,n-k}    rectangular
*        
*        V_{2,2}\in\R^{k-l,k-l}  unit upper triangular
*        V_{2,3}\in\R^{k-l,n-k}  rectangular
*
*        Where l = floor(k/2)
*
*        We will construct the T matrix 
*        T = |---------------|
*            |T_{1,1} T_{1,2}|
*            |0       T_{2,2}|
*            |---------------|
*
*        T is the triangular factor obtained from block reflectors. 
*        To motivate the structure, assume we have already computed T_{1,1}
*        and T_{2,2}. Then collect the associated reflectors in V_1 and V_2
*
*        T_{1,1}\in\R^{l, l}     upper triangular
*        T_{2,2}\in\R^{k-l, k-l} upper triangular
*        T_{1,2}\in\R^{l, k-l}   rectangular
*
*        Then, consider the product:
*        
*        (I - V_1'*T_{1,1}*V_1)*(I - V_2'*T_{2,2}*V_2)
*        = I - V_1'*T_{1,1}*V_1 - V_2'*T_{2,2}*V_2 + V_1'*T_{1,1}*V_1*V_2'*T_{2,2}*V_2
*        
*        Define T_{1,2} = -T_{1,1}*V_1*V_2'*T_{2,2}
*        
*        Then, we can define the matrix V as 
*        V = |---|
*            |V_1|
*            |V_2|
*            |---|
*        
*        So, our product is equivalent to the matrix product
*        I - V'*T*V
*        This means, we can compute T_{1,1} and T_{2,2}, then use this information
*        to compute T_{1,2}
*
*        Compute T_{1,1} recursively
*
         CALL SLARFT(DIRECT, STOREV, N, L, V, LDV, TAU, T, LDT)
*
*        Compute T_{2,2} recursively
*
         CALL SLARFT(DIRECT, STOREV, N-L, K-L, V(L+1, L+1), LDV, 
     $               TAU(L+1), T(L+1, L+1), LDT)

*
*        Compute T_{1,2}
*        T_{1,2} = V_{1,2}
*
         CALL SLACPY('All', L, K-L, V(1, L+1), LDV, T(1, L+1), LDT)
*
*        T_{1,2} = T_{1,2}*V_{2,2}'
*
         CALL STRMM('Right', 'Upper', 'Transpose', 'Unit', L, K-L,
     $               ONE, V(L+1, L+1), LDV, T(1, L+1), LDT)

*
*        T_{1,2} = V_{1,3}*V_{2,3}' + T_{1,2}
*        Note: We assume K <= N, and GEMM will do nothing if N=K
*
         CALL SGEMM('No transpose', 'Transpose', L, K-L, N-K, ONE,
     $               V(1, K+1), LDV, V(L+1, K+1), LDV, ONE, 
     $               T(1, L+1), LDT)
*
*        At this point, we have that T_{1,2} = V_1*V_2'
*        All that is left is to pre and post multiply by -T_{1,1} and T_{2,2}
*        respectively.
*
*        T_{1,2} = -T_{1,1}*T_{1,2}
*
         CALL STRMM('Left', 'Upper', 'No transpose', 'Non-unit', L,
     $               K-L, NEG_ONE, T, LDT, T(1, L+1), LDT)

*
*        T_{1,2} = T_{1,2}*T_{2,2}
*
         CALL STRMM('Right', 'Upper', 'No transpose', 'Non-unit', L,
     $               K-L, ONE, T(L+1, L+1), LDT, T(1, L+1), LDT)
      ELSE IF(QL) THEN
*
*        Break V apart into 6 components
*
*        V = |---------------|
*            |V_{1,1} V_{1,2}|
*            |V_{2,1} V_{2,2}|
*            |0       V_{3,2}|
*            |---------------|
*
*        V_{1,1}\in\R^{n-k,k-l}  rectangular
*        V_{2,1}\in\R^{k-l,k-l}  unit upper triangular
*        
*        V_{1,2}\in\R^{n-k,l}    rectangular
*        V_{2,2}\in\R^{k-l,l}    rectangular
*        V_{3,2}\in\R^{l,l}      unit upper triangular
*
*        We will construct the T matrix 
*        T = |---------------|
*            |T_{1,1} 0      |
*            |T_{2,1} T_{2,2}|
*            |---------------|
*
*        T is the triangular factor obtained from block reflectors. 
*        To motivate the structure, assume we have already computed T_{1,1}
*        and T_{2,2}. Then collect the associated reflectors in V_1 and V_2
*
*        T_{1,1}\in\R^{k-l, k-l} non-unit lower triangular
*        T_{2,2}\in\R^{l, l}     non-unit lower triangular
*        T_{2,1}\in\R^{k-l, l}   rectangular
*
*        Where l = floor(k/2)
*
*        Then, consider the product:
*        
*        (I - V_2*T_{2,2}*V_2')*(I - V_1*T_{1,1}*V_1')
*        = I - V_2*T_{2,2}*V_2' - V_1*T_{1,1}*V_1' + V_2*T_{2,2}*V_2'*V_1*T_{1,1}*V_1'
*        
*        Define T_{2,1} = -T_{2,2}*V_2'*V_1*T_{1,1}
*        
*        Then, we can define the matrix V as 
*        V = |-------|
*            |V_1 V_2|
*            |-------|
*        
*        So, our product is equivalent to the matrix product
*        I - V*T*V'
*        This means, we can compute T_{1,1} and T_{2,2}, then use this information
*        to compute T_{2,1}
*
*        Compute T_{1,1} recursively
*
         CALL SLARFT(DIRECT, STOREV, N-L, K-L, V, LDV, TAU, T, LDT)
*
*        Compute T_{2,2} recursively
*
         CALL SLARFT(DIRECT, STOREV, N, L, V(1, K-L+1), LDV,
     $               TAU(K-L+1), T(K-L+1, K-L+1), LDT)
*
*        Compute T_{2,1}
*        T_{2,1} = V_{2,2}'
*
         DO J = 1, K-L
            DO I = 1, L
               T(K-L+I, J) = V(N-K+J, K-L+I)
            END DO
         END DO
*
*        T_{2,1} = T_{2,1}*V_{2,1}
*
         CALL STRMM('Right', 'Upper', 'No transpose', 'Unit', L,
     $               K-L, ONE, V(N-K+1, 1), LDV, T(K-L+1, 1), LDT)

*
*        T_{2,1} = V_{2,2}'*V_{2,1} + T_{2,1}
*        Note: We assume K <= N, and GEMM will do nothing if N=K
*
         CALL SGEMM('Transpose', 'No transpose', L, K-L, N-K, ONE,
     $               V(1, K-L+1), LDV, V, LDV, ONE, T(K-L+1, 1), 
     $               LDT)
*
*        At this point, we have that T_{2,1} = V_2'*V_1
*        All that is left is to pre and post multiply by -T_{2,2} and T_{1,1}
*        respectively.
*
*        T_{2,1} = -T_{2,2}*T_{2,1}
*
         CALL STRMM('Left', 'Lower', 'No transpose', 'Non-unit', L,
     $               K-L, NEG_ONE, T(K-L+1, K-L+1), LDT, 
     $               T(K-L+1, 1), LDT)
*
*        T_{2,1} = T_{2,1}*T_{1,1}
*
         CALL STRMM('Right', 'Lower', 'No transpose', 'Non-unit', L,
     $               K-L, ONE, T, LDT, T(K-L+1, 1), LDT)
      ELSE
*
*        Else means RQ case
*
*        Break V apart into 6 components
*
*        V = |-----------------------|
*            |V_{1,1} V_{1,2} 0      |
*            |V_{2,1} V_{2,2} V_{2,3}|
*            |-----------------------|
*
*        V_{1,1}\in\R^{k-l,n-k}  rectangular
*        V_{1,2}\in\R^{k-l,k-l}  unit lower triangular
*
*        V_{2,1}\in\R^{l,n-k}    rectangular
*        V_{2,2}\in\R^{l,k-l}    rectangular
*        V_{2,3}\in\R^{l,l}      unit lower triangular
*
*        We will construct the T matrix 
*        T = |---------------|
*            |T_{1,1} 0      |
*            |T_{2,1} T_{2,2}|
*            |---------------|
*
*        T is the triangular factor obtained from block reflectors. 
*        To motivate the structure, assume we have already computed T_{1,1}
*        and T_{2,2}. Then collect the associated reflectors in V_1 and V_2
*
*        T_{1,1}\in\R^{k-l, k-l} non-unit lower triangular
*        T_{2,2}\in\R^{l, l}     non-unit lower triangular
*        T_{2,1}\in\R^{k-l, l}   rectangular
*
*        Where l = floor(k/2)
*
*        Then, consider the product:
*        
*        (I - V_2'*T_{2,2}*V_2)*(I - V_1'*T_{1,1}*V_1)
*        = I - V_2'*T_{2,2}*V_2 - V_1'*T_{1,1}*V_1 + V_2'*T_{2,2}*V_2*V_1'*T_{1,1}*V_1
*        
*        Define T_{2,1} = -T_{2,2}*V_2*V_1'*T_{1,1}
*        
*        Then, we can define the matrix V as 
*        V = |---|
*            |V_1|
*            |V_2|
*            |---|
*        
*        So, our product is equivalent to the matrix product
*        I - V'TV
*        This means, we can compute T_{1,1} and T_{2,2}, then use this information
*        to compute T_{2,1}
*
*        Compute T_{1,1} recursively
*
         CALL SLARFT(DIRECT, STOREV, N-L, K-L, V, LDV, TAU, T, LDT)
*
*        Compute T_{2,2} recursively
*
         CALL SLARFT(DIRECT, STOREV, N, L, V(K-L+1, 1), LDV,
     $               TAU(K-L+1), T(K-L+1, K-L+1), LDT)
*
*        Compute T_{2,1}
*        T_{2,1} = V_{2,2}
*
         CALL SLACPY('All', L, K-L, V(K-L+1, N-K+1), LDV,
     $      T(K-L+1, 1), LDT)

*
*        T_{2,1} = T_{2,1}*V_{1,2}'
*
         CALL STRMM('Right', 'Lower', 'Transpose', 'Unit', L, K-L,
     $               ONE, V(1, N-K+1), LDV, T(K-L+1, 1), LDT)

*
*        T_{2,1} = V_{2,1}*V_{1,1}' + T_{2,1} 
*        Note: We assume K <= N, and GEMM will do nothing if N=K
*
         CALL SGEMM('No transpose', 'Transpose', L, K-L, N-K, ONE, 
     $               V(K-L+1, 1), LDV, V, LDV, ONE, T(K-L+1, 1),
     $               LDT)

*
*        At this point, we have that T_{2,1} = V_2*V_1'
*        All that is left is to pre and post multiply by -T_{2,2} and T_{1,1}
*        respectively.
*
*        T_{2,1} = -T_{2,2}*T_{2,1}
*
         CALL STRMM('Left', 'Lower', 'No tranpose', 'Non-unit', L,
     $               K-L, NEG_ONE, T(K-L+1, K-L+1), LDT, 
     $               T(K-L+1, 1), LDT)

*
*        T_{2,1} = T_{2,1}*T_{1,1}
*
         CALL STRMM('Right', 'Lower', 'No tranpose', 'Non-unit', L,
     $               K-L, ONE, T, LDT, T(K-L+1, 1), LDT)
      END IF
      END SUBROUTINE
