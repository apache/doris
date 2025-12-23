*> \brief <b> CGESVDQ computes the singular value decomposition (SVD) with a QR-Preconditioned QR SVD Method for GE matrices</b>
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CGESVDQ + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/cgesvdq.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/cgesvdq.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/cgesvdq.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*      SUBROUTINE CGESVDQ( JOBA, JOBP, JOBR, JOBU, JOBV, M, N, A, LDA,
*                          S, U, LDU, V, LDV, NUMRANK, IWORK, LIWORK,
*                          CWORK, LCWORK, RWORK, LRWORK, INFO )
*
*     .. Scalar Arguments ..
*      IMPLICIT    NONE
*      CHARACTER   JOBA, JOBP, JOBR, JOBU, JOBV
*      INTEGER     M, N, LDA, LDU, LDV, NUMRANK, LIWORK, LCWORK, LRWORK,
*                  INFO
*     ..
*     .. Array Arguments ..
*      COMPLEX     A( LDA, * ), U( LDU, * ), V( LDV, * ), CWORK( * )
*      REAL        S( * ), RWORK( * )
*      INTEGER     IWORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CGESVDQ computes the singular value decomposition (SVD) of a complex
*> M-by-N matrix A, where M >= N. The SVD of A is written as
*>                                    [++]   [xx]   [x0]   [xx]
*>              A = U * SIGMA * V^*,  [++] = [xx] * [ox] * [xx]
*>                                    [++]   [xx]
*> where SIGMA is an N-by-N diagonal matrix, U is an M-by-N orthonormal
*> matrix, and V is an N-by-N unitary matrix. The diagonal elements
*> of SIGMA are the singular values of A. The columns of U and V are the
*> left and the right singular vectors of A, respectively.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] JOBA
*> \verbatim
*>  JOBA is CHARACTER*1
*>  Specifies the level of accuracy in the computed SVD
*>  = 'A' The requested accuracy corresponds to having the backward
*>        error bounded by || delta A ||_F <= f(m,n) * EPS * || A ||_F,
*>        where EPS = SLAMCH('Epsilon'). This authorises CGESVDQ to
*>        truncate the computed triangular factor in a rank revealing
*>        QR factorization whenever the truncated part is below the
*>        threshold of the order of EPS * ||A||_F. This is aggressive
*>        truncation level.
*>  = 'M' Similarly as with 'A', but the truncation is more gentle: it
*>        is allowed only when there is a drop on the diagonal of the
*>        triangular factor in the QR factorization. This is medium
*>        truncation level.
*>  = 'H' High accuracy requested. No numerical rank determination based
*>        on the rank revealing QR factorization is attempted.
*>  = 'E' Same as 'H', and in addition the condition number of column
*>        scaled A is estimated and returned in  RWORK(1).
*>        N^(-1/4)*RWORK(1) <= ||pinv(A_scaled)||_2 <= N^(1/4)*RWORK(1)
*> \endverbatim
*>
*> \param[in] JOBP
*> \verbatim
*>  JOBP is CHARACTER*1
*>  = 'P' The rows of A are ordered in decreasing order with respect to
*>        ||A(i,:)||_\infty. This enhances numerical accuracy at the cost
*>        of extra data movement. Recommended for numerical robustness.
*>  = 'N' No row pivoting.
*> \endverbatim
*>
*> \param[in] JOBR
*> \verbatim
*>          JOBR is CHARACTER*1
*>          = 'T' After the initial pivoted QR factorization, CGESVD is applied to
*>          the adjoint R**H of the computed triangular factor R. This involves
*>          some extra data movement (matrix transpositions). Useful for
*>          experiments, research and development.
*>          = 'N' The triangular factor R is given as input to CGESVD. This may be
*>          preferred as it involves less data movement.
*> \endverbatim
*>
*> \param[in] JOBU
*> \verbatim
*>          JOBU is CHARACTER*1
*>          = 'A' All M left singular vectors are computed and returned in the
*>          matrix U. See the description of U.
*>          = 'S' or 'U' N = min(M,N) left singular vectors are computed and returned
*>          in the matrix U. See the description of U.
*>          = 'R' Numerical rank NUMRANK is determined and only NUMRANK left singular
*>          vectors are computed and returned in the matrix U.
*>          = 'F' The N left singular vectors are returned in factored form as the
*>          product of the Q factor from the initial QR factorization and the
*>          N left singular vectors of (R**H , 0)**H. If row pivoting is used,
*>          then the necessary information on the row pivoting is stored in
*>          IWORK(N+1:N+M-1).
*>          = 'N' The left singular vectors are not computed.
*> \endverbatim
*>
*> \param[in] JOBV
*> \verbatim
*>          JOBV is CHARACTER*1
*>          = 'A', 'V' All N right singular vectors are computed and returned in
*>          the matrix V.
*>          = 'R' Numerical rank NUMRANK is determined and only NUMRANK right singular
*>          vectors are computed and returned in the matrix V. This option is
*>          allowed only if JOBU = 'R' or JOBU = 'N'; otherwise it is illegal.
*>          = 'N' The right singular vectors are not computed.
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the input matrix A.  M >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the input matrix A.  M >= N >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX array of dimensions LDA x N
*>          On entry, the input matrix A.
*>          On exit, if JOBU .NE. 'N' or JOBV .NE. 'N', the lower triangle of A contains
*>          the Householder vectors as stored by CGEQP3. If JOBU = 'F', these Householder
*>          vectors together with CWORK(1:N) can be used to restore the Q factors from
*>          the initial pivoted QR factorization of A. See the description of U.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER.
*>          The leading dimension of the array A.  LDA >= max(1,M).
*> \endverbatim
*>
*> \param[out] S
*> \verbatim
*>          S is REAL array of dimension N.
*>          The singular values of A, ordered so that S(i) >= S(i+1).
*> \endverbatim
*>
*> \param[out] U
*> \verbatim
*>          U is COMPLEX array, dimension
*>          LDU x M if JOBU = 'A'; see the description of LDU. In this case,
*>          on exit, U contains the M left singular vectors.
*>          LDU x N if JOBU = 'S', 'U', 'R' ; see the description of LDU. In this
*>          case, U contains the leading N or the leading NUMRANK left singular vectors.
*>          LDU x N if JOBU = 'F' ; see the description of LDU. In this case U
*>          contains N x N unitary matrix that can be used to form the left
*>          singular vectors.
*>          If JOBU = 'N', U is not referenced.
*> \endverbatim
*>
*> \param[in] LDU
*> \verbatim
*>          LDU is INTEGER.
*>          The leading dimension of the array U.
*>          If JOBU = 'A', 'S', 'U', 'R',  LDU >= max(1,M).
*>          If JOBU = 'F',                 LDU >= max(1,N).
*>          Otherwise,                     LDU >= 1.
*> \endverbatim
*>
*> \param[out] V
*> \verbatim
*>          V is COMPLEX array, dimension
*>          LDV x N if JOBV = 'A', 'V', 'R' or if JOBA = 'E' .
*>          If JOBV = 'A', or 'V',  V contains the N-by-N unitary matrix  V**H;
*>          If JOBV = 'R', V contains the first NUMRANK rows of V**H (the right
*>          singular vectors, stored rowwise, of the NUMRANK largest singular values).
*>          If JOBV = 'N' and JOBA = 'E', V is used as a workspace.
*>          If JOBV = 'N', and JOBA.NE.'E', V is not referenced.
*> \endverbatim
*>
*> \param[in] LDV
*> \verbatim
*>          LDV is INTEGER
*>          The leading dimension of the array V.
*>          If JOBV = 'A', 'V', 'R',  or JOBA = 'E', LDV >= max(1,N).
*>          Otherwise,                               LDV >= 1.
*> \endverbatim
*>
*> \param[out] NUMRANK
*> \verbatim
*>          NUMRANK is INTEGER
*>          NUMRANK is the numerical rank first determined after the rank
*>          revealing QR factorization, following the strategy specified by the
*>          value of JOBA. If JOBV = 'R' and JOBU = 'R', only NUMRANK
*>          leading singular values and vectors are then requested in the call
*>          of CGESVD. The final value of NUMRANK might be further reduced if
*>          some singular values are computed as zeros.
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (max(1, LIWORK)).
*>          On exit, IWORK(1:N) contains column pivoting permutation of the
*>          rank revealing QR factorization.
*>          If JOBP = 'P', IWORK(N+1:N+M-1) contains the indices of the sequence
*>          of row swaps used in row pivoting. These can be used to restore the
*>          left singular vectors in the case JOBU = 'F'.
*>
*>          If LIWORK, LCWORK, or LRWORK = -1, then on exit, if INFO = 0,
*>          IWORK(1) returns the minimal LIWORK.
*> \endverbatim
*>
*> \param[in] LIWORK
*> \verbatim
*>          LIWORK is INTEGER
*>          The dimension of the array IWORK.
*>          LIWORK >= N + M - 1,  if JOBP = 'P';
*>          LIWORK >= N           if JOBP = 'N'.
*>
*>          If LIWORK = -1, then a workspace query is assumed; the routine
*>          only calculates and returns the optimal and minimal sizes
*>          for the CWORK, IWORK, and RWORK arrays, and no error
*>          message related to LCWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] CWORK
*> \verbatim
*>          CWORK is COMPLEX array, dimension (max(2, LCWORK)), used as a workspace.
*>          On exit, if, on entry, LCWORK.NE.-1, CWORK(1:N) contains parameters
*>          needed to recover the Q factor from the QR factorization computed by
*>          CGEQP3.
*>
*>          If LIWORK, LCWORK, or LRWORK = -1, then on exit, if INFO = 0,
*>          CWORK(1) returns the optimal LCWORK, and
*>          CWORK(2) returns the minimal LCWORK.
*> \endverbatim
*>
*> \param[in,out] LCWORK
*> \verbatim
*>          LCWORK is INTEGER
*>          The dimension of the array CWORK. It is determined as follows:
*>          Let  LWQP3 = N+1,  LWCON = 2*N, and let
*>          LWUNQ = { MAX( N, 1 ),  if JOBU = 'R', 'S', or 'U'
*>                  { MAX( M, 1 ),  if JOBU = 'A'
*>          LWSVD = MAX( 3*N, 1 )
*>          LWLQF = MAX( N/2, 1 ), LWSVD2 = MAX( 3*(N/2), 1 ), LWUNLQ = MAX( N, 1 ),
*>          LWQRF = MAX( N/2, 1 ), LWUNQ2 = MAX( N, 1 )
*>          Then the minimal value of LCWORK is:
*>          = MAX( N + LWQP3, LWSVD )        if only the singular values are needed;
*>          = MAX( N + LWQP3, LWCON, LWSVD ) if only the singular values are needed,
*>                                   and a scaled condition estimate requested;
*>
*>          = N + MAX( LWQP3, LWSVD, LWUNQ ) if the singular values and the left
*>                                   singular vectors are requested;
*>          = N + MAX( LWQP3, LWCON, LWSVD, LWUNQ ) if the singular values and the left
*>                                   singular vectors are requested, and also
*>                                   a scaled condition estimate requested;
*>
*>          = N + MAX( LWQP3, LWSVD )        if the singular values and the right
*>                                   singular vectors are requested;
*>          = N + MAX( LWQP3, LWCON, LWSVD ) if the singular values and the right
*>                                   singular vectors are requested, and also
*>                                   a scaled condition etimate requested;
*>
*>          = N + MAX( LWQP3, LWSVD, LWUNQ ) if the full SVD is requested with JOBV = 'R';
*>                                   independent of JOBR;
*>          = N + MAX( LWQP3, LWCON, LWSVD, LWUNQ ) if the full SVD is requested,
*>                                   JOBV = 'R' and, also a scaled condition
*>                                   estimate requested; independent of JOBR;
*>          = MAX( N + MAX( LWQP3, LWSVD, LWUNQ ),
*>         N + MAX( LWQP3, N/2+LWLQF, N/2+LWSVD2, N/2+LWUNLQ, LWUNQ) ) if the
*>                         full SVD is requested with JOBV = 'A' or 'V', and
*>                         JOBR ='N'
*>          = MAX( N + MAX( LWQP3, LWCON, LWSVD, LWUNQ ),
*>         N + MAX( LWQP3, LWCON, N/2+LWLQF, N/2+LWSVD2, N/2+LWUNLQ, LWUNQ ) )
*>                         if the full SVD is requested with JOBV = 'A' or 'V', and
*>                         JOBR ='N', and also a scaled condition number estimate
*>                         requested.
*>          = MAX( N + MAX( LWQP3, LWSVD, LWUNQ ),
*>         N + MAX( LWQP3, N/2+LWQRF, N/2+LWSVD2, N/2+LWUNQ2, LWUNQ ) ) if the
*>                         full SVD is requested with JOBV = 'A', 'V', and JOBR ='T'
*>          = MAX( N + MAX( LWQP3, LWCON, LWSVD, LWUNQ ),
*>         N + MAX( LWQP3, LWCON, N/2+LWQRF, N/2+LWSVD2, N/2+LWUNQ2, LWUNQ ) )
*>                         if the full SVD is requested with JOBV = 'A', 'V' and
*>                         JOBR ='T', and also a scaled condition number estimate
*>                         requested.
*>          Finally, LCWORK must be at least two: LCWORK = MAX( 2, LCWORK ).
*>
*>          If LCWORK = -1, then a workspace query is assumed; the routine
*>          only calculates and returns the optimal and minimal sizes
*>          for the CWORK, IWORK, and RWORK arrays, and no error
*>          message related to LCWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is REAL array, dimension (max(1, LRWORK)).
*>          On exit,
*>          1. If JOBA = 'E', RWORK(1) contains an estimate of the condition
*>          number of column scaled A. If A = C * D where D is diagonal and C
*>          has unit columns in the Euclidean norm, then, assuming full column rank,
*>          N^(-1/4) * RWORK(1) <= ||pinv(C)||_2 <= N^(1/4) * RWORK(1).
*>          Otherwise, RWORK(1) = -1.
*>          2. RWORK(2) contains the number of singular values computed as
*>          exact zeros in CGESVD applied to the upper triangular or trapezoidal
*>          R (from the initial QR factorization). In case of early exit (no call to
*>          CGESVD, such as in the case of zero matrix) RWORK(2) = -1.
*>
*>          If LIWORK, LCWORK, or LRWORK = -1, then on exit, if INFO = 0,
*>          RWORK(1) returns the minimal LRWORK.
*> \endverbatim
*>
*> \param[in] LRWORK
*> \verbatim
*>          LRWORK is INTEGER.
*>          The dimension of the array RWORK.
*>          If JOBP ='P', then LRWORK >= MAX(2, M, 5*N);
*>          Otherwise, LRWORK >= MAX(2, 5*N).
*>
*>          If LRWORK = -1, then a workspace query is assumed; the routine
*>          only calculates and returns the optimal and minimal sizes
*>          for the CWORK, IWORK, and RWORK arrays, and no error
*>          message related to LCWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit.
*>          < 0:  if INFO = -i, the i-th argument had an illegal value.
*>          > 0:  if CBDSQR did not converge, INFO specifies how many superdiagonals
*>          of an intermediate bidiagonal form B (computed in CGESVD) did not
*>          converge to zero.
*> \endverbatim
*
*> \par Further Details:
*  ========================
*>
*> \verbatim
*>
*>   1. The data movement (matrix transpose) is coded using simple nested
*>   DO-loops because BLAS and LAPACK do not provide corresponding subroutines.
*>   Those DO-loops are easily identified in this source code - by the CONTINUE
*>   statements labeled with 11**. In an optimized version of this code, the
*>   nested DO loops should be replaced with calls to an optimized subroutine.
*>   2. This code scales A by 1/SQRT(M) if the largest ABS(A(i,j)) could cause
*>   column norm overflow. This is the minial precaution and it is left to the
*>   SVD routine (CGESVD) to do its own preemptive scaling if potential over-
*>   or underflows are detected. To avoid repeated scanning of the array A,
*>   an optimal implementation would do all necessary scaling before calling
*>   CGESVD and the scaling in CGESVD can be switched off.
*>   3. Other comments related to code optimization are given in comments in the
*>   code, enclosed in [[double brackets]].
*> \endverbatim
*
*> \par Bugs, examples and comments
*  ===========================
*
*> \verbatim
*>  Please report all bugs and send interesting examples and/or comments to
*>  drmac@math.hr. Thank you.
*> \endverbatim
*
*> \par References
*  ===============
*
*> \verbatim
*>  [1] Zlatko Drmac, Algorithm 977: A QR-Preconditioned QR SVD Method for
*>      Computing the SVD with High Accuracy. ACM Trans. Math. Softw.
*>      44(1): 11:1-11:30 (2017)
*>
*>  SIGMA library, xGESVDQ section updated February 2016.
*>  Developed and coded by Zlatko Drmac, Department of Mathematics
*>  University of Zagreb, Croatia, drmac@math.hr
*> \endverbatim
*
*
*> \par Contributors:
*  ==================
*>
*> \verbatim
*> Developed and coded by Zlatko Drmac, Department of Mathematics
*>  University of Zagreb, Croatia, drmac@math.hr
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
*> \ingroup complexGEsing
*
*  =====================================================================
      SUBROUTINE CGESVDQ( JOBA, JOBP, JOBR, JOBU, JOBV, M, N, A, LDA,
     $                    S, U, LDU, V, LDV, NUMRANK, IWORK, LIWORK,
     $                    CWORK, LCWORK, RWORK, LRWORK, INFO )
*     .. Scalar Arguments ..
      IMPLICIT    NONE
      CHARACTER   JOBA, JOBP, JOBR, JOBU, JOBV
      INTEGER     M, N, LDA, LDU, LDV, NUMRANK, LIWORK, LCWORK, LRWORK,
     $            INFO
*     ..
*     .. Array Arguments ..
      COMPLEX     A( LDA, * ), U( LDU, * ), V( LDV, * ), CWORK( * )
      REAL        S( * ), RWORK( * )
      INTEGER     IWORK( * )
*
*  =====================================================================
*
*     .. Parameters ..
      REAL        ZERO,         ONE
      PARAMETER ( ZERO = 0.0E0, ONE = 1.0E0 )
      COMPLEX     CZERO,                    CONE
      PARAMETER ( CZERO = ( 0.0E0, 0.0E0 ), CONE = ( 1.0E0, 0.0E0 ) )
*     ..
*     .. Local Scalars ..
      INTEGER     IERR, NR, N1, OPTRATIO, p, q
      INTEGER     LWCON, LWQP3, LWRK_CGELQF, LWRK_CGESVD, LWRK_CGESVD2,
     $            LWRK_CGEQP3, LWRK_CGEQRF, LWRK_CUNMLQ, LWRK_CUNMQR,
     $            LWRK_CUNMQR2, LWLQF, LWQRF, LWSVD, LWSVD2, LWUNQ,
     $            LWUNQ2, LWUNLQ, MINWRK, MINWRK2, OPTWRK, OPTWRK2,
     $            IMINWRK, RMINWRK
      LOGICAL     ACCLA,  ACCLM, ACCLH, ASCALED, CONDA, DNTWU,  DNTWV,
     $            LQUERY, LSVC0, LSVEC, ROWPRM,  RSVEC, RTRANS, WNTUA,
     $            WNTUF,  WNTUR, WNTUS, WNTVA,   WNTVR
      REAL        BIG, EPSLN, RTMP, SCONDA, SFMIN
      COMPLEX     CTMP
*     ..
*     .. Local Arrays
      COMPLEX     CDUMMY(1)
      REAL        RDUMMY(1)
*     ..
*     .. External Subroutines (BLAS, LAPACK)
      EXTERNAL    CGELQF, CGEQP3, CGEQRF, CGESVD, CLACPY, CLAPMT,
     $            CLASCL, CLASET, CLASWP, CSSCAL, SLASET, SLASCL,
     $            CPOCON, CUNMLQ, CUNMQR, XERBLA
*     ..
*     .. External Functions (BLAS, LAPACK)
      LOGICAL    LSAME
      INTEGER    ISAMAX
      REAL       CLANGE, SCNRM2, SLAMCH
      EXTERNAL   CLANGE, LSAME, ISAMAX, SCNRM2, SLAMCH
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC   ABS, CONJG, MAX, MIN, REAL, SQRT
*     ..
*     .. Executable Statements ..
*
*     Test the input arguments
*
      WNTUS  = LSAME( JOBU, 'S' ) .OR. LSAME( JOBU, 'U' )
      WNTUR  = LSAME( JOBU, 'R' )
      WNTUA  = LSAME( JOBU, 'A' )
      WNTUF  = LSAME( JOBU, 'F' )
      LSVC0  = WNTUS .OR. WNTUR .OR. WNTUA
      LSVEC  = LSVC0 .OR. WNTUF
      DNTWU  = LSAME( JOBU, 'N' )
*
      WNTVR  = LSAME( JOBV, 'R' )
      WNTVA  = LSAME( JOBV, 'A' ) .OR. LSAME( JOBV, 'V' )
      RSVEC  = WNTVR .OR. WNTVA
      DNTWV  = LSAME( JOBV, 'N' )
*
      ACCLA  = LSAME( JOBA, 'A' )
      ACCLM  = LSAME( JOBA, 'M' )
      CONDA  = LSAME( JOBA, 'E' )
      ACCLH  = LSAME( JOBA, 'H' ) .OR. CONDA
*
      ROWPRM = LSAME( JOBP, 'P' )
      RTRANS = LSAME( JOBR, 'T' )
*
      IF ( ROWPRM ) THEN
         IMINWRK = MAX( 1, N + M - 1 )
         RMINWRK = MAX( 2, M, 5*N )
      ELSE
         IMINWRK = MAX( 1, N )
         RMINWRK = MAX( 2, 5*N )
      END IF
      LQUERY = (LIWORK .EQ. -1 .OR. LCWORK .EQ. -1 .OR. LRWORK .EQ. -1)
      INFO  = 0
      IF ( .NOT. ( ACCLA .OR. ACCLM .OR. ACCLH ) ) THEN
         INFO = -1
      ELSE IF ( .NOT.( ROWPRM .OR. LSAME( JOBP, 'N' ) ) ) THEN
          INFO = -2
      ELSE IF ( .NOT.( RTRANS .OR. LSAME( JOBR, 'N' ) ) ) THEN
          INFO = -3
      ELSE IF ( .NOT.( LSVEC .OR. DNTWU ) ) THEN
         INFO = -4
      ELSE IF ( WNTUR .AND. WNTVA ) THEN
         INFO = -5
      ELSE IF ( .NOT.( RSVEC .OR. DNTWV )) THEN
         INFO = -5
      ELSE IF ( M.LT.0 ) THEN
         INFO = -6
      ELSE IF ( ( N.LT.0 ) .OR. ( N.GT.M ) ) THEN
         INFO = -7
      ELSE IF ( LDA.LT.MAX( 1, M ) ) THEN
         INFO = -9
      ELSE IF ( LDU.LT.1 .OR. ( LSVC0 .AND. LDU.LT.M ) .OR.
     $       ( WNTUF .AND. LDU.LT.N ) ) THEN
         INFO = -12
      ELSE IF ( LDV.LT.1 .OR. ( RSVEC .AND. LDV.LT.N ) .OR.
     $          ( CONDA .AND. LDV.LT.N ) ) THEN
         INFO = -14
      ELSE IF ( LIWORK .LT. IMINWRK .AND. .NOT. LQUERY ) THEN
         INFO = -17
      END IF
*
*
      IF ( INFO .EQ. 0 ) THEN
*
*     Compute workspace
*        .. compute the minimal and the optimal workspace lengths
*        [[The expressions for computing the minimal and the optimal
*        values of LCWORK are written with a lot of redundancy and
*        can be simplified. However, this detailed form is easier for
*        maintenance and modifications of the code.]]
*
*        .. minimal workspace length for CGEQP3 of an M x N matrix
         LWQP3 = N+1
*        .. minimal workspace length for CUNMQR to build left singular vectors
         IF ( WNTUS .OR. WNTUR ) THEN
             LWUNQ  = MAX( N  , 1 )
         ELSE IF ( WNTUA ) THEN
             LWUNQ = MAX( M , 1 )
         END IF
*        .. minimal workspace length for CPOCON of an N x N matrix
         LWCON = 2 * N
*        .. CGESVD of an N x N matrix
         LWSVD = MAX( 3 * N, 1 )
         IF ( LQUERY ) THEN
             CALL CGEQP3( M, N, A, LDA, IWORK, CDUMMY, CDUMMY, -1,
     $            RDUMMY, IERR )
             LWRK_CGEQP3 = INT( CDUMMY(1) )
             IF ( WNTUS .OR. WNTUR ) THEN
                 CALL CUNMQR( 'L', 'N', M, N, N, A, LDA, CDUMMY, U,
     $                LDU, CDUMMY, -1, IERR )
                 LWRK_CUNMQR = INT( CDUMMY(1) )
             ELSE IF ( WNTUA ) THEN
                 CALL CUNMQR( 'L', 'N', M, M, N, A, LDA, CDUMMY, U,
     $                LDU, CDUMMY, -1, IERR )
                 LWRK_CUNMQR = INT( CDUMMY(1) )
             ELSE
                 LWRK_CUNMQR = 0
             END IF
         END IF
         MINWRK = 2
         OPTWRK = 2
         IF ( .NOT. (LSVEC .OR. RSVEC )) THEN
*            .. minimal and optimal sizes of the complex workspace if
*            only the singular values are requested
             IF ( CONDA ) THEN
                MINWRK = MAX( N+LWQP3, LWCON, LWSVD )
             ELSE
                MINWRK = MAX( N+LWQP3, LWSVD )
             END IF
             IF ( LQUERY ) THEN
                 CALL CGESVD( 'N', 'N', N, N, A, LDA, S, U, LDU,
     $                V, LDV, CDUMMY, -1, RDUMMY, IERR )
                 LWRK_CGESVD = INT( CDUMMY(1) )
                 IF ( CONDA ) THEN
                    OPTWRK = MAX( N+LWRK_CGEQP3, N+LWCON, LWRK_CGESVD )
                 ELSE
                    OPTWRK = MAX( N+LWRK_CGEQP3, LWRK_CGESVD )
                 END IF
             END IF
         ELSE IF ( LSVEC .AND. (.NOT.RSVEC) ) THEN
*            .. minimal and optimal sizes of the complex workspace if the
*            singular values and the left singular vectors are requested
             IF ( CONDA ) THEN
                 MINWRK = N + MAX( LWQP3, LWCON, LWSVD, LWUNQ )
             ELSE
                 MINWRK = N + MAX( LWQP3, LWSVD, LWUNQ )
             END IF
             IF ( LQUERY ) THEN
                IF ( RTRANS ) THEN
                   CALL CGESVD( 'N', 'O', N, N, A, LDA, S, U, LDU,
     $                  V, LDV, CDUMMY, -1, RDUMMY, IERR )
                ELSE
                   CALL CGESVD( 'O', 'N', N, N, A, LDA, S, U, LDU,
     $                  V, LDV, CDUMMY, -1, RDUMMY, IERR )
                END IF
                LWRK_CGESVD = INT( CDUMMY(1) )
                IF ( CONDA ) THEN
                    OPTWRK = N + MAX( LWRK_CGEQP3, LWCON, LWRK_CGESVD,
     $                               LWRK_CUNMQR )
                ELSE
                    OPTWRK = N + MAX( LWRK_CGEQP3, LWRK_CGESVD,
     $                               LWRK_CUNMQR )
                END IF
             END IF
         ELSE IF ( RSVEC .AND. (.NOT.LSVEC) ) THEN
*            .. minimal and optimal sizes of the complex workspace if the
*            singular values and the right singular vectors are requested
             IF ( CONDA ) THEN
                 MINWRK = N + MAX( LWQP3, LWCON, LWSVD )
             ELSE
                 MINWRK = N + MAX( LWQP3, LWSVD )
             END IF
             IF ( LQUERY ) THEN
                 IF ( RTRANS ) THEN
                     CALL CGESVD( 'O', 'N', N, N, A, LDA, S, U, LDU,
     $                    V, LDV, CDUMMY, -1, RDUMMY, IERR )
                 ELSE
                     CALL CGESVD( 'N', 'O', N, N, A, LDA, S, U, LDU,
     $                    V, LDV, CDUMMY, -1, RDUMMY, IERR )
                 END IF
                 LWRK_CGESVD = INT( CDUMMY(1) )
                 IF ( CONDA ) THEN
                     OPTWRK = N + MAX( LWRK_CGEQP3, LWCON, LWRK_CGESVD )
                 ELSE
                     OPTWRK = N + MAX( LWRK_CGEQP3, LWRK_CGESVD )
                 END IF
             END IF
         ELSE
*            .. minimal and optimal sizes of the complex workspace if the
*            full SVD is requested
             IF ( RTRANS ) THEN
                 MINWRK = MAX( LWQP3, LWSVD, LWUNQ )
                 IF ( CONDA ) MINWRK = MAX( MINWRK, LWCON )
                 MINWRK = MINWRK + N
                 IF ( WNTVA ) THEN
*                   .. minimal workspace length for N x N/2 CGEQRF
                    LWQRF  = MAX( N/2, 1 )
*                   .. minimal workspace length for N/2 x N/2 CGESVD
                    LWSVD2 = MAX( 3 * (N/2), 1 )
                    LWUNQ2 = MAX( N, 1 )
                    MINWRK2 = MAX( LWQP3, N/2+LWQRF, N/2+LWSVD2,
     $                        N/2+LWUNQ2, LWUNQ )
                    IF ( CONDA ) MINWRK2 = MAX( MINWRK2, LWCON )
                    MINWRK2 = N + MINWRK2
                    MINWRK = MAX( MINWRK, MINWRK2 )
                 END IF
             ELSE
                 MINWRK = MAX( LWQP3, LWSVD, LWUNQ )
                 IF ( CONDA ) MINWRK = MAX( MINWRK, LWCON )
                 MINWRK = MINWRK + N
                 IF ( WNTVA ) THEN
*                   .. minimal workspace length for N/2 x N CGELQF
                    LWLQF  = MAX( N/2, 1 )
                    LWSVD2 = MAX( 3 * (N/2), 1 )
                    LWUNLQ = MAX( N , 1 )
                    MINWRK2 = MAX( LWQP3, N/2+LWLQF, N/2+LWSVD2,
     $                        N/2+LWUNLQ, LWUNQ )
                    IF ( CONDA ) MINWRK2 = MAX( MINWRK2, LWCON )
                    MINWRK2 = N + MINWRK2
                    MINWRK = MAX( MINWRK, MINWRK2 )
                 END IF
             END IF
             IF ( LQUERY ) THEN
                IF ( RTRANS ) THEN
                   CALL CGESVD( 'O', 'A', N, N, A, LDA, S, U, LDU,
     $                  V, LDV, CDUMMY, -1, RDUMMY, IERR )
                   LWRK_CGESVD = INT( CDUMMY(1) )
                   OPTWRK = MAX(LWRK_CGEQP3,LWRK_CGESVD,LWRK_CUNMQR)
                   IF ( CONDA ) OPTWRK = MAX( OPTWRK, LWCON )
                   OPTWRK = N + OPTWRK
                   IF ( WNTVA ) THEN
                       CALL CGEQRF(N,N/2,U,LDU,CDUMMY,CDUMMY,-1,IERR)
                       LWRK_CGEQRF = INT( CDUMMY(1) )
                       CALL CGESVD( 'S', 'O', N/2,N/2, V,LDV, S, U,LDU,
     $                      V, LDV, CDUMMY, -1, RDUMMY, IERR )
                       LWRK_CGESVD2 = INT( CDUMMY(1) )
                       CALL CUNMQR( 'R', 'C', N, N, N/2, U, LDU, CDUMMY,
     $                      V, LDV, CDUMMY, -1, IERR )
                       LWRK_CUNMQR2 = INT( CDUMMY(1) )
                       OPTWRK2 = MAX( LWRK_CGEQP3, N/2+LWRK_CGEQRF,
     $                           N/2+LWRK_CGESVD2, N/2+LWRK_CUNMQR2 )
                       IF ( CONDA ) OPTWRK2 = MAX( OPTWRK2, LWCON )
                       OPTWRK2 = N + OPTWRK2
                       OPTWRK = MAX( OPTWRK, OPTWRK2 )
                   END IF
                ELSE
                   CALL CGESVD( 'S', 'O', N, N, A, LDA, S, U, LDU,
     $                  V, LDV, CDUMMY, -1, RDUMMY, IERR )
                   LWRK_CGESVD = INT( CDUMMY(1) )
                   OPTWRK = MAX(LWRK_CGEQP3,LWRK_CGESVD,LWRK_CUNMQR)
                   IF ( CONDA ) OPTWRK = MAX( OPTWRK, LWCON )
                   OPTWRK = N + OPTWRK
                   IF ( WNTVA ) THEN
                      CALL CGELQF(N/2,N,U,LDU,CDUMMY,CDUMMY,-1,IERR)
                      LWRK_CGELQF = INT( CDUMMY(1) )
                      CALL CGESVD( 'S','O', N/2,N/2, V, LDV, S, U, LDU,
     $                     V, LDV, CDUMMY, -1, RDUMMY, IERR )
                      LWRK_CGESVD2 = INT( CDUMMY(1) )
                      CALL CUNMLQ( 'R', 'N', N, N, N/2, U, LDU, CDUMMY,
     $                     V, LDV, CDUMMY,-1,IERR )
                      LWRK_CUNMLQ = INT( CDUMMY(1) )
                      OPTWRK2 = MAX( LWRK_CGEQP3, N/2+LWRK_CGELQF,
     $                           N/2+LWRK_CGESVD2, N/2+LWRK_CUNMLQ )
                       IF ( CONDA ) OPTWRK2 = MAX( OPTWRK2, LWCON )
                       OPTWRK2 = N + OPTWRK2
                       OPTWRK = MAX( OPTWRK, OPTWRK2 )
                   END IF
                END IF
             END IF
         END IF
*
         MINWRK = MAX( 2, MINWRK )
         OPTWRK = MAX( 2, OPTWRK )
         IF ( LCWORK .LT. MINWRK .AND. (.NOT.LQUERY) ) INFO = -19
*
      END IF
*
      IF (INFO .EQ. 0 .AND. LRWORK .LT. RMINWRK .AND. .NOT. LQUERY) THEN
         INFO = -21
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'CGESVDQ', -INFO )
         RETURN
      ELSE IF ( LQUERY ) THEN
*
*     Return optimal workspace
*
          IWORK(1) = IMINWRK
          CWORK(1) = OPTWRK
          CWORK(2) = MINWRK
          RWORK(1) = RMINWRK
          RETURN
      END IF
*
*     Quick return if the matrix is void.
*
      IF( ( M.EQ.0 ) .OR. ( N.EQ.0 ) ) THEN
*     .. all output is void.
         RETURN
      END IF
*
      BIG = SLAMCH('O')
      ASCALED = .FALSE.
      IF ( ROWPRM ) THEN
*           .. reordering the rows in decreasing sequence in the
*           ell-infinity norm - this enhances numerical robustness in
*           the case of differently scaled rows.
            DO 1904 p = 1, M
*               RWORK(p) = ABS( A(p,ICAMAX(N,A(p,1),LDA)) )
*               [[CLANGE will return NaN if an entry of the p-th row is Nan]]
                RWORK(p) = CLANGE( 'M', 1, N, A(p,1), LDA, RDUMMY )
*               .. check for NaN's and Inf's
                IF ( ( RWORK(p) .NE. RWORK(p) ) .OR.
     $               ( (RWORK(p)*ZERO) .NE. ZERO ) ) THEN
                    INFO = - 8
                    CALL XERBLA( 'CGESVDQ', -INFO )
                    RETURN
                END IF
 1904       CONTINUE
            DO 1952 p = 1, M - 1
            q = ISAMAX( M-p+1, RWORK(p), 1 ) + p - 1
            IWORK(N+p) = q
            IF ( p .NE. q ) THEN
               RTMP     = RWORK(p)
               RWORK(p) = RWORK(q)
               RWORK(q) = RTMP
            END IF
 1952       CONTINUE
*
            IF ( RWORK(1) .EQ. ZERO ) THEN
*              Quick return: A is the M x N zero matrix.
               NUMRANK = 0
               CALL SLASET( 'G', N, 1, ZERO, ZERO, S, N )
               IF ( WNTUS ) CALL CLASET('G', M, N, CZERO, CONE, U, LDU)
               IF ( WNTUA ) CALL CLASET('G', M, M, CZERO, CONE, U, LDU)
               IF ( WNTVA ) CALL CLASET('G', N, N, CZERO, CONE, V, LDV)
               IF ( WNTUF ) THEN
                   CALL CLASET( 'G', N, 1, CZERO, CZERO, CWORK, N )
                   CALL CLASET( 'G', M, N, CZERO, CONE, U, LDU )
               END IF
               DO 5001 p = 1, N
                   IWORK(p) = p
 5001          CONTINUE
               IF ( ROWPRM ) THEN
                   DO 5002 p = N + 1, N + M - 1
                       IWORK(p) = p - N
 5002              CONTINUE
               END IF
               IF ( CONDA ) RWORK(1) = -1
               RWORK(2) = -1
               RETURN
            END IF
*
            IF ( RWORK(1) .GT. BIG / SQRT(REAL(M)) ) THEN
*               .. to prevent overflow in the QR factorization, scale the
*               matrix by 1/sqrt(M) if too large entry detected
                CALL CLASCL('G',0,0,SQRT(REAL(M)),ONE, M,N, A,LDA, IERR)
                ASCALED = .TRUE.
            END IF
            CALL CLASWP( N, A, LDA, 1, M-1, IWORK(N+1), 1 )
      END IF
*
*    .. At this stage, preemptive scaling is done only to avoid column
*    norms overflows during the QR factorization. The SVD procedure should
*    have its own scaling to save the singular values from overflows and
*    underflows. That depends on the SVD procedure.
*
      IF ( .NOT.ROWPRM ) THEN
          RTMP = CLANGE( 'M', M, N, A, LDA, RWORK )
          IF ( ( RTMP .NE. RTMP ) .OR.
     $         ( (RTMP*ZERO) .NE. ZERO ) ) THEN
               INFO = - 8
               CALL XERBLA( 'CGESVDQ', -INFO )
               RETURN
          END IF
          IF ( RTMP .GT. BIG / SQRT(REAL(M)) ) THEN
*             .. to prevent overflow in the QR factorization, scale the
*             matrix by 1/sqrt(M) if too large entry detected
              CALL CLASCL('G',0,0, SQRT(REAL(M)),ONE, M,N, A,LDA, IERR)
              ASCALED = .TRUE.
          END IF
      END IF
*
*     .. QR factorization with column pivoting
*
*     A * P = Q * [ R ]
*                 [ 0 ]
*
      DO 1963 p = 1, N
*        .. all columns are free columns
         IWORK(p) = 0
 1963 CONTINUE
      CALL CGEQP3( M, N, A, LDA, IWORK, CWORK, CWORK(N+1), LCWORK-N,
     $     RWORK, IERR )
*
*    If the user requested accuracy level allows truncation in the
*    computed upper triangular factor, the matrix R is examined and,
*    if possible, replaced with its leading upper trapezoidal part.
*
      EPSLN = SLAMCH('E')
      SFMIN = SLAMCH('S')
*     SMALL = SFMIN / EPSLN
      NR = N
*
      IF ( ACCLA ) THEN
*
*        Standard absolute error bound suffices. All sigma_i with
*        sigma_i < N*EPS*||A||_F are flushed to zero. This is an
*        aggressive enforcement of lower numerical rank by introducing a
*        backward error of the order of N*EPS*||A||_F.
         NR = 1
         RTMP = SQRT(REAL(N))*EPSLN
         DO 3001 p = 2, N
            IF ( ABS(A(p,p)) .LT. (RTMP*ABS(A(1,1))) ) GO TO 3002
               NR = NR + 1
 3001    CONTINUE
 3002    CONTINUE
*
      ELSEIF ( ACCLM ) THEN
*        .. similarly as above, only slightly more gentle (less aggressive).
*        Sudden drop on the diagonal of R is used as the criterion for being
*        close-to-rank-deficient. The threshold is set to EPSLN=SLAMCH('E').
*        [[This can be made more flexible by replacing this hard-coded value
*        with a user specified threshold.]] Also, the values that underflow
*        will be truncated.
         NR = 1
         DO 3401 p = 2, N
            IF ( ( ABS(A(p,p)) .LT. (EPSLN*ABS(A(p-1,p-1))) ) .OR.
     $           ( ABS(A(p,p)) .LT. SFMIN ) ) GO TO 3402
            NR = NR + 1
 3401    CONTINUE
 3402    CONTINUE
*
      ELSE
*        .. RRQR not authorized to determine numerical rank except in the
*        obvious case of zero pivots.
*        .. inspect R for exact zeros on the diagonal;
*        R(i,i)=0 => R(i:N,i:N)=0.
         NR = 1
         DO 3501 p = 2, N
            IF ( ABS(A(p,p)) .EQ. ZERO ) GO TO 3502
            NR = NR + 1
 3501    CONTINUE
 3502    CONTINUE
*
         IF ( CONDA ) THEN
*           Estimate the scaled condition number of A. Use the fact that it is
*           the same as the scaled condition number of R.
*              .. V is used as workspace
               CALL CLACPY( 'U', N, N, A, LDA, V, LDV )
*              Only the leading NR x NR submatrix of the triangular factor
*              is considered. Only if NR=N will this give a reliable error
*              bound. However, even for NR < N, this can be used on an
*              expert level and obtain useful information in the sense of
*              perturbation theory.
               DO 3053 p = 1, NR
                  RTMP = SCNRM2( p, V(1,p), 1 )
                  CALL CSSCAL( p, ONE/RTMP, V(1,p), 1 )
 3053          CONTINUE
               IF ( .NOT. ( LSVEC .OR. RSVEC ) ) THEN
                   CALL CPOCON( 'U', NR, V, LDV, ONE, RTMP,
     $                  CWORK, RWORK, IERR )
               ELSE
                   CALL CPOCON( 'U', NR, V, LDV, ONE, RTMP,
     $                  CWORK(N+1), RWORK, IERR )
               END IF
               SCONDA = ONE / SQRT(RTMP)
*           For NR=N, SCONDA is an estimate of SQRT(||(R^* * R)^(-1)||_1),
*           N^(-1/4) * SCONDA <= ||R^(-1)||_2 <= N^(1/4) * SCONDA
*           See the reference [1] for more details.
         END IF
*
      ENDIF
*
      IF ( WNTUR ) THEN
          N1 = NR
      ELSE IF ( WNTUS .OR. WNTUF) THEN
          N1 = N
      ELSE IF ( WNTUA ) THEN
          N1 = M
      END IF
*
      IF ( .NOT. ( RSVEC .OR. LSVEC ) ) THEN
*.......................................................................
*        .. only the singular values are requested
*.......................................................................
         IF ( RTRANS ) THEN
*
*         .. compute the singular values of R**H = [A](1:NR,1:N)**H
*           .. set the lower triangle of [A] to [A](1:NR,1:N)**H and
*           the upper triangle of [A] to zero.
            DO 1146 p = 1, MIN( N, NR )
               A(p,p) = CONJG(A(p,p))
               DO 1147 q = p + 1, N
                  A(q,p) = CONJG(A(p,q))
                  IF ( q .LE. NR ) A(p,q) = CZERO
 1147          CONTINUE
 1146       CONTINUE
*
            CALL CGESVD( 'N', 'N', N, NR, A, LDA, S, U, LDU,
     $           V, LDV, CWORK, LCWORK, RWORK, INFO )
*
         ELSE
*
*           .. compute the singular values of R = [A](1:NR,1:N)
*
            IF ( NR .GT. 1 )
     $          CALL CLASET( 'L', NR-1,NR-1, CZERO,CZERO, A(2,1), LDA )
            CALL CGESVD( 'N', 'N', NR, N, A, LDA, S, U, LDU,
     $           V, LDV, CWORK, LCWORK, RWORK, INFO )
*
         END IF
*
      ELSE IF ( LSVEC .AND. ( .NOT. RSVEC) ) THEN
*.......................................................................
*       .. the singular values and the left singular vectors requested
*.......................................................................""""""""
         IF ( RTRANS ) THEN
*            .. apply CGESVD to R**H
*            .. copy R**H into [U] and overwrite [U] with the right singular
*            vectors of R
            DO 1192 p = 1, NR
               DO 1193 q = p, N
                  U(q,p) = CONJG(A(p,q))
 1193          CONTINUE
 1192       CONTINUE
            IF ( NR .GT. 1 )
     $          CALL CLASET( 'U', NR-1,NR-1, CZERO,CZERO, U(1,2), LDU )
*           .. the left singular vectors not computed, the NR right singular
*           vectors overwrite [U](1:NR,1:NR) as conjugate transposed. These
*           will be pre-multiplied by Q to build the left singular vectors of A.
               CALL CGESVD( 'N', 'O', N, NR, U, LDU, S, U, LDU,
     $              U, LDU, CWORK(N+1), LCWORK-N, RWORK, INFO )
*
               DO 1119 p = 1, NR
                   U(p,p) = CONJG(U(p,p))
                   DO 1120 q = p + 1, NR
                      CTMP   = CONJG(U(q,p))
                      U(q,p) = CONJG(U(p,q))
                      U(p,q) = CTMP
 1120              CONTINUE
 1119          CONTINUE
*
         ELSE
*            .. apply CGESVD to R
*            .. copy R into [U] and overwrite [U] with the left singular vectors
             CALL CLACPY( 'U', NR, N, A, LDA, U, LDU )
             IF ( NR .GT. 1 )
     $         CALL CLASET( 'L', NR-1, NR-1, CZERO, CZERO, U(2,1), LDU )
*            .. the right singular vectors not computed, the NR left singular
*            vectors overwrite [U](1:NR,1:NR)
                CALL CGESVD( 'O', 'N', NR, N, U, LDU, S, U, LDU,
     $               V, LDV, CWORK(N+1), LCWORK-N, RWORK, INFO )
*               .. now [U](1:NR,1:NR) contains the NR left singular vectors of
*               R. These will be pre-multiplied by Q to build the left singular
*               vectors of A.
         END IF
*
*           .. assemble the left singular vector matrix U of dimensions
*              (M x NR) or (M x N) or (M x M).
         IF ( ( NR .LT. M ) .AND. ( .NOT.WNTUF ) ) THEN
             CALL CLASET('A', M-NR, NR, CZERO, CZERO, U(NR+1,1), LDU)
             IF ( NR .LT. N1 ) THEN
                CALL CLASET( 'A',NR,N1-NR,CZERO,CZERO,U(1,NR+1), LDU )
                CALL CLASET( 'A',M-NR,N1-NR,CZERO,CONE,
     $               U(NR+1,NR+1), LDU )
             END IF
         END IF
*
*           The Q matrix from the first QRF is built into the left singular
*           vectors matrix U.
*
         IF ( .NOT.WNTUF )
     $       CALL CUNMQR( 'L', 'N', M, N1, N, A, LDA, CWORK, U,
     $            LDU, CWORK(N+1), LCWORK-N, IERR )
         IF ( ROWPRM .AND. .NOT.WNTUF )
     $          CALL CLASWP( N1, U, LDU, 1, M-1, IWORK(N+1), -1 )
*
      ELSE IF ( RSVEC .AND. ( .NOT. LSVEC ) ) THEN
*.......................................................................
*       .. the singular values and the right singular vectors requested
*.......................................................................
          IF ( RTRANS ) THEN
*            .. apply CGESVD to R**H
*            .. copy R**H into V and overwrite V with the left singular vectors
            DO 1165 p = 1, NR
               DO 1166 q = p, N
                  V(q,p) = CONJG(A(p,q))
 1166          CONTINUE
 1165       CONTINUE
            IF ( NR .GT. 1 )
     $          CALL CLASET( 'U', NR-1,NR-1, CZERO,CZERO, V(1,2), LDV )
*           .. the left singular vectors of R**H overwrite V, the right singular
*           vectors not computed
            IF ( WNTVR .OR. ( NR .EQ. N ) ) THEN
               CALL CGESVD( 'O', 'N', N, NR, V, LDV, S, U, LDU,
     $              U, LDU, CWORK(N+1), LCWORK-N, RWORK, INFO )
*
               DO 1121 p = 1, NR
                   V(p,p) = CONJG(V(p,p))
                   DO 1122 q = p + 1, NR
                      CTMP   = CONJG(V(q,p))
                      V(q,p) = CONJG(V(p,q))
                      V(p,q) = CTMP
 1122              CONTINUE
 1121          CONTINUE
*
               IF ( NR .LT. N ) THEN
                   DO 1103 p = 1, NR
                      DO 1104 q = NR + 1, N
                          V(p,q) = CONJG(V(q,p))
 1104                 CONTINUE
 1103              CONTINUE
               END IF
               CALL CLAPMT( .FALSE., NR, N, V, LDV, IWORK )
            ELSE
*               .. need all N right singular vectors and NR < N
*               [!] This is simple implementation that augments [V](1:N,1:NR)
*               by padding a zero block. In the case NR << N, a more efficient
*               way is to first use the QR factorization. For more details
*               how to implement this, see the " FULL SVD " branch.
                CALL CLASET('G', N, N-NR, CZERO, CZERO, V(1,NR+1), LDV)
                CALL CGESVD( 'O', 'N', N, N, V, LDV, S, U, LDU,
     $               U, LDU, CWORK(N+1), LCWORK-N, RWORK, INFO )
*
                DO 1123 p = 1, N
                   V(p,p) = CONJG(V(p,p))
                   DO 1124 q = p + 1, N
                      CTMP   = CONJG(V(q,p))
                      V(q,p) = CONJG(V(p,q))
                      V(p,q) = CTMP
 1124              CONTINUE
 1123           CONTINUE
                CALL CLAPMT( .FALSE., N, N, V, LDV, IWORK )
            END IF
*
          ELSE
*            .. aply CGESVD to R
*            .. copy R into V and overwrite V with the right singular vectors
             CALL CLACPY( 'U', NR, N, A, LDA, V, LDV )
             IF ( NR .GT. 1 )
     $         CALL CLASET( 'L', NR-1, NR-1, CZERO, CZERO, V(2,1), LDV )
*            .. the right singular vectors overwrite V, the NR left singular
*            vectors stored in U(1:NR,1:NR)
             IF ( WNTVR .OR. ( NR .EQ. N ) ) THEN
                CALL CGESVD( 'N', 'O', NR, N, V, LDV, S, U, LDU,
     $               V, LDV, CWORK(N+1), LCWORK-N, RWORK, INFO )
                CALL CLAPMT( .FALSE., NR, N, V, LDV, IWORK )
*               .. now [V](1:NR,1:N) contains V(1:N,1:NR)**H
             ELSE
*               .. need all N right singular vectors and NR < N
*               [!] This is simple implementation that augments [V](1:NR,1:N)
*               by padding a zero block. In the case NR << N, a more efficient
*               way is to first use the LQ factorization. For more details
*               how to implement this, see the " FULL SVD " branch.
                 CALL CLASET('G', N-NR, N, CZERO,CZERO, V(NR+1,1), LDV)
                 CALL CGESVD( 'N', 'O', N, N, V, LDV, S, U, LDU,
     $                V, LDV, CWORK(N+1), LCWORK-N, RWORK, INFO )
                 CALL CLAPMT( .FALSE., N, N, V, LDV, IWORK )
             END IF
*            .. now [V] contains the adjoint of the matrix of the right singular
*            vectors of A.
          END IF
*
      ELSE
*.......................................................................
*       .. FULL SVD requested
*.......................................................................
         IF ( RTRANS ) THEN
*
*            .. apply CGESVD to R**H [[this option is left for R&D&T]]
*
            IF ( WNTVR .OR. ( NR .EQ. N ) ) THEN
*            .. copy R**H into [V] and overwrite [V] with the left singular
*            vectors of R**H
            DO 1168 p = 1, NR
               DO 1169 q = p, N
                  V(q,p) = CONJG(A(p,q))
 1169          CONTINUE
 1168       CONTINUE
            IF ( NR .GT. 1 )
     $          CALL CLASET( 'U', NR-1,NR-1, CZERO,CZERO, V(1,2), LDV )
*
*           .. the left singular vectors of R**H overwrite [V], the NR right
*           singular vectors of R**H stored in [U](1:NR,1:NR) as conjugate
*           transposed
               CALL CGESVD( 'O', 'A', N, NR, V, LDV, S, V, LDV,
     $              U, LDU, CWORK(N+1), LCWORK-N, RWORK, INFO )
*              .. assemble V
               DO 1115 p = 1, NR
                  V(p,p) = CONJG(V(p,p))
                  DO 1116 q = p + 1, NR
                     CTMP   = CONJG(V(q,p))
                     V(q,p) = CONJG(V(p,q))
                     V(p,q) = CTMP
 1116             CONTINUE
 1115          CONTINUE
               IF ( NR .LT. N ) THEN
                   DO 1101 p = 1, NR
                      DO 1102 q = NR+1, N
                         V(p,q) = CONJG(V(q,p))
 1102                 CONTINUE
 1101              CONTINUE
               END IF
               CALL CLAPMT( .FALSE., NR, N, V, LDV, IWORK )
*
                DO 1117 p = 1, NR
                   U(p,p) = CONJG(U(p,p))
                   DO 1118 q = p + 1, NR
                      CTMP   = CONJG(U(q,p))
                      U(q,p) = CONJG(U(p,q))
                      U(p,q) = CTMP
 1118              CONTINUE
 1117           CONTINUE
*
                IF ( ( NR .LT. M ) .AND. .NOT.(WNTUF)) THEN
                  CALL CLASET('A', M-NR,NR, CZERO,CZERO, U(NR+1,1), LDU)
                  IF ( NR .LT. N1 ) THEN
                     CALL CLASET('A',NR,N1-NR,CZERO,CZERO,U(1,NR+1),LDU)
                     CALL CLASET( 'A',M-NR,N1-NR,CZERO,CONE,
     $                    U(NR+1,NR+1), LDU )
                  END IF
               END IF
*
            ELSE
*               .. need all N right singular vectors and NR < N
*            .. copy R**H into [V] and overwrite [V] with the left singular
*            vectors of R**H
*               [[The optimal ratio N/NR for using QRF instead of padding
*                 with zeros. Here hard coded to 2; it must be at least
*                 two due to work space constraints.]]
*               OPTRATIO = ILAENV(6, 'CGESVD', 'S' // 'O', NR,N,0,0)
*               OPTRATIO = MAX( OPTRATIO, 2 )
                OPTRATIO = 2
                IF ( OPTRATIO*NR .GT. N ) THEN
                   DO 1198 p = 1, NR
                      DO 1199 q = p, N
                         V(q,p) = CONJG(A(p,q))
 1199                 CONTINUE
 1198              CONTINUE
                   IF ( NR .GT. 1 )
     $             CALL CLASET('U',NR-1,NR-1, CZERO,CZERO, V(1,2),LDV)
*
                   CALL CLASET('A',N,N-NR,CZERO,CZERO,V(1,NR+1),LDV)
                   CALL CGESVD( 'O', 'A', N, N, V, LDV, S, V, LDV,
     $                  U, LDU, CWORK(N+1), LCWORK-N, RWORK, INFO )
*
                   DO 1113 p = 1, N
                      V(p,p) = CONJG(V(p,p))
                      DO 1114 q = p + 1, N
                         CTMP   = CONJG(V(q,p))
                         V(q,p) = CONJG(V(p,q))
                         V(p,q) = CTMP
 1114                 CONTINUE
 1113              CONTINUE
                   CALL CLAPMT( .FALSE., N, N, V, LDV, IWORK )
*              .. assemble the left singular vector matrix U of dimensions
*              (M x N1), i.e. (M x N) or (M x M).
*
                   DO 1111 p = 1, N
                      U(p,p) = CONJG(U(p,p))
                      DO 1112 q = p + 1, N
                         CTMP   = CONJG(U(q,p))
                         U(q,p) = CONJG(U(p,q))
                         U(p,q) = CTMP
 1112                 CONTINUE
 1111              CONTINUE
*
                   IF ( ( N .LT. M ) .AND. .NOT.(WNTUF)) THEN
                      CALL CLASET('A',M-N,N,CZERO,CZERO,U(N+1,1),LDU)
                      IF ( N .LT. N1 ) THEN
                        CALL CLASET('A',N,N1-N,CZERO,CZERO,U(1,N+1),LDU)
                        CALL CLASET('A',M-N,N1-N,CZERO,CONE,
     $                       U(N+1,N+1), LDU )
                      END IF
                   END IF
                ELSE
*                  .. copy R**H into [U] and overwrite [U] with the right
*                  singular vectors of R
                   DO 1196 p = 1, NR
                      DO 1197 q = p, N
                         U(q,NR+p) = CONJG(A(p,q))
 1197                 CONTINUE
 1196              CONTINUE
                   IF ( NR .GT. 1 )
     $             CALL CLASET('U',NR-1,NR-1,CZERO,CZERO,U(1,NR+2),LDU)
                   CALL CGEQRF( N, NR, U(1,NR+1), LDU, CWORK(N+1),
     $                  CWORK(N+NR+1), LCWORK-N-NR, IERR )
                   DO 1143 p = 1, NR
                       DO 1144 q = 1, N
                           V(q,p) = CONJG(U(p,NR+q))
 1144                  CONTINUE
 1143              CONTINUE
                  CALL CLASET('U',NR-1,NR-1,CZERO,CZERO,V(1,2),LDV)
                  CALL CGESVD( 'S', 'O', NR, NR, V, LDV, S, U, LDU,
     $                 V,LDV, CWORK(N+NR+1),LCWORK-N-NR,RWORK, INFO )
                  CALL CLASET('A',N-NR,NR,CZERO,CZERO,V(NR+1,1),LDV)
                  CALL CLASET('A',NR,N-NR,CZERO,CZERO,V(1,NR+1),LDV)
                  CALL CLASET('A',N-NR,N-NR,CZERO,CONE,V(NR+1,NR+1),LDV)
                  CALL CUNMQR('R','C', N, N, NR, U(1,NR+1), LDU,
     $                 CWORK(N+1),V,LDV,CWORK(N+NR+1),LCWORK-N-NR,IERR)
                  CALL CLAPMT( .FALSE., N, N, V, LDV, IWORK )
*                 .. assemble the left singular vector matrix U of dimensions
*                 (M x NR) or (M x N) or (M x M).
                  IF ( ( NR .LT. M ) .AND. .NOT.(WNTUF)) THEN
                     CALL CLASET('A',M-NR,NR,CZERO,CZERO,U(NR+1,1),LDU)
                     IF ( NR .LT. N1 ) THEN
                     CALL CLASET('A',NR,N1-NR,CZERO,CZERO,U(1,NR+1),LDU)
                     CALL CLASET( 'A',M-NR,N1-NR,CZERO,CONE,
     $                    U(NR+1,NR+1),LDU)
                     END IF
                  END IF
                END IF
            END IF
*
         ELSE
*
*            .. apply CGESVD to R [[this is the recommended option]]
*
             IF ( WNTVR .OR. ( NR .EQ. N ) ) THEN
*                .. copy R into [V] and overwrite V with the right singular vectors
                 CALL CLACPY( 'U', NR, N, A, LDA, V, LDV )
                IF ( NR .GT. 1 )
     $          CALL CLASET( 'L', NR-1,NR-1, CZERO,CZERO, V(2,1), LDV )
*               .. the right singular vectors of R overwrite [V], the NR left
*               singular vectors of R stored in [U](1:NR,1:NR)
                CALL CGESVD( 'S', 'O', NR, N, V, LDV, S, U, LDU,
     $               V, LDV, CWORK(N+1), LCWORK-N, RWORK, INFO )
                CALL CLAPMT( .FALSE., NR, N, V, LDV, IWORK )
*               .. now [V](1:NR,1:N) contains V(1:N,1:NR)**H
*               .. assemble the left singular vector matrix U of dimensions
*              (M x NR) or (M x N) or (M x M).
               IF ( ( NR .LT. M ) .AND. .NOT.(WNTUF)) THEN
                  CALL CLASET('A', M-NR,NR, CZERO,CZERO, U(NR+1,1), LDU)
                  IF ( NR .LT. N1 ) THEN
                     CALL CLASET('A',NR,N1-NR,CZERO,CZERO,U(1,NR+1),LDU)
                     CALL CLASET( 'A',M-NR,N1-NR,CZERO,CONE,
     $                    U(NR+1,NR+1), LDU )
                  END IF
               END IF
*
             ELSE
*              .. need all N right singular vectors and NR < N
*              .. the requested number of the left singular vectors
*               is then N1 (N or M)
*               [[The optimal ratio N/NR for using LQ instead of padding
*                 with zeros. Here hard coded to 2; it must be at least
*                 two due to work space constraints.]]
*               OPTRATIO = ILAENV(6, 'CGESVD', 'S' // 'O', NR,N,0,0)
*               OPTRATIO = MAX( OPTRATIO, 2 )
               OPTRATIO = 2
               IF ( OPTRATIO * NR .GT. N ) THEN
                  CALL CLACPY( 'U', NR, N, A, LDA, V, LDV )
                  IF ( NR .GT. 1 )
     $            CALL CLASET('L', NR-1,NR-1, CZERO,CZERO, V(2,1),LDV)
*              .. the right singular vectors of R overwrite [V], the NR left
*                 singular vectors of R stored in [U](1:NR,1:NR)
                  CALL CLASET('A', N-NR,N, CZERO,CZERO, V(NR+1,1),LDV)
                  CALL CGESVD( 'S', 'O', N, N, V, LDV, S, U, LDU,
     $                 V, LDV, CWORK(N+1), LCWORK-N, RWORK, INFO )
                  CALL CLAPMT( .FALSE., N, N, V, LDV, IWORK )
*                 .. now [V] contains the adjoint of the matrix of the right
*                 singular vectors of A. The leading N left singular vectors
*                 are in [U](1:N,1:N)
*                 .. assemble the left singular vector matrix U of dimensions
*                 (M x N1), i.e. (M x N) or (M x M).
                  IF ( ( N .LT. M ) .AND. .NOT.(WNTUF)) THEN
                      CALL CLASET('A',M-N,N,CZERO,CZERO,U(N+1,1),LDU)
                      IF ( N .LT. N1 ) THEN
                        CALL CLASET('A',N,N1-N,CZERO,CZERO,U(1,N+1),LDU)
                        CALL CLASET( 'A',M-N,N1-N,CZERO,CONE,
     $                       U(N+1,N+1), LDU )
                      END IF
                  END IF
               ELSE
                  CALL CLACPY( 'U', NR, N, A, LDA, U(NR+1,1), LDU )
                  IF ( NR .GT. 1 )
     $            CALL CLASET('L',NR-1,NR-1,CZERO,CZERO,U(NR+2,1),LDU)
                  CALL CGELQF( NR, N, U(NR+1,1), LDU, CWORK(N+1),
     $                 CWORK(N+NR+1), LCWORK-N-NR, IERR )
                  CALL CLACPY('L',NR,NR,U(NR+1,1),LDU,V,LDV)
                  IF ( NR .GT. 1 )
     $            CALL CLASET('U',NR-1,NR-1,CZERO,CZERO,V(1,2),LDV)
                  CALL CGESVD( 'S', 'O', NR, NR, V, LDV, S, U, LDU,
     $                 V, LDV, CWORK(N+NR+1), LCWORK-N-NR, RWORK, INFO )
                  CALL CLASET('A',N-NR,NR,CZERO,CZERO,V(NR+1,1),LDV)
                  CALL CLASET('A',NR,N-NR,CZERO,CZERO,V(1,NR+1),LDV)
                  CALL CLASET('A',N-NR,N-NR,CZERO,CONE,V(NR+1,NR+1),LDV)
                  CALL CUNMLQ('R','N',N,N,NR,U(NR+1,1),LDU,CWORK(N+1),
     $                 V, LDV, CWORK(N+NR+1),LCWORK-N-NR,IERR)
                  CALL CLAPMT( .FALSE., N, N, V, LDV, IWORK )
*               .. assemble the left singular vector matrix U of dimensions
*              (M x NR) or (M x N) or (M x M).
                  IF ( ( NR .LT. M ) .AND. .NOT.(WNTUF)) THEN
                     CALL CLASET('A',M-NR,NR,CZERO,CZERO,U(NR+1,1),LDU)
                     IF ( NR .LT. N1 ) THEN
                     CALL CLASET('A',NR,N1-NR,CZERO,CZERO,U(1,NR+1),LDU)
                     CALL CLASET( 'A',M-NR,N1-NR,CZERO,CONE,
     $                    U(NR+1,NR+1), LDU )
                     END IF
                  END IF
               END IF
             END IF
*        .. end of the "R**H or R" branch
         END IF
*
*           The Q matrix from the first QRF is built into the left singular
*           vectors matrix U.
*
         IF ( .NOT. WNTUF )
     $       CALL CUNMQR( 'L', 'N', M, N1, N, A, LDA, CWORK, U,
     $            LDU, CWORK(N+1), LCWORK-N, IERR )
         IF ( ROWPRM .AND. .NOT.WNTUF )
     $          CALL CLASWP( N1, U, LDU, 1, M-1, IWORK(N+1), -1 )
*
*     ... end of the "full SVD" branch
      END IF
*
*     Check whether some singular values are returned as zeros, e.g.
*     due to underflow, and update the numerical rank.
      p = NR
      DO 4001 q = p, 1, -1
          IF ( S(q) .GT. ZERO ) GO TO 4002
          NR = NR - 1
 4001 CONTINUE
 4002 CONTINUE
*
*     .. if numerical rank deficiency is detected, the truncated
*     singular values are set to zero.
      IF ( NR .LT. N ) CALL SLASET( 'G', N-NR,1, ZERO,ZERO, S(NR+1), N )
*     .. undo scaling; this may cause overflow in the largest singular
*     values.
      IF ( ASCALED )
     $   CALL SLASCL( 'G',0,0, ONE,SQRT(REAL(M)), NR,1, S, N, IERR )
      IF ( CONDA ) RWORK(1) = SCONDA
      RWORK(2) = p - NR
*     .. p-NR is the number of singular values that are computed as
*     exact zeros in CGESVD() applied to the (possibly truncated)
*     full row rank triangular (trapezoidal) factor of A.
      NUMRANK = NR
*
      RETURN
*
*     End of CGESVDQ
*
      END
