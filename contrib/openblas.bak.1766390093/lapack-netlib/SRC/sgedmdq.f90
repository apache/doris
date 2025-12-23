SUBROUTINE SGEDMDQ( JOBS,  JOBZ, JOBR, JOBQ, JOBT, JOBF,   &
                    WHTSVD,   M, N, F, LDF,  X, LDX,  Y,   &
                    LDY,   NRNK,  TOL,   K,  REIG, IMEIG,  &
                    Z, LDZ, RES,  B,     LDB,   V, LDV,    & 
                    S, LDS, WORK, LWORK, IWORK, LIWORK, INFO )
! March 2023
!.....
      USE                   iso_fortran_env 
      IMPLICIT NONE
      INTEGER, PARAMETER :: WP = real32     
!.....      
!     Scalar arguments       
      CHARACTER, INTENT(IN)  :: JOBS, JOBZ, JOBR, JOBQ,    &
                                JOBT, JOBF
      INTEGER,   INTENT(IN)  :: WHTSVD, M, N,   LDF, LDX,  &
                                LDY, NRNK, LDZ, LDB, LDV,  &
                                LDS, LWORK,  LIWORK
      INTEGER,   INTENT(OUT) :: INFO,   K      
      REAL(KIND=WP), INTENT(IN)    ::   TOL     
!     Array arguments      
      REAL(KIND=WP), INTENT(INOUT) :: F(LDF,*)
      REAL(KIND=WP), INTENT(OUT)   :: X(LDX,*), Y(LDY,*),  &
                                      Z(LDZ,*), B(LDB,*),  &
                                      V(LDV,*), S(LDS,*)
      REAL(KIND=WP), INTENT(OUT)   :: REIG(*),  IMEIG(*),  &
                                      RES(*)
      REAL(KIND=WP), INTENT(OUT)   :: WORK(*)  
      INTEGER,       INTENT(OUT)   :: IWORK(*)
!.....      
!     Purpose  
!     =======
!     SGEDMDQ computes the Dynamic Mode Decomposition (DMD) for
!     a pair of data snapshot matrices, using a QR factorization
!     based compression of the data. For the input matrices
!     X and Y such that Y = A*X with an unaccessible matrix
!     A, SGEDMDQ computes a certain number of Ritz pairs of A using
!     the standard Rayleigh-Ritz extraction from a subspace of
!     range(X) that is determined using the leading left singular 
!     vectors of X. Optionally, SGEDMDQ returns the residuals 
!     of the computed Ritz pairs, the information needed for
!     a refinement of the Ritz vectors, or the eigenvectors of
!     the Exact DMD.
!     For further details see the references listed
!     below. For more details of the implementation see [3].      
!
!     References
!     ==========
!     [1] P. Schmid: Dynamic mode decomposition of numerical
!         and experimental data,
!         Journal of Fluid Mechanics 656, 5-28, 2010.
!     [2] Z. Drmac, I. Mezic, R. Mohr: Data driven modal
!         decompositions: analysis and enhancements,
!         SIAM J. on Sci. Comp. 40 (4), A2253-A2285, 2018.
!     [3] Z. Drmac: A LAPACK implementation of the Dynamic
!         Mode Decomposition I. Technical report. AIMDyn Inc.
!         and LAPACK Working Note 298.      
!     [4] J. Tu, C. W. Rowley, D. M. Luchtenburg, S. L. 
!         Brunton, N. Kutz: On Dynamic Mode Decomposition:
!         Theory and Applications, Journal of Computational
!         Dynamics 1(2), 391 -421, 2014.
!
!     Developed and supported by:
!     ===========================
!     Developed and coded by Zlatko Drmac, Faculty of Science,
!     University of Zagreb;  drmac@math.hr
!     In cooperation with
!     AIMdyn Inc., Santa Barbara, CA.
!     and supported by
!     - DARPA SBIR project "Koopman Operator-Based Forecasting
!     for Nonstationary Processes from Near-Term, Limited
!     Observational Data" Contract No: W31P4Q-21-C-0007
!     - DARPA PAI project "Physics-Informed Machine Learning
!     Methodologies" Contract No: HR0011-18-9-0033
!     - DARPA MoDyL project "A Data-Driven, Operator-Theoretic
!     Framework for Space-Time Analysis of Process Dynamics"
!     Contract No: HR0011-16-C-0116
!     Any opinions, findings and conclusions or recommendations 
!     expressed in this material are those of the author and 
!     do not necessarily reflect the views of the DARPA SBIR 
!     Program Office.      
!============================================================
!     Distribution Statement A: 
!     Approved for Public Release, Distribution Unlimited.
!     Cleared by DARPA on September 29, 2022
!============================================================      
!......................................................................      
!     Arguments
!     =========
!     JOBS (input) CHARACTER*1
!     Determines whether the initial data snapshots are scaled
!     by a diagonal matrix. The data snapshots are the columns
!     of F. The leading N-1 columns of F are denoted X and the
!     trailing N-1 columns are denoted Y. 
!     'S' :: The data snapshots matrices X and Y are multiplied
!            with a diagonal matrix D so that X*D has unit
!            nonzero columns (in the Euclidean 2-norm)
!     'C' :: The snapshots are scaled as with the 'S' option.
!            If it is found that an i-th column of X is zero
!            vector and the corresponding i-th column of Y is
!            non-zero, then the i-th column of Y is set to
!            zero and a warning flag is raised.
!     'Y' :: The data snapshots matrices X and Y are multiplied
!            by a diagonal matrix D so that Y*D has unit
!            nonzero columns (in the Euclidean 2-norm)    
!     'N' :: No data scaling.   
!.....
!     JOBZ (input) CHARACTER*1
!     Determines whether the eigenvectors (Koopman modes) will
!     be computed.
!     'V' :: The eigenvectors (Koopman modes) will be computed
!            and returned in the matrix Z.
!            See the description of Z.
!     'F' :: The eigenvectors (Koopman modes) will be returned
!            in factored form as the product Z*V, where Z
!            is orthonormal and V contains the eigenvectors
!            of the corresponding Rayleigh quotient.
!            See the descriptions of F, V, Z.
!     'Q' :: The eigenvectors (Koopman modes) will be returned
!            in factored form as the product Q*Z, where Z
!            contains the eigenvectors of the compression of the
!            underlying discretized operator onto the span of
!            the data snapshots. See the descriptions of F, V, Z. 
!            Q is from the initial QR factorization.  
!     'N' :: The eigenvectors are not computed.  
!.....      
!     JOBR (input) CHARACTER*1 
!     Determines whether to compute the residuals.
!     'R' :: The residuals for the computed eigenpairs will
!            be computed and stored in the array RES.
!            See the description of RES.
!            For this option to be legal, JOBZ must be 'V'.
!     'N' :: The residuals are not computed.
!.....
!     JOBQ (input) CHARACTER*1 
!     Specifies whether to explicitly compute and return the
!     orthogonal matrix from the QR factorization.
!     'Q' :: The matrix Q of the QR factorization of the data
!            snapshot matrix is computed and stored in the
!            array F. See the description of F.       
!     'N' :: The matrix Q is not explicitly computed.
!.....
!     JOBT (input) CHARACTER*1 
!     Specifies whether to return the upper triangular factor
!     from the QR factorization.
!     'R' :: The matrix R of the QR factorization of the data 
!            snapshot matrix F is returned in the array Y.
!            See the description of Y and Further details.       
!     'N' :: The matrix R is not returned.    
!.....
!     JOBF (input) CHARACTER*1
!     Specifies whether to store information needed for post-
!     processing (e.g. computing refined Ritz vectors)
!     'R' :: The matrix needed for the refinement of the Ritz
!            vectors is computed and stored in the array B.
!            See the description of B.
!     'E' :: The unscaled eigenvectors of the Exact DMD are 
!            computed and returned in the array B. See the
!            description of B.
!     'N' :: No eigenvector refinement data is computed.   
!     To be useful on exit, this option needs JOBQ='Q'.      
!.....
!     WHTSVD (input) INTEGER, WHSTVD in { 1, 2, 3, 4 }
!     Allows for a selection of the SVD algorithm from the
!     LAPACK library.
!     1 :: SGESVD (the QR SVD algorithm)
!     2 :: SGESDD (the Divide and Conquer algorithm; if enough
!          workspace available, this is the fastest option)
!     3 :: SGESVDQ (the preconditioned QR SVD  ; this and 4
!          are the most accurate options)
!     4 :: SGEJSV (the preconditioned Jacobi SVD; this and 3
!          are the most accurate options)
!     For the four methods above, a significant difference in
!     the accuracy of small singular values is possible if
!     the snapshots vary in norm so that X is severely
!     ill-conditioned. If small (smaller than EPS*||X||)
!     singular values are of interest and JOBS=='N',  then
!     the options (3, 4) give the most accurate results, where
!     the option 4 is slightly better and with stronger 
!     theoretical background.
!     If JOBS=='S', i.e. the columns of X will be normalized,
!     then all methods give nearly equally accurate results.
!.....
!     M (input) INTEGER, M >= 0 
!     The state space dimension (the number of rows of F)
!.....      
!     N (input) INTEGER, 0 <= N <= M
!     The number of data snapshots from a single trajectory,
!     taken at equidistant discrete times. This is the 
!     number of columns of F.
!.....
!     F (input/output) REAL(KIND=WP) M-by-N array
!     > On entry,
!     the columns of F are the sequence of data snapshots 
!     from a single trajectory, taken at equidistant discrete
!     times. It is assumed that the column norms of F are 
!     in the range of the normalized floating point numbers. 
!     < On exit,
!     If JOBQ == 'Q', the array F contains the orthogonal 
!     matrix/factor of the QR factorization of the initial 
!     data snapshots matrix F. See the description of JOBQ. 
!     If JOBQ == 'N', the entries in F strictly below the main
!     diagonal contain, column-wise, the information on the 
!     Householder vectors, as returned by SGEQRF. The 
!     remaining information to restore the orthogonal matrix
!     of the initial QR factorization is stored in WORK(1:N). 
!     See the description of WORK.
!.....
!     LDF (input) INTEGER, LDF >= M 
!     The leading dimension of the array F.
!.....
!     X (workspace/output) REAL(KIND=WP) MIN(M,N)-by-(N-1) array
!     X is used as workspace to hold representations of the
!     leading N-1 snapshots in the orthonormal basis computed
!     in the QR factorization of F.
!     On exit, the leading K columns of X contain the leading
!     K left singular vectors of the above described content
!     of X. To lift them to the space of the left singular
!     vectors U(:,1:K)of the input data, pre-multiply with the 
!     Q factor from the initial QR factorization. 
!     See the descriptions of F, K, V  and Z.
!.....      
!     LDX (input) INTEGER, LDX >= N  
!     The leading dimension of the array X 
!.....
!     Y (workspace/output) REAL(KIND=WP) MIN(M,N)-by-(N-1) array
!     Y is used as workspace to hold representations of the
!     trailing N-1 snapshots in the orthonormal basis computed
!     in the QR factorization of F.
!     On exit, 
!     If JOBT == 'R', Y contains the MIN(M,N)-by-N upper
!     triangular factor from the QR factorization of the data
!     snapshot matrix F.
!.....      
!     LDY (input) INTEGER , LDY >= N
!     The leading dimension of the array Y   
!.....
!     NRNK (input) INTEGER
!     Determines the mode how to compute the numerical rank,
!     i.e. how to truncate small singular values of the input
!     matrix X. On input, if
!     NRNK = -1 :: i-th singular value sigma(i) is truncated
!                  if sigma(i) <= TOL*sigma(1)
!                  This option is recommended.   
!     NRNK = -2 :: i-th singular value sigma(i) is truncated
!                  if sigma(i) <= TOL*sigma(i-1)
!                  This option is included for R&D purposes.
!                  It requires highly accurate SVD, which
!                  may not be feasible.     
!     The numerical rank can be enforced by using positive 
!     value of NRNK as follows: 
!     0 < NRNK <= N-1 :: at most NRNK largest singular values
!     will be used. If the number of the computed nonzero
!     singular values is less than NRNK, then only those
!     nonzero values will be used and the actually used
!     dimension is less than NRNK. The actual number of
!     the nonzero singular values is returned in the variable
!     K. See the description of K.
!.....
!     TOL (input) REAL(KIND=WP), 0 <= TOL < 1
!     The tolerance for truncating small singular values.
!     See the description of NRNK.  
!.....
!     K (output) INTEGER,  0 <= K <= N 
!     The dimension of the SVD/POD basis for the leading N-1
!     data snapshots (columns of F) and the number of the 
!     computed Ritz pairs. The value of K is determined
!     according to the rule set by the parameters NRNK and 
!     TOL. See the descriptions of NRNK and TOL. 
!.....
!     REIG (output) REAL(KIND=WP) (N-1)-by-1 array
!     The leading K (K<=N) entries of REIG contain
!     the real parts of the computed eigenvalues
!     REIG(1:K) + sqrt(-1)*IMEIG(1:K).
!     See the descriptions of K, IMEIG, Z.
!.....
!     IMEIG (output) REAL(KIND=WP) (N-1)-by-1 array
!     The leading K (K<N) entries of REIG contain
!     the imaginary parts of the computed eigenvalues
!     REIG(1:K) + sqrt(-1)*IMEIG(1:K).
!     The eigenvalues are determined as follows:
!     If IMEIG(i) == 0, then the corresponding eigenvalue is
!     real, LAMBDA(i) = REIG(i).
!     If IMEIG(i)>0, then the corresponding complex
!     conjugate pair of eigenvalues reads
!     LAMBDA(i)   = REIG(i) + sqrt(-1)*IMAG(i)
!     LAMBDA(i+1) = REIG(i) - sqrt(-1)*IMAG(i)
!     That is, complex conjugate pairs have consecutive
!     indices (i,i+1), with the positive imaginary part
!     listed first.
!     See the descriptions of K, REIG, Z.     
!.....      
!     Z (workspace/output) REAL(KIND=WP)  M-by-(N-1) array
!     If JOBZ =='V' then
!        Z contains real Ritz vectors as follows:
!        If IMEIG(i)=0, then Z(:,i) is an eigenvector of
!        the i-th Ritz value.
!        If IMEIG(i) > 0 (and IMEIG(i+1) < 0) then
!        [Z(:,i) Z(:,i+1)] span an invariant subspace and
!        the Ritz values extracted from this subspace are
!        REIG(i) + sqrt(-1)*IMEIG(i) and
!        REIG(i) - sqrt(-1)*IMEIG(i).
!        The corresponding eigenvectors are
!        Z(:,i) + sqrt(-1)*Z(:,i+1) and
!        Z(:,i) - sqrt(-1)*Z(:,i+1), respectively.
!     If JOBZ == 'F', then the above descriptions hold for
!     the columns of Z*V, where the columns of V are the
!     eigenvectors of the K-by-K Rayleigh quotient, and Z is
!     orthonormal. The columns of V are similarly structured:
!     If IMEIG(i) == 0 then Z*V(:,i) is an eigenvector, and if 
!     IMEIG(i) > 0 then Z*V(:,i)+sqrt(-1)*Z*V(:,i+1) and
!                       Z*V(:,i)-sqrt(-1)*Z*V(:,i+1)
!     are the eigenvectors of LAMBDA(i), LAMBDA(i+1).
!     See the descriptions of REIG, IMEIG, X and V.
!.....
!     LDZ (input) INTEGER , LDZ >= M
!     The leading dimension of the array Z.
!.....
!     RES (output) REAL(KIND=WP) (N-1)-by-1 array
!     RES(1:K) contains the residuals for the K computed 
!     Ritz pairs.       
!     If LAMBDA(i) is real, then
!        RES(i) = || A * Z(:,i) - LAMBDA(i)*Z(:,i))||_2.
!     If [LAMBDA(i), LAMBDA(i+1)] is a complex conjugate pair
!     then
!     RES(i)=RES(i+1) = || A * Z(:,i:i+1) - Z(:,i:i+1) *B||_F
!     where B = [ real(LAMBDA(i)) imag(LAMBDA(i)) ]
!               [-imag(LAMBDA(i)) real(LAMBDA(i)) ].
!     It holds that
!     RES(i)   = || A*ZC(:,i)   - LAMBDA(i)  *ZC(:,i)   ||_2
!     RES(i+1) = || A*ZC(:,i+1) - LAMBDA(i+1)*ZC(:,i+1) ||_2
!     where ZC(:,i)   =  Z(:,i) + sqrt(-1)*Z(:,i+1)
!           ZC(:,i+1) =  Z(:,i) - sqrt(-1)*Z(:,i+1)
!     See the description of Z.
!.....
!     B (output) REAL(KIND=WP)  MIN(M,N)-by-(N-1) array.
!     IF JOBF =='R', B(1:N,1:K) contains A*U(:,1:K), and can
!     be used for computing the refined vectors; see further 
!     details in the provided references. 
!     If JOBF == 'E', B(1:N,1;K) contains 
!     A*U(:,1:K)*W(1:K,1:K), which are the vectors from the
!     Exact DMD, up to scaling by the inverse eigenvalues.   
!     In both cases, the content of B can be lifted to the 
!     original dimension of the input data by pre-multiplying
!     with the Q factor from the initial QR factorization.     
!     Here A denotes a compression of the underlying operator.      
!     See the descriptions of F and X.
!     If JOBF =='N', then B is not referenced.
!.....
!     LDB (input) INTEGER, LDB >= MIN(M,N)
!     The leading dimension of the array B.
!.....      
!     V (workspace/output) REAL(KIND=WP) (N-1)-by-(N-1) array
!     On exit, V(1:K,1:K) contains the K eigenvectors of
!     the Rayleigh quotient. The eigenvectors of a complex
!     conjugate pair of eigenvalues are returned in real form
!     as explained in the description of Z. The Ritz vectors
!     (returned in Z) are the product of X and V; see
!     the descriptions of X and Z.
!.....
!     LDV (input) INTEGER, LDV >= N-1
!     The leading dimension of the array V.
!.....      
!     S (output) REAL(KIND=WP) (N-1)-by-(N-1) array
!     The array S(1:K,1:K) is used for the matrix Rayleigh
!     quotient. This content is overwritten during
!     the eigenvalue decomposition by SGEEV.
!     See the description of K.
!.....
!     LDS (input) INTEGER, LDS >= N-1        
!     The leading dimension of the array S.
!.....
!     WORK (workspace/output) REAL(KIND=WP) LWORK-by-1 array
!     On exit, 
!     WORK(1:MIN(M,N)) contains the scalar factors of the 
!     elementary reflectors as returned by SGEQRF of the 
!     M-by-N input matrix F.
!     WORK(MIN(M,N)+1:MIN(M,N)+N-1) contains the singular values of 
!     the input submatrix F(1:M,1:N-1).
!     If the call to SGEDMDQ is only workspace query, then
!     WORK(1) contains the minimal workspace length and
!     WORK(2) is the optimal workspace length. Hence, the
!     length of work is at least 2.
!     See the description of LWORK.
!.....
!     LWORK (input) INTEGER
!     The minimal length of the  workspace vector WORK.
!     LWORK is calculated as follows:
!     Let MLWQR  = N (minimal workspace for SGEQRF[M,N])
!         MLWDMD = minimal workspace for SGEDMD (see the
!                  description of LWORK in SGEDMD) for 
!                  snapshots of dimensions MIN(M,N)-by-(N-1)
!         MLWMQR = N (minimal workspace for 
!                    SORMQR['L','N',M,N,N])
!         MLWGQR = N (minimal workspace for SORGQR[M,N,N])
!     Then
!     LWORK = MAX(N+MLWQR, N+MLWDMD)
!     is updated as follows:
!        if   JOBZ == 'V' or JOBZ == 'F' THEN 
!             LWORK = MAX( LWORK,MIN(M,N)+N-1 +MLWMQR )
!        if   JOBQ == 'Q' THEN
!             LWORK = MAX( LWORK,MIN(M,N)+N-1+MLWGQR)
!     If on entry LWORK = -1, then a workspace query is
!     assumed and the procedure only computes the minimal
!     and the optimal workspace lengths for both WORK and
!     IWORK. See the descriptions of WORK and IWORK.          
!.....
!     IWORK (workspace/output) INTEGER LIWORK-by-1 array
!     Workspace that is required only if WHTSVD equals
!     2 , 3 or 4. (See the description of WHTSVD).
!     If on entry LWORK =-1 or LIWORK=-1, then the
!     minimal length of IWORK is computed and returned in
!     IWORK(1). See the description of LIWORK.
!.....
!     LIWORK (input) INTEGER
!     The minimal length of the workspace vector IWORK.
!     If WHTSVD == 1, then only IWORK(1) is used; LIWORK >=1
!     Let M1=MIN(M,N), N1=N-1. Then
!     If WHTSVD == 2, then LIWORK >= MAX(1,8*MIN(M1,N1))
!     If WHTSVD == 3, then LIWORK >= MAX(1,M1+N1-1)
!     If WHTSVD == 4, then LIWORK >= MAX(3,M1+3*N1)
!     If on entry LIWORK = -1, then a worskpace query is
!     assumed and the procedure only computes the minimal
!     and the optimal workspace lengths for both WORK and
!     IWORK. See the descriptions of WORK and IWORK.
!..... 
!     INFO (output) INTEGER
!     -i < 0 :: On entry, the i-th argument had an
!               illegal value
!        = 0 :: Successful return.
!        = 1 :: Void input. Quick exit (M=0 or N=0).
!        = 2 :: The SVD computation of X did not converge.
!               Suggestion: Check the input data and/or
!               repeat with different WHTSVD.
!        = 3 :: The computation of the eigenvalues did not
!               converge.
!        = 4 :: If data scaling was requested on input and
!               the procedure found inconsistency in the data
!               such that for some column index i,
!               X(:,i) = 0 but Y(:,i) /= 0, then Y(:,i) is set
!               to zero if JOBS=='C'. The computation proceeds
!               with original or modified data and warning
!               flag is set with INFO=4.  
!.............................................................
!.............................................................
!     Parameters
!     ~~~~~~~~~~      
      REAL(KIND=WP), PARAMETER ::  ONE = 1.0_WP
      REAL(KIND=WP), PARAMETER :: ZERO = 0.0_WP
!      
!     Local scalars      
!     ~~~~~~~~~~~~~
      INTEGER           :: IMINWR, INFO1,  MLWDMD, MLWGQR, &
                           MLWMQR, MLWORK, MLWQR,  MINMN,  & 
                           OLWDMD, OLWGQR, OLWMQR, OLWORK, &
                           OLWQR
      LOGICAL           :: LQUERY, SCCOLX, SCCOLY, WANTQ,  &
                           WNTTRF, WNTRES, WNTVEC, WNTVCF, &
                           WNTVCQ, WNTREF, WNTEX
      CHARACTER(LEN=1)  :: JOBVL
!      
!     Local array      
!     ~~~~~~~~~~~      
      REAL(KIND=WP) :: RDUMMY(2)
!      
!     External functions (BLAS and LAPACK)
!     ~~~~~~~~~~~~~~~~~
      LOGICAL       LSAME
      EXTERNAL      LSAME 
!
!     External subroutines (BLAS and LAPACK)
!     ~~~~~~~~~~~~~~~~~~~~
      EXTERNAL      SGEMM 
      EXTERNAL      SGEQRF, SLACPY, SLASET, SORGQR, & 
                    SORMQR, XERBLA

!     External subroutines
!     ~~~~~~~~~~~~~~~~~~~~
      EXTERNAL      SGEDMD 
      
!     Intrinsic functions
!     ~~~~~~~~~~~~~~~~~~~
      INTRINSIC      MAX, MIN, INT         
 !..........................................................  
 !
 !    Test the input arguments    
      WNTRES = LSAME(JOBR,'R')
      SCCOLX = LSAME(JOBS,'S') .OR. LSAME( JOBS, 'C' )
      SCCOLY = LSAME(JOBS,'Y')
      WNTVEC = LSAME(JOBZ,'V') 
      WNTVCF = LSAME(JOBZ,'F')
      WNTVCQ = LSAME(JOBZ,'Q')
      WNTREF = LSAME(JOBF,'R') 
      WNTEX  = LSAME(JOBF,'E')
      WANTQ  = LSAME(JOBQ,'Q')
      WNTTRF = LSAME(JOBT,'R')     
      MINMN  = MIN(M,N)
      INFO = 0 
      LQUERY = ( ( LWORK == -1 ) .OR. ( LIWORK == -1 ) )
!       
      IF ( .NOT. (SCCOLX .OR. SCCOLY .OR. LSAME(JOBS,'N')) )  THEN 
          INFO = -1
      ELSE IF ( .NOT. (WNTVEC .OR. WNTVCF .OR. WNTVCQ       &
                              .OR. LSAME(JOBZ,'N')) ) THEN
          INFO = -2
      ELSE IF ( .NOT. (WNTRES .OR. LSAME(JOBR,'N')) .OR.    & 
          ( WNTRES .AND. LSAME(JOBZ,'N') ) ) THEN
          INFO = -3
      ELSE IF ( .NOT. (WANTQ .OR. LSAME(JOBQ,'N')) ) THEN
          INFO = -4                 
      ELSE IF ( .NOT. ( WNTTRF .OR. LSAME(JOBT,'N') ) )  THEN
          INFO = -5
      ELSE IF ( .NOT. (WNTREF .OR. WNTEX .OR.             & 
                LSAME(JOBF,'N') ) )                    THEN
          INFO = -6    
      ELSE IF ( .NOT. ((WHTSVD == 1).OR.(WHTSVD == 2).OR.   &
                       (WHTSVD == 3).OR.(WHTSVD == 4)) ) THEN
          INFO = -7
      ELSE IF ( M < 0 ) THEN
          INFO = -8
      ELSE IF ( ( N < 0 ) .OR. ( N > M+1 ) ) THEN
          INFO = -9
      ELSE IF ( LDF < M ) THEN
          INFO = -11
      ELSE IF ( LDX < MINMN ) THEN
          INFO = -13
      ELSE IF ( LDY < MINMN ) THEN
          INFO = -15
      ELSE IF ( .NOT. (( NRNK == -2).OR.(NRNK == -1).OR.    & 
                       ((NRNK >= 1).AND.(NRNK <=N ))) )  THEN
          INFO = -16
      ELSE IF ( ( TOL < ZERO ) .OR. ( TOL >= ONE ) ) THEN
          INFO = -17
      ELSE IF ( LDZ < M ) THEN
          INFO = -22
      ELSE IF ( (WNTREF.OR.WNTEX ).AND.( LDB < MINMN ) ) THEN
          INFO = -25
      ELSE IF ( LDV < N-1 ) THEN
          INFO = -27
      ELSE IF ( LDS < N-1 ) THEN
          INFO = -29
      END IF
!      
      IF ( WNTVEC .OR. WNTVCF ) THEN
          JOBVL = 'V'
      ELSE
          JOBVL = 'N'
      END IF     
      IF ( INFO == 0 ) THEN  
          ! Compute the minimal and the optimal workspace
          ! requirements. Simulate running the code and 
          ! determine minimal and optimal sizes of the 
          ! workspace at any moment of the run.         
         IF ( ( N == 0 ) .OR. ( N == 1 ) ) THEN
             ! All output except K is void. INFO=1 signals
             ! the void input. In case of a workspace query,
             ! the minimal workspace lengths are returned.
            IF ( LQUERY ) THEN  
               IWORK(1) = 1
                WORK(1) = 2
                WORK(2) = 2
            ELSE                
               K = 0
            END IF             
            INFO = 1  
            RETURN
         END IF     
         MLWQR  = MAX(1,N)  ! Minimal workspace length for SGEQRF.
         MLWORK = MIN(M,N) + MLWQR 
         IF ( LQUERY ) THEN 
             CALL SGEQRF( M, N, F, LDF, WORK, RDUMMY, -1, &
                          INFO1 )
             OLWQR = INT(RDUMMY(1))
             OLWORK = MIN(M,N) + OLWQR           
         END IF
         CALL SGEDMD( JOBS, JOBVL, JOBR, JOBF, WHTSVD, MINMN,& 
                      N-1, X, LDX, Y, LDY, NRNK, TOL, K,     & 
                      REIG, IMEIG, Z, LDZ, RES,  B, LDB,     & 
                      V, LDV, S, LDS, WORK, -1, IWORK,       &
                      LIWORK, INFO1 )
         MLWDMD = INT(WORK(1))
         MLWORK = MAX(MLWORK, MINMN + MLWDMD)
         IMINWR = IWORK(1)
         IF ( LQUERY ) THEN 
             OLWDMD = INT(WORK(2))
             OLWORK = MAX(OLWORK, MINMN+OLWDMD)
         END IF
         IF ( WNTVEC .OR. WNTVCF ) THEN
            MLWMQR = MAX(1,N) 
            MLWORK = MAX(MLWORK,MINMN+N-1+MLWMQR)
            IF ( LQUERY ) THEN
               CALL SORMQR( 'L','N', M, N, MINMN, F, LDF,  & 
                            WORK, Z, LDZ, WORK, -1, INFO1 )
               OLWMQR = INT(WORK(1))
               OLWORK = MAX(OLWORK,MINMN+N-1+OLWMQR)
            END IF
         END IF  
         IF ( WANTQ ) THEN
            MLWGQR = N
            MLWORK = MAX(MLWORK,MINMN+N-1+MLWGQR)
            IF ( LQUERY ) THEN 
                CALL SORGQR( M, MINMN, MINMN, F, LDF, WORK, &
                             WORK, -1, INFO1 )        
                OLWGQR = INT(WORK(1))
                OLWORK = MAX(OLWORK,MINMN+N-1+OLWGQR)
            END IF            
         END IF   
         IMINWR = MAX( 1, IMINWR )
         MLWORK = MAX( 2, MLWORK )      
         IF (  LWORK < MLWORK .AND. (.NOT.LQUERY) ) INFO = -31
         IF ( LIWORK < IMINWR .AND. (.NOT.LQUERY) ) INFO = -33
      END IF  
      IF( INFO /= 0 ) THEN
         CALL XERBLA( 'SGEDMDQ', -INFO )
         RETURN
      ELSE IF ( LQUERY ) THEN
!     Return minimal and optimal workspace sizes
          IWORK(1) = IMINWR
          WORK(1)  = MLWORK
          WORK(2)  = OLWORK
          RETURN
      END IF   
!.....	  
!     Initial QR factorization that is used to represent the
!     snapshots as elements of lower dimensional subspace.
!     For large scale computation with M >>N , at this place 
!     one can use an out of core QRF.
!   
      CALL SGEQRF( M, N, F, LDF, WORK,         & 
                   WORK(MINMN+1), LWORK-MINMN, INFO1 )
!      
!     Define X and Y as the snapshots representations in the
!     orthogonal basis computed in the QR factorization.
!     X corresponds to the leading N-1 and Y to the trailing
!     N-1 snapshots.
      CALL SLASET( 'L', MINMN, N-1, ZERO,  ZERO, X, LDX )
      CALL SLACPY( 'U', MINMN, N-1, F,      LDF, X, LDX )
      CALL SLACPY( 'A', MINMN, N-1, F(1,2), LDF, Y, LDY )
      IF ( M >= 3 ) THEN
          CALL SLASET( 'L', MINMN-2, N-2, ZERO,  ZERO, &
                       Y(3,1), LDY )  
      END IF
!
!     Compute the DMD of the projected snapshot pairs (X,Y)   
      CALL SGEDMD( JOBS, JOBVL, JOBR, JOBF, WHTSVD, MINMN,  &
                   N-1, X, LDX, Y, LDY, NRNK,   TOL, K,     &
                   REIG, IMEIG, Z, LDZ, RES, B, LDB, V,     &
                   LDV, S, LDS, WORK(MINMN+1), LWORK-MINMN, IWORK,  & 
                   LIWORK, INFO1 )
      IF ( INFO1 == 2 .OR. INFO1 == 3 ) THEN
          ! Return with error code.
          INFO = INFO1
          RETURN
      ELSE
          INFO = INFO1
      END IF    
!      
!     The Ritz vectors (Koopman modes) can be explicitly 
!     formed or returned in factored form.
      IF ( WNTVEC ) THEN
        ! Compute the eigenvectors explicitly.  
        IF ( M > MINMN ) CALL SLASET( 'A', M-MINMN, K, ZERO, &
                                     ZERO, Z(MINMN+1,1), LDZ )
        CALL SORMQR( 'L','N', M, K, MINMN, F, LDF, WORK, Z,  &
                     LDZ, WORK(MINMN+N), LWORK-(MINMN+N-1), INFO1 )
      ELSE IF ( WNTVCF ) THEN   
        !   Return the Ritz vectors (eigenvectors) in factored
        !   form Z*V, where Z contains orthonormal matrix (the
        !   product of Q from the initial QR factorization and 
        !   the SVD/POD_basis returned by SGEDMD in X) and the 
        !   second factor (the eigenvectors of the Rayleigh 
        !   quotient) is in the array V, as returned by SGEDMD.
        CALL SLACPY( 'A', N, K, X, LDX, Z, LDZ )
        IF ( M > N ) CALL SLASET( 'A', M-N, K, ZERO, ZERO,   & 
                                  Z(N+1,1), LDZ )
        CALL SORMQR( 'L','N', M, K, MINMN, F, LDF, WORK, Z,  &
             LDZ, WORK(MINMN+N), LWORK-(MINMN+N-1), INFO1 )
      END IF
!     
!     Some optional output variables:
!
!     The upper triangular factor in the initial QR 
!     factorization is optionally returned in the array Y.
!     This is useful if this call to SGEDMDQ is to be 
!     followed by a streaming DMD that is implemented in a 
!     QR compressed form.
      IF ( WNTTRF ) THEN ! Return the upper triangular R in Y 
         CALL SLASET( 'A', MINMN, N, ZERO,  ZERO, Y, LDY )
         CALL SLACPY( 'U', MINMN, N, F, LDF,      Y, LDY )
      END IF    
!
!     The orthonormal/orthogonal factor in the initial QR 
!     factorization is optionally returned in the array F. 
!     Same as with the triangular factor above, this is 
!     useful in a streaming DMD.
      IF ( WANTQ ) THEN  ! Q overwrites F 
         CALL SORGQR( M, MINMN, MINMN, F, LDF, WORK, &
              WORK(MINMN+N), LWORK-(MINMN+N-1), INFO1 )  
      END IF
!      
      RETURN
!      
      END SUBROUTINE SGEDMDQ
    