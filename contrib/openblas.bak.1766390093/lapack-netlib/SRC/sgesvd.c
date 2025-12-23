#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <complex.h>
#ifdef complex
#undef complex
#endif
#ifdef I
#undef I
#endif

#if defined(_WIN64)
typedef long long BLASLONG;
typedef unsigned long long BLASULONG;
#else
typedef long BLASLONG;
typedef unsigned long BLASULONG;
#endif

#ifdef LAPACK_ILP64
typedef BLASLONG blasint;
#if defined(_WIN64)
#define blasabs(x) llabs(x)
#else
#define blasabs(x) labs(x)
#endif
#else
typedef int blasint;
#define blasabs(x) abs(x)
#endif

typedef blasint integer;

typedef unsigned int uinteger;
typedef char *address;
typedef short int shortint;
typedef float real;
typedef double doublereal;
typedef struct { real r, i; } complex;
typedef struct { doublereal r, i; } doublecomplex;
#ifdef _MSC_VER
static inline _Fcomplex Cf(complex *z) {_Fcomplex zz={z->r , z->i}; return zz;}
static inline _Dcomplex Cd(doublecomplex *z) {_Dcomplex zz={z->r , z->i};return zz;}
static inline _Fcomplex * _pCf(complex *z) {return (_Fcomplex*)z;}
static inline _Dcomplex * _pCd(doublecomplex *z) {return (_Dcomplex*)z;}
#else
static inline _Complex float Cf(complex *z) {return z->r + z->i*_Complex_I;}
static inline _Complex double Cd(doublecomplex *z) {return z->r + z->i*_Complex_I;}
static inline _Complex float * _pCf(complex *z) {return (_Complex float*)z;}
static inline _Complex double * _pCd(doublecomplex *z) {return (_Complex double*)z;}
#endif
#define pCf(z) (*_pCf(z))
#define pCd(z) (*_pCd(z))
typedef blasint logical;

typedef char logical1;
typedef char integer1;

#define TRUE_ (1)
#define FALSE_ (0)

/* Extern is for use with -E */
#ifndef Extern
#define Extern extern
#endif

/* I/O stuff */

typedef int flag;
typedef int ftnlen;
typedef int ftnint;

/*external read, write*/
typedef struct
{	flag cierr;
	ftnint ciunit;
	flag ciend;
	char *cifmt;
	ftnint cirec;
} cilist;

/*internal read, write*/
typedef struct
{	flag icierr;
	char *iciunit;
	flag iciend;
	char *icifmt;
	ftnint icirlen;
	ftnint icirnum;
} icilist;

/*open*/
typedef struct
{	flag oerr;
	ftnint ounit;
	char *ofnm;
	ftnlen ofnmlen;
	char *osta;
	char *oacc;
	char *ofm;
	ftnint orl;
	char *oblnk;
} olist;

/*close*/
typedef struct
{	flag cerr;
	ftnint cunit;
	char *csta;
} cllist;

/*rewind, backspace, endfile*/
typedef struct
{	flag aerr;
	ftnint aunit;
} alist;

/* inquire */
typedef struct
{	flag inerr;
	ftnint inunit;
	char *infile;
	ftnlen infilen;
	ftnint	*inex;	/*parameters in standard's order*/
	ftnint	*inopen;
	ftnint	*innum;
	ftnint	*innamed;
	char	*inname;
	ftnlen	innamlen;
	char	*inacc;
	ftnlen	inacclen;
	char	*inseq;
	ftnlen	inseqlen;
	char 	*indir;
	ftnlen	indirlen;
	char	*infmt;
	ftnlen	infmtlen;
	char	*inform;
	ftnint	informlen;
	char	*inunf;
	ftnlen	inunflen;
	ftnint	*inrecl;
	ftnint	*innrec;
	char	*inblank;
	ftnlen	inblanklen;
} inlist;

#define VOID void

union Multitype {	/* for multiple entry points */
	integer1 g;
	shortint h;
	integer i;
	/* longint j; */
	real r;
	doublereal d;
	complex c;
	doublecomplex z;
	};

typedef union Multitype Multitype;

struct Vardesc {	/* for Namelist */
	char *name;
	char *addr;
	ftnlen *dims;
	int  type;
	};
typedef struct Vardesc Vardesc;

struct Namelist {
	char *name;
	Vardesc **vars;
	int nvars;
	};
typedef struct Namelist Namelist;

#define abs(x) ((x) >= 0 ? (x) : -(x))
#define dabs(x) (fabs(x))
#define f2cmin(a,b) ((a) <= (b) ? (a) : (b))
#define f2cmax(a,b) ((a) >= (b) ? (a) : (b))
#define dmin(a,b) (f2cmin(a,b))
#define dmax(a,b) (f2cmax(a,b))
#define bit_test(a,b)	((a) >> (b) & 1)
#define bit_clear(a,b)	((a) & ~((uinteger)1 << (b)))
#define bit_set(a,b)	((a) |  ((uinteger)1 << (b)))

#define abort_() { sig_die("Fortran abort routine called", 1); }
#define c_abs(z) (cabsf(Cf(z)))
#define c_cos(R,Z) { pCf(R)=ccos(Cf(Z)); }
#ifdef _MSC_VER
#define c_div(c, a, b) {Cf(c)._Val[0] = (Cf(a)._Val[0]/Cf(b)._Val[0]); Cf(c)._Val[1]=(Cf(a)._Val[1]/Cf(b)._Val[1]);}
#define z_div(c, a, b) {Cd(c)._Val[0] = (Cd(a)._Val[0]/Cd(b)._Val[0]); Cd(c)._Val[1]=(Cd(a)._Val[1]/df(b)._Val[1]);}
#else
#define c_div(c, a, b) {pCf(c) = Cf(a)/Cf(b);}
#define z_div(c, a, b) {pCd(c) = Cd(a)/Cd(b);}
#endif
#define c_exp(R, Z) {pCf(R) = cexpf(Cf(Z));}
#define c_log(R, Z) {pCf(R) = clogf(Cf(Z));}
#define c_sin(R, Z) {pCf(R) = csinf(Cf(Z));}
//#define c_sqrt(R, Z) {*(R) = csqrtf(Cf(Z));}
#define c_sqrt(R, Z) {pCf(R) = csqrtf(Cf(Z));}
#define d_abs(x) (fabs(*(x)))
#define d_acos(x) (acos(*(x)))
#define d_asin(x) (asin(*(x)))
#define d_atan(x) (atan(*(x)))
#define d_atn2(x, y) (atan2(*(x),*(y)))
#define d_cnjg(R, Z) { pCd(R) = conj(Cd(Z)); }
#define r_cnjg(R, Z) { pCf(R) = conjf(Cf(Z)); }
#define d_cos(x) (cos(*(x)))
#define d_cosh(x) (cosh(*(x)))
#define d_dim(__a, __b) ( *(__a) > *(__b) ? *(__a) - *(__b) : 0.0 )
#define d_exp(x) (exp(*(x)))
#define d_imag(z) (cimag(Cd(z)))
#define r_imag(z) (cimagf(Cf(z)))
#define d_int(__x) (*(__x)>0 ? floor(*(__x)) : -floor(- *(__x)))
#define r_int(__x) (*(__x)>0 ? floor(*(__x)) : -floor(- *(__x)))
#define d_lg10(x) ( 0.43429448190325182765 * log(*(x)) )
#define r_lg10(x) ( 0.43429448190325182765 * log(*(x)) )
#define d_log(x) (log(*(x)))
#define d_mod(x, y) (fmod(*(x), *(y)))
#define u_nint(__x) ((__x)>=0 ? floor((__x) + .5) : -floor(.5 - (__x)))
#define d_nint(x) u_nint(*(x))
#define u_sign(__a,__b) ((__b) >= 0 ? ((__a) >= 0 ? (__a) : -(__a)) : -((__a) >= 0 ? (__a) : -(__a)))
#define d_sign(a,b) u_sign(*(a),*(b))
#define r_sign(a,b) u_sign(*(a),*(b))
#define d_sin(x) (sin(*(x)))
#define d_sinh(x) (sinh(*(x)))
#define d_sqrt(x) (sqrt(*(x)))
#define d_tan(x) (tan(*(x)))
#define d_tanh(x) (tanh(*(x)))
#define i_abs(x) abs(*(x))
#define i_dnnt(x) ((integer)u_nint(*(x)))
#define i_len(s, n) (n)
#define i_nint(x) ((integer)u_nint(*(x)))
#define i_sign(a,b) ((integer)u_sign((integer)*(a),(integer)*(b)))
#define s_cat(lpp, rpp, rnp, np, llp) { 	ftnlen i, nc, ll; char *f__rp, *lp; 	ll = (llp); lp = (lpp); 	for(i=0; i < (int)*(np); ++i) {         	nc = ll; 	        if((rnp)[i] < nc) nc = (rnp)[i]; 	        ll -= nc;         	f__rp = (rpp)[i]; 	        while(--nc >= 0) *lp++ = *(f__rp)++;         } 	while(--ll >= 0) *lp++ = ' '; }
#define s_cmp(a,b,c,d) ((integer)strncmp((a),(b),f2cmin((c),(d))))
#define s_copy(A,B,C,D) { int __i,__m; for (__i=0, __m=f2cmin((C),(D)); __i<__m && (B)[__i] != 0; ++__i) (A)[__i] = (B)[__i]; }
#define sig_die(s, kill) { exit(1); }
#define s_stop(s, n) {exit(0);}
#define z_abs(z) (cabs(Cd(z)))
#define z_exp(R, Z) {pCd(R) = cexp(Cd(Z));}
#define z_sqrt(R, Z) {pCd(R) = csqrt(Cd(Z));}
#define myexit_() break;
#define mycycle() continue;
#define myceiling(w) {ceil(w)}
#define myhuge(w) {HUGE_VAL}
//#define mymaxloc_(w,s,e,n) {if (sizeof(*(w)) == sizeof(double)) dmaxloc_((w),*(s),*(e),n); else dmaxloc_((w),*(s),*(e),n);}
#define mymaxloc(w,s,e,n) {dmaxloc_(w,*(s),*(e),n)}

/*  -- translated by f2c (version 20000121).
   You must link the resulting object file with the libraries:
	-lf2c -lm   (in that order)
*/




/* Table of constant values */

static integer c__6 = 6;
static integer c__0 = 0;
static integer c__2 = 2;
static integer c_n1 = -1;
static real c_b57 = 0.f;
static integer c__1 = 1;
static real c_b79 = 1.f;

/* > \brief <b> SGESVD computes the singular value decomposition (SVD) for GE matrices</b> */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download SGESVD + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/sgesvd.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/sgesvd.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/sgesvd.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE SGESVD( JOBU, JOBVT, M, N, A, LDA, S, U, LDU, VT, LDVT, */
/*                          WORK, LWORK, INFO ) */

/*       CHARACTER          JOBU, JOBVT */
/*       INTEGER            INFO, LDA, LDU, LDVT, LWORK, M, N */
/*       REAL               A( LDA, * ), S( * ), U( LDU, * ), */
/*      $                   VT( LDVT, * ), WORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > SGESVD computes the singular value decomposition (SVD) of a real */
/* > M-by-N matrix A, optionally computing the left and/or right singular */
/* > vectors. The SVD is written */
/* > */
/* >      A = U * SIGMA * transpose(V) */
/* > */
/* > where SIGMA is an M-by-N matrix which is zero except for its */
/* > f2cmin(m,n) diagonal elements, U is an M-by-M orthogonal matrix, and */
/* > V is an N-by-N orthogonal matrix.  The diagonal elements of SIGMA */
/* > are the singular values of A; they are real and non-negative, and */
/* > are returned in descending order.  The first f2cmin(m,n) columns of */
/* > U and V are the left and right singular vectors of A. */
/* > */
/* > Note that the routine returns V**T, not V. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] JOBU */
/* > \verbatim */
/* >          JOBU is CHARACTER*1 */
/* >          Specifies options for computing all or part of the matrix U: */
/* >          = 'A':  all M columns of U are returned in array U: */
/* >          = 'S':  the first f2cmin(m,n) columns of U (the left singular */
/* >                  vectors) are returned in the array U; */
/* >          = 'O':  the first f2cmin(m,n) columns of U (the left singular */
/* >                  vectors) are overwritten on the array A; */
/* >          = 'N':  no columns of U (no left singular vectors) are */
/* >                  computed. */
/* > \endverbatim */
/* > */
/* > \param[in] JOBVT */
/* > \verbatim */
/* >          JOBVT is CHARACTER*1 */
/* >          Specifies options for computing all or part of the matrix */
/* >          V**T: */
/* >          = 'A':  all N rows of V**T are returned in the array VT; */
/* >          = 'S':  the first f2cmin(m,n) rows of V**T (the right singular */
/* >                  vectors) are returned in the array VT; */
/* >          = 'O':  the first f2cmin(m,n) rows of V**T (the right singular */
/* >                  vectors) are overwritten on the array A; */
/* >          = 'N':  no rows of V**T (no right singular vectors) are */
/* >                  computed. */
/* > */
/* >          JOBVT and JOBU cannot both be 'O'. */
/* > \endverbatim */
/* > */
/* > \param[in] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >          The number of rows of the input matrix A.  M >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The number of columns of the input matrix A.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] A */
/* > \verbatim */
/* >          A is REAL array, dimension (LDA,N) */
/* >          On entry, the M-by-N matrix A. */
/* >          On exit, */
/* >          if JOBU = 'O',  A is overwritten with the first f2cmin(m,n) */
/* >                          columns of U (the left singular vectors, */
/* >                          stored columnwise); */
/* >          if JOBVT = 'O', A is overwritten with the first f2cmin(m,n) */
/* >                          rows of V**T (the right singular vectors, */
/* >                          stored rowwise); */
/* >          if JOBU .ne. 'O' and JOBVT .ne. 'O', the contents of A */
/* >                          are destroyed. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of the array A.  LDA >= f2cmax(1,M). */
/* > \endverbatim */
/* > */
/* > \param[out] S */
/* > \verbatim */
/* >          S is REAL array, dimension (f2cmin(M,N)) */
/* >          The singular values of A, sorted so that S(i) >= S(i+1). */
/* > \endverbatim */
/* > */
/* > \param[out] U */
/* > \verbatim */
/* >          U is REAL array, dimension (LDU,UCOL) */
/* >          (LDU,M) if JOBU = 'A' or (LDU,f2cmin(M,N)) if JOBU = 'S'. */
/* >          If JOBU = 'A', U contains the M-by-M orthogonal matrix U; */
/* >          if JOBU = 'S', U contains the first f2cmin(m,n) columns of U */
/* >          (the left singular vectors, stored columnwise); */
/* >          if JOBU = 'N' or 'O', U is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDU */
/* > \verbatim */
/* >          LDU is INTEGER */
/* >          The leading dimension of the array U.  LDU >= 1; if */
/* >          JOBU = 'S' or 'A', LDU >= M. */
/* > \endverbatim */
/* > */
/* > \param[out] VT */
/* > \verbatim */
/* >          VT is REAL array, dimension (LDVT,N) */
/* >          If JOBVT = 'A', VT contains the N-by-N orthogonal matrix */
/* >          V**T; */
/* >          if JOBVT = 'S', VT contains the first f2cmin(m,n) rows of */
/* >          V**T (the right singular vectors, stored rowwise); */
/* >          if JOBVT = 'N' or 'O', VT is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDVT */
/* > \verbatim */
/* >          LDVT is INTEGER */
/* >          The leading dimension of the array VT.  LDVT >= 1; if */
/* >          JOBVT = 'A', LDVT >= N; if JOBVT = 'S', LDVT >= f2cmin(M,N). */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is REAL array, dimension (MAX(1,LWORK)) */
/* >          On exit, if INFO = 0, WORK(1) returns the optimal LWORK; */
/* >          if INFO > 0, WORK(2:MIN(M,N)) contains the unconverged */
/* >          superdiagonal elements of an upper bidiagonal matrix B */
/* >          whose diagonal is in S (not necessarily sorted). B */
/* >          satisfies A = U * B * VT, so it has the same singular values */
/* >          as A, and singular vectors related by U and VT. */
/* > \endverbatim */
/* > */
/* > \param[in] LWORK */
/* > \verbatim */
/* >          LWORK is INTEGER */
/* >          The dimension of the array WORK. */
/* >          LWORK >= MAX(1,5*MIN(M,N)) for the paths (see comments inside code): */
/* >             - PATH 1  (M much larger than N, JOBU='N') */
/* >             - PATH 1t (N much larger than M, JOBVT='N') */
/* >          LWORK >= MAX(1,3*MIN(M,N)+MAX(M,N),5*MIN(M,N)) for the other paths */
/* >          For good performance, LWORK should generally be larger. */
/* > */
/* >          If LWORK = -1, then a workspace query is assumed; the routine */
/* >          only calculates the optimal size of the WORK array, returns */
/* >          this value as the first entry of the WORK array, and no error */
/* >          message related to LWORK is issued by XERBLA. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit. */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value. */
/* >          > 0:  if SBDSQR did not converge, INFO specifies how many */
/* >                superdiagonals of an intermediate bidiagonal form B */
/* >                did not converge to zero. See the description of WORK */
/* >                above for details. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date April 2012 */

/* > \ingroup realGEsing */

/*  ===================================================================== */
/* Subroutine */ void sgesvd_(char *jobu, char *jobvt, integer *m, integer *n, 
	real *a, integer *lda, real *s, real *u, integer *ldu, real *vt, 
	integer *ldvt, real *work, integer *lwork, integer *info)
{
    /* System generated locals */
    address a__1[2];
    integer a_dim1, a_offset, u_dim1, u_offset, vt_dim1, vt_offset, i__1[2], 
	    i__2, i__3, i__4;
    char ch__1[2];

    /* Local variables */
    integer iscl;
    real anrm;
    integer ierr, itau, ncvt, nrvt, lwork_sgebrd__, lwork_sgelqf__, 
	    lwork_sgeqrf__, i__;
    extern logical lsame_(char *, char *);
    integer chunk;
    extern /* Subroutine */ void sgemm_(char *, char *, integer *, integer *, 
	    integer *, real *, real *, integer *, real *, integer *, real *, 
	    real *, integer *);
    integer minmn, wrkbl, itaup, itauq, mnthr, iwork;
    logical wntua, wntva, wntun, wntuo, wntvn, wntvo, wntus, wntvs;
    integer ie, ir, bdspac, iu;
    extern /* Subroutine */ void sgebrd_(integer *, integer *, real *, integer 
	    *, real *, real *, real *, real *, real *, integer *, integer *);
    extern real slamch_(char *), slange_(char *, integer *, integer *,
	     real *, integer *, real *);
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    extern integer ilaenv_(integer *, char *, char *, integer *, integer *, 
	    integer *, integer *, ftnlen, ftnlen);
    real bignum;
    extern /* Subroutine */ void sgelqf_(integer *, integer *, real *, integer 
	    *, real *, real *, integer *, integer *), slascl_(char *, integer 
	    *, integer *, real *, real *, integer *, integer *, real *, 
	    integer *, integer *), sgeqrf_(integer *, integer *, real 
	    *, integer *, real *, real *, integer *, integer *), slacpy_(char 
	    *, integer *, integer *, real *, integer *, real *, integer *), slaset_(char *, integer *, integer *, real *, real *, 
	    real *, integer *), sbdsqr_(char *, integer *, integer *, 
	    integer *, integer *, real *, real *, real *, integer *, real *, 
	    integer *, real *, integer *, real *, integer *), sorgbr_(
	    char *, integer *, integer *, integer *, real *, integer *, real *
	    , real *, integer *, integer *), sormbr_(char *, char *, 
	    char *, integer *, integer *, integer *, real *, integer *, real *
	    , real *, integer *, real *, integer *, integer *);
    integer ldwrkr, minwrk, ldwrku, maxwrk;
    extern /* Subroutine */ void sorglq_(integer *, integer *, integer *, real 
	    *, integer *, real *, real *, integer *, integer *);
    real smlnum;
    extern /* Subroutine */ void sorgqr_(integer *, integer *, integer *, real 
	    *, integer *, real *, real *, integer *, integer *);
    logical lquery, wntuas, wntvas;
    integer blk, lwork_sorgbr_p__, lwork_sorgbr_q__, lwork_sorglq_m__, 
	    lwork_sorglq_n__, ncu, lwork_sorgqr_n__, lwork_sorgqr_m__;
    real eps, dum[1];
    integer nru;


/*  -- LAPACK driver routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     April 2012 */


/*  ===================================================================== */


/*     Test the input arguments */

    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    --s;
    u_dim1 = *ldu;
    u_offset = 1 + u_dim1 * 1;
    u -= u_offset;
    vt_dim1 = *ldvt;
    vt_offset = 1 + vt_dim1 * 1;
    vt -= vt_offset;
    --work;

    /* Function Body */
    *info = 0;
    minmn = f2cmin(*m,*n);
    wntua = lsame_(jobu, "A");
    wntus = lsame_(jobu, "S");
    wntuas = wntua || wntus;
    wntuo = lsame_(jobu, "O");
    wntun = lsame_(jobu, "N");
    wntva = lsame_(jobvt, "A");
    wntvs = lsame_(jobvt, "S");
    wntvas = wntva || wntvs;
    wntvo = lsame_(jobvt, "O");
    wntvn = lsame_(jobvt, "N");
    lquery = *lwork == -1;

    if (! (wntua || wntus || wntuo || wntun)) {
	*info = -1;
    } else if (! (wntva || wntvs || wntvo || wntvn) || wntvo && wntuo) {
	*info = -2;
    } else if (*m < 0) {
	*info = -3;
    } else if (*n < 0) {
	*info = -4;
    } else if (*lda < f2cmax(1,*m)) {
	*info = -6;
    } else if (*ldu < 1 || wntuas && *ldu < *m) {
	*info = -9;
    } else if (*ldvt < 1 || wntva && *ldvt < *n || wntvs && *ldvt < minmn) {
	*info = -11;
    }

/*     Compute workspace */
/*      (Note: Comments in the code beginning "Workspace:" describe the */
/*       minimal amount of workspace needed at that point in the code, */
/*       as well as the preferred amount for good performance. */
/*       NB refers to the optimal block size for the immediately */
/*       following subroutine, as returned by ILAENV.) */

    if (*info == 0) {
	minwrk = 1;
	maxwrk = 1;
	if (*m >= *n && minmn > 0) {

/*           Compute space needed for SBDSQR */

/* Writing concatenation */
	    i__1[0] = 1, a__1[0] = jobu;
	    i__1[1] = 1, a__1[1] = jobvt;
	    s_cat(ch__1, a__1, i__1, &c__2, (ftnlen)2);
	    mnthr = ilaenv_(&c__6, "SGESVD", ch__1, m, n, &c__0, &c__0, (
		    ftnlen)6, (ftnlen)2);
	    bdspac = *n * 5;
/*           Compute space needed for SGEQRF */
	    sgeqrf_(m, n, &a[a_offset], lda, dum, dum, &c_n1, &ierr);
	    lwork_sgeqrf__ = (integer) dum[0];
/*           Compute space needed for SORGQR */
	    sorgqr_(m, n, n, &a[a_offset], lda, dum, dum, &c_n1, &ierr);
	    lwork_sorgqr_n__ = (integer) dum[0];
	    sorgqr_(m, m, n, &a[a_offset], lda, dum, dum, &c_n1, &ierr);
	    lwork_sorgqr_m__ = (integer) dum[0];
/*           Compute space needed for SGEBRD */
	    sgebrd_(n, n, &a[a_offset], lda, &s[1], dum, dum, dum, dum, &c_n1,
		     &ierr);
	    lwork_sgebrd__ = (integer) dum[0];
/*           Compute space needed for SORGBR P */
	    sorgbr_("P", n, n, n, &a[a_offset], lda, dum, dum, &c_n1, &ierr);
	    lwork_sorgbr_p__ = (integer) dum[0];
/*           Compute space needed for SORGBR Q */
	    sorgbr_("Q", n, n, n, &a[a_offset], lda, dum, dum, &c_n1, &ierr);
	    lwork_sorgbr_q__ = (integer) dum[0];

	    if (*m >= mnthr) {
		if (wntun) {

/*                 Path 1 (M much larger than N, JOBU='N') */

		    maxwrk = *n + lwork_sgeqrf__;
/* Computing MAX */
		    i__2 = maxwrk, i__3 = *n * 3 + lwork_sgebrd__;
		    maxwrk = f2cmax(i__2,i__3);
		    if (wntvo || wntvas) {
/* Computing MAX */
			i__2 = maxwrk, i__3 = *n * 3 + lwork_sorgbr_p__;
			maxwrk = f2cmax(i__2,i__3);
		    }
		    maxwrk = f2cmax(maxwrk,bdspac);
/* Computing MAX */
		    i__2 = *n << 2;
		    minwrk = f2cmax(i__2,bdspac);
		} else if (wntuo && wntvn) {

/*                 Path 2 (M much larger than N, JOBU='O', JOBVT='N') */

		    wrkbl = *n + lwork_sgeqrf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n + lwork_sorgqr_n__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n * 3 + lwork_sgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n * 3 + lwork_sorgbr_q__;
		    wrkbl = f2cmax(i__2,i__3);
		    wrkbl = f2cmax(wrkbl,bdspac);
/* Computing MAX */
		    i__2 = *n * *n + wrkbl, i__3 = *n * *n + *m * *n + *n;
		    maxwrk = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = *n * 3 + *m;
		    minwrk = f2cmax(i__2,bdspac);
		} else if (wntuo && wntvas) {

/*                 Path 3 (M much larger than N, JOBU='O', JOBVT='S' or */
/*                 'A') */

		    wrkbl = *n + lwork_sgeqrf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n + lwork_sorgqr_n__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n * 3 + lwork_sgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n * 3 + lwork_sorgbr_q__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n * 3 + lwork_sorgbr_p__;
		    wrkbl = f2cmax(i__2,i__3);
		    wrkbl = f2cmax(wrkbl,bdspac);
/* Computing MAX */
		    i__2 = *n * *n + wrkbl, i__3 = *n * *n + *m * *n + *n;
		    maxwrk = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = *n * 3 + *m;
		    minwrk = f2cmax(i__2,bdspac);
		} else if (wntus && wntvn) {

/*                 Path 4 (M much larger than N, JOBU='S', JOBVT='N') */

		    wrkbl = *n + lwork_sgeqrf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n + lwork_sorgqr_n__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n * 3 + lwork_sgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n * 3 + lwork_sorgbr_q__;
		    wrkbl = f2cmax(i__2,i__3);
		    wrkbl = f2cmax(wrkbl,bdspac);
		    maxwrk = *n * *n + wrkbl;
/* Computing MAX */
		    i__2 = *n * 3 + *m;
		    minwrk = f2cmax(i__2,bdspac);
		} else if (wntus && wntvo) {

/*                 Path 5 (M much larger than N, JOBU='S', JOBVT='O') */

		    wrkbl = *n + lwork_sgeqrf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n + lwork_sorgqr_n__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n * 3 + lwork_sgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n * 3 + lwork_sorgbr_q__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n * 3 + lwork_sorgbr_p__;
		    wrkbl = f2cmax(i__2,i__3);
		    wrkbl = f2cmax(wrkbl,bdspac);
		    maxwrk = (*n << 1) * *n + wrkbl;
/* Computing MAX */
		    i__2 = *n * 3 + *m;
		    minwrk = f2cmax(i__2,bdspac);
		} else if (wntus && wntvas) {

/*                 Path 6 (M much larger than N, JOBU='S', JOBVT='S' or */
/*                 'A') */

		    wrkbl = *n + lwork_sgeqrf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n + lwork_sorgqr_n__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n * 3 + lwork_sgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n * 3 + lwork_sorgbr_q__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n * 3 + lwork_sorgbr_p__;
		    wrkbl = f2cmax(i__2,i__3);
		    wrkbl = f2cmax(wrkbl,bdspac);
		    maxwrk = *n * *n + wrkbl;
/* Computing MAX */
		    i__2 = *n * 3 + *m;
		    minwrk = f2cmax(i__2,bdspac);
		} else if (wntua && wntvn) {

/*                 Path 7 (M much larger than N, JOBU='A', JOBVT='N') */

		    wrkbl = *n + lwork_sgeqrf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n + lwork_sorgqr_m__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n * 3 + lwork_sgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n * 3 + lwork_sorgbr_q__;
		    wrkbl = f2cmax(i__2,i__3);
		    wrkbl = f2cmax(wrkbl,bdspac);
		    maxwrk = *n * *n + wrkbl;
/* Computing MAX */
		    i__2 = *n * 3 + *m;
		    minwrk = f2cmax(i__2,bdspac);
		} else if (wntua && wntvo) {

/*                 Path 8 (M much larger than N, JOBU='A', JOBVT='O') */

		    wrkbl = *n + lwork_sgeqrf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n + lwork_sorgqr_m__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n * 3 + lwork_sgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n * 3 + lwork_sorgbr_q__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n * 3 + lwork_sorgbr_p__;
		    wrkbl = f2cmax(i__2,i__3);
		    wrkbl = f2cmax(wrkbl,bdspac);
		    maxwrk = (*n << 1) * *n + wrkbl;
/* Computing MAX */
		    i__2 = *n * 3 + *m;
		    minwrk = f2cmax(i__2,bdspac);
		} else if (wntua && wntvas) {

/*                 Path 9 (M much larger than N, JOBU='A', JOBVT='S' or */
/*                 'A') */

		    wrkbl = *n + lwork_sgeqrf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n + lwork_sorgqr_m__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n * 3 + lwork_sgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n * 3 + lwork_sorgbr_q__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *n * 3 + lwork_sorgbr_p__;
		    wrkbl = f2cmax(i__2,i__3);
		    wrkbl = f2cmax(wrkbl,bdspac);
		    maxwrk = *n * *n + wrkbl;
/* Computing MAX */
		    i__2 = *n * 3 + *m;
		    minwrk = f2cmax(i__2,bdspac);
		}
	    } else {

/*              Path 10 (M at least N, but not much larger) */

		sgebrd_(m, n, &a[a_offset], lda, &s[1], dum, dum, dum, dum, &
			c_n1, &ierr);
		lwork_sgebrd__ = (integer) dum[0];
		maxwrk = *n * 3 + lwork_sgebrd__;
		if (wntus || wntuo) {
		    sorgbr_("Q", m, n, n, &a[a_offset], lda, dum, dum, &c_n1, 
			    &ierr);
		    lwork_sorgbr_q__ = (integer) dum[0];
/* Computing MAX */
		    i__2 = maxwrk, i__3 = *n * 3 + lwork_sorgbr_q__;
		    maxwrk = f2cmax(i__2,i__3);
		}
		if (wntua) {
		    sorgbr_("Q", m, m, n, &a[a_offset], lda, dum, dum, &c_n1, 
			    &ierr);
		    lwork_sorgbr_q__ = (integer) dum[0];
/* Computing MAX */
		    i__2 = maxwrk, i__3 = *n * 3 + lwork_sorgbr_q__;
		    maxwrk = f2cmax(i__2,i__3);
		}
		if (! wntvn) {
/* Computing MAX */
		    i__2 = maxwrk, i__3 = *n * 3 + lwork_sorgbr_p__;
		    maxwrk = f2cmax(i__2,i__3);
		}
		maxwrk = f2cmax(maxwrk,bdspac);
/* Computing MAX */
		i__2 = *n * 3 + *m;
		minwrk = f2cmax(i__2,bdspac);
	    }
	} else if (minmn > 0) {

/*           Compute space needed for SBDSQR */

/* Writing concatenation */
	    i__1[0] = 1, a__1[0] = jobu;
	    i__1[1] = 1, a__1[1] = jobvt;
	    s_cat(ch__1, a__1, i__1, &c__2, (ftnlen)2);
	    mnthr = ilaenv_(&c__6, "SGESVD", ch__1, m, n, &c__0, &c__0, (
		    ftnlen)6, (ftnlen)2);
	    bdspac = *m * 5;
/*           Compute space needed for SGELQF */
	    sgelqf_(m, n, &a[a_offset], lda, dum, dum, &c_n1, &ierr);
	    lwork_sgelqf__ = (integer) dum[0];
/*           Compute space needed for SORGLQ */
	    sorglq_(n, n, m, dum, n, dum, dum, &c_n1, &ierr);
	    lwork_sorglq_n__ = (integer) dum[0];
	    sorglq_(m, n, m, &a[a_offset], lda, dum, dum, &c_n1, &ierr);
	    lwork_sorglq_m__ = (integer) dum[0];
/*           Compute space needed for SGEBRD */
	    sgebrd_(m, m, &a[a_offset], lda, &s[1], dum, dum, dum, dum, &c_n1,
		     &ierr);
	    lwork_sgebrd__ = (integer) dum[0];
/*            Compute space needed for SORGBR P */
	    sorgbr_("P", m, m, m, &a[a_offset], n, dum, dum, &c_n1, &ierr);
	    lwork_sorgbr_p__ = (integer) dum[0];
/*           Compute space needed for SORGBR Q */
	    sorgbr_("Q", m, m, m, &a[a_offset], n, dum, dum, &c_n1, &ierr);
	    lwork_sorgbr_q__ = (integer) dum[0];
	    if (*n >= mnthr) {
		if (wntvn) {

/*                 Path 1t(N much larger than M, JOBVT='N') */

		    maxwrk = *m + lwork_sgelqf__;
/* Computing MAX */
		    i__2 = maxwrk, i__3 = *m * 3 + lwork_sgebrd__;
		    maxwrk = f2cmax(i__2,i__3);
		    if (wntuo || wntuas) {
/* Computing MAX */
			i__2 = maxwrk, i__3 = *m * 3 + lwork_sorgbr_q__;
			maxwrk = f2cmax(i__2,i__3);
		    }
		    maxwrk = f2cmax(maxwrk,bdspac);
/* Computing MAX */
		    i__2 = *m << 2;
		    minwrk = f2cmax(i__2,bdspac);
		} else if (wntvo && wntun) {

/*                 Path 2t(N much larger than M, JOBU='N', JOBVT='O') */

		    wrkbl = *m + lwork_sgelqf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m + lwork_sorglq_m__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m * 3 + lwork_sgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m * 3 + lwork_sorgbr_p__;
		    wrkbl = f2cmax(i__2,i__3);
		    wrkbl = f2cmax(wrkbl,bdspac);
/* Computing MAX */
		    i__2 = *m * *m + wrkbl, i__3 = *m * *m + *m * *n + *m;
		    maxwrk = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = *m * 3 + *n;
		    minwrk = f2cmax(i__2,bdspac);
		} else if (wntvo && wntuas) {

/*                 Path 3t(N much larger than M, JOBU='S' or 'A', */
/*                 JOBVT='O') */

		    wrkbl = *m + lwork_sgelqf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m + lwork_sorglq_m__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m * 3 + lwork_sgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m * 3 + lwork_sorgbr_p__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m * 3 + lwork_sorgbr_q__;
		    wrkbl = f2cmax(i__2,i__3);
		    wrkbl = f2cmax(wrkbl,bdspac);
/* Computing MAX */
		    i__2 = *m * *m + wrkbl, i__3 = *m * *m + *m * *n + *m;
		    maxwrk = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = *m * 3 + *n;
		    minwrk = f2cmax(i__2,bdspac);
		} else if (wntvs && wntun) {

/*                 Path 4t(N much larger than M, JOBU='N', JOBVT='S') */

		    wrkbl = *m + lwork_sgelqf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m + lwork_sorglq_m__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m * 3 + lwork_sgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m * 3 + lwork_sorgbr_p__;
		    wrkbl = f2cmax(i__2,i__3);
		    wrkbl = f2cmax(wrkbl,bdspac);
		    maxwrk = *m * *m + wrkbl;
/* Computing MAX */
		    i__2 = *m * 3 + *n;
		    minwrk = f2cmax(i__2,bdspac);
		} else if (wntvs && wntuo) {

/*                 Path 5t(N much larger than M, JOBU='O', JOBVT='S') */

		    wrkbl = *m + lwork_sgelqf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m + lwork_sorglq_m__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m * 3 + lwork_sgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m * 3 + lwork_sorgbr_p__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m * 3 + lwork_sorgbr_q__;
		    wrkbl = f2cmax(i__2,i__3);
		    wrkbl = f2cmax(wrkbl,bdspac);
		    maxwrk = (*m << 1) * *m + wrkbl;
/* Computing MAX */
		    i__2 = *m * 3 + *n;
		    minwrk = f2cmax(i__2,bdspac);
		    maxwrk = f2cmax(maxwrk,minwrk);
		} else if (wntvs && wntuas) {

/*                 Path 6t(N much larger than M, JOBU='S' or 'A', */
/*                 JOBVT='S') */

		    wrkbl = *m + lwork_sgelqf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m + lwork_sorglq_m__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m * 3 + lwork_sgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m * 3 + lwork_sorgbr_p__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m * 3 + lwork_sorgbr_q__;
		    wrkbl = f2cmax(i__2,i__3);
		    wrkbl = f2cmax(wrkbl,bdspac);
		    maxwrk = *m * *m + wrkbl;
/* Computing MAX */
		    i__2 = *m * 3 + *n;
		    minwrk = f2cmax(i__2,bdspac);
		} else if (wntva && wntun) {

/*                 Path 7t(N much larger than M, JOBU='N', JOBVT='A') */

		    wrkbl = *m + lwork_sgelqf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m + lwork_sorglq_n__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m * 3 + lwork_sgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m * 3 + lwork_sorgbr_p__;
		    wrkbl = f2cmax(i__2,i__3);
		    wrkbl = f2cmax(wrkbl,bdspac);
		    maxwrk = *m * *m + wrkbl;
/* Computing MAX */
		    i__2 = *m * 3 + *n;
		    minwrk = f2cmax(i__2,bdspac);
		} else if (wntva && wntuo) {

/*                 Path 8t(N much larger than M, JOBU='O', JOBVT='A') */

		    wrkbl = *m + lwork_sgelqf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m + lwork_sorglq_n__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m * 3 + lwork_sgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m * 3 + lwork_sorgbr_p__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m * 3 + lwork_sorgbr_q__;
		    wrkbl = f2cmax(i__2,i__3);
		    wrkbl = f2cmax(wrkbl,bdspac);
		    maxwrk = (*m << 1) * *m + wrkbl;
/* Computing MAX */
		    i__2 = *m * 3 + *n;
		    minwrk = f2cmax(i__2,bdspac);
		} else if (wntva && wntuas) {

/*                 Path 9t(N much larger than M, JOBU='S' or 'A', */
/*                 JOBVT='A') */

		    wrkbl = *m + lwork_sgelqf__;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m + lwork_sorglq_n__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m * 3 + lwork_sgebrd__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m * 3 + lwork_sorgbr_p__;
		    wrkbl = f2cmax(i__2,i__3);
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *m * 3 + lwork_sorgbr_q__;
		    wrkbl = f2cmax(i__2,i__3);
		    wrkbl = f2cmax(wrkbl,bdspac);
		    maxwrk = *m * *m + wrkbl;
/* Computing MAX */
		    i__2 = *m * 3 + *n;
		    minwrk = f2cmax(i__2,bdspac);
		}
	    } else {

/*              Path 10t(N greater than M, but not much larger) */

		sgebrd_(m, n, &a[a_offset], lda, &s[1], dum, dum, dum, dum, &
			c_n1, &ierr);
		lwork_sgebrd__ = (integer) dum[0];
		maxwrk = *m * 3 + lwork_sgebrd__;
		if (wntvs || wntvo) {
/*                Compute space needed for SORGBR P */
		    sorgbr_("P", m, n, m, &a[a_offset], n, dum, dum, &c_n1, &
			    ierr);
		    lwork_sorgbr_p__ = (integer) dum[0];
/* Computing MAX */
		    i__2 = maxwrk, i__3 = *m * 3 + lwork_sorgbr_p__;
		    maxwrk = f2cmax(i__2,i__3);
		}
		if (wntva) {
		    sorgbr_("P", n, n, m, &a[a_offset], n, dum, dum, &c_n1, &
			    ierr);
		    lwork_sorgbr_p__ = (integer) dum[0];
/* Computing MAX */
		    i__2 = maxwrk, i__3 = *m * 3 + lwork_sorgbr_p__;
		    maxwrk = f2cmax(i__2,i__3);
		}
		if (! wntun) {
/* Computing MAX */
		    i__2 = maxwrk, i__3 = *m * 3 + lwork_sorgbr_q__;
		    maxwrk = f2cmax(i__2,i__3);
		}
		maxwrk = f2cmax(maxwrk,bdspac);
/* Computing MAX */
		i__2 = *m * 3 + *n;
		minwrk = f2cmax(i__2,bdspac);
	    }
	}
	maxwrk = f2cmax(maxwrk,minwrk);
	work[1] = (real) maxwrk;

	if (*lwork < minwrk && ! lquery) {
	    *info = -13;
	}
    }

    if (*info != 0) {
	i__2 = -(*info);
	xerbla_("SGESVD", &i__2, (ftnlen)6);
	return;
    } else if (lquery) {
	return;
    }

/*     Quick return if possible */

    if (*m == 0 || *n == 0) {
	return;
    }

/*     Get machine constants */

    eps = slamch_("P");
    smlnum = sqrt(slamch_("S")) / eps;
    bignum = 1.f / smlnum;

/*     Scale A if f2cmax element outside range [SMLNUM,BIGNUM] */

    anrm = slange_("M", m, n, &a[a_offset], lda, dum);
    iscl = 0;
    if (anrm > 0.f && anrm < smlnum) {
	iscl = 1;
	slascl_("G", &c__0, &c__0, &anrm, &smlnum, m, n, &a[a_offset], lda, &
		ierr);
    } else if (anrm > bignum) {
	iscl = 1;
	slascl_("G", &c__0, &c__0, &anrm, &bignum, m, n, &a[a_offset], lda, &
		ierr);
    }

    if (*m >= *n) {

/*        A has at least as many rows as columns. If A has sufficiently */
/*        more rows than columns, first reduce using the QR */
/*        decomposition (if sufficient workspace available) */

	if (*m >= mnthr) {

	    if (wntun) {

/*              Path 1 (M much larger than N, JOBU='N') */
/*              No left singular vectors to be computed */

		itau = 1;
		iwork = itau + *n;

/*              Compute A=Q*R */
/*              (Workspace: need 2*N, prefer N+N*NB) */

		i__2 = *lwork - iwork + 1;
		sgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[iwork], &
			i__2, &ierr);

/*              Zero out below R */

		if (*n > 1) {
		    i__2 = *n - 1;
		    i__3 = *n - 1;
		    slaset_("L", &i__2, &i__3, &c_b57, &c_b57, &a[a_dim1 + 2],
			     lda);
		}
		ie = 1;
		itauq = ie + *n;
		itaup = itauq + *n;
		iwork = itaup + *n;

/*              Bidiagonalize R in A */
/*              (Workspace: need 4*N, prefer 3*N+2*N*NB) */

		i__2 = *lwork - iwork + 1;
		sgebrd_(n, n, &a[a_offset], lda, &s[1], &work[ie], &work[
			itauq], &work[itaup], &work[iwork], &i__2, &ierr);
		ncvt = 0;
		if (wntvo || wntvas) {

/*                 If right singular vectors desired, generate P'. */
/*                 (Workspace: need 4*N-1, prefer 3*N+(N-1)*NB) */

		    i__2 = *lwork - iwork + 1;
		    sorgbr_("P", n, n, n, &a[a_offset], lda, &work[itaup], &
			    work[iwork], &i__2, &ierr);
		    ncvt = *n;
		}
		iwork = ie + *n;

/*              Perform bidiagonal QR iteration, computing right */
/*              singular vectors of A in A if desired */
/*              (Workspace: need BDSPAC) */

		sbdsqr_("U", n, &ncvt, &c__0, &c__0, &s[1], &work[ie], &a[
			a_offset], lda, dum, &c__1, dum, &c__1, &work[iwork], 
			info);

/*              If right singular vectors desired in VT, copy them there */

		if (wntvas) {
		    slacpy_("F", n, n, &a[a_offset], lda, &vt[vt_offset], 
			    ldvt);
		}

	    } else if (wntuo && wntvn) {

/*              Path 2 (M much larger than N, JOBU='O', JOBVT='N') */
/*              N left singular vectors to be overwritten on A and */
/*              no right singular vectors to be computed */

/* Computing MAX */
		i__2 = *n << 2;
		if (*lwork >= *n * *n + f2cmax(i__2,bdspac)) {

/*                 Sufficient workspace for a fast algorithm */

		    ir = 1;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *lda * *n + *n;
		    if (*lwork >= f2cmax(i__2,i__3) + *lda * *n) {

/*                    WORK(IU) is LDA by N, WORK(IR) is LDA by N */

			ldwrku = *lda;
			ldwrkr = *lda;
		    } else /* if(complicated condition) */ {
/* Computing MAX */
			i__2 = wrkbl, i__3 = *lda * *n + *n;
			if (*lwork >= f2cmax(i__2,i__3) + *n * *n) {

/*                    WORK(IU) is LDA by N, WORK(IR) is N by N */

			    ldwrku = *lda;
			    ldwrkr = *n;
			} else {

/*                    WORK(IU) is LDWRKU by N, WORK(IR) is N by N */

			    ldwrku = (*lwork - *n * *n - *n) / *n;
			    ldwrkr = *n;
			}
		    }
		    itau = ir + ldwrkr * *n;
		    iwork = itau + *n;

/*                 Compute A=Q*R */
/*                 (Workspace: need N*N+2*N, prefer N*N+N+N*NB) */

		    i__2 = *lwork - iwork + 1;
		    sgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[iwork]
			    , &i__2, &ierr);

/*                 Copy R to WORK(IR) and zero out below it */

		    slacpy_("U", n, n, &a[a_offset], lda, &work[ir], &ldwrkr);
		    i__2 = *n - 1;
		    i__3 = *n - 1;
		    slaset_("L", &i__2, &i__3, &c_b57, &c_b57, &work[ir + 1], 
			    &ldwrkr);

/*                 Generate Q in A */
/*                 (Workspace: need N*N+2*N, prefer N*N+N+N*NB) */

		    i__2 = *lwork - iwork + 1;
		    sorgqr_(m, n, n, &a[a_offset], lda, &work[itau], &work[
			    iwork], &i__2, &ierr);
		    ie = itau;
		    itauq = ie + *n;
		    itaup = itauq + *n;
		    iwork = itaup + *n;

/*                 Bidiagonalize R in WORK(IR) */
/*                 (Workspace: need N*N+4*N, prefer N*N+3*N+2*N*NB) */

		    i__2 = *lwork - iwork + 1;
		    sgebrd_(n, n, &work[ir], &ldwrkr, &s[1], &work[ie], &work[
			    itauq], &work[itaup], &work[iwork], &i__2, &ierr);

/*                 Generate left vectors bidiagonalizing R */
/*                 (Workspace: need N*N+4*N, prefer N*N+3*N+N*NB) */

		    i__2 = *lwork - iwork + 1;
		    sorgbr_("Q", n, n, n, &work[ir], &ldwrkr, &work[itauq], &
			    work[iwork], &i__2, &ierr);
		    iwork = ie + *n;

/*                 Perform bidiagonal QR iteration, computing left */
/*                 singular vectors of R in WORK(IR) */
/*                 (Workspace: need N*N+BDSPAC) */

		    sbdsqr_("U", n, &c__0, n, &c__0, &s[1], &work[ie], dum, &
			    c__1, &work[ir], &ldwrkr, dum, &c__1, &work[iwork]
			    , info);
		    iu = ie + *n;

/*                 Multiply Q in A by left singular vectors of R in */
/*                 WORK(IR), storing result in WORK(IU) and copying to A */
/*                 (Workspace: need N*N+2*N, prefer N*N+M*N+N) */

		    i__2 = *m;
		    i__3 = ldwrku;
		    for (i__ = 1; i__3 < 0 ? i__ >= i__2 : i__ <= i__2; i__ +=
			     i__3) {
/* Computing MIN */
			i__4 = *m - i__ + 1;
			chunk = f2cmin(i__4,ldwrku);
			sgemm_("N", "N", &chunk, n, n, &c_b79, &a[i__ + 
				a_dim1], lda, &work[ir], &ldwrkr, &c_b57, &
				work[iu], &ldwrku);
			slacpy_("F", &chunk, n, &work[iu], &ldwrku, &a[i__ + 
				a_dim1], lda);
/* L10: */
		    }

		} else {

/*                 Insufficient workspace for a fast algorithm */

		    ie = 1;
		    itauq = ie + *n;
		    itaup = itauq + *n;
		    iwork = itaup + *n;

/*                 Bidiagonalize A */
/*                 (Workspace: need 3*N+M, prefer 3*N+(M+N)*NB) */

		    i__3 = *lwork - iwork + 1;
		    sgebrd_(m, n, &a[a_offset], lda, &s[1], &work[ie], &work[
			    itauq], &work[itaup], &work[iwork], &i__3, &ierr);

/*                 Generate left vectors bidiagonalizing A */
/*                 (Workspace: need 4*N, prefer 3*N+N*NB) */

		    i__3 = *lwork - iwork + 1;
		    sorgbr_("Q", m, n, n, &a[a_offset], lda, &work[itauq], &
			    work[iwork], &i__3, &ierr);
		    iwork = ie + *n;

/*                 Perform bidiagonal QR iteration, computing left */
/*                 singular vectors of A in A */
/*                 (Workspace: need BDSPAC) */

		    sbdsqr_("U", n, &c__0, m, &c__0, &s[1], &work[ie], dum, &
			    c__1, &a[a_offset], lda, dum, &c__1, &work[iwork],
			     info);

		}

	    } else if (wntuo && wntvas) {

/*              Path 3 (M much larger than N, JOBU='O', JOBVT='S' or 'A') */
/*              N left singular vectors to be overwritten on A and */
/*              N right singular vectors to be computed in VT */

/* Computing MAX */
		i__3 = *n << 2;
		if (*lwork >= *n * *n + f2cmax(i__3,bdspac)) {

/*                 Sufficient workspace for a fast algorithm */

		    ir = 1;
/* Computing MAX */
		    i__3 = wrkbl, i__2 = *lda * *n + *n;
		    if (*lwork >= f2cmax(i__3,i__2) + *lda * *n) {

/*                    WORK(IU) is LDA by N and WORK(IR) is LDA by N */

			ldwrku = *lda;
			ldwrkr = *lda;
		    } else /* if(complicated condition) */ {
/* Computing MAX */
			i__3 = wrkbl, i__2 = *lda * *n + *n;
			if (*lwork >= f2cmax(i__3,i__2) + *n * *n) {

/*                    WORK(IU) is LDA by N and WORK(IR) is N by N */

			    ldwrku = *lda;
			    ldwrkr = *n;
			} else {

/*                    WORK(IU) is LDWRKU by N and WORK(IR) is N by N */

			    ldwrku = (*lwork - *n * *n - *n) / *n;
			    ldwrkr = *n;
			}
		    }
		    itau = ir + ldwrkr * *n;
		    iwork = itau + *n;

/*                 Compute A=Q*R */
/*                 (Workspace: need N*N+2*N, prefer N*N+N+N*NB) */

		    i__3 = *lwork - iwork + 1;
		    sgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[iwork]
			    , &i__3, &ierr);

/*                 Copy R to VT, zeroing out below it */

		    slacpy_("U", n, n, &a[a_offset], lda, &vt[vt_offset], 
			    ldvt);
		    if (*n > 1) {
			i__3 = *n - 1;
			i__2 = *n - 1;
			slaset_("L", &i__3, &i__2, &c_b57, &c_b57, &vt[
				vt_dim1 + 2], ldvt);
		    }

/*                 Generate Q in A */
/*                 (Workspace: need N*N+2*N, prefer N*N+N+N*NB) */

		    i__3 = *lwork - iwork + 1;
		    sorgqr_(m, n, n, &a[a_offset], lda, &work[itau], &work[
			    iwork], &i__3, &ierr);
		    ie = itau;
		    itauq = ie + *n;
		    itaup = itauq + *n;
		    iwork = itaup + *n;

/*                 Bidiagonalize R in VT, copying result to WORK(IR) */
/*                 (Workspace: need N*N+4*N, prefer N*N+3*N+2*N*NB) */

		    i__3 = *lwork - iwork + 1;
		    sgebrd_(n, n, &vt[vt_offset], ldvt, &s[1], &work[ie], &
			    work[itauq], &work[itaup], &work[iwork], &i__3, &
			    ierr);
		    slacpy_("L", n, n, &vt[vt_offset], ldvt, &work[ir], &
			    ldwrkr);

/*                 Generate left vectors bidiagonalizing R in WORK(IR) */
/*                 (Workspace: need N*N+4*N, prefer N*N+3*N+N*NB) */

		    i__3 = *lwork - iwork + 1;
		    sorgbr_("Q", n, n, n, &work[ir], &ldwrkr, &work[itauq], &
			    work[iwork], &i__3, &ierr);

/*                 Generate right vectors bidiagonalizing R in VT */
/*                 (Workspace: need N*N+4*N-1, prefer N*N+3*N+(N-1)*NB) */

		    i__3 = *lwork - iwork + 1;
		    sorgbr_("P", n, n, n, &vt[vt_offset], ldvt, &work[itaup], 
			    &work[iwork], &i__3, &ierr);
		    iwork = ie + *n;

/*                 Perform bidiagonal QR iteration, computing left */
/*                 singular vectors of R in WORK(IR) and computing right */
/*                 singular vectors of R in VT */
/*                 (Workspace: need N*N+BDSPAC) */

		    sbdsqr_("U", n, n, n, &c__0, &s[1], &work[ie], &vt[
			    vt_offset], ldvt, &work[ir], &ldwrkr, dum, &c__1, 
			    &work[iwork], info);
		    iu = ie + *n;

/*                 Multiply Q in A by left singular vectors of R in */
/*                 WORK(IR), storing result in WORK(IU) and copying to A */
/*                 (Workspace: need N*N+2*N, prefer N*N+M*N+N) */

		    i__3 = *m;
		    i__2 = ldwrku;
		    for (i__ = 1; i__2 < 0 ? i__ >= i__3 : i__ <= i__3; i__ +=
			     i__2) {
/* Computing MIN */
			i__4 = *m - i__ + 1;
			chunk = f2cmin(i__4,ldwrku);
			sgemm_("N", "N", &chunk, n, n, &c_b79, &a[i__ + 
				a_dim1], lda, &work[ir], &ldwrkr, &c_b57, &
				work[iu], &ldwrku);
			slacpy_("F", &chunk, n, &work[iu], &ldwrku, &a[i__ + 
				a_dim1], lda);
/* L20: */
		    }

		} else {

/*                 Insufficient workspace for a fast algorithm */

		    itau = 1;
		    iwork = itau + *n;

/*                 Compute A=Q*R */
/*                 (Workspace: need 2*N, prefer N+N*NB) */

		    i__2 = *lwork - iwork + 1;
		    sgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[iwork]
			    , &i__2, &ierr);

/*                 Copy R to VT, zeroing out below it */

		    slacpy_("U", n, n, &a[a_offset], lda, &vt[vt_offset], 
			    ldvt);
		    if (*n > 1) {
			i__2 = *n - 1;
			i__3 = *n - 1;
			slaset_("L", &i__2, &i__3, &c_b57, &c_b57, &vt[
				vt_dim1 + 2], ldvt);
		    }

/*                 Generate Q in A */
/*                 (Workspace: need 2*N, prefer N+N*NB) */

		    i__2 = *lwork - iwork + 1;
		    sorgqr_(m, n, n, &a[a_offset], lda, &work[itau], &work[
			    iwork], &i__2, &ierr);
		    ie = itau;
		    itauq = ie + *n;
		    itaup = itauq + *n;
		    iwork = itaup + *n;

/*                 Bidiagonalize R in VT */
/*                 (Workspace: need 4*N, prefer 3*N+2*N*NB) */

		    i__2 = *lwork - iwork + 1;
		    sgebrd_(n, n, &vt[vt_offset], ldvt, &s[1], &work[ie], &
			    work[itauq], &work[itaup], &work[iwork], &i__2, &
			    ierr);

/*                 Multiply Q in A by left vectors bidiagonalizing R */
/*                 (Workspace: need 3*N+M, prefer 3*N+M*NB) */

		    i__2 = *lwork - iwork + 1;
		    sormbr_("Q", "R", "N", m, n, n, &vt[vt_offset], ldvt, &
			    work[itauq], &a[a_offset], lda, &work[iwork], &
			    i__2, &ierr);

/*                 Generate right vectors bidiagonalizing R in VT */
/*                 (Workspace: need 4*N-1, prefer 3*N+(N-1)*NB) */

		    i__2 = *lwork - iwork + 1;
		    sorgbr_("P", n, n, n, &vt[vt_offset], ldvt, &work[itaup], 
			    &work[iwork], &i__2, &ierr);
		    iwork = ie + *n;

/*                 Perform bidiagonal QR iteration, computing left */
/*                 singular vectors of A in A and computing right */
/*                 singular vectors of A in VT */
/*                 (Workspace: need BDSPAC) */

		    sbdsqr_("U", n, n, m, &c__0, &s[1], &work[ie], &vt[
			    vt_offset], ldvt, &a[a_offset], lda, dum, &c__1, &
			    work[iwork], info);

		}

	    } else if (wntus) {

		if (wntvn) {

/*                 Path 4 (M much larger than N, JOBU='S', JOBVT='N') */
/*                 N left singular vectors to be computed in U and */
/*                 no right singular vectors to be computed */

/* Computing MAX */
		    i__2 = *n << 2;
		    if (*lwork >= *n * *n + f2cmax(i__2,bdspac)) {

/*                    Sufficient workspace for a fast algorithm */

			ir = 1;
			if (*lwork >= wrkbl + *lda * *n) {

/*                       WORK(IR) is LDA by N */

			    ldwrkr = *lda;
			} else {

/*                       WORK(IR) is N by N */

			    ldwrkr = *n;
			}
			itau = ir + ldwrkr * *n;
			iwork = itau + *n;

/*                    Compute A=Q*R */
/*                    (Workspace: need N*N+2*N, prefer N*N+N+N*NB) */

			i__2 = *lwork - iwork + 1;
			sgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);

/*                    Copy R to WORK(IR), zeroing out below it */

			slacpy_("U", n, n, &a[a_offset], lda, &work[ir], &
				ldwrkr);
			i__2 = *n - 1;
			i__3 = *n - 1;
			slaset_("L", &i__2, &i__3, &c_b57, &c_b57, &work[ir + 
				1], &ldwrkr);

/*                    Generate Q in A */
/*                    (Workspace: need N*N+2*N, prefer N*N+N+N*NB) */

			i__2 = *lwork - iwork + 1;
			sorgqr_(m, n, n, &a[a_offset], lda, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = itau;
			itauq = ie + *n;
			itaup = itauq + *n;
			iwork = itaup + *n;

/*                    Bidiagonalize R in WORK(IR) */
/*                    (Workspace: need N*N+4*N, prefer N*N+3*N+2*N*NB) */

			i__2 = *lwork - iwork + 1;
			sgebrd_(n, n, &work[ir], &ldwrkr, &s[1], &work[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Generate left vectors bidiagonalizing R in WORK(IR) */
/*                    (Workspace: need N*N+4*N, prefer N*N+3*N+N*NB) */

			i__2 = *lwork - iwork + 1;
			sorgbr_("Q", n, n, n, &work[ir], &ldwrkr, &work[itauq]
				, &work[iwork], &i__2, &ierr);
			iwork = ie + *n;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of R in WORK(IR) */
/*                    (Workspace: need N*N+BDSPAC) */

			sbdsqr_("U", n, &c__0, n, &c__0, &s[1], &work[ie], 
				dum, &c__1, &work[ir], &ldwrkr, dum, &c__1, &
				work[iwork], info);

/*                    Multiply Q in A by left singular vectors of R in */
/*                    WORK(IR), storing result in U */
/*                    (Workspace: need N*N) */

			sgemm_("N", "N", m, n, n, &c_b79, &a[a_offset], lda, &
				work[ir], &ldwrkr, &c_b57, &u[u_offset], ldu);

		    } else {

/*                    Insufficient workspace for a fast algorithm */

			itau = 1;
			iwork = itau + *n;

/*                    Compute A=Q*R, copying result to U */
/*                    (Workspace: need 2*N, prefer N+N*NB) */

			i__2 = *lwork - iwork + 1;
			sgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			slacpy_("L", m, n, &a[a_offset], lda, &u[u_offset], 
				ldu);

/*                    Generate Q in U */
/*                    (Workspace: need 2*N, prefer N+N*NB) */

			i__2 = *lwork - iwork + 1;
			sorgqr_(m, n, n, &u[u_offset], ldu, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = itau;
			itauq = ie + *n;
			itaup = itauq + *n;
			iwork = itaup + *n;

/*                    Zero out below R in A */

			if (*n > 1) {
			    i__2 = *n - 1;
			    i__3 = *n - 1;
			    slaset_("L", &i__2, &i__3, &c_b57, &c_b57, &a[
				    a_dim1 + 2], lda);
			}

/*                    Bidiagonalize R in A */
/*                    (Workspace: need 4*N, prefer 3*N+2*N*NB) */

			i__2 = *lwork - iwork + 1;
			sgebrd_(n, n, &a[a_offset], lda, &s[1], &work[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Multiply Q in U by left vectors bidiagonalizing R */
/*                    (Workspace: need 3*N+M, prefer 3*N+M*NB) */

			i__2 = *lwork - iwork + 1;
			sormbr_("Q", "R", "N", m, n, n, &a[a_offset], lda, &
				work[itauq], &u[u_offset], ldu, &work[iwork], 
				&i__2, &ierr)
				;
			iwork = ie + *n;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of A in U */
/*                    (Workspace: need BDSPAC) */

			sbdsqr_("U", n, &c__0, m, &c__0, &s[1], &work[ie], 
				dum, &c__1, &u[u_offset], ldu, dum, &c__1, &
				work[iwork], info);

		    }

		} else if (wntvo) {

/*                 Path 5 (M much larger than N, JOBU='S', JOBVT='O') */
/*                 N left singular vectors to be computed in U and */
/*                 N right singular vectors to be overwritten on A */

/* Computing MAX */
		    i__2 = *n << 2;
		    if (*lwork >= (*n << 1) * *n + f2cmax(i__2,bdspac)) {

/*                    Sufficient workspace for a fast algorithm */

			iu = 1;
			if (*lwork >= wrkbl + (*lda << 1) * *n) {

/*                       WORK(IU) is LDA by N and WORK(IR) is LDA by N */

			    ldwrku = *lda;
			    ir = iu + ldwrku * *n;
			    ldwrkr = *lda;
			} else if (*lwork >= wrkbl + (*lda + *n) * *n) {

/*                       WORK(IU) is LDA by N and WORK(IR) is N by N */

			    ldwrku = *lda;
			    ir = iu + ldwrku * *n;
			    ldwrkr = *n;
			} else {

/*                       WORK(IU) is N by N and WORK(IR) is N by N */

			    ldwrku = *n;
			    ir = iu + ldwrku * *n;
			    ldwrkr = *n;
			}
			itau = ir + ldwrkr * *n;
			iwork = itau + *n;

/*                    Compute A=Q*R */
/*                    (Workspace: need 2*N*N+2*N, prefer 2*N*N+N+N*NB) */

			i__2 = *lwork - iwork + 1;
			sgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);

/*                    Copy R to WORK(IU), zeroing out below it */

			slacpy_("U", n, n, &a[a_offset], lda, &work[iu], &
				ldwrku);
			i__2 = *n - 1;
			i__3 = *n - 1;
			slaset_("L", &i__2, &i__3, &c_b57, &c_b57, &work[iu + 
				1], &ldwrku);

/*                    Generate Q in A */
/*                    (Workspace: need 2*N*N+2*N, prefer 2*N*N+N+N*NB) */

			i__2 = *lwork - iwork + 1;
			sorgqr_(m, n, n, &a[a_offset], lda, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = itau;
			itauq = ie + *n;
			itaup = itauq + *n;
			iwork = itaup + *n;

/*                    Bidiagonalize R in WORK(IU), copying result to */
/*                    WORK(IR) */
/*                    (Workspace: need 2*N*N+4*N, */
/*                                prefer 2*N*N+3*N+2*N*NB) */

			i__2 = *lwork - iwork + 1;
			sgebrd_(n, n, &work[iu], &ldwrku, &s[1], &work[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);
			slacpy_("U", n, n, &work[iu], &ldwrku, &work[ir], &
				ldwrkr);

/*                    Generate left bidiagonalizing vectors in WORK(IU) */
/*                    (Workspace: need 2*N*N+4*N, prefer 2*N*N+3*N+N*NB) */

			i__2 = *lwork - iwork + 1;
			sorgbr_("Q", n, n, n, &work[iu], &ldwrku, &work[itauq]
				, &work[iwork], &i__2, &ierr);

/*                    Generate right bidiagonalizing vectors in WORK(IR) */
/*                    (Workspace: need 2*N*N+4*N-1, */
/*                                prefer 2*N*N+3*N+(N-1)*NB) */

			i__2 = *lwork - iwork + 1;
			sorgbr_("P", n, n, n, &work[ir], &ldwrkr, &work[itaup]
				, &work[iwork], &i__2, &ierr);
			iwork = ie + *n;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of R in WORK(IU) and computing */
/*                    right singular vectors of R in WORK(IR) */
/*                    (Workspace: need 2*N*N+BDSPAC) */

			sbdsqr_("U", n, n, n, &c__0, &s[1], &work[ie], &work[
				ir], &ldwrkr, &work[iu], &ldwrku, dum, &c__1, 
				&work[iwork], info);

/*                    Multiply Q in A by left singular vectors of R in */
/*                    WORK(IU), storing result in U */
/*                    (Workspace: need N*N) */

			sgemm_("N", "N", m, n, n, &c_b79, &a[a_offset], lda, &
				work[iu], &ldwrku, &c_b57, &u[u_offset], ldu);

/*                    Copy right singular vectors of R to A */
/*                    (Workspace: need N*N) */

			slacpy_("F", n, n, &work[ir], &ldwrkr, &a[a_offset], 
				lda);

		    } else {

/*                    Insufficient workspace for a fast algorithm */

			itau = 1;
			iwork = itau + *n;

/*                    Compute A=Q*R, copying result to U */
/*                    (Workspace: need 2*N, prefer N+N*NB) */

			i__2 = *lwork - iwork + 1;
			sgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			slacpy_("L", m, n, &a[a_offset], lda, &u[u_offset], 
				ldu);

/*                    Generate Q in U */
/*                    (Workspace: need 2*N, prefer N+N*NB) */

			i__2 = *lwork - iwork + 1;
			sorgqr_(m, n, n, &u[u_offset], ldu, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = itau;
			itauq = ie + *n;
			itaup = itauq + *n;
			iwork = itaup + *n;

/*                    Zero out below R in A */

			if (*n > 1) {
			    i__2 = *n - 1;
			    i__3 = *n - 1;
			    slaset_("L", &i__2, &i__3, &c_b57, &c_b57, &a[
				    a_dim1 + 2], lda);
			}

/*                    Bidiagonalize R in A */
/*                    (Workspace: need 4*N, prefer 3*N+2*N*NB) */

			i__2 = *lwork - iwork + 1;
			sgebrd_(n, n, &a[a_offset], lda, &s[1], &work[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Multiply Q in U by left vectors bidiagonalizing R */
/*                    (Workspace: need 3*N+M, prefer 3*N+M*NB) */

			i__2 = *lwork - iwork + 1;
			sormbr_("Q", "R", "N", m, n, n, &a[a_offset], lda, &
				work[itauq], &u[u_offset], ldu, &work[iwork], 
				&i__2, &ierr)
				;

/*                    Generate right vectors bidiagonalizing R in A */
/*                    (Workspace: need 4*N-1, prefer 3*N+(N-1)*NB) */

			i__2 = *lwork - iwork + 1;
			sorgbr_("P", n, n, n, &a[a_offset], lda, &work[itaup],
				 &work[iwork], &i__2, &ierr);
			iwork = ie + *n;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of A in U and computing right */
/*                    singular vectors of A in A */
/*                    (Workspace: need BDSPAC) */

			sbdsqr_("U", n, n, m, &c__0, &s[1], &work[ie], &a[
				a_offset], lda, &u[u_offset], ldu, dum, &c__1,
				 &work[iwork], info);

		    }

		} else if (wntvas) {

/*                 Path 6 (M much larger than N, JOBU='S', JOBVT='S' */
/*                         or 'A') */
/*                 N left singular vectors to be computed in U and */
/*                 N right singular vectors to be computed in VT */

/* Computing MAX */
		    i__2 = *n << 2;
		    if (*lwork >= *n * *n + f2cmax(i__2,bdspac)) {

/*                    Sufficient workspace for a fast algorithm */

			iu = 1;
			if (*lwork >= wrkbl + *lda * *n) {

/*                       WORK(IU) is LDA by N */

			    ldwrku = *lda;
			} else {

/*                       WORK(IU) is N by N */

			    ldwrku = *n;
			}
			itau = iu + ldwrku * *n;
			iwork = itau + *n;

/*                    Compute A=Q*R */
/*                    (Workspace: need N*N+2*N, prefer N*N+N+N*NB) */

			i__2 = *lwork - iwork + 1;
			sgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);

/*                    Copy R to WORK(IU), zeroing out below it */

			slacpy_("U", n, n, &a[a_offset], lda, &work[iu], &
				ldwrku);
			i__2 = *n - 1;
			i__3 = *n - 1;
			slaset_("L", &i__2, &i__3, &c_b57, &c_b57, &work[iu + 
				1], &ldwrku);

/*                    Generate Q in A */
/*                    (Workspace: need N*N+2*N, prefer N*N+N+N*NB) */

			i__2 = *lwork - iwork + 1;
			sorgqr_(m, n, n, &a[a_offset], lda, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = itau;
			itauq = ie + *n;
			itaup = itauq + *n;
			iwork = itaup + *n;

/*                    Bidiagonalize R in WORK(IU), copying result to VT */
/*                    (Workspace: need N*N+4*N, prefer N*N+3*N+2*N*NB) */

			i__2 = *lwork - iwork + 1;
			sgebrd_(n, n, &work[iu], &ldwrku, &s[1], &work[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);
			slacpy_("U", n, n, &work[iu], &ldwrku, &vt[vt_offset],
				 ldvt);

/*                    Generate left bidiagonalizing vectors in WORK(IU) */
/*                    (Workspace: need N*N+4*N, prefer N*N+3*N+N*NB) */

			i__2 = *lwork - iwork + 1;
			sorgbr_("Q", n, n, n, &work[iu], &ldwrku, &work[itauq]
				, &work[iwork], &i__2, &ierr);

/*                    Generate right bidiagonalizing vectors in VT */
/*                    (Workspace: need N*N+4*N-1, */
/*                                prefer N*N+3*N+(N-1)*NB) */

			i__2 = *lwork - iwork + 1;
			sorgbr_("P", n, n, n, &vt[vt_offset], ldvt, &work[
				itaup], &work[iwork], &i__2, &ierr)
				;
			iwork = ie + *n;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of R in WORK(IU) and computing */
/*                    right singular vectors of R in VT */
/*                    (Workspace: need N*N+BDSPAC) */

			sbdsqr_("U", n, n, n, &c__0, &s[1], &work[ie], &vt[
				vt_offset], ldvt, &work[iu], &ldwrku, dum, &
				c__1, &work[iwork], info);

/*                    Multiply Q in A by left singular vectors of R in */
/*                    WORK(IU), storing result in U */
/*                    (Workspace: need N*N) */

			sgemm_("N", "N", m, n, n, &c_b79, &a[a_offset], lda, &
				work[iu], &ldwrku, &c_b57, &u[u_offset], ldu);

		    } else {

/*                    Insufficient workspace for a fast algorithm */

			itau = 1;
			iwork = itau + *n;

/*                    Compute A=Q*R, copying result to U */
/*                    (Workspace: need 2*N, prefer N+N*NB) */

			i__2 = *lwork - iwork + 1;
			sgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			slacpy_("L", m, n, &a[a_offset], lda, &u[u_offset], 
				ldu);

/*                    Generate Q in U */
/*                    (Workspace: need 2*N, prefer N+N*NB) */

			i__2 = *lwork - iwork + 1;
			sorgqr_(m, n, n, &u[u_offset], ldu, &work[itau], &
				work[iwork], &i__2, &ierr);

/*                    Copy R to VT, zeroing out below it */

			slacpy_("U", n, n, &a[a_offset], lda, &vt[vt_offset], 
				ldvt);
			if (*n > 1) {
			    i__2 = *n - 1;
			    i__3 = *n - 1;
			    slaset_("L", &i__2, &i__3, &c_b57, &c_b57, &vt[
				    vt_dim1 + 2], ldvt);
			}
			ie = itau;
			itauq = ie + *n;
			itaup = itauq + *n;
			iwork = itaup + *n;

/*                    Bidiagonalize R in VT */
/*                    (Workspace: need 4*N, prefer 3*N+2*N*NB) */

			i__2 = *lwork - iwork + 1;
			sgebrd_(n, n, &vt[vt_offset], ldvt, &s[1], &work[ie], 
				&work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Multiply Q in U by left bidiagonalizing vectors */
/*                    in VT */
/*                    (Workspace: need 3*N+M, prefer 3*N+M*NB) */

			i__2 = *lwork - iwork + 1;
			sormbr_("Q", "R", "N", m, n, n, &vt[vt_offset], ldvt, 
				&work[itauq], &u[u_offset], ldu, &work[iwork],
				 &i__2, &ierr);

/*                    Generate right bidiagonalizing vectors in VT */
/*                    (Workspace: need 4*N-1, prefer 3*N+(N-1)*NB) */

			i__2 = *lwork - iwork + 1;
			sorgbr_("P", n, n, n, &vt[vt_offset], ldvt, &work[
				itaup], &work[iwork], &i__2, &ierr)
				;
			iwork = ie + *n;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of A in U and computing right */
/*                    singular vectors of A in VT */
/*                    (Workspace: need BDSPAC) */

			sbdsqr_("U", n, n, m, &c__0, &s[1], &work[ie], &vt[
				vt_offset], ldvt, &u[u_offset], ldu, dum, &
				c__1, &work[iwork], info);

		    }

		}

	    } else if (wntua) {

		if (wntvn) {

/*                 Path 7 (M much larger than N, JOBU='A', JOBVT='N') */
/*                 M left singular vectors to be computed in U and */
/*                 no right singular vectors to be computed */

/* Computing MAX */
		    i__2 = *n + *m, i__3 = *n << 2, i__2 = f2cmax(i__2,i__3);
		    if (*lwork >= *n * *n + f2cmax(i__2,bdspac)) {

/*                    Sufficient workspace for a fast algorithm */

			ir = 1;
			if (*lwork >= wrkbl + *lda * *n) {

/*                       WORK(IR) is LDA by N */

			    ldwrkr = *lda;
			} else {

/*                       WORK(IR) is N by N */

			    ldwrkr = *n;
			}
			itau = ir + ldwrkr * *n;
			iwork = itau + *n;

/*                    Compute A=Q*R, copying result to U */
/*                    (Workspace: need N*N+2*N, prefer N*N+N+N*NB) */

			i__2 = *lwork - iwork + 1;
			sgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			slacpy_("L", m, n, &a[a_offset], lda, &u[u_offset], 
				ldu);

/*                    Copy R to WORK(IR), zeroing out below it */

			slacpy_("U", n, n, &a[a_offset], lda, &work[ir], &
				ldwrkr);
			i__2 = *n - 1;
			i__3 = *n - 1;
			slaset_("L", &i__2, &i__3, &c_b57, &c_b57, &work[ir + 
				1], &ldwrkr);

/*                    Generate Q in U */
/*                    (Workspace: need N*N+N+M, prefer N*N+N+M*NB) */

			i__2 = *lwork - iwork + 1;
			sorgqr_(m, m, n, &u[u_offset], ldu, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = itau;
			itauq = ie + *n;
			itaup = itauq + *n;
			iwork = itaup + *n;

/*                    Bidiagonalize R in WORK(IR) */
/*                    (Workspace: need N*N+4*N, prefer N*N+3*N+2*N*NB) */

			i__2 = *lwork - iwork + 1;
			sgebrd_(n, n, &work[ir], &ldwrkr, &s[1], &work[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Generate left bidiagonalizing vectors in WORK(IR) */
/*                    (Workspace: need N*N+4*N, prefer N*N+3*N+N*NB) */

			i__2 = *lwork - iwork + 1;
			sorgbr_("Q", n, n, n, &work[ir], &ldwrkr, &work[itauq]
				, &work[iwork], &i__2, &ierr);
			iwork = ie + *n;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of R in WORK(IR) */
/*                    (Workspace: need N*N+BDSPAC) */

			sbdsqr_("U", n, &c__0, n, &c__0, &s[1], &work[ie], 
				dum, &c__1, &work[ir], &ldwrkr, dum, &c__1, &
				work[iwork], info);

/*                    Multiply Q in U by left singular vectors of R in */
/*                    WORK(IR), storing result in A */
/*                    (Workspace: need N*N) */

			sgemm_("N", "N", m, n, n, &c_b79, &u[u_offset], ldu, &
				work[ir], &ldwrkr, &c_b57, &a[a_offset], lda);

/*                    Copy left singular vectors of A from A to U */

			slacpy_("F", m, n, &a[a_offset], lda, &u[u_offset], 
				ldu);

		    } else {

/*                    Insufficient workspace for a fast algorithm */

			itau = 1;
			iwork = itau + *n;

/*                    Compute A=Q*R, copying result to U */
/*                    (Workspace: need 2*N, prefer N+N*NB) */

			i__2 = *lwork - iwork + 1;
			sgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			slacpy_("L", m, n, &a[a_offset], lda, &u[u_offset], 
				ldu);

/*                    Generate Q in U */
/*                    (Workspace: need N+M, prefer N+M*NB) */

			i__2 = *lwork - iwork + 1;
			sorgqr_(m, m, n, &u[u_offset], ldu, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = itau;
			itauq = ie + *n;
			itaup = itauq + *n;
			iwork = itaup + *n;

/*                    Zero out below R in A */

			if (*n > 1) {
			    i__2 = *n - 1;
			    i__3 = *n - 1;
			    slaset_("L", &i__2, &i__3, &c_b57, &c_b57, &a[
				    a_dim1 + 2], lda);
			}

/*                    Bidiagonalize R in A */
/*                    (Workspace: need 4*N, prefer 3*N+2*N*NB) */

			i__2 = *lwork - iwork + 1;
			sgebrd_(n, n, &a[a_offset], lda, &s[1], &work[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Multiply Q in U by left bidiagonalizing vectors */
/*                    in A */
/*                    (Workspace: need 3*N+M, prefer 3*N+M*NB) */

			i__2 = *lwork - iwork + 1;
			sormbr_("Q", "R", "N", m, n, n, &a[a_offset], lda, &
				work[itauq], &u[u_offset], ldu, &work[iwork], 
				&i__2, &ierr)
				;
			iwork = ie + *n;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of A in U */
/*                    (Workspace: need BDSPAC) */

			sbdsqr_("U", n, &c__0, m, &c__0, &s[1], &work[ie], 
				dum, &c__1, &u[u_offset], ldu, dum, &c__1, &
				work[iwork], info);

		    }

		} else if (wntvo) {

/*                 Path 8 (M much larger than N, JOBU='A', JOBVT='O') */
/*                 M left singular vectors to be computed in U and */
/*                 N right singular vectors to be overwritten on A */

/* Computing MAX */
		    i__2 = *n + *m, i__3 = *n << 2, i__2 = f2cmax(i__2,i__3);
		    if (*lwork >= (*n << 1) * *n + f2cmax(i__2,bdspac)) {

/*                    Sufficient workspace for a fast algorithm */

			iu = 1;
			if (*lwork >= wrkbl + (*lda << 1) * *n) {

/*                       WORK(IU) is LDA by N and WORK(IR) is LDA by N */

			    ldwrku = *lda;
			    ir = iu + ldwrku * *n;
			    ldwrkr = *lda;
			} else if (*lwork >= wrkbl + (*lda + *n) * *n) {

/*                       WORK(IU) is LDA by N and WORK(IR) is N by N */

			    ldwrku = *lda;
			    ir = iu + ldwrku * *n;
			    ldwrkr = *n;
			} else {

/*                       WORK(IU) is N by N and WORK(IR) is N by N */

			    ldwrku = *n;
			    ir = iu + ldwrku * *n;
			    ldwrkr = *n;
			}
			itau = ir + ldwrkr * *n;
			iwork = itau + *n;

/*                    Compute A=Q*R, copying result to U */
/*                    (Workspace: need 2*N*N+2*N, prefer 2*N*N+N+N*NB) */

			i__2 = *lwork - iwork + 1;
			sgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			slacpy_("L", m, n, &a[a_offset], lda, &u[u_offset], 
				ldu);

/*                    Generate Q in U */
/*                    (Workspace: need 2*N*N+N+M, prefer 2*N*N+N+M*NB) */

			i__2 = *lwork - iwork + 1;
			sorgqr_(m, m, n, &u[u_offset], ldu, &work[itau], &
				work[iwork], &i__2, &ierr);

/*                    Copy R to WORK(IU), zeroing out below it */

			slacpy_("U", n, n, &a[a_offset], lda, &work[iu], &
				ldwrku);
			i__2 = *n - 1;
			i__3 = *n - 1;
			slaset_("L", &i__2, &i__3, &c_b57, &c_b57, &work[iu + 
				1], &ldwrku);
			ie = itau;
			itauq = ie + *n;
			itaup = itauq + *n;
			iwork = itaup + *n;

/*                    Bidiagonalize R in WORK(IU), copying result to */
/*                    WORK(IR) */
/*                    (Workspace: need 2*N*N+4*N, */
/*                                prefer 2*N*N+3*N+2*N*NB) */

			i__2 = *lwork - iwork + 1;
			sgebrd_(n, n, &work[iu], &ldwrku, &s[1], &work[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);
			slacpy_("U", n, n, &work[iu], &ldwrku, &work[ir], &
				ldwrkr);

/*                    Generate left bidiagonalizing vectors in WORK(IU) */
/*                    (Workspace: need 2*N*N+4*N, prefer 2*N*N+3*N+N*NB) */

			i__2 = *lwork - iwork + 1;
			sorgbr_("Q", n, n, n, &work[iu], &ldwrku, &work[itauq]
				, &work[iwork], &i__2, &ierr);

/*                    Generate right bidiagonalizing vectors in WORK(IR) */
/*                    (Workspace: need 2*N*N+4*N-1, */
/*                                prefer 2*N*N+3*N+(N-1)*NB) */

			i__2 = *lwork - iwork + 1;
			sorgbr_("P", n, n, n, &work[ir], &ldwrkr, &work[itaup]
				, &work[iwork], &i__2, &ierr);
			iwork = ie + *n;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of R in WORK(IU) and computing */
/*                    right singular vectors of R in WORK(IR) */
/*                    (Workspace: need 2*N*N+BDSPAC) */

			sbdsqr_("U", n, n, n, &c__0, &s[1], &work[ie], &work[
				ir], &ldwrkr, &work[iu], &ldwrku, dum, &c__1, 
				&work[iwork], info);

/*                    Multiply Q in U by left singular vectors of R in */
/*                    WORK(IU), storing result in A */
/*                    (Workspace: need N*N) */

			sgemm_("N", "N", m, n, n, &c_b79, &u[u_offset], ldu, &
				work[iu], &ldwrku, &c_b57, &a[a_offset], lda);

/*                    Copy left singular vectors of A from A to U */

			slacpy_("F", m, n, &a[a_offset], lda, &u[u_offset], 
				ldu);

/*                    Copy right singular vectors of R from WORK(IR) to A */

			slacpy_("F", n, n, &work[ir], &ldwrkr, &a[a_offset], 
				lda);

		    } else {

/*                    Insufficient workspace for a fast algorithm */

			itau = 1;
			iwork = itau + *n;

/*                    Compute A=Q*R, copying result to U */
/*                    (Workspace: need 2*N, prefer N+N*NB) */

			i__2 = *lwork - iwork + 1;
			sgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			slacpy_("L", m, n, &a[a_offset], lda, &u[u_offset], 
				ldu);

/*                    Generate Q in U */
/*                    (Workspace: need N+M, prefer N+M*NB) */

			i__2 = *lwork - iwork + 1;
			sorgqr_(m, m, n, &u[u_offset], ldu, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = itau;
			itauq = ie + *n;
			itaup = itauq + *n;
			iwork = itaup + *n;

/*                    Zero out below R in A */

			if (*n > 1) {
			    i__2 = *n - 1;
			    i__3 = *n - 1;
			    slaset_("L", &i__2, &i__3, &c_b57, &c_b57, &a[
				    a_dim1 + 2], lda);
			}

/*                    Bidiagonalize R in A */
/*                    (Workspace: need 4*N, prefer 3*N+2*N*NB) */

			i__2 = *lwork - iwork + 1;
			sgebrd_(n, n, &a[a_offset], lda, &s[1], &work[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Multiply Q in U by left bidiagonalizing vectors */
/*                    in A */
/*                    (Workspace: need 3*N+M, prefer 3*N+M*NB) */

			i__2 = *lwork - iwork + 1;
			sormbr_("Q", "R", "N", m, n, n, &a[a_offset], lda, &
				work[itauq], &u[u_offset], ldu, &work[iwork], 
				&i__2, &ierr)
				;

/*                    Generate right bidiagonalizing vectors in A */
/*                    (Workspace: need 4*N-1, prefer 3*N+(N-1)*NB) */

			i__2 = *lwork - iwork + 1;
			sorgbr_("P", n, n, n, &a[a_offset], lda, &work[itaup],
				 &work[iwork], &i__2, &ierr);
			iwork = ie + *n;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of A in U and computing right */
/*                    singular vectors of A in A */
/*                    (Workspace: need BDSPAC) */

			sbdsqr_("U", n, n, m, &c__0, &s[1], &work[ie], &a[
				a_offset], lda, &u[u_offset], ldu, dum, &c__1,
				 &work[iwork], info);

		    }

		} else if (wntvas) {

/*                 Path 9 (M much larger than N, JOBU='A', JOBVT='S' */
/*                         or 'A') */
/*                 M left singular vectors to be computed in U and */
/*                 N right singular vectors to be computed in VT */

/* Computing MAX */
		    i__2 = *n + *m, i__3 = *n << 2, i__2 = f2cmax(i__2,i__3);
		    if (*lwork >= *n * *n + f2cmax(i__2,bdspac)) {

/*                    Sufficient workspace for a fast algorithm */

			iu = 1;
			if (*lwork >= wrkbl + *lda * *n) {

/*                       WORK(IU) is LDA by N */

			    ldwrku = *lda;
			} else {

/*                       WORK(IU) is N by N */

			    ldwrku = *n;
			}
			itau = iu + ldwrku * *n;
			iwork = itau + *n;

/*                    Compute A=Q*R, copying result to U */
/*                    (Workspace: need N*N+2*N, prefer N*N+N+N*NB) */

			i__2 = *lwork - iwork + 1;
			sgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			slacpy_("L", m, n, &a[a_offset], lda, &u[u_offset], 
				ldu);

/*                    Generate Q in U */
/*                    (Workspace: need N*N+N+M, prefer N*N+N+M*NB) */

			i__2 = *lwork - iwork + 1;
			sorgqr_(m, m, n, &u[u_offset], ldu, &work[itau], &
				work[iwork], &i__2, &ierr);

/*                    Copy R to WORK(IU), zeroing out below it */

			slacpy_("U", n, n, &a[a_offset], lda, &work[iu], &
				ldwrku);
			i__2 = *n - 1;
			i__3 = *n - 1;
			slaset_("L", &i__2, &i__3, &c_b57, &c_b57, &work[iu + 
				1], &ldwrku);
			ie = itau;
			itauq = ie + *n;
			itaup = itauq + *n;
			iwork = itaup + *n;

/*                    Bidiagonalize R in WORK(IU), copying result to VT */
/*                    (Workspace: need N*N+4*N, prefer N*N+3*N+2*N*NB) */

			i__2 = *lwork - iwork + 1;
			sgebrd_(n, n, &work[iu], &ldwrku, &s[1], &work[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);
			slacpy_("U", n, n, &work[iu], &ldwrku, &vt[vt_offset],
				 ldvt);

/*                    Generate left bidiagonalizing vectors in WORK(IU) */
/*                    (Workspace: need N*N+4*N, prefer N*N+3*N+N*NB) */

			i__2 = *lwork - iwork + 1;
			sorgbr_("Q", n, n, n, &work[iu], &ldwrku, &work[itauq]
				, &work[iwork], &i__2, &ierr);

/*                    Generate right bidiagonalizing vectors in VT */
/*                    (Workspace: need N*N+4*N-1, */
/*                                prefer N*N+3*N+(N-1)*NB) */

			i__2 = *lwork - iwork + 1;
			sorgbr_("P", n, n, n, &vt[vt_offset], ldvt, &work[
				itaup], &work[iwork], &i__2, &ierr)
				;
			iwork = ie + *n;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of R in WORK(IU) and computing */
/*                    right singular vectors of R in VT */
/*                    (Workspace: need N*N+BDSPAC) */

			sbdsqr_("U", n, n, n, &c__0, &s[1], &work[ie], &vt[
				vt_offset], ldvt, &work[iu], &ldwrku, dum, &
				c__1, &work[iwork], info);

/*                    Multiply Q in U by left singular vectors of R in */
/*                    WORK(IU), storing result in A */
/*                    (Workspace: need N*N) */

			sgemm_("N", "N", m, n, n, &c_b79, &u[u_offset], ldu, &
				work[iu], &ldwrku, &c_b57, &a[a_offset], lda);

/*                    Copy left singular vectors of A from A to U */

			slacpy_("F", m, n, &a[a_offset], lda, &u[u_offset], 
				ldu);

		    } else {

/*                    Insufficient workspace for a fast algorithm */

			itau = 1;
			iwork = itau + *n;

/*                    Compute A=Q*R, copying result to U */
/*                    (Workspace: need 2*N, prefer N+N*NB) */

			i__2 = *lwork - iwork + 1;
			sgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			slacpy_("L", m, n, &a[a_offset], lda, &u[u_offset], 
				ldu);

/*                    Generate Q in U */
/*                    (Workspace: need N+M, prefer N+M*NB) */

			i__2 = *lwork - iwork + 1;
			sorgqr_(m, m, n, &u[u_offset], ldu, &work[itau], &
				work[iwork], &i__2, &ierr);

/*                    Copy R from A to VT, zeroing out below it */

			slacpy_("U", n, n, &a[a_offset], lda, &vt[vt_offset], 
				ldvt);
			if (*n > 1) {
			    i__2 = *n - 1;
			    i__3 = *n - 1;
			    slaset_("L", &i__2, &i__3, &c_b57, &c_b57, &vt[
				    vt_dim1 + 2], ldvt);
			}
			ie = itau;
			itauq = ie + *n;
			itaup = itauq + *n;
			iwork = itaup + *n;

/*                    Bidiagonalize R in VT */
/*                    (Workspace: need 4*N, prefer 3*N+2*N*NB) */

			i__2 = *lwork - iwork + 1;
			sgebrd_(n, n, &vt[vt_offset], ldvt, &s[1], &work[ie], 
				&work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Multiply Q in U by left bidiagonalizing vectors */
/*                    in VT */
/*                    (Workspace: need 3*N+M, prefer 3*N+M*NB) */

			i__2 = *lwork - iwork + 1;
			sormbr_("Q", "R", "N", m, n, n, &vt[vt_offset], ldvt, 
				&work[itauq], &u[u_offset], ldu, &work[iwork],
				 &i__2, &ierr);

/*                    Generate right bidiagonalizing vectors in VT */
/*                    (Workspace: need 4*N-1, prefer 3*N+(N-1)*NB) */

			i__2 = *lwork - iwork + 1;
			sorgbr_("P", n, n, n, &vt[vt_offset], ldvt, &work[
				itaup], &work[iwork], &i__2, &ierr)
				;
			iwork = ie + *n;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of A in U and computing right */
/*                    singular vectors of A in VT */
/*                    (Workspace: need BDSPAC) */

			sbdsqr_("U", n, n, m, &c__0, &s[1], &work[ie], &vt[
				vt_offset], ldvt, &u[u_offset], ldu, dum, &
				c__1, &work[iwork], info);

		    }

		}

	    }

	} else {

/*           M .LT. MNTHR */

/*           Path 10 (M at least N, but not much larger) */
/*           Reduce to bidiagonal form without QR decomposition */

	    ie = 1;
	    itauq = ie + *n;
	    itaup = itauq + *n;
	    iwork = itaup + *n;

/*           Bidiagonalize A */
/*           (Workspace: need 3*N+M, prefer 3*N+(M+N)*NB) */

	    i__2 = *lwork - iwork + 1;
	    sgebrd_(m, n, &a[a_offset], lda, &s[1], &work[ie], &work[itauq], &
		    work[itaup], &work[iwork], &i__2, &ierr);
	    if (wntuas) {

/*              If left singular vectors desired in U, copy result to U */
/*              and generate left bidiagonalizing vectors in U */
/*              (Workspace: need 3*N+NCU, prefer 3*N+NCU*NB) */

		slacpy_("L", m, n, &a[a_offset], lda, &u[u_offset], ldu);
		if (wntus) {
		    ncu = *n;
		}
		if (wntua) {
		    ncu = *m;
		}
		i__2 = *lwork - iwork + 1;
		sorgbr_("Q", m, &ncu, n, &u[u_offset], ldu, &work[itauq], &
			work[iwork], &i__2, &ierr);
	    }
	    if (wntvas) {

/*              If right singular vectors desired in VT, copy result to */
/*              VT and generate right bidiagonalizing vectors in VT */
/*              (Workspace: need 4*N-1, prefer 3*N+(N-1)*NB) */

		slacpy_("U", n, n, &a[a_offset], lda, &vt[vt_offset], ldvt);
		i__2 = *lwork - iwork + 1;
		sorgbr_("P", n, n, n, &vt[vt_offset], ldvt, &work[itaup], &
			work[iwork], &i__2, &ierr);
	    }
	    if (wntuo) {

/*              If left singular vectors desired in A, generate left */
/*              bidiagonalizing vectors in A */
/*              (Workspace: need 4*N, prefer 3*N+N*NB) */

		i__2 = *lwork - iwork + 1;
		sorgbr_("Q", m, n, n, &a[a_offset], lda, &work[itauq], &work[
			iwork], &i__2, &ierr);
	    }
	    if (wntvo) {

/*              If right singular vectors desired in A, generate right */
/*              bidiagonalizing vectors in A */
/*              (Workspace: need 4*N-1, prefer 3*N+(N-1)*NB) */

		i__2 = *lwork - iwork + 1;
		sorgbr_("P", n, n, n, &a[a_offset], lda, &work[itaup], &work[
			iwork], &i__2, &ierr);
	    }
	    iwork = ie + *n;
	    if (wntuas || wntuo) {
		nru = *m;
	    }
	    if (wntun) {
		nru = 0;
	    }
	    if (wntvas || wntvo) {
		ncvt = *n;
	    }
	    if (wntvn) {
		ncvt = 0;
	    }
	    if (! wntuo && ! wntvo) {

/*              Perform bidiagonal QR iteration, if desired, computing */
/*              left singular vectors in U and computing right singular */
/*              vectors in VT */
/*              (Workspace: need BDSPAC) */

		sbdsqr_("U", n, &ncvt, &nru, &c__0, &s[1], &work[ie], &vt[
			vt_offset], ldvt, &u[u_offset], ldu, dum, &c__1, &
			work[iwork], info);
	    } else if (! wntuo && wntvo) {

/*              Perform bidiagonal QR iteration, if desired, computing */
/*              left singular vectors in U and computing right singular */
/*              vectors in A */
/*              (Workspace: need BDSPAC) */

		sbdsqr_("U", n, &ncvt, &nru, &c__0, &s[1], &work[ie], &a[
			a_offset], lda, &u[u_offset], ldu, dum, &c__1, &work[
			iwork], info);
	    } else {

/*              Perform bidiagonal QR iteration, if desired, computing */
/*              left singular vectors in A and computing right singular */
/*              vectors in VT */
/*              (Workspace: need BDSPAC) */

		sbdsqr_("U", n, &ncvt, &nru, &c__0, &s[1], &work[ie], &vt[
			vt_offset], ldvt, &a[a_offset], lda, dum, &c__1, &
			work[iwork], info);
	    }

	}

    } else {

/*        A has more columns than rows. If A has sufficiently more */
/*        columns than rows, first reduce using the LQ decomposition (if */
/*        sufficient workspace available) */

	if (*n >= mnthr) {

	    if (wntvn) {

/*              Path 1t(N much larger than M, JOBVT='N') */
/*              No right singular vectors to be computed */

		itau = 1;
		iwork = itau + *m;

/*              Compute A=L*Q */
/*              (Workspace: need 2*M, prefer M+M*NB) */

		i__2 = *lwork - iwork + 1;
		sgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[iwork], &
			i__2, &ierr);

/*              Zero out above L */

		i__2 = *m - 1;
		i__3 = *m - 1;
		slaset_("U", &i__2, &i__3, &c_b57, &c_b57, &a[(a_dim1 << 1) + 
			1], lda);
		ie = 1;
		itauq = ie + *m;
		itaup = itauq + *m;
		iwork = itaup + *m;

/*              Bidiagonalize L in A */
/*              (Workspace: need 4*M, prefer 3*M+2*M*NB) */

		i__2 = *lwork - iwork + 1;
		sgebrd_(m, m, &a[a_offset], lda, &s[1], &work[ie], &work[
			itauq], &work[itaup], &work[iwork], &i__2, &ierr);
		if (wntuo || wntuas) {

/*                 If left singular vectors desired, generate Q */
/*                 (Workspace: need 4*M, prefer 3*M+M*NB) */

		    i__2 = *lwork - iwork + 1;
		    sorgbr_("Q", m, m, m, &a[a_offset], lda, &work[itauq], &
			    work[iwork], &i__2, &ierr);
		}
		iwork = ie + *m;
		nru = 0;
		if (wntuo || wntuas) {
		    nru = *m;
		}

/*              Perform bidiagonal QR iteration, computing left singular */
/*              vectors of A in A if desired */
/*              (Workspace: need BDSPAC) */

		sbdsqr_("U", m, &c__0, &nru, &c__0, &s[1], &work[ie], dum, &
			c__1, &a[a_offset], lda, dum, &c__1, &work[iwork], 
			info);

/*              If left singular vectors desired in U, copy them there */

		if (wntuas) {
		    slacpy_("F", m, m, &a[a_offset], lda, &u[u_offset], ldu);
		}

	    } else if (wntvo && wntun) {

/*              Path 2t(N much larger than M, JOBU='N', JOBVT='O') */
/*              M right singular vectors to be overwritten on A and */
/*              no left singular vectors to be computed */

/* Computing MAX */
		i__2 = *m << 2;
		if (*lwork >= *m * *m + f2cmax(i__2,bdspac)) {

/*                 Sufficient workspace for a fast algorithm */

		    ir = 1;
/* Computing MAX */
		    i__2 = wrkbl, i__3 = *lda * *n + *m;
		    if (*lwork >= f2cmax(i__2,i__3) + *lda * *m) {

/*                    WORK(IU) is LDA by N and WORK(IR) is LDA by M */

			ldwrku = *lda;
			chunk = *n;
			ldwrkr = *lda;
		    } else /* if(complicated condition) */ {
/* Computing MAX */
			i__2 = wrkbl, i__3 = *lda * *n + *m;
			if (*lwork >= f2cmax(i__2,i__3) + *m * *m) {

/*                    WORK(IU) is LDA by N and WORK(IR) is M by M */

			    ldwrku = *lda;
			    chunk = *n;
			    ldwrkr = *m;
			} else {

/*                    WORK(IU) is M by CHUNK and WORK(IR) is M by M */

			    ldwrku = *m;
			    chunk = (*lwork - *m * *m - *m) / *m;
			    ldwrkr = *m;
			}
		    }
		    itau = ir + ldwrkr * *m;
		    iwork = itau + *m;

/*                 Compute A=L*Q */
/*                 (Workspace: need M*M+2*M, prefer M*M+M+M*NB) */

		    i__2 = *lwork - iwork + 1;
		    sgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[iwork]
			    , &i__2, &ierr);

/*                 Copy L to WORK(IR) and zero out above it */

		    slacpy_("L", m, m, &a[a_offset], lda, &work[ir], &ldwrkr);
		    i__2 = *m - 1;
		    i__3 = *m - 1;
		    slaset_("U", &i__2, &i__3, &c_b57, &c_b57, &work[ir + 
			    ldwrkr], &ldwrkr);

/*                 Generate Q in A */
/*                 (Workspace: need M*M+2*M, prefer M*M+M+M*NB) */

		    i__2 = *lwork - iwork + 1;
		    sorglq_(m, n, m, &a[a_offset], lda, &work[itau], &work[
			    iwork], &i__2, &ierr);
		    ie = itau;
		    itauq = ie + *m;
		    itaup = itauq + *m;
		    iwork = itaup + *m;

/*                 Bidiagonalize L in WORK(IR) */
/*                 (Workspace: need M*M+4*M, prefer M*M+3*M+2*M*NB) */

		    i__2 = *lwork - iwork + 1;
		    sgebrd_(m, m, &work[ir], &ldwrkr, &s[1], &work[ie], &work[
			    itauq], &work[itaup], &work[iwork], &i__2, &ierr);

/*                 Generate right vectors bidiagonalizing L */
/*                 (Workspace: need M*M+4*M-1, prefer M*M+3*M+(M-1)*NB) */

		    i__2 = *lwork - iwork + 1;
		    sorgbr_("P", m, m, m, &work[ir], &ldwrkr, &work[itaup], &
			    work[iwork], &i__2, &ierr);
		    iwork = ie + *m;

/*                 Perform bidiagonal QR iteration, computing right */
/*                 singular vectors of L in WORK(IR) */
/*                 (Workspace: need M*M+BDSPAC) */

		    sbdsqr_("U", m, m, &c__0, &c__0, &s[1], &work[ie], &work[
			    ir], &ldwrkr, dum, &c__1, dum, &c__1, &work[iwork]
			    , info);
		    iu = ie + *m;

/*                 Multiply right singular vectors of L in WORK(IR) by Q */
/*                 in A, storing result in WORK(IU) and copying to A */
/*                 (Workspace: need M*M+2*M, prefer M*M+M*N+M) */

		    i__2 = *n;
		    i__3 = chunk;
		    for (i__ = 1; i__3 < 0 ? i__ >= i__2 : i__ <= i__2; i__ +=
			     i__3) {
/* Computing MIN */
			i__4 = *n - i__ + 1;
			blk = f2cmin(i__4,chunk);
			sgemm_("N", "N", m, &blk, m, &c_b79, &work[ir], &
				ldwrkr, &a[i__ * a_dim1 + 1], lda, &c_b57, &
				work[iu], &ldwrku);
			slacpy_("F", m, &blk, &work[iu], &ldwrku, &a[i__ * 
				a_dim1 + 1], lda);
/* L30: */
		    }

		} else {

/*                 Insufficient workspace for a fast algorithm */

		    ie = 1;
		    itauq = ie + *m;
		    itaup = itauq + *m;
		    iwork = itaup + *m;

/*                 Bidiagonalize A */
/*                 (Workspace: need 3*M+N, prefer 3*M+(M+N)*NB) */

		    i__3 = *lwork - iwork + 1;
		    sgebrd_(m, n, &a[a_offset], lda, &s[1], &work[ie], &work[
			    itauq], &work[itaup], &work[iwork], &i__3, &ierr);

/*                 Generate right vectors bidiagonalizing A */
/*                 (Workspace: need 4*M, prefer 3*M+M*NB) */

		    i__3 = *lwork - iwork + 1;
		    sorgbr_("P", m, n, m, &a[a_offset], lda, &work[itaup], &
			    work[iwork], &i__3, &ierr);
		    iwork = ie + *m;

/*                 Perform bidiagonal QR iteration, computing right */
/*                 singular vectors of A in A */
/*                 (Workspace: need BDSPAC) */

		    sbdsqr_("L", m, n, &c__0, &c__0, &s[1], &work[ie], &a[
			    a_offset], lda, dum, &c__1, dum, &c__1, &work[
			    iwork], info);

		}

	    } else if (wntvo && wntuas) {

/*              Path 3t(N much larger than M, JOBU='S' or 'A', JOBVT='O') */
/*              M right singular vectors to be overwritten on A and */
/*              M left singular vectors to be computed in U */

/* Computing MAX */
		i__3 = *m << 2;
		if (*lwork >= *m * *m + f2cmax(i__3,bdspac)) {

/*                 Sufficient workspace for a fast algorithm */

		    ir = 1;
/* Computing MAX */
		    i__3 = wrkbl, i__2 = *lda * *n + *m;
		    if (*lwork >= f2cmax(i__3,i__2) + *lda * *m) {

/*                    WORK(IU) is LDA by N and WORK(IR) is LDA by M */

			ldwrku = *lda;
			chunk = *n;
			ldwrkr = *lda;
		    } else /* if(complicated condition) */ {
/* Computing MAX */
			i__3 = wrkbl, i__2 = *lda * *n + *m;
			if (*lwork >= f2cmax(i__3,i__2) + *m * *m) {

/*                    WORK(IU) is LDA by N and WORK(IR) is M by M */

			    ldwrku = *lda;
			    chunk = *n;
			    ldwrkr = *m;
			} else {

/*                    WORK(IU) is M by CHUNK and WORK(IR) is M by M */

			    ldwrku = *m;
			    chunk = (*lwork - *m * *m - *m) / *m;
			    ldwrkr = *m;
			}
		    }
		    itau = ir + ldwrkr * *m;
		    iwork = itau + *m;

/*                 Compute A=L*Q */
/*                 (Workspace: need M*M+2*M, prefer M*M+M+M*NB) */

		    i__3 = *lwork - iwork + 1;
		    sgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[iwork]
			    , &i__3, &ierr);

/*                 Copy L to U, zeroing about above it */

		    slacpy_("L", m, m, &a[a_offset], lda, &u[u_offset], ldu);
		    i__3 = *m - 1;
		    i__2 = *m - 1;
		    slaset_("U", &i__3, &i__2, &c_b57, &c_b57, &u[(u_dim1 << 
			    1) + 1], ldu);

/*                 Generate Q in A */
/*                 (Workspace: need M*M+2*M, prefer M*M+M+M*NB) */

		    i__3 = *lwork - iwork + 1;
		    sorglq_(m, n, m, &a[a_offset], lda, &work[itau], &work[
			    iwork], &i__3, &ierr);
		    ie = itau;
		    itauq = ie + *m;
		    itaup = itauq + *m;
		    iwork = itaup + *m;

/*                 Bidiagonalize L in U, copying result to WORK(IR) */
/*                 (Workspace: need M*M+4*M, prefer M*M+3*M+2*M*NB) */

		    i__3 = *lwork - iwork + 1;
		    sgebrd_(m, m, &u[u_offset], ldu, &s[1], &work[ie], &work[
			    itauq], &work[itaup], &work[iwork], &i__3, &ierr);
		    slacpy_("U", m, m, &u[u_offset], ldu, &work[ir], &ldwrkr);

/*                 Generate right vectors bidiagonalizing L in WORK(IR) */
/*                 (Workspace: need M*M+4*M-1, prefer M*M+3*M+(M-1)*NB) */

		    i__3 = *lwork - iwork + 1;
		    sorgbr_("P", m, m, m, &work[ir], &ldwrkr, &work[itaup], &
			    work[iwork], &i__3, &ierr);

/*                 Generate left vectors bidiagonalizing L in U */
/*                 (Workspace: need M*M+4*M, prefer M*M+3*M+M*NB) */

		    i__3 = *lwork - iwork + 1;
		    sorgbr_("Q", m, m, m, &u[u_offset], ldu, &work[itauq], &
			    work[iwork], &i__3, &ierr);
		    iwork = ie + *m;

/*                 Perform bidiagonal QR iteration, computing left */
/*                 singular vectors of L in U, and computing right */
/*                 singular vectors of L in WORK(IR) */
/*                 (Workspace: need M*M+BDSPAC) */

		    sbdsqr_("U", m, m, m, &c__0, &s[1], &work[ie], &work[ir], 
			    &ldwrkr, &u[u_offset], ldu, dum, &c__1, &work[
			    iwork], info);
		    iu = ie + *m;

/*                 Multiply right singular vectors of L in WORK(IR) by Q */
/*                 in A, storing result in WORK(IU) and copying to A */
/*                 (Workspace: need M*M+2*M, prefer M*M+M*N+M)) */

		    i__3 = *n;
		    i__2 = chunk;
		    for (i__ = 1; i__2 < 0 ? i__ >= i__3 : i__ <= i__3; i__ +=
			     i__2) {
/* Computing MIN */
			i__4 = *n - i__ + 1;
			blk = f2cmin(i__4,chunk);
			sgemm_("N", "N", m, &blk, m, &c_b79, &work[ir], &
				ldwrkr, &a[i__ * a_dim1 + 1], lda, &c_b57, &
				work[iu], &ldwrku);
			slacpy_("F", m, &blk, &work[iu], &ldwrku, &a[i__ * 
				a_dim1 + 1], lda);
/* L40: */
		    }

		} else {

/*                 Insufficient workspace for a fast algorithm */

		    itau = 1;
		    iwork = itau + *m;

/*                 Compute A=L*Q */
/*                 (Workspace: need 2*M, prefer M+M*NB) */

		    i__2 = *lwork - iwork + 1;
		    sgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[iwork]
			    , &i__2, &ierr);

/*                 Copy L to U, zeroing out above it */

		    slacpy_("L", m, m, &a[a_offset], lda, &u[u_offset], ldu);
		    i__2 = *m - 1;
		    i__3 = *m - 1;
		    slaset_("U", &i__2, &i__3, &c_b57, &c_b57, &u[(u_dim1 << 
			    1) + 1], ldu);

/*                 Generate Q in A */
/*                 (Workspace: need 2*M, prefer M+M*NB) */

		    i__2 = *lwork - iwork + 1;
		    sorglq_(m, n, m, &a[a_offset], lda, &work[itau], &work[
			    iwork], &i__2, &ierr);
		    ie = itau;
		    itauq = ie + *m;
		    itaup = itauq + *m;
		    iwork = itaup + *m;

/*                 Bidiagonalize L in U */
/*                 (Workspace: need 4*M, prefer 3*M+2*M*NB) */

		    i__2 = *lwork - iwork + 1;
		    sgebrd_(m, m, &u[u_offset], ldu, &s[1], &work[ie], &work[
			    itauq], &work[itaup], &work[iwork], &i__2, &ierr);

/*                 Multiply right vectors bidiagonalizing L by Q in A */
/*                 (Workspace: need 3*M+N, prefer 3*M+N*NB) */

		    i__2 = *lwork - iwork + 1;
		    sormbr_("P", "L", "T", m, n, m, &u[u_offset], ldu, &work[
			    itaup], &a[a_offset], lda, &work[iwork], &i__2, &
			    ierr);

/*                 Generate left vectors bidiagonalizing L in U */
/*                 (Workspace: need 4*M, prefer 3*M+M*NB) */

		    i__2 = *lwork - iwork + 1;
		    sorgbr_("Q", m, m, m, &u[u_offset], ldu, &work[itauq], &
			    work[iwork], &i__2, &ierr);
		    iwork = ie + *m;

/*                 Perform bidiagonal QR iteration, computing left */
/*                 singular vectors of A in U and computing right */
/*                 singular vectors of A in A */
/*                 (Workspace: need BDSPAC) */

		    sbdsqr_("U", m, n, m, &c__0, &s[1], &work[ie], &a[
			    a_offset], lda, &u[u_offset], ldu, dum, &c__1, &
			    work[iwork], info);

		}

	    } else if (wntvs) {

		if (wntun) {

/*                 Path 4t(N much larger than M, JOBU='N', JOBVT='S') */
/*                 M right singular vectors to be computed in VT and */
/*                 no left singular vectors to be computed */

/* Computing MAX */
		    i__2 = *m << 2;
		    if (*lwork >= *m * *m + f2cmax(i__2,bdspac)) {

/*                    Sufficient workspace for a fast algorithm */

			ir = 1;
			if (*lwork >= wrkbl + *lda * *m) {

/*                       WORK(IR) is LDA by M */

			    ldwrkr = *lda;
			} else {

/*                       WORK(IR) is M by M */

			    ldwrkr = *m;
			}
			itau = ir + ldwrkr * *m;
			iwork = itau + *m;

/*                    Compute A=L*Q */
/*                    (Workspace: need M*M+2*M, prefer M*M+M+M*NB) */

			i__2 = *lwork - iwork + 1;
			sgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);

/*                    Copy L to WORK(IR), zeroing out above it */

			slacpy_("L", m, m, &a[a_offset], lda, &work[ir], &
				ldwrkr);
			i__2 = *m - 1;
			i__3 = *m - 1;
			slaset_("U", &i__2, &i__3, &c_b57, &c_b57, &work[ir + 
				ldwrkr], &ldwrkr);

/*                    Generate Q in A */
/*                    (Workspace: need M*M+2*M, prefer M*M+M+M*NB) */

			i__2 = *lwork - iwork + 1;
			sorglq_(m, n, m, &a[a_offset], lda, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = itau;
			itauq = ie + *m;
			itaup = itauq + *m;
			iwork = itaup + *m;

/*                    Bidiagonalize L in WORK(IR) */
/*                    (Workspace: need M*M+4*M, prefer M*M+3*M+2*M*NB) */

			i__2 = *lwork - iwork + 1;
			sgebrd_(m, m, &work[ir], &ldwrkr, &s[1], &work[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Generate right vectors bidiagonalizing L in */
/*                    WORK(IR) */
/*                    (Workspace: need M*M+4*M, prefer M*M+3*M+(M-1)*NB) */

			i__2 = *lwork - iwork + 1;
			sorgbr_("P", m, m, m, &work[ir], &ldwrkr, &work[itaup]
				, &work[iwork], &i__2, &ierr);
			iwork = ie + *m;

/*                    Perform bidiagonal QR iteration, computing right */
/*                    singular vectors of L in WORK(IR) */
/*                    (Workspace: need M*M+BDSPAC) */

			sbdsqr_("U", m, m, &c__0, &c__0, &s[1], &work[ie], &
				work[ir], &ldwrkr, dum, &c__1, dum, &c__1, &
				work[iwork], info);

/*                    Multiply right singular vectors of L in WORK(IR) by */
/*                    Q in A, storing result in VT */
/*                    (Workspace: need M*M) */

			sgemm_("N", "N", m, n, m, &c_b79, &work[ir], &ldwrkr, 
				&a[a_offset], lda, &c_b57, &vt[vt_offset], 
				ldvt);

		    } else {

/*                    Insufficient workspace for a fast algorithm */

			itau = 1;
			iwork = itau + *m;

/*                    Compute A=L*Q */
/*                    (Workspace: need 2*M, prefer M+M*NB) */

			i__2 = *lwork - iwork + 1;
			sgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);

/*                    Copy result to VT */

			slacpy_("U", m, n, &a[a_offset], lda, &vt[vt_offset], 
				ldvt);

/*                    Generate Q in VT */
/*                    (Workspace: need 2*M, prefer M+M*NB) */

			i__2 = *lwork - iwork + 1;
			sorglq_(m, n, m, &vt[vt_offset], ldvt, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = itau;
			itauq = ie + *m;
			itaup = itauq + *m;
			iwork = itaup + *m;

/*                    Zero out above L in A */

			i__2 = *m - 1;
			i__3 = *m - 1;
			slaset_("U", &i__2, &i__3, &c_b57, &c_b57, &a[(a_dim1 
				<< 1) + 1], lda);

/*                    Bidiagonalize L in A */
/*                    (Workspace: need 4*M, prefer 3*M+2*M*NB) */

			i__2 = *lwork - iwork + 1;
			sgebrd_(m, m, &a[a_offset], lda, &s[1], &work[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Multiply right vectors bidiagonalizing L by Q in VT */
/*                    (Workspace: need 3*M+N, prefer 3*M+N*NB) */

			i__2 = *lwork - iwork + 1;
			sormbr_("P", "L", "T", m, n, m, &a[a_offset], lda, &
				work[itaup], &vt[vt_offset], ldvt, &work[
				iwork], &i__2, &ierr);
			iwork = ie + *m;

/*                    Perform bidiagonal QR iteration, computing right */
/*                    singular vectors of A in VT */
/*                    (Workspace: need BDSPAC) */

			sbdsqr_("U", m, n, &c__0, &c__0, &s[1], &work[ie], &
				vt[vt_offset], ldvt, dum, &c__1, dum, &c__1, &
				work[iwork], info);

		    }

		} else if (wntuo) {

/*                 Path 5t(N much larger than M, JOBU='O', JOBVT='S') */
/*                 M right singular vectors to be computed in VT and */
/*                 M left singular vectors to be overwritten on A */

/* Computing MAX */
		    i__2 = *m << 2;
		    if (*lwork >= (*m << 1) * *m + f2cmax(i__2,bdspac)) {

/*                    Sufficient workspace for a fast algorithm */

			iu = 1;
			if (*lwork >= wrkbl + (*lda << 1) * *m) {

/*                       WORK(IU) is LDA by M and WORK(IR) is LDA by M */

			    ldwrku = *lda;
			    ir = iu + ldwrku * *m;
			    ldwrkr = *lda;
			} else if (*lwork >= wrkbl + (*lda + *m) * *m) {

/*                       WORK(IU) is LDA by M and WORK(IR) is M by M */

			    ldwrku = *lda;
			    ir = iu + ldwrku * *m;
			    ldwrkr = *m;
			} else {

/*                       WORK(IU) is M by M and WORK(IR) is M by M */

			    ldwrku = *m;
			    ir = iu + ldwrku * *m;
			    ldwrkr = *m;
			}
			itau = ir + ldwrkr * *m;
			iwork = itau + *m;

/*                    Compute A=L*Q */
/*                    (Workspace: need 2*M*M+2*M, prefer 2*M*M+M+M*NB) */

			i__2 = *lwork - iwork + 1;
			sgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);

/*                    Copy L to WORK(IU), zeroing out below it */

			slacpy_("L", m, m, &a[a_offset], lda, &work[iu], &
				ldwrku);
			i__2 = *m - 1;
			i__3 = *m - 1;
			slaset_("U", &i__2, &i__3, &c_b57, &c_b57, &work[iu + 
				ldwrku], &ldwrku);

/*                    Generate Q in A */
/*                    (Workspace: need 2*M*M+2*M, prefer 2*M*M+M+M*NB) */

			i__2 = *lwork - iwork + 1;
			sorglq_(m, n, m, &a[a_offset], lda, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = itau;
			itauq = ie + *m;
			itaup = itauq + *m;
			iwork = itaup + *m;

/*                    Bidiagonalize L in WORK(IU), copying result to */
/*                    WORK(IR) */
/*                    (Workspace: need 2*M*M+4*M, */
/*                                prefer 2*M*M+3*M+2*M*NB) */

			i__2 = *lwork - iwork + 1;
			sgebrd_(m, m, &work[iu], &ldwrku, &s[1], &work[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);
			slacpy_("L", m, m, &work[iu], &ldwrku, &work[ir], &
				ldwrkr);

/*                    Generate right bidiagonalizing vectors in WORK(IU) */
/*                    (Workspace: need 2*M*M+4*M-1, */
/*                                prefer 2*M*M+3*M+(M-1)*NB) */

			i__2 = *lwork - iwork + 1;
			sorgbr_("P", m, m, m, &work[iu], &ldwrku, &work[itaup]
				, &work[iwork], &i__2, &ierr);

/*                    Generate left bidiagonalizing vectors in WORK(IR) */
/*                    (Workspace: need 2*M*M+4*M, prefer 2*M*M+3*M+M*NB) */

			i__2 = *lwork - iwork + 1;
			sorgbr_("Q", m, m, m, &work[ir], &ldwrkr, &work[itauq]
				, &work[iwork], &i__2, &ierr);
			iwork = ie + *m;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of L in WORK(IR) and computing */
/*                    right singular vectors of L in WORK(IU) */
/*                    (Workspace: need 2*M*M+BDSPAC) */

			sbdsqr_("U", m, m, m, &c__0, &s[1], &work[ie], &work[
				iu], &ldwrku, &work[ir], &ldwrkr, dum, &c__1, 
				&work[iwork], info);

/*                    Multiply right singular vectors of L in WORK(IU) by */
/*                    Q in A, storing result in VT */
/*                    (Workspace: need M*M) */

			sgemm_("N", "N", m, n, m, &c_b79, &work[iu], &ldwrku, 
				&a[a_offset], lda, &c_b57, &vt[vt_offset], 
				ldvt);

/*                    Copy left singular vectors of L to A */
/*                    (Workspace: need M*M) */

			slacpy_("F", m, m, &work[ir], &ldwrkr, &a[a_offset], 
				lda);

		    } else {

/*                    Insufficient workspace for a fast algorithm */

			itau = 1;
			iwork = itau + *m;

/*                    Compute A=L*Q, copying result to VT */
/*                    (Workspace: need 2*M, prefer M+M*NB) */

			i__2 = *lwork - iwork + 1;
			sgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			slacpy_("U", m, n, &a[a_offset], lda, &vt[vt_offset], 
				ldvt);

/*                    Generate Q in VT */
/*                    (Workspace: need 2*M, prefer M+M*NB) */

			i__2 = *lwork - iwork + 1;
			sorglq_(m, n, m, &vt[vt_offset], ldvt, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = itau;
			itauq = ie + *m;
			itaup = itauq + *m;
			iwork = itaup + *m;

/*                    Zero out above L in A */

			i__2 = *m - 1;
			i__3 = *m - 1;
			slaset_("U", &i__2, &i__3, &c_b57, &c_b57, &a[(a_dim1 
				<< 1) + 1], lda);

/*                    Bidiagonalize L in A */
/*                    (Workspace: need 4*M, prefer 3*M+2*M*NB) */

			i__2 = *lwork - iwork + 1;
			sgebrd_(m, m, &a[a_offset], lda, &s[1], &work[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Multiply right vectors bidiagonalizing L by Q in VT */
/*                    (Workspace: need 3*M+N, prefer 3*M+N*NB) */

			i__2 = *lwork - iwork + 1;
			sormbr_("P", "L", "T", m, n, m, &a[a_offset], lda, &
				work[itaup], &vt[vt_offset], ldvt, &work[
				iwork], &i__2, &ierr);

/*                    Generate left bidiagonalizing vectors of L in A */
/*                    (Workspace: need 4*M, prefer 3*M+M*NB) */

			i__2 = *lwork - iwork + 1;
			sorgbr_("Q", m, m, m, &a[a_offset], lda, &work[itauq],
				 &work[iwork], &i__2, &ierr);
			iwork = ie + *m;

/*                    Perform bidiagonal QR iteration, compute left */
/*                    singular vectors of A in A and compute right */
/*                    singular vectors of A in VT */
/*                    (Workspace: need BDSPAC) */

			sbdsqr_("U", m, n, m, &c__0, &s[1], &work[ie], &vt[
				vt_offset], ldvt, &a[a_offset], lda, dum, &
				c__1, &work[iwork], info);

		    }

		} else if (wntuas) {

/*                 Path 6t(N much larger than M, JOBU='S' or 'A', */
/*                         JOBVT='S') */
/*                 M right singular vectors to be computed in VT and */
/*                 M left singular vectors to be computed in U */

/* Computing MAX */
		    i__2 = *m << 2;
		    if (*lwork >= *m * *m + f2cmax(i__2,bdspac)) {

/*                    Sufficient workspace for a fast algorithm */

			iu = 1;
			if (*lwork >= wrkbl + *lda * *m) {

/*                       WORK(IU) is LDA by N */

			    ldwrku = *lda;
			} else {

/*                       WORK(IU) is LDA by M */

			    ldwrku = *m;
			}
			itau = iu + ldwrku * *m;
			iwork = itau + *m;

/*                    Compute A=L*Q */
/*                    (Workspace: need M*M+2*M, prefer M*M+M+M*NB) */

			i__2 = *lwork - iwork + 1;
			sgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);

/*                    Copy L to WORK(IU), zeroing out above it */

			slacpy_("L", m, m, &a[a_offset], lda, &work[iu], &
				ldwrku);
			i__2 = *m - 1;
			i__3 = *m - 1;
			slaset_("U", &i__2, &i__3, &c_b57, &c_b57, &work[iu + 
				ldwrku], &ldwrku);

/*                    Generate Q in A */
/*                    (Workspace: need M*M+2*M, prefer M*M+M+M*NB) */

			i__2 = *lwork - iwork + 1;
			sorglq_(m, n, m, &a[a_offset], lda, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = itau;
			itauq = ie + *m;
			itaup = itauq + *m;
			iwork = itaup + *m;

/*                    Bidiagonalize L in WORK(IU), copying result to U */
/*                    (Workspace: need M*M+4*M, prefer M*M+3*M+2*M*NB) */

			i__2 = *lwork - iwork + 1;
			sgebrd_(m, m, &work[iu], &ldwrku, &s[1], &work[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);
			slacpy_("L", m, m, &work[iu], &ldwrku, &u[u_offset], 
				ldu);

/*                    Generate right bidiagonalizing vectors in WORK(IU) */
/*                    (Workspace: need M*M+4*M-1, */
/*                                prefer M*M+3*M+(M-1)*NB) */

			i__2 = *lwork - iwork + 1;
			sorgbr_("P", m, m, m, &work[iu], &ldwrku, &work[itaup]
				, &work[iwork], &i__2, &ierr);

/*                    Generate left bidiagonalizing vectors in U */
/*                    (Workspace: need M*M+4*M, prefer M*M+3*M+M*NB) */

			i__2 = *lwork - iwork + 1;
			sorgbr_("Q", m, m, m, &u[u_offset], ldu, &work[itauq],
				 &work[iwork], &i__2, &ierr);
			iwork = ie + *m;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of L in U and computing right */
/*                    singular vectors of L in WORK(IU) */
/*                    (Workspace: need M*M+BDSPAC) */

			sbdsqr_("U", m, m, m, &c__0, &s[1], &work[ie], &work[
				iu], &ldwrku, &u[u_offset], ldu, dum, &c__1, &
				work[iwork], info);

/*                    Multiply right singular vectors of L in WORK(IU) by */
/*                    Q in A, storing result in VT */
/*                    (Workspace: need M*M) */

			sgemm_("N", "N", m, n, m, &c_b79, &work[iu], &ldwrku, 
				&a[a_offset], lda, &c_b57, &vt[vt_offset], 
				ldvt);

		    } else {

/*                    Insufficient workspace for a fast algorithm */

			itau = 1;
			iwork = itau + *m;

/*                    Compute A=L*Q, copying result to VT */
/*                    (Workspace: need 2*M, prefer M+M*NB) */

			i__2 = *lwork - iwork + 1;
			sgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			slacpy_("U", m, n, &a[a_offset], lda, &vt[vt_offset], 
				ldvt);

/*                    Generate Q in VT */
/*                    (Workspace: need 2*M, prefer M+M*NB) */

			i__2 = *lwork - iwork + 1;
			sorglq_(m, n, m, &vt[vt_offset], ldvt, &work[itau], &
				work[iwork], &i__2, &ierr);

/*                    Copy L to U, zeroing out above it */

			slacpy_("L", m, m, &a[a_offset], lda, &u[u_offset], 
				ldu);
			i__2 = *m - 1;
			i__3 = *m - 1;
			slaset_("U", &i__2, &i__3, &c_b57, &c_b57, &u[(u_dim1 
				<< 1) + 1], ldu);
			ie = itau;
			itauq = ie + *m;
			itaup = itauq + *m;
			iwork = itaup + *m;

/*                    Bidiagonalize L in U */
/*                    (Workspace: need 4*M, prefer 3*M+2*M*NB) */

			i__2 = *lwork - iwork + 1;
			sgebrd_(m, m, &u[u_offset], ldu, &s[1], &work[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Multiply right bidiagonalizing vectors in U by Q */
/*                    in VT */
/*                    (Workspace: need 3*M+N, prefer 3*M+N*NB) */

			i__2 = *lwork - iwork + 1;
			sormbr_("P", "L", "T", m, n, m, &u[u_offset], ldu, &
				work[itaup], &vt[vt_offset], ldvt, &work[
				iwork], &i__2, &ierr);

/*                    Generate left bidiagonalizing vectors in U */
/*                    (Workspace: need 4*M, prefer 3*M+M*NB) */

			i__2 = *lwork - iwork + 1;
			sorgbr_("Q", m, m, m, &u[u_offset], ldu, &work[itauq],
				 &work[iwork], &i__2, &ierr);
			iwork = ie + *m;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of A in U and computing right */
/*                    singular vectors of A in VT */
/*                    (Workspace: need BDSPAC) */

			sbdsqr_("U", m, n, m, &c__0, &s[1], &work[ie], &vt[
				vt_offset], ldvt, &u[u_offset], ldu, dum, &
				c__1, &work[iwork], info);

		    }

		}

	    } else if (wntva) {

		if (wntun) {

/*                 Path 7t(N much larger than M, JOBU='N', JOBVT='A') */
/*                 N right singular vectors to be computed in VT and */
/*                 no left singular vectors to be computed */

/* Computing MAX */
		    i__2 = *n + *m, i__3 = *m << 2, i__2 = f2cmax(i__2,i__3);
		    if (*lwork >= *m * *m + f2cmax(i__2,bdspac)) {

/*                    Sufficient workspace for a fast algorithm */

			ir = 1;
			if (*lwork >= wrkbl + *lda * *m) {

/*                       WORK(IR) is LDA by M */

			    ldwrkr = *lda;
			} else {

/*                       WORK(IR) is M by M */

			    ldwrkr = *m;
			}
			itau = ir + ldwrkr * *m;
			iwork = itau + *m;

/*                    Compute A=L*Q, copying result to VT */
/*                    (Workspace: need M*M+2*M, prefer M*M+M+M*NB) */

			i__2 = *lwork - iwork + 1;
			sgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			slacpy_("U", m, n, &a[a_offset], lda, &vt[vt_offset], 
				ldvt);

/*                    Copy L to WORK(IR), zeroing out above it */

			slacpy_("L", m, m, &a[a_offset], lda, &work[ir], &
				ldwrkr);
			i__2 = *m - 1;
			i__3 = *m - 1;
			slaset_("U", &i__2, &i__3, &c_b57, &c_b57, &work[ir + 
				ldwrkr], &ldwrkr);

/*                    Generate Q in VT */
/*                    (Workspace: need M*M+M+N, prefer M*M+M+N*NB) */

			i__2 = *lwork - iwork + 1;
			sorglq_(n, n, m, &vt[vt_offset], ldvt, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = itau;
			itauq = ie + *m;
			itaup = itauq + *m;
			iwork = itaup + *m;

/*                    Bidiagonalize L in WORK(IR) */
/*                    (Workspace: need M*M+4*M, prefer M*M+3*M+2*M*NB) */

			i__2 = *lwork - iwork + 1;
			sgebrd_(m, m, &work[ir], &ldwrkr, &s[1], &work[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Generate right bidiagonalizing vectors in WORK(IR) */
/*                    (Workspace: need M*M+4*M-1, */
/*                                prefer M*M+3*M+(M-1)*NB) */

			i__2 = *lwork - iwork + 1;
			sorgbr_("P", m, m, m, &work[ir], &ldwrkr, &work[itaup]
				, &work[iwork], &i__2, &ierr);
			iwork = ie + *m;

/*                    Perform bidiagonal QR iteration, computing right */
/*                    singular vectors of L in WORK(IR) */
/*                    (Workspace: need M*M+BDSPAC) */

			sbdsqr_("U", m, m, &c__0, &c__0, &s[1], &work[ie], &
				work[ir], &ldwrkr, dum, &c__1, dum, &c__1, &
				work[iwork], info);

/*                    Multiply right singular vectors of L in WORK(IR) by */
/*                    Q in VT, storing result in A */
/*                    (Workspace: need M*M) */

			sgemm_("N", "N", m, n, m, &c_b79, &work[ir], &ldwrkr, 
				&vt[vt_offset], ldvt, &c_b57, &a[a_offset], 
				lda);

/*                    Copy right singular vectors of A from A to VT */

			slacpy_("F", m, n, &a[a_offset], lda, &vt[vt_offset], 
				ldvt);

		    } else {

/*                    Insufficient workspace for a fast algorithm */

			itau = 1;
			iwork = itau + *m;

/*                    Compute A=L*Q, copying result to VT */
/*                    (Workspace: need 2*M, prefer M+M*NB) */

			i__2 = *lwork - iwork + 1;
			sgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			slacpy_("U", m, n, &a[a_offset], lda, &vt[vt_offset], 
				ldvt);

/*                    Generate Q in VT */
/*                    (Workspace: need M+N, prefer M+N*NB) */

			i__2 = *lwork - iwork + 1;
			sorglq_(n, n, m, &vt[vt_offset], ldvt, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = itau;
			itauq = ie + *m;
			itaup = itauq + *m;
			iwork = itaup + *m;

/*                    Zero out above L in A */

			i__2 = *m - 1;
			i__3 = *m - 1;
			slaset_("U", &i__2, &i__3, &c_b57, &c_b57, &a[(a_dim1 
				<< 1) + 1], lda);

/*                    Bidiagonalize L in A */
/*                    (Workspace: need 4*M, prefer 3*M+2*M*NB) */

			i__2 = *lwork - iwork + 1;
			sgebrd_(m, m, &a[a_offset], lda, &s[1], &work[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Multiply right bidiagonalizing vectors in A by Q */
/*                    in VT */
/*                    (Workspace: need 3*M+N, prefer 3*M+N*NB) */

			i__2 = *lwork - iwork + 1;
			sormbr_("P", "L", "T", m, n, m, &a[a_offset], lda, &
				work[itaup], &vt[vt_offset], ldvt, &work[
				iwork], &i__2, &ierr);
			iwork = ie + *m;

/*                    Perform bidiagonal QR iteration, computing right */
/*                    singular vectors of A in VT */
/*                    (Workspace: need BDSPAC) */

			sbdsqr_("U", m, n, &c__0, &c__0, &s[1], &work[ie], &
				vt[vt_offset], ldvt, dum, &c__1, dum, &c__1, &
				work[iwork], info);

		    }

		} else if (wntuo) {

/*                 Path 8t(N much larger than M, JOBU='O', JOBVT='A') */
/*                 N right singular vectors to be computed in VT and */
/*                 M left singular vectors to be overwritten on A */

/* Computing MAX */
		    i__2 = *n + *m, i__3 = *m << 2, i__2 = f2cmax(i__2,i__3);
		    if (*lwork >= (*m << 1) * *m + f2cmax(i__2,bdspac)) {

/*                    Sufficient workspace for a fast algorithm */

			iu = 1;
			if (*lwork >= wrkbl + (*lda << 1) * *m) {

/*                       WORK(IU) is LDA by M and WORK(IR) is LDA by M */

			    ldwrku = *lda;
			    ir = iu + ldwrku * *m;
			    ldwrkr = *lda;
			} else if (*lwork >= wrkbl + (*lda + *m) * *m) {

/*                       WORK(IU) is LDA by M and WORK(IR) is M by M */

			    ldwrku = *lda;
			    ir = iu + ldwrku * *m;
			    ldwrkr = *m;
			} else {

/*                       WORK(IU) is M by M and WORK(IR) is M by M */

			    ldwrku = *m;
			    ir = iu + ldwrku * *m;
			    ldwrkr = *m;
			}
			itau = ir + ldwrkr * *m;
			iwork = itau + *m;

/*                    Compute A=L*Q, copying result to VT */
/*                    (Workspace: need 2*M*M+2*M, prefer 2*M*M+M+M*NB) */

			i__2 = *lwork - iwork + 1;
			sgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			slacpy_("U", m, n, &a[a_offset], lda, &vt[vt_offset], 
				ldvt);

/*                    Generate Q in VT */
/*                    (Workspace: need 2*M*M+M+N, prefer 2*M*M+M+N*NB) */

			i__2 = *lwork - iwork + 1;
			sorglq_(n, n, m, &vt[vt_offset], ldvt, &work[itau], &
				work[iwork], &i__2, &ierr);

/*                    Copy L to WORK(IU), zeroing out above it */

			slacpy_("L", m, m, &a[a_offset], lda, &work[iu], &
				ldwrku);
			i__2 = *m - 1;
			i__3 = *m - 1;
			slaset_("U", &i__2, &i__3, &c_b57, &c_b57, &work[iu + 
				ldwrku], &ldwrku);
			ie = itau;
			itauq = ie + *m;
			itaup = itauq + *m;
			iwork = itaup + *m;

/*                    Bidiagonalize L in WORK(IU), copying result to */
/*                    WORK(IR) */
/*                    (Workspace: need 2*M*M+4*M, */
/*                                prefer 2*M*M+3*M+2*M*NB) */

			i__2 = *lwork - iwork + 1;
			sgebrd_(m, m, &work[iu], &ldwrku, &s[1], &work[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);
			slacpy_("L", m, m, &work[iu], &ldwrku, &work[ir], &
				ldwrkr);

/*                    Generate right bidiagonalizing vectors in WORK(IU) */
/*                    (Workspace: need 2*M*M+4*M-1, */
/*                                prefer 2*M*M+3*M+(M-1)*NB) */

			i__2 = *lwork - iwork + 1;
			sorgbr_("P", m, m, m, &work[iu], &ldwrku, &work[itaup]
				, &work[iwork], &i__2, &ierr);

/*                    Generate left bidiagonalizing vectors in WORK(IR) */
/*                    (Workspace: need 2*M*M+4*M, prefer 2*M*M+3*M+M*NB) */

			i__2 = *lwork - iwork + 1;
			sorgbr_("Q", m, m, m, &work[ir], &ldwrkr, &work[itauq]
				, &work[iwork], &i__2, &ierr);
			iwork = ie + *m;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of L in WORK(IR) and computing */
/*                    right singular vectors of L in WORK(IU) */
/*                    (Workspace: need 2*M*M+BDSPAC) */

			sbdsqr_("U", m, m, m, &c__0, &s[1], &work[ie], &work[
				iu], &ldwrku, &work[ir], &ldwrkr, dum, &c__1, 
				&work[iwork], info);

/*                    Multiply right singular vectors of L in WORK(IU) by */
/*                    Q in VT, storing result in A */
/*                    (Workspace: need M*M) */

			sgemm_("N", "N", m, n, m, &c_b79, &work[iu], &ldwrku, 
				&vt[vt_offset], ldvt, &c_b57, &a[a_offset], 
				lda);

/*                    Copy right singular vectors of A from A to VT */

			slacpy_("F", m, n, &a[a_offset], lda, &vt[vt_offset], 
				ldvt);

/*                    Copy left singular vectors of A from WORK(IR) to A */

			slacpy_("F", m, m, &work[ir], &ldwrkr, &a[a_offset], 
				lda);

		    } else {

/*                    Insufficient workspace for a fast algorithm */

			itau = 1;
			iwork = itau + *m;

/*                    Compute A=L*Q, copying result to VT */
/*                    (Workspace: need 2*M, prefer M+M*NB) */

			i__2 = *lwork - iwork + 1;
			sgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			slacpy_("U", m, n, &a[a_offset], lda, &vt[vt_offset], 
				ldvt);

/*                    Generate Q in VT */
/*                    (Workspace: need M+N, prefer M+N*NB) */

			i__2 = *lwork - iwork + 1;
			sorglq_(n, n, m, &vt[vt_offset], ldvt, &work[itau], &
				work[iwork], &i__2, &ierr);
			ie = itau;
			itauq = ie + *m;
			itaup = itauq + *m;
			iwork = itaup + *m;

/*                    Zero out above L in A */

			i__2 = *m - 1;
			i__3 = *m - 1;
			slaset_("U", &i__2, &i__3, &c_b57, &c_b57, &a[(a_dim1 
				<< 1) + 1], lda);

/*                    Bidiagonalize L in A */
/*                    (Workspace: need 4*M, prefer 3*M+2*M*NB) */

			i__2 = *lwork - iwork + 1;
			sgebrd_(m, m, &a[a_offset], lda, &s[1], &work[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Multiply right bidiagonalizing vectors in A by Q */
/*                    in VT */
/*                    (Workspace: need 3*M+N, prefer 3*M+N*NB) */

			i__2 = *lwork - iwork + 1;
			sormbr_("P", "L", "T", m, n, m, &a[a_offset], lda, &
				work[itaup], &vt[vt_offset], ldvt, &work[
				iwork], &i__2, &ierr);

/*                    Generate left bidiagonalizing vectors in A */
/*                    (Workspace: need 4*M, prefer 3*M+M*NB) */

			i__2 = *lwork - iwork + 1;
			sorgbr_("Q", m, m, m, &a[a_offset], lda, &work[itauq],
				 &work[iwork], &i__2, &ierr);
			iwork = ie + *m;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of A in A and computing right */
/*                    singular vectors of A in VT */
/*                    (Workspace: need BDSPAC) */

			sbdsqr_("U", m, n, m, &c__0, &s[1], &work[ie], &vt[
				vt_offset], ldvt, &a[a_offset], lda, dum, &
				c__1, &work[iwork], info);

		    }

		} else if (wntuas) {

/*                 Path 9t(N much larger than M, JOBU='S' or 'A', */
/*                         JOBVT='A') */
/*                 N right singular vectors to be computed in VT and */
/*                 M left singular vectors to be computed in U */

/* Computing MAX */
		    i__2 = *n + *m, i__3 = *m << 2, i__2 = f2cmax(i__2,i__3);
		    if (*lwork >= *m * *m + f2cmax(i__2,bdspac)) {

/*                    Sufficient workspace for a fast algorithm */

			iu = 1;
			if (*lwork >= wrkbl + *lda * *m) {

/*                       WORK(IU) is LDA by M */

			    ldwrku = *lda;
			} else {

/*                       WORK(IU) is M by M */

			    ldwrku = *m;
			}
			itau = iu + ldwrku * *m;
			iwork = itau + *m;

/*                    Compute A=L*Q, copying result to VT */
/*                    (Workspace: need M*M+2*M, prefer M*M+M+M*NB) */

			i__2 = *lwork - iwork + 1;
			sgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			slacpy_("U", m, n, &a[a_offset], lda, &vt[vt_offset], 
				ldvt);

/*                    Generate Q in VT */
/*                    (Workspace: need M*M+M+N, prefer M*M+M+N*NB) */

			i__2 = *lwork - iwork + 1;
			sorglq_(n, n, m, &vt[vt_offset], ldvt, &work[itau], &
				work[iwork], &i__2, &ierr);

/*                    Copy L to WORK(IU), zeroing out above it */

			slacpy_("L", m, m, &a[a_offset], lda, &work[iu], &
				ldwrku);
			i__2 = *m - 1;
			i__3 = *m - 1;
			slaset_("U", &i__2, &i__3, &c_b57, &c_b57, &work[iu + 
				ldwrku], &ldwrku);
			ie = itau;
			itauq = ie + *m;
			itaup = itauq + *m;
			iwork = itaup + *m;

/*                    Bidiagonalize L in WORK(IU), copying result to U */
/*                    (Workspace: need M*M+4*M, prefer M*M+3*M+2*M*NB) */

			i__2 = *lwork - iwork + 1;
			sgebrd_(m, m, &work[iu], &ldwrku, &s[1], &work[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);
			slacpy_("L", m, m, &work[iu], &ldwrku, &u[u_offset], 
				ldu);

/*                    Generate right bidiagonalizing vectors in WORK(IU) */
/*                    (Workspace: need M*M+4*M, prefer M*M+3*M+(M-1)*NB) */

			i__2 = *lwork - iwork + 1;
			sorgbr_("P", m, m, m, &work[iu], &ldwrku, &work[itaup]
				, &work[iwork], &i__2, &ierr);

/*                    Generate left bidiagonalizing vectors in U */
/*                    (Workspace: need M*M+4*M, prefer M*M+3*M+M*NB) */

			i__2 = *lwork - iwork + 1;
			sorgbr_("Q", m, m, m, &u[u_offset], ldu, &work[itauq],
				 &work[iwork], &i__2, &ierr);
			iwork = ie + *m;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of L in U and computing right */
/*                    singular vectors of L in WORK(IU) */
/*                    (Workspace: need M*M+BDSPAC) */

			sbdsqr_("U", m, m, m, &c__0, &s[1], &work[ie], &work[
				iu], &ldwrku, &u[u_offset], ldu, dum, &c__1, &
				work[iwork], info);

/*                    Multiply right singular vectors of L in WORK(IU) by */
/*                    Q in VT, storing result in A */
/*                    (Workspace: need M*M) */

			sgemm_("N", "N", m, n, m, &c_b79, &work[iu], &ldwrku, 
				&vt[vt_offset], ldvt, &c_b57, &a[a_offset], 
				lda);

/*                    Copy right singular vectors of A from A to VT */

			slacpy_("F", m, n, &a[a_offset], lda, &vt[vt_offset], 
				ldvt);

		    } else {

/*                    Insufficient workspace for a fast algorithm */

			itau = 1;
			iwork = itau + *m;

/*                    Compute A=L*Q, copying result to VT */
/*                    (Workspace: need 2*M, prefer M+M*NB) */

			i__2 = *lwork - iwork + 1;
			sgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[
				iwork], &i__2, &ierr);
			slacpy_("U", m, n, &a[a_offset], lda, &vt[vt_offset], 
				ldvt);

/*                    Generate Q in VT */
/*                    (Workspace: need M+N, prefer M+N*NB) */

			i__2 = *lwork - iwork + 1;
			sorglq_(n, n, m, &vt[vt_offset], ldvt, &work[itau], &
				work[iwork], &i__2, &ierr);

/*                    Copy L to U, zeroing out above it */

			slacpy_("L", m, m, &a[a_offset], lda, &u[u_offset], 
				ldu);
			i__2 = *m - 1;
			i__3 = *m - 1;
			slaset_("U", &i__2, &i__3, &c_b57, &c_b57, &u[(u_dim1 
				<< 1) + 1], ldu);
			ie = itau;
			itauq = ie + *m;
			itaup = itauq + *m;
			iwork = itaup + *m;

/*                    Bidiagonalize L in U */
/*                    (Workspace: need 4*M, prefer 3*M+2*M*NB) */

			i__2 = *lwork - iwork + 1;
			sgebrd_(m, m, &u[u_offset], ldu, &s[1], &work[ie], &
				work[itauq], &work[itaup], &work[iwork], &
				i__2, &ierr);

/*                    Multiply right bidiagonalizing vectors in U by Q */
/*                    in VT */
/*                    (Workspace: need 3*M+N, prefer 3*M+N*NB) */

			i__2 = *lwork - iwork + 1;
			sormbr_("P", "L", "T", m, n, m, &u[u_offset], ldu, &
				work[itaup], &vt[vt_offset], ldvt, &work[
				iwork], &i__2, &ierr);

/*                    Generate left bidiagonalizing vectors in U */
/*                    (Workspace: need 4*M, prefer 3*M+M*NB) */

			i__2 = *lwork - iwork + 1;
			sorgbr_("Q", m, m, m, &u[u_offset], ldu, &work[itauq],
				 &work[iwork], &i__2, &ierr);
			iwork = ie + *m;

/*                    Perform bidiagonal QR iteration, computing left */
/*                    singular vectors of A in U and computing right */
/*                    singular vectors of A in VT */
/*                    (Workspace: need BDSPAC) */

			sbdsqr_("U", m, n, m, &c__0, &s[1], &work[ie], &vt[
				vt_offset], ldvt, &u[u_offset], ldu, dum, &
				c__1, &work[iwork], info);

		    }

		}

	    }

	} else {

/*           N .LT. MNTHR */

/*           Path 10t(N greater than M, but not much larger) */
/*           Reduce to bidiagonal form without LQ decomposition */

	    ie = 1;
	    itauq = ie + *m;
	    itaup = itauq + *m;
	    iwork = itaup + *m;

/*           Bidiagonalize A */
/*           (Workspace: need 3*M+N, prefer 3*M+(M+N)*NB) */

	    i__2 = *lwork - iwork + 1;
	    sgebrd_(m, n, &a[a_offset], lda, &s[1], &work[ie], &work[itauq], &
		    work[itaup], &work[iwork], &i__2, &ierr);
	    if (wntuas) {

/*              If left singular vectors desired in U, copy result to U */
/*              and generate left bidiagonalizing vectors in U */
/*              (Workspace: need 4*M-1, prefer 3*M+(M-1)*NB) */

		slacpy_("L", m, m, &a[a_offset], lda, &u[u_offset], ldu);
		i__2 = *lwork - iwork + 1;
		sorgbr_("Q", m, m, n, &u[u_offset], ldu, &work[itauq], &work[
			iwork], &i__2, &ierr);
	    }
	    if (wntvas) {

/*              If right singular vectors desired in VT, copy result to */
/*              VT and generate right bidiagonalizing vectors in VT */
/*              (Workspace: need 3*M+NRVT, prefer 3*M+NRVT*NB) */

		slacpy_("U", m, n, &a[a_offset], lda, &vt[vt_offset], ldvt);
		if (wntva) {
		    nrvt = *n;
		}
		if (wntvs) {
		    nrvt = *m;
		}
		i__2 = *lwork - iwork + 1;
		sorgbr_("P", &nrvt, n, m, &vt[vt_offset], ldvt, &work[itaup], 
			&work[iwork], &i__2, &ierr);
	    }
	    if (wntuo) {

/*              If left singular vectors desired in A, generate left */
/*              bidiagonalizing vectors in A */
/*              (Workspace: need 4*M-1, prefer 3*M+(M-1)*NB) */

		i__2 = *lwork - iwork + 1;
		sorgbr_("Q", m, m, n, &a[a_offset], lda, &work[itauq], &work[
			iwork], &i__2, &ierr);
	    }
	    if (wntvo) {

/*              If right singular vectors desired in A, generate right */
/*              bidiagonalizing vectors in A */
/*              (Workspace: need 4*M, prefer 3*M+M*NB) */

		i__2 = *lwork - iwork + 1;
		sorgbr_("P", m, n, m, &a[a_offset], lda, &work[itaup], &work[
			iwork], &i__2, &ierr);
	    }
	    iwork = ie + *m;
	    if (wntuas || wntuo) {
		nru = *m;
	    }
	    if (wntun) {
		nru = 0;
	    }
	    if (wntvas || wntvo) {
		ncvt = *n;
	    }
	    if (wntvn) {
		ncvt = 0;
	    }
	    if (! wntuo && ! wntvo) {

/*              Perform bidiagonal QR iteration, if desired, computing */
/*              left singular vectors in U and computing right singular */
/*              vectors in VT */
/*              (Workspace: need BDSPAC) */

		sbdsqr_("L", m, &ncvt, &nru, &c__0, &s[1], &work[ie], &vt[
			vt_offset], ldvt, &u[u_offset], ldu, dum, &c__1, &
			work[iwork], info);
	    } else if (! wntuo && wntvo) {

/*              Perform bidiagonal QR iteration, if desired, computing */
/*              left singular vectors in U and computing right singular */
/*              vectors in A */
/*              (Workspace: need BDSPAC) */

		sbdsqr_("L", m, &ncvt, &nru, &c__0, &s[1], &work[ie], &a[
			a_offset], lda, &u[u_offset], ldu, dum, &c__1, &work[
			iwork], info);
	    } else {

/*              Perform bidiagonal QR iteration, if desired, computing */
/*              left singular vectors in A and computing right singular */
/*              vectors in VT */
/*              (Workspace: need BDSPAC) */

		sbdsqr_("L", m, &ncvt, &nru, &c__0, &s[1], &work[ie], &vt[
			vt_offset], ldvt, &a[a_offset], lda, dum, &c__1, &
			work[iwork], info);
	    }

	}

    }

/*     If SBDSQR failed to converge, copy unconverged superdiagonals */
/*     to WORK( 2:MINMN ) */

    if (*info != 0) {
	if (ie > 2) {
	    i__2 = minmn - 1;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		work[i__ + 1] = work[i__ + ie - 1];
/* L50: */
	    }
	}
	if (ie < 2) {
	    for (i__ = minmn - 1; i__ >= 1; --i__) {
		work[i__ + 1] = work[i__ + ie - 1];
/* L60: */
	    }
	}
    }

/*     Undo scaling if necessary */

    if (iscl == 1) {
	if (anrm > bignum) {
	    slascl_("G", &c__0, &c__0, &bignum, &anrm, &minmn, &c__1, &s[1], &
		    minmn, &ierr);
	}
	if (*info != 0 && anrm > bignum) {
	    i__2 = minmn - 1;
	    slascl_("G", &c__0, &c__0, &bignum, &anrm, &i__2, &c__1, &work[2],
		     &minmn, &ierr);
	}
	if (anrm < smlnum) {
	    slascl_("G", &c__0, &c__0, &smlnum, &anrm, &minmn, &c__1, &s[1], &
		    minmn, &ierr);
	}
	if (*info != 0 && anrm < smlnum) {
	    i__2 = minmn - 1;
	    slascl_("G", &c__0, &c__0, &smlnum, &anrm, &i__2, &c__1, &work[2],
		     &minmn, &ierr);
	}
    }

/*     Return optimal workspace in WORK(1) */

    work[1] = (real) maxwrk;

    return;

/*     End of SGESVD */

} /* sgesvd_ */

