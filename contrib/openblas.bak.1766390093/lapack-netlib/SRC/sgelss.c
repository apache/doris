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
static integer c_n1 = -1;
static integer c__1 = 1;
static integer c__0 = 0;
static real c_b50 = 0.f;
static real c_b83 = 1.f;

/* > \brief <b> SGELSS solves overdetermined or underdetermined systems for GE matrices</b> */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download SGELSS + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/sgelss.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/sgelss.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/sgelss.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE SGELSS( M, N, NRHS, A, LDA, B, LDB, S, RCOND, RANK, */
/*                          WORK, LWORK, INFO ) */

/*       INTEGER            INFO, LDA, LDB, LWORK, M, N, NRHS, RANK */
/*       REAL               RCOND */
/*       REAL               A( LDA, * ), B( LDB, * ), S( * ), WORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > SGELSS computes the minimum norm solution to a real linear least */
/* > squares problem: */
/* > */
/* > Minimize 2-norm(| b - A*x |). */
/* > */
/* > using the singular value decomposition (SVD) of A. A is an M-by-N */
/* > matrix which may be rank-deficient. */
/* > */
/* > Several right hand side vectors b and solution vectors x can be */
/* > handled in a single call; they are stored as the columns of the */
/* > M-by-NRHS right hand side matrix B and the N-by-NRHS solution matrix */
/* > X. */
/* > */
/* > The effective rank of A is determined by treating as zero those */
/* > singular values which are less than RCOND times the largest singular */
/* > value. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >          The number of rows of the matrix A. M >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The number of columns of the matrix A. N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] NRHS */
/* > \verbatim */
/* >          NRHS is INTEGER */
/* >          The number of right hand sides, i.e., the number of columns */
/* >          of the matrices B and X. NRHS >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] A */
/* > \verbatim */
/* >          A is REAL array, dimension (LDA,N) */
/* >          On entry, the M-by-N matrix A. */
/* >          On exit, the first f2cmin(m,n) rows of A are overwritten with */
/* >          its right singular vectors, stored rowwise. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of the array A.  LDA >= f2cmax(1,M). */
/* > \endverbatim */
/* > */
/* > \param[in,out] B */
/* > \verbatim */
/* >          B is REAL array, dimension (LDB,NRHS) */
/* >          On entry, the M-by-NRHS right hand side matrix B. */
/* >          On exit, B is overwritten by the N-by-NRHS solution */
/* >          matrix X.  If m >= n and RANK = n, the residual */
/* >          sum-of-squares for the solution in the i-th column is given */
/* >          by the sum of squares of elements n+1:m in that column. */
/* > \endverbatim */
/* > */
/* > \param[in] LDB */
/* > \verbatim */
/* >          LDB is INTEGER */
/* >          The leading dimension of the array B. LDB >= f2cmax(1,f2cmax(M,N)). */
/* > \endverbatim */
/* > */
/* > \param[out] S */
/* > \verbatim */
/* >          S is REAL array, dimension (f2cmin(M,N)) */
/* >          The singular values of A in decreasing order. */
/* >          The condition number of A in the 2-norm = S(1)/S(f2cmin(m,n)). */
/* > \endverbatim */
/* > */
/* > \param[in] RCOND */
/* > \verbatim */
/* >          RCOND is REAL */
/* >          RCOND is used to determine the effective rank of A. */
/* >          Singular values S(i) <= RCOND*S(1) are treated as zero. */
/* >          If RCOND < 0, machine precision is used instead. */
/* > \endverbatim */
/* > */
/* > \param[out] RANK */
/* > \verbatim */
/* >          RANK is INTEGER */
/* >          The effective rank of A, i.e., the number of singular values */
/* >          which are greater than RCOND*S(1). */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is REAL array, dimension (MAX(1,LWORK)) */
/* >          On exit, if INFO = 0, WORK(1) returns the optimal LWORK. */
/* > \endverbatim */
/* > */
/* > \param[in] LWORK */
/* > \verbatim */
/* >          LWORK is INTEGER */
/* >          The dimension of the array WORK. LWORK >= 1, and also: */
/* >          LWORK >= 3*f2cmin(M,N) + f2cmax( 2*f2cmin(M,N), f2cmax(M,N), NRHS ) */
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
/* >          = 0:  successful exit */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value. */
/* >          > 0:  the algorithm for computing the SVD failed to converge; */
/* >                if INFO = i, i off-diagonal elements of an intermediate */
/* >                bidiagonal form did not converge to zero. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup realGEsolve */

/*  ===================================================================== */
/* Subroutine */ void sgelss_(integer *m, integer *n, integer *nrhs, real *a, 
	integer *lda, real *b, integer *ldb, real *s, real *rcond, integer *
	rank, real *work, integer *lwork, integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, i__1, i__2, i__3, i__4;
    real r__1;

    /* Local variables */
    real anrm, bnrm;
    integer itau, lwork_sgebrd__, lwork_sgeqrf__, i__, lwork_sorgbr__, 
	    lwork_sormbr__, lwork_sormlq__, iascl, ibscl, lwork_sormqr__, 
	    chunk;
    extern /* Subroutine */ void sgemm_(char *, char *, integer *, integer *, 
	    integer *, real *, real *, integer *, real *, integer *, real *, 
	    real *, integer *);
    real sfmin;
    integer minmn, maxmn;
    extern /* Subroutine */ void sgemv_(char *, integer *, integer *, real *, 
	    real *, integer *, real *, integer *, real *, real *, integer *);
    integer itaup, itauq;
    extern /* Subroutine */ void srscl_(integer *, real *, real *, integer *);
    integer mnthr, iwork;
    extern /* Subroutine */ void scopy_(integer *, real *, integer *, real *, 
	    integer *);
    integer bl, ie, il;
    extern /* Subroutine */ void slabad_(real *, real *);
    integer mm, bdspac;
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
	    , real *, integer *, integer *);
    integer ldwork;
    extern /* Subroutine */ void sormbr_(char *, char *, char *, integer *, 
	    integer *, integer *, real *, integer *, real *, real *, integer *
	    , real *, integer *, integer *);
    integer minwrk, maxwrk;
    real smlnum;
    extern /* Subroutine */ void sormlq_(char *, char *, integer *, integer *, 
	    integer *, real *, integer *, real *, real *, integer *, real *, 
	    integer *, integer *);
    logical lquery;
    extern /* Subroutine */ void sormqr_(char *, char *, integer *, integer *, 
	    integer *, real *, integer *, real *, real *, integer *, real *, 
	    integer *, integer *);
    real dum[1], eps, thr;


/*  -- LAPACK driver routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


/*     Test the input arguments */

    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    b_dim1 = *ldb;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    --s;
    --work;

    /* Function Body */
    *info = 0;
    minmn = f2cmin(*m,*n);
    maxmn = f2cmax(*m,*n);
    lquery = *lwork == -1;
    if (*m < 0) {
	*info = -1;
    } else if (*n < 0) {
	*info = -2;
    } else if (*nrhs < 0) {
	*info = -3;
    } else if (*lda < f2cmax(1,*m)) {
	*info = -5;
    } else if (*ldb < f2cmax(1,maxmn)) {
	*info = -7;
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
	if (minmn > 0) {
	    mm = *m;
	    mnthr = ilaenv_(&c__6, "SGELSS", " ", m, n, nrhs, &c_n1, (ftnlen)
		    6, (ftnlen)1);
	    if (*m >= *n && *m >= mnthr) {

/*              Path 1a - overdetermined, with many more rows than */
/*                        columns */

/*              Compute space needed for SGEQRF */
		sgeqrf_(m, n, &a[a_offset], lda, dum, dum, &c_n1, info);
		lwork_sgeqrf__ = dum[0];
/*              Compute space needed for SORMQR */
		sormqr_("L", "T", m, nrhs, n, &a[a_offset], lda, dum, &b[
			b_offset], ldb, dum, &c_n1, info);
		lwork_sormqr__ = dum[0];
		mm = *n;
/* Computing MAX */
		i__1 = maxwrk, i__2 = *n + lwork_sgeqrf__;
		maxwrk = f2cmax(i__1,i__2);
/* Computing MAX */
		i__1 = maxwrk, i__2 = *n + lwork_sormqr__;
		maxwrk = f2cmax(i__1,i__2);
	    }
	    if (*m >= *n) {

/*              Path 1 - overdetermined or exactly determined */

/*              Compute workspace needed for SBDSQR */

/* Computing MAX */
		i__1 = 1, i__2 = *n * 5;
		bdspac = f2cmax(i__1,i__2);
/*              Compute space needed for SGEBRD */
		sgebrd_(&mm, n, &a[a_offset], lda, &s[1], dum, dum, dum, dum, 
			&c_n1, info);
		lwork_sgebrd__ = dum[0];
/*              Compute space needed for SORMBR */
		sormbr_("Q", "L", "T", &mm, nrhs, n, &a[a_offset], lda, dum, &
			b[b_offset], ldb, dum, &c_n1, info);
		lwork_sormbr__ = dum[0];
/*              Compute space needed for SORGBR */
		sorgbr_("P", n, n, n, &a[a_offset], lda, dum, dum, &c_n1, 
			info);
		lwork_sorgbr__ = dum[0];
/*              Compute total workspace needed */
/* Computing MAX */
		i__1 = maxwrk, i__2 = *n * 3 + lwork_sgebrd__;
		maxwrk = f2cmax(i__1,i__2);
/* Computing MAX */
		i__1 = maxwrk, i__2 = *n * 3 + lwork_sormbr__;
		maxwrk = f2cmax(i__1,i__2);
/* Computing MAX */
		i__1 = maxwrk, i__2 = *n * 3 + lwork_sorgbr__;
		maxwrk = f2cmax(i__1,i__2);
		maxwrk = f2cmax(maxwrk,bdspac);
/* Computing MAX */
		i__1 = maxwrk, i__2 = *n * *nrhs;
		maxwrk = f2cmax(i__1,i__2);
/* Computing MAX */
		i__1 = *n * 3 + mm, i__2 = *n * 3 + *nrhs, i__1 = f2cmax(i__1,
			i__2);
		minwrk = f2cmax(i__1,bdspac);
		maxwrk = f2cmax(minwrk,maxwrk);
	    }
	    if (*n > *m) {

/*              Compute workspace needed for SBDSQR */

/* Computing MAX */
		i__1 = 1, i__2 = *m * 5;
		bdspac = f2cmax(i__1,i__2);
/* Computing MAX */
		i__1 = *m * 3 + *nrhs, i__2 = *m * 3 + *n, i__1 = f2cmax(i__1,
			i__2);
		minwrk = f2cmax(i__1,bdspac);
		if (*n >= mnthr) {

/*                 Path 2a - underdetermined, with many more columns */
/*                 than rows */

/*                 Compute space needed for SGEBRD */
		    sgebrd_(m, m, &a[a_offset], lda, &s[1], dum, dum, dum, 
			    dum, &c_n1, info);
		    lwork_sgebrd__ = dum[0];
/*                 Compute space needed for SORMBR */
		    sormbr_("Q", "L", "T", m, nrhs, n, &a[a_offset], lda, dum,
			     &b[b_offset], ldb, dum, &c_n1, info);
		    lwork_sormbr__ = dum[0];
/*                 Compute space needed for SORGBR */
		    sorgbr_("P", m, m, m, &a[a_offset], lda, dum, dum, &c_n1, 
			    info);
		    lwork_sorgbr__ = dum[0];
/*                 Compute space needed for SORMLQ */
		    sormlq_("L", "T", n, nrhs, m, &a[a_offset], lda, dum, &b[
			    b_offset], ldb, dum, &c_n1, info);
		    lwork_sormlq__ = dum[0];
/*                 Compute total workspace needed */
		    maxwrk = *m + *m * ilaenv_(&c__1, "SGELQF", " ", m, n, &
			    c_n1, &c_n1, (ftnlen)6, (ftnlen)1);
/* Computing MAX */
		    i__1 = maxwrk, i__2 = *m * *m + (*m << 2) + 
			    lwork_sgebrd__;
		    maxwrk = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = maxwrk, i__2 = *m * *m + (*m << 2) + 
			    lwork_sormbr__;
		    maxwrk = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = maxwrk, i__2 = *m * *m + (*m << 2) + 
			    lwork_sorgbr__;
		    maxwrk = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = maxwrk, i__2 = *m * *m + *m + bdspac;
		    maxwrk = f2cmax(i__1,i__2);
		    if (*nrhs > 1) {
/* Computing MAX */
			i__1 = maxwrk, i__2 = *m * *m + *m + *m * *nrhs;
			maxwrk = f2cmax(i__1,i__2);
		    } else {
/* Computing MAX */
			i__1 = maxwrk, i__2 = *m * *m + (*m << 1);
			maxwrk = f2cmax(i__1,i__2);
		    }
/* Computing MAX */
		    i__1 = maxwrk, i__2 = *m + lwork_sormlq__;
		    maxwrk = f2cmax(i__1,i__2);
		} else {

/*                 Path 2 - underdetermined */

/*                 Compute space needed for SGEBRD */
		    sgebrd_(m, n, &a[a_offset], lda, &s[1], dum, dum, dum, 
			    dum, &c_n1, info);
		    lwork_sgebrd__ = dum[0];
/*                 Compute space needed for SORMBR */
		    sormbr_("Q", "L", "T", m, nrhs, m, &a[a_offset], lda, dum,
			     &b[b_offset], ldb, dum, &c_n1, info);
		    lwork_sormbr__ = dum[0];
/*                 Compute space needed for SORGBR */
		    sorgbr_("P", m, n, m, &a[a_offset], lda, dum, dum, &c_n1, 
			    info);
		    lwork_sorgbr__ = dum[0];
		    maxwrk = *m * 3 + lwork_sgebrd__;
/* Computing MAX */
		    i__1 = maxwrk, i__2 = *m * 3 + lwork_sormbr__;
		    maxwrk = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = maxwrk, i__2 = *m * 3 + lwork_sorgbr__;
		    maxwrk = f2cmax(i__1,i__2);
		    maxwrk = f2cmax(maxwrk,bdspac);
/* Computing MAX */
		    i__1 = maxwrk, i__2 = *n * *nrhs;
		    maxwrk = f2cmax(i__1,i__2);
		}
	    }
	    maxwrk = f2cmax(minwrk,maxwrk);
	}
	work[1] = (real) maxwrk;

	if (*lwork < minwrk && ! lquery) {
	    *info = -12;
	}
    }

    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("SGELSS", &i__1, (ftnlen)6);
	return;
    } else if (lquery) {
	return;
    }

/*     Quick return if possible */

    if (*m == 0 || *n == 0) {
	*rank = 0;
	return;
    }

/*     Get machine parameters */

    eps = slamch_("P");
    sfmin = slamch_("S");
    smlnum = sfmin / eps;
    bignum = 1.f / smlnum;
    slabad_(&smlnum, &bignum);

/*     Scale A if f2cmax element outside range [SMLNUM,BIGNUM] */

    anrm = slange_("M", m, n, &a[a_offset], lda, &work[1]);
    iascl = 0;
    if (anrm > 0.f && anrm < smlnum) {

/*        Scale matrix norm up to SMLNUM */

	slascl_("G", &c__0, &c__0, &anrm, &smlnum, m, n, &a[a_offset], lda, 
		info);
	iascl = 1;
    } else if (anrm > bignum) {

/*        Scale matrix norm down to BIGNUM */

	slascl_("G", &c__0, &c__0, &anrm, &bignum, m, n, &a[a_offset], lda, 
		info);
	iascl = 2;
    } else if (anrm == 0.f) {

/*        Matrix all zero. Return zero solution. */

	i__1 = f2cmax(*m,*n);
	slaset_("F", &i__1, nrhs, &c_b50, &c_b50, &b[b_offset], ldb);
	slaset_("F", &minmn, &c__1, &c_b50, &c_b50, &s[1], &minmn);
	*rank = 0;
	goto L70;
    }

/*     Scale B if f2cmax element outside range [SMLNUM,BIGNUM] */

    bnrm = slange_("M", m, nrhs, &b[b_offset], ldb, &work[1]);
    ibscl = 0;
    if (bnrm > 0.f && bnrm < smlnum) {

/*        Scale matrix norm up to SMLNUM */

	slascl_("G", &c__0, &c__0, &bnrm, &smlnum, m, nrhs, &b[b_offset], ldb,
		 info);
	ibscl = 1;
    } else if (bnrm > bignum) {

/*        Scale matrix norm down to BIGNUM */

	slascl_("G", &c__0, &c__0, &bnrm, &bignum, m, nrhs, &b[b_offset], ldb,
		 info);
	ibscl = 2;
    }

/*     Overdetermined case */

    if (*m >= *n) {

/*        Path 1 - overdetermined or exactly determined */

	mm = *m;
	if (*m >= mnthr) {

/*           Path 1a - overdetermined, with many more rows than columns */

	    mm = *n;
	    itau = 1;
	    iwork = itau + *n;

/*           Compute A=Q*R */
/*           (Workspace: need 2*N, prefer N+N*NB) */

	    i__1 = *lwork - iwork + 1;
	    sgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[iwork], &i__1,
		     info);

/*           Multiply B by transpose(Q) */
/*           (Workspace: need N+NRHS, prefer N+NRHS*NB) */

	    i__1 = *lwork - iwork + 1;
	    sormqr_("L", "T", m, nrhs, n, &a[a_offset], lda, &work[itau], &b[
		    b_offset], ldb, &work[iwork], &i__1, info);

/*           Zero out below R */

	    if (*n > 1) {
		i__1 = *n - 1;
		i__2 = *n - 1;
		slaset_("L", &i__1, &i__2, &c_b50, &c_b50, &a[a_dim1 + 2], 
			lda);
	    }
	}

	ie = 1;
	itauq = ie + *n;
	itaup = itauq + *n;
	iwork = itaup + *n;

/*        Bidiagonalize R in A */
/*        (Workspace: need 3*N+MM, prefer 3*N+(MM+N)*NB) */

	i__1 = *lwork - iwork + 1;
	sgebrd_(&mm, n, &a[a_offset], lda, &s[1], &work[ie], &work[itauq], &
		work[itaup], &work[iwork], &i__1, info);

/*        Multiply B by transpose of left bidiagonalizing vectors of R */
/*        (Workspace: need 3*N+NRHS, prefer 3*N+NRHS*NB) */

	i__1 = *lwork - iwork + 1;
	sormbr_("Q", "L", "T", &mm, nrhs, n, &a[a_offset], lda, &work[itauq], 
		&b[b_offset], ldb, &work[iwork], &i__1, info);

/*        Generate right bidiagonalizing vectors of R in A */
/*        (Workspace: need 4*N-1, prefer 3*N+(N-1)*NB) */

	i__1 = *lwork - iwork + 1;
	sorgbr_("P", n, n, n, &a[a_offset], lda, &work[itaup], &work[iwork], &
		i__1, info);
	iwork = ie + *n;

/*        Perform bidiagonal QR iteration */
/*          multiply B by transpose of left singular vectors */
/*          compute right singular vectors in A */
/*        (Workspace: need BDSPAC) */

	sbdsqr_("U", n, n, &c__0, nrhs, &s[1], &work[ie], &a[a_offset], lda, 
		dum, &c__1, &b[b_offset], ldb, &work[iwork], info);
	if (*info != 0) {
	    goto L70;
	}

/*        Multiply B by reciprocals of singular values */

/* Computing MAX */
	r__1 = *rcond * s[1];
	thr = f2cmax(r__1,sfmin);
	if (*rcond < 0.f) {
/* Computing MAX */
	    r__1 = eps * s[1];
	    thr = f2cmax(r__1,sfmin);
	}
	*rank = 0;
	i__1 = *n;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    if (s[i__] > thr) {
		srscl_(nrhs, &s[i__], &b[i__ + b_dim1], ldb);
		++(*rank);
	    } else {
		slaset_("F", &c__1, nrhs, &c_b50, &c_b50, &b[i__ + b_dim1], 
			ldb);
	    }
/* L10: */
	}

/*        Multiply B by right singular vectors */
/*        (Workspace: need N, prefer N*NRHS) */

	if (*lwork >= *ldb * *nrhs && *nrhs > 1) {
	    sgemm_("T", "N", n, nrhs, n, &c_b83, &a[a_offset], lda, &b[
		    b_offset], ldb, &c_b50, &work[1], ldb);
	    slacpy_("G", n, nrhs, &work[1], ldb, &b[b_offset], ldb)
		    ;
	} else if (*nrhs > 1) {
	    chunk = *lwork / *n;
	    i__1 = *nrhs;
	    i__2 = chunk;
	    for (i__ = 1; i__2 < 0 ? i__ >= i__1 : i__ <= i__1; i__ += i__2) {
/* Computing MIN */
		i__3 = *nrhs - i__ + 1;
		bl = f2cmin(i__3,chunk);
		sgemm_("T", "N", n, &bl, n, &c_b83, &a[a_offset], lda, &b[i__ 
			* b_dim1 + 1], ldb, &c_b50, &work[1], n);
		slacpy_("G", n, &bl, &work[1], n, &b[i__ * b_dim1 + 1], ldb);
/* L20: */
	    }
	} else {
	    sgemv_("T", n, n, &c_b83, &a[a_offset], lda, &b[b_offset], &c__1, 
		    &c_b50, &work[1], &c__1);
	    scopy_(n, &work[1], &c__1, &b[b_offset], &c__1);
	}

    } else /* if(complicated condition) */ {
/* Computing MAX */
	i__2 = *m, i__1 = (*m << 1) - 4, i__2 = f2cmax(i__2,i__1), i__2 = f2cmax(
		i__2,*nrhs), i__1 = *n - *m * 3;
	if (*n >= mnthr && *lwork >= (*m << 2) + *m * *m + f2cmax(i__2,i__1)) {

/*        Path 2a - underdetermined, with many more columns than rows */
/*        and sufficient workspace for an efficient algorithm */

	    ldwork = *m;
/* Computing MAX */
/* Computing MAX */
	    i__3 = *m, i__4 = (*m << 1) - 4, i__3 = f2cmax(i__3,i__4), i__3 = 
		    f2cmax(i__3,*nrhs), i__4 = *n - *m * 3;
	    i__2 = (*m << 2) + *m * *lda + f2cmax(i__3,i__4), i__1 = *m * *lda + 
		    *m + *m * *nrhs;
	    if (*lwork >= f2cmax(i__2,i__1)) {
		ldwork = *lda;
	    }
	    itau = 1;
	    iwork = *m + 1;

/*        Compute A=L*Q */
/*        (Workspace: need 2*M, prefer M+M*NB) */

	    i__2 = *lwork - iwork + 1;
	    sgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[iwork], &i__2,
		     info);
	    il = iwork;

/*        Copy L to WORK(IL), zeroing out above it */

	    slacpy_("L", m, m, &a[a_offset], lda, &work[il], &ldwork);
	    i__2 = *m - 1;
	    i__1 = *m - 1;
	    slaset_("U", &i__2, &i__1, &c_b50, &c_b50, &work[il + ldwork], &
		    ldwork);
	    ie = il + ldwork * *m;
	    itauq = ie + *m;
	    itaup = itauq + *m;
	    iwork = itaup + *m;

/*        Bidiagonalize L in WORK(IL) */
/*        (Workspace: need M*M+5*M, prefer M*M+4*M+2*M*NB) */

	    i__2 = *lwork - iwork + 1;
	    sgebrd_(m, m, &work[il], &ldwork, &s[1], &work[ie], &work[itauq], 
		    &work[itaup], &work[iwork], &i__2, info);

/*        Multiply B by transpose of left bidiagonalizing vectors of L */
/*        (Workspace: need M*M+4*M+NRHS, prefer M*M+4*M+NRHS*NB) */

	    i__2 = *lwork - iwork + 1;
	    sormbr_("Q", "L", "T", m, nrhs, m, &work[il], &ldwork, &work[
		    itauq], &b[b_offset], ldb, &work[iwork], &i__2, info);

/*        Generate right bidiagonalizing vectors of R in WORK(IL) */
/*        (Workspace: need M*M+5*M-1, prefer M*M+4*M+(M-1)*NB) */

	    i__2 = *lwork - iwork + 1;
	    sorgbr_("P", m, m, m, &work[il], &ldwork, &work[itaup], &work[
		    iwork], &i__2, info);
	    iwork = ie + *m;

/*        Perform bidiagonal QR iteration, */
/*           computing right singular vectors of L in WORK(IL) and */
/*           multiplying B by transpose of left singular vectors */
/*        (Workspace: need M*M+M+BDSPAC) */

	    sbdsqr_("U", m, m, &c__0, nrhs, &s[1], &work[ie], &work[il], &
		    ldwork, &a[a_offset], lda, &b[b_offset], ldb, &work[iwork]
		    , info);
	    if (*info != 0) {
		goto L70;
	    }

/*        Multiply B by reciprocals of singular values */

/* Computing MAX */
	    r__1 = *rcond * s[1];
	    thr = f2cmax(r__1,sfmin);
	    if (*rcond < 0.f) {
/* Computing MAX */
		r__1 = eps * s[1];
		thr = f2cmax(r__1,sfmin);
	    }
	    *rank = 0;
	    i__2 = *m;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		if (s[i__] > thr) {
		    srscl_(nrhs, &s[i__], &b[i__ + b_dim1], ldb);
		    ++(*rank);
		} else {
		    slaset_("F", &c__1, nrhs, &c_b50, &c_b50, &b[i__ + b_dim1]
			    , ldb);
		}
/* L30: */
	    }
	    iwork = ie;

/*        Multiply B by right singular vectors of L in WORK(IL) */
/*        (Workspace: need M*M+2*M, prefer M*M+M+M*NRHS) */

	    if (*lwork >= *ldb * *nrhs + iwork - 1 && *nrhs > 1) {
		sgemm_("T", "N", m, nrhs, m, &c_b83, &work[il], &ldwork, &b[
			b_offset], ldb, &c_b50, &work[iwork], ldb);
		slacpy_("G", m, nrhs, &work[iwork], ldb, &b[b_offset], ldb);
	    } else if (*nrhs > 1) {
		chunk = (*lwork - iwork + 1) / *m;
		i__2 = *nrhs;
		i__1 = chunk;
		for (i__ = 1; i__1 < 0 ? i__ >= i__2 : i__ <= i__2; i__ += 
			i__1) {
/* Computing MIN */
		    i__3 = *nrhs - i__ + 1;
		    bl = f2cmin(i__3,chunk);
		    sgemm_("T", "N", m, &bl, m, &c_b83, &work[il], &ldwork, &
			    b[i__ * b_dim1 + 1], ldb, &c_b50, &work[iwork], m);
		    slacpy_("G", m, &bl, &work[iwork], m, &b[i__ * b_dim1 + 1]
			    , ldb);
/* L40: */
		}
	    } else {
		sgemv_("T", m, m, &c_b83, &work[il], &ldwork, &b[b_dim1 + 1], 
			&c__1, &c_b50, &work[iwork], &c__1);
		scopy_(m, &work[iwork], &c__1, &b[b_dim1 + 1], &c__1);
	    }

/*        Zero out below first M rows of B */

	    i__1 = *n - *m;
	    slaset_("F", &i__1, nrhs, &c_b50, &c_b50, &b[*m + 1 + b_dim1], 
		    ldb);
	    iwork = itau + *m;

/*        Multiply transpose(Q) by B */
/*        (Workspace: need M+NRHS, prefer M+NRHS*NB) */

	    i__1 = *lwork - iwork + 1;
	    sormlq_("L", "T", n, nrhs, m, &a[a_offset], lda, &work[itau], &b[
		    b_offset], ldb, &work[iwork], &i__1, info);

	} else {

/*        Path 2 - remaining underdetermined cases */

	    ie = 1;
	    itauq = ie + *m;
	    itaup = itauq + *m;
	    iwork = itaup + *m;

/*        Bidiagonalize A */
/*        (Workspace: need 3*M+N, prefer 3*M+(M+N)*NB) */

	    i__1 = *lwork - iwork + 1;
	    sgebrd_(m, n, &a[a_offset], lda, &s[1], &work[ie], &work[itauq], &
		    work[itaup], &work[iwork], &i__1, info);

/*        Multiply B by transpose of left bidiagonalizing vectors */
/*        (Workspace: need 3*M+NRHS, prefer 3*M+NRHS*NB) */

	    i__1 = *lwork - iwork + 1;
	    sormbr_("Q", "L", "T", m, nrhs, n, &a[a_offset], lda, &work[itauq]
		    , &b[b_offset], ldb, &work[iwork], &i__1, info);

/*        Generate right bidiagonalizing vectors in A */
/*        (Workspace: need 4*M, prefer 3*M+M*NB) */

	    i__1 = *lwork - iwork + 1;
	    sorgbr_("P", m, n, m, &a[a_offset], lda, &work[itaup], &work[
		    iwork], &i__1, info);
	    iwork = ie + *m;

/*        Perform bidiagonal QR iteration, */
/*           computing right singular vectors of A in A and */
/*           multiplying B by transpose of left singular vectors */
/*        (Workspace: need BDSPAC) */

	    sbdsqr_("L", m, n, &c__0, nrhs, &s[1], &work[ie], &a[a_offset], 
		    lda, dum, &c__1, &b[b_offset], ldb, &work[iwork], info);
	    if (*info != 0) {
		goto L70;
	    }

/*        Multiply B by reciprocals of singular values */

/* Computing MAX */
	    r__1 = *rcond * s[1];
	    thr = f2cmax(r__1,sfmin);
	    if (*rcond < 0.f) {
/* Computing MAX */
		r__1 = eps * s[1];
		thr = f2cmax(r__1,sfmin);
	    }
	    *rank = 0;
	    i__1 = *m;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		if (s[i__] > thr) {
		    srscl_(nrhs, &s[i__], &b[i__ + b_dim1], ldb);
		    ++(*rank);
		} else {
		    slaset_("F", &c__1, nrhs, &c_b50, &c_b50, &b[i__ + b_dim1]
			    , ldb);
		}
/* L50: */
	    }

/*        Multiply B by right singular vectors of A */
/*        (Workspace: need N, prefer N*NRHS) */

	    if (*lwork >= *ldb * *nrhs && *nrhs > 1) {
		sgemm_("T", "N", n, nrhs, m, &c_b83, &a[a_offset], lda, &b[
			b_offset], ldb, &c_b50, &work[1], ldb);
		slacpy_("F", n, nrhs, &work[1], ldb, &b[b_offset], ldb);
	    } else if (*nrhs > 1) {
		chunk = *lwork / *n;
		i__1 = *nrhs;
		i__2 = chunk;
		for (i__ = 1; i__2 < 0 ? i__ >= i__1 : i__ <= i__1; i__ += 
			i__2) {
/* Computing MIN */
		    i__3 = *nrhs - i__ + 1;
		    bl = f2cmin(i__3,chunk);
		    sgemm_("T", "N", n, &bl, m, &c_b83, &a[a_offset], lda, &b[
			    i__ * b_dim1 + 1], ldb, &c_b50, &work[1], n);
		    slacpy_("F", n, &bl, &work[1], n, &b[i__ * b_dim1 + 1], 
			    ldb);
/* L60: */
		}
	    } else {
		sgemv_("T", m, n, &c_b83, &a[a_offset], lda, &b[b_offset], &
			c__1, &c_b50, &work[1], &c__1);
		scopy_(n, &work[1], &c__1, &b[b_offset], &c__1);
	    }
	}
    }

/*     Undo scaling */

    if (iascl == 1) {
	slascl_("G", &c__0, &c__0, &anrm, &smlnum, n, nrhs, &b[b_offset], ldb,
		 info);
	slascl_("G", &c__0, &c__0, &smlnum, &anrm, &minmn, &c__1, &s[1], &
		minmn, info);
    } else if (iascl == 2) {
	slascl_("G", &c__0, &c__0, &anrm, &bignum, n, nrhs, &b[b_offset], ldb,
		 info);
	slascl_("G", &c__0, &c__0, &bignum, &anrm, &minmn, &c__1, &s[1], &
		minmn, info);
    }
    if (ibscl == 1) {
	slascl_("G", &c__0, &c__0, &smlnum, &bnrm, n, nrhs, &b[b_offset], ldb,
		 info);
    } else if (ibscl == 2) {
	slascl_("G", &c__0, &c__0, &bignum, &bnrm, n, nrhs, &b[b_offset], ldb,
		 info);
    }

L70:
    work[1] = (real) maxwrk;
    return;

/*     End of SGELSS */

} /* sgelss_ */

