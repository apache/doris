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
#define pow_dd(ap, bp) ( pow(*(ap), *(bp)))
#define pow_si(B,E) spow_ui(*(B),*(E))
#define pow_ri(B,E) spow_ui(*(B),*(E))
#define pow_di(B,E) dpow_ui(*(B),*(E))
#define pow_zi(p, a, b) {pCd(p) = zpow_ui(Cd(a), *(b));}
#define pow_ci(p, a, b) {pCf(p) = cpow_ui(Cf(a), *(b));}
#define pow_zz(R,A,B) {pCd(R) = cpow(Cd(A),*(B));}
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

/* procedure parameter types for -A and -C++ */



/*  -- translated by f2c (version 20000121).
   You must link the resulting object file with the libraries:
	-lf2c -lm   (in that order)
*/




/* Table of constant values */

static doublecomplex c_b1 = {0.,0.};
static doublecomplex c_b2 = {1.,0.};
static integer c__1 = 1;
static integer c_n1 = -1;

/* > \brief <b> ZGEEVX computes the eigenvalues and, optionally, the left and/or right eigenvectors for GE mat
rices</b> */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download ZGEGS + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zgegs.f
"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zgegs.f
"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zgegs.f
"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE ZGEGS( JOBVSL, JOBVSR, N, A, LDA, B, LDB, ALPHA, BETA, */
/*                         VSL, LDVSL, VSR, LDVSR, WORK, LWORK, RWORK, */
/*                         INFO ) */

/*       CHARACTER          JOBVSL, JOBVSR */
/*       INTEGER            INFO, LDA, LDB, LDVSL, LDVSR, LWORK, N */
/*       DOUBLE PRECISION   RWORK( * ) */
/*       COMPLEX*16         A( LDA, * ), ALPHA( * ), B( LDB, * ), */
/*      $                   BETA( * ), VSL( LDVSL, * ), VSR( LDVSR, * ), */
/*      $                   WORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > This routine is deprecated and has been replaced by routine ZGGES. */
/* > */
/* > ZGEGS computes the eigenvalues, Schur form, and, optionally, the */
/* > left and or/right Schur vectors of a complex matrix pair (A,B). */
/* > Given two square matrices A and B, the generalized Schur */
/* > factorization has the form */
/* > */
/* >    A = Q*S*Z**H,  B = Q*T*Z**H */
/* > */
/* > where Q and Z are unitary matrices and S and T are upper triangular. */
/* > The columns of Q are the left Schur vectors */
/* > and the columns of Z are the right Schur vectors. */
/* > */
/* > If only the eigenvalues of (A,B) are needed, the driver routine */
/* > ZGEGV should be used instead.  See ZGEGV for a description of the */
/* > eigenvalues of the generalized nonsymmetric eigenvalue problem */
/* > (GNEP). */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] JOBVSL */
/* > \verbatim */
/* >          JOBVSL is CHARACTER*1 */
/* >          = 'N':  do not compute the left Schur vectors; */
/* >          = 'V':  compute the left Schur vectors (returned in VSL). */
/* > \endverbatim */
/* > */
/* > \param[in] JOBVSR */
/* > \verbatim */
/* >          JOBVSR is CHARACTER*1 */
/* >          = 'N':  do not compute the right Schur vectors; */
/* >          = 'V':  compute the right Schur vectors (returned in VSR). */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrices A, B, VSL, and VSR.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] A */
/* > \verbatim */
/* >          A is COMPLEX*16 array, dimension (LDA, N) */
/* >          On entry, the matrix A. */
/* >          On exit, the upper triangular matrix S from the generalized */
/* >          Schur factorization. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of A.  LDA >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in,out] B */
/* > \verbatim */
/* >          B is COMPLEX*16 array, dimension (LDB, N) */
/* >          On entry, the matrix B. */
/* >          On exit, the upper triangular matrix T from the generalized */
/* >          Schur factorization. */
/* > \endverbatim */
/* > */
/* > \param[in] LDB */
/* > \verbatim */
/* >          LDB is INTEGER */
/* >          The leading dimension of B.  LDB >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[out] ALPHA */
/* > \verbatim */
/* >          ALPHA is COMPLEX*16 array, dimension (N) */
/* >          The complex scalars alpha that define the eigenvalues of */
/* >          GNEP.  ALPHA(j) = S(j,j), the diagonal element of the Schur */
/* >          form of A. */
/* > \endverbatim */
/* > */
/* > \param[out] BETA */
/* > \verbatim */
/* >          BETA is COMPLEX*16 array, dimension (N) */
/* >          The non-negative real scalars beta that define the */
/* >          eigenvalues of GNEP.  BETA(j) = T(j,j), the diagonal element */
/* >          of the triangular factor T. */
/* > */
/* >          Together, the quantities alpha = ALPHA(j) and beta = BETA(j) */
/* >          represent the j-th eigenvalue of the matrix pair (A,B), in */
/* >          one of the forms lambda = alpha/beta or mu = beta/alpha. */
/* >          Since either lambda or mu may overflow, they should not, */
/* >          in general, be computed. */
/* > \endverbatim */
/* > */
/* > \param[out] VSL */
/* > \verbatim */
/* >          VSL is COMPLEX*16 array, dimension (LDVSL,N) */
/* >          If JOBVSL = 'V', the matrix of left Schur vectors Q. */
/* >          Not referenced if JOBVSL = 'N'. */
/* > \endverbatim */
/* > */
/* > \param[in] LDVSL */
/* > \verbatim */
/* >          LDVSL is INTEGER */
/* >          The leading dimension of the matrix VSL. LDVSL >= 1, and */
/* >          if JOBVSL = 'V', LDVSL >= N. */
/* > \endverbatim */
/* > */
/* > \param[out] VSR */
/* > \verbatim */
/* >          VSR is COMPLEX*16 array, dimension (LDVSR,N) */
/* >          If JOBVSR = 'V', the matrix of right Schur vectors Z. */
/* >          Not referenced if JOBVSR = 'N'. */
/* > \endverbatim */
/* > */
/* > \param[in] LDVSR */
/* > \verbatim */
/* >          LDVSR is INTEGER */
/* >          The leading dimension of the matrix VSR. LDVSR >= 1, and */
/* >          if JOBVSR = 'V', LDVSR >= N. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is COMPLEX*16 array, dimension (MAX(1,LWORK)) */
/* >          On exit, if INFO = 0, WORK(1) returns the optimal LWORK. */
/* > \endverbatim */
/* > */
/* > \param[in] LWORK */
/* > \verbatim */
/* >          LWORK is INTEGER */
/* >          The dimension of the array WORK.  LWORK >= f2cmax(1,2*N). */
/* >          For good performance, LWORK must generally be larger. */
/* >          To compute the optimal value of LWORK, call ILAENV to get */
/* >          blocksizes (for ZGEQRF, ZUNMQR, and CUNGQR.)  Then compute: */
/* >          NB  -- MAX of the blocksizes for ZGEQRF, ZUNMQR, and CUNGQR; */
/* >          the optimal LWORK is N*(NB+1). */
/* > */
/* >          If LWORK = -1, then a workspace query is assumed; the routine */
/* >          only calculates the optimal size of the WORK array, returns */
/* >          this value as the first entry of the WORK array, and no error */
/* >          message related to LWORK is issued by XERBLA. */
/* > \endverbatim */
/* > */
/* > \param[out] RWORK */
/* > \verbatim */
/* >          RWORK is DOUBLE PRECISION array, dimension (3*N) */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value. */
/* >          =1,...,N: */
/* >                The QZ iteration failed.  (A,B) are not in Schur */
/* >                form, but ALPHA(j) and BETA(j) should be correct for */
/* >                j=INFO+1,...,N. */
/* >          > N:  errors that usually indicate LAPACK problems: */
/* >                =N+1: error return from ZGGBAL */
/* >                =N+2: error return from ZGEQRF */
/* >                =N+3: error return from ZUNMQR */
/* >                =N+4: error return from ZUNGQR */
/* >                =N+5: error return from ZGGHRD */
/* >                =N+6: error return from ZHGEQZ (other than failed */
/* >                                               iteration) */
/* >                =N+7: error return from ZGGBAK (computing VSL) */
/* >                =N+8: error return from ZGGBAK (computing VSR) */
/* >                =N+9: error return from ZLASCL (various places) */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup complex16GEeigen */

/*  ===================================================================== */
/* Subroutine */ void zgegs_(char *jobvsl, char *jobvsr, integer *n, 
	doublecomplex *a, integer *lda, doublecomplex *b, integer *ldb, 
	doublecomplex *alpha, doublecomplex *beta, doublecomplex *vsl, 
	integer *ldvsl, doublecomplex *vsr, integer *ldvsr, doublecomplex *
	work, integer *lwork, doublereal *rwork, integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, vsl_dim1, vsl_offset, 
	    vsr_dim1, vsr_offset, i__1, i__2, i__3;

    /* Local variables */
    doublereal anrm, bnrm;
    integer itau, lopt;
    extern logical lsame_(char *, char *);
    integer ileft, iinfo, icols;
    logical ilvsl;
    integer iwork;
    logical ilvsr;
    integer irows, nb;
    extern doublereal dlamch_(char *);
    extern /* Subroutine */ void zggbak_(char *, char *, integer *, integer *, 
	    integer *, doublereal *, doublereal *, integer *, doublecomplex *,
	     integer *, integer *), zggbal_(char *, integer *,
	     doublecomplex *, integer *, doublecomplex *, integer *, integer *
	    , integer *, doublereal *, doublereal *, doublereal *, integer *);
    logical ilascl, ilbscl;
    doublereal safmin;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    extern integer ilaenv_(integer *, char *, char *, integer *, integer *, 
	    integer *, integer *, ftnlen, ftnlen);
    extern doublereal zlange_(char *, integer *, integer *, doublecomplex *, 
	    integer *, doublereal *);
    doublereal bignum;
    integer ijobvl, iright;
    extern /* Subroutine */ void zgghrd_(char *, char *, integer *, integer *, 
	    integer *, doublecomplex *, integer *, doublecomplex *, integer *,
	     doublecomplex *, integer *, doublecomplex *, integer *, integer *
	    ), zlascl_(char *, integer *, integer *, 
	    doublereal *, doublereal *, integer *, integer *, doublecomplex *,
	     integer *, integer *);
    integer ijobvr;
    extern /* Subroutine */ void zgeqrf_(integer *, integer *, doublecomplex *,
	     integer *, doublecomplex *, doublecomplex *, integer *, integer *
	    );
    doublereal anrmto;
    integer lwkmin, nb1, nb2, nb3;
    doublereal bnrmto;
    extern /* Subroutine */ void zlacpy_(char *, integer *, integer *, 
	    doublecomplex *, integer *, doublecomplex *, integer *), 
	    zhgeqz_(char *, char *, char *, integer *, integer *, integer *, 
	    doublecomplex *, integer *, doublecomplex *, integer *, 
	    doublecomplex *, doublecomplex *, doublecomplex *, integer *, 
	    doublecomplex *, integer *, doublecomplex *, integer *, 
	    doublereal *, integer *), zlaset_(char *, 
	    integer *, integer *, doublecomplex *, doublecomplex *, 
	    doublecomplex *, integer *);
    doublereal smlnum;
    integer irwork, lwkopt;
    logical lquery;
    extern /* Subroutine */ void zungqr_(integer *, integer *, integer *, 
	    doublecomplex *, integer *, doublecomplex *, doublecomplex *, 
	    integer *, integer *), zunmqr_(char *, char *, integer *, integer 
	    *, integer *, doublecomplex *, integer *, doublecomplex *, 
	    doublecomplex *, integer *, doublecomplex *, integer *, integer *);
    integer ihi, ilo;
    doublereal eps;


/*  -- LAPACK driver routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


/*     Decode the input arguments */

    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    b_dim1 = *ldb;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    --alpha;
    --beta;
    vsl_dim1 = *ldvsl;
    vsl_offset = 1 + vsl_dim1 * 1;
    vsl -= vsl_offset;
    vsr_dim1 = *ldvsr;
    vsr_offset = 1 + vsr_dim1 * 1;
    vsr -= vsr_offset;
    --work;
    --rwork;

    /* Function Body */
    if (lsame_(jobvsl, "N")) {
	ijobvl = 1;
	ilvsl = FALSE_;
    } else if (lsame_(jobvsl, "V")) {
	ijobvl = 2;
	ilvsl = TRUE_;
    } else {
	ijobvl = -1;
	ilvsl = FALSE_;
    }

    if (lsame_(jobvsr, "N")) {
	ijobvr = 1;
	ilvsr = FALSE_;
    } else if (lsame_(jobvsr, "V")) {
	ijobvr = 2;
	ilvsr = TRUE_;
    } else {
	ijobvr = -1;
	ilvsr = FALSE_;
    }

/*     Test the input arguments */

/* Computing MAX */
    i__1 = *n << 1;
    lwkmin = f2cmax(i__1,1);
    lwkopt = lwkmin;
    work[1].r = (doublereal) lwkopt, work[1].i = 0.;
    lquery = *lwork == -1;
    *info = 0;
    if (ijobvl <= 0) {
	*info = -1;
    } else if (ijobvr <= 0) {
	*info = -2;
    } else if (*n < 0) {
	*info = -3;
    } else if (*lda < f2cmax(1,*n)) {
	*info = -5;
    } else if (*ldb < f2cmax(1,*n)) {
	*info = -7;
    } else if (*ldvsl < 1 || ilvsl && *ldvsl < *n) {
	*info = -11;
    } else if (*ldvsr < 1 || ilvsr && *ldvsr < *n) {
	*info = -13;
    } else if (*lwork < lwkmin && ! lquery) {
	*info = -15;
    }

    if (*info == 0) {
	nb1 = ilaenv_(&c__1, "ZGEQRF", " ", n, n, &c_n1, &c_n1, (ftnlen)6, (
		ftnlen)1);
	nb2 = ilaenv_(&c__1, "ZUNMQR", " ", n, n, n, &c_n1, (ftnlen)6, (
		ftnlen)1);
	nb3 = ilaenv_(&c__1, "ZUNGQR", " ", n, n, n, &c_n1, (ftnlen)6, (
		ftnlen)1);
/* Computing MAX */
	i__1 = f2cmax(nb1,nb2);
	nb = f2cmax(i__1,nb3);
	lopt = *n * (nb + 1);
	work[1].r = (doublereal) lopt, work[1].i = 0.;
    }

    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("ZGEGS ", &i__1, 6);
	return;
    } else if (lquery) {
	return;
    }

/*     Quick return if possible */

    if (*n == 0) {
	return;
    }

/*     Get machine constants */

    eps = dlamch_("E") * dlamch_("B");
    safmin = dlamch_("S");
    smlnum = *n * safmin / eps;
    bignum = 1. / smlnum;

/*     Scale A if f2cmax element outside range [SMLNUM,BIGNUM] */

    anrm = zlange_("M", n, n, &a[a_offset], lda, &rwork[1]);
    ilascl = FALSE_;
    if (anrm > 0. && anrm < smlnum) {
	anrmto = smlnum;
	ilascl = TRUE_;
    } else if (anrm > bignum) {
	anrmto = bignum;
	ilascl = TRUE_;
    }

    if (ilascl) {
	zlascl_("G", &c_n1, &c_n1, &anrm, &anrmto, n, n, &a[a_offset], lda, &
		iinfo);
	if (iinfo != 0) {
	    *info = *n + 9;
	    return;
	}
    }

/*     Scale B if f2cmax element outside range [SMLNUM,BIGNUM] */

    bnrm = zlange_("M", n, n, &b[b_offset], ldb, &rwork[1]);
    ilbscl = FALSE_;
    if (bnrm > 0. && bnrm < smlnum) {
	bnrmto = smlnum;
	ilbscl = TRUE_;
    } else if (bnrm > bignum) {
	bnrmto = bignum;
	ilbscl = TRUE_;
    }

    if (ilbscl) {
	zlascl_("G", &c_n1, &c_n1, &bnrm, &bnrmto, n, n, &b[b_offset], ldb, &
		iinfo);
	if (iinfo != 0) {
	    *info = *n + 9;
	    return;
	}
    }

/*     Permute the matrix to make it more nearly triangular */

    ileft = 1;
    iright = *n + 1;
    irwork = iright + *n;
    iwork = 1;
    zggbal_("P", n, &a[a_offset], lda, &b[b_offset], ldb, &ilo, &ihi, &rwork[
	    ileft], &rwork[iright], &rwork[irwork], &iinfo);
    if (iinfo != 0) {
	*info = *n + 1;
	goto L10;
    }

/*     Reduce B to triangular form, and initialize VSL and/or VSR */

    irows = ihi + 1 - ilo;
    icols = *n + 1 - ilo;
    itau = iwork;
    iwork = itau + irows;
    i__1 = *lwork + 1 - iwork;
    zgeqrf_(&irows, &icols, &b[ilo + ilo * b_dim1], ldb, &work[itau], &work[
	    iwork], &i__1, &iinfo);
    if (iinfo >= 0) {
/* Computing MAX */
	i__3 = iwork;
	i__1 = lwkopt, i__2 = (integer) work[i__3].r + iwork - 1;
	lwkopt = f2cmax(i__1,i__2);
    }
    if (iinfo != 0) {
	*info = *n + 2;
	goto L10;
    }

    i__1 = *lwork + 1 - iwork;
    zunmqr_("L", "C", &irows, &icols, &irows, &b[ilo + ilo * b_dim1], ldb, &
	    work[itau], &a[ilo + ilo * a_dim1], lda, &work[iwork], &i__1, &
	    iinfo);
    if (iinfo >= 0) {
/* Computing MAX */
	i__3 = iwork;
	i__1 = lwkopt, i__2 = (integer) work[i__3].r + iwork - 1;
	lwkopt = f2cmax(i__1,i__2);
    }
    if (iinfo != 0) {
	*info = *n + 3;
	goto L10;
    }

    if (ilvsl) {
	zlaset_("Full", n, n, &c_b1, &c_b2, &vsl[vsl_offset], ldvsl);
	i__1 = irows - 1;
	i__2 = irows - 1;
	zlacpy_("L", &i__1, &i__2, &b[ilo + 1 + ilo * b_dim1], ldb, &vsl[ilo 
		+ 1 + ilo * vsl_dim1], ldvsl);
	i__1 = *lwork + 1 - iwork;
	zungqr_(&irows, &irows, &irows, &vsl[ilo + ilo * vsl_dim1], ldvsl, &
		work[itau], &work[iwork], &i__1, &iinfo);
	if (iinfo >= 0) {
/* Computing MAX */
	    i__3 = iwork;
	    i__1 = lwkopt, i__2 = (integer) work[i__3].r + iwork - 1;
	    lwkopt = f2cmax(i__1,i__2);
	}
	if (iinfo != 0) {
	    *info = *n + 4;
	    goto L10;
	}
    }

    if (ilvsr) {
	zlaset_("Full", n, n, &c_b1, &c_b2, &vsr[vsr_offset], ldvsr);
    }

/*     Reduce to generalized Hessenberg form */

    zgghrd_(jobvsl, jobvsr, n, &ilo, &ihi, &a[a_offset], lda, &b[b_offset], 
	    ldb, &vsl[vsl_offset], ldvsl, &vsr[vsr_offset], ldvsr, &iinfo);
    if (iinfo != 0) {
	*info = *n + 5;
	goto L10;
    }

/*     Perform QZ algorithm, computing Schur vectors if desired */

    iwork = itau;
    i__1 = *lwork + 1 - iwork;
    zhgeqz_("S", jobvsl, jobvsr, n, &ilo, &ihi, &a[a_offset], lda, &b[
	    b_offset], ldb, &alpha[1], &beta[1], &vsl[vsl_offset], ldvsl, &
	    vsr[vsr_offset], ldvsr, &work[iwork], &i__1, &rwork[irwork], &
	    iinfo);
    if (iinfo >= 0) {
/* Computing MAX */
	i__3 = iwork;
	i__1 = lwkopt, i__2 = (integer) work[i__3].r + iwork - 1;
	lwkopt = f2cmax(i__1,i__2);
    }
    if (iinfo != 0) {
	if (iinfo > 0 && iinfo <= *n) {
	    *info = iinfo;
	} else if (iinfo > *n && iinfo <= *n << 1) {
	    *info = iinfo - *n;
	} else {
	    *info = *n + 6;
	}
	goto L10;
    }

/*     Apply permutation to VSL and VSR */

    if (ilvsl) {
	zggbak_("P", "L", n, &ilo, &ihi, &rwork[ileft], &rwork[iright], n, &
		vsl[vsl_offset], ldvsl, &iinfo);
	if (iinfo != 0) {
	    *info = *n + 7;
	    goto L10;
	}
    }
    if (ilvsr) {
	zggbak_("P", "R", n, &ilo, &ihi, &rwork[ileft], &rwork[iright], n, &
		vsr[vsr_offset], ldvsr, &iinfo);
	if (iinfo != 0) {
	    *info = *n + 8;
	    goto L10;
	}
    }

/*     Undo scaling */

    if (ilascl) {
	zlascl_("U", &c_n1, &c_n1, &anrmto, &anrm, n, n, &a[a_offset], lda, &
		iinfo);
	if (iinfo != 0) {
	    *info = *n + 9;
	    return;
	}
	zlascl_("G", &c_n1, &c_n1, &anrmto, &anrm, n, &c__1, &alpha[1], n, &
		iinfo);
	if (iinfo != 0) {
	    *info = *n + 9;
	    return;
	}
    }

    if (ilbscl) {
	zlascl_("U", &c_n1, &c_n1, &bnrmto, &bnrm, n, n, &b[b_offset], ldb, &
		iinfo);
	if (iinfo != 0) {
	    *info = *n + 9;
	    return;
	}
	zlascl_("G", &c_n1, &c_n1, &bnrmto, &bnrm, n, &c__1, &beta[1], n, &
		iinfo);
	if (iinfo != 0) {
	    *info = *n + 9;
	    return;
	}
    }

L10:
    work[1].r = (doublereal) lwkopt, work[1].i = 0.;

    return;

/*     End of ZGEGS */

} /* zgegs_ */

