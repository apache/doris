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




/* Table of constant values */

static integer c__1 = 1;
static integer c__4 = 4;
static integer c__8 = 8;
static integer c__24 = 24;

/* > \brief \b CLATM6 */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE CLATM6( TYPE, N, A, LDA, B, X, LDX, Y, LDY, ALPHA, */
/*                          BETA, WX, WY, S, DIF ) */

/*       INTEGER            LDA, LDX, LDY, N, TYPE */
/*       COMPLEX            ALPHA, BETA, WX, WY */
/*       REAL               DIF( * ), S( * ) */
/*       COMPLEX            A( LDA, * ), B( LDA, * ), X( LDX, * ), */
/*      $                   Y( LDY, * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > CLATM6 generates test matrices for the generalized eigenvalue */
/* > problem, their corresponding right and left eigenvector matrices, */
/* > and also reciprocal condition numbers for all eigenvalues and */
/* > the reciprocal condition numbers of eigenvectors corresponding to */
/* > the 1th and 5th eigenvalues. */
/* > */
/* > Test Matrices */
/* > ============= */
/* > */
/* > Two kinds of test matrix pairs */
/* >          (A, B) = inverse(YH) * (Da, Db) * inverse(X) */
/* > are used in the tests: */
/* > */
/* > Type 1: */
/* >    Da = 1+a   0    0    0    0    Db = 1   0   0   0   0 */
/* >          0   2+a   0    0    0         0   1   0   0   0 */
/* >          0    0   3+a   0    0         0   0   1   0   0 */
/* >          0    0    0   4+a   0         0   0   0   1   0 */
/* >          0    0    0    0   5+a ,      0   0   0   0   1 */
/* > and Type 2: */
/* >    Da = 1+i   0    0       0       0    Db = 1   0   0   0   0 */
/* >          0   1-i   0       0       0         0   1   0   0   0 */
/* >          0    0    1       0       0         0   0   1   0   0 */
/* >          0    0    0 (1+a)+(1+b)i  0         0   0   0   1   0 */
/* >          0    0    0       0 (1+a)-(1+b)i,   0   0   0   0   1 . */
/* > */
/* > In both cases the same inverse(YH) and inverse(X) are used to compute */
/* > (A, B), giving the exact eigenvectors to (A,B) as (YH, X): */
/* > */
/* > YH:  =  1    0   -y    y   -y    X =  1   0  -x  -x   x */
/* >         0    1   -y    y   -y         0   1   x  -x  -x */
/* >         0    0    1    0    0         0   0   1   0   0 */
/* >         0    0    0    1    0         0   0   0   1   0 */
/* >         0    0    0    0    1,        0   0   0   0   1 , where */
/* > */
/* > a, b, x and y will have all values independently of each other. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] TYPE */
/* > \verbatim */
/* >          TYPE is INTEGER */
/* >          Specifies the problem type (see further details). */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          Size of the matrices A and B. */
/* > \endverbatim */
/* > */
/* > \param[out] A */
/* > \verbatim */
/* >          A is COMPLEX array, dimension (LDA, N). */
/* >          On exit A N-by-N is initialized according to TYPE. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of A and of B. */
/* > \endverbatim */
/* > */
/* > \param[out] B */
/* > \verbatim */
/* >          B is COMPLEX array, dimension (LDA, N). */
/* >          On exit B N-by-N is initialized according to TYPE. */
/* > \endverbatim */
/* > */
/* > \param[out] X */
/* > \verbatim */
/* >          X is COMPLEX array, dimension (LDX, N). */
/* >          On exit X is the N-by-N matrix of right eigenvectors. */
/* > \endverbatim */
/* > */
/* > \param[in] LDX */
/* > \verbatim */
/* >          LDX is INTEGER */
/* >          The leading dimension of X. */
/* > \endverbatim */
/* > */
/* > \param[out] Y */
/* > \verbatim */
/* >          Y is COMPLEX array, dimension (LDY, N). */
/* >          On exit Y is the N-by-N matrix of left eigenvectors. */
/* > \endverbatim */
/* > */
/* > \param[in] LDY */
/* > \verbatim */
/* >          LDY is INTEGER */
/* >          The leading dimension of Y. */
/* > \endverbatim */
/* > */
/* > \param[in] ALPHA */
/* > \verbatim */
/* >          ALPHA is COMPLEX */
/* > \endverbatim */
/* > */
/* > \param[in] BETA */
/* > \verbatim */
/* >          BETA is COMPLEX */
/* > */
/* >          Weighting constants for matrix A. */
/* > \endverbatim */
/* > */
/* > \param[in] WX */
/* > \verbatim */
/* >          WX is COMPLEX */
/* >          Constant for right eigenvector matrix. */
/* > \endverbatim */
/* > */
/* > \param[in] WY */
/* > \verbatim */
/* >          WY is COMPLEX */
/* >          Constant for left eigenvector matrix. */
/* > \endverbatim */
/* > */
/* > \param[out] S */
/* > \verbatim */
/* >          S is REAL array, dimension (N) */
/* >          S(i) is the reciprocal condition number for eigenvalue i. */
/* > \endverbatim */
/* > */
/* > \param[out] DIF */
/* > \verbatim */
/* >          DIF is REAL array, dimension (N) */
/* >          DIF(i) is the reciprocal condition number for eigenvector i. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup complex_matgen */

/*  ===================================================================== */
/* Subroutine */ void clatm6_(integer *type__, integer *n, complex *a, integer 
	*lda, complex *b, complex *x, integer *ldx, complex *y, integer *ldy, 
	complex *alpha, complex *beta, complex *wx, complex *wy, real *s, 
	real *dif)
{
    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, x_dim1, x_offset, y_dim1, 
	    y_offset, i__1, i__2, i__3;
    real r__1, r__2;
    complex q__1, q__2, q__3, q__4;

    /* Local variables */
    integer info;
    complex work[26];
    integer i__, j;
    complex z__[64]	/* was [8][8] */;
    extern /* Subroutine */ void clakf2_(integer *, integer *, complex *, 
	    integer *, complex *, complex *, complex *, complex *, integer *);
    real rwork[50];
    extern /* Subroutine */ void cgesvd_(char *, char *, integer *, integer *, 
	    complex *, integer *, real *, complex *, integer *, complex *, 
	    integer *, complex *, integer *, real *, integer *), clacpy_(char *, integer *, integer *, complex *, integer 
	    *, complex *, integer *);


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


/*     Generate test problem ... */
/*     (Da, Db) ... */

    /* Parameter adjustments */
    b_dim1 = *lda;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    x_dim1 = *ldx;
    x_offset = 1 + x_dim1 * 1;
    x -= x_offset;
    y_dim1 = *ldy;
    y_offset = 1 + y_dim1 * 1;
    y -= y_offset;
    --s;
    --dif;

    /* Function Body */
    i__1 = *n;
    for (i__ = 1; i__ <= i__1; ++i__) {
	i__2 = *n;
	for (j = 1; j <= i__2; ++j) {

	    if (i__ == j) {
		i__3 = i__ + i__ * a_dim1;
		q__2.r = (real) i__, q__2.i = 0.f;
		q__1.r = q__2.r + alpha->r, q__1.i = q__2.i + alpha->i;
		a[i__3].r = q__1.r, a[i__3].i = q__1.i;
		i__3 = i__ + i__ * b_dim1;
		b[i__3].r = 1.f, b[i__3].i = 0.f;
	    } else {
		i__3 = i__ + j * a_dim1;
		a[i__3].r = 0.f, a[i__3].i = 0.f;
		i__3 = i__ + j * b_dim1;
		b[i__3].r = 0.f, b[i__3].i = 0.f;
	    }

/* L10: */
	}
/* L20: */
    }
    if (*type__ == 2) {
	i__1 = a_dim1 + 1;
	a[i__1].r = 1.f, a[i__1].i = 1.f;
	i__1 = (a_dim1 << 1) + 2;
	r_cnjg(&q__1, &a[a_dim1 + 1]);
	a[i__1].r = q__1.r, a[i__1].i = q__1.i;
	i__1 = a_dim1 * 3 + 3;
	a[i__1].r = 1.f, a[i__1].i = 0.f;
	i__1 = (a_dim1 << 2) + 4;
	q__2.r = alpha->r + 1.f, q__2.i = alpha->i + 0.f;
	r__1 = q__2.r;
	q__3.r = beta->r + 1.f, q__3.i = beta->i + 0.f;
	r__2 = q__3.r;
	q__1.r = r__1, q__1.i = r__2;
	a[i__1].r = q__1.r, a[i__1].i = q__1.i;
	i__1 = a_dim1 * 5 + 5;
	r_cnjg(&q__1, &a[(a_dim1 << 2) + 4]);
	a[i__1].r = q__1.r, a[i__1].i = q__1.i;
    }

/*     Form X and Y */

    clacpy_("F", n, n, &b[b_offset], lda, &y[y_offset], ldy);
    i__1 = y_dim1 + 3;
    r_cnjg(&q__2, wy);
    q__1.r = -q__2.r, q__1.i = -q__2.i;
    y[i__1].r = q__1.r, y[i__1].i = q__1.i;
    i__1 = y_dim1 + 4;
    r_cnjg(&q__1, wy);
    y[i__1].r = q__1.r, y[i__1].i = q__1.i;
    i__1 = y_dim1 + 5;
    r_cnjg(&q__2, wy);
    q__1.r = -q__2.r, q__1.i = -q__2.i;
    y[i__1].r = q__1.r, y[i__1].i = q__1.i;
    i__1 = (y_dim1 << 1) + 3;
    r_cnjg(&q__2, wy);
    q__1.r = -q__2.r, q__1.i = -q__2.i;
    y[i__1].r = q__1.r, y[i__1].i = q__1.i;
    i__1 = (y_dim1 << 1) + 4;
    r_cnjg(&q__1, wy);
    y[i__1].r = q__1.r, y[i__1].i = q__1.i;
    i__1 = (y_dim1 << 1) + 5;
    r_cnjg(&q__2, wy);
    q__1.r = -q__2.r, q__1.i = -q__2.i;
    y[i__1].r = q__1.r, y[i__1].i = q__1.i;

    clacpy_("F", n, n, &b[b_offset], lda, &x[x_offset], ldx);
    i__1 = x_dim1 * 3 + 1;
    q__1.r = -wx->r, q__1.i = -wx->i;
    x[i__1].r = q__1.r, x[i__1].i = q__1.i;
    i__1 = (x_dim1 << 2) + 1;
    q__1.r = -wx->r, q__1.i = -wx->i;
    x[i__1].r = q__1.r, x[i__1].i = q__1.i;
    i__1 = x_dim1 * 5 + 1;
    x[i__1].r = wx->r, x[i__1].i = wx->i;
    i__1 = x_dim1 * 3 + 2;
    x[i__1].r = wx->r, x[i__1].i = wx->i;
    i__1 = (x_dim1 << 2) + 2;
    q__1.r = -wx->r, q__1.i = -wx->i;
    x[i__1].r = q__1.r, x[i__1].i = q__1.i;
    i__1 = x_dim1 * 5 + 2;
    q__1.r = -wx->r, q__1.i = -wx->i;
    x[i__1].r = q__1.r, x[i__1].i = q__1.i;

/*     Form (A, B) */

    i__1 = b_dim1 * 3 + 1;
    q__1.r = wx->r + wy->r, q__1.i = wx->i + wy->i;
    b[i__1].r = q__1.r, b[i__1].i = q__1.i;
    i__1 = b_dim1 * 3 + 2;
    q__2.r = -wx->r, q__2.i = -wx->i;
    q__1.r = q__2.r + wy->r, q__1.i = q__2.i + wy->i;
    b[i__1].r = q__1.r, b[i__1].i = q__1.i;
    i__1 = (b_dim1 << 2) + 1;
    q__1.r = wx->r - wy->r, q__1.i = wx->i - wy->i;
    b[i__1].r = q__1.r, b[i__1].i = q__1.i;
    i__1 = (b_dim1 << 2) + 2;
    q__1.r = wx->r - wy->r, q__1.i = wx->i - wy->i;
    b[i__1].r = q__1.r, b[i__1].i = q__1.i;
    i__1 = b_dim1 * 5 + 1;
    q__2.r = -wx->r, q__2.i = -wx->i;
    q__1.r = q__2.r + wy->r, q__1.i = q__2.i + wy->i;
    b[i__1].r = q__1.r, b[i__1].i = q__1.i;
    i__1 = b_dim1 * 5 + 2;
    q__1.r = wx->r + wy->r, q__1.i = wx->i + wy->i;
    b[i__1].r = q__1.r, b[i__1].i = q__1.i;
    i__1 = a_dim1 * 3 + 1;
    i__2 = a_dim1 + 1;
    q__2.r = wx->r * a[i__2].r - wx->i * a[i__2].i, q__2.i = wx->r * a[i__2]
	    .i + wx->i * a[i__2].r;
    i__3 = a_dim1 * 3 + 3;
    q__3.r = wy->r * a[i__3].r - wy->i * a[i__3].i, q__3.i = wy->r * a[i__3]
	    .i + wy->i * a[i__3].r;
    q__1.r = q__2.r + q__3.r, q__1.i = q__2.i + q__3.i;
    a[i__1].r = q__1.r, a[i__1].i = q__1.i;
    i__1 = a_dim1 * 3 + 2;
    q__3.r = -wx->r, q__3.i = -wx->i;
    i__2 = (a_dim1 << 1) + 2;
    q__2.r = q__3.r * a[i__2].r - q__3.i * a[i__2].i, q__2.i = q__3.r * a[
	    i__2].i + q__3.i * a[i__2].r;
    i__3 = a_dim1 * 3 + 3;
    q__4.r = wy->r * a[i__3].r - wy->i * a[i__3].i, q__4.i = wy->r * a[i__3]
	    .i + wy->i * a[i__3].r;
    q__1.r = q__2.r + q__4.r, q__1.i = q__2.i + q__4.i;
    a[i__1].r = q__1.r, a[i__1].i = q__1.i;
    i__1 = (a_dim1 << 2) + 1;
    i__2 = a_dim1 + 1;
    q__2.r = wx->r * a[i__2].r - wx->i * a[i__2].i, q__2.i = wx->r * a[i__2]
	    .i + wx->i * a[i__2].r;
    i__3 = (a_dim1 << 2) + 4;
    q__3.r = wy->r * a[i__3].r - wy->i * a[i__3].i, q__3.i = wy->r * a[i__3]
	    .i + wy->i * a[i__3].r;
    q__1.r = q__2.r - q__3.r, q__1.i = q__2.i - q__3.i;
    a[i__1].r = q__1.r, a[i__1].i = q__1.i;
    i__1 = (a_dim1 << 2) + 2;
    i__2 = (a_dim1 << 1) + 2;
    q__2.r = wx->r * a[i__2].r - wx->i * a[i__2].i, q__2.i = wx->r * a[i__2]
	    .i + wx->i * a[i__2].r;
    i__3 = (a_dim1 << 2) + 4;
    q__3.r = wy->r * a[i__3].r - wy->i * a[i__3].i, q__3.i = wy->r * a[i__3]
	    .i + wy->i * a[i__3].r;
    q__1.r = q__2.r - q__3.r, q__1.i = q__2.i - q__3.i;
    a[i__1].r = q__1.r, a[i__1].i = q__1.i;
    i__1 = a_dim1 * 5 + 1;
    q__3.r = -wx->r, q__3.i = -wx->i;
    i__2 = a_dim1 + 1;
    q__2.r = q__3.r * a[i__2].r - q__3.i * a[i__2].i, q__2.i = q__3.r * a[
	    i__2].i + q__3.i * a[i__2].r;
    i__3 = a_dim1 * 5 + 5;
    q__4.r = wy->r * a[i__3].r - wy->i * a[i__3].i, q__4.i = wy->r * a[i__3]
	    .i + wy->i * a[i__3].r;
    q__1.r = q__2.r + q__4.r, q__1.i = q__2.i + q__4.i;
    a[i__1].r = q__1.r, a[i__1].i = q__1.i;
    i__1 = a_dim1 * 5 + 2;
    i__2 = (a_dim1 << 1) + 2;
    q__2.r = wx->r * a[i__2].r - wx->i * a[i__2].i, q__2.i = wx->r * a[i__2]
	    .i + wx->i * a[i__2].r;
    i__3 = a_dim1 * 5 + 5;
    q__3.r = wy->r * a[i__3].r - wy->i * a[i__3].i, q__3.i = wy->r * a[i__3]
	    .i + wy->i * a[i__3].r;
    q__1.r = q__2.r + q__3.r, q__1.i = q__2.i + q__3.i;
    a[i__1].r = q__1.r, a[i__1].i = q__1.i;

/*     Compute condition numbers */

    s[1] = 1.f / sqrt((c_abs(wy) * 3.f * c_abs(wy) + 1.f) / (c_abs(&a[a_dim1 
	    + 1]) * c_abs(&a[a_dim1 + 1]) + 1.f));
    s[2] = 1.f / sqrt((c_abs(wy) * 3.f * c_abs(wy) + 1.f) / (c_abs(&a[(a_dim1 
	    << 1) + 2]) * c_abs(&a[(a_dim1 << 1) + 2]) + 1.f));
    s[3] = 1.f / sqrt((c_abs(wx) * 2.f * c_abs(wx) + 1.f) / (c_abs(&a[a_dim1 *
	     3 + 3]) * c_abs(&a[a_dim1 * 3 + 3]) + 1.f));
    s[4] = 1.f / sqrt((c_abs(wx) * 2.f * c_abs(wx) + 1.f) / (c_abs(&a[(a_dim1 
	    << 2) + 4]) * c_abs(&a[(a_dim1 << 2) + 4]) + 1.f));
    s[5] = 1.f / sqrt((c_abs(wx) * 2.f * c_abs(wx) + 1.f) / (c_abs(&a[a_dim1 *
	     5 + 5]) * c_abs(&a[a_dim1 * 5 + 5]) + 1.f));

    clakf2_(&c__1, &c__4, &a[a_offset], lda, &a[(a_dim1 << 1) + 2], &b[
	    b_offset], &b[(b_dim1 << 1) + 2], z__, &c__8);
    cgesvd_("N", "N", &c__8, &c__8, z__, &c__8, rwork, work, &c__1, &work[1], 
	    &c__1, &work[2], &c__24, &rwork[8], &info);
    dif[1] = rwork[7];

    clakf2_(&c__4, &c__1, &a[a_offset], lda, &a[a_dim1 * 5 + 5], &b[b_offset],
	     &b[b_dim1 * 5 + 5], z__, &c__8);
    cgesvd_("N", "N", &c__8, &c__8, z__, &c__8, rwork, work, &c__1, &work[1], 
	    &c__1, &work[2], &c__24, &rwork[8], &info);
    dif[5] = rwork[7];

    return;

/*     End of CLATM6 */

} /* clatm6_ */

