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
#define z_div(c, a, b) {Cd(c)._Val[0] = (Cd(a)._Val[0]/Cd(b)._Val[0]); Cd(c)._Val[1]=(Cd(a)._Val[1]/Cd(b)._Val[1]);}
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
#define mycycle_() continue;
#define myceiling_(w) {ceil(w)}
#define myhuge_(w) {HUGE_VAL}
//#define mymaxloc_(w,s,e,n) {if (sizeof(*(w)) == sizeof(double)) dmaxloc_((w),*(s),*(e),n); else dmaxloc_((w),*(s),*(e),n);}
#define mymaxloc_(w,s,e,n) {dmaxloc_(w,*(s),*(e),n)}

/* procedure parameter types for -A and -C++ */




/* Table of constant values */

static doublecomplex c_b1 = {0.,0.};
static doublecomplex c_b2 = {1.,0.};
static integer c__3 = 3;
static integer c__1 = 1;

/* > \brief \b ZLAGSY */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE ZLAGSY( N, K, D, A, LDA, ISEED, WORK, INFO ) */

/*       INTEGER            INFO, K, LDA, N */
/*       INTEGER            ISEED( 4 ) */
/*       DOUBLE PRECISION   D( * ) */
/*       COMPLEX*16         A( LDA, * ), WORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > ZLAGSY generates a complex symmetric matrix A, by pre- and post- */
/* > multiplying a real diagonal matrix D with a random unitary matrix: */
/* > A = U*D*U**T. The semi-bandwidth may then be reduced to k by */
/* > additional unitary transformations. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrix A.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] K */
/* > \verbatim */
/* >          K is INTEGER */
/* >          The number of nonzero subdiagonals within the band of A. */
/* >          0 <= K <= N-1. */
/* > \endverbatim */
/* > */
/* > \param[in] D */
/* > \verbatim */
/* >          D is DOUBLE PRECISION array, dimension (N) */
/* >          The diagonal elements of the diagonal matrix D. */
/* > \endverbatim */
/* > */
/* > \param[out] A */
/* > \verbatim */
/* >          A is COMPLEX*16 array, dimension (LDA,N) */
/* >          The generated n by n symmetric matrix A (the full matrix is */
/* >          stored). */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of the array A.  LDA >= N. */
/* > \endverbatim */
/* > */
/* > \param[in,out] ISEED */
/* > \verbatim */
/* >          ISEED is INTEGER array, dimension (4) */
/* >          On entry, the seed of the random number generator; the array */
/* >          elements must be between 0 and 4095, and ISEED(4) must be */
/* >          odd. */
/* >          On exit, the seed is updated. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is COMPLEX*16 array, dimension (2*N) */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0: successful exit */
/* >          < 0: if INFO = -i, the i-th argument had an illegal value */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup complex16_matgen */

/*  ===================================================================== */
/* Subroutine */ void zlagsy_(integer *n, integer *k, doublereal *d__, 
	doublecomplex *a, integer *lda, integer *iseed, doublecomplex *work, 
	integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, i__1, i__2, i__3, i__4, i__5, i__6, i__7, i__8, 
	    i__9;
    doublereal d__1;
    doublecomplex z__1, z__2, z__3, z__4;

    /* Local variables */
    integer i__, j;
    doublecomplex alpha;
    extern /* Subroutine */ void zgerc_(integer *, integer *, doublecomplex *, 
	    doublecomplex *, integer *, doublecomplex *, integer *, 
	    doublecomplex *, integer *), zscal_(integer *, doublecomplex *, 
	    doublecomplex *, integer *);
    extern /* Double Complex */ VOID zdotc_(doublecomplex *, integer *, 
	    doublecomplex *, integer *, doublecomplex *, integer *);
    extern /* Subroutine */ void zgemv_(char *, integer *, integer *, 
	    doublecomplex *, doublecomplex *, integer *, doublecomplex *, 
	    integer *, doublecomplex *, doublecomplex *, integer *), 
	    zaxpy_(integer *, doublecomplex *, doublecomplex *, integer *, 
	    doublecomplex *, integer *), zsymv_(char *, integer *, 
	    doublecomplex *, doublecomplex *, integer *, doublecomplex *, 
	    integer *, doublecomplex *, doublecomplex *, integer *);
    extern doublereal dznrm2_(integer *, doublecomplex *, integer *);
    integer ii, jj;
    doublecomplex wa, wb;
    doublereal wn;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    extern void zlacgv_(
	    integer *, doublecomplex *, integer *), zlarnv_(integer *, 
	    integer *, integer *, doublecomplex *);
    doublecomplex tau;


/*  -- LAPACK auxiliary routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


/*     Test the input arguments */

    /* Parameter adjustments */
    --d__;
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    --iseed;
    --work;

    /* Function Body */
    *info = 0;
    if (*n < 0) {
	*info = -1;
    } else if (*k < 0 || *k > *n - 1) {
	*info = -2;
    } else if (*lda < f2cmax(1,*n)) {
	*info = -5;
    }
    if (*info < 0) {
	i__1 = -(*info);
	xerbla_("ZLAGSY", &i__1, 6);
	return;
    }

/*     initialize lower triangle of A to diagonal matrix */

    i__1 = *n;
    for (j = 1; j <= i__1; ++j) {
	i__2 = *n;
	for (i__ = j + 1; i__ <= i__2; ++i__) {
	    i__3 = i__ + j * a_dim1;
	    a[i__3].r = 0., a[i__3].i = 0.;
/* L10: */
	}
/* L20: */
    }
    i__1 = *n;
    for (i__ = 1; i__ <= i__1; ++i__) {
	i__2 = i__ + i__ * a_dim1;
	i__3 = i__;
	a[i__2].r = d__[i__3], a[i__2].i = 0.;
/* L30: */
    }

/*     Generate lower triangle of symmetric matrix */

    for (i__ = *n - 1; i__ >= 1; --i__) {

/*        generate random reflection */

	i__1 = *n - i__ + 1;
	zlarnv_(&c__3, &iseed[1], &i__1, &work[1]);
	i__1 = *n - i__ + 1;
	wn = dznrm2_(&i__1, &work[1], &c__1);
	d__1 = wn / z_abs(&work[1]);
	z__1.r = d__1 * work[1].r, z__1.i = d__1 * work[1].i;
	wa.r = z__1.r, wa.i = z__1.i;
	if (wn == 0.) {
	    tau.r = 0., tau.i = 0.;
	} else {
	    z__1.r = work[1].r + wa.r, z__1.i = work[1].i + wa.i;
	    wb.r = z__1.r, wb.i = z__1.i;
	    i__1 = *n - i__;
	    z_div(&z__1, &c_b2, &wb);
	    zscal_(&i__1, &z__1, &work[2], &c__1);
	    work[1].r = 1., work[1].i = 0.;
	    z_div(&z__1, &wb, &wa);
	    d__1 = z__1.r;
	    tau.r = d__1, tau.i = 0.;
	}

/*        apply random reflection to A(i:n,i:n) from the left */
/*        and the right */

/*        compute  y := tau * A * conjg(u) */

	i__1 = *n - i__ + 1;
	zlacgv_(&i__1, &work[1], &c__1);
	i__1 = *n - i__ + 1;
	zsymv_("Lower", &i__1, &tau, &a[i__ + i__ * a_dim1], lda, &work[1], &
		c__1, &c_b1, &work[*n + 1], &c__1);
	i__1 = *n - i__ + 1;
	zlacgv_(&i__1, &work[1], &c__1);

/*        compute  v := y - 1/2 * tau * ( u, y ) * u */

	z__3.r = -.5, z__3.i = 0.;
	z__2.r = z__3.r * tau.r - z__3.i * tau.i, z__2.i = z__3.r * tau.i + 
		z__3.i * tau.r;
	i__1 = *n - i__ + 1;
	zdotc_(&z__4, &i__1, &work[1], &c__1, &work[*n + 1], &c__1);
	z__1.r = z__2.r * z__4.r - z__2.i * z__4.i, z__1.i = z__2.r * z__4.i 
		+ z__2.i * z__4.r;
	alpha.r = z__1.r, alpha.i = z__1.i;
	i__1 = *n - i__ + 1;
	zaxpy_(&i__1, &alpha, &work[1], &c__1, &work[*n + 1], &c__1);

/*        apply the transformation as a rank-2 update to A(i:n,i:n) */

/*        CALL ZSYR2( 'Lower', N-I+1, -ONE, WORK, 1, WORK( N+1 ), 1, */
/*        $               A( I, I ), LDA ) */

	i__1 = *n;
	for (jj = i__; jj <= i__1; ++jj) {
	    i__2 = *n;
	    for (ii = jj; ii <= i__2; ++ii) {
		i__3 = ii + jj * a_dim1;
		i__4 = ii + jj * a_dim1;
		i__5 = ii - i__ + 1;
		i__6 = *n + jj - i__ + 1;
		z__3.r = work[i__5].r * work[i__6].r - work[i__5].i * work[
			i__6].i, z__3.i = work[i__5].r * work[i__6].i + work[
			i__5].i * work[i__6].r;
		z__2.r = a[i__4].r - z__3.r, z__2.i = a[i__4].i - z__3.i;
		i__7 = *n + ii - i__ + 1;
		i__8 = jj - i__ + 1;
		z__4.r = work[i__7].r * work[i__8].r - work[i__7].i * work[
			i__8].i, z__4.i = work[i__7].r * work[i__8].i + work[
			i__7].i * work[i__8].r;
		z__1.r = z__2.r - z__4.r, z__1.i = z__2.i - z__4.i;
		a[i__3].r = z__1.r, a[i__3].i = z__1.i;
/* L40: */
	    }
/* L50: */
	}
/* L60: */
    }

/*     Reduce number of subdiagonals to K */

    i__1 = *n - 1 - *k;
    for (i__ = 1; i__ <= i__1; ++i__) {

/*        generate reflection to annihilate A(k+i+1:n,i) */

	i__2 = *n - *k - i__ + 1;
	wn = dznrm2_(&i__2, &a[*k + i__ + i__ * a_dim1], &c__1);
	d__1 = wn / z_abs(&a[*k + i__ + i__ * a_dim1]);
	i__2 = *k + i__ + i__ * a_dim1;
	z__1.r = d__1 * a[i__2].r, z__1.i = d__1 * a[i__2].i;
	wa.r = z__1.r, wa.i = z__1.i;
	if (wn == 0.) {
	    tau.r = 0., tau.i = 0.;
	} else {
	    i__2 = *k + i__ + i__ * a_dim1;
	    z__1.r = a[i__2].r + wa.r, z__1.i = a[i__2].i + wa.i;
	    wb.r = z__1.r, wb.i = z__1.i;
	    i__2 = *n - *k - i__;
	    z_div(&z__1, &c_b2, &wb);
	    zscal_(&i__2, &z__1, &a[*k + i__ + 1 + i__ * a_dim1], &c__1);
	    i__2 = *k + i__ + i__ * a_dim1;
	    a[i__2].r = 1., a[i__2].i = 0.;
	    z_div(&z__1, &wb, &wa);
	    d__1 = z__1.r;
	    tau.r = d__1, tau.i = 0.;
	}

/*        apply reflection to A(k+i:n,i+1:k+i-1) from the left */

	i__2 = *n - *k - i__ + 1;
	i__3 = *k - 1;
	zgemv_("Conjugate transpose", &i__2, &i__3, &c_b2, &a[*k + i__ + (i__ 
		+ 1) * a_dim1], lda, &a[*k + i__ + i__ * a_dim1], &c__1, &
		c_b1, &work[1], &c__1);
	i__2 = *n - *k - i__ + 1;
	i__3 = *k - 1;
	z__1.r = -tau.r, z__1.i = -tau.i;
	zgerc_(&i__2, &i__3, &z__1, &a[*k + i__ + i__ * a_dim1], &c__1, &work[
		1], &c__1, &a[*k + i__ + (i__ + 1) * a_dim1], lda);

/*        apply reflection to A(k+i:n,k+i:n) from the left and the right */

/*        compute  y := tau * A * conjg(u) */

	i__2 = *n - *k - i__ + 1;
	zlacgv_(&i__2, &a[*k + i__ + i__ * a_dim1], &c__1);
	i__2 = *n - *k - i__ + 1;
	zsymv_("Lower", &i__2, &tau, &a[*k + i__ + (*k + i__) * a_dim1], lda, 
		&a[*k + i__ + i__ * a_dim1], &c__1, &c_b1, &work[1], &c__1);
	i__2 = *n - *k - i__ + 1;
	zlacgv_(&i__2, &a[*k + i__ + i__ * a_dim1], &c__1);

/*        compute  v := y - 1/2 * tau * ( u, y ) * u */

	z__3.r = -.5, z__3.i = 0.;
	z__2.r = z__3.r * tau.r - z__3.i * tau.i, z__2.i = z__3.r * tau.i + 
		z__3.i * tau.r;
	i__2 = *n - *k - i__ + 1;
	zdotc_(&z__4, &i__2, &a[*k + i__ + i__ * a_dim1], &c__1, &work[1], &
		c__1);
	z__1.r = z__2.r * z__4.r - z__2.i * z__4.i, z__1.i = z__2.r * z__4.i 
		+ z__2.i * z__4.r;
	alpha.r = z__1.r, alpha.i = z__1.i;
	i__2 = *n - *k - i__ + 1;
	zaxpy_(&i__2, &alpha, &a[*k + i__ + i__ * a_dim1], &c__1, &work[1], &
		c__1);

/*        apply symmetric rank-2 update to A(k+i:n,k+i:n) */

/*        CALL ZSYR2( 'Lower', N-K-I+1, -ONE, A( K+I, I ), 1, WORK, 1, */
/*        $               A( K+I, K+I ), LDA ) */

	i__2 = *n;
	for (jj = *k + i__; jj <= i__2; ++jj) {
	    i__3 = *n;
	    for (ii = jj; ii <= i__3; ++ii) {
		i__4 = ii + jj * a_dim1;
		i__5 = ii + jj * a_dim1;
		i__6 = ii + i__ * a_dim1;
		i__7 = jj - *k - i__ + 1;
		z__3.r = a[i__6].r * work[i__7].r - a[i__6].i * work[i__7].i, 
			z__3.i = a[i__6].r * work[i__7].i + a[i__6].i * work[
			i__7].r;
		z__2.r = a[i__5].r - z__3.r, z__2.i = a[i__5].i - z__3.i;
		i__8 = ii - *k - i__ + 1;
		i__9 = jj + i__ * a_dim1;
		z__4.r = work[i__8].r * a[i__9].r - work[i__8].i * a[i__9].i, 
			z__4.i = work[i__8].r * a[i__9].i + work[i__8].i * a[
			i__9].r;
		z__1.r = z__2.r - z__4.r, z__1.i = z__2.i - z__4.i;
		a[i__4].r = z__1.r, a[i__4].i = z__1.i;
/* L70: */
	    }
/* L80: */
	}

	i__2 = *k + i__ + i__ * a_dim1;
	z__1.r = -wa.r, z__1.i = -wa.i;
	a[i__2].r = z__1.r, a[i__2].i = z__1.i;
	i__2 = *n;
	for (j = *k + i__ + 1; j <= i__2; ++j) {
	    i__3 = j + i__ * a_dim1;
	    a[i__3].r = 0., a[i__3].i = 0.;
/* L90: */
	}
/* L100: */
    }

/*     Store full symmetric matrix */

    i__1 = *n;
    for (j = 1; j <= i__1; ++j) {
	i__2 = *n;
	for (i__ = j + 1; i__ <= i__2; ++i__) {
	    i__3 = j + i__ * a_dim1;
	    i__4 = i__ + j * a_dim1;
	    a[i__3].r = a[i__4].r, a[i__3].i = a[i__4].i;
/* L110: */
	}
/* L120: */
    }
    return;

/*     End of ZLAGSY */

} /* zlagsy_ */

