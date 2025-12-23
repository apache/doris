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




/* Table of constant values */

static doublereal c_b9 = 0.;
static doublereal c_b10 = 1.;
static integer c__3 = 3;
static integer c__1 = 1;

/* > \brief \b DLAROR */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE DLAROR( SIDE, INIT, M, N, A, LDA, ISEED, X, INFO ) */

/*       CHARACTER          INIT, SIDE */
/*       INTEGER            INFO, LDA, M, N */
/*       INTEGER            ISEED( 4 ) */
/*       DOUBLE PRECISION   A( LDA, * ), X( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > DLAROR pre- or post-multiplies an M by N matrix A by a random */
/* > orthogonal matrix U, overwriting A.  A may optionally be initialized */
/* > to the identity matrix before multiplying by U.  U is generated using */
/* > the method of G.W. Stewart (SIAM J. Numer. Anal. 17, 1980, 403-409). */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] SIDE */
/* > \verbatim */
/* >          SIDE is CHARACTER*1 */
/* >          Specifies whether A is multiplied on the left or right by U. */
/* >          = 'L':         Multiply A on the left (premultiply) by U */
/* >          = 'R':         Multiply A on the right (postmultiply) by U' */
/* >          = 'C' or 'T':  Multiply A on the left by U and the right */
/* >                          by U' (Here, U' means U-transpose.) */
/* > \endverbatim */
/* > */
/* > \param[in] INIT */
/* > \verbatim */
/* >          INIT is CHARACTER*1 */
/* >          Specifies whether or not A should be initialized to the */
/* >          identity matrix. */
/* >          = 'I':  Initialize A to (a section of) the identity matrix */
/* >                   before applying U. */
/* >          = 'N':  No initialization.  Apply U to the input matrix A. */
/* > */
/* >          INIT = 'I' may be used to generate square or rectangular */
/* >          orthogonal matrices: */
/* > */
/* >          For M = N and SIDE = 'L' or 'R', the rows will be orthogonal */
/* >          to each other, as will the columns. */
/* > */
/* >          If M < N, SIDE = 'R' produces a dense matrix whose rows are */
/* >          orthogonal and whose columns are not, while SIDE = 'L' */
/* >          produces a matrix whose rows are orthogonal, and whose first */
/* >          M columns are orthogonal, and whose remaining columns are */
/* >          zero. */
/* > */
/* >          If M > N, SIDE = 'L' produces a dense matrix whose columns */
/* >          are orthogonal and whose rows are not, while SIDE = 'R' */
/* >          produces a matrix whose columns are orthogonal, and whose */
/* >          first M rows are orthogonal, and whose remaining rows are */
/* >          zero. */
/* > \endverbatim */
/* > */
/* > \param[in] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >          The number of rows of A. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The number of columns of A. */
/* > \endverbatim */
/* > */
/* > \param[in,out] A */
/* > \verbatim */
/* >          A is DOUBLE PRECISION array, dimension (LDA, N) */
/* >          On entry, the array A. */
/* >          On exit, overwritten by U A ( if SIDE = 'L' ), */
/* >           or by A U ( if SIDE = 'R' ), */
/* >           or by U A U' ( if SIDE = 'C' or 'T'). */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of the array A.  LDA >= f2cmax(1,M). */
/* > \endverbatim */
/* > */
/* > \param[in,out] ISEED */
/* > \verbatim */
/* >          ISEED is INTEGER array, dimension (4) */
/* >          On entry ISEED specifies the seed of the random number */
/* >          generator. The array elements should be between 0 and 4095; */
/* >          if not they will be reduced mod 4096.  Also, ISEED(4) must */
/* >          be odd.  The random number generator uses a linear */
/* >          congruential sequence limited to small integers, and so */
/* >          should produce machine independent random numbers. The */
/* >          values of ISEED are changed on exit, and can be used in the */
/* >          next call to DLAROR to continue the same random number */
/* >          sequence. */
/* > \endverbatim */
/* > */
/* > \param[out] X */
/* > \verbatim */
/* >          X is DOUBLE PRECISION array, dimension (3*MAX( M, N )) */
/* >          Workspace of length */
/* >              2*M + N if SIDE = 'L', */
/* >              2*N + M if SIDE = 'R', */
/* >              3*N     if SIDE = 'C' or 'T'. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          An error flag.  It is set to: */
/* >          = 0:  normal return */
/* >          < 0:  if INFO = -k, the k-th argument had an illegal value */
/* >          = 1:  if the random numbers generated by DLARND are bad. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup double_matgen */

/*  ===================================================================== */
/* Subroutine */ void dlaror_(char *side, char *init, integer *m, integer *n, 
	doublereal *a, integer *lda, integer *iseed, doublereal *x, integer *
	info)
{
    /* System generated locals */
    integer a_dim1, a_offset, i__1, i__2;
    doublereal d__1;

    /* Local variables */
    integer kbeg;
    extern /* Subroutine */ void dger_(integer *, integer *, doublereal *, 
	    doublereal *, integer *, doublereal *, integer *, doublereal *, 
	    integer *);
    integer jcol, irow;
    extern doublereal dnrm2_(integer *, doublereal *, integer *);
    integer j;
    extern /* Subroutine */ void dscal_(integer *, doublereal *, doublereal *, 
	    integer *);
    extern logical lsame_(char *, char *);
    extern /* Subroutine */ void dgemv_(char *, integer *, integer *, 
	    doublereal *, doublereal *, integer *, doublereal *, integer *, 
	    doublereal *, doublereal *, integer *);
    integer ixfrm, itype, nxfrm;
    doublereal xnorm;
    extern doublereal dlarnd_(integer *, integer *);
    extern /* Subroutine */ void dlaset_(char *, integer *, integer *, 
	    doublereal *, doublereal *, doublereal *, integer *); 
    extern int xerbla_(char *, integer *, ftnlen);
    doublereal factor, xnorms;


/*  -- LAPACK auxiliary routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    --iseed;
    --x;

    /* Function Body */
    *info = 0;
    if (*n == 0 || *m == 0) {
	return;
    }

    itype = 0;
    if (lsame_(side, "L")) {
	itype = 1;
    } else if (lsame_(side, "R")) {
	itype = 2;
    } else if (lsame_(side, "C") || lsame_(side, "T")) {
	itype = 3;
    }

/*     Check for argument errors. */

    if (itype == 0) {
	*info = -1;
    } else if (*m < 0) {
	*info = -3;
    } else if (*n < 0 || itype == 3 && *n != *m) {
	*info = -4;
    } else if (*lda < *m) {
	*info = -6;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("DLAROR", &i__1, 6);
	return;
    }

    if (itype == 1) {
	nxfrm = *m;
    } else {
	nxfrm = *n;
    }

/*     Initialize A to the identity matrix if desired */

    if (lsame_(init, "I")) {
	dlaset_("Full", m, n, &c_b9, &c_b10, &a[a_offset], lda);
    }

/*     If no rotation possible, multiply by random +/-1 */

/*     Compute rotation by computing Householder transformations */
/*     H(2), H(3), ..., H(nhouse) */

    i__1 = nxfrm;
    for (j = 1; j <= i__1; ++j) {
	x[j] = 0.;
/* L10: */
    }

    i__1 = nxfrm;
    for (ixfrm = 2; ixfrm <= i__1; ++ixfrm) {
	kbeg = nxfrm - ixfrm + 1;

/*        Generate independent normal( 0, 1 ) random numbers */

	i__2 = nxfrm;
	for (j = kbeg; j <= i__2; ++j) {
	    x[j] = dlarnd_(&c__3, &iseed[1]);
/* L20: */
	}

/*        Generate a Householder transformation from the random vector X */

	xnorm = dnrm2_(&ixfrm, &x[kbeg], &c__1);
	xnorms = d_sign(&xnorm, &x[kbeg]);
	d__1 = -x[kbeg];
	x[kbeg + nxfrm] = d_sign(&c_b10, &d__1);
	factor = xnorms * (xnorms + x[kbeg]);
	if (abs(factor) < 1e-20) {
	    *info = 1;
	    xerbla_("DLAROR", info, 6);
	    return;
	} else {
	    factor = 1. / factor;
	}
	x[kbeg] += xnorms;

/*        Apply Householder transformation to A */

	if (itype == 1 || itype == 3) {

/*           Apply H(k) from the left. */

	    dgemv_("T", &ixfrm, n, &c_b10, &a[kbeg + a_dim1], lda, &x[kbeg], &
		    c__1, &c_b9, &x[(nxfrm << 1) + 1], &c__1);
	    d__1 = -factor;
	    dger_(&ixfrm, n, &d__1, &x[kbeg], &c__1, &x[(nxfrm << 1) + 1], &
		    c__1, &a[kbeg + a_dim1], lda);

	}

	if (itype == 2 || itype == 3) {

/*           Apply H(k) from the right. */

	    dgemv_("N", m, &ixfrm, &c_b10, &a[kbeg * a_dim1 + 1], lda, &x[
		    kbeg], &c__1, &c_b9, &x[(nxfrm << 1) + 1], &c__1);
	    d__1 = -factor;
	    dger_(m, &ixfrm, &d__1, &x[(nxfrm << 1) + 1], &c__1, &x[kbeg], &
		    c__1, &a[kbeg * a_dim1 + 1], lda);

	}
/* L30: */
    }

    d__1 = dlarnd_(&c__3, &iseed[1]);
    x[nxfrm * 2] = d_sign(&c_b10, &d__1);

/*     Scale the matrix A by D. */

    if (itype == 1 || itype == 3) {
	i__1 = *m;
	for (irow = 1; irow <= i__1; ++irow) {
	    dscal_(n, &x[nxfrm + irow], &a[irow + a_dim1], lda);
/* L40: */
	}
    }

    if (itype == 2 || itype == 3) {
	i__1 = *n;
	for (jcol = 1; jcol <= i__1; ++jcol) {
	    dscal_(m, &x[nxfrm + jcol], &a[jcol * a_dim1 + 1], &c__1);
/* L50: */
	}
    }
    return;

/*     End of DLAROR */

} /* dlaror_ */

