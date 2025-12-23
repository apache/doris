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

/* > \brief \b ZLAROR */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE ZLAROR( SIDE, INIT, M, N, A, LDA, ISEED, X, INFO ) */

/*       CHARACTER          INIT, SIDE */
/*       INTEGER            INFO, LDA, M, N */
/*       INTEGER            ISEED( 4 ) */
/*       COMPLEX*16         A( LDA, * ), X( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* >    ZLAROR pre- or post-multiplies an M by N matrix A by a random */
/* >    unitary matrix U, overwriting A. A may optionally be */
/* >    initialized to the identity matrix before multiplying by U. */
/* >    U is generated using the method of G.W. Stewart */
/* >    ( SIAM J. Numer. Anal. 17, 1980, pp. 403-409 ). */
/* >    (BLAS-2 version) */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] SIDE */
/* > \verbatim */
/* >          SIDE is CHARACTER*1 */
/* >           SIDE specifies whether A is multiplied on the left or right */
/* >           by U. */
/* >       SIDE = 'L'   Multiply A on the left (premultiply) by U */
/* >       SIDE = 'R'   Multiply A on the right (postmultiply) by UC>       SIDE = 'C'   Multiply A on the lef
t by U and the right by UC>       SIDE = 'T'   Multiply A on the left by U and the right by U' */
/* >           Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] INIT */
/* > \verbatim */
/* >          INIT is CHARACTER*1 */
/* >           INIT specifies whether or not A should be initialized to */
/* >           the identity matrix. */
/* >              INIT = 'I'   Initialize A to (a section of) the */
/* >                           identity matrix before applying U. */
/* >              INIT = 'N'   No initialization.  Apply U to the */
/* >                           input matrix A. */
/* > */
/* >           INIT = 'I' may be used to generate square (i.e., unitary) */
/* >           or rectangular orthogonal matrices (orthogonality being */
/* >           in the sense of ZDOTC): */
/* > */
/* >           For square matrices, M=N, and SIDE many be either 'L' or */
/* >           'R'; the rows will be orthogonal to each other, as will the */
/* >           columns. */
/* >           For rectangular matrices where M < N, SIDE = 'R' will */
/* >           produce a dense matrix whose rows will be orthogonal and */
/* >           whose columns will not, while SIDE = 'L' will produce a */
/* >           matrix whose rows will be orthogonal, and whose first M */
/* >           columns will be orthogonal, the remaining columns being */
/* >           zero. */
/* >           For matrices where M > N, just use the previous */
/* >           explanation, interchanging 'L' and 'R' and "rows" and */
/* >           "columns". */
/* > */
/* >           Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >           Number of rows of A. Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >           Number of columns of A. Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in,out] A */
/* > \verbatim */
/* >           A is COMPLEX*16 array, dimension ( LDA, N ) */
/* >           Input and output array. Overwritten by U A ( if SIDE = 'L' ) */
/* >           or by A U ( if SIDE = 'R' ) */
/* >           or by U A U* ( if SIDE = 'C') */
/* >           or by U A U' ( if SIDE = 'T') on exit. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >           Leading dimension of A. Must be at least MAX ( 1, M ). */
/* >           Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in,out] ISEED */
/* > \verbatim */
/* >          ISEED is INTEGER array, dimension ( 4 ) */
/* >           On entry ISEED specifies the seed of the random number */
/* >           generator. The array elements should be between 0 and 4095; */
/* >           if not they will be reduced mod 4096.  Also, ISEED(4) must */
/* >           be odd.  The random number generator uses a linear */
/* >           congruential sequence limited to small integers, and so */
/* >           should produce machine independent random numbers. The */
/* >           values of ISEED are changed on exit, and can be used in the */
/* >           next call to ZLAROR to continue the same random number */
/* >           sequence. */
/* >           Modified. */
/* > \endverbatim */
/* > */
/* > \param[out] X */
/* > \verbatim */
/* >          X is COMPLEX*16 array, dimension ( 3*MAX( M, N ) ) */
/* >           Workspace. Of length: */
/* >               2*M + N if SIDE = 'L', */
/* >               2*N + M if SIDE = 'R', */
/* >               3*N     if SIDE = 'C' or 'T'. */
/* >           Modified. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >           An error flag.  It is set to: */
/* >            0  if no error. */
/* >            1  if ZLARND returned a bad random number (installation */
/* >               problem) */
/* >           -1  if SIDE is not L, R, C, or T. */
/* >           -3  if M is negative. */
/* >           -4  if N is negative or if SIDE is C or T and N is not equal */
/* >               to M. */
/* >           -6  if LDA is less than M. */
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
/* Subroutine */ void zlaror_(char *side, char *init, integer *m, integer *n, 
	doublecomplex *a, integer *lda, integer *iseed, doublecomplex *x, 
	integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, i__1, i__2, i__3;
    doublecomplex z__1, z__2;

    /* Local variables */
    integer kbeg, jcol;
    doublereal xabs;
    integer irow, j;
    extern logical lsame_(char *, char *);
    doublecomplex csign;
    extern /* Subroutine */ void zgerc_(integer *, integer *, doublecomplex *, 
	    doublecomplex *, integer *, doublecomplex *, integer *, 
	    doublecomplex *, integer *), zscal_(integer *, doublecomplex *, 
	    doublecomplex *, integer *);
    integer ixfrm;
    extern /* Subroutine */ void zgemv_(char *, integer *, integer *, 
	    doublecomplex *, doublecomplex *, integer *, doublecomplex *, 
	    integer *, doublecomplex *, doublecomplex *, integer *);
    integer itype, nxfrm;
    doublereal xnorm;
    extern doublereal dznrm2_(integer *, doublecomplex *, integer *);
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    doublereal factor;
    extern /* Subroutine */ void zlacgv_(integer *, doublecomplex *, integer *)
	    ;
    //extern /* Double Complex */ VOID zlarnd_(doublecomplex *, integer *, 
    extern doublecomplex zlarnd_(integer *, 
	    integer *);
    extern /* Subroutine */ void zlaset_(char *, integer *, integer *, 
	    doublecomplex *, doublecomplex *, doublecomplex *, integer *);
    doublecomplex xnorms;


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
    } else if (lsame_(side, "C")) {
	itype = 3;
    } else if (lsame_(side, "T")) {
	itype = 4;
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
	xerbla_("ZLAROR", &i__1, 6);
	return;
    }

    if (itype == 1) {
	nxfrm = *m;
    } else {
	nxfrm = *n;
    }

/*     Initialize A to the identity matrix if desired */

    if (lsame_(init, "I")) {
	zlaset_("Full", m, n, &c_b1, &c_b2, &a[a_offset], lda);
    }

/*     If no rotation possible, still multiply by */
/*     a random complex number from the circle |x| = 1 */

/*      2)      Compute Rotation by computing Householder */
/*              Transformations H(2), H(3), ..., H(n).  Note that the */
/*              order in which they are computed is irrelevant. */

    i__1 = nxfrm;
    for (j = 1; j <= i__1; ++j) {
	i__2 = j;
	x[i__2].r = 0., x[i__2].i = 0.;
/* L10: */
    }

    i__1 = nxfrm;
    for (ixfrm = 2; ixfrm <= i__1; ++ixfrm) {
	kbeg = nxfrm - ixfrm + 1;

/*        Generate independent normal( 0, 1 ) random numbers */

	i__2 = nxfrm;
	for (j = kbeg; j <= i__2; ++j) {
	    i__3 = j;
	    //zlarnd_(&z__1, &c__3, &iseed[1]);
	    z__1=zlarnd_(&c__3, &iseed[1]);
	    x[i__3].r = z__1.r, x[i__3].i = z__1.i;
/* L20: */
	}

/*        Generate a Householder transformation from the random vector X */

	xnorm = dznrm2_(&ixfrm, &x[kbeg], &c__1);
	xabs = z_abs(&x[kbeg]);
	if (xabs != 0.) {
	    i__2 = kbeg;
	    z__1.r = x[i__2].r / xabs, z__1.i = x[i__2].i / xabs;
	    csign.r = z__1.r, csign.i = z__1.i;
	} else {
	    csign.r = 1., csign.i = 0.;
	}
	z__1.r = xnorm * csign.r, z__1.i = xnorm * csign.i;
	xnorms.r = z__1.r, xnorms.i = z__1.i;
	i__2 = nxfrm + kbeg;
	z__1.r = -csign.r, z__1.i = -csign.i;
	x[i__2].r = z__1.r, x[i__2].i = z__1.i;
	factor = xnorm * (xnorm + xabs);
	if (abs(factor) < 1e-20) {
	    *info = 1;
	    i__2 = -(*info);
	    xerbla_("ZLAROR", &i__2, 6);
	    return;
	} else {
	    factor = 1. / factor;
	}
	i__2 = kbeg;
	i__3 = kbeg;
	z__1.r = x[i__3].r + xnorms.r, z__1.i = x[i__3].i + xnorms.i;
	x[i__2].r = z__1.r, x[i__2].i = z__1.i;

/*        Apply Householder transformation to A */

	if (itype == 1 || itype == 3 || itype == 4) {

/*           Apply H(k) on the left of A */

	    zgemv_("C", &ixfrm, n, &c_b2, &a[kbeg + a_dim1], lda, &x[kbeg], &
		    c__1, &c_b1, &x[(nxfrm << 1) + 1], &c__1);
	    z__2.r = factor, z__2.i = 0.;
	    z__1.r = -z__2.r, z__1.i = -z__2.i;
	    zgerc_(&ixfrm, n, &z__1, &x[kbeg], &c__1, &x[(nxfrm << 1) + 1], &
		    c__1, &a[kbeg + a_dim1], lda);

	}

	if (itype >= 2 && itype <= 4) {

/*           Apply H(k)* (or H(k)') on the right of A */

	    if (itype == 4) {
		zlacgv_(&ixfrm, &x[kbeg], &c__1);
	    }

	    zgemv_("N", m, &ixfrm, &c_b2, &a[kbeg * a_dim1 + 1], lda, &x[kbeg]
		    , &c__1, &c_b1, &x[(nxfrm << 1) + 1], &c__1);
	    z__2.r = factor, z__2.i = 0.;
	    z__1.r = -z__2.r, z__1.i = -z__2.i;
	    zgerc_(m, &ixfrm, &z__1, &x[(nxfrm << 1) + 1], &c__1, &x[kbeg], &
		    c__1, &a[kbeg * a_dim1 + 1], lda);

	}
/* L30: */
    }

    //zlarnd_(&z__1, &c__3, &iseed[1]);
    z__1=zlarnd_(&c__3, &iseed[1]);
    x[1].r = z__1.r, x[1].i = z__1.i;
    xabs = z_abs(&x[1]);
    if (xabs != 0.) {
	z__1.r = x[1].r / xabs, z__1.i = x[1].i / xabs;
	csign.r = z__1.r, csign.i = z__1.i;
    } else {
	csign.r = 1., csign.i = 0.;
    }
    i__1 = nxfrm << 1;
    x[i__1].r = csign.r, x[i__1].i = csign.i;

/*     Scale the matrix A by D. */

    if (itype == 1 || itype == 3 || itype == 4) {
	i__1 = *m;
	for (irow = 1; irow <= i__1; ++irow) {
	    d_cnjg(&z__1, &x[nxfrm + irow]);
	    zscal_(n, &z__1, &a[irow + a_dim1], lda);
/* L40: */
	}
    }

    if (itype == 2 || itype == 3) {
	i__1 = *n;
	for (jcol = 1; jcol <= i__1; ++jcol) {
	    zscal_(m, &x[nxfrm + jcol], &a[jcol * a_dim1 + 1], &c__1);
/* L50: */
	}
    }

    if (itype == 4) {
	i__1 = *n;
	for (jcol = 1; jcol <= i__1; ++jcol) {
	    d_cnjg(&z__1, &x[nxfrm + jcol]);
	    zscal_(m, &z__1, &a[jcol * a_dim1 + 1], &c__1);
/* L60: */
	}
    }
    return;

/*     End of ZLAROR */

} /* zlaror_ */

