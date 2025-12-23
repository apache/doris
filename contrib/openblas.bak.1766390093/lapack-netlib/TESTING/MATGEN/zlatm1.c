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



static double dpow_ui(double x, integer n) {
	double pow=1.0; unsigned long int u;
	if(n != 0) {
		if(n < 0) n = -n, x = 1/x;
		for(u = n; ; ) {
			if(u & 01) pow *= x;
			if(u >>= 1) x *= x;
			else break;
		}
	}
	return pow;
}


/* Table of constant values */

static integer c__3 = 3;

/* > \brief \b ZLATM1 */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE ZLATM1( MODE, COND, IRSIGN, IDIST, ISEED, D, N, INFO ) */

/*       INTEGER            IDIST, INFO, IRSIGN, MODE, N */
/*       DOUBLE PRECISION   COND */
/*       INTEGER            ISEED( 4 ) */
/*       COMPLEX*16         D( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* >    ZLATM1 computes the entries of D(1..N) as specified by */
/* >    MODE, COND and IRSIGN. IDIST and ISEED determine the generation */
/* >    of random numbers. ZLATM1 is called by ZLATMR to generate */
/* >    random test matrices for LAPACK programs. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] MODE */
/* > \verbatim */
/* >          MODE is INTEGER */
/* >           On entry describes how D is to be computed: */
/* >           MODE = 0 means do not change D. */
/* >           MODE = 1 sets D(1)=1 and D(2:N)=1.0/COND */
/* >           MODE = 2 sets D(1:N-1)=1 and D(N)=1.0/COND */
/* >           MODE = 3 sets D(I)=COND**(-(I-1)/(N-1)) */
/* >           MODE = 4 sets D(i)=1 - (i-1)/(N-1)*(1 - 1/COND) */
/* >           MODE = 5 sets D to random numbers in the range */
/* >                    ( 1/COND , 1 ) such that their logarithms */
/* >                    are uniformly distributed. */
/* >           MODE = 6 set D to random numbers from same distribution */
/* >                    as the rest of the matrix. */
/* >           MODE < 0 has the same meaning as ABS(MODE), except that */
/* >              the order of the elements of D is reversed. */
/* >           Thus if MODE is positive, D has entries ranging from */
/* >              1 to 1/COND, if negative, from 1/COND to 1, */
/* >           Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] COND */
/* > \verbatim */
/* >          COND is DOUBLE PRECISION */
/* >           On entry, used as described under MODE above. */
/* >           If used, it must be >= 1. Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in] IRSIGN */
/* > \verbatim */
/* >          IRSIGN is INTEGER */
/* >           On entry, if MODE neither -6, 0 nor 6, determines sign of */
/* >           entries of D */
/* >           0 => leave entries of D unchanged */
/* >           1 => multiply each entry of D by random complex number */
/* >                uniformly distributed with absolute value 1 */
/* > \endverbatim */
/* > */
/* > \param[in] IDIST */
/* > \verbatim */
/* >          IDIST is INTEGER */
/* >           On entry, IDIST specifies the type of distribution to be */
/* >           used to generate a random matrix . */
/* >           1 => real and imaginary parts each UNIFORM( 0, 1 ) */
/* >           2 => real and imaginary parts each UNIFORM( -1, 1 ) */
/* >           3 => real and imaginary parts each NORMAL( 0, 1 ) */
/* >           4 => complex number uniform in DISK( 0, 1 ) */
/* >           Not modified. */
/* > \endverbatim */
/* > */
/* > \param[in,out] ISEED */
/* > \verbatim */
/* >          ISEED is INTEGER array, dimension ( 4 ) */
/* >           On entry ISEED specifies the seed of the random number */
/* >           generator. The random number generator uses a */
/* >           linear congruential sequence limited to small */
/* >           integers, and so should produce machine independent */
/* >           random numbers. The values of ISEED are changed on */
/* >           exit, and can be used in the next call to ZLATM1 */
/* >           to continue the same random number sequence. */
/* >           Changed on exit. */
/* > \endverbatim */
/* > */
/* > \param[in,out] D */
/* > \verbatim */
/* >          D is COMPLEX*16 array, dimension ( N ) */
/* >           Array to be computed according to MODE, COND and IRSIGN. */
/* >           May be changed on exit if MODE is nonzero. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >           Number of entries of D. Not modified. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >            0  => normal termination */
/* >           -1  => if MODE not in range -6 to 6 */
/* >           -2  => if MODE neither -6, 0 nor 6, and */
/* >                  IRSIGN neither 0 nor 1 */
/* >           -3  => if MODE neither -6, 0 nor 6 and COND less than 1 */
/* >           -4  => if MODE equals 6 or -6 and IDIST not in range 1 to 4 */
/* >           -7  => if N negative */
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
/* Subroutine */ void zlatm1_(integer *mode, doublereal *cond, integer *irsign,
	 integer *idist, integer *iseed, doublecomplex *d__, integer *n, 
	integer *info)
{
    /* System generated locals */
    integer i__1, i__2, i__3;
    doublereal d__1;
    doublecomplex z__1, z__2;

    /* Local variables */
    doublereal temp;
    integer i__;
    doublereal alpha;
    doublecomplex ctemp;
    extern doublereal dlaran_(integer *);
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    //extern /* Double Complex */ VOID zlarnd_(doublecomplex *, integer *, 
    extern doublecomplex zlarnd_(integer *, 
	    integer *);
    extern /* Subroutine */ void zlarnv_(integer *, integer *, integer *, 
	    doublecomplex *);


/*  -- LAPACK auxiliary routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


/*     Decode and Test the input parameters. Initialize flags & seed. */

    /* Parameter adjustments */
    --d__;
    --iseed;

    /* Function Body */
    *info = 0;

/*     Quick return if possible */

    if (*n == 0) {
	return;
    }

/*     Set INFO if an error */

    if (*mode < -6 || *mode > 6) {
	*info = -1;
    } else if (*mode != -6 && *mode != 0 && *mode != 6 && (*irsign != 0 && *
	    irsign != 1)) {
	*info = -2;
    } else if (*mode != -6 && *mode != 0 && *mode != 6 && *cond < 1.) {
	*info = -3;
    } else if ((*mode == 6 || *mode == -6) && (*idist < 1 || *idist > 4)) {
	*info = -4;
    } else if (*n < 0) {
	*info = -7;
    }

    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("ZLATM1", &i__1, 6);
	return;
    }

/*     Compute D according to COND and MODE */

    if (*mode != 0) {
	switch (abs(*mode)) {
	    case 1:  goto L10;
	    case 2:  goto L30;
	    case 3:  goto L50;
	    case 4:  goto L70;
	    case 5:  goto L90;
	    case 6:  goto L110;
	}

/*        One large D value: */

L10:
	i__1 = *n;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    i__2 = i__;
	    d__1 = 1. / *cond;
	    d__[i__2].r = d__1, d__[i__2].i = 0.;
/* L20: */
	}
	d__[1].r = 1., d__[1].i = 0.;
	goto L120;

/*        One small D value: */

L30:
	i__1 = *n;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    i__2 = i__;
	    d__[i__2].r = 1., d__[i__2].i = 0.;
/* L40: */
	}
	i__1 = *n;
	d__1 = 1. / *cond;
	d__[i__1].r = d__1, d__[i__1].i = 0.;
	goto L120;

/*        Exponentially distributed D values: */

L50:
	d__[1].r = 1., d__[1].i = 0.;
	if (*n > 1) {
	    d__1 = -1. / (doublereal) (*n - 1);
	    alpha = pow_dd(cond, &d__1);
	    i__1 = *n;
	    for (i__ = 2; i__ <= i__1; ++i__) {
		i__2 = i__;
		i__3 = i__ - 1;
		d__1 = pow_di(&alpha, &i__3);
		d__[i__2].r = d__1, d__[i__2].i = 0.;
/* L60: */
	    }
	}
	goto L120;

/*        Arithmetically distributed D values: */

L70:
	d__[1].r = 1., d__[1].i = 0.;
	if (*n > 1) {
	    temp = 1. / *cond;
	    alpha = (1. - temp) / (doublereal) (*n - 1);
	    i__1 = *n;
	    for (i__ = 2; i__ <= i__1; ++i__) {
		i__2 = i__;
		d__1 = (doublereal) (*n - i__) * alpha + temp;
		d__[i__2].r = d__1, d__[i__2].i = 0.;
/* L80: */
	    }
	}
	goto L120;

/*        Randomly distributed D values on ( 1/COND , 1): */

L90:
	alpha = log(1. / *cond);
	i__1 = *n;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    i__2 = i__;
	    d__1 = exp(alpha * dlaran_(&iseed[1]));
	    d__[i__2].r = d__1, d__[i__2].i = 0.;
/* L100: */
	}
	goto L120;

/*        Randomly distributed D values from IDIST */

L110:
	zlarnv_(idist, &iseed[1], n, &d__[1]);

L120:

/*        If MODE neither -6 nor 0 nor 6, and IRSIGN = 1, assign */
/*        random signs to D */

	if (*mode != -6 && *mode != 0 && *mode != 6 && *irsign == 1) {
	    i__1 = *n;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		//zlarnd_(&z__1, &c__3, &iseed[1]);
		z__1=zlarnd_(&c__3, &iseed[1]);
		ctemp.r = z__1.r, ctemp.i = z__1.i;
		i__2 = i__;
		i__3 = i__;
		d__1 = z_abs(&ctemp);
		z__2.r = ctemp.r / d__1, z__2.i = ctemp.i / d__1;
		z__1.r = d__[i__3].r * z__2.r - d__[i__3].i * z__2.i, z__1.i =
			 d__[i__3].r * z__2.i + d__[i__3].i * z__2.r;
		d__[i__2].r = z__1.r, d__[i__2].i = z__1.i;
/* L130: */
	    }
	}

/*        Reverse if MODE < 0 */

	if (*mode < 0) {
	    i__1 = *n / 2;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		i__2 = i__;
		ctemp.r = d__[i__2].r, ctemp.i = d__[i__2].i;
		i__2 = i__;
		i__3 = *n + 1 - i__;
		d__[i__2].r = d__[i__3].r, d__[i__2].i = d__[i__3].i;
		i__2 = *n + 1 - i__;
		d__[i__2].r = ctemp.r, d__[i__2].i = ctemp.i;
/* L140: */
	    }
	}

    }

    return;

/*     End of ZLATM1 */

} /* zlatm1_ */

