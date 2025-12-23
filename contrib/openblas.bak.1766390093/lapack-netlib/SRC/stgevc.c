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
static char junk[] = "\n@(#)LIBF77 VERSION 19990503\n";
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


#ifdef __cplusplus
typedef logical (*L_fp)(...);
#else
typedef logical (*L_fp)();
#endif

static float spow_ui(float x, integer n) {
	float pow=1.0; unsigned long int u;
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
#ifdef _MSC_VER
static _Fcomplex cpow_ui(complex x, integer n) {
	complex pow={1.0,0.0}; unsigned long int u;
		if(n != 0) {
		if(n < 0) n = -n, x.r = 1/x.r, x.i=1/x.i;
		for(u = n; ; ) {
			if(u & 01) pow.r *= x.r, pow.i *= x.i;
			if(u >>= 1) x.r *= x.r, x.i *= x.i;
			else break;
		}
	}
	_Fcomplex p={pow.r, pow.i};
	return p;
}
#else
static _Complex float cpow_ui(_Complex float x, integer n) {
	_Complex float pow=1.0; unsigned long int u;
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
#endif
#ifdef _MSC_VER
static _Dcomplex zpow_ui(_Dcomplex x, integer n) {
	_Dcomplex pow={1.0,0.0}; unsigned long int u;
	if(n != 0) {
		if(n < 0) n = -n, x._Val[0] = 1/x._Val[0], x._Val[1] =1/x._Val[1];
		for(u = n; ; ) {
			if(u & 01) pow._Val[0] *= x._Val[0], pow._Val[1] *= x._Val[1];
			if(u >>= 1) x._Val[0] *= x._Val[0], x._Val[1] *= x._Val[1];
			else break;
		}
	}
	_Dcomplex p = {pow._Val[0], pow._Val[1]};
	return p;
}
#else
static _Complex double zpow_ui(_Complex double x, integer n) {
	_Complex double pow=1.0; unsigned long int u;
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
#endif
static integer pow_ii(integer x, integer n) {
	integer pow; unsigned long int u;
	if (n <= 0) {
		if (n == 0 || x == 1) pow = 1;
		else if (x != -1) pow = x == 0 ? 1/x : 0;
		else n = -n;
	}
	if ((n > 0) || !(n == 0 || x == 1 || x != -1)) {
		u = n;
		for(pow = 1; ; ) {
			if(u & 01) pow *= x;
			if(u >>= 1) x *= x;
			else break;
		}
	}
	return pow;
}
static integer dmaxloc_(double *w, integer s, integer e, integer *n)
{
	double m; integer i, mi;
	for(m=w[s-1], mi=s, i=s+1; i<=e; i++)
		if (w[i-1]>m) mi=i ,m=w[i-1];
	return mi-s+1;
}
static integer smaxloc_(float *w, integer s, integer e, integer *n)
{
	float m; integer i, mi;
	for(m=w[s-1], mi=s, i=s+1; i<=e; i++)
		if (w[i-1]>m) mi=i ,m=w[i-1];
	return mi-s+1;
}
static inline void cdotc_(complex *z, integer *n_, complex *x, integer *incx_, complex *y, integer *incy_) {
	integer n = *n_, incx = *incx_, incy = *incy_, i;
#ifdef _MSC_VER
	_Fcomplex zdotc = {0.0, 0.0};
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += conjf(Cf(&x[i]))._Val[0] * Cf(&y[i])._Val[0];
			zdotc._Val[1] += conjf(Cf(&x[i]))._Val[1] * Cf(&y[i])._Val[1];
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += conjf(Cf(&x[i*incx]))._Val[0] * Cf(&y[i*incy])._Val[0];
			zdotc._Val[1] += conjf(Cf(&x[i*incx]))._Val[1] * Cf(&y[i*incy])._Val[1];
		}
	}
	pCf(z) = zdotc;
}
#else
	_Complex float zdotc = 0.0;
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += conjf(Cf(&x[i])) * Cf(&y[i]);
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += conjf(Cf(&x[i*incx])) * Cf(&y[i*incy]);
		}
	}
	pCf(z) = zdotc;
}
#endif
static inline void zdotc_(doublecomplex *z, integer *n_, doublecomplex *x, integer *incx_, doublecomplex *y, integer *incy_) {
	integer n = *n_, incx = *incx_, incy = *incy_, i;
#ifdef _MSC_VER
	_Dcomplex zdotc = {0.0, 0.0};
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += conj(Cd(&x[i]))._Val[0] * Cd(&y[i])._Val[0];
			zdotc._Val[1] += conj(Cd(&x[i]))._Val[1] * Cd(&y[i])._Val[1];
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += conj(Cd(&x[i*incx]))._Val[0] * Cd(&y[i*incy])._Val[0];
			zdotc._Val[1] += conj(Cd(&x[i*incx]))._Val[1] * Cd(&y[i*incy])._Val[1];
		}
	}
	pCd(z) = zdotc;
}
#else
	_Complex double zdotc = 0.0;
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += conj(Cd(&x[i])) * Cd(&y[i]);
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += conj(Cd(&x[i*incx])) * Cd(&y[i*incy]);
		}
	}
	pCd(z) = zdotc;
}
#endif	
static inline void cdotu_(complex *z, integer *n_, complex *x, integer *incx_, complex *y, integer *incy_) {
	integer n = *n_, incx = *incx_, incy = *incy_, i;
#ifdef _MSC_VER
	_Fcomplex zdotc = {0.0, 0.0};
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += Cf(&x[i])._Val[0] * Cf(&y[i])._Val[0];
			zdotc._Val[1] += Cf(&x[i])._Val[1] * Cf(&y[i])._Val[1];
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += Cf(&x[i*incx])._Val[0] * Cf(&y[i*incy])._Val[0];
			zdotc._Val[1] += Cf(&x[i*incx])._Val[1] * Cf(&y[i*incy])._Val[1];
		}
	}
	pCf(z) = zdotc;
}
#else
	_Complex float zdotc = 0.0;
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += Cf(&x[i]) * Cf(&y[i]);
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += Cf(&x[i*incx]) * Cf(&y[i*incy]);
		}
	}
	pCf(z) = zdotc;
}
#endif
static inline void zdotu_(doublecomplex *z, integer *n_, doublecomplex *x, integer *incx_, doublecomplex *y, integer *incy_) {
	integer n = *n_, incx = *incx_, incy = *incy_, i;
#ifdef _MSC_VER
	_Dcomplex zdotc = {0.0, 0.0};
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += Cd(&x[i])._Val[0] * Cd(&y[i])._Val[0];
			zdotc._Val[1] += Cd(&x[i])._Val[1] * Cd(&y[i])._Val[1];
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += Cd(&x[i*incx])._Val[0] * Cd(&y[i*incy])._Val[0];
			zdotc._Val[1] += Cd(&x[i*incx])._Val[1] * Cd(&y[i*incy])._Val[1];
		}
	}
	pCd(z) = zdotc;
}
#else
	_Complex double zdotc = 0.0;
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += Cd(&x[i]) * Cd(&y[i]);
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += Cd(&x[i*incx]) * Cd(&y[i*incy]);
		}
	}
	pCd(z) = zdotc;
}
#endif
/*  -- translated by f2c (version 20000121).
   You must link the resulting object file with the libraries:
	-lf2c -lm   (in that order)
*/




/* Table of constant values */

static logical c_true = TRUE_;
static integer c__2 = 2;
static real c_b34 = 1.f;
static integer c__1 = 1;
static real c_b36 = 0.f;
static logical c_false = FALSE_;

/* > \brief \b STGEVC */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download STGEVC + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/stgevc.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/stgevc.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/stgevc.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE STGEVC( SIDE, HOWMNY, SELECT, N, S, LDS, P, LDP, VL, */
/*                          LDVL, VR, LDVR, MM, M, WORK, INFO ) */

/*       CHARACTER          HOWMNY, SIDE */
/*       INTEGER            INFO, LDP, LDS, LDVL, LDVR, M, MM, N */
/*       LOGICAL            SELECT( * ) */
/*       REAL               P( LDP, * ), S( LDS, * ), VL( LDVL, * ), */
/*      $                   VR( LDVR, * ), WORK( * ) */



/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > STGEVC computes some or all of the right and/or left eigenvectors of */
/* > a pair of real matrices (S,P), where S is a quasi-triangular matrix */
/* > and P is upper triangular.  Matrix pairs of this type are produced by */
/* > the generalized Schur factorization of a matrix pair (A,B): */
/* > */
/* >    A = Q*S*Z**T,  B = Q*P*Z**T */
/* > */
/* > as computed by SGGHRD + SHGEQZ. */
/* > */
/* > The right eigenvector x and the left eigenvector y of (S,P) */
/* > corresponding to an eigenvalue w are defined by: */
/* > */
/* >    S*x = w*P*x,  (y**H)*S = w*(y**H)*P, */
/* > */
/* > where y**H denotes the conjugate tranpose of y. */
/* > The eigenvalues are not input to this routine, but are computed */
/* > directly from the diagonal blocks of S and P. */
/* > */
/* > This routine returns the matrices X and/or Y of right and left */
/* > eigenvectors of (S,P), or the products Z*X and/or Q*Y, */
/* > where Z and Q are input matrices. */
/* > If Q and Z are the orthogonal factors from the generalized Schur */
/* > factorization of a matrix pair (A,B), then Z*X and Q*Y */
/* > are the matrices of right and left eigenvectors of (A,B). */
/* > */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] SIDE */
/* > \verbatim */
/* >          SIDE is CHARACTER*1 */
/* >          = 'R': compute right eigenvectors only; */
/* >          = 'L': compute left eigenvectors only; */
/* >          = 'B': compute both right and left eigenvectors. */
/* > \endverbatim */
/* > */
/* > \param[in] HOWMNY */
/* > \verbatim */
/* >          HOWMNY is CHARACTER*1 */
/* >          = 'A': compute all right and/or left eigenvectors; */
/* >          = 'B': compute all right and/or left eigenvectors, */
/* >                 backtransformed by the matrices in VR and/or VL; */
/* >          = 'S': compute selected right and/or left eigenvectors, */
/* >                 specified by the logical array SELECT. */
/* > \endverbatim */
/* > */
/* > \param[in] SELECT */
/* > \verbatim */
/* >          SELECT is LOGICAL array, dimension (N) */
/* >          If HOWMNY='S', SELECT specifies the eigenvectors to be */
/* >          computed.  If w(j) is a real eigenvalue, the corresponding */
/* >          real eigenvector is computed if SELECT(j) is .TRUE.. */
/* >          If w(j) and w(j+1) are the real and imaginary parts of a */
/* >          complex eigenvalue, the corresponding complex eigenvector */
/* >          is computed if either SELECT(j) or SELECT(j+1) is .TRUE., */
/* >          and on exit SELECT(j) is set to .TRUE. and SELECT(j+1) is */
/* >          set to .FALSE.. */
/* >          Not referenced if HOWMNY = 'A' or 'B'. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrices S and P.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] S */
/* > \verbatim */
/* >          S is REAL array, dimension (LDS,N) */
/* >          The upper quasi-triangular matrix S from a generalized Schur */
/* >          factorization, as computed by SHGEQZ. */
/* > \endverbatim */
/* > */
/* > \param[in] LDS */
/* > \verbatim */
/* >          LDS is INTEGER */
/* >          The leading dimension of array S.  LDS >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in] P */
/* > \verbatim */
/* >          P is REAL array, dimension (LDP,N) */
/* >          The upper triangular matrix P from a generalized Schur */
/* >          factorization, as computed by SHGEQZ. */
/* >          2-by-2 diagonal blocks of P corresponding to 2-by-2 blocks */
/* >          of S must be in positive diagonal form. */
/* > \endverbatim */
/* > */
/* > \param[in] LDP */
/* > \verbatim */
/* >          LDP is INTEGER */
/* >          The leading dimension of array P.  LDP >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in,out] VL */
/* > \verbatim */
/* >          VL is REAL array, dimension (LDVL,MM) */
/* >          On entry, if SIDE = 'L' or 'B' and HOWMNY = 'B', VL must */
/* >          contain an N-by-N matrix Q (usually the orthogonal matrix Q */
/* >          of left Schur vectors returned by SHGEQZ). */
/* >          On exit, if SIDE = 'L' or 'B', VL contains: */
/* >          if HOWMNY = 'A', the matrix Y of left eigenvectors of (S,P); */
/* >          if HOWMNY = 'B', the matrix Q*Y; */
/* >          if HOWMNY = 'S', the left eigenvectors of (S,P) specified by */
/* >                      SELECT, stored consecutively in the columns of */
/* >                      VL, in the same order as their eigenvalues. */
/* > */
/* >          A complex eigenvector corresponding to a complex eigenvalue */
/* >          is stored in two consecutive columns, the first holding the */
/* >          real part, and the second the imaginary part. */
/* > */
/* >          Not referenced if SIDE = 'R'. */
/* > \endverbatim */
/* > */
/* > \param[in] LDVL */
/* > \verbatim */
/* >          LDVL is INTEGER */
/* >          The leading dimension of array VL.  LDVL >= 1, and if */
/* >          SIDE = 'L' or 'B', LDVL >= N. */
/* > \endverbatim */
/* > */
/* > \param[in,out] VR */
/* > \verbatim */
/* >          VR is REAL array, dimension (LDVR,MM) */
/* >          On entry, if SIDE = 'R' or 'B' and HOWMNY = 'B', VR must */
/* >          contain an N-by-N matrix Z (usually the orthogonal matrix Z */
/* >          of right Schur vectors returned by SHGEQZ). */
/* > */
/* >          On exit, if SIDE = 'R' or 'B', VR contains: */
/* >          if HOWMNY = 'A', the matrix X of right eigenvectors of (S,P); */
/* >          if HOWMNY = 'B' or 'b', the matrix Z*X; */
/* >          if HOWMNY = 'S' or 's', the right eigenvectors of (S,P) */
/* >                      specified by SELECT, stored consecutively in the */
/* >                      columns of VR, in the same order as their */
/* >                      eigenvalues. */
/* > */
/* >          A complex eigenvector corresponding to a complex eigenvalue */
/* >          is stored in two consecutive columns, the first holding the */
/* >          real part and the second the imaginary part. */
/* > */
/* >          Not referenced if SIDE = 'L'. */
/* > \endverbatim */
/* > */
/* > \param[in] LDVR */
/* > \verbatim */
/* >          LDVR is INTEGER */
/* >          The leading dimension of the array VR.  LDVR >= 1, and if */
/* >          SIDE = 'R' or 'B', LDVR >= N. */
/* > \endverbatim */
/* > */
/* > \param[in] MM */
/* > \verbatim */
/* >          MM is INTEGER */
/* >          The number of columns in the arrays VL and/or VR. MM >= M. */
/* > \endverbatim */
/* > */
/* > \param[out] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >          The number of columns in the arrays VL and/or VR actually */
/* >          used to store the eigenvectors.  If HOWMNY = 'A' or 'B', M */
/* >          is set to N.  Each selected real eigenvector occupies one */
/* >          column and each selected complex eigenvector occupies two */
/* >          columns. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is REAL array, dimension (6*N) */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit. */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value. */
/* >          > 0:  the 2-by-2 block (INFO:INFO+1) does not have a complex */
/* >                eigenvalue. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup realGEcomputational */

/* > \par Further Details: */
/*  ===================== */
/* > */
/* > \verbatim */
/* > */
/* >  Allocation of workspace: */
/* >  ---------- -- --------- */
/* > */
/* >     WORK( j ) = 1-norm of j-th column of A, above the diagonal */
/* >     WORK( N+j ) = 1-norm of j-th column of B, above the diagonal */
/* >     WORK( 2*N+1:3*N ) = real part of eigenvector */
/* >     WORK( 3*N+1:4*N ) = imaginary part of eigenvector */
/* >     WORK( 4*N+1:5*N ) = real part of back-transformed eigenvector */
/* >     WORK( 5*N+1:6*N ) = imaginary part of back-transformed eigenvector */
/* > */
/* >  Rowwise vs. columnwise solution methods: */
/* >  ------- --  ---------- -------- ------- */
/* > */
/* >  Finding a generalized eigenvector consists basically of solving the */
/* >  singular triangular system */
/* > */
/* >   (A - w B) x = 0     (for right) or:   (A - w B)**H y = 0  (for left) */
/* > */
/* >  Consider finding the i-th right eigenvector (assume all eigenvalues */
/* >  are real). The equation to be solved is: */
/* >       n                   i */
/* >  0 = sum  C(j,k) v(k)  = sum  C(j,k) v(k)     for j = i,. . .,1 */
/* >      k=j                 k=j */
/* > */
/* >  where  C = (A - w B)  (The components v(i+1:n) are 0.) */
/* > */
/* >  The "rowwise" method is: */
/* > */
/* >  (1)  v(i) := 1 */
/* >  for j = i-1,. . .,1: */
/* >                          i */
/* >      (2) compute  s = - sum C(j,k) v(k)   and */
/* >                        k=j+1 */
/* > */
/* >      (3) v(j) := s / C(j,j) */
/* > */
/* >  Step 2 is sometimes called the "dot product" step, since it is an */
/* >  inner product between the j-th row and the portion of the eigenvector */
/* >  that has been computed so far. */
/* > */
/* >  The "columnwise" method consists basically in doing the sums */
/* >  for all the rows in parallel.  As each v(j) is computed, the */
/* >  contribution of v(j) times the j-th column of C is added to the */
/* >  partial sums.  Since FORTRAN arrays are stored columnwise, this has */
/* >  the advantage that at each step, the elements of C that are accessed */
/* >  are adjacent to one another, whereas with the rowwise method, the */
/* >  elements accessed at a step are spaced LDS (and LDP) words apart. */
/* > */
/* >  When finding left eigenvectors, the matrix in question is the */
/* >  transpose of the one in storage, so the rowwise method then */
/* >  actually accesses columns of A and B at each step, and so is the */
/* >  preferred method. */
/* > \endverbatim */
/* > */
/*  ===================================================================== */
/* Subroutine */ void stgevc_(char *side, char *howmny, logical *select, 
	integer *n, real *s, integer *lds, real *p, integer *ldp, real *vl, 
	integer *ldvl, real *vr, integer *ldvr, integer *mm, integer *m, real 
	*work, integer *info)
{
    /* System generated locals */
    integer p_dim1, p_offset, s_dim1, s_offset, vl_dim1, vl_offset, vr_dim1, 
	    vr_offset, i__1, i__2, i__3, i__4, i__5;
    real r__1, r__2, r__3, r__4, r__5, r__6;

    /* Local variables */
    integer ibeg, ieig, iend;
    real dmin__, temp, xmax, sump[4]	/* was [2][2] */, sums[4]	/* 
	    was [2][2] */, cim2a, cim2b, cre2a, cre2b;
    extern /* Subroutine */ void slag2_(real *, integer *, real *, integer *, 
	    real *, real *, real *, real *, real *, real *);
    real temp2, bdiag[2];
    integer i__, j;
    real acoef, scale;
    logical ilall;
    integer iside;
    real sbeta;
    extern logical lsame_(char *, char *);
    logical il2by2;
    integer iinfo;
    real small;
    logical compl;
    real anorm, bnorm;
    logical compr;
    extern /* Subroutine */ void sgemv_(char *, integer *, integer *, real *, 
	    real *, integer *, real *, integer *, real *, real *, integer *), slaln2_(logical *, integer *, integer *, real *, real *, 
	    real *, integer *, real *, real *, real *, integer *, real *, 
	    real *, real *, integer *, real *, real *, integer *);
    real temp2i, temp2r;
    integer ja;
    logical ilabad, ilbbad;
    integer jc, je, na;
    real acoefa, bcoefa, cimaga, cimagb;
    logical ilback;
    integer im;
    real bcoefi, ascale, bscale, creala;
    integer jr;
    real crealb;
    extern /* Subroutine */ void slabad_(real *, real *);
    real bcoefr;
    integer jw, nw;
    extern real slamch_(char *);
    real salfar, safmin;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    real xscale, bignum;
    logical ilcomp, ilcplx;
    extern /* Subroutine */ void slacpy_(char *, integer *, integer *, real *, 
	    integer *, real *, integer *);
    integer ihwmny;
    real big;
    logical lsa, lsb;
    real ulp, sum[4]	/* was [2][2] */;


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */



/*  ===================================================================== */


/*     Decode and Test the input parameters */

    /* Parameter adjustments */
    --select;
    s_dim1 = *lds;
    s_offset = 1 + s_dim1 * 1;
    s -= s_offset;
    p_dim1 = *ldp;
    p_offset = 1 + p_dim1 * 1;
    p -= p_offset;
    vl_dim1 = *ldvl;
    vl_offset = 1 + vl_dim1 * 1;
    vl -= vl_offset;
    vr_dim1 = *ldvr;
    vr_offset = 1 + vr_dim1 * 1;
    vr -= vr_offset;
    --work;

    /* Function Body */
    if (lsame_(howmny, "A")) {
	ihwmny = 1;
	ilall = TRUE_;
	ilback = FALSE_;
    } else if (lsame_(howmny, "S")) {
	ihwmny = 2;
	ilall = FALSE_;
	ilback = FALSE_;
    } else if (lsame_(howmny, "B")) {
	ihwmny = 3;
	ilall = TRUE_;
	ilback = TRUE_;
    } else {
	ihwmny = -1;
	ilall = TRUE_;
    }

    if (lsame_(side, "R")) {
	iside = 1;
	compl = FALSE_;
	compr = TRUE_;
    } else if (lsame_(side, "L")) {
	iside = 2;
	compl = TRUE_;
	compr = FALSE_;
    } else if (lsame_(side, "B")) {
	iside = 3;
	compl = TRUE_;
	compr = TRUE_;
    } else {
	iside = -1;
    }

    *info = 0;
    if (iside < 0) {
	*info = -1;
    } else if (ihwmny < 0) {
	*info = -2;
    } else if (*n < 0) {
	*info = -4;
    } else if (*lds < f2cmax(1,*n)) {
	*info = -6;
    } else if (*ldp < f2cmax(1,*n)) {
	*info = -8;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("STGEVC", &i__1, (ftnlen)6);
	return;
    }

/*     Count the number of eigenvectors to be computed */

    if (! ilall) {
	im = 0;
	ilcplx = FALSE_;
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    if (ilcplx) {
		ilcplx = FALSE_;
		goto L10;
	    }
	    if (j < *n) {
		if (s[j + 1 + j * s_dim1] != 0.f) {
		    ilcplx = TRUE_;
		}
	    }
	    if (ilcplx) {
		if (select[j] || select[j + 1]) {
		    im += 2;
		}
	    } else {
		if (select[j]) {
		    ++im;
		}
	    }
L10:
	    ;
	}
    } else {
	im = *n;
    }

/*     Check 2-by-2 diagonal blocks of A, B */

    ilabad = FALSE_;
    ilbbad = FALSE_;
    i__1 = *n - 1;
    for (j = 1; j <= i__1; ++j) {
	if (s[j + 1 + j * s_dim1] != 0.f) {
	    if (p[j + j * p_dim1] == 0.f || p[j + 1 + (j + 1) * p_dim1] == 
		    0.f || p[j + (j + 1) * p_dim1] != 0.f) {
		ilbbad = TRUE_;
	    }
	    if (j < *n - 1) {
		if (s[j + 2 + (j + 1) * s_dim1] != 0.f) {
		    ilabad = TRUE_;
		}
	    }
	}
/* L20: */
    }

    if (ilabad) {
	*info = -5;
    } else if (ilbbad) {
	*info = -7;
    } else if (compl && *ldvl < *n || *ldvl < 1) {
	*info = -10;
    } else if (compr && *ldvr < *n || *ldvr < 1) {
	*info = -12;
    } else if (*mm < im) {
	*info = -13;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("STGEVC", &i__1, (ftnlen)6);
	return;
    }

/*     Quick return if possible */

    *m = im;
    if (*n == 0) {
	return;
    }

/*     Machine Constants */

    safmin = slamch_("Safe minimum");
    big = 1.f / safmin;
    slabad_(&safmin, &big);
    ulp = slamch_("Epsilon") * slamch_("Base");
    small = safmin * *n / ulp;
    big = 1.f / small;
    bignum = 1.f / (safmin * *n);

/*     Compute the 1-norm of each column of the strictly upper triangular */
/*     part (i.e., excluding all elements belonging to the diagonal */
/*     blocks) of A and B to check for possible overflow in the */
/*     triangular solver. */

    anorm = (r__1 = s[s_dim1 + 1], abs(r__1));
    if (*n > 1) {
	anorm += (r__1 = s[s_dim1 + 2], abs(r__1));
    }
    bnorm = (r__1 = p[p_dim1 + 1], abs(r__1));
    work[1] = 0.f;
    work[*n + 1] = 0.f;

    i__1 = *n;
    for (j = 2; j <= i__1; ++j) {
	temp = 0.f;
	temp2 = 0.f;
	if (s[j + (j - 1) * s_dim1] == 0.f) {
	    iend = j - 1;
	} else {
	    iend = j - 2;
	}
	i__2 = iend;
	for (i__ = 1; i__ <= i__2; ++i__) {
	    temp += (r__1 = s[i__ + j * s_dim1], abs(r__1));
	    temp2 += (r__1 = p[i__ + j * p_dim1], abs(r__1));
/* L30: */
	}
	work[j] = temp;
	work[*n + j] = temp2;
/* Computing MIN */
	i__3 = j + 1;
	i__2 = f2cmin(i__3,*n);
	for (i__ = iend + 1; i__ <= i__2; ++i__) {
	    temp += (r__1 = s[i__ + j * s_dim1], abs(r__1));
	    temp2 += (r__1 = p[i__ + j * p_dim1], abs(r__1));
/* L40: */
	}
	anorm = f2cmax(anorm,temp);
	bnorm = f2cmax(bnorm,temp2);
/* L50: */
    }

    ascale = 1.f / f2cmax(anorm,safmin);
    bscale = 1.f / f2cmax(bnorm,safmin);

/*     Left eigenvectors */

    if (compl) {
	ieig = 0;

/*        Main loop over eigenvalues */

	ilcplx = FALSE_;
	i__1 = *n;
	for (je = 1; je <= i__1; ++je) {

/*           Skip this iteration if (a) HOWMNY='S' and SELECT=.FALSE., or */
/*           (b) this would be the second of a complex pair. */
/*           Check for complex eigenvalue, so as to be sure of which */
/*           entry(-ies) of SELECT to look at. */

	    if (ilcplx) {
		ilcplx = FALSE_;
		goto L220;
	    }
	    nw = 1;
	    if (je < *n) {
		if (s[je + 1 + je * s_dim1] != 0.f) {
		    ilcplx = TRUE_;
		    nw = 2;
		}
	    }
	    if (ilall) {
		ilcomp = TRUE_;
	    } else if (ilcplx) {
		ilcomp = select[je] || select[je + 1];
	    } else {
		ilcomp = select[je];
	    }
	    if (! ilcomp) {
		goto L220;
	    }

/*           Decide if (a) singular pencil, (b) real eigenvalue, or */
/*           (c) complex eigenvalue. */

	    if (! ilcplx) {
		if ((r__1 = s[je + je * s_dim1], abs(r__1)) <= safmin && (
			r__2 = p[je + je * p_dim1], abs(r__2)) <= safmin) {

/*                 Singular matrix pencil -- return unit eigenvector */

		    ++ieig;
		    i__2 = *n;
		    for (jr = 1; jr <= i__2; ++jr) {
			vl[jr + ieig * vl_dim1] = 0.f;
/* L60: */
		    }
		    vl[ieig + ieig * vl_dim1] = 1.f;
		    goto L220;
		}
	    }

/*           Clear vector */

	    i__2 = nw * *n;
	    for (jr = 1; jr <= i__2; ++jr) {
		work[(*n << 1) + jr] = 0.f;
/* L70: */
	    }
/*                                                 T */
/*           Compute coefficients in  ( a A - b B )  y = 0 */
/*              a  is  ACOEF */
/*              b  is  BCOEFR + i*BCOEFI */

	    if (! ilcplx) {

/*              Real eigenvalue */

/* Computing MAX */
		r__3 = (r__1 = s[je + je * s_dim1], abs(r__1)) * ascale, r__4 
			= (r__2 = p[je + je * p_dim1], abs(r__2)) * bscale, 
			r__3 = f2cmax(r__3,r__4);
		temp = 1.f / f2cmax(r__3,safmin);
		salfar = temp * s[je + je * s_dim1] * ascale;
		sbeta = temp * p[je + je * p_dim1] * bscale;
		acoef = sbeta * ascale;
		bcoefr = salfar * bscale;
		bcoefi = 0.f;

/*              Scale to avoid underflow */

		scale = 1.f;
		lsa = abs(sbeta) >= safmin && abs(acoef) < small;
		lsb = abs(salfar) >= safmin && abs(bcoefr) < small;
		if (lsa) {
		    scale = small / abs(sbeta) * f2cmin(anorm,big);
		}
		if (lsb) {
/* Computing MAX */
		    r__1 = scale, r__2 = small / abs(salfar) * f2cmin(bnorm,big);
		    scale = f2cmax(r__1,r__2);
		}
		if (lsa || lsb) {
/* Computing MIN */
/* Computing MAX */
		    r__3 = 1.f, r__4 = abs(acoef), r__3 = f2cmax(r__3,r__4), 
			    r__4 = abs(bcoefr);
		    r__1 = scale, r__2 = 1.f / (safmin * f2cmax(r__3,r__4));
		    scale = f2cmin(r__1,r__2);
		    if (lsa) {
			acoef = ascale * (scale * sbeta);
		    } else {
			acoef = scale * acoef;
		    }
		    if (lsb) {
			bcoefr = bscale * (scale * salfar);
		    } else {
			bcoefr = scale * bcoefr;
		    }
		}
		acoefa = abs(acoef);
		bcoefa = abs(bcoefr);

/*              First component is 1 */

		work[(*n << 1) + je] = 1.f;
		xmax = 1.f;
	    } else {

/*              Complex eigenvalue */

		r__1 = safmin * 100.f;
		slag2_(&s[je + je * s_dim1], lds, &p[je + je * p_dim1], ldp, &
			r__1, &acoef, &temp, &bcoefr, &temp2, &bcoefi);
		bcoefi = -bcoefi;
		if (bcoefi == 0.f) {
		    *info = je;
		    return;
		}

/*              Scale to avoid over/underflow */

		acoefa = abs(acoef);
		bcoefa = abs(bcoefr) + abs(bcoefi);
		scale = 1.f;
		if (acoefa * ulp < safmin && acoefa >= safmin) {
		    scale = safmin / ulp / acoefa;
		}
		if (bcoefa * ulp < safmin && bcoefa >= safmin) {
/* Computing MAX */
		    r__1 = scale, r__2 = safmin / ulp / bcoefa;
		    scale = f2cmax(r__1,r__2);
		}
		if (safmin * acoefa > ascale) {
		    scale = ascale / (safmin * acoefa);
		}
		if (safmin * bcoefa > bscale) {
/* Computing MIN */
		    r__1 = scale, r__2 = bscale / (safmin * bcoefa);
		    scale = f2cmin(r__1,r__2);
		}
		if (scale != 1.f) {
		    acoef = scale * acoef;
		    acoefa = abs(acoef);
		    bcoefr = scale * bcoefr;
		    bcoefi = scale * bcoefi;
		    bcoefa = abs(bcoefr) + abs(bcoefi);
		}

/*              Compute first two components of eigenvector */

		temp = acoef * s[je + 1 + je * s_dim1];
		temp2r = acoef * s[je + je * s_dim1] - bcoefr * p[je + je * 
			p_dim1];
		temp2i = -bcoefi * p[je + je * p_dim1];
		if (abs(temp) > abs(temp2r) + abs(temp2i)) {
		    work[(*n << 1) + je] = 1.f;
		    work[*n * 3 + je] = 0.f;
		    work[(*n << 1) + je + 1] = -temp2r / temp;
		    work[*n * 3 + je + 1] = -temp2i / temp;
		} else {
		    work[(*n << 1) + je + 1] = 1.f;
		    work[*n * 3 + je + 1] = 0.f;
		    temp = acoef * s[je + (je + 1) * s_dim1];
		    work[(*n << 1) + je] = (bcoefr * p[je + 1 + (je + 1) * 
			    p_dim1] - acoef * s[je + 1 + (je + 1) * s_dim1]) /
			     temp;
		    work[*n * 3 + je] = bcoefi * p[je + 1 + (je + 1) * p_dim1]
			     / temp;
		}
/* Computing MAX */
		r__5 = (r__1 = work[(*n << 1) + je], abs(r__1)) + (r__2 = 
			work[*n * 3 + je], abs(r__2)), r__6 = (r__3 = work[(*
			n << 1) + je + 1], abs(r__3)) + (r__4 = work[*n * 3 + 
			je + 1], abs(r__4));
		xmax = f2cmax(r__5,r__6);
	    }

/* Computing MAX */
	    r__1 = ulp * acoefa * anorm, r__2 = ulp * bcoefa * bnorm, r__1 = 
		    f2cmax(r__1,r__2);
	    dmin__ = f2cmax(r__1,safmin);

/*                                           T */
/*           Triangular solve of  (a A - b B)  y = 0 */

/*                                   T */
/*           (rowwise in  (a A - b B) , or columnwise in (a A - b B) ) */

	    il2by2 = FALSE_;

	    i__2 = *n;
	    for (j = je + nw; j <= i__2; ++j) {
		if (il2by2) {
		    il2by2 = FALSE_;
		    goto L160;
		}

		na = 1;
		bdiag[0] = p[j + j * p_dim1];
		if (j < *n) {
		    if (s[j + 1 + j * s_dim1] != 0.f) {
			il2by2 = TRUE_;
			bdiag[1] = p[j + 1 + (j + 1) * p_dim1];
			na = 2;
		    }
		}

/*              Check whether scaling is necessary for dot products */

		xscale = 1.f / f2cmax(1.f,xmax);
/* Computing MAX */
		r__1 = work[j], r__2 = work[*n + j], r__1 = f2cmax(r__1,r__2), 
			r__2 = acoefa * work[j] + bcoefa * work[*n + j];
		temp = f2cmax(r__1,r__2);
		if (il2by2) {
/* Computing MAX */
		    r__1 = temp, r__2 = work[j + 1], r__1 = f2cmax(r__1,r__2), 
			    r__2 = work[*n + j + 1], r__1 = f2cmax(r__1,r__2), 
			    r__2 = acoefa * work[j + 1] + bcoefa * work[*n + 
			    j + 1];
		    temp = f2cmax(r__1,r__2);
		}
		if (temp > bignum * xscale) {
		    i__3 = nw - 1;
		    for (jw = 0; jw <= i__3; ++jw) {
			i__4 = j - 1;
			for (jr = je; jr <= i__4; ++jr) {
			    work[(jw + 2) * *n + jr] = xscale * work[(jw + 2) 
				    * *n + jr];
/* L80: */
			}
/* L90: */
		    }
		    xmax *= xscale;
		}

/*              Compute dot products */

/*                    j-1 */
/*              SUM = sum  conjg( a*S(k,j) - b*P(k,j) )*x(k) */
/*                    k=je */

/*              To reduce the op count, this is done as */

/*              _        j-1                  _        j-1 */
/*              a*conjg( sum  S(k,j)*x(k) ) - b*conjg( sum  P(k,j)*x(k) ) */
/*                       k=je                          k=je */

/*              which may cause underflow problems if A or B are close */
/*              to underflow.  (E.g., less than SMALL.) */


		i__3 = nw;
		for (jw = 1; jw <= i__3; ++jw) {
		    i__4 = na;
		    for (ja = 1; ja <= i__4; ++ja) {
			sums[ja + (jw << 1) - 3] = 0.f;
			sump[ja + (jw << 1) - 3] = 0.f;

			i__5 = j - 1;
			for (jr = je; jr <= i__5; ++jr) {
			    sums[ja + (jw << 1) - 3] += s[jr + (j + ja - 1) * 
				    s_dim1] * work[(jw + 1) * *n + jr];
			    sump[ja + (jw << 1) - 3] += p[jr + (j + ja - 1) * 
				    p_dim1] * work[(jw + 1) * *n + jr];
/* L100: */
			}
/* L110: */
		    }
/* L120: */
		}

		i__3 = na;
		for (ja = 1; ja <= i__3; ++ja) {
		    if (ilcplx) {
			sum[ja - 1] = -acoef * sums[ja - 1] + bcoefr * sump[
				ja - 1] - bcoefi * sump[ja + 1];
			sum[ja + 1] = -acoef * sums[ja + 1] + bcoefr * sump[
				ja + 1] + bcoefi * sump[ja - 1];
		    } else {
			sum[ja - 1] = -acoef * sums[ja - 1] + bcoefr * sump[
				ja - 1];
		    }
/* L130: */
		}

/*                                  T */
/*              Solve  ( a A - b B )  y = SUM(,) */
/*              with scaling and perturbation of the denominator */

		slaln2_(&c_true, &na, &nw, &dmin__, &acoef, &s[j + j * s_dim1]
			, lds, bdiag, &bdiag[1], sum, &c__2, &bcoefr, &bcoefi,
			 &work[(*n << 1) + j], n, &scale, &temp, &iinfo);
		if (scale < 1.f) {
		    i__3 = nw - 1;
		    for (jw = 0; jw <= i__3; ++jw) {
			i__4 = j - 1;
			for (jr = je; jr <= i__4; ++jr) {
			    work[(jw + 2) * *n + jr] = scale * work[(jw + 2) *
				     *n + jr];
/* L140: */
			}
/* L150: */
		    }
		    xmax = scale * xmax;
		}
		xmax = f2cmax(xmax,temp);
L160:
		;
	    }

/*           Copy eigenvector to VL, back transforming if */
/*           HOWMNY='B'. */

	    ++ieig;
	    if (ilback) {
		i__2 = nw - 1;
		for (jw = 0; jw <= i__2; ++jw) {
		    i__3 = *n + 1 - je;
		    sgemv_("N", n, &i__3, &c_b34, &vl[je * vl_dim1 + 1], ldvl,
			     &work[(jw + 2) * *n + je], &c__1, &c_b36, &work[(
			    jw + 4) * *n + 1], &c__1);
/* L170: */
		}
		slacpy_(" ", n, &nw, &work[(*n << 2) + 1], n, &vl[je * 
			vl_dim1 + 1], ldvl);
		ibeg = 1;
	    } else {
		slacpy_(" ", n, &nw, &work[(*n << 1) + 1], n, &vl[ieig * 
			vl_dim1 + 1], ldvl);
		ibeg = je;
	    }

/*           Scale eigenvector */

	    xmax = 0.f;
	    if (ilcplx) {
		i__2 = *n;
		for (j = ibeg; j <= i__2; ++j) {
/* Computing MAX */
		    r__3 = xmax, r__4 = (r__1 = vl[j + ieig * vl_dim1], abs(
			    r__1)) + (r__2 = vl[j + (ieig + 1) * vl_dim1], 
			    abs(r__2));
		    xmax = f2cmax(r__3,r__4);
/* L180: */
		}
	    } else {
		i__2 = *n;
		for (j = ibeg; j <= i__2; ++j) {
/* Computing MAX */
		    r__2 = xmax, r__3 = (r__1 = vl[j + ieig * vl_dim1], abs(
			    r__1));
		    xmax = f2cmax(r__2,r__3);
/* L190: */
		}
	    }

	    if (xmax > safmin) {
		xscale = 1.f / xmax;

		i__2 = nw - 1;
		for (jw = 0; jw <= i__2; ++jw) {
		    i__3 = *n;
		    for (jr = ibeg; jr <= i__3; ++jr) {
			vl[jr + (ieig + jw) * vl_dim1] = xscale * vl[jr + (
				ieig + jw) * vl_dim1];
/* L200: */
		    }
/* L210: */
		}
	    }
	    ieig = ieig + nw - 1;

L220:
	    ;
	}
    }

/*     Right eigenvectors */

    if (compr) {
	ieig = im + 1;

/*        Main loop over eigenvalues */

	ilcplx = FALSE_;
	for (je = *n; je >= 1; --je) {

/*           Skip this iteration if (a) HOWMNY='S' and SELECT=.FALSE., or */
/*           (b) this would be the second of a complex pair. */
/*           Check for complex eigenvalue, so as to be sure of which */
/*           entry(-ies) of SELECT to look at -- if complex, SELECT(JE) */
/*           or SELECT(JE-1). */
/*           If this is a complex pair, the 2-by-2 diagonal block */
/*           corresponding to the eigenvalue is in rows/columns JE-1:JE */

	    if (ilcplx) {
		ilcplx = FALSE_;
		goto L500;
	    }
	    nw = 1;
	    if (je > 1) {
		if (s[je + (je - 1) * s_dim1] != 0.f) {
		    ilcplx = TRUE_;
		    nw = 2;
		}
	    }
	    if (ilall) {
		ilcomp = TRUE_;
	    } else if (ilcplx) {
		ilcomp = select[je] || select[je - 1];
	    } else {
		ilcomp = select[je];
	    }
	    if (! ilcomp) {
		goto L500;
	    }

/*           Decide if (a) singular pencil, (b) real eigenvalue, or */
/*           (c) complex eigenvalue. */

	    if (! ilcplx) {
		if ((r__1 = s[je + je * s_dim1], abs(r__1)) <= safmin && (
			r__2 = p[je + je * p_dim1], abs(r__2)) <= safmin) {

/*                 Singular matrix pencil -- unit eigenvector */

		    --ieig;
		    i__1 = *n;
		    for (jr = 1; jr <= i__1; ++jr) {
			vr[jr + ieig * vr_dim1] = 0.f;
/* L230: */
		    }
		    vr[ieig + ieig * vr_dim1] = 1.f;
		    goto L500;
		}
	    }

/*           Clear vector */

	    i__1 = nw - 1;
	    for (jw = 0; jw <= i__1; ++jw) {
		i__2 = *n;
		for (jr = 1; jr <= i__2; ++jr) {
		    work[(jw + 2) * *n + jr] = 0.f;
/* L240: */
		}
/* L250: */
	    }

/*           Compute coefficients in  ( a A - b B ) x = 0 */
/*              a  is  ACOEF */
/*              b  is  BCOEFR + i*BCOEFI */

	    if (! ilcplx) {

/*              Real eigenvalue */

/* Computing MAX */
		r__3 = (r__1 = s[je + je * s_dim1], abs(r__1)) * ascale, r__4 
			= (r__2 = p[je + je * p_dim1], abs(r__2)) * bscale, 
			r__3 = f2cmax(r__3,r__4);
		temp = 1.f / f2cmax(r__3,safmin);
		salfar = temp * s[je + je * s_dim1] * ascale;
		sbeta = temp * p[je + je * p_dim1] * bscale;
		acoef = sbeta * ascale;
		bcoefr = salfar * bscale;
		bcoefi = 0.f;

/*              Scale to avoid underflow */

		scale = 1.f;
		lsa = abs(sbeta) >= safmin && abs(acoef) < small;
		lsb = abs(salfar) >= safmin && abs(bcoefr) < small;
		if (lsa) {
		    scale = small / abs(sbeta) * f2cmin(anorm,big);
		}
		if (lsb) {
/* Computing MAX */
		    r__1 = scale, r__2 = small / abs(salfar) * f2cmin(bnorm,big);
		    scale = f2cmax(r__1,r__2);
		}
		if (lsa || lsb) {
/* Computing MIN */
/* Computing MAX */
		    r__3 = 1.f, r__4 = abs(acoef), r__3 = f2cmax(r__3,r__4), 
			    r__4 = abs(bcoefr);
		    r__1 = scale, r__2 = 1.f / (safmin * f2cmax(r__3,r__4));
		    scale = f2cmin(r__1,r__2);
		    if (lsa) {
			acoef = ascale * (scale * sbeta);
		    } else {
			acoef = scale * acoef;
		    }
		    if (lsb) {
			bcoefr = bscale * (scale * salfar);
		    } else {
			bcoefr = scale * bcoefr;
		    }
		}
		acoefa = abs(acoef);
		bcoefa = abs(bcoefr);

/*              First component is 1 */

		work[(*n << 1) + je] = 1.f;
		xmax = 1.f;

/*              Compute contribution from column JE of A and B to sum */
/*              (See "Further Details", above.) */

		i__1 = je - 1;
		for (jr = 1; jr <= i__1; ++jr) {
		    work[(*n << 1) + jr] = bcoefr * p[jr + je * p_dim1] - 
			    acoef * s[jr + je * s_dim1];
/* L260: */
		}
	    } else {

/*              Complex eigenvalue */

		r__1 = safmin * 100.f;
		slag2_(&s[je - 1 + (je - 1) * s_dim1], lds, &p[je - 1 + (je - 
			1) * p_dim1], ldp, &r__1, &acoef, &temp, &bcoefr, &
			temp2, &bcoefi);
		if (bcoefi == 0.f) {
		    *info = je - 1;
		    return;
		}

/*              Scale to avoid over/underflow */

		acoefa = abs(acoef);
		bcoefa = abs(bcoefr) + abs(bcoefi);
		scale = 1.f;
		if (acoefa * ulp < safmin && acoefa >= safmin) {
		    scale = safmin / ulp / acoefa;
		}
		if (bcoefa * ulp < safmin && bcoefa >= safmin) {
/* Computing MAX */
		    r__1 = scale, r__2 = safmin / ulp / bcoefa;
		    scale = f2cmax(r__1,r__2);
		}
		if (safmin * acoefa > ascale) {
		    scale = ascale / (safmin * acoefa);
		}
		if (safmin * bcoefa > bscale) {
/* Computing MIN */
		    r__1 = scale, r__2 = bscale / (safmin * bcoefa);
		    scale = f2cmin(r__1,r__2);
		}
		if (scale != 1.f) {
		    acoef = scale * acoef;
		    acoefa = abs(acoef);
		    bcoefr = scale * bcoefr;
		    bcoefi = scale * bcoefi;
		    bcoefa = abs(bcoefr) + abs(bcoefi);
		}

/*              Compute first two components of eigenvector */
/*              and contribution to sums */

		temp = acoef * s[je + (je - 1) * s_dim1];
		temp2r = acoef * s[je + je * s_dim1] - bcoefr * p[je + je * 
			p_dim1];
		temp2i = -bcoefi * p[je + je * p_dim1];
		if (abs(temp) >= abs(temp2r) + abs(temp2i)) {
		    work[(*n << 1) + je] = 1.f;
		    work[*n * 3 + je] = 0.f;
		    work[(*n << 1) + je - 1] = -temp2r / temp;
		    work[*n * 3 + je - 1] = -temp2i / temp;
		} else {
		    work[(*n << 1) + je - 1] = 1.f;
		    work[*n * 3 + je - 1] = 0.f;
		    temp = acoef * s[je - 1 + je * s_dim1];
		    work[(*n << 1) + je] = (bcoefr * p[je - 1 + (je - 1) * 
			    p_dim1] - acoef * s[je - 1 + (je - 1) * s_dim1]) /
			     temp;
		    work[*n * 3 + je] = bcoefi * p[je - 1 + (je - 1) * p_dim1]
			     / temp;
		}

/* Computing MAX */
		r__5 = (r__1 = work[(*n << 1) + je], abs(r__1)) + (r__2 = 
			work[*n * 3 + je], abs(r__2)), r__6 = (r__3 = work[(*
			n << 1) + je - 1], abs(r__3)) + (r__4 = work[*n * 3 + 
			je - 1], abs(r__4));
		xmax = f2cmax(r__5,r__6);

/*              Compute contribution from columns JE and JE-1 */
/*              of A and B to the sums. */

		creala = acoef * work[(*n << 1) + je - 1];
		cimaga = acoef * work[*n * 3 + je - 1];
		crealb = bcoefr * work[(*n << 1) + je - 1] - bcoefi * work[*n 
			* 3 + je - 1];
		cimagb = bcoefi * work[(*n << 1) + je - 1] + bcoefr * work[*n 
			* 3 + je - 1];
		cre2a = acoef * work[(*n << 1) + je];
		cim2a = acoef * work[*n * 3 + je];
		cre2b = bcoefr * work[(*n << 1) + je] - bcoefi * work[*n * 3 
			+ je];
		cim2b = bcoefi * work[(*n << 1) + je] + bcoefr * work[*n * 3 
			+ je];
		i__1 = je - 2;
		for (jr = 1; jr <= i__1; ++jr) {
		    work[(*n << 1) + jr] = -creala * s[jr + (je - 1) * s_dim1]
			     + crealb * p[jr + (je - 1) * p_dim1] - cre2a * s[
			    jr + je * s_dim1] + cre2b * p[jr + je * p_dim1];
		    work[*n * 3 + jr] = -cimaga * s[jr + (je - 1) * s_dim1] + 
			    cimagb * p[jr + (je - 1) * p_dim1] - cim2a * s[jr 
			    + je * s_dim1] + cim2b * p[jr + je * p_dim1];
/* L270: */
		}
	    }

/* Computing MAX */
	    r__1 = ulp * acoefa * anorm, r__2 = ulp * bcoefa * bnorm, r__1 = 
		    f2cmax(r__1,r__2);
	    dmin__ = f2cmax(r__1,safmin);

/*           Columnwise triangular solve of  (a A - b B)  x = 0 */

	    il2by2 = FALSE_;
	    for (j = je - nw; j >= 1; --j) {

/*              If a 2-by-2 block, is in position j-1:j, wait until */
/*              next iteration to process it (when it will be j:j+1) */

		if (! il2by2 && j > 1) {
		    if (s[j + (j - 1) * s_dim1] != 0.f) {
			il2by2 = TRUE_;
			goto L370;
		    }
		}
		bdiag[0] = p[j + j * p_dim1];
		if (il2by2) {
		    na = 2;
		    bdiag[1] = p[j + 1 + (j + 1) * p_dim1];
		} else {
		    na = 1;
		}

/*              Compute x(j) (and x(j+1), if 2-by-2 block) */

		slaln2_(&c_false, &na, &nw, &dmin__, &acoef, &s[j + j * 
			s_dim1], lds, bdiag, &bdiag[1], &work[(*n << 1) + j], 
			n, &bcoefr, &bcoefi, sum, &c__2, &scale, &temp, &
			iinfo);
		if (scale < 1.f) {

		    i__1 = nw - 1;
		    for (jw = 0; jw <= i__1; ++jw) {
			i__2 = je;
			for (jr = 1; jr <= i__2; ++jr) {
			    work[(jw + 2) * *n + jr] = scale * work[(jw + 2) *
				     *n + jr];
/* L280: */
			}
/* L290: */
		    }
		}
/* Computing MAX */
		r__1 = scale * xmax;
		xmax = f2cmax(r__1,temp);

		i__1 = nw;
		for (jw = 1; jw <= i__1; ++jw) {
		    i__2 = na;
		    for (ja = 1; ja <= i__2; ++ja) {
			work[(jw + 1) * *n + j + ja - 1] = sum[ja + (jw << 1) 
				- 3];
/* L300: */
		    }
/* L310: */
		}

/*              w = w + x(j)*(a S(*,j) - b P(*,j) ) with scaling */

		if (j > 1) {

/*                 Check whether scaling is necessary for sum. */

		    xscale = 1.f / f2cmax(1.f,xmax);
		    temp = acoefa * work[j] + bcoefa * work[*n + j];
		    if (il2by2) {
/* Computing MAX */
			r__1 = temp, r__2 = acoefa * work[j + 1] + bcoefa * 
				work[*n + j + 1];
			temp = f2cmax(r__1,r__2);
		    }
/* Computing MAX */
		    r__1 = f2cmax(temp,acoefa);
		    temp = f2cmax(r__1,bcoefa);
		    if (temp > bignum * xscale) {

			i__1 = nw - 1;
			for (jw = 0; jw <= i__1; ++jw) {
			    i__2 = je;
			    for (jr = 1; jr <= i__2; ++jr) {
				work[(jw + 2) * *n + jr] = xscale * work[(jw 
					+ 2) * *n + jr];
/* L320: */
			    }
/* L330: */
			}
			xmax *= xscale;
		    }

/*                 Compute the contributions of the off-diagonals of */
/*                 column j (and j+1, if 2-by-2 block) of A and B to the */
/*                 sums. */


		    i__1 = na;
		    for (ja = 1; ja <= i__1; ++ja) {
			if (ilcplx) {
			    creala = acoef * work[(*n << 1) + j + ja - 1];
			    cimaga = acoef * work[*n * 3 + j + ja - 1];
			    crealb = bcoefr * work[(*n << 1) + j + ja - 1] - 
				    bcoefi * work[*n * 3 + j + ja - 1];
			    cimagb = bcoefi * work[(*n << 1) + j + ja - 1] + 
				    bcoefr * work[*n * 3 + j + ja - 1];
			    i__2 = j - 1;
			    for (jr = 1; jr <= i__2; ++jr) {
				work[(*n << 1) + jr] = work[(*n << 1) + jr] - 
					creala * s[jr + (j + ja - 1) * s_dim1]
					 + crealb * p[jr + (j + ja - 1) * 
					p_dim1];
				work[*n * 3 + jr] = work[*n * 3 + jr] - 
					cimaga * s[jr + (j + ja - 1) * s_dim1]
					 + cimagb * p[jr + (j + ja - 1) * 
					p_dim1];
/* L340: */
			    }
			} else {
			    creala = acoef * work[(*n << 1) + j + ja - 1];
			    crealb = bcoefr * work[(*n << 1) + j + ja - 1];
			    i__2 = j - 1;
			    for (jr = 1; jr <= i__2; ++jr) {
				work[(*n << 1) + jr] = work[(*n << 1) + jr] - 
					creala * s[jr + (j + ja - 1) * s_dim1]
					 + crealb * p[jr + (j + ja - 1) * 
					p_dim1];
/* L350: */
			    }
			}
/* L360: */
		    }
		}

		il2by2 = FALSE_;
L370:
		;
	    }

/*           Copy eigenvector to VR, back transforming if */
/*           HOWMNY='B'. */

	    ieig -= nw;
	    if (ilback) {

		i__1 = nw - 1;
		for (jw = 0; jw <= i__1; ++jw) {
		    i__2 = *n;
		    for (jr = 1; jr <= i__2; ++jr) {
			work[(jw + 4) * *n + jr] = work[(jw + 2) * *n + 1] * 
				vr[jr + vr_dim1];
/* L380: */
		    }

/*                 A series of compiler directives to defeat */
/*                 vectorization for the next loop */


		    i__2 = je;
		    for (jc = 2; jc <= i__2; ++jc) {
			i__3 = *n;
			for (jr = 1; jr <= i__3; ++jr) {
			    work[(jw + 4) * *n + jr] += work[(jw + 2) * *n + 
				    jc] * vr[jr + jc * vr_dim1];
/* L390: */
			}
/* L400: */
		    }
/* L410: */
		}

		i__1 = nw - 1;
		for (jw = 0; jw <= i__1; ++jw) {
		    i__2 = *n;
		    for (jr = 1; jr <= i__2; ++jr) {
			vr[jr + (ieig + jw) * vr_dim1] = work[(jw + 4) * *n + 
				jr];
/* L420: */
		    }
/* L430: */
		}

		iend = *n;
	    } else {
		i__1 = nw - 1;
		for (jw = 0; jw <= i__1; ++jw) {
		    i__2 = *n;
		    for (jr = 1; jr <= i__2; ++jr) {
			vr[jr + (ieig + jw) * vr_dim1] = work[(jw + 2) * *n + 
				jr];
/* L440: */
		    }
/* L450: */
		}

		iend = je;
	    }

/*           Scale eigenvector */

	    xmax = 0.f;
	    if (ilcplx) {
		i__1 = iend;
		for (j = 1; j <= i__1; ++j) {
/* Computing MAX */
		    r__3 = xmax, r__4 = (r__1 = vr[j + ieig * vr_dim1], abs(
			    r__1)) + (r__2 = vr[j + (ieig + 1) * vr_dim1], 
			    abs(r__2));
		    xmax = f2cmax(r__3,r__4);
/* L460: */
		}
	    } else {
		i__1 = iend;
		for (j = 1; j <= i__1; ++j) {
/* Computing MAX */
		    r__2 = xmax, r__3 = (r__1 = vr[j + ieig * vr_dim1], abs(
			    r__1));
		    xmax = f2cmax(r__2,r__3);
/* L470: */
		}
	    }

	    if (xmax > safmin) {
		xscale = 1.f / xmax;
		i__1 = nw - 1;
		for (jw = 0; jw <= i__1; ++jw) {
		    i__2 = iend;
		    for (jr = 1; jr <= i__2; ++jr) {
			vr[jr + (ieig + jw) * vr_dim1] = xscale * vr[jr + (
				ieig + jw) * vr_dim1];
/* L480: */
		    }
/* L490: */
		}
	    }
L500:
	    ;
	}
    }

    return;

/*     End of STGEVC */

} /* stgevc_ */

