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

static integer c__1 = 1;
static integer c_n1 = -1;
static integer c__3 = 3;
static integer c__2 = 2;
static integer c__0 = 0;

/* > \brief \b DSTEBZ */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download DSTEBZ + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dstebz.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dstebz.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dstebz.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE DSTEBZ( RANGE, ORDER, N, VL, VU, IL, IU, ABSTOL, D, E, */
/*                          M, NSPLIT, W, IBLOCK, ISPLIT, WORK, IWORK, */
/*                          INFO ) */

/*       CHARACTER          ORDER, RANGE */
/*       INTEGER            IL, INFO, IU, M, N, NSPLIT */
/*       DOUBLE PRECISION   ABSTOL, VL, VU */
/*       INTEGER            IBLOCK( * ), ISPLIT( * ), IWORK( * ) */
/*       DOUBLE PRECISION   D( * ), E( * ), W( * ), WORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > DSTEBZ computes the eigenvalues of a symmetric tridiagonal */
/* > matrix T.  The user may ask for all eigenvalues, all eigenvalues */
/* > in the half-open interval (VL, VU], or the IL-th through IU-th */
/* > eigenvalues. */
/* > */
/* > To avoid overflow, the matrix must be scaled so that its */
/* > largest element is no greater than overflow**(1/2) * underflow**(1/4) in absolute value, and for greatest
 */
/* > accuracy, it should not be much smaller than that. */
/* > */
/* > See W. Kahan "Accurate Eigenvalues of a Symmetric Tridiagonal */
/* > Matrix", Report CS41, Computer Science Dept., Stanford */
/* > University, July 21, 1966. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] RANGE */
/* > \verbatim */
/* >          RANGE is CHARACTER*1 */
/* >          = 'A': ("All")   all eigenvalues will be found. */
/* >          = 'V': ("Value") all eigenvalues in the half-open interval */
/* >                           (VL, VU] will be found. */
/* >          = 'I': ("Index") the IL-th through IU-th eigenvalues (of the */
/* >                           entire matrix) will be found. */
/* > \endverbatim */
/* > */
/* > \param[in] ORDER */
/* > \verbatim */
/* >          ORDER is CHARACTER*1 */
/* >          = 'B': ("By Block") the eigenvalues will be grouped by */
/* >                              split-off block (see IBLOCK, ISPLIT) and */
/* >                              ordered from smallest to largest within */
/* >                              the block. */
/* >          = 'E': ("Entire matrix") */
/* >                              the eigenvalues for the entire matrix */
/* >                              will be ordered from smallest to */
/* >                              largest. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the tridiagonal matrix T.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] VL */
/* > \verbatim */
/* >          VL is DOUBLE PRECISION */
/* > */
/* >          If RANGE='V', the lower bound of the interval to */
/* >          be searched for eigenvalues.  Eigenvalues less than or equal */
/* >          to VL, or greater than VU, will not be returned.  VL < VU. */
/* >          Not referenced if RANGE = 'A' or 'I'. */
/* > \endverbatim */
/* > */
/* > \param[in] VU */
/* > \verbatim */
/* >          VU is DOUBLE PRECISION */
/* > */
/* >          If RANGE='V', the upper bound of the interval to */
/* >          be searched for eigenvalues.  Eigenvalues less than or equal */
/* >          to VL, or greater than VU, will not be returned.  VL < VU. */
/* >          Not referenced if RANGE = 'A' or 'I'. */
/* > \endverbatim */
/* > */
/* > \param[in] IL */
/* > \verbatim */
/* >          IL is INTEGER */
/* > */
/* >          If RANGE='I', the index of the */
/* >          smallest eigenvalue to be returned. */
/* >          1 <= IL <= IU <= N, if N > 0; IL = 1 and IU = 0 if N = 0. */
/* >          Not referenced if RANGE = 'A' or 'V'. */
/* > \endverbatim */
/* > */
/* > \param[in] IU */
/* > \verbatim */
/* >          IU is INTEGER */
/* > */
/* >          If RANGE='I', the index of the */
/* >          largest eigenvalue to be returned. */
/* >          1 <= IL <= IU <= N, if N > 0; IL = 1 and IU = 0 if N = 0. */
/* >          Not referenced if RANGE = 'A' or 'V'. */
/* > \endverbatim */
/* > */
/* > \param[in] ABSTOL */
/* > \verbatim */
/* >          ABSTOL is DOUBLE PRECISION */
/* >          The absolute tolerance for the eigenvalues.  An eigenvalue */
/* >          (or cluster) is considered to be located if it has been */
/* >          determined to lie in an interval whose width is ABSTOL or */
/* >          less.  If ABSTOL is less than or equal to zero, then ULP*|T| */
/* >          will be used, where |T| means the 1-norm of T. */
/* > */
/* >          Eigenvalues will be computed most accurately when ABSTOL is */
/* >          set to twice the underflow threshold 2*DLAMCH('S'), not zero. */
/* > \endverbatim */
/* > */
/* > \param[in] D */
/* > \verbatim */
/* >          D is DOUBLE PRECISION array, dimension (N) */
/* >          The n diagonal elements of the tridiagonal matrix T. */
/* > \endverbatim */
/* > */
/* > \param[in] E */
/* > \verbatim */
/* >          E is DOUBLE PRECISION array, dimension (N-1) */
/* >          The (n-1) off-diagonal elements of the tridiagonal matrix T. */
/* > \endverbatim */
/* > */
/* > \param[out] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >          The actual number of eigenvalues found. 0 <= M <= N. */
/* >          (See also the description of INFO=2,3.) */
/* > \endverbatim */
/* > */
/* > \param[out] NSPLIT */
/* > \verbatim */
/* >          NSPLIT is INTEGER */
/* >          The number of diagonal blocks in the matrix T. */
/* >          1 <= NSPLIT <= N. */
/* > \endverbatim */
/* > */
/* > \param[out] W */
/* > \verbatim */
/* >          W is DOUBLE PRECISION array, dimension (N) */
/* >          On exit, the first M elements of W will contain the */
/* >          eigenvalues.  (DSTEBZ may use the remaining N-M elements as */
/* >          workspace.) */
/* > \endverbatim */
/* > */
/* > \param[out] IBLOCK */
/* > \verbatim */
/* >          IBLOCK is INTEGER array, dimension (N) */
/* >          At each row/column j where E(j) is zero or small, the */
/* >          matrix T is considered to split into a block diagonal */
/* >          matrix.  On exit, if INFO = 0, IBLOCK(i) specifies to which */
/* >          block (from 1 to the number of blocks) the eigenvalue W(i) */
/* >          belongs.  (DSTEBZ may use the remaining N-M elements as */
/* >          workspace.) */
/* > \endverbatim */
/* > */
/* > \param[out] ISPLIT */
/* > \verbatim */
/* >          ISPLIT is INTEGER array, dimension (N) */
/* >          The splitting points, at which T breaks up into submatrices. */
/* >          The first submatrix consists of rows/columns 1 to ISPLIT(1), */
/* >          the second of rows/columns ISPLIT(1)+1 through ISPLIT(2), */
/* >          etc., and the NSPLIT-th consists of rows/columns */
/* >          ISPLIT(NSPLIT-1)+1 through ISPLIT(NSPLIT)=N. */
/* >          (Only the first NSPLIT elements will actually be used, but */
/* >          since the user cannot know a priori what value NSPLIT will */
/* >          have, N words must be reserved for ISPLIT.) */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is DOUBLE PRECISION array, dimension (4*N) */
/* > \endverbatim */
/* > */
/* > \param[out] IWORK */
/* > \verbatim */
/* >          IWORK is INTEGER array, dimension (3*N) */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value */
/* >          > 0:  some or all of the eigenvalues failed to converge or */
/* >                were not computed: */
/* >                =1 or 3: Bisection failed to converge for some */
/* >                        eigenvalues; these eigenvalues are flagged by a */
/* >                        negative block number.  The effect is that the */
/* >                        eigenvalues may not be as accurate as the */
/* >                        absolute and relative tolerances.  This is */
/* >                        generally caused by unexpectedly inaccurate */
/* >                        arithmetic. */
/* >                =2 or 3: RANGE='I' only: Not all of the eigenvalues */
/* >                        IL:IU were found. */
/* >                        Effect: M < IU+1-IL */
/* >                        Cause:  non-monotonic arithmetic, causing the */
/* >                                Sturm sequence to be non-monotonic. */
/* >                        Cure:   recalculate, using RANGE='A', and pick */
/* >                                out eigenvalues IL:IU.  In some cases, */
/* >                                increasing the PARAMETER "FUDGE" may */
/* >                                make things work. */
/* >                = 4:    RANGE='I', and the Gershgorin interval */
/* >                        initially used was too small.  No eigenvalues */
/* >                        were computed. */
/* >                        Probable cause: your machine has sloppy */
/* >                                        floating-point arithmetic. */
/* >                        Cure: Increase the PARAMETER "FUDGE", */
/* >                              recompile, and try again. */
/* > \endverbatim */

/* > \par Internal Parameters: */
/*  ========================= */
/* > */
/* > \verbatim */
/* >  RELFAC  DOUBLE PRECISION, default = 2.0e0 */
/* >          The relative tolerance.  An interval (a,b] lies within */
/* >          "relative tolerance" if  b-a < RELFAC*ulp*f2cmax(|a|,|b|), */
/* >          where "ulp" is the machine precision (distance from 1 to */
/* >          the next larger floating point number.) */
/* > */
/* >  FUDGE   DOUBLE PRECISION, default = 2 */
/* >          A "fudge factor" to widen the Gershgorin intervals.  Ideally, */
/* >          a value of 1 should work, but on machines with sloppy */
/* >          arithmetic, this needs to be larger.  The default for */
/* >          publicly released versions should be large enough to handle */
/* >          the worst machine around.  Note that this has no effect */
/* >          on accuracy of the solution. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date June 2016 */

/* > \ingroup auxOTHERcomputational */

/*  ===================================================================== */
/* Subroutine */ void dstebz_(char *range, char *order, integer *n, doublereal 
	*vl, doublereal *vu, integer *il, integer *iu, doublereal *abstol, 
	doublereal *d__, doublereal *e, integer *m, integer *nsplit, 
	doublereal *w, integer *iblock, integer *isplit, doublereal *work, 
	integer *iwork, integer *info)
{
    /* System generated locals */
    integer i__1, i__2, i__3;
    doublereal d__1, d__2, d__3, d__4, d__5;

    /* Local variables */
    integer iend, ioff, iout, itmp1, j, jdisc;
    extern logical lsame_(char *, char *);
    integer iinfo;
    doublereal atoli;
    integer iwoff;
    doublereal bnorm;
    integer itmax;
    doublereal wkill, rtoli, tnorm;
    integer ib, jb, ie, je, nb;
    doublereal gl;
    integer im, in;
    extern doublereal dlamch_(char *);
    integer ibegin;
    doublereal gu;
    integer iw;
    extern /* Subroutine */ void dlaebz_(integer *, integer *, integer *, 
	    integer *, integer *, integer *, doublereal *, doublereal *, 
	    doublereal *, doublereal *, doublereal *, doublereal *, integer *,
	     doublereal *, doublereal *, integer *, integer *, doublereal *, 
	    integer *, integer *);
    doublereal wl;
    integer irange, idiscl;
    doublereal safemn, wu;
    integer idumma[1];
    extern integer ilaenv_(integer *, char *, char *, integer *, integer *, 
	    integer *, integer *, ftnlen, ftnlen);
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    integer idiscu, iorder;
    logical ncnvrg;
    doublereal pivmin;
    logical toofew;
    integer nwl;
    doublereal ulp, wlu, wul;
    integer nwu;
    doublereal tmp1, tmp2;


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     June 2016 */


/*  ===================================================================== */


    /* Parameter adjustments */
    --iwork;
    --work;
    --isplit;
    --iblock;
    --w;
    --e;
    --d__;

    /* Function Body */
    *info = 0;

/*     Decode RANGE */

    if (lsame_(range, "A")) {
	irange = 1;
    } else if (lsame_(range, "V")) {
	irange = 2;
    } else if (lsame_(range, "I")) {
	irange = 3;
    } else {
	irange = 0;
    }

/*     Decode ORDER */

    if (lsame_(order, "B")) {
	iorder = 2;
    } else if (lsame_(order, "E")) {
	iorder = 1;
    } else {
	iorder = 0;
    }

/*     Check for Errors */

    if (irange <= 0) {
	*info = -1;
    } else if (iorder <= 0) {
	*info = -2;
    } else if (*n < 0) {
	*info = -3;
    } else if (irange == 2) {
	if (*vl >= *vu) {
	    *info = -5;
	}
    } else if (irange == 3 && (*il < 1 || *il > f2cmax(1,*n))) {
	*info = -6;
    } else if (irange == 3 && (*iu < f2cmin(*n,*il) || *iu > *n)) {
	*info = -7;
    }

    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("DSTEBZ", &i__1, (ftnlen)6);
	return;
    }

/*     Initialize error flags */

    *info = 0;
    ncnvrg = FALSE_;
    toofew = FALSE_;

/*     Quick return if possible */

    *m = 0;
    if (*n == 0) {
	return;
    }

/*     Simplifications: */

    if (irange == 3 && *il == 1 && *iu == *n) {
	irange = 1;
    }

/*     Get machine constants */
/*     NB is the minimum vector length for vector bisection, or 0 */
/*     if only scalar is to be done. */

    safemn = dlamch_("S");
    ulp = dlamch_("P");
    rtoli = ulp * 2.;
    nb = ilaenv_(&c__1, "DSTEBZ", " ", n, &c_n1, &c_n1, &c_n1, (ftnlen)6, (
	    ftnlen)1);
    if (nb <= 1) {
	nb = 0;
    }

/*     Special Case when N=1 */

    if (*n == 1) {
	*nsplit = 1;
	isplit[1] = 1;
	if (irange == 2 && (*vl >= d__[1] || *vu < d__[1])) {
	    *m = 0;
	} else {
	    w[1] = d__[1];
	    iblock[1] = 1;
	    *m = 1;
	}
	return;
    }

/*     Compute Splitting Points */

    *nsplit = 1;
    work[*n] = 0.;
    pivmin = 1.;

    i__1 = *n;
    for (j = 2; j <= i__1; ++j) {
/* Computing 2nd power */
	d__1 = e[j - 1];
	tmp1 = d__1 * d__1;
/* Computing 2nd power */
	d__2 = ulp;
	if ((d__1 = d__[j] * d__[j - 1], abs(d__1)) * (d__2 * d__2) + safemn 
		> tmp1) {
	    isplit[*nsplit] = j - 1;
	    ++(*nsplit);
	    work[j - 1] = 0.;
	} else {
	    work[j - 1] = tmp1;
	    pivmin = f2cmax(pivmin,tmp1);
	}
/* L10: */
    }
    isplit[*nsplit] = *n;
    pivmin *= safemn;

/*     Compute Interval and ATOLI */

    if (irange == 3) {

/*        RANGE='I': Compute the interval containing eigenvalues */
/*                   IL through IU. */

/*        Compute Gershgorin interval for entire (split) matrix */
/*        and use it as the initial interval */

	gu = d__[1];
	gl = d__[1];
	tmp1 = 0.;

	i__1 = *n - 1;
	for (j = 1; j <= i__1; ++j) {
	    tmp2 = sqrt(work[j]);
/* Computing MAX */
	    d__1 = gu, d__2 = d__[j] + tmp1 + tmp2;
	    gu = f2cmax(d__1,d__2);
/* Computing MIN */
	    d__1 = gl, d__2 = d__[j] - tmp1 - tmp2;
	    gl = f2cmin(d__1,d__2);
	    tmp1 = tmp2;
/* L20: */
	}

/* Computing MAX */
	d__1 = gu, d__2 = d__[*n] + tmp1;
	gu = f2cmax(d__1,d__2);
/* Computing MIN */
	d__1 = gl, d__2 = d__[*n] - tmp1;
	gl = f2cmin(d__1,d__2);
/* Computing MAX */
	d__1 = abs(gl), d__2 = abs(gu);
	tnorm = f2cmax(d__1,d__2);
	gl = gl - tnorm * 2.1 * ulp * *n - pivmin * 4.2000000000000002;
	gu = gu + tnorm * 2.1 * ulp * *n + pivmin * 2.1;

/*        Compute Iteration parameters */

	itmax = (integer) ((log(tnorm + pivmin) - log(pivmin)) / log(2.)) + 2;
	if (*abstol <= 0.) {
	    atoli = ulp * tnorm;
	} else {
	    atoli = *abstol;
	}

	work[*n + 1] = gl;
	work[*n + 2] = gl;
	work[*n + 3] = gu;
	work[*n + 4] = gu;
	work[*n + 5] = gl;
	work[*n + 6] = gu;
	iwork[1] = -1;
	iwork[2] = -1;
	iwork[3] = *n + 1;
	iwork[4] = *n + 1;
	iwork[5] = *il - 1;
	iwork[6] = *iu;

	dlaebz_(&c__3, &itmax, n, &c__2, &c__2, &nb, &atoli, &rtoli, &pivmin, 
		&d__[1], &e[1], &work[1], &iwork[5], &work[*n + 1], &work[*n 
		+ 5], &iout, &iwork[1], &w[1], &iblock[1], &iinfo);

	if (iwork[6] == *iu) {
	    wl = work[*n + 1];
	    wlu = work[*n + 3];
	    nwl = iwork[1];
	    wu = work[*n + 4];
	    wul = work[*n + 2];
	    nwu = iwork[4];
	} else {
	    wl = work[*n + 2];
	    wlu = work[*n + 4];
	    nwl = iwork[2];
	    wu = work[*n + 3];
	    wul = work[*n + 1];
	    nwu = iwork[3];
	}

	if (nwl < 0 || nwl >= *n || nwu < 1 || nwu > *n) {
	    *info = 4;
	    return;
	}
    } else {

/*        RANGE='A' or 'V' -- Set ATOLI */

/* Computing MAX */
	d__3 = abs(d__[1]) + abs(e[1]), d__4 = (d__1 = d__[*n], abs(d__1)) + (
		d__2 = e[*n - 1], abs(d__2));
	tnorm = f2cmax(d__3,d__4);

	i__1 = *n - 1;
	for (j = 2; j <= i__1; ++j) {
/* Computing MAX */
	    d__4 = tnorm, d__5 = (d__1 = d__[j], abs(d__1)) + (d__2 = e[j - 1]
		    , abs(d__2)) + (d__3 = e[j], abs(d__3));
	    tnorm = f2cmax(d__4,d__5);
/* L30: */
	}

	if (*abstol <= 0.) {
	    atoli = ulp * tnorm;
	} else {
	    atoli = *abstol;
	}

	if (irange == 2) {
	    wl = *vl;
	    wu = *vu;
	} else {
	    wl = 0.;
	    wu = 0.;
	}
    }

/*     Find Eigenvalues -- Loop Over Blocks and recompute NWL and NWU. */
/*     NWL accumulates the number of eigenvalues .le. WL, */
/*     NWU accumulates the number of eigenvalues .le. WU */

    *m = 0;
    iend = 0;
    *info = 0;
    nwl = 0;
    nwu = 0;

    i__1 = *nsplit;
    for (jb = 1; jb <= i__1; ++jb) {
	ioff = iend;
	ibegin = ioff + 1;
	iend = isplit[jb];
	in = iend - ioff;

	if (in == 1) {

/*           Special Case -- IN=1 */

	    if (irange == 1 || wl >= d__[ibegin] - pivmin) {
		++nwl;
	    }
	    if (irange == 1 || wu >= d__[ibegin] - pivmin) {
		++nwu;
	    }
	    if (irange == 1 || wl < d__[ibegin] - pivmin && wu >= d__[ibegin] 
		    - pivmin) {
		++(*m);
		w[*m] = d__[ibegin];
		iblock[*m] = jb;
	    }
	} else {

/*           General Case -- IN > 1 */

/*           Compute Gershgorin Interval */
/*           and use it as the initial interval */

	    gu = d__[ibegin];
	    gl = d__[ibegin];
	    tmp1 = 0.;

	    i__2 = iend - 1;
	    for (j = ibegin; j <= i__2; ++j) {
		tmp2 = (d__1 = e[j], abs(d__1));
/* Computing MAX */
		d__1 = gu, d__2 = d__[j] + tmp1 + tmp2;
		gu = f2cmax(d__1,d__2);
/* Computing MIN */
		d__1 = gl, d__2 = d__[j] - tmp1 - tmp2;
		gl = f2cmin(d__1,d__2);
		tmp1 = tmp2;
/* L40: */
	    }

/* Computing MAX */
	    d__1 = gu, d__2 = d__[iend] + tmp1;
	    gu = f2cmax(d__1,d__2);
/* Computing MIN */
	    d__1 = gl, d__2 = d__[iend] - tmp1;
	    gl = f2cmin(d__1,d__2);
/* Computing MAX */
	    d__1 = abs(gl), d__2 = abs(gu);
	    bnorm = f2cmax(d__1,d__2);
	    gl = gl - bnorm * 2.1 * ulp * in - pivmin * 2.1;
	    gu = gu + bnorm * 2.1 * ulp * in + pivmin * 2.1;

/*           Compute ATOLI for the current submatrix */

	    if (*abstol <= 0.) {
/* Computing MAX */
		d__1 = abs(gl), d__2 = abs(gu);
		atoli = ulp * f2cmax(d__1,d__2);
	    } else {
		atoli = *abstol;
	    }

	    if (irange > 1) {
		if (gu < wl) {
		    nwl += in;
		    nwu += in;
		    goto L70;
		}
		gl = f2cmax(gl,wl);
		gu = f2cmin(gu,wu);
		if (gl >= gu) {
		    goto L70;
		}
	    }

/*           Set Up Initial Interval */

	    work[*n + 1] = gl;
	    work[*n + in + 1] = gu;
	    dlaebz_(&c__1, &c__0, &in, &in, &c__1, &nb, &atoli, &rtoli, &
		    pivmin, &d__[ibegin], &e[ibegin], &work[ibegin], idumma, &
		    work[*n + 1], &work[*n + (in << 1) + 1], &im, &iwork[1], &
		    w[*m + 1], &iblock[*m + 1], &iinfo);

	    nwl += iwork[1];
	    nwu += iwork[in + 1];
	    iwoff = *m - iwork[1];

/*           Compute Eigenvalues */

	    itmax = (integer) ((log(gu - gl + pivmin) - log(pivmin)) / log(2.)
		    ) + 2;
	    dlaebz_(&c__2, &itmax, &in, &in, &c__1, &nb, &atoli, &rtoli, &
		    pivmin, &d__[ibegin], &e[ibegin], &work[ibegin], idumma, &
		    work[*n + 1], &work[*n + (in << 1) + 1], &iout, &iwork[1],
		     &w[*m + 1], &iblock[*m + 1], &iinfo);

/*           Copy Eigenvalues Into W and IBLOCK */
/*           Use -JB for block number for unconverged eigenvalues. */

	    i__2 = iout;
	    for (j = 1; j <= i__2; ++j) {
		tmp1 = (work[j + *n] + work[j + in + *n]) * .5;

/*              Flag non-convergence. */

		if (j > iout - iinfo) {
		    ncnvrg = TRUE_;
		    ib = -jb;
		} else {
		    ib = jb;
		}
		i__3 = iwork[j + in] + iwoff;
		for (je = iwork[j] + 1 + iwoff; je <= i__3; ++je) {
		    w[je] = tmp1;
		    iblock[je] = ib;
/* L50: */
		}
/* L60: */
	    }

	    *m += im;
	}
L70:
	;
    }

/*     If RANGE='I', then (WL,WU) contains eigenvalues NWL+1,...,NWU */
/*     If NWL+1 < IL or NWU > IU, discard extra eigenvalues. */

    if (irange == 3) {
	im = 0;
	idiscl = *il - 1 - nwl;
	idiscu = nwu - *iu;

	if (idiscl > 0 || idiscu > 0) {
	    i__1 = *m;
	    for (je = 1; je <= i__1; ++je) {
		if (w[je] <= wlu && idiscl > 0) {
		    --idiscl;
		} else if (w[je] >= wul && idiscu > 0) {
		    --idiscu;
		} else {
		    ++im;
		    w[im] = w[je];
		    iblock[im] = iblock[je];
		}
/* L80: */
	    }
	    *m = im;
	}
	if (idiscl > 0 || idiscu > 0) {

/*           Code to deal with effects of bad arithmetic: */
/*           Some low eigenvalues to be discarded are not in (WL,WLU], */
/*           or high eigenvalues to be discarded are not in (WUL,WU] */
/*           so just kill off the smallest IDISCL/largest IDISCU */
/*           eigenvalues, by simply finding the smallest/largest */
/*           eigenvalue(s). */

/*           (If N(w) is monotone non-decreasing, this should never */
/*               happen.) */

	    if (idiscl > 0) {
		wkill = wu;
		i__1 = idiscl;
		for (jdisc = 1; jdisc <= i__1; ++jdisc) {
		    iw = 0;
		    i__2 = *m;
		    for (je = 1; je <= i__2; ++je) {
			if (iblock[je] != 0 && (w[je] < wkill || iw == 0)) {
			    iw = je;
			    wkill = w[je];
			}
/* L90: */
		    }
		    iblock[iw] = 0;
/* L100: */
		}
	    }
	    if (idiscu > 0) {

		wkill = wl;
		i__1 = idiscu;
		for (jdisc = 1; jdisc <= i__1; ++jdisc) {
		    iw = 0;
		    i__2 = *m;
		    for (je = 1; je <= i__2; ++je) {
			if (iblock[je] != 0 && (w[je] > wkill || iw == 0)) {
			    iw = je;
			    wkill = w[je];
			}
/* L110: */
		    }
		    iblock[iw] = 0;
/* L120: */
		}
	    }
	    im = 0;
	    i__1 = *m;
	    for (je = 1; je <= i__1; ++je) {
		if (iblock[je] != 0) {
		    ++im;
		    w[im] = w[je];
		    iblock[im] = iblock[je];
		}
/* L130: */
	    }
	    *m = im;
	}
	if (idiscl < 0 || idiscu < 0) {
	    toofew = TRUE_;
	}
    }

/*     If ORDER='B', do nothing -- the eigenvalues are already sorted */
/*        by block. */
/*     If ORDER='E', sort the eigenvalues from smallest to largest */

    if (iorder == 1 && *nsplit > 1) {
	i__1 = *m - 1;
	for (je = 1; je <= i__1; ++je) {
	    ie = 0;
	    tmp1 = w[je];
	    i__2 = *m;
	    for (j = je + 1; j <= i__2; ++j) {
		if (w[j] < tmp1) {
		    ie = j;
		    tmp1 = w[j];
		}
/* L140: */
	    }

	    if (ie != 0) {
		itmp1 = iblock[ie];
		w[ie] = w[je];
		iblock[ie] = iblock[je];
		w[je] = tmp1;
		iblock[je] = itmp1;
	    }
/* L150: */
	}
    }

    *info = 0;
    if (ncnvrg) {
	++(*info);
    }
    if (toofew) {
	*info += 2;
    }
    return;

/*     End of DSTEBZ */

} /* dstebz_ */

