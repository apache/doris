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

static doublecomplex c_b1 = {-1.,0.};
static doublereal c_b11 = -.125;
static integer c__1 = 1;

/* > \brief \b ZBBCSD */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download ZBBCSD + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zbbcsd.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zbbcsd.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zbbcsd.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE ZBBCSD( JOBU1, JOBU2, JOBV1T, JOBV2T, TRANS, M, P, Q, */
/*                          THETA, PHI, U1, LDU1, U2, LDU2, V1T, LDV1T, */
/*                          V2T, LDV2T, B11D, B11E, B12D, B12E, B21D, B21E, */
/*                          B22D, B22E, RWORK, LRWORK, INFO ) */

/*       CHARACTER          JOBU1, JOBU2, JOBV1T, JOBV2T, TRANS */
/*       INTEGER            INFO, LDU1, LDU2, LDV1T, LDV2T, LRWORK, M, P, Q */
/*       DOUBLE PRECISION   B11D( * ), B11E( * ), B12D( * ), B12E( * ), */
/*      $                   B21D( * ), B21E( * ), B22D( * ), B22E( * ), */
/*      $                   PHI( * ), THETA( * ), RWORK( * ) */
/*       COMPLEX*16         U1( LDU1, * ), U2( LDU2, * ), V1T( LDV1T, * ), */
/*      $                   V2T( LDV2T, * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > ZBBCSD computes the CS decomposition of a unitary matrix in */
/* > bidiagonal-block form, */
/* > */
/* > */
/* >     [ B11 | B12 0  0 ] */
/* >     [  0  |  0 -I  0 ] */
/* > X = [----------------] */
/* >     [ B21 | B22 0  0 ] */
/* >     [  0  |  0  0  I ] */
/* > */
/* >                               [  C | -S  0  0 ] */
/* >                   [ U1 |    ] [  0 |  0 -I  0 ] [ V1 |    ]**H */
/* >                 = [---------] [---------------] [---------]   . */
/* >                   [    | U2 ] [  S |  C  0  0 ] [    | V2 ] */
/* >                               [  0 |  0  0  I ] */
/* > */
/* > X is M-by-M, its top-left block is P-by-Q, and Q must be no larger */
/* > than P, M-P, or M-Q. (If Q is not the smallest index, then X must be */
/* > transposed and/or permuted. This can be done in constant time using */
/* > the TRANS and SIGNS options. See ZUNCSD for details.) */
/* > */
/* > The bidiagonal matrices B11, B12, B21, and B22 are represented */
/* > implicitly by angles THETA(1:Q) and PHI(1:Q-1). */
/* > */
/* > The unitary matrices U1, U2, V1T, and V2T are input/output. */
/* > The input matrices are pre- or post-multiplied by the appropriate */
/* > singular vector matrices. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] JOBU1 */
/* > \verbatim */
/* >          JOBU1 is CHARACTER */
/* >          = 'Y':      U1 is updated; */
/* >          otherwise:  U1 is not updated. */
/* > \endverbatim */
/* > */
/* > \param[in] JOBU2 */
/* > \verbatim */
/* >          JOBU2 is CHARACTER */
/* >          = 'Y':      U2 is updated; */
/* >          otherwise:  U2 is not updated. */
/* > \endverbatim */
/* > */
/* > \param[in] JOBV1T */
/* > \verbatim */
/* >          JOBV1T is CHARACTER */
/* >          = 'Y':      V1T is updated; */
/* >          otherwise:  V1T is not updated. */
/* > \endverbatim */
/* > */
/* > \param[in] JOBV2T */
/* > \verbatim */
/* >          JOBV2T is CHARACTER */
/* >          = 'Y':      V2T is updated; */
/* >          otherwise:  V2T is not updated. */
/* > \endverbatim */
/* > */
/* > \param[in] TRANS */
/* > \verbatim */
/* >          TRANS is CHARACTER */
/* >          = 'T':      X, U1, U2, V1T, and V2T are stored in row-major */
/* >                      order; */
/* >          otherwise:  X, U1, U2, V1T, and V2T are stored in column- */
/* >                      major order. */
/* > \endverbatim */
/* > */
/* > \param[in] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >          The number of rows and columns in X, the unitary matrix in */
/* >          bidiagonal-block form. */
/* > \endverbatim */
/* > */
/* > \param[in] P */
/* > \verbatim */
/* >          P is INTEGER */
/* >          The number of rows in the top-left block of X. 0 <= P <= M. */
/* > \endverbatim */
/* > */
/* > \param[in] Q */
/* > \verbatim */
/* >          Q is INTEGER */
/* >          The number of columns in the top-left block of X. */
/* >          0 <= Q <= MIN(P,M-P,M-Q). */
/* > \endverbatim */
/* > */
/* > \param[in,out] THETA */
/* > \verbatim */
/* >          THETA is DOUBLE PRECISION array, dimension (Q) */
/* >          On entry, the angles THETA(1),...,THETA(Q) that, along with */
/* >          PHI(1), ...,PHI(Q-1), define the matrix in bidiagonal-block */
/* >          form. On exit, the angles whose cosines and sines define the */
/* >          diagonal blocks in the CS decomposition. */
/* > \endverbatim */
/* > */
/* > \param[in,out] PHI */
/* > \verbatim */
/* >          PHI is DOUBLE PRECISION array, dimension (Q-1) */
/* >          The angles PHI(1),...,PHI(Q-1) that, along with THETA(1),..., */
/* >          THETA(Q), define the matrix in bidiagonal-block form. */
/* > \endverbatim */
/* > */
/* > \param[in,out] U1 */
/* > \verbatim */
/* >          U1 is COMPLEX*16 array, dimension (LDU1,P) */
/* >          On entry, a P-by-P matrix. On exit, U1 is postmultiplied */
/* >          by the left singular vector matrix common to [ B11 ; 0 ] and */
/* >          [ B12 0 0 ; 0 -I 0 0 ]. */
/* > \endverbatim */
/* > */
/* > \param[in] LDU1 */
/* > \verbatim */
/* >          LDU1 is INTEGER */
/* >          The leading dimension of the array U1, LDU1 >= MAX(1,P). */
/* > \endverbatim */
/* > */
/* > \param[in,out] U2 */
/* > \verbatim */
/* >          U2 is COMPLEX*16 array, dimension (LDU2,M-P) */
/* >          On entry, an (M-P)-by-(M-P) matrix. On exit, U2 is */
/* >          postmultiplied by the left singular vector matrix common to */
/* >          [ B21 ; 0 ] and [ B22 0 0 ; 0 0 I ]. */
/* > \endverbatim */
/* > */
/* > \param[in] LDU2 */
/* > \verbatim */
/* >          LDU2 is INTEGER */
/* >          The leading dimension of the array U2, LDU2 >= MAX(1,M-P). */
/* > \endverbatim */
/* > */
/* > \param[in,out] V1T */
/* > \verbatim */
/* >          V1T is COMPLEX*16 array, dimension (LDV1T,Q) */
/* >          On entry, a Q-by-Q matrix. On exit, V1T is premultiplied */
/* >          by the conjugate transpose of the right singular vector */
/* >          matrix common to [ B11 ; 0 ] and [ B21 ; 0 ]. */
/* > \endverbatim */
/* > */
/* > \param[in] LDV1T */
/* > \verbatim */
/* >          LDV1T is INTEGER */
/* >          The leading dimension of the array V1T, LDV1T >= MAX(1,Q). */
/* > \endverbatim */
/* > */
/* > \param[in,out] V2T */
/* > \verbatim */
/* >          V2T is COMPLEX*16 array, dimension (LDV2T,M-Q) */
/* >          On entry, an (M-Q)-by-(M-Q) matrix. On exit, V2T is */
/* >          premultiplied by the conjugate transpose of the right */
/* >          singular vector matrix common to [ B12 0 0 ; 0 -I 0 ] and */
/* >          [ B22 0 0 ; 0 0 I ]. */
/* > \endverbatim */
/* > */
/* > \param[in] LDV2T */
/* > \verbatim */
/* >          LDV2T is INTEGER */
/* >          The leading dimension of the array V2T, LDV2T >= MAX(1,M-Q). */
/* > \endverbatim */
/* > */
/* > \param[out] B11D */
/* > \verbatim */
/* >          B11D is DOUBLE PRECISION array, dimension (Q) */
/* >          When ZBBCSD converges, B11D contains the cosines of THETA(1), */
/* >          ..., THETA(Q). If ZBBCSD fails to converge, then B11D */
/* >          contains the diagonal of the partially reduced top-left */
/* >          block. */
/* > \endverbatim */
/* > */
/* > \param[out] B11E */
/* > \verbatim */
/* >          B11E is DOUBLE PRECISION array, dimension (Q-1) */
/* >          When ZBBCSD converges, B11E contains zeros. If ZBBCSD fails */
/* >          to converge, then B11E contains the superdiagonal of the */
/* >          partially reduced top-left block. */
/* > \endverbatim */
/* > */
/* > \param[out] B12D */
/* > \verbatim */
/* >          B12D is DOUBLE PRECISION array, dimension (Q) */
/* >          When ZBBCSD converges, B12D contains the negative sines of */
/* >          THETA(1), ..., THETA(Q). If ZBBCSD fails to converge, then */
/* >          B12D contains the diagonal of the partially reduced top-right */
/* >          block. */
/* > \endverbatim */
/* > */
/* > \param[out] B12E */
/* > \verbatim */
/* >          B12E is DOUBLE PRECISION array, dimension (Q-1) */
/* >          When ZBBCSD converges, B12E contains zeros. If ZBBCSD fails */
/* >          to converge, then B12E contains the subdiagonal of the */
/* >          partially reduced top-right block. */
/* > \endverbatim */
/* > */
/* > \param[out] B21D */
/* > \verbatim */
/* >          B21D is DOUBLE PRECISION array, dimension (Q) */
/* >          When ZBBCSD converges, B21D contains the negative sines of */
/* >          THETA(1), ..., THETA(Q). If ZBBCSD fails to converge, then */
/* >          B21D contains the diagonal of the partially reduced bottom-left */
/* >          block. */
/* > \endverbatim */
/* > */
/* > \param[out] B21E */
/* > \verbatim */
/* >          B21E is DOUBLE PRECISION array, dimension (Q-1) */
/* >          When ZBBCSD converges, B21E contains zeros. If ZBBCSD fails */
/* >          to converge, then B21E contains the subdiagonal of the */
/* >          partially reduced bottom-left block. */
/* > \endverbatim */
/* > */
/* > \param[out] B22D */
/* > \verbatim */
/* >          B22D is DOUBLE PRECISION array, dimension (Q) */
/* >          When ZBBCSD converges, B22D contains the negative sines of */
/* >          THETA(1), ..., THETA(Q). If ZBBCSD fails to converge, then */
/* >          B22D contains the diagonal of the partially reduced bottom-right */
/* >          block. */
/* > \endverbatim */
/* > */
/* > \param[out] B22E */
/* > \verbatim */
/* >          B22E is DOUBLE PRECISION array, dimension (Q-1) */
/* >          When ZBBCSD converges, B22E contains zeros. If ZBBCSD fails */
/* >          to converge, then B22E contains the subdiagonal of the */
/* >          partially reduced bottom-right block. */
/* > \endverbatim */
/* > */
/* > \param[out] RWORK */
/* > \verbatim */
/* >          RWORK is DOUBLE PRECISION array, dimension (MAX(1,LRWORK)) */
/* >          On exit, if INFO = 0, RWORK(1) returns the optimal LRWORK. */
/* > \endverbatim */
/* > */
/* > \param[in] LRWORK */
/* > \verbatim */
/* >          LRWORK is INTEGER */
/* >          The dimension of the array RWORK. LRWORK >= MAX(1,8*Q). */
/* > */
/* >          If LRWORK = -1, then a workspace query is assumed; the */
/* >          routine only calculates the optimal size of the RWORK array, */
/* >          returns this value as the first entry of the work array, and */
/* >          no error message related to LRWORK is issued by XERBLA. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit. */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value. */
/* >          > 0:  if ZBBCSD did not converge, INFO specifies the number */
/* >                of nonzero entries in PHI, and B11D, B11E, etc., */
/* >                contain the partially reduced matrix. */
/* > \endverbatim */

/* > \par Internal Parameters: */
/*  ========================= */
/* > */
/* > \verbatim */
/* >  TOLMUL  DOUBLE PRECISION, default = MAX(10,MIN(100,EPS**(-1/8))) */
/* >          TOLMUL controls the convergence criterion of the QR loop. */
/* >          Angles THETA(i), PHI(i) are rounded to 0 or PI/2 when they */
/* >          are within TOLMUL*EPS of either bound. */
/* > \endverbatim */

/* > \par References: */
/*  ================ */
/* > */
/* >  [1] Brian D. Sutton. Computing the complete CS decomposition. Numer. */
/* >      Algorithms, 50(1):33-65, 2009. */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date June 2016 */

/* > \ingroup complex16OTHERcomputational */

/*  ===================================================================== */
/* Subroutine */ void zbbcsd_(char *jobu1, char *jobu2, char *jobv1t, char *
	jobv2t, char *trans, integer *m, integer *p, integer *q, doublereal *
	theta, doublereal *phi, doublecomplex *u1, integer *ldu1, 
	doublecomplex *u2, integer *ldu2, doublecomplex *v1t, integer *ldv1t, 
	doublecomplex *v2t, integer *ldv2t, doublereal *b11d, doublereal *
	b11e, doublereal *b12d, doublereal *b12e, doublereal *b21d, 
	doublereal *b21e, doublereal *b22d, doublereal *b22e, doublereal *
	rwork, integer *lrwork, integer *info)
{
    /* System generated locals */
    integer u1_dim1, u1_offset, u2_dim1, u2_offset, v1t_dim1, v1t_offset, 
	    v2t_dim1, v2t_offset, i__1, i__2;
    doublereal d__1, d__2, d__3, d__4;

    /* Local variables */
    integer imin, mini, imax, iter;
    doublereal unfl, temp;
    logical colmajor;
    doublereal thetamin, thetamax;
    logical restart11, restart12, restart21, restart22;
    extern /* Subroutine */ void dlas2_(doublereal *, doublereal *, doublereal 
	    *, doublereal *, doublereal *);
    integer iu1cs, iu2cs, iu1sn, iu2sn, i__, j;
    doublereal r__;
    extern logical lsame_(char *, char *);
    extern /* Subroutine */ void zscal_(integer *, doublecomplex *, 
	    doublecomplex *, integer *);
    integer maxit;
    doublereal dummy;
    extern /* Subroutine */ void zlasr_(char *, char *, char *, integer *, 
	    integer *, doublereal *, doublereal *, doublecomplex *, integer *), zswap_(integer *, doublecomplex *, 
	    integer *, doublecomplex *, integer *);
    doublereal x1, x2, y1, y2;
    integer lrworkmin, iv1tcs, iv2tcs;
    logical wantu1, wantu2;
    integer lrworkopt, iv1tsn, iv2tsn;
    extern doublereal dlamch_(char *);
    doublereal mu, nu, sigma11, sigma21;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    doublereal thresh, tolmul;
    extern /* Subroutine */ void mecago_();
    logical lquery;
    doublereal b11bulge;
    logical wantv1t, wantv2t;
    doublereal b12bulge, b21bulge, b22bulge, eps, tol;
    extern /* Subroutine */ void dlartgp_(doublereal *, doublereal *, 
	    doublereal *, doublereal *, doublereal *), dlartgs_(doublereal *, 
	    doublereal *, doublereal *, doublereal *, doublereal *);


/*  -- LAPACK computational routine (version 3.7.1) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     June 2016 */


/*  =================================================================== */



/*     Test input arguments */

    /* Parameter adjustments */
    --theta;
    --phi;
    u1_dim1 = *ldu1;
    u1_offset = 1 + u1_dim1 * 1;
    u1 -= u1_offset;
    u2_dim1 = *ldu2;
    u2_offset = 1 + u2_dim1 * 1;
    u2 -= u2_offset;
    v1t_dim1 = *ldv1t;
    v1t_offset = 1 + v1t_dim1 * 1;
    v1t -= v1t_offset;
    v2t_dim1 = *ldv2t;
    v2t_offset = 1 + v2t_dim1 * 1;
    v2t -= v2t_offset;
    --b11d;
    --b11e;
    --b12d;
    --b12e;
    --b21d;
    --b21e;
    --b22d;
    --b22e;
    --rwork;

    /* Function Body */
    *info = 0;
    lquery = *lrwork == -1;
    wantu1 = lsame_(jobu1, "Y");
    wantu2 = lsame_(jobu2, "Y");
    wantv1t = lsame_(jobv1t, "Y");
    wantv2t = lsame_(jobv2t, "Y");
    colmajor = ! lsame_(trans, "T");

    if (*m < 0) {
	*info = -6;
    } else if (*p < 0 || *p > *m) {
	*info = -7;
    } else if (*q < 0 || *q > *m) {
	*info = -8;
    } else if (*q > *p || *q > *m - *p || *q > *m - *q) {
	*info = -8;
    } else if (wantu1 && *ldu1 < *p) {
	*info = -12;
    } else if (wantu2 && *ldu2 < *m - *p) {
	*info = -14;
    } else if (wantv1t && *ldv1t < *q) {
	*info = -16;
    } else if (wantv2t && *ldv2t < *m - *q) {
	*info = -18;
    }

/*     Quick return if Q = 0 */

    if (*info == 0 && *q == 0) {
	lrworkmin = 1;
	rwork[1] = (doublereal) lrworkmin;
	return;
    }

/*     Compute workspace */

    if (*info == 0) {
	iu1cs = 1;
	iu1sn = iu1cs + *q;
	iu2cs = iu1sn + *q;
	iu2sn = iu2cs + *q;
	iv1tcs = iu2sn + *q;
	iv1tsn = iv1tcs + *q;
	iv2tcs = iv1tsn + *q;
	iv2tsn = iv2tcs + *q;
	lrworkopt = iv2tsn + *q - 1;
	lrworkmin = lrworkopt;
	rwork[1] = (doublereal) lrworkopt;
	if (*lrwork < lrworkmin && ! lquery) {
	    *info = -28;
	}
    }

    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("ZBBCSD", &i__1, (ftnlen)6);
	return;
    } else if (lquery) {
	return;
    }

/*     Get machine constants */

    eps = dlamch_("Epsilon");
    unfl = dlamch_("Safe minimum");
/* Computing MAX */
/* Computing MIN */
    d__3 = 100., d__4 = pow_dd(&eps, &c_b11);
    d__1 = 10., d__2 = f2cmin(d__3,d__4);
    tolmul = f2cmax(d__1,d__2);
    tol = tolmul * eps;
/* Computing MAX */
    d__1 = tol, d__2 = *q * 6 * *q * unfl;
    thresh = f2cmax(d__1,d__2);

/*     Test for negligible sines or cosines */

    i__1 = *q;
    for (i__ = 1; i__ <= i__1; ++i__) {
	if (theta[i__] < thresh) {
	    theta[i__] = 0.;
	} else if (theta[i__] > 1.57079632679489662 - thresh) {
	    theta[i__] = 1.57079632679489662;
	}
    }
    i__1 = *q - 1;
    for (i__ = 1; i__ <= i__1; ++i__) {
	if (phi[i__] < thresh) {
	    phi[i__] = 0.;
	} else if (phi[i__] > 1.57079632679489662 - thresh) {
	    phi[i__] = 1.57079632679489662;
	}
    }

/*     Initial deflation */

    imax = *q;
    while(imax > 1) {
	if (phi[imax - 1] != 0.) {
	    myexit_();
	}
	--imax;
    }
    imin = imax - 1;
    if (imin > 1) {
	while(phi[imin - 1] != 0.) {
	    --imin;
	    if (imin <= 1) {
		myexit_();
	    }
	}
    }

/*     Initialize iteration counter */

    maxit = *q * 6 * *q;
    iter = 0;

/*     Begin main iteration loop */

    while(imax > 1) {

/*        Compute the matrix entries */

	b11d[imin] = cos(theta[imin]);
	b21d[imin] = -sin(theta[imin]);
	i__1 = imax - 1;
	for (i__ = imin; i__ <= i__1; ++i__) {
	    b11e[i__] = -sin(theta[i__]) * sin(phi[i__]);
	    b11d[i__ + 1] = cos(theta[i__ + 1]) * cos(phi[i__]);
	    b12d[i__] = sin(theta[i__]) * cos(phi[i__]);
	    b12e[i__] = cos(theta[i__ + 1]) * sin(phi[i__]);
	    b21e[i__] = -cos(theta[i__]) * sin(phi[i__]);
	    b21d[i__ + 1] = -sin(theta[i__ + 1]) * cos(phi[i__]);
	    b22d[i__] = cos(theta[i__]) * cos(phi[i__]);
	    b22e[i__] = -sin(theta[i__ + 1]) * sin(phi[i__]);
	}
	b12d[imax] = sin(theta[imax]);
	b22d[imax] = cos(theta[imax]);

/*        Abort if not converging; otherwise, increment ITER */

	if (iter > maxit) {
	    *info = 0;
	    i__1 = *q;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		if (phi[i__] != 0.) {
		    ++(*info);
		}
	    }
	    return;
	}

	iter = iter + imax - imin;

/*        Compute shifts */

	thetamax = theta[imin];
	thetamin = theta[imin];
	i__1 = imax;
	for (i__ = imin + 1; i__ <= i__1; ++i__) {
	    if (theta[i__] > thetamax) {
		thetamax = theta[i__];
	    }
	    if (theta[i__] < thetamin) {
		thetamin = theta[i__];
	    }
	}

	if (thetamax > 1.57079632679489662 - thresh) {

/*           Zero on diagonals of B11 and B22; induce deflation with a */
/*           zero shift */

	    mu = 0.;
	    nu = 1.;

	} else if (thetamin < thresh) {

/*           Zero on diagonals of B12 and B22; induce deflation with a */
/*           zero shift */

	    mu = 1.;
	    nu = 0.;

	} else {

/*           Compute shifts for B11 and B21 and use the lesser */

	    dlas2_(&b11d[imax - 1], &b11e[imax - 1], &b11d[imax], &sigma11, &
		    dummy);
	    dlas2_(&b21d[imax - 1], &b21e[imax - 1], &b21d[imax], &sigma21, &
		    dummy);

	    if (sigma11 <= sigma21) {
		mu = sigma11;
/* Computing 2nd power */
		d__1 = mu;
		nu = sqrt(1. - d__1 * d__1);
		if (mu < thresh) {
		    mu = 0.;
		    nu = 1.;
		}
	    } else {
		nu = sigma21;
/* Computing 2nd power */
		d__1 = nu;
		mu = sqrt(1.f - d__1 * d__1);
		if (nu < thresh) {
		    mu = 1.;
		    nu = 0.;
		}
	    }
	}

/*        Rotate to produce bulges in B11 and B21 */

	if (mu <= nu) {
	    dlartgs_(&b11d[imin], &b11e[imin], &mu, &rwork[iv1tcs + imin - 1],
		     &rwork[iv1tsn + imin - 1]);
	} else {
	    dlartgs_(&b21d[imin], &b21e[imin], &nu, &rwork[iv1tcs + imin - 1],
		     &rwork[iv1tsn + imin - 1]);
	}

	temp = rwork[iv1tcs + imin - 1] * b11d[imin] + rwork[iv1tsn + imin - 
		1] * b11e[imin];
	b11e[imin] = rwork[iv1tcs + imin - 1] * b11e[imin] - rwork[iv1tsn + 
		imin - 1] * b11d[imin];
	b11d[imin] = temp;
	b11bulge = rwork[iv1tsn + imin - 1] * b11d[imin + 1];
	b11d[imin + 1] = rwork[iv1tcs + imin - 1] * b11d[imin + 1];
	temp = rwork[iv1tcs + imin - 1] * b21d[imin] + rwork[iv1tsn + imin - 
		1] * b21e[imin];
	b21e[imin] = rwork[iv1tcs + imin - 1] * b21e[imin] - rwork[iv1tsn + 
		imin - 1] * b21d[imin];
	b21d[imin] = temp;
	b21bulge = rwork[iv1tsn + imin - 1] * b21d[imin + 1];
	b21d[imin + 1] = rwork[iv1tcs + imin - 1] * b21d[imin + 1];

/*        Compute THETA(IMIN) */

/* Computing 2nd power */
	d__1 = b21d[imin];
/* Computing 2nd power */
	d__2 = b21bulge;
/* Computing 2nd power */
	d__3 = b11d[imin];
/* Computing 2nd power */
	d__4 = b11bulge;
	theta[imin] = atan2(sqrt(d__1 * d__1 + d__2 * d__2), sqrt(d__3 * d__3 
		+ d__4 * d__4));

/*        Chase the bulges in B11(IMIN+1,IMIN) and B21(IMIN+1,IMIN) */

/* Computing 2nd power */
	d__1 = b11d[imin];
/* Computing 2nd power */
	d__2 = b11bulge;
/* Computing 2nd power */
	d__3 = thresh;
	if (d__1 * d__1 + d__2 * d__2 > d__3 * d__3) {
	    dlartgp_(&b11bulge, &b11d[imin], &rwork[iu1sn + imin - 1], &rwork[
		    iu1cs + imin - 1], &r__);
	} else if (mu <= nu) {
	    dlartgs_(&b11e[imin], &b11d[imin + 1], &mu, &rwork[iu1cs + imin - 
		    1], &rwork[iu1sn + imin - 1]);
	} else {
	    dlartgs_(&b12d[imin], &b12e[imin], &nu, &rwork[iu1cs + imin - 1], 
		    &rwork[iu1sn + imin - 1]);
	}
/* Computing 2nd power */
	d__1 = b21d[imin];
/* Computing 2nd power */
	d__2 = b21bulge;
/* Computing 2nd power */
	d__3 = thresh;
	if (d__1 * d__1 + d__2 * d__2 > d__3 * d__3) {
	    dlartgp_(&b21bulge, &b21d[imin], &rwork[iu2sn + imin - 1], &rwork[
		    iu2cs + imin - 1], &r__);
	} else if (nu < mu) {
	    dlartgs_(&b21e[imin], &b21d[imin + 1], &nu, &rwork[iu2cs + imin - 
		    1], &rwork[iu2sn + imin - 1]);
	} else {
	    dlartgs_(&b22d[imin], &b22e[imin], &mu, &rwork[iu2cs + imin - 1], 
		    &rwork[iu2sn + imin - 1]);
	}
	rwork[iu2cs + imin - 1] = -rwork[iu2cs + imin - 1];
	rwork[iu2sn + imin - 1] = -rwork[iu2sn + imin - 1];

	temp = rwork[iu1cs + imin - 1] * b11e[imin] + rwork[iu1sn + imin - 1] 
		* b11d[imin + 1];
	b11d[imin + 1] = rwork[iu1cs + imin - 1] * b11d[imin + 1] - rwork[
		iu1sn + imin - 1] * b11e[imin];
	b11e[imin] = temp;
	if (imax > imin + 1) {
	    b11bulge = rwork[iu1sn + imin - 1] * b11e[imin + 1];
	    b11e[imin + 1] = rwork[iu1cs + imin - 1] * b11e[imin + 1];
	}
	temp = rwork[iu1cs + imin - 1] * b12d[imin] + rwork[iu1sn + imin - 1] 
		* b12e[imin];
	b12e[imin] = rwork[iu1cs + imin - 1] * b12e[imin] - rwork[iu1sn + 
		imin - 1] * b12d[imin];
	b12d[imin] = temp;
	b12bulge = rwork[iu1sn + imin - 1] * b12d[imin + 1];
	b12d[imin + 1] = rwork[iu1cs + imin - 1] * b12d[imin + 1];
	temp = rwork[iu2cs + imin - 1] * b21e[imin] + rwork[iu2sn + imin - 1] 
		* b21d[imin + 1];
	b21d[imin + 1] = rwork[iu2cs + imin - 1] * b21d[imin + 1] - rwork[
		iu2sn + imin - 1] * b21e[imin];
	b21e[imin] = temp;
	if (imax > imin + 1) {
	    b21bulge = rwork[iu2sn + imin - 1] * b21e[imin + 1];
	    b21e[imin + 1] = rwork[iu2cs + imin - 1] * b21e[imin + 1];
	}
	temp = rwork[iu2cs + imin - 1] * b22d[imin] + rwork[iu2sn + imin - 1] 
		* b22e[imin];
	b22e[imin] = rwork[iu2cs + imin - 1] * b22e[imin] - rwork[iu2sn + 
		imin - 1] * b22d[imin];
	b22d[imin] = temp;
	b22bulge = rwork[iu2sn + imin - 1] * b22d[imin + 1];
	b22d[imin + 1] = rwork[iu2cs + imin - 1] * b22d[imin + 1];

/*        Inner loop: chase bulges from B11(IMIN,IMIN+2), */
/*        B12(IMIN,IMIN+1), B21(IMIN,IMIN+2), and B22(IMIN,IMIN+1) to */
/*        bottom-right */

	i__1 = imax - 1;
	for (i__ = imin + 1; i__ <= i__1; ++i__) {

/*           Compute PHI(I-1) */

	    x1 = sin(theta[i__ - 1]) * b11e[i__ - 1] + cos(theta[i__ - 1]) * 
		    b21e[i__ - 1];
	    x2 = sin(theta[i__ - 1]) * b11bulge + cos(theta[i__ - 1]) * 
		    b21bulge;
	    y1 = sin(theta[i__ - 1]) * b12d[i__ - 1] + cos(theta[i__ - 1]) * 
		    b22d[i__ - 1];
	    y2 = sin(theta[i__ - 1]) * b12bulge + cos(theta[i__ - 1]) * 
		    b22bulge;

/* Computing 2nd power */
	    d__1 = x1;
/* Computing 2nd power */
	    d__2 = x2;
/* Computing 2nd power */
	    d__3 = y1;
/* Computing 2nd power */
	    d__4 = y2;
	    phi[i__ - 1] = atan2(sqrt(d__1 * d__1 + d__2 * d__2), sqrt(d__3 * 
		    d__3 + d__4 * d__4));

/*           Determine if there are bulges to chase or if a new direct */
/*           summand has been reached */

/* Computing 2nd power */
	    d__1 = b11e[i__ - 1];
/* Computing 2nd power */
	    d__2 = b11bulge;
/* Computing 2nd power */
	    d__3 = thresh;
	    restart11 = d__1 * d__1 + d__2 * d__2 <= d__3 * d__3;
/* Computing 2nd power */
	    d__1 = b21e[i__ - 1];
/* Computing 2nd power */
	    d__2 = b21bulge;
/* Computing 2nd power */
	    d__3 = thresh;
	    restart21 = d__1 * d__1 + d__2 * d__2 <= d__3 * d__3;
/* Computing 2nd power */
	    d__1 = b12d[i__ - 1];
/* Computing 2nd power */
	    d__2 = b12bulge;
/* Computing 2nd power */
	    d__3 = thresh;
	    restart12 = d__1 * d__1 + d__2 * d__2 <= d__3 * d__3;
/* Computing 2nd power */
	    d__1 = b22d[i__ - 1];
/* Computing 2nd power */
	    d__2 = b22bulge;
/* Computing 2nd power */
	    d__3 = thresh;
	    restart22 = d__1 * d__1 + d__2 * d__2 <= d__3 * d__3;

/*           If possible, chase bulges from B11(I-1,I+1), B12(I-1,I), */
/*           B21(I-1,I+1), and B22(I-1,I). If necessary, restart bulge- */
/*           chasing by applying the original shift again. */

	    if (! restart11 && ! restart21) {
		dlartgp_(&x2, &x1, &rwork[iv1tsn + i__ - 1], &rwork[iv1tcs + 
			i__ - 1], &r__);
	    } else if (! restart11 && restart21) {
		dlartgp_(&b11bulge, &b11e[i__ - 1], &rwork[iv1tsn + i__ - 1], 
			&rwork[iv1tcs + i__ - 1], &r__);
	    } else if (restart11 && ! restart21) {
		dlartgp_(&b21bulge, &b21e[i__ - 1], &rwork[iv1tsn + i__ - 1], 
			&rwork[iv1tcs + i__ - 1], &r__);
	    } else if (mu <= nu) {
		dlartgs_(&b11d[i__], &b11e[i__], &mu, &rwork[iv1tcs + i__ - 1]
			, &rwork[iv1tsn + i__ - 1]);
	    } else {
		dlartgs_(&b21d[i__], &b21e[i__], &nu, &rwork[iv1tcs + i__ - 1]
			, &rwork[iv1tsn + i__ - 1]);
	    }
	    rwork[iv1tcs + i__ - 1] = -rwork[iv1tcs + i__ - 1];
	    rwork[iv1tsn + i__ - 1] = -rwork[iv1tsn + i__ - 1];
	    if (! restart12 && ! restart22) {
		dlartgp_(&y2, &y1, &rwork[iv2tsn + i__ - 2], &rwork[iv2tcs + 
			i__ - 2], &r__);
	    } else if (! restart12 && restart22) {
		dlartgp_(&b12bulge, &b12d[i__ - 1], &rwork[iv2tsn + i__ - 2], 
			&rwork[iv2tcs + i__ - 2], &r__);
	    } else if (restart12 && ! restart22) {
		dlartgp_(&b22bulge, &b22d[i__ - 1], &rwork[iv2tsn + i__ - 2], 
			&rwork[iv2tcs + i__ - 2], &r__);
	    } else if (nu < mu) {
		dlartgs_(&b12e[i__ - 1], &b12d[i__], &nu, &rwork[iv2tcs + i__ 
			- 2], &rwork[iv2tsn + i__ - 2]);
	    } else {
		dlartgs_(&b22e[i__ - 1], &b22d[i__], &mu, &rwork[iv2tcs + i__ 
			- 2], &rwork[iv2tsn + i__ - 2]);
	    }

	    temp = rwork[iv1tcs + i__ - 1] * b11d[i__] + rwork[iv1tsn + i__ - 
		    1] * b11e[i__];
	    b11e[i__] = rwork[iv1tcs + i__ - 1] * b11e[i__] - rwork[iv1tsn + 
		    i__ - 1] * b11d[i__];
	    b11d[i__] = temp;
	    b11bulge = rwork[iv1tsn + i__ - 1] * b11d[i__ + 1];
	    b11d[i__ + 1] = rwork[iv1tcs + i__ - 1] * b11d[i__ + 1];
	    temp = rwork[iv1tcs + i__ - 1] * b21d[i__] + rwork[iv1tsn + i__ - 
		    1] * b21e[i__];
	    b21e[i__] = rwork[iv1tcs + i__ - 1] * b21e[i__] - rwork[iv1tsn + 
		    i__ - 1] * b21d[i__];
	    b21d[i__] = temp;
	    b21bulge = rwork[iv1tsn + i__ - 1] * b21d[i__ + 1];
	    b21d[i__ + 1] = rwork[iv1tcs + i__ - 1] * b21d[i__ + 1];
	    temp = rwork[iv2tcs + i__ - 2] * b12e[i__ - 1] + rwork[iv2tsn + 
		    i__ - 2] * b12d[i__];
	    b12d[i__] = rwork[iv2tcs + i__ - 2] * b12d[i__] - rwork[iv2tsn + 
		    i__ - 2] * b12e[i__ - 1];
	    b12e[i__ - 1] = temp;
	    b12bulge = rwork[iv2tsn + i__ - 2] * b12e[i__];
	    b12e[i__] = rwork[iv2tcs + i__ - 2] * b12e[i__];
	    temp = rwork[iv2tcs + i__ - 2] * b22e[i__ - 1] + rwork[iv2tsn + 
		    i__ - 2] * b22d[i__];
	    b22d[i__] = rwork[iv2tcs + i__ - 2] * b22d[i__] - rwork[iv2tsn + 
		    i__ - 2] * b22e[i__ - 1];
	    b22e[i__ - 1] = temp;
	    b22bulge = rwork[iv2tsn + i__ - 2] * b22e[i__];
	    b22e[i__] = rwork[iv2tcs + i__ - 2] * b22e[i__];

/*           Compute THETA(I) */

	    x1 = cos(phi[i__ - 1]) * b11d[i__] + sin(phi[i__ - 1]) * b12e[i__ 
		    - 1];
	    x2 = cos(phi[i__ - 1]) * b11bulge + sin(phi[i__ - 1]) * b12bulge;
	    y1 = cos(phi[i__ - 1]) * b21d[i__] + sin(phi[i__ - 1]) * b22e[i__ 
		    - 1];
	    y2 = cos(phi[i__ - 1]) * b21bulge + sin(phi[i__ - 1]) * b22bulge;

/* Computing 2nd power */
	    d__1 = y1;
/* Computing 2nd power */
	    d__2 = y2;
/* Computing 2nd power */
	    d__3 = x1;
/* Computing 2nd power */
	    d__4 = x2;
	    theta[i__] = atan2(sqrt(d__1 * d__1 + d__2 * d__2), sqrt(d__3 * 
		    d__3 + d__4 * d__4));

/*           Determine if there are bulges to chase or if a new direct */
/*           summand has been reached */

/* Computing 2nd power */
	    d__1 = b11d[i__];
/* Computing 2nd power */
	    d__2 = b11bulge;
/* Computing 2nd power */
	    d__3 = thresh;
	    restart11 = d__1 * d__1 + d__2 * d__2 <= d__3 * d__3;
/* Computing 2nd power */
	    d__1 = b12e[i__ - 1];
/* Computing 2nd power */
	    d__2 = b12bulge;
/* Computing 2nd power */
	    d__3 = thresh;
	    restart12 = d__1 * d__1 + d__2 * d__2 <= d__3 * d__3;
/* Computing 2nd power */
	    d__1 = b21d[i__];
/* Computing 2nd power */
	    d__2 = b21bulge;
/* Computing 2nd power */
	    d__3 = thresh;
	    restart21 = d__1 * d__1 + d__2 * d__2 <= d__3 * d__3;
/* Computing 2nd power */
	    d__1 = b22e[i__ - 1];
/* Computing 2nd power */
	    d__2 = b22bulge;
/* Computing 2nd power */
	    d__3 = thresh;
	    restart22 = d__1 * d__1 + d__2 * d__2 <= d__3 * d__3;

/*           If possible, chase bulges from B11(I+1,I), B12(I+1,I-1), */
/*           B21(I+1,I), and B22(I+1,I-1). If necessary, restart bulge- */
/*           chasing by applying the original shift again. */

	    if (! restart11 && ! restart12) {
		dlartgp_(&x2, &x1, &rwork[iu1sn + i__ - 1], &rwork[iu1cs + 
			i__ - 1], &r__);
	    } else if (! restart11 && restart12) {
		dlartgp_(&b11bulge, &b11d[i__], &rwork[iu1sn + i__ - 1], &
			rwork[iu1cs + i__ - 1], &r__);
	    } else if (restart11 && ! restart12) {
		dlartgp_(&b12bulge, &b12e[i__ - 1], &rwork[iu1sn + i__ - 1], &
			rwork[iu1cs + i__ - 1], &r__);
	    } else if (mu <= nu) {
		dlartgs_(&b11e[i__], &b11d[i__ + 1], &mu, &rwork[iu1cs + i__ 
			- 1], &rwork[iu1sn + i__ - 1]);
	    } else {
		dlartgs_(&b12d[i__], &b12e[i__], &nu, &rwork[iu1cs + i__ - 1],
			 &rwork[iu1sn + i__ - 1]);
	    }
	    if (! restart21 && ! restart22) {
		dlartgp_(&y2, &y1, &rwork[iu2sn + i__ - 1], &rwork[iu2cs + 
			i__ - 1], &r__);
	    } else if (! restart21 && restart22) {
		dlartgp_(&b21bulge, &b21d[i__], &rwork[iu2sn + i__ - 1], &
			rwork[iu2cs + i__ - 1], &r__);
	    } else if (restart21 && ! restart22) {
		dlartgp_(&b22bulge, &b22e[i__ - 1], &rwork[iu2sn + i__ - 1], &
			rwork[iu2cs + i__ - 1], &r__);
	    } else if (nu < mu) {
		dlartgs_(&b21e[i__], &b21e[i__ + 1], &nu, &rwork[iu2cs + i__ 
			- 1], &rwork[iu2sn + i__ - 1]);
	    } else {
		dlartgs_(&b22d[i__], &b22e[i__], &mu, &rwork[iu2cs + i__ - 1],
			 &rwork[iu2sn + i__ - 1]);
	    }
	    rwork[iu2cs + i__ - 1] = -rwork[iu2cs + i__ - 1];
	    rwork[iu2sn + i__ - 1] = -rwork[iu2sn + i__ - 1];

	    temp = rwork[iu1cs + i__ - 1] * b11e[i__] + rwork[iu1sn + i__ - 1]
		     * b11d[i__ + 1];
	    b11d[i__ + 1] = rwork[iu1cs + i__ - 1] * b11d[i__ + 1] - rwork[
		    iu1sn + i__ - 1] * b11e[i__];
	    b11e[i__] = temp;
	    if (i__ < imax - 1) {
		b11bulge = rwork[iu1sn + i__ - 1] * b11e[i__ + 1];
		b11e[i__ + 1] = rwork[iu1cs + i__ - 1] * b11e[i__ + 1];
	    }
	    temp = rwork[iu2cs + i__ - 1] * b21e[i__] + rwork[iu2sn + i__ - 1]
		     * b21d[i__ + 1];
	    b21d[i__ + 1] = rwork[iu2cs + i__ - 1] * b21d[i__ + 1] - rwork[
		    iu2sn + i__ - 1] * b21e[i__];
	    b21e[i__] = temp;
	    if (i__ < imax - 1) {
		b21bulge = rwork[iu2sn + i__ - 1] * b21e[i__ + 1];
		b21e[i__ + 1] = rwork[iu2cs + i__ - 1] * b21e[i__ + 1];
	    }
	    temp = rwork[iu1cs + i__ - 1] * b12d[i__] + rwork[iu1sn + i__ - 1]
		     * b12e[i__];
	    b12e[i__] = rwork[iu1cs + i__ - 1] * b12e[i__] - rwork[iu1sn + 
		    i__ - 1] * b12d[i__];
	    b12d[i__] = temp;
	    b12bulge = rwork[iu1sn + i__ - 1] * b12d[i__ + 1];
	    b12d[i__ + 1] = rwork[iu1cs + i__ - 1] * b12d[i__ + 1];
	    temp = rwork[iu2cs + i__ - 1] * b22d[i__] + rwork[iu2sn + i__ - 1]
		     * b22e[i__];
	    b22e[i__] = rwork[iu2cs + i__ - 1] * b22e[i__] - rwork[iu2sn + 
		    i__ - 1] * b22d[i__];
	    b22d[i__] = temp;
	    b22bulge = rwork[iu2sn + i__ - 1] * b22d[i__ + 1];
	    b22d[i__ + 1] = rwork[iu2cs + i__ - 1] * b22d[i__ + 1];

	}

/*        Compute PHI(IMAX-1) */

	x1 = sin(theta[imax - 1]) * b11e[imax - 1] + cos(theta[imax - 1]) * 
		b21e[imax - 1];
	y1 = sin(theta[imax - 1]) * b12d[imax - 1] + cos(theta[imax - 1]) * 
		b22d[imax - 1];
	y2 = sin(theta[imax - 1]) * b12bulge + cos(theta[imax - 1]) * 
		b22bulge;

/* Computing 2nd power */
	d__1 = y1;
/* Computing 2nd power */
	d__2 = y2;
	phi[imax - 1] = atan2((abs(x1)), sqrt(d__1 * d__1 + d__2 * d__2));

/*        Chase bulges from B12(IMAX-1,IMAX) and B22(IMAX-1,IMAX) */

/* Computing 2nd power */
	d__1 = b12d[imax - 1];
/* Computing 2nd power */
	d__2 = b12bulge;
/* Computing 2nd power */
	d__3 = thresh;
	restart12 = d__1 * d__1 + d__2 * d__2 <= d__3 * d__3;
/* Computing 2nd power */
	d__1 = b22d[imax - 1];
/* Computing 2nd power */
	d__2 = b22bulge;
/* Computing 2nd power */
	d__3 = thresh;
	restart22 = d__1 * d__1 + d__2 * d__2 <= d__3 * d__3;

	if (! restart12 && ! restart22) {
	    dlartgp_(&y2, &y1, &rwork[iv2tsn + imax - 2], &rwork[iv2tcs + 
		    imax - 2], &r__);
	} else if (! restart12 && restart22) {
	    dlartgp_(&b12bulge, &b12d[imax - 1], &rwork[iv2tsn + imax - 2], &
		    rwork[iv2tcs + imax - 2], &r__);
	} else if (restart12 && ! restart22) {
	    dlartgp_(&b22bulge, &b22d[imax - 1], &rwork[iv2tsn + imax - 2], &
		    rwork[iv2tcs + imax - 2], &r__);
	} else if (nu < mu) {
	    dlartgs_(&b12e[imax - 1], &b12d[imax], &nu, &rwork[iv2tcs + imax 
		    - 2], &rwork[iv2tsn + imax - 2]);
	} else {
	    dlartgs_(&b22e[imax - 1], &b22d[imax], &mu, &rwork[iv2tcs + imax 
		    - 2], &rwork[iv2tsn + imax - 2]);
	}

	temp = rwork[iv2tcs + imax - 2] * b12e[imax - 1] + rwork[iv2tsn + 
		imax - 2] * b12d[imax];
	b12d[imax] = rwork[iv2tcs + imax - 2] * b12d[imax] - rwork[iv2tsn + 
		imax - 2] * b12e[imax - 1];
	b12e[imax - 1] = temp;
	temp = rwork[iv2tcs + imax - 2] * b22e[imax - 1] + rwork[iv2tsn + 
		imax - 2] * b22d[imax];
	b22d[imax] = rwork[iv2tcs + imax - 2] * b22d[imax] - rwork[iv2tsn + 
		imax - 2] * b22e[imax - 1];
	b22e[imax - 1] = temp;

/*        Update singular vectors */

	if (wantu1) {
	    if (colmajor) {
		i__1 = imax - imin + 1;
		zlasr_("R", "V", "F", p, &i__1, &rwork[iu1cs + imin - 1], &
			rwork[iu1sn + imin - 1], &u1[imin * u1_dim1 + 1], 
			ldu1);
	    } else {
		i__1 = imax - imin + 1;
		zlasr_("L", "V", "F", &i__1, p, &rwork[iu1cs + imin - 1], &
			rwork[iu1sn + imin - 1], &u1[imin + u1_dim1], ldu1);
	    }
	}
	if (wantu2) {
	    if (colmajor) {
		i__1 = *m - *p;
		i__2 = imax - imin + 1;
		zlasr_("R", "V", "F", &i__1, &i__2, &rwork[iu2cs + imin - 1], 
			&rwork[iu2sn + imin - 1], &u2[imin * u2_dim1 + 1], 
			ldu2);
	    } else {
		i__1 = imax - imin + 1;
		i__2 = *m - *p;
		zlasr_("L", "V", "F", &i__1, &i__2, &rwork[iu2cs + imin - 1], 
			&rwork[iu2sn + imin - 1], &u2[imin + u2_dim1], ldu2);
	    }
	}
	if (wantv1t) {
	    if (colmajor) {
		i__1 = imax - imin + 1;
		zlasr_("L", "V", "F", &i__1, q, &rwork[iv1tcs + imin - 1], &
			rwork[iv1tsn + imin - 1], &v1t[imin + v1t_dim1], 
			ldv1t);
	    } else {
		i__1 = imax - imin + 1;
		zlasr_("R", "V", "F", q, &i__1, &rwork[iv1tcs + imin - 1], &
			rwork[iv1tsn + imin - 1], &v1t[imin * v1t_dim1 + 1], 
			ldv1t);
	    }
	}
	if (wantv2t) {
	    if (colmajor) {
		i__1 = imax - imin + 1;
		i__2 = *m - *q;
		zlasr_("L", "V", "F", &i__1, &i__2, &rwork[iv2tcs + imin - 1],
			 &rwork[iv2tsn + imin - 1], &v2t[imin + v2t_dim1], 
			ldv2t);
	    } else {
		i__1 = *m - *q;
		i__2 = imax - imin + 1;
		zlasr_("R", "V", "F", &i__1, &i__2, &rwork[iv2tcs + imin - 1],
			 &rwork[iv2tsn + imin - 1], &v2t[imin * v2t_dim1 + 1],
			 ldv2t);
	    }
	}

/*        Fix signs on B11(IMAX-1,IMAX) and B21(IMAX-1,IMAX) */

	if (b11e[imax - 1] + b21e[imax - 1] > 0.) {
	    b11d[imax] = -b11d[imax];
	    b21d[imax] = -b21d[imax];
	    if (wantv1t) {
		if (colmajor) {
		    zscal_(q, &c_b1, &v1t[imax + v1t_dim1], ldv1t);
		} else {
		    zscal_(q, &c_b1, &v1t[imax * v1t_dim1 + 1], &c__1);
		}
	    }
	}

/*        Compute THETA(IMAX) */

	x1 = cos(phi[imax - 1]) * b11d[imax] + sin(phi[imax - 1]) * b12e[imax 
		- 1];
	y1 = cos(phi[imax - 1]) * b21d[imax] + sin(phi[imax - 1]) * b22e[imax 
		- 1];

	theta[imax] = atan2((abs(y1)), (abs(x1)));

/*        Fix signs on B11(IMAX,IMAX), B12(IMAX,IMAX-1), B21(IMAX,IMAX), */
/*        and B22(IMAX,IMAX-1) */

	if (b11d[imax] + b12e[imax - 1] < 0.) {
	    b12d[imax] = -b12d[imax];
	    if (wantu1) {
		if (colmajor) {
		    zscal_(p, &c_b1, &u1[imax * u1_dim1 + 1], &c__1);
		} else {
		    zscal_(p, &c_b1, &u1[imax + u1_dim1], ldu1);
		}
	    }
	}
	if (b21d[imax] + b22e[imax - 1] > 0.) {
	    b22d[imax] = -b22d[imax];
	    if (wantu2) {
		if (colmajor) {
		    i__1 = *m - *p;
		    zscal_(&i__1, &c_b1, &u2[imax * u2_dim1 + 1], &c__1);
		} else {
		    i__1 = *m - *p;
		    zscal_(&i__1, &c_b1, &u2[imax + u2_dim1], ldu2);
		}
	    }
	}

/*        Fix signs on B12(IMAX,IMAX) and B22(IMAX,IMAX) */

	if (b12d[imax] + b22d[imax] < 0.) {
	    if (wantv2t) {
		if (colmajor) {
		    i__1 = *m - *q;
		    zscal_(&i__1, &c_b1, &v2t[imax + v2t_dim1], ldv2t);
		} else {
		    i__1 = *m - *q;
		    zscal_(&i__1, &c_b1, &v2t[imax * v2t_dim1 + 1], &c__1);
		}
	    }
	}

/*        Test for negligible sines or cosines */

	i__1 = imax;
	for (i__ = imin; i__ <= i__1; ++i__) {
	    if (theta[i__] < thresh) {
		theta[i__] = 0.;
	    } else if (theta[i__] > 1.57079632679489662 - thresh) {
		theta[i__] = 1.57079632679489662;
	    }
	}
	i__1 = imax - 1;
	for (i__ = imin; i__ <= i__1; ++i__) {
	    if (phi[i__] < thresh) {
		phi[i__] = 0.;
	    } else if (phi[i__] > 1.57079632679489662 - thresh) {
		phi[i__] = 1.57079632679489662;
	    }
	}

/*        Deflate */

	if (imax > 1) {
	    while(phi[imax - 1] == 0.) {
		--imax;
		if (imax <= 1) {
		    myexit_();
		}
	    }
	}
	if (imin > imax - 1) {
	    imin = imax - 1;
	}
	if (imin > 1) {
	    while(phi[imin - 1] != 0.) {
		--imin;
		if (imin <= 1) {
		    myexit_();
		}
	    }
	}

/*        Repeat main iteration loop */

    }

/*     Postprocessing: order THETA from least to greatest */

    i__1 = *q;
    for (i__ = 1; i__ <= i__1; ++i__) {

	mini = i__;
	thetamin = theta[i__];
	i__2 = *q;
	for (j = i__ + 1; j <= i__2; ++j) {
	    if (theta[j] < thetamin) {
		mini = j;
		thetamin = theta[j];
	    }
	}

	if (mini != i__) {
	    theta[mini] = theta[i__];
	    theta[i__] = thetamin;
	    if (colmajor) {
		if (wantu1) {
		    zswap_(p, &u1[i__ * u1_dim1 + 1], &c__1, &u1[mini * 
			    u1_dim1 + 1], &c__1);
		}
		if (wantu2) {
		    i__2 = *m - *p;
		    zswap_(&i__2, &u2[i__ * u2_dim1 + 1], &c__1, &u2[mini * 
			    u2_dim1 + 1], &c__1);
		}
		if (wantv1t) {
		    zswap_(q, &v1t[i__ + v1t_dim1], ldv1t, &v1t[mini + 
			    v1t_dim1], ldv1t);
		}
		if (wantv2t) {
		    i__2 = *m - *q;
		    zswap_(&i__2, &v2t[i__ + v2t_dim1], ldv2t, &v2t[mini + 
			    v2t_dim1], ldv2t);
		}
	    } else {
		if (wantu1) {
		    zswap_(p, &u1[i__ + u1_dim1], ldu1, &u1[mini + u1_dim1], 
			    ldu1);
		}
		if (wantu2) {
		    i__2 = *m - *p;
		    zswap_(&i__2, &u2[i__ + u2_dim1], ldu2, &u2[mini + 
			    u2_dim1], ldu2);
		}
		if (wantv1t) {
		    zswap_(q, &v1t[i__ * v1t_dim1 + 1], &c__1, &v1t[mini * 
			    v1t_dim1 + 1], &c__1);
		}
		if (wantv2t) {
		    i__2 = *m - *q;
		    zswap_(&i__2, &v2t[i__ * v2t_dim1 + 1], &c__1, &v2t[mini *
			     v2t_dim1 + 1], &c__1);
		}
	    }
	}

    }

    return;

/*     End of ZBBCSD */

} /* zbbcsd_ */

