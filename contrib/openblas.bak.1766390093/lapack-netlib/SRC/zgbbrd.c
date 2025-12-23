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

static doublecomplex c_b1 = {0.,0.};
static doublecomplex c_b2 = {1.,0.};
static integer c__1 = 1;

/* > \brief \b ZGBBRD */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download ZGBBRD + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zgbbrd.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zgbbrd.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zgbbrd.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE ZGBBRD( VECT, M, N, NCC, KL, KU, AB, LDAB, D, E, Q, */
/*                          LDQ, PT, LDPT, C, LDC, WORK, RWORK, INFO ) */

/*       CHARACTER          VECT */
/*       INTEGER            INFO, KL, KU, LDAB, LDC, LDPT, LDQ, M, N, NCC */
/*       DOUBLE PRECISION   D( * ), E( * ), RWORK( * ) */
/*       COMPLEX*16         AB( LDAB, * ), C( LDC, * ), PT( LDPT, * ), */
/*      $                   Q( LDQ, * ), WORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > ZGBBRD reduces a complex general m-by-n band matrix A to real upper */
/* > bidiagonal form B by a unitary transformation: Q**H * A * P = B. */
/* > */
/* > The routine computes B, and optionally forms Q or P**H, or computes */
/* > Q**H*C for a given matrix C. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] VECT */
/* > \verbatim */
/* >          VECT is CHARACTER*1 */
/* >          Specifies whether or not the matrices Q and P**H are to be */
/* >          formed. */
/* >          = 'N': do not form Q or P**H; */
/* >          = 'Q': form Q only; */
/* >          = 'P': form P**H only; */
/* >          = 'B': form both. */
/* > \endverbatim */
/* > */
/* > \param[in] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >          The number of rows of the matrix A.  M >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The number of columns of the matrix A.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] NCC */
/* > \verbatim */
/* >          NCC is INTEGER */
/* >          The number of columns of the matrix C.  NCC >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] KL */
/* > \verbatim */
/* >          KL is INTEGER */
/* >          The number of subdiagonals of the matrix A. KL >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] KU */
/* > \verbatim */
/* >          KU is INTEGER */
/* >          The number of superdiagonals of the matrix A. KU >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] AB */
/* > \verbatim */
/* >          AB is COMPLEX*16 array, dimension (LDAB,N) */
/* >          On entry, the m-by-n band matrix A, stored in rows 1 to */
/* >          KL+KU+1. The j-th column of A is stored in the j-th column of */
/* >          the array AB as follows: */
/* >          AB(ku+1+i-j,j) = A(i,j) for f2cmax(1,j-ku)<=i<=f2cmin(m,j+kl). */
/* >          On exit, A is overwritten by values generated during the */
/* >          reduction. */
/* > \endverbatim */
/* > */
/* > \param[in] LDAB */
/* > \verbatim */
/* >          LDAB is INTEGER */
/* >          The leading dimension of the array A. LDAB >= KL+KU+1. */
/* > \endverbatim */
/* > */
/* > \param[out] D */
/* > \verbatim */
/* >          D is DOUBLE PRECISION array, dimension (f2cmin(M,N)) */
/* >          The diagonal elements of the bidiagonal matrix B. */
/* > \endverbatim */
/* > */
/* > \param[out] E */
/* > \verbatim */
/* >          E is DOUBLE PRECISION array, dimension (f2cmin(M,N)-1) */
/* >          The superdiagonal elements of the bidiagonal matrix B. */
/* > \endverbatim */
/* > */
/* > \param[out] Q */
/* > \verbatim */
/* >          Q is COMPLEX*16 array, dimension (LDQ,M) */
/* >          If VECT = 'Q' or 'B', the m-by-m unitary matrix Q. */
/* >          If VECT = 'N' or 'P', the array Q is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDQ */
/* > \verbatim */
/* >          LDQ is INTEGER */
/* >          The leading dimension of the array Q. */
/* >          LDQ >= f2cmax(1,M) if VECT = 'Q' or 'B'; LDQ >= 1 otherwise. */
/* > \endverbatim */
/* > */
/* > \param[out] PT */
/* > \verbatim */
/* >          PT is COMPLEX*16 array, dimension (LDPT,N) */
/* >          If VECT = 'P' or 'B', the n-by-n unitary matrix P'. */
/* >          If VECT = 'N' or 'Q', the array PT is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDPT */
/* > \verbatim */
/* >          LDPT is INTEGER */
/* >          The leading dimension of the array PT. */
/* >          LDPT >= f2cmax(1,N) if VECT = 'P' or 'B'; LDPT >= 1 otherwise. */
/* > \endverbatim */
/* > */
/* > \param[in,out] C */
/* > \verbatim */
/* >          C is COMPLEX*16 array, dimension (LDC,NCC) */
/* >          On entry, an m-by-ncc matrix C. */
/* >          On exit, C is overwritten by Q**H*C. */
/* >          C is not referenced if NCC = 0. */
/* > \endverbatim */
/* > */
/* > \param[in] LDC */
/* > \verbatim */
/* >          LDC is INTEGER */
/* >          The leading dimension of the array C. */
/* >          LDC >= f2cmax(1,M) if NCC > 0; LDC >= 1 if NCC = 0. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is COMPLEX*16 array, dimension (f2cmax(M,N)) */
/* > \endverbatim */
/* > */
/* > \param[out] RWORK */
/* > \verbatim */
/* >          RWORK is DOUBLE PRECISION array, dimension (f2cmax(M,N)) */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit. */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup complex16GBcomputational */

/*  ===================================================================== */
/* Subroutine */ void zgbbrd_(char *vect, integer *m, integer *n, integer *ncc,
	 integer *kl, integer *ku, doublecomplex *ab, integer *ldab, 
	doublereal *d__, doublereal *e, doublecomplex *q, integer *ldq, 
	doublecomplex *pt, integer *ldpt, doublecomplex *c__, integer *ldc, 
	doublecomplex *work, doublereal *rwork, integer *info)
{
    /* System generated locals */
    integer ab_dim1, ab_offset, c_dim1, c_offset, pt_dim1, pt_offset, q_dim1, 
	    q_offset, i__1, i__2, i__3, i__4, i__5, i__6, i__7;
    doublecomplex z__1, z__2, z__3;

    /* Local variables */
    integer inca;
    doublereal abst;
    extern /* Subroutine */ void zrot_(integer *, doublecomplex *, integer *, 
	    doublecomplex *, integer *, doublereal *, doublecomplex *);
    integer i__, j, l;
    doublecomplex t;
    extern logical lsame_(char *, char *);
    logical wantb, wantc;
    extern /* Subroutine */ void zscal_(integer *, doublecomplex *, 
	    doublecomplex *, integer *);
    integer minmn;
    logical wantq;
    integer j1, j2, kb;
    doublecomplex ra, rb;
    doublereal rc;
    integer kk, ml, nr, mu;
    doublecomplex rs;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    integer kb1;
    extern /* Subroutine */ void zlaset_(char *, integer *, integer *, 
	    doublecomplex *, doublecomplex *, doublecomplex *, integer *), zlartg_(doublecomplex *, doublecomplex *, doublereal *, 
	    doublecomplex *, doublecomplex *), zlargv_(integer *, 
	    doublecomplex *, integer *, doublecomplex *, integer *, 
	    doublereal *, integer *);
    integer ml0;
    logical wantpt;
    integer mu0;
    extern /* Subroutine */ void zlartv_(integer *, doublecomplex *, integer *,
	     doublecomplex *, integer *, doublereal *, doublecomplex *, 
	    integer *);
    integer klm, kun, nrt, klu1;


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


/*     Test the input parameters */

    /* Parameter adjustments */
    ab_dim1 = *ldab;
    ab_offset = 1 + ab_dim1 * 1;
    ab -= ab_offset;
    --d__;
    --e;
    q_dim1 = *ldq;
    q_offset = 1 + q_dim1 * 1;
    q -= q_offset;
    pt_dim1 = *ldpt;
    pt_offset = 1 + pt_dim1 * 1;
    pt -= pt_offset;
    c_dim1 = *ldc;
    c_offset = 1 + c_dim1 * 1;
    c__ -= c_offset;
    --work;
    --rwork;

    /* Function Body */
    wantb = lsame_(vect, "B");
    wantq = lsame_(vect, "Q") || wantb;
    wantpt = lsame_(vect, "P") || wantb;
    wantc = *ncc > 0;
    klu1 = *kl + *ku + 1;
    *info = 0;
    if (! wantq && ! wantpt && ! lsame_(vect, "N")) {
	*info = -1;
    } else if (*m < 0) {
	*info = -2;
    } else if (*n < 0) {
	*info = -3;
    } else if (*ncc < 0) {
	*info = -4;
    } else if (*kl < 0) {
	*info = -5;
    } else if (*ku < 0) {
	*info = -6;
    } else if (*ldab < klu1) {
	*info = -8;
    } else if (*ldq < 1 || wantq && *ldq < f2cmax(1,*m)) {
	*info = -12;
    } else if (*ldpt < 1 || wantpt && *ldpt < f2cmax(1,*n)) {
	*info = -14;
    } else if (*ldc < 1 || wantc && *ldc < f2cmax(1,*m)) {
	*info = -16;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("ZGBBRD", &i__1, (ftnlen)6);
	return;
    }

/*     Initialize Q and P**H to the unit matrix, if needed */

    if (wantq) {
	zlaset_("Full", m, m, &c_b1, &c_b2, &q[q_offset], ldq);
    }
    if (wantpt) {
	zlaset_("Full", n, n, &c_b1, &c_b2, &pt[pt_offset], ldpt);
    }

/*     Quick return if possible. */

    if (*m == 0 || *n == 0) {
	return;
    }

    minmn = f2cmin(*m,*n);

    if (*kl + *ku > 1) {

/*        Reduce to upper bidiagonal form if KU > 0; if KU = 0, reduce */
/*        first to lower bidiagonal form and then transform to upper */
/*        bidiagonal */

	if (*ku > 0) {
	    ml0 = 1;
	    mu0 = 2;
	} else {
	    ml0 = 2;
	    mu0 = 1;
	}

/*        Wherever possible, plane rotations are generated and applied in */
/*        vector operations of length NR over the index set J1:J2:KLU1. */

/*        The complex sines of the plane rotations are stored in WORK, */
/*        and the real cosines in RWORK. */

/* Computing MIN */
	i__1 = *m - 1;
	klm = f2cmin(i__1,*kl);
/* Computing MIN */
	i__1 = *n - 1;
	kun = f2cmin(i__1,*ku);
	kb = klm + kun;
	kb1 = kb + 1;
	inca = kb1 * *ldab;
	nr = 0;
	j1 = klm + 2;
	j2 = 1 - kun;

	i__1 = minmn;
	for (i__ = 1; i__ <= i__1; ++i__) {

/*           Reduce i-th column and i-th row of matrix to bidiagonal form */

	    ml = klm + 1;
	    mu = kun + 1;
	    i__2 = kb;
	    for (kk = 1; kk <= i__2; ++kk) {
		j1 += kb;
		j2 += kb;

/*              generate plane rotations to annihilate nonzero elements */
/*              which have been created below the band */

		if (nr > 0) {
		    zlargv_(&nr, &ab[klu1 + (j1 - klm - 1) * ab_dim1], &inca, 
			    &work[j1], &kb1, &rwork[j1], &kb1);
		}

/*              apply plane rotations from the left */

		i__3 = kb;
		for (l = 1; l <= i__3; ++l) {
		    if (j2 - klm + l - 1 > *n) {
			nrt = nr - 1;
		    } else {
			nrt = nr;
		    }
		    if (nrt > 0) {
			zlartv_(&nrt, &ab[klu1 - l + (j1 - klm + l - 1) * 
				ab_dim1], &inca, &ab[klu1 - l + 1 + (j1 - klm 
				+ l - 1) * ab_dim1], &inca, &rwork[j1], &work[
				j1], &kb1);
		    }
/* L10: */
		}

		if (ml > ml0) {
		    if (ml <= *m - i__ + 1) {

/*                    generate plane rotation to annihilate a(i+ml-1,i) */
/*                    within the band, and apply rotation from the left */

			zlartg_(&ab[*ku + ml - 1 + i__ * ab_dim1], &ab[*ku + 
				ml + i__ * ab_dim1], &rwork[i__ + ml - 1], &
				work[i__ + ml - 1], &ra);
			i__3 = *ku + ml - 1 + i__ * ab_dim1;
			ab[i__3].r = ra.r, ab[i__3].i = ra.i;
			if (i__ < *n) {
/* Computing MIN */
			    i__4 = *ku + ml - 2, i__5 = *n - i__;
			    i__3 = f2cmin(i__4,i__5);
			    i__6 = *ldab - 1;
			    i__7 = *ldab - 1;
			    zrot_(&i__3, &ab[*ku + ml - 2 + (i__ + 1) * 
				    ab_dim1], &i__6, &ab[*ku + ml - 1 + (i__ 
				    + 1) * ab_dim1], &i__7, &rwork[i__ + ml - 
				    1], &work[i__ + ml - 1]);
			}
		    }
		    ++nr;
		    j1 -= kb1;
		}

		if (wantq) {

/*                 accumulate product of plane rotations in Q */

		    i__3 = j2;
		    i__4 = kb1;
		    for (j = j1; i__4 < 0 ? j >= i__3 : j <= i__3; j += i__4) 
			    {
			d_cnjg(&z__1, &work[j]);
			zrot_(m, &q[(j - 1) * q_dim1 + 1], &c__1, &q[j * 
				q_dim1 + 1], &c__1, &rwork[j], &z__1);
/* L20: */
		    }
		}

		if (wantc) {

/*                 apply plane rotations to C */

		    i__4 = j2;
		    i__3 = kb1;
		    for (j = j1; i__3 < 0 ? j >= i__4 : j <= i__4; j += i__3) 
			    {
			zrot_(ncc, &c__[j - 1 + c_dim1], ldc, &c__[j + c_dim1]
				, ldc, &rwork[j], &work[j]);
/* L30: */
		    }
		}

		if (j2 + kun > *n) {

/*                 adjust J2 to keep within the bounds of the matrix */

		    --nr;
		    j2 -= kb1;
		}

		i__3 = j2;
		i__4 = kb1;
		for (j = j1; i__4 < 0 ? j >= i__3 : j <= i__3; j += i__4) {

/*                 create nonzero element a(j-1,j+ku) above the band */
/*                 and store it in WORK(n+1:2*n) */

		    i__5 = j + kun;
		    i__6 = j;
		    i__7 = (j + kun) * ab_dim1 + 1;
		    z__1.r = work[i__6].r * ab[i__7].r - work[i__6].i * ab[
			    i__7].i, z__1.i = work[i__6].r * ab[i__7].i + 
			    work[i__6].i * ab[i__7].r;
		    work[i__5].r = z__1.r, work[i__5].i = z__1.i;
		    i__5 = (j + kun) * ab_dim1 + 1;
		    i__6 = j;
		    i__7 = (j + kun) * ab_dim1 + 1;
		    z__1.r = rwork[i__6] * ab[i__7].r, z__1.i = rwork[i__6] * 
			    ab[i__7].i;
		    ab[i__5].r = z__1.r, ab[i__5].i = z__1.i;
/* L40: */
		}

/*              generate plane rotations to annihilate nonzero elements */
/*              which have been generated above the band */

		if (nr > 0) {
		    zlargv_(&nr, &ab[(j1 + kun - 1) * ab_dim1 + 1], &inca, &
			    work[j1 + kun], &kb1, &rwork[j1 + kun], &kb1);
		}

/*              apply plane rotations from the right */

		i__4 = kb;
		for (l = 1; l <= i__4; ++l) {
		    if (j2 + l - 1 > *m) {
			nrt = nr - 1;
		    } else {
			nrt = nr;
		    }
		    if (nrt > 0) {
			zlartv_(&nrt, &ab[l + 1 + (j1 + kun - 1) * ab_dim1], &
				inca, &ab[l + (j1 + kun) * ab_dim1], &inca, &
				rwork[j1 + kun], &work[j1 + kun], &kb1);
		    }
/* L50: */
		}

		if (ml == ml0 && mu > mu0) {
		    if (mu <= *n - i__ + 1) {

/*                    generate plane rotation to annihilate a(i,i+mu-1) */
/*                    within the band, and apply rotation from the right */

			zlartg_(&ab[*ku - mu + 3 + (i__ + mu - 2) * ab_dim1], 
				&ab[*ku - mu + 2 + (i__ + mu - 1) * ab_dim1], 
				&rwork[i__ + mu - 1], &work[i__ + mu - 1], &
				ra);
			i__4 = *ku - mu + 3 + (i__ + mu - 2) * ab_dim1;
			ab[i__4].r = ra.r, ab[i__4].i = ra.i;
/* Computing MIN */
			i__3 = *kl + mu - 2, i__5 = *m - i__;
			i__4 = f2cmin(i__3,i__5);
			zrot_(&i__4, &ab[*ku - mu + 4 + (i__ + mu - 2) * 
				ab_dim1], &c__1, &ab[*ku - mu + 3 + (i__ + mu 
				- 1) * ab_dim1], &c__1, &rwork[i__ + mu - 1], 
				&work[i__ + mu - 1]);
		    }
		    ++nr;
		    j1 -= kb1;
		}

		if (wantpt) {

/*                 accumulate product of plane rotations in P**H */

		    i__4 = j2;
		    i__3 = kb1;
		    for (j = j1; i__3 < 0 ? j >= i__4 : j <= i__4; j += i__3) 
			    {
			d_cnjg(&z__1, &work[j + kun]);
			zrot_(n, &pt[j + kun - 1 + pt_dim1], ldpt, &pt[j + 
				kun + pt_dim1], ldpt, &rwork[j + kun], &z__1);
/* L60: */
		    }
		}

		if (j2 + kb > *m) {

/*                 adjust J2 to keep within the bounds of the matrix */

		    --nr;
		    j2 -= kb1;
		}

		i__3 = j2;
		i__4 = kb1;
		for (j = j1; i__4 < 0 ? j >= i__3 : j <= i__3; j += i__4) {

/*                 create nonzero element a(j+kl+ku,j+ku-1) below the */
/*                 band and store it in WORK(1:n) */

		    i__5 = j + kb;
		    i__6 = j + kun;
		    i__7 = klu1 + (j + kun) * ab_dim1;
		    z__1.r = work[i__6].r * ab[i__7].r - work[i__6].i * ab[
			    i__7].i, z__1.i = work[i__6].r * ab[i__7].i + 
			    work[i__6].i * ab[i__7].r;
		    work[i__5].r = z__1.r, work[i__5].i = z__1.i;
		    i__5 = klu1 + (j + kun) * ab_dim1;
		    i__6 = j + kun;
		    i__7 = klu1 + (j + kun) * ab_dim1;
		    z__1.r = rwork[i__6] * ab[i__7].r, z__1.i = rwork[i__6] * 
			    ab[i__7].i;
		    ab[i__5].r = z__1.r, ab[i__5].i = z__1.i;
/* L70: */
		}

		if (ml > ml0) {
		    --ml;
		} else {
		    --mu;
		}
/* L80: */
	    }
/* L90: */
	}
    }

    if (*ku == 0 && *kl > 0) {

/*        A has been reduced to complex lower bidiagonal form */

/*        Transform lower bidiagonal form to upper bidiagonal by applying */
/*        plane rotations from the left, overwriting superdiagonal */
/*        elements on subdiagonal elements */

/* Computing MIN */
	i__2 = *m - 1;
	i__1 = f2cmin(i__2,*n);
	for (i__ = 1; i__ <= i__1; ++i__) {
	    zlartg_(&ab[i__ * ab_dim1 + 1], &ab[i__ * ab_dim1 + 2], &rc, &rs, 
		    &ra);
	    i__2 = i__ * ab_dim1 + 1;
	    ab[i__2].r = ra.r, ab[i__2].i = ra.i;
	    if (i__ < *n) {
		i__2 = i__ * ab_dim1 + 2;
		i__4 = (i__ + 1) * ab_dim1 + 1;
		z__1.r = rs.r * ab[i__4].r - rs.i * ab[i__4].i, z__1.i = rs.r 
			* ab[i__4].i + rs.i * ab[i__4].r;
		ab[i__2].r = z__1.r, ab[i__2].i = z__1.i;
		i__2 = (i__ + 1) * ab_dim1 + 1;
		i__4 = (i__ + 1) * ab_dim1 + 1;
		z__1.r = rc * ab[i__4].r, z__1.i = rc * ab[i__4].i;
		ab[i__2].r = z__1.r, ab[i__2].i = z__1.i;
	    }
	    if (wantq) {
		d_cnjg(&z__1, &rs);
		zrot_(m, &q[i__ * q_dim1 + 1], &c__1, &q[(i__ + 1) * q_dim1 + 
			1], &c__1, &rc, &z__1);
	    }
	    if (wantc) {
		zrot_(ncc, &c__[i__ + c_dim1], ldc, &c__[i__ + 1 + c_dim1], 
			ldc, &rc, &rs);
	    }
/* L100: */
	}
    } else {

/*        A has been reduced to complex upper bidiagonal form or is */
/*        diagonal */

	if (*ku > 0 && *m < *n) {

/*           Annihilate a(m,m+1) by applying plane rotations from the */
/*           right */

	    i__1 = *ku + (*m + 1) * ab_dim1;
	    rb.r = ab[i__1].r, rb.i = ab[i__1].i;
	    for (i__ = *m; i__ >= 1; --i__) {
		zlartg_(&ab[*ku + 1 + i__ * ab_dim1], &rb, &rc, &rs, &ra);
		i__1 = *ku + 1 + i__ * ab_dim1;
		ab[i__1].r = ra.r, ab[i__1].i = ra.i;
		if (i__ > 1) {
		    d_cnjg(&z__3, &rs);
		    z__2.r = -z__3.r, z__2.i = -z__3.i;
		    i__1 = *ku + i__ * ab_dim1;
		    z__1.r = z__2.r * ab[i__1].r - z__2.i * ab[i__1].i, 
			    z__1.i = z__2.r * ab[i__1].i + z__2.i * ab[i__1]
			    .r;
		    rb.r = z__1.r, rb.i = z__1.i;
		    i__1 = *ku + i__ * ab_dim1;
		    i__2 = *ku + i__ * ab_dim1;
		    z__1.r = rc * ab[i__2].r, z__1.i = rc * ab[i__2].i;
		    ab[i__1].r = z__1.r, ab[i__1].i = z__1.i;
		}
		if (wantpt) {
		    d_cnjg(&z__1, &rs);
		    zrot_(n, &pt[i__ + pt_dim1], ldpt, &pt[*m + 1 + pt_dim1], 
			    ldpt, &rc, &z__1);
		}
/* L110: */
	    }
	}
    }

/*     Make diagonal and superdiagonal elements real, storing them in D */
/*     and E */

    i__1 = *ku + 1 + ab_dim1;
    t.r = ab[i__1].r, t.i = ab[i__1].i;
    i__1 = minmn;
    for (i__ = 1; i__ <= i__1; ++i__) {
	abst = z_abs(&t);
	d__[i__] = abst;
	if (abst != 0.) {
	    z__1.r = t.r / abst, z__1.i = t.i / abst;
	    t.r = z__1.r, t.i = z__1.i;
	} else {
	    t.r = 1., t.i = 0.;
	}
	if (wantq) {
	    zscal_(m, &t, &q[i__ * q_dim1 + 1], &c__1);
	}
	if (wantc) {
	    d_cnjg(&z__1, &t);
	    zscal_(ncc, &z__1, &c__[i__ + c_dim1], ldc);
	}
	if (i__ < minmn) {
	    if (*ku == 0 && *kl == 0) {
		e[i__] = 0.;
		i__2 = (i__ + 1) * ab_dim1 + 1;
		t.r = ab[i__2].r, t.i = ab[i__2].i;
	    } else {
		if (*ku == 0) {
		    i__2 = i__ * ab_dim1 + 2;
		    d_cnjg(&z__2, &t);
		    z__1.r = ab[i__2].r * z__2.r - ab[i__2].i * z__2.i, 
			    z__1.i = ab[i__2].r * z__2.i + ab[i__2].i * 
			    z__2.r;
		    t.r = z__1.r, t.i = z__1.i;
		} else {
		    i__2 = *ku + (i__ + 1) * ab_dim1;
		    d_cnjg(&z__2, &t);
		    z__1.r = ab[i__2].r * z__2.r - ab[i__2].i * z__2.i, 
			    z__1.i = ab[i__2].r * z__2.i + ab[i__2].i * 
			    z__2.r;
		    t.r = z__1.r, t.i = z__1.i;
		}
		abst = z_abs(&t);
		e[i__] = abst;
		if (abst != 0.) {
		    z__1.r = t.r / abst, z__1.i = t.i / abst;
		    t.r = z__1.r, t.i = z__1.i;
		} else {
		    t.r = 1., t.i = 0.;
		}
		if (wantpt) {
		    zscal_(n, &t, &pt[i__ + 1 + pt_dim1], ldpt);
		}
		i__2 = *ku + 1 + (i__ + 1) * ab_dim1;
		d_cnjg(&z__2, &t);
		z__1.r = ab[i__2].r * z__2.r - ab[i__2].i * z__2.i, z__1.i = 
			ab[i__2].r * z__2.i + ab[i__2].i * z__2.r;
		t.r = z__1.r, t.i = z__1.i;
	    }
	}
/* L120: */
    }
    return;

/*     End of ZGBBRD */

} /* zgbbrd_ */

