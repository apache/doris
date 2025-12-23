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

static real c_b9 = 0.f;
static real c_b10 = 1.f;
static integer c__1 = 1;

/* > \brief \b SSBTRD */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download SSBTRD + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/ssbtrd.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/ssbtrd.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/ssbtrd.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE SSBTRD( VECT, UPLO, N, KD, AB, LDAB, D, E, Q, LDQ, */
/*                          WORK, INFO ) */

/*       CHARACTER          UPLO, VECT */
/*       INTEGER            INFO, KD, LDAB, LDQ, N */
/*       REAL               AB( LDAB, * ), D( * ), E( * ), Q( LDQ, * ), */
/*      $                   WORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > SSBTRD reduces a real symmetric band matrix A to symmetric */
/* > tridiagonal form T by an orthogonal similarity transformation: */
/* > Q**T * A * Q = T. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] VECT */
/* > \verbatim */
/* >          VECT is CHARACTER*1 */
/* >          = 'N':  do not form Q; */
/* >          = 'V':  form Q; */
/* >          = 'U':  update a matrix X, by forming X*Q. */
/* > \endverbatim */
/* > */
/* > \param[in] UPLO */
/* > \verbatim */
/* >          UPLO is CHARACTER*1 */
/* >          = 'U':  Upper triangle of A is stored; */
/* >          = 'L':  Lower triangle of A is stored. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrix A.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] KD */
/* > \verbatim */
/* >          KD is INTEGER */
/* >          The number of superdiagonals of the matrix A if UPLO = 'U', */
/* >          or the number of subdiagonals if UPLO = 'L'.  KD >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] AB */
/* > \verbatim */
/* >          AB is REAL array, dimension (LDAB,N) */
/* >          On entry, the upper or lower triangle of the symmetric band */
/* >          matrix A, stored in the first KD+1 rows of the array.  The */
/* >          j-th column of A is stored in the j-th column of the array AB */
/* >          as follows: */
/* >          if UPLO = 'U', AB(kd+1+i-j,j) = A(i,j) for f2cmax(1,j-kd)<=i<=j; */
/* >          if UPLO = 'L', AB(1+i-j,j)    = A(i,j) for j<=i<=f2cmin(n,j+kd). */
/* >          On exit, the diagonal elements of AB are overwritten by the */
/* >          diagonal elements of the tridiagonal matrix T; if KD > 0, the */
/* >          elements on the first superdiagonal (if UPLO = 'U') or the */
/* >          first subdiagonal (if UPLO = 'L') are overwritten by the */
/* >          off-diagonal elements of T; the rest of AB is overwritten by */
/* >          values generated during the reduction. */
/* > \endverbatim */
/* > */
/* > \param[in] LDAB */
/* > \verbatim */
/* >          LDAB is INTEGER */
/* >          The leading dimension of the array AB.  LDAB >= KD+1. */
/* > \endverbatim */
/* > */
/* > \param[out] D */
/* > \verbatim */
/* >          D is REAL array, dimension (N) */
/* >          The diagonal elements of the tridiagonal matrix T. */
/* > \endverbatim */
/* > */
/* > \param[out] E */
/* > \verbatim */
/* >          E is REAL array, dimension (N-1) */
/* >          The off-diagonal elements of the tridiagonal matrix T: */
/* >          E(i) = T(i,i+1) if UPLO = 'U'; E(i) = T(i+1,i) if UPLO = 'L'. */
/* > \endverbatim */
/* > */
/* > \param[in,out] Q */
/* > \verbatim */
/* >          Q is REAL array, dimension (LDQ,N) */
/* >          On entry, if VECT = 'U', then Q must contain an N-by-N */
/* >          matrix X; if VECT = 'N' or 'V', then Q need not be set. */
/* > */
/* >          On exit: */
/* >          if VECT = 'V', Q contains the N-by-N orthogonal matrix Q; */
/* >          if VECT = 'U', Q contains the product X*Q; */
/* >          if VECT = 'N', the array Q is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDQ */
/* > \verbatim */
/* >          LDQ is INTEGER */
/* >          The leading dimension of the array Q. */
/* >          LDQ >= 1, and LDQ >= N if VECT = 'V' or 'U'. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is REAL array, dimension (N) */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup realOTHERcomputational */

/* > \par Further Details: */
/*  ===================== */
/* > */
/* > \verbatim */
/* > */
/* >  Modified by Linda Kaufman, Bell Labs. */
/* > \endverbatim */
/* > */
/*  ===================================================================== */
/* Subroutine */ void ssbtrd_(char *vect, char *uplo, integer *n, integer *kd, 
	real *ab, integer *ldab, real *d__, real *e, real *q, integer *ldq, 
	real *work, integer *info)
{
    /* System generated locals */
    integer ab_dim1, ab_offset, q_dim1, q_offset, i__1, i__2, i__3, i__4, 
	    i__5;

    /* Local variables */
    integer inca, jend, lend, jinc, incx, last;
    real temp;
    extern /* Subroutine */ void srot_(integer *, real *, integer *, real *, 
	    integer *, real *, real *);
    integer j1end, j1inc, i__, j, k, l, iqend;
    extern logical lsame_(char *, char *);
    logical initq, wantq, upper;
    integer i2, j1, j2;
    extern /* Subroutine */ void slar2v_(integer *, real *, real *, real *, 
	    integer *, real *, real *, integer *);
    integer nq, nr, iqaend;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    extern void slaset_(
	    char *, integer *, integer *, real *, real *, real *, integer *), slartg_(real *, real *, real *, real *, real *), slargv_(
	    integer *, real *, integer *, real *, integer *, real *, integer *
	    );
    integer kd1;
    extern /* Subroutine */ void slartv_(integer *, real *, integer *, real *, 
	    integer *, real *, real *, integer *);
    integer ibl, iqb, kdn, jin, nrt, kdm1;


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
    --work;

    /* Function Body */
    initq = lsame_(vect, "V");
    wantq = initq || lsame_(vect, "U");
    upper = lsame_(uplo, "U");
    kd1 = *kd + 1;
    kdm1 = *kd - 1;
    incx = *ldab - 1;
    iqend = 1;

    *info = 0;
    if (! wantq && ! lsame_(vect, "N")) {
	*info = -1;
    } else if (! upper && ! lsame_(uplo, "L")) {
	*info = -2;
    } else if (*n < 0) {
	*info = -3;
    } else if (*kd < 0) {
	*info = -4;
    } else if (*ldab < kd1) {
	*info = -6;
    } else if (*ldq < f2cmax(1,*n) && wantq) {
	*info = -10;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("SSBTRD", &i__1, (ftnlen)6);
	return;
    }

/*     Quick return if possible */

    if (*n == 0) {
	return;
    }

/*     Initialize Q to the unit matrix, if needed */

    if (initq) {
	slaset_("Full", n, n, &c_b9, &c_b10, &q[q_offset], ldq);
    }

/*     Wherever possible, plane rotations are generated and applied in */
/*     vector operations of length NR over the index set J1:J2:KD1. */

/*     The cosines and sines of the plane rotations are stored in the */
/*     arrays D and WORK. */

    inca = kd1 * *ldab;
/* Computing MIN */
    i__1 = *n - 1;
    kdn = f2cmin(i__1,*kd);
    if (upper) {

	if (*kd > 1) {

/*           Reduce to tridiagonal form, working with upper triangle */

	    nr = 0;
	    j1 = kdn + 2;
	    j2 = 1;

	    i__1 = *n - 2;
	    for (i__ = 1; i__ <= i__1; ++i__) {

/*              Reduce i-th row of matrix to tridiagonal form */

		for (k = kdn + 1; k >= 2; --k) {
		    j1 += kdn;
		    j2 += kdn;

		    if (nr > 0) {

/*                    generate plane rotations to annihilate nonzero */
/*                    elements which have been created outside the band */

			slargv_(&nr, &ab[(j1 - 1) * ab_dim1 + 1], &inca, &
				work[j1], &kd1, &d__[j1], &kd1);

/*                    apply rotations from the right */


/*                    Dependent on the the number of diagonals either */
/*                    SLARTV or SROT is used */

			if (nr >= (*kd << 1) - 1) {
			    i__2 = *kd - 1;
			    for (l = 1; l <= i__2; ++l) {
				slartv_(&nr, &ab[l + 1 + (j1 - 1) * ab_dim1], 
					&inca, &ab[l + j1 * ab_dim1], &inca, &
					d__[j1], &work[j1], &kd1);
/* L10: */
			    }

			} else {
			    jend = j1 + (nr - 1) * kd1;
			    i__2 = jend;
			    i__3 = kd1;
			    for (jinc = j1; i__3 < 0 ? jinc >= i__2 : jinc <= 
				    i__2; jinc += i__3) {
				srot_(&kdm1, &ab[(jinc - 1) * ab_dim1 + 2], &
					c__1, &ab[jinc * ab_dim1 + 1], &c__1, 
					&d__[jinc], &work[jinc]);
/* L20: */
			    }
			}
		    }


		    if (k > 2) {
			if (k <= *n - i__ + 1) {

/*                       generate plane rotation to annihilate a(i,i+k-1) */
/*                       within the band */

			    slartg_(&ab[*kd - k + 3 + (i__ + k - 2) * ab_dim1]
				    , &ab[*kd - k + 2 + (i__ + k - 1) * 
				    ab_dim1], &d__[i__ + k - 1], &work[i__ + 
				    k - 1], &temp);
			    ab[*kd - k + 3 + (i__ + k - 2) * ab_dim1] = temp;

/*                       apply rotation from the right */

			    i__3 = k - 3;
			    srot_(&i__3, &ab[*kd - k + 4 + (i__ + k - 2) * 
				    ab_dim1], &c__1, &ab[*kd - k + 3 + (i__ + 
				    k - 1) * ab_dim1], &c__1, &d__[i__ + k - 
				    1], &work[i__ + k - 1]);
			}
			++nr;
			j1 = j1 - kdn - 1;
		    }

/*                 apply plane rotations from both sides to diagonal */
/*                 blocks */

		    if (nr > 0) {
			slar2v_(&nr, &ab[kd1 + (j1 - 1) * ab_dim1], &ab[kd1 + 
				j1 * ab_dim1], &ab[*kd + j1 * ab_dim1], &inca,
				 &d__[j1], &work[j1], &kd1);
		    }

/*                 apply plane rotations from the left */

		    if (nr > 0) {
			if ((*kd << 1) - 1 < nr) {

/*                    Dependent on the the number of diagonals either */
/*                    SLARTV or SROT is used */

			    i__3 = *kd - 1;
			    for (l = 1; l <= i__3; ++l) {
				if (j2 + l > *n) {
				    nrt = nr - 1;
				} else {
				    nrt = nr;
				}
				if (nrt > 0) {
				    slartv_(&nrt, &ab[*kd - l + (j1 + l) * 
					    ab_dim1], &inca, &ab[*kd - l + 1 
					    + (j1 + l) * ab_dim1], &inca, &
					    d__[j1], &work[j1], &kd1);
				}
/* L30: */
			    }
			} else {
			    j1end = j1 + kd1 * (nr - 2);
			    if (j1end >= j1) {
				i__3 = j1end;
				i__2 = kd1;
				for (jin = j1; i__2 < 0 ? jin >= i__3 : jin <=
					 i__3; jin += i__2) {
				    i__4 = *kd - 1;
				    srot_(&i__4, &ab[*kd - 1 + (jin + 1) * 
					    ab_dim1], &incx, &ab[*kd + (jin + 
					    1) * ab_dim1], &incx, &d__[jin], &
					    work[jin]);
/* L40: */
				}
			    }
/* Computing MIN */
			    i__2 = kdm1, i__3 = *n - j2;
			    lend = f2cmin(i__2,i__3);
			    last = j1end + kd1;
			    if (lend > 0) {
				srot_(&lend, &ab[*kd - 1 + (last + 1) * 
					ab_dim1], &incx, &ab[*kd + (last + 1) 
					* ab_dim1], &incx, &d__[last], &work[
					last]);
			    }
			}
		    }

		    if (wantq) {

/*                    accumulate product of plane rotations in Q */

			if (initq) {

/*                 take advantage of the fact that Q was */
/*                 initially the Identity matrix */

			    iqend = f2cmax(iqend,j2);
/* Computing MAX */
			    i__2 = 0, i__3 = k - 3;
			    i2 = f2cmax(i__2,i__3);
			    iqaend = i__ * *kd + 1;
			    if (k == 2) {
				iqaend += *kd;
			    }
			    iqaend = f2cmin(iqaend,iqend);
			    i__2 = j2;
			    i__3 = kd1;
			    for (j = j1; i__3 < 0 ? j >= i__2 : j <= i__2; j 
				    += i__3) {
				ibl = i__ - i2 / kdm1;
				++i2;
/* Computing MAX */
				i__4 = 1, i__5 = j - ibl;
				iqb = f2cmax(i__4,i__5);
				nq = iqaend + 1 - iqb;
/* Computing MIN */
				i__4 = iqaend + *kd;
				iqaend = f2cmin(i__4,iqend);
				srot_(&nq, &q[iqb + (j - 1) * q_dim1], &c__1, 
					&q[iqb + j * q_dim1], &c__1, &d__[j], 
					&work[j]);
/* L50: */
			    }
			} else {

			    i__3 = j2;
			    i__2 = kd1;
			    for (j = j1; i__2 < 0 ? j >= i__3 : j <= i__3; j 
				    += i__2) {
				srot_(n, &q[(j - 1) * q_dim1 + 1], &c__1, &q[
					j * q_dim1 + 1], &c__1, &d__[j], &
					work[j]);
/* L60: */
			    }
			}

		    }

		    if (j2 + kdn > *n) {

/*                    adjust J2 to keep within the bounds of the matrix */

			--nr;
			j2 = j2 - kdn - 1;
		    }

		    i__2 = j2;
		    i__3 = kd1;
		    for (j = j1; i__3 < 0 ? j >= i__2 : j <= i__2; j += i__3) 
			    {

/*                    create nonzero element a(j-1,j+kd) outside the band */
/*                    and store it in WORK */

			work[j + *kd] = work[j] * ab[(j + *kd) * ab_dim1 + 1];
			ab[(j + *kd) * ab_dim1 + 1] = d__[j] * ab[(j + *kd) * 
				ab_dim1 + 1];
/* L70: */
		    }
/* L80: */
		}
/* L90: */
	    }
	}

	if (*kd > 0) {

/*           copy off-diagonal elements to E */

	    i__1 = *n - 1;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		e[i__] = ab[*kd + (i__ + 1) * ab_dim1];
/* L100: */
	    }
	} else {

/*           set E to zero if original matrix was diagonal */

	    i__1 = *n - 1;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		e[i__] = 0.f;
/* L110: */
	    }
	}

/*        copy diagonal elements to D */

	i__1 = *n;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    d__[i__] = ab[kd1 + i__ * ab_dim1];
/* L120: */
	}

    } else {

	if (*kd > 1) {

/*           Reduce to tridiagonal form, working with lower triangle */

	    nr = 0;
	    j1 = kdn + 2;
	    j2 = 1;

	    i__1 = *n - 2;
	    for (i__ = 1; i__ <= i__1; ++i__) {

/*              Reduce i-th column of matrix to tridiagonal form */

		for (k = kdn + 1; k >= 2; --k) {
		    j1 += kdn;
		    j2 += kdn;

		    if (nr > 0) {

/*                    generate plane rotations to annihilate nonzero */
/*                    elements which have been created outside the band */

			slargv_(&nr, &ab[kd1 + (j1 - kd1) * ab_dim1], &inca, &
				work[j1], &kd1, &d__[j1], &kd1);

/*                    apply plane rotations from one side */


/*                    Dependent on the the number of diagonals either */
/*                    SLARTV or SROT is used */

			if (nr > (*kd << 1) - 1) {
			    i__3 = *kd - 1;
			    for (l = 1; l <= i__3; ++l) {
				slartv_(&nr, &ab[kd1 - l + (j1 - kd1 + l) * 
					ab_dim1], &inca, &ab[kd1 - l + 1 + (
					j1 - kd1 + l) * ab_dim1], &inca, &d__[
					j1], &work[j1], &kd1);
/* L130: */
			    }
			} else {
			    jend = j1 + kd1 * (nr - 1);
			    i__3 = jend;
			    i__2 = kd1;
			    for (jinc = j1; i__2 < 0 ? jinc >= i__3 : jinc <= 
				    i__3; jinc += i__2) {
				srot_(&kdm1, &ab[*kd + (jinc - *kd) * ab_dim1]
					, &incx, &ab[kd1 + (jinc - *kd) * 
					ab_dim1], &incx, &d__[jinc], &work[
					jinc]);
/* L140: */
			    }
			}

		    }

		    if (k > 2) {
			if (k <= *n - i__ + 1) {

/*                       generate plane rotation to annihilate a(i+k-1,i) */
/*                       within the band */

			    slartg_(&ab[k - 1 + i__ * ab_dim1], &ab[k + i__ * 
				    ab_dim1], &d__[i__ + k - 1], &work[i__ + 
				    k - 1], &temp);
			    ab[k - 1 + i__ * ab_dim1] = temp;

/*                       apply rotation from the left */

			    i__2 = k - 3;
			    i__3 = *ldab - 1;
			    i__4 = *ldab - 1;
			    srot_(&i__2, &ab[k - 2 + (i__ + 1) * ab_dim1], &
				    i__3, &ab[k - 1 + (i__ + 1) * ab_dim1], &
				    i__4, &d__[i__ + k - 1], &work[i__ + k - 
				    1]);
			}
			++nr;
			j1 = j1 - kdn - 1;
		    }

/*                 apply plane rotations from both sides to diagonal */
/*                 blocks */

		    if (nr > 0) {
			slar2v_(&nr, &ab[(j1 - 1) * ab_dim1 + 1], &ab[j1 * 
				ab_dim1 + 1], &ab[(j1 - 1) * ab_dim1 + 2], &
				inca, &d__[j1], &work[j1], &kd1);
		    }

/*                 apply plane rotations from the right */


/*                    Dependent on the the number of diagonals either */
/*                    SLARTV or SROT is used */

		    if (nr > 0) {
			if (nr > (*kd << 1) - 1) {
			    i__2 = *kd - 1;
			    for (l = 1; l <= i__2; ++l) {
				if (j2 + l > *n) {
				    nrt = nr - 1;
				} else {
				    nrt = nr;
				}
				if (nrt > 0) {
				    slartv_(&nrt, &ab[l + 2 + (j1 - 1) * 
					    ab_dim1], &inca, &ab[l + 1 + j1 * 
					    ab_dim1], &inca, &d__[j1], &work[
					    j1], &kd1);
				}
/* L150: */
			    }
			} else {
			    j1end = j1 + kd1 * (nr - 2);
			    if (j1end >= j1) {
				i__2 = j1end;
				i__3 = kd1;
				for (j1inc = j1; i__3 < 0 ? j1inc >= i__2 : 
					j1inc <= i__2; j1inc += i__3) {
				    srot_(&kdm1, &ab[(j1inc - 1) * ab_dim1 + 
					    3], &c__1, &ab[j1inc * ab_dim1 + 
					    2], &c__1, &d__[j1inc], &work[
					    j1inc]);
/* L160: */
				}
			    }
/* Computing MIN */
			    i__3 = kdm1, i__2 = *n - j2;
			    lend = f2cmin(i__3,i__2);
			    last = j1end + kd1;
			    if (lend > 0) {
				srot_(&lend, &ab[(last - 1) * ab_dim1 + 3], &
					c__1, &ab[last * ab_dim1 + 2], &c__1, 
					&d__[last], &work[last]);
			    }
			}
		    }



		    if (wantq) {

/*                    accumulate product of plane rotations in Q */

			if (initq) {

/*                 take advantage of the fact that Q was */
/*                 initially the Identity matrix */

			    iqend = f2cmax(iqend,j2);
/* Computing MAX */
			    i__3 = 0, i__2 = k - 3;
			    i2 = f2cmax(i__3,i__2);
			    iqaend = i__ * *kd + 1;
			    if (k == 2) {
				iqaend += *kd;
			    }
			    iqaend = f2cmin(iqaend,iqend);
			    i__3 = j2;
			    i__2 = kd1;
			    for (j = j1; i__2 < 0 ? j >= i__3 : j <= i__3; j 
				    += i__2) {
				ibl = i__ - i2 / kdm1;
				++i2;
/* Computing MAX */
				i__4 = 1, i__5 = j - ibl;
				iqb = f2cmax(i__4,i__5);
				nq = iqaend + 1 - iqb;
/* Computing MIN */
				i__4 = iqaend + *kd;
				iqaend = f2cmin(i__4,iqend);
				srot_(&nq, &q[iqb + (j - 1) * q_dim1], &c__1, 
					&q[iqb + j * q_dim1], &c__1, &d__[j], 
					&work[j]);
/* L170: */
			    }
			} else {

			    i__2 = j2;
			    i__3 = kd1;
			    for (j = j1; i__3 < 0 ? j >= i__2 : j <= i__2; j 
				    += i__3) {
				srot_(n, &q[(j - 1) * q_dim1 + 1], &c__1, &q[
					j * q_dim1 + 1], &c__1, &d__[j], &
					work[j]);
/* L180: */
			    }
			}
		    }

		    if (j2 + kdn > *n) {

/*                    adjust J2 to keep within the bounds of the matrix */

			--nr;
			j2 = j2 - kdn - 1;
		    }

		    i__3 = j2;
		    i__2 = kd1;
		    for (j = j1; i__2 < 0 ? j >= i__3 : j <= i__3; j += i__2) 
			    {

/*                    create nonzero element a(j+kd,j-1) outside the */
/*                    band and store it in WORK */

			work[j + *kd] = work[j] * ab[kd1 + j * ab_dim1];
			ab[kd1 + j * ab_dim1] = d__[j] * ab[kd1 + j * ab_dim1]
				;
/* L190: */
		    }
/* L200: */
		}
/* L210: */
	    }
	}

	if (*kd > 0) {

/*           copy off-diagonal elements to E */

	    i__1 = *n - 1;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		e[i__] = ab[i__ * ab_dim1 + 2];
/* L220: */
	    }
	} else {

/*           set E to zero if original matrix was diagonal */

	    i__1 = *n - 1;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		e[i__] = 0.f;
/* L230: */
	    }
	}

/*        copy diagonal elements to D */

	i__1 = *n;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    d__[i__] = ab[i__ * ab_dim1 + 1];
/* L240: */
	}
    }

    return;

/*     End of SSBTRD */

} /* ssbtrd_ */

