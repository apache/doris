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

static integer c__2 = 2;
static integer c__1 = 1;

/* > \brief \b DLAGV2 computes the Generalized Schur factorization of a real 2-by-2 matrix pencil (A,B) where 
B is upper triangular. */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download DLAGV2 + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dlagv2.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dlagv2.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dlagv2.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE DLAGV2( A, LDA, B, LDB, ALPHAR, ALPHAI, BETA, CSL, SNL, */
/*                          CSR, SNR ) */

/*       INTEGER            LDA, LDB */
/*       DOUBLE PRECISION   CSL, CSR, SNL, SNR */
/*       DOUBLE PRECISION   A( LDA, * ), ALPHAI( 2 ), ALPHAR( 2 ), */
/*      $                   B( LDB, * ), BETA( 2 ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > DLAGV2 computes the Generalized Schur factorization of a real 2-by-2 */
/* > matrix pencil (A,B) where B is upper triangular. This routine */
/* > computes orthogonal (rotation) matrices given by CSL, SNL and CSR, */
/* > SNR such that */
/* > */
/* > 1) if the pencil (A,B) has two real eigenvalues (include 0/0 or 1/0 */
/* >    types), then */
/* > */
/* >    [ a11 a12 ] := [  CSL  SNL ] [ a11 a12 ] [  CSR -SNR ] */
/* >    [  0  a22 ]    [ -SNL  CSL ] [ a21 a22 ] [  SNR  CSR ] */
/* > */
/* >    [ b11 b12 ] := [  CSL  SNL ] [ b11 b12 ] [  CSR -SNR ] */
/* >    [  0  b22 ]    [ -SNL  CSL ] [  0  b22 ] [  SNR  CSR ], */
/* > */
/* > 2) if the pencil (A,B) has a pair of complex conjugate eigenvalues, */
/* >    then */
/* > */
/* >    [ a11 a12 ] := [  CSL  SNL ] [ a11 a12 ] [  CSR -SNR ] */
/* >    [ a21 a22 ]    [ -SNL  CSL ] [ a21 a22 ] [  SNR  CSR ] */
/* > */
/* >    [ b11  0  ] := [  CSL  SNL ] [ b11 b12 ] [  CSR -SNR ] */
/* >    [  0  b22 ]    [ -SNL  CSL ] [  0  b22 ] [  SNR  CSR ] */
/* > */
/* >    where b11 >= b22 > 0. */
/* > */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in,out] A */
/* > \verbatim */
/* >          A is DOUBLE PRECISION array, dimension (LDA, 2) */
/* >          On entry, the 2 x 2 matrix A. */
/* >          On exit, A is overwritten by the ``A-part'' of the */
/* >          generalized Schur form. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          THe leading dimension of the array A.  LDA >= 2. */
/* > \endverbatim */
/* > */
/* > \param[in,out] B */
/* > \verbatim */
/* >          B is DOUBLE PRECISION array, dimension (LDB, 2) */
/* >          On entry, the upper triangular 2 x 2 matrix B. */
/* >          On exit, B is overwritten by the ``B-part'' of the */
/* >          generalized Schur form. */
/* > \endverbatim */
/* > */
/* > \param[in] LDB */
/* > \verbatim */
/* >          LDB is INTEGER */
/* >          THe leading dimension of the array B.  LDB >= 2. */
/* > \endverbatim */
/* > */
/* > \param[out] ALPHAR */
/* > \verbatim */
/* >          ALPHAR is DOUBLE PRECISION array, dimension (2) */
/* > \endverbatim */
/* > */
/* > \param[out] ALPHAI */
/* > \verbatim */
/* >          ALPHAI is DOUBLE PRECISION array, dimension (2) */
/* > \endverbatim */
/* > */
/* > \param[out] BETA */
/* > \verbatim */
/* >          BETA is DOUBLE PRECISION array, dimension (2) */
/* >          (ALPHAR(k)+i*ALPHAI(k))/BETA(k) are the eigenvalues of the */
/* >          pencil (A,B), k=1,2, i = sqrt(-1).  Note that BETA(k) may */
/* >          be zero. */
/* > \endverbatim */
/* > */
/* > \param[out] CSL */
/* > \verbatim */
/* >          CSL is DOUBLE PRECISION */
/* >          The cosine of the left rotation matrix. */
/* > \endverbatim */
/* > */
/* > \param[out] SNL */
/* > \verbatim */
/* >          SNL is DOUBLE PRECISION */
/* >          The sine of the left rotation matrix. */
/* > \endverbatim */
/* > */
/* > \param[out] CSR */
/* > \verbatim */
/* >          CSR is DOUBLE PRECISION */
/* >          The cosine of the right rotation matrix. */
/* > \endverbatim */
/* > */
/* > \param[out] SNR */
/* > \verbatim */
/* >          SNR is DOUBLE PRECISION */
/* >          The sine of the right rotation matrix. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup doubleOTHERauxiliary */

/* > \par Contributors: */
/*  ================== */
/* > */
/* >     Mark Fahey, Department of Mathematics, Univ. of Kentucky, USA */

/*  ===================================================================== */
/* Subroutine */ void dlagv2_(doublereal *a, integer *lda, doublereal *b, 
	integer *ldb, doublereal *alphar, doublereal *alphai, doublereal *
	beta, doublereal *csl, doublereal *snl, doublereal *csr, doublereal *
	snr)
{
    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset;
    doublereal d__1, d__2, d__3, d__4, d__5, d__6;

    /* Local variables */
    extern /* Subroutine */ void drot_(integer *, doublereal *, integer *, 
	    doublereal *, integer *, doublereal *, doublereal *), dlag2_(
	    doublereal *, integer *, doublereal *, integer *, doublereal *, 
	    doublereal *, doublereal *, doublereal *, doublereal *, 
	    doublereal *);
    doublereal r__, t, anorm, bnorm, h1, h2, h3, scale1, scale2;
    extern /* Subroutine */ void dlasv2_(doublereal *, doublereal *, 
	    doublereal *, doublereal *, doublereal *, doublereal *, 
	    doublereal *, doublereal *, doublereal *);
    extern doublereal dlapy2_(doublereal *, doublereal *);
    doublereal ascale, bscale;
    extern doublereal dlamch_(char *);
    doublereal wi, qq, rr, safmin;
    extern /* Subroutine */ void dlartg_(doublereal *, doublereal *, 
	    doublereal *, doublereal *, doublereal *);
    doublereal wr1, wr2, ulp;


/*  -- LAPACK auxiliary routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    b_dim1 = *ldb;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    --alphar;
    --alphai;
    --beta;

    /* Function Body */
    safmin = dlamch_("S");
    ulp = dlamch_("P");

/*     Scale A */

/* Computing MAX */
    d__5 = (d__1 = a[a_dim1 + 1], abs(d__1)) + (d__2 = a[a_dim1 + 2], abs(
	    d__2)), d__6 = (d__3 = a[(a_dim1 << 1) + 1], abs(d__3)) + (d__4 = 
	    a[(a_dim1 << 1) + 2], abs(d__4)), d__5 = f2cmax(d__5,d__6);
    anorm = f2cmax(d__5,safmin);
    ascale = 1. / anorm;
    a[a_dim1 + 1] = ascale * a[a_dim1 + 1];
    a[(a_dim1 << 1) + 1] = ascale * a[(a_dim1 << 1) + 1];
    a[a_dim1 + 2] = ascale * a[a_dim1 + 2];
    a[(a_dim1 << 1) + 2] = ascale * a[(a_dim1 << 1) + 2];

/*     Scale B */

/* Computing MAX */
    d__4 = (d__3 = b[b_dim1 + 1], abs(d__3)), d__5 = (d__1 = b[(b_dim1 << 1) 
	    + 1], abs(d__1)) + (d__2 = b[(b_dim1 << 1) + 2], abs(d__2)), d__4 
	    = f2cmax(d__4,d__5);
    bnorm = f2cmax(d__4,safmin);
    bscale = 1. / bnorm;
    b[b_dim1 + 1] = bscale * b[b_dim1 + 1];
    b[(b_dim1 << 1) + 1] = bscale * b[(b_dim1 << 1) + 1];
    b[(b_dim1 << 1) + 2] = bscale * b[(b_dim1 << 1) + 2];

/*     Check if A can be deflated */

    if ((d__1 = a[a_dim1 + 2], abs(d__1)) <= ulp) {
	*csl = 1.;
	*snl = 0.;
	*csr = 1.;
	*snr = 0.;
	a[a_dim1 + 2] = 0.;
	b[b_dim1 + 2] = 0.;
	wi = 0.;

/*     Check if B is singular */

    } else if ((d__1 = b[b_dim1 + 1], abs(d__1)) <= ulp) {
	dlartg_(&a[a_dim1 + 1], &a[a_dim1 + 2], csl, snl, &r__);
	*csr = 1.;
	*snr = 0.;
	drot_(&c__2, &a[a_dim1 + 1], lda, &a[a_dim1 + 2], lda, csl, snl);
	drot_(&c__2, &b[b_dim1 + 1], ldb, &b[b_dim1 + 2], ldb, csl, snl);
	a[a_dim1 + 2] = 0.;
	b[b_dim1 + 1] = 0.;
	b[b_dim1 + 2] = 0.;
	wi = 0.;

    } else if ((d__1 = b[(b_dim1 << 1) + 2], abs(d__1)) <= ulp) {
	dlartg_(&a[(a_dim1 << 1) + 2], &a[a_dim1 + 2], csr, snr, &t);
	*snr = -(*snr);
	drot_(&c__2, &a[a_dim1 + 1], &c__1, &a[(a_dim1 << 1) + 1], &c__1, csr,
		 snr);
	drot_(&c__2, &b[b_dim1 + 1], &c__1, &b[(b_dim1 << 1) + 1], &c__1, csr,
		 snr);
	*csl = 1.;
	*snl = 0.;
	a[a_dim1 + 2] = 0.;
	b[b_dim1 + 2] = 0.;
	b[(b_dim1 << 1) + 2] = 0.;
	wi = 0.;

    } else {

/*        B is nonsingular, first compute the eigenvalues of (A,B) */

	dlag2_(&a[a_offset], lda, &b[b_offset], ldb, &safmin, &scale1, &
		scale2, &wr1, &wr2, &wi);

	if (wi == 0.) {

/*           two real eigenvalues, compute s*A-w*B */

	    h1 = scale1 * a[a_dim1 + 1] - wr1 * b[b_dim1 + 1];
	    h2 = scale1 * a[(a_dim1 << 1) + 1] - wr1 * b[(b_dim1 << 1) + 1];
	    h3 = scale1 * a[(a_dim1 << 1) + 2] - wr1 * b[(b_dim1 << 1) + 2];

	    rr = dlapy2_(&h1, &h2);
	    d__1 = scale1 * a[a_dim1 + 2];
	    qq = dlapy2_(&d__1, &h3);

	    if (rr > qq) {

/*              find right rotation matrix to zero 1,1 element of */
/*              (sA - wB) */

		dlartg_(&h2, &h1, csr, snr, &t);

	    } else {

/*              find right rotation matrix to zero 2,1 element of */
/*              (sA - wB) */

		d__1 = scale1 * a[a_dim1 + 2];
		dlartg_(&h3, &d__1, csr, snr, &t);

	    }

	    *snr = -(*snr);
	    drot_(&c__2, &a[a_dim1 + 1], &c__1, &a[(a_dim1 << 1) + 1], &c__1, 
		    csr, snr);
	    drot_(&c__2, &b[b_dim1 + 1], &c__1, &b[(b_dim1 << 1) + 1], &c__1, 
		    csr, snr);

/*           compute inf norms of A and B */

/* Computing MAX */
	    d__5 = (d__1 = a[a_dim1 + 1], abs(d__1)) + (d__2 = a[(a_dim1 << 1)
		     + 1], abs(d__2)), d__6 = (d__3 = a[a_dim1 + 2], abs(d__3)
		    ) + (d__4 = a[(a_dim1 << 1) + 2], abs(d__4));
	    h1 = f2cmax(d__5,d__6);
/* Computing MAX */
	    d__5 = (d__1 = b[b_dim1 + 1], abs(d__1)) + (d__2 = b[(b_dim1 << 1)
		     + 1], abs(d__2)), d__6 = (d__3 = b[b_dim1 + 2], abs(d__3)
		    ) + (d__4 = b[(b_dim1 << 1) + 2], abs(d__4));
	    h2 = f2cmax(d__5,d__6);

	    if (scale1 * h1 >= abs(wr1) * h2) {

/*              find left rotation matrix Q to zero out B(2,1) */

		dlartg_(&b[b_dim1 + 1], &b[b_dim1 + 2], csl, snl, &r__);

	    } else {

/*              find left rotation matrix Q to zero out A(2,1) */

		dlartg_(&a[a_dim1 + 1], &a[a_dim1 + 2], csl, snl, &r__);

	    }

	    drot_(&c__2, &a[a_dim1 + 1], lda, &a[a_dim1 + 2], lda, csl, snl);
	    drot_(&c__2, &b[b_dim1 + 1], ldb, &b[b_dim1 + 2], ldb, csl, snl);

	    a[a_dim1 + 2] = 0.;
	    b[b_dim1 + 2] = 0.;

	} else {

/*           a pair of complex conjugate eigenvalues */
/*           first compute the SVD of the matrix B */

	    dlasv2_(&b[b_dim1 + 1], &b[(b_dim1 << 1) + 1], &b[(b_dim1 << 1) + 
		    2], &r__, &t, snr, csr, snl, csl);

/*           Form (A,B) := Q(A,B)Z**T where Q is left rotation matrix and */
/*           Z is right rotation matrix computed from DLASV2 */

	    drot_(&c__2, &a[a_dim1 + 1], lda, &a[a_dim1 + 2], lda, csl, snl);
	    drot_(&c__2, &b[b_dim1 + 1], ldb, &b[b_dim1 + 2], ldb, csl, snl);
	    drot_(&c__2, &a[a_dim1 + 1], &c__1, &a[(a_dim1 << 1) + 1], &c__1, 
		    csr, snr);
	    drot_(&c__2, &b[b_dim1 + 1], &c__1, &b[(b_dim1 << 1) + 1], &c__1, 
		    csr, snr);

	    b[b_dim1 + 2] = 0.;
	    b[(b_dim1 << 1) + 1] = 0.;

	}

    }

/*     Unscaling */

    a[a_dim1 + 1] = anorm * a[a_dim1 + 1];
    a[a_dim1 + 2] = anorm * a[a_dim1 + 2];
    a[(a_dim1 << 1) + 1] = anorm * a[(a_dim1 << 1) + 1];
    a[(a_dim1 << 1) + 2] = anorm * a[(a_dim1 << 1) + 2];
    b[b_dim1 + 1] = bnorm * b[b_dim1 + 1];
    b[b_dim1 + 2] = bnorm * b[b_dim1 + 2];
    b[(b_dim1 << 1) + 1] = bnorm * b[(b_dim1 << 1) + 1];
    b[(b_dim1 << 1) + 2] = bnorm * b[(b_dim1 << 1) + 2];

    if (wi == 0.) {
	alphar[1] = a[a_dim1 + 1];
	alphar[2] = a[(a_dim1 << 1) + 2];
	alphai[1] = 0.;
	alphai[2] = 0.;
	beta[1] = b[b_dim1 + 1];
	beta[2] = b[(b_dim1 << 1) + 2];
    } else {
	alphar[1] = anorm * wr1 / scale1 / bnorm;
	alphai[1] = anorm * wi / scale1 / bnorm;
	alphar[2] = alphar[1];
	alphai[2] = -alphai[1];
	beta[1] = 1.;
	beta[2] = 1.;
    }

    return;

/*     End of DLAGV2 */

} /* dlagv2_ */

