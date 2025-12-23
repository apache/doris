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
static char junk[] = "\n@(#)LIBF77 VERSION 19990503\n";
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

static doublecomplex c_b14 = {1.,0.};
static doublecomplex c_b22 = {0.,0.};
static doublecomplex c_b29 = {-1.,0.};

/* > \brief \b ZTPRFB applies a real or complex "triangular-pentagonal" blocked reflector to a real or complex
 matrix, which is composed of two blocks. */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download ZTPRFB + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/ztprfb.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/ztprfb.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/ztprfb.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE ZTPRFB( SIDE, TRANS, DIRECT, STOREV, M, N, K, L, */
/*                          V, LDV, T, LDT, A, LDA, B, LDB, WORK, LDWORK ) */

/*       CHARACTER DIRECT, SIDE, STOREV, TRANS */
/*       INTEGER   K, L, LDA, LDB, LDT, LDV, LDWORK, M, N */
/*       COMPLEX*16   A( LDA, * ), B( LDB, * ), T( LDT, * ), */
/*      $          V( LDV, * ), WORK( LDWORK, * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > ZTPRFB applies a complex "triangular-pentagonal" block reflector H or its */
/* > conjugate transpose H**H to a complex matrix C, which is composed of two */
/* > blocks A and B, either from the left or right. */
/* > */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] SIDE */
/* > \verbatim */
/* >          SIDE is CHARACTER*1 */
/* >          = 'L': apply H or H**H from the Left */
/* >          = 'R': apply H or H**H from the Right */
/* > \endverbatim */
/* > */
/* > \param[in] TRANS */
/* > \verbatim */
/* >          TRANS is CHARACTER*1 */
/* >          = 'N': apply H (No transpose) */
/* >          = 'C': apply H**H (Conjugate transpose) */
/* > \endverbatim */
/* > */
/* > \param[in] DIRECT */
/* > \verbatim */
/* >          DIRECT is CHARACTER*1 */
/* >          Indicates how H is formed from a product of elementary */
/* >          reflectors */
/* >          = 'F': H = H(1) H(2) . . . H(k) (Forward) */
/* >          = 'B': H = H(k) . . . H(2) H(1) (Backward) */
/* > \endverbatim */
/* > */
/* > \param[in] STOREV */
/* > \verbatim */
/* >          STOREV is CHARACTER*1 */
/* >          Indicates how the vectors which define the elementary */
/* >          reflectors are stored: */
/* >          = 'C': Columns */
/* >          = 'R': Rows */
/* > \endverbatim */
/* > */
/* > \param[in] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >          The number of rows of the matrix B. */
/* >          M >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The number of columns of the matrix B. */
/* >          N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] K */
/* > \verbatim */
/* >          K is INTEGER */
/* >          The order of the matrix T, i.e. the number of elementary */
/* >          reflectors whose product defines the block reflector. */
/* >          K >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] L */
/* > \verbatim */
/* >          L is INTEGER */
/* >          The order of the trapezoidal part of V. */
/* >          K >= L >= 0.  See Further Details. */
/* > \endverbatim */
/* > */
/* > \param[in] V */
/* > \verbatim */
/* >          V is COMPLEX*16 array, dimension */
/* >                                (LDV,K) if STOREV = 'C' */
/* >                                (LDV,M) if STOREV = 'R' and SIDE = 'L' */
/* >                                (LDV,N) if STOREV = 'R' and SIDE = 'R' */
/* >          The pentagonal matrix V, which contains the elementary reflectors */
/* >          H(1), H(2), ..., H(K).  See Further Details. */
/* > \endverbatim */
/* > */
/* > \param[in] LDV */
/* > \verbatim */
/* >          LDV is INTEGER */
/* >          The leading dimension of the array V. */
/* >          If STOREV = 'C' and SIDE = 'L', LDV >= f2cmax(1,M); */
/* >          if STOREV = 'C' and SIDE = 'R', LDV >= f2cmax(1,N); */
/* >          if STOREV = 'R', LDV >= K. */
/* > \endverbatim */
/* > */
/* > \param[in] T */
/* > \verbatim */
/* >          T is COMPLEX*16 array, dimension (LDT,K) */
/* >          The triangular K-by-K matrix T in the representation of the */
/* >          block reflector. */
/* > \endverbatim */
/* > */
/* > \param[in] LDT */
/* > \verbatim */
/* >          LDT is INTEGER */
/* >          The leading dimension of the array T. */
/* >          LDT >= K. */
/* > \endverbatim */
/* > */
/* > \param[in,out] A */
/* > \verbatim */
/* >          A is COMPLEX*16 array, dimension */
/* >          (LDA,N) if SIDE = 'L' or (LDA,K) if SIDE = 'R' */
/* >          On entry, the K-by-N or M-by-K matrix A. */
/* >          On exit, A is overwritten by the corresponding block of */
/* >          H*C or H**H*C or C*H or C*H**H.  See Further Details. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of the array A. */
/* >          If SIDE = 'L', LDA >= f2cmax(1,K); */
/* >          If SIDE = 'R', LDA >= f2cmax(1,M). */
/* > \endverbatim */
/* > */
/* > \param[in,out] B */
/* > \verbatim */
/* >          B is COMPLEX*16 array, dimension (LDB,N) */
/* >          On entry, the M-by-N matrix B. */
/* >          On exit, B is overwritten by the corresponding block of */
/* >          H*C or H**H*C or C*H or C*H**H.  See Further Details. */
/* > \endverbatim */
/* > */
/* > \param[in] LDB */
/* > \verbatim */
/* >          LDB is INTEGER */
/* >          The leading dimension of the array B. */
/* >          LDB >= f2cmax(1,M). */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is COMPLEX*16 array, dimension */
/* >          (LDWORK,N) if SIDE = 'L', */
/* >          (LDWORK,K) if SIDE = 'R'. */
/* > \endverbatim */
/* > */
/* > \param[in] LDWORK */
/* > \verbatim */
/* >          LDWORK is INTEGER */
/* >          The leading dimension of the array WORK. */
/* >          If SIDE = 'L', LDWORK >= K; */
/* >          if SIDE = 'R', LDWORK >= M. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup complex16OTHERauxiliary */

/* > \par Further Details: */
/*  ===================== */
/* > */
/* > \verbatim */
/* > */
/* >  The matrix C is a composite matrix formed from blocks A and B. */
/* >  The block B is of size M-by-N; if SIDE = 'R', A is of size M-by-K, */
/* >  and if SIDE = 'L', A is of size K-by-N. */
/* > */
/* >  If SIDE = 'R' and DIRECT = 'F', C = [A B]. */
/* > */
/* >  If SIDE = 'L' and DIRECT = 'F', C = [A] */
/* >                                      [B]. */
/* > */
/* >  If SIDE = 'R' and DIRECT = 'B', C = [B A]. */
/* > */
/* >  If SIDE = 'L' and DIRECT = 'B', C = [B] */
/* >                                      [A]. */
/* > */
/* >  The pentagonal matrix V is composed of a rectangular block V1 and a */
/* >  trapezoidal block V2.  The size of the trapezoidal block is determined by */
/* >  the parameter L, where 0<=L<=K.  If L=K, the V2 block of V is triangular; */
/* >  if L=0, there is no trapezoidal block, thus V = V1 is rectangular. */
/* > */
/* >  If DIRECT = 'F' and STOREV = 'C':  V = [V1] */
/* >                                         [V2] */
/* >     - V2 is upper trapezoidal (first L rows of K-by-K upper triangular) */
/* > */
/* >  If DIRECT = 'F' and STOREV = 'R':  V = [V1 V2] */
/* > */
/* >     - V2 is lower trapezoidal (first L columns of K-by-K lower triangular) */
/* > */
/* >  If DIRECT = 'B' and STOREV = 'C':  V = [V2] */
/* >                                         [V1] */
/* >     - V2 is lower trapezoidal (last L rows of K-by-K lower triangular) */
/* > */
/* >  If DIRECT = 'B' and STOREV = 'R':  V = [V2 V1] */
/* > */
/* >     - V2 is upper trapezoidal (last L columns of K-by-K upper triangular) */
/* > */
/* >  If STOREV = 'C' and SIDE = 'L', V is M-by-K with V2 L-by-K. */
/* > */
/* >  If STOREV = 'C' and SIDE = 'R', V is N-by-K with V2 L-by-K. */
/* > */
/* >  If STOREV = 'R' and SIDE = 'L', V is K-by-M with V2 K-by-L. */
/* > */
/* >  If STOREV = 'R' and SIDE = 'R', V is K-by-N with V2 K-by-L. */
/* > \endverbatim */
/* > */
/*  ===================================================================== */
/* Subroutine */ void ztprfb_(char *side, char *trans, char *direct, char *
	storev, integer *m, integer *n, integer *k, integer *l, doublecomplex 
	*v, integer *ldv, doublecomplex *t, integer *ldt, doublecomplex *a, 
	integer *lda, doublecomplex *b, integer *ldb, doublecomplex *work, 
	integer *ldwork)
{
    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, t_dim1, t_offset, v_dim1, 
	    v_offset, work_dim1, work_offset, i__1, i__2, i__3, i__4, i__5;
    doublecomplex z__1;

    /* Local variables */
    logical left, backward;
    integer i__, j;
    extern logical lsame_(char *, char *);
    logical right;
    extern /* Subroutine */ void zgemm_(char *, char *, integer *, integer *, 
	    integer *, doublecomplex *, doublecomplex *, integer *, 
	    doublecomplex *, integer *, doublecomplex *, doublecomplex *, 
	    integer *), ztrmm_(char *, char *, char *, char *,
	     integer *, integer *, doublecomplex *, doublecomplex *, integer *
	    , doublecomplex *, integer *);
    integer kp, mp, np;
    logical column, row, forward;


/*  -- LAPACK auxiliary routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ========================================================================== */


/*     Quick return if possible */

    /* Parameter adjustments */
    v_dim1 = *ldv;
    v_offset = 1 + v_dim1 * 1;
    v -= v_offset;
    t_dim1 = *ldt;
    t_offset = 1 + t_dim1 * 1;
    t -= t_offset;
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    b_dim1 = *ldb;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    work_dim1 = *ldwork;
    work_offset = 1 + work_dim1 * 1;
    work -= work_offset;

    /* Function Body */
    if (*m <= 0 || *n <= 0 || *k <= 0 || *l < 0) {
	return;
    }

    if (lsame_(storev, "C")) {
	column = TRUE_;
	row = FALSE_;
    } else if (lsame_(storev, "R")) {
	column = FALSE_;
	row = TRUE_;
    } else {
	column = FALSE_;
	row = FALSE_;
    }

    if (lsame_(side, "L")) {
	left = TRUE_;
	right = FALSE_;
    } else if (lsame_(side, "R")) {
	left = FALSE_;
	right = TRUE_;
    } else {
	left = FALSE_;
	right = FALSE_;
    }

    if (lsame_(direct, "F")) {
	forward = TRUE_;
	backward = FALSE_;
    } else if (lsame_(direct, "B")) {
	forward = FALSE_;
	backward = TRUE_;
    } else {
	forward = FALSE_;
	backward = FALSE_;
    }

/* --------------------------------------------------------------------------- */

    if (column && forward && left) {

/* --------------------------------------------------------------------------- */

/*        Let  W =  [ I ]    (K-by-K) */
/*                  [ V ]    (M-by-K) */

/*        Form  H C  or  H**H C  where  C = [ A ]  (K-by-N) */
/*                                          [ B ]  (M-by-N) */

/*        H = I - W T W**H          or  H**H = I - W T**H W**H */

/*        A = A -   T (A + V**H B)  or  A = A -   T**H (A + V**H B) */
/*        B = B - V T (A + V**H B)  or  B = B - V T**H (A + V**H B) */

/* --------------------------------------------------------------------------- */

/* Computing MIN */
	i__1 = *m - *l + 1;
	mp = f2cmin(i__1,*m);
/* Computing MIN */
	i__1 = *l + 1;
	kp = f2cmin(i__1,*k);

	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *l;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + j * work_dim1;
		i__4 = *m - *l + i__ + j * b_dim1;
		work[i__3].r = b[i__4].r, work[i__3].i = b[i__4].i;
	    }
	}
	ztrmm_("L", "U", "C", "N", l, n, &c_b14, &v[mp + v_dim1], ldv, &work[
		work_offset], ldwork);
	i__1 = *m - *l;
	zgemm_("C", "N", l, n, &i__1, &c_b14, &v[v_offset], ldv, &b[b_offset],
		 ldb, &c_b14, &work[work_offset], ldwork);
	i__1 = *k - *l;
	zgemm_("C", "N", &i__1, n, m, &c_b14, &v[kp * v_dim1 + 1], ldv, &b[
		b_offset], ldb, &c_b22, &work[kp + work_dim1], ldwork);

	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *k;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + j * work_dim1;
		i__4 = i__ + j * work_dim1;
		i__5 = i__ + j * a_dim1;
		z__1.r = work[i__4].r + a[i__5].r, z__1.i = work[i__4].i + a[
			i__5].i;
		work[i__3].r = z__1.r, work[i__3].i = z__1.i;
	    }
	}

	ztrmm_("L", "U", trans, "N", k, n, &c_b14, &t[t_offset], ldt, &work[
		work_offset], ldwork);

	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *k;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + j * a_dim1;
		i__4 = i__ + j * a_dim1;
		i__5 = i__ + j * work_dim1;
		z__1.r = a[i__4].r - work[i__5].r, z__1.i = a[i__4].i - work[
			i__5].i;
		a[i__3].r = z__1.r, a[i__3].i = z__1.i;
	    }
	}

	i__1 = *m - *l;
	zgemm_("N", "N", &i__1, n, k, &c_b29, &v[v_offset], ldv, &work[
		work_offset], ldwork, &c_b14, &b[b_offset], ldb);
	i__1 = *k - *l;
	zgemm_("N", "N", l, n, &i__1, &c_b29, &v[mp + kp * v_dim1], ldv, &
		work[kp + work_dim1], ldwork, &c_b14, &b[mp + b_dim1], ldb);
	ztrmm_("L", "U", "N", "N", l, n, &c_b14, &v[mp + v_dim1], ldv, &work[
		work_offset], ldwork);
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *l;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = *m - *l + i__ + j * b_dim1;
		i__4 = *m - *l + i__ + j * b_dim1;
		i__5 = i__ + j * work_dim1;
		z__1.r = b[i__4].r - work[i__5].r, z__1.i = b[i__4].i - work[
			i__5].i;
		b[i__3].r = z__1.r, b[i__3].i = z__1.i;
	    }
	}

/* --------------------------------------------------------------------------- */

    } else if (column && forward && right) {

/* --------------------------------------------------------------------------- */

/*        Let  W =  [ I ]    (K-by-K) */
/*                  [ V ]    (N-by-K) */

/*        Form  C H or  C H**H  where  C = [ A B ] (A is M-by-K, B is M-by-N) */

/*        H = I - W T W**H          or  H**H = I - W T**H W**H */

/*        A = A - (A + B V) T      or  A = A - (A + B V) T**H */
/*        B = B - (A + B V) T V**H  or  B = B - (A + B V) T**H V**H */

/* --------------------------------------------------------------------------- */

/* Computing MIN */
	i__1 = *n - *l + 1;
	np = f2cmin(i__1,*n);
/* Computing MIN */
	i__1 = *l + 1;
	kp = f2cmin(i__1,*k);

	i__1 = *l;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *m;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + j * work_dim1;
		i__4 = i__ + (*n - *l + j) * b_dim1;
		work[i__3].r = b[i__4].r, work[i__3].i = b[i__4].i;
	    }
	}
	ztrmm_("R", "U", "N", "N", m, l, &c_b14, &v[np + v_dim1], ldv, &work[
		work_offset], ldwork);
	i__1 = *n - *l;
	zgemm_("N", "N", m, l, &i__1, &c_b14, &b[b_offset], ldb, &v[v_offset],
		 ldv, &c_b14, &work[work_offset], ldwork);
	i__1 = *k - *l;
	zgemm_("N", "N", m, &i__1, n, &c_b14, &b[b_offset], ldb, &v[kp * 
		v_dim1 + 1], ldv, &c_b22, &work[kp * work_dim1 + 1], ldwork);

	i__1 = *k;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *m;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + j * work_dim1;
		i__4 = i__ + j * work_dim1;
		i__5 = i__ + j * a_dim1;
		z__1.r = work[i__4].r + a[i__5].r, z__1.i = work[i__4].i + a[
			i__5].i;
		work[i__3].r = z__1.r, work[i__3].i = z__1.i;
	    }
	}

	ztrmm_("R", "U", trans, "N", m, k, &c_b14, &t[t_offset], ldt, &work[
		work_offset], ldwork);

	i__1 = *k;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *m;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + j * a_dim1;
		i__4 = i__ + j * a_dim1;
		i__5 = i__ + j * work_dim1;
		z__1.r = a[i__4].r - work[i__5].r, z__1.i = a[i__4].i - work[
			i__5].i;
		a[i__3].r = z__1.r, a[i__3].i = z__1.i;
	    }
	}

	i__1 = *n - *l;
	zgemm_("N", "C", m, &i__1, k, &c_b29, &work[work_offset], ldwork, &v[
		v_offset], ldv, &c_b14, &b[b_offset], ldb);
	i__1 = *k - *l;
	zgemm_("N", "C", m, l, &i__1, &c_b29, &work[kp * work_dim1 + 1], 
		ldwork, &v[np + kp * v_dim1], ldv, &c_b14, &b[np * b_dim1 + 1]
		, ldb);
	ztrmm_("R", "U", "C", "N", m, l, &c_b14, &v[np + v_dim1], ldv, &work[
		work_offset], ldwork);
	i__1 = *l;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *m;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + (*n - *l + j) * b_dim1;
		i__4 = i__ + (*n - *l + j) * b_dim1;
		i__5 = i__ + j * work_dim1;
		z__1.r = b[i__4].r - work[i__5].r, z__1.i = b[i__4].i - work[
			i__5].i;
		b[i__3].r = z__1.r, b[i__3].i = z__1.i;
	    }
	}

/* --------------------------------------------------------------------------- */

    } else if (column && backward && left) {

/* --------------------------------------------------------------------------- */

/*        Let  W =  [ V ]    (M-by-K) */
/*                  [ I ]    (K-by-K) */

/*        Form  H C  or  H**H C  where  C = [ B ]  (M-by-N) */
/*                                          [ A ]  (K-by-N) */

/*        H = I - W T W**H          or  H**H = I - W T**H W**H */

/*        A = A -   T (A + V**H B)  or  A = A -   T**H (A + V**H B) */
/*        B = B - V T (A + V**H B)  or  B = B - V T**H (A + V**H B) */

/* --------------------------------------------------------------------------- */

/* Computing MIN */
	i__1 = *l + 1;
	mp = f2cmin(i__1,*m);
/* Computing MIN */
	i__1 = *k - *l + 1;
	kp = f2cmin(i__1,*k);

	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *l;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = *k - *l + i__ + j * work_dim1;
		i__4 = i__ + j * b_dim1;
		work[i__3].r = b[i__4].r, work[i__3].i = b[i__4].i;
	    }
	}

	ztrmm_("L", "L", "C", "N", l, n, &c_b14, &v[kp * v_dim1 + 1], ldv, &
		work[kp + work_dim1], ldwork);
	i__1 = *m - *l;
	zgemm_("C", "N", l, n, &i__1, &c_b14, &v[mp + kp * v_dim1], ldv, &b[
		mp + b_dim1], ldb, &c_b14, &work[kp + work_dim1], ldwork);
	i__1 = *k - *l;
	zgemm_("C", "N", &i__1, n, m, &c_b14, &v[v_offset], ldv, &b[b_offset],
		 ldb, &c_b22, &work[work_offset], ldwork);

	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *k;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + j * work_dim1;
		i__4 = i__ + j * work_dim1;
		i__5 = i__ + j * a_dim1;
		z__1.r = work[i__4].r + a[i__5].r, z__1.i = work[i__4].i + a[
			i__5].i;
		work[i__3].r = z__1.r, work[i__3].i = z__1.i;
	    }
	}

	ztrmm_("L", "L", trans, "N", k, n, &c_b14, &t[t_offset], ldt, &work[
		work_offset], ldwork);

	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *k;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + j * a_dim1;
		i__4 = i__ + j * a_dim1;
		i__5 = i__ + j * work_dim1;
		z__1.r = a[i__4].r - work[i__5].r, z__1.i = a[i__4].i - work[
			i__5].i;
		a[i__3].r = z__1.r, a[i__3].i = z__1.i;
	    }
	}

	i__1 = *m - *l;
	zgemm_("N", "N", &i__1, n, k, &c_b29, &v[mp + v_dim1], ldv, &work[
		work_offset], ldwork, &c_b14, &b[mp + b_dim1], ldb);
	i__1 = *k - *l;
	zgemm_("N", "N", l, n, &i__1, &c_b29, &v[v_offset], ldv, &work[
		work_offset], ldwork, &c_b14, &b[b_offset], ldb);
	ztrmm_("L", "L", "N", "N", l, n, &c_b14, &v[kp * v_dim1 + 1], ldv, &
		work[kp + work_dim1], ldwork);
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *l;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + j * b_dim1;
		i__4 = i__ + j * b_dim1;
		i__5 = *k - *l + i__ + j * work_dim1;
		z__1.r = b[i__4].r - work[i__5].r, z__1.i = b[i__4].i - work[
			i__5].i;
		b[i__3].r = z__1.r, b[i__3].i = z__1.i;
	    }
	}

/* --------------------------------------------------------------------------- */

    } else if (column && backward && right) {

/* --------------------------------------------------------------------------- */

/*        Let  W =  [ V ]    (N-by-K) */
/*                  [ I ]    (K-by-K) */

/*        Form  C H  or  C H**H  where  C = [ B A ] (B is M-by-N, A is M-by-K) */

/*        H = I - W T W**H          or  H**H = I - W T**H W**H */

/*        A = A - (A + B V) T      or  A = A - (A + B V) T**H */
/*        B = B - (A + B V) T V**H  or  B = B - (A + B V) T**H V**H */

/* --------------------------------------------------------------------------- */

/* Computing MIN */
	i__1 = *l + 1;
	np = f2cmin(i__1,*n);
/* Computing MIN */
	i__1 = *k - *l + 1;
	kp = f2cmin(i__1,*k);

	i__1 = *l;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *m;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + (*k - *l + j) * work_dim1;
		i__4 = i__ + j * b_dim1;
		work[i__3].r = b[i__4].r, work[i__3].i = b[i__4].i;
	    }
	}
	ztrmm_("R", "L", "N", "N", m, l, &c_b14, &v[kp * v_dim1 + 1], ldv, &
		work[kp * work_dim1 + 1], ldwork);
	i__1 = *n - *l;
	zgemm_("N", "N", m, l, &i__1, &c_b14, &b[np * b_dim1 + 1], ldb, &v[np 
		+ kp * v_dim1], ldv, &c_b14, &work[kp * work_dim1 + 1], 
		ldwork);
	i__1 = *k - *l;
	zgemm_("N", "N", m, &i__1, n, &c_b14, &b[b_offset], ldb, &v[v_offset],
		 ldv, &c_b22, &work[work_offset], ldwork);

	i__1 = *k;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *m;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + j * work_dim1;
		i__4 = i__ + j * work_dim1;
		i__5 = i__ + j * a_dim1;
		z__1.r = work[i__4].r + a[i__5].r, z__1.i = work[i__4].i + a[
			i__5].i;
		work[i__3].r = z__1.r, work[i__3].i = z__1.i;
	    }
	}

	ztrmm_("R", "L", trans, "N", m, k, &c_b14, &t[t_offset], ldt, &work[
		work_offset], ldwork);

	i__1 = *k;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *m;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + j * a_dim1;
		i__4 = i__ + j * a_dim1;
		i__5 = i__ + j * work_dim1;
		z__1.r = a[i__4].r - work[i__5].r, z__1.i = a[i__4].i - work[
			i__5].i;
		a[i__3].r = z__1.r, a[i__3].i = z__1.i;
	    }
	}

	i__1 = *n - *l;
	zgemm_("N", "C", m, &i__1, k, &c_b29, &work[work_offset], ldwork, &v[
		np + v_dim1], ldv, &c_b14, &b[np * b_dim1 + 1], ldb);
	i__1 = *k - *l;
	zgemm_("N", "C", m, l, &i__1, &c_b29, &work[work_offset], ldwork, &v[
		v_offset], ldv, &c_b14, &b[b_offset], ldb);
	ztrmm_("R", "L", "C", "N", m, l, &c_b14, &v[kp * v_dim1 + 1], ldv, &
		work[kp * work_dim1 + 1], ldwork);
	i__1 = *l;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *m;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + j * b_dim1;
		i__4 = i__ + j * b_dim1;
		i__5 = i__ + (*k - *l + j) * work_dim1;
		z__1.r = b[i__4].r - work[i__5].r, z__1.i = b[i__4].i - work[
			i__5].i;
		b[i__3].r = z__1.r, b[i__3].i = z__1.i;
	    }
	}

/* --------------------------------------------------------------------------- */

    } else if (row && forward && left) {

/* --------------------------------------------------------------------------- */

/*        Let  W =  [ I V ] ( I is K-by-K, V is K-by-M ) */

/*        Form  H C  or  H**H C  where  C = [ A ]  (K-by-N) */
/*                                          [ B ]  (M-by-N) */

/*        H = I - W**H T W          or  H**H = I - W**H T**H W */

/*        A = A -     T (A + V B)  or  A = A -     T**H (A + V B) */
/*        B = B - V**H T (A + V B)  or  B = B - V**H T**H (A + V B) */

/* --------------------------------------------------------------------------- */

/* Computing MIN */
	i__1 = *m - *l + 1;
	mp = f2cmin(i__1,*m);
/* Computing MIN */
	i__1 = *l + 1;
	kp = f2cmin(i__1,*k);

	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *l;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + j * work_dim1;
		i__4 = *m - *l + i__ + j * b_dim1;
		work[i__3].r = b[i__4].r, work[i__3].i = b[i__4].i;
	    }
	}
	ztrmm_("L", "L", "N", "N", l, n, &c_b14, &v[mp * v_dim1 + 1], ldv, &
		work[work_offset], ldb);
	i__1 = *m - *l;
	zgemm_("N", "N", l, n, &i__1, &c_b14, &v[v_offset], ldv, &b[b_offset],
		 ldb, &c_b14, &work[work_offset], ldwork);
	i__1 = *k - *l;
	zgemm_("N", "N", &i__1, n, m, &c_b14, &v[kp + v_dim1], ldv, &b[
		b_offset], ldb, &c_b22, &work[kp + work_dim1], ldwork);

	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *k;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + j * work_dim1;
		i__4 = i__ + j * work_dim1;
		i__5 = i__ + j * a_dim1;
		z__1.r = work[i__4].r + a[i__5].r, z__1.i = work[i__4].i + a[
			i__5].i;
		work[i__3].r = z__1.r, work[i__3].i = z__1.i;
	    }
	}

	ztrmm_("L", "U", trans, "N", k, n, &c_b14, &t[t_offset], ldt, &work[
		work_offset], ldwork);

	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *k;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + j * a_dim1;
		i__4 = i__ + j * a_dim1;
		i__5 = i__ + j * work_dim1;
		z__1.r = a[i__4].r - work[i__5].r, z__1.i = a[i__4].i - work[
			i__5].i;
		a[i__3].r = z__1.r, a[i__3].i = z__1.i;
	    }
	}

	i__1 = *m - *l;
	zgemm_("C", "N", &i__1, n, k, &c_b29, &v[v_offset], ldv, &work[
		work_offset], ldwork, &c_b14, &b[b_offset], ldb);
	i__1 = *k - *l;
	zgemm_("C", "N", l, n, &i__1, &c_b29, &v[kp + mp * v_dim1], ldv, &
		work[kp + work_dim1], ldwork, &c_b14, &b[mp + b_dim1], ldb);
	ztrmm_("L", "L", "C", "N", l, n, &c_b14, &v[mp * v_dim1 + 1], ldv, &
		work[work_offset], ldwork);
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *l;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = *m - *l + i__ + j * b_dim1;
		i__4 = *m - *l + i__ + j * b_dim1;
		i__5 = i__ + j * work_dim1;
		z__1.r = b[i__4].r - work[i__5].r, z__1.i = b[i__4].i - work[
			i__5].i;
		b[i__3].r = z__1.r, b[i__3].i = z__1.i;
	    }
	}

/* --------------------------------------------------------------------------- */

    } else if (row && forward && right) {

/* --------------------------------------------------------------------------- */

/*        Let  W =  [ I V ] ( I is K-by-K, V is K-by-N ) */

/*        Form  C H  or  C H**H  where  C = [ A B ] (A is M-by-K, B is M-by-N) */

/*        H = I - W**H T W            or  H**H = I - W**H T**H W */

/*        A = A - (A + B V**H) T      or  A = A - (A + B V**H) T**H */
/*        B = B - (A + B V**H) T V    or  B = B - (A + B V**H) T**H V */

/* --------------------------------------------------------------------------- */

/* Computing MIN */
	i__1 = *n - *l + 1;
	np = f2cmin(i__1,*n);
/* Computing MIN */
	i__1 = *l + 1;
	kp = f2cmin(i__1,*k);

	i__1 = *l;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *m;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + j * work_dim1;
		i__4 = i__ + (*n - *l + j) * b_dim1;
		work[i__3].r = b[i__4].r, work[i__3].i = b[i__4].i;
	    }
	}
	ztrmm_("R", "L", "C", "N", m, l, &c_b14, &v[np * v_dim1 + 1], ldv, &
		work[work_offset], ldwork);
	i__1 = *n - *l;
	zgemm_("N", "C", m, l, &i__1, &c_b14, &b[b_offset], ldb, &v[v_offset],
		 ldv, &c_b14, &work[work_offset], ldwork);
	i__1 = *k - *l;
	zgemm_("N", "C", m, &i__1, n, &c_b14, &b[b_offset], ldb, &v[kp + 
		v_dim1], ldv, &c_b22, &work[kp * work_dim1 + 1], ldwork);

	i__1 = *k;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *m;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + j * work_dim1;
		i__4 = i__ + j * work_dim1;
		i__5 = i__ + j * a_dim1;
		z__1.r = work[i__4].r + a[i__5].r, z__1.i = work[i__4].i + a[
			i__5].i;
		work[i__3].r = z__1.r, work[i__3].i = z__1.i;
	    }
	}

	ztrmm_("R", "U", trans, "N", m, k, &c_b14, &t[t_offset], ldt, &work[
		work_offset], ldwork);

	i__1 = *k;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *m;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + j * a_dim1;
		i__4 = i__ + j * a_dim1;
		i__5 = i__ + j * work_dim1;
		z__1.r = a[i__4].r - work[i__5].r, z__1.i = a[i__4].i - work[
			i__5].i;
		a[i__3].r = z__1.r, a[i__3].i = z__1.i;
	    }
	}

	i__1 = *n - *l;
	zgemm_("N", "N", m, &i__1, k, &c_b29, &work[work_offset], ldwork, &v[
		v_offset], ldv, &c_b14, &b[b_offset], ldb);
	i__1 = *k - *l;
	zgemm_("N", "N", m, l, &i__1, &c_b29, &work[kp * work_dim1 + 1], 
		ldwork, &v[kp + np * v_dim1], ldv, &c_b14, &b[np * b_dim1 + 1]
		, ldb);
	ztrmm_("R", "L", "N", "N", m, l, &c_b14, &v[np * v_dim1 + 1], ldv, &
		work[work_offset], ldwork);
	i__1 = *l;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *m;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + (*n - *l + j) * b_dim1;
		i__4 = i__ + (*n - *l + j) * b_dim1;
		i__5 = i__ + j * work_dim1;
		z__1.r = b[i__4].r - work[i__5].r, z__1.i = b[i__4].i - work[
			i__5].i;
		b[i__3].r = z__1.r, b[i__3].i = z__1.i;
	    }
	}

/* --------------------------------------------------------------------------- */

    } else if (row && backward && left) {

/* --------------------------------------------------------------------------- */

/*        Let  W =  [ V I ] ( I is K-by-K, V is K-by-M ) */

/*        Form  H C  or  H**H C  where  C = [ B ]  (M-by-N) */
/*                                          [ A ]  (K-by-N) */

/*        H = I - W**H T W          or  H**H = I - W**H T**H W */

/*        A = A -     T (A + V B)  or  A = A -     T**H (A + V B) */
/*        B = B - V**H T (A + V B)  or  B = B - V**H T**H (A + V B) */

/* --------------------------------------------------------------------------- */

/* Computing MIN */
	i__1 = *l + 1;
	mp = f2cmin(i__1,*m);
/* Computing MIN */
	i__1 = *k - *l + 1;
	kp = f2cmin(i__1,*k);

	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *l;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = *k - *l + i__ + j * work_dim1;
		i__4 = i__ + j * b_dim1;
		work[i__3].r = b[i__4].r, work[i__3].i = b[i__4].i;
	    }
	}
	ztrmm_("L", "U", "N", "N", l, n, &c_b14, &v[kp + v_dim1], ldv, &work[
		kp + work_dim1], ldwork);
	i__1 = *m - *l;
	zgemm_("N", "N", l, n, &i__1, &c_b14, &v[kp + mp * v_dim1], ldv, &b[
		mp + b_dim1], ldb, &c_b14, &work[kp + work_dim1], ldwork);
	i__1 = *k - *l;
	zgemm_("N", "N", &i__1, n, m, &c_b14, &v[v_offset], ldv, &b[b_offset],
		 ldb, &c_b22, &work[work_offset], ldwork);

	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *k;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + j * work_dim1;
		i__4 = i__ + j * work_dim1;
		i__5 = i__ + j * a_dim1;
		z__1.r = work[i__4].r + a[i__5].r, z__1.i = work[i__4].i + a[
			i__5].i;
		work[i__3].r = z__1.r, work[i__3].i = z__1.i;
	    }
	}

	ztrmm_("L", "L ", trans, "N", k, n, &c_b14, &t[t_offset], ldt, &work[
		work_offset], ldwork);

	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *k;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + j * a_dim1;
		i__4 = i__ + j * a_dim1;
		i__5 = i__ + j * work_dim1;
		z__1.r = a[i__4].r - work[i__5].r, z__1.i = a[i__4].i - work[
			i__5].i;
		a[i__3].r = z__1.r, a[i__3].i = z__1.i;
	    }
	}

	i__1 = *m - *l;
	zgemm_("C", "N", &i__1, n, k, &c_b29, &v[mp * v_dim1 + 1], ldv, &work[
		work_offset], ldwork, &c_b14, &b[mp + b_dim1], ldb);
	i__1 = *k - *l;
	zgemm_("C", "N", l, n, &i__1, &c_b29, &v[v_offset], ldv, &work[
		work_offset], ldwork, &c_b14, &b[b_offset], ldb);
	ztrmm_("L", "U", "C", "N", l, n, &c_b14, &v[kp + v_dim1], ldv, &work[
		kp + work_dim1], ldwork);
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *l;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + j * b_dim1;
		i__4 = i__ + j * b_dim1;
		i__5 = *k - *l + i__ + j * work_dim1;
		z__1.r = b[i__4].r - work[i__5].r, z__1.i = b[i__4].i - work[
			i__5].i;
		b[i__3].r = z__1.r, b[i__3].i = z__1.i;
	    }
	}

/* --------------------------------------------------------------------------- */

    } else if (row && backward && right) {

/* --------------------------------------------------------------------------- */

/*        Let  W =  [ V I ] ( I is K-by-K, V is K-by-N ) */

/*        Form  C H  or  C H**H  where  C = [ B A ] (A is M-by-K, B is M-by-N) */

/*        H = I - W**H T W            or  H**H = I - W**H T**H W */

/*        A = A - (A + B V**H) T      or  A = A - (A + B V**H) T**H */
/*        B = B - (A + B V**H) T V    or  B = B - (A + B V**H) T**H V */

/* --------------------------------------------------------------------------- */

/* Computing MIN */
	i__1 = *l + 1;
	np = f2cmin(i__1,*n);
/* Computing MIN */
	i__1 = *k - *l + 1;
	kp = f2cmin(i__1,*k);

	i__1 = *l;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *m;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + (*k - *l + j) * work_dim1;
		i__4 = i__ + j * b_dim1;
		work[i__3].r = b[i__4].r, work[i__3].i = b[i__4].i;
	    }
	}
	ztrmm_("R", "U", "C", "N", m, l, &c_b14, &v[kp + v_dim1], ldv, &work[
		kp * work_dim1 + 1], ldwork);
	i__1 = *n - *l;
	zgemm_("N", "C", m, l, &i__1, &c_b14, &b[np * b_dim1 + 1], ldb, &v[kp 
		+ np * v_dim1], ldv, &c_b14, &work[kp * work_dim1 + 1], 
		ldwork);
	i__1 = *k - *l;
	zgemm_("N", "C", m, &i__1, n, &c_b14, &b[b_offset], ldb, &v[v_offset],
		 ldv, &c_b22, &work[work_offset], ldwork);

	i__1 = *k;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *m;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + j * work_dim1;
		i__4 = i__ + j * work_dim1;
		i__5 = i__ + j * a_dim1;
		z__1.r = work[i__4].r + a[i__5].r, z__1.i = work[i__4].i + a[
			i__5].i;
		work[i__3].r = z__1.r, work[i__3].i = z__1.i;
	    }
	}

	ztrmm_("R", "L", trans, "N", m, k, &c_b14, &t[t_offset], ldt, &work[
		work_offset], ldwork);

	i__1 = *k;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *m;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + j * a_dim1;
		i__4 = i__ + j * a_dim1;
		i__5 = i__ + j * work_dim1;
		z__1.r = a[i__4].r - work[i__5].r, z__1.i = a[i__4].i - work[
			i__5].i;
		a[i__3].r = z__1.r, a[i__3].i = z__1.i;
	    }
	}

	i__1 = *n - *l;
	zgemm_("N", "N", m, &i__1, k, &c_b29, &work[work_offset], ldwork, &v[
		np * v_dim1 + 1], ldv, &c_b14, &b[np * b_dim1 + 1], ldb);
	i__1 = *k - *l;
	zgemm_("N", "N", m, l, &i__1, &c_b29, &work[work_offset], ldwork, &v[
		v_offset], ldv, &c_b14, &b[b_offset], ldb);
	ztrmm_("R", "U", "N", "N", m, l, &c_b14, &v[kp + v_dim1], ldv, &work[
		kp * work_dim1 + 1], ldwork);
	i__1 = *l;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *m;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + j * b_dim1;
		i__4 = i__ + j * b_dim1;
		i__5 = i__ + (*k - *l + j) * work_dim1;
		z__1.r = b[i__4].r - work[i__5].r, z__1.i = b[i__4].i - work[
			i__5].i;
		b[i__3].r = z__1.r, b[i__3].i = z__1.i;
	    }
	}

    }

    return;

/*     End of ZTPRFB */

} /* ztprfb_ */

