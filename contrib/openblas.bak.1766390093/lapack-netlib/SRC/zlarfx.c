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

/* > \brief \b ZLARFX applies an elementary reflector to a general rectangular matrix, with loop unrolling whe
n the reflector has order ≤ 10. */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download ZLARFX + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zlarfx.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zlarfx.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zlarfx.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE ZLARFX( SIDE, M, N, V, TAU, C, LDC, WORK ) */

/*       CHARACTER          SIDE */
/*       INTEGER            LDC, M, N */
/*       COMPLEX*16         TAU */
/*       COMPLEX*16         C( LDC, * ), V( * ), WORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > ZLARFX applies a complex elementary reflector H to a complex m by n */
/* > matrix C, from either the left or the right. H is represented in the */
/* > form */
/* > */
/* >       H = I - tau * v * v**H */
/* > */
/* > where tau is a complex scalar and v is a complex vector. */
/* > */
/* > If tau = 0, then H is taken to be the unit matrix */
/* > */
/* > This version uses inline code if H has order < 11. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] SIDE */
/* > \verbatim */
/* >          SIDE is CHARACTER*1 */
/* >          = 'L': form  H * C */
/* >          = 'R': form  C * H */
/* > \endverbatim */
/* > */
/* > \param[in] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >          The number of rows of the matrix C. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The number of columns of the matrix C. */
/* > \endverbatim */
/* > */
/* > \param[in] V */
/* > \verbatim */
/* >          V is COMPLEX*16 array, dimension (M) if SIDE = 'L' */
/* >                                        or (N) if SIDE = 'R' */
/* >          The vector v in the representation of H. */
/* > \endverbatim */
/* > */
/* > \param[in] TAU */
/* > \verbatim */
/* >          TAU is COMPLEX*16 */
/* >          The value tau in the representation of H. */
/* > \endverbatim */
/* > */
/* > \param[in,out] C */
/* > \verbatim */
/* >          C is COMPLEX*16 array, dimension (LDC,N) */
/* >          On entry, the m by n matrix C. */
/* >          On exit, C is overwritten by the matrix H * C if SIDE = 'L', */
/* >          or C * H if SIDE = 'R'. */
/* > \endverbatim */
/* > */
/* > \param[in] LDC */
/* > \verbatim */
/* >          LDC is INTEGER */
/* >          The leading dimension of the array C. LDC >= f2cmax(1,M). */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is COMPLEX*16 array, dimension (N) if SIDE = 'L' */
/* >                                            or (M) if SIDE = 'R' */
/* >          WORK is not referenced if H has order < 11. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup complex16OTHERauxiliary */

/*  ===================================================================== */
/* Subroutine */ void zlarfx_(char *side, integer *m, integer *n, 
	doublecomplex *v, doublecomplex *tau, doublecomplex *c__, integer *
	ldc, doublecomplex *work)
{
    /* System generated locals */
    integer c_dim1, c_offset, i__1, i__2, i__3, i__4, i__5, i__6, i__7, i__8, 
	    i__9, i__10, i__11;
    doublecomplex z__1, z__2, z__3, z__4, z__5, z__6, z__7, z__8, z__9, z__10,
	     z__11, z__12, z__13, z__14, z__15, z__16, z__17, z__18, z__19;

    /* Local variables */
    integer j;
    extern logical lsame_(char *, char *);
    extern /* Subroutine */ void zlarf_(char *, integer *, integer *, 
	    doublecomplex *, integer *, doublecomplex *, doublecomplex *, 
	    integer *, doublecomplex *);
    doublecomplex t1, t2, t3, t4, t5, t6, t7, t8, t9, v1, v2, v3, v4, v5, v6, 
	    v7, v8, v9, t10, v10, sum;


/*  -- LAPACK auxiliary routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


    /* Parameter adjustments */
    --v;
    c_dim1 = *ldc;
    c_offset = 1 + c_dim1 * 1;
    c__ -= c_offset;
    --work;

    /* Function Body */
    if (tau->r == 0. && tau->i == 0.) {
	return;
    }
    if (lsame_(side, "L")) {

/*        Form  H * C, where H has order m. */

	switch (*m) {
	    case 1:  goto L10;
	    case 2:  goto L30;
	    case 3:  goto L50;
	    case 4:  goto L70;
	    case 5:  goto L90;
	    case 6:  goto L110;
	    case 7:  goto L130;
	    case 8:  goto L150;
	    case 9:  goto L170;
	    case 10:  goto L190;
	}

/*        Code for general M */

	zlarf_(side, m, n, &v[1], &c__1, tau, &c__[c_offset], ldc, &work[1]);
	goto L410;
L10:

/*        Special code for 1 x 1 Householder */

	z__3.r = tau->r * v[1].r - tau->i * v[1].i, z__3.i = tau->r * v[1].i 
		+ tau->i * v[1].r;
	d_cnjg(&z__4, &v[1]);
	z__2.r = z__3.r * z__4.r - z__3.i * z__4.i, z__2.i = z__3.r * z__4.i 
		+ z__3.i * z__4.r;
	z__1.r = 1. - z__2.r, z__1.i = 0. - z__2.i;
	t1.r = z__1.r, t1.i = z__1.i;
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = j * c_dim1 + 1;
	    i__3 = j * c_dim1 + 1;
	    z__1.r = t1.r * c__[i__3].r - t1.i * c__[i__3].i, z__1.i = t1.r * 
		    c__[i__3].i + t1.i * c__[i__3].r;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
/* L20: */
	}
	goto L410;
L30:

/*        Special code for 2 x 2 Householder */

	d_cnjg(&z__1, &v[1]);
	v1.r = z__1.r, v1.i = z__1.i;
	d_cnjg(&z__2, &v1);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t1.r = z__1.r, t1.i = z__1.i;
	d_cnjg(&z__1, &v[2]);
	v2.r = z__1.r, v2.i = z__1.i;
	d_cnjg(&z__2, &v2);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t2.r = z__1.r, t2.i = z__1.i;
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = j * c_dim1 + 1;
	    z__2.r = v1.r * c__[i__2].r - v1.i * c__[i__2].i, z__2.i = v1.r * 
		    c__[i__2].i + v1.i * c__[i__2].r;
	    i__3 = j * c_dim1 + 2;
	    z__3.r = v2.r * c__[i__3].r - v2.i * c__[i__3].i, z__3.i = v2.r * 
		    c__[i__3].i + v2.i * c__[i__3].r;
	    z__1.r = z__2.r + z__3.r, z__1.i = z__2.i + z__3.i;
	    sum.r = z__1.r, sum.i = z__1.i;
	    i__2 = j * c_dim1 + 1;
	    i__3 = j * c_dim1 + 1;
	    z__2.r = sum.r * t1.r - sum.i * t1.i, z__2.i = sum.r * t1.i + 
		    sum.i * t1.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 2;
	    i__3 = j * c_dim1 + 2;
	    z__2.r = sum.r * t2.r - sum.i * t2.i, z__2.i = sum.r * t2.i + 
		    sum.i * t2.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
/* L40: */
	}
	goto L410;
L50:

/*        Special code for 3 x 3 Householder */

	d_cnjg(&z__1, &v[1]);
	v1.r = z__1.r, v1.i = z__1.i;
	d_cnjg(&z__2, &v1);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t1.r = z__1.r, t1.i = z__1.i;
	d_cnjg(&z__1, &v[2]);
	v2.r = z__1.r, v2.i = z__1.i;
	d_cnjg(&z__2, &v2);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t2.r = z__1.r, t2.i = z__1.i;
	d_cnjg(&z__1, &v[3]);
	v3.r = z__1.r, v3.i = z__1.i;
	d_cnjg(&z__2, &v3);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t3.r = z__1.r, t3.i = z__1.i;
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = j * c_dim1 + 1;
	    z__3.r = v1.r * c__[i__2].r - v1.i * c__[i__2].i, z__3.i = v1.r * 
		    c__[i__2].i + v1.i * c__[i__2].r;
	    i__3 = j * c_dim1 + 2;
	    z__4.r = v2.r * c__[i__3].r - v2.i * c__[i__3].i, z__4.i = v2.r * 
		    c__[i__3].i + v2.i * c__[i__3].r;
	    z__2.r = z__3.r + z__4.r, z__2.i = z__3.i + z__4.i;
	    i__4 = j * c_dim1 + 3;
	    z__5.r = v3.r * c__[i__4].r - v3.i * c__[i__4].i, z__5.i = v3.r * 
		    c__[i__4].i + v3.i * c__[i__4].r;
	    z__1.r = z__2.r + z__5.r, z__1.i = z__2.i + z__5.i;
	    sum.r = z__1.r, sum.i = z__1.i;
	    i__2 = j * c_dim1 + 1;
	    i__3 = j * c_dim1 + 1;
	    z__2.r = sum.r * t1.r - sum.i * t1.i, z__2.i = sum.r * t1.i + 
		    sum.i * t1.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 2;
	    i__3 = j * c_dim1 + 2;
	    z__2.r = sum.r * t2.r - sum.i * t2.i, z__2.i = sum.r * t2.i + 
		    sum.i * t2.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 3;
	    i__3 = j * c_dim1 + 3;
	    z__2.r = sum.r * t3.r - sum.i * t3.i, z__2.i = sum.r * t3.i + 
		    sum.i * t3.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
/* L60: */
	}
	goto L410;
L70:

/*        Special code for 4 x 4 Householder */

	d_cnjg(&z__1, &v[1]);
	v1.r = z__1.r, v1.i = z__1.i;
	d_cnjg(&z__2, &v1);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t1.r = z__1.r, t1.i = z__1.i;
	d_cnjg(&z__1, &v[2]);
	v2.r = z__1.r, v2.i = z__1.i;
	d_cnjg(&z__2, &v2);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t2.r = z__1.r, t2.i = z__1.i;
	d_cnjg(&z__1, &v[3]);
	v3.r = z__1.r, v3.i = z__1.i;
	d_cnjg(&z__2, &v3);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t3.r = z__1.r, t3.i = z__1.i;
	d_cnjg(&z__1, &v[4]);
	v4.r = z__1.r, v4.i = z__1.i;
	d_cnjg(&z__2, &v4);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t4.r = z__1.r, t4.i = z__1.i;
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = j * c_dim1 + 1;
	    z__4.r = v1.r * c__[i__2].r - v1.i * c__[i__2].i, z__4.i = v1.r * 
		    c__[i__2].i + v1.i * c__[i__2].r;
	    i__3 = j * c_dim1 + 2;
	    z__5.r = v2.r * c__[i__3].r - v2.i * c__[i__3].i, z__5.i = v2.r * 
		    c__[i__3].i + v2.i * c__[i__3].r;
	    z__3.r = z__4.r + z__5.r, z__3.i = z__4.i + z__5.i;
	    i__4 = j * c_dim1 + 3;
	    z__6.r = v3.r * c__[i__4].r - v3.i * c__[i__4].i, z__6.i = v3.r * 
		    c__[i__4].i + v3.i * c__[i__4].r;
	    z__2.r = z__3.r + z__6.r, z__2.i = z__3.i + z__6.i;
	    i__5 = j * c_dim1 + 4;
	    z__7.r = v4.r * c__[i__5].r - v4.i * c__[i__5].i, z__7.i = v4.r * 
		    c__[i__5].i + v4.i * c__[i__5].r;
	    z__1.r = z__2.r + z__7.r, z__1.i = z__2.i + z__7.i;
	    sum.r = z__1.r, sum.i = z__1.i;
	    i__2 = j * c_dim1 + 1;
	    i__3 = j * c_dim1 + 1;
	    z__2.r = sum.r * t1.r - sum.i * t1.i, z__2.i = sum.r * t1.i + 
		    sum.i * t1.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 2;
	    i__3 = j * c_dim1 + 2;
	    z__2.r = sum.r * t2.r - sum.i * t2.i, z__2.i = sum.r * t2.i + 
		    sum.i * t2.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 3;
	    i__3 = j * c_dim1 + 3;
	    z__2.r = sum.r * t3.r - sum.i * t3.i, z__2.i = sum.r * t3.i + 
		    sum.i * t3.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 4;
	    i__3 = j * c_dim1 + 4;
	    z__2.r = sum.r * t4.r - sum.i * t4.i, z__2.i = sum.r * t4.i + 
		    sum.i * t4.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
/* L80: */
	}
	goto L410;
L90:

/*        Special code for 5 x 5 Householder */

	d_cnjg(&z__1, &v[1]);
	v1.r = z__1.r, v1.i = z__1.i;
	d_cnjg(&z__2, &v1);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t1.r = z__1.r, t1.i = z__1.i;
	d_cnjg(&z__1, &v[2]);
	v2.r = z__1.r, v2.i = z__1.i;
	d_cnjg(&z__2, &v2);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t2.r = z__1.r, t2.i = z__1.i;
	d_cnjg(&z__1, &v[3]);
	v3.r = z__1.r, v3.i = z__1.i;
	d_cnjg(&z__2, &v3);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t3.r = z__1.r, t3.i = z__1.i;
	d_cnjg(&z__1, &v[4]);
	v4.r = z__1.r, v4.i = z__1.i;
	d_cnjg(&z__2, &v4);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t4.r = z__1.r, t4.i = z__1.i;
	d_cnjg(&z__1, &v[5]);
	v5.r = z__1.r, v5.i = z__1.i;
	d_cnjg(&z__2, &v5);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t5.r = z__1.r, t5.i = z__1.i;
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = j * c_dim1 + 1;
	    z__5.r = v1.r * c__[i__2].r - v1.i * c__[i__2].i, z__5.i = v1.r * 
		    c__[i__2].i + v1.i * c__[i__2].r;
	    i__3 = j * c_dim1 + 2;
	    z__6.r = v2.r * c__[i__3].r - v2.i * c__[i__3].i, z__6.i = v2.r * 
		    c__[i__3].i + v2.i * c__[i__3].r;
	    z__4.r = z__5.r + z__6.r, z__4.i = z__5.i + z__6.i;
	    i__4 = j * c_dim1 + 3;
	    z__7.r = v3.r * c__[i__4].r - v3.i * c__[i__4].i, z__7.i = v3.r * 
		    c__[i__4].i + v3.i * c__[i__4].r;
	    z__3.r = z__4.r + z__7.r, z__3.i = z__4.i + z__7.i;
	    i__5 = j * c_dim1 + 4;
	    z__8.r = v4.r * c__[i__5].r - v4.i * c__[i__5].i, z__8.i = v4.r * 
		    c__[i__5].i + v4.i * c__[i__5].r;
	    z__2.r = z__3.r + z__8.r, z__2.i = z__3.i + z__8.i;
	    i__6 = j * c_dim1 + 5;
	    z__9.r = v5.r * c__[i__6].r - v5.i * c__[i__6].i, z__9.i = v5.r * 
		    c__[i__6].i + v5.i * c__[i__6].r;
	    z__1.r = z__2.r + z__9.r, z__1.i = z__2.i + z__9.i;
	    sum.r = z__1.r, sum.i = z__1.i;
	    i__2 = j * c_dim1 + 1;
	    i__3 = j * c_dim1 + 1;
	    z__2.r = sum.r * t1.r - sum.i * t1.i, z__2.i = sum.r * t1.i + 
		    sum.i * t1.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 2;
	    i__3 = j * c_dim1 + 2;
	    z__2.r = sum.r * t2.r - sum.i * t2.i, z__2.i = sum.r * t2.i + 
		    sum.i * t2.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 3;
	    i__3 = j * c_dim1 + 3;
	    z__2.r = sum.r * t3.r - sum.i * t3.i, z__2.i = sum.r * t3.i + 
		    sum.i * t3.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 4;
	    i__3 = j * c_dim1 + 4;
	    z__2.r = sum.r * t4.r - sum.i * t4.i, z__2.i = sum.r * t4.i + 
		    sum.i * t4.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 5;
	    i__3 = j * c_dim1 + 5;
	    z__2.r = sum.r * t5.r - sum.i * t5.i, z__2.i = sum.r * t5.i + 
		    sum.i * t5.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
/* L100: */
	}
	goto L410;
L110:

/*        Special code for 6 x 6 Householder */

	d_cnjg(&z__1, &v[1]);
	v1.r = z__1.r, v1.i = z__1.i;
	d_cnjg(&z__2, &v1);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t1.r = z__1.r, t1.i = z__1.i;
	d_cnjg(&z__1, &v[2]);
	v2.r = z__1.r, v2.i = z__1.i;
	d_cnjg(&z__2, &v2);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t2.r = z__1.r, t2.i = z__1.i;
	d_cnjg(&z__1, &v[3]);
	v3.r = z__1.r, v3.i = z__1.i;
	d_cnjg(&z__2, &v3);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t3.r = z__1.r, t3.i = z__1.i;
	d_cnjg(&z__1, &v[4]);
	v4.r = z__1.r, v4.i = z__1.i;
	d_cnjg(&z__2, &v4);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t4.r = z__1.r, t4.i = z__1.i;
	d_cnjg(&z__1, &v[5]);
	v5.r = z__1.r, v5.i = z__1.i;
	d_cnjg(&z__2, &v5);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t5.r = z__1.r, t5.i = z__1.i;
	d_cnjg(&z__1, &v[6]);
	v6.r = z__1.r, v6.i = z__1.i;
	d_cnjg(&z__2, &v6);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t6.r = z__1.r, t6.i = z__1.i;
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = j * c_dim1 + 1;
	    z__6.r = v1.r * c__[i__2].r - v1.i * c__[i__2].i, z__6.i = v1.r * 
		    c__[i__2].i + v1.i * c__[i__2].r;
	    i__3 = j * c_dim1 + 2;
	    z__7.r = v2.r * c__[i__3].r - v2.i * c__[i__3].i, z__7.i = v2.r * 
		    c__[i__3].i + v2.i * c__[i__3].r;
	    z__5.r = z__6.r + z__7.r, z__5.i = z__6.i + z__7.i;
	    i__4 = j * c_dim1 + 3;
	    z__8.r = v3.r * c__[i__4].r - v3.i * c__[i__4].i, z__8.i = v3.r * 
		    c__[i__4].i + v3.i * c__[i__4].r;
	    z__4.r = z__5.r + z__8.r, z__4.i = z__5.i + z__8.i;
	    i__5 = j * c_dim1 + 4;
	    z__9.r = v4.r * c__[i__5].r - v4.i * c__[i__5].i, z__9.i = v4.r * 
		    c__[i__5].i + v4.i * c__[i__5].r;
	    z__3.r = z__4.r + z__9.r, z__3.i = z__4.i + z__9.i;
	    i__6 = j * c_dim1 + 5;
	    z__10.r = v5.r * c__[i__6].r - v5.i * c__[i__6].i, z__10.i = v5.r 
		    * c__[i__6].i + v5.i * c__[i__6].r;
	    z__2.r = z__3.r + z__10.r, z__2.i = z__3.i + z__10.i;
	    i__7 = j * c_dim1 + 6;
	    z__11.r = v6.r * c__[i__7].r - v6.i * c__[i__7].i, z__11.i = v6.r 
		    * c__[i__7].i + v6.i * c__[i__7].r;
	    z__1.r = z__2.r + z__11.r, z__1.i = z__2.i + z__11.i;
	    sum.r = z__1.r, sum.i = z__1.i;
	    i__2 = j * c_dim1 + 1;
	    i__3 = j * c_dim1 + 1;
	    z__2.r = sum.r * t1.r - sum.i * t1.i, z__2.i = sum.r * t1.i + 
		    sum.i * t1.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 2;
	    i__3 = j * c_dim1 + 2;
	    z__2.r = sum.r * t2.r - sum.i * t2.i, z__2.i = sum.r * t2.i + 
		    sum.i * t2.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 3;
	    i__3 = j * c_dim1 + 3;
	    z__2.r = sum.r * t3.r - sum.i * t3.i, z__2.i = sum.r * t3.i + 
		    sum.i * t3.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 4;
	    i__3 = j * c_dim1 + 4;
	    z__2.r = sum.r * t4.r - sum.i * t4.i, z__2.i = sum.r * t4.i + 
		    sum.i * t4.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 5;
	    i__3 = j * c_dim1 + 5;
	    z__2.r = sum.r * t5.r - sum.i * t5.i, z__2.i = sum.r * t5.i + 
		    sum.i * t5.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 6;
	    i__3 = j * c_dim1 + 6;
	    z__2.r = sum.r * t6.r - sum.i * t6.i, z__2.i = sum.r * t6.i + 
		    sum.i * t6.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
/* L120: */
	}
	goto L410;
L130:

/*        Special code for 7 x 7 Householder */

	d_cnjg(&z__1, &v[1]);
	v1.r = z__1.r, v1.i = z__1.i;
	d_cnjg(&z__2, &v1);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t1.r = z__1.r, t1.i = z__1.i;
	d_cnjg(&z__1, &v[2]);
	v2.r = z__1.r, v2.i = z__1.i;
	d_cnjg(&z__2, &v2);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t2.r = z__1.r, t2.i = z__1.i;
	d_cnjg(&z__1, &v[3]);
	v3.r = z__1.r, v3.i = z__1.i;
	d_cnjg(&z__2, &v3);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t3.r = z__1.r, t3.i = z__1.i;
	d_cnjg(&z__1, &v[4]);
	v4.r = z__1.r, v4.i = z__1.i;
	d_cnjg(&z__2, &v4);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t4.r = z__1.r, t4.i = z__1.i;
	d_cnjg(&z__1, &v[5]);
	v5.r = z__1.r, v5.i = z__1.i;
	d_cnjg(&z__2, &v5);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t5.r = z__1.r, t5.i = z__1.i;
	d_cnjg(&z__1, &v[6]);
	v6.r = z__1.r, v6.i = z__1.i;
	d_cnjg(&z__2, &v6);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t6.r = z__1.r, t6.i = z__1.i;
	d_cnjg(&z__1, &v[7]);
	v7.r = z__1.r, v7.i = z__1.i;
	d_cnjg(&z__2, &v7);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t7.r = z__1.r, t7.i = z__1.i;
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = j * c_dim1 + 1;
	    z__7.r = v1.r * c__[i__2].r - v1.i * c__[i__2].i, z__7.i = v1.r * 
		    c__[i__2].i + v1.i * c__[i__2].r;
	    i__3 = j * c_dim1 + 2;
	    z__8.r = v2.r * c__[i__3].r - v2.i * c__[i__3].i, z__8.i = v2.r * 
		    c__[i__3].i + v2.i * c__[i__3].r;
	    z__6.r = z__7.r + z__8.r, z__6.i = z__7.i + z__8.i;
	    i__4 = j * c_dim1 + 3;
	    z__9.r = v3.r * c__[i__4].r - v3.i * c__[i__4].i, z__9.i = v3.r * 
		    c__[i__4].i + v3.i * c__[i__4].r;
	    z__5.r = z__6.r + z__9.r, z__5.i = z__6.i + z__9.i;
	    i__5 = j * c_dim1 + 4;
	    z__10.r = v4.r * c__[i__5].r - v4.i * c__[i__5].i, z__10.i = v4.r 
		    * c__[i__5].i + v4.i * c__[i__5].r;
	    z__4.r = z__5.r + z__10.r, z__4.i = z__5.i + z__10.i;
	    i__6 = j * c_dim1 + 5;
	    z__11.r = v5.r * c__[i__6].r - v5.i * c__[i__6].i, z__11.i = v5.r 
		    * c__[i__6].i + v5.i * c__[i__6].r;
	    z__3.r = z__4.r + z__11.r, z__3.i = z__4.i + z__11.i;
	    i__7 = j * c_dim1 + 6;
	    z__12.r = v6.r * c__[i__7].r - v6.i * c__[i__7].i, z__12.i = v6.r 
		    * c__[i__7].i + v6.i * c__[i__7].r;
	    z__2.r = z__3.r + z__12.r, z__2.i = z__3.i + z__12.i;
	    i__8 = j * c_dim1 + 7;
	    z__13.r = v7.r * c__[i__8].r - v7.i * c__[i__8].i, z__13.i = v7.r 
		    * c__[i__8].i + v7.i * c__[i__8].r;
	    z__1.r = z__2.r + z__13.r, z__1.i = z__2.i + z__13.i;
	    sum.r = z__1.r, sum.i = z__1.i;
	    i__2 = j * c_dim1 + 1;
	    i__3 = j * c_dim1 + 1;
	    z__2.r = sum.r * t1.r - sum.i * t1.i, z__2.i = sum.r * t1.i + 
		    sum.i * t1.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 2;
	    i__3 = j * c_dim1 + 2;
	    z__2.r = sum.r * t2.r - sum.i * t2.i, z__2.i = sum.r * t2.i + 
		    sum.i * t2.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 3;
	    i__3 = j * c_dim1 + 3;
	    z__2.r = sum.r * t3.r - sum.i * t3.i, z__2.i = sum.r * t3.i + 
		    sum.i * t3.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 4;
	    i__3 = j * c_dim1 + 4;
	    z__2.r = sum.r * t4.r - sum.i * t4.i, z__2.i = sum.r * t4.i + 
		    sum.i * t4.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 5;
	    i__3 = j * c_dim1 + 5;
	    z__2.r = sum.r * t5.r - sum.i * t5.i, z__2.i = sum.r * t5.i + 
		    sum.i * t5.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 6;
	    i__3 = j * c_dim1 + 6;
	    z__2.r = sum.r * t6.r - sum.i * t6.i, z__2.i = sum.r * t6.i + 
		    sum.i * t6.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 7;
	    i__3 = j * c_dim1 + 7;
	    z__2.r = sum.r * t7.r - sum.i * t7.i, z__2.i = sum.r * t7.i + 
		    sum.i * t7.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
/* L140: */
	}
	goto L410;
L150:

/*        Special code for 8 x 8 Householder */

	d_cnjg(&z__1, &v[1]);
	v1.r = z__1.r, v1.i = z__1.i;
	d_cnjg(&z__2, &v1);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t1.r = z__1.r, t1.i = z__1.i;
	d_cnjg(&z__1, &v[2]);
	v2.r = z__1.r, v2.i = z__1.i;
	d_cnjg(&z__2, &v2);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t2.r = z__1.r, t2.i = z__1.i;
	d_cnjg(&z__1, &v[3]);
	v3.r = z__1.r, v3.i = z__1.i;
	d_cnjg(&z__2, &v3);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t3.r = z__1.r, t3.i = z__1.i;
	d_cnjg(&z__1, &v[4]);
	v4.r = z__1.r, v4.i = z__1.i;
	d_cnjg(&z__2, &v4);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t4.r = z__1.r, t4.i = z__1.i;
	d_cnjg(&z__1, &v[5]);
	v5.r = z__1.r, v5.i = z__1.i;
	d_cnjg(&z__2, &v5);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t5.r = z__1.r, t5.i = z__1.i;
	d_cnjg(&z__1, &v[6]);
	v6.r = z__1.r, v6.i = z__1.i;
	d_cnjg(&z__2, &v6);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t6.r = z__1.r, t6.i = z__1.i;
	d_cnjg(&z__1, &v[7]);
	v7.r = z__1.r, v7.i = z__1.i;
	d_cnjg(&z__2, &v7);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t7.r = z__1.r, t7.i = z__1.i;
	d_cnjg(&z__1, &v[8]);
	v8.r = z__1.r, v8.i = z__1.i;
	d_cnjg(&z__2, &v8);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t8.r = z__1.r, t8.i = z__1.i;
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = j * c_dim1 + 1;
	    z__8.r = v1.r * c__[i__2].r - v1.i * c__[i__2].i, z__8.i = v1.r * 
		    c__[i__2].i + v1.i * c__[i__2].r;
	    i__3 = j * c_dim1 + 2;
	    z__9.r = v2.r * c__[i__3].r - v2.i * c__[i__3].i, z__9.i = v2.r * 
		    c__[i__3].i + v2.i * c__[i__3].r;
	    z__7.r = z__8.r + z__9.r, z__7.i = z__8.i + z__9.i;
	    i__4 = j * c_dim1 + 3;
	    z__10.r = v3.r * c__[i__4].r - v3.i * c__[i__4].i, z__10.i = v3.r 
		    * c__[i__4].i + v3.i * c__[i__4].r;
	    z__6.r = z__7.r + z__10.r, z__6.i = z__7.i + z__10.i;
	    i__5 = j * c_dim1 + 4;
	    z__11.r = v4.r * c__[i__5].r - v4.i * c__[i__5].i, z__11.i = v4.r 
		    * c__[i__5].i + v4.i * c__[i__5].r;
	    z__5.r = z__6.r + z__11.r, z__5.i = z__6.i + z__11.i;
	    i__6 = j * c_dim1 + 5;
	    z__12.r = v5.r * c__[i__6].r - v5.i * c__[i__6].i, z__12.i = v5.r 
		    * c__[i__6].i + v5.i * c__[i__6].r;
	    z__4.r = z__5.r + z__12.r, z__4.i = z__5.i + z__12.i;
	    i__7 = j * c_dim1 + 6;
	    z__13.r = v6.r * c__[i__7].r - v6.i * c__[i__7].i, z__13.i = v6.r 
		    * c__[i__7].i + v6.i * c__[i__7].r;
	    z__3.r = z__4.r + z__13.r, z__3.i = z__4.i + z__13.i;
	    i__8 = j * c_dim1 + 7;
	    z__14.r = v7.r * c__[i__8].r - v7.i * c__[i__8].i, z__14.i = v7.r 
		    * c__[i__8].i + v7.i * c__[i__8].r;
	    z__2.r = z__3.r + z__14.r, z__2.i = z__3.i + z__14.i;
	    i__9 = j * c_dim1 + 8;
	    z__15.r = v8.r * c__[i__9].r - v8.i * c__[i__9].i, z__15.i = v8.r 
		    * c__[i__9].i + v8.i * c__[i__9].r;
	    z__1.r = z__2.r + z__15.r, z__1.i = z__2.i + z__15.i;
	    sum.r = z__1.r, sum.i = z__1.i;
	    i__2 = j * c_dim1 + 1;
	    i__3 = j * c_dim1 + 1;
	    z__2.r = sum.r * t1.r - sum.i * t1.i, z__2.i = sum.r * t1.i + 
		    sum.i * t1.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 2;
	    i__3 = j * c_dim1 + 2;
	    z__2.r = sum.r * t2.r - sum.i * t2.i, z__2.i = sum.r * t2.i + 
		    sum.i * t2.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 3;
	    i__3 = j * c_dim1 + 3;
	    z__2.r = sum.r * t3.r - sum.i * t3.i, z__2.i = sum.r * t3.i + 
		    sum.i * t3.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 4;
	    i__3 = j * c_dim1 + 4;
	    z__2.r = sum.r * t4.r - sum.i * t4.i, z__2.i = sum.r * t4.i + 
		    sum.i * t4.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 5;
	    i__3 = j * c_dim1 + 5;
	    z__2.r = sum.r * t5.r - sum.i * t5.i, z__2.i = sum.r * t5.i + 
		    sum.i * t5.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 6;
	    i__3 = j * c_dim1 + 6;
	    z__2.r = sum.r * t6.r - sum.i * t6.i, z__2.i = sum.r * t6.i + 
		    sum.i * t6.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 7;
	    i__3 = j * c_dim1 + 7;
	    z__2.r = sum.r * t7.r - sum.i * t7.i, z__2.i = sum.r * t7.i + 
		    sum.i * t7.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 8;
	    i__3 = j * c_dim1 + 8;
	    z__2.r = sum.r * t8.r - sum.i * t8.i, z__2.i = sum.r * t8.i + 
		    sum.i * t8.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
/* L160: */
	}
	goto L410;
L170:

/*        Special code for 9 x 9 Householder */

	d_cnjg(&z__1, &v[1]);
	v1.r = z__1.r, v1.i = z__1.i;
	d_cnjg(&z__2, &v1);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t1.r = z__1.r, t1.i = z__1.i;
	d_cnjg(&z__1, &v[2]);
	v2.r = z__1.r, v2.i = z__1.i;
	d_cnjg(&z__2, &v2);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t2.r = z__1.r, t2.i = z__1.i;
	d_cnjg(&z__1, &v[3]);
	v3.r = z__1.r, v3.i = z__1.i;
	d_cnjg(&z__2, &v3);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t3.r = z__1.r, t3.i = z__1.i;
	d_cnjg(&z__1, &v[4]);
	v4.r = z__1.r, v4.i = z__1.i;
	d_cnjg(&z__2, &v4);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t4.r = z__1.r, t4.i = z__1.i;
	d_cnjg(&z__1, &v[5]);
	v5.r = z__1.r, v5.i = z__1.i;
	d_cnjg(&z__2, &v5);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t5.r = z__1.r, t5.i = z__1.i;
	d_cnjg(&z__1, &v[6]);
	v6.r = z__1.r, v6.i = z__1.i;
	d_cnjg(&z__2, &v6);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t6.r = z__1.r, t6.i = z__1.i;
	d_cnjg(&z__1, &v[7]);
	v7.r = z__1.r, v7.i = z__1.i;
	d_cnjg(&z__2, &v7);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t7.r = z__1.r, t7.i = z__1.i;
	d_cnjg(&z__1, &v[8]);
	v8.r = z__1.r, v8.i = z__1.i;
	d_cnjg(&z__2, &v8);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t8.r = z__1.r, t8.i = z__1.i;
	d_cnjg(&z__1, &v[9]);
	v9.r = z__1.r, v9.i = z__1.i;
	d_cnjg(&z__2, &v9);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t9.r = z__1.r, t9.i = z__1.i;
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = j * c_dim1 + 1;
	    z__9.r = v1.r * c__[i__2].r - v1.i * c__[i__2].i, z__9.i = v1.r * 
		    c__[i__2].i + v1.i * c__[i__2].r;
	    i__3 = j * c_dim1 + 2;
	    z__10.r = v2.r * c__[i__3].r - v2.i * c__[i__3].i, z__10.i = v2.r 
		    * c__[i__3].i + v2.i * c__[i__3].r;
	    z__8.r = z__9.r + z__10.r, z__8.i = z__9.i + z__10.i;
	    i__4 = j * c_dim1 + 3;
	    z__11.r = v3.r * c__[i__4].r - v3.i * c__[i__4].i, z__11.i = v3.r 
		    * c__[i__4].i + v3.i * c__[i__4].r;
	    z__7.r = z__8.r + z__11.r, z__7.i = z__8.i + z__11.i;
	    i__5 = j * c_dim1 + 4;
	    z__12.r = v4.r * c__[i__5].r - v4.i * c__[i__5].i, z__12.i = v4.r 
		    * c__[i__5].i + v4.i * c__[i__5].r;
	    z__6.r = z__7.r + z__12.r, z__6.i = z__7.i + z__12.i;
	    i__6 = j * c_dim1 + 5;
	    z__13.r = v5.r * c__[i__6].r - v5.i * c__[i__6].i, z__13.i = v5.r 
		    * c__[i__6].i + v5.i * c__[i__6].r;
	    z__5.r = z__6.r + z__13.r, z__5.i = z__6.i + z__13.i;
	    i__7 = j * c_dim1 + 6;
	    z__14.r = v6.r * c__[i__7].r - v6.i * c__[i__7].i, z__14.i = v6.r 
		    * c__[i__7].i + v6.i * c__[i__7].r;
	    z__4.r = z__5.r + z__14.r, z__4.i = z__5.i + z__14.i;
	    i__8 = j * c_dim1 + 7;
	    z__15.r = v7.r * c__[i__8].r - v7.i * c__[i__8].i, z__15.i = v7.r 
		    * c__[i__8].i + v7.i * c__[i__8].r;
	    z__3.r = z__4.r + z__15.r, z__3.i = z__4.i + z__15.i;
	    i__9 = j * c_dim1 + 8;
	    z__16.r = v8.r * c__[i__9].r - v8.i * c__[i__9].i, z__16.i = v8.r 
		    * c__[i__9].i + v8.i * c__[i__9].r;
	    z__2.r = z__3.r + z__16.r, z__2.i = z__3.i + z__16.i;
	    i__10 = j * c_dim1 + 9;
	    z__17.r = v9.r * c__[i__10].r - v9.i * c__[i__10].i, z__17.i = 
		    v9.r * c__[i__10].i + v9.i * c__[i__10].r;
	    z__1.r = z__2.r + z__17.r, z__1.i = z__2.i + z__17.i;
	    sum.r = z__1.r, sum.i = z__1.i;
	    i__2 = j * c_dim1 + 1;
	    i__3 = j * c_dim1 + 1;
	    z__2.r = sum.r * t1.r - sum.i * t1.i, z__2.i = sum.r * t1.i + 
		    sum.i * t1.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 2;
	    i__3 = j * c_dim1 + 2;
	    z__2.r = sum.r * t2.r - sum.i * t2.i, z__2.i = sum.r * t2.i + 
		    sum.i * t2.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 3;
	    i__3 = j * c_dim1 + 3;
	    z__2.r = sum.r * t3.r - sum.i * t3.i, z__2.i = sum.r * t3.i + 
		    sum.i * t3.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 4;
	    i__3 = j * c_dim1 + 4;
	    z__2.r = sum.r * t4.r - sum.i * t4.i, z__2.i = sum.r * t4.i + 
		    sum.i * t4.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 5;
	    i__3 = j * c_dim1 + 5;
	    z__2.r = sum.r * t5.r - sum.i * t5.i, z__2.i = sum.r * t5.i + 
		    sum.i * t5.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 6;
	    i__3 = j * c_dim1 + 6;
	    z__2.r = sum.r * t6.r - sum.i * t6.i, z__2.i = sum.r * t6.i + 
		    sum.i * t6.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 7;
	    i__3 = j * c_dim1 + 7;
	    z__2.r = sum.r * t7.r - sum.i * t7.i, z__2.i = sum.r * t7.i + 
		    sum.i * t7.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 8;
	    i__3 = j * c_dim1 + 8;
	    z__2.r = sum.r * t8.r - sum.i * t8.i, z__2.i = sum.r * t8.i + 
		    sum.i * t8.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 9;
	    i__3 = j * c_dim1 + 9;
	    z__2.r = sum.r * t9.r - sum.i * t9.i, z__2.i = sum.r * t9.i + 
		    sum.i * t9.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
/* L180: */
	}
	goto L410;
L190:

/*        Special code for 10 x 10 Householder */

	d_cnjg(&z__1, &v[1]);
	v1.r = z__1.r, v1.i = z__1.i;
	d_cnjg(&z__2, &v1);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t1.r = z__1.r, t1.i = z__1.i;
	d_cnjg(&z__1, &v[2]);
	v2.r = z__1.r, v2.i = z__1.i;
	d_cnjg(&z__2, &v2);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t2.r = z__1.r, t2.i = z__1.i;
	d_cnjg(&z__1, &v[3]);
	v3.r = z__1.r, v3.i = z__1.i;
	d_cnjg(&z__2, &v3);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t3.r = z__1.r, t3.i = z__1.i;
	d_cnjg(&z__1, &v[4]);
	v4.r = z__1.r, v4.i = z__1.i;
	d_cnjg(&z__2, &v4);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t4.r = z__1.r, t4.i = z__1.i;
	d_cnjg(&z__1, &v[5]);
	v5.r = z__1.r, v5.i = z__1.i;
	d_cnjg(&z__2, &v5);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t5.r = z__1.r, t5.i = z__1.i;
	d_cnjg(&z__1, &v[6]);
	v6.r = z__1.r, v6.i = z__1.i;
	d_cnjg(&z__2, &v6);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t6.r = z__1.r, t6.i = z__1.i;
	d_cnjg(&z__1, &v[7]);
	v7.r = z__1.r, v7.i = z__1.i;
	d_cnjg(&z__2, &v7);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t7.r = z__1.r, t7.i = z__1.i;
	d_cnjg(&z__1, &v[8]);
	v8.r = z__1.r, v8.i = z__1.i;
	d_cnjg(&z__2, &v8);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t8.r = z__1.r, t8.i = z__1.i;
	d_cnjg(&z__1, &v[9]);
	v9.r = z__1.r, v9.i = z__1.i;
	d_cnjg(&z__2, &v9);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t9.r = z__1.r, t9.i = z__1.i;
	d_cnjg(&z__1, &v[10]);
	v10.r = z__1.r, v10.i = z__1.i;
	d_cnjg(&z__2, &v10);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t10.r = z__1.r, t10.i = z__1.i;
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = j * c_dim1 + 1;
	    z__10.r = v1.r * c__[i__2].r - v1.i * c__[i__2].i, z__10.i = v1.r 
		    * c__[i__2].i + v1.i * c__[i__2].r;
	    i__3 = j * c_dim1 + 2;
	    z__11.r = v2.r * c__[i__3].r - v2.i * c__[i__3].i, z__11.i = v2.r 
		    * c__[i__3].i + v2.i * c__[i__3].r;
	    z__9.r = z__10.r + z__11.r, z__9.i = z__10.i + z__11.i;
	    i__4 = j * c_dim1 + 3;
	    z__12.r = v3.r * c__[i__4].r - v3.i * c__[i__4].i, z__12.i = v3.r 
		    * c__[i__4].i + v3.i * c__[i__4].r;
	    z__8.r = z__9.r + z__12.r, z__8.i = z__9.i + z__12.i;
	    i__5 = j * c_dim1 + 4;
	    z__13.r = v4.r * c__[i__5].r - v4.i * c__[i__5].i, z__13.i = v4.r 
		    * c__[i__5].i + v4.i * c__[i__5].r;
	    z__7.r = z__8.r + z__13.r, z__7.i = z__8.i + z__13.i;
	    i__6 = j * c_dim1 + 5;
	    z__14.r = v5.r * c__[i__6].r - v5.i * c__[i__6].i, z__14.i = v5.r 
		    * c__[i__6].i + v5.i * c__[i__6].r;
	    z__6.r = z__7.r + z__14.r, z__6.i = z__7.i + z__14.i;
	    i__7 = j * c_dim1 + 6;
	    z__15.r = v6.r * c__[i__7].r - v6.i * c__[i__7].i, z__15.i = v6.r 
		    * c__[i__7].i + v6.i * c__[i__7].r;
	    z__5.r = z__6.r + z__15.r, z__5.i = z__6.i + z__15.i;
	    i__8 = j * c_dim1 + 7;
	    z__16.r = v7.r * c__[i__8].r - v7.i * c__[i__8].i, z__16.i = v7.r 
		    * c__[i__8].i + v7.i * c__[i__8].r;
	    z__4.r = z__5.r + z__16.r, z__4.i = z__5.i + z__16.i;
	    i__9 = j * c_dim1 + 8;
	    z__17.r = v8.r * c__[i__9].r - v8.i * c__[i__9].i, z__17.i = v8.r 
		    * c__[i__9].i + v8.i * c__[i__9].r;
	    z__3.r = z__4.r + z__17.r, z__3.i = z__4.i + z__17.i;
	    i__10 = j * c_dim1 + 9;
	    z__18.r = v9.r * c__[i__10].r - v9.i * c__[i__10].i, z__18.i = 
		    v9.r * c__[i__10].i + v9.i * c__[i__10].r;
	    z__2.r = z__3.r + z__18.r, z__2.i = z__3.i + z__18.i;
	    i__11 = j * c_dim1 + 10;
	    z__19.r = v10.r * c__[i__11].r - v10.i * c__[i__11].i, z__19.i = 
		    v10.r * c__[i__11].i + v10.i * c__[i__11].r;
	    z__1.r = z__2.r + z__19.r, z__1.i = z__2.i + z__19.i;
	    sum.r = z__1.r, sum.i = z__1.i;
	    i__2 = j * c_dim1 + 1;
	    i__3 = j * c_dim1 + 1;
	    z__2.r = sum.r * t1.r - sum.i * t1.i, z__2.i = sum.r * t1.i + 
		    sum.i * t1.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 2;
	    i__3 = j * c_dim1 + 2;
	    z__2.r = sum.r * t2.r - sum.i * t2.i, z__2.i = sum.r * t2.i + 
		    sum.i * t2.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 3;
	    i__3 = j * c_dim1 + 3;
	    z__2.r = sum.r * t3.r - sum.i * t3.i, z__2.i = sum.r * t3.i + 
		    sum.i * t3.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 4;
	    i__3 = j * c_dim1 + 4;
	    z__2.r = sum.r * t4.r - sum.i * t4.i, z__2.i = sum.r * t4.i + 
		    sum.i * t4.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 5;
	    i__3 = j * c_dim1 + 5;
	    z__2.r = sum.r * t5.r - sum.i * t5.i, z__2.i = sum.r * t5.i + 
		    sum.i * t5.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 6;
	    i__3 = j * c_dim1 + 6;
	    z__2.r = sum.r * t6.r - sum.i * t6.i, z__2.i = sum.r * t6.i + 
		    sum.i * t6.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 7;
	    i__3 = j * c_dim1 + 7;
	    z__2.r = sum.r * t7.r - sum.i * t7.i, z__2.i = sum.r * t7.i + 
		    sum.i * t7.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 8;
	    i__3 = j * c_dim1 + 8;
	    z__2.r = sum.r * t8.r - sum.i * t8.i, z__2.i = sum.r * t8.i + 
		    sum.i * t8.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 9;
	    i__3 = j * c_dim1 + 9;
	    z__2.r = sum.r * t9.r - sum.i * t9.i, z__2.i = sum.r * t9.i + 
		    sum.i * t9.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j * c_dim1 + 10;
	    i__3 = j * c_dim1 + 10;
	    z__2.r = sum.r * t10.r - sum.i * t10.i, z__2.i = sum.r * t10.i + 
		    sum.i * t10.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
/* L200: */
	}
	goto L410;
    } else {

/*        Form  C * H, where H has order n. */

	switch (*n) {
	    case 1:  goto L210;
	    case 2:  goto L230;
	    case 3:  goto L250;
	    case 4:  goto L270;
	    case 5:  goto L290;
	    case 6:  goto L310;
	    case 7:  goto L330;
	    case 8:  goto L350;
	    case 9:  goto L370;
	    case 10:  goto L390;
	}

/*        Code for general N */

	zlarf_(side, m, n, &v[1], &c__1, tau, &c__[c_offset], ldc, &work[1]);
	goto L410;
L210:

/*        Special code for 1 x 1 Householder */

	z__3.r = tau->r * v[1].r - tau->i * v[1].i, z__3.i = tau->r * v[1].i 
		+ tau->i * v[1].r;
	d_cnjg(&z__4, &v[1]);
	z__2.r = z__3.r * z__4.r - z__3.i * z__4.i, z__2.i = z__3.r * z__4.i 
		+ z__3.i * z__4.r;
	z__1.r = 1. - z__2.r, z__1.i = 0. - z__2.i;
	t1.r = z__1.r, t1.i = z__1.i;
	i__1 = *m;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = j + c_dim1;
	    i__3 = j + c_dim1;
	    z__1.r = t1.r * c__[i__3].r - t1.i * c__[i__3].i, z__1.i = t1.r * 
		    c__[i__3].i + t1.i * c__[i__3].r;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
/* L220: */
	}
	goto L410;
L230:

/*        Special code for 2 x 2 Householder */

	v1.r = v[1].r, v1.i = v[1].i;
	d_cnjg(&z__2, &v1);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t1.r = z__1.r, t1.i = z__1.i;
	v2.r = v[2].r, v2.i = v[2].i;
	d_cnjg(&z__2, &v2);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t2.r = z__1.r, t2.i = z__1.i;
	i__1 = *m;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = j + c_dim1;
	    z__2.r = v1.r * c__[i__2].r - v1.i * c__[i__2].i, z__2.i = v1.r * 
		    c__[i__2].i + v1.i * c__[i__2].r;
	    i__3 = j + (c_dim1 << 1);
	    z__3.r = v2.r * c__[i__3].r - v2.i * c__[i__3].i, z__3.i = v2.r * 
		    c__[i__3].i + v2.i * c__[i__3].r;
	    z__1.r = z__2.r + z__3.r, z__1.i = z__2.i + z__3.i;
	    sum.r = z__1.r, sum.i = z__1.i;
	    i__2 = j + c_dim1;
	    i__3 = j + c_dim1;
	    z__2.r = sum.r * t1.r - sum.i * t1.i, z__2.i = sum.r * t1.i + 
		    sum.i * t1.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + (c_dim1 << 1);
	    i__3 = j + (c_dim1 << 1);
	    z__2.r = sum.r * t2.r - sum.i * t2.i, z__2.i = sum.r * t2.i + 
		    sum.i * t2.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
/* L240: */
	}
	goto L410;
L250:

/*        Special code for 3 x 3 Householder */

	v1.r = v[1].r, v1.i = v[1].i;
	d_cnjg(&z__2, &v1);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t1.r = z__1.r, t1.i = z__1.i;
	v2.r = v[2].r, v2.i = v[2].i;
	d_cnjg(&z__2, &v2);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t2.r = z__1.r, t2.i = z__1.i;
	v3.r = v[3].r, v3.i = v[3].i;
	d_cnjg(&z__2, &v3);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t3.r = z__1.r, t3.i = z__1.i;
	i__1 = *m;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = j + c_dim1;
	    z__3.r = v1.r * c__[i__2].r - v1.i * c__[i__2].i, z__3.i = v1.r * 
		    c__[i__2].i + v1.i * c__[i__2].r;
	    i__3 = j + (c_dim1 << 1);
	    z__4.r = v2.r * c__[i__3].r - v2.i * c__[i__3].i, z__4.i = v2.r * 
		    c__[i__3].i + v2.i * c__[i__3].r;
	    z__2.r = z__3.r + z__4.r, z__2.i = z__3.i + z__4.i;
	    i__4 = j + c_dim1 * 3;
	    z__5.r = v3.r * c__[i__4].r - v3.i * c__[i__4].i, z__5.i = v3.r * 
		    c__[i__4].i + v3.i * c__[i__4].r;
	    z__1.r = z__2.r + z__5.r, z__1.i = z__2.i + z__5.i;
	    sum.r = z__1.r, sum.i = z__1.i;
	    i__2 = j + c_dim1;
	    i__3 = j + c_dim1;
	    z__2.r = sum.r * t1.r - sum.i * t1.i, z__2.i = sum.r * t1.i + 
		    sum.i * t1.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + (c_dim1 << 1);
	    i__3 = j + (c_dim1 << 1);
	    z__2.r = sum.r * t2.r - sum.i * t2.i, z__2.i = sum.r * t2.i + 
		    sum.i * t2.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + c_dim1 * 3;
	    i__3 = j + c_dim1 * 3;
	    z__2.r = sum.r * t3.r - sum.i * t3.i, z__2.i = sum.r * t3.i + 
		    sum.i * t3.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
/* L260: */
	}
	goto L410;
L270:

/*        Special code for 4 x 4 Householder */

	v1.r = v[1].r, v1.i = v[1].i;
	d_cnjg(&z__2, &v1);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t1.r = z__1.r, t1.i = z__1.i;
	v2.r = v[2].r, v2.i = v[2].i;
	d_cnjg(&z__2, &v2);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t2.r = z__1.r, t2.i = z__1.i;
	v3.r = v[3].r, v3.i = v[3].i;
	d_cnjg(&z__2, &v3);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t3.r = z__1.r, t3.i = z__1.i;
	v4.r = v[4].r, v4.i = v[4].i;
	d_cnjg(&z__2, &v4);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t4.r = z__1.r, t4.i = z__1.i;
	i__1 = *m;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = j + c_dim1;
	    z__4.r = v1.r * c__[i__2].r - v1.i * c__[i__2].i, z__4.i = v1.r * 
		    c__[i__2].i + v1.i * c__[i__2].r;
	    i__3 = j + (c_dim1 << 1);
	    z__5.r = v2.r * c__[i__3].r - v2.i * c__[i__3].i, z__5.i = v2.r * 
		    c__[i__3].i + v2.i * c__[i__3].r;
	    z__3.r = z__4.r + z__5.r, z__3.i = z__4.i + z__5.i;
	    i__4 = j + c_dim1 * 3;
	    z__6.r = v3.r * c__[i__4].r - v3.i * c__[i__4].i, z__6.i = v3.r * 
		    c__[i__4].i + v3.i * c__[i__4].r;
	    z__2.r = z__3.r + z__6.r, z__2.i = z__3.i + z__6.i;
	    i__5 = j + (c_dim1 << 2);
	    z__7.r = v4.r * c__[i__5].r - v4.i * c__[i__5].i, z__7.i = v4.r * 
		    c__[i__5].i + v4.i * c__[i__5].r;
	    z__1.r = z__2.r + z__7.r, z__1.i = z__2.i + z__7.i;
	    sum.r = z__1.r, sum.i = z__1.i;
	    i__2 = j + c_dim1;
	    i__3 = j + c_dim1;
	    z__2.r = sum.r * t1.r - sum.i * t1.i, z__2.i = sum.r * t1.i + 
		    sum.i * t1.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + (c_dim1 << 1);
	    i__3 = j + (c_dim1 << 1);
	    z__2.r = sum.r * t2.r - sum.i * t2.i, z__2.i = sum.r * t2.i + 
		    sum.i * t2.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + c_dim1 * 3;
	    i__3 = j + c_dim1 * 3;
	    z__2.r = sum.r * t3.r - sum.i * t3.i, z__2.i = sum.r * t3.i + 
		    sum.i * t3.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + (c_dim1 << 2);
	    i__3 = j + (c_dim1 << 2);
	    z__2.r = sum.r * t4.r - sum.i * t4.i, z__2.i = sum.r * t4.i + 
		    sum.i * t4.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
/* L280: */
	}
	goto L410;
L290:

/*        Special code for 5 x 5 Householder */

	v1.r = v[1].r, v1.i = v[1].i;
	d_cnjg(&z__2, &v1);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t1.r = z__1.r, t1.i = z__1.i;
	v2.r = v[2].r, v2.i = v[2].i;
	d_cnjg(&z__2, &v2);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t2.r = z__1.r, t2.i = z__1.i;
	v3.r = v[3].r, v3.i = v[3].i;
	d_cnjg(&z__2, &v3);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t3.r = z__1.r, t3.i = z__1.i;
	v4.r = v[4].r, v4.i = v[4].i;
	d_cnjg(&z__2, &v4);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t4.r = z__1.r, t4.i = z__1.i;
	v5.r = v[5].r, v5.i = v[5].i;
	d_cnjg(&z__2, &v5);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t5.r = z__1.r, t5.i = z__1.i;
	i__1 = *m;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = j + c_dim1;
	    z__5.r = v1.r * c__[i__2].r - v1.i * c__[i__2].i, z__5.i = v1.r * 
		    c__[i__2].i + v1.i * c__[i__2].r;
	    i__3 = j + (c_dim1 << 1);
	    z__6.r = v2.r * c__[i__3].r - v2.i * c__[i__3].i, z__6.i = v2.r * 
		    c__[i__3].i + v2.i * c__[i__3].r;
	    z__4.r = z__5.r + z__6.r, z__4.i = z__5.i + z__6.i;
	    i__4 = j + c_dim1 * 3;
	    z__7.r = v3.r * c__[i__4].r - v3.i * c__[i__4].i, z__7.i = v3.r * 
		    c__[i__4].i + v3.i * c__[i__4].r;
	    z__3.r = z__4.r + z__7.r, z__3.i = z__4.i + z__7.i;
	    i__5 = j + (c_dim1 << 2);
	    z__8.r = v4.r * c__[i__5].r - v4.i * c__[i__5].i, z__8.i = v4.r * 
		    c__[i__5].i + v4.i * c__[i__5].r;
	    z__2.r = z__3.r + z__8.r, z__2.i = z__3.i + z__8.i;
	    i__6 = j + c_dim1 * 5;
	    z__9.r = v5.r * c__[i__6].r - v5.i * c__[i__6].i, z__9.i = v5.r * 
		    c__[i__6].i + v5.i * c__[i__6].r;
	    z__1.r = z__2.r + z__9.r, z__1.i = z__2.i + z__9.i;
	    sum.r = z__1.r, sum.i = z__1.i;
	    i__2 = j + c_dim1;
	    i__3 = j + c_dim1;
	    z__2.r = sum.r * t1.r - sum.i * t1.i, z__2.i = sum.r * t1.i + 
		    sum.i * t1.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + (c_dim1 << 1);
	    i__3 = j + (c_dim1 << 1);
	    z__2.r = sum.r * t2.r - sum.i * t2.i, z__2.i = sum.r * t2.i + 
		    sum.i * t2.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + c_dim1 * 3;
	    i__3 = j + c_dim1 * 3;
	    z__2.r = sum.r * t3.r - sum.i * t3.i, z__2.i = sum.r * t3.i + 
		    sum.i * t3.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + (c_dim1 << 2);
	    i__3 = j + (c_dim1 << 2);
	    z__2.r = sum.r * t4.r - sum.i * t4.i, z__2.i = sum.r * t4.i + 
		    sum.i * t4.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + c_dim1 * 5;
	    i__3 = j + c_dim1 * 5;
	    z__2.r = sum.r * t5.r - sum.i * t5.i, z__2.i = sum.r * t5.i + 
		    sum.i * t5.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
/* L300: */
	}
	goto L410;
L310:

/*        Special code for 6 x 6 Householder */

	v1.r = v[1].r, v1.i = v[1].i;
	d_cnjg(&z__2, &v1);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t1.r = z__1.r, t1.i = z__1.i;
	v2.r = v[2].r, v2.i = v[2].i;
	d_cnjg(&z__2, &v2);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t2.r = z__1.r, t2.i = z__1.i;
	v3.r = v[3].r, v3.i = v[3].i;
	d_cnjg(&z__2, &v3);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t3.r = z__1.r, t3.i = z__1.i;
	v4.r = v[4].r, v4.i = v[4].i;
	d_cnjg(&z__2, &v4);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t4.r = z__1.r, t4.i = z__1.i;
	v5.r = v[5].r, v5.i = v[5].i;
	d_cnjg(&z__2, &v5);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t5.r = z__1.r, t5.i = z__1.i;
	v6.r = v[6].r, v6.i = v[6].i;
	d_cnjg(&z__2, &v6);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t6.r = z__1.r, t6.i = z__1.i;
	i__1 = *m;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = j + c_dim1;
	    z__6.r = v1.r * c__[i__2].r - v1.i * c__[i__2].i, z__6.i = v1.r * 
		    c__[i__2].i + v1.i * c__[i__2].r;
	    i__3 = j + (c_dim1 << 1);
	    z__7.r = v2.r * c__[i__3].r - v2.i * c__[i__3].i, z__7.i = v2.r * 
		    c__[i__3].i + v2.i * c__[i__3].r;
	    z__5.r = z__6.r + z__7.r, z__5.i = z__6.i + z__7.i;
	    i__4 = j + c_dim1 * 3;
	    z__8.r = v3.r * c__[i__4].r - v3.i * c__[i__4].i, z__8.i = v3.r * 
		    c__[i__4].i + v3.i * c__[i__4].r;
	    z__4.r = z__5.r + z__8.r, z__4.i = z__5.i + z__8.i;
	    i__5 = j + (c_dim1 << 2);
	    z__9.r = v4.r * c__[i__5].r - v4.i * c__[i__5].i, z__9.i = v4.r * 
		    c__[i__5].i + v4.i * c__[i__5].r;
	    z__3.r = z__4.r + z__9.r, z__3.i = z__4.i + z__9.i;
	    i__6 = j + c_dim1 * 5;
	    z__10.r = v5.r * c__[i__6].r - v5.i * c__[i__6].i, z__10.i = v5.r 
		    * c__[i__6].i + v5.i * c__[i__6].r;
	    z__2.r = z__3.r + z__10.r, z__2.i = z__3.i + z__10.i;
	    i__7 = j + c_dim1 * 6;
	    z__11.r = v6.r * c__[i__7].r - v6.i * c__[i__7].i, z__11.i = v6.r 
		    * c__[i__7].i + v6.i * c__[i__7].r;
	    z__1.r = z__2.r + z__11.r, z__1.i = z__2.i + z__11.i;
	    sum.r = z__1.r, sum.i = z__1.i;
	    i__2 = j + c_dim1;
	    i__3 = j + c_dim1;
	    z__2.r = sum.r * t1.r - sum.i * t1.i, z__2.i = sum.r * t1.i + 
		    sum.i * t1.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + (c_dim1 << 1);
	    i__3 = j + (c_dim1 << 1);
	    z__2.r = sum.r * t2.r - sum.i * t2.i, z__2.i = sum.r * t2.i + 
		    sum.i * t2.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + c_dim1 * 3;
	    i__3 = j + c_dim1 * 3;
	    z__2.r = sum.r * t3.r - sum.i * t3.i, z__2.i = sum.r * t3.i + 
		    sum.i * t3.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + (c_dim1 << 2);
	    i__3 = j + (c_dim1 << 2);
	    z__2.r = sum.r * t4.r - sum.i * t4.i, z__2.i = sum.r * t4.i + 
		    sum.i * t4.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + c_dim1 * 5;
	    i__3 = j + c_dim1 * 5;
	    z__2.r = sum.r * t5.r - sum.i * t5.i, z__2.i = sum.r * t5.i + 
		    sum.i * t5.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + c_dim1 * 6;
	    i__3 = j + c_dim1 * 6;
	    z__2.r = sum.r * t6.r - sum.i * t6.i, z__2.i = sum.r * t6.i + 
		    sum.i * t6.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
/* L320: */
	}
	goto L410;
L330:

/*        Special code for 7 x 7 Householder */

	v1.r = v[1].r, v1.i = v[1].i;
	d_cnjg(&z__2, &v1);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t1.r = z__1.r, t1.i = z__1.i;
	v2.r = v[2].r, v2.i = v[2].i;
	d_cnjg(&z__2, &v2);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t2.r = z__1.r, t2.i = z__1.i;
	v3.r = v[3].r, v3.i = v[3].i;
	d_cnjg(&z__2, &v3);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t3.r = z__1.r, t3.i = z__1.i;
	v4.r = v[4].r, v4.i = v[4].i;
	d_cnjg(&z__2, &v4);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t4.r = z__1.r, t4.i = z__1.i;
	v5.r = v[5].r, v5.i = v[5].i;
	d_cnjg(&z__2, &v5);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t5.r = z__1.r, t5.i = z__1.i;
	v6.r = v[6].r, v6.i = v[6].i;
	d_cnjg(&z__2, &v6);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t6.r = z__1.r, t6.i = z__1.i;
	v7.r = v[7].r, v7.i = v[7].i;
	d_cnjg(&z__2, &v7);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t7.r = z__1.r, t7.i = z__1.i;
	i__1 = *m;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = j + c_dim1;
	    z__7.r = v1.r * c__[i__2].r - v1.i * c__[i__2].i, z__7.i = v1.r * 
		    c__[i__2].i + v1.i * c__[i__2].r;
	    i__3 = j + (c_dim1 << 1);
	    z__8.r = v2.r * c__[i__3].r - v2.i * c__[i__3].i, z__8.i = v2.r * 
		    c__[i__3].i + v2.i * c__[i__3].r;
	    z__6.r = z__7.r + z__8.r, z__6.i = z__7.i + z__8.i;
	    i__4 = j + c_dim1 * 3;
	    z__9.r = v3.r * c__[i__4].r - v3.i * c__[i__4].i, z__9.i = v3.r * 
		    c__[i__4].i + v3.i * c__[i__4].r;
	    z__5.r = z__6.r + z__9.r, z__5.i = z__6.i + z__9.i;
	    i__5 = j + (c_dim1 << 2);
	    z__10.r = v4.r * c__[i__5].r - v4.i * c__[i__5].i, z__10.i = v4.r 
		    * c__[i__5].i + v4.i * c__[i__5].r;
	    z__4.r = z__5.r + z__10.r, z__4.i = z__5.i + z__10.i;
	    i__6 = j + c_dim1 * 5;
	    z__11.r = v5.r * c__[i__6].r - v5.i * c__[i__6].i, z__11.i = v5.r 
		    * c__[i__6].i + v5.i * c__[i__6].r;
	    z__3.r = z__4.r + z__11.r, z__3.i = z__4.i + z__11.i;
	    i__7 = j + c_dim1 * 6;
	    z__12.r = v6.r * c__[i__7].r - v6.i * c__[i__7].i, z__12.i = v6.r 
		    * c__[i__7].i + v6.i * c__[i__7].r;
	    z__2.r = z__3.r + z__12.r, z__2.i = z__3.i + z__12.i;
	    i__8 = j + c_dim1 * 7;
	    z__13.r = v7.r * c__[i__8].r - v7.i * c__[i__8].i, z__13.i = v7.r 
		    * c__[i__8].i + v7.i * c__[i__8].r;
	    z__1.r = z__2.r + z__13.r, z__1.i = z__2.i + z__13.i;
	    sum.r = z__1.r, sum.i = z__1.i;
	    i__2 = j + c_dim1;
	    i__3 = j + c_dim1;
	    z__2.r = sum.r * t1.r - sum.i * t1.i, z__2.i = sum.r * t1.i + 
		    sum.i * t1.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + (c_dim1 << 1);
	    i__3 = j + (c_dim1 << 1);
	    z__2.r = sum.r * t2.r - sum.i * t2.i, z__2.i = sum.r * t2.i + 
		    sum.i * t2.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + c_dim1 * 3;
	    i__3 = j + c_dim1 * 3;
	    z__2.r = sum.r * t3.r - sum.i * t3.i, z__2.i = sum.r * t3.i + 
		    sum.i * t3.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + (c_dim1 << 2);
	    i__3 = j + (c_dim1 << 2);
	    z__2.r = sum.r * t4.r - sum.i * t4.i, z__2.i = sum.r * t4.i + 
		    sum.i * t4.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + c_dim1 * 5;
	    i__3 = j + c_dim1 * 5;
	    z__2.r = sum.r * t5.r - sum.i * t5.i, z__2.i = sum.r * t5.i + 
		    sum.i * t5.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + c_dim1 * 6;
	    i__3 = j + c_dim1 * 6;
	    z__2.r = sum.r * t6.r - sum.i * t6.i, z__2.i = sum.r * t6.i + 
		    sum.i * t6.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + c_dim1 * 7;
	    i__3 = j + c_dim1 * 7;
	    z__2.r = sum.r * t7.r - sum.i * t7.i, z__2.i = sum.r * t7.i + 
		    sum.i * t7.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
/* L340: */
	}
	goto L410;
L350:

/*        Special code for 8 x 8 Householder */

	v1.r = v[1].r, v1.i = v[1].i;
	d_cnjg(&z__2, &v1);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t1.r = z__1.r, t1.i = z__1.i;
	v2.r = v[2].r, v2.i = v[2].i;
	d_cnjg(&z__2, &v2);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t2.r = z__1.r, t2.i = z__1.i;
	v3.r = v[3].r, v3.i = v[3].i;
	d_cnjg(&z__2, &v3);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t3.r = z__1.r, t3.i = z__1.i;
	v4.r = v[4].r, v4.i = v[4].i;
	d_cnjg(&z__2, &v4);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t4.r = z__1.r, t4.i = z__1.i;
	v5.r = v[5].r, v5.i = v[5].i;
	d_cnjg(&z__2, &v5);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t5.r = z__1.r, t5.i = z__1.i;
	v6.r = v[6].r, v6.i = v[6].i;
	d_cnjg(&z__2, &v6);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t6.r = z__1.r, t6.i = z__1.i;
	v7.r = v[7].r, v7.i = v[7].i;
	d_cnjg(&z__2, &v7);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t7.r = z__1.r, t7.i = z__1.i;
	v8.r = v[8].r, v8.i = v[8].i;
	d_cnjg(&z__2, &v8);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t8.r = z__1.r, t8.i = z__1.i;
	i__1 = *m;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = j + c_dim1;
	    z__8.r = v1.r * c__[i__2].r - v1.i * c__[i__2].i, z__8.i = v1.r * 
		    c__[i__2].i + v1.i * c__[i__2].r;
	    i__3 = j + (c_dim1 << 1);
	    z__9.r = v2.r * c__[i__3].r - v2.i * c__[i__3].i, z__9.i = v2.r * 
		    c__[i__3].i + v2.i * c__[i__3].r;
	    z__7.r = z__8.r + z__9.r, z__7.i = z__8.i + z__9.i;
	    i__4 = j + c_dim1 * 3;
	    z__10.r = v3.r * c__[i__4].r - v3.i * c__[i__4].i, z__10.i = v3.r 
		    * c__[i__4].i + v3.i * c__[i__4].r;
	    z__6.r = z__7.r + z__10.r, z__6.i = z__7.i + z__10.i;
	    i__5 = j + (c_dim1 << 2);
	    z__11.r = v4.r * c__[i__5].r - v4.i * c__[i__5].i, z__11.i = v4.r 
		    * c__[i__5].i + v4.i * c__[i__5].r;
	    z__5.r = z__6.r + z__11.r, z__5.i = z__6.i + z__11.i;
	    i__6 = j + c_dim1 * 5;
	    z__12.r = v5.r * c__[i__6].r - v5.i * c__[i__6].i, z__12.i = v5.r 
		    * c__[i__6].i + v5.i * c__[i__6].r;
	    z__4.r = z__5.r + z__12.r, z__4.i = z__5.i + z__12.i;
	    i__7 = j + c_dim1 * 6;
	    z__13.r = v6.r * c__[i__7].r - v6.i * c__[i__7].i, z__13.i = v6.r 
		    * c__[i__7].i + v6.i * c__[i__7].r;
	    z__3.r = z__4.r + z__13.r, z__3.i = z__4.i + z__13.i;
	    i__8 = j + c_dim1 * 7;
	    z__14.r = v7.r * c__[i__8].r - v7.i * c__[i__8].i, z__14.i = v7.r 
		    * c__[i__8].i + v7.i * c__[i__8].r;
	    z__2.r = z__3.r + z__14.r, z__2.i = z__3.i + z__14.i;
	    i__9 = j + (c_dim1 << 3);
	    z__15.r = v8.r * c__[i__9].r - v8.i * c__[i__9].i, z__15.i = v8.r 
		    * c__[i__9].i + v8.i * c__[i__9].r;
	    z__1.r = z__2.r + z__15.r, z__1.i = z__2.i + z__15.i;
	    sum.r = z__1.r, sum.i = z__1.i;
	    i__2 = j + c_dim1;
	    i__3 = j + c_dim1;
	    z__2.r = sum.r * t1.r - sum.i * t1.i, z__2.i = sum.r * t1.i + 
		    sum.i * t1.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + (c_dim1 << 1);
	    i__3 = j + (c_dim1 << 1);
	    z__2.r = sum.r * t2.r - sum.i * t2.i, z__2.i = sum.r * t2.i + 
		    sum.i * t2.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + c_dim1 * 3;
	    i__3 = j + c_dim1 * 3;
	    z__2.r = sum.r * t3.r - sum.i * t3.i, z__2.i = sum.r * t3.i + 
		    sum.i * t3.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + (c_dim1 << 2);
	    i__3 = j + (c_dim1 << 2);
	    z__2.r = sum.r * t4.r - sum.i * t4.i, z__2.i = sum.r * t4.i + 
		    sum.i * t4.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + c_dim1 * 5;
	    i__3 = j + c_dim1 * 5;
	    z__2.r = sum.r * t5.r - sum.i * t5.i, z__2.i = sum.r * t5.i + 
		    sum.i * t5.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + c_dim1 * 6;
	    i__3 = j + c_dim1 * 6;
	    z__2.r = sum.r * t6.r - sum.i * t6.i, z__2.i = sum.r * t6.i + 
		    sum.i * t6.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + c_dim1 * 7;
	    i__3 = j + c_dim1 * 7;
	    z__2.r = sum.r * t7.r - sum.i * t7.i, z__2.i = sum.r * t7.i + 
		    sum.i * t7.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + (c_dim1 << 3);
	    i__3 = j + (c_dim1 << 3);
	    z__2.r = sum.r * t8.r - sum.i * t8.i, z__2.i = sum.r * t8.i + 
		    sum.i * t8.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
/* L360: */
	}
	goto L410;
L370:

/*        Special code for 9 x 9 Householder */

	v1.r = v[1].r, v1.i = v[1].i;
	d_cnjg(&z__2, &v1);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t1.r = z__1.r, t1.i = z__1.i;
	v2.r = v[2].r, v2.i = v[2].i;
	d_cnjg(&z__2, &v2);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t2.r = z__1.r, t2.i = z__1.i;
	v3.r = v[3].r, v3.i = v[3].i;
	d_cnjg(&z__2, &v3);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t3.r = z__1.r, t3.i = z__1.i;
	v4.r = v[4].r, v4.i = v[4].i;
	d_cnjg(&z__2, &v4);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t4.r = z__1.r, t4.i = z__1.i;
	v5.r = v[5].r, v5.i = v[5].i;
	d_cnjg(&z__2, &v5);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t5.r = z__1.r, t5.i = z__1.i;
	v6.r = v[6].r, v6.i = v[6].i;
	d_cnjg(&z__2, &v6);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t6.r = z__1.r, t6.i = z__1.i;
	v7.r = v[7].r, v7.i = v[7].i;
	d_cnjg(&z__2, &v7);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t7.r = z__1.r, t7.i = z__1.i;
	v8.r = v[8].r, v8.i = v[8].i;
	d_cnjg(&z__2, &v8);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t8.r = z__1.r, t8.i = z__1.i;
	v9.r = v[9].r, v9.i = v[9].i;
	d_cnjg(&z__2, &v9);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t9.r = z__1.r, t9.i = z__1.i;
	i__1 = *m;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = j + c_dim1;
	    z__9.r = v1.r * c__[i__2].r - v1.i * c__[i__2].i, z__9.i = v1.r * 
		    c__[i__2].i + v1.i * c__[i__2].r;
	    i__3 = j + (c_dim1 << 1);
	    z__10.r = v2.r * c__[i__3].r - v2.i * c__[i__3].i, z__10.i = v2.r 
		    * c__[i__3].i + v2.i * c__[i__3].r;
	    z__8.r = z__9.r + z__10.r, z__8.i = z__9.i + z__10.i;
	    i__4 = j + c_dim1 * 3;
	    z__11.r = v3.r * c__[i__4].r - v3.i * c__[i__4].i, z__11.i = v3.r 
		    * c__[i__4].i + v3.i * c__[i__4].r;
	    z__7.r = z__8.r + z__11.r, z__7.i = z__8.i + z__11.i;
	    i__5 = j + (c_dim1 << 2);
	    z__12.r = v4.r * c__[i__5].r - v4.i * c__[i__5].i, z__12.i = v4.r 
		    * c__[i__5].i + v4.i * c__[i__5].r;
	    z__6.r = z__7.r + z__12.r, z__6.i = z__7.i + z__12.i;
	    i__6 = j + c_dim1 * 5;
	    z__13.r = v5.r * c__[i__6].r - v5.i * c__[i__6].i, z__13.i = v5.r 
		    * c__[i__6].i + v5.i * c__[i__6].r;
	    z__5.r = z__6.r + z__13.r, z__5.i = z__6.i + z__13.i;
	    i__7 = j + c_dim1 * 6;
	    z__14.r = v6.r * c__[i__7].r - v6.i * c__[i__7].i, z__14.i = v6.r 
		    * c__[i__7].i + v6.i * c__[i__7].r;
	    z__4.r = z__5.r + z__14.r, z__4.i = z__5.i + z__14.i;
	    i__8 = j + c_dim1 * 7;
	    z__15.r = v7.r * c__[i__8].r - v7.i * c__[i__8].i, z__15.i = v7.r 
		    * c__[i__8].i + v7.i * c__[i__8].r;
	    z__3.r = z__4.r + z__15.r, z__3.i = z__4.i + z__15.i;
	    i__9 = j + (c_dim1 << 3);
	    z__16.r = v8.r * c__[i__9].r - v8.i * c__[i__9].i, z__16.i = v8.r 
		    * c__[i__9].i + v8.i * c__[i__9].r;
	    z__2.r = z__3.r + z__16.r, z__2.i = z__3.i + z__16.i;
	    i__10 = j + c_dim1 * 9;
	    z__17.r = v9.r * c__[i__10].r - v9.i * c__[i__10].i, z__17.i = 
		    v9.r * c__[i__10].i + v9.i * c__[i__10].r;
	    z__1.r = z__2.r + z__17.r, z__1.i = z__2.i + z__17.i;
	    sum.r = z__1.r, sum.i = z__1.i;
	    i__2 = j + c_dim1;
	    i__3 = j + c_dim1;
	    z__2.r = sum.r * t1.r - sum.i * t1.i, z__2.i = sum.r * t1.i + 
		    sum.i * t1.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + (c_dim1 << 1);
	    i__3 = j + (c_dim1 << 1);
	    z__2.r = sum.r * t2.r - sum.i * t2.i, z__2.i = sum.r * t2.i + 
		    sum.i * t2.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + c_dim1 * 3;
	    i__3 = j + c_dim1 * 3;
	    z__2.r = sum.r * t3.r - sum.i * t3.i, z__2.i = sum.r * t3.i + 
		    sum.i * t3.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + (c_dim1 << 2);
	    i__3 = j + (c_dim1 << 2);
	    z__2.r = sum.r * t4.r - sum.i * t4.i, z__2.i = sum.r * t4.i + 
		    sum.i * t4.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + c_dim1 * 5;
	    i__3 = j + c_dim1 * 5;
	    z__2.r = sum.r * t5.r - sum.i * t5.i, z__2.i = sum.r * t5.i + 
		    sum.i * t5.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + c_dim1 * 6;
	    i__3 = j + c_dim1 * 6;
	    z__2.r = sum.r * t6.r - sum.i * t6.i, z__2.i = sum.r * t6.i + 
		    sum.i * t6.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + c_dim1 * 7;
	    i__3 = j + c_dim1 * 7;
	    z__2.r = sum.r * t7.r - sum.i * t7.i, z__2.i = sum.r * t7.i + 
		    sum.i * t7.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + (c_dim1 << 3);
	    i__3 = j + (c_dim1 << 3);
	    z__2.r = sum.r * t8.r - sum.i * t8.i, z__2.i = sum.r * t8.i + 
		    sum.i * t8.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + c_dim1 * 9;
	    i__3 = j + c_dim1 * 9;
	    z__2.r = sum.r * t9.r - sum.i * t9.i, z__2.i = sum.r * t9.i + 
		    sum.i * t9.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
/* L380: */
	}
	goto L410;
L390:

/*        Special code for 10 x 10 Householder */

	v1.r = v[1].r, v1.i = v[1].i;
	d_cnjg(&z__2, &v1);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t1.r = z__1.r, t1.i = z__1.i;
	v2.r = v[2].r, v2.i = v[2].i;
	d_cnjg(&z__2, &v2);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t2.r = z__1.r, t2.i = z__1.i;
	v3.r = v[3].r, v3.i = v[3].i;
	d_cnjg(&z__2, &v3);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t3.r = z__1.r, t3.i = z__1.i;
	v4.r = v[4].r, v4.i = v[4].i;
	d_cnjg(&z__2, &v4);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t4.r = z__1.r, t4.i = z__1.i;
	v5.r = v[5].r, v5.i = v[5].i;
	d_cnjg(&z__2, &v5);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t5.r = z__1.r, t5.i = z__1.i;
	v6.r = v[6].r, v6.i = v[6].i;
	d_cnjg(&z__2, &v6);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t6.r = z__1.r, t6.i = z__1.i;
	v7.r = v[7].r, v7.i = v[7].i;
	d_cnjg(&z__2, &v7);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t7.r = z__1.r, t7.i = z__1.i;
	v8.r = v[8].r, v8.i = v[8].i;
	d_cnjg(&z__2, &v8);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t8.r = z__1.r, t8.i = z__1.i;
	v9.r = v[9].r, v9.i = v[9].i;
	d_cnjg(&z__2, &v9);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t9.r = z__1.r, t9.i = z__1.i;
	v10.r = v[10].r, v10.i = v[10].i;
	d_cnjg(&z__2, &v10);
	z__1.r = tau->r * z__2.r - tau->i * z__2.i, z__1.i = tau->r * z__2.i 
		+ tau->i * z__2.r;
	t10.r = z__1.r, t10.i = z__1.i;
	i__1 = *m;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = j + c_dim1;
	    z__10.r = v1.r * c__[i__2].r - v1.i * c__[i__2].i, z__10.i = v1.r 
		    * c__[i__2].i + v1.i * c__[i__2].r;
	    i__3 = j + (c_dim1 << 1);
	    z__11.r = v2.r * c__[i__3].r - v2.i * c__[i__3].i, z__11.i = v2.r 
		    * c__[i__3].i + v2.i * c__[i__3].r;
	    z__9.r = z__10.r + z__11.r, z__9.i = z__10.i + z__11.i;
	    i__4 = j + c_dim1 * 3;
	    z__12.r = v3.r * c__[i__4].r - v3.i * c__[i__4].i, z__12.i = v3.r 
		    * c__[i__4].i + v3.i * c__[i__4].r;
	    z__8.r = z__9.r + z__12.r, z__8.i = z__9.i + z__12.i;
	    i__5 = j + (c_dim1 << 2);
	    z__13.r = v4.r * c__[i__5].r - v4.i * c__[i__5].i, z__13.i = v4.r 
		    * c__[i__5].i + v4.i * c__[i__5].r;
	    z__7.r = z__8.r + z__13.r, z__7.i = z__8.i + z__13.i;
	    i__6 = j + c_dim1 * 5;
	    z__14.r = v5.r * c__[i__6].r - v5.i * c__[i__6].i, z__14.i = v5.r 
		    * c__[i__6].i + v5.i * c__[i__6].r;
	    z__6.r = z__7.r + z__14.r, z__6.i = z__7.i + z__14.i;
	    i__7 = j + c_dim1 * 6;
	    z__15.r = v6.r * c__[i__7].r - v6.i * c__[i__7].i, z__15.i = v6.r 
		    * c__[i__7].i + v6.i * c__[i__7].r;
	    z__5.r = z__6.r + z__15.r, z__5.i = z__6.i + z__15.i;
	    i__8 = j + c_dim1 * 7;
	    z__16.r = v7.r * c__[i__8].r - v7.i * c__[i__8].i, z__16.i = v7.r 
		    * c__[i__8].i + v7.i * c__[i__8].r;
	    z__4.r = z__5.r + z__16.r, z__4.i = z__5.i + z__16.i;
	    i__9 = j + (c_dim1 << 3);
	    z__17.r = v8.r * c__[i__9].r - v8.i * c__[i__9].i, z__17.i = v8.r 
		    * c__[i__9].i + v8.i * c__[i__9].r;
	    z__3.r = z__4.r + z__17.r, z__3.i = z__4.i + z__17.i;
	    i__10 = j + c_dim1 * 9;
	    z__18.r = v9.r * c__[i__10].r - v9.i * c__[i__10].i, z__18.i = 
		    v9.r * c__[i__10].i + v9.i * c__[i__10].r;
	    z__2.r = z__3.r + z__18.r, z__2.i = z__3.i + z__18.i;
	    i__11 = j + c_dim1 * 10;
	    z__19.r = v10.r * c__[i__11].r - v10.i * c__[i__11].i, z__19.i = 
		    v10.r * c__[i__11].i + v10.i * c__[i__11].r;
	    z__1.r = z__2.r + z__19.r, z__1.i = z__2.i + z__19.i;
	    sum.r = z__1.r, sum.i = z__1.i;
	    i__2 = j + c_dim1;
	    i__3 = j + c_dim1;
	    z__2.r = sum.r * t1.r - sum.i * t1.i, z__2.i = sum.r * t1.i + 
		    sum.i * t1.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + (c_dim1 << 1);
	    i__3 = j + (c_dim1 << 1);
	    z__2.r = sum.r * t2.r - sum.i * t2.i, z__2.i = sum.r * t2.i + 
		    sum.i * t2.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + c_dim1 * 3;
	    i__3 = j + c_dim1 * 3;
	    z__2.r = sum.r * t3.r - sum.i * t3.i, z__2.i = sum.r * t3.i + 
		    sum.i * t3.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + (c_dim1 << 2);
	    i__3 = j + (c_dim1 << 2);
	    z__2.r = sum.r * t4.r - sum.i * t4.i, z__2.i = sum.r * t4.i + 
		    sum.i * t4.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + c_dim1 * 5;
	    i__3 = j + c_dim1 * 5;
	    z__2.r = sum.r * t5.r - sum.i * t5.i, z__2.i = sum.r * t5.i + 
		    sum.i * t5.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + c_dim1 * 6;
	    i__3 = j + c_dim1 * 6;
	    z__2.r = sum.r * t6.r - sum.i * t6.i, z__2.i = sum.r * t6.i + 
		    sum.i * t6.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + c_dim1 * 7;
	    i__3 = j + c_dim1 * 7;
	    z__2.r = sum.r * t7.r - sum.i * t7.i, z__2.i = sum.r * t7.i + 
		    sum.i * t7.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + (c_dim1 << 3);
	    i__3 = j + (c_dim1 << 3);
	    z__2.r = sum.r * t8.r - sum.i * t8.i, z__2.i = sum.r * t8.i + 
		    sum.i * t8.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + c_dim1 * 9;
	    i__3 = j + c_dim1 * 9;
	    z__2.r = sum.r * t9.r - sum.i * t9.i, z__2.i = sum.r * t9.i + 
		    sum.i * t9.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
	    i__2 = j + c_dim1 * 10;
	    i__3 = j + c_dim1 * 10;
	    z__2.r = sum.r * t10.r - sum.i * t10.i, z__2.i = sum.r * t10.i + 
		    sum.i * t10.r;
	    z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
	    c__[i__2].r = z__1.r, c__[i__2].i = z__1.i;
/* L400: */
	}
	goto L410;
    }
L410:
    return;

/*     End of ZLARFX */

} /* zlarfx_ */

