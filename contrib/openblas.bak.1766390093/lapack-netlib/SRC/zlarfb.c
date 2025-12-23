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

static doublecomplex c_b1 = {1.,0.};
static integer c__1 = 1;

/* > \brief \b ZLARFB applies a block reflector or its conjugate-transpose to a general rectangular matrix. */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download ZLARFB + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zlarfb.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zlarfb.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zlarfb.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE ZLARFB( SIDE, TRANS, DIRECT, STOREV, M, N, K, V, LDV, */
/*                          T, LDT, C, LDC, WORK, LDWORK ) */

/*       CHARACTER          DIRECT, SIDE, STOREV, TRANS */
/*       INTEGER            K, LDC, LDT, LDV, LDWORK, M, N */
/*       COMPLEX*16         C( LDC, * ), T( LDT, * ), V( LDV, * ), */
/*      $                   WORK( LDWORK, * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > ZLARFB applies a complex block reflector H or its transpose H**H to a */
/* > complex M-by-N matrix C, from either the left or the right. */
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
/* >          = 'C': Columnwise */
/* >          = 'R': Rowwise */
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
/* > \param[in] K */
/* > \verbatim */
/* >          K is INTEGER */
/* >          The order of the matrix T (= the number of elementary */
/* >          reflectors whose product defines the block reflector). */
/* >          If SIDE = 'L', M >= K >= 0; */
/* >          if SIDE = 'R', N >= K >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] V */
/* > \verbatim */
/* >          V is COMPLEX*16 array, dimension */
/* >                                (LDV,K) if STOREV = 'C' */
/* >                                (LDV,M) if STOREV = 'R' and SIDE = 'L' */
/* >                                (LDV,N) if STOREV = 'R' and SIDE = 'R' */
/* >          See Further Details. */
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
/* >          The leading dimension of the array T. LDT >= K. */
/* > \endverbatim */
/* > */
/* > \param[in,out] C */
/* > \verbatim */
/* >          C is COMPLEX*16 array, dimension (LDC,N) */
/* >          On entry, the M-by-N matrix C. */
/* >          On exit, C is overwritten by H*C or H**H*C or C*H or C*H**H. */
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
/* >          WORK is COMPLEX*16 array, dimension (LDWORK,K) */
/* > \endverbatim */
/* > */
/* > \param[in] LDWORK */
/* > \verbatim */
/* >          LDWORK is INTEGER */
/* >          The leading dimension of the array WORK. */
/* >          If SIDE = 'L', LDWORK >= f2cmax(1,N); */
/* >          if SIDE = 'R', LDWORK >= f2cmax(1,M). */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date June 2013 */

/* > \ingroup complex16OTHERauxiliary */

/* > \par Further Details: */
/*  ===================== */
/* > */
/* > \verbatim */
/* > */
/* >  The shape of the matrix V and the storage of the vectors which define */
/* >  the H(i) is best illustrated by the following example with n = 5 and */
/* >  k = 3. The elements equal to 1 are not stored; the corresponding */
/* >  array elements are modified but restored on exit. The rest of the */
/* >  array is not used. */
/* > */
/* >  DIRECT = 'F' and STOREV = 'C':         DIRECT = 'F' and STOREV = 'R': */
/* > */
/* >               V = (  1       )                 V = (  1 v1 v1 v1 v1 ) */
/* >                   ( v1  1    )                     (     1 v2 v2 v2 ) */
/* >                   ( v1 v2  1 )                     (        1 v3 v3 ) */
/* >                   ( v1 v2 v3 ) */
/* >                   ( v1 v2 v3 ) */
/* > */
/* >  DIRECT = 'B' and STOREV = 'C':         DIRECT = 'B' and STOREV = 'R': */
/* > */
/* >               V = ( v1 v2 v3 )                 V = ( v1 v1  1       ) */
/* >                   ( v1 v2 v3 )                     ( v2 v2 v2  1    ) */
/* >                   (  1 v2 v3 )                     ( v3 v3 v3 v3  1 ) */
/* >                   (     1 v3 ) */
/* >                   (        1 ) */
/* > \endverbatim */
/* > */
/*  ===================================================================== */
/* Subroutine */ void zlarfb_(char *side, char *trans, char *direct, char *
	storev, integer *m, integer *n, integer *k, doublecomplex *v, integer 
	*ldv, doublecomplex *t, integer *ldt, doublecomplex *c__, integer *
	ldc, doublecomplex *work, integer *ldwork)
{
    /* System generated locals */
    integer c_dim1, c_offset, t_dim1, t_offset, v_dim1, v_offset, work_dim1, 
	    work_offset, i__1, i__2, i__3, i__4, i__5;
    doublecomplex z__1, z__2;

    /* Local variables */
    integer i__, j;
    extern logical lsame_(char *, char *);
    extern /* Subroutine */ void zgemm_(char *, char *, integer *, integer *, 
	    integer *, doublecomplex *, doublecomplex *, integer *, 
	    doublecomplex *, integer *, doublecomplex *, doublecomplex *, 
	    integer *), zcopy_(integer *, doublecomplex *, 
	    integer *, doublecomplex *, integer *), ztrmm_(char *, char *, 
	    char *, char *, integer *, integer *, doublecomplex *, 
	    doublecomplex *, integer *, doublecomplex *, integer *), zlacgv_(integer *, doublecomplex *, 
	    integer *);
    char transt[1];


/*  -- LAPACK auxiliary routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     June 2013 */


/*  ===================================================================== */


/*     Quick return if possible */

    /* Parameter adjustments */
    v_dim1 = *ldv;
    v_offset = 1 + v_dim1 * 1;
    v -= v_offset;
    t_dim1 = *ldt;
    t_offset = 1 + t_dim1 * 1;
    t -= t_offset;
    c_dim1 = *ldc;
    c_offset = 1 + c_dim1 * 1;
    c__ -= c_offset;
    work_dim1 = *ldwork;
    work_offset = 1 + work_dim1 * 1;
    work -= work_offset;

    /* Function Body */
    if (*m <= 0 || *n <= 0) {
	return;
    }

    if (lsame_(trans, "N")) {
	*(unsigned char *)transt = 'C';
    } else {
	*(unsigned char *)transt = 'N';
    }

    if (lsame_(storev, "C")) {

	if (lsame_(direct, "F")) {

/*           Let  V =  ( V1 )    (first K rows) */
/*                     ( V2 ) */
/*           where  V1  is unit lower triangular. */

	    if (lsame_(side, "L")) {

/*              Form  H * C  or  H**H * C  where  C = ( C1 ) */
/*                                                    ( C2 ) */

/*              W := C**H * V  =  (C1**H * V1 + C2**H * V2)  (stored in WORK) */

/*              W := C1**H */

		i__1 = *k;
		for (j = 1; j <= i__1; ++j) {
		    zcopy_(n, &c__[j + c_dim1], ldc, &work[j * work_dim1 + 1],
			     &c__1);
		    zlacgv_(n, &work[j * work_dim1 + 1], &c__1);
/* L10: */
		}

/*              W := W * V1 */

		ztrmm_("Right", "Lower", "No transpose", "Unit", n, k, &c_b1, 
			&v[v_offset], ldv, &work[work_offset], ldwork);
		if (*m > *k) {

/*                 W := W + C2**H * V2 */

		    i__1 = *m - *k;
		    zgemm_("Conjugate transpose", "No transpose", n, k, &i__1,
			     &c_b1, &c__[*k + 1 + c_dim1], ldc, &v[*k + 1 + 
			    v_dim1], ldv, &c_b1, &work[work_offset], ldwork);
		}

/*              W := W * T**H  or  W * T */

		ztrmm_("Right", "Upper", transt, "Non-unit", n, k, &c_b1, &t[
			t_offset], ldt, &work[work_offset], ldwork);

/*              C := C - V * W**H */

		if (*m > *k) {

/*                 C2 := C2 - V2 * W**H */

		    i__1 = *m - *k;
		    z__1.r = -1., z__1.i = 0.;
		    zgemm_("No transpose", "Conjugate transpose", &i__1, n, k,
			     &z__1, &v[*k + 1 + v_dim1], ldv, &work[
			    work_offset], ldwork, &c_b1, &c__[*k + 1 + c_dim1]
			    , ldc);
		}

/*              W := W * V1**H */

		ztrmm_("Right", "Lower", "Conjugate transpose", "Unit", n, k, 
			&c_b1, &v[v_offset], ldv, &work[work_offset], ldwork);

/*              C1 := C1 - W**H */

		i__1 = *k;
		for (j = 1; j <= i__1; ++j) {
		    i__2 = *n;
		    for (i__ = 1; i__ <= i__2; ++i__) {
			i__3 = j + i__ * c_dim1;
			i__4 = j + i__ * c_dim1;
			d_cnjg(&z__2, &work[i__ + j * work_dim1]);
			z__1.r = c__[i__4].r - z__2.r, z__1.i = c__[i__4].i - 
				z__2.i;
			c__[i__3].r = z__1.r, c__[i__3].i = z__1.i;
/* L20: */
		    }
/* L30: */
		}

	    } else if (lsame_(side, "R")) {

/*              Form  C * H  or  C * H**H  where  C = ( C1  C2 ) */

/*              W := C * V  =  (C1*V1 + C2*V2)  (stored in WORK) */

/*              W := C1 */

		i__1 = *k;
		for (j = 1; j <= i__1; ++j) {
		    zcopy_(m, &c__[j * c_dim1 + 1], &c__1, &work[j * 
			    work_dim1 + 1], &c__1);
/* L40: */
		}

/*              W := W * V1 */

		ztrmm_("Right", "Lower", "No transpose", "Unit", m, k, &c_b1, 
			&v[v_offset], ldv, &work[work_offset], ldwork);
		if (*n > *k) {

/*                 W := W + C2 * V2 */

		    i__1 = *n - *k;
		    zgemm_("No transpose", "No transpose", m, k, &i__1, &c_b1,
			     &c__[(*k + 1) * c_dim1 + 1], ldc, &v[*k + 1 + 
			    v_dim1], ldv, &c_b1, &work[work_offset], ldwork);
		}

/*              W := W * T  or  W * T**H */

		ztrmm_("Right", "Upper", trans, "Non-unit", m, k, &c_b1, &t[
			t_offset], ldt, &work[work_offset], ldwork);

/*              C := C - W * V**H */

		if (*n > *k) {

/*                 C2 := C2 - W * V2**H */

		    i__1 = *n - *k;
		    z__1.r = -1., z__1.i = 0.;
		    zgemm_("No transpose", "Conjugate transpose", m, &i__1, k,
			     &z__1, &work[work_offset], ldwork, &v[*k + 1 + 
			    v_dim1], ldv, &c_b1, &c__[(*k + 1) * c_dim1 + 1], 
			    ldc);
		}

/*              W := W * V1**H */

		ztrmm_("Right", "Lower", "Conjugate transpose", "Unit", m, k, 
			&c_b1, &v[v_offset], ldv, &work[work_offset], ldwork);

/*              C1 := C1 - W */

		i__1 = *k;
		for (j = 1; j <= i__1; ++j) {
		    i__2 = *m;
		    for (i__ = 1; i__ <= i__2; ++i__) {
			i__3 = i__ + j * c_dim1;
			i__4 = i__ + j * c_dim1;
			i__5 = i__ + j * work_dim1;
			z__1.r = c__[i__4].r - work[i__5].r, z__1.i = c__[
				i__4].i - work[i__5].i;
			c__[i__3].r = z__1.r, c__[i__3].i = z__1.i;
/* L50: */
		    }
/* L60: */
		}
	    }

	} else {

/*           Let  V =  ( V1 ) */
/*                     ( V2 )    (last K rows) */
/*           where  V2  is unit upper triangular. */

	    if (lsame_(side, "L")) {

/*              Form  H * C  or  H**H * C  where  C = ( C1 ) */
/*                                                    ( C2 ) */

/*              W := C**H * V  =  (C1**H * V1 + C2**H * V2)  (stored in WORK) */

/*              W := C2**H */

		i__1 = *k;
		for (j = 1; j <= i__1; ++j) {
		    zcopy_(n, &c__[*m - *k + j + c_dim1], ldc, &work[j * 
			    work_dim1 + 1], &c__1);
		    zlacgv_(n, &work[j * work_dim1 + 1], &c__1);
/* L70: */
		}

/*              W := W * V2 */

		ztrmm_("Right", "Upper", "No transpose", "Unit", n, k, &c_b1, 
			&v[*m - *k + 1 + v_dim1], ldv, &work[work_offset], 
			ldwork);
		if (*m > *k) {

/*                 W := W + C1**H * V1 */

		    i__1 = *m - *k;
		    zgemm_("Conjugate transpose", "No transpose", n, k, &i__1,
			     &c_b1, &c__[c_offset], ldc, &v[v_offset], ldv, &
			    c_b1, &work[work_offset], ldwork);
		}

/*              W := W * T**H  or  W * T */

		ztrmm_("Right", "Lower", transt, "Non-unit", n, k, &c_b1, &t[
			t_offset], ldt, &work[work_offset], ldwork);

/*              C := C - V * W**H */

		if (*m > *k) {

/*                 C1 := C1 - V1 * W**H */

		    i__1 = *m - *k;
		    z__1.r = -1., z__1.i = 0.;
		    zgemm_("No transpose", "Conjugate transpose", &i__1, n, k,
			     &z__1, &v[v_offset], ldv, &work[work_offset], 
			    ldwork, &c_b1, &c__[c_offset], ldc);
		}

/*              W := W * V2**H */

		ztrmm_("Right", "Upper", "Conjugate transpose", "Unit", n, k, 
			&c_b1, &v[*m - *k + 1 + v_dim1], ldv, &work[
			work_offset], ldwork);

/*              C2 := C2 - W**H */

		i__1 = *k;
		for (j = 1; j <= i__1; ++j) {
		    i__2 = *n;
		    for (i__ = 1; i__ <= i__2; ++i__) {
			i__3 = *m - *k + j + i__ * c_dim1;
			i__4 = *m - *k + j + i__ * c_dim1;
			d_cnjg(&z__2, &work[i__ + j * work_dim1]);
			z__1.r = c__[i__4].r - z__2.r, z__1.i = c__[i__4].i - 
				z__2.i;
			c__[i__3].r = z__1.r, c__[i__3].i = z__1.i;
/* L80: */
		    }
/* L90: */
		}

	    } else if (lsame_(side, "R")) {

/*              Form  C * H  or  C * H**H  where  C = ( C1  C2 ) */

/*              W := C * V  =  (C1*V1 + C2*V2)  (stored in WORK) */

/*              W := C2 */

		i__1 = *k;
		for (j = 1; j <= i__1; ++j) {
		    zcopy_(m, &c__[(*n - *k + j) * c_dim1 + 1], &c__1, &work[
			    j * work_dim1 + 1], &c__1);
/* L100: */
		}

/*              W := W * V2 */

		ztrmm_("Right", "Upper", "No transpose", "Unit", m, k, &c_b1, 
			&v[*n - *k + 1 + v_dim1], ldv, &work[work_offset], 
			ldwork);
		if (*n > *k) {

/*                 W := W + C1 * V1 */

		    i__1 = *n - *k;
		    zgemm_("No transpose", "No transpose", m, k, &i__1, &c_b1,
			     &c__[c_offset], ldc, &v[v_offset], ldv, &c_b1, &
			    work[work_offset], ldwork)
			    ;
		}

/*              W := W * T  or  W * T**H */

		ztrmm_("Right", "Lower", trans, "Non-unit", m, k, &c_b1, &t[
			t_offset], ldt, &work[work_offset], ldwork);

/*              C := C - W * V**H */

		if (*n > *k) {

/*                 C1 := C1 - W * V1**H */

		    i__1 = *n - *k;
		    z__1.r = -1., z__1.i = 0.;
		    zgemm_("No transpose", "Conjugate transpose", m, &i__1, k,
			     &z__1, &work[work_offset], ldwork, &v[v_offset], 
			    ldv, &c_b1, &c__[c_offset], ldc);
		}

/*              W := W * V2**H */

		ztrmm_("Right", "Upper", "Conjugate transpose", "Unit", m, k, 
			&c_b1, &v[*n - *k + 1 + v_dim1], ldv, &work[
			work_offset], ldwork);

/*              C2 := C2 - W */

		i__1 = *k;
		for (j = 1; j <= i__1; ++j) {
		    i__2 = *m;
		    for (i__ = 1; i__ <= i__2; ++i__) {
			i__3 = i__ + (*n - *k + j) * c_dim1;
			i__4 = i__ + (*n - *k + j) * c_dim1;
			i__5 = i__ + j * work_dim1;
			z__1.r = c__[i__4].r - work[i__5].r, z__1.i = c__[
				i__4].i - work[i__5].i;
			c__[i__3].r = z__1.r, c__[i__3].i = z__1.i;
/* L110: */
		    }
/* L120: */
		}
	    }
	}

    } else if (lsame_(storev, "R")) {

	if (lsame_(direct, "F")) {

/*           Let  V =  ( V1  V2 )    (V1: first K columns) */
/*           where  V1  is unit upper triangular. */

	    if (lsame_(side, "L")) {

/*              Form  H * C  or  H**H * C  where  C = ( C1 ) */
/*                                                    ( C2 ) */

/*              W := C**H * V**H  =  (C1**H * V1**H + C2**H * V2**H) (stored in WORK) */

/*              W := C1**H */

		i__1 = *k;
		for (j = 1; j <= i__1; ++j) {
		    zcopy_(n, &c__[j + c_dim1], ldc, &work[j * work_dim1 + 1],
			     &c__1);
		    zlacgv_(n, &work[j * work_dim1 + 1], &c__1);
/* L130: */
		}

/*              W := W * V1**H */

		ztrmm_("Right", "Upper", "Conjugate transpose", "Unit", n, k, 
			&c_b1, &v[v_offset], ldv, &work[work_offset], ldwork);
		if (*m > *k) {

/*                 W := W + C2**H * V2**H */

		    i__1 = *m - *k;
		    zgemm_("Conjugate transpose", "Conjugate transpose", n, k,
			     &i__1, &c_b1, &c__[*k + 1 + c_dim1], ldc, &v[(*k 
			    + 1) * v_dim1 + 1], ldv, &c_b1, &work[work_offset]
			    , ldwork);
		}

/*              W := W * T**H  or  W * T */

		ztrmm_("Right", "Upper", transt, "Non-unit", n, k, &c_b1, &t[
			t_offset], ldt, &work[work_offset], ldwork);

/*              C := C - V**H * W**H */

		if (*m > *k) {

/*                 C2 := C2 - V2**H * W**H */

		    i__1 = *m - *k;
		    z__1.r = -1., z__1.i = 0.;
		    zgemm_("Conjugate transpose", "Conjugate transpose", &
			    i__1, n, k, &z__1, &v[(*k + 1) * v_dim1 + 1], ldv,
			     &work[work_offset], ldwork, &c_b1, &c__[*k + 1 + 
			    c_dim1], ldc);
		}

/*              W := W * V1 */

		ztrmm_("Right", "Upper", "No transpose", "Unit", n, k, &c_b1, 
			&v[v_offset], ldv, &work[work_offset], ldwork);

/*              C1 := C1 - W**H */

		i__1 = *k;
		for (j = 1; j <= i__1; ++j) {
		    i__2 = *n;
		    for (i__ = 1; i__ <= i__2; ++i__) {
			i__3 = j + i__ * c_dim1;
			i__4 = j + i__ * c_dim1;
			d_cnjg(&z__2, &work[i__ + j * work_dim1]);
			z__1.r = c__[i__4].r - z__2.r, z__1.i = c__[i__4].i - 
				z__2.i;
			c__[i__3].r = z__1.r, c__[i__3].i = z__1.i;
/* L140: */
		    }
/* L150: */
		}

	    } else if (lsame_(side, "R")) {

/*              Form  C * H  or  C * H**H  where  C = ( C1  C2 ) */

/*              W := C * V**H  =  (C1*V1**H + C2*V2**H)  (stored in WORK) */

/*              W := C1 */

		i__1 = *k;
		for (j = 1; j <= i__1; ++j) {
		    zcopy_(m, &c__[j * c_dim1 + 1], &c__1, &work[j * 
			    work_dim1 + 1], &c__1);
/* L160: */
		}

/*              W := W * V1**H */

		ztrmm_("Right", "Upper", "Conjugate transpose", "Unit", m, k, 
			&c_b1, &v[v_offset], ldv, &work[work_offset], ldwork);
		if (*n > *k) {

/*                 W := W + C2 * V2**H */

		    i__1 = *n - *k;
		    zgemm_("No transpose", "Conjugate transpose", m, k, &i__1,
			     &c_b1, &c__[(*k + 1) * c_dim1 + 1], ldc, &v[(*k 
			    + 1) * v_dim1 + 1], ldv, &c_b1, &work[work_offset]
			    , ldwork);
		}

/*              W := W * T  or  W * T**H */

		ztrmm_("Right", "Upper", trans, "Non-unit", m, k, &c_b1, &t[
			t_offset], ldt, &work[work_offset], ldwork);

/*              C := C - W * V */

		if (*n > *k) {

/*                 C2 := C2 - W * V2 */

		    i__1 = *n - *k;
		    z__1.r = -1., z__1.i = 0.;
		    zgemm_("No transpose", "No transpose", m, &i__1, k, &z__1,
			     &work[work_offset], ldwork, &v[(*k + 1) * v_dim1 
			    + 1], ldv, &c_b1, &c__[(*k + 1) * c_dim1 + 1], 
			    ldc);
		}

/*              W := W * V1 */

		ztrmm_("Right", "Upper", "No transpose", "Unit", m, k, &c_b1, 
			&v[v_offset], ldv, &work[work_offset], ldwork);

/*              C1 := C1 - W */

		i__1 = *k;
		for (j = 1; j <= i__1; ++j) {
		    i__2 = *m;
		    for (i__ = 1; i__ <= i__2; ++i__) {
			i__3 = i__ + j * c_dim1;
			i__4 = i__ + j * c_dim1;
			i__5 = i__ + j * work_dim1;
			z__1.r = c__[i__4].r - work[i__5].r, z__1.i = c__[
				i__4].i - work[i__5].i;
			c__[i__3].r = z__1.r, c__[i__3].i = z__1.i;
/* L170: */
		    }
/* L180: */
		}

	    }

	} else {

/*           Let  V =  ( V1  V2 )    (V2: last K columns) */
/*           where  V2  is unit lower triangular. */

	    if (lsame_(side, "L")) {

/*              Form  H * C  or  H**H * C  where  C = ( C1 ) */
/*                                                    ( C2 ) */

/*              W := C**H * V**H  =  (C1**H * V1**H + C2**H * V2**H) (stored in WORK) */

/*              W := C2**H */

		i__1 = *k;
		for (j = 1; j <= i__1; ++j) {
		    zcopy_(n, &c__[*m - *k + j + c_dim1], ldc, &work[j * 
			    work_dim1 + 1], &c__1);
		    zlacgv_(n, &work[j * work_dim1 + 1], &c__1);
/* L190: */
		}

/*              W := W * V2**H */

		ztrmm_("Right", "Lower", "Conjugate transpose", "Unit", n, k, 
			&c_b1, &v[(*m - *k + 1) * v_dim1 + 1], ldv, &work[
			work_offset], ldwork);
		if (*m > *k) {

/*                 W := W + C1**H * V1**H */

		    i__1 = *m - *k;
		    zgemm_("Conjugate transpose", "Conjugate transpose", n, k,
			     &i__1, &c_b1, &c__[c_offset], ldc, &v[v_offset], 
			    ldv, &c_b1, &work[work_offset], ldwork);
		}

/*              W := W * T**H  or  W * T */

		ztrmm_("Right", "Lower", transt, "Non-unit", n, k, &c_b1, &t[
			t_offset], ldt, &work[work_offset], ldwork);

/*              C := C - V**H * W**H */

		if (*m > *k) {

/*                 C1 := C1 - V1**H * W**H */

		    i__1 = *m - *k;
		    z__1.r = -1., z__1.i = 0.;
		    zgemm_("Conjugate transpose", "Conjugate transpose", &
			    i__1, n, k, &z__1, &v[v_offset], ldv, &work[
			    work_offset], ldwork, &c_b1, &c__[c_offset], ldc);
		}

/*              W := W * V2 */

		ztrmm_("Right", "Lower", "No transpose", "Unit", n, k, &c_b1, 
			&v[(*m - *k + 1) * v_dim1 + 1], ldv, &work[
			work_offset], ldwork);

/*              C2 := C2 - W**H */

		i__1 = *k;
		for (j = 1; j <= i__1; ++j) {
		    i__2 = *n;
		    for (i__ = 1; i__ <= i__2; ++i__) {
			i__3 = *m - *k + j + i__ * c_dim1;
			i__4 = *m - *k + j + i__ * c_dim1;
			d_cnjg(&z__2, &work[i__ + j * work_dim1]);
			z__1.r = c__[i__4].r - z__2.r, z__1.i = c__[i__4].i - 
				z__2.i;
			c__[i__3].r = z__1.r, c__[i__3].i = z__1.i;
/* L200: */
		    }
/* L210: */
		}

	    } else if (lsame_(side, "R")) {

/*              Form  C * H  or  C * H**H  where  C = ( C1  C2 ) */

/*              W := C * V**H  =  (C1*V1**H + C2*V2**H)  (stored in WORK) */

/*              W := C2 */

		i__1 = *k;
		for (j = 1; j <= i__1; ++j) {
		    zcopy_(m, &c__[(*n - *k + j) * c_dim1 + 1], &c__1, &work[
			    j * work_dim1 + 1], &c__1);
/* L220: */
		}

/*              W := W * V2**H */

		ztrmm_("Right", "Lower", "Conjugate transpose", "Unit", m, k, 
			&c_b1, &v[(*n - *k + 1) * v_dim1 + 1], ldv, &work[
			work_offset], ldwork);
		if (*n > *k) {

/*                 W := W + C1 * V1**H */

		    i__1 = *n - *k;
		    zgemm_("No transpose", "Conjugate transpose", m, k, &i__1,
			     &c_b1, &c__[c_offset], ldc, &v[v_offset], ldv, &
			    c_b1, &work[work_offset], ldwork);
		}

/*              W := W * T  or  W * T**H */

		ztrmm_("Right", "Lower", trans, "Non-unit", m, k, &c_b1, &t[
			t_offset], ldt, &work[work_offset], ldwork);

/*              C := C - W * V */

		if (*n > *k) {

/*                 C1 := C1 - W * V1 */

		    i__1 = *n - *k;
		    z__1.r = -1., z__1.i = 0.;
		    zgemm_("No transpose", "No transpose", m, &i__1, k, &z__1,
			     &work[work_offset], ldwork, &v[v_offset], ldv, &
			    c_b1, &c__[c_offset], ldc)
			    ;
		}

/*              W := W * V2 */

		ztrmm_("Right", "Lower", "No transpose", "Unit", m, k, &c_b1, 
			&v[(*n - *k + 1) * v_dim1 + 1], ldv, &work[
			work_offset], ldwork);

/*              C1 := C1 - W */

		i__1 = *k;
		for (j = 1; j <= i__1; ++j) {
		    i__2 = *m;
		    for (i__ = 1; i__ <= i__2; ++i__) {
			i__3 = i__ + (*n - *k + j) * c_dim1;
			i__4 = i__ + (*n - *k + j) * c_dim1;
			i__5 = i__ + j * work_dim1;
			z__1.r = c__[i__4].r - work[i__5].r, z__1.i = c__[
				i__4].i - work[i__5].i;
			c__[i__3].r = z__1.r, c__[i__3].i = z__1.i;
/* L230: */
		    }
/* L240: */
		}

	    }

	}
    }

    return;

/*     End of ZLARFB */

} /* zlarfb_ */

