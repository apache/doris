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

static doublereal c_b7 = 0.;
static doublereal c_b8 = 1.;
static integer c__2 = 2;
static integer c__1 = 1;
static integer c__3 = 3;

/* > \brief \b DLAQR5 performs a single small-bulge multi-shift QR sweep. */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download DLAQR5 + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dlaqr5.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dlaqr5.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dlaqr5.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE DLAQR5( WANTT, WANTZ, KACC22, N, KTOP, KBOT, NSHFTS, */
/*                          SR, SI, H, LDH, ILOZ, IHIZ, Z, LDZ, V, LDV, U, */
/*                          LDU, NV, WV, LDWV, NH, WH, LDWH ) */

/*       INTEGER            IHIZ, ILOZ, KACC22, KBOT, KTOP, LDH, LDU, LDV, */
/*      $                   LDWH, LDWV, LDZ, N, NH, NSHFTS, NV */
/*       LOGICAL            WANTT, WANTZ */
/*       DOUBLE PRECISION   H( LDH, * ), SI( * ), SR( * ), U( LDU, * ), */
/*      $                   V( LDV, * ), WH( LDWH, * ), WV( LDWV, * ), */
/*      $                   Z( LDZ, * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* >    DLAQR5, called by DLAQR0, performs a */
/* >    single small-bulge multi-shift QR sweep. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] WANTT */
/* > \verbatim */
/* >          WANTT is LOGICAL */
/* >             WANTT = .true. if the quasi-triangular Schur factor */
/* >             is being computed.  WANTT is set to .false. otherwise. */
/* > \endverbatim */
/* > */
/* > \param[in] WANTZ */
/* > \verbatim */
/* >          WANTZ is LOGICAL */
/* >             WANTZ = .true. if the orthogonal Schur factor is being */
/* >             computed.  WANTZ is set to .false. otherwise. */
/* > \endverbatim */
/* > */
/* > \param[in] KACC22 */
/* > \verbatim */
/* >          KACC22 is INTEGER with value 0, 1, or 2. */
/* >             Specifies the computation mode of far-from-diagonal */
/* >             orthogonal updates. */
/* >        = 0: DLAQR5 does not accumulate reflections and does not */
/* >             use matrix-matrix multiply to update far-from-diagonal */
/* >             matrix entries. */
/* >        = 1: DLAQR5 accumulates reflections and uses matrix-matrix */
/* >             multiply to update the far-from-diagonal matrix entries. */
/* >        = 2: Same as KACC22 = 1. This option used to enable exploiting */
/* >             the 2-by-2 structure during matrix multiplications, but */
/* >             this is no longer supported. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >             N is the order of the Hessenberg matrix H upon which this */
/* >             subroutine operates. */
/* > \endverbatim */
/* > */
/* > \param[in] KTOP */
/* > \verbatim */
/* >          KTOP is INTEGER */
/* > \endverbatim */
/* > */
/* > \param[in] KBOT */
/* > \verbatim */
/* >          KBOT is INTEGER */
/* >             These are the first and last rows and columns of an */
/* >             isolated diagonal block upon which the QR sweep is to be */
/* >             applied. It is assumed without a check that */
/* >                       either KTOP = 1  or   H(KTOP,KTOP-1) = 0 */
/* >             and */
/* >                       either KBOT = N  or   H(KBOT+1,KBOT) = 0. */
/* > \endverbatim */
/* > */
/* > \param[in] NSHFTS */
/* > \verbatim */
/* >          NSHFTS is INTEGER */
/* >             NSHFTS gives the number of simultaneous shifts.  NSHFTS */
/* >             must be positive and even. */
/* > \endverbatim */
/* > */
/* > \param[in,out] SR */
/* > \verbatim */
/* >          SR is DOUBLE PRECISION array, dimension (NSHFTS) */
/* > \endverbatim */
/* > */
/* > \param[in,out] SI */
/* > \verbatim */
/* >          SI is DOUBLE PRECISION array, dimension (NSHFTS) */
/* >             SR contains the real parts and SI contains the imaginary */
/* >             parts of the NSHFTS shifts of origin that define the */
/* >             multi-shift QR sweep.  On output SR and SI may be */
/* >             reordered. */
/* > \endverbatim */
/* > */
/* > \param[in,out] H */
/* > \verbatim */
/* >          H is DOUBLE PRECISION array, dimension (LDH,N) */
/* >             On input H contains a Hessenberg matrix.  On output a */
/* >             multi-shift QR sweep with shifts SR(J)+i*SI(J) is applied */
/* >             to the isolated diagonal block in rows and columns KTOP */
/* >             through KBOT. */
/* > \endverbatim */
/* > */
/* > \param[in] LDH */
/* > \verbatim */
/* >          LDH is INTEGER */
/* >             LDH is the leading dimension of H just as declared in the */
/* >             calling procedure.  LDH >= MAX(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in] ILOZ */
/* > \verbatim */
/* >          ILOZ is INTEGER */
/* > \endverbatim */
/* > */
/* > \param[in] IHIZ */
/* > \verbatim */
/* >          IHIZ is INTEGER */
/* >             Specify the rows of Z to which transformations must be */
/* >             applied if WANTZ is .TRUE.. 1 <= ILOZ <= IHIZ <= N */
/* > \endverbatim */
/* > */
/* > \param[in,out] Z */
/* > \verbatim */
/* >          Z is DOUBLE PRECISION array, dimension (LDZ,IHIZ) */
/* >             If WANTZ = .TRUE., then the QR Sweep orthogonal */
/* >             similarity transformation is accumulated into */
/* >             Z(ILOZ:IHIZ,ILOZ:IHIZ) from the right. */
/* >             If WANTZ = .FALSE., then Z is unreferenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDZ */
/* > \verbatim */
/* >          LDZ is INTEGER */
/* >             LDA is the leading dimension of Z just as declared in */
/* >             the calling procedure. LDZ >= N. */
/* > \endverbatim */
/* > */
/* > \param[out] V */
/* > \verbatim */
/* >          V is DOUBLE PRECISION array, dimension (LDV,NSHFTS/2) */
/* > \endverbatim */
/* > */
/* > \param[in] LDV */
/* > \verbatim */
/* >          LDV is INTEGER */
/* >             LDV is the leading dimension of V as declared in the */
/* >             calling procedure.  LDV >= 3. */
/* > \endverbatim */
/* > */
/* > \param[out] U */
/* > \verbatim */
/* >          U is DOUBLE PRECISION array, dimension (LDU,2*NSHFTS) */
/* > \endverbatim */
/* > */
/* > \param[in] LDU */
/* > \verbatim */
/* >          LDU is INTEGER */
/* >             LDU is the leading dimension of U just as declared in the */
/* >             in the calling subroutine.  LDU >= 2*NSHFTS. */
/* > \endverbatim */
/* > */
/* > \param[in] NV */
/* > \verbatim */
/* >          NV is INTEGER */
/* >             NV is the number of rows in WV agailable for workspace. */
/* >             NV >= 1. */
/* > \endverbatim */
/* > */
/* > \param[out] WV */
/* > \verbatim */
/* >          WV is DOUBLE PRECISION array, dimension (LDWV,2*NSHFTS) */
/* > \endverbatim */
/* > */
/* > \param[in] LDWV */
/* > \verbatim */
/* >          LDWV is INTEGER */
/* >             LDWV is the leading dimension of WV as declared in the */
/* >             in the calling subroutine.  LDWV >= NV. */
/* > \endverbatim */

/* > \param[in] NH */
/* > \verbatim */
/* >          NH is INTEGER */
/* >             NH is the number of columns in array WH available for */
/* >             workspace. NH >= 1. */
/* > \endverbatim */
/* > */
/* > \param[out] WH */
/* > \verbatim */
/* >          WH is DOUBLE PRECISION array, dimension (LDWH,NH) */
/* > \endverbatim */
/* > */
/* > \param[in] LDWH */
/* > \verbatim */
/* >          LDWH is INTEGER */
/* >             Leading dimension of WH just as declared in the */
/* >             calling procedure.  LDWH >= 2*NSHFTS. */
/* > \endverbatim */
/* > */
/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date January 2021 */

/* > \ingroup doubleOTHERauxiliary */

/* > \par Contributors: */
/*  ================== */
/* > */
/* >       Karen Braman and Ralph Byers, Department of Mathematics, */
/* >       University of Kansas, USA */
/* > */
/* >       Lars Karlsson, Daniel Kressner, and Bruno Lang */
/* > */
/* >       Thijs Steel, Department of Computer science, */
/* >       KU Leuven, Belgium */

/* > \par References: */
/*  ================ */
/* > */
/* >       K. Braman, R. Byers and R. Mathias, The Multi-Shift QR */
/* >       Algorithm Part I: Maintaining Well Focused Shifts, and Level 3 */
/* >       Performance, SIAM Journal of Matrix Analysis, volume 23, pages */
/* >       929--947, 2002. */
/* > */
/* >       Lars Karlsson, Daniel Kressner, and Bruno Lang, Optimally packed */
/* >       chains of bulges in multishift QR algorithms. */
/* >       ACM Trans. Math. Softw. 40, 2, Article 12 (February 2014). */
/* > */
/*  ===================================================================== */
/* Subroutine */ void dlaqr5_(logical *wantt, logical *wantz, integer *kacc22, 
	integer *n, integer *ktop, integer *kbot, integer *nshfts, doublereal 
	*sr, doublereal *si, doublereal *h__, integer *ldh, integer *iloz, 
	integer *ihiz, doublereal *z__, integer *ldz, doublereal *v, integer *
	ldv, doublereal *u, integer *ldu, integer *nv, doublereal *wv, 
	integer *ldwv, integer *nh, doublereal *wh, integer *ldwh)
{
    /* System generated locals */
    integer h_dim1, h_offset, u_dim1, u_offset, v_dim1, v_offset, wh_dim1, 
	    wh_offset, wv_dim1, wv_offset, z_dim1, z_offset, i__1, i__2, i__3,
	     i__4, i__5, i__6, i__7;
    doublereal d__1, d__2, d__3, d__4, d__5;

    /* Local variables */
    doublereal beta;
    logical bmp22;
    integer jcol, jlen, jbot, mbot;
    doublereal swap;
    integer jtop, jrow, mtop, i__, j, k, m;
    doublereal alpha;
    logical accum;
    extern /* Subroutine */ void dgemm_(char *, char *, integer *, integer *, 
	    integer *, doublereal *, doublereal *, integer *, doublereal *, 
	    integer *, doublereal *, doublereal *, integer *);
    integer ndcol, incol, krcol, nbmps, i2, k1, i4;
    extern /* Subroutine */ void dlaqr1_(integer *, doublereal *, integer *, 
	    doublereal *, doublereal *, doublereal *, doublereal *, 
	    doublereal *), dlabad_(doublereal *, doublereal *);
    doublereal h11, h12, h21, h22;
    integer m22;
    extern doublereal dlamch_(char *);
    extern /* Subroutine */ void dlarfg_(integer *, doublereal *, doublereal *,
	     integer *, doublereal *);
    integer ns, nu;
    doublereal vt[3];
    extern /* Subroutine */ void dlacpy_(char *, integer *, integer *, 
	    doublereal *, integer *, doublereal *, integer *);
    doublereal safmin, safmax;
    extern /* Subroutine */ void dlaset_(char *, integer *, integer *, 
	    doublereal *, doublereal *, doublereal *, integer *);
    doublereal refsum, smlnum, scl;
    integer kdu, kms;
    doublereal ulp;
    doublereal tst1, tst2;


/*  -- LAPACK auxiliary routine (version 3.7.1) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     June 2016 */


/*  ================================================================ */


/*     ==== If there are no shifts, then there is nothing to do. ==== */

    /* Parameter adjustments */
    --sr;
    --si;
    h_dim1 = *ldh;
    h_offset = 1 + h_dim1 * 1;
    h__ -= h_offset;
    z_dim1 = *ldz;
    z_offset = 1 + z_dim1 * 1;
    z__ -= z_offset;
    v_dim1 = *ldv;
    v_offset = 1 + v_dim1 * 1;
    v -= v_offset;
    u_dim1 = *ldu;
    u_offset = 1 + u_dim1 * 1;
    u -= u_offset;
    wv_dim1 = *ldwv;
    wv_offset = 1 + wv_dim1 * 1;
    wv -= wv_offset;
    wh_dim1 = *ldwh;
    wh_offset = 1 + wh_dim1 * 1;
    wh -= wh_offset;

    /* Function Body */
    if (*nshfts < 2) {
	return;
    }

/*     ==== If the active block is empty or 1-by-1, then there */
/*     .    is nothing to do. ==== */

    if (*ktop >= *kbot) {
	return;
    }

/*     ==== Shuffle shifts into pairs of real shifts and pairs */
/*     .    of complex conjugate shifts assuming complex */
/*     .    conjugate shifts are already adjacent to one */
/*     .    another. ==== */

    i__1 = *nshfts - 2;
    for (i__ = 1; i__ <= i__1; i__ += 2) {
	if (si[i__] != -si[i__ + 1]) {

	    swap = sr[i__];
	    sr[i__] = sr[i__ + 1];
	    sr[i__ + 1] = sr[i__ + 2];
	    sr[i__ + 2] = swap;

	    swap = si[i__];
	    si[i__] = si[i__ + 1];
	    si[i__ + 1] = si[i__ + 2];
	    si[i__ + 2] = swap;
	}
/* L10: */
    }

/*     ==== NSHFTS is supposed to be even, but if it is odd, */
/*     .    then simply reduce it by one.  The shuffle above */
/*     .    ensures that the dropped shift is real and that */
/*     .    the remaining shifts are paired. ==== */

    ns = *nshfts - *nshfts % 2;

/*     ==== Machine constants for deflation ==== */

    safmin = dlamch_("SAFE MINIMUM");
    safmax = 1. / safmin;
    dlabad_(&safmin, &safmax);
    ulp = dlamch_("PRECISION");
    smlnum = safmin * ((doublereal) (*n) / ulp);

/*     ==== Use accumulated reflections to update far-from-diagonal */
/*     .    entries ? ==== */

    accum = *kacc22 == 1 || *kacc22 == 2;

/*     ==== clear trash ==== */

    if (*ktop + 2 <= *kbot) {
	h__[*ktop + 2 + *ktop * h_dim1] = 0.;
    }

/*     ==== NBMPS = number of 2-shift bulges in the chain ==== */

    nbmps = ns / 2;

/*     ==== KDU = width of slab ==== */

    kdu = nbmps << 2;

/*     ==== Create and chase chains of NBMPS bulges ==== */

    i__1 = *kbot - 2;
    i__2 = nbmps << 1;
    for (incol = *ktop - (nbmps << 1) + 1; i__2 < 0 ? incol >= i__1 : incol <=
	     i__1; incol += i__2) {

/*        JTOP = Index from which updates from the right start. */

	if (accum) {
	    jtop = f2cmax(*ktop,incol);
	} else if (*wantt) {
	    jtop = 1;
	} else {
	    jtop = *ktop;
	}

	ndcol = incol + kdu;
	if (accum) {
	    dlaset_("ALL", &kdu, &kdu, &c_b7, &c_b8, &u[u_offset], ldu);
	}

/*        ==== Near-the-diagonal bulge chase.  The following loop */
/*        .    performs the near-the-diagonal part of a small bulge */
/*        .    multi-shift QR sweep.  Each 4*NBMPS column diagonal */
/*        .    chunk extends from column INCOL to column NDCOL */
/*        .    (including both column INCOL and column NDCOL). The */
/*        .    following loop chases a 2*NBMPS+1 column long chain of */
/*        .    NBMPS bulges 2*NBMPS columns to the right.  (INCOL */
/*        .    may be less than KTOP and and NDCOL may be greater than */
/*        .    KBOT indicating phantom columns from which to chase */
/*        .    bulges before they are actually introduced or to which */
/*        .    to chase bulges beyond column KBOT.)  ==== */

/* Computing MIN */
	i__4 = incol + (nbmps << 1) - 1, i__5 = *kbot - 2;
	i__3 = f2cmin(i__4,i__5);
	for (krcol = incol; krcol <= i__3; ++krcol) {

/*           ==== Bulges number MTOP to MBOT are active double implicit */
/*           .    shift bulges.  There may or may not also be small */
/*           .    2-by-2 bulge, if there is room.  The inactive bulges */
/*           .    (if any) must wait until the active bulges have moved */
/*           .    down the diagonal to make room.  The phantom matrix */
/*           .    paradigm described above helps keep track.  ==== */

/* Computing MAX */
	    i__4 = 1, i__5 = (*ktop - krcol) / 2 + 1;
	    mtop = f2cmax(i__4,i__5);
/* Computing MIN */
	    i__4 = nbmps, i__5 = (*kbot - krcol - 1) / 2;
	    mbot = f2cmin(i__4,i__5);
	    m22 = mbot + 1;
	    bmp22 = mbot < nbmps && krcol + (m22 - 1 << 1) == *kbot - 2;

/*           ==== Generate reflections to chase the chain right */
/*           .    one column.  (The minimum value of K is KTOP-1.) ==== */

	    if (bmp22) {

/*              ==== Special case: 2-by-2 reflection at bottom treated */
/*              .    separately ==== */

		k = krcol + (m22 - 1 << 1);
		if (k == *ktop - 1) {
		    dlaqr1_(&c__2, &h__[k + 1 + (k + 1) * h_dim1], ldh, &sr[(
			    m22 << 1) - 1], &si[(m22 << 1) - 1], &sr[m22 * 2],
			     &si[m22 * 2], &v[m22 * v_dim1 + 1]);
		    beta = v[m22 * v_dim1 + 1];
		    dlarfg_(&c__2, &beta, &v[m22 * v_dim1 + 2], &c__1, &v[m22 
			    * v_dim1 + 1]);
		} else {
		    beta = h__[k + 1 + k * h_dim1];
		    v[m22 * v_dim1 + 2] = h__[k + 2 + k * h_dim1];
		    dlarfg_(&c__2, &beta, &v[m22 * v_dim1 + 2], &c__1, &v[m22 
			    * v_dim1 + 1]);
		    h__[k + 1 + k * h_dim1] = beta;
		    h__[k + 2 + k * h_dim1] = 0.;
		}

/*              ==== Perform update from right within */
/*              .    computational window. ==== */

/* Computing MIN */
		i__5 = *kbot, i__6 = k + 3;
		i__4 = f2cmin(i__5,i__6);
		for (j = jtop; j <= i__4; ++j) {
		    refsum = v[m22 * v_dim1 + 1] * (h__[j + (k + 1) * h_dim1] 
			    + v[m22 * v_dim1 + 2] * h__[j + (k + 2) * h_dim1])
			    ;
		    h__[j + (k + 1) * h_dim1] -= refsum;
		    h__[j + (k + 2) * h_dim1] -= refsum * v[m22 * v_dim1 + 2];
/* L30: */
		}

/*              ==== Perform update from left within */
/*              .    computational window. ==== */

		if (accum) {
		    jbot = f2cmin(ndcol,*kbot);
		} else if (*wantt) {
		    jbot = *n;
		} else {
		    jbot = *kbot;
		}
		i__4 = jbot;
		for (j = k + 1; j <= i__4; ++j) {
		    refsum = v[m22 * v_dim1 + 1] * (h__[k + 1 + j * h_dim1] + 
			    v[m22 * v_dim1 + 2] * h__[k + 2 + j * h_dim1]);
		    h__[k + 1 + j * h_dim1] -= refsum;
		    h__[k + 2 + j * h_dim1] -= refsum * v[m22 * v_dim1 + 2];
/* L40: */
		}

/*              ==== The following convergence test requires that */
/*              .    the tradition small-compared-to-nearby-diagonals */
/*              .    criterion and the Ahues & Tisseur (LAWN 122, 1997) */
/*              .    criteria both be satisfied.  The latter improves */
/*              .    accuracy in some examples. Falling back on an */
/*              .    alternate convergence criterion when TST1 or TST2 */
/*              .    is zero (as done here) is traditional but probably */
/*              .    unnecessary. ==== */

		if (k >= *ktop) {
		    if (h__[k + 1 + k * h_dim1] != 0.) {
			tst1 = (d__1 = h__[k + k * h_dim1], abs(d__1)) + (
				d__2 = h__[k + 1 + (k + 1) * h_dim1], abs(
				d__2));
			if (tst1 == 0.) {
			    if (k >= *ktop + 1) {
				tst1 += (d__1 = h__[k + (k - 1) * h_dim1], 
					abs(d__1));
			    }
			    if (k >= *ktop + 2) {
				tst1 += (d__1 = h__[k + (k - 2) * h_dim1], 
					abs(d__1));
			    }
			    if (k >= *ktop + 3) {
				tst1 += (d__1 = h__[k + (k - 3) * h_dim1], 
					abs(d__1));
			    }
			    if (k <= *kbot - 2) {
				tst1 += (d__1 = h__[k + 2 + (k + 1) * h_dim1],
					 abs(d__1));
			    }
			    if (k <= *kbot - 3) {
				tst1 += (d__1 = h__[k + 3 + (k + 1) * h_dim1],
					 abs(d__1));
			    }
			    if (k <= *kbot - 4) {
				tst1 += (d__1 = h__[k + 4 + (k + 1) * h_dim1],
					 abs(d__1));
			    }
			}
/* Computing MAX */
			d__2 = smlnum, d__3 = ulp * tst1;
			if ((d__1 = h__[k + 1 + k * h_dim1], abs(d__1)) <= 
				f2cmax(d__2,d__3)) {
/* Computing MAX */
			    d__3 = (d__1 = h__[k + 1 + k * h_dim1], abs(d__1))
				    , d__4 = (d__2 = h__[k + (k + 1) * h_dim1]
				    , abs(d__2));
			    h12 = f2cmax(d__3,d__4);
/* Computing MIN */
			    d__3 = (d__1 = h__[k + 1 + k * h_dim1], abs(d__1))
				    , d__4 = (d__2 = h__[k + (k + 1) * h_dim1]
				    , abs(d__2));
			    h21 = f2cmin(d__3,d__4);
/* Computing MAX */
			    d__3 = (d__1 = h__[k + 1 + (k + 1) * h_dim1], abs(
				    d__1)), d__4 = (d__2 = h__[k + k * h_dim1]
				     - h__[k + 1 + (k + 1) * h_dim1], abs(
				    d__2));
			    h11 = f2cmax(d__3,d__4);
/* Computing MIN */
			    d__3 = (d__1 = h__[k + 1 + (k + 1) * h_dim1], abs(
				    d__1)), d__4 = (d__2 = h__[k + k * h_dim1]
				     - h__[k + 1 + (k + 1) * h_dim1], abs(
				    d__2));
			    h22 = f2cmin(d__3,d__4);
			    scl = h11 + h12;
			    tst2 = h22 * (h11 / scl);

/* Computing MAX */
			    d__1 = smlnum, d__2 = ulp * tst2;
			    if (tst2 == 0. || h21 * (h12 / scl) <= f2cmax(d__1,
				    d__2)) {
				h__[k + 1 + k * h_dim1] = 0.;
			    }
			}
		    }
		}

/*              ==== Accumulate orthogonal transformations. ==== */

		if (accum) {
		    kms = k - incol;
/* Computing MAX */
		    i__4 = 1, i__5 = *ktop - incol;
		    i__6 = kdu;
		    for (j = f2cmax(i__4,i__5); j <= i__6; ++j) {
			refsum = v[m22 * v_dim1 + 1] * (u[j + (kms + 1) * 
				u_dim1] + v[m22 * v_dim1 + 2] * u[j + (kms + 
				2) * u_dim1]);
			u[j + (kms + 1) * u_dim1] -= refsum;
			u[j + (kms + 2) * u_dim1] -= refsum * v[m22 * v_dim1 
				+ 2];
/* L50: */
		    }
		} else if (*wantz) {
		    i__6 = *ihiz;
		    for (j = *iloz; j <= i__6; ++j) {
			refsum = v[m22 * v_dim1 + 1] * (z__[j + (k + 1) * 
				z_dim1] + v[m22 * v_dim1 + 2] * z__[j + (k + 
				2) * z_dim1]);
			z__[j + (k + 1) * z_dim1] -= refsum;
			z__[j + (k + 2) * z_dim1] -= refsum * v[m22 * v_dim1 
				+ 2];
/* L60: */
		    }
		}
	    }

/*           ==== Normal case: Chain of 3-by-3 reflections ==== */

	    i__6 = mtop;
	    for (m = mbot; m >= i__6; --m) {
		k = krcol + (m - 1 << 1);
		if (k == *ktop - 1) {
		    dlaqr1_(&c__3, &h__[*ktop + *ktop * h_dim1], ldh, &sr[(m 
			    << 1) - 1], &si[(m << 1) - 1], &sr[m * 2], &si[m *
			     2], &v[m * v_dim1 + 1]);
		    alpha = v[m * v_dim1 + 1];
		    dlarfg_(&c__3, &alpha, &v[m * v_dim1 + 2], &c__1, &v[m * 
			    v_dim1 + 1]);
		} else {

/*                 ==== Perform delayed transformation of row below */
/*                 .    Mth bulge. Exploit fact that first two elements */
/*                 .    of row are actually zero. ==== */

		    refsum = v[m * v_dim1 + 1] * v[m * v_dim1 + 3] * h__[k + 
			    3 + (k + 2) * h_dim1];
		    h__[k + 3 + k * h_dim1] = -refsum;
		    h__[k + 3 + (k + 1) * h_dim1] = -refsum * v[m * v_dim1 + 
			    2];
		    h__[k + 3 + (k + 2) * h_dim1] -= refsum * v[m * v_dim1 + 
			    3];

/*                 ==== Calculate reflection to move */
/*                 .    Mth bulge one step. ==== */

		    beta = h__[k + 1 + k * h_dim1];
		    v[m * v_dim1 + 2] = h__[k + 2 + k * h_dim1];
		    v[m * v_dim1 + 3] = h__[k + 3 + k * h_dim1];
		    dlarfg_(&c__3, &beta, &v[m * v_dim1 + 2], &c__1, &v[m * 
			    v_dim1 + 1]);

/*                 ==== A Bulge may collapse because of vigilant */
/*                 .    deflation or destructive underflow.  In the */
/*                 .    underflow case, try the two-small-subdiagonals */
/*                 .    trick to try to reinflate the bulge.  ==== */

		    if (h__[k + 3 + k * h_dim1] != 0. || h__[k + 3 + (k + 1) *
			     h_dim1] != 0. || h__[k + 3 + (k + 2) * h_dim1] ==
			     0.) {

/*                    ==== Typical case: not collapsed (yet). ==== */

			h__[k + 1 + k * h_dim1] = beta;
			h__[k + 2 + k * h_dim1] = 0.;
			h__[k + 3 + k * h_dim1] = 0.;
		    } else {

/*                    ==== Atypical case: collapsed.  Attempt to */
/*                    .    reintroduce ignoring H(K+1,K) and H(K+2,K). */
/*                    .    If the fill resulting from the new */
/*                    .    reflector is too large, then abandon it. */
/*                    .    Otherwise, use the new one. ==== */

			dlaqr1_(&c__3, &h__[k + 1 + (k + 1) * h_dim1], ldh, &
				sr[(m << 1) - 1], &si[(m << 1) - 1], &sr[m * 
				2], &si[m * 2], vt);
			alpha = vt[0];
			dlarfg_(&c__3, &alpha, &vt[1], &c__1, vt);
			refsum = vt[0] * (h__[k + 1 + k * h_dim1] + vt[1] * 
				h__[k + 2 + k * h_dim1]);

			if ((d__1 = h__[k + 2 + k * h_dim1] - refsum * vt[1], 
				abs(d__1)) + (d__2 = refsum * vt[2], abs(d__2)
				) > ulp * ((d__3 = h__[k + k * h_dim1], abs(
				d__3)) + (d__4 = h__[k + 1 + (k + 1) * h_dim1]
				, abs(d__4)) + (d__5 = h__[k + 2 + (k + 2) * 
				h_dim1], abs(d__5)))) {

/*                       ==== Starting a new bulge here would */
/*                       .    create non-negligible fill.  Use */
/*                       .    the old one with trepidation. ==== */

			    h__[k + 1 + k * h_dim1] = beta;
			    h__[k + 2 + k * h_dim1] = 0.;
			    h__[k + 3 + k * h_dim1] = 0.;
			} else {

/*                       ==== Starting a new bulge here would */
/*                       .    create only negligible fill. */
/*                       .    Replace the old reflector with */
/*                       .    the new one. ==== */

			    h__[k + 1 + k * h_dim1] -= refsum;
			    h__[k + 2 + k * h_dim1] = 0.;
			    h__[k + 3 + k * h_dim1] = 0.;
			    v[m * v_dim1 + 1] = vt[0];
			    v[m * v_dim1 + 2] = vt[1];
			    v[m * v_dim1 + 3] = vt[2];
			}
		    }
		}

/*              ====  Apply reflection from the right and */
/*              .     the first column of update from the left. */
/*              .     These updates are required for the vigilant */
/*              .     deflation check. We still delay most of the */
/*              .     updates from the left for efficiency. ==== */

/* Computing MIN */
		i__5 = *kbot, i__7 = k + 3;
		i__4 = f2cmin(i__5,i__7);
		for (j = jtop; j <= i__4; ++j) {
		    refsum = v[m * v_dim1 + 1] * (h__[j + (k + 1) * h_dim1] + 
			    v[m * v_dim1 + 2] * h__[j + (k + 2) * h_dim1] + v[
			    m * v_dim1 + 3] * h__[j + (k + 3) * h_dim1]);
		    h__[j + (k + 1) * h_dim1] -= refsum;
		    h__[j + (k + 2) * h_dim1] -= refsum * v[m * v_dim1 + 2];
		    h__[j + (k + 3) * h_dim1] -= refsum * v[m * v_dim1 + 3];
/* L70: */
		}

/*              ==== Perform update from left for subsequent */
/*              .    column. ==== */

		refsum = v[m * v_dim1 + 1] * (h__[k + 1 + (k + 1) * h_dim1] + 
			v[m * v_dim1 + 2] * h__[k + 2 + (k + 1) * h_dim1] + v[
			m * v_dim1 + 3] * h__[k + 3 + (k + 1) * h_dim1]);
		h__[k + 1 + (k + 1) * h_dim1] -= refsum;
		h__[k + 2 + (k + 1) * h_dim1] -= refsum * v[m * v_dim1 + 2];
		h__[k + 3 + (k + 1) * h_dim1] -= refsum * v[m * v_dim1 + 3];

/*              ==== The following convergence test requires that */
/*              .    the tradition small-compared-to-nearby-diagonals */
/*              .    criterion and the Ahues & Tisseur (LAWN 122, 1997) */
/*              .    criteria both be satisfied.  The latter improves */
/*              .    accuracy in some examples. Falling back on an */
/*              .    alternate convergence criterion when TST1 or TST2 */
/*              .    is zero (as done here) is traditional but probably */
/*              .    unnecessary. ==== */

		if (k < *ktop) {
		    mycycle_();
		}
		if (h__[k + 1 + k * h_dim1] != 0.) {
		    tst1 = (d__1 = h__[k + k * h_dim1], abs(d__1)) + (d__2 = 
			    h__[k + 1 + (k + 1) * h_dim1], abs(d__2));
		    if (tst1 == 0.) {
			if (k >= *ktop + 1) {
			    tst1 += (d__1 = h__[k + (k - 1) * h_dim1], abs(
				    d__1));
			}
			if (k >= *ktop + 2) {
			    tst1 += (d__1 = h__[k + (k - 2) * h_dim1], abs(
				    d__1));
			}
			if (k >= *ktop + 3) {
			    tst1 += (d__1 = h__[k + (k - 3) * h_dim1], abs(
				    d__1));
			}
			if (k <= *kbot - 2) {
			    tst1 += (d__1 = h__[k + 2 + (k + 1) * h_dim1], 
				    abs(d__1));
			}
			if (k <= *kbot - 3) {
			    tst1 += (d__1 = h__[k + 3 + (k + 1) * h_dim1], 
				    abs(d__1));
			}
			if (k <= *kbot - 4) {
			    tst1 += (d__1 = h__[k + 4 + (k + 1) * h_dim1], 
				    abs(d__1));
			}
		    }
/* Computing MAX */
		    d__2 = smlnum, d__3 = ulp * tst1;
		    if ((d__1 = h__[k + 1 + k * h_dim1], abs(d__1)) <= f2cmax(
			    d__2,d__3)) {
/* Computing MAX */
			d__3 = (d__1 = h__[k + 1 + k * h_dim1], abs(d__1)), 
				d__4 = (d__2 = h__[k + (k + 1) * h_dim1], abs(
				d__2));
			h12 = f2cmax(d__3,d__4);
/* Computing MIN */
			d__3 = (d__1 = h__[k + 1 + k * h_dim1], abs(d__1)), 
				d__4 = (d__2 = h__[k + (k + 1) * h_dim1], abs(
				d__2));
			h21 = f2cmin(d__3,d__4);
/* Computing MAX */
			d__3 = (d__1 = h__[k + 1 + (k + 1) * h_dim1], abs(
				d__1)), d__4 = (d__2 = h__[k + k * h_dim1] - 
				h__[k + 1 + (k + 1) * h_dim1], abs(d__2));
			h11 = f2cmax(d__3,d__4);
/* Computing MIN */
			d__3 = (d__1 = h__[k + 1 + (k + 1) * h_dim1], abs(
				d__1)), d__4 = (d__2 = h__[k + k * h_dim1] - 
				h__[k + 1 + (k + 1) * h_dim1], abs(d__2));
			h22 = f2cmin(d__3,d__4);
			scl = h11 + h12;
			tst2 = h22 * (h11 / scl);

/* Computing MAX */
			d__1 = smlnum, d__2 = ulp * tst2;
			if (tst2 == 0. || h21 * (h12 / scl) <= f2cmax(d__1,d__2))
				 {
			    h__[k + 1 + k * h_dim1] = 0.;
			}
		    }
		}
/* L80: */
	    }

/*           ==== Multiply H by reflections from the left ==== */

	    if (accum) {
		jbot = f2cmin(ndcol,*kbot);
	    } else if (*wantt) {
		jbot = *n;
	    } else {
		jbot = *kbot;
	    }

	    i__6 = mtop;
	    for (m = mbot; m >= i__6; --m) {
		k = krcol + (m - 1 << 1);
/* Computing MAX */
		i__4 = *ktop, i__5 = krcol + (m << 1);
		i__7 = jbot;
		for (j = f2cmax(i__4,i__5); j <= i__7; ++j) {
		    refsum = v[m * v_dim1 + 1] * (h__[k + 1 + j * h_dim1] + v[
			    m * v_dim1 + 2] * h__[k + 2 + j * h_dim1] + v[m * 
			    v_dim1 + 3] * h__[k + 3 + j * h_dim1]);
		    h__[k + 1 + j * h_dim1] -= refsum;
		    h__[k + 2 + j * h_dim1] -= refsum * v[m * v_dim1 + 2];
		    h__[k + 3 + j * h_dim1] -= refsum * v[m * v_dim1 + 3];
/* L90: */
		}
/* L100: */
	    }

/*           ==== Accumulate orthogonal transformations. ==== */

	    if (accum) {

/*              ==== Accumulate U. (If needed, update Z later */
/*              .    with an efficient matrix-matrix */
/*              .    multiply.) ==== */

		i__6 = mtop;
		for (m = mbot; m >= i__6; --m) {
		    k = krcol + (m - 1 << 1);
		    kms = k - incol;
/* Computing MAX */
		    i__7 = 1, i__4 = *ktop - incol;
		    i2 = f2cmax(i__7,i__4);
/* Computing MAX */
		    i__7 = i2, i__4 = kms - (krcol - incol) + 1;
		    i2 = f2cmax(i__7,i__4);
/* Computing MIN */
		    i__7 = kdu, i__4 = krcol + (mbot - 1 << 1) - incol + 5;
		    i4 = f2cmin(i__7,i__4);
		    i__7 = i4;
		    for (j = i2; j <= i__7; ++j) {
			refsum = v[m * v_dim1 + 1] * (u[j + (kms + 1) * 
				u_dim1] + v[m * v_dim1 + 2] * u[j + (kms + 2) 
				* u_dim1] + v[m * v_dim1 + 3] * u[j + (kms + 
				3) * u_dim1]);
			u[j + (kms + 1) * u_dim1] -= refsum;
			u[j + (kms + 2) * u_dim1] -= refsum * v[m * v_dim1 + 
				2];
			u[j + (kms + 3) * u_dim1] -= refsum * v[m * v_dim1 + 
				3];
/* L110: */
		    }
/* L120: */
		}
	    } else if (*wantz) {

/*              ==== U is not accumulated, so update Z */
/*              .    now by multiplying by reflections */
/*              .    from the right. ==== */

		i__6 = mtop;
		for (m = mbot; m >= i__6; --m) {
		    k = krcol + (m - 1 << 1);
		    i__7 = *ihiz;
		    for (j = *iloz; j <= i__7; ++j) {
			refsum = v[m * v_dim1 + 1] * (z__[j + (k + 1) * 
				z_dim1] + v[m * v_dim1 + 2] * z__[j + (k + 2) 
				* z_dim1] + v[m * v_dim1 + 3] * z__[j + (k + 
				3) * z_dim1]);
			z__[j + (k + 1) * z_dim1] -= refsum;
			z__[j + (k + 2) * z_dim1] -= refsum * v[m * v_dim1 + 
				2];
			z__[j + (k + 3) * z_dim1] -= refsum * v[m * v_dim1 + 
				3];
/* L130: */
		    }
/* L140: */
		}
	    }

/*           ==== End of near-the-diagonal bulge chase. ==== */

/* L145: */
	}

/*        ==== Use U (if accumulated) to update far-from-diagonal */
/*        .    entries in H.  If required, use U to update Z as */
/*        .    well. ==== */

	if (accum) {
	    if (*wantt) {
		jtop = 1;
		jbot = *n;
	    } else {
		jtop = *ktop;
		jbot = *kbot;
	    }
/* Computing MAX */
	    i__3 = 1, i__6 = *ktop - incol;
	    k1 = f2cmax(i__3,i__6);
/* Computing MAX */
	    i__3 = 0, i__6 = ndcol - *kbot;
	    nu = kdu - f2cmax(i__3,i__6) - k1 + 1;

/*           ==== Horizontal Multiply ==== */

	    i__3 = jbot;
	    i__6 = *nh;
	    for (jcol = f2cmin(ndcol,*kbot) + 1; i__6 < 0 ? jcol >= i__3 : jcol 
		    <= i__3; jcol += i__6) {
/* Computing MIN */
		i__7 = *nh, i__4 = jbot - jcol + 1;
		jlen = f2cmin(i__7,i__4);
		dgemm_("C", "N", &nu, &jlen, &nu, &c_b8, &u[k1 + k1 * u_dim1],
			 ldu, &h__[incol + k1 + jcol * h_dim1], ldh, &c_b7, &
			wh[wh_offset], ldwh);
		dlacpy_("ALL", &nu, &jlen, &wh[wh_offset], ldwh, &h__[incol + 
			k1 + jcol * h_dim1], ldh);
/* L150: */
	    }

/*           ==== Vertical multiply ==== */

	    i__6 = f2cmax(*ktop,incol) - 1;
	    i__3 = *nv;
	    for (jrow = jtop; i__3 < 0 ? jrow >= i__6 : jrow <= i__6; jrow += 
		    i__3) {
/* Computing MIN */
		i__7 = *nv, i__4 = f2cmax(*ktop,incol) - jrow;
		jlen = f2cmin(i__7,i__4);
		dgemm_("N", "N", &jlen, &nu, &nu, &c_b8, &h__[jrow + (incol + 
			k1) * h_dim1], ldh, &u[k1 + k1 * u_dim1], ldu, &c_b7, 
			&wv[wv_offset], ldwv);
		dlacpy_("ALL", &jlen, &nu, &wv[wv_offset], ldwv, &h__[jrow + (
			incol + k1) * h_dim1], ldh);
/* L160: */
	    }

/*           ==== Z multiply (also vertical) ==== */

	    if (*wantz) {
		i__3 = *ihiz;
		i__6 = *nv;
		for (jrow = *iloz; i__6 < 0 ? jrow >= i__3 : jrow <= i__3; 
			jrow += i__6) {
/* Computing MIN */
		    i__7 = *nv, i__4 = *ihiz - jrow + 1;
		    jlen = f2cmin(i__7,i__4);
		    dgemm_("N", "N", &jlen, &nu, &nu, &c_b8, &z__[jrow + (
			    incol + k1) * z_dim1], ldz, &u[k1 + k1 * u_dim1], 
			    ldu, &c_b7, &wv[wv_offset], ldwv);
		    dlacpy_("ALL", &jlen, &nu, &wv[wv_offset], ldwv, &z__[
			    jrow + (incol + k1) * z_dim1], ldz);
/* L170: */
		}
	    }
	}
/* L180: */
    }

/*     ==== End of DLAQR5 ==== */

    return;
} /* dlaqr5_ */

