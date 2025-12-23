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

/* > \brief \b SLASD7 merges the two sets of singular values together into a single sorted set. Then it tries 
to deflate the size of the problem. Used by sbdsdc. */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download SLASD7 + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slasd7.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slasd7.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slasd7.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE SLASD7( ICOMPQ, NL, NR, SQRE, K, D, Z, ZW, VF, VFW, VL, */
/*                          VLW, ALPHA, BETA, DSIGMA, IDX, IDXP, IDXQ, */
/*                          PERM, GIVPTR, GIVCOL, LDGCOL, GIVNUM, LDGNUM, */
/*                          C, S, INFO ) */

/*       INTEGER            GIVPTR, ICOMPQ, INFO, K, LDGCOL, LDGNUM, NL, */
/*      $                   NR, SQRE */
/*       REAL               ALPHA, BETA, C, S */
/*       INTEGER            GIVCOL( LDGCOL, * ), IDX( * ), IDXP( * ), */
/*      $                   IDXQ( * ), PERM( * ) */
/*       REAL               D( * ), DSIGMA( * ), GIVNUM( LDGNUM, * ), */
/*      $                   VF( * ), VFW( * ), VL( * ), VLW( * ), Z( * ), */
/*      $                   ZW( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > SLASD7 merges the two sets of singular values together into a single */
/* > sorted set. Then it tries to deflate the size of the problem. There */
/* > are two ways in which deflation can occur:  when two or more singular */
/* > values are close together or if there is a tiny entry in the Z */
/* > vector. For each such occurrence the order of the related */
/* > secular equation problem is reduced by one. */
/* > */
/* > SLASD7 is called from SLASD6. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] ICOMPQ */
/* > \verbatim */
/* >          ICOMPQ is INTEGER */
/* >          Specifies whether singular vectors are to be computed */
/* >          in compact form, as follows: */
/* >          = 0: Compute singular values only. */
/* >          = 1: Compute singular vectors of upper */
/* >               bidiagonal matrix in compact form. */
/* > \endverbatim */
/* > */
/* > \param[in] NL */
/* > \verbatim */
/* >          NL is INTEGER */
/* >         The row dimension of the upper block. NL >= 1. */
/* > \endverbatim */
/* > */
/* > \param[in] NR */
/* > \verbatim */
/* >          NR is INTEGER */
/* >         The row dimension of the lower block. NR >= 1. */
/* > \endverbatim */
/* > */
/* > \param[in] SQRE */
/* > \verbatim */
/* >          SQRE is INTEGER */
/* >         = 0: the lower block is an NR-by-NR square matrix. */
/* >         = 1: the lower block is an NR-by-(NR+1) rectangular matrix. */
/* > */
/* >         The bidiagonal matrix has */
/* >         N = NL + NR + 1 rows and */
/* >         M = N + SQRE >= N columns. */
/* > \endverbatim */
/* > */
/* > \param[out] K */
/* > \verbatim */
/* >          K is INTEGER */
/* >         Contains the dimension of the non-deflated matrix, this is */
/* >         the order of the related secular equation. 1 <= K <=N. */
/* > \endverbatim */
/* > */
/* > \param[in,out] D */
/* > \verbatim */
/* >          D is REAL array, dimension ( N ) */
/* >         On entry D contains the singular values of the two submatrices */
/* >         to be combined. On exit D contains the trailing (N-K) updated */
/* >         singular values (those which were deflated) sorted into */
/* >         increasing order. */
/* > \endverbatim */
/* > */
/* > \param[out] Z */
/* > \verbatim */
/* >          Z is REAL array, dimension ( M ) */
/* >         On exit Z contains the updating row vector in the secular */
/* >         equation. */
/* > \endverbatim */
/* > */
/* > \param[out] ZW */
/* > \verbatim */
/* >          ZW is REAL array, dimension ( M ) */
/* >         Workspace for Z. */
/* > \endverbatim */
/* > */
/* > \param[in,out] VF */
/* > \verbatim */
/* >          VF is REAL array, dimension ( M ) */
/* >         On entry, VF(1:NL+1) contains the first components of all */
/* >         right singular vectors of the upper block; and VF(NL+2:M) */
/* >         contains the first components of all right singular vectors */
/* >         of the lower block. On exit, VF contains the first components */
/* >         of all right singular vectors of the bidiagonal matrix. */
/* > \endverbatim */
/* > */
/* > \param[out] VFW */
/* > \verbatim */
/* >          VFW is REAL array, dimension ( M ) */
/* >         Workspace for VF. */
/* > \endverbatim */
/* > */
/* > \param[in,out] VL */
/* > \verbatim */
/* >          VL is REAL array, dimension ( M ) */
/* >         On entry, VL(1:NL+1) contains the  last components of all */
/* >         right singular vectors of the upper block; and VL(NL+2:M) */
/* >         contains the last components of all right singular vectors */
/* >         of the lower block. On exit, VL contains the last components */
/* >         of all right singular vectors of the bidiagonal matrix. */
/* > \endverbatim */
/* > */
/* > \param[out] VLW */
/* > \verbatim */
/* >          VLW is REAL array, dimension ( M ) */
/* >         Workspace for VL. */
/* > \endverbatim */
/* > */
/* > \param[in] ALPHA */
/* > \verbatim */
/* >          ALPHA is REAL */
/* >         Contains the diagonal element associated with the added row. */
/* > \endverbatim */
/* > */
/* > \param[in] BETA */
/* > \verbatim */
/* >          BETA is REAL */
/* >         Contains the off-diagonal element associated with the added */
/* >         row. */
/* > \endverbatim */
/* > */
/* > \param[out] DSIGMA */
/* > \verbatim */
/* >          DSIGMA is REAL array, dimension ( N ) */
/* >         Contains a copy of the diagonal elements (K-1 singular values */
/* >         and one zero) in the secular equation. */
/* > \endverbatim */
/* > */
/* > \param[out] IDX */
/* > \verbatim */
/* >          IDX is INTEGER array, dimension ( N ) */
/* >         This will contain the permutation used to sort the contents of */
/* >         D into ascending order. */
/* > \endverbatim */
/* > */
/* > \param[out] IDXP */
/* > \verbatim */
/* >          IDXP is INTEGER array, dimension ( N ) */
/* >         This will contain the permutation used to place deflated */
/* >         values of D at the end of the array. On output IDXP(2:K) */
/* >         points to the nondeflated D-values and IDXP(K+1:N) */
/* >         points to the deflated singular values. */
/* > \endverbatim */
/* > */
/* > \param[in] IDXQ */
/* > \verbatim */
/* >          IDXQ is INTEGER array, dimension ( N ) */
/* >         This contains the permutation which separately sorts the two */
/* >         sub-problems in D into ascending order.  Note that entries in */
/* >         the first half of this permutation must first be moved one */
/* >         position backward; and entries in the second half */
/* >         must first have NL+1 added to their values. */
/* > \endverbatim */
/* > */
/* > \param[out] PERM */
/* > \verbatim */
/* >          PERM is INTEGER array, dimension ( N ) */
/* >         The permutations (from deflation and sorting) to be applied */
/* >         to each singular block. Not referenced if ICOMPQ = 0. */
/* > \endverbatim */
/* > */
/* > \param[out] GIVPTR */
/* > \verbatim */
/* >          GIVPTR is INTEGER */
/* >         The number of Givens rotations which took place in this */
/* >         subproblem. Not referenced if ICOMPQ = 0. */
/* > \endverbatim */
/* > */
/* > \param[out] GIVCOL */
/* > \verbatim */
/* >          GIVCOL is INTEGER array, dimension ( LDGCOL, 2 ) */
/* >         Each pair of numbers indicates a pair of columns to take place */
/* >         in a Givens rotation. Not referenced if ICOMPQ = 0. */
/* > \endverbatim */
/* > */
/* > \param[in] LDGCOL */
/* > \verbatim */
/* >          LDGCOL is INTEGER */
/* >         The leading dimension of GIVCOL, must be at least N. */
/* > \endverbatim */
/* > */
/* > \param[out] GIVNUM */
/* > \verbatim */
/* >          GIVNUM is REAL array, dimension ( LDGNUM, 2 ) */
/* >         Each number indicates the C or S value to be used in the */
/* >         corresponding Givens rotation. Not referenced if ICOMPQ = 0. */
/* > \endverbatim */
/* > */
/* > \param[in] LDGNUM */
/* > \verbatim */
/* >          LDGNUM is INTEGER */
/* >         The leading dimension of GIVNUM, must be at least N. */
/* > \endverbatim */
/* > */
/* > \param[out] C */
/* > \verbatim */
/* >          C is REAL */
/* >         C contains garbage if SQRE =0 and the C-value of a Givens */
/* >         rotation related to the right null space if SQRE = 1. */
/* > \endverbatim */
/* > */
/* > \param[out] S */
/* > \verbatim */
/* >          S is REAL */
/* >         S contains garbage if SQRE =0 and the S-value of a Givens */
/* >         rotation related to the right null space if SQRE = 1. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >         = 0:  successful exit. */
/* >         < 0:  if INFO = -i, the i-th argument had an illegal value. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup OTHERauxiliary */

/* > \par Contributors: */
/*  ================== */
/* > */
/* >     Ming Gu and Huan Ren, Computer Science Division, University of */
/* >     California at Berkeley, USA */
/* > */
/*  ===================================================================== */
/* Subroutine */ void slasd7_(integer *icompq, integer *nl, integer *nr, 
	integer *sqre, integer *k, real *d__, real *z__, real *zw, real *vf, 
	real *vfw, real *vl, real *vlw, real *alpha, real *beta, real *dsigma,
	 integer *idx, integer *idxp, integer *idxq, integer *perm, integer *
	givptr, integer *givcol, integer *ldgcol, real *givnum, integer *
	ldgnum, real *c__, real *s, integer *info)
{
    /* System generated locals */
    integer givcol_dim1, givcol_offset, givnum_dim1, givnum_offset, i__1;
    real r__1, r__2;

    /* Local variables */
    integer idxi, idxj;
    extern /* Subroutine */ void srot_(integer *, real *, integer *, real *, 
	    integer *, real *, real *);
    integer i__, j, m, n, idxjp, jprev, k2;
    extern /* Subroutine */ void scopy_(integer *, real *, integer *, real *, 
	    integer *);
    real z1;
    extern real slapy2_(real *, real *);
    integer jp;
    extern real slamch_(char *);
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    extern void slamrg_(
	    integer *, integer *, real *, integer *, integer *, integer *);
    real hlftol, eps, tau, tol;
    integer nlp1, nlp2;


/*  -- LAPACK auxiliary routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */



/*     Test the input parameters. */

    /* Parameter adjustments */
    --d__;
    --z__;
    --zw;
    --vf;
    --vfw;
    --vl;
    --vlw;
    --dsigma;
    --idx;
    --idxp;
    --idxq;
    --perm;
    givcol_dim1 = *ldgcol;
    givcol_offset = 1 + givcol_dim1 * 1;
    givcol -= givcol_offset;
    givnum_dim1 = *ldgnum;
    givnum_offset = 1 + givnum_dim1 * 1;
    givnum -= givnum_offset;

    /* Function Body */
    *info = 0;
    n = *nl + *nr + 1;
    m = n + *sqre;

    if (*icompq < 0 || *icompq > 1) {
	*info = -1;
    } else if (*nl < 1) {
	*info = -2;
    } else if (*nr < 1) {
	*info = -3;
    } else if (*sqre < 0 || *sqre > 1) {
	*info = -4;
    } else if (*ldgcol < n) {
	*info = -22;
    } else if (*ldgnum < n) {
	*info = -24;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("SLASD7", &i__1, (ftnlen)6);
	return;
    }

    nlp1 = *nl + 1;
    nlp2 = *nl + 2;
    if (*icompq == 1) {
	*givptr = 0;
    }

/*     Generate the first part of the vector Z and move the singular */
/*     values in the first part of D one position backward. */

    z1 = *alpha * vl[nlp1];
    vl[nlp1] = 0.f;
    tau = vf[nlp1];
    for (i__ = *nl; i__ >= 1; --i__) {
	z__[i__ + 1] = *alpha * vl[i__];
	vl[i__] = 0.f;
	vf[i__ + 1] = vf[i__];
	d__[i__ + 1] = d__[i__];
	idxq[i__ + 1] = idxq[i__] + 1;
/* L10: */
    }
    vf[1] = tau;

/*     Generate the second part of the vector Z. */

    i__1 = m;
    for (i__ = nlp2; i__ <= i__1; ++i__) {
	z__[i__] = *beta * vf[i__];
	vf[i__] = 0.f;
/* L20: */
    }

/*     Sort the singular values into increasing order */

    i__1 = n;
    for (i__ = nlp2; i__ <= i__1; ++i__) {
	idxq[i__] += nlp1;
/* L30: */
    }

/*     DSIGMA, IDXC, IDXC, and ZW are used as storage space. */

    i__1 = n;
    for (i__ = 2; i__ <= i__1; ++i__) {
	dsigma[i__] = d__[idxq[i__]];
	zw[i__] = z__[idxq[i__]];
	vfw[i__] = vf[idxq[i__]];
	vlw[i__] = vl[idxq[i__]];
/* L40: */
    }

    slamrg_(nl, nr, &dsigma[2], &c__1, &c__1, &idx[2]);

    i__1 = n;
    for (i__ = 2; i__ <= i__1; ++i__) {
	idxi = idx[i__] + 1;
	d__[i__] = dsigma[idxi];
	z__[i__] = zw[idxi];
	vf[i__] = vfw[idxi];
	vl[i__] = vlw[idxi];
/* L50: */
    }

/*     Calculate the allowable deflation tolerance */

    eps = slamch_("Epsilon");
/* Computing MAX */
    r__1 = abs(*alpha), r__2 = abs(*beta);
    tol = f2cmax(r__1,r__2);
/* Computing MAX */
    r__2 = (r__1 = d__[n], abs(r__1));
    tol = eps * 64.f * f2cmax(r__2,tol);

/*     There are 2 kinds of deflation -- first a value in the z-vector */
/*     is small, second two (or more) singular values are very close */
/*     together (their difference is small). */

/*     If the value in the z-vector is small, we simply permute the */
/*     array so that the corresponding singular value is moved to the */
/*     end. */

/*     If two values in the D-vector are close, we perform a two-sided */
/*     rotation designed to make one of the corresponding z-vector */
/*     entries zero, and then permute the array so that the deflated */
/*     singular value is moved to the end. */

/*     If there are multiple singular values then the problem deflates. */
/*     Here the number of equal singular values are found.  As each equal */
/*     singular value is found, an elementary reflector is computed to */
/*     rotate the corresponding singular subspace so that the */
/*     corresponding components of Z are zero in this new basis. */

    *k = 1;
    k2 = n + 1;
    i__1 = n;
    for (j = 2; j <= i__1; ++j) {
	if ((r__1 = z__[j], abs(r__1)) <= tol) {

/*           Deflate due to small z component. */

	    --k2;
	    idxp[k2] = j;
	    if (j == n) {
		goto L100;
	    }
	} else {
	    jprev = j;
	    goto L70;
	}
/* L60: */
    }
L70:
    j = jprev;
L80:
    ++j;
    if (j > n) {
	goto L90;
    }
    if ((r__1 = z__[j], abs(r__1)) <= tol) {

/*        Deflate due to small z component. */

	--k2;
	idxp[k2] = j;
    } else {

/*        Check if singular values are close enough to allow deflation. */

	if ((r__1 = d__[j] - d__[jprev], abs(r__1)) <= tol) {

/*           Deflation is possible. */

	    *s = z__[jprev];
	    *c__ = z__[j];

/*           Find sqrt(a**2+b**2) without overflow or */
/*           destructive underflow. */

	    tau = slapy2_(c__, s);
	    z__[j] = tau;
	    z__[jprev] = 0.f;
	    *c__ /= tau;
	    *s = -(*s) / tau;

/*           Record the appropriate Givens rotation */

	    if (*icompq == 1) {
		++(*givptr);
		idxjp = idxq[idx[jprev] + 1];
		idxj = idxq[idx[j] + 1];
		if (idxjp <= nlp1) {
		    --idxjp;
		}
		if (idxj <= nlp1) {
		    --idxj;
		}
		givcol[*givptr + (givcol_dim1 << 1)] = idxjp;
		givcol[*givptr + givcol_dim1] = idxj;
		givnum[*givptr + (givnum_dim1 << 1)] = *c__;
		givnum[*givptr + givnum_dim1] = *s;
	    }
	    srot_(&c__1, &vf[jprev], &c__1, &vf[j], &c__1, c__, s);
	    srot_(&c__1, &vl[jprev], &c__1, &vl[j], &c__1, c__, s);
	    --k2;
	    idxp[k2] = jprev;
	    jprev = j;
	} else {
	    ++(*k);
	    zw[*k] = z__[jprev];
	    dsigma[*k] = d__[jprev];
	    idxp[*k] = jprev;
	    jprev = j;
	}
    }
    goto L80;
L90:

/*     Record the last singular value. */

    ++(*k);
    zw[*k] = z__[jprev];
    dsigma[*k] = d__[jprev];
    idxp[*k] = jprev;

L100:

/*     Sort the singular values into DSIGMA. The singular values which */
/*     were not deflated go into the first K slots of DSIGMA, except */
/*     that DSIGMA(1) is treated separately. */

    i__1 = n;
    for (j = 2; j <= i__1; ++j) {
	jp = idxp[j];
	dsigma[j] = d__[jp];
	vfw[j] = vf[jp];
	vlw[j] = vl[jp];
/* L110: */
    }
    if (*icompq == 1) {
	i__1 = n;
	for (j = 2; j <= i__1; ++j) {
	    jp = idxp[j];
	    perm[j] = idxq[idx[jp] + 1];
	    if (perm[j] <= nlp1) {
		--perm[j];
	    }
/* L120: */
	}
    }

/*     The deflated singular values go back into the last N - K slots of */
/*     D. */

    i__1 = n - *k;
    scopy_(&i__1, &dsigma[*k + 1], &c__1, &d__[*k + 1], &c__1);

/*     Determine DSIGMA(1), DSIGMA(2), Z(1), VF(1), VL(1), VF(M), and */
/*     VL(M). */

    dsigma[1] = 0.f;
    hlftol = tol / 2.f;
    if (abs(dsigma[2]) <= hlftol) {
	dsigma[2] = hlftol;
    }
    if (m > n) {
	z__[1] = slapy2_(&z1, &z__[m]);
	if (z__[1] <= tol) {
	    *c__ = 1.f;
	    *s = 0.f;
	    z__[1] = tol;
	} else {
	    *c__ = z1 / z__[1];
	    *s = -z__[m] / z__[1];
	}
	srot_(&c__1, &vf[m], &c__1, &vf[1], &c__1, c__, s);
	srot_(&c__1, &vl[m], &c__1, &vl[1], &c__1, c__, s);
    } else {
	if (abs(z1) <= tol) {
	    z__[1] = tol;
	} else {
	    z__[1] = z1;
	}
    }

/*     Restore Z, VF, and VL. */

    i__1 = *k - 1;
    scopy_(&i__1, &zw[2], &c__1, &z__[2], &c__1);
    i__1 = n - 1;
    scopy_(&i__1, &vfw[2], &c__1, &vf[2], &c__1);
    i__1 = n - 1;
    scopy_(&i__1, &vlw[2], &c__1, &vl[2], &c__1);

    return;

/*     End of SLASD7 */

} /* slasd7_ */

