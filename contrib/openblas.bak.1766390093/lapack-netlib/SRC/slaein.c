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

/* > \brief \b SLAEIN computes a specified right or left eigenvector of an upper Hessenberg matrix by inverse 
iteration. */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download SLAEIN + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slaein.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slaein.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slaein.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE SLAEIN( RIGHTV, NOINIT, N, H, LDH, WR, WI, VR, VI, B, */
/*                          LDB, WORK, EPS3, SMLNUM, BIGNUM, INFO ) */

/*       LOGICAL            NOINIT, RIGHTV */
/*       INTEGER            INFO, LDB, LDH, N */
/*       REAL               BIGNUM, EPS3, SMLNUM, WI, WR */
/*       REAL               B( LDB, * ), H( LDH, * ), VI( * ), VR( * ), */
/*      $                   WORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > SLAEIN uses inverse iteration to find a right or left eigenvector */
/* > corresponding to the eigenvalue (WR,WI) of a real upper Hessenberg */
/* > matrix H. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] RIGHTV */
/* > \verbatim */
/* >          RIGHTV is LOGICAL */
/* >          = .TRUE. : compute right eigenvector; */
/* >          = .FALSE.: compute left eigenvector. */
/* > \endverbatim */
/* > */
/* > \param[in] NOINIT */
/* > \verbatim */
/* >          NOINIT is LOGICAL */
/* >          = .TRUE. : no initial vector supplied in (VR,VI). */
/* >          = .FALSE.: initial vector supplied in (VR,VI). */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrix H.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] H */
/* > \verbatim */
/* >          H is REAL array, dimension (LDH,N) */
/* >          The upper Hessenberg matrix H. */
/* > \endverbatim */
/* > */
/* > \param[in] LDH */
/* > \verbatim */
/* >          LDH is INTEGER */
/* >          The leading dimension of the array H.  LDH >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in] WR */
/* > \verbatim */
/* >          WR is REAL */
/* > \endverbatim */
/* > */
/* > \param[in] WI */
/* > \verbatim */
/* >          WI is REAL */
/* >          The real and imaginary parts of the eigenvalue of H whose */
/* >          corresponding right or left eigenvector is to be computed. */
/* > \endverbatim */
/* > */
/* > \param[in,out] VR */
/* > \verbatim */
/* >          VR is REAL array, dimension (N) */
/* > \endverbatim */
/* > */
/* > \param[in,out] VI */
/* > \verbatim */
/* >          VI is REAL array, dimension (N) */
/* >          On entry, if NOINIT = .FALSE. and WI = 0.0, VR must contain */
/* >          a real starting vector for inverse iteration using the real */
/* >          eigenvalue WR; if NOINIT = .FALSE. and WI.ne.0.0, VR and VI */
/* >          must contain the real and imaginary parts of a complex */
/* >          starting vector for inverse iteration using the complex */
/* >          eigenvalue (WR,WI); otherwise VR and VI need not be set. */
/* >          On exit, if WI = 0.0 (real eigenvalue), VR contains the */
/* >          computed real eigenvector; if WI.ne.0.0 (complex eigenvalue), */
/* >          VR and VI contain the real and imaginary parts of the */
/* >          computed complex eigenvector. The eigenvector is normalized */
/* >          so that the component of largest magnitude has magnitude 1; */
/* >          here the magnitude of a complex number (x,y) is taken to be */
/* >          |x| + |y|. */
/* >          VI is not referenced if WI = 0.0. */
/* > \endverbatim */
/* > */
/* > \param[out] B */
/* > \verbatim */
/* >          B is REAL array, dimension (LDB,N) */
/* > \endverbatim */
/* > */
/* > \param[in] LDB */
/* > \verbatim */
/* >          LDB is INTEGER */
/* >          The leading dimension of the array B.  LDB >= N+1. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is REAL array, dimension (N) */
/* > \endverbatim */
/* > */
/* > \param[in] EPS3 */
/* > \verbatim */
/* >          EPS3 is REAL */
/* >          A small machine-dependent value which is used to perturb */
/* >          close eigenvalues, and to replace zero pivots. */
/* > \endverbatim */
/* > */
/* > \param[in] SMLNUM */
/* > \verbatim */
/* >          SMLNUM is REAL */
/* >          A machine-dependent value close to the underflow threshold. */
/* > \endverbatim */
/* > */
/* > \param[in] BIGNUM */
/* > \verbatim */
/* >          BIGNUM is REAL */
/* >          A machine-dependent value close to the overflow threshold. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit */
/* >          = 1:  inverse iteration did not converge; VR is set to the */
/* >                last iterate, and so is VI if WI.ne.0.0. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup realOTHERauxiliary */

/*  ===================================================================== */
/* Subroutine */ void slaein_(logical *rightv, logical *noinit, integer *n, 
	real *h__, integer *ldh, real *wr, real *wi, real *vr, real *vi, real 
	*b, integer *ldb, real *work, real *eps3, real *smlnum, real *bignum, 
	integer *info)
{
    /* System generated locals */
    integer b_dim1, b_offset, h_dim1, h_offset, i__1, i__2, i__3, i__4;
    real r__1, r__2, r__3, r__4;

    /* Local variables */
    integer ierr;
    real temp, norm, vmax;
    extern real snrm2_(integer *, real *, integer *);
    integer i__, j;
    real scale, w, x, y;
    extern /* Subroutine */ void sscal_(integer *, real *, real *, integer *);
    char trans[1];
    real vcrit;
    extern real sasum_(integer *, real *, integer *);
    integer i1, i2, i3;
    real rootn, vnorm, w1;
    extern real slapy2_(real *, real *);
    real ei, ej, absbii, absbjj, xi, xr;
    extern integer isamax_(integer *, real *, integer *);
    extern /* Subroutine */ void sladiv_(real *, real *, real *, real *, real *
	    , real *);
    char normin[1];
    real nrmsml;
    extern /* Subroutine */ void slatrs_(char *, char *, char *, char *, 
	    integer *, real *, integer *, real *, real *, real *, integer *);
    real growto, rec;
    integer its;


/*  -- LAPACK auxiliary routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


    /* Parameter adjustments */
    h_dim1 = *ldh;
    h_offset = 1 + h_dim1 * 1;
    h__ -= h_offset;
    --vr;
    --vi;
    b_dim1 = *ldb;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    --work;

    /* Function Body */
    *info = 0;

/*     GROWTO is the threshold used in the acceptance test for an */
/*     eigenvector. */

    rootn = sqrt((real) (*n));
    growto = .1f / rootn;
/* Computing MAX */
    r__1 = 1.f, r__2 = *eps3 * rootn;
    nrmsml = f2cmax(r__1,r__2) * *smlnum;

/*     Form B = H - (WR,WI)*I (except that the subdiagonal elements and */
/*     the imaginary parts of the diagonal elements are not stored). */

    i__1 = *n;
    for (j = 1; j <= i__1; ++j) {
	i__2 = j - 1;
	for (i__ = 1; i__ <= i__2; ++i__) {
	    b[i__ + j * b_dim1] = h__[i__ + j * h_dim1];
/* L10: */
	}
	b[j + j * b_dim1] = h__[j + j * h_dim1] - *wr;
/* L20: */
    }

    if (*wi == 0.f) {

/*        Real eigenvalue. */

	if (*noinit) {

/*           Set initial vector. */

	    i__1 = *n;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		vr[i__] = *eps3;
/* L30: */
	    }
	} else {

/*           Scale supplied initial vector. */

	    vnorm = snrm2_(n, &vr[1], &c__1);
	    r__1 = *eps3 * rootn / f2cmax(vnorm,nrmsml);
	    sscal_(n, &r__1, &vr[1], &c__1);
	}

	if (*rightv) {

/*           LU decomposition with partial pivoting of B, replacing zero */
/*           pivots by EPS3. */

	    i__1 = *n - 1;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		ei = h__[i__ + 1 + i__ * h_dim1];
		if ((r__1 = b[i__ + i__ * b_dim1], abs(r__1)) < abs(ei)) {

/*                 Interchange rows and eliminate. */

		    x = b[i__ + i__ * b_dim1] / ei;
		    b[i__ + i__ * b_dim1] = ei;
		    i__2 = *n;
		    for (j = i__ + 1; j <= i__2; ++j) {
			temp = b[i__ + 1 + j * b_dim1];
			b[i__ + 1 + j * b_dim1] = b[i__ + j * b_dim1] - x * 
				temp;
			b[i__ + j * b_dim1] = temp;
/* L40: */
		    }
		} else {

/*                 Eliminate without interchange. */

		    if (b[i__ + i__ * b_dim1] == 0.f) {
			b[i__ + i__ * b_dim1] = *eps3;
		    }
		    x = ei / b[i__ + i__ * b_dim1];
		    if (x != 0.f) {
			i__2 = *n;
			for (j = i__ + 1; j <= i__2; ++j) {
			    b[i__ + 1 + j * b_dim1] -= x * b[i__ + j * b_dim1]
				    ;
/* L50: */
			}
		    }
		}
/* L60: */
	    }
	    if (b[*n + *n * b_dim1] == 0.f) {
		b[*n + *n * b_dim1] = *eps3;
	    }

	    *(unsigned char *)trans = 'N';

	} else {

/*           UL decomposition with partial pivoting of B, replacing zero */
/*           pivots by EPS3. */

	    for (j = *n; j >= 2; --j) {
		ej = h__[j + (j - 1) * h_dim1];
		if ((r__1 = b[j + j * b_dim1], abs(r__1)) < abs(ej)) {

/*                 Interchange columns and eliminate. */

		    x = b[j + j * b_dim1] / ej;
		    b[j + j * b_dim1] = ej;
		    i__1 = j - 1;
		    for (i__ = 1; i__ <= i__1; ++i__) {
			temp = b[i__ + (j - 1) * b_dim1];
			b[i__ + (j - 1) * b_dim1] = b[i__ + j * b_dim1] - x * 
				temp;
			b[i__ + j * b_dim1] = temp;
/* L70: */
		    }
		} else {

/*                 Eliminate without interchange. */

		    if (b[j + j * b_dim1] == 0.f) {
			b[j + j * b_dim1] = *eps3;
		    }
		    x = ej / b[j + j * b_dim1];
		    if (x != 0.f) {
			i__1 = j - 1;
			for (i__ = 1; i__ <= i__1; ++i__) {
			    b[i__ + (j - 1) * b_dim1] -= x * b[i__ + j * 
				    b_dim1];
/* L80: */
			}
		    }
		}
/* L90: */
	    }
	    if (b[b_dim1 + 1] == 0.f) {
		b[b_dim1 + 1] = *eps3;
	    }

	    *(unsigned char *)trans = 'T';

	}

	*(unsigned char *)normin = 'N';
	i__1 = *n;
	for (its = 1; its <= i__1; ++its) {

/*           Solve U*x = scale*v for a right eigenvector */
/*             or U**T*x = scale*v for a left eigenvector, */
/*           overwriting x on v. */

	    slatrs_("Upper", trans, "Nonunit", normin, n, &b[b_offset], ldb, &
		    vr[1], &scale, &work[1], &ierr);
	    *(unsigned char *)normin = 'Y';

/*           Test for sufficient growth in the norm of v. */

	    vnorm = sasum_(n, &vr[1], &c__1);
	    if (vnorm >= growto * scale) {
		goto L120;
	    }

/*           Choose new orthogonal starting vector and try again. */

	    temp = *eps3 / (rootn + 1.f);
	    vr[1] = *eps3;
	    i__2 = *n;
	    for (i__ = 2; i__ <= i__2; ++i__) {
		vr[i__] = temp;
/* L100: */
	    }
	    vr[*n - its + 1] -= *eps3 * rootn;
/* L110: */
	}

/*        Failure to find eigenvector in N iterations. */

	*info = 1;

L120:

/*        Normalize eigenvector. */

	i__ = isamax_(n, &vr[1], &c__1);
	r__2 = 1.f / (r__1 = vr[i__], abs(r__1));
	sscal_(n, &r__2, &vr[1], &c__1);
    } else {

/*        Complex eigenvalue. */

	if (*noinit) {

/*           Set initial vector. */

	    i__1 = *n;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		vr[i__] = *eps3;
		vi[i__] = 0.f;
/* L130: */
	    }
	} else {

/*           Scale supplied initial vector. */

	    r__1 = snrm2_(n, &vr[1], &c__1);
	    r__2 = snrm2_(n, &vi[1], &c__1);
	    norm = slapy2_(&r__1, &r__2);
	    rec = *eps3 * rootn / f2cmax(norm,nrmsml);
	    sscal_(n, &rec, &vr[1], &c__1);
	    sscal_(n, &rec, &vi[1], &c__1);
	}

	if (*rightv) {

/*           LU decomposition with partial pivoting of B, replacing zero */
/*           pivots by EPS3. */

/*           The imaginary part of the (i,j)-th element of U is stored in */
/*           B(j+1,i). */

	    b[b_dim1 + 2] = -(*wi);
	    i__1 = *n;
	    for (i__ = 2; i__ <= i__1; ++i__) {
		b[i__ + 1 + b_dim1] = 0.f;
/* L140: */
	    }

	    i__1 = *n - 1;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		absbii = slapy2_(&b[i__ + i__ * b_dim1], &b[i__ + 1 + i__ * 
			b_dim1]);
		ei = h__[i__ + 1 + i__ * h_dim1];
		if (absbii < abs(ei)) {

/*                 Interchange rows and eliminate. */

		    xr = b[i__ + i__ * b_dim1] / ei;
		    xi = b[i__ + 1 + i__ * b_dim1] / ei;
		    b[i__ + i__ * b_dim1] = ei;
		    b[i__ + 1 + i__ * b_dim1] = 0.f;
		    i__2 = *n;
		    for (j = i__ + 1; j <= i__2; ++j) {
			temp = b[i__ + 1 + j * b_dim1];
			b[i__ + 1 + j * b_dim1] = b[i__ + j * b_dim1] - xr * 
				temp;
			b[j + 1 + (i__ + 1) * b_dim1] = b[j + 1 + i__ * 
				b_dim1] - xi * temp;
			b[i__ + j * b_dim1] = temp;
			b[j + 1 + i__ * b_dim1] = 0.f;
/* L150: */
		    }
		    b[i__ + 2 + i__ * b_dim1] = -(*wi);
		    b[i__ + 1 + (i__ + 1) * b_dim1] -= xi * *wi;
		    b[i__ + 2 + (i__ + 1) * b_dim1] += xr * *wi;
		} else {

/*                 Eliminate without interchanging rows. */

		    if (absbii == 0.f) {
			b[i__ + i__ * b_dim1] = *eps3;
			b[i__ + 1 + i__ * b_dim1] = 0.f;
			absbii = *eps3;
		    }
		    ei = ei / absbii / absbii;
		    xr = b[i__ + i__ * b_dim1] * ei;
		    xi = -b[i__ + 1 + i__ * b_dim1] * ei;
		    i__2 = *n;
		    for (j = i__ + 1; j <= i__2; ++j) {
			b[i__ + 1 + j * b_dim1] = b[i__ + 1 + j * b_dim1] - 
				xr * b[i__ + j * b_dim1] + xi * b[j + 1 + i__ 
				* b_dim1];
			b[j + 1 + (i__ + 1) * b_dim1] = -xr * b[j + 1 + i__ * 
				b_dim1] - xi * b[i__ + j * b_dim1];
/* L160: */
		    }
		    b[i__ + 2 + (i__ + 1) * b_dim1] -= *wi;
		}

/*              Compute 1-norm of offdiagonal elements of i-th row. */

		i__2 = *n - i__;
		i__3 = *n - i__;
		work[i__] = sasum_(&i__2, &b[i__ + (i__ + 1) * b_dim1], ldb) 
			+ sasum_(&i__3, &b[i__ + 2 + i__ * b_dim1], &c__1);
/* L170: */
	    }
	    if (b[*n + *n * b_dim1] == 0.f && b[*n + 1 + *n * b_dim1] == 0.f) 
		    {
		b[*n + *n * b_dim1] = *eps3;
	    }
	    work[*n] = 0.f;

	    i1 = *n;
	    i2 = 1;
	    i3 = -1;
	} else {

/*           UL decomposition with partial pivoting of conjg(B), */
/*           replacing zero pivots by EPS3. */

/*           The imaginary part of the (i,j)-th element of U is stored in */
/*           B(j+1,i). */

	    b[*n + 1 + *n * b_dim1] = *wi;
	    i__1 = *n - 1;
	    for (j = 1; j <= i__1; ++j) {
		b[*n + 1 + j * b_dim1] = 0.f;
/* L180: */
	    }

	    for (j = *n; j >= 2; --j) {
		ej = h__[j + (j - 1) * h_dim1];
		absbjj = slapy2_(&b[j + j * b_dim1], &b[j + 1 + j * b_dim1]);
		if (absbjj < abs(ej)) {

/*                 Interchange columns and eliminate */

		    xr = b[j + j * b_dim1] / ej;
		    xi = b[j + 1 + j * b_dim1] / ej;
		    b[j + j * b_dim1] = ej;
		    b[j + 1 + j * b_dim1] = 0.f;
		    i__1 = j - 1;
		    for (i__ = 1; i__ <= i__1; ++i__) {
			temp = b[i__ + (j - 1) * b_dim1];
			b[i__ + (j - 1) * b_dim1] = b[i__ + j * b_dim1] - xr *
				 temp;
			b[j + i__ * b_dim1] = b[j + 1 + i__ * b_dim1] - xi * 
				temp;
			b[i__ + j * b_dim1] = temp;
			b[j + 1 + i__ * b_dim1] = 0.f;
/* L190: */
		    }
		    b[j + 1 + (j - 1) * b_dim1] = *wi;
		    b[j - 1 + (j - 1) * b_dim1] += xi * *wi;
		    b[j + (j - 1) * b_dim1] -= xr * *wi;
		} else {

/*                 Eliminate without interchange. */

		    if (absbjj == 0.f) {
			b[j + j * b_dim1] = *eps3;
			b[j + 1 + j * b_dim1] = 0.f;
			absbjj = *eps3;
		    }
		    ej = ej / absbjj / absbjj;
		    xr = b[j + j * b_dim1] * ej;
		    xi = -b[j + 1 + j * b_dim1] * ej;
		    i__1 = j - 1;
		    for (i__ = 1; i__ <= i__1; ++i__) {
			b[i__ + (j - 1) * b_dim1] = b[i__ + (j - 1) * b_dim1] 
				- xr * b[i__ + j * b_dim1] + xi * b[j + 1 + 
				i__ * b_dim1];
			b[j + i__ * b_dim1] = -xr * b[j + 1 + i__ * b_dim1] - 
				xi * b[i__ + j * b_dim1];
/* L200: */
		    }
		    b[j + (j - 1) * b_dim1] += *wi;
		}

/*              Compute 1-norm of offdiagonal elements of j-th column. */

		i__1 = j - 1;
		i__2 = j - 1;
		work[j] = sasum_(&i__1, &b[j * b_dim1 + 1], &c__1) + sasum_(&
			i__2, &b[j + 1 + b_dim1], ldb);
/* L210: */
	    }
	    if (b[b_dim1 + 1] == 0.f && b[b_dim1 + 2] == 0.f) {
		b[b_dim1 + 1] = *eps3;
	    }
	    work[1] = 0.f;

	    i1 = 1;
	    i2 = *n;
	    i3 = 1;
	}

	i__1 = *n;
	for (its = 1; its <= i__1; ++its) {
	    scale = 1.f;
	    vmax = 1.f;
	    vcrit = *bignum;

/*           Solve U*(xr,xi) = scale*(vr,vi) for a right eigenvector, */
/*             or U**T*(xr,xi) = scale*(vr,vi) for a left eigenvector, */
/*           overwriting (xr,xi) on (vr,vi). */

	    i__2 = i2;
	    i__3 = i3;
	    for (i__ = i1; i__3 < 0 ? i__ >= i__2 : i__ <= i__2; i__ += i__3) 
		    {

		if (work[i__] > vcrit) {
		    rec = 1.f / vmax;
		    sscal_(n, &rec, &vr[1], &c__1);
		    sscal_(n, &rec, &vi[1], &c__1);
		    scale *= rec;
		    vmax = 1.f;
		    vcrit = *bignum;
		}

		xr = vr[i__];
		xi = vi[i__];
		if (*rightv) {
		    i__4 = *n;
		    for (j = i__ + 1; j <= i__4; ++j) {
			xr = xr - b[i__ + j * b_dim1] * vr[j] + b[j + 1 + i__ 
				* b_dim1] * vi[j];
			xi = xi - b[i__ + j * b_dim1] * vi[j] - b[j + 1 + i__ 
				* b_dim1] * vr[j];
/* L220: */
		    }
		} else {
		    i__4 = i__ - 1;
		    for (j = 1; j <= i__4; ++j) {
			xr = xr - b[j + i__ * b_dim1] * vr[j] + b[i__ + 1 + j 
				* b_dim1] * vi[j];
			xi = xi - b[j + i__ * b_dim1] * vi[j] - b[i__ + 1 + j 
				* b_dim1] * vr[j];
/* L230: */
		    }
		}

		w = (r__1 = b[i__ + i__ * b_dim1], abs(r__1)) + (r__2 = b[i__ 
			+ 1 + i__ * b_dim1], abs(r__2));
		if (w > *smlnum) {
		    if (w < 1.f) {
			w1 = abs(xr) + abs(xi);
			if (w1 > w * *bignum) {
			    rec = 1.f / w1;
			    sscal_(n, &rec, &vr[1], &c__1);
			    sscal_(n, &rec, &vi[1], &c__1);
			    xr = vr[i__];
			    xi = vi[i__];
			    scale *= rec;
			    vmax *= rec;
			}
		    }

/*                 Divide by diagonal element of B. */

		    sladiv_(&xr, &xi, &b[i__ + i__ * b_dim1], &b[i__ + 1 + 
			    i__ * b_dim1], &vr[i__], &vi[i__]);
/* Computing MAX */
		    r__3 = (r__1 = vr[i__], abs(r__1)) + (r__2 = vi[i__], abs(
			    r__2));
		    vmax = f2cmax(r__3,vmax);
		    vcrit = *bignum / vmax;
		} else {
		    i__4 = *n;
		    for (j = 1; j <= i__4; ++j) {
			vr[j] = 0.f;
			vi[j] = 0.f;
/* L240: */
		    }
		    vr[i__] = 1.f;
		    vi[i__] = 1.f;
		    scale = 0.f;
		    vmax = 1.f;
		    vcrit = *bignum;
		}
/* L250: */
	    }

/*           Test for sufficient growth in the norm of (VR,VI). */

	    vnorm = sasum_(n, &vr[1], &c__1) + sasum_(n, &vi[1], &c__1);
	    if (vnorm >= growto * scale) {
		goto L280;
	    }

/*           Choose a new orthogonal starting vector and try again. */

	    y = *eps3 / (rootn + 1.f);
	    vr[1] = *eps3;
	    vi[1] = 0.f;

	    i__3 = *n;
	    for (i__ = 2; i__ <= i__3; ++i__) {
		vr[i__] = y;
		vi[i__] = 0.f;
/* L260: */
	    }
	    vr[*n - its + 1] -= *eps3 * rootn;
/* L270: */
	}

/*        Failure to find eigenvector in N iterations */

	*info = 1;

L280:

/*        Normalize eigenvector. */

	vnorm = 0.f;
	i__1 = *n;
	for (i__ = 1; i__ <= i__1; ++i__) {
/* Computing MAX */
	    r__3 = vnorm, r__4 = (r__1 = vr[i__], abs(r__1)) + (r__2 = vi[i__]
		    , abs(r__2));
	    vnorm = f2cmax(r__3,r__4);
/* L290: */
	}
	r__1 = 1.f / vnorm;
	sscal_(n, &r__1, &vr[1], &c__1);
	r__1 = 1.f / vnorm;
	sscal_(n, &r__1, &vi[1], &c__1);

    }

    return;

/*     End of SLAEIN */

} /* slaein_ */

