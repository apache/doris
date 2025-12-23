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

static integer c_n1 = -1;
static logical c_false = FALSE_;

/* > \brief \b ZUNCSD */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download ZUNCSD + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zuncsd.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zuncsd.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zuncsd.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*        SUBROUTINE ZUNCSD( JOBU1, JOBU2, JOBV1T, JOBV2T, TRANS, */
/*                                    SIGNS, M, P, Q, X11, LDX11, X12, */
/*                                    LDX12, X21, LDX21, X22, LDX22, THETA, */
/*                                    U1, LDU1, U2, LDU2, V1T, LDV1T, V2T, */
/*                                    LDV2T, WORK, LWORK, RWORK, LRWORK, */
/*                                    IWORK, INFO ) */

/*       CHARACTER          JOBU1, JOBU2, JOBV1T, JOBV2T, SIGNS, TRANS */
/*       INTEGER            INFO, LDU1, LDU2, LDV1T, LDV2T, LDX11, LDX12, */
/*      $                   LDX21, LDX22, LRWORK, LWORK, M, P, Q */
/*       INTEGER            IWORK( * ) */
/*       DOUBLE PRECISION   THETA( * ) */
/*       DOUBLE PRECISION   RWORK( * ) */
/*       COMPLEX*16         U1( LDU1, * ), U2( LDU2, * ), V1T( LDV1T, * ), */
/*      $                   V2T( LDV2T, * ), WORK( * ), X11( LDX11, * ), */
/*      $                   X12( LDX12, * ), X21( LDX21, * ), X22( LDX22, */
/*      $                   * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > ZUNCSD computes the CS decomposition of an M-by-M partitioned */
/* > unitary matrix X: */
/* > */
/* >                                 [  I  0  0 |  0  0  0 ] */
/* >                                 [  0  C  0 |  0 -S  0 ] */
/* >     [ X11 | X12 ]   [ U1 |    ] [  0  0  0 |  0  0 -I ] [ V1 |    ]**H */
/* > X = [-----------] = [---------] [---------------------] [---------]   . */
/* >     [ X21 | X22 ]   [    | U2 ] [  0  0  0 |  I  0  0 ] [    | V2 ] */
/* >                                 [  0  S  0 |  0  C  0 ] */
/* >                                 [  0  0  I |  0  0  0 ] */
/* > */
/* > X11 is P-by-Q. The unitary matrices U1, U2, V1, and V2 are P-by-P, */
/* > (M-P)-by-(M-P), Q-by-Q, and (M-Q)-by-(M-Q), respectively. C and S are */
/* > R-by-R nonnegative diagonal matrices satisfying C^2 + S^2 = I, in */
/* > which R = MIN(P,M-P,Q,M-Q). */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] JOBU1 */
/* > \verbatim */
/* >          JOBU1 is CHARACTER */
/* >          = 'Y':      U1 is computed; */
/* >          otherwise:  U1 is not computed. */
/* > \endverbatim */
/* > */
/* > \param[in] JOBU2 */
/* > \verbatim */
/* >          JOBU2 is CHARACTER */
/* >          = 'Y':      U2 is computed; */
/* >          otherwise:  U2 is not computed. */
/* > \endverbatim */
/* > */
/* > \param[in] JOBV1T */
/* > \verbatim */
/* >          JOBV1T is CHARACTER */
/* >          = 'Y':      V1T is computed; */
/* >          otherwise:  V1T is not computed. */
/* > \endverbatim */
/* > */
/* > \param[in] JOBV2T */
/* > \verbatim */
/* >          JOBV2T is CHARACTER */
/* >          = 'Y':      V2T is computed; */
/* >          otherwise:  V2T is not computed. */
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
/* > \param[in] SIGNS */
/* > \verbatim */
/* >          SIGNS is CHARACTER */
/* >          = 'O':      The lower-left block is made nonpositive (the */
/* >                      "other" convention); */
/* >          otherwise:  The upper-right block is made nonpositive (the */
/* >                      "default" convention). */
/* > \endverbatim */
/* > */
/* > \param[in] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >          The number of rows and columns in X. */
/* > \endverbatim */
/* > */
/* > \param[in] P */
/* > \verbatim */
/* >          P is INTEGER */
/* >          The number of rows in X11 and X12. 0 <= P <= M. */
/* > \endverbatim */
/* > */
/* > \param[in] Q */
/* > \verbatim */
/* >          Q is INTEGER */
/* >          The number of columns in X11 and X21. 0 <= Q <= M. */
/* > \endverbatim */
/* > */
/* > \param[in,out] X11 */
/* > \verbatim */
/* >          X11 is COMPLEX*16 array, dimension (LDX11,Q) */
/* >          On entry, part of the unitary matrix whose CSD is desired. */
/* > \endverbatim */
/* > */
/* > \param[in] LDX11 */
/* > \verbatim */
/* >          LDX11 is INTEGER */
/* >          The leading dimension of X11. LDX11 >= MAX(1,P). */
/* > \endverbatim */
/* > */
/* > \param[in,out] X12 */
/* > \verbatim */
/* >          X12 is COMPLEX*16 array, dimension (LDX12,M-Q) */
/* >          On entry, part of the unitary matrix whose CSD is desired. */
/* > \endverbatim */
/* > */
/* > \param[in] LDX12 */
/* > \verbatim */
/* >          LDX12 is INTEGER */
/* >          The leading dimension of X12. LDX12 >= MAX(1,P). */
/* > \endverbatim */
/* > */
/* > \param[in,out] X21 */
/* > \verbatim */
/* >          X21 is COMPLEX*16 array, dimension (LDX21,Q) */
/* >          On entry, part of the unitary matrix whose CSD is desired. */
/* > \endverbatim */
/* > */
/* > \param[in] LDX21 */
/* > \verbatim */
/* >          LDX21 is INTEGER */
/* >          The leading dimension of X11. LDX21 >= MAX(1,M-P). */
/* > \endverbatim */
/* > */
/* > \param[in,out] X22 */
/* > \verbatim */
/* >          X22 is COMPLEX*16 array, dimension (LDX22,M-Q) */
/* >          On entry, part of the unitary matrix whose CSD is desired. */
/* > \endverbatim */
/* > */
/* > \param[in] LDX22 */
/* > \verbatim */
/* >          LDX22 is INTEGER */
/* >          The leading dimension of X11. LDX22 >= MAX(1,M-P). */
/* > \endverbatim */
/* > */
/* > \param[out] THETA */
/* > \verbatim */
/* >          THETA is DOUBLE PRECISION array, dimension (R), in which R = */
/* >          MIN(P,M-P,Q,M-Q). */
/* >          C = DIAG( COS(THETA(1)), ... , COS(THETA(R)) ) and */
/* >          S = DIAG( SIN(THETA(1)), ... , SIN(THETA(R)) ). */
/* > \endverbatim */
/* > */
/* > \param[out] U1 */
/* > \verbatim */
/* >          U1 is COMPLEX*16 array, dimension (LDU1,P) */
/* >          If JOBU1 = 'Y', U1 contains the P-by-P unitary matrix U1. */
/* > \endverbatim */
/* > */
/* > \param[in] LDU1 */
/* > \verbatim */
/* >          LDU1 is INTEGER */
/* >          The leading dimension of U1. If JOBU1 = 'Y', LDU1 >= */
/* >          MAX(1,P). */
/* > \endverbatim */
/* > */
/* > \param[out] U2 */
/* > \verbatim */
/* >          U2 is COMPLEX*16 array, dimension (LDU2,M-P) */
/* >          If JOBU2 = 'Y', U2 contains the (M-P)-by-(M-P) unitary */
/* >          matrix U2. */
/* > \endverbatim */
/* > */
/* > \param[in] LDU2 */
/* > \verbatim */
/* >          LDU2 is INTEGER */
/* >          The leading dimension of U2. If JOBU2 = 'Y', LDU2 >= */
/* >          MAX(1,M-P). */
/* > \endverbatim */
/* > */
/* > \param[out] V1T */
/* > \verbatim */
/* >          V1T is COMPLEX*16 array, dimension (LDV1T,Q) */
/* >          If JOBV1T = 'Y', V1T contains the Q-by-Q matrix unitary */
/* >          matrix V1**H. */
/* > \endverbatim */
/* > */
/* > \param[in] LDV1T */
/* > \verbatim */
/* >          LDV1T is INTEGER */
/* >          The leading dimension of V1T. If JOBV1T = 'Y', LDV1T >= */
/* >          MAX(1,Q). */
/* > \endverbatim */
/* > */
/* > \param[out] V2T */
/* > \verbatim */
/* >          V2T is COMPLEX*16 array, dimension (LDV2T,M-Q) */
/* >          If JOBV2T = 'Y', V2T contains the (M-Q)-by-(M-Q) unitary */
/* >          matrix V2**H. */
/* > \endverbatim */
/* > */
/* > \param[in] LDV2T */
/* > \verbatim */
/* >          LDV2T is INTEGER */
/* >          The leading dimension of V2T. If JOBV2T = 'Y', LDV2T >= */
/* >          MAX(1,M-Q). */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is COMPLEX*16 array, dimension (MAX(1,LWORK)) */
/* >          On exit, if INFO = 0, WORK(1) returns the optimal LWORK. */
/* > \endverbatim */
/* > */
/* > \param[in] LWORK */
/* > \verbatim */
/* >          LWORK is INTEGER */
/* >          The dimension of the array WORK. */
/* > */
/* >          If LWORK = -1, then a workspace query is assumed; the routine */
/* >          only calculates the optimal size of the WORK array, returns */
/* >          this value as the first entry of the work array, and no error */
/* >          message related to LWORK is issued by XERBLA. */
/* > \endverbatim */
/* > */
/* > \param[out] RWORK */
/* > \verbatim */
/* >          RWORK is DOUBLE PRECISION array, dimension MAX(1,LRWORK) */
/* >          On exit, if INFO = 0, RWORK(1) returns the optimal LRWORK. */
/* >          If INFO > 0 on exit, RWORK(2:R) contains the values PHI(1), */
/* >          ..., PHI(R-1) that, together with THETA(1), ..., THETA(R), */
/* >          define the matrix in intermediate bidiagonal-block form */
/* >          remaining after nonconvergence. INFO specifies the number */
/* >          of nonzero PHI's. */
/* > \endverbatim */
/* > */
/* > \param[in] LRWORK */
/* > \verbatim */
/* >          LRWORK is INTEGER */
/* >          The dimension of the array RWORK. */
/* > */
/* >          If LRWORK = -1, then a workspace query is assumed; the routine */
/* >          only calculates the optimal size of the RWORK array, returns */
/* >          this value as the first entry of the work array, and no error */
/* >          message related to LRWORK is issued by XERBLA. */
/* > \endverbatim */
/* > */
/* > \param[out] IWORK */
/* > \verbatim */
/* >          IWORK is INTEGER array, dimension (M-MIN(P,M-P,Q,M-Q)) */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit. */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value. */
/* >          > 0:  ZBBCSD did not converge. See the description of RWORK */
/* >                above for details. */
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

/* > \date June 2017 */

/* > \ingroup complex16OTHERcomputational */

/*  ===================================================================== */
/* Subroutine */ void zuncsd_(char *jobu1, char *jobu2, char *jobv1t, char *
	jobv2t, char *trans, char *signs, integer *m, integer *p, integer *q, 
	doublecomplex *x11, integer *ldx11, doublecomplex *x12, integer *
	ldx12, doublecomplex *x21, integer *ldx21, doublecomplex *x22, 
	integer *ldx22, doublereal *theta, doublecomplex *u1, integer *ldu1, 
	doublecomplex *u2, integer *ldu2, doublecomplex *v1t, integer *ldv1t, 
	doublecomplex *v2t, integer *ldv2t, doublecomplex *work, integer *
	lwork, doublereal *rwork, integer *lrwork, integer *iwork, integer *
	info)
{
    /* System generated locals */
    integer u1_dim1, u1_offset, u2_dim1, u2_offset, v1t_dim1, v1t_offset, 
	    v2t_dim1, v2t_offset, x11_dim1, x11_offset, x12_dim1, x12_offset, 
	    x21_dim1, x21_offset, x22_dim1, x22_offset, i__1, i__2, i__3, 
	    i__4, i__5, i__6;

    /* Local variables */
    integer ib11d, ib11e, ib12d, ib12e, ib21d, ib21e, ib22d, ib22e, iphi;
    logical colmajor;
    integer lworkmin;
    logical defaultsigns;
    integer lworkopt, i__, j;
    extern logical lsame_(char *, char *);
    integer childinfo, p1, q1, lbbcsdworkmin, itaup1, itaup2, itauq1, itauq2, 
	    lorbdbworkmin, lrworkmin, lbbcsdworkopt;
    logical wantu1, wantu2;
    integer lrworkopt, ibbcsd, lorbdbworkopt, iorbdb, lorglqworkmin;
    extern /* Subroutine */ void zbbcsd_(char *, char *, char *, char *, char *
	    , integer *, integer *, integer *, doublereal *, doublereal *, 
	    doublecomplex *, integer *, doublecomplex *, integer *, 
	    doublecomplex *, integer *, doublecomplex *, integer *, 
	    doublereal *, doublereal *, doublereal *, doublereal *, 
	    doublereal *, doublereal *, doublereal *, doublereal *, 
	    doublereal *, integer *, integer *);
    integer lorgqrworkmin;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    integer lorglqworkopt;
    extern /* Subroutine */ void zunbdb_(char *, char *, integer *, integer *, 
	    integer *, doublecomplex *, integer *, doublecomplex *, integer *,
	     doublecomplex *, integer *, doublecomplex *, integer *, 
	    doublereal *, doublereal *, doublecomplex *, doublecomplex *, 
	    doublecomplex *, doublecomplex *, doublecomplex *, integer *, 
	    integer *);
    integer lorgqrworkopt, iorglq;
    extern /* Subroutine */ void zlacpy_(char *, integer *, integer *, 
	    doublecomplex *, integer *, doublecomplex *, integer *);
    integer iorgqr;
    extern /* Subroutine */ void zlapmr_(logical *, integer *, integer *, 
	    doublecomplex *, integer *, integer *);
    char signst[1];
    extern /* Subroutine */ void zlapmt_(logical *, integer *, integer *, 
	    doublecomplex *, integer *, integer *);
    char transt[1];
    integer lbbcsdwork;
    logical lquery;
    extern /* Subroutine */ void zunglq_(integer *, integer *, integer *, 
	    doublecomplex *, integer *, doublecomplex *, doublecomplex *, 
	    integer *, integer *);
    integer lorbdbwork;
    extern /* Subroutine */ void zungqr_(integer *, integer *, integer *, 
	    doublecomplex *, integer *, doublecomplex *, doublecomplex *, 
	    integer *, integer *);
    integer lorglqwork, lorgqrwork;
    logical wantv1t, wantv2t, lrquery;


/*  -- LAPACK computational routine (version 3.7.1) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     June 2017 */


/*  =================================================================== */


/*     Test input arguments */

    /* Parameter adjustments */
    x11_dim1 = *ldx11;
    x11_offset = 1 + x11_dim1 * 1;
    x11 -= x11_offset;
    x12_dim1 = *ldx12;
    x12_offset = 1 + x12_dim1 * 1;
    x12 -= x12_offset;
    x21_dim1 = *ldx21;
    x21_offset = 1 + x21_dim1 * 1;
    x21 -= x21_offset;
    x22_dim1 = *ldx22;
    x22_offset = 1 + x22_dim1 * 1;
    x22 -= x22_offset;
    --theta;
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
    --work;
    --rwork;
    --iwork;

    /* Function Body */
    *info = 0;
    wantu1 = lsame_(jobu1, "Y");
    wantu2 = lsame_(jobu2, "Y");
    wantv1t = lsame_(jobv1t, "Y");
    wantv2t = lsame_(jobv2t, "Y");
    colmajor = ! lsame_(trans, "T");
    defaultsigns = ! lsame_(signs, "O");
    lquery = *lwork == -1;
    lrquery = *lrwork == -1;
    if (*m < 0) {
	*info = -7;
    } else if (*p < 0 || *p > *m) {
	*info = -8;
    } else if (*q < 0 || *q > *m) {
	*info = -9;
    } else if (colmajor && *ldx11 < f2cmax(1,*p)) {
	*info = -11;
    } else if (! colmajor && *ldx11 < f2cmax(1,*q)) {
	*info = -11;
    } else if (colmajor && *ldx12 < f2cmax(1,*p)) {
	*info = -13;
    } else /* if(complicated condition) */ {
/* Computing MAX */
	i__1 = 1, i__2 = *m - *q;
	if (! colmajor && *ldx12 < f2cmax(i__1,i__2)) {
	    *info = -13;
	} else /* if(complicated condition) */ {
/* Computing MAX */
	    i__1 = 1, i__2 = *m - *p;
	    if (colmajor && *ldx21 < f2cmax(i__1,i__2)) {
		*info = -15;
	    } else if (! colmajor && *ldx21 < f2cmax(1,*q)) {
		*info = -15;
	    } else /* if(complicated condition) */ {
/* Computing MAX */
		i__1 = 1, i__2 = *m - *p;
		if (colmajor && *ldx22 < f2cmax(i__1,i__2)) {
		    *info = -17;
		} else /* if(complicated condition) */ {
/* Computing MAX */
		    i__1 = 1, i__2 = *m - *q;
		    if (! colmajor && *ldx22 < f2cmax(i__1,i__2)) {
			*info = -17;
		    } else if (wantu1 && *ldu1 < *p) {
			*info = -20;
		    } else if (wantu2 && *ldu2 < *m - *p) {
			*info = -22;
		    } else if (wantv1t && *ldv1t < *q) {
			*info = -24;
		    } else if (wantv2t && *ldv2t < *m - *q) {
			*info = -26;
		    }
		}
	    }
	}
    }

/*     Work with transpose if convenient */

/* Computing MIN */
    i__1 = *p, i__2 = *m - *p;
/* Computing MIN */
    i__3 = *q, i__4 = *m - *q;
    if (*info == 0 && f2cmin(i__1,i__2) < f2cmin(i__3,i__4)) {
	if (colmajor) {
	    *(unsigned char *)transt = 'T';
	} else {
	    *(unsigned char *)transt = 'N';
	}
	if (defaultsigns) {
	    *(unsigned char *)signst = 'O';
	} else {
	    *(unsigned char *)signst = 'D';
	}
	zuncsd_(jobv1t, jobv2t, jobu1, jobu2, transt, signst, m, q, p, &x11[
		x11_offset], ldx11, &x21[x21_offset], ldx21, &x12[x12_offset],
		 ldx12, &x22[x22_offset], ldx22, &theta[1], &v1t[v1t_offset], 
		ldv1t, &v2t[v2t_offset], ldv2t, &u1[u1_offset], ldu1, &u2[
		u2_offset], ldu2, &work[1], lwork, &rwork[1], lrwork, &iwork[
		1], info);
	return;
    }

/*     Work with permutation [ 0 I; I 0 ] * X * [ 0 I; I 0 ] if */
/*     convenient */

    if (*info == 0 && *m - *q < *q) {
	if (defaultsigns) {
	    *(unsigned char *)signst = 'O';
	} else {
	    *(unsigned char *)signst = 'D';
	}
	i__1 = *m - *p;
	i__2 = *m - *q;
	zuncsd_(jobu2, jobu1, jobv2t, jobv1t, trans, signst, m, &i__1, &i__2, 
		&x22[x22_offset], ldx22, &x21[x21_offset], ldx21, &x12[
		x12_offset], ldx12, &x11[x11_offset], ldx11, &theta[1], &u2[
		u2_offset], ldu2, &u1[u1_offset], ldu1, &v2t[v2t_offset], 
		ldv2t, &v1t[v1t_offset], ldv1t, &work[1], lwork, &rwork[1], 
		lrwork, &iwork[1], info);
	return;
    }

/*     Compute workspace */

    if (*info == 0) {

/*        Real workspace */

	iphi = 2;
/* Computing MAX */
	i__1 = 1, i__2 = *q - 1;
	ib11d = iphi + f2cmax(i__1,i__2);
	ib11e = ib11d + f2cmax(1,*q);
/* Computing MAX */
	i__1 = 1, i__2 = *q - 1;
	ib12d = ib11e + f2cmax(i__1,i__2);
	ib12e = ib12d + f2cmax(1,*q);
/* Computing MAX */
	i__1 = 1, i__2 = *q - 1;
	ib21d = ib12e + f2cmax(i__1,i__2);
	ib21e = ib21d + f2cmax(1,*q);
/* Computing MAX */
	i__1 = 1, i__2 = *q - 1;
	ib22d = ib21e + f2cmax(i__1,i__2);
	ib22e = ib22d + f2cmax(1,*q);
/* Computing MAX */
	i__1 = 1, i__2 = *q - 1;
	ibbcsd = ib22e + f2cmax(i__1,i__2);
	zbbcsd_(jobu1, jobu2, jobv1t, jobv2t, trans, m, p, q, &theta[1], &
		theta[1], &u1[u1_offset], ldu1, &u2[u2_offset], ldu2, &v1t[
		v1t_offset], ldv1t, &v2t[v2t_offset], ldv2t, &theta[1], &
		theta[1], &theta[1], &theta[1], &theta[1], &theta[1], &theta[
		1], &theta[1], &rwork[1], &c_n1, &childinfo);
	lbbcsdworkopt = (integer) rwork[1];
	lbbcsdworkmin = lbbcsdworkopt;
	lrworkopt = ibbcsd + lbbcsdworkopt - 1;
	lrworkmin = ibbcsd + lbbcsdworkmin - 1;
	rwork[1] = (doublereal) lrworkopt;

/*        Complex workspace */

	itaup1 = 2;
	itaup2 = itaup1 + f2cmax(1,*p);
/* Computing MAX */
	i__1 = 1, i__2 = *m - *p;
	itauq1 = itaup2 + f2cmax(i__1,i__2);
	itauq2 = itauq1 + f2cmax(1,*q);
/* Computing MAX */
	i__1 = 1, i__2 = *m - *q;
	iorgqr = itauq2 + f2cmax(i__1,i__2);
	i__1 = *m - *q;
	i__2 = *m - *q;
	i__3 = *m - *q;
/* Computing MAX */
	i__5 = 1, i__6 = *m - *q;
	i__4 = f2cmax(i__5,i__6);
	zungqr_(&i__1, &i__2, &i__3, &u1[u1_offset], &i__4, &u1[u1_offset], &
		work[1], &c_n1, &childinfo);
	lorgqrworkopt = (integer) work[1].r;
/* Computing MAX */
	i__1 = 1, i__2 = *m - *q;
	lorgqrworkmin = f2cmax(i__1,i__2);
/* Computing MAX */
	i__1 = 1, i__2 = *m - *q;
	iorglq = itauq2 + f2cmax(i__1,i__2);
	i__1 = *m - *q;
	i__2 = *m - *q;
	i__3 = *m - *q;
/* Computing MAX */
	i__5 = 1, i__6 = *m - *q;
	i__4 = f2cmax(i__5,i__6);
	zunglq_(&i__1, &i__2, &i__3, &u1[u1_offset], &i__4, &u1[u1_offset], &
		work[1], &c_n1, &childinfo);
	lorglqworkopt = (integer) work[1].r;
/* Computing MAX */
	i__1 = 1, i__2 = *m - *q;
	lorglqworkmin = f2cmax(i__1,i__2);
/* Computing MAX */
	i__1 = 1, i__2 = *m - *q;
	iorbdb = itauq2 + f2cmax(i__1,i__2);
	zunbdb_(trans, signs, m, p, q, &x11[x11_offset], ldx11, &x12[
		x12_offset], ldx12, &x21[x21_offset], ldx21, &x22[x22_offset],
		 ldx22, &theta[1], &theta[1], &u1[u1_offset], &u2[u2_offset], 
		&v1t[v1t_offset], &v2t[v2t_offset], &work[1], &c_n1, &
		childinfo);
	lorbdbworkopt = (integer) work[1].r;
	lorbdbworkmin = lorbdbworkopt;
/* Computing MAX */
	i__1 = iorgqr + lorgqrworkopt, i__2 = iorglq + lorglqworkopt, i__1 = 
		f2cmax(i__1,i__2), i__2 = iorbdb + lorbdbworkopt;
	lworkopt = f2cmax(i__1,i__2) - 1;
/* Computing MAX */
	i__1 = iorgqr + lorgqrworkmin, i__2 = iorglq + lorglqworkmin, i__1 = 
		f2cmax(i__1,i__2), i__2 = iorbdb + lorbdbworkmin;
	lworkmin = f2cmax(i__1,i__2) - 1;
	i__1 = f2cmax(lworkopt,lworkmin);
	work[1].r = (doublereal) i__1, work[1].i = 0.;

	if (*lwork < lworkmin && ! (lquery || lrquery)) {
	    *info = -22;
	} else if (*lrwork < lrworkmin && ! (lquery || lrquery)) {
	    *info = -24;
	} else {
	    lorgqrwork = *lwork - iorgqr + 1;
	    lorglqwork = *lwork - iorglq + 1;
	    lorbdbwork = *lwork - iorbdb + 1;
	    lbbcsdwork = *lrwork - ibbcsd + 1;
	}
    }

/*     Abort if any illegal arguments */

    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("ZUNCSD", &i__1, (ftnlen)6);
	return;
    } else if (lquery || lrquery) {
	return;
    }

/*     Transform to bidiagonal block form */

    zunbdb_(trans, signs, m, p, q, &x11[x11_offset], ldx11, &x12[x12_offset], 
	    ldx12, &x21[x21_offset], ldx21, &x22[x22_offset], ldx22, &theta[1]
	    , &rwork[iphi], &work[itaup1], &work[itaup2], &work[itauq1], &
	    work[itauq2], &work[iorbdb], &lorbdbwork, &childinfo);

/*     Accumulate Householder reflectors */

    if (colmajor) {
	if (wantu1 && *p > 0) {
	    zlacpy_("L", p, q, &x11[x11_offset], ldx11, &u1[u1_offset], ldu1);
	    zungqr_(p, p, q, &u1[u1_offset], ldu1, &work[itaup1], &work[
		    iorgqr], &lorgqrwork, info);
	}
	if (wantu2 && *m - *p > 0) {
	    i__1 = *m - *p;
	    zlacpy_("L", &i__1, q, &x21[x21_offset], ldx21, &u2[u2_offset], 
		    ldu2);
	    i__1 = *m - *p;
	    i__2 = *m - *p;
	    zungqr_(&i__1, &i__2, q, &u2[u2_offset], ldu2, &work[itaup2], &
		    work[iorgqr], &lorgqrwork, info);
	}
	if (wantv1t && *q > 0) {
	    i__1 = *q - 1;
	    i__2 = *q - 1;
	    zlacpy_("U", &i__1, &i__2, &x11[(x11_dim1 << 1) + 1], ldx11, &v1t[
		    (v1t_dim1 << 1) + 2], ldv1t);
	    i__1 = v1t_dim1 + 1;
	    v1t[i__1].r = 1., v1t[i__1].i = 0.;
	    i__1 = *q;
	    for (j = 2; j <= i__1; ++j) {
		i__2 = j * v1t_dim1 + 1;
		v1t[i__2].r = 0., v1t[i__2].i = 0.;
		i__2 = j + v1t_dim1;
		v1t[i__2].r = 0., v1t[i__2].i = 0.;
	    }
	    i__1 = *q - 1;
	    i__2 = *q - 1;
	    i__3 = *q - 1;
	    zunglq_(&i__1, &i__2, &i__3, &v1t[(v1t_dim1 << 1) + 2], ldv1t, &
		    work[itauq1], &work[iorglq], &lorglqwork, info);
	}
	if (wantv2t && *m - *q > 0) {
	    i__1 = *m - *q;
	    zlacpy_("U", p, &i__1, &x12[x12_offset], ldx12, &v2t[v2t_offset], 
		    ldv2t);
	    if (*m - *p > *q) {
		i__1 = *m - *p - *q;
		i__2 = *m - *p - *q;
		zlacpy_("U", &i__1, &i__2, &x22[*q + 1 + (*p + 1) * x22_dim1],
			 ldx22, &v2t[*p + 1 + (*p + 1) * v2t_dim1], ldv2t);
	    }
	    if (*m > *q) {
		i__1 = *m - *q;
		i__2 = *m - *q;
		i__3 = *m - *q;
		zunglq_(&i__1, &i__2, &i__3, &v2t[v2t_offset], ldv2t, &work[
			itauq2], &work[iorglq], &lorglqwork, info);
	    }
	}
    } else {
	if (wantu1 && *p > 0) {
	    zlacpy_("U", q, p, &x11[x11_offset], ldx11, &u1[u1_offset], ldu1);
	    zunglq_(p, p, q, &u1[u1_offset], ldu1, &work[itaup1], &work[
		    iorglq], &lorglqwork, info);
	}
	if (wantu2 && *m - *p > 0) {
	    i__1 = *m - *p;
	    zlacpy_("U", q, &i__1, &x21[x21_offset], ldx21, &u2[u2_offset], 
		    ldu2);
	    i__1 = *m - *p;
	    i__2 = *m - *p;
	    zunglq_(&i__1, &i__2, q, &u2[u2_offset], ldu2, &work[itaup2], &
		    work[iorglq], &lorglqwork, info);
	}
	if (wantv1t && *q > 0) {
	    i__1 = *q - 1;
	    i__2 = *q - 1;
	    zlacpy_("L", &i__1, &i__2, &x11[x11_dim1 + 2], ldx11, &v1t[(
		    v1t_dim1 << 1) + 2], ldv1t);
	    i__1 = v1t_dim1 + 1;
	    v1t[i__1].r = 1., v1t[i__1].i = 0.;
	    i__1 = *q;
	    for (j = 2; j <= i__1; ++j) {
		i__2 = j * v1t_dim1 + 1;
		v1t[i__2].r = 0., v1t[i__2].i = 0.;
		i__2 = j + v1t_dim1;
		v1t[i__2].r = 0., v1t[i__2].i = 0.;
	    }
	    i__1 = *q - 1;
	    i__2 = *q - 1;
	    i__3 = *q - 1;
	    zungqr_(&i__1, &i__2, &i__3, &v1t[(v1t_dim1 << 1) + 2], ldv1t, &
		    work[itauq1], &work[iorgqr], &lorgqrwork, info);
	}
	if (wantv2t && *m - *q > 0) {
/* Computing MIN */
	    i__1 = *p + 1;
	    p1 = f2cmin(i__1,*m);
/* Computing MIN */
	    i__1 = *q + 1;
	    q1 = f2cmin(i__1,*m);
	    i__1 = *m - *q;
	    zlacpy_("L", &i__1, p, &x12[x12_offset], ldx12, &v2t[v2t_offset], 
		    ldv2t);
	    if (*m > *p + *q) {
		i__1 = *m - *p - *q;
		i__2 = *m - *p - *q;
		zlacpy_("L", &i__1, &i__2, &x22[p1 + q1 * x22_dim1], ldx22, &
			v2t[*p + 1 + (*p + 1) * v2t_dim1], ldv2t);
	    }
	    i__1 = *m - *q;
	    i__2 = *m - *q;
	    i__3 = *m - *q;
	    zungqr_(&i__1, &i__2, &i__3, &v2t[v2t_offset], ldv2t, &work[
		    itauq2], &work[iorgqr], &lorgqrwork, info);
	}
    }

/*     Compute the CSD of the matrix in bidiagonal-block form */

    zbbcsd_(jobu1, jobu2, jobv1t, jobv2t, trans, m, p, q, &theta[1], &rwork[
	    iphi], &u1[u1_offset], ldu1, &u2[u2_offset], ldu2, &v1t[
	    v1t_offset], ldv1t, &v2t[v2t_offset], ldv2t, &rwork[ib11d], &
	    rwork[ib11e], &rwork[ib12d], &rwork[ib12e], &rwork[ib21d], &rwork[
	    ib21e], &rwork[ib22d], &rwork[ib22e], &rwork[ibbcsd], &lbbcsdwork,
	     info);

/*     Permute rows and columns to place identity submatrices in top- */
/*     left corner of (1,1)-block and/or bottom-right corner of (1,2)- */
/*     block and/or bottom-right corner of (2,1)-block and/or top-left */
/*     corner of (2,2)-block */

    if (*q > 0 && wantu2) {
	i__1 = *q;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    iwork[i__] = *m - *p - *q + i__;
	}
	i__1 = *m - *p;
	for (i__ = *q + 1; i__ <= i__1; ++i__) {
	    iwork[i__] = i__ - *q;
	}
	if (colmajor) {
	    i__1 = *m - *p;
	    i__2 = *m - *p;
	    zlapmt_(&c_false, &i__1, &i__2, &u2[u2_offset], ldu2, &iwork[1]);
	} else {
	    i__1 = *m - *p;
	    i__2 = *m - *p;
	    zlapmr_(&c_false, &i__1, &i__2, &u2[u2_offset], ldu2, &iwork[1]);
	}
    }
    if (*m > 0 && wantv2t) {
	i__1 = *p;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    iwork[i__] = *m - *p - *q + i__;
	}
	i__1 = *m - *q;
	for (i__ = *p + 1; i__ <= i__1; ++i__) {
	    iwork[i__] = i__ - *p;
	}
	if (! colmajor) {
	    i__1 = *m - *q;
	    i__2 = *m - *q;
	    zlapmt_(&c_false, &i__1, &i__2, &v2t[v2t_offset], ldv2t, &iwork[1]
		    );
	} else {
	    i__1 = *m - *q;
	    i__2 = *m - *q;
	    zlapmr_(&c_false, &i__1, &i__2, &v2t[v2t_offset], ldv2t, &iwork[1]
		    );
	}
    }

    return;

/*     End ZUNCSD */

} /* zuncsd_ */

