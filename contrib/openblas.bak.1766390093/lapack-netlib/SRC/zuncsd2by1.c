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
static integer c__1 = 1;
static logical c_false = FALSE_;

/* > \brief \b ZUNCSD2BY1 */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download ZUNCSD2BY1 + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zuncsd2
by1.f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zuncsd2
by1.f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zuncsd2
by1.f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE ZUNCSD2BY1( JOBU1, JOBU2, JOBV1T, M, P, Q, X11, LDX11, */
/*                              X21, LDX21, THETA, U1, LDU1, U2, LDU2, V1T, */
/*                              LDV1T, WORK, LWORK, RWORK, LRWORK, IWORK, */
/*                              INFO ) */

/*       CHARACTER          JOBU1, JOBU2, JOBV1T */
/*       INTEGER            INFO, LDU1, LDU2, LDV1T, LWORK, LDX11, LDX21, */
/*      $                   M, P, Q */
/*       INTEGER            LRWORK, LRWORKMIN, LRWORKOPT */
/*       DOUBLE PRECISION   RWORK(*) */
/*       DOUBLE PRECISION   THETA(*) */
/*       COMPLEX*16         U1(LDU1,*), U2(LDU2,*), V1T(LDV1T,*), WORK(*), */
/*      $                   X11(LDX11,*), X21(LDX21,*) */
/*       INTEGER            IWORK(*) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* >\verbatim */
/* > */
/* > ZUNCSD2BY1 computes the CS decomposition of an M-by-Q matrix X with */
/* > orthonormal columns that has been partitioned into a 2-by-1 block */
/* > structure: */
/* > */
/* >                                [  I1 0  0 ] */
/* >                                [  0  C  0 ] */
/* >          [ X11 ]   [ U1 |    ] [  0  0  0 ] */
/* >      X = [-----] = [---------] [----------] V1**T . */
/* >          [ X21 ]   [    | U2 ] [  0  0  0 ] */
/* >                                [  0  S  0 ] */
/* >                                [  0  0  I2] */
/* > */
/* > X11 is P-by-Q. The unitary matrices U1, U2, and V1 are P-by-P, */
/* > (M-P)-by-(M-P), and Q-by-Q, respectively. C and S are R-by-R */
/* > nonnegative diagonal matrices satisfying C^2 + S^2 = I, in which */
/* > R = MIN(P,M-P,Q,M-Q). I1 is a K1-by-K1 identity matrix and I2 is a */
/* > K2-by-K2 identity matrix, where K1 = MAX(Q+P-M,0), K2 = MAX(Q-P,0). */
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
/* > \param[in] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >          The number of rows in X. */
/* > \endverbatim */
/* > */
/* > \param[in] P */
/* > \verbatim */
/* >          P is INTEGER */
/* >          The number of rows in X11. 0 <= P <= M. */
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
/* > \param[in,out] X21 */
/* > \verbatim */
/* >          X21 is COMPLEX*16 array, dimension (LDX21,Q) */
/* >          On entry, part of the unitary matrix whose CSD is desired. */
/* > \endverbatim */
/* > */
/* > \param[in] LDX21 */
/* > \verbatim */
/* >          LDX21 is INTEGER */
/* >          The leading dimension of X21. LDX21 >= MAX(1,M-P). */
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
/* >          U1 is COMPLEX*16 array, dimension (P) */
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
/* >          U2 is COMPLEX*16 array, dimension (M-P) */
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
/* >          V1T is COMPLEX*16 array, dimension (Q) */
/* >          If JOBV1T = 'Y', V1T contains the Q-by-Q matrix unitary */
/* >          matrix V1**T. */
/* > \endverbatim */
/* > */
/* > \param[in] LDV1T */
/* > \verbatim */
/* >          LDV1T is INTEGER */
/* >          The leading dimension of V1T. If JOBV1T = 'Y', LDV1T >= */
/* >          MAX(1,Q). */
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
/* >          RWORK is DOUBLE PRECISION array, dimension (MAX(1,LRWORK)) */
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
/* >          > 0:  ZBBCSD did not converge. See the description of WORK */
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

/* > \date July 2012 */

/* > \ingroup complex16OTHERcomputational */

/*  ===================================================================== */
/* Subroutine */ void zuncsd2by1_(char *jobu1, char *jobu2, char *jobv1t, 
	integer *m, integer *p, integer *q, doublecomplex *x11, integer *
	ldx11, doublecomplex *x21, integer *ldx21, doublereal *theta, 
	doublecomplex *u1, integer *ldu1, doublecomplex *u2, integer *ldu2, 
	doublecomplex *v1t, integer *ldv1t, doublecomplex *work, integer *
	lwork, doublereal *rwork, integer *lrwork, integer *iwork, integer *
	info)
{
    /* System generated locals */
    integer u1_dim1, u1_offset, u2_dim1, u2_offset, v1t_dim1, v1t_offset, 
	    x11_dim1, x11_offset, x21_dim1, x21_offset, i__1, i__2, i__3;

    /* Local variables */
    integer ib11d, ib11e, ib12d, ib12e, ib21d, ib21e, ib22d, ib22e;
    doublecomplex cdum[1]	/* was [1][1] */;
    integer iphi, lworkmin, lworkopt, i__, j, r__;
    extern logical lsame_(char *, char *);
    integer childinfo;
    extern /* Subroutine */ void zcopy_(integer *, doublecomplex *, integer *, 
	    doublecomplex *, integer *);
    integer lorglqmin, lorgqrmin, lorglqopt, lrworkmin, itaup1, itaup2, 
	    itauq1, lorgqropt;
    logical wantu1, wantu2;
    integer lrworkopt, ibbcsd, lbbcsd, iorbdb, lorbdb;
    extern /* Subroutine */ void zbbcsd_(char *, char *, char *, char *, char *
	    , integer *, integer *, integer *, doublereal *, doublereal *, 
	    doublecomplex *, integer *, doublecomplex *, integer *, 
	    doublecomplex *, integer *, doublecomplex *, integer *, 
	    doublereal *, doublereal *, doublereal *, doublereal *, 
	    doublereal *, doublereal *, doublereal *, doublereal *, 
	    doublereal *, integer *, integer *);
    extern int xerbla_(char *, integer *, ftnlen);
    integer iorglq, lorglq;
    extern /* Subroutine */ void zlacpy_(char *, integer *, integer *, 
	    doublecomplex *, integer *, doublecomplex *, integer *);
    integer iorgqr;
    extern /* Subroutine */ void zlapmr_(logical *, integer *, integer *, 
	    doublecomplex *, integer *, integer *);
    integer lorgqr;
    extern /* Subroutine */ void zlapmt_(logical *, integer *, integer *, 
	    doublecomplex *, integer *, integer *);
    logical lquery;
    extern /* Subroutine */ void zunglq_(integer *, integer *, integer *, 
	    doublecomplex *, integer *, doublecomplex *, doublecomplex *, 
	    integer *, integer *), zungqr_(integer *, integer *, integer *, 
	    doublecomplex *, integer *, doublecomplex *, doublecomplex *, 
	    integer *, integer *), zunbdb1_(integer *, integer *, integer *, 
	    doublecomplex *, integer *, doublecomplex *, integer *, 
	    doublereal *, doublereal *, doublecomplex *, doublecomplex *, 
	    doublecomplex *, doublecomplex *, integer *, integer *), zunbdb2_(
	    integer *, integer *, integer *, doublecomplex *, integer *, 
	    doublecomplex *, integer *, doublereal *, doublereal *, 
	    doublecomplex *, doublecomplex *, doublecomplex *, doublecomplex *
	    , integer *, integer *), zunbdb3_(integer *, integer *, integer *,
	     doublecomplex *, integer *, doublecomplex *, integer *, 
	    doublereal *, doublereal *, doublecomplex *, doublecomplex *, 
	    doublecomplex *, doublecomplex *, integer *, integer *), zunbdb4_(
	    integer *, integer *, integer *, doublecomplex *, integer *, 
	    doublecomplex *, integer *, doublereal *, doublereal *, 
	    doublecomplex *, doublecomplex *, doublecomplex *, doublecomplex *
	    , doublecomplex *, integer *, integer *);
    logical wantv1t;
    doublereal dum[1];


/*  -- LAPACK computational routine (version 3.7.1) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     July 2012 */


/*  ===================================================================== */


/*     Test input arguments */

    /* Parameter adjustments */
    x11_dim1 = *ldx11;
    x11_offset = 1 + x11_dim1 * 1;
    x11 -= x11_offset;
    x21_dim1 = *ldx21;
    x21_offset = 1 + x21_dim1 * 1;
    x21 -= x21_offset;
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
    --work;
    --rwork;
    --iwork;

    /* Function Body */
    *info = 0;
    wantu1 = lsame_(jobu1, "Y");
    wantu2 = lsame_(jobu2, "Y");
    wantv1t = lsame_(jobv1t, "Y");
    lquery = *lwork == -1;

    if (*m < 0) {
	*info = -4;
    } else if (*p < 0 || *p > *m) {
	*info = -5;
    } else if (*q < 0 || *q > *m) {
	*info = -6;
    } else if (*ldx11 < f2cmax(1,*p)) {
	*info = -8;
    } else /* if(complicated condition) */ {
/* Computing MAX */
	i__1 = 1, i__2 = *m - *p;
	if (*ldx21 < f2cmax(i__1,i__2)) {
	    *info = -10;
	} else if (wantu1 && *ldu1 < f2cmax(1,*p)) {
	    *info = -13;
	} else /* if(complicated condition) */ {
/* Computing MAX */
	    i__1 = 1, i__2 = *m - *p;
	    if (wantu2 && *ldu2 < f2cmax(i__1,i__2)) {
		*info = -15;
	    } else if (wantv1t && *ldv1t < f2cmax(1,*q)) {
		*info = -17;
	    }
	}
    }

/* Computing MIN */
    i__1 = *p, i__2 = *m - *p, i__1 = f2cmin(i__1,i__2), i__1 = f2cmin(i__1,*q), 
	    i__2 = *m - *q;
    r__ = f2cmin(i__1,i__2);

/*     Compute workspace */

/*       WORK layout: */
/*     |-----------------------------------------| */
/*     | LWORKOPT (1)                            | */
/*     |-----------------------------------------| */
/*     | TAUP1 (MAX(1,P))                        | */
/*     | TAUP2 (MAX(1,M-P))                      | */
/*     | TAUQ1 (MAX(1,Q))                        | */
/*     |-----------------------------------------| */
/*     | ZUNBDB WORK | ZUNGQR WORK | ZUNGLQ WORK | */
/*     |             |             |             | */
/*     |             |             |             | */
/*     |             |             |             | */
/*     |             |             |             | */
/*     |-----------------------------------------| */
/*       RWORK layout: */
/*     |------------------| */
/*     | LRWORKOPT (1)    | */
/*     |------------------| */
/*     | PHI (MAX(1,R-1)) | */
/*     |------------------| */
/*     | B11D (R)         | */
/*     | B11E (R-1)       | */
/*     | B12D (R)         | */
/*     | B12E (R-1)       | */
/*     | B21D (R)         | */
/*     | B21E (R-1)       | */
/*     | B22D (R)         | */
/*     | B22E (R-1)       | */
/*     | ZBBCSD RWORK     | */
/*     |------------------| */

    if (*info == 0) {
	iphi = 2;
/* Computing MAX */
	i__1 = 1, i__2 = r__ - 1;
	ib11d = iphi + f2cmax(i__1,i__2);
	ib11e = ib11d + f2cmax(1,r__);
/* Computing MAX */
	i__1 = 1, i__2 = r__ - 1;
	ib12d = ib11e + f2cmax(i__1,i__2);
	ib12e = ib12d + f2cmax(1,r__);
/* Computing MAX */
	i__1 = 1, i__2 = r__ - 1;
	ib21d = ib12e + f2cmax(i__1,i__2);
	ib21e = ib21d + f2cmax(1,r__);
/* Computing MAX */
	i__1 = 1, i__2 = r__ - 1;
	ib22d = ib21e + f2cmax(i__1,i__2);
	ib22e = ib22d + f2cmax(1,r__);
/* Computing MAX */
	i__1 = 1, i__2 = r__ - 1;
	ibbcsd = ib22e + f2cmax(i__1,i__2);
	itaup1 = 2;
	itaup2 = itaup1 + f2cmax(1,*p);
/* Computing MAX */
	i__1 = 1, i__2 = *m - *p;
	itauq1 = itaup2 + f2cmax(i__1,i__2);
	iorbdb = itauq1 + f2cmax(1,*q);
	iorgqr = itauq1 + f2cmax(1,*q);
	iorglq = itauq1 + f2cmax(1,*q);
	lorgqrmin = 1;
	lorgqropt = 1;
	lorglqmin = 1;
	lorglqopt = 1;
	if (r__ == *q) {
	    zunbdb1_(m, p, q, &x11[x11_offset], ldx11, &x21[x21_offset], 
		    ldx21, &theta[1], dum, cdum, cdum, cdum, &work[1], &c_n1, 
		    &childinfo);
	    lorbdb = (integer) work[1].r;
	    if (wantu1 && *p > 0) {
		zungqr_(p, p, q, &u1[u1_offset], ldu1, cdum, &work[1], &c_n1, 
			&childinfo);
		lorgqrmin = f2cmax(lorgqrmin,*p);
/* Computing MAX */
		i__1 = lorgqropt, i__2 = (integer) work[1].r;
		lorgqropt = f2cmax(i__1,i__2);
	    }
	    if (wantu2 && *m - *p > 0) {
		i__1 = *m - *p;
		i__2 = *m - *p;
		zungqr_(&i__1, &i__2, q, &u2[u2_offset], ldu2, cdum, &work[1],
			 &c_n1, &childinfo);
/* Computing MAX */
		i__1 = lorgqrmin, i__2 = *m - *p;
		lorgqrmin = f2cmax(i__1,i__2);
/* Computing MAX */
		i__1 = lorgqropt, i__2 = (integer) work[1].r;
		lorgqropt = f2cmax(i__1,i__2);
	    }
	    if (wantv1t && *q > 0) {
		i__1 = *q - 1;
		i__2 = *q - 1;
		i__3 = *q - 1;
		zunglq_(&i__1, &i__2, &i__3, &v1t[v1t_offset], ldv1t, cdum, &
			work[1], &c_n1, &childinfo);
/* Computing MAX */
		i__1 = lorglqmin, i__2 = *q - 1;
		lorglqmin = f2cmax(i__1,i__2);
/* Computing MAX */
		i__1 = lorglqopt, i__2 = (integer) work[1].r;
		lorglqopt = f2cmax(i__1,i__2);
	    }
	    zbbcsd_(jobu1, jobu2, jobv1t, "N", "N", m, p, q, &theta[1], dum, &
		    u1[u1_offset], ldu1, &u2[u2_offset], ldu2, &v1t[
		    v1t_offset], ldv1t, cdum, &c__1, dum, dum, dum, dum, dum, 
		    dum, dum, dum, &rwork[1], &c_n1, &childinfo);
	    lbbcsd = (integer) rwork[1];
	} else if (r__ == *p) {
	    zunbdb2_(m, p, q, &x11[x11_offset], ldx11, &x21[x21_offset], 
		    ldx21, &theta[1], dum, cdum, cdum, cdum, &work[1], &c_n1, 
		    &childinfo);
	    lorbdb = (integer) work[1].r;
	    if (wantu1 && *p > 0) {
		i__1 = *p - 1;
		i__2 = *p - 1;
		i__3 = *p - 1;
		zungqr_(&i__1, &i__2, &i__3, &u1[(u1_dim1 << 1) + 2], ldu1, 
			cdum, &work[1], &c_n1, &childinfo);
/* Computing MAX */
		i__1 = lorgqrmin, i__2 = *p - 1;
		lorgqrmin = f2cmax(i__1,i__2);
/* Computing MAX */
		i__1 = lorgqropt, i__2 = (integer) work[1].r;
		lorgqropt = f2cmax(i__1,i__2);
	    }
	    if (wantu2 && *m - *p > 0) {
		i__1 = *m - *p;
		i__2 = *m - *p;
		zungqr_(&i__1, &i__2, q, &u2[u2_offset], ldu2, cdum, &work[1],
			 &c_n1, &childinfo);
/* Computing MAX */
		i__1 = lorgqrmin, i__2 = *m - *p;
		lorgqrmin = f2cmax(i__1,i__2);
/* Computing MAX */
		i__1 = lorgqropt, i__2 = (integer) work[1].r;
		lorgqropt = f2cmax(i__1,i__2);
	    }
	    if (wantv1t && *q > 0) {
		zunglq_(q, q, &r__, &v1t[v1t_offset], ldv1t, cdum, &work[1], &
			c_n1, &childinfo);
		lorglqmin = f2cmax(lorglqmin,*q);
/* Computing MAX */
		i__1 = lorglqopt, i__2 = (integer) work[1].r;
		lorglqopt = f2cmax(i__1,i__2);
	    }
	    zbbcsd_(jobv1t, "N", jobu1, jobu2, "T", m, q, p, &theta[1], dum, &
		    v1t[v1t_offset], ldv1t, cdum, &c__1, &u1[u1_offset], ldu1,
		     &u2[u2_offset], ldu2, dum, dum, dum, dum, dum, dum, dum, 
		    dum, &rwork[1], &c_n1, &childinfo);
	    lbbcsd = (integer) rwork[1];
	} else if (r__ == *m - *p) {
	    zunbdb3_(m, p, q, &x11[x11_offset], ldx11, &x21[x21_offset], 
		    ldx21, &theta[1], dum, cdum, cdum, cdum, &work[1], &c_n1, 
		    &childinfo);
	    lorbdb = (integer) work[1].r;
	    if (wantu1 && *p > 0) {
		zungqr_(p, p, q, &u1[u1_offset], ldu1, cdum, &work[1], &c_n1, 
			&childinfo);
		lorgqrmin = f2cmax(lorgqrmin,*p);
/* Computing MAX */
		i__1 = lorgqropt, i__2 = (integer) work[1].r;
		lorgqropt = f2cmax(i__1,i__2);
	    }
	    if (wantu2 && *m - *p > 0) {
		i__1 = *m - *p - 1;
		i__2 = *m - *p - 1;
		i__3 = *m - *p - 1;
		zungqr_(&i__1, &i__2, &i__3, &u2[(u2_dim1 << 1) + 2], ldu2, 
			cdum, &work[1], &c_n1, &childinfo);
/* Computing MAX */
		i__1 = lorgqrmin, i__2 = *m - *p - 1;
		lorgqrmin = f2cmax(i__1,i__2);
/* Computing MAX */
		i__1 = lorgqropt, i__2 = (integer) work[1].r;
		lorgqropt = f2cmax(i__1,i__2);
	    }
	    if (wantv1t && *q > 0) {
		zunglq_(q, q, &r__, &v1t[v1t_offset], ldv1t, cdum, &work[1], &
			c_n1, &childinfo);
		lorglqmin = f2cmax(lorglqmin,*q);
/* Computing MAX */
		i__1 = lorglqopt, i__2 = (integer) work[1].r;
		lorglqopt = f2cmax(i__1,i__2);
	    }
	    i__1 = *m - *q;
	    i__2 = *m - *p;
	    zbbcsd_("N", jobv1t, jobu2, jobu1, "T", m, &i__1, &i__2, &theta[1]
		    , dum, cdum, &c__1, &v1t[v1t_offset], ldv1t, &u2[
		    u2_offset], ldu2, &u1[u1_offset], ldu1, dum, dum, dum, 
		    dum, dum, dum, dum, dum, &rwork[1], &c_n1, &childinfo);
	    lbbcsd = (integer) rwork[1];
	} else {
	    zunbdb4_(m, p, q, &x11[x11_offset], ldx11, &x21[x21_offset], 
		    ldx21, &theta[1], dum, cdum, cdum, cdum, cdum, &work[1], &
		    c_n1, &childinfo);
	    lorbdb = *m + (integer) work[1].r;
	    if (wantu1 && *p > 0) {
		i__1 = *m - *q;
		zungqr_(p, p, &i__1, &u1[u1_offset], ldu1, cdum, &work[1], &
			c_n1, &childinfo);
		lorgqrmin = f2cmax(lorgqrmin,*p);
/* Computing MAX */
		i__1 = lorgqropt, i__2 = (integer) work[1].r;
		lorgqropt = f2cmax(i__1,i__2);
	    }
	    if (wantu2 && *m - *p > 0) {
		i__1 = *m - *p;
		i__2 = *m - *p;
		i__3 = *m - *q;
		zungqr_(&i__1, &i__2, &i__3, &u2[u2_offset], ldu2, cdum, &
			work[1], &c_n1, &childinfo);
/* Computing MAX */
		i__1 = lorgqrmin, i__2 = *m - *p;
		lorgqrmin = f2cmax(i__1,i__2);
/* Computing MAX */
		i__1 = lorgqropt, i__2 = (integer) work[1].r;
		lorgqropt = f2cmax(i__1,i__2);
	    }
	    if (wantv1t && *q > 0) {
		zunglq_(q, q, q, &v1t[v1t_offset], ldv1t, cdum, &work[1], &
			c_n1, &childinfo);
		lorglqmin = f2cmax(lorglqmin,*q);
/* Computing MAX */
		i__1 = lorglqopt, i__2 = (integer) work[1].r;
		lorglqopt = f2cmax(i__1,i__2);
	    }
	    i__1 = *m - *p;
	    i__2 = *m - *q;
	    zbbcsd_(jobu2, jobu1, "N", jobv1t, "N", m, &i__1, &i__2, &theta[1]
		    , dum, &u2[u2_offset], ldu2, &u1[u1_offset], ldu1, cdum, &
		    c__1, &v1t[v1t_offset], ldv1t, dum, dum, dum, dum, dum, 
		    dum, dum, dum, &rwork[1], &c_n1, &childinfo);
	    lbbcsd = (integer) rwork[1];
	}
	lrworkmin = ibbcsd + lbbcsd - 1;
	lrworkopt = lrworkmin;
	rwork[1] = (doublereal) lrworkopt;
/* Computing MAX */
	i__1 = iorbdb + lorbdb - 1, i__2 = iorgqr + lorgqrmin - 1, i__1 = f2cmax(
		i__1,i__2), i__2 = iorglq + lorglqmin - 1;
	lworkmin = f2cmax(i__1,i__2);
/* Computing MAX */
	i__1 = iorbdb + lorbdb - 1, i__2 = iorgqr + lorgqropt - 1, i__1 = f2cmax(
		i__1,i__2), i__2 = iorglq + lorglqopt - 1;
	lworkopt = f2cmax(i__1,i__2);
	work[1].r = (doublereal) lworkopt, work[1].i = 0.;
	if (*lwork < lworkmin && ! lquery) {
	    *info = -19;
	}
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("ZUNCSD2BY1", &i__1, (ftnlen)10);
	return;
    } else if (lquery) {
	return;
    }
    lorgqr = *lwork - iorgqr + 1;
    lorglq = *lwork - iorglq + 1;

/*     Handle four cases separately: R = Q, R = P, R = M-P, and R = M-Q, */
/*     in which R = MIN(P,M-P,Q,M-Q) */

    if (r__ == *q) {

/*        Case 1: R = Q */

/*        Simultaneously bidiagonalize X11 and X21 */

	zunbdb1_(m, p, q, &x11[x11_offset], ldx11, &x21[x21_offset], ldx21, &
		theta[1], &rwork[iphi], &work[itaup1], &work[itaup2], &work[
		itauq1], &work[iorbdb], &lorbdb, &childinfo);

/*        Accumulate Householder reflectors */

	if (wantu1 && *p > 0) {
	    zlacpy_("L", p, q, &x11[x11_offset], ldx11, &u1[u1_offset], ldu1);
	    zungqr_(p, p, q, &u1[u1_offset], ldu1, &work[itaup1], &work[
		    iorgqr], &lorgqr, &childinfo);
	}
	if (wantu2 && *m - *p > 0) {
	    i__1 = *m - *p;
	    zlacpy_("L", &i__1, q, &x21[x21_offset], ldx21, &u2[u2_offset], 
		    ldu2);
	    i__1 = *m - *p;
	    i__2 = *m - *p;
	    zungqr_(&i__1, &i__2, q, &u2[u2_offset], ldu2, &work[itaup2], &
		    work[iorgqr], &lorgqr, &childinfo);
	}
	if (wantv1t && *q > 0) {
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
	    zlacpy_("U", &i__1, &i__2, &x21[(x21_dim1 << 1) + 1], ldx21, &v1t[
		    (v1t_dim1 << 1) + 2], ldv1t);
	    i__1 = *q - 1;
	    i__2 = *q - 1;
	    i__3 = *q - 1;
	    zunglq_(&i__1, &i__2, &i__3, &v1t[(v1t_dim1 << 1) + 2], ldv1t, &
		    work[itauq1], &work[iorglq], &lorglq, &childinfo);
	}

/*        Simultaneously diagonalize X11 and X21. */

	zbbcsd_(jobu1, jobu2, jobv1t, "N", "N", m, p, q, &theta[1], &rwork[
		iphi], &u1[u1_offset], ldu1, &u2[u2_offset], ldu2, &v1t[
		v1t_offset], ldv1t, cdum, &c__1, &rwork[ib11d], &rwork[ib11e],
		 &rwork[ib12d], &rwork[ib12e], &rwork[ib21d], &rwork[ib21e], &
		rwork[ib22d], &rwork[ib22e], &rwork[ibbcsd], &lbbcsd, &
		childinfo);

/*        Permute rows and columns to place zero submatrices in */
/*        preferred positions */

	if (*q > 0 && wantu2) {
	    i__1 = *q;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		iwork[i__] = *m - *p - *q + i__;
	    }
	    i__1 = *m - *p;
	    for (i__ = *q + 1; i__ <= i__1; ++i__) {
		iwork[i__] = i__ - *q;
	    }
	    i__1 = *m - *p;
	    i__2 = *m - *p;
	    zlapmt_(&c_false, &i__1, &i__2, &u2[u2_offset], ldu2, &iwork[1]);
	}
    } else if (r__ == *p) {

/*        Case 2: R = P */

/*        Simultaneously bidiagonalize X11 and X21 */

	zunbdb2_(m, p, q, &x11[x11_offset], ldx11, &x21[x21_offset], ldx21, &
		theta[1], &rwork[iphi], &work[itaup1], &work[itaup2], &work[
		itauq1], &work[iorbdb], &lorbdb, &childinfo);

/*        Accumulate Householder reflectors */

	if (wantu1 && *p > 0) {
	    i__1 = u1_dim1 + 1;
	    u1[i__1].r = 1., u1[i__1].i = 0.;
	    i__1 = *p;
	    for (j = 2; j <= i__1; ++j) {
		i__2 = j * u1_dim1 + 1;
		u1[i__2].r = 0., u1[i__2].i = 0.;
		i__2 = j + u1_dim1;
		u1[i__2].r = 0., u1[i__2].i = 0.;
	    }
	    i__1 = *p - 1;
	    i__2 = *p - 1;
	    zlacpy_("L", &i__1, &i__2, &x11[x11_dim1 + 2], ldx11, &u1[(
		    u1_dim1 << 1) + 2], ldu1);
	    i__1 = *p - 1;
	    i__2 = *p - 1;
	    i__3 = *p - 1;
	    zungqr_(&i__1, &i__2, &i__3, &u1[(u1_dim1 << 1) + 2], ldu1, &work[
		    itaup1], &work[iorgqr], &lorgqr, &childinfo);
	}
	if (wantu2 && *m - *p > 0) {
	    i__1 = *m - *p;
	    zlacpy_("L", &i__1, q, &x21[x21_offset], ldx21, &u2[u2_offset], 
		    ldu2);
	    i__1 = *m - *p;
	    i__2 = *m - *p;
	    zungqr_(&i__1, &i__2, q, &u2[u2_offset], ldu2, &work[itaup2], &
		    work[iorgqr], &lorgqr, &childinfo);
	}
	if (wantv1t && *q > 0) {
	    zlacpy_("U", p, q, &x11[x11_offset], ldx11, &v1t[v1t_offset], 
		    ldv1t);
	    zunglq_(q, q, &r__, &v1t[v1t_offset], ldv1t, &work[itauq1], &work[
		    iorglq], &lorglq, &childinfo);
	}

/*        Simultaneously diagonalize X11 and X21. */

	zbbcsd_(jobv1t, "N", jobu1, jobu2, "T", m, q, p, &theta[1], &rwork[
		iphi], &v1t[v1t_offset], ldv1t, cdum, &c__1, &u1[u1_offset], 
		ldu1, &u2[u2_offset], ldu2, &rwork[ib11d], &rwork[ib11e], &
		rwork[ib12d], &rwork[ib12e], &rwork[ib21d], &rwork[ib21e], &
		rwork[ib22d], &rwork[ib22e], &rwork[ibbcsd], &lbbcsd, &
		childinfo);

/*        Permute rows and columns to place identity submatrices in */
/*        preferred positions */

	if (*q > 0 && wantu2) {
	    i__1 = *q;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		iwork[i__] = *m - *p - *q + i__;
	    }
	    i__1 = *m - *p;
	    for (i__ = *q + 1; i__ <= i__1; ++i__) {
		iwork[i__] = i__ - *q;
	    }
	    i__1 = *m - *p;
	    i__2 = *m - *p;
	    zlapmt_(&c_false, &i__1, &i__2, &u2[u2_offset], ldu2, &iwork[1]);
	}
    } else if (r__ == *m - *p) {

/*        Case 3: R = M-P */

/*        Simultaneously bidiagonalize X11 and X21 */

	zunbdb3_(m, p, q, &x11[x11_offset], ldx11, &x21[x21_offset], ldx21, &
		theta[1], &rwork[iphi], &work[itaup1], &work[itaup2], &work[
		itauq1], &work[iorbdb], &lorbdb, &childinfo);

/*        Accumulate Householder reflectors */

	if (wantu1 && *p > 0) {
	    zlacpy_("L", p, q, &x11[x11_offset], ldx11, &u1[u1_offset], ldu1);
	    zungqr_(p, p, q, &u1[u1_offset], ldu1, &work[itaup1], &work[
		    iorgqr], &lorgqr, &childinfo);
	}
	if (wantu2 && *m - *p > 0) {
	    i__1 = u2_dim1 + 1;
	    u2[i__1].r = 1., u2[i__1].i = 0.;
	    i__1 = *m - *p;
	    for (j = 2; j <= i__1; ++j) {
		i__2 = j * u2_dim1 + 1;
		u2[i__2].r = 0., u2[i__2].i = 0.;
		i__2 = j + u2_dim1;
		u2[i__2].r = 0., u2[i__2].i = 0.;
	    }
	    i__1 = *m - *p - 1;
	    i__2 = *m - *p - 1;
	    zlacpy_("L", &i__1, &i__2, &x21[x21_dim1 + 2], ldx21, &u2[(
		    u2_dim1 << 1) + 2], ldu2);
	    i__1 = *m - *p - 1;
	    i__2 = *m - *p - 1;
	    i__3 = *m - *p - 1;
	    zungqr_(&i__1, &i__2, &i__3, &u2[(u2_dim1 << 1) + 2], ldu2, &work[
		    itaup2], &work[iorgqr], &lorgqr, &childinfo);
	}
	if (wantv1t && *q > 0) {
	    i__1 = *m - *p;
	    zlacpy_("U", &i__1, q, &x21[x21_offset], ldx21, &v1t[v1t_offset], 
		    ldv1t);
	    zunglq_(q, q, &r__, &v1t[v1t_offset], ldv1t, &work[itauq1], &work[
		    iorglq], &lorglq, &childinfo);
	}

/*        Simultaneously diagonalize X11 and X21. */

	i__1 = *m - *q;
	i__2 = *m - *p;
	zbbcsd_("N", jobv1t, jobu2, jobu1, "T", m, &i__1, &i__2, &theta[1], &
		rwork[iphi], cdum, &c__1, &v1t[v1t_offset], ldv1t, &u2[
		u2_offset], ldu2, &u1[u1_offset], ldu1, &rwork[ib11d], &rwork[
		ib11e], &rwork[ib12d], &rwork[ib12e], &rwork[ib21d], &rwork[
		ib21e], &rwork[ib22d], &rwork[ib22e], &rwork[ibbcsd], &lbbcsd,
		 &childinfo);

/*        Permute rows and columns to place identity submatrices in */
/*        preferred positions */

	if (*q > r__) {
	    i__1 = r__;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		iwork[i__] = *q - r__ + i__;
	    }
	    i__1 = *q;
	    for (i__ = r__ + 1; i__ <= i__1; ++i__) {
		iwork[i__] = i__ - r__;
	    }
	    if (wantu1) {
		zlapmt_(&c_false, p, q, &u1[u1_offset], ldu1, &iwork[1]);
	    }
	    if (wantv1t) {
		zlapmr_(&c_false, q, q, &v1t[v1t_offset], ldv1t, &iwork[1]);
	    }
	}
    } else {

/*        Case 4: R = M-Q */

/*        Simultaneously bidiagonalize X11 and X21 */

	i__1 = lorbdb - *m;
	zunbdb4_(m, p, q, &x11[x11_offset], ldx11, &x21[x21_offset], ldx21, &
		theta[1], &rwork[iphi], &work[itaup1], &work[itaup2], &work[
		itauq1], &work[iorbdb], &work[iorbdb + *m], &i__1, &childinfo)
		;

/*        Accumulate Householder reflectors */

	if (wantu1 && *p > 0) {
	    zcopy_(p, &work[iorbdb], &c__1, &u1[u1_offset], &c__1);
	    i__1 = *p;
	    for (j = 2; j <= i__1; ++j) {
		i__2 = j * u1_dim1 + 1;
		u1[i__2].r = 0., u1[i__2].i = 0.;
	    }
	    i__1 = *p - 1;
	    i__2 = *m - *q - 1;
	    zlacpy_("L", &i__1, &i__2, &x11[x11_dim1 + 2], ldx11, &u1[(
		    u1_dim1 << 1) + 2], ldu1);
	    i__1 = *m - *q;
	    zungqr_(p, p, &i__1, &u1[u1_offset], ldu1, &work[itaup1], &work[
		    iorgqr], &lorgqr, &childinfo);
	}
	if (wantu2 && *m - *p > 0) {
	    i__1 = *m - *p;
	    zcopy_(&i__1, &work[iorbdb + *p], &c__1, &u2[u2_offset], &c__1);
	    i__1 = *m - *p;
	    for (j = 2; j <= i__1; ++j) {
		i__2 = j * u2_dim1 + 1;
		u2[i__2].r = 0., u2[i__2].i = 0.;
	    }
	    i__1 = *m - *p - 1;
	    i__2 = *m - *q - 1;
	    zlacpy_("L", &i__1, &i__2, &x21[x21_dim1 + 2], ldx21, &u2[(
		    u2_dim1 << 1) + 2], ldu2);
	    i__1 = *m - *p;
	    i__2 = *m - *p;
	    i__3 = *m - *q;
	    zungqr_(&i__1, &i__2, &i__3, &u2[u2_offset], ldu2, &work[itaup2], 
		    &work[iorgqr], &lorgqr, &childinfo);
	}
	if (wantv1t && *q > 0) {
	    i__1 = *m - *q;
	    zlacpy_("U", &i__1, q, &x21[x21_offset], ldx21, &v1t[v1t_offset], 
		    ldv1t);
	    i__1 = *p - (*m - *q);
	    i__2 = *q - (*m - *q);
	    zlacpy_("U", &i__1, &i__2, &x11[*m - *q + 1 + (*m - *q + 1) * 
		    x11_dim1], ldx11, &v1t[*m - *q + 1 + (*m - *q + 1) * 
		    v1t_dim1], ldv1t);
	    i__1 = -(*p) + *q;
	    i__2 = *q - *p;
	    zlacpy_("U", &i__1, &i__2, &x21[*m - *q + 1 + (*p + 1) * x21_dim1]
		    , ldx21, &v1t[*p + 1 + (*p + 1) * v1t_dim1], ldv1t);
	    zunglq_(q, q, q, &v1t[v1t_offset], ldv1t, &work[itauq1], &work[
		    iorglq], &lorglq, &childinfo);
	}

/*        Simultaneously diagonalize X11 and X21. */

	i__1 = *m - *p;
	i__2 = *m - *q;
	zbbcsd_(jobu2, jobu1, "N", jobv1t, "N", m, &i__1, &i__2, &theta[1], &
		rwork[iphi], &u2[u2_offset], ldu2, &u1[u1_offset], ldu1, cdum,
		 &c__1, &v1t[v1t_offset], ldv1t, &rwork[ib11d], &rwork[ib11e],
		 &rwork[ib12d], &rwork[ib12e], &rwork[ib21d], &rwork[ib21e], &
		rwork[ib22d], &rwork[ib22e], &rwork[ibbcsd], &lbbcsd, &
		childinfo);

/*        Permute rows and columns to place identity submatrices in */
/*        preferred positions */

	if (*p > r__) {
	    i__1 = r__;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		iwork[i__] = *p - r__ + i__;
	    }
	    i__1 = *p;
	    for (i__ = r__ + 1; i__ <= i__1; ++i__) {
		iwork[i__] = i__ - r__;
	    }
	    if (wantu1) {
		zlapmt_(&c_false, p, p, &u1[u1_offset], ldu1, &iwork[1]);
	    }
	    if (wantv1t) {
		zlapmr_(&c_false, p, q, &v1t[v1t_offset], ldv1t, &iwork[1]);
	    }
	}
    }

    return;

/*     End of ZUNCSD2BY1 */

} /* zuncsd2by1_ */

