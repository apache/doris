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

static integer c__1 = 1;

/* > \brief \b ZUNBDB */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download ZUNBDB + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zunbdb.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zunbdb.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zunbdb.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE ZUNBDB( TRANS, SIGNS, M, P, Q, X11, LDX11, X12, LDX12, */
/*                          X21, LDX21, X22, LDX22, THETA, PHI, TAUP1, */
/*                          TAUP2, TAUQ1, TAUQ2, WORK, LWORK, INFO ) */

/*       CHARACTER          SIGNS, TRANS */
/*       INTEGER            INFO, LDX11, LDX12, LDX21, LDX22, LWORK, M, P, */
/*      $                   Q */
/*       DOUBLE PRECISION   PHI( * ), THETA( * ) */
/*       COMPLEX*16         TAUP1( * ), TAUP2( * ), TAUQ1( * ), TAUQ2( * ), */
/*      $                   WORK( * ), X11( LDX11, * ), X12( LDX12, * ), */
/*      $                   X21( LDX21, * ), X22( LDX22, * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > ZUNBDB simultaneously bidiagonalizes the blocks of an M-by-M */
/* > partitioned unitary matrix X: */
/* > */
/* >                                 [ B11 | B12 0  0 ] */
/* >     [ X11 | X12 ]   [ P1 |    ] [  0  |  0 -I  0 ] [ Q1 |    ]**H */
/* > X = [-----------] = [---------] [----------------] [---------]   . */
/* >     [ X21 | X22 ]   [    | P2 ] [ B21 | B22 0  0 ] [    | Q2 ] */
/* >                                 [  0  |  0  0  I ] */
/* > */
/* > X11 is P-by-Q. Q must be no larger than P, M-P, or M-Q. (If this is */
/* > not the case, then X must be transposed and/or permuted. This can be */
/* > done in constant time using the TRANS and SIGNS options. See ZUNCSD */
/* > for details.) */
/* > */
/* > The unitary matrices P1, P2, Q1, and Q2 are P-by-P, (M-P)-by- */
/* > (M-P), Q-by-Q, and (M-Q)-by-(M-Q), respectively. They are */
/* > represented implicitly by Householder vectors. */
/* > */
/* > B11, B12, B21, and B22 are Q-by-Q bidiagonal matrices represented */
/* > implicitly by angles THETA, PHI. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

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
/* >          The number of columns in X11 and X21. 0 <= Q <= */
/* >          MIN(P,M-P,M-Q). */
/* > \endverbatim */
/* > */
/* > \param[in,out] X11 */
/* > \verbatim */
/* >          X11 is COMPLEX*16 array, dimension (LDX11,Q) */
/* >          On entry, the top-left block of the unitary matrix to be */
/* >          reduced. On exit, the form depends on TRANS: */
/* >          If TRANS = 'N', then */
/* >             the columns of tril(X11) specify reflectors for P1, */
/* >             the rows of triu(X11,1) specify reflectors for Q1; */
/* >          else TRANS = 'T', and */
/* >             the rows of triu(X11) specify reflectors for P1, */
/* >             the columns of tril(X11,-1) specify reflectors for Q1. */
/* > \endverbatim */
/* > */
/* > \param[in] LDX11 */
/* > \verbatim */
/* >          LDX11 is INTEGER */
/* >          The leading dimension of X11. If TRANS = 'N', then LDX11 >= */
/* >          P; else LDX11 >= Q. */
/* > \endverbatim */
/* > */
/* > \param[in,out] X12 */
/* > \verbatim */
/* >          X12 is COMPLEX*16 array, dimension (LDX12,M-Q) */
/* >          On entry, the top-right block of the unitary matrix to */
/* >          be reduced. On exit, the form depends on TRANS: */
/* >          If TRANS = 'N', then */
/* >             the rows of triu(X12) specify the first P reflectors for */
/* >             Q2; */
/* >          else TRANS = 'T', and */
/* >             the columns of tril(X12) specify the first P reflectors */
/* >             for Q2. */
/* > \endverbatim */
/* > */
/* > \param[in] LDX12 */
/* > \verbatim */
/* >          LDX12 is INTEGER */
/* >          The leading dimension of X12. If TRANS = 'N', then LDX12 >= */
/* >          P; else LDX11 >= M-Q. */
/* > \endverbatim */
/* > */
/* > \param[in,out] X21 */
/* > \verbatim */
/* >          X21 is COMPLEX*16 array, dimension (LDX21,Q) */
/* >          On entry, the bottom-left block of the unitary matrix to */
/* >          be reduced. On exit, the form depends on TRANS: */
/* >          If TRANS = 'N', then */
/* >             the columns of tril(X21) specify reflectors for P2; */
/* >          else TRANS = 'T', and */
/* >             the rows of triu(X21) specify reflectors for P2. */
/* > \endverbatim */
/* > */
/* > \param[in] LDX21 */
/* > \verbatim */
/* >          LDX21 is INTEGER */
/* >          The leading dimension of X21. If TRANS = 'N', then LDX21 >= */
/* >          M-P; else LDX21 >= Q. */
/* > \endverbatim */
/* > */
/* > \param[in,out] X22 */
/* > \verbatim */
/* >          X22 is COMPLEX*16 array, dimension (LDX22,M-Q) */
/* >          On entry, the bottom-right block of the unitary matrix to */
/* >          be reduced. On exit, the form depends on TRANS: */
/* >          If TRANS = 'N', then */
/* >             the rows of triu(X22(Q+1:M-P,P+1:M-Q)) specify the last */
/* >             M-P-Q reflectors for Q2, */
/* >          else TRANS = 'T', and */
/* >             the columns of tril(X22(P+1:M-Q,Q+1:M-P)) specify the last */
/* >             M-P-Q reflectors for P2. */
/* > \endverbatim */
/* > */
/* > \param[in] LDX22 */
/* > \verbatim */
/* >          LDX22 is INTEGER */
/* >          The leading dimension of X22. If TRANS = 'N', then LDX22 >= */
/* >          M-P; else LDX22 >= M-Q. */
/* > \endverbatim */
/* > */
/* > \param[out] THETA */
/* > \verbatim */
/* >          THETA is DOUBLE PRECISION array, dimension (Q) */
/* >          The entries of the bidiagonal blocks B11, B12, B21, B22 can */
/* >          be computed from the angles THETA and PHI. See Further */
/* >          Details. */
/* > \endverbatim */
/* > */
/* > \param[out] PHI */
/* > \verbatim */
/* >          PHI is DOUBLE PRECISION array, dimension (Q-1) */
/* >          The entries of the bidiagonal blocks B11, B12, B21, B22 can */
/* >          be computed from the angles THETA and PHI. See Further */
/* >          Details. */
/* > \endverbatim */
/* > */
/* > \param[out] TAUP1 */
/* > \verbatim */
/* >          TAUP1 is COMPLEX*16 array, dimension (P) */
/* >          The scalar factors of the elementary reflectors that define */
/* >          P1. */
/* > \endverbatim */
/* > */
/* > \param[out] TAUP2 */
/* > \verbatim */
/* >          TAUP2 is COMPLEX*16 array, dimension (M-P) */
/* >          The scalar factors of the elementary reflectors that define */
/* >          P2. */
/* > \endverbatim */
/* > */
/* > \param[out] TAUQ1 */
/* > \verbatim */
/* >          TAUQ1 is COMPLEX*16 array, dimension (Q) */
/* >          The scalar factors of the elementary reflectors that define */
/* >          Q1. */
/* > \endverbatim */
/* > */
/* > \param[out] TAUQ2 */
/* > \verbatim */
/* >          TAUQ2 is COMPLEX*16 array, dimension (M-Q) */
/* >          The scalar factors of the elementary reflectors that define */
/* >          Q2. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is COMPLEX*16 array, dimension (LWORK) */
/* > \endverbatim */
/* > */
/* > \param[in] LWORK */
/* > \verbatim */
/* >          LWORK is INTEGER */
/* >          The dimension of the array WORK. LWORK >= M-Q. */
/* > */
/* >          If LWORK = -1, then a workspace query is assumed; the routine */
/* >          only calculates the optimal size of the WORK array, returns */
/* >          this value as the first entry of the WORK array, and no error */
/* >          message related to LWORK is issued by XERBLA. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit. */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup complex16OTHERcomputational */

/* > \par Further Details: */
/*  ===================== */
/* > */
/* > \verbatim */
/* > */
/* >  The bidiagonal blocks B11, B12, B21, and B22 are represented */
/* >  implicitly by angles THETA(1), ..., THETA(Q) and PHI(1), ..., */
/* >  PHI(Q-1). B11 and B21 are upper bidiagonal, while B21 and B22 are */
/* >  lower bidiagonal. Every entry in each bidiagonal band is a product */
/* >  of a sine or cosine of a THETA with a sine or cosine of a PHI. See */
/* >  [1] or ZUNCSD for details. */
/* > */
/* >  P1, P2, Q1, and Q2 are represented as products of elementary */
/* >  reflectors. See ZUNCSD for details on generating P1, P2, Q1, and Q2 */
/* >  using ZUNGQR and ZUNGLQ. */
/* > \endverbatim */

/* > \par References: */
/*  ================ */
/* > */
/* >  [1] Brian D. Sutton. Computing the complete CS decomposition. Numer. */
/* >      Algorithms, 50(1):33-65, 2009. */
/* > */
/*  ===================================================================== */
/* Subroutine */ void zunbdb_(char *trans, char *signs, integer *m, integer *p,
	 integer *q, doublecomplex *x11, integer *ldx11, doublecomplex *x12, 
	integer *ldx12, doublecomplex *x21, integer *ldx21, doublecomplex *
	x22, integer *ldx22, doublereal *theta, doublereal *phi, 
	doublecomplex *taup1, doublecomplex *taup2, doublecomplex *tauq1, 
	doublecomplex *tauq2, doublecomplex *work, integer *lwork, integer *
	info)
{
    /* System generated locals */
    integer x11_dim1, x11_offset, x12_dim1, x12_offset, x21_dim1, x21_offset, 
	    x22_dim1, x22_offset, i__1, i__2, i__3;
    doublereal d__1;
    doublecomplex z__1;

    /* Local variables */
    logical colmajor;
    integer lworkmin, lworkopt, i__;
    extern logical lsame_(char *, char *);
    extern /* Subroutine */ void zscal_(integer *, doublecomplex *, 
	    doublecomplex *, integer *), zlarf_(char *, integer *, integer *, 
	    doublecomplex *, integer *, doublecomplex *, doublecomplex *, 
	    integer *, doublecomplex *);
    doublereal z1, z2, z3, z4;
    extern /* Subroutine */ void zaxpy_(integer *, doublecomplex *, 
	    doublecomplex *, integer *, doublecomplex *, integer *);
    extern doublereal dznrm2_(integer *, doublecomplex *, integer *);
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    extern void zlacgv_(
	    integer *, doublecomplex *, integer *);
    logical lquery;
    extern /* Subroutine */ void zlarfgp_(integer *, doublecomplex *, 
	    doublecomplex *, integer *, doublecomplex *);


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ==================================================================== */



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
    --phi;
    --taup1;
    --taup2;
    --tauq1;
    --tauq2;
    --work;

    /* Function Body */
    *info = 0;
    colmajor = ! lsame_(trans, "T");
    if (! lsame_(signs, "O")) {
	z1 = 1.;
	z2 = 1.;
	z3 = 1.;
	z4 = 1.;
    } else {
	z1 = 1.;
	z2 = -1.;
	z3 = 1.;
	z4 = -1.;
    }
    lquery = *lwork == -1;

    if (*m < 0) {
	*info = -3;
    } else if (*p < 0 || *p > *m) {
	*info = -4;
    } else if (*q < 0 || *q > *p || *q > *m - *p || *q > *m - *q) {
	*info = -5;
    } else if (colmajor && *ldx11 < f2cmax(1,*p)) {
	*info = -7;
    } else if (! colmajor && *ldx11 < f2cmax(1,*q)) {
	*info = -7;
    } else if (colmajor && *ldx12 < f2cmax(1,*p)) {
	*info = -9;
    } else /* if(complicated condition) */ {
/* Computing MAX */
	i__1 = 1, i__2 = *m - *q;
	if (! colmajor && *ldx12 < f2cmax(i__1,i__2)) {
	    *info = -9;
	} else /* if(complicated condition) */ {
/* Computing MAX */
	    i__1 = 1, i__2 = *m - *p;
	    if (colmajor && *ldx21 < f2cmax(i__1,i__2)) {
		*info = -11;
	    } else if (! colmajor && *ldx21 < f2cmax(1,*q)) {
		*info = -11;
	    } else /* if(complicated condition) */ {
/* Computing MAX */
		i__1 = 1, i__2 = *m - *p;
		if (colmajor && *ldx22 < f2cmax(i__1,i__2)) {
		    *info = -13;
		} else /* if(complicated condition) */ {
/* Computing MAX */
		    i__1 = 1, i__2 = *m - *q;
		    if (! colmajor && *ldx22 < f2cmax(i__1,i__2)) {
			*info = -13;
		    }
		}
	    }
	}
    }

/*     Compute workspace */

    if (*info == 0) {
	lworkopt = *m - *q;
	lworkmin = *m - *q;
	work[1].r = (doublereal) lworkopt, work[1].i = 0.;
	if (*lwork < lworkmin && ! lquery) {
	    *info = -21;
	}
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("xORBDB", &i__1, (ftnlen)6);
	return;
    } else if (lquery) {
	return;
    }

/*     Handle column-major and row-major separately */

    if (colmajor) {

/*        Reduce columns 1, ..., Q of X11, X12, X21, and X22 */

	i__1 = *q;
	for (i__ = 1; i__ <= i__1; ++i__) {

	    if (i__ == 1) {
		i__2 = *p - i__ + 1;
		z__1.r = z1, z__1.i = 0.;
		zscal_(&i__2, &z__1, &x11[i__ + i__ * x11_dim1], &c__1);
	    } else {
		i__2 = *p - i__ + 1;
		d__1 = z1 * cos(phi[i__ - 1]);
		z__1.r = d__1, z__1.i = 0.;
		zscal_(&i__2, &z__1, &x11[i__ + i__ * x11_dim1], &c__1);
		i__2 = *p - i__ + 1;
		d__1 = -z1 * z3 * z4 * sin(phi[i__ - 1]);
		z__1.r = d__1, z__1.i = 0.;
		zaxpy_(&i__2, &z__1, &x12[i__ + (i__ - 1) * x12_dim1], &c__1, 
			&x11[i__ + i__ * x11_dim1], &c__1);
	    }
	    if (i__ == 1) {
		i__2 = *m - *p - i__ + 1;
		z__1.r = z2, z__1.i = 0.;
		zscal_(&i__2, &z__1, &x21[i__ + i__ * x21_dim1], &c__1);
	    } else {
		i__2 = *m - *p - i__ + 1;
		d__1 = z2 * cos(phi[i__ - 1]);
		z__1.r = d__1, z__1.i = 0.;
		zscal_(&i__2, &z__1, &x21[i__ + i__ * x21_dim1], &c__1);
		i__2 = *m - *p - i__ + 1;
		d__1 = -z2 * z3 * z4 * sin(phi[i__ - 1]);
		z__1.r = d__1, z__1.i = 0.;
		zaxpy_(&i__2, &z__1, &x22[i__ + (i__ - 1) * x22_dim1], &c__1, 
			&x21[i__ + i__ * x21_dim1], &c__1);
	    }

	    i__2 = *m - *p - i__ + 1;
	    i__3 = *p - i__ + 1;
	    theta[i__] = atan2(dznrm2_(&i__2, &x21[i__ + i__ * x21_dim1], &
		    c__1), dznrm2_(&i__3, &x11[i__ + i__ * x11_dim1], &c__1));

	    if (*p > i__) {
		i__2 = *p - i__ + 1;
		zlarfgp_(&i__2, &x11[i__ + i__ * x11_dim1], &x11[i__ + 1 + 
			i__ * x11_dim1], &c__1, &taup1[i__]);
	    } else if (*p == i__) {
		i__2 = *p - i__ + 1;
		zlarfgp_(&i__2, &x11[i__ + i__ * x11_dim1], &x11[i__ + i__ * 
			x11_dim1], &c__1, &taup1[i__]);
	    }
	    i__2 = i__ + i__ * x11_dim1;
	    x11[i__2].r = 1., x11[i__2].i = 0.;
	    if (*m - *p > i__) {
		i__2 = *m - *p - i__ + 1;
		zlarfgp_(&i__2, &x21[i__ + i__ * x21_dim1], &x21[i__ + 1 + 
			i__ * x21_dim1], &c__1, &taup2[i__]);
	    } else if (*m - *p == i__) {
		i__2 = *m - *p - i__ + 1;
		zlarfgp_(&i__2, &x21[i__ + i__ * x21_dim1], &x21[i__ + i__ * 
			x21_dim1], &c__1, &taup2[i__]);
	    }
	    i__2 = i__ + i__ * x21_dim1;
	    x21[i__2].r = 1., x21[i__2].i = 0.;

	    if (*q > i__) {
		i__2 = *p - i__ + 1;
		i__3 = *q - i__;
		d_cnjg(&z__1, &taup1[i__]);
		zlarf_("L", &i__2, &i__3, &x11[i__ + i__ * x11_dim1], &c__1, &
			z__1, &x11[i__ + (i__ + 1) * x11_dim1], ldx11, &work[
			1]);
		i__2 = *m - *p - i__ + 1;
		i__3 = *q - i__;
		d_cnjg(&z__1, &taup2[i__]);
		zlarf_("L", &i__2, &i__3, &x21[i__ + i__ * x21_dim1], &c__1, &
			z__1, &x21[i__ + (i__ + 1) * x21_dim1], ldx21, &work[
			1]);
	    }
	    if (*m - *q + 1 > i__) {
		i__2 = *p - i__ + 1;
		i__3 = *m - *q - i__ + 1;
		d_cnjg(&z__1, &taup1[i__]);
		zlarf_("L", &i__2, &i__3, &x11[i__ + i__ * x11_dim1], &c__1, &
			z__1, &x12[i__ + i__ * x12_dim1], ldx12, &work[1]);
		i__2 = *m - *p - i__ + 1;
		i__3 = *m - *q - i__ + 1;
		d_cnjg(&z__1, &taup2[i__]);
		zlarf_("L", &i__2, &i__3, &x21[i__ + i__ * x21_dim1], &c__1, &
			z__1, &x22[i__ + i__ * x22_dim1], ldx22, &work[1]);
	    }

	    if (i__ < *q) {
		i__2 = *q - i__;
		d__1 = -z1 * z3 * sin(theta[i__]);
		z__1.r = d__1, z__1.i = 0.;
		zscal_(&i__2, &z__1, &x11[i__ + (i__ + 1) * x11_dim1], ldx11);
		i__2 = *q - i__;
		d__1 = z2 * z3 * cos(theta[i__]);
		z__1.r = d__1, z__1.i = 0.;
		zaxpy_(&i__2, &z__1, &x21[i__ + (i__ + 1) * x21_dim1], ldx21, 
			&x11[i__ + (i__ + 1) * x11_dim1], ldx11);
	    }
	    i__2 = *m - *q - i__ + 1;
	    d__1 = -z1 * z4 * sin(theta[i__]);
	    z__1.r = d__1, z__1.i = 0.;
	    zscal_(&i__2, &z__1, &x12[i__ + i__ * x12_dim1], ldx12);
	    i__2 = *m - *q - i__ + 1;
	    d__1 = z2 * z4 * cos(theta[i__]);
	    z__1.r = d__1, z__1.i = 0.;
	    zaxpy_(&i__2, &z__1, &x22[i__ + i__ * x22_dim1], ldx22, &x12[i__ 
		    + i__ * x12_dim1], ldx12);

	    if (i__ < *q) {
		i__2 = *q - i__;
		i__3 = *m - *q - i__ + 1;
		phi[i__] = atan2(dznrm2_(&i__2, &x11[i__ + (i__ + 1) * 
			x11_dim1], ldx11), dznrm2_(&i__3, &x12[i__ + i__ * 
			x12_dim1], ldx12));
	    }

	    if (i__ < *q) {
		i__2 = *q - i__;
		zlacgv_(&i__2, &x11[i__ + (i__ + 1) * x11_dim1], ldx11);
		if (i__ == *q - 1) {
		    i__2 = *q - i__;
		    zlarfgp_(&i__2, &x11[i__ + (i__ + 1) * x11_dim1], &x11[
			    i__ + (i__ + 1) * x11_dim1], ldx11, &tauq1[i__]);
		} else {
		    i__2 = *q - i__;
		    zlarfgp_(&i__2, &x11[i__ + (i__ + 1) * x11_dim1], &x11[
			    i__ + (i__ + 2) * x11_dim1], ldx11, &tauq1[i__]);
		}
		i__2 = i__ + (i__ + 1) * x11_dim1;
		x11[i__2].r = 1., x11[i__2].i = 0.;
	    }
	    if (*m - *q + 1 > i__) {
		i__2 = *m - *q - i__ + 1;
		zlacgv_(&i__2, &x12[i__ + i__ * x12_dim1], ldx12);
		if (*m - *q == i__) {
		    i__2 = *m - *q - i__ + 1;
		    zlarfgp_(&i__2, &x12[i__ + i__ * x12_dim1], &x12[i__ + 
			    i__ * x12_dim1], ldx12, &tauq2[i__]);
		} else {
		    i__2 = *m - *q - i__ + 1;
		    zlarfgp_(&i__2, &x12[i__ + i__ * x12_dim1], &x12[i__ + (
			    i__ + 1) * x12_dim1], ldx12, &tauq2[i__]);
		}
	    }
	    i__2 = i__ + i__ * x12_dim1;
	    x12[i__2].r = 1., x12[i__2].i = 0.;

	    if (i__ < *q) {
		i__2 = *p - i__;
		i__3 = *q - i__;
		zlarf_("R", &i__2, &i__3, &x11[i__ + (i__ + 1) * x11_dim1], 
			ldx11, &tauq1[i__], &x11[i__ + 1 + (i__ + 1) * 
			x11_dim1], ldx11, &work[1]);
		i__2 = *m - *p - i__;
		i__3 = *q - i__;
		zlarf_("R", &i__2, &i__3, &x11[i__ + (i__ + 1) * x11_dim1], 
			ldx11, &tauq1[i__], &x21[i__ + 1 + (i__ + 1) * 
			x21_dim1], ldx21, &work[1]);
	    }
	    if (*p > i__) {
		i__2 = *p - i__;
		i__3 = *m - *q - i__ + 1;
		zlarf_("R", &i__2, &i__3, &x12[i__ + i__ * x12_dim1], ldx12, &
			tauq2[i__], &x12[i__ + 1 + i__ * x12_dim1], ldx12, &
			work[1]);
	    }
	    if (*m - *p > i__) {
		i__2 = *m - *p - i__;
		i__3 = *m - *q - i__ + 1;
		zlarf_("R", &i__2, &i__3, &x12[i__ + i__ * x12_dim1], ldx12, &
			tauq2[i__], &x22[i__ + 1 + i__ * x22_dim1], ldx22, &
			work[1]);
	    }

	    if (i__ < *q) {
		i__2 = *q - i__;
		zlacgv_(&i__2, &x11[i__ + (i__ + 1) * x11_dim1], ldx11);
	    }
	    i__2 = *m - *q - i__ + 1;
	    zlacgv_(&i__2, &x12[i__ + i__ * x12_dim1], ldx12);

	}

/*        Reduce columns Q + 1, ..., P of X12, X22 */

	i__1 = *p;
	for (i__ = *q + 1; i__ <= i__1; ++i__) {

	    i__2 = *m - *q - i__ + 1;
	    d__1 = -z1 * z4;
	    z__1.r = d__1, z__1.i = 0.;
	    zscal_(&i__2, &z__1, &x12[i__ + i__ * x12_dim1], ldx12);
	    i__2 = *m - *q - i__ + 1;
	    zlacgv_(&i__2, &x12[i__ + i__ * x12_dim1], ldx12);
	    if (i__ >= *m - *q) {
		i__2 = *m - *q - i__ + 1;
		zlarfgp_(&i__2, &x12[i__ + i__ * x12_dim1], &x12[i__ + i__ * 
			x12_dim1], ldx12, &tauq2[i__]);
	    } else {
		i__2 = *m - *q - i__ + 1;
		zlarfgp_(&i__2, &x12[i__ + i__ * x12_dim1], &x12[i__ + (i__ + 
			1) * x12_dim1], ldx12, &tauq2[i__]);
	    }
	    i__2 = i__ + i__ * x12_dim1;
	    x12[i__2].r = 1., x12[i__2].i = 0.;

	    if (*p > i__) {
		i__2 = *p - i__;
		i__3 = *m - *q - i__ + 1;
		zlarf_("R", &i__2, &i__3, &x12[i__ + i__ * x12_dim1], ldx12, &
			tauq2[i__], &x12[i__ + 1 + i__ * x12_dim1], ldx12, &
			work[1]);
	    }
	    if (*m - *p - *q >= 1) {
		i__2 = *m - *p - *q;
		i__3 = *m - *q - i__ + 1;
		zlarf_("R", &i__2, &i__3, &x12[i__ + i__ * x12_dim1], ldx12, &
			tauq2[i__], &x22[*q + 1 + i__ * x22_dim1], ldx22, &
			work[1]);
	    }

	    i__2 = *m - *q - i__ + 1;
	    zlacgv_(&i__2, &x12[i__ + i__ * x12_dim1], ldx12);

	}

/*        Reduce columns P + 1, ..., M - Q of X12, X22 */

	i__1 = *m - *p - *q;
	for (i__ = 1; i__ <= i__1; ++i__) {

	    i__2 = *m - *p - *q - i__ + 1;
	    d__1 = z2 * z4;
	    z__1.r = d__1, z__1.i = 0.;
	    zscal_(&i__2, &z__1, &x22[*q + i__ + (*p + i__) * x22_dim1], 
		    ldx22);
	    i__2 = *m - *p - *q - i__ + 1;
	    zlacgv_(&i__2, &x22[*q + i__ + (*p + i__) * x22_dim1], ldx22);
	    i__2 = *m - *p - *q - i__ + 1;
	    zlarfgp_(&i__2, &x22[*q + i__ + (*p + i__) * x22_dim1], &x22[*q + 
		    i__ + (*p + i__ + 1) * x22_dim1], ldx22, &tauq2[*p + i__])
		    ;
	    i__2 = *q + i__ + (*p + i__) * x22_dim1;
	    x22[i__2].r = 1., x22[i__2].i = 0.;
	    i__2 = *m - *p - *q - i__;
	    i__3 = *m - *p - *q - i__ + 1;
	    zlarf_("R", &i__2, &i__3, &x22[*q + i__ + (*p + i__) * x22_dim1], 
		    ldx22, &tauq2[*p + i__], &x22[*q + i__ + 1 + (*p + i__) * 
		    x22_dim1], ldx22, &work[1]);

	    i__2 = *m - *p - *q - i__ + 1;
	    zlacgv_(&i__2, &x22[*q + i__ + (*p + i__) * x22_dim1], ldx22);

	}

    } else {

/*        Reduce columns 1, ..., Q of X11, X12, X21, X22 */

	i__1 = *q;
	for (i__ = 1; i__ <= i__1; ++i__) {

	    if (i__ == 1) {
		i__2 = *p - i__ + 1;
		z__1.r = z1, z__1.i = 0.;
		zscal_(&i__2, &z__1, &x11[i__ + i__ * x11_dim1], ldx11);
	    } else {
		i__2 = *p - i__ + 1;
		d__1 = z1 * cos(phi[i__ - 1]);
		z__1.r = d__1, z__1.i = 0.;
		zscal_(&i__2, &z__1, &x11[i__ + i__ * x11_dim1], ldx11);
		i__2 = *p - i__ + 1;
		d__1 = -z1 * z3 * z4 * sin(phi[i__ - 1]);
		z__1.r = d__1, z__1.i = 0.;
		zaxpy_(&i__2, &z__1, &x12[i__ - 1 + i__ * x12_dim1], ldx12, &
			x11[i__ + i__ * x11_dim1], ldx11);
	    }
	    if (i__ == 1) {
		i__2 = *m - *p - i__ + 1;
		z__1.r = z2, z__1.i = 0.;
		zscal_(&i__2, &z__1, &x21[i__ + i__ * x21_dim1], ldx21);
	    } else {
		i__2 = *m - *p - i__ + 1;
		d__1 = z2 * cos(phi[i__ - 1]);
		z__1.r = d__1, z__1.i = 0.;
		zscal_(&i__2, &z__1, &x21[i__ + i__ * x21_dim1], ldx21);
		i__2 = *m - *p - i__ + 1;
		d__1 = -z2 * z3 * z4 * sin(phi[i__ - 1]);
		z__1.r = d__1, z__1.i = 0.;
		zaxpy_(&i__2, &z__1, &x22[i__ - 1 + i__ * x22_dim1], ldx22, &
			x21[i__ + i__ * x21_dim1], ldx21);
	    }

	    i__2 = *m - *p - i__ + 1;
	    i__3 = *p - i__ + 1;
	    theta[i__] = atan2(dznrm2_(&i__2, &x21[i__ + i__ * x21_dim1], 
		    ldx21), dznrm2_(&i__3, &x11[i__ + i__ * x11_dim1], ldx11))
		    ;

	    i__2 = *p - i__ + 1;
	    zlacgv_(&i__2, &x11[i__ + i__ * x11_dim1], ldx11);
	    i__2 = *m - *p - i__ + 1;
	    zlacgv_(&i__2, &x21[i__ + i__ * x21_dim1], ldx21);

	    i__2 = *p - i__ + 1;
	    zlarfgp_(&i__2, &x11[i__ + i__ * x11_dim1], &x11[i__ + (i__ + 1) *
		     x11_dim1], ldx11, &taup1[i__]);
	    i__2 = i__ + i__ * x11_dim1;
	    x11[i__2].r = 1., x11[i__2].i = 0.;
	    if (i__ == *m - *p) {
		i__2 = *m - *p - i__ + 1;
		zlarfgp_(&i__2, &x21[i__ + i__ * x21_dim1], &x21[i__ + i__ * 
			x21_dim1], ldx21, &taup2[i__]);
	    } else {
		i__2 = *m - *p - i__ + 1;
		zlarfgp_(&i__2, &x21[i__ + i__ * x21_dim1], &x21[i__ + (i__ + 
			1) * x21_dim1], ldx21, &taup2[i__]);
	    }
	    i__2 = i__ + i__ * x21_dim1;
	    x21[i__2].r = 1., x21[i__2].i = 0.;

	    i__2 = *q - i__;
	    i__3 = *p - i__ + 1;
	    zlarf_("R", &i__2, &i__3, &x11[i__ + i__ * x11_dim1], ldx11, &
		    taup1[i__], &x11[i__ + 1 + i__ * x11_dim1], ldx11, &work[
		    1]);
	    i__2 = *m - *q - i__ + 1;
	    i__3 = *p - i__ + 1;
	    zlarf_("R", &i__2, &i__3, &x11[i__ + i__ * x11_dim1], ldx11, &
		    taup1[i__], &x12[i__ + i__ * x12_dim1], ldx12, &work[1]);
	    i__2 = *q - i__;
	    i__3 = *m - *p - i__ + 1;
	    zlarf_("R", &i__2, &i__3, &x21[i__ + i__ * x21_dim1], ldx21, &
		    taup2[i__], &x21[i__ + 1 + i__ * x21_dim1], ldx21, &work[
		    1]);
	    i__2 = *m - *q - i__ + 1;
	    i__3 = *m - *p - i__ + 1;
	    zlarf_("R", &i__2, &i__3, &x21[i__ + i__ * x21_dim1], ldx21, &
		    taup2[i__], &x22[i__ + i__ * x22_dim1], ldx22, &work[1]);

	    i__2 = *p - i__ + 1;
	    zlacgv_(&i__2, &x11[i__ + i__ * x11_dim1], ldx11);
	    i__2 = *m - *p - i__ + 1;
	    zlacgv_(&i__2, &x21[i__ + i__ * x21_dim1], ldx21);

	    if (i__ < *q) {
		i__2 = *q - i__;
		d__1 = -z1 * z3 * sin(theta[i__]);
		z__1.r = d__1, z__1.i = 0.;
		zscal_(&i__2, &z__1, &x11[i__ + 1 + i__ * x11_dim1], &c__1);
		i__2 = *q - i__;
		d__1 = z2 * z3 * cos(theta[i__]);
		z__1.r = d__1, z__1.i = 0.;
		zaxpy_(&i__2, &z__1, &x21[i__ + 1 + i__ * x21_dim1], &c__1, &
			x11[i__ + 1 + i__ * x11_dim1], &c__1);
	    }
	    i__2 = *m - *q - i__ + 1;
	    d__1 = -z1 * z4 * sin(theta[i__]);
	    z__1.r = d__1, z__1.i = 0.;
	    zscal_(&i__2, &z__1, &x12[i__ + i__ * x12_dim1], &c__1);
	    i__2 = *m - *q - i__ + 1;
	    d__1 = z2 * z4 * cos(theta[i__]);
	    z__1.r = d__1, z__1.i = 0.;
	    zaxpy_(&i__2, &z__1, &x22[i__ + i__ * x22_dim1], &c__1, &x12[i__ 
		    + i__ * x12_dim1], &c__1);

	    if (i__ < *q) {
		i__2 = *q - i__;
		i__3 = *m - *q - i__ + 1;
		phi[i__] = atan2(dznrm2_(&i__2, &x11[i__ + 1 + i__ * x11_dim1]
			, &c__1), dznrm2_(&i__3, &x12[i__ + i__ * x12_dim1], &
			c__1));
	    }

	    if (i__ < *q) {
		i__2 = *q - i__;
		zlarfgp_(&i__2, &x11[i__ + 1 + i__ * x11_dim1], &x11[i__ + 2 
			+ i__ * x11_dim1], &c__1, &tauq1[i__]);
		i__2 = i__ + 1 + i__ * x11_dim1;
		x11[i__2].r = 1., x11[i__2].i = 0.;
	    }
	    i__2 = *m - *q - i__ + 1;
	    zlarfgp_(&i__2, &x12[i__ + i__ * x12_dim1], &x12[i__ + 1 + i__ * 
		    x12_dim1], &c__1, &tauq2[i__]);
	    i__2 = i__ + i__ * x12_dim1;
	    x12[i__2].r = 1., x12[i__2].i = 0.;

	    if (i__ < *q) {
		i__2 = *q - i__;
		i__3 = *p - i__;
		d_cnjg(&z__1, &tauq1[i__]);
		zlarf_("L", &i__2, &i__3, &x11[i__ + 1 + i__ * x11_dim1], &
			c__1, &z__1, &x11[i__ + 1 + (i__ + 1) * x11_dim1], 
			ldx11, &work[1]);
		i__2 = *q - i__;
		i__3 = *m - *p - i__;
		d_cnjg(&z__1, &tauq1[i__]);
		zlarf_("L", &i__2, &i__3, &x11[i__ + 1 + i__ * x11_dim1], &
			c__1, &z__1, &x21[i__ + 1 + (i__ + 1) * x21_dim1], 
			ldx21, &work[1]);
	    }
	    i__2 = *m - *q - i__ + 1;
	    i__3 = *p - i__;
	    d_cnjg(&z__1, &tauq2[i__]);
	    zlarf_("L", &i__2, &i__3, &x12[i__ + i__ * x12_dim1], &c__1, &
		    z__1, &x12[i__ + (i__ + 1) * x12_dim1], ldx12, &work[1]);
	    if (*m - *p > i__) {
		i__2 = *m - *q - i__ + 1;
		i__3 = *m - *p - i__;
		d_cnjg(&z__1, &tauq2[i__]);
		zlarf_("L", &i__2, &i__3, &x12[i__ + i__ * x12_dim1], &c__1, &
			z__1, &x22[i__ + (i__ + 1) * x22_dim1], ldx22, &work[
			1]);
	    }

	}

/*        Reduce columns Q + 1, ..., P of X12, X22 */

	i__1 = *p;
	for (i__ = *q + 1; i__ <= i__1; ++i__) {

	    i__2 = *m - *q - i__ + 1;
	    d__1 = -z1 * z4;
	    z__1.r = d__1, z__1.i = 0.;
	    zscal_(&i__2, &z__1, &x12[i__ + i__ * x12_dim1], &c__1);
	    i__2 = *m - *q - i__ + 1;
	    zlarfgp_(&i__2, &x12[i__ + i__ * x12_dim1], &x12[i__ + 1 + i__ * 
		    x12_dim1], &c__1, &tauq2[i__]);
	    i__2 = i__ + i__ * x12_dim1;
	    x12[i__2].r = 1., x12[i__2].i = 0.;

	    if (*p > i__) {
		i__2 = *m - *q - i__ + 1;
		i__3 = *p - i__;
		d_cnjg(&z__1, &tauq2[i__]);
		zlarf_("L", &i__2, &i__3, &x12[i__ + i__ * x12_dim1], &c__1, &
			z__1, &x12[i__ + (i__ + 1) * x12_dim1], ldx12, &work[
			1]);
	    }
	    if (*m - *p - *q >= 1) {
		i__2 = *m - *q - i__ + 1;
		i__3 = *m - *p - *q;
		d_cnjg(&z__1, &tauq2[i__]);
		zlarf_("L", &i__2, &i__3, &x12[i__ + i__ * x12_dim1], &c__1, &
			z__1, &x22[i__ + (*q + 1) * x22_dim1], ldx22, &work[1]
			);
	    }

	}

/*        Reduce columns P + 1, ..., M - Q of X12, X22 */

	i__1 = *m - *p - *q;
	for (i__ = 1; i__ <= i__1; ++i__) {

	    i__2 = *m - *p - *q - i__ + 1;
	    d__1 = z2 * z4;
	    z__1.r = d__1, z__1.i = 0.;
	    zscal_(&i__2, &z__1, &x22[*p + i__ + (*q + i__) * x22_dim1], &
		    c__1);
	    i__2 = *m - *p - *q - i__ + 1;
	    zlarfgp_(&i__2, &x22[*p + i__ + (*q + i__) * x22_dim1], &x22[*p + 
		    i__ + 1 + (*q + i__) * x22_dim1], &c__1, &tauq2[*p + i__])
		    ;
	    i__2 = *p + i__ + (*q + i__) * x22_dim1;
	    x22[i__2].r = 1., x22[i__2].i = 0.;

	    if (*m - *p - *q != i__) {
		i__2 = *m - *p - *q - i__ + 1;
		i__3 = *m - *p - *q - i__;
		d_cnjg(&z__1, &tauq2[*p + i__]);
		zlarf_("L", &i__2, &i__3, &x22[*p + i__ + (*q + i__) * 
			x22_dim1], &c__1, &z__1, &x22[*p + i__ + (*q + i__ + 
			1) * x22_dim1], ldx22, &work[1]);
	    }

	}

    }

    return;

/*     End of ZUNBDB */

} /* zunbdb_ */

