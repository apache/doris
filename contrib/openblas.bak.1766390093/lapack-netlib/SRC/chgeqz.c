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
static _Fcomplex cpow_ui(_Fcomplex x, integer n) {
	_Fcomplex pow={1.0,0.0}; complex tmp; unsigned long int u;
		if(n != 0) {
		if(n < 0) n = -n, x._Val[0] = 1./x._Val[0], x._Val[1]=1./x._Val[1];
		for(u = n; ; ) {
			if(u & 01) pow = _FCmulcc(pow,x);
			if(u >>= 1) x = _FCmulcc(x,x);
			else break;
		}
	}
	return pow;
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
			if(u & 01) pow = _Cmulcc(pow,x);
			if(u >>= 1) x = _Cmulcc(x,x);
			else break;
		}
	}
	return pow;
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

static complex c_b1 = {0.f,0.f};
static complex c_b2 = {1.f,0.f};
static integer c__1 = 1;
static integer c__2 = 2;

/* > \brief \b CHGEQZ */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download CHGEQZ + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/chgeqz.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/chgeqz.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/chgeqz.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE CHGEQZ( JOB, COMPQ, COMPZ, N, ILO, IHI, H, LDH, T, LDT, */
/*                          ALPHA, BETA, Q, LDQ, Z, LDZ, WORK, LWORK, */
/*                          RWORK, INFO ) */

/*       CHARACTER          COMPQ, COMPZ, JOB */
/*       INTEGER            IHI, ILO, INFO, LDH, LDQ, LDT, LDZ, LWORK, N */
/*       REAL               RWORK( * ) */
/*       COMPLEX            ALPHA( * ), BETA( * ), H( LDH, * ), */
/*      $                   Q( LDQ, * ), T( LDT, * ), WORK( * ), */
/*      $                   Z( LDZ, * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > CHGEQZ computes the eigenvalues of a complex matrix pair (H,T), */
/* > where H is an upper Hessenberg matrix and T is upper triangular, */
/* > using the single-shift QZ method. */
/* > Matrix pairs of this type are produced by the reduction to */
/* > generalized upper Hessenberg form of a complex matrix pair (A,B): */
/* > */
/* >    A = Q1*H*Z1**H,  B = Q1*T*Z1**H, */
/* > */
/* > as computed by CGGHRD. */
/* > */
/* > If JOB='S', then the Hessenberg-triangular pair (H,T) is */
/* > also reduced to generalized Schur form, */
/* > */
/* >    H = Q*S*Z**H,  T = Q*P*Z**H, */
/* > */
/* > where Q and Z are unitary matrices and S and P are upper triangular. */
/* > */
/* > Optionally, the unitary matrix Q from the generalized Schur */
/* > factorization may be postmultiplied into an input matrix Q1, and the */
/* > unitary matrix Z may be postmultiplied into an input matrix Z1. */
/* > If Q1 and Z1 are the unitary matrices from CGGHRD that reduced */
/* > the matrix pair (A,B) to generalized Hessenberg form, then the output */
/* > matrices Q1*Q and Z1*Z are the unitary factors from the generalized */
/* > Schur factorization of (A,B): */
/* > */
/* >    A = (Q1*Q)*S*(Z1*Z)**H,  B = (Q1*Q)*P*(Z1*Z)**H. */
/* > */
/* > To avoid overflow, eigenvalues of the matrix pair (H,T) */
/* > (equivalently, of (A,B)) are computed as a pair of complex values */
/* > (alpha,beta).  If beta is nonzero, lambda = alpha / beta is an */
/* > eigenvalue of the generalized nonsymmetric eigenvalue problem (GNEP) */
/* >    A*x = lambda*B*x */
/* > and if alpha is nonzero, mu = beta / alpha is an eigenvalue of the */
/* > alternate form of the GNEP */
/* >    mu*A*y = B*y. */
/* > The values of alpha and beta for the i-th eigenvalue can be read */
/* > directly from the generalized Schur form:  alpha = S(i,i), */
/* > beta = P(i,i). */
/* > */
/* > Ref: C.B. Moler & G.W. Stewart, "An Algorithm for Generalized Matrix */
/* >      Eigenvalue Problems", SIAM J. Numer. Anal., 10(1973), */
/* >      pp. 241--256. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] JOB */
/* > \verbatim */
/* >          JOB is CHARACTER*1 */
/* >          = 'E': Compute eigenvalues only; */
/* >          = 'S': Computer eigenvalues and the Schur form. */
/* > \endverbatim */
/* > */
/* > \param[in] COMPQ */
/* > \verbatim */
/* >          COMPQ is CHARACTER*1 */
/* >          = 'N': Left Schur vectors (Q) are not computed; */
/* >          = 'I': Q is initialized to the unit matrix and the matrix Q */
/* >                 of left Schur vectors of (H,T) is returned; */
/* >          = 'V': Q must contain a unitary matrix Q1 on entry and */
/* >                 the product Q1*Q is returned. */
/* > \endverbatim */
/* > */
/* > \param[in] COMPZ */
/* > \verbatim */
/* >          COMPZ is CHARACTER*1 */
/* >          = 'N': Right Schur vectors (Z) are not computed; */
/* >          = 'I': Q is initialized to the unit matrix and the matrix Z */
/* >                 of right Schur vectors of (H,T) is returned; */
/* >          = 'V': Z must contain a unitary matrix Z1 on entry and */
/* >                 the product Z1*Z is returned. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrices H, T, Q, and Z.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] ILO */
/* > \verbatim */
/* >          ILO is INTEGER */
/* > \endverbatim */
/* > */
/* > \param[in] IHI */
/* > \verbatim */
/* >          IHI is INTEGER */
/* >          ILO and IHI mark the rows and columns of H which are in */
/* >          Hessenberg form.  It is assumed that A is already upper */
/* >          triangular in rows and columns 1:ILO-1 and IHI+1:N. */
/* >          If N > 0, 1 <= ILO <= IHI <= N; if N = 0, ILO=1 and IHI=0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] H */
/* > \verbatim */
/* >          H is COMPLEX array, dimension (LDH, N) */
/* >          On entry, the N-by-N upper Hessenberg matrix H. */
/* >          On exit, if JOB = 'S', H contains the upper triangular */
/* >          matrix S from the generalized Schur factorization. */
/* >          If JOB = 'E', the diagonal of H matches that of S, but */
/* >          the rest of H is unspecified. */
/* > \endverbatim */
/* > */
/* > \param[in] LDH */
/* > \verbatim */
/* >          LDH is INTEGER */
/* >          The leading dimension of the array H.  LDH >= f2cmax( 1, N ). */
/* > \endverbatim */
/* > */
/* > \param[in,out] T */
/* > \verbatim */
/* >          T is COMPLEX array, dimension (LDT, N) */
/* >          On entry, the N-by-N upper triangular matrix T. */
/* >          On exit, if JOB = 'S', T contains the upper triangular */
/* >          matrix P from the generalized Schur factorization. */
/* >          If JOB = 'E', the diagonal of T matches that of P, but */
/* >          the rest of T is unspecified. */
/* > \endverbatim */
/* > */
/* > \param[in] LDT */
/* > \verbatim */
/* >          LDT is INTEGER */
/* >          The leading dimension of the array T.  LDT >= f2cmax( 1, N ). */
/* > \endverbatim */
/* > */
/* > \param[out] ALPHA */
/* > \verbatim */
/* >          ALPHA is COMPLEX array, dimension (N) */
/* >          The complex scalars alpha that define the eigenvalues of */
/* >          GNEP.  ALPHA(i) = S(i,i) in the generalized Schur */
/* >          factorization. */
/* > \endverbatim */
/* > */
/* > \param[out] BETA */
/* > \verbatim */
/* >          BETA is COMPLEX array, dimension (N) */
/* >          The real non-negative scalars beta that define the */
/* >          eigenvalues of GNEP.  BETA(i) = P(i,i) in the generalized */
/* >          Schur factorization. */
/* > */
/* >          Together, the quantities alpha = ALPHA(j) and beta = BETA(j) */
/* >          represent the j-th eigenvalue of the matrix pair (A,B), in */
/* >          one of the forms lambda = alpha/beta or mu = beta/alpha. */
/* >          Since either lambda or mu may overflow, they should not, */
/* >          in general, be computed. */
/* > \endverbatim */
/* > */
/* > \param[in,out] Q */
/* > \verbatim */
/* >          Q is COMPLEX array, dimension (LDQ, N) */
/* >          On entry, if COMPQ = 'V', the unitary matrix Q1 used in the */
/* >          reduction of (A,B) to generalized Hessenberg form. */
/* >          On exit, if COMPQ = 'I', the unitary matrix of left Schur */
/* >          vectors of (H,T), and if COMPQ = 'V', the unitary matrix of */
/* >          left Schur vectors of (A,B). */
/* >          Not referenced if COMPQ = 'N'. */
/* > \endverbatim */
/* > */
/* > \param[in] LDQ */
/* > \verbatim */
/* >          LDQ is INTEGER */
/* >          The leading dimension of the array Q.  LDQ >= 1. */
/* >          If COMPQ='V' or 'I', then LDQ >= N. */
/* > \endverbatim */
/* > */
/* > \param[in,out] Z */
/* > \verbatim */
/* >          Z is COMPLEX array, dimension (LDZ, N) */
/* >          On entry, if COMPZ = 'V', the unitary matrix Z1 used in the */
/* >          reduction of (A,B) to generalized Hessenberg form. */
/* >          On exit, if COMPZ = 'I', the unitary matrix of right Schur */
/* >          vectors of (H,T), and if COMPZ = 'V', the unitary matrix of */
/* >          right Schur vectors of (A,B). */
/* >          Not referenced if COMPZ = 'N'. */
/* > \endverbatim */
/* > */
/* > \param[in] LDZ */
/* > \verbatim */
/* >          LDZ is INTEGER */
/* >          The leading dimension of the array Z.  LDZ >= 1. */
/* >          If COMPZ='V' or 'I', then LDZ >= N. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is COMPLEX array, dimension (MAX(1,LWORK)) */
/* >          On exit, if INFO >= 0, WORK(1) returns the optimal LWORK. */
/* > \endverbatim */
/* > */
/* > \param[in] LWORK */
/* > \verbatim */
/* >          LWORK is INTEGER */
/* >          The dimension of the array WORK.  LWORK >= f2cmax(1,N). */
/* > */
/* >          If LWORK = -1, then a workspace query is assumed; the routine */
/* >          only calculates the optimal size of the WORK array, returns */
/* >          this value as the first entry of the WORK array, and no error */
/* >          message related to LWORK is issued by XERBLA. */
/* > \endverbatim */
/* > */
/* > \param[out] RWORK */
/* > \verbatim */
/* >          RWORK is REAL array, dimension (N) */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0: successful exit */
/* >          < 0: if INFO = -i, the i-th argument had an illegal value */
/* >          = 1,...,N: the QZ iteration did not converge.  (H,T) is not */
/* >                     in Schur form, but ALPHA(i) and BETA(i), */
/* >                     i=INFO+1,...,N should be correct. */
/* >          = N+1,...,2*N: the shift calculation failed.  (H,T) is not */
/* >                     in Schur form, but ALPHA(i) and BETA(i), */
/* >                     i=INFO-N+1,...,N should be correct. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date April 2012 */

/* > \ingroup complexGEcomputational */

/* > \par Further Details: */
/*  ===================== */
/* > */
/* > \verbatim */
/* > */
/* >  We assume that complex ABS works as long as its value is less than */
/* >  overflow. */
/* > \endverbatim */
/* > */
/*  ===================================================================== */
/* Subroutine */ void chgeqz_(char *job, char *compq, char *compz, integer *n, 
	integer *ilo, integer *ihi, complex *h__, integer *ldh, complex *t, 
	integer *ldt, complex *alpha, complex *beta, complex *q, integer *ldq,
	 complex *z__, integer *ldz, complex *work, integer *lwork, real *
	rwork, integer *info)
{
    /* System generated locals */
    integer h_dim1, h_offset, q_dim1, q_offset, t_dim1, t_offset, z_dim1, 
	    z_offset, i__1, i__2, i__3, i__4, i__5, i__6;
    real r__1, r__2, r__3, r__4, r__5, r__6;
    complex q__1, q__2, q__3, q__4, q__5, q__6, q__7;

    /* Local variables */
    real absb, atol, btol, temp;
    extern /* Subroutine */ void crot_(integer *, complex *, integer *, 
	    complex *, integer *, real *, complex *);
    real temp2, c__;
    integer j;
    complex s;
    extern /* Subroutine */ void cscal_(integer *, complex *, complex *, 
	    integer *);
    complex x, y;
    extern logical lsame_(char *, char *);
    complex ctemp;
    integer iiter, ilast, jiter;
    real anorm, bnorm;
    integer maxit;
    complex shift;
    real tempr;
    complex ctemp2, ctemp3;
    logical ilazr2;
    integer jc, in;
    real ascale, bscale;
    complex u12;
    integer jr;
    extern /* Complex */ VOID cladiv_(complex *, complex *, complex *);
    complex signbc;
    extern real slamch_(char *), clanhs_(char *, integer *, complex *,
	     integer *, real *);
    extern /* Subroutine */ void claset_(char *, integer *, integer *, complex 
	    *, complex *, complex *, integer *), clartg_(complex *, 
	    complex *, real *, complex *, complex *);
    real safmin;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    complex eshift;
    logical ilschr;
    integer icompq, ilastm, ischur;
    logical ilazro;
    integer icompz, ifirst, ifrstm, istart;
    logical lquery;
    complex ad11, ad12, ad21, ad22;
    integer jch;
    logical ilq, ilz;
    real ulp;
    complex abi12, abi22;


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     April 2012 */


/*  ===================================================================== */


/*     Decode JOB, COMPQ, COMPZ */

    /* Parameter adjustments */
    h_dim1 = *ldh;
    h_offset = 1 + h_dim1 * 1;
    h__ -= h_offset;
    t_dim1 = *ldt;
    t_offset = 1 + t_dim1 * 1;
    t -= t_offset;
    --alpha;
    --beta;
    q_dim1 = *ldq;
    q_offset = 1 + q_dim1 * 1;
    q -= q_offset;
    z_dim1 = *ldz;
    z_offset = 1 + z_dim1 * 1;
    z__ -= z_offset;
    --work;
    --rwork;

    /* Function Body */
    if (lsame_(job, "E")) {
	ilschr = FALSE_;
	ischur = 1;
    } else if (lsame_(job, "S")) {
	ilschr = TRUE_;
	ischur = 2;
    } else {
	ilschr = TRUE_;
	ischur = 0;
    }

    if (lsame_(compq, "N")) {
	ilq = FALSE_;
	icompq = 1;
    } else if (lsame_(compq, "V")) {
	ilq = TRUE_;
	icompq = 2;
    } else if (lsame_(compq, "I")) {
	ilq = TRUE_;
	icompq = 3;
    } else {
	ilq = TRUE_;
	icompq = 0;
    }

    if (lsame_(compz, "N")) {
	ilz = FALSE_;
	icompz = 1;
    } else if (lsame_(compz, "V")) {
	ilz = TRUE_;
	icompz = 2;
    } else if (lsame_(compz, "I")) {
	ilz = TRUE_;
	icompz = 3;
    } else {
	ilz = TRUE_;
	icompz = 0;
    }

/*     Check Argument Values */

    *info = 0;
    i__1 = f2cmax(1,*n);
    work[1].r = (real) i__1, work[1].i = 0.f;
    lquery = *lwork == -1;
    if (ischur == 0) {
	*info = -1;
    } else if (icompq == 0) {
	*info = -2;
    } else if (icompz == 0) {
	*info = -3;
    } else if (*n < 0) {
	*info = -4;
    } else if (*ilo < 1) {
	*info = -5;
    } else if (*ihi > *n || *ihi < *ilo - 1) {
	*info = -6;
    } else if (*ldh < *n) {
	*info = -8;
    } else if (*ldt < *n) {
	*info = -10;
    } else if (*ldq < 1 || ilq && *ldq < *n) {
	*info = -14;
    } else if (*ldz < 1 || ilz && *ldz < *n) {
	*info = -16;
    } else if (*lwork < f2cmax(1,*n) && ! lquery) {
	*info = -18;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("CHGEQZ", &i__1, (ftnlen)6);
	return;
    } else if (lquery) {
	return;
    }

/*     Quick return if possible */

/*     WORK( 1 ) = CMPLX( 1 ) */
    if (*n <= 0) {
	work[1].r = 1.f, work[1].i = 0.f;
	return;
    }

/*     Initialize Q and Z */

    if (icompq == 3) {
	claset_("Full", n, n, &c_b1, &c_b2, &q[q_offset], ldq);
    }
    if (icompz == 3) {
	claset_("Full", n, n, &c_b1, &c_b2, &z__[z_offset], ldz);
    }

/*     Machine Constants */

    in = *ihi + 1 - *ilo;
    safmin = slamch_("S");
    ulp = slamch_("E") * slamch_("B");
    anorm = clanhs_("F", &in, &h__[*ilo + *ilo * h_dim1], ldh, &rwork[1]);
    bnorm = clanhs_("F", &in, &t[*ilo + *ilo * t_dim1], ldt, &rwork[1]);
/* Computing MAX */
    r__1 = safmin, r__2 = ulp * anorm;
    atol = f2cmax(r__1,r__2);
/* Computing MAX */
    r__1 = safmin, r__2 = ulp * bnorm;
    btol = f2cmax(r__1,r__2);
    ascale = 1.f / f2cmax(safmin,anorm);
    bscale = 1.f / f2cmax(safmin,bnorm);


/*     Set Eigenvalues IHI+1:N */

    i__1 = *n;
    for (j = *ihi + 1; j <= i__1; ++j) {
	absb = c_abs(&t[j + j * t_dim1]);
	if (absb > safmin) {
	    i__2 = j + j * t_dim1;
	    q__2.r = t[i__2].r / absb, q__2.i = t[i__2].i / absb;
	    r_cnjg(&q__1, &q__2);
	    signbc.r = q__1.r, signbc.i = q__1.i;
	    i__2 = j + j * t_dim1;
	    t[i__2].r = absb, t[i__2].i = 0.f;
	    if (ilschr) {
		i__2 = j - 1;
		cscal_(&i__2, &signbc, &t[j * t_dim1 + 1], &c__1);
		cscal_(&j, &signbc, &h__[j * h_dim1 + 1], &c__1);
	    } else {
		cscal_(&c__1, &signbc, &h__[j + j * h_dim1], &c__1);
	    }
	    if (ilz) {
		cscal_(n, &signbc, &z__[j * z_dim1 + 1], &c__1);
	    }
	} else {
	    i__2 = j + j * t_dim1;
	    t[i__2].r = 0.f, t[i__2].i = 0.f;
	}
	i__2 = j;
	i__3 = j + j * h_dim1;
	alpha[i__2].r = h__[i__3].r, alpha[i__2].i = h__[i__3].i;
	i__2 = j;
	i__3 = j + j * t_dim1;
	beta[i__2].r = t[i__3].r, beta[i__2].i = t[i__3].i;
/* L10: */
    }

/*     If IHI < ILO, skip QZ steps */

    if (*ihi < *ilo) {
	goto L190;
    }

/*     MAIN QZ ITERATION LOOP */

/*     Initialize dynamic indices */

/*     Eigenvalues ILAST+1:N have been found. */
/*        Column operations modify rows IFRSTM:whatever */
/*        Row operations modify columns whatever:ILASTM */

/*     If only eigenvalues are being computed, then */
/*        IFRSTM is the row of the last splitting row above row ILAST; */
/*        this is always at least ILO. */
/*     IITER counts iterations since the last eigenvalue was found, */
/*        to tell when to use an extraordinary shift. */
/*     MAXIT is the maximum number of QZ sweeps allowed. */

    ilast = *ihi;
    if (ilschr) {
	ifrstm = 1;
	ilastm = *n;
    } else {
	ifrstm = *ilo;
	ilastm = *ihi;
    }
    iiter = 0;
    eshift.r = 0.f, eshift.i = 0.f;
    maxit = (*ihi - *ilo + 1) * 30;

    i__1 = maxit;
    for (jiter = 1; jiter <= i__1; ++jiter) {

/*        Check for too many iterations. */

	if (jiter > maxit) {
	    goto L180;
	}

/*        Split the matrix if possible. */

/*        Two tests: */
/*           1: H(j,j-1)=0  or  j=ILO */
/*           2: T(j,j)=0 */

/*        Special case: j=ILAST */

	if (ilast == *ilo) {
	    goto L60;
	} else {
	    i__2 = ilast + (ilast - 1) * h_dim1;
	    if ((r__1 = h__[i__2].r, abs(r__1)) + (r__2 = r_imag(&h__[ilast + 
		    (ilast - 1) * h_dim1]), abs(r__2)) <= atol) {
		i__2 = ilast + (ilast - 1) * h_dim1;
		h__[i__2].r = 0.f, h__[i__2].i = 0.f;
		goto L60;
	    }
	}

	if (c_abs(&t[ilast + ilast * t_dim1]) <= btol) {
	    i__2 = ilast + ilast * t_dim1;
	    t[i__2].r = 0.f, t[i__2].i = 0.f;
	    goto L50;
	}

/*        General case: j<ILAST */

	i__2 = *ilo;
	for (j = ilast - 1; j >= i__2; --j) {

/*           Test 1: for H(j,j-1)=0 or j=ILO */

	    if (j == *ilo) {
		ilazro = TRUE_;
	    } else {
		i__3 = j + (j - 1) * h_dim1;
		if ((r__1 = h__[i__3].r, abs(r__1)) + (r__2 = r_imag(&h__[j + 
			(j - 1) * h_dim1]), abs(r__2)) <= atol) {
		    i__3 = j + (j - 1) * h_dim1;
		    h__[i__3].r = 0.f, h__[i__3].i = 0.f;
		    ilazro = TRUE_;
		} else {
		    ilazro = FALSE_;
		}
	    }

/*           Test 2: for T(j,j)=0 */

	    if (c_abs(&t[j + j * t_dim1]) < btol) {
		i__3 = j + j * t_dim1;
		t[i__3].r = 0.f, t[i__3].i = 0.f;

/*              Test 1a: Check for 2 consecutive small subdiagonals in A */

		ilazr2 = FALSE_;
		if (! ilazro) {
		    i__3 = j + (j - 1) * h_dim1;
		    i__4 = j + 1 + j * h_dim1;
		    i__5 = j + j * h_dim1;
		    if (((r__1 = h__[i__3].r, abs(r__1)) + (r__2 = r_imag(&
			    h__[j + (j - 1) * h_dim1]), abs(r__2))) * (ascale 
			    * ((r__3 = h__[i__4].r, abs(r__3)) + (r__4 = 
			    r_imag(&h__[j + 1 + j * h_dim1]), abs(r__4)))) <= 
			    ((r__5 = h__[i__5].r, abs(r__5)) + (r__6 = r_imag(
			    &h__[j + j * h_dim1]), abs(r__6))) * (ascale * 
			    atol)) {
			ilazr2 = TRUE_;
		    }
		}

/*              If both tests pass (1 & 2), i.e., the leading diagonal */
/*              element of B in the block is zero, split a 1x1 block off */
/*              at the top. (I.e., at the J-th row/column) The leading */
/*              diagonal element of the remainder can also be zero, so */
/*              this may have to be done repeatedly. */

		if (ilazro || ilazr2) {
		    i__3 = ilast - 1;
		    for (jch = j; jch <= i__3; ++jch) {
			i__4 = jch + jch * h_dim1;
			ctemp.r = h__[i__4].r, ctemp.i = h__[i__4].i;
			clartg_(&ctemp, &h__[jch + 1 + jch * h_dim1], &c__, &
				s, &h__[jch + jch * h_dim1]);
			i__4 = jch + 1 + jch * h_dim1;
			h__[i__4].r = 0.f, h__[i__4].i = 0.f;
			i__4 = ilastm - jch;
			crot_(&i__4, &h__[jch + (jch + 1) * h_dim1], ldh, &
				h__[jch + 1 + (jch + 1) * h_dim1], ldh, &c__, 
				&s);
			i__4 = ilastm - jch;
			crot_(&i__4, &t[jch + (jch + 1) * t_dim1], ldt, &t[
				jch + 1 + (jch + 1) * t_dim1], ldt, &c__, &s);
			if (ilq) {
			    r_cnjg(&q__1, &s);
			    crot_(n, &q[jch * q_dim1 + 1], &c__1, &q[(jch + 1)
				     * q_dim1 + 1], &c__1, &c__, &q__1);
			}
			if (ilazr2) {
			    i__4 = jch + (jch - 1) * h_dim1;
			    i__5 = jch + (jch - 1) * h_dim1;
			    q__1.r = c__ * h__[i__5].r, q__1.i = c__ * h__[
				    i__5].i;
			    h__[i__4].r = q__1.r, h__[i__4].i = q__1.i;
			}
			ilazr2 = FALSE_;
			i__4 = jch + 1 + (jch + 1) * t_dim1;
			if ((r__1 = t[i__4].r, abs(r__1)) + (r__2 = r_imag(&t[
				jch + 1 + (jch + 1) * t_dim1]), abs(r__2)) >= 
				btol) {
			    if (jch + 1 >= ilast) {
				goto L60;
			    } else {
				ifirst = jch + 1;
				goto L70;
			    }
			}
			i__4 = jch + 1 + (jch + 1) * t_dim1;
			t[i__4].r = 0.f, t[i__4].i = 0.f;
/* L20: */
		    }
		    goto L50;
		} else {

/*                 Only test 2 passed -- chase the zero to T(ILAST,ILAST) */
/*                 Then process as in the case T(ILAST,ILAST)=0 */

		    i__3 = ilast - 1;
		    for (jch = j; jch <= i__3; ++jch) {
			i__4 = jch + (jch + 1) * t_dim1;
			ctemp.r = t[i__4].r, ctemp.i = t[i__4].i;
			clartg_(&ctemp, &t[jch + 1 + (jch + 1) * t_dim1], &
				c__, &s, &t[jch + (jch + 1) * t_dim1]);
			i__4 = jch + 1 + (jch + 1) * t_dim1;
			t[i__4].r = 0.f, t[i__4].i = 0.f;
			if (jch < ilastm - 1) {
			    i__4 = ilastm - jch - 1;
			    crot_(&i__4, &t[jch + (jch + 2) * t_dim1], ldt, &
				    t[jch + 1 + (jch + 2) * t_dim1], ldt, &
				    c__, &s);
			}
			i__4 = ilastm - jch + 2;
			crot_(&i__4, &h__[jch + (jch - 1) * h_dim1], ldh, &
				h__[jch + 1 + (jch - 1) * h_dim1], ldh, &c__, 
				&s);
			if (ilq) {
			    r_cnjg(&q__1, &s);
			    crot_(n, &q[jch * q_dim1 + 1], &c__1, &q[(jch + 1)
				     * q_dim1 + 1], &c__1, &c__, &q__1);
			}
			i__4 = jch + 1 + jch * h_dim1;
			ctemp.r = h__[i__4].r, ctemp.i = h__[i__4].i;
			clartg_(&ctemp, &h__[jch + 1 + (jch - 1) * h_dim1], &
				c__, &s, &h__[jch + 1 + jch * h_dim1]);
			i__4 = jch + 1 + (jch - 1) * h_dim1;
			h__[i__4].r = 0.f, h__[i__4].i = 0.f;
			i__4 = jch + 1 - ifrstm;
			crot_(&i__4, &h__[ifrstm + jch * h_dim1], &c__1, &h__[
				ifrstm + (jch - 1) * h_dim1], &c__1, &c__, &s)
				;
			i__4 = jch - ifrstm;
			crot_(&i__4, &t[ifrstm + jch * t_dim1], &c__1, &t[
				ifrstm + (jch - 1) * t_dim1], &c__1, &c__, &s)
				;
			if (ilz) {
			    crot_(n, &z__[jch * z_dim1 + 1], &c__1, &z__[(jch 
				    - 1) * z_dim1 + 1], &c__1, &c__, &s);
			}
/* L30: */
		    }
		    goto L50;
		}
	    } else if (ilazro) {

/*              Only test 1 passed -- work on J:ILAST */

		ifirst = j;
		goto L70;
	    }

/*           Neither test passed -- try next J */

/* L40: */
	}

/*        (Drop-through is "impossible") */

	*info = (*n << 1) + 1;
	goto L210;

/*        T(ILAST,ILAST)=0 -- clear H(ILAST,ILAST-1) to split off a */
/*        1x1 block. */

L50:
	i__2 = ilast + ilast * h_dim1;
	ctemp.r = h__[i__2].r, ctemp.i = h__[i__2].i;
	clartg_(&ctemp, &h__[ilast + (ilast - 1) * h_dim1], &c__, &s, &h__[
		ilast + ilast * h_dim1]);
	i__2 = ilast + (ilast - 1) * h_dim1;
	h__[i__2].r = 0.f, h__[i__2].i = 0.f;
	i__2 = ilast - ifrstm;
	crot_(&i__2, &h__[ifrstm + ilast * h_dim1], &c__1, &h__[ifrstm + (
		ilast - 1) * h_dim1], &c__1, &c__, &s);
	i__2 = ilast - ifrstm;
	crot_(&i__2, &t[ifrstm + ilast * t_dim1], &c__1, &t[ifrstm + (ilast - 
		1) * t_dim1], &c__1, &c__, &s);
	if (ilz) {
	    crot_(n, &z__[ilast * z_dim1 + 1], &c__1, &z__[(ilast - 1) * 
		    z_dim1 + 1], &c__1, &c__, &s);
	}

/*        H(ILAST,ILAST-1)=0 -- Standardize B, set ALPHA and BETA */

L60:
	absb = c_abs(&t[ilast + ilast * t_dim1]);
	if (absb > safmin) {
	    i__2 = ilast + ilast * t_dim1;
	    q__2.r = t[i__2].r / absb, q__2.i = t[i__2].i / absb;
	    r_cnjg(&q__1, &q__2);
	    signbc.r = q__1.r, signbc.i = q__1.i;
	    i__2 = ilast + ilast * t_dim1;
	    t[i__2].r = absb, t[i__2].i = 0.f;
	    if (ilschr) {
		i__2 = ilast - ifrstm;
		cscal_(&i__2, &signbc, &t[ifrstm + ilast * t_dim1], &c__1);
		i__2 = ilast + 1 - ifrstm;
		cscal_(&i__2, &signbc, &h__[ifrstm + ilast * h_dim1], &c__1);
	    } else {
		cscal_(&c__1, &signbc, &h__[ilast + ilast * h_dim1], &c__1);
	    }
	    if (ilz) {
		cscal_(n, &signbc, &z__[ilast * z_dim1 + 1], &c__1);
	    }
	} else {
	    i__2 = ilast + ilast * t_dim1;
	    t[i__2].r = 0.f, t[i__2].i = 0.f;
	}
	i__2 = ilast;
	i__3 = ilast + ilast * h_dim1;
	alpha[i__2].r = h__[i__3].r, alpha[i__2].i = h__[i__3].i;
	i__2 = ilast;
	i__3 = ilast + ilast * t_dim1;
	beta[i__2].r = t[i__3].r, beta[i__2].i = t[i__3].i;

/*        Go to next block -- exit if finished. */

	--ilast;
	if (ilast < *ilo) {
	    goto L190;
	}

/*        Reset counters */

	iiter = 0;
	eshift.r = 0.f, eshift.i = 0.f;
	if (! ilschr) {
	    ilastm = ilast;
	    if (ifrstm > ilast) {
		ifrstm = *ilo;
	    }
	}
	goto L160;

/*        QZ step */

/*        This iteration only involves rows/columns IFIRST:ILAST.  We */
/*        assume IFIRST < ILAST, and that the diagonal of B is non-zero. */

L70:
	++iiter;
	if (! ilschr) {
	    ifrstm = ifirst;
	}

/*        Compute the Shift. */

/*        At this point, IFIRST < ILAST, and the diagonal elements of */
/*        T(IFIRST:ILAST,IFIRST,ILAST) are larger than BTOL (in */
/*        magnitude) */

	if (iiter / 10 * 10 != iiter) {

/*           The Wilkinson shift (AEP p.512), i.e., the eigenvalue of */
/*           the bottom-right 2x2 block of A inv(B) which is nearest to */
/*           the bottom-right element. */

/*           We factor B as U*D, where U has unit diagonals, and */
/*           compute (A*inv(D))*inv(U). */

	    i__2 = ilast - 1 + ilast * t_dim1;
	    q__2.r = bscale * t[i__2].r, q__2.i = bscale * t[i__2].i;
	    i__3 = ilast + ilast * t_dim1;
	    q__3.r = bscale * t[i__3].r, q__3.i = bscale * t[i__3].i;
	    c_div(&q__1, &q__2, &q__3);
	    u12.r = q__1.r, u12.i = q__1.i;
	    i__2 = ilast - 1 + (ilast - 1) * h_dim1;
	    q__2.r = ascale * h__[i__2].r, q__2.i = ascale * h__[i__2].i;
	    i__3 = ilast - 1 + (ilast - 1) * t_dim1;
	    q__3.r = bscale * t[i__3].r, q__3.i = bscale * t[i__3].i;
	    c_div(&q__1, &q__2, &q__3);
	    ad11.r = q__1.r, ad11.i = q__1.i;
	    i__2 = ilast + (ilast - 1) * h_dim1;
	    q__2.r = ascale * h__[i__2].r, q__2.i = ascale * h__[i__2].i;
	    i__3 = ilast - 1 + (ilast - 1) * t_dim1;
	    q__3.r = bscale * t[i__3].r, q__3.i = bscale * t[i__3].i;
	    c_div(&q__1, &q__2, &q__3);
	    ad21.r = q__1.r, ad21.i = q__1.i;
	    i__2 = ilast - 1 + ilast * h_dim1;
	    q__2.r = ascale * h__[i__2].r, q__2.i = ascale * h__[i__2].i;
	    i__3 = ilast + ilast * t_dim1;
	    q__3.r = bscale * t[i__3].r, q__3.i = bscale * t[i__3].i;
	    c_div(&q__1, &q__2, &q__3);
	    ad12.r = q__1.r, ad12.i = q__1.i;
	    i__2 = ilast + ilast * h_dim1;
	    q__2.r = ascale * h__[i__2].r, q__2.i = ascale * h__[i__2].i;
	    i__3 = ilast + ilast * t_dim1;
	    q__3.r = bscale * t[i__3].r, q__3.i = bscale * t[i__3].i;
	    c_div(&q__1, &q__2, &q__3);
	    ad22.r = q__1.r, ad22.i = q__1.i;
	    q__2.r = u12.r * ad21.r - u12.i * ad21.i, q__2.i = u12.r * ad21.i 
		    + u12.i * ad21.r;
	    q__1.r = ad22.r - q__2.r, q__1.i = ad22.i - q__2.i;
	    abi22.r = q__1.r, abi22.i = q__1.i;
	    q__2.r = u12.r * ad11.r - u12.i * ad11.i, q__2.i = u12.r * ad11.i 
		    + u12.i * ad11.r;
	    q__1.r = ad12.r - q__2.r, q__1.i = ad12.i - q__2.i;
	    abi12.r = q__1.r, abi12.i = q__1.i;

	    shift.r = abi22.r, shift.i = abi22.i;
	    c_sqrt(&q__2, &abi12);
	    c_sqrt(&q__3, &ad21);
	    q__1.r = q__2.r * q__3.r - q__2.i * q__3.i, q__1.i = q__2.r * 
		    q__3.i + q__2.i * q__3.r;
	    ctemp.r = q__1.r, ctemp.i = q__1.i;
	    temp = (r__1 = ctemp.r, abs(r__1)) + (r__2 = r_imag(&ctemp), abs(
		    r__2));
	    if (ctemp.r != 0.f || ctemp.i != 0.f) {
		q__2.r = ad11.r - shift.r, q__2.i = ad11.i - shift.i;
		q__1.r = q__2.r * .5f, q__1.i = q__2.i * .5f;
		x.r = q__1.r, x.i = q__1.i;
		temp2 = (r__1 = x.r, abs(r__1)) + (r__2 = r_imag(&x), abs(
			r__2));
/* Computing MAX */
		r__3 = temp, r__4 = (r__1 = x.r, abs(r__1)) + (r__2 = r_imag(&
			x), abs(r__2));
		temp = f2cmax(r__3,r__4);
		q__5.r = x.r / temp, q__5.i = x.i / temp;
		pow_ci(&q__4, &q__5, &c__2);
		q__7.r = ctemp.r / temp, q__7.i = ctemp.i / temp;
		pow_ci(&q__6, &q__7, &c__2);
		q__3.r = q__4.r + q__6.r, q__3.i = q__4.i + q__6.i;
		c_sqrt(&q__2, &q__3);
		q__1.r = temp * q__2.r, q__1.i = temp * q__2.i;
		y.r = q__1.r, y.i = q__1.i;
		if (temp2 > 0.f) {
		    q__1.r = x.r / temp2, q__1.i = x.i / temp2;
		    q__2.r = x.r / temp2, q__2.i = x.i / temp2;
		    if (q__1.r * y.r + r_imag(&q__2) * r_imag(&y) < 0.f) {
			q__3.r = -y.r, q__3.i = -y.i;
			y.r = q__3.r, y.i = q__3.i;
		    }
		}
		q__4.r = x.r + y.r, q__4.i = x.i + y.i;
		cladiv_(&q__3, &ctemp, &q__4);
		q__2.r = ctemp.r * q__3.r - ctemp.i * q__3.i, q__2.i = 
			ctemp.r * q__3.i + ctemp.i * q__3.r;
		q__1.r = shift.r - q__2.r, q__1.i = shift.i - q__2.i;
		shift.r = q__1.r, shift.i = q__1.i;
	    }
	} else {

/*           Exceptional shift.  Chosen for no particularly good reason. */

	    i__2 = ilast + ilast * t_dim1;
	    if (iiter / 20 * 20 == iiter && bscale * ((r__1 = t[i__2].r, abs(
		    r__1)) + (r__2 = r_imag(&t[ilast + ilast * t_dim1]), abs(
		    r__2))) > safmin) {
		i__2 = ilast + ilast * h_dim1;
		q__3.r = ascale * h__[i__2].r, q__3.i = ascale * h__[i__2].i;
		i__3 = ilast + ilast * t_dim1;
		q__4.r = bscale * t[i__3].r, q__4.i = bscale * t[i__3].i;
		c_div(&q__2, &q__3, &q__4);
		q__1.r = eshift.r + q__2.r, q__1.i = eshift.i + q__2.i;
		eshift.r = q__1.r, eshift.i = q__1.i;
	    } else {
		i__2 = ilast + (ilast - 1) * h_dim1;
		q__3.r = ascale * h__[i__2].r, q__3.i = ascale * h__[i__2].i;
		i__3 = ilast - 1 + (ilast - 1) * t_dim1;
		q__4.r = bscale * t[i__3].r, q__4.i = bscale * t[i__3].i;
		c_div(&q__2, &q__3, &q__4);
		q__1.r = eshift.r + q__2.r, q__1.i = eshift.i + q__2.i;
		eshift.r = q__1.r, eshift.i = q__1.i;
	    }
	    shift.r = eshift.r, shift.i = eshift.i;
	}

/*        Now check for two consecutive small subdiagonals. */

	i__2 = ifirst + 1;
	for (j = ilast - 1; j >= i__2; --j) {
	    istart = j;
	    i__3 = j + j * h_dim1;
	    q__2.r = ascale * h__[i__3].r, q__2.i = ascale * h__[i__3].i;
	    i__4 = j + j * t_dim1;
	    q__4.r = bscale * t[i__4].r, q__4.i = bscale * t[i__4].i;
	    q__3.r = shift.r * q__4.r - shift.i * q__4.i, q__3.i = shift.r * 
		    q__4.i + shift.i * q__4.r;
	    q__1.r = q__2.r - q__3.r, q__1.i = q__2.i - q__3.i;
	    ctemp.r = q__1.r, ctemp.i = q__1.i;
	    temp = (r__1 = ctemp.r, abs(r__1)) + (r__2 = r_imag(&ctemp), abs(
		    r__2));
	    i__3 = j + 1 + j * h_dim1;
	    temp2 = ascale * ((r__1 = h__[i__3].r, abs(r__1)) + (r__2 = 
		    r_imag(&h__[j + 1 + j * h_dim1]), abs(r__2)));
	    tempr = f2cmax(temp,temp2);
	    if (tempr < 1.f && tempr != 0.f) {
		temp /= tempr;
		temp2 /= tempr;
	    }
	    i__3 = j + (j - 1) * h_dim1;
	    if (((r__1 = h__[i__3].r, abs(r__1)) + (r__2 = r_imag(&h__[j + (j 
		    - 1) * h_dim1]), abs(r__2))) * temp2 <= temp * atol) {
		goto L90;
	    }
/* L80: */
	}

	istart = ifirst;
	i__2 = ifirst + ifirst * h_dim1;
	q__2.r = ascale * h__[i__2].r, q__2.i = ascale * h__[i__2].i;
	i__3 = ifirst + ifirst * t_dim1;
	q__4.r = bscale * t[i__3].r, q__4.i = bscale * t[i__3].i;
	q__3.r = shift.r * q__4.r - shift.i * q__4.i, q__3.i = shift.r * 
		q__4.i + shift.i * q__4.r;
	q__1.r = q__2.r - q__3.r, q__1.i = q__2.i - q__3.i;
	ctemp.r = q__1.r, ctemp.i = q__1.i;
L90:

/*        Do an implicit-shift QZ sweep. */

/*        Initial Q */

	i__2 = istart + 1 + istart * h_dim1;
	q__1.r = ascale * h__[i__2].r, q__1.i = ascale * h__[i__2].i;
	ctemp2.r = q__1.r, ctemp2.i = q__1.i;
	clartg_(&ctemp, &ctemp2, &c__, &s, &ctemp3);

/*        Sweep */

	i__2 = ilast - 1;
	for (j = istart; j <= i__2; ++j) {
	    if (j > istart) {
		i__3 = j + (j - 1) * h_dim1;
		ctemp.r = h__[i__3].r, ctemp.i = h__[i__3].i;
		clartg_(&ctemp, &h__[j + 1 + (j - 1) * h_dim1], &c__, &s, &
			h__[j + (j - 1) * h_dim1]);
		i__3 = j + 1 + (j - 1) * h_dim1;
		h__[i__3].r = 0.f, h__[i__3].i = 0.f;
	    }

	    i__3 = ilastm;
	    for (jc = j; jc <= i__3; ++jc) {
		i__4 = j + jc * h_dim1;
		q__2.r = c__ * h__[i__4].r, q__2.i = c__ * h__[i__4].i;
		i__5 = j + 1 + jc * h_dim1;
		q__3.r = s.r * h__[i__5].r - s.i * h__[i__5].i, q__3.i = s.r *
			 h__[i__5].i + s.i * h__[i__5].r;
		q__1.r = q__2.r + q__3.r, q__1.i = q__2.i + q__3.i;
		ctemp.r = q__1.r, ctemp.i = q__1.i;
		i__4 = j + 1 + jc * h_dim1;
		r_cnjg(&q__4, &s);
		q__3.r = -q__4.r, q__3.i = -q__4.i;
		i__5 = j + jc * h_dim1;
		q__2.r = q__3.r * h__[i__5].r - q__3.i * h__[i__5].i, q__2.i =
			 q__3.r * h__[i__5].i + q__3.i * h__[i__5].r;
		i__6 = j + 1 + jc * h_dim1;
		q__5.r = c__ * h__[i__6].r, q__5.i = c__ * h__[i__6].i;
		q__1.r = q__2.r + q__5.r, q__1.i = q__2.i + q__5.i;
		h__[i__4].r = q__1.r, h__[i__4].i = q__1.i;
		i__4 = j + jc * h_dim1;
		h__[i__4].r = ctemp.r, h__[i__4].i = ctemp.i;
		i__4 = j + jc * t_dim1;
		q__2.r = c__ * t[i__4].r, q__2.i = c__ * t[i__4].i;
		i__5 = j + 1 + jc * t_dim1;
		q__3.r = s.r * t[i__5].r - s.i * t[i__5].i, q__3.i = s.r * t[
			i__5].i + s.i * t[i__5].r;
		q__1.r = q__2.r + q__3.r, q__1.i = q__2.i + q__3.i;
		ctemp2.r = q__1.r, ctemp2.i = q__1.i;
		i__4 = j + 1 + jc * t_dim1;
		r_cnjg(&q__4, &s);
		q__3.r = -q__4.r, q__3.i = -q__4.i;
		i__5 = j + jc * t_dim1;
		q__2.r = q__3.r * t[i__5].r - q__3.i * t[i__5].i, q__2.i = 
			q__3.r * t[i__5].i + q__3.i * t[i__5].r;
		i__6 = j + 1 + jc * t_dim1;
		q__5.r = c__ * t[i__6].r, q__5.i = c__ * t[i__6].i;
		q__1.r = q__2.r + q__5.r, q__1.i = q__2.i + q__5.i;
		t[i__4].r = q__1.r, t[i__4].i = q__1.i;
		i__4 = j + jc * t_dim1;
		t[i__4].r = ctemp2.r, t[i__4].i = ctemp2.i;
/* L100: */
	    }
	    if (ilq) {
		i__3 = *n;
		for (jr = 1; jr <= i__3; ++jr) {
		    i__4 = jr + j * q_dim1;
		    q__2.r = c__ * q[i__4].r, q__2.i = c__ * q[i__4].i;
		    r_cnjg(&q__4, &s);
		    i__5 = jr + (j + 1) * q_dim1;
		    q__3.r = q__4.r * q[i__5].r - q__4.i * q[i__5].i, q__3.i =
			     q__4.r * q[i__5].i + q__4.i * q[i__5].r;
		    q__1.r = q__2.r + q__3.r, q__1.i = q__2.i + q__3.i;
		    ctemp.r = q__1.r, ctemp.i = q__1.i;
		    i__4 = jr + (j + 1) * q_dim1;
		    q__3.r = -s.r, q__3.i = -s.i;
		    i__5 = jr + j * q_dim1;
		    q__2.r = q__3.r * q[i__5].r - q__3.i * q[i__5].i, q__2.i =
			     q__3.r * q[i__5].i + q__3.i * q[i__5].r;
		    i__6 = jr + (j + 1) * q_dim1;
		    q__4.r = c__ * q[i__6].r, q__4.i = c__ * q[i__6].i;
		    q__1.r = q__2.r + q__4.r, q__1.i = q__2.i + q__4.i;
		    q[i__4].r = q__1.r, q[i__4].i = q__1.i;
		    i__4 = jr + j * q_dim1;
		    q[i__4].r = ctemp.r, q[i__4].i = ctemp.i;
/* L110: */
		}
	    }

	    i__3 = j + 1 + (j + 1) * t_dim1;
	    ctemp.r = t[i__3].r, ctemp.i = t[i__3].i;
	    clartg_(&ctemp, &t[j + 1 + j * t_dim1], &c__, &s, &t[j + 1 + (j + 
		    1) * t_dim1]);
	    i__3 = j + 1 + j * t_dim1;
	    t[i__3].r = 0.f, t[i__3].i = 0.f;

/* Computing MIN */
	    i__4 = j + 2;
	    i__3 = f2cmin(i__4,ilast);
	    for (jr = ifrstm; jr <= i__3; ++jr) {
		i__4 = jr + (j + 1) * h_dim1;
		q__2.r = c__ * h__[i__4].r, q__2.i = c__ * h__[i__4].i;
		i__5 = jr + j * h_dim1;
		q__3.r = s.r * h__[i__5].r - s.i * h__[i__5].i, q__3.i = s.r *
			 h__[i__5].i + s.i * h__[i__5].r;
		q__1.r = q__2.r + q__3.r, q__1.i = q__2.i + q__3.i;
		ctemp.r = q__1.r, ctemp.i = q__1.i;
		i__4 = jr + j * h_dim1;
		r_cnjg(&q__4, &s);
		q__3.r = -q__4.r, q__3.i = -q__4.i;
		i__5 = jr + (j + 1) * h_dim1;
		q__2.r = q__3.r * h__[i__5].r - q__3.i * h__[i__5].i, q__2.i =
			 q__3.r * h__[i__5].i + q__3.i * h__[i__5].r;
		i__6 = jr + j * h_dim1;
		q__5.r = c__ * h__[i__6].r, q__5.i = c__ * h__[i__6].i;
		q__1.r = q__2.r + q__5.r, q__1.i = q__2.i + q__5.i;
		h__[i__4].r = q__1.r, h__[i__4].i = q__1.i;
		i__4 = jr + (j + 1) * h_dim1;
		h__[i__4].r = ctemp.r, h__[i__4].i = ctemp.i;
/* L120: */
	    }
	    i__3 = j;
	    for (jr = ifrstm; jr <= i__3; ++jr) {
		i__4 = jr + (j + 1) * t_dim1;
		q__2.r = c__ * t[i__4].r, q__2.i = c__ * t[i__4].i;
		i__5 = jr + j * t_dim1;
		q__3.r = s.r * t[i__5].r - s.i * t[i__5].i, q__3.i = s.r * t[
			i__5].i + s.i * t[i__5].r;
		q__1.r = q__2.r + q__3.r, q__1.i = q__2.i + q__3.i;
		ctemp.r = q__1.r, ctemp.i = q__1.i;
		i__4 = jr + j * t_dim1;
		r_cnjg(&q__4, &s);
		q__3.r = -q__4.r, q__3.i = -q__4.i;
		i__5 = jr + (j + 1) * t_dim1;
		q__2.r = q__3.r * t[i__5].r - q__3.i * t[i__5].i, q__2.i = 
			q__3.r * t[i__5].i + q__3.i * t[i__5].r;
		i__6 = jr + j * t_dim1;
		q__5.r = c__ * t[i__6].r, q__5.i = c__ * t[i__6].i;
		q__1.r = q__2.r + q__5.r, q__1.i = q__2.i + q__5.i;
		t[i__4].r = q__1.r, t[i__4].i = q__1.i;
		i__4 = jr + (j + 1) * t_dim1;
		t[i__4].r = ctemp.r, t[i__4].i = ctemp.i;
/* L130: */
	    }
	    if (ilz) {
		i__3 = *n;
		for (jr = 1; jr <= i__3; ++jr) {
		    i__4 = jr + (j + 1) * z_dim1;
		    q__2.r = c__ * z__[i__4].r, q__2.i = c__ * z__[i__4].i;
		    i__5 = jr + j * z_dim1;
		    q__3.r = s.r * z__[i__5].r - s.i * z__[i__5].i, q__3.i = 
			    s.r * z__[i__5].i + s.i * z__[i__5].r;
		    q__1.r = q__2.r + q__3.r, q__1.i = q__2.i + q__3.i;
		    ctemp.r = q__1.r, ctemp.i = q__1.i;
		    i__4 = jr + j * z_dim1;
		    r_cnjg(&q__4, &s);
		    q__3.r = -q__4.r, q__3.i = -q__4.i;
		    i__5 = jr + (j + 1) * z_dim1;
		    q__2.r = q__3.r * z__[i__5].r - q__3.i * z__[i__5].i, 
			    q__2.i = q__3.r * z__[i__5].i + q__3.i * z__[i__5]
			    .r;
		    i__6 = jr + j * z_dim1;
		    q__5.r = c__ * z__[i__6].r, q__5.i = c__ * z__[i__6].i;
		    q__1.r = q__2.r + q__5.r, q__1.i = q__2.i + q__5.i;
		    z__[i__4].r = q__1.r, z__[i__4].i = q__1.i;
		    i__4 = jr + (j + 1) * z_dim1;
		    z__[i__4].r = ctemp.r, z__[i__4].i = ctemp.i;
/* L140: */
		}
	    }
/* L150: */
	}

L160:

/* L170: */
	;
    }

/*     Drop-through = non-convergence */

L180:
    *info = ilast;
    goto L210;

/*     Successful completion of all QZ steps */

L190:

/*     Set Eigenvalues 1:ILO-1 */

    i__1 = *ilo - 1;
    for (j = 1; j <= i__1; ++j) {
	absb = c_abs(&t[j + j * t_dim1]);
	if (absb > safmin) {
	    i__2 = j + j * t_dim1;
	    q__2.r = t[i__2].r / absb, q__2.i = t[i__2].i / absb;
	    r_cnjg(&q__1, &q__2);
	    signbc.r = q__1.r, signbc.i = q__1.i;
	    i__2 = j + j * t_dim1;
	    t[i__2].r = absb, t[i__2].i = 0.f;
	    if (ilschr) {
		i__2 = j - 1;
		cscal_(&i__2, &signbc, &t[j * t_dim1 + 1], &c__1);
		cscal_(&j, &signbc, &h__[j * h_dim1 + 1], &c__1);
	    } else {
		cscal_(&c__1, &signbc, &h__[j + j * h_dim1], &c__1);
	    }
	    if (ilz) {
		cscal_(n, &signbc, &z__[j * z_dim1 + 1], &c__1);
	    }
	} else {
	    i__2 = j + j * t_dim1;
	    t[i__2].r = 0.f, t[i__2].i = 0.f;
	}
	i__2 = j;
	i__3 = j + j * h_dim1;
	alpha[i__2].r = h__[i__3].r, alpha[i__2].i = h__[i__3].i;
	i__2 = j;
	i__3 = j + j * t_dim1;
	beta[i__2].r = t[i__3].r, beta[i__2].i = t[i__3].i;
/* L200: */
    }

/*     Normal Termination */

    *info = 0;

/*     Exit (other than argument error) -- return optimal workspace size */

L210:
    q__1.r = (real) (*n), q__1.i = 0.f;
    work[1].r = q__1.r, work[1].i = q__1.i;
    return;

/*     End of CHGEQZ */

} /* chgeqz_ */

