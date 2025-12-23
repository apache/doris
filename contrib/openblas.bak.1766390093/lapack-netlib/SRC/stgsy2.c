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

static integer c__8 = 8;
static integer c__1 = 1;
static real c_b27 = -1.f;
static real c_b42 = 1.f;
static real c_b56 = 0.f;

/* > \brief \b STGSY2 solves the generalized Sylvester equation (unblocked algorithm). */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download STGSY2 + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/stgsy2.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/stgsy2.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/stgsy2.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE STGSY2( TRANS, IJOB, M, N, A, LDA, B, LDB, C, LDC, D, */
/*                          LDD, E, LDE, F, LDF, SCALE, RDSUM, RDSCAL, */
/*                          IWORK, PQ, INFO ) */

/*       CHARACTER          TRANS */
/*       INTEGER            IJOB, INFO, LDA, LDB, LDC, LDD, LDE, LDF, M, N, */
/*      $                   PQ */
/*       REAL               RDSCAL, RDSUM, SCALE */
/*       INTEGER            IWORK( * ) */
/*       REAL               A( LDA, * ), B( LDB, * ), C( LDC, * ), */
/*      $                   D( LDD, * ), E( LDE, * ), F( LDF, * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > STGSY2 solves the generalized Sylvester equation: */
/* > */
/* >             A * R - L * B = scale * C                (1) */
/* >             D * R - L * E = scale * F, */
/* > */
/* > using Level 1 and 2 BLAS. where R and L are unknown M-by-N matrices, */
/* > (A, D), (B, E) and (C, F) are given matrix pairs of size M-by-M, */
/* > N-by-N and M-by-N, respectively, with real entries. (A, D) and (B, E) */
/* > must be in generalized Schur canonical form, i.e. A, B are upper */
/* > quasi triangular and D, E are upper triangular. The solution (R, L) */
/* > overwrites (C, F). 0 <= SCALE <= 1 is an output scaling factor */
/* > chosen to avoid overflow. */
/* > */
/* > In matrix notation solving equation (1) corresponds to solve */
/* > Z*x = scale*b, where Z is defined as */
/* > */
/* >        Z = [ kron(In, A)  -kron(B**T, Im) ]             (2) */
/* >            [ kron(In, D)  -kron(E**T, Im) ], */
/* > */
/* > Ik is the identity matrix of size k and X**T is the transpose of X. */
/* > kron(X, Y) is the Kronecker product between the matrices X and Y. */
/* > In the process of solving (1), we solve a number of such systems */
/* > where Dim(In), Dim(In) = 1 or 2. */
/* > */
/* > If TRANS = 'T', solve the transposed system Z**T*y = scale*b for y, */
/* > which is equivalent to solve for R and L in */
/* > */
/* >             A**T * R  + D**T * L   = scale * C           (3) */
/* >             R  * B**T + L  * E**T  = scale * -F */
/* > */
/* > This case is used to compute an estimate of Dif[(A, D), (B, E)] = */
/* > sigma_min(Z) using reverse communication with SLACON. */
/* > */
/* > STGSY2 also (IJOB >= 1) contributes to the computation in STGSYL */
/* > of an upper bound on the separation between to matrix pairs. Then */
/* > the input (A, D), (B, E) are sub-pencils of the matrix pair in */
/* > STGSYL. See STGSYL for details. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] TRANS */
/* > \verbatim */
/* >          TRANS is CHARACTER*1 */
/* >          = 'N': solve the generalized Sylvester equation (1). */
/* >          = 'T': solve the 'transposed' system (3). */
/* > \endverbatim */
/* > */
/* > \param[in] IJOB */
/* > \verbatim */
/* >          IJOB is INTEGER */
/* >          Specifies what kind of functionality to be performed. */
/* >          = 0: solve (1) only. */
/* >          = 1: A contribution from this subsystem to a Frobenius */
/* >               norm-based estimate of the separation between two matrix */
/* >               pairs is computed. (look ahead strategy is used). */
/* >          = 2: A contribution from this subsystem to a Frobenius */
/* >               norm-based estimate of the separation between two matrix */
/* >               pairs is computed. (SGECON on sub-systems is used.) */
/* >          Not referenced if TRANS = 'T'. */
/* > \endverbatim */
/* > */
/* > \param[in] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >          On entry, M specifies the order of A and D, and the row */
/* >          dimension of C, F, R and L. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          On entry, N specifies the order of B and E, and the column */
/* >          dimension of C, F, R and L. */
/* > \endverbatim */
/* > */
/* > \param[in] A */
/* > \verbatim */
/* >          A is REAL array, dimension (LDA, M) */
/* >          On entry, A contains an upper quasi triangular matrix. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of the matrix A. LDA >= f2cmax(1, M). */
/* > \endverbatim */
/* > */
/* > \param[in] B */
/* > \verbatim */
/* >          B is REAL array, dimension (LDB, N) */
/* >          On entry, B contains an upper quasi triangular matrix. */
/* > \endverbatim */
/* > */
/* > \param[in] LDB */
/* > \verbatim */
/* >          LDB is INTEGER */
/* >          The leading dimension of the matrix B. LDB >= f2cmax(1, N). */
/* > \endverbatim */
/* > */
/* > \param[in,out] C */
/* > \verbatim */
/* >          C is REAL array, dimension (LDC, N) */
/* >          On entry, C contains the right-hand-side of the first matrix */
/* >          equation in (1). */
/* >          On exit, if IJOB = 0, C has been overwritten by the */
/* >          solution R. */
/* > \endverbatim */
/* > */
/* > \param[in] LDC */
/* > \verbatim */
/* >          LDC is INTEGER */
/* >          The leading dimension of the matrix C. LDC >= f2cmax(1, M). */
/* > \endverbatim */
/* > */
/* > \param[in] D */
/* > \verbatim */
/* >          D is REAL array, dimension (LDD, M) */
/* >          On entry, D contains an upper triangular matrix. */
/* > \endverbatim */
/* > */
/* > \param[in] LDD */
/* > \verbatim */
/* >          LDD is INTEGER */
/* >          The leading dimension of the matrix D. LDD >= f2cmax(1, M). */
/* > \endverbatim */
/* > */
/* > \param[in] E */
/* > \verbatim */
/* >          E is REAL array, dimension (LDE, N) */
/* >          On entry, E contains an upper triangular matrix. */
/* > \endverbatim */
/* > */
/* > \param[in] LDE */
/* > \verbatim */
/* >          LDE is INTEGER */
/* >          The leading dimension of the matrix E. LDE >= f2cmax(1, N). */
/* > \endverbatim */
/* > */
/* > \param[in,out] F */
/* > \verbatim */
/* >          F is REAL array, dimension (LDF, N) */
/* >          On entry, F contains the right-hand-side of the second matrix */
/* >          equation in (1). */
/* >          On exit, if IJOB = 0, F has been overwritten by the */
/* >          solution L. */
/* > \endverbatim */
/* > */
/* > \param[in] LDF */
/* > \verbatim */
/* >          LDF is INTEGER */
/* >          The leading dimension of the matrix F. LDF >= f2cmax(1, M). */
/* > \endverbatim */
/* > */
/* > \param[out] SCALE */
/* > \verbatim */
/* >          SCALE is REAL */
/* >          On exit, 0 <= SCALE <= 1. If 0 < SCALE < 1, the solutions */
/* >          R and L (C and F on entry) will hold the solutions to a */
/* >          slightly perturbed system but the input matrices A, B, D and */
/* >          E have not been changed. If SCALE = 0, R and L will hold the */
/* >          solutions to the homogeneous system with C = F = 0. Normally, */
/* >          SCALE = 1. */
/* > \endverbatim */
/* > */
/* > \param[in,out] RDSUM */
/* > \verbatim */
/* >          RDSUM is REAL */
/* >          On entry, the sum of squares of computed contributions to */
/* >          the Dif-estimate under computation by STGSYL, where the */
/* >          scaling factor RDSCAL (see below) has been factored out. */
/* >          On exit, the corresponding sum of squares updated with the */
/* >          contributions from the current sub-system. */
/* >          If TRANS = 'T' RDSUM is not touched. */
/* >          NOTE: RDSUM only makes sense when STGSY2 is called by STGSYL. */
/* > \endverbatim */
/* > */
/* > \param[in,out] RDSCAL */
/* > \verbatim */
/* >          RDSCAL is REAL */
/* >          On entry, scaling factor used to prevent overflow in RDSUM. */
/* >          On exit, RDSCAL is updated w.r.t. the current contributions */
/* >          in RDSUM. */
/* >          If TRANS = 'T', RDSCAL is not touched. */
/* >          NOTE: RDSCAL only makes sense when STGSY2 is called by */
/* >                STGSYL. */
/* > \endverbatim */
/* > */
/* > \param[out] IWORK */
/* > \verbatim */
/* >          IWORK is INTEGER array, dimension (M+N+2) */
/* > \endverbatim */
/* > */
/* > \param[out] PQ */
/* > \verbatim */
/* >          PQ is INTEGER */
/* >          On exit, the number of subsystems (of size 2-by-2, 4-by-4 and */
/* >          8-by-8) solved by this routine. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          On exit, if INFO is set to */
/* >            =0: Successful exit */
/* >            <0: If INFO = -i, the i-th argument had an illegal value. */
/* >            >0: The matrix pairs (A, D) and (B, E) have common or very */
/* >                close eigenvalues. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup realSYauxiliary */

/* > \par Contributors: */
/*  ================== */
/* > */
/* >     Bo Kagstrom and Peter Poromaa, Department of Computing Science, */
/* >     Umea University, S-901 87 Umea, Sweden. */

/*  ===================================================================== */
/* Subroutine */ void stgsy2_(char *trans, integer *ijob, integer *m, integer *
	n, real *a, integer *lda, real *b, integer *ldb, real *c__, integer *
	ldc, real *d__, integer *ldd, real *e, integer *lde, real *f, integer 
	*ldf, real *scale, real *rdsum, real *rdscal, integer *iwork, integer 
	*pq, integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, c_dim1, c_offset, d_dim1, 
	    d_offset, e_dim1, e_offset, f_dim1, f_offset, i__1, i__2, i__3;

    /* Local variables */
    extern /* Subroutine */ void sger_(integer *, integer *, real *, real *, 
	    integer *, real *, integer *, real *, integer *);
    integer ierr, zdim, ipiv[8], jpiv[8], i__, j, k, p, q;
    real alpha, z__[64]	/* was [8][8] */;
    extern logical lsame_(char *, char *);
    extern /* Subroutine */ void sscal_(integer *, real *, real *, integer *), 
	    sgemm_(char *, char *, integer *, integer *, integer *, real *, 
	    real *, integer *, real *, integer *, real *, real *, integer *), sgemv_(char *, integer *, integer *, real *, 
	    real *, integer *, real *, integer *, real *, real *, integer *), scopy_(integer *, real *, integer *, real *, integer *), 
	    saxpy_(integer *, real *, real *, integer *, real *, integer *), 
	    sgesc2_(integer *, real *, integer *, real *, integer *, integer *
	    , real *), sgetc2_(integer *, real *, integer *, integer *, 
	    integer *, integer *);
    integer ie, je, mb, nb, ii, jj, is, js;
    real scaloc;
    extern /* Subroutine */ void slatdf_(integer *, integer *, real *, integer 
	    *, real *, real *, real *, integer *, integer *);
    extern int xerbla_(char *, integer *, ftnlen);
    extern void slaset_(char *, integer *, integer *, real *, 
	    real *, real *, integer *);
    logical notran;
    real rhs[8];
    integer isp1, jsp1;


/*  -- LAPACK auxiliary routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */
/*  Replaced various illegal calls to SCOPY by calls to SLASET. */
/*  Sven Hammarling, 27/5/02. */


/*     Decode and test input parameters */

    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    b_dim1 = *ldb;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    c_dim1 = *ldc;
    c_offset = 1 + c_dim1 * 1;
    c__ -= c_offset;
    d_dim1 = *ldd;
    d_offset = 1 + d_dim1 * 1;
    d__ -= d_offset;
    e_dim1 = *lde;
    e_offset = 1 + e_dim1 * 1;
    e -= e_offset;
    f_dim1 = *ldf;
    f_offset = 1 + f_dim1 * 1;
    f -= f_offset;
    --iwork;

    /* Function Body */
    *info = 0;
    ierr = 0;
    notran = lsame_(trans, "N");
    if (! notran && ! lsame_(trans, "T")) {
	*info = -1;
    } else if (notran) {
	if (*ijob < 0 || *ijob > 2) {
	    *info = -2;
	}
    }
    if (*info == 0) {
	if (*m <= 0) {
	    *info = -3;
	} else if (*n <= 0) {
	    *info = -4;
	} else if (*lda < f2cmax(1,*m)) {
	    *info = -6;
	} else if (*ldb < f2cmax(1,*n)) {
	    *info = -8;
	} else if (*ldc < f2cmax(1,*m)) {
	    *info = -10;
	} else if (*ldd < f2cmax(1,*m)) {
	    *info = -12;
	} else if (*lde < f2cmax(1,*n)) {
	    *info = -14;
	} else if (*ldf < f2cmax(1,*m)) {
	    *info = -16;
	}
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("STGSY2", &i__1, (ftnlen)6);
	return;
    }

/*     Determine block structure of A */

    *pq = 0;
    p = 0;
    i__ = 1;
L10:
    if (i__ > *m) {
	goto L20;
    }
    ++p;
    iwork[p] = i__;
    if (i__ == *m) {
	goto L20;
    }
    if (a[i__ + 1 + i__ * a_dim1] != 0.f) {
	i__ += 2;
    } else {
	++i__;
    }
    goto L10;
L20:
    iwork[p + 1] = *m + 1;

/*     Determine block structure of B */

    q = p + 1;
    j = 1;
L30:
    if (j > *n) {
	goto L40;
    }
    ++q;
    iwork[q] = j;
    if (j == *n) {
	goto L40;
    }
    if (b[j + 1 + j * b_dim1] != 0.f) {
	j += 2;
    } else {
	++j;
    }
    goto L30;
L40:
    iwork[q + 1] = *n + 1;
    *pq = p * (q - p - 1);

    if (notran) {

/*        Solve (I, J) - subsystem */
/*           A(I, I) * R(I, J) - L(I, J) * B(J, J) = C(I, J) */
/*           D(I, I) * R(I, J) - L(I, J) * E(J, J) = F(I, J) */
/*        for I = P, P - 1, ..., 1; J = 1, 2, ..., Q */

	*scale = 1.f;
	scaloc = 1.f;
	i__1 = q;
	for (j = p + 2; j <= i__1; ++j) {
	    js = iwork[j];
	    jsp1 = js + 1;
	    je = iwork[j + 1] - 1;
	    nb = je - js + 1;
	    for (i__ = p; i__ >= 1; --i__) {

		is = iwork[i__];
		isp1 = is + 1;
		ie = iwork[i__ + 1] - 1;
		mb = ie - is + 1;
		zdim = mb * nb << 1;

		if (mb == 1 && nb == 1) {

/*                 Build a 2-by-2 system Z * x = RHS */

		    z__[0] = a[is + is * a_dim1];
		    z__[1] = d__[is + is * d_dim1];
		    z__[8] = -b[js + js * b_dim1];
		    z__[9] = -e[js + js * e_dim1];

/*                 Set up right hand side(s) */

		    rhs[0] = c__[is + js * c_dim1];
		    rhs[1] = f[is + js * f_dim1];

/*                 Solve Z * x = RHS */

		    sgetc2_(&zdim, z__, &c__8, ipiv, jpiv, &ierr);
		    if (ierr > 0) {
			*info = ierr;
		    }

		    if (*ijob == 0) {
			sgesc2_(&zdim, z__, &c__8, rhs, ipiv, jpiv, &scaloc);
			if (scaloc != 1.f) {
			    i__2 = *n;
			    for (k = 1; k <= i__2; ++k) {
				sscal_(m, &scaloc, &c__[k * c_dim1 + 1], &
					c__1);
				sscal_(m, &scaloc, &f[k * f_dim1 + 1], &c__1);
/* L50: */
			    }
			    *scale *= scaloc;
			}
		    } else {
			slatdf_(ijob, &zdim, z__, &c__8, rhs, rdsum, rdscal, 
				ipiv, jpiv);
		    }

/*                 Unpack solution vector(s) */

		    c__[is + js * c_dim1] = rhs[0];
		    f[is + js * f_dim1] = rhs[1];

/*                 Substitute R(I, J) and L(I, J) into remaining */
/*                 equation. */

		    if (i__ > 1) {
			alpha = -rhs[0];
			i__2 = is - 1;
			saxpy_(&i__2, &alpha, &a[is * a_dim1 + 1], &c__1, &
				c__[js * c_dim1 + 1], &c__1);
			i__2 = is - 1;
			saxpy_(&i__2, &alpha, &d__[is * d_dim1 + 1], &c__1, &
				f[js * f_dim1 + 1], &c__1);
		    }
		    if (j < q) {
			i__2 = *n - je;
			saxpy_(&i__2, &rhs[1], &b[js + (je + 1) * b_dim1], 
				ldb, &c__[is + (je + 1) * c_dim1], ldc);
			i__2 = *n - je;
			saxpy_(&i__2, &rhs[1], &e[js + (je + 1) * e_dim1], 
				lde, &f[is + (je + 1) * f_dim1], ldf);
		    }

		} else if (mb == 1 && nb == 2) {

/*                 Build a 4-by-4 system Z * x = RHS */

		    z__[0] = a[is + is * a_dim1];
		    z__[1] = 0.f;
		    z__[2] = d__[is + is * d_dim1];
		    z__[3] = 0.f;

		    z__[8] = 0.f;
		    z__[9] = a[is + is * a_dim1];
		    z__[10] = 0.f;
		    z__[11] = d__[is + is * d_dim1];

		    z__[16] = -b[js + js * b_dim1];
		    z__[17] = -b[js + jsp1 * b_dim1];
		    z__[18] = -e[js + js * e_dim1];
		    z__[19] = -e[js + jsp1 * e_dim1];

		    z__[24] = -b[jsp1 + js * b_dim1];
		    z__[25] = -b[jsp1 + jsp1 * b_dim1];
		    z__[26] = 0.f;
		    z__[27] = -e[jsp1 + jsp1 * e_dim1];

/*                 Set up right hand side(s) */

		    rhs[0] = c__[is + js * c_dim1];
		    rhs[1] = c__[is + jsp1 * c_dim1];
		    rhs[2] = f[is + js * f_dim1];
		    rhs[3] = f[is + jsp1 * f_dim1];

/*                 Solve Z * x = RHS */

		    sgetc2_(&zdim, z__, &c__8, ipiv, jpiv, &ierr);
		    if (ierr > 0) {
			*info = ierr;
		    }

		    if (*ijob == 0) {
			sgesc2_(&zdim, z__, &c__8, rhs, ipiv, jpiv, &scaloc);
			if (scaloc != 1.f) {
			    i__2 = *n;
			    for (k = 1; k <= i__2; ++k) {
				sscal_(m, &scaloc, &c__[k * c_dim1 + 1], &
					c__1);
				sscal_(m, &scaloc, &f[k * f_dim1 + 1], &c__1);
/* L60: */
			    }
			    *scale *= scaloc;
			}
		    } else {
			slatdf_(ijob, &zdim, z__, &c__8, rhs, rdsum, rdscal, 
				ipiv, jpiv);
		    }

/*                 Unpack solution vector(s) */

		    c__[is + js * c_dim1] = rhs[0];
		    c__[is + jsp1 * c_dim1] = rhs[1];
		    f[is + js * f_dim1] = rhs[2];
		    f[is + jsp1 * f_dim1] = rhs[3];

/*                 Substitute R(I, J) and L(I, J) into remaining */
/*                 equation. */

		    if (i__ > 1) {
			i__2 = is - 1;
			sger_(&i__2, &nb, &c_b27, &a[is * a_dim1 + 1], &c__1, 
				rhs, &c__1, &c__[js * c_dim1 + 1], ldc);
			i__2 = is - 1;
			sger_(&i__2, &nb, &c_b27, &d__[is * d_dim1 + 1], &
				c__1, rhs, &c__1, &f[js * f_dim1 + 1], ldf);
		    }
		    if (j < q) {
			i__2 = *n - je;
			saxpy_(&i__2, &rhs[2], &b[js + (je + 1) * b_dim1], 
				ldb, &c__[is + (je + 1) * c_dim1], ldc);
			i__2 = *n - je;
			saxpy_(&i__2, &rhs[2], &e[js + (je + 1) * e_dim1], 
				lde, &f[is + (je + 1) * f_dim1], ldf);
			i__2 = *n - je;
			saxpy_(&i__2, &rhs[3], &b[jsp1 + (je + 1) * b_dim1], 
				ldb, &c__[is + (je + 1) * c_dim1], ldc);
			i__2 = *n - je;
			saxpy_(&i__2, &rhs[3], &e[jsp1 + (je + 1) * e_dim1], 
				lde, &f[is + (je + 1) * f_dim1], ldf);
		    }

		} else if (mb == 2 && nb == 1) {

/*                 Build a 4-by-4 system Z * x = RHS */

		    z__[0] = a[is + is * a_dim1];
		    z__[1] = a[isp1 + is * a_dim1];
		    z__[2] = d__[is + is * d_dim1];
		    z__[3] = 0.f;

		    z__[8] = a[is + isp1 * a_dim1];
		    z__[9] = a[isp1 + isp1 * a_dim1];
		    z__[10] = d__[is + isp1 * d_dim1];
		    z__[11] = d__[isp1 + isp1 * d_dim1];

		    z__[16] = -b[js + js * b_dim1];
		    z__[17] = 0.f;
		    z__[18] = -e[js + js * e_dim1];
		    z__[19] = 0.f;

		    z__[24] = 0.f;
		    z__[25] = -b[js + js * b_dim1];
		    z__[26] = 0.f;
		    z__[27] = -e[js + js * e_dim1];

/*                 Set up right hand side(s) */

		    rhs[0] = c__[is + js * c_dim1];
		    rhs[1] = c__[isp1 + js * c_dim1];
		    rhs[2] = f[is + js * f_dim1];
		    rhs[3] = f[isp1 + js * f_dim1];

/*                 Solve Z * x = RHS */

		    sgetc2_(&zdim, z__, &c__8, ipiv, jpiv, &ierr);
		    if (ierr > 0) {
			*info = ierr;
		    }
		    if (*ijob == 0) {
			sgesc2_(&zdim, z__, &c__8, rhs, ipiv, jpiv, &scaloc);
			if (scaloc != 1.f) {
			    i__2 = *n;
			    for (k = 1; k <= i__2; ++k) {
				sscal_(m, &scaloc, &c__[k * c_dim1 + 1], &
					c__1);
				sscal_(m, &scaloc, &f[k * f_dim1 + 1], &c__1);
/* L70: */
			    }
			    *scale *= scaloc;
			}
		    } else {
			slatdf_(ijob, &zdim, z__, &c__8, rhs, rdsum, rdscal, 
				ipiv, jpiv);
		    }

/*                 Unpack solution vector(s) */

		    c__[is + js * c_dim1] = rhs[0];
		    c__[isp1 + js * c_dim1] = rhs[1];
		    f[is + js * f_dim1] = rhs[2];
		    f[isp1 + js * f_dim1] = rhs[3];

/*                 Substitute R(I, J) and L(I, J) into remaining */
/*                 equation. */

		    if (i__ > 1) {
			i__2 = is - 1;
			sgemv_("N", &i__2, &mb, &c_b27, &a[is * a_dim1 + 1], 
				lda, rhs, &c__1, &c_b42, &c__[js * c_dim1 + 1]
				, &c__1);
			i__2 = is - 1;
			sgemv_("N", &i__2, &mb, &c_b27, &d__[is * d_dim1 + 1],
				 ldd, rhs, &c__1, &c_b42, &f[js * f_dim1 + 1],
				 &c__1);
		    }
		    if (j < q) {
			i__2 = *n - je;
			sger_(&mb, &i__2, &c_b42, &rhs[2], &c__1, &b[js + (je 
				+ 1) * b_dim1], ldb, &c__[is + (je + 1) * 
				c_dim1], ldc);
			i__2 = *n - je;
			sger_(&mb, &i__2, &c_b42, &rhs[2], &c__1, &e[js + (je 
				+ 1) * e_dim1], lde, &f[is + (je + 1) * 
				f_dim1], ldf);
		    }

		} else if (mb == 2 && nb == 2) {

/*                 Build an 8-by-8 system Z * x = RHS */

		    slaset_("F", &c__8, &c__8, &c_b56, &c_b56, z__, &c__8);

		    z__[0] = a[is + is * a_dim1];
		    z__[1] = a[isp1 + is * a_dim1];
		    z__[4] = d__[is + is * d_dim1];

		    z__[8] = a[is + isp1 * a_dim1];
		    z__[9] = a[isp1 + isp1 * a_dim1];
		    z__[12] = d__[is + isp1 * d_dim1];
		    z__[13] = d__[isp1 + isp1 * d_dim1];

		    z__[18] = a[is + is * a_dim1];
		    z__[19] = a[isp1 + is * a_dim1];
		    z__[22] = d__[is + is * d_dim1];

		    z__[26] = a[is + isp1 * a_dim1];
		    z__[27] = a[isp1 + isp1 * a_dim1];
		    z__[30] = d__[is + isp1 * d_dim1];
		    z__[31] = d__[isp1 + isp1 * d_dim1];

		    z__[32] = -b[js + js * b_dim1];
		    z__[34] = -b[js + jsp1 * b_dim1];
		    z__[36] = -e[js + js * e_dim1];
		    z__[38] = -e[js + jsp1 * e_dim1];

		    z__[41] = -b[js + js * b_dim1];
		    z__[43] = -b[js + jsp1 * b_dim1];
		    z__[45] = -e[js + js * e_dim1];
		    z__[47] = -e[js + jsp1 * e_dim1];

		    z__[48] = -b[jsp1 + js * b_dim1];
		    z__[50] = -b[jsp1 + jsp1 * b_dim1];
		    z__[54] = -e[jsp1 + jsp1 * e_dim1];

		    z__[57] = -b[jsp1 + js * b_dim1];
		    z__[59] = -b[jsp1 + jsp1 * b_dim1];
		    z__[63] = -e[jsp1 + jsp1 * e_dim1];

/*                 Set up right hand side(s) */

		    k = 1;
		    ii = mb * nb + 1;
		    i__2 = nb - 1;
		    for (jj = 0; jj <= i__2; ++jj) {
			scopy_(&mb, &c__[is + (js + jj) * c_dim1], &c__1, &
				rhs[k - 1], &c__1);
			scopy_(&mb, &f[is + (js + jj) * f_dim1], &c__1, &rhs[
				ii - 1], &c__1);
			k += mb;
			ii += mb;
/* L80: */
		    }

/*                 Solve Z * x = RHS */

		    sgetc2_(&zdim, z__, &c__8, ipiv, jpiv, &ierr);
		    if (ierr > 0) {
			*info = ierr;
		    }
		    if (*ijob == 0) {
			sgesc2_(&zdim, z__, &c__8, rhs, ipiv, jpiv, &scaloc);
			if (scaloc != 1.f) {
			    i__2 = *n;
			    for (k = 1; k <= i__2; ++k) {
				sscal_(m, &scaloc, &c__[k * c_dim1 + 1], &
					c__1);
				sscal_(m, &scaloc, &f[k * f_dim1 + 1], &c__1);
/* L90: */
			    }
			    *scale *= scaloc;
			}
		    } else {
			slatdf_(ijob, &zdim, z__, &c__8, rhs, rdsum, rdscal, 
				ipiv, jpiv);
		    }

/*                 Unpack solution vector(s) */

		    k = 1;
		    ii = mb * nb + 1;
		    i__2 = nb - 1;
		    for (jj = 0; jj <= i__2; ++jj) {
			scopy_(&mb, &rhs[k - 1], &c__1, &c__[is + (js + jj) * 
				c_dim1], &c__1);
			scopy_(&mb, &rhs[ii - 1], &c__1, &f[is + (js + jj) * 
				f_dim1], &c__1);
			k += mb;
			ii += mb;
/* L100: */
		    }

/*                 Substitute R(I, J) and L(I, J) into remaining */
/*                 equation. */

		    if (i__ > 1) {
			i__2 = is - 1;
			sgemm_("N", "N", &i__2, &nb, &mb, &c_b27, &a[is * 
				a_dim1 + 1], lda, rhs, &mb, &c_b42, &c__[js * 
				c_dim1 + 1], ldc);
			i__2 = is - 1;
			sgemm_("N", "N", &i__2, &nb, &mb, &c_b27, &d__[is * 
				d_dim1 + 1], ldd, rhs, &mb, &c_b42, &f[js * 
				f_dim1 + 1], ldf);
		    }
		    if (j < q) {
			k = mb * nb + 1;
			i__2 = *n - je;
			sgemm_("N", "N", &mb, &i__2, &nb, &c_b42, &rhs[k - 1],
				 &mb, &b[js + (je + 1) * b_dim1], ldb, &c_b42,
				 &c__[is + (je + 1) * c_dim1], ldc);
			i__2 = *n - je;
			sgemm_("N", "N", &mb, &i__2, &nb, &c_b42, &rhs[k - 1],
				 &mb, &e[js + (je + 1) * e_dim1], lde, &c_b42,
				 &f[is + (je + 1) * f_dim1], ldf);
		    }

		}

/* L110: */
	    }
/* L120: */
	}
    } else {

/*        Solve (I, J) - subsystem */
/*             A(I, I)**T * R(I, J) + D(I, I)**T * L(J, J)  =  C(I, J) */
/*             R(I, I)  * B(J, J) + L(I, J)  * E(J, J)  = -F(I, J) */
/*        for I = 1, 2, ..., P, J = Q, Q - 1, ..., 1 */

	*scale = 1.f;
	scaloc = 1.f;
	i__1 = p;
	for (i__ = 1; i__ <= i__1; ++i__) {

	    is = iwork[i__];
	    isp1 = is + 1;
	    ie = iwork[i__ + 1] - 1;
	    mb = ie - is + 1;
	    i__2 = p + 2;
	    for (j = q; j >= i__2; --j) {

		js = iwork[j];
		jsp1 = js + 1;
		je = iwork[j + 1] - 1;
		nb = je - js + 1;
		zdim = mb * nb << 1;
		if (mb == 1 && nb == 1) {

/*                 Build a 2-by-2 system Z**T * x = RHS */

		    z__[0] = a[is + is * a_dim1];
		    z__[1] = -b[js + js * b_dim1];
		    z__[8] = d__[is + is * d_dim1];
		    z__[9] = -e[js + js * e_dim1];

/*                 Set up right hand side(s) */

		    rhs[0] = c__[is + js * c_dim1];
		    rhs[1] = f[is + js * f_dim1];

/*                 Solve Z**T * x = RHS */

		    sgetc2_(&zdim, z__, &c__8, ipiv, jpiv, &ierr);
		    if (ierr > 0) {
			*info = ierr;
		    }

		    sgesc2_(&zdim, z__, &c__8, rhs, ipiv, jpiv, &scaloc);
		    if (scaloc != 1.f) {
			i__3 = *n;
			for (k = 1; k <= i__3; ++k) {
			    sscal_(m, &scaloc, &c__[k * c_dim1 + 1], &c__1);
			    sscal_(m, &scaloc, &f[k * f_dim1 + 1], &c__1);
/* L130: */
			}
			*scale *= scaloc;
		    }

/*                 Unpack solution vector(s) */

		    c__[is + js * c_dim1] = rhs[0];
		    f[is + js * f_dim1] = rhs[1];

/*                 Substitute R(I, J) and L(I, J) into remaining */
/*                 equation. */

		    if (j > p + 2) {
			alpha = rhs[0];
			i__3 = js - 1;
			saxpy_(&i__3, &alpha, &b[js * b_dim1 + 1], &c__1, &f[
				is + f_dim1], ldf);
			alpha = rhs[1];
			i__3 = js - 1;
			saxpy_(&i__3, &alpha, &e[js * e_dim1 + 1], &c__1, &f[
				is + f_dim1], ldf);
		    }
		    if (i__ < p) {
			alpha = -rhs[0];
			i__3 = *m - ie;
			saxpy_(&i__3, &alpha, &a[is + (ie + 1) * a_dim1], lda,
				 &c__[ie + 1 + js * c_dim1], &c__1);
			alpha = -rhs[1];
			i__3 = *m - ie;
			saxpy_(&i__3, &alpha, &d__[is + (ie + 1) * d_dim1], 
				ldd, &c__[ie + 1 + js * c_dim1], &c__1);
		    }

		} else if (mb == 1 && nb == 2) {

/*                 Build a 4-by-4 system Z**T * x = RHS */

		    z__[0] = a[is + is * a_dim1];
		    z__[1] = 0.f;
		    z__[2] = -b[js + js * b_dim1];
		    z__[3] = -b[jsp1 + js * b_dim1];

		    z__[8] = 0.f;
		    z__[9] = a[is + is * a_dim1];
		    z__[10] = -b[js + jsp1 * b_dim1];
		    z__[11] = -b[jsp1 + jsp1 * b_dim1];

		    z__[16] = d__[is + is * d_dim1];
		    z__[17] = 0.f;
		    z__[18] = -e[js + js * e_dim1];
		    z__[19] = 0.f;

		    z__[24] = 0.f;
		    z__[25] = d__[is + is * d_dim1];
		    z__[26] = -e[js + jsp1 * e_dim1];
		    z__[27] = -e[jsp1 + jsp1 * e_dim1];

/*                 Set up right hand side(s) */

		    rhs[0] = c__[is + js * c_dim1];
		    rhs[1] = c__[is + jsp1 * c_dim1];
		    rhs[2] = f[is + js * f_dim1];
		    rhs[3] = f[is + jsp1 * f_dim1];

/*                 Solve Z**T * x = RHS */

		    sgetc2_(&zdim, z__, &c__8, ipiv, jpiv, &ierr);
		    if (ierr > 0) {
			*info = ierr;
		    }
		    sgesc2_(&zdim, z__, &c__8, rhs, ipiv, jpiv, &scaloc);
		    if (scaloc != 1.f) {
			i__3 = *n;
			for (k = 1; k <= i__3; ++k) {
			    sscal_(m, &scaloc, &c__[k * c_dim1 + 1], &c__1);
			    sscal_(m, &scaloc, &f[k * f_dim1 + 1], &c__1);
/* L140: */
			}
			*scale *= scaloc;
		    }

/*                 Unpack solution vector(s) */

		    c__[is + js * c_dim1] = rhs[0];
		    c__[is + jsp1 * c_dim1] = rhs[1];
		    f[is + js * f_dim1] = rhs[2];
		    f[is + jsp1 * f_dim1] = rhs[3];

/*                 Substitute R(I, J) and L(I, J) into remaining */
/*                 equation. */

		    if (j > p + 2) {
			i__3 = js - 1;
			saxpy_(&i__3, rhs, &b[js * b_dim1 + 1], &c__1, &f[is 
				+ f_dim1], ldf);
			i__3 = js - 1;
			saxpy_(&i__3, &rhs[1], &b[jsp1 * b_dim1 + 1], &c__1, &
				f[is + f_dim1], ldf);
			i__3 = js - 1;
			saxpy_(&i__3, &rhs[2], &e[js * e_dim1 + 1], &c__1, &f[
				is + f_dim1], ldf);
			i__3 = js - 1;
			saxpy_(&i__3, &rhs[3], &e[jsp1 * e_dim1 + 1], &c__1, &
				f[is + f_dim1], ldf);
		    }
		    if (i__ < p) {
			i__3 = *m - ie;
			sger_(&i__3, &nb, &c_b27, &a[is + (ie + 1) * a_dim1], 
				lda, rhs, &c__1, &c__[ie + 1 + js * c_dim1], 
				ldc);
			i__3 = *m - ie;
			sger_(&i__3, &nb, &c_b27, &d__[is + (ie + 1) * d_dim1]
				, ldd, &rhs[2], &c__1, &c__[ie + 1 + js * 
				c_dim1], ldc);
		    }

		} else if (mb == 2 && nb == 1) {

/*                 Build a 4-by-4 system Z**T * x = RHS */

		    z__[0] = a[is + is * a_dim1];
		    z__[1] = a[is + isp1 * a_dim1];
		    z__[2] = -b[js + js * b_dim1];
		    z__[3] = 0.f;

		    z__[8] = a[isp1 + is * a_dim1];
		    z__[9] = a[isp1 + isp1 * a_dim1];
		    z__[10] = 0.f;
		    z__[11] = -b[js + js * b_dim1];

		    z__[16] = d__[is + is * d_dim1];
		    z__[17] = d__[is + isp1 * d_dim1];
		    z__[18] = -e[js + js * e_dim1];
		    z__[19] = 0.f;

		    z__[24] = 0.f;
		    z__[25] = d__[isp1 + isp1 * d_dim1];
		    z__[26] = 0.f;
		    z__[27] = -e[js + js * e_dim1];

/*                 Set up right hand side(s) */

		    rhs[0] = c__[is + js * c_dim1];
		    rhs[1] = c__[isp1 + js * c_dim1];
		    rhs[2] = f[is + js * f_dim1];
		    rhs[3] = f[isp1 + js * f_dim1];

/*                 Solve Z**T * x = RHS */

		    sgetc2_(&zdim, z__, &c__8, ipiv, jpiv, &ierr);
		    if (ierr > 0) {
			*info = ierr;
		    }

		    sgesc2_(&zdim, z__, &c__8, rhs, ipiv, jpiv, &scaloc);
		    if (scaloc != 1.f) {
			i__3 = *n;
			for (k = 1; k <= i__3; ++k) {
			    sscal_(m, &scaloc, &c__[k * c_dim1 + 1], &c__1);
			    sscal_(m, &scaloc, &f[k * f_dim1 + 1], &c__1);
/* L150: */
			}
			*scale *= scaloc;
		    }

/*                 Unpack solution vector(s) */

		    c__[is + js * c_dim1] = rhs[0];
		    c__[isp1 + js * c_dim1] = rhs[1];
		    f[is + js * f_dim1] = rhs[2];
		    f[isp1 + js * f_dim1] = rhs[3];

/*                 Substitute R(I, J) and L(I, J) into remaining */
/*                 equation. */

		    if (j > p + 2) {
			i__3 = js - 1;
			sger_(&mb, &i__3, &c_b42, rhs, &c__1, &b[js * b_dim1 
				+ 1], &c__1, &f[is + f_dim1], ldf);
			i__3 = js - 1;
			sger_(&mb, &i__3, &c_b42, &rhs[2], &c__1, &e[js * 
				e_dim1 + 1], &c__1, &f[is + f_dim1], ldf);
		    }
		    if (i__ < p) {
			i__3 = *m - ie;
			sgemv_("T", &mb, &i__3, &c_b27, &a[is + (ie + 1) * 
				a_dim1], lda, rhs, &c__1, &c_b42, &c__[ie + 1 
				+ js * c_dim1], &c__1);
			i__3 = *m - ie;
			sgemv_("T", &mb, &i__3, &c_b27, &d__[is + (ie + 1) * 
				d_dim1], ldd, &rhs[2], &c__1, &c_b42, &c__[ie 
				+ 1 + js * c_dim1], &c__1);
		    }

		} else if (mb == 2 && nb == 2) {

/*                 Build an 8-by-8 system Z**T * x = RHS */

		    slaset_("F", &c__8, &c__8, &c_b56, &c_b56, z__, &c__8);

		    z__[0] = a[is + is * a_dim1];
		    z__[1] = a[is + isp1 * a_dim1];
		    z__[4] = -b[js + js * b_dim1];
		    z__[6] = -b[jsp1 + js * b_dim1];

		    z__[8] = a[isp1 + is * a_dim1];
		    z__[9] = a[isp1 + isp1 * a_dim1];
		    z__[13] = -b[js + js * b_dim1];
		    z__[15] = -b[jsp1 + js * b_dim1];

		    z__[18] = a[is + is * a_dim1];
		    z__[19] = a[is + isp1 * a_dim1];
		    z__[20] = -b[js + jsp1 * b_dim1];
		    z__[22] = -b[jsp1 + jsp1 * b_dim1];

		    z__[26] = a[isp1 + is * a_dim1];
		    z__[27] = a[isp1 + isp1 * a_dim1];
		    z__[29] = -b[js + jsp1 * b_dim1];
		    z__[31] = -b[jsp1 + jsp1 * b_dim1];

		    z__[32] = d__[is + is * d_dim1];
		    z__[33] = d__[is + isp1 * d_dim1];
		    z__[36] = -e[js + js * e_dim1];

		    z__[41] = d__[isp1 + isp1 * d_dim1];
		    z__[45] = -e[js + js * e_dim1];

		    z__[50] = d__[is + is * d_dim1];
		    z__[51] = d__[is + isp1 * d_dim1];
		    z__[52] = -e[js + jsp1 * e_dim1];
		    z__[54] = -e[jsp1 + jsp1 * e_dim1];

		    z__[59] = d__[isp1 + isp1 * d_dim1];
		    z__[61] = -e[js + jsp1 * e_dim1];
		    z__[63] = -e[jsp1 + jsp1 * e_dim1];

/*                 Set up right hand side(s) */

		    k = 1;
		    ii = mb * nb + 1;
		    i__3 = nb - 1;
		    for (jj = 0; jj <= i__3; ++jj) {
			scopy_(&mb, &c__[is + (js + jj) * c_dim1], &c__1, &
				rhs[k - 1], &c__1);
			scopy_(&mb, &f[is + (js + jj) * f_dim1], &c__1, &rhs[
				ii - 1], &c__1);
			k += mb;
			ii += mb;
/* L160: */
		    }


/*                 Solve Z**T * x = RHS */

		    sgetc2_(&zdim, z__, &c__8, ipiv, jpiv, &ierr);
		    if (ierr > 0) {
			*info = ierr;
		    }

		    sgesc2_(&zdim, z__, &c__8, rhs, ipiv, jpiv, &scaloc);
		    if (scaloc != 1.f) {
			i__3 = *n;
			for (k = 1; k <= i__3; ++k) {
			    sscal_(m, &scaloc, &c__[k * c_dim1 + 1], &c__1);
			    sscal_(m, &scaloc, &f[k * f_dim1 + 1], &c__1);
/* L170: */
			}
			*scale *= scaloc;
		    }

/*                 Unpack solution vector(s) */

		    k = 1;
		    ii = mb * nb + 1;
		    i__3 = nb - 1;
		    for (jj = 0; jj <= i__3; ++jj) {
			scopy_(&mb, &rhs[k - 1], &c__1, &c__[is + (js + jj) * 
				c_dim1], &c__1);
			scopy_(&mb, &rhs[ii - 1], &c__1, &f[is + (js + jj) * 
				f_dim1], &c__1);
			k += mb;
			ii += mb;
/* L180: */
		    }

/*                 Substitute R(I, J) and L(I, J) into remaining */
/*                 equation. */

		    if (j > p + 2) {
			i__3 = js - 1;
			sgemm_("N", "T", &mb, &i__3, &nb, &c_b42, &c__[is + 
				js * c_dim1], ldc, &b[js * b_dim1 + 1], ldb, &
				c_b42, &f[is + f_dim1], ldf);
			i__3 = js - 1;
			sgemm_("N", "T", &mb, &i__3, &nb, &c_b42, &f[is + js *
				 f_dim1], ldf, &e[js * e_dim1 + 1], lde, &
				c_b42, &f[is + f_dim1], ldf);
		    }
		    if (i__ < p) {
			i__3 = *m - ie;
			sgemm_("T", "N", &i__3, &nb, &mb, &c_b27, &a[is + (ie 
				+ 1) * a_dim1], lda, &c__[is + js * c_dim1], 
				ldc, &c_b42, &c__[ie + 1 + js * c_dim1], ldc);
			i__3 = *m - ie;
			sgemm_("T", "N", &i__3, &nb, &mb, &c_b27, &d__[is + (
				ie + 1) * d_dim1], ldd, &f[is + js * f_dim1], 
				ldf, &c_b42, &c__[ie + 1 + js * c_dim1], ldc);
		    }

		}

/* L190: */
	    }
/* L200: */
	}

    }
    return;

/*     End of STGSY2 */

} /* stgsy2_ */

