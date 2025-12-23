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

static integer c__2 = 2;
static integer c_n1 = -1;
static integer c__5 = 5;
static doublereal c_b14 = 0.;
static integer c__1 = 1;
static doublereal c_b51 = -1.;
static doublereal c_b52 = 1.;

/* > \brief \b DTGSYL */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download DTGSYL + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dtgsyl.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dtgsyl.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dtgsyl.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE DTGSYL( TRANS, IJOB, M, N, A, LDA, B, LDB, C, LDC, D, */
/*                          LDD, E, LDE, F, LDF, SCALE, DIF, WORK, LWORK, */
/*                          IWORK, INFO ) */

/*       CHARACTER          TRANS */
/*       INTEGER            IJOB, INFO, LDA, LDB, LDC, LDD, LDE, LDF, */
/*      $                   LWORK, M, N */
/*       DOUBLE PRECISION   DIF, SCALE */
/*       INTEGER            IWORK( * ) */
/*       DOUBLE PRECISION   A( LDA, * ), B( LDB, * ), C( LDC, * ), */
/*      $                   D( LDD, * ), E( LDE, * ), F( LDF, * ), */
/*      $                   WORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > DTGSYL solves the generalized Sylvester equation: */
/* > */
/* >             A * R - L * B = scale * C                 (1) */
/* >             D * R - L * E = scale * F */
/* > */
/* > where R and L are unknown m-by-n matrices, (A, D), (B, E) and */
/* > (C, F) are given matrix pairs of size m-by-m, n-by-n and m-by-n, */
/* > respectively, with real entries. (A, D) and (B, E) must be in */
/* > generalized (real) Schur canonical form, i.e. A, B are upper quasi */
/* > triangular and D, E are upper triangular. */
/* > */
/* > The solution (R, L) overwrites (C, F). 0 <= SCALE <= 1 is an output */
/* > scaling factor chosen to avoid overflow. */
/* > */
/* > In matrix notation (1) is equivalent to solve  Zx = scale b, where */
/* > Z is defined as */
/* > */
/* >            Z = [ kron(In, A)  -kron(B**T, Im) ]         (2) */
/* >                [ kron(In, D)  -kron(E**T, Im) ]. */
/* > */
/* > Here Ik is the identity matrix of size k and X**T is the transpose of */
/* > X. kron(X, Y) is the Kronecker product between the matrices X and Y. */
/* > */
/* > If TRANS = 'T', DTGSYL solves the transposed system Z**T*y = scale*b, */
/* > which is equivalent to solve for R and L in */
/* > */
/* >             A**T * R + D**T * L = scale * C           (3) */
/* >             R * B**T + L * E**T = scale * -F */
/* > */
/* > This case (TRANS = 'T') is used to compute an one-norm-based estimate */
/* > of Dif[(A,D), (B,E)], the separation between the matrix pairs (A,D) */
/* > and (B,E), using DLACON. */
/* > */
/* > If IJOB >= 1, DTGSYL computes a Frobenius norm-based estimate */
/* > of Dif[(A,D),(B,E)]. That is, the reciprocal of a lower bound on the */
/* > reciprocal of the smallest singular value of Z. See [1-2] for more */
/* > information. */
/* > */
/* > This is a level 3 BLAS algorithm. */
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
/* >          = 1: The functionality of 0 and 3. */
/* >          = 2: The functionality of 0 and 4. */
/* >          = 3: Only an estimate of Dif[(A,D), (B,E)] is computed. */
/* >               (look ahead strategy IJOB  = 1 is used). */
/* >          = 4: Only an estimate of Dif[(A,D), (B,E)] is computed. */
/* >               ( DGECON on sub-systems is used ). */
/* >          Not referenced if TRANS = 'T'. */
/* > \endverbatim */
/* > */
/* > \param[in] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >          The order of the matrices A and D, and the row dimension of */
/* >          the matrices C, F, R and L. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrices B and E, and the column dimension */
/* >          of the matrices C, F, R and L. */
/* > \endverbatim */
/* > */
/* > \param[in] A */
/* > \verbatim */
/* >          A is DOUBLE PRECISION array, dimension (LDA, M) */
/* >          The upper quasi triangular matrix A. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of the array A. LDA >= f2cmax(1, M). */
/* > \endverbatim */
/* > */
/* > \param[in] B */
/* > \verbatim */
/* >          B is DOUBLE PRECISION array, dimension (LDB, N) */
/* >          The upper quasi triangular matrix B. */
/* > \endverbatim */
/* > */
/* > \param[in] LDB */
/* > \verbatim */
/* >          LDB is INTEGER */
/* >          The leading dimension of the array B. LDB >= f2cmax(1, N). */
/* > \endverbatim */
/* > */
/* > \param[in,out] C */
/* > \verbatim */
/* >          C is DOUBLE PRECISION array, dimension (LDC, N) */
/* >          On entry, C contains the right-hand-side of the first matrix */
/* >          equation in (1) or (3). */
/* >          On exit, if IJOB = 0, 1 or 2, C has been overwritten by */
/* >          the solution R. If IJOB = 3 or 4 and TRANS = 'N', C holds R, */
/* >          the solution achieved during the computation of the */
/* >          Dif-estimate. */
/* > \endverbatim */
/* > */
/* > \param[in] LDC */
/* > \verbatim */
/* >          LDC is INTEGER */
/* >          The leading dimension of the array C. LDC >= f2cmax(1, M). */
/* > \endverbatim */
/* > */
/* > \param[in] D */
/* > \verbatim */
/* >          D is DOUBLE PRECISION array, dimension (LDD, M) */
/* >          The upper triangular matrix D. */
/* > \endverbatim */
/* > */
/* > \param[in] LDD */
/* > \verbatim */
/* >          LDD is INTEGER */
/* >          The leading dimension of the array D. LDD >= f2cmax(1, M). */
/* > \endverbatim */
/* > */
/* > \param[in] E */
/* > \verbatim */
/* >          E is DOUBLE PRECISION array, dimension (LDE, N) */
/* >          The upper triangular matrix E. */
/* > \endverbatim */
/* > */
/* > \param[in] LDE */
/* > \verbatim */
/* >          LDE is INTEGER */
/* >          The leading dimension of the array E. LDE >= f2cmax(1, N). */
/* > \endverbatim */
/* > */
/* > \param[in,out] F */
/* > \verbatim */
/* >          F is DOUBLE PRECISION array, dimension (LDF, N) */
/* >          On entry, F contains the right-hand-side of the second matrix */
/* >          equation in (1) or (3). */
/* >          On exit, if IJOB = 0, 1 or 2, F has been overwritten by */
/* >          the solution L. If IJOB = 3 or 4 and TRANS = 'N', F holds L, */
/* >          the solution achieved during the computation of the */
/* >          Dif-estimate. */
/* > \endverbatim */
/* > */
/* > \param[in] LDF */
/* > \verbatim */
/* >          LDF is INTEGER */
/* >          The leading dimension of the array F. LDF >= f2cmax(1, M). */
/* > \endverbatim */
/* > */
/* > \param[out] DIF */
/* > \verbatim */
/* >          DIF is DOUBLE PRECISION */
/* >          On exit DIF is the reciprocal of a lower bound of the */
/* >          reciprocal of the Dif-function, i.e. DIF is an upper bound of */
/* >          Dif[(A,D), (B,E)] = sigma_min(Z), where Z as in (2). */
/* >          IF IJOB = 0 or TRANS = 'T', DIF is not touched. */
/* > \endverbatim */
/* > */
/* > \param[out] SCALE */
/* > \verbatim */
/* >          SCALE is DOUBLE PRECISION */
/* >          On exit SCALE is the scaling factor in (1) or (3). */
/* >          If 0 < SCALE < 1, C and F hold the solutions R and L, resp., */
/* >          to a slightly perturbed system but the input matrices A, B, D */
/* >          and E have not been changed. If SCALE = 0, C and F hold the */
/* >          solutions R and L, respectively, to the homogeneous system */
/* >          with C = F = 0. Normally, SCALE = 1. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is DOUBLE PRECISION array, dimension (MAX(1,LWORK)) */
/* >          On exit, if INFO = 0, WORK(1) returns the optimal LWORK. */
/* > \endverbatim */
/* > */
/* > \param[in] LWORK */
/* > \verbatim */
/* >          LWORK is INTEGER */
/* >          The dimension of the array WORK. LWORK > = 1. */
/* >          If IJOB = 1 or 2 and TRANS = 'N', LWORK >= f2cmax(1,2*M*N). */
/* > */
/* >          If LWORK = -1, then a workspace query is assumed; the routine */
/* >          only calculates the optimal size of the WORK array, returns */
/* >          this value as the first entry of the WORK array, and no error */
/* >          message related to LWORK is issued by XERBLA. */
/* > \endverbatim */
/* > */
/* > \param[out] IWORK */
/* > \verbatim */
/* >          IWORK is INTEGER array, dimension (M+N+6) */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >            =0: successful exit */
/* >            <0: If INFO = -i, the i-th argument had an illegal value. */
/* >            >0: (A, D) and (B, E) have common or close eigenvalues. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup doubleSYcomputational */

/* > \par Contributors: */
/*  ================== */
/* > */
/* >     Bo Kagstrom and Peter Poromaa, Department of Computing Science, */
/* >     Umea University, S-901 87 Umea, Sweden. */

/* > \par References: */
/*  ================ */
/* > */
/* > \verbatim */
/* > */
/* >  [1] B. Kagstrom and P. Poromaa, LAPACK-Style Algorithms and Software */
/* >      for Solving the Generalized Sylvester Equation and Estimating the */
/* >      Separation between Regular Matrix Pairs, Report UMINF - 93.23, */
/* >      Department of Computing Science, Umea University, S-901 87 Umea, */
/* >      Sweden, December 1993, Revised April 1994, Also as LAPACK Working */
/* >      Note 75.  To appear in ACM Trans. on Math. Software, Vol 22, */
/* >      No 1, 1996. */
/* > */
/* >  [2] B. Kagstrom, A Perturbation Analysis of the Generalized Sylvester */
/* >      Equation (AR - LB, DR - LE ) = (C, F), SIAM J. Matrix Anal. */
/* >      Appl., 15(4):1045-1060, 1994 */
/* > */
/* >  [3] B. Kagstrom and L. Westin, Generalized Schur Methods with */
/* >      Condition Estimators for Solving the Generalized Sylvester */
/* >      Equation, IEEE Transactions on Automatic Control, Vol. 34, No. 7, */
/* >      July 1989, pp 745-751. */
/* > \endverbatim */
/* > */
/*  ===================================================================== */
/* Subroutine */ void dtgsyl_(char *trans, integer *ijob, integer *m, integer *
	n, doublereal *a, integer *lda, doublereal *b, integer *ldb, 
	doublereal *c__, integer *ldc, doublereal *d__, integer *ldd, 
	doublereal *e, integer *lde, doublereal *f, integer *ldf, doublereal *
	scale, doublereal *dif, doublereal *work, integer *lwork, integer *
	iwork, integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, c_dim1, c_offset, d_dim1, 
	    d_offset, e_dim1, e_offset, f_dim1, f_offset, i__1, i__2, i__3, 
	    i__4;

    /* Local variables */
    doublereal dsum;
    integer ppqq, i__, j, k, p, q;
    extern /* Subroutine */ void dscal_(integer *, doublereal *, doublereal *, 
	    integer *), dgemm_(char *, char *, integer *, integer *, integer *
	    , doublereal *, doublereal *, integer *, doublereal *, integer *, 
	    doublereal *, doublereal *, integer *);
    extern logical lsame_(char *, char *);
    integer ifunc, linfo, lwmin;
    doublereal scale2;
    extern /* Subroutine */ void dtgsy2_(char *, integer *, integer *, integer 
	    *, doublereal *, integer *, doublereal *, integer *, doublereal *,
	     integer *, doublereal *, integer *, doublereal *, integer *, 
	    doublereal *, integer *, doublereal *, doublereal *, doublereal *,
	     integer *, integer *, integer *);
    integer ie, je, mb, nb;
    doublereal dscale;
    integer is, js, pq;
    doublereal scaloc;
    extern /* Subroutine */ void dlacpy_(char *, integer *, integer *, 
	    doublereal *, integer *, doublereal *, integer *), 
	    dlaset_(char *, integer *, integer *, doublereal *, doublereal *, 
	    doublereal *, integer *);
    extern integer ilaenv_(integer *, char *, char *, integer *, integer *, 
	    integer *, integer *, ftnlen, ftnlen);
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    integer iround;
    logical notran;
    integer isolve;
    logical lquery;


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */
/*  Replaced various illegal calls to DCOPY by calls to DLASET. */
/*  Sven Hammarling, 1/5/02. */


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
    --work;
    --iwork;

    /* Function Body */
    *info = 0;
    notran = lsame_(trans, "N");
    lquery = *lwork == -1;

    if (! notran && ! lsame_(trans, "T")) {
	*info = -1;
    } else if (notran) {
	if (*ijob < 0 || *ijob > 4) {
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

    if (*info == 0) {
	if (notran) {
	    if (*ijob == 1 || *ijob == 2) {
/* Computing MAX */
		i__1 = 1, i__2 = (*m << 1) * *n;
		lwmin = f2cmax(i__1,i__2);
	    } else {
		lwmin = 1;
	    }
	} else {
	    lwmin = 1;
	}
	work[1] = (doublereal) lwmin;

	if (*lwork < lwmin && ! lquery) {
	    *info = -20;
	}
    }

    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("DTGSYL", &i__1, (ftnlen)6);
	return;
    } else if (lquery) {
	return;
    }

/*     Quick return if possible */

    if (*m == 0 || *n == 0) {
	*scale = 1.;
	if (notran) {
	    if (*ijob != 0) {
		*dif = 0.;
	    }
	}
	return;
    }

/*     Determine optimal block sizes MB and NB */

    mb = ilaenv_(&c__2, "DTGSYL", trans, m, n, &c_n1, &c_n1, (ftnlen)6, (
	    ftnlen)1);
    nb = ilaenv_(&c__5, "DTGSYL", trans, m, n, &c_n1, &c_n1, (ftnlen)6, (
	    ftnlen)1);

    isolve = 1;
    ifunc = 0;
    if (notran) {
	if (*ijob >= 3) {
	    ifunc = *ijob - 2;
	    dlaset_("F", m, n, &c_b14, &c_b14, &c__[c_offset], ldc)
		    ;
	    dlaset_("F", m, n, &c_b14, &c_b14, &f[f_offset], ldf);
	} else if (*ijob >= 1) {
	    isolve = 2;
	}
    }

    if (mb <= 1 && nb <= 1 || mb >= *m && nb >= *n) {

	i__1 = isolve;
	for (iround = 1; iround <= i__1; ++iround) {

/*           Use unblocked Level 2 solver */

	    dscale = 0.;
	    dsum = 1.;
	    pq = 0;
	    dtgsy2_(trans, &ifunc, m, n, &a[a_offset], lda, &b[b_offset], ldb,
		     &c__[c_offset], ldc, &d__[d_offset], ldd, &e[e_offset], 
		    lde, &f[f_offset], ldf, scale, &dsum, &dscale, &iwork[1], 
		    &pq, info);
	    if (dscale != 0.) {
		if (*ijob == 1 || *ijob == 3) {
		    *dif = sqrt((doublereal) ((*m << 1) * *n)) / (dscale * 
			    sqrt(dsum));
		} else {
		    *dif = sqrt((doublereal) pq) / (dscale * sqrt(dsum));
		}
	    }

	    if (isolve == 2 && iround == 1) {
		if (notran) {
		    ifunc = *ijob;
		}
		scale2 = *scale;
		dlacpy_("F", m, n, &c__[c_offset], ldc, &work[1], m);
		dlacpy_("F", m, n, &f[f_offset], ldf, &work[*m * *n + 1], m);
		dlaset_("F", m, n, &c_b14, &c_b14, &c__[c_offset], ldc);
		dlaset_("F", m, n, &c_b14, &c_b14, &f[f_offset], ldf);
	    } else if (isolve == 2 && iround == 2) {
		dlacpy_("F", m, n, &work[1], m, &c__[c_offset], ldc);
		dlacpy_("F", m, n, &work[*m * *n + 1], m, &f[f_offset], ldf);
		*scale = scale2;
	    }
/* L30: */
	}

	return;
    }

/*     Determine block structure of A */

    p = 0;
    i__ = 1;
L40:
    if (i__ > *m) {
	goto L50;
    }
    ++p;
    iwork[p] = i__;
    i__ += mb;
    if (i__ >= *m) {
	goto L50;
    }
    if (a[i__ + (i__ - 1) * a_dim1] != 0.) {
	++i__;
    }
    goto L40;
L50:

    iwork[p + 1] = *m + 1;
    if (iwork[p] == iwork[p + 1]) {
	--p;
    }

/*     Determine block structure of B */

    q = p + 1;
    j = 1;
L60:
    if (j > *n) {
	goto L70;
    }
    ++q;
    iwork[q] = j;
    j += nb;
    if (j >= *n) {
	goto L70;
    }
    if (b[j + (j - 1) * b_dim1] != 0.) {
	++j;
    }
    goto L60;
L70:

    iwork[q + 1] = *n + 1;
    if (iwork[q] == iwork[q + 1]) {
	--q;
    }

    if (notran) {

	i__1 = isolve;
	for (iround = 1; iround <= i__1; ++iround) {

/*           Solve (I, J)-subsystem */
/*               A(I, I) * R(I, J) - L(I, J) * B(J, J) = C(I, J) */
/*               D(I, I) * R(I, J) - L(I, J) * E(J, J) = F(I, J) */
/*           for I = P, P - 1,..., 1; J = 1, 2,..., Q */

	    dscale = 0.;
	    dsum = 1.;
	    pq = 0;
	    *scale = 1.;
	    i__2 = q;
	    for (j = p + 2; j <= i__2; ++j) {
		js = iwork[j];
		je = iwork[j + 1] - 1;
		nb = je - js + 1;
		for (i__ = p; i__ >= 1; --i__) {
		    is = iwork[i__];
		    ie = iwork[i__ + 1] - 1;
		    mb = ie - is + 1;
		    ppqq = 0;
		    dtgsy2_(trans, &ifunc, &mb, &nb, &a[is + is * a_dim1], 
			    lda, &b[js + js * b_dim1], ldb, &c__[is + js * 
			    c_dim1], ldc, &d__[is + is * d_dim1], ldd, &e[js 
			    + js * e_dim1], lde, &f[is + js * f_dim1], ldf, &
			    scaloc, &dsum, &dscale, &iwork[q + 2], &ppqq, &
			    linfo);
		    if (linfo > 0) {
			*info = linfo;
		    }

		    pq += ppqq;
		    if (scaloc != 1.) {
			i__3 = js - 1;
			for (k = 1; k <= i__3; ++k) {
			    dscal_(m, &scaloc, &c__[k * c_dim1 + 1], &c__1);
			    dscal_(m, &scaloc, &f[k * f_dim1 + 1], &c__1);
/* L80: */
			}
			i__3 = je;
			for (k = js; k <= i__3; ++k) {
			    i__4 = is - 1;
			    dscal_(&i__4, &scaloc, &c__[k * c_dim1 + 1], &
				    c__1);
			    i__4 = is - 1;
			    dscal_(&i__4, &scaloc, &f[k * f_dim1 + 1], &c__1);
/* L90: */
			}
			i__3 = je;
			for (k = js; k <= i__3; ++k) {
			    i__4 = *m - ie;
			    dscal_(&i__4, &scaloc, &c__[ie + 1 + k * c_dim1], 
				    &c__1);
			    i__4 = *m - ie;
			    dscal_(&i__4, &scaloc, &f[ie + 1 + k * f_dim1], &
				    c__1);
/* L100: */
			}
			i__3 = *n;
			for (k = je + 1; k <= i__3; ++k) {
			    dscal_(m, &scaloc, &c__[k * c_dim1 + 1], &c__1);
			    dscal_(m, &scaloc, &f[k * f_dim1 + 1], &c__1);
/* L110: */
			}
			*scale *= scaloc;
		    }

/*                 Substitute R(I, J) and L(I, J) into remaining */
/*                 equation. */

		    if (i__ > 1) {
			i__3 = is - 1;
			dgemm_("N", "N", &i__3, &nb, &mb, &c_b51, &a[is * 
				a_dim1 + 1], lda, &c__[is + js * c_dim1], ldc,
				 &c_b52, &c__[js * c_dim1 + 1], ldc);
			i__3 = is - 1;
			dgemm_("N", "N", &i__3, &nb, &mb, &c_b51, &d__[is * 
				d_dim1 + 1], ldd, &c__[is + js * c_dim1], ldc,
				 &c_b52, &f[js * f_dim1 + 1], ldf);
		    }
		    if (j < q) {
			i__3 = *n - je;
			dgemm_("N", "N", &mb, &i__3, &nb, &c_b52, &f[is + js *
				 f_dim1], ldf, &b[js + (je + 1) * b_dim1], 
				ldb, &c_b52, &c__[is + (je + 1) * c_dim1], 
				ldc);
			i__3 = *n - je;
			dgemm_("N", "N", &mb, &i__3, &nb, &c_b52, &f[is + js *
				 f_dim1], ldf, &e[js + (je + 1) * e_dim1], 
				lde, &c_b52, &f[is + (je + 1) * f_dim1], ldf);
		    }
/* L120: */
		}
/* L130: */
	    }
	    if (dscale != 0.) {
		if (*ijob == 1 || *ijob == 3) {
		    *dif = sqrt((doublereal) ((*m << 1) * *n)) / (dscale * 
			    sqrt(dsum));
		} else {
		    *dif = sqrt((doublereal) pq) / (dscale * sqrt(dsum));
		}
	    }
	    if (isolve == 2 && iround == 1) {
		if (notran) {
		    ifunc = *ijob;
		}
		scale2 = *scale;
		dlacpy_("F", m, n, &c__[c_offset], ldc, &work[1], m);
		dlacpy_("F", m, n, &f[f_offset], ldf, &work[*m * *n + 1], m);
		dlaset_("F", m, n, &c_b14, &c_b14, &c__[c_offset], ldc);
		dlaset_("F", m, n, &c_b14, &c_b14, &f[f_offset], ldf);
	    } else if (isolve == 2 && iround == 2) {
		dlacpy_("F", m, n, &work[1], m, &c__[c_offset], ldc);
		dlacpy_("F", m, n, &work[*m * *n + 1], m, &f[f_offset], ldf);
		*scale = scale2;
	    }
/* L150: */
	}

    } else {

/*        Solve transposed (I, J)-subsystem */
/*             A(I, I)**T * R(I, J)  + D(I, I)**T * L(I, J)  =  C(I, J) */
/*             R(I, J)  * B(J, J)**T + L(I, J)  * E(J, J)**T = -F(I, J) */
/*        for I = 1,2,..., P; J = Q, Q-1,..., 1 */

	*scale = 1.;
	i__1 = p;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    is = iwork[i__];
	    ie = iwork[i__ + 1] - 1;
	    mb = ie - is + 1;
	    i__2 = p + 2;
	    for (j = q; j >= i__2; --j) {
		js = iwork[j];
		je = iwork[j + 1] - 1;
		nb = je - js + 1;
		dtgsy2_(trans, &ifunc, &mb, &nb, &a[is + is * a_dim1], lda, &
			b[js + js * b_dim1], ldb, &c__[is + js * c_dim1], ldc,
			 &d__[is + is * d_dim1], ldd, &e[js + js * e_dim1], 
			lde, &f[is + js * f_dim1], ldf, &scaloc, &dsum, &
			dscale, &iwork[q + 2], &ppqq, &linfo);
		if (linfo > 0) {
		    *info = linfo;
		}
		if (scaloc != 1.) {
		    i__3 = js - 1;
		    for (k = 1; k <= i__3; ++k) {
			dscal_(m, &scaloc, &c__[k * c_dim1 + 1], &c__1);
			dscal_(m, &scaloc, &f[k * f_dim1 + 1], &c__1);
/* L160: */
		    }
		    i__3 = je;
		    for (k = js; k <= i__3; ++k) {
			i__4 = is - 1;
			dscal_(&i__4, &scaloc, &c__[k * c_dim1 + 1], &c__1);
			i__4 = is - 1;
			dscal_(&i__4, &scaloc, &f[k * f_dim1 + 1], &c__1);
/* L170: */
		    }
		    i__3 = je;
		    for (k = js; k <= i__3; ++k) {
			i__4 = *m - ie;
			dscal_(&i__4, &scaloc, &c__[ie + 1 + k * c_dim1], &
				c__1);
			i__4 = *m - ie;
			dscal_(&i__4, &scaloc, &f[ie + 1 + k * f_dim1], &c__1)
				;
/* L180: */
		    }
		    i__3 = *n;
		    for (k = je + 1; k <= i__3; ++k) {
			dscal_(m, &scaloc, &c__[k * c_dim1 + 1], &c__1);
			dscal_(m, &scaloc, &f[k * f_dim1 + 1], &c__1);
/* L190: */
		    }
		    *scale *= scaloc;
		}

/*              Substitute R(I, J) and L(I, J) into remaining equation. */

		if (j > p + 2) {
		    i__3 = js - 1;
		    dgemm_("N", "T", &mb, &i__3, &nb, &c_b52, &c__[is + js * 
			    c_dim1], ldc, &b[js * b_dim1 + 1], ldb, &c_b52, &
			    f[is + f_dim1], ldf);
		    i__3 = js - 1;
		    dgemm_("N", "T", &mb, &i__3, &nb, &c_b52, &f[is + js * 
			    f_dim1], ldf, &e[js * e_dim1 + 1], lde, &c_b52, &
			    f[is + f_dim1], ldf);
		}
		if (i__ < p) {
		    i__3 = *m - ie;
		    dgemm_("T", "N", &i__3, &nb, &mb, &c_b51, &a[is + (ie + 1)
			     * a_dim1], lda, &c__[is + js * c_dim1], ldc, &
			    c_b52, &c__[ie + 1 + js * c_dim1], ldc);
		    i__3 = *m - ie;
		    dgemm_("T", "N", &i__3, &nb, &mb, &c_b51, &d__[is + (ie + 
			    1) * d_dim1], ldd, &f[is + js * f_dim1], ldf, &
			    c_b52, &c__[ie + 1 + js * c_dim1], ldc);
		}
/* L200: */
	    }
/* L210: */
	}

    }

    work[1] = (doublereal) lwmin;

    return;

/*     End of DTGSYL */

} /* dtgsyl_ */

