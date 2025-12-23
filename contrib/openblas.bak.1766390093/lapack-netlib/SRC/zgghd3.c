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

static doublecomplex c_b1 = {1.,0.};
static doublecomplex c_b2 = {0.,0.};
static integer c__1 = 1;
static integer c_n1 = -1;
static integer c__2 = 2;
static integer c__3 = 3;
static integer c__16 = 16;

/* > \brief \b ZGGHD3 */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download ZGGHD3 + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zgghd3.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zgghd3.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zgghd3.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE ZGGHD3( COMPQ, COMPZ, N, ILO, IHI, A, LDA, B, LDB, Q, */
/*                          LDQ, Z, LDZ, WORK, LWORK, INFO ) */

/*       CHARACTER          COMPQ, COMPZ */
/*       INTEGER            IHI, ILO, INFO, LDA, LDB, LDQ, LDZ, N, LWORK */
/*       COMPLEX*16         A( LDA, * ), B( LDB, * ), Q( LDQ, * ), */
/*      $                   Z( LDZ, * ), WORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > ZGGHD3 reduces a pair of complex matrices (A,B) to generalized upper */
/* > Hessenberg form using unitary transformations, where A is a */
/* > general matrix and B is upper triangular.  The form of the */
/* > generalized eigenvalue problem is */
/* >    A*x = lambda*B*x, */
/* > and B is typically made upper triangular by computing its QR */
/* > factorization and moving the unitary matrix Q to the left side */
/* > of the equation. */
/* > */
/* > This subroutine simultaneously reduces A to a Hessenberg matrix H: */
/* >    Q**H*A*Z = H */
/* > and transforms B to another upper triangular matrix T: */
/* >    Q**H*B*Z = T */
/* > in order to reduce the problem to its standard form */
/* >    H*y = lambda*T*y */
/* > where y = Z**H*x. */
/* > */
/* > The unitary matrices Q and Z are determined as products of Givens */
/* > rotations.  They may either be formed explicitly, or they may be */
/* > postmultiplied into input matrices Q1 and Z1, so that */
/* >      Q1 * A * Z1**H = (Q1*Q) * H * (Z1*Z)**H */
/* >      Q1 * B * Z1**H = (Q1*Q) * T * (Z1*Z)**H */
/* > If Q1 is the unitary matrix from the QR factorization of B in the */
/* > original equation A*x = lambda*B*x, then ZGGHD3 reduces the original */
/* > problem to generalized Hessenberg form. */
/* > */
/* > This is a blocked variant of CGGHRD, using matrix-matrix */
/* > multiplications for parts of the computation to enhance performance. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] COMPQ */
/* > \verbatim */
/* >          COMPQ is CHARACTER*1 */
/* >          = 'N': do not compute Q; */
/* >          = 'I': Q is initialized to the unit matrix, and the */
/* >                 unitary matrix Q is returned; */
/* >          = 'V': Q must contain a unitary matrix Q1 on entry, */
/* >                 and the product Q1*Q is returned. */
/* > \endverbatim */
/* > */
/* > \param[in] COMPZ */
/* > \verbatim */
/* >          COMPZ is CHARACTER*1 */
/* >          = 'N': do not compute Z; */
/* >          = 'I': Z is initialized to the unit matrix, and the */
/* >                 unitary matrix Z is returned; */
/* >          = 'V': Z must contain a unitary matrix Z1 on entry, */
/* >                 and the product Z1*Z is returned. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrices A and B.  N >= 0. */
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
/* > */
/* >          ILO and IHI mark the rows and columns of A which are to be */
/* >          reduced.  It is assumed that A is already upper triangular */
/* >          in rows and columns 1:ILO-1 and IHI+1:N.  ILO and IHI are */
/* >          normally set by a previous call to ZGGBAL; otherwise they */
/* >          should be set to 1 and N respectively. */
/* >          1 <= ILO <= IHI <= N, if N > 0; ILO=1 and IHI=0, if N=0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] A */
/* > \verbatim */
/* >          A is COMPLEX*16 array, dimension (LDA, N) */
/* >          On entry, the N-by-N general matrix to be reduced. */
/* >          On exit, the upper triangle and the first subdiagonal of A */
/* >          are overwritten with the upper Hessenberg matrix H, and the */
/* >          rest is set to zero. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of the array A.  LDA >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in,out] B */
/* > \verbatim */
/* >          B is COMPLEX*16 array, dimension (LDB, N) */
/* >          On entry, the N-by-N upper triangular matrix B. */
/* >          On exit, the upper triangular matrix T = Q**H B Z.  The */
/* >          elements below the diagonal are set to zero. */
/* > \endverbatim */
/* > */
/* > \param[in] LDB */
/* > \verbatim */
/* >          LDB is INTEGER */
/* >          The leading dimension of the array B.  LDB >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in,out] Q */
/* > \verbatim */
/* >          Q is COMPLEX*16 array, dimension (LDQ, N) */
/* >          On entry, if COMPQ = 'V', the unitary matrix Q1, typically */
/* >          from the QR factorization of B. */
/* >          On exit, if COMPQ='I', the unitary matrix Q, and if */
/* >          COMPQ = 'V', the product Q1*Q. */
/* >          Not referenced if COMPQ='N'. */
/* > \endverbatim */
/* > */
/* > \param[in] LDQ */
/* > \verbatim */
/* >          LDQ is INTEGER */
/* >          The leading dimension of the array Q. */
/* >          LDQ >= N if COMPQ='V' or 'I'; LDQ >= 1 otherwise. */
/* > \endverbatim */
/* > */
/* > \param[in,out] Z */
/* > \verbatim */
/* >          Z is COMPLEX*16 array, dimension (LDZ, N) */
/* >          On entry, if COMPZ = 'V', the unitary matrix Z1. */
/* >          On exit, if COMPZ='I', the unitary matrix Z, and if */
/* >          COMPZ = 'V', the product Z1*Z. */
/* >          Not referenced if COMPZ='N'. */
/* > \endverbatim */
/* > */
/* > \param[in] LDZ */
/* > \verbatim */
/* >          LDZ is INTEGER */
/* >          The leading dimension of the array Z. */
/* >          LDZ >= N if COMPZ='V' or 'I'; LDZ >= 1 otherwise. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is COMPLEX*16 array, dimension (LWORK) */
/* >          On exit, if INFO = 0, WORK(1) returns the optimal LWORK. */
/* > \endverbatim */
/* > */
/* > \param[in]  LWORK */
/* > \verbatim */
/* >          LWORK is INTEGER */
/* >          The length of the array WORK.  LWORK >= 1. */
/* >          For optimum performance LWORK >= 6*N*NB, where NB is the */
/* >          optimal blocksize. */
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

/* > \date January 2015 */

/* > \ingroup complex16OTHERcomputational */

/* > \par Further Details: */
/*  ===================== */
/* > */
/* > \verbatim */
/* > */
/* >  This routine reduces A to Hessenberg form and maintains B in */
/* >  using a blocked variant of Moler and Stewart's original algorithm, */
/* >  as described by Kagstrom, Kressner, Quintana-Orti, and Quintana-Orti */
/* >  (BIT 2008). */
/* > \endverbatim */
/* > */
/*  ===================================================================== */
/* Subroutine */ void zgghd3_(char *compq, char *compz, integer *n, integer *
	ilo, integer *ihi, doublecomplex *a, integer *lda, doublecomplex *b, 
	integer *ldb, doublecomplex *q, integer *ldq, doublecomplex *z__, 
	integer *ldz, doublecomplex *work, integer *lwork, integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, q_dim1, q_offset, z_dim1, 
	    z_offset, i__1, i__2, i__3, i__4, i__5, i__6, i__7, i__8, i__9;
    doublecomplex z__1, z__2, z__3, z__4;

    /* Local variables */
    logical blk22;
    integer cola, jcol, ierr;
    doublecomplex temp;
    integer jrow, topq, ppwo;
    extern /* Subroutine */ void zrot_(integer *, doublecomplex *, integer *, 
	    doublecomplex *, integer *, doublereal *, doublecomplex *);
    doublecomplex temp1, temp2, temp3;
    doublereal c__;
    integer kacc22, i__, j, k;
    doublecomplex s;
    extern logical lsame_(char *, char *);
    integer nbmin;
    doublecomplex ctemp;
    extern /* Subroutine */ void zgemm_(char *, char *, integer *, integer *, 
	    integer *, doublecomplex *, doublecomplex *, integer *, 
	    doublecomplex *, integer *, doublecomplex *, doublecomplex *, 
	    integer *);
    integer nblst;
    logical initq;
    doublecomplex c1, c2;
    extern /* Subroutine */ void zgemv_(char *, integer *, integer *, 
	    doublecomplex *, doublecomplex *, integer *, doublecomplex *, 
	    integer *, doublecomplex *, doublecomplex *, integer *);
    logical wantq;
    integer j0;
    logical initz;
    extern /* Subroutine */ void zunm22_(char *, char *, integer *, integer *, 
	    integer *, integer *, doublecomplex *, integer *, doublecomplex *,
	     integer *, doublecomplex *, integer *, integer *)
	    ;
    logical wantz;
    doublecomplex s1, s2;
    extern /* Subroutine */ void ztrmv_(char *, char *, char *, integer *, 
	    doublecomplex *, integer *, doublecomplex *, integer *);
    char compq2[1], compz2[1];
    integer nb, jj, nh, nx, pw;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    extern integer ilaenv_(integer *, char *, char *, integer *, integer *, 
	    integer *, integer *, ftnlen, ftnlen);
    extern /* Subroutine */ void zgghrd_(char *, char *, integer *, integer *, 
	    integer *, doublecomplex *, integer *, doublecomplex *, integer *,
	     doublecomplex *, integer *, doublecomplex *, integer *, integer *
	    ), zlaset_(char *, integer *, integer *, 
	    doublecomplex *, doublecomplex *, doublecomplex *, integer *), zlartg_(doublecomplex *, doublecomplex *, doublereal *, 
	    doublecomplex *, doublecomplex *), zlacpy_(char *, integer *, 
	    integer *, doublecomplex *, integer *, doublecomplex *, integer *);
    integer lwkopt;
    logical lquery;
    integer nnb, len, top, ppw, n2nb;


/*  -- LAPACK computational routine (version 3.8.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     January 2015 */



/*  ===================================================================== */


/*     Decode and test the input parameters. */

    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    b_dim1 = *ldb;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    q_dim1 = *ldq;
    q_offset = 1 + q_dim1 * 1;
    q -= q_offset;
    z_dim1 = *ldz;
    z_offset = 1 + z_dim1 * 1;
    z__ -= z_offset;
    --work;

    /* Function Body */
    *info = 0;
    nb = ilaenv_(&c__1, "ZGGHD3", " ", n, ilo, ihi, &c_n1, (ftnlen)6, (ftnlen)
	    1);
/* Computing MAX */
    i__1 = *n * 6 * nb;
    lwkopt = f2cmax(i__1,1);
    z__1.r = (doublereal) lwkopt, z__1.i = 0.;
    work[1].r = z__1.r, work[1].i = z__1.i;
    initq = lsame_(compq, "I");
    wantq = initq || lsame_(compq, "V");
    initz = lsame_(compz, "I");
    wantz = initz || lsame_(compz, "V");
    lquery = *lwork == -1;

    if (! lsame_(compq, "N") && ! wantq) {
	*info = -1;
    } else if (! lsame_(compz, "N") && ! wantz) {
	*info = -2;
    } else if (*n < 0) {
	*info = -3;
    } else if (*ilo < 1) {
	*info = -4;
    } else if (*ihi > *n || *ihi < *ilo - 1) {
	*info = -5;
    } else if (*lda < f2cmax(1,*n)) {
	*info = -7;
    } else if (*ldb < f2cmax(1,*n)) {
	*info = -9;
    } else if (wantq && *ldq < *n || *ldq < 1) {
	*info = -11;
    } else if (wantz && *ldz < *n || *ldz < 1) {
	*info = -13;
    } else if (*lwork < 1 && ! lquery) {
	*info = -15;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("ZGGHD3", &i__1, (ftnlen)6);
	return;
    } else if (lquery) {
	return;
    }

/*     Initialize Q and Z if desired. */

    if (initq) {
	zlaset_("All", n, n, &c_b2, &c_b1, &q[q_offset], ldq);
    }
    if (initz) {
	zlaset_("All", n, n, &c_b2, &c_b1, &z__[z_offset], ldz);
    }

/*     Zero out lower triangle of B. */

    if (*n > 1) {
	i__1 = *n - 1;
	i__2 = *n - 1;
	zlaset_("Lower", &i__1, &i__2, &c_b2, &c_b2, &b[b_dim1 + 2], ldb);
    }

/*     Quick return if possible */

    nh = *ihi - *ilo + 1;
    if (nh <= 1) {
	work[1].r = 1., work[1].i = 0.;
	return;
    }

/*     Determine the blocksize. */

    nbmin = ilaenv_(&c__2, "ZGGHD3", " ", n, ilo, ihi, &c_n1, (ftnlen)6, (
	    ftnlen)1);
    if (nb > 1 && nb < nh) {

/*        Determine when to use unblocked instead of blocked code. */

/* Computing MAX */
	i__1 = nb, i__2 = ilaenv_(&c__3, "ZGGHD3", " ", n, ilo, ihi, &c_n1, (
		ftnlen)6, (ftnlen)1);
	nx = f2cmax(i__1,i__2);
	if (nx < nh) {

/*           Determine if workspace is large enough for blocked code. */

	    if (*lwork < lwkopt) {

/*              Not enough workspace to use optimal NB:  determine the */
/*              minimum value of NB, and reduce NB or force use of */
/*              unblocked code. */

/* Computing MAX */
		i__1 = 2, i__2 = ilaenv_(&c__2, "ZGGHD3", " ", n, ilo, ihi, &
			c_n1, (ftnlen)6, (ftnlen)1);
		nbmin = f2cmax(i__1,i__2);
		if (*lwork >= *n * 6 * nbmin) {
		    nb = *lwork / (*n * 6);
		} else {
		    nb = 1;
		}
	    }
	}
    }

    if (nb < nbmin || nb >= nh) {

/*        Use unblocked code below */

	jcol = *ilo;

    } else {

/*        Use blocked code */

	kacc22 = ilaenv_(&c__16, "ZGGHD3", " ", n, ilo, ihi, &c_n1, (ftnlen)6,
		 (ftnlen)1);
	blk22 = kacc22 == 2;
	i__1 = *ihi - 2;
	i__2 = nb;
	for (jcol = *ilo; i__2 < 0 ? jcol >= i__1 : jcol <= i__1; jcol += 
		i__2) {
/* Computing MIN */
	    i__3 = nb, i__4 = *ihi - jcol - 1;
	    nnb = f2cmin(i__3,i__4);

/*           Initialize small unitary factors that will hold the */
/*           accumulated Givens rotations in workspace. */
/*           N2NB   denotes the number of 2*NNB-by-2*NNB factors */
/*           NBLST  denotes the (possibly smaller) order of the last */
/*                  factor. */

	    n2nb = (*ihi - jcol - 1) / nnb - 1;
	    nblst = *ihi - jcol - n2nb * nnb;
	    zlaset_("All", &nblst, &nblst, &c_b2, &c_b1, &work[1], &nblst);
	    pw = nblst * nblst + 1;
	    i__3 = n2nb;
	    for (i__ = 1; i__ <= i__3; ++i__) {
		i__4 = nnb << 1;
		i__5 = nnb << 1;
		i__6 = nnb << 1;
		zlaset_("All", &i__4, &i__5, &c_b2, &c_b1, &work[pw], &i__6);
		pw += (nnb << 2) * nnb;
	    }

/*           Reduce columns JCOL:JCOL+NNB-1 of A to Hessenberg form. */

	    i__3 = jcol + nnb - 1;
	    for (j = jcol; j <= i__3; ++j) {

/*              Reduce Jth column of A. Store cosines and sines in Jth */
/*              column of A and B, respectively. */

		i__4 = j + 2;
		for (i__ = *ihi; i__ >= i__4; --i__) {
		    i__5 = i__ - 1 + j * a_dim1;
		    temp.r = a[i__5].r, temp.i = a[i__5].i;
		    zlartg_(&temp, &a[i__ + j * a_dim1], &c__, &s, &a[i__ - 1 
			    + j * a_dim1]);
		    i__5 = i__ + j * a_dim1;
		    z__1.r = c__, z__1.i = 0.;
		    a[i__5].r = z__1.r, a[i__5].i = z__1.i;
		    i__5 = i__ + j * b_dim1;
		    b[i__5].r = s.r, b[i__5].i = s.i;
		}

/*              Accumulate Givens rotations into workspace array. */

		ppw = (nblst + 1) * (nblst - 2) - j + jcol + 1;
		len = j + 2 - jcol;
		jrow = j + n2nb * nnb + 2;
		i__4 = jrow;
		for (i__ = *ihi; i__ >= i__4; --i__) {
		    i__5 = i__ + j * a_dim1;
		    ctemp.r = a[i__5].r, ctemp.i = a[i__5].i;
		    i__5 = i__ + j * b_dim1;
		    s.r = b[i__5].r, s.i = b[i__5].i;
		    i__5 = ppw + len - 1;
		    for (jj = ppw; jj <= i__5; ++jj) {
			i__6 = jj + nblst;
			temp.r = work[i__6].r, temp.i = work[i__6].i;
			i__6 = jj + nblst;
			z__2.r = ctemp.r * temp.r - ctemp.i * temp.i, z__2.i =
				 ctemp.r * temp.i + ctemp.i * temp.r;
			i__7 = jj;
			z__3.r = s.r * work[i__7].r - s.i * work[i__7].i, 
				z__3.i = s.r * work[i__7].i + s.i * work[i__7]
				.r;
			z__1.r = z__2.r - z__3.r, z__1.i = z__2.i - z__3.i;
			work[i__6].r = z__1.r, work[i__6].i = z__1.i;
			i__6 = jj;
			d_cnjg(&z__3, &s);
			z__2.r = z__3.r * temp.r - z__3.i * temp.i, z__2.i = 
				z__3.r * temp.i + z__3.i * temp.r;
			i__7 = jj;
			z__4.r = ctemp.r * work[i__7].r - ctemp.i * work[i__7]
				.i, z__4.i = ctemp.r * work[i__7].i + ctemp.i 
				* work[i__7].r;
			z__1.r = z__2.r + z__4.r, z__1.i = z__2.i + z__4.i;
			work[i__6].r = z__1.r, work[i__6].i = z__1.i;
		    }
		    ++len;
		    ppw = ppw - nblst - 1;
		}

		ppwo = nblst * nblst + (nnb + j - jcol - 1 << 1) * nnb + nnb;
		j0 = jrow - nnb;
		i__4 = j + 2;
		i__5 = -nnb;
		for (jrow = j0; i__5 < 0 ? jrow >= i__4 : jrow <= i__4; jrow 
			+= i__5) {
		    ppw = ppwo;
		    len = j + 2 - jcol;
		    i__6 = jrow;
		    for (i__ = jrow + nnb - 1; i__ >= i__6; --i__) {
			i__7 = i__ + j * a_dim1;
			ctemp.r = a[i__7].r, ctemp.i = a[i__7].i;
			i__7 = i__ + j * b_dim1;
			s.r = b[i__7].r, s.i = b[i__7].i;
			i__7 = ppw + len - 1;
			for (jj = ppw; jj <= i__7; ++jj) {
			    i__8 = jj + (nnb << 1);
			    temp.r = work[i__8].r, temp.i = work[i__8].i;
			    i__8 = jj + (nnb << 1);
			    z__2.r = ctemp.r * temp.r - ctemp.i * temp.i, 
				    z__2.i = ctemp.r * temp.i + ctemp.i * 
				    temp.r;
			    i__9 = jj;
			    z__3.r = s.r * work[i__9].r - s.i * work[i__9].i, 
				    z__3.i = s.r * work[i__9].i + s.i * work[
				    i__9].r;
			    z__1.r = z__2.r - z__3.r, z__1.i = z__2.i - 
				    z__3.i;
			    work[i__8].r = z__1.r, work[i__8].i = z__1.i;
			    i__8 = jj;
			    d_cnjg(&z__3, &s);
			    z__2.r = z__3.r * temp.r - z__3.i * temp.i, 
				    z__2.i = z__3.r * temp.i + z__3.i * 
				    temp.r;
			    i__9 = jj;
			    z__4.r = ctemp.r * work[i__9].r - ctemp.i * work[
				    i__9].i, z__4.i = ctemp.r * work[i__9].i 
				    + ctemp.i * work[i__9].r;
			    z__1.r = z__2.r + z__4.r, z__1.i = z__2.i + 
				    z__4.i;
			    work[i__8].r = z__1.r, work[i__8].i = z__1.i;
			}
			++len;
			ppw = ppw - (nnb << 1) - 1;
		    }
		    ppwo += (nnb << 2) * nnb;
		}

/*              TOP denotes the number of top rows in A and B that will */
/*              not be updated during the next steps. */

		if (jcol <= 2) {
		    top = 0;
		} else {
		    top = jcol;
		}

/*              Propagate transformations through B and replace stored */
/*              left sines/cosines by right sines/cosines. */

		i__5 = j + 1;
		for (jj = *n; jj >= i__5; --jj) {

/*                 Update JJth column of B. */

/* Computing MIN */
		    i__4 = jj + 1;
		    i__6 = j + 2;
		    for (i__ = f2cmin(i__4,*ihi); i__ >= i__6; --i__) {
			i__4 = i__ + j * a_dim1;
			ctemp.r = a[i__4].r, ctemp.i = a[i__4].i;
			i__4 = i__ + j * b_dim1;
			s.r = b[i__4].r, s.i = b[i__4].i;
			i__4 = i__ + jj * b_dim1;
			temp.r = b[i__4].r, temp.i = b[i__4].i;
			i__4 = i__ + jj * b_dim1;
			z__2.r = ctemp.r * temp.r - ctemp.i * temp.i, z__2.i =
				 ctemp.r * temp.i + ctemp.i * temp.r;
			d_cnjg(&z__4, &s);
			i__7 = i__ - 1 + jj * b_dim1;
			z__3.r = z__4.r * b[i__7].r - z__4.i * b[i__7].i, 
				z__3.i = z__4.r * b[i__7].i + z__4.i * b[i__7]
				.r;
			z__1.r = z__2.r - z__3.r, z__1.i = z__2.i - z__3.i;
			b[i__4].r = z__1.r, b[i__4].i = z__1.i;
			i__4 = i__ - 1 + jj * b_dim1;
			z__2.r = s.r * temp.r - s.i * temp.i, z__2.i = s.r * 
				temp.i + s.i * temp.r;
			i__7 = i__ - 1 + jj * b_dim1;
			z__3.r = ctemp.r * b[i__7].r - ctemp.i * b[i__7].i, 
				z__3.i = ctemp.r * b[i__7].i + ctemp.i * b[
				i__7].r;
			z__1.r = z__2.r + z__3.r, z__1.i = z__2.i + z__3.i;
			b[i__4].r = z__1.r, b[i__4].i = z__1.i;
		    }

/*                 Annihilate B( JJ+1, JJ ). */

		    if (jj < *ihi) {
			i__6 = jj + 1 + (jj + 1) * b_dim1;
			temp.r = b[i__6].r, temp.i = b[i__6].i;
			zlartg_(&temp, &b[jj + 1 + jj * b_dim1], &c__, &s, &b[
				jj + 1 + (jj + 1) * b_dim1]);
			i__6 = jj + 1 + jj * b_dim1;
			b[i__6].r = 0., b[i__6].i = 0.;
			i__6 = jj - top;
			zrot_(&i__6, &b[top + 1 + (jj + 1) * b_dim1], &c__1, &
				b[top + 1 + jj * b_dim1], &c__1, &c__, &s);
			i__6 = jj + 1 + j * a_dim1;
			z__1.r = c__, z__1.i = 0.;
			a[i__6].r = z__1.r, a[i__6].i = z__1.i;
			i__6 = jj + 1 + j * b_dim1;
			d_cnjg(&z__2, &s);
			z__1.r = -z__2.r, z__1.i = -z__2.i;
			b[i__6].r = z__1.r, b[i__6].i = z__1.i;
		    }
		}

/*              Update A by transformations from right. */

		jj = (*ihi - j - 1) % 3;
		i__5 = jj + 1;
		for (i__ = *ihi - j - 3; i__ >= i__5; i__ += -3) {
		    i__6 = j + 1 + i__ + j * a_dim1;
		    ctemp.r = a[i__6].r, ctemp.i = a[i__6].i;
		    i__6 = j + 1 + i__ + j * b_dim1;
		    z__1.r = -b[i__6].r, z__1.i = -b[i__6].i;
		    s.r = z__1.r, s.i = z__1.i;
		    i__6 = j + 2 + i__ + j * a_dim1;
		    c1.r = a[i__6].r, c1.i = a[i__6].i;
		    i__6 = j + 2 + i__ + j * b_dim1;
		    z__1.r = -b[i__6].r, z__1.i = -b[i__6].i;
		    s1.r = z__1.r, s1.i = z__1.i;
		    i__6 = j + 3 + i__ + j * a_dim1;
		    c2.r = a[i__6].r, c2.i = a[i__6].i;
		    i__6 = j + 3 + i__ + j * b_dim1;
		    z__1.r = -b[i__6].r, z__1.i = -b[i__6].i;
		    s2.r = z__1.r, s2.i = z__1.i;

		    i__6 = *ihi;
		    for (k = top + 1; k <= i__6; ++k) {
			i__4 = k + (j + i__) * a_dim1;
			temp.r = a[i__4].r, temp.i = a[i__4].i;
			i__4 = k + (j + i__ + 1) * a_dim1;
			temp1.r = a[i__4].r, temp1.i = a[i__4].i;
			i__4 = k + (j + i__ + 2) * a_dim1;
			temp2.r = a[i__4].r, temp2.i = a[i__4].i;
			i__4 = k + (j + i__ + 3) * a_dim1;
			temp3.r = a[i__4].r, temp3.i = a[i__4].i;
			i__4 = k + (j + i__ + 3) * a_dim1;
			z__2.r = c2.r * temp3.r - c2.i * temp3.i, z__2.i = 
				c2.r * temp3.i + c2.i * temp3.r;
			d_cnjg(&z__4, &s2);
			z__3.r = z__4.r * temp2.r - z__4.i * temp2.i, z__3.i =
				 z__4.r * temp2.i + z__4.i * temp2.r;
			z__1.r = z__2.r + z__3.r, z__1.i = z__2.i + z__3.i;
			a[i__4].r = z__1.r, a[i__4].i = z__1.i;
			z__3.r = -s2.r, z__3.i = -s2.i;
			z__2.r = z__3.r * temp3.r - z__3.i * temp3.i, z__2.i =
				 z__3.r * temp3.i + z__3.i * temp3.r;
			z__4.r = c2.r * temp2.r - c2.i * temp2.i, z__4.i = 
				c2.r * temp2.i + c2.i * temp2.r;
			z__1.r = z__2.r + z__4.r, z__1.i = z__2.i + z__4.i;
			temp2.r = z__1.r, temp2.i = z__1.i;
			i__4 = k + (j + i__ + 2) * a_dim1;
			z__2.r = c1.r * temp2.r - c1.i * temp2.i, z__2.i = 
				c1.r * temp2.i + c1.i * temp2.r;
			d_cnjg(&z__4, &s1);
			z__3.r = z__4.r * temp1.r - z__4.i * temp1.i, z__3.i =
				 z__4.r * temp1.i + z__4.i * temp1.r;
			z__1.r = z__2.r + z__3.r, z__1.i = z__2.i + z__3.i;
			a[i__4].r = z__1.r, a[i__4].i = z__1.i;
			z__3.r = -s1.r, z__3.i = -s1.i;
			z__2.r = z__3.r * temp2.r - z__3.i * temp2.i, z__2.i =
				 z__3.r * temp2.i + z__3.i * temp2.r;
			z__4.r = c1.r * temp1.r - c1.i * temp1.i, z__4.i = 
				c1.r * temp1.i + c1.i * temp1.r;
			z__1.r = z__2.r + z__4.r, z__1.i = z__2.i + z__4.i;
			temp1.r = z__1.r, temp1.i = z__1.i;
			i__4 = k + (j + i__ + 1) * a_dim1;
			z__2.r = ctemp.r * temp1.r - ctemp.i * temp1.i, 
				z__2.i = ctemp.r * temp1.i + ctemp.i * 
				temp1.r;
			d_cnjg(&z__4, &s);
			z__3.r = z__4.r * temp.r - z__4.i * temp.i, z__3.i = 
				z__4.r * temp.i + z__4.i * temp.r;
			z__1.r = z__2.r + z__3.r, z__1.i = z__2.i + z__3.i;
			a[i__4].r = z__1.r, a[i__4].i = z__1.i;
			i__4 = k + (j + i__) * a_dim1;
			z__3.r = -s.r, z__3.i = -s.i;
			z__2.r = z__3.r * temp1.r - z__3.i * temp1.i, z__2.i =
				 z__3.r * temp1.i + z__3.i * temp1.r;
			z__4.r = ctemp.r * temp.r - ctemp.i * temp.i, z__4.i =
				 ctemp.r * temp.i + ctemp.i * temp.r;
			z__1.r = z__2.r + z__4.r, z__1.i = z__2.i + z__4.i;
			a[i__4].r = z__1.r, a[i__4].i = z__1.i;
		    }
		}

		if (jj > 0) {
		    for (i__ = jj; i__ >= 1; --i__) {
			i__5 = j + 1 + i__ + j * a_dim1;
			c__ = a[i__5].r;
			i__5 = *ihi - top;
			d_cnjg(&z__2, &b[j + 1 + i__ + j * b_dim1]);
			z__1.r = -z__2.r, z__1.i = -z__2.i;
			zrot_(&i__5, &a[top + 1 + (j + i__ + 1) * a_dim1], &
				c__1, &a[top + 1 + (j + i__) * a_dim1], &c__1,
				 &c__, &z__1);
		    }
		}

/*              Update (J+1)th column of A by transformations from left. */

		if (j < jcol + nnb - 1) {
		    len = j + 1 - jcol;

/*                 Multiply with the trailing accumulated unitary */
/*                 matrix, which takes the form */

/*                        [  U11  U12  ] */
/*                    U = [            ], */
/*                        [  U21  U22  ] */

/*                 where U21 is a LEN-by-LEN matrix and U12 is lower */
/*                 triangular. */

		    jrow = *ihi - nblst + 1;
		    zgemv_("Conjugate", &nblst, &len, &c_b1, &work[1], &nblst,
			     &a[jrow + (j + 1) * a_dim1], &c__1, &c_b2, &work[
			    pw], &c__1);
		    ppw = pw + len;
		    i__5 = jrow + nblst - len - 1;
		    for (i__ = jrow; i__ <= i__5; ++i__) {
			i__6 = ppw;
			i__4 = i__ + (j + 1) * a_dim1;
			work[i__6].r = a[i__4].r, work[i__6].i = a[i__4].i;
			++ppw;
		    }
		    i__5 = nblst - len;
		    ztrmv_("Lower", "Conjugate", "Non-unit", &i__5, &work[len 
			    * nblst + 1], &nblst, &work[pw + len], &c__1);
		    i__5 = nblst - len;
		    zgemv_("Conjugate", &len, &i__5, &c_b1, &work[(len + 1) * 
			    nblst - len + 1], &nblst, &a[jrow + nblst - len + 
			    (j + 1) * a_dim1], &c__1, &c_b1, &work[pw + len], 
			    &c__1);
		    ppw = pw;
		    i__5 = jrow + nblst - 1;
		    for (i__ = jrow; i__ <= i__5; ++i__) {
			i__6 = i__ + (j + 1) * a_dim1;
			i__4 = ppw;
			a[i__6].r = work[i__4].r, a[i__6].i = work[i__4].i;
			++ppw;
		    }

/*                 Multiply with the other accumulated unitary */
/*                 matrices, which take the form */

/*                        [  U11  U12   0  ] */
/*                        [                ] */
/*                    U = [  U21  U22   0  ], */
/*                        [                ] */
/*                        [   0    0    I  ] */

/*                 where I denotes the (NNB-LEN)-by-(NNB-LEN) identity */
/*                 matrix, U21 is a LEN-by-LEN upper triangular matrix */
/*                 and U12 is an NNB-by-NNB lower triangular matrix. */

		    ppwo = nblst * nblst + 1;
		    j0 = jrow - nnb;
		    i__5 = jcol + 1;
		    i__6 = -nnb;
		    for (jrow = j0; i__6 < 0 ? jrow >= i__5 : jrow <= i__5; 
			    jrow += i__6) {
			ppw = pw + len;
			i__4 = jrow + nnb - 1;
			for (i__ = jrow; i__ <= i__4; ++i__) {
			    i__7 = ppw;
			    i__8 = i__ + (j + 1) * a_dim1;
			    work[i__7].r = a[i__8].r, work[i__7].i = a[i__8]
				    .i;
			    ++ppw;
			}
			ppw = pw;
			i__4 = jrow + nnb + len - 1;
			for (i__ = jrow + nnb; i__ <= i__4; ++i__) {
			    i__7 = ppw;
			    i__8 = i__ + (j + 1) * a_dim1;
			    work[i__7].r = a[i__8].r, work[i__7].i = a[i__8]
				    .i;
			    ++ppw;
			}
			i__4 = nnb << 1;
			ztrmv_("Upper", "Conjugate", "Non-unit", &len, &work[
				ppwo + nnb], &i__4, &work[pw], &c__1);
			i__4 = nnb << 1;
			ztrmv_("Lower", "Conjugate", "Non-unit", &nnb, &work[
				ppwo + (len << 1) * nnb], &i__4, &work[pw + 
				len], &c__1);
			i__4 = nnb << 1;
			zgemv_("Conjugate", &nnb, &len, &c_b1, &work[ppwo], &
				i__4, &a[jrow + (j + 1) * a_dim1], &c__1, &
				c_b1, &work[pw], &c__1);
			i__4 = nnb << 1;
			zgemv_("Conjugate", &len, &nnb, &c_b1, &work[ppwo + (
				len << 1) * nnb + nnb], &i__4, &a[jrow + nnb 
				+ (j + 1) * a_dim1], &c__1, &c_b1, &work[pw + 
				len], &c__1);
			ppw = pw;
			i__4 = jrow + len + nnb - 1;
			for (i__ = jrow; i__ <= i__4; ++i__) {
			    i__7 = i__ + (j + 1) * a_dim1;
			    i__8 = ppw;
			    a[i__7].r = work[i__8].r, a[i__7].i = work[i__8]
				    .i;
			    ++ppw;
			}
			ppwo += (nnb << 2) * nnb;
		    }
		}
	    }

/*           Apply accumulated unitary matrices to A. */

	    cola = *n - jcol - nnb + 1;
	    j = *ihi - nblst + 1;
	    zgemm_("Conjugate", "No Transpose", &nblst, &cola, &nblst, &c_b1, 
		    &work[1], &nblst, &a[j + (jcol + nnb) * a_dim1], lda, &
		    c_b2, &work[pw], &nblst);
	    zlacpy_("All", &nblst, &cola, &work[pw], &nblst, &a[j + (jcol + 
		    nnb) * a_dim1], lda);
	    ppwo = nblst * nblst + 1;
	    j0 = j - nnb;
	    i__3 = jcol + 1;
	    i__6 = -nnb;
	    for (j = j0; i__6 < 0 ? j >= i__3 : j <= i__3; j += i__6) {
		if (blk22) {

/*                 Exploit the structure of */

/*                        [  U11  U12  ] */
/*                    U = [            ] */
/*                        [  U21  U22  ], */

/*                 where all blocks are NNB-by-NNB, U21 is upper */
/*                 triangular and U12 is lower triangular. */

		    i__5 = nnb << 1;
		    i__4 = nnb << 1;
		    i__7 = *lwork - pw + 1;
		    zunm22_("Left", "Conjugate", &i__5, &cola, &nnb, &nnb, &
			    work[ppwo], &i__4, &a[j + (jcol + nnb) * a_dim1], 
			    lda, &work[pw], &i__7, &ierr);
		} else {

/*                 Ignore the structure of U. */

		    i__5 = nnb << 1;
		    i__4 = nnb << 1;
		    i__7 = nnb << 1;
		    i__8 = nnb << 1;
		    zgemm_("Conjugate", "No Transpose", &i__5, &cola, &i__4, &
			    c_b1, &work[ppwo], &i__7, &a[j + (jcol + nnb) * 
			    a_dim1], lda, &c_b2, &work[pw], &i__8);
		    i__5 = nnb << 1;
		    i__4 = nnb << 1;
		    zlacpy_("All", &i__5, &cola, &work[pw], &i__4, &a[j + (
			    jcol + nnb) * a_dim1], lda);
		}
		ppwo += (nnb << 2) * nnb;
	    }

/*           Apply accumulated unitary matrices to Q. */

	    if (wantq) {
		j = *ihi - nblst + 1;
		if (initq) {
/* Computing MAX */
		    i__6 = 2, i__3 = j - jcol + 1;
		    topq = f2cmax(i__6,i__3);
		    nh = *ihi - topq + 1;
		} else {
		    topq = 1;
		    nh = *n;
		}
		zgemm_("No Transpose", "No Transpose", &nh, &nblst, &nblst, &
			c_b1, &q[topq + j * q_dim1], ldq, &work[1], &nblst, &
			c_b2, &work[pw], &nh);
		zlacpy_("All", &nh, &nblst, &work[pw], &nh, &q[topq + j * 
			q_dim1], ldq);
		ppwo = nblst * nblst + 1;
		j0 = j - nnb;
		i__6 = jcol + 1;
		i__3 = -nnb;
		for (j = j0; i__3 < 0 ? j >= i__6 : j <= i__6; j += i__3) {
		    if (initq) {
/* Computing MAX */
			i__5 = 2, i__4 = j - jcol + 1;
			topq = f2cmax(i__5,i__4);
			nh = *ihi - topq + 1;
		    }
		    if (blk22) {

/*                    Exploit the structure of U. */

			i__5 = nnb << 1;
			i__4 = nnb << 1;
			i__7 = *lwork - pw + 1;
			zunm22_("Right", "No Transpose", &nh, &i__5, &nnb, &
				nnb, &work[ppwo], &i__4, &q[topq + j * q_dim1]
				, ldq, &work[pw], &i__7, &ierr);
		    } else {

/*                    Ignore the structure of U. */

			i__5 = nnb << 1;
			i__4 = nnb << 1;
			i__7 = nnb << 1;
			zgemm_("No Transpose", "No Transpose", &nh, &i__5, &
				i__4, &c_b1, &q[topq + j * q_dim1], ldq, &
				work[ppwo], &i__7, &c_b2, &work[pw], &nh);
			i__5 = nnb << 1;
			zlacpy_("All", &nh, &i__5, &work[pw], &nh, &q[topq + 
				j * q_dim1], ldq);
		    }
		    ppwo += (nnb << 2) * nnb;
		}
	    }

/*           Accumulate right Givens rotations if required. */

	    if (wantz || top > 0) {

/*              Initialize small unitary factors that will hold the */
/*              accumulated Givens rotations in workspace. */

		zlaset_("All", &nblst, &nblst, &c_b2, &c_b1, &work[1], &nblst);
		pw = nblst * nblst + 1;
		i__3 = n2nb;
		for (i__ = 1; i__ <= i__3; ++i__) {
		    i__6 = nnb << 1;
		    i__5 = nnb << 1;
		    i__4 = nnb << 1;
		    zlaset_("All", &i__6, &i__5, &c_b2, &c_b1, &work[pw], &
			    i__4);
		    pw += (nnb << 2) * nnb;
		}

/*              Accumulate Givens rotations into workspace array. */

		i__3 = jcol + nnb - 1;
		for (j = jcol; j <= i__3; ++j) {
		    ppw = (nblst + 1) * (nblst - 2) - j + jcol + 1;
		    len = j + 2 - jcol;
		    jrow = j + n2nb * nnb + 2;
		    i__6 = jrow;
		    for (i__ = *ihi; i__ >= i__6; --i__) {
			i__5 = i__ + j * a_dim1;
			ctemp.r = a[i__5].r, ctemp.i = a[i__5].i;
			i__5 = i__ + j * a_dim1;
			a[i__5].r = 0., a[i__5].i = 0.;
			i__5 = i__ + j * b_dim1;
			s.r = b[i__5].r, s.i = b[i__5].i;
			i__5 = i__ + j * b_dim1;
			b[i__5].r = 0., b[i__5].i = 0.;
			i__5 = ppw + len - 1;
			for (jj = ppw; jj <= i__5; ++jj) {
			    i__4 = jj + nblst;
			    temp.r = work[i__4].r, temp.i = work[i__4].i;
			    i__4 = jj + nblst;
			    z__2.r = ctemp.r * temp.r - ctemp.i * temp.i, 
				    z__2.i = ctemp.r * temp.i + ctemp.i * 
				    temp.r;
			    d_cnjg(&z__4, &s);
			    i__7 = jj;
			    z__3.r = z__4.r * work[i__7].r - z__4.i * work[
				    i__7].i, z__3.i = z__4.r * work[i__7].i + 
				    z__4.i * work[i__7].r;
			    z__1.r = z__2.r - z__3.r, z__1.i = z__2.i - 
				    z__3.i;
			    work[i__4].r = z__1.r, work[i__4].i = z__1.i;
			    i__4 = jj;
			    z__2.r = s.r * temp.r - s.i * temp.i, z__2.i = 
				    s.r * temp.i + s.i * temp.r;
			    i__7 = jj;
			    z__3.r = ctemp.r * work[i__7].r - ctemp.i * work[
				    i__7].i, z__3.i = ctemp.r * work[i__7].i 
				    + ctemp.i * work[i__7].r;
			    z__1.r = z__2.r + z__3.r, z__1.i = z__2.i + 
				    z__3.i;
			    work[i__4].r = z__1.r, work[i__4].i = z__1.i;
			}
			++len;
			ppw = ppw - nblst - 1;
		    }

		    ppwo = nblst * nblst + (nnb + j - jcol - 1 << 1) * nnb + 
			    nnb;
		    j0 = jrow - nnb;
		    i__6 = j + 2;
		    i__5 = -nnb;
		    for (jrow = j0; i__5 < 0 ? jrow >= i__6 : jrow <= i__6; 
			    jrow += i__5) {
			ppw = ppwo;
			len = j + 2 - jcol;
			i__4 = jrow;
			for (i__ = jrow + nnb - 1; i__ >= i__4; --i__) {
			    i__7 = i__ + j * a_dim1;
			    ctemp.r = a[i__7].r, ctemp.i = a[i__7].i;
			    i__7 = i__ + j * a_dim1;
			    a[i__7].r = 0., a[i__7].i = 0.;
			    i__7 = i__ + j * b_dim1;
			    s.r = b[i__7].r, s.i = b[i__7].i;
			    i__7 = i__ + j * b_dim1;
			    b[i__7].r = 0., b[i__7].i = 0.;
			    i__7 = ppw + len - 1;
			    for (jj = ppw; jj <= i__7; ++jj) {
				i__8 = jj + (nnb << 1);
				temp.r = work[i__8].r, temp.i = work[i__8].i;
				i__8 = jj + (nnb << 1);
				z__2.r = ctemp.r * temp.r - ctemp.i * temp.i, 
					z__2.i = ctemp.r * temp.i + ctemp.i * 
					temp.r;
				d_cnjg(&z__4, &s);
				i__9 = jj;
				z__3.r = z__4.r * work[i__9].r - z__4.i * 
					work[i__9].i, z__3.i = z__4.r * work[
					i__9].i + z__4.i * work[i__9].r;
				z__1.r = z__2.r - z__3.r, z__1.i = z__2.i - 
					z__3.i;
				work[i__8].r = z__1.r, work[i__8].i = z__1.i;
				i__8 = jj;
				z__2.r = s.r * temp.r - s.i * temp.i, z__2.i =
					 s.r * temp.i + s.i * temp.r;
				i__9 = jj;
				z__3.r = ctemp.r * work[i__9].r - ctemp.i * 
					work[i__9].i, z__3.i = ctemp.r * work[
					i__9].i + ctemp.i * work[i__9].r;
				z__1.r = z__2.r + z__3.r, z__1.i = z__2.i + 
					z__3.i;
				work[i__8].r = z__1.r, work[i__8].i = z__1.i;
			    }
			    ++len;
			    ppw = ppw - (nnb << 1) - 1;
			}
			ppwo += (nnb << 2) * nnb;
		    }
		}
	    } else {

		i__3 = *ihi - jcol - 1;
		zlaset_("Lower", &i__3, &nnb, &c_b2, &c_b2, &a[jcol + 2 + 
			jcol * a_dim1], lda);
		i__3 = *ihi - jcol - 1;
		zlaset_("Lower", &i__3, &nnb, &c_b2, &c_b2, &b[jcol + 2 + 
			jcol * b_dim1], ldb);
	    }

/*           Apply accumulated unitary matrices to A and B. */

	    if (top > 0) {
		j = *ihi - nblst + 1;
		zgemm_("No Transpose", "No Transpose", &top, &nblst, &nblst, &
			c_b1, &a[j * a_dim1 + 1], lda, &work[1], &nblst, &
			c_b2, &work[pw], &top);
		zlacpy_("All", &top, &nblst, &work[pw], &top, &a[j * a_dim1 + 
			1], lda);
		ppwo = nblst * nblst + 1;
		j0 = j - nnb;
		i__3 = jcol + 1;
		i__5 = -nnb;
		for (j = j0; i__5 < 0 ? j >= i__3 : j <= i__3; j += i__5) {
		    if (blk22) {

/*                    Exploit the structure of U. */

			i__6 = nnb << 1;
			i__4 = nnb << 1;
			i__7 = *lwork - pw + 1;
			zunm22_("Right", "No Transpose", &top, &i__6, &nnb, &
				nnb, &work[ppwo], &i__4, &a[j * a_dim1 + 1], 
				lda, &work[pw], &i__7, &ierr);
		    } else {

/*                    Ignore the structure of U. */

			i__6 = nnb << 1;
			i__4 = nnb << 1;
			i__7 = nnb << 1;
			zgemm_("No Transpose", "No Transpose", &top, &i__6, &
				i__4, &c_b1, &a[j * a_dim1 + 1], lda, &work[
				ppwo], &i__7, &c_b2, &work[pw], &top);
			i__6 = nnb << 1;
			zlacpy_("All", &top, &i__6, &work[pw], &top, &a[j * 
				a_dim1 + 1], lda);
		    }
		    ppwo += (nnb << 2) * nnb;
		}

		j = *ihi - nblst + 1;
		zgemm_("No Transpose", "No Transpose", &top, &nblst, &nblst, &
			c_b1, &b[j * b_dim1 + 1], ldb, &work[1], &nblst, &
			c_b2, &work[pw], &top);
		zlacpy_("All", &top, &nblst, &work[pw], &top, &b[j * b_dim1 + 
			1], ldb);
		ppwo = nblst * nblst + 1;
		j0 = j - nnb;
		i__5 = jcol + 1;
		i__3 = -nnb;
		for (j = j0; i__3 < 0 ? j >= i__5 : j <= i__5; j += i__3) {
		    if (blk22) {

/*                    Exploit the structure of U. */

			i__6 = nnb << 1;
			i__4 = nnb << 1;
			i__7 = *lwork - pw + 1;
			zunm22_("Right", "No Transpose", &top, &i__6, &nnb, &
				nnb, &work[ppwo], &i__4, &b[j * b_dim1 + 1], 
				ldb, &work[pw], &i__7, &ierr);
		    } else {

/*                    Ignore the structure of U. */

			i__6 = nnb << 1;
			i__4 = nnb << 1;
			i__7 = nnb << 1;
			zgemm_("No Transpose", "No Transpose", &top, &i__6, &
				i__4, &c_b1, &b[j * b_dim1 + 1], ldb, &work[
				ppwo], &i__7, &c_b2, &work[pw], &top);
			i__6 = nnb << 1;
			zlacpy_("All", &top, &i__6, &work[pw], &top, &b[j * 
				b_dim1 + 1], ldb);
		    }
		    ppwo += (nnb << 2) * nnb;
		}
	    }

/*           Apply accumulated unitary matrices to Z. */

	    if (wantz) {
		j = *ihi - nblst + 1;
		if (initq) {
/* Computing MAX */
		    i__3 = 2, i__5 = j - jcol + 1;
		    topq = f2cmax(i__3,i__5);
		    nh = *ihi - topq + 1;
		} else {
		    topq = 1;
		    nh = *n;
		}
		zgemm_("No Transpose", "No Transpose", &nh, &nblst, &nblst, &
			c_b1, &z__[topq + j * z_dim1], ldz, &work[1], &nblst, 
			&c_b2, &work[pw], &nh);
		zlacpy_("All", &nh, &nblst, &work[pw], &nh, &z__[topq + j * 
			z_dim1], ldz);
		ppwo = nblst * nblst + 1;
		j0 = j - nnb;
		i__3 = jcol + 1;
		i__5 = -nnb;
		for (j = j0; i__5 < 0 ? j >= i__3 : j <= i__3; j += i__5) {
		    if (initq) {
/* Computing MAX */
			i__6 = 2, i__4 = j - jcol + 1;
			topq = f2cmax(i__6,i__4);
			nh = *ihi - topq + 1;
		    }
		    if (blk22) {

/*                    Exploit the structure of U. */

			i__6 = nnb << 1;
			i__4 = nnb << 1;
			i__7 = *lwork - pw + 1;
			zunm22_("Right", "No Transpose", &nh, &i__6, &nnb, &
				nnb, &work[ppwo], &i__4, &z__[topq + j * 
				z_dim1], ldz, &work[pw], &i__7, &ierr);
		    } else {

/*                    Ignore the structure of U. */

			i__6 = nnb << 1;
			i__4 = nnb << 1;
			i__7 = nnb << 1;
			zgemm_("No Transpose", "No Transpose", &nh, &i__6, &
				i__4, &c_b1, &z__[topq + j * z_dim1], ldz, &
				work[ppwo], &i__7, &c_b2, &work[pw], &nh);
			i__6 = nnb << 1;
			zlacpy_("All", &nh, &i__6, &work[pw], &nh, &z__[topq 
				+ j * z_dim1], ldz);
		    }
		    ppwo += (nnb << 2) * nnb;
		}
	    }
	}
    }

/*     Use unblocked code to reduce the rest of the matrix */
/*     Avoid re-initialization of modified Q and Z. */

    *(unsigned char *)compq2 = *(unsigned char *)compq;
    *(unsigned char *)compz2 = *(unsigned char *)compz;
    if (jcol != *ilo) {
	if (wantq) {
	    *(unsigned char *)compq2 = 'V';
	}
	if (wantz) {
	    *(unsigned char *)compz2 = 'V';
	}
    }

    if (jcol < *ihi) {
	zgghrd_(compq2, compz2, n, &jcol, ihi, &a[a_offset], lda, &b[b_offset]
		, ldb, &q[q_offset], ldq, &z__[z_offset], ldz, &ierr);
    }
    z__1.r = (doublereal) lwkopt, z__1.i = 0.;
    work[1].r = z__1.r, work[1].i = z__1.i;

    return;

/*     End of ZGGHD3 */

} /* zgghd3_ */

