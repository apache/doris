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




/* > \brief \b DLAED4 used by sstedc. Finds a single root of the secular equation. */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download DLAED4 + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dlaed4.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dlaed4.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dlaed4.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE DLAED4( N, I, D, Z, DELTA, RHO, DLAM, INFO ) */

/*       INTEGER            I, INFO, N */
/*       DOUBLE PRECISION   DLAM, RHO */
/*       DOUBLE PRECISION   D( * ), DELTA( * ), Z( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > This subroutine computes the I-th updated eigenvalue of a symmetric */
/* > rank-one modification to a diagonal matrix whose elements are */
/* > given in the array d, and that */
/* > */
/* >            D(i) < D(j)  for  i < j */
/* > */
/* > and that RHO > 0.  This is arranged by the calling routine, and is */
/* > no loss in generality.  The rank-one modified system is thus */
/* > */
/* >            diag( D )  +  RHO * Z * Z_transpose. */
/* > */
/* > where we assume the Euclidean norm of Z is 1. */
/* > */
/* > The method consists of approximating the rational functions in the */
/* > secular equation by simpler interpolating rational functions. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >         The length of all arrays. */
/* > \endverbatim */
/* > */
/* > \param[in] I */
/* > \verbatim */
/* >          I is INTEGER */
/* >         The index of the eigenvalue to be computed.  1 <= I <= N. */
/* > \endverbatim */
/* > */
/* > \param[in] D */
/* > \verbatim */
/* >          D is DOUBLE PRECISION array, dimension (N) */
/* >         The original eigenvalues.  It is assumed that they are in */
/* >         order, D(I) < D(J)  for I < J. */
/* > \endverbatim */
/* > */
/* > \param[in] Z */
/* > \verbatim */
/* >          Z is DOUBLE PRECISION array, dimension (N) */
/* >         The components of the updating vector. */
/* > \endverbatim */
/* > */
/* > \param[out] DELTA */
/* > \verbatim */
/* >          DELTA is DOUBLE PRECISION array, dimension (N) */
/* >         If N > 2, DELTA contains (D(j) - lambda_I) in its  j-th */
/* >         component.  If N = 1, then DELTA(1) = 1. If N = 2, see DLAED5 */
/* >         for detail. The vector DELTA contains the information necessary */
/* >         to construct the eigenvectors by DLAED3 and DLAED9. */
/* > \endverbatim */
/* > */
/* > \param[in] RHO */
/* > \verbatim */
/* >          RHO is DOUBLE PRECISION */
/* >         The scalar in the symmetric updating formula. */
/* > \endverbatim */
/* > */
/* > \param[out] DLAM */
/* > \verbatim */
/* >          DLAM is DOUBLE PRECISION */
/* >         The computed lambda_I, the I-th updated eigenvalue. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >         = 0:  successful exit */
/* >         > 0:  if INFO = 1, the updating process failed. */
/* > \endverbatim */

/* > \par Internal Parameters: */
/*  ========================= */
/* > */
/* > \verbatim */
/* >  Logical variable ORGATI (origin-at-i?) is used for distinguishing */
/* >  whether D(i) or D(i+1) is treated as the origin. */
/* > */
/* >            ORGATI = .true.    origin at i */
/* >            ORGATI = .false.   origin at i+1 */
/* > */
/* >   Logical variable SWTCH3 (switch-for-3-poles?) is for noting */
/* >   if we are working with THREE poles! */
/* > */
/* >   MAXIT is the maximum number of iterations allowed for each */
/* >   eigenvalue. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup auxOTHERcomputational */

/* > \par Contributors: */
/*  ================== */
/* > */
/* >     Ren-Cang Li, Computer Science Division, University of California */
/* >     at Berkeley, USA */
/* > */
/*  ===================================================================== */
/* Subroutine */ void dlaed4_(integer *n, integer *i__, doublereal *d__, 
	doublereal *z__, doublereal *delta, doublereal *rho, doublereal *dlam,
	 integer *info)
{
    /* System generated locals */
    integer i__1;
    doublereal d__1;

    /* Local variables */
    doublereal dphi, dpsi;
    integer iter;
    doublereal temp, prew, temp1, a, b, c__;
    integer j;
    doublereal w, dltlb, dltub, midpt;
    integer niter;
    logical swtch;
    extern /* Subroutine */ void dlaed5_(integer *, doublereal *, doublereal *,
	     doublereal *, doublereal *, doublereal *), dlaed6_(integer *, 
	    logical *, doublereal *, doublereal *, doublereal *, doublereal *,
	     doublereal *, integer *);
    logical swtch3;
    integer ii;
    extern doublereal dlamch_(char *);
    doublereal dw, zz[3];
    logical orgati;
    doublereal erretm, rhoinv;
    integer ip1;
    doublereal del, eta, phi, eps, tau, psi;
    integer iim1, iip1;


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


/*     Since this routine is called in an inner loop, we do no argument */
/*     checking. */

/*     Quick return for N=1 and 2. */

    /* Parameter adjustments */
    --delta;
    --z__;
    --d__;

    /* Function Body */
    *info = 0;
    if (*n == 1) {

/*         Presumably, I=1 upon entry */

	*dlam = d__[1] + *rho * z__[1] * z__[1];
	delta[1] = 1.;
	return;
    }
    if (*n == 2) {
	dlaed5_(i__, &d__[1], &z__[1], &delta[1], rho, dlam);
	return;
    }

/*     Compute machine epsilon */

    eps = dlamch_("Epsilon");
    rhoinv = 1. / *rho;

/*     The case I = N */

    if (*i__ == *n) {

/*        Initialize some basic variables */

	ii = *n - 1;
	niter = 1;

/*        Calculate initial guess */

	midpt = *rho / 2.;

/*        If ||Z||_2 is not one, then TEMP should be set to */
/*        RHO * ||Z||_2^2 / TWO */

	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    delta[j] = d__[j] - d__[*i__] - midpt;
/* L10: */
	}

	psi = 0.;
	i__1 = *n - 2;
	for (j = 1; j <= i__1; ++j) {
	    psi += z__[j] * z__[j] / delta[j];
/* L20: */
	}

	c__ = rhoinv + psi;
	w = c__ + z__[ii] * z__[ii] / delta[ii] + z__[*n] * z__[*n] / delta[*
		n];

	if (w <= 0.) {
	    temp = z__[*n - 1] * z__[*n - 1] / (d__[*n] - d__[*n - 1] + *rho) 
		    + z__[*n] * z__[*n] / *rho;
	    if (c__ <= temp) {
		tau = *rho;
	    } else {
		del = d__[*n] - d__[*n - 1];
		a = -c__ * del + z__[*n - 1] * z__[*n - 1] + z__[*n] * z__[*n]
			;
		b = z__[*n] * z__[*n] * del;
		if (a < 0.) {
		    tau = b * 2. / (sqrt(a * a + b * 4. * c__) - a);
		} else {
		    tau = (a + sqrt(a * a + b * 4. * c__)) / (c__ * 2.);
		}
	    }

/*           It can be proved that */
/*               D(N)+RHO/2 <= LAMBDA(N) < D(N)+TAU <= D(N)+RHO */

	    dltlb = midpt;
	    dltub = *rho;
	} else {
	    del = d__[*n] - d__[*n - 1];
	    a = -c__ * del + z__[*n - 1] * z__[*n - 1] + z__[*n] * z__[*n];
	    b = z__[*n] * z__[*n] * del;
	    if (a < 0.) {
		tau = b * 2. / (sqrt(a * a + b * 4. * c__) - a);
	    } else {
		tau = (a + sqrt(a * a + b * 4. * c__)) / (c__ * 2.);
	    }

/*           It can be proved that */
/*               D(N) < D(N)+TAU < LAMBDA(N) < D(N)+RHO/2 */

	    dltlb = 0.;
	    dltub = midpt;
	}

	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    delta[j] = d__[j] - d__[*i__] - tau;
/* L30: */
	}

/*        Evaluate PSI and the derivative DPSI */

	dpsi = 0.;
	psi = 0.;
	erretm = 0.;
	i__1 = ii;
	for (j = 1; j <= i__1; ++j) {
	    temp = z__[j] / delta[j];
	    psi += z__[j] * temp;
	    dpsi += temp * temp;
	    erretm += psi;
/* L40: */
	}
	erretm = abs(erretm);

/*        Evaluate PHI and the derivative DPHI */

	temp = z__[*n] / delta[*n];
	phi = z__[*n] * temp;
	dphi = temp * temp;
	erretm = (-phi - psi) * 8. + erretm - phi + rhoinv + abs(tau) * (dpsi 
		+ dphi);

	w = rhoinv + phi + psi;

/*        Test for convergence */

	if (abs(w) <= eps * erretm) {
	    *dlam = d__[*i__] + tau;
	    goto L250;
	}

	if (w <= 0.) {
	    dltlb = f2cmax(dltlb,tau);
	} else {
	    dltub = f2cmin(dltub,tau);
	}

/*        Calculate the new step */

	++niter;
	c__ = w - delta[*n - 1] * dpsi - delta[*n] * dphi;
	a = (delta[*n - 1] + delta[*n]) * w - delta[*n - 1] * delta[*n] * (
		dpsi + dphi);
	b = delta[*n - 1] * delta[*n] * w;
	if (c__ < 0.) {
	    c__ = abs(c__);
	}
	if (c__ == 0.) {
/*          ETA = B/A */
/*           ETA = RHO - TAU */
	    eta = dltub - tau;
	} else if (a >= 0.) {
	    eta = (a + sqrt((d__1 = a * a - b * 4. * c__, abs(d__1)))) / (c__ 
		    * 2.);
	} else {
	    eta = b * 2. / (a - sqrt((d__1 = a * a - b * 4. * c__, abs(d__1)))
		    );
	}

/*        Note, eta should be positive if w is negative, and */
/*        eta should be negative otherwise. However, */
/*        if for some reason caused by roundoff, eta*w > 0, */
/*        we simply use one Newton step instead. This way */
/*        will guarantee eta*w < 0. */

	if (w * eta > 0.) {
	    eta = -w / (dpsi + dphi);
	}
	temp = tau + eta;
	if (temp > dltub || temp < dltlb) {
	    if (w < 0.) {
		eta = (dltub - tau) / 2.;
	    } else {
		eta = (dltlb - tau) / 2.;
	    }
	}
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    delta[j] -= eta;
/* L50: */
	}

	tau += eta;

/*        Evaluate PSI and the derivative DPSI */

	dpsi = 0.;
	psi = 0.;
	erretm = 0.;
	i__1 = ii;
	for (j = 1; j <= i__1; ++j) {
	    temp = z__[j] / delta[j];
	    psi += z__[j] * temp;
	    dpsi += temp * temp;
	    erretm += psi;
/* L60: */
	}
	erretm = abs(erretm);

/*        Evaluate PHI and the derivative DPHI */

	temp = z__[*n] / delta[*n];
	phi = z__[*n] * temp;
	dphi = temp * temp;
	erretm = (-phi - psi) * 8. + erretm - phi + rhoinv + abs(tau) * (dpsi 
		+ dphi);

	w = rhoinv + phi + psi;

/*        Main loop to update the values of the array   DELTA */

	iter = niter + 1;

	for (niter = iter; niter <= 30; ++niter) {

/*           Test for convergence */

	    if (abs(w) <= eps * erretm) {
		*dlam = d__[*i__] + tau;
		goto L250;
	    }

	    if (w <= 0.) {
		dltlb = f2cmax(dltlb,tau);
	    } else {
		dltub = f2cmin(dltub,tau);
	    }

/*           Calculate the new step */

	    c__ = w - delta[*n - 1] * dpsi - delta[*n] * dphi;
	    a = (delta[*n - 1] + delta[*n]) * w - delta[*n - 1] * delta[*n] * 
		    (dpsi + dphi);
	    b = delta[*n - 1] * delta[*n] * w;
	    if (a >= 0.) {
		eta = (a + sqrt((d__1 = a * a - b * 4. * c__, abs(d__1)))) / (
			c__ * 2.);
	    } else {
		eta = b * 2. / (a - sqrt((d__1 = a * a - b * 4. * c__, abs(
			d__1))));
	    }

/*           Note, eta should be positive if w is negative, and */
/*           eta should be negative otherwise. However, */
/*           if for some reason caused by roundoff, eta*w > 0, */
/*           we simply use one Newton step instead. This way */
/*           will guarantee eta*w < 0. */

	    if (w * eta > 0.) {
		eta = -w / (dpsi + dphi);
	    }
	    temp = tau + eta;
	    if (temp > dltub || temp < dltlb) {
		if (w < 0.) {
		    eta = (dltub - tau) / 2.;
		} else {
		    eta = (dltlb - tau) / 2.;
		}
	    }
	    i__1 = *n;
	    for (j = 1; j <= i__1; ++j) {
		delta[j] -= eta;
/* L70: */
	    }

	    tau += eta;

/*           Evaluate PSI and the derivative DPSI */

	    dpsi = 0.;
	    psi = 0.;
	    erretm = 0.;
	    i__1 = ii;
	    for (j = 1; j <= i__1; ++j) {
		temp = z__[j] / delta[j];
		psi += z__[j] * temp;
		dpsi += temp * temp;
		erretm += psi;
/* L80: */
	    }
	    erretm = abs(erretm);

/*           Evaluate PHI and the derivative DPHI */

	    temp = z__[*n] / delta[*n];
	    phi = z__[*n] * temp;
	    dphi = temp * temp;
	    erretm = (-phi - psi) * 8. + erretm - phi + rhoinv + abs(tau) * (
		    dpsi + dphi);

	    w = rhoinv + phi + psi;
/* L90: */
	}

/*        Return with INFO = 1, NITER = MAXIT and not converged */

	*info = 1;
	*dlam = d__[*i__] + tau;
	goto L250;

/*        End for the case I = N */

    } else {

/*        The case for I < N */

	niter = 1;
	ip1 = *i__ + 1;

/*        Calculate initial guess */

	del = d__[ip1] - d__[*i__];
	midpt = del / 2.;
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    delta[j] = d__[j] - d__[*i__] - midpt;
/* L100: */
	}

	psi = 0.;
	i__1 = *i__ - 1;
	for (j = 1; j <= i__1; ++j) {
	    psi += z__[j] * z__[j] / delta[j];
/* L110: */
	}

	phi = 0.;
	i__1 = *i__ + 2;
	for (j = *n; j >= i__1; --j) {
	    phi += z__[j] * z__[j] / delta[j];
/* L120: */
	}
	c__ = rhoinv + psi + phi;
	w = c__ + z__[*i__] * z__[*i__] / delta[*i__] + z__[ip1] * z__[ip1] / 
		delta[ip1];

	if (w > 0.) {

/*           d(i)< the ith eigenvalue < (d(i)+d(i+1))/2 */

/*           We choose d(i) as origin. */

	    orgati = TRUE_;
	    a = c__ * del + z__[*i__] * z__[*i__] + z__[ip1] * z__[ip1];
	    b = z__[*i__] * z__[*i__] * del;
	    if (a > 0.) {
		tau = b * 2. / (a + sqrt((d__1 = a * a - b * 4. * c__, abs(
			d__1))));
	    } else {
		tau = (a - sqrt((d__1 = a * a - b * 4. * c__, abs(d__1)))) / (
			c__ * 2.);
	    }
	    dltlb = 0.;
	    dltub = midpt;
	} else {

/*           (d(i)+d(i+1))/2 <= the ith eigenvalue < d(i+1) */

/*           We choose d(i+1) as origin. */

	    orgati = FALSE_;
	    a = c__ * del - z__[*i__] * z__[*i__] - z__[ip1] * z__[ip1];
	    b = z__[ip1] * z__[ip1] * del;
	    if (a < 0.) {
		tau = b * 2. / (a - sqrt((d__1 = a * a + b * 4. * c__, abs(
			d__1))));
	    } else {
		tau = -(a + sqrt((d__1 = a * a + b * 4. * c__, abs(d__1)))) / 
			(c__ * 2.);
	    }
	    dltlb = -midpt;
	    dltub = 0.;
	}

	if (orgati) {
	    i__1 = *n;
	    for (j = 1; j <= i__1; ++j) {
		delta[j] = d__[j] - d__[*i__] - tau;
/* L130: */
	    }
	} else {
	    i__1 = *n;
	    for (j = 1; j <= i__1; ++j) {
		delta[j] = d__[j] - d__[ip1] - tau;
/* L140: */
	    }
	}
	if (orgati) {
	    ii = *i__;
	} else {
	    ii = *i__ + 1;
	}
	iim1 = ii - 1;
	iip1 = ii + 1;

/*        Evaluate PSI and the derivative DPSI */

	dpsi = 0.;
	psi = 0.;
	erretm = 0.;
	i__1 = iim1;
	for (j = 1; j <= i__1; ++j) {
	    temp = z__[j] / delta[j];
	    psi += z__[j] * temp;
	    dpsi += temp * temp;
	    erretm += psi;
/* L150: */
	}
	erretm = abs(erretm);

/*        Evaluate PHI and the derivative DPHI */

	dphi = 0.;
	phi = 0.;
	i__1 = iip1;
	for (j = *n; j >= i__1; --j) {
	    temp = z__[j] / delta[j];
	    phi += z__[j] * temp;
	    dphi += temp * temp;
	    erretm += phi;
/* L160: */
	}

	w = rhoinv + phi + psi;

/*        W is the value of the secular function with */
/*        its ii-th element removed. */

	swtch3 = FALSE_;
	if (orgati) {
	    if (w < 0.) {
		swtch3 = TRUE_;
	    }
	} else {
	    if (w > 0.) {
		swtch3 = TRUE_;
	    }
	}
	if (ii == 1 || ii == *n) {
	    swtch3 = FALSE_;
	}

	temp = z__[ii] / delta[ii];
	dw = dpsi + dphi + temp * temp;
	temp = z__[ii] * temp;
	w += temp;
	erretm = (phi - psi) * 8. + erretm + rhoinv * 2. + abs(temp) * 3. + 
		abs(tau) * dw;

/*        Test for convergence */

	if (abs(w) <= eps * erretm) {
	    if (orgati) {
		*dlam = d__[*i__] + tau;
	    } else {
		*dlam = d__[ip1] + tau;
	    }
	    goto L250;
	}

	if (w <= 0.) {
	    dltlb = f2cmax(dltlb,tau);
	} else {
	    dltub = f2cmin(dltub,tau);
	}

/*        Calculate the new step */

	++niter;
	if (! swtch3) {
	    if (orgati) {
/* Computing 2nd power */
		d__1 = z__[*i__] / delta[*i__];
		c__ = w - delta[ip1] * dw - (d__[*i__] - d__[ip1]) * (d__1 * 
			d__1);
	    } else {
/* Computing 2nd power */
		d__1 = z__[ip1] / delta[ip1];
		c__ = w - delta[*i__] * dw - (d__[ip1] - d__[*i__]) * (d__1 * 
			d__1);
	    }
	    a = (delta[*i__] + delta[ip1]) * w - delta[*i__] * delta[ip1] * 
		    dw;
	    b = delta[*i__] * delta[ip1] * w;
	    if (c__ == 0.) {
		if (a == 0.) {
		    if (orgati) {
			a = z__[*i__] * z__[*i__] + delta[ip1] * delta[ip1] * 
				(dpsi + dphi);
		    } else {
			a = z__[ip1] * z__[ip1] + delta[*i__] * delta[*i__] * 
				(dpsi + dphi);
		    }
		}
		eta = b / a;
	    } else if (a <= 0.) {
		eta = (a - sqrt((d__1 = a * a - b * 4. * c__, abs(d__1)))) / (
			c__ * 2.);
	    } else {
		eta = b * 2. / (a + sqrt((d__1 = a * a - b * 4. * c__, abs(
			d__1))));
	    }
	} else {

/*           Interpolation using THREE most relevant poles */

	    temp = rhoinv + psi + phi;
	    if (orgati) {
		temp1 = z__[iim1] / delta[iim1];
		temp1 *= temp1;
		c__ = temp - delta[iip1] * (dpsi + dphi) - (d__[iim1] - d__[
			iip1]) * temp1;
		zz[0] = z__[iim1] * z__[iim1];
		zz[2] = delta[iip1] * delta[iip1] * (dpsi - temp1 + dphi);
	    } else {
		temp1 = z__[iip1] / delta[iip1];
		temp1 *= temp1;
		c__ = temp - delta[iim1] * (dpsi + dphi) - (d__[iip1] - d__[
			iim1]) * temp1;
		zz[0] = delta[iim1] * delta[iim1] * (dpsi + (dphi - temp1));
		zz[2] = z__[iip1] * z__[iip1];
	    }
	    zz[1] = z__[ii] * z__[ii];
	    dlaed6_(&niter, &orgati, &c__, &delta[iim1], zz, &w, &eta, info);
	    if (*info != 0) {
		goto L250;
	    }
	}

/*        Note, eta should be positive if w is negative, and */
/*        eta should be negative otherwise. However, */
/*        if for some reason caused by roundoff, eta*w > 0, */
/*        we simply use one Newton step instead. This way */
/*        will guarantee eta*w < 0. */

	if (w * eta >= 0.) {
	    eta = -w / dw;
	}
	temp = tau + eta;
	if (temp > dltub || temp < dltlb) {
	    if (w < 0.) {
		eta = (dltub - tau) / 2.;
	    } else {
		eta = (dltlb - tau) / 2.;
	    }
	}

	prew = w;

	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    delta[j] -= eta;
/* L180: */
	}

/*        Evaluate PSI and the derivative DPSI */

	dpsi = 0.;
	psi = 0.;
	erretm = 0.;
	i__1 = iim1;
	for (j = 1; j <= i__1; ++j) {
	    temp = z__[j] / delta[j];
	    psi += z__[j] * temp;
	    dpsi += temp * temp;
	    erretm += psi;
/* L190: */
	}
	erretm = abs(erretm);

/*        Evaluate PHI and the derivative DPHI */

	dphi = 0.;
	phi = 0.;
	i__1 = iip1;
	for (j = *n; j >= i__1; --j) {
	    temp = z__[j] / delta[j];
	    phi += z__[j] * temp;
	    dphi += temp * temp;
	    erretm += phi;
/* L200: */
	}

	temp = z__[ii] / delta[ii];
	dw = dpsi + dphi + temp * temp;
	temp = z__[ii] * temp;
	w = rhoinv + phi + psi + temp;
	erretm = (phi - psi) * 8. + erretm + rhoinv * 2. + abs(temp) * 3. + (
		d__1 = tau + eta, abs(d__1)) * dw;

	swtch = FALSE_;
	if (orgati) {
	    if (-w > abs(prew) / 10.) {
		swtch = TRUE_;
	    }
	} else {
	    if (w > abs(prew) / 10.) {
		swtch = TRUE_;
	    }
	}

	tau += eta;

/*        Main loop to update the values of the array   DELTA */

	iter = niter + 1;

	for (niter = iter; niter <= 30; ++niter) {

/*           Test for convergence */

	    if (abs(w) <= eps * erretm) {
		if (orgati) {
		    *dlam = d__[*i__] + tau;
		} else {
		    *dlam = d__[ip1] + tau;
		}
		goto L250;
	    }

	    if (w <= 0.) {
		dltlb = f2cmax(dltlb,tau);
	    } else {
		dltub = f2cmin(dltub,tau);
	    }

/*           Calculate the new step */

	    if (! swtch3) {
		if (! swtch) {
		    if (orgati) {
/* Computing 2nd power */
			d__1 = z__[*i__] / delta[*i__];
			c__ = w - delta[ip1] * dw - (d__[*i__] - d__[ip1]) * (
				d__1 * d__1);
		    } else {
/* Computing 2nd power */
			d__1 = z__[ip1] / delta[ip1];
			c__ = w - delta[*i__] * dw - (d__[ip1] - d__[*i__]) * 
				(d__1 * d__1);
		    }
		} else {
		    temp = z__[ii] / delta[ii];
		    if (orgati) {
			dpsi += temp * temp;
		    } else {
			dphi += temp * temp;
		    }
		    c__ = w - delta[*i__] * dpsi - delta[ip1] * dphi;
		}
		a = (delta[*i__] + delta[ip1]) * w - delta[*i__] * delta[ip1] 
			* dw;
		b = delta[*i__] * delta[ip1] * w;
		if (c__ == 0.) {
		    if (a == 0.) {
			if (! swtch) {
			    if (orgati) {
				a = z__[*i__] * z__[*i__] + delta[ip1] * 
					delta[ip1] * (dpsi + dphi);
			    } else {
				a = z__[ip1] * z__[ip1] + delta[*i__] * delta[
					*i__] * (dpsi + dphi);
			    }
			} else {
			    a = delta[*i__] * delta[*i__] * dpsi + delta[ip1] 
				    * delta[ip1] * dphi;
			}
		    }
		    eta = b / a;
		} else if (a <= 0.) {
		    eta = (a - sqrt((d__1 = a * a - b * 4. * c__, abs(d__1))))
			     / (c__ * 2.);
		} else {
		    eta = b * 2. / (a + sqrt((d__1 = a * a - b * 4. * c__, 
			    abs(d__1))));
		}
	    } else {

/*              Interpolation using THREE most relevant poles */

		temp = rhoinv + psi + phi;
		if (swtch) {
		    c__ = temp - delta[iim1] * dpsi - delta[iip1] * dphi;
		    zz[0] = delta[iim1] * delta[iim1] * dpsi;
		    zz[2] = delta[iip1] * delta[iip1] * dphi;
		} else {
		    if (orgati) {
			temp1 = z__[iim1] / delta[iim1];
			temp1 *= temp1;
			c__ = temp - delta[iip1] * (dpsi + dphi) - (d__[iim1] 
				- d__[iip1]) * temp1;
			zz[0] = z__[iim1] * z__[iim1];
			zz[2] = delta[iip1] * delta[iip1] * (dpsi - temp1 + 
				dphi);
		    } else {
			temp1 = z__[iip1] / delta[iip1];
			temp1 *= temp1;
			c__ = temp - delta[iim1] * (dpsi + dphi) - (d__[iip1] 
				- d__[iim1]) * temp1;
			zz[0] = delta[iim1] * delta[iim1] * (dpsi + (dphi - 
				temp1));
			zz[2] = z__[iip1] * z__[iip1];
		    }
		}
		dlaed6_(&niter, &orgati, &c__, &delta[iim1], zz, &w, &eta, 
			info);
		if (*info != 0) {
		    goto L250;
		}
	    }

/*           Note, eta should be positive if w is negative, and */
/*           eta should be negative otherwise. However, */
/*           if for some reason caused by roundoff, eta*w > 0, */
/*           we simply use one Newton step instead. This way */
/*           will guarantee eta*w < 0. */

	    if (w * eta >= 0.) {
		eta = -w / dw;
	    }
	    temp = tau + eta;
	    if (temp > dltub || temp < dltlb) {
		if (w < 0.) {
		    eta = (dltub - tau) / 2.;
		} else {
		    eta = (dltlb - tau) / 2.;
		}
	    }

	    i__1 = *n;
	    for (j = 1; j <= i__1; ++j) {
		delta[j] -= eta;
/* L210: */
	    }

	    tau += eta;
	    prew = w;

/*           Evaluate PSI and the derivative DPSI */

	    dpsi = 0.;
	    psi = 0.;
	    erretm = 0.;
	    i__1 = iim1;
	    for (j = 1; j <= i__1; ++j) {
		temp = z__[j] / delta[j];
		psi += z__[j] * temp;
		dpsi += temp * temp;
		erretm += psi;
/* L220: */
	    }
	    erretm = abs(erretm);

/*           Evaluate PHI and the derivative DPHI */

	    dphi = 0.;
	    phi = 0.;
	    i__1 = iip1;
	    for (j = *n; j >= i__1; --j) {
		temp = z__[j] / delta[j];
		phi += z__[j] * temp;
		dphi += temp * temp;
		erretm += phi;
/* L230: */
	    }

	    temp = z__[ii] / delta[ii];
	    dw = dpsi + dphi + temp * temp;
	    temp = z__[ii] * temp;
	    w = rhoinv + phi + psi + temp;
	    erretm = (phi - psi) * 8. + erretm + rhoinv * 2. + abs(temp) * 3. 
		    + abs(tau) * dw;
	    if (w * prew > 0. && abs(w) > abs(prew) / 10.) {
		swtch = ! swtch;
	    }

/* L240: */
	}

/*        Return with INFO = 1, NITER = MAXIT and not converged */

	*info = 1;
	if (orgati) {
	    *dlam = d__[*i__] + tau;
	} else {
	    *dlam = d__[ip1] + tau;
	}

    }

L250:

    return;

/*     End of DLAED4 */

} /* dlaed4_ */

