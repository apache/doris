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
static integer c__2 = 2;
static integer c__10 = 10;
static integer c__3 = 3;
static integer c__4 = 4;
static integer c__11 = 11;

/* > \brief \b DLASQ2 computes all the eigenvalues of the symmetric positive definite tridiagonal matrix assoc
iated with the qd Array Z to high relative accuracy. Used by sbdsqr and sstegr. */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download DLASQ2 + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dlasq2.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dlasq2.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dlasq2.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE DLASQ2( N, Z, INFO ) */

/*       INTEGER            INFO, N */
/*       DOUBLE PRECISION   Z( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > DLASQ2 computes all the eigenvalues of the symmetric positive */
/* > definite tridiagonal matrix associated with the qd array Z to high */
/* > relative accuracy are computed to high relative accuracy, in the */
/* > absence of denormalization, underflow and overflow. */
/* > */
/* > To see the relation of Z to the tridiagonal matrix, let L be a */
/* > unit lower bidiagonal matrix with subdiagonals Z(2,4,6,,..) and */
/* > let U be an upper bidiagonal matrix with 1's above and diagonal */
/* > Z(1,3,5,,..). The tridiagonal is L*U or, if you prefer, the */
/* > symmetric tridiagonal to which it is similar. */
/* > */
/* > Note : DLASQ2 defines a logical variable, IEEE, which is true */
/* > on machines which follow ieee-754 floating-point standard in their */
/* > handling of infinities and NaNs, and false otherwise. This variable */
/* > is passed to DLASQ3. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >        The number of rows and columns in the matrix. N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] Z */
/* > \verbatim */
/* >          Z is DOUBLE PRECISION array, dimension ( 4*N ) */
/* >        On entry Z holds the qd array. On exit, entries 1 to N hold */
/* >        the eigenvalues in decreasing order, Z( 2*N+1 ) holds the */
/* >        trace, and Z( 2*N+2 ) holds the sum of the eigenvalues. If */
/* >        N > 2, then Z( 2*N+3 ) holds the iteration count, Z( 2*N+4 ) */
/* >        holds NDIVS/NIN^2, and Z( 2*N+5 ) holds the percentage of */
/* >        shifts that failed. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >        = 0: successful exit */
/* >        < 0: if the i-th argument is a scalar and had an illegal */
/* >             value, then INFO = -i, if the i-th argument is an */
/* >             array and the j-entry had an illegal value, then */
/* >             INFO = -(i*100+j) */
/* >        > 0: the algorithm failed */
/* >              = 1, a split was marked by a positive value in E */
/* >              = 2, current block of Z not diagonalized after 100*N */
/* >                   iterations (in inner while loop).  On exit Z holds */
/* >                   a qd array with the same eigenvalues as the given Z. */
/* >              = 3, termination criterion of outer while loop not met */
/* >                   (program created more than N unreduced blocks) */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup auxOTHERcomputational */

/* > \par Further Details: */
/*  ===================== */
/* > */
/* > \verbatim */
/* > */
/* >  Local Variables: I0:N0 defines a current unreduced segment of Z. */
/* >  The shifts are accumulated in SIGMA. Iteration count is in ITER. */
/* >  Ping-pong is controlled by PP (alternates between 0 and 1). */
/* > \endverbatim */
/* > */
/*  ===================================================================== */
/* Subroutine */ void dlasq2_(integer *n, doublereal *z__, integer *info)
{
    /* System generated locals */
    integer i__1, i__2, i__3;
    doublereal d__1, d__2;

    /* Local variables */
    logical ieee;
    integer nbig;
    doublereal dmin__, emin, emax;
    integer kmin, ndiv, iter;
    doublereal qmin, temp, qmax, zmax;
    integer splt;
    doublereal dmin1, dmin2, d__, e, g;
    integer k;
    doublereal s, t;
    integer nfail;
    doublereal desig, trace, sigma;
    integer iinfo;
    doublereal tempe, tempq;
    integer i0, i1, i4, n0, n1, ttype;
    extern /* Subroutine */ void dlasq3_(integer *, integer *, doublereal *, 
	    integer *, doublereal *, doublereal *, doublereal *, doublereal *,
	     integer *, integer *, integer *, logical *, integer *, 
	    doublereal *, doublereal *, doublereal *, doublereal *, 
	    doublereal *, doublereal *, doublereal *);
    doublereal dn;
    extern doublereal dlamch_(char *);
    integer pp;
    doublereal deemin;
    integer iwhila, iwhilb;
    doublereal oldemn, safmin;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    extern integer ilaenv_(integer *, char *, char *, integer *, integer *, 
	    integer *, integer *, ftnlen, ftnlen);
    extern /* Subroutine */ void dlasrt_(char *, integer *, doublereal *, 
	    integer *);
    doublereal dn1, dn2, dee, eps, tau, tol;
    integer ipn4;
    doublereal tol2;


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


/*     Test the input arguments. */
/*     (in case DLASQ2 is not called by DLASQ1) */

    /* Parameter adjustments */
    --z__;

    /* Function Body */
    *info = 0;
    eps = dlamch_("Precision");
    safmin = dlamch_("Safe minimum");
    tol = eps * 100.;
/* Computing 2nd power */
    d__1 = tol;
    tol2 = d__1 * d__1;

    if (*n < 0) {
	*info = -1;
	xerbla_("DLASQ2", &c__1, (ftnlen)6);
	return;
    } else if (*n == 0) {
	return;
    } else if (*n == 1) {

/*        1-by-1 case. */

	if (z__[1] < 0.) {
	    *info = -201;
	    xerbla_("DLASQ2", &c__2, (ftnlen)6);
	}
	return;
    } else if (*n == 2) {

/*        2-by-2 case. */

	if (z__[1] < 0.) {
	    *info = -201;
	    xerbla_("DLASQ2", &c__2, (ftnlen)6);
	    return;
	} else if (z__[2] < 0.) {
	    *info = -202;
	    xerbla_("DLASQ2", &c__2, (ftnlen)6);
	    return;
	} else if (z__[3] < 0.) {
	    *info = -203;
	    xerbla_("DLASQ2", &c__2, (ftnlen)6);
	    return;
	} else if (z__[3] > z__[1]) {
	    d__ = z__[3];
	    z__[3] = z__[1];
	    z__[1] = d__;
	}
	z__[5] = z__[1] + z__[2] + z__[3];
	if (z__[2] > z__[3] * tol2) {
	    t = (z__[1] - z__[3] + z__[2]) * .5;
	    s = z__[3] * (z__[2] / t);
	    if (s <= t) {
		s = z__[3] * (z__[2] / (t * (sqrt(s / t + 1.) + 1.)));
	    } else {
		s = z__[3] * (z__[2] / (t + sqrt(t) * sqrt(t + s)));
	    }
	    t = z__[1] + (s + z__[2]);
	    z__[3] *= z__[1] / t;
	    z__[1] = t;
	}
	z__[2] = z__[3];
	z__[6] = z__[2] + z__[1];
	return;
    }

/*     Check for negative data and compute sums of q's and e's. */

    z__[*n * 2] = 0.;
    emin = z__[2];
    qmax = 0.;
    zmax = 0.;
    d__ = 0.;
    e = 0.;

    i__1 = *n - 1 << 1;
    for (k = 1; k <= i__1; k += 2) {
	if (z__[k] < 0.) {
	    *info = -(k + 200);
	    xerbla_("DLASQ2", &c__2, (ftnlen)6);
	    return;
	} else if (z__[k + 1] < 0.) {
	    *info = -(k + 201);
	    xerbla_("DLASQ2", &c__2, (ftnlen)6);
	    return;
	}
	d__ += z__[k];
	e += z__[k + 1];
/* Computing MAX */
	d__1 = qmax, d__2 = z__[k];
	qmax = f2cmax(d__1,d__2);
/* Computing MIN */
	d__1 = emin, d__2 = z__[k + 1];
	emin = f2cmin(d__1,d__2);
/* Computing MAX */
	d__1 = f2cmax(qmax,zmax), d__2 = z__[k + 1];
	zmax = f2cmax(d__1,d__2);
/* L10: */
    }
    if (z__[(*n << 1) - 1] < 0.) {
	*info = -((*n << 1) + 199);
	xerbla_("DLASQ2", &c__2, (ftnlen)6);
	return;
    }
    d__ += z__[(*n << 1) - 1];
/* Computing MAX */
    d__1 = qmax, d__2 = z__[(*n << 1) - 1];
    qmax = f2cmax(d__1,d__2);
    zmax = f2cmax(qmax,zmax);

/*     Check for diagonality. */

    if (e == 0.) {
	i__1 = *n;
	for (k = 2; k <= i__1; ++k) {
	    z__[k] = z__[(k << 1) - 1];
/* L20: */
	}
	dlasrt_("D", n, &z__[1], &iinfo);
	z__[(*n << 1) - 1] = d__;
	return;
    }

    trace = d__ + e;

/*     Check for zero data. */

    if (trace == 0.) {
	z__[(*n << 1) - 1] = 0.;
	return;
    }

/*     Check whether the machine is IEEE conformable. */

    ieee = ilaenv_(&c__10, "DLASQ2", "N", &c__1, &c__2, &c__3, &c__4, (ftnlen)
	    6, (ftnlen)1) == 1 && ilaenv_(&c__11, "DLASQ2", "N", &c__1, &c__2,
	     &c__3, &c__4, (ftnlen)6, (ftnlen)1) == 1;

/*     Rearrange data for locality: Z=(q1,qq1,e1,ee1,q2,qq2,e2,ee2,...). */

    for (k = *n << 1; k >= 2; k += -2) {
	z__[k * 2] = 0.;
	z__[(k << 1) - 1] = z__[k];
	z__[(k << 1) - 2] = 0.;
	z__[(k << 1) - 3] = z__[k - 1];
/* L30: */
    }

    i0 = 1;
    n0 = *n;

/*     Reverse the qd-array, if warranted. */

    if (z__[(i0 << 2) - 3] * 1.5 < z__[(n0 << 2) - 3]) {
	ipn4 = i0 + n0 << 2;
	i__1 = i0 + n0 - 1 << 1;
	for (i4 = i0 << 2; i4 <= i__1; i4 += 4) {
	    temp = z__[i4 - 3];
	    z__[i4 - 3] = z__[ipn4 - i4 - 3];
	    z__[ipn4 - i4 - 3] = temp;
	    temp = z__[i4 - 1];
	    z__[i4 - 1] = z__[ipn4 - i4 - 5];
	    z__[ipn4 - i4 - 5] = temp;
/* L40: */
	}
    }

/*     Initial split checking via dqd and Li's test. */

    pp = 0;

    for (k = 1; k <= 2; ++k) {

	d__ = z__[(n0 << 2) + pp - 3];
	i__1 = (i0 << 2) + pp;
	for (i4 = (n0 - 1 << 2) + pp; i4 >= i__1; i4 += -4) {
	    if (z__[i4 - 1] <= tol2 * d__) {
		z__[i4 - 1] = 0.;
		d__ = z__[i4 - 3];
	    } else {
		d__ = z__[i4 - 3] * (d__ / (d__ + z__[i4 - 1]));
	    }
/* L50: */
	}

/*        dqd maps Z to ZZ plus Li's test. */

	emin = z__[(i0 << 2) + pp + 1];
	d__ = z__[(i0 << 2) + pp - 3];
	i__1 = (n0 - 1 << 2) + pp;
	for (i4 = (i0 << 2) + pp; i4 <= i__1; i4 += 4) {
	    z__[i4 - (pp << 1) - 2] = d__ + z__[i4 - 1];
	    if (z__[i4 - 1] <= tol2 * d__) {
		z__[i4 - 1] = 0.;
		z__[i4 - (pp << 1) - 2] = d__;
		z__[i4 - (pp << 1)] = 0.;
		d__ = z__[i4 + 1];
	    } else if (safmin * z__[i4 + 1] < z__[i4 - (pp << 1) - 2] && 
		    safmin * z__[i4 - (pp << 1) - 2] < z__[i4 + 1]) {
		temp = z__[i4 + 1] / z__[i4 - (pp << 1) - 2];
		z__[i4 - (pp << 1)] = z__[i4 - 1] * temp;
		d__ *= temp;
	    } else {
		z__[i4 - (pp << 1)] = z__[i4 + 1] * (z__[i4 - 1] / z__[i4 - (
			pp << 1) - 2]);
		d__ = z__[i4 + 1] * (d__ / z__[i4 - (pp << 1) - 2]);
	    }
/* Computing MIN */
	    d__1 = emin, d__2 = z__[i4 - (pp << 1)];
	    emin = f2cmin(d__1,d__2);
/* L60: */
	}
	z__[(n0 << 2) - pp - 2] = d__;

/*        Now find qmax. */

	qmax = z__[(i0 << 2) - pp - 2];
	i__1 = (n0 << 2) - pp - 2;
	for (i4 = (i0 << 2) - pp + 2; i4 <= i__1; i4 += 4) {
/* Computing MAX */
	    d__1 = qmax, d__2 = z__[i4];
	    qmax = f2cmax(d__1,d__2);
/* L70: */
	}

/*        Prepare for the next iteration on K. */

	pp = 1 - pp;
/* L80: */
    }

/*     Initialise variables to pass to DLASQ3. */

    ttype = 0;
    dmin1 = 0.;
    dmin2 = 0.;
    dn = 0.;
    dn1 = 0.;
    dn2 = 0.;
    g = 0.;
    tau = 0.;

    iter = 2;
    nfail = 0;
    ndiv = n0 - i0 << 1;

    i__1 = *n + 1;
    for (iwhila = 1; iwhila <= i__1; ++iwhila) {
	if (n0 < 1) {
	    goto L170;
	}

/*        While array unfinished do */

/*        E(N0) holds the value of SIGMA when submatrix in I0:N0 */
/*        splits from the rest of the array, but is negated. */

	desig = 0.;
	if (n0 == *n) {
	    sigma = 0.;
	} else {
	    sigma = -z__[(n0 << 2) - 1];
	}
	if (sigma < 0.) {
	    *info = 1;
	    return;
	}

/*        Find last unreduced submatrix's top index I0, find QMAX and */
/*        EMIN. Find Gershgorin-type bound if Q's much greater than E's. */

	emax = 0.;
	if (n0 > i0) {
	    emin = (d__1 = z__[(n0 << 2) - 5], abs(d__1));
	} else {
	    emin = 0.;
	}
	qmin = z__[(n0 << 2) - 3];
	qmax = qmin;
	for (i4 = n0 << 2; i4 >= 8; i4 += -4) {
	    if (z__[i4 - 5] <= 0.) {
		goto L100;
	    }
	    if (qmin >= emax * 4.) {
/* Computing MIN */
		d__1 = qmin, d__2 = z__[i4 - 3];
		qmin = f2cmin(d__1,d__2);
/* Computing MAX */
		d__1 = emax, d__2 = z__[i4 - 5];
		emax = f2cmax(d__1,d__2);
	    }
/* Computing MAX */
	    d__1 = qmax, d__2 = z__[i4 - 7] + z__[i4 - 5];
	    qmax = f2cmax(d__1,d__2);
/* Computing MIN */
	    d__1 = emin, d__2 = z__[i4 - 5];
	    emin = f2cmin(d__1,d__2);
/* L90: */
	}
	i4 = 4;

L100:
	i0 = i4 / 4;
	pp = 0;

	if (n0 - i0 > 1) {
	    dee = z__[(i0 << 2) - 3];
	    deemin = dee;
	    kmin = i0;
	    i__2 = (n0 << 2) - 3;
	    for (i4 = (i0 << 2) + 1; i4 <= i__2; i4 += 4) {
		dee = z__[i4] * (dee / (dee + z__[i4 - 2]));
		if (dee <= deemin) {
		    deemin = dee;
		    kmin = (i4 + 3) / 4;
		}
/* L110: */
	    }
	    if (kmin - i0 << 1 < n0 - kmin && deemin <= z__[(n0 << 2) - 3] * 
		    .5) {
		ipn4 = i0 + n0 << 2;
		pp = 2;
		i__2 = i0 + n0 - 1 << 1;
		for (i4 = i0 << 2; i4 <= i__2; i4 += 4) {
		    temp = z__[i4 - 3];
		    z__[i4 - 3] = z__[ipn4 - i4 - 3];
		    z__[ipn4 - i4 - 3] = temp;
		    temp = z__[i4 - 2];
		    z__[i4 - 2] = z__[ipn4 - i4 - 2];
		    z__[ipn4 - i4 - 2] = temp;
		    temp = z__[i4 - 1];
		    z__[i4 - 1] = z__[ipn4 - i4 - 5];
		    z__[ipn4 - i4 - 5] = temp;
		    temp = z__[i4];
		    z__[i4] = z__[ipn4 - i4 - 4];
		    z__[ipn4 - i4 - 4] = temp;
/* L120: */
		}
	    }
	}

/*        Put -(initial shift) into DMIN. */

/* Computing MAX */
	d__1 = 0., d__2 = qmin - sqrt(qmin) * 2. * sqrt(emax);
	dmin__ = -f2cmax(d__1,d__2);

/*        Now I0:N0 is unreduced. */
/*        PP = 0 for ping, PP = 1 for pong. */
/*        PP = 2 indicates that flipping was applied to the Z array and */
/*               and that the tests for deflation upon entry in DLASQ3 */
/*               should not be performed. */

	nbig = (n0 - i0 + 1) * 100;
	i__2 = nbig;
	for (iwhilb = 1; iwhilb <= i__2; ++iwhilb) {
	    if (i0 > n0) {
		goto L150;
	    }

/*           While submatrix unfinished take a good dqds step. */

	    dlasq3_(&i0, &n0, &z__[1], &pp, &dmin__, &sigma, &desig, &qmax, &
		    nfail, &iter, &ndiv, &ieee, &ttype, &dmin1, &dmin2, &dn, &
		    dn1, &dn2, &g, &tau);

	    pp = 1 - pp;

/*           When EMIN is very small check for splits. */

	    if (pp == 0 && n0 - i0 >= 3) {
		if (z__[n0 * 4] <= tol2 * qmax || z__[(n0 << 2) - 1] <= tol2 *
			 sigma) {
		    splt = i0 - 1;
		    qmax = z__[(i0 << 2) - 3];
		    emin = z__[(i0 << 2) - 1];
		    oldemn = z__[i0 * 4];
		    i__3 = n0 - 3 << 2;
		    for (i4 = i0 << 2; i4 <= i__3; i4 += 4) {
			if (z__[i4] <= tol2 * z__[i4 - 3] || z__[i4 - 1] <= 
				tol2 * sigma) {
			    z__[i4 - 1] = -sigma;
			    splt = i4 / 4;
			    qmax = 0.;
			    emin = z__[i4 + 3];
			    oldemn = z__[i4 + 4];
			} else {
/* Computing MAX */
			    d__1 = qmax, d__2 = z__[i4 + 1];
			    qmax = f2cmax(d__1,d__2);
/* Computing MIN */
			    d__1 = emin, d__2 = z__[i4 - 1];
			    emin = f2cmin(d__1,d__2);
/* Computing MIN */
			    d__1 = oldemn, d__2 = z__[i4];
			    oldemn = f2cmin(d__1,d__2);
			}
/* L130: */
		    }
		    z__[(n0 << 2) - 1] = emin;
		    z__[n0 * 4] = oldemn;
		    i0 = splt + 1;
		}
	    }

/* L140: */
	}

	*info = 2;

/*        Maximum number of iterations exceeded, restore the shift */
/*        SIGMA and place the new d's and e's in a qd array. */
/*        This might need to be done for several blocks */

	i1 = i0;
	n1 = n0;
L145:
	tempq = z__[(i0 << 2) - 3];
	z__[(i0 << 2) - 3] += sigma;
	i__2 = n0;
	for (k = i0 + 1; k <= i__2; ++k) {
	    tempe = z__[(k << 2) - 5];
	    z__[(k << 2) - 5] *= tempq / z__[(k << 2) - 7];
	    tempq = z__[(k << 2) - 3];
	    z__[(k << 2) - 3] = z__[(k << 2) - 3] + sigma + tempe - z__[(k << 
		    2) - 5];
	}

/*        Prepare to do this on the previous block if there is one */

	if (i1 > 1) {
	    n1 = i1 - 1;
	    while(i1 >= 2 && z__[(i1 << 2) - 5] >= 0.) {
		--i1;
	    }
	    sigma = -z__[(n1 << 2) - 1];
	    goto L145;
	}
	i__2 = *n;
	for (k = 1; k <= i__2; ++k) {
	    z__[(k << 1) - 1] = z__[(k << 2) - 3];

/*        Only the block 1..N0 is unfinished.  The rest of the e's */
/*        must be essentially zero, although sometimes other data */
/*        has been stored in them. */

	    if (k < n0) {
		z__[k * 2] = z__[(k << 2) - 1];
	    } else {
		z__[k * 2] = 0.;
	    }
	}
	return;

/*        end IWHILB */

L150:

/* L160: */
	;
    }

    *info = 3;
    return;

/*     end IWHILA */

L170:

/*     Move q's to the front. */

    i__1 = *n;
    for (k = 2; k <= i__1; ++k) {
	z__[k] = z__[(k << 2) - 3];
/* L180: */
    }

/*     Sort and compute sum of eigenvalues. */

    dlasrt_("D", n, &z__[1], &iinfo);

    e = 0.;
    for (k = *n; k >= 1; --k) {
	e += z__[k];
/* L190: */
    }

/*     Store trace, sum(eigenvalues) and information on performance. */

    z__[(*n << 1) + 1] = trace;
    z__[(*n << 1) + 2] = e;
    z__[(*n << 1) + 3] = (doublereal) iter;
/* Computing 2nd power */
    i__1 = *n;
    z__[(*n << 1) + 4] = (doublereal) ndiv / (doublereal) (i__1 * i__1);
    z__[(*n << 1) + 5] = nfail * 100. / (doublereal) iter;
    return;

/*     End of DLASQ2 */

} /* dlasq2_ */

