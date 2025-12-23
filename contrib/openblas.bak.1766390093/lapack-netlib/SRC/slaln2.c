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




/* > \brief \b SLALN2 solves a 1-by-1 or 2-by-2 linear system of equations of the specified form. */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download SLALN2 + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slaln2.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slaln2.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slaln2.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE SLALN2( LTRANS, NA, NW, SMIN, CA, A, LDA, D1, D2, B, */
/*                          LDB, WR, WI, X, LDX, SCALE, XNORM, INFO ) */

/*       LOGICAL            LTRANS */
/*       INTEGER            INFO, LDA, LDB, LDX, NA, NW */
/*       REAL               CA, D1, D2, SCALE, SMIN, WI, WR, XNORM */
/*       REAL               A( LDA, * ), B( LDB, * ), X( LDX, * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > SLALN2 solves a system of the form  (ca A - w D ) X = s B */
/* > or (ca A**T - w D) X = s B   with possible scaling ("s") and */
/* > perturbation of A.  (A**T means A-transpose.) */
/* > */
/* > A is an NA x NA real matrix, ca is a real scalar, D is an NA x NA */
/* > real diagonal matrix, w is a real or complex value, and X and B are */
/* > NA x 1 matrices -- real if w is real, complex if w is complex.  NA */
/* > may be 1 or 2. */
/* > */
/* > If w is complex, X and B are represented as NA x 2 matrices, */
/* > the first column of each being the real part and the second */
/* > being the imaginary part. */
/* > */
/* > "s" is a scaling factor (<= 1), computed by SLALN2, which is */
/* > so chosen that X can be computed without overflow.  X is further */
/* > scaled if necessary to assure that norm(ca A - w D)*norm(X) is less */
/* > than overflow. */
/* > */
/* > If both singular values of (ca A - w D) are less than SMIN, */
/* > SMIN*identity will be used instead of (ca A - w D).  If only one */
/* > singular value is less than SMIN, one element of (ca A - w D) will be */
/* > perturbed enough to make the smallest singular value roughly SMIN. */
/* > If both singular values are at least SMIN, (ca A - w D) will not be */
/* > perturbed.  In any case, the perturbation will be at most some small */
/* > multiple of f2cmax( SMIN, ulp*norm(ca A - w D) ).  The singular values */
/* > are computed by infinity-norm approximations, and thus will only be */
/* > correct to a factor of 2 or so. */
/* > */
/* > Note: all input quantities are assumed to be smaller than overflow */
/* > by a reasonable factor.  (See BIGNUM.) */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] LTRANS */
/* > \verbatim */
/* >          LTRANS is LOGICAL */
/* >          =.TRUE.:  A-transpose will be used. */
/* >          =.FALSE.: A will be used (not transposed.) */
/* > \endverbatim */
/* > */
/* > \param[in] NA */
/* > \verbatim */
/* >          NA is INTEGER */
/* >          The size of the matrix A.  It may (only) be 1 or 2. */
/* > \endverbatim */
/* > */
/* > \param[in] NW */
/* > \verbatim */
/* >          NW is INTEGER */
/* >          1 if "w" is real, 2 if "w" is complex.  It may only be 1 */
/* >          or 2. */
/* > \endverbatim */
/* > */
/* > \param[in] SMIN */
/* > \verbatim */
/* >          SMIN is REAL */
/* >          The desired lower bound on the singular values of A.  This */
/* >          should be a safe distance away from underflow or overflow, */
/* >          say, between (underflow/machine precision) and  (machine */
/* >          precision * overflow ).  (See BIGNUM and ULP.) */
/* > \endverbatim */
/* > */
/* > \param[in] CA */
/* > \verbatim */
/* >          CA is REAL */
/* >          The coefficient c, which A is multiplied by. */
/* > \endverbatim */
/* > */
/* > \param[in] A */
/* > \verbatim */
/* >          A is REAL array, dimension (LDA,NA) */
/* >          The NA x NA matrix A. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of A.  It must be at least NA. */
/* > \endverbatim */
/* > */
/* > \param[in] D1 */
/* > \verbatim */
/* >          D1 is REAL */
/* >          The 1,1 element in the diagonal matrix D. */
/* > \endverbatim */
/* > */
/* > \param[in] D2 */
/* > \verbatim */
/* >          D2 is REAL */
/* >          The 2,2 element in the diagonal matrix D.  Not used if NA=1. */
/* > \endverbatim */
/* > */
/* > \param[in] B */
/* > \verbatim */
/* >          B is REAL array, dimension (LDB,NW) */
/* >          The NA x NW matrix B (right-hand side).  If NW=2 ("w" is */
/* >          complex), column 1 contains the real part of B and column 2 */
/* >          contains the imaginary part. */
/* > \endverbatim */
/* > */
/* > \param[in] LDB */
/* > \verbatim */
/* >          LDB is INTEGER */
/* >          The leading dimension of B.  It must be at least NA. */
/* > \endverbatim */
/* > */
/* > \param[in] WR */
/* > \verbatim */
/* >          WR is REAL */
/* >          The real part of the scalar "w". */
/* > \endverbatim */
/* > */
/* > \param[in] WI */
/* > \verbatim */
/* >          WI is REAL */
/* >          The imaginary part of the scalar "w".  Not used if NW=1. */
/* > \endverbatim */
/* > */
/* > \param[out] X */
/* > \verbatim */
/* >          X is REAL array, dimension (LDX,NW) */
/* >          The NA x NW matrix X (unknowns), as computed by SLALN2. */
/* >          If NW=2 ("w" is complex), on exit, column 1 will contain */
/* >          the real part of X and column 2 will contain the imaginary */
/* >          part. */
/* > \endverbatim */
/* > */
/* > \param[in] LDX */
/* > \verbatim */
/* >          LDX is INTEGER */
/* >          The leading dimension of X.  It must be at least NA. */
/* > \endverbatim */
/* > */
/* > \param[out] SCALE */
/* > \verbatim */
/* >          SCALE is REAL */
/* >          The scale factor that B must be multiplied by to insure */
/* >          that overflow does not occur when computing X.  Thus, */
/* >          (ca A - w D) X  will be SCALE*B, not B (ignoring */
/* >          perturbations of A.)  It will be at most 1. */
/* > \endverbatim */
/* > */
/* > \param[out] XNORM */
/* > \verbatim */
/* >          XNORM is REAL */
/* >          The infinity-norm of X, when X is regarded as an NA x NW */
/* >          real matrix. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          An error flag.  It will be set to zero if no error occurs, */
/* >          a negative number if an argument is in error, or a positive */
/* >          number if  ca A - w D  had to be perturbed. */
/* >          The possible values are: */
/* >          = 0: No error occurred, and (ca A - w D) did not have to be */
/* >                 perturbed. */
/* >          = 1: (ca A - w D) had to be perturbed to make its smallest */
/* >               (or only) singular value greater than SMIN. */
/* >          NOTE: In the interests of speed, this routine does not */
/* >                check the inputs for errors. */
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
/* Subroutine */ void slaln2_(logical *ltrans, integer *na, integer *nw, real *
	smin, real *ca, real *a, integer *lda, real *d1, real *d2, real *b, 
	integer *ldb, real *wr, real *wi, real *x, integer *ldx, real *scale, 
	real *xnorm, integer *info)
{
    /* Initialized data */

    static logical cswap[4] = { FALSE_,FALSE_,TRUE_,TRUE_ };
    static logical rswap[4] = { FALSE_,TRUE_,FALSE_,TRUE_ };
    static integer ipivot[16]	/* was [4][4] */ = { 1,2,3,4,2,1,4,3,3,4,1,2,
	    4,3,2,1 };

    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, x_dim1, x_offset;
    real r__1, r__2, r__3, r__4, r__5, r__6;
    static real equiv_0[4], equiv_1[4];

    /* Local variables */
    real bbnd, cmax, ui11r, ui12s, temp, ur11r, ur12s;
    integer j;
    real u22abs;
    integer icmax;
    real bnorm, cnorm, smini;
#define ci (equiv_0)
#define cr (equiv_1)
    extern real slamch_(char *);
    real bignum;
    extern /* Subroutine */ void sladiv_(real *, real *, real *, real *, real *
	    , real *);
    real bi1, bi2, br1, br2, smlnum, xi1, xi2, xr1, xr2, ci21, ci22, cr21, 
	    cr22, li21, csi, ui11, lr21, ui12, ui22;
#define civ (equiv_0)
    real csr, ur11, ur12, ur22;
#define crv (equiv_1)


/*  -- LAPACK auxiliary routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/* ===================================================================== */

    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    b_dim1 = *ldb;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    x_dim1 = *ldx;
    x_offset = 1 + x_dim1 * 1;
    x -= x_offset;

    /* Function Body */

/*     Compute BIGNUM */

    smlnum = 2.f * slamch_("Safe minimum");
    bignum = 1.f / smlnum;
    smini = f2cmax(*smin,smlnum);

/*     Don't check for input errors */

    *info = 0;

/*     Standard Initializations */

    *scale = 1.f;

    if (*na == 1) {

/*        1 x 1  (i.e., scalar) system   C X = B */

	if (*nw == 1) {

/*           Real 1x1 system. */

/*           C = ca A - w D */

	    csr = *ca * a[a_dim1 + 1] - *wr * *d1;
	    cnorm = abs(csr);

/*           If | C | < SMINI, use C = SMINI */

	    if (cnorm < smini) {
		csr = smini;
		cnorm = smini;
		*info = 1;
	    }

/*           Check scaling for  X = B / C */

	    bnorm = (r__1 = b[b_dim1 + 1], abs(r__1));
	    if (cnorm < 1.f && bnorm > 1.f) {
		if (bnorm > bignum * cnorm) {
		    *scale = 1.f / bnorm;
		}
	    }

/*           Compute X */

	    x[x_dim1 + 1] = b[b_dim1 + 1] * *scale / csr;
	    *xnorm = (r__1 = x[x_dim1 + 1], abs(r__1));
	} else {

/*           Complex 1x1 system (w is complex) */

/*           C = ca A - w D */

	    csr = *ca * a[a_dim1 + 1] - *wr * *d1;
	    csi = -(*wi) * *d1;
	    cnorm = abs(csr) + abs(csi);

/*           If | C | < SMINI, use C = SMINI */

	    if (cnorm < smini) {
		csr = smini;
		csi = 0.f;
		cnorm = smini;
		*info = 1;
	    }

/*           Check scaling for  X = B / C */

	    bnorm = (r__1 = b[b_dim1 + 1], abs(r__1)) + (r__2 = b[(b_dim1 << 
		    1) + 1], abs(r__2));
	    if (cnorm < 1.f && bnorm > 1.f) {
		if (bnorm > bignum * cnorm) {
		    *scale = 1.f / bnorm;
		}
	    }

/*           Compute X */

	    r__1 = *scale * b[b_dim1 + 1];
	    r__2 = *scale * b[(b_dim1 << 1) + 1];
	    sladiv_(&r__1, &r__2, &csr, &csi, &x[x_dim1 + 1], &x[(x_dim1 << 1)
		     + 1]);
	    *xnorm = (r__1 = x[x_dim1 + 1], abs(r__1)) + (r__2 = x[(x_dim1 << 
		    1) + 1], abs(r__2));
	}

    } else {

/*        2x2 System */

/*        Compute the real part of  C = ca A - w D  (or  ca A**T - w D ) */

	cr[0] = *ca * a[a_dim1 + 1] - *wr * *d1;
	cr[3] = *ca * a[(a_dim1 << 1) + 2] - *wr * *d2;
	if (*ltrans) {
	    cr[2] = *ca * a[a_dim1 + 2];
	    cr[1] = *ca * a[(a_dim1 << 1) + 1];
	} else {
	    cr[1] = *ca * a[a_dim1 + 2];
	    cr[2] = *ca * a[(a_dim1 << 1) + 1];
	}

	if (*nw == 1) {

/*           Real 2x2 system  (w is real) */

/*           Find the largest element in C */

	    cmax = 0.f;
	    icmax = 0;

	    for (j = 1; j <= 4; ++j) {
		if ((r__1 = crv[j - 1], abs(r__1)) > cmax) {
		    cmax = (r__1 = crv[j - 1], abs(r__1));
		    icmax = j;
		}
/* L10: */
	    }

/*           If norm(C) < SMINI, use SMINI*identity. */

	    if (cmax < smini) {
/* Computing MAX */
		r__3 = (r__1 = b[b_dim1 + 1], abs(r__1)), r__4 = (r__2 = b[
			b_dim1 + 2], abs(r__2));
		bnorm = f2cmax(r__3,r__4);
		if (smini < 1.f && bnorm > 1.f) {
		    if (bnorm > bignum * smini) {
			*scale = 1.f / bnorm;
		    }
		}
		temp = *scale / smini;
		x[x_dim1 + 1] = temp * b[b_dim1 + 1];
		x[x_dim1 + 2] = temp * b[b_dim1 + 2];
		*xnorm = temp * bnorm;
		*info = 1;
		return;
	    }

/*           Gaussian elimination with complete pivoting. */

	    ur11 = crv[icmax - 1];
	    cr21 = crv[ipivot[(icmax << 2) - 3] - 1];
	    ur12 = crv[ipivot[(icmax << 2) - 2] - 1];
	    cr22 = crv[ipivot[(icmax << 2) - 1] - 1];
	    ur11r = 1.f / ur11;
	    lr21 = ur11r * cr21;
	    ur22 = cr22 - ur12 * lr21;

/*           If smaller pivot < SMINI, use SMINI */

	    if (abs(ur22) < smini) {
		ur22 = smini;
		*info = 1;
	    }
	    if (rswap[icmax - 1]) {
		br1 = b[b_dim1 + 2];
		br2 = b[b_dim1 + 1];
	    } else {
		br1 = b[b_dim1 + 1];
		br2 = b[b_dim1 + 2];
	    }
	    br2 -= lr21 * br1;
/* Computing MAX */
	    r__2 = (r__1 = br1 * (ur22 * ur11r), abs(r__1)), r__3 = abs(br2);
	    bbnd = f2cmax(r__2,r__3);
	    if (bbnd > 1.f && abs(ur22) < 1.f) {
		if (bbnd >= bignum * abs(ur22)) {
		    *scale = 1.f / bbnd;
		}
	    }

	    xr2 = br2 * *scale / ur22;
	    xr1 = *scale * br1 * ur11r - xr2 * (ur11r * ur12);
	    if (cswap[icmax - 1]) {
		x[x_dim1 + 1] = xr2;
		x[x_dim1 + 2] = xr1;
	    } else {
		x[x_dim1 + 1] = xr1;
		x[x_dim1 + 2] = xr2;
	    }
/* Computing MAX */
	    r__1 = abs(xr1), r__2 = abs(xr2);
	    *xnorm = f2cmax(r__1,r__2);

/*           Further scaling if  norm(A) norm(X) > overflow */

	    if (*xnorm > 1.f && cmax > 1.f) {
		if (*xnorm > bignum / cmax) {
		    temp = cmax / bignum;
		    x[x_dim1 + 1] = temp * x[x_dim1 + 1];
		    x[x_dim1 + 2] = temp * x[x_dim1 + 2];
		    *xnorm = temp * *xnorm;
		    *scale = temp * *scale;
		}
	    }
	} else {

/*           Complex 2x2 system  (w is complex) */

/*           Find the largest element in C */

	    ci[0] = -(*wi) * *d1;
	    ci[1] = 0.f;
	    ci[2] = 0.f;
	    ci[3] = -(*wi) * *d2;
	    cmax = 0.f;
	    icmax = 0;

	    for (j = 1; j <= 4; ++j) {
		if ((r__1 = crv[j - 1], abs(r__1)) + (r__2 = civ[j - 1], abs(
			r__2)) > cmax) {
		    cmax = (r__1 = crv[j - 1], abs(r__1)) + (r__2 = civ[j - 1]
			    , abs(r__2));
		    icmax = j;
		}
/* L20: */
	    }

/*           If norm(C) < SMINI, use SMINI*identity. */

	    if (cmax < smini) {
/* Computing MAX */
		r__5 = (r__1 = b[b_dim1 + 1], abs(r__1)) + (r__2 = b[(b_dim1 
			<< 1) + 1], abs(r__2)), r__6 = (r__3 = b[b_dim1 + 2], 
			abs(r__3)) + (r__4 = b[(b_dim1 << 1) + 2], abs(r__4));
		bnorm = f2cmax(r__5,r__6);
		if (smini < 1.f && bnorm > 1.f) {
		    if (bnorm > bignum * smini) {
			*scale = 1.f / bnorm;
		    }
		}
		temp = *scale / smini;
		x[x_dim1 + 1] = temp * b[b_dim1 + 1];
		x[x_dim1 + 2] = temp * b[b_dim1 + 2];
		x[(x_dim1 << 1) + 1] = temp * b[(b_dim1 << 1) + 1];
		x[(x_dim1 << 1) + 2] = temp * b[(b_dim1 << 1) + 2];
		*xnorm = temp * bnorm;
		*info = 1;
		return;
	    }

/*           Gaussian elimination with complete pivoting. */

	    ur11 = crv[icmax - 1];
	    ui11 = civ[icmax - 1];
	    cr21 = crv[ipivot[(icmax << 2) - 3] - 1];
	    ci21 = civ[ipivot[(icmax << 2) - 3] - 1];
	    ur12 = crv[ipivot[(icmax << 2) - 2] - 1];
	    ui12 = civ[ipivot[(icmax << 2) - 2] - 1];
	    cr22 = crv[ipivot[(icmax << 2) - 1] - 1];
	    ci22 = civ[ipivot[(icmax << 2) - 1] - 1];
	    if (icmax == 1 || icmax == 4) {

/*              Code when off-diagonals of pivoted C are real */

		if (abs(ur11) > abs(ui11)) {
		    temp = ui11 / ur11;
/* Computing 2nd power */
		    r__1 = temp;
		    ur11r = 1.f / (ur11 * (r__1 * r__1 + 1.f));
		    ui11r = -temp * ur11r;
		} else {
		    temp = ur11 / ui11;
/* Computing 2nd power */
		    r__1 = temp;
		    ui11r = -1.f / (ui11 * (r__1 * r__1 + 1.f));
		    ur11r = -temp * ui11r;
		}
		lr21 = cr21 * ur11r;
		li21 = cr21 * ui11r;
		ur12s = ur12 * ur11r;
		ui12s = ur12 * ui11r;
		ur22 = cr22 - ur12 * lr21;
		ui22 = ci22 - ur12 * li21;
	    } else {

/*              Code when diagonals of pivoted C are real */

		ur11r = 1.f / ur11;
		ui11r = 0.f;
		lr21 = cr21 * ur11r;
		li21 = ci21 * ur11r;
		ur12s = ur12 * ur11r;
		ui12s = ui12 * ur11r;
		ur22 = cr22 - ur12 * lr21 + ui12 * li21;
		ui22 = -ur12 * li21 - ui12 * lr21;
	    }
	    u22abs = abs(ur22) + abs(ui22);

/*           If smaller pivot < SMINI, use SMINI */

	    if (u22abs < smini) {
		ur22 = smini;
		ui22 = 0.f;
		*info = 1;
	    }
	    if (rswap[icmax - 1]) {
		br2 = b[b_dim1 + 1];
		br1 = b[b_dim1 + 2];
		bi2 = b[(b_dim1 << 1) + 1];
		bi1 = b[(b_dim1 << 1) + 2];
	    } else {
		br1 = b[b_dim1 + 1];
		br2 = b[b_dim1 + 2];
		bi1 = b[(b_dim1 << 1) + 1];
		bi2 = b[(b_dim1 << 1) + 2];
	    }
	    br2 = br2 - lr21 * br1 + li21 * bi1;
	    bi2 = bi2 - li21 * br1 - lr21 * bi1;
/* Computing MAX */
	    r__1 = (abs(br1) + abs(bi1)) * (u22abs * (abs(ur11r) + abs(ui11r))
		    ), r__2 = abs(br2) + abs(bi2);
	    bbnd = f2cmax(r__1,r__2);
	    if (bbnd > 1.f && u22abs < 1.f) {
		if (bbnd >= bignum * u22abs) {
		    *scale = 1.f / bbnd;
		    br1 = *scale * br1;
		    bi1 = *scale * bi1;
		    br2 = *scale * br2;
		    bi2 = *scale * bi2;
		}
	    }

	    sladiv_(&br2, &bi2, &ur22, &ui22, &xr2, &xi2);
	    xr1 = ur11r * br1 - ui11r * bi1 - ur12s * xr2 + ui12s * xi2;
	    xi1 = ui11r * br1 + ur11r * bi1 - ui12s * xr2 - ur12s * xi2;
	    if (cswap[icmax - 1]) {
		x[x_dim1 + 1] = xr2;
		x[x_dim1 + 2] = xr1;
		x[(x_dim1 << 1) + 1] = xi2;
		x[(x_dim1 << 1) + 2] = xi1;
	    } else {
		x[x_dim1 + 1] = xr1;
		x[x_dim1 + 2] = xr2;
		x[(x_dim1 << 1) + 1] = xi1;
		x[(x_dim1 << 1) + 2] = xi2;
	    }
/* Computing MAX */
	    r__1 = abs(xr1) + abs(xi1), r__2 = abs(xr2) + abs(xi2);
	    *xnorm = f2cmax(r__1,r__2);

/*           Further scaling if  norm(A) norm(X) > overflow */

	    if (*xnorm > 1.f && cmax > 1.f) {
		if (*xnorm > bignum / cmax) {
		    temp = cmax / bignum;
		    x[x_dim1 + 1] = temp * x[x_dim1 + 1];
		    x[x_dim1 + 2] = temp * x[x_dim1 + 2];
		    x[(x_dim1 << 1) + 1] = temp * x[(x_dim1 << 1) + 1];
		    x[(x_dim1 << 1) + 2] = temp * x[(x_dim1 << 1) + 2];
		    *xnorm = temp * *xnorm;
		    *scale = temp * *scale;
		}
	    }
	}
    }

    return;

/*     End of SLALN2 */

} /* slaln2_ */

#undef crv
#undef civ
#undef cr
#undef ci


