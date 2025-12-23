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

static complex c_b1 = {0.f,0.f};
static complex c_b2 = {1.f,0.f};
static integer c__1 = 1;

/* > \brief \b CTGEVC */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download CTGEVC + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/ctgevc.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/ctgevc.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/ctgevc.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE CTGEVC( SIDE, HOWMNY, SELECT, N, S, LDS, P, LDP, VL, */
/*                          LDVL, VR, LDVR, MM, M, WORK, RWORK, INFO ) */

/*       CHARACTER          HOWMNY, SIDE */
/*       INTEGER            INFO, LDP, LDS, LDVL, LDVR, M, MM, N */
/*       LOGICAL            SELECT( * ) */
/*       REAL               RWORK( * ) */
/*       COMPLEX            P( LDP, * ), S( LDS, * ), VL( LDVL, * ), */
/*      $                   VR( LDVR, * ), WORK( * ) */



/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > CTGEVC computes some or all of the right and/or left eigenvectors of */
/* > a pair of complex matrices (S,P), where S and P are upper triangular. */
/* > Matrix pairs of this type are produced by the generalized Schur */
/* > factorization of a complex matrix pair (A,B): */
/* > */
/* >    A = Q*S*Z**H,  B = Q*P*Z**H */
/* > */
/* > as computed by CGGHRD + CHGEQZ. */
/* > */
/* > The right eigenvector x and the left eigenvector y of (S,P) */
/* > corresponding to an eigenvalue w are defined by: */
/* > */
/* >    S*x = w*P*x,  (y**H)*S = w*(y**H)*P, */
/* > */
/* > where y**H denotes the conjugate tranpose of y. */
/* > The eigenvalues are not input to this routine, but are computed */
/* > directly from the diagonal elements of S and P. */
/* > */
/* > This routine returns the matrices X and/or Y of right and left */
/* > eigenvectors of (S,P), or the products Z*X and/or Q*Y, */
/* > where Z and Q are input matrices. */
/* > If Q and Z are the unitary factors from the generalized Schur */
/* > factorization of a matrix pair (A,B), then Z*X and Q*Y */
/* > are the matrices of right and left eigenvectors of (A,B). */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] SIDE */
/* > \verbatim */
/* >          SIDE is CHARACTER*1 */
/* >          = 'R': compute right eigenvectors only; */
/* >          = 'L': compute left eigenvectors only; */
/* >          = 'B': compute both right and left eigenvectors. */
/* > \endverbatim */
/* > */
/* > \param[in] HOWMNY */
/* > \verbatim */
/* >          HOWMNY is CHARACTER*1 */
/* >          = 'A': compute all right and/or left eigenvectors; */
/* >          = 'B': compute all right and/or left eigenvectors, */
/* >                 backtransformed by the matrices in VR and/or VL; */
/* >          = 'S': compute selected right and/or left eigenvectors, */
/* >                 specified by the logical array SELECT. */
/* > \endverbatim */
/* > */
/* > \param[in] SELECT */
/* > \verbatim */
/* >          SELECT is LOGICAL array, dimension (N) */
/* >          If HOWMNY='S', SELECT specifies the eigenvectors to be */
/* >          computed.  The eigenvector corresponding to the j-th */
/* >          eigenvalue is computed if SELECT(j) = .TRUE.. */
/* >          Not referenced if HOWMNY = 'A' or 'B'. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrices S and P.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] S */
/* > \verbatim */
/* >          S is COMPLEX array, dimension (LDS,N) */
/* >          The upper triangular matrix S from a generalized Schur */
/* >          factorization, as computed by CHGEQZ. */
/* > \endverbatim */
/* > */
/* > \param[in] LDS */
/* > \verbatim */
/* >          LDS is INTEGER */
/* >          The leading dimension of array S.  LDS >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in] P */
/* > \verbatim */
/* >          P is COMPLEX array, dimension (LDP,N) */
/* >          The upper triangular matrix P from a generalized Schur */
/* >          factorization, as computed by CHGEQZ.  P must have real */
/* >          diagonal elements. */
/* > \endverbatim */
/* > */
/* > \param[in] LDP */
/* > \verbatim */
/* >          LDP is INTEGER */
/* >          The leading dimension of array P.  LDP >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in,out] VL */
/* > \verbatim */
/* >          VL is COMPLEX array, dimension (LDVL,MM) */
/* >          On entry, if SIDE = 'L' or 'B' and HOWMNY = 'B', VL must */
/* >          contain an N-by-N matrix Q (usually the unitary matrix Q */
/* >          of left Schur vectors returned by CHGEQZ). */
/* >          On exit, if SIDE = 'L' or 'B', VL contains: */
/* >          if HOWMNY = 'A', the matrix Y of left eigenvectors of (S,P); */
/* >          if HOWMNY = 'B', the matrix Q*Y; */
/* >          if HOWMNY = 'S', the left eigenvectors of (S,P) specified by */
/* >                      SELECT, stored consecutively in the columns of */
/* >                      VL, in the same order as their eigenvalues. */
/* >          Not referenced if SIDE = 'R'. */
/* > \endverbatim */
/* > */
/* > \param[in] LDVL */
/* > \verbatim */
/* >          LDVL is INTEGER */
/* >          The leading dimension of array VL.  LDVL >= 1, and if */
/* >          SIDE = 'L' or 'l' or 'B' or 'b', LDVL >= N. */
/* > \endverbatim */
/* > */
/* > \param[in,out] VR */
/* > \verbatim */
/* >          VR is COMPLEX array, dimension (LDVR,MM) */
/* >          On entry, if SIDE = 'R' or 'B' and HOWMNY = 'B', VR must */
/* >          contain an N-by-N matrix Q (usually the unitary matrix Z */
/* >          of right Schur vectors returned by CHGEQZ). */
/* >          On exit, if SIDE = 'R' or 'B', VR contains: */
/* >          if HOWMNY = 'A', the matrix X of right eigenvectors of (S,P); */
/* >          if HOWMNY = 'B', the matrix Z*X; */
/* >          if HOWMNY = 'S', the right eigenvectors of (S,P) specified by */
/* >                      SELECT, stored consecutively in the columns of */
/* >                      VR, in the same order as their eigenvalues. */
/* >          Not referenced if SIDE = 'L'. */
/* > \endverbatim */
/* > */
/* > \param[in] LDVR */
/* > \verbatim */
/* >          LDVR is INTEGER */
/* >          The leading dimension of the array VR.  LDVR >= 1, and if */
/* >          SIDE = 'R' or 'B', LDVR >= N. */
/* > \endverbatim */
/* > */
/* > \param[in] MM */
/* > \verbatim */
/* >          MM is INTEGER */
/* >          The number of columns in the arrays VL and/or VR. MM >= M. */
/* > \endverbatim */
/* > */
/* > \param[out] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >          The number of columns in the arrays VL and/or VR actually */
/* >          used to store the eigenvectors.  If HOWMNY = 'A' or 'B', M */
/* >          is set to N.  Each selected eigenvector occupies one column. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is COMPLEX array, dimension (2*N) */
/* > \endverbatim */
/* > */
/* > \param[out] RWORK */
/* > \verbatim */
/* >          RWORK is REAL array, dimension (2*N) */
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

/* > \ingroup complexGEcomputational */

/*  ===================================================================== */
/* Subroutine */ void ctgevc_(char *side, char *howmny, logical *select, 
	integer *n, complex *s, integer *lds, complex *p, integer *ldp, 
	complex *vl, integer *ldvl, complex *vr, integer *ldvr, integer *mm, 
	integer *m, complex *work, real *rwork, integer *info)
{
    /* System generated locals */
    integer p_dim1, p_offset, s_dim1, s_offset, vl_dim1, vl_offset, vr_dim1, 
	    vr_offset, i__1, i__2, i__3, i__4, i__5;
    real r__1, r__2, r__3, r__4, r__5, r__6;
    complex q__1, q__2, q__3, q__4;

    /* Local variables */
    integer ibeg, ieig, iend;
    real dmin__;
    integer isrc;
    real temp;
    complex suma, sumb;
    real xmax;
    complex d__;
    integer i__, j;
    real scale;
    logical ilall;
    integer iside;
    real sbeta;
    extern logical lsame_(char *, char *);
    extern /* Subroutine */ void cgemv_(char *, integer *, integer *, complex *
	    , complex *, integer *, complex *, integer *, complex *, complex *
	    , integer *);
    real small;
    logical compl;
    real anorm, bnorm;
    logical compr;
    complex ca, cb;
    logical ilbbad;
    real acoefa;
    integer je;
    real bcoefa, acoeff;
    complex bcoeff;
    logical ilback;
    integer im;
    extern /* Subroutine */ void slabad_(real *, real *);
    real ascale, bscale;
    integer jr;
    extern /* Complex */ VOID cladiv_(complex *, complex *, complex *);
    extern real slamch_(char *);
    complex salpha;
    real safmin;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    real bignum;
    logical ilcomp;
    integer ihwmny;
    real big;
    logical lsa, lsb;
    real ulp;
    complex sum;


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */



/*  ===================================================================== */


/*     Decode and Test the input parameters */

    /* Parameter adjustments */
    --select;
    s_dim1 = *lds;
    s_offset = 1 + s_dim1 * 1;
    s -= s_offset;
    p_dim1 = *ldp;
    p_offset = 1 + p_dim1 * 1;
    p -= p_offset;
    vl_dim1 = *ldvl;
    vl_offset = 1 + vl_dim1 * 1;
    vl -= vl_offset;
    vr_dim1 = *ldvr;
    vr_offset = 1 + vr_dim1 * 1;
    vr -= vr_offset;
    --work;
    --rwork;

    /* Function Body */
    if (lsame_(howmny, "A")) {
	ihwmny = 1;
	ilall = TRUE_;
	ilback = FALSE_;
    } else if (lsame_(howmny, "S")) {
	ihwmny = 2;
	ilall = FALSE_;
	ilback = FALSE_;
    } else if (lsame_(howmny, "B")) {
	ihwmny = 3;
	ilall = TRUE_;
	ilback = TRUE_;
    } else {
	ihwmny = -1;
    }

    if (lsame_(side, "R")) {
	iside = 1;
	compl = FALSE_;
	compr = TRUE_;
    } else if (lsame_(side, "L")) {
	iside = 2;
	compl = TRUE_;
	compr = FALSE_;
    } else if (lsame_(side, "B")) {
	iside = 3;
	compl = TRUE_;
	compr = TRUE_;
    } else {
	iside = -1;
    }

    *info = 0;
    if (iside < 0) {
	*info = -1;
    } else if (ihwmny < 0) {
	*info = -2;
    } else if (*n < 0) {
	*info = -4;
    } else if (*lds < f2cmax(1,*n)) {
	*info = -6;
    } else if (*ldp < f2cmax(1,*n)) {
	*info = -8;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("CTGEVC", &i__1, (ftnlen)6);
	return;
    }

/*     Count the number of eigenvectors */

    if (! ilall) {
	im = 0;
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    if (select[j]) {
		++im;
	    }
/* L10: */
	}
    } else {
	im = *n;
    }

/*     Check diagonal of B */

    ilbbad = FALSE_;
    i__1 = *n;
    for (j = 1; j <= i__1; ++j) {
	if (r_imag(&p[j + j * p_dim1]) != 0.f) {
	    ilbbad = TRUE_;
	}
/* L20: */
    }

    if (ilbbad) {
	*info = -7;
    } else if (compl && *ldvl < *n || *ldvl < 1) {
	*info = -10;
    } else if (compr && *ldvr < *n || *ldvr < 1) {
	*info = -12;
    } else if (*mm < im) {
	*info = -13;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("CTGEVC", &i__1, (ftnlen)6);
	return;
    }

/*     Quick return if possible */

    *m = im;
    if (*n == 0) {
	return;
    }

/*     Machine Constants */

    safmin = slamch_("Safe minimum");
    big = 1.f / safmin;
    slabad_(&safmin, &big);
    ulp = slamch_("Epsilon") * slamch_("Base");
    small = safmin * *n / ulp;
    big = 1.f / small;
    bignum = 1.f / (safmin * *n);

/*     Compute the 1-norm of each column of the strictly upper triangular */
/*     part of A and B to check for possible overflow in the triangular */
/*     solver. */

    i__1 = s_dim1 + 1;
    anorm = (r__1 = s[i__1].r, abs(r__1)) + (r__2 = r_imag(&s[s_dim1 + 1]), 
	    abs(r__2));
    i__1 = p_dim1 + 1;
    bnorm = (r__1 = p[i__1].r, abs(r__1)) + (r__2 = r_imag(&p[p_dim1 + 1]), 
	    abs(r__2));
    rwork[1] = 0.f;
    rwork[*n + 1] = 0.f;
    i__1 = *n;
    for (j = 2; j <= i__1; ++j) {
	rwork[j] = 0.f;
	rwork[*n + j] = 0.f;
	i__2 = j - 1;
	for (i__ = 1; i__ <= i__2; ++i__) {
	    i__3 = i__ + j * s_dim1;
	    rwork[j] += (r__1 = s[i__3].r, abs(r__1)) + (r__2 = r_imag(&s[i__ 
		    + j * s_dim1]), abs(r__2));
	    i__3 = i__ + j * p_dim1;
	    rwork[*n + j] += (r__1 = p[i__3].r, abs(r__1)) + (r__2 = r_imag(&
		    p[i__ + j * p_dim1]), abs(r__2));
/* L30: */
	}
/* Computing MAX */
	i__2 = j + j * s_dim1;
	r__3 = anorm, r__4 = rwork[j] + ((r__1 = s[i__2].r, abs(r__1)) + (
		r__2 = r_imag(&s[j + j * s_dim1]), abs(r__2)));
	anorm = f2cmax(r__3,r__4);
/* Computing MAX */
	i__2 = j + j * p_dim1;
	r__3 = bnorm, r__4 = rwork[*n + j] + ((r__1 = p[i__2].r, abs(r__1)) + 
		(r__2 = r_imag(&p[j + j * p_dim1]), abs(r__2)));
	bnorm = f2cmax(r__3,r__4);
/* L40: */
    }

    ascale = 1.f / f2cmax(anorm,safmin);
    bscale = 1.f / f2cmax(bnorm,safmin);

/*     Left eigenvectors */

    if (compl) {
	ieig = 0;

/*        Main loop over eigenvalues */

	i__1 = *n;
	for (je = 1; je <= i__1; ++je) {
	    if (ilall) {
		ilcomp = TRUE_;
	    } else {
		ilcomp = select[je];
	    }
	    if (ilcomp) {
		++ieig;

		i__2 = je + je * s_dim1;
		i__3 = je + je * p_dim1;
		if ((r__2 = s[i__2].r, abs(r__2)) + (r__3 = r_imag(&s[je + je 
			* s_dim1]), abs(r__3)) <= safmin && (r__1 = p[i__3].r,
			 abs(r__1)) <= safmin) {

/*                 Singular matrix pencil -- return unit eigenvector */

		    i__2 = *n;
		    for (jr = 1; jr <= i__2; ++jr) {
			i__3 = jr + ieig * vl_dim1;
			vl[i__3].r = 0.f, vl[i__3].i = 0.f;
/* L50: */
		    }
		    i__2 = ieig + ieig * vl_dim1;
		    vl[i__2].r = 1.f, vl[i__2].i = 0.f;
		    goto L140;
		}

/*              Non-singular eigenvalue: */
/*              Compute coefficients  a  and  b  in */
/*                   H */
/*                 y  ( a A - b B ) = 0 */

/* Computing MAX */
		i__2 = je + je * s_dim1;
		i__3 = je + je * p_dim1;
		r__4 = ((r__2 = s[i__2].r, abs(r__2)) + (r__3 = r_imag(&s[je 
			+ je * s_dim1]), abs(r__3))) * ascale, r__5 = (r__1 = 
			p[i__3].r, abs(r__1)) * bscale, r__4 = f2cmax(r__4,r__5);
		temp = 1.f / f2cmax(r__4,safmin);
		i__2 = je + je * s_dim1;
		q__2.r = temp * s[i__2].r, q__2.i = temp * s[i__2].i;
		q__1.r = ascale * q__2.r, q__1.i = ascale * q__2.i;
		salpha.r = q__1.r, salpha.i = q__1.i;
		i__2 = je + je * p_dim1;
		sbeta = temp * p[i__2].r * bscale;
		acoeff = sbeta * ascale;
		q__1.r = bscale * salpha.r, q__1.i = bscale * salpha.i;
		bcoeff.r = q__1.r, bcoeff.i = q__1.i;

/*              Scale to avoid underflow */

		lsa = abs(sbeta) >= safmin && abs(acoeff) < small;
		lsb = (r__1 = salpha.r, abs(r__1)) + (r__2 = r_imag(&salpha), 
			abs(r__2)) >= safmin && (r__3 = bcoeff.r, abs(r__3)) 
			+ (r__4 = r_imag(&bcoeff), abs(r__4)) < small;

		scale = 1.f;
		if (lsa) {
		    scale = small / abs(sbeta) * f2cmin(anorm,big);
		}
		if (lsb) {
/* Computing MAX */
		    r__3 = scale, r__4 = small / ((r__1 = salpha.r, abs(r__1))
			     + (r__2 = r_imag(&salpha), abs(r__2))) * f2cmin(
			    bnorm,big);
		    scale = f2cmax(r__3,r__4);
		}
		if (lsa || lsb) {
/* Computing MIN */
/* Computing MAX */
		    r__5 = 1.f, r__6 = abs(acoeff), r__5 = f2cmax(r__5,r__6), 
			    r__6 = (r__1 = bcoeff.r, abs(r__1)) + (r__2 = 
			    r_imag(&bcoeff), abs(r__2));
		    r__3 = scale, r__4 = 1.f / (safmin * f2cmax(r__5,r__6));
		    scale = f2cmin(r__3,r__4);
		    if (lsa) {
			acoeff = ascale * (scale * sbeta);
		    } else {
			acoeff = scale * acoeff;
		    }
		    if (lsb) {
			q__2.r = scale * salpha.r, q__2.i = scale * salpha.i;
			q__1.r = bscale * q__2.r, q__1.i = bscale * q__2.i;
			bcoeff.r = q__1.r, bcoeff.i = q__1.i;
		    } else {
			q__1.r = scale * bcoeff.r, q__1.i = scale * bcoeff.i;
			bcoeff.r = q__1.r, bcoeff.i = q__1.i;
		    }
		}

		acoefa = abs(acoeff);
		bcoefa = (r__1 = bcoeff.r, abs(r__1)) + (r__2 = r_imag(&
			bcoeff), abs(r__2));
		xmax = 1.f;
		i__2 = *n;
		for (jr = 1; jr <= i__2; ++jr) {
		    i__3 = jr;
		    work[i__3].r = 0.f, work[i__3].i = 0.f;
/* L60: */
		}
		i__2 = je;
		work[i__2].r = 1.f, work[i__2].i = 0.f;
/* Computing MAX */
		r__1 = ulp * acoefa * anorm, r__2 = ulp * bcoefa * bnorm, 
			r__1 = f2cmax(r__1,r__2);
		dmin__ = f2cmax(r__1,safmin);

/*                                              H */
/*              Triangular solve of  (a A - b B)  y = 0 */

/*                                      H */
/*              (rowwise in  (a A - b B) , or columnwise in a A - b B) */

		i__2 = *n;
		for (j = je + 1; j <= i__2; ++j) {

/*                 Compute */
/*                       j-1 */
/*                 SUM = sum  conjg( a*S(k,j) - b*P(k,j) )*x(k) */
/*                       k=je */
/*                 (Scale if necessary) */

		    temp = 1.f / xmax;
		    if (acoefa * rwork[j] + bcoefa * rwork[*n + j] > bignum * 
			    temp) {
			i__3 = j - 1;
			for (jr = je; jr <= i__3; ++jr) {
			    i__4 = jr;
			    i__5 = jr;
			    q__1.r = temp * work[i__5].r, q__1.i = temp * 
				    work[i__5].i;
			    work[i__4].r = q__1.r, work[i__4].i = q__1.i;
/* L70: */
			}
			xmax = 1.f;
		    }
		    suma.r = 0.f, suma.i = 0.f;
		    sumb.r = 0.f, sumb.i = 0.f;

		    i__3 = j - 1;
		    for (jr = je; jr <= i__3; ++jr) {
			r_cnjg(&q__3, &s[jr + j * s_dim1]);
			i__4 = jr;
			q__2.r = q__3.r * work[i__4].r - q__3.i * work[i__4]
				.i, q__2.i = q__3.r * work[i__4].i + q__3.i * 
				work[i__4].r;
			q__1.r = suma.r + q__2.r, q__1.i = suma.i + q__2.i;
			suma.r = q__1.r, suma.i = q__1.i;
			r_cnjg(&q__3, &p[jr + j * p_dim1]);
			i__4 = jr;
			q__2.r = q__3.r * work[i__4].r - q__3.i * work[i__4]
				.i, q__2.i = q__3.r * work[i__4].i + q__3.i * 
				work[i__4].r;
			q__1.r = sumb.r + q__2.r, q__1.i = sumb.i + q__2.i;
			sumb.r = q__1.r, sumb.i = q__1.i;
/* L80: */
		    }
		    q__2.r = acoeff * suma.r, q__2.i = acoeff * suma.i;
		    r_cnjg(&q__4, &bcoeff);
		    q__3.r = q__4.r * sumb.r - q__4.i * sumb.i, q__3.i = 
			    q__4.r * sumb.i + q__4.i * sumb.r;
		    q__1.r = q__2.r - q__3.r, q__1.i = q__2.i - q__3.i;
		    sum.r = q__1.r, sum.i = q__1.i;

/*                 Form x(j) = - SUM / conjg( a*S(j,j) - b*P(j,j) ) */

/*                 with scaling and perturbation of the denominator */

		    i__3 = j + j * s_dim1;
		    q__3.r = acoeff * s[i__3].r, q__3.i = acoeff * s[i__3].i;
		    i__4 = j + j * p_dim1;
		    q__4.r = bcoeff.r * p[i__4].r - bcoeff.i * p[i__4].i, 
			    q__4.i = bcoeff.r * p[i__4].i + bcoeff.i * p[i__4]
			    .r;
		    q__2.r = q__3.r - q__4.r, q__2.i = q__3.i - q__4.i;
		    r_cnjg(&q__1, &q__2);
		    d__.r = q__1.r, d__.i = q__1.i;
		    if ((r__1 = d__.r, abs(r__1)) + (r__2 = r_imag(&d__), abs(
			    r__2)) <= dmin__) {
			q__1.r = dmin__, q__1.i = 0.f;
			d__.r = q__1.r, d__.i = q__1.i;
		    }

		    if ((r__1 = d__.r, abs(r__1)) + (r__2 = r_imag(&d__), abs(
			    r__2)) < 1.f) {
			if ((r__1 = sum.r, abs(r__1)) + (r__2 = r_imag(&sum), 
				abs(r__2)) >= bignum * ((r__3 = d__.r, abs(
				r__3)) + (r__4 = r_imag(&d__), abs(r__4)))) {
			    temp = 1.f / ((r__1 = sum.r, abs(r__1)) + (r__2 = 
				    r_imag(&sum), abs(r__2)));
			    i__3 = j - 1;
			    for (jr = je; jr <= i__3; ++jr) {
				i__4 = jr;
				i__5 = jr;
				q__1.r = temp * work[i__5].r, q__1.i = temp * 
					work[i__5].i;
				work[i__4].r = q__1.r, work[i__4].i = q__1.i;
/* L90: */
			    }
			    xmax = temp * xmax;
			    q__1.r = temp * sum.r, q__1.i = temp * sum.i;
			    sum.r = q__1.r, sum.i = q__1.i;
			}
		    }
		    i__3 = j;
		    q__2.r = -sum.r, q__2.i = -sum.i;
		    cladiv_(&q__1, &q__2, &d__);
		    work[i__3].r = q__1.r, work[i__3].i = q__1.i;
/* Computing MAX */
		    i__3 = j;
		    r__3 = xmax, r__4 = (r__1 = work[i__3].r, abs(r__1)) + (
			    r__2 = r_imag(&work[j]), abs(r__2));
		    xmax = f2cmax(r__3,r__4);
/* L100: */
		}

/*              Back transform eigenvector if HOWMNY='B'. */

		if (ilback) {
		    i__2 = *n + 1 - je;
		    cgemv_("N", n, &i__2, &c_b2, &vl[je * vl_dim1 + 1], ldvl, 
			    &work[je], &c__1, &c_b1, &work[*n + 1], &c__1);
		    isrc = 2;
		    ibeg = 1;
		} else {
		    isrc = 1;
		    ibeg = je;
		}

/*              Copy and scale eigenvector into column of VL */

		xmax = 0.f;
		i__2 = *n;
		for (jr = ibeg; jr <= i__2; ++jr) {
/* Computing MAX */
		    i__3 = (isrc - 1) * *n + jr;
		    r__3 = xmax, r__4 = (r__1 = work[i__3].r, abs(r__1)) + (
			    r__2 = r_imag(&work[(isrc - 1) * *n + jr]), abs(
			    r__2));
		    xmax = f2cmax(r__3,r__4);
/* L110: */
		}

		if (xmax > safmin) {
		    temp = 1.f / xmax;
		    i__2 = *n;
		    for (jr = ibeg; jr <= i__2; ++jr) {
			i__3 = jr + ieig * vl_dim1;
			i__4 = (isrc - 1) * *n + jr;
			q__1.r = temp * work[i__4].r, q__1.i = temp * work[
				i__4].i;
			vl[i__3].r = q__1.r, vl[i__3].i = q__1.i;
/* L120: */
		    }
		} else {
		    ibeg = *n + 1;
		}

		i__2 = ibeg - 1;
		for (jr = 1; jr <= i__2; ++jr) {
		    i__3 = jr + ieig * vl_dim1;
		    vl[i__3].r = 0.f, vl[i__3].i = 0.f;
/* L130: */
		}

	    }
L140:
	    ;
	}
    }

/*     Right eigenvectors */

    if (compr) {
	ieig = im + 1;

/*        Main loop over eigenvalues */

	for (je = *n; je >= 1; --je) {
	    if (ilall) {
		ilcomp = TRUE_;
	    } else {
		ilcomp = select[je];
	    }
	    if (ilcomp) {
		--ieig;

		i__1 = je + je * s_dim1;
		i__2 = je + je * p_dim1;
		if ((r__2 = s[i__1].r, abs(r__2)) + (r__3 = r_imag(&s[je + je 
			* s_dim1]), abs(r__3)) <= safmin && (r__1 = p[i__2].r,
			 abs(r__1)) <= safmin) {

/*                 Singular matrix pencil -- return unit eigenvector */

		    i__1 = *n;
		    for (jr = 1; jr <= i__1; ++jr) {
			i__2 = jr + ieig * vr_dim1;
			vr[i__2].r = 0.f, vr[i__2].i = 0.f;
/* L150: */
		    }
		    i__1 = ieig + ieig * vr_dim1;
		    vr[i__1].r = 1.f, vr[i__1].i = 0.f;
		    goto L250;
		}

/*              Non-singular eigenvalue: */
/*              Compute coefficients  a  and  b  in */

/*              ( a A - b B ) x  = 0 */

/* Computing MAX */
		i__1 = je + je * s_dim1;
		i__2 = je + je * p_dim1;
		r__4 = ((r__2 = s[i__1].r, abs(r__2)) + (r__3 = r_imag(&s[je 
			+ je * s_dim1]), abs(r__3))) * ascale, r__5 = (r__1 = 
			p[i__2].r, abs(r__1)) * bscale, r__4 = f2cmax(r__4,r__5);
		temp = 1.f / f2cmax(r__4,safmin);
		i__1 = je + je * s_dim1;
		q__2.r = temp * s[i__1].r, q__2.i = temp * s[i__1].i;
		q__1.r = ascale * q__2.r, q__1.i = ascale * q__2.i;
		salpha.r = q__1.r, salpha.i = q__1.i;
		i__1 = je + je * p_dim1;
		sbeta = temp * p[i__1].r * bscale;
		acoeff = sbeta * ascale;
		q__1.r = bscale * salpha.r, q__1.i = bscale * salpha.i;
		bcoeff.r = q__1.r, bcoeff.i = q__1.i;

/*              Scale to avoid underflow */

		lsa = abs(sbeta) >= safmin && abs(acoeff) < small;
		lsb = (r__1 = salpha.r, abs(r__1)) + (r__2 = r_imag(&salpha), 
			abs(r__2)) >= safmin && (r__3 = bcoeff.r, abs(r__3)) 
			+ (r__4 = r_imag(&bcoeff), abs(r__4)) < small;

		scale = 1.f;
		if (lsa) {
		    scale = small / abs(sbeta) * f2cmin(anorm,big);
		}
		if (lsb) {
/* Computing MAX */
		    r__3 = scale, r__4 = small / ((r__1 = salpha.r, abs(r__1))
			     + (r__2 = r_imag(&salpha), abs(r__2))) * f2cmin(
			    bnorm,big);
		    scale = f2cmax(r__3,r__4);
		}
		if (lsa || lsb) {
/* Computing MIN */
/* Computing MAX */
		    r__5 = 1.f, r__6 = abs(acoeff), r__5 = f2cmax(r__5,r__6), 
			    r__6 = (r__1 = bcoeff.r, abs(r__1)) + (r__2 = 
			    r_imag(&bcoeff), abs(r__2));
		    r__3 = scale, r__4 = 1.f / (safmin * f2cmax(r__5,r__6));
		    scale = f2cmin(r__3,r__4);
		    if (lsa) {
			acoeff = ascale * (scale * sbeta);
		    } else {
			acoeff = scale * acoeff;
		    }
		    if (lsb) {
			q__2.r = scale * salpha.r, q__2.i = scale * salpha.i;
			q__1.r = bscale * q__2.r, q__1.i = bscale * q__2.i;
			bcoeff.r = q__1.r, bcoeff.i = q__1.i;
		    } else {
			q__1.r = scale * bcoeff.r, q__1.i = scale * bcoeff.i;
			bcoeff.r = q__1.r, bcoeff.i = q__1.i;
		    }
		}

		acoefa = abs(acoeff);
		bcoefa = (r__1 = bcoeff.r, abs(r__1)) + (r__2 = r_imag(&
			bcoeff), abs(r__2));
		xmax = 1.f;
		i__1 = *n;
		for (jr = 1; jr <= i__1; ++jr) {
		    i__2 = jr;
		    work[i__2].r = 0.f, work[i__2].i = 0.f;
/* L160: */
		}
		i__1 = je;
		work[i__1].r = 1.f, work[i__1].i = 0.f;
/* Computing MAX */
		r__1 = ulp * acoefa * anorm, r__2 = ulp * bcoefa * bnorm, 
			r__1 = f2cmax(r__1,r__2);
		dmin__ = f2cmax(r__1,safmin);

/*              Triangular solve of  (a A - b B) x = 0  (columnwise) */

/*              WORK(1:j-1) contains sums w, */
/*              WORK(j+1:JE) contains x */

		i__1 = je - 1;
		for (jr = 1; jr <= i__1; ++jr) {
		    i__2 = jr;
		    i__3 = jr + je * s_dim1;
		    q__2.r = acoeff * s[i__3].r, q__2.i = acoeff * s[i__3].i;
		    i__4 = jr + je * p_dim1;
		    q__3.r = bcoeff.r * p[i__4].r - bcoeff.i * p[i__4].i, 
			    q__3.i = bcoeff.r * p[i__4].i + bcoeff.i * p[i__4]
			    .r;
		    q__1.r = q__2.r - q__3.r, q__1.i = q__2.i - q__3.i;
		    work[i__2].r = q__1.r, work[i__2].i = q__1.i;
/* L170: */
		}
		i__1 = je;
		work[i__1].r = 1.f, work[i__1].i = 0.f;

		for (j = je - 1; j >= 1; --j) {

/*                 Form x(j) := - w(j) / d */
/*                 with scaling and perturbation of the denominator */

		    i__1 = j + j * s_dim1;
		    q__2.r = acoeff * s[i__1].r, q__2.i = acoeff * s[i__1].i;
		    i__2 = j + j * p_dim1;
		    q__3.r = bcoeff.r * p[i__2].r - bcoeff.i * p[i__2].i, 
			    q__3.i = bcoeff.r * p[i__2].i + bcoeff.i * p[i__2]
			    .r;
		    q__1.r = q__2.r - q__3.r, q__1.i = q__2.i - q__3.i;
		    d__.r = q__1.r, d__.i = q__1.i;
		    if ((r__1 = d__.r, abs(r__1)) + (r__2 = r_imag(&d__), abs(
			    r__2)) <= dmin__) {
			q__1.r = dmin__, q__1.i = 0.f;
			d__.r = q__1.r, d__.i = q__1.i;
		    }

		    if ((r__1 = d__.r, abs(r__1)) + (r__2 = r_imag(&d__), abs(
			    r__2)) < 1.f) {
			i__1 = j;
			if ((r__1 = work[i__1].r, abs(r__1)) + (r__2 = r_imag(
				&work[j]), abs(r__2)) >= bignum * ((r__3 = 
				d__.r, abs(r__3)) + (r__4 = r_imag(&d__), abs(
				r__4)))) {
			    i__1 = j;
			    temp = 1.f / ((r__1 = work[i__1].r, abs(r__1)) + (
				    r__2 = r_imag(&work[j]), abs(r__2)));
			    i__1 = je;
			    for (jr = 1; jr <= i__1; ++jr) {
				i__2 = jr;
				i__3 = jr;
				q__1.r = temp * work[i__3].r, q__1.i = temp * 
					work[i__3].i;
				work[i__2].r = q__1.r, work[i__2].i = q__1.i;
/* L180: */
			    }
			}
		    }

		    i__1 = j;
		    i__2 = j;
		    q__2.r = -work[i__2].r, q__2.i = -work[i__2].i;
		    cladiv_(&q__1, &q__2, &d__);
		    work[i__1].r = q__1.r, work[i__1].i = q__1.i;

		    if (j > 1) {

/*                    w = w + x(j)*(a S(*,j) - b P(*,j) ) with scaling */

			i__1 = j;
			if ((r__1 = work[i__1].r, abs(r__1)) + (r__2 = r_imag(
				&work[j]), abs(r__2)) > 1.f) {
			    i__1 = j;
			    temp = 1.f / ((r__1 = work[i__1].r, abs(r__1)) + (
				    r__2 = r_imag(&work[j]), abs(r__2)));
			    if (acoefa * rwork[j] + bcoefa * rwork[*n + j] >= 
				    bignum * temp) {
				i__1 = je;
				for (jr = 1; jr <= i__1; ++jr) {
				    i__2 = jr;
				    i__3 = jr;
				    q__1.r = temp * work[i__3].r, q__1.i = 
					    temp * work[i__3].i;
				    work[i__2].r = q__1.r, work[i__2].i = 
					    q__1.i;
/* L190: */
				}
			    }
			}

			i__1 = j;
			q__1.r = acoeff * work[i__1].r, q__1.i = acoeff * 
				work[i__1].i;
			ca.r = q__1.r, ca.i = q__1.i;
			i__1 = j;
			q__1.r = bcoeff.r * work[i__1].r - bcoeff.i * work[
				i__1].i, q__1.i = bcoeff.r * work[i__1].i + 
				bcoeff.i * work[i__1].r;
			cb.r = q__1.r, cb.i = q__1.i;
			i__1 = j - 1;
			for (jr = 1; jr <= i__1; ++jr) {
			    i__2 = jr;
			    i__3 = jr;
			    i__4 = jr + j * s_dim1;
			    q__3.r = ca.r * s[i__4].r - ca.i * s[i__4].i, 
				    q__3.i = ca.r * s[i__4].i + ca.i * s[i__4]
				    .r;
			    q__2.r = work[i__3].r + q__3.r, q__2.i = work[
				    i__3].i + q__3.i;
			    i__5 = jr + j * p_dim1;
			    q__4.r = cb.r * p[i__5].r - cb.i * p[i__5].i, 
				    q__4.i = cb.r * p[i__5].i + cb.i * p[i__5]
				    .r;
			    q__1.r = q__2.r - q__4.r, q__1.i = q__2.i - 
				    q__4.i;
			    work[i__2].r = q__1.r, work[i__2].i = q__1.i;
/* L200: */
			}
		    }
/* L210: */
		}

/*              Back transform eigenvector if HOWMNY='B'. */

		if (ilback) {
		    cgemv_("N", n, &je, &c_b2, &vr[vr_offset], ldvr, &work[1],
			     &c__1, &c_b1, &work[*n + 1], &c__1);
		    isrc = 2;
		    iend = *n;
		} else {
		    isrc = 1;
		    iend = je;
		}

/*              Copy and scale eigenvector into column of VR */

		xmax = 0.f;
		i__1 = iend;
		for (jr = 1; jr <= i__1; ++jr) {
/* Computing MAX */
		    i__2 = (isrc - 1) * *n + jr;
		    r__3 = xmax, r__4 = (r__1 = work[i__2].r, abs(r__1)) + (
			    r__2 = r_imag(&work[(isrc - 1) * *n + jr]), abs(
			    r__2));
		    xmax = f2cmax(r__3,r__4);
/* L220: */
		}

		if (xmax > safmin) {
		    temp = 1.f / xmax;
		    i__1 = iend;
		    for (jr = 1; jr <= i__1; ++jr) {
			i__2 = jr + ieig * vr_dim1;
			i__3 = (isrc - 1) * *n + jr;
			q__1.r = temp * work[i__3].r, q__1.i = temp * work[
				i__3].i;
			vr[i__2].r = q__1.r, vr[i__2].i = q__1.i;
/* L230: */
		    }
		} else {
		    iend = 0;
		}

		i__1 = *n;
		for (jr = iend + 1; jr <= i__1; ++jr) {
		    i__2 = jr + ieig * vr_dim1;
		    vr[i__2].r = 0.f, vr[i__2].i = 0.f;
/* L240: */
		}

	    }
L250:
	    ;
	}
    }

    return;

/*     End of CTGEVC */

} /* ctgevc_ */

