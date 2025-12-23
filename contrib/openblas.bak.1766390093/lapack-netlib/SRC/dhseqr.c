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

static doublereal c_b11 = 0.;
static doublereal c_b12 = 1.;
static integer c__12 = 12;
static integer c__2 = 2;
static integer c__49 = 49;

/* > \brief \b DHSEQR */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download DHSEQR + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dhseqr.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dhseqr.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dhseqr.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE DHSEQR( JOB, COMPZ, N, ILO, IHI, H, LDH, WR, WI, Z, */
/*                          LDZ, WORK, LWORK, INFO ) */

/*       INTEGER            IHI, ILO, INFO, LDH, LDZ, LWORK, N */
/*       CHARACTER          COMPZ, JOB */
/*       DOUBLE PRECISION   H( LDH, * ), WI( * ), WORK( * ), WR( * ), */
/*      $                   Z( LDZ, * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* >    DHSEQR computes the eigenvalues of a Hessenberg matrix H */
/* >    and, optionally, the matrices T and Z from the Schur decomposition */
/* >    H = Z T Z**T, where T is an upper quasi-triangular matrix (the */
/* >    Schur form), and Z is the orthogonal matrix of Schur vectors. */
/* > */
/* >    Optionally Z may be postmultiplied into an input orthogonal */
/* >    matrix Q so that this routine can give the Schur factorization */
/* >    of a matrix A which has been reduced to the Hessenberg form H */
/* >    by the orthogonal matrix Q:  A = Q*H*Q**T = (QZ)*T*(QZ)**T. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] JOB */
/* > \verbatim */
/* >          JOB is CHARACTER*1 */
/* >           = 'E':  compute eigenvalues only; */
/* >           = 'S':  compute eigenvalues and the Schur form T. */
/* > \endverbatim */
/* > */
/* > \param[in] COMPZ */
/* > \verbatim */
/* >          COMPZ is CHARACTER*1 */
/* >           = 'N':  no Schur vectors are computed; */
/* >           = 'I':  Z is initialized to the unit matrix and the matrix Z */
/* >                   of Schur vectors of H is returned; */
/* >           = 'V':  Z must contain an orthogonal matrix Q on entry, and */
/* >                   the product Q*Z is returned. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >           The order of the matrix H.  N >= 0. */
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
/* >           It is assumed that H is already upper triangular in rows */
/* >           and columns 1:ILO-1 and IHI+1:N. ILO and IHI are normally */
/* >           set by a previous call to DGEBAL, and then passed to ZGEHRD */
/* >           when the matrix output by DGEBAL is reduced to Hessenberg */
/* >           form. Otherwise ILO and IHI should be set to 1 and N */
/* >           respectively.  If N > 0, then 1 <= ILO <= IHI <= N. */
/* >           If N = 0, then ILO = 1 and IHI = 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] H */
/* > \verbatim */
/* >          H is DOUBLE PRECISION array, dimension (LDH,N) */
/* >           On entry, the upper Hessenberg matrix H. */
/* >           On exit, if INFO = 0 and JOB = 'S', then H contains the */
/* >           upper quasi-triangular matrix T from the Schur decomposition */
/* >           (the Schur form); 2-by-2 diagonal blocks (corresponding to */
/* >           complex conjugate pairs of eigenvalues) are returned in */
/* >           standard form, with H(i,i) = H(i+1,i+1) and */
/* >           H(i+1,i)*H(i,i+1) < 0. If INFO = 0 and JOB = 'E', the */
/* >           contents of H are unspecified on exit.  (The output value of */
/* >           H when INFO > 0 is given under the description of INFO */
/* >           below.) */
/* > */
/* >           Unlike earlier versions of DHSEQR, this subroutine may */
/* >           explicitly H(i,j) = 0 for i > j and j = 1, 2, ... ILO-1 */
/* >           or j = IHI+1, IHI+2, ... N. */
/* > \endverbatim */
/* > */
/* > \param[in] LDH */
/* > \verbatim */
/* >          LDH is INTEGER */
/* >           The leading dimension of the array H. LDH >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[out] WR */
/* > \verbatim */
/* >          WR is DOUBLE PRECISION array, dimension (N) */
/* > \endverbatim */
/* > */
/* > \param[out] WI */
/* > \verbatim */
/* >          WI is DOUBLE PRECISION array, dimension (N) */
/* > */
/* >           The real and imaginary parts, respectively, of the computed */
/* >           eigenvalues. If two eigenvalues are computed as a complex */
/* >           conjugate pair, they are stored in consecutive elements of */
/* >           WR and WI, say the i-th and (i+1)th, with WI(i) > 0 and */
/* >           WI(i+1) < 0. If JOB = 'S', the eigenvalues are stored in */
/* >           the same order as on the diagonal of the Schur form returned */
/* >           in H, with WR(i) = H(i,i) and, if H(i:i+1,i:i+1) is a 2-by-2 */
/* >           diagonal block, WI(i) = sqrt(-H(i+1,i)*H(i,i+1)) and */
/* >           WI(i+1) = -WI(i). */
/* > \endverbatim */
/* > */
/* > \param[in,out] Z */
/* > \verbatim */
/* >          Z is DOUBLE PRECISION array, dimension (LDZ,N) */
/* >           If COMPZ = 'N', Z is not referenced. */
/* >           If COMPZ = 'I', on entry Z need not be set and on exit, */
/* >           if INFO = 0, Z contains the orthogonal matrix Z of the Schur */
/* >           vectors of H.  If COMPZ = 'V', on entry Z must contain an */
/* >           N-by-N matrix Q, which is assumed to be equal to the unit */
/* >           matrix except for the submatrix Z(ILO:IHI,ILO:IHI). On exit, */
/* >           if INFO = 0, Z contains Q*Z. */
/* >           Normally Q is the orthogonal matrix generated by DORGHR */
/* >           after the call to DGEHRD which formed the Hessenberg matrix */
/* >           H. (The output value of Z when INFO > 0 is given under */
/* >           the description of INFO below.) */
/* > \endverbatim */
/* > */
/* > \param[in] LDZ */
/* > \verbatim */
/* >          LDZ is INTEGER */
/* >           The leading dimension of the array Z.  if COMPZ = 'I' or */
/* >           COMPZ = 'V', then LDZ >= MAX(1,N).  Otherwise, LDZ >= 1. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is DOUBLE PRECISION array, dimension (LWORK) */
/* >           On exit, if INFO = 0, WORK(1) returns an estimate of */
/* >           the optimal value for LWORK. */
/* > \endverbatim */
/* > */
/* > \param[in] LWORK */
/* > \verbatim */
/* >          LWORK is INTEGER */
/* >           The dimension of the array WORK.  LWORK >= f2cmax(1,N) */
/* >           is sufficient and delivers very good and sometimes */
/* >           optimal performance.  However, LWORK as large as 11*N */
/* >           may be required for optimal performance.  A workspace */
/* >           query is recommended to determine the optimal workspace */
/* >           size. */
/* > */
/* >           If LWORK = -1, then DHSEQR does a workspace query. */
/* >           In this case, DHSEQR checks the input parameters and */
/* >           estimates the optimal workspace size for the given */
/* >           values of N, ILO and IHI.  The estimate is returned */
/* >           in WORK(1).  No error message related to LWORK is */
/* >           issued by XERBLA.  Neither H nor Z are accessed. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >             = 0:  successful exit */
/* >             < 0:  if INFO = -i, the i-th argument had an illegal */
/* >                    value */
/* >             > 0:  if INFO = i, DHSEQR failed to compute all of */
/* >                the eigenvalues.  Elements 1:ilo-1 and i+1:n of WR */
/* >                and WI contain those eigenvalues which have been */
/* >                successfully computed.  (Failures are rare.) */
/* > */
/* >                If INFO > 0 and JOB = 'E', then on exit, the */
/* >                remaining unconverged eigenvalues are the eigen- */
/* >                values of the upper Hessenberg matrix rows and */
/* >                columns ILO through INFO of the final, output */
/* >                value of H. */
/* > */
/* >                If INFO > 0 and JOB   = 'S', then on exit */
/* > */
/* >           (*)  (initial value of H)*U  = U*(final value of H) */
/* > */
/* >                where U is an orthogonal matrix.  The final */
/* >                value of H is upper Hessenberg and quasi-triangular */
/* >                in rows and columns INFO+1 through IHI. */
/* > */
/* >                If INFO > 0 and COMPZ = 'V', then on exit */
/* > */
/* >                  (final value of Z)  =  (initial value of Z)*U */
/* > */
/* >                where U is the orthogonal matrix in (*) (regard- */
/* >                less of the value of JOB.) */
/* > */
/* >                If INFO > 0 and COMPZ = 'I', then on exit */
/* >                      (final value of Z)  = U */
/* >                where U is the orthogonal matrix in (*) (regard- */
/* >                less of the value of JOB.) */
/* > */
/* >                If INFO > 0 and COMPZ = 'N', then Z is not */
/* >                accessed. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup doubleOTHERcomputational */

/* > \par Contributors: */
/*  ================== */
/* > */
/* >       Karen Braman and Ralph Byers, Department of Mathematics, */
/* >       University of Kansas, USA */

/* > \par Further Details: */
/*  ===================== */
/* > */
/* > \verbatim */
/* > */
/* >             Default values supplied by */
/* >             ILAENV(ISPEC,'DHSEQR',JOB(:1)//COMPZ(:1),N,ILO,IHI,LWORK). */
/* >             It is suggested that these defaults be adjusted in order */
/* >             to attain best performance in each particular */
/* >             computational environment. */
/* > */
/* >            ISPEC=12: The DLAHQR vs DLAQR0 crossover point. */
/* >                      Default: 75. (Must be at least 11.) */
/* > */
/* >            ISPEC=13: Recommended deflation window size. */
/* >                      This depends on ILO, IHI and NS.  NS is the */
/* >                      number of simultaneous shifts returned */
/* >                      by ILAENV(ISPEC=15).  (See ISPEC=15 below.) */
/* >                      The default for (IHI-ILO+1) <= 500 is NS. */
/* >                      The default for (IHI-ILO+1) >  500 is 3*NS/2. */
/* > */
/* >            ISPEC=14: Nibble crossover point. (See IPARMQ for */
/* >                      details.)  Default: 14% of deflation window */
/* >                      size. */
/* > */
/* >            ISPEC=15: Number of simultaneous shifts in a multishift */
/* >                      QR iteration. */
/* > */
/* >                      If IHI-ILO+1 is ... */
/* > */
/* >                      greater than      ...but less    ... the */
/* >                      or equal to ...      than        default is */
/* > */
/* >                           1               30          NS =   2(+) */
/* >                          30               60          NS =   4(+) */
/* >                          60              150          NS =  10(+) */
/* >                         150              590          NS =  ** */
/* >                         590             3000          NS =  64 */
/* >                        3000             6000          NS = 128 */
/* >                        6000             infinity      NS = 256 */
/* > */
/* >                  (+)  By default some or all matrices of this order */
/* >                       are passed to the implicit double shift routine */
/* >                       DLAHQR and this parameter is ignored.  See */
/* >                       ISPEC=12 above and comments in IPARMQ for */
/* >                       details. */
/* > */
/* >                 (**)  The asterisks (**) indicate an ad-hoc */
/* >                       function of N increasing from 10 to 64. */
/* > */
/* >            ISPEC=16: Select structured matrix multiply. */
/* >                      If the number of simultaneous shifts (specified */
/* >                      by ISPEC=15) is less than 14, then the default */
/* >                      for ISPEC=16 is 0.  Otherwise the default for */
/* >                      ISPEC=16 is 2. */
/* > \endverbatim */

/* > \par References: */
/*  ================ */
/* > */
/* >       K. Braman, R. Byers and R. Mathias, The Multi-Shift QR */
/* >       Algorithm Part I: Maintaining Well Focused Shifts, and Level 3 */
/* >       Performance, SIAM Journal of Matrix Analysis, volume 23, pages */
/* >       929--947, 2002. */
/* > \n */
/* >       K. Braman, R. Byers and R. Mathias, The Multi-Shift QR */
/* >       Algorithm Part II: Aggressive Early Deflation, SIAM Journal */
/* >       of Matrix Analysis, volume 23, pages 948--973, 2002. */

/*  ===================================================================== */
/* Subroutine */ void dhseqr_(char *job, char *compz, integer *n, integer *ilo,
	 integer *ihi, doublereal *h__, integer *ldh, doublereal *wr, 
	doublereal *wi, doublereal *z__, integer *ldz, doublereal *work, 
	integer *lwork, integer *info)
{
    /* System generated locals */
    address a__1[2];
    integer h_dim1, h_offset, z_dim1, z_offset, i__1, i__2[2], i__3;
    doublereal d__1;
    char ch__1[2];

    /* Local variables */
    integer kbot, nmin, i__;
    extern logical lsame_(char *, char *);
    logical initz;
    doublereal workl[49];
    logical wantt, wantz;
    extern /* Subroutine */ void dlaqr0_(logical *, logical *, integer *, 
	    integer *, integer *, doublereal *, integer *, doublereal *, 
	    doublereal *, integer *, integer *, doublereal *, integer *, 
	    doublereal *, integer *, integer *);
    doublereal hl[2401]	/* was [49][49] */;
    extern /* Subroutine */ void dlahqr_(logical *, logical *, integer *, 
	    integer *, integer *, doublereal *, integer *, doublereal *, 
	    doublereal *, integer *, integer *, doublereal *, integer *, 
	    integer *), dlacpy_(char *, integer *, integer *, doublereal *, 
	    integer *, doublereal *, integer *), dlaset_(char *, 
	    integer *, integer *, doublereal *, doublereal *, doublereal *, 
	    integer *);
    extern integer ilaenv_(integer *, char *, char *, integer *, integer *, 
	    integer *, integer *, ftnlen, ftnlen);
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    logical lquery;


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


/*     ==== Matrices of order NTINY or smaller must be processed by */
/*     .    DLAHQR because of insufficient subdiagonal scratch space. */
/*     .    (This is a hard limit.) ==== */

/*     ==== NL allocates some local workspace to help small matrices */
/*     .    through a rare DLAHQR failure.  NL > NTINY = 15 is */
/*     .    required and NL <= NMIN = ILAENV(ISPEC=12,...) is recom- */
/*     .    mended.  (The default value of NMIN is 75.)  Using NL = 49 */
/*     .    allows up to six simultaneous shifts and a 16-by-16 */
/*     .    deflation window.  ==== */

/*     ==== Decode and check the input parameters. ==== */

    /* Parameter adjustments */
    h_dim1 = *ldh;
    h_offset = 1 + h_dim1 * 1;
    h__ -= h_offset;
    --wr;
    --wi;
    z_dim1 = *ldz;
    z_offset = 1 + z_dim1 * 1;
    z__ -= z_offset;
    --work;

    /* Function Body */
    wantt = lsame_(job, "S");
    initz = lsame_(compz, "I");
    wantz = initz || lsame_(compz, "V");
    work[1] = (doublereal) f2cmax(1,*n);
    lquery = *lwork == -1;

    *info = 0;
    if (! lsame_(job, "E") && ! wantt) {
	*info = -1;
    } else if (! lsame_(compz, "N") && ! wantz) {
	*info = -2;
    } else if (*n < 0) {
	*info = -3;
    } else if (*ilo < 1 || *ilo > f2cmax(1,*n)) {
	*info = -4;
    } else if (*ihi < f2cmin(*ilo,*n) || *ihi > *n) {
	*info = -5;
    } else if (*ldh < f2cmax(1,*n)) {
	*info = -7;
    } else if (*ldz < 1 || wantz && *ldz < f2cmax(1,*n)) {
	*info = -11;
    } else if (*lwork < f2cmax(1,*n) && ! lquery) {
	*info = -13;
    }

    if (*info != 0) {

/*        ==== Quick return in case of invalid argument. ==== */

	i__1 = -(*info);
	xerbla_("DHSEQR", &i__1, (ftnlen)6);
	return;

    } else if (*n == 0) {

/*        ==== Quick return in case N = 0; nothing to do. ==== */

	return;

    } else if (lquery) {

/*        ==== Quick return in case of a workspace query ==== */

	dlaqr0_(&wantt, &wantz, n, ilo, ihi, &h__[h_offset], ldh, &wr[1], &wi[
		1], ilo, ihi, &z__[z_offset], ldz, &work[1], lwork, info);
/*        ==== Ensure reported workspace size is backward-compatible with */
/*        .    previous LAPACK versions. ==== */
/* Computing MAX */
	d__1 = (doublereal) f2cmax(1,*n);
	work[1] = f2cmax(d__1,work[1]);
	return;

    } else {

/*        ==== copy eigenvalues isolated by DGEBAL ==== */

	i__1 = *ilo - 1;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    wr[i__] = h__[i__ + i__ * h_dim1];
	    wi[i__] = 0.;
/* L10: */
	}
	i__1 = *n;
	for (i__ = *ihi + 1; i__ <= i__1; ++i__) {
	    wr[i__] = h__[i__ + i__ * h_dim1];
	    wi[i__] = 0.;
/* L20: */
	}

/*        ==== Initialize Z, if requested ==== */

	if (initz) {
	    dlaset_("A", n, n, &c_b11, &c_b12, &z__[z_offset], ldz)
		    ;
	}

/*        ==== Quick return if possible ==== */

	if (*ilo == *ihi) {
	    wr[*ilo] = h__[*ilo + *ilo * h_dim1];
	    wi[*ilo] = 0.;
	    return;
	}

/*        ==== DLAHQR/DLAQR0 crossover point ==== */

/* Writing concatenation */
	i__2[0] = 1, a__1[0] = job;
	i__2[1] = 1, a__1[1] = compz;
	s_cat(ch__1, a__1, i__2, &c__2, (ftnlen)2);
	nmin = ilaenv_(&c__12, "DHSEQR", ch__1, n, ilo, ihi, lwork, (ftnlen)6,
		 (ftnlen)2);
	nmin = f2cmax(15,nmin);

/*        ==== DLAQR0 for big matrices; DLAHQR for small ones ==== */

	if (*n > nmin) {
	    dlaqr0_(&wantt, &wantz, n, ilo, ihi, &h__[h_offset], ldh, &wr[1], 
		    &wi[1], ilo, ihi, &z__[z_offset], ldz, &work[1], lwork, 
		    info);
	} else {

/*           ==== Small matrix ==== */

	    dlahqr_(&wantt, &wantz, n, ilo, ihi, &h__[h_offset], ldh, &wr[1], 
		    &wi[1], ilo, ihi, &z__[z_offset], ldz, info);

	    if (*info > 0) {

/*              ==== A rare DLAHQR failure!  DLAQR0 sometimes succeeds */
/*              .    when DLAHQR fails. ==== */

		kbot = *info;

		if (*n >= 49) {

/*                 ==== Larger matrices have enough subdiagonal scratch */
/*                 .    space to call DLAQR0 directly. ==== */

		    dlaqr0_(&wantt, &wantz, n, ilo, &kbot, &h__[h_offset], 
			    ldh, &wr[1], &wi[1], ilo, ihi, &z__[z_offset], 
			    ldz, &work[1], lwork, info);

		} else {

/*                 ==== Tiny matrices don't have enough subdiagonal */
/*                 .    scratch space to benefit from DLAQR0.  Hence, */
/*                 .    tiny matrices must be copied into a larger */
/*                 .    array before calling DLAQR0. ==== */

		    dlacpy_("A", n, n, &h__[h_offset], ldh, hl, &c__49);
		    hl[*n + 1 + *n * 49 - 50] = 0.;
		    i__1 = 49 - *n;
		    dlaset_("A", &c__49, &i__1, &c_b11, &c_b11, &hl[(*n + 1) *
			     49 - 49], &c__49);
		    dlaqr0_(&wantt, &wantz, &c__49, ilo, &kbot, hl, &c__49, &
			    wr[1], &wi[1], ilo, ihi, &z__[z_offset], ldz, 
			    workl, &c__49, info);
		    if (wantt || *info != 0) {
			dlacpy_("A", n, n, hl, &c__49, &h__[h_offset], ldh);
		    }
		}
	    }
	}

/*        ==== Clear out the trash, if necessary. ==== */

	if ((wantt || *info != 0) && *n > 2) {
	    i__1 = *n - 2;
	    i__3 = *n - 2;
	    dlaset_("L", &i__1, &i__3, &c_b11, &c_b11, &h__[h_dim1 + 3], ldh);
	}

/*        ==== Ensure reported workspace size is backward-compatible with */
/*        .    previous LAPACK versions. ==== */

/* Computing MAX */
	d__1 = (doublereal) f2cmax(1,*n);
	work[1] = f2cmax(d__1,work[1]);
    }

/*     ==== End of DHSEQR ==== */

    return;
} /* dhseqr_ */

