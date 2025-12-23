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
static integer c__4 = 4;
static logical c_false = FALSE_;
static integer c_n1 = -1;
static integer c__2 = 2;
static integer c__3 = 3;

/* > \brief \b DLAEXC swaps adjacent diagonal blocks of a real upper quasi-triangular matrix in Schur canonica
l form, by an orthogonal similarity transformation. */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download DLAEXC + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dlaexc.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dlaexc.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dlaexc.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE DLAEXC( WANTQ, N, T, LDT, Q, LDQ, J1, N1, N2, WORK, */
/*                          INFO ) */

/*       LOGICAL            WANTQ */
/*       INTEGER            INFO, J1, LDQ, LDT, N, N1, N2 */
/*       DOUBLE PRECISION   Q( LDQ, * ), T( LDT, * ), WORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > DLAEXC swaps adjacent diagonal blocks T11 and T22 of order 1 or 2 in */
/* > an upper quasi-triangular matrix T by an orthogonal similarity */
/* > transformation. */
/* > */
/* > T must be in Schur canonical form, that is, block upper triangular */
/* > with 1-by-1 and 2-by-2 diagonal blocks; each 2-by-2 diagonal block */
/* > has its diagonal elemnts equal and its off-diagonal elements of */
/* > opposite sign. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] WANTQ */
/* > \verbatim */
/* >          WANTQ is LOGICAL */
/* >          = .TRUE. : accumulate the transformation in the matrix Q; */
/* >          = .FALSE.: do not accumulate the transformation. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrix T. N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] T */
/* > \verbatim */
/* >          T is DOUBLE PRECISION array, dimension (LDT,N) */
/* >          On entry, the upper quasi-triangular matrix T, in Schur */
/* >          canonical form. */
/* >          On exit, the updated matrix T, again in Schur canonical form. */
/* > \endverbatim */
/* > */
/* > \param[in] LDT */
/* > \verbatim */
/* >          LDT is INTEGER */
/* >          The leading dimension of the array T. LDT >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in,out] Q */
/* > \verbatim */
/* >          Q is DOUBLE PRECISION array, dimension (LDQ,N) */
/* >          On entry, if WANTQ is .TRUE., the orthogonal matrix Q. */
/* >          On exit, if WANTQ is .TRUE., the updated matrix Q. */
/* >          If WANTQ is .FALSE., Q is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDQ */
/* > \verbatim */
/* >          LDQ is INTEGER */
/* >          The leading dimension of the array Q. */
/* >          LDQ >= 1; and if WANTQ is .TRUE., LDQ >= N. */
/* > \endverbatim */
/* > */
/* > \param[in] J1 */
/* > \verbatim */
/* >          J1 is INTEGER */
/* >          The index of the first row of the first block T11. */
/* > \endverbatim */
/* > */
/* > \param[in] N1 */
/* > \verbatim */
/* >          N1 is INTEGER */
/* >          The order of the first block T11. N1 = 0, 1 or 2. */
/* > \endverbatim */
/* > */
/* > \param[in] N2 */
/* > \verbatim */
/* >          N2 is INTEGER */
/* >          The order of the second block T22. N2 = 0, 1 or 2. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is DOUBLE PRECISION array, dimension (N) */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0: successful exit */
/* >          = 1: the transformed matrix T would be too far from Schur */
/* >               form; the blocks are not swapped and T and Q are */
/* >               unchanged. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup doubleOTHERauxiliary */

/*  ===================================================================== */
/* Subroutine */ void dlaexc_(logical *wantq, integer *n, doublereal *t, 
	integer *ldt, doublereal *q, integer *ldq, integer *j1, integer *n1, 
	integer *n2, doublereal *work, integer *info)
{
    /* System generated locals */
    integer q_dim1, q_offset, t_dim1, t_offset, i__1;
    doublereal d__1, d__2, d__3;

    /* Local variables */
    integer ierr;
    doublereal temp;
    extern /* Subroutine */ void drot_(integer *, doublereal *, integer *, 
	    doublereal *, integer *, doublereal *, doublereal *);
    doublereal d__[16]	/* was [4][4] */;
    integer k;
    doublereal u[3], scale, x[4]	/* was [2][2] */, dnorm;
    integer j2, j3, j4;
    doublereal xnorm, u1[3], u2[3];
    extern /* Subroutine */ void dlanv2_(doublereal *, doublereal *, 
	    doublereal *, doublereal *, doublereal *, doublereal *, 
	    doublereal *, doublereal *, doublereal *, doublereal *), dlasy2_(
	    logical *, logical *, integer *, integer *, integer *, doublereal 
	    *, integer *, doublereal *, integer *, doublereal *, integer *, 
	    doublereal *, doublereal *, integer *, doublereal *, integer *);
    integer nd;
    doublereal cs, t11, t22;
    extern doublereal dlamch_(char *);
    doublereal t33;
    extern doublereal dlange_(char *, integer *, integer *, doublereal *, 
	    integer *, doublereal *);
    extern /* Subroutine */ void dlarfg_(integer *, doublereal *, doublereal *,
	     integer *, doublereal *);
    doublereal sn;
    extern /* Subroutine */ void dlacpy_(char *, integer *, integer *, 
	    doublereal *, integer *, doublereal *, integer *), 
	    dlartg_(doublereal *, doublereal *, doublereal *, doublereal *, 
	    doublereal *), dlarfx_(char *, integer *, integer *, doublereal *,
	     doublereal *, doublereal *, integer *, doublereal *);
    doublereal thresh, smlnum, wi1, wi2, wr1, wr2, eps, tau, tau1, tau2;


/*  -- LAPACK auxiliary routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


    /* Parameter adjustments */
    t_dim1 = *ldt;
    t_offset = 1 + t_dim1 * 1;
    t -= t_offset;
    q_dim1 = *ldq;
    q_offset = 1 + q_dim1 * 1;
    q -= q_offset;
    --work;

    /* Function Body */
    *info = 0;

/*     Quick return if possible */

    if (*n == 0 || *n1 == 0 || *n2 == 0) {
	return;
    }
    if (*j1 + *n1 > *n) {
	return;
    }

    j2 = *j1 + 1;
    j3 = *j1 + 2;
    j4 = *j1 + 3;

    if (*n1 == 1 && *n2 == 1) {

/*        Swap two 1-by-1 blocks. */

	t11 = t[*j1 + *j1 * t_dim1];
	t22 = t[j2 + j2 * t_dim1];

/*        Determine the transformation to perform the interchange. */

	d__1 = t22 - t11;
	dlartg_(&t[*j1 + j2 * t_dim1], &d__1, &cs, &sn, &temp);

/*        Apply transformation to the matrix T. */

	if (j3 <= *n) {
	    i__1 = *n - *j1 - 1;
	    drot_(&i__1, &t[*j1 + j3 * t_dim1], ldt, &t[j2 + j3 * t_dim1], 
		    ldt, &cs, &sn);
	}
	i__1 = *j1 - 1;
	drot_(&i__1, &t[*j1 * t_dim1 + 1], &c__1, &t[j2 * t_dim1 + 1], &c__1, 
		&cs, &sn);

	t[*j1 + *j1 * t_dim1] = t22;
	t[j2 + j2 * t_dim1] = t11;

	if (*wantq) {

/*           Accumulate transformation in the matrix Q. */

	    drot_(n, &q[*j1 * q_dim1 + 1], &c__1, &q[j2 * q_dim1 + 1], &c__1, 
		    &cs, &sn);
	}

    } else {

/*        Swapping involves at least one 2-by-2 block. */

/*        Copy the diagonal block of order N1+N2 to the local array D */
/*        and compute its norm. */

	nd = *n1 + *n2;
	dlacpy_("Full", &nd, &nd, &t[*j1 + *j1 * t_dim1], ldt, d__, &c__4);
	dnorm = dlange_("Max", &nd, &nd, d__, &c__4, &work[1]);

/*        Compute machine-dependent threshold for test for accepting */
/*        swap. */

	eps = dlamch_("P");
	smlnum = dlamch_("S") / eps;
/* Computing MAX */
	d__1 = eps * 10. * dnorm;
	thresh = f2cmax(d__1,smlnum);

/*        Solve T11*X - X*T22 = scale*T12 for X. */

	dlasy2_(&c_false, &c_false, &c_n1, n1, n2, d__, &c__4, &d__[*n1 + 1 + 
		(*n1 + 1 << 2) - 5], &c__4, &d__[(*n1 + 1 << 2) - 4], &c__4, &
		scale, x, &c__2, &xnorm, &ierr);

/*        Swap the adjacent diagonal blocks. */

	k = *n1 + *n1 + *n2 - 3;
	switch (k) {
	    case 1:  goto L10;
	    case 2:  goto L20;
	    case 3:  goto L30;
	}

L10:

/*        N1 = 1, N2 = 2: generate elementary reflector H so that: */

/*        ( scale, X11, X12 ) H = ( 0, 0, * ) */

	u[0] = scale;
	u[1] = x[0];
	u[2] = x[2];
	dlarfg_(&c__3, &u[2], u, &c__1, &tau);
	u[2] = 1.;
	t11 = t[*j1 + *j1 * t_dim1];

/*        Perform swap provisionally on diagonal block in D. */

	dlarfx_("L", &c__3, &c__3, u, &tau, d__, &c__4, &work[1]);
	dlarfx_("R", &c__3, &c__3, u, &tau, d__, &c__4, &work[1]);

/*        Test whether to reject swap. */

/* Computing MAX */
	d__2 = abs(d__[2]), d__3 = abs(d__[6]), d__2 = f2cmax(d__2,d__3), d__3 = 
		(d__1 = d__[10] - t11, abs(d__1));
	if (f2cmax(d__2,d__3) > thresh) {
	    goto L50;
	}

/*        Accept swap: apply transformation to the entire matrix T. */

	i__1 = *n - *j1 + 1;
	dlarfx_("L", &c__3, &i__1, u, &tau, &t[*j1 + *j1 * t_dim1], ldt, &
		work[1]);
	dlarfx_("R", &j2, &c__3, u, &tau, &t[*j1 * t_dim1 + 1], ldt, &work[1]);

	t[j3 + *j1 * t_dim1] = 0.;
	t[j3 + j2 * t_dim1] = 0.;
	t[j3 + j3 * t_dim1] = t11;

	if (*wantq) {

/*           Accumulate transformation in the matrix Q. */

	    dlarfx_("R", n, &c__3, u, &tau, &q[*j1 * q_dim1 + 1], ldq, &work[
		    1]);
	}
	goto L40;

L20:

/*        N1 = 2, N2 = 1: generate elementary reflector H so that: */

/*        H (  -X11 ) = ( * ) */
/*          (  -X21 ) = ( 0 ) */
/*          ( scale ) = ( 0 ) */

	u[0] = -x[0];
	u[1] = -x[1];
	u[2] = scale;
	dlarfg_(&c__3, u, &u[1], &c__1, &tau);
	u[0] = 1.;
	t33 = t[j3 + j3 * t_dim1];

/*        Perform swap provisionally on diagonal block in D. */

	dlarfx_("L", &c__3, &c__3, u, &tau, d__, &c__4, &work[1]);
	dlarfx_("R", &c__3, &c__3, u, &tau, d__, &c__4, &work[1]);

/*        Test whether to reject swap. */

/* Computing MAX */
	d__2 = abs(d__[1]), d__3 = abs(d__[2]), d__2 = f2cmax(d__2,d__3), d__3 = 
		(d__1 = d__[0] - t33, abs(d__1));
	if (f2cmax(d__2,d__3) > thresh) {
	    goto L50;
	}

/*        Accept swap: apply transformation to the entire matrix T. */

	dlarfx_("R", &j3, &c__3, u, &tau, &t[*j1 * t_dim1 + 1], ldt, &work[1]);
	i__1 = *n - *j1;
	dlarfx_("L", &c__3, &i__1, u, &tau, &t[*j1 + j2 * t_dim1], ldt, &work[
		1]);

	t[*j1 + *j1 * t_dim1] = t33;
	t[j2 + *j1 * t_dim1] = 0.;
	t[j3 + *j1 * t_dim1] = 0.;

	if (*wantq) {

/*           Accumulate transformation in the matrix Q. */

	    dlarfx_("R", n, &c__3, u, &tau, &q[*j1 * q_dim1 + 1], ldq, &work[
		    1]);
	}
	goto L40;

L30:

/*        N1 = 2, N2 = 2: generate elementary reflectors H(1) and H(2) so */
/*        that: */

/*        H(2) H(1) (  -X11  -X12 ) = (  *  * ) */
/*                  (  -X21  -X22 )   (  0  * ) */
/*                  ( scale    0  )   (  0  0 ) */
/*                  (    0  scale )   (  0  0 ) */

	u1[0] = -x[0];
	u1[1] = -x[1];
	u1[2] = scale;
	dlarfg_(&c__3, u1, &u1[1], &c__1, &tau1);
	u1[0] = 1.;

	temp = -tau1 * (x[2] + u1[1] * x[3]);
	u2[0] = -temp * u1[1] - x[3];
	u2[1] = -temp * u1[2];
	u2[2] = scale;
	dlarfg_(&c__3, u2, &u2[1], &c__1, &tau2);
	u2[0] = 1.;

/*        Perform swap provisionally on diagonal block in D. */

	dlarfx_("L", &c__3, &c__4, u1, &tau1, d__, &c__4, &work[1])
		;
	dlarfx_("R", &c__4, &c__3, u1, &tau1, d__, &c__4, &work[1])
		;
	dlarfx_("L", &c__3, &c__4, u2, &tau2, &d__[1], &c__4, &work[1]);
	dlarfx_("R", &c__4, &c__3, u2, &tau2, &d__[4], &c__4, &work[1]);

/*        Test whether to reject swap. */

/* Computing MAX */
	d__1 = abs(d__[2]), d__2 = abs(d__[6]), d__1 = f2cmax(d__1,d__2), d__2 = 
		abs(d__[3]), d__1 = f2cmax(d__1,d__2), d__2 = abs(d__[7]);
	if (f2cmax(d__1,d__2) > thresh) {
	    goto L50;
	}

/*        Accept swap: apply transformation to the entire matrix T. */

	i__1 = *n - *j1 + 1;
	dlarfx_("L", &c__3, &i__1, u1, &tau1, &t[*j1 + *j1 * t_dim1], ldt, &
		work[1]);
	dlarfx_("R", &j4, &c__3, u1, &tau1, &t[*j1 * t_dim1 + 1], ldt, &work[
		1]);
	i__1 = *n - *j1 + 1;
	dlarfx_("L", &c__3, &i__1, u2, &tau2, &t[j2 + *j1 * t_dim1], ldt, &
		work[1]);
	dlarfx_("R", &j4, &c__3, u2, &tau2, &t[j2 * t_dim1 + 1], ldt, &work[1]
		);

	t[j3 + *j1 * t_dim1] = 0.;
	t[j3 + j2 * t_dim1] = 0.;
	t[j4 + *j1 * t_dim1] = 0.;
	t[j4 + j2 * t_dim1] = 0.;

	if (*wantq) {

/*           Accumulate transformation in the matrix Q. */

	    dlarfx_("R", n, &c__3, u1, &tau1, &q[*j1 * q_dim1 + 1], ldq, &
		    work[1]);
	    dlarfx_("R", n, &c__3, u2, &tau2, &q[j2 * q_dim1 + 1], ldq, &work[
		    1]);
	}

L40:

	if (*n2 == 2) {

/*           Standardize new 2-by-2 block T11 */

	    dlanv2_(&t[*j1 + *j1 * t_dim1], &t[*j1 + j2 * t_dim1], &t[j2 + *
		    j1 * t_dim1], &t[j2 + j2 * t_dim1], &wr1, &wi1, &wr2, &
		    wi2, &cs, &sn);
	    i__1 = *n - *j1 - 1;
	    drot_(&i__1, &t[*j1 + (*j1 + 2) * t_dim1], ldt, &t[j2 + (*j1 + 2) 
		    * t_dim1], ldt, &cs, &sn);
	    i__1 = *j1 - 1;
	    drot_(&i__1, &t[*j1 * t_dim1 + 1], &c__1, &t[j2 * t_dim1 + 1], &
		    c__1, &cs, &sn);
	    if (*wantq) {
		drot_(n, &q[*j1 * q_dim1 + 1], &c__1, &q[j2 * q_dim1 + 1], &
			c__1, &cs, &sn);
	    }
	}

	if (*n1 == 2) {

/*           Standardize new 2-by-2 block T22 */

	    j3 = *j1 + *n2;
	    j4 = j3 + 1;
	    dlanv2_(&t[j3 + j3 * t_dim1], &t[j3 + j4 * t_dim1], &t[j4 + j3 * 
		    t_dim1], &t[j4 + j4 * t_dim1], &wr1, &wi1, &wr2, &wi2, &
		    cs, &sn);
	    if (j3 + 2 <= *n) {
		i__1 = *n - j3 - 1;
		drot_(&i__1, &t[j3 + (j3 + 2) * t_dim1], ldt, &t[j4 + (j3 + 2)
			 * t_dim1], ldt, &cs, &sn);
	    }
	    i__1 = j3 - 1;
	    drot_(&i__1, &t[j3 * t_dim1 + 1], &c__1, &t[j4 * t_dim1 + 1], &
		    c__1, &cs, &sn);
	    if (*wantq) {
		drot_(n, &q[j3 * q_dim1 + 1], &c__1, &q[j4 * q_dim1 + 1], &
			c__1, &cs, &sn);
	    }
	}

    }
    return;

/*     Exit with INFO = 1 if swap was rejected. */

L50:
    *info = 1;
    return;

/*     End of DLAEXC */

} /* dlaexc_ */

