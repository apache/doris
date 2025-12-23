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
#define z_div(c, a, b) {Cd(c)._Val[0] = (Cd(a)._Val[0]/Cd(b)._Val[0]); Cd(c)._Val[1]=(Cd(a)._Val[1]/Cd(b)._Val[1]);}
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

/* > \brief \b ZLAHQR computes the eigenvalues and Schur factorization of an upper Hessenberg matrix, using th
e double-shift/single-shift QR algorithm. */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download ZLAHQR + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zlahqr.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zlahqr.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zlahqr.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE ZLAHQR( WANTT, WANTZ, N, ILO, IHI, H, LDH, W, ILOZ, */
/*                          IHIZ, Z, LDZ, INFO ) */

/*       INTEGER            IHI, IHIZ, ILO, ILOZ, INFO, LDH, LDZ, N */
/*       LOGICAL            WANTT, WANTZ */
/*       COMPLEX*16         H( LDH, * ), W( * ), Z( LDZ, * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* >    ZLAHQR is an auxiliary routine called by CHSEQR to update the */
/* >    eigenvalues and Schur decomposition already computed by CHSEQR, by */
/* >    dealing with the Hessenberg submatrix in rows and columns ILO to */
/* >    IHI. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] WANTT */
/* > \verbatim */
/* >          WANTT is LOGICAL */
/* >          = .TRUE. : the full Schur form T is required; */
/* >          = .FALSE.: only eigenvalues are required. */
/* > \endverbatim */
/* > */
/* > \param[in] WANTZ */
/* > \verbatim */
/* >          WANTZ is LOGICAL */
/* >          = .TRUE. : the matrix of Schur vectors Z is required; */
/* >          = .FALSE.: Schur vectors are not required. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrix H.  N >= 0. */
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
/* >          It is assumed that H is already upper triangular in rows and */
/* >          columns IHI+1:N, and that H(ILO,ILO-1) = 0 (unless ILO = 1). */
/* >          ZLAHQR works primarily with the Hessenberg submatrix in rows */
/* >          and columns ILO to IHI, but applies transformations to all of */
/* >          H if WANTT is .TRUE.. */
/* >          1 <= ILO <= f2cmax(1,IHI); IHI <= N. */
/* > \endverbatim */
/* > */
/* > \param[in,out] H */
/* > \verbatim */
/* >          H is COMPLEX*16 array, dimension (LDH,N) */
/* >          On entry, the upper Hessenberg matrix H. */
/* >          On exit, if INFO is zero and if WANTT is .TRUE., then H */
/* >          is upper triangular in rows and columns ILO:IHI.  If INFO */
/* >          is zero and if WANTT is .FALSE., then the contents of H */
/* >          are unspecified on exit.  The output state of H in case */
/* >          INF is positive is below under the description of INFO. */
/* > \endverbatim */
/* > */
/* > \param[in] LDH */
/* > \verbatim */
/* >          LDH is INTEGER */
/* >          The leading dimension of the array H. LDH >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[out] W */
/* > \verbatim */
/* >          W is COMPLEX*16 array, dimension (N) */
/* >          The computed eigenvalues ILO to IHI are stored in the */
/* >          corresponding elements of W. If WANTT is .TRUE., the */
/* >          eigenvalues are stored in the same order as on the diagonal */
/* >          of the Schur form returned in H, with W(i) = H(i,i). */
/* > \endverbatim */
/* > */
/* > \param[in] ILOZ */
/* > \verbatim */
/* >          ILOZ is INTEGER */
/* > \endverbatim */
/* > */
/* > \param[in] IHIZ */
/* > \verbatim */
/* >          IHIZ is INTEGER */
/* >          Specify the rows of Z to which transformations must be */
/* >          applied if WANTZ is .TRUE.. */
/* >          1 <= ILOZ <= ILO; IHI <= IHIZ <= N. */
/* > \endverbatim */
/* > */
/* > \param[in,out] Z */
/* > \verbatim */
/* >          Z is COMPLEX*16 array, dimension (LDZ,N) */
/* >          If WANTZ is .TRUE., on entry Z must contain the current */
/* >          matrix Z of transformations accumulated by CHSEQR, and on */
/* >          exit Z has been updated; transformations are applied only to */
/* >          the submatrix Z(ILOZ:IHIZ,ILO:IHI). */
/* >          If WANTZ is .FALSE., Z is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDZ */
/* > \verbatim */
/* >          LDZ is INTEGER */
/* >          The leading dimension of the array Z. LDZ >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >           = 0:   successful exit */
/* >           > 0:   if INFO = i, ZLAHQR failed to compute all the */
/* >                  eigenvalues ILO to IHI in a total of 30 iterations */
/* >                  per eigenvalue; elements i+1:ihi of W contain */
/* >                  those eigenvalues which have been successfully */
/* >                  computed. */
/* > */
/* >                  If INFO > 0 and WANTT is .FALSE., then on exit, */
/* >                  the remaining unconverged eigenvalues are the */
/* >                  eigenvalues of the upper Hessenberg matrix */
/* >                  rows and columns ILO through INFO of the final, */
/* >                  output value of H. */
/* > */
/* >                  If INFO > 0 and WANTT is .TRUE., then on exit */
/* >          (*)       (initial value of H)*U  = U*(final value of H) */
/* >                  where U is an orthogonal matrix.    The final */
/* >                  value of H is upper Hessenberg and triangular in */
/* >                  rows and columns INFO+1 through IHI. */
/* > */
/* >                  If INFO > 0 and WANTZ is .TRUE., then on exit */
/* >                      (final value of Z)  = (initial value of Z)*U */
/* >                  where U is the orthogonal matrix in (*) */
/* >                  (regardless of the value of WANTT.) */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup complex16OTHERauxiliary */

/* > \par Contributors: */
/*  ================== */
/* > */
/* > \verbatim */
/* > */
/* >     02-96 Based on modifications by */
/* >     David Day, Sandia National Laboratory, USA */
/* > */
/* >     12-04 Further modifications by */
/* >     Ralph Byers, University of Kansas, USA */
/* >     This is a modified version of ZLAHQR from LAPACK version 3.0. */
/* >     It is (1) more robust against overflow and underflow and */
/* >     (2) adopts the more conservative Ahues & Tisseur stopping */
/* >     criterion (LAWN 122, 1997). */
/* > \endverbatim */
/* > */
/*  ===================================================================== */
/* Subroutine */ void zlahqr_(logical *wantt, logical *wantz, integer *n, 
	integer *ilo, integer *ihi, doublecomplex *h__, integer *ldh, 
	doublecomplex *w, integer *iloz, integer *ihiz, doublecomplex *z__, 
	integer *ldz, integer *info)
{
    /* System generated locals */
    integer h_dim1, h_offset, z_dim1, z_offset, i__1, i__2, i__3, i__4, i__5;
    doublereal d__1, d__2, d__3, d__4, d__5, d__6;
    doublecomplex z__1, z__2, z__3, z__4, z__5, z__6, z__7;

    /* Local variables */
    doublecomplex temp;
    integer i__, j, k, l, m;
    doublereal s;
    doublecomplex t, u, v[2], x, y;
    extern /* Subroutine */ void zscal_(integer *, doublecomplex *, 
	    doublecomplex *, integer *);
    integer itmax;
    doublereal rtemp;
    integer i1, i2;
    extern /* Subroutine */ void zcopy_(integer *, doublecomplex *, integer *, 
	    doublecomplex *, integer *);
    doublecomplex t1;
    doublereal t2;
    doublecomplex v2;
    doublereal aa, ab, ba, bb;
    extern /* Subroutine */ void dlabad_(doublereal *, doublereal *);
    doublereal h10;
    doublecomplex h11;
    doublereal h21;
    doublecomplex h22, sc;
    integer nh;
    extern doublereal dlamch_(char *);
    integer nz;
    doublereal sx, safmin, safmax;
    extern /* Subroutine */ void zlarfg_(integer *, doublecomplex *, 
	    doublecomplex *, integer *, doublecomplex *);
    extern /* Double Complex */ VOID zladiv_(doublecomplex *, doublecomplex *,
	     doublecomplex *);
    doublereal smlnum;
    integer jhi;
    doublecomplex h11s;
    integer jlo, its;
    doublereal ulp;
    doublecomplex sum;
    doublereal tst;


/*  -- LAPACK auxiliary routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ========================================================= */


    /* Parameter adjustments */
    h_dim1 = *ldh;
    h_offset = 1 + h_dim1 * 1;
    h__ -= h_offset;
    --w;
    z_dim1 = *ldz;
    z_offset = 1 + z_dim1 * 1;
    z__ -= z_offset;

    /* Function Body */
    *info = 0;

/*     Quick return if possible */

    if (*n == 0) {
	return;
    }
    if (*ilo == *ihi) {
	i__1 = *ilo;
	i__2 = *ilo + *ilo * h_dim1;
	w[i__1].r = h__[i__2].r, w[i__1].i = h__[i__2].i;
	return;
    }

/*     ==== clear out the trash ==== */
    i__1 = *ihi - 3;
    for (j = *ilo; j <= i__1; ++j) {
	i__2 = j + 2 + j * h_dim1;
	h__[i__2].r = 0., h__[i__2].i = 0.;
	i__2 = j + 3 + j * h_dim1;
	h__[i__2].r = 0., h__[i__2].i = 0.;
/* L10: */
    }
    if (*ilo <= *ihi - 2) {
	i__1 = *ihi + (*ihi - 2) * h_dim1;
	h__[i__1].r = 0., h__[i__1].i = 0.;
    }
/*     ==== ensure that subdiagonal entries are real ==== */
    if (*wantt) {
	jlo = 1;
	jhi = *n;
    } else {
	jlo = *ilo;
	jhi = *ihi;
    }
    i__1 = *ihi;
    for (i__ = *ilo + 1; i__ <= i__1; ++i__) {
	if (d_imag(&h__[i__ + (i__ - 1) * h_dim1]) != 0.) {
/*           ==== The following redundant normalization */
/*           .    avoids problems with both gradual and */
/*           .    sudden underflow in ABS(H(I,I-1)) ==== */
	    i__2 = i__ + (i__ - 1) * h_dim1;
	    i__3 = i__ + (i__ - 1) * h_dim1;
	    d__3 = (d__1 = h__[i__3].r, abs(d__1)) + (d__2 = d_imag(&h__[i__ 
		    + (i__ - 1) * h_dim1]), abs(d__2));
	    z__1.r = h__[i__2].r / d__3, z__1.i = h__[i__2].i / d__3;
	    sc.r = z__1.r, sc.i = z__1.i;
	    d_cnjg(&z__2, &sc);
	    d__1 = z_abs(&sc);
	    z__1.r = z__2.r / d__1, z__1.i = z__2.i / d__1;
	    sc.r = z__1.r, sc.i = z__1.i;
	    i__2 = i__ + (i__ - 1) * h_dim1;
	    d__1 = z_abs(&h__[i__ + (i__ - 1) * h_dim1]);
	    h__[i__2].r = d__1, h__[i__2].i = 0.;
	    i__2 = jhi - i__ + 1;
	    zscal_(&i__2, &sc, &h__[i__ + i__ * h_dim1], ldh);
/* Computing MIN */
	    i__3 = jhi, i__4 = i__ + 1;
	    i__2 = f2cmin(i__3,i__4) - jlo + 1;
	    d_cnjg(&z__1, &sc);
	    zscal_(&i__2, &z__1, &h__[jlo + i__ * h_dim1], &c__1);
	    if (*wantz) {
		i__2 = *ihiz - *iloz + 1;
		d_cnjg(&z__1, &sc);
		zscal_(&i__2, &z__1, &z__[*iloz + i__ * z_dim1], &c__1);
	    }
	}
/* L20: */
    }

    nh = *ihi - *ilo + 1;
    nz = *ihiz - *iloz + 1;

/*     Set machine-dependent constants for the stopping criterion. */

    safmin = dlamch_("SAFE MINIMUM");
    safmax = 1. / safmin;
    dlabad_(&safmin, &safmax);
    ulp = dlamch_("PRECISION");
    smlnum = safmin * ((doublereal) nh / ulp);

/*     I1 and I2 are the indices of the first row and last column of H */
/*     to which transformations must be applied. If eigenvalues only are */
/*     being computed, I1 and I2 are set inside the main loop. */

    if (*wantt) {
	i1 = 1;
	i2 = *n;
    }

/*     ITMAX is the total number of QR iterations allowed. */

    itmax = f2cmax(10,nh) * 30;

/*     The main loop begins here. I is the loop index and decreases from */
/*     IHI to ILO in steps of 1. Each iteration of the loop works */
/*     with the active submatrix in rows and columns L to I. */
/*     Eigenvalues I+1 to IHI have already converged. Either L = ILO, or */
/*     H(L,L-1) is negligible so that the matrix splits. */

    i__ = *ihi;
L30:
    if (i__ < *ilo) {
	goto L150;
    }

/*     Perform QR iterations on rows and columns ILO to I until a */
/*     submatrix of order 1 splits off at the bottom because a */
/*     subdiagonal element has become negligible. */

    l = *ilo;
    i__1 = itmax;
    for (its = 0; its <= i__1; ++its) {

/*        Look for a single small subdiagonal element. */

	i__2 = l + 1;
	for (k = i__; k >= i__2; --k) {
	    i__3 = k + (k - 1) * h_dim1;
	    if ((d__1 = h__[i__3].r, abs(d__1)) + (d__2 = d_imag(&h__[k + (k 
		    - 1) * h_dim1]), abs(d__2)) <= smlnum) {
		goto L50;
	    }
	    i__3 = k - 1 + (k - 1) * h_dim1;
	    i__4 = k + k * h_dim1;
	    tst = (d__1 = h__[i__3].r, abs(d__1)) + (d__2 = d_imag(&h__[k - 1 
		    + (k - 1) * h_dim1]), abs(d__2)) + ((d__3 = h__[i__4].r, 
		    abs(d__3)) + (d__4 = d_imag(&h__[k + k * h_dim1]), abs(
		    d__4)));
	    if (tst == 0.) {
		if (k - 2 >= *ilo) {
		    i__3 = k - 1 + (k - 2) * h_dim1;
		    tst += (d__1 = h__[i__3].r, abs(d__1));
		}
		if (k + 1 <= *ihi) {
		    i__3 = k + 1 + k * h_dim1;
		    tst += (d__1 = h__[i__3].r, abs(d__1));
		}
	    }
/*           ==== The following is a conservative small subdiagonal */
/*           .    deflation criterion due to Ahues & Tisseur (LAWN 122, */
/*           .    1997). It has better mathematical foundation and */
/*           .    improves accuracy in some examples.  ==== */
	    i__3 = k + (k - 1) * h_dim1;
	    if ((d__1 = h__[i__3].r, abs(d__1)) <= ulp * tst) {
/* Computing MAX */
		i__3 = k + (k - 1) * h_dim1;
		i__4 = k - 1 + k * h_dim1;
		d__5 = (d__1 = h__[i__3].r, abs(d__1)) + (d__2 = d_imag(&h__[
			k + (k - 1) * h_dim1]), abs(d__2)), d__6 = (d__3 = 
			h__[i__4].r, abs(d__3)) + (d__4 = d_imag(&h__[k - 1 + 
			k * h_dim1]), abs(d__4));
		ab = f2cmax(d__5,d__6);
/* Computing MIN */
		i__3 = k + (k - 1) * h_dim1;
		i__4 = k - 1 + k * h_dim1;
		d__5 = (d__1 = h__[i__3].r, abs(d__1)) + (d__2 = d_imag(&h__[
			k + (k - 1) * h_dim1]), abs(d__2)), d__6 = (d__3 = 
			h__[i__4].r, abs(d__3)) + (d__4 = d_imag(&h__[k - 1 + 
			k * h_dim1]), abs(d__4));
		ba = f2cmin(d__5,d__6);
		i__3 = k - 1 + (k - 1) * h_dim1;
		i__4 = k + k * h_dim1;
		z__2.r = h__[i__3].r - h__[i__4].r, z__2.i = h__[i__3].i - 
			h__[i__4].i;
		z__1.r = z__2.r, z__1.i = z__2.i;
/* Computing MAX */
		i__5 = k + k * h_dim1;
		d__5 = (d__1 = h__[i__5].r, abs(d__1)) + (d__2 = d_imag(&h__[
			k + k * h_dim1]), abs(d__2)), d__6 = (d__3 = z__1.r, 
			abs(d__3)) + (d__4 = d_imag(&z__1), abs(d__4));
		aa = f2cmax(d__5,d__6);
		i__3 = k - 1 + (k - 1) * h_dim1;
		i__4 = k + k * h_dim1;
		z__2.r = h__[i__3].r - h__[i__4].r, z__2.i = h__[i__3].i - 
			h__[i__4].i;
		z__1.r = z__2.r, z__1.i = z__2.i;
/* Computing MIN */
		i__5 = k + k * h_dim1;
		d__5 = (d__1 = h__[i__5].r, abs(d__1)) + (d__2 = d_imag(&h__[
			k + k * h_dim1]), abs(d__2)), d__6 = (d__3 = z__1.r, 
			abs(d__3)) + (d__4 = d_imag(&z__1), abs(d__4));
		bb = f2cmin(d__5,d__6);
		s = aa + ab;
/* Computing MAX */
		d__1 = smlnum, d__2 = ulp * (bb * (aa / s));
		if (ba * (ab / s) <= f2cmax(d__1,d__2)) {
		    goto L50;
		}
	    }
/* L40: */
	}
L50:
	l = k;
	if (l > *ilo) {

/*           H(L,L-1) is negligible */

	    i__2 = l + (l - 1) * h_dim1;
	    h__[i__2].r = 0., h__[i__2].i = 0.;
	}

/*        Exit from loop if a submatrix of order 1 has split off. */

	if (l >= i__) {
	    goto L140;
	}

/*        Now the active submatrix is in rows and columns L to I. If */
/*        eigenvalues only are being computed, only the active submatrix */
/*        need be transformed. */

	if (! (*wantt)) {
	    i1 = l;
	    i2 = i__;
	}

	if (its == 10) {

/*           Exceptional shift. */

	    i__2 = l + 1 + l * h_dim1;
	    s = (d__1 = h__[i__2].r, abs(d__1)) * .75;
	    i__2 = l + l * h_dim1;
	    z__1.r = s + h__[i__2].r, z__1.i = h__[i__2].i;
	    t.r = z__1.r, t.i = z__1.i;
	} else if (its == 20) {

/*           Exceptional shift. */

	    i__2 = i__ + (i__ - 1) * h_dim1;
	    s = (d__1 = h__[i__2].r, abs(d__1)) * .75;
	    i__2 = i__ + i__ * h_dim1;
	    z__1.r = s + h__[i__2].r, z__1.i = h__[i__2].i;
	    t.r = z__1.r, t.i = z__1.i;
	} else {

/*           Wilkinson's shift. */

	    i__2 = i__ + i__ * h_dim1;
	    t.r = h__[i__2].r, t.i = h__[i__2].i;
	    z_sqrt(&z__2, &h__[i__ - 1 + i__ * h_dim1]);
	    z_sqrt(&z__3, &h__[i__ + (i__ - 1) * h_dim1]);
	    z__1.r = z__2.r * z__3.r - z__2.i * z__3.i, z__1.i = z__2.r * 
		    z__3.i + z__2.i * z__3.r;
	    u.r = z__1.r, u.i = z__1.i;
	    s = (d__1 = u.r, abs(d__1)) + (d__2 = d_imag(&u), abs(d__2));
	    if (s != 0.) {
		i__2 = i__ - 1 + (i__ - 1) * h_dim1;
		z__2.r = h__[i__2].r - t.r, z__2.i = h__[i__2].i - t.i;
		z__1.r = z__2.r * .5, z__1.i = z__2.i * .5;
		x.r = z__1.r, x.i = z__1.i;
		sx = (d__1 = x.r, abs(d__1)) + (d__2 = d_imag(&x), abs(d__2));
/* Computing MAX */
		d__3 = s, d__4 = (d__1 = x.r, abs(d__1)) + (d__2 = d_imag(&x),
			 abs(d__2));
		s = f2cmax(d__3,d__4);
		z__5.r = x.r / s, z__5.i = x.i / s;
		pow_zi(&z__4, &z__5, &c__2);
		z__7.r = u.r / s, z__7.i = u.i / s;
		pow_zi(&z__6, &z__7, &c__2);
		z__3.r = z__4.r + z__6.r, z__3.i = z__4.i + z__6.i;
		z_sqrt(&z__2, &z__3);
		z__1.r = s * z__2.r, z__1.i = s * z__2.i;
		y.r = z__1.r, y.i = z__1.i;
		if (sx > 0.) {
		    z__1.r = x.r / sx, z__1.i = x.i / sx;
		    z__2.r = x.r / sx, z__2.i = x.i / sx;
		    if (z__1.r * y.r + d_imag(&z__2) * d_imag(&y) < 0.) {
			z__3.r = -y.r, z__3.i = -y.i;
			y.r = z__3.r, y.i = z__3.i;
		    }
		}
		z__4.r = x.r + y.r, z__4.i = x.i + y.i;
		zladiv_(&z__3, &u, &z__4);
		z__2.r = u.r * z__3.r - u.i * z__3.i, z__2.i = u.r * z__3.i + 
			u.i * z__3.r;
		z__1.r = t.r - z__2.r, z__1.i = t.i - z__2.i;
		t.r = z__1.r, t.i = z__1.i;
	    }
	}

/*        Look for two consecutive small subdiagonal elements. */

	i__2 = l + 1;
	for (m = i__ - 1; m >= i__2; --m) {

/*           Determine the effect of starting the single-shift QR */
/*           iteration at row M, and see if this would make H(M,M-1) */
/*           negligible. */

	    i__3 = m + m * h_dim1;
	    h11.r = h__[i__3].r, h11.i = h__[i__3].i;
	    i__3 = m + 1 + (m + 1) * h_dim1;
	    h22.r = h__[i__3].r, h22.i = h__[i__3].i;
	    z__1.r = h11.r - t.r, z__1.i = h11.i - t.i;
	    h11s.r = z__1.r, h11s.i = z__1.i;
	    i__3 = m + 1 + m * h_dim1;
	    h21 = h__[i__3].r;
	    s = (d__1 = h11s.r, abs(d__1)) + (d__2 = d_imag(&h11s), abs(d__2))
		     + abs(h21);
	    z__1.r = h11s.r / s, z__1.i = h11s.i / s;
	    h11s.r = z__1.r, h11s.i = z__1.i;
	    h21 /= s;
	    v[0].r = h11s.r, v[0].i = h11s.i;
	    v[1].r = h21, v[1].i = 0.;
	    i__3 = m + (m - 1) * h_dim1;
	    h10 = h__[i__3].r;
	    if (abs(h10) * abs(h21) <= ulp * (((d__1 = h11s.r, abs(d__1)) + (
		    d__2 = d_imag(&h11s), abs(d__2))) * ((d__3 = h11.r, abs(
		    d__3)) + (d__4 = d_imag(&h11), abs(d__4)) + ((d__5 = 
		    h22.r, abs(d__5)) + (d__6 = d_imag(&h22), abs(d__6)))))) {
		goto L70;
	    }
/* L60: */
	}
	i__2 = l + l * h_dim1;
	h11.r = h__[i__2].r, h11.i = h__[i__2].i;
	i__2 = l + 1 + (l + 1) * h_dim1;
	h22.r = h__[i__2].r, h22.i = h__[i__2].i;
	z__1.r = h11.r - t.r, z__1.i = h11.i - t.i;
	h11s.r = z__1.r, h11s.i = z__1.i;
	i__2 = l + 1 + l * h_dim1;
	h21 = h__[i__2].r;
	s = (d__1 = h11s.r, abs(d__1)) + (d__2 = d_imag(&h11s), abs(d__2)) + 
		abs(h21);
	z__1.r = h11s.r / s, z__1.i = h11s.i / s;
	h11s.r = z__1.r, h11s.i = z__1.i;
	h21 /= s;
	v[0].r = h11s.r, v[0].i = h11s.i;
	v[1].r = h21, v[1].i = 0.;
L70:

/*        Single-shift QR step */

	i__2 = i__ - 1;
	for (k = m; k <= i__2; ++k) {

/*           The first iteration of this loop determines a reflection G */
/*           from the vector V and applies it from left and right to H, */
/*           thus creating a nonzero bulge below the subdiagonal. */

/*           Each subsequent iteration determines a reflection G to */
/*           restore the Hessenberg form in the (K-1)th column, and thus */
/*           chases the bulge one step toward the bottom of the active */
/*           submatrix. */

/*           V(2) is always real before the call to ZLARFG, and hence */
/*           after the call T2 ( = T1*V(2) ) is also real. */

	    if (k > m) {
		zcopy_(&c__2, &h__[k + (k - 1) * h_dim1], &c__1, v, &c__1);
	    }
	    zlarfg_(&c__2, v, &v[1], &c__1, &t1);
	    if (k > m) {
		i__3 = k + (k - 1) * h_dim1;
		h__[i__3].r = v[0].r, h__[i__3].i = v[0].i;
		i__3 = k + 1 + (k - 1) * h_dim1;
		h__[i__3].r = 0., h__[i__3].i = 0.;
	    }
	    v2.r = v[1].r, v2.i = v[1].i;
	    z__1.r = t1.r * v2.r - t1.i * v2.i, z__1.i = t1.r * v2.i + t1.i * 
		    v2.r;
	    t2 = z__1.r;

/*           Apply G from the left to transform the rows of the matrix */
/*           in columns K to I2. */

	    i__3 = i2;
	    for (j = k; j <= i__3; ++j) {
		d_cnjg(&z__3, &t1);
		i__4 = k + j * h_dim1;
		z__2.r = z__3.r * h__[i__4].r - z__3.i * h__[i__4].i, z__2.i =
			 z__3.r * h__[i__4].i + z__3.i * h__[i__4].r;
		i__5 = k + 1 + j * h_dim1;
		z__4.r = t2 * h__[i__5].r, z__4.i = t2 * h__[i__5].i;
		z__1.r = z__2.r + z__4.r, z__1.i = z__2.i + z__4.i;
		sum.r = z__1.r, sum.i = z__1.i;
		i__4 = k + j * h_dim1;
		i__5 = k + j * h_dim1;
		z__1.r = h__[i__5].r - sum.r, z__1.i = h__[i__5].i - sum.i;
		h__[i__4].r = z__1.r, h__[i__4].i = z__1.i;
		i__4 = k + 1 + j * h_dim1;
		i__5 = k + 1 + j * h_dim1;
		z__2.r = sum.r * v2.r - sum.i * v2.i, z__2.i = sum.r * v2.i + 
			sum.i * v2.r;
		z__1.r = h__[i__5].r - z__2.r, z__1.i = h__[i__5].i - z__2.i;
		h__[i__4].r = z__1.r, h__[i__4].i = z__1.i;
/* L80: */
	    }

/*           Apply G from the right to transform the columns of the */
/*           matrix in rows I1 to f2cmin(K+2,I). */

/* Computing MIN */
	    i__4 = k + 2;
	    i__3 = f2cmin(i__4,i__);
	    for (j = i1; j <= i__3; ++j) {
		i__4 = j + k * h_dim1;
		z__2.r = t1.r * h__[i__4].r - t1.i * h__[i__4].i, z__2.i = 
			t1.r * h__[i__4].i + t1.i * h__[i__4].r;
		i__5 = j + (k + 1) * h_dim1;
		z__3.r = t2 * h__[i__5].r, z__3.i = t2 * h__[i__5].i;
		z__1.r = z__2.r + z__3.r, z__1.i = z__2.i + z__3.i;
		sum.r = z__1.r, sum.i = z__1.i;
		i__4 = j + k * h_dim1;
		i__5 = j + k * h_dim1;
		z__1.r = h__[i__5].r - sum.r, z__1.i = h__[i__5].i - sum.i;
		h__[i__4].r = z__1.r, h__[i__4].i = z__1.i;
		i__4 = j + (k + 1) * h_dim1;
		i__5 = j + (k + 1) * h_dim1;
		d_cnjg(&z__3, &v2);
		z__2.r = sum.r * z__3.r - sum.i * z__3.i, z__2.i = sum.r * 
			z__3.i + sum.i * z__3.r;
		z__1.r = h__[i__5].r - z__2.r, z__1.i = h__[i__5].i - z__2.i;
		h__[i__4].r = z__1.r, h__[i__4].i = z__1.i;
/* L90: */
	    }

	    if (*wantz) {

/*              Accumulate transformations in the matrix Z */

		i__3 = *ihiz;
		for (j = *iloz; j <= i__3; ++j) {
		    i__4 = j + k * z_dim1;
		    z__2.r = t1.r * z__[i__4].r - t1.i * z__[i__4].i, z__2.i =
			     t1.r * z__[i__4].i + t1.i * z__[i__4].r;
		    i__5 = j + (k + 1) * z_dim1;
		    z__3.r = t2 * z__[i__5].r, z__3.i = t2 * z__[i__5].i;
		    z__1.r = z__2.r + z__3.r, z__1.i = z__2.i + z__3.i;
		    sum.r = z__1.r, sum.i = z__1.i;
		    i__4 = j + k * z_dim1;
		    i__5 = j + k * z_dim1;
		    z__1.r = z__[i__5].r - sum.r, z__1.i = z__[i__5].i - 
			    sum.i;
		    z__[i__4].r = z__1.r, z__[i__4].i = z__1.i;
		    i__4 = j + (k + 1) * z_dim1;
		    i__5 = j + (k + 1) * z_dim1;
		    d_cnjg(&z__3, &v2);
		    z__2.r = sum.r * z__3.r - sum.i * z__3.i, z__2.i = sum.r *
			     z__3.i + sum.i * z__3.r;
		    z__1.r = z__[i__5].r - z__2.r, z__1.i = z__[i__5].i - 
			    z__2.i;
		    z__[i__4].r = z__1.r, z__[i__4].i = z__1.i;
/* L100: */
		}
	    }

	    if (k == m && m > l) {

/*              If the QR step was started at row M > L because two */
/*              consecutive small subdiagonals were found, then extra */
/*              scaling must be performed to ensure that H(M,M-1) remains */
/*              real. */

		z__1.r = 1. - t1.r, z__1.i = 0. - t1.i;
		temp.r = z__1.r, temp.i = z__1.i;
		d__1 = z_abs(&temp);
		z__1.r = temp.r / d__1, z__1.i = temp.i / d__1;
		temp.r = z__1.r, temp.i = z__1.i;
		i__3 = m + 1 + m * h_dim1;
		i__4 = m + 1 + m * h_dim1;
		d_cnjg(&z__2, &temp);
		z__1.r = h__[i__4].r * z__2.r - h__[i__4].i * z__2.i, z__1.i =
			 h__[i__4].r * z__2.i + h__[i__4].i * z__2.r;
		h__[i__3].r = z__1.r, h__[i__3].i = z__1.i;
		if (m + 2 <= i__) {
		    i__3 = m + 2 + (m + 1) * h_dim1;
		    i__4 = m + 2 + (m + 1) * h_dim1;
		    z__1.r = h__[i__4].r * temp.r - h__[i__4].i * temp.i, 
			    z__1.i = h__[i__4].r * temp.i + h__[i__4].i * 
			    temp.r;
		    h__[i__3].r = z__1.r, h__[i__3].i = z__1.i;
		}
		i__3 = i__;
		for (j = m; j <= i__3; ++j) {
		    if (j != m + 1) {
			if (i2 > j) {
			    i__4 = i2 - j;
			    zscal_(&i__4, &temp, &h__[j + (j + 1) * h_dim1], 
				    ldh);
			}
			i__4 = j - i1;
			d_cnjg(&z__1, &temp);
			zscal_(&i__4, &z__1, &h__[i1 + j * h_dim1], &c__1);
			if (*wantz) {
			    d_cnjg(&z__1, &temp);
			    zscal_(&nz, &z__1, &z__[*iloz + j * z_dim1], &
				    c__1);
			}
		    }
/* L110: */
		}
	    }
/* L120: */
	}

/*        Ensure that H(I,I-1) is real. */

	i__2 = i__ + (i__ - 1) * h_dim1;
	temp.r = h__[i__2].r, temp.i = h__[i__2].i;
	if (d_imag(&temp) != 0.) {
	    rtemp = z_abs(&temp);
	    i__2 = i__ + (i__ - 1) * h_dim1;
	    h__[i__2].r = rtemp, h__[i__2].i = 0.;
	    z__1.r = temp.r / rtemp, z__1.i = temp.i / rtemp;
	    temp.r = z__1.r, temp.i = z__1.i;
	    if (i2 > i__) {
		i__2 = i2 - i__;
		d_cnjg(&z__1, &temp);
		zscal_(&i__2, &z__1, &h__[i__ + (i__ + 1) * h_dim1], ldh);
	    }
	    i__2 = i__ - i1;
	    zscal_(&i__2, &temp, &h__[i1 + i__ * h_dim1], &c__1);
	    if (*wantz) {
		zscal_(&nz, &temp, &z__[*iloz + i__ * z_dim1], &c__1);
	    }
	}

/* L130: */
    }

/*     Failure to converge in remaining number of iterations */

    *info = i__;
    return;

L140:

/*     H(I,I-1) is negligible: one eigenvalue has converged. */

    i__1 = i__;
    i__2 = i__ + i__ * h_dim1;
    w[i__1].r = h__[i__2].r, w[i__1].i = h__[i__2].i;

/*     return to start of the main loop with new value of I. */

    i__ = l - 1;
    goto L30;

L150:
    return;

/*     End of ZLAHQR */

} /* zlahqr_ */

