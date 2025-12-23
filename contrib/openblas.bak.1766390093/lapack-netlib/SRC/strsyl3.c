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
#define mycycle_() continue;
#define myceiling_(w) {ceil(w)}
#define myhuge_(w) {HUGE_VAL}
//#define mymaxloc_(w,s,e,n) {if (sizeof(*(w)) == sizeof(double)) dmaxloc_((w),*(s),*(e),n); else dmaxloc_((w),*(s),*(e),n);}
#define mymaxloc_(w,s,e,n) dmaxloc_(w,*(s),*(e),n)
#define myexp_(w) my_expfunc(w)

static int my_expfunc(float *x) {int e; (void)frexpf(*x,&e); return e;}

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
static integer c_n1 = -1;
static real c_b19 = 2.f;
static real c_b31 = -1.f;
static real c_b32 = 1.f;

/* > \brief \b STRSYL3 */

/* Definition: */
/* =========== */


/* >  \par Purpose */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* >  STRSYL3 solves the real Sylvester matrix equation: */
/* > */
/* >     op(A)*X + X*op(B) = scale*C or */
/* >     op(A)*X - X*op(B) = scale*C, */
/* > */
/* >  where op(A) = A or A**T, and  A and B are both upper quasi- */
/* >  triangular. A is M-by-M and B is N-by-N; the right hand side C and */
/* >  the solution X are M-by-N; and scale is an output scale factor, set */
/* >  <= 1 to avoid overflow in X. */
/* > */
/* >  A and B must be in Schur canonical form (as returned by SHSEQR), that */
/* >  is, block upper triangular with 1-by-1 and 2-by-2 diagonal blocks; */
/* >  each 2-by-2 diagonal block has its diagonal elements equal and its */
/* >  off-diagonal elements of opposite sign. */
/* > */
/* >  This is the block version of the algorithm. */
/* > \endverbatim */

/*  Arguments */
/*  ========= */

/* > \param[in] TRANA */
/* > \verbatim */
/* >          TRANA is CHARACTER*1 */
/* >          Specifies the option op(A): */
/* >          = 'N': op(A) = A    (No transpose) */
/* >          = 'T': op(A) = A**T (Transpose) */
/* >          = 'C': op(A) = A**H (Conjugate transpose = Transpose) */
/* > \endverbatim */
/* > */
/* > \param[in] TRANB */
/* > \verbatim */
/* >          TRANB is CHARACTER*1 */
/* >          Specifies the option op(B): */
/* >          = 'N': op(B) = B    (No transpose) */
/* >          = 'T': op(B) = B**T (Transpose) */
/* >          = 'C': op(B) = B**H (Conjugate transpose = Transpose) */
/* > \endverbatim */
/* > */
/* > \param[in] ISGN */
/* > \verbatim */
/* >          ISGN is INTEGER */
/* >          Specifies the sign in the equation: */
/* >          = +1: solve op(A)*X + X*op(B) = scale*C */
/* >          = -1: solve op(A)*X - X*op(B) = scale*C */
/* > \endverbatim */
/* > */
/* > \param[in] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >          The order of the matrix A, and the number of rows in the */
/* >          matrices X and C. M >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrix B, and the number of columns in the */
/* >          matrices X and C. N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] A */
/* > \verbatim */
/* >          A is REAL array, dimension (LDA,M) */
/* >          The upper quasi-triangular matrix A, in Schur canonical form. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of the array A. LDA >= f2cmax(1,M). */
/* > \endverbatim */
/* > */
/* > \param[in] B */
/* > \verbatim */
/* >          B is REAL array, dimension (LDB,N) */
/* >          The upper quasi-triangular matrix B, in Schur canonical form. */
/* > \endverbatim */
/* > */
/* > \param[in] LDB */
/* > \verbatim */
/* >          LDB is INTEGER */
/* >          The leading dimension of the array B. LDB >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in,out] C */
/* > \verbatim */
/* >          C is REAL array, dimension (LDC,N) */
/* >          On entry, the M-by-N right hand side matrix C. */
/* >          On exit, C is overwritten by the solution matrix X. */
/* > \endverbatim */
/* > */
/* > \param[in] LDC */
/* > \verbatim */
/* >          LDC is INTEGER */
/* >          The leading dimension of the array C. LDC >= f2cmax(1,M) */
/* > \endverbatim */
/* > */
/* > \param[out] SCALE */
/* > \verbatim */
/* >          SCALE is REAL */
/* >          The scale factor, scale, set <= 1 to avoid overflow in X. */
/* > \endverbatim */
/* > */
/* > \param[out] IWORK */
/* > \verbatim */
/* >          IWORK is INTEGER array, dimension (MAX(1,LIWORK)) */
/* >          On exit, if INFO = 0, IWORK(1) returns the optimal LIWORK. */
/* > \endverbatim */
/* > */
/* > \param[in] LIWORK */
/* > \verbatim */
/* >          IWORK is INTEGER */
/* >          The dimension of the array IWORK. LIWORK >=  ((M + NB - 1) / NB + 1) */
/* >          + ((N + NB - 1) / NB + 1), where NB is the optimal block size. */
/* > */
/* >          If LIWORK = -1, then a workspace query is assumed; the routine */
/* >          only calculates the optimal dimension of the IWORK array, */
/* >          returns this value as the first entry of the IWORK array, and */
/* >          no error message related to LIWORK is issued by XERBLA. */
/* > \endverbatim */
/* > */
/* > \param[out] SWORK */
/* > \verbatim */
/* >          SWORK is REAL array, dimension (MAX(2, ROWS), */
/* >          MAX(1,COLS)). */
/* >          On exit, if INFO = 0, SWORK(1) returns the optimal value ROWS */
/* >          and SWORK(2) returns the optimal COLS. */
/* > \endverbatim */
/* > */
/* > \param[in] LDSWORK */
/* > \verbatim */
/* >          LDSWORK is INTEGER */
/* >          LDSWORK >= MAX(2,ROWS), where ROWS = ((M + NB - 1) / NB + 1) */
/* >          and NB is the optimal block size. */
/* > */
/* >          If LDSWORK = -1, then a workspace query is assumed; the routine */
/* >          only calculates the optimal dimensions of the SWORK matrix, */
/* >          returns these values as the first and second entry of the SWORK */
/* >          matrix, and no error message related LWORK is issued by XERBLA. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0: successful exit */
/* >          < 0: if INFO = -i, the i-th argument had an illegal value */
/* >          = 1: A and B have common or very close eigenvalues; perturbed */
/* >               values were used to solve the equation (but the matrices */
/* >               A and B are unchanged). */
/* > \endverbatim */

/*  ===================================================================== */
/*  References: */
/*   E. S. Quintana-Orti and R. A. Van De Geijn (2003). Formal derivation of */
/*   algorithms: The triangular Sylvester equation, ACM Transactions */
/*   on Mathematical Software (TOMS), volume 29, pages 218--243. */

/*   A. Schwarz and C. C. Kjelgaard Mikkelsen (2020). Robust Task-Parallel */
/*   Solution of the Triangular Sylvester Equation. Lecture Notes in */
/*   Computer Science, vol 12043, pages 82--92, Springer. */

/*  Contributor: */
/*   Angelika Schwarz, Umea University, Sweden. */

/*  ===================================================================== */
/* Subroutine */ void strsyl3_(char *trana, char *tranb, integer *isgn, 
	integer *m, integer *n, real *a, integer *lda, real *b, integer *ldb, 
	real *c__, integer *ldc, real *scale, integer *iwork, integer *liwork,
	 real *swork, integer *ldswork, integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, c_dim1, c_offset, swork_dim1, 
	    swork_offset, i__1, i__2, i__3, i__4, i__5, i__6;
    real r__1, r__2, r__3;

    /* Local variables */
    real scal, anrm, bnrm, cnrm;
    integer awrk, bwrk;
    logical skip;
    real *wnrm, xnrm;
    integer i__, j, k, l;
    extern logical lsame_(char *, char *);
    integer iinfo;
    extern /* Subroutine */ void sscal_(integer *, real *, real *, integer *), 
	    sgemm_(char *, char *, integer *, integer *, integer *, real *, 
	    real *, integer *, real *, integer *, real *, real *, integer *);
    integer i1, i2, j1, j2, k1, k2, l1;
//    extern integer myexp_(real *);
    integer l2, nb, pc, jj, ll;
    real scaloc;
    extern real slamch_(char *), slange_(char *, integer *, integer *,
	     real *, integer *, real *);
    real scamin;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    extern integer ilaenv_(integer *, char *, char *, integer *, integer *, 
	    integer *, integer *, ftnlen, ftnlen);
    real bignum;
    extern /* Subroutine */ void slascl_(char *, integer *, integer *, real *, 
	    real *, integer *, integer *, real *, integer *, integer *);
    extern real slarmm_(real *, real *, real *);
    logical notrna, notrnb;
    real smlnum;
    logical lquery;
    extern /* Subroutine */ void strsyl_(char *, char *, integer *, integer *, 
	    integer *, real *, integer *, real *, integer *, real *, integer *
	    , real *, integer *);
    integer nba, nbb;
    real buf, sgn;

/*     Decode and Test input parameters */

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
    --iwork;
    swork_dim1 = *ldswork;
    swork_offset = 1 + swork_dim1 * 1;
    swork -= swork_offset;

    /* Function Body */
    notrna = lsame_(trana, "N");
    notrnb = lsame_(tranb, "N");

/*     Use the same block size for all matrices. */

/* Computing MAX */
    i__1 = 8, i__2 = ilaenv_(&c__1, "STRSYL", "", m, n, &c_n1, &c_n1, (ftnlen)
	    6, (ftnlen)0);
    nb = f2cmax(i__1,i__2);

/*     Compute number of blocks in A and B */

/* Computing MAX */
    i__1 = 1, i__2 = (*m + nb - 1) / nb;
    nba = f2cmax(i__1,i__2);
/* Computing MAX */
    i__1 = 1, i__2 = (*n + nb - 1) / nb;
    nbb = f2cmax(i__1,i__2);

/*     Compute workspace */

    *info = 0;
    lquery = *liwork == -1 || *ldswork == -1;
    iwork[1] = nba + nbb + 2;
    if (lquery) {
	*ldswork = 2;
	swork[swork_dim1 + 1] = (real) f2cmax(nba,nbb);
	swork[swork_dim1 + 2] = (real) ((nbb << 1) + nba);
    }

/*     Test the input arguments */

    if (! notrna && ! lsame_(trana, "T") && ! lsame_(
	    trana, "C")) {
	*info = -1;
    } else if (! notrnb && ! lsame_(tranb, "T") && ! 
	    lsame_(tranb, "C")) {
	*info = -2;
    } else if (*isgn != 1 && *isgn != -1) {
	*info = -3;
    } else if (*m < 0) {
	*info = -4;
    } else if (*n < 0) {
	*info = -5;
    } else if (*lda < f2cmax(1,*m)) {
	*info = -7;
    } else if (*ldb < f2cmax(1,*n)) {
	*info = -9;
    } else if (*ldc < f2cmax(1,*m)) {
	*info = -11;
    } else if (! lquery && *liwork < iwork[1]) {
	*info = -14;
    } else if (! lquery && *ldswork < f2cmax(nba,nbb)) {
	*info = -16;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("STRSYL3", &i__1, 7);
	return;
    } else if (lquery) {
	return;
    }

/*     Quick return if possible */

    *scale = 1.f;
    if (*m == 0 || *n == 0) {
	return;
    }

/*     Use unblocked code for small problems or if insufficient */
/*     workspaces are provided */

    if (f2cmin(nba,nbb) == 1 || *ldswork < f2cmax(nba,nbb) || *liwork < iwork[1]) {
	strsyl_(trana, tranb, isgn, m, n, &a[a_offset], lda, &b[b_offset], 
		ldb, &c__[c_offset], ldc, scale, info);
	return;
    }


/*      REAL               WNRM( MAX( M, N ) ) */
    wnrm=(real*)malloc (f2cmax(*m,*n)*sizeof(real));

/*     Set constants to control overflow */

    smlnum = slamch_("S");
    bignum = 1.f / smlnum;

/*      Partition A such that 2-by-2 blocks on the diagonal are not split */

    skip = FALSE_;
    i__1 = nba;
    for (i__ = 1; i__ <= i__1; ++i__) {
	iwork[i__] = (i__ - 1) * nb + 1;
    }
    iwork[nba + 1] = *m + 1;
    i__1 = nba;
    for (k = 1; k <= i__1; ++k) {
	l1 = iwork[k];
	l2 = iwork[k + 1] - 1;
	i__2 = l2;
	for (l = l1; l <= i__2; ++l) {
	    if (skip) {
		skip = FALSE_;
		mycycle_();
	    }
	    if (l >= *m) {
/*               A( M, M ) is a 1-by-1 block */
		mycycle_();
	    }
	    if (a[l + (l + 1) * a_dim1] != 0.f && a[l + 1 + l * a_dim1] != 
		    0.f) {
/*               Check if 2-by-2 block is split */
		if (l + 1 == iwork[k + 1]) {
		    ++iwork[k + 1];
		    mycycle_();
		}
		skip = TRUE_;
	    }
	}
    }
    iwork[nba + 1] = *m + 1;
    if (iwork[nba] >= iwork[nba + 1]) {
	iwork[nba] = iwork[nba + 1];
	--nba;
    }

/*      Partition B such that 2-by-2 blocks on the diagonal are not split */

    pc = nba + 1;
    skip = FALSE_;
    i__1 = nbb;
    for (i__ = 1; i__ <= i__1; ++i__) {
	iwork[pc + i__] = (i__ - 1) * nb + 1;
    }
    iwork[pc + nbb + 1] = *n + 1;
    i__1 = nbb;
    for (k = 1; k <= i__1; ++k) {
	l1 = iwork[pc + k];
	l2 = iwork[pc + k + 1] - 1;
	i__2 = l2;
	for (l = l1; l <= i__2; ++l) {
	    if (skip) {
		skip = FALSE_;
		mycycle_();
	    }
	    if (l >= *n) {
/*               B( N, N ) is a 1-by-1 block */
		mycycle_();
	    }
	    if (b[l + (l + 1) * b_dim1] != 0.f && b[l + 1 + l * b_dim1] != 
		    0.f) {
/*               Check if 2-by-2 block is split */
		if (l + 1 == iwork[pc + k + 1]) {
		    ++iwork[pc + k + 1];
		    mycycle_();
		}
		skip = TRUE_;
	    }
	}
    }
    iwork[pc + nbb + 1] = *n + 1;
    if (iwork[pc + nbb] >= iwork[pc + nbb + 1]) {
	iwork[pc + nbb] = iwork[pc + nbb + 1];
	--nbb;
    }

/*     Set local scaling factors - must never attain zero. */

    i__1 = nbb;
    for (l = 1; l <= i__1; ++l) {
	i__2 = nba;
	for (k = 1; k <= i__2; ++k) {
	    swork[k + l * swork_dim1] = 1.f;
	}
    }

/*     Fallback scaling factor to prevent flushing of SWORK( K, L ) to zero. */
/*     This scaling is to ensure compatibility with TRSYL and may get flushed. */

    buf = 1.f;

/*     Compute upper bounds of blocks of A and B */

    awrk = nbb;
    i__1 = nba;
    for (k = 1; k <= i__1; ++k) {
	k1 = iwork[k];
	k2 = iwork[k + 1];
	i__2 = nba;
	for (l = k; l <= i__2; ++l) {
	    l1 = iwork[l];
	    l2 = iwork[l + 1];
	    if (notrna) {
		i__3 = k2 - k1;
		i__4 = l2 - l1;
		swork[k + (awrk + l) * swork_dim1] = slange_("I", &i__3, &
			i__4, &a[k1 + l1 * a_dim1], lda, wnrm);
	    } else {
		i__3 = k2 - k1;
		i__4 = l2 - l1;
		swork[l + (awrk + k) * swork_dim1] = slange_("1", &i__3, &
			i__4, &a[k1 + l1 * a_dim1], lda, wnrm);
	    }
	}
    }
    bwrk = nbb + nba;
    i__1 = nbb;
    for (k = 1; k <= i__1; ++k) {
	k1 = iwork[pc + k];
	k2 = iwork[pc + k + 1];
	i__2 = nbb;
	for (l = k; l <= i__2; ++l) {
	    l1 = iwork[pc + l];
	    l2 = iwork[pc + l + 1];
	    if (notrnb) {
		i__3 = k2 - k1;
		i__4 = l2 - l1;
		swork[k + (bwrk + l) * swork_dim1] = slange_("I", &i__3, &
			i__4, &b[k1 + l1 * b_dim1], ldb, wnrm);
	    } else {
		i__3 = k2 - k1;
		i__4 = l2 - l1;
		swork[l + (bwrk + k) * swork_dim1] = slange_("1", &i__3, &
			i__4, &b[k1 + l1 * b_dim1], ldb, wnrm);
	    }
	}
    }

    sgn = (real) (*isgn);

    if (notrna && notrnb) {

/*        Solve    A*X + ISGN*X*B = scale*C. */

/*        The (K,L)th block of X is determined starting from */
/*        bottom-left corner column by column by */

/*         A(K,K)*X(K,L) + ISGN*X(K,L)*B(L,L) = C(K,L) - R(K,L) */

/*        Where */
/*                  M                         L-1 */
/*        R(K,L) = SUM [A(K,I)*X(I,L)] + ISGN*SUM [X(K,J)*B(J,L)]. */
/*                I=K+1                       J=1 */

/*        Start loop over block rows (index = K) and block columns (index = L) */

	for (k = nba; k >= 1; --k) {

/*           K1: row index of the first row in X( K, L ) */
/*           K2: row index of the first row in X( K+1, L ) */
/*           so the K2 - K1 is the column count of the block X( K, L ) */

	    k1 = iwork[k];
	    k2 = iwork[k + 1];
	    i__1 = nbb;
	    for (l = 1; l <= i__1; ++l) {

/*              L1: column index of the first column in X( K, L ) */
/*              L2: column index of the first column in X( K, L + 1) */
/*              so that L2 - L1 is the row count of the block X( K, L ) */

		l1 = iwork[pc + l];
		l2 = iwork[pc + l + 1];

		i__2 = k2 - k1;
		i__3 = l2 - l1;
		strsyl_(trana, tranb, isgn, &i__2, &i__3, &a[k1 + k1 * a_dim1]
			, lda, &b[l1 + l1 * b_dim1], ldb, &c__[k1 + l1 * 
			c_dim1], ldc, &scaloc, &iinfo);
		*info = f2cmax(*info,iinfo);

		if (scaloc * swork[k + l * swork_dim1] == 0.f) {
		    if (scaloc == 0.f) {
/*                    The magnitude of the largest entry of X(K1:K2-1, L1:L2-1) */
/*                    is larger than the product of BIGNUM**2 and cannot be */
/*                    represented in the form (1/SCALE)*X(K1:K2-1, L1:L2-1). */
/*                    Mark the computation as pointless. */
			buf = 0.f;
		    } else {
/*                    Use second scaling factor to prevent flushing to zero. */
			i__2 = myexp_(&scaloc);
			buf *= pow_ri(&c_b19, &i__2);
		    }
		    i__2 = nbb;
		    for (jj = 1; jj <= i__2; ++jj) {
			i__3 = nba;
			for (ll = 1; ll <= i__3; ++ll) {
/*                       Bound by BIGNUM to not introduce Inf. The value */
/*                       is irrelevant; corresponding entries of the */
/*                       solution will be flushed in consistency scaling. */
/* Computing MIN */
			    i__4 = myexp_(&scaloc);
			    r__1 = bignum, r__2 = swork[ll + jj * swork_dim1] 
				    / pow_ri(&c_b19, &i__4);
			    swork[ll + jj * swork_dim1] = f2cmin(r__1,r__2);
			}
		    }
		}
		swork[k + l * swork_dim1] = scaloc * swork[k + l * swork_dim1]
			;
		i__2 = k2 - k1;
		i__3 = l2 - l1;
		xnrm = slange_("I", &i__2, &i__3, &c__[k1 + l1 * c_dim1], ldc,
			 wnrm);

		for (i__ = k - 1; i__ >= 1; --i__) {

/*                 C( I, L ) := C( I, L ) - A( I, K ) * C( K, L ) */

		    i1 = iwork[i__];
		    i2 = iwork[i__ + 1];

/*                 Compute scaling factor to survive the linear update */
/*                 simulating consistent scaling. */

		    i__2 = i2 - i1;
		    i__3 = l2 - l1;
		    cnrm = slange_("I", &i__2, &i__3, &c__[i1 + l1 * c_dim1], 
			    ldc, wnrm);
/* Computing MIN */
		    r__1 = swork[i__ + l * swork_dim1], r__2 = swork[k + l * 
			    swork_dim1];
		    scamin = f2cmin(r__1,r__2);
		    cnrm *= scamin / swork[i__ + l * swork_dim1];
		    xnrm *= scamin / swork[k + l * swork_dim1];
		    anrm = swork[i__ + (awrk + k) * swork_dim1];
		    scaloc = slarmm_(&anrm, &xnrm, &cnrm);
		    if (scaloc * scamin == 0.f) {
/*                    Use second scaling factor to prevent flushing to zero. */
			i__2 = myexp_(&scaloc);
			buf *= pow_ri(&c_b19, &i__2);
			i__2 = nbb;
			for (jj = 1; jj <= i__2; ++jj) {
			    i__3 = nba;
			    for (ll = 1; ll <= i__3; ++ll) {
/* Computing MIN */
				i__4 = myexp_(&scaloc);
				r__1 = bignum, r__2 = swork[ll + jj * 
					swork_dim1] / pow_ri(&c_b19, &i__4);
				swork[ll + jj * swork_dim1] = f2cmin(r__1,r__2);
			    }
			}
			i__2 = myexp_(&scaloc);
			scamin /= pow_ri(&c_b19, &i__2);
			i__2 = myexp_(&scaloc);
			scaloc /= pow_ri(&c_b19, &i__2);
		    }
		    cnrm *= scaloc;
		    xnrm *= scaloc;

/*                 Simultaneously apply the robust update factor and the */
/*                 consistency scaling factor to C( I, L ) and C( K, L ). */

		    scal = scamin / swork[k + l * swork_dim1] * scaloc;
		    if (scal != 1.f) {
			i__2 = l2 - 1;
			for (jj = l1; jj <= i__2; ++jj) {
			    i__3 = k2 - k1;
			    sscal_(&i__3, &scal, &c__[k1 + jj * c_dim1], &
				    c__1);
			}
		    }

		    scal = scamin / swork[i__ + l * swork_dim1] * scaloc;
		    if (scal != 1.f) {
			i__2 = l2 - 1;
			for (ll = l1; ll <= i__2; ++ll) {
			    i__3 = i2 - i1;
			    sscal_(&i__3, &scal, &c__[i1 + ll * c_dim1], &
				    c__1);
			}
		    }

/*                 Record current scaling factor */

		    swork[k + l * swork_dim1] = scamin * scaloc;
		    swork[i__ + l * swork_dim1] = scamin * scaloc;

		    i__2 = i2 - i1;
		    i__3 = l2 - l1;
		    i__4 = k2 - k1;
		    sgemm_("N", "N", &i__2, &i__3, &i__4, &c_b31, &a[i1 + k1 *
			     a_dim1], lda, &c__[k1 + l1 * c_dim1], ldc, &
			    c_b32, &c__[i1 + l1 * c_dim1], ldc);

		}

		i__2 = nbb;
		for (j = l + 1; j <= i__2; ++j) {

/*                 C( K, J ) := C( K, J ) - SGN * C( K, L ) * B( L, J ) */

		    j1 = iwork[pc + j];
		    j2 = iwork[pc + j + 1];

/*                 Compute scaling factor to survive the linear update */
/*                 simulating consistent scaling. */

		    i__3 = k2 - k1;
		    i__4 = j2 - j1;
		    cnrm = slange_("I", &i__3, &i__4, &c__[k1 + j1 * c_dim1], 
			    ldc, wnrm);
/* Computing MIN */
		    r__1 = swork[k + j * swork_dim1], r__2 = swork[k + l * 
			    swork_dim1];
		    scamin = f2cmin(r__1,r__2);
		    cnrm *= scamin / swork[k + j * swork_dim1];
		    xnrm *= scamin / swork[k + l * swork_dim1];
		    bnrm = swork[l + (bwrk + j) * swork_dim1];
		    scaloc = slarmm_(&bnrm, &xnrm, &cnrm);
		    if (scaloc * scamin == 0.f) {
/*                    Use second scaling factor to prevent flushing to zero. */
			i__3 = myexp_(&scaloc);
			buf *= pow_ri(&c_b19, &i__3);
			i__3 = nbb;
			for (jj = 1; jj <= i__3; ++jj) {
			    i__4 = nba;
			    for (ll = 1; ll <= i__4; ++ll) {
/* Computing MIN */
				i__5 = myexp_(&scaloc);
				r__1 = bignum, r__2 = swork[ll + jj * 
					swork_dim1] / pow_ri(&c_b19, &i__5);
				swork[ll + jj * swork_dim1] = f2cmin(r__1,r__2);
			    }
			}
			i__3 = myexp_(&scaloc);
			scamin /= pow_ri(&c_b19, &i__3);
			i__3 = myexp_(&scaloc);
			scaloc /= pow_ri(&c_b19, &i__3);
		    }
		    cnrm *= scaloc;
		    xnrm *= scaloc;

/*                 Simultaneously apply the robust update factor and the */
/*                 consistency scaling factor to C( K, J ) and C( K, L). */

		    scal = scamin / swork[k + l * swork_dim1] * scaloc;
		    if (scal != 1.f) {
			i__3 = l2 - 1;
			for (ll = l1; ll <= i__3; ++ll) {
			    i__4 = k2 - k1;
			    sscal_(&i__4, &scal, &c__[k1 + ll * c_dim1], &
				    c__1);
			}
		    }

		    scal = scamin / swork[k + j * swork_dim1] * scaloc;
		    if (scal != 1.f) {
			i__3 = j2 - 1;
			for (jj = j1; jj <= i__3; ++jj) {
			    i__4 = k2 - k1;
			    sscal_(&i__4, &scal, &c__[k1 + jj * c_dim1], &
				    c__1);
			}
		    }

/*                 Record current scaling factor */

		    swork[k + l * swork_dim1] = scamin * scaloc;
		    swork[k + j * swork_dim1] = scamin * scaloc;

		    i__3 = k2 - k1;
		    i__4 = j2 - j1;
		    i__5 = l2 - l1;
		    r__1 = -sgn;
		    sgemm_("N", "N", &i__3, &i__4, &i__5, &r__1, &c__[k1 + l1 
			    * c_dim1], ldc, &b[l1 + j1 * b_dim1], ldb, &c_b32,
			     &c__[k1 + j1 * c_dim1], ldc);
		}
	    }
	}
    } else if (! notrna && notrnb) {

/*        Solve    A**T*X + ISGN*X*B = scale*C. */

/*        The (K,L)th block of X is determined starting from */
/*        upper-left corner column by column by */

/*          A(K,K)**T*X(K,L) + ISGN*X(K,L)*B(L,L) = C(K,L) - R(K,L) */

/*        Where */
/*                   K-1                        L-1 */
/*          R(K,L) = SUM [A(I,K)**T*X(I,L)] +ISGN*SUM [X(K,J)*B(J,L)] */
/*                   I=1                        J=1 */

/*        Start loop over block rows (index = K) and block columns (index = L) */

	i__1 = nba;
	for (k = 1; k <= i__1; ++k) {

/*           K1: row index of the first row in X( K, L ) */
/*           K2: row index of the first row in X( K+1, L ) */
/*           so the K2 - K1 is the column count of the block X( K, L ) */

	    k1 = iwork[k];
	    k2 = iwork[k + 1];
	    i__2 = nbb;
	    for (l = 1; l <= i__2; ++l) {

/*              L1: column index of the first column in X( K, L ) */
/*              L2: column index of the first column in X( K, L + 1) */
/*              so that L2 - L1 is the row count of the block X( K, L ) */

		l1 = iwork[pc + l];
		l2 = iwork[pc + l + 1];

		i__3 = k2 - k1;
		i__4 = l2 - l1;
		strsyl_(trana, tranb, isgn, &i__3, &i__4, &a[k1 + k1 * a_dim1]
			, lda, &b[l1 + l1 * b_dim1], ldb, &c__[k1 + l1 * 
			c_dim1], ldc, &scaloc, &iinfo);
		*info = f2cmax(*info,iinfo);

		if (scaloc * swork[k + l * swork_dim1] == 0.f) {
		    if (scaloc == 0.f) {
/*                    The magnitude of the largest entry of X(K1:K2-1, L1:L2-1) */
/*                    is larger than the product of BIGNUM**2 and cannot be */
/*                    represented in the form (1/SCALE)*X(K1:K2-1, L1:L2-1). */
/*                    Mark the computation as pointless. */
			buf = 0.f;
		    } else {
/*                    Use second scaling factor to prevent flushing to zero. */
			i__3 = myexp_(&scaloc);
			buf *= pow_ri(&c_b19, &i__3);
		    }
		    i__3 = nbb;
		    for (jj = 1; jj <= i__3; ++jj) {
			i__4 = nba;
			for (ll = 1; ll <= i__4; ++ll) {
/*                       Bound by BIGNUM to not introduce Inf. The value */
/*                       is irrelevant; corresponding entries of the */
/*                       solution will be flushed in consistency scaling. */
/* Computing MIN */
			    i__5 = myexp_(&scaloc);
			    r__1 = bignum, r__2 = swork[ll + jj * swork_dim1] 
				    / pow_ri(&c_b19, &i__5);
			    swork[ll + jj * swork_dim1] = f2cmin(r__1,r__2);
			}
		    }
		}
		swork[k + l * swork_dim1] = scaloc * swork[k + l * swork_dim1]
			;
		i__3 = k2 - k1;
		i__4 = l2 - l1;
		xnrm = slange_("I", &i__3, &i__4, &c__[k1 + l1 * c_dim1], ldc,
			 wnrm);

		i__3 = nba;
		for (i__ = k + 1; i__ <= i__3; ++i__) {

/*                 C( I, L ) := C( I, L ) - A( K, I )**T * C( K, L ) */

		    i1 = iwork[i__];
		    i2 = iwork[i__ + 1];

/*                 Compute scaling factor to survive the linear update */
/*                 simulating consistent scaling. */

		    i__4 = i2 - i1;
		    i__5 = l2 - l1;
		    cnrm = slange_("I", &i__4, &i__5, &c__[i1 + l1 * c_dim1], 
			    ldc, wnrm);
/* Computing MIN */
		    r__1 = swork[i__ + l * swork_dim1], r__2 = swork[k + l * 
			    swork_dim1];
		    scamin = f2cmin(r__1,r__2);
		    cnrm *= scamin / swork[i__ + l * swork_dim1];
		    xnrm *= scamin / swork[k + l * swork_dim1];
		    anrm = swork[i__ + (awrk + k) * swork_dim1];
		    scaloc = slarmm_(&anrm, &xnrm, &cnrm);
		    if (scaloc * scamin == 0.f) {
/*                    Use second scaling factor to prevent flushing to zero. */
			i__4 = myexp_(&scaloc);
			buf *= pow_ri(&c_b19, &i__4);
			i__4 = nbb;
			for (jj = 1; jj <= i__4; ++jj) {
			    i__5 = nba;
			    for (ll = 1; ll <= i__5; ++ll) {
/* Computing MIN */
				i__6 = myexp_(&scaloc);
				r__1 = bignum, r__2 = swork[ll + jj * 
					swork_dim1] / pow_ri(&c_b19, &i__6);
				swork[ll + jj * swork_dim1] = f2cmin(r__1,r__2);
			    }
			}
			i__4 = myexp_(&scaloc);
			scamin /= pow_ri(&c_b19, &i__4);
			i__4 = myexp_(&scaloc);
			scaloc /= pow_ri(&c_b19, &i__4);
		    }
		    cnrm *= scaloc;
		    xnrm *= scaloc;

/*                 Simultaneously apply the robust update factor and the */
/*                 consistency scaling factor to to C( I, L ) and C( K, L ). */

		    scal = scamin / swork[k + l * swork_dim1] * scaloc;
		    if (scal != 1.f) {
			i__4 = l2 - 1;
			for (ll = l1; ll <= i__4; ++ll) {
			    i__5 = k2 - k1;
			    sscal_(&i__5, &scal, &c__[k1 + ll * c_dim1], &
				    c__1);
			}
		    }

		    scal = scamin / swork[i__ + l * swork_dim1] * scaloc;
		    if (scal != 1.f) {
			i__4 = l2 - 1;
			for (ll = l1; ll <= i__4; ++ll) {
			    i__5 = i2 - i1;
			    sscal_(&i__5, &scal, &c__[i1 + ll * c_dim1], &
				    c__1);
			}
		    }

/*                 Record current scaling factor */

		    swork[k + l * swork_dim1] = scamin * scaloc;
		    swork[i__ + l * swork_dim1] = scamin * scaloc;

		    i__4 = i2 - i1;
		    i__5 = l2 - l1;
		    i__6 = k2 - k1;
		    sgemm_("T", "N", &i__4, &i__5, &i__6, &c_b31, &a[k1 + i1 *
			     a_dim1], lda, &c__[k1 + l1 * c_dim1], ldc, &
			    c_b32, &c__[i1 + l1 * c_dim1], ldc);
		}

		i__3 = nbb;
		for (j = l + 1; j <= i__3; ++j) {

/*                 C( K, J ) := C( K, J ) - SGN * C( K, L ) * B( L, J ) */

		    j1 = iwork[pc + j];
		    j2 = iwork[pc + j + 1];

/*                 Compute scaling factor to survive the linear update */
/*                 simulating consistent scaling. */

		    i__4 = k2 - k1;
		    i__5 = j2 - j1;
		    cnrm = slange_("I", &i__4, &i__5, &c__[k1 + j1 * c_dim1], 
			    ldc, wnrm);
/* Computing MIN */
		    r__1 = swork[k + j * swork_dim1], r__2 = swork[k + l * 
			    swork_dim1];
		    scamin = f2cmin(r__1,r__2);
		    cnrm *= scamin / swork[k + j * swork_dim1];
		    xnrm *= scamin / swork[k + l * swork_dim1];
		    bnrm = swork[l + (bwrk + j) * swork_dim1];
		    scaloc = slarmm_(&bnrm, &xnrm, &cnrm);
		    if (scaloc * scamin == 0.f) {
/*                    Use second scaling factor to prevent flushing to zero. */
			i__4 = myexp_(&scaloc);
			buf *= pow_ri(&c_b19, &i__4);
			i__4 = nbb;
			for (jj = 1; jj <= i__4; ++jj) {
			    i__5 = nba;
			    for (ll = 1; ll <= i__5; ++ll) {
/* Computing MIN */
				i__6 = myexp_(&scaloc);
				r__1 = bignum, r__2 = swork[ll + jj * 
					swork_dim1] / pow_ri(&c_b19, &i__6);
				swork[ll + jj * swork_dim1] = f2cmin(r__1,r__2);
			    }
			}
			i__4 = myexp_(&scaloc);
			scamin /= pow_ri(&c_b19, &i__4);
			i__4 = myexp_(&scaloc);
			scaloc /= pow_ri(&c_b19, &i__4);
		    }
		    cnrm *= scaloc;
		    xnrm *= scaloc;

/*                 Simultaneously apply the robust update factor and the */
/*                 consistency scaling factor to to C( K, J ) and C( K, L ). */

		    scal = scamin / swork[k + l * swork_dim1] * scaloc;
		    if (scal != 1.f) {
			i__4 = l2 - 1;
			for (ll = l1; ll <= i__4; ++ll) {
			    i__5 = k2 - k1;
			    sscal_(&i__5, &scal, &c__[k1 + ll * c_dim1], &
				    c__1);
			}
		    }

		    scal = scamin / swork[k + j * swork_dim1] * scaloc;
		    if (scal != 1.f) {
			i__4 = j2 - 1;
			for (jj = j1; jj <= i__4; ++jj) {
			    i__5 = k2 - k1;
			    sscal_(&i__5, &scal, &c__[k1 + jj * c_dim1], &
				    c__1);
			}
		    }

/*                 Record current scaling factor */

		    swork[k + l * swork_dim1] = scamin * scaloc;
		    swork[k + j * swork_dim1] = scamin * scaloc;

		    i__4 = k2 - k1;
		    i__5 = j2 - j1;
		    i__6 = l2 - l1;
		    r__1 = -sgn;
		    sgemm_("N", "N", &i__4, &i__5, &i__6, &r__1, &c__[k1 + l1 
			    * c_dim1], ldc, &b[l1 + j1 * b_dim1], ldb, &c_b32,
			     &c__[k1 + j1 * c_dim1], ldc);
		}
	    }
	}
    } else if (! notrna && ! notrnb) {

/*        Solve    A**T*X + ISGN*X*B**T = scale*C. */

/*        The (K,L)th block of X is determined starting from */
/*        top-right corner column by column by */

/*           A(K,K)**T*X(K,L) + ISGN*X(K,L)*B(L,L)**T = C(K,L) - R(K,L) */

/*        Where */
/*                     K-1                          N */
/*            R(K,L) = SUM [A(I,K)**T*X(I,L)] + ISGN*SUM [X(K,J)*B(L,J)**T]. */
/*                     I=1                        J=L+1 */

/*        Start loop over block rows (index = K) and block columns (index = L) */

	i__1 = nba;
	for (k = 1; k <= i__1; ++k) {

/*           K1: row index of the first row in X( K, L ) */
/*           K2: row index of the first row in X( K+1, L ) */
/*           so the K2 - K1 is the column count of the block X( K, L ) */

	    k1 = iwork[k];
	    k2 = iwork[k + 1];
	    for (l = nbb; l >= 1; --l) {

/*              L1: column index of the first column in X( K, L ) */
/*              L2: column index of the first column in X( K, L + 1) */
/*              so that L2 - L1 is the row count of the block X( K, L ) */

		l1 = iwork[pc + l];
		l2 = iwork[pc + l + 1];

		i__2 = k2 - k1;
		i__3 = l2 - l1;
		strsyl_(trana, tranb, isgn, &i__2, &i__3, &a[k1 + k1 * a_dim1]
			, lda, &b[l1 + l1 * b_dim1], ldb, &c__[k1 + l1 * 
			c_dim1], ldc, &scaloc, &iinfo);
		*info = f2cmax(*info,iinfo);

		if (scaloc * swork[k + l * swork_dim1] == 0.f) {
		    if (scaloc == 0.f) {
/*                    The magnitude of the largest entry of X(K1:K2-1, L1:L2-1) */
/*                    is larger than the product of BIGNUM**2 and cannot be */
/*                    represented in the form (1/SCALE)*X(K1:K2-1, L1:L2-1). */
/*                    Mark the computation as pointless. */
			buf = 0.f;
		    } else {
/*                    Use second scaling factor to prevent flushing to zero. */
			i__2 = myexp_(&scaloc);
			buf *= pow_ri(&c_b19, &i__2);
		    }
		    i__2 = nbb;
		    for (jj = 1; jj <= i__2; ++jj) {
			i__3 = nba;
			for (ll = 1; ll <= i__3; ++ll) {
/*                       Bound by BIGNUM to not introduce Inf. The value */
/*                       is irrelevant; corresponding entries of the */
/*                       solution will be flushed in consistency scaling. */
/* Computing MIN */
			    i__4 = myexp_(&scaloc);
			    r__1 = bignum, r__2 = swork[ll + jj * swork_dim1] 
				    / pow_ri(&c_b19, &i__4);
			    swork[ll + jj * swork_dim1] = f2cmin(r__1,r__2);
			}
		    }
		}
		swork[k + l * swork_dim1] = scaloc * swork[k + l * swork_dim1]
			;
		i__2 = k2 - k1;
		i__3 = l2 - l1;
		xnrm = slange_("I", &i__2, &i__3, &c__[k1 + l1 * c_dim1], ldc,
			 wnrm);

		i__2 = nba;
		for (i__ = k + 1; i__ <= i__2; ++i__) {

/*                 C( I, L ) := C( I, L ) - A( K, I )**T * C( K, L ) */

		    i1 = iwork[i__];
		    i2 = iwork[i__ + 1];

/*                 Compute scaling factor to survive the linear update */
/*                 simulating consistent scaling. */

		    i__3 = i2 - i1;
		    i__4 = l2 - l1;
		    cnrm = slange_("I", &i__3, &i__4, &c__[i1 + l1 * c_dim1], 
			    ldc, wnrm);
/* Computing MIN */
		    r__1 = swork[i__ + l * swork_dim1], r__2 = swork[k + l * 
			    swork_dim1];
		    scamin = f2cmin(r__1,r__2);
		    cnrm *= scamin / swork[i__ + l * swork_dim1];
		    xnrm *= scamin / swork[k + l * swork_dim1];
		    anrm = swork[i__ + (awrk + k) * swork_dim1];
		    scaloc = slarmm_(&anrm, &xnrm, &cnrm);
		    if (scaloc * scamin == 0.f) {
/*                    Use second scaling factor to prevent flushing to zero. */
			i__3 = myexp_(&scaloc);
			buf *= pow_ri(&c_b19, &i__3);
			i__3 = nbb;
			for (jj = 1; jj <= i__3; ++jj) {
			    i__4 = nba;
			    for (ll = 1; ll <= i__4; ++ll) {
/* Computing MIN */
				i__5 = myexp_(&scaloc);
				r__1 = bignum, r__2 = swork[ll + jj * 
					swork_dim1] / pow_ri(&c_b19, &i__5);
				swork[ll + jj * swork_dim1] = f2cmin(r__1,r__2);
			    }
			}
			i__3 = myexp_(&scaloc);
			scamin /= pow_ri(&c_b19, &i__3);
			i__3 = myexp_(&scaloc);
			scaloc /= pow_ri(&c_b19, &i__3);
		    }
		    cnrm *= scaloc;
		    xnrm *= scaloc;

/*                 Simultaneously apply the robust update factor and the */
/*                 consistency scaling factor to C( I, L ) and C( K, L ). */

		    scal = scamin / swork[k + l * swork_dim1] * scaloc;
		    if (scal != 1.f) {
			i__3 = l2 - 1;
			for (ll = l1; ll <= i__3; ++ll) {
			    i__4 = k2 - k1;
			    sscal_(&i__4, &scal, &c__[k1 + ll * c_dim1], &
				    c__1);
			}
		    }

		    scal = scamin / swork[i__ + l * swork_dim1] * scaloc;
		    if (scal != 1.f) {
			i__3 = l2 - 1;
			for (ll = l1; ll <= i__3; ++ll) {
			    i__4 = i2 - i1;
			    sscal_(&i__4, &scal, &c__[i1 + ll * c_dim1], &
				    c__1);
			}
		    }

/*                 Record current scaling factor */

		    swork[k + l * swork_dim1] = scamin * scaloc;
		    swork[i__ + l * swork_dim1] = scamin * scaloc;

		    i__3 = i2 - i1;
		    i__4 = l2 - l1;
		    i__5 = k2 - k1;
		    sgemm_("T", "N", &i__3, &i__4, &i__5, &c_b31, &a[k1 + i1 *
			     a_dim1], lda, &c__[k1 + l1 * c_dim1], ldc, &
			    c_b32, &c__[i1 + l1 * c_dim1], ldc);
		}

		i__2 = l - 1;
		for (j = 1; j <= i__2; ++j) {

/*                 C( K, J ) := C( K, J ) - SGN * C( K, L ) * B( J, L )**T */

		    j1 = iwork[pc + j];
		    j2 = iwork[pc + j + 1];

/*                 Compute scaling factor to survive the linear update */
/*                 simulating consistent scaling. */

		    i__3 = k2 - k1;
		    i__4 = j2 - j1;
		    cnrm = slange_("I", &i__3, &i__4, &c__[k1 + j1 * c_dim1], 
			    ldc, wnrm);
/* Computing MIN */
		    r__1 = swork[k + j * swork_dim1], r__2 = swork[k + l * 
			    swork_dim1];
		    scamin = f2cmin(r__1,r__2);
		    cnrm *= scamin / swork[k + j * swork_dim1];
		    xnrm *= scamin / swork[k + l * swork_dim1];
		    bnrm = swork[l + (bwrk + j) * swork_dim1];
		    scaloc = slarmm_(&bnrm, &xnrm, &cnrm);
		    if (scaloc * scamin == 0.f) {
/*                    Use second scaling factor to prevent flushing to zero. */
			i__3 = myexp_(&scaloc);
			buf *= pow_ri(&c_b19, &i__3);
			i__3 = nbb;
			for (jj = 1; jj <= i__3; ++jj) {
			    i__4 = nba;
			    for (ll = 1; ll <= i__4; ++ll) {
/* Computing MIN */
				i__5 = myexp_(&scaloc);
				r__1 = bignum, r__2 = swork[ll + jj * 
					swork_dim1] / pow_ri(&c_b19, &i__5);
				swork[ll + jj * swork_dim1] = f2cmin(r__1,r__2);
			    }
			}
			i__3 = myexp_(&scaloc);
			scamin /= pow_ri(&c_b19, &i__3);
			i__3 = myexp_(&scaloc);
			scaloc /= pow_ri(&c_b19, &i__3);
		    }
		    cnrm *= scaloc;
		    xnrm *= scaloc;

/*                 Simultaneously apply the robust update factor and the */
/*                 consistency scaling factor to C( K, J ) and C( K, L ). */

		    scal = scamin / swork[k + l * swork_dim1] * scaloc;
		    if (scal != 1.f) {
			i__3 = l2 - 1;
			for (ll = l1; ll <= i__3; ++ll) {
			    i__4 = k2 - k1;
			    sscal_(&i__4, &scal, &c__[k1 + ll * c_dim1], &
				    c__1);
			}
		    }

		    scal = scamin / swork[k + j * swork_dim1] * scaloc;
		    if (scal != 1.f) {
			i__3 = j2 - 1;
			for (jj = j1; jj <= i__3; ++jj) {
			    i__4 = k2 - k1;
			    sscal_(&i__4, &scal, &c__[k1 + jj * c_dim1], &
				    c__1);
			}
		    }

/*                 Record current scaling factor */

		    swork[k + l * swork_dim1] = scamin * scaloc;
		    swork[k + j * swork_dim1] = scamin * scaloc;

		    i__3 = k2 - k1;
		    i__4 = j2 - j1;
		    i__5 = l2 - l1;
		    r__1 = -sgn;
		    sgemm_("N", "T", &i__3, &i__4, &i__5, &r__1, &c__[k1 + l1 
			    * c_dim1], ldc, &b[j1 + l1 * b_dim1], ldb, &c_b32,
			     &c__[k1 + j1 * c_dim1], ldc);
		}
	    }
	}
    } else if (notrna && ! notrnb) {

/*        Solve    A*X + ISGN*X*B**T = scale*C. */

/*        The (K,L)th block of X is determined starting from */
/*        bottom-right corner column by column by */

/*            A(K,K)*X(K,L) + ISGN*X(K,L)*B(L,L)**T = C(K,L) - R(K,L) */

/*        Where */
/*                      M                          N */
/*            R(K,L) = SUM [A(K,I)*X(I,L)] + ISGN*SUM [X(K,J)*B(L,J)**T]. */
/*                    I=K+1                      J=L+1 */

/*        Start loop over block rows (index = K) and block columns (index = L) */

	for (k = nba; k >= 1; --k) {

/*           K1: row index of the first row in X( K, L ) */
/*           K2: row index of the first row in X( K+1, L ) */
/*           so the K2 - K1 is the column count of the block X( K, L ) */

	    k1 = iwork[k];
	    k2 = iwork[k + 1];
	    for (l = nbb; l >= 1; --l) {

/*              L1: column index of the first column in X( K, L ) */
/*              L2: column index of the first column in X( K, L + 1) */
/*              so that L2 - L1 is the row count of the block X( K, L ) */

		l1 = iwork[pc + l];
		l2 = iwork[pc + l + 1];

		i__1 = k2 - k1;
		i__2 = l2 - l1;
		strsyl_(trana, tranb, isgn, &i__1, &i__2, &a[k1 + k1 * a_dim1]
			, lda, &b[l1 + l1 * b_dim1], ldb, &c__[k1 + l1 * 
			c_dim1], ldc, &scaloc, &iinfo);
		*info = f2cmax(*info,iinfo);

		if (scaloc * swork[k + l * swork_dim1] == 0.f) {
		    if (scaloc == 0.f) {
/*                    The magnitude of the largest entry of X(K1:K2-1, L1:L2-1) */
/*                    is larger than the product of BIGNUM**2 and cannot be */
/*                    represented in the form (1/SCALE)*X(K1:K2-1, L1:L2-1). */
/*                    Mark the computation as pointless. */
			buf = 0.f;
		    } else {
/*                    Use second scaling factor to prevent flushing to zero. */
			i__1 = myexp_(&scaloc);
			buf *= pow_ri(&c_b19, &i__1);
		    }
		    i__1 = nbb;
		    for (jj = 1; jj <= i__1; ++jj) {
			i__2 = nba;
			for (ll = 1; ll <= i__2; ++ll) {
/*                       Bound by BIGNUM to not introduce Inf. The value */
/*                       is irrelevant; corresponding entries of the */
/*                       solution will be flushed in consistency scaling. */
/* Computing MIN */
			    i__3 = myexp_(&scaloc);
			    r__1 = bignum, r__2 = swork[ll + jj * swork_dim1] 
				    / pow_ri(&c_b19, &i__3);
			    swork[ll + jj * swork_dim1] = f2cmin(r__1,r__2);
			}
		    }
		}
		swork[k + l * swork_dim1] = scaloc * swork[k + l * swork_dim1]
			;
		i__1 = k2 - k1;
		i__2 = l2 - l1;
		xnrm = slange_("I", &i__1, &i__2, &c__[k1 + l1 * c_dim1], ldc,
			 wnrm);

		i__1 = k - 1;
		for (i__ = 1; i__ <= i__1; ++i__) {

/*                 C( I, L ) := C( I, L ) - A( I, K ) * C( K, L ) */

		    i1 = iwork[i__];
		    i2 = iwork[i__ + 1];

/*                 Compute scaling factor to survive the linear update */
/*                 simulating consistent scaling. */

		    i__2 = i2 - i1;
		    i__3 = l2 - l1;
		    cnrm = slange_("I", &i__2, &i__3, &c__[i1 + l1 * c_dim1], 
			    ldc, wnrm);
/* Computing MIN */
		    r__1 = swork[i__ + l * swork_dim1], r__2 = swork[k + l * 
			    swork_dim1];
		    scamin = f2cmin(r__1,r__2);
		    cnrm *= scamin / swork[i__ + l * swork_dim1];
		    xnrm *= scamin / swork[k + l * swork_dim1];
		    anrm = swork[i__ + (awrk + k) * swork_dim1];
		    scaloc = slarmm_(&anrm, &xnrm, &cnrm);
		    if (scaloc * scamin == 0.f) {
/*                    Use second scaling factor to prevent flushing to zero. */
			i__2 = myexp_(&scaloc);
			buf *= pow_ri(&c_b19, &i__2);
			i__2 = nbb;
			for (jj = 1; jj <= i__2; ++jj) {
			    i__3 = nba;
			    for (ll = 1; ll <= i__3; ++ll) {
/* Computing MIN */
				i__4 = myexp_(&scaloc);
				r__1 = bignum, r__2 = swork[ll + jj * 
					swork_dim1] / pow_ri(&c_b19, &i__4);
				swork[ll + jj * swork_dim1] = f2cmin(r__1,r__2);
			    }
			}
			i__2 = myexp_(&scaloc);
			scamin /= pow_ri(&c_b19, &i__2);
			i__2 = myexp_(&scaloc);
			scaloc /= pow_ri(&c_b19, &i__2);
		    }
		    cnrm *= scaloc;
		    xnrm *= scaloc;

/*                 Simultaneously apply the robust update factor and the */
/*                 consistency scaling factor to C( I, L ) and C( K, L ). */

		    scal = scamin / swork[k + l * swork_dim1] * scaloc;
		    if (scal != 1.f) {
			i__2 = l2 - 1;
			for (ll = l1; ll <= i__2; ++ll) {
			    i__3 = k2 - k1;
			    sscal_(&i__3, &scal, &c__[k1 + ll * c_dim1], &
				    c__1);
			}
		    }

		    scal = scamin / swork[i__ + l * swork_dim1] * scaloc;
		    if (scal != 1.f) {
			i__2 = l2 - 1;
			for (ll = l1; ll <= i__2; ++ll) {
			    i__3 = i2 - i1;
			    sscal_(&i__3, &scal, &c__[i1 + ll * c_dim1], &
				    c__1);
			}
		    }

/*                 Record current scaling factor */

		    swork[k + l * swork_dim1] = scamin * scaloc;
		    swork[i__ + l * swork_dim1] = scamin * scaloc;

		    i__2 = i2 - i1;
		    i__3 = l2 - l1;
		    i__4 = k2 - k1;
		    sgemm_("N", "N", &i__2, &i__3, &i__4, &c_b31, &a[i1 + k1 *
			     a_dim1], lda, &c__[k1 + l1 * c_dim1], ldc, &
			    c_b32, &c__[i1 + l1 * c_dim1], ldc);

		}

		i__1 = l - 1;
		for (j = 1; j <= i__1; ++j) {

/*                 C( K, J ) := C( K, J ) - SGN * C( K, L ) * B( J, L )**T */

		    j1 = iwork[pc + j];
		    j2 = iwork[pc + j + 1];

/*                 Compute scaling factor to survive the linear update */
/*                 simulating consistent scaling. */

		    i__2 = k2 - k1;
		    i__3 = j2 - j1;
		    cnrm = slange_("I", &i__2, &i__3, &c__[k1 + j1 * c_dim1], 
			    ldc, wnrm);
/* Computing MIN */
		    r__1 = swork[k + j * swork_dim1], r__2 = swork[k + l * 
			    swork_dim1];
		    scamin = f2cmin(r__1,r__2);
		    cnrm *= scamin / swork[k + j * swork_dim1];
		    xnrm *= scamin / swork[k + l * swork_dim1];
		    bnrm = swork[l + (bwrk + j) * swork_dim1];
		    scaloc = slarmm_(&bnrm, &xnrm, &cnrm);
		    if (scaloc * scamin == 0.f) {
/*                    Use second scaling factor to prevent flushing to zero. */
			i__2 = myexp_(&scaloc);
			buf *= pow_ri(&c_b19, &i__2);
			i__2 = nbb;
			for (jj = 1; jj <= i__2; ++jj) {
			    i__3 = nba;
			    for (ll = 1; ll <= i__3; ++ll) {
/* Computing MIN */
				i__4 = myexp_(&scaloc);
				r__1 = bignum, r__2 = swork[ll + jj * 
					swork_dim1] / pow_ri(&c_b19, &i__4);
				swork[ll + jj * swork_dim1] = f2cmin(r__1,r__2);
			    }
			}
			i__2 = myexp_(&scaloc);
			scamin /= pow_ri(&c_b19, &i__2);
			i__2 = myexp_(&scaloc);
			scaloc /= pow_ri(&c_b19, &i__2);
		    }
		    cnrm *= scaloc;
		    xnrm *= scaloc;

/*                 Simultaneously apply the robust update factor and the */
/*                 consistency scaling factor to C( K, J ) and C( K, L ). */

		    scal = scamin / swork[k + l * swork_dim1] * scaloc;
		    if (scal != 1.f) {
			i__2 = l2 - 1;
			for (jj = l1; jj <= i__2; ++jj) {
			    i__3 = k2 - k1;
			    sscal_(&i__3, &scal, &c__[k1 + jj * c_dim1], &
				    c__1);
			}
		    }

		    scal = scamin / swork[k + j * swork_dim1] * scaloc;
		    if (scal != 1.f) {
			i__2 = j2 - 1;
			for (jj = j1; jj <= i__2; ++jj) {
			    i__3 = k2 - k1;
			    sscal_(&i__3, &scal, &c__[k1 + jj * c_dim1], &
				    c__1);
			}
		    }

/*                 Record current scaling factor */

		    swork[k + l * swork_dim1] = scamin * scaloc;
		    swork[k + j * swork_dim1] = scamin * scaloc;

		    i__2 = k2 - k1;
		    i__3 = j2 - j1;
		    i__4 = l2 - l1;
		    r__1 = -sgn;
		    sgemm_("N", "T", &i__2, &i__3, &i__4, &r__1, &c__[k1 + l1 
			    * c_dim1], ldc, &b[j1 + l1 * b_dim1], ldb, &c_b32,
			     &c__[k1 + j1 * c_dim1], ldc);
		}
	    }
	}

    }

    free(wnrm);
/*     Reduce local scaling factors */

    *scale = swork[swork_dim1 + 1];
    i__1 = nba;
    for (k = 1; k <= i__1; ++k) {
	i__2 = nbb;
	for (l = 1; l <= i__2; ++l) {
/* Computing MIN */
	    r__1 = *scale, r__2 = swork[k + l * swork_dim1];
	    *scale = f2cmin(r__1,r__2);
	}
    }

    if (*scale == 0.f) {

/*        The magnitude of the largest entry of the solution is larger */
/*        than the product of BIGNUM**2 and cannot be represented in the */
/*        form (1/SCALE)*X if SCALE is REAL. Set SCALE to zero and give up. */

	iwork[1] = nba + nbb + 2;
	swork[swork_dim1 + 1] = (real) f2cmax(nba,nbb);
	swork[swork_dim1 + 2] = (real) ((nbb << 1) + nba);
	return;
    }

/*     Realize consistent scaling */

    i__1 = nba;
    for (k = 1; k <= i__1; ++k) {
	k1 = iwork[k];
	k2 = iwork[k + 1];
	i__2 = nbb;
	for (l = 1; l <= i__2; ++l) {
	    l1 = iwork[pc + l];
	    l2 = iwork[pc + l + 1];
	    scal = *scale / swork[k + l * swork_dim1];
	    if (scal != 1.f) {
		i__3 = l2 - 1;
		for (ll = l1; ll <= i__3; ++ll) {
		    i__4 = k2 - k1;
		    sscal_(&i__4, &scal, &c__[k1 + ll * c_dim1], &c__1);
		}
	    }
	}
    }

    if (buf != 1.f && buf > 0.f) {

/*        Decrease SCALE as much as possible. */

/* Computing MIN */
	r__1 = *scale / smlnum, r__2 = 1.f / buf;
	scaloc = f2cmin(r__1,r__2);
	buf *= scaloc;
	*scale /= scaloc;
    }
    if (buf != 1.f && buf > 0.f) {

/*        In case of overly aggressive scaling during the computation, */
/*        flushing of the global scale factor may be prevented by */
/*        undoing some of the scaling. This step is to ensure that */
/*        this routine flushes only scale factors that TRSYL also */
/*        flushes and be usable as a drop-in replacement. */

/*        How much can the normwise largest entry be upscaled? */

	scal = c__[c_dim1 + 1];
	i__1 = *m;
	for (k = 1; k <= i__1; ++k) {
	    i__2 = *n;
	    for (l = 1; l <= i__2; ++l) {
/* Computing MAX */
		r__2 = scal, r__3 = (r__1 = c__[k + l * c_dim1], abs(r__1));
		scal = f2cmax(r__2,r__3);
	    }
	}

/*        Increase BUF as close to 1 as possible and apply scaling. */

/* Computing MIN */
	r__1 = bignum / scal, r__2 = 1.f / buf;
	scaloc = f2cmin(r__1,r__2);
	buf *= scaloc;
	slascl_("G", &c_n1, &c_n1, &c_b32, &scaloc, m, n, &c__[c_offset], ldc,
		 &iwork[1]);
    }

/*     Combine with buffer scaling factor. SCALE will be flushed if */
/*     BUF is less than one here. */

    *scale *= buf;

/*     Restore workspace dimensions */

    iwork[1] = nba + nbb + 2;
    swork[swork_dim1 + 1] = (real) f2cmax(nba,nbb);
    swork[swork_dim1 + 2] = (real) ((nbb << 1) + nba);

    return;

/*     End of STRSYL3 */

} /* strsyl3_ */

