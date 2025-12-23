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



/*  -- translated by f2c (version 20000121).
   You must link the resulting object file with the libraries:
	-lf2c -lm   (in that order)
*/



/* Table of constant values */

static integer c__1 = 1;

/* Subroutine */ int slaqp2rk_(integer *m, integer *n, integer *nrhs, integer 
	*ioffset, integer *kmax, real *abstol, real *reltol, integer *kp1, 
	real *maxc2nrm, real *a, integer *lda, integer *k, real *maxc2nrmk, 
	real *relmaxc2nrmk, integer *jpiv, real *tau, real *vn1, real *vn2, 
	real *work, integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, i__1, i__2, i__3;
    real r__1, r__2;

    /* Local variables */
    real aikk, temp, temp2;
    extern real snrm2_(integer *, real *, integer *);
    integer i__, j;
    real tol3z;
    integer jmaxc2nrm;
    extern /* Subroutine */ int slarf_(char *, integer *, integer *, real *, 
	    integer *, real *, real *, integer *, real *);
    integer itemp, minmnfact;
    extern /* Subroutine */ int sswap_(integer *, real *, integer *, real *, 
	    integer *);
    real myhugeval;
    integer minmnupdt, kk, kp;
    extern real slamch_(char *);
    extern /* Subroutine */ int slarfg_(integer *, real *, real *, integer *, 
	    real *);
    extern integer isamax_(integer *, real *, integer *);
    extern logical sisnan_(real *);


/*  -- LAPACK auxiliary routine -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */


/*  ===================================================================== */


/*     Initialize INFO */

    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    --jpiv;
    --tau;
    --vn1;
    --vn2;
    --work;

    /* Function Body */
    *info = 0;

/*     MINMNFACT in the smallest dimension of the submatrix */
/*     A(IOFFSET+1:M,1:N) to be factorized. */

/*     MINMNUPDT is the smallest dimension */
/*     of the subarray A(IOFFSET+1:M,1:N+NRHS) to be udated, which */
/*     contains the submatrices A(IOFFSET+1:M,1:N) and */
/*     B(IOFFSET+1:M,1:NRHS) as column blocks. */

/* Computing MIN */
    i__1 = *m - *ioffset;
    minmnfact = f2cmin(i__1,*n);
/* Computing MIN */
    i__1 = *m - *ioffset, i__2 = *n + *nrhs;
    minmnupdt = f2cmin(i__1,i__2);
    *kmax = f2cmin(*kmax,minmnfact);
    tol3z = sqrt(slamch_("Epsilon"));
    myhugeval = slamch_("Overflow");

/*     Compute the factorization, KK is the lomn loop index. */

    i__1 = *kmax;
    for (kk = 1; kk <= i__1; ++kk) {

	i__ = *ioffset + kk;

	if (i__ == 1) {

/*           ============================================================ */

/*           We are at the first column of the original whole matrix A, */
/*           therefore we use the computed KP1 and MAXC2NRM from the */
/*           main routine. */

	    kp = *kp1;

/*           ============================================================ */

	} else {

/*           ============================================================ */

/*           Determine the pivot column in KK-th step, i.e. the index */
/*           of the column with the maximum 2-norm in the */
/*           submatrix A(I:M,K:N). */

	    i__2 = *n - kk + 1;
	    kp = kk - 1 + isamax_(&i__2, &vn1[kk], &c__1);

/*           Determine the maximum column 2-norm and the relative maximum */
/*           column 2-norm of the submatrix A(I:M,KK:N) in step KK. */
/*           RELMAXC2NRMK  will be computed later, after somecondition */
/*           checks on MAXC2NRMK. */

	    *maxc2nrmk = vn1[kp];

/*           ============================================================ */

/*           Check if the submatrix A(I:M,KK:N) contains NaN, and set */
/*           INFO parameter to the column number, where the first NaN */
/*           is found and return from the routine. */
/*           We need to check the condition only if the */
/*           column index (same as row index) of the original whole */
/*           matrix is larger than 1, since the condition for whole */
/*           original matrix is checked in the main routine. */

	    if (sisnan_(maxc2nrmk)) {

/*              Set K, the number of factorized columns. */
/*              that are not zero. */

		*k = kk - 1;
		*info = *k + kp;

/*               Set RELMAXC2NRMK to NaN. */

		*relmaxc2nrmk = *maxc2nrmk;

/*               Array TAU(K+1:MINMNFACT) is not set and contains */
/*               undefined elements. */

		return 0;
	    }

/*           ============================================================ */

/*           Quick return, if the submatrix A(I:M,KK:N) is */
/*           a zero matrix. */
/*           We need to check the condition only if the */
/*           column index (same as row index) of the original whole */
/*           matrix is larger than 1, since the condition for whole */
/*           original matrix is checked in the main routine. */

	    if (*maxc2nrmk == 0.f) {

/*              Set K, the number of factorized columns. */
/*              that are not zero. */

		*k = kk - 1;
		*relmaxc2nrmk = 0.f;

/*              Set TAUs corresponding to the columns that were not */
/*              factorized to ZERO, i.e. set TAU(KK:MINMNFACT) to ZERO. */

		i__2 = minmnfact;
		for (j = kk; j <= i__2; ++j) {
		    tau[j] = 0.f;
		}

/*              Return from the routine. */

		return 0;

	    }

/*           ============================================================ */

/*           Check if the submatrix A(I:M,KK:N) contains Inf, */
/*           set INFO parameter to the column number, where */
/*           the first Inf is found plus N, and continue */
/*           the computation. */
/*           We need to check the condition only if the */
/*           column index (same as row index) of the original whole */
/*           matrix is larger than 1, since the condition for whole */
/*           original matrix is checked in the main routine. */

	    if (*info == 0 && *maxc2nrmk > myhugeval) {
		*info = *n + kk - 1 + kp;
	    }

/*           ============================================================ */

/*           Test for the second and third stopping criteria. */
/*           NOTE: There is no need to test for ABSTOL >= ZERO, since */
/*           MAXC2NRMK is non-negative. Similarly, there is no need */
/*           to test for RELTOL >= ZERO, since RELMAXC2NRMK is */
/*           non-negative. */
/*           We need to check the condition only if the */
/*           column index (same as row index) of the original whole */
/*           matrix is larger than 1, since the condition for whole */
/*           original matrix is checked in the main routine. */
	    *relmaxc2nrmk = *maxc2nrmk / *maxc2nrm;

	    if (*maxc2nrmk <= *abstol || *relmaxc2nrmk <= *reltol) {

/*              Set K, the number of factorized columns. */

		*k = kk - 1;

/*              Set TAUs corresponding to the columns that were not */
/*              factorized to ZERO, i.e. set TAU(KK:MINMNFACT) to ZERO. */

		i__2 = minmnfact;
		for (j = kk; j <= i__2; ++j) {
		    tau[j] = 0.f;
		}

/*              Return from the routine. */

		return 0;

	    }

/*           ============================================================ */

/*           End ELSE of IF(I.EQ.1) */

	}

/*        =============================================================== */

/*        If the pivot column is not the first column of the */
/*        subblock A(1:M,KK:N): */
/*        1) swap the KK-th column and the KP-th pivot column */
/*           in A(1:M,1:N); */
/*        2) copy the KK-th element into the KP-th element of the partial */
/*           and exact 2-norm vectors VN1 and VN2. ( Swap is not needed */
/*           for VN1 and VN2 since we use the element with the index */
/*           larger than KK in the next loop step.) */
/*        3) Save the pivot interchange with the indices relative to the */
/*           the original matrix A, not the block A(1:M,1:N). */

	if (kp != kk) {
	    sswap_(m, &a[kp * a_dim1 + 1], &c__1, &a[kk * a_dim1 + 1], &c__1);
	    vn1[kp] = vn1[kk];
	    vn2[kp] = vn2[kk];
	    itemp = jpiv[kp];
	    jpiv[kp] = jpiv[kk];
	    jpiv[kk] = itemp;
	}

/*        Generate elementary reflector H(KK) using the column A(I:M,KK), */
/*        if the column has more than one element, otherwise */
/*        the elementary reflector would be an identity matrix, */
/*        and TAU(KK) = ZERO. */

	if (i__ < *m) {
	    i__2 = *m - i__ + 1;
	    slarfg_(&i__2, &a[i__ + kk * a_dim1], &a[i__ + 1 + kk * a_dim1], &
		    c__1, &tau[kk]);
	} else {
	    tau[kk] = 0.f;
	}

/*        Check if TAU(KK) contains NaN, set INFO parameter */
/*        to the column number where NaN is found and return from */
/*        the routine. */
/*        NOTE: There is no need to check TAU(KK) for Inf, */
/*        since SLARFG cannot produce TAU(KK) or Householder vector */
/*        below the diagonal containing Inf. Only BETA on the diagonal, */
/*        returned by SLARFG can contain Inf, which requires */
/*        TAU(KK) to contain NaN. Therefore, this case of generating Inf */
/*        by SLARFG is covered by checking TAU(KK) for NaN. */

	if (sisnan_(&tau[kk])) {
	    *k = kk - 1;
	    *info = kk;

/*           Set MAXC2NRMK and  RELMAXC2NRMK to NaN. */

	    *maxc2nrmk = tau[kk];
	    *relmaxc2nrmk = tau[kk];

/*           Array TAU(KK:MINMNFACT) is not set and contains */
/*           undefined elements, except the first element TAU(KK) = NaN. */

	    return 0;
	}

/*        Apply H(KK)**T to A(I:M,KK+1:N+NRHS) from the left. */
/*        ( If M >= N, then at KK = N there is no residual matrix, */
/*         i.e. no columns of A to update, only columns of B. */
/*         If M < N, then at KK = M-IOFFSET, I = M and we have a */
/*         one-row residual matrix in A and the elementary */
/*         reflector is a unit matrix, TAU(KK) = ZERO, i.e. no update */
/*         is needed for the residual matrix in A and the */
/*         right-hand-side-matrix in B. */
/*         Therefore, we update only if */
/*         KK < MINMNUPDT = f2cmin(M-IOFFSET, N+NRHS) */
/*         condition is satisfied, not only KK < N+NRHS ) */

	if (kk < minmnupdt) {
	    aikk = a[i__ + kk * a_dim1];
	    a[i__ + kk * a_dim1] = 1.f;
	    i__2 = *m - i__ + 1;
	    i__3 = *n + *nrhs - kk;
	    slarf_("Left", &i__2, &i__3, &a[i__ + kk * a_dim1], &c__1, &tau[
		    kk], &a[i__ + (kk + 1) * a_dim1], lda, &work[1]);
	    a[i__ + kk * a_dim1] = aikk;
	}

	if (kk < minmnfact) {

/*           Update the partial column 2-norms for the residual matrix, */
/*           only if the residual matrix A(I+1:M,KK+1:N) exists, i.e. */
/*           when KK < f2cmin(M-IOFFSET, N). */

	    i__2 = *n;
	    for (j = kk + 1; j <= i__2; ++j) {
		if (vn1[j] != 0.f) {

/*                 NOTE: The following lines follow from the analysis in */
/*                 Lapack Working Note 176. */

/* Computing 2nd power */
		    r__2 = (r__1 = a[i__ + j * a_dim1], abs(r__1)) / vn1[j];
		    temp = 1.f - r__2 * r__2;
		    temp = f2cmax(temp,0.f);
/* Computing 2nd power */
		    r__1 = vn1[j] / vn2[j];
		    temp2 = temp * (r__1 * r__1);
		    if (temp2 <= tol3z) {

/*                    Compute the column 2-norm for the partial */
/*                    column A(I+1:M,J) by explicitly computing it, */
/*                    and store it in both partial 2-norm vector VN1 */
/*                    and exact column 2-norm vector VN2. */

			i__3 = *m - i__;
			vn1[j] = snrm2_(&i__3, &a[i__ + 1 + j * a_dim1], &
				c__1);
			vn2[j] = vn1[j];

		    } else {

/*                    Update the column 2-norm for the partial */
/*                    column A(I+1:M,J) by removing one */
/*                    element A(I,J) and store it in partial */
/*                    2-norm vector VN1. */

			vn1[j] *= sqrt(temp);

		    }
		}
	    }

	}

/*     End factorization loop */

    }

/*     If we reached this point, all colunms have been factorized, */
/*     i.e. no condition was triggered to exit the routine. */
/*     Set the number of factorized columns. */

    *k = *kmax;

/*     We reached the end of the loop, i.e. all KMAX columns were */
/*     factorized, we need to set MAXC2NRMK and RELMAXC2NRMK before */
/*     we return. */

    if (*k < minmnfact) {

	i__1 = *n - *k;
	jmaxc2nrm = *k + isamax_(&i__1, &vn1[*k + 1], &c__1);
	*maxc2nrmk = vn1[jmaxc2nrm];

	if (*k == 0) {
	    *relmaxc2nrmk = 1.f;
	} else {
	    *relmaxc2nrmk = *maxc2nrmk / *maxc2nrm;
	}

    } else {
	*maxc2nrmk = 0.f;
	*relmaxc2nrmk = 0.f;
    }

/*     We reached the end of the loop, i.e. all KMAX columns were */
/*     factorized, set TAUs corresponding to the columns that were */
/*     not factorized to ZERO, i.e. TAU(K+1:MINMNFACT) set to ZERO. */

    i__1 = minmnfact;
    for (j = *k + 1; j <= i__1; ++j) {
	tau[j] = 0.f;
    }

    return 0;

/*     End of SLAQP2RK */

} /* slaqp2rk_ */

