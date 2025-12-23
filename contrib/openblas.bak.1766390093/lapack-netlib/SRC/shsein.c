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
#define s_cat(lpp, rpp, rnp, np, llp) { 	ftnlen i, nc, ll; char *f__rp, *lp; 	ll = (llp); lp = (lpp); 	for(i=0; i < (int)*(np); ++i) {         	nc = ll; 	        if((rnp)[i] < nc) nc = (rnp)[i]; 	        ll -= nc;         	f__rp = (rpp)[i]; 	        while(--nc >= 0) *lp++ = *(f__rp)++;         } 	while(--ll >= 0) *lp++ = ' '; }
#define s_cmp(a,b,c,d) ((integer)strncmp((a),(b),f2cmin((c),(d))))
#define s_copy(A,B,C,D) { int __i,__m; for (__i=0, __m=f2cmin((C),(D)); __i<__m && (B)[__i] != 0; ++__i) (A)[__i] = (B)[__i]; }
#define sig_die(s, kill) { exit(1); }
#define s_stop(s, n) {exit(0);}
#define z_abs(z) (cabs(Cd(z)))
#define z_exp(R, Z) {pCd(R) = cexp(Cd(Z));}
#define z_sqrt(R, Z) {pCd(R) = csqrt(Cd(Z));}
#define myexit_() break;
#define mycycle() continue;
#define myceiling(w) {ceil(w)}
#define myhuge(w) {HUGE_VAL}
//#define mymaxloc_(w,s,e,n) {if (sizeof(*(w)) == sizeof(double)) dmaxloc_((w),*(s),*(e),n); else dmaxloc_((w),*(s),*(e),n);}
#define mymaxloc(w,s,e,n) {dmaxloc_(w,*(s),*(e),n)}

/*  -- translated by f2c (version 20000121).
   You must link the resulting object file with the libraries:
	-lf2c -lm   (in that order)
*/




/* Table of constant values */

static logical c_false = FALSE_;
static logical c_true = TRUE_;

/* > \brief \b SHSEIN */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download SHSEIN + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/shsein.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/shsein.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/shsein.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE SHSEIN( SIDE, EIGSRC, INITV, SELECT, N, H, LDH, WR, WI, */
/*                          VL, LDVL, VR, LDVR, MM, M, WORK, IFAILL, */
/*                          IFAILR, INFO ) */

/*       CHARACTER          EIGSRC, INITV, SIDE */
/*       INTEGER            INFO, LDH, LDVL, LDVR, M, MM, N */
/*       LOGICAL            SELECT( * ) */
/*       INTEGER            IFAILL( * ), IFAILR( * ) */
/*       REAL               H( LDH, * ), VL( LDVL, * ), VR( LDVR, * ), */
/*      $                   WI( * ), WORK( * ), WR( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > SHSEIN uses inverse iteration to find specified right and/or left */
/* > eigenvectors of a real upper Hessenberg matrix H. */
/* > */
/* > The right eigenvector x and the left eigenvector y of the matrix H */
/* > corresponding to an eigenvalue w are defined by: */
/* > */
/* >              H * x = w * x,     y**h * H = w * y**h */
/* > */
/* > where y**h denotes the conjugate transpose of the vector y. */
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
/* > \param[in] EIGSRC */
/* > \verbatim */
/* >          EIGSRC is CHARACTER*1 */
/* >          Specifies the source of eigenvalues supplied in (WR,WI): */
/* >          = 'Q': the eigenvalues were found using SHSEQR; thus, if */
/* >                 H has zero subdiagonal elements, and so is */
/* >                 block-triangular, then the j-th eigenvalue can be */
/* >                 assumed to be an eigenvalue of the block containing */
/* >                 the j-th row/column.  This property allows SHSEIN to */
/* >                 perform inverse iteration on just one diagonal block. */
/* >          = 'N': no assumptions are made on the correspondence */
/* >                 between eigenvalues and diagonal blocks.  In this */
/* >                 case, SHSEIN must always perform inverse iteration */
/* >                 using the whole matrix H. */
/* > \endverbatim */
/* > */
/* > \param[in] INITV */
/* > \verbatim */
/* >          INITV is CHARACTER*1 */
/* >          = 'N': no initial vectors are supplied; */
/* >          = 'U': user-supplied initial vectors are stored in the arrays */
/* >                 VL and/or VR. */
/* > \endverbatim */
/* > */
/* > \param[in,out] SELECT */
/* > \verbatim */
/* >          SELECT is LOGICAL array, dimension (N) */
/* >          Specifies the eigenvectors to be computed. To select the */
/* >          real eigenvector corresponding to a real eigenvalue WR(j), */
/* >          SELECT(j) must be set to .TRUE.. To select the complex */
/* >          eigenvector corresponding to a complex eigenvalue */
/* >          (WR(j),WI(j)), with complex conjugate (WR(j+1),WI(j+1)), */
/* >          either SELECT(j) or SELECT(j+1) or both must be set to */
/* >          .TRUE.; then on exit SELECT(j) is .TRUE. and SELECT(j+1) is */
/* >          .FALSE.. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrix H.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] H */
/* > \verbatim */
/* >          H is REAL array, dimension (LDH,N) */
/* >          The upper Hessenberg matrix H. */
/* >          If a NaN is detected in H, the routine will return with INFO=-6. */
/* > \endverbatim */
/* > */
/* > \param[in] LDH */
/* > \verbatim */
/* >          LDH is INTEGER */
/* >          The leading dimension of the array H.  LDH >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in,out] WR */
/* > \verbatim */
/* >          WR is REAL array, dimension (N) */
/* > \endverbatim */
/* > */
/* > \param[in] WI */
/* > \verbatim */
/* >          WI is REAL array, dimension (N) */
/* > */
/* >          On entry, the real and imaginary parts of the eigenvalues of */
/* >          H; a complex conjugate pair of eigenvalues must be stored in */
/* >          consecutive elements of WR and WI. */
/* >          On exit, WR may have been altered since close eigenvalues */
/* >          are perturbed slightly in searching for independent */
/* >          eigenvectors. */
/* > \endverbatim */
/* > */
/* > \param[in,out] VL */
/* > \verbatim */
/* >          VL is REAL array, dimension (LDVL,MM) */
/* >          On entry, if INITV = 'U' and SIDE = 'L' or 'B', VL must */
/* >          contain starting vectors for the inverse iteration for the */
/* >          left eigenvectors; the starting vector for each eigenvector */
/* >          must be in the same column(s) in which the eigenvector will */
/* >          be stored. */
/* >          On exit, if SIDE = 'L' or 'B', the left eigenvectors */
/* >          specified by SELECT will be stored consecutively in the */
/* >          columns of VL, in the same order as their eigenvalues. A */
/* >          complex eigenvector corresponding to a complex eigenvalue is */
/* >          stored in two consecutive columns, the first holding the real */
/* >          part and the second the imaginary part. */
/* >          If SIDE = 'R', VL is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDVL */
/* > \verbatim */
/* >          LDVL is INTEGER */
/* >          The leading dimension of the array VL. */
/* >          LDVL >= f2cmax(1,N) if SIDE = 'L' or 'B'; LDVL >= 1 otherwise. */
/* > \endverbatim */
/* > */
/* > \param[in,out] VR */
/* > \verbatim */
/* >          VR is REAL array, dimension (LDVR,MM) */
/* >          On entry, if INITV = 'U' and SIDE = 'R' or 'B', VR must */
/* >          contain starting vectors for the inverse iteration for the */
/* >          right eigenvectors; the starting vector for each eigenvector */
/* >          must be in the same column(s) in which the eigenvector will */
/* >          be stored. */
/* >          On exit, if SIDE = 'R' or 'B', the right eigenvectors */
/* >          specified by SELECT will be stored consecutively in the */
/* >          columns of VR, in the same order as their eigenvalues. A */
/* >          complex eigenvector corresponding to a complex eigenvalue is */
/* >          stored in two consecutive columns, the first holding the real */
/* >          part and the second the imaginary part. */
/* >          If SIDE = 'L', VR is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDVR */
/* > \verbatim */
/* >          LDVR is INTEGER */
/* >          The leading dimension of the array VR. */
/* >          LDVR >= f2cmax(1,N) if SIDE = 'R' or 'B'; LDVR >= 1 otherwise. */
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
/* >          The number of columns in the arrays VL and/or VR required to */
/* >          store the eigenvectors; each selected real eigenvector */
/* >          occupies one column and each selected complex eigenvector */
/* >          occupies two columns. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is REAL array, dimension ((N+2)*N) */
/* > \endverbatim */
/* > */
/* > \param[out] IFAILL */
/* > \verbatim */
/* >          IFAILL is INTEGER array, dimension (MM) */
/* >          If SIDE = 'L' or 'B', IFAILL(i) = j > 0 if the left */
/* >          eigenvector in the i-th column of VL (corresponding to the */
/* >          eigenvalue w(j)) failed to converge; IFAILL(i) = 0 if the */
/* >          eigenvector converged satisfactorily. If the i-th and (i+1)th */
/* >          columns of VL hold a complex eigenvector, then IFAILL(i) and */
/* >          IFAILL(i+1) are set to the same value. */
/* >          If SIDE = 'R', IFAILL is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[out] IFAILR */
/* > \verbatim */
/* >          IFAILR is INTEGER array, dimension (MM) */
/* >          If SIDE = 'R' or 'B', IFAILR(i) = j > 0 if the right */
/* >          eigenvector in the i-th column of VR (corresponding to the */
/* >          eigenvalue w(j)) failed to converge; IFAILR(i) = 0 if the */
/* >          eigenvector converged satisfactorily. If the i-th and (i+1)th */
/* >          columns of VR hold a complex eigenvector, then IFAILR(i) and */
/* >          IFAILR(i+1) are set to the same value. */
/* >          If SIDE = 'L', IFAILR is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value */
/* >          > 0:  if INFO = i, i is the number of eigenvectors which */
/* >                failed to converge; see IFAILL and IFAILR for further */
/* >                details. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup realOTHERcomputational */

/* > \par Further Details: */
/*  ===================== */
/* > */
/* > \verbatim */
/* > */
/* >  Each eigenvector is normalized so that the element of largest */
/* >  magnitude has magnitude 1; here the magnitude of a complex number */
/* >  (x,y) is taken to be |x|+|y|. */
/* > \endverbatim */
/* > */
/*  ===================================================================== */
/* Subroutine */ void shsein_(char *side, char *eigsrc, char *initv, logical *
	select, integer *n, real *h__, integer *ldh, real *wr, real *wi, real 
	*vl, integer *ldvl, real *vr, integer *ldvr, integer *mm, integer *m, 
	real *work, integer *ifaill, integer *ifailr, integer *info)
{
    /* System generated locals */
    integer h_dim1, h_offset, vl_dim1, vl_offset, vr_dim1, vr_offset, i__1, 
	    i__2;
    real r__1, r__2;

    /* Local variables */
    logical pair;
    real unfl;
    integer i__, k;
    extern logical lsame_(char *, char *);
    integer iinfo;
    logical leftv, bothv;
    real hnorm;
    integer kl, kr;
    extern real slamch_(char *);
    extern /* Subroutine */ void slaein_(logical *, logical *, integer *, real 
	    *, integer *, real *, real *, real *, real *, real *, integer *, 
	    real *, real *, real *, real *, integer *);
    extern int xerbla_(char *, integer *, ftnlen);
    real bignum;
    extern real slanhs_(char *, integer *, real *, integer *, real *);
    extern logical sisnan_(real *);
    logical noinit;
    integer ldwork;
    logical rightv, fromqr;
    real smlnum;
    integer kln, ksi;
    real wki;
    integer ksr;
    real ulp, wkr, eps3;


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


/*     Decode and test the input parameters. */

    /* Parameter adjustments */
    --select;
    h_dim1 = *ldh;
    h_offset = 1 + h_dim1 * 1;
    h__ -= h_offset;
    --wr;
    --wi;
    vl_dim1 = *ldvl;
    vl_offset = 1 + vl_dim1 * 1;
    vl -= vl_offset;
    vr_dim1 = *ldvr;
    vr_offset = 1 + vr_dim1 * 1;
    vr -= vr_offset;
    --work;
    --ifaill;
    --ifailr;

    /* Function Body */
    bothv = lsame_(side, "B");
    rightv = lsame_(side, "R") || bothv;
    leftv = lsame_(side, "L") || bothv;

    fromqr = lsame_(eigsrc, "Q");

    noinit = lsame_(initv, "N");

/*     Set M to the number of columns required to store the selected */
/*     eigenvectors, and standardize the array SELECT. */

    *m = 0;
    pair = FALSE_;
    i__1 = *n;
    for (k = 1; k <= i__1; ++k) {
	if (pair) {
	    pair = FALSE_;
	    select[k] = FALSE_;
	} else {
	    if (wi[k] == 0.f) {
		if (select[k]) {
		    ++(*m);
		}
	    } else {
		pair = TRUE_;
		if (select[k] || select[k + 1]) {
		    select[k] = TRUE_;
		    *m += 2;
		}
	    }
	}
/* L10: */
    }

    *info = 0;
    if (! rightv && ! leftv) {
	*info = -1;
    } else if (! fromqr && ! lsame_(eigsrc, "N")) {
	*info = -2;
    } else if (! noinit && ! lsame_(initv, "U")) {
	*info = -3;
    } else if (*n < 0) {
	*info = -5;
    } else if (*ldh < f2cmax(1,*n)) {
	*info = -7;
    } else if (*ldvl < 1 || leftv && *ldvl < *n) {
	*info = -11;
    } else if (*ldvr < 1 || rightv && *ldvr < *n) {
	*info = -13;
    } else if (*mm < *m) {
	*info = -14;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("SHSEIN", &i__1, (ftnlen)6);
	return;
    }

/*     Quick return if possible. */

    if (*n == 0) {
	return;
    }

/*     Set machine-dependent constants. */

    unfl = slamch_("Safe minimum");
    ulp = slamch_("Precision");
    smlnum = unfl * (*n / ulp);
    bignum = (1.f - ulp) / smlnum;

    ldwork = *n + 1;

    kl = 1;
    kln = 0;
    if (fromqr) {
	kr = 0;
    } else {
	kr = *n;
    }
    ksr = 1;

    i__1 = *n;
    for (k = 1; k <= i__1; ++k) {
	if (select[k]) {

/*           Compute eigenvector(s) corresponding to W(K). */

	    if (fromqr) {

/*              If affiliation of eigenvalues is known, check whether */
/*              the matrix splits. */

/*              Determine KL and KR such that 1 <= KL <= K <= KR <= N */
/*              and H(KL,KL-1) and H(KR+1,KR) are zero (or KL = 1 or */
/*              KR = N). */

/*              Then inverse iteration can be performed with the */
/*              submatrix H(KL:N,KL:N) for a left eigenvector, and with */
/*              the submatrix H(1:KR,1:KR) for a right eigenvector. */

		i__2 = kl + 1;
		for (i__ = k; i__ >= i__2; --i__) {
		    if (h__[i__ + (i__ - 1) * h_dim1] == 0.f) {
			goto L30;
		    }
/* L20: */
		}
L30:
		kl = i__;
		if (k > kr) {
		    i__2 = *n - 1;
		    for (i__ = k; i__ <= i__2; ++i__) {
			if (h__[i__ + 1 + i__ * h_dim1] == 0.f) {
			    goto L50;
			}
/* L40: */
		    }
L50:
		    kr = i__;
		}
	    }

	    if (kl != kln) {
		kln = kl;

/*              Compute infinity-norm of submatrix H(KL:KR,KL:KR) if it */
/*              has not ben computed before. */

		i__2 = kr - kl + 1;
		hnorm = slanhs_("I", &i__2, &h__[kl + kl * h_dim1], ldh, &
			work[1]);
		if (sisnan_(&hnorm)) {
		    *info = -6;
		    return;
		} else if (hnorm > 0.f) {
		    eps3 = hnorm * ulp;
		} else {
		    eps3 = smlnum;
		}
	    }

/*           Perturb eigenvalue if it is close to any previous */
/*           selected eigenvalues affiliated to the submatrix */
/*           H(KL:KR,KL:KR). Close roots are modified by EPS3. */

	    wkr = wr[k];
	    wki = wi[k];
L60:
	    i__2 = kl;
	    for (i__ = k - 1; i__ >= i__2; --i__) {
		if (select[i__] && (r__1 = wr[i__] - wkr, abs(r__1)) + (r__2 =
			 wi[i__] - wki, abs(r__2)) < eps3) {
		    wkr += eps3;
		    goto L60;
		}
/* L70: */
	    }
	    wr[k] = wkr;

	    pair = wki != 0.f;
	    if (pair) {
		ksi = ksr + 1;
	    } else {
		ksi = ksr;
	    }
	    if (leftv) {

/*              Compute left eigenvector. */

		i__2 = *n - kl + 1;
		slaein_(&c_false, &noinit, &i__2, &h__[kl + kl * h_dim1], ldh,
			 &wkr, &wki, &vl[kl + ksr * vl_dim1], &vl[kl + ksi * 
			vl_dim1], &work[1], &ldwork, &work[*n * *n + *n + 1], 
			&eps3, &smlnum, &bignum, &iinfo);
		if (iinfo > 0) {
		    if (pair) {
			*info += 2;
		    } else {
			++(*info);
		    }
		    ifaill[ksr] = k;
		    ifaill[ksi] = k;
		} else {
		    ifaill[ksr] = 0;
		    ifaill[ksi] = 0;
		}
		i__2 = kl - 1;
		for (i__ = 1; i__ <= i__2; ++i__) {
		    vl[i__ + ksr * vl_dim1] = 0.f;
/* L80: */
		}
		if (pair) {
		    i__2 = kl - 1;
		    for (i__ = 1; i__ <= i__2; ++i__) {
			vl[i__ + ksi * vl_dim1] = 0.f;
/* L90: */
		    }
		}
	    }
	    if (rightv) {

/*              Compute right eigenvector. */

		slaein_(&c_true, &noinit, &kr, &h__[h_offset], ldh, &wkr, &
			wki, &vr[ksr * vr_dim1 + 1], &vr[ksi * vr_dim1 + 1], &
			work[1], &ldwork, &work[*n * *n + *n + 1], &eps3, &
			smlnum, &bignum, &iinfo);
		if (iinfo > 0) {
		    if (pair) {
			*info += 2;
		    } else {
			++(*info);
		    }
		    ifailr[ksr] = k;
		    ifailr[ksi] = k;
		} else {
		    ifailr[ksr] = 0;
		    ifailr[ksi] = 0;
		}
		i__2 = *n;
		for (i__ = kr + 1; i__ <= i__2; ++i__) {
		    vr[i__ + ksr * vr_dim1] = 0.f;
/* L100: */
		}
		if (pair) {
		    i__2 = *n;
		    for (i__ = kr + 1; i__ <= i__2; ++i__) {
			vr[i__ + ksi * vr_dim1] = 0.f;
/* L110: */
		    }
		}
	    }

	    if (pair) {
		ksr += 2;
	    } else {
		++ksr;
	    }
	}
/* L120: */
    }

    return;

/*     End of SHSEIN */

} /* shsein_ */

