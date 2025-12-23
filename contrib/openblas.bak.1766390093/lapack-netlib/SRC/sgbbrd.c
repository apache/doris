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

static real c_b8 = 0.f;
static real c_b9 = 1.f;
static integer c__1 = 1;

/* > \brief \b SGBBRD */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download SGBBRD + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/sgbbrd.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/sgbbrd.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/sgbbrd.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE SGBBRD( VECT, M, N, NCC, KL, KU, AB, LDAB, D, E, Q, */
/*                          LDQ, PT, LDPT, C, LDC, WORK, INFO ) */

/*       CHARACTER          VECT */
/*       INTEGER            INFO, KL, KU, LDAB, LDC, LDPT, LDQ, M, N, NCC */
/*       REAL               AB( LDAB, * ), C( LDC, * ), D( * ), E( * ), */
/*      $                   PT( LDPT, * ), Q( LDQ, * ), WORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > SGBBRD reduces a real general m-by-n band matrix A to upper */
/* > bidiagonal form B by an orthogonal transformation: Q**T * A * P = B. */
/* > */
/* > The routine computes B, and optionally forms Q or P**T, or computes */
/* > Q**T*C for a given matrix C. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] VECT */
/* > \verbatim */
/* >          VECT is CHARACTER*1 */
/* >          Specifies whether or not the matrices Q and P**T are to be */
/* >          formed. */
/* >          = 'N': do not form Q or P**T; */
/* >          = 'Q': form Q only; */
/* >          = 'P': form P**T only; */
/* >          = 'B': form both. */
/* > \endverbatim */
/* > */
/* > \param[in] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >          The number of rows of the matrix A.  M >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The number of columns of the matrix A.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] NCC */
/* > \verbatim */
/* >          NCC is INTEGER */
/* >          The number of columns of the matrix C.  NCC >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] KL */
/* > \verbatim */
/* >          KL is INTEGER */
/* >          The number of subdiagonals of the matrix A. KL >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] KU */
/* > \verbatim */
/* >          KU is INTEGER */
/* >          The number of superdiagonals of the matrix A. KU >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] AB */
/* > \verbatim */
/* >          AB is REAL array, dimension (LDAB,N) */
/* >          On entry, the m-by-n band matrix A, stored in rows 1 to */
/* >          KL+KU+1. The j-th column of A is stored in the j-th column of */
/* >          the array AB as follows: */
/* >          AB(ku+1+i-j,j) = A(i,j) for f2cmax(1,j-ku)<=i<=f2cmin(m,j+kl). */
/* >          On exit, A is overwritten by values generated during the */
/* >          reduction. */
/* > \endverbatim */
/* > */
/* > \param[in] LDAB */
/* > \verbatim */
/* >          LDAB is INTEGER */
/* >          The leading dimension of the array A. LDAB >= KL+KU+1. */
/* > \endverbatim */
/* > */
/* > \param[out] D */
/* > \verbatim */
/* >          D is REAL array, dimension (f2cmin(M,N)) */
/* >          The diagonal elements of the bidiagonal matrix B. */
/* > \endverbatim */
/* > */
/* > \param[out] E */
/* > \verbatim */
/* >          E is REAL array, dimension (f2cmin(M,N)-1) */
/* >          The superdiagonal elements of the bidiagonal matrix B. */
/* > \endverbatim */
/* > */
/* > \param[out] Q */
/* > \verbatim */
/* >          Q is REAL array, dimension (LDQ,M) */
/* >          If VECT = 'Q' or 'B', the m-by-m orthogonal matrix Q. */
/* >          If VECT = 'N' or 'P', the array Q is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDQ */
/* > \verbatim */
/* >          LDQ is INTEGER */
/* >          The leading dimension of the array Q. */
/* >          LDQ >= f2cmax(1,M) if VECT = 'Q' or 'B'; LDQ >= 1 otherwise. */
/* > \endverbatim */
/* > */
/* > \param[out] PT */
/* > \verbatim */
/* >          PT is REAL array, dimension (LDPT,N) */
/* >          If VECT = 'P' or 'B', the n-by-n orthogonal matrix P'. */
/* >          If VECT = 'N' or 'Q', the array PT is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDPT */
/* > \verbatim */
/* >          LDPT is INTEGER */
/* >          The leading dimension of the array PT. */
/* >          LDPT >= f2cmax(1,N) if VECT = 'P' or 'B'; LDPT >= 1 otherwise. */
/* > \endverbatim */
/* > */
/* > \param[in,out] C */
/* > \verbatim */
/* >          C is REAL array, dimension (LDC,NCC) */
/* >          On entry, an m-by-ncc matrix C. */
/* >          On exit, C is overwritten by Q**T*C. */
/* >          C is not referenced if NCC = 0. */
/* > \endverbatim */
/* > */
/* > \param[in] LDC */
/* > \verbatim */
/* >          LDC is INTEGER */
/* >          The leading dimension of the array C. */
/* >          LDC >= f2cmax(1,M) if NCC > 0; LDC >= 1 if NCC = 0. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is REAL array, dimension (2*f2cmax(M,N)) */
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

/* > \ingroup realGBcomputational */

/*  ===================================================================== */
/* Subroutine */ void sgbbrd_(char *vect, integer *m, integer *n, integer *ncc,
	 integer *kl, integer *ku, real *ab, integer *ldab, real *d__, real *
	e, real *q, integer *ldq, real *pt, integer *ldpt, real *c__, integer 
	*ldc, real *work, integer *info)
{
    /* System generated locals */
    integer ab_dim1, ab_offset, c_dim1, c_offset, pt_dim1, pt_offset, q_dim1, 
	    q_offset, i__1, i__2, i__3, i__4, i__5, i__6, i__7;

    /* Local variables */
    integer inca;
    extern /* Subroutine */ void srot_(integer *, real *, integer *, real *, 
	    integer *, real *, real *);
    integer i__, j, l;
    extern logical lsame_(char *, char *);
    logical wantb, wantc;
    integer minmn;
    logical wantq;
    integer j1, j2, kb;
    real ra, rb, rc;
    integer kk, ml, mn, nr, mu;
    real rs;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    extern void slaset_(
	    char *, integer *, integer *, real *, real *, real *, integer *), slartg_(real *, real *, real *, real *, real *);
    integer kb1;
    extern /* Subroutine */ void slargv_(integer *, real *, integer *, real *, 
	    integer *, real *, integer *);
    integer ml0;
    extern /* Subroutine */ void slartv_(integer *, real *, integer *, real *, 
	    integer *, real *, real *, integer *);
    logical wantpt;
    integer mu0, klm, kun, nrt, klu1;


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


/*     Test the input parameters */

    /* Parameter adjustments */
    ab_dim1 = *ldab;
    ab_offset = 1 + ab_dim1 * 1;
    ab -= ab_offset;
    --d__;
    --e;
    q_dim1 = *ldq;
    q_offset = 1 + q_dim1 * 1;
    q -= q_offset;
    pt_dim1 = *ldpt;
    pt_offset = 1 + pt_dim1 * 1;
    pt -= pt_offset;
    c_dim1 = *ldc;
    c_offset = 1 + c_dim1 * 1;
    c__ -= c_offset;
    --work;

    /* Function Body */
    wantb = lsame_(vect, "B");
    wantq = lsame_(vect, "Q") || wantb;
    wantpt = lsame_(vect, "P") || wantb;
    wantc = *ncc > 0;
    klu1 = *kl + *ku + 1;
    *info = 0;
    if (! wantq && ! wantpt && ! lsame_(vect, "N")) {
	*info = -1;
    } else if (*m < 0) {
	*info = -2;
    } else if (*n < 0) {
	*info = -3;
    } else if (*ncc < 0) {
	*info = -4;
    } else if (*kl < 0) {
	*info = -5;
    } else if (*ku < 0) {
	*info = -6;
    } else if (*ldab < klu1) {
	*info = -8;
    } else if (*ldq < 1 || wantq && *ldq < f2cmax(1,*m)) {
	*info = -12;
    } else if (*ldpt < 1 || wantpt && *ldpt < f2cmax(1,*n)) {
	*info = -14;
    } else if (*ldc < 1 || wantc && *ldc < f2cmax(1,*m)) {
	*info = -16;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("SGBBRD", &i__1, (ftnlen)6);
	return;
    }

/*     Initialize Q and P**T to the unit matrix, if needed */

    if (wantq) {
	slaset_("Full", m, m, &c_b8, &c_b9, &q[q_offset], ldq);
    }
    if (wantpt) {
	slaset_("Full", n, n, &c_b8, &c_b9, &pt[pt_offset], ldpt);
    }

/*     Quick return if possible. */

    if (*m == 0 || *n == 0) {
	return;
    }

    minmn = f2cmin(*m,*n);

    if (*kl + *ku > 1) {

/*        Reduce to upper bidiagonal form if KU > 0; if KU = 0, reduce */
/*        first to lower bidiagonal form and then transform to upper */
/*        bidiagonal */

	if (*ku > 0) {
	    ml0 = 1;
	    mu0 = 2;
	} else {
	    ml0 = 2;
	    mu0 = 1;
	}

/*        Wherever possible, plane rotations are generated and applied in */
/*        vector operations of length NR over the index set J1:J2:KLU1. */

/*        The sines of the plane rotations are stored in WORK(1:f2cmax(m,n)) */
/*        and the cosines in WORK(f2cmax(m,n)+1:2*f2cmax(m,n)). */

	mn = f2cmax(*m,*n);
/* Computing MIN */
	i__1 = *m - 1;
	klm = f2cmin(i__1,*kl);
/* Computing MIN */
	i__1 = *n - 1;
	kun = f2cmin(i__1,*ku);
	kb = klm + kun;
	kb1 = kb + 1;
	inca = kb1 * *ldab;
	nr = 0;
	j1 = klm + 2;
	j2 = 1 - kun;

	i__1 = minmn;
	for (i__ = 1; i__ <= i__1; ++i__) {

/*           Reduce i-th column and i-th row of matrix to bidiagonal form */

	    ml = klm + 1;
	    mu = kun + 1;
	    i__2 = kb;
	    for (kk = 1; kk <= i__2; ++kk) {
		j1 += kb;
		j2 += kb;

/*              generate plane rotations to annihilate nonzero elements */
/*              which have been created below the band */

		if (nr > 0) {
		    slargv_(&nr, &ab[klu1 + (j1 - klm - 1) * ab_dim1], &inca, 
			    &work[j1], &kb1, &work[mn + j1], &kb1);
		}

/*              apply plane rotations from the left */

		i__3 = kb;
		for (l = 1; l <= i__3; ++l) {
		    if (j2 - klm + l - 1 > *n) {
			nrt = nr - 1;
		    } else {
			nrt = nr;
		    }
		    if (nrt > 0) {
			slartv_(&nrt, &ab[klu1 - l + (j1 - klm + l - 1) * 
				ab_dim1], &inca, &ab[klu1 - l + 1 + (j1 - klm 
				+ l - 1) * ab_dim1], &inca, &work[mn + j1], &
				work[j1], &kb1);
		    }
/* L10: */
		}

		if (ml > ml0) {
		    if (ml <= *m - i__ + 1) {

/*                    generate plane rotation to annihilate a(i+ml-1,i) */
/*                    within the band, and apply rotation from the left */

			slartg_(&ab[*ku + ml - 1 + i__ * ab_dim1], &ab[*ku + 
				ml + i__ * ab_dim1], &work[mn + i__ + ml - 1],
				 &work[i__ + ml - 1], &ra);
			ab[*ku + ml - 1 + i__ * ab_dim1] = ra;
			if (i__ < *n) {
/* Computing MIN */
			    i__4 = *ku + ml - 2, i__5 = *n - i__;
			    i__3 = f2cmin(i__4,i__5);
			    i__6 = *ldab - 1;
			    i__7 = *ldab - 1;
			    srot_(&i__3, &ab[*ku + ml - 2 + (i__ + 1) * 
				    ab_dim1], &i__6, &ab[*ku + ml - 1 + (i__ 
				    + 1) * ab_dim1], &i__7, &work[mn + i__ + 
				    ml - 1], &work[i__ + ml - 1]);
			}
		    }
		    ++nr;
		    j1 -= kb1;
		}

		if (wantq) {

/*                 accumulate product of plane rotations in Q */

		    i__3 = j2;
		    i__4 = kb1;
		    for (j = j1; i__4 < 0 ? j >= i__3 : j <= i__3; j += i__4) 
			    {
			srot_(m, &q[(j - 1) * q_dim1 + 1], &c__1, &q[j * 
				q_dim1 + 1], &c__1, &work[mn + j], &work[j]);
/* L20: */
		    }
		}

		if (wantc) {

/*                 apply plane rotations to C */

		    i__4 = j2;
		    i__3 = kb1;
		    for (j = j1; i__3 < 0 ? j >= i__4 : j <= i__4; j += i__3) 
			    {
			srot_(ncc, &c__[j - 1 + c_dim1], ldc, &c__[j + c_dim1]
				, ldc, &work[mn + j], &work[j]);
/* L30: */
		    }
		}

		if (j2 + kun > *n) {

/*                 adjust J2 to keep within the bounds of the matrix */

		    --nr;
		    j2 -= kb1;
		}

		i__3 = j2;
		i__4 = kb1;
		for (j = j1; i__4 < 0 ? j >= i__3 : j <= i__3; j += i__4) {

/*                 create nonzero element a(j-1,j+ku) above the band */
/*                 and store it in WORK(n+1:2*n) */

		    work[j + kun] = work[j] * ab[(j + kun) * ab_dim1 + 1];
		    ab[(j + kun) * ab_dim1 + 1] = work[mn + j] * ab[(j + kun) 
			    * ab_dim1 + 1];
/* L40: */
		}

/*              generate plane rotations to annihilate nonzero elements */
/*              which have been generated above the band */

		if (nr > 0) {
		    slargv_(&nr, &ab[(j1 + kun - 1) * ab_dim1 + 1], &inca, &
			    work[j1 + kun], &kb1, &work[mn + j1 + kun], &kb1);
		}

/*              apply plane rotations from the right */

		i__4 = kb;
		for (l = 1; l <= i__4; ++l) {
		    if (j2 + l - 1 > *m) {
			nrt = nr - 1;
		    } else {
			nrt = nr;
		    }
		    if (nrt > 0) {
			slartv_(&nrt, &ab[l + 1 + (j1 + kun - 1) * ab_dim1], &
				inca, &ab[l + (j1 + kun) * ab_dim1], &inca, &
				work[mn + j1 + kun], &work[j1 + kun], &kb1);
		    }
/* L50: */
		}

		if (ml == ml0 && mu > mu0) {
		    if (mu <= *n - i__ + 1) {

/*                    generate plane rotation to annihilate a(i,i+mu-1) */
/*                    within the band, and apply rotation from the right */

			slartg_(&ab[*ku - mu + 3 + (i__ + mu - 2) * ab_dim1], 
				&ab[*ku - mu + 2 + (i__ + mu - 1) * ab_dim1], 
				&work[mn + i__ + mu - 1], &work[i__ + mu - 1],
				 &ra);
			ab[*ku - mu + 3 + (i__ + mu - 2) * ab_dim1] = ra;
/* Computing MIN */
			i__3 = *kl + mu - 2, i__5 = *m - i__;
			i__4 = f2cmin(i__3,i__5);
			srot_(&i__4, &ab[*ku - mu + 4 + (i__ + mu - 2) * 
				ab_dim1], &c__1, &ab[*ku - mu + 3 + (i__ + mu 
				- 1) * ab_dim1], &c__1, &work[mn + i__ + mu - 
				1], &work[i__ + mu - 1]);
		    }
		    ++nr;
		    j1 -= kb1;
		}

		if (wantpt) {

/*                 accumulate product of plane rotations in P**T */

		    i__4 = j2;
		    i__3 = kb1;
		    for (j = j1; i__3 < 0 ? j >= i__4 : j <= i__4; j += i__3) 
			    {
			srot_(n, &pt[j + kun - 1 + pt_dim1], ldpt, &pt[j + 
				kun + pt_dim1], ldpt, &work[mn + j + kun], &
				work[j + kun]);
/* L60: */
		    }
		}

		if (j2 + kb > *m) {

/*                 adjust J2 to keep within the bounds of the matrix */

		    --nr;
		    j2 -= kb1;
		}

		i__3 = j2;
		i__4 = kb1;
		for (j = j1; i__4 < 0 ? j >= i__3 : j <= i__3; j += i__4) {

/*                 create nonzero element a(j+kl+ku,j+ku-1) below the */
/*                 band and store it in WORK(1:n) */

		    work[j + kb] = work[j + kun] * ab[klu1 + (j + kun) * 
			    ab_dim1];
		    ab[klu1 + (j + kun) * ab_dim1] = work[mn + j + kun] * ab[
			    klu1 + (j + kun) * ab_dim1];
/* L70: */
		}

		if (ml > ml0) {
		    --ml;
		} else {
		    --mu;
		}
/* L80: */
	    }
/* L90: */
	}
    }

    if (*ku == 0 && *kl > 0) {

/*        A has been reduced to lower bidiagonal form */

/*        Transform lower bidiagonal form to upper bidiagonal by applying */
/*        plane rotations from the left, storing diagonal elements in D */
/*        and off-diagonal elements in E */

/* Computing MIN */
	i__2 = *m - 1;
	i__1 = f2cmin(i__2,*n);
	for (i__ = 1; i__ <= i__1; ++i__) {
	    slartg_(&ab[i__ * ab_dim1 + 1], &ab[i__ * ab_dim1 + 2], &rc, &rs, 
		    &ra);
	    d__[i__] = ra;
	    if (i__ < *n) {
		e[i__] = rs * ab[(i__ + 1) * ab_dim1 + 1];
		ab[(i__ + 1) * ab_dim1 + 1] = rc * ab[(i__ + 1) * ab_dim1 + 1]
			;
	    }
	    if (wantq) {
		srot_(m, &q[i__ * q_dim1 + 1], &c__1, &q[(i__ + 1) * q_dim1 + 
			1], &c__1, &rc, &rs);
	    }
	    if (wantc) {
		srot_(ncc, &c__[i__ + c_dim1], ldc, &c__[i__ + 1 + c_dim1], 
			ldc, &rc, &rs);
	    }
/* L100: */
	}
	if (*m <= *n) {
	    d__[*m] = ab[*m * ab_dim1 + 1];
	}
    } else if (*ku > 0) {

/*        A has been reduced to upper bidiagonal form */

	if (*m < *n) {

/*           Annihilate a(m,m+1) by applying plane rotations from the */
/*           right, storing diagonal elements in D and off-diagonal */
/*           elements in E */

	    rb = ab[*ku + (*m + 1) * ab_dim1];
	    for (i__ = *m; i__ >= 1; --i__) {
		slartg_(&ab[*ku + 1 + i__ * ab_dim1], &rb, &rc, &rs, &ra);
		d__[i__] = ra;
		if (i__ > 1) {
		    rb = -rs * ab[*ku + i__ * ab_dim1];
		    e[i__ - 1] = rc * ab[*ku + i__ * ab_dim1];
		}
		if (wantpt) {
		    srot_(n, &pt[i__ + pt_dim1], ldpt, &pt[*m + 1 + pt_dim1], 
			    ldpt, &rc, &rs);
		}
/* L110: */
	    }
	} else {

/*           Copy off-diagonal elements to E and diagonal elements to D */

	    i__1 = minmn - 1;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		e[i__] = ab[*ku + (i__ + 1) * ab_dim1];
/* L120: */
	    }
	    i__1 = minmn;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		d__[i__] = ab[*ku + 1 + i__ * ab_dim1];
/* L130: */
	    }
	}
    } else {

/*        A is diagonal. Set elements of E to zero and copy diagonal */
/*        elements to D. */

	i__1 = minmn - 1;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    e[i__] = 0.f;
/* L140: */
	}
	i__1 = minmn;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    d__[i__] = ab[i__ * ab_dim1 + 1];
/* L150: */
	}
    }
    return;

/*     End of SGBBRD */

} /* sgbbrd_ */

