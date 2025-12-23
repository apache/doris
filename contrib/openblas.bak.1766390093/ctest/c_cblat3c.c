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

#include "common.h"

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
typedef int logical;
typedef short int shortlogical;
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

#define F2C_proc_par_types 1


/* Common Block Declarations */

struct {
    integer infot, noutc;
    logical ok, lerr;
} infoc_;

#define infoc_1 infoc_

struct {
    char srnamt[12];
} srnamc_;

#define srnamc_1 srnamc_

/* Table of constant values */

static complex c_b1 = {0.f,0.f};
static complex c_b2 = {1.f,0.f};
static integer c__1 = 1;
static integer c__65 = 65;
static integer c__6 = 6;
static real c_b91 = 1.f;
static logical c_true = TRUE_;
static integer c__0 = 0;
static logical c_false = FALSE_;

int /* Main program */ main(void)
{
    /* Initialized data */

    static char snames[9][13] = {"cblas_cgemm ", "cblas_chemm ", "cblas_csymm ", 
	    "cblas_ctrmm ", "cblas_ctrsm ", "cblas_cherk ", "cblas_csyrk ", 
	    "cblas_cher2k", "cblas_csyr2k"};

    /* System generated locals */
    integer i__1, i__2, i__3, i__4, i__5;
    real r__1;

    /* Local variables */
    integer nalf, idim[9];
    logical same;
    integer nbet, ntra;
    logical rewi;
    extern /* Subroutine */ int cchk1_(char *, real *, real *, integer *, 
	    integer *, logical *, logical *, logical *, integer *, integer *, 
	    integer *, complex *, integer *, complex *, integer *, complex *, 
	    complex *, complex *, complex *, complex *, complex *, complex *, 
	    complex *, complex *, complex *, real *, integer *), 
	    cchk2_(char *, real *, real *, integer *, integer *, logical *, 
	    logical *, logical *, integer *, integer *, integer *, complex *, 
	    integer *, complex *, integer *, complex *, complex *, complex *, 
	    complex *, complex *, complex *, complex *, complex *, complex *, 
	    complex *, real *, integer *), cchk3_(char *, real *, 
	    real *, integer *, integer *, logical *, logical *, logical *, 
	    integer *, integer *, integer *, complex *, integer *, complex *, 
	    complex *, complex *, complex *, complex *, complex *, complex *, 
	    real *, complex *, integer *), cchk4_(char *, real *, 
	    real *, integer *, integer *, logical *, logical *, logical *, 
	    integer *, integer *, integer *, complex *, integer *, complex *, 
	    integer *, complex *, complex *, complex *, complex *, complex *, 
	    complex *, complex *, complex *, complex *, complex *, real *, 
	    integer *), cchk5_(char *, real *, real *, integer *, 
	    integer *, logical *, logical *, logical *, integer *, integer *, 
	    integer *, complex *, integer *, complex *, integer *, complex *, 
	    complex *, complex *, complex *, complex *, complex *, complex *, 
	    complex *, complex *, real *, complex *, integer *);
    complex c__[4225]	/* was [65][65] */;
    real g[65];
    integer i__, j, n;
    logical fatal;
    complex w[130];
    extern /* Subroutine */ int cmmch_(char *, char *, integer *, integer *, 
	    integer *, complex *, complex *, integer *, complex *, integer *, 
	    complex *, complex *, integer *, complex *, real *, complex *, 
	    integer *, real *, real *, logical *, integer *, logical *);
    extern real sdiff_(real *, real *);
    logical trace;
    integer nidim;
    char snaps[32];
    integer isnum;
    logical ltest[9];
    complex aa[4225], ab[8450]	/* was [65][130] */, bb[4225], cc[4225], as[
	    4225], bs[4225], cs[4225], ct[65];
    logical sfatal, corder;
    char snamet[12], transa[1], transb[1];
    real thresh;
    logical rorder;
    extern /* Subroutine */ int cc3chke_(char *);
    integer layout;
    logical ltestt, tsterr;
    complex alf[7];
    extern logical lce_(complex *, complex *, integer *);
    complex bet[7];
    real eps, err;
    char tmpchar;

/*  Test program for the COMPLEX          Level 3 Blas. */

/*  The program must be driven by a short data file. The first 13 records */
/*  of the file are read using list-directed input, the last 9 records */
/*  are read using the format ( A12, L2 ). An annotated example of a data */
/*  file can be obtained by deleting the first 3 characters from the */
/*  following 22 lines: */
/*  'CBLAT3.SNAP'     NAME OF SNAPSHOT OUTPUT FILE */
/*  -1                UNIT NUMBER OF SNAPSHOT FILE (NOT USED IF .LT. 0) */
/*  F        LOGICAL FLAG, T TO REWIND SNAPSHOT FILE AFTER EACH RECORD. */
/*  F        LOGICAL FLAG, T TO STOP ON FAILURES. */
/*  T        LOGICAL FLAG, T TO TEST ERROR CALL MYEXITS. */
/*  2        0 TO TEST COLUMN-MAJOR, 1 TO TEST ROW-MAJOR, 2 TO TEST BOTH */
/*  16.0     THRESHOLD VALUE OF TEST RATIO */
/*  6                 NUMBER OF VALUES OF N */
/*  0 1 2 3 5 9       VALUES OF N */
/*  3                 NUMBER OF VALUES OF ALPHA */
/*  (0.0,0.0) (1.0,0.0) (0.7,-0.9)       VALUES OF ALPHA */
/*  3                 NUMBER OF VALUES OF BETA */
/*  (0.0,0.0) (1.0,0.0) (1.3,-1.1)       VALUES OF BETA */
/*  cblas_cgemm  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  cblas_chemm  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  cblas_csymm  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  cblas_ctrmm  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  cblas_ctrsm  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  cblas_cherk  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  cblas_csyrk  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  cblas_cher2k T PUT F FOR NO TEST. SAME COLUMNS. */
/*  cblas_csyr2k T PUT F FOR NO TEST. SAME COLUMNS. */

/*  See: */

/*     Dongarra J. J., Du Croz J. J., Duff I. S. and Hammarling S. */
/*     A Set of Level 3 Basic Linear Algebra Subprograms. */

/*     Technical Memorandum No.88 (Revision 1), Mathematics and */
/*     Computer Science Division, Argonne National Laboratory, 9700 */
/*     South Cass Avenue, Argonne, Illinois 60439, US. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */


    infoc_1.noutc = 6;

/*     Read name and unit number for snapshot output file and open file. */
    char line[80];
    
    fgets(line,80,stdin);
    sscanf(line,"'%s'",snaps);
    fgets(line,80,stdin);
#ifdef USE64BITINT
    sscanf(line,"%ld",&ntra);
#else
    sscanf(line,"%d",&ntra);
#endif
    trace = ntra >= 0;
    if (trace) {
/*	o__1.oerr = 0;
	o__1.ounit = ntra;
	o__1.ofnmlen = 32;
	o__1.ofnm = snaps;
	o__1.orl = 0;
	o__1.osta = 0;
	o__1.oacc = 0;
	o__1.ofm = 0;
	o__1.oblnk = 0;
	f_open(&o__1);*/
    }
/*     Read the flag that directs rewinding of the snapshot file. */
   fgets(line,80,stdin);
   sscanf(line,"%d",&rewi);
   rewi = rewi && trace;
/*     Read the flag that directs stopping on any failure. */
   fgets(line,80,stdin);
   sscanf(line,"%c",&tmpchar);
   sfatal=FALSE_;
   if (tmpchar=='T')sfatal=TRUE_;
   fgets(line,80,stdin);
   sscanf(line,"%c",&tmpchar);
   tsterr=FALSE_;
   if (tmpchar=='T')tsterr=TRUE_;
   fgets(line,80,stdin);
   sscanf(line,"%d",&layout);
   fgets(line,80,stdin);
   sscanf(line,"%f",&thresh);


/*     Read and check the parameter values for the tests. */

/*     Values of N */
   fgets(line,80,stdin);
#ifdef USE64BITINT
   sscanf(line,"%ld",&nidim);
#else
   sscanf(line,"%d",&nidim);
#endif

    if (nidim < 1 || nidim > 9) {
	fprintf(stderr,"NUMBER OF VALUES OF N IS LESS THAN 1 OR GREATER THAN 9");
	goto L220;
    }
   fgets(line,80,stdin);
#ifdef USE64BITINT
   sscanf(line,"%ld %ld %ld %ld %ld %ld %ld %ld %ld",&idim[0],&idim[1],&idim[2],
    &idim[3],&idim[4],&idim[5],&idim[6],&idim[7],&idim[8]);
#else
   sscanf(line,"%d %d %d %d %d %d %d %d %d",&idim[0],&idim[1],&idim[2],
    &idim[3],&idim[4],&idim[5],&idim[6],&idim[7],&idim[8]);
#endif
    i__1 = nidim;
    for (i__ = 1; i__ <= i__1; ++i__) {
	if (idim[i__ - 1] < 0 || idim[i__ - 1] > 65) {
	fprintf(stderr,"VALUE OF N IS LESS THAN 0 OR GREATER THAN 65\n");
	    goto L220;
	}
/* L10: */
    }
/*     Values of ALPHA */
   fgets(line,80,stdin);
#ifdef USE64BITINT
   sscanf(line,"%ld",&nalf);
#else
   sscanf(line,"%d",&nalf);
#endif
    if (nalf < 1 || nalf > 7) {
	fprintf(stderr,"VALUE OF ALPHA IS LESS THAN 0 OR GREATER THAN 7\n");
	goto L220;
    }
   fgets(line,80,stdin);
   sscanf(line,"(%f,%f) (%f,%f) (%f,%f) (%f,%f) (%f,%f) (%f,%f) (%f,%f)",&alf[0].r,&alf[0].i,&alf[1].r,&alf[1].i,&alf[2].r,&alf[2].i,&alf[3].r,&alf[3].i,
   &alf[4].r,&alf[4].i,&alf[5].r,&alf[5].i,&alf[6].r,&alf[6].i);

//    i__1 = nalf;
//    for (i__ = 1; i__ <= i__1; ++i__) {
//	do_lio(&c__6, &c__1, (char *)&alf[i__ - 1], (ftnlen)sizeof(complex));
//    }
/*     Values of BETA */
   fgets(line,80,stdin);
#ifdef USE64BITINT
   sscanf(line,"%ld",&nbet);
#else
   sscanf(line,"%d",&nbet);
#endif
    if (nalf < 1 || nbet > 7) {
	fprintf(stderr,"VALUE OF BETA IS LESS THAN 0 OR GREATER THAN 7\n");
	goto L220;
    }
   fgets(line,80,stdin);
   sscanf(line,"(%f,%f) (%f,%f) (%f,%f) (%f,%f) (%f,%f) (%f,%f) (%f,%f)",&bet[0].r,&bet[0].i,&bet[1].r,&bet[1].i,&bet[2].r,&bet[2].i,&bet[3].r,&bet[3].i,
   &bet[4].r,&bet[4].i,&bet[5].r,&bet[5].i,&bet[6].r,&bet[6].i);


/*     Report values of parameters. */

    printf("TESTS OF THE COMPLEX    LEVEL 3 BLAS\nTHE FOLLOWING PARAMETER VALUES WILL BE USED:\n");
    printf(" FOR N");
    for (i__ =1; i__ <=nidim;++i__) printf(" %d",idim[i__-1]);
    printf("\n");    
    printf(" FOR ALPHA");
    for (i__ =1; i__ <=nalf;++i__) printf(" (%f,%f)",alf[i__-1].r,alf[i__-1].i);
    printf("\n");    
    printf(" FOR BETA");
    for (i__ =1; i__ <=nbet;++i__) printf(" (%f,%f)",bet[i__-1].r,bet[i__-1].i);
    printf("\n");    

    if (! tsterr) {
      printf(" ERROR-EXITS WILL NOT BE TESTED\n"); 
    }
    printf("ROUTINES PASS COMPUTATIONAL TESTS IF TEST RATIO IS LESS THAN %f\n",thresh);
    rorder = FALSE_;
    corder = FALSE_;
    if (layout == 2) {
	rorder = TRUE_;
	corder = TRUE_;
        printf("COLUMN-MAJOR AND ROW-MAJOR DATA LAYOUTS ARE TESTED\n");
    } else if (layout == 1) {
	rorder = TRUE_;
        printf("ROW-MAJOR DATA LAYOUT IS TESTED\n");
    } else if (layout == 0) {
	corder = TRUE_;
        printf("COLUMN-MAJOR DATA LAYOUT IS TESTED\n");
    }

/*     Read names of subroutines and flags which indicate */
/*     whether they are to be tested. */

    for (i__ = 1; i__ <= 9; ++i__) {
	ltest[i__ - 1] = FALSE_;
/* L20: */
    }
L30:
   if (! fgets(line,80,stdin)) {
	goto L60;
    }
   i__1 = sscanf(line,"%12c %c",snamet,&tmpchar);
   ltestt=FALSE_;
   if (tmpchar=='T')ltestt=TRUE_;
    if (i__1 < 2) {
	goto L60;
    }
    for (i__ = 1; i__ <= 9; ++i__) {
	if (s_cmp(snamet, snames[i__ - 1] , (ftnlen)12, (ftnlen)12) == 
		0) {
	    goto L50;
	}
/* L40: */
    }
    printf("SUBPROGRAM NAME %s NOT RECOGNIZED\n****** TESTS ABANDONED ******\n",snamet);
    exit(1);
L50:
    ltest[i__ - 1] = ltestt;
    goto L30;

L60:
/*    cl__1.cerr = 0;
    cl__1.cunit = 5;
    cl__1.csta = 0;
    f_clos(&cl__1);*/

/*     Compute EPS (the machine precision). */

    eps = 1.f;
L70:
    r__1 = eps + 1.f;
    if (sdiff_(&r__1, &c_b91) == 0.f) {
	goto L80;
    }
    eps *= .5f;
    goto L70;
L80:
    eps += eps;
    printf("RELATIVE MACHINE PRECISION IS TAKEN TO BE %9.1g\n",eps);

/*     Check the reliability of CMMCH using exact data. */

    n = 32;
    i__1 = n;
    for (j = 1; j <= i__1; ++j) {
	i__2 = n;
	for (i__ = 1; i__ <= i__2; ++i__) {
	    i__3 = i__ + j * 65 - 66;
/* Computing MAX */
	    i__5 = i__ - j + 1;
	    i__4 = f2cmax(i__5,0);
	    ab[i__3].r = (real) i__4, ab[i__3].i = 0.f;
/* L90: */
	}
	i__2 = j + 4224;
	ab[i__2].r = (real) j, ab[i__2].i = 0.f;
	i__2 = (j + 65) * 65 - 65;
	ab[i__2].r = (real) j, ab[i__2].i = 0.f;
	i__2 = j - 1;
	c__[i__2].r = 0.f, c__[i__2].i = 0.f;
/* L100: */
    }
    i__1 = n;
    for (j = 1; j <= i__1; ++j) {
	i__2 = j - 1;
	i__3 = j * ((j + 1) * j) / 2 - (j + 1) * j * (j - 1) / 3;
	cc[i__2].r = (real) i__3, cc[i__2].i = 0.f;
/* L110: */
    }
/*     CC holds the exact result. On exit from CMMCH CT holds */
/*     the result computed by CMMCH. */
    *(unsigned char *)transa = 'N';
    *(unsigned char *)transb = 'N';
    cmmch_(transa, transb, &n, &c__1, &n, &c_b2, ab, &c__65, &ab[4225], &
	    c__65, &c_b1, c__, &c__65, ct, g, cc, &c__65, &eps, &err, &fatal, 
	    &c__6, &c_true);
    same = lce_(cc, ct, &n);
    if (! same || err != 0.f) {
      printf("ERROR IN CMMCH - IN-LINE DOT PRODUCTS ARE BEING EVALUATED WRONGLY\n");
      printf("CMMCH WAS CALLED WITH TRANSA = %s AND TRANSB = %s\n", transa,transb);
      printf("AND RETURNED SAME = %c AND ERR = %12.3f.\n",(same==FALSE_? 'F':'T'),err);
      printf("THIS MAY BE DUE TO FAULTS IN THE ARITHMETIC OR THE COMPILER.\n");
      printf("****** TESTS ABANDONED ******\n");
      exit(1);
    }
    *(unsigned char *)transb = 'C';
    cmmch_(transa, transb, &n, &c__1, &n, &c_b2, ab, &c__65, &ab[4225], &
	    c__65, &c_b1, c__, &c__65, ct, g, cc, &c__65, &eps, &err, &fatal, 
	    &c__6, &c_true);
    same = lce_(cc, ct, &n);
    if (! same || err != 0.f) {
      printf("ERROR IN CMMCH - IN-LINE DOT PRODUCTS ARE BEING EVALUATED WRONGLY\n");
      printf("CMMCH WAS CALLED WITH TRANSA = %s AND TRANSB = %s\n", transa,transb);
      printf("AND RETURNED SAME = %c AND ERR = %12.3f.\n",(same==FALSE_? 'F':'T'),err);
      printf("THIS MAY BE DUE TO FAULTS IN THE ARITHMETIC OR THE COMPILER.\n");
      printf("****** TESTS ABANDONED ******\n");
      exit(1);
    }
    i__1 = n;
    for (j = 1; j <= i__1; ++j) {
	i__2 = j + 4224;
	i__3 = n - j + 1;
	ab[i__2].r = (real) i__3, ab[i__2].i = 0.f;
	i__2 = (j + 65) * 65 - 65;
	i__3 = n - j + 1;
	ab[i__2].r = (real) i__3, ab[i__2].i = 0.f;
/* L120: */
    }
    i__1 = n;
    for (j = 1; j <= i__1; ++j) {
	i__2 = n - j;
	i__3 = j * ((j + 1) * j) / 2 - (j + 1) * j * (j - 1) / 3;
	cc[i__2].r = (real) i__3, cc[i__2].i = 0.f;
/* L130: */
    }
    *(unsigned char *)transa = 'C';
    *(unsigned char *)transb = 'N';
    cmmch_(transa, transb, &n, &c__1, &n, &c_b2, ab, &c__65, &ab[4225], &
	    c__65, &c_b1, c__, &c__65, ct, g, cc, &c__65, &eps, &err, &fatal, 
	    &c__6, &c_true);
    same = lce_(cc, ct, &n);
    if (! same || err != 0.f) {
      printf("ERROR IN CMMCH - IN-LINE DOT PRODUCTS ARE BEING EVALUATED WRONGLY\n");
      printf("CMMCH WAS CALLED WITH TRANSA = %s AND TRANSB = %s\n", transa,transb);
      printf("AND RETURNED SAME = %c AND ERR = %12.3f.\n",(same==FALSE_? 'F':'T'),err);
      printf("THIS MAY BE DUE TO FAULTS IN THE ARITHMETIC OR THE COMPILER.\n");
      printf("****** TESTS ABANDONED ******\n");
      exit(1);
    }
    *(unsigned char *)transb = 'C';
    cmmch_(transa, transb, &n, &c__1, &n, &c_b2, ab, &c__65, &ab[4225], &
	    c__65, &c_b1, c__, &c__65, ct, g, cc, &c__65, &eps, &err, &fatal, 
	    &c__6, &c_true);
    same = lce_(cc, ct, &n);
    if (! same || err != 0.f) {
      printf("ERROR IN CMMCH - IN-LINE DOT PRODUCTS ARE BEING EVALUATED WRONGLY\n");
      printf("CMMCH WAS CALLED WITH TRANSA = %s AND TRANSB = %s\n", transa,transb);
      printf("AND RETURNED SAME = %c AND ERR = %12.3f.\n",(same==FALSE_? 'F':'T'),err);
      printf("THIS MAY BE DUE TO FAULTS IN THE ARITHMETIC OR THE COMPILER.\n");
      printf("****** TESTS ABANDONED ******\n");
      exit(1);
    }

/*     Test each subroutine in turn. */

    for (isnum = 1; isnum <= 9; ++isnum) {
	if (! ltest[isnum - 1]) {
/*           Subprogram is not to be tested. */
           printf("%12s WAS NOT TESTED\n",snames[isnum-1]);
	} else {
	    s_copy(srnamc_1.srnamt, snames[isnum - 1], (ftnlen)12, (
		    ftnlen)12);
/*           Test error exits. */
	    if (tsterr) {
		cc3chke_(snames[isnum - 1]);
	    }
/*           Test computations. */
	    infoc_1.infot = 0;
	    infoc_1.ok = TRUE_;
	    fatal = FALSE_;
	    switch (isnum) {
		case 1:  goto L140;
		case 2:  goto L150;
		case 3:  goto L150;
		case 4:  goto L160;
		case 5:  goto L160;
		case 6:  goto L170;
		case 7:  goto L170;
		case 8:  goto L180;
		case 9:  goto L180;
	    }
/*           Test CGEMM, 01. */
L140:
	    if (corder) {
		cchk1_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			nbet, bet, &c__65, ab, aa, as, &ab[4225], bb, bs, c__,
			 cc, cs, ct, g, &c__0);
	    }
	    if (rorder) {
		cchk1_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			nbet, bet, &c__65, ab, aa, as, &ab[4225], bb, bs, c__,
			 cc, cs, ct, g, &c__1);
	    }
	    goto L190;
/*           Test CHEMM, 02, CSYMM, 03. */
L150:
	    if (corder) {
		cchk2_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			nbet, bet, &c__65, ab, aa, as, &ab[4225], bb, bs, c__,
			 cc, cs, ct, g, &c__0);
	    }
	    if (rorder) {
		cchk2_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			nbet, bet, &c__65, ab, aa, as, &ab[4225], bb, bs, c__,
			 cc, cs, ct, g, &c__1);
	    }
	    goto L190;
/*           Test CTRMM, 04, CTRSM, 05. */
L160:
	    if (corder) {
		cchk3_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			c__65, ab, aa, as, &ab[4225], bb, bs, ct, g, c__, &
			c__0);
	    }
	    if (rorder) {
		cchk3_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			c__65, ab, aa, as, &ab[4225], bb, bs, ct, g, c__, &
			c__1);
	    }
	    goto L190;
/*           Test CHERK, 06, CSYRK, 07. */
L170:
	    if (corder) {
		cchk4_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			nbet, bet, &c__65, ab, aa, as, &ab[4225], bb, bs, c__,
			 cc, cs, ct, g, &c__0);
	    }
	    if (rorder) {
		cchk4_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			nbet, bet, &c__65, ab, aa, as, &ab[4225], bb, bs, c__,
			 cc, cs, ct, g, &c__1);
	    }
	    goto L190;
/*           Test CHER2K, 08, CSYR2K, 09. */
L180:
	    if (corder) {
		cchk5_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			nbet, bet, &c__65, ab, aa, as, bb, bs, c__, cc, cs, 
			ct, g, w, &c__0);
	    }
	    if (rorder) {
		cchk5_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			nbet, bet, &c__65, ab, aa, as, bb, bs, c__, cc, cs, 
			ct, g, w, &c__1);
	    }
	    goto L190;

L190:
	    if (fatal && sfatal) {
		goto L210;
	    }
	}
/* L200: */
    }
    printf("\nEND OF TESTS\n");
    goto L230;

L210:
    printf("\n****** FATAL ERROR - TESTS ABANDONED ******\n");
    goto L230;

L220:
    printf("AMEND DATA FILE OR INCREASE ARRAY SIZES IN PROGRAM\n");
    printf("****** TESTS ABANDONED ******\n");
L230:
    if (trace) {
/*	cl__1.cerr = 0;
	cl__1.cunit = ntra;
	cl__1.csta = 0;
	f_clos(&cl__1);*/
    }
/*    cl__1.cerr = 0;
    cl__1.cunit = 6;
    cl__1.csta = 0;
    f_clos(&cl__1);
    s_stop("", (ftnlen)0);*/
     exit(0);

/*     End of CBLAT3. */

    return 0;
} /* MAIN__ */

/* Subroutine */ int cchk1_(char *sname, real *eps, real *thresh, integer *
	nout, integer *ntra, logical *trace, logical *rewi, logical *fatal, 
	integer *nidim, integer *idim, integer *nalf, complex *alf, integer *
	nbet, complex *bet, integer *nmax, complex *a, complex *aa, complex *
	as, complex *b, complex *bb, complex *bs, complex *c__, complex *cc, 
	complex *cs, complex *ct, real *g, integer *iorder)
{
    /* Initialized data */

    static char ich[3] = "NTC";

    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, c_dim1, c_offset, i__1, i__2, 
	    i__3, i__4, i__5, i__6, i__7, i__8;

    /* Local variables */
    complex beta;
    integer ldas, ldbs, ldcs;
    logical same, null;
    integer i__, k, m, n;
    extern /* Subroutine */ int cmake_(char *, char *, char *, integer *, 
	    integer *, complex *, integer *, complex *, integer *, logical *, 
	    complex *);
    complex alpha;
    extern /* Subroutine */ int cmmch_(char *, char *, integer *, integer *, 
	    integer *, complex *, complex *, integer *, complex *, integer *, 
	    complex *, complex *, integer *, complex *, real *, complex *, 
	    integer *, real *, real *, logical *, integer *, logical *);
    logical isame[13], trana, tranb;
    integer nargs;
    logical reset;
    extern /* Subroutine */ int cprcn1_(integer *, integer *, char *, integer 
	    *, char *, char *, integer *, integer *, integer *, complex *, 
	    integer *, integer *, complex *, integer *);
    integer ia, ib, ma, mb, na, nb, nc, ik, im, in;
    extern /* Subroutine */ int ccgemm_(integer *, char *, char *, integer *, 
	    integer *, integer *, complex *, complex *, integer *, complex *, 
	    integer *, complex *, complex *, integer *);
    integer ks, ms, ns;
    extern logical lceres_(char *, char *, integer *, integer *, complex *, 
	    complex *, integer *);
    char tranas[1], tranbs[1], transa[1], transb[1];
    real errmax;
    integer ica, icb, laa, lbb, lda, lcc, ldb, ldc;
    extern logical lce_(complex *, complex *, integer *);
    complex als, bls;
    real err;

/*  Tests CGEMM. */

/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

    /* Parameter adjustments */
    --idim;
    --alf;
    --bet;
    --g;
    --ct;
    --cs;
    --cc;
    c_dim1 = *nmax;
    c_offset = 1 + c_dim1 * 1;
    c__ -= c_offset;
    --bs;
    --bb;
    b_dim1 = *nmax;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    --as;
    --aa;
    a_dim1 = *nmax;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;

    /* Function Body */

    nargs = 13;
    nc = 0;
    reset = TRUE_;
    errmax = 0.f;

    i__1 = *nidim;
    for (im = 1; im <= i__1; ++im) {
	m = idim[im];

	i__2 = *nidim;
	for (in = 1; in <= i__2; ++in) {
	    n = idim[in];
/*           Set LDC to 1 more than minimum value if room. */
	    ldc = m;
	    if (ldc < *nmax) {
		++ldc;
	    }
/*           Skip tests if not enough room. */
	    if (ldc > *nmax) {
		goto L100;
	    }
	    lcc = ldc * n;
	    null = n <= 0 || m <= 0;

	    i__3 = *nidim;
	    for (ik = 1; ik <= i__3; ++ik) {
		k = idim[ik];

		for (ica = 1; ica <= 3; ++ica) {
		    *(unsigned char *)transa = *(unsigned char *)&ich[ica - 1]
			    ;
		    trana = *(unsigned char *)transa == 'T' || *(unsigned 
			    char *)transa == 'C';

		    if (trana) {
			ma = k;
			na = m;
		    } else {
			ma = m;
			na = k;
		    }
/*                 Set LDA to 1 more than minimum value if room. */
		    lda = ma;
		    if (lda < *nmax) {
			++lda;
		    }
/*                 Skip tests if not enough room. */
		    if (lda > *nmax) {
			goto L80;
		    }
		    laa = lda * na;

/*                 Generate the matrix A. */

		    cmake_("ge", " ", " ", &ma, &na, &a[a_offset], nmax, &aa[
			    1], &lda, &reset, &c_b1);

		    for (icb = 1; icb <= 3; ++icb) {
			*(unsigned char *)transb = *(unsigned char *)&ich[icb 
				- 1];
			tranb = *(unsigned char *)transb == 'T' || *(unsigned 
				char *)transb == 'C';

			if (tranb) {
			    mb = n;
			    nb = k;
			} else {
			    mb = k;
			    nb = n;
			}
/*                    Set LDB to 1 more than minimum value if room. */
			ldb = mb;
			if (ldb < *nmax) {
			    ++ldb;
			}
/*                    Skip tests if not enough room. */
			if (ldb > *nmax) {
			    goto L70;
			}
			lbb = ldb * nb;

/*                    Generate the matrix B. */

			cmake_("ge", " ", " ", &mb, &nb, &b[b_offset], nmax, &
				bb[1], &ldb, &reset, &c_b1);

			i__4 = *nalf;
			for (ia = 1; ia <= i__4; ++ia) {
			    i__5 = ia;
			    alpha.r = alf[i__5].r, alpha.i = alf[i__5].i;

			    i__5 = *nbet;
			    for (ib = 1; ib <= i__5; ++ib) {
				i__6 = ib;
				beta.r = bet[i__6].r, beta.i = bet[i__6].i;

/*                          Generate the matrix C. */

				cmake_("ge", " ", " ", &m, &n, &c__[c_offset],
					 nmax, &cc[1], &ldc, &reset, &c_b1);

				++nc;

/*                          Save every datum before calling the */
/*                          subroutine. */

				*(unsigned char *)tranas = *(unsigned char *)
					transa;
				*(unsigned char *)tranbs = *(unsigned char *)
					transb;
				ms = m;
				ns = n;
				ks = k;
				als.r = alpha.r, als.i = alpha.i;
				i__6 = laa;
				for (i__ = 1; i__ <= i__6; ++i__) {
				    i__7 = i__;
				    i__8 = i__;
				    as[i__7].r = aa[i__8].r, as[i__7].i = aa[
					    i__8].i;
/* L10: */
				}
				ldas = lda;
				i__6 = lbb;
				for (i__ = 1; i__ <= i__6; ++i__) {
				    i__7 = i__;
				    i__8 = i__;
				    bs[i__7].r = bb[i__8].r, bs[i__7].i = bb[
					    i__8].i;
/* L20: */
				}
				ldbs = ldb;
				bls.r = beta.r, bls.i = beta.i;
				i__6 = lcc;
				for (i__ = 1; i__ <= i__6; ++i__) {
				    i__7 = i__;
				    i__8 = i__;
				    cs[i__7].r = cc[i__8].r, cs[i__7].i = cc[
					    i__8].i;
/* L30: */
				}
				ldcs = ldc;

/*                          Call the subroutine. */

				if (*trace) {
				    cprcn1_(ntra, &nc, sname, iorder, transa, 
					    transb, &m, &n, &k, &alpha, &lda, 
					    &ldb, &beta, &ldc);
				}
				if (*rewi) {
/*				    al__1.aerr = 0;
				    al__1.aunit = *ntra;
				    f_rew(&al__1); */
				}
				ccgemm_(iorder, transa, transb, &m, &n, &k, &
					alpha, &aa[1], &lda, &bb[1], &ldb, &
					beta, &cc[1], &ldc);

/*                          Check if error-exit was taken incorrectly. */

				if (! infoc_1.ok) {
//				    io___128.ciunit = *nout;
//				    s_wsfe(&io___128);
//				    e_wsfe();
				    printf(" *** FATAL ERROR - ERROR-CALL MYEXIT TAKEN ON VALID CALL\n");
				    *fatal = TRUE_;
				    goto L120;
				}

/*                          See what data changed inside subroutines. */

				isame[0] = *(unsigned char *)transa == *(
					unsigned char *)tranas;
				isame[1] = *(unsigned char *)transb == *(
					unsigned char *)tranbs;
				isame[2] = ms == m;
				isame[3] = ns == n;
				isame[4] = ks == k;
				isame[5] = als.r == alpha.r && als.i == 
					alpha.i;
				isame[6] = lce_(&as[1], &aa[1], &laa);
				isame[7] = ldas == lda;
				isame[8] = lce_(&bs[1], &bb[1], &lbb);
				isame[9] = ldbs == ldb;
				isame[10] = bls.r == beta.r && bls.i == 
					beta.i;
				if (null) {
				    isame[11] = lce_(&cs[1], &cc[1], &lcc);
				} else {
				    isame[11] = lceres_("ge", " ", &m, &n, &
					    cs[1], &cc[1], &ldc);
				}
				isame[12] = ldcs == ldc;

/*                          If data was incorrectly changed, report */
/*                          and return. */

				same = TRUE_;
				i__6 = nargs;
				for (i__ = 1; i__ <= i__6; ++i__) {
				    same = same && isame[i__ - 1];
				    if (! isame[i__ - 1]) {
    				printf(" ******* FATAL ERROR - PARAMETER NUMBER %d WAS CHANGED INCORRECTLY *******\n",i__);;
				    }
/* L40: */
				}
				if (! same) {
				    *fatal = TRUE_;
				    goto L120;
				}

				if (! null) {

/*                             Check the result. */

				    cmmch_(transa, transb, &m, &n, &k, &alpha,
					     &a[a_offset], nmax, &b[b_offset],
					     nmax, &beta, &c__[c_offset], 
					    nmax, &ct[1], &g[1], &cc[1], &ldc,
					     eps, &err, fatal, nout, &c_true);
				    errmax = f2cmax(errmax,err);
/*                             If got really bad answer, report and */
/*                             return. */
				    if (*fatal) {
					goto L120;
				    }
				}

/* L50: */
			    }

/* L60: */
			}

L70:
			;
		    }

L80:
		    ;
		}

/* L90: */
	    }

L100:
	    ;
	}

/* L110: */
    }

/*     Report result. */

    if (errmax < *thresh) {
	if (*iorder == 0) {
            printf("%s PASSED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%d CALLS)\n",sname,nc);
	}
	if (*iorder == 1) {
            printf("%s PASSED THE ROW-MAJOR COMPUTATIONAL TESTS (%d CALLS)\n",sname,nc);
	}
    } else {
	if (*iorder == 0) {
            printf("%s COMPLETED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%d CALLS)/n",sname,nc);
	    printf("***** BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******/n",errmax);
	}
	if (*iorder == 1) {
            printf("%s COMPLETED THE ROW-MAJOR COMPUTATIONAL TESTS (%d CALLS)/n",sname,nc);
	    printf("***** BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******/n",errmax);
	}
    }
    goto L130;

L120:
    printf(" ******* %s FAILED ON CALL NUMBER:\n",sname);
    cprcn1_(nout, &nc, sname, iorder, transa, transb, &m, &n, &k, &alpha, &
	    lda, &ldb, &beta, &ldc);

L130:
    return 0;

/* 9995 FORMAT( 1X, I6, ': ', A12,'(''', A1, ''',''', A1, ''',', */
/*     $     3( I3, ',' ), '(', F4.1, ',', F4.1, '), A,', I3, ', B,', I3, */
/*     $     ',(', F4.1, ',', F4.1, '), C,', I3, ').' ) */

/*     End of CCHK1. */

} /* cchk1_ */


/* Subroutine */ int cprcn1_(integer *nout, integer *nc, char *sname, integer 
	*iorder, char *transa, char *transb, integer *m, integer *n, integer *
	k, complex *alpha, integer *lda, integer *ldb, complex *beta, integer 
	*ldc)
{
    /* Local variables */
    char crc[14], cta[14], ctb[14];

    if (*(unsigned char *)transa == 'N') {
	s_copy(cta, "  CblasNoTrans", (ftnlen)14, (ftnlen)14);
    } else if (*(unsigned char *)transa == 'T') {
	s_copy(cta, "    CblasTrans", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(cta, "CblasConjTrans", (ftnlen)14, (ftnlen)14);
    }
    if (*(unsigned char *)transb == 'N') {
	s_copy(ctb, "  CblasNoTrans", (ftnlen)14, (ftnlen)14);
    } else if (*(unsigned char *)transb == 'T') {
	s_copy(ctb, "    CblasTrans", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(ctb, "CblasConjTrans", (ftnlen)14, (ftnlen)14);
    }
    if (*iorder == 1) {
	s_copy(crc, " CblasRowMajor", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(crc, " CblasColMajor", (ftnlen)14, (ftnlen)14);
    }
    printf("%6d: %s %s %s %s\n",*nc,sname,crc,cta,ctb);
    printf("%d %d %d (%4.1f,%4.1f) , A, %d, B, %d, (%4.1f,%4.1f) , C, %d.\n",*m,*n,*k,alpha->r,alpha->i,*lda,*ldb,beta->r,beta->i,*ldc);
    return 0;
} /* cprcn1_ */


/* Subroutine */ int cchk2_(char *sname, real *eps, real *thresh, integer *
	nout, integer *ntra, logical *trace, logical *rewi, logical *fatal, 
	integer *nidim, integer *idim, integer *nalf, complex *alf, integer *
	nbet, complex *bet, integer *nmax, complex *a, complex *aa, complex *
	as, complex *b, complex *bb, complex *bs, complex *c__, complex *cc, 
	complex *cs, complex *ct, real *g, integer *iorder)
{
    /* Initialized data */

    static char ichs[2] = "LR";
    static char ichu[2] = "UL";

    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, c_dim1, c_offset, i__1, i__2, 
	    i__3, i__4, i__5, i__6, i__7;

    /* Local variables */
    complex beta;
    integer ldas, ldbs, ldcs;
    logical same;
    char side[1];
    logical conj, left, null;
    char uplo[1];
    integer i__, m, n;
    extern /* Subroutine */ int cmake_(char *, char *, char *, integer *, 
	    integer *, complex *, integer *, complex *, integer *, logical *, 
	    complex *);
    complex alpha;
    extern /* Subroutine */ int cmmch_(char *, char *, integer *, integer *, 
	    integer *, complex *, complex *, integer *, complex *, integer *, 
	    complex *, complex *, integer *, complex *, real *, complex *, 
	    integer *, real *, real *, logical *, integer *, logical *);
    logical isame[13];
    char sides[1];
    integer nargs;
    logical reset;
    char uplos[1];
    extern /* Subroutine */ int cprcn2_(integer *, integer *, char *, integer 
	    *, char *, char *, integer *, integer *, complex *, integer *, 
	    integer *, complex *, integer *);
    integer ia, ib, na, nc, im, in;
    extern /* Subroutine */ int cchemm_(integer *, char *, char *, integer *, 
	    integer *, complex *, complex *, integer *, complex *, integer *, 
	    complex *, complex *, integer *);
    integer ms, ns;
    extern logical lceres_(char *, char *, integer *, integer *, complex *, 
	    complex *, integer *);
    extern /* Subroutine */ int ccsymm_(integer *, char *, char *, integer *, 
	    integer *, complex *, complex *, integer *, complex *, integer *, 
	    complex *, complex *, integer *);
    real errmax;
    integer laa, lbb, lda, lcc, ldb, ldc;
    extern logical lce_(complex *, complex *, integer *);
    integer ics;
    complex als, bls;
    integer icu;
    real err;

/*  Tests CHEMM and CSYMM. */

/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

    /* Parameter adjustments */
    --idim;
    --alf;
    --bet;
    --g;
    --ct;
    --cs;
    --cc;
    c_dim1 = *nmax;
    c_offset = 1 + c_dim1 * 1;
    c__ -= c_offset;
    --bs;
    --bb;
    b_dim1 = *nmax;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    --as;
    --aa;
    a_dim1 = *nmax;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;

    /* Function Body */
    conj = s_cmp(sname + 7, "he", (ftnlen)2, (ftnlen)2) == 0;

    nargs = 12;
    nc = 0;
    reset = TRUE_;
    errmax = 0.f;

    i__1 = *nidim;
    for (im = 1; im <= i__1; ++im) {
	m = idim[im];

	i__2 = *nidim;
	for (in = 1; in <= i__2; ++in) {
	    n = idim[in];
/*           Set LDC to 1 more than minimum value if room. */
	    ldc = m;
	    if (ldc < *nmax) {
		++ldc;
	    }
/*           Skip tests if not enough room. */
	    if (ldc > *nmax) {
		goto L90;
	    }
	    lcc = ldc * n;
	    null = n <= 0 || m <= 0;
/*           Set LDB to 1 more than minimum value if room. */
	    ldb = m;
	    if (ldb < *nmax) {
		++ldb;
	    }
/*           Skip tests if not enough room. */
	    if (ldb > *nmax) {
		goto L90;
	    }
	    lbb = ldb * n;

/*           Generate the matrix B. */

	    cmake_("ge", " ", " ", &m, &n, &b[b_offset], nmax, &bb[1], &ldb, &
		    reset, &c_b1);

	    for (ics = 1; ics <= 2; ++ics) {
		*(unsigned char *)side = *(unsigned char *)&ichs[ics - 1];
		left = *(unsigned char *)side == 'L';

		if (left) {
		    na = m;
		} else {
		    na = n;
		}
/*              Set LDA to 1 more than minimum value if room. */
		lda = na;
		if (lda < *nmax) {
		    ++lda;
		}
/*              Skip tests if not enough room. */
		if (lda > *nmax) {
		    goto L80;
		}
		laa = lda * na;

		for (icu = 1; icu <= 2; ++icu) {
		    *(unsigned char *)uplo = *(unsigned char *)&ichu[icu - 1];

/*                 Generate the hermitian or symmetric matrix A. */

		    cmake_(sname + 7, uplo, " ", &na, &na, &a[a_offset], nmax,
			     &aa[1], &lda, &reset, &c_b1);

		    i__3 = *nalf;
		    for (ia = 1; ia <= i__3; ++ia) {
			i__4 = ia;
			alpha.r = alf[i__4].r, alpha.i = alf[i__4].i;

			i__4 = *nbet;
			for (ib = 1; ib <= i__4; ++ib) {
			    i__5 = ib;
			    beta.r = bet[i__5].r, beta.i = bet[i__5].i;

/*                       Generate the matrix C. */

			    cmake_("ge", " ", " ", &m, &n, &c__[c_offset], 
				    nmax, &cc[1], &ldc, &reset, &c_b1);

			    ++nc;

/*                       Save every datum before calling the */
/*                       subroutine. */

			    *(unsigned char *)sides = *(unsigned char *)side;
			    *(unsigned char *)uplos = *(unsigned char *)uplo;
			    ms = m;
			    ns = n;
			    als.r = alpha.r, als.i = alpha.i;
			    i__5 = laa;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				i__6 = i__;
				i__7 = i__;
				as[i__6].r = aa[i__7].r, as[i__6].i = aa[i__7]
					.i;
/* L10: */
			    }
			    ldas = lda;
			    i__5 = lbb;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				i__6 = i__;
				i__7 = i__;
				bs[i__6].r = bb[i__7].r, bs[i__6].i = bb[i__7]
					.i;
/* L20: */
			    }
			    ldbs = ldb;
			    bls.r = beta.r, bls.i = beta.i;
			    i__5 = lcc;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				i__6 = i__;
				i__7 = i__;
				cs[i__6].r = cc[i__7].r, cs[i__6].i = cc[i__7]
					.i;
/* L30: */
			    }
			    ldcs = ldc;

/*                       Call the subroutine. */

			    if (*trace) {
				cprcn2_(ntra, &nc, sname, iorder, side, uplo, 
					&m, &n, &alpha, &lda, &ldb, &beta, &
					ldc)
					;
			    }
			    if (*rewi) {
/*				al__1.aerr = 0;
				al__1.aunit = *ntra;
				f_rew(&al__1);*/
			    }
			    if (conj) {
				cchemm_(iorder, side, uplo, &m, &n, &alpha, &
					aa[1], &lda, &bb[1], &ldb, &beta, &cc[
					1], &ldc);
			    } else {
				ccsymm_(iorder, side, uplo, &m, &n, &alpha, &
					aa[1], &lda, &bb[1], &ldb, &beta, &cc[
					1], &ldc);
			    }

/*                       Check if error-exit was taken incorrectly. */

			    if (! infoc_1.ok) {
    printf("*** FATAL ERROR - ERROR-CALL MYEXIT TAKEN ON VALID CALL\n");
				*fatal = TRUE_;
				goto L110;
			    }

/*                       See what data changed inside subroutines. */

			    isame[0] = *(unsigned char *)sides == *(unsigned 
				    char *)side;
			    isame[1] = *(unsigned char *)uplos == *(unsigned 
				    char *)uplo;
			    isame[2] = ms == m;
			    isame[3] = ns == n;
			    isame[4] = als.r == alpha.r && als.i == alpha.i;
			    isame[5] = lce_(&as[1], &aa[1], &laa);
			    isame[6] = ldas == lda;
			    isame[7] = lce_(&bs[1], &bb[1], &lbb);
			    isame[8] = ldbs == ldb;
			    isame[9] = bls.r == beta.r && bls.i == beta.i;
			    if (null) {
				isame[10] = lce_(&cs[1], &cc[1], &lcc);
			    } else {
				isame[10] = lceres_("ge", " ", &m, &n, &cs[1],
					 &cc[1], &ldc);
			    }
			    isame[11] = ldcs == ldc;

/*                       If data was incorrectly changed, report and */
/*                       return. */

			    same = TRUE_;
			    i__5 = nargs;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				same = same && isame[i__ - 1];
				if (! isame[i__ - 1]) {
				printf(" ******* FATAL ERROR - PARAMETER NUMBER %d WAS CHANGED INCORRECTLY *******\n",i__);
				}
/* L40: */
			    }
			    if (! same) {
				*fatal = TRUE_;
				goto L110;
			    }

			    if (! null) {

/*                          Check the result. */

				if (left) {
				    cmmch_("N", "N", &m, &n, &m, &alpha, &a[
					    a_offset], nmax, &b[b_offset], 
					    nmax, &beta, &c__[c_offset], nmax,
					     &ct[1], &g[1], &cc[1], &ldc, eps,
					     &err, fatal, nout, &c_true);
				} else {
				    cmmch_("N", "N", &m, &n, &n, &alpha, &b[
					    b_offset], nmax, &a[a_offset], 
					    nmax, &beta, &c__[c_offset], nmax,
					     &ct[1], &g[1], &cc[1], &ldc, eps,
					     &err, fatal, nout, &c_true);
				}
				errmax = f2cmax(errmax,err);
/*                          If got really bad answer, report and */
/*                          return. */
				if (*fatal) {
				    goto L110;
				}
			    }

/* L50: */
			}

/* L60: */
		    }

/* L70: */
		}

L80:
		;
	    }

L90:
	    ;
	}

/* L100: */
    }

/*     Report result. */

    if (errmax < *thresh) {
	if (*iorder == 0) {
            printf("%s PASSED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%d CALLS)\n",sname,nc);
	}
	if (*iorder == 1) {
            printf("%s PASSED THE ROW-MAJOR COMPUTATIONAL TESTS (%d CALLS)\n",sname,nc);
	}
    } else {
	if (*iorder == 0) {
            printf("%s COMPLETED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%d CALLS)/n",sname,nc);
	    printf("***** BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******/n",errmax);
	}
	if (*iorder == 1) {
            printf("%s COMPLETED THE ROW-MAJOR COMPUTATIONAL TESTS (%d CALLS)/n",sname,nc);
	    printf("***** BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******/n",errmax);
	}
    }
    goto L120;

L110:
    printf(" ******* %s FAILED ON CALL NUMBER:\n",sname);
    cprcn2_(nout, &nc, sname, iorder, side, uplo, &m, &n, &alpha, &lda, &ldb, 
	    &beta, &ldc);

L120:
    return 0;

/* 9995 FORMAT(1X, I6, ': ', A12,'(', 2( '''', A1, ''',' ), 2( I3, ',' ), */
/*     $      '(', F4.1, ',', F4.1, '), A,', I3, ', B,', I3, ',(', F4.1, */
/*     $      ',', F4.1, '), C,', I3, ')    .' ) */

/*     End of CCHK2. */

} /* cchk2_ */


/* Subroutine */ int cprcn2_(integer *nout, integer *nc, char *sname, integer 
	*iorder, char *side, char *uplo, integer *m, integer *n, complex *
	alpha, integer *lda, integer *ldb, complex *beta, integer *ldc)
{
    /* Local variables */
    char cs[14], cu[14], crc[14];

    if (*(unsigned char *)side == 'L') {
	s_copy(cs, "     CblasLeft", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(cs, "    CblasRight", (ftnlen)14, (ftnlen)14);
    }
    if (*(unsigned char *)uplo == 'U') {
	s_copy(cu, "    CblasUpper", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(cu, "    CblasLower", (ftnlen)14, (ftnlen)14);
    }
    if (*iorder == 1) {
	s_copy(crc, " CblasRowMajor", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(crc, " CblasColMajor", (ftnlen)14, (ftnlen)14);
    }
    printf("%6d: %s %s %s %s\n",*nc,sname,crc,cs,cu);
    printf("%d %d (%4.1f,%4.1f) , A, %d, B, %d, (%4.1f,%4.1f) , C, %d.\n",*m,*n,alpha->r,alpha->i,*lda,*ldb,beta->r,beta->i,*ldc);
    return 0;
} /* cprcn2_ */


/* Subroutine */ int cchk3_(char *sname, real *eps, real *thresh, integer *
	nout, integer *ntra, logical *trace, logical *rewi, logical *fatal, 
	integer *nidim, integer *idim, integer *nalf, complex *alf, integer *
	nmax, complex *a, complex *aa, complex *as, complex *b, complex *bb, 
	complex *bs, complex *ct, real *g, complex *c__, integer *iorder)
{
    /* Initialized data */

    static char ichu[2] = "UL";
    static char icht[3] = "NTC";
    static char ichd[2] = "UN";
    static char ichs[2] = "LR";

    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, c_dim1, c_offset, i__1, i__2, 
	    i__3, i__4, i__5, i__6, i__7;
    complex q__1;

    /* Local variables */
    char diag[1];
    integer ldas, ldbs;
    logical same;
    char side[1];
    logical left, null;
    char uplo[1];
    integer i__, j, m, n;
    extern /* Subroutine */ int cmake_(char *, char *, char *, integer *, 
	    integer *, complex *, integer *, complex *, integer *, logical *, 
	    complex *);
    complex alpha;
    char diags[1];
    extern /* Subroutine */ int cmmch_(char *, char *, integer *, integer *, 
	    integer *, complex *, complex *, integer *, complex *, integer *, 
	    complex *, complex *, integer *, complex *, real *, complex *, 
	    integer *, real *, real *, logical *, integer *, logical *);
    logical isame[13];
    char sides[1];
    integer nargs;
    logical reset;
    char uplos[1];
    extern /* Subroutine */ int cprcn3_(integer *, integer *, char *, integer 
	    *, char *, char *, char *, char *, integer *, integer *, complex *
	    , integer *, integer *);
    integer ia, na, nc, im, in, ms, ns;
    extern logical lceres_(char *, char *, integer *, integer *, complex *, 
	    complex *, integer *);
    extern /* Subroutine */ int cctrmm_(integer *, char *, char *, char *, 
	    char *, integer *, integer *, complex *, complex *, integer *, 
	    complex *, integer *);
    char tranas[1], transa[1];
    extern /* Subroutine */ int cctrsm_(integer *, char *, char *, char *, 
	    char *, integer *, integer *, complex *, complex *, integer *, 
	    complex *, integer *);
    real errmax;
    integer laa, icd, lbb, lda, ldb;
    extern logical lce_(complex *, complex *, integer *);
    integer ics;
    complex als;
    integer ict, icu;
    real err;

/*  Tests CTRMM and CTRSM. */

/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

    /* Parameter adjustments */
    --idim;
    --alf;
    c_dim1 = *nmax;
    c_offset = 1 + c_dim1 * 1;
    c__ -= c_offset;
    --g;
    --ct;
    --bs;
    --bb;
    b_dim1 = *nmax;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    --as;
    --aa;
    a_dim1 = *nmax;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;

    /* Function Body */

    nargs = 11;
    nc = 0;
    reset = TRUE_;
    errmax = 0.f;
/*     Set up zero matrix for CMMCH. */
    i__1 = *nmax;
    for (j = 1; j <= i__1; ++j) {
	i__2 = *nmax;
	for (i__ = 1; i__ <= i__2; ++i__) {
	    i__3 = i__ + j * c_dim1;
	    c__[i__3].r = 0.f, c__[i__3].i = 0.f;
/* L10: */
	}
/* L20: */
    }

    i__1 = *nidim;
    for (im = 1; im <= i__1; ++im) {
	m = idim[im];

	i__2 = *nidim;
	for (in = 1; in <= i__2; ++in) {
	    n = idim[in];
/*           Set LDB to 1 more than minimum value if room. */
	    ldb = m;
	    if (ldb < *nmax) {
		++ldb;
	    }
/*           Skip tests if not enough room. */
	    if (ldb > *nmax) {
		goto L130;
	    }
	    lbb = ldb * n;
	    null = m <= 0 || n <= 0;

	    for (ics = 1; ics <= 2; ++ics) {
		*(unsigned char *)side = *(unsigned char *)&ichs[ics - 1];
		left = *(unsigned char *)side == 'L';
		if (left) {
		    na = m;
		} else {
		    na = n;
		}
/*              Set LDA to 1 more than minimum value if room. */
		lda = na;
		if (lda < *nmax) {
		    ++lda;
		}
/*              Skip tests if not enough room. */
		if (lda > *nmax) {
		    goto L130;
		}
		laa = lda * na;

		for (icu = 1; icu <= 2; ++icu) {
		    *(unsigned char *)uplo = *(unsigned char *)&ichu[icu - 1];

		    for (ict = 1; ict <= 3; ++ict) {
			*(unsigned char *)transa = *(unsigned char *)&icht[
				ict - 1];

			for (icd = 1; icd <= 2; ++icd) {
			    *(unsigned char *)diag = *(unsigned char *)&ichd[
				    icd - 1];

			    i__3 = *nalf;
			    for (ia = 1; ia <= i__3; ++ia) {
				i__4 = ia;
				alpha.r = alf[i__4].r, alpha.i = alf[i__4].i;

/*                          Generate the matrix A. */

				cmake_("tr", uplo, diag, &na, &na, &a[
					a_offset], nmax, &aa[1], &lda, &reset,
					 &c_b1);

/*                          Generate the matrix B. */

				cmake_("ge", " ", " ", &m, &n, &b[b_offset], 
					nmax, &bb[1], &ldb, &reset, &c_b1);

				++nc;

/*                          Save every datum before calling the */
/*                          subroutine. */

				*(unsigned char *)sides = *(unsigned char *)
					side;
				*(unsigned char *)uplos = *(unsigned char *)
					uplo;
				*(unsigned char *)tranas = *(unsigned char *)
					transa;
				*(unsigned char *)diags = *(unsigned char *)
					diag;
				ms = m;
				ns = n;
				als.r = alpha.r, als.i = alpha.i;
				i__4 = laa;
				for (i__ = 1; i__ <= i__4; ++i__) {
				    i__5 = i__;
				    i__6 = i__;
				    as[i__5].r = aa[i__6].r, as[i__5].i = aa[
					    i__6].i;
/* L30: */
				}
				ldas = lda;
				i__4 = lbb;
				for (i__ = 1; i__ <= i__4; ++i__) {
				    i__5 = i__;
				    i__6 = i__;
				    bs[i__5].r = bb[i__6].r, bs[i__5].i = bb[
					    i__6].i;
/* L40: */
				}
				ldbs = ldb;

/*                          Call the subroutine. */

				if (s_cmp(sname + 9, "mm", (ftnlen)2, (ftnlen)
					2) == 0) {
				    if (*trace) {
					cprcn3_(ntra, &nc, sname, iorder, 
						side, uplo, transa, diag, &m, 
						&n, &alpha, &lda, &ldb/*, (
						ftnlen)12, (ftnlen)1, (ftnlen)
						1, (ftnlen)1, (ftnlen)1*/);
				    }
				    if (*rewi) {
/*					al__1.aerr = 0;
					al__1.aunit = *ntra;
					f_rew(&al__1);*/
				    }
				    cctrmm_(iorder, side, uplo, transa, diag, 
					    &m, &n, &alpha, &aa[1], &lda, &bb[
					    1], &ldb);
				} else if (s_cmp(sname + 9, "sm", (ftnlen)2, (
					ftnlen)2) == 0) {
				    if (*trace) {
					cprcn3_(ntra, &nc, sname, iorder, 
						side, uplo, transa, diag, &m, 
						&n, &alpha, &lda, &ldb/*, (
						ftnlen)12, (ftnlen)1, (ftnlen)
						1, (ftnlen)1, (ftnlen)1*/);
				    }
				    if (*rewi) {
/*					al__1.aerr = 0;
					al__1.aunit = *ntra;
					f_rew(&al__1);*/
				    }
				    cctrsm_(iorder, side, uplo, transa, diag, 
					    &m, &n, &alpha, &aa[1], &lda, &bb[
					    1], &ldb);
				}

/*                          Check if error-exit was taken incorrectly. */

				if (! infoc_1.ok) {
				    printf("*** FATAL ERROR - ERROR-CALL MYEXIT TAKEN ON VALID CALL\n");
				    *fatal = TRUE_;
				    goto L150;
				}

/*                          See what data changed inside subroutines. */

				isame[0] = *(unsigned char *)sides == *(
					unsigned char *)side;
				isame[1] = *(unsigned char *)uplos == *(
					unsigned char *)uplo;
				isame[2] = *(unsigned char *)tranas == *(
					unsigned char *)transa;
				isame[3] = *(unsigned char *)diags == *(
					unsigned char *)diag;
				isame[4] = ms == m;
				isame[5] = ns == n;
				isame[6] = als.r == alpha.r && als.i == 
					alpha.i;
				isame[7] = lce_(&as[1], &aa[1], &laa);
				isame[8] = ldas == lda;
				if (null) {
				    isame[9] = lce_(&bs[1], &bb[1], &lbb);
				} else {
				    isame[9] = lceres_("ge", " ", &m, &n, &bs[
					    1], &bb[1], &ldb);
				}
				isame[10] = ldbs == ldb;

/*                          If data was incorrectly changed, report and */
/*                          return. */

				same = TRUE_;
				i__4 = nargs;
				for (i__ = 1; i__ <= i__4; ++i__) {
				    same = same && isame[i__ - 1];
				    if (! isame[i__ - 1]) {
					printf(" ******* FATAL ERROR - PARAMETER NUMBER %d WAS CHANGED INCORRECTLY *******\n",i__);
				    }
/* L50: */
				}
				if (! same) {
				    *fatal = TRUE_;
				    goto L150;
				}

				if (! null) {
				    if (s_cmp(sname + 9, "mm", (ftnlen)2, (
					    ftnlen)2) == 0) {

/*                                Check the result. */

					if (left) {
					    cmmch_(transa, "N", &m, &n, &m, &
						    alpha, &a[a_offset], nmax,
						     &b[b_offset], nmax, &
						    c_b1, &c__[c_offset], 
						    nmax, &ct[1], &g[1], &bb[
						    1], &ldb, eps, &err, 
						    fatal, nout, &c_true/*, (
						    ftnlen)1, (ftnlen)1*/);
					} else {
					    cmmch_("N", transa, &m, &n, &n, &
						    alpha, &b[b_offset], nmax,
						     &a[a_offset], nmax, &
						    c_b1, &c__[c_offset], 
						    nmax, &ct[1], &g[1], &bb[
						    1], &ldb, eps, &err, 
						    fatal, nout, &c_true);
					}
				    } else if (s_cmp(sname + 9, "sm", (ftnlen)
					    2, (ftnlen)2) == 0) {

/*                                Compute approximation to original */
/*                                matrix. */

					i__4 = n;
					for (j = 1; j <= i__4; ++j) {
					    i__5 = m;
					    for (i__ = 1; i__ <= i__5; ++i__) 
						    {
			  i__6 = i__ + j * c_dim1;
			  i__7 = i__ + (j - 1) * ldb;
			  c__[i__6].r = bb[i__7].r, c__[i__6].i = bb[i__7].i;
			  i__6 = i__ + (j - 1) * ldb;
			  i__7 = i__ + j * b_dim1;
			  q__1.r = alpha.r * b[i__7].r - alpha.i * b[i__7].i, 
				  q__1.i = alpha.r * b[i__7].i + alpha.i * b[
				  i__7].r;
			  bb[i__6].r = q__1.r, bb[i__6].i = q__1.i;
/* L60: */
					    }
/* L70: */
					}

					if (left) {
					    cmmch_(transa, "N", &m, &n, &m, &
						    c_b2, &a[a_offset], nmax, 
						    &c__[c_offset], nmax, &
						    c_b1, &b[b_offset], nmax, 
						    &ct[1], &g[1], &bb[1], &
						    ldb, eps, &err, fatal, 
						    nout, &c_false);
					} else {
					    cmmch_("N", transa, &m, &n, &n, &
						    c_b2, &c__[c_offset], 
						    nmax, &a[a_offset], nmax, 
						    &c_b1, &b[b_offset], nmax,
						     &ct[1], &g[1], &bb[1], &
						    ldb, eps, &err, fatal, 
						    nout, &c_false);
					}
				    }
				    errmax = f2cmax(errmax,err);
/*                             If got really bad answer, report and */
/*                             return. */
				    if (*fatal) {
					goto L150;
				    }
				}

/* L80: */
			    }

/* L90: */
			}

/* L100: */
		    }

/* L110: */
		}

/* L120: */
	    }

L130:
	    ;
	}

/* L140: */
    }

/*     Report result. */

    if (errmax < *thresh) {
	if (*iorder == 0) {
            printf("%s PASSED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%d CALLS)\n",sname,nc);
	}
	if (*iorder == 1) {
            printf("%s PASSED THE ROW-MAJOR COMPUTATIONAL TESTS (%d CALLS)\n",sname,nc);
	}
    } else {
	if (*iorder == 0) {
            printf("%s COMPLETED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%d CALLS)/n",sname,nc);
	    printf("***** BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******/n",errmax);
	}
	if (*iorder == 1) {
            printf("%s COMPLETED THE ROW-MAJOR COMPUTATIONAL TESTS (%d CALLS)/n",sname,nc);
	    printf("***** BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******/n",errmax);
	}
    }
    goto L160;

L150:
    printf(" ******* %s FAILED ON CALL NUMBER:\n",sname);
    if (*trace) {
	cprcn3_(ntra, &nc, sname, iorder, side, uplo, transa, diag, &m, &n, &
		alpha, &lda, &ldb);
    }

L160:
    return 0;

/* 9995 FORMAT(1X, I6, ': ', A12,'(', 4( '''', A1, ''',' ), 2( I3, ',' ), */
/*     $     '(', F4.1, ',', F4.1, '), A,', I3, ', B,', I3, ')         ', */
/*     $      '      .' ) */

/*     End of CCHK3. */

} /* cchk3_ */


/* Subroutine */ int cprcn3_(integer *nout, integer *nc, char *sname, integer 
	*iorder, char *side, char *uplo, char *transa, char *diag, integer *m,
	 integer *n, complex *alpha, integer *lda, integer *ldb)
{
    /* Local variables */
    char ca[14], cd[14], cs[14], cu[14], crc[14];

    if (*(unsigned char *)side == 'L') {
	s_copy(cs, "     CblasLeft", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(cs, "    CblasRight", (ftnlen)14, (ftnlen)14);
    }
    if (*(unsigned char *)uplo == 'U') {
	s_copy(cu, "    CblasUpper", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(cu, "    CblasLower", (ftnlen)14, (ftnlen)14);
    }
    if (*(unsigned char *)transa == 'N') {
	s_copy(ca, "  CblasNoTrans", (ftnlen)14, (ftnlen)14);
    } else if (*(unsigned char *)transa == 'T') {
	s_copy(ca, "    CblasTrans", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(ca, "CblasConjTrans", (ftnlen)14, (ftnlen)14);
    }
    if (*(unsigned char *)diag == 'N') {
	s_copy(cd, "  CblasNonUnit", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(cd, "     CblasUnit", (ftnlen)14, (ftnlen)14);
    }
    if (*iorder == 1) {
	s_copy(crc, " CblasRowMajor", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(crc, " CblasColMajor", (ftnlen)14, (ftnlen)14);
    }
    printf("%6d: %s %s %s %s\n",*nc,sname,crc,cs,cu);
    printf("         %s %s %d %d (%4.1f,%4.1f) A %d B %d\n",ca,cd,*m,*n,alpha->r,alpha->i,*lda,*ldb);

    return 0;
} /* cprcn3_ */


/* Subroutine */ int cchk4_(char *sname, real *eps, real *thresh, integer *
	nout, integer *ntra, logical *trace, logical *rewi, logical *fatal, 
	integer *nidim, integer *idim, integer *nalf, complex *alf, integer *
	nbet, complex *bet, integer *nmax, complex *a, complex *aa, complex *
	as, complex *b, complex *bb, complex *bs, complex *c__, complex *cc, 
	complex *cs, complex *ct, real *g, integer *iorder)
{
    /* Initialized data */

    static char icht[2] = "NC";
    static char ichu[2] = "UL";

    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, c_dim1, c_offset, i__1, i__2, 
	    i__3, i__4, i__5, i__6, i__7;
    complex q__1;

    /* Local variables */
    complex beta;
    integer ldas, ldcs;
    logical same, conj;
    complex bets;
    real rals;
    logical tran, null;
    char uplo[1];
    integer i__, j, k, n;
    extern /* Subroutine */ int cmake_(char *, char *, char *, integer *, 
	    integer *, complex *, integer *, complex *, integer *, logical *, 
	    complex *);
    complex alpha;
    extern /* Subroutine */ int cmmch_(char *, char *, integer *, integer *, 
	    integer *, complex *, complex *, integer *, complex *, integer *, 
	    complex *, complex *, integer *, complex *, real *, complex *, 
	    integer *, real *, real *, logical *, integer *, logical *);
    real rbeta;
    logical isame[13];
    integer nargs;
    real rbets;
    logical reset;
    char trans[1];
    logical upper;
    char uplos[1];
    extern /* Subroutine */ int cprcn4_(integer *, integer *, char *, integer 
	    *, char *, char *, integer *, integer *, complex *, integer *, 
	    complex *, integer *), cprcn6_(integer *, 
	    integer *, char *, integer *, char *, char *, integer *, integer *
	    , real *, integer *, real *, integer *);
    integer ia, ib, jc, ma, na, nc, ik, in, jj, lj, ks;
    extern /* Subroutine */ int ccherk_(integer *, char *, char *, integer *, 
	    integer *, real *, complex *, integer *, real *, complex *, 
	    integer *);
    integer ns;
    real ralpha;
    extern logical lceres_(char *, char *, integer *, integer *, complex *, 
	    complex *, integer *);
    real errmax;
    extern /* Subroutine */ int ccsyrk_(integer *, char *, char *, integer *, 
	    integer *, complex *, complex *, integer *, complex *, complex *, 
	    integer *);
    char transs[1], transt[1];
    integer laa, lda, lcc, ldc;
    extern logical lce_(complex *, complex *, integer *);
    complex als;
    integer ict, icu;
    real err;

/*  Tests CHERK and CSYRK. */

/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

    /* Parameter adjustments */
    --idim;
    --alf;
    --bet;
    --g;
    --ct;
    --cs;
    --cc;
    c_dim1 = *nmax;
    c_offset = 1 + c_dim1 * 1;
    c__ -= c_offset;
    --bs;
    --bb;
    b_dim1 = *nmax;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    --as;
    --aa;
    a_dim1 = *nmax;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;

    /* Function Body */
    conj = s_cmp(sname + 7, "he", (ftnlen)2, (ftnlen)2) == 0;

    nargs = 10;
    nc = 0;
    reset = TRUE_;
    errmax = 0.f;
    rals = 1.f;
    rbets = 1.f;

    i__1 = *nidim;
    for (in = 1; in <= i__1; ++in) {
	n = idim[in];
/*        Set LDC to 1 more than minimum value if room. */
	ldc = n;
	if (ldc < *nmax) {
	    ++ldc;
	}
/*        Skip tests if not enough room. */
	if (ldc > *nmax) {
	    goto L100;
	}
	lcc = ldc * n;

	i__2 = *nidim;
	for (ik = 1; ik <= i__2; ++ik) {
	    k = idim[ik];

	    for (ict = 1; ict <= 2; ++ict) {
		*(unsigned char *)trans = *(unsigned char *)&icht[ict - 1];
		tran = *(unsigned char *)trans == 'C';
		if (tran && ! conj) {
		    *(unsigned char *)trans = 'T';
		}
		if (tran) {
		    ma = k;
		    na = n;
		} else {
		    ma = n;
		    na = k;
		}
/*              Set LDA to 1 more than minimum value if room. */
		lda = ma;
		if (lda < *nmax) {
		    ++lda;
		}
/*              Skip tests if not enough room. */
		if (lda > *nmax) {
		    goto L80;
		}
		laa = lda * na;

/*              Generate the matrix A. */

		cmake_("ge", " ", " ", &ma, &na, &a[a_offset], nmax, &aa[1], &
			lda, &reset, &c_b1);

		for (icu = 1; icu <= 2; ++icu) {
		    *(unsigned char *)uplo = *(unsigned char *)&ichu[icu - 1];
		    upper = *(unsigned char *)uplo == 'U';

		    i__3 = *nalf;
		    for (ia = 1; ia <= i__3; ++ia) {
			i__4 = ia;
			alpha.r = alf[i__4].r, alpha.i = alf[i__4].i;
			if (conj) {
			    ralpha = alpha.r;
			    q__1.r = ralpha, q__1.i = 0.f;
			    alpha.r = q__1.r, alpha.i = q__1.i;
			}

			i__4 = *nbet;
			for (ib = 1; ib <= i__4; ++ib) {
			    i__5 = ib;
			    beta.r = bet[i__5].r, beta.i = bet[i__5].i;
			    if (conj) {
				rbeta = beta.r;
				q__1.r = rbeta, q__1.i = 0.f;
				beta.r = q__1.r, beta.i = q__1.i;
			    }
			    null = n <= 0;
			    if (conj) {
				null = null || ((k <= 0 || ralpha == 0.f) && 
					rbeta == 1.f);
			    }

/*                       Generate the matrix C. */

			    cmake_(sname + 7, uplo, " ", &n, &n, &c__[
				    c_offset], nmax, &cc[1], &ldc, &reset, &
				    c_b1);

			    ++nc;

/*                       Save every datum before calling the subroutine. */

			    *(unsigned char *)uplos = *(unsigned char *)uplo;
			    *(unsigned char *)transs = *(unsigned char *)
				    trans;
			    ns = n;
			    ks = k;
			    if (conj) {
				rals = ralpha;
			    } else {
				als.r = alpha.r, als.i = alpha.i;
			    }
			    i__5 = laa;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				i__6 = i__;
				i__7 = i__;
				as[i__6].r = aa[i__7].r, as[i__6].i = aa[i__7]
					.i;
/* L10: */
			    }
			    ldas = lda;
			    if (conj) {
				rbets = rbeta;
			    } else {
				bets.r = beta.r, bets.i = beta.i;
			    }
			    i__5 = lcc;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				i__6 = i__;
				i__7 = i__;
				cs[i__6].r = cc[i__7].r, cs[i__6].i = cc[i__7]
					.i;
/* L20: */
			    }
			    ldcs = ldc;

/*                       Call the subroutine. */

			    if (conj) {
				if (*trace) {
				    cprcn6_(ntra, &nc, sname, iorder, uplo, 
					    trans, &n, &k, &ralpha, &lda, &
					    rbeta, &ldc);
				}
				if (*rewi) {
/*				    al__1.aerr = 0;
				    al__1.aunit = *ntra;
				    f_rew(&al__1);*/
				}
				ccherk_(iorder, uplo, trans, &n, &k, &ralpha, 
					&aa[1], &lda, &rbeta, &cc[1], &ldc);
			    } else {
				if (*trace) {
				    cprcn4_(ntra, &nc, sname, iorder, uplo, 
					    trans, &n, &k, &alpha, &lda, &
					    beta, &ldc);
				}
				if (*rewi) {
/*				    al__1.aerr = 0;
				    al__1.aunit = *ntra;
				    f_rew(&al__1);*/
				}
				ccsyrk_(iorder, uplo, trans, &n, &k, &alpha, &
					aa[1], &lda, &beta, &cc[1], &ldc);
			    }

/*                       Check if error-exit was taken incorrectly. */

			    if (! infoc_1.ok) {
				printf("*** FATAL ERROR - ERROR-CALL MYEXIT TAKEN ON VALID CALL\n");
				*fatal = TRUE_;
				goto L120;
			    }

/*                       See what data changed inside subroutines. */

			    isame[0] = *(unsigned char *)uplos == *(unsigned 
				    char *)uplo;
			    isame[1] = *(unsigned char *)transs == *(unsigned 
				    char *)trans;
			    isame[2] = ns == n;
			    isame[3] = ks == k;
			    if (conj) {
				isame[4] = rals == ralpha;
			    } else {
				isame[4] = als.r == alpha.r && als.i == 
					alpha.i;
			    }
			    isame[5] = lce_(&as[1], &aa[1], &laa);
			    isame[6] = ldas == lda;
			    if (conj) {
				isame[7] = rbets == rbeta;
			    } else {
				isame[7] = bets.r == beta.r && bets.i == 
					beta.i;
			    }
			    if (null) {
				isame[8] = lce_(&cs[1], &cc[1], &lcc);
			    } else {
				isame[8] = lceres_(sname + 7, uplo, &n, &n, &
					cs[1], &cc[1], &ldc);
			    }
			    isame[9] = ldcs == ldc;

/*                       If data was incorrectly changed, report and */
/*                       return. */

			    same = TRUE_;
			    i__5 = nargs;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				same = same && isame[i__ - 1];
				if (! isame[i__ - 1]) {
				    printf(" ******* FATAL ERROR - PARAMETER NUMBER %d WAS CHANGED INCORRECTLY *******\n",i__);
				}
/* L30: */
			    }
			    if (! same) {
				*fatal = TRUE_;
				goto L120;
			    }

			    if (! null) {

/*                          Check the result column by column. */

				if (conj) {
				    *(unsigned char *)transt = 'C';
				} else {
				    *(unsigned char *)transt = 'T';
				}
				jc = 1;
				i__5 = n;
				for (j = 1; j <= i__5; ++j) {
				    if (upper) {
					jj = 1;
					lj = j;
				    } else {
					jj = j;
					lj = n - j + 1;
				    }
				    if (tran) {
					cmmch_(transt, "N", &lj, &c__1, &k, &
						alpha, &a[jj * a_dim1 + 1], 
						nmax, &a[j * a_dim1 + 1], 
						nmax, &beta, &c__[jj + j * 
						c_dim1], nmax, &ct[1], &g[1], 
						&cc[jc], &ldc, eps, &err, 
						fatal, nout, &c_true);
				    } else {
					cmmch_("N", transt, &lj, &c__1, &k, &
						alpha, &a[jj + a_dim1], nmax, 
						&a[j + a_dim1], nmax, &beta, &
						c__[jj + j * c_dim1], nmax, &
						ct[1], &g[1], &cc[jc], &ldc, 
						eps, &err, fatal, nout, &
						c_true);
				    }
				    if (upper) {
					jc += ldc;
				    } else {
					jc = jc + ldc + 1;
				    }
				    errmax = f2cmax(errmax,err);
/*                             If got really bad answer, report and */
/*                             return. */
				    if (*fatal) {
					goto L110;
				    }
/* L40: */
				}
			    }

/* L50: */
			}

/* L60: */
		    }

/* L70: */
		}

L80:
		;
	    }

/* L90: */
	}

L100:
	;
    }

/*     Report result. */

    if (errmax < *thresh) {
	if (*iorder == 0) {
            printf("%s PASSED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%d CALLS)\n",sname,nc);
	}
	if (*iorder == 1) {
            printf("%s PASSED THE ROW-MAJOR COMPUTATIONAL TESTS (%d CALLS)\n",sname,nc);
	}
    } else {
	if (*iorder == 0) {
            printf("%s COMPLETED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%d CALLS)/n",sname,nc);
	    printf("***** BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******/n",errmax);
	}
	if (*iorder == 1) {
            printf("%s COMPLETED THE ROW-MAJOR COMPUTATIONAL TESTS (%d CALLS)/n",sname,nc);
	    printf("***** BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******/n",errmax);
	}
    }
    goto L130;

L110:
    if (n > 1) {
        printf("      THESE ARE THE RESULTS FOR COLUMN %d:\n",j);
    }

L120:
    printf(" ******* %s FAILED ON CALL NUMBER:\n",sname);
    if (conj) {
	cprcn6_(nout, &nc, sname, iorder, uplo, trans, &n, &k, &ralpha, &lda, 
		&rbeta, &ldc);
    } else {
	cprcn4_(nout, &nc, sname, iorder, uplo, trans, &n, &k, &alpha, &lda, &
		beta, &ldc);
    }

L130:
    return 0;

/* 9994 FORMAT(1X, I6, ': ', A12,'(', 2( '''', A1, ''',' ), 2( I3, ',' ), */
/*     $     F4.1, ', A,', I3, ',', F4.1, ', C,', I3, ')               ', */
/*     $      '          .' ) */
/* 9993 FORMAT(1X, I6, ': ', A12,'(', 2( '''', A1, ''',' ), 2( I3, ',' ), */
/*     $      '(', F4.1, ',', F4.1, ') , A,', I3, ',(', F4.1, ',', F4.1, */
/*     $      '), C,', I3, ')          .' ) */

/*     End of CCHK4. */

} /* cchk4_ */


/* Subroutine */ int cprcn4_(integer *nout, integer *nc, char *sname, integer 
	*iorder, char *uplo, char *transa, integer *n, integer *k, complex *
	alpha, integer *lda, complex *beta, integer *ldc)
{
    /* Local variables */
    char ca[14], cu[14], crc[14];

    if (*(unsigned char *)uplo == 'U') {
	s_copy(cu, "    CblasUpper", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(cu, "    CblasLower", (ftnlen)14, (ftnlen)14);
    }
    if (*(unsigned char *)transa == 'N') {
	s_copy(ca, "  CblasNoTrans", (ftnlen)14, (ftnlen)14);
    } else if (*(unsigned char *)transa == 'T') {
	s_copy(ca, "    CblasTrans", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(ca, "CblasConjTrans", (ftnlen)14, (ftnlen)14);
    }
    if (*iorder == 1) {
	s_copy(crc, " CblasRowMajor", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(crc, " CblasColMajor", (ftnlen)14, (ftnlen)14);
    }
    printf("%6d: %s %s %s %s\n",*nc,sname,crc,cu,ca);
    printf("(          %d %d (%4.1f,%4.1f) A %d (%4.1f,%4.1f) C %d\n",*n,*k,alpha->r,alpha->i,*lda,beta->r,beta->i,*ldc);
    return 0;
} /* cprcn4_ */



/* Subroutine */ int cprcn6_(integer *nout, integer *nc, char *sname, integer 
	*iorder, char *uplo, char *transa, integer *n, integer *k, real *
	alpha, integer *lda, real *beta, integer *ldc)
{
    /* Local variables */
    char ca[14], cu[14], crc[14];

    if (*(unsigned char *)uplo == 'U') {
	s_copy(cu, "    CblasUpper", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(cu, "    CblasLower", (ftnlen)14, (ftnlen)14);
    }
    if (*(unsigned char *)transa == 'N') {
	s_copy(ca, "  CblasNoTrans", (ftnlen)14, (ftnlen)14);
    } else if (*(unsigned char *)transa == 'T') {
	s_copy(ca, "    CblasTrans", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(ca, "CblasConjTrans", (ftnlen)14, (ftnlen)14);
    }
    if (*iorder == 1) {
	s_copy(crc, " CblasRowMajor", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(crc, " CblasColMajor", (ftnlen)14, (ftnlen)14);
    }
    printf("%6d: %s %s %s %s\n",*nc,sname,crc,cu,ca);
    printf("(          %d %d %4.1f A %d %4.1f C %d\n",*n,*k,*alpha,*lda,*beta,*ldc);
    return 0;
} /* cprcn6_ */


/* Subroutine */ int cchk5_(char *sname, real *eps, real *thresh, integer *
	nout, integer *ntra, logical *trace, logical *rewi, logical *fatal, 
	integer *nidim, integer *idim, integer *nalf, complex *alf, integer *
	nbet, complex *bet, integer *nmax, complex *ab, complex *aa, complex *
	as, complex *bb, complex *bs, complex *c__, complex *cc, complex *cs, 
	complex *ct, real *g, complex *w, integer *iorder)
{
    /* Initialized data */

    static char icht[2] = "NC";
    static char ichu[2] = "UL";


    /* System generated locals */
    integer c_dim1, c_offset, i__1, i__2, i__3, i__4, i__5, i__6, i__7, i__8;
    complex q__1, q__2;

    /* Local variables */
    integer jjab;
    complex beta;
    integer ldas, ldbs, ldcs;
    logical same, conj;
    complex bets;
    logical tran, null;
    char uplo[1];
    integer i__, j, k, n;
    extern /* Subroutine */ int cmake_(char *, char *, char *, integer *, 
	    integer *, complex *, integer *, complex *, integer *, logical *, 
	    complex *);
    complex alpha;
    extern /* Subroutine */ int cmmch_(char *, char *, integer *, integer *, 
	    integer *, complex *, complex *, integer *, complex *, integer *, 
	    complex *, complex *, integer *, complex *, real *, complex *, 
	    integer *, real *, real *, logical *, integer *, logical *);
    real rbeta;
    logical isame[13];
    integer nargs;
    real rbets;
    logical reset;
    char trans[1];
    logical upper;
    char uplos[1];
    extern /* Subroutine */ int cprcn5_(integer *, integer *, char *, integer 
	    *, char *, char *, integer *, integer *, complex *, integer *, 
	    integer *, complex *, integer *), cprcn7_(
	    integer *, integer *, char *, integer *, char *, char *, integer *
	    , integer *, complex *, integer *, integer *, real *, integer *);
    integer ia, ib, jc, ma, na, nc, ik, in, jj, lj, ks, ns;
    extern logical lceres_(char *, char *, integer *, integer *, complex *, 
	    complex *, integer *);
    real errmax;
    char transs[1], transt[1];
    extern /* Subroutine */ int ccher2k_(integer *, char *, char *, integer *,
	     integer *, complex *, complex *, integer *, complex *, integer *,
	     real *, complex *, integer *);
    integer laa, lbb, lda, lcc, ldb, ldc;
    extern logical lce_(complex *, complex *, integer *);
    extern /* Subroutine */ int ccsyr2k_(integer *, char *, char *, integer *,
	     integer *, complex *, complex *, integer *, complex *, integer *,
	     complex *, complex *, integer *);
    complex als;
    integer ict, icu;
    real err;

/*  Tests CHER2K and CSYR2K. */

/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

    /* Parameter adjustments */
    --idim;
    --alf;
    --bet;
    --w;
    --g;
    --ct;
    --cs;
    --cc;
    c_dim1 = *nmax;
    c_offset = 1 + c_dim1 * 1;
    c__ -= c_offset;
    --bs;
    --bb;
    --as;
    --aa;
    --ab;

    /* Function Body */
    conj = s_cmp(sname + 7, "he", (ftnlen)2, (ftnlen)2) == 0;

    nargs = 12;
    nc = 0;
    reset = TRUE_;
    errmax = 0.f;

    i__1 = *nidim;
    for (in = 1; in <= i__1; ++in) {
	n = idim[in];
/*        Set LDC to 1 more than minimum value if room. */
	ldc = n;
	if (ldc < *nmax) {
	    ++ldc;
	}
/*        Skip tests if not enough room. */
	if (ldc > *nmax) {
	    goto L130;
	}
	lcc = ldc * n;

	i__2 = *nidim;
	for (ik = 1; ik <= i__2; ++ik) {
	    k = idim[ik];

	    for (ict = 1; ict <= 2; ++ict) {
		*(unsigned char *)trans = *(unsigned char *)&icht[ict - 1];
		tran = *(unsigned char *)trans == 'C';
		if (tran && ! conj) {
		    *(unsigned char *)trans = 'T';
		}
		if (tran) {
		    ma = k;
		    na = n;
		} else {
		    ma = n;
		    na = k;
		}
/*              Set LDA to 1 more than minimum value if room. */
		lda = ma;
		if (lda < *nmax) {
		    ++lda;
		}
/*              Skip tests if not enough room. */
		if (lda > *nmax) {
		    goto L110;
		}
		laa = lda * na;

/*              Generate the matrix A. */

		if (tran) {
		    i__3 = *nmax << 1;
		    cmake_("ge", " ", " ", &ma, &na, &ab[1], &i__3, &aa[1], &
			    lda, &reset, &c_b1);
		} else {
		    cmake_("ge", " ", " ", &ma, &na, &ab[1], nmax, &aa[1], &
			    lda, &reset, &c_b1);
		}

/*              Generate the matrix B. */

		ldb = lda;
		lbb = laa;
		if (tran) {
		    i__3 = *nmax << 1;
		    cmake_("ge", " ", " ", &ma, &na, &ab[k + 1], &i__3, &bb[1]
			    , &ldb, &reset, &c_b1);
		} else {
		    cmake_("ge", " ", " ", &ma, &na, &ab[k * *nmax + 1], nmax,
			     &bb[1], &ldb, &reset, &c_b1);
		}

		for (icu = 1; icu <= 2; ++icu) {
		    *(unsigned char *)uplo = *(unsigned char *)&ichu[icu - 1];
		    upper = *(unsigned char *)uplo == 'U';

		    i__3 = *nalf;
		    for (ia = 1; ia <= i__3; ++ia) {
			i__4 = ia;
			alpha.r = alf[i__4].r, alpha.i = alf[i__4].i;

			i__4 = *nbet;
			for (ib = 1; ib <= i__4; ++ib) {
			    i__5 = ib;
			    beta.r = bet[i__5].r, beta.i = bet[i__5].i;
			    if (conj) {
				rbeta = beta.r;
				q__1.r = rbeta, q__1.i = 0.f;
				beta.r = q__1.r, beta.i = q__1.i;
			    }
			    null = n <= 0;
			    if (conj) {
				null = null || ((k <= 0 || (alpha.r == 0.f && 
					alpha.i == 0.f)) && rbeta == 1.f);
			    }

/*                       Generate the matrix C. */

			    cmake_(sname + 7, uplo, " ", &n, &n, &c__[
				    c_offset], nmax, &cc[1], &ldc, &reset, &
				    c_b1);

			    ++nc;

/*                       Save every datum before calling the subroutine. */

			    *(unsigned char *)uplos = *(unsigned char *)uplo;
			    *(unsigned char *)transs = *(unsigned char *)
				    trans;
			    ns = n;
			    ks = k;
			    als.r = alpha.r, als.i = alpha.i;
			    i__5 = laa;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				i__6 = i__;
				i__7 = i__;
				as[i__6].r = aa[i__7].r, as[i__6].i = aa[i__7]
					.i;
/* L10: */
			    }
			    ldas = lda;
			    i__5 = lbb;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				i__6 = i__;
				i__7 = i__;
				bs[i__6].r = bb[i__7].r, bs[i__6].i = bb[i__7]
					.i;
/* L20: */
			    }
			    ldbs = ldb;
			    if (conj) {
				rbets = rbeta;
			    } else {
				bets.r = beta.r, bets.i = beta.i;
			    }
			    i__5 = lcc;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				i__6 = i__;
				i__7 = i__;
				cs[i__6].r = cc[i__7].r, cs[i__6].i = cc[i__7]
					.i;
/* L30: */
			    }
			    ldcs = ldc;

/*                       Call the subroutine. */

			    if (conj) {
				if (*trace) {
				    cprcn7_(ntra, &nc, sname, iorder, uplo, 
					    trans, &n, &k, &alpha, &lda, &ldb,
					     &rbeta, &ldc);
				}
				if (*rewi) {
/*				    al__1.aerr = 0;
				    al__1.aunit = *ntra;
				    f_rew(&al__1);*/
				}
				ccher2k_(iorder, uplo, trans, &n, &k, &alpha, 
					&aa[1], &lda, &bb[1], &ldb, &rbeta, &
					cc[1], &ldc);
			    } else {
				if (*trace) {
				    cprcn5_(ntra, &nc, sname, iorder, uplo, 
					    trans, &n, &k, &alpha, &lda, &ldb,
					     &beta, &ldc);
				}
				if (*rewi) {
/*				    al__1.aerr = 0;
				    al__1.aunit = *ntra;
				    f_rew(&al__1);*/
				}
				ccsyr2k_(iorder, uplo, trans, &n, &k, &alpha, 
					&aa[1], &lda, &bb[1], &ldb, &beta, &
					cc[1], &ldc);
			    }

/*                       Check if error-exit was taken incorrectly. */

			    if (! infoc_1.ok) {
				printf("*** FATAL ERROR - ERROR-CALL MYEXIT TAKEN ON VALID CALL\n");
				*fatal = TRUE_;
				goto L150;
			    }

/*                       See what data changed inside subroutines. */

			    isame[0] = *(unsigned char *)uplos == *(unsigned 
				    char *)uplo;
			    isame[1] = *(unsigned char *)transs == *(unsigned 
				    char *)trans;
			    isame[2] = ns == n;
			    isame[3] = ks == k;
			    isame[4] = als.r == alpha.r && als.i == alpha.i;
			    isame[5] = lce_(&as[1], &aa[1], &laa);
			    isame[6] = ldas == lda;
			    isame[7] = lce_(&bs[1], &bb[1], &lbb);
			    isame[8] = ldbs == ldb;
			    if (conj) {
				isame[9] = rbets == rbeta;
			    } else {
				isame[9] = bets.r == beta.r && bets.i == 
					beta.i;
			    }
			    if (null) {
				isame[10] = lce_(&cs[1], &cc[1], &lcc);
			    } else {
				isame[10] = lceres_("he", uplo, &n, &n, &cs[1]
					, &cc[1], &ldc);
			    }
			    isame[11] = ldcs == ldc;

/*                       If data was incorrectly changed, report and */
/*                       return. */

			    same = TRUE_;
			    i__5 = nargs;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				same = same && isame[i__ - 1];
				if (! isame[i__ - 1]) {
				    printf(" ******* FATAL ERROR - PARAMETER NUMBER %d WAS CHANGED INCORRECTLY *******\n",i__);
				}
/* L40: */
			    }
			    if (! same) {
				*fatal = TRUE_;
				goto L150;
			    }

			    if (! null) {

/*                          Check the result column by column. */

				if (conj) {
				    *(unsigned char *)transt = 'C';
				} else {
				    *(unsigned char *)transt = 'T';
				}
				jjab = 1;
				jc = 1;
				i__5 = n;
				for (j = 1; j <= i__5; ++j) {
				    if (upper) {
					jj = 1;
					lj = j;
				    } else {
					jj = j;
					lj = n - j + 1;
				    }
				    if (tran) {
					i__6 = k;
					for (i__ = 1; i__ <= i__6; ++i__) {
					    i__7 = i__;
					    i__8 = ((j - 1) << 1) * *nmax + k + 
						    i__;
					    q__1.r = alpha.r * ab[i__8].r - 
						    alpha.i * ab[i__8].i, 
						    q__1.i = alpha.r * ab[
						    i__8].i + alpha.i * ab[
						    i__8].r;
					    w[i__7].r = q__1.r, w[i__7].i = 
						    q__1.i;
					    if (conj) {
			  i__7 = k + i__;
			  r_cnjg(&q__2, &alpha);
			  i__8 = ((j - 1) << 1) * *nmax + i__;
			  q__1.r = q__2.r * ab[i__8].r - q__2.i * ab[i__8].i, 
				  q__1.i = q__2.r * ab[i__8].i + q__2.i * ab[
				  i__8].r;
			  w[i__7].r = q__1.r, w[i__7].i = q__1.i;
					    } else {
			  i__7 = k + i__;
			  i__8 = ((j - 1) << 1) * *nmax + i__;
			  q__1.r = alpha.r * ab[i__8].r - alpha.i * ab[i__8]
				  .i, q__1.i = alpha.r * ab[i__8].i + alpha.i 
				  * ab[i__8].r;
			  w[i__7].r = q__1.r, w[i__7].i = q__1.i;
					    }
/* L50: */
					}
					i__6 = k << 1;
					i__7 = *nmax << 1;
					i__8 = *nmax << 1;
					cmmch_(transt, "N", &lj, &c__1, &i__6,
						 &c_b2, &ab[jjab], &i__7, &w[
						1], &i__8, &beta, &c__[jj + j 
						* c_dim1], nmax, &ct[1], &g[1]
						, &cc[jc], &ldc, eps, &err, 
						fatal, nout, &c_true);
				    } else {
					i__6 = k;
					for (i__ = 1; i__ <= i__6; ++i__) {
					    if (conj) {
			  i__7 = i__;
			  r_cnjg(&q__2, &ab[(k + i__ - 1) * *nmax + j]);
			  q__1.r = alpha.r * q__2.r - alpha.i * q__2.i, 
				  q__1.i = alpha.r * q__2.i + alpha.i * 
				  q__2.r;
			  w[i__7].r = q__1.r, w[i__7].i = q__1.i;
			  i__7 = k + i__;
			  i__8 = (i__ - 1) * *nmax + j;
			  q__2.r = alpha.r * ab[i__8].r - alpha.i * ab[i__8]
				  .i, q__2.i = alpha.r * ab[i__8].i + alpha.i 
				  * ab[i__8].r;
			  r_cnjg(&q__1, &q__2);
			  w[i__7].r = q__1.r, w[i__7].i = q__1.i;
					    } else {
			  i__7 = i__;
			  i__8 = (k + i__ - 1) * *nmax + j;
			  q__1.r = alpha.r * ab[i__8].r - alpha.i * ab[i__8]
				  .i, q__1.i = alpha.r * ab[i__8].i + alpha.i 
				  * ab[i__8].r;
			  w[i__7].r = q__1.r, w[i__7].i = q__1.i;
			  i__7 = k + i__;
			  i__8 = (i__ - 1) * *nmax + j;
			  q__1.r = alpha.r * ab[i__8].r - alpha.i * ab[i__8]
				  .i, q__1.i = alpha.r * ab[i__8].i + alpha.i 
				  * ab[i__8].r;
			  w[i__7].r = q__1.r, w[i__7].i = q__1.i;
					    }
/* L60: */
					}
					i__6 = k << 1;
					i__7 = *nmax << 1;
					cmmch_("N", "N", &lj, &c__1, &i__6, &
						c_b2, &ab[jj], nmax, &w[1], &
						i__7, &beta, &c__[jj + j * 
						c_dim1], nmax, &ct[1], &g[1], 
						&cc[jc], &ldc, eps, &err, 
						fatal, nout, &c_true);
				    }
				    if (upper) {
					jc += ldc;
				    } else {
					jc = jc + ldc + 1;
					if (tran) {
					    jjab += *nmax << 1;
					}
				    }
				    errmax = f2cmax(errmax,err);
/*                             If got really bad answer, report and */
/*                             return. */
				    if (*fatal) {
					goto L140;
				    }
/* L70: */
				}
			    }

/* L80: */
			}

/* L90: */
		    }

/* L100: */
		}

L110:
		;
	    }

/* L120: */
	}

L130:
	;
    }

/*     Report result. */

    if (errmax < *thresh) {
	if (*iorder == 0) {
            printf("%s PASSED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%d CALLS)\n",sname,nc);
	}
	if (*iorder == 1) {
            printf("%s PASSED THE ROW-MAJOR COMPUTATIONAL TESTS (%d CALLS)\n",sname,nc);
	}
    } else {
	if (*iorder == 0) {
            printf("%s COMPLETED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%d CALLS)/n",sname,nc);
	    printf("***** BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******/n",errmax);
	}
	if (*iorder == 1) {
            printf("%s COMPLETED THE ROW-MAJOR COMPUTATIONAL TESTS (%d CALLS)/n",sname,nc);
	    printf("***** BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******/n",errmax);
	}
    }
    goto L160;

L140:
    if (n > 1) {
        printf("      THESE ARE THE RESULTS FOR COLUMN %d:\n",j);
    }

L150:
    printf(" ******* %s FAILED ON CALL NUMBER:\n",sname);
    if (conj) {
	cprcn7_(nout, &nc, sname, iorder, uplo, trans, &n, &k, &alpha, &lda, &
		ldb, &rbeta, &ldc);
    } else {
	cprcn5_(nout, &nc, sname, iorder, uplo, trans, &n, &k, &alpha, &lda, &
		ldb, &beta, &ldc);
    }

L160:
    return 0;

/* 9994 FORMAT(1X, I6, ': ', A12,'(', 2( '''', A1, ''',' ), 2( I3, ',' ), */
/*     $      '(', F4.1, ',', F4.1, '), A,', I3, ', B,', I3, ',', F4.1, */
/*     $      ', C,', I3, ')           .' ) */
/* 9993 FORMAT(1X, I6, ': ', A12,'(', 2( '''', A1, ''',' ), 2( I3, ',' ), */
/*     $      '(', F4.1, ',', F4.1, '), A,', I3, ', B,', I3, ',(', F4.1, */
/*     $      ',', F4.1, '), C,', I3, ')    .' ) */

/*     End of CCHK5. */

} /* cchk5_ */


/* Subroutine */ int cprcn5_(integer *nout, integer *nc, char *sname, integer 
	*iorder, char *uplo, char *transa, integer *n, integer *k, complex *
	alpha, integer *lda, integer *ldb, complex *beta, integer *ldc)
{

    /* Local variables */
    char ca[14], cu[14], crc[14];

    if (*(unsigned char *)uplo == 'U') {
	s_copy(cu, "    CblasUpper", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(cu, "    CblasLower", (ftnlen)14, (ftnlen)14);
    }
    if (*(unsigned char *)transa == 'N') {
	s_copy(ca, "  CblasNoTrans", (ftnlen)14, (ftnlen)14);
    } else if (*(unsigned char *)transa == 'T') {
	s_copy(ca, "    CblasTrans", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(ca, "CblasConjTrans", (ftnlen)14, (ftnlen)14);
    }
    if (*iorder == 1) {
	s_copy(crc, " CblasRowMajor", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(crc, " CblasColMajor", (ftnlen)14, (ftnlen)14);
    }
    printf("%6d: %s %s %s %s\n",*nc,sname,crc,cu,ca);
    printf("%d %d (%4.1f,%4.1f) , A, %d, B, %d, (%4.1f,%4.1f) , C, %d.\n",*n,*k,alpha->r,alpha->i,*lda,*ldb,beta->r,beta->i,*ldc);
    return 0;
} /* cprcn5_ */



/* Subroutine */ int cprcn7_(integer *nout, integer *nc, char *sname, integer 
	*iorder, char *uplo, char *transa, integer *n, integer *k, complex *
	alpha, integer *lda, integer *ldb, real *beta, integer *ldc)
{

    /* Local variables */
    char ca[14], cu[14], crc[14];

    if (*(unsigned char *)uplo == 'U') {
	s_copy(cu, "    CblasUpper", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(cu, "    CblasLower", (ftnlen)14, (ftnlen)14);
    }
    if (*(unsigned char *)transa == 'N') {
	s_copy(ca, "  CblasNoTrans", (ftnlen)14, (ftnlen)14);
    } else if (*(unsigned char *)transa == 'T') {
	s_copy(ca, "    CblasTrans", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(ca, "CblasConjTrans", (ftnlen)14, (ftnlen)14);
    }
    if (*iorder == 1) {
	s_copy(crc, " CblasRowMajor", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(crc, " CblasColMajor", (ftnlen)14, (ftnlen)14);
    }
    printf("%6d: %s %s %s %s\n",*nc,sname,crc,cu,ca);
    printf("%d %d (%4.1f,%4.1f), A, %d, B, %d, %4.1f, C, %d.\n",*n,*k,alpha->r,alpha->i,*lda,*ldb,*beta,*ldc);
    return 0;
} /* cprcn7_ */


/* Subroutine */ int cmake_(char *type__, char *uplo, char *diag, integer *m, 
	integer *n, complex *a, integer *nmax, complex *aa, integer *lda, 
	logical *reset, complex *transl)
{
    /* System generated locals */
    integer a_dim1, a_offset, i__1, i__2, i__3, i__4;
    real r__1;
    complex q__1, q__2;

    /* Local variables */
    extern /* Complex */ VOID cbeg_(complex *, logical *);
    integer ibeg, iend;
    logical unit;
    integer i__, j;
    logical lower, upper;
    integer jj;
    logical gen, her, tri, sym;


/*  Generates values for an M by N matrix A. */
/*  Stores the values in the array AA in the data structure required */
/*  by the routine, with unwanted elements set to rogue value. */

/*  TYPE is 'ge', 'he', 'sy' or 'tr'. */

/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

    /* Parameter adjustments */
    a_dim1 = *nmax;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    --aa;

    /* Function Body */
    gen = s_cmp(type__, "ge", (ftnlen)2, (ftnlen)2) == 0;
    her = s_cmp(type__, "he", (ftnlen)2, (ftnlen)2) == 0;
    sym = s_cmp(type__, "sy", (ftnlen)2, (ftnlen)2) == 0;
    tri = s_cmp(type__, "tr", (ftnlen)2, (ftnlen)2) == 0;
    upper = (her || sym || tri) && *(unsigned char *)uplo == 'U';
    lower = (her || sym || tri) && *(unsigned char *)uplo == 'L';
    unit = tri && *(unsigned char *)diag == 'U';

/*     Generate data in array A. */

    i__1 = *n;
    for (j = 1; j <= i__1; ++j) {
	i__2 = *m;
	for (i__ = 1; i__ <= i__2; ++i__) {
	    if (gen || (upper && i__ <= j) || (lower && i__ >= j)) {
		i__3 = i__ + j * a_dim1;
		cbeg_(&q__2, reset);
		q__1.r = q__2.r + transl->r, q__1.i = q__2.i + transl->i;
		a[i__3].r = q__1.r, a[i__3].i = q__1.i;
		if (i__ != j) {
/*                 Set some elements to zero */
		    if (*n > 3 && j == *n / 2) {
			i__3 = i__ + j * a_dim1;
			a[i__3].r = 0.f, a[i__3].i = 0.f;
		    }
		    if (her) {
			i__3 = j + i__ * a_dim1;
			r_cnjg(&q__1, &a[i__ + j * a_dim1]);
			a[i__3].r = q__1.r, a[i__3].i = q__1.i;
		    } else if (sym) {
			i__3 = j + i__ * a_dim1;
			i__4 = i__ + j * a_dim1;
			a[i__3].r = a[i__4].r, a[i__3].i = a[i__4].i;
		    } else if (tri) {
			i__3 = j + i__ * a_dim1;
			a[i__3].r = 0.f, a[i__3].i = 0.f;
		    }
		}
	    }
/* L10: */
	}
	if (her) {
	    i__2 = j + j * a_dim1;
	    i__3 = j + j * a_dim1;
	    r__1 = a[i__3].r;
	    q__1.r = r__1, q__1.i = 0.f;
	    a[i__2].r = q__1.r, a[i__2].i = q__1.i;
	}
	if (tri) {
	    i__2 = j + j * a_dim1;
	    i__3 = j + j * a_dim1;
	    q__1.r = a[i__3].r + 1.f, q__1.i = a[i__3].i + 0.f;
	    a[i__2].r = q__1.r, a[i__2].i = q__1.i;
	}
	if (unit) {
	    i__2 = j + j * a_dim1;
	    a[i__2].r = 1.f, a[i__2].i = 0.f;
	}
/* L20: */
    }

/*     Store elements in array AS in data structure required by routine. */

    if (s_cmp(type__, "ge", (ftnlen)2, (ftnlen)2) == 0) {
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *m;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + (j - 1) * *lda;
		i__4 = i__ + j * a_dim1;
		aa[i__3].r = a[i__4].r, aa[i__3].i = a[i__4].i;
/* L30: */
	    }
	    i__2 = *lda;
	    for (i__ = *m + 1; i__ <= i__2; ++i__) {
		i__3 = i__ + (j - 1) * *lda;
		aa[i__3].r = -1e10f, aa[i__3].i = 1e10f;
/* L40: */
	    }
/* L50: */
	}
    } else if (s_cmp(type__, "he", (ftnlen)2, (ftnlen)2) == 0 || s_cmp(type__,
	     "sy", (ftnlen)2, (ftnlen)2) == 0 || s_cmp(type__, "tr", (ftnlen)
	    2, (ftnlen)2) == 0) {
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    if (upper) {
		ibeg = 1;
		if (unit) {
		    iend = j - 1;
		} else {
		    iend = j;
		}
	    } else {
		if (unit) {
		    ibeg = j + 1;
		} else {
		    ibeg = j;
		}
		iend = *n;
	    }
	    i__2 = ibeg - 1;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + (j - 1) * *lda;
		aa[i__3].r = -1e10f, aa[i__3].i = 1e10f;
/* L60: */
	    }
	    i__2 = iend;
	    for (i__ = ibeg; i__ <= i__2; ++i__) {
		i__3 = i__ + (j - 1) * *lda;
		i__4 = i__ + j * a_dim1;
		aa[i__3].r = a[i__4].r, aa[i__3].i = a[i__4].i;
/* L70: */
	    }
	    i__2 = *lda;
	    for (i__ = iend + 1; i__ <= i__2; ++i__) {
		i__3 = i__ + (j - 1) * *lda;
		aa[i__3].r = -1e10f, aa[i__3].i = 1e10f;
/* L80: */
	    }
	    if (her) {
		jj = j + (j - 1) * *lda;
		i__2 = jj;
		i__3 = jj;
		r__1 = aa[i__3].r;
		q__1.r = r__1, q__1.i = -1e10f;
		aa[i__2].r = q__1.r, aa[i__2].i = q__1.i;
	    }
/* L90: */
	}
    }
    return 0;

/*     End of CMAKE. */

} /* cmake_ */

/* Subroutine */ int cmmch_(char *transa, char *transb, integer *m, integer *
	n, integer *kk, complex *alpha, complex *a, integer *lda, complex *b, 
	integer *ldb, complex *beta, complex *c__, integer *ldc, complex *ct, 
	real *g, complex *cc, integer *ldcc, real *eps, real *err, logical *
	fatal, integer *nout, logical *mv)
{

    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, c_dim1, c_offset, cc_dim1, 
	    cc_offset, i__1, i__2, i__3, i__4, i__5, i__6, i__7;
    real r__1, r__2, r__3, r__4, r__5, r__6;
    complex q__1, q__2, q__3, q__4;

    /* Local variables */
    real erri;
    integer i__, j, k;
    logical trana, tranb, ctrana, ctranb;

/*  Checks the results of the computational tests. */

/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

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
    --ct;
    --g;
    cc_dim1 = *ldcc;
    cc_offset = 1 + cc_dim1 * 1;
    cc -= cc_offset;

    /* Function Body */
    trana = *(unsigned char *)transa == 'T' || *(unsigned char *)transa == 
	    'C';
    tranb = *(unsigned char *)transb == 'T' || *(unsigned char *)transb == 
	    'C';
    ctrana = *(unsigned char *)transa == 'C';
    ctranb = *(unsigned char *)transb == 'C';

/*     Compute expected result, one column at a time, in CT using data */
/*     in A, B and C. */
/*     Compute gauges in G. */

    i__1 = *n;
    for (j = 1; j <= i__1; ++j) {

	i__2 = *m;
	for (i__ = 1; i__ <= i__2; ++i__) {
	    i__3 = i__;
	    ct[i__3].r = 0.f, ct[i__3].i = 0.f;
	    g[i__] = 0.f;
/* L10: */
	}
	if (! trana && ! tranb) {
	    i__2 = *kk;
	    for (k = 1; k <= i__2; ++k) {
		i__3 = *m;
		for (i__ = 1; i__ <= i__3; ++i__) {
		    i__4 = i__;
		    i__5 = i__;
		    i__6 = i__ + k * a_dim1;
		    i__7 = k + j * b_dim1;
		    q__2.r = a[i__6].r * b[i__7].r - a[i__6].i * b[i__7].i, 
			    q__2.i = a[i__6].r * b[i__7].i + a[i__6].i * b[
			    i__7].r;
		    q__1.r = ct[i__5].r + q__2.r, q__1.i = ct[i__5].i + 
			    q__2.i;
		    ct[i__4].r = q__1.r, ct[i__4].i = q__1.i;
		    i__4 = i__ + k * a_dim1;
		    i__5 = k + j * b_dim1;
		    g[i__] += ((r__1 = a[i__4].r, abs(r__1)) + (r__2 = r_imag(
			    &a[i__ + k * a_dim1]), abs(r__2))) * ((r__3 = b[
			    i__5].r, abs(r__3)) + (r__4 = r_imag(&b[k + j * 
			    b_dim1]), abs(r__4)));
/* L20: */
		}
/* L30: */
	    }
	} else if (trana && ! tranb) {
	    if (ctrana) {
		i__2 = *kk;
		for (k = 1; k <= i__2; ++k) {
		    i__3 = *m;
		    for (i__ = 1; i__ <= i__3; ++i__) {
			i__4 = i__;
			i__5 = i__;
			r_cnjg(&q__3, &a[k + i__ * a_dim1]);
			i__6 = k + j * b_dim1;
			q__2.r = q__3.r * b[i__6].r - q__3.i * b[i__6].i, 
				q__2.i = q__3.r * b[i__6].i + q__3.i * b[i__6]
				.r;
			q__1.r = ct[i__5].r + q__2.r, q__1.i = ct[i__5].i + 
				q__2.i;
			ct[i__4].r = q__1.r, ct[i__4].i = q__1.i;
			i__4 = k + i__ * a_dim1;
			i__5 = k + j * b_dim1;
			g[i__] += ((r__1 = a[i__4].r, abs(r__1)) + (r__2 = 
				r_imag(&a[k + i__ * a_dim1]), abs(r__2))) * ((
				r__3 = b[i__5].r, abs(r__3)) + (r__4 = r_imag(
				&b[k + j * b_dim1]), abs(r__4)));
/* L40: */
		    }
/* L50: */
		}
	    } else {
		i__2 = *kk;
		for (k = 1; k <= i__2; ++k) {
		    i__3 = *m;
		    for (i__ = 1; i__ <= i__3; ++i__) {
			i__4 = i__;
			i__5 = i__;
			i__6 = k + i__ * a_dim1;
			i__7 = k + j * b_dim1;
			q__2.r = a[i__6].r * b[i__7].r - a[i__6].i * b[i__7]
				.i, q__2.i = a[i__6].r * b[i__7].i + a[i__6]
				.i * b[i__7].r;
			q__1.r = ct[i__5].r + q__2.r, q__1.i = ct[i__5].i + 
				q__2.i;
			ct[i__4].r = q__1.r, ct[i__4].i = q__1.i;
			i__4 = k + i__ * a_dim1;
			i__5 = k + j * b_dim1;
			g[i__] += ((r__1 = a[i__4].r, abs(r__1)) + (r__2 = 
				r_imag(&a[k + i__ * a_dim1]), abs(r__2))) * ((
				r__3 = b[i__5].r, abs(r__3)) + (r__4 = r_imag(
				&b[k + j * b_dim1]), abs(r__4)));
/* L60: */
		    }
/* L70: */
		}
	    }
	} else if (! trana && tranb) {
	    if (ctranb) {
		i__2 = *kk;
		for (k = 1; k <= i__2; ++k) {
		    i__3 = *m;
		    for (i__ = 1; i__ <= i__3; ++i__) {
			i__4 = i__;
			i__5 = i__;
			i__6 = i__ + k * a_dim1;
			r_cnjg(&q__3, &b[j + k * b_dim1]);
			q__2.r = a[i__6].r * q__3.r - a[i__6].i * q__3.i, 
				q__2.i = a[i__6].r * q__3.i + a[i__6].i * 
				q__3.r;
			q__1.r = ct[i__5].r + q__2.r, q__1.i = ct[i__5].i + 
				q__2.i;
			ct[i__4].r = q__1.r, ct[i__4].i = q__1.i;
			i__4 = i__ + k * a_dim1;
			i__5 = j + k * b_dim1;
			g[i__] += ((r__1 = a[i__4].r, abs(r__1)) + (r__2 = 
				r_imag(&a[i__ + k * a_dim1]), abs(r__2))) * ((
				r__3 = b[i__5].r, abs(r__3)) + (r__4 = r_imag(
				&b[j + k * b_dim1]), abs(r__4)));
/* L80: */
		    }
/* L90: */
		}
	    } else {
		i__2 = *kk;
		for (k = 1; k <= i__2; ++k) {
		    i__3 = *m;
		    for (i__ = 1; i__ <= i__3; ++i__) {
			i__4 = i__;
			i__5 = i__;
			i__6 = i__ + k * a_dim1;
			i__7 = j + k * b_dim1;
			q__2.r = a[i__6].r * b[i__7].r - a[i__6].i * b[i__7]
				.i, q__2.i = a[i__6].r * b[i__7].i + a[i__6]
				.i * b[i__7].r;
			q__1.r = ct[i__5].r + q__2.r, q__1.i = ct[i__5].i + 
				q__2.i;
			ct[i__4].r = q__1.r, ct[i__4].i = q__1.i;
			i__4 = i__ + k * a_dim1;
			i__5 = j + k * b_dim1;
			g[i__] += ((r__1 = a[i__4].r, abs(r__1)) + (r__2 = 
				r_imag(&a[i__ + k * a_dim1]), abs(r__2))) * ((
				r__3 = b[i__5].r, abs(r__3)) + (r__4 = r_imag(
				&b[j + k * b_dim1]), abs(r__4)));
/* L100: */
		    }
/* L110: */
		}
	    }
	} else if (trana && tranb) {
	    if (ctrana) {
		if (ctranb) {
		    i__2 = *kk;
		    for (k = 1; k <= i__2; ++k) {
			i__3 = *m;
			for (i__ = 1; i__ <= i__3; ++i__) {
			    i__4 = i__;
			    i__5 = i__;
			    r_cnjg(&q__3, &a[k + i__ * a_dim1]);
			    r_cnjg(&q__4, &b[j + k * b_dim1]);
			    q__2.r = q__3.r * q__4.r - q__3.i * q__4.i, 
				    q__2.i = q__3.r * q__4.i + q__3.i * 
				    q__4.r;
			    q__1.r = ct[i__5].r + q__2.r, q__1.i = ct[i__5].i 
				    + q__2.i;
			    ct[i__4].r = q__1.r, ct[i__4].i = q__1.i;
			    i__4 = k + i__ * a_dim1;
			    i__5 = j + k * b_dim1;
			    g[i__] += ((r__1 = a[i__4].r, abs(r__1)) + (r__2 =
				     r_imag(&a[k + i__ * a_dim1]), abs(r__2)))
				     * ((r__3 = b[i__5].r, abs(r__3)) + (r__4 
				    = r_imag(&b[j + k * b_dim1]), abs(r__4)));
/* L120: */
			}
/* L130: */
		    }
		} else {
		    i__2 = *kk;
		    for (k = 1; k <= i__2; ++k) {
			i__3 = *m;
			for (i__ = 1; i__ <= i__3; ++i__) {
			    i__4 = i__;
			    i__5 = i__;
			    r_cnjg(&q__3, &a[k + i__ * a_dim1]);
			    i__6 = j + k * b_dim1;
			    q__2.r = q__3.r * b[i__6].r - q__3.i * b[i__6].i, 
				    q__2.i = q__3.r * b[i__6].i + q__3.i * b[
				    i__6].r;
			    q__1.r = ct[i__5].r + q__2.r, q__1.i = ct[i__5].i 
				    + q__2.i;
			    ct[i__4].r = q__1.r, ct[i__4].i = q__1.i;
			    i__4 = k + i__ * a_dim1;
			    i__5 = j + k * b_dim1;
			    g[i__] += ((r__1 = a[i__4].r, abs(r__1)) + (r__2 =
				     r_imag(&a[k + i__ * a_dim1]), abs(r__2)))
				     * ((r__3 = b[i__5].r, abs(r__3)) + (r__4 
				    = r_imag(&b[j + k * b_dim1]), abs(r__4)));
/* L140: */
			}
/* L150: */
		    }
		}
	    } else {
		if (ctranb) {
		    i__2 = *kk;
		    for (k = 1; k <= i__2; ++k) {
			i__3 = *m;
			for (i__ = 1; i__ <= i__3; ++i__) {
			    i__4 = i__;
			    i__5 = i__;
			    i__6 = k + i__ * a_dim1;
			    r_cnjg(&q__3, &b[j + k * b_dim1]);
			    q__2.r = a[i__6].r * q__3.r - a[i__6].i * q__3.i, 
				    q__2.i = a[i__6].r * q__3.i + a[i__6].i * 
				    q__3.r;
			    q__1.r = ct[i__5].r + q__2.r, q__1.i = ct[i__5].i 
				    + q__2.i;
			    ct[i__4].r = q__1.r, ct[i__4].i = q__1.i;
			    i__4 = k + i__ * a_dim1;
			    i__5 = j + k * b_dim1;
			    g[i__] += ((r__1 = a[i__4].r, abs(r__1)) + (r__2 =
				     r_imag(&a[k + i__ * a_dim1]), abs(r__2)))
				     * ((r__3 = b[i__5].r, abs(r__3)) + (r__4 
				    = r_imag(&b[j + k * b_dim1]), abs(r__4)));
/* L160: */
			}
/* L170: */
		    }
		} else {
		    i__2 = *kk;
		    for (k = 1; k <= i__2; ++k) {
			i__3 = *m;
			for (i__ = 1; i__ <= i__3; ++i__) {
			    i__4 = i__;
			    i__5 = i__;
			    i__6 = k + i__ * a_dim1;
			    i__7 = j + k * b_dim1;
			    q__2.r = a[i__6].r * b[i__7].r - a[i__6].i * b[
				    i__7].i, q__2.i = a[i__6].r * b[i__7].i + 
				    a[i__6].i * b[i__7].r;
			    q__1.r = ct[i__5].r + q__2.r, q__1.i = ct[i__5].i 
				    + q__2.i;
			    ct[i__4].r = q__1.r, ct[i__4].i = q__1.i;
			    i__4 = k + i__ * a_dim1;
			    i__5 = j + k * b_dim1;
			    g[i__] += ((r__1 = a[i__4].r, abs(r__1)) + (r__2 =
				     r_imag(&a[k + i__ * a_dim1]), abs(r__2)))
				     * ((r__3 = b[i__5].r, abs(r__3)) + (r__4 
				    = r_imag(&b[j + k * b_dim1]), abs(r__4)));
/* L180: */
			}
/* L190: */
		    }
		}
	    }
	}
	i__2 = *m;
	for (i__ = 1; i__ <= i__2; ++i__) {
	    i__3 = i__;
	    i__4 = i__;
	    q__2.r = alpha->r * ct[i__4].r - alpha->i * ct[i__4].i, q__2.i = 
		    alpha->r * ct[i__4].i + alpha->i * ct[i__4].r;
	    i__5 = i__ + j * c_dim1;
	    q__3.r = beta->r * c__[i__5].r - beta->i * c__[i__5].i, q__3.i = 
		    beta->r * c__[i__5].i + beta->i * c__[i__5].r;
	    q__1.r = q__2.r + q__3.r, q__1.i = q__2.i + q__3.i;
	    ct[i__3].r = q__1.r, ct[i__3].i = q__1.i;
	    i__3 = i__ + j * c_dim1;
	    g[i__] = ((r__1 = alpha->r, abs(r__1)) + (r__2 = r_imag(alpha), 
		    abs(r__2))) * g[i__] + ((r__3 = beta->r, abs(r__3)) + (
		    r__4 = r_imag(beta), abs(r__4))) * ((r__5 = c__[i__3].r, 
		    abs(r__5)) + (r__6 = r_imag(&c__[i__ + j * c_dim1]), abs(
		    r__6)));
/* L200: */
	}

/*        Compute the error ratio for this result. */

	*err = 0.f;
	i__2 = *m;
	for (i__ = 1; i__ <= i__2; ++i__) {
	    i__3 = i__;
	    i__4 = i__ + j * cc_dim1;
	    q__2.r = ct[i__3].r - cc[i__4].r, q__2.i = ct[i__3].i - cc[i__4]
		    .i;
	    q__1.r = q__2.r, q__1.i = q__2.i;
	    erri = ((r__1 = q__1.r, abs(r__1)) + (r__2 = r_imag(&q__1), abs(
		    r__2))) / *eps;
	    if (g[i__] != 0.f) {
		erri /= g[i__];
	    }
	    *err = f2cmax(*err,erri);
	    if (*err * sqrt(*eps) >= 1.f) {
		goto L230;
	    }
/* L210: */
	}

/* L220: */
    }

/*     If the loop completes, all results are at least half accurate. */
    goto L250;

/*     Report fatal error. */

L230:
    *fatal = TRUE_;
    printf(" ******* FATAL ERROR - COMPUTED RESULT IS LESS THAN HALF ACCURATE *******\n");
    printf("         EXPECTED RESULT                    COMPUTED RESULT\n");
    i__1 = *m;
    for (i__ = 1; i__ <= i__1; ++i__) {
	if (*mv) {
            printf("%7d (%15.6g,%15.6g) (%15.6g,%15.6g)\n",i__,ct[i__].r,ct[i__].i,cc[i__+j*cc_dim1].r,cc[i__+j*cc_dim1].i);
	} else {
            printf("%7d (%15.6g,%15.6g) (%15.6g,%15.6g)\n",i__,cc[i__+j*cc_dim1].r,cc[i__+j*cc_dim1].i,ct[i__].r,ct[i__].i);
	}
/* L240: */
    }
    if (*n > 1) {
    	printf("      THESE ARE THE RESULTS FOR COLUMN %d\n",j);
    }

L250:
    return 0;


/*     End of CMMCH. */

} /* cmmch_ */

logical lce_(complex *ri, complex *rj, integer *lr)
{
    /* System generated locals */
    integer i__1, i__2, i__3;
    logical ret_val;

    /* Local variables */
    integer i__;


/*  Tests if two arrays are identical. */

/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

    /* Parameter adjustments */
    --rj;
    --ri;

    /* Function Body */
    i__1 = *lr;
    for (i__ = 1; i__ <= i__1; ++i__) {
	i__2 = i__;
	i__3 = i__;
	if (ri[i__2].r != rj[i__3].r || ri[i__2].i != rj[i__3].i) {
	    goto L20;
	}
/* L10: */
    }
    ret_val = TRUE_;
    goto L30;
L20:
    ret_val = FALSE_;
L30:
    return ret_val;

/*     End of LCE. */

} /* lce_ */

logical lceres_(char *type__, char *uplo, integer *m, integer *n, complex *aa,
	 complex *as, integer *lda)
{
    /* System generated locals */
    integer aa_dim1, aa_offset, as_dim1, as_offset, i__1, i__2, i__3, i__4;
    logical ret_val;

    /* Local variables */
    integer ibeg, iend, i__, j;
    logical upper;


/*  Tests if selected elements in two arrays are equal. */

/*  TYPE is 'ge' or 'he' or 'sy'. */

/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

    /* Parameter adjustments */
    as_dim1 = *lda;
    as_offset = 1 + as_dim1 * 1;
    as -= as_offset;
    aa_dim1 = *lda;
    aa_offset = 1 + aa_dim1 * 1;
    aa -= aa_offset;

    /* Function Body */
    upper = *(unsigned char *)uplo == 'U';
    if (s_cmp(type__, "ge", (ftnlen)2, (ftnlen)2) == 0) {
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *lda;
	    for (i__ = *m + 1; i__ <= i__2; ++i__) {
		i__3 = i__ + j * aa_dim1;
		i__4 = i__ + j * as_dim1;
		if (aa[i__3].r != as[i__4].r || aa[i__3].i != as[i__4].i) {
		    goto L70;
		}
/* L10: */
	    }
/* L20: */
	}
    } else if (s_cmp(type__, "he", (ftnlen)2, (ftnlen)2) == 0 || s_cmp(type__,
	     "sy", (ftnlen)2, (ftnlen)2) == 0) {
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    if (upper) {
		ibeg = 1;
		iend = j;
	    } else {
		ibeg = j;
		iend = *n;
	    }
	    i__2 = ibeg - 1;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + j * aa_dim1;
		i__4 = i__ + j * as_dim1;
		if (aa[i__3].r != as[i__4].r || aa[i__3].i != as[i__4].i) {
		    goto L70;
		}
/* L30: */
	    }
	    i__2 = *lda;
	    for (i__ = iend + 1; i__ <= i__2; ++i__) {
		i__3 = i__ + j * aa_dim1;
		i__4 = i__ + j * as_dim1;
		if (aa[i__3].r != as[i__4].r || aa[i__3].i != as[i__4].i) {
		    goto L70;
		}
/* L40: */
	    }
/* L50: */
	}
    }

/*   60 CONTINUE */
    ret_val = TRUE_;
    goto L80;
L70:
    ret_val = FALSE_;
L80:
    return ret_val;

/*     End of LCERES. */

} /* lceres_ */

/* Complex */ VOID cbeg_(complex * ret_val, logical *reset)
{
    /* System generated locals */
    real r__1, r__2;
    complex q__1;

    /* Local variables */
    static integer i__, j, ic, mi, mj;


/*  Generates complex numbers as pairs of random numbers uniformly */
/*  distributed between -0.5 and 0.5. */

/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

    if (*reset) {
/*        Initialize local variables. */
	mi = 891;
	mj = 457;
	i__ = 7;
	j = 7;
	ic = 0;
	*reset = FALSE_;
    }

/*     The sequence of values of I or J is bounded between 1 and 999. */
/*     If initial I or J = 1,2,3,6,7 or 9, the period will be 50. */
/*     If initial I or J = 4 or 8, the period will be 25. */
/*     If initial I or J = 5, the period will be 10. */
/*     IC is used to break up the period by skipping 1 value of I or J */
/*     in 6. */

    ++ic;
L10:
    i__ *= mi;
    j *= mj;
    i__ -= i__ / 1000 * 1000;
    j -= j / 1000 * 1000;
    if (ic >= 5) {
	ic = 0;
	goto L10;
    }
    r__1 = (i__ - 500) / 1001.f;
    r__2 = (j - 500) / 1001.f;
    q__1.r = r__1, q__1.i = r__2;
     ret_val->r = q__1.r,  ret_val->i = q__1.i;
    return ;

/*     End of CBEG. */

} /* cbeg_ */

real sdiff_(real *x, real *y)
{
    /* System generated locals */
    real ret_val;


/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

    ret_val = *x - *y;
    return ret_val;

/*     End of SDIFF. */

} /* sdiff_ */

/* Main program alias */ /*int cblat3_ () { MAIN__ (); return 0; }*/
