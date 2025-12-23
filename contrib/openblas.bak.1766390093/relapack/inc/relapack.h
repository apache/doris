#ifndef RELAPACK_H
#define RELAPACK_H

#ifdef USE64BITINT
  typedef BLASLONG blasint;
  #if defined(OS_WINDOWS) && defined(__64BIT__)
     #define blasabs(x) llabs(x)
  #else
     #define blasabs(x) labs(x)
  #endif
#else
  typedef int blasint;
  #define blasabs(x) abs(x)
#endif

void RELAPACK_slauum(const char *, const blasint *, float *, const blasint *, blasint *);
void RELAPACK_dlauum(const char *, const blasint *, double *, const blasint *, blasint *);
void RELAPACK_clauum(const char *, const blasint *, float *, const blasint *, blasint *);
void RELAPACK_zlauum(const char *, const blasint *, double *, const blasint *, blasint *);

void RELAPACK_strtri(const char *, const char *, const blasint *, float *, const blasint *, blasint *);
void RELAPACK_dtrtri(const char *, const char *, const blasint *, double *, const blasint *, blasint *);
void RELAPACK_ctrtri(const char *, const char *, const blasint *, float *, const blasint *, blasint *);
void RELAPACK_ztrtri(const char *, const char *, const blasint *, double *, const blasint *, blasint *);

void RELAPACK_spotrf(const char *, const blasint *, float *, const blasint *, blasint *);
void RELAPACK_dpotrf(const char *, const blasint *, double *, const blasint *, blasint *);
void RELAPACK_cpotrf(const char *, const blasint *, float *, const blasint *, blasint *);
void RELAPACK_zpotrf(const char *, const blasint *, double *, const blasint *, blasint *);

void RELAPACK_spbtrf(const char *, const blasint *, const blasint *, float *, const blasint *, blasint *);
void RELAPACK_dpbtrf(const char *, const blasint *, const blasint *, double *, const blasint *, blasint *);
void RELAPACK_cpbtrf(const char *, const blasint *, const blasint *, float *, const blasint *, blasint *);
void RELAPACK_zpbtrf(const char *, const blasint *, const blasint *, double *, const blasint *, blasint *);

void RELAPACK_ssytrf(const char *, const blasint *, float *, const blasint *, blasint *, float *, const blasint *, blasint *);
void RELAPACK_dsytrf(const char *, const blasint *, double *, const blasint *, blasint *, double *, const blasint *, blasint *);
void RELAPACK_csytrf(const char *, const blasint *, float *, const blasint *, blasint *, float *, const blasint *, blasint *);
void RELAPACK_chetrf(const char *, const blasint *, float *, const blasint *, blasint *, float *, const blasint *, blasint *);
void RELAPACK_zsytrf(const char *, const blasint *, double *, const blasint *, blasint *, double *, const blasint *, blasint *);
void RELAPACK_zhetrf(const char *, const blasint *, double *, const blasint *, blasint *, double *, const blasint *, blasint *);
void RELAPACK_ssytrf_rook(const char *, const blasint *, float *, const blasint *, blasint *, float *, const blasint *, blasint *);
void RELAPACK_dsytrf_rook(const char *, const blasint *, double *, const blasint *, blasint *, double *, const blasint *, blasint *);
void RELAPACK_csytrf_rook(const char *, const blasint *, float *, const blasint *, blasint *, float *, const blasint *, blasint *);
void RELAPACK_chetrf_rook(const char *, const blasint *, float *, const blasint *, blasint *, float *, const blasint *, blasint *);
void RELAPACK_zsytrf_rook(const char *, const blasint *, double *, const blasint *, blasint *, double *, const blasint *, blasint *);
void RELAPACK_zhetrf_rook(const char *, const blasint *, double *, const blasint *, blasint *, double *, const blasint *, blasint *);

void RELAPACK_sgetrf(const blasint *, const blasint *, float *, const blasint *, blasint *, blasint *);
void RELAPACK_dgetrf(const blasint *, const blasint *, double *, const blasint *, blasint *, blasint *);
void RELAPACK_cgetrf(const blasint *, const blasint *, float *, const blasint *, blasint *, blasint *);
void RELAPACK_zgetrf(const blasint *, const blasint *, double *, const blasint *, blasint *, blasint *);

void RELAPACK_sgbtrf(const blasint *, const blasint *, const blasint *, const blasint *, float *, const blasint *, blasint *, blasint *);
void RELAPACK_dgbtrf(const blasint *, const blasint *, const blasint *, const blasint *, double *, const blasint *, blasint *, blasint *);
void RELAPACK_cgbtrf(const blasint *, const blasint *, const blasint *, const blasint *, float *, const blasint *, blasint *, blasint *);
void RELAPACK_zgbtrf(const blasint *, const blasint *, const blasint *, const blasint *, double *, const blasint *, blasint *, blasint *);

void RELAPACK_ssygst(const blasint *, const char *, const blasint *, float *, const blasint *, const float *, const blasint *, blasint *);
void RELAPACK_dsygst(const blasint *, const char *, const blasint *, double *, const blasint *, const double *, const blasint *, blasint *);
void RELAPACK_chegst(const blasint *, const char *, const blasint *, float *, const blasint *, const float *, const blasint *, blasint *);
void RELAPACK_zhegst(const blasint *, const char *, const blasint *, double *, const blasint *, const double *, const blasint *, blasint *);

void RELAPACK_strsyl(const char *, const char *, const blasint *, const blasint *, const blasint *, const float *, const blasint *, const float *, const blasint *, float *, const blasint *, float *, blasint *);
void RELAPACK_dtrsyl(const char *, const char *, const blasint *, const blasint *, const blasint *, const double *, const blasint *, const double *, const blasint *, double *, const blasint *, double *, blasint *);
void RELAPACK_ctrsyl(const char *, const char *, const blasint *, const blasint *, const blasint *, const float *, const blasint *, const float *, const blasint *, float *, const blasint *, float *, blasint *);
void RELAPACK_ztrsyl(const char *, const char *, const blasint *, const blasint *, const blasint *, const double *, const blasint *, const double *, const blasint *, double *, const blasint *, double *, blasint *);

void RELAPACK_stgsyl(const char *, const blasint *, const blasint *, const blasint *, const float *, const blasint *, const float *, const blasint *, float *, const blasint *, const float *, const blasint *, const float *, const blasint *, float *, const blasint *, float *, float *, float *, const blasint *, blasint *, blasint *);
void RELAPACK_dtgsyl(const char *, const blasint *, const blasint *, const blasint *, const double *, const blasint *, const double *, const blasint *, double *, const blasint *, const double *, const blasint *, const double *, const blasint *, double *, const blasint *, double *, double *, double *, const blasint *, blasint *, blasint *);
void RELAPACK_ctgsyl(const char *, const blasint *, const blasint *, const blasint *, const float *, const blasint *, const float *, const blasint *, float *, const blasint *, const float *, const blasint *, const float *, const blasint *, float *, const blasint *, float *, float *, float *, const blasint *, blasint *, blasint *);
void RELAPACK_ztgsyl(const char *, const blasint *, const blasint *, const blasint *, const double *, const blasint *, const double *, const blasint *, double *, const blasint *, const double *, const blasint *, const double *, const blasint *, double *, const blasint *, double *, double *, double *, const blasint *, blasint *, blasint *);

void RELAPACK_sgemmt(const char *, const char *, const char *, const blasint *, const blasint *, const float *, const float *, const blasint *, const float *, const blasint *, const float *, float *, const blasint *);
void RELAPACK_dgemmt(const char *, const char *, const char *, const blasint *, const blasint *, const double *, const double *, const blasint *, const double *, const blasint *, const double *, double *, const blasint *);
void RELAPACK_cgemmt(const char *, const char *, const char *, const blasint *, const blasint *, const float *, const float *, const blasint *, const float *, const blasint *, const float *, float *, const blasint *);
void RELAPACK_zgemmt(const char *, const char *, const char *, const blasint *, const blasint *, const double *, const double *, const blasint *, const double *, const blasint *, const double *, double *, const blasint *);

#endif /*  RELAPACK_H */
