#!/usr/bin/python3

import sys, os
import contextlib

#-----------------------------------------------------------------------
def ERROR(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)
    sys.exit(-1)

class Target(object):
    def __init__( self, out, mappings, initial_level=0, tab_width=4 ):
        self._level = initial_level
        self._tab_width = tab_width
        self._out = out
        self._mappings = mappings

    @contextlib.contextmanager
    def map( self, **items ):
        old_mappings = self._mappings
        self._mappings = dict(old_mappings, **items)
        yield self._mappings
        self._mappings = old_mappings

    @contextlib.contextmanager
    def block( self, start=None, end=None, **args ):
        with self.map(**args):
            if start is not None:
                self.write();
                self.write(start)
            self._level += 1
            yield self._level
            self._level -= 1
            if end is not None:
                self.write(end)
                self.write()

    def write( self, fmt=None, *args, **kwargs ):
        if fmt is not None:
            mappings = dict(self._mappings, **kwargs) if kwargs else self._mappings
            self._out(self._indent_str() + fmt.format(*args, **mappings))
        else:
            self._out("")

    def _indent_str( self ):
        return ' ' * (self._level * self._tab_width)

#-----------------------------------------------------------------------
def generate_trmm_block( dest ):
    dest.write("{index_type} pass_K = K;")
    dest.write("#ifdef LEFT")
    with dest.block():
        dest.write("{index_type} off = offset + m_top;")
    dest.write("#else")
    with dest.block():
        dest.write("{index_type} off = -offset + n_top;")
    dest.write("#endif")

    dest.write("#ifdef BACKWARDS")
    with dest.block():
        dest.write("ai += off*{M}{elt_size};")
        dest.write("bi += off*{N}{elt_size};")
        dest.write("pass_K -= off;")
    dest.write("#else")
    with dest.block():
        dest.write("#ifdef LEFT")
        with dest.block():
            dest.write("pass_K = off + {M};")
        dest.write("#else")
        with dest.block():
            dest.write("pass_K = off + {N};")
        dest.write("#endif")
    dest.write("#endif")

#-----------------------------------------------------------------------
def generate_gemm_kernel_inner_real( settings, dest, M, N, vlen, a_regs ):
    TRMM           = (settings['op'].value == 'trmm')
    narrow_result  = (settings['param_precision'].value != 'double') and settings['force_acc_double'].value

    with dest.map( 
        M=M, 
        N=N, 
    ):
        dest.write("{index_type} ai=m_top*K{elt_size};")
        dest.write("{index_type} bi=n_top*K{elt_size};")
        if TRMM:
            generate_trmm_block( dest )

        for i in range(N):
            dest.write("{param_scalar_t} B{i} = B[bi+{i}];", i=i)
        dest.write("bi += {N};")
        dest.write()

        for i in range(a_regs):
            dest.write("{param_vector_t} A{i} = {VLEV}( &A[ai+{i}*gvl], gvl );", i=i)
        dest.write("ai += {M};")
        dest.write()

        for j in range(N):
            for i in range(a_regs):
                dest.write("{acc_vector_t} result{dest} = {VMUL_TO_ACC}( A{i}, B{j}, gvl);", dest=j*a_regs+i, i=i, j=j)

        with dest.block("for({index_type} k=1; k<{Kend}; k++) {{", "}}", Kend=('pass_K' if TRMM else 'K')):
            for i in range(N):
                dest.write("B{i} = B[bi+{i}];", i=i )
            dest.write("bi += {N};")
            dest.write()

            for i in range(a_regs):
                dest.write("A{i} = {VLEV}( &A[ai+{i}*gvl], gvl );", i=i)

            dest.write("ai += {M};")
            dest.write()


            for j in range(N):
                for i in range(a_regs):
                    dest.write("result{dest} = {VMACC_TO_ACC}( result{dest}, B{j}, A{i}, gvl);", dest= j*a_regs+i, j=j, i=i )

        dest.write()
        dest.write("{index_type} ci=n_top*ldc+m_top;")
        dest.write()

        if narrow_result:
            for j in range(N):
                for i in range(a_regs):
                    dest.write("{param_vector_t} narrowed{idx} = {VFNCVT}( result{idx}, gvl );", idx=j*a_regs+i)

        if not TRMM:
            for j in range(N):
                for i in range(a_regs):
                    idx = j*a_regs+i
                    increment = ' ci += ldc-gvl*{};'.format(a_regs-1) if (i == a_regs-1) else ' ci += gvl;'
                    if idx == N*a_regs-1:
                        increment = ''
                    dest.write("{param_vector_t} c{idx} = {VLEV}( &C[ci], gvl);{increment}", idx=idx, increment=increment)

        if narrow_result:
            for j in range(N):
                for i in range(a_regs):
                    idx = j*a_regs+i
                    if TRMM:
                        dest.write("{param_vector_t} c{idx} = {VFMUL}( narrowed{idx}, alpha, gvl );", idx=idx)
                    else:
                        dest.write("c{idx} = {VFMACC}( c{idx}, alpha, narrowed{idx}, gvl );", idx=idx)
        else:
            for j in range(N):
                for i in range(a_regs):
                    idx = j*a_regs+i
                    if TRMM:
                        dest.write("{param_vector_t} c{idx} = {VFMUL}( result{idx}, alpha, gvl );", idx=idx)
                    else:
                        dest.write("c{idx} = {VFMACC}( c{idx}, alpha, result{idx}, gvl );", idx=idx)
            

        if not TRMM:
            dest.write()
            dest.write("ci=n_top*ldc+m_top;")
            dest.write()

        for j in range(N):
            for i in range(a_regs):
                idx = j*a_regs+i
                increment = ' ci += ldc-gvl*{};'.format(a_regs-1) if (i == a_regs-1) else ' ci += gvl;'
                if idx == N*a_regs-1:
                    increment = ''
                dest.write("{VSEV}( &C[ci], c{idx}, gvl);{increment}", idx=idx, increment=increment)


#-----------------------------------------------------------------------
def generate_gemm_kernel_inner_complex( settings, dest, M, N, vlen, a_regs ):
    TRMM           = (settings['op'].value == 'trmm')
    narrow_result  = (settings['param_precision'].value != 'double') and settings['force_acc_double'].value

    if narrow_result:
        raise RuntimeError("wide accumulator not supported for generated complex kernels")
        # we could, but we run out of registers really really fast

    with dest.map( 
        M=M, 
        N=N, 
    ):
        dest.write("{index_type} ai=m_top*K*2;")
        dest.write("{index_type} bi=n_top*K*2;")
        if TRMM:
            generate_trmm_block( dest )

        for i in range(N):
            dest.write("{param_scalar_t} B{i}r = B[bi+{i}*2+0];", i=i)
            dest.write("{param_scalar_t} B{i}i = B[bi+{i}*2+1];", i=i)
        dest.write("bi += {N}*2;")
        dest.write()

        for i in range(a_regs):
            dest.write("{param_vector_t} A{i}r = {VLSEV}( &A[ai+{i}*gvl*2], sizeof(FLOAT)*2, gvl );", i=i)
            dest.write("{param_vector_t} A{i}i = {VLSEV}( &A[ai+{i}*gvl*2+1], sizeof(FLOAT)*2, gvl );", i=i)
        dest.write("ai += {M}*2;")
        dest.write()

        # for each vector register loaded from matrix A, we require N registers to hold vector-scalar multiply-accumulate results
        accumulation_regs = a_regs * N
        dest.write("// {a_regs} vector regs to hold A array contents, {accumulation_regs} regs to hold values accumulated over k",
                a_regs=a_regs*2, accumulation_regs=accumulation_regs*2
            )
        pass_regs = (accumulation_regs + a_regs)*2
        tmp_regs = (32 // settings['LMUL_ACC'].value) - pass_regs
        if tmp_regs < 2:
            raise RuntimeError("Complex kernel would use too many registers!")

        dest.write("// leaving {tmp_regs} vector registers for temporaries", tmp_regs=tmp_regs)

        tmp_unroll_i = min(tmp_regs, a_regs)
        tmp_unroll_j = N
        while tmp_unroll_j > 1 and (tmp_regs/(tmp_unroll_i*2)) < tmp_unroll_j:
            tmp_unroll_j = int(tmp_unroll_j / 2)

        if tmp_unroll_i < a_regs or tmp_unroll_j < N:
            dest.write("// performing {ops} operations between reuses of temporaries", ops=tmp_unroll_j*tmp_unroll_i)

        for tj in range(0, N, tmp_unroll_j):
            for ti in range(0, a_regs, tmp_unroll_i):
                for j in range(tj, tj+tmp_unroll_j):
                    for i in range(ti, ti+tmp_unroll_i):
                        with dest.map(dest=j*a_regs+i, tmp=(i-ti)+tmp_unroll_i*(j-tj), i=i, j=j):
                            if ti == 0 and tj==0:
                                dest.write("{acc_vector_t} tmp{tmp}r = {VMUL_TO_ACC}( A{i}i, B{j}i, gvl);")
                                dest.write("{acc_vector_t} tmp{tmp}i = {VMUL_TO_ACC}( A{i}r, B{j}i, gvl);")
                            else:
                                dest.write("tmp{tmp}r = {VMUL_TO_ACC}( A{i}i, B{j}i, gvl);")
                                dest.write("tmp{tmp}i = {VMUL_TO_ACC}( A{i}r, B{j}i, gvl);")
                for j in range(tj, tj+tmp_unroll_j):
                    for i in range(ti, ti+tmp_unroll_i):
                        with dest.map(dest=j*a_regs+i, tmp=(i-ti)+tmp_unroll_i*(j-tj), i=i, j=j):
                            dest.write("tmp{tmp}r = VFMACC_RR( tmp{tmp}r, B{j}r, A{i}r, gvl);")
                            dest.write("tmp{tmp}i = VFMACC_RI( tmp{tmp}i, B{j}r, A{i}i, gvl);")

                for j in range(tj, tj+tmp_unroll_j):
                    for i in range(ti, ti+tmp_unroll_i):
                        with dest.map(dest=j*a_regs+i, tmp=(i-ti)+tmp_unroll_i*(j-tj), i=i, j=j):
                            dest.write("{acc_vector_t} ACC{dest}r = tmp{tmp}r;")
                            dest.write("{acc_vector_t} ACC{dest}i = tmp{tmp}i;")

        with dest.block("for({index_type} k=1; k<{Kend}; k++) {{", "}}", Kend=('pass_K' if TRMM else 'K')):
            for i in range(N):
                dest.write("B{i}r = B[bi+{i}*2+0];", i=i)
                dest.write("B{i}i = B[bi+{i}*2+1];", i=i)
            dest.write("bi += {N}*2;")
            dest.write()

            for i in range(a_regs):
                dest.write("A{i}r = {VLSEV}( &A[ai+{i}*gvl*2], sizeof(FLOAT)*2, gvl );", i=i)
                dest.write("A{i}i = {VLSEV}( &A[ai+{i}*gvl*2+1], sizeof(FLOAT)*2, gvl );", i=i)

            dest.write("ai += {M}*2;")
            dest.write()


            for tj in range(0, N, tmp_unroll_j):
                for ti in range(0, a_regs, tmp_unroll_i):
                    # note the values in tmp{tmp}* are frequently of similar magnitude and opposite sign
                    # so accumulating them directly to ACC would lose precision when ACC is larger

                    for j in range(tj, tj+tmp_unroll_j):
                        for i in range(ti, ti+tmp_unroll_i):
                            with dest.map(dest=j*a_regs+i, tmp=(i-ti)+tmp_unroll_i*(j-tj), i=i, j=j):
                                dest.write("tmp{tmp}r = {VMUL_TO_ACC}( A{i}i, B{j}i, gvl);")
                                dest.write("tmp{tmp}i = {VMUL_TO_ACC}( A{i}r, B{j}i, gvl);")
                    for j in range(tj, tj+tmp_unroll_j):
                        for i in range(ti, ti+tmp_unroll_i):
                            with dest.map(dest=j*a_regs+i, tmp=(i-ti)+tmp_unroll_i*(j-tj), i=i, j=j):
                                dest.write("tmp{tmp}r = VFMACC_RR( tmp{tmp}r, B{j}r, A{i}r, gvl);")
                                dest.write("tmp{tmp}i = VFMACC_RI( tmp{tmp}i, B{j}r, A{i}i, gvl);")
                    for j in range(tj, tj+tmp_unroll_j):
                        for i in range(ti, ti+tmp_unroll_i):
                            with dest.map(dest=j*a_regs+i, tmp=(i-ti)+tmp_unroll_i*(j-tj), i=i, j=j):
                                dest.write("ACC{dest}r = {__riscv_}vfadd( ACC{dest}r, tmp{tmp}r, gvl);")
                                dest.write("ACC{dest}i = {__riscv_}vfadd( ACC{dest}i, tmp{tmp}i, gvl);")

        dest.write()
        dest.write("{index_type} ci=n_top*ldc+m_top;")
        dest.write()

        for j in range(N):
            if TRMM:
                for i in range(a_regs):
                    with dest.map(idx=j*a_regs+i):
                        dest.write("{param_vector_t} C{idx}r = {__riscv_}vfmul( ACC{idx}r, alphar, gvl );")
                        dest.write("{param_vector_t} C{idx}i = {__riscv_}vfmul( ACC{idx}i, alphar, gvl );")
            else:
                for i in range(a_regs):
                    idx = j*a_regs+i
                    increment = 'ci += ldc-gvl*{};'.format(a_regs-1) if (i == a_regs-1) else ' ci += gvl;'
                    if idx == N*a_regs-1:
                        increment = ''                    
                    with dest.map(idx=j*a_regs+i, increment=increment):
                        dest.write("{param_vector_t} C{idx}r = {VLSEV}( &C[ci*2+0], sizeof(FLOAT)*2, gvl );")
                        dest.write("{param_vector_t} C{idx}i = {VLSEV}( &C[ci*2+1], sizeof(FLOAT)*2, gvl );")
                        dest.write("{increment}")

        if not TRMM:
            for j in range(N):
                for i in range(a_regs):
                    with dest.map(idx=j*a_regs+i):
                        dest.write("C{idx}r = {__riscv_}vfmacc( C{idx}r, alphar, ACC{idx}r, gvl );")
                        dest.write("C{idx}i = {__riscv_}vfmacc( C{idx}i, alphar, ACC{idx}i, gvl );")

        for j in range(N):
            for i in range(a_regs):
                with dest.map(idx=j*a_regs+i):
                    dest.write("C{idx}r = {__riscv_}vfnmsac( C{idx}r, alphai, ACC{idx}i, gvl );")
                    dest.write("C{idx}i = {__riscv_}vfmacc ( C{idx}i, alphai, ACC{idx}r, gvl );")

        if not TRMM:
            dest.write()
            dest.write("ci=n_top*ldc+m_top;")
            dest.write()

        for j in range(N):
            for i in range(a_regs):
                idx = j*a_regs+i
                increment = 'ci += ldc-gvl*{};'.format(a_regs-1) if (i == a_regs-1) else ' ci += gvl;'
                if idx == N*a_regs-1:
                    increment = ''                    
                with dest.map(idx=j*a_regs+i, increment=increment):
                    dest.write("{VSSEV}( &C[ci*2+0], sizeof(FLOAT)*2, C{idx}r, gvl);")
                    dest.write("{VSSEV}( &C[ci*2+1], sizeof(FLOAT)*2, C{idx}i, gvl);")
                    dest.write("{increment}")

#-----------------------------------------------------------------------
def generate_gemm_kernel( settings, OUTPUT ):
    if settings['conjugate'].value:
        ERROR('conjugate gemm not yet supported')

    is_complex = settings['complex'].value
    generate_gemm_kernel_inner = generate_gemm_kernel_inner_complex if is_complex else generate_gemm_kernel_inner_real
    dest = Target(OUTPUT, { k:str(settings[k].value) for k in settings })

    M = settings['M'].value
    N = settings['N'].value
    vlenmax = int(settings['reg_width_bits'].value * settings['LMUL_ACC'].value /
                  settings['ELEN_PARAM'].value)
    a_regs = max(int(M/vlenmax), 1)

    # for each vector register loaded from matrix A, we require N registers to hold vector-scalar multiply-accumulate results
    accumulation_regs = a_regs * N
    required_regs = accumulation_regs + a_regs
    if is_complex:
        required_regs = required_regs * 2 + 2
        dest.write('''
#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
    #define S0  1
    #define S1 -1
    #define S2  1
    #define S3  1
    #define VFMACC_RR __riscv_vfmsac{tail_policy}
    #define VFMACC_RI __riscv_vfmacc{tail_policy}
#endif
#if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
    #define S0  1
    #define S1  1
    #define S2  1
    #define S3 -1
    #define VFMACC_RR __riscv_vfmacc{tail_policy}
    #define VFMACC_RI __riscv_vfmsac{tail_policy}
#endif
#if   defined(RN) || defined(RT) || defined(CN) || defined(CT)
    #define S0  1
    #define S1  1
    #define S2 -1
    #define S3  1
    #define VFMACC_RR __riscv_vfmacc{tail_policy}
    #define VFMACC_RI __riscv_vfnmsac{tail_policy}
#endif
#if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
    #define S0  1
    #define S1 -1
    #define S2 -1
    #define S3 -1
    #define VFMACC_RR __riscv_vfmsac{tail_policy}
    #define VFMACC_RI __riscv_vfnmacc{tail_policy}
#endif
'''.format(tail_policy=settings['tail_policy'].value))


    if required_regs > (32 // settings['LMUL_ACC'].value):
        raise Exception("{} vector registers needed during accumulation for unrolling {} x {}{} but only {} are available".format(
            required_regs, N, M, (" with wide accumulator" if settings['LMUL_ACC'].value > 1 else ''), 32 // settings['LMUL_ACC'].value
            ))

    TRMM = (settings['op'].value == 'trmm')
    if TRMM:
        with dest.block("#if defined(LEFT) != defined(TRANSA)", "#endif"):
            dest.write("#define BACKWARDS")

    dest.write("int CNAME(BLASLONG M, BLASLONG N, BLASLONG K, {alpha}, FLOAT* A, FLOAT* B, FLOAT* C, BLASLONG ldc{trmm})",
            alpha = ('FLOAT alphar, FLOAT alphai' if is_complex else 'FLOAT alpha'),
            trmm = (', BLASLONG offset' if TRMM else '')
        )

    with dest.block("{{", "}}", elt_size='*2' if is_complex else ''):
        if settings['trace'].value:
            dest.write("printf(\"\\n\\nENTRY: %s(%d) M %d N %d K %d ldc %d\\n\", __FILE__, __LINE__, M, N, K, ldc);")
        dest.write("{index_type} gvl = 0;")
        dest.write("{index_type} m_top = 0;")
        dest.write("{index_type} n_top = 0;")

        dest.write()
        dest.write()
        dest.write("// -- MAIN PASS")

        with dest.block("for ({index_type} j=0; j<N/{N}; j+=1) {{", "}}"):
            dest.write("m_top = 0;")
            dest.write("{index_type} gvl = {VSETVL}({vlenmax});", vlenmax=min(vlenmax,max(int(M/a_regs),1)))
            dest.write()
            with dest.block("for ({index_type} i=0; i<M/{M}; i+=1) {{", "}}"):
                generate_gemm_kernel_inner( settings, dest, M, N, vlenmax, a_regs )
                dest.write( "m_top += {M};" )

            dest.write()
            dest.write()
            dest.write("// -- tails for main pass")
            generate_M_tails( dest, settings, M, N )

            dest.write( "n_top += {N};" )


        N_tail = int(N/2)
        while( N_tail > 0 ):
            with dest.map(N=N_tail):
                dest.write()
                dest.write()
                dest.write("// -- tails for N={N}")
                with dest.block("if( N & {N} ) {{", "}}" ):
                    if settings['trace'].value:
                        dest.write("printf(\"N tail entry: %s(%d) M %d N %d K %d m_top %d n_top %d\\n\", __FILE__, __LINE__, M, N, K, m_top, n_top);")
                    dest.write("gvl = {VSETVL}({vlenmax});", vlenmax=min(vlenmax,max(int(M/a_regs),1)))
                    dest.write("m_top = 0;")
                    with dest.block("for ({index_type} i=0; i<M/{M}; i+=1) {{", "}}"):
                        generate_gemm_kernel_inner( settings, dest, M, N_tail, vlenmax, a_regs )
                        dest.write("m_top += {M};")

                    generate_M_tails( dest, settings, M, N_tail )
                    dest.write("n_top += {N};")
            N_tail = int(N_tail/2)

        dest.write("return 0;");


#-----------------------------------------------------------------------
def generate_M_tails( dest, settings, M, N ):
    M_tail = int(M/2)
    M_tail_min = settings['M_tail_scalar_from'].value
    vlenmax = int(settings['reg_width_bits'].value * settings['LMUL_ACC'].value
                  / settings['ELEN_PARAM'].value )
    TRMM           = (settings['op'].value == 'trmm')
    is_complex = settings['complex'].value
    generate_gemm_kernel_inner = generate_gemm_kernel_inner_complex if is_complex else generate_gemm_kernel_inner_real

    while( M_tail > M_tail_min ):
        with dest.block("if( M & {M_tail} ) {{", "}}", M_tail=M_tail ):
            if settings['trace'].value:
                dest.write("printf(\"tail: %s(%d) M %d N %d K %d m_top %d n_top %d\\n\", __FILE__, __LINE__, M, N, K, m_top, n_top);")
            a_regs = max( 1, int(M_tail/vlenmax) )
            vlen = int(M_tail/a_regs)
            dest.write("gvl = {VSETVL}({vlen});\n", vlen=vlen)

            generate_gemm_kernel_inner( settings, dest, M_tail, N, vlen, a_regs )
            dest.write( "m_top += {M_tail};" )

        M_tail = int( M_tail / 2 )

    while( M_tail > 0 ):
        with dest.block("if( M & {M_tail} ) {{", "}}", 
                M_tail=M_tail, 
                N=N, 
                result_t = ('double' if settings['force_acc_double'].value else settings['param_scalar_t'].value) 
        ):
            if settings['trace'].value:
                dest.write("printf(\"tail: %s(%d) M %d N %d K %d m_top %d n_top %d\\n\", __FILE__, __LINE__, M, N, K, m_top, n_top);")
            for r in range(M_tail * N * (2 if is_complex else 1)):
                dest.write("{result_t} result{r} = 0;",
                    r=r
                )

            dest.write("{index_type} ai=m_top*K{elt_size};")
            dest.write("{index_type} bi=n_top*K{elt_size};")

            if TRMM:
                with dest.map(M=M_tail, N=N):
                    generate_trmm_block( dest )

            with dest.block("for({index_type} k=0; k<{Kend}; k++) {{", "}}", Kend = ('pass_K' if TRMM else 'K') ):
                for ki in range( N ):
                    for kj in range( M_tail ):
                        if is_complex:
                            dest.write("result{dest}+=S0*A[ai+{kj}+0]*B[bi+{ki}+0] + S1*A[ai+{kj}+1]*B[bi+{ki}+1];".format(
                                        dest=(ki*M_tail+kj)*2, kj=kj*2, ki=ki*2
                                    ))                            
                            dest.write("result{dest}+=S2*A[ai+{kj}+1]*B[bi+{ki}+0] + S3*A[ai+{kj}+0]*B[bi+{ki}+1];".format(
                                        dest=(ki*M_tail+kj)*2+1, kj=kj*2, ki=ki*2
                                    ))                            
                        else:
                            dest.write("result{dest}+=A[ai+{kj}]*B[bi+{ki}];".format(
                                    dest=ki*M_tail+kj, kj=kj, ki=ki
                                ))
                dest.write("ai+={M_tail}{elt_size};")
                dest.write("bi+={N}{elt_size};")

            dest.write("{index_type} ci=n_top*ldc+m_top;")
            if is_complex:
                dest.write("{result_t} Cr, Ci;")
            for ki in range( N ):
                for kj in range( M_tail ):
                    if is_complex:
                        if TRMM:
                            dest.write('Cr = result{dest}*alphar;', dest=(ki*M_tail+kj)*2+0)
                            dest.write('Ci = result{dest}*alphar;', dest=(ki*M_tail+kj)*2+1)
                        else:
                            dest.write('Cr = C[(ci+{ki}*ldc+{kj})*2+0];', ki=ki, kj=kj)
                            dest.write('Ci = C[(ci+{ki}*ldc+{kj})*2+1];', ki=ki, kj=kj)
                            dest.write('Cr += result{dest}*alphar;', dest=(ki*M_tail+kj)*2+0)
                            dest.write('Ci += result{dest}*alphar;', dest=(ki*M_tail+kj)*2+1)
                        dest.write('Cr -= result{dest}*alphai;', dest=(ki*M_tail+kj)*2+1)
                        dest.write('Ci += result{dest}*alphai;', dest=(ki*M_tail+kj)*2+0)
                        dest.write("C[(ci+{ki}*ldc+{kj})*2+0] = Cr;", ki=ki, kj=kj )
                        dest.write("C[(ci+{ki}*ldc+{kj})*2+1] = Ci;", ki=ki, kj=kj )
                    else:
                        op = '' if TRMM else '+'
                        dest.write("C[ci+{ki}*ldc+{kj}] {op}= alpha * result{dest};",
                                ki=ki, kj=kj, op=op, dest=ki*M_tail+kj
                            )
            dest.write("m_top+={M_tail};")

        M_tail = int(M_tail/2)


#-----------------------------------------------------------------------
class Setting(object):
    def __init__( self, value, convert = None ):
        self._value = value
        self._convert = convert

    @classmethod
    def ENUM( cls, *values ):
        def closure( values ):
            return lambda value: values[value.lower()]
        return closure( { v.lower():v for v in values } )

    @classmethod
    def BOOL( cls, value ):
        return value.lower().startswith('t') or value == '1'

    @property
    def value( self ):
        return self._value

    @property
    def configurable( self ):
        return self._convert is not None

    @value.setter
    def value( self, value ):
        self._value = self._convert( value )

    def __str__( self ):
        return str(self._value)

#-----------------------------------------------------------------------
def main():
    settings = {
        'op':               Setting( 'gemm', Setting.ENUM( 'gemm', 'trmm' ) ),
        'M':                Setting( 16, int ),
        'N':                Setting( 4, int ),
        'reg_width_bits':   Setting( 256, int ),
        'LMUL':             Setting( 1, int ),
        'M_tail_scalar_from':Setting( 2, int ),
        'cpu':              Setting( 'zvl256b', str ),
        'param_precision':  Setting( 'float', Setting.ENUM( 'float', 'double' ) ),
        'force_acc_double': Setting( False, Setting.BOOL ),
        'complex':          Setting( False, Setting.BOOL ),
        'conjugate':        Setting( False, Setting.BOOL ),
        'index_type':       Setting( 'BLASLONG', str ),
        'trace':            Setting( False, Setting.BOOL ),
        'output':           Setting( None, str ),
        'tail_policy':      Setting( '', str ), # _ta, if toolchain supports it
        '__riscv_':         Setting( '__riscv_', str),
    }

    for item in sys.argv[1:]:
        try:
            name, value = tuple(item.split( '=', 1 ))
        except:
            ERROR("couldn't parse {}, expected arguments of the form name=value".format(item))

        if name not in settings:
            ERROR("couldn't parse {}, {} it is not a known option\n".format( item, name )
                  +"options (and current defaults) are\n{}".format(
                   " ".join([ '{}={}'.format(k, settings[k].value) for k in settings.keys()]))
                )

        try:
            settings[name].value = value
        except:
            import traceback
            traceback.print_exc()
            ERROR("couldn't parse {}".format(item))

    if settings['output'].value is None:
        if settings['complex'].value:
            prefix = 'z' if settings['param_precision'].value == 'double' else 'c'
        else:
            prefix = 'd' if settings['param_precision'].value == 'double' else 's'
        settings['output'] = Setting('{}{}_kernel_{}x{}_{}.c'.format(
                prefix,
                settings['op'],
                settings['M'],
                settings['N'],
                settings['cpu']
            ))

    if settings['param_precision'].value == 'double':
        settings['param_scalar_t'] = Setting( 'double' )
        settings['ELEN_PARAM'] = Setting(64)
    else:
        settings['param_scalar_t'] = Setting( 'float' )
        settings['ELEN_PARAM'] = Setting(32)

    settings['VFMUL'] = Setting( '{}vfmul_vf_f{}m{}{}'.format(settings['__riscv_'], settings['ELEN_PARAM'], settings['LMUL'], settings['tail_policy']) )
    settings['VFMACC'] = Setting( '{}vfmacc_vf_f{}m{}{}'.format(settings['__riscv_'], settings['ELEN_PARAM'], settings['LMUL'], settings['tail_policy']) )

    settings['ELEN_ACC'] = settings['ELEN_PARAM']
    settings['LMUL_ACC'] = Setting(settings['LMUL'].value)
    widen = ''

    if settings['force_acc_double'].value and (settings['param_precision'].value == 'float'):
        settings['ELEN_ACC'] = Setting(64)
        settings['LMUL_ACC'] = Setting(settings['LMUL'].value*2)
        settings['VFNCVT']   = Setting('{}vfncvt_f_f_w_f{}m{}{}'.format(settings['__riscv_'], settings['ELEN_PARAM'], settings['LMUL'], settings['tail_policy']))
        widen = 'w'

    settings['VMUL_TO_ACC'] = Setting( '{}vf{}mul_vf_f{}m{}{}'.format(settings['__riscv_'], widen, settings['ELEN_ACC'], settings['LMUL_ACC'], settings['tail_policy']) )
    settings['VMACC_TO_ACC'] = Setting( '{}vf{}macc_vf_f{}m{}{}'.format(settings['__riscv_'], widen, settings['ELEN_ACC'], settings['LMUL_ACC'], settings['tail_policy']) )

    settings['param_vector_t']=Setting('vfloat{}m{}_t'.format(settings['ELEN_PARAM'], settings['LMUL']))
    settings['acc_vector_t']  =Setting('vfloat{}m{}_t'.format(settings['ELEN_ACC'], settings['LMUL_ACC']))
    settings['VLEV']          =Setting('{}vle{}_v_f{}m{}'.format(settings['__riscv_'], settings['ELEN_PARAM'], settings['ELEN_PARAM'], settings['LMUL']))
    settings['VSEV']          =Setting('{}vse{}_v_f{}m{}'.format(settings['__riscv_'], settings['ELEN_PARAM'], settings['ELEN_PARAM'], settings['LMUL']))
    settings['VLSEV']         =Setting('{}vlse{}_v_f{}m{}'.format(settings['__riscv_'], settings['ELEN_PARAM'], settings['ELEN_PARAM'], settings['LMUL']))
    settings['VSSEV']         =Setting('{}vsse{}_v_f{}m{}'.format(settings['__riscv_'], settings['ELEN_PARAM'], settings['ELEN_PARAM'], settings['LMUL']))
    settings['VSETVL']        =Setting('{}vsetvl_e{}m{}'.format(settings['__riscv_'], settings['ELEN_PARAM'], settings['LMUL']))


    to_stdout = (settings['output'].value == '-')
    if not to_stdout:
        print("Writing {}".format(settings['output'].value), file=sys.stderr)

    with open(sys.stdout.fileno() if to_stdout else settings['output'].value, 'w') as destination_file:
        def OUTPUT(*args, **kwargs):
            print(*args, file=destination_file, **kwargs)

        OUTPUT("/*\n\nAUTOGENERATED KERNEL\nSettings:\n {}".format(" ".join([ "{}={}\n".format(k, repr(settings[k].value)) for k in sorted(settings.keys()) if settings[k].configurable])))
        OUTPUT("Derived:\n {}\n*/\n".format(" ".join([ "{}={}\n".format(k, repr(settings[k].value)) for k in sorted(settings.keys()) if not settings[k].configurable])))

        OUTPUT('#include "common.h"')
        OUTPUT("\n")

        if settings['op'].value in ('gemm', 'trmm'):
            generate_gemm_kernel(settings, OUTPUT)
        else:
            ERROR("unsupported kernel type {}".format(settings['op']))

if __name__ == "__main__":
    main()
