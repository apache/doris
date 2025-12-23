import pytest
import numpy as np
import openblas_wrap as ow

dtype_map = {
    's': np.float32,
    'd': np.float64,
    'c': np.complex64,
    'z': np.complex128,
    'dz': np.complex128,
}


# ### BLAS level 1 ###

# dnrm2

dnrm2_sizes = [100, 1000]

def run_dnrm2(n, x, incx, func):
    res = func(x, n, incx=incx)
    return res


@pytest.mark.parametrize('variant', ['d', 'dz'])
@pytest.mark.parametrize('n', dnrm2_sizes)
def test_nrm2(benchmark, n, variant):
    rndm = np.random.RandomState(1234)
    dtyp = dtype_map[variant]

    x = np.array(rndm.uniform(size=(n,)), dtype=dtyp)
    nrm2 = ow.get_func('nrm2', variant)
    result = benchmark(run_dnrm2, n, x, 1, nrm2)


# ddot

ddot_sizes = [100, 1000]

def run_ddot(x, y, func):
    res = func(x, y)
    return res


@pytest.mark.parametrize('n', ddot_sizes)
def test_dot(benchmark, n):
    rndm = np.random.RandomState(1234)

    x = np.array(rndm.uniform(size=(n,)), dtype=float)
    y = np.array(rndm.uniform(size=(n,)), dtype=float)
    dot = ow.get_func('dot', 'd')
    result = benchmark(run_ddot, x, y, dot)


# daxpy

daxpy_sizes = [100, 1000]

def run_daxpy(x, y, func):
    res = func(x, y, a=2.0)
    return res


@pytest.mark.parametrize('variant', ['s', 'd', 'c', 'z'])
@pytest.mark.parametrize('n', daxpy_sizes)
def test_daxpy(benchmark, n, variant):
    rndm = np.random.RandomState(1234)
    dtyp = dtype_map[variant]

    x = np.array(rndm.uniform(size=(n,)), dtype=dtyp)
    y = np.array(rndm.uniform(size=(n,)), dtype=dtyp)
    axpy = ow.get_func('axpy', variant)
    result = benchmark(run_daxpy, x, y, axpy)


# ### BLAS level 2 ###

gemv_sizes = [100, 1000]

def run_gemv(a, x, y, func):
    res = func(1.0, a, x, y=y, overwrite_y=True)
    return res


@pytest.mark.parametrize('variant', ['s', 'd', 'c', 'z'])
@pytest.mark.parametrize('n', gemv_sizes)
def test_dgemv(benchmark, n, variant):
    rndm = np.random.RandomState(1234)
    dtyp = dtype_map[variant]

    x = np.array(rndm.uniform(size=(n,)), dtype=dtyp)
    y = np.empty(n, dtype=dtyp)

    a = np.array(rndm.uniform(size=(n,n)), dtype=dtyp)
    x = np.array(rndm.uniform(size=(n,)), dtype=dtyp)
    y = np.zeros(n, dtype=dtyp)

    gemv = ow.get_func('gemv', variant)
    result = benchmark(run_gemv, a, x, y, gemv)

    assert result is y


# dgbmv

dgbmv_sizes = [100, 1000]

def run_gbmv(m, n, kl, ku, a, x, y, func):
    res = func(m, n, kl, ku, 1.0, a, x, y=y, overwrite_y=True)
    return res



@pytest.mark.parametrize('variant', ['s', 'd', 'c', 'z'])
@pytest.mark.parametrize('n', dgbmv_sizes)
@pytest.mark.parametrize('kl', [1])
def test_dgbmv(benchmark, n, kl, variant):
    rndm = np.random.RandomState(1234)
    dtyp = dtype_map[variant]

    x = np.array(rndm.uniform(size=(n,)), dtype=dtyp)
    y = np.empty(n, dtype=dtyp)

    m = n

    a = rndm.uniform(size=(2*kl + 1, n))
    a = np.array(a, dtype=dtyp, order='F')

    gbmv = ow.get_func('gbmv', variant)
    result = benchmark(run_gbmv, m, n, kl, kl, a, x, y, gbmv)
    assert result is y


# ### BLAS level 3 ###

# dgemm

gemm_sizes = [100, 1000]

def run_gemm(a, b, c, func):
    alpha = 1.0
    res = func(alpha, a, b, c=c, overwrite_c=True)
    return res


@pytest.mark.parametrize('variant', ['s', 'd', 'c', 'z'])
@pytest.mark.parametrize('n', gemm_sizes)
def test_gemm(benchmark, n, variant):
    rndm = np.random.RandomState(1234)
    dtyp = dtype_map[variant]
    a = np.array(rndm.uniform(size=(n, n)), dtype=dtyp, order='F')
    b = np.array(rndm.uniform(size=(n, n)), dtype=dtyp, order='F')
    c = np.empty((n, n), dtype=dtyp, order='F')
    gemm = ow.get_func('gemm', variant)
    result = benchmark(run_gemm, a, b, c, gemm)
    assert result is c


# dsyrk

syrk_sizes = [100, 1000]


def run_syrk(a, c, func):
    res = func(1.0, a, c=c, overwrite_c=True)
    return res


@pytest.mark.parametrize('variant', ['s', 'd', 'c', 'z'])
@pytest.mark.parametrize('n', syrk_sizes)
def test_syrk(benchmark, n, variant):
    rndm = np.random.RandomState(1234)
    dtyp = dtype_map[variant]
    a = np.array(rndm.uniform(size=(n, n)), dtype=dtyp, order='F')
    c = np.empty((n, n), dtype=dtyp, order='F')
    syrk = ow.get_func('syrk', variant)
    result = benchmark(run_syrk, a, c, syrk)
    assert result is c


# ### LAPACK ###

# linalg.solve

gesv_sizes = [100, 1000]


def run_gesv(a, b, func):
    res = func(a, b, overwrite_a=True, overwrite_b=True)
    return res


@pytest.mark.parametrize('variant', ['s', 'd', 'c', 'z'])
@pytest.mark.parametrize('n', gesv_sizes)
def test_gesv(benchmark, n, variant):
    rndm = np.random.RandomState(1234)
    dtyp = dtype_map[variant]

    a = (np.array(rndm.uniform(size=(n, n)), dtype=dtyp, order='F') +
         np.eye(n, dtype=dtyp, order='F'))
    b = np.array(rndm.uniform(size=(n, 1)), dtype=dtyp, order='F')
    gesv = ow.get_func('gesv', variant)
    lu, piv, x, info = benchmark(run_gesv, a, b, gesv)
    assert lu is a
    assert x is b
    assert info == 0


# linalg.svd

gesdd_sizes = [(100, 5), (1000, 222)]


def run_gesdd(a, lwork, func):
    res = func(a, lwork=lwork, full_matrices=False, overwrite_a=False)
    return res


@pytest.mark.parametrize('variant', ['s', 'd'])
@pytest.mark.parametrize('mn', gesdd_sizes)
def test_gesdd(benchmark, mn, variant):
    m, n = mn
    rndm = np.random.RandomState(1234)
    dtyp = dtype_map[variant]

    a = np.array(rndm.uniform(size=(m, n)), dtype=dtyp, order='F')

    gesdd_lwork = ow.get_func('gesdd_lwork', variant)

    lwork, info = gesdd_lwork(m, n)
    lwork = int(lwork)
    assert info == 0

    gesdd = ow.get_func('gesdd', variant)
    u, s, vt, info = benchmark(run_gesdd, a, lwork, gesdd)

    assert info == 0

    atol = {'s': 1e-5, 'd': 1e-13}
    np.testing.assert_allclose(u @ np.diag(s) @ vt, a, atol=atol[variant])


# linalg.eigh

syev_sizes = [50, 200]


def run_syev(a, lwork, func):
    res = func(a, lwork=lwork, overwrite_a=True)
    return res


@pytest.mark.parametrize('variant', ['s', 'd'])
@pytest.mark.parametrize('n', syev_sizes)
def test_syev(benchmark, n, variant):
    rndm = np.random.RandomState(1234)
    dtyp = dtype_map[variant]

    a = rndm.uniform(size=(n, n))
    a = np.asarray(a + a.T, dtype=dtyp, order='F')
    a_ = a.copy()

    dsyev_lwork = ow.get_func('syev_lwork', variant)
    lwork, info = dsyev_lwork(n)
    lwork = int(lwork)
    assert info == 0

    syev = ow.get_func('syev', variant)
    w, v, info = benchmark(run_syev, a, lwork, syev)

    assert info == 0
    assert a is v  # overwrite_a=True


