"""
Trampoline to hide the LAPACK details (scipy.lapack.linalg or scipy_openblas32 or...)
from benchmarking.
"""

__version__ = "0.1"  


from . import _flapack

PREFIX = ''


def get_func(name, variant):
    """get_func('gesv', 'c') -> cgesv etc."""
    return getattr(_flapack, PREFIX + variant + name)

