import numpy as np
import numba
from itertools import chain, zip_longest


def tree_reduce(func, iterable):
    maybereduce = lambda a,b: func(a,b) if b is not None else a
    it = iter(iterable)
    pairs = zip_longest(it, it)
    peek = next(pairs)
    if peek[1] is None:
        return peek[0]
    resume = (maybereduce(a,b) for a,b in chain([peek], pairs))
    peek = None
    return tree_reduce(func, resume)


@numba.njit
def set_union(a, b):
    if a.size == 0:
        return b
    elif b.size == 0:
        return a
    out = np.empty(a.size+b.size, dtype=a.dtype)
    ia = 0
    ib = 0
    iout = 0
    while ia < a.size and ib < b.size:
        if a[ia] < b[ib]:
            out[iout] = a[ia]
            iout += 1
            ia += 1
        elif a[ia] == b[ib]:
            out[iout] = a[ia]
            iout += 1
            ia += 1
            ib += 1
        elif a[ia] > b[ib]:
            out[iout] = b[ib]
            iout += 1
            ib += 1
    while ia < a.size:
        out[iout] = a[ia]
        iout += 1
        ia += 1
    while ib < b.size:
        out[iout] = b[ib]
        iout += 1
        ib += 1
    return out[:iout]


@numba.njit
def set_difference(a, b):
    if a.size == 0 or b.size == 0:
        return a
    out = np.empty(a.size, dtype=a.dtype)
    ia = 0
    ib = 0
    iout = 0
    while ia < a.size and ib < b.size:
        if a[ia] < b[ib]:
            out[iout] = a[ia]
            iout += 1
            ia += 1
        elif a[ia] == b[ib]:
            ia += 1
            ib += 1
        elif a[ia] > b[ib]:
            ib += 1
    while ia < a.size:
        out[iout] = a[ia]
        iout += 1
        ia += 1
    return out[:iout]


@numba.njit
def set_intersection(a, b):
    if a.size == 0:
        return a
    elif b.size == 0:
        return b
    out = np.empty(np.minimum(a.size, b.size), dtype=a.dtype)
    ia = 0
    ib = 0
    iout = 0
    while ia < a.size and ib < b.size:
        if a[ia] < b[ib]:
            ia += 1
        elif a[ia] == b[ib]:
            out[iout] = a[ia]
            iout += 1
            ia += 1
            ib += 1
        elif a[ia] > b[ib]:
            ib += 1
    return out[:iout]


class FastSet(object):
    def __init__(self, set_in=None, dtype='i8'):
        if set_in is None:
            self._set = np.array([], dtype=dtype)
        elif isinstance(set_in, FastSet):
            self._set = set_in._set.astype(dtype)
        else:
            self._set = np.fromiter(set_in, dtype=dtype, count=len(set_in))
            self._set.sort()

    def __len__(self):
        return self._set.size

    def __str__(self):
        return str(self._set)

    def __iter__(self):
        return iter(self._set)

    def union(self, other):
        sout = FastSet(dtype=self._set.dtype)
        sout._set = set_union(self._set, other._set)
        return sout

    def difference(self, other):
        sout = FastSet(dtype=self._set.dtype)
        sout._set = set_difference(self._set, other._set)
        return sout

    def intersection(self, other):
        sout = FastSet(dtype=self._set.dtype)
        sout._set = set_intersection(self._set, other._set)
        return sout

    def __add__(self, other):
        return self.union(other)

    def __sub__(self, other):
        return self.difference(other)

    def __mul__(self, other):
        return self.intersection(other)

    def set(self):
        return set(self._set)
