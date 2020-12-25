from eth_utils import add_0x_prefix, is_hexstr

from typing import List, Tuple


def is_256bit_hash(v):
    if not is_hexstr(v):
        return False

    v = add_0x_prefix(v)

    return len(v) == 66


def index(iter, val):
    """ Find the index in an iterable for a value """
    for i, x in enumerate(iter):
        if x == val:
            return i
    return -1


def indexf(iter, func):
    """ Find the index in an iterable for a value """
    for i, x in enumerate(iter):
        if func(x):
            return i
    return -1


def del_list(_list, val):
    idx = index(_list, val)
    del _list[idx]


def del_listf(_list, func):
    idx = indexf(_list, func)
    del _list[idx]


def validate_conditions(conds: List[Tuple[bool, str]]) -> Tuple[bool, List[str]]:
    """ validate a list of conditions.  If they're not True, return the given
    error provided with the condition.
    """
    errors = []

    if not all([x for x, _ in conds]):
        for cond in conds:
            if cond[0] is not True:
                errors.append(cond[1])

        return False, errors

    return True, errors
