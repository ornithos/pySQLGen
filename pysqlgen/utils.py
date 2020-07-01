import re
import time
from functools import reduce
from collections import OrderedDict


def node_isin_context(x, context, allow_custom=False, allow_None=False):
    if x in context.nodes:
        return True
    elif allow_None and x is None:
        return True
    elif allow_custom and isinstance(x, str) and x.lower() == 'custom':
        return True
    else:
        return False


def in_list(x, L, allow_None=False):
    if allow_None and x is None:
        return True
    return x in L


def str_to_fieldname(x):
    name_rep = x.strip().replace(' ', '_').replace('(', '_').replace(')', '_')
    return re.sub("(__+)", "_", name_rep).rstrip('_')


def make_unique_name(x, *existing_names):
    x = x.lower()
    name = x[:1]
    a_ix = 0
    for cur in existing_names:
        cur = cur.values() if isinstance(cur, dict) else cur
        while name in cur:
            a_str = '' if a_ix == 0 else str(a_ix)
            a_ix += 1
            name = ''.join([n[0].lower() for n in x.split("_")]) + a_str
    return name


def sync_index(i, cur_list, dest_list, None_is_str=False):
    cur_item = cur_list[i]
    cur_item = None if (None_is_str and (cur_item is None)) else cur_item
    return dest_list.index(cur_item)


def flatten(l):
    return [item for sublist in l for item in sublist]


def ilen(iterable):
    return reduce(lambda sum, element: sum + 1, iterable, 0)


def not_none(*args):
    return next((el for el in args if el is not None), None)


def get_nth_chunk(n, indexable, chunksizes):
    chunk_start = sum(chunksizes[:n])
    chunk_end = chunk_start + chunksizes[n]
    return indexable[chunk_start:chunk_end]


def rm_alias_placeholder(x):
    return re.sub('{alias(:s)?}', '', x)


def cur_time_ms():
    return int(round(time.time() * 1000))


def replace_in_ordered_dict(od, replace_key, key, value):
    """
    Overwrite a key in an OrderedDict while maintaining the original order.
    This function will insert the (key, value) pair into the current position where
    the `replace_key` sits.
    """
    return OrderedDict((key, value) if k == replace_key else (k, v)
                       for k, v in od.items())
