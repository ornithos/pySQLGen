from .dbtree import SchemaNode


def is_node(x, allow_custom=False):
    if isinstance(x, SchemaNode):
        return True
    elif allow_custom and isinstance(x, str):
        return x.lower() == "custom"
    else:
        return False


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
    return x.strip().replace(' ', '_')


def sync_index(i, cur_list, dest_list, None_is_str=False):
    cur_item = cur_list[i]
    cur_item = None if (None_is_str and (cur_item is None)) else cur_item
    return dest_list.index(cur_item)


def get_nth_chunk(n, indexable, chunksizes):
    chunk_start = sum(chunksizes[:n])
    chunk_end = chunk_start + chunksizes[n]
    return indexable[chunk_start:chunk_end]
