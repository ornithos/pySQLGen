from .utils import sync_index, get_nth_chunk


def app_state_to_opts(args, primary_fields, secondary_fields):
    assert (len(args) - 3) % 4 == 0, "Expecting 3+4n args (n>0)"
    num_secondary = (len(args) - 3) // 4
    use_opts = []
    debug = []
    chunksizes = [3, *([4] * num_secondary)]
    for i in range(num_secondary + 1):
        # Get i'th row of indices selected from the UI
        c_args = get_nth_chunk(i, args, chunksizes)
        debug.append(", ".join([str(x) for x in c_args]))
        c_ix = c_args[0]

        # Get the field that the user has selected (dropdown column 1)
        if i == 0:
            opt = primary_fields[c_ix].copy()
        elif c_ix > 0:
            opt = secondary_fields[c_ix - 1].copy()
        else:
            continue

        # Get the transformation/aggregation selected in dropdown columns 2/3.
        # -------------------------------------------------------------------
        # try/except: might be out of range if dropdowns have changed, and hence None.
        t_val, agg_val = c_args[1], c_args[2]
        try:
            t = opt.transformations[t_val]
            opt.set_transform(t)
        except (IndexError, TypeError):
            opt.set_transform(None)
        try:
            a = opt.aggregations[agg_val]
            opt.set_aggregation(a)
        except (IndexError, TypeError):
            opt.set_aggregation(None)

        # Get the 'name' flag in column 4 (n.b. not available in row 1.)
        if i > 0:
            use_name = len(c_args[3]) > 0  # checklist returns list of selected values.
            opt.perform_lkp = use_name

        use_opts.append(opt)

    debug = "\n".join(debug)
    return use_opts, debug
