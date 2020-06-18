from dash import callback_context
from .utils import sync_index, get_nth_chunk


class RowOptionsSelected:
    """
    A convenience wrapper for the 3/4 values from a row of the user drop-downs.
    In each case the first three values are the indices of the
    * field name (i.e. `.item` of the UserOption)
    * transformation (i.e. `.set_transform` of the UserOption)
    * aggregation (i.e. `.set_aggregation` of the UserOption)
    For the secondary fields, a final option of `Name` (i.e. `.perform_lkp` of UserOption)
    is available too.

    Constructors:
    * RowOptionsSelected(3) -- create empty object with 3 values
    * RowOptionsSelected(0,2,1 [,True]) -- create object with 3[4] specified valuaes.

    This class allows named values for easier-to-read code.
    """
    def __init__(self, *args):
        self.n = len(args)
        if self.n == 1:
            assert args[0] in (3,4), "Expecting 3 or 4 arguments for RowOptionsSelected"
            self.n = args[0]
            self.item_id = None
            self.trans_id = None
            self.agg_id = None
            self.perform_lkp = None
            return

        assert self.n in (3,4), "Expecting 3 or 4 arguments for RowOptionsSelected"
        self.item_id = args[0]
        self.trans_id = args[1]
        self.agg_id = args[2]
        if len(args) == 4:
            if isinstance(args[3], bool):
                self.perform_lkp = args[3]
            elif isinstance(args[3], list):
                self.perform_lkp = len(args[3]) > 0
            else:
                raise RuntimeError(f"Unexpected args[4] type: {type(args[3])}")

    @property
    def has_name_flag(self):
        return self.n == 4

    def to_list(self):
        if not self.has_name_flag:
            return [self.item_id, self.trans_id, self.agg_id]
        else:
            return [self.item_id, self.trans_id, self.agg_id, self.perform_lkp]


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
        selected = RowOptionsSelected(*c_args)
        # Get the field that the user has selected (dropdown column 1)
        if i == 0:
            opt = primary_fields[selected.item_id].copy()
        elif selected.item_id is None:
            continue   # user has [x] the current item and hit submit without selecting.
        elif selected.item_id > 0:
            opt = secondary_fields[selected.item_id - 1].copy()
        else:
            continue

        # Get the transformation/aggregation selected in dropdown columns 2/3.
        # -------------------------------------------------------------------
        # try/except: might be out of range if dropdowns have changed, and hence None.
        try:
            t = opt.transformations[selected.trans_id]
            opt.set_transform(t)
        except (IndexError, TypeError):
            opt.set_transform(None)
        try:
            a = opt.aggregations[selected.agg_id]
            opt.set_aggregation(a)
        except (IndexError, TypeError):
            opt.set_aggregation(None)

        # Get the 'name' flag in column 4 (n.b. not available in row 1.)
        if i > 0:
            opt.perform_lkp = selected.perform_lkp

        use_opts.append(opt)

    debug = "\n".join(debug)
    return use_opts, debug


def find_in_item_names(query, user_opts, error_about='list'):
    try:
        return [x.item.lower() for x in user_opts].index(query)
    except ValueError as e:
        raise ValueError(f'Standard query: Cannot find in {error_about}: {query}')


def get_trigger(default=None):
    # what called the function?
    ctx = callback_context
    if not ctx.triggered:
        trigger_id = default
    else:
        trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
    return trigger_id