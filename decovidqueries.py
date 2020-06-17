import json
from collections import OrderedDict
from pysqlgen.apputils import find_in_item_names, RowOptionsSelected, app_state_to_opts
import decovid


standard_queries_json = """
{
  "Distribution of Sex": [
    ["Person", null, "count"],
    ["Sex", null, null, true]
  ],
  "Distribution of Age": [
    ["Person", null, "count"],
    ["Age", "tens", null, false]
  ]
}
"""

standard_queries = json.JSONDecoder(object_pairs_hook=OrderedDict).decode(
    standard_queries_json)


def get_query_from_index(i, queries):
    q_txt = list(queries.keys())[i]
    return standard_queries[q_txt]


def standard_query_to_panel_indices(query, primary_opts, secondary_opts,
                                    secondary_appends_none=True, as_obj=False):
    # primary
    primary_row = RowOptionsSelected(3)
    ix = 0
    primary_row.item_id = find_in_item_names(query[ix][0].lower(), primary_opts,
                                             error_about="primary options")
    chosen_field = primary_opts[primary_row.item_id]
    primary_row.trans_id = chosen_field.transformations.index(query[ix][1])
    primary_row.agg_id = chosen_field.aggregations.index(query[ix][2])
    out = primary_row.to_list()
    out_obj = [primary_row]

    # secondary
    num_secondary = len(query) - 1
    for i in range(num_secondary):
        ix = i + 1
        secondary_row = RowOptionsSelected(4)
        secondary_row.item_id = find_in_item_names(query[ix][0].lower(),
                                                   secondary_opts,
                                                   error_about="secondary options")
        chosen_field = secondary_opts[secondary_row.item_id]
        if secondary_appends_none:
            # If UI appends an additional <None> field to possible fieldnames.
            # Note that we must therefore increment *AFTER* we've selected from opts above
            secondary_row.item_id += 1
        secondary_row.trans_id = chosen_field.transformations.index(query[ix][1])
        secondary_row.agg_id = chosen_field.aggregations.index(query[ix][2])
        secondary_row.perform_lkp = query[ix][3]
        out.extend(secondary_row.to_list())
        out_obj.append(secondary_row)

    return out_obj if as_obj else out


def standard_query_to_opts(query, primary_opts, secondary_opts):
    indices = standard_query_to_panel_indices(query, primary_opts, secondary_opts)
    return app_state_to_opts(indices, primary_opts, secondary_opts)


# print(standard_query_to_panel_indices(standard_queries['Distribution of Age'],
#                                 decovid.opts_primary, decovid.opts_split))
#
# print(standard_query_to_opts(standard_queries['Distribution of Age'],
#                                 decovid.opts_primary, decovid.opts_split)[0])
