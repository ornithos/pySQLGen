import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from pysqlgen.apputils import app_state_to_opts, RowOptionsSelected
from pysqlgen.utils import not_none

import decovid
from decovidqueries import standard_queries, standard_query_to_panel_indices, \
    standard_query_to_opts, get_query_from_index

# --------- "GLOBALS" -------------------------------------------------
main_text_style = {'text-align': 'center', 'max-width': '800px', 'margin': 'auto'}
tab_header_text_style = {'text-align': 'center', 'max-width': '800px', 'margin': 'auto',
                         'font-size': '14px'}
lhs_text_style = {'text-align': 'left', 'max-width': '800px', 'margin': 'auto'}
rhs_text_style = {'text-align': 'right', 'max-width': '800px', 'margin': 'auto'}
main_div_style = {'margin':'auto', 'padding-left': '100px', 'padding-right': '100px',
                  'padding-top':'20px', 'max-width': '1100px'}


# --------- DATA ------------------------------------------------------
primary_fields = decovid.opts_primary
primary_fields[0].set_aggregation('count')
secondary_fields = decovid.opts_split
debug_ui = True
print("BEGIN")

# --------- DEFINE INPUT ----------------------------------------------
dropdown_sQuery = dcc.Dropdown(
            id='dropdown-squery',
            options=[{'label': k, 'value': i}
                     for i, (k, v) in enumerate(standard_queries.items())],
            style={'font-size': '13px'}, value=0)
dropdown_Primary = dcc.Dropdown(
            id='dropdown-primary',
            options=[{'label': opt.item, 'value': i}
                     for i, opt in enumerate(primary_fields)],
            style={'font-size': '13px'}, value=0)
dropdown_Primary_trans = dcc.Dropdown(
            id='dropdown-primary-trans',
            options=[
                {'label': '<None>', 'value': 0}
            ], style={'font-size': '13px'}, value=0)
dropdown_Primary_agg = dcc.Dropdown(
            id='dropdown-primary-agg',
            options=[
                {'label': '<None>', 'value': 0},
                {'label': 'Count', 'value': 1}
            ], style={'font-size': '13px'}, value=0)


secondary_var_options = [{'label': opt.item, 'value': i} if i > 0 else
                         {'label': '<None>', 'value': 0} for i, opt in
                         enumerate(secondary_fields)]

def construct_dropdowns(id, opts):
    opts = [None, *opts]
    return dcc.Dropdown(id=id,
                        options=secondary_var_options,
                        style={'font-size': '13px'}, value=0), \
           dcc.Dropdown(id=id+'-trans',
                        options=[{'label': '<None>', 'value': 0}],
                        style={'font-size': '13px'}, value=None), \
           dcc.Dropdown(id=id+'-agg',
                        options=[{'label': '<None>', 'value': 0}],
                        style={'font-size': '13px'}, value=None)


def construct_checkbox(id):
    return dcc.Checklist(id=id,
                         options=[{'label': '', 'value': 1}], value=[])


num_secondary = 4
secondary_dropdown_div = []
for i in range(num_secondary):
    # create a row of secondary variable dropdowns
    dropdowns = construct_dropdowns('dropdown-' + str(i), secondary_fields)
    secondary_dropdown_div.append(
        html.Div([
                html.Div(dropdowns[0], className="four columns"),
                html.Div(dropdowns[1], className="three columns"),
                html.Div(dropdowns[2], className="three columns"),
                html.Div(construct_checkbox(f'check-{i}'), className="one column")
            ], className="row", style={'padding-bottom': '15px'})
    )

# --------- COPY ------------------------------------------------------

introduction = '''
### SQL Generation for OMOP Data

For the purposes of this demonstration, we will assume one wants to aggregate some
features of EHR data to a person level from an OMOP standard database.

See the drop-down menus on the left to select your query, 
'''



# --------- CONSTRUCT APP ---------------------------------------------
# external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__) #, external_stylesheets=external_stylesheets)
server = app.server

custom_space = lambda x: html.Div([html.Br()], style={'line-height': f'{x}%'})

app.layout = html.Div([
    dcc.Markdown(children=introduction, style=main_text_style, className="row"),
    html.Br(),
    html.Div([
        html.Div([
                html.Div([
                    dcc.Markdown("**Standard query**:", style=tab_header_text_style,
                                 className="four columns"),
                    html.Div(dropdown_sQuery, className="six columns"),
                ], className="row"),
                html.Br(),
                html.Button(id='submit-button-standard', n_clicks=0,
                            children='Submit', className="four offset-by-four columns")
            ], className="row", style={'background-color': '#EEEEEE', 'padding': '10px'}
        ),
        html.Br(),
        html.Div([
            html.Div([
                dcc.Markdown("&nbsp;&nbsp;**Customise**:"),
                html.Br(),
                dcc.Markdown("Primary variable:", style=tab_header_text_style,
                             className="four columns"),
                dcc.Markdown("Transform:", style=tab_header_text_style,
                             className="three columns"),
                dcc.Markdown("Aggregation:", style=tab_header_text_style,
                             className="four columns"),
                dcc.Markdown("Name:", style=tab_header_text_style,
                             className="one column")
            ], className="row"),
            custom_space(30),
            html.Div([
                html.Div(dropdown_Primary, className="four columns"),
                html.Div(dropdown_Primary_trans, className="three columns"),
                html.Div(dropdown_Primary_agg, className="three columns"),
            ], className="row"),
            html.Br(),
            html.Div([
                dcc.Markdown("Secondary variables:", style=tab_header_text_style,
                             className="four columns"),
            ], className="row"),
            custom_space(30),
            *secondary_dropdown_div,
            html.Br(),
            html.Button(id='submit-button', n_clicks=0, children='Submit'),
        ], style={'background-color': '#EEEEEE', 'padding': '10px'})
    ], className="four columns"),
    html.Div([
        html.Div(id='sql-output-container')
        ], className="six columns", style={'border': 'solid #CCCCCC 1px',
                                           'padding': '10px'})

])


# --------- REACTIVE -------------------------------------------------

###############################################
# Click *either* "Submit" button to generate SQL
###############################################
# (note that each output may currently have a max
#  of ONE function to change it, and hence must
#  share the same function if needed :( )
all_states = [State('dropdown-primary', 'value'),
               State('dropdown-primary-trans', 'value'),
               State('dropdown-primary-agg', 'value')]
for i in range(num_secondary):
    all_states.extend([State(f'dropdown-{i}', 'value'),
                   State(f'dropdown-{i}-trans', 'value'),
                   State(f'dropdown-{i}-agg', 'value'),
                   State(f'check-{i}', 'value')])
all_states.append(State(f'dropdown-squery', 'value'))


@app.callback(Output('sql-output-container', 'children'),
              [Input('submit-button', 'n_clicks'),
               Input('submit-button-standard', 'n_clicks')],
              all_states)
def update_output(n_clicks1, n_clicks2, *args):

    # which button called the function?
    ctx = dash.callback_context
    if not ctx.triggered:
        button_id = 'submit-button-standard'
    else:
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    if button_id == 'submit-button-standard':
        query = get_query_from_index(args[-1], standard_queries)
        use_opts, dbg_str = standard_query_to_opts(query, primary_fields,
                                                   secondary_fields)
    else:
        print(args)
        use_opts, dbg_str = app_state_to_opts(args[:-1], primary_fields, secondary_fields)

    print(use_opts)
    if len(use_opts) > 0:
        print(f"Create query with {len(use_opts)} fields selected")
        sql = decovid.construct_query(*use_opts)
    else:
        sql = "\n\n~~~~ NO VARIABLES SELECTED ~~~~~\n\n"

    if debug_ui:
        sql += '\n\n\n' + dbg_str
    return html.Pre(sql)


##########################################
# Update dropdowns based on selected field
##########################################
# def _generate_update_dd(trans_or_agg, fields, has_none_field=False):
#     # below, val is the *index* of the selected variable in the LHS dropdown
#     def update_dd_trans(val):
#         if val is None or \
#                 (has_none_field and val == 0):
#             # Clear dropdowns if:
#             # * User has cleared the Field dropdown using [x]
#             # * No variable is selected via the <None> field.
#             # in this context, we should show nothing / blank out dropdown.
#             return [{'label': '<None>', 'value': 0}], -1, True
#         else:
#             # val-1 if <none> field exists, o.w. val
#             val = val -1 if has_none_field else val
#             opt = fields[val]
#             if trans_or_agg == 'transformation':
#                 options_list = opt.transformations
#                 value = options_list.index(opt.default_transformation)
#             elif trans_or_agg == 'aggregation':
#                 options_list = opt.aggregations
#                 value = options_list.index(opt.default_aggregation)
#             else:
#                 raise RuntimeError(f'Unknown drop-down type: {trans_or_agg}')
#             print(options_list)
#             print(value)
#             disable = True if ((len(options_list) == 1) and (options_list[0] is None)) \
#                 else False
#
#             return ([{'label': t, 'value': i} if t is not None else
#                     {'label': '<None>', 'value': i} for i, t in enumerate(options_list)],
#                     value, disable)
#
#     return update_dd_trans
#
#
# def _generate_update_check(fields, has_none_field=False):
#     # below, val is the *index* of the selected variable in the LHS dropdown
#     def update_check(val):
#         if val is None or \
#                 (has_none_field and val == 0):
#             # Clear dropdowns if:
#             # * User has cleared the Field dropdown using [x]
#             # * No variable is selected via the <None> field.
#             # in this context, we should show nothing / blank out dropdown.
#             return [{'label': '', 'value': 1, 'disabled': True}]
#         else:
#             # val-1 if <none> field exists, o.w. val
#             val = val -1 if has_none_field else val
#             if fields[val].has_dim_lkp:
#                 return [{'label': '', 'value': 1, 'disabled': False}]
#             else:
#                 return [{'label': '', 'value': 1, 'disabled': True}]
#     return update_check
#
# ################################################################################
# # Update secondary transformations
# for i in range(num_secondary):
#     update_dd_trans = _generate_update_dd('transformation', secondary_fields,
#                                           has_none_field=True)
#     app.callback([Output(f'dropdown-{i}-trans', 'options'),
#                   Output(f'dropdown-{i}-trans', 'value'),
#                   Output(f'dropdown-{i}-trans', 'disabled')],
#                  [Input(f'dropdown-{i}', 'value')])(update_dd_trans)
#
# # Update secondary aggregations
# for i in range(num_secondary):
#     update_dd_agg = _generate_update_dd('aggregation', secondary_fields,
#                                         has_none_field=True)
#     app.callback([Output(f'dropdown-{i}-agg', 'options'),
#                   Output(f'dropdown-{i}-agg', 'value'),
#                   Output(f'dropdown-{i}-agg', 'disabled')],
#                  [Input(f'dropdown-{i}', 'value')])(update_dd_agg)
#
# # Update secondary checklists
# for i in range(num_secondary):
#     update_chk = _generate_update_check(secondary_fields, has_none_field=True)
#     app.callback(Output(f'check-{i}', 'options'),
#                  [Input(f'dropdown-{i}', 'value')])(update_chk)
#
#
# ################################################################################
# # Update primary transformation
# update_dd_trans_p = _generate_update_dd('transformation', primary_fields)
# app.callback([Output('dropdown-primary-trans', 'options'),
#               Output('dropdown-primary-trans', 'value'),
#               Output('dropdown-primary-trans', 'disabled')],
#              [Input(f'dropdown-primary', 'value')])(update_dd_trans_p)
#
# # Update primary aggregation
# update_dd_agg_p = _generate_update_dd('aggregation', primary_fields)
# app.callback([Output('dropdown-primary-agg', 'options'),
#               Output('dropdown-primary-agg', 'value'),
#               Output('dropdown-primary-agg', 'disabled')],
#              [Input(f'dropdown-primary', 'value')])(update_dd_agg_p)


###################################################
# Update dropdowns based on selected STANDARD QUERY
#     OR any of the LHS variable dropdowns.
###################################################
elements_to_update = ['options', 'value', 'disabled']
dd_types = ['', '-trans', '-agg']
primary_outs = [Output(f'dropdown-primary{t}', element) for element in elements_to_update
              for t in dd_types]

secondary_outs = []
for i in range(num_secondary):
    row = []
    row.extend([Output(f'dropdown-{i}{t}', element) for element in
                       elements_to_update for t in dd_types])
    row.append(Output(f'check-{i}', 'options'))
    row.append(Output(f'check-{i}', 'value'))
    secondary_outs.append(row)

secondary_var_inputs = [Input(f'dropdown-{i}', 'value') for i in range(num_secondary)]


@app.callback(primary_outs,
              [Input('submit-button-standard', 'n_clicks'),
               Input(f'dropdown-primary', 'value')],
              [State('dropdown-squery', 'value')])
def update_dds_primary(n_clicks, val, query_ix):

    # what called the function?
    ctx = dash.callback_context
    if not ctx.triggered:
        trigger_id = 'submit-button-standard'
    else:
        trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]

    out = []
    if trigger_id == 'submit-button-standard':
        query = get_query_from_index(query_ix, standard_queries)
        panel_rows = standard_query_to_panel_indices(query, primary_fields,
                                                     secondary_fields,
                                                     as_obj=True)
        # primary variable
        first_row = panel_rows[0]
        field = primary_fields[first_row.item_id]
        #  * primary variable dropdown
        out.extend([dropdown_Primary.options, first_row.item_id, False])
        #  * transformation dropdown
        trans_id = not_none(first_row.trans_id, 0)
        out.extend([field.transformations, trans_id, field.transformation_is_disabled])
        #  * aggregation dropdown
        agg_id = not_none(first_row.agg_id, 0)
        out.extend([field.aggregations, agg_id, field.aggregation_is_disabled])
    elif trigger_id == 'dropdown-primary':
        if val is None:
            # * User has cleared the Field dropdown using [x]
            return (dash.no_update, dash.no_update, dash.no_update,
                    [{'label': '<None>', 'value': 0}], -1, True,
                    [{'label': '<None>', 'value': 0}], -1, True)
        else:
            field = primary_fields[val]
            out = [dash.no_update, dash.no_update, dash.no_update]
            out.extend([field.transformation_options, field.default_transformation_ix,
                     field.transformation_is_disabled])
            out.extend([field.aggregation_options, field.default_aggregation_ix,
                        field.aggregation_is_disabled])
    return out


def _generate_update_dds_secondary(i):
    def update_dds_secondary(n_clicks, val, query_ix):

        # what called the function?
        ctx = dash.callback_context
        if not ctx.triggered:
            trigger_id = 'submit-button-standard'
        else:
            trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]

        out = []
        if trigger_id == 'submit-button-standard':
            query = get_query_from_index(query_ix, standard_queries)
            panel_rows = standard_query_to_panel_indices(query, primary_fields,
                                                         secondary_fields,
                                                         as_obj=True)

            row = panel_rows[i+1]
            field = secondary_fields[row.item_id]
            #  * variable dropdown
            out.extend([secondary_var_options, row.item_id, False])
            #  * transformation dropdown
            trans_id = not_none(row.trans_id, 0)
            out.extend([field.transformation_options, trans_id,
                        field.transformation_is_disabled])
            #  * aggregation dropdown
            agg_id = not_none(row.agg_id, 0)
            out.extend([field.aggregations_options, agg_id,
                        field.aggregation_is_disabled])
            out.extend([field.lkp_options, [1] if row.perform_lkp else [] ])
        elif trigger_id == f'dropdown-{i}':
            if val is None or val == 0:
                # * User has cleared the Field dropdown using [x], OR
                # * "No variable" is selected via the <None> field.
                return (dash.no_update, dash.no_update, dash.no_update,
                        [{'label': '<None>', 'value': 0}], -1, True,
                        [{'label': '<None>', 'value': 0}], -1, True,
                        [{'label': '', 'value': 1, 'disabled': True}], [])
            else:
                field = secondary_fields[val]
                out = [dash.no_update, dash.no_update, dash.no_update]
                out.extend([field.transformation_options, field.default_transformation_ix,
                            field.transformation_is_disabled])
                out.extend([field.aggregation_options, field.default_aggregation_ix,
                            field.aggregation_is_disabled])
                out.extend([field.lkp_options, [] ])
        return out
    return update_dds_secondary


for i in range(num_secondary):
    update_dd = _generate_update_dds_secondary(i)
    app.callback(secondary_outs[i],
                 [Input('submit-button-standard', 'n_clicks'),
                  Input(f'dropdown-{i}', 'value')],
                 [State('dropdown-squery', 'value')])(update_dd)

# @app.callback(all_outs,
#               [Input('submit-button-standard', 'n_clicks'),
#                Input(f'dropdown-primary', 'value'),
#                *secondary_var_inputs],
#               [State('dropdown-squery', 'value')])
# def update_dds_from_standard(n_clicks, *args):
#     query_ix = args.pop(-1)
#
#     # what called the function?
#     ctx = dash.callback_context
#     if not ctx.triggered:
#         trigger_id = 'submit-button-standard'
#     else:
#         trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
#
#     if trigger_id == 'submit-button-standard':
#         query = get_query_from_index(query_ix, standard_queries)
#         panel_rows = standard_query_to_panel_indices(query, primary_fields, secondary_fields,
#                                                     as_obj=True)
#         n = len(panel_rows)
#         first_row = panel_rows[0]
#         secondary_rows = panel_rows[1:]
#         primary, secondary = True, True
#     elif trigger_id == 'dropdown-primary':
#         out = []
#         # primary variable
#         first_row = panel_rows[0]
#         field = primary_fields[first_row.item_id]
#         #  * primary variable dropdown
#         out.extend([dropdown_Primary.options, first_row.item_id, False])
#         #  * transformation dropdown
#         trans_id = first_row.trans_id if first_row.trans_id is not None else 0
#         trans_disable = (len(field.transformations) == 1) and (field.transformations[0] is None)
#         out.extend([field.transformations, trans_id, trans_disable])
#         #  * aggregation dropdown
#         agg_id = first_row.agg_id if first_row.agg_id is not None else 0
#         agg_disable = (len(field.aggregations) == 1) and (field.aggregations[0] is None)
#         out.extend([field.aggregations, agg_id, agg_disable])
#
#         # secondary variables
#         checklists = []
#         for i in range(n-1):
#             # variable
#             row = panel_rows[i+1]
#             field = secondary_fields[row.item_id]
#             #  * variable dropdown
#             out.extend([secondary_var_options, row.item_id, False])
#             #  * transformation dropdown
#             trans_id = row.trans_id if row.trans_id is not None else 0
#             trans_disable = (len(field.transformations) == 1) and (
#                         field.transformations[0] is None)
#             out.extend([field.transformations, trans_id, trans_disable])
#             #  * aggregation dropdown
#             agg_id = row.agg_id if row.agg_id is not None else 0
#             agg_disable = (len(field.aggregations) == 1) and (field.aggregations[0] is None)
#             out.extend([field.aggregations, agg_id, agg_disable])
#
#             # checklist
#             chk_disabled = field.has_dim_lkp
#             checklists.append([{'label': '', 'value': 1, 'disabled': chk_disabled}])
#             checklists.append([1] if row.perform_lkp else [])  # checklist value
#
#         for i in range(num_secondary - (n - 1)):
#             out.extend([secondary_var_options, 0, False])
#             checklists.append([{'label': '', 'value': 1, 'disabled': True}])
#             checklists.append([])  # checklist value
#
#         out.extend(checklists)
#     return out


# --------- RUN APP -------------------------------------------------

if __name__ == '__main__':
    app.run_server(debug=True)





"""
TODO:
=============
* problem if "Name" unavailable and was previously ticked -> gets locked and breaks.
* transformation --> aggregation via subqueries if necessary.

"""