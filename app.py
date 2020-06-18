import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from pysqlgen.apputils import app_state_to_opts, get_trigger
from pysqlgen.utils import not_none, cur_time_ms

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
primary_stack = dcc.Store(id='stack-primary')


secondary_var_options = [{'label': '<None>', 'value': 0}]
secondary_var_options.extend([{'label': opt.item, 'value': i+1} for i, opt in
                              enumerate(secondary_fields)])

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
secondary_stacks = [dcc.Store(id=f'stack-{i}') for i in range(num_secondary)]

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
            html.Button(id='submit-button', n_clicks=0, children='Submit')
        ], style={'background-color': '#EEEEEE', 'padding': '10px'})
    ], className="four columns"),
    html.Div([
        html.Div(id='sql-output-container')
        ], className="six columns", style={'border': 'solid #CCCCCC 1px',
                                           'padding': '10px'}),
    primary_stack,
    *secondary_stacks

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


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
###################################################
# Update dropdowns based on selected STANDARD QUERY
#     OR any of the LHS variable dropdowns.
###################################################
elements_to_update = ['options', 'value', 'disabled']
dd_types = ['-trans', '-agg']
primary_outs = [Output(f'dropdown-primary{t}', element)
                for t in dd_types for element in elements_to_update]

secondary_outs = []
for i in range(num_secondary):
    row = []
    row.extend([Output(f'dropdown-{i}{t}', element) for t in dd_types for element in
                elements_to_update])
    row.append(Output(f'check-{i}', 'options'))
    row.append(Output(f'check-{i}', 'value'))
    secondary_outs.append(row)

secondary_var_inputs = [Input(f'dropdown-{i}', 'value') for i in range(num_secondary)]

##################
# PRIMARY VARIABLE
##################
@app.callback([Output('dropdown-primary', 'value'),
               Output('stack-primary', 'data'),
               Output('stack-primary', 'modified_timestamp')],
              [Input('submit-button-standard', 'n_clicks')],
              [State('dropdown-squery', 'value')])
def update_std_primary(n_clicks, query_ix):

    query = get_query_from_index(query_ix, standard_queries)
    panel_rows = standard_query_to_panel_indices(query, primary_fields,
                                                 secondary_fields,
                                                 as_obj=True)
    # primary variable
    first_row = panel_rows[0]
    trans_id = not_none(first_row.trans_id, 0)
    agg_id = not_none(first_row.agg_id, 0)
    push_to_stack = f'{trans_id},{agg_id}'

    return [first_row.item_id, push_to_stack, cur_time_ms()]


@app.callback([*primary_outs],
              [Input(f'dropdown-primary', 'value')],
              [State('stack-primary', 'data'),
               State('stack-primary', 'modified_timestamp')])
def update_dds_primary(val, stack, stack_ts):
    if val is None:
        # * User has cleared the Field dropdown using [x]
        return ([{'label': '<None>', 'value': 0}], -1, True,
                [{'label': '<None>', 'value': 0}], -1, True)

    cur_time = cur_time_ms()
    timedelta_s = (cur_time - stack_ts)/1000
    stale = timedelta_s > 1  # (> 1 seconds since stack last pushed)
    if stale or (stack is None) or (len(stack) == 0):
        print(f'Not using primary stack. Timedelta: {timedelta_s}. Contents: {stack}')
        stackvals = []
        use_stack = False
    else:
        print(f'Using primary stack. Contents: {stack}')
        try:
            stackvals = [int(x) for x in stack.split(',')]
        except Exception as e:
            print("STACK IS:", stack)
            raise e
        assert len(stackvals) == 2, f"stack {stack} is invalid for primary row"
        use_stack = True

    field = primary_fields[val]
    out = []
    trans_ix = stackvals[0] if use_stack else field.default_transformation_ix
    agg_ix = stackvals[1] if use_stack else field.default_aggregation_ix
    out.extend([field.transformation_options, trans_ix, field.transformation_is_disabled])
    out.extend([field.aggregation_options, agg_ix, field.aggregation_is_disabled])

    return out


#####################
# SECONDARY VARIABLES
#####################
def _generate_update_std_secondary(i):
    def update_std(n_clicks, query_ix):

        query = get_query_from_index(query_ix, standard_queries)
        panel_rows = standard_query_to_panel_indices(query, primary_fields,
                                                     secondary_fields,
                                                     as_obj=True)
        if len(panel_rows) > i+1:
            row = panel_rows[i+1]
            ix = row.item_id
            push_to_stack = f'{row.trans_id},{row.agg_id},{row.perform_lkp}'
        else:
            ix = None
            push_to_stack = ''
        return [ix, push_to_stack, cur_time_ms()]
    return update_std


for i in range(num_secondary):
    update_std = _generate_update_std_secondary(i)
    app.callback([Output(f'dropdown-{i}', 'value'),
                  Output(f'stack-{i}', 'data'),
                  Output(f'stack-{i}', 'modified_timestamp')],
                 [Input('submit-button-standard', 'n_clicks')],
                 [State('dropdown-squery', 'value')])(update_std)


def _generate_update_dds_secondary(i):
    def update_dds(val, stack, stack_ts):
        if val is None or val == 0:
            # EITHER:
            # * User has cleared the Field dropdown using [x]
            # * No variable is selected via the <None> field.
            return ([{'label': '<None>', 'value': 0}], -1, True,
                    [{'label': '<None>', 'value': 0}], -1, True,
                    [{'label': '', 'value': 1, 'disabled': True}],
                    [])

        cur_time = cur_time_ms()
        timedelta_s = (cur_time - stack_ts) / 1000
        stale = timedelta_s > 1  # (> 1 seconds since stack last pushed)
        if stale or (stack is None) or (len(stack) == 0):
            print(f'Not using stack {i}. Timedelta: {timedelta_s}. Contents: {stack}')
            stackvals = []
            use_stack = False
        else:
            print(f'Using stack {i}. Timedelta: {timedelta_s}. Contents: {stack}')
            stacksplit = stack.split(',')
            stackvals = [int(x) for x in stacksplit[:2]]
            stackvals.append(stacksplit[2] == 'True')
            assert len(stackvals) == 3, f"stack {stack} is invalid for secondary row"
            use_stack = True


        field = secondary_fields[val-1]
        out = []
        trans_ix = stackvals[0] if use_stack else field.default_transformation_ix
        agg_ix = stackvals[1] if use_stack else field.default_aggregation_ix
        chkmark = stackvals[2] if use_stack else False
        out.extend([field.transformation_options, trans_ix,
                    field.transformation_is_disabled])
        out.extend([field.aggregation_options, agg_ix,
                    field.aggregation_is_disabled])

        chk_disabled = not field.has_dim_lkp
        out.append([{'label': '', 'value': 1, 'disabled': chk_disabled}])
        out.append([1] if chkmark else [])  # checklist value
        # print(out)
        return out
    return update_dds


for i in range(num_secondary):
    update_dds = _generate_update_dds_secondary(i)
    app.callback(secondary_outs[i],
                 [Input(f'dropdown-{i}', 'value')],
                 [State(f'stack-{i}', 'data'),
                  State(f'stack-{i}', 'modified_timestamp')])(update_dds)


# --------- RUN APP -------------------------------------------------

if __name__ == '__main__':
    app.run_server(debug=True)




"""
TODO:
=============
* problem if "Name" unavailable and was previously ticked -> gets locked and breaks.
* transformation --> aggregation via subqueries if necessary.

"""