import dash
import dash_core_components as dcc
import dash_html_components as html
from pysqlgen.utils import sync_index, get_nth_chunk
from dash.dependencies import Input, Output, State

# --------- "GLOBALS" -------------------------------------------------
main_text_style = {'text-align': 'center', 'max-width': '800px', 'margin': 'auto'}
tab_header_text_style = {'text-align': 'center', 'max-width': '800px', 'margin': 'auto',
                         'font-size':'14px'}
lhs_text_style = {'text-align': 'left', 'max-width': '800px', 'margin': 'auto'}
rhs_text_style = {'text-align': 'right', 'max-width': '800px', 'margin': 'auto'}
main_div_style = {'margin':'auto', 'padding-left': '100px', 'padding-right': '100px',
                  'padding-top':'20px', 'max-width': '1100px'}


# --------- DATA ------------------------------------------------------
import example

primary_fields = example.opts_aggregation
primary_fields[0].set_aggregation('count')
secondary_fields = example.opts_split
debug_ui = True

# --------- DEFINE INPUT ----------------------------------------------
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


def construct_dropdowns(id, opts):
    opts = [None, *opts]
    return dcc.Dropdown(id=id,
                        options=[{'label': opt.item, 'value': i} if i > 0 else
                        {'label': '<None>', 'value': 0} for i, opt in enumerate(opts)],
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


app.layout = html.Div([
    dcc.Markdown(children=introduction, style=main_text_style, className="row"),
    html.Br(),
    html.Div([
        html.Div([
            dcc.Markdown("**Primary variable**:", style=tab_header_text_style,
                         className="four columns"),
            dcc.Markdown("**Transform**:", style=tab_header_text_style,
                         className="three columns"),
            dcc.Markdown("**Aggregation**:", style=tab_header_text_style,
                         className="four columns"),
            dcc.Markdown("**Name**:", style=tab_header_text_style,
                         className="one column")
        ], className="row"),
        html.Br(),
        html.Div([
            html.Div(dropdown_Primary, className="four columns"),
            html.Div(dropdown_Primary_trans, className="three columns"),
            html.Div(dropdown_Primary_agg, className="three columns"),
        ], className="row"),
        html.Br(),
        html.Div([
            dcc.Markdown("**Secondary variables**:", style=tab_header_text_style,
                         className="four columns"),
        ], className="row"),
        html.Br(),
        *secondary_dropdown_div,
        html.Br(),
        html.Button(id='submit-button', n_clicks=0, children='Submit'),
    ], className="four columns",
        style={'background-color': '#EEEEEE', 'padding': '10px'}),
    html.Div([
        html.Div(id='sql-output-container')
        ], className="six columns", style={'border': 'solid #CCCCCC 1px',
                                           'padding': '10px'})

])


# --------- REACTIVE -------------------------------------------------

################################
# Click "Submit" to generate SQL
################################
all_states = [State('dropdown-primary', 'value'),
               State('dropdown-primary-trans', 'value'),
               State('dropdown-primary-agg', 'value')]
for i in range(num_secondary):
    all_states.extend([State(f'dropdown-{i}', 'value'),
                   State(f'dropdown-{i}-trans', 'value'),
                   State(f'dropdown-{i}-agg', 'value'),
                   State(f'check-{i}', 'value')])

@app.callback(Output('sql-output-container', 'children'),
              [Input('submit-button', 'n_clicks')],
              all_states)
def update_output(n_clicks, *args):
    # tmp = construct_query(opts_aggregation[0], opts_split[0], *opts_split[1:])
    # print(tmp)
    use_opts = []
    chunksizes = [3]
    chunksizes.extend([4]*num_secondary)
    for i in range(num_secondary+1):
        c_args = get_nth_chunk(i, args, chunksizes)
        c_ix = c_args[0]

        if i == 0:
            opt = primary_fields[c_ix]
        elif c_ix > 0:
            opt = secondary_fields[c_ix - 1]
        else:
            continue

        t_val, agg_val = c_args[1], c_args[2]
        # try/except: might be out of range if dropdowns have changed, and hence None.
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
        print(opt.item)
        use_opts.append(opt)

    if len(use_opts) > 0:
        sql = example.construct_query(*use_opts)
    else:
        sql = "\n\n~~~~ NO VARIABLES SELECTED ~~~~~\n\n"

    if debug_ui:
        raw = []
        for i in range(1+num_secondary):
            line = get_nth_chunk(i, args, [3, *[4]*num_secondary])
            line = ", ".join([str(x) for x in line])
            raw.append(line)
        sql += '\n\n\n' + "\n".join(raw)
    return html.Pre(sql)


##########################################
# Update dropdowns based on selected field
##########################################
def _generate_update_dd(trans_or_agg, fields, has_none_field=False):
    # below, val is the *index* of the selected variable in the LHS dropdown
    def update_dd_trans(val):
        if val is None or \
                (has_none_field and val == 0):
            # Clear dropdowns if:
            # * User has cleared the Field dropdown using [x]
            # * No variable is selected via the <None> field.
            # in this context, we should show nothing / blank out dropdown.
            return [{'label': '<None>', 'value': 0}], -1, True
        else:
            # val-1 if <none> field exists, o.w. val
            val = val -1 if has_none_field else val
            opt = fields[val]
            if trans_or_agg == 'transformation':
                options_list = [*opt.transformations]
                value = options_list.index(opt.default_transformation)
            elif trans_or_agg == 'aggregation':
                options_list = [*opt.aggregations]
                value = options_list.index(opt.default_aggregation)
            else:
                raise RuntimeError(f'Unknown drop-down type: {trans_or_agg}')
            print(options_list)
            print(value)
            disable = True if ((len(options_list) == 1) and (options_list[0] is None)) \
                else False

            print(disable)
            return [{'label': t, 'value': i} if t is not None else
                    {'label': '<None>', 'value': i} for i, t in enumerate(options_list)],\
                    value, disable

    return update_dd_trans


for i in range(num_secondary):
    update_dd_trans = _generate_update_dd('transformation', secondary_fields,
                                          has_none_field=True)
    app.callback([Output(f'dropdown-{i}-trans', 'options'),
                  Output(f'dropdown-{i}-trans', 'value'),
                  Output(f'dropdown-{i}-trans', 'disabled')],
                 [Input(f'dropdown-{i}', 'value')])(update_dd_trans)


for i in range(num_secondary):
    update_dd_agg = _generate_update_dd('aggregation', secondary_fields,
                                        has_none_field=True)
    app.callback([Output(f'dropdown-{i}-agg', 'options'),
                  Output(f'dropdown-{i}-agg', 'value'),
                  Output(f'dropdown-{i}-agg', 'disabled')],
                 [Input(f'dropdown-{i}', 'value')])(update_dd_agg)

update_dd_trans_p = _generate_update_dd('transformation', primary_fields)
app.callback([Output('dropdown-primary-trans', 'options'),
              Output('dropdown-primary-trans', 'value'),
              Output('dropdown-primary-trans', 'disabled')],
             [Input(f'dropdown-primary', 'value')])(update_dd_trans_p)

update_dd_agg_p = _generate_update_dd('aggregation', primary_fields)
app.callback([Output('dropdown-primary-agg', 'options'),
              Output('dropdown-primary-agg', 'value'),
              Output('dropdown-primary-agg', 'disabled')],
             [Input(f'dropdown-primary', 'value')])(update_dd_agg_p)


if __name__ == '__main__':
    app.run_server(debug=True)





"""
TODO:
=============

* Want to allow BOTH aggregations AND transformations.
    * GUI allows only one or the other.
    * SQL Generator only allows one or the other.

GUI:
* ~~Distinction of primary vs secondary variable can probably be dropped~~

SQL Generation
* Add "as_english" / join to concept_id table for selected IDs. I think "as_english"
is wrong, but instead should just specify the Dimension table.
* transformation --> aggregation via subqueries if necessary.

"""