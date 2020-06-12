import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State

# --------- "GLOBALS" -------------------------------------------------
main_text_style = {'text-align': 'center', 'max-width': '800px', 'margin': 'auto'}
lhs_text_style = {'text-align': 'left', 'max-width': '800px', 'margin': 'auto'}
rhs_text_style = {'text-align': 'right', 'max-width': '800px', 'margin': 'auto'}
main_div_style = {'margin':'auto', 'padding-left': '100px', 'padding-right': '100px',
                  'padding-top':'20px', 'max-width': '1100px'}


# --------- DATA ------------------------------------------------------
import example



# --------- DEFINE INPUT ----------------------------------------------

dropdown_Primary = dcc.Dropdown(
            id='dropdown-primary',
            options=[{'label': opt.item, 'value': i}
                     for i, opt in enumerate(example.opts_aggregation)],
    value=0)
dropdown_Primary_trans = dcc.Dropdown(
            id='dropdown-primary-trans',
            options=[
                {'label': '<None>', 'value': 0}
            ], value=0)
dropdown_Primary_agg = dcc.Dropdown(
            id='dropdown-primary-agg',
            options=[
                {'label': '<None>', 'value': 0},
                {'label': 'Count', 'value': 1}
            ], value=0)


def construct_dropdowns(id, opts):
    opts = [None, *opts]
    return dcc.Dropdown(
            id=id,
            options=[{'label': opt.item, 'value': i} if i > 0 else
                     {'label': '<None>', 'value': 0} for i, opt in enumerate(opts)],
            value=0), dcc.Dropdown(
            id=id+'-trans', options=[{'label': '<None>', 'value': 0}],
            value=0), dcc.Dropdown(
            id=id+'-agg', options=[{'label': '<None>', 'value': 0}], value=0)


num_secondary = 4
dropdowns = [construct_dropdowns('dropdown-'+str(i), example.opts_split)
             for i in range(num_secondary)]

secondary_dropdown_div = [
        html.Div([
                    html.Div(dropdowns[i][0], className="four columns"),
                    html.Div(dropdowns[i][1], className="four columns"),
                    html.Div(dropdowns[i][2], className="four columns"),
                ], className="row", style={'padding-bottom': '15px'})
        for i in range(num_secondary)]

# --------- COPY ------------------------------------------------------

introduction = '''
### SQL Generation for OMOP Data

For the purposes of this demonstration, we will assume one wants to aggregate some
features of EHR data to a person level from an OMOP standard database.

See the drop-down menus on the left to select your query, 
'''



# --------- CONSTRUCT APP ---------------------------------------------
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server


app.layout = html.Div([
    dcc.Markdown(children=introduction, style=main_text_style, className="row"),
    html.Br(),
    html.Div([
        html.Div([
            dcc.Markdown("**Primary variable**:", style=main_text_style,
                         className="four columns"),
            dcc.Markdown("**Transformation**:", style=main_text_style,
                         className="four columns"),
            dcc.Markdown("**Aggregation**:", style=main_text_style,
                         className="four columns")
        ], className="row"),
        html.Br(),
        html.Div([
            html.Div(dropdown_Primary, className="four columns"),
            html.Div(dropdown_Primary_trans, className="four columns"),
            html.Div(dropdown_Primary_agg, className="four columns"),
        ], className="row"),
        html.Br(),
        html.Div([
            dcc.Markdown("**Secondary variables**:", style=main_text_style,
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
all_states = [State('dropdown-primary', 'value'),
               State('dropdown-primary-trans', 'value'),
               State('dropdown-primary-agg', 'value')]
for i in range(num_secondary):
    all_states.extend([State(f'dropdown-{i}', 'value'),
                   State(f'dropdown-{i}-trans', 'value'),
                   State(f'dropdown-{i}-agg', 'value')])

@app.callback(Output('sql-output-container', 'children'),
              [Input('submit-button', 'n_clicks')],
              all_states)
def update_output(n_clicks, *args):
    # tmp = construct_query(opts_aggregation[0], opts_split[0], *opts_split[1:])
    # print(tmp)
    use_opts = []
    for i in range(num_secondary+1):
        c_ix = args[i*3]
        if i == 0:
            opt = example.opts_aggregation[c_ix]
        elif c_ix > 0:
            opt = example.opts_split[c_ix-1]
        else:
            continue
        t_val, agg_val = args[i*3+1], args[i*3+2]
        if t_val > 0:
            t = opt.transformations[t_val-1]
        elif agg_val > 0:
            t = opt.transformations[agg_val-1]
        else:
            t = None
        opt.select_transform(t)
        use_opts.append(opt)

    if len(use_opts) > 0:
        sql = example.construct_query(*use_opts)
    else:
        sql = "\n\n~~~~ NO VARIABLES SELECTED ~~~~~\n\n"
    return html.Pre(sql) #[html.P(l) for l in lines]


def _generate_update_dd_trans():
    def update_dd_trans(val): \
        # we've prepended an additional None element to the variable selector, hence
        # we must treat val==0 differently, and subtract 1 from all other treatments.
        if val == 0:
            return [{'label': '<None>', 'value': 0}]
        else:
            print(val)
            trans = [None, *example.opts_split[val - 1].transformations]
            print(trans)
            return [{'label': t, 'value': i} if i > 0 else
                    {'label': '<None>', 'value': 0} for i, t in enumerate(trans)]
    return update_dd_trans


for i in range(num_secondary):
    update_dd_trans = _generate_update_dd_trans()
    app.callback(Output(f'dropdown-{i}-trans', 'options'),
                 [Input(f'dropdown-{i}', 'value')])(update_dd_trans)





if __name__ == '__main__':
    app.run_server(debug=True)





"""
TODO:
=============

* Want to allow BOTH aggregations AND transformations.
    * GUI allows only one or the other.
    * SQL Generator only allows one or the other.

GUI:
* Distinction of primary vs secondary variable can probably be dropped
* Allow aggregation and transformation (see SQL Generation)
* Dropdowns should disappear when there are no options.

SQL Generation
* GROUP BY retains aliases from `sql_transform` (i.e. foo AS bar)
* Add "as_english" / join to concept_id table for selected IDs. I think "as_english"
is wrong, but instead should just specify the Dimension table.
* transformation --> aggregation via subqueries if necessary.
* Need more consistency with Nones -- I think some Opts have the explicit option, whereas
some do not. I think this can then remove the "<NONE>" special case from the app.

"""