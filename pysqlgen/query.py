from collections import UserList, OrderedDict, defaultdict
import copy
import re
import textwrap
from .dbtree import CTENode, minimum_subtree, topological_sort_hierarchical
from .fields import UserOption, construct_simple_field
from .utils import rm_alias_placeholder, make_unique_name, flatten, ilen, \
    replace_in_ordered_dict
from . import graph


class Statement:
    """
    The Statement class maintains lists of all components of the
    SELECT, FROM, WHERE and GROUP BY elements of a SQL query. Each
    of these four query aspects can be treated individually, and
    generate separate statements; but the overall Statement object
    can generate the entire query, and moreover, maintains the
    mapping of tables to aliases used in the JOIN statement.
    """

    def __init__(self, context):
        self.aliases = dict()
        self.lkp_aliases = dict()
        self.context = context
        self._from = StmtFrom(parent=self)
        self.select = StmtGeneric('SELECT', parent=self)
        self.where = StmtGeneric('WHERE', parent=self, conj=' AND', wrappers=('(', ')'))
        self.groupby = StmtGeneric('GROUP BY', parent=self)
        self.ctes = StmtCTE(parent=self)

    def generate_statement(self, dialect='MSSS'):
        out1 = self.ctes.generate_statement(dialect=dialect)
        out2 = self.select.generate_statement()
        out3 = self._from.generate_statement()
        out4 = self.where.generate_statement()
        out5 = self.groupby.generate_statement()
        return out1 + out2 + out3 + out4 + out5
        # return self.ctes.generate_statement(dialect=dialect) + \
        #        self.select.generate_statement() + \
        #        self._from.generate_statement() + \
        #        self.where.generate_statement() + \
        #        self.groupby.generate_statement()


class StmtGeneric(UserList):
    """
    StmtGeneric is a list-like class which is expected to sit within
    a larger Statement object. The object admits a `generate_statement`
    method which will generate a SELECT, WHERE or GROUP BY statement
    (for example) from the list of items it contains. However, due to
    the complexities of JOINs, the FROM statement uses a different class.
    """

    def __init__(self, statement_type, parent, conj=None, wrappers=None, ws=None):
        super().__init__()
        self.statement = statement_type
        self.whitespace = ws if ws is not None else (len(self.statement) + 1)
        self.parent = parent
        self.conj = ',' if conj is None else conj
        self.wrappers = wrappers

    def generate_statement(self):
        lines = list(filter(lambda x: x is not None and len(x) > 0, self))
        if len(lines) == 0:
            return ''
        ws = self.whitespace
        # whitespace for multi-line clauses
        lines = [('\n' + ' ' * ws).join(x.split('\n')) for x in lines]
        # add () etc. in case need to separate clauses
        if self.wrappers:
            lines = [self.wrappers[0] + l + self.wrappers[1] for l in lines]
        return f'{self.statement} ' + \
               (self.conj + '\n' + ' ' * ws).join(lines) + '\n\n'


class StmtCTE(StmtGeneric):

    def __init__(self, parent, conj=None):
        statement_type, wrappers, ws = 'WITH', None, 4
        super().__init__(statement_type, parent, conj, wrappers, ws)

    def generate_statement(self, dialect='MSSS'):
        tables = list(filter(lambda x: isinstance(x, CTENode), self))
        if len(tables) == 0:
            return ''
        out = []
        for i, node in enumerate(tables):
            # construct inner CTE query (and add margin)
            q = construct_query(*node.fields, dialect=dialect).strip()
            q = "\n".join([" "*self.whitespace + line for line in q.split("\n")])
            # construct outer part of CTE query.
            fields = [x.field_alias for x in node.fields]
            # wrap header of CTE query if required, and indent subsequent lines
            indent_str = ' ' * (len(node.name) + 3 + 4*(i == 0))
            wrap = textwrap.TextWrapper(width=100, subsequent_indent=indent_str)
            cte_head = wrap.fill(f'{node.name} ({", ".join(fields)})')
            cte_head += '\nAS '
            # concatenate result to `out`
            cte = cte_head + f'(\n{q}\n)'
            out.append(cte)
        return f'{self.statement} ' + ("\n\n" + self.conj).join(out) + '\n\n'


class StmtFrom(OrderedDict):
    """
    StmtFrom serves a similar purpose to StmtGeneric but subclasses a
    dictionary in order to store both the tables and the join conditions.
    """

    def __init__(self, parent):
        super().__init__()
        self.parent = parent
        self.generated = None
        self.additional_lkps = None

    def __setitem__(self, key, value):
        if key in self.keys():
            if self[key] != value:
                raise Exception(f'Trying to place\n {key}: {value} \n\nin the FROM' + \
                                f' dict, but\n {key}: {self[key]} \n\nalready exists!')
        else:
            super().__setitem__(key, value)

    def generate_basic_statement(self, force_alias=False):
        if self.generated is None:
            nodes = self.keys()  #topological_sort_hierarchical(list(self.keys()))
            n = len(nodes)
            join_list = []
            global_schema = self.parent.context.schema
            global_schema = '' if global_schema == '' else global_schema + '.'
            aliases = self.parent.aliases
            for i, node in enumerate(nodes):
                v = self[node]
                if node.schema is None:
                    schema = global_schema
                else:
                    schema = '' if node.schema == '' else node.schema + '.'
                if i == 0:
                    assert len(v) == 0, "first table should not have a join condition"
                    alias = '' if (n == 1 and not force_alias) else node.name[0].lower()
                    aliases[node] = alias
                    join_list.append(f'     {schema}{node.name} {alias}')
                else:
                    # make human readable alias (using table prefix, not simply a,b,c,...)
                    alias = make_unique_name(node.name, aliases)
                    aliases[node] = alias
                    a = (aliases[v[0][0]], aliases[v[1][0]])  #  join aliases
                    c = (v[0][1], v[1][1])  # join columns
                    s = len(c[0])           # number of join conditions
                    on_stmt = [f'{a[0]}.{c[0][i]} = {a[1]}.{c[1][i]}' for i in range(s)]
                    join_list.append(f'LEFT JOIN {schema}{node.name} {alias}\nON        '
                                     + '\nAND       '.join(on_stmt))
            self.generated = 'FROM ' + '\n'.join(join_list)
        return self.generated + '\n'

    def add_lookups_to_statement(self, joins):
        assert self.generated is not None, "Please run 'generate_basic_statement' first."
        join_list = []
        global_schema = self.parent.context.schema
        global_schema = '' if global_schema == '' else global_schema + '.'
        aliases = self.parent.aliases
        lkp_aliases = self.parent.lkp_aliases
        for i, join in enumerate(joins):
            ((tbl_existing, f_existing), (tbl_dim, f_dim)) = join
            schema = global_schema if tbl_dim.schema is None else tbl_dim.schema
            alias = make_unique_name(tbl_dim.name, aliases, lkp_aliases)
            lkp_aliases[join] = alias
            join_list.append(f'LEFT JOIN {schema}{tbl_dim.name} {alias}\nON        '
                             + f'{aliases[tbl_existing]}.{f_existing} = {alias}.{f_dim}')
        self.additional_lkps = '\n'.join(join_list)

    def generate_statement(self):
        assert self.generated is not None, "Please run 'generate_basic_statement' first."
        generated = self.generated
        if self.additional_lkps is not None and len(self.additional_lkps) > 0:
            generated += '\n' + self.additional_lkps
        return generated + '\n\n'


def construct_query(*args, dialect='MSSS', allow_coalesce=True):
    """
    Construct a SQL query from a list of various UserOptions. Each option
    contains a field, a transformation/aggregation, and the table in which
    the field resides. This function handles constructing the query, which
    uses the graph implicit in the SchemaNodes residing in the UserOptions.

    :param dialect - may be MS SQL Server ('MSSS') or 'Postgres'. Very limited
    customisation is currently available for these. The differences have been
    populated on a case-by-case basis rather than anything systematic.
    :return: (string) SQL statement
    """
    n = len(args)
    assert all([isinstance(o, UserOption) for o in args]), "Not all args are UserOptions"
    invalids = [not o.validate() for o in args]
    assert not any(invalids), "{:d}/{:d} options have not been validated".format(
        sum(invalids), n)
    assert all([args[0].context == args[i+1].context for i in range(n-1)]), "Different" +\
        "contexts associated with the User Opts. Ensure these are the same."
    stmt = Statement(args[0].context)

    # ==== GET ALL TABLES AND FORM BASIC JOIN SUBTREE ============
    context = args[0].context
    nodes = [o.get_table() for o in args]
    unique_nodes = list(set(nodes))
    join_tree = minimum_subtree(unique_nodes)

    # The primary table is considered the "root node" of the tree -- edges are undirected.
    primary = [o for o in args if not o.is_secondary]
    assert len(primary) == 1, f'Expecting one primary field. Got {len(primary)}.'
    primary = primary[0]
    primary_tbl = primary.get_table()

    # topological sort
    edges = [(k, v[0][0]) for (i,(k,v)) in enumerate(join_tree.items()) if i > 0]
    G = graph.Graph(edges, directed=False)
    sorted_nodes = G.topological_sort(leave_until_last=primary_tbl)
    # (Recreate G since the topological_sort mutates G (ikr - should fix this!))
    G = graph.Graph(edges, directed=False)

    # ======= CREATE CTEs WHENEVER WE FIND A NESTED AGGREGATION ============
    field_tbl_lkp = defaultdict(list)
    for arg in args:
        field_tbl_lkp[arg.get_table()].append(arg)
    all_fields = dict()

    # Traverse the join tree of the selected tables, making CTEs wherever
    # there exists secondary aggregation (i.e. not of the primary variable).
    for i, v in enumerate(sorted_nodes[:-1]):
        v_fields = field_tbl_lkp[v]
        any_v_has_agg = any([arg.has_aggregation for arg in v_fields])

        # Get 'child tables' and 'parent tables' (wrt topological sort)
        child_tbls = G._graph[v] - set(sorted_nodes[i:])
        parent_tbl = join_tree[v][0][0]
        parent_fks, v_pks = join_tree[v][0][1], join_tree[v][1][1]

        # The current table contains an aggregation, so create a CTE.
        if any_v_has_agg:
            # Get all arguments for table which are not aggregated
            f_non_agg = [arg for arg in v_fields if not arg.has_aggregation]
            f_non_agg += flatten([all_fields[w] for w in child_tbls])

            # Add in table PKs if not already included in `f_non_agg`.
            f_non_agg_names = [f.sql_fieldname for f in f_non_agg]
            pk_names_to_add = [f for f in v_pks if f not in f_non_agg_names]
            pks_to_add = [construct_simple_field(f, v, context) for f in pk_names_to_add]

            # Make a copy of all non-aggregated AND aggregated fields.
            f_non_agg_cte = [f.copy() for f in f_non_agg]
            f_agg_cte = [arg for arg in v_fields if arg.has_aggregation]
            f_agg = [f.copy() for f in f_agg_cte]

            # consolidate CTE fields and add primary designator to (any) field in root
            cte_fields = pks_to_add + f_non_agg_cte + f_agg_cte
            for field in cte_fields:
                if field.sql_fieldname in v_pks:
                    field.is_secondary = False
                    break

            # Defer lookups until the final query
            for field in cte_fields:
                field.perform_lkp = False

            # Construct CTE
            cte = CTENode([parent_tbl],    # parent
                          v_pks,           # pk
                          cte_fields)      # cte_fields
            stmt.ctes.append(cte)

            # Point the [references to the aggregations] outside the CTE to the CTE field
            for f in f_agg:
                f.field_alias = f.field_alias  # this is *not* a noop! (@property...)
                f.sql_item = '{alias}' + f.field_alias
                f.table = cte
                f.set_aggregation(None, force=True)
                f.set_transform(None, force=True)
                if allow_coalesce:
                    f.coalesce = 0                  # assumes that aggregation is numeric

            for f in f_non_agg:
                f.table = cte
                f.sql_item = '{alias}'+f._field_alias_logic(will_perform_lkp=False)

            # Push these pointers to within the CTE up to the parent (although not PKs)
            all_fields[v] = f_non_agg + f_agg

            # # overwrite the node `v` in the subtree with `cte`
            # join_cond = tree_final[v]
            # join_cond = (join_cond[0], (cte, join_cond[1][1]))
            # tree_final = replace_in_ordered_dict(tree_final, v, cte, join_cond)
        else:
            all_fields[v] = v_fields + flatten([all_fields[w] for w in child_tbls])

    # Set args to the args propagated up to the root node
    primary_children = G._graph[primary_tbl]
    args = field_tbl_lkp[primary_tbl] + flatten([all_fields[c] for c in primary_children])
    # Place the resulting tree into the FROM clause of the Statement object.

    nodes = [o.get_table() for o in args]
    unique_nodes = list(set(nodes))
    tree_final = minimum_subtree(unique_nodes)

    stmt._from.update(tree_final)

    # Add dimension tables (if requested to joins)
    lkp_joins = []
    for o in args:
        if o.perform_lkp:
            dtbl = o.dimension_table
            assert not dtbl.is_cte, "Currently unable to support CTEs for dimension " + \
                                    "tables. (But it only requires thinking about how " + \
                                    "to avoid multiple copies.)"
            fk = o.sql_fieldname
            lkp_joins.append(((o.table, fk), (dtbl, dtbl.pk[0])))

    # Generate statement in order to populate aliases dict
    stmt._from.generate_basic_statement(force_alias=(len(lkp_joins) > 0))
    stmt._from.add_lookups_to_statement(lkp_joins)

    # === CONSTRUCT SELECT / WHERE ===========
    has_agg = any([arg.has_aggregation for arg in args])

    for o in args:
        # Get table alias for field, depending on whether has lookup table or not
        if not o.perform_lkp:
            alias = stmt.aliases[o.get_table()]
            # coalesce is usually None anyway
            coalesce = o.coalesce if allow_coalesce else None
        else:
            dtbl = o.dimension_table
            fk = rm_alias_placeholder(o.sql_item)
            alias = stmt.lkp_aliases[((o.table, fk), (dtbl, dtbl.pk[0]))]
            coalesce = context.coalesce_default if allow_coalesce else None

        sel, where = o.sql_transform(alias=alias, dialect=dialect, coalesce=coalesce)
        # SELECT
        stmt.select.append(sel)
        # WHERE
        if len(where) > 0:
            stmt.where.extend(where)
        # GROUP BY
        if has_agg and not o.has_aggregation:
            gby = re.sub('AS [a-zA-Z0-9_]+$', '', sel).strip()
            stmt.groupby.append(gby)

    return stmt.generate_statement(dialect=dialect)
