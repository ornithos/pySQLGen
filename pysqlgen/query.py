from collections import UserList, OrderedDict
from .dbtree import topological_sort, CTENode
from .fields import UserOption
from .utils import rm_alias_placeholder, make_unique_name, flatten
import re

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
        for node in tables:
            # construct inner CTE query (and add margin)
            q = construct_query(*node.fields, dialect=dialect).strip()
            q = "\n".join([" "*self.whitespace + line for line in q.split("\n")])
            # construct outer part of CTE query, and concatenate result to `out`.
            fields = [x.field_alias for x in node.fields]
            cte = f'{node.name} ({", ".join(fields)}) AS (\n{q}\n)'
            out.append(cte)
        return f'{self.statement} ' + ("\n" + self.conj).join(out) + '\n\n'


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
            nodes = topological_sort(list(self.keys()))
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
                    join_list.append(f'LEFT JOIN {schema}{node.name} {alias}\nON        '
                                     + f'{a[0]}.{c[0]} = {a[1]}.{c[1]}')
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


def construct_query(*args, dialect='MSSS'):
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

    # ==== TRANSFORM SECONDARY AGGREGATIONS INTO CTEs =============
    has_secondary = any([arg.is_secondary for arg in args])
    if has_secondary:
        primary = list(filter(lambda x: not x.is_secondary, args))
        assert len(primary) == 1, "There must be exactly one primary field."
        primary = primary[0]
        ctes = []
        for arg in args:
            if arg.is_secondary and arg.has_aggregation:
                arg_copy = arg.copy()
                primary_copy = primary.copy()
                # process arg: strip of CTE-making recursion
                arg_copy.is_secondary = False
                # process primary: strip of transformation / aggregation (done outside)
                primary_copy.set_transform(None)
                primary_copy.set_aggregation(None)
                primary_copy.field_alias = rm_alias_placeholder(primary_copy.sql_item)
                # create CTE
                print(arg_copy)
                print(primary_copy)
                cte_fields = [arg_copy, primary_copy]
                cte = CTENode(primary_copy.table,
                              primary_copy.field_alias,
                              cte_fields)
                print(cte.pk)
                print(cte.fks)
                ctes.append(cte)
                # replace original arg with references to CTE
                arg.sql_item = '{alias}' + arg_copy.field_alias
                arg.table = cte
                arg.field_alias = ''
                arg.set_aggregation(None)
                arg.set_transform(None)

    # ==== GET ALL TABLES AND COMMON TABLE EXPRESSIONS ============
    nodes = [o.get_table() for o in args]
    unique_nodes = list(set(nodes))
    stmt.ctes.extend(unique_nodes)  # CTE statement filters for CTEs.

    # ==== CONSTRUCT JOINS ==================
    if len(unique_nodes) == 1:
        tbl = args[0].get_table()
        stmt._from[tbl] = ()
    else:
        # We have in general something like a Steiner Tree Problem (NP-hard)
        # to solve here, but for most schemas, a brute force approach
        # like the below will work absolutely fine.
        #
        # Note that when the same key is present in many tables, the
        #  intermediate joins up a tree may be unnecessary. In general, without
        # consideration of the constraints, I think this is the safest thing
        # to but the query optimiser will perform join elimination if it has
        # access to the constraints
        node_order = topological_sort(unique_nodes, return_perm=True)
        for ix_prev, ix in zip(node_order[:-1], node_order[1:]):
            node_prev, node = unique_nodes[ix_prev], unique_nodes[ix]
            ancestor, path = node_prev.traverse_to_ancestor(node)
            if len(stmt._from) == 0:
                stmt._from[ancestor] = ()
            for edge in path:
                key = edge[0].common_key(edge[1])
                stmt._from[edge[0]] = ((edge[0], key), (edge[1], key))

    # Add dimension tables (if requested to joins)
    lkp_joins = []
    for o in args:
        if o.perform_lkp:
            dtbl = o.dimension_table
            assert not dtbl.is_cte, "Currently unable to support CTEs for dimension " + \
                                    "tables. (But it only requires thinking about how " + \
                                    "to avoid multiple copies.)"
            fk = rm_alias_placeholder(o.sql_item)
            lkp_joins.append(((o.table, fk), (dtbl, dtbl.pk)))

    # Generate statement in order to populate aliases dict
    stmt._from.generate_basic_statement(force_alias=(len(lkp_joins) > 0))
    stmt._from.add_lookups_to_statement(lkp_joins)

    # === CONSTRUCT SELECT / WHERE ===========
    has_agg = any([arg.has_aggregation for arg in args])

    for o in args:
        # Get table alias for field, depending on whether has lookup table or not
        if not o.perform_lkp:
            alias = stmt.aliases[o.get_table()]
            coalesce = None
        else:
            dtbl = o.dimension_table
            fk = rm_alias_placeholder(o.sql_item)
            alias = stmt.lkp_aliases[((o.table, fk), (dtbl, dtbl.pk))]
            coalesce = args[0].context.coalesce_default

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
