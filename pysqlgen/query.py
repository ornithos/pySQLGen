from collections import UserList, OrderedDict
from .dbtree import topological_sort
from .fields import UserOption
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
        self.context = context
        self._from = StmtFrom(parent=self)
        self.select = StmtGeneric('SELECT', parent=self)
        self.where = StmtGeneric('WHERE', parent=self, conj=' AND', wrappers=('(', ')'))
        self.groupby = StmtGeneric('GROUP BY', parent=self)

    def generate_statement(self):
        return self.select.generate_statement() + \
               self._from.generate_statement() + \
               self.where.generate_statement() + \
               self.groupby.generate_statement()


class StmtGeneric(UserList):
    """
    StmtGeneric is a list-like class which is expected to sit within
    a larger Statement object. The object admits a `generate_statement`
    method which will generate a SELECT, WHERE or GROUP BY statement
    (for example) from the list of items it contains. However, due to
    the complexities of JOINs, the FROM statement uses a different class.
    """

    def __init__(self, statement_type, parent, conj=None, wrappers=None):
        super().__init__()
        self.statement = statement_type
        self.parent = parent
        self.conj = ',' if conj is None else conj
        self.wrappers = wrappers

    def generate_statement(self):
        if len(self) == 0:
            return ''
        length = len(self.statement) + 1
        lines = list(self)
        if self.wrappers:
            lines = [self.wrappers[0] + l + self.wrappers[1] for l in lines]
        return f'{self.statement} ' + \
               (self.conj + '\n' + ' ' * length).join(lines) + '\n\n'


class StmtFrom(OrderedDict):
    """
    StmtFrom serves a similar purpose to StmtGeneric but subclasses a
    dictionary in order to store both the tables and the join conditions.
    """

    def __init__(self, parent):
        super().__init__()
        self.parent = parent
        self.generated = None

    def __setitem__(self, key, value):
        if key in self.keys():
            if self[key] != value:
                raise Exception(f'Trying to place\n {key}: {value} \n\nin the FROM' + \
                                f' dict, but\n {key}: {self[key]} \n\nalready exists!')
        else:
            super().__setitem__(key, value)

    def generate_statement(self):
        if self.generated is None:
            nodes = topological_sort(list(self.keys()))
            n = len(nodes)
            join_list = []
            schema = self.parent.context.schema
            schema = '' if schema == '' else schema + '.'
            aliases = self.parent.aliases
            for i, node in enumerate(nodes):
                v = self[node]
                if i == 0:
                    assert len(v) == 0, "first table should not have a join condition"
                    alias = '' if n == 1 else node.name[0].lower()
                    aliases[node] = alias
                    join_list.append(f'     {schema}{node.name} {alias}')
                else:
                    # make human readable alias (using table prefix, not simply a,b,c,...)
                    alias = node.name[:1].lower()
                    a_ix = 0
                    while alias in aliases.values():
                        a_ix, a_str = (a_ix + 1), ('' if a_ix == 0 else str(a_ix))
                        alias = ''.join([n[0].lower() for n in node.name.split("_")]) \
                                + a_str
                    aliases[node] = alias
                    a = (aliases[v[0][0]], aliases[v[1][0]])  #  join aliases
                    c = (v[0][1], v[1][1])  # join columns
                    join_list.append(f'LEFT JOIN {schema}{node.name} {alias}\nON        '
                                     + f'{a[0]}.{c[0]} = {a[1]}.{c[1]}')
            self.generated = 'FROM ' + '\n'.join(join_list) + '\n\n'
        return self.generated


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

    # ==== CONSTRUCT JOINS ==================
    nodes = [o.get_table() for o in args]
    unique_nodes = list(set(nodes))
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

    # Generate statement in order to populate aliases dict
    stmt._from.generate_statement();

    # === CONSTRUCT SELECT / WHERE ===========
    has_agg = any([arg.has_aggregation for arg in args])

    for arg in args:
        alias = stmt.aliases[arg.get_table()]
        sel, where = arg.sql_transform(alias=alias, dialect=dialect)
        # SELECT
        stmt.select.append(sel)
        # WHERE
        if len(where) > 0:
            stmt.where.append(where)
        # GROUP BY
        if has_agg and not arg.has_aggregation:
            gby = re.sub('AS [a-zA-Z0-9_]+$', '', sel).strip()
            stmt.groupby.append(gby)

    return stmt.generate_statement()
