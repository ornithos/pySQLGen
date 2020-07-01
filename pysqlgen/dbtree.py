import queue
import math
from collections import OrderedDict
from warnings import warn
from .utils import str_to_fieldname, rm_alias_placeholder
from .graph import Graph

class DBMetadata:
    """
    DBMetadata: storage for the DB graph ('nodes'), any custom table SQL, and
    the lists of allowed AGGREGATIONS and TRANSFORMATIONS.
    """
    def __init__(self, nodes, custom_tables, schema, AGGREGATIONS, TRANSFORMATIONS,
                 coalesce_default='Unknown', agg_alias_lkp=None):

        # Calculate children
        for node in nodes:
            for p in node.parents:
                p.children.append(node)
        for node in nodes:
            node.children = list(set(node.children))

        self.nodes = nodes
        self.custom_tables = custom_tables
        self.AGGREGATIONS = AGGREGATIONS
        self.TRANSFORMATIONS = TRANSFORMATIONS
        self.schema = schema
        self.coalesce_default = coalesce_default
        self.agg_alias_lkp = agg_alias_lkp if not None else dict()


class SchemaNode:
    """
    SchemaNode: a node of the DB Schema graph. Each node keeps track of a
    number of properties of a table, such as the name, parents/children,
    the primary date field (e.g. if a query wants the 'first' of something)
    and the primary and foreign keys.

    A number of useful methods are defined, such as the `num_parents`: the
    number of parents of the node within the graph before hitting the root,
    `common_key`, which takes another node as an argument and finds a
    common key within the primary and foreign keys (if any), and
    `traverse_to_ancestor` which finds a path between any two nodes via the
    first common ancestor.
    """
    def __init__(self, name, parents, pk, fks, datefield,
                 default_lkp=None, schema=None, children=None):
        assert isinstance(parents, list), "parents must be a list of nodes"
        self.name = name
        self.parents = parents
        self.children = children if children is not None else []
        self.pk = pk
        self.fks = fks
        self.primary_date_field = datefield
        self.default_lkp = default_lkp     # if used as a Dimension table
        self.schema = schema
        self.is_cte = False

    def __repr__(self):
        return f'{self.name} Table <SchemaNode with parent(s) ' + \
               f'{[p.__str__() for p in self.parents]}>'

    def __str__(self):
        return f'{self.name} Table'

    def num_parents(self):
        """
        Calculates the number of parents above the node (only using the *FIRST* parent
        in the `.parents` list, in the case there are > 1).
        """
        return 0 if len(self.parents) == 0 else self.parents[0].num_parents() + 1

    def parent_rank(self, parent):
        try:
            return self.parents.index(parent)
        except ValueError:
            raise Exception(f"parent_rank: cannot find parent {parent} in {self}.")

    def common_keys(self, node_to):
        parent_keys = set(node_to.pk + node_to.fks)
        pk_intersect = list(filter(lambda x: x in parent_keys, self.pk))
        if len(pk_intersect) > 0:
            return pk_intersect
        fk_intersect = list(filter(lambda x: x in parent_keys, self.fks))
        if len(fk_intersect) > 0:
            return fk_intersect
        raise RuntimeError(f'No common keys between {self.name} and {node_to.name}.')

    def traverse_to_ancestor(self, b, internal=False):
        assert isinstance(b, SchemaNode)
        a = self
        if a == b:
            return [(a, b)]
        n_a, n_b = a.num_parents(), b.num_parents()
        if n_a > n_b:
            add_to_list, a = (a, a.parent), a.parent
        else:
            add_to_list, b = (b, b.parent), b.parent
        out = a.traverse_to_ancestor(b, internal=True)
        out.append(add_to_list)
        return out if internal else (out[0][0], out[1:])

    def is_leaf(self, directed=True):
        if directed:
            return len(self.children) == 0
        else:
            edges = len(self.children) + len(self.parents)
            (edges >= 1) or warn(str(self) + " is an orphan node")
            return edges == 1

    def __copy__(self):
        obj = type(self).__new__(self.__class__)
        obj.__dict__.update(self.__dict__)
        return obj

    def copy(self):
        return self.__copy__()


class CTENode(SchemaNode):
    def __init__(self, parents, pk, fields, default_lkp=None, children=None):
        assert isinstance(parents, list), "parents must be a list of nodes"
        assert isinstance(fields, list), "fields must be a list of UserOptions"

        agg_fields = [x.item for x in fields if x.has_aggregation]
        name = '_'.join([str_to_fieldname(x) for x in agg_fields]) + '_agg'

        self.name = name
        self.parents = parents
        self.pk = pk
        self.children = children if children is not None else []
        self.fields = fields
        self.primary_date_field = None
        self.default_lkp = default_lkp     # if used as a Dimension table
        self.schema = ''
        self.is_cte = True

    @property
    def fks(self):
        return [f.sql_fieldname for f in self.fields]

    def __repr__(self):
        return f'{self.name} Table <CTENode with parent(s) {self.parents}>'

    def __str__(self):
        return f'{self.name} Table with fields: {[x.item for x in self.fields.keys()]}'

    def __copy__(self):
        obj = type(self).__new__(self.__class__)
        obj.__dict__.update(self.__dict__)
        return obj

    def copy(self):
        return self.__copy__()


def topological_sort_hierarchical(nodes, return_perm):
    """
    This MASSIVELY takes advantage of an assumed star schema.
    If the schema is a more complex DAG, a more general
    algorithm must be used
    (https://en.wikipedia.org/wiki/Topological_sorting).

    We only need to ensure that the nodes are sorted in terms
    of the *level* (in the BFS sense).
    """
    level = [node.num_parents() for node in nodes]
    sort_perm = sorted(range(len(level)), key=lambda k: level[k])
    return [nodes[i] for i in sort_perm] if not return_perm else sort_perm


def breadthfirstsearch(A, B):
    """
    Perform BFS to find shortest path from v -> w through nodes defined by "nodes"
    """

    Q = queue.Queue()
    Q.put(A)
    path_exist = {A: None}    # mark visited, and capture the preceding node

    while not Q.empty():
        v = Q.get()
        if v is B:
            # Success
            path = [v]
            while path_exist[v] is not None:
                v = path_exist[v]
                path.append(v)
            return path
        for w in [*v.parents, *v.children]:
            if w not in path_exist:
                path_exist[w] = v
                Q.put(w)


def minimum_subtree(nodes):
    """
    The goal of this function is to return the subtree of minimum size which contains
    all of the nodes specified in the argument. This is a graph Steiner Tree problem which
    is NP-Hard, so here we use a heuristic approach.
    """
    assert len(nodes) > 0, "require a non-empty list of nodes"
    if len(nodes) == 1:
        return {nodes[0]: ()}

    levels = [node.num_parents() for node in nodes]
    level_lkp = {}                        # level --> nodes lookup
    for (node, level) in zip(nodes, levels):
        level_lkp[level] = level_lkp.get(level, []) + [node]
    unique_levels = sorted(level_lkp.keys())

    result = OrderedDict()
    for level in unique_levels:
        c_nodes = level_lkp[level]
        for node in c_nodes:
            if len(result) == 0:
                result[node] = ()
            else:
                # find shortest path to `result` (existing tree) =: P
                existing_vertices = list(result.keys())
                shortest_path = (math.inf, None, None)
                for destination in existing_vertices:
                    path = breadthfirstsearch(node, destination)
                    if path is None:
                        continue
                    dist = len(path) - 1
                    takes_precedence = ((shortest_path[2] is None) or
                                        (shortest_path[2] not in node.parents) or
                                        (node.parent_rank(destination) <
                                         node.parent_rank(shortest_path[2])))
                    if dist <= shortest_path[0] and takes_precedence:
                        shortest_path = (dist, path, node)
                P = shortest_path[1]   # first entry will already exist in `result`.

                # for each edge in P, find common keys and add to join dict.
                for (v, w) in zip(P[:-1], P[1:]):
                    keys = v.common_keys(w)
                    result[w] = ((v, keys), (w, keys))
    return result


def is_node(x, allow_custom=False):
    if isinstance(x, SchemaNode):
        return True
    elif allow_custom and isinstance(x, str):
        return x.lower() == "custom"
    else:
        return False
