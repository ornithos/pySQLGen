class DBMetadata:
    """
    DBMetadata: storage for the DB graph ('nodes'), any custom table SQL, and
    the lists of allowed AGGREGATIONS and TRANSFORMATIONS.
    """
    def __init__(self, nodes, custom_tables, schema, AGGREGATIONS, TRANSFORMATIONS):
        self.nodes = nodes
        self.custom_tables = custom_tables
        self.AGGREGATIONS = AGGREGATIONS
        self.TRANSFORMATIONS = TRANSFORMATIONS
        self.schema = schema


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
    def __init__(self, name, parent, pk, children, fks, datefield):
        self.name = name
        self.primary_date_field = datefield
        self.parent = parent
        self.children = children
        self.pk = pk
        self.fks = fks

    def __repr__(self):
        return f'{self.name} Table <SchemaNode with parent {self.parent}>'

    def __str__(self):
        return f'{self.name} Table'

    def num_parents(self):
        return 0 if self.parent is None else self.parent.num_parents() + 1

    def common_key(self, node_to):
        parent_keys = [node_to.pk, *node_to.fks]
        if self.pk in parent_keys:
            return self.pk
        else:
            for fk in self.fks:
                if fk in parent_keys:
                    return fk
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


def topological_sort(nodes, return_perm=False):
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

