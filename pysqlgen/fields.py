import copy
from .utils import *
from .dbtree import *
from warnings import warn


class UserOption:
    """
    UserOptions may be thought of as elements of a drop-down menu for users
    to select when defining the query. Each option consists of its English
    name (for the menu), the SQL field or statement required to select it
    from the source table, a possible transformation and/or aggregation, a
    list of options for these, the underlying table as a SchemaNode, and
    the DBMetadata context for all of this.
    """
    def __init__(self, item, sql_item,
                 table, context,
                 transformations=[None], aggregations=[None],
                 default_transformation=None, default_aggregation=None,
                 field_alias=None, sql_where=None,
                 dimension_table=None, dim_where=None, perform_lkp=False, lkp_field=None,
                 verbose=True):
        assert isinstance(context, DBMetadata), "context is not a DBContext object"
        assert is_node(table, allow_custom=True), \
            "Please ensure the table is a SchemaNode object or the string 'custom'."
        assert node_isin_context(table, context, allow_custom=True), \
            f"Specified table '{table}' is not available in 'context'."
        assert in_list(default_transformation, transformations, allow_None=True),\
            "default transformation must be in list of possible transformations."
        assert in_list(default_aggregation, aggregations, allow_None=True), \
            "default aggregation must be in list of possible aggregations."
        assert node_isin_context(dimension_table, context, allow_None=True), \
            f"Specified dimension table '{table}' is not available in 'context'."

        assert all([(t is None) or (t.lower() in context.TRANSFORMATIONS)
                    for t in transformations]), \
            "Please ensure the transformations are a subset of (" + \
            ", ".join(context.TRANSFORMATIONS) + ")."
        assert all([(a is None) or (a.lower() in context.AGGREGATIONS)
                    for a in aggregations]), \
            "Please ensure the aggregations are a subset of (" + \
            ", ".join(context.AGGREGATIONS) + ")."

        if dimension_table is not None:
            sql_item_proc = rm_alias_placeholder(sql_item.strip())
            assert re.fullmatch('[A-Za-z0-9_.]+', sql_item_proc) is not None, \
                f"Field names (`sql_item`: {sql_item}) must be pure, not an " + \
                "expression if attaching a dimension table."

        if verbose:
            if sql_item.find('{alias') < 0:
                warn(f"No table alias placeholder found for {item} " + \
                     "(expecting e.g. '{alias}field_name')")
            if not (None in transformations):
                warn("None not found in transformations: therefore you will be " + \
                     "unable to select the raw field.")
            if not (None in aggregations):
                warn("None not found in aggregations: therefore you will be " + \
                     "unable to select the raw field.")

        self.item = item
        self.sql_item = sql_item
        self.field_alias = field_alias
        self.context = context
        self.transformations = [None if t is None else t.lower() for t in transformations]
        self.default_transformation = default_transformation
        self.selected_transform = default_transformation

        self.aggregations = [None if t is None else t.lower() for t in aggregations]
        self.default_aggregation = default_aggregation
        self.selected_aggregation = default_aggregation
        self.table = table if isinstance(table, SchemaNode) else table.title()
        self.dimension_table = dimension_table
        self._perform_lkp = perform_lkp
        self.sql_where = sql_where
        self.dim_where = dim_where

        if dimension_table is not None:
            if lkp_field is not None:
                self.lkp_field = lkp_field
            else:
                assert self.dimension_table.default_lkp is not None, \
                    f'{item} No `default_lkp` field in dimension table. You must supply' \
                    + ' a `lkp_field` in the UserOption arguments.'
                self.lkp_field = self.dimension_table.default_lkp

    def set_transform(self, t):
        # if t is not None:
        assert t in self.transformations, f'{t} is an invalid transformation. ' + \
                                          'Allowed=' + ','.join(self.transformations)
        self.selected_transform = t

    def set_aggregation(self, a):
        # if a is not None:
        assert a in self.aggregations, f'{a} is an invalid aggregation. Allowed=' + \
                                          ','.join(self.aggregations)
        self.selected_aggregation = a

    @property
    def has_transformation(self):
        return self.selected_transform is not None

    @property
    def default_transformation_ix(self):
        return self.transformations.index(self.default_transformation)

    @property
    def transformation_options(self):
        return [{'label': t, 'value': i} if t is not None else
                {'label': '<None>', 'value': i} for i, t in
                enumerate(self.transformations)]

    @property
    def transformation_is_disabled(self):
        if (len(self.transformations) == 1) and (self.transformations[0] is None):
            return True
        else:
            return False

    @property
    def has_aggregation(self):
        return self.selected_aggregation is not None

    @property
    def default_aggregation_ix(self):
        return self.aggregations.index(self.default_aggregation)

    @property
    def aggregation_options(self):
        return [{'label': t, 'value': i} if t is not None else
                {'label': '<None>', 'value': i} for i, t in
                enumerate(self.aggregations)]

    @property
    def aggregation_is_disabled(self):
        if (len(self.aggregations) == 1) and (self.aggregations[0] is None):
            return True
        else:
            return False

    @property
    def has_dim_lkp(self):
        return self.dimension_table is not None

    @property
    def perform_lkp(self):
        return self._perform_lkp

    @perform_lkp.setter
    def perform_lkp(self, val):
        if val is True:
            assert self.has_dim_lkp, "Cannot set `perform_lkp=True` - no dimension table!"
        self._perform_lkp = val

    def lkp_options(self):
        return [{'label': '', 'value': 1, 'disabled': self.has_dim_lkp}]

    def validate(self):
        if self.table == 'Custom':
            return self.item in self.context.custom_tables
        return True

    def get_table(self):
        if self.table == 'Custom':
            return '(\n' + self.context.custom_tables[self.item] + '\n)'
        return self.table

    def __copy__(self):
        obj = type(self).__new__(self.__class__)
        obj.__dict__.update(self.__dict__)
        return obj

    def copy(self):
        return copy.copy(self)

    def __repr__(self):
        return f'UserOption({hex(id(self))}, {self.item}, tf={self.selected_transform}, ' + \
            f'agg={self.selected_aggregation}, lkp={self.perform_lkp})'

    def __str__(self):
        return f'UserOption({self.item}, tf={self.selected_transform}, ' + \
            f'agg={self.selected_aggregation}, lkp={self.perform_lkp})'

    def sql_transform(self, alias=None, dialect="MSSS", coalesce=None):
        """
        A kind of look-up table for transformations such as 'MIN', 'AVG', 'YEAR',
        'IS NOT NULL'...

        This function creates the necessary transformation in the SELECT statement
        and possibly a WHERE clause too, hence the `sel`, `where` variables.

        The return value is the tuple (`sel`, `where`).

        """

        assert isinstance(self, UserOption), "opt is not a UserOption"

        where = []  # initialise empty where clause
        alias = '' if (alias is None or len(alias) == 0) else alias + '.'
        if not self.perform_lkp:
            name, table = self.sql_item, self.table
        else:
            name, table = self.lkp_field, self.dimension_table
            name = '{alias:s}' + name
            if self.dim_where is not None:
                dimension_where = self.dim_where.format(alias=alias)
                where.append(dimension_where)
        name = name.format(alias=alias)
        datefield = self.table.primary_date_field


        dialect = dialect.lower()
        assert dialect in ["msss",
                           "postgres"], "dialect must be 'MSSS' (SQL Server) or 'Postgres'"

        sel = f'{name}'
        # _____________________ TRANSFORMATION _________________________________________

        if self.has_transformation:
            t = self.selected_transform.lower().strip()
            if t == 'not null':
                sel = f'CASE WHEN {name:s} IS NOT NULL THEN 1 ELSE 0 END'
            elif t in ['day', 'month', 'year']:
                if dialect == 'msss':
                    sel = f'{t.upper()}({name:s})'
                elif dialect == 'postgres':
                    sel = f'EXTRACT({t.upper()} FROM {name:s})'
                else:
                    raise Exception("Unreachable Error")
            elif t == 'week':
                if dialect == 'msss':
                    sel = f'DATEADD({name:s}, (DATEDIFF({name:s}, 0, GETDATE()) / 7) * 7 + 7, 0)'
                elif dialect == 'postgres':
                    sel = f'{name:s} - CAST(EXTRACT(DOW FROM {name:s}) AS INT) + 1'
                else:
                    raise Exception("Unreachable Error")
            elif t == 'first':
                assert table.num_parents() < 2, "can only use 'first' on tables which join to the Person table."
                sel = f'{name:s}'
                where.append('ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY {datefield:s}) = 1')
            elif t == 'tens':
                if dialect == 'msss':
                    sel = f"CAST((({name}) / 10)*10 AS VARCHAR) + '-' +\n" + " "*10 + \
                          f"CAST((({name}) / 10)*10+9 AS VARCHAR)"
                elif dialect == 'postgres':
                    sel = f"CONCAT(CAST(({name} / 10)*10 AS VARCHAR), '-', CAST(({name} / 10)*10+9 AS VARCHAR))"
                else:
                    raise Exception("Unreachable Error")
            else:
                raise KeyError(f'sql_transform: Unknown transformation: {t:s}')

        # _____________________ AGGREGATION ____________________________________________
        if self.has_aggregation:
            a = self.selected_aggregation.lower().strip()
            if a == 'rows':
                sel = f'COUNT({sel})'
            elif a == 'count':
                sel = f'COUNT(DISTINCT {sel})'
            else:
                if self.verbose:
                    warn(f'Aggregation {a} is not explicitly plumbed in. ' +
                         f'Trying {a.upper()}')
                sel = '{:s}({:s})'.format(a.upper(), sel)

        # _____________________ Get field alias ________________________________________
        if self.field_alias is None:
            field_alias = str_to_fieldname(self.item)
            field_alias += '_id' if (self.has_dim_lkp and not self.perform_lkp) else ''
        else:
            field_alias = self.field_alias
        if self.has_aggregation and self.field_alias is None:
            field_alias = self.selected_aggregation.lower().strip() + '_' + field_alias

        # ________________ Coalesce with default (if left join) _______________________
        if coalesce is not None:
            sel = f"COALESCE({sel}, '{coalesce}')"

        sel += f' AS {field_alias}'
        return sel, where


class UserOptionCompound(UserOption):
    pass

# e.g. discharge type including death and C19 status.
