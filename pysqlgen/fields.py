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
    def __init__(self, item, sql_item, transformations, default_transformation, table,
                 context, field_alias=None, verbose=True):
        assert isinstance(table, SchemaNode) or (table.title() == "Custom"), \
            "Please ensure the table is a SchemaNode object or the string 'custom'."
        assert (table == 'custom') or (table in context.nodes), \
            f"Specified table '{table}' is not available in 'context'."
        assert (default_transformation is None) or \
               (default_transformation in transformations), \
            "default transformation must be in list of possible transformations."
        assert isinstance(context, DBMetadata), "context is not a DBContext object"

        if verbose and (sql_item.find('{alias') < 0):
            warn(f"No table alias placeholder found for {item} " + \
                 "(expecting e.g. '{alias}field_name')")

        self.item = item
        self.sql_item = sql_item
        self.field_alias = field_alias
        self.context = context
        self.transformations = [None if t is None else t.lower() for t in transformations]
        self.default_transformation = default_transformation
        self.table = table if isinstance(table, SchemaNode) else table.title()
        self._set_transform_type(default_transformation)
        self.selected_transform = default_transformation

    def _set_transform_type(self, t):
        self.is_transformation, self.is_aggregation = False, False
        if t in self.context.TRANSFORMATIONS:
            self.is_transformation = True
        elif t in self.context.AGGREGATIONS:
            self.is_aggregation = True

    def select_transform(self, t):
        if t is not None:
            assert t in self.transformations, f'{t} is invalid. Allowed=' + \
                                              ','.join(self.transformations)
        self._set_transform_type(t)
        self.selected_transform = t

    def validate(self):
        if self.table == 'Custom':
            return self.item in self.context.custom_tables
        return True

    def get_table(self):
        if self.table == 'Custom':
            return '(\n' + self.context.custom_tables[self.item] + '\n)'
        return self.table


class UserOptionAggregation(UserOption):

    def __init__(self, item, sql_item, transformations, default_transformation, table,
                 context, field_alias=None):
        # assert all([t.lower() in context.AGGREGATIONS for t in transformations]), \
        #     "Please ensure the transformations are a subset of (" + \
        #     ", ".join(context.AGGREGATIONS) + ")."
        super().__init__(item, sql_item, transformations, default_transformation, table,
                         context, field_alias=field_alias)


class UserOptionSplit(UserOption):

    def __init__(self, item, sql_item, table, context, field_alias=None, sql_where=None,
                 transformations=[], default_transformation=None, as_english=False):
        assert all([t.lower() in context.TRANSFORMATIONS for t in transformations]), \
            "Please ensure the transformations are a subset of (" + \
            ", ".join(context.TRANSFORMATIONS) + ")."
        super().__init__(item, sql_item, transformations, default_transformation, table,
                         context, field_alias=field_alias)
        self.sql_where = sql_where
        self.as_english = as_english


class UserOptionCompound(UserOption):
    pass

# e.g. discharge type including death and C19 status.


def sql_transform(opt, alias=None, dialect="MSSS"):
    """
    A kind of look-up table for transformations such as 'MIN', 'AVG', 'YEAR',
    'IS NOT NULL'...

    This function creates the necessary transformation in the SELECT statement
    and possibly a WHERE clause too, hence the `sel`, `where` variables.

    The return value is the tuple (`sel`, `where`).

    """

    assert isinstance(opt, UserOption), "opt is not a UserOption"
    alias = '' if (alias is None or len(alias) == 0) else alias + '.'

    name, table, datefield = opt.sql_item, opt.table, opt.table.primary_date_field
    name_no_tbl_alias = name.format(alias='')
    name = name.format(alias=alias)

    if opt.selected_transform is None:
        sel = f'{name}' if opt.field_alias is None else f"{name} AS {opt.field_alias}"
        # if opt.as_english:
        #     sel = f'{name}' if opt.field_alias is None else f"{name} AS {opt.field_alias}"
        # else:
        #     sel = f'{name}' if opt.field_alias is None else f"{name} AS {opt.field_alias}"
        return sel, ''

    dialect = dialect.lower()
    assert dialect in ["msss", "postgres"], "dialect must be 'MSSS' (SQL Server) or 'Postgres'"

    where = ''
    t = opt.selected_transform.lower().strip()
    field_alias = opt.field_alias if opt.field_alias is not None else name_no_tbl_alias
    if opt.is_aggregation:
        sel = "{:s}({:s}) AS {:s}".format(t.upper(), name, field_alias)
    elif opt.is_transformation:
        if t == 'not null':
            sel = f'CASE WHEN {name:s} IS NOT NULL THEN 1 ELSE 0 AS {field_alias}'
        elif t in ['day', 'month', 'year']:
            if dialect == 'msss':
                sel = f'{t.upper()}({name:s}) AS {field_alias}'
            elif dialect == 'postgres':
                sel = f'EXTRACT({t.upper()} FROM {name:s}) AS {field_alias}'
            else:
                raise Exception("Unreachable Error")
        elif t == 'week':
            if dialect == 'msss':
                sel = f'DATEADD({name:s}, (DATEDIFF({name:s}, 0, GETDATE()) / 7) * 7 + 7, 0)'\
                        + f' AS {field_alias}'
            elif dialect == 'postgres':
                sel = f'{name:s} - CAST(EXTRACT(DOW FROM {name:s}) AS INT) + 1 AS'\
                        + f' {field_alias}'
            else:
                raise Exception("Unreachable Error")
        elif t == 'first':
            assert table.num_parents() < 2, "can only use 'first' on tables which join to the Person table."
            sel = f'ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY {datefield:s}) AS rn, {name:s}'
            where = 'rn = 1'
        else:
            raise KeyError(f'sql_transform: Unknown transformation: {t:s}')
    else:
        raise KeyError(f'Transformation not in list: {t:s}')

    return sel, where
