import copy
import yaml
from warnings import warn
from .utils import *
from .dbtree import *


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
                 field_alias=None, sql_where=None, is_secondary=False,
                 dimension_table=None, dim_where=None, perform_lkp=False, lkp_field=None,
                 verbose=True):
        assert isinstance(context, DBMetadata), "context is not a DBContext object"
        assert is_node(table, allow_custom=True), \
            "Please ensure the table is a SchemaNode object or the string 'custom'."
        assert node_isin_context(table, context, allow_custom=True), \
            f"Specified table '{table}' is not available in 'context'."
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
        self._field_alias = field_alias
        self.sql_where = sql_where
        self.is_secondary = is_secondary
        self.context = context
        self.verbose = verbose

        self.transformations = [None if t is None else t.lower() for t in transformations]
        _def_trans = default_transformation if default_transformation in \
                                               self.transformations \
                                            else self.transformations[0]
        self.default_transformation = _def_trans
        self.selected_transform = None  # to keep linter happy
        self.set_transform(_def_trans)

        self.aggregations = [None if t is None else t.lower() for t in aggregations]
        self.default_aggregation = default_aggregation
        self.selected_aggregation = None  # to keep linter happy
        self.set_aggregation(default_aggregation)

        self.table = table if isinstance(table, SchemaNode) else table.title()
        self.dimension_table = dimension_table
        self._perform_lkp = perform_lkp
        self.dim_where = dim_where
        self.coalesce = None

        if dimension_table is not None:
            if lkp_field is not None:
                self.lkp_field = lkp_field
            else:
                assert self.dimension_table.default_lkp is not None, \
                    f'{item} No `default_lkp` field in dimension table. You must supply' \
                    + ' a `lkp_field` in the UserOption arguments.'
                self.lkp_field = self.dimension_table.default_lkp

    def set_transform(self, t, force=False):
        # if self.transformations is None:
        #     raise Exception(f"Cannot apply transform: None are allowed for {self.item}")
        if not force:
            assert t in self.transformations, f'{t} is an invalid transformation. ' + \
                                          'Allowed=' + \
                                          ','.join([str(s) for s in self.transformations])
        self.selected_transform = t

    def set_aggregation(self, a, force=False):
        # if self.aggregations is None:
        #     raise Exception(f"Cannot apply aggregation: None allowed for {self.item}")
        if not force:
            assert a in self.aggregations, f'{a} is an invalid aggregation. Allowed=' + \
                                             ','.join([str(s) for s in self.aggregations])
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

    @property
    def field_alias(self):
        return self._field_alias_logic(will_perform_lkp=self.perform_lkp)

    @field_alias.setter
    def field_alias(self, value):
        self._field_alias = value

    def _field_alias_logic(self, will_perform_lkp=True, depend_agg=True):
        if self._field_alias is None:
            field_alias = str_to_fieldname(self.item)
            field_alias += '_id' if (self.has_dim_lkp and not will_perform_lkp) else ''
        else:
            field_alias = self._field_alias
        if depend_agg and self.has_aggregation and self._field_alias is None:
            agg_prefix = self.selected_aggregation.lower().strip()
            # Transform max->has in case of CASE WHEN statements
            if agg_prefix == 'max':
                field_stmt = self.sql_fieldname
                if len(field_stmt) > 3 and field_stmt[:4].lower() == 'case':
                    agg_prefix = 'has'
            agg_prefix = self.context.agg_alias_lkp.get(agg_prefix, agg_prefix)
            field_alias = agg_prefix + '_' + field_alias
        return field_alias

    @property
    def sql_fieldname(self):
        return rm_alias_placeholder(self.sql_item)

    def __copy__(self, set_item_name=None):
        obj = type(self).__new__(self.__class__)
        obj.__dict__.update(self.__dict__)
        if set_item_name:
            obj.item = set_item_name
        return obj

    def copy(self, set_item_name=None):
        return self.__copy__(set_item_name=set_item_name)

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
                    sel = f'{t.upper():s}({name:s})'
                elif dialect == 'postgres':
                    sel = f'EXTRACT({t.upper():s} FROM {name:s})'
                else:
                    raise Exception("Unreachable Error")
            elif t == 'week':
                if dialect == 'msss':
                    sel = f'DATEADD({name:s}, (DATEDIFF({name:s}, 0, GETDATE()) / 7) * 7 + 7, 0)'
                elif dialect == 'postgres':
                    sel = f'{name:s} - CAST(EXTRACT(DOW FROM {name:s}) AS INT) + 1'
                else:
                    raise Exception("Unreachable Error")
            elif t in ['hour', 'weekday']:
                if dialect == 'msss':
                    sel = f'DATEPART({t.upper():s}, {name:s})'
                elif dialect == 'postgres':
                    tform = t if t != 'weekday' else 'dow'
                    sel = f'EXTRACT({tform.upper():s} FROM {name:s}) AS INT) + 1'
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

        # ________________ Coalesce with default (if left join) _______________________
        if coalesce is not None:
            coalesce = f"'{coalesce}'" if isinstance(coalesce, str) else str(coalesce)
            sel = f"COALESCE({sel}, {coalesce})"

        if self._field_alias != '':
            sel += f' AS {self.field_alias}'
        return sel, where


class UserOptionCompound(UserOption):
    pass

# e.g. discharge type including death and C19 status.


def read_all_fields_from_yaml(filename, context, tbl_lkp, dim_lkp_where=None):
    with open(filename, "r") as f:
        fields_data = yaml.load(f, Loader=yaml.CLoader)

    all_fields = dict()
    for tbl_nm, tbl in tbl_lkp.items():
        items_in_tbl = fields_data.get(tbl_nm, [])
        if len(items_in_tbl) == 0:
            # No items for Table in YAML file.
            continue
        for field_nm, payload in items_in_tbl.items():
            if (len(payload) == 5) and payload[4] is not None:
                # IGNORE
                continue
            stmt = payload[0]
            has_transforms = (len(payload) > 1) and (payload[1] is not None)
            transformations = payload[1] if has_transforms else [None]
            has_aggregations = (len(payload) > 2) and (payload[2] is not None)
            aggregations = payload[2] if has_aggregations else [None]
            def_agg = None if None in aggregations else aggregations[0]

            if len(payload) > 3:
                lkp_tbl, lkp_def, lkp_where = payload[3]
                lkp_tbl = tbl_lkp[lkp_tbl]
                if lkp_where is not None and lkp_where[0] == '$':
                    assert dim_lkp_where is not None, "dim_lkp_where must be specified."
                    lkp_where = dim_lkp_where[lkp_where[1:]]
            else:
                lkp_tbl, lkp_def, lkp_where = None, False, None
            field = UserOption(field_nm, stmt, tbl, context,
                               transformations=transformations,
                               aggregations=aggregations,
                               dimension_table=lkp_tbl,
                               perform_lkp=lkp_def,
                               dim_where=lkp_where,
                               default_aggregation=def_agg)
            all_fields[field_nm] = field

    return all_fields


def construct_simple_field(name, table, context, is_secondary=True):
    return UserOption(name, '{alias}'+name, table, context,
                      transformations=[None],
                      aggregations=[None],
                      dimension_table=None,
                      perform_lkp=False,
                      dim_where=None,
                      is_secondary=is_secondary)
