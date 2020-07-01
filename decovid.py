import yaml, json
from collections import OrderedDict
from pysqlgen.dbtree import *
from pysqlgen.fields import *
from pysqlgen.query import construct_query

# ########################## OBJECTS REFLECTING DATABASE ##############################
# ~~~~~~~~~~~~~~~~~~~~ Define Schema ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Person = SchemaNode('Person', [], ['person_id'],
                    ['person_id'], None)
Visit_Occurrence = SchemaNode('Visit_Occurrence', [Person],
                              ['visit_occurrence_id', 'person_id'],
                              [], 'visit_start_datetime')
Visit_Detail = SchemaNode('Visit_Detail', [Visit_Occurrence, Person],
                          ['visit_detail_id', 'visit_occurrence_id', 'person_id'],
                          ['care_site_id'], 'visit_start_date')
Care_Site = SchemaNode('Care_Site', [Visit_Detail], ['care_site_id'], [], None,
                       default_lkp='care_site_name')
Death = SchemaNode('Death', [Person], ['person_id'], [], 'death_date')
Measurement = SchemaNode('Measurement', [Visit_Detail, Visit_Occurrence, Person],
                         ['person_id', 'visit_occurrence_id', 'visit_detail_id'],
                         [], 'measurement_datetime')
Drug_Exposure = SchemaNode('Drug_Exposure', [Visit_Detail, Visit_Occurrence, Person],
                           ['person_id', 'visit_occurrence_id', 'visit_detail_id'],
                           [], 'drug_exposure_start_datetime')

Concept = SchemaNode('Concept', [], ['concept_id'], ['concept_id'], None,
                     default_lkp='concept_name')
nodes = [Person, Visit_Detail, Care_Site, Visit_Occurrence, Death, Measurement,
         Drug_Exposure, Concept]
node_lkp = {n.name: n for n in nodes}
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# ~~~~~~~~~~~~~~~~~~~~ Custom Tables ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
custom_admission_date = """
SELECT person_id,
       MIN(visit_start_datetime) AS admission_date
       
FROM   {schema}.Visit_Occurrence
GROUP BY person_id
"""
custom_tables = dict()
custom_tables['first admission date'] = custom_admission_date
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Construct metadata
AGGREGATIONS = ['rows', 'count', 'avg', 'sum', 'max', 'min', 'first', 'last']
TRANSFORMATIONS = ['not null', 'day', 'week', 'month', 'tens']
schema = 'public'   # name of schema within DB

agg_name_alias = dict(rows='num')  # For automatically constructed field names
coalesce_default = 'Unknown'       # 'NULL' value representation.
context = DBMetadata(nodes, custom_tables, schema, AGGREGATIONS, TRANSFORMATIONS,
                     coalesce_default=coalesce_default, agg_alias_lkp=agg_name_alias)

# ############################ GET ALL QUERY FIELDS #####################################

dim_lkp_where = dict()
dim_lkp_where['standard'] = "{alias:s}standard_concept = 'S'"

# Read all field definitions (incl transforms/aggs/lkps) from YAML file.
all_fields = read_all_fields_from_yaml("db_fields.yaml", context, tbl_lkp=node_lkp,
                                       dim_lkp_where=dim_lkp_where)


# Create options for primary variable
# -------------------------------------------------------------------------

opts_primary = (
    all_fields['person_id'].copy(set_item_name='person'),
    all_fields['measurement_type'].copy(set_item_name='measurement type')
)


# Create options for secondary variables
# -------------------------------------------------------------------------

opts_secondary = [
    all_fields['age'].copy(),
    all_fields['sex'].copy(),
    all_fields['race'].copy(),
    all_fields['visit_type'].copy(set_item_name='visit type'),
    all_fields['admission_type'].copy(set_item_name='admission type'),
    all_fields['visit_start_date'].copy(set_item_name='visit start date'),
    all_fields['length_of_stay_visit'].copy(set_item_name='length of stay (visit)'),
    all_fields['length_of_stay_detail'].copy(set_item_name='length of stay (detail)'),
    all_fields['care_site'].copy(set_item_name='care site'),
    all_fields['death'],
    all_fields['measurement_type'].copy(set_item_name='measurement type'),
]


# ############################## DEFAULTS ###############################################

default_transformations = dict()
default_aggregations = dict()
default_transformations['death'] = ['week', 'secondary']

for i, opts in enumerate([opts_primary, opts_secondary]):
    for opt in opts:
        if opt.item in default_transformations:
            trans = default_transformations[opt.item]
            if trans[1] != ['primary', 'secondary'][i]:
                continue
            opt.set_transform(trans[0])
        if opt.item in default_aggregations:
            agg = default_aggregations[opt.item]
            if agg[1] != ['primary', 'secondary'][i]:
                continue
            opt.set_aggregation(agg[0])


# ############################## STANDARD QUERIES ########################################


with open("standard_queries.json", 'r') as f:
    standard_queries = json.JSONDecoder(object_pairs_hook=OrderedDict).decode(f.read())


if __name__ == "__main__":
    agg_opt = opts_primary[0]
    agg_opt.set_aggregation('count')
    _opts = opts_secondary[6:8]
    _opts[0].set_aggregation('avg')
    _opts.append(opts_secondary[-3])
    for o in _opts:
        o.is_secondary=True
    tmp = construct_query(agg_opt, *_opts)
    print(tmp)
