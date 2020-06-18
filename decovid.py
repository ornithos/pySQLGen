import yaml, json
from collections import OrderedDict
from pysqlgen.dbtree import *
from pysqlgen.fields import UserOption

# ########################## OBJECTS REFLECTING DATABASE ##############################
# ~~~~~~~~~~~~~~~~~~~~ Define Schema ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Person = SchemaNode('Person', None, 'person_id',
                    ['Visit_Detail', 'Visit_Occurrence',
                     'Death', 'Measurement'],
                    ['person_id'], None)
Visit_Detail = SchemaNode('Visit_Detail', Person, 'visit_detail_id',
                          ['Care_Site'],
                          ['care_site_id', 'person_id'],
                          'visit_start_date')
Care_Site = SchemaNode('Care_Site', Visit_Detail, 'care_site_id', [], [], None,
                       default_lkp='care_site_name')
Visit_Occurrence = SchemaNode('Visit_Occurrence', Person, 'visit_occurrence_id',
                              [], ['person_id'],
                              'visit_start_datetime')
Death = SchemaNode('Death', Person, 'person_id', [], [], 'death_date')
Measurement = SchemaNode('Measurement', Person, 'person_id', [], [],
                         'measurement_datetime')

Concept = SchemaNode('Concept', None, 'concept_id', [], ['concept_id'], None,
                     default_lkp='concept_name')
nodes = [Person, Visit_Detail, Care_Site, Visit_Occurrence, Death, Measurement, Concept]
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


# ################################# QUERY FIELDS #######################################

with open("select_statements.yaml", "r") as f:
    select_fragments = yaml.load(f, Loader=yaml.CLoader)

# Define options for user selection
opts_primary = (
    UserOption('person', '{alias:s}person_id', Person, context,
               aggregations=[None, 'rows', 'count'], default_aggregation='count'),
    UserOption('measurement_types', '{alias:s}measurement_concept_id', Measurement,
               context, aggregations=[None, 'rows', 'count']),
    # UserOption('length_of_stay', '{alias:s}length_of_stay', 'custom', context,
    #            aggregations=[None, 'avg']),
)

standard_concept = "{alias:s}standard_concept = 'S'"   # WHERE clause for CONCEPT table
opts_split = (
    UserOption('age', '2020 - {alias:s}year_of_birth', Person, context,
               transformations=[None, 'Tens'], field_alias='age'),
    UserOption('sex', '{alias:s}gender_concept_id', Person, context,
               dimension_table=Concept, perform_lkp=True, dim_where=standard_concept),
    UserOption('race', '{alias:s}race_concept_id', Person, context,
               dimension_table=Concept, perform_lkp=True, dim_where=standard_concept),
    UserOption('visit type', '{alias:s}visit_concept_id',
               Visit_Occurrence, context,
               dimension_table=Concept, perform_lkp=True, dim_where=standard_concept),
    UserOption('admission type', '{alias:s}admitting_source_concept_id',
               Visit_Occurrence, context,
               dimension_table=Concept, perform_lkp=True, dim_where=standard_concept),
    UserOption('discharge type', '{alias:s}discharge_to_concept_id',
               Visit_Occurrence, context,
               dimension_table=Concept, perform_lkp=True, dim_where=standard_concept),
    UserOption('visit start date', '{alias:s}visit_start_datetime', Visit_Occurrence,
               context, aggregations=[None, 'rows']),
    UserOption('length of stay', select_fragments['length_of_stay'], Visit_Occurrence,
               context, aggregations=[None, 'avg']),
    UserOption('care site', '{alias:s}care_site_id', Visit_Detail, context,
               dimension_table=Care_Site, perform_lkp=True),
    UserOption('death', '{alias:s}death_date', Death, context,
               transformations=['not null', 'day', 'week', 'month'],
               default_transformation='week'),
)


# ############################## STANDARD QUERIES ########################################


with open("standard_queries.json", 'r') as f:
    standard_queries = json.JSONDecoder(object_pairs_hook=OrderedDict).decode(f.read())

# agg_opt = opts_aggregation[0]
# agg_opt.set_aggregation('count')
# tmp = construct_query(agg_opt, opts_split[0], *opts_split[1:])
# print(tmp)

