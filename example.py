from pysqlgen.dbtree import *
from pysqlgen.fields import UserOption
from pysqlgen.query import construct_query

# ~~~~~~~~~~~~~~~~~~~~ Define Schema ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Person = SchemaNode('Person', None, 'person_id',
                    ['Visit_Detail', 'Visit_Occurrence',
                     'Death', 'Measurement'],
                    ['person_id'], None)
Visit_Detail = SchemaNode('Visit_Detail', Person, 'visit_detail_id',
                          ['Care_Site'],
                          ['care_site_id', 'person_id'],
                          'visit_start_date')
Care_Site = SchemaNode('Care_Site', Visit_Detail, 'care_site_id', [], [], None)
Visit_Occurrence = SchemaNode('Visit_Occurrence', Person, 'visit_occurrence_id',
                              [], ['person_id'],
                              'visit_start_datetime')
Death = SchemaNode('Death', Person, 'person_id', [], [], 'death_date')
Measurement = SchemaNode('Measurement', Person, 'person_id', [], [],
                         'measurement_datetime')

Concept = SchemaNode('Concept', None, 'concept_id', [], ['concept_id'], None)
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
AGGREGATIONS = ['rows', 'count', 'avg', 'sum', 'max', 'min']
TRANSFORMATIONS = ['not null', 'day', 'week', 'month', 'first', 'tens']
schema = 'public'   # name of schema within DB
context = DBMetadata(nodes, custom_tables, schema, AGGREGATIONS, TRANSFORMATIONS)


# Define options for user selection
opts_aggregation = (
    UserOption('person', '{alias:s}person_id', Person, context,
               aggregations=[None, 'rows', 'count'], default_aggregation='count'),
    UserOption('measurement_types', '{alias:s}measurement_concept_id', Measurement,
               context, aggregations=[None, 'rows', 'count']),
    UserOption('length_of_stay', '{alias:s}length_of_stay', 'custom', context,
               aggregations=[None, 'avg']),
)

opts_split = (
    UserOption('age', '2020 - {alias:s}year_of_birth', Person, context,
               transformations=[None, 'Tens'], field_alias='age'),
    UserOption('sex', '{alias:s}gender_concept_id', Person, context,
               dimension_table=Concept, lkp_dimension=True),
    UserOption('race', '{alias:s}race_concept_id', Person, context,
               dimension_table=Concept, lkp_dimension=True),
    UserOption('visit type', '{alias:s}visit_concept_id',
               Visit_Occurrence, context, dimension_table=Concept, lkp_dimension=True),
    UserOption('admission type', '{alias:s}admitting_source_concept_id',
               Visit_Occurrence, context, dimension_table=Concept, lkp_dimension=True),
    UserOption('discharge type', '{alias:s}discharge_to_concept_id',
               Visit_Occurrence, context, dimension_table=Concept, lkp_dimension=True),
    # UserOptionSplit('first admission date', '{alias:s}admission_date',
    #                 'custom', context),   # <--- STILL NEED TO ADD IN CUSTOM TABLES INTO JOIN LOGIC
                                            #      N.B. How to do graph traversal when custom tables
                                            #      are not part of the schema model?
    UserOption('care site type', '{alias:s}care_site_name', Care_Site, context),
    UserOption('death', '{alias:s}death_date', Death, context,
               transformations=['not null', 'day', 'week', 'month'],
               default_transformation='week'),
)


# agg_opt = opts_aggregation[0]
# agg_opt.set_aggregation('count')
# tmp = construct_query(agg_opt, opts_split[0], *opts_split[1:])
# print(tmp)