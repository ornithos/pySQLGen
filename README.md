## PySQLGen

[![Heroku](http://heroku-badge.herokuapp.com/?app=sqlgen&style=flat&svg=1&root=index.html)](https://sqlgen.herokuapp.com/)

(Heroku is on an unpaid tier and may be asleep -- please refresh after 10-20s if the above badge is unavailable.)

This project generates a subset of SQL for a given schema. It's been developed to support the [DECOVID](https://www.decovid.org/) project, and is currently being tested on data following the [OMOP schema](https://ohdsi.github.io/TheBookOfOhdsi/CommonDataModel.html). I make no claims of correctness or intuitiveness for more general schema. See the [`heroku`](www.heroku.com) deployment (above) for an example.

The intended use for the generated SQL is flexible aggregations for data visualisation as in e.g. dashboards. The high level 'language' or specification requires the user to specify:

* one or more fields (from arbitrary tables), with one being specified as the *primary* field.
    * The table of the primary field is considered the root of the query tree, and aggregations on this table will be performed last.
* any transformations from a prespecified list (see the [`db_fields.yaml`](db_fields.yaml) file.
* any aggregations from a prespecified list (see the [`db_fields.yaml`](db_fields.yaml) file.
* The query tree will be constructed automatically from the relationships between the tables specified (for current specification, see the [`decovid.py`](decovid.py) file).
* Any aggregations required prior to the root table will occur within Common Table Expressions.

There is no guarantee of optimality of the query -- primary keys are used in order to calculate the joins, but no table constraints are used.

### How does it work?

This is a fairly quick prototype project, so it's worth highlighting here what's going on under the hood. The code assumes that the database schema can be adequately described via a tree structure of tables, with parents / child tables appropriately defined.

**App setup**

Metadata is provided via config files:
 * The app is populated with the names of the relevant tables from the database, along with their parents, and primary / foreign keys. (Note this is currently done via a `.py` file (`decovid.py`), but the specification is fairly high level, and can/should be moved out. While each table must specify a parent (if applicable), "shortcuts" can be defined, and will be used if possible. For instance in the OMOP schema, the `visit_detail` has the following relation `visit_detail --> visit_occurrence --> person`, but if no fields from the intermediate table are used, it can be ignored, since `visit_detail` contains the primary key of the `person` table.
 * Each table's fields (columns) are specified in the `db_fields.yaml` file. One can specify available aggregations (`AVG`, `COUNT` etc.) and transformations (e.g. `MONTH`, `NOT NULL` etc.) for each field (often only a subset of operations make sense for each field). One can also specify a dimension table where the field resides in a fact table and corresponds to a name/quantity in a dimension table.
 
 **User specification**
* The user specifies $k$ different fields, along with transformations, aggregations, and whether to look up a field in a dimension table.

**App function**
* The tables corresponding to each field are extracted, and a query structure (required tables, intermediate tables, join keys) is calculated. This is a graph Steiner Tree problem for which heuristics are used. Shortcuts may be used as specified in the app setup where possible.
* Working from the leaves up, tables are recursively transformed into subqueries are created wherever an aggregation needs to take place (except at the root node).
   * The purpose of the 'primary' variable in the app is to indirectly specify this root node. The query may not retain the same directions as present in the graph structure of the schema, and hence the root node is otherwise undefined.
   * The graph is therefore topologically sorted before this operation can take place.
* The clauses (`SELECT`, `FROM`, `WHERE`, `GROUP BY`) within each subquery are generated, including any specified transformations.

Currently very little customisation is possible for `WHERE` clauses as it is not yet of primary interest for this project.

### Example usage

![example-gif](https://i.imgur.com/2B2Gf90.gif)
