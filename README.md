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

### Early screenshot of Dash-based UI

![Screenshot](assets/screenshot.png)
