# Tips: Pyspark in DataBricks
****************************************

## Non-Determinism and how to avoid it

If you are using databrick chances are you work with highly regulated data. 
The biggest "gotcha" I've ever encounted is how Spark's non-strict evaluation interacts with the delta lake architecture of databrcks.

#### Never use pyspark.sql.functions.drop_duplicates

Just don't. I know it seems fine. And it might be for a while. Delta Lake's whole thing is immutabability and time travel stuff. 

## The Delta Lake Time Travel Stuff

Time Travel is a fancy way of saying that the Delta Lake architecture allows you to query any historical version of a table, compare outputs across time, and reproduce historical analyses. But time travel only works correctly if your transformations are deterministic, ergo the immutability. 

## The Gotcha

If this all seems obvious so far, good. We haven't gotten to the not so obvious part yet. When I said never use drop_duplicates(), I don't just mean on a subset of data. 
I mean never use it, even if each row has a unique identifier. Audit logs and system columns must also be deterministic. This has caused many pipeline failures in my experience
and its hard to debug. 

Example:

| patient_id | visit_date | test_result | delta_commit_version      | _commit_timestamp        |
|------------|------------|-------------|------------------|--------------------|
| P001       | 2024-01-15 | POSITIVE    | cewaf4 | randomdate1             |
| P001       | 2024-01-15 | POSITIVE    | 2aw4rt | randomdate2              |
| P001       | 2024-01-15 | POSITIVE    | awfawf | randomdate3     |

Generally, metadata columns are added as the table is pushed from bronze to silver for the first time. They will not change the next time the pipeline is run, nor will they ever be included in the drop_duplicate() function.
Depending on how your Org sets up their data governance, now you have a pipeline failure.

## Solution

Try using the package in this repo. Ill provide examples soon.