# Tips: Pyspark in DataBricks
****************************************

## Non-Determinism and how to avoid it

If you are using databrick chances are you work with highly regulated data. 
The biggest "gotcha" I've ever encounted is how Spark's non-strict evaluation interacts with the delta lake architecture of databrcks.

If deterministic filtering/subset results are important to your Organization's auditing policies, it's best to avoid the drop_duplicates() function. The results will not 
be deterministic.

## TLDR

It's not unlikely that to encounter pipelines with extremely strict data governance. An example of a data governance function is provided. Scripts of those kind are used to prevent records from being changed retroactively. You can think of it almost like a git commit log. Audit logs and system columns must also be deterministic. This has caused many pipeline failures in my experience
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

In general, you will want to use windowing functions to partition your dataframe. There are some utility functions provided that could be useful. 
