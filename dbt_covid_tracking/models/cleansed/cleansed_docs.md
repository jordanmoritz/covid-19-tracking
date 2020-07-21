{% docs daily_cases_deaths_incremental %}

This model references source data that has unique column values for each day
formatted as '\_m\_dd\_yy'. Every day the source table has a new column added for
the previous day.

To melt this down into a single date field it uses a macro to grab all the
column values. Those are used to union selects together for each value,
while formatting into a date value, and using the original text value for
column selection.

Ideally, this would entirely leverage the native dbt incremental strategy, but
given the structure of this source the incremental behavior seems to be
recreating the entire table from scratch, and using that to determine which
additional dates partitions to append to the existing table from the model
({{ this }}). This results in unnecessary compute/processing.

To work around this, it uses a macro to determine which dates (and by virtue of
the source structure, which columns) should be processed based on the current
max date in the destination table, and the dates present in the source. Using
this, the model conditionally builds the table only for those dates which are
not already present in the destination table, which significantly reduces
compute/processing.

From here, the dbt incremental strategy proceeds as expected, only appending
new data to the destination table. However, this wrestles with some of the
native features of the framework, specifically the --full_refresh
flag on model run. On full refresh, dbt switches from its merge strategy to
create and replace of existing table, but because the model is intervening and
conditionally building the table only for new data that should be appended to
the destination table, this effectively truncates the destination table and
recreates it only with the new data that was intended to be appended.

{% enddocs %}
