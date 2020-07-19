{#
Returns date columns to unpivot into single date field.
Unique to structure of usafacts cases/deaths dataset.

Args:
    column_list: All date field columns in source dataset
    max_partition_date: Current max date in cleansed table
#}

-- Construct union of dates in column_list



-- filter down based on max_partition_date


-- get results into list structgure


-- handle for no results in the table (maybe this is done elsewhere?)
