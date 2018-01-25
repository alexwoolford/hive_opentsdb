# hive_opentsdb

This reads a table from Hive and loads the data into OpenTSDB.

The Hive, OpenTSDB, and table configurations are defined in the .conf file, e.g.

    [hive]
    host=hdp03.woolford.io
    port=10500
    db=default
    table=tommy
    timestamp_column=ts
    tag_columns=host
    value_columns=storage_used,compressed_capacity
    
    [opentsdb]
    host=hdp01.woolford.io
    port=4242

