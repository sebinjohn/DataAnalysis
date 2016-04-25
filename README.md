## ValueExtractor
A MapReduce program which can be used for extracting records from text files, which have specific values in the specified column.
equivalent to `select * from <table> where column='value'`

## Why can't we use Apache Hive for the same purpose?

We could use, but this tool is useful in places where data is not already in Hive,
and the records obtained is not going back into Hive
