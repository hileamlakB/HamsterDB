-- Create a table to run batch queries on
--
-- Loads data from: data3_batch.csv
--
-- Create Table
create(tbl,"tbl3_batch",db1,4)
create(col,"col1",db1.tbl3_batch)
create(col,"col2",db1.tbl3_batch)
create(col,"col3",db1.tbl3_batch)
create(col,"col4",db1.tbl3_batch)
--
-- Load data immediately
load("/home/cs165/cs165-management-scripts/project_tests_2017/data3_batch.csv")
--
-- Testing that the data is durable on disk.
shutdown
