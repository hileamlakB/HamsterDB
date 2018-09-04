-- Test for creating table with duplicates for joins
--
-- Table tbl5 has duplicates in col4 (each value has ~50 instances)
--
-- Loads data from: data5.csv
--
-- Create Table
create(tbl,"tbl5",db1,4)
create(col,"col1",db1.tbl5)
create(col,"col2",db1.tbl5)
create(col,"col3",db1.tbl5)
create(col,"col4",db1.tbl5)
-- Create an unclustered btree index on col2
create(idx,db1.tbl5.col4,btree,unclustered)
--
--
-- Load data immediately in the form of a clustered index
load("/home/cs165/cs165-management-scripts/project_tests_2017/data5.csv")
--
-- Testing that the data and their indexes are durable on disk.
shutdown

