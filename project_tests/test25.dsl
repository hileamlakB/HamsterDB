-- Test for creating table with indexes
--
-- Table tbl4 has a clustered index in the form of a btree and an unclustered sorted index.
--
-- Loads data from: data4.csv
--
-- Create Table
create(tbl,"tbl4",db1,4)
create(col,"col1",db1.tbl4)
create(col,"col2",db1.tbl4)
create(col,"col3",db1.tbl4)
create(col,"col4",db1.tbl4)
-- Create a clustered index on col1
create(idx,db1.tbl4.col1,btree,clustered)
-- Create an unclustered btree index on col2
create(idx,db1.tbl4.col2,sorted,unclustered)
--
--
-- Load data immediately in the form of a clustered index
load("/home/cs165/cs165-management-scripts/project_tests_2017/data4.csv")
--
-- Testing that the data and their indexes are durable on disk.
shutdown
