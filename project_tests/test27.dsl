--
-- tbl4 has a clustered b-tree tree index on col1, and a sorted unclustered index on col2
-- testing for correctness
--
-- Query in SQL:
-- SELECT col3 FROM tbl4 WHERE col1 >= 800 and col1 < 810;
--
-- since col1 has a clustered b-tree index, the index is expected to be used by the select operator
s1=select(db1.tbl4.col1,800,810)
f1=fetch(db1.tbl4.col3,s1)
print(f1)
