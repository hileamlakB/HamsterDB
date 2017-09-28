--
-- tbl4 has a clustered b-tree tree index on col1, and a sorted unclustered index on col2
-- testing for correctness
--
-- Query in SQL:
-- SELECT col3 FROM tbl4 WHERE col2 >= 800 and col2 < 810;
--
-- since col2 has a sorted unclustered sorted index, the index is expected to be used by the select operator
s2=select(db1.tbl4.col2,800,810)
f2=fetch(db1.tbl4.col3,s2)
print(f2)
