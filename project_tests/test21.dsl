--
-- tbl3 has a secondary b-tree tree index on col2, and a clustered index on col1 with the form of a sorted column
-- testing for correctness
--
-- Query in SQL:
-- SELECT col3 FROM tbl3 WHERE col1 >= 800 and col1 < 810;
--
-- since col1 has a clustered index, the index is expected to be used by the select operator
s1=select(db1.tbl3.col1,800,810)
f1=fetch(db1.tbl3.col3,s1)
print(f1)
