--
-- tbl3 has a secondary b-tree tree index on col2, and a clustered index on col1 with the form of a sorted column
-- testing for correctness
--
-- Query in SQL:
-- SELECT col3 FROM tbl3 WHERE col2 >= 800 and col2 < 810;
--
-- since col2 has a secondary b-tree index, the index is expected to be used by the select operator
s2=select(db1.tbl3.col2,800,810)
f2=fetch(db1.tbl3.col3,s2)
print(f2)
