-- Correctness test: Test for updates on columns with index
--
-- SELECT col1 FROM tbl3 WHERE col2 >= -100 AND col2 < 2;
--
s1=select(db1.tbl3.col2,-100,2)
f1=fetch(db1.tbl3.col1,s1)
print(f1)
--
-- Needs test10.dsl and test28.dsl to have been executed first.
-- Correctness test: Test for updates on columns with index
--
-- SELECT col3 FROM tbl3 WHERE col1 >= -3 AND col1 < 100;
--
s2=select(db1.tbl3.col1,-3,100)
f2=fetch(db1.tbl3.col3,s2)
print(f2)
