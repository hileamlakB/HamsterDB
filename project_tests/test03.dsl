-- Test Select + Fetch
--
-- SELECT col1 FROM tbl1 WHERE col1 < 20;
s1=select(db1.tbl1.col1,null,20)
f1=fetch(db1.tbl1.col1,s1)
print(f1)
--
-- SELECT col1 FROM tbl1 WHERE col1 >= -1;
s2=select(db1.tbl1.col1,-1,null)
f2=fetch(db1.tbl1.col1,s2)
print(f2)
--
-- SELECT col2 FROM tbl2 WHERE col1 >= -1000 AND col1 < 2;
s3=select(db1.tbl2.col1,-1000,2)
f3=fetch(db1.tbl2.col2,s3)
print(f3)
--
-- SELECT col1 FROM tbl2 WHERE col4 >= -1000 AND col4 < 1200;
s4=select(db1.tbl2.col4,-1000,1200)
f4=fetch(db1.tbl2.col1,s4)
print(f4)

