-- Min and Max
--
-- Min
-- SELECT min(col1) FROM tbl2 WHERE col1 >= -5 AND col1 < 5;
s1=select(db1.tbl2.col1,-5,5)
f1=fetch(db1.tbl2.col1,s1)
m1=min(f1)
print(m1)
--
-- SELECT min(col2) FROM tbl2 WHERE col1 >= -5 AND col1 < 5;
f2=fetch(db1.tbl2.col2,s1)
m2=min(f2)
print(m2)
--
--
-- Max
-- SELECT max(col1) FROM tbl2 WHERE col1 >= -5 AND col1 < 5;
s1=select(db1.tbl2.col1,-5,5)
f1=fetch(db1.tbl2.col1,s1)
m1=max(f1)
print(m1)
--
-- SELECT max(col2) FROM tbl2 WHERE col1 >= -5 AND col1 < 5;
f2=fetch(db1.tbl2.col2,s1)
m2=max(f2)
print(m2)

