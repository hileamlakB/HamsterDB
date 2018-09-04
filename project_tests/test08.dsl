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
s21=select(db1.tbl2.col1,-5,5)
f21=fetch(db1.tbl2.col1,s21)
m21=max(f21)
print(m21)
--
-- SELECT max(col2) FROM tbl2 WHERE col1 >= -5 AND col1 < 5;
f22=fetch(db1.tbl2.col2,s21)
m22=max(f22)
print(m22)

