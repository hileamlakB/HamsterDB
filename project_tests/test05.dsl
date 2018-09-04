-- Summation
--
-- SELECT SUM(col4) FROM tbl2 WHERE col1 >= -2 AND col1 < 1;
s1=select(db1.tbl2.col1,-2,1)
f1=fetch(db1.tbl2.col4,s1)
a1=sum(f1)
print(a1)
--
-- SELECT SUM(col1) FROM tbl2;
a2=sum(db1.tbl2.col1)
print(a2)

