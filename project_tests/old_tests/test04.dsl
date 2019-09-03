-- Average
--
-- SELECT avg(col4) FROM tbl2 WHERE col1 >= -2 AND col1 < 1;;
s1=select(db1.tbl2.col1,-2,1)
f1=fetch(db1.tbl2.col4,s1)
a1=avg(f1)
print(a1)
--
-- SELECT avg(col1) FROM tbl2;
a2=avg(db1.tbl2.col1)
print(a2)

