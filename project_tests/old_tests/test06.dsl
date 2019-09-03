-- Addition 
--
-- SELECT col2+col3 FROM tbl2 WHERE col1 >= -1 AND col1 < 10;
s11=select(db1.tbl2.col1,-1,10)
f11=fetch(db1.tbl2.col2,s11)
f12=fetch(db1.tbl2.col3,s11)
a11=add(f11,f12)
print(a11)

