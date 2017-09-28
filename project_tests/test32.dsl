-- Testing hash join 1
--
--
-- Query in SQL: 
-- SELECT tbl2.col1, tbl3.col2 FROM tbl2,tbl3 WHERE tbl2.col2=tbl3.col3 AND tbl2.col2>= 500 AND tbl3.col3<6100;
--
--
p1=select(db1.tbl2.col2,500,null)
p2=select(db1.tbl3.col3,null,6100)
f1=fetch(db1.tbl2.col2,p1)
f2=fetch(db1.tbl3.col3,p2)
t1,t2=join(f1,p1,f2,p2,hash)
out1=fetch(db1.tbl2.col1,t1)
out2=fetch(db1.tbl3.col2,t2)
print(out1,out2)

