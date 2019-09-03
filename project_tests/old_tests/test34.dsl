-- Testing hash join 3
--
--
-- Query in SQL: 
-- SELECT sum(tbl2.col1-tbl3.col2) FROM tbl2,tbl3 WHERE tbl2.col2=tbl3.col2 AND tbl2.col2>=-100 AND tbl3.col3<800;
--
--
p1=select(db1.tbl2.col2,-100,null)
p2=select(db1.tbl3.col3,null,800)
f1=fetch(db1.tbl2.col2,p1)
f2=fetch(db1.tbl3.col2,p2)
t1,t2=join(f1,p1,f2,p2,hash)
j1=fetch(db1.tbl2.col1,t1)
j2=fetch(db1.tbl3.col2,t2)
sub1=sub(j1,j2)
out1=sum(sub1)
print(out1)

