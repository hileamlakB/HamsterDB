-- Testing hash join with a complex query
--
--
-- Query in SQL:
-- SELECT sum(tbl2.col1+tbl5.col2-tbl5.col3) FROM tbl2,tbl5 WHERE tbl2.col1=tbl5.col4 AND tbl5.col2<100 AND tbl5.col4<15 AND tbl2.col2>=5;
--
--
p1=select(db1.tbl5.col2,null,100)
f1=fetch(db1.tbl5.col4,p1)
p2=select(p1,f1,null,15)
f2=fetch(db1.tbl5.col4,p2)
p3=select(db1.tbl2.col2,5,null)
f3=fetch(db1.tbl2.col1,p3)
t1,t2=join(f2,p2,f3,p3,hash)
j1=fetch(db1.tbl2.col1,t2)
j2=fetch(db1.tbl5.col2,t1)
j3=fetch(db1.tbl5.col3,t1)
add1=add(j1,j2)
sub1=sub(add1,j3)
out1=sum(sub1)
print(out1)
