--
-- Query in SQL:
-- SELECT col3 FROM tbl3_ctrl WHERE col2 >= 800 and col2 < 810;
--
-- Control test for test 15.dsl
s2=select(db1.tbl3_ctrl.col2,800,810)
f2=fetch(db1.tbl3_ctrl.col3,s2)
print(f2)
