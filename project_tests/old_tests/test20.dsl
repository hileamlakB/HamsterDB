--
-- Query in SQL:
-- SELECT col3 FROM tbl3_ctrl WHERE col1 >= 800 and col1 < 810;
--
-- Control test for test13.dsl
s1=select(db1.tbl3_ctrl.col1,800,810)
f1=fetch(db1.tbl3_ctrl.col3,s1)
print(f1)
