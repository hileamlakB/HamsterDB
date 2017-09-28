--
-- Control test for test19.dsl
--
-- Query in SQL:
-- SELECT col3 FROM tbl4_ctrl WHERE col1 >= 800 and col1 < 810;
--
s1=select(db1.tbl4_ctrl.col1,800,810)
f1=fetch(db1.tbl4_ctrl.col3,s1)
print(f1)
