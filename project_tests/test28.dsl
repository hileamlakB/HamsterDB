--
-- Control test for test21.dsl
--
-- Query in SQL:
-- SELECT col3 FROM tbl4_ctrl WHERE col2 >= 800 and col2 < 810;
--
s2=select(db1.tbl4_ctrl.col2,800,810)
f2=fetch(db1.tbl4_ctrl.col3,s2)
print(f2)
