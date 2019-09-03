-- Correctness test: Delete values
--
-- DELETE FROM tbl3 WHERE col1 = -10;
-- DELETE FROM tbl3 WHERE col2 = -22;
-- DELETE FROM tbl3 WHERE col1 = -30;
-- DELETE FROM tbl3 WHERE col3 = -444;
-- DELETE FROM tbl3 WHERE col1 = -50;
--
d1=select(db1.tbl3.col1,-10,-9)
relational_delete(db1.tbl3,d1)
d2=select(db1.tbl3.col2,-22,-21)
relational_delete(db1.tbl3,d2)
d3=select(db1.tbl3.col1,-30,-29)
relational_delete(db1.tbl3,d3)
d4=select(db1.tbl3.col3,-444,-443)
relational_delete(db1.tbl3,d4)
d5=select(db1.tbl3.col1,-50,-49)
relational_delete(db1.tbl3,d5)
