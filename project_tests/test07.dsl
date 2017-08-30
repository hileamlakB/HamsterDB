-- Subtraction
--
-- SELECT col2-col3 FROM tbl2 WHERE col1 >= -1 AND col1 < 10;
s21=select(db1.tbl2.col1,-1,10)
f21=fetch(db1.tbl2.col2,s21)
f22=fetch(db1.tbl2.col3,s21)
s21=sub(f21,f22)
print(s21)

