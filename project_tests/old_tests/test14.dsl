--
-- Testing for batching queries
-- First test is 5 queries with NO overlap
--
-- Query in SQL:
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 10 AND col1 < 20;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 110 AND col1 < 120;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 210 AND col1 < 220;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 310 AND col1 < 320;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 410 AND col1 < 420;
--
--
--
batch_queries()
s1=select(db1.tbl3_batch.col1,10,20)
s2=select(db1.tbl3_batch.col1,110,120)
s3=select(db1.tbl3_batch.col1,210,220)
s4=select(db1.tbl3_batch.col1,310,320)
s5=select(db1.tbl3_batch.col1,410,420)
batch_execute()
f1=fetch(db1.tbl3_batch.col4,s1)
f2=fetch(db1.tbl3_batch.col4,s2)
f3=fetch(db1.tbl3_batch.col4,s3)
f4=fetch(db1.tbl3_batch.col4,s4)
f5=fetch(db1.tbl3_batch.col4,s5)
print(f1)
print(f2)
print(f3)
print(f4)
print(f5)
