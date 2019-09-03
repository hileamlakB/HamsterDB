--
-- Testing for batching queries
-- First test is 2 queries with partial overlap
--
-- Query in SQL:
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 600 AND col1 < 820;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 800 AND col1 < 830;
--
--
batch_queries()
s1=select(db1.tbl3_batch.col1,600,820)
s2=select(db1.tbl3_batch.col1,800,830)
batch_execute()
f1=fetch(db1.tbl3_batch.col4,s1)
f2=fetch(db1.tbl3_batch.col4,s2)
print(f1)
print(f2)
