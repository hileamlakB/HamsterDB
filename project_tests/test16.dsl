--
-- control test for test16.dsl without batching
--
-- Query in SQL:
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 10 AND col1 < 12000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 110 AND col1 < 22000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 210 AND col1 < 32000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 310 AND col1 < 42000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 10 AND col1 < 42000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 10 AND col1 < 12000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 110 AND col1 < 22000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 210 AND col1 < 32000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 310 AND col1 < 42000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 10 AND col1 < 42000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 10 AND col1 < 12000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 110 AND col1 < 22000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 210 AND col1 < 32000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 310 AND col1 < 42000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 10 AND col1 < 42000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 10 AND col1 < 12000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 110 AND col1 < 22000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 210 AND col1 < 32000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 310 AND col1 < 42000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 10 AND col1 < 42000;
--
--
--
s1=select(db1.tbl3_batch.col1,10,12000)
s2=select(db1.tbl3_batch.col1,110,22000)
s3=select(db1.tbl3_batch.col1,210,32000)
s4=select(db1.tbl3_batch.col1,310,42000)
s5=select(db1.tbl3_batch.col1,10,42000)
s6=select(db1.tbl3_batch.col1,10,12000)
s7=select(db1.tbl3_batch.col1,110,22000)
s8=select(db1.tbl3_batch.col1,210,32000)
s9=select(db1.tbl3_batch.col1,310,42000)
s10=select(db1.tbl3_batch.col1,10,42000)
s11=select(db1.tbl3_batch.col1,10,12000)
s12=select(db1.tbl3_batch.col1,110,22000)
s13=select(db1.tbl3_batch.col1,210,32000)
s14=select(db1.tbl3_batch.col1,310,42000)
s15=select(db1.tbl3_batch.col1,10,42000)
s16=select(db1.tbl3_batch.col1,10,12000)
s17=select(db1.tbl3_batch.col1,110,22000)
s18=select(db1.tbl3_batch.col1,210,32000)
s19=select(db1.tbl3_batch.col1,310,42000)
s20=select(db1.tbl3_batch.col1,10,42000)
--
f1=fetch(db1.tbl3_batch.col4,s1)
f2=fetch(db1.tbl3_batch.col4,s2)
f3=fetch(db1.tbl3_batch.col4,s3)
f4=fetch(db1.tbl3_batch.col4,s4)
f5=fetch(db1.tbl3_batch.col4,s5)
f6=fetch(db1.tbl3_batch.col4,s6)
f7=fetch(db1.tbl3_batch.col4,s7)
f8=fetch(db1.tbl3_batch.col4,s8)
f9=fetch(db1.tbl3_batch.col4,s9)
f10=fetch(db1.tbl3_batch.col4,s10)
f11=fetch(db1.tbl3_batch.col4,s11)
f12=fetch(db1.tbl3_batch.col4,s12)
f13=fetch(db1.tbl3_batch.col4,s13)
f14=fetch(db1.tbl3_batch.col4,s14)
f15=fetch(db1.tbl3_batch.col4,s15)
f16=fetch(db1.tbl3_batch.col4,s16)
f17=fetch(db1.tbl3_batch.col4,s17)
f18=fetch(db1.tbl3_batch.col4,s18)
f19=fetch(db1.tbl3_batch.col4,s19)
f20=fetch(db1.tbl3_batch.col4,s20)
--
print(f1)
print(f2)
print(f3)
print(f4)
print(f5)
print(f6)
print(f7)
print(f8)
print(f9)
print(f10)
print(f11)
print(f12)
print(f13)
print(f14)
print(f15)
print(f16)
print(f17)
print(f18)
print(f19)
print(f20)
