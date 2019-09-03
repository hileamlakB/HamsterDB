-- Correctness test: Do inserts in tbl3.
--
-- Table tbl3 has a secondary index and a clustered index, so, all should be maintained when we insert new data.
-- This means that the table should be always sorted on col1 and the secondary indexes on col2 should be updated
--
-- INSERT INTO tbl3 VALUES (-1,-11,-111,-1111);
-- INSERT INTO tbl3 VALUES (-2,-22,-222,-2222);
-- INSERT INTO tbl3 VALUES (-3,-33,-333,-2222);
-- INSERT INTO tbl3 VALUES (-4,-44,-444,-2222);
-- INSERT INTO tbl3 VALUES (-5,-55,-555,-2222);
--
relational_insert(db1.tbl3,-1,-11,-111,-1111)
relational_insert(db1.tbl3,-2,-22,-222,-2222)
relational_insert(db1.tbl3,-3,-33,-333,-2222)
relational_insert(db1.tbl3,-4,-44,-444,-2222)
relational_insert(db1.tbl3,-5,-55,-555,-2222)
