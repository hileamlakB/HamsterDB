-- Load+create Data and shut down of tbl2 which has 4 attributes
create(tbl,"tbl2",db1,4)
create(col,"col1",db1.tbl2)
create(col,"col2",db1.tbl2)
create(col,"col3",db1.tbl2)
create(col,"col4",db1.tbl2)
load("../project_tests/data2.csv")
relational_insert(db1.tbl2,-1,-11,-111,-1111)
relational_insert(db1.tbl2,-2,-22,-222,-2222)
relational_insert(db1.tbl2,-3,-33,-333,-2222)
relational_insert(db1.tbl2,-4,-44,-444,-2222)
relational_insert(db1.tbl2,-5,-55,-555,-2222)
relational_insert(db1.tbl2,-6,-66,-666,-2222)
relational_insert(db1.tbl2,-7,-77,-777,-2222)
relational_insert(db1.tbl2,-8,-88,-888,-2222)
relational_insert(db1.tbl2,-9,-99,-999,-2222)
relational_insert(db1.tbl2,-10,-11,0,-34)
shutdown

