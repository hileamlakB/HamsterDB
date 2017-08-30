-- Load+create Data and shut down of tbl1 which has 1 attribute only
create(db,"db1")
create(tbl,"tbl1",db1,1)
create(col,"col1",db1.tbl1)
load("../project_tests/data1.csv")
relational_insert(db1.tbl1,-1)
relational_insert(db1.tbl1,-2)
relational_insert(db1.tbl1,-3)
relational_insert(db1.tbl1,-4)
relational_insert(db1.tbl1,-5)
relational_insert(db1.tbl1,-6)
relational_insert(db1.tbl1,-7)
relational_insert(db1.tbl1,-8)
relational_insert(db1.tbl1,-9)
relational_insert(db1.tbl1,-10)
shutdown

