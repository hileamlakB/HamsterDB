-=-=-=-=-=-=-=-=-=-
CS 165: Data Systems
Project tests
-=-=-=-=-=-=-=-=-=-

-=-=-=-=
Overview
-=-=-=-=

There are a total of 41 tests divided between different milestones as follows:

Milestone 1: test01 through test09
Milestone 2: test10 through test17
Milestone 3: test18 through test29
Milestone 4: test30 through test35
Milestone 5: test36 through test41

For these tests, we provide all required data sets [dataX.csv] as well as the
expected output [testX.exp] so that you can run and verify the tests on your own
machine.

-=-=-=-=-=-=-=-=-=
Performance testing
-=-=-=-=-=-=-=-=-=

In addition to testing for correctness, we also test the performance of scan
sharing (M2), indexing (M3), and hash joins (M4). We do this by comparing the
runtime of the corresponding test with the runtime of a control test (i.e., a
test that yields the same result but does not use the functionality for which
performance is tested). We expect some amount of speed up in the tests resulting
from scan sharing, indexing, and hash joins. This expectation is set based on
last year’s submissions.

We use the following tests to test for performance:

Milestone 2: test17 (control: test16)

Milestone 3: test21 (control: test20); test23 (control: test22); test27
(control: test26); test29 (control: test28)

Milestone 4: test32 (control: test31)

For instance, we test the performance of scan sharing as follows: First, we run
test16 (20 queries with no scan sharing) and then we run test17 (the same 20
queries with scan sharing). We compare the time taken to run these two tests
and see if their relative performance meets (or exceeds) the performance
expectation.

-=-=-=-=-=-=-
Data file paths
-=-=-=-=-=-=-

In the provided tests, the paths to the data files are set based on our testing
infrastructure. For instance, we use
“load("/home/cs165/cs165-management-scripts/project_tests_2017/data1.csv”)” to
load data1.csv. You can (and most probably will have to) change this path to
any absolute or relative path that matches your directory structure.

-=-=-=-=-=-=-=-=-=-
Running on the server
-=-=-=-=-=-=-=-=-=-

Please note that when testing your system on the server, we will run these
tests on much larger data sets (with 1M to 10M rows).
