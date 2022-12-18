# HamsterDB

## Introduction

HamsterDB is a column-store database built as a capstone project for the Harvard Undergraduate data systems course CS 165. It was designed to efficiently store and query large amounts of data.

As a column store database, it is optimized for aggrigate operations over large rows. And it has the following features.

1. **Basic database operations**: 
  - Inserting, Bulk loading, Selecting, and Fetching data
  - Aggregate operations like sum, add, sub, avg

2. **Threaded search**:
  - Improved performance of select and fetch operations through the use of
    - shared scans and 
    - parallelization

3. **Indexing**:
  - Btrees and External sorts for improved searches
  - Multiclusterd Indexs, with data duplication
  - Unclustered Indexs

4. **Joins**:
  - Nested loop joins 
  - Hash joins


## File organization

The `client.c` file contains the source code for the client side of the database application. This is responsible for handling socket connections with the server.

The `server.c` file contains the source code for the server side of the database application.

The `include` directory contains header files for the project, including declarations of functions and data structures used throughout the project.

The `Join` directory contains files related to the implementation of the join operation in the database.

The `Parser` directory contains files related to the parsing of queries and commands for the database. There are multiple parsers realted for each sql queries. The names of each file is indicative of the type of parser. 

The `Serializer` directory contains files related to the serialization and deserialization of data for storage and retrieval in the database.

The `Create` directory contains files related to the implementation of the create operation in the database.

The `Indexing` directory contains files related to the implementation of indexing in the database.

The `Makefile` is a file used to build the project. It contains instructions for the build system on how to compile and link the various source files and libraries into the final executable.

The `primes.long` file contains a list of prime numbers that may be used by the database for various purposes.

The `Datastructures` directory contains files defining the data structures used in the database, such as `HashTables`, `Queues`, `LinkedLists`, and `BTrees`.

The `Others` directory contains miscellaneous files that do not fit into any of the other categories.

The `Parallelization` directory contains files related to the implementation of parallelization in the database.

The `Printer` directory contains files related to the printing of data from the database.

The `Utils` directory contains utility functions used throughout the project.

The `Engine` directory contains files related to the database engine, including command execution.

The `Insert` directory contains files related to the implementation of the insert operation in the database, including insert and bulk loading.

The `Select` directory contains files related to the implementation of the select operation in the database.

The `Varpool` directory contains files related to the management of variables in the database. The varpool uses a hashtable to keep track of intermidiary variables that are assigned by the client.

## Installation and Usage


First download this repository. This project is tested on a linux environment with the above docker file. But as long us you have the development tools setup any linux system will do.

```
git clone git@github.com:hileamlakB/HamsterDB.git
```

You can the go to the src directory with 
```
cd src
```

Run the make file with 
```
make
```

You will then have two main executables. 
`client` and `server`.

You can then frist run `server` and then on another terminal `client`. Or `server` on the background and `client` in the foreground. 

# Queries

You can play with the database by loading data and running different queries. The query  uses a domain specfic langauge, taken from the CS 165 webiste, described below.

<details>
<summary> HamsterDB Query(taken from CS165)</summary>
<li> Keywords are unqualified text and symbols. For example: <em>create</em>, <em>tbl</em>, <em>col</em> etc.
   (These are static words that you can use to parse the instructions. They
   will appear in the same order and location in the string).</li>
<li> Items that appear in brackets are required but indicate good opportunities
   for extensions, or relate to one of the extra features found in the project description.
   For example, 'create(idx,&lt;col_name&gt;,[btree])' means that you must support
   creating at least B-tree indexes, but you may want to also support additional
   indexes like zone maps or hash maps.
</li>

<li> Variables appear in-between angle brackets. They are strings that appear in
   165L and are either identifiers like the name of a table or are labels that
   the system must carry through the execution of the commands (this will
   become more clear through the examples). 
</li>
<li> End of line indicates next instruction (but your design can buffer or parse
   multiple lines at a time as you see fit).
</li>
<li> Comments are marked with '--' and continue until the end of the line.
</li>
</ol>

<br><br><h1>Creating new database objects</h1>


<code>create(&lt;object_type&gt;,&lt;object_name&gt;,&lt;parameter1&gt;,&lt;parameter2&gt;,...)</code>
<br>
<br>

The create function creates new structures in the system. The possible structures are <em>databases</em>,
<em>tables</em>, <em>columns</em>, and <em>indexes</em>. It does not return anything. Below you can see all possible instances.
<br><br>
<pre>create(db,&lt;db_name&gt;)
create(tbl,&lt;t_name&gt;,&lt;db_name&gt;,&lt;col_cnt&gt;)
create(col,&lt;col_name&gt;,&lt;tbl_var)
create(idx,&lt;col_name&gt;,[btree, sorted], [clustered, unclustered])
</pre>

<h2>Usage</h2>
<pre>
create(db,"awesomebase") -- create db with name "awesomebase"
create(tbl,"grades",awesomebase,6) -- create table "grades" with 6 columns in the "awesomebase"
create(col,"project",awesomebase.grades) -- create column 1 with name "project"
create(col,"midterm1",awesomebase.grades) -- create column 2 with name "midterm1"
create(col,"midterm2",awesomebase.grades) -- create column 3 with name "midterm2"
create(col,"class",awesomebase.grades) -- create column 4 with name "class"
create(col,"quizzes",awesomebase.grades) -- create column 5 with name "quizzes"
create(col,"student_id",awesomebase.grades) -- create column 6 with name "student_id"
</pre>

<h2>SQL Example</h2>
<pre>
CREATE DATABASE awesomebase
CREATE TABLE grades (grades int, project int, midterm1 int, midterm2 int, class int, quizzes int, student_id int)
</pre>
<br>
In the create table statement, the first value of a parameter is the column name and the second parameter is its type. VARCHAR(n), BINARY(n), BIGINT, and TIMESTAMP are examples of other SQL data types. 

<br><h1>Loading</h1>

<code>load(&lt;filename&gt;)</code>
<br>
<br>

This function loads values from a file. Both absolute and relative paths should be supported. The columns within the file are assigned names that correspond to already created database objects. This filename should be a file on the client's side and the client should pass the data in this file to the server for loading. 

<h2>Parameters</h2>
&lt;filename&gt;: The name of the file to load the database from.

<h2>Return Value</h2>
None.

<h2>Usage</h2>
<br><pre>
load(&quot;/path/to/myfile.txt&quot;)
-- or relative path
load(&quot;./data/myfile.txt&quot;)
</pre>

<h2>File format</h2>
Input data will be provided as ASCII-encoded CSV files. For example:

<pre>foo.t1.a,foo.t1.b
10,-23
-22,910</pre>

This file would insert two rows into columns 'a' and 'b' of table 't1' in database 'foo'.

<h2>SQL Example</h2>
There is no direct correlate in SQL to the load command. That being said, almost all vendors have commands to load a file into a table. The MySQL version would be:
<pre> LOAD DATA INFILE myfile.txt </pre>

<h1>Inserting rows into a table</h1>

The system will support relational, that is, row-wise (one row at a time) inserts:

<br><br><code>relational_insert(&lt;tbl_var&gt;,[INT1],[INT2],...)</code><br><br>

<!-- Internally, the insert will have to be translated into a series of columnar inserts:

<br><br><code>insert(&lt;col_var&gt;,[INT])</code><br><br> -->

<h2>Parameters</h2>
&lt;col_var&gt;: A fully qualified column name.<br/>
&lt;tbl_var&gt;: A fully qualified table name.<br/>
INT/INTk: The value to be inserted (32 bit signed).

<h2>Return Value</h2>
None.

<h2>Usage</h2>
<br><pre>
relational_insert(awesomebase.grades,107,80,75,95,93,1) 
</pre>

<h2>SQL Example</h2>
There are two different insert statements in SQL. In the first statement below, the column names are omitted and the values are inserted into the columns of the table in the order those columns were declared in table creation. In the second statement, column names are included and the values in the insert statement are put in the corresponding given column. The two statements below perform the same action. 

<pre>INSERT INTO grades VALUES (107,80,75,95,93,1)
INSERT INTO grades (midterm1, project, midterm2, class, quizzes, student_id) VALUES (80,107,75,95,93,1)
</pre>

<h1>Selecting values</h1>

There are two kinds of select commands. <br>

<br><i>Select from within a column:</i> <br><br>

<code>&lt;vec_pos&gt;=select(&lt;col_name&gt;,&lt;low&gt;,&lt;high&gt;)</code><br>

<h2>Parameters</h2>
&lt;col_name&gt;: A fully qualified column name or an intermediate vector of values<br/>
&lt;low&gt;: The lowest qualifying value in the range.<br/>
&lt;high&gt;: The exclusive upper bound.<br>
<b>null</b>: specifies an infinite upper or lower bound.
<br><br>

<br><i>Select from pre-selected positions of a vector of values:</i> <br><br>

<code>&lt;vec_pos&gt;=select(&lt;posn_vec&gt;,&lt;val_vec&gt;,&lt;low&gt;,&lt;high&gt;)</code><br>
<h2>Parameters</h2>
&lt;posn_vec&gt;: A vector of positions<br/>
&lt;val_vec&gt;: A vector of values. </br>
&lt;low&gt;: The lowest qualifying value in the range.<br/>
&lt;high&gt;: The exclusive upper bound.<br>
<b>null</b>: specifies an infinite upper or lower bound.


<h2>Return Value</h2>
&lt;vec_pos&gt;: A vector of qualifying positions.

<h2>Usage</h2>
<br>
<pre>
-- select
pos_1=select(awesomebase.grades.project,90,100) -- Find the rows with project score between 90 and 99
pos_2=select(awesomebase.grades.project,90,null) -- Find the rows with project greater or equal to 90</pre>

<h2>SQL Example</h2>

<pre>SELECT student_id FROM grades WHERE midterm1 > 90 AND midterm2 > 90</pre>
<br>
In the statement above, we might select on midterm1 using the first select, then select on midterm2 using the second type of select. 

<h1>Fetching values</h1>

This function collects the values from a column at given positions.

<br><br><code>&lt;vec_val&gt;=fetch(&lt;col_var&gt;,&lt;vec_pos&gt;)</code><br><br>

<h2>Parameters</h2>
&lt;col_var&gt;: A fully qualified column name.<br/>
&lt;vec_pos&gt;: A vector of positions that qualify (as returned by select or join).<br/>

<h2>Return Value</h2>
&lt;vec_val&gt;: A vector of qualifying values.


<h2>Usage</h2>
<br><pre>a_plus=select(awesomebase.grades.project,100,null) -- Find the rows with project greater or equal to 100
ids_of_top_students=fetch(awesomebase.grades.student_id,a_plus) -- Return student id of the qualifying rows
</pre>

<h2>SQL Example</h2>
The fetch command would be an internal operation at the end of a SQL query. For example, using our last query:
<pre> SELECT student_id FROM grades WHERE midterm1 > 90 AND midterm2 > 90 </pre>
<br>
The last part of this query after the two WHERE clauses had been evaluated would use a fetch on column student_id.  

<h1>Deleting rows</h1>

Row deletions happen using the relational_delete operation. It will internally issue multiple separate column deletes.

<br><br><code>relational_delete(&lt;tbl_var&gt;,&lt;vec_pos&gt;)</code><br><br>


<h2>Parameters</h2>
&lt;tbl_var&gt;: A fully qualified table name.<br/>
&lt;vec_pos&gt;: A vector of positions.

<h2>Return Value</h2>
None.

<h2>Usage</h2>
<br><pre>low_project=select(awesomebase.grades.project,0,10) -- Find the rows with project less than 10
relational_delete(awesomebase.grades,low_project) -- Clearly this is a mistake!!
</pre>

<h2>SQL Example</h2>
<pre>DELETE FROM grades WHERE midterm1 < 40 AND midterm2 < 40 </pre>
<br>

<h1>Joining columns</h1>

This function performs a join between two inputs, given both the values and respective positions of each input. We expect at least a hash and nested-loop join to be implemented, but implementing others (such as sort-merge) is a possibility. 

<br><br><code>&lt;vec_pos1_out&gt;,&lt;vec_pos2_out&gt;=join(&lt;vec_val1&gt;,&lt;vec_pos1&gt;,&lt;vec_val2&gt;,&lt;vec_pos2&gt;, [hash,nested-loop,...])</code><br><br>

<h2>Parameters</h2>
&lt;vec_val_1&gt;: The vector of values 1.<br/>
&lt;vec_pos_1&gt;: The vector of positions 1.<br/>
&lt;vec_val_2&gt;: The vector of values 2.<br/>
&lt;vec_pos_2&gt;: The vector of positions 2.<br/>
&lt;type&gt;: The type of join (i.e. hash, sort-merge)<br/>

<p><strong>NOTE:</strong> There is no explicit indication which is the smaller relation. Why this matters will become apparent when we discuss joins.</p>

<h2>Return Value</h2>
&lt;vec_pos1_out&gt;,&lt;vec_pos2_out&gt;: The concatenation of the positions in each input table of the resulting join.

<h2>Usage</h2>
<br><pre>
positions1=select(awesomebase.cs165.project_score,100,null) -- select positions where project score >= 100 in cs165
positions2=select(awesomebase.cs265.project_score,100,null) -- select positions where project score >= 100 in cs265
values1=fetch(awesomebase.cs165.student_id,positions1)
values2=fetch(awesomebase.cs265.student_id,positions2)
r1, r2 = join(values1,positions1,values2,positions2,hash) -- positions of students who have project score >= 100 in both classes
student_ids = fetch(awesomebase.cs165.student_id, r1)
print(student_ids)
</pre>

<h2>SQL Example</h2>

<pre>SELECT student_id FROM cs165_grades JOIN cs265_grades 
WHERE cs165_grades.project > 100 
AND cs165_grades.project > 100 
AND cs165_grades.student_id = cs265_grades.student_id</pre>
<br>

<h1>Min aggregate</h1>

There are two kinds of min aggregate commands.  <br><br>

<code>&lt;min_val&gt;=min(&lt;vec_val&gt;)</code><br><br>

The first min aggregation signature returns the minimum value of the values held in &lt;vec_val&gt;.

<h2>Parameters</h2>
&lt;vec_val&gt;: A vector of values to search for the min OR a fully qualified name.<br/>

<h2>Return Value</h2>
&lt;min_val&gt;: The minimum value of the input &lt;vec_val&gt;.

<br/><br><br>

The second min aggregation signature returns the minimum value and the corresponding position(s) (as held in &lt;vec_pos&gt;) from the values in &lt;vec_val&gt;.

<code>&lt;min_pos&gt;,&lt;min_val&gt;=min(&lt;vec_pos&gt;,&lt;vec_val&gt;)</code><br><br>

<h2>Parameters</h2>
&lt;vec_pos&gt;: A vector of positions corresponding to the values in &lt;vec_val&gt;.<br>
&lt;vec_val&gt;: A vector of values to search for the min OR a fully qualified name.<br/>
<b>Note: </b> When null is specified as the first input of the function, it returns the position of the min from the &lt;vec_val&gt; array.<!-- This means that if the argmin(vec_val)=i, then:
<br>
<ul>
<li> If vec_posn is null, return i.
<li> If vec_posn is not null, return vec_val[i].
</ul> 
-->

<h2>Return Value</h2>
&lt;min_pos&gt;: The position (as defined in &lt;vec_pos&gt;) of the min.</br>
&lt;min_val&gt;: The minimum value of the input &lt;vec_val&gt;.</br>



<h2>Usage</h2>
<br><pre>
positions1=select(awesomebase.grades.project,100,null) -- select students with project more than or equal to 100
values1=fetch(awesomebase.grades.midterm1,positions1)
-- used here
min1=min(values1) -- the lowest midterm1 grade for students who got 100 or more in their project
</pre>

<h2>SQL Example</h2>
<pre>SELECT min(midterm1) FROM grades WHERE project >= 100 </pre>



<h1>Max aggregate</h1>

There are two kinds of max aggregate commands.  <br><br>

<code>&lt;max_val&gt;=max(&lt;vec_val&gt;)</code><br><br>

The first max aggregation signature returns the maximum value of the values held in &lt;vec_val&gt;.

<h2>Parameters</h2>
&lt;vec_val&gt;: A vector of values to search for the max OR a fully qualified name.<br/>

<h2>Return Value</h2>
&lt;max_val&gt;: The maximum value of the input &lt;vec_val&gt;.

<br/><br><br>

The second max aggregation signaturereturns the maximum value and the corresponding position(s) (as held in &lt;vec_pos&gt;) from the values in &lt;vec_val&gt;.
<code>&lt;max_pos&gt;,&lt;max_vals&gt;=max(&lt;vec_pos&gt;,&lt;vec_val&gt;)</code><br><br>

<h2>Parameters</h2>
&lt;vec_pos&gt;: A vector of positions corresponding to the values in &lt;vec_val&gt;.<br>
&lt;vec_val&gt;: A vector of values to search for the max OR a fully qualified name.<br/>
<b>Note: </b> When null is specified as the first input of the function, it returns the position of the max from the &lt;vec_val&gt; array.<!-- This means that if the argmin(vec_val)=i, then:
<br>
<ul>
<li> If vec_posn is null, return i.
<li> If vec_posn is not null, return vec_val[i].
</ul> 
-->

<h2>Return Value</h2>
&lt;max_pos&gt;: The position (as defined in &lt;vec_pos&gt;) of the max.</br>
&lt;max_val&gt;: The maximum value of the input &lt;vec_val&gt;.</br>

<h2>Usage</h2>
<br><pre>
positions1=select(awesomebase.grades.midterm1,null,90) -- select students with midterm less than 90
values1=fetch(awesomebase.grades.project,positions1)
-- used here
max1=max(values1) -- get the maximum project grade for students with midterm less than 90
</pre>

<h2>SQL Example</h2>
<pre>SELECT MAX(project) FROM grades WHERE midterm1 < 90 </pre>


<h1>Sum aggregate</h1>

<code>&lt;scl_val&gt;=sum(&lt;vec_val&gt;)</code><br><br>

This is the aggregation function sum. It returns the sum of the values in &lt;vec_val&gt;.

<h2>Parameters</h2>
&lt;vec_val&gt;: A vector of values.


<h2>Return Value</h2>
&lt;scl_val&gt;: The scalar value of the sum.


<h2>Usage</h2>
<br><pre>positions1=select(awesomebase.grades.project,100,null) -- select students with project more than or equal to 100
values1=fetch(awesomebase.grades.quizzes,positions1)
-- used here
sum_quizzes=sum(values1) -- get the sum of the quizzes grade for students with project more than or equal to 100
</pre>

<h2>SQL Example</h2>
<pre>SELECT SUM(quizzes) FROM grades WHERE project>=100</pre>


<h1>Average aggregate</h1>

<code>&lt;scl_val&gt;=avg(&lt;vec_val&gt;)</code><br><br>

This is the aggregation function average. It returns the arithmetic mean of the values in &lt;vec_val&gt;.

<h2>Parameters</h2>
&lt;vec_val&gt;: A vector of values.


<h2>Return Value</h2>
&lt;scl_val&gt;: The scalar value of the average. 
For the average operator, in staff automated grading we expect your system to provide 2 places of decimal precision (e.g. 0.00).


<h2>Usage</h2>
<br><pre>positions1=select(awesomebase.grades.project,100,null) -- select students with project more than or equal to 100
values1=fetch(awesomebase.grades.quizzes,positions1)
-- used here
avg_quizzes=avg(values1) -- get the average quizzes grade for students with project more than or equal to 100
</pre>

<h2>SQL Example</h2>
<pre>SELECT AVG(quizzes) FROM grades WHERE project>=100</pre>

<h1>Adding two vectors</h1>

<code>&lt;vec_val&gt;=add(&lt;vec_val1&gt;,&lt;vec_val2&gt;)</code><br><br>

This function adds two vectors of values.

<h2>Parameters</h2>
&lt;vec_val1&gt;: The vector of values 1. <br/>
&lt;vec_val2&gt;: The vector of values 2.


<h2>Return Value</h2>
&lt;vec_val&gt;: A vector of values equal to the component-wise addition of the two inputs.


<h2>Usage</h2>
<br><pre>
midterms=add(awesomebase.grades.midterm1,awesomebase.grades.midterm2)
</pre>

<h2>SQL Example</h2>
<pre>SELECT midterm1 + midterm2 FROM grades</pre>


<h1>Subtracting two vectors</h1>

<code>&lt;vec_val&gt;=sub(&lt;vec_val1&gt;,&lt;vec_val2&gt;)</code><br><br>

This function subtracts two vectors of values.

<h2>Parameters</h2>
&lt;vec_val1&gt;: The vector of values 1. <br/>
&lt;vec_val2&gt;: The vector of values 2.


<h2>Return Value</h2>
&lt;vec_val&gt;: A vector of values equal to the component-wise addition of the two inputs.


<h2>Usage</h2>
<br><pre>
-- used here
score=sub(awesomebase.grades.project,awesomebase.grades.penalty)
</pre>

<h2>SQL Example </h2>
<pre>SELECT AVG(midterm2 - midterm1) FROM grades </pre>

<h1>Updating values</h1>

This function updates values from a column at given positions with a given value.

<br><br><code>relational_update(&lt;col_var&gt;,&lt;vec_pos&gt;,[INT])</code><br><br>

<h2>Parameters</h2>
&lt;col_var&gt;: A variable that indicates the column to update.<br/>
&lt;vec_pos&gt;: A vector of positions.<br/>
INT: The new value.


<h2>Return Value</h2>
None.

<h2>Usage</h2>
<br><pre>
project_to_update=select(awesomebase.grades.project,0,100) -- ...it should obviously be over 100!
-- used here
relational_update(awesomebase.grades.project,project_to_update,113)</pre>

<h2>SQL Example </h2>
<pre>UPDATE grades SET midterm1 = 100 WHERE midterm2 = 100</pre>

<h1>Printing results</h1>

<code>print(&lt;vec_val1&gt;,...)</code><br><br>

The print command prints one or more vector in a tabular format.  
<h2>Parameters</h2>
&lt;vec_val1&gt;: One or more vectors of values to be combined and printed.


<h2>Return Value</h2>
None.

<h2>Usage</h2>
<br><pre>-- used here
print(awesomebase.grades.project,awesomebase.grades.quizzes) -- print project grades and quiz grades
--OR--
pos_high_project=select(awesomebase.grades.project,80,null) -- select project more than or equal to 80
val_project=fetch(awesomebase.grades.project,pos1) -- fetch project grades
val_studentid=fetch(awesomebase.grades.student_id,pos1) -- fetch student id
val_quizzes=fetch(awesomebase.grades.quizzes,pos1) -- fetch quizz grades
print(val_studentid,val_project,val_quizzes) -- print student_id, project grades and quiz grades for projects more than or equal to 80
</pre>

Then, the result should be:<br><br>
<pre>1,107,93
2,92,85
3,110,95
4,88,95</pre>

<h2>SQL Example</h2>
This instruction is used to print out the results of a query. As such, this command is used in every query in a database which returns values. 

<h1>Batching Commands</h1>

Batching consists of two commands. The first command, batch_queries, tells the server to hold the execution of the subsequent requests. The second command, batch_execute, then tells the server to execute these queries. 

<br><br>
<code>batch_queries()</code><br><br>
<code>batch_execute()</code><br>
<h2>Return Value</h2> 
batch_queries: None. <br>
batch_execute: No explicit return value, but the server must work out with the client when it is done sending results of the batched queries. <br><br>
<h2>Usage</h2>
<pre>
batch_queries()
a_plus=select(awesomebase.grades.project,90,100) -- Find the students (rows) with project grade between 90 and 100
a=select(awesomebase.grades.project,80,90) -- Find the students (rows) with project grade between 80 and 90
super_awesome_peeps=select(awesomebase.grades.project,95,105)
ids_of_students_with_top_project=fetch(awesomebase.grades.student_id,a_plus) -- Find the student id of the a_plus students
batch_execute() -- The three selects should run as a shared scan
</pre>

<h2>SQL Example</h2>
There is no batching command in the SQL syntax. However, almost all commercial databases have a command to submit a batch of queries. 

<h1>Shutting down</h1>
This command shuts down the server. Data relating to databases, tables, and columns should be persisted so that they are available again when the server is restarted. Intermediate results and the variable pool should not be persisted.
<br><br>
<code>shutdown</code><br>
<h2>Return Value</h2>
None.<br><br>
<h2>Usage</h2>
<pre>
shutdown
</pre>



</li>
</details>



Here is a sample tests on insertion, select, fetch, sum, and print.  
```
create(db,"db1")
create(tbl,"tbl1",db1,2)
create(col,"col1",db1.tbl1)
create(col,"col2",db1.tbl1)
load("test.csv")
relational_insert(db1tbl1,-1,-11)
relational_insert(db1tbl1,-2,-22)
relational_insert(db1tbl1,-3,-33)
relational_insert(db1tbl1,-4,-44)
relational_insert(db1tbl1,-5,-55)
relational_insert(db1tbl1,-6,-66)
relational_insert(db1tbl1,-7,-77)
relational_insert(db1tbl1,-8,-88)
relational_insert(db1tbl1,-9,-99)
s1=select(db1.tbl1.col1,-45869,34131)
f1=fetch(db1.tbl1.col2,s1)
a1=sum(f1)
print(a1)

shutdown
```


# Todo

[-] Implement updates
[-] Implement Delets
[-] Implement Grace Join
[-] Improve design based on Inputs 
[-] Implement SIMDS


# Credit 




