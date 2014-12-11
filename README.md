whitecat
========

Just a UDAF example of grouping a complex data type into a list. Please test before using in production. 

ADD JAR whitecat-1.0.jar;

CREATE TEMPORARY FUNCTION CollectStructs AS 'siwicki.whitecat.CollectStructs';

example use:

hive> select dtstmp,CollectStructs(named_struct("stock_id", id, "value", dtstmp)) from loadtest group by dtstmp;

20140101	[{"stock_id":4,"value":"20140101"},{"stock_id":5,"value":"20140101"},{"stock_id":7,"value":"20140101"},{"stock_id":5,"value":"20140101"},{"stock_id":7,"value":"20140101"},{"stock_id":1,"value":"20140101"},{"stock_id":5,"value":"20140101"},{"stock_id":3,"value":"20140101"},{"stock_id":5,"value":"20140101"},{"stock_id":7,"value":"20140101"},{"stock_id":5,"value":"20140101"},{"stock_id":7,"value":"20140101"},{"stock_id":5,"value":"20140101"},{"stock_id":7,"value":"20140101"},{"stock_id":5,"value":"20140101"},{"stock_id":7,"value":"20140101"},{"stock_id":5,"value":"20140101"},{"stock_id":7,"value":"20140101"},{"stock_id":2,"value":"20140101"},{"stock_id":5,"value":"20140101"},{"stock_id":7,"value":"20140101"},{"stock_id":5,"value":"20140101"},{"stock_id":7,"value":"20140101"}]

or

hive> create table complex as select dtstmp,CollectStructs(named_struct("stock_id", id, "value", dtstmp)) from loadtest group by dtstmp;
Total MapReduce jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Estimated from input data size: 1
.......

which is a query projecting a base table into a complex type - i.e. an object containing a list of structs.

it can be described as below

hive> describe complex;
OK
dtstmp              	string              	None                
_c1                 	array<struct<stock_id:int,value:string>>	None                
Time taken: 0.086 seconds, Fetched: 2 row(s)
hive> 

which is an example of a table of complex nested types.

hive> select * from complex;
OK
20140101	[{"stock_id":4,"value":"20140101"},{"stock_id":5,"value":"20140101"},{"stock_id":7,"value":"20140101"},{"stock_id":5,"value":"20140101"},{"stock_id":7,"value":"20140101"},{"stock_id":1,"value":"20140101"},{"stock_id":5,"value":"20140101"},{"stock_id":3,"value":"20140101"},{"stock_id":5,"value":"20140101"},{"stock_id":7,"value":"20140101"},{"stock_id":5,"value":"20140101"},{"stock_id":7,"value":"20140101"},{"stock_id":5,"value":"20140101"},{"stock_id":7,"value":"20140101"},{"stock_id":5,"value":"20140101"},{"stock_id":7,"value":"20140101"},{"stock_id":5,"value":"20140101"},{"stock_id":7,"value":"20140101"},{"stock_id":2,"value":"20140101"},{"stock_id":5,"value":"20140101"},{"stock_id":7,"value":"20140101"},{"stock_id":5,"value":"20140101"},{"stock_id":7,"value":"20140101"}]
20140102	[{"stock_id":3,"value":"20140102"}]
Time taken: 0.083 seconds, Fetched: 2 row(s)




