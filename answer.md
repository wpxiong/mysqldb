## Task 1

Consider the following points for the current state of the library.

* Does the library fulfill the requirements described in the background section?

answer: this library didn't filfill the requirements, because if one slave db is crashed, but when we execute query sql
we will find some sql will execute by crashed slavedb ,and always fail.

  
* Is the library easy to use?

answer: the library is not very easy to use, because ,when some slave db didn't collect , we will found 
  the ping method will return error ,so db can't be used,
  this didn't make sense
  
* Is the code quality assured?
answer: 
 answer: the source code quality is not assured, i find some bug for example if the value of count is over the max value,
 if execute count++ , program will crash.

* Is the code readable?
answer: the code is also not readable,  did't use any design mode to write source. 
i think we should provide a interface to operate db 
and then we can provide an implementation for this operate.
so in future, if we want to rewrite the source , i think we shouldn't 

* Is the library thread-safe?
answer: the library is not thread-safe, for example  this method readReplicaRoundRobin didn't consider mutil-thread
the db.count will not safe, we should use atomic to increase db.count



## Task 3

Explain what you did in the previous task and your reasoning behind it.

In the previous task, i developed a distribute system for payment .
and we should make sure one transaction must be success in many db.
event one transaction in one db has an exception, whether let all of the local transcation success or 
fail. i developed  some framework for supporting distribute transction .
and also i developed a shareding db framework for automaticly select one db to insert or query.
for example if we want to insert a order in db ,but if we only one physical db , i think maybe there is performance issue
so i think we can use many physical db to save data.
but we should know which data should query from physical db.
and form application it only can use these physical db  as one logic db .
