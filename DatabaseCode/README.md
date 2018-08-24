Author: Jonah Taurog  
June - August, 2018

This project is a multi-user, multi-threaded relational database.
The project consists of three servers: API, Database and resurrection.
We first start up the resurrection server which then starts 
up the API and Database servers. 
Then after that, every 1 second, the Resurrection server will send a ping 
to the other 2 servers to see if they are running. If it finds those servers 
are down, it will restart them. 

Meanwhile, the way this system works is that the API server receives a SQL query
in the form of a GET request from a client and sends that query to the Database Server.
If it's a SELECT query, it sends a GET request and any other query is a POST.
The Database server takes that query and calls on the Database Driver to
execute the query and perform all the database logic on it. 
The Database Driver then returns a ResultSet object back to the Database Server.
This ResultSet object has several fields:
1. A list of all the column names.
2. A list of the column types.
3. A list of lists, which is the actual table the user is dealing with. 
4. A  result which is an object. The result can simple be a boolean, which 
    is true if the query was successful, or false if it's not. The result
    can also be the result of a SELECT query, such as giving back the average of 
    a column.
5. A String version that summarizes the ResultSet. Consisiting of a table,
 with the column names at the top and the result below the table
   
Once the ResultSet object is returned, the Database Server take the string version of
the ResultSet and sends that string as a response to the API Server. The API server 
then sends the response to the client that sent the request. 

Regarding the multi-threading aspect of this project, the servers can accept 
multiple requests at once. The requests come in and are sent to a threading pool
with a capacity of 20 threads and are executed FIFO style. 
Since multiple users can access the database at once, I had to go through the process of 
making the database locksafe in order to prevent erroneous responses back to users or unwanted
changes to the table.
I locked down database by giving a ReentrantReadWriteLock to each table, and to each column and row within
the table as well as to each Btree. In order to avoid deadlock, I be sure to lock and unlock
the locks in exactly the same order. Addition, by surrounding the locks in a try/finally block and unlocking the
locks in the finally block, the locks are 100% guaranteed to release their locks. 

Regarding memory, everytime something is written to the database, the database gets saved.
I have Serialized the Database object and every time theres a write, it 
saves the object to a file on the computer. So whenever the Database server boots up it 
grabs that file and deserializes it back the Database object. This way, if the server ever 
crashes, the database will still be there.