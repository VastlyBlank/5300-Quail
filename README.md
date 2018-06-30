# 5300-Quail

This is our implementaiton of a DBMS interpereter in this folder are:

sql5300.cpp <- The Milestone 1 implementation
Makefile <- used for building the DBMS
LICENCE <- the agreement and terms of use of Berkley DB

We will use RecNo file types and storing one of our database blocks
per BerkeleyDb record. Documentation is here:
        http://docs.oracle.com/cd/E17076_05/html/index.html

To run a program linked against our private Berkeley DB library, you have
to export LD_LIBRARY_PATH. So do this (you can put it in your ~/.bash_profile,
without the leading $ prompt):

$ export LD_LIBRARY_PATH=/usr/local/db6/lib:$LD_LIBRARY_PATH

You can build the example using the Makefile:

$ make

Then you can run the program that's been linked against the Berkeley DB
software library. The program reqires a r/w directory path as a command 
line agruemnt which is where the DB will be constructed ie:

$ ./sql5300 /home/st/mouserj/cpsc5300/data

