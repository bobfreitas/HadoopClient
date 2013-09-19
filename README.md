HadoopClient
============

Client to be able to submit dynamic jobs to a cluster.  The basic idea 
is to use reflections in a generic job running class.  You would pass in 
a set of parameters of strings, and then it would use reflection to load 
the actual classes from the strings.  

The example code was kept simple to illustrate the basic concept.  An actual
implementation in a production system would need to be much more complicated.  

This was done with CDH 4.3 and Eclipse.  
