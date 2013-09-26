HadoopClient
============

Client to be able to submit dynamic jobs to a cluster.  The basic idea 
is to use reflections in a generic job running class.  You would pass in 
a set of parameters of strings, and then it would use reflection to load 
the actual classes from the strings.  

The example code was kept simple to illustrate the basic concept.  An actual
implementation in a production system would need to be much more complicated.  
In fact, this approach can be used to create a web interface.  

This was done with CDH 4.3 and Eclipse.  

There is a supporting article on my website that discusses how this can be 
used to create a Hadoop web interface, go the URL to learn more:

http://www.lopakalogic.com/articles/hadoop-articles/hadoop-web-interface/

