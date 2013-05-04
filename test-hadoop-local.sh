#!/bin/bash
# Compiles java code, jars it, and executes in hadoop.  deletes output folder before 

HADOOP_VERSION=1.1.2
HADOOP_HOME=~/hadoop-1.1.2

# compile java
rm -r java/MapReduce/bin
mkdir java/MapReduce/bin
javac -classpath ${HADOOP_HOME}/hadoop-core-${HADOOP_VERSION}.jar -d java/MapReduce/bin java/MapReduce/src/org/columbia/*.java

# jar java file (it requires 'cd'ing into the proper path...not sure how to get the 'jar' app to work without all these 'cd's)
rm java/MapReduce/EmailGrapher.jar
cd java/MapReduce/bin
jar cvf EmailGrapher.jar ../bin .
cd ..
cd ..
cd ..

# run hadoop on java jar
rm -r test-output # remove existing output
~/hadoop-1.1.2/bin/hadoop jar java/MapReduce/bin/EmailGrapher.jar org.columbia.ExtractEmailAddresses test-input test-output