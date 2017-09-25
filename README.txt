Class Names:
DocWordCount.java
TermFrequency.java
TFIDF.java
Search.java
Rank.java

Jar file: 
assign2.jar (This jar file contains the source code of all the above java classes)


Execution instructions (Required steps):
1. Create a new directory by the name assign2 inside the /user/cloudera directory by executing the following command in terminal:
   hadoop fs -mkdir /user/cloudera/assign2

2. Create a new input directory inside assign2.
   hadoop fs -mkdir /user/cloudera/assign2/input

3. Put the files extracted to the /user/cloudera/assign2/input directory
   hadoop fs -put alice29.txt asyoulik.txt cp.html fields.c grammar.lsp lcet10.txt plrabn12.txt xargs.1 /user/cloudera/assign2/input


Execution steps using Jar file provided (assign2.jar):- 
1. Run DocWordCount class by providing first argument as input path and second argument as output path.
   hadoop jar assign2.jar DocWordCount /user/cloudera/assign2/input /user/cloudera/assign2/outputdocwordcount

2. Check the output in the directory /user/cloudera/assign2/outputdocwordcount
   hadoop fs -cat /user/cloudera/assign2/outputwordcount/*

3. Run TermFrequency class by providing first argument as input path and second argument as output path.
   hadoop jar assign2.jar TermFrequency /user/cloudera/assign2/input /user/cloudera/assign2/outputtermfreq

4. Check the output in the directory /user/cloudera/assign2/outputtermfreq
   hadoop fs -cat /user/cloudera/assign2/outputtermfreq/*

5. Run TFIDF class by providing first argument as input path, second argument as output path of the first job run and third argument as the output path of the second
   job run.
   hadoop jar assign2.jar TFIDF /user/cloudera/assign2/input /user/cloudera/assign2/outputtfidf1 /user/cloudera/assign2/outputtfidf2

6. Check the output in the directory /user/cloudera/assign2/outputtfidf2
   hadoop fs -cat /user/cloudera/assign2/outputtfidf2/*

7. Run Search class by providing input path as first argument and output path as second argument, the susequent arguments are search terms.
   For Search, the input file is the output of TFIDF program.
   hadoop jar assign2.jar Search /user/cloudera/assign2/outputtfidf2 /user/cloudera/assign2/outputsearch computer science

8. Check the output in the directory /user/cloudera/assign2/outputsearch 
   hadoop fs -cat /user/cloudera/assign2/outputsearch/*

9. Run Rank class by providing input path as first argument and output path as second argument.
   For Rank, the input file is the output of Search program.
   hadoop jar assign2.jar Search /user/cloudera/assign2/outputsearch /user/cloudera/assign2/outputrank

10. Check the output in the directory /user/cloudera/assign2/outputrank 
   hadoop fs -cat /user/cloudera/assign2/outputrank/*

Execution steps without the jar file:-
For every java class do the following:
1. Compile the Java class.
   mkdir -p build
   javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* DocWordCount.java -d build -Xlint 
2. Create a JAR file for the application.
   jar -cvf docwordcount.jar -C build/ . 
3. Run the program from the JAR file by passing input and output paths.
   hadoop jar docwordcount.jar DocWordCount /user/cloudera/assign2/input /user/cloudera/assign2/outputdocwordcount
4. Check the output.
   hadoop fs -cat /user/cloudera/assign2/outputwordcount/*


Notes:
* TFIDF requires 3 arguments: one for input path, second for output path of first MR job, third for output path of second MR job.
* The output path of TFIDF becomes the input path for Search program. The search program also takes additional arguments which are the words to be searched.
* The output path of Search becomes the input path for Rank program.
* DocWordCount, TermFrequency and TFIDF require 2 arguments: one is input path which contains all the files on which search is to be carried out, 
  second is the output path to store output data.
* While specifying the output directory path in the second argument, one has to make sure that the directory is not already existing.

