
/*
 * 
 * Neha Kishor Dalvi
 * ndalvi1@uncc.edu
 * 
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import org.apache.log4j.Logger;



public class TFIDF extends Configured implements Tool{
	//private static final Logger LOG = Logger .getLogger( TFIDF.class);
	private static final String FILECOUNT = "filecount"; 

	   public static void main( String[] args) throws  Exception {
	      int res  = ToolRunner .run( new TFIDF(), args);
	      System .exit(res);
	   }

	   /*
	    * Created two jobs, output of Reducer in the first job is sent as input to the Mapper of the second job. 
	    * 
	    */
	   public int run( String[] args) throws  Exception {
		   
		  //Creation of first job
	      Job job  = Job .getInstance(getConf(), " idf ");
	      job.setJarByClass( this .getClass());

	      FileInputFormat.addInputPaths(job,  args[0]);
	      FileOutputFormat.setOutputPath(job,new Path(args[1]));
	      job.setMapperClass( Map .class);
	      job.setReducerClass( Reduce .class);
	      job.setOutputKeyClass( Text .class);
	      job.setOutputValueClass( DoubleWritable .class);
	     
	      boolean success = job.waitForCompletion(true);  //check if first job completed.
	     
	      if(success){	//if job 1 completed
	    	  /*The below lines will count the number of input files from input directory and add it
	    	   * to the context object. Job will then read the count from this context object.
	    	   * 
	    	   */
	    	  FileSystem fs = FileSystem.get(getConf());
	          Path path = new Path(args[0]);
	          ContentSummary cs = fs.getContentSummary(path);
	          long inputFiles = cs.getFileCount();
	          getConf().set(FILECOUNT, ""+inputFiles);
	    	  
	          //Creation of second job
	    	  Job job2  = Job .getInstance(getConf(), " tfidf ");
		      job2.setJarByClass( this .getClass());

		      FileInputFormat.addInputPaths(job2,  args[1]); //output path of first job is set as input to second job.
		      FileOutputFormat.setOutputPath(job2,  new Path(args[2]));
		      job2.setMapperClass( MapTwo .class);
		      job2.setReducerClass( ReduceTwo .class);
		      job2.setOutputKeyClass( Text .class);
		      job2.setMapOutputValueClass(Text.class);
		      job2.setOutputValueClass( DoubleWritable .class);
		      return job2.waitForCompletion(true) ? 0 : 1;
	      }

	      return job.waitForCompletion( true)  ? 0 : 1;
	      
	   }
	   
	   /*
	    * This Mapper will read words from the input file path,fetch the filename and append '#####' 
	    * and file name to the word to form a key. The value of each key is set to 1. Similar to Mapper of TermFrequency class.
	    * 
	    */
	   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  DoubleWritable > {
	      private final static DoubleWritable one  = new DoubleWritable( 1);

	      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

	      public void map( LongWritable offset,  Text lineText,  Context context)
	        throws  IOException,  InterruptedException {

	         String line  = lineText.toString();
	         Text currentWord  = new Text();
	         
	         // fetching file name using context object
	         InputSplit inputSplit = context.getInputSplit();
	         String fileName = ((FileSplit) inputSplit).getPath().getName();

	         for ( String word  : WORD_BOUNDARY .split(line)) {
	            if (word.isEmpty()) {
	               continue;
	            }
	            
	            //appending file name to '#####' and word and this is the key which is passed to the reducer
	            currentWord  = new Text(word.toLowerCase()+"#####"+fileName);
	            context.write(currentWord,one);
	         }
	      }
	   }
	   
	   /*
	    * Reducer of first Job. Same as reducer of TermFrequency class.
	    */
	   
	  
	   public static class Reduce extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {
	      @Override 
	      public void reduce( Text word,  Iterable<DoubleWritable > counts,  Context context)
	         throws IOException,  InterruptedException {
	         double sum  = 0;
	         double tf = 0;
	         for ( DoubleWritable count  : counts) {
	            sum  += count.get();
	         }
	         if(sum>0){
	        	 tf = 1+Math.log10(sum);
	         }else{
	        	 tf=0;
	         }
	         context.write(word,  new DoubleWritable(tf));
	      }
	   }
	   
	   /*
	    * This is the Mapper of Job2. It's input is the output of Job1's Reducer which is <key,value> 
	    * where key is word<delimiter>filename and key is word count. This Mapper will split the key using the delimiter(#####)
	    * into <key,value> which is word and its data. This data contains file name and tab separated TF value.
	    * The <key,value> pair is then passed to the Reducer of Job 2.
	    */
	   public static class MapTwo extends Mapper<LongWritable ,  Text, Text , Text>{
		  	   
		@Override
		protected void map(LongWritable offset,  Text lineText, Mapper<LongWritable ,Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			Text key, value;
			String line  = lineText.toString();
			if(line.isEmpty())
				return;
			
			String[] keyValuePair = line.split("#####");
			if (keyValuePair.length < 2) {
	                return;
	        }
			//String valueString = keyValuePair[1].replace("\t", "=");
			key = new Text(keyValuePair[0]);
			value = new Text(keyValuePair[1]);
			context.write(key,value);
		
		}
		   
	   }
	   
	   /*
	    * Reducer of Job 2. It takes <word, file name + tf> as <key,value> pair and computes the tfidf value and 
	    * sends it to the output file (3rd command line argument).
	    */
	   public static class ReduceTwo extends Reducer<Text , Text, Text, DoubleWritable>{

		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1,Reducer<Text, Text, Text, DoubleWritable>.Context arg2)
				throws IOException, InterruptedException {
			
			//ArrayList defined for storing values which are then used to calculate tfidf by iterating over the list.
			ArrayList<Text> fileList = new ArrayList<>();
			
			//fetching the number of files/documents from the context's configuration
			long totalDocs = arg2.getConfiguration().getLong(FILECOUNT, 1);
			
            double idf = 0;
            int docsContainingTerm = 0;
           
            for (Text text : arg1) {
                fileList.add(new Text(text.toString())); //passing all the values received to an arraylist
            }
			
            //looping over ArrayList to calculate tfidf value
            for(Text files:fileList){
            	String[] splitKey = files.toString().split("\t");
            	double tfidf = 0;
            	docsContainingTerm = fileList.size();    //size of arraylist equals number of docs containing the word/term
            	idf = Math.log10(1+ totalDocs/docsContainingTerm);
            	tfidf = Double.parseDouble(splitKey[1])*idf;
            	arg2.write(new Text(arg0.toString()+"#####"+splitKey[0]),new DoubleWritable(tfidf));
            }
		}
		   
   }

}
