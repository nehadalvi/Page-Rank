/*
 * 
 * Neha Kishor Dalvi
 * ndalvi1@uncc.edu
 * 
 */

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
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



public class TermFrequency extends Configured implements Tool{
	//private static final Logger LOG = Logger .getLogger( DocWordCount.class);

	   public static void main( String[] args) throws  Exception {
	      int res  = ToolRunner .run( new TermFrequency(), args);
	      System .exit(res);
	   }

	   public int run( String[] args) throws  Exception {
	      Job job  = Job .getInstance(getConf(), " termfrequency ");
	      job.setJarByClass( this .getClass());

	      FileInputFormat.addInputPaths(job,  args[0]);
	      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
	      job.setMapperClass( Map .class);
	      job.setReducerClass( Reduce .class);
	      job.setOutputKeyClass( Text .class);
	      job.setOutputValueClass( DoubleWritable .class);

	      return job.waitForCompletion( true)  ? 0 : 1;
	   }
	   
	   /*
	    * This Mapper class is same as DocWordCount class' Mapper.
	    */
	   
	   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  DoubleWritable > {
	      private final static DoubleWritable one  = new DoubleWritable( 1);

	      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

	      public void map( LongWritable offset,  Text lineText,  Context context)
	        throws  IOException,  InterruptedException {

	         String line  = lineText.toString();
	         Text currentWord  = new Text();
	         
	         // fetching file name using Context object.
	         InputSplit inputSplit = context.getInputSplit();
	         String fileName = ((FileSplit) inputSplit).getPath().getName();

	         for ( String word  : WORD_BOUNDARY .split(line)) {
	            if (word.isEmpty()) {
	               continue;
	            }
	            
	            //appending file name to '#####' and word.This is the key which is passed to the reducer.
	            currentWord  = new Text(word.toLowerCase()+"#####"+fileName);
	            context.write(currentWord,one);
	         }
	      }
	   }
	   
	   /*
	    * This Reducer class calculates the term frequency of every key(term) by summing up their values.
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
	        	 tf = 1+Math.log10(sum);  //if term freq is greater than 0
	         }else{
	        	 tf=0; //otherwise term freq is 0.
	         }
	         context.write(word,  new DoubleWritable(tf));
	      }
	   }

}
