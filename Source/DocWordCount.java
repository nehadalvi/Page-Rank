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
import org.apache.hadoop.io.IntWritable;
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



public class DocWordCount extends Configured implements Tool {
	//private static final Logger LOG = Logger .getLogger( DocWordCount.class);

	   public static void main( String[] args) throws  Exception {
	      int res  = ToolRunner .run( new DocWordCount(), args);
	      System .exit(res);
	   }

	   public int run( String[] args) throws  Exception {
	      Job job  = Job .getInstance(getConf(), " docwordcount ");
	      job.setJarByClass( this .getClass());
	      	
	      FileInputFormat.addInputPaths(job,  args[0]);
	      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
	      job.setMapperClass( Map .class);
	      job.setReducerClass( Reduce .class);
	      job.setOutputKeyClass( Text .class);
	      job.setOutputValueClass( IntWritable .class);

	      return job.waitForCompletion( true)  ? 0 : 1;
	   }
	   
	   /*
	    * This Mapper class reads data from the specified file path and for each word fetches the filename in which it occurs.
	    * The word and file name are separated by a delimiter '#####' and this forms the key to be sent to the Reducer. 
	    * The value of each key is set to 1.
	    */
	   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
	      private final static IntWritable one  = new IntWritable( 1);
	      //private Text word  = new Text();

	      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

	      public void map( LongWritable offset,  Text lineText,  Context context)
	        throws  IOException,  InterruptedException {

	         String line  = lineText.toString();
	         Text currentWord  = new Text();
	         
	         /*
	          * fetching file name using Context object.
	          */
	         InputSplit inputSplit = context.getInputSplit();
	         String fileName = ((FileSplit) inputSplit).getPath().getName();

	         for ( String word  : WORD_BOUNDARY .split(line)) {
	            if (word.isEmpty()) {
	               continue;
	            }
	            currentWord  = new Text(word.toLowerCase()+"#####"+fileName); //append the delimiter '#####' and filename to the word. This is the key.
	            context.write(currentWord,one);
	         }
	      }
	   }
	   
	   /*
	    * The Reducer receives word + filename as key and thus counts the number of times this name,filename pair occur 
	    * together. It is similar to the Reducer of WordCount program.
	    */

	   public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  IntWritable > {
	      @Override 
	      public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
	         throws IOException,  InterruptedException {
	         int sum  = 0;
	         for ( IntWritable count  : counts) {
	            sum  += count.get();
	         }
	         context.write(word,  new IntWritable(sum));
	      }
	   }

}
