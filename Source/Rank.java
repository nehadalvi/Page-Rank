/*
 * 
 * Neha Kishor Dalvi
 * ndalvi1@uncc.edu
 * 
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class Rank extends Configured implements Tool{
	
    public static void main(String[] args) throws Exception { 
    	int res = ToolRunner.run(new Rank(), args);
	    System.exit(res);
    } 
    
    /*
     * Driver function that creates an MR job and sets mapper class and reducer class.
     */
	@Override
	public int run(String[] args) throws Exception {
		
		Job jobRank = Job.getInstance(getConf(), "rank");
        jobRank.setJarByClass(this.getClass());
        
        FileInputFormat.addInputPaths(jobRank, args[0]);
        FileOutputFormat.setOutputPath(jobRank, new Path(args[1]));
        jobRank.setMapperClass(MapForRank.class);
        jobRank.setReducerClass(ReduceForRank.class);
        jobRank.setOutputKeyClass(Text.class);
        jobRank.setOutputValueClass(DoubleWritable.class);
        jobRank.setMapOutputKeyClass(DoubleWritable.class);
        jobRank.setMapOutputValueClass(Text.class);
        jobRank.setSortComparatorClass(Comparator.class);
        jobRank.setNumReduceTasks(1);
        return jobRank.waitForCompletion(true) ? 0 : 1;

	}
	
	/*
	 * This Mapper class reads the file(output of Search.java) line by line and splits the line into <key,value> pair.
	 * They key here is the tfidf value and value is the filename. Different files can have the same tfidf value and we want
	 * to sort the output based on tfidf. So it should be the key.
	 */
	
	public static class MapForRank extends Mapper<LongWritable, Text, DoubleWritable, Text> {
		
		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			
            //input fetched line by line
            String line = lineText.toString();  
            if(line.isEmpty())
            	return;
            String[] splitLine = line.split("\t");
            context.write(new DoubleWritable(Double.parseDouble(splitLine[1])),new Text(splitLine[0]));
 
        }
	}
	
	/*
	 * This Reducer class receives key,value pairs from the Mapper, interchanges them and generates the output.
	 */
	
	public static class ReduceForRank extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
		
		
		protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
		
			for(Text file:values){
				context.write(file, key);
			}	
		}
	}
	
	/*
	 * This comparator class compares the keys received from the mapper and sorts them in descending order
	 */
	
	public static class Comparator extends WritableComparator{
		
		protected Comparator(){
			super(DoubleWritable.class,true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			DoubleWritable key1 = (DoubleWritable) a;
			DoubleWritable key2 = (DoubleWritable) b;
			
			return -1*key1.compareTo(key2);
		}
		
		
	}

}
