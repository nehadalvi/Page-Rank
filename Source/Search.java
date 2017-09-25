/*
 * 
 * Neha Kishor Dalvi
 * ndalvi1@uncc.edu
 * 
 */

import java.io.IOException;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class Search extends Configured implements Tool {

    
    private static final String SEARCHQUERY = "searchQuery";

    public static void main(String[] args) throws Exception {
        
        int res = ToolRunner.run(new Search(), args);
        System.exit(res);
    }

    /*
     * This method takes the query words passed as command line arguments and appends it to the 
     * configuration as array of strings.
     */
        
  
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "search");
        job.setJarByClass(this.getClass());
        
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        /*
         * size of String args is greater than String query by 2, since it contains two additional arguments for storing paths
         */
        

        String[] query = new String[args.length - 2]; 
        for (int i = 2; i < args.length; i++) {  //passing third arg onwards to string query.
            String arg = args[i];
            query[i - 2] = arg;
        }
        
        job.getConfiguration().setStrings(SEARCHQUERY, query);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /*
     * Mapper searches for the searchquery in the file line by line.
     * If present it will send <file, tfidf> as <key,value> pair to reducer.
     */
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        TreeSet<String> words; //treeset for storing words from the query

        /*
         * This method reads the query keywords which are to be searched.
         * It executes only once before map method is called.
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
        	words = new TreeSet<>();
            String[] s = context.getConfiguration().getStrings(SEARCHQUERY); //fetches the query string based on the keyword 'SEARCHQUERY'
            
            if (s != null) {
                for (int i = 0; i < s.length; i++) {
                    String term = s[i];
                    words.add(term.toLowerCase());
                }
            } else {
                words.add("null");
            }
        }

        /*
         * This method checks for keyword, and sends filename and tfidf values to reducer.
         */
        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
           
            //input fetched line by line
            String line = lineText.toString();
            
            String[] splitLine = line.split("#####");
            if (words.contains(splitLine[0].toLowerCase())) {  //if word found
               
                String[] keyValue = splitLine[1].split("\t");
                context.write(new Text(keyValue[0]), new Text(keyValue[1]));
            } else {
                //no word found
            }
        }

    }
    
    /*
     * This Reducer receives all tfidf values associated with a file, adds them and outputs this value 
     * along with with file name. 
     */

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           
            double sum = 0;
            for (Text value : values) {
                try {
                    sum += Double.parseDouble(value.toString());
                } catch (Exception e) {
                    context.write(key, value);
                }
            }
            Text t = new Text("" + sum);
            context.write(key, t);

        }
    }

}

