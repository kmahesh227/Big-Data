package WordCount.WordCount;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private  Map<String, String> wordList = null;

    @Override
    public void setup(Context context) {
    	Configuration conf = context.getConfiguration();
    	Path pt = new Path("/user/mxk166930/positive-words.txt");
    	Path pt2 = new Path("/user/mxk166930/negative-words.txt");
    	BufferedReader br,br2;
    	try {
    	FileSystem fs = FileSystem.get(conf);
    	br=new BufferedReader(new InputStreamReader(fs.open(pt)));
    	br2=new BufferedReader(new InputStreamReader(fs.open(pt2)));
    	wordList = new HashMap<String, String>();
    	  String word ;
   // Maps the matched word with Positive Tag Value. 	 
    	  while ((word=br.readLine())!= null){
    		  if(!word.startsWith(";")){
    			  wordList.put(word, "POSITIVE");
    		  }
    	  }
   // Maps the matched word with Negative Tag Value.    	  
    	  while ((word=br2.readLine())!= null){
      		  if(!word.startsWith(";")){
      			  System.out.println(word);    		  		
      			wordList.put(word, "NEGATIVE");
      		  }
      	  }
    	} catch(Exception e) {
    	  e.printStackTrace();
    	}
    }
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
     
      String token;
      try {
		 while (itr.hasMoreTokens()) {
			 token = itr.nextToken().trim().toLowerCase();   
			if(wordList.containsKey(token)){	
				word.set(wordList.get(token));
		        context.write(word, one);
				}
		  }
	} catch (Exception e) {
		e.printStackTrace();
		}     
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int psum = 0;
      int nsum = 0;
      String k = key.toString();
      if(k.equals("POSITIVE")){
    	  for (IntWritable val : values) {
    		  psum += val.get();
    	  }
      context.write(new Text("Number of Positive Words : "), new IntWritable(psum));
      }
      else if(k.equals("NEGATIVE")){
    	  for (IntWritable val : values) {
    		  nsum += val.get();
    	  }
      context.write(new Text("Number of Negative Words : "), new IntWritable(nsum));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
    conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
    conf.set("mapreduce.framework.name", "yarn");
    Job job = Job.getInstance(conf, "Count of Positive and negative words");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setNumReduceTasks(1);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}