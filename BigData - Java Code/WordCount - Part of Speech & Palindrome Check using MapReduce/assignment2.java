package JavaHDFS.JavaHDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.*;
import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;
import java.util.HashMap;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class assignment2 {

	static Text valWord = new Text("1word1");
	static Text valPal = new Text("1palindrome1");
	private static HashMap<String,String> posMap = new HashMap<String,String>();

	
public static class myMapper extends Mapper<Object, Text, IntWritable,Text>{

	private Text word = new Text();
	
	
	public static boolean checkPalindrome(Text word)
	{
		String testWord = word.toString();
		int length = testWord.length();
		
		for(int i =0; i< length/2;i++)
		{
			if(testWord.charAt(i)!= testWord.charAt(length-i-1))
			{
				return false;
			}
		}
		
		return true;
	}

	
	
	void parseWordsList(FileSystem fs, Path wordsListPath) {
        //HashSet<String> words = new HashSet<String>();
        try {

            if (fs.exists(wordsListPath)) {
                FSDataInputStream fi = fs.open(wordsListPath);

                BufferedReader br = new BufferedReader(new InputStreamReader(fi));
                String line = null;
                String[] wordTag;
                while ((line = br.readLine()) != null) {
                    if (line.length() > 0 ) {
                        wordTag = line.split("\\*");
                       posMap.put(wordTag[0], wordTag[1]);
                    }
                }

                fi.close();
          
        }
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        //return words;
    }
	
	
	@Override
    public void setup(Context context) {
        Configuration conf = context.getConfiguration();
        try {
            FileSystem fs = FileSystem.get(conf);

            String posWordsLocation = conf.get("job.posfile.path");
            parseWordsList(fs, new Path(posWordsLocation));
            //negativeWords = parseWordsList(fs, new Path(negativeWordsLocation));

            //fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }	
	

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		StringTokenizer itr = new StringTokenizer(value.toString());
		int length;
		String strLength = new String();
		IntWritable keyWord = new IntWritable();
		String tag = new String();

		while (itr.hasMoreTokens()) 
		{
			word.set(itr.nextToken());
			
			length = word.getLength();
			strLength = Integer.toString(length);
			if(length >5)
			{
				keyWord.set(length);

				if(true == checkPalindrome(word))
				{
					//valPal.set();
					context.write(keyWord,valPal);

				}
				if(posMap.containsKey(word.toString()))
				{
				tag = posMap.get(word.toString());
				context.write(keyWord,new Text(tag));

				}
			}

		}
	}
}





public static class IntSumReducer
     extends Reducer<IntWritable,Text,Text,IntWritable> {
  private IntWritable result = new IntWritable();

  int wordCount = 0;
  int palCount = 0; 
  int[] posCount = new int[15];
		  
		
  public void reduce(IntWritable key, Iterable<Text> values,
                     Context context ) throws IOException, InterruptedException {
    
	  wordCount = 0;
	  palCount =0;
	  
	  
	  for(int i=0;i<posCount.length;i++)
	  {
		  posCount[i] = 0;
	  }
    for (Text val : values) {
    
		wordCount = wordCount + 1;
		String strVal = new String(val.toString());
   	   if(strVal.compareTo(valPal.toString()) == 0)
   	   {
   		palCount = palCount+1;
   	  }
   	   else
   	   {
   		wordCount= wordCount+1;
   		char[] valArray = strVal.toCharArray();
   		switch(valArray[0])
   		{
   		case 'N' : posCount[0]++;
   					break;
   		case 'p' : posCount[1]++;
   					break;
   		case 'h' : posCount[2]++;
			break;
   		case 'V' : posCount[3]++;
			break;
   		case 't' : posCount[4]++;
			break;
   		case 'i' : posCount[5]++;
			break;
   		case 'A' : posCount[6]++;
			break;
   		case 'v' : posCount[7]++;
			break;
   		case 'C' : posCount[8]++;
			break;
   		case 'P' : posCount[9]++;
			break;
   		case '!' : posCount[10]++;
			break;
   		case 'r' : posCount[11]++;
			break;
   		case 'D' : posCount[12]++;
			break;
   		case 'I' : posCount[13]++;
			break;
   		case 'o' : posCount[14]++;
			break;	
   		}
   	   }
    }
    result.set(wordCount);
    context.write(new Text("Length: "),key);
    context.write(new Text("Count of Words: "), new IntWritable((wordCount)));

   result.set(palCount);
    context.write(new Text("Palindrome count"),result);
    context.write(new Text ("Distribution of POS\n noun:"), new IntWritable(posCount[0]));
    context.write(new Text ("Plural:"), new IntWritable(posCount[1]));
    context.write(new Text ("Noun Phrase:"), new IntWritable(posCount[2]));
    context.write(new Text ("Verb-Participle:"), new IntWritable(posCount[3]));
    context.write(new Text ("Verb-Transitive:"), new IntWritable(posCount[4]));
    context.write(new Text ("Verb-intransitive:"), new IntWritable(posCount[5]));
    context.write(new Text ("Adjective:"), new IntWritable(posCount[6]));
    context.write(new Text ("Adverb:"), new IntWritable(posCount[7]));
    context.write(new Text ("Conjunction:"), new IntWritable(posCount[8]));
    context.write(new Text ("Proposition:"), new IntWritable(posCount[9]));
    context.write(new Text ("Interjenction:"), new IntWritable(posCount[10]));
    context.write(new Text ("Pronoun:"), new IntWritable(posCount[11]));
    context.write(new Text ("Defenite Article:"), new IntWritable(posCount[12]));
    context.write(new Text ("Indefenite Article:"), new IntWritable(posCount[13]));
    context.write(new Text ("Nominative:"), new IntWritable(posCount[14]));



  }
}


	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
	    conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
	    conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
	    conf.set("mapreduce.framework.name", "yarn");
	    conf.set("job.posfile.path", args[0]);
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(assignment2.class);
	    job.setMapperClass(myMapper.class);
	    //job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
