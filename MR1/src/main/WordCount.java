package main;
 
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.StringTokenizer;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
  
public class WordCount { 
 static String _null = "null";
public static class MapperClass extends Mapper<LongWritable, Text, Text, ValuePair> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
         String[] line = value.toString().split("\t");
         if(line.length<1)
		return;
	String[] gram = line[0].split(" ");
	if(gram.length<3)
		return;
       	String newkey = gram[0]+" "+gram[1]+" "+gram[2];
        word.set(gram[0]);
        context.write(word, new ValuePair(_null,1));
        word.set(gram[1]);
        context.write(word, new ValuePair(newkey,1));
        word.set(gram[2]);
        context.write(word, new ValuePair(newkey,1));
        word.set(gram[0]+" " + gram[1]);
        context.write(word, new ValuePair(newkey,1));
        word.set(gram[1]+" " + gram[2]);
        context.write(word, new ValuePair(newkey,1));
        word.set(newkey);
        context.write(word, new ValuePair(newkey,1));
      
    }
  }
public static class CombinatorClass extends Reducer<Text,ValuePair,Text,ValuePair> {
	private Text word = new Text();
	private IntWritable sum = new IntWritable();
    @Override
    public void reduce(Text key, Iterable<ValuePair> values, Context context) throws IOException,  InterruptedException {
    	 Map<String, Integer> h = new HashMap<String, Integer>();
    	  for (ValuePair value : values) {
    	   if(h.containsKey(value.getKey()))
    		   h.replace(value.getKey(),value.getValue() + h.get(value.getKey())); 
    	  
    	  else h.put(value.getKey(), value.getValue());
    	 }
    	  for (Map.Entry<String, Integer> pair: h.entrySet())
    	  {
    		  context.write(key, new ValuePair(pair.getKey(),pair.getValue()));
    	   }
    	  }
}
  public static class ReducerClass extends Reducer<Text,ValuePair,Text,ValuePair> {
	  private Text word = new Text();
    @Override
    public void reduce(Text key, Iterable<ValuePair> values, Context context) throws IOException,  InterruptedException {
      int sum = 0;
      String[] gram = key.toString().split(" "); 
      ArrayList<String> keys = new ArrayList<String>();
      for (ValuePair value : values) {
        sum += value.getValue();
        if(!keys.contains(value.getKey()))
        	keys.add(value.getKey());
      }
      if(gram.length<3)
      for(String newkey : keys)
      {
    	  if(_null.contentEquals(newkey))
    		  continue;
    	  word.set(newkey);
    	  context.write(word, new ValuePair(key.toString(),sum));
      }
      else 
    	  context.write(key, new ValuePair(key.toString(),sum));
       
    }
  }
 
    public static class PartitionerClass extends Partitioner<Text, ValuePair> {
      @Override
      public int getPartition(Text key, ValuePair value, int numPartitions) {
        return Math.abs(key.hashCode()) % numPartitions;
      }    
    }
 
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "MR1 prob");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(MapperClass.class);
    job.setPartitionerClass(PartitionerClass.class);
    job.setCombinerClass(CombinatorClass.class);
    job.setReducerClass(ReducerClass.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(ValuePair.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(ValuePair.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    job.setInputFormatClass(SequenceFileInputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
