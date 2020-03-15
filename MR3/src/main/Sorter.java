package main;
 
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.StringTokenizer;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
  
public class Sorter { 
public static class MapperClass extends Mapper<LongWritable, Text, ValuePair,Text > {
       private Text word = new Text();
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
         String[] input = value.toString().split("\t");
         if(input.length < 2)
        	 return;
        	 
           double prob = Double.parseDouble(input[1]);
            String gram[] = input[0].split(" ");
            String newkey = gram[0]+" "+gram[1];
            word.set(gram[2]);
           context.write(new ValuePair(newkey, prob), word);         
  }
}
  public static class ReducerClass extends Reducer<ValuePair,Text,Text,DoubleWritable> {
	  private Text word = new Text();
	  
	
    @Override
    public void reduce(ValuePair key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
       String  pref= key.getKey();
       DoubleWritable prob = new DoubleWritable(key.getValue());
       for (Text suf : values)
       {
    	   word.set(pref+" "+suf.toString());
    	   context.write(word, prob);
       }
       }
  }
 
    public static class PartitionerClass extends Partitioner<ValuePair,Text> {
      @Override
      public int getPartition(ValuePair key, Text value, int numPartitions) {
        return Math.abs(key.getKey().hashCode()) % numPartitions;
      }    
    }
 
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    @SuppressWarnings("deprecation")
	Job job = new Job(conf, "MR3 prob");
    job.setJarByClass(Sorter.class);
    job.setMapperClass(MapperClass.class);
    job.setPartitionerClass(PartitionerClass.class);
    //job.setCombinerClass(ReducerClass.class);
    job.setReducerClass(ReducerClass.class);
    job.setMapOutputKeyClass(ValuePair.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
   // job.setInputFormatClass(SequenceFileInputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
