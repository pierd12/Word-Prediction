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
  
public class Join { 
	   public enum C0{
	        Count
	    };
 
public static class MapperClass extends Mapper<LongWritable, Text, Text, ValuePair> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
         String[] gram = value.toString().split("\t");
         if(gram.length<3) {
         word.set("error");
         context.write(word,new ValuePair("error" ,0));
    }
         else {
        	word.set(gram[0]);
        	context.write(word,new ValuePair(gram[1] ,Integer.parseInt(gram[2])));
        	if(gram[1].split(" ").length == 3)
        	{
        	context.getCounter(C0.Count).increment(3*Integer.parseInt(gram[2]));
        	}
         }
  }
}
  public static class ReducerClass extends Reducer<Text,ValuePair,Text,DoubleWritable> {
	  private Text word = new Text();
	  private double c0 =0;
	  public void setup(Context context) throws IOException, InterruptedException{
	        Configuration conf = context.getConfiguration();
	        Cluster cluster = new Cluster(conf);
	        Job currentJob = cluster.getJob(context.getJobID());
	        c0 = currentJob.getCounters().findCounter(C0.Count).getValue();  
	    }
    @Override
    public void reduce(Text key, Iterable<ValuePair> values, Context context) throws IOException,  InterruptedException {
      double c1=0,c2=0,n1=0,n2 = 0,n3 = 0,k2,k3,p = 0;
      String [] ngram = key.toString().split(" ");
      
      for (ValuePair value : values) {
    	  if(value.getKey().contentEquals(ngram[2]))
    		  n1 = value.getValue();
    	  if(value.getKey().contentEquals(ngram[1]))
    		  c1 = value.getValue();
    	  if(value.getKey().contentEquals(ngram[1]+" "+ngram[2]))
    		  n2 = value.getValue();
    	  if(value.getKey().contentEquals(ngram[0]+" "+ngram[1]))
    		  c2 = value.getValue();
    	  if(value.getKey().contentEquals(key.toString()))
    		  n3 = value.getValue();
      }
      k2 =(Math.log(n2+1)+1) /(Math.log(n2+1)+2);
      k3 =(Math.log(n3+1)+1) /(Math.log(n3+1)+2);
      if(c0==0)
    	     p = 0;
      else 
      p = (k3*n3/c2) +((1 -k3)*k2*n2/c1) + ((1-k3)*(1-k2)*(n1/c0));
       context.write(key,  new DoubleWritable (p));
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
    Job job = new Job(conf, "MR2 prob");
    job.setJarByClass(Join.class);
    job.setMapperClass(MapperClass.class);
    job.setPartitionerClass(PartitionerClass.class);
    //job.setCombinerClass(ReducerClass.class);
    job.setReducerClass(ReducerClass.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(ValuePair.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
   // job.setInputFormatClass(SequenceFileInputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
