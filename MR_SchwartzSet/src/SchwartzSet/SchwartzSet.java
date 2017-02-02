package SchwartzSet;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * The MapReduce program for computing the SchwartzSet. <br>
 * 
 * 
 * @author -----
 *
 */
public class SchwartzSet {

	/**
	 * The number of candidates/vertices.
	 */
	public static int N;

	/**
	 * Mapreduce counter used to count the number of active vertices in the reduce phase. 
	 *
	 */
	protected static enum MyCounter {
	    ACTIVE_VERTICES;
	};
	
	/**
	 * MapReduce program to compute the SchwartzSet. The algorithm implemented in this package is proposed
	 * in the AAAI-17 paper "Winner Determination in Huge Elections with MapReduce".
	 * 
	 * @param args inputpath outputpath {@link #N}.
	 */
	public static void main(String[] args) throws Exception {

		  int N = Integer.parseInt(args[2]);
		  int count=1;
		  int max;
		  long active=10;
		  Counters c;
		  Counter cnt;
		  
		  max = (int) java.lang.Math.floor(java.lang.Math.log(N)/java.lang.Math.log(2));
		  
		   Configuration conf = new Configuration();
		   conf.setInt("SchwartzSet.N", N);
		   
		   
		   //first round - map reads from input stream (matrix form)
		   Job job = Job.getInstance(conf, "square1");
		   job.setJarByClass(SchwartzSet.class);
		   job.setOutputKeyClass(IntWritable.class);
		   job.setOutputValueClass(VertexWritable.class);
		   job.setMapperClass(FirstMap.class);
		   job.setMapOutputKeyClass(IntWritable.class);
		   job.setMapOutputValueClass(VertexArrayWritable.class);
		   job.setReducerClass(ReduceVertex.class);
		   job.setInputFormatClass(KeyValueTextInputFormat.class);
		   job.setOutputFormatClass(SequenceFileOutputFormat.class);
		   FileInputFormat.addInputPath(job, new Path(args[0]));
		   FileOutputFormat.setOutputPath(job, new Path(args[1]+count));
		   job.waitForCompletion(true);
		   
		   //c = job.getCounters();
		   //cnt = c.findCounter(MyCounter.ACTIVE_VERTICES);
		   active=N;
		  
		   

		   if(max>N) max=N/2;
		   
		   while(count<=max && active>0){
			   count++;
			   
			   conf = new Configuration();
			   conf.setInt("SchwartzSet.N", N);
			   
			   job = Job.getInstance(conf, "square"+count);
			   job.setJarByClass(SchwartzSet.class);
			   job.setOutputKeyClass(IntWritable.class);
			   job.setOutputValueClass(VertexWritable.class);
			   job.setMapperClass(MapVertex.class);
			   job.setMapOutputKeyClass(IntWritable.class);
			   job.setMapOutputValueClass(VertexArrayWritable.class);
			   job.setReducerClass(ReduceVertex.class);
			   job.setInputFormatClass(SequenceFileInputFormat.class);
			   job.setOutputFormatClass(SequenceFileOutputFormat.class);
			   FileInputFormat.addInputPath(job, new Path(args[1]+(count-1)));
			   FileOutputFormat.setOutputPath(job, new Path(args[1]+count));
			   job.waitForCompletion(true);
			   
			   c = job.getCounters();
			   cnt = c.findCounter(MyCounter.ACTIVE_VERTICES);
			   active=cnt.getValue(); 
		   }
		   
		   //Last Round to generate Output
		   
		   conf = new Configuration();
		   conf.setInt("SchwartzSet.N", N);
		   job = Job.getInstance(conf, "square"+count);
		   job.setJarByClass(SchwartzSet.class);
		   job.setOutputKeyClass(NullWritable.class);
		   job.setOutputValueClass(IntArrayWritable.class);
		   job.setMapperClass(LastMap.class);
		   job.setMapOutputKeyClass(NullWritable.class);
		   job.setMapOutputValueClass(IntWritable.class);		   
		   job.setReducerClass(LastReduce.class);
		   job.setInputFormatClass(SequenceFileInputFormat.class);
		   job.setOutputFormatClass(TextOutputFormat.class);
		   FileInputFormat.addInputPath(job, new Path(args[1]+count));
		   FileOutputFormat.setOutputPath(job, new Path(args[1]+"byrow"));
		   job.waitForCompletion(true);
		   
	}

}
