package SchwartzSet;
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Reads each vertex and emits values to the reducer. The Inputkey is the index of the vertex (i)
 * of type {@link IntWritable} and the InputValue contains all vertices that are know to be reached by i (old and neu) or can 
 * reach i (reachedBy) as {@link VertexWritable}. See {@link VertexWritable} for more information on the structure of the inputvalue.
 * <br><br>
 * The OutputKey is always an index of a vertex ({@link IntWritable} and the OutputValue of type {@link VertexArrayWritable} 
 * contains an array of vertices together with a mode (neu(1), old(2) or reachedby(3)). 
 * 
 *  
 * @author ------
 *
 */
public class MapVertex extends Mapper<IntWritable, VertexWritable, IntWritable, VertexArrayWritable>
{
	public void map (IntWritable key, VertexWritable vertex, Context context)
		throws IOException, InterruptedException 
	{
		
		IntWritable[] k = new IntWritable[1];
		k[0] = key;
		
		if(vertex.neu.values.length>0) context.write(k[0], new VertexArrayWritable(vertex.neu.values,new IntWritable(2)));
		if(vertex.old.values.length>0) context.write(k[0], new VertexArrayWritable(vertex.old.values,new IntWritable(2)));
		if(vertex.reachedBy.values.length>0) context.write(k[0], new VertexArrayWritable(vertex.reachedBy.values,new IntWritable(3)));
 
		if(vertex.neu.values.length>0){
			for(IntWritable i : vertex.reachedBy.values){
				context.write(i, new VertexArrayWritable(vertex.neu.values,new IntWritable(1)));
			}
			
			for(IntWritable i : vertex.neu.values){
				context.write(i, new VertexArrayWritable(k,new IntWritable(3)));
				context.write(i, new VertexArrayWritable(vertex.reachedBy.values,new IntWritable(3)));
			}
		}
	}   
}