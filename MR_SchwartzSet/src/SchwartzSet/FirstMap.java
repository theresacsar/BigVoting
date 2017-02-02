package SchwartzSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This is the map method for the first MapReduce round of the computation of the Schwartzset. The strict dominance matrix is read from the input files and 
 * the datastructure needed for the following computation is created. 
 * <br>The input files contain the strict dominance matrix by row (each file contains one or more rows). 
 * <br>The first entry in each line of the inputfile is the row number and
 * is read as InputKey of type {@link Text Text}. 
 * <br>The InputValue (the remaining characters in the line) is the corresponding row of the adjacency matrix of the dominance graph.  
 * <br>
 * <br>
 * The (key,value)-pairs created by this method have keys of type {@link IntWritable} and values of type {@link VertexArrayWritable}.
 * 
 * 
 * @author ------
 *
 */
public class FirstMap extends Mapper<Text, Text, IntWritable, VertexArrayWritable>
	{

		public void map (Text key, Text row, Context context)
			throws IOException, InterruptedException 
		{
			
			StringTokenizer elements = new StringTokenizer(row.toString(),",");
			Configuration conf = context.getConfiguration();
			int N = conf.getInt("SchwartzSet.N", 1);
			int i;
			
			//Convert the key from Text to IntWritable
			IntWritable k = new IntWritable(Integer.parseInt(key.toString())); 

			//Read the row of of the adjacency matrix to the buffer
			ArrayList<IntWritable> buffer = new ArrayList<IntWritable>(N);
			for(int j=0; j<N;j++){
				// if the rows contain a 1
				if(Integer.parseInt(elements.nextToken().toString())==1){
					buffer.add(new IntWritable(j));
				}
			}
			
			IntWritable[] vertices = new IntWritable[buffer.size()];
			IntWritable[] me = new IntWritable[1];
			me[0]=k;
			
			i=0;
			for(IntWritable element : buffer){
				vertices[i]=element;
				
				// tell "element", that it is reached by me
				context.write(element, new VertexArrayWritable(me, new IntWritable(3))); 
				i++;
			}
			
			
			//emit all vertices that can be reached from k as "new" to the corresponding reducer
			context.write(k, new VertexArrayWritable(vertices, new IntWritable(1))); 
		}   
	}