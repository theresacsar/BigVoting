package SchwartzSet;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Last Map Method of the Schwartzset Computation. This method emits the vertex as outputvalue ({@link IntWritable})
 * if it is contained in the Schwartzset.
 * 
 * @author -----
 *
 */
public class LastMap extends Mapper<IntWritable, VertexWritable, NullWritable, IntWritable>
{
	public void map (IntWritable key, VertexWritable vertex, Context context)
		throws IOException, InterruptedException 
	{
		
		
		IntWritable k = new IntWritable(Integer.parseInt(key.toString()));
		
		//IntWritable tmp;
		Boolean Schwartzset=true, found=false;
	

		if(vertex.reachedBy.values.length<=vertex.old.values.length+vertex.neu.values.length){

			for(IntWritable i : vertex.reachedBy.values){
				found=false;
				if(i.equals(k)) {
					found=true;
				}else{
					for(int j=0; j<vertex.neu.values.length; j++){
						if(vertex.neu.values.equals(i)){
							found = true;
							break;
						}
					}
				}
				if(!found){
					for(int j=0; j<vertex.old.values.length; j++){
						if(vertex.old.values.equals(i)){
							found = true;
							break;
						}
					}
				}
				if(!found) {
					Schwartzset=false;
					break;
				}
			}
		}else{
			Schwartzset=false;
		}
		
		if(Schwartzset){
			context.write(NullWritable.get(), k);
		}
	}   
}