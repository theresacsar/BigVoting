package SchwartzSet;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * ReduceVertex combines all arrays of vertices with mode neu, old and reachby to a new vertex ({@link VertexWritable}). The InputKey is the 
 * index of the corresponding vertex ({@link IntWritable}).
 * 
 * @author ------
 *
 */
public class ReduceVertex extends Reducer<IntWritable, VertexArrayWritable, IntWritable, VertexWritable> {
	
	protected void reduce(IntWritable key, java.lang.Iterable<VertexArrayWritable> arrays, org.apache.hadoop.mapreduce.Reducer<IntWritable, VertexArrayWritable, IntWritable, VertexWritable>.Context context)
	
	throws IOException, InterruptedException {
		//Configuration conf = context.getConfiguration();
		//int N = conf.getInt("TransitiveClosure.N", 1);
		ArrayList<IntWritable> buffer = new ArrayList<IntWritable>();
		ArrayList<IntWritable> bufferNeu = new ArrayList<IntWritable>();
		ArrayList<IntWritable> bufferOld = new ArrayList<IntWritable>();
		ArrayList<IntWritable> bufferReachedBy = new ArrayList<IntWritable>();
		int j; 
		IntWritable tmp, mode;
		
		for (VertexArrayWritable array: arrays){
				buffer.clear();
				mode=new IntWritable(array.mode.get());
				for(IntWritable v : array.vertices){
					buffer.add(v);
				}
				
				switch(mode.get()){
					case 1: for(IntWritable v: buffer){
								if(!bufferNeu.contains(v) && !bufferOld.contains(v)){
									bufferNeu.add(new IntWritable(v.get()));
								}
							}
							break;
					case 2: for(IntWritable v: buffer){
								if(!bufferOld.contains(v)){
									bufferOld.add(new IntWritable(v.get()));
								}
							}
							break;
					case 3: for(IntWritable v: buffer){
								if(!bufferReachedBy.contains(v)){
									bufferReachedBy.add(new IntWritable(v.get()));
								}
							}
							break;
					default: break;
				}
			}
		
		j=0;	
		while(j<bufferNeu.size() && bufferOld.size()>0){
			tmp=bufferNeu.get(j);
			if(bufferOld.contains(tmp)) {
				bufferNeu.remove(j);
			}else{
				j++;
			}
		}
		
	
		if(!bufferNeu.isEmpty()) {
			context.getCounter(SchwartzSet.MyCounter.ACTIVE_VERTICES).increment(1);
		}

		context.write(key, new VertexWritable(
				bufferNeu.toArray(new IntWritable[bufferNeu.size()]),
				bufferOld.toArray(new IntWritable[bufferOld.size()]),
				bufferReachedBy.toArray(new IntWritable[bufferReachedBy.size()])));
				
	}
	}