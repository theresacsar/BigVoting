package SchwartzSet;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This last reduce method collects all vertices contained in the Schwartzset.
 * 
 * @author --------
 *
 */
public class LastReduce extends Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {
	
	protected void reduce(NullWritable key, java.lang.Iterable<IntWritable> vertices, org.apache.hadoop.mapreduce.Reducer<NullWritable, IntWritable, NullWritable, IntWritable>.Context context)
	
	throws IOException, InterruptedException {

		for (IntWritable v: vertices){
				context.write(NullWritable.get(), v);
			}
		}
	}
