package SchwartzSet;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Datatype containing an array of {@link IntWritable IntWritables}.
 * 
 * @author -----
 *
 */
public class IntArrayWritable implements WritableComparable<Object> {

	/**
	 * Array of {@link IntWritable IntWritables}.
	 */
	public IntWritable[] values;

	/**
	 * Constructs an {@link IntArrayWritable} with an array of length N.
	 * @param N number of {@link IntWritable IntWritables} in the object 
	 */
	public IntArrayWritable(int N) {
		values = new IntWritable[N];
	}

	/**
	 * Constructs an {@link IntArrayWritable} with an array of length 0.
	 */
	public IntArrayWritable() {
		values = new IntWritable[0];
	}
	
	/**
	 * Constructs an {@link IntArrayWritable} containing the input array values.
	 * @param values array of {@link IntWritable IntWritables}
	 */
	public IntArrayWritable(IntWritable[] values) {
		this.values = values;
	}


	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(values.length);
		for(int i=0; i<values.length; i++){
			values[i].write(out);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		values = new IntWritable[in.readInt()];
		for (int i=0; i<values.length; i++){
			IntWritable value = new IntWritable();
			value.readFields(in);
			values[i]=value;
		}
	}

	@Override
	public int compareTo(Object o) {
		IntArrayWritable other = (IntArrayWritable) o;
		return(this.values[1].compareTo(other.values[1]));
	}

	public void setValues(int[] values) {
		this.values = new IntWritable[values.length];
		for(int i=0; i<values.length; i++){
			this.values[i].set(values[i]);
		}
	}


	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}

		IntArrayWritable other = (IntArrayWritable) obj;
		
		if(values.length != other.values.length){
			return false;
		}
		
		for (int i=0; i<values.length; i++){
			if(values[i].get()!=other.values[i].get()){
				return false;
			}
		}
		return true;
	}

	@Override
	public int hashCode() {
		if (values == null) {
			return 0;
		}
		if(values.length==0){
			return 0;
		}
		
		return values[0].hashCode()+values.length*13;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(String.valueOf(values.length));
		if(values.length>0) {
			sb.append(",");
			for (int i = 0; i < values.length; i++) {
				sb.append(values[i].toString());
				if (i != values.length - 1) {
					sb.append(",");
				}
			}
		}
		
		return sb.toString();
	}
	
}