package SchwartzSet;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * For a given vertex (v) the {@link VertexWritable} contains arrays of vertices {@link IntArrayWritable} that 
 * can be reached from v (old and neu)
 * and vertices that are known to reach v (reachedBy). 
 * 
 * @author --------
 *
 */
public class VertexWritable implements WritableComparable<Object> {

	/**
	 * Contains the new vertices found to be reached by v. 
	 */
	public IntArrayWritable neu; //"new" cannot be used as a variable name - hope you don't mind some german.
	
	/**
	 * Contains the old vertices, that are already know to be reached by v. 
	 */
	public IntArrayWritable old;
	/**
	 * Contains vertices that can reach v. 
	 */
	public IntArrayWritable reachedBy;
	
	public VertexWritable(int Nnew, int Nold, int NreachedBy) {
		this.neu = new IntArrayWritable(Nnew);
		this.old = new IntArrayWritable(Nold);
		this.reachedBy = new IntArrayWritable(NreachedBy);
	}
	
	public VertexWritable() {
		this.neu = new IntArrayWritable(0);
		this.old = new IntArrayWritable(0);
		this.reachedBy = new IntArrayWritable(0);
		}
	

	public VertexWritable(IntWritable[] neu,IntWritable[] old,IntWritable[] reachedBy) {
		this.neu = new IntArrayWritable(neu);
		this.old = new IntArrayWritable(old);
		this.reachedBy = new IntArrayWritable(reachedBy);
	}


	@Override
	public void write(DataOutput out) throws IOException {
		neu.write(out);
		old.write(out);
		reachedBy.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		neu = new IntArrayWritable();
		old = new IntArrayWritable();
		reachedBy = new IntArrayWritable();
		neu.readFields(in);
		old.readFields(in);
		reachedBy.readFields(in);
	}

	@Override
	public int compareTo(Object o) {
		VertexWritable other = (VertexWritable) o;
		return(this.old.compareTo(other.old));
	}

/*	public void setValues(int[] values) {
		this.values = new IntWritable[values.length];
		for(int i=0; i<values.length; i++){
			this.values[i].set(values[i]);
		}
	}
*/

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}

		VertexWritable other = (VertexWritable) obj;
		
		if(neu.equals(other.neu) == false){
			return false;
		}
		if(old.equals(other.old) == false){
			return false;
		}
		if(reachedBy.equals(other.reachedBy) == false){
			return false;
		}
		
		return true;
	}

	@Override
	public int hashCode() {
		if (reachedBy == null) {
			return 0;
		}else if (neu==null){
			return reachedBy.hashCode();
		}else{
			return reachedBy.hashCode()+13*neu.hashCode();
		}
			
			
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(neu.toString());
		sb.append(",");
		sb.append(old.toString());
		sb.append(",");
		sb.append(reachedBy.toString());
		return sb.toString();
	}
	
}