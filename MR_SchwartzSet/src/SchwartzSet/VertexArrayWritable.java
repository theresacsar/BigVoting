package SchwartzSet;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Contains an array of {@link IntWritable IntWritables} together with a mode ((1)new, (2)old or (3)reachedBy).
 * @author -----
 *
 */
public class VertexArrayWritable implements WritableComparable<Object> {

	public IntWritable[] vertices;
	public IntWritable mode; //(1)new, (2)old, (3)reachedBy

	public VertexArrayWritable(int N) {
		vertices = new IntWritable[N];
		mode = new IntWritable(1);
	}

	public VertexArrayWritable() {
		vertices = new IntWritable[0];
		mode = new IntWritable(1);
	}
	
	public VertexArrayWritable(IntWritable[] vertices, IntWritable mode) {
		this.vertices = new IntWritable[vertices.length];
		for(int i=0;i<vertices.length;i++){
			this.vertices[i]=vertices[i];
		}
		this.mode=mode;
	}
	

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(mode.get());
		out.writeInt(vertices.length);
		for(int i=0; i<vertices.length; i++){
			out.writeInt(vertices[i].get());
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		mode = new IntWritable(in.readInt());
		vertices = new IntWritable[in.readInt()];
		for (int i=0; i<vertices.length; i++){
			vertices[i] = new IntWritable(in.readInt());
		}
	}

	@Override
	public int compareTo(Object o) {
		VertexArrayWritable other = (VertexArrayWritable) o;
		return(this.vertices[0].compareTo(other.vertices[0]));
	}

	public void setValues(int[] vertices, int mode) {
		this.vertices = new IntWritable[vertices.length];
		for(int i=0; i<vertices.length; i++){
			this.vertices[i].set(vertices[i]);
		}
		this.mode = new IntWritable(mode);
	}


	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}

		VertexArrayWritable other = (VertexArrayWritable) obj;
		
		if(vertices.length != other.vertices.length){
			return false;
		}
		
		for (int i=0; i<vertices.length; i++){
			if(vertices[i].get()!=other.vertices[i].get()){
				return false;
			}
		}
		return true;
	}

	@Override
	public int hashCode() {
		if (vertices == null) {
			return 0;
		}
		return vertices[0].hashCode()+mode.hashCode();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(String.valueOf(mode.get())+",");
		sb.append(String.valueOf(vertices.length));
		if(vertices.length>0) sb.append(",");
		for (int i = 0; i < vertices.length; i++) {
			sb.append(vertices[i].toString());
			if (i != vertices.length - 1) {
				sb.append(",");
			}
		}
		return sb.toString();
	}
	
}