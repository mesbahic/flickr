import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class StringAndInt implements WritableComparable, Cloneable {

	private Text tagName;
	private IntWritable tagOccurence;

	public StringAndInt() {
		tagName = new Text();
		tagOccurence = new IntWritable();
	}

	public StringAndInt(String tagName, int tagOccurence) {
		this.tagName = new Text(tagName);
		this.tagOccurence = new IntWritable(tagOccurence);
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		if (o == null)
			return 1;
		if (!(o instanceof StringAndInt))
			return 1;
		StringAndInt other = (StringAndInt) o;

		if (other.getTagOccurence().get() > this.tagOccurence.get())
			return -1;
		else if (other.getTagOccurence().get() < this.tagOccurence.get())
			return 1;
		else
			return 0;
	}

	@Override
	public StringAndInt clone() {
		StringAndInt loc = new StringAndInt(tagName.toString(), tagOccurence.get());
		return loc;
	}

	public Text getTagName() {
		return tagName;
	}

	public void setTagName(Text tagName) {
		this.tagName = tagName;
	}

	public IntWritable getTagOccurence() {
		return tagOccurence;
	}

	public void setTagOccurence(IntWritable tagOccurence) {
		this.tagOccurence = tagOccurence;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		tagName.readFields(in);
		tagOccurence.readFields(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		tagName.write(out);
		tagOccurence.write(out);

	}

	@Override
	public String toString() {
		return tagName.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((tagName == null) ? 0 : tagName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		StringAndInt other = (StringAndInt) obj;
		if (tagName == null) {
			if (other.tagName != null)
				return false;
		} else if (!tagName.equals(other.tagName))
			return false;
		return true;
	}

}
