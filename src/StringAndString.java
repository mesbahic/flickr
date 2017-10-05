import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class StringAndString implements WritableComparable {

	private Text pays;
	private Text tag;

	public StringAndString() {
		// TODO Auto-generated constructor stub
		pays = new Text();
		tag = new Text();
	}

	public StringAndString(String pays, String tag) {
		// TODO Auto-generated constructor stub
		this.pays = new Text(pays);
		this.tag = new Text(tag);
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		pays.readFields(arg0);
		tag.readFields(arg0);

	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		pays.write(arg0);
		tag.write(arg0);

	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((pays == null) ? 0 : pays.hashCode());
		result = prime * result + ((tag == null) ? 0 : tag.hashCode());
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
		StringAndString other = (StringAndString) obj;
		if (pays == null) {
			if (other.pays != null)
				return false;
		} else if (!pays.equals(other.pays))
			return false;
		if (tag == null) {
			if (other.tag != null)
				return false;
		} else if (!tag.equals(other.tag))
			return false;
		return true;
	}

	public Text getPays() {
		return pays;
	}

	public void setPays(Text pays) {
		this.pays = pays;
	}

	public Text getTag() {
		return tag;
	}

	public void setTag(Text tag) {
		this.tag = tag;
	}

	@Override
	public int compareTo(Object o) {
		StringAndString other = (StringAndString) o;
		int compare_pays = this.pays.compareTo(other.pays);
		return (compare_pays == 0) ? this.tag.compareTo(other.tag) : compare_pays;

	}

	@Override
	public String toString() {
		return pays + "," + tag;
	}

}
