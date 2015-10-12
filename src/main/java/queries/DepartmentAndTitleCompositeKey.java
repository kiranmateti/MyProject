/**
 * 
 */
package queries;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author kiran
 *
 */
public class DepartmentAndTitleCompositeKey implements WritableComparable<DepartmentAndTitleCompositeKey>{
 
	private Text department = new Text();	
	
	private Text title = new Text();
	
	public DepartmentAndTitleCompositeKey(){}
	
	public DepartmentAndTitleCompositeKey(final Text dept, final Text title){
		this.department = dept;
		this.title = title;
	}
	
	public Text getDepartment() {
		return department;
	}

	public void setDepartment(Text department) {
		this.department = department;
	}

	public Text getTitle() {
		return title;
	}

	public void setTitle(Text title) {
		this.title = title;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		if( null != department){
			System.out.println(" department = "+department.toString());
		}else{
			System.out.println(" department is null ");
		}
		department.readFields(in);
		title.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		department.write(out);
		title.write(out);
		
	}

	@Override
	public int compareTo(DepartmentAndTitleCompositeKey key2) {
		
		int result = this.getDepartment().compareTo(key2.getDepartment());
		if( result == 0 ){
			return this.getTitle().compareTo(key2.getTitle());	
		}
		return result;			
		
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((department == null) ? 0 : department.toString().hashCode());
		result = prime * result + ((title == null) ? 0 : title.toString().hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof DepartmentAndTitleCompositeKey)) {
			return false;
		}
		DepartmentAndTitleCompositeKey other = (DepartmentAndTitleCompositeKey) obj;
		if (department == null) {
			if (other.department != null) {
				return false;
			}
		} else if (!department.toString().equals(other.department.toString())) {
			return false;
		}
		if (title == null) {
			if (other.title != null) {
				return false;
			}
		} else if (!title.toString().equals(other.title.toString())) {
			return false;
		}
		return true;
	}
	
	public String toString(){
		return department.toString() +":"+ title.toString();
	}
	
	
}
