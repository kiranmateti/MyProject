/**
 * 
 */
package queries;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author kiran
 *
 */
public class ComparatorsAndPartitioners { 

	public static class DepartmentAndTitleSortingComparator extends WritableComparator{
		
		protected DepartmentAndTitleSortingComparator(){
			super(DepartmentAndTitleCompositeKey.class,true);
		}
		
		@SuppressWarnings("rawtypes")
	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
			DepartmentAndTitleCompositeKey k1 = (DepartmentAndTitleCompositeKey)w1;
			DepartmentAndTitleCompositeKey k2 = (DepartmentAndTitleCompositeKey)w2;
	         
	        int result = k1.getDepartment().compareTo(k2.getDepartment());
	        if(0 == result) {
	        	//ascending order
	            result = k1.getTitle().compareTo(k2.getTitle());
	            //descending order
	            //result = -1* k1.getTitle().compareTo(k2.getTitle());
	        }
	        return result;
	    }
	}
	
	//grouping comparator
    public static class DepartmentAndTitleGroupingComparator extends WritableComparator{
		
		protected DepartmentAndTitleGroupingComparator(){
			super(DepartmentAndTitleCompositeKey.class,true);
		}
		
		@SuppressWarnings("rawtypes")
	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
			DepartmentAndTitleCompositeKey k1 = (DepartmentAndTitleCompositeKey)w1;
			DepartmentAndTitleCompositeKey k2 = (DepartmentAndTitleCompositeKey)w2;
	         
	        return k1.getDepartment().compareTo(k2.getDepartment());
	       	        
	    }
	}
    
    public static class DepartmentPatitioner extends Partitioner<DepartmentAndTitleCompositeKey, DoubleWritable> {
    	 
        @Override
        public int getPartition(DepartmentAndTitleCompositeKey key, DoubleWritable val, int numPartitions) {
            int hash = key.getDepartment().hashCode();
            int partition = hash % numPartitions;
            return partition;
        }
     
    }
	
}
