/**
 * 
 */
package queries;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author kiran
 *
 *Sample file data:
 *
 * Name,Position Title,Department,Employee Annual Salary
 * "ABBATACOLA,  ROBERT J",ELECTRICAL MECHANIC,AVIATION,$91520.00
 *
 *Goal:  Write a MR program to list the the employees by department & Title sort order
 *
 *Data set: Employee_Names__Salaries__and_Position_Titles.csv
 *
 * Output format: Department Title Highest Annual Salary
 *   XXX XXX XXX
 *   XXX XXX XXX
 */
public class EmployeeMapReduce3 {

	//mapper
	public static class EmployeeMapper3 extends Mapper<LongWritable, Text, DepartmentAndTitleCompositeKey, DoubleWritable>{
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			final String row = value.toString();
			
			if( null == row || row.trim().equals("")){
				System.out.println(" This line is empty");
				return;
			}
			
			row.replace("\"", "");
			String[] columns = row.split(",");		
			
			Double salary = 0.0;
			String salaryStr = columns[columns.length-1];
			try{
				 salary = Double.parseDouble(salaryStr.substring(1));
			}catch(Exception ex){
				return;
			}
			
			System.out.println(columns[3]  + " " + columns[2]);
			context.write(new DepartmentAndTitleCompositeKey(new Text(columns[3]), new Text(columns[2])), 
					new DoubleWritable(salary));			
			
		}
		
	}
	
	//reducer
	public static class EmployeeReducer3 extends Reducer<DepartmentAndTitleCompositeKey, DoubleWritable, Text, Text>{
			
		
			public void reduce(DepartmentAndTitleCompositeKey deptAndTitle, Iterable<DoubleWritable> salaries, Context context)
						throws IOException , InterruptedException {
				
				double maxSal = 0.0;
				for ( DoubleWritable salary : salaries){
					if( salary.get() >= maxSal ){
						maxSal = salary.get();
					}
				}				
				
				context.write(new Text( deptAndTitle.getDepartment().toString() + ":" + deptAndTitle.getTitle().toString()), 
						new Text("$"+maxSal));
			}
	}
}
