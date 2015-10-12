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
 *Goal:  Write a MR program to find the highest salary for each title in each department
 *
 *Data set: Employee_Names__Salaries__and_Position_Titles.csv
 *
 * Output format: Department Title Highest Annual Salary
 *   XXX XXX XXX
 *   XXX XXX XXX
 */
public class EmployeeMapReduce4 {

	//mapper
	public static class EmployeeMapper4 extends Mapper<LongWritable, Text, Text, DoubleWritable>{
		
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
			context.write(new Text(columns[3] +":"+columns[2]), new DoubleWritable(salary));			
			
		}
		
	}
	
	//reducer
	public static class EmployeeReducer4 extends Reducer<Text, DoubleWritable, Text, Text>{
			
		
			public void reduce(Text deptAndTitle, Iterable<DoubleWritable> salaries, Context context)
						throws IOException , InterruptedException {
				
				double maxSal = 0.0;
				for ( DoubleWritable salary : salaries){
					if( salary.get() >= maxSal ){
						maxSal = salary.get();
					}
				}				
				
				context.write(deptAndTitle, new Text("$"+maxSal));
			}
	}
}
