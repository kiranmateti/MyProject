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
 *Sample:
 *
 * Name,Position Title,Department,Employee Annual Salary
 * "ABBATACOLA,  ROBERT J",ELECTRICAL MECHANIC,AVIATION,$91520.00
 *
 *Goal:  Write a MR program to get the employee details who is getting highest salary
 *
 *Data set: Employee_Names__Salaries__and_Position_Titles.csv
 *
 * Output format: Name,Position Title,Department,Employee Annual Salary
 *   XXX, XXX, XXX, XXX
 *   XXX, XXX, XXX, XXX
 */
public class EmployeeMapReduce {

	//mapper
	public static class EmployeeMapper1 extends Mapper<LongWritable, Text, DoubleWritable, Text>{
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			final String row = value.toString();
			
			if( null == row || row.trim().equals("")){
				System.out.println(" This line is empty");
				return;
			}
			
			String[] columns = row.split(",");		
			
			Double salary = 0.0;
			String salaryStr = columns[columns.length-1];
			try{
				 salary = Double.parseDouble(salaryStr.substring(1));
			}catch(Exception ex){
				return;
			}
			
			
			context.write(new DoubleWritable(salary), value);			
			
		}
		
	}
	
	//reducer
	public static class EmployeeReducer1 extends Reducer<DoubleWritable, Text, DoubleWritable, Text>{
			
		
			public void reduce(DoubleWritable salary, Iterable<Text> details, Context context)
						throws IOException , InterruptedException {
				
				StringBuffer buffer = new StringBuffer();
				for ( Text text : details){
					buffer.append(text.toString()).append("\n");
				}				
				
				context.write(salary, new Text(buffer.toString()));
			}
	}
}
