/**
 * 
 */
package queries;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import queries.EmployeeMapReduce4.EmployeeMapper4;
import queries.EmployeeMapReduce4.EmployeeReducer4;

/**
 * @author kiran
 *
 */
public class EmployeeQueriesToolRunner extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
	  int result = ToolRunner.run(new EmployeeQueriesToolRunner(), args);
	  System.exit(result);

	}
	
	public int run(String[] args) throws Exception{
		if(args.length != 2 ){
			System.err.println("Invalid arguments: Expected format EmployeeQueriesToolRunner <inputfile path> <outputfile path>");
			return -1;
		}
		
		Job job = Job.getInstance(this.getConf(), "Employee Queries");
		job.setJarByClass(EmployeeMapReduce.class);
		
	    FileInputFormat.setInputPaths(job, new Path(args[0]));        
	    Path fileOutputPath = new Path(args[1]); 
	    FileOutputFormat.setOutputPath(job, fileOutputPath);
	    //delete the path if already exist
	    fileOutputPath.getFileSystem(this.getConf()).delete(fileOutputPath,true);
	        
		job.setMapperClass(EmployeeMapper4.class);
	    job.setReducerClass(EmployeeReducer4.class);
		
	    job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
		
		
		
	}
	

}
