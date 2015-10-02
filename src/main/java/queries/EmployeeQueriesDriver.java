package queries;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import queries.EmployeeMapReduce.EmployeeMapper1;
import queries.EmployeeMapReduce.EmployeeReducer1;

public class EmployeeQueriesDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		if(args.length != 2 ){
			System.err.println("Invalid arguments: Expected format WordCount <inputfile path> <outputfile path>");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		          
        Job job = new Job(conf, "My word Count");
        job.setJarByClass(EmployeeMapReduce.class);
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));        
        Path fileOutputPath = new Path(args[1]); 
        FileOutputFormat.setOutputPath(job, fileOutputPath);
        //delete the path if already exist
        fileOutputPath.getFileSystem(conf).delete(fileOutputPath,true);
        
        job.setMapperClass(EmployeeMapper1.class);
        job.setReducerClass(EmployeeReducer1.class);
        
        
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
