/**
 * 
 */
package knowbigdata.recommandations;

import java.io.IOException;

import knowbigdata.wordcount.WordCountMain;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @author Kiran
 *
 */
//knowbigdata.recommandations.RecommandationMain
//knowbigdata.recommandations.RecommandationMain

public class RecommandationMain {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		if(args.length != 2 ){
			System.err.println("Invalid arguments: Expected format: RecommandationMain <inputfile path> <outputfile path>");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		          
        Job job = new Job(conf, "Finding best recommandation in file");
        job.setJarByClass(WordCountMain.class);
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        
        Path fileOutputPath = new Path(args[1]); 
        FileOutputFormat.setOutputPath(job, fileOutputPath);
        //delete the path if already exist
        fileOutputPath.getFileSystem(conf).delete(fileOutputPath,true);
        
        job.setMapperClass(RecommandatioonsMapper.class);
        job.setReducerClass(RecommanndationsReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);        
        
            
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
            
       
            
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
