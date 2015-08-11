/**
 * 
 */
package knowbigdata.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * @author Kiran
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
	
		String line = value.toString();
		String[] words = line.split(" ");
		
		if( null == words || words.length == 0 ){
			System.out.println(" Line is empty");
			return;
		}
		
		for( int i=0 ; i < words.length; i++){
			if( words[i].trim().equals("")){
				continue;
			}
			context.write(new Text(words[i]), new IntWritable(1));
		}
		
		
	}

}
