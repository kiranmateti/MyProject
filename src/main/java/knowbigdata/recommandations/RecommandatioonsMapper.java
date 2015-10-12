/**
 * 
 */
package knowbigdata.recommandations;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Kiran
 *
 */
public class RecommandatioonsMapper extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String[] words = value.toString().split(" ");
		
		if( null == words){
			return;
		}
		
		for( int i=0; i < words.length; i++){
			 if( i == words.length-1){
				 context.write(new Text(words[i].trim()),new Text(""));
			 }else{
				 context.write(new Text(words[i].trim()),new Text(words[i+1].trim()));
			 }
		}
		
				
	}
}
