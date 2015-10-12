/**
 * 
 */
package knowbigdata.wordcount;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * @author Kiran
 *
 */
public class WordCountReducer  extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text word, Iterable<IntWritable> mergedValues,Context context)
			throws IOException , InterruptedException {
		
		if( null == mergedValues ){
			return;
		}
		
		int count =0;
		Iterator<IntWritable> ite = mergedValues.iterator();
		while( ite.hasNext()){
			count = count + ite.next().get();
		}
		
		context.write(word, new IntWritable(count));
		
	}

}
