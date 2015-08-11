/**
 * 
 */
package knowbigdata.anagram;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Kiran
 *
 */
public class AnagramReducer extends Reducer<Text, Text, Text, Text>{

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		
		if( null == values){
			return;
		}
		
		Iterator<Text> ite = values.iterator();
		StringBuffer buffer = new StringBuffer();
		Set<String> uniqueWords = new HashSet<String>();
		while( ite.hasNext() ){
			final String word = ite.next().toString();
			//don't show duplicate words
			if( uniqueWords.add(word)){
				buffer.append(word).append(",");
			}
			
		}
		context.write(new Text(""), new Text(buffer.toString()));
	}
}
