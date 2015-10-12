/**
 * 
 */
package knowbigdata.anagram;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Kiran
 *
 */
public class AnagramMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		//my cat name is dog
		if( null == value){
			return;
		}
		
		final String[] words = value.toString().split(" ");
		if( null == words){
			return;
		}
		for( int i=0; i < words.length; i++){
			context.write(new Text(this.getSortedKey(words[i].trim())), new Text(words[i].trim()));
		}
	}
	
   private String getSortedKey(final String word){
		
		Set<Character> sortedChars = new TreeSet<Character>();
        for( int i=0; i < word.length(); i++){
        	sortedChars.add(Character.toUpperCase(word.charAt(i)));
        }        
        return sortedChars.toString();
		
	}

}
