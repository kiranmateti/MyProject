/**
 * 
 */
package knowbigdata.recommandations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Kiran
 *
 */
public class RecommanndationsReducer extends Reducer<Text, Text, Text, Text>{

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		
		//these values are not in sorted order
		if( null == values){
			return;
		}
		
		     
        
        //convert iterator to list
        List<String> sortedWordsList = new ArrayList<String>();       
        Iterator<Text> ite = values.iterator();
        while( ite.hasNext()){
        	sortedWordsList.add(ite.next().toString());
        }
        //sort the list
        Collections.sort(sortedWordsList);
        
        int count = 1;    
        Map<String, Integer> duplicateWordCountMap = new HashMap<String, Integer>(); 
        //logic to find duplicate words count
        String lastValue = null;
        for( String value : sortedWordsList){
        	
        	 if( null != lastValue){
	    		 if( value.trim().equals(lastValue))    {
	 	        	count++;
	 	        }else{
	 	        	duplicateWordCountMap.put(lastValue.trim(), count);
	 	        	count = 1;
	 	        }
	    	 }
	       
	        lastValue = value.trim();        
        }      
       
        if( null != lastValue){
        	duplicateWordCountMap.put(lastValue, count);
        }
	    
        //sort the map by value(duplicate word count)
        Map<String, Integer> sortedValuesMap = new TreeMap<String, Integer>(new SortedMapByValueCompartor(duplicateWordCountMap));
        sortedValuesMap.putAll(duplicateWordCountMap);
        
        //logic to find top 3 recommendations
        int i=1;
        int mapSize = sortedValuesMap.size();
        StringBuffer recommendations = new StringBuffer();
        for( Map.Entry<String, Integer> mapEntry : sortedValuesMap.entrySet()){
        	//top 3 recommendations
        	if( i == mapSize || i == 3){
        		recommendations.append(mapEntry.getKey());
        		context.write(key, new Text(recommendations.toString()));
        		break;
        	}
        	
        	recommendations.append(mapEntry.getKey()).append(",");
        	
			i++;
        }
        
	}
}

class SortedMapByValueCompartor implements Comparator<String>{

	private Map<String, Integer> keyValueMap;
	
	public SortedMapByValueCompartor(Map<String, Integer> keyValueMap){
		this.keyValueMap = keyValueMap;
	}
	@Override
	public int compare(String o1, String o2) {		
		
		//System.out.println(" o1 = "+o1+", o2 = "+o2);
		return keyValueMap.get(o1) < keyValueMap.get(o2) ? 1 : -1;
		
	
	}
	
}