

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;






public class RulesGeneration {
	public static double minConfidence;
	public static Map<String,Integer> map= new HashMap<String, Integer>();
	

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      
    	int index=stringTrimmer(value.toString());
    	String newItemSet= value.toString().substring(0,index);
    	int  support=Integer.parseInt(value.toString().substring(index+1));
    	
    				map.put(newItemSet, support);
    				IntWritable supp= new IntWritable(support);
    		        word.set(newItemSet);
    		        
    		        context.write(word, supp);
      
    	}
    }
    
  
    
    public static int stringTrimmer(String string){
    	
    	int x=string.length()-1;
    	for(int i=string.length()-1;i>=0;i--){
    	 if(string.charAt(i)=='\t'){
    		 x=i; break;
    	 }	
    	 }
    	return x;
    	
    	
    }
    
    public static class IntSumReducer
    
    extends Reducer<Text,IntWritable,Text,DoubleWritable> {

	 
	
 public void reduce(Text key, Iterable<IntWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
   
List<String> generatedRules= generateRules(key.toString());
 
 for(String eachRule:generatedRules){
	 
	 StringTokenizer tokens= new StringTokenizer(eachRule,"}--->{");
	 String precedent= tokens.nextToken();
	 String consequent= tokens.nextToken();
	 
	 double confidence=calculateConfidence(precedent, consequent);
	 
	 if(confidence>=minConfidence){
	Text Rule= new Text();
	 DoubleWritable result = new DoubleWritable(confidence);
	Rule.set("{"+eachRule.replaceAll(" ","")+"}");
		 context.write(Rule, result);
	 }
 }


 
 }
}
    
    
    private static double calculateConfidence(String precedent,String consequent){
		int wholeSupport = 0;
		int precedentSupport = 1;
		for(String key : map.keySet()){
			if(match(precedent,key)){
				precedentSupport = map.get(key);
				
			}
			if(match(precedent+","+consequent,key)){
				wholeSupport =map.get(key);
				
			}
		}
		return ((double)wholeSupport/(double)precedentSupport);
	}
	
	private static boolean match(String key,String itemSet){
		String[] sets=key.split(",");
		String[] items = itemSet.split(",");
		if(sets.length!=items.length){return false;}
		else{
		for(String item : items){
			if(!key.contains(item))
				return false;
		}
		return true;}
	
	}
	
    
	public static List<String> generateRules( String itemSet){
		int k;
		
		List<String>  values;
		String precedent,consequent;
		List<String> allRules = new ArrayList<String>();
		allRules.clear();
		String[] items;
		
			items = itemSet.split(",");
			k = items.length;
			
			values = generateBinaryStrings(k);
			
			for(String binValues : values){
				
				//System.out.println(binValues);
				precedent = "";
				consequent = "";
				
				for(int i = 0;i < binValues.length();i++){
					if(binValues.charAt(i) == '1')
						precedent += items[i]+",";
					if(binValues.charAt(i) == '0')
						consequent += items[i] + ",";
				}
				
				
				precedent = precedent.substring(0, precedent.length()-1);
				consequent = consequent.substring(0, consequent.length()-1);
				
				
					allRules.add(precedent+"}--->{"+consequent);
			
		}
		return allRules;
	}
	
	public static List<String> generateBinaryStrings(int value){
		String prefix = "";
		
		for(int k = 0;k < value;k++)
			prefix += "0";
		String bin;
		List<String> binStrings = new ArrayList<String>();
		binStrings.clear();
		int max = (int)Math.pow(2, value);
		for(int i = 0;i < max;i++){
			bin = Integer.toBinaryString(i);
			bin = prefix.substring(0, value - bin.length()) + bin;
			if(countCharacter(bin,'1') >= 1 && countCharacter(bin,'1') < bin.length())
				binStrings.add(bin);
			}
		return binStrings;
	}

	private static int countCharacter(String str,char c){
		int num = 0;
		for(int i = 0;i < str.length();i++){
			if(str.charAt(i) == c)
				num++;
		}
		return num;
	}
	
  

  public void calcu(double minConfidence, String output) throws Exception {
    Configuration conf3 = new Configuration();
    Job job = Job.getInstance(conf3, "RuleGenerator");
    
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    
    RulesGeneration.minConfidence=minConfidence;
    job.setOutputKeyClass(Text.class);
    
    job.setOutputValueClass(IntWritable.class);
    
    FileSystem fs= FileSystem.get(conf3);
    ContentSummary cs= fs.getContentSummary(new Path("/user/hduser/inter"));
    int k=(int) cs.getDirectoryCount();
   
  for(int i=1; i<k;i++){  
	  String input="/user/hduser/inter/out"+i;
    FileInputFormat.addInputPath(job, new Path(input));}
  
  
  
    FileOutputFormat.setOutputPath(job, new Path(output));
    
    
    job.waitForCompletion(true);
  }
  
  public int returnSize(){
	  
	  return CandidateGenerator.x;
  }
  
  public  List<String> returnk_oneList(){
	  
	  return CandidateGenerator.unique;
  }
}