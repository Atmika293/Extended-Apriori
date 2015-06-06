

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Merger {
	
public static int minSupport=0;
    
	public static List<String> coke=new ArrayList<String>();
	public static int x=0;
	
	
	public static class TokenizerMapper
	       extends Mapper<Object, Text, Text,IntWritable>{
	

	    public void map(Object key, Text value, Context context
	                    ) throws IOException, InterruptedException {

	       Text word = new Text();
	       

	        	int index=stringTrimmer(value.toString());
	        	
	        	String newItemSet= value.toString().substring(0,index);
	        	
	        	int  support=Integer.parseInt(value.toString().substring(index+1));
	        	
	        	
	        	IntWritable supp= new IntWritable(support);
	        	
	        	
	        	int position=hasAlready(coke,newItemSet);
	        	if(position==-1){
	        				word.set(newItemSet);
	        		        coke.add(newItemSet);
	        		       
	        									}
	        	else {
	        		word.set(coke.get(position));
	        	
	        	}
	        	
	        	System.out.println(word.toString());
	        	 context.write(word, supp);
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
		
		public static int hasAlready(List<String> list,String str){
	    	int i=0;
	    	for(String string:list){
	    	
	    		Set<String> set= new HashSet<String>();
	    		
	    		StringTokenizer token= new StringTokenizer(string,",");
	    		
	    		while(token.hasMoreTokens()){
	    			set.add(token.nextToken());
	    			
	    		}
	    		Set<String> set1= new HashSet<String>();
	    		StringTokenizer tok= new StringTokenizer(str,",");
	    		int x=0;
	    		while(tok.hasMoreTokens()){
	    			String toke=tok.nextToken();
	    			set1.add(toke);
	    			if(set.contains(toke)){
	    				x++;
	    			}
	    		}
	    		
	    		if(x==set.size() && x==set1.size()){
	    			return i;
	    		}
	    	i++;
	    		
	    	}
	    	return -1;
	    }
	}
		
		public static class IntSumReducer
		  
	       extends Reducer<Text,IntWritable,Text,IntWritable> {
	  
		  private IntWritable result = new IntWritable();
		
	    public void reduce(Text key, Iterable<IntWritable> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	      int sum = 0;
	      for (IntWritable val : values) {
	        sum += val.get();
	      }
	      result.set(sum);
	      if(sum >=minSupport){
	    	 
	      context.write(key, result);}
	    }
		
}

		
		
public void merge(String input1, String input2, String minSupport, int k)throws Exception{ 
	 Configuration conf6 = new Configuration();
	    Job job = Job.getInstance(conf6, "Merger");
	    
	    
	    
	    job.setMapperClass(TokenizerMapper.class);
	    
	    job.setReducerClass(IntSumReducer.class);
	  
	   Merger.minSupport=Integer.parseInt(minSupport); 
	   
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);

	   
Merger.coke.clear();

	    
	    FileSystem fs= FileSystem.get(conf6);
	    
	    
if(fs.exists(new Path(input1)) && fs.exists(new Path(input2))){
	    
FileInputFormat.addInputPath(job, new Path(input1));
FileInputFormat.addInputPath(job, new Path(input2));

}
	    
	    else if(fs.exists(new Path(input1))){
	    	
	    	FileInputFormat.addInputPath(job, new Path(input1));
	    	
	    	
	    }
	    
	    else {
	    	
	    	FileInputFormat.addInputPath(job, new Path(input2));
	    	
	    	
	    }
	    
	    
	    FileOutputFormat.setOutputPath(job, new Path("/user/hduser/inter/out"+k));
	    
	    
job.waitForCompletion(true);

	
	
}


}

	
	

