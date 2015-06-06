
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class CandidateGenerator {
	public static int x;
	public static int minSupport;
	public static List<String> k_oneList;
	
	public static List<String> items;
	public static List<String> unique=new ArrayList<String>();
	public static Map<String,List<Integer>> map;
	
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      
    	String trim=stringTrimmer(value.toString());
    	
    	for(String each:items){ 
    		String newItemSet=trim+","+each;
    		
    		if(isInK_oneList(newItemSet)){
    			
    			if(!hasAlready(unique,newItemSet)){
    		
    			if(!trim.contains(each)){
    				int support=	countNumber(trim, each);
    	    		
    	    	
    	if(support>=minSupport){
    				IntWritable supp= new IntWritable(support);
    		        word.set(newItemSet);
    		        unique.add(newItemSet);
    		        context.write(word, supp);
    	    		x++;
    			
    			} 
    		}
    			}
    		}
      
    	}
    }
    
    public Boolean isInK_oneList(String string){
    	List<String> subsets= generateSubSets(string);
    	for(String each:subsets){
    		if(!hasAlready(k_oneList,each)){
    			return false;
    			}}    	
    	return true;
    	
    }
    
    
    public List<String> generateSubSets(String item){
		String subset;
		List<String> subsets = new ArrayList<String>();
		
		int i = 0;
		String[] items = item.split(",");
		while(i < items.length){
			subset = "";
			for(int j = 0;j < items.length;j++){
				if(j != i)
					subset += items[j] + ",";
			}
			subsets.add(subset.substring(0, subset.length()-1));
			i++;
		}
		return subsets;	
	}
    
    public String stringTrimmer(String string){
    	int x=string.length()-1;
    	for(int i=string.length()-1;i>=0;i--){
    	 if(string.charAt(i)=='\t'){
    		 x=i; break;
    	 }	
    	 }
    	return string.substring(0, x);
    	
    	
    }
    public Boolean hasAlready(List<String> list,String str){
    	
    	for(String string:list){
    		Set<String> set= new HashSet<String>();
    		
    		StringTokenizer token= new StringTokenizer(string,",");
    		while(token.hasMoreTokens()){
    			set.add(token.nextToken());
    			
    		}
    		StringTokenizer tok= new StringTokenizer(str,",");
    		int x=0;
    		while(tok.hasMoreTokens()){
    			
    			if(set.contains(tok.nextToken())){
    				x++;
    			}
    		}
    		
    		if(x==set.size()){
    			return true;
    		}
    		
    	}
    	return false;
    }
    
    public int countNumber(String ItemSet, String each){
    	List<Integer> ln1 = map.get(ItemSet);
        List<Integer> ln2 = map.get(each);
	List<Integer> ln = new ArrayList<Integer>();
	for(Integer num : ln1){
		if(ln2.contains(num)){
				ln.add(num);
			}
		}
    	map.put(ItemSet+","+each,ln);
    	return ln.size();
    }
    
  
  }
  public Map<String, List<Integer>> returnMap(){
	  return map;}

  
  

  public void generate(String input, String output,int minsupport, List<String> items,List<String> k_oneList,Map<String,List<Integer>> map) throws Exception {
    Configuration conf2 = new Configuration();
    Job job = Job.getInstance(conf2, "candidate generator");
    
    job.setMapperClass(TokenizerMapper.class);
    job.setNumReduceTasks(0);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    CandidateGenerator.x=0;
    CandidateGenerator.k_oneList=k_oneList;
    CandidateGenerator.unique=new ArrayList<String>();
    CandidateGenerator.minSupport=minsupport;
    
    CandidateGenerator.items=items;
   CandidateGenerator.map=map;
    
    FileInputFormat.addInputPath(job, new Path(input));
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