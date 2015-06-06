

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;








import java.util.*;

public class Dynamic {
	
	public static int minSupport;
	public static List<String> one_list=new ArrayList<String>();
	public static List<String[]> dataSet= new ArrayList<String[]>();
	public static Map<String,List<Integer>> map= new HashMap<String,List<Integer>>();
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(),",");
    
      List<String> lineList= new ArrayList<String>();
      while (itr.hasMoreTokens()) {
    	  String str=itr.nextToken();
    	 lineList.add(str);
        word.set(str);
        context.write(word, one);
        
      }
      if(lineList.size()>0){
      String[] line=new String[lineList.size()];
      
      dataSet.add(lineList.toArray(line));}
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
    	  one_list.add(key.toString());
      context.write(key, result);}
    }
    
    
    
  }

  public static void main(String[] args) throws Exception {
    Configuration conf1 = new Configuration();
    Job job = Job.getInstance(conf1, "Dynamic");
    job.setJarByClass(Dynamic.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    Dynamic.minSupport=Integer.parseInt(args[2]);
    Format format= new Format();
    format.format(args[0],args[1]);
    
    FileInputFormat.addInputPath(job, new Path("/user/hduser/newOutput/processedData"));
    FileOutputFormat.setOutputPath(job, new Path("/user/hduser/newOutput/new1"));
    job.waitForCompletion(true);
  int k=0;
   int x=50;
   List<String> k_oneList= Dynamic.one_list;
   int y=0;
    while(x>0){
    	k++;
    	String input="/user/hduser/newOutput/new"+k;
    	y=k+1;
    	String output="/user/hduser/newOutput/new"+y;
    	CandidateGenerator genera= new CandidateGenerator();
  genera.generate(input, output, Dynamic.minSupport, Dynamic.one_list,k_oneList,Dynamic.map);
  Dynamic.map=genera.returnMap();
  k_oneList=genera.returnk_oneList();
   x=genera.returnSize();
   System.out.println("I love India:"+x); 
    }
    
  FileSystem fs= FileSystem.get(conf1);
  ContentSummary cs= fs.getContentSummary(new Path("/user/hduser/oldOutput"));
  int k1=(int) cs.getDirectoryCount();
  if(fs.exists(new Path("/user/hduser/oldOutput/procData"))){
	  k1 = k1-2;
  }
    
    for(int l=1; l<=Math.max(k1,k); l++){
    Merger combiner= new Merger();
    
    
    String input1="/user/hduser/oldOutput/out"+l;
    String input2="/user/hduser/newOutput/new"+l;
    combiner.merge(input1, input2,args[1],l);
    }
   
    //fs.delete(new Path("/user/hduser/output"));
    
    RulesGeneration rules= new RulesGeneration();
    rules.calcu(Double.parseDouble(args[2]), args[4]) ;
    
    fs.delete(new Path("/user/hduser/oldOutput"));
    fs.rename(new Path("/user/hduser/inter"),new Path("/user/hduser/oldOutput"));
    
  }
}