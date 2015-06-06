import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class Format{

	/**
	 * @param args
	 */

	public static int lineNum=1;
	public static Map<Integer, String[]> map= new HashMap<Integer,String[]>();
		public static class TokenizerMapper
	       extends Mapper<Object, Text, Text,Text>{

			

	    public void map(Object key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    
	      
	      String[] row = value.toString().split(",");
	      
	      map.put(lineNum,returnRemove(row));
	    System.out.println(lineNum+row[0]);
	    
	     lineNum++;
	    }}
		
		public static String[] returnRemove(String[] row){
			String[] row1= new String[row.length];
			for(int i=0; i< row.length;i++){
				row1[i]=row[i].replaceAll("\"", "");
				
			}
			return row1;
			
		}
		
		
		
		
public void format(String format, String input)throws Exception{ 
	 Configuration conf4 = new Configuration();
	    Job job = Job.getInstance(conf4, "PreProcessing");
	    
	    job.setMapperClass(TokenizerMapper.class);
	   job.setNumReduceTasks(0);
	   
	   
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);


	    FileSystem fs= FileSystem.get(conf4);
	    
	    fs.mkdirs(new Path("/user/hduser/oldOutput"));
FileInputFormat.addInputPath(job, new Path(format));
FileOutputFormat.setOutputPath(job, new Path("/user/hduser/newOutput/ProcData"));
job.waitForCompletion(true);

PreProcessing process= new PreProcessing();
process.process(map,input);




	
	
}
}
