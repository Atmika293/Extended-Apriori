import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class PreProcessing {

	/**
	 * @param args
	 */

	public static Map<Integer, String[]> map= new HashMap<Integer, String[]>();
		public static class TokenizerMapper
	       extends Mapper<Object, Text, Text,Text>{

			

	    public void map(Object key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    
	      
	      String[] row = value.toString().split(",");
	      
	     String newRow[]=returnRemove(row);
	      String write="";
				int canWrite=0;
				for(int i=0;i<newRow.length;i++){
					
					
					if(map.get(i+1)[0].compareTo("Integer")==0){
						
						String newValue=discretize(map,i+1,newRow[i]);
						
						
						if(newValue!=null){
							
							if(i+1!=newRow.length){
								
								
							write= write+newValue+",";}
							else write=write+newValue;
						}
						else
						{ canWrite=50; //break;
						}
												}
					else if(map.get(i+1)[0].compareTo("Delete")==0){
					}
					
					else if(map.get(i+1)[0].compareTo("Alphanumeric")==0){
					
						if(!missing(map,i+1,newRow[i])){
							canWrite=50;
							//break;
													}

						if(i+1!=newRow.length){
						write= write+newRow[i]+",";}
						else write=write+newRow[i];
				
				}}
				if( canWrite==0){
					Text newLine= new Text();
					System.out.println(write+" ####");
					newLine.set(write);
					context.write(null,newLine);
					
				}
			}
		}
		
		public static String[] returnRemove(String[] row){
			String[] row1= new String[row.length];
			for(int i=0; i< row.length;i++){
				row1[i]=row[i].replaceAll("\"", "");
				
			}
			return row1;
			
		}
		
		public static Boolean missing(Map<Integer, String[]> map,Integer i, String str){
			String[] AttributeValues=map.get(i);
			for(int x=0; x<AttributeValues.length;x++){
				
				if(AttributeValues[x].contains(str)){
					
					return true;
				}
			}
			
			return false;
			
			
		}
		
		public static String discretize(Map<Integer, String[]> map, Integer i, String str){
			
			List<String>list=new ArrayList<String>(Arrays.asList(map.get(i)));
			list.remove(0);
			String[] AttributeValues= new String[list.size()];
			AttributeValues=list.toArray(AttributeValues);
			for( int x=0; x<AttributeValues.length && AttributeValues[x].compareTo("")!=0;x++){
			String[] values= AttributeValues[x].split(":");
			
			String[] range=values[1].split("-");
			str=str.trim();
			Integer num=Integer.valueOf(str);
			Integer int2= Integer.valueOf(range[1]);
			
			
			if(range[0].compareTo("<")==0  ){if(num<int2) return values[0];
											}
			
			else if( range[0].compareTo(">")==0 ){if(num>int2) return values[0];
											}
			
			else if(range[0].compareTo("=")==0){if(num==int2) return values[0];
											}
			
			else{
				Integer int1= Integer.valueOf(range[0]);
				
					if(num>int1-1 && num<int2+1){return values[0];
												}
				}	
			
														}
		return null;
		}
		
		
		
		
		
public void process(Map<Integer, String[]> map, String input)throws Exception{ 
	 Configuration conf4 = new Configuration();
	    Job job = Job.getInstance(conf4, "PreProcessing");
	    
	    job.setMapperClass(TokenizerMapper.class);
	   job.setNumReduceTasks(0);
	   
	   
	    PreProcessing.map=map;
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);


	    
	    
	    
FileInputFormat.addInputPath(job, new Path(input));
FileOutputFormat.setOutputPath(job, new Path("/user/hduser/newOutput/PreDuplicate"));
job.waitForCompletion(true);

Deduplication duplicate = new Deduplication();
duplicate.deduplicate();

	
}
}
