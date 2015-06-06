import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Deduplication {

  public static class DuplicateMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      
        word.set(value.toString());
        context.write(word, one);
    }
  }

  public static class DuplicateRemover
       extends Reducer<Text,IntWritable,Text,Text> {

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      context.write(key,null);
    }
  }

  public void deduplicate() throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Removing Duplicates");
    job.setJarByClass(Deduplication.class);
    job.setMapperClass(DuplicateMapper.class);
    
    //job.setCombinerClass(DuplicateRemover.class);
    
    job.setReducerClass(DuplicateRemover.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path("/user/hduser/newOutput/PreDuplicate"));
    FileOutputFormat.setOutputPath(job, new Path("/user/hduser/newOutput/ProcessedData"));
    job.waitForCompletion(true);
  }
}
