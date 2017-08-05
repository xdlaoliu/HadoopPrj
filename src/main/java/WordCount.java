import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by Administrator on 2017/7/17.
 */
public class WordCount {

    // Step 1: Map Class
    public static class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

        private Text mapOutputkey =  new Text();
        private final static IntWritable mapOutputValue = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lineValue = value.toString();
            StringTokenizer stringTokenizer = new StringTokenizer(lineValue);
            while(stringTokenizer.hasMoreTokens()){
                String wordValue = stringTokenizer.nextToken();
                mapOutputkey.set(wordValue);
                context.write(mapOutputkey,mapOutputValue);
            }
        }
    }

    // Step 2: Reduce Class
    public static class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable>{

        private IntWritable outputValue = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value:values){
                sum += value.get();
            }
            outputValue.set(sum);
            context.write(key,outputValue);
        }
    }

    // Step 3: Driver
    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration,this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());
        Path input = new Path(args[0]);
        FileInputFormat.addInputPath(job,input);

        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(WordCountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        Path outPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job,outPath);

        boolean isSuccess = job.waitForCompletion(true);
        return isSuccess?1:0;
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        int status =new WordCount().run(args);
        System.out.println(status);
    }

}
