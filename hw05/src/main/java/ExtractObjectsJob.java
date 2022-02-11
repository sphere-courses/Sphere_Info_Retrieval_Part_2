import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

public class ExtractObjectsJob extends Configured implements Tool {
    private final static Text query = new Text("query");
    private final static Text url = new Text("url");

    private final static String queryString = "query";
    final static String urlString = "url";

    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new ExtractObjectsJob(), args);
        System.exit(rc);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = GetJobConf(getConf(), args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class ExtractObjectsPartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int nPartitions) {
            if(value.toString().compareTo(queryString) == 0){
                return 0;
            } else{
                return 1;
            }
        }
    }

    public static class ExtractObjectsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException {
            String line = new String(value.getBytes(), StandardCharsets.UTF_8);

            // line = query @ other
            String[] partsFirst = line.split(Pattern.quote("@"));
            // other = geo \t urls \t urlsClicked \t timestamps
            String[] partsSecond = partsFirst[1].split(Pattern.quote("\t"));
            // urls = url , ... , url
            String[] partsThird = partsSecond[1].split(Pattern.quote(","));

            context.write(new Text(partsFirst[0]), query);
            for(String part: partsThird){
                context.write(new Text(part), url);
            }

        }
    }

    public static class ExtractObjectsReducer extends Reducer<Text, Text, Text, LongWritable> {
        long currentIdx = 0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, new LongWritable(currentIdx));
            currentIdx += 1;
        }
    }

    private static Job GetJobConf(Configuration conf, String input, String out_dir) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(ExtractObjectsJob.class);
        job.setJobName(ExtractObjectsJob.class.getCanonicalName());

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(out_dir));

        job.setMapperClass(ExtractObjectsMapper.class);
        job.setReducerClass(ExtractObjectsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(ExtractObjectsPartitioner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setNumReduceTasks(2);

        return job;
    }
}
