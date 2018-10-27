import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;

public class HITSInitJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception{
        // Temporary solution for fast debugging
        FileUtils.deleteDirectory(new File("output_HITS"));

        int exitCode = ToolRunner.run(new HITSInitJob(), args);

        System.exit(exitCode);
    }

    private Job GetJobConf(Configuration configuration, String input, String outDir) throws IOException {
        Job job = Job.getInstance(configuration);
        job.setJarByClass(HITSInitJob.class);
        job.setJobName(HITSInitJob.class.getCanonicalName());

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(outDir));

        job.setMapperClass(HITSInitJob.HITSInitMapper.class);
        job.setReducerClass(HITSInitJob.HITSInitReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception{
        Job job = GetJobConf(getConf(), args[0], args[1]);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class HITSInitMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text nodeId, Text edgesText, Context context) throws IOException, InterruptedException {
            String[] edges = edgesText.toString().split(",");
            char direction = 'O';
            for(String edge : edges) {
                if(edge.contains("-1")){
                    direction = 'I';
                    continue;
                }
                context.write(nodeId, new Text(edge + ',' + direction));
            }
        }
    }

    public static class HITSInitReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text nodeId, Iterable<Text> edges, Context context) throws IOException, InterruptedException {
            // Line format: ID\t(ID,d,a,h); ... ; (ID,d,a,h)
            //                 |          allEdges          |
            // Note: parentheses are used just for visual indentation and don't appears in line
            String leftSide = nodeId.toString();
            StringBuilder rightSide = new StringBuilder();
            for(Text edge : edges){
                rightSide.append(edge).append(",1,1;");
            }
            String edgesLine = rightSide.toString().substring(0, rightSide.length() - 1);
            context.write(new Text(leftSide), new Text(edgesLine));
        }
    }
}
