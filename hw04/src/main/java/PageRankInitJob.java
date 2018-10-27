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

public class PageRankInitJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception{
        // Temporary solution for fast debugging
        FileUtils.deleteDirectory(new File("output_PR"));

        int exitCode = ToolRunner.run(new PageRankInitJob(), args);

        System.exit(exitCode);
    }

    private Job GetJobConf(Configuration configuration, String input, String outDir) throws IOException {
        Job job = Job.getInstance(configuration);
        job.setJarByClass(PageRankInitJob.class);
        job.setJobName(PageRankInitJob.class.getCanonicalName());

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(outDir));

        job.setMapperClass(PageRankInitJob.PageRankInitMapper.class);
        job.setReducerClass(PageRankInitJob.PageRankInitReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception{
        Job job = GetJobConf(getConf(), args[0], args[1]);

        Configuration configuration = job.getConfiguration();
        configuration.setInt("nNodes", 564497);

        int errorCode = job.waitForCompletion(true) ? 0 : 1;

        Utils.writeDouble(configuration, args[2], 0.);

        return errorCode;
    }

    public static class PageRankInitMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text nodeId, Text edgesText, Context context) throws IOException, InterruptedException {
            String[] edges = edgesText.toString().split(",");
            for(String edge : edges){
                if(edge.equals("-1")){
                    break;
                }
                context.write(nodeId, new Text(edge));
            }
            if(edges[0].equals("-1")){
                context.write(nodeId, new Text(""));
            }
        }
    }

    public static class PageRankInitReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text nodeId, Iterable<Text> outEdges, Context context) throws IOException, InterruptedException {
            // Line format: ID, Pr\tID, ... , ID
            //                     |  outEdges  |
            int nNodes = context.getConfiguration().getInt("nNodes", 1);
            double Pr = 1. / nNodes;
            String leftSide = nodeId.toString() + ',' + String.valueOf(Pr);
            StringBuilder rightSide = new StringBuilder();
            for(Text edge : outEdges){
                rightSide.append(edge).append(',');
            }
            String edgesLine = rightSide.toString().substring(0, rightSide.length() - 1);
            context.write(new Text(leftSide), new Text(edgesLine));
        }
    }
}
