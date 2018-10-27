import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
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

import java.io.IOException;
import java.util.ArrayList;

public class HITSJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception{
        int nIters = Integer.valueOf(args[1]), exitCode = 0;
        for(int iter = 0; iter < nIters; ++iter){
            exitCode = ToolRunner
                    .run(
                            new HITSJob(),
                            new String[]{
                                    args[0] + '/' + String.valueOf(iter) + "/part-r-*",
                                    args[0] + '/' + String.valueOf(iter + 1),
                            }
                    );
            if(exitCode != 0){
                System.exit(exitCode);
            }
        }
        System.exit(0);
    }

    private Job GetJobConf(Configuration configuration, String input, String outDir) throws IOException {
        Job job = Job.getInstance(configuration);
        job.setJarByClass(HITSJob.class);
        job.setJobName(HITSJob.class.getCanonicalName());

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(outDir));

        job.setMapperClass(HITSJob.HITSMapper.class);
        job.setReducerClass(HITSJob.HITSReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception{
        Job job = GetJobConf(getConf(), args[0], args[1]);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class HITSMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text nodeId, Text edgesText, Context context) throws IOException, InterruptedException {
            String[] edgesInfo = edgesText.toString().split(";");
            ArrayList<String> outEdges = new ArrayList<>(), inEdges = new ArrayList<>();
            double aNew = 0., hNew = 0.;

            // edgeInfo = (nodeID, direction, a, h)
            String[] edgeInfo;
            for(String edge : edgesInfo){
                edgeInfo = edge.split(",");
                if(edgeInfo[1].equals("O")){
                    outEdges.add(edgeInfo[0]);
                    hNew += Double.valueOf(edgeInfo[2]);
                } else {
                    inEdges.add(edgeInfo[0]);
                    aNew += Double.valueOf(edgeInfo[3]);
                }
            }
            for(String edge : outEdges){
                context.write(
                        new Text(edge),
                        new Text(
                                nodeId.toString() +
                                        ",I," +
                                        String.valueOf(aNew) + ','
                                        + String.valueOf(hNew)
                        )
                );
            }
            for(String edge : inEdges){
                context.write(
                        new Text(edge),
                        new Text(
                                nodeId.toString() +
                                        ",O," +
                                        String.valueOf(aNew) + ','
                                        + String.valueOf(hNew)
                        )
                );
            }
        }

    }

    public static class HITSReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text nodeId, Iterable<Text> edges, Context context) throws IOException, InterruptedException {
            // Line format: ID\t(ID,d,a,h); ... ; (ID,d,a,h)
            //                 |          allEdges          |
            // Note: parentheses are used just for visual indentation and don't appears in line
            StringBuilder rightSide = new StringBuilder();
            for(Text edge : edges){
                rightSide.append(edge.toString()).append(';');
            }
            String edgesLine = rightSide.toString().substring(0, rightSide.length() - 1);
            context.write(nodeId, new Text(edgesLine));
        }
    }
}
