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

public class PageRankJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception{
        int nIters = Integer.valueOf(args[1]), exitCode = 0;
        for(int iter = 0; iter < nIters; ++iter){
            exitCode = ToolRunner
                    .run(
                            new PageRankJob(),
                            new String[]{
                                    args[0] + '/' + String.valueOf(iter) + "/part-r-*",
                                    args[0] + '/' + String.valueOf(iter + 1),
                                    args[0] + '/' + String.valueOf(iter) + "/missingPr",
                                    args[0] + '/' + String.valueOf(iter + 1) + "/missingPr"
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
        job.setJarByClass(PageRankJob.class);
        job.setJobName(PageRankJob.class.getCanonicalName());

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(outDir));

        job.setMapperClass(PageRankJob.PageRankMapper.class);
        job.setReducerClass(PageRankJob.PageRankReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception{
        // args[0] - input files
        // args[1] - output dir
        // args[2] - missed Page Rank file
        Job job = GetJobConf(getConf(), args[0], args[1]);

        Configuration configuration = job.getConfiguration();
        // Number of nodes in graph
        configuration.setInt("nNodes", 564497);
        // Probability of random jump
        configuration.setDouble("alpha", 0.15);
        // Missed Page Rank from terminal vertexes from previous iteration
        configuration.setDouble("missedPr", Utils.readDouble(configuration, args[2]));
        // File to write value of missed Page Rank
        configuration.set("missedPrFile", args[3]);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class PageRankMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text nodeInfoText, Text edgesText, Context context) throws IOException, InterruptedException {
            String[] nodeInfo = nodeInfoText.toString().split(",");
            String[] edges = edgesText.toString().split(",");

            double Pr = Double.valueOf(nodeInfo[1]);

            int nNodes = context.getConfiguration().getInt("nNodes", 1);
            double alpha = context.getConfiguration().getDouble("alpha", 0.);
            double missedPr = context.getConfiguration().getDouble("missedPr", 0.);

            // Fix Page Rank with missing Page Rank
            Pr += (1 - alpha) * missedPr / nNodes;

            // If vertex is terminal then send all Page Rank to virtual vertex
            if(edgesText.toString().equals("")){
                context.write(new Text("-1"), new Text(String.valueOf(Pr)));
                return;
            }

            for(String edge : edges){
                // Retain graph structure
                context.write(new Text(nodeInfo[0]), new Text(edge));
                // Send Page Rang to descendants
                context.write(new Text(edge), new Text("D" + String.valueOf(Pr / edges.length)));
            }

        }

    }

    public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text nodeId, Iterable<Text> outEdges, Context context) throws IOException, InterruptedException {
            // Line format: ID, Pr\tID, ... , ID
            //                     |  outEdges  |

            // Process virtual vertex and save missing Page Rank
            if(nodeId.toString().equals("-1")){
                double missingPr = 0.;
                for(Text edge : outEdges){
                    missingPr += Double.valueOf(edge.toString());
                }
                Configuration configuration = context.getConfiguration();
                Utils.writeDouble(configuration, configuration.get("missedPrFile"), missingPr);
                return;
            }

            int nNodes = context.getConfiguration().getInt("nNodes", 1);
            double alpha = context.getConfiguration().getDouble("alpha", 0.);

            double Pr = 0.;
            StringBuilder rightSide = new StringBuilder();
            for(Text edge : outEdges){
                if(edge.toString().contains("D")){
                    Pr += Double.valueOf(edge.toString().substring(1));
                } else {
                    rightSide.append(edge.toString()).append(',');
                }
            }
            Pr = alpha / nNodes + (1. - alpha) * Pr;
            String leftSide = nodeId.toString() + ',' + String.valueOf(Pr);

            String edgesLine = rightSide.toString();
            if(edgesLine.length() > 0) {
                edgesLine = edgesLine.substring(0, rightSide.length() - 1);
            }
            context.write(new Text(leftSide), new Text(edgesLine));
        }
    }
}
